//! `StoreMesh` — public wrapper for engenho-store's Raft group.
//!
//! Mirrors `engenho-revoada::consensus::RaftMesh` but for the
//! resource catalog. K8s API operations (apiserver) decompose into
//! `propose` of typed [`crate::ResourceCommand`]s through this
//! handle; reads go through `get` / `list` on the local catalog
//! (every node has the full data via Raft replication).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::raft::ClientWriteResponse;
use openraft::{BasicNode, Config, Raft};
use tokio::sync::mpsc;

use crate::command::ResourceCommand;
use crate::fjall_store::{FjallStore, Flushed, ImageTripwire};
use crate::network::{InProcessRouter, RpcRequest};
use crate::owned_task::{OwnedTask, TaskStop};
use crate::pagination::PageAtRevision;
use crate::resource::{ListScope, ResourceKey, ResourceValue};
use crate::state::ResourceCatalog;
use crate::store::InMemoryStore;
use crate::type_config::{ApplyResult, RaftNodeId, TypeConfig};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("openraft config invalid: {0}")]
    ConfigInvalid(String),
    #[error("raft initialize failed: {0}")]
    InitializeFailed(String),
    #[error("client_write failed: {0}")]
    ClientWriteFailed(String),
    #[error("raft fatal: {0}")]
    Fatal(String),
    /// The durable image could not be written by [`StoreMesh::flush`]. The
    /// log still holds every applied entry, so nothing acknowledged is lost;
    /// the next boot replays them. Boxed so the error stays one pointer wide
    /// in every `Result` that carries it.
    #[error("persist the applied image: {0}")]
    Persist(Box<openraft::StorageError<RaftNodeId>>),
}

engenho_substrate::impl_error_kind! {
    StoreError {
        (ConfigInvalid(_)) => "config_invalid",
        (InitializeFailed(_)) => "initialize_failed",
        (ClientWriteFailed(_)) => "client_write_failed",
        (Fatal(_)) => "fatal",
        (Persist(_)) => "persist_failed",
    }
}

/// What [`StoreMesh::flush`] did, per backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum MeshFlushed {
    /// The in-memory backend: nothing outlives the process, so there is no
    /// image to write and nothing a boot could replay.
    Ephemeral,
    /// The durable backend's answer — see [`Flushed`].
    Durable(Flushed),
}

/// The store backend a [`StoreMesh`] is built over. Both variants
/// impl the same four openraft v2 traits + the same read/watch
/// surface; the enum routes the handle-side reads to whichever was
/// chosen at construction. The ephemeral [`InMemoryStore`] stays the
/// test double; [`FjallStore`] is the durable backend that survives
/// restart.
#[derive(Clone)]
enum StoreBackend {
    Memory(InMemoryStore),
    Fjall(FjallStore),
}

impl StoreBackend {
    async fn get_resource(&self, key: &ResourceKey) -> Option<ResourceValue> {
        match self {
            Self::Memory(s) => s.get_resource(key).await,
            Self::Fjall(s) => s.get_resource(key).await,
        }
    }

    /// The current MVCC revision, WITHOUT cloning the catalog.
    async fn current_revision(&self) -> crate::revision::Revision {
        match self {
            Self::Memory(s) => s.current_revision().await,
            Self::Fjall(s) => s.current_revision().await,
        }
    }

    /// List + revision without cloning the catalog's watch-replay ring — see
    /// [`crate::store::InMemoryStore::list_at_revision`] for the measurement
    /// that motivated it.
    async fn list_at_revision(
        &self,
        group: &str,
        version: &str,
        kind: &str,
        namespace: Option<&str>,
    ) -> (Vec<(ResourceKey, ResourceValue)>, crate::revision::Revision) {
        match self {
            Self::Memory(s) => s.list_at_revision(group, version, kind, namespace).await,
            Self::Fjall(s) => s.list_at_revision(group, version, kind, namespace).await,
        }
    }

    /// One page + revision under ONE backend guard, cloning only the page —
    /// see [`crate::store::InMemoryStore::list_page_at_revision`].
    async fn list_page_at_revision(
        &self,
        scope: ListScope<'_>,
        after: Option<&ResourceKey>,
        limit: usize,
    ) -> PageAtRevision {
        match self {
            Self::Memory(s) => s.list_page_at_revision(scope, after, limit).await,
            Self::Fjall(s) => s.list_page_at_revision(scope, after, limit).await,
        }
    }

    async fn current_catalog(&self) -> ResourceCatalog {
        match self {
            Self::Memory(s) => s.current_catalog().await,
            Self::Fjall(s) => s.current_catalog().await,
        }
    }

    async fn watch_from(
        &self,
        opts: crate::watch_backend::WatchOpts,
    ) -> Result<crate::watch_backend::WatchStream, crate::watch_backend::WatchGone> {
        match self {
            Self::Memory(s) => s.watch_from(opts).await,
            Self::Fjall(s) => s.watch_from(opts).await,
        }
    }

    async fn watch_subscribe(
        &self,
    ) -> Result<crate::watch_backend::WatchStream, crate::watch_backend::WatchGone> {
        match self {
            Self::Memory(s) => s.watch_subscribe().await,
            Self::Fjall(s) => s.watch_subscribe().await,
        }
    }

    async fn watch_subscriber_count(&self) -> usize {
        match self {
            Self::Memory(s) => s.watch_subscriber_count().await,
            Self::Fjall(s) => s.watch_subscriber_count().await,
        }
    }

    /// `true` if the underlying store already holds Raft state. The
    /// in-memory store is never durably initialized — it always reports
    /// `false` so the ephemeral path always initializes on `start`.
    async fn is_initialized(&self) -> bool {
        match self {
            Self::Memory(_) => false,
            Self::Fjall(s) => s.is_initialized().await,
        }
    }

    async fn quiesce_bookmarks(&self) -> TaskStop {
        match self {
            Self::Memory(s) => s.quiesce_bookmarks().await,
            Self::Fjall(s) => s.quiesce_bookmarks().await,
        }
    }

    async fn flush(&self) -> Result<MeshFlushed, StoreError> {
        match self {
            Self::Memory(_) => Ok(MeshFlushed::Ephemeral),
            Self::Fjall(s) => s
                .flush()
                .await
                .map(MeshFlushed::Durable)
                .map_err(|e| StoreError::Persist(Box::new(e))),
        }
    }
}

/// How [`StoreMesh::quiesce`] left each background task the mesh owns.
///
/// One field per owned task. `quiesce` destructures `StoreMesh` without
/// `..`, so a new field on the mesh does not compile (E0027) until quiesce
/// names it — stopped here, or bound to `_` with the reason it is not a
/// task. Choosing `_` wrongly is caught only by review.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct Quiesced {
    /// The raft RPC pump: the task that feeds peer RPCs from the router
    /// into `raft`.
    pub rpc_pump: TaskStop,
    /// The store's bookmark ticker, which holds an upgraded store reference
    /// for the length of each tick.
    pub bookmark_ticker: TaskStop,
}

impl Quiesced {
    /// `true` if either task had panicked before it was stopped — it is gone
    /// now, but it had stopped doing its job some time before.
    pub fn any_panicked(&self) -> bool {
        self.rpc_pump == TaskStop::Panicked || self.bookmark_ticker == TaskStop::Panicked
    }
}

/// Raft-replicated K8s resource store.
pub struct StoreMesh {
    raft: Raft<TypeConfig>,
    store: StoreBackend,
    node_id: RaftNodeId,
    listen_addr: String,
    router: InProcessRouter,
    /// Feeds peer RPCs from the router into `raft`, holding a `Raft` clone.
    /// Owned so [`Self::quiesce`] can abort AND await it.
    rpc_pump: OwnedTask,
}

impl StoreMesh {
    /// Start an EPHEMERAL (in-memory) store mesh — the test / dev
    /// path. Process restart discards all state. For durable
    /// single-node restart-safe storage use [`Self::start_durable`].
    pub async fn start(
        node_id: RaftNodeId,
        listen_addr: String,
        router: InProcessRouter,
        config: Arc<Config>,
    ) -> Result<Self, StoreError> {
        let store = InMemoryStore::new();
        Self::start_with_backend(
            node_id,
            listen_addr,
            router,
            config,
            store.clone(),
            store.clone(),
            StoreBackend::Memory(store),
        )
        .await
    }

    /// Start a DURABLE store mesh backed by a fjall keyspace at
    /// `store_path`. The keyspace is opened (or created) + hydrated
    /// before `Raft::new`, so openraft sees any persisted vote / log /
    /// applied position and resumes WITHOUT re-initializing. On a
    /// fresh dir the store is empty and the caller must call
    /// [`Self::initialize_if_fresh`] (or [`Self::start_or_resume`],
    /// which does it for you).
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Fatal`] if the keyspace can't be opened /
    /// hydrated or `Raft::new` fails.
    pub async fn start_durable(
        node_id: RaftNodeId,
        listen_addr: String,
        router: InProcessRouter,
        config: Arc<Config>,
        store_path: impl Into<std::path::PathBuf>,
    ) -> Result<Self, StoreError> {
        let store = FjallStore::open(store_path)?;
        Self::start_with_backend(
            node_id,
            listen_addr,
            router,
            config,
            store.clone(),
            store.clone(),
            StoreBackend::Fjall(store),
        )
        .await
    }

    /// Start a durable mesh AND initialize it iff the store is fresh
    /// (no persisted vote / applied position). On a restart this is a
    /// no-op for initialization — openraft resumes from disk. The
    /// boolean in the return tuple is `true` when the mesh was freshly
    /// initialized this call, `false` when it resumed existing state.
    ///
    /// # Errors
    ///
    /// Propagates [`Self::start_durable`] + initialize failures.
    pub async fn start_or_resume(
        node_id: RaftNodeId,
        listen_addr: String,
        router: InProcessRouter,
        config: Arc<Config>,
        store_path: impl Into<std::path::PathBuf>,
    ) -> Result<(Self, bool), StoreError> {
        let mesh = Self::start_durable(node_id, listen_addr, router, config, store_path).await?;
        let initialized = mesh.initialize_if_fresh().await?;
        Ok((mesh, initialized))
    }

    /// Shared raft + rpc-task wiring. The store is passed three times
    /// — as the log store, the state machine, and the read-side
    /// backend handle — all clones of the same underlying store.
    async fn start_with_backend<LS, SM>(
        node_id: RaftNodeId,
        listen_addr: String,
        router: InProcessRouter,
        config: Arc<Config>,
        log_store: LS,
        state_machine: SM,
        backend: StoreBackend,
    ) -> Result<Self, StoreError>
    where
        LS: openraft::storage::RaftLogStorage<TypeConfig>,
        SM: openraft::storage::RaftStateMachine<TypeConfig>,
    {
        let (tx_rpc, mut rx_rpc) = mpsc::channel::<RpcRequest>(256);

        let raft =
            Raft::<TypeConfig>::new(node_id, config, router.clone(), log_store, state_machine)
                .await
                .map_err(|e| StoreError::Fatal(e.to_string()))?;

        router.register(node_id, tx_rpc).await;

        let raft_for_rpc = raft.clone();
        let rpc_pump = OwnedTask::spawn(async move {
            while let Some(req) = rx_rpc.recv().await {
                match req {
                    RpcRequest::AppendEntries(rpc, reply) => {
                        if let Ok(resp) = raft_for_rpc.append_entries(rpc).await {
                            let _ = reply.send(resp);
                        }
                    }
                    RpcRequest::Vote(rpc, reply) => {
                        if let Ok(resp) = raft_for_rpc.vote(rpc).await {
                            let _ = reply.send(resp);
                        }
                    }
                    RpcRequest::InstallSnapshot(rpc, reply) => {
                        if let Ok(resp) = raft_for_rpc.install_snapshot(rpc).await {
                            let _ = reply.send(resp);
                        }
                    }
                }
            }
        });

        Ok(Self {
            raft,
            store: backend,
            node_id,
            listen_addr,
            router,
            rpc_pump,
        })
    }

    /// `true` if the underlying store already holds Raft state (a
    /// vote was persisted OR a command was applied). In-memory meshes
    /// always report `false`.
    pub async fn is_initialized(&self) -> bool {
        self.store.is_initialized().await
    }

    /// Initialize the singleton cluster ONLY when the store is fresh.
    /// Returns `true` if it initialized, `false` if the store already
    /// held state (a restart — calling `initialize` again would error).
    ///
    /// # Errors
    ///
    /// Propagates [`Self::initialize_singleton`] failures on a fresh
    /// store.
    pub async fn initialize_if_fresh(&self) -> Result<bool, StoreError> {
        if self.is_initialized().await {
            return Ok(false);
        }
        self.initialize_singleton().await?;
        Ok(true)
    }

    pub async fn initialize_singleton(&self) -> Result<(), StoreError> {
        let mut members = BTreeMap::new();
        members.insert(
            self.node_id,
            BasicNode {
                addr: self.listen_addr.clone(),
            },
        );
        self.raft
            .initialize(members)
            .await
            .map_err(|e| StoreError::InitializeFailed(e.to_string()))?;
        Ok(())
    }

    pub async fn initialize_with_voters(
        &self,
        voters: Vec<(RaftNodeId, String)>,
    ) -> Result<(), StoreError> {
        let mut members = BTreeMap::new();
        for (id, addr) in voters {
            members.insert(id, BasicNode { addr });
        }
        self.raft
            .initialize(members)
            .await
            .map_err(|e| StoreError::InitializeFailed(e.to_string()))?;
        Ok(())
    }

    pub async fn propose(&self, cmd: ResourceCommand) -> Result<ApplyResult, StoreError> {
        let resp: ClientWriteResponse<TypeConfig> = self
            .raft
            .client_write(cmd)
            .await
            .map_err(|e| StoreError::ClientWriteFailed(e.to_string()))?;
        Ok(resp.data)
    }

    /// Read a single resource by typed key — synchronous catalog
    /// read (no Raft round-trip needed; the local replica has the
    /// data once apply has caught up).
    pub async fn get(&self, key: &ResourceKey) -> Option<ResourceValue> {
        self.store.get_resource(key).await
    }

    /// List resources matching (group, version, kind), optionally
    /// namespace-scoped — dropping the snapshot revision.
    ///
    /// Thin wrapper over [`Self::list_at_revision`]: callers that need
    /// the atomic list-then-watch resume point MUST use that instead.
    /// This method exists for the (rare) callers that only want the
    /// items.
    pub async fn list(
        &self,
        group: &str,
        version: &str,
        kind: &str,
        namespace: Option<&str>,
    ) -> Vec<(ResourceKey, ResourceValue)> {
        self.list_at_revision(group, version, kind, namespace)
            .await
            .0
    }

    /// List resources matching (group, version, kind), optionally
    /// namespace-scoped, AND the snapshot [`Revision`] read under the
    /// SAME backend guard — atomically.
    ///
    /// This is the atomic list-then-watch primitive the apiserver
    /// builds its `LIST` envelope on. Both the items and `rev` are read
    /// under ONE lock of the backend's catalog, so they are a consistent
    /// snapshot of each other. A
    /// client that LISTs at the returned `rev` then `watch_from(rev)`
    /// resumes from exactly that snapshot boundary — gap-free + dup-free.
    ///
    /// Returns `current_revision` (the dense MVCC counter the watch
    /// backend keys on), NEVER `last_applied_index` (the Raft log
    /// index) — the latter is the load-bearing bug item 4 fixes.
    pub async fn list_at_revision(
        &self,
        group: &str,
        version: &str,
        kind: &str,
        namespace: Option<&str>,
    ) -> (Vec<(ResourceKey, ResourceValue)>, crate::revision::Revision) {
        // ★ NOT `current_catalog()`. That clones the whole ResourceCatalog —
        // including its 8192-entry watch-replay history ring — so serving a
        // LIST cost time proportional to the cluster's AGE rather than to the
        // number of objects listed. Measured 2026-09-14: 4.4ms → 31.8ms as the
        // ring filled while ONE object existed, plateauing exactly at the cap;
        // on rio, whose ring held ~100KB Helm release Secrets, the same clone
        // pushed a single write to 12s fresh and 60s+ after hours and wedged
        // Flux for five days. Atomicity is unchanged and in fact tightened:
        // items and revision now come from one guard, not from a clone.
        self.store
            .list_at_revision(group, version, kind, namespace)
            .await
    }

    /// One page of resources matching (group, version, kind), optionally
    /// namespace-scoped, AND the [`crate::revision::Revision`] read under
    /// the SAME backend guard — so the items + the reported revision are
    /// mutually consistent FOR THIS CALL. The range-pagination sibling of
    /// [`Self::list_at_revision`].
    ///
    /// ★ NOT `current_catalog()`. That deep-clones every resource plus the
    /// 8192-entry watch-replay ring, and this runs once per PAGE of every
    /// informer relist: after a restart every client relists at once, so a
    /// per-page clone multiplies the cost that wedged Flux on rio by the
    /// number of pages. The page is read under one guard, over the scope's
    /// own run of keys (see [`ListScope`]), and only its items are cloned.
    ///
    /// ## Consistency: per-call read, NOT cross-page snapshot isolation
    ///
    /// Each call reads the CURRENT catalog; the returned `snapshot_rev` is
    /// the live revision at THIS call. Across a page SERIES this is
    /// cursor-based pagination (gap/dup-free for a quiescent key set, and a
    /// PRE-cursor late insert cannot resurface), NOT true MVCC snapshot
    /// isolation: a POST-cursor insert committed between page calls WILL
    /// appear on a later page, because the next call reads the live catalog
    /// rather than reading AS OF the token's first-page revision. The
    /// `snapshot_rev` baked into the continue token is the envelope
    /// `resourceVersion` LABEL, not a read-isolation mechanism. See
    /// [`crate::state::ResourceCatalog::list_page`] for the destination
    /// (revision-indexed historical reads, deferred — needs retained
    /// historical MVCC views).
    ///
    /// Returns `(items, snapshot_rev, next, remaining)` — the fields of
    /// [`PageAtRevision`]:
    ///   * `items` — up to `limit` `(key, value)` pairs starting strictly
    ///     after `after`, in total key order.
    ///   * `snapshot_rev` — the catalog revision at this call (the LABEL
    ///     reported in the LIST envelope `resourceVersion`).
    ///   * `next` — the cursor key for the following page (the last
    ///     emitted key iff more matching items remain), else `None`.
    ///   * `remaining` — count of still-unreturned matching items.
    pub async fn list_page_at_revision(
        &self,
        group: &str,
        version: &str,
        kind: &str,
        namespace: Option<&str>,
        after: Option<&ResourceKey>,
        limit: usize,
    ) -> (
        Vec<(ResourceKey, ResourceValue)>,
        crate::revision::Revision,
        Option<ResourceKey>,
        u64,
    ) {
        let PageAtRevision {
            items,
            revision,
            next,
            remaining,
        } = self
            .store
            .list_page_at_revision(
                ListScope::new(group, version, kind, namespace),
                after,
                limit,
            )
            .await;
        (items, revision, next, remaining)
    }

    /// Read-only snapshot of the whole catalog.
    pub async fn current_catalog(&self) -> ResourceCatalog {
        self.store.current_catalog().await
    }

    /// The current MVCC revision, read WITHOUT cloning the catalog.
    ///
    /// ★ Reach for this instead of `current_catalog().revision()`. That reads
    /// one `u64` by deep-cloning every resource plus the 8192-entry
    /// watch-replay ring (two full resource bodies per entry). Measured on
    /// rio: it made each watch establishment cost hundreds of MB of memcpy
    /// under the same lock `apply` needs, stalling writes for tens of seconds
    /// at ~2 cores of CPU while FluxCD held dozens of watches.
    pub async fn current_revision(&self) -> crate::revision::Revision {
        self.store.current_revision().await
    }

    /// Open a RESUMABLE, gap-free watch from `opts.from`. The
    /// subscription registers under the backend's catalog lock, so the
    /// replay snapshot + the live-tail attachment are one atomic act —
    /// no change committed during subscription is missed or doubled.
    /// Routes through [`StoreBackend`] so both Memory + Fjall get the
    /// single implementation.
    ///
    /// This is the entry point the apiserver hands to kubectl
    /// `--watch` + controller informers: list at revision R, then
    /// `watch_from(R)` resumes with no gap + no dup.
    ///
    /// # Errors
    ///
    /// [`crate::watch_backend::WatchGone::CompactedTooOld`] when
    /// `opts.from` is below the compaction watermark.
    pub async fn watch_from(
        &self,
        opts: crate::watch_backend::WatchOpts,
    ) -> Result<crate::watch_backend::WatchStream, crate::watch_backend::WatchGone> {
        self.store.watch_from(opts).await
    }

    /// Subscribe to the LIVE-TAIL watch stream (compatibility shim over
    /// [`Self::watch_from`]) — attaches from the current revision
    /// forward with NO replay + NO bookmarks. Surfaces overflow as a
    /// typed [`crate::watch_backend::WatchGone::Overflow`] (the
    /// consumer re-lists / resumes), never a silent
    /// `broadcast::RecvError::Lagged`.
    ///
    /// # Errors
    ///
    /// Never errors in practice (live-tail resumes from current
    /// revision); the `Result` matches `watch_from`.
    pub async fn watch(
        &self,
    ) -> Result<crate::watch_backend::WatchStream, crate::watch_backend::WatchGone> {
        self.store.watch_subscribe().await
    }

    /// Active watch subscriber count (live registry size; telemetry +
    /// test helper).
    pub async fn watch_subscriber_count(&self) -> usize {
        self.store.watch_subscriber_count().await
    }

    pub async fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    pub async fn wait_for_leadership(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut rx = self.raft.metrics().clone();
        loop {
            if rx.borrow().current_leader == Some(self.node_id) {
                return true;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }
            if tokio::time::timeout(remaining, rx.changed()).await.is_err() {
                return false;
            }
        }
    }

    /// Wait for the state-machine apply index to reach `target`
    /// or `timeout` elapses.
    pub async fn wait_for_applied(&self, target: u64, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let catalog = self.current_catalog().await;
            if catalog.last_applied_index >= target {
                return true;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(50.min(remaining.as_millis() as u64))).await;
        }
    }

    pub fn node_id(&self) -> RaftNodeId {
        self.node_id
    }

    /// Stop every background task the mesh owns — the raft RPC pump and
    /// the store's bookmark ticker — aborting AND awaiting each, so that on
    /// return neither holds anything of the store's.
    ///
    /// Takes `&self` so a shutdown can call it while the mesh is still
    /// behind an `Arc`, before `Arc::try_unwrap` + [`Self::terminate`].
    ///
    /// Afterwards: peers can no longer reach this node (a request is refused
    /// at the send, never accepted and then dropped), and watchers receive
    /// no more periodic bookmarks. Raft itself keeps running until
    /// `terminate`. One-way and idempotent: a second call reports
    /// [`TaskStop::AlreadyStopped`] for both tasks.
    ///
    /// What it cannot reach: a task outside the mesh that upgrades a
    /// `Weak<StoreMesh>` (the etcd façade does, per request). Draining those
    /// is the caller's job; nothing here prevents a new one.
    pub async fn quiesce(&self) -> Quiesced {
        // Exhaustive on purpose (no `..`): every field is a decision.
        let Self {
            // Not a task: stopped by `terminate` (`Raft::shutdown`).
            raft: _,
            // Owns the bookmark ticker, stopped below.
            store,
            // Plain data.
            node_id: _,
            listen_addr: _,
            // A registry of senders, not a task; `terminate` deregisters.
            router: _,
            rpc_pump,
        } = self;
        let rpc_pump = rpc_pump.stop().await;
        let bookmark_ticker = store.quiesce_bookmarks().await;
        Quiesced {
            rpc_pump,
            bookmark_ticker,
        }
    }

    /// Bring the durable image up to the applied state, so the next boot
    /// replays nothing applied before this call — see
    /// [`FjallStore::flush`]. The in-memory backend answers
    /// [`MeshFlushed::Ephemeral`].
    ///
    /// Takes `&self` so a stop can call it while the mesh is still behind an
    /// `Arc`, after [`Self::quiesce`] and before `Arc::try_unwrap`: if a
    /// leaked clone then makes the unwrap fail and [`Self::terminate`] never
    /// runs, the image is already current. `terminate` flushes again as its
    /// last step, which answers `AlreadyDurable` unless something was applied
    /// in between.
    ///
    /// # Errors
    ///
    /// [`StoreError::Persist`] if the durable batch cannot be written.
    pub async fn flush(&self) -> Result<MeshFlushed, StoreError> {
        self.store.flush().await
    }

    /// The durable-image tripwire counts (T3.4) — see
    /// [`FjallStore::image_tripwire`]. `None` for the in-memory backend,
    /// which has no durable image to disagree with itself.
    pub async fn image_tripwire(&self) -> Option<ImageTripwire> {
        match &self.store {
            StoreBackend::Memory(_) => None,
            StoreBackend::Fjall(s) => Some(s.image_tripwire().await),
        }
    }

    /// Deregister from the router, [`Self::quiesce`] (awaiting both owned
    /// tasks), shut raft down, then [`Self::flush`] — so a clean stop leaves
    /// the next boot nothing to replay. A task that had panicked is logged at
    /// ERROR rather than read as a clean stop.
    ///
    /// The flush runs after `Raft::shutdown`, when raft no longer drives
    /// applies. Not guaranteed on return: openraft's state-machine worker
    /// holds a store clone and `Raft::shutdown` does not join it, so that
    /// clone is released when that worker next runs, not necessarily before
    /// this returns; an entry it applies after the flush is durable in the
    /// log and replayed on the next boot.
    ///
    /// # Errors
    ///
    /// [`StoreError::Persist`] if the final flush cannot be written.
    pub async fn terminate(self) -> Result<(), StoreError> {
        self.router.deregister(self.node_id).await;
        let quiesced = self.quiesce().await;
        if quiesced.any_panicked() {
            tracing::error!(
                node_id = self.node_id,
                ?quiesced,
                "a store background task had panicked before terminate"
            );
        }
        let _ = self.raft.shutdown().await;
        let flushed = self.store.flush().await?;
        tracing::info!(
            node_id = self.node_id,
            ?flushed,
            "store flushed at terminate"
        );
        Ok(())
    }
}

pub fn default_config(cluster_name: &str) -> Result<Arc<Config>, StoreError> {
    let cfg = Config {
        cluster_name: cluster_name.to_string(),
        heartbeat_interval: 250,
        election_timeout_min: 500,
        election_timeout_max: 1000,
        enable_tick: true,
        enable_heartbeat: true,
        enable_elect: true,
        ..Default::default()
    };
    let validated = cfg
        .validate()
        .map_err(|e| StoreError::ConfigInvalid(e.to_string()))?;
    Ok(Arc::new(validated))
}
