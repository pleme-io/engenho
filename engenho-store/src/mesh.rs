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
use crate::fjall_store::FjallStore;
use crate::network::{InProcessRouter, RpcRequest};
use crate::resource::{ResourceKey, ResourceValue};
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
}

/// Raft-replicated K8s resource store.
pub struct StoreMesh {
    raft: Raft<TypeConfig>,
    store: StoreBackend,
    node_id: RaftNodeId,
    listen_addr: String,
    router: InProcessRouter,
    rpc_task: tokio::task::JoinHandle<()>,
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
        let rpc_task = tokio::spawn(async move {
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
            rpc_task,
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
    /// namespace-scoped, AND the snapshot [`Revision`] captured from
    /// the SAME catalog clone — atomically.
    ///
    /// This is the atomic list-then-watch primitive the apiserver
    /// builds its `LIST` envelope on. Both the items and `rev` come from
    /// ONE `current_catalog()` clone: within that clone `cat.list(...)`
    /// and `cat.revision()` are a consistent snapshot of each other. A
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
        let cat = self.store.current_catalog().await;
        let items = cat
            .list(group, version, kind, namespace)
            .into_iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        (items, cat.revision())
    }

    /// Read-only snapshot of the whole catalog.
    pub async fn current_catalog(&self) -> ResourceCatalog {
        self.store.current_catalog().await
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

    pub async fn terminate(self) -> Result<(), StoreError> {
        self.router.deregister(self.node_id).await;
        self.rpc_task.abort();
        let _ = self.raft.shutdown().await;
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
