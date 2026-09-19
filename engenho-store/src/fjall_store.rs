//! fjall-backed `RaftLogStorage` + `RaftStateMachine` for the
//! engenho-store Raft group — the durable sibling of
//! [`crate::store::InMemoryStore`].
//!
//! ## Destination (R6.5, item-2 durable persistence)
//!
//! engenho-store has ONE durable storage stack: a fjall keyspace
//! (LSM embedded KV) implementing openraft v2's storage traits so a
//! single-node restart is non-destructive — the Raft log, vote,
//! committed pointer, membership, AND the full
//! [`ResourceCatalog`] (resources + MVCC revision state) all survive
//! a process restart. The three previously-orphaned on-disk formats
//! (journal log / catalog-snapshot file / durable-store wrapper)
//! collapse into this single keyspace. This is the same shape
//! engenho-revoada will later reuse via `pleme-io/openraft-mem`.
//!
//! ## Keyspace layout (one keyspace per store dir)
//!
//!   * `log` partition — Raft log entries. Key = `u64` log index as
//!     8-byte BIG-ENDIAN (so fjall's lexicographic range order ==
//!     numeric index order, required by `try_get_log_entries(range)`
//!     and `get_log_state`'s "last entry"). Value =
//!     `serde_json::to_vec(&Entry<TypeConfig>)`.
//!   * `meta` partition — fixed string keys → serde_json bytes:
//!     `vote`, `committed`, `last_purged`, `last_applied`,
//!     `last_membership`, `snapshot_meta`, `snapshot_data`,
//!     `snapshot_index`.
//!   * `catalog` partition — the materialized state machine WITH
//!     revision state, single key `catalog` → `serde_json::to_vec(
//!     &ResourceCatalog)`. The catalog's hand-written Serialize
//!     already carries resources (value + VersionMeta),
//!     last_applied_term/index, current_revision + compacted_revision.
//!     The history ring is not persisted, so on load the compaction
//!     floor is set to current_revision and the stored floor is
//!     ignored (T3.3); the ring refills from the replay above it. One
//!     value = all of item-1's durable revision state.
//!
//! ## Durability discipline
//!
//!   * `append` fsyncs (`keyspace.persist(PersistMode::SyncAll)`)
//!     BEFORE calling `callback.log_io_completed(Ok(()))` — the real
//!     durability the in-memory `append`'s synchronous no-fsync path
//!     lacks.
//!   * `apply` persists the APPLIED IMAGE — the catalog blob,
//!     `last_applied` and `last_membership` — as ONE atomic, fsynced
//!     fjall batch, on a count/time cadence (see
//!     `CATALOG_PERSIST_EVERY`). Watch events are fanned only after
//!     the entry is durable in the log — a watcher never observes an
//!     event that didn't survive to disk.
//!   * [`FjallStore::flush`] writes the same image through the same
//!     batch, whatever the cadence says. A clean stop calls it last, so
//!     the next boot has nothing to replay.
//!   * Every disk failure maps to a typed
//!     `StorageError::IO { source: StorageIOError::… }`. NEVER panic
//!     / unwrap on a disk error.

use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, RaftLogReader, RaftSnapshotBuilder, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership, Vote,
};
use tokio::sync::Mutex;

use crate::mesh::StoreError;
use crate::owned_task::TaskStop;
use crate::state::ResourceCatalog;
use crate::type_config::{ApplyResult, RaftNodeId, TypeConfig};
use crate::watch_backend::{
    BookmarkTickable, BookmarkTicker, WatchGone, WatchOpts, WatchStream, WatcherRegistry,
};

/// Watch buffer capacity — mirrors [`crate::store::InMemoryStore`].
const WATCH_CHANNEL_CAPACITY: usize = crate::watch_backend::WATCH_CHANNEL_CAPACITY;

/// fjall partition names.
const LOG_PARTITION: &str = "log";
const META_PARTITION: &str = "meta";
const CATALOG_PARTITION: &str = "catalog";

/// `meta` partition fixed keys.
const META_VOTE: &[u8] = b"vote";
const META_COMMITTED: &[u8] = b"committed";
const META_LAST_PURGED: &[u8] = b"last_purged";
const META_LAST_APPLIED: &[u8] = b"last_applied";
const META_LAST_MEMBERSHIP: &[u8] = b"last_membership";
const META_SNAPSHOT_META: &[u8] = b"snapshot_meta";
const META_SNAPSHOT_DATA: &[u8] = b"snapshot_data";
const META_SNAPSHOT_INDEX: &[u8] = b"snapshot_index";

/// `catalog` partition single key.
const CATALOG_KEY: &[u8] = b"catalog";

/// How many applied entries may pass before the catalog blob is rewritten.
///
/// ★ THE CATALOG BLOB IS A CACHE, NOT THE DURABILITY SOURCE. Every log entry
/// is already serialized and **fsynced into the `log` partition before it is
/// applied** (see `append`), so a crash loses nothing: openraft replays from
/// the persisted `last_applied`, and replaying a raft log is deterministic —
/// the same entries against the same starting catalog produce the same
/// resources AND the same MVCC revisions. That determinism is the whole
/// premise of a replicated state machine; it is what makes skipping this
/// write safe rather than merely cheap.
///
/// Rewriting it on EVERY apply is what made writes unusable. Measured on rio
/// 2026-09-15, on an idle box: a single ConfigMap PUT took **4.9–17.3 s** and
/// climbed across consecutive samples, while the `catalog` partition had
/// grown to **578 MB for 42 live objects** — accumulated versions of one hot
/// key that compaction could not keep ahead of. The cost is not the fsync, it
/// is serializing the whole cluster state and handing an LSM a multi-hundred-
/// kilobyte value on every write.
///
/// The consequence was not slowness. Flux's controllers renew a leader-election
/// Lease on a ~10 s deadline; a 17 s write overruns it, controller-runtime logs
/// `leader election lost` and exits by design, so kustomize-controller and
/// helm-controller crash-looped and nothing reconciled.
///
/// ★ THE INVARIANT THAT MAKES THIS CORRECT: the catalog and `last_applied` are
/// written **together or not at all**. Persisting `last_applied` while skipping
/// the catalog would make a restart trust a catalog that is behind it — the one
/// way to actually lose data here. On the apply side there is one writer of the
/// pair, [`FjallStore::persist_applied_image`], and it writes both (with
/// `last_membership`) in a single atomic fjall batch — so neither can land
/// without the other, not even across a crash between two inserts.
const CATALOG_PERSIST_EVERY: usize = 64;

/// Wall-clock bound on the same decision, so a cluster that writes rarely
/// still lands its catalog promptly instead of waiting for a 64th entry that
/// may be hours away. A crash then costs a bounded replay, not a long one.
const CATALOG_PERSIST_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

type RNodeId = RaftNodeId;
type RMembership = StoredMembership<RaftNodeId, openraft::BasicNode>;

/// What [`FjallStore::flush`] found, and what it did about it.
///
/// On either arm the durable image — the catalog blob, `last_applied` and
/// `last_membership` — equals the applied state as of the call, so a boot
/// from this directory replays nothing that was applied before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum Flushed {
    /// Nothing had been applied since the image was last written or loaded,
    /// so it already matched; nothing was written.
    AlreadyDurable {
        /// The applied position the durable image stands at.
        last_applied: Option<LogId<RNodeId>>,
    },
    /// The image was behind the applied state — those entries were waiting
    /// for the next boot to replay them. It was rewritten in one fsynced batch.
    Persisted {
        /// The applied position the durable image now stands at.
        last_applied: Option<LogId<RNodeId>>,
    },
}

/// fjall-backed durable store. Same external shape as
/// [`crate::store::InMemoryStore`] (Clone, the same four openraft
/// traits, the same watch surface). The fjall partitions are the
/// source of truth on disk; an in-RAM [`MaterializedState`] mirrors
/// the working state-machine copy for fast reads + watch.
#[derive(Clone)]
pub struct FjallStore {
    inner: Arc<FjallInner>,
    /// Store-level bookmark ticker — one per store, driving the
    /// per-watcher bookmark cadence under the catalog lock. `Arc` so
    /// every clone of the store shares (and keeps alive) the single
    /// ticker; dropping the last store clone aborts it, and
    /// [`Self::quiesce_bookmarks`] aborts AND awaits it. The difference
    /// matters here more than anywhere: a tick holds an upgraded
    /// `Arc<FjallInner>`, and `FjallInner` owns the data-directory lock.
    bookmark_ticker: Arc<BookmarkTicker>,
}

/// The on-disk handles + the in-RAM working copy.
struct FjallInner {
    /// Exclusive claim on the data directory, held for as long as this store
    /// is open.
    ///
    /// ── ★ DO NOT DROP THIS EARLY, AND DO NOT `_`-PREFIX IT ─────────────
    /// Its Drop releases the directory. fjall does not lock its own
    /// directory (2.11.2 depends on no locking crate and has no
    /// already-open error), so while this is held is exactly the window in
    /// which a second opener is refused. A store was destroyed on
    /// 2026-09-01 by two processes opening one directory: it went
    /// `Poisoned`, then `Storage(Unrecoverable)`, and never opened again.
    /// See `crate::data_dir_lock`.
    _dir_lock: crate::data_dir_lock::DataDirLock,
    keyspace: fjall::Keyspace,
    log: fjall::PartitionHandle,
    meta: fjall::PartitionHandle,
    catalog: fjall::PartitionHandle,
    /// In-RAM mirror of the state machine — hydrated on open,
    /// kept in lockstep with the disk partitions under one lock.
    state: Mutex<MaterializedState>,
}

/// The ticker drives bookmarks under the SAME `Mutex<MaterializedState>`
/// the apply path holds — so a bookmark + a concurrent apply can't
/// interleave on the registry. The trait is local, so this impl on the
/// foreign `tokio::sync::Mutex` is allowed by the orphan rule.
impl BookmarkTickable for FjallInner {
    async fn tick_once(&self) {
        let mut state = self.state.lock().await;
        let rev = state.catalog.revision();
        let now = tokio::time::Instant::now();
        state.watchers.tick_bookmarks(rev, now);
    }
}

/// In-RAM working copy of the durable state. Mirrors `Inner` from
/// the in-memory store — the cached vote/committed/etc let reads
/// skip a disk round-trip; the catalog carries the live history ring.
#[derive(Default)]
struct MaterializedState {
    vote: Option<Vote<RNodeId>>,
    committed: Option<LogId<RNodeId>>,
    last_purged: Option<LogId<RNodeId>>,
    catalog: ResourceCatalog,
    last_applied: Option<LogId<RNodeId>>,
    last_membership: RMembership,
    snapshot_index: u64,
    /// Live watch registry — lives UNDER this catalog lock so
    /// subscription + replay-capture + live fan-out are atomic against
    /// `catalog.apply`. The crux of the gap-free / dup-free handoff,
    /// identical to the in-memory store.
    watchers: WatcherRegistry,
    /// Applied entries since the catalog blob was last written to disk.
    /// See [`CATALOG_PERSIST_EVERY`].
    applies_since_catalog_persist: usize,
    /// When the catalog blob was last written. `None` == never this process,
    /// which forces the first apply to persist — so a node that takes one
    /// write and then idles still lands its state.
    last_catalog_persist: Option<std::time::Instant>,
}

// ── error mapping helpers ─────────────────────────────────────────
// Every fjall failure becomes a typed StorageError::IO. The verb
// (read/write logs/vote/state_machine/snapshot) is chosen by call
// site so operators see WHERE the disk failed.

fn io_to_anyerror(e: &(impl std::error::Error + 'static)) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

fn write_logs_err(e: &(impl std::error::Error + 'static)) -> StorageError<RNodeId> {
    StorageError::IO {
        source: StorageIOError::write_logs(&io_to_anyerror(e)),
    }
}

fn read_logs_err(e: &(impl std::error::Error + 'static)) -> StorageError<RNodeId> {
    StorageError::IO {
        source: StorageIOError::read_logs(&io_to_anyerror(e)),
    }
}

fn write_vote_err(e: &(impl std::error::Error + 'static)) -> StorageError<RNodeId> {
    StorageError::IO {
        source: StorageIOError::write_vote(&io_to_anyerror(e)),
    }
}

fn write_sm_err(e: &(impl std::error::Error + 'static)) -> StorageError<RNodeId> {
    StorageError::IO {
        source: StorageIOError::write_state_machine(&io_to_anyerror(e)),
    }
}

fn write_snapshot_err(
    sig: Option<openraft::storage::SnapshotSignature<RNodeId>>,
    e: &(impl std::error::Error + 'static),
) -> StorageError<RNodeId> {
    StorageError::IO {
        source: StorageIOError::write_snapshot(sig, &io_to_anyerror(e)),
    }
}

fn read_snapshot_err(
    sig: Option<openraft::storage::SnapshotSignature<RNodeId>>,
    e: &(impl std::error::Error + 'static),
) -> StorageError<RNodeId> {
    StorageError::IO {
        source: StorageIOError::read_snapshot(sig, &io_to_anyerror(e)),
    }
}

impl FjallStore {
    /// Open or create the keyspace at `path`, open all partitions,
    /// then hydrate the in-RAM [`MaterializedState`] from the
    /// persisted `catalog` + `meta` partitions.
    ///
    /// openraft drives recovery itself once `Raft::new` calls
    /// `read_vote` / `get_log_state` / `applied_state` — it sees the
    /// persisted log + applied position + vote and resumes WITHOUT
    /// re-initializing. Callers MUST gate `initialize` on
    /// [`Self::is_initialized`] (see `StoreMesh::initialize_if_fresh`).
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Fatal`] if the keyspace / partitions
    /// can't be opened or a persisted value can't be deserialized.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StoreError> {
        let path = path.into();
        // ── ★ TAKE THE DIRECTORY BEFORE READING A BYTE OF IT ──────────────
        // fjall will happily open a directory a live process is already
        // writing, and the result is an unrecoverable store rather than an
        // error (measured 2026-09-01). The apiserver's port bind used to be
        // the de-facto single-instance guard, but it happens AFTER the store
        // is opened and seeded — far too late. Lock first, so a second opener
        // fails with a typed conflict instead of corrupting the first.
        //
        // The lock is `<path>.lock` — derived from the store's OWN path, so its
        // identity is the store's identity. Locking the parent directory
        // instead makes every store under a shared parent contend for one
        // lock, which is a false conflict (caught by this crate's own tests).
        let dir_lock = crate::data_dir_lock::DataDirLock::acquire(&path)
            .map_err(|e| StoreError::Fatal(e.to_string()))?;
        let keyspace = fjall::Config::new(&path)
            .open()
            .map_err(|e| StoreError::Fatal(format!("fjall open {}: {e}", path.display())))?;
        let log = keyspace
            .open_partition(LOG_PARTITION, fjall::PartitionCreateOptions::default())
            .map_err(|e| StoreError::Fatal(format!("open partition {LOG_PARTITION}: {e}")))?;
        let meta = keyspace
            .open_partition(META_PARTITION, fjall::PartitionCreateOptions::default())
            .map_err(|e| StoreError::Fatal(format!("open partition {META_PARTITION}: {e}")))?;
        let catalog = keyspace
            .open_partition(CATALOG_PARTITION, fjall::PartitionCreateOptions::default())
            .map_err(|e| StoreError::Fatal(format!("open partition {CATALOG_PARTITION}: {e}")))?;

        // ── hydrate the in-RAM working copy from disk ──────────────
        let mut state = MaterializedState::default();
        if let Some(bytes) = catalog
            .get(CATALOG_KEY)
            .map_err(|e| StoreError::Fatal(format!("read catalog: {e}")))?
        {
            state.catalog = serde_json::from_slice(&bytes)
                .map_err(|e| StoreError::Fatal(format!("decode catalog: {e}")))?;
        }
        state.vote = meta_get_json(&meta, META_VOTE)?;
        state.committed = meta_get_json(&meta, META_COMMITTED)?;
        state.last_purged = meta_get_json(&meta, META_LAST_PURGED)?;
        state.last_applied = meta_get_json(&meta, META_LAST_APPLIED)?;
        if let Some(m) = meta_get_json::<RMembership>(&meta, META_LAST_MEMBERSHIP)? {
            state.last_membership = m;
        }
        state.snapshot_index = meta_get_json(&meta, META_SNAPSHOT_INDEX)?.unwrap_or(0);

        let inner = Arc::new(FjallInner {
            _dir_lock: dir_lock,
            keyspace,
            log,
            meta,
            catalog,
            state: Mutex::new(state),
        });
        // The ticker holds a Weak<FjallInner> so it never keeps the
        // store alive; it exits when the last store clone drops.
        let ticker = BookmarkTicker::spawn(Arc::downgrade(&inner));
        Ok(Self {
            inner,
            bookmark_ticker: Arc::new(ticker),
        })
    }

    /// `true` if the store already holds Raft state — a vote was
    /// persisted OR a command was applied. On a restart this returns
    /// `true`, so the caller must NOT call `initialize` again (which
    /// errors on an already-initialized cluster).
    ///
    /// # Errors
    ///
    /// This is a pure in-RAM read; it never fails. The `Result` is
    /// kept for symmetry with the openraft storage surface + future
    /// disk-backed variants.
    pub async fn is_initialized(&self) -> bool {
        let s = self.inner.state.lock().await;
        s.vote.is_some() || s.last_applied.is_some()
    }

    /// Read-only snapshot of the current materialized catalog —
    /// the durable + watch-relevant state machine copy.
    pub async fn current_catalog(&self) -> ResourceCatalog {
        self.inner.state.lock().await.catalog.clone()
    }

    /// Stop this store's bookmark ticker and wait until it has ended, so it
    /// holds no reference to the store — see [`BookmarkTicker::quiesce`].
    ///
    /// For this backend that includes the data-directory lock: after
    /// `quiesce_bookmarks`, dropping the last store clone releases the
    /// directory at once, even if the ticker was parked mid-tick. The
    /// ticker is shared by every clone; afterwards no clone's watchers
    /// receive periodic bookmarks (live events still flow). One-way.
    pub async fn quiesce_bookmarks(&self) -> TaskStop {
        self.bookmark_ticker.quiesce().await
    }

    /// The current MVCC revision, read under the lock WITHOUT cloning.
    ///
    /// Durable sibling of [`crate::store::InMemoryStore::current_revision`];
    /// that method's header carries the measurement. Short version: reading
    /// this scalar via `current_catalog()` deep-clones the whole catalog and
    /// its 8192-entry replay ring, which is what made establishing a watch
    /// stall every concurrent write.
    pub async fn current_revision(&self) -> crate::revision::Revision {
        self.inner.state.lock().await.catalog.revision()
    }

    /// List + revision from ONE locked look, cloning only the MATCHED items.
    ///
    /// The durable sibling of [`crate::store::InMemoryStore::list_at_revision`];
    /// that method's header carries the measurement and the reasoning. Short
    /// version: cloning the whole [`ResourceCatalog`] to serve a LIST also
    /// clones its 8192-entry watch-replay ring, making every read cost scale
    /// with cluster age instead of object count.
    pub async fn list_at_revision(
        &self,
        group: &str,
        version: &str,
        kind: &str,
        namespace: Option<&str>,
    ) -> (
        Vec<(crate::resource::ResourceKey, crate::resource::ResourceValue)>,
        crate::revision::Revision,
    ) {
        self.inner
            .state
            .lock()
            .await
            .catalog
            .list_at_revision(crate::resource::ListScope::new(
                group, version, kind, namespace,
            ))
    }

    /// One page + revision from ONE locked look, cloning only the page's
    /// items. Durable sibling of
    /// [`crate::store::InMemoryStore::list_page_at_revision`].
    pub async fn list_page_at_revision(
        &self,
        scope: crate::resource::ListScope<'_>,
        after: Option<&crate::resource::ResourceKey>,
        limit: usize,
    ) -> crate::pagination::PageAtRevision {
        self.inner
            .state
            .lock()
            .await
            .catalog
            .list_page_at_revision(scope, after, limit)
    }

    /// Direct catalog read by typed key (skips Raft — the local
    /// replica has the data once apply has caught up).
    pub async fn get_resource(
        &self,
        key: &crate::resource::ResourceKey,
    ) -> Option<crate::resource::ResourceValue> {
        self.inner.state.lock().await.catalog.get(key).cloned()
    }

    /// Open a resumable, gap-free watch from `opts.from`. Same contract
    /// + same single implementation as
    /// [`crate::store::InMemoryStore::watch_from`] — registration under
    /// the catalog lock makes the replay→live handoff atomic. For Fjall,
    /// live events are only fanned AFTER the apply's fsync returns
    /// (durable-before-observable), preserving "a watcher never sees an
    /// event that didn't survive to disk."
    ///
    /// # Errors
    ///
    /// [`WatchGone::CompactedTooOld`] when `opts.from` is below the
    /// compaction watermark (no channel is allocated).
    pub async fn watch_from(&self, opts: WatchOpts) -> Result<WatchStream, WatchGone> {
        // Registration enqueues the replay IN REVISION ORDER before the
        // handle is reachable by `fan_change` — all under THIS lock, so
        // the replay→live handoff is atomic AND structurally ordered (no
        // feeder task, no second producer racing apply).
        let mut state = self.inner.state.lock().await;
        let replay = state
            .catalog
            .changes_since(opts.from)
            .map_err(WatchGone::from)?;
        let boundary = state.catalog.revision();
        Ok(state.watchers.register_captured(replay, boundary, &opts))
    }

    /// Subscribe to the LIVE-TAIL watch stream (compatibility shim over
    /// [`Self::watch_from`]) — attaches from the current revision
    /// forward with NO replay + NO bookmarks, surfacing overflow as a
    /// typed [`WatchGone::Overflow`] instead of a silent
    /// `broadcast::RecvError::Lagged`.
    ///
    /// # Errors
    ///
    /// Never errors in practice (live-tail resumes from the current
    /// revision); the `Result` matches `watch_from`.
    pub async fn watch_subscribe(&self) -> Result<WatchStream, WatchGone> {
        let from = self.inner.state.lock().await.catalog.revision();
        self.watch_from(WatchOpts::live_tail(from, WATCH_CHANNEL_CAPACITY))
            .await
    }

    /// Active watch subscriber count (live registry size).
    pub async fn watch_subscriber_count(&self) -> usize {
        self.inner.state.lock().await.watchers.len()
    }

    /// Persist (fsync) the keyspace. Helper centralizing the verb so
    /// every durable write goes through one place.
    fn persist(
        &self,
        map: impl Fn(&fjall::Error) -> StorageError<RNodeId>,
    ) -> Result<(), StorageError<RNodeId>> {
        self.inner
            .keyspace
            .persist(fjall::PersistMode::SyncAll)
            .map_err(|e| map(&e))
    }

    /// Durably write a batch of log entries: serialize each, insert
    /// under its big-endian index key, then fsync ONCE. Shared by the
    /// openraft `append` trait method (which acks the flush callback
    /// after this returns) and unit tests (which can't construct the
    /// crate-private `LogFlushed`).
    fn append_entries_durable(
        &self,
        entries: impl IntoIterator<Item = Entry<TypeConfig>>,
    ) -> Result<(), StorageError<RNodeId>> {
        for e in entries {
            let idx = e.log_id.index;
            let bytes = serde_json::to_vec(&e).map_err(|e| write_logs_err(&e))?;
            self.inner
                .log
                .insert(idx.to_be_bytes(), bytes)
                .map_err(|e| write_logs_err(&e))?;
        }
        self.persist(write_logs_err)
    }

    /// Bring the durable image up to the applied state, so the next boot
    /// replays nothing that was applied before this call.
    ///
    /// `apply` batches the image write (see `CATALOG_PERSIST_EVERY`), so
    /// between writes the image lags the applied state by up to 63 apply
    /// calls or 5 s. That is correct — the log holds those entries and a boot
    /// replays them — but it means every stop leaves a replay behind, and a
    /// replay is exactly what makes rolling back across a change to apply
    /// semantics unsafe. A clean stop calls this after the writers are gone
    /// and leaves nothing to replay.
    ///
    /// Writes the same triple `apply` writes, through the same single batch
    /// (`persist_applied_image`), under the same state lock — so it
    /// never interleaves with an apply and never splits the pair.
    ///
    /// Not covered: an apply that runs AFTER this returns. openraft's
    /// state-machine worker is not joined by `Raft::shutdown`, so an entry it
    /// applies late is batched as usual — durable through the log, replayed
    /// on the next boot, never lost.
    ///
    /// # Errors
    ///
    /// A typed [`StorageError`] if the batch cannot be serialized, written or
    /// fsynced. The in-RAM copy stays marked unwritten, so a retry writes
    /// again, and the log still holds every applied entry.
    pub async fn flush(&self) -> Result<Flushed, StorageError<RNodeId>> {
        let mut state = self.inner.state.lock().await;
        if state.applies_since_catalog_persist == 0 {
            return Ok(Flushed::AlreadyDurable {
                last_applied: state.last_applied,
            });
        }
        self.persist_applied_image(&mut state)?;
        Ok(Flushed::Persisted {
            last_applied: state.last_applied,
        })
    }

    /// Write the APPLIED IMAGE — the catalog blob, `last_applied` and
    /// `last_membership`, everything `applied_state` and a reopen answer from
    /// — as ONE atomic fjall batch fsynced with `SyncAll`, then mark the
    /// in-RAM copy written.
    ///
    /// ★ The single apply-side writer of the pair. Separate inserts followed
    /// by one fsync are not atomic: a crash between them can journal the
    /// catalog without `last_applied`, and the next boot would replay entries
    /// onto a catalog that already holds them. One batch makes that split
    /// unrepresentable for `apply` and [`Self::flush`]. (The snapshot paths
    /// still write their own keys; T3.4 moves them onto one batch.)
    ///
    /// The caller holds the state lock and passes the guarded state, so the
    /// image written is exactly the applied state no apply can race.
    fn persist_applied_image(
        &self,
        state: &mut MaterializedState,
    ) -> Result<(), StorageError<RNodeId>> {
        let catalog = serde_json::to_vec(&state.catalog).map_err(|e| write_sm_err(&e))?;
        let last_applied = serde_json::to_vec(&state.last_applied).map_err(|e| write_sm_err(&e))?;
        let last_membership =
            serde_json::to_vec(&state.last_membership).map_err(|e| write_sm_err(&e))?;
        let mut batch = self
            .inner
            .keyspace
            .batch()
            .durability(Some(fjall::PersistMode::SyncAll));
        batch.insert(&self.inner.catalog, CATALOG_KEY, catalog);
        batch.insert(&self.inner.meta, META_LAST_APPLIED, last_applied);
        batch.insert(&self.inner.meta, META_LAST_MEMBERSHIP, last_membership);
        batch.commit().map_err(|e| write_sm_err(&e))?;
        state.applies_since_catalog_persist = 0;
        state.last_catalog_persist = Some(std::time::Instant::now());
        Ok(())
    }
}

/// Read + deserialize a JSON value from the `meta` partition.
fn meta_get_json<T: serde::de::DeserializeOwned>(
    meta: &fjall::PartitionHandle,
    key: &[u8],
) -> Result<Option<T>, StoreError> {
    match meta.get(key).map_err(|e| {
        StoreError::Fatal(format!("read meta {}: {e}", String::from_utf8_lossy(key)))
    })? {
        Some(bytes) => {
            let v = serde_json::from_slice(&bytes).map_err(|e| {
                StoreError::Fatal(format!("decode meta {}: {e}", String::from_utf8_lossy(key)))
            })?;
            Ok(Some(v))
        }
        None => Ok(None),
    }
}

/// Write a JSON value into the `meta` partition (no fsync — caller
/// batches the `persist`).
fn meta_put_json<T: serde::Serialize>(
    meta: &fjall::PartitionHandle,
    key: &[u8],
    value: &T,
    map: impl Fn(&dyn std::error::Error) -> StorageError<RNodeId>,
) -> Result<(), StorageError<RNodeId>> {
    let bytes = serde_json::to_vec(value).map_err(|e| map(&e))?;
    meta.insert(key, bytes).map_err(|e| map(&e))
}

// =================================================================
// RaftLogReader
// =================================================================

impl RaftLogReader<TypeConfig> for FjallStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<RNodeId>> {
        // Translate the u64 range into a byte-key range over the
        // big-endian-encoded log partition.
        let byte_range = u64_range_to_be_bytes(&range);
        let mut entries = Vec::new();
        for kv in self.inner.log.range(byte_range) {
            let (_, v) = kv.map_err(|e| read_logs_err(&e))?;
            let entry: Entry<TypeConfig> =
                serde_json::from_slice(&v).map_err(|e| read_logs_err(&e))?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

// =================================================================
// RaftLogStorage
// =================================================================

impl RaftLogStorage<TypeConfig> for FjallStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<RNodeId>> {
        let last_purged_log_id = { self.inner.state.lock().await.last_purged };
        // Last log id = the entry under the highest key in the log
        // partition, or last_purged if the log is empty.
        let last_in_log = self
            .inner
            .log
            .last_key_value()
            .map_err(|e| read_logs_err(&e))?;
        let last_log_id = match last_in_log {
            Some((_, v)) => {
                let entry: Entry<TypeConfig> =
                    serde_json::from_slice(&v).map_err(|e| read_logs_err(&e))?;
                Some(entry.log_id)
            }
            None => last_purged_log_id,
        };
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<RNodeId>) -> Result<(), StorageError<RNodeId>> {
        meta_put_json(&self.inner.meta, META_VOTE, vote, |e| {
            write_vote_err(&io_to_anyerror_dyn(e))
        })?;
        self.persist(write_vote_err)?;
        self.inner.state.lock().await.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<RNodeId>>, StorageError<RNodeId>> {
        Ok(self.inner.state.lock().await.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<RNodeId>>,
    ) -> Result<(), StorageError<RNodeId>> {
        meta_put_json(&self.inner.meta, META_COMMITTED, &committed, |e| {
            write_sm_err(&io_to_anyerror_dyn(e))
        })?;
        self.persist(write_sm_err)?;
        self.inner.state.lock().await.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<RNodeId>>, StorageError<RNodeId>> {
        Ok(self.inner.state.lock().await.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<RNodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        // REAL durability: serialize + insert + fsync the log BEFORE
        // acking the flush callback.
        self.append_entries_durable(entries)?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<RNodeId>) -> Result<(), StorageError<RNodeId>> {
        // Remove all entries with idx >= log_id.index.
        let from = log_id.index.to_be_bytes();
        let keys: Vec<Vec<u8>> = collect_keys_in_range(&self.inner.log, from.., None)?;
        for k in keys {
            self.inner.log.remove(k).map_err(|e| write_logs_err(&e))?;
        }
        self.persist(write_logs_err)
    }

    async fn purge(&mut self, log_id: LogId<RNodeId>) -> Result<(), StorageError<RNodeId>> {
        // Remove all entries with idx <= log_id.index.
        let keys: Vec<Vec<u8>> = collect_keys_in_range(&self.inner.log, .., Some(log_id.index))?;
        for k in keys {
            self.inner.log.remove(k).map_err(|e| write_logs_err(&e))?;
        }
        meta_put_json(&self.inner.meta, META_LAST_PURGED, &Some(log_id), |e| {
            write_logs_err(&io_to_anyerror_dyn(e))
        })?;
        self.persist(write_logs_err)?;
        self.inner.state.lock().await.last_purged = Some(log_id);
        Ok(())
    }
}

/// Translate a `RangeBounds<u64>` into a `RangeBounds<[u8; 8]>` over
/// big-endian keys. We materialize a concrete `(Bound, Bound)` so
/// the lifetime is owned (fjall's `range` keeps the bounds).
fn u64_range_to_be_bytes<RB: RangeBounds<u64>>(
    range: &RB,
) -> (std::ops::Bound<[u8; 8]>, std::ops::Bound<[u8; 8]>) {
    use std::ops::Bound;
    let lo = match range.start_bound() {
        Bound::Included(n) => Bound::Included(n.to_be_bytes()),
        Bound::Excluded(n) => Bound::Excluded(n.to_be_bytes()),
        Bound::Unbounded => Bound::Unbounded,
    };
    let hi = match range.end_bound() {
        Bound::Included(n) => Bound::Included(n.to_be_bytes()),
        Bound::Excluded(n) => Bound::Excluded(n.to_be_bytes()),
        Bound::Unbounded => Bound::Unbounded,
    };
    (lo, hi)
}

/// Collect the keys of every log entry in `[from..]` (when
/// `up_to_inclusive` is `None`) or with `index <= up_to_inclusive`
/// (when `Some`). Used by truncate/purge which then issue removes.
fn collect_keys_in_range(
    log: &fjall::PartitionHandle,
    from: impl RangeBounds<[u8; 8]>,
    up_to_inclusive: Option<u64>,
) -> Result<Vec<Vec<u8>>, StorageError<RNodeId>> {
    let mut keys = Vec::new();
    match up_to_inclusive {
        None => {
            for kv in log.range(from) {
                let (k, _) = kv.map_err(|e| write_logs_err(&e))?;
                keys.push(k.to_vec());
            }
        }
        Some(limit) => {
            for kv in log.iter() {
                let (k, _) = kv.map_err(|e| write_logs_err(&e))?;
                let idx = be_bytes_to_u64(&k);
                if idx <= limit {
                    keys.push(k.to_vec());
                }
            }
        }
    }
    Ok(keys)
}

/// Decode a big-endian 8-byte log key into its `u64` index.
fn be_bytes_to_u64(bytes: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    let n = bytes.len().min(8);
    buf[8 - n..].copy_from_slice(&bytes[..n]);
    u64::from_be_bytes(buf)
}

/// Bridge a `&dyn std::error::Error` through the `&io::Error`-typed
/// error mappers (which take `impl Error + 'static`).
fn io_to_anyerror_dyn(e: &dyn std::error::Error) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

// =================================================================
// Snapshot builder
// =================================================================

#[derive(Clone)]
pub struct FjallSnapshotBuilder {
    store: FjallStore,
}

impl RaftSnapshotBuilder<TypeConfig> for FjallSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<RNodeId>> {
        let mut state = self.store.inner.state.lock().await;
        let last_applied = state.last_applied;
        let last_membership = state.last_membership.clone();
        // Byte-identical to InMemorySnapshotBuilder: serde_json over
        // the materialized catalog (carries the full revision state).
        let catalog_bytes =
            serde_json::to_vec(&state.catalog).map_err(|e| write_snapshot_err(None, &e))?;
        state.snapshot_index += 1;
        let snapshot_index = state.snapshot_index;
        let snapshot_id = format!("snap-{snapshot_index}");

        // Persist snapshot bytes + meta + bumped index so
        // get_current_snapshot can serve it after a restart.
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id,
        };
        meta_put_json(
            &self.store.inner.meta,
            META_SNAPSHOT_INDEX,
            &snapshot_index,
            |e| write_snapshot_err(None, &io_to_anyerror_dyn(e)),
        )?;
        meta_put_json(&self.store.inner.meta, META_SNAPSHOT_META, &meta, |e| {
            write_snapshot_err(None, &io_to_anyerror_dyn(e))
        })?;
        self.store
            .inner
            .meta
            .insert(META_SNAPSHOT_DATA, catalog_bytes.clone())
            .map_err(|e| write_snapshot_err(None, &e))?;
        self.store.persist(|e| write_snapshot_err(None, e))?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(catalog_bytes)),
        })
    }
}

// =================================================================
// RaftStateMachine
// =================================================================

impl RaftStateMachine<TypeConfig> for FjallStore {
    type SnapshotBuilder = FjallSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<RNodeId>>, RMembership), StorageError<RNodeId>> {
        let state = self.inner.state.lock().await;
        Ok((state.last_applied, state.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ApplyResult>, StorageError<RNodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut state = self.inner.state.lock().await;
        let mut results = Vec::new();
        // Buffer the committed changes; we fan them to watchers AFTER
        // the fsync (durable-before-observable) but BEFORE dropping the
        // lock (so the replay→live handoff stays atomic).
        let mut committed: Vec<crate::revision::Change> = Vec::new();
        // Track whether last_membership changed so we persist it.
        let mut membership_changed = false;

        for entry in entries {
            let log_id = entry.log_id;
            let (op, patch_error) = match entry.payload {
                EntryPayload::Blank => (crate::command::ResourceOp::NoOp, None),
                EntryPayload::Normal(ref cmd) => {
                    let outcome = state
                        .catalog
                        .apply(cmd, log_id.leader_id.term, log_id.index);
                    let op = outcome.op;
                    let patch_error = outcome.patch_error.clone();
                    if let Some(change) = outcome.change {
                        committed.push(change);
                    }
                    (op, patch_error)
                }
                EntryPayload::Membership(m) => {
                    state.last_membership = StoredMembership::new(Some(log_id), m);
                    membership_changed = true;
                    (crate::command::ResourceOp::NoOp, None)
                }
            };
            state.last_applied = Some(log_id);
            results.push(ApplyResult {
                applied_index: log_id.index,
                applied_term: log_id.leader_id.term,
                op,
                revision: state.catalog.revision().get(),
                patch_error,
            });
        }

        // ── durable write ──────────────────────────────────────────
        // The catalog blob is a CACHE of applied state, not the durability
        // source: every entry was already fsynced into the `log` partition
        // BEFORE it reached this function, so a crash replays rather than
        // loses. Rewriting the whole blob per apply is what made writes take
        // seconds — see CATALOG_PERSIST_EVERY for the measurement and why
        // replay is safe.
        //
        // ★ The catalog and `last_applied` move TOGETHER. Advancing the
        // persisted `last_applied` past the persisted catalog is the one way
        // to actually lose data here, so the only write is
        // `persist_applied_image`, which lands both in one atomic batch.
        // A membership change always forces a write — raft's own consistency
        // rests on it, and it is far too rare to be worth batching.
        state.applies_since_catalog_persist += 1;
        let due_by_count = state.applies_since_catalog_persist >= CATALOG_PERSIST_EVERY;
        let due_by_time = state
            .last_catalog_persist
            .is_none_or(|t| t.elapsed() >= CATALOG_PERSIST_INTERVAL);
        if membership_changed || due_by_count || due_by_time {
            self.persist_applied_image(&mut state)?;
        }

        // ── fan watch events: DURABLE-BEFORE-OBSERVABLE, UNDER THE LOCK ──
        // "A watcher never sees an event that didn't survive to disk" still
        // holds, and it is worth being explicit about WHY now that the
        // catalog write above is batched and may not have run on this pass:
        // the guarantee rests on the LOG, not on the catalog blob. Every
        // entry was serialized and fsynced into the `log` partition before
        // `apply` was ever called, so a change a watcher observes is already
        // durable — a crash replays it rather than losing it. Batching the
        // catalog cache does not weaken the promise; it only changes which
        // fsync backs it.
        //
        // Still-under-lock = the replay→live handoff stays atomic against a
        // concurrent `watch_from` registration (which also runs under this
        // same `state` lock). Non-blocking try_send (overflow → typed Gone,
        // never silent, never blocks).
        for change in &committed {
            state.watchers.fan_change(change);
        }
        drop(state);
        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        FjallSnapshotBuilder {
            store: self.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<RNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<RNodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<RNodeId>> {
        let bytes = snapshot.into_inner();
        let catalog: ResourceCatalog = serde_json::from_slice(&bytes)
            .map_err(|e| read_snapshot_err(Some(meta.signature()), &e))?;

        // Overwrite catalog partition + in-RAM copy + applied/membership.
        self.inner
            .catalog
            .insert(CATALOG_KEY, bytes.clone())
            .map_err(|e| write_snapshot_err(Some(meta.signature()), &e))?;
        meta_put_json(
            &self.inner.meta,
            META_LAST_APPLIED,
            &meta.last_log_id,
            |e| write_snapshot_err(Some(meta.signature()), &io_to_anyerror_dyn(e)),
        )?;
        meta_put_json(
            &self.inner.meta,
            META_LAST_MEMBERSHIP,
            &meta.last_membership,
            |e| write_snapshot_err(Some(meta.signature()), &io_to_anyerror_dyn(e)),
        )?;
        // Keep the snapshot bytes + meta so get_current_snapshot
        // serves it after restart.
        self.inner
            .meta
            .insert(META_SNAPSHOT_DATA, bytes)
            .map_err(|e| write_snapshot_err(Some(meta.signature()), &e))?;
        meta_put_json(&self.inner.meta, META_SNAPSHOT_META, meta, |e| {
            write_snapshot_err(Some(meta.signature()), &io_to_anyerror_dyn(e))
        })?;
        self.persist(|e| write_snapshot_err(Some(meta.signature()), e))?;

        let mut state = self.inner.state.lock().await;
        state.catalog = catalog;
        state.last_applied = meta.last_log_id;
        state.last_membership = meta.last_membership.clone();
        // The image just written IS the applied state now, so nothing is
        // pending for `flush` or the cadence in `apply`.
        state.applies_since_catalog_persist = 0;
        state.last_catalog_persist = Some(std::time::Instant::now());
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<RNodeId>> {
        let meta: Option<SnapshotMeta<RNodeId, openraft::BasicNode>> =
            meta_get_json(&self.inner.meta, META_SNAPSHOT_META)
                .map_err(|e| read_snapshot_err(None, &io_to_anyerror(&e)))?;
        let Some(meta) = meta else {
            return Ok(None);
        };
        let data = self
            .inner
            .meta
            .get(META_SNAPSHOT_DATA)
            .map_err(|e| read_snapshot_err(Some(meta.signature()), &e))?;
        let Some(data) = data else {
            return Ok(None);
        };
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data.to_vec())),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::{Reason, ResourceCommand};
    use crate::resource::ResourceKey;
    use crate::revision::{Revision, VersionMeta};
    use openraft::{CommittedLeaderId, EntryPayload, LogId};

    fn temp_dir(suffix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("engenho-fjall-{}-{suffix}", std::process::id()))
    }

    fn put_entry(idx: u64, name: &str) -> Entry<TypeConfig> {
        let cmd = ResourceCommand::Put {
            key: ResourceKey::namespaced("", "v1", "Pod", "default", name),
            value: serde_json::json!({"spec": {"image": "v1"}}),
            expected: None,
            reason: Reason::Operator,
        };
        Entry {
            log_id: LogId {
                leader_id: CommittedLeaderId::new(1, 0),
                index: idx,
            },
            payload: EntryPayload::Normal(cmd),
        }
    }

    #[tokio::test]
    async fn empty_store_reports_no_log_state_and_not_initialized() {
        let dir = temp_dir("empty");
        let _ = std::fs::remove_dir_all(&dir);
        let mut s = FjallStore::open(&dir).unwrap();
        let state = s.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());
        assert!(!s.is_initialized().await);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn vote_round_trips_across_reopen() {
        let dir = temp_dir("vote-reopen");
        let _ = std::fs::remove_dir_all(&dir);
        let vote = Vote::new(1, 42);
        {
            let mut s = FjallStore::open(&dir).unwrap();
            assert!(s.read_vote().await.unwrap().is_none());
            s.save_vote(&vote).await.unwrap();
            assert_eq!(s.read_vote().await.unwrap(), Some(vote));
            assert!(s.is_initialized().await);
        }
        // Drop + reopen the same dir — vote must survive.
        let mut s2 = FjallStore::open(&dir).unwrap();
        assert_eq!(s2.read_vote().await.unwrap(), Some(vote));
        assert!(s2.is_initialized().await);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn committed_round_trips_across_reopen() {
        let dir = temp_dir("committed-reopen");
        let _ = std::fs::remove_dir_all(&dir);
        let committed = LogId {
            leader_id: CommittedLeaderId::new(3, 0),
            index: 9,
        };
        {
            let mut s = FjallStore::open(&dir).unwrap();
            s.save_committed(Some(committed)).await.unwrap();
            assert_eq!(s.read_committed().await.unwrap(), Some(committed));
        }
        let mut s2 = FjallStore::open(&dir).unwrap();
        assert_eq!(s2.read_committed().await.unwrap(), Some(committed));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn log_append_persist_then_reopen_reads_back() {
        let dir = temp_dir("log-reopen");
        let _ = std::fs::remove_dir_all(&dir);
        {
            let s = FjallStore::open(&dir).unwrap();
            let entries = vec![put_entry(1, "a"), put_entry(2, "b"), put_entry(3, "c")];
            s.append_entries_durable(entries).unwrap();
        }
        let mut s2 = FjallStore::open(&dir).unwrap();
        let read = s2.try_get_log_entries(..).await.unwrap();
        assert_eq!(read.len(), 3);
        assert_eq!(read[0].log_id.index, 1);
        assert_eq!(read[2].log_id.index, 3);
        // get_log_state sees the last log id.
        let state = s2.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.map(|l| l.index), Some(3));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn truncate_then_reopen() {
        let dir = temp_dir("truncate-reopen");
        let _ = std::fs::remove_dir_all(&dir);
        {
            let mut s = FjallStore::open(&dir).unwrap();
            let entries = vec![put_entry(1, "a"), put_entry(2, "b"), put_entry(3, "c")];
            s.append_entries_durable(entries).unwrap();
            // Truncate from index 2 → keep only index 1.
            s.truncate(LogId {
                leader_id: CommittedLeaderId::new(1, 0),
                index: 2,
            })
            .await
            .unwrap();
        }
        let mut s2 = FjallStore::open(&dir).unwrap();
        let read = s2.try_get_log_entries(..).await.unwrap();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].log_id.index, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn purge_then_reopen() {
        let dir = temp_dir("purge-reopen");
        let _ = std::fs::remove_dir_all(&dir);
        {
            let mut s = FjallStore::open(&dir).unwrap();
            let entries = vec![put_entry(1, "a"), put_entry(2, "b"), put_entry(3, "c")];
            s.append_entries_durable(entries).unwrap();
            // Purge up to and including index 2 → keep only index 3.
            s.purge(LogId {
                leader_id: CommittedLeaderId::new(1, 0),
                index: 2,
            })
            .await
            .unwrap();
        }
        let mut s2 = FjallStore::open(&dir).unwrap();
        let read = s2.try_get_log_entries(..).await.unwrap();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].log_id.index, 3);
        // last_purged survives → get_log_state sees it when log empty.
        let state = s2.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.map(|l| l.index), Some(2));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// ★ THE INVARIANT THE BATCHED CATALOG WRITE RESTS ON.
    ///
    /// The catalog blob and `last_applied` are written together or not at
    /// all, so after a reopen the applied position INSIDE the rehydrated
    /// catalog must equal the separately-persisted `last_applied`. If
    /// `last_applied` were ever ahead, a restart would trust a catalog that
    /// is behind it and openraft would never replay the difference — the one
    /// way this optimization could actually lose data.
    ///
    /// Applies several entries in quick succession, so entries after the
    /// first are deliberately NOT persisted (the count and time bounds are
    /// both far away). The assertion is not "everything survived" — it is
    /// "whatever survived is CONSISTENT", which is what makes the replay
    /// openraft performs afterwards correct.
    #[tokio::test]
    async fn a_reopened_catalog_is_never_behind_its_own_applied_position() {
        let dir = temp_dir("apply-batched-consistency");
        let _ = std::fs::remove_dir_all(&dir);
        {
            let mut s = FjallStore::open(&dir).unwrap();
            // Several applies well inside both the count and time bounds, so
            // most of them skip the blob write.
            for i in 2..8u64 {
                s.apply(vec![put_entry(i, &format!("pod-{i}"))])
                    .await
                    .unwrap();
            }
        }
        let s2 = FjallStore::open(&dir).unwrap();
        let cat = s2.current_catalog().await;
        let persisted_applied = s2.inner.state.lock().await.last_applied;

        assert_eq!(
            cat.last_applied_index,
            persisted_applied.map_or(0, |l| l.index),
            "the rehydrated catalog and the persisted last_applied disagree;              a restart would trust a catalog behind its own applied position"
        );
        // ★ NEGATIVE CONTROL — the test is only meaningful if writes were
        // actually SKIPPED. Six applies inside both bounds means only the
        // first persists (the time bound fires once, on `None`), so a
        // reopened catalog carrying all six would mean the batching never
        // engaged and the consistency assertion above proved nothing.
        assert!(
            cat.len() < 6,
            "batching did not engage — all {} applies persisted, so the \
             consistency assertion above is vacuous",
            cat.len()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn log_id(idx: u64) -> LogId<RNodeId> {
        LogId {
            leader_id: CommittedLeaderId::new(1, 0),
            index: idx,
        }
    }

    /// Log and apply `n` Puts at indexes `1..=n`, one apply each — the shape
    /// of `n` writes arriving inside one persist window.
    async fn log_and_apply(s: &mut FjallStore, n: usize) {
        for i in 1..=n {
            let idx = u64::try_from(i).unwrap();
            let e = put_entry(idx, &format!("pod-{i}"));
            s.append_entries_durable(vec![e.clone()]).unwrap();
            s.apply(vec![e]).await.unwrap();
        }
    }

    /// Reopen `dir` raw and report `(entries a boot would replay, objects in
    /// the rehydrated catalog)`. A boot replays the log entries after the
    /// persisted `last_applied` (up to `committed`, which never passes the
    /// last entry), so the first number bounds the replay from above.
    async fn replay_left_by(dir: &std::path::Path) -> (usize, usize) {
        let mut s = FjallStore::open(dir).unwrap();
        let (last_applied, _) = s.applied_state().await.unwrap();
        let from = last_applied.map_or(0, |l| l.index + 1);
        let pending = s.try_get_log_entries(from..).await.unwrap().len();
        (pending, s.current_catalog().await.len())
    }

    /// T2.9-store: `flush`, then the process goes away WITHOUT `terminate`.
    /// The reopened store has nothing to replay and every applied object.
    #[tokio::test]
    async fn flush_then_drop_without_terminate_leaves_nothing_to_replay() {
        const N: usize = 6;
        let tmp = tempfile::tempdir().unwrap();

        // ★ NEGATIVE CONTROL: the same writes, dropped with no flush, leave a
        // replay behind. Without it, a green result below could mean the
        // cadence happened to persist everything and `flush` did nothing.
        let control = tmp.path().join("control");
        {
            let mut s = FjallStore::open(&control).unwrap();
            log_and_apply(&mut s, N).await;
        }
        let (control_pending, _) = replay_left_by(&control).await;
        assert!(
            control_pending > 0,
            "HARNESS PRECONDITION: without a flush the durable image must lag the log, \
             or this test proves nothing"
        );

        let dir = tmp.path().join("flushed");
        {
            let mut s = FjallStore::open(&dir).unwrap();
            log_and_apply(&mut s, N).await;
            assert_eq!(
                s.flush().await.unwrap(),
                Flushed::Persisted {
                    last_applied: Some(log_id(u64::try_from(N).unwrap())),
                },
                "flush must report that it wrote the image the cadence had left behind"
            );
            // Dropped here: no terminate.
        }
        let (pending, objects) = replay_left_by(&dir).await;
        assert_eq!(
            pending, 0,
            "a flushed store left {pending} log entries for the next boot to replay"
        );
        assert_eq!(objects, N, "the flushed image is missing applied objects");
    }

    /// `flush` writes only when something was applied since the image was
    /// last written or loaded, and always names the position the image stands
    /// at: a fresh store, a second flush in a row and a reopened store all
    /// answer `AlreadyDurable`.
    #[tokio::test]
    async fn flush_is_idempotent_and_names_the_position_it_left() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("store");
        let mut s = FjallStore::open(&dir).unwrap();
        assert_eq!(
            s.flush().await.unwrap(),
            Flushed::AlreadyDurable { last_applied: None }
        );

        log_and_apply(&mut s, 3).await;
        let (Flushed::Persisted { last_applied } | Flushed::AlreadyDurable { last_applied }) =
            s.flush().await.unwrap();
        assert_eq!(last_applied, Some(log_id(3)));
        assert_eq!(
            s.flush().await.unwrap(),
            Flushed::AlreadyDurable {
                last_applied: Some(log_id(3))
            },
            "a second flush with no apply in between must write nothing"
        );
        drop(s);

        let reopened = FjallStore::open(&dir).unwrap();
        assert_eq!(
            reopened.flush().await.unwrap(),
            Flushed::AlreadyDurable {
                last_applied: Some(log_id(3))
            },
            "a reopened store holds exactly the image it loaded"
        );
    }

    /// T3.3: a reopened store holds no history, so every watch resume point
    /// below the revision it loaded is refused with the 410 a client relists
    /// on. Resuming from exactly the loaded revision is honoured.
    #[tokio::test]
    async fn a_reopened_store_refuses_a_watch_below_the_revision_it_loaded() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("store");
        let mut s = FjallStore::open(&dir).unwrap();
        log_and_apply(&mut s, 3).await;
        assert!(
            s.watch_from(WatchOpts::live_tail(Revision::ZERO, 16))
                .await
                .is_ok(),
            "before the restart the ring backs every revision"
        );
        let (Flushed::Persisted { last_applied } | Flushed::AlreadyDurable { last_applied }) =
            s.flush().await.unwrap();
        assert_eq!(last_applied, Some(log_id(3)), "the image holds all three");
        drop(s);

        let reopened = FjallStore::open(&dir).unwrap();
        let head = reopened.current_revision().await;
        assert_eq!(
            head,
            Revision(3),
            "the flushed image holds all three writes"
        );
        for from in 0..head.get() {
            assert_eq!(
                reopened
                    .watch_from(WatchOpts::live_tail(Revision(from), 16))
                    .await
                    .err(),
                Some(WatchGone::CompactedTooOld {
                    requested: Revision(from),
                    compacted: head,
                }),
                "watch_from({from}) after a reopen must be refused: the ring did not survive"
            );
        }
        assert!(
            reopened
                .watch_from(WatchOpts::live_tail(head, 16))
                .await
                .is_ok(),
            "resuming from exactly the loaded revision is honoured"
        );
    }

    /// An installed snapshot IS the durable image, so `flush` afterwards has
    /// nothing to write — even when the store had unwritten applies before.
    #[tokio::test]
    async fn install_snapshot_leaves_nothing_for_flush_to_write() {
        let tmp = tempfile::tempdir().unwrap();
        let mut src = FjallStore::open(tmp.path().join("src")).unwrap();
        log_and_apply(&mut src, 4).await;
        let snap = src
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();

        let mut dst = FjallStore::open(tmp.path().join("dst")).unwrap();
        log_and_apply(&mut dst, 2).await;
        dst.install_snapshot(&snap.meta, snap.snapshot)
            .await
            .unwrap();
        assert_eq!(
            dst.flush().await.unwrap(),
            Flushed::AlreadyDurable {
                last_applied: snap.meta.last_log_id
            }
        );
    }

    #[tokio::test]
    async fn apply_then_reopen_rehydrates_catalog_with_revision() {
        let dir = temp_dir("apply-reopen");
        let _ = std::fs::remove_dir_all(&dir);
        let key = ResourceKey::namespaced("", "v1", "Pod", "default", "podinfo");
        {
            let mut s = FjallStore::open(&dir).unwrap();
            let res = s.apply(vec![put_entry(2, "podinfo")]).await.unwrap();
            assert_eq!(res.len(), 1);
            assert_eq!(res[0].op, crate::command::ResourceOp::Created);
            assert_eq!(res[0].revision, 1);
        }
        let s2 = FjallStore::open(&dir).unwrap();
        let cat = s2.current_catalog().await;
        assert_eq!(cat.len(), 1);
        assert!(cat.get(&key).is_some());
        assert_eq!(cat.current_revision, Revision(1));
        let (_, meta) = cat.get_with_meta(&key).unwrap();
        assert_eq!(meta, VersionMeta::created_at(Revision(1)));
        // Applied position survived → store is initialized on reopen.
        assert!(s2.is_initialized().await);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn snapshot_build_install_round_trip_preserves_revision_and_floors_history_at_it() {
        let dir_a = temp_dir("snap-src");
        let dir_b = temp_dir("snap-dst");
        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);

        // Source store: apply a few puts so revision advances.
        let mut src = FjallStore::open(&dir_a).unwrap();
        for i in 2..=5u64 {
            src.apply(vec![put_entry(i, &format!("p{i}"))])
                .await
                .unwrap();
        }
        let src_cat = src.current_catalog().await;
        let src_rev = src_cat.current_revision;
        assert!(src_rev.get() >= 4);

        // Build snapshot from src.
        let mut builder = src.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();
        let snap_bytes = snap.snapshot.get_ref().clone();
        let snap_meta = snap.meta.clone();

        // Install into a fresh destination store.
        let mut dst = FjallStore::open(&dir_b).unwrap();
        dst.install_snapshot(&snap_meta, Box::new(Cursor::new(snap_bytes)))
            .await
            .unwrap();
        let dst_cat = dst.current_catalog().await;
        assert_eq!(dst_cat.current_revision, src_rev);
        // T3.3: the source evicted nothing, so its floor is 0 and its ring
        // backs every revision. A snapshot carries no ring, so the
        // destination's floor is the installed revision, and a watch below
        // it is a 410 rather than an empty replay.
        assert_eq!(src_cat.compacted_revision, Revision::ZERO);
        assert_eq!(dst_cat.compacted_revision, src_rev);
        assert_eq!(
            dst.watch_from(WatchOpts::live_tail(Revision::ZERO, 16))
                .await
                .err(),
            Some(WatchGone::CompactedTooOld {
                requested: Revision::ZERO,
                compacted: src_rev,
            }),
            "a watch from before the installed snapshot must be refused, not answered empty"
        );
        // Per-key VersionMeta matches.
        let k = ResourceKey::namespaced("", "v1", "Pod", "default", "p5");
        assert_eq!(
            dst_cat.get_with_meta(&k).map(|(_, m)| m),
            src_cat.get_with_meta(&k).map(|(_, m)| m)
        );

        // get_current_snapshot on dst serves the installed snapshot.
        let current = dst.get_current_snapshot().await.unwrap();
        assert!(current.is_some());

        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
    }

    /// T2.1-store: a ticker parked mid-tick holds an upgraded
    /// `Arc<FjallInner>`, and with it the data-directory lock. After
    /// `quiesce_bookmarks`, the store is held by its owner alone, so
    /// dropping the store frees the directory at once and it reopens.
    #[tokio::test]
    async fn quiesce_bookmarks_frees_the_data_dir_even_when_the_ticker_is_mid_tick() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let store = FjallStore::open(&path).unwrap();

        let apply_in_progress = store.inner.state.lock().await;
        assert!(
            crate::watch_backend::await_strong_count(&store.inner, 2).await,
            "precondition: the ticker upgraded its Weak and is parked on the state lock"
        );

        assert_eq!(store.quiesce_bookmarks().await, TaskStop::Cancelled);
        assert_eq!(
            Arc::strong_count(&store.inner),
            1,
            "quiesce_bookmarks returned while the ticker still held FjallInner"
        );

        drop(apply_in_progress);
        drop(store);
        let reopened = FjallStore::open(&path);
        assert!(
            reopened.is_ok(),
            "the data dir must be free the moment the last store handle drops: {:?}",
            reopened.err()
        );
    }
}
