//! In-memory `RaftLogStorage` + `RaftStateMachine` for the
//! engenho-store Raft group.
//!
//! Same shape as engenho-revoada's InMemoryStore — sibling impl
//! that will be unified at R6.5 into a shared
//! `pleme-io/openraft-mem` crate once a third Raft-using site
//! emerges. For now, we copy + adapt to engenho-store's state
//! machine (ResourceCatalog) so engenho-store ships unblocked.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, RaftLogReader, RaftSnapshotBuilder, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership, Vote,
};
use tokio::sync::Mutex;

use crate::state::ResourceCatalog;
use crate::type_config::{ApplyResult, RaftNodeId, TypeConfig};
use crate::watch_backend::{
    BookmarkTickable, BookmarkTicker, WatchGone, WatchOpts, WatchStream, WatcherRegistry,
};

/// Default per-watcher buffer — same capacity the legacy broadcast
/// path used.
const WATCH_CHANNEL_CAPACITY: usize = crate::watch_backend::WATCH_CHANNEL_CAPACITY;

#[derive(Clone)]
pub struct InMemoryStore {
    inner: Arc<Mutex<Inner>>,
    /// Store-level bookmark ticker — one per store, driving the
    /// per-watcher bookmark cadence under the catalog lock. `Arc` so
    /// every clone of the store shares (and keeps alive) the single
    /// ticker; dropping the last store clone aborts it.
    _bookmark_ticker: Arc<BookmarkTicker>,
}

impl Default for InMemoryStore {
    fn default() -> Self {
        let inner = Arc::new(Mutex::new(Inner::default()));
        // The ticker holds a Weak<Mutex<Inner>> so it never keeps the
        // store alive; it exits when the last store clone drops.
        let ticker = BookmarkTicker::spawn(Arc::downgrade(&inner));
        Self {
            inner,
            _bookmark_ticker: Arc::new(ticker),
        }
    }
}

#[derive(Default)]
struct Inner {
    vote: Option<Vote<RaftNodeId>>,
    committed: Option<LogId<RaftNodeId>>,
    log: BTreeMap<u64, Entry<TypeConfig>>,
    last_purged: Option<LogId<RaftNodeId>>,
    catalog: ResourceCatalog,
    last_applied: Option<LogId<RaftNodeId>>,
    last_membership: StoredMembership<RaftNodeId, openraft::BasicNode>,
    snapshot: Option<Snapshot<TypeConfig>>,
    snapshot_index: u64,
    /// Live watch registry — lives UNDER this catalog lock so
    /// subscription + replay-capture + live fan-out are atomic against
    /// `catalog.apply` (the crux of the gap-free / dup-free handoff).
    watchers: WatcherRegistry,
}

/// The ticker drives bookmarks under the SAME `Mutex<Inner>` the apply
/// path holds — so a bookmark + a concurrent apply can't interleave on
/// the registry. The trait is local, so this impl on the foreign
/// `tokio::sync::Mutex` is allowed by the orphan rule.
impl BookmarkTickable for Mutex<Inner> {
    async fn tick_once(&self) {
        let mut guard = self.lock().await;
        let rev = guard.catalog.revision();
        let now = tokio::time::Instant::now();
        guard.watchers.tick_bookmarks(rev, now);
    }
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Open a resumable, gap-free watch from `opts.from`. See
    /// [`crate::watch_backend`] for the full contract.
    ///
    /// The subscription registers UNDER the catalog lock, so the replay
    /// snapshot + the live-tail attachment are one atomic act — no
    /// change committed during subscription is missed or doubled.
    ///
    /// # Errors
    ///
    /// [`WatchGone::CompactedTooOld`] when `opts.from` is below the
    /// catalog's compaction watermark (no channel is allocated).
    pub async fn watch_from(&self, opts: WatchOpts) -> Result<WatchStream, WatchGone> {
        // ── under the catalog lock ─────────────────────────────────
        let mut guard = self.inner.lock().await;
        // Read replay + boundary from the catalog (immutable), then
        // register the sender on the registry (mutable). `register_captured`
        // enqueues the entire replay into the channel IN REVISION ORDER
        // before the handle is reachable by `fan_change` — all under THIS
        // lock, so the replay→live handoff is atomic AND structurally
        // ordered (no feeder task, no second producer racing apply).
        // Borrows are sequential (no split-borrow of the MutexGuard's
        // Deref) and the catalog ring is NOT cloned.
        let replay = guard
            .catalog
            .changes_since(opts.from)
            .map_err(WatchGone::from)?;
        let boundary = guard.catalog.revision();
        Ok(guard.watchers.register_captured(replay, boundary, &opts))
    }

    pub async fn current_catalog(&self) -> ResourceCatalog {
        self.inner.lock().await.catalog.clone()
    }

    /// Direct read — used by RaftStore consumers without going
    /// through Raft (read-after-write callers use `applied_index`).
    pub async fn get_resource(
        &self,
        key: &crate::resource::ResourceKey,
    ) -> Option<crate::resource::ResourceValue> {
        self.inner.lock().await.catalog.get(key).cloned()
    }

    /// Subscribe to the LIVE-TAIL watch stream (the compatibility shim
    /// over [`Self::watch_from`]). Attaches from the current revision
    /// forward with NO replay + NO bookmarks — exactly the legacy
    /// `watch_subscribe` semantics, but as a typed [`WatchStream`] that
    /// surfaces overflow as [`WatchGone::Overflow`] instead of a silent
    /// `broadcast::RecvError::Lagged`.
    ///
    /// For resumable, gap-free watches use [`Self::watch_from`].
    ///
    /// # Errors
    ///
    /// Never errors in practice — live-tail resumes from the current
    /// revision, which is never below the compaction watermark. The
    /// `Result` matches `watch_from`'s signature.
    pub async fn watch_subscribe(&self) -> Result<WatchStream, WatchGone> {
        let from = self.inner.lock().await.catalog.revision();
        self.watch_from(WatchOpts::live_tail(from, WATCH_CHANNEL_CAPACITY))
            .await
    }

    /// Active watch subscriber count (live registry size). Useful for
    /// telemetry + test instrumentation.
    pub async fn watch_subscriber_count(&self) -> usize {
        self.inner.lock().await.watchers.len()
    }
}

// =================================================================
// RaftLogReader
// =================================================================

impl RaftLogReader<TypeConfig> for InMemoryStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<RaftNodeId>> {
        let guard = self.inner.lock().await;
        let entries: Vec<Entry<TypeConfig>> =
            guard.log.range(range).map(|(_, e)| e.clone()).collect();
        Ok(entries)
    }
}

// =================================================================
// RaftLogStorage
// =================================================================

impl RaftLogStorage<TypeConfig> for InMemoryStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<RaftNodeId>> {
        let guard = self.inner.lock().await;
        let last_purged_log_id = guard.last_purged;
        let last_log_id = guard
            .log
            .iter()
            .next_back()
            .map(|(_, e)| e.log_id)
            .or(last_purged_log_id);
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<RaftNodeId>) -> Result<(), StorageError<RaftNodeId>> {
        self.inner.lock().await.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<RaftNodeId>>, StorageError<RaftNodeId>> {
        Ok(self.inner.lock().await.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<RaftNodeId>>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        self.inner.lock().await.committed = committed;
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<RaftNodeId>>, StorageError<RaftNodeId>> {
        Ok(self.inner.lock().await.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<RaftNodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut guard = self.inner.lock().await;
        for e in entries {
            let idx = e.log_id.index;
            guard.log.insert(idx, e);
        }
        drop(guard);
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(
        &mut self,
        log_id: LogId<RaftNodeId>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        let mut guard = self.inner.lock().await;
        guard.log.retain(|&idx, _| idx < log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<RaftNodeId>) -> Result<(), StorageError<RaftNodeId>> {
        let mut guard = self.inner.lock().await;
        guard.last_purged = Some(log_id);
        guard.log.retain(|&idx, _| idx > log_id.index);
        Ok(())
    }
}

// =================================================================
// Snapshot builder
// =================================================================

#[derive(Clone)]
pub struct InMemorySnapshotBuilder {
    store: InMemoryStore,
}

impl RaftSnapshotBuilder<TypeConfig> for InMemorySnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<RaftNodeId>> {
        let mut guard = self.store.inner.lock().await;
        let last_applied = guard.last_applied;
        let last_membership = guard.last_membership.clone();
        let catalog_bytes = serde_json::to_vec(&guard.catalog).map_err(|e| StorageError::IO {
            source: StorageIOError::read_snapshot(None, &e),
        })?;
        guard.snapshot_index += 1;
        let snapshot_id = format!("snap-{}", guard.snapshot_index);
        let snapshot = Snapshot {
            meta: SnapshotMeta {
                last_log_id: last_applied,
                last_membership,
                snapshot_id,
            },
            snapshot: Box::new(Cursor::new(catalog_bytes)),
        };
        guard.snapshot = Some(clone_snapshot(&snapshot));
        Ok(snapshot)
    }
}

fn clone_snapshot(s: &Snapshot<TypeConfig>) -> Snapshot<TypeConfig> {
    let buf = s.snapshot.get_ref().clone();
    Snapshot {
        meta: s.meta.clone(),
        snapshot: Box::new(Cursor::new(buf)),
    }
}

// =================================================================
// RaftStateMachine
// =================================================================

impl RaftStateMachine<TypeConfig> for InMemoryStore {
    type SnapshotBuilder = InMemorySnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<RaftNodeId>>,
            StoredMembership<RaftNodeId, openraft::BasicNode>,
        ),
        StorageError<RaftNodeId>,
    > {
        let guard = self.inner.lock().await;
        Ok((guard.last_applied, guard.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ApplyResult>, StorageError<RaftNodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut guard = self.inner.lock().await;
        let mut results = Vec::new();
        for entry in entries {
            let log_id = entry.log_id;
            let op = match entry.payload {
                EntryPayload::Blank => crate::command::ResourceOp::NoOp,
                EntryPayload::Normal(ref cmd) => {
                    let outcome = guard
                        .catalog
                        .apply(cmd, log_id.leader_id.term, log_id.index);
                    // Fan the committed change to live watchers WHILE
                    // STILL HOLDING the catalog lock — this closes the
                    // replay→live race window entirely (the legacy
                    // post-drop broadcast left a gap/dup window). The
                    // catalog returns the committed Change, so the
                    // Deleted event carries the REAL prior object (the
                    // tombstone), never Null. `fan_change` derives
                    // Added vs Modified from `change.prior` + uses
                    // non-blocking try_send (overflow → typed Gone,
                    // never a silent drop, never a block on a slow
                    // consumer).
                    if let Some(change) = outcome.change {
                        guard.watchers.fan_change(&change);
                    }
                    outcome.op
                }
                EntryPayload::Membership(m) => {
                    guard.last_membership = StoredMembership::new(Some(log_id), m);
                    crate::command::ResourceOp::NoOp
                }
            };
            guard.last_applied = Some(log_id);
            results.push(ApplyResult {
                applied_index: log_id.index,
                applied_term: log_id.leader_id.term,
                op,
                revision: guard.catalog.revision().get(),
            });
        }
        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        InMemorySnapshotBuilder {
            store: self.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<RaftNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<RaftNodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        let bytes = snapshot.into_inner();
        let catalog: ResourceCatalog =
            serde_json::from_slice(&bytes).map_err(|e| StorageError::IO {
                source: StorageIOError::read_snapshot(Some(meta.signature()), &e),
            })?;
        let mut guard = self.inner.lock().await;
        guard.catalog = catalog;
        guard.last_applied = meta.last_log_id;
        guard.last_membership = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<RaftNodeId>> {
        let guard = self.inner.lock().await;
        Ok(guard.snapshot.as_ref().map(clone_snapshot))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::{Reason, ResourceCommand};
    use crate::resource::ResourceKey;
    use openraft::{CommittedLeaderId, EntryPayload, LogId};

    fn put_entry(idx: u64, name: &str) -> Entry<TypeConfig> {
        let cmd = ResourceCommand::Put {
            key: ResourceKey::namespaced("", "v1", "Pod", "default", name),
            value: serde_json::json!({"spec": {"image": "v1"}}),
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
    async fn empty_store_reports_no_log_state() {
        let mut s = InMemoryStore::new();
        let state = s.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());
    }

    #[tokio::test]
    async fn apply_put_writes_to_catalog() {
        let mut s = InMemoryStore::new();
        let res = s.apply(vec![put_entry(1, "podinfo")]).await.unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].applied_index, 1);
        assert_eq!(res[0].op, crate::command::ResourceOp::Created);
        let catalog = s.current_catalog().await;
        assert_eq!(catalog.len(), 1);
        let key = ResourceKey::namespaced("", "v1", "Pod", "default", "podinfo");
        assert!(catalog.get(&key).is_some());
    }

    #[tokio::test]
    async fn vote_round_trips() {
        let mut s = InMemoryStore::new();
        assert!(s.read_vote().await.unwrap().is_none());
        let vote = Vote::new(1, 42);
        s.save_vote(&vote).await.unwrap();
        assert_eq!(s.read_vote().await.unwrap(), Some(vote));
    }

    #[tokio::test]
    async fn snapshot_builder_serializes_catalog() {
        let mut s = InMemoryStore::new();
        s.apply(vec![put_entry(1, "podinfo")]).await.unwrap();
        let mut builder = s.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();
        let bytes = snap.snapshot.get_ref();
        // The catalog JSON has the pod's metadata.name
        let s = std::str::from_utf8(bytes).unwrap();
        assert!(s.contains("podinfo"));
    }
}
