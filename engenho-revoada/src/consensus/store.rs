//! In-memory `RaftLogStorage` + `RaftStateMachine` impls for
//! engenho-revoada.
//!
//! This is the R2 minimum-viable store: log and state-machine
//! state live in `Arc<Mutex<...>>`. Sufficient for the in-process
//! single-node integration test that proves the openraft wiring
//! correctly applies a [`RoleAssignment`] to the typed [`MeshShape`].
//!
//! R2.5+ replaces this with a persistent backend (sled or redb)
//! and adds full snapshot-builder semantics so log compaction is
//! sound. The shape of the public API does not change between
//! R2 and R2.5 — only the backing storage.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, RaftLogReader, RaftSnapshotBuilder,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use tokio::sync::Mutex;

use crate::consensus::type_config::{ApplyResult, RaftNodeId, TypeConfig};
use crate::consensus::MeshShape;

/// Combined in-memory store. Both [`RaftLogStorage`] and
/// [`RaftStateMachine`] share the same Arc<Mutex<Inner>> so a
/// single owner can clone the handle and drive both traits.
#[derive(Clone, Default)]
pub struct InMemoryStore {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
struct Inner {
    /// Persisted vote (election state).
    vote: Option<Vote<RaftNodeId>>,
    /// Persisted committed log id.
    committed: Option<LogId<RaftNodeId>>,
    /// Log entries by index.
    log: BTreeMap<u64, Entry<TypeConfig>>,
    /// Last purged log id (for log compaction housekeeping).
    last_purged: Option<LogId<RaftNodeId>>,
    /// State-machine view — the typed mesh shape.
    shape: MeshShape,
    /// Last applied to state machine.
    last_applied: Option<LogId<RaftNodeId>>,
    /// Membership last observed during apply.
    last_membership: StoredMembership<RaftNodeId, openraft::BasicNode>,
    /// Most recent snapshot (if any).
    snapshot: Option<Snapshot<TypeConfig>>,
    /// Counter for snapshot id assignments.
    snapshot_index: u64,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Read-only snapshot of the typed MeshShape (the application
    /// state). Used by the wrapper layer's `RaftMesh::current_shape`.
    pub async fn current_shape(&self) -> MeshShape {
        self.inner.lock().await.shape.clone()
    }
}

// ============================================================
//   RaftLogReader
// ============================================================

impl RaftLogReader<TypeConfig> for InMemoryStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<RaftNodeId>> {
        let guard = self.inner.lock().await;
        let entries: Vec<Entry<TypeConfig>> = guard
            .log
            .range(range)
            .map(|(_, e)| e.clone())
            .collect();
        Ok(entries)
    }
}

// ============================================================
//   RaftLogStorage
// ============================================================

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

    async fn save_vote(
        &mut self,
        vote: &Vote<RaftNodeId>,
    ) -> Result<(), StorageError<RaftNodeId>> {
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
        // In-memory => I/O is synchronous. Signal callback immediately.
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

    async fn purge(
        &mut self,
        log_id: LogId<RaftNodeId>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        let mut guard = self.inner.lock().await;
        guard.last_purged = Some(log_id);
        guard.log.retain(|&idx, _| idx > log_id.index);
        Ok(())
    }
}

// ============================================================
//   RaftSnapshotBuilder
// ============================================================

#[derive(Clone)]
pub struct InMemorySnapshotBuilder {
    store: InMemoryStore,
}

impl RaftSnapshotBuilder<TypeConfig> for InMemorySnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<RaftNodeId>> {
        let mut guard = self.store.inner.lock().await;
        let last_applied = guard.last_applied;
        let last_membership = guard.last_membership.clone();
        let shape_bytes = serde_json::to_vec(&guard.shape).map_err(|e| {
            StorageError::IO {
                source: StorageIOError::read_snapshot(None, &e),
            }
        })?;
        guard.snapshot_index += 1;
        let snapshot_id = format!("snap-{}", guard.snapshot_index);
        let snapshot = Snapshot {
            meta: SnapshotMeta {
                last_log_id: last_applied,
                last_membership: last_membership.clone(),
                snapshot_id,
            },
            snapshot: Box::new(Cursor::new(shape_bytes)),
        };
        guard.snapshot = Some(snapshot.clone_snapshot_data_or_skip());
        Ok(snapshot)
    }
}

/// We need a way to clone Snapshot — since SnapshotData is
/// `Cursor<Vec<u8>>` we can deep-clone it ourselves.
trait SnapshotClone {
    fn clone_snapshot_data_or_skip(&self) -> Snapshot<TypeConfig>;
}

impl SnapshotClone for Snapshot<TypeConfig> {
    fn clone_snapshot_data_or_skip(&self) -> Snapshot<TypeConfig> {
        let buf = self.snapshot.get_ref().clone();
        Snapshot {
            meta: self.meta.clone(),
            snapshot: Box::new(Cursor::new(buf)),
        }
    }
}

// ============================================================
//   RaftStateMachine
// ============================================================

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

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<ApplyResult>, StorageError<RaftNodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut guard = self.inner.lock().await;
        let mut results = Vec::new();
        for entry in entries {
            let log_id = entry.log_id;
            match entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(cmd) => {
                    guard.shape.apply(&cmd, log_id.leader_id.term, log_id.index);
                }
                EntryPayload::Membership(m) => {
                    guard.last_membership = StoredMembership::new(Some(log_id), m);
                }
            }
            guard.last_applied = Some(log_id);
            results.push(ApplyResult {
                applied_index: log_id.index,
                applied_term: log_id.leader_id.term,
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
        let shape: MeshShape = serde_json::from_slice(&bytes).map_err(|e| {
            StorageError::IO {
                source: StorageIOError::read_snapshot(Some(meta.signature()), &e),
            }
        })?;
        let mut guard = self.inner.lock().await;
        guard.shape = shape;
        guard.last_applied = meta.last_log_id;
        guard.last_membership = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<RaftNodeId>> {
        let guard = self.inner.lock().await;
        Ok(guard.snapshot.as_ref().map(SnapshotClone::clone_snapshot_data_or_skip))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::{Reason, RoleAssignment};
    use crate::membership::NodeRole;
    use crate::NodeId;
    use openraft::EntryPayload;
    use openraft::{CommittedLeaderId, LogId};

    fn promote_entry(idx: u64) -> Entry<TypeConfig> {
        let mut role_set = std::collections::BTreeSet::new();
        role_set.insert(NodeRole::ApiServer);
        let cmd = RoleAssignment::Promote {
            node_id: NodeId::new([1; 32]),
            roles: role_set,
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
        assert!(state.last_purged_log_id.is_none());
    }

    #[tokio::test]
    async fn apply_promote_mutates_mesh_shape() {
        let mut s = InMemoryStore::new();
        let entry = promote_entry(1);
        let results = s.apply(vec![entry]).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].applied_index, 1);
        let shape = s.current_shape().await;
        let holders = shape.holders(NodeRole::ApiServer);
        assert_eq!(holders.len(), 1);
        assert_eq!(shape.last_applied_index, 1);
    }

    #[tokio::test]
    async fn vote_round_trips() {
        let mut s = InMemoryStore::new();
        assert!(s.read_vote().await.unwrap().is_none());
        let vote = Vote::new(1, 42);
        s.save_vote(&vote).await.unwrap();
        assert_eq!(s.read_vote().await.unwrap(), Some(vote));
    }
}
