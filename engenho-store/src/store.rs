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
    Entry, EntryPayload, LogId, OptionalSend, RaftLogReader, RaftSnapshotBuilder,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use tokio::sync::Mutex;

use crate::state::ResourceCatalog;
use crate::type_config::{ApplyResult, RaftNodeId, TypeConfig};

#[derive(Clone, Default)]
pub struct InMemoryStore {
    inner: Arc<Mutex<Inner>>,
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
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::default()
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
                EntryPayload::Normal(cmd) => {
                    guard.catalog.apply(&cmd, log_id.leader_id.term, log_id.index)
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
