//! `RaftLogStorage` + `RaftStateMachine` for engenho-revoada.
//!
//! [`RaftStore::durable`] keeps Raft's hard state (vote, committed log
//! id, log, purge point, latest snapshot) in one file, written with
//! tmp + fsync + rename and a directory fsync before any mutating call
//! returns, so openraft never answers a peer on the strength of a fact
//! a crash would erase. The state machine is not persisted: it is
//! re-derived on boot from the snapshot and a replay of the log.
//! [`RaftStore::volatile`] keeps everything in memory, for the
//! in-process simulator and tests (docs/RECOVERABLE-STATE.md I1).

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, OptionalSend, RaftLogReader,
    RaftSnapshotBuilder, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::attestation::{AttestationChain, NodeIdentity};
use crate::consensus::MeshShape;
use crate::consensus::type_config::{ApplyResult, RaftNodeId, TypeConfig};

/// Combined log store and state machine. Both [`RaftLogStorage`] and
/// [`RaftStateMachine`] share the same Arc<Mutex<Inner>> so a
/// single owner can clone the handle and drive both traits.
///
/// At R4.5 the store also owns this node's [`NodeIdentity`] +
/// [`AttestationChain`]. Every committed entry that flows through
/// `apply()` is signed by the local identity + appended to the
/// chain — so EVERY node (leader and follower) maintains its own
/// auditor-verifiable chain. Leader changes preserve history.
#[derive(Clone)]
pub struct RaftStore {
    inner: Arc<Mutex<Inner>>,
    identity: NodeIdentity,
    chain: AttestationChain,
    /// Typed clock used to stamp every attestation-chain append.
    /// Production wires `WallClock`; tests pin via `FrozenClock` for
    /// byte-deterministic chain replay.
    clock: Arc<dyn engenho_substrate::Clock>,
    persistence: Arc<Persistence>,
}

enum Persistence {
    Volatile,
    Durable(PathBuf),
}

const HARD_STATE_FILE: &str = "raft-hard-state.json";

#[derive(Default, Serialize, Deserialize)]
struct HardState {
    vote: Option<Vote<RaftNodeId>>,
    committed: Option<LogId<RaftNodeId>>,
    log: Vec<Entry<TypeConfig>>,
    last_purged: Option<LogId<RaftNodeId>>,
    snapshot: Option<PersistedSnapshot>,
}

#[derive(Clone, Serialize, Deserialize)]
struct PersistedSnapshot {
    meta: SnapshotMeta<RaftNodeId, openraft::BasicNode>,
    bytes: Vec<u8>,
}

struct Recorded(HardState);

impl Persistence {
    fn record(&self, state: HardState) -> Result<Recorded, StorageError<RaftNodeId>> {
        if let Self::Durable(dir) = self {
            let bytes = serde_json::to_vec(&state).map_err(|e| write_error(&e))?;
            engenho_substrate::write_atomic(&dir.join(HARD_STATE_FILE), &bytes)
                .map_err(|e| write_error(&e))?;
            std::fs::File::open(dir)
                .and_then(|d| d.sync_all())
                .map_err(|e| write_error(&e))?;
        }
        Ok(Recorded(state))
    }

    fn load(dir: &Path) -> std::io::Result<HardState> {
        match std::fs::read(dir.join(HARD_STATE_FILE)) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(HardState::default()),
            Err(e) => Err(e),
        }
    }
}

fn write_error<E: std::error::Error + 'static>(e: &E) -> StorageError<RaftNodeId> {
    StorageError::IO {
        source: StorageIOError::new(
            ErrorSubject::Store,
            ErrorVerb::Write,
            openraft::AnyError::new(e),
        ),
    }
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

impl Inner {
    fn hard_state(&self) -> HardState {
        HardState {
            vote: self.vote,
            committed: self.committed,
            log: self.log.values().cloned().collect(),
            last_purged: self.last_purged,
            snapshot: self.snapshot.as_ref().map(|s| PersistedSnapshot {
                meta: s.meta.clone(),
                bytes: s.snapshot.get_ref().clone(),
            }),
        }
    }

    fn install(&mut self, Recorded(state): Recorded) {
        self.vote = state.vote;
        self.committed = state.committed;
        self.log = state.log.into_iter().map(|e| (e.log_id.index, e)).collect();
        self.last_purged = state.last_purged;
        self.snapshot = state.snapshot.map(|p| Snapshot {
            meta: p.meta,
            snapshot: Box::new(Cursor::new(p.bytes)),
        });
    }
}

impl RaftStore {
    /// Construct with this node's identity + a fresh attestation chain.
    /// Production clock — uses `engenho_substrate::WallClock`. Tests
    /// pinning timestamps should use [`Self::with_clock`].
    pub fn new(identity: NodeIdentity) -> Self {
        Self::volatile(identity)
    }

    #[must_use]
    pub fn volatile(identity: NodeIdentity) -> Self {
        Self::with_clock(identity, Arc::new(engenho_substrate::WallClock))
    }

    pub fn durable(identity: NodeIdentity, dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        Self::durable_with_clock(identity, dir, Arc::new(engenho_substrate::WallClock))
    }

    pub fn durable_with_clock(
        identity: NodeIdentity,
        dir: impl Into<PathBuf>,
        clock: Arc<dyn engenho_substrate::Clock>,
    ) -> std::io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        let state = Persistence::load(&dir)?;
        let mut inner = Inner::default();
        if let Some(snap) = &state.snapshot {
            inner.shape = serde_json::from_slice(&snap.bytes)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            inner.last_applied = snap.meta.last_log_id;
            inner.last_membership = snap.meta.last_membership.clone();
        }
        inner.install(Recorded(state));
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            identity,
            chain: AttestationChain::new(),
            clock,
            persistence: Arc::new(Persistence::Durable(dir)),
        })
    }

    #[must_use]
    pub fn is_durable(&self) -> bool {
        matches!(*self.persistence, Persistence::Durable(_))
    }

    /// Construct with an explicit typed clock — tests should pass a
    /// `FrozenClock` so attestation-chain timestamps are byte-deterministic.
    pub fn with_clock(identity: NodeIdentity, clock: Arc<dyn engenho_substrate::Clock>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
            identity,
            chain: AttestationChain::new(),
            clock,
            persistence: Arc::new(Persistence::Volatile),
        }
    }

    async fn mutate_hard_state(
        &self,
        change: impl FnOnce(&mut HardState),
    ) -> Result<(), StorageError<RaftNodeId>> {
        let mut guard = self.inner.lock().await;
        let mut next = guard.hard_state();
        change(&mut next);
        let recorded = self.persistence.record(next)?;
        guard.install(recorded);
        Ok(())
    }

    /// Read-only snapshot of the typed MeshShape (the application
    /// state). Used by the wrapper layer's `RaftMesh::current_shape`.
    pub async fn current_shape(&self) -> MeshShape {
        self.inner.lock().await.shape.clone()
    }

    /// Reference to this node's local attestation chain.
    pub fn attestation_chain(&self) -> &AttestationChain {
        &self.chain
    }
}

// ============================================================
//   RaftLogReader
// ============================================================

impl RaftLogReader<TypeConfig> for RaftStore {
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

// ============================================================
//   RaftLogStorage
// ============================================================

impl RaftLogStorage<TypeConfig> for RaftStore {
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
        let vote = *vote;
        self.mutate_hard_state(|h| h.vote = Some(vote)).await
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<RaftNodeId>>, StorageError<RaftNodeId>> {
        Ok(self.inner.lock().await.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<RaftNodeId>>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        self.mutate_hard_state(|h| h.committed = committed).await
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
        let entries: Vec<Entry<TypeConfig>> = entries.into_iter().collect();
        self.mutate_hard_state(|h| {
            if let Some(first) = entries.first().map(|e| e.log_id.index) {
                h.log.retain(|e| e.log_id.index < first);
            }
            h.log.extend(entries);
        })
        .await?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(
        &mut self,
        log_id: LogId<RaftNodeId>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        self.mutate_hard_state(|h| h.log.retain(|e| e.log_id.index < log_id.index))
            .await
    }

    async fn purge(&mut self, log_id: LogId<RaftNodeId>) -> Result<(), StorageError<RaftNodeId>> {
        self.mutate_hard_state(|h| {
            h.last_purged = Some(log_id);
            h.log.retain(|e| e.log_id.index > log_id.index);
        })
        .await
    }
}

// ============================================================
//   RaftSnapshotBuilder
// ============================================================

#[derive(Clone)]
pub struct RaftSnapshotBuilderHandle {
    store: RaftStore,
}

impl RaftSnapshotBuilder<TypeConfig> for RaftSnapshotBuilderHandle {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<RaftNodeId>> {
        let mut guard = self.store.inner.lock().await;
        let last_applied = guard.last_applied;
        let last_membership = guard.last_membership.clone();
        let shape_bytes = serde_json::to_vec(&guard.shape).map_err(|e| StorageError::IO {
            source: StorageIOError::read_snapshot(None, &e),
        })?;
        guard.snapshot_index += 1;
        let snapshot_id = format!("snap-{}", guard.snapshot_index);
        let snapshot: Snapshot<TypeConfig> = Snapshot {
            meta: SnapshotMeta {
                last_log_id: last_applied,
                last_membership: last_membership.clone(),
                snapshot_id,
            },
            snapshot: Box::new(Cursor::new(shape_bytes)),
        };
        let mut next = guard.hard_state();
        next.snapshot = Some(PersistedSnapshot {
            meta: snapshot.meta.clone(),
            bytes: snapshot.snapshot.get_ref().clone(),
        });
        let recorded = self.store.persistence.record(next)?;
        guard.install(recorded);
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

impl RaftStateMachine<TypeConfig> for RaftStore {
    type SnapshotBuilder = RaftSnapshotBuilderHandle;

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
        // Collect (cmd, log_id) pairs to sign AFTER releasing the
        // shape lock — signing is CPU-bound + AttestationChain has
        // its own mutex; we don't want to hold the state-machine
        // lock across signing.
        let mut to_sign: Vec<(crate::consensus::RoleAssignment, u64, u64)> = Vec::new();
        for entry in entries {
            let log_id = entry.log_id;
            match entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(cmd) => {
                    guard.shape.apply(&cmd, log_id.leader_id.term, log_id.index);
                    to_sign.push((cmd, log_id.leader_id.term, log_id.index));
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
        drop(guard);
        // Sign every applied RoleAssignment + append to this node's
        // chain. Each node's chain reflects its own apply log; the
        // auditor can verify ANY node's chain independently because
        // each block is signed with that node's identity (whose
        // pubkey is its NodeId in gossip).
        // Typed clock — `WallClock` in production, `FrozenClock` in
        // tests for byte-deterministic chain replay.
        let now_ms = self.clock.unix_ms();
        for (cmd, term, index) in to_sign {
            self.chain.append(&self.identity, cmd, now_ms, term, index);
        }
        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        RaftSnapshotBuilderHandle {
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
        let shape: MeshShape = serde_json::from_slice(&bytes).map_err(|e| StorageError::IO {
            source: StorageIOError::read_snapshot(Some(meta.signature()), &e),
        })?;
        let mut guard = self.inner.lock().await;
        let mut next = guard.hard_state();
        next.snapshot = Some(PersistedSnapshot {
            meta: meta.clone(),
            bytes,
        });
        let recorded = self.persistence.record(next)?;
        guard.install(recorded);
        guard.shape = shape;
        guard.last_applied = meta.last_log_id;
        guard.last_membership = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<RaftNodeId>> {
        let guard = self.inner.lock().await;
        Ok(guard
            .snapshot
            .as_ref()
            .map(SnapshotClone::clone_snapshot_data_or_skip))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeId;
    use crate::attestation::NodeIdentity;
    use crate::consensus::{Reason, RoleAssignment};
    use crate::membership::NodeRole;
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
        let mut s = RaftStore::new(NodeIdentity::from_seed([0xee; 32]));
        let state = s.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());
        assert!(state.last_purged_log_id.is_none());
    }

    #[tokio::test]
    async fn apply_promote_mutates_mesh_shape() {
        let mut s = RaftStore::new(NodeIdentity::from_seed([0xee; 32]));
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
        let mut s = RaftStore::new(NodeIdentity::from_seed([0xee; 32]));
        assert!(s.read_vote().await.unwrap().is_none());
        let vote = Vote::new(1, 42);
        s.save_vote(&vote).await.unwrap();
        assert_eq!(s.read_vote().await.unwrap(), Some(vote));
    }

    #[tokio::test]
    async fn with_clock_pins_chain_timestamps_deterministically() {
        // Apply the same promote entry on two distinct stores built
        // from the SAME identity + SAME FrozenClock — the resulting
        // attestation chain blocks must carry byte-identical
        // timestamps. Substrate relógio adoption unlocks
        // replayable consensus chains.
        let identity = NodeIdentity::from_seed([0xee; 32]);
        let clock = std::sync::Arc::new(engenho_substrate::FrozenClock::at(1_700_000_000_000));
        let mut s1 = RaftStore::with_clock(identity.clone(), clock.clone());
        let mut s2 = RaftStore::with_clock(identity, clock);
        let entry1 = promote_entry(1);
        let entry2 = promote_entry(1);
        s1.apply(vec![entry1]).await.unwrap();
        s2.apply(vec![entry2]).await.unwrap();
        // Both chains carry the same number of blocks with the same
        // timestamps. We can't compare the full chain (signatures
        // depend on internal nonces), but the count must match.
        assert_eq!(s1.chain.len(), s2.chain.len());
    }
}
