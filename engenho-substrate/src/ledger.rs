//! MaterializationLedger — cluster-wide receipt accumulator.
//!
//! Holds one [`QuorumTracker`] per (stage_id, kind, subject) tuple
//! and routes incoming receipts to the right tracker. The roça
//! runtime consults the ledger to decide whether each stage's
//! `ConfirmacaoPolicy` is satisfied.
//!
//! ## Trait shape
//!
//! Pluggable so consumers can swap in cluster-wide backends
//! (chitchat-gossiped ledger / Raft-committed ledger / federation
//! ledger) without changing call sites.
//!
//! ## Composition with prior primitives
//!
//! - Ingests [`MaterializationReceipt`] / [`VerificationReceipt`]
//! - Routes to [`QuorumTracker`] keyed by (stage_id, kind, subject)
//! - Returns the tracker's own [`QuorumVerdict`] per query. A backend
//!   never folds receipts itself: the verdict is sealed, so the only way
//!   to answer is to ask the tracker.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::RwLock;

use crate::quorum::{QuorumTracker, QuorumVerdict};
use crate::receipt::{MaterializationReceipt, ReceiptKind};
use crate::roca::StageId;

/// Composite key identifying one tracked materialization slot.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LedgerKey {
    /// Stage this receipt belongs to.
    pub stage_id: StageId,
    /// What kind of receipt (drv / nar / build_result / shape).
    pub kind: ReceiptKind,
    /// Subject hash the receipt is attesting.
    pub subject: [u8; 32],
}

/// Ledger errors.
#[derive(Debug, Clone, Error)]
pub enum LedgerError {
    /// Backend (gossip / Raft / disk) returned an error.
    #[error("backend: {0}")]
    Backend(String),
}

crate::impl_error_kind! {
    LedgerError {
        (Backend(_)) => "backend",
    }
}

/// Pluggable receipt accumulator.
#[async_trait]
pub trait MaterializationLedger: Send + Sync {
    /// Backend identifier for telemetry.
    fn name(&self) -> &'static str;

    /// Ingest a receipt against the given stage. Returns the
    /// post-ingest verdict for the tracker. `threshold` is fixed by the
    /// first receipt for a slot; later receipts for the same slot are
    /// counted against that threshold.
    ///
    /// # Errors
    /// [`LedgerError::Backend`] on backend failure.
    async fn ingest(
        &self,
        stage_id: &StageId,
        threshold: NonZeroUsize,
        receipt: &MaterializationReceipt,
    ) -> Result<QuorumVerdict, LedgerError>;

    /// Query the current verdict for a tracked slot, counted against
    /// the slot's own threshold. Returns `None` if no receipts have
    /// been recorded for that slot yet.
    ///
    /// # Errors
    /// [`LedgerError::Backend`] on backend failure.
    async fn outcome(&self, key: &LedgerKey) -> Result<Option<QuorumVerdict>, LedgerError>;

    /// Forget every receipt for a stage — useful after eviction
    /// or rollback of a Plantio.
    ///
    /// # Errors
    /// [`LedgerError::Backend`] on backend failure.
    async fn forget_stage(&self, stage_id: &StageId) -> Result<(), LedgerError>;
}

// =================================================================
// MemoryLedger — deterministic backend for tests + bootstrap
// =================================================================

/// In-memory backend. Per-key QuorumTracker; thread-safe via
/// `RwLock` so the apiserver + controllers share one instance.
#[derive(Default, Clone)]
pub struct MemoryLedger {
    inner: Arc<RwLock<MemoryLedgerState>>,
}

#[derive(Default)]
struct MemoryLedgerState {
    trackers: BTreeMap<LedgerKey, QuorumTracker>,
}

impl MemoryLedger {
    /// Fresh empty ledger.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of distinct tracked keys.
    pub async fn keys(&self) -> Vec<LedgerKey> {
        self.inner.read().await.trackers.keys().cloned().collect()
    }

    /// Count of distinct tracked slots.
    pub async fn len(&self) -> usize {
        self.inner.read().await.trackers.len()
    }
}

#[async_trait]
impl MaterializationLedger for MemoryLedger {
    fn name(&self) -> &'static str {
        "memory"
    }

    async fn ingest(
        &self,
        stage_id: &StageId,
        threshold: NonZeroUsize,
        receipt: &MaterializationReceipt,
    ) -> Result<QuorumVerdict, LedgerError> {
        let key = LedgerKey {
            stage_id: stage_id.clone(),
            kind: receipt.kind.clone(),
            subject: receipt.subject,
        };
        let mut state = self.inner.write().await;
        let tracker = state.trackers.entry(key).or_insert_with(|| {
            QuorumTracker::new(receipt.kind.clone(), receipt.subject, threshold)
        });
        Ok(tracker.ingest(receipt))
    }

    async fn outcome(&self, key: &LedgerKey) -> Result<Option<QuorumVerdict>, LedgerError> {
        let state = self.inner.read().await;
        Ok(state.trackers.get(key).map(QuorumTracker::verdict))
    }

    async fn forget_stage(&self, stage_id: &StageId) -> Result<(), LedgerError> {
        let mut state = self.inner.write().await;
        state.trackers.retain(|k, _| &k.stage_id != stage_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quorum::QuorumState;
    use crate::receipt::{MaterializationReceipt, NodeId, ReceiptKind};

    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).expect("test thresholds are non-zero")
    }

    fn key() -> LedgerKey {
        LedgerKey {
            stage_id: stage(),
            kind: ReceiptKind::Drv,
            subject: [7u8; 32],
        }
    }

    fn rcpt(emitter: u8, evidence: u8) -> MaterializationReceipt {
        MaterializationReceipt::for_drv([7u8; 32], NodeId::new([emitter; 32]), 100, [evidence; 32])
    }

    fn stage() -> StageId {
        StageId::new("build-image")
    }

    #[tokio::test]
    async fn fresh_ledger_is_empty() {
        let l = MemoryLedger::new();
        assert_eq!(l.len().await, 0);
        assert!(l.keys().await.is_empty());
    }

    #[tokio::test]
    async fn ingest_creates_tracker_for_new_key() {
        let l = MemoryLedger::new();
        l.ingest(&stage(), nz(3), &rcpt(1, 5)).await.unwrap();
        assert_eq!(l.len().await, 1);
    }

    #[tokio::test]
    async fn ingest_routes_by_stage_id() {
        let l = MemoryLedger::new();
        l.ingest(&StageId::new("a"), nz(3), &rcpt(1, 5))
            .await
            .unwrap();
        l.ingest(&StageId::new("b"), nz(3), &rcpt(1, 5))
            .await
            .unwrap();
        assert_eq!(l.len().await, 2);
    }

    #[tokio::test]
    async fn ingest_reaches_quorum_with_distinct_emitters() {
        let l = MemoryLedger::new();
        let o1 = l.ingest(&stage(), nz(3), &rcpt(1, 5)).await.unwrap();
        assert_eq!((o1.state(), o1.confirmed()), (QuorumState::Pending, 1));
        let o2 = l.ingest(&stage(), nz(3), &rcpt(2, 5)).await.unwrap();
        assert_eq!((o2.state(), o2.confirmed()), (QuorumState::Pending, 2));
        let o3 = l.ingest(&stage(), nz(3), &rcpt(3, 5)).await.unwrap();
        assert!(o3.is_reached());
        assert_eq!((o3.confirmed(), o3.threshold()), (3, nz(3)));
    }

    #[tokio::test]
    async fn dissent_surfaces_when_evidence_disagrees() {
        let l = MemoryLedger::new();
        l.ingest(&stage(), nz(2), &rcpt(1, 5)).await.unwrap();
        let o = l.ingest(&stage(), nz(2), &rcpt(2, 6)).await.unwrap();
        assert_eq!(o.state(), QuorumState::Dissent);
    }

    // The read path used to re-derive the verdict without the slot's
    // threshold: any agreeing confirmation read back as `Reached` and
    // any disagreement as `Dissent`. It now returns the tracker's own
    // verdict, so a read and the ingest that preceded it agree.

    #[tokio::test]
    async fn outcome_counts_against_the_slots_threshold() {
        let l = MemoryLedger::new();
        let ingested = l.ingest(&stage(), nz(3), &rcpt(1, 5)).await.unwrap();
        let read = l.outcome(&key()).await.unwrap().expect("slot exists");
        assert_eq!(read.state(), QuorumState::Pending, "1 of 3 is not a quorum");
        assert_eq!((read.confirmed(), read.threshold()), (1, nz(3)));
        assert_eq!(read, ingested);
    }

    #[tokio::test]
    async fn outcome_reports_disagreement_below_threshold_as_pending() {
        let l = MemoryLedger::new();
        l.ingest(&stage(), nz(3), &rcpt(1, 5)).await.unwrap();
        let ingested = l.ingest(&stage(), nz(3), &rcpt(2, 6)).await.unwrap();
        let read = l.outcome(&key()).await.unwrap().expect("slot exists");
        assert_eq!(read.state(), QuorumState::Pending);
        assert_eq!(read, ingested);
    }

    #[tokio::test]
    async fn later_receipts_count_against_the_first_threshold() {
        let l = MemoryLedger::new();
        l.ingest(&stage(), nz(3), &rcpt(1, 5)).await.unwrap();
        let v = l.ingest(&stage(), nz(1), &rcpt(2, 5)).await.unwrap();
        assert_eq!((v.state(), v.threshold()), (QuorumState::Pending, nz(3)));
    }

    #[tokio::test]
    async fn outcome_returns_none_for_unknown_key() {
        let l = MemoryLedger::new();
        assert!(l.outcome(&key()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn outcome_returns_some_after_ingest() {
        let l = MemoryLedger::new();
        l.ingest(&stage(), nz(1), &rcpt(1, 5)).await.unwrap();
        assert!(l.outcome(&key()).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn forget_stage_drops_all_its_trackers() {
        let l = MemoryLedger::new();
        l.ingest(&StageId::new("a"), nz(3), &rcpt(1, 5))
            .await
            .unwrap();
        l.ingest(&StageId::new("a"), nz(3), &rcpt(2, 5))
            .await
            .unwrap();
        l.ingest(&StageId::new("b"), nz(3), &rcpt(1, 5))
            .await
            .unwrap();
        assert_eq!(l.len().await, 2);
        l.forget_stage(&StageId::new("a")).await.unwrap();
        assert_eq!(l.len().await, 1);
        assert_eq!(l.keys().await[0].stage_id, StageId::new("b"));
    }

    #[tokio::test]
    async fn forget_stage_is_idempotent() {
        let l = MemoryLedger::new();
        l.forget_stage(&stage()).await.unwrap();
        l.forget_stage(&stage()).await.unwrap();
    }

    #[tokio::test]
    async fn duplicate_emitter_doesnt_double_count() {
        let l = MemoryLedger::new();
        l.ingest(&stage(), nz(3), &rcpt(1, 5)).await.unwrap();
        l.ingest(&stage(), nz(3), &rcpt(1, 5)).await.unwrap();
        l.ingest(&stage(), nz(3), &rcpt(1, 5)).await.unwrap();
        // Same emitter → still 1 confirmation, so still pending.
        let verdict = l.outcome(&key()).await.unwrap().unwrap();
        assert_eq!(verdict.confirmed(), 1);
        assert_eq!(verdict.state(), QuorumState::Pending);
    }

    #[tokio::test]
    async fn backend_name_is_stable() {
        assert_eq!(MemoryLedger::new().name(), "memory");
    }

    #[test]
    fn error_kind_is_stable() {
        assert_eq!(LedgerError::Backend("x".into()).kind(), "backend");
    }

    #[test]
    fn ledger_key_orders_deterministically() {
        let k1 = LedgerKey {
            stage_id: StageId::new("a"),
            kind: ReceiptKind::Drv,
            subject: [1u8; 32],
        };
        let k2 = LedgerKey {
            stage_id: StageId::new("a"),
            kind: ReceiptKind::Drv,
            subject: [2u8; 32],
        };
        assert!(k1 < k2);
    }
}
