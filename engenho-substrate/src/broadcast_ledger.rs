//! BroadcastLedger — wraps any `MaterializationLedger` + emits
//! typed events on every ingest.
//!
//! Same pattern as `WatchedCache` (which wraps `DerivationCacheBackend`).
//! Gives consumers an event-driven path so PlantioController can
//! schedule a reconcile tick the instant a receipt lands (paired
//! with EventDrivenController from engenho-controllers).
//!
//! ## Composition
//!
//! ```ignore
//! let inner = MemoryLedger::new();
//! let watched = BroadcastLedger::new(Arc::new(inner));
//! let mut rx = watched.subscribe(64);
//! watched.ingest(&stage_id, 3, &receipt).await?;
//! // rx receives LedgerEvent::ReceiptIngested { stage_id, outcome }
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::broadcast;

use crate::ledger::{LedgerError, LedgerKey, MaterializationLedger};
use crate::quorum::QuorumOutcome;
use crate::receipt::MaterializationReceipt;
use crate::roca::StageId;

/// Typed ledger event broadcast to subscribers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LedgerEvent {
    /// A receipt was ingested and the tracker's post-state is `outcome`.
    ReceiptIngested {
        /// Stage the receipt belonged to.
        stage_id: StageId,
        /// What the tracker says now.
        outcome: QuorumOutcome,
    },
    /// `forget_stage(stage_id)` was invoked.
    StageForgotten {
        /// Stage that was forgotten.
        stage_id: StageId,
    },
}

/// Wrapped ledger that broadcasts events on every write.
pub struct BroadcastLedger {
    inner: Arc<dyn MaterializationLedger>,
    sender: broadcast::Sender<LedgerEvent>,
}

impl BroadcastLedger {
    /// Wrap `inner` with a broadcast channel of capacity 128.
    #[must_use]
    pub fn new(inner: Arc<dyn MaterializationLedger>) -> Self {
        Self::with_capacity(inner, 128)
    }

    /// Wrap with an explicit channel capacity.
    #[must_use]
    pub fn with_capacity(inner: Arc<dyn MaterializationLedger>, capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { inner, sender }
    }

    /// Subscribe to ledger events.
    pub fn subscribe(&self) -> broadcast::Receiver<LedgerEvent> {
        self.sender.subscribe()
    }

    /// Number of currently-active subscribers.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }

    /// Borrow the wrapped ledger.
    #[must_use]
    pub fn inner(&self) -> &Arc<dyn MaterializationLedger> {
        &self.inner
    }
}

/// Backwards-compat alias for the per-wrapper snapshot type that
/// shipped in v0.86. Migrated to the canonical
/// [`crate::mirante::SubscriberSnapshot`] in v0.87 per the TSR
/// extraction. Pattern #6 (deprecation alias) — zero-churn rename.
#[deprecated(
    since = "0.1.4",
    note = "use engenho_substrate::SubscriberSnapshot directly (v0.87)"
)]
pub type BroadcastLedgerSnapshot = crate::mirante::SubscriberSnapshot;

crate::define_named!(BroadcastLedger, "broadcast");

impl crate::mirante::Observable for BroadcastLedger {
    type Snapshot = crate::mirante::SubscriberSnapshot;

    fn snapshot(&self) -> Self::Snapshot {
        crate::mirante::SubscriberSnapshot {
            name: "broadcast",
            subscriber_count: self.subscriber_count(),
        }
    }
}

#[async_trait]
impl MaterializationLedger for BroadcastLedger {
    fn name(&self) -> &'static str {
        "broadcast"
    }

    async fn ingest(
        &self,
        stage_id: &StageId,
        threshold: usize,
        receipt: &MaterializationReceipt,
    ) -> Result<QuorumOutcome, LedgerError> {
        let outcome = self.inner.ingest(stage_id, threshold, receipt).await?;
        // Best-effort broadcast — no subscribers is not an error.
        let _ = self.sender.send(LedgerEvent::ReceiptIngested {
            stage_id: stage_id.clone(),
            outcome: outcome.clone(),
        });
        Ok(outcome)
    }

    async fn outcome(&self, key: &LedgerKey) -> Result<Option<QuorumOutcome>, LedgerError> {
        self.inner.outcome(key).await
    }

    async fn forget_stage(&self, stage_id: &StageId) -> Result<(), LedgerError> {
        self.inner.forget_stage(stage_id).await?;
        let _ = self.sender.send(LedgerEvent::StageForgotten {
            stage_id: stage_id.clone(),
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::MemoryLedger;
    use crate::receipt::{MaterializationReceipt, NodeId};

    fn rcpt(emitter: u8, evidence: u8) -> MaterializationReceipt {
        MaterializationReceipt::for_drv([7u8; 32], NodeId::new([emitter; 32]), 100, [evidence; 32])
    }

    fn stage() -> StageId {
        StageId::new("x")
    }

    fn wrap_memory() -> BroadcastLedger {
        BroadcastLedger::new(Arc::new(MemoryLedger::new()))
    }

    #[tokio::test]
    async fn ingest_emits_event() {
        let l = wrap_memory();
        let mut rx = l.subscribe();
        l.ingest(&stage(), 1, &rcpt(1, 5)).await.unwrap();
        let event = rx.recv().await.unwrap();
        match event {
            LedgerEvent::ReceiptIngested {
                stage_id,
                outcome: _,
            } => {
                assert_eq!(stage_id, stage());
            }
            _ => panic!("expected ReceiptIngested"),
        }
    }

    #[tokio::test]
    async fn outcome_event_propagates_reached() {
        let l = wrap_memory();
        let mut rx = l.subscribe();
        l.ingest(&stage(), 1, &rcpt(1, 5)).await.unwrap();
        let event = rx.recv().await.unwrap();
        if let LedgerEvent::ReceiptIngested { outcome, .. } = event {
            assert!(matches!(outcome, QuorumOutcome::Reached { .. }));
        }
    }

    #[tokio::test]
    async fn outcome_event_propagates_dissent() {
        let l = wrap_memory();
        let mut rx = l.subscribe();
        l.ingest(&stage(), 2, &rcpt(1, 5)).await.unwrap();
        let _ = rx.recv().await; // first Pending
        l.ingest(&stage(), 2, &rcpt(2, 6)).await.unwrap(); // dissent
        let event = rx.recv().await.unwrap();
        if let LedgerEvent::ReceiptIngested { outcome, .. } = event {
            assert!(matches!(outcome, QuorumOutcome::Dissent { .. }));
        }
    }

    #[tokio::test]
    async fn forget_stage_emits_event() {
        let l = wrap_memory();
        let mut rx = l.subscribe();
        l.forget_stage(&stage()).await.unwrap();
        let event = rx.recv().await.unwrap();
        assert_eq!(event, LedgerEvent::StageForgotten { stage_id: stage() });
    }

    #[tokio::test]
    async fn outcome_read_does_not_emit_event() {
        let l = wrap_memory();
        l.ingest(&stage(), 1, &rcpt(1, 5)).await.unwrap();
        let mut rx = l.subscribe();
        let key = LedgerKey {
            stage_id: stage(),
            kind: crate::receipt::ReceiptKind::Drv,
            subject: [7u8; 32],
        };
        let _ = l.outcome(&key).await.unwrap();
        // Read after subscribe — no event should arrive.
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn multiple_subscribers_all_receive() {
        let l = wrap_memory();
        let mut rx1 = l.subscribe();
        let mut rx2 = l.subscribe();
        assert_eq!(l.subscriber_count(), 2);
        l.ingest(&stage(), 1, &rcpt(1, 5)).await.unwrap();
        let e1 = rx1.recv().await.unwrap();
        let e2 = rx2.recv().await.unwrap();
        assert_eq!(e1, e2);
    }

    #[tokio::test]
    async fn writes_with_no_subscribers_dont_fail() {
        let l = wrap_memory();
        l.ingest(&stage(), 1, &rcpt(1, 5)).await.unwrap();
    }

    #[tokio::test]
    async fn wrapped_ledger_writes_actually_persist() {
        let inner = Arc::new(MemoryLedger::new());
        let l = BroadcastLedger::new(inner.clone());
        l.ingest(&stage(), 1, &rcpt(1, 5)).await.unwrap();
        // Underlying ledger has the receipt.
        assert_eq!(inner.len().await, 1);
    }

    #[tokio::test]
    async fn subscriber_count_decreases_when_receiver_dropped() {
        let l = wrap_memory();
        let rx = l.subscribe();
        assert_eq!(l.subscriber_count(), 1);
        drop(rx);
        assert_eq!(l.subscriber_count(), 0);
    }

    #[tokio::test]
    async fn backend_name_is_stable() {
        let l = wrap_memory();
        assert_eq!(l.name(), "broadcast");
    }
}
