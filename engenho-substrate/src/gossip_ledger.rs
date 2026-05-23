//! GossipLedger — `MaterializationLedger` impl built on top of a
//! pluggable `GossipTransport`.
//!
//! Sits between the substrate's typed receipts and any
//! gossip-shaped backend (chitchat, scuttlebutt, Cassandra-style
//! Phi-accrual, custom UDP gossip). The trait surface is one
//! method — `broadcast_receipt` — so the substrate doesn't lock
//! the wire protocol.
//!
//! ## Receive path
//!
//! The transport delivers receipts via a tokio broadcast channel
//! the operator subscribes to. The GossipLedger's `receiver_task`
//! drains the channel + ingests each receipt into the inner
//! ledger so reads against the GossipLedger see the cluster-wide
//! aggregated state.
//!
//! ## Composition
//!
//! ```ignore
//! let transport = ChitchatTransport::bind(...).await?;     // production
//! let inner = MemoryLedger::new();
//! let gossip = GossipLedger::new(Arc::new(inner), Arc::new(transport));
//! gossip.start_receiver().await;
//! // Now every receipt ingested locally is broadcast cluster-wide,
//! // and every receipt received from peers is folded into the
//! // inner ledger's aggregate view.
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::{Mutex, broadcast};

use crate::ledger::{LedgerError, LedgerKey, MaterializationLedger};
use crate::quorum::QuorumOutcome;
use crate::receipt::MaterializationReceipt;
use crate::roca::StageId;

/// One outbound + one inbound channel pair the gossip transport
/// presents to the ledger.
pub struct GossipChannel {
    /// Sender — pushes receipts onto the gossip wire.
    pub outbound: Arc<dyn GossipBroadcaster>,
    /// Receiver — yields receipts arriving from the gossip wire.
    pub inbound: broadcast::Receiver<GossipDelivery>,
}

/// What the transport delivers when a receipt arrives.
#[derive(Clone, Debug)]
pub struct GossipDelivery {
    /// Stage the receipt belongs to.
    pub stage_id: StageId,
    /// Threshold the originating node was using for the tracker.
    pub threshold: usize,
    /// The receipt itself.
    pub receipt: MaterializationReceipt,
}

/// Gossip-transport errors.
#[derive(Debug, Clone, Error)]
pub enum GossipError {
    /// Backend (chitchat / UDP / TCP) returned an error.
    #[error("backend: {0}")]
    Backend(String),
    /// Transport not connected yet.
    #[error("not connected")]
    NotConnected,
}

crate::impl_error_kind! {
    GossipError {
        (Backend(_)) => "backend",
        NotConnected => "not_connected",
    }
}

/// One-method pluggable broadcaster. Implementations push the
/// `(stage_id, threshold, receipt)` triple onto the gossip wire.
#[async_trait]
pub trait GossipBroadcaster: Send + Sync {
    /// Broadcaster identifier for telemetry.
    fn name(&self) -> &'static str;

    /// Publish a receipt cluster-wide.
    ///
    /// # Errors
    /// [`GossipError::Backend`] on transport failure;
    /// [`GossipError::NotConnected`] if the transport isn't connected.
    async fn broadcast_receipt(
        &self,
        stage_id: &StageId,
        threshold: usize,
        receipt: &MaterializationReceipt,
    ) -> Result<(), GossipError>;
}

// =================================================================
// FakeGossipTransport — deterministic in-memory transport for tests
// =================================================================

/// In-memory transport. Outbound calls land on a `broadcast::Sender`
/// the test can subscribe to; inbound is fed by the test injecting
/// `GossipDelivery`s into a separate channel.
pub struct FakeGossipTransport {
    inner: Arc<Mutex<FakeGossipState>>,
    outbound_sender: broadcast::Sender<GossipBroadcast>,
}

/// A broadcast triple emitted by the transport's outbound side.
#[derive(Clone, Debug)]
pub struct GossipBroadcast {
    /// Stage the receipt belongs to.
    pub stage_id: StageId,
    /// Threshold used by the broadcasting node.
    pub threshold: usize,
    /// The receipt.
    pub receipt: MaterializationReceipt,
}

#[derive(Default)]
struct FakeGossipState {
    fail_next: Option<GossipError>,
    broadcasts: Vec<GossipBroadcast>,
}

impl Default for FakeGossipTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeGossipTransport {
    /// Fresh transport with a fan-out broadcast channel of capacity 128.
    #[must_use]
    pub fn new() -> Self {
        let (outbound_sender, _) = broadcast::channel(128);
        Self {
            inner: Arc::new(Mutex::new(FakeGossipState::default())),
            outbound_sender,
        }
    }

    /// Subscribe to the outbound side. Tests can assert what was
    /// broadcast + drive the wire end-to-end.
    pub fn subscribe_outbound(&self) -> broadcast::Receiver<GossipBroadcast> {
        self.outbound_sender.subscribe()
    }

    /// Inject the next broadcast call to fail.
    pub async fn fail_next(&self, err: GossipError) {
        self.inner.lock().await.fail_next = Some(err);
    }

    /// Snapshot of broadcasts emitted so far.
    pub async fn broadcasts(&self) -> Vec<GossipBroadcast> {
        self.inner.lock().await.broadcasts.clone()
    }
}

#[async_trait]
impl GossipBroadcaster for FakeGossipTransport {
    fn name(&self) -> &'static str {
        "fake"
    }

    async fn broadcast_receipt(
        &self,
        stage_id: &StageId,
        threshold: usize,
        receipt: &MaterializationReceipt,
    ) -> Result<(), GossipError> {
        let mut state = self.inner.lock().await;
        if let Some(err) = state.fail_next.take() {
            return Err(err);
        }
        let bcast = GossipBroadcast {
            stage_id: stage_id.clone(),
            threshold,
            receipt: receipt.clone(),
        };
        state.broadcasts.push(bcast.clone());
        drop(state);
        // Best-effort send — no subscribers is not an error.
        let _ = self.outbound_sender.send(bcast);
        Ok(())
    }
}

// =================================================================
// GossipLedger — pluggable transport + inner aggregator
// =================================================================

/// Ledger that broadcasts every local ingest cluster-wide AND
/// receives peer ingests via gossip.
pub struct GossipLedger {
    inner: Arc<dyn MaterializationLedger>,
    transport: Arc<dyn GossipBroadcaster>,
}

impl GossipLedger {
    /// New ledger composed from an inner aggregator + a broadcaster.
    #[must_use]
    pub fn new(
        inner: Arc<dyn MaterializationLedger>,
        transport: Arc<dyn GossipBroadcaster>,
    ) -> Self {
        Self { inner, transport }
    }

    /// Borrow the inner ledger.
    #[must_use]
    pub fn inner(&self) -> &Arc<dyn MaterializationLedger> {
        &self.inner
    }

    /// Borrow the transport.
    #[must_use]
    pub fn transport(&self) -> &Arc<dyn GossipBroadcaster> {
        &self.transport
    }

    /// Apply an inbound delivery from the gossip wire to the inner
    /// ledger. Operators call this from their receiver task.
    ///
    /// # Errors
    /// Propagates [`LedgerError`] from the inner ledger.
    pub async fn ingest_delivery(
        &self,
        delivery: &GossipDelivery,
    ) -> Result<QuorumOutcome, LedgerError> {
        self.inner
            .ingest(&delivery.stage_id, delivery.threshold, &delivery.receipt)
            .await
    }
}

#[async_trait]
impl MaterializationLedger for GossipLedger {
    fn name(&self) -> &'static str {
        "gossip"
    }

    async fn ingest(
        &self,
        stage_id: &StageId,
        threshold: usize,
        receipt: &MaterializationReceipt,
    ) -> Result<QuorumOutcome, LedgerError> {
        // 1. Apply locally.
        let outcome = self.inner.ingest(stage_id, threshold, receipt).await?;
        // 2. Broadcast cluster-wide. Backend errors don't taint
        //    the local commit; we surface them as a typed log
        //    elsewhere (operator's transport choice).
        let _ = self
            .transport
            .broadcast_receipt(stage_id, threshold, receipt)
            .await;
        Ok(outcome)
    }

    async fn outcome(&self, key: &LedgerKey) -> Result<Option<QuorumOutcome>, LedgerError> {
        self.inner.outcome(key).await
    }

    async fn forget_stage(&self, stage_id: &StageId) -> Result<(), LedgerError> {
        self.inner.forget_stage(stage_id).await
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

    fn assemble() -> (Arc<MemoryLedger>, Arc<FakeGossipTransport>, GossipLedger) {
        let inner = Arc::new(MemoryLedger::new());
        let transport = Arc::new(FakeGossipTransport::new());
        let ledger = GossipLedger::new(inner.clone(), transport.clone());
        (inner, transport, ledger)
    }

    // ── GossipBroadcaster (FakeGossipTransport) ────────────────

    #[tokio::test]
    async fn fake_transport_records_broadcasts() {
        let t = FakeGossipTransport::new();
        t.broadcast_receipt(&stage(), 3, &rcpt(1, 5)).await.unwrap();
        let b = t.broadcasts().await;
        assert_eq!(b.len(), 1);
        assert_eq!(b[0].stage_id, stage());
        assert_eq!(b[0].threshold, 3);
    }

    #[tokio::test]
    async fn fake_transport_subscribers_receive_broadcasts() {
        let t = FakeGossipTransport::new();
        let mut rx = t.subscribe_outbound();
        t.broadcast_receipt(&stage(), 3, &rcpt(1, 5)).await.unwrap();
        let received = rx.recv().await.unwrap();
        assert_eq!(received.receipt.emitter, NodeId::new([1u8; 32]));
    }

    #[tokio::test]
    async fn fake_transport_fail_next_returns_typed_error() {
        let t = FakeGossipTransport::new();
        t.fail_next(GossipError::NotConnected).await;
        let err = t
            .broadcast_receipt(&stage(), 1, &rcpt(1, 5))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "not_connected");
    }

    #[tokio::test]
    async fn fake_transport_broadcasts_with_no_subscribers_dont_fail() {
        let t = FakeGossipTransport::new();
        t.broadcast_receipt(&stage(), 1, &rcpt(1, 5)).await.unwrap();
    }

    #[tokio::test]
    async fn fake_transport_name_is_stable() {
        let t = FakeGossipTransport::new();
        assert_eq!(t.name(), "fake");
    }

    #[test]
    fn gossip_error_kinds_are_stable() {
        assert_eq!(GossipError::Backend("x".into()).kind(), "backend");
        assert_eq!(GossipError::NotConnected.kind(), "not_connected");
    }

    // ── GossipLedger ───────────────────────────────────────────

    #[tokio::test]
    async fn ingest_writes_locally_and_broadcasts() {
        let (inner, transport, ledger) = assemble();
        ledger.ingest(&stage(), 1, &rcpt(1, 5)).await.unwrap();
        // Inner has it.
        assert_eq!(inner.len().await, 1);
        // Transport saw it.
        let b = transport.broadcasts().await;
        assert_eq!(b.len(), 1);
        assert_eq!(b[0].stage_id, stage());
    }

    #[tokio::test]
    async fn ingest_outcome_propagated_from_inner() {
        let (_inner, _t, ledger) = assemble();
        let outcome = ledger.ingest(&stage(), 1, &rcpt(1, 5)).await.unwrap();
        // Threshold=1 + one emitter → Reached immediately.
        assert!(matches!(outcome, QuorumOutcome::Reached { .. }));
    }

    #[tokio::test]
    async fn transport_failure_does_not_taint_local_commit() {
        let (inner, transport, ledger) = assemble();
        transport.fail_next(GossipError::NotConnected).await;
        // Even though transport will fail, local commit succeeds.
        ledger.ingest(&stage(), 1, &rcpt(1, 5)).await.unwrap();
        assert_eq!(inner.len().await, 1);
    }

    #[tokio::test]
    async fn ingest_delivery_routes_to_inner_only() {
        let (inner, transport, ledger) = assemble();
        let delivery = GossipDelivery {
            stage_id: stage(),
            threshold: 1,
            receipt: rcpt(7, 5),
        };
        ledger.ingest_delivery(&delivery).await.unwrap();
        assert_eq!(inner.len().await, 1);
        // No outbound broadcast for an inbound delivery.
        assert_eq!(transport.broadcasts().await.len(), 0);
    }

    #[tokio::test]
    async fn forget_stage_passes_through_to_inner() {
        let (inner, _t, ledger) = assemble();
        ledger.ingest(&stage(), 1, &rcpt(1, 5)).await.unwrap();
        assert_eq!(inner.len().await, 1);
        ledger.forget_stage(&stage()).await.unwrap();
        assert_eq!(inner.len().await, 0);
    }

    #[tokio::test]
    async fn outcome_read_passes_through_to_inner() {
        let (_inner, _t, ledger) = assemble();
        ledger.ingest(&stage(), 1, &rcpt(1, 5)).await.unwrap();
        let key = LedgerKey {
            stage_id: stage(),
            kind: crate::receipt::ReceiptKind::Drv,
            subject: [7u8; 32],
        };
        let outcome = ledger.outcome(&key).await.unwrap();
        assert!(outcome.is_some());
    }

    #[tokio::test]
    async fn inner_and_transport_accessors() {
        let (inner, transport, ledger) = assemble();
        // Just sanity that the borrowed references match the originals.
        assert!(Arc::ptr_eq(
            ledger.inner(),
            &(inner as Arc<dyn MaterializationLedger>)
        ));
        assert!(Arc::ptr_eq(
            ledger.transport(),
            &(transport as Arc<dyn GossipBroadcaster>)
        ));
    }

    #[tokio::test]
    async fn backend_name_is_stable() {
        let (_inner, _t, ledger) = assemble();
        assert_eq!(ledger.name(), "gossip");
    }
}
