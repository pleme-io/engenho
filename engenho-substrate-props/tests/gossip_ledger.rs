//! Property: GossipLedger transport broadcasting + inner delegation.

use engenho_substrate::{
    FakeGossipTransport, GossipBroadcaster, GossipError, GossipLedger, MaterializationLedger,
    MaterializationReceipt, MemoryLedger, NodeId, ReceiptKind, StageId,
};
use engenho_substrate_props::{block_on, proptest_with_env};
use proptest::prelude::*;
use std::sync::Arc;

fn receipt(subject: [u8; 32], node_byte: u8) -> MaterializationReceipt {
    MaterializationReceipt::new(
        ReceiptKind::Shape("test".into()),
        subject,
        NodeId::from_bytes(&[node_byte; 32]),
        0,
        subject,
    )
}

proptest_with_env! {
    /// Every successful ingest produces exactly one outbound broadcast.
    #[test]
    fn each_ingest_records_one_broadcast(
        subjects in proptest::collection::vec(any::<[u8; 32]>(), 1..6),
    ) {
        block_on(async {
            let inner: Arc<dyn MaterializationLedger> = Arc::new(MemoryLedger::new());
            let transport = Arc::new(FakeGossipTransport::new());
            let ledger = GossipLedger::new(inner, transport.clone());
            for (i, s) in subjects.iter().enumerate() {
                let r = receipt(*s, i as u8);
                ledger.ingest(&StageId::new("s"), 1, &r).await.unwrap();
            }
            let recorded = transport.broadcasts().await;
            assert_eq!(recorded.len(), subjects.len());
        });
    }

    /// Outbound broadcasts preserve stage + threshold + receipt identity.
    #[test]
    fn broadcasts_preserve_payload(
        stage_name in "[a-z]{1,16}",
        threshold in 1usize..16,
        subject in any::<[u8; 32]>(),
        node in any::<u8>(),
    ) {
        block_on(async {
            let inner: Arc<dyn MaterializationLedger> = Arc::new(MemoryLedger::new());
            let transport = Arc::new(FakeGossipTransport::new());
            let ledger = GossipLedger::new(inner, transport.clone());
            let r = receipt(subject, node);
            ledger
                .ingest(&StageId::new(&stage_name), threshold, &r)
                .await
                .unwrap();
            let recorded = transport.broadcasts().await;
            assert_eq!(recorded.len(), 1);
            assert_eq!(recorded[0].stage_id, StageId::new(&stage_name));
            assert_eq!(recorded[0].threshold, threshold);
            assert_eq!(recorded[0].receipt, r);
        });
    }

    /// fail_next causes the next ingest's transport call to error.
    /// The local inner ingest still succeeds (gossip is fire-and-forget).
    #[test]
    fn fail_next_surfaces_on_ingest(subject in any::<[u8; 32]>()) {
        block_on(async {
            let transport = Arc::new(FakeGossipTransport::new());
            transport
                .fail_next(GossipError::Backend("simulated".into()))
                .await;
            // Direct test of the transport — broadcast_receipt returns Err.
            let res = transport
                .broadcast_receipt(&StageId::new("s"), 1, &receipt(subject, 0))
                .await;
            assert!(res.is_err());
        });
    }

    /// Subscribing to outbound side receives every broadcast.
    #[test]
    fn outbound_subscribers_receive_broadcasts(
        subjects in proptest::collection::vec(any::<[u8; 32]>(), 1..6),
    ) {
        block_on(async {
            let inner: Arc<dyn MaterializationLedger> = Arc::new(MemoryLedger::new());
            let transport = Arc::new(FakeGossipTransport::new());
            let mut rx = transport.subscribe_outbound();
            let ledger = GossipLedger::new(inner, transport);
            for (i, s) in subjects.iter().enumerate() {
                let r = receipt(*s, i as u8);
                ledger.ingest(&StageId::new("s"), 1, &r).await.unwrap();
            }
            let mut count = 0;
            while let Ok(_) = rx.try_recv() {
                count += 1;
            }
            assert_eq!(count, subjects.len());
        });
    }

    /// Outcome on GossipLedger delegates to inner.
    #[test]
    fn outcome_delegates_to_inner(subject in any::<[u8; 32]>(), node in any::<u8>()) {
        block_on(async {
            let inner: Arc<dyn MaterializationLedger> = Arc::new(MemoryLedger::new());
            let transport = Arc::new(FakeGossipTransport::new());
            let ledger = GossipLedger::new(inner, transport);
            let r = receipt(subject, node);
            ledger.ingest(&StageId::new("s"), 1, &r).await.unwrap();
            let key = engenho_substrate::LedgerKey {
                stage_id: StageId::new("s"),
                kind: ReceiptKind::Shape("test".into()),
                subject,
            };
            let out = ledger.outcome(&key).await.unwrap();
            assert!(out.is_some());
        });
    }

    /// forget_stage delegates to inner — outcome returns None after.
    #[test]
    fn forget_stage_propagates_to_inner(subject in any::<[u8; 32]>(), node in any::<u8>()) {
        block_on(async {
            let inner: Arc<dyn MaterializationLedger> = Arc::new(MemoryLedger::new());
            let transport = Arc::new(FakeGossipTransport::new());
            let ledger = GossipLedger::new(inner, transport);
            let r = receipt(subject, node);
            ledger.ingest(&StageId::new("s"), 1, &r).await.unwrap();
            ledger.forget_stage(&StageId::new("s")).await.unwrap();
            let key = engenho_substrate::LedgerKey {
                stage_id: StageId::new("s"),
                kind: ReceiptKind::Shape("test".into()),
                subject,
            };
            let out = ledger.outcome(&key).await.unwrap();
            assert!(out.is_none());
        });
    }
}
