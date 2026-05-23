//! Property: BroadcastLedger event emission + subscriber semantics.

use engenho_substrate::{
    BroadcastLedger, LedgerEvent, MaterializationLedger, MaterializationReceipt, MemoryLedger,
    NodeId, ReceiptKind, StageId,
};
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;
use std::sync::Arc;

fn receipt(subject: [u8; 32], node_bytes: u8) -> MaterializationReceipt {
    MaterializationReceipt::new(
        ReceiptKind::Shape("test".into()),
        subject,
        NodeId::from_bytes(&[node_bytes; 32]),
        0,
        subject,
    )
}

proptest_with_env! {
    /// Every successful ingest emits exactly one ReceiptIngested event
    /// to each active subscriber.
    #[test]
    fn each_ingest_emits_one_event(
        subjects in proptest::collection::vec(any::<[u8; 32]>(), 1..8),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let inner: Arc<dyn MaterializationLedger> = Arc::new(MemoryLedger::new());
            let ledger = BroadcastLedger::new(inner);
            let mut rx = ledger.subscribe();
            for (i, s) in subjects.iter().enumerate() {
                let r = receipt(*s, i as u8);
                ledger.ingest(&StageId::new("s"), 1, &r).await.unwrap();
            }
            let mut count = 0;
            while let Ok(ev) = rx.try_recv() {
                if matches!(ev, LedgerEvent::ReceiptIngested { .. }) {
                    count += 1;
                }
            }
            assert_eq!(count, subjects.len());
        });
    }

    /// forget_stage emits exactly one StageForgotten event.
    #[test]
    fn forget_stage_emits_one_event(stage_name in "[a-z]{1,16}") {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let inner: Arc<dyn MaterializationLedger> = Arc::new(MemoryLedger::new());
            let ledger = BroadcastLedger::new(inner);
            let mut rx = ledger.subscribe();
            ledger
                .forget_stage(&StageId::new(&stage_name))
                .await
                .unwrap();
            let ev = rx.try_recv().unwrap();
            match ev {
                LedgerEvent::StageForgotten { stage_id } => {
                    assert_eq!(stage_id, StageId::new(&stage_name));
                }
                other => panic!("expected StageForgotten, got {other:?}"),
            }
        });
    }

    /// subscriber_count tracks subscribe/drop cycle.
    #[test]
    fn subscriber_count_tracks_lifecycle(n in 0usize..8) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let inner: Arc<dyn MaterializationLedger> = Arc::new(MemoryLedger::new());
            let ledger = BroadcastLedger::new(inner);
            let baseline = ledger.subscriber_count();
            let receivers: Vec<_> = (0..n).map(|_| ledger.subscribe()).collect();
            assert_eq!(ledger.subscriber_count(), baseline + n);
            drop(receivers);
            assert_eq!(ledger.subscriber_count(), baseline);
        });
    }

    /// Ingesting with no subscribers doesn't error (broadcast is best-effort).
    #[test]
    fn ingest_with_no_subscribers_succeeds(subject in any::<[u8; 32]>()) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let inner: Arc<dyn MaterializationLedger> = Arc::new(MemoryLedger::new());
            let ledger = BroadcastLedger::new(inner);
            // No subscribe() call.
            let r = receipt(subject, 0);
            let res = ledger.ingest(&StageId::new("s"), 1, &r).await;
            assert!(res.is_ok());
        });
    }

    /// Multiple subscribers all see the same event.
    #[test]
    fn multiple_subscribers_all_receive(subject in any::<[u8; 32]>()) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let inner: Arc<dyn MaterializationLedger> = Arc::new(MemoryLedger::new());
            let ledger = BroadcastLedger::new(inner);
            let mut rx1 = ledger.subscribe();
            let mut rx2 = ledger.subscribe();
            let r = receipt(subject, 0);
            ledger.ingest(&StageId::new("s"), 1, &r).await.unwrap();
            let ev1 = rx1.try_recv().unwrap();
            let ev2 = rx2.try_recv().unwrap();
            assert_eq!(ev1, ev2);
        });
    }

    /// BroadcastLedger preserves inner ledger semantics for outcome().
    #[test]
    fn outcome_delegates_to_inner(subject in any::<[u8; 32]>(), node in any::<u8>()) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let inner: Arc<dyn MaterializationLedger> = Arc::new(MemoryLedger::new());
            let ledger = BroadcastLedger::new(inner);
            let r = receipt(subject, node);
            ledger.ingest(&StageId::new("s"), 1, &r).await.unwrap();
            // outcome() delegates to inner — same shape.
            let key = engenho_substrate::LedgerKey {
                stage_id: StageId::new("s"),
                kind: ReceiptKind::Shape("test".into()),
                subject,
            };
            let out = ledger.outcome(&key).await.unwrap();
            assert!(out.is_some());
        });
    }
}
