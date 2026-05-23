//! Property: MemoryLedger invariants.

use engenho_substrate::{
    LedgerKey, MaterializationLedger, MaterializationReceipt, MemoryLedger, NodeId, QuorumOutcome,
    ReceiptKind, StageId,
};
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;

fn receipt(subject: [u8; 32], node_bytes: u8, emitted_at: u64) -> MaterializationReceipt {
    MaterializationReceipt::new(
        ReceiptKind::Shape("test/shape".into()),
        subject,
        NodeId::from_bytes(&[node_bytes; 32]),
        emitted_at,
        subject, // evidence = subject
    )
}

fn key(stage: &str, subject: [u8; 32]) -> LedgerKey {
    LedgerKey {
        stage_id: StageId::new(stage),
        kind: ReceiptKind::Shape("test/shape".into()),
        subject,
    }
}

proptest_with_env! {
    /// After ingesting a receipt, outcome(key) returns SOMETHING (not None).
    #[test]
    fn ingest_then_outcome_is_some(subject in any::<[u8; 32]>(), node in any::<u8>()) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ledger = MemoryLedger::new();
            let r = receipt(subject, node, 100);
            ledger.ingest(&StageId::new("s"), 1, &r).await.unwrap();
            let out = ledger.outcome(&key("s", subject)).await.unwrap();
            assert!(out.is_some());
        });
    }

    /// Two ingests of the same receipt (same node) are idempotent —
    /// the second doesn't crash + outcome stays well-formed.
    #[test]
    fn double_ingest_same_node_is_idempotent(subject in any::<[u8; 32]>(), node in any::<u8>()) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ledger = MemoryLedger::new();
            let r = receipt(subject, node, 100);
            ledger.ingest(&StageId::new("s"), 1, &r).await.unwrap();
            let out2 = ledger.ingest(&StageId::new("s"), 1, &r).await.unwrap();
            // Second ingest succeeds; outcome is Reached (threshold=1 met by single node).
            assert!(matches!(out2, QuorumOutcome::Reached { .. }));
        });
    }

    /// Threshold-of-1 with single ingest reaches quorum immediately.
    #[test]
    fn threshold_one_single_ingest_reaches(subject in any::<[u8; 32]>(), node in any::<u8>()) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ledger = MemoryLedger::new();
            let r = receipt(subject, node, 100);
            let out = ledger.ingest(&StageId::new("s"), 1, &r).await.unwrap();
            assert!(matches!(out, QuorumOutcome::Reached { .. }));
        });
    }

    /// forget_stage removes the slot — outcome returns None afterward.
    #[test]
    fn forget_stage_removes_outcome(subject in any::<[u8; 32]>(), node in any::<u8>()) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ledger = MemoryLedger::new();
            let r = receipt(subject, node, 100);
            ledger.ingest(&StageId::new("s"), 1, &r).await.unwrap();
            assert!(ledger.outcome(&key("s", subject)).await.unwrap().is_some());
            ledger.forget_stage(&StageId::new("s")).await.unwrap();
            assert!(ledger.outcome(&key("s", subject)).await.unwrap().is_none());
        });
    }

    /// Distinct (stage, subject) pairs each get distinct ledger slots.
    #[test]
    fn distinct_subjects_distinct_slots(
        subjects in proptest::collection::vec(any::<[u8; 32]>(), 2..6),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ledger = MemoryLedger::new();
            for (i, s) in subjects.iter().enumerate() {
                let r = receipt(*s, i as u8, 100);
                ledger.ingest(&StageId::new("s"), 1, &r).await.unwrap();
            }
            let len = ledger.len().await;
            // BTreeSet dedup — distinct subjects produce distinct keys.
            let unique = subjects.iter().copied().collect::<std::collections::BTreeSet<_>>().len();
            assert_eq!(len, unique);
        });
    }

    /// Ingesting to a non-existent stage doesn't crash on later outcome().
    #[test]
    fn outcome_on_unseen_key_returns_none(subject in any::<[u8; 32]>()) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ledger = MemoryLedger::new();
            let out = ledger.outcome(&key("never-seen", subject)).await.unwrap();
            assert!(out.is_none());
        });
    }

    /// forget_stage on an unseen stage doesn't error.
    #[test]
    fn forget_unseen_stage_is_no_op(stage_name in "[a-z]{1,16}") {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ledger = MemoryLedger::new();
            let res = ledger.forget_stage(&StageId::new(&stage_name)).await;
            assert!(res.is_ok());
        });
    }
}
