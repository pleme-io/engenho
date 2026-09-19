//! Property: MemoryLedger invariants.

use engenho_substrate::{LedgerKey, MaterializationLedger, MemoryLedger, ReceiptKind, StageId};
use engenho_substrate_props::helpers::{sample_emitter, sample_receipt as receipt, threshold_in};
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;
use std::num::NonZeroUsize;

fn key(stage: &str, subject: [u8; 32]) -> LedgerKey {
    LedgerKey {
        stage_id: StageId::new(stage),
        kind: ReceiptKind::Shape("test".into()),
        subject,
    }
}

proptest_with_env! {
    /// After ingesting a receipt, outcome(key) returns SOMETHING (not None).
    #[test]
    fn ingest_then_outcome_is_some(subject in any::<[u8; 32]>(), node in any::<u8>()) {
        engenho_substrate_props::block_on(async {
            let ledger = MemoryLedger::new();
            let r = receipt(subject, node);
            ledger.ingest(&StageId::new("s"), NonZeroUsize::MIN, &r).await.unwrap();
            let out = ledger.outcome(&key("s", subject)).await.unwrap();
            assert!(out.is_some());
    });
    }

    /// Two ingests of the same receipt (same node) are idempotent —
    /// the second doesn't crash + outcome stays well-formed.
    #[test]
    fn double_ingest_same_node_is_idempotent(subject in any::<[u8; 32]>(), node in any::<u8>()) {
        engenho_substrate_props::block_on(async {
            let ledger = MemoryLedger::new();
            let r = receipt(subject, node);
            ledger.ingest(&StageId::new("s"), NonZeroUsize::MIN, &r).await.unwrap();
            let out2 = ledger.ingest(&StageId::new("s"), NonZeroUsize::MIN, &r).await.unwrap();
            // Second ingest succeeds; outcome is Reached (threshold=1 met by single node).
            assert!(out2.is_reached());
    });
    }

    /// Reading a slot returns exactly the verdict its last ingest
    /// returned, whatever the threshold: the read path asks the
    /// tracker instead of re-deriving a verdict without the threshold.
    #[test]
    fn outcome_equals_the_last_ingest_verdict(
        threshold in threshold_in(1..6),
        subject in any::<[u8; 32]>(),
        votes in proptest::collection::vec((0u8..6, 0u8..3), 1..12),
    ) {
        engenho_substrate_props::block_on(async {
            let ledger = MemoryLedger::new();
            for (node, evidence) in &votes {
                let r = engenho_substrate::MaterializationReceipt::new(
                    ReceiptKind::Shape("test".into()),
                    subject,
                    sample_emitter(*node),
                    0,
                    [*evidence; 32],
                );
                let ingested = ledger.ingest(&StageId::new("s"), threshold, &r).await.unwrap();
                let read = ledger.outcome(&key("s", subject)).await.unwrap();
                assert_eq!(read, Some(ingested));
            }
        });
    }

    /// Threshold-of-1 with single ingest reaches quorum immediately.
    #[test]
    fn threshold_one_single_ingest_reaches(subject in any::<[u8; 32]>(), node in any::<u8>()) {
        engenho_substrate_props::block_on(async {
            let ledger = MemoryLedger::new();
            let r = receipt(subject, node);
            let out = ledger.ingest(&StageId::new("s"), NonZeroUsize::MIN, &r).await.unwrap();
            assert!(out.is_reached());
    });
    }

    /// forget_stage removes the slot — outcome returns None afterward.
    #[test]
    fn forget_stage_removes_outcome(subject in any::<[u8; 32]>(), node in any::<u8>()) {
        engenho_substrate_props::block_on(async {
            let ledger = MemoryLedger::new();
            let r = receipt(subject, node);
            ledger.ingest(&StageId::new("s"), NonZeroUsize::MIN, &r).await.unwrap();
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
        engenho_substrate_props::block_on(async {
            let ledger = MemoryLedger::new();
            for (i, s) in subjects.iter().enumerate() {
                let r = receipt(*s, i as u8);
                ledger.ingest(&StageId::new("s"), NonZeroUsize::MIN, &r).await.unwrap();
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
        engenho_substrate_props::block_on(async {
            let ledger = MemoryLedger::new();
            let out = ledger.outcome(&key("never-seen", subject)).await.unwrap();
            assert!(out.is_none());
    });
    }

    /// forget_stage on an unseen stage doesn't error.
    #[test]
    fn forget_unseen_stage_is_no_op(stage_name in "[a-z]{1,16}") {
        engenho_substrate_props::block_on(async {
            let ledger = MemoryLedger::new();
            let res = ledger.forget_stage(&StageId::new(&stage_name)).await;
            assert!(res.is_ok());
    });
    }
}
