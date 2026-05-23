//! Property: MaterializationReceipt round-trip + content-addressed id.

use engenho_substrate::{MaterializationReceipt, NodeId, ReceiptKind};
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;

fn receipt_kind_strategy() -> impl Strategy<Value = ReceiptKind> {
    prop_oneof![
        Just(ReceiptKind::Drv),
        Just(ReceiptKind::Nar),
        Just(ReceiptKind::Realisation),
        Just(ReceiptKind::BuildResult),
        "[a-z][a-z0-9_-]{0,16}".prop_map(ReceiptKind::Shape),
    ]
}

proptest_with_env! {
    /// Receipt round-trips through serde_json.
    #[test]
    fn receipt_serde_round_trip(
        kind in receipt_kind_strategy(),
        subject in any::<[u8; 32]>(),
        emitter_bytes in any::<[u8; 32]>(),
        emitted_at in 0u64..i64::MAX as u64,
        evidence in any::<[u8; 32]>(),
    ) {
        let r = MaterializationReceipt::new(
            kind, subject, NodeId::new(emitter_bytes), emitted_at, evidence,
        );
        let bytes = serde_json::to_vec(&r).unwrap();
        let back: MaterializationReceipt = serde_json::from_slice(&bytes).unwrap();
        prop_assert_eq!(back, r);
    }

    /// receipt.id() is deterministic for the same receipt.
    #[test]
    fn receipt_id_is_deterministic(
        kind in receipt_kind_strategy(),
        subject in any::<[u8; 32]>(),
        emitter_bytes in any::<[u8; 32]>(),
        emitted_at in 0u64..1_000_000,
        evidence in any::<[u8; 32]>(),
    ) {
        let r1 = MaterializationReceipt::new(
            kind.clone(), subject, NodeId::new(emitter_bytes), emitted_at, evidence,
        );
        let r2 = MaterializationReceipt::new(
            kind, subject, NodeId::new(emitter_bytes), emitted_at, evidence,
        );
        prop_assert_eq!(r1.id(), r2.id());
    }

    /// receipt.id() diverges when any field changes.
    #[test]
    fn receipt_id_diverges_per_subject(
        kind in receipt_kind_strategy(),
        s1 in any::<[u8; 32]>(),
        s2 in any::<[u8; 32]>(),
        emitter_bytes in any::<[u8; 32]>(),
        emitted_at in 0u64..1_000_000,
        evidence in any::<[u8; 32]>(),
    ) {
        prop_assume!(s1 != s2);
        let r1 = MaterializationReceipt::new(kind.clone(), s1, NodeId::new(emitter_bytes), emitted_at, evidence);
        let r2 = MaterializationReceipt::new(kind, s2, NodeId::new(emitter_bytes), emitted_at, evidence);
        prop_assert_ne!(r1.id(), r2.id());
    }

    /// same_subject ignores emitter + timestamp + evidence.
    #[test]
    fn same_subject_ignores_emitter_timestamp(
        kind in receipt_kind_strategy(),
        subject in any::<[u8; 32]>(),
        e1 in any::<[u8; 32]>(),
        e2 in any::<[u8; 32]>(),
        ts1 in 0u64..1_000_000,
        ts2 in 0u64..1_000_000,
        ev1 in any::<[u8; 32]>(),
        ev2 in any::<[u8; 32]>(),
    ) {
        let r1 = MaterializationReceipt::new(kind.clone(), subject, NodeId::new(e1), ts1, ev1);
        let r2 = MaterializationReceipt::new(kind, subject, NodeId::new(e2), ts2, ev2);
        prop_assert!(r1.same_subject(&r2));
    }

    /// agrees_with requires matching subject AND evidence.
    #[test]
    fn agrees_with_requires_matching_evidence(
        kind in receipt_kind_strategy(),
        subject in any::<[u8; 32]>(),
        e1 in any::<[u8; 32]>(),
        ev1 in any::<[u8; 32]>(),
        ev2 in any::<[u8; 32]>(),
    ) {
        prop_assume!(ev1 != ev2);
        let r1 = MaterializationReceipt::new(kind.clone(), subject, NodeId::new(e1), 0, ev1);
        let r2 = MaterializationReceipt::new(kind, subject, NodeId::new(e1), 0, ev2);
        prop_assert!(r1.same_subject(&r2));
        prop_assert!(!r1.agrees_with(&r2));
    }
}
