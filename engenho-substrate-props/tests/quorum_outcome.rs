//! Property: QuorumTracker outcome state machine.

use engenho_substrate::{
    MaterializationReceipt, NodeId, QuorumOutcome, QuorumTracker, ReceiptKind,
};
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;

fn receipt(emitter_byte: u8, evidence_byte: u8) -> MaterializationReceipt {
    MaterializationReceipt::for_drv(
        [7u8; 32],
        NodeId::new([emitter_byte; 32]),
        100,
        [evidence_byte; 32],
    )
}

proptest_with_env! {
    /// Ingest order doesn't affect final outcome (commutative).
    #[test]
    fn ingest_order_commutative(
        threshold in 1usize..8,
        evidence_byte in any::<u8>(),
        order in proptest::collection::vec(0u8..8, 1..16),
    ) {
        let mut t1 = QuorumTracker::new(ReceiptKind::Drv, [7u8; 32], threshold);
        for &e in &order {
            t1.ingest(&receipt(e, evidence_byte));
        }
        let mut t2 = QuorumTracker::new(ReceiptKind::Drv, [7u8; 32], threshold);
        let mut reversed = order.clone();
        reversed.reverse();
        for &e in &reversed {
            t2.ingest(&receipt(e, evidence_byte));
        }
        prop_assert_eq!(t1.confirmed_count(), t2.confirmed_count());
        prop_assert_eq!(t1.evidence_variants(), t2.evidence_variants());
    }

    /// QuorumOutcome variants are exhaustive: Pending OR Reached OR Dissent.
    #[test]
    fn outcome_is_one_of_three_variants(
        threshold in 1usize..8,
        emitters in proptest::collection::vec(0u8..16, 1..20),
        evidences in proptest::collection::vec(0u8..4, 1..20),
    ) {
        let mut t = QuorumTracker::new(ReceiptKind::Drv, [7u8; 32], threshold);
        let pairs: Vec<(u8, u8)> = emitters
            .into_iter()
            .zip(evidences.into_iter().cycle())
            .collect();
        let mut last_outcome = None;
        for (e, ev) in &pairs {
            last_outcome = Some(t.ingest(&receipt(*e, *ev)));
        }
        let outcome = last_outcome.unwrap();
        // Must be one of three variants — exhaustive enum.
        let is_one_of_three = matches!(
            outcome,
            QuorumOutcome::Pending { .. }
                | QuorumOutcome::Reached { .. }
                | QuorumOutcome::Dissent { .. }
        );
        prop_assert!(is_one_of_three);
    }

    /// Reset returns tracker to initial state regardless of history.
    #[test]
    fn reset_restores_initial_state(
        threshold in 1usize..8,
        pairs in proptest::collection::vec((0u8..16, 0u8..16), 0..32),
    ) {
        let mut t = QuorumTracker::new(ReceiptKind::Drv, [7u8; 32], threshold);
        for (e, ev) in &pairs {
            t.ingest(&receipt(*e, *ev));
        }
        t.reset();
        prop_assert_eq!(t.confirmed_count(), 0);
        prop_assert_eq!(t.evidence_variants(), 0);
        prop_assert!(!t.has_quorum());
    }

    /// emitters() length equals confirmed_count().
    #[test]
    fn emitters_length_equals_count(
        threshold in 1usize..8,
        pairs in proptest::collection::vec((0u8..16, 0u8..4), 0..32),
    ) {
        let mut t = QuorumTracker::new(ReceiptKind::Drv, [7u8; 32], threshold);
        for (e, ev) in &pairs {
            t.ingest(&receipt(*e, *ev));
        }
        prop_assert_eq!(t.emitters().len(), t.confirmed_count());
    }
}
