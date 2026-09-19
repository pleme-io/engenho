//! Property: QuorumTracker outcome state machine.

use std::collections::{BTreeMap, BTreeSet};

use engenho_substrate::{MaterializationReceipt, NodeId, QuorumState, QuorumTracker, ReceiptKind};
use engenho_substrate_props::helpers::threshold_in;
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
        threshold in threshold_in(1..8),
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

    /// The verdict matches a reference model of the fold: Pending
    /// below the threshold, otherwise Reached when every distinct
    /// emitter's latest evidence agrees and Dissent when it does not.
    #[test]
    fn verdict_matches_the_reference_fold(
        threshold in threshold_in(1..8),
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
        let verdict = last_outcome.unwrap();
        let mut latest: BTreeMap<u8, u8> = BTreeMap::new();
        for (e, ev) in &pairs {
            latest.insert(*e, *ev);
        }
        let variants = latest.values().collect::<BTreeSet<_>>().len();
        let expected = if latest.len() < threshold.get() {
            QuorumState::Pending
        } else if variants > 1 {
            QuorumState::Dissent
        } else {
            QuorumState::Reached
        };
        prop_assert_eq!(verdict.state(), expected);
        prop_assert_eq!(verdict.confirmed(), latest.len());
        prop_assert_eq!(verdict.threshold(), threshold);
        prop_assert_eq!(verdict.evidence_variants(), variants);
        prop_assert_eq!(t.verdict(), verdict);
    }

    /// Reset returns tracker to initial state regardless of history.
    #[test]
    fn reset_restores_initial_state(
        threshold in threshold_in(1..8),
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
        threshold in threshold_in(1..8),
        pairs in proptest::collection::vec((0u8..16, 0u8..4), 0..32),
    ) {
        let mut t = QuorumTracker::new(ReceiptKind::Drv, [7u8; 32], threshold);
        for (e, ev) in &pairs {
            t.ingest(&receipt(*e, *ev));
        }
        prop_assert_eq!(t.emitters().len(), t.confirmed_count());
    }
}
