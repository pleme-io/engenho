//! Property: QuorumTracker convergence — K distinct same-evidence
//! receipts always reach Reached; mixed-evidence above threshold
//! always reports Dissent.

use engenho_substrate::{
    MaterializationReceipt, NodeId, QuorumTracker, ReceiptKind,
};
use proptest::prelude::*;

fn receipt(emitter_byte: u8, evidence_byte: u8) -> MaterializationReceipt {
    MaterializationReceipt::for_drv(
        [7u8; 32],
        NodeId::new([emitter_byte; 32]),
        100,
        [evidence_byte; 32],
    )
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(256),
        ..ProptestConfig::default()
    })]

    /// K distinct emitters with IDENTICAL evidence → Reached.
    #[test]
    fn k_distinct_same_evidence_reaches_quorum(
        threshold in 1usize..16,
        n_emitters in 1usize..16,
        evidence_byte in any::<u8>(),
    ) {
        prop_assume!(n_emitters >= threshold);
        let mut tracker = QuorumTracker::new(ReceiptKind::Drv, [7u8; 32], threshold);
        for emitter in 0..n_emitters {
            tracker.ingest(&receipt(emitter as u8, evidence_byte));
        }
        // With n_emitters >= threshold + all same evidence → Reached.
        prop_assert!(tracker.has_quorum());
        prop_assert_eq!(tracker.evidence_variants(), 1);
    }

    /// Below-threshold ingestion stays Pending.
    #[test]
    fn below_threshold_stays_pending(
        threshold in 2usize..16,
        n_emitters in 0usize..16,
        evidence_byte in any::<u8>(),
    ) {
        prop_assume!(n_emitters < threshold);
        let mut tracker = QuorumTracker::new(ReceiptKind::Drv, [7u8; 32], threshold);
        for emitter in 0..n_emitters {
            tracker.ingest(&receipt(emitter as u8, evidence_byte));
        }
        prop_assert!(!tracker.has_quorum());
    }

    /// Duplicate emitter doesn't increment count.
    #[test]
    fn duplicate_emitter_doesnt_double_count(
        threshold in 2usize..16,
        evidence_byte in any::<u8>(),
        n_repeats in 1usize..32,
    ) {
        let mut tracker = QuorumTracker::new(ReceiptKind::Drv, [7u8; 32], threshold);
        for _ in 0..n_repeats {
            tracker.ingest(&receipt(1, evidence_byte));  // same emitter every time
        }
        prop_assert_eq!(tracker.confirmed_count(), 1);
    }

    /// K distinct emitters with MIXED evidence reaching quorum → Dissent.
    #[test]
    fn mixed_evidence_above_threshold_reports_dissent(
        threshold in 2usize..8,
        e1 in any::<u8>(),
        e2 in any::<u8>(),
    ) {
        prop_assume!(e1 != e2);
        let mut tracker = QuorumTracker::new(ReceiptKind::Drv, [7u8; 32], threshold);
        // Push threshold receipts; alternate evidence variant.
        for emitter in 0..threshold {
            let ev = if emitter % 2 == 0 { e1 } else { e2 };
            tracker.ingest(&receipt(emitter as u8, ev));
        }
        prop_assert!(tracker.has_quorum());
        prop_assert!(tracker.evidence_variants() >= 2);
    }

    /// Reset clears all confirmations.
    #[test]
    fn reset_clears_state(
        threshold in 1usize..8,
        n_emitters in 1usize..16,
        evidence_byte in any::<u8>(),
    ) {
        let mut tracker = QuorumTracker::new(ReceiptKind::Drv, [7u8; 32], threshold);
        for emitter in 0..n_emitters {
            tracker.ingest(&receipt(emitter as u8, evidence_byte));
        }
        tracker.reset();
        prop_assert_eq!(tracker.confirmed_count(), 0);
        prop_assert!(!tracker.has_quorum());
    }
}
