//! QuorumTracker — accumulates receipts + signals when threshold reached.
//!
//! The substrate's eventually-consistent confirmation primitive.
//! A controller, scheduler, or DAG stage feeds it receipts from
//! gossip / Raft / direct emission; the tracker reports when:
//!
//!   * Quorum reached: K-of-N (or K-absolute) distinct emitters
//!     have confirmed the SAME (kind, subject) pair.
//!   * Dissent: at least one receipt's evidence_hash disagrees
//!     with the majority's — surfaced as a typed fault for the
//!     consumer to act on (re-derivation, eviction, alert).
//!
//! ## Why per-(kind, subject) tracking
//!
//! A node can confirm many things; a tracker is scoped to ONE
//! materialization target. Composition: a stage with N targets
//! holds N trackers.

use std::collections::{BTreeMap, BTreeSet};

use crate::receipt::{MaterializationReceipt, NodeId, ReceiptKind};

/// Outcome of evaluating the tracker after each receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QuorumOutcome {
    /// Not yet at threshold; current count `< threshold`.
    Pending {
        /// How many distinct emitters have confirmed so far.
        confirmed: usize,
        /// Threshold required to reach quorum.
        threshold: usize,
    },
    /// Quorum reached AND all evidence agrees.
    Reached {
        /// Total distinct emitters confirmed.
        confirmed: usize,
        /// Threshold required.
        threshold: usize,
    },
    /// Quorum count reached BUT at least one emitter disagrees on
    /// evidence_hash. The consumer must investigate.
    Dissent {
        /// Total distinct emitters confirmed.
        confirmed: usize,
        /// Number of distinct evidence hashes seen.
        evidence_variants: usize,
    },
}

/// Per-target receipt accumulator.
#[derive(Debug, Clone)]
pub struct QuorumTracker {
    kind: ReceiptKind,
    subject: [u8; 32],
    threshold: usize,
    /// `emitter -> evidence_hash`. Most recent receipt per emitter
    /// wins (idempotent re-emission).
    confirmations: BTreeMap<NodeId, [u8; 32]>,
}

impl QuorumTracker {
    /// New tracker for `(kind, subject)` with threshold `K`.
    /// `threshold` must be ≥ 1; clamps silently.
    #[must_use]
    pub fn new(kind: ReceiptKind, subject: [u8; 32], threshold: usize) -> Self {
        Self {
            kind,
            subject,
            threshold: threshold.max(1),
            confirmations: BTreeMap::new(),
        }
    }

    /// Subject the tracker is gating on.
    #[must_use]
    pub fn subject(&self) -> &[u8; 32] {
        &self.subject
    }

    /// Kind the tracker is gating on.
    #[must_use]
    pub fn kind(&self) -> &ReceiptKind {
        &self.kind
    }

    /// Current distinct-emitter count.
    #[must_use]
    pub fn confirmed_count(&self) -> usize {
        self.confirmations.len()
    }

    /// True if quorum has been reached (≥ threshold distinct emitters)
    /// — independent of evidence agreement.
    #[must_use]
    pub fn has_quorum(&self) -> bool {
        self.confirmations.len() >= self.threshold
    }

    /// Distinct evidence-hash count. 1 = all emitters agree.
    /// 2+ = dissent. 0 = no receipts yet.
    #[must_use]
    pub fn evidence_variants(&self) -> usize {
        self.confirmations.values().collect::<BTreeSet<_>>().len()
    }

    /// Snapshot of distinct confirming emitters.
    #[must_use]
    pub fn emitters(&self) -> Vec<NodeId> {
        self.confirmations.keys().copied().collect()
    }

    /// Ingest a receipt. Returns the post-ingest outcome.
    ///
    /// Receipts for a DIFFERENT (kind, subject) are silently ignored
    /// — callers should route per tracker. Re-emission from the
    /// same emitter overwrites the prior evidence_hash (most-recent
    /// wins; assumption: emitter is itself the authority for its
    /// own claim).
    pub fn ingest(&mut self, receipt: &MaterializationReceipt) -> QuorumOutcome {
        if receipt.kind != self.kind || receipt.subject != self.subject {
            // Wrong target — no state change.
            return self.outcome();
        }
        self.confirmations
            .insert(receipt.emitter, receipt.evidence_hash);
        self.outcome()
    }

    /// Compute the outcome without mutating.
    fn outcome(&self) -> QuorumOutcome {
        let confirmed = self.confirmed_count();
        let variants = self.evidence_variants();
        if variants > 1 && confirmed >= self.threshold {
            return QuorumOutcome::Dissent {
                confirmed,
                evidence_variants: variants,
            };
        }
        if confirmed >= self.threshold {
            QuorumOutcome::Reached {
                confirmed,
                threshold: self.threshold,
            }
        } else {
            QuorumOutcome::Pending {
                confirmed,
                threshold: self.threshold,
            }
        }
    }

    /// Forget every confirmation — useful after evicting a target.
    pub fn reset(&mut self) {
        self.confirmations.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(emitter: u8, evidence: u8) -> MaterializationReceipt {
        MaterializationReceipt::for_drv([1u8; 32], NodeId::new([emitter; 32]), 100, [evidence; 32])
    }

    fn other_subject_receipt(emitter: u8) -> MaterializationReceipt {
        MaterializationReceipt::for_drv(
            [99u8; 32], // different subject
            NodeId::new([emitter; 32]),
            100,
            [0u8; 32],
        )
    }

    fn other_kind_receipt(emitter: u8) -> MaterializationReceipt {
        MaterializationReceipt::for_nar([1u8; 32], NodeId::new([emitter; 32]), 100, [0u8; 32])
    }

    #[test]
    fn starts_pending_with_zero_confirmations() {
        let t = QuorumTracker::new(ReceiptKind::Drv, [1u8; 32], 3);
        assert_eq!(
            t.outcome(),
            QuorumOutcome::Pending {
                confirmed: 0,
                threshold: 3
            }
        );
    }

    #[test]
    fn reaches_quorum_when_threshold_distinct_emitters_agree() {
        let mut t = QuorumTracker::new(ReceiptKind::Drv, [1u8; 32], 3);
        assert!(matches!(
            t.ingest(&r(1, 5)),
            QuorumOutcome::Pending { confirmed: 1, .. }
        ));
        assert!(matches!(
            t.ingest(&r(2, 5)),
            QuorumOutcome::Pending { confirmed: 2, .. }
        ));
        let o = t.ingest(&r(3, 5));
        assert_eq!(
            o,
            QuorumOutcome::Reached {
                confirmed: 3,
                threshold: 3
            }
        );
    }

    #[test]
    fn duplicate_emitter_doesnt_increment_count() {
        let mut t = QuorumTracker::new(ReceiptKind::Drv, [1u8; 32], 3);
        t.ingest(&r(1, 5));
        t.ingest(&r(1, 5)); // same emitter, same evidence
        t.ingest(&r(1, 5));
        assert_eq!(t.confirmed_count(), 1);
        assert!(!t.has_quorum());
    }

    #[test]
    fn dissent_when_evidence_variants_diverge_past_quorum() {
        let mut t = QuorumTracker::new(ReceiptKind::Drv, [1u8; 32], 2);
        t.ingest(&r(1, 5));
        let o = t.ingest(&r(2, 6)); // different evidence
        assert!(matches!(
            o,
            QuorumOutcome::Dissent {
                confirmed: 2,
                evidence_variants: 2
            }
        ));
    }

    #[test]
    fn dissent_overrides_reached_when_thresholds_both_met() {
        let mut t = QuorumTracker::new(ReceiptKind::Drv, [1u8; 32], 2);
        t.ingest(&r(1, 5));
        t.ingest(&r(2, 5)); // Reached
        let o = t.ingest(&r(3, 7)); // Adds dissent.
        assert!(matches!(o, QuorumOutcome::Dissent { .. }));
    }

    #[test]
    fn most_recent_evidence_per_emitter_wins() {
        let mut t = QuorumTracker::new(ReceiptKind::Drv, [1u8; 32], 1);
        t.ingest(&r(1, 5));
        t.ingest(&r(1, 6)); // emitter 1 changes its mind
        assert_eq!(t.confirmed_count(), 1);
        // Only one evidence hash now → no dissent.
        assert_eq!(t.evidence_variants(), 1);
    }

    #[test]
    fn ignores_receipts_with_different_subject() {
        let mut t = QuorumTracker::new(ReceiptKind::Drv, [1u8; 32], 1);
        t.ingest(&other_subject_receipt(1));
        assert_eq!(t.confirmed_count(), 0);
    }

    #[test]
    fn ignores_receipts_with_different_kind() {
        let mut t = QuorumTracker::new(ReceiptKind::Drv, [1u8; 32], 1);
        t.ingest(&other_kind_receipt(1));
        assert_eq!(t.confirmed_count(), 0);
    }

    #[test]
    fn threshold_clamps_to_one_for_zero_input() {
        let t = QuorumTracker::new(ReceiptKind::Drv, [1u8; 32], 0);
        assert!(!t.has_quorum());
        // Threshold internal must be ≥ 1.
        if let QuorumOutcome::Pending { threshold, .. } = t.outcome() {
            assert_eq!(threshold, 1);
        }
    }

    #[test]
    fn reset_clears_state() {
        let mut t = QuorumTracker::new(ReceiptKind::Drv, [1u8; 32], 2);
        t.ingest(&r(1, 5));
        t.ingest(&r(2, 5));
        assert!(t.has_quorum());
        t.reset();
        assert_eq!(t.confirmed_count(), 0);
        assert!(!t.has_quorum());
    }

    #[test]
    fn emitters_returns_distinct_set() {
        let mut t = QuorumTracker::new(ReceiptKind::Drv, [1u8; 32], 3);
        t.ingest(&r(1, 5));
        t.ingest(&r(2, 5));
        t.ingest(&r(1, 5)); // duplicate
        let mut ids = t.emitters();
        ids.sort();
        assert_eq!(ids.len(), 2);
    }
}
