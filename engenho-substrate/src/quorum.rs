//! QuorumTracker — the one quorum fold.
//!
//! Every place engenho asks "have enough distinct voters agreed?"
//! answers through [`Tally::verdict`]. There is no second copy of the
//! arithmetic: a ledger's read path, revoada's majority check and its
//! promotion gate all fold through this module.
//!
//!   * [`QuorumTracker`] is the fold scoped to one materialization
//!     target (a `(kind, subject)` pair) and fed
//!     [`MaterializationReceipt`]s. It is the substrate's
//!     eventually-consistent confirmation primitive.
//!   * [`Tally`] is the same fold over any voter and evidence type.
//!     It is public so a voter set that is not a receipt stream (for
//!     example revoada's configured voters) counts through this code
//!     instead of re-deriving it.
//!
//! The fold reports one of three states ([`QuorumState`]):
//!
//!   * `Pending`: fewer distinct voters than the threshold.
//!   * `Reached`: at least the threshold, and every voter holds the
//!     same evidence.
//!   * `Dissent`: at least the threshold, but voters disagree on the
//!     evidence. The consumer must investigate (re-derivation,
//!     eviction, alert).
//!
//! ## What the types rule out
//!
//!   * **A zero threshold.** It is a [`NonZeroUsize`]. A zero-vote
//!     quorum would be satisfied by an empty set; the old constructor
//!     clamped it to one silently, and now the caller has to decide.
//!   * **A verdict nobody folded.** [`QuorumVerdict`]'s fields are
//!     private and [`Tally::verdict`] is its only constructor, so code
//!     outside this module cannot build one (E0451). A ledger cannot
//!     answer `Reached` from arithmetic of its own; the in-memory
//!     ledger's read path once did, reporting 1 of 3 confirmations as
//!     reached.
//!   * **A voter counted twice.** Votes are a map keyed by voter; a
//!     voter that votes again replaces its earlier evidence.
//!
//! What the types do not rule out: a caller can still hand the fold
//! the wrong threshold. That is checked by the tests at each call
//! site, not by a type.
//!
//! ## Why per-(kind, subject) tracking
//!
//! A node can confirm many things; a tracker is scoped to ONE
//! materialization target. Composition: a stage with N targets
//! holds N trackers.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use crate::receipt::{MaterializationReceipt, NodeId, ReceiptKind};

/// Which of the three states a quorum fold reached.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum QuorumState {
    /// Fewer distinct voters than the threshold.
    Pending,
    /// At least the threshold, and every voter holds the same evidence.
    Reached,
    /// At least the threshold, but voters disagree on the evidence.
    Dissent,
}

/// The verdict of the one quorum fold.
///
/// Sealed: every field is private and [`Tally::verdict`] is the only
/// constructor, so holding a `Reached` verdict means the fold ran on
/// the tracker's own threshold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QuorumVerdict {
    state: QuorumState,
    confirmed: usize,
    threshold: NonZeroUsize,
    evidence_variants: usize,
}

impl QuorumVerdict {
    /// Which of the three states the fold reached.
    #[must_use]
    pub fn state(&self) -> QuorumState {
        self.state
    }

    /// True when the state is [`QuorumState::Reached`].
    #[must_use]
    pub fn is_reached(&self) -> bool {
        self.state == QuorumState::Reached
    }

    /// Distinct voters counted.
    #[must_use]
    pub fn confirmed(&self) -> usize {
        self.confirmed
    }

    /// The threshold the fold counted against.
    #[must_use]
    pub fn threshold(&self) -> NonZeroUsize {
        self.threshold
    }

    /// Distinct pieces of evidence among the counted voters. 0 means
    /// no votes yet, 1 means every voter agrees, 2 or more is
    /// disagreement.
    #[must_use]
    pub fn evidence_variants(&self) -> usize {
        self.evidence_variants
    }
}

/// Distinct voters, each holding one piece of evidence, counted
/// against a non-zero threshold. The fold behind [`QuorumTracker`].
#[derive(Clone, Debug)]
pub struct Tally<V, E> {
    threshold: NonZeroUsize,
    /// `voter -> evidence`. The most recent vote per voter wins.
    votes: BTreeMap<V, E>,
}

impl<V: Ord, E: Ord> Tally<V, E> {
    /// An empty tally that needs `threshold` distinct voters.
    #[must_use]
    pub fn new(threshold: NonZeroUsize) -> Self {
        Self {
            threshold,
            votes: BTreeMap::new(),
        }
    }

    /// An empty tally that needs a strict majority of `voters`:
    /// `voters / 2 + 1` of them.
    #[must_use]
    pub fn majority_of(voters: NonZeroUsize) -> Self {
        Self::new(NonZeroUsize::MIN.saturating_add(voters.get() / 2))
    }

    /// The threshold this tally counts against.
    #[must_use]
    pub fn threshold(&self) -> NonZeroUsize {
        self.threshold
    }

    /// Record `voter`'s evidence and return the verdict after it. A
    /// voter that votes again replaces its earlier evidence (the voter
    /// is the authority for its own claim).
    pub fn record(&mut self, voter: V, evidence: E) -> QuorumVerdict {
        self.votes.insert(voter, evidence);
        self.verdict()
    }

    /// Distinct voters counted so far.
    #[must_use]
    pub fn confirmed_count(&self) -> usize {
        self.votes.len()
    }

    /// Distinct pieces of evidence among the counted voters.
    #[must_use]
    pub fn evidence_variants(&self) -> usize {
        self.votes.values().collect::<BTreeSet<_>>().len()
    }

    /// The distinct voters counted so far, in order.
    pub fn voters(&self) -> impl Iterator<Item = &V> {
        self.votes.keys()
    }

    /// Forget every vote. The threshold stays.
    pub fn reset(&mut self) {
        self.votes.clear();
    }

    /// The fold: the only place a [`QuorumVerdict`] is built.
    #[must_use]
    pub fn verdict(&self) -> QuorumVerdict {
        let confirmed = self.confirmed_count();
        let evidence_variants = self.evidence_variants();
        let state = if confirmed < self.threshold.get() {
            QuorumState::Pending
        } else if evidence_variants > 1 {
            QuorumState::Dissent
        } else {
            QuorumState::Reached
        };
        QuorumVerdict {
            state,
            confirmed,
            threshold: self.threshold,
            evidence_variants,
        }
    }
}

/// Per-target receipt accumulator: a [`Tally`] of emitters and their
/// evidence hashes, scoped to one `(kind, subject)` pair.
#[derive(Debug, Clone)]
pub struct QuorumTracker {
    kind: ReceiptKind,
    subject: [u8; 32],
    tally: Tally<NodeId, [u8; 32]>,
}

impl QuorumTracker {
    /// New tracker for `(kind, subject)` that needs `threshold`
    /// distinct emitters.
    #[must_use]
    pub fn new(kind: ReceiptKind, subject: [u8; 32], threshold: NonZeroUsize) -> Self {
        Self {
            kind,
            subject,
            tally: Tally::new(threshold),
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

    /// The threshold the tracker counts against.
    #[must_use]
    pub fn threshold(&self) -> NonZeroUsize {
        self.tally.threshold()
    }

    /// Current distinct-emitter count.
    #[must_use]
    pub fn confirmed_count(&self) -> usize {
        self.tally.confirmed_count()
    }

    /// True once the fold is past `Pending` (enough distinct emitters),
    /// whether or not their evidence agrees.
    #[must_use]
    pub fn has_quorum(&self) -> bool {
        self.verdict().state() != QuorumState::Pending
    }

    /// Distinct evidence-hash count. 1 = all emitters agree.
    /// 2+ = dissent. 0 = no receipts yet.
    #[must_use]
    pub fn evidence_variants(&self) -> usize {
        self.tally.evidence_variants()
    }

    /// Snapshot of distinct confirming emitters.
    #[must_use]
    pub fn emitters(&self) -> Vec<NodeId> {
        self.tally.voters().copied().collect()
    }

    /// Ingest a receipt. Returns the post-ingest verdict.
    ///
    /// Receipts for a DIFFERENT (kind, subject) are ignored: callers
    /// route per tracker. Re-emission from the same emitter overwrites
    /// the prior evidence hash (the emitter is the authority for its
    /// own claim).
    pub fn ingest(&mut self, receipt: &MaterializationReceipt) -> QuorumVerdict {
        if receipt.kind != self.kind || receipt.subject != self.subject {
            return self.verdict();
        }
        self.tally.record(receipt.emitter, receipt.evidence_hash)
    }

    /// The current verdict, without ingesting anything.
    #[must_use]
    pub fn verdict(&self) -> QuorumVerdict {
        self.tally.verdict()
    }

    /// Forget every confirmation — useful after evicting a target.
    pub fn reset(&mut self) {
        self.tally.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).expect("test thresholds are non-zero")
    }

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

    fn tracker(threshold: usize) -> QuorumTracker {
        QuorumTracker::new(ReceiptKind::Drv, [1u8; 32], nz(threshold))
    }

    #[test]
    fn starts_pending_with_zero_confirmations() {
        let v = tracker(3).verdict();
        assert_eq!(v.state(), QuorumState::Pending);
        assert_eq!(v.confirmed(), 0);
        assert_eq!(v.threshold(), nz(3));
        assert_eq!(v.evidence_variants(), 0);
    }

    #[test]
    fn reaches_quorum_when_threshold_distinct_emitters_agree() {
        let mut t = tracker(3);
        let v1 = t.ingest(&r(1, 5));
        assert_eq!((v1.state(), v1.confirmed()), (QuorumState::Pending, 1));
        let v2 = t.ingest(&r(2, 5));
        assert_eq!((v2.state(), v2.confirmed()), (QuorumState::Pending, 2));
        let v3 = t.ingest(&r(3, 5));
        assert!(v3.is_reached());
        assert_eq!((v3.confirmed(), v3.threshold()), (3, nz(3)));
    }

    #[test]
    fn duplicate_emitter_doesnt_increment_count() {
        let mut t = tracker(3);
        t.ingest(&r(1, 5));
        t.ingest(&r(1, 5)); // same emitter, same evidence
        t.ingest(&r(1, 5));
        assert_eq!(t.confirmed_count(), 1);
        assert!(!t.has_quorum());
    }

    #[test]
    fn dissent_when_evidence_variants_diverge_past_quorum() {
        let mut t = tracker(2);
        t.ingest(&r(1, 5));
        let v = t.ingest(&r(2, 6)); // different evidence
        assert_eq!(v.state(), QuorumState::Dissent);
        assert_eq!((v.confirmed(), v.evidence_variants()), (2, 2));
    }

    #[test]
    fn dissent_overrides_reached_when_thresholds_both_met() {
        let mut t = tracker(2);
        t.ingest(&r(1, 5));
        assert!(t.ingest(&r(2, 5)).is_reached());
        let v = t.ingest(&r(3, 7)); // Adds dissent.
        assert_eq!(v.state(), QuorumState::Dissent);
    }

    #[test]
    fn disagreement_below_threshold_is_still_pending() {
        let mut t = tracker(3);
        t.ingest(&r(1, 5));
        let v = t.ingest(&r(2, 6));
        assert_eq!(v.state(), QuorumState::Pending);
        assert_eq!(v.evidence_variants(), 2);
    }

    #[test]
    fn most_recent_evidence_per_emitter_wins() {
        let mut t = tracker(1);
        t.ingest(&r(1, 5));
        t.ingest(&r(1, 6)); // emitter 1 changes its mind
        assert_eq!(t.confirmed_count(), 1);
        // Only one evidence hash now → no dissent.
        assert_eq!(t.evidence_variants(), 1);
        assert!(t.verdict().is_reached());
    }

    #[test]
    fn ignores_receipts_with_different_subject() {
        let mut t = tracker(1);
        t.ingest(&other_subject_receipt(1));
        assert_eq!(t.confirmed_count(), 0);
    }

    #[test]
    fn ignores_receipts_with_different_kind() {
        let mut t = tracker(1);
        t.ingest(&other_kind_receipt(1));
        assert_eq!(t.confirmed_count(), 0);
    }

    #[test]
    fn verdict_reports_the_trackers_own_threshold() {
        let mut t = tracker(5);
        let v = t.ingest(&r(1, 5));
        assert_eq!(v.threshold(), nz(5));
        assert_eq!(t.threshold(), nz(5));
        assert_eq!(
            t.verdict(),
            v,
            "reading the verdict is the same fold as ingesting"
        );
    }

    #[test]
    fn reset_clears_state() {
        let mut t = tracker(2);
        t.ingest(&r(1, 5));
        t.ingest(&r(2, 5));
        assert!(t.has_quorum());
        t.reset();
        assert_eq!(t.confirmed_count(), 0);
        assert!(!t.has_quorum());
        assert_eq!(t.threshold(), nz(2), "reset keeps the threshold");
    }

    #[test]
    fn emitters_returns_distinct_set() {
        let mut t = tracker(3);
        t.ingest(&r(1, 5));
        t.ingest(&r(2, 5));
        t.ingest(&r(1, 5)); // duplicate
        assert_eq!(t.emitters().len(), 2);
    }

    #[test]
    fn majority_of_is_a_strict_majority() {
        let cases = [(1, 1), (2, 2), (3, 2), (4, 3), (5, 3), (6, 4), (7, 4)];
        for (voters, needed) in cases {
            let t = Tally::<u8, ()>::majority_of(nz(voters));
            assert_eq!(t.threshold(), nz(needed), "majority of {voters}");
        }
    }

    #[test]
    fn tally_counts_distinct_voters_once() {
        let mut t = Tally::majority_of(nz(5));
        for voter in [1u8, 1, 2, 2] {
            t.record(voter, ());
        }
        assert_eq!(t.verdict().state(), QuorumState::Pending);
        assert!(t.record(3, ()).is_reached());
    }
}
