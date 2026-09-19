//! `WatchHistory`: the watch-replay ring, its compaction floor, the head
//! revision and the ring's capacity, held as ONE value (T3.3 seal).
//!
//! ## The promise
//!
//! The floor says: every change committed above me is in the ring, and
//! nothing at or below me is. So the ring holds exactly the revisions
//! `floor+1 ..= head`, each one whole and in order, and never more than
//! `capacity` changes. [`WatchHistory::split`] is the one place a resume
//! point is judged against that promise. The watch replay, the etcd façade's
//! prefix filter and the historical reads all go through it.
//!
//! ## Why one type
//!
//! These were four sibling `pub` fields of the catalog (`current_revision`,
//! `history`, `compacted_revision`, `history_capacity`), so any line in the
//! crate could move one without the others. A restarted store holding floor 0
//! over an empty ring at revision 57 was exactly that: a floor the ring did
//! not back. It answered `changes_since(0)` with nothing, and a client read
//! that as "nothing happened". Now the fields are private to this module, and
//! the only ways to change them are these four:
//!
//!   * [`WatchHistory::new`]: nothing committed yet, so floor = head = 0.
//!   * [`WatchHistory::loaded_at`]: a catalog read back from disk or from a
//!     snapshot. The ring does not make that trip, so floor = head.
//!   * [`WatchHistory::commit`]: one revision, whole. The head advances by
//!     exactly one, then the oldest whole revisions leave until the ring
//!     fits, and the floor rises to the last one to leave.
//!   * [`WatchHistory::compact`]: etcd's `Compact`. The floor rises to the
//!     target, clamped to the head, and never moves back down.
//!
//! Each keeps the promise. A floor the ring does not back has no code path,
//! and neither does a ring over its capacity or a transaction kept in part.
//!
//! ## Tier
//!
//! Module privacy is the seal: no module but this one can name a field, so
//! no other code can build a history by hand or move one field alone. The
//! compiler refuses it. The type is crate-private, which keeps T3.2b's seal
//! on the ring (a `pub fn` that hands it out is a `private_interfaces`
//! error). That also means a `compile_fail` doctest could not name the type
//! at all and would pass for the wrong reason, so
//! `the_fields_are_private_to_this_module` below is the gate that keeps the
//! fields private. Inside this module the four operations are checked
//! against every operation sequence by
//! `every_sequence_keeps_the_ring_backing_its_floor`.
//!
//! One contract is debug-asserted rather than typed: each change handed to
//! [`WatchHistory::commit`] carries the revision [`WatchHistory::next_revision`]
//! named. The catalog stamps every change of a command from that one call.

use std::collections::{VecDeque, vec_deque};
use std::num::NonZeroUsize;

use crate::revision::{Change, CompactedTooOld, Revision};

/// Default bound on the history ring. 8192 committed changes cover the
/// recent window plus the live tail every local watch consumer replays;
/// older revisions fall to the compaction floor.
pub const DEFAULT_HISTORY_CAPACITY: usize = 8192;

/// [`DEFAULT_HISTORY_CAPACITY`] as the type the ring is bounded by.
pub(crate) const DEFAULT_CAPACITY: NonZeroUsize = match NonZeroUsize::new(DEFAULT_HISTORY_CAPACITY)
{
    Some(capacity) => capacity,
    None => NonZeroUsize::MIN,
};

/// The watch-replay ring together with the floor it backs, the head it runs
/// up to, and the most changes it may hold. See the module docs for the
/// promise and for the four operations that keep it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct WatchHistory {
    /// Every change above `floor`, oldest first, grouped by revision. Never
    /// longer than `capacity`.
    ring: VecDeque<Change>,
    /// The compaction floor. Every change above it is in `ring`; none at or
    /// below it is.
    floor: Revision,
    /// The newest committed revision: the catalog's current revision.
    head: Revision,
    /// The most changes `ring` may hold.
    capacity: NonZeroUsize,
}

impl Default for WatchHistory {
    fn default() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }
}

impl WatchHistory {
    /// A history with nothing committed: floor and head both at
    /// [`Revision::ZERO`], so every revision is still replayable.
    #[must_use]
    pub(crate) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            ring: VecDeque::new(),
            floor: Revision::ZERO,
            head: Revision::ZERO,
            capacity,
        }
    }

    /// The history of a catalog read back at `head` from disk or from a
    /// snapshot.
    ///
    /// The ring is never persisted, so nothing at or below `head` can be
    /// replayed, and the floor is `head` itself. Whatever floor the writer
    /// held described a ring that did not make the trip. Every change
    /// committed after the load lands above it and is replayable.
    #[must_use]
    pub(crate) fn loaded_at(head: Revision, capacity: NonZeroUsize) -> Self {
        Self {
            ring: VecDeque::new(),
            floor: head,
            head,
            capacity,
        }
    }

    /// The newest committed revision.
    #[must_use]
    pub(crate) fn head(&self) -> Revision {
        self.head
    }

    /// The compaction floor: the oldest revision a watch may resume from.
    #[must_use]
    pub(crate) fn floor(&self) -> Revision {
        self.floor
    }

    /// The revision the next [`Self::commit`] records.
    #[must_use]
    pub(crate) fn next_revision(&self) -> Revision {
        self.head.next()
    }

    /// The retained changes split at `rv`: those at or below it, then those
    /// strictly after it, both oldest first. The second half is what a
    /// client that last saw `rv` must replay to be caught up.
    ///
    /// # Errors
    ///
    /// [`CompactedTooOld`] when `rv` is below the floor: the changes between
    /// `rv` and the floor are gone, so no replay from `rv` can be complete.
    /// Resuming from exactly the floor, or from anywhere above it, is always
    /// honoured, and past the head it replays nothing.
    pub(crate) fn split(
        &self,
        rv: Revision,
    ) -> Result<(vec_deque::Iter<'_, Change>, vec_deque::Iter<'_, Change>), CompactedTooOld> {
        if rv < self.floor {
            return Err(CompactedTooOld {
                requested: rv,
                compacted: self.floor,
            });
        }
        // The ring is in revision order, so the boundary is a binary search.
        let boundary = self.ring.partition_point(|c| c.revision <= rv);
        Ok((self.ring.range(..boundary), self.ring.range(boundary..)))
    }

    /// Every retained change strictly after `rv`, oldest first. The
    /// watch-replay window; see [`Self::split`].
    ///
    /// # Errors
    ///
    /// [`CompactedTooOld`] when `rv` is below the floor.
    pub(crate) fn since(
        &self,
        rv: Revision,
    ) -> Result<vec_deque::Iter<'_, Change>, CompactedTooOld> {
        self.split(rv).map(|(_, after)| after)
    }

    /// Record one committed revision and advance the head to it: `first`,
    /// plus every further key a transaction touched at the same revision.
    /// Returns the revision recorded.
    ///
    /// The revision goes in whole. If the ring then holds more than
    /// `capacity` changes, the oldest revisions leave it whole too, oldest
    /// first, and the floor rises to the last one to leave. So a
    /// transaction is never kept in part, and nothing at or below the floor
    /// is ever retained. A revision with more changes than the whole ring
    /// evicts itself: the ring empties and the floor reaches the head, the
    /// honest answer when the ring cannot hold it.
    pub(crate) fn commit(&mut self, first: &Change, rest: &[Change]) -> Revision {
        let revision = self.next_revision();
        debug_assert!(
            std::iter::once(first)
                .chain(rest)
                .all(|c| c.revision == revision),
            "every change of one commit carries the revision next_revision() named"
        );
        self.ring.push_back(first.clone());
        self.ring.extend(rest.iter().cloned());
        self.head = revision;
        while self.ring.len() > self.capacity.get() {
            self.evict_oldest_revision();
        }
        revision
    }

    /// Drop every change of the oldest retained revision and raise the floor
    /// to it.
    fn evict_oldest_revision(&mut self) {
        let Some(oldest) = self.ring.front().map(|c| c.revision) else {
            return;
        };
        let end = self.ring.partition_point(|c| c.revision <= oldest);
        self.ring.drain(..end);
        self.floor = oldest;
    }

    /// etcd's `Compact`: discard every retained change at or below `target`
    /// and raise the floor to it. Returns the floor afterwards.
    ///
    /// A target above the head is clamped to it, since revisions that do not
    /// exist yet cannot be compacted away. A target at or below the floor
    /// changes nothing: the floor only rises, because lowering it would
    /// promise history that is already gone.
    pub(crate) fn compact(&mut self, target: Revision) -> Revision {
        let target = target.min(self.head);
        if target > self.floor {
            let end = self.ring.partition_point(|c| c.revision <= target);
            self.ring.drain(..end);
            self.floor = target;
        }
        self.floor
    }

    /// The most changes the ring may hold.
    #[cfg(test)]
    pub(crate) fn capacity(&self) -> NonZeroUsize {
        self.capacity
    }

    /// How many changes the ring holds.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.ring.len()
    }

    /// Whether the ring holds no change.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    /// Every retained change, oldest first.
    #[cfg(test)]
    pub(crate) fn iter(&self) -> vec_deque::Iter<'_, Change> {
        self.ring.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::ResourceKey;
    use crate::revision::{ChangeKind, VersionMeta};
    use proptest::prelude::*;

    fn cap(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).expect("a test capacity is non-zero")
    }

    /// One change to key `name`, stamped at `revision`.
    fn change(revision: Revision, name: &str) -> Change {
        Change {
            revision,
            key: ResourceKey::namespaced("", "v1", "ConfigMap", "default", name),
            kind: ChangeKind::Put,
            value: serde_json::json!({ "name": name }),
            prior: None,
            version_meta: VersionMeta::created_at(revision),
        }
    }

    /// Commit one revision touching `keys` keys; returns what was committed.
    fn commit_n(h: &mut WatchHistory, keys: usize) -> Vec<Change> {
        let revision = h.next_revision();
        let group: Vec<Change> = (0..keys)
            .map(|i| change(revision, &format!("r{}-k{i}", revision.get())))
            .collect();
        let recorded = h.commit(&group[0], &group[1..]);
        assert_eq!(recorded, revision, "commit records the revision it named");
        group
    }

    fn revisions<'a>(changes: impl Iterator<Item = &'a Change>) -> Vec<u64> {
        changes.map(|c| c.revision.get()).collect()
    }

    /// The seal, checked from the outside: the ring fits, the floor is at or
    /// below the head, the ring holds exactly the revisions above the floor,
    /// and a replay from the floor is the whole ring.
    fn assert_sealed(h: &WatchHistory) -> Result<(), TestCaseError> {
        prop_assert!(
            h.len() <= h.capacity().get(),
            "the ring is over capacity: {h:?}"
        );
        prop_assert!(h.floor() <= h.head(), "the floor is above the head: {h:?}");
        let mut distinct = revisions(h.iter());
        prop_assert!(
            distinct.windows(2).all(|w| w[0] <= w[1]),
            "the ring is out of revision order: {h:?}"
        );
        distinct.dedup();
        let backed: Vec<u64> = (h.floor().get() + 1..=h.head().get()).collect();
        prop_assert_eq!(
            distinct,
            backed,
            "the ring must hold exactly the revisions above the floor, through the head"
        );
        prop_assert_eq!(
            h.since(h.floor()).map(|after| after.len()),
            Ok(h.len()),
            "a replay from the floor must be the whole ring"
        );
        if let Some(below) = h.floor().get().checked_sub(1) {
            prop_assert_eq!(
                h.since(Revision(below)).err(),
                Some(CompactedTooOld {
                    requested: Revision(below),
                    compacted: h.floor(),
                })
            );
        }
        Ok(())
    }

    #[test]
    fn a_new_history_has_committed_nothing_and_refuses_nothing() {
        let h = WatchHistory::new(cap(4));
        assert_eq!((h.floor(), h.head()), (Revision::ZERO, Revision::ZERO));
        assert_eq!(h.next_revision(), Revision(1));
        assert_eq!(h.since(Revision::ZERO).map(|after| after.len()), Ok(0));
        assert_eq!(WatchHistory::default().capacity(), DEFAULT_CAPACITY);
        assert_eq!(DEFAULT_CAPACITY.get(), DEFAULT_HISTORY_CAPACITY);
    }

    #[test]
    fn a_loaded_history_refuses_every_resume_point_below_its_head() {
        let mut h = WatchHistory::loaded_at(Revision(57), cap(4));
        assert_eq!((h.floor(), h.head()), (Revision(57), Revision(57)));
        for from in [0, 1, 56] {
            assert_eq!(
                h.since(Revision(from)).err(),
                Some(CompactedTooOld {
                    requested: Revision(from),
                    compacted: Revision(57),
                }),
                "resuming from {from} after a load must be a 410, not an empty replay"
            );
        }
        assert_eq!(h.since(Revision(57)).map(|after| after.len()), Ok(0));

        commit_n(&mut h, 1);
        assert_eq!(
            h.since(Revision(57)).map(revisions),
            Ok(vec![58]),
            "a change committed after the load is replayable from the load revision"
        );
    }

    #[test]
    fn one_commit_is_one_revision_however_many_keys_it_touched() {
        let mut h = WatchHistory::new(cap(8));
        commit_n(&mut h, 3);
        commit_n(&mut h, 1);
        assert_eq!(h.head(), Revision(2));
        assert_eq!(revisions(h.iter()), vec![1, 1, 1, 2]);
        assert_eq!(h.since(Revision(1)).map(revisions), Ok(vec![2]));
    }

    /// Eviction takes the oldest revision whole. Change by change, the ring
    /// would keep one key of revision 2 at the floor, where no replay from
    /// the floor could ever reach it.
    #[test]
    fn eviction_takes_whole_revisions_so_a_transaction_is_never_kept_in_part() {
        let mut h = WatchHistory::new(cap(3));
        commit_n(&mut h, 1); // rev 1
        commit_n(&mut h, 2); // rev 2: the ring is full
        commit_n(&mut h, 2); // rev 3: revisions 1 and 2 must leave

        assert_eq!(h.floor(), Revision(2));
        assert_eq!(revisions(h.iter()), vec![3, 3]);
        assert_eq!(
            h.since(h.floor()).map(|after| after.len()),
            Ok(h.len()),
            "everything the ring retains is replayable from its floor"
        );
    }

    #[test]
    fn a_revision_larger_than_the_ring_is_not_kept_in_part() {
        let mut h = WatchHistory::new(cap(2));
        commit_n(&mut h, 3);
        assert!(
            h.is_empty(),
            "a revision that cannot fit is not kept at all"
        );
        assert_eq!((h.floor(), h.head()), (Revision(1), Revision(1)));
        assert_eq!(h.since(Revision(1)).map(|after| after.len()), Ok(0));
        assert!(h.since(Revision::ZERO).is_err());
    }

    #[test]
    fn compaction_only_rises_and_never_past_the_head() {
        let mut h = WatchHistory::new(cap(8));
        for _ in 0..4 {
            commit_n(&mut h, 1);
        }
        assert_eq!(h.compact(Revision(2)), Revision(2));
        assert_eq!(revisions(h.iter()), vec![3, 4]);
        assert_eq!(h.compact(Revision(1)), Revision(2), "never back down");
        assert_eq!(revisions(h.iter()), vec![3, 4]);
        assert_eq!(h.compact(Revision(99)), Revision(4), "clamped to the head");
        assert!(h.is_empty());
        commit_n(&mut h, 1);
        assert_eq!(
            h.since(Revision(4)).map(revisions),
            Ok(vec![5]),
            "a full compaction keeps committing above the floor"
        );
    }

    /// The gate for the seal a `compile_fail` doctest cannot reach (see the
    /// module docs): every field of `WatchHistory` stays private, so only
    /// this module can build one or move a field. The positive control is
    /// the field list itself, so a scan that found nothing fails.
    #[test]
    fn the_fields_are_private_to_this_module() {
        let source = include_str!("watch_history.rs");
        let body: Vec<&str> = source
            .lines()
            .skip_while(|line| *line != "pub(crate) struct WatchHistory {")
            .skip(1)
            .take_while(|line| *line != "}")
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with("//"))
            .collect();
        let names: Vec<&str> = body
            .iter()
            .filter_map(|field| field.split(':').next())
            .collect();
        assert_eq!(names, ["ring", "floor", "head", "capacity"]);
        for field in &body {
            assert!(
                !field.starts_with("pub"),
                "`{field}` would let code outside this module move one part of the history alone"
            );
        }
    }

    #[derive(Clone, Debug)]
    enum Op {
        /// Commit one revision touching this many keys.
        Commit(usize),
        /// Compact to this revision.
        Compact(u64),
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            4 => (1usize..=4).prop_map(Op::Commit),
            1 => (0u64..40).prop_map(Op::Compact),
        ]
    }

    proptest! {
        /// Every sequence of commits and compactions, from a fresh or a
        /// loaded history, at every capacity: the seal holds after each
        /// step, and the ring is exactly the log's changes above a floor
        /// that the plainest model of the rules computes.
        #[test]
        fn every_sequence_keeps_the_ring_backing_its_floor(
            capacity in 1usize..=6,
            loaded in prop::option::of(0u64..10),
            ops in prop::collection::vec(op(), 0..60),
        ) {
            let mut h = match loaded {
                Some(head) => WatchHistory::loaded_at(Revision(head), cap(capacity)),
                None => WatchHistory::new(cap(capacity)),
            };
            let mut log: Vec<Change> = Vec::new();
            let mut model_floor = h.floor();
            assert_sealed(&h)?;
            for op in ops {
                match op {
                    Op::Commit(keys) => {
                        log.extend(commit_n(&mut h, keys));
                        // Model: drop the oldest whole revision while the
                        // changes above the floor do not fit.
                        while log.iter().filter(|c| c.revision > model_floor).count() > capacity {
                            model_floor = log
                                .iter()
                                .map(|c| c.revision)
                                .find(|r| *r > model_floor)
                                .unwrap_or(model_floor);
                        }
                    }
                    Op::Compact(target) => {
                        let expected = model_floor.max(Revision(target).min(h.head()));
                        prop_assert_eq!(h.compact(Revision(target)), expected);
                        model_floor = expected;
                    }
                }
                assert_sealed(&h)?;
                prop_assert_eq!(h.floor(), model_floor);
                let above: Vec<&Change> = log.iter().filter(|c| c.revision > model_floor).collect();
                prop_assert_eq!(h.iter().collect::<Vec<_>>(), above);
                for rv in model_floor.get()..=h.head().get() + 1 {
                    let expected: Vec<&Change> = log.iter().filter(|c| c.revision.get() > rv).collect();
                    prop_assert_eq!(
                        h.since(Revision(rv)).map(Iterator::collect::<Vec<_>>),
                        Ok(expected)
                    );
                }
            }
        }
    }
}
