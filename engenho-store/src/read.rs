//! `ReadConsistency`: the store half of the Kubernetes read contract (T3.9b).
//!
//! A LIST or GET names how fresh its answer must be, with `resourceVersion`
//! and `resourceVersionMatch`. Before T3.9b the store had no way to hear
//! that: every read was served from the current catalog, whatever revision
//! the client asked for. So a LIST at `resourceVersion=N` against a replica
//! still at `M < N` answered with data older than the client had already
//! seen, and a LIST with `resourceVersionMatch=Exact` answered with the
//! present instead of the past it named. Both answers were 200s.
//!
//! Now a read carries a [`ReadConsistency`], and the store either serves it
//! or refuses it with a typed [`ReadRefused`]:
//!
//! | consistency | store at `head`, floor `floor` | answer |
//! |---|---|---|
//! | [`ReadConsistency::Latest`] | any | the catalog at `head` |
//! | [`ReadConsistency::NotOlderThan`]`(rv)` | `rv <= head` | the catalog at `head` |
//! | [`ReadConsistency::NotOlderThan`]`(rv)` | `rv > head` | [`ReadRefused::TooLarge`] |
//! | [`ReadConsistency::Exact`]`(rv)` | `floor <= rv <= head` | the catalog as it was at `rv` |
//! | [`ReadConsistency::Exact`]`(rv)` | `rv > head` | [`ReadRefused::TooLarge`] |
//! | [`ReadConsistency::Exact`]`(rv)` | `rv < floor` | [`ReadRefused::Expired`] |
//!
//! `NotOlderThan` below the floor is served: the present is not older than
//! any past, so nothing about the floor stops it.
//!
//! ## Upstream (kubernetes v1.34.0)
//!
//! * An rv the cache has not reached is waited for, up to `blockTimeout`
//!   (3 s), then refused with `504 Timeout` and the cause
//!   `ResourceVersionTooLarge`, message `Too large resource version: N,
//!   current: M` (`storage/errors.go` `NewTooLargeResourceVersionError`,
//!   `cacher/watch_cache.go` `waitUntilFreshAndBlock`). That holds for GET
//!   with an rv, LIST with `NotOlderThan`, and LIST with `Exact`
//!   (`waitAndListExactRV` waits first too). The wait lives on
//!   [`crate::StoreMesh`], which knows it is a replica that can lag; this
//!   module judges, and the judgement is taken under the catalog guard the
//!   read holds, so a rewind between the wait and the read is caught.
//! * An `Exact` rv whose history is gone answers `410 Expired`, `too old
//!   resource version: N` (`waitAndListExactRV`).
//!
//! ## What this module does not decide
//!
//! How the wire maps to a consistency (`resourceVersion` unset, `"0"`, a
//! number; `resourceVersionMatch`; the legacy rule that a limited LIST at a
//! non-zero rv is `Exact`; a continue token's own revision) is the
//! apiserver's parse, in one place, `ListWatchParams`. The store takes the
//! parsed value. `resourceVersion="0"` ("any, however stale") is served as
//! [`ReadConsistency::Latest`] here: every read comes from the one catalog,
//! and no older copy exists to serve instead.
//!
//! ## Tier
//!
//! A refused read has no path to data: [`ReadRefused`] is the whole `Err`
//! side, and the items only exist on the `Ok` side. That the judgement and
//! the read share one guard is a property of the catalog methods that take a
//! [`ReadConsistency`] (each judges and reads inside one `&self` call); it is
//! pinned by tests, not by a type. [`ReadConsistency::Latest`] is this
//! replica's newest applied revision: on the leader of a single-voter store
//! (engenho's deployed shape) that is the newest committed revision, on a
//! follower it is not a quorum read.

use crate::revision::{CompactedTooOld, Revision};

/// How fresh a read must be: the store's reading of `resourceVersion` +
/// `resourceVersionMatch`. See the module docs for the full table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ReadConsistency {
    /// The newest revision this replica has applied (`resourceVersion`
    /// unset, or `"0"`).
    Latest,
    /// Any revision at or after this one (`resourceVersion=N`, match unset
    /// or `NotOlderThan`). Served at the current revision once the store
    /// has reached it; refused as [`ReadRefused::TooLarge`] while it has not.
    NotOlderThan(Revision),
    /// Exactly this revision (`resourceVersion=N&resourceVersionMatch=Exact`,
    /// and every continue page of a series that began at `N`). Served as the
    /// catalog was at `N` while `N`'s history is retained.
    Exact(Revision),
}

/// Where a judged read is served from. Crate-private: only the catalog
/// method that judged it reads it, under the same guard.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReadPoint {
    /// The catalog as it is now, at its head revision.
    Head,
    /// The catalog as it was at this revision: at or above the floor and
    /// strictly below the head.
    Past(Revision),
}

impl ReadConsistency {
    /// The revision this replica must have applied before it can answer, if
    /// any: what [`crate::StoreMesh`] waits for before it reads.
    #[must_use]
    pub(crate) fn required(self) -> Option<Revision> {
        match self {
            Self::Latest => None,
            Self::NotOlderThan(rv) | Self::Exact(rv) => Some(rv),
        }
    }

    /// Judge this consistency against a catalog at `head` whose history
    /// reaches back to `floor` (`floor <= head`): the one place the table in
    /// the module docs is decided.
    ///
    /// # Errors
    ///
    /// * [`ReadRefused::TooLarge`] when the read needs a revision past
    ///   `head`.
    /// * [`ReadRefused::Expired`] when an exact read needs a revision below
    ///   `floor`.
    pub(crate) fn judge(self, head: Revision, floor: Revision) -> Result<ReadPoint, ReadRefused> {
        match self {
            Self::NotOlderThan(rv) | Self::Exact(rv) if rv > head => Err(ReadRefused::TooLarge {
                requested: rv,
                current: head,
            }),
            Self::Exact(rv) if rv < floor => Err(ReadRefused::Expired {
                requested: rv,
                compacted: floor,
            }),
            Self::Exact(rv) if rv < head => Ok(ReadPoint::Past(rv)),
            // The latest; not older than a revision the store has reached;
            // exactly the head.
            Self::Latest | Self::NotOlderThan(_) | Self::Exact(_) => Ok(ReadPoint::Head),
        }
    }
}

/// Why a read under a [`ReadConsistency`] was not served.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ReadRefused {
    /// The read needs a revision this store has not reached. Upstream's
    /// `504 Timeout` with cause `ResourceVersionTooLarge`; the message is
    /// upstream's own, so an apiserver that renders `Timeout: {this}` says
    /// exactly what kube-apiserver says, and client-go's too-large check
    /// matches it.
    #[error("Too large resource version: {requested}, current: {current}")]
    TooLarge {
        requested: Revision,
        current: Revision,
    },
    /// An exact read needs a revision below the compaction floor: the
    /// changes that would rewind the catalog to it are gone. Upstream's
    /// `410 Expired`.
    #[error("too old resource version: {requested} ({compacted})")]
    Expired {
        requested: Revision,
        compacted: Revision,
    },
}

engenho_substrate::impl_error_kind! {
    ReadRefused {
        { TooLarge { .. } } => "too_large",
        { Expired { .. } } => "expired",
    }
}

impl From<CompactedTooOld> for ReadRefused {
    fn from(e: CompactedTooOld) -> Self {
        Self::Expired {
            requested: e.requested,
            compacted: e.compacted,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// The upstream message, verbatim from the watch-410 oracle row
    /// `future_rv.watchlist_blocks_3s_then_504_too_large` (kubernetes
    /// v1.34.0 `storage/errors.go:233-242`, wrapped by
    /// `apimachinery errors.go:NewTimeoutError` in `Timeout: `).
    #[test]
    fn too_large_says_what_kube_apiserver_says() {
        let refused = ReadRefused::TooLarge {
            requested: Revision(105),
            current: Revision(100),
        };
        assert_eq!(
            ["Timeout: ", &refused.to_string()].concat(),
            "Timeout: Too large resource version: 105, current: 100"
        );
    }

    #[test]
    fn refusals_have_stable_kinds() {
        let too_large = ReadRefused::TooLarge {
            requested: Revision(2),
            current: Revision(1),
        };
        let expired = ReadRefused::Expired {
            requested: Revision(1),
            compacted: Revision(2),
        };
        assert_eq!(too_large.kind(), "too_large");
        assert_eq!(expired.kind(), "expired");
    }

    #[test]
    fn a_compacted_resume_point_is_an_expired_read() {
        assert_eq!(
            ReadRefused::from(CompactedTooOld {
                requested: Revision(3),
                compacted: Revision(7),
            }),
            ReadRefused::Expired {
                requested: Revision(3),
                compacted: Revision(7),
            }
        );
    }

    /// The module-doc table, row by row, over a store at head 10 whose
    /// history reaches back to 4.
    #[test]
    fn the_table_rows() {
        let (head, floor) = (Revision(10), Revision(4));
        let judge = |c: ReadConsistency| c.judge(head, floor);
        let too_large = |rv| {
            Err(ReadRefused::TooLarge {
                requested: Revision(rv),
                current: head,
            })
        };

        assert_eq!(judge(ReadConsistency::Latest), Ok(ReadPoint::Head));

        for rv in [0, 1, 3, 4, 9, 10] {
            assert_eq!(
                judge(ReadConsistency::NotOlderThan(Revision(rv))),
                Ok(ReadPoint::Head),
                "not older than {rv}: the present at 10 is not older than it, floor or no floor"
            );
        }
        assert_eq!(
            judge(ReadConsistency::NotOlderThan(Revision(11))),
            too_large(11)
        );

        assert_eq!(
            judge(ReadConsistency::Exact(Revision(10))),
            Ok(ReadPoint::Head)
        );
        for rv in 4..10 {
            assert_eq!(
                judge(ReadConsistency::Exact(Revision(rv))),
                Ok(ReadPoint::Past(Revision(rv)))
            );
        }
        assert_eq!(judge(ReadConsistency::Exact(Revision(11))), too_large(11));
        assert_eq!(
            judge(ReadConsistency::Exact(Revision(3))),
            Err(ReadRefused::Expired {
                requested: Revision(3),
                compacted: floor,
            })
        );
    }

    /// A store just loaded from disk has no history behind its head (floor
    /// equals head): the present is still readable exactly, and every past
    /// revision is refused rather than approximated by the present.
    #[test]
    fn a_store_with_no_history_serves_only_its_head_exactly() {
        let at = Revision(57);
        assert_eq!(
            ReadConsistency::Exact(at).judge(at, at),
            Ok(ReadPoint::Head)
        );
        assert_eq!(
            ReadConsistency::Exact(Revision(56)).judge(at, at),
            Err(ReadRefused::Expired {
                requested: Revision(56),
                compacted: at,
            })
        );
    }

    fn consistency() -> impl Strategy<Value = ReadConsistency> {
        prop_oneof![
            Just(ReadConsistency::Latest),
            (0u64..40).prop_map(|rv| ReadConsistency::NotOlderThan(Revision(rv))),
            (0u64..40).prop_map(|rv| ReadConsistency::Exact(Revision(rv))),
        ]
    }

    proptest! {
        /// Whatever the store and the request: a served read is never at a
        /// revision older than the request allows, never past the head, and a
        /// past read is never below the floor. A refusal names the head or the
        /// floor that caused it.
        #[test]
        fn a_served_read_keeps_the_promise_it_was_asked_for(
            floor in 0u64..30,
            above in 0u64..10,
            c in consistency(),
        ) {
            let (floor, head) = (Revision(floor), Revision(floor + above));
            match c.judge(head, floor) {
                Ok(ReadPoint::Head) => {
                    if let Some(rv) = c.required() {
                        prop_assert!(rv <= head, "{c:?} served at head {head} it has not reached");
                    }
                    if let ReadConsistency::Exact(rv) = c {
                        prop_assert_eq!(rv, head, "an exact read served from the head names the head");
                    }
                }
                Ok(ReadPoint::Past(rv)) => {
                    prop_assert_eq!(c, ReadConsistency::Exact(rv), "only an exact read reads the past");
                    prop_assert!(floor <= rv && rv < head, "a past read at {rv} outside [{floor}, {head})");
                }
                Err(ReadRefused::TooLarge { requested, current }) => {
                    prop_assert_eq!(Some(requested), c.required());
                    prop_assert_eq!(current, head);
                    prop_assert!(requested > head);
                }
                Err(ReadRefused::Expired { requested, compacted }) => {
                    prop_assert_eq!(c, ReadConsistency::Exact(requested));
                    prop_assert_eq!(compacted, floor);
                    prop_assert!(requested < floor);
                }
            }
        }
    }
}
