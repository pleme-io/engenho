//! COUNTS COME FROM EFFECTS — a change is counted only when a write landed.
//!
//! ★ THE DEFECT. A tick reports how many objects it changed, and every
//! site used to count the same way: propose a command, `?` the transport
//! error, add one. The [`ResourceOp`] the store sent back was never read,
//! so a write the store answered with "nothing happened" was reported as a
//! change:
//!
//!   * GC deleting an orphan that bears a finalizer, with no clock to stamp
//!     (the store leaves the object untouched: `NoOp`), counted one change
//!     per tick, forever;
//!   * a delete of an object already gone (`NoOp`) counted one;
//!   * `write_status_cas` read only `Conflict`, so a status patch the store
//!     REFUSED (`PatchRejected`, `ApplyConflict`) came back as `Written`;
//!   * a datapath removal whose error was discarded (`let _ = remove`)
//!     counted one.
//!
//! ★ THE SHAPE.
//!
//!   * [`Effect::of`] reads the op by an exhaustive match with no wildcard,
//!     so a new `ResourceOp` variant does not compile (E0004) until someone
//!     says whether it changed the catalog.
//!   * [`Effect::applied`] is the same answer for a datapath backend (a
//!     router, an enforcer, a DNS table): `Ok` landed, and the error goes
//!     back to the caller uncounted.
//!   * [`crate::ReconcileReport::record`] counts an `Effect`;
//!     `ObjectOutcome::from(Effect)` turns one into a sweep outcome.
//!   * [`Landed`] has no public constructor. `ObjectOutcome::Changed` holds
//!     one, so a sweep cannot report an object as changed without an
//!     `Effect` that said a write landed.
//!
//! Tier-honest: in a controller that runs through [`crate::Sweep`], a
//! change nobody observed is a compile error (no `Landed`, no `Changed`).
//! In the legacy counter path `ReconcileReport::objects_changed` is still a
//! public field, which the kubelet and the scheduler write directly; there a
//! bare `+= 1` is caught only by review.

use std::fmt;

use engenho_store::command::ResourceOp;

/// What one write did — to the store's catalog, or to a datapath backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[must_use = "an effect that is not recorded is a change nobody counts"]
pub enum Effect {
    /// The write landed: the catalog changed (a revision was consumed and a
    /// watch event fired), or the backend call returned `Ok`.
    Written(Landed),
    /// The store accepted the command and changed nothing: a delete of an
    /// absent key, a delete of an object already Terminating, a delete of a
    /// finalizer-bearing object with no timestamp to stamp, an
    /// unconditional patch of a missing key.
    #[default]
    Unchanged,
    /// The store refused the command. The catalog is byte-identical.
    Rejected(Refusal),
}

/// Proof that a write landed. Its field is private: [`Effect::of`] and
/// [`Effect::applied`] are the only ways to get one, so holding a `Landed`
/// means an observed write said so.
///
/// A sweep outcome of `Changed` is built from an effect:
///
/// ```
/// use engenho_controllers::{Effect, ObjectOutcome};
/// use engenho_store::command::ResourceOp;
///
/// let outcome = ObjectOutcome::from(Effect::of(ResourceOp::Created));
/// assert!(matches!(outcome, ObjectOutcome::Changed(_)));
/// ```
///
/// and never claimed without one. This differs from the example above only
/// in how the `Landed` is obtained, and does not compile:
///
/// ```compile_fail
/// use engenho_controllers::{Landed, ObjectOutcome};
///
/// let outcome = ObjectOutcome::Changed(Landed(()));
/// assert!(matches!(outcome, ObjectOutcome::Changed(_)));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Landed(());

/// Why the store refused a write. One arm per refusing [`ResourceOp`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// The CAS precondition failed: the object moved since it was read.
    /// The write that moved it fires a watch event, which re-ticks.
    Conflict,
    /// The patch algorithm refused the patch body.
    PatchRejected,
    /// A server-side apply hit a field owned by another manager.
    ApplyConflict,
}

impl Refusal {
    /// A fixed description, used as a sweep's skip note.
    #[must_use]
    pub const fn note(self) -> &'static str {
        match self {
            Self::Conflict => "the store refused the write: the object changed since it was read",
            Self::PatchRejected => "the store refused the write: the patch body was rejected",
            Self::ApplyConflict => {
                "the store refused the write: a field is owned by another manager"
            }
        }
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.note())
    }
}

impl Effect {
    /// What the store did, read off the op it returned.
    ///
    /// Exhaustive with no wildcard: a new [`ResourceOp`] variant does not
    /// compile until it is placed here.
    pub const fn of(op: ResourceOp) -> Self {
        match op {
            ResourceOp::Created
            | ResourceOp::Replaced
            | ResourceOp::Patched
            | ResourceOp::Deleted
            | ResourceOp::DeletionPending => Self::Written(Landed(())),
            ResourceOp::NoOp => Self::Unchanged,
            ResourceOp::Conflict => Self::Rejected(Refusal::Conflict),
            ResourceOp::PatchRejected => Self::Rejected(Refusal::PatchRejected),
            ResourceOp::ApplyConflict => Self::Rejected(Refusal::ApplyConflict),
        }
    }

    /// What a datapath backend call did: `Ok` landed.
    ///
    /// # Errors
    ///
    /// The backend's own error, unchanged and uncounted. The caller decides
    /// whether it ends the object, the sweep, or nothing.
    pub fn applied<E>(result: Result<(), E>) -> Result<Self, E> {
        result.map(|()| Self::Written(Landed(())))
    }

    /// The effect of two writes made for the same object: it changed if
    /// either landed; otherwise it was refused if either was refused (the
    /// first refusal is kept); otherwise nothing changed.
    pub const fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::Written(landed), _) | (_, Self::Written(landed)) => Self::Written(landed),
            (Self::Rejected(refusal), _) | (_, Self::Rejected(refusal)) => Self::Rejected(refusal),
            (Self::Unchanged, Self::Unchanged) => Self::Unchanged,
        }
    }

    /// True iff the write landed.
    #[must_use]
    pub const fn landed(self) -> bool {
        matches!(self, Self::Written(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every op, and what it means. Written as a table so a new variant is
    /// a new row here as well as an arm in `of`.
    const EVERY_OP: [(ResourceOp, Effect); 9] = [
        (ResourceOp::Created, Effect::Written(Landed(()))),
        (ResourceOp::Replaced, Effect::Written(Landed(()))),
        (ResourceOp::Patched, Effect::Written(Landed(()))),
        (ResourceOp::Deleted, Effect::Written(Landed(()))),
        (ResourceOp::DeletionPending, Effect::Written(Landed(()))),
        (ResourceOp::NoOp, Effect::Unchanged),
        (ResourceOp::Conflict, Effect::Rejected(Refusal::Conflict)),
        (
            ResourceOp::PatchRejected,
            Effect::Rejected(Refusal::PatchRejected),
        ),
        (
            ResourceOp::ApplyConflict,
            Effect::Rejected(Refusal::ApplyConflict),
        ),
    ];

    #[test]
    fn only_an_op_that_changed_the_catalog_landed() {
        for (op, expected) in EVERY_OP {
            assert_eq!(Effect::of(op), expected, "{op:?}");
            let changed_the_catalog = matches!(
                op,
                ResourceOp::Created
                    | ResourceOp::Replaced
                    | ResourceOp::Patched
                    | ResourceOp::Deleted
                    | ResourceOp::DeletionPending
            );
            assert_eq!(Effect::of(op).landed(), changed_the_catalog, "{op:?}");
        }
    }

    #[test]
    fn a_no_op_is_unchanged_and_a_refusal_is_rejected() {
        assert_eq!(Effect::of(ResourceOp::NoOp), Effect::Unchanged);
        for op in [
            ResourceOp::Conflict,
            ResourceOp::PatchRejected,
            ResourceOp::ApplyConflict,
        ] {
            assert!(
                matches!(Effect::of(op), Effect::Rejected(_)),
                "{op:?} is a refusal"
            );
        }
    }

    #[test]
    fn a_backend_error_is_returned_and_never_landed() {
        assert!(Effect::applied(Ok::<(), &str>(())).unwrap().landed());
        assert_eq!(Effect::applied(Err::<(), _>("rule busy")), Err("rule busy"));
    }

    #[test]
    fn two_writes_for_one_object_changed_it_if_either_landed() {
        let written = Effect::of(ResourceOp::Created);
        let refused = Effect::of(ResourceOp::Conflict);
        let unchanged = Effect::Unchanged;
        for (a, b, expected) in [
            (written, unchanged, written),
            (unchanged, written, written),
            (refused, written, written),
            (refused, unchanged, refused),
            (unchanged, refused, refused),
            (unchanged, unchanged, unchanged),
        ] {
            assert_eq!(a.and(b), expected, "{a:?} and {b:?}");
        }
        assert_eq!(
            Effect::of(ResourceOp::Conflict).and(Effect::of(ResourceOp::PatchRejected)),
            Effect::Rejected(Refusal::Conflict),
            "the first refusal is kept"
        );
    }

    #[test]
    fn no_write_is_the_default() {
        assert_eq!(Effect::default(), Effect::Unchanged);
        assert!(!Effect::default().landed());
    }
}
