//! MVCC revision model — the etcd-equivalent versioning core for
//! engenho-store.
//!
//! This module replaces the R6 `resourceVersion = last-global-Raft-
//! log-index` hack with a true multi-version concurrency-control
//! revision model living inside the pure, network-free state
//! machine ([`crate::state::ResourceCatalog`]).
//!
//! ## Why this is the foundation
//!
//! Monotonic revisions are the precondition for:
//!
//!   * **resumable WATCH** — a client resumes a stream from a known
//!     revision; the store replays everything strictly after it.
//!   * **consistent list-then-watch** — `list()` captures the global
//!     [`Revision`] atomically; the subsequent watch resumes from
//!     exactly that point with no gap and no duplicate.
//!   * **optimistic concurrency** — a client's expected
//!     [`VersionMeta::mod_revision`] is compared against the stored
//!     one; mismatch ⇒ conflict (the 409 equivalent).
//!   * **compaction** — old revisions below a watermark can be
//!     reclaimed; reads below the watermark return a typed
//!     [`CompactedTooOld`] (the 410 Gone equivalent).
//!
//! ## Decoupling from the Raft log index
//!
//! The wire `metadata.resourceVersion` is now stamped from the
//! global [`Revision`], NOT the Raft log index. Blank entries
//! (openraft writes one on `initialize`), membership-change entries,
//! and no-op mutations (delete-not-found, patch-missing) advance the
//! Raft index but DO NOT consume a revision. This keeps the revision
//! space dense (no gaps) — the property resumable watch relies on.

use serde::{Deserialize, Serialize};

use crate::resource::{ResourceKey, ResourceValue};

/// A global, strictly-monotonic store revision.
///
/// `Revision(0)` is the "before any write" sentinel — a key that has
/// never been written has no `create_revision` and the catalog's
/// `current_revision` starts at `Revision(0)`. The first committed
/// mutation lands at `Revision(1)`.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Revision(pub u64);

impl Revision {
    /// The "before any write" sentinel.
    pub const ZERO: Revision = Revision(0);

    /// The raw counter value.
    #[must_use]
    pub fn get(self) -> u64 {
        self.0
    }

    /// The next revision (this + 1). Used by the apply path to
    /// advance the global counter by exactly one per real mutation.
    #[must_use]
    pub fn next(self) -> Revision {
        Revision(self.0 + 1)
    }
}

impl std::fmt::Display for Revision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Per-key version metadata — mirrors etcd's `(create_revision,
/// mod_revision, version)` triple.
///
///   * `create_revision` — the revision at which the key was (last)
///     created. Stable for a key's lifetime; RESETS to a fresh value
///     when a key is recreated after a delete.
///   * `mod_revision` — the revision of the most recent mutation.
///     Advances on every Put / Patch / recreate.
///   * `version` — the per-key bump count (etcd `Version`). Starts at
///     1 on create, increments by exactly 1 per mutation, RESETS to 1
///     on recreate-after-delete.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionMeta {
    pub create_revision: Revision,
    pub mod_revision: Revision,
    pub version: u64,
}

impl VersionMeta {
    /// Construct the metadata for a freshly-created key at `rev`.
    #[must_use]
    pub fn created_at(rev: Revision) -> Self {
        Self {
            create_revision: rev,
            mod_revision: rev,
            version: 1,
        }
    }

    /// Advance for an in-place mutation (replace / patch) at `rev`:
    /// `create_revision` is preserved, `mod_revision = rev`,
    /// `version += 1`.
    #[must_use]
    pub fn bumped_at(self, rev: Revision) -> Self {
        Self {
            create_revision: self.create_revision,
            mod_revision: rev,
            version: self.version + 1,
        }
    }
}

/// What kind of mutation a [`Change`] records.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    /// Create or replace (covers etcd PUT + our Put/Patch).
    Put,
    /// Tombstone — the key was removed.
    Delete,
}

/// One committed mutation in the catalog's history ring.
///
/// The watch backend replays the catalog by streaming `Change`s; the
/// durable backend persists them as the replayable tail.
///
///   * `value` is the POST-image. For a `Put` it is the new stored
///     object (already stamped with `resourceVersion` + `uid`). For a
///     `Delete` it is the tombstone — by convention the LAST-KNOWN
///     object (so watch consumers see what was deleted), which is the
///     same as `prior`.
///   * `prior` is the PRE-image: `None` on first create, `Some(old)`
///     on replace / patch / delete. This is the field that fixes the
///     "Deleted event carries Null" bug — the prior object is
///     captured BEFORE the mutation, inside the catalog, so it is
///     always the real object.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Change {
    /// The global revision this mutation was stamped with.
    pub revision: Revision,
    /// The key that was mutated.
    pub key: ResourceKey,
    /// Put or Delete.
    pub kind: ChangeKind,
    /// Post-image (tombstone == prior object for Delete).
    pub value: ResourceValue,
    /// Pre-image: `None` on first create, `Some(old)` otherwise.
    pub prior: Option<ResourceValue>,
    /// Per-key version metadata at this revision.
    pub version_meta: VersionMeta,
}

/// The typed "requested revision has been compacted away" error —
/// the 410 Gone equivalent. Returned by
/// [`crate::state::ResourceCatalog::changes_since`] when the caller
/// asks for history below the compaction watermark.
///
/// `requested` is the revision the caller passed; `compacted` is the
/// catalog's current `compacted_revision` (the lowest revision still
/// retained in the history ring).
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "requested revision {requested} has been compacted (lowest retained revision is {compacted})"
)]
pub struct CompactedTooOld {
    pub requested: Revision,
    pub compacted: Revision,
}

// `CompactedTooOld` is a struct (not an enum), so the variant-based
// `impl_error_kind!` macro doesn't apply. The inherent `kind()` +
// `ErrorKind` trait impl below match exactly what the macro emits for
// the single-shape case — same stable-identifier surface every other
// substrate error exposes.
impl CompactedTooOld {
    /// Stable identifier — see [`engenho_substrate::ErrorKind`].
    #[must_use]
    pub fn kind(&self) -> &'static str {
        "compacted_too_old"
    }
}

impl engenho_substrate::ErrorKind for CompactedTooOld {
    fn kind(&self) -> &'static str {
        Self::kind(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revision_next_advances_by_one() {
        assert_eq!(Revision::ZERO.next(), Revision(1));
        assert_eq!(Revision(41).next(), Revision(42));
    }

    #[test]
    fn revision_orders_numerically() {
        assert!(Revision(1) < Revision(2));
        assert!(Revision(10) > Revision(9));
    }

    #[test]
    fn version_meta_created_then_bumped() {
        let m = VersionMeta::created_at(Revision(5));
        assert_eq!(m.create_revision, Revision(5));
        assert_eq!(m.mod_revision, Revision(5));
        assert_eq!(m.version, 1);

        let m2 = m.bumped_at(Revision(8));
        // create_revision is stable across the bump
        assert_eq!(m2.create_revision, Revision(5));
        assert_eq!(m2.mod_revision, Revision(8));
        assert_eq!(m2.version, 2);
    }

    #[test]
    fn compacted_too_old_kind_is_stable() {
        let e = CompactedTooOld {
            requested: Revision(2),
            compacted: Revision(5),
        };
        assert_eq!(e.kind(), "compacted_too_old");
    }

    #[test]
    fn change_kind_round_trips() {
        for k in [ChangeKind::Put, ChangeKind::Delete] {
            let s = serde_json::to_string(&k).unwrap();
            let back: ChangeKind = serde_json::from_str(&s).unwrap();
            assert_eq!(back, k);
        }
    }
}
