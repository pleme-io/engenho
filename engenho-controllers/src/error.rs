//! Typed errors shared across controller impls.

use engenho_store::StoreError;
use shigoto_types::failure::FailureKind;

use crate::meta::ShapeError;

#[derive(Debug, thiserror::Error)]
pub enum ControllerError {
    #[error("store error during reconcile: {0}")]
    Store(#[from] StoreError),

    #[error("invalid resource: {0}")]
    InvalidResource(String),

    /// A stored object has the wrong JSON type where a controller has to
    /// write. Typed, not flattened into `InvalidResource`'s string, so the
    /// path and the type found survive to the Event.
    #[error("malformed object: {0}")]
    Shape(#[from] ShapeError),

    #[error("internal: {0}")]
    Internal(String),
}

engenho_substrate::impl_error_kind! {
    ControllerError {
        (Store(_)) => "store",
        (InvalidResource(_)) => "invalid_resource",
        (Shape(_)) => "shape",
        (Internal(_)) => "internal",
    }
}

impl ControllerError {
    /// Classify this error for the reconcile-loop retry decision.
    ///
    /// Decided by the VARIANT, never by the message. Both matches below
    /// are exhaustive with no wildcard, so a new `ControllerError` or
    /// `StoreError` variant does not compile (E0004) until someone says
    /// which class it is.
    ///
    /// The previous shape rendered `Store` and `Internal` to a string and
    /// ran it through `shigoto_types::failure::classify`'s signature list,
    /// so the class hung on wording: a `raft fatal` or `openraft config
    /// invalid` store error was Transient (no signature matched, and the
    /// default is Transient) and was retried forever, while an `Internal`
    /// whose text happened to contain "does not exist" was dropped as
    /// Declarative. The fleet classifier is right for what it was built
    /// for — raw Nix and tool output, where the string is all there is.
    /// Here the type already carries the answer.
    ///
    ///   * `InvalidResource` — Declarative. A malformed declaration does
    ///     not fix itself by being retried.
    ///   * `Shape` — Declarative, for the same reason: the object holds the
    ///     wrong JSON type until someone edits it.
    ///   * `Internal` — Transient. It wraps I/O and serialization failures
    ///     in a controller's own work (provisioning a directory, copying a
    ///     snapshot, a store round-trip); the conservative default, as in
    ///     shigoto: an extra retry on the growing curve is cheaper than
    ///     wedging a controller over a condition that would have cleared.
    ///   * `Store` — by [`store_class`].
    #[must_use]
    pub const fn classify(&self) -> FailureKind {
        match self {
            Self::Store(e) => store_class(e),
            Self::InvalidResource(_) | Self::Shape(_) => FailureKind::Declarative,
            Self::Internal(_) => FailureKind::Transient,
        }
    }
}

/// Where an error's damage stops, inside one sweep over many objects.
///
/// A sweep reconciles every object of a kind in one tick. Before this type
/// the only exit was `?`, which ended the WHOLE sweep: one PVC whose
/// backing directory could not be created kept every later PVC in the list
/// Pending, tick after tick, for as long as that one directory failed. The
/// scope says which failures are allowed to do that.
///
///   * `Item` — the failure belongs to one object: its declaration is
///     unusable, or a host effect made on its behalf failed. The next
///     object is unaffected, so the sweep records the failure against
///     this object and carries on.
///   * `Sweep` — the failure belongs to what every object shares: the
///     store. The next object's write would meet the same refusal, and a
///     sweep whose write did not land is acting on a view that is now
///     stale, so the sweep stops and the tick fails.
///
/// Read through [`ControllerError::scope`]; acted on by
/// [`crate::sweep::Sweep`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorScope {
    Item,
    Sweep,
}

impl ControllerError {
    /// How far this error's damage reaches inside a sweep.
    ///
    /// Decided by the VARIANT, like [`Self::classify`], and by an
    /// exhaustive match with no wildcard, so a new variant does not compile
    /// (E0004) until someone says whether it may stop a sweep.
    ///
    ///   * `Store` — Sweep, for every [`StoreError`]; see [`store_scope`].
    ///   * `InvalidResource`, `Shape` — Item. One object's declaration is
    ///     malformed; its neighbours' are not.
    ///   * `Internal` — Item. It wraps a controller's own work on one
    ///     object (a directory it provisions, a tree it copies).
    #[must_use]
    pub const fn scope(&self) -> ErrorScope {
        match self {
            Self::Store(e) => store_scope(e),
            Self::InvalidResource(_) | Self::Shape(_) | Self::Internal(_) => ErrorScope::Item,
        }
    }
}

/// The sweep scope of a store error: always [`ErrorScope::Sweep`].
///
/// Every variant is the store failing as a whole — a proposal that did not
/// commit, a store built wrong, a Raft core that stopped — never one key
/// being refused. Spelled out per variant rather than as `Store(_)` so a
/// per-key variant, if the store ever grows one, has to be placed.
const fn store_scope(e: &StoreError) -> ErrorScope {
    match e {
        StoreError::ClientWriteFailed(_)
        | StoreError::ConfigInvalid(_)
        | StoreError::InitializeFailed(_)
        | StoreError::Fatal(_)
        | StoreError::Persist(_) => ErrorScope::Sweep,
    }
}

/// The retry class of a store error.
///
///   * `ClientWriteFailed` — Transient. A proposal that did not commit
///     (no leader yet, a forward that failed, a quorum that was briefly
///     lost) is the store's ordinary weather; the next attempt may land.
///   * `ConfigInvalid`, `InitializeFailed`, `Fatal` — Declarative. The
///     store was built wrong or the Raft core has stopped; no reconcile
///     retry repairs that, and hammering it only buries the one log line
///     that says so.
///   * `Persist` — Transient. Only `StoreMesh::flush` returns it: the durable
///     image was not written, but the log still holds every applied entry,
///     so nothing is lost and a later flush can succeed (disk freed). No
///     controller calls flush today; the arm exists so the class is decided
///     rather than defaulted if one ever does.
const fn store_class(e: &StoreError) -> FailureKind {
    match e {
        StoreError::ClientWriteFailed(_) | StoreError::Persist(_) => FailureKind::Transient,
        StoreError::ConfigInvalid(_) | StoreError::InitializeFailed(_) | StoreError::Fatal(_) => {
            FailureKind::Declarative
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `StoreError::Persist`, built the way `FjallStore` builds one.
    fn persist() -> StoreError {
        StoreError::Persist(Box::new(openraft::StorageError::IO {
            source: openraft::StorageIOError::write(&openraft::AnyError::error("disk full")),
        }))
    }

    fn shape_error() -> ShapeError {
        let mut v = serde_json::json!({"metadata": "x"});
        crate::meta::object_mut(&mut v, &["metadata"]).unwrap_err()
    }

    #[test]
    fn error_kind_is_stable() {
        for (e, k) in [
            (
                ControllerError::Store(StoreError::ClientWriteFailed("x".into())),
                "store",
            ),
            (
                ControllerError::InvalidResource("x".into()),
                "invalid_resource",
            ),
            (ControllerError::Shape(shape_error()), "shape"),
            (ControllerError::Internal("x".into()), "internal"),
        ] {
            assert_eq!(e.kind(), k);
        }
    }

    /// One row per variant of both enums, in the order they are declared.
    ///
    /// The rows are built by an exhaustive match, so a new variant in
    /// either enum stops this test compiling until it has a row — the
    /// table cannot silently fall behind `classify`.
    fn every_variant() -> Vec<(ControllerError, FailureKind)> {
        fn row(e: ControllerError) -> (ControllerError, FailureKind) {
            let want = match &e {
                ControllerError::Store(s) => match s {
                    StoreError::ClientWriteFailed(_) | StoreError::Persist(_) => {
                        FailureKind::Transient
                    }
                    StoreError::ConfigInvalid(_)
                    | StoreError::InitializeFailed(_)
                    | StoreError::Fatal(_) => FailureKind::Declarative,
                },
                ControllerError::InvalidResource(_) | ControllerError::Shape(_) => {
                    FailureKind::Declarative
                }
                ControllerError::Internal(_) => FailureKind::Transient,
            };
            (e, want)
        }
        let m = String::new;
        vec![
            row(ControllerError::Store(StoreError::ConfigInvalid(m()))),
            row(ControllerError::Store(StoreError::InitializeFailed(m()))),
            row(ControllerError::Store(StoreError::ClientWriteFailed(m()))),
            row(ControllerError::Store(StoreError::Fatal(m()))),
            row(ControllerError::Store(persist())),
            row(ControllerError::InvalidResource(m())),
            row(ControllerError::Shape(shape_error())),
            row(ControllerError::Internal(m())),
        ]
    }

    /// One row per variant, in declaration order, with the scope it must
    /// have. Built by an exhaustive match for the same reason as
    /// `every_variant`: a new variant stops this compiling until it has a
    /// row.
    fn every_scope() -> Vec<(ControllerError, ErrorScope)> {
        fn row(e: ControllerError) -> (ControllerError, ErrorScope) {
            let want = match &e {
                ControllerError::Store(s) => match s {
                    StoreError::ClientWriteFailed(_)
                    | StoreError::ConfigInvalid(_)
                    | StoreError::InitializeFailed(_)
                    | StoreError::Fatal(_)
                    | StoreError::Persist(_) => ErrorScope::Sweep,
                },
                ControllerError::InvalidResource(_)
                | ControllerError::Shape(_)
                | ControllerError::Internal(_) => ErrorScope::Item,
            };
            (e, want)
        }
        every_variant().into_iter().map(|(e, _)| row(e)).collect()
    }

    #[test]
    fn store_errors_stop_a_sweep_and_nothing_else_does() {
        let rows = every_scope();
        assert_eq!(rows.len(), 8, "one row per variant");
        for (e, want) in rows {
            assert_eq!(e.scope(), want, "{e:?}");
        }
    }

    #[test]
    fn every_error_variant_maps_to_its_retry_class() {
        let rows = every_variant();
        assert_eq!(rows.len(), 8, "one row per variant");
        for (e, want) in rows {
            assert_eq!(e.classify(), want, "{e:?}");
        }
    }

    /// The class belongs to the variant. The same variant carrying a
    /// message that reads like the OTHER class keeps its own class — the
    /// wording that used to decide it is inert.
    #[test]
    fn the_message_text_does_not_decide_the_class() {
        // Texts the fleet's string classifier reads as Declarative.
        for text in [
            "schema validation failed: unknown attribute",
            "flake 'x' does not provide attribute 'y'",
            "path '/nix/store/abc' does not exist",
        ] {
            assert_eq!(
                ControllerError::Internal(text.into()).classify(),
                FailureKind::Transient,
                "Internal({text:?})"
            );
            assert_eq!(
                ControllerError::Store(StoreError::ClientWriteFailed(text.into())).classify(),
                FailureKind::Transient,
                "ClientWriteFailed({text:?})"
            );
        }
        // Texts that read as Transient on a Declarative variant.
        for text in ["connection refused", "timed out", "503 Service Unavailable"] {
            assert_eq!(
                ControllerError::InvalidResource(text.into()).classify(),
                FailureKind::Declarative,
                "InvalidResource({text:?})"
            );
            assert_eq!(
                ControllerError::Store(StoreError::Fatal(text.into())).classify(),
                FailureKind::Declarative,
                "Fatal({text:?})"
            );
        }
    }
}
