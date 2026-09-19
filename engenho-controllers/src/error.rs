//! Typed errors shared across controller impls.

use engenho_store::StoreError;
use shigoto_types::failure::FailureKind;

#[derive(Debug, thiserror::Error)]
pub enum ControllerError {
    #[error("store error during reconcile: {0}")]
    Store(#[from] StoreError),

    #[error("invalid resource: {0}")]
    InvalidResource(String),

    #[error("internal: {0}")]
    Internal(String),
}

engenho_substrate::impl_error_kind! {
    ControllerError {
        (Store(_)) => "store",
        (InvalidResource(_)) => "invalid_resource",
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
            Self::InvalidResource(_) => FailureKind::Declarative,
            Self::Internal(_) => FailureKind::Transient,
        }
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
const fn store_class(e: &StoreError) -> FailureKind {
    match e {
        StoreError::ClientWriteFailed(_) => FailureKind::Transient,
        StoreError::ConfigInvalid(_) | StoreError::InitializeFailed(_) | StoreError::Fatal(_) => {
            FailureKind::Declarative
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                    StoreError::ClientWriteFailed(_) => FailureKind::Transient,
                    StoreError::ConfigInvalid(_)
                    | StoreError::InitializeFailed(_)
                    | StoreError::Fatal(_) => FailureKind::Declarative,
                },
                ControllerError::InvalidResource(_) => FailureKind::Declarative,
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
            row(ControllerError::InvalidResource(m())),
            row(ControllerError::Internal(m())),
        ]
    }

    #[test]
    fn every_error_variant_maps_to_its_retry_class() {
        let rows = every_variant();
        assert_eq!(rows.len(), 6, "one row per variant");
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
