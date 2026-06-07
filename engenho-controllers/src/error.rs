//! Typed errors shared across controller impls.

use std::time::Duration;

use shigoto_types::failure::FailureKind;

#[derive(Debug, thiserror::Error)]
pub enum ControllerError {
    #[error("store error during reconcile: {0}")]
    Store(#[from] engenho_store::StoreError),

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
    /// `ControllerError` wraps `StoreError` (NOT `KubeError`), so the
    /// `KubeError::classify` machinery on the apiserver-client path is
    /// not directly reachable here. Per the convergence directive we
    /// consume the FLEET primitive instead of forking it:
    /// [`shigoto_types::failure::classify`] maps a raw error string to
    /// `Transient` / `Declarative` against the documented fleet-wide
    /// signature set (Nix eval failures, schema mismatches, etc.).
    /// Structural variants short-circuit to a typed classification:
    /// `InvalidResource` is always `Declarative` (a malformed
    /// declaration retrying won't fix); `Store`/`Internal` fall through
    /// to the string classifier, which defaults to `Transient`.
    #[must_use]
    pub fn classify(&self) -> FailureKind {
        match self {
            // A malformed resource is an operator-declaration bug — never
            // self-clears by retrying.
            Self::InvalidResource(_) => FailureKind::Declarative,
            // Store/Internal: consult the shared signature classifier
            // (defaults to Transient on unknown shapes).
            other => shigoto_types::failure::classify(&other.to_string()),
        }
    }

    /// Suggested retry delay for a [`FailureKind::Transient`] error.
    /// `None` for `Declarative` — the loop surfaces it instead of
    /// blind-retrying. Transient store/network errors back off 1s
    /// (mirroring `KubeError::retry_after`'s network default), replacing
    /// the flat 30s fallback the loop waited on before.
    #[must_use]
    pub fn retry_after(&self) -> Option<Duration> {
        match self.classify() {
            FailureKind::Declarative => None,
            // Transient (+ any future variant, conservatively): back off
            // 1s. FailureKind is #[non_exhaustive]; the wildcard keeps the
            // "keep trying" default if shigoto adds a class.
            _ => Some(Duration::from_secs(1)),
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
                ControllerError::Store(engenho_store::StoreError::ClientWriteFailed("x".into())),
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
}
