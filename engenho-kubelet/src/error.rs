//! Typed errors for kubelet operations.

use thiserror::Error;

/// Kubelet errors — store mutation failures, backend failures,
/// invalid Pod manifests.
#[derive(Debug, Clone, Error)]
pub enum KubeletError {
    /// Backing store mutation failed (Raft commit or read).
    #[error("store: {0}")]
    Store(String),

    /// Container runtime backend returned an error.
    #[error("backend: {0}")]
    Backend(String),

    /// Pod manifest in the store is missing required fields.
    #[error("invalid pod {pod}: {reason}")]
    InvalidPod {
        /// Pod's namespace/name label.
        pod: String,
        /// Why it's invalid.
        reason: String,
    },
}

engenho_substrate::impl_error_kind! {
    KubeletError {
        (Store(_)) => "store",
        (Backend(_)) => "backend",
        { InvalidPod { .. } } => "invalid_pod",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_kinds_are_stable() {
        assert_eq!(KubeletError::Store("x".into()).kind(), "store");
        assert_eq!(KubeletError::Backend("x".into()).kind(), "backend");
        assert_eq!(
            KubeletError::InvalidPod {
                pod: "x/y".into(),
                reason: "z".into()
            }
            .kind(),
            "invalid_pod"
        );
    }
}
