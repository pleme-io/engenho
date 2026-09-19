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

    /// The runtime refused to drop a container's record because the
    /// container's process has not been reaped yet — it is still inside its
    /// SIGTERM → SIGKILL window, or was never stopped.
    ///
    /// Not a failure: the stop is in flight and the record goes once the
    /// process is waited on. Distinct from [`Self::Backend`] so a caller can
    /// retry soon and quietly instead of reporting a broken runtime, and so a
    /// replacement is never started beside a process that is still alive.
    #[error("container {container_id} has not been reaped yet; its record stays until it is")]
    NotReaped {
        /// The container whose process is still owed a wait.
        container_id: String,
    },

    /// A volume could not be torn down.
    #[error("volume teardown: {0}")]
    VolumeTeardown(crate::pod_volume::VolumeResolveError),
}

engenho_substrate::impl_error_kind! {
    KubeletError {
        (Store(_)) => "store",
        (Backend(_)) => "backend",
        { InvalidPod { .. } } => "invalid_pod",
        { NotReaped { .. } } => "not_reaped",
        (VolumeTeardown(_)) => "volume_teardown",
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
        assert_eq!(
            KubeletError::NotReaped {
                container_id: "c".into()
            }
            .kind(),
            "not_reaped"
        );
    }
}
