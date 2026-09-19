//! Typed errors for kubelet operations.

use thiserror::Error;

use crate::probe::{ProbeKind, ProbeParseError};

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

    /// A container declares a probe the kubelet cannot run: no handler, an
    /// empty exec command, or a grpc probe. The pod is refused rather than
    /// run with a probe that would pass falsely.
    #[error("invalid pod {pod}: {}: {source}", .kind.field())]
    InvalidProbe {
        /// Pod's namespace/name label.
        pod: String,
        /// Which of the container's probes.
        kind: ProbeKind,
        /// Why it cannot run.
        source: ProbeParseError,
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
        { InvalidProbe { .. } } => "invalid_pod",
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
            KubeletError::InvalidProbe {
                pod: "x/y".into(),
                kind: ProbeKind::Readiness,
                source: ProbeParseError::NoHandler,
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

    #[test]
    fn an_invalid_probe_names_the_pod_the_field_and_the_reason() {
        let e = KubeletError::InvalidProbe {
            pod: "default/web".into(),
            kind: ProbeKind::Liveness,
            source: ProbeParseError::UnsupportedHandler { kind: "grpc" },
        };
        assert_eq!(
            e.to_string(),
            "invalid pod default/web: livenessProbe: unsupported probe handler: grpc"
        );
        assert!(
            std::error::Error::source(&e).is_some(),
            "the parse error stays reachable as the source"
        );
    }
}
