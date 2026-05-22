//! Bridge from [`engenho_config`] selections to a concrete
//! [`Box<dyn ContainerRuntime>`].

use std::sync::Arc;

use crate::backend::{ContainerRuntime, FakeBackend, PodmanBackend};

/// Operator-facing kubelet backend choice. Mirrors
/// `engenho_config::KubeletBackendKind` (lives in this crate to
/// avoid a forward declaration in engenho-config).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KubeletBackendKind {
    /// Real podman shell-out (production for local Mac/Linux).
    Podman,
    /// In-memory deterministic fake (tests + dev environments).
    Fake,
}

/// Construct the runtime trait object from the operator's choice.
#[must_use]
pub fn make_container_runtime(
    kind: KubeletBackendKind,
    podman_binary: Option<&str>,
) -> Arc<dyn ContainerRuntime> {
    match kind {
        KubeletBackendKind::Podman => match podman_binary {
            Some(path) => Arc::new(PodmanBackend::with_binary(path)),
            None => Arc::new(PodmanBackend::new()),
        },
        KubeletBackendKind::Fake => Arc::new(FakeBackend::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fake_backend_constructed() {
        let rt = make_container_runtime(KubeletBackendKind::Fake, None);
        assert_eq!(rt.name(), "fake");
    }

    #[test]
    fn podman_backend_default_path() {
        let rt = make_container_runtime(KubeletBackendKind::Podman, None);
        assert_eq!(rt.name(), "podman");
    }

    #[test]
    fn podman_backend_custom_path() {
        let rt = make_container_runtime(
            KubeletBackendKind::Podman,
            Some("/opt/podman/bin/podman"),
        );
        assert_eq!(rt.name(), "podman");
    }
}
