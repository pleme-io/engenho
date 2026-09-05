//! Bridge from [`engenho_config`] selections to a concrete
//! [`Box<dyn ContainerRuntime>`].

use std::sync::Arc;

use crate::backend::{ContainerRuntime, FakeBackend, PodmanBackend};

/// Operator-facing kubelet backend choice. Mirrors
/// `engenho_config::KubeletBackendKind` (lives in this crate to
/// avoid a forward declaration in engenho-config).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KubeletBackendKind {
    /// podman over its libpod REST API — no subprocess. The default.
    PodmanApi,
    /// Real podman shell-out. Retained because it is the only backend that
    /// currently serves `exec` and `logs`.
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
    make_container_runtime_with_apiserver(kind, podman_binary, None)
}

/// As [`make_container_runtime`], plus the API-server `(host, port)` injected
/// into every container as the `KUBERNETES_SERVICE_*` block.
///
/// Separate constructor rather than a changed signature: every existing
/// caller keeps working and gets `None`, which is the pre-existing behaviour.
/// A `None` here means an in-cluster client sees no service env and reports
/// "no kubeconfig" — correct-but-useless, and exactly what the real
/// pangea-operator hit.
#[must_use]
pub fn make_container_runtime_with_apiserver(
    kind: KubeletBackendKind,
    podman_binary: Option<&str>,
    apiserver: Option<(String, u16)>,
) -> Arc<dyn ContainerRuntime> {
    match kind {
        KubeletBackendKind::PodmanApi => {
            // ── A MISSING SOCKET FALLS BACK, LOUDLY ───────────────────────
            // Refusing to construct would mean a node with podman installed
            // but its socket unit not enabled fails to start its kubelet
            // entirely — a worse outcome than the shell-out path it had
            // yesterday. So it degrades to the CLI backend and says so at WARN
            // with the remedy in the message.
            //
            // This is a fallback, NOT a silent one: the log line names the
            // socket paths tried and the systemctl command that fixes it. The
            // failure it prevents (a kubelet that will not start) is worse
            // than the one it accepts (a subprocess seam for one boot).
            match crate::podman_api::PodmanApiBackend::discover() {
                Ok(b) => {
                    tracing::info!(
                        socket = %b.endpoint_path().display(),
                        "kubelet driving podman over its API socket (no subprocess)"
                    );
                    Arc::new(b)
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "podman API socket unavailable — falling back to the shell-out backend \
                         for this boot"
                    );
                    let b = match podman_binary {
                        Some(path) => PodmanBackend::with_binary(path),
                        None => PodmanBackend::new(),
                    };
                    Arc::new(match apiserver {
                        Some((h, p)) => b.with_kubernetes_service(h, p),
                        None => b,
                    })
                }
            }
        }
        KubeletBackendKind::Podman => {
            let b = match podman_binary {
                Some(path) => PodmanBackend::with_binary(path),
                None => PodmanBackend::new(),
            };
            Arc::new(match apiserver {
                Some((h, p)) => b.with_kubernetes_service(h, p),
                None => b,
            })
        }
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
        let rt = make_container_runtime(KubeletBackendKind::Podman, Some("/opt/podman/bin/podman"));
        assert_eq!(rt.name(), "podman");
    }
}
