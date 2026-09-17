//! Bridge from [`engenho_config`] selections to a concrete
//! [`Box<dyn ContainerRuntime>`].

use std::sync::Arc;

use crate::backend::{ContainerRuntime, FakeBackend, PodmanBackend};

/// Operator-facing kubelet backend choice. Mirrors
/// `engenho_config::KubeletBackendKind` (lives in this crate to
/// avoid a forward declaration in engenho-config).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KubeletBackendKind {
    /// A CRI runtime over gRPC (containerd / CRI-O / youki) — what upstream's
    /// kubelet speaks, and the only backend that makes the runtime
    /// substitutable.
    Cri,
    /// podman over its libpod REST API — no subprocess. The default.
    PodmanApi,
    /// Real podman shell-out. Retained because it is the only backend that
    /// currently serves `exec` and `logs`.
    Podman,
    /// In-memory deterministic fake (tests + dev environments).
    Fake,
    /// Native host processes out of Nix closures — NO runtime underneath.
    ///
    /// The only backend that does not end at a Linux runtime, and therefore
    /// the only one that runs a workload on macOS without a VM. It runs
    /// `nix:` closure images and REFUSES OCI references, so it is selected
    /// deliberately, never inherited.
    Native,
}

/// Where the native backend keeps container logs.
///
/// Under the user's state dir rather than /var/log: this backend runs as
/// whatever principal the daemon has, and a path it cannot write would turn
/// every container start into a failure at the log-file step.
fn native_log_dir() -> std::path::PathBuf {
    std::env::var_os("ENGENHO_NATIVE_LOG_DIR").map_or_else(
        || {
            let mut p = dirs_home();
            p.push(".local/share/engenho/containers");
            p
        },
        std::path::PathBuf::from,
    )
}

fn dirs_home() -> std::path::PathBuf {
    std::env::var_os("HOME").map_or_else(std::env::temp_dir, std::path::PathBuf::from)
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
                    Arc::new(match apiserver {
                        Some((h, p)) => b.with_kubernetes_service(h, p),
                        None => b,
                    })
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
        KubeletBackendKind::Cri => {
            // ── A MISSING SOCKET FALLS BACK, LOUDLY — same rule as PodmanApi.
            // An operator who selected `cri` on a node whose containerd has not
            // come up yet gets a working kubelet and a WARN naming the paths
            // tried, rather than a node that refuses to start. The difference
            // from PodmanApi's fallback is that this one says which runtime it
            // ended up on, because silently running podman when the operator
            // asked for CRI is the kind of substitution that gets discovered
            // three debugging hours later.
            match crate::cri_backend::CriBackend::discover(None) {
                Some(b) => {
                    tracing::info!(
                        endpoint = b.endpoint_path(),
                        "kubelet driving a CRI runtime over gRPC"
                    );
                    Arc::new(match apiserver {
                        Some((h, p)) => b.with_kubernetes_service(h, p),
                        None => b,
                    })
                }
                None => {
                    tracing::warn!(
                        tried = ?crate::cri::DEFAULT_ENDPOINTS,
                        "no CRI socket found — falling back to podman for this boot; \
                         the kubelet is NOT on the runtime that was selected"
                    );
                    let b = PodmanBackend::new();
                    Arc::new(match apiserver {
                        Some((h, p)) => b.with_kubernetes_service(h, p),
                        None => b,
                    })
                }
            }
        }
        KubeletBackendKind::Fake => Arc::new(FakeBackend::new()),
        KubeletBackendKind::Native => Arc::new(crate::native_backend::NativeBackend::new(
            // The honest tier, named at the construction site so it appears in
            // review rather than only in a doc comment: this backend confines
            // nothing yet. See native_backend's module docs.
            crate::native_backend::Isolation::HostProcess,
            native_log_dir(),
        )),
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
