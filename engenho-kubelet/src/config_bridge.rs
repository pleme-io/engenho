//! Bridge from [`engenho_config`] selections to a concrete
//! [`Box<dyn ContainerRuntime>`].

use std::sync::Arc;

use crate::backend::{ContainerRuntime, FakeBackend, PodmanBackend};
use crate::cri_backend::CriGaps;

/// The operator's backend choice: [`engenho_config::KubeletBackendKind`].
///
/// Re-exported, not mirrored (I39). This crate used to declare its own enum
/// with the same five arms, and the runtime converted a parsed config into it
/// arm by arm. Two lists of one fact is where a new backend lands in one and
/// not the other; with one type a parsed config's choice reaches
/// [`make_container_runtime`] as-is. The path `config_bridge::KubeletBackendKind`
/// still resolves.
pub use engenho_config::KubeletBackendKind;

/// Why `kind` may not be constructed, or `None` when it may.
///
/// Decided from the kind alone — no socket is probed — so a node's config
/// can be checked before anything boots, and a host that happens to run
/// containerd is refused exactly like one that does not.
///
/// This is the refusal from EVIDENCE: `cri` is refused while
/// [`CriGaps::outstanding`] names a gap. The config refuses the same kinds by
/// policy, when it is parsed ([`KubeletBackendKind::selection_refusal`]),
/// because `engenho-config` cannot see the gap list; a test here holds the
/// two answers equal for every kind.
#[must_use]
pub fn construction_refusal(kind: KubeletBackendKind) -> Option<BackendRefused> {
    match kind {
        KubeletBackendKind::Cri => {
            CriGaps::outstanding().map(|missing| BackendRefused::CriIncomplete { missing })
        }
        KubeletBackendKind::PodmanApi
        | KubeletBackendKind::Podman
        | KubeletBackendKind::Fake
        | KubeletBackendKind::Native => None,
    }
}

/// Why [`make_container_runtime`] refused to build the selected backend.
///
/// A construction refusal, not an operation error: it is returned before any
/// runtime exists, so a node configured for a refused backend never runs a
/// pod on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BackendRefused {
    /// `cri` was selected and the CRI backend still silently drops part of
    /// every Pod.
    ///
    /// Before this refusal, selecting `cri` built a runtime anyway — CRI when
    /// a socket existed, podman when none did — and every pod on the CRI path
    /// started without its volumes, its pod IP or its securityContext.
    #[error(
        "kubelet backend `cri` is refused at construction: it {missing} \
         (pending-cri); select `podman_api` or `native`"
    )]
    CriIncomplete {
        /// What the CRI backend does not do yet. Never empty.
        missing: CriGaps,
    },
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
///
/// # Errors
/// [`BackendRefused`] when `kind` is refused — see [`construction_refusal`].
pub fn make_container_runtime(
    kind: KubeletBackendKind,
    podman_binary: Option<&str>,
) -> Result<Arc<dyn ContainerRuntime>, BackendRefused> {
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
///
/// # Errors
/// [`BackendRefused`] when `kind` is refused — see [`construction_refusal`].
/// Checked before anything is probed or built.
pub fn make_container_runtime_with_apiserver(
    kind: KubeletBackendKind,
    podman_binary: Option<&str>,
    apiserver: Option<(String, u16)>,
) -> Result<Arc<dyn ContainerRuntime>, BackendRefused> {
    if let Some(refused) = construction_refusal(kind) {
        return Err(refused);
    }
    let runtime: Arc<dyn ContainerRuntime> = match kind {
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
            // Reached only once `cri_backend::UNSUPPORTED` is empty; until
            // then `construction_refusal` returned above.
            //
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
    };
    Ok(runtime)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::cri_backend::CriGap;

    #[test]
    fn fake_backend_constructed() {
        let rt =
            make_container_runtime(KubeletBackendKind::Fake, None).expect("fake is not refused");
        assert_eq!(rt.name(), "fake");
    }

    #[test]
    fn podman_backend_default_path() {
        let rt = make_container_runtime(KubeletBackendKind::Podman, None)
            .expect("podman is not refused");
        assert_eq!(rt.name(), "podman");
    }

    #[test]
    fn podman_backend_custom_path() {
        let rt = make_container_runtime(KubeletBackendKind::Podman, Some("/opt/podman/bin/podman"))
            .expect("podman is not refused");
        assert_eq!(rt.name(), "podman");
    }

    // ── T5.9: CRI is refused at construction ──────────────────────────────

    /// The refusal `cri` must produce, or a panic naming the runtime that was
    /// built instead.
    fn expect_cri_refused(built: Result<Arc<dyn ContainerRuntime>, BackendRefused>) -> CriGaps {
        match built {
            Err(BackendRefused::CriIncomplete { missing }) => missing,
            Ok(rt) => panic!(
                "`cri` must be refused at construction, but a `{}` runtime was built",
                rt.name()
            ),
        }
    }

    #[test]
    fn cri_is_refused_at_construction_with_a_typed_reason() {
        let missing = expect_cri_refused(make_container_runtime(KubeletBackendKind::Cri, None));
        assert!(
            missing.contains(CriGap::Mounts),
            "the reason names mounts: {missing:?}"
        );
        assert!(
            missing.contains(CriGap::PodIp),
            "the reason names the pod IP: {missing:?}"
        );
    }

    #[test]
    fn cri_is_refused_on_the_constructor_the_runtime_boots_through() {
        // engenho-runtime's build_backend calls this one, with the apiserver
        // coordinates. A refusal on the other constructor alone would leave
        // the boot path open.
        let missing = expect_cri_refused(make_container_runtime_with_apiserver(
            KubeletBackendKind::Cri,
            Some("/opt/podman/bin/podman"),
            Some(("10.43.0.1".to_string(), 443)),
        ));
        assert!(missing.contains(CriGap::Mounts) && missing.contains(CriGap::PodIp));
    }

    #[test]
    fn the_refusal_is_decided_before_anything_is_probed() {
        // Pure: a config can be checked before boot, and the answer does not
        // depend on whether this host runs containerd.
        let refused = construction_refusal(KubeletBackendKind::Cri).expect("cri is refused");
        let BackendRefused::CriIncomplete { missing } = refused;
        assert_eq!(missing.as_slice(), crate::cri_backend::UNSUPPORTED);
    }

    #[test]
    fn no_backend_a_node_runs_today_is_refused() {
        // rio and plo run podman_api, ryn runs native. The refusal must strand
        // none of them.
        for kind in [
            KubeletBackendKind::PodmanApi,
            KubeletBackendKind::Podman,
            KubeletBackendKind::Fake,
            KubeletBackendKind::Native,
        ] {
            assert_eq!(
                construction_refusal(kind),
                None,
                "{kind:?} must not be refused"
            );
        }
    }

    #[test]
    fn the_refusal_tells_the_operator_every_gap_and_what_to_select() {
        let refused = construction_refusal(KubeletBackendKind::Cri).expect("cri is refused");
        let BackendRefused::CriIncomplete { missing } = refused;
        let shown = refused.to_string();
        assert!(shown.contains("`cri`"), "names the backend: {shown}");
        for gap in missing.as_slice() {
            assert!(shown.contains(&gap.to_string()), "names {gap:?}: {shown}");
        }
        assert!(
            shown.contains("podman_api"),
            "names a backend that works: {shown}"
        );
    }

    // ── I38/I39: one backend type, refused at parse and at construction ──

    /// Every kind, for the tests that must cover all of them.
    ///
    /// The `match` has no wildcard, so a new variant does not compile here
    /// until it is placed; the list beside it is then the one to extend. That
    /// keeps the list honest by review, not by type.
    fn every_kind() -> [KubeletBackendKind; 5] {
        let every = [
            KubeletBackendKind::Cri,
            KubeletBackendKind::PodmanApi,
            KubeletBackendKind::Podman,
            KubeletBackendKind::Fake,
            KubeletBackendKind::Native,
        ];
        for kind in every {
            match kind {
                KubeletBackendKind::Cri
                | KubeletBackendKind::PodmanApi
                | KubeletBackendKind::Podman
                | KubeletBackendKind::Fake
                | KubeletBackendKind::Native => {}
            }
        }
        every
    }

    #[test]
    fn the_config_refuses_exactly_the_backends_the_kubelet_refuses() {
        // The config refuses by policy because it cannot see the gap list;
        // this holds the policy to the evidence. Closing the last CRI gap turns
        // it red until the config's refusal lifts too, and a config that lifted
        // it early turns it red the other way.
        for kind in every_kind() {
            assert_eq!(
                kind.selection_refusal().is_some(),
                construction_refusal(kind).is_some(),
                "{kind:?}: the config (selection_refusal) and the kubelet \
                 (construction_refusal) disagree on whether it may be used"
            );
        }
    }

    #[test]
    fn a_parsed_config_selects_the_runtime_with_no_conversion() {
        // One type end to end: the value the config parsed is the value the
        // kubelet constructs from.
        let cfg = engenho_config::EngenhoConfig::from_yaml_with_defaults(
            "runtime:\n  kubelet_backend: fake\n",
        )
        .expect("`fake` parses");
        let rt =
            make_container_runtime(cfg.runtime.kubelet_backend, None).expect("fake is not refused");
        assert_eq!(rt.name(), "fake");
    }

    #[test]
    fn a_config_selecting_cri_never_reaches_the_kubelet() {
        // The parse boundary refuses first; the construction refusal above is
        // the second line, for a config built in code that skipped validate.
        match engenho_config::EngenhoConfig::from_yaml_with_defaults(
            "runtime:\n  kubelet_backend: cri\n",
        ) {
            Err(engenho_config::ConfigError::KubeletBackendRefused(_)) => {}
            other => panic!("`cri` must be refused when the config is parsed, got {other:?}"),
        }
    }
}
