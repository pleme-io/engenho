//! Runtime config — the single-process assembly knobs.
//!
//! The other sub-structs ([`crate::SchedulerConfig`],
//! [`crate::ControllersConfig`], …) tune individual subsystems. This
//! section models the **process-level** decisions the assembly layer
//! (`engenho-runtime`) needs to boot every subsystem over ONE
//! [`engenho_store::StoreMesh`]:
//!
//!   * where the apiserver binds (`listen_addr`)
//!   * where the durable store keeps its fjall keyspace (`data_dir`)
//!   * whether the store is durable (restart-safe) or ephemeral (tests)
//!   * this node's name — the kubelet binds Pods whose
//!     `spec.nodeName` matches it, and the Runtime self-registers a
//!     schedulable Node object under this name at boot
//!   * which container backend the kubelet drives (`kubelet_backend`)
//!   * how long to wait for raft leadership before giving up
//!
//! Mirrors [`crate::KubeletBackendKind`] — the typed backend choice
//! lives here (config side) and `engenho_kubelet::KubeletBackendKind`
//! is the runtime-side mirror the assembly layer converts to.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use shikumi::TieredConfig;

use crate::error::ConfigError;
use crate::tls::TlsConfig;

/// Operator-facing kubelet backend choice — config-side mirror of
/// `engenho_kubelet::KubeletBackendKind`. Lives here so `engenho-config`
/// has no dependency on `engenho-kubelet` (the assembly layer maps one
/// to the other).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KubeletBackendKind {
    /// podman over its libpod REST API on the unix socket — **the default**.
    ///
    /// No subprocess: typed JSON requests, status codes instead of parsed
    /// error prose, and a versioned contract instead of a command line. This
    /// is the naturalized seam, and it is the default because the alternative
    /// below is a NO SHELL violation that happened to be the only option.
    ///
    /// Requires `podman.socket` to be served — `systemctl enable --now
    /// podman.socket` (or the `--user` form for rootless). engenho reports the
    /// socket it chose at startup so an operator can see WHICH container store
    /// is being driven.
    ///
    /// Does not yet implement `exec` or `logs`: libpod streams both as
    /// multiplexed frames, and both refuse with a typed error naming
    /// `kubeletBackend = "podman"` as the fallback rather than returning a
    /// fabricated success or an empty string.
    PodmanApi,
    /// Real podman SHELL-OUT — argv, subprocess, stdout parsing.
    ///
    /// Retained, not deleted (★★ MODULARIZE, DON'T DELETE): it is the only
    /// backend that currently serves `exec` and `logs`, so a node running
    /// exec-based probes or serving `kubectl logs` still needs it.
    ///
    /// Its failure modes are the reason it is no longer the default. A CLI
    /// error is prose with no schema; `podman rm` can exit 0 while removing
    /// nothing (measured 2026-09-01, invisible from the exit code); and a
    /// renamed flag in a podman release is a silent behaviour change.
    Podman,
    /// In-memory deterministic fake (tests + dev environments).
    Fake,
}

/// Process-level assembly config.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    /// Where the apiserver binds. `0.0.0.0:6443` in production;
    /// tests pass `127.0.0.1:0` to get an OS-assigned ephemeral port.
    pub listen_addr: String,
    /// Durable-store keyspace root. The Runtime opens the fjall store
    /// at `data_dir.join("store")`. Tests pass a tempdir.
    pub data_dir: PathBuf,
    /// `true` → durable [`engenho_store::StoreMesh::start_or_resume`]
    /// (restart-safe). `false` → ephemeral in-memory store (tests,
    /// dev). Production is always durable.
    pub durable: bool,
    /// This node's name. The kubelet binds Pods whose
    /// `spec.nodeName == node_name`; the Runtime self-registers a
    /// schedulable `Node/<node_name>` at boot so the scheduler has a
    /// target.
    pub node_name: String,
    /// Where the boot kubeconfig is ALSO published, so ordinary tooling
    /// finds it without being told.
    ///
    /// ── ★ WHY A SECOND PATH AND NOT JUST `data_dir/kubeconfig` ───────
    /// The daemon has always written `data_dir/kubeconfig`, and nothing
    /// reads it: kubectl, k9s and flux resolve through `$KUBECONFIG`,
    /// which the fleet composes from `~/.kube/configs/*` (nix:
    /// `modules/shared/kubeconfig-paths.nix`, the typed
    /// `pleme.kubeconfigs` list). A kubeconfig nobody looks at is a
    /// kubeconfig that does not exist.
    ///
    /// ── ★ WHY THE PATH IS STABLE AND THE *NAME* CARRIES THE NODE ─────
    /// The cluster NAME is per-node and hashed
    /// ([`crate::cluster::default_cluster_name`]), but the PATH is not —
    /// and that asymmetry is deliberate. Nix has to declare this file in
    /// `pleme.kubeconfigs` at BUILD time, and `builtins.hashString`
    /// offers md5/sha1/sha256/sha512 — **no blake3**. Nix therefore
    /// cannot reproduce `engenho-<node>-<hash>` to name the file it
    /// declares. A stable path removes the need: nix declares one
    /// literal, and the per-node identity lives in the cluster/context
    /// name INSIDE the file, where kubectl shows it. Each node runs one
    /// engenho, so the path needs no hash to disambiguate.
    ///
    /// Empty string disables the publish (the `data_dir` copy is always
    /// written), which is what tests use to stay out of `$HOME`.
    pub kubeconfig_publish_path: String,
    /// Where a POD-FACING kubeconfig is published, or empty for none.
    ///
    /// ── ★ WHY A SECOND KUBECONFIG EXISTS ──────────────────────────────
    /// `kubeconfig_publish_path`'s file points at LOOPBACK, which is right
    /// for the operator's kubectl and useless from inside a pod: on darwin
    /// the apiserver binds the host while containers live in a VM.
    ///
    /// In-cluster config is not the answer today. engenho projects a real
    /// ServiceAccount token, and its own authenticator rejects it —
    /// `service account token authentication is not yet supported`, HTTP
    /// 401 (measured 2026-09-01). So a workload that needs the API needs a
    /// kubeconfig, and the only thing that can mint one with the right
    /// server address and a valid client cert is engenho itself.
    ///
    /// This file therefore exists so a consumer can create a Secret from it
    /// rather than hand-assembling one from the admin cert — which is what
    /// the pangea stack was doing, with the address pasted in by hand.
    ///
    /// Empty by default: it embeds admin credentials, and a file that
    /// appears without being asked for is one nobody decided to create.
    pub pod_kubeconfig_publish_path: String,
    /// The address REMOTE clients dial to reach this apiserver — a hostname or
    /// IP, optionally with `:port`.
    ///
    /// ── ★ THE ADDRESS AND THE CERTIFICATE ARE ONE FACT ────────────────────
    /// Setting this does two things that must never be done separately: it is
    /// the `server:` of the remote kubeconfig, AND it is added to the serving
    /// certificate's SANs. engenho already learned this once — after the
    /// runtime began advertising `host.containers.internal` to pods, in-cluster
    /// clients still failed, because the name now ROUTED but the certificate did
    /// not NAME it. Two layers, one symptom, and the second invisible from the
    /// first. A test pins that pair; this field makes the same pair
    /// unconstructible for the remote case, rather than pinned after the fact.
    ///
    /// Empty means this apiserver is node-local: no remote kubeconfig is
    /// published and no extra SAN is derived.
    ///
    /// The port is optional and defaults to the one in `listen_addr`. Giving a
    /// different one is legitimate and expected — a reverse proxy, a tailnet
    /// forward, a NAT — and only the HOST reaches the certificate, since a
    /// certificate names hosts, not ports.
    pub advertise_address: String,
    /// Where to publish a kubeconfig whose `server:` is [`Self::advertise_address`].
    ///
    /// The third audience, and the one that had no path. `kubeconfig_publish_path`
    /// writes a LOOPBACK url — correct on the node, useless anywhere else — and
    /// `pod_kubeconfig_publish_path` writes the address CONTAINERS reach. Neither
    /// serves the operator on another machine, so distributing access meant
    /// hand-editing a copied kubeconfig's `server:` line, which is the shape that
    /// silently disagrees with the certificate.
    ///
    /// Empty by default: like the pod-facing file it embeds admin credentials,
    /// and a file carrying those should never appear unasked.
    pub remote_kubeconfig_publish_path: String,
    /// Which container runtime the kubelet drives.
    /// Where the KUBELET's own HTTP surface binds (`/containerLogs`,
    /// `/pods`, `/exec`, `/healthz`) — upstream's :10250.
    ///
    /// ★ THE DEFAULT IS LOOPBACK, NOT `0.0.0.0`, and that is a decision.
    /// This surface serves container logs and runs commands inside
    /// containers, and it has NO authentication of its own — upstream gates
    /// :10250 behind webhook authn/authz, which engenho does not have yet.
    /// Binding every interface by default would publish unauthenticated
    /// exec on the node. Loopback keeps it usable for the apiserver on the
    /// same host (the single-binary layout today) and reachable from
    /// elsewhere only by an explicit operator decision. Widen it when the
    /// authn gate exists, not before.
    pub kubelet_listen_addr: String,

    /// Where the etcd v3 FAÇADE binds — upstream's :2379.
    ///
    /// ★ WHAT THIS BUYS, since engenho runs no etcd and its own apiserver
    /// never speaks it. Nothing in the ecosystem asks "do you have etcd?";
    /// it asks to `Range` a keyspace, to `snapshot save`, to be pointed at
    /// `--etcd-servers`. Those verbs are the contract every backup tool,
    /// runbook and dashboard drives. Serving them makes engenho
    /// substitutable for k3s to that whole class of software — and it is
    /// the door through which upstream's REAL kube-apiserver can one day be
    /// pointed at engenho-store, turning every Kubernetes conformance suite
    /// into a test of our storage layer against the genuine article.
    ///
    /// ★ LOOPBACK BY DEFAULT, for the same reason as :10250 above: the
    /// façade has no authentication of its own. Upstream etcd gates :2379
    /// behind mutual TLS; engenho does not yet. It is READ-ONLY today,
    /// which bounds the exposure to disclosure rather than mutation — but
    /// the whole cluster state, Secrets included, is a disclosure worth
    /// keeping on the loopback interface until the TLS gate exists.
    ///
    /// Empty string DISABLES the listener, which is what tests use.
    pub etcd_listen_addr: String,

    /// Which container backend the kubelet drives.
    ///
    /// Defaults to [`KubeletBackendKind::PodmanApi`] — podman over its libpod
    /// socket, no subprocess. See that enum for why the shell-out backend is
    /// retained but no longer the default.
    pub kubelet_backend: KubeletBackendKind,
    /// Optional explicit podman binary path.
    ///
    /// Used by the shell-out backend, and by the `PodmanApi` backend ONLY on
    /// its fallback path (when no podman socket is available). `None` = resolve
    /// `podman` from `$PATH`, which is the case that fails silently under a
    /// launchd agent — prefer setting it from a package.
    pub podman_binary: Option<String>,
    /// How long to wait for raft leadership before the Runtime gives
    /// up at boot. Must be > 0.
    pub leadership_timeout_seconds: u32,
    /// Apiserver TLS posture (HTTPS + self-generated cluster CA). The
    /// load-bearing half of local-kubectl compatibility — real kubectl
    /// refuses a `server: https://…` cluster over plaintext.
    pub tls: TlsConfig,
}

impl TieredConfig for RuntimeConfig {
    fn bare() -> Self {
        Self {
            listen_addr: String::new(),
            data_dir: PathBuf::new(),
            durable: false,
            node_name: String::new(),
            kubeconfig_publish_path: String::new(),
            pod_kubeconfig_publish_path: String::new(),
            advertise_address: String::new(),
            remote_kubeconfig_publish_path: String::new(),
            kubelet_listen_addr: String::new(),
            etcd_listen_addr: String::new(),
            kubelet_backend: KubeletBackendKind::Fake,
            podman_binary: None,
            leadership_timeout_seconds: 0,
            tls: TlsConfig::bare(),
        }
    }

    /// Tier 1 — environment auto-detect wired through the shikumi
    /// [`shikumi::DiscoveryLayer`] seam (never a hand-rolled struct literal).
    /// The only genuinely-detected process-level field is `node_name` (this
    /// host's name via [`crate::discovery::HostnameLayer`]); every other field
    /// stays at its `bare()` floor here so the curated
    /// [`Self::prescribed_default`] shows through in the progressive fold.
    ///
    /// An undetectable host (`$HOSTNAME` unset) makes the layer contribute an
    /// empty dict, so `node_name` degenerates cleanly to the `bare()` empty
    /// string and `prescribed_default()` supplies the fallback.
    fn discovered() -> Self {
        let hostname = crate::discovery::HostnameLayer::from_env();
        Self::discovered_from_layers(&[&hostname])
    }

    fn prescribed_default() -> Self {
        // `node_name` is built ON the discovered() tier: when the environment
        // detected a hostname we re-emit it here, so the change-aware
        // progressive fold credits that leaf to the *Discovered* tier (it sees
        // the same value at Default and skips re-attribution). When discovery
        // couldn't answer (empty), we supply the documented axis fallback. The
        // effective value — detected hostname, else "engenho-node" — is
        // byte-identical to the pre-seam `discovered_hostname()` behavior.
        let discovered_node = Self::discovered().node_name;
        let node_name = if discovered_node.is_empty() {
            crate::discovery::NODE_NAME_FALLBACK.to_string()
        } else {
            discovered_node
        };
        Self {
            listen_addr: "0.0.0.0:6443".into(),
            data_dir: PathBuf::from("/var/lib/engenho"),
            durable: true,
            node_name,
            // The fleet convention (`~/.kube/configs/<name>`), which
            // `modules/shared/kubeconfig-paths.nix` folds into KUBECONFIG.
            // `$HOME` is resolved at write time, not here, so the default
            // stays a pure value.
            kubeconfig_publish_path: "~/.kube/configs/engenho".into(),
            // Empty: publishing admin credentials is opt-in.
            pod_kubeconfig_publish_path: String::new(),
            advertise_address: String::new(),
            remote_kubeconfig_publish_path: String::new(),
            kubelet_listen_addr: "127.0.0.1:10250".into(),
            etcd_listen_addr: "127.0.0.1:2379".into(),
            // ★ The API backend, not the shell-out one. A default that
            // violates the fleet's NO SHELL law is a default that has to be
            // overridden on every node to be correct.
            kubelet_backend: KubeletBackendKind::PodmanApi,
            podman_binary: None,
            leadership_timeout_seconds: 10,
            tls: TlsConfig::prescribed_default(),
        }
    }

    fn extend(self, base: &Self) -> Self {
        Self {
            listen_addr: if self.listen_addr.is_empty() {
                base.listen_addr.clone()
            } else {
                self.listen_addr
            },
            data_dir: if self.data_dir.as_os_str().is_empty() {
                base.data_dir.clone()
            } else {
                self.data_dir
            },
            // `durable` is a plain bool with no "unset" sentinel — the
            // overlay's value wins (operator opting into ephemeral is
            // explicit).
            durable: self.durable,
            node_name: if self.node_name.is_empty() {
                base.node_name.clone()
            } else {
                self.node_name
            },
            kubeconfig_publish_path: if self.kubeconfig_publish_path.is_empty() {
                base.kubeconfig_publish_path.clone()
            } else {
                self.kubeconfig_publish_path
            },
            pod_kubeconfig_publish_path: if self.pod_kubeconfig_publish_path.is_empty() {
                base.pod_kubeconfig_publish_path.clone()
            } else {
                self.pod_kubeconfig_publish_path.clone()
            },
            advertise_address: if self.advertise_address.is_empty() {
                base.advertise_address.clone()
            } else {
                self.advertise_address.clone()
            },
            remote_kubeconfig_publish_path: if self.remote_kubeconfig_publish_path.is_empty() {
                base.remote_kubeconfig_publish_path.clone()
            } else {
                self.remote_kubeconfig_publish_path.clone()
            },
            kubelet_listen_addr: if self.kubelet_listen_addr.is_empty() {
                base.kubelet_listen_addr.clone()
            } else {
                self.kubelet_listen_addr
            },
            etcd_listen_addr: if self.etcd_listen_addr.is_empty() {
                base.etcd_listen_addr.clone()
            } else {
                self.etcd_listen_addr
            },
            kubelet_backend: self.kubelet_backend,
            podman_binary: self.podman_binary.or_else(|| base.podman_binary.clone()),
            leadership_timeout_seconds: if self.leadership_timeout_seconds == 0 {
                base.leadership_timeout_seconds
            } else {
                self.leadership_timeout_seconds
            },
            tls: self.tls.extend(&base.tls),
        }
    }
}

impl RuntimeConfig {
    /// Validate the runtime config.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidField`] when:
    ///
    ///   * `node_name` is empty (the kubelet + Node registration both
    ///     key on it)
    ///   * `listen_addr` is empty (the apiserver has nowhere to bind)
    ///   * `leadership_timeout_seconds` is zero (boot would never wait
    ///     for raft leadership and the first `propose` would fail)
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.node_name.is_empty() {
            return Err(ConfigError::InvalidField {
                field: "runtime.node_name".into(),
                reason: "node name cannot be empty (kubelet + Node registration key on it)".into(),
            });
        }
        if self.listen_addr.is_empty() {
            return Err(ConfigError::InvalidField {
                field: "runtime.listen_addr".into(),
                reason: "listen addr cannot be empty (apiserver has nowhere to bind)".into(),
            });
        }
        if self.leadership_timeout_seconds == 0 {
            return Err(ConfigError::InvalidField {
                field: "runtime.leadership_timeout_seconds".into(),
                reason: "leadership timeout must be > 0".into(),
            });
        }
        self.tls.validate()?;
        Ok(())
    }

    /// The effective CA cert path: explicit `tls.ca_cert_path` or the
    /// derived `data_dir/pki/ca.crt`.
    #[must_use]
    pub fn ca_cert_path(&self) -> PathBuf {
        self.tls
            .ca_cert_path
            .clone()
            .unwrap_or_else(|| self.data_dir.join("pki").join("ca.crt"))
    }

    /// The effective CA key path: explicit `tls.ca_key_path` or the
    /// derived `data_dir/pki/ca.key`.
    #[must_use]
    pub fn ca_key_path(&self) -> PathBuf {
        self.tls
            .ca_key_path
            .clone()
            .unwrap_or_else(|| self.data_dir.join("pki").join("ca.key"))
    }
}

#[cfg(test)]
mod backend_kind_wire {
    use super::KubeletBackendKind;
    use shikumi::TieredConfig;

    /// The Nix enum and the serde name must agree, or a declared value produces
    /// a config engenho refuses to parse.
    ///
    /// These strings live in two repositories — `nix/typed-config.nix`'s
    /// `types.enum [ "podman_api" "podman" "fake" ]` and this enum's
    /// `rename_all = "snake_case"`. Nothing but this test connects them, and the
    /// failure is at DAEMON START on the node, long after the nix build went
    /// green.
    #[test]
    fn the_wire_names_are_exactly_what_the_nix_enum_offers() {
        let cases = [
            (KubeletBackendKind::PodmanApi, "\"podman_api\""),
            (KubeletBackendKind::Podman, "\"podman\""),
            (KubeletBackendKind::Fake, "\"fake\""),
        ];
        for (kind, want) in cases {
            assert_eq!(
                serde_json::to_string(&kind).unwrap(),
                want,
                "nix/typed-config.nix offers {want} for {kind:?}"
            );
        }
    }

    /// The naturalized backend is what a node gets without asking.
    ///
    /// A default that violates the fleet's NO SHELL law is a default that must
    /// be overridden on every node to be correct, which is not a default.
    #[test]
    fn the_prescribed_default_is_the_api_backend_not_the_shell_out_one() {
        assert_eq!(
            super::RuntimeConfig::prescribed_default().kubelet_backend,
            KubeletBackendKind::PodmanApi
        );
    }

    /// The shell-out backend is still SELECTABLE.
    ///
    /// ★★ MODULARIZE, DON'T DELETE — it is the only backend serving `exec` and
    /// `logs`, so removing it would take exec-based probes and `kubectl logs`
    /// with it. Retirement here is a changed default, not a deletion.
    #[test]
    fn the_shell_out_backend_remains_reachable() {
        assert_eq!(
            serde_json::from_str::<KubeletBackendKind>("\"podman\"").unwrap(),
            KubeletBackendKind::Podman
        );
    }
}

#[cfg(test)]
mod tests {

    /// The pod-facing kubeconfig is OPT-IN: it embeds admin credentials, so a
    /// file appearing without being asked for is one nobody decided to create.
    #[test]
    fn the_pod_kubeconfig_is_not_published_unless_asked() {
        let prescribed = RuntimeConfig::prescribed_default();
        assert_eq!(
            prescribed.pod_kubeconfig_publish_path, "",
            "prescribed defaults must not publish admin credentials"
        );
        assert_eq!(
            prescribed.kubeconfig_publish_path, "~/.kube/configs/engenho",
            "the loopback publish IS a default — it is what makes kubectl work"
        );
    }

    /// An operator's value must survive the progressive fold, or the option is
    /// declared and inert.
    #[test]
    fn an_operator_pod_kubeconfig_path_wins_the_fold() {
        let mut file = RuntimeConfig::bare();
        file.pod_kubeconfig_publish_path = "~/.kube/configs/engenho-pod".into();
        let folded = file.extend(&RuntimeConfig::prescribed_default());
        assert_eq!(
            folded.pod_kubeconfig_publish_path, "~/.kube/configs/engenho-pod",
            "the file tier must beat the empty prescribed default"
        );
    }
    use super::*;
    use crate::discovery::{HostnameLayer, NODE_NAME_FALLBACK};

    #[test]
    fn prescribed_default_validates() {
        RuntimeConfig::prescribed_default().validate().unwrap();
    }

    #[test]
    fn discovered_tier_picks_up_hostname_via_the_seam() {
        // The discovered() tier resolves node_name through the DiscoveryLayer
        // seam (kanchi-shaped), NOT the old hand-rolled `discovered_hostname`
        // fn: a layer that reports a host name lands it on `node_name`.
        let cfg = RuntimeConfig::discovered_from_layers(&[&HostnameLayer::from_raw(Some(
            "node-Z".into(),
        ))]);
        assert_eq!(cfg.node_name, "node-Z");
    }

    #[test]
    fn discovered_tier_is_bare_when_hostname_undetectable() {
        // Discovery totality: an undetectable host leaves node_name at the
        // bare() floor (empty) — prescribed_default() then supplies the
        // documented fallback.
        let cfg = RuntimeConfig::discovered_from_layers(&[&HostnameLayer::from_raw(None)]);
        assert!(cfg.node_name.is_empty());
        // And the fallback the prescribed tier applies is the axis fallback.
        assert_eq!(NODE_NAME_FALLBACK, "engenho-node");
    }

    #[test]
    fn bare_fails_validation() {
        assert!(RuntimeConfig::bare().validate().is_err());
    }

    #[test]
    fn empty_node_name_rejected() {
        let mut cfg = RuntimeConfig::prescribed_default();
        cfg.node_name = String::new();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn empty_listen_addr_rejected() {
        let mut cfg = RuntimeConfig::prescribed_default();
        cfg.listen_addr = String::new();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn zero_leadership_timeout_rejected() {
        let mut cfg = RuntimeConfig::prescribed_default();
        cfg.leadership_timeout_seconds = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn extend_fills_empty_fields_from_base() {
        let overlay = RuntimeConfig {
            listen_addr: "127.0.0.1:0".into(),
            data_dir: PathBuf::new(),
            durable: false,
            node_name: "node-A".into(),
            // Empty ⇒ `extend` must fill it from the base, which is
            // precisely what this test asserts for every other field.
            kubeconfig_publish_path: String::new(),
            pod_kubeconfig_publish_path: String::new(),
            advertise_address: String::new(),
            remote_kubeconfig_publish_path: String::new(),
            kubelet_listen_addr: String::new(),
            etcd_listen_addr: String::new(),
            kubelet_backend: KubeletBackendKind::Fake,
            podman_binary: None,
            leadership_timeout_seconds: 0,
            tls: TlsConfig::bare(),
        };
        let base = RuntimeConfig::prescribed_default();
        let merged = overlay.extend(&base);
        assert_eq!(merged.listen_addr, "127.0.0.1:0");
        assert_eq!(merged.node_name, "node-A");
        // data_dir empty in overlay → falls back to base.
        assert_eq!(merged.data_dir, base.data_dir);
        // leadership timeout 0 in overlay → falls back to base.
        assert_eq!(
            merged.leadership_timeout_seconds,
            base.leadership_timeout_seconds
        );
    }

    #[test]
    fn backend_kind_serializes_snake_case() {
        let json = serde_json::to_string(&KubeletBackendKind::Fake).unwrap();
        assert_eq!(json, "\"fake\"");
    }

    #[test]
    fn prescribed_default_enables_tls() {
        let cfg = RuntimeConfig::prescribed_default();
        assert!(cfg.tls.enabled);
        assert!(cfg.tls.auto_generate);
    }

    #[test]
    fn ca_paths_derive_from_data_dir_when_unset() {
        let mut cfg = RuntimeConfig::prescribed_default();
        cfg.data_dir = PathBuf::from("/srv/engenho");
        assert_eq!(cfg.ca_cert_path(), PathBuf::from("/srv/engenho/pki/ca.crt"));
        assert_eq!(cfg.ca_key_path(), PathBuf::from("/srv/engenho/pki/ca.key"));
    }

    #[test]
    fn explicit_ca_paths_override_derived() {
        let mut cfg = RuntimeConfig::prescribed_default();
        cfg.data_dir = PathBuf::from("/srv/engenho");
        cfg.tls.ca_cert_path = Some(PathBuf::from("/custom/ca.crt"));
        assert_eq!(cfg.ca_cert_path(), PathBuf::from("/custom/ca.crt"));
        // ca_key still derives (only ca_cert was overridden).
        assert_eq!(cfg.ca_key_path(), PathBuf::from("/srv/engenho/pki/ca.key"));
    }
}
