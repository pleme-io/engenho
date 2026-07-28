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
    /// Real podman shell-out (production for local Mac/Linux).
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
    /// Which container runtime the kubelet drives.
    pub kubelet_backend: KubeletBackendKind,
    /// Optional explicit podman binary path (ignored unless
    /// `kubelet_backend == Podman`). `None` = default `podman` on PATH.
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
            kubelet_backend: KubeletBackendKind::Podman,
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
mod tests {
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
