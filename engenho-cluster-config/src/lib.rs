//! # engenho-cluster-config
//!
//! Typed schema for everything a kasou-VM (or AMI) k3s cluster needs to be
//! configured beyond "make a VM and run k3s." The schema is the single
//! source of truth: nix consumers (nixos-k3s-vm) deserialize a YAML/JSON
//! representation of [`ClusterConfig`], invoke the renderers, and emit
//! every artifact (k3s `config.yaml`, k3s server cmdline flags, manifests
//! dropped into `/var/lib/rancher/k3s/server/manifests/`) mechanically.
//!
//! ## Why a crate (not just nix options)
//!
//! Two reasons, both load-bearing:
//!
//! 1. **Validation.** Cross-field invariants (CIDR non-overlap, kube-proxy
//!    mode + CNI compatibility, dual-stack consistency) are hard to express
//!    in nix's module system and easy in Rust. [`ClusterConfig::validate`]
//!    runs at deserialize time and fails closed.
//!
//! 2. **Render parity.** k3s today, engenho tomorrow (theory/ENGENHO.md
//!    §X). When engenho-native lands, the same typed config drives the
//!    native renderer instead of the k3s-flag emitter. Keeping the schema
//!    in Rust means the migration is a renderer swap, not a re-modeling.
//!
//! ## Surface
//!
//! * [`ClusterConfig`] — root: `network`, `bootstrap`, `cluster`
//! * [`NetworkConfig`] — every k3s network knob (CNI, kube-proxy, CIDRs,
//!   ingress, LB, DNS, IPv6 dual-stack, MTU, node-port range, TLS SANs,
//!   bind/advertise addresses, disabled components)
//! * [`BootstrapConfig`] — GitOps bootstrap: FluxCD + ArgoCD, each
//!   independently enableable, both pointing at typed sources.
//!
//! ## Render targets
//!
//! * [`ClusterConfig::render_k3s_config_yaml`] — emits the YAML k3s
//!   reads from `/etc/rancher/k3s/config.yaml` (preferred over flags).
//! * [`ClusterConfig::render_k3s_server_args`] — emits extra `--key=val`
//!   flags for knobs not covered by config.yaml (e.g. `--disable`).
//! * [`ClusterConfig::render_bootstrap_manifests`] — emits a `Vec` of
//!   `Manifest` (kind + name + body) for `/var/lib/rancher/k3s/server/
//!   manifests/`. k3s auto-applies these at startup.

#![warn(missing_docs)]

pub mod bootstrap;
pub mod k3s_yaml;
pub mod manifest;
pub mod network;
pub mod render;
pub mod renderer;
pub mod validate;

pub use k3s_yaml::K3sServerConfig;
pub use renderer::{ClusterConfigRenderer, EngenhoNativeRenderer, K3sRenderer};

use serde::{Deserialize, Serialize};

pub use bootstrap::{BootstrapConfig, FluxcdBootstrap, ArgocdBootstrap, GitopsSource, SecretRef};
pub use manifest::Manifest;
pub use network::{
    CniChoice, DnsChoice, FlannelBackend, IngressChoice, Ipv6Config,
    KubeProxyConfig, KubeProxyMode, K3sComponent, LoadBalancerChoice,
    NetworkConfig, NetworkPolicyConfig, NetworkPolicyEnforce, PortRange,
};
pub use validate::ConfigError;

/// Root config — exactly what a kasou-VM or AMI consumer passes in.
///
/// Deserialized from YAML/JSON; nix consumers serialize a fragment of
/// their module tree and pass it to [`Self::from_yaml_str`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct ClusterConfig {
    /// Required: the cluster's name. Used in TLS SAN derivation,
    /// manifest namespacing, and operator-facing labels.
    pub cluster_name: String,

    /// Required: the cluster's primary node IP, baked into the k3s
    /// `--tls-san` list + `--advertise-address` default.
    pub node_ip: std::net::Ipv4Addr,

    /// k3s/k8s network surface. Defaults match k3s' built-in shape
    /// (flannel + vxlan + iptables kube-proxy + traefik + servicelb +
    /// coredns) so a config that only sets `cluster_name` + `node_ip`
    /// renders to identical behavior as today.
    #[serde(default)]
    pub network: NetworkConfig,

    /// GitOps bootstrap. Both fluxcd and argocd default to disabled;
    /// enabling either drops the relevant install manifest into k3s'
    /// auto-apply directory at activation time.
    #[serde(default)]
    pub bootstrap: BootstrapConfig,
}

impl ClusterConfig {
    /// Parse YAML, validate, return a guaranteed-coherent config.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Parse`] for malformed YAML or
    /// [`ConfigError::Invalid`] for any cross-field violation.
    pub fn from_yaml_str(s: &str) -> Result<Self, ConfigError> {
        let cfg: Self = serde_yaml::from_str(s).map_err(|e| ConfigError::Parse(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Parse JSON, validate, return a guaranteed-coherent config.
    ///
    /// # Errors
    ///
    /// Same as [`Self::from_yaml_str`] but for JSON.
    pub fn from_json_str(s: &str) -> Result<Self, ConfigError> {
        let cfg: Self = serde_json::from_str(s).map_err(|e| ConfigError::Parse(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validate cross-field invariants. Called automatically by
    /// `from_yaml_str` + `from_json_str`; expose publicly so consumers
    /// constructing via the builder API can opt in.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Invalid`] with a human-readable explanation
    /// of which invariant failed.
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate::validate(self)
    }
}
