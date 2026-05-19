//! `ClusterConfigRenderer` trait — pluggable backends for emitting
//! cluster artifacts from a typed [`ClusterConfig`].
//!
//! Today's only backend is [`K3sRenderer`] (the existing render fns
//! promoted into a trait impl). The trait exists so:
//!
//! 1. When engenho-native lands (theory/ENGENHO.md M0.4+), a second
//!    `EngenhoNativeRenderer` slots in without touching the consumer
//!    (`nixos-k3s-vm` swaps the renderer; the typed schema stays put).
//! 2. Test-only renderers (e.g. "emit JSON diff vs golden") attach
//!    via the same surface — no special-casing.
//! 3. The compounding move per pleme-io CLAUDE.md Operating Principle
//!    #1: every emit-from-typed-config path goes through ONE trait,
//!    extended by adding a variant, not by adding free fns.

use crate::manifest::Manifest;
use crate::ClusterConfig;

/// What every cluster runtime needs to be configured at install time.
///
/// `K3sRenderer` produces the existing config.yaml + cmdline + manifests
/// targeting k3s. Future `EngenhoNativeRenderer` will produce engenho's
/// own typed init config (TBD shape — see theory/ENGENHO.md §IX).
pub trait ClusterConfigRenderer {
    /// Human-readable name of the runtime this renderer targets.
    /// Used in operator-visible logs + the generated artifacts'
    /// header comments.
    fn name(&self) -> &'static str;

    /// Emit the runtime's main config YAML (e.g. `/etc/rancher/k3s/
    /// config.yaml`). Empty string ⇒ this runtime has no equivalent
    /// (rare; document why).
    fn render_config_yaml(&self, cfg: &ClusterConfig) -> String;

    /// Emit additional cmdline args appended after the runtime's
    /// `serve` invocation.
    fn render_server_args(&self, cfg: &ClusterConfig) -> Vec<String>;

    /// Emit the manifests dropped into the runtime's auto-apply
    /// directory (e.g. `/var/lib/rancher/k3s/server/manifests/` for
    /// k3s). Each [`Manifest`] is uniquely-keyed by filename.
    fn render_bootstrap_manifests(&self, cfg: &ClusterConfig) -> Vec<Manifest>;
}

/// Concrete renderer for k3s — wraps the existing
/// [`ClusterConfig::render_k3s_config_yaml`],
/// [`ClusterConfig::render_k3s_server_args`],
/// [`ClusterConfig::render_bootstrap_manifests`] methods.
///
/// Constructed via `K3sRenderer::default()`; zero state.
#[derive(Default, Clone, Copy)]
pub struct K3sRenderer;

impl ClusterConfigRenderer for K3sRenderer {
    fn name(&self) -> &'static str { "k3s" }

    fn render_config_yaml(&self, cfg: &ClusterConfig) -> String {
        cfg.render_k3s_config_yaml()
    }

    fn render_server_args(&self, cfg: &ClusterConfig) -> Vec<String> {
        cfg.render_k3s_server_args()
    }

    fn render_bootstrap_manifests(&self, cfg: &ClusterConfig) -> Vec<Manifest> {
        cfg.render_bootstrap_manifests()
    }
}

/// Stub renderer for engenho-native (M0.4+ destination). Compiles +
/// satisfies the trait so the surface is exercised; rendering itself
/// is a `todo!()` panic — replace when engenho-native init lands.
///
/// Why ship a stub: the trait surface is the load-bearing thing.
/// Consumers (nixos-k3s-vm + future engenho-vm) select a renderer
/// by trait object; having TWO impls compiles enforces the trait
/// stays general — a single-impl trait is just a fn in disguise.
#[derive(Default, Clone, Copy)]
pub struct EngenhoNativeRenderer;

impl ClusterConfigRenderer for EngenhoNativeRenderer {
    fn name(&self) -> &'static str { "engenho-native" }

    fn render_config_yaml(&self, _cfg: &ClusterConfig) -> String {
        // engenho-native consumes a different on-disk config shape
        // (TBD per theory/ENGENHO.md §IX — typed via tatara-lisp's
        // defguest daemon-supervision form, not a YAML file). Until
        // M0.4 lands the actual renderer, callers MUST NOT instantiate
        // this renderer; the build-time selection in nixos-k3s-vm
        // gates on the kasou-VM-vs-engenho-vm distinction.
        String::new()
    }

    fn render_server_args(&self, _cfg: &ClusterConfig) -> Vec<String> {
        Vec::new()
    }

    fn render_bootstrap_manifests(&self, _cfg: &ClusterConfig) -> Vec<Manifest> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn smoke_config() -> ClusterConfig {
        ClusterConfig {
            cluster_name: "smoke".into(),
            node_ip:      Ipv4Addr::new(10, 0, 0, 1),
            network:      crate::network::NetworkConfig::default(),
            bootstrap:    crate::bootstrap::BootstrapConfig::default(),
        }
    }

    #[test]
    fn k3s_renderer_round_trips_via_trait_object() {
        let r: Box<dyn ClusterConfigRenderer> = Box::new(K3sRenderer);
        assert_eq!(r.name(), "k3s");
        let cfg = smoke_config();
        let yaml = r.render_config_yaml(&cfg);
        assert!(yaml.contains("node-name: smoke"));
        let args = r.render_server_args(&cfg);
        assert!(args.is_empty()); // default config = no extra disables
        let manifests = r.render_bootstrap_manifests(&cfg);
        assert!(manifests.is_empty()); // default = no bootstrap manifests
    }

    #[test]
    fn engenho_native_renderer_is_stub() {
        let r: Box<dyn ClusterConfigRenderer> = Box::new(EngenhoNativeRenderer);
        assert_eq!(r.name(), "engenho-native");
        let cfg = smoke_config();
        assert_eq!(r.render_config_yaml(&cfg), "");
        assert!(r.render_server_args(&cfg).is_empty());
        assert!(r.render_bootstrap_manifests(&cfg).is_empty());
    }

    #[test]
    fn renderer_trait_is_dyn_compatible() {
        // The whole point: a Vec<Box<dyn ClusterConfigRenderer>> lets
        // a consumer enumerate available renderers + pick one at
        // runtime/build-time without knowing the concrete type.
        let renderers: Vec<Box<dyn ClusterConfigRenderer>> = vec![
            Box::new(K3sRenderer),
            Box::new(EngenhoNativeRenderer),
        ];
        let names: Vec<&str> = renderers.iter().map(|r| r.name()).collect();
        assert_eq!(names, vec!["k3s", "engenho-native"]);
    }
}
