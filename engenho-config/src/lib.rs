//! # engenho-config
//!
//! The shikumi-back **top-level typed config surface** for the entire
//! engenho substrate. Per the audit (docs/MANY-FACES.md §"Gaps"):
//!
//! > *"Zero shikumi adoption fleet-wide in engenho. Per the org-level
//! > audit table, this is a clean-slate gap."*
//!
//! This crate closes that gap. [`EngenhoConfig`] owns one nested
//! struct per substrate layer:
//!
//!   * [`ClusterConfig`] — cluster identity (name, region)
//!   * [`RevoadaConfig`] — distribution layer (topology + fabric + membership)
//!   * [`Fabric`] — how the parts reach one another: `in_binary`, the only
//!     arm (NATS is not engenho's fabric; docs/IMPROVEMENT-PLAN.md §5.1)
//!   * [`SchedulerConfig`] — engenho-scheduler tunables
//!   * [`ControllersConfig`] — engenho-controllers tunables + per-controller toggles
//!   * [`ConsistencyConfig`] — per-resource `ConsistencyTier` defaults
//!
//! Each sub-struct implements `shikumi::TieredConfig`:
//!
//!   * `bare()` — zero-opinion floor (empty names, disabled features)
//!   * `prescribed_default()` — what 90% of operators want on first launch
//!     (Phalanx topology, the in-binary fabric, every controller enabled,
//!     strong consistency)
//!   * `extend(base)` — layered overlay (operator yaml on top of defaults)
//!
//! ## Operator-facing YAML
//!
//! ```yaml
//! cluster:
//!   name: rio
//!   region: us-east-2
//!
//! revoada:
//!   topology:
//!     strategy: phalanx
//!     min_nodes: 1
//!     grace_period_seconds: 10
//!
//! fabric: in_binary
//!
//! scheduler:
//!   strategy: round_robin
//!   tick_interval_seconds: 5
//!
//! controllers:
//!   enable:
//!     replicaset: true
//!     deployment: true
//!     endpoints: true
//!     gc: true
//!   fallback_interval_seconds: 30
//!
//! consistency:
//!   default_tier: strong
//! ```
//!
//! ## Discovery cascade (shikumi standard)
//!
//! ```text
//! Tier 1: $ENGENHO_CONFIG (single file)
//! Tier 2: $XDG_CONFIG_HOME/engenho/engenho.yaml
//! Tier 3: /etc/engenho/engenho.yaml
//! Tier 4: prescribed_default() — compiled-in safe values
//! ```
//!
//! The first file found is the **declared** file (the one Nix writes). Over
//! it sits the **override tier**: leaves set through the daemon's control
//! plane, kept in `data_dir/control/overrides.yaml` and folded after the
//! declared file, so an override wins leaf by leaf and `config-show` credits
//! it to that file ([`OverrideLayer`], [`EngenhoConfig::resolve_progressively_with`]).
//! Which leaves may be overridden, and what changing each one does to a
//! running engenho, is [`mutability`]'s sealed table. There is no
//! `ConfigMap` tier: the control plane is not the Kubernetes API.
//!
//! ## Validation
//!
//! [`EngenhoConfig::validate`] runs cross-section coherence checks
//! (e.g. Solo topology + 3-node consensus quorum is incoherent;
//! Phalanx with `min_nodes=0` makes no sense). Returns [`ConfigError`]
//! naming the violated invariant.
//!
//! ## Deprecated keys
//!
//! A key engenho no longer reads may still be accepted for one release so a
//! node carrying it boots. [`EngenhoConfig::deprecations`] names each one as a
//! typed [`ConfigDeprecation`] for the boot path to log; this crate has no
//! logger, so until the runtime does that, nothing prints them. Today the one
//! such key is the legacy `teia:` section, superseded by `fabric: in_binary`.

#![warn(clippy::pedantic)]
#![warn(missing_docs)]
#![allow(clippy::module_name_repetitions)]

use figment::{
    Figment,
    providers::{Format, Yaml},
    value::Dict,
};
use serde::{Deserialize, Serialize};
use shikumi::ProgressiveLayer;

// The shikumi config surface the operator CLI (engenho `config-show` /
// `config-diff`) consumes, re-exported so the thin binary depends only on
// engenho-config, not on shikumi directly.
pub use shikumi::{
    ConfigTier, ConfigTierKind, ProgressiveResolution, Provenance, ProvenanceMap, TieredConfig,
};

mod cluster;
mod consistency;
mod control;
mod controllers;
mod discovery;
mod error;
mod fabric;
pub mod leaf;
pub mod mutability;
mod networking;
mod node_local;
mod revoada;
mod runtime;
mod scheduler;
mod teia;
mod tls;

pub use cluster::ClusterConfig;
pub use consistency::{ConsistencyConfig, ConsistencyTierKind};
pub use control::{
    ControlConfig, ControlSocketConfig, GroupTier, SOCKET_PATH_MAX, SYSTEM_SOCKET_PATH,
    SocketAccess, SocketDefaults, check_socket_path_len,
};
pub use controllers::{ControllerEnable, ControllersConfig};
pub use discovery::{HostnameLayer, NODE_NAME_FALLBACK};
pub use error::ConfigError;
pub use fabric::{ConfigDeprecation, Fabric, LegacyTeiaSection};
pub use leaf::{BadLeafPath, LeafPath};
pub use mutability::{Anchor, InertWhy, LeafSpec, LiveEffect, Mutability, RespawnSet};
pub use networking::{DatapathMode, NetworkingConfig, ResolvedDatapath, parse_ipv4_cidr};
pub use node_local::{ListenerAddrRejection, LoopbackAddr, NodeLocalListener};
pub use revoada::{RevoadaConfig, TopologyConfig, TopologyStrategyKind};
pub use runtime::{KubeconfigVisibility, KubeletBackendKind, KubeletBackendRefusal, RuntimeConfig};
pub use scheduler::{SchedulerConfig, SchedulerStrategyKind};
pub use teia::TeiaConfig;
pub use tls::TlsConfig;

/// The top-level engenho config. One per process.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngenhoConfig {
    /// Cluster identity (name, region).
    pub cluster: ClusterConfig,
    /// Distribution layer (topology + fabric + membership).
    pub revoada: RevoadaConfig,
    /// How engenho's parts reach one another. `#[serde(default)]` so operator
    /// YAML written before this key existed still parses; the only arm is
    /// [`Fabric::InBinary`].
    #[serde(default)]
    pub fabric: Fabric,
    /// The retired `teia:` section, accepted for one release and read by
    /// nothing (see [`LegacyTeiaSection`]). Present only when the operator's
    /// config still carries the key; [`Self::deprecations`] reports it.
    #[serde(default, rename = "teia", skip_serializing_if = "Option::is_none")]
    pub legacy_teia: Option<LegacyTeiaSection>,
    /// Scheduler tunables.
    pub scheduler: SchedulerConfig,
    /// Controller suite tunables + per-controller toggles.
    pub controllers: ControllersConfig,
    /// Per-resource `ConsistencyTier` defaults.
    pub consistency: ConsistencyConfig,
    /// Cluster networking — the Service `ClusterIP` CIDR. `#[serde(default)]`
    /// is REQUIRED (the struct is `deny_unknown_fields`) so operator YAML
    /// written before this section existed still deserializes; the default
    /// is the upstream `10.96.0.0/12` range.
    #[serde(default = "NetworkingConfig::prescribed_default")]
    pub networking: NetworkingConfig,
    /// Single-process assembly knobs (listen addr, data dir, node
    /// name, kubelet backend, leadership timeout).
    pub runtime: RuntimeConfig,
    /// The daemon's own control plane (the local socket). Read by the
    /// supervisor, never by the runtime. `#[serde(default)]` so operator YAML
    /// written before this section existed still parses.
    #[serde(default)]
    pub control: ControlConfig,
}

impl TieredConfig for EngenhoConfig {
    fn bare() -> Self {
        Self {
            cluster: ClusterConfig::bare(),
            revoada: RevoadaConfig::bare(),
            fabric: Fabric::InBinary,
            legacy_teia: None,
            scheduler: SchedulerConfig::bare(),
            controllers: ControllersConfig::bare(),
            consistency: ConsistencyConfig::bare(),
            networking: NetworkingConfig::bare(),
            runtime: RuntimeConfig::bare(),
            control: ControlConfig::bare(),
        }
    }

    /// Tier 1 — the composed environment-discovery tier. Each sub-struct owns
    /// its own `discovered()` (most default to `bare()`); the only genuinely
    /// detected process-level field today is `runtime.node_name` (the host's
    /// name via [`RuntimeConfig::discovered`] → [`HostnameLayer`]). Composing
    /// here is what lets a detected hostname flow through the sealed
    /// progressive fold at the Discovered tier and be credited there.
    fn discovered() -> Self {
        Self {
            cluster: ClusterConfig::discovered(),
            revoada: RevoadaConfig::discovered(),
            fabric: Fabric::InBinary,
            legacy_teia: None,
            scheduler: SchedulerConfig::discovered(),
            controllers: ControllersConfig::discovered(),
            consistency: ConsistencyConfig::discovered(),
            networking: NetworkingConfig::discovered(),
            runtime: RuntimeConfig::discovered(),
            control: ControlConfig::discovered(),
        }
    }

    fn prescribed_default() -> Self {
        Self {
            cluster: ClusterConfig::prescribed_default(),
            revoada: RevoadaConfig::prescribed_default(),
            fabric: Fabric::InBinary,
            legacy_teia: None,
            scheduler: SchedulerConfig::prescribed_default(),
            controllers: ControllersConfig::prescribed_default(),
            consistency: ConsistencyConfig::prescribed_default(),
            networking: NetworkingConfig::prescribed_default(),
            runtime: RuntimeConfig::prescribed_default(),
            control: ControlConfig::prescribed_default(),
        }
    }

    fn extend(self, base: &Self) -> Self {
        Self {
            cluster: self.cluster.extend(&base.cluster),
            revoada: self.revoada.extend(&base.revoada),
            // One arm: an overlay cannot disagree with its base yet. When a
            // second arm lands, `fabric` needs an explicit "no opinion" tier.
            fabric: self.fabric,
            legacy_teia: self.legacy_teia.or_else(|| base.legacy_teia.clone()),
            scheduler: self.scheduler.extend(&base.scheduler),
            controllers: self.controllers.extend(&base.controllers),
            consistency: self.consistency.extend(&base.consistency),
            networking: self.networking.extend(&base.networking),
            runtime: self.runtime.extend(&base.runtime),
            control: self.control.extend(&base.control),
        }
    }
}

impl Default for EngenhoConfig {
    fn default() -> Self {
        Self::prescribed_default()
    }
}

impl EngenhoConfig {
    /// Run cross-section coherence checks. The substrate refuses
    /// to start with an incoherent config.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] naming the violated invariant:
    ///
    ///   * Quorum requires ≥3 nodes but topology `min_nodes` < 3
    ///   * Scheduler tick interval is zero (would hot-loop)
    ///   * Controllers fallback interval is zero (same)
    ///   * Empty cluster name
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.cluster.validate()?;
        self.revoada.validate()?;
        self.scheduler.validate()?;
        self.controllers.validate()?;
        self.consistency.validate()?;
        self.networking.validate()?;
        self.runtime.validate()?;
        self.control.validate()?;

        // Cross-section: quorum-requiring consensus needs >=3 nodes.
        if matches!(
            self.revoada.topology.strategy,
            TopologyStrategyKind::Quorum3M | TopologyStrategyKind::Cluster3MNW
        ) && self.revoada.topology.min_nodes < 3
        {
            return Err(ConfigError::Incoherent(format!(
                "topology {:?} requires min_nodes >= 3 but config has {}",
                self.revoada.topology.strategy, self.revoada.topology.min_nodes
            )));
        }
        Ok(())
    }

    /// Every key this config carries that engenho accepts but no longer reads,
    /// in a stable order. Empty for a config that uses none. For the boot path
    /// to log; nothing here changes behaviour.
    #[must_use]
    pub fn deprecations(&self) -> Vec<ConfigDeprecation> {
        // Named field by field, no `..`: a new key cannot land without a
        // decision here about whether it is one of these (E0027).
        let Self {
            legacy_teia,
            cluster: _,
            revoada: _,
            fabric: _,
            scheduler: _,
            controllers: _,
            consistency: _,
            networking: _,
            runtime: _,
            control: _,
        } = self;
        legacy_teia
            .iter()
            .map(|_| ConfigDeprecation::TeiaSection)
            .collect()
    }

    /// Discover + load the operator config via the shikumi cascade,
    /// layered on [`Self::prescribed_default`]:
    ///
    /// ```text
    /// 1. $ENGENHO_CONFIG (single file)
    /// 2. $XDG_CONFIG_HOME/engenho/engenho.yaml (~/.config/engenho/…)
    /// 3. /etc/engenho/engenho.yaml
    /// 4. prescribed_default() — compiled-in safe values (no file found)
    /// ```
    ///
    /// The first existing file wins; its YAML overlays the prescribed
    /// defaults (operators specify only what they override). When no
    /// file is found, returns `prescribed_default()` unchanged. The
    /// returned config is always `validate()`d.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Parse`] if a discovered file can't be
    /// read or is malformed; [`ConfigError::Incoherent`] /
    /// [`ConfigError::InvalidField`] on a validation failure.
    pub fn discover() -> Result<Self, ConfigError> {
        if let Some(path) = Self::discover_path() {
            let yaml = std::fs::read_to_string(&path)
                .map_err(|e| ConfigError::Parse(format!("reading {}: {e}", path.display())))?;
            Self::from_yaml_with_defaults(&yaml)
        } else {
            let cfg = Self::prescribed_default();
            cfg.validate()?;
            Ok(cfg)
        }
    }

    /// Resolve the first existing config file path via the cascade
    /// (`$ENGENHO_CONFIG` → XDG → `/etc/engenho/engenho.yaml`), or
    /// `None` when no file exists. Separated from [`Self::discover`]
    /// so callers (and tests) can introspect which file would be used.
    #[must_use]
    pub fn discover_path() -> Option<std::path::PathBuf> {
        // Tier 1 + 2: env override + XDG/home standard paths.
        let discovery = shikumi::ConfigDiscovery::new("engenho").env_override("ENGENHO_CONFIG");
        if let Ok(path) = discovery.discover() {
            return Some(path);
        }
        // Tier 3: system-wide /etc location (not part of shikumi's
        // default XDG/home scan).
        let etc = std::path::PathBuf::from("/etc/engenho/engenho.yaml");
        if etc.exists() {
            return Some(etc);
        }
        None
    }

    /// Parse an `EngenhoConfig` from YAML, layered on prescribed
    /// defaults. Operators only need to specify fields they want
    /// to override.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Parse`] on malformed YAML;
    /// [`ConfigError::Incoherent`] on cross-section invariant
    /// violation.
    pub fn from_yaml_with_defaults(yaml: &str) -> Result<Self, ConfigError> {
        let overlay: serde_yaml::Value = serde_yaml::from_str(yaml)
            .map_err(|e| ConfigError::Parse(format!("operator YAML: {e}")))?;
        Self::from_yaml_value_with_defaults(overlay)
    }

    /// [`Self::from_yaml_with_defaults`] over an already-parsed overlay.
    fn from_yaml_value_with_defaults(overlay: serde_yaml::Value) -> Result<Self, ConfigError> {
        // The operator YAML is parsed into a full struct (serde
        // requires all fields). We accept partial YAML via merging
        // serde_yaml::Value onto the default's Value, then
        // re-deserializing.
        let default_v: serde_yaml::Value = serde_yaml::to_value(Self::prescribed_default())
            .map_err(|e| ConfigError::Parse(format!("serialize default: {e}")))?;
        let merged = merge_yaml(default_v, overlay);
        let cfg: Self = serde_yaml::from_value(merged)
            .map_err(|e| ConfigError::Parse(format!("merge round-trip: {e}")))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// **The progressive-fold resolution — the first-class default.**
    ///
    /// Resolves the effective config through shikumi's sealed progressive
    /// fold — `bare() → discovered()[DiscoveryLayer seam] → prescribed_default()
    /// → operator-file overlay` — stamping every effective leaf with its typed
    /// [`shikumi::Provenance`] (which tier produced it). This is the resolution
    /// the daemon boots on: unlike [`TieredConfig::resolve_tier`]`(Default)`
    /// (which returns `prescribed_default()` alone and silently skips
    /// discovery), the fold composes the discovered tier *underneath* the
    /// curated defaults, so a detected value (e.g. the host's name) shows
    /// through and is credited to Discovered.
    ///
    /// The operator file (via the shikumi discovery cascade — `$ENGENHO_CONFIG`
    /// → XDG → `/etc/engenho/engenho.yaml`) is folded in as the top
    /// `Custom`/`File` overlay, so `config-show` can report which leaves came
    /// from the file vs the prescribed defaults vs discovery.
    ///
    /// The returned value is always `validate()`d. The legacy
    /// [`Self::discover`] / [`TieredConfig::resolve_tier`] paths are unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Parse`] if a discovered file can't be read or is
    /// malformed / carries an unknown field, and [`ConfigError::Incoherent`] /
    /// [`ConfigError::InvalidField`] on a validation failure — the same strict
    /// surface [`Self::discover`] enforces (no silent fallback).
    pub fn resolve_progressively() -> Result<ProgressiveResolution<Self>, ConfigError> {
        Self::resolve_progressively_with(None)
    }

    /// [`Self::resolve_progressively`] with the override tier folded over the
    /// declared file: an overridden leaf takes the override's value and is
    /// credited to [`OverrideLayer::path`].
    ///
    /// The strict gate runs over the declared file and the overrides
    /// together — the configuration a boot would get — so an override can
    /// repair a declared file that does not validate on its own, and an
    /// override that breaks one is refused.
    ///
    /// # Errors
    ///
    /// As [`Self::resolve_progressively`]; an error the override tier
    /// caused names it.
    pub fn resolve_progressively_with(
        overrides: Option<&OverrideLayer>,
    ) -> Result<ProgressiveResolution<Self>, ConfigError> {
        let declared = match Self::discover_path() {
            Some(path) => {
                let yaml = std::fs::read_to_string(&path)
                    .map_err(|e| ConfigError::Parse(format!("reading {}: {e}", path.display())))?;
                Some((path, yaml))
            }
            None => None,
        };
        Self::resolve_over(
            declared
                .as_ref()
                .map(|(path, yaml)| (path.as_path(), yaml.as_str())),
            overrides,
        )
    }

    /// The fold over an explicit declared file (`(path, yaml)`) and override
    /// tier. [`Self::resolve_progressively_with`] reads the discovered file
    /// and calls this.
    ///
    /// # Errors
    ///
    /// As [`Self::resolve_progressively_with`].
    pub fn resolve_over(
        declared: Option<(&std::path::Path, &str)>,
        overrides: Option<&OverrideLayer>,
    ) -> Result<ProgressiveResolution<Self>, ConfigError> {
        let overrides = overrides.filter(|o| !o.values.is_empty());
        let declared_value = match declared {
            Some((_, yaml)) if !yaml.trim().is_empty() => serde_yaml::from_str(yaml)
                .map_err(|e| ConfigError::Parse(format!("operator YAML: {e}")))?,
            _ => serde_yaml::Value::Null,
        };

        // The strict gate: deny_unknown_fields and every validator, over
        // exactly what the fold is about to see.
        let mut overlays = Vec::new();
        if let Some(overrides) = overrides {
            let override_value = overrides.to_yaml_value()?;
            Self::from_yaml_value_with_defaults(merge_yaml(declared_value, override_value.clone()))
                .map_err(|e| overrides.blame(e))?;
            if let Some((path, yaml)) = declared {
                overlays.push(ProgressiveLayer::file(path, yaml_to_dict(yaml)?));
            }
            let text = serde_yaml::to_string(&override_value)
                .map_err(|e| ConfigError::Parse(format!("serialize the override tier: {e}")))?;
            overlays.push(ProgressiveLayer::file(
                &overrides.path,
                yaml_to_dict(&text)?,
            ));
        } else if let Some((path, yaml)) = declared {
            Self::from_yaml_value_with_defaults(declared_value)?;
            overlays.push(ProgressiveLayer::file(path, yaml_to_dict(yaml)?));
        }

        // Every overlay is a Custom-tier file layer; the fold's sort is
        // stable, so the override tier (pushed last) wins leaf by leaf.
        let resolution = <Self as TieredConfig>::resolve_progressive_with(&overlays);
        resolution.value().validate()?;
        Ok(resolution)
    }

    /// The progressively-resolved config value alone (drops provenance) —
    /// the convenience wrapper for call sites that just need the config.
    ///
    /// # Errors
    ///
    /// As [`Self::resolve_progressively`].
    pub fn resolve() -> Result<Self, ConfigError> {
        Ok(Self::resolve_progressively()?.into_value())
    }

    /// Render this config as YAML — the operator-facing `config-show` body.
    /// Typed serialization (serde), never a `format!()` of config syntax.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Parse`] if serialization fails (a struct-shaped
    /// config never does in practice).
    pub fn to_yaml(&self) -> Result<String, ConfigError> {
        serde_yaml::to_string(self)
            .map_err(|e| ConfigError::Parse(format!("serialize config: {e}")))
    }
}

/// The override tier: leaves set through the daemon's control plane, folded
/// over the declared file ([`EngenhoConfig::resolve_progressively_with`]).
///
/// This crate only folds it. Keeping it — who set each leaf and when, the
/// generation, the file under `data_dir/control/` — is the daemon's.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OverrideLayer {
    /// Where the tier is kept; provenance credits overridden leaves to it.
    pub path: std::path::PathBuf,
    /// The overriding values, by leaf.
    pub values: std::collections::BTreeMap<LeafPath, serde_json::Value>,
}

impl OverrideLayer {
    fn to_yaml_value(&self) -> Result<serde_yaml::Value, ConfigError> {
        serde_yaml::to_value(leaf::nest(&self.values))
            .map_err(|e| ConfigError::Parse(format!("serialize the override tier: {e}")))
    }

    /// `err`, saying the override tier was folded in when it happened.
    fn blame(&self, err: ConfigError) -> ConfigError {
        let with =
            |what: String| format!("{what} (with the override tier {})", self.path.display());
        match err {
            ConfigError::Parse(what) => ConfigError::Parse(with(what)),
            ConfigError::Incoherent(what) => ConfigError::Incoherent(with(what)),
            other => other,
        }
    }
}

/// Parse partial operator YAML into a figment [`Dict`] for the progressive
/// fold's `File` overlay. Empty / whitespace-only YAML yields an empty dict
/// (a no-op overlay). Uses figment's canonical Yaml provider so the produced
/// dict shape matches the tiers the fold merges it against.
fn yaml_to_dict(yaml: &str) -> Result<Dict, ConfigError> {
    if yaml.trim().is_empty() {
        return Ok(Dict::new());
    }
    Figment::new()
        .merge(Yaml::string(yaml))
        .extract::<Dict>()
        .map_err(|e| ConfigError::Parse(format!("operator YAML overlay dict: {e}")))
}

/// Render a per-leaf provenance summary for the operator `config-show`
/// output — one comment line per effective leaf
/// (`#   <dotted.path>  <-  <tier>[ (source)]`), preceded by a header naming
/// the contributing tiers. Each value routes through the typed
/// [`shikumi::Provenance`] `Display` (typed emission; no `format!()` of the
/// body) — mirroring shikumi's own `ConfigDiff::render_unified` builder style.
#[must_use]
pub fn render_provenance(prov: &ProvenanceMap) -> String {
    let mut out = String::new();
    out.push_str("# provenance: ");
    out.push_str(&prov.len().to_string());
    out.push_str(" leaves; tiers: ");
    let tiers: Vec<&str> = prov
        .contributing_tiers()
        .iter()
        .map(|t| t.as_str())
        .collect();
    out.push_str(&tiers.join(", "));
    out.push('\n');
    for (path, provenance) in prov.entries() {
        out.push_str("#   ");
        out.push_str(&path.join("."));
        out.push_str("  <-  ");
        out.push_str(&provenance.to_string());
        out.push('\n');
    }
    out
}

/// Deep-merge `overlay` onto `base`. Maps merge key-by-key; other
/// values are overwritten by overlay. Arrays are replaced (not
/// concatenated) — operator intent is to specify a full list, not
/// extend defaults.
fn merge_yaml(base: serde_yaml::Value, overlay: serde_yaml::Value) -> serde_yaml::Value {
    match (base, overlay) {
        // Null overlay = "no override" — keep base. Empty operator
        // YAML parses to Null at the root, and unset keys parse to
        // Null inside Mappings; both cases should leave base intact.
        (base, serde_yaml::Value::Null) => base,
        (serde_yaml::Value::Mapping(mut b), serde_yaml::Value::Mapping(o)) => {
            for (k, v) in o {
                let entry = b.remove(&k).unwrap_or(serde_yaml::Value::Null);
                b.insert(k, merge_yaml(entry, v));
            }
            serde_yaml::Value::Mapping(b)
        }
        (_, overlay) => overlay,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prescribed_default_is_valid() {
        EngenhoConfig::prescribed_default().validate().unwrap();
    }

    #[test]
    fn default_is_prescribed_default() {
        assert_eq!(
            EngenhoConfig::default(),
            EngenhoConfig::prescribed_default()
        );
    }

    #[test]
    fn bare_differs_from_prescribed_default() {
        assert_ne!(EngenhoConfig::bare(), EngenhoConfig::prescribed_default());
    }

    #[test]
    fn empty_overlay_yields_defaults() {
        let cfg = EngenhoConfig::from_yaml_with_defaults("").unwrap();
        assert_eq!(cfg, EngenhoConfig::prescribed_default());
    }

    #[test]
    fn partial_overlay_changes_only_specified_fields() {
        let yaml = "\
cluster:
  name: rio
";
        let cfg = EngenhoConfig::from_yaml_with_defaults(yaml).unwrap();
        assert_eq!(cfg.cluster.name, "rio");
        // Other defaults preserved.
        let default = EngenhoConfig::prescribed_default();
        assert_eq!(cfg.scheduler, default.scheduler);
        assert_eq!(cfg.controllers, default.controllers);
    }

    /// T4.9 at the parse boundary: operator YAML that widens a node-local
    /// listener off loopback is refused by the parse itself, not left for a
    /// bind to discover.
    #[test]
    fn yaml_that_widens_a_node_local_listener_is_refused() {
        let cases = [
            (
                "runtime:\n  kubelet_listen_addr: 0.0.0.0:10250\n",
                NodeLocalListener::Kubelet,
            ),
            (
                "runtime:\n  etcd_listen_addr: \"[::]:2379\"\n",
                NodeLocalListener::EtcdFacade,
            ),
        ];
        for (yaml, want) in cases {
            match EngenhoConfig::from_yaml_with_defaults(yaml) {
                Err(ConfigError::NodeLocalListener {
                    listener,
                    rejection: ListenerAddrRejection::NotLoopback(_),
                }) => assert_eq!(listener, want),
                other => panic!("{yaml:?}: expected a NotLoopback refusal, got {other:?}"),
            }
        }
    }

    #[test]
    fn yaml_that_moves_a_node_local_listener_within_loopback_parses() {
        let yaml = "\
runtime:
  kubelet_listen_addr: \"[::1]:10251\"
  etcd_listen_addr: 127.0.0.1:12379
";
        let cfg = EngenhoConfig::from_yaml_with_defaults(yaml).unwrap();
        let kubelet = cfg
            .runtime
            .node_local_addr(NodeLocalListener::Kubelet)
            .unwrap()
            .map(LoopbackAddr::socket_addr);
        let etcd = cfg
            .runtime
            .node_local_addr(NodeLocalListener::EtcdFacade)
            .unwrap()
            .map(LoopbackAddr::socket_addr);
        assert_eq!(kubelet, Some("[::1]:10251".parse().unwrap()));
        assert_eq!(etcd, Some("127.0.0.1:12379".parse().unwrap()));
    }

    #[test]
    fn validate_rejects_incoherent_quorum_topology() {
        let mut cfg = EngenhoConfig::prescribed_default();
        cfg.revoada.topology.strategy = TopologyStrategyKind::Quorum3M;
        cfg.revoada.topology.min_nodes = 1;
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, ConfigError::Incoherent(_)));
    }

    #[test]
    fn round_trip_through_yaml() {
        let original = EngenhoConfig::prescribed_default();
        let yaml = serde_yaml::to_string(&original).unwrap();
        let parsed: EngenhoConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn extend_overlays_self_on_base() {
        let mut overlay = EngenhoConfig::bare();
        overlay.cluster.name = "override".into();
        let base = EngenhoConfig::prescribed_default();
        let merged = overlay.extend(&base);
        // Cluster name took the overlay's value...
        assert_eq!(merged.cluster.name, "override");
    }

    // ── progressive-discovery resolution ────────────────────────────────

    use shikumi::ConfigTierKind;

    /// Collect the dotted paths of every non-null leaf of a serialized config.
    /// Mappings recurse; arrays + scalars are wholesale leaves (matching the
    /// shikumi fold's per-leaf attribution); `null` (a `None` option) is
    /// skipped (it carries no effective value and no provenance entry).
    fn non_null_leaf_paths(v: &serde_yaml::Value, prefix: &[String], out: &mut Vec<Vec<String>>) {
        match v {
            serde_yaml::Value::Mapping(m) => {
                for (k, val) in m {
                    let mut p = prefix.to_vec();
                    p.push(k.as_str().unwrap_or_default().to_string());
                    non_null_leaf_paths(val, &p, out);
                }
            }
            serde_yaml::Value::Null => {}
            _ => out.push(prefix.to_vec()),
        }
    }

    fn leaf_paths_of(cfg: &EngenhoConfig) -> Vec<Vec<String>> {
        let value = serde_yaml::to_value(cfg).unwrap();
        let mut out = Vec::new();
        non_null_leaf_paths(&value, &[], &mut out);
        out
    }

    #[test]
    fn resolve_progressive_folds_to_default_and_validates() {
        // The progressive fold (bare → discovered → prescribed) resolves to the
        // curated defaults where discovery has no opinion, and always validates.
        let r = EngenhoConfig::resolve_progressive();
        r.value().validate().unwrap();
        // The cluster name is now DERIVED per node (see
        // `cluster::default_cluster_name`), so it is no longer a constant
        // this test can pin — asserting `"engenho-local"` here would pass
        // on exactly one machine. Assert the SHAPE and the determinism
        // instead, which is what the default actually promises.
        let name = &r.value().cluster.name;
        assert!(
            name.starts_with("engenho-"),
            "derived cluster name must carry the prefix, got {name}"
        );
        assert_eq!(
            *name,
            crate::cluster::default_cluster_name(),
            "the resolved default must equal the derived name for THIS node"
        );
        assert_eq!(r.value().scheduler.tick_interval_seconds, 5);
    }

    #[test]
    fn progressive_provenance_credits_prescribed_leaves() {
        // A leaf only the prescribed_default tier sets is credited to Default.
        let r = EngenhoConfig::resolve_progressive();
        let prov = r.provenance();
        for path in [
            ["cluster", "name"].as_slice(),
            ["runtime", "listen_addr"].as_slice(),
            ["scheduler", "tick_interval_seconds"].as_slice(),
            ["controllers", "fallback_interval_seconds"].as_slice(),
        ] {
            let p = prov
                .provenance_of(path)
                .unwrap_or_else(|| panic!("no provenance for {path:?}"));
            assert_eq!(p.tier(), ConfigTierKind::Default, "for leaf {path:?}");
        }
    }

    #[test]
    fn node_name_provenance_matches_ambient_hostname() {
        // The discovered() tier shows through the fold for node_name. This
        // reads (never mutates) the ambient $HOSTNAME and asserts the correct
        // branch: detected ⇒ value shows through, credited Discovered; absent
        // ⇒ prescribed fallback, credited Default. Deterministic per-env, no
        // process-environment mutation (edition-2024 `set_var` is unsafe).
        let r = EngenhoConfig::resolve_progressive();
        let p = r
            .provenance()
            .provenance_of(&["runtime", "node_name"])
            .expect("node_name has provenance");
        // ── ★ DETECTION IS NO LONGER `$HOSTNAME`-ONLY ────────────────
        // This used to branch on `$HOSTNAME` alone, because that was the
        // only signal `detected_node_name()` read. It now falls back to a
        // real `gethostname(2)` — which MATTERS, because `$HOSTNAME` is
        // not exported by default on macOS, so the old code took the
        // "absent" branch on every Mac and every node answered to the
        // same fallback name.
        //
        // So the branch is on the DETECTOR, not on one of its inputs.
        // Asserting `$HOSTNAME` here would now be asserting an
        // implementation detail that is no longer the whole mechanism.
        if let Some(host) = crate::discovery::detected_node_name() {
            assert_eq!(r.value().runtime.node_name, host);
            assert_eq!(p.tier(), ConfigTierKind::Discovered);
        } else {
            assert_eq!(r.value().runtime.node_name, NODE_NAME_FALLBACK);
            assert_eq!(p.tier(), ConfigTierKind::Default);
        }
    }

    #[test]
    fn progressive_provenance_complete_over_every_non_null_leaf() {
        // I5 (provenance completeness) over the whole EngenhoConfig leaf-set:
        // every effective non-null leaf of the resolved config carries a
        // provenance entry (the fold seeds from bare(), which enumerates all).
        let r = EngenhoConfig::resolve_progressive();
        let prov = r.provenance();
        let paths = leaf_paths_of(r.value());
        assert!(!paths.is_empty());
        for path in paths {
            assert!(
                prov.provenance_of_owned(&path).is_some(),
                "leaf {path:?} has no provenance"
            );
        }
    }

    #[test]
    fn bare_enumerates_every_field() {
        // The shikumi progressive fold seeds provenance from bare(); this holds
        // only if bare() enumerates every field. Prove it: bare() and
        // prescribed_default() expose the identical non-null leaf-path set.
        let mut bare = leaf_paths_of(&EngenhoConfig::bare());
        let mut pd = leaf_paths_of(&EngenhoConfig::prescribed_default());
        bare.sort();
        pd.sort();
        assert_eq!(bare, pd, "bare() must enumerate every prescribed field");
    }

    #[test]
    fn file_overlay_wins_and_is_credited_custom() {
        // The operator-file overlay (Custom/File tier) beats prescribed defaults
        // and is credited accordingly; untouched leaves stay Default. Built as a
        // partial dict directly (what `file_overlay_layers` produces from YAML)
        // so the fold is exercised with no filesystem / env dependency.
        let mut cluster = Dict::new();
        cluster.insert("name".into(), figment::value::Value::from("rio"));
        let mut root = Dict::new();
        root.insert("cluster".into(), figment::value::Value::from(cluster));
        let overlay = ProgressiveLayer::file("/tmp/engenho.yaml", root);

        let r = <EngenhoConfig as TieredConfig>::resolve_progressive_with(&[overlay]);
        assert_eq!(r.value().cluster.name, "rio");
        assert_eq!(
            r.provenance()
                .provenance_of(&["cluster", "name"])
                .unwrap()
                .tier(),
            ConfigTierKind::Custom
        );
        // A leaf the overlay didn't touch stays credited to the default tier.
        assert_eq!(r.value().cluster.region, "homelab");
        assert_eq!(
            r.provenance()
                .provenance_of(&["cluster", "region"])
                .unwrap()
                .tier(),
            ConfigTierKind::Default
        );
    }

    #[test]
    fn resolve_progressively_produces_valid_config() {
        // The daemon/CLI entry point resolves end-to-end (folding in any
        // discovered operator file) and always returns a validated config.
        let cfg = EngenhoConfig::resolve().unwrap();
        cfg.validate().unwrap();
        assert!(!cfg.cluster.name.is_empty());
        assert!(!cfg.runtime.node_name.is_empty());
    }

    /// ★ ONE CLUSTER IDENTITY — the invariant, not just today's values.
    ///
    /// `teia.cluster` used to be its own hardcoded `"engenho-local"`,
    /// independent of `cluster.name`. That was invisible while both were
    /// the same literal, and became a real split the moment `cluster.name`
    /// went per-node: the cluster called itself one thing and published
    /// its teia subjects under another, so two nodes' meshes collided on
    /// `engenho-local` exactly as per-node naming was meant to prevent.
    ///
    /// Asserting they are EQUAL (rather than asserting either value) is
    /// what stops a future edit to one default from silently re-splitting
    /// them.
    ///
    /// `teia` is no longer a section of `EngenhoConfig` (§5.1), but
    /// [`TeiaConfig`] is still what engenho-teia converts from under
    /// `teia-nats`, so its default must keep agreeing with the cluster.
    #[test]
    fn teia_subject_namespace_matches_the_cluster_identity() {
        let d = EngenhoConfig::prescribed_default();
        let teia = TeiaConfig::prescribed_default();
        assert_eq!(
            teia.cluster, d.cluster.name,
            "teia.cluster and cluster.name must not diverge — a second \
             hardcoded copy of the cluster identity is how they last did"
        );
        assert!(
            d.cluster.name.starts_with("engenho-"),
            "and both must be the DERIVED per-node name, got {}",
            d.cluster.name
        );
    }

    #[test]
    fn config_diff_bare_to_default_is_nonempty() {
        // The `config-diff` CLI path: shikumi ConfigDiff between two tiers.
        let diff = EngenhoConfig::prescribed_default().diff_against(&EngenhoConfig::bare());
        assert!(!diff.is_empty_diff());
        // Was `contains("engenho-local")` — a literal that only held while
        // the cluster name was HARDCODED. `cluster.name` now derives per
        // node, so that string is gone and
        // pinning today's derived value would pass on one machine only.
        // Assert what the test's name actually claims: the rendered diff
        // carries the cluster identity.
        let rendered = diff.render_unified();
        assert!(
            rendered.contains(&crate::cluster::default_cluster_name()),
            "the bare→default diff must show the derived cluster name; got:\n{rendered}"
        );
    }

    // ── fabric: in_binary; the legacy `teia:` key (T5.2, §5.1) ───────────

    /// The `teia:` section as nix/typed-config.nix rendered it (every leaf
    /// set), the partial form an operator could write because the section was
    /// merged onto a default, and the empty mapping.
    const LEGACY_TEIA_SECTIONS: [&str; 3] = [
        "teia:\n  servers: [\"nats://10.0.0.1:4222\"]\n  cluster: rio\n  \
         credentials_path: /etc/nats/engenho.creds\n  connect_timeout_seconds: 15\n",
        "teia:\n  servers: [\"nats://10.0.0.1:4222\"]\n",
        "teia: {}\n",
    ];

    #[test]
    fn the_fabric_is_in_binary_and_spelled_in_binary_on_the_wire() {
        let d = EngenhoConfig::prescribed_default();
        assert_eq!(d.fabric, Fabric::InBinary);
        let v: serde_yaml::Value = serde_yaml::from_str(&d.to_yaml().unwrap()).unwrap();
        assert_eq!(v["fabric"], serde_yaml::Value::from("in_binary"));
        let explicit = EngenhoConfig::from_yaml_with_defaults("fabric: in_binary\n").unwrap();
        assert_eq!(explicit, d);
    }

    /// There is no arm for an external broker, so asking for one is a parse
    /// error, not a setting that boots and is ignored.
    #[test]
    fn a_nats_fabric_is_refused_at_parse() {
        for yaml in ["fabric: nats\n", "fabric: teia\n"] {
            match EngenhoConfig::from_yaml_with_defaults(yaml) {
                Err(ConfigError::Parse(_)) => {}
                other => panic!("{yaml:?}: expected a parse refusal, got {other:?}"),
            }
        }
    }

    #[test]
    fn the_default_config_carries_no_teia_section_and_no_deprecation() {
        let d = EngenhoConfig::prescribed_default();
        let v: serde_yaml::Value = serde_yaml::from_str(&d.to_yaml().unwrap()).unwrap();
        assert!(
            v.get("teia").is_none(),
            "the default config renders a teia section again:\n{}",
            d.to_yaml().unwrap()
        );
        assert_eq!(d.deprecations(), Vec::new());
        assert_eq!(EngenhoConfig::bare().deprecations(), Vec::new());
        assert_eq!(
            EngenhoConfig::resolve_progressive().value().deprecations(),
            Vec::new()
        );
    }

    /// The legacy key is accepted for one release, changes nothing else, and
    /// is reported as a typed deprecation.
    #[test]
    fn a_legacy_teia_section_is_accepted_and_reported() {
        for yaml in LEGACY_TEIA_SECTIONS {
            let cfg = EngenhoConfig::from_yaml_with_defaults(yaml)
                .unwrap_or_else(|e| panic!("{yaml:?}: legacy key refused: {e}"));
            assert_eq!(
                cfg.deprecations(),
                vec![ConfigDeprecation::TeiaSection],
                "{yaml:?}"
            );
            assert_eq!(cfg.fabric, Fabric::InBinary, "{yaml:?}");
            let without = EngenhoConfig {
                legacy_teia: None,
                ..cfg
            };
            assert_eq!(
                without,
                EngenhoConfig::prescribed_default(),
                "{yaml:?}: the legacy key changed something besides itself"
            );
        }
    }

    /// Nothing reads the section, so nothing validates it: an empty server
    /// list or a dotted cluster id (both refused at boot before T5.2) no
    /// longer stop a node.
    #[test]
    fn a_legacy_teia_section_is_not_validated() {
        let yaml = "teia:\n  servers: []\n  cluster: eng.eho\n";
        let cfg = EngenhoConfig::from_yaml_with_defaults(yaml).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.deprecations(), vec![ConfigDeprecation::TeiaSection]);
    }

    /// Accepting the old key is not a licence for new typos: a sub-key the
    /// section never had is still refused, as it was before.
    #[test]
    fn a_legacy_teia_section_still_refuses_unknown_sub_keys() {
        let yaml = "teia:\n  servres: [\"nats://10.0.0.1:4222\"]\n";
        match EngenhoConfig::from_yaml_with_defaults(yaml) {
            Err(ConfigError::Parse(_)) => {}
            other => panic!("expected a parse refusal for `servres`, got {other:?}"),
        }
    }

    /// A full dump written before `fabric` existed (a `teia:` section and no
    /// `fabric:` key) still parses strictly, with no defaults merged in.
    #[test]
    fn a_full_pre_fabric_dump_still_parses() {
        let mut v = serde_yaml::to_value(EngenhoConfig::prescribed_default()).unwrap();
        let map = v.as_mapping_mut().unwrap();
        map.remove("fabric");
        map.insert(
            "teia".into(),
            serde_yaml::from_str("{servers: [\"nats://127.0.0.1:4222\"], cluster: rio}").unwrap(),
        );
        let cfg: EngenhoConfig = serde_yaml::from_value(v).unwrap();
        assert_eq!(cfg.fabric, Fabric::InBinary);
        assert_eq!(cfg.deprecations(), vec![ConfigDeprecation::TeiaSection]);
    }

    /// The daemon boots on the progressive fold, not on
    /// `from_yaml_with_defaults`; the legacy key must survive that path too.
    #[test]
    fn a_legacy_teia_section_reaches_deprecations_through_the_progressive_fold() {
        let mut teia = Dict::new();
        teia.insert(
            "servers".into(),
            figment::value::Value::from(vec!["nats://10.0.0.1:4222".to_string()]),
        );
        let mut root = Dict::new();
        root.insert("teia".into(), figment::value::Value::from(teia));
        let overlay = ProgressiveLayer::file("/etc/engenho/engenho.yaml", root);
        let r = <EngenhoConfig as TieredConfig>::resolve_progressive_with(&[overlay]);
        r.value().validate().unwrap();
        assert_eq!(r.value().fabric, Fabric::InBinary);
        assert_eq!(
            r.value().deprecations(),
            vec![ConfigDeprecation::TeiaSection]
        );
    }

    #[test]
    fn the_teia_deprecation_names_the_key_and_its_replacement() {
        let d = ConfigDeprecation::TeiaSection;
        let rendered = d.to_string();
        assert!(rendered.contains("`teia`"), "{rendered}");
        assert!(rendered.contains("`fabric: in_binary`"), "{rendered}");
        assert_eq!(d.key(), "teia");
        assert_eq!(d.kind(), "teia_section");
    }

    #[test]
    fn to_yaml_round_trips() {
        // The `config-show` body renderer produces YAML that re-parses to the
        // same config.
        let cfg = EngenhoConfig::prescribed_default();
        let yaml = cfg.to_yaml().unwrap();
        let back: EngenhoConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(cfg, back);
    }

    #[test]
    fn render_provenance_names_leaves_and_tiers() {
        // The `config-show` provenance summary names the contributing tiers and
        // one line per leaf, routed through the typed Provenance Display.
        let r = EngenhoConfig::resolve_progressive();
        let rendered = render_provenance(r.provenance());
        assert!(rendered.contains("# provenance:"));
        assert!(rendered.contains("default"));
        assert!(rendered.contains("cluster.name  <-  default"));
    }

    // ── the override tier ─────────────────────────────────────────────────

    fn overrides(pairs: &[(&str, serde_json::Value)]) -> OverrideLayer {
        OverrideLayer {
            path: std::path::PathBuf::from("/data/control/overrides.yaml"),
            values: pairs
                .iter()
                .map(|(path, value)| (LeafPath::parse(path).unwrap(), value.clone()))
                .collect(),
        }
    }

    fn source_of<'a>(
        r: &'a ProgressiveResolution<EngenhoConfig>,
        path: &[&str],
    ) -> Option<&'a std::path::Path> {
        r.provenance()
            .provenance_of(path)
            .and_then(|p| p.source().as_path())
    }

    const DECLARED: &str =
        "scheduler:\n  tick_interval_seconds: 7\ncontrollers:\n  namespace: declared\n";

    #[test]
    fn an_override_wins_its_leaf_and_is_credited_to_its_file() {
        let declared = std::path::Path::new("/etc/engenho/engenho.yaml");
        let tier = overrides(&[("scheduler.tick_interval_seconds", serde_json::json!(9))]);
        let r = EngenhoConfig::resolve_over(Some((declared, DECLARED)), Some(&tier)).unwrap();

        assert_eq!(
            r.value().scheduler.tick_interval_seconds,
            9,
            "the override wins"
        );
        assert_eq!(
            r.value().controllers.namespace,
            "declared",
            "the rest is the declared file's"
        );
        assert_eq!(
            source_of(&r, &["scheduler", "tick_interval_seconds"]),
            Some(tier.path.as_path())
        );
        assert_eq!(source_of(&r, &["controllers", "namespace"]), Some(declared));
    }

    #[test]
    fn no_overrides_is_the_declared_fold() {
        let declared = std::path::Path::new("/etc/engenho/engenho.yaml");
        let with_empty =
            EngenhoConfig::resolve_over(Some((declared, DECLARED)), Some(&overrides(&[]))).unwrap();
        let without = EngenhoConfig::resolve_over(Some((declared, DECLARED)), None).unwrap();
        assert_eq!(with_empty.value(), without.value());
        assert_eq!(with_empty.provenance(), without.provenance());
    }

    #[test]
    fn the_gate_sees_the_declared_file_and_the_overrides_together() {
        let declared = std::path::Path::new("/etc/engenho/engenho.yaml");
        let broken = "scheduler:\n  tick_interval_seconds: 0\n";
        assert!(EngenhoConfig::resolve_over(Some((declared, broken)), None).is_err());

        // An override can repair what the declared file broke...
        let repair = overrides(&[("scheduler.tick_interval_seconds", serde_json::json!(5))]);
        let repaired =
            EngenhoConfig::resolve_over(Some((declared, broken)), Some(&repair)).unwrap();
        assert_eq!(repaired.value().scheduler.tick_interval_seconds, 5);

        // ...and breaking a good one is refused, naming the tier.
        let breaks = overrides(&[("scheduler.tick_interval_seconds", serde_json::json!("soon"))]);
        let err =
            EngenhoConfig::resolve_over(Some((declared, DECLARED)), Some(&breaks)).unwrap_err();
        assert!(err.to_string().contains("override tier"), "{err}");
        let unknown = overrides(&[("scheduler.not_a_leaf", serde_json::json!(1))]);
        assert!(EngenhoConfig::resolve_over(None, Some(&unknown)).is_err());
    }
}
