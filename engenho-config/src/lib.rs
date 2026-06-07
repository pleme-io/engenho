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
//!   * [`TeiaConfig`] — NATS fabric (servers, cluster, leaf-nodes)
//!   * [`SchedulerConfig`] — engenho-scheduler tunables
//!   * [`ControllersConfig`] — engenho-controllers tunables + per-controller toggles
//!   * [`ConsistencyConfig`] — per-resource ConsistencyTier defaults
//!
//! Each sub-struct implements `shikumi::TieredConfig`:
//!
//!   * `bare()` — zero-opinion floor (empty names, disabled features)
//!   * `prescribed_default()` — what 90% of operators want on first launch
//!     (Phalanx topology, in-process NATS, every controller enabled,
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
//! teia:
//!   servers: ["nats://engenho-nats:4222"]
//!   cluster: rio
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
//! Tier 4: ConfigMap (future, hot-reload)
//! Tier 5: prescribed_default() — compiled-in safe values
//! ```
//!
//! ## Validation
//!
//! [`EngenhoConfig::validate`] runs cross-section coherence checks
//! (e.g. Solo topology + 3-node consensus quorum is incoherent;
//! Phalanx with min_nodes=0 makes no sense). Returns [`ConfigError`]
//! naming the violated invariant.

#![warn(clippy::pedantic)]
#![warn(missing_docs)]
#![allow(clippy::module_name_repetitions)]

use serde::{Deserialize, Serialize};
use shikumi::TieredConfig;

mod cluster;
mod consistency;
mod controllers;
mod error;
mod revoada;
mod runtime;
mod scheduler;
mod teia;

pub use cluster::ClusterConfig;
pub use consistency::{ConsistencyConfig, ConsistencyTierKind};
pub use controllers::{ControllerEnable, ControllersConfig};
pub use error::ConfigError;
pub use revoada::{RevoadaConfig, TopologyConfig, TopologyStrategyKind};
pub use runtime::{KubeletBackendKind, RuntimeConfig};
pub use scheduler::{SchedulerConfig, SchedulerStrategyKind};
pub use teia::TeiaConfig;

/// The top-level engenho config. One per process.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngenhoConfig {
    /// Cluster identity (name, region).
    pub cluster: ClusterConfig,
    /// Distribution layer (topology + fabric + membership).
    pub revoada: RevoadaConfig,
    /// NATS fabric.
    pub teia: TeiaConfig,
    /// Scheduler tunables.
    pub scheduler: SchedulerConfig,
    /// Controller suite tunables + per-controller toggles.
    pub controllers: ControllersConfig,
    /// Per-resource ConsistencyTier defaults.
    pub consistency: ConsistencyConfig,
    /// Single-process assembly knobs (listen addr, data dir, node
    /// name, kubelet backend, leadership timeout).
    pub runtime: RuntimeConfig,
}

impl TieredConfig for EngenhoConfig {
    fn bare() -> Self {
        Self {
            cluster: ClusterConfig::bare(),
            revoada: RevoadaConfig::bare(),
            teia: TeiaConfig::bare(),
            scheduler: SchedulerConfig::bare(),
            controllers: ControllersConfig::bare(),
            consistency: ConsistencyConfig::bare(),
            runtime: RuntimeConfig::bare(),
        }
    }

    fn prescribed_default() -> Self {
        Self {
            cluster: ClusterConfig::prescribed_default(),
            revoada: RevoadaConfig::prescribed_default(),
            teia: TeiaConfig::prescribed_default(),
            scheduler: SchedulerConfig::prescribed_default(),
            controllers: ControllersConfig::prescribed_default(),
            consistency: ConsistencyConfig::prescribed_default(),
            runtime: RuntimeConfig::prescribed_default(),
        }
    }

    fn extend(self, base: &Self) -> Self {
        Self {
            cluster: self.cluster.extend(&base.cluster),
            revoada: self.revoada.extend(&base.revoada),
            teia: self.teia.extend(&base.teia),
            scheduler: self.scheduler.extend(&base.scheduler),
            controllers: self.controllers.extend(&base.controllers),
            consistency: self.consistency.extend(&base.consistency),
            runtime: self.runtime.extend(&base.runtime),
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
    ///   * Quorum requires ≥3 nodes but topology min_nodes < 3
    ///   * Scheduler tick interval is zero (would hot-loop)
    ///   * Controllers fallback interval is zero (same)
    ///   * Empty cluster name
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.cluster.validate()?;
        self.revoada.validate()?;
        self.teia.validate()?;
        self.scheduler.validate()?;
        self.controllers.validate()?;
        self.consistency.validate()?;
        self.runtime.validate()?;

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
        match Self::discover_path() {
            Some(path) => {
                let yaml = std::fs::read_to_string(&path).map_err(|e| {
                    ConfigError::Parse(format!("reading {}: {e}", path.display()))
                })?;
                Self::from_yaml_with_defaults(&yaml)
            }
            None => {
                let cfg = Self::prescribed_default();
                cfg.validate()?;
                Ok(cfg)
            }
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
        // The operator YAML is parsed into a full struct (serde
        // requires all fields). We accept partial YAML via merging
        // serde_yaml::Value onto the default's Value, then
        // re-deserializing.
        let default_v: serde_yaml::Value = serde_yaml::to_value(Self::prescribed_default())
            .map_err(|e| ConfigError::Parse(format!("serialize default: {e}")))?;
        let overlay: serde_yaml::Value = serde_yaml::from_str(yaml)
            .map_err(|e| ConfigError::Parse(format!("operator YAML: {e}")))?;
        let merged = merge_yaml(default_v, overlay);
        let cfg: Self = serde_yaml::from_value(merged)
            .map_err(|e| ConfigError::Parse(format!("merge round-trip: {e}")))?;
        cfg.validate()?;
        Ok(cfg)
    }
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
}
