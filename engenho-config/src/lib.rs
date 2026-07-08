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
mod controllers;
mod discovery;
mod error;
mod networking;
mod revoada;
mod runtime;
mod scheduler;
mod teia;
mod tls;

pub use cluster::ClusterConfig;
pub use consistency::{ConsistencyConfig, ConsistencyTierKind};
pub use controllers::{ControllerEnable, ControllersConfig};
pub use discovery::{HostnameLayer, NODE_NAME_FALLBACK};
pub use error::ConfigError;
pub use networking::{DatapathMode, NetworkingConfig, ResolvedDatapath, parse_ipv4_cidr};
pub use revoada::{RevoadaConfig, TopologyConfig, TopologyStrategyKind};
pub use runtime::{KubeletBackendKind, RuntimeConfig};
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
    /// NATS fabric.
    pub teia: TeiaConfig,
    /// Scheduler tunables.
    pub scheduler: SchedulerConfig,
    /// Controller suite tunables + per-controller toggles.
    pub controllers: ControllersConfig,
    /// Per-resource ConsistencyTier defaults.
    pub consistency: ConsistencyConfig,
    /// Cluster networking — the Service ClusterIP CIDR. `#[serde(default)]`
    /// is REQUIRED (the struct is `deny_unknown_fields`) so operator YAML
    /// written before this section existed still deserializes; the default
    /// is the upstream `10.96.0.0/12` range.
    #[serde(default = "NetworkingConfig::prescribed_default")]
    pub networking: NetworkingConfig,
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
            networking: NetworkingConfig::bare(),
            runtime: RuntimeConfig::bare(),
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
            teia: TeiaConfig::discovered(),
            scheduler: SchedulerConfig::discovered(),
            controllers: ControllersConfig::discovered(),
            consistency: ConsistencyConfig::discovered(),
            networking: NetworkingConfig::discovered(),
            runtime: RuntimeConfig::discovered(),
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
            networking: NetworkingConfig::prescribed_default(),
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
            networking: self.networking.extend(&base.networking),
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
        self.networking.validate()?;
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
        let overlays = Self::file_overlay_layers()?;
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

    /// Build the operator-file overlay layer(s) for the progressive fold from
    /// the shikumi discovery cascade. Empty when no config file is found (the
    /// fold then resolves to the three trait tiers alone).
    ///
    /// The discovered file is first run through the proven
    /// [`Self::from_yaml_with_defaults`] path as a **strict gate**, so a
    /// malformed / unknown-field / cross-section-invalid overlay errors here
    /// exactly as [`Self::discover`] would — never a silent fallback. The
    /// overlay itself is the *partial* operator dict (only the keys the
    /// operator set), stamped with `File` provenance.
    fn file_overlay_layers() -> Result<Vec<ProgressiveLayer>, ConfigError> {
        let Some(path) = Self::discover_path() else {
            return Ok(Vec::new());
        };
        let yaml = std::fs::read_to_string(&path)
            .map_err(|e| ConfigError::Parse(format!("reading {}: {e}", path.display())))?;
        // Strict gate — reuse the deny_unknown_fields + cross-section validate
        // path so a bad overlay surfaces the same error the legacy loader does.
        Self::from_yaml_with_defaults(&yaml)?;
        let dict = yaml_to_dict(&yaml)?;
        Ok(vec![ProgressiveLayer::file(path, dict)])
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
        assert_eq!(r.value().cluster.name, "engenho-local");
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
        match std::env::var("HOSTNAME").ok().filter(|h| !h.is_empty()) {
            Some(host) => {
                assert_eq!(r.value().runtime.node_name, host);
                assert_eq!(p.tier(), ConfigTierKind::Discovered);
            }
            None => {
                assert_eq!(r.value().runtime.node_name, NODE_NAME_FALLBACK);
                assert_eq!(p.tier(), ConfigTierKind::Default);
            }
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

    #[test]
    fn config_diff_bare_to_default_is_nonempty() {
        // The `config-diff` CLI path: shikumi ConfigDiff between two tiers.
        let diff = EngenhoConfig::prescribed_default().diff_against(&EngenhoConfig::bare());
        assert!(!diff.is_empty_diff());
        assert!(diff.render_unified().contains("engenho-local"));
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
}
