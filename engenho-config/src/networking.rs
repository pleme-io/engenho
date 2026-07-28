//! Cluster networking config — the Service VIP (`ClusterIP`) range.
//!
//! The `service_cidr` is the address pool the `ClusterIP` allocator
//! (`engenho_controllers::cluster_ip`) draws Service virtual IPs from.
//! Upstream Kubernetes defaults this to `10.96.0.0/12`; engenho mirrors
//! that default so a Service created with no explicit `clusterIP` lands
//! a VIP in the same range kube-proxy + every consuming controller
//! expect.

use serde::{Deserialize, Serialize};
use shikumi::TieredConfig;

use crate::error::ConfigError;

/// Which Service VIP datapath backend the `ServiceRoutingController`
/// installs — the typed, operator-overridable selection of *how* a
/// Service's computed routes reach the kernel (or whether they do).
///
/// `Auto` (the default) platform-detects: on a Linux node it resolves to
/// `Iptables` (the kernel installs the `KUBE-SVC`/`KUBE-SEP` chains); on a
/// non-Linux host (e.g. engenho on Darwin with pods in a podman Linux VM,
/// the bootstrap dev topology) it resolves to `ComputeOnly`, where the
/// controller still runs + computes + observes the desired rules but never
/// shells to a non-existent `iptables-restore`. The explicit arms let an
/// operator force a backend (or force compute-only) regardless of platform.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatapathMode {
    /// Platform-detect: Linux → `Iptables`, non-Linux → `ComputeOnly`.
    Auto,
    /// Force the iptables kernel backend (a Linux node).
    Iptables,
    /// Force the ipvs kernel backend (a Linux node, scalable).
    Ipvs,
    /// Force compute-only: routes are computed + observable, no kernel
    /// install attempted. The fail-safe state on a non-Linux host.
    ComputeOnly,
}

/// The concrete datapath backend `Auto` (or an explicit arm) resolves to
/// once the host platform is known — a kernel backend or compute-only.
/// Pure output of [`DatapathMode::resolve`]; the runtime maps each arm to
/// the matching `ServiceRouter` implementation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolvedDatapath {
    /// Install the iptables kernel datapath.
    Iptables,
    /// Install the ipvs kernel datapath.
    Ipvs,
    /// Compute routes only; install nothing in the kernel.
    ComputeOnly,
}

impl DatapathMode {
    /// Resolve this mode against a host platform (`is_linux`) into the
    /// concrete backend the runtime should construct.
    ///
    /// Pure — takes `is_linux` explicitly (rather than reading `cfg!`) so
    /// the platform-selection logic is directly unit-testable for both
    /// arms. The runtime calls `resolve(cfg!(target_os = "linux"))`.
    ///
    /// - `Auto` + Linux → `Iptables`; `Auto` + non-Linux → `ComputeOnly`.
    /// - `Iptables` / `Ipvs` / `ComputeOnly` → the matching arm verbatim
    ///   (an operator override is honored on any platform — forcing a
    ///   kernel backend off-Linux is the operator's explicit choice, and
    ///   the backend's own spawn would surface the missing binary as a
    ///   typed `RouterError::Backend`, never a silent skip).
    #[must_use]
    pub fn resolve(self, is_linux: bool) -> ResolvedDatapath {
        match self {
            Self::Auto => {
                if is_linux {
                    ResolvedDatapath::Iptables
                } else {
                    ResolvedDatapath::ComputeOnly
                }
            }
            Self::Iptables => ResolvedDatapath::Iptables,
            Self::Ipvs => ResolvedDatapath::Ipvs,
            Self::ComputeOnly => ResolvedDatapath::ComputeOnly,
        }
    }
}

/// serde default for `datapath_mode` — `Auto` so an operator YAML written
/// before the field existed (a `deny_unknown_fields` struct) still
/// deserializes, platform-detecting the backend (mirrors
/// `prescribed_default`).
fn default_datapath_mode() -> DatapathMode {
    DatapathMode::Auto
}

/// Networking config — the Service `ClusterIP` CIDR + VIP datapath backend.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkingConfig {
    /// The CIDR the `ClusterIP` allocator draws Service VIPs from. The
    /// allocator skips the network address (first host) per upstream
    /// convention and never reuses a VIP held by a live Service.
    /// Default `10.96.0.0/12` (upstream parity).
    pub service_cidr: String,
    /// Which Service VIP datapath backend the `ServiceRoutingController`
    /// installs. Default `Auto` (platform-detect: Linux → iptables,
    /// non-Linux → compute-only). `#[serde(default)]` is REQUIRED (the
    /// struct is `deny_unknown_fields`) so pre-existing operator YAML
    /// written before this field still deserializes.
    #[serde(default = "default_datapath_mode")]
    pub datapath_mode: DatapathMode,
}

impl TieredConfig for NetworkingConfig {
    fn bare() -> Self {
        Self {
            service_cidr: String::new(),
            datapath_mode: DatapathMode::Auto,
        }
    }

    fn prescribed_default() -> Self {
        Self {
            // Upstream kube-apiserver default service CIDR.
            service_cidr: "10.96.0.0/12".into(),
            // Platform-detect: Linux installs the kernel datapath, a
            // non-Linux dev host runs compute-only.
            datapath_mode: DatapathMode::Auto,
        }
    }

    fn extend(self, base: &Self) -> Self {
        // `datapath_mode` is `Copy` with a meaningful zero-arg default
        // (`Auto`); an overlay always carries an explicit value, so take
        // the overlay's verbatim (no "empty-means-inherit" sentinel exists
        // for an enum — `Auto` IS the inherit-platform behavior).
        let _ = base;
        Self {
            service_cidr: if self.service_cidr.is_empty() {
                base.service_cidr.clone()
            } else {
                self.service_cidr
            },
            datapath_mode: self.datapath_mode,
        }
    }
}

impl NetworkingConfig {
    /// Validate the networking config.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidField`] when `service_cidr` is empty
    /// or not a parseable `a.b.c.d/prefix` IPv4 CIDR.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.service_cidr.is_empty() {
            return Err(ConfigError::InvalidField {
                field: "networking.service_cidr".into(),
                reason: "service CIDR cannot be empty".into(),
            });
        }
        // Parse `a.b.c.d/prefix` into octets + prefix; a malformed value
        // is a typed config error, never a silent fallback.
        parse_ipv4_cidr(&self.service_cidr).map_err(|reason| ConfigError::InvalidField {
            field: "networking.service_cidr".into(),
            reason,
        })?;
        Ok(())
    }
}

/// Parse an IPv4 CIDR `a.b.c.d/prefix` into `(base_u32, prefix_len)`.
///
/// Pure — shared by the config validator + the allocator so both agree
/// on what a legal CIDR is (solve-once). `prefix` must be `0..=32`.
///
/// # Errors
/// A human-readable reason string on any malformed input.
pub fn parse_ipv4_cidr(cidr: &str) -> Result<(u32, u8), String> {
    let (addr, prefix) = cidr
        .split_once('/')
        .ok_or_else(|| format!("'{cidr}' is not a CIDR (missing '/prefix')"))?;
    let prefix: u8 = prefix
        .parse()
        .map_err(|_| format!("'{cidr}' has a non-numeric prefix"))?;
    if prefix > 32 {
        return Err(format!("'{cidr}' prefix {prefix} exceeds 32"));
    }
    let octets: Vec<&str> = addr.split('.').collect();
    if octets.len() != 4 {
        return Err(format!("'{addr}' is not a dotted-quad IPv4 address"));
    }
    let mut base: u32 = 0;
    for oct in octets {
        let v: u8 = oct
            .parse()
            .map_err(|_| format!("'{addr}' has a non-numeric octet '{oct}'"))?;
        base = (base << 8) | u32::from(v);
    }
    Ok((base, prefix))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prescribed_default_validates() {
        NetworkingConfig::prescribed_default().validate().unwrap();
    }

    #[test]
    fn bare_fails_validation() {
        assert!(NetworkingConfig::bare().validate().is_err());
    }

    #[test]
    fn prescribed_default_is_upstream_range() {
        assert_eq!(
            NetworkingConfig::prescribed_default().service_cidr,
            "10.96.0.0/12"
        );
    }

    #[test]
    fn extend_fills_empty_from_base() {
        let overlay = NetworkingConfig {
            service_cidr: String::new(),
            datapath_mode: DatapathMode::Auto,
        };
        let base = NetworkingConfig::prescribed_default();
        let merged = overlay.extend(&base);
        assert_eq!(merged.service_cidr, "10.96.0.0/12");
    }

    #[test]
    fn extend_keeps_override() {
        let overlay = NetworkingConfig {
            service_cidr: "10.43.0.0/16".into(),
            datapath_mode: DatapathMode::Ipvs,
        };
        let base = NetworkingConfig::prescribed_default();
        let merged = overlay.extend(&base);
        assert_eq!(merged.service_cidr, "10.43.0.0/16");
        // The overlay's explicit datapath_mode wins over the base default.
        assert_eq!(merged.datapath_mode, DatapathMode::Ipvs);
    }

    #[test]
    fn prescribed_default_datapath_mode_is_auto() {
        assert_eq!(
            NetworkingConfig::prescribed_default().datapath_mode,
            DatapathMode::Auto
        );
    }

    #[test]
    fn datapath_auto_resolves_by_platform() {
        // Auto on a Linux node installs the iptables kernel datapath.
        assert_eq!(DatapathMode::Auto.resolve(true), ResolvedDatapath::Iptables);
        // Auto off-Linux (Darwin dev host) runs compute-only — no kernel
        // install is attempted, so the local daemon keeps running fine.
        assert_eq!(
            DatapathMode::Auto.resolve(false),
            ResolvedDatapath::ComputeOnly
        );
    }

    #[test]
    fn datapath_explicit_arms_ignore_platform() {
        // An explicit override is honored regardless of host platform.
        assert_eq!(
            DatapathMode::Iptables.resolve(false),
            ResolvedDatapath::Iptables
        );
        assert_eq!(DatapathMode::Ipvs.resolve(true), ResolvedDatapath::Ipvs);
        assert_eq!(
            DatapathMode::ComputeOnly.resolve(true),
            ResolvedDatapath::ComputeOnly
        );
    }

    #[test]
    fn pre_existing_yaml_without_datapath_mode_still_deserializes() {
        // The serde default contract: an operator YAML written before
        // `datapath_mode` existed (deny_unknown_fields struct) still
        // deserializes, defaulting to Auto.
        let yaml = "service_cidr: \"10.96.0.0/12\"\n";
        let cfg: NetworkingConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.datapath_mode, DatapathMode::Auto);
    }

    #[test]
    fn datapath_mode_serde_snake_case() {
        let yaml = "service_cidr: \"10.96.0.0/12\"\ndatapath_mode: compute_only\n";
        let cfg: NetworkingConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.datapath_mode, DatapathMode::ComputeOnly);
    }

    #[test]
    fn parse_cidr_extracts_base_and_prefix() {
        let (base, prefix) = parse_ipv4_cidr("10.96.0.0/12").unwrap();
        assert_eq!(prefix, 12);
        // 10.96.0.0 = 0x0A_60_00_00
        assert_eq!(base, 0x0A60_0000);
    }

    #[test]
    fn parse_cidr_rejects_malformed() {
        assert!(parse_ipv4_cidr("not-a-cidr").is_err());
        assert!(parse_ipv4_cidr("10.96.0.0").is_err());
        assert!(parse_ipv4_cidr("10.96.0/12").is_err());
        assert!(parse_ipv4_cidr("10.96.0.0/33").is_err());
        assert!(parse_ipv4_cidr("10.96.0.x/12").is_err());
    }

    #[test]
    fn validate_rejects_malformed_cidr() {
        let cfg = NetworkingConfig {
            service_cidr: "10.96.0.0".into(),
            datapath_mode: DatapathMode::Auto,
        };
        assert!(cfg.validate().is_err());
    }
}
