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

/// Networking config — the Service `ClusterIP` CIDR.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkingConfig {
    /// The CIDR the `ClusterIP` allocator draws Service VIPs from. The
    /// allocator skips the network address (first host) per upstream
    /// convention and never reuses a VIP held by a live Service.
    /// Default `10.96.0.0/12` (upstream parity).
    pub service_cidr: String,
}

impl TieredConfig for NetworkingConfig {
    fn bare() -> Self {
        Self {
            service_cidr: String::new(),
        }
    }

    fn prescribed_default() -> Self {
        Self {
            // Upstream kube-apiserver default service CIDR.
            service_cidr: "10.96.0.0/12".into(),
        }
    }

    fn extend(self, base: &Self) -> Self {
        Self {
            service_cidr: if self.service_cidr.is_empty() {
                base.service_cidr.clone()
            } else {
                self.service_cidr
            },
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
        };
        let base = NetworkingConfig::prescribed_default();
        let merged = overlay.extend(&base);
        assert_eq!(merged.service_cidr, "10.96.0.0/12");
    }

    #[test]
    fn extend_keeps_override() {
        let overlay = NetworkingConfig {
            service_cidr: "10.43.0.0/16".into(),
        };
        let base = NetworkingConfig::prescribed_default();
        assert_eq!(overlay.extend(&base).service_cidr, "10.43.0.0/16");
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
        };
        assert!(cfg.validate().is_err());
    }
}
