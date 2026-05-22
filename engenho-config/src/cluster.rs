//! Cluster identity config.

use serde::{Deserialize, Serialize};
use shikumi::TieredConfig;

use crate::error::ConfigError;

/// Cluster identity — name + region.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    /// Cluster name (e.g. "rio", "engenho-local"). Used as the
    /// teia subject namespace + Helm release name + per-resource
    /// labels.
    pub name: String,
    /// Cloud region or homelab location identifier.
    pub region: String,
}

impl TieredConfig for ClusterConfig {
    fn bare() -> Self {
        Self {
            name: String::new(),
            region: String::new(),
        }
    }

    fn prescribed_default() -> Self {
        Self {
            name: "engenho-local".into(),
            region: "homelab".into(),
        }
    }

    fn extend(self, base: &Self) -> Self {
        Self {
            name: if self.name.is_empty() {
                base.name.clone()
            } else {
                self.name
            },
            region: if self.region.is_empty() {
                base.region.clone()
            } else {
                self.region
            },
        }
    }
}

impl ClusterConfig {
    /// Validate the cluster config.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidField`] if name is empty or
    /// contains dots/spaces (the substrate uses cluster name in
    /// NATS subjects which require alphanumeric tokens).
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.name.is_empty() {
            return Err(ConfigError::InvalidField {
                field: "cluster.name".into(),
                reason: "cluster name cannot be empty".into(),
            });
        }
        if self.name.contains('.') || self.name.contains(' ') {
            return Err(ConfigError::InvalidField {
                field: "cluster.name".into(),
                reason: format!("cluster name '{}' contains invalid chars (.,space)", self.name),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prescribed_default_validates() {
        ClusterConfig::prescribed_default().validate().unwrap();
    }

    #[test]
    fn bare_fails_validation() {
        assert!(ClusterConfig::bare().validate().is_err());
    }

    #[test]
    fn name_with_dot_rejected() {
        let cfg = ClusterConfig {
            name: "rio.example".into(),
            region: "x".into(),
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn extend_fills_empty_fields_from_base() {
        let overlay = ClusterConfig {
            name: "myname".into(),
            region: String::new(),
        };
        let base = ClusterConfig::prescribed_default();
        let merged = overlay.extend(&base);
        assert_eq!(merged.name, "myname");
        assert_eq!(merged.region, "homelab");
    }
}
