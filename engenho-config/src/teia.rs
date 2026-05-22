//! NATS fabric config — teia (the cross-cluster transport).

use serde::{Deserialize, Serialize};
use shikumi::TieredConfig;

use crate::error::ConfigError;

/// NATS fabric config — mirrors engenho-teia::TeiaConfig.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeiaConfig {
    /// NATS server URLs. Order matters: client tries each in
    /// sequence on connect.
    pub servers: Vec<String>,
    /// Logical engenho cluster identifier (used as the second
    /// component of every subject).
    pub cluster: String,
    /// Optional JWT credentials path.
    pub credentials_path: Option<String>,
    /// Connect timeout in seconds (total).
    pub connect_timeout_seconds: u32,
}

impl TieredConfig for TeiaConfig {
    fn bare() -> Self {
        Self {
            servers: Vec::new(),
            cluster: String::new(),
            credentials_path: None,
            connect_timeout_seconds: 0,
        }
    }

    fn prescribed_default() -> Self {
        Self {
            servers: vec!["nats://127.0.0.1:4222".into()],
            cluster: "engenho-local".into(),
            credentials_path: None,
            connect_timeout_seconds: 10,
        }
    }

    fn extend(self, base: &Self) -> Self {
        Self {
            servers: if self.servers.is_empty() {
                base.servers.clone()
            } else {
                self.servers
            },
            cluster: if self.cluster.is_empty() {
                base.cluster.clone()
            } else {
                self.cluster
            },
            credentials_path: self.credentials_path.or_else(|| base.credentials_path.clone()),
            connect_timeout_seconds: if self.connect_timeout_seconds == 0 {
                base.connect_timeout_seconds
            } else {
                self.connect_timeout_seconds
            },
        }
    }
}

impl TeiaConfig {
    /// Validate the teia config.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidField`] when the servers list
    /// is empty (would fail to connect on the first publish).
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.servers.is_empty() {
            return Err(ConfigError::InvalidField {
                field: "teia.servers".into(),
                reason: "at least one NATS server URL required".into(),
            });
        }
        if self.cluster.contains('.') || self.cluster.contains(' ') {
            return Err(ConfigError::InvalidField {
                field: "teia.cluster".into(),
                reason: "cluster identifier must not contain dots/spaces".into(),
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
        TeiaConfig::prescribed_default().validate().unwrap();
    }

    #[test]
    fn bare_fails_validation_due_to_empty_servers() {
        assert!(TeiaConfig::bare().validate().is_err());
    }

    #[test]
    fn extend_fills_empty_servers_from_base() {
        let overlay = TeiaConfig {
            servers: Vec::new(),
            cluster: "x".into(),
            credentials_path: None,
            connect_timeout_seconds: 0,
        };
        let base = TeiaConfig::prescribed_default();
        let merged = overlay.extend(&base);
        assert!(!merged.servers.is_empty());
        assert_eq!(merged.cluster, "x");
        assert_eq!(merged.connect_timeout_seconds, 10);
    }
}
