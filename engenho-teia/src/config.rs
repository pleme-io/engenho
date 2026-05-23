//! `TeiaConfig` — the typed NATS connection config.
//!
//! Sane defaults for an engenho-local cluster; production callers
//! configure via shikumi-style TOML loaded from
//! `$ENGENHO_CONFIG/teia.toml` (wiring lives in engenho-revoada +
//! engenho-store callers).

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::TeiaError;

/// Wire engenho-config's top-level TeiaConfig into this runtime
/// TeiaConfig. Operators set one yaml file; this conversion gives
/// the teia runtime its exact slice. Server-name list and cluster
/// name are passed through verbatim; the runtime applies per-attempt
/// + total timeouts derived from the single `connect_timeout_seconds`
/// (per-attempt is 1/3 of total, capped at 3s).
impl From<&engenho_config::TeiaConfig> for TeiaConfig {
    fn from(top: &engenho_config::TeiaConfig) -> Self {
        let total = Duration::from_secs(u64::from(top.connect_timeout_seconds));
        let per_attempt = total
            .checked_div(3)
            .unwrap_or(Duration::from_secs(3))
            .min(Duration::from_secs(3));
        Self {
            servers: top.servers.clone(),
            cluster: top.cluster.clone(),
            credentials_path: top.credentials_path.clone(),
            connect_timeout: total,
            server_timeout: per_attempt,
            client_name: Some("engenho-teia".to_string()),
            leaf_remotes: Vec::new(),
        }
    }
}

impl From<engenho_config::TeiaConfig> for TeiaConfig {
    fn from(top: engenho_config::TeiaConfig) -> Self {
        (&top).into()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TeiaConfig {
    /// NATS server URLs (any of: `nats://host:port`, `tls://host:port`,
    /// `nats-leaf://host:port` for leaf-node connection).
    /// Order matters: client tries each in sequence on connect.
    pub servers: Vec<String>,

    /// Logical engenho cluster identifier. Used as the second
    /// component of every subject: `engenho.{cluster}.*`. Different
    /// engenho clusters can share a NATS supercluster.
    pub cluster: String,

    /// Optional JWT credentials path (NSC-style). If `None`,
    /// connects anonymously (suitable for in-cluster localhost
    /// connections behind an Ingress).
    pub credentials_path: Option<String>,

    /// Maximum total connect timeout. After this we give up
    /// (caller handles retry).
    pub connect_timeout: Duration,

    /// Per-attempt connect timeout for each server URL.
    pub server_timeout: Duration,

    /// Optional client name advertised to NATS for ops visibility.
    pub client_name: Option<String>,

    /// Leaf-node remotes (this teia client's perspective). When
    /// populated, this client connects via the leaf-node port
    /// (typically 7422) instead of the regular port. Used for
    /// edge clusters dialing out to a regional hub.
    #[serde(default)]
    pub leaf_remotes: Vec<LeafNodeRemote>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeafNodeRemote {
    pub url: String,
    pub credentials_path: Option<String>,
}

impl Default for TeiaConfig {
    fn default() -> Self {
        Self {
            servers: vec!["nats://127.0.0.1:4222".to_string()],
            cluster: "engenho-local".to_string(),
            credentials_path: None,
            connect_timeout: Duration::from_secs(10),
            server_timeout: Duration::from_secs(3),
            client_name: Some("engenho-teia".to_string()),
            leaf_remotes: Vec::new(),
        }
    }
}

impl TeiaConfig {
    /// Construct a config for an in-cluster engenho deployment
    /// using the standard service name `engenho-nats:4222`.
    #[must_use]
    pub fn in_cluster(cluster_name: impl Into<String>) -> Self {
        Self {
            servers: vec!["nats://engenho-nats:4222".to_string()],
            cluster: cluster_name.into(),
            ..Default::default()
        }
    }

    /// Builder helper: set the NATS servers list.
    #[must_use]
    pub fn with_servers(mut self, servers: Vec<String>) -> Self {
        self.servers = servers;
        self
    }

    /// Builder helper: set the cluster identifier.
    #[must_use]
    pub fn with_cluster(mut self, cluster: impl Into<String>) -> Self {
        self.cluster = cluster.into();
        self
    }

    /// Builder helper: set credentials.
    #[must_use]
    pub fn with_credentials(mut self, path: impl Into<String>) -> Self {
        self.credentials_path = Some(path.into());
        self
    }

    /// Builder helper: add a leaf-node remote.
    #[must_use]
    pub fn with_leaf_remote(mut self, remote: LeafNodeRemote) -> Self {
        self.leaf_remotes.push(remote);
        self
    }

    /// Validate the config. Used by [`crate::TeiaClient::connect`]
    /// before attempting a network connection.
    ///
    /// # Errors
    ///
    /// Returns [`TeiaError::InvalidConfig`] if servers list is
    /// empty or cluster name is malformed.
    pub fn validate(&self) -> Result<(), TeiaError> {
        if self.servers.is_empty() {
            return Err(TeiaError::InvalidConfig(
                "at least one NATS server URL required".into(),
            ));
        }
        if self.cluster.is_empty() {
            return Err(TeiaError::InvalidConfig(
                "cluster identifier required".into(),
            ));
        }
        if self.cluster.contains('.') || self.cluster.contains(' ') {
            return Err(TeiaError::InvalidConfig(format!(
                "cluster id must not contain dots/spaces: {:?}",
                self.cluster
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        TeiaConfig::default().validate().expect("default valid");
    }

    #[test]
    fn in_cluster_config_is_valid() {
        let cfg = TeiaConfig::in_cluster("engenho-prod");
        cfg.validate().unwrap();
        assert_eq!(cfg.cluster, "engenho-prod");
        assert_eq!(cfg.servers, vec!["nats://engenho-nats:4222"]);
    }

    #[test]
    fn empty_servers_rejected() {
        let cfg = TeiaConfig::default().with_servers(vec![]);
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.kind(), "invalid_config");
    }

    #[test]
    fn empty_cluster_rejected() {
        let cfg = TeiaConfig::default().with_cluster("");
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.kind(), "invalid_config");
    }

    #[test]
    fn cluster_with_dots_rejected() {
        let cfg = TeiaConfig::default().with_cluster("eng.eho");
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.kind(), "invalid_config");
    }

    #[test]
    fn from_engenho_config_carries_servers_and_cluster() {
        use engenho_config::TeiaConfig as TopTeia;
        let top = TopTeia {
            servers: vec!["nats://nats:4222".into()],
            cluster: "rio".into(),
            credentials_path: Some("/etc/creds".into()),
            connect_timeout_seconds: 15,
        };
        let runtime: TeiaConfig = (&top).into();
        assert_eq!(runtime.servers, vec!["nats://nats:4222"]);
        assert_eq!(runtime.cluster, "rio");
        assert_eq!(runtime.credentials_path.as_deref(), Some("/etc/creds"));
        assert_eq!(runtime.connect_timeout, std::time::Duration::from_secs(15));
        runtime.validate().unwrap();
    }

    #[test]
    fn from_owned_engenho_config_works_too() {
        use engenho_config::TeiaConfig as TopTeia;
        let top = TopTeia {
            servers: vec!["nats://a:4222".into(), "nats://b:4222".into()],
            cluster: "test".into(),
            credentials_path: None,
            connect_timeout_seconds: 10,
        };
        let runtime: TeiaConfig = top.into();
        assert_eq!(runtime.servers.len(), 2);
    }

    #[test]
    fn builder_chains() {
        let cfg = TeiaConfig::default()
            .with_cluster("test")
            .with_servers(vec!["nats://test:4222".into()])
            .with_credentials("/etc/nats/test.creds")
            .with_leaf_remote(LeafNodeRemote {
                url: "nats://hub:7422".into(),
                credentials_path: Some("/etc/nats/leaf.creds".into()),
            });
        assert_eq!(cfg.cluster, "test");
        assert_eq!(cfg.servers, vec!["nats://test:4222"]);
        assert_eq!(
            cfg.credentials_path.as_deref(),
            Some("/etc/nats/test.creds")
        );
        assert_eq!(cfg.leaf_remotes.len(), 1);
        cfg.validate().unwrap();
    }

    #[test]
    fn config_serde_round_trips() {
        let cfg = TeiaConfig::default()
            .with_cluster("test")
            .with_credentials("/etc/x");
        let json = serde_json::to_string(&cfg).unwrap();
        let back: TeiaConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.cluster, cfg.cluster);
        assert_eq!(back.credentials_path, cfg.credentials_path);
    }
}
