//! Revoada (distribution layer) config — topology + membership.

use serde::{Deserialize, Serialize};
use shikumi::TieredConfig;

use crate::error::ConfigError;

/// Top-level revoada config.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RevoadaConfig {
    /// Topology strategy (formation) + invariants.
    pub topology: TopologyConfig,
}

/// Per-formation strategy config.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyConfig {
    /// Which formation the cluster aims for.
    pub strategy: TopologyStrategyKind,
    /// Minimum eligible nodes before the strategy reacts.
    pub min_nodes: u32,
    /// Grace period before reacting to a phi-flagged node.
    /// Handles transient network blips without an unneeded
    /// promote/evict round-trip.
    pub grace_period_seconds: u32,
}

/// Closed enum of pre-packed topology strategies. Mirrors the
/// engenho-revoada::topology::TopologyStrategy concrete impls.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopologyStrategyKind {
    /// Single-node — dev / homelab.
    Solo,
    /// Two active-passive masters.
    Pair,
    /// Three masters for Raft quorum.
    Quorum3M,
    /// 3 masters + N workers (typical k8s).
    Cluster3MNW,
    /// All peers as masters (symmetric).
    MeshAllPeers,
    /// Scales with cluster size (⌈2N/5⌉ masters).
    Phalanx,
}

impl TieredConfig for RevoadaConfig {
    fn bare() -> Self {
        Self {
            topology: TopologyConfig {
                strategy: TopologyStrategyKind::Solo,
                min_nodes: 0,
                grace_period_seconds: 0,
            },
        }
    }

    fn prescribed_default() -> Self {
        Self {
            topology: TopologyConfig {
                strategy: TopologyStrategyKind::Phalanx,
                min_nodes: 1,
                grace_period_seconds: 10,
            },
        }
    }

    fn extend(self, _base: &Self) -> Self {
        self
    }
}

impl RevoadaConfig {
    /// Validate the revoada config.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidField`] if min_nodes is 0
    /// for a strategy that needs voters.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if matches!(
            self.topology.strategy,
            TopologyStrategyKind::Solo | TopologyStrategyKind::MeshAllPeers
        ) {
            // These permit min_nodes 0+ (Solo needs >= 1, but bare
            // is allowed to be 0 because operator hasn't decided yet).
            Ok(())
        } else if self.topology.min_nodes < 1 {
            Err(ConfigError::InvalidField {
                field: "revoada.topology.min_nodes".into(),
                reason: format!(
                    "strategy {:?} requires min_nodes >= 1",
                    self.topology.strategy
                ),
            })
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prescribed_default_uses_phalanx() {
        let cfg = RevoadaConfig::prescribed_default();
        assert_eq!(cfg.topology.strategy, TopologyStrategyKind::Phalanx);
        assert_eq!(cfg.topology.min_nodes, 1);
        assert_eq!(cfg.topology.grace_period_seconds, 10);
        cfg.validate().unwrap();
    }

    #[test]
    fn bare_uses_solo_with_zero_min_nodes() {
        let cfg = RevoadaConfig::bare();
        assert_eq!(cfg.topology.strategy, TopologyStrategyKind::Solo);
        assert_eq!(cfg.topology.min_nodes, 0);
        cfg.validate().unwrap(); // Solo permits 0
    }

    #[test]
    fn quorum_with_zero_min_nodes_fails_field_validation() {
        let cfg = RevoadaConfig {
            topology: TopologyConfig {
                strategy: TopologyStrategyKind::Quorum3M,
                min_nodes: 0,
                grace_period_seconds: 10,
            },
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn strategy_kind_serializes_snake_case() {
        // serde's snake_case turns "Cluster3MNW" into "cluster3_m_n_w" —
        // every uppercase letter (including after digits) becomes a
        // word boundary.
        let json = serde_json::to_string(&TopologyStrategyKind::Cluster3MNW).unwrap();
        assert_eq!(json, "\"cluster3_m_n_w\"");
    }
}
