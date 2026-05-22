//! Per-resource ConsistencyTier defaults.

use serde::{Deserialize, Serialize};
use shikumi::TieredConfig;

use crate::error::ConfigError;

/// Consistency-tier defaults — mirrors
/// `engenho_types::ConsistencyTier` but in config form (no
/// runtime trait deps).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsistencyConfig {
    /// Default tier when a resource has no annotation. Strong is
    /// the safe choice — controllers can read linearizably.
    pub default_tier: ConsistencyTierKind,
}

/// Closed enum mirroring engenho_types::ConsistencyTier variants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsistencyTierKind {
    /// Linearizable Raft commits. Default.
    Strong,
    /// Chitchat gossip — 1-3s convergence.
    EventualGossip,
    /// NATS JetStream — replayable durable stream.
    DurableStream,
    /// iroh / NATS Object — content-addressed.
    Content,
}

impl TieredConfig for ConsistencyConfig {
    fn bare() -> Self {
        Self {
            default_tier: ConsistencyTierKind::Strong,
        }
    }

    fn prescribed_default() -> Self {
        Self {
            default_tier: ConsistencyTierKind::Strong,
        }
    }

    fn extend(self, _base: &Self) -> Self {
        self
    }
}

impl ConsistencyConfig {
    /// Validate. Currently no field-level rules — every variant is valid.
    ///
    /// # Errors
    ///
    /// Currently infallible; reserved for future cross-section
    /// invariants.
    pub fn validate(&self) -> Result<(), ConfigError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prescribed_default_uses_strong() {
        let cfg = ConsistencyConfig::prescribed_default();
        assert_eq!(cfg.default_tier, ConsistencyTierKind::Strong);
        cfg.validate().unwrap();
    }

    #[test]
    fn tier_kind_serializes_snake_case() {
        let json = serde_json::to_string(&ConsistencyTierKind::EventualGossip).unwrap();
        assert_eq!(json, "\"eventual_gossip\"");
    }
}
