//! engenho-scheduler tunables.

use serde::{Deserialize, Serialize};
use shikumi::TieredConfig;

use crate::error::ConfigError;

/// Scheduler config — pluggable strategy + tick interval.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerConfig {
    /// Pluggable scheduling strategy.
    pub strategy: SchedulerStrategyKind,
    /// Namespace to scope the scheduler to. Empty = all namespaces.
    pub namespace: String,
    /// Polling fallback tick interval in seconds. (Primary path
    /// is WatchDriver; this fires when the watch stream goes
    /// silent.)
    pub tick_interval_seconds: u32,
}

/// Closed enum of scheduling strategies.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerStrategyKind {
    /// Round-robin across schedulable nodes.
    RoundRobin,
    /// Pack pods onto the fewest nodes (future).
    BinPack,
    /// Honor affinity/anti-affinity (future).
    Affinity,
}

impl TieredConfig for SchedulerConfig {
    fn bare() -> Self {
        Self {
            strategy: SchedulerStrategyKind::RoundRobin,
            namespace: String::new(),
            tick_interval_seconds: 0,
        }
    }

    fn prescribed_default() -> Self {
        Self {
            strategy: SchedulerStrategyKind::RoundRobin,
            namespace: String::new(),
            tick_interval_seconds: 5,
        }
    }

    fn extend(self, base: &Self) -> Self {
        Self {
            strategy: self.strategy,
            namespace: if self.namespace.is_empty() {
                base.namespace.clone()
            } else {
                self.namespace
            },
            tick_interval_seconds: if self.tick_interval_seconds == 0 {
                base.tick_interval_seconds
            } else {
                self.tick_interval_seconds
            },
        }
    }
}

impl SchedulerConfig {
    /// Validate the scheduler config.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidField`] when tick interval is zero
    /// (would hot-loop the controller runtime), or when `strategy` names
    /// a designed-but-unimplemented strategy (`BinPack`, `Affinity`).
    /// Rejecting the strategy HERE — before the cluster starts — is the
    /// fail-fast that replaces the old silent runtime downgrade to
    /// round-robin (a warn-then-wrong-answer). The operator gets a typed
    /// config error at discover/validate time, never a wrong scheduler at
    /// runtime.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.tick_interval_seconds == 0 {
            return Err(ConfigError::InvalidField {
                field: "scheduler.tick_interval_seconds".into(),
                reason: "tick interval must be > 0 (zero would hot-loop)".into(),
            });
        }
        match self.strategy {
            SchedulerStrategyKind::RoundRobin => {}
            SchedulerStrategyKind::BinPack | SchedulerStrategyKind::Affinity => {
                return Err(ConfigError::InvalidField {
                    field: "scheduler.strategy".into(),
                    reason: format!(
                        "scheduling strategy {:?} is designed but not yet implemented; \
                         only round_robin is available (no silent fallback)",
                        self.strategy
                    ),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prescribed_default_validates() {
        SchedulerConfig::prescribed_default().validate().unwrap();
    }

    #[test]
    fn bare_fails_validation_due_to_zero_tick() {
        assert!(SchedulerConfig::bare().validate().is_err());
    }

    #[test]
    fn strategy_kind_serializes_snake_case() {
        let json = serde_json::to_string(&SchedulerStrategyKind::BinPack).unwrap();
        assert_eq!(json, "\"bin_pack\"");
    }

    #[test]
    fn round_robin_strategy_validates() {
        let cfg = SchedulerConfig {
            strategy: SchedulerStrategyKind::RoundRobin,
            namespace: String::new(),
            tick_interval_seconds: 5,
        };
        cfg.validate().unwrap();
    }

    #[test]
    fn unimplemented_strategies_fail_validation() {
        // BinPack / Affinity are designed but not yet implemented — they
        // must fail config validation (fail before the cluster starts),
        // NOT silently downgrade to round-robin at runtime.
        for kind in [
            SchedulerStrategyKind::BinPack,
            SchedulerStrategyKind::Affinity,
        ] {
            let cfg = SchedulerConfig {
                strategy: kind,
                namespace: String::new(),
                tick_interval_seconds: 5,
            };
            match cfg.validate() {
                Err(ConfigError::InvalidField { field, .. }) => {
                    assert_eq!(field, "scheduler.strategy");
                }
                other => panic!("expected InvalidField on scheduler.strategy, got {other:?}"),
            }
        }
    }
}
