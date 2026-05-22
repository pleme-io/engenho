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
    /// Returns [`ConfigError::InvalidField`] when tick interval
    /// is zero (would hot-loop the controller runtime).
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.tick_interval_seconds == 0 {
            return Err(ConfigError::InvalidField {
                field: "scheduler.tick_interval_seconds".into(),
                reason: "tick interval must be > 0 (zero would hot-loop)".into(),
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
}
