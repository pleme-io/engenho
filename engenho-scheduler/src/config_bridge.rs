//! Bridge from operator-facing [`engenho_config::SchedulerConfig`]
//! to a runtime [`Box<dyn SchedulingStrategy>`].
//!
//! Factory function rather than [`From`] because the target is a
//! trait object — `From` and `Box<dyn Trait>` don't compose cleanly.

use engenho_config::SchedulerStrategyKind;

use crate::strategy::{RoundRobinStrategy, SchedulingStrategy};

/// Construct the strategy trait object the operator's config asks
/// for. Future strategies (BinPack, Affinity) plug in here as new
/// match arms.
#[must_use]
pub fn make_scheduling_strategy(
    cfg: &engenho_config::SchedulerConfig,
) -> Box<dyn SchedulingStrategy> {
    match cfg.strategy {
        SchedulerStrategyKind::RoundRobin => Box::new(RoundRobinStrategy::new()),
        // BinPack + Affinity aren't implemented yet (designed); fall
        // back to RoundRobin so operators don't crash when they pick
        // a future strategy that isn't yet shipped.
        SchedulerStrategyKind::BinPack | SchedulerStrategyKind::Affinity => {
            tracing::warn!(
                strategy = ?cfg.strategy,
                "strategy not yet implemented; falling back to round_robin"
            );
            Box::new(RoundRobinStrategy::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_config::SchedulerConfig;

    #[test]
    fn round_robin_strategy_is_constructed() {
        let cfg = SchedulerConfig {
            strategy: SchedulerStrategyKind::RoundRobin,
            namespace: String::new(),
            tick_interval_seconds: 5,
        };
        let s = make_scheduling_strategy(&cfg);
        assert_eq!(s.name(), "round_robin");
    }

    #[test]
    fn unimplemented_strategies_fall_back_to_round_robin() {
        for kind in [
            SchedulerStrategyKind::BinPack,
            SchedulerStrategyKind::Affinity,
        ] {
            let cfg = SchedulerConfig {
                strategy: kind,
                namespace: String::new(),
                tick_interval_seconds: 5,
            };
            let s = make_scheduling_strategy(&cfg);
            // Until BinPack/Affinity are implemented, callers get
            // RoundRobin + a warning log.
            assert_eq!(s.name(), "round_robin");
        }
    }

    #[test]
    fn default_config_yields_round_robin() {
        // Construct an explicit "default-shaped" SchedulerConfig
        // — keeps scheduler crate independent of shikumi.
        let cfg = SchedulerConfig {
            strategy: SchedulerStrategyKind::RoundRobin,
            namespace: String::new(),
            tick_interval_seconds: 5,
        };
        let s = make_scheduling_strategy(&cfg);
        assert_eq!(s.name(), "round_robin");
    }
}
