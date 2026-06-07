//! Bridge from operator-facing [`engenho_config::SchedulerConfig`]
//! to a runtime [`Box<dyn SchedulingStrategy>`].
//!
//! Factory function rather than [`From`] because the target is a
//! trait object — `From` and `Box<dyn Trait>` don't compose cleanly.

use engenho_config::SchedulerStrategyKind;

use crate::error::SchedulerError;
use crate::strategy::{RoundRobinStrategy, SchedulingStrategy};

/// Construct the strategy trait object the operator's config asks for.
///
/// Future strategies (`BinPack`, `Affinity`) plug in here as new match
/// arms. Until a strategy is actually implemented, requesting it returns
/// a typed [`SchedulerError::UnsupportedStrategy`] — NOT a silent
/// downgrade to round-robin. The operator asked for `BinPack`; giving them
/// round-robin + a warning log is a warn-then-wrong-answer that the
/// CONTINUOUS-CONVERGENCE + TYPED-SPEC rules forbid. A misconfigured
/// strategy now fails fast at config-validate / boot time with a named
/// error instead of running the wrong scheduler.
///
/// # Errors
///
/// Returns [`SchedulerError::UnsupportedStrategy`] when `cfg.strategy` is
/// a designed-but-unimplemented strategy (`BinPack`, `Affinity`).
pub fn make_scheduling_strategy(
    cfg: &engenho_config::SchedulerConfig,
) -> Result<Box<dyn SchedulingStrategy>, SchedulerError> {
    match cfg.strategy {
        SchedulerStrategyKind::RoundRobin => Ok(Box::new(RoundRobinStrategy::new())),
        requested @ (SchedulerStrategyKind::BinPack | SchedulerStrategyKind::Affinity) => {
            Err(SchedulerError::UnsupportedStrategy { requested })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_config::SchedulerConfig;

    fn cfg_with(strategy: SchedulerStrategyKind) -> SchedulerConfig {
        SchedulerConfig {
            strategy,
            namespace: String::new(),
            tick_interval_seconds: 5,
        }
    }

    #[test]
    fn round_robin_constructs_ok() {
        let s = make_scheduling_strategy(&cfg_with(SchedulerStrategyKind::RoundRobin))
            .expect("round_robin always constructs");
        assert_eq!(s.name(), "round_robin");
    }

    #[test]
    fn unsupported_strategy_is_typed_error() {
        for kind in [
            SchedulerStrategyKind::BinPack,
            SchedulerStrategyKind::Affinity,
        ] {
            // `Box<dyn SchedulingStrategy>` isn't `Debug`, so match the
            // Result rather than `expect_err`.
            match make_scheduling_strategy(&cfg_with(kind)) {
                Err(SchedulerError::UnsupportedStrategy { requested }) => {
                    assert_eq!(requested, kind);
                }
                Err(other) => panic!("expected UnsupportedStrategy, got {other:?}"),
                Ok(_) => panic!(
                    "unimplemented strategy {kind:?} must be a typed error, NOT a round-robin fallback"
                ),
            }
        }
    }
}
