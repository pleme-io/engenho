//! Bridge from operator-facing [`engenho_config::SchedulerConfig`] to a
//! running scheduler.
//!
//! [`Scheduler::from_config`] is the one reader of `SchedulerConfig`. It
//! destructures the struct with no `..`, so a field added to the config
//! does not compile (E0027) until this file says what it means. A field
//! can still be discarded as `field: _`, and only review catches that.
//!
//! [`make_scheduling_strategy`] reads `strategy` alone. It stays for the
//! caller that has not moved to [`Scheduler::from_config`] yet.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use engenho_config::{SchedulerConfig, SchedulerStrategyKind};
use engenho_controllers::{WatchDriver, WatchDriverConfig};
use engenho_store::StoreMesh;

use crate::error::SchedulerError;
use crate::scheduler::Scheduler;
use crate::scope::NamespaceScope;
use crate::strategy::{RoundRobinStrategy, SchedulingStrategy};

/// A [`Scheduler`] built from [`SchedulerConfig`], together with the
/// fallback cadence that config names.
///
/// The cadence belongs to the driver that hosts the scheduler, not to the
/// reconcile itself, so it is carried beside the scheduler rather than in
/// it. [`Self::into_watch_driver`] is how it reaches the driver.
pub struct ConfiguredScheduler {
    scheduler: Scheduler,
    /// Non-zero by construction: built only from a [`NonZeroU32`].
    fallback_interval: Duration,
}

impl ConfiguredScheduler {
    /// The configured scheduler.
    #[must_use]
    pub fn scheduler(&self) -> &Scheduler {
        &self.scheduler
    }

    /// How long the scheduler's driver waits with no Pod or Node event
    /// before ticking anyway (`scheduler.tick_interval_seconds`).
    #[must_use]
    pub fn fallback_interval(&self) -> Duration {
        self.fallback_interval
    }

    /// Host the scheduler in a [`WatchDriver`] over its own store.
    ///
    /// `base` carries the runtime-wide driver policy (wake filter,
    /// debounce, stuck-tick window). Its `fallback_interval` is replaced by
    /// the configured one; everything else is kept.
    #[must_use]
    pub fn into_watch_driver(self, base: WatchDriverConfig) -> WatchDriver<Scheduler> {
        let Self {
            scheduler,
            fallback_interval,
        } = self;
        let store = Arc::clone(scheduler.store());
        WatchDriver::new(
            scheduler,
            store,
            WatchDriverConfig {
                fallback_interval,
                ..base
            },
        )
    }
}

impl Scheduler {
    /// Build a scheduler over `store` from the operator's config.
    ///
    /// Every field of [`SchedulerConfig`] is read here:
    ///
    /// - `strategy` constructs the strategy. An unimplemented one is an
    ///   error, never a round-robin fallback.
    /// - `namespace` scopes which pods are placed. Empty is every
    ///   namespace ([`NamespaceScope::from_name`]).
    /// - `tick_interval_seconds` is the driver's fallback tick
    ///   ([`ConfiguredScheduler::fallback_interval`]).
    ///
    /// # Errors
    ///
    /// - [`SchedulerError::UnsupportedStrategy`] for `BinPack` or
    ///   `Affinity`.
    /// - [`SchedulerError::ZeroTickInterval`] when
    ///   `tick_interval_seconds` is 0.
    pub fn from_config(
        store: Arc<StoreMesh>,
        cfg: &SchedulerConfig,
    ) -> Result<ConfiguredScheduler, SchedulerError> {
        // ★ No `..`. A new SchedulerConfig field is E0027 here until it is
        // given a meaning below.
        let SchedulerConfig {
            strategy,
            namespace,
            tick_interval_seconds,
        } = cfg;
        let strategy = strategy_for(*strategy)?;
        let scope = NamespaceScope::from_name(namespace);
        let tick =
            NonZeroU32::new(*tick_interval_seconds).ok_or(SchedulerError::ZeroTickInterval)?;
        Ok(ConfiguredScheduler {
            scheduler: Scheduler::assemble(store, strategy, scope),
            fallback_interval: Duration::from_secs(u64::from(tick.get())),
        })
    }
}

/// Construct the strategy trait object the operator's config asks for.
///
/// Reads `cfg.strategy` and nothing else. [`Scheduler::from_config`] reads
/// the whole config and is the path new callers take.
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
    cfg: &SchedulerConfig,
) -> Result<Box<dyn SchedulingStrategy>, SchedulerError> {
    strategy_for(cfg.strategy)
}

fn strategy_for(
    kind: SchedulerStrategyKind,
) -> Result<Box<dyn SchedulingStrategy>, SchedulerError> {
    match kind {
        SchedulerStrategyKind::RoundRobin => Ok(Box::new(RoundRobinStrategy::new())),
        requested @ (SchedulerStrategyKind::BinPack | SchedulerStrategyKind::Affinity) => {
            Err(SchedulerError::UnsupportedStrategy { requested })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
