//! `ControllerRuntime` — runs N [`Controller`] impls on a shared
//! tokio runtime with per-controller intervals.
//!
//! Operator code instantiates one [`ControllerRuntime`], registers
//! controllers, calls [`ControllerRuntime::run`] which spawns
//! per-controller tick loops. Each loop logs the [`ReconcileReport`]
//! after every tick.
//!
//! At R9 the runtime is the simplest possible thing: a Vec of
//! controllers + per-controller intervals. R9.5+ may add leader
//! election (only one runtime instance ticks; others stand by),
//! priority queues, and shared work queues — but the trait surface
//! doesn't change.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::controller::Controller;

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    /// Default reconcile interval if a controller doesn't specify one.
    pub default_interval: Duration,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            default_interval: Duration::from_secs(10),
        }
    }
}

/// Wire engenho-config's top-level ControllersConfig into the runtime.
/// The single fallback_interval_seconds becomes default_interval —
/// individual controllers can still override per-registration.
impl From<&engenho_config::ControllersConfig> for RuntimeConfig {
    fn from(top: &engenho_config::ControllersConfig) -> Self {
        Self {
            default_interval: Duration::from_secs(u64::from(top.fallback_interval_seconds)),
        }
    }
}

impl From<engenho_config::ControllersConfig> for RuntimeConfig {
    fn from(top: engenho_config::ControllersConfig) -> Self {
        (&top).into()
    }
}

pub struct ControllerRuntime {
    config: RuntimeConfig,
    controllers: Vec<(Arc<dyn Controller>, Duration)>,
}

impl ControllerRuntime {
    #[must_use]
    pub fn new(config: RuntimeConfig) -> Self {
        Self {
            config,
            controllers: Vec::new(),
        }
    }

    /// Register a controller with the default interval.
    pub fn register<C: Controller + 'static>(&mut self, controller: C) -> &mut Self {
        let interval = self.config.default_interval;
        self.controllers.push((Arc::new(controller), interval));
        self
    }

    /// Register a controller with an explicit interval.
    pub fn register_with_interval<C: Controller + 'static>(
        &mut self,
        controller: C,
        interval: Duration,
    ) -> &mut Self {
        self.controllers.push((Arc::new(controller), interval));
        self
    }

    /// Spawn the per-controller tick loops. Returns one JoinHandle
    /// per controller. The caller can abort each handle to stop a
    /// controller individually.
    #[must_use]
    pub fn spawn(self) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::new();
        for (controller, interval) in self.controllers {
            let handle = tokio::spawn(async move {
                loop {
                    // The interval-only fallback path now consumes the typed
                    // ReconcileOutcome the same way the WatchDriver does: on
                    // a requeue request, re-tick at `after` (capped at the
                    // configured interval — never wait LONGER than the
                    // fallback would); on a Declarative error, surface +
                    // wait the normal interval (no faster blind retry); on
                    // a Transient error, retry at the derived delay.
                    let next_delay = match controller.tick().await {
                        Ok(outcome) => {
                            outcome.log(controller.name());
                            match outcome.result.requeue_after() {
                                Some(after) => after.min(interval),
                                None => interval,
                            }
                        }
                        Err(e) => {
                            if e.classify() == shigoto_types::failure::FailureKind::Declarative {
                                tracing::error!(
                                    controller = controller.name(),
                                    error = %e,
                                    "reconcile failed (declarative — surfacing; waiting normal interval)"
                                );
                                interval
                            } else {
                                // Transient (+ future class): retry at the
                                // derived delay, capped at the interval.
                                let after = e
                                    .retry_after()
                                    .unwrap_or(Duration::from_secs(1))
                                    .min(interval);
                                tracing::warn!(
                                    controller = controller.name(),
                                    error = %e,
                                    "reconcile failed (transient — retrying)"
                                );
                                after
                            }
                        }
                    };
                    tokio::time::sleep(next_delay).await;
                }
            });
            handles.push(handle);
        }
        handles
    }

    /// Number of registered controllers (for testing + telemetry).
    #[must_use]
    pub fn len(&self) -> usize {
        self.controllers.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.controllers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::{Controller, ReconcileOutcome, ReconcileReport};
    use crate::error::ControllerError;
    use async_trait::async_trait;

    struct Counter {
        name: &'static str,
    }

    #[async_trait]
    impl Controller for Counter {
        fn name(&self) -> &'static str {
            self.name
        }
        async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
            Ok(ReconcileReport {
                objects_examined: 1,
                ..Default::default()
            }
            .into())
        }
    }

    #[test]
    fn runtime_starts_empty() {
        let rt = ControllerRuntime::new(RuntimeConfig::default());
        assert!(rt.is_empty());
        assert_eq!(rt.len(), 0);
    }

    #[test]
    fn runtime_config_from_engenho_config_carries_fallback_interval() {
        use engenho_config::{ControllerEnable, ControllersConfig};
        let top = ControllersConfig {
            enable: ControllerEnable {
                replicaset: true,
                deployment: true,
                statefulset: true,
                daemonset: true,
                job: true,
                cronjob: true,
                endpoints: true,
                service_routing: true,
                gc: true,
                crd: true,
                namespace: true,
            },
            namespace: String::new(),
            fallback_interval_seconds: 45,
            debounce_milliseconds: 100,
        };
        let runtime: RuntimeConfig = (&top).into();
        assert_eq!(runtime.default_interval, Duration::from_secs(45));
        // From-owned variant works too.
        let runtime_owned: RuntimeConfig = top.into();
        assert_eq!(runtime_owned.default_interval, Duration::from_secs(45));
    }

    #[test]
    fn runtime_registers_multiple_controllers() {
        let mut rt = ControllerRuntime::new(RuntimeConfig::default());
        rt.register(Counter { name: "a" });
        rt.register(Counter { name: "b" });
        rt.register_with_interval(Counter { name: "c" }, Duration::from_secs(60));
        assert_eq!(rt.len(), 3);
    }

    #[tokio::test]
    async fn runtime_spawn_runs_at_least_one_tick() {
        let mut rt = ControllerRuntime::new(RuntimeConfig {
            default_interval: Duration::from_millis(10),
        });
        rt.register(Counter { name: "t" });
        let handles = rt.spawn();
        // Let it tick a few times.
        tokio::time::sleep(Duration::from_millis(50)).await;
        for h in handles {
            h.abort();
        }
    }
}
