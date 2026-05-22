//! `WatchDriver` — event-driven controller wakeup.
//!
//! Wraps any [`Controller`] + an `Arc<StoreMesh>`. Subscribes to
//! `store.watch()` (the C2 broadcast stream); when an event of
//! interest arrives, calls `controller.tick()`. Includes a
//! periodic fallback tick so missed events (slow consumer
//! Lagged errors) don't strand the controller.
//!
//! ## Why
//!
//! Pre-C2 every controller polled at a fixed interval — typically
//! 30s in production. After C2, controllers can react in
//! microseconds to a commit. `WatchDriver` is the canonical glue.
//!
//! ## Filtering
//!
//! Each driver instance carries a [`KindFilter`] declaring which
//! resource kinds it cares about. The driver only wakes the
//! controller for matching events; everything else is dropped
//! before the controller sees it.
//!
//! ## Backpressure
//!
//! `broadcast::Receiver::recv` returns `Err(Lagged(n))` when the
//! driver falls behind by more than the channel capacity. The
//! driver treats Lagged as "tick once anyway" (the periodic
//! fallback would catch it eventually, but we don't want to wait).
//! The lag count is logged for observability.
//!
//! ## Coalescing
//!
//! Events frequently bunch: a single kubectl apply produces N
//! create events; without coalescing the controller ticks N
//! times. `WatchDriver` collects events in a short debounce
//! window (default 50ms) + ticks once per window when at least
//! one event matched.

use std::sync::Arc;
use std::time::Duration;

use engenho_store::{StoreMesh, WatchEvent};
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::controller::Controller;

/// Filter: which resource kinds wake this driver's controller?
#[derive(Clone, Debug)]
pub enum KindFilter {
    /// Wake on any committed mutation.
    All,
    /// Wake only on events whose `key.kind` is in the list. Match
    /// is case-sensitive — pass canonical K8s kind names
    /// ("Pod", "ReplicaSet", "Service", …).
    Kinds(Vec<String>),
}

impl KindFilter {
    /// Convenience: filter for a single kind.
    #[must_use]
    pub fn kind(name: impl Into<String>) -> Self {
        Self::Kinds(vec![name.into()])
    }

    fn matches(&self, ev: &WatchEvent) -> bool {
        match self {
            Self::All => true,
            Self::Kinds(list) => list.iter().any(|k| k == &ev.key.kind),
        }
    }
}

#[derive(Clone, Debug)]
pub struct WatchDriverConfig {
    /// Filter for which events wake the controller.
    pub filter: KindFilter,
    /// Debounce window: events arriving within this window
    /// coalesce into one tick. Default 50ms.
    pub debounce: Duration,
    /// Periodic fallback tick — runs even with zero events,
    /// covering missed events (Lagged) + cold start. Default
    /// 30s. Set to `Duration::MAX` to disable.
    pub fallback_interval: Duration,
}

impl Default for WatchDriverConfig {
    fn default() -> Self {
        Self {
            filter: KindFilter::All,
            debounce: Duration::from_millis(50),
            fallback_interval: Duration::from_secs(30),
        }
    }
}

/// The driver. Owns a `Controller` + an `Arc<StoreMesh>` +
/// configuration. `spawn()` starts the event loop.
pub struct WatchDriver<C: Controller> {
    controller: Arc<C>,
    store: Arc<StoreMesh>,
    config: WatchDriverConfig,
}

impl<C: Controller + 'static> WatchDriver<C> {
    #[must_use]
    pub fn new(controller: C, store: Arc<StoreMesh>, config: WatchDriverConfig) -> Self {
        Self {
            controller: Arc::new(controller),
            store,
            config,
        }
    }

    /// Spawn the event loop. Returns a [`JoinHandle`] the caller
    /// can abort to stop the driver.
    pub fn spawn(self) -> JoinHandle<()> {
        let controller = self.controller;
        let store = self.store;
        let config = self.config;
        tokio::spawn(async move {
            run(controller, store, config).await;
        })
    }

    /// One tick of the inner controller — useful in tests where
    /// the caller wants synchronous reconcile without the event
    /// loop.
    pub async fn tick_once(&self) -> Result<crate::controller::ReconcileReport, crate::error::ControllerError>
    {
        self.controller.tick().await
    }
}

async fn run<C: Controller + ?Sized>(
    controller: Arc<C>,
    store: Arc<StoreMesh>,
    config: WatchDriverConfig,
) {
    let mut rx = store.watch();
    info!(
        controller = controller.name(),
        debounce_ms = config.debounce.as_millis() as u64,
        fallback_s = if config.fallback_interval == Duration::MAX {
            0
        } else {
            config.fallback_interval.as_secs()
        },
        "watch driver started"
    );

    loop {
        let next_event = wait_for_relevant_event(&mut rx, &config).await;
        match next_event {
            EventOrTimer::Event => {
                // Active debounce: sleep the full window so bursting
                // events get absorbed into one tick. After the window,
                // drain queued events (the controller's tick reads
                // current state, so the events are just wake signals).
                tokio::time::sleep(config.debounce).await;
                while let Ok(ev) = rx.try_recv() {
                    if config.filter.matches(&ev) {
                        debug!(
                            controller = controller.name(),
                            key = %ev.key.label(),
                            "coalesced event"
                        );
                    }
                }
                tick_and_log(controller.as_ref()).await;
            }
            EventOrTimer::FallbackTimer => {
                tick_and_log(controller.as_ref()).await;
            }
            EventOrTimer::Shutdown => {
                info!(controller = controller.name(), "watch driver shutting down");
                return;
            }
        }
    }
}

#[derive(Debug)]
enum EventOrTimer {
    Event,
    FallbackTimer,
    Shutdown,
}

async fn wait_for_relevant_event(
    rx: &mut tokio::sync::broadcast::Receiver<WatchEvent>,
    config: &WatchDriverConfig,
) -> EventOrTimer {
    let timer = tokio::time::sleep(config.fallback_interval);
    tokio::pin!(timer);
    loop {
        tokio::select! {
            biased;
            res = rx.recv() => match res {
                Ok(ev) => {
                    if config.filter.matches(&ev) {
                        return EventOrTimer::Event;
                    }
                    // Else loop — wait for next event.
                }
                Err(RecvError::Lagged(n)) => {
                    warn!(lagged = n, "watch driver fell behind; ticking anyway");
                    return EventOrTimer::Event;
                }
                Err(RecvError::Closed) => return EventOrTimer::Shutdown,
            },
            _ = &mut timer => return EventOrTimer::FallbackTimer,
        }
    }
}

async fn tick_and_log<C: Controller + ?Sized>(controller: &C) {
    match controller.tick().await {
        Ok(report) => report.log(controller.name()),
        Err(e) => warn!(
            controller = controller.name(),
            error = %e,
            "reconcile failed"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn ev(kind: &str, name: &str) -> WatchEvent {
        use engenho_store::ResourceKey;
        WatchEvent {
            kind: engenho_store::WatchEventKind::Added,
            object: Value::Null,
            key: ResourceKey::namespaced("", "v1", kind, "default", name),
            resource_version: 1,
        }
    }

    #[test]
    fn kind_filter_all_matches_anything() {
        let f = KindFilter::All;
        assert!(f.matches(&ev("Pod", "x")));
        assert!(f.matches(&ev("Service", "x")));
    }

    #[test]
    fn kind_filter_specific_matches_only_listed() {
        let f = KindFilter::Kinds(vec!["Pod".into(), "Endpoints".into()]);
        assert!(f.matches(&ev("Pod", "x")));
        assert!(f.matches(&ev("Endpoints", "x")));
        assert!(!f.matches(&ev("Service", "x")));
    }

    #[test]
    fn kind_filter_kind_helper() {
        let f = KindFilter::kind("Pod");
        assert!(f.matches(&ev("Pod", "p")));
        assert!(!f.matches(&ev("Node", "n")));
    }

    #[test]
    fn config_default_is_sensible() {
        let cfg = WatchDriverConfig::default();
        assert_eq!(cfg.debounce, Duration::from_millis(50));
        assert_eq!(cfg.fallback_interval, Duration::from_secs(30));
        assert!(matches!(cfg.filter, KindFilter::All));
    }
}
