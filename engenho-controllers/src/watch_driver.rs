//! `WatchDriver` — event-driven controller wakeup.
//!
//! Wraps any [`Controller`] + an `Arc<StoreMesh>`. Subscribes to
//! `store.watch()` (the live-tail [`WatchStream`]); when a signal of
//! interest arrives, calls `controller.tick()`. Includes a periodic
//! fallback tick so missed signals don't strand the controller.
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
//! before the controller sees it. Bookmarks are progress markers, not
//! state changes — they never wake the controller.
//!
//! ## Backpressure (M0.1 item 3 — resumable watch backend)
//!
//! The per-watcher stream surfaces a typed
//! [`engenho_store::WatchGone`] terminal instead of a silent
//! `broadcast::RecvError::Lagged`. On `WatchGone::Overflow` the driver
//! ticks once (the periodic fallback would catch it eventually, but we
//! don't want to wait) AND RE-ESTABLISHES the stream from the live
//! tail — the controller's `tick()` reads current state, so a fresh
//! live-tail subscription is a correct (if coarse) recovery: the next
//! commit wakes it again, and the immediate tick covers the gap. On
//! `WatchGone::CompactedTooOld` (only reachable via `watch_from`, not
//! the live-tail shim) the driver likewise re-subscribes + ticks.
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

use engenho_store::{StoreMesh, WatchEvent, WatchGone, WatchSignal, WatchStream};
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
    /// 30s. Set to `Duration::MAX` to disable. This is upstream's
    /// informer `resyncPeriod` / kubelet housekeeping ticker: with no
    /// events at all the controller still runs.
    pub fallback_interval: Duration,
    /// How long one `tick()` may run before the driver starts reporting
    /// it as BLOCKED, once per window, until it returns.
    ///
    /// The tick is **never cancelled**. Upstream does not cancel a slow
    /// `syncLoop` either — it fails `/healthz`'s `syncLoop` check (no
    /// iteration within `2 * SyncFrequency`) so the process is visibly
    /// unhealthy and gets restarted. Cancelling mid-tick here would drop
    /// a `start()` that already spawned a process, leaving a running
    /// container with no local record — a worse state than a slow one.
    pub stuck_tick_after: Duration,
}

impl Default for WatchDriverConfig {
    fn default() -> Self {
        Self {
            filter: KindFilter::All,
            debounce: Duration::from_millis(50),
            fallback_interval: Duration::from_secs(30),
            stuck_tick_after: Duration::from_secs(120),
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
    /// loop. Returns the typed [`ReconcileOutcome`] (report + requeue
    /// decision).
    pub async fn tick_once(
        &self,
    ) -> Result<crate::controller::ReconcileOutcome, crate::error::ControllerError> {
        self.controller.tick().await
    }
}

/// Bounds on how fast the driver re-subscribes after the stream ends.
///
/// Upstream's reflector wraps `ListAndWatch` in `wait.BackoffUntil`, so a
/// store that keeps ending the subscription is retried forever but never
/// hot-looped. Same shape, same reason.
const RESUBSCRIBE_BACKOFF_MIN: Duration = Duration::from_millis(100);
const RESUBSCRIBE_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Subscribe, sleeping `delay` first (zero on the initial attempt).
///
/// Returns `None` when the store refuses a subscription; the caller keeps
/// looping on the fallback timer and tries again, exactly as
/// `BackoffUntil` re-enters `ListAndWatch`.
async fn subscribe(
    store: &Arc<StoreMesh>,
    controller: &'static str,
    delay: Duration,
) -> Option<WatchStream> {
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
    match store.watch().await {
        Ok(s) => Some(s),
        Err(e) => {
            warn!(
                controller,
                error = %e,
                "watch subscribe failed; fallback-tick only until the next attempt"
            );
            None
        }
    }
}

/// Grow the re-subscribe delay geometrically, capped.
fn grow(delay: Duration) -> Duration {
    if delay.is_zero() {
        RESUBSCRIBE_BACKOFF_MIN
    } else {
        (delay * 2).min(RESUBSCRIBE_BACKOFF_MAX)
    }
}

async fn run<C: Controller + ?Sized + 'static>(
    controller: Arc<C>,
    store: Arc<StoreMesh>,
    config: WatchDriverConfig,
) {
    let name = controller.name();
    let mut rx = subscribe(&store, name, Duration::ZERO).await;
    // Reset to zero whenever a subscription actually delivers an event —
    // upstream resets its backoff on a watch that made progress.
    let mut backoff = Duration::ZERO;

    info!(
        controller = name,
        debounce_ms = config.debounce.as_millis() as u64,
        fallback_s = if config.fallback_interval == Duration::MAX {
            0
        } else {
            config.fallback_interval.as_secs()
        },
        "watch driver started"
    );

    // ── THE LOOP NEVER RETURNS ─────────────────────────────────────────
    //
    // There is deliberately no in-band stop: shutdown is `handle.abort()`
    // on the `JoinHandle` `spawn()` returned, which is upstream's `stopCh`
    // — a channel SEPARATE from the data. Conflating the two is what this
    // loop used to do and what retired a controller for 12 hours:
    //
    //   1. a coalescing drain consumed the stream's one-shot terminal,
    //   2. the drained stream then yielded `None` forever,
    //   3. `None` was read as `Shutdown`, and `run` RETURNED.
    //
    // Nothing logged an error, nothing supervised the task, and the three
    // other controllers kept ticking, so the daemon looked healthy while
    // no pod on the node could be started, restarted or reaped. The
    // `Shutdown` variant is gone: a data stream ending is a RE-SUBSCRIBE,
    // the way a closed `ResultChan` sends upstream's reflector back
    // through `ListAndWatch`.
    loop {
        match wait_for_relevant_event(rx.as_mut(), &config).await {
            EventOrTimer::Event => {
                backoff = Duration::ZERO;
                // Coalesce the burst, then tick once.
                tokio::time::sleep(config.debounce).await;
                // A terminal can surface mid-drain. It is returned, never
                // swallowed: the stream emits it EXACTLY ONCE, so dropping
                // it here is how the subscription became silently dead.
                let gone = rx
                    .as_mut()
                    .and_then(|stream| drain_pending(stream, name, &config));
                tick_and_log(&controller, config.stuck_tick_after).await;
                if let Some(gone) = gone {
                    warn!(
                        controller = name,
                        gone = %gone,
                        "watch stream gone while coalescing; re-subscribing"
                    );
                    backoff = grow(backoff);
                    rx = subscribe(&store, name, backoff).await;
                }
            }
            EventOrTimer::Gone(gone) => {
                warn!(
                    controller = name,
                    gone = %gone,
                    "watch stream gone; ticking + re-subscribing"
                );
                tick_and_log(&controller, config.stuck_tick_after).await;
                backoff = grow(backoff);
                rx = subscribe(&store, name, backoff).await;
            }
            EventOrTimer::StreamEnded => {
                // NOT a shutdown. The subscription is over — the terminal
                // was already delivered, or the store dropped the sender —
                // and the controller still owns its resources.
                warn!(
                    controller = name,
                    "watch stream ended; ticking + re-subscribing (not a shutdown)"
                );
                tick_and_log(&controller, config.stuck_tick_after).await;
                backoff = grow(backoff);
                rx = subscribe(&store, name, backoff).await;
            }
            EventOrTimer::FallbackTimer => {
                tick_and_log(&controller, config.stuck_tick_after).await;
                if rx.is_none() {
                    backoff = grow(backoff);
                    rx = subscribe(&store, name, backoff).await;
                }
            }
        }
    }
}

/// Arm a one-shot re-tick of `controller` after `delay`. A coarse
/// whole-kind re-tick (the controller's `tick` is already a full sweep),
/// fired off the hot loop so the requeue doesn't block event processing.
/// This is the brick that makes `ReconcileResult::Requeue` LOAD-BEARING;
/// the per-key DelayQueue RequeueDriver is the next brick + simply swaps
/// this one-shot sleep for a keyed coalescing queue.
fn arm_requeue<C: Controller + ?Sized + 'static>(
    controller: Arc<C>,
    delay: Duration,
    stuck_after: Duration,
) {
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        debug!(controller = controller.name(), "requeue timer fired");
        tick_and_log(&controller, stuck_after).await;
    });
}

#[derive(Debug)]
enum EventOrTimer {
    Event,
    /// The stream terminated with a typed [`WatchGone`] — tick + resub.
    Gone(WatchGone),
    FallbackTimer,
    /// The subscription is over with no terminal left to read: the
    /// one-shot [`WatchGone`] was already consumed, or the store dropped
    /// the sender. It means RE-SUBSCRIBE, and it is spelled that way so
    /// nobody can read it as a stop again — there is no `Shutdown`
    /// variant to fall into. Stopping a driver is `handle.abort()`, out
    /// of band, the way upstream keeps `stopCh` separate from
    /// `ResultChan`.
    StreamEnded,
}

async fn wait_for_relevant_event(
    rx: Option<&mut WatchStream>,
    config: &WatchDriverConfig,
) -> EventOrTimer {
    let timer = tokio::time::sleep(config.fallback_interval);
    tokio::pin!(timer);

    // No live stream → only the fallback timer can fire.
    let Some(rx) = rx else {
        timer.await;
        return EventOrTimer::FallbackTimer;
    };

    loop {
        tokio::select! {
            biased;
            res = rx.next() => match res {
                Some(Ok(WatchSignal::Event(ev))) => {
                    if config.filter.matches(&ev) {
                        return EventOrTimer::Event;
                    }
                    // Else loop — wait for the next signal.
                }
                // Bookmarks are progress markers, never wake the
                // controller (no state change to reconcile).
                Some(Ok(WatchSignal::Bookmark(_))) => {}
                Some(Err(gone)) => return EventOrTimer::Gone(gone),
                None => return EventOrTimer::StreamEnded,
            },
            _ = &mut timer => return EventOrTimer::FallbackTimer,
        }
    }
}

/// Drain whatever signals are already buffered (non-blocking), logging
/// coalesced matching events.
///
/// Returns the terminal [`WatchGone`] if one surfaced, because the stream
/// emits it **exactly once** and `try_next` CONSUMES it — after which the
/// stream yields `None` forever. The previous version matched `Err(_) =>
/// break` under a comment claiming it left the terminal "for the next
/// `wait_for_relevant_event`". It did not: the value was already gone,
/// the next wait saw `None`, and the driver retired the controller.
/// Handing it back is what lets the coalescing path recover.
#[must_use = "the terminal is one-shot: drop it and the subscription is silently dead"]
fn drain_pending(
    stream: &mut WatchStream,
    controller_name: &'static str,
    config: &WatchDriverConfig,
) -> Option<WatchGone> {
    while let Some(item) = stream.try_next() {
        match item {
            Ok(WatchSignal::Event(ev)) => {
                if config.filter.matches(&ev) {
                    debug!(controller = controller_name, key = %ev.key.label(), "coalesced event");
                }
            }
            Ok(WatchSignal::Bookmark(_)) => {}
            Err(gone) => return Some(gone),
        }
    }
    None
}

/// Run one `tick()`, reporting it at ERROR once per `stuck_after` window
/// while it has not returned — and **never cancelling it**.
///
/// `tokio::time::timeout` takes `&mut fut`, so an elapsed window does not
/// drop the tick; it only lets us say so. This is the kubelet
/// `syncLoopHealthCheck` shape: upstream does not abort a slow sync, it
/// makes the stall VISIBLE. A tick that hangs forever now produces a
/// repeating ERROR naming the controller and the elapsed seconds, instead
/// of the total silence that hid this defect for 12 hours.
async fn tick_observed<C: Controller + ?Sized + 'static>(
    controller: &Arc<C>,
    stuck_after: Duration,
) -> Result<crate::controller::ReconcileOutcome, crate::error::ControllerError> {
    let started = std::time::Instant::now();
    let fut = controller.tick();
    tokio::pin!(fut);
    loop {
        match tokio::time::timeout(stuck_after, &mut fut).await {
            Ok(outcome) => return outcome,
            Err(_) => tracing::error!(
                controller = controller.name(),
                elapsed_s = started.elapsed().as_secs(),
                "reconcile tick has NOT returned; this controller is blocked (not cancelled \u{2014} cancelling mid-tick would strand side effects)"
            ),
        }
    }
}

/// Tick the controller, log its report, AND act on the typed outcome —
/// the propagation that replaces the pre-unification swallow.
///
///   * `Ok(outcome)` with `result == Done` → log only (today's behavior).
///   * `Ok(outcome)` with `result == Requeue{after}` /
///     `RequeueWithProgress{after}` → log + arm a one-shot re-tick at
///     `after` (instead of waiting up to `fallback_interval` for a blind
///     re-tick).
///   * `Err(e)` → classify via the fleet `FailureKind` classifier
///     ([`ControllerError::classify`]):
///       - `Declarative` → log at ERROR + DO NOT schedule a retry (the
///         operator's declaration is broken; blind-retrying is futile —
///         the fallback timer still re-ticks coarsely, but we don't
///         pile on a targeted retry).
///       - `Transient` → log at WARN + arm a retry at the
///         `retry_after`-derived delay (1s) instead of the flat 30s
///         fallback wait.
async fn tick_and_log<C: Controller + ?Sized + 'static>(
    controller: &Arc<C>,
    stuck_after: Duration,
) {
    match tick_observed(controller, stuck_after).await {
        Ok(outcome) => {
            outcome.log(controller.name());
            if let Some(after) = outcome.result.requeue_after() {
                debug!(
                    controller = controller.name(),
                    after_ms = after.as_millis() as u64,
                    "controller requested requeue; arming one-shot re-tick"
                );
                arm_requeue(controller.clone(), after, stuck_after);
            }
        }
        Err(e) => {
            if e.classify() == shigoto_types::failure::FailureKind::Declarative {
                // Surfaced, NOT blind-retried — the prior loop retried a
                // declarative error forever at the 30s fallback. (The
                // periodic fallback still re-ticks coarsely; we just don't
                // pile a targeted retry on a broken declaration.)
                tracing::error!(
                    controller = controller.name(),
                    error = %e,
                    "reconcile failed (declarative — surfacing, not scheduling a targeted retry)"
                );
            } else {
                // Transient (+ any future #[non_exhaustive] class,
                // conservatively): schedule a targeted retry at the derived
                // delay instead of waiting for the flat fallback.
                let after = e.retry_after().unwrap_or(Duration::from_secs(1));
                warn!(
                    controller = controller.name(),
                    error = %e,
                    after_ms = after.as_millis() as u64,
                    "reconcile failed (transient — scheduling targeted retry)"
                );
                arm_requeue(controller.clone(), after, stuck_after);
            }
        }
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

    // ── The 2026-09-18 ryn defect, sealed ──────────────────────────────
    //
    // engenho's kubelet controller stopped reconciling for 12 hours and
    // nothing said so. The chain was:
    //
    //   1. a burst overflowed the kubelet's watch buffer, arming the
    //      stream's ONE-SHOT terminal;
    //   2. the coalescing drain read it with `try_next` and matched
    //      `Err(_) => break`, under a comment claiming it left the
    //      terminal "for the next wait_for_relevant_event";
    //   3. the terminal was in fact CONSUMED, so the stream yielded
    //      `None` forever;
    //   4. `None` was mapped to `EventOrTimer::Shutdown` and `run`
    //      RETURNED — retiring the controller permanently.
    //
    // Nothing logged an error. The driver task was never joined. Three
    // other controllers kept ticking, so the daemon read as healthy while
    // no pod on the node could start, restart or be reaped. The only
    // evidence was the ABSENCE of one log line.
    //
    // These two tests pin the load-bearing half: the terminal comes back
    // to the caller. The other half — a stream ending means re-subscribe,
    // never stop — is enforced by there being no `Shutdown` variant left
    // to return.

    use engenho_store::watch_backend::WatcherRegistry;
    use engenho_store::{Change, ChangeKind, ResourceKey, Revision, VersionMeta, WatchOpts};

    fn change(rev: u64) -> Change {
        let mut r = Revision::ZERO;
        for _ in 0..rev {
            r = r.next();
        }
        Change {
            revision: r,
            key: ResourceKey::namespaced("", "v1", "Pod", "default", "p1"),
            kind: ChangeKind::Put,
            value: serde_json::json!({"spec": {}}),
            prior: None,
            version_meta: VersionMeta::created_at(r),
        }
    }

    /// A tiny buffer plus a burst is exactly the production trigger.
    fn overflowed_stream() -> WatchStream {
        let mut reg = WatcherRegistry::new();
        let stream = reg.register_captured(
            Vec::new(),
            Revision::ZERO,
            &WatchOpts {
                from: Revision::ZERO,
                buffer: 2,
                bookmark_every: Duration::ZERO,
            },
        );
        for rev in 1..=8 {
            reg.fan_change(&change(rev));
        }
        stream
    }

    #[test]
    fn a_terminal_reached_while_coalescing_is_returned_to_the_caller() {
        let mut stream = overflowed_stream();
        let config = WatchDriverConfig::default();

        let gone = drain_pending(&mut stream, "t", &config);

        assert!(
            gone.is_some(),
            "the drain consumed the one-shot terminal and reported nothing; \
             that is the exact swallow that retired the kubelet"
        );
        assert!(matches!(gone, Some(WatchGone::Overflow { .. })));
    }

    /// Why returning it is not merely tidier: once drained, the stream is
    /// finished. If the caller does not act on what `drain_pending` hands
    /// back, there is nothing left to notice the subscription is dead.
    #[tokio::test]
    async fn the_drained_stream_is_finished_so_the_terminal_is_the_only_notice() {
        let mut stream = overflowed_stream();
        let config = WatchDriverConfig::default();

        let _ = drain_pending(&mut stream, "t", &config);

        assert!(
            stream.next().await.is_none(),
            "expected the stream to be over after its terminal was read"
        );
    }

    #[test]
    fn resubscribe_backoff_grows_from_zero_and_caps() {
        let mut d = Duration::ZERO;
        d = grow(d);
        assert_eq!(d, RESUBSCRIBE_BACKOFF_MIN, "first retry is the floor");
        for _ in 0..20 {
            d = grow(d);
        }
        assert_eq!(
            d, RESUBSCRIBE_BACKOFF_MAX,
            "a failing store must not hot-loop"
        );
    }
}
