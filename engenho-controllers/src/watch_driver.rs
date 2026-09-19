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
//!
//! ## Requeue: one slot per driver
//!
//! A controller asks to be ticked again by returning
//! `ReconcileResult::Requeue(after)`; a Transient error asks the same
//! thing implicitly. Both become ONE deadline in a `RequeueSlot` owned
//! by the loop — never a task. [`next_wake`] is the single decision
//! (outcome → delay); the slot is cleared when any tick starts, because a
//! tick is a full sweep and satisfies whatever was pending, then re-armed
//! from that tick's outcome. `wait_for_relevant_event` sleeps until the
//! earlier of the fallback and the slot's deadline.
//!
//! ## Retries grow
//!
//! A Transient failure's targeted retry is read off [`TRANSIENT_RETRY`]
//! (1 s doubling to 60 s) at the driver's count of consecutive failed
//! ticks, which any `Ok` resets — upstream's per-item rate limiter,
//! per driver. A re-subscribe after the stream ends is read off
//! [`RESUBSCRIBE`] (100 ms doubling to 30 s), reset by a delivered event.
//! Both are one [`Curve`]; neither is a flat retry, because a flat retry
//! against a store that is down is a hot loop for as long as it is down.
//!
//! So a driver has at most one pending re-tick and at most one tick in
//! flight, whatever the controller returns. The previous shape spawned a
//! detached sleep-then-tick task per requeue: every such tick armed
//! another, so each event or fallback tick left behind a chain that
//! re-armed itself forever, and the chains ran `tick` concurrently. The
//! tick rate grew with uptime, and a sleeping chain kept its
//! `Arc<StoreMesh>` alive past `handle.abort()`.
//!
//! ## The loop cannot return (T2.6)
//!
//! [`WatchDriver::run`] is `-> Infallible`. A driver loop that returns is a
//! type error (E0308), not a silent retirement: the only ways a driver task
//! ends are a panic and an abort, and the runtime's supervisor observes both.
//! There is no `spawn()` here any more — the caller spawns the future into
//! whatever owns it (the runtime's child set in production), so no driver is
//! detached with nobody holding its join handle.
//!
//! Every tick is recorded in the driver's [`Heartbeat`]: when it began and
//! ended and how it ended, readable without a lock by whoever supervises it.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use engenho_store::{StoreMesh, WatchEvent, WatchGone, WatchSignal, WatchStream};
use tokio::time::Instant;
use tracing::{debug, info, warn};

use shigoto_types::failure::FailureKind;

use crate::controller::{Controller, ReconcileOutcome};
use crate::curve::{Curve, Streak};
use crate::error::ControllerError;
use crate::heartbeat::{Heartbeat, TickClass};

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

    /// Whether an event on an object of `kind` wakes the controller.
    #[must_use]
    pub fn wakes_on(&self, kind: &str) -> bool {
        match self {
            Self::All => true,
            Self::Kinds(list) => list.iter().any(|k| k == kind),
        }
    }

    fn matches(&self, ev: &WatchEvent) -> bool {
        self.wakes_on(&ev.key.kind)
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
/// configuration + the [`Heartbeat`] its ticks are recorded in.
/// [`WatchDriver::run`] is the event loop.
pub struct WatchDriver<C: Controller> {
    controller: Arc<C>,
    store: Arc<StoreMesh>,
    config: WatchDriverConfig,
    beat: Arc<Heartbeat>,
}

impl<C: Controller + 'static> WatchDriver<C> {
    #[must_use]
    pub fn new(controller: C, store: Arc<StoreMesh>, config: WatchDriverConfig) -> Self {
        Self {
            controller: Arc::new(controller),
            store,
            config,
            beat: Arc::new(Heartbeat::new()),
        }
    }

    /// The heartbeat this driver's ticks are recorded in. Take it before
    /// [`run`](Self::run) consumes the driver.
    #[must_use]
    pub fn heartbeat(&self) -> Arc<Heartbeat> {
        self.beat.clone()
    }

    /// Which events wake this driver's controller. Read it before
    /// [`run`](Self::run) consumes the driver.
    #[must_use]
    pub fn wakes(&self) -> &KindFilter {
        &self.config.filter
    }

    /// The event loop. It never returns: the `Infallible` output makes a
    /// loop that ends a type error. The task running it ends only by panic
    /// or abort — the caller owns the task and sees both.
    pub async fn run(self) -> Infallible {
        run(self.controller, self.store, self.config, self.beat).await
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

/// How fast the driver re-subscribes after the stream ends: 100 ms
/// doubling to 30 s, reset when a subscription delivers an event.
///
/// Upstream's reflector wraps `ListAndWatch` in `wait.BackoffUntil`, so a
/// store that keeps ending the subscription is retried forever but never
/// hot-looped. Same shape, same reason.
pub const RESUBSCRIBE: Curve = Curve::from_millis::<100, 30_000>();

/// The targeted retry after a Transient failure: 1 s doubling to 60 s,
/// indexed by the driver's [`ConsecutiveFailures`] and reset by any `Ok`.
///
/// Upstream's controller workqueue does the same per item (5 ms doubling
/// to 1000 s); this is per driver, because a driver's tick is a full sweep
/// and its failure is the sweep's. The cap sits above the default 30 s
/// fallback on purpose: a driver that has failed for a minute is re-ticked
/// by its events and its fallback, not by a retry of its own.
pub const TRANSIENT_RETRY: Curve = Curve::from_millis::<1_000, 60_000>();

/// Where the loop gets its live-tail subscription.
///
/// Production is [`StoreMesh`]. The seam exists so the loop's scheduling —
/// the requeue slot, the fallback, the re-subscribe path — can be driven
/// under paused time by a feed the test controls, with no Raft group
/// behind it. The loop owns an `Arc` of the source for its lifetime, so
/// aborting the driver still drops the store clone it held.
#[async_trait::async_trait]
trait WatchSource: Send + Sync {
    async fn watch(&self) -> Result<WatchStream, WatchGone>;
}

#[async_trait::async_trait]
impl WatchSource for StoreMesh {
    async fn watch(&self) -> Result<WatchStream, WatchGone> {
        StoreMesh::watch(self).await
    }
}

/// Subscribe, sleeping `delay` first (zero on the initial attempt).
///
/// Returns `None` when the store refuses a subscription; the caller keeps
/// looping on its timers and tries again, exactly as `BackoffUntil`
/// re-enters `ListAndWatch`.
async fn subscribe<S: WatchSource + ?Sized>(
    source: &S,
    controller: &'static str,
    delay: Duration,
) -> Option<WatchStream> {
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
    match source.watch().await {
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

/// Re-subscribe after the stream ended or was refused, waiting the delay
/// the next miss on `attempts` owes first.
async fn resubscribe<S: WatchSource + ?Sized>(
    source: &S,
    controller: &'static str,
    attempts: &mut Streak,
) -> Option<WatchStream> {
    subscribe(source, controller, attempts.miss()).await
}

async fn run<C: Controller + ?Sized + 'static, S: WatchSource + ?Sized>(
    controller: Arc<C>,
    store: Arc<S>,
    config: WatchDriverConfig,
    beat: Arc<Heartbeat>,
) -> Infallible {
    let name = controller.name();
    let mut rx = subscribe(store.as_ref(), name, Duration::ZERO).await;
    // Re-subscribes since the last delivered event: reset whenever a
    // subscription actually delivers one — upstream resets its backoff on
    // a watch that made progress.
    let mut resubscribes = Streak::new(RESUBSCRIBE);
    // The ONE pending re-tick this driver may have, and its failed ticks
    // in a row. Every tick below goes through `ticker.tick()`, which
    // clears the slot and re-arms it; nothing else can schedule a tick, so
    // there is no second one to pile up.
    let mut ticker = Ticker {
        controller,
        stuck_after: config.stuck_tick_after,
        slot: RequeueSlot::default(),
        failures: ConsecutiveFailures::default(),
        beat,
    };

    info!(
        controller = name,
        debounce_ms = millis(config.debounce),
        fallback_s = if config.fallback_interval == Duration::MAX {
            0
        } else {
            config.fallback_interval.as_secs()
        },
        "watch driver started"
    );

    // ── THE LOOP NEVER RETURNS ─────────────────────────────────────────
    //
    // And it cannot: `run` is `-> Infallible`, so a `break` or `return`
    // here is a type error. There is deliberately no in-band stop: stopping
    // is aborting the task that runs this future, which is upstream's
    // `stopCh` — a channel SEPARATE from the data. Conflating the two is what this
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
        match wait_for_relevant_event(rx.as_mut(), &config, ticker.slot).await {
            EventOrTimer::Event => {
                resubscribes.reset();
                // Coalesce the burst, then tick once.
                tokio::time::sleep(config.debounce).await;
                // A terminal can surface mid-drain. It is returned, never
                // swallowed: the stream emits it EXACTLY ONCE, so dropping
                // it here is how the subscription became silently dead.
                let gone = rx
                    .as_mut()
                    .and_then(|stream| drain_pending(stream, name, &config));
                ticker.tick().await;
                if let Some(gone) = gone {
                    warn!(
                        controller = name,
                        gone = %gone,
                        "watch stream gone while coalescing; re-subscribing"
                    );
                    rx = resubscribe(store.as_ref(), name, &mut resubscribes).await;
                }
            }
            EventOrTimer::Gone(gone) => {
                warn!(
                    controller = name,
                    gone = %gone,
                    "watch stream gone; ticking + re-subscribing"
                );
                ticker.tick().await;
                rx = resubscribe(store.as_ref(), name, &mut resubscribes).await;
            }
            EventOrTimer::StreamEnded => {
                // NOT a shutdown. The subscription is over — the terminal
                // was already delivered, or the store dropped the sender —
                // and the controller still owns its resources.
                warn!(
                    controller = name,
                    "watch stream ended; ticking + re-subscribing (not a shutdown)"
                );
                ticker.tick().await;
                rx = resubscribe(store.as_ref(), name, &mut resubscribes).await;
            }
            wake @ (EventOrTimer::Requeue | EventOrTimer::FallbackTimer) => {
                debug!(controller = name, ?wake, "timer wake");
                ticker.tick().await;
                // Either timer is the retry point for a refused
                // subscription. Both must be: with a requeue due every
                // second, the fallback (restarted on every wait) never
                // fires, and a driver that re-subscribed only on the
                // fallback would stay stream-less forever.
                if rx.is_none() {
                    rx = resubscribe(store.as_ref(), name, &mut resubscribes).await;
                }
            }
        }
    }
}

/// A driver's run of failed ticks in a row, read against
/// [`TRANSIENT_RETRY`]. One per driver loop, owned by it; [`next_wake`]
/// is the only thing that advances or resets it.
///
/// The curve is fixed by construction — there is no way to build one on a
/// different curve — so every driver retries on the same schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsecutiveFailures(Streak);

impl Default for ConsecutiveFailures {
    fn default() -> Self {
        Self(Streak::new(TRANSIENT_RETRY))
    }
}

impl ConsecutiveFailures {
    /// Failed ticks since the last `Ok`.
    #[must_use]
    pub const fn count(&self) -> u32 {
        self.0.misses()
    }
}

/// When the driver should tick again ON ITS OWN, given how the last tick
/// ended — and fold that tick into the driver's `failures`. The one
/// decision every self-scheduled re-tick goes through, in both drivers
/// ([`WatchDriver`] and [`crate::ControllerRuntime`]).
///
///   * `Ok` + `Done` → `None`: the next event or the fallback wakes it.
///   * `Ok` + `Requeue(d)` / `RequeueWithProgress(d)` → `Some(d)`.
///   * `Err`, Declarative → `None`: a broken declaration does not fix
///     itself by being retried. It is surfaced, and the next event or
///     the fallback still re-ticks it coarsely.
///   * `Err`, Transient → a targeted retry at [`TRANSIENT_RETRY`]'s next
///     step: 1 s after the first failure in a row, doubling to 60 s.
///
/// Every `Ok` resets `failures`; every `Err`, of either class, extends it,
/// so a Transient failure after a run of Declarative ones is retried as
/// the Nth failure in a row, which it is.
#[must_use]
pub fn next_wake(
    result: &Result<ReconcileOutcome, ControllerError>,
    failures: &mut ConsecutiveFailures,
) -> Option<Duration> {
    match result {
        Ok(outcome) => {
            failures.0.reset();
            outcome.result.requeue_after()
        }
        Err(e) => {
            let owed = failures.0.miss();
            match e.classify() {
                FailureKind::Declarative => None,
                FailureKind::Transient => Some(owed),
                // `FailureKind` is `#[non_exhaustive]`, so a class shigoto
                // adds later lands here. It is retried on the same growing
                // curve — kept trying, never faster than a Transient — and
                // never at a flat delay.
                #[allow(
                    clippy::match_same_arms,
                    reason = "the known class and the unknown ones are separate decisions that happen to agree today"
                )]
                _ => Some(owed),
            }
        }
    }
}

/// Log how a tick ended and what `wake` (its [`next_wake`]) will do
/// about it. Shared by both drivers so the two loops cannot describe the
/// same outcome differently.
pub(crate) fn log_tick(
    controller: &'static str,
    result: &Result<ReconcileOutcome, ControllerError>,
    wake: Option<Duration>,
) {
    match (result, wake) {
        (Ok(outcome), None) => outcome.log(controller),
        (Ok(outcome), Some(after)) => {
            outcome.log(controller);
            debug!(
                controller,
                after_ms = millis(after),
                "controller requested requeue"
            );
        }
        (Err(e), None) => tracing::error!(
            controller,
            error = %e,
            "reconcile failed (declarative — surfacing, not scheduling a targeted retry)"
        ),
        (Err(e), Some(after)) => warn!(
            controller,
            error = %e,
            after_ms = millis(after),
            "reconcile failed (transient — scheduling a targeted retry)"
        ),
    }
}

/// A duration as whole milliseconds for a log field, saturating rather
/// than truncating: `Duration::MAX` does not fit in a `u64` of millis.
fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// The driver's ONE pending re-tick: a deadline held by the loop, never
/// a task.
///
/// Owned by `run`'s [`Ticker`]: only [`Ticker::tick`] writes it, and
/// `wait_for_relevant_event` gets a copy to sleep on. There is no
/// second holder of a pending re-tick, so there is nothing to pile up.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct RequeueSlot {
    at: Option<Instant>,
}

impl RequeueSlot {
    /// A tick is a full sweep: whatever was pending is satisfied by it.
    fn clear(&mut self) {
        self.at = None;
    }

    /// Hold a re-tick at `deadline`. The earlier deadline wins, so arming
    /// can never push a due re-tick further out.
    fn arm(&mut self, deadline: Instant) {
        self.at = Some(self.at.map_or(deadline, |at| at.min(deadline)));
    }

    /// Sleep until the deadline; pend forever when nothing is armed.
    async fn due(self) {
        match self.at {
            Some(at) => tokio::time::sleep_until(at).await,
            None => std::future::pending::<()>().await,
        }
    }
}

/// What `run` ticks through: the controller, the one requeue slot, the
/// driver's failed ticks in a row, and the heartbeat every tick is recorded
/// in. `tick` is the ONLY way `run` ticks.
struct Ticker<C: Controller + ?Sized + 'static> {
    controller: Arc<C>,
    stuck_after: Duration,
    slot: RequeueSlot,
    failures: ConsecutiveFailures,
    beat: Arc<Heartbeat>,
}

impl<C: Controller + ?Sized + 'static> Ticker<C> {
    /// Tick through the slot: clear it, tick, log, re-arm from the outcome
    /// (which also advances or resets the failure streak).
    ///
    /// A requeue far enough out that the deadline does not fit in an
    /// `Instant` is left unarmed — it is further away than the fallback
    /// anyway.
    async fn tick(&mut self) {
        self.slot.clear();
        self.beat.begin();
        let result = tick_observed(&self.controller, self.stuck_after).await;
        self.beat.end(TickClass::of(&result));
        let wake = next_wake(&result, &mut self.failures);
        log_tick(self.controller.name(), &result, wake);
        if let Some(deadline) = wake.and_then(|after| Instant::now().checked_add(after)) {
            self.slot.arm(deadline);
        }
    }
}

#[derive(Debug)]
enum EventOrTimer {
    Event,
    /// The stream terminated with a typed [`WatchGone`] — tick + resub.
    Gone(WatchGone),
    /// The slot's deadline came first: the re-tick the last outcome asked
    /// for.
    Requeue,
    FallbackTimer,
    /// The subscription is over with no terminal left to read: the
    /// one-shot [`WatchGone`] was already consumed, or the store dropped
    /// the sender. It means RE-SUBSCRIBE, and it is spelled that way so
    /// nobody can read it as a stop again — there is no `Shutdown`
    /// variant to fall into. Stopping a driver is aborting its task, out
    /// of band, the way upstream keeps `stopCh` separate from
    /// `ResultChan`.
    StreamEnded,
}

/// Wait for the first of: a matching event (or the stream's end), the
/// requeue slot's deadline, the fallback. That is, sleep until
/// `min(fallback, requeue)` unless the stream says something first.
async fn wait_for_relevant_event(
    rx: Option<&mut WatchStream>,
    config: &WatchDriverConfig,
    slot: RequeueSlot,
) -> EventOrTimer {
    let fallback = tokio::time::sleep(config.fallback_interval);
    tokio::pin!(fallback);
    let requeue = slot.due();
    tokio::pin!(requeue);

    // No live stream → only the timers can fire.
    let Some(rx) = rx else {
        return tokio::select! {
            biased;
            () = &mut requeue => EventOrTimer::Requeue,
            () = &mut fallback => EventOrTimer::FallbackTimer,
        };
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
            () = &mut requeue => return EventOrTimer::Requeue,
            () = &mut fallback => return EventOrTimer::FallbackTimer,
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
        change_at(Revision(rev))
    }

    fn change_at(r: Revision) -> Change {
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

    // ── T2.1: one requeue slot per driver ──────────────────────────────
    //
    // The shape this replaces spawned a detached sleep-then-tick task for
    // every requeue. The re-tick armed another, so every event or fallback
    // tick of a requeueing controller left behind a chain that re-armed
    // itself forever: ~120 new chains an hour from the 30 s fallback
    // alone, plus one per event, all running `tick` concurrently. The tick
    // rate grew with uptime. On plo a Transient failure retried ~3.5x/s
    // against an advertised 1 s backoff.
    //
    // These tests drive the real `run` loop for one VIRTUAL hour under
    // paused time: an event every 5 s, a 30 s fallback, and a tick that
    // takes 100 ms (it does I/O — which is what lets concurrent ticks
    // overlap at all).

    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::controller::{ReconcileReport, ReconcileResult};

    const HOUR: Duration = Duration::from_secs(3600);
    const EVENT_EVERY: Duration = Duration::from_secs(5);
    const FALLBACK: Duration = Duration::from_secs(30);
    const TICK_TAKES: Duration = Duration::from_millis(100);
    const REQUEUE_AFTER: Duration = Duration::from_secs(1);

    const EVENTS: u64 = HOUR.as_secs() / EVENT_EVERY.as_secs();
    const FALLBACKS: u64 = HOUR.as_secs() / FALLBACK.as_secs();
    /// Every tick an event or the fallback explains on its own: 720 + 120.
    const WITHOUT_TARGETED_RETRY: u64 = EVENTS + FALLBACKS;
    /// What ONE slot can reach at most: one re-tick per requeue interval,
    /// plus every event and every fallback — 3600 + 720 + 120 = 4440.
    const ONE_SLOT_CEILING: u64 = HOUR.as_secs() / REQUEUE_AFTER.as_secs() + WITHOUT_TARGETED_RETRY;
    const _: () = assert!(ONE_SLOT_CEILING == 4440, "the plan's bound, derived");

    #[derive(Clone, Copy, Debug)]
    enum Answer {
        Done,
        Requeue(Duration),
        Transient,
        Declarative,
    }

    impl Answer {
        fn result(self) -> Result<ReconcileOutcome, ControllerError> {
            match self {
                Self::Done => Ok(ReconcileOutcome::new(
                    ReconcileReport::default(),
                    ReconcileResult::Done,
                )),
                Self::Requeue(after) => Ok(ReconcileOutcome::new(
                    ReconcileReport::default(),
                    ReconcileResult::Requeue(after),
                )),
                Self::Transient => Err(ControllerError::Store(
                    engenho_store::StoreError::ClientWriteFailed("connection refused".into()),
                )),
                Self::Declarative => Err(ControllerError::InvalidResource(
                    "spec.template is required".into(),
                )),
            }
        }
    }

    #[derive(Debug, Default, Clone, Copy)]
    struct Stats {
        ticks: u64,
        in_flight: u64,
        max_in_flight: u64,
        last_start: Option<Instant>,
        /// Longest gap between two consecutive tick starts.
        max_gap: Duration,
    }

    /// A controller that always answers the same way and records how it
    /// was driven.
    struct Probe {
        answer: Answer,
        /// How long one tick takes.
        takes: Duration,
        stats: Mutex<Stats>,
        /// When each tick started, in order.
        starts: Mutex<Vec<Instant>>,
    }

    impl Probe {
        fn new(answer: Answer) -> Arc<Self> {
            Self::taking(answer, TICK_TAKES)
        }

        fn taking(answer: Answer, takes: Duration) -> Arc<Self> {
            Arc::new(Self {
                answer,
                takes,
                stats: Mutex::new(Stats::default()),
                starts: Mutex::new(Vec::new()),
            })
        }

        fn stats(&self) -> Stats {
            *self.stats.lock().unwrap()
        }

        /// The gaps between consecutive tick starts.
        fn gaps(&self) -> Vec<Duration> {
            gaps(&self.starts.lock().unwrap())
        }
    }

    fn gaps(at: &[Instant]) -> Vec<Duration> {
        at.windows(2).map(|w| w[1] - w[0]).collect()
    }

    #[async_trait::async_trait]
    impl Controller for Probe {
        fn name(&self) -> &'static str {
            "probe"
        }

        async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
            {
                let now = Instant::now();
                let mut s = self.stats.lock().unwrap();
                if let Some(prev) = s.last_start {
                    s.max_gap = s.max_gap.max(now - prev);
                }
                s.last_start = Some(now);
                s.ticks += 1;
                s.in_flight += 1;
                s.max_in_flight = s.max_in_flight.max(s.in_flight);
                self.starts.lock().unwrap().push(now);
            }
            tokio::time::sleep(self.takes).await;
            self.stats.lock().unwrap().in_flight -= 1;
            self.answer.result()
        }
    }

    /// The store's live tail reduced to what the loop reads: a registry
    /// the test fans changes into, which can refuse subscriptions or end
    /// every one it grants.
    struct Feed {
        state: Mutex<(WatcherRegistry, Revision)>,
        refuse_next: AtomicUsize,
        accepted: AtomicUsize,
        /// Grant each subscription from a registry that is dropped at
        /// once, so the stream ends as soon as it is read.
        end_every_stream: bool,
        /// When each subscription was asked for, in order.
        asked: Mutex<Vec<Instant>>,
    }

    impl Feed {
        fn new() -> Arc<Self> {
            Self::refusing(0)
        }

        fn refusing(n: usize) -> Arc<Self> {
            Arc::new(Self {
                state: Mutex::new((WatcherRegistry::new(), Revision::ZERO)),
                refuse_next: AtomicUsize::new(n),
                accepted: AtomicUsize::new(0),
                end_every_stream: false,
                asked: Mutex::new(Vec::new()),
            })
        }

        fn ending_every_stream() -> Arc<Self> {
            Arc::new(Self {
                state: Mutex::new((WatcherRegistry::new(), Revision::ZERO)),
                refuse_next: AtomicUsize::new(0),
                accepted: AtomicUsize::new(0),
                end_every_stream: true,
                asked: Mutex::new(Vec::new()),
            })
        }

        fn emit(&self) {
            let mut state = self.state.lock().unwrap();
            let (reg, rev) = &mut *state;
            *rev = rev.next();
            reg.fan_change(&change_at(*rev));
        }
    }

    #[async_trait::async_trait]
    impl WatchSource for Feed {
        async fn watch(&self) -> Result<WatchStream, WatchGone> {
            self.asked.lock().unwrap().push(Instant::now());
            let refused = self
                .refuse_next
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok();
            if refused {
                return Err(WatchGone::CompactedTooOld {
                    requested: Revision::ZERO,
                    compacted: Revision::ZERO,
                });
            }
            self.accepted.fetch_add(1, Ordering::SeqCst);
            let opts = |from| WatchOpts {
                from,
                buffer: 1024,
                bookmark_every: Duration::ZERO,
            };
            if self.end_every_stream {
                // The registry, and with it the stream's sender, is
                // dropped on return: the stream reads as ended.
                return Ok(WatcherRegistry::new().register_captured(
                    Vec::new(),
                    Revision::ZERO,
                    &opts(Revision::ZERO),
                ));
            }
            let mut state = self.state.lock().unwrap();
            let (reg, rev) = &mut *state;
            Ok(reg.register_captured(Vec::new(), *rev, &opts(*rev)))
        }
    }

    /// Drive the real loop for one virtual hour: an event every 5 s and a
    /// 30 s fallback.
    async fn an_hour_of(probe: &Arc<Probe>, feed: &Arc<Feed>) -> Stats {
        drive(probe, feed, FALLBACK, Some(EVENT_EVERY), HOUR).await
    }

    /// Drive the real loop for `run_for` of virtual time, with the given
    /// fallback, emitting an event every `event_every` if set.
    async fn drive(
        probe: &Arc<Probe>,
        feed: &Arc<Feed>,
        fallback: Duration,
        event_every: Option<Duration>,
        run_for: Duration,
    ) -> Stats {
        let config = WatchDriverConfig {
            fallback_interval: fallback,
            ..WatchDriverConfig::default()
        };
        let driver = tokio::spawn(run(
            probe.clone(),
            feed.clone(),
            config,
            Arc::new(Heartbeat::new()),
        ));
        let events = event_every.map(|every| {
            let feed = feed.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(every).await;
                    feed.emit();
                }
            })
        });
        tokio::time::sleep(run_for).await;
        driver.abort();
        let _ = driver.await;
        if let Some(events) = events {
            events.abort();
            let _ = events.await;
        }
        probe.stats()
    }

    #[tokio::test(start_paused = true)]
    async fn a_controller_that_always_requeues_gets_one_slot_not_a_chain() {
        let probe = Probe::new(Answer::Requeue(REQUEUE_AFTER));
        let s = an_hour_of(&probe, &Feed::new()).await;

        assert_eq!(
            s.max_in_flight, 1,
            "{} ticks of one controller ran at once; a driver has one slot",
            s.max_in_flight
        );
        assert!(
            s.ticks <= ONE_SLOT_CEILING,
            "{} ticks in an hour, above the one-slot ceiling of {ONE_SLOT_CEILING}: \
             re-ticks are piling up",
            s.ticks
        );
        // …and the requeue is honoured, not swallowed.
        assert!(
            s.ticks > WITHOUT_TARGETED_RETRY,
            "{} ticks is no more than events + fallback explain; the requeue was dropped",
            s.ticks
        );
        assert!(
            s.max_gap <= REQUEUE_AFTER + TICK_TAKES + Duration::from_millis(10),
            "a {:?} gap between ticks: the 1 s requeue was not kept",
            s.max_gap
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_controller_that_always_fails_transiently_gets_one_slot_not_a_chain() {
        let probe = Probe::new(Answer::Transient);
        assert!(
            next_wake(
                &Answer::Transient.result(),
                &mut ConsecutiveFailures::default()
            )
            .is_some(),
            "precondition: a Transient error is retried"
        );
        let s = an_hour_of(&probe, &Feed::new()).await;

        assert_eq!(
            s.max_in_flight, 1,
            "{} failing ticks ran at once; retries must share one slot",
            s.max_in_flight
        );
        assert!(
            s.ticks <= ONE_SLOT_CEILING,
            "{} retries in an hour, above the one-slot ceiling of {ONE_SLOT_CEILING}",
            s.ticks
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_declarative_failure_is_ticked_by_events_and_the_fallback_only() {
        let probe = Probe::new(Answer::Declarative);
        let s = an_hour_of(&probe, &Feed::new()).await;

        assert!(
            s.ticks <= WITHOUT_TARGETED_RETRY,
            "{} ticks: a broken declaration got a targeted retry",
            s.ticks
        );
        // Surfaced, not retired: events still reach the controller. The
        // last event lands at the end of the hour, inside its debounce.
        assert!(
            s.ticks + 1 >= EVENTS,
            "{} ticks for {EVENTS} events: the driver stopped ticking a failing controller",
            s.ticks
        );
    }

    /// A requeue due every second keeps restarting the wait, so the
    /// fallback never fires. A driver whose subscription was refused must
    /// retry it on the requeue wake too, or it stays stream-less forever.
    #[tokio::test(start_paused = true)]
    async fn a_refused_subscription_is_retried_even_when_requeues_outpace_the_fallback() {
        let probe = Probe::new(Answer::Requeue(REQUEUE_AFTER));
        let feed = Feed::refusing(3);
        let _ = an_hour_of(&probe, &feed).await;

        assert!(
            feed.accepted.load(Ordering::SeqCst) >= 1,
            "the store refused three subscriptions and the driver never asked again"
        );
    }

    #[test]
    fn next_wake_on_a_declarative_error_is_none() {
        let mut failures = ConsecutiveFailures::default();
        for _ in 0..3 {
            assert_eq!(
                next_wake(&Answer::Declarative.result(), &mut failures),
                None
            );
        }
    }

    #[test]
    fn next_wake_follows_the_outcome() {
        let d = Duration::from_millis(250);
        let ok = |result| Ok(ReconcileOutcome::new(ReconcileReport::default(), result));
        let mut failures = ConsecutiveFailures::default();
        assert_eq!(next_wake(&ok(ReconcileResult::Done), &mut failures), None);
        assert_eq!(
            next_wake(&ok(ReconcileResult::Requeue(d)), &mut failures),
            Some(d)
        );
        assert_eq!(
            next_wake(&ok(ReconcileResult::RequeueWithProgress(d)), &mut failures),
            Some(d)
        );
        // The first Transient failure in a row is retried at the base.
        assert_eq!(
            next_wake(&Answer::Transient.result(), &mut failures),
            Some(Duration::from_secs(1))
        );
    }

    // ── T2.2: retries grow ─────────────────────────────────────────────
    //
    // A Transient failure used to be retried at a flat 1 s for as long as
    // it lasted: a store that was down for an hour cost every driver 3,600
    // failed sweeps, each a log line and a round-trip. The retry now grows
    // on one curve, and forgets on success.

    /// Measured on the real loop, with no events. The fallback fires the
    /// first tick and is longer than the cap, so it never fires again once
    /// the retries start: every later tick is a targeted retry. Each gap is
    /// at least 1.8x the one before until the cap, and then holds at it.
    #[tokio::test(start_paused = true)]
    async fn transient_retry_gaps_grow_until_the_cap() {
        const KICK: Duration = Duration::from_secs(90);
        const _: () = assert!(KICK.as_millis() > TRANSIENT_RETRY.cap().as_millis() + 100);
        let probe = Probe::new(Answer::Transient);
        let _ = drive(&probe, &Feed::new(), KICK, None, Duration::from_secs(600)).await;
        let gaps = probe.gaps();
        let cap = TRANSIENT_RETRY.cap() + TICK_TAKES;

        assert_eq!(
            gaps.first().copied(),
            Some(TRANSIENT_RETRY.base() + TICK_TAKES),
            "the first retry is at the base: {gaps:?}"
        );
        assert_grows_then_holds(&gaps, cap);
        assert!(
            gaps.iter().filter(|g| **g == cap).count() >= 3,
            "ten minutes of failure never reached the {cap:?} cap: {gaps:?}"
        );
    }

    /// Every gap is at least 1.8x the one before it until one reaches
    /// `cap`; from there every gap is exactly `cap`.
    fn assert_grows_then_holds(gaps: &[Duration], cap: Duration) {
        assert!(gaps.len() >= 3, "too few retries to see a curve: {gaps:?}");
        for pair in gaps.windows(2) {
            let (a, b) = (pair[0], pair[1]);
            assert!(b <= cap, "a {b:?} gap is past the {cap:?} cap: {gaps:?}");
            if a < cap {
                assert!(
                    b == cap || b.as_micros() * 10 >= a.as_micros() * 18,
                    "{a:?} then {b:?}: the retry gap did not grow 1.8x toward the cap: {gaps:?}"
                );
            } else {
                assert_eq!(b, cap, "past the cap the gap holds: {gaps:?}");
            }
        }
    }

    #[test]
    fn a_success_resets_the_retry_curve() {
        let mut failures = ConsecutiveFailures::default();
        let owed: Vec<_> = (0..4)
            .map(|_| next_wake(&Answer::Transient.result(), &mut failures))
            .collect();
        assert_eq!(
            owed,
            [1, 2, 4, 8].map(|s| Some(Duration::from_secs(s))),
            "consecutive failures walk the curve"
        );
        assert_eq!(failures.count(), 4);

        let _ = next_wake(&Answer::Done.result(), &mut failures);
        assert_eq!(failures.count(), 0, "an Ok ends the streak");
        assert_eq!(
            next_wake(&Answer::Transient.result(), &mut failures),
            Some(TRANSIENT_RETRY.base()),
            "the first failure after a success is retried at the base again"
        );
    }

    /// A Declarative failure is not retried, but it is a failure in a row:
    /// a Transient one after it is retried further out, not at the base.
    #[test]
    fn a_declarative_failure_extends_the_streak_without_a_retry() {
        let mut failures = ConsecutiveFailures::default();
        assert_eq!(
            next_wake(&Answer::Declarative.result(), &mut failures),
            None
        );
        assert_eq!(failures.count(), 1);
        assert_eq!(
            next_wake(&Answer::Transient.result(), &mut failures),
            Some(TRANSIENT_RETRY.delay(1))
        );
    }

    /// The re-subscribe path on the same curve family: a store that ends
    /// every stream it grants is asked again at gaps growing 1.8x per
    /// attempt up to the 30 s cap — never hot-looped, never abandoned.
    #[tokio::test(start_paused = true)]
    async fn resubscribe_gaps_grow_until_the_cap() {
        let probe = Probe::taking(Answer::Done, Duration::ZERO);
        let feed = Feed::ending_every_stream();
        let _ = drive(&probe, &feed, HOUR, None, Duration::from_secs(300)).await;
        let gaps = gaps(&feed.asked.lock().unwrap());

        assert_eq!(
            gaps.first().copied(),
            Some(RESUBSCRIBE.base()),
            "the first re-subscribe waits the base: {gaps:?}"
        );
        assert_grows_then_holds(&gaps, RESUBSCRIBE.cap());
        assert!(
            gaps.iter().filter(|g| **g == RESUBSCRIBE.cap()).count() >= 3,
            "five minutes of ended streams never reached the cap: {gaps:?}"
        );
    }

    #[test]
    fn the_slot_keeps_the_earlier_deadline_and_a_tick_clears_it() {
        let now = Instant::now();
        let mut slot = RequeueSlot::default();
        slot.arm(now + Duration::from_secs(5));
        slot.arm(now + Duration::from_secs(1));
        slot.arm(now + Duration::from_secs(3));
        assert_eq!(slot.at, Some(now + Duration::from_secs(1)));
        slot.clear();
        assert_eq!(slot, RequeueSlot::default());
    }

    // ── T2.6: the loop's ticks are recorded where a supervisor can read them
    //
    // The ryn defect's only evidence was a missing log line. A driver now
    // writes every tick into its heartbeat — begun, ended, and how — so
    // "when did the kubelet last finish a tick?" has an answer in-process.

    /// Drive the real loop with `answer` for `run_for`, returning the
    /// heartbeat it wrote.
    async fn heartbeat_of(probe: &Arc<Probe>, run_for: Duration) -> Arc<Heartbeat> {
        let beat = Arc::new(Heartbeat::new());
        let config = WatchDriverConfig {
            fallback_interval: FALLBACK,
            ..WatchDriverConfig::default()
        };
        let driver = tokio::spawn(run(probe.clone(), Feed::new(), config, beat.clone()));
        tokio::time::sleep(run_for).await;
        driver.abort();
        let _ = driver.await;
        beat
    }

    #[tokio::test(start_paused = true)]
    async fn every_tick_is_recorded_in_the_heartbeat_with_its_class() {
        for (answer, class) in [
            (Answer::Done, TickClass::Done),
            (Answer::Requeue(REQUEUE_AFTER), TickClass::Requeued),
            (Answer::Transient, TickClass::Transient),
            (Answer::Declarative, TickClass::Declarative),
        ] {
            let probe = Probe::new(answer);
            let beat = heartbeat_of(&probe, Duration::from_secs(95))
                .await
                .snapshot();
            let ticks = probe.stats().ticks;

            assert!(ticks >= 3, "{answer:?}: only {ticks} ticks in 95 s");
            assert_eq!(
                beat.ticks_started, ticks,
                "{answer:?}: the heartbeat missed a tick the controller saw"
            );
            // The abort can land inside a tick: that one began, never ended.
            assert!(
                beat.ticks_finished + 1 >= beat.ticks_started,
                "{answer:?}: {beat:?}"
            );
            assert_eq!(beat.last_class, Some(class), "{answer:?}: {beat:?}");
            assert!(beat.last_end.is_some() && beat.last_start.is_some());
        }
    }

    /// A tick that has begun and not returned reads as in flight, with the
    /// time it began — the fact T2.8's stall detection is built on.
    #[tokio::test(start_paused = true)]
    async fn a_tick_in_progress_reads_as_in_flight() {
        let probe = Probe::taking(Answer::Done, Duration::from_secs(20));
        let beat = Arc::new(Heartbeat::new());
        let config = WatchDriverConfig {
            fallback_interval: FALLBACK,
            ..WatchDriverConfig::default()
        };
        let driver = tokio::spawn(run(probe.clone(), Feed::new(), config, beat.clone()));
        // The first fallback tick begins at 30 s and runs until 50 s.
        tokio::time::sleep(FALLBACK + Duration::from_secs(5)).await;
        let mid = beat.snapshot();
        driver.abort();
        let _ = driver.await;

        assert!(
            mid.in_flight(),
            "a 20 s tick 5 s in is not in flight: {mid:?}"
        );
        assert_eq!((mid.ticks_started, mid.ticks_finished), (1, 0));
        assert_eq!(mid.last_class, None, "no tick has ended yet: {mid:?}");
        assert!(mid.last_start.is_some());
    }
}
