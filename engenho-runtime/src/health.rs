//! Health derived from observation: the runtime's half (T2.8, I10).
//!
//! ## What this replaces
//!
//! The apiserver's `/livez`, `/healthz` and `/readyz` answered the constant
//! `"ok"`, and its `/metrics` a literal store revision of `0`. On plo
//! (2026-09-06) a controller retried hot enough to peg a core and hang every
//! API read while `/healthz` said ok; on ryn (2026-09-18) the kubelet's
//! controller was retired for 12 hours and nothing in the process said so.
//! The apiserver half of T2.8 turned every one of those answers into a read
//! through a seam, a [`LivenessSource`] and a [`MetricsSource`], and left
//! both unwired: a router with no source fails every health check and omits
//! the runtime's metric families. [`Health`] is what the runtime installs in
//! both.
//!
//! ## What each child is judged from
//!
//! Every row comes from two observations the runtime already owns: the
//! child's [`Heartbeat`] and its task's [`AbortHandle`]. An ended task is
//! [`Liveness::Dead`] whether or not the supervisor has polled
//! [`crate::Children::next_dead`] yet. A running one is read by what it does
//! ([`Pulse`]):
//!
//! | child | in flight | not in flight |
//! |---|---|---|
//! | tick loop (a driver, the node lease) | alive until the tick is older than the stuck window, then stalled since it began | alive until the last tick ended longer ago than the idle window, then stalled since it ended |
//! | listener | serving: alive (a serve that accepts nothing reads the same: W6's `Unobserved` row) | halted or panicked, waiting to rebind: stalled since it ended |
//!
//! A child that has never beaten is [`Liveness::Unknown`], which no health
//! endpoint renders as ok.
//!
//! Both windows come from ONE [`Windows`] value, built from the config and
//! handed to the drivers too, so the tick a driver logs as BLOCKED is the
//! tick liveness reports as stalled: the two thresholds are one value, not
//! two constants that agree today. A driver's fallback and debounce are read
//! off the same value, so the idle window is derived from the fallback the
//! loop really runs on. Two loops run on a fallback of their own, the
//! scheduler (`scheduler.tick_interval_seconds`) and the node lease
//! ([`Windows::node_lease`]); [`Windows::of_child`] picks each loop's windows,
//! and both the driver and the judge read them from there.
//!
//! One row's judgement ([`Row::liveness`]) is also what the node lease
//! renews by: the lease is renewed only while the kubelet's row is alive
//! (T1.3c, [`crate::node_lease`]), so `/livez` and the node's readiness
//! cannot disagree about whether the kubelet is alive.
//!
//! ## The metrics
//!
//! One [`MetricsSnapshot`] per scrape: the store's real revision, the panic
//! count of the hook [`PanicCounter`] proves installed, each driver's
//! reconciles by result, and when each driver last finished a tick. Every
//! driven controller is wrapped in [`Tallied`], which counts how each tick
//! ended; a tick that panicked never returns to the wrapper, so it is counted
//! as an `error` from the heartbeat's panic count (controller-runtime counts
//! a recovered panic as an error the same way).
//!
//! ## The propose-rate detector
//!
//! [`Tallied`] also records, per second, whether a tick landed a write (the
//! controller's own `objects_changed`, which since T1.8 counts only writes
//! the store applied). A controller that landed a write in at least
//! [`CONTINUOUS_AFTER`] of the last [`SPAN`] seconds is proposing
//! continuously ([`ProposeRate::Continuous`]) and is logged at WARN once when
//! it starts and at INFO once when it stops. That is the gc churn class: on
//! ryn (2026-09-18) gc deleted `pitr-lab/mysql-0` about ten times a second
//! for days, `changed=1` on every tick, and nothing flagged it. A burst (a
//! rollout, a boot sweep) fills a handful of seconds; a Job whose pods finish
//! every few seconds fills well under the threshold.
//!
//! ## Tier
//!
//! * The constant health answer: gone, and the runtime's rows are derived,
//!   never asserted. That the runtime installs its sources at all is caught
//!   by `tests/t2_8_health_from_observation.rs`, not a type: `RouterState`
//!   still builds without them (and then fails every check).
//! * `/readyz`'s store read is linearizable because the runtime's store is a
//!   single voter (`boot_store` builds node 1 alone): the leader is the
//!   whole quorum and every acknowledged write was applied before its ack.
//!   A multi-voter store needs a read-index — deferred to engenho-store.
//! * The detector reads the controller's own report of what it changed. A
//!   controller that writes without counting it is invisible here; the
//!   store-side detector keyed by `(key, controller)` is deferred (T2.8).
//! * It is a detector: it logs and reports, it never slows or stops a
//!   controller.

use std::fmt;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use arc_swap::ArcSwap;
use engenho_apiserver::{
    ChildLiveness, DrainState, LastTick, LivenessSource, MetricsSnapshot, MetricsSource,
    ReconcileCount, ReconcileResult,
};
use engenho_controllers::node_lease::RENEW_INTERVAL;
use engenho_controllers::{
    Beat, Controller, ControllerError, ControllerType, Heartbeat, RESUBSCRIBE, ReconcileOutcome,
};
use engenho_store::{Revision, StoreError, StoreMesh};
use engenho_substrate::{
    Clock as _, Instant as WallInstant, Liveness, StaleAfter, TaskState, WallClock,
};
use tokio::task::AbortHandle;
use tokio::time::Instant;
use tracing::{info, warn};

use crate::child::{Child, Driver};
use crate::panics::PanicCounter;

/// How long a tick may be in flight before the driver logs it BLOCKED and
/// liveness reports its driver stalled. The tick is never cancelled; see
/// `watch_driver::tick_observed`, which copies the kubelet's
/// `syncLoopHealthCheck` posture.
///
/// It is also T1.3c's pod-sync threshold: the node lease is renewed only
/// while the kubelet's row is alive, so a kubelet tick in flight longer than
/// this stops the renewals, and the node reads `NotReady` one
/// [`engenho_controllers::node_lease::GRACE_PERIOD`] after the last one.
/// Upstream's PLEG health check allows three minutes; this plus the grace
/// period is about 160 s. An image pull longer than this is read as a hung
/// tick.
///
/// `pending-config: controllers.stuck_tick_after_seconds` — the field
/// belongs in engenho-config. Until it lands, this is the value
/// [`Windows::new`] reads, and the only place it is written.
pub(crate) const STUCK_TICK_AFTER: Duration = Duration::from_secs(120);

/// How many recent seconds the propose-rate detector looks at.
pub const SPAN: u32 = 60;

/// How many of the last [`SPAN`] seconds must each have landed a write for a
/// controller to be proposing continuously: five in six.
pub const CONTINUOUS_AFTER: u32 = 50;

/// The bits of a [`ProposeWindow`] that fall inside [`SPAN`].
const SPAN_MASK: u64 = (1_u64 << SPAN) - 1;

// ── windows ────────────────────────────────────────────────────────────

/// What a tick loop runs on and is judged against, built once from the
/// config: its fallback and debounce, and the two windows they imply.
///
/// The runtime drives every tick loop from this value (its fallback,
/// debounce and `stuck_tick_after`) and hands the same value to [`Health`],
/// so the driver's BLOCKED log and liveness's stall are one threshold, and
/// the idle window is derived from the fallback the loop really runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Windows {
    stuck: Duration,
    fallback: Duration,
    debounce: Duration,
    /// The scheduler's own fallback (`scheduler.tick_interval_seconds`,
    /// T5.8). A field rather than a default, so windows that would drive the
    /// scheduler on the controllers' fallback cannot be built.
    scheduler_fallback: Duration,
}

impl Windows {
    /// The windows for loops that fall back every `fallback` and coalesce
    /// events for `debounce`, with the scheduler falling back every
    /// `scheduler_fallback`. The runtime builds them from its config
    /// (`BootConfig::windows`).
    #[must_use]
    pub const fn new(fallback: Duration, debounce: Duration, scheduler_fallback: Duration) -> Self {
        Self {
            stuck: STUCK_TICK_AFTER,
            fallback,
            debounce,
            scheduler_fallback,
        }
    }

    /// The windows `child` is driven on and judged by: the scheduler and the
    /// node lease on their own fallback, every other loop on these.
    ///
    /// The one place a loop's windows are chosen. The runtime drives each
    /// loop with this value and [`Pulse::of`] judges it with the same one, so
    /// a loop cannot be judged on a fallback it does not run on.
    #[must_use]
    pub const fn of_child(self, child: Child) -> Self {
        match child {
            Child::Driver(Driver::Scheduler) => self.with_fallback(self.scheduler_fallback),
            Child::NodeLease => self.node_lease(),
            Child::Driver(_) | Child::Listener(_) => self,
        }
    }

    /// The same windows for a loop that falls back every `fallback`.
    #[must_use]
    pub const fn with_fallback(self, fallback: Duration) -> Self {
        Self { fallback, ..self }
    }

    /// The same windows with a stuck-tick threshold of `stuck`, so a test
    /// can watch a hung tick go stalled in a second rather than two minutes.
    /// Test-only until `controllers.stuck_tick_after_seconds` lands.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn with_stuck_tick_after(self, stuck: Duration) -> Self {
        Self { stuck, ..self }
    }

    /// The windows the node lease is driven on and judged by: its fallback
    /// is the lease's renew interval
    /// ([`engenho_controllers::node_lease::RENEW_INTERVAL`]), because no
    /// store event wakes it.
    #[must_use]
    pub const fn node_lease(self) -> Self {
        self.with_fallback(RENEW_INTERVAL)
    }

    /// How long one tick may run before it is stalled (and logged BLOCKED).
    #[must_use]
    pub const fn stuck_tick_after(self) -> Duration {
        self.stuck
    }

    /// How long the loop waits for an event before it ticks anyway.
    #[must_use]
    pub const fn fallback(self) -> Duration {
        self.fallback
    }

    /// How long the loop coalesces a burst of events before it ticks.
    #[must_use]
    pub const fn debounce(self) -> Duration {
        self.debounce
    }

    /// How long an idle tick loop may go without beginning a tick.
    ///
    /// An idle loop begins its next tick at most one fallback after its last
    /// one ended, plus the debounce, plus a re-subscribe wait
    /// ([`RESUBSCRIBE`]'s cap) when its stream was lost. The window is twice
    /// the fallback plus both, the way upstream's syncLoop health check
    /// allows twice its resync interval.
    #[must_use]
    pub const fn idle_after(self) -> Duration {
        self.fallback
            .saturating_mul(2)
            .saturating_add(RESUBSCRIBE.cap())
            .saturating_add(self.debounce)
    }
}

/// `d` as a freshness window: rounded UP to the next whole second, so a
/// child is judged stalled a moment late rather than a healthy one early.
/// Infallible: the result is at least one second.
fn window(d: Duration) -> StaleAfter {
    StaleAfter::from_secs(NonZeroU64::MIN.saturating_add(d.as_secs()))
}

// ── the judge ──────────────────────────────────────────────────────────

/// What a child's heartbeat means, by what the child does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pulse {
    /// A tick loop. In flight, its last tick's start is judged against the
    /// stuck window; idle, its last tick's end against the idle window.
    Ticks(Windows),
    /// A listener. In flight is serving (binding is part of it); not in
    /// flight is halted, waiting to rebind: its port is closed, so it is not
    /// alive whatever the age.
    Serves,
}

impl Pulse {
    /// How `child`'s heartbeat is read. Exhaustive over the catalog, so a
    /// new child shape does not compile until it says how it beats.
    #[must_use]
    pub const fn of(child: Child, windows: Windows) -> Self {
        match child {
            // The windows it is driven on: its own fallback for the
            // scheduler and the node lease.
            Child::Driver(_) | Child::NodeLease => Self::Ticks(windows.of_child(child)),
            Child::Listener(_) => Self::Serves,
        }
    }

    /// Judge one child as of `now` (the heartbeat's monotonic clock).
    /// `wall_now` only writes a stall's start down as a wall time.
    #[must_use]
    pub fn judge(
        self,
        task: TaskState,
        beat: &Beat,
        now: Instant,
        wall_now: WallInstant,
    ) -> Liveness {
        if task == TaskState::Ended {
            return Liveness::Dead;
        }
        let wall = |at: Option<Instant>| at.map(|at| wall_of(at, now, wall_now));
        match self {
            Self::Ticks(windows) if beat.in_flight() => Liveness::judge(
                task,
                wall(beat.last_start),
                wall_now,
                window(windows.stuck_tick_after()),
            ),
            Self::Ticks(windows) => Liveness::judge(
                task,
                wall(beat.last_end),
                wall_now,
                window(windows.idle_after()),
            ),
            Self::Serves if beat.in_flight() => Liveness::Alive,
            Self::Serves => match wall(beat.last_end) {
                None => Liveness::Unknown,
                Some(since) => Liveness::Stalled { since },
            },
        }
    }
}

/// `at` on the wall clock: `wall_now` less `at`'s age on the monotonic clock
/// the heartbeat stamps with. Every age comes from that one clock; the wall
/// only writes the instant down. The runtime's relist ledger
/// ([`crate::runtime_health`]) writes its instants down the same way.
pub(crate) fn wall_of(at: Instant, now: Instant, wall_now: WallInstant) -> WallInstant {
    let age_ms = u64::try_from(now.saturating_duration_since(at).as_millis()).unwrap_or(u64::MAX);
    WallInstant::from_ms(wall_now.physical_ms.saturating_sub(age_ms))
}

/// Whether a spawned child's task is still running, observed on its handle.
fn task_state(task: &AbortHandle) -> TaskState {
    if task.is_finished() {
        TaskState::Ended
    } else {
        TaskState::Running
    }
}

// ── the propose-rate detector ──────────────────────────────────────────

/// Which of the recent whole seconds a controller landed a write in: one bit
/// per second, the newest second lowest.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProposeWindow {
    /// The second bit 0 stands for.
    newest: u64,
    busy: u64,
}

impl ProposeWindow {
    /// Record a tick that ended in `second`, which `landed` a write or not.
    /// A second before the newest (one monotonic clock never goes back) is
    /// folded into the newest.
    pub fn record(&mut self, second: u64, landed: bool) {
        self.busy = shifted(self.busy, second.saturating_sub(self.newest));
        self.newest = self.newest.max(second);
        if landed {
            self.busy |= 1;
        }
    }

    /// How many of the [`SPAN`] seconds ending at `now` landed a write.
    #[must_use]
    pub fn busy_seconds(self, now: u64) -> u32 {
        (shifted(self.busy, now.saturating_sub(self.newest)) & SPAN_MASK).count_ones()
    }

    /// The verdict as of `now`.
    #[must_use]
    pub fn judge(self, now: u64) -> ProposeRate {
        let busy_seconds = self.busy_seconds(now);
        if busy_seconds >= CONTINUOUS_AFTER {
            ProposeRate::Continuous { busy_seconds }
        } else {
            ProposeRate::Below
        }
    }
}

/// `bits` aged by `seconds`: shifted up, the oldest falling off the top.
fn shifted(bits: u64, seconds: u64) -> u64 {
    u32::try_from(seconds)
        .ok()
        .and_then(|by| bits.checked_shl(by))
        .unwrap_or(0)
}

/// Whether a controller is landing writes continuously.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProposeRate {
    /// Writes landed in fewer than [`CONTINUOUS_AFTER`] of the last [`SPAN`]
    /// seconds, including none at all. Not a claim the controller is
    /// settled: only that this detector has no evidence it is not.
    Below,
    /// Writes landed in `busy_seconds` of the last [`SPAN`] seconds, at
    /// least [`CONTINUOUS_AFTER`]: the controller is not reaching a fixpoint.
    Continuous {
        /// Seconds of the last [`SPAN`] in which a write landed.
        busy_seconds: u32,
    },
}

impl fmt::Display for ProposeRate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Below => f.write_str("below the continuous threshold"),
            Self::Continuous { busy_seconds } => {
                write!(
                    f,
                    "continuous: a write landed in {busy_seconds} of the last {SPAN}s"
                )
            }
        }
    }
}

/// The detector's state for one controller: the window and whether the
/// controller is currently reported continuous (so the log is one line per
/// transition, not one per tick).
#[derive(Debug, Default)]
struct Proposals {
    window: ProposeWindow,
    continuous: bool,
}

// ── the tally ──────────────────────────────────────────────────────────

/// Every tick of one driven controller, counted by how it ended, and the
/// seconds in which it landed a write.
#[derive(Debug)]
pub(crate) struct Tally {
    controller: &'static str,
    success: AtomicU64,
    error: AtomicU64,
    requeue_after: AtomicU64,
    /// Seconds for the propose window are counted from here, on the same
    /// monotonic clock as the heartbeat.
    epoch: Instant,
    proposals: Mutex<Proposals>,
}

impl Tally {
    pub(crate) fn new(controller: &'static str) -> Self {
        Self {
            controller,
            success: AtomicU64::new(0),
            error: AtomicU64::new(0),
            requeue_after: AtomicU64::new(0),
            epoch: Instant::now(),
            proposals: Mutex::new(Proposals::default()),
        }
    }

    /// Count one tick that returned `result`, as of `at`.
    fn record(&self, result: &Result<ReconcileOutcome, ControllerError>, at: Instant) {
        let (counter, landed) = match result {
            Ok(outcome) if outcome.result.requeue_after().is_some() => {
                (&self.requeue_after, outcome.report.objects_changed > 0)
            }
            Ok(outcome) => (&self.success, outcome.report.objects_changed > 0),
            Err(_) => (&self.error, false),
        };
        counter.fetch_add(1, Ordering::Relaxed);
        self.observe_writes(landed, at);
    }

    fn second(&self, at: Instant) -> u64 {
        at.saturating_duration_since(self.epoch).as_secs()
    }

    fn observe_writes(&self, landed: bool, at: Instant) {
        let second = self.second(at);
        let mut proposals = self
            .proposals
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        proposals.window.record(second, landed);
        let rate = proposals.window.judge(second);
        match (proposals.continuous, rate) {
            (false, ProposeRate::Continuous { busy_seconds }) => {
                proposals.continuous = true;
                warn!(
                    controller = self.controller,
                    busy_seconds,
                    span_s = SPAN,
                    "controller is proposing continuously: it landed a write in almost every \
                     second of the last minute and is not reaching a fixpoint"
                );
            }
            (true, ProposeRate::Below) => {
                proposals.continuous = false;
                info!(
                    controller = self.controller,
                    "controller is no longer proposing continuously"
                );
            }
            (false, ProposeRate::Below) | (true, ProposeRate::Continuous { .. }) => {}
        }
    }

    /// The detector's verdict as of `now`: seconds with no tick age out too.
    fn propose_rate(&self, now: Instant) -> ProposeRate {
        let proposals = self
            .proposals
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        proposals.window.judge(self.second(now))
    }

    /// `controller_runtime_reconcile_total` for this controller. A panicked
    /// tick never returns here, so its count comes from `panics`.
    fn reconciles(&self, panics: u64) -> [ReconcileCount; 3] {
        let count = |result, n: u64| ReconcileCount {
            controller: self.controller,
            result,
            count: n,
        };
        [
            count(
                ReconcileResult::Success,
                self.success.load(Ordering::Relaxed),
            ),
            count(
                ReconcileResult::Error,
                self.error.load(Ordering::Relaxed).saturating_add(panics),
            ),
            count(
                ReconcileResult::RequeueAfter,
                self.requeue_after.load(Ordering::Relaxed),
            ),
        ]
    }
}

/// A controller whose every returned tick is counted into a [`Tally`]. It
/// only forwards: its name and type are the wrapped controller's, so no
/// driver ever records the adapter as what it runs.
pub(crate) struct Tallied<C> {
    controller: C,
    tally: Arc<Tally>,
}

impl<C> Tallied<C> {
    pub(crate) fn new(controller: C, tally: Arc<Tally>) -> Self {
        Self { controller, tally }
    }
}

#[async_trait::async_trait]
impl<C: Controller> Controller for Tallied<C> {
    fn name(&self) -> &'static str {
        self.controller.name()
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let result = self.controller.tick().await;
        self.tally.record(&result, Instant::now());
        result
    }

    fn controller_type(&self) -> ControllerType {
        self.controller.controller_type()
    }
}

// ── the rows and the source ────────────────────────────────────────────

/// One spawned child, as [`Health`] reads it.
#[derive(Debug, Clone)]
pub(crate) struct Row {
    child: Child,
    beat: Arc<Heartbeat>,
    task: AbortHandle,
    tally: Option<Arc<Tally>>,
}

impl Row {
    pub(crate) fn new(
        child: Child,
        beat: Arc<Heartbeat>,
        task: AbortHandle,
        tally: Option<Arc<Tally>>,
    ) -> Self {
        Self {
            child,
            beat,
            task,
            tally,
        }
    }

    /// This child's liveness as of `now` (the heartbeat's monotonic clock),
    /// judged against `windows` by the child's [`Pulse`].
    ///
    /// The one judgement of a row: `/livez` renders it, and the node lease
    /// is renewed only while the kubelet's is [`Liveness::Alive`].
    pub(crate) fn liveness(
        &self,
        windows: Windows,
        now: Instant,
        wall_now: WallInstant,
    ) -> Liveness {
        Pulse::of(self.child, windows).judge(
            task_state(&self.task),
            &self.beat.snapshot(),
            now,
            wall_now,
        )
    }
}

/// What the runtime's health endpoints and metric families are read from:
/// its spawned children, its drain state and its store.
///
/// Built before the apiserver binds (the router must hold it from its first
/// request) and handed the children once they are spawned — and again each
/// time the set changes (a child respawned, a driver enabled or disabled
/// through the control plane). Until the first set it reports no children,
/// which the apiserver fails on a `children` check: nothing observed is
/// never ok.
///
/// Holds the store WEAKLY: health must never be what keeps the store alive
/// past shutdown.
#[derive(Debug)]
pub struct Health {
    /// Swapped whole, never edited: a reader holds one consistent set.
    rows: ArcSwap<Vec<Row>>,
    windows: Windows,
    draining: AtomicBool,
    store: Weak<StoreMesh>,
    /// The last revision a scrape read, reported if the store is gone.
    last_revision: AtomicU64,
    panics: PanicCounter,
}

impl Health {
    pub(crate) fn new(store: &Arc<StoreMesh>, windows: Windows, panics: PanicCounter) -> Self {
        Self {
            rows: ArcSwap::from_pointee(Vec::new()),
            windows,
            draining: AtomicBool::new(false),
            store: Arc::downgrade(store),
            last_revision: AtomicU64::new(0),
            panics,
        }
    }

    /// Take the spawned children's rows, replacing the set held so far.
    pub(crate) fn publish(&self, rows: Vec<Row>) {
        self.rows.store(Arc::new(rows));
    }

    /// The node has begun to stop: `/readyz` fails its `shutdown` check from
    /// here on.
    pub(crate) fn begin_drain(&self) {
        self.draining.store(true, Ordering::Release);
    }

    fn rows(&self) -> Arc<Vec<Row>> {
        self.rows.load_full()
    }

    /// The windows a tick loop is judged against.
    #[must_use]
    pub fn windows(&self) -> Windows {
        self.windows
    }

    /// Every spawned child, judged now, in catalog order. Empty until the
    /// children are spawned.
    #[must_use]
    pub fn liveness(&self) -> Vec<(Child, Liveness)> {
        let now = Instant::now();
        let wall_now = WallClock.now();
        self.rows()
            .iter()
            .map(|row| (row.child, row.liveness(self.windows, now, wall_now)))
            .collect()
    }

    /// One child's liveness now; `None` if it was never spawned.
    #[must_use]
    pub fn liveness_of(&self, child: Child) -> Option<Liveness> {
        self.liveness()
            .into_iter()
            .find_map(|(c, liveness)| (c == child).then_some(liveness))
    }

    /// Every driver's propose rate now, in catalog order.
    #[must_use]
    pub fn propose_rates(&self) -> Vec<(Driver, ProposeRate)> {
        let now = Instant::now();
        self.rows()
            .iter()
            .filter_map(|row| match (row.child, &row.tally) {
                (Child::Driver(driver), Some(tally)) => Some((driver, tally.propose_rate(now))),
                (Child::Driver(_) | Child::Listener(_) | Child::NodeLease, _) => None,
            })
            .collect()
    }

    /// The store's current revision, or the last one read if it is gone.
    async fn store_revision(&self) -> Revision {
        match self.store.upgrade() {
            Some(store) => {
                let revision = store.current_revision().await;
                self.last_revision.store(revision.get(), Ordering::Relaxed);
                revision
            }
            None => Revision(self.last_revision.load(Ordering::Relaxed)),
        }
    }
}

#[async_trait::async_trait]
impl LivenessSource for Health {
    fn children(&self) -> Vec<ChildLiveness> {
        self.liveness()
            .into_iter()
            .map(|(child, liveness)| ChildLiveness {
                name: child.name(),
                liveness,
            })
            .collect()
    }

    fn drain_state(&self) -> DrainState {
        if self.draining.load(Ordering::Acquire) {
            DrainState::Draining
        } else {
            DrainState::Serving
        }
    }

    /// Linearizable because the runtime's store is a single voter: this
    /// node, while leader, is the whole quorum, and a write is acknowledged
    /// only after it is applied here, so a local read begun after any ack
    /// observes it. Not leader means the group has failed, and says so.
    async fn linearizable_read(&self) -> Result<Revision, StoreError> {
        let Some(store) = self.store.upgrade() else {
            return Err(StoreError::Fatal(String::from(
                "the store has shut down; there is nothing to read",
            )));
        };
        if !store.is_leader().await {
            return Err(StoreError::Fatal(String::from(
                "this single-voter store has no leader, so no read of it is linearizable",
            )));
        }
        Ok(store.current_revision().await)
    }
}

#[async_trait::async_trait]
impl MetricsSource for Health {
    async fn snapshot(&self) -> MetricsSnapshot {
        let store_revision = self.store_revision().await;
        let now = Instant::now();
        let wall_now = WallClock.now();
        let mut reconciles = Vec::new();
        let mut last_ticks = Vec::new();
        for row in self.rows().iter() {
            let Some(tally) = &row.tally else {
                continue;
            };
            let beat = row.beat.snapshot();
            reconciles.extend(tally.reconciles(beat.panics));
            if let Some(at) = beat.last_end {
                last_ticks.push(LastTick {
                    controller: row.child.name(),
                    at: wall_of(at, now, wall_now),
                });
            }
        }
        MetricsSnapshot {
            // Counting objects per resource is a keyspace walk; a scrape
            // must not be one. Empty omits the family rather than charting
            // zeros. `pending-metrics: store-maintained per-resource counts`.
            object_counts: Vec::new(),
            store_revision,
            panics_total: Some(self.panics.total()),
            reconciles,
            last_ticks,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::future::Future;

    use engenho_apiserver::health::{CHILDREN_CHECK, Endpoint, gather};
    use engenho_controllers::{ReconcileReport, ReconcileResult as Requeue, TickClass};

    use super::*;
    use crate::boot_config::BootConfig;
    use crate::child::{ChildTask, Children, Listener};

    const FALLBACK_S: u64 = 30;
    /// The scheduler's fallback, distinct from every other loop's.
    const SCHEDULER_FALLBACK_S: u64 = 7;

    fn windows() -> Windows {
        Windows::new(
            secs(FALLBACK_S),
            Duration::from_millis(50),
            secs(SCHEDULER_FALLBACK_S),
        )
    }

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    /// A heartbeat snapshot: `started` ticks begun, `finished` ended, the
    /// last begun `start_ago` and the last ended `end_ago` before `now`.
    fn beat(
        now: Instant,
        started: u64,
        finished: u64,
        start_ago: Option<Duration>,
        end_ago: Option<Duration>,
    ) -> Beat {
        Beat {
            ticks_started: started,
            ticks_finished: finished,
            last_start: start_ago.map(|ago| now - ago),
            last_end: end_ago.map(|ago| now - ago),
            last_class: end_ago.map(|_| TickClass::Done),
            panics: 0,
        }
    }

    fn never(now: Instant) -> Beat {
        beat(now, 0, 0, None, None)
    }

    fn idle(now: Instant, ended_ago: Duration) -> Beat {
        beat(now, 1, 1, Some(ended_ago + secs(1)), Some(ended_ago))
    }

    fn in_flight(now: Instant, began_ago: Duration) -> Beat {
        beat(now, 2, 1, Some(began_ago), Some(began_ago + secs(1)))
    }

    const WALL: WallInstant = WallInstant {
        physical_ms: 1_758_000_000_000,
        logical: 0,
    };

    fn wall_ago(ago: Duration) -> WallInstant {
        WallInstant::from_ms(WALL.physical_ms - u64::try_from(ago.as_millis()).unwrap())
    }

    fn judge(pulse: Pulse, task: TaskState, beat: &Beat, now: Instant) -> Liveness {
        pulse.judge(task, beat, now, WALL)
    }

    // ── the judge ────────────────────────────────────────────────────

    #[test]
    fn the_idle_window_is_twice_the_fallback_plus_a_resubscribe_and_the_debounce() {
        let w = windows();
        assert_eq!(w.stuck_tick_after(), STUCK_TICK_AFTER);
        assert_eq!(
            w.idle_after(),
            secs(2 * FALLBACK_S) + RESUBSCRIBE.cap() + Duration::from_millis(50)
        );
        assert!(
            w.idle_after() > secs(FALLBACK_S),
            "an idle driver ticks once per fallback: a window under it would read every idle driver as stalled"
        );
    }

    #[test]
    fn a_tick_loop_that_has_never_beaten_is_unknown_not_alive() {
        let now = Instant::now();
        let pulse = Pulse::Ticks(windows());
        assert_eq!(
            judge(pulse, TaskState::Running, &never(now), now),
            Liveness::Unknown
        );
    }

    #[test]
    fn an_idle_tick_loop_is_alive_inside_its_window_and_stalled_since_its_last_tick_past_it() {
        let now = Instant::now();
        let w = windows();
        let pulse = Pulse::Ticks(w);
        assert_eq!(
            judge(pulse, TaskState::Running, &idle(now, secs(FALLBACK_S)), now),
            Liveness::Alive,
            "a driver waiting out its fallback is alive"
        );
        let quiet = w.idle_after() + secs(2);
        assert_eq!(
            judge(pulse, TaskState::Running, &idle(now, quiet), now),
            Liveness::Stalled {
                since: wall_ago(quiet)
            },
            "a driver that has not begun a tick for longer than the idle window has stalled, since its last tick ended"
        );
    }

    #[test]
    fn a_tick_in_flight_past_the_stuck_window_is_stalled_since_it_began() {
        let now = Instant::now();
        let w = windows();
        let pulse = Pulse::Ticks(w);
        assert_eq!(
            judge(pulse, TaskState::Running, &in_flight(now, secs(60)), now),
            Liveness::Alive,
            "a long tick inside the stuck window is progress"
        );
        let stuck = w.stuck_tick_after() + secs(2);
        assert_eq!(
            judge(pulse, TaskState::Running, &in_flight(now, stuck), now),
            Liveness::Stalled {
                since: wall_ago(stuck)
            }
        );
    }

    #[test]
    fn an_ended_task_is_dead_whatever_its_heartbeat_says() {
        let now = Instant::now();
        for pulse in [Pulse::Ticks(windows()), Pulse::Serves] {
            for b in [never(now), idle(now, secs(1)), in_flight(now, secs(1))] {
                assert_eq!(
                    judge(pulse, TaskState::Ended, &b, now),
                    Liveness::Dead,
                    "{pulse:?} {b:?}"
                );
            }
        }
    }

    #[test]
    fn a_listener_is_alive_while_it_serves_and_stalled_from_the_moment_it_halts() {
        let now = Instant::now();
        assert_eq!(
            judge(Pulse::Serves, TaskState::Running, &never(now), now),
            Liveness::Unknown
        );
        assert_eq!(
            judge(
                Pulse::Serves,
                TaskState::Running,
                &beat(now, 1, 0, Some(secs(86_400)), None),
                now
            ),
            Liveness::Alive,
            "a listener that has served for a day is alive: serving has no age"
        );
        let halted = beat(now, 1, 1, Some(secs(10)), Some(secs(3)));
        assert_eq!(
            judge(Pulse::Serves, TaskState::Running, &halted, now),
            Liveness::Stalled {
                since: wall_ago(secs(3))
            },
            "a listener waiting to rebind has a closed port"
        );
    }

    #[test]
    fn every_child_shape_says_how_it_beats() {
        let w = windows();
        for child in Child::all() {
            let pulse = Pulse::of(child, w);
            match child {
                Child::Listener(_) => assert_eq!(pulse, Pulse::Serves, "{child}"),
                Child::Driver(Driver::Scheduler) => assert_eq!(
                    pulse,
                    Pulse::Ticks(w.with_fallback(secs(SCHEDULER_FALLBACK_S))),
                    "{child}"
                ),
                Child::Driver(_) => assert_eq!(pulse, Pulse::Ticks(w), "{child}"),
                Child::NodeLease => assert_eq!(pulse, Pulse::Ticks(w.node_lease()), "{child}"),
            }
        }
    }

    /// T5.8: the scheduler is judged on the fallback it is driven on,
    /// `scheduler.tick_interval_seconds`, not the controllers'. Judged on
    /// the controllers' 30s, a scheduler falling back every 7s would read
    /// alive for 46s after it can no longer be idle; judged on a shorter
    /// one, a long configured tick would read stalled while idle.
    #[test]
    fn the_scheduler_is_judged_on_its_own_fallback() {
        let w = windows();
        let now = Instant::now();
        // Idle 50s: past the scheduler's idle window (2 x 7s + the 30s
        // re-subscribe cap + debounce), inside the controllers' (2 x 30s + ...).
        let idle = beat(now, 3, 3, Some(secs(51)), Some(secs(50)));
        assert!(
            w.of_child(Child::Driver(Driver::Scheduler)).idle_after() < secs(49),
            "precondition: 50s idle is past the scheduler's window"
        );
        assert!(
            w.idle_after() > secs(50),
            "precondition: 50s idle is inside every other driver's window"
        );
        assert!(
            matches!(
                judge(
                    Pulse::of(Child::Driver(Driver::Scheduler), w),
                    TaskState::Running,
                    &idle,
                    now
                ),
                Liveness::Stalled { .. }
            ),
            "a scheduler idle past its own window is stalled"
        );
        assert_eq!(
            judge(
                Pulse::of(Child::Driver(Driver::Gc), w),
                TaskState::Running,
                &idle,
                now
            ),
            Liveness::Alive,
            "the same beat is inside a 30s loop's window"
        );
    }

    // ── the source, over real children ───────────────────────────────

    async fn store() -> Arc<StoreMesh> {
        crate::testing::single_voter_store("health-test").await
    }

    /// Spawn exactly `bodies` through the catalog (every other child is
    /// left unspawned) and hand the rows to a fresh [`Health`].
    fn spawned(store: &Arc<StoreMesh>, mut bodies: Vec<(Child, ChildTask)>) -> (Children, Health) {
        let children = Children::spawn_catalog(&BootConfig::prescribed(), |child, _| {
            let at = bodies.iter().position(|(c, _)| *c == child)?;
            Some(bodies.remove(at).1)
        });
        assert!(
            bodies.is_empty(),
            "every body is a child the config enables"
        );
        let health = Health::new(store, windows(), PanicCounter::install());
        health.publish(children.rows());
        (children, health)
    }

    fn body(run: impl Future<Output = Infallible> + Send + 'static) -> (Arc<Heartbeat>, ChildTask) {
        let beat = Arc::new(Heartbeat::new());
        (beat.clone(), ChildTask::new(beat, run))
    }

    /// A tick loop that ticks every second forever.
    fn ticking() -> ChildTask {
        let beat = Arc::new(Heartbeat::new());
        ChildTask::new(beat.clone(), tick_every_second(beat))
    }

    async fn tick_every_second(beat: Arc<Heartbeat>) -> Infallible {
        loop {
            beat.begin();
            beat.end(TickClass::Done);
            tokio::time::sleep(secs(1)).await;
        }
    }

    /// A tick loop that ticks once and then never again.
    fn ticks_once_then_hangs() -> ChildTask {
        let beat = Arc::new(Heartbeat::new());
        let run = {
            let beat = beat.clone();
            async move {
                beat.begin();
                beat.end(TickClass::Done);
                std::future::pending().await
            }
        };
        ChildTask::new(beat, run)
    }

    async fn advance(by: Duration) {
        tokio::time::advance(by).await;
        // Let every woken task run.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }

    fn named(health: &Health) -> Vec<(&'static str, Liveness)> {
        health
            .children()
            .into_iter()
            .map(|c| (c.name, c.liveness))
            .collect()
    }

    /// The ryn failure (2026-09-18): one controller stopped ticking while
    /// the others kept going. The row says which one, and since when.
    #[tokio::test(start_paused = true)]
    async fn a_child_whose_heartbeat_goes_stale_is_reported_by_name() {
        let store = store().await;
        let (_children, health) = spawned(
            &store,
            vec![
                (Child::Driver(Driver::Gc), ticks_once_then_hangs()),
                (Child::Driver(Driver::Kubelet), ticking()),
            ],
        );
        advance(secs(2)).await;
        assert_eq!(
            named(&health),
            [("gc", Liveness::Alive), ("kubelet", Liveness::Alive)]
        );

        advance(health.windows().idle_after() + secs(2)).await;

        let rows = named(&health);
        assert_eq!(rows[1], ("kubelet", Liveness::Alive), "{rows:?}");
        assert_eq!(rows[0].0, "gc", "{rows:?}");
        assert!(
            matches!(rows[0].1, Liveness::Stalled { .. }),
            "the child that stopped ticking is reported stalled, by name: {rows:?}"
        );

        let report = gather(Endpoint::Livez, Some(&health), secs(1)).await;
        let failed: Vec<&str> = report
            .checks()
            .iter()
            .filter(|c| c.verdict.is_err())
            .map(|c| c.name)
            .collect();
        assert_eq!(
            failed,
            ["gc"],
            "/livez fails on the stalled child, and only on it"
        );
    }

    /// A child whose task ended reads Dead from its task handle, before the
    /// supervisor has polled `next_dead`.
    #[tokio::test(start_paused = true)]
    async fn a_dead_child_is_dead_before_the_supervisor_has_looked() {
        let store = store().await;
        let (_, doomed) = body(async { panic!("the tick blew up") });
        let (_children, health) = spawned(
            &store,
            vec![
                (Child::Driver(Driver::Job), doomed),
                (Child::Listener(Listener::KubeletHttp), serving()),
            ],
        );
        advance(secs(1)).await;
        assert_eq!(
            health.liveness_of(Child::Driver(Driver::Job)),
            Some(Liveness::Dead)
        );
        assert_eq!(
            health.liveness_of(Child::Listener(Listener::KubeletHttp)),
            Some(Liveness::Alive)
        );
        assert_eq!(health.liveness_of(Child::Driver(Driver::Gc)), None);
    }

    fn serving() -> ChildTask {
        let beat = Arc::new(Heartbeat::new());
        let run = {
            let beat = beat.clone();
            async move {
                beat.begin();
                std::future::pending().await
            }
        };
        ChildTask::new(beat, run)
    }

    /// Before the children are spawned there is nothing to report, and the
    /// apiserver fails that on its `children` check rather than passing.
    #[tokio::test]
    async fn before_the_children_are_adopted_every_health_endpoint_fails() {
        let store = store().await;
        let health = Health::new(&store, windows(), PanicCounter::install());
        assert!(health.children().is_empty());
        for endpoint in [Endpoint::Livez, Endpoint::Readyz] {
            let report = gather(endpoint, Some(&health), secs(1)).await;
            assert!(!report.passed(), "{endpoint:?}");
            assert!(
                report
                    .checks()
                    .iter()
                    .any(|c| c.name == CHILDREN_CHECK && c.verdict.is_err()),
                "{endpoint:?}: {:?}",
                report.checks()
            );
        }
    }

    #[tokio::test]
    async fn readiness_reads_the_store_and_fails_once_the_node_drains() {
        let store = store().await;
        let health = Health::new(&store, windows(), PanicCounter::install());
        assert_eq!(
            health.linearizable_read().await.unwrap(),
            store.current_revision().await
        );
        assert_eq!(health.drain_state(), DrainState::Serving);
        health.begin_drain();
        assert_eq!(health.drain_state(), DrainState::Draining);
    }

    #[tokio::test]
    async fn the_scrape_reads_the_stores_revision_not_a_literal() {
        let store = store().await;
        store
            .propose(engenho_store::command::ResourceCommand::put(
                engenho_store::ResourceKey::namespaced("", "v1", "ConfigMap", "default", "a"),
                serde_json::json!({"data": {"k": "v"}}),
                engenho_store::command::Reason::Controller,
            ))
            .await
            .unwrap();
        let health = Health::new(&store, windows(), PanicCounter::install());
        let snapshot = health.snapshot().await;
        assert_eq!(snapshot.store_revision, store.current_revision().await);
        assert_ne!(snapshot.store_revision, Revision(0));
        assert!(snapshot.panics_total.is_some());
    }

    // ── the tally and the propose-rate detector ──────────────────────

    #[test]
    fn a_write_in_every_second_of_a_minute_is_continuous() {
        let mut window = ProposeWindow::default();
        for second in 0..60 {
            window.record(second, true);
        }
        assert_eq!(
            window.judge(59),
            ProposeRate::Continuous { busy_seconds: 60 }
        );
    }

    #[test]
    fn a_burst_is_not_continuous() {
        let mut window = ProposeWindow::default();
        for second in 0..10 {
            window.record(second, true);
        }
        for second in 10..60 {
            window.record(second, false);
        }
        assert_eq!(window.busy_seconds(59), 10);
        assert_eq!(window.judge(59), ProposeRate::Below);
    }

    #[test]
    fn a_write_every_few_seconds_is_not_continuous() {
        let mut window = ProposeWindow::default();
        for second in (0..60).step_by(3) {
            window.record(second, true);
        }
        assert_eq!(window.judge(59), ProposeRate::Below);
    }

    #[test]
    fn continuous_ages_out_with_no_tick_at_all() {
        let mut window = ProposeWindow::default();
        for second in 0..60 {
            window.record(second, true);
        }
        assert!(matches!(window.judge(59), ProposeRate::Continuous { .. }));
        assert_eq!(
            window.judge(59 + u64::from(SPAN)),
            ProposeRate::Below,
            "a controller that stopped ticking has stopped proposing"
        );
        assert_eq!(window.busy_seconds(10_000), 0);
    }

    /// How every tick of a [`Scripted`] controller ends.
    #[derive(Debug, Clone, Copy)]
    enum Ends {
        Done,
        Requeued,
        Failed,
    }

    /// A controller whose every tick lands `changed` writes and ends as
    /// `ends` says.
    struct Scripted {
        changed: usize,
        ends: Ends,
    }

    #[async_trait::async_trait]
    impl Controller for Scripted {
        fn name(&self) -> &'static str {
            "scripted"
        }

        async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
            // Counted the way every count is (T1.8), not written.
            let mut report = ReconcileReport::default();
            for _ in 0..self.changed {
                report.record(engenho_controllers::Effect::answered(true));
            }
            match self.ends {
                Ends::Done => Ok(ReconcileOutcome::new(report, Requeue::Done)),
                Ends::Requeued => Ok(ReconcileOutcome::new(report, Requeue::Requeue(secs(1)))),
                Ends::Failed => Err(ControllerError::InvalidResource("bad".into())),
            }
        }
    }

    fn counts(tally: &Tally, panics: u64) -> Vec<(ReconcileResult, u64)> {
        tally
            .reconciles(panics)
            .iter()
            .map(|c| (c.result, c.count))
            .collect()
    }

    #[tokio::test]
    async fn every_returned_tick_is_counted_by_how_it_ended_and_a_panic_is_an_error() {
        let tally = Arc::new(Tally::new("scripted"));
        for ends in [Ends::Done, Ends::Done, Ends::Requeued, Ends::Failed] {
            let controller = Tallied::new(Scripted { changed: 0, ends }, tally.clone());
            let _ = controller.tick().await;
        }
        assert_eq!(
            counts(&tally, 0),
            [
                (ReconcileResult::Success, 2),
                (ReconcileResult::Error, 1),
                (ReconcileResult::RequeueAfter, 1),
            ]
        );
        assert_eq!(
            counts(&tally, 2)[1],
            (ReconcileResult::Error, 3),
            "a panicked tick never returns to the tally; the heartbeat's count is added"
        );
    }

    /// The gc churn class: a controller whose every tick lands a write,
    /// ticking faster than once a second, for a minute.
    #[tokio::test(start_paused = true)]
    async fn a_controller_landing_a_write_every_tick_is_flagged_and_one_writing_nothing_is_not() {
        let churn = Arc::new(Tally::new("churn"));
        let still = Arc::new(Tally::new("still"));
        let churning = Tallied::new(
            Scripted {
                changed: 1,
                ends: Ends::Done,
            },
            churn.clone(),
        );
        let settled = Tallied::new(
            Scripted {
                changed: 0,
                ends: Ends::Done,
            },
            still.clone(),
        );
        for _ in 0..(10 * SPAN) {
            let _ = churning.tick().await;
            let _ = settled.tick().await;
            tokio::time::advance(Duration::from_millis(100)).await;
        }
        let now = Instant::now();
        assert!(
            matches!(churn.propose_rate(now), ProposeRate::Continuous { busy_seconds } if busy_seconds >= CONTINUOUS_AFTER),
            "{}",
            churn.propose_rate(now)
        );
        assert_eq!(still.propose_rate(now), ProposeRate::Below);
    }

    #[test]
    fn the_adapter_forwards_its_controllers_name_and_type() {
        let tallied = Tallied::new(
            Scripted {
                changed: 0,
                ends: Ends::Done,
            },
            Arc::new(Tally::new("scripted")),
        );
        assert_eq!(tallied.name(), "scripted");
        assert_eq!(tallied.controller_type(), ControllerType::of::<Scripted>());
        assert_ne!(
            tallied.controller_type(),
            ControllerType::of::<Tallied<Scripted>>()
        );
    }
}
