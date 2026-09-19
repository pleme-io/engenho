//! The node lease as its own child, renewed only while the kubelet is alive
//! (T1.3c, I9).
//!
//! ## What this replaces
//!
//! The kubelet renewed its node's Lease at the top of its own tick. The
//! Lease then said "a kubelet tick began in the last 40 s", which is wrong
//! in both directions:
//!
//! * a tick that runs long (an image pull) renews nothing until it ends, so
//!   the node read `NotReady` while its kubelet was working;
//! * the renewal was one write inside the loop it vouched for, so nothing
//!   judged that loop before vouching for it.
//!
//! ## What it is now
//!
//! [`Child::NodeLease`] is a tick loop of its own, driven like every driver
//! (a `WatchDriver`, woken by no store event, falling back every
//! [`RENEW_INTERVAL`]). Each tick reads the kubelet's liveness row, the same
//! [`Row::liveness`] over the same heartbeat, task handle and windows that
//! `/livez` renders, and renews the Lease only when that row is
//! [`Liveness::Alive`]:
//!
//! | the kubelet's row | the lease |
//! |---|---|
//! | alive: idle inside its idle window, or a tick in flight inside the stuck window | renewed, next in one [`RENEW_INTERVAL`] |
//! | never beaten (booting) | withheld, looked at again in [`RECHECK`] |
//! | stalled: a tick in flight past the stuck window, or idle past the idle window | withheld, so the node reads `NotReady` one [`GRACE_PERIOD`] after the last renewal |
//! | dead: its task ended | withheld from the next check |
//!
//! `renewTime` is a `MicroTime`, as upstream writes it: two renewals are
//! never the same bytes, so the store never answers one `Unchanged`.
//!
//! This deviates from upstream on purpose. Upstream's lease controller
//! renews whenever the kubelet process runs, and reports the PLEG health
//! check on the Node's Ready condition. engenho's Lease is the only input to
//! its readiness projection (the apiserver's read of a Node, the scheduler's
//! `NodeReady` filter), so it must carry progress, not just presence.
//!
//! ## Tier
//!
//! * `/livez` and the node's readiness cannot disagree about whether the
//!   kubelet is alive: both are [`Row::liveness`] over one row. Structural,
//!   within this crate.
//! * The lease reads the kubelet's row, so the kubelet must be spawned
//!   before it: the catalog walks it last, and a test holds that order.
//! * `pending-node-lease: kubelet-renewal`. The kubelet still renews the
//!   same Lease at the top of its tick (engenho-kubelet `renew_node_lease`).
//!   That write cannot keep a wedged or dead kubelet's node Ready (it is
//!   made only when a tick begins), but it is a second writer; deleting it
//!   is the kubelet's half of T1.3c.
//! * Not yet an input: the container runtime's own health (W8, the
//!   `RuntimeHealth` relist). A kubelet whose ticks finish against a dead runtime still
//!   renews.
//! * A dead engenho process renews nothing and publishes nothing; saying so
//!   takes a peer reading the lease (`pending-node-lifecycle-controller`).
//!
//! [`GRACE_PERIOD`]: engenho_controllers::node_lease::GRACE_PERIOD
//! [`RENEW_INTERVAL`]: engenho_controllers::node_lease::RENEW_INTERVAL

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use engenho_controllers::node_lease::{GRACE_PERIOD, lease_key, lease_value};
use engenho_controllers::{
    Controller, ControllerError, DeclaresReads, Effect, Reads, ReconcileOutcome, ReconcileReport,
    ReconcileResult,
};
use engenho_store::StoreMesh;
use engenho_store::command::{Reason, ResourceCommand};
use engenho_substrate::{Clock as _, Liveness, WallClock};
use tokio::time::Instant;
use tracing::{info, warn};

use crate::child::Child;
use crate::health::{Row, Windows};

/// How soon the lease looks again after a check that renewed nothing.
///
/// A check is an atomic read of the kubelet's heartbeat, so looking often
/// costs nothing, and a node whose kubelet finishes its first tick, or
/// recovers from a stall, is renewed within a second rather than within a
/// [`RENEW_INTERVAL`](engenho_controllers::node_lease::RENEW_INTERVAL).
pub(crate) const RECHECK: Duration = Duration::from_secs(1);

/// The node-lease controller: one check per tick, renewing this node's
/// Lease only while the kubelet's row is alive.
pub(crate) struct NodeLease {
    store: Arc<StoreMesh>,
    node: String,
    /// The kubelet's row, as health holds it.
    kubelet: Row,
    /// The windows the kubelet is judged against: the runtime's, the ones
    /// `/livez` judges it against.
    windows: Windows,
    /// The kubelet's liveness at the last check, so a change is logged once
    /// rather than on every check. Log state only: no decision reads it.
    last: Mutex<Option<Liveness>>,
}

impl NodeLease {
    /// The lease for `node`, renewed while `kubelet` is alive as `windows`
    /// judge it.
    pub(crate) fn new(
        store: Arc<StoreMesh>,
        node: impl Into<String>,
        kubelet: Row,
        windows: Windows,
    ) -> Self {
        Self {
            store,
            node: node.into(),
            kubelet,
            windows,
            last: Mutex::new(None),
        }
    }

    /// Log the kubelet's liveness when it differs in kind from the last
    /// check's.
    fn note(&self, kubelet: Liveness) {
        let changed = {
            let mut last = self.last.lock().unwrap_or_else(PoisonError::into_inner);
            let changed = last.is_none_or(|was| was.as_str() != kubelet.as_str());
            *last = Some(kubelet);
            changed
        };
        if !changed {
            return;
        }
        let node = self.node.as_str();
        match kubelet {
            Liveness::Alive => info!(node, "node lease renewing: the kubelet is alive"),
            Liveness::Unknown => info!(
                node,
                "node lease withheld until the kubelet begins its first tick"
            ),
            Liveness::Stalled { .. } | Liveness::Dead => warn!(
                node,
                %kubelet,
                grace_s = GRACE_PERIOD.as_secs(),
                "node lease withheld: the kubelet is not alive, so the node reads NotReady once \
                 its lease is older than the grace period"
            ),
        }
    }
}

#[async_trait::async_trait]
impl Controller for NodeLease {
    fn name(&self) -> &'static str {
        Child::NodeLease.name()
    }

    /// Judge the kubelet's row; renew the Lease if it is alive, otherwise
    /// write nothing and ask to be ticked again in [`RECHECK`].
    ///
    /// The write is unconditional: a heartbeat is last-writer-wins, and a
    /// lost compare-and-swap would read as a dead node.
    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let kubelet = self
            .kubelet
            .liveness(self.windows, Instant::now(), WallClock.now());
        self.note(kubelet);
        let mut report = ReconcileReport {
            objects_examined: 1,
            ..ReconcileReport::default()
        };
        if !kubelet.is_alive() {
            return Ok(ReconcileOutcome::new(
                report,
                ReconcileResult::Requeue(RECHECK),
            ));
        }
        let applied = self
            .store
            .propose(ResourceCommand::put(
                lease_key(&self.node),
                lease_value(&self.node, &engenho_types::time::now_micro_time_utc(), 0),
                Reason::Controller,
            ))
            .await?;
        report.record(Effect::of(applied.op));
        Ok(ReconcileOutcome::new(report, ReconcileResult::Done))
    }
}

/// It reads nothing from the store: the kubelet's heartbeat is its input,
/// and its fallback is its renew interval.
impl DeclaresReads for NodeLease {
    fn reads(&self) -> Reads {
        Reads::nothing()
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use engenho_config::EngenhoConfig;
    use engenho_controllers::node_lease::{NodeReadiness, RENEW_INTERVAL, readiness};
    use engenho_controllers::{Heartbeat, TickClass};
    use serde_json::Value;
    use shikumi::TieredConfig as _;
    use tokio::task::JoinHandle;

    use super::*;
    use crate::child::{ChildTask, Children, Driver};
    use crate::runtime::drive_node_lease;
    use crate::testing::single_voter_store;

    const NODE: &str = "node-A";

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn windows() -> Windows {
        Windows::of(&EngenhoConfig::prescribed_default().controllers)
    }

    /// A kubelet as the lease sees it: a heartbeat the test drives, and a
    /// task whose end the test decides.
    struct Kubelet {
        beat: Arc<Heartbeat>,
        task: JoinHandle<()>,
    }

    impl Kubelet {
        fn spawn() -> Self {
            Self {
                beat: Arc::new(Heartbeat::new()),
                task: tokio::spawn(std::future::pending()),
            }
        }

        fn row(&self) -> Row {
            Row::new(
                Child::Driver(Driver::Kubelet),
                self.beat.clone(),
                self.task.abort_handle(),
                None,
            )
        }

        /// One whole tick.
        fn ticks(&self) {
            self.beat.begin();
            self.beat.end(TickClass::Done);
        }
    }

    async fn lease_for(kubelet: &Kubelet) -> (Arc<StoreMesh>, NodeLease) {
        let store = single_voter_store("node-lease").await;
        let lease = NodeLease::new(store.clone(), NODE, kubelet.row(), windows());
        (store, lease)
    }

    /// This node's Lease as the store has it.
    async fn stored(store: &StoreMesh) -> Option<Value> {
        store.get(&lease_key(NODE)).await
    }

    /// Its `renewTime`, the field readiness is judged from.
    async fn renew_time(store: &StoreMesh) -> Option<String> {
        stored(store)
            .await?
            .pointer("/spec/renewTime")?
            .as_str()
            .map(str::to_owned)
    }

    async fn check(lease: &NodeLease) -> ReconcileResult {
        lease.tick().await.expect("a check does not fail").result
    }

    #[tokio::test(start_paused = true)]
    async fn a_live_kubelet_gets_its_node_a_fresh_lease_in_micro_time() {
        let kubelet = Kubelet::spawn();
        kubelet.ticks();
        let (store, lease) = lease_for(&kubelet).await;

        assert_eq!(check(&lease).await, ReconcileResult::Done);

        let value = stored(&store).await.expect("the lease was written");
        assert_eq!(
            value.pointer("/spec/holderIdentity"),
            Some(&Value::from(NODE))
        );
        let renewed = renew_time(&store).await.expect("a renewTime");
        let fraction = renewed
            .strip_suffix('Z')
            .and_then(|t| t.rsplit_once('.'))
            .map(|(_, f)| f.len());
        assert_eq!(fraction, Some(6), "renewTime is a MicroTime: {renewed}");
        let age = engenho_types::time::age_since_rfc3339(&renewed);
        assert_eq!(
            readiness(age),
            NodeReadiness::Ready,
            "a node whose kubelet is alive reads Ready: {renewed}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_kubelet_that_has_never_ticked_gets_no_lease_and_is_looked_at_again_soon() {
        let kubelet = Kubelet::spawn();
        let (store, lease) = lease_for(&kubelet).await;

        let result = check(&lease).await;
        assert_eq!(
            stored(&store).await,
            None,
            "no lease before the kubelet's first tick"
        );
        assert_eq!(result, ReconcileResult::Requeue(RECHECK));

        kubelet.beat.begin();
        assert_eq!(
            check(&lease).await,
            ReconcileResult::Done,
            "a kubelet in its first tick is alive"
        );
        assert!(stored(&store).await.is_some());
    }

    /// The image-pull case: a tick in flight inside the stuck window is
    /// progress, and the lease keeps being renewed through it. Renewing at
    /// the top of the kubelet's tick renewed nothing until the tick ended.
    #[tokio::test(start_paused = true)]
    async fn a_long_kubelet_tick_inside_the_stuck_window_keeps_the_lease_fresh() {
        let kubelet = Kubelet::spawn();
        let (store, lease) = lease_for(&kubelet).await;
        kubelet.beat.begin();

        let mut seen = Vec::new();
        let span = windows().stuck_tick_after().saturating_sub(secs(1));
        let mut elapsed = Duration::ZERO;
        while elapsed < span {
            assert_eq!(check(&lease).await, ReconcileResult::Done, "at {elapsed:?}");
            seen.push(renew_time(&store).await.expect("renewed"));
            tokio::time::advance(RENEW_INTERVAL).await;
            elapsed += RENEW_INTERVAL;
        }
        let distinct: std::collections::BTreeSet<&String> = seen.iter().collect();
        assert_eq!(
            distinct.len(),
            seen.len(),
            "every check during the long tick renewed the lease: {seen:?}"
        );
        assert!(
            seen.len() * usize::try_from(RENEW_INTERVAL.as_secs()).unwrap()
                > usize::try_from(GRACE_PERIOD.as_secs()).unwrap(),
            "HARNESS PRECONDITION: the tick outlasted the grace period"
        );
    }

    /// The wedge: a kubelet tick that never ends. Once it is past the stuck
    /// window the lease is not renewed again, however often it is checked,
    /// so the node's readiness goes stale one grace period later.
    #[tokio::test(start_paused = true)]
    async fn a_wedged_kubelet_stops_the_renewals_and_its_node_goes_not_ready() {
        let kubelet = Kubelet::spawn();
        let (store, lease) = lease_for(&kubelet).await;
        kubelet.beat.begin();
        tokio::time::advance(windows().stuck_tick_after().saturating_sub(secs(1))).await;
        assert_eq!(
            check(&lease).await,
            ReconcileResult::Done,
            "a tick in flight inside the stuck window is progress"
        );
        let last = renew_time(&store).await;
        assert!(last.is_some());

        // Past the stuck window, which is judged in whole seconds rounded up.
        let mut since_renewal = secs(3);
        tokio::time::advance(since_renewal).await;
        while since_renewal <= GRACE_PERIOD {
            let result = check(&lease).await;
            assert_eq!(
                renew_time(&store).await,
                last,
                "a wedged kubelet's lease was renewed {since_renewal:?} after its last renewal, \
                 its tick in flight past the stuck window"
            );
            assert_eq!(result, ReconcileResult::Requeue(RECHECK));
            tokio::time::advance(RECHECK).await;
            since_renewal += RECHECK;
        }
        assert_eq!(
            readiness(Some(since_renewal)),
            NodeReadiness::Stale,
            "a lease unrenewed for {since_renewal:?} reads the node NotReady"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_dead_kubelet_stops_the_renewals_at_the_next_check() {
        let kubelet = Kubelet::spawn();
        kubelet.ticks();
        let (store, lease) = lease_for(&kubelet).await;
        assert_eq!(check(&lease).await, ReconcileResult::Done);
        let last = renew_time(&store).await;

        kubelet.task.abort();
        while !kubelet.task.is_finished() {
            tokio::task::yield_now().await;
        }
        kubelet.ticks();

        let result = check(&lease).await;
        assert_eq!(
            renew_time(&store).await,
            last,
            "a dead kubelet's lease was renewed: a fresh heartbeat from a task that has ended \
             is history, not life"
        );
        assert_eq!(result, ReconcileResult::Requeue(RECHECK));
    }

    #[tokio::test(start_paused = true)]
    async fn a_kubelet_that_recovers_gets_its_lease_renewed_again() {
        let kubelet = Kubelet::spawn();
        kubelet.ticks();
        let (store, lease) = lease_for(&kubelet).await;
        tokio::time::advance(windows().idle_after() + secs(1)).await;
        let result = check(&lease).await;
        assert_eq!(
            stored(&store).await,
            None,
            "a kubelet idle past its idle window got a lease"
        );
        assert_eq!(result, ReconcileResult::Requeue(RECHECK));

        kubelet.ticks();
        assert_eq!(check(&lease).await, ReconcileResult::Done);
        assert!(stored(&store).await.is_some());
    }

    // ── the child, driven through the catalog ─────────────────────────

    /// A kubelet child the test scripts: it ticks every second until
    /// `wedge` is set, then begins a tick that never ends.
    fn scripted_kubelet(wedge: Arc<tokio::sync::Notify>) -> (Arc<Heartbeat>, ChildTask) {
        let beat = Arc::new(Heartbeat::new());
        let run = {
            let beat = beat.clone();
            async move {
                loop {
                    beat.begin();
                    tokio::select! {
                        () = wedge.notified() => break,
                        () = tokio::time::sleep(secs(1)) => beat.end(TickClass::Done),
                    }
                }
                std::future::pending::<Infallible>().await
            }
        };
        (beat.clone(), ChildTask::new(beat, run))
    }

    /// Every new renewTime the store held for this node's lease, read every
    /// second of (paused) time for `span`: the renewals made in it.
    async fn renewals_over(store: &StoreMesh, span: Duration) -> Vec<String> {
        let mut last = renew_time(store).await;
        let mut renewals: Vec<String> = Vec::new();
        let mut elapsed = Duration::ZERO;
        while elapsed < span {
            tokio::time::sleep(secs(1)).await;
            elapsed += secs(1);
            let now = renew_time(store).await;
            if now != last {
                renewals.extend(now.clone());
                last = now;
            }
        }
        renewals
    }

    /// The lease child as the runtime builds it: spawned after the kubelet,
    /// from its row, driven on its own fallback. It renews on its interval
    /// while the kubelet ticks, and stops once the kubelet wedges.
    #[tokio::test(start_paused = true)]
    async fn the_lease_child_renews_on_its_interval_until_the_kubelet_wedges() {
        let store = single_voter_store("node-lease-child").await;
        let config = EngenhoConfig::prescribed_default();
        let wedge = Arc::new(tokio::sync::Notify::new());
        let mut kubelet = Some(scripted_kubelet(wedge.clone()).1);
        let children = Children::spawn_catalog(&config, |child, before| match child {
            Child::Driver(Driver::Kubelet) => kubelet.take(),
            Child::NodeLease => before
                .row(Child::Driver(Driver::Kubelet))
                .map(|row| drive_node_lease(&store, NODE, row, windows())),
            Child::Driver(_) | Child::Listener(_) => None,
        });
        assert_eq!(
            children.len(),
            2,
            "HARNESS PRECONDITION: kubelet and lease spawned"
        );

        let ticking = renewals_over(&store, RENEW_INTERVAL * 5).await;
        assert!(
            (3..=6).contains(&ticking.len()),
            "about one renewal per {RENEW_INTERVAL:?} while the kubelet ticks: {ticking:?}"
        );

        wedge.notify_one();
        let span = windows().stuck_tick_after() + GRACE_PERIOD + RENEW_INTERVAL;
        let wedged = renewals_over(&store, span).await;
        let after_stuck =
            usize::try_from(windows().stuck_tick_after().as_secs() / RENEW_INTERVAL.as_secs())
                .unwrap();
        assert!(
            wedged.len() <= after_stuck + 1,
            "renewals kept coming after the kubelet's tick passed the stuck window: \
             {} in {span:?}: {wedged:?}",
            wedged.len()
        );
        let quiet = renewals_over(&store, GRACE_PERIOD + RENEW_INTERVAL).await;
        assert!(
            quiet.is_empty(),
            "a wedged kubelet's lease is renewed no more: {quiet:?}"
        );
        assert!(
            children
                .get(Child::NodeLease)
                .is_some_and(|h| h.beat().snapshot().ticks_finished > 0),
            "the lease child ticked"
        );
    }

    /// The lease is not a second opinion of the kubelet's liveness: it
    /// renews exactly when the row `/livez` renders for the kubelet is
    /// alive.
    #[tokio::test(start_paused = true)]
    async fn the_lease_renews_exactly_when_the_kubelets_liveness_row_is_alive() {
        let kubelet = Kubelet::spawn();
        let (store, lease) = lease_for(&kubelet).await;
        let row = kubelet.row();
        kubelet.ticks();
        let mut verdicts = Vec::new();
        // Idle 0 s, 30 s, 60 s, then 150 s (past the idle window), then a
        // fresh tick.
        for (step, tick_first) in [
            (secs(0), false),
            (secs(30), false),
            (secs(30), false),
            (secs(90), false),
            (secs(0), true),
        ] {
            tokio::time::advance(step).await;
            if tick_first {
                kubelet.ticks();
            }
            let alive = row
                .liveness(windows(), Instant::now(), WallClock.now())
                .is_alive();
            let before = renew_time(&store).await;
            let _ = check(&lease).await;
            let renewed = renew_time(&store).await != before;
            assert_eq!(renewed, alive, "after {step:?} (tick first: {tick_first})");
            verdicts.push(alive);
        }
        assert!(
            verdicts.contains(&true) && verdicts.contains(&false),
            "HARNESS PRECONDITION: the steps cover both verdicts: {verdicts:?}"
        );
    }
}
