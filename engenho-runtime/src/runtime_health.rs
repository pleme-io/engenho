//! The container runtime's health, derived from relists (W8).
//!
//! ## What this closes
//!
//! Upstream's kubelet lists every container its runtime holds once a second
//! (the PLEG's relist, `relistPeriod`) and calls the runtime healthy while
//! the last relist that SUCCEEDED began less than three minutes ago
//! (`relistThreshold`); past that the node reads `NotReady`, "PLEG is not
//! healthy". engenho's kubelet asks its runtime about one container at a
//! time and nothing judges the answers, so the node lease (T1.3c) vouched
//! for a kubelet whose ticks finished against a dead podman socket: the node
//! read Ready while no container on it could be started, stopped or seen.
//!
//! ## What it is
//!
//! * [`Relist`] is the seam: one call that lists every container the
//!   runtime holds, or a typed [`RelistFault`].
//! * [`Relister`] is the loop's tick: one relist, its outcome written into a
//!   [`RelistLedger`]. A failed relist fails the tick, so it is counted as a
//!   reconcile error; the next fallback relists again.
//! * [`RelistLedger::health`] is the one judgement, a [`RuntimeHealth`]. It
//!   is the fleet's freshness judge ([`Freshness::judge`]) over when the last
//!   successful relist BEGAN, against [`RELIST_THRESHOLD`]. Upstream stamps a
//!   relist before it lists, so a slow relist vouches for the moment it
//!   asked, not the moment it heard back; so does this.
//!
//! | relists so far | health |
//! |---|---|
//! | none has succeeded | [`RuntimeHealth::NotYetObserved`], saying why: none has finished, or the last one failed |
//! | the last success began within the threshold | [`RuntimeHealth::Healthy`], even while later relists fail: one failed relist is not a dead runtime |
//! | the last success began longer ago | [`RuntimeHealth::Unhealthy`], saying why: the last relist to finish failed, or none has finished since (a hung relist) |
//!
//! A relist still in flight is evidence of nothing. A hung one leaves the
//! last success to age out, as upstream's does.
//!
//! ## Who reads it
//!
//! The node lease, through a [`RuntimeHealthSource`]. Over a ledger
//! ([`RuntimeHealthSource::Relisted`]) it renews only while the kubelet is
//! alive AND the runtime is [`RuntimeHealth::Healthy`], so a node whose
//! runtime stops answering reads `NotReady` one grace period after the
//! threshold, however healthy its kubelet's ticks look.
//!
//! ## Tier
//!
//! * One judgement: the lease reads [`RuntimeSight::permits_renewal`] off the
//!   ledger the relister writes, and no second threshold exists. Structural.
//! * A runtime no relist has answered never reads healthy to the lease. The
//!   lease reads only a ledger's judgement ([`RuntimeHealthSource::sight`]),
//!   a ledger judges `Healthy` only from a recorded success, and only a
//!   [`Relister`] records one: the ledger's writer is private to this
//!   module. Structural, within this crate. [`RuntimeHealth`]'s variants are
//!   public data, so a value built by hand is possible; nothing takes one as
//!   an input.
//! * `pending-runtime-relist`: NOTHING RELISTS IN PRODUCTION YET. The
//!   kubelet's `ContainerRuntime` (engenho-kubelet) has no relist method, so
//!   there is no [`Relist`] to build a relister over. [`Relister`] is the
//!   [`Dormant`](crate::Dormant) row `RuntimeRelist`, and the runtime builds
//!   the lease over [`RuntimeHealthSource::Unobserved`]: it renews by the
//!   kubelet alone, as before W8, and logs that once. Waking it takes a
//!   `ContainerRuntime::relist` in every backend, an `impl Relist` over the
//!   kubelet's backend here, a `Child::RuntimeRelist` tick loop driving
//!   [`Relister`] every [`RELIST_PERIOD`], and the lease built over its
//!   ledger, with the dormant row deleted in the same commit.
//! * Not yet read: a Ready=False condition naming the runtime
//!   (`NodeReadiness` in engenho-controllers renders Ready, Stale and
//!   Unknown only) and a `pleg` check on the kubelet's `/healthz`.

use std::fmt;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use engenho_controllers::{
    Controller, ControllerError, DeclaresReads, Reads, ReconcileOutcome, ReconcileReport,
    ReconcileResult,
};
use engenho_substrate::{Freshness, Instant as WallInstant, StaleAfter};
use tokio::time::Instant;
use tracing::{info, warn};

use crate::health::wall_of;

/// How often the relister lists the runtime's containers: upstream's
/// `relistPeriod`.
pub const RELIST_PERIOD: Duration = Duration::from_secs(1);

/// How long ago the last successful relist may have begun for the runtime
/// to be healthy: upstream's `relistThreshold`, three minutes.
///
/// Three minutes, then the lease's grace period: a node whose runtime stops
/// answering reads `NotReady` about 220 s later, beside the kubelet's own
/// stuck-tick window plus the grace period (about 160 s).
pub const RELIST_THRESHOLD: StaleAfter = StaleAfter::from_secs(NonZeroU64::MIN.saturating_add(179));

// ── the seam ───────────────────────────────────────────────────────────

/// What one relist saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Relisted {
    containers: usize,
}

impl Relisted {
    /// A relist that listed `containers` containers.
    #[must_use]
    pub const fn new(containers: usize) -> Self {
        Self { containers }
    }

    /// How many containers the runtime listed.
    #[must_use]
    pub const fn containers(self) -> usize {
        self.containers
    }
}

/// A relist the runtime did not answer with a list.
///
/// Carries the runtime's own error, typed, so its message reaches the log
/// and the health reading unrendered until they render it.
#[derive(Debug, Clone, thiserror::Error)]
#[error("the container runtime failed a relist: {cause}")]
pub struct RelistFault {
    cause: Arc<dyn std::error::Error + Send + Sync>,
}

impl RelistFault {
    /// The relist failed because of `cause`.
    #[must_use]
    pub fn new(cause: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self {
            cause: Arc::new(cause),
        }
    }
}

/// The seam: one call that lists every container the runtime holds.
///
/// `pending-runtime-relist`: the implementation belongs over the kubelet's
/// `ContainerRuntime`, once that trait can relist.
#[async_trait::async_trait]
pub trait Relist: Send + Sync {
    /// List every container the runtime holds.
    ///
    /// # Errors
    ///
    /// [`RelistFault`] when the runtime did not answer with a list.
    async fn relist(&self) -> Result<Relisted, RelistFault>;
}

// ── the judgement ──────────────────────────────────────────────────────

/// Why no fresh relist stands behind the runtime.
#[derive(Debug, Clone)]
pub enum Staleness {
    /// The last relist to finish failed, with this fault.
    Failing(RelistFault),
    /// No relist has finished since the last success (or ever): one is in
    /// flight and has not returned, or nothing is relisting.
    Silent,
}

impl fmt::Display for Staleness {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failing(fault) => fault.fmt(f),
            Self::Silent => f.write_str("no relist has finished"),
        }
    }
}

/// The container runtime's health, as its relists observed it.
///
/// The lease reads it only as [`RelistLedger::health`] derives it.
#[derive(Debug, Clone)]
pub enum RuntimeHealth {
    /// No relist has succeeded yet: the runtime has never been seen to
    /// answer. Upstream's "PLEG has yet to be successful".
    NotYetObserved(Staleness),
    /// The last successful relist began within [`RELIST_THRESHOLD`].
    Healthy {
        /// When it began.
        last_relist: WallInstant,
    },
    /// The last successful relist began longer ago than
    /// [`RELIST_THRESHOLD`].
    Unhealthy {
        /// When it began.
        last_relist: WallInstant,
        /// Why nothing fresher stands behind it.
        why: Staleness,
    },
}

impl RuntimeHealth {
    /// Whether the runtime may be vouched for. Only [`RuntimeHealth::Healthy`]
    /// may.
    #[must_use]
    pub const fn is_healthy(&self) -> bool {
        matches!(self, Self::Healthy { .. })
    }

    /// The stable lowercase name, for logs.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NotYetObserved(_) => "not_yet_observed",
            Self::Healthy { .. } => "healthy",
            Self::Unhealthy { .. } => "unhealthy",
        }
    }
}

impl fmt::Display for RuntimeHealth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotYetObserved(why) => write!(f, "not yet observed: {why}"),
            Self::Healthy { last_relist } => {
                write!(
                    f,
                    "healthy: the last successful relist began at {last_relist}"
                )
            }
            Self::Unhealthy { last_relist, why } => write!(
                f,
                "unhealthy: the last successful relist began at {last_relist}, over {}s ago: {why}",
                RELIST_THRESHOLD.get().as_secs()
            ),
        }
    }
}

/// What the relists have observed: when the last one that succeeded began,
/// and how the last one to finish after it failed, if one did.
///
/// Written only by the [`Relister`]; read by whoever judges the runtime.
#[derive(Debug, Default)]
pub struct RelistLedger {
    record: Mutex<Record>,
}

#[derive(Debug, Default)]
struct Record {
    /// When the last successful relist began.
    last_success: Option<Instant>,
    /// The fault of the last relist to finish, when it failed after the
    /// last success.
    last_fault: Option<RelistFault>,
}

impl RelistLedger {
    /// A ledger no relist has written.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn record(&self) -> MutexGuard<'_, Record> {
        self.record.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Write down the outcome of a relist that began at `began`.
    fn write(&self, began: Instant, outcome: &Result<Relisted, RelistFault>) {
        let mut record = self.record();
        match outcome {
            Ok(_) => {
                record.last_success = Some(record.last_success.map_or(began, |at| at.max(began)));
                record.last_fault = None;
            }
            Err(fault) => record.last_fault = Some(fault.clone()),
        }
    }

    /// The runtime's health as of `now` (the relister's monotonic clock).
    /// `wall_now` only writes the last relist's start down as a wall time.
    #[must_use]
    pub fn health(&self, now: Instant, wall_now: WallInstant) -> RuntimeHealth {
        let record = self.record();
        let why = || match &record.last_fault {
            Some(fault) => Staleness::Failing(fault.clone()),
            None => Staleness::Silent,
        };
        let Some(began) = record.last_success else {
            return RuntimeHealth::NotYetObserved(why());
        };
        let last_relist = wall_of(began, now, wall_now);
        match Freshness::judge(Some(last_relist), wall_now, RELIST_THRESHOLD) {
            Freshness::Fresh => RuntimeHealth::Healthy { last_relist },
            Freshness::Stale { since } => RuntimeHealth::Unhealthy {
                last_relist: since,
                why: why(),
            },
            // Not reached: the judge was handed an observation. Were it, the
            // conservative reading is the one that vouches for nothing.
            Freshness::NeverObserved => RuntimeHealth::NotYetObserved(why()),
        }
    }
}

// ── the loop's tick ────────────────────────────────────────────────────

/// The relister: one relist per tick, written into its [`RelistLedger`].
///
/// A [`Controller`] so the runtime can drive it like every tick loop, on its
/// own fallback of [`RELIST_PERIOD`], with a heartbeat `/livez` judges. It
/// reads nothing from the store. `pending-runtime-relist`: nothing drives
/// it yet ([`crate::Dormant`]).
pub struct Relister {
    relist: Arc<dyn Relist>,
    ledger: Arc<RelistLedger>,
    /// Whether the last relist succeeded, so a change is logged once rather
    /// than on every tick. Log state only: no decision reads it.
    answered: Mutex<Option<bool>>,
}

impl Relister {
    /// The relister over `relist`, writing into `ledger`.
    #[must_use]
    pub fn new(relist: Arc<dyn Relist>, ledger: Arc<RelistLedger>) -> Self {
        Self {
            relist,
            ledger,
            answered: Mutex::new(None),
        }
    }

    /// Log the relist's outcome when it differs from the last one's.
    fn note(&self, outcome: &Result<Relisted, RelistFault>) {
        let answered = outcome.is_ok();
        let was = self
            .answered
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(answered);
        if was == Some(answered) {
            return;
        }
        match outcome {
            Ok(seen) => info!(
                containers = seen.containers(),
                "the container runtime answers its relists"
            ),
            Err(fault) => warn!(
                %fault,
                threshold_s = RELIST_THRESHOLD.get().as_secs(),
                "the container runtime failed a relist; it is judged unhealthy once no relist has \
                 succeeded for the threshold"
            ),
        }
    }
}

#[async_trait::async_trait]
impl Controller for Relister {
    fn name(&self) -> &'static str {
        "runtime-relist"
    }

    /// Relist once and write the outcome down, stamped with when the relist
    /// began.
    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let began = Instant::now();
        let outcome = self.relist.relist().await;
        self.ledger.write(began, &outcome);
        self.note(&outcome);
        match outcome {
            Ok(seen) => Ok(ReconcileOutcome::new(
                ReconcileReport {
                    objects_examined: seen.containers(),
                    ..ReconcileReport::default()
                },
                ReconcileResult::Done,
            )),
            Err(fault) => Err(ControllerError::Internal(fault.to_string())),
        }
    }
}

/// It reads nothing from the store: the runtime is its input, and its
/// fallback is its period.
impl DeclaresReads for Relister {
    fn reads(&self) -> Reads {
        Reads::nothing()
    }
}

// ── what the lease reads ───────────────────────────────────────────────

/// Where the node lease reads the container runtime's health from.
#[derive(Debug, Clone)]
pub enum RuntimeHealthSource {
    /// Nothing relists the runtime (`pending-runtime-relist`). The lease
    /// renews by the kubelet alone, as it did before W8.
    Unobserved,
    /// The ledger a [`Relister`] writes.
    Relisted(Arc<RelistLedger>),
}

impl RuntimeHealthSource {
    /// The runtime as of `now` (the monotonic clock the ledger is stamped
    /// with).
    #[must_use]
    pub fn sight(&self, now: Instant, wall_now: WallInstant) -> RuntimeSight {
        match self {
            Self::Unobserved => RuntimeSight::Blind,
            Self::Relisted(ledger) => RuntimeSight::Judged(ledger.health(now, wall_now)),
        }
    }
}

/// The container runtime as the node lease sees it at one check.
#[derive(Debug, Clone)]
pub enum RuntimeSight {
    /// Nothing observes the runtime. A named blind spot, not a verdict of
    /// health.
    Blind,
    /// The relists' judgement.
    Judged(RuntimeHealth),
}

impl RuntimeSight {
    /// Whether the runtime lets the lease renew. Blind, it does not stand in
    /// the way: the kubelet alone decides, as before W8, because withholding
    /// on no observation would read every node `NotReady`. Judged, only while
    /// [`RuntimeHealth::Healthy`].
    #[must_use]
    pub const fn permits_renewal(&self) -> bool {
        match self {
            Self::Blind => true,
            Self::Judged(health) => health.is_healthy(),
        }
    }

    /// The stable lowercase name, for logs.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Blind => "blind",
            Self::Judged(health) => health.as_str(),
        }
    }
}

impl fmt::Display for RuntimeSight {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Blind => f.write_str("not observed: nothing relists the container runtime"),
            Self::Judged(health) => health.fmt(f),
        }
    }
}

#[cfg(test)]
mod tests {
    use engenho_substrate::{Clock as _, WallClock};

    use super::*;
    use crate::testing::{RelistAnswer as Answer, ScriptedRuntime};

    fn relister() -> (Arc<ScriptedRuntime>, Arc<RelistLedger>, Arc<Relister>) {
        let runtime = Arc::new(ScriptedRuntime::default());
        let ledger = Arc::new(RelistLedger::new());
        let relister = Arc::new(Relister::new(runtime.clone(), ledger.clone()));
        (runtime, ledger, relister)
    }

    fn health(ledger: &RelistLedger) -> RuntimeHealth {
        ledger.health(Instant::now(), WallClock.now())
    }

    fn threshold() -> Duration {
        RELIST_THRESHOLD.get()
    }

    /// Relist once every period for `span`, as the loop would.
    async fn relist_for(relister: &Relister, span: Duration) {
        let mut elapsed = Duration::ZERO;
        while elapsed < span {
            let _ = relister.tick().await;
            tokio::time::advance(RELIST_PERIOD).await;
            elapsed += RELIST_PERIOD;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_runtime_no_relist_has_answered_is_not_yet_observed_and_not_healthy() {
        let (_, ledger, _) = relister();
        let seen = health(&ledger);
        assert!(
            matches!(seen, RuntimeHealth::NotYetObserved(Staleness::Silent)),
            "{seen}"
        );
        assert!(
            !seen.is_healthy(),
            "a runtime nobody has relisted is healthy"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_first_relist_that_fails_leaves_the_runtime_not_yet_observed_with_its_fault() {
        let (runtime, ledger, relister) = relister();
        runtime.set(Answer::Fails);

        let tick = relister.tick().await;
        assert!(
            matches!(tick, Err(ControllerError::Internal(_))),
            "a failed relist fails the tick, so it is counted: {tick:?}"
        );
        let seen = health(&ledger);
        assert!(
            matches!(seen, RuntimeHealth::NotYetObserved(Staleness::Failing(_))),
            "{seen}"
        );
        assert!(
            seen.to_string().contains("connection refused"),
            "the runtime's own error reaches the reading: {seen}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_relist_the_runtime_answers_makes_it_healthy() {
        let (_, ledger, relister) = relister();
        let outcome = relister.tick().await.expect("the relist answered");
        assert_eq!(outcome.report.objects_examined, ScriptedRuntime::CONTAINERS);
        assert_eq!(outcome.result, ReconcileResult::Done);
        let seen = health(&ledger);
        assert!(seen.is_healthy(), "{seen}");
    }

    /// Upstream's tolerance: health is the age of the last success, so
    /// failed relists inside the threshold do not make the runtime
    /// unhealthy. One refused connection is not a dead runtime.
    #[tokio::test(start_paused = true)]
    async fn relists_that_fail_inside_the_threshold_leave_the_runtime_healthy() {
        let (runtime, ledger, relister) = relister();
        let _ = relister.tick().await;
        runtime.set(Answer::Fails);

        let mut elapsed = Duration::ZERO;
        while elapsed < threshold() {
            let _ = relister.tick().await;
            let seen = health(&ledger);
            assert!(seen.is_healthy(), "{elapsed:?} into the failures: {seen}");
            tokio::time::advance(RELIST_PERIOD).await;
            elapsed += RELIST_PERIOD;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_runtime_whose_relists_fail_past_the_threshold_is_unhealthy_and_says_why() {
        let (runtime, ledger, relister) = relister();
        let _ = relister.tick().await;
        assert!(
            health(&ledger).is_healthy(),
            "HARNESS PRECONDITION: the first relist answered"
        );
        runtime.set(Answer::Fails);

        relist_for(&relister, threshold() + Duration::from_secs(2)).await;

        let wall_now = WallClock.now();
        let seen = ledger.health(Instant::now(), wall_now);
        let RuntimeHealth::Unhealthy {
            last_relist,
            why: Staleness::Failing(fault),
        } = &seen
        else {
            panic!("three minutes of failed relists read as {seen}");
        };
        let age_ms = wall_now.physical_ms.saturating_sub(last_relist.physical_ms);
        assert!(
            u128::from(age_ms) > threshold().as_millis(),
            "the reading names the last success, not the last attempt, a second ago: {seen}"
        );
        assert!(fault.to_string().contains("connection refused"), "{seen}");
    }

    /// A relist that never returns is not a failure the relister can
    /// record: it is silence, and the last success ages out under it.
    #[tokio::test(start_paused = true)]
    async fn a_hung_relist_leaves_the_last_success_to_age_out_as_silence() {
        let (runtime, ledger, relister) = relister();
        let _ = relister.tick().await;
        runtime.set(Answer::Hangs);
        let hung = tokio::spawn({
            let relister = relister.clone();
            async move { relister.tick().await }
        });

        tokio::time::advance(threshold().saturating_sub(Duration::from_secs(1))).await;
        assert!(
            health(&ledger).is_healthy(),
            "inside the threshold the hung relist is evidence of nothing yet"
        );
        tokio::time::advance(Duration::from_secs(2)).await;
        let seen = health(&ledger);
        assert!(
            matches!(
                seen,
                RuntimeHealth::Unhealthy {
                    why: Staleness::Silent,
                    ..
                }
            ),
            "a relist hung past the threshold reads as {seen}"
        );
        assert!(
            !hung.is_finished(),
            "HARNESS PRECONDITION: the relist hangs"
        );
        hung.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_runtime_that_answers_again_is_healthy_again() {
        let (runtime, ledger, relister) = relister();
        runtime.set(Answer::Fails);
        relist_for(&relister, threshold() + Duration::from_secs(2)).await;
        assert!(
            !health(&ledger).is_healthy(),
            "HARNESS PRECONDITION: the runtime was unhealthy"
        );

        runtime.set(Answer::Lists);
        let _ = relister.tick().await;
        let seen = health(&ledger);
        assert!(seen.is_healthy(), "{seen}");
    }

    /// Stamped when it asked, as upstream stamps it: a relist that took ten
    /// seconds to answer vouches for the runtime as it was when asked.
    #[tokio::test(start_paused = true)]
    async fn a_slow_relist_vouches_for_the_moment_it_asked() {
        let (runtime, ledger, relister) = relister();
        let slow = Duration::from_secs(10);
        runtime.set(Answer::Slowly(slow));
        let _ = relister.tick().await;
        assert!(
            health(&ledger).is_healthy(),
            "HARNESS PRECONDITION: it answered"
        );

        tokio::time::advance(threshold().saturating_sub(slow) + Duration::from_secs(2)).await;
        let seen = health(&ledger);
        assert!(
            !seen.is_healthy(),
            "a success begun over the threshold ago still vouched for the runtime: {seen}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_lease_is_held_back_only_by_a_judged_runtime_that_is_not_healthy() {
        let (runtime, ledger, relister) = relister();
        let judged = RuntimeHealthSource::Relisted(ledger);
        let now = || (Instant::now(), WallClock.now());

        let (at, wall) = now();
        assert!(
            RuntimeHealthSource::Unobserved
                .sight(at, wall)
                .permits_renewal(),
            "an unobserved runtime leaves the lease to the kubelet"
        );
        assert!(
            !judged.sight(at, wall).permits_renewal(),
            "a runtime no relist has answered holds the lease back"
        );

        let _ = relister.tick().await;
        let (at, wall) = now();
        assert!(
            judged.sight(at, wall).permits_renewal(),
            "a healthy runtime"
        );

        runtime.set(Answer::Fails);
        relist_for(&relister, threshold() + Duration::from_secs(2)).await;
        let (at, wall) = now();
        assert!(
            !judged.sight(at, wall).permits_renewal(),
            "an unhealthy runtime holds the lease back"
        );
    }
}
