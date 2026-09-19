//! CRASH-RESTART BACKOFF — `CrashLoopBackOff`.
//!
//! ★ WHY THIS EXISTS. Restart policy was implemented; backoff was not. A
//! container that exits is restarted on the very next tick, forever, with
//! no delay and no `CrashLoopBackOff` reason ever reported. Two distinct
//! harms, and the second is the worse one:
//!
//!   1. A genuinely broken container is restarted as fast as the sync loop
//!      spins, which on a busy node is a hot loop against the runtime.
//!   2. **The cluster cannot say a pod is broken.** Upstream's
//!      `CrashLoopBackOff` is the single most-recognised signal in
//!      Kubernetes operations — it is what `kubectl get pods` shows, what
//!      alerts fire on, and what a human looks for first. Without it a
//!      crash-looping pod reports `Running`, which is what the operator
//!      saw on cid 2026-08-29: a pod with 160 restarts displaying
//!      `Running 1/1` while restarting every three minutes.
//!
//! ★ THE CURVE IS UPSTREAM'S: 10s doubling to a 5-minute cap, reset once
//! the container has stayed up for MORE than 10 minutes. Those constants are
//! not taste — an operator reading `kubectl describe` compares the observed
//! delay against the one they know, and a different curve reads as a
//! malfunction. The reset rule is the subtle half: without it a container
//! that recovers stays penalised forever, and a pod that crashed once at
//! boot would take five minutes to restart a week later.
//!
//! ★ ONE CURVE TYPE, NOT A THIRD HAND-WRITTEN DOUBLING. The delay is read off
//! [`engenho_controllers::curve::Curve`], the shape every retry delay in the
//! controllers takes, so the doubling, the cap and the saturation at a huge
//! step are written once. A curve whose cap does not exceed its base fails
//! `cargo build` with E0080 (see `Curve::from_millis`; a post-monomorphization
//! error, so `cargo check` alone may not see it), so [`CRASH`] cannot be
//! edited into a flat retry that ships.
//!
//! ★ TWO CURVES LIVE HERE. [`CRASH`] paces container starts and restarts;
//! [`VOLUME_PENDING`] paces how soon the kubelet asks to come back for a pod
//! whose volumes did not resolve. That retry used to be a flat second,
//! forever: a pod naming a `ConfigMap` that never appears kept an otherwise
//! idle kubelet ticking once a second for as long as the pod existed.
//!
//! ★ PURE, AND CLOCK-INJECTED. Every decision is a function of the
//! container's [`CrashBackoff`] entry, when it exited and `now`, so the whole
//! curve is testable without sleeping — the same `TestClock` discipline the
//! probe engine already uses. The entry is upstream's (`flowcontrol.Backoff`),
//! not the restart count: see [`CrashBackoff`] for why that matters.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use engenho_controllers::curve::{Curve, Streak};

/// Upstream's crash-loop curve: 10 s after the first crash, doubling to a
/// 5-minute cap (`initialCrashLoopBackOff`, `MaxContainerBackOff`; kubelet
/// v1.34 with its alpha gates off). Multiplier 2, no jitter.
///
/// Step `n` of the curve is the wait before restart `n + 1`, so the first
/// restart, which owes nothing, is not on it: see [`delay_for`].
pub const CRASH: Curve = Curve::from_millis::<10_000, 300_000>();

/// A container that ran for LONGER than this before it exited has its
/// backoff forgiven.
///
/// Strictly longer, as upstream (`FinishedAt - lastUpdate > 600s`,
/// kubelet.go:1004-1006): a container that ran exactly 600 s is not
/// forgiven. Without this a container that recovers stays penalised forever.
pub const RESET_AFTER: Duration = Duration::from_secs(600);

/// The waiting reason of a container held off before its next start. The
/// exact upstream string: `kubectl get pods` prints it in the STATUS column
/// and every alerting rule matches on it.
pub const CRASH_LOOP_BACK_OFF: &str = "CrashLoopBackOff";

/// What the kubelet should do with a terminated, restartable container.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackoffDecision {
    /// Restart now.
    Restart,
    /// Hold. Carries how much longer, so the caller can report it and a
    /// test can assert the curve rather than the mere fact of waiting.
    Wait { remaining: Duration },
}

impl BackoffDecision {
    /// Hold for whatever is left of `owed` after `elapsed`, or restart once
    /// it has been served.
    const fn after(owed: Duration, elapsed: Duration) -> Self {
        match still_owed(owed, elapsed) {
            Some(remaining) => Self::Wait { remaining },
            None => Self::Restart,
        }
    }

    /// The `status.containerStatuses[].state.waiting.reason` to publish.
    ///
    /// `None` when restarting — there is no waiting state to report.
    #[must_use]
    pub fn waiting_reason(self) -> Option<&'static str> {
        match self {
            Self::Restart => None,
            Self::Wait { .. } => Some(CRASH_LOOP_BACK_OFF),
        }
    }
}

/// The delay owed after `repeats` consecutive failures: the start curve's
/// reading of [`CRASH`] ([`decide_start`], [`StartLedger`]).
///
/// `0` ⇒ no delay: the FIRST attempt is immediate. Backoff is a response to
/// repetition, not to a single failure. Every later one reads [`CRASH`]:
/// 10 s, 20 s, 40 s … capped at 300 s, and a huge count saturates at the cap
/// rather than wrapping to a short delay, because `Curve::delay` does.
///
/// A container that RAN and exited is paced by its [`CrashBackoff`] entry
/// instead, never by its restart count.
#[must_use]
pub const fn delay_for(repeats: u32) -> Duration {
    match repeats.checked_sub(1) {
        None => Duration::ZERO,
        Some(step) => CRASH.delay(step),
    }
}

// ── THE CRASH ENTRY: what a container that keeps exiting owes ──────────────

/// One container's crash-loop penalty: upstream's `flowcontrol.Backoff`
/// entry for the container (`backoffEntry{backoff, lastUpdate}`).
///
/// ★ THE PENALTY IS ITS OWN STATE, NOT THE RESTART COUNT. It used to be read
/// off `restartCount` (`delay_for(restart_count)`), and a restart count never
/// goes down. A container that earned its reset by running past
/// [`RESET_AFTER`] was restarted at once, as it should be, and then its NEXT
/// short crash owed `delay_for(restartCount)`: five minutes for any container
/// that had crashed six times in its life, where upstream owes ten seconds.
/// Upstream's reset re-initialises the entry, and so does [`crash_gate`].
///
/// ★ THE WAIT IS A STEP ON [`CRASH`], NEVER A FREE DURATION, so an entry that
/// owes a wait off the curve cannot be built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CrashBackoff {
    /// The wait the container's next crash owes, as a step on [`CRASH`]
    /// (upstream `backoff`). A step past the cap saturates at it.
    pub step: u32,
    /// When the kubelet last let the container go (upstream `lastUpdate`,
    /// stamped by `Next` just before the start).
    pub last_update: Instant,
}

impl CrashBackoff {
    /// The wait a crash owes under this entry.
    #[must_use]
    pub const fn owed(self) -> Duration {
        CRASH.delay(self.step)
    }

    /// Does the exit that finished at `finished_at` wipe this entry? Upstream's
    /// kubelet `HasExpiredFunc` (kubelet.go:1004-1006): the container ran for
    /// MORE than [`RESET_AFTER`] from the entry's `lastUpdate` to its exit.
    ///
    /// Measured to the exit, never to now: a short run followed by an hour of
    /// idle wall-clock is not forgiveness.
    #[must_use]
    pub fn forgiven_by(self, finished_at: Instant) -> bool {
        finished_at.saturating_duration_since(self.last_update) > RESET_AFTER
    }
}

/// What the crash-loop gate says about a container that exited and that its
/// restart policy restarts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "a Go carries the entry the container must record once it restarts"]
pub enum CrashGate {
    /// Held in `CrashLoopBackOff` (upstream `IsInBackOffSince`). The entry is
    /// unchanged.
    Hold {
        /// How much longer until the restart is allowed.
        remaining: Duration,
    },
    /// Restart now. Carries the entry the container has from this restart on
    /// (upstream `Next`): record it once the replacement has started.
    Go(CrashBackoff),
}

impl CrashGate {
    /// The `status.containerStatuses[].state.waiting.reason` to publish:
    /// [`CRASH_LOOP_BACK_OFF`] while held, `None` when restarting.
    #[must_use]
    pub fn waiting_reason(self) -> Option<&'static str> {
        match self {
            Self::Hold { .. } => Some(CRASH_LOOP_BACK_OFF),
            Self::Go(_) => None,
        }
    }
}

/// Upstream's `doBackOff` (kuberuntime_manager.go:1607-1638) for one exit.
///
/// `entry` is the container's penalty so far (`None` before its first
/// restart), `finished_at` when the exit happened, `now` the caller's clock.
///
/// - An entry the exit [forgives](CrashBackoff::forgiven_by) is as good as
///   none.
/// - With a live entry, the container is held while `now - finished_at` is
///   STRICTLY less than what the entry owes.
/// - Otherwise it goes, and the entry it goes with is the base of the curve
///   when it had none (or it was forgiven), the next step when it had one.
///
/// So the first crash restarts at once and the second owes 10 s, as
/// upstream: backoff answers repetition, not a single exit.
pub fn crash_gate(entry: Option<CrashBackoff>, finished_at: Instant, now: Instant) -> CrashGate {
    let live = entry.filter(|e| !e.forgiven_by(finished_at));
    if let Some(e) = live
        && let Some(remaining) = still_owed(e.owed(), now.saturating_duration_since(finished_at))
    {
        return CrashGate::Hold { remaining };
    }
    CrashGate::Go(CrashBackoff {
        step: live.map_or(0, |e| e.step.saturating_add(1)),
        last_update: now,
    })
}

/// The same curve, for a container that has never started at all.
///
/// [`crash_gate`] answers "this container RAN and exited"; its reset rule
/// keys on how long it ran, which a container that never started did not. A
/// start that
/// FAILS — an unpullable image, a backend that cannot run the image at all —
/// is the other half of the same question and had no backoff whatsoever.
///
/// Measured on ryn 2026-09-18: `pitr-lab/mysql-0` declares
/// `docker.io/library/mysql:8.0`, the native macOS backend cannot run an OCI
/// image, and the kubelet retried that permanently-impossible start **twice a
/// second**, each attempt writing a log line and a status patch. Backed off it
/// is four attempts in the first minute and one every five minutes after.
///
/// `failures` is the count of CONSECUTIVE failed starts, so the first attempt
/// is immediate (`delay_for(0) == ZERO`) and a success clears it. The cap is a
/// ceiling on the wait, never a stop: a permanently broken container keeps
/// being retried, because "gave up" must be a state an operator can see rather
/// than infer from silence.
#[must_use]
pub fn decide_start(failures: u32, since_last_attempt: Duration) -> BackoffDecision {
    BackoffDecision::after(delay_for(failures), since_last_attempt)
}

/// What is left of an `owed` wait once `elapsed` has passed, or `None` once
/// it has been served in full. Served means `elapsed >= owed`: upstream holds
/// only while `now - FinishedAt < backoff`, strictly.
const fn still_owed(owed: Duration, elapsed: Duration) -> Option<Duration> {
    match owed.checked_sub(elapsed) {
        Some(remaining) if !remaining.is_zero() => Some(remaining),
        _ => None,
    }
}

// ── THE START LEDGER: every start attempt of a container asks it first ──

/// Consecutive failed starts per container, and when the last attempt was.
///
/// ★ ONE GATE FOR EVERY START. The kubelet starts a container from three
/// places: the first start of a pod's containers, a restart after an exit or
/// a probe failure, and the init sequence. Only the first of them consulted
/// [`decide_start`], so a start that kept failing anywhere else was retried
/// at the sync loop's speed — the shape measured as `pitr-lab/mysql-0` being
/// retried twice a second. A launch now needs a [`StartPermit`], and the only
/// constructor of one is [`StartLedger::permit`], which says no while the
/// container is owed a wait. So a start that skips the curve cannot be
/// written through the permit path; a call that goes around the permit to the
/// runtime is still possible and is caught by the kubelet's tests, not by the
/// compiler.
///
/// Keyed by pod, then container. Kubernetes requires container names to be
/// unique across a pod's init and app containers, so one map serves both.
#[derive(Debug, Default)]
pub struct StartLedger {
    failures: HashMap<String, HashMap<String, (u32, Instant)>>,
}

/// Leave to make ONE start attempt of one container.
///
/// Built only by [`StartLedger::permit`] and consumed by
/// [`StartLedger::succeeded`] or [`StartLedger::failed`], so the outcome of
/// an attempt is recorded against the container the permit was issued for.
/// Not `Clone`: one permit, one attempt.
#[derive(Debug)]
#[must_use = "a permit is leave to start one container; record the attempt's outcome with it"]
pub struct StartPermit {
    pod: String,
    container: String,
    at: Instant,
}

/// The ledger refused a start: the container's last starts failed and the
/// curve still owes a wait.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StartHeld {
    /// How much longer until the next attempt is allowed.
    pub remaining: Duration,
    /// The consecutive failed starts that earned the wait.
    pub consecutive_failures: u32,
}

/// What a failed start cost: how many in a row, and when the next attempt is
/// allowed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StartFailed {
    /// Consecutive failed starts, this one included.
    pub consecutive_failures: u32,
    /// The wait the curve now owes before the next attempt.
    pub next_attempt_in: Duration,
}

impl StartLedger {
    /// Leave to start `container` of `pod` at `now`, or how long it must
    /// still wait.
    ///
    /// A container with no failed start on record is always allowed: the
    /// first attempt is immediate, as [`decide_start`] says.
    ///
    /// # Errors
    ///
    /// [`StartHeld`] while the curve owes a wait after a failed start.
    pub fn permit(
        &self,
        pod: &str,
        container: &str,
        now: Instant,
    ) -> Result<StartPermit, StartHeld> {
        if let Some(&(failures, last)) = self.failures.get(pod).and_then(|c| c.get(container))
            && let BackoffDecision::Wait { remaining } =
                decide_start(failures, now.saturating_duration_since(last))
        {
            return Err(StartHeld {
                remaining,
                consecutive_failures: failures,
            });
        }
        Ok(StartPermit {
            pod: pod.to_string(),
            container: container.to_string(),
            at: now,
        })
    }

    /// The attempt started the container: its failure count is cleared.
    pub fn succeeded(&mut self, permit: StartPermit) {
        let StartPermit { pod, container, .. } = permit;
        if let Some(containers) = self.failures.get_mut(&pod) {
            containers.remove(&container);
            if containers.is_empty() {
                self.failures.remove(&pod);
            }
        }
    }

    /// The attempt failed: one more consecutive failure, stamped at the
    /// instant the permit was issued.
    pub fn failed(&mut self, permit: StartPermit) -> StartFailed {
        let entry = self
            .failures
            .entry(permit.pod)
            .or_default()
            .entry(permit.container)
            .or_insert((0, permit.at));
        entry.0 = entry.0.saturating_add(1);
        entry.1 = permit.at;
        StartFailed {
            consecutive_failures: entry.0,
            next_attempt_in: delay_for(entry.0),
        }
    }

    /// Forget every pod `keep` rejects. A pod that is gone takes its
    /// penalties with it, so the ledger does not grow for the process's
    /// lifetime and a recreated pod of the same name starts clean.
    pub fn retain_pods(&mut self, mut keep: impl FnMut(&str) -> bool) {
        self.failures.retain(|pod, _| keep(pod));
    }
}

// ── THE VOLUME-PENDING LEDGER: how soon to come back for an unresolved pod ──

/// Upstream's volume-operation retry curve: 500 ms after the first failure,
/// doubling to 2m2s (`initialDurationBeforeRetry`,
/// `maxDurationBeforeRetry`; pkg/util/goroutinemap/exponentialbackoff,
/// v1.34). It is the curve behind upstream's familiar
/// `No retries permitted until … (durationBeforeRetry 2m2s)`.
///
/// The kubelet never asks to be re-ticked sooner than its 1 s requeue floor,
/// so the first step is observed as 1 s; the curve is kept at upstream's
/// constants so the cap, the part an operator waits on, is the one they know.
pub const VOLUME_PENDING: Curve = Curve::from_millis::<500, 122_000>();

/// Per pod, the run of volume-resolution misses and when the pod is next due.
///
/// ★ WHAT THIS PACES, AND WHAT IT DOES NOT. It paces the kubelet's OWN timer:
/// the requeue it asks for when a pod's volumes did not resolve, which was a
/// flat 1 s forever. It never refuses an attempt. The kubelet is also woken
/// by writes to the kinds a pod's volumes read (`ConfigMap`, `Secret`,
/// `PersistentVolumeClaim`, `PersistentVolume`), and a pod woken that way
/// re-resolves at once: holding it until the curve allowed would make a pod
/// whose `ConfigMap` was just created wait up to 2m2s for nothing. That is the
/// one place this departs from upstream, which refuses every retry before
/// its `durationBeforeRetry`.
///
/// So only an attempt made when the pod was DUE counts as a miss on the
/// curve; an earlier one (a write woke the kubelet, or another pod's timer
/// did) is [`VolumeRetry::Early`] and leaves the curve where it was. The gap
/// between counted misses therefore grows with wall time, not with how busy
/// the kubelet happens to be.
///
/// Keyed by pod. A resolution that succeeds clears the pod's entry, and a pod
/// no longer bound here is dropped by [`VolumePendingLedger::retain_pods`].
#[derive(Debug, Default)]
pub struct VolumePendingLedger {
    pods: HashMap<String, PendingPod>,
}

/// One pod's misses on [`VOLUME_PENDING`]: when the last counted miss was,
/// and the wait it owed.
#[derive(Clone, Copy, Debug)]
struct PendingPod {
    streak: Streak,
    missed_at: Instant,
    owed: Duration,
}

/// What an unresolved attempt cost, and when the pod should be tried again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VolumeRetry {
    /// The pod was due: one more miss on the curve.
    Missed {
        /// Consecutive counted misses, this one included.
        consecutive: u32,
        /// The wait the curve now owes before the pod is due again.
        next_attempt_in: Duration,
    },
    /// The pod was not yet due (a write or another pod's timer woke the
    /// kubelet): the curve is unchanged.
    Early {
        /// Consecutive counted misses so far.
        consecutive: u32,
        /// How much longer until the pod is due.
        remaining: Duration,
    },
}

impl VolumeRetry {
    /// How long the kubelet should wait before trying this pod again.
    #[must_use]
    pub const fn next_attempt_in(self) -> Duration {
        match self {
            Self::Missed {
                next_attempt_in, ..
            } => next_attempt_in,
            Self::Early { remaining, .. } => remaining,
        }
    }
}

impl VolumePendingLedger {
    /// `pod`'s volumes did not resolve at `now`.
    ///
    /// Counts a miss when the pod was due (or has no entry), and otherwise
    /// reports how long until it is.
    pub fn unresolved(&mut self, pod: &str, now: Instant) -> VolumeRetry {
        // A pod with no entry owes nothing yet, so its first miss counts.
        let entry = self.pods.entry(pod.to_string()).or_insert(PendingPod {
            streak: Streak::new(VOLUME_PENDING),
            missed_at: now,
            owed: Duration::ZERO,
        });
        let elapsed = now.saturating_duration_since(entry.missed_at);
        if let Some(remaining) = still_owed(entry.owed, elapsed) {
            return VolumeRetry::Early {
                consecutive: entry.streak.misses(),
                remaining,
            };
        }
        entry.owed = entry.streak.miss();
        entry.missed_at = now;
        VolumeRetry::Missed {
            consecutive: entry.streak.misses(),
            next_attempt_in: entry.owed,
        }
    }

    /// `pod`'s volumes resolved: its next miss, if any, starts from the base.
    pub fn resolved(&mut self, pod: &str) {
        self.pods.remove(pod);
    }

    /// Forget every pod `keep` rejects, so a pod that is gone takes its
    /// streak with it and a recreated pod of the same name starts clean.
    pub fn retain_pods(&mut self, mut keep: impl FnMut(&str) -> bool) {
        self.pods.retain(|pod, _| keep(pod));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: fn(u64) -> Duration = Duration::from_secs;

    /// `seconds` after `base`, for writing upstream's rows in their units.
    fn at(base: Instant, seconds: u64) -> Instant {
        base + S(seconds)
    }

    /// The entry a container is left with once `crashes` short runs have each
    /// been restarted as soon as allowed. A run lasts `ran`, and each restart
    /// waits exactly what it owes.
    fn after_short_crashes(base: Instant, crashes: u32, ran: Duration) -> (CrashBackoff, Instant) {
        let mut entry = None;
        let mut started = base;
        for _ in 0..crashes {
            let finished = started + ran;
            let owed = entry.map_or(Duration::ZERO, CrashBackoff::owed);
            let now = finished + owed;
            match crash_gate(entry, finished, now) {
                CrashGate::Go(next) => entry = Some(next),
                CrashGate::Hold { remaining } => {
                    panic!("served its wait, still held {remaining:?}")
                }
            }
            started = now;
        }
        (entry.expect("at least one crash"), started)
    }

    #[test]
    fn the_first_restart_is_immediate() {
        // Backoff answers repetition, not a single exit. A pod that exits
        // once must not wait 10s to come back.
        let t0 = Instant::now();
        assert_eq!(delay_for(0), Duration::ZERO);
        assert_eq!(
            crash_gate(None, t0, t0),
            CrashGate::Go(CrashBackoff {
                step: 0,
                last_update: t0
            }),
            "no entry yet: restart now, and the next crash owes the base"
        );
    }

    #[test]
    fn the_curve_is_upstreams_ten_seconds_doubling_to_five_minutes() {
        // Not taste: an operator compares the observed delay against the
        // one they know, and a different curve reads as a malfunction.
        assert_eq!(delay_for(1), S(10));
        assert_eq!(delay_for(2), S(20));
        assert_eq!(delay_for(3), S(40));
        assert_eq!(delay_for(4), S(80));
        assert_eq!(delay_for(5), S(160));
        // Capped at 5 minutes thereafter.
        assert_eq!(delay_for(6), S(300));
        assert_eq!(delay_for(7), S(300));
    }

    /// The upstream oracle row `backoff/kubelet default schedule 10s..300s`
    /// (container-restart.json): what the entry owes after each of eight
    /// restarts, none of them forgiven.
    #[test]
    fn eight_crashes_owe_upstreams_exact_schedule() {
        let t0 = Instant::now();
        let owed: Vec<u64> = (1..=8)
            .map(|n| after_short_crashes(t0, n, S(1)).0.owed().as_secs())
            .collect();
        assert_eq!(owed, [10, 20, 40, 80, 160, 300, 300, 300]);
    }

    #[test]
    fn a_large_step_saturates_rather_than_overflowing() {
        // The overflow bug would appear only after ~30 crashes — exactly
        // when backoff matters most — and would present as a hot loop.
        let t0 = Instant::now();
        for step in [30u32, 31, 32, 33, 1_000, u32::MAX] {
            let entry = CrashBackoff {
                step,
                last_update: t0,
            };
            assert_eq!(entry.owed(), S(300), "step {step} must cap");
            let CrashGate::Go(next) = crash_gate(Some(entry), t0, at(t0, 300)) else {
                panic!("the capped wait was served");
            };
            assert_eq!(next.owed(), S(300), "step {step} must stay capped");
        }
        assert_eq!(delay_for(u32::MAX), S(300));
    }

    #[test]
    fn waiting_reports_crashloopbackoff_verbatim() {
        // kubectl prints this in STATUS; alerting rules match on it.
        let t0 = Instant::now();
        let held = crash_gate(
            Some(CrashBackoff {
                step: 2,
                last_update: t0,
            }),
            at(t0, 1),
            at(t0, 2),
        );
        assert_eq!(held.waiting_reason(), Some("CrashLoopBackOff"));
        let went = crash_gate(None, t0, t0);
        assert_eq!(went.waiting_reason(), None);
        assert_eq!(BackoffDecision::Restart.waiting_reason(), None);
    }

    #[test]
    fn the_remaining_time_is_reported_not_just_the_fact_of_waiting() {
        let t0 = Instant::now();
        let twenty = Some(CrashBackoff {
            step: 1,
            last_update: t0,
        });
        let finished = at(t0, 1);
        assert_eq!(
            crash_gate(twenty, finished, finished + S(5)),
            CrashGate::Hold { remaining: S(15) }
        );
        // Served means `now - finished >= owed`: upstream holds only while
        // strictly less.
        assert!(matches!(
            crash_gate(twenty, finished, finished + S(20)),
            CrashGate::Go(_)
        ));
        assert!(matches!(
            crash_gate(twenty, finished, finished + S(21)),
            CrashGate::Go(_)
        ));
    }

    /// ★ THE RESET RE-INITIALISES THE PENALTY. A container at the 5-minute
    /// cap that then runs past `RESET_AFTER` is restarted at once — and its
    /// next short crash owes 10 s, as upstream's re-initialised entry does,
    /// not the 5 minutes its lifetime restart count would.
    #[test]
    fn a_forgiven_container_owes_the_base_again_not_its_old_penalty() {
        let t0 = Instant::now();
        let (capped, started) = after_short_crashes(t0, 7, S(1));
        assert_eq!(capped.owed(), S(300), "seven short crashes reach the cap");

        // It stays up for eleven minutes, then dies: restarted at once.
        let finished = started + S(660);
        let CrashGate::Go(fresh) = crash_gate(Some(capped), finished, finished) else {
            panic!("a container that ran past RESET_AFTER is restarted at once");
        };
        assert_eq!(
            fresh.owed(),
            S(10),
            "the forgiven entry starts from the base"
        );

        // Its next crash, a second into the replacement, is held 10 s.
        let crashed = finished + S(1);
        assert_eq!(
            crash_gate(Some(fresh), crashed, crashed),
            CrashGate::Hold { remaining: S(10) }
        );
    }

    #[test]
    fn a_container_that_stayed_up_long_enough_is_forgiven() {
        // Without the reset, a pod that crashed once at boot would still be
        // waiting five minutes to restart a week later.
        let t0 = Instant::now();
        let capped = Some(CrashBackoff {
            step: 5,
            last_update: t0,
        });
        assert!(matches!(
            crash_gate(capped, at(t0, 601), at(t0, 601)),
            CrashGate::Go(CrashBackoff { step: 0, .. })
        ));
        // Well short of the threshold is NOT forgiven.
        assert!(matches!(
            crash_gate(capped, at(t0, 599), at(t0, 599)),
            CrashGate::Hold { .. }
        ));
    }

    /// The upstream oracle rows `backoff/kubelet reset threshold: exactly
    /// 600s is NOT expired` and `…: 600s + 1ns is expired`
    /// (kubelet.go:1004-1006, `> 600*time.Second`).
    #[test]
    fn exactly_ten_minutes_up_is_not_forgiven_and_a_nanosecond_more_is() {
        let t0 = Instant::now();
        let capped = CrashBackoff {
            step: 5,
            last_update: t0,
        };
        assert!(
            !capped.forgiven_by(at(t0, 600)),
            "a container that ran exactly 600s keeps its backoff"
        );
        assert_eq!(
            crash_gate(Some(capped), at(t0, 600), at(t0, 600)),
            CrashGate::Hold { remaining: S(300) }
        );
        assert!(capped.forgiven_by(at(t0, 600) + Duration::from_nanos(1)));
    }

    /// The upstream oracle row `do-backoff/long idle wall-clock does NOT
    /// reset`: a short run, a crash, and an hour of idle wall-clock is not
    /// forgiveness. The reset keys on how long the container RAN.
    #[test]
    fn idle_wall_clock_after_a_short_run_is_not_forgiveness() {
        let t0 = Instant::now();
        let one_sixty = Some(CrashBackoff {
            step: 4,
            last_update: t0,
        });
        // It is restarted (the owed wait is long past) …
        let CrashGate::Go(next) = crash_gate(one_sixty, at(t0, 30), at(t0, 3_630)) else {
            panic!("the owed wait was served an hour ago");
        };
        // … but it goes with the next, longer step.
        assert_eq!(next.owed(), S(300));
    }

    #[test]
    fn the_measured_incident_would_now_be_visible() {
        // cid 2026-08-29: a container exiting every 180s, restarted 160
        // times, reported Running the whole way. With backoff it enters
        // CrashLoopBackOff and kubectl says so.
        let t0 = Instant::now();
        let (entry, started) = after_short_crashes(t0, 160, S(180));
        let finished = started + S(180);
        let d = crash_gate(Some(entry), finished, finished + S(1));
        assert_eq!(d.waiting_reason(), Some("CrashLoopBackOff"));
        // 180s runs are well under the 600s forgiveness threshold, so the
        // penalty keeps mattering — which is the point.
        assert!(matches!(d, CrashGate::Hold { .. }));
    }

    // ── decide_start: the never-started half of the same curve ─────────

    #[test]
    fn the_first_start_attempt_is_immediate() {
        assert_eq!(decide_start(0, S(0)), BackoffDecision::Restart);
    }

    #[test]
    fn a_failed_start_defers_the_next_attempt_on_the_upstream_curve() {
        assert_eq!(
            decide_start(1, S(0)),
            BackoffDecision::Wait { remaining: S(10) }
        );
        assert_eq!(decide_start(1, S(10)), BackoffDecision::Restart);
        assert_eq!(
            decide_start(2, S(0)),
            BackoffDecision::Wait { remaining: S(20) }
        );
        assert_eq!(
            decide_start(3, S(0)),
            BackoffDecision::Wait { remaining: S(40) }
        );
    }

    /// The measured defect, as arithmetic: a start that can never succeed was
    /// polled twice a second. Replay that cadence against the gate.
    #[test]
    fn a_permanently_failing_start_costs_a_handful_of_attempts_a_minute() {
        let mut failures = 0u32;
        let mut since = S(0);
        let mut attempts = 0;
        for _ in 0..120 {
            // 120 polls = 60s at the observed 2/s
            if decide_start(failures, since) == BackoffDecision::Restart {
                attempts += 1;
                failures += 1;
                since = S(0);
            }
            since += Duration::from_millis(500);
        }
        assert!(
            (2..=4).contains(&attempts),
            "expected a handful of attempts in a minute, got {attempts}"
        );
    }

    #[test]
    fn a_start_penalty_caps_rather_than_growing_forever() {
        assert_eq!(delay_for(u32::MAX), S(300));
        assert_eq!(
            decide_start(u32::MAX, S(300)),
            BackoffDecision::Restart,
            "a capped penalty still expires; backoff never becomes give-up"
        );
    }

    // ── StartLedger: the gate every start attempt goes through ──────────

    #[test]
    fn a_container_with_no_failed_start_is_always_permitted() {
        let ledger = StartLedger::default();
        assert!(ledger.permit("default/p", "app", Instant::now()).is_ok());
    }

    #[test]
    fn a_failed_start_holds_that_container_and_no_other() {
        let t0 = Instant::now();
        let mut ledger = StartLedger::default();
        let permit = ledger.permit("default/p", "app", t0).unwrap();
        let failed = ledger.failed(permit);
        assert_eq!(
            failed,
            StartFailed {
                consecutive_failures: 1,
                next_attempt_in: S(10),
            }
        );
        assert_eq!(
            ledger.permit("default/p", "app", t0 + S(3)).unwrap_err(),
            StartHeld {
                remaining: S(7),
                consecutive_failures: 1,
            }
        );
        // A sibling, and the same name in another pod, are not penalised.
        assert!(ledger.permit("default/p", "sidecar", t0).is_ok());
        assert!(ledger.permit("default/q", "app", t0).is_ok());
        // The wait expires on the curve.
        assert!(ledger.permit("default/p", "app", t0 + S(10)).is_ok());
    }

    #[test]
    fn a_successful_start_clears_the_penalty() {
        let t0 = Instant::now();
        let mut ledger = StartLedger::default();
        for n in 0..4u64 {
            let permit = ledger
                .permit("default/p", "app", t0 + S(1_000 * n))
                .unwrap();
            let _ = ledger.failed(permit);
        }
        let permit = ledger.permit("default/p", "app", t0 + S(10_000)).unwrap();
        ledger.succeeded(permit);
        // The next failure is the FIRST again: a 10s wait, not 160s.
        let permit = ledger.permit("default/p", "app", t0 + S(10_001)).unwrap();
        assert_eq!(ledger.failed(permit).next_attempt_in, S(10));
    }

    /// The T2.4 bound, as arithmetic: a start that always fails, asked about
    /// once a second for an hour, is let through at most 16 times — 10s
    /// doubling to the 5-minute cap — and is still being retried at the end.
    #[test]
    fn a_start_that_always_fails_is_attempted_at_most_sixteen_times_an_hour() {
        let t0 = Instant::now();
        let mut ledger = StartLedger::default();
        let mut attempts = Vec::new();
        for second in 0..3_600u64 {
            let now = t0 + S(second);
            if let Ok(permit) = ledger.permit("default/p", "app", now) {
                attempts.push(second);
                let _ = ledger.failed(permit);
            }
        }
        let first: Vec<u64> = attempts.iter().copied().take(8).collect();
        assert!(
            attempts.len() <= 16,
            "at most 16 attempts in an hour, got {} (first at {first:?})",
            attempts.len()
        );
        assert!(
            attempts.last().is_some_and(|last| *last >= 3_600 - 300),
            "the cap is a ceiling on the wait, never a stop: last attempt at {:?}",
            attempts.last()
        );
    }

    #[test]
    fn a_pod_that_is_gone_takes_its_penalties_with_it() {
        let t0 = Instant::now();
        let mut ledger = StartLedger::default();
        for pod in ["default/gone", "default/kept"] {
            let permit = ledger.permit(pod, "app", t0).unwrap();
            let _ = ledger.failed(permit);
        }
        ledger.retain_pods(|pod| pod == "default/kept");
        assert!(ledger.permit("default/gone", "app", t0).is_ok());
        assert!(ledger.permit("default/kept", "app", t0).is_err());
    }

    // ── VolumePendingLedger: how soon to come back for an unresolved pod ──

    const MS: fn(u64) -> Duration = Duration::from_millis;

    /// Poll a pod whose volumes never resolve once a second for `seconds`,
    /// returning the virtual second of every attempt that counted.
    fn counted_misses_over(seconds: u64) -> Vec<u64> {
        let t0 = Instant::now();
        let mut ledger = VolumePendingLedger::default();
        (0..seconds)
            .filter(|second| {
                matches!(
                    ledger.unresolved("default/p", t0 + S(*second)),
                    VolumeRetry::Missed { .. }
                )
            })
            .collect()
    }

    #[test]
    fn the_volume_curve_is_upstreams_half_second_doubling_to_two_minutes_two() {
        let t0 = Instant::now();
        let mut ledger = VolumePendingLedger::default();
        let mut at = t0;
        let mut owed = Vec::new();
        for _ in 0..10 {
            let retry = ledger.unresolved("default/p", at);
            owed.push(retry.next_attempt_in());
            at += retry.next_attempt_in();
        }
        assert_eq!(
            owed,
            [
                MS(500),
                S(1),
                S(2),
                S(4),
                S(8),
                S(16),
                S(32),
                S(64),
                S(122),
                S(122)
            ]
        );
    }

    /// The measured defect, replayed: a pod whose `ConfigMap` never appears,
    /// asked about once a second for an hour. Flat, that is 3 600 attempts
    /// and 3 600 warnings; on the curve it is a few dozen, the gap between
    /// them grows until the cap, and the pod is still being retried at the
    /// end of the hour.
    #[test]
    fn a_pod_that_never_resolves_is_retried_on_a_growing_curve_not_every_second() {
        let misses = counted_misses_over(3_600);
        assert!(
            misses.len() <= 40,
            "at most 40 counted attempts in an hour, got {} ({misses:?})",
            misses.len()
        );
        let gaps: Vec<u64> = misses.windows(2).map(|w| w[1] - w[0]).collect();
        for pair in gaps.windows(2) {
            assert!(
                pair[1] >= pair[0],
                "the gap between counted attempts never shrinks: {gaps:?}"
            );
        }
        assert!(
            gaps.iter().all(|gap| *gap <= 122),
            "the cap is 2m2s: {gaps:?}"
        );
        assert!(
            misses.last().is_some_and(|last| *last >= 3_600 - 122),
            "the cap is a ceiling on the wait, never a stop: last attempt at {:?}",
            misses.last()
        );
    }

    #[test]
    fn an_early_attempt_does_not_grow_the_curve() {
        // A write, or another pod's timer, can wake the kubelet before this
        // pod is due. That attempt still happens; it just does not count.
        let t0 = Instant::now();
        let mut ledger = VolumePendingLedger::default();
        let _ = ledger.unresolved("default/p", t0);
        let _ = ledger.unresolved("default/p", t0 + MS(500));
        assert_eq!(
            ledger.unresolved("default/p", t0 + MS(500) + MS(300)),
            VolumeRetry::Early {
                consecutive: 2,
                remaining: MS(700),
            }
        );
        // When the pod IS due, the curve takes its next step from where it
        // was, not from where the early attempts would have pushed it.
        assert_eq!(
            ledger.unresolved("default/p", t0 + MS(1_500)),
            VolumeRetry::Missed {
                consecutive: 3,
                next_attempt_in: S(2),
            }
        );
    }

    #[test]
    fn a_pod_whose_volumes_resolve_starts_its_next_streak_from_the_base() {
        let t0 = Instant::now();
        let mut ledger = VolumePendingLedger::default();
        let mut at = t0;
        for _ in 0..6 {
            at += ledger.unresolved("default/p", at).next_attempt_in();
        }
        ledger.resolved("default/p");
        assert_eq!(
            ledger.unresolved("default/p", at),
            VolumeRetry::Missed {
                consecutive: 1,
                next_attempt_in: MS(500),
            },
            "a resolved pod owes the base again, not the grown step"
        );
    }

    #[test]
    fn one_pods_streak_does_not_slow_another_and_a_gone_pod_takes_its_streak() {
        let t0 = Instant::now();
        let mut ledger = VolumePendingLedger::default();
        let mut at = t0;
        for _ in 0..5 {
            at += ledger.unresolved("default/slow", at).next_attempt_in();
        }
        assert_eq!(
            ledger.unresolved("default/fresh", at).next_attempt_in(),
            MS(500)
        );
        ledger.retain_pods(|pod| pod == "default/fresh");
        assert_eq!(
            ledger.unresolved("default/slow", at),
            VolumeRetry::Missed {
                consecutive: 1,
                next_attempt_in: MS(500),
            },
            "a recreated pod of the same name starts clean"
        );
    }
}
