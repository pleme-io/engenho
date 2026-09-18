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
//! ★ THE CURVE IS UPSTREAM'S: 10s doubling to a 5-minute cap, reset after
//! the container has stayed up for 10 minutes. Those constants are not
//! taste — an operator reading `kubectl describe` compares the observed
//! delay against the one they know, and a different curve reads as a
//! malfunction. The reset rule is the subtle half: without it a container
//! that recovers stays penalised forever, and a pod that crashed once at
//! boot would take five minutes to restart a week later.
//!
//! ★ PURE, AND CLOCK-INJECTED. Every decision is a function of
//! `(restart_count, last_exit, now)`, so the whole curve is testable
//! without sleeping — the same `TestClock` discipline the probe engine
//! already uses.

use std::time::Duration;

/// Upstream's initial delay after the first crash.
pub const BASE_DELAY: Duration = Duration::from_secs(10);

/// Upstream's ceiling. The delay doubles up to this and no further.
pub const MAX_DELAY: Duration = Duration::from_secs(300);

/// How long a container must stay up before its backoff is forgiven.
///
/// Without this a container that recovers stays penalised forever.
pub const RESET_AFTER: Duration = Duration::from_secs(600);

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
    /// The `status.containerStatuses[].state.waiting.reason` to publish.
    ///
    /// `None` when restarting — there is no waiting state to report.
    #[must_use]
    pub fn waiting_reason(self) -> Option<&'static str> {
        match self {
            Self::Restart => None,
            // The exact upstream string. `kubectl get pods` prints this in
            // the STATUS column and every alerting rule matches on it.
            Self::Wait { .. } => Some("CrashLoopBackOff"),
        }
    }
}

/// The delay owed after `restart_count` prior restarts.
///
/// `0` prior restarts ⇒ no delay: the FIRST restart is immediate, matching
/// upstream. Backoff is a response to repetition, not to a single exit.
#[must_use]
pub fn delay_for(restart_count: u32) -> Duration {
    if restart_count == 0 {
        return Duration::ZERO;
    }
    // 10s, 20s, 40s … capped. `checked_mul` rather than a shift so a large
    // restart_count saturates at the cap instead of overflowing to a tiny
    // delay — the failure mode would be a hot loop appearing only after a
    // container had crashed ~30 times, which is exactly when it matters.
    let factor = 1u32.checked_shl(restart_count - 1).unwrap_or(u32::MAX);
    BASE_DELAY
        .checked_mul(factor)
        .unwrap_or(MAX_DELAY)
        .min(MAX_DELAY)
}

/// Decide whether to restart now.
///
/// `since_exit` is how long ago the container terminated; `uptime_before_exit`
/// is how long it had been running. All durations are supplied by the
/// caller's clock so this stays pure.
#[must_use]
pub fn decide(
    restart_count: u32,
    since_exit: Duration,
    uptime_before_exit: Duration,
) -> BackoffDecision {
    // A container that stayed up long enough has earned a clean slate.
    // Checked BEFORE the delay so a recovered container is never penalised
    // for an old crash.
    if uptime_before_exit >= RESET_AFTER {
        return BackoffDecision::Restart;
    }
    let owed = delay_for(restart_count);
    if since_exit >= owed {
        BackoffDecision::Restart
    } else {
        BackoffDecision::Wait {
            remaining: owed - since_exit,
        }
    }
}

/// The same curve, for a container that has never started at all.
///
/// [`decide`] answers "this container RAN and exited"; its reset rule keys on
/// uptime, which a container that never started does not have. A start that
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
    let owed = delay_for(failures);
    if since_last_attempt >= owed {
        BackoffDecision::Restart
    } else {
        BackoffDecision::Wait {
            remaining: owed - since_last_attempt,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: fn(u64) -> Duration = Duration::from_secs;

    #[test]
    fn the_first_restart_is_immediate() {
        // Backoff answers repetition, not a single exit. A pod that exits
        // once must not wait 10s to come back.
        assert_eq!(delay_for(0), Duration::ZERO);
        assert_eq!(decide(0, S(0), S(1)), BackoffDecision::Restart);
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
        assert_eq!(delay_for(6), MAX_DELAY);
        assert_eq!(delay_for(7), MAX_DELAY);
    }

    #[test]
    fn a_large_restart_count_saturates_rather_than_overflowing() {
        // The overflow bug would appear only after ~30 crashes — exactly
        // when backoff matters most — and would present as a hot loop.
        for n in [30u32, 31, 32, 33, 1_000, u32::MAX] {
            assert_eq!(delay_for(n), MAX_DELAY, "restart_count {n} must cap");
        }
    }

    #[test]
    fn waiting_reports_crashloopbackoff_verbatim() {
        // kubectl prints this in STATUS; alerting rules match on it.
        let d = decide(3, S(1), S(1));
        assert_eq!(d.waiting_reason(), Some("CrashLoopBackOff"));
        assert_eq!(BackoffDecision::Restart.waiting_reason(), None);
    }

    #[test]
    fn the_remaining_time_is_reported_not_just_the_fact_of_waiting() {
        match decide(2, S(5), S(1)) {
            BackoffDecision::Wait { remaining } => assert_eq!(remaining, S(15)),
            other => panic!("expected a wait, got {other:?}"),
        }
        // And once the delay has elapsed it restarts.
        assert_eq!(decide(2, S(20), S(1)), BackoffDecision::Restart);
        assert_eq!(decide(2, S(21), S(1)), BackoffDecision::Restart);
    }

    #[test]
    fn a_container_that_stayed_up_long_enough_is_forgiven() {
        // Without the reset, a pod that crashed once at boot would still be
        // waiting five minutes to restart a week later.
        assert_eq!(
            decide(10, Duration::ZERO, RESET_AFTER),
            BackoffDecision::Restart
        );
        // One second short of the threshold is NOT forgiven.
        assert!(matches!(
            decide(10, Duration::ZERO, RESET_AFTER - S(1)),
            BackoffDecision::Wait { .. }
        ));
    }

    #[test]
    fn the_measured_incident_would_now_be_visible() {
        // cid 2026-08-29: a container exiting every 180s, restarted 160
        // times, reported Running the whole way. With backoff it enters
        // CrashLoopBackOff and kubectl says so.
        let d = decide(160, S(1), S(180));
        assert_eq!(d.waiting_reason(), Some("CrashLoopBackOff"));
        // 180s uptime is well under the 600s forgiveness threshold, so the
        // restart count keeps mattering — which is the point.
        assert!(matches!(d, BackoffDecision::Wait { .. }));
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
        assert_eq!(delay_for(u32::MAX), MAX_DELAY);
        assert_eq!(
            decide_start(u32::MAX, MAX_DELAY),
            BackoffDecision::Restart,
            "a capped penalty still expires; backoff never becomes give-up"
        );
    }
}
