//! freshness — how old an observation is, and what that says about a child.
//!
//! ★ WHY THIS EXISTS. engenho's health endpoints returned the constant
//! `"ok"`. On plo (2026-09-06) a controller retried hot enough to peg a core
//! and hang every API read while `/healthz` said ok; on ryn (2026-09-18) the
//! kubelet controller was retired for 12 hours and the only evidence was a
//! tick line that stopped appearing. Both failures were visible in data the
//! process already had (when each child last did anything) and invisible in
//! what it reported. This module is the judge that turns that data into an
//! answer: [`Freshness`] for one observation, [`Liveness`] for one child.
//!
//! ★ THE JUDGE IS PURE. [`Freshness::judge`] and [`Liveness::judge`] take
//! the observation, the current instant and the window as arguments and
//! read no clock, so every row of their truth tables is a plain unit test.
//! The caller supplies `now` from a [`crate::relogio::Clock`].
//!
//! ★ NEVER OBSERVED IS ITS OWN STATE. A child that has not reported yet is
//! [`Freshness::NeverObserved`] / [`Liveness::Unknown`], not fresh and not
//! stale. A health endpoint that folded it into either would be the constant
//! answer again, just computed. The one question every consumer asks,
//! "may I render this as ok?", is [`Liveness::is_alive`], and only
//! [`Liveness::Alive`] answers yes.
//!
//! ★ A ZERO WINDOW CANNOT BE BUILT. [`StaleAfter`] has no public field and
//! its constructors refuse a window shorter than the clock's resolution (one
//! millisecond), which would judge every observation stale the instant after
//! it was made.
//!
//! Clock caveat (only-mitigated): judge beats and `now` from ONE clock. A
//! [`crate::relogio::HlcClock`] never goes backwards, so the judge's
//! saturating age cannot run negative; but after a wall-clock step backwards
//! it holds its physical time until the wall catches up, and for that span a
//! stalled child reads fresh. An observation stamped after `now` (two clocks
//! mixed) reads [`Freshness::Fresh`]: the judge never manufactures staleness
//! from clock skew, and never manufactures freshness for longer than the
//! skew.

use std::fmt;
use std::time::Duration;

use crate::relogio::Instant;

/// How long an observation stays fresh.
///
/// At least one millisecond, the resolution of [`Instant`]; there is no way
/// to build a shorter one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StaleAfter {
    window_ms: u64,
}

/// A freshness window shorter than the clock's one-millisecond resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a freshness window must be at least 1ms (got {requested:?})")]
pub struct WindowTooShort {
    /// The window that was asked for.
    pub requested: Duration,
}

impl StaleAfter {
    /// A window of `window`, refused when it is shorter than 1ms.
    ///
    /// # Errors
    ///
    /// [`WindowTooShort`] when `window` rounds down to zero milliseconds.
    pub fn new(window: Duration) -> Result<Self, WindowTooShort> {
        // Saturate: a window past u64::MAX ms (~584 million years) is
        // "never stale" in every practical sense.
        let window_ms = u64::try_from(window.as_millis()).unwrap_or(u64::MAX);
        if window_ms == 0 {
            return Err(WindowTooShort { requested: window });
        }
        Ok(Self { window_ms })
    }

    /// A window of `secs` whole seconds. Infallible: the type forbids zero.
    #[must_use]
    pub const fn from_secs(secs: std::num::NonZeroU64) -> Self {
        Self {
            window_ms: secs.get().saturating_mul(1000),
        }
    }

    /// The window as a [`Duration`].
    #[must_use]
    pub const fn get(self) -> Duration {
        Duration::from_millis(self.window_ms)
    }
}

/// How old one observation is, judged against a [`StaleAfter`] window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Freshness {
    /// Nothing has been observed yet.
    NeverObserved,
    /// The last observation is no older than the window.
    Fresh,
    /// The last observation is older than the window.
    Stale {
        /// When the last observation was made.
        since: Instant,
    },
}

impl Freshness {
    /// Judge the observation made at `last` (if any) as of `now`.
    ///
    /// An observation exactly `window` old is still fresh; one millisecond
    /// more is stale.
    #[must_use]
    pub fn judge(last: Option<Instant>, now: Instant, window: StaleAfter) -> Self {
        match last {
            None => Self::NeverObserved,
            Some(at) => {
                let age_ms = now.physical_ms.saturating_sub(at.physical_ms);
                if age_ms > window.window_ms {
                    Self::Stale { since: at }
                } else {
                    Self::Fresh
                }
            }
        }
    }

    /// The stable lowercase name, for logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NeverObserved => "never_observed",
            Self::Fresh => "fresh",
            Self::Stale { .. } => "stale",
        }
    }
}

impl fmt::Display for Freshness {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stale { since } => write!(f, "stale since {since}"),
            other => f.write_str(other.as_str()),
        }
    }
}

/// Whether the task behind a child is still running.
///
/// Observed by whoever holds the child's join handle: a supervised child
/// whose task has returned has, by construction, panicked or been aborted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskState {
    /// The task has not returned.
    Running,
    /// The task has returned (panicked or aborted).
    Ended,
}

/// One child's liveness, derived from what was observed of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Liveness {
    /// The child has not reported since it was spawned.
    Unknown,
    /// The child reported within its window.
    Alive,
    /// The child is running but has not reported within its window.
    Stalled {
        /// When it last reported.
        since: Instant,
    },
    /// The child's task has ended. Nothing re-runs it.
    Dead,
}

impl Liveness {
    /// Judge a child from its task state and its last heartbeat.
    ///
    /// An ended task is [`Liveness::Dead`] whatever its heartbeat says: a
    /// fresh beat from a task that has since returned is history, not life.
    /// Otherwise the heartbeat's [`Freshness`] decides.
    #[must_use]
    pub fn judge(
        task: TaskState,
        last_beat: Option<Instant>,
        now: Instant,
        window: StaleAfter,
    ) -> Self {
        match task {
            TaskState::Ended => Self::Dead,
            TaskState::Running => Freshness::judge(last_beat, now, window).into(),
        }
    }

    /// Whether a health endpoint may render this child as ok. Only
    /// [`Liveness::Alive`] may.
    #[must_use]
    pub const fn is_alive(self) -> bool {
        matches!(self, Self::Alive)
    }

    /// Whether the child has been observed at all: every state except
    /// [`Liveness::Unknown`]. Readiness asks this ("has it booted?");
    /// liveness asks [`Self::is_alive`].
    #[must_use]
    pub const fn is_observed(self) -> bool {
        !matches!(self, Self::Unknown)
    }

    /// The stable lowercase name, for logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Alive => "alive",
            Self::Stalled { .. } => "stalled",
            Self::Dead => "dead",
        }
    }
}

impl From<Freshness> for Liveness {
    fn from(f: Freshness) -> Self {
        match f {
            Freshness::NeverObserved => Self::Unknown,
            Freshness::Fresh => Self::Alive,
            Freshness::Stale { since } => Self::Stalled { since },
        }
    }
}

impl fmt::Display for Liveness {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stalled { since } => write!(f, "stalled since {since}"),
            other => f.write_str(other.as_str()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU64;

    fn window(secs: u64) -> StaleAfter {
        StaleAfter::from_secs(NonZeroU64::new(secs).expect("test windows are non-zero"))
    }

    fn at(ms: u64) -> Instant {
        Instant::from_ms(ms)
    }

    #[test]
    fn nothing_observed_is_its_own_state_not_fresh_and_not_stale() {
        assert_eq!(
            Freshness::judge(None, at(1_000_000), window(30)),
            Freshness::NeverObserved
        );
    }

    #[test]
    fn an_observation_inside_the_window_is_fresh() {
        assert_eq!(
            Freshness::judge(Some(at(1_000)), at(1_000), window(30)),
            Freshness::Fresh
        );
        assert_eq!(
            Freshness::judge(Some(at(1_000)), at(31_000), window(30)),
            Freshness::Fresh,
            "exactly the window old is still fresh"
        );
    }

    #[test]
    fn one_millisecond_past_the_window_is_stale_and_says_since_when() {
        assert_eq!(
            Freshness::judge(Some(at(1_000)), at(31_001), window(30)),
            Freshness::Stale { since: at(1_000) }
        );
    }

    #[test]
    fn an_observation_from_the_future_reads_fresh_rather_than_negative() {
        // Two clocks mixed: never manufacture staleness from skew.
        assert_eq!(
            Freshness::judge(Some(at(50_000)), at(1_000), window(30)),
            Freshness::Fresh
        );
    }

    #[test]
    fn a_window_under_the_clock_resolution_cannot_be_built() {
        assert_eq!(
            StaleAfter::new(Duration::ZERO),
            Err(WindowTooShort {
                requested: Duration::ZERO
            })
        );
        assert!(StaleAfter::new(Duration::from_micros(999)).is_err());
        assert_eq!(
            StaleAfter::new(Duration::from_millis(1)).map(StaleAfter::get),
            Ok(Duration::from_millis(1))
        );
        assert_eq!(window(30).get(), Duration::from_secs(30));
    }

    #[test]
    fn liveness_follows_freshness_while_the_task_runs() {
        let w = window(30);
        assert_eq!(
            Liveness::judge(TaskState::Running, None, at(5_000), w),
            Liveness::Unknown
        );
        assert_eq!(
            Liveness::judge(TaskState::Running, Some(at(5_000)), at(6_000), w),
            Liveness::Alive
        );
        assert_eq!(
            Liveness::judge(TaskState::Running, Some(at(5_000)), at(40_000), w),
            Liveness::Stalled { since: at(5_000) }
        );
    }

    #[test]
    fn an_ended_task_is_dead_whatever_its_last_beat_says() {
        let w = window(30);
        for last in [None, Some(at(5_000))] {
            assert_eq!(
                Liveness::judge(TaskState::Ended, last, at(5_001), w),
                Liveness::Dead
            );
        }
    }

    #[test]
    fn only_alive_may_render_as_ok_and_only_unknown_is_unobserved() {
        let stalled = Liveness::Stalled { since: at(1) };
        let all = [Liveness::Unknown, Liveness::Alive, stalled, Liveness::Dead];
        let alive: Vec<bool> = all.iter().map(|l| l.is_alive()).collect();
        assert_eq!(alive, [false, true, false, false]);
        let observed: Vec<bool> = all.iter().map(|l| l.is_observed()).collect();
        assert_eq!(observed, [false, true, true, true]);
    }

    #[test]
    fn display_names_the_state_and_when_a_stall_began() {
        assert_eq!(Liveness::Unknown.to_string(), "unknown");
        assert_eq!(
            Liveness::Stalled { since: at(7) }.to_string(),
            "stalled since 7.00000"
        );
        assert_eq!(
            Freshness::Stale { since: at(7) }.to_string(),
            "stale since 7.00000"
        );
        assert_eq!(Freshness::NeverObserved.to_string(), "never_observed");
    }
}
