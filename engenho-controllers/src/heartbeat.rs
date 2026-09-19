//! `Heartbeat` — what a supervised child last did, readable from outside
//! without a lock.
//!
//! One per [`crate::WatchDriver`], and one per runtime listener. The child
//! writes it; the runtime's supervisor and (from T2.8) the liveness
//! projection read it. Every field is an atomic, so a reader never blocks a
//! tick and a tick never waits for a reader.
//!
//! It records FACTS, not a verdict: when a tick last started and ended, how
//! the last one ended, and how many panics the supervisor saw. Turning those
//! into Alive / Stalled / Dead is the liveness projection's job, which is why
//! there is no `is_healthy` here — a heartbeat that has never beaten reads as
//! "never", not as "fine".
//!
//! ## Why it exists
//!
//! On ryn (2026-09-18) the kubelet's driver stopped ticking for 12 hours and
//! the only evidence was the ABSENCE of a log line among three healthy
//! controllers. Nothing in the process could say when a given controller last
//! finished a tick. This is that record.
//!
//! ## Consistency
//!
//! Each field is individually consistent; a [`Beat`] is not a transaction. A
//! snapshot taken while a tick starts may see the new start count with the
//! previous start time. That is fine for what reads it — ages measured in
//! seconds — and it is why the counts are written LAST (release) and read
//! FIRST (acquire): a reader that sees a count also sees the time written
//! before it.

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use shigoto_types::failure::FailureKind;
use tokio::time::Instant;

use crate::controller::ReconcileOutcome;
use crate::error::ControllerError;

crate::closed_enum! {
    /// How one tick of a child ended.
    ///
    /// Stored in a [`Heartbeat`] as a one-byte code, `0` meaning "never", so
    /// a child that has not finished a tick has no class at all rather than a
    /// default one that reads as success.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum TickClass {
        /// `Ok` + `ReconcileResult::Done`.
        Done,
        /// `Ok` + a requeue.
        Requeued,
        /// `Err`, classified Transient: it will be retried on the curve.
        Transient,
        /// `Err`, classified Declarative: it is surfaced, not retried.
        Declarative,
        /// `Err` in a failure class this crate does not know yet
        /// (`FailureKind` is `#[non_exhaustive]`). Recorded as unknown
        /// rather than rounded to either known class.
        Unclassified,
        /// A child that is not a tick loop — a listener — stopped doing its
        /// work: its bind failed or its server returned. It rebinds on a
        /// growing backoff (T2.7). A listener serves while its heartbeat is
        /// in flight; `Halted` with nothing in flight is one waiting to
        /// rebind, and liveness must read that as not-ok.
        Halted,
        /// The tick panicked and the driver contained it (T2.7): only a
        /// Stateless child's tick is contained. Counted as a panic, and
        /// re-ticked by the next event or the fallback — never by a
        /// targeted retry. Its own class so a bug is never read as a
        /// malformed declaration.
        Panicked,
    }
}

impl TickClass {
    /// Classify one controller tick's result.
    #[must_use]
    pub fn of(result: &Result<ReconcileOutcome, ControllerError>) -> Self {
        match result {
            Ok(outcome) if outcome.result.requeue_after().is_some() => Self::Requeued,
            Ok(_) => Self::Done,
            Err(ControllerError::Panicked(_)) => Self::Panicked,
            Err(e) => match e.classify() {
                FailureKind::Transient => Self::Transient,
                FailureKind::Declarative => Self::Declarative,
                // `FailureKind` is `#[non_exhaustive]`: a class shigoto adds
                // later lands here and is recorded as exactly that — unknown.
                _ => Self::Unclassified,
            },
        }
    }

    /// The stored code: the discriminant plus one, so `0` is "never".
    const fn code(self) -> u8 {
        // A fieldless enum's discriminants are 0..N in declaration order.
        self as u8 + 1
    }

    /// Decode a stored code. `0` (never recorded) and any code past the
    /// catalog decode to `None`.
    fn from_code(code: u8) -> Option<Self> {
        let index = usize::from(code.checked_sub(1)?);
        Self::ALL.get(index).copied()
    }
}

/// The lock-free record of a child's ticks.
#[derive(Debug)]
pub struct Heartbeat {
    /// Every stored time is measured from here. `tokio::time::Instant`, so
    /// paused-time tests read the same clock the loop runs on.
    epoch: Instant,
    started: AtomicU64,
    finished: AtomicU64,
    /// Microseconds since `epoch`, plus one; `0` = never.
    last_start: AtomicU64,
    /// Microseconds since `epoch`, plus one; `0` = never.
    last_end: AtomicU64,
    /// [`TickClass::code`]; `0` = never.
    last_class: AtomicU8,
    panics: AtomicU64,
}

impl Default for Heartbeat {
    fn default() -> Self {
        Self::new()
    }
}

impl Heartbeat {
    /// A heartbeat that has never beaten.
    #[must_use]
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
            started: AtomicU64::new(0),
            finished: AtomicU64::new(0),
            last_start: AtomicU64::new(0),
            last_end: AtomicU64::new(0),
            last_class: AtomicU8::new(0),
            panics: AtomicU64::new(0),
        }
    }

    /// A tick (or, for a listener, its serve) has started.
    pub fn begin(&self) {
        self.last_start.store(self.stamp(), Ordering::Release);
        self.started.fetch_add(1, Ordering::Release);
    }

    /// The tick that last [`begin`](Self::begin)-ed has ended as `class`.
    ///
    /// A tick that ended [`TickClass::Panicked`] is also a panic this
    /// heartbeat counts: the class is the one decision, so a contained panic
    /// cannot be recorded without being counted.
    pub fn end(&self, class: TickClass) {
        if class == TickClass::Panicked {
            self.record_panic();
        }
        self.last_end.store(self.stamp(), Ordering::Release);
        self.last_class.store(class.code(), Ordering::Release);
        self.finished.fetch_add(1, Ordering::Release);
    }

    /// A panic in this child: its task's, seen by the supervisor, or a
    /// contained tick's, recorded by [`end`](Self::end).
    pub fn record_panic(&self) {
        self.panics.fetch_add(1, Ordering::Release);
    }

    /// What this heartbeat has recorded, as plain values.
    #[must_use]
    pub fn snapshot(&self) -> Beat {
        // Counts first (acquire): the times written before them are visible.
        let ticks_finished = self.finished.load(Ordering::Acquire);
        let ticks_started = self.started.load(Ordering::Acquire);
        Beat {
            ticks_started,
            ticks_finished,
            last_start: self.at(self.last_start.load(Ordering::Acquire)),
            last_end: self.at(self.last_end.load(Ordering::Acquire)),
            last_class: TickClass::from_code(self.last_class.load(Ordering::Acquire)),
            panics: self.panics.load(Ordering::Acquire),
        }
    }

    /// Now, as a stored stamp: microseconds since `epoch` plus one,
    /// saturating rather than wrapping.
    fn stamp(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_micros())
            .unwrap_or(u64::MAX)
            .saturating_add(1)
    }

    /// A stored stamp back to an instant; `0` is "never".
    fn at(&self, stamp: u64) -> Option<Instant> {
        let micros = stamp.checked_sub(1)?;
        self.epoch.checked_add(Duration::from_micros(micros))
    }
}

/// A [`Heartbeat`] read at one moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Beat {
    /// Ticks begun since the child was spawned.
    pub ticks_started: u64,
    /// Ticks ended since the child was spawned.
    pub ticks_finished: u64,
    /// When the last tick began; `None` if none has.
    pub last_start: Option<Instant>,
    /// When the last tick ended; `None` if none has.
    pub last_end: Option<Instant>,
    /// How the last tick ended; `None` if none has.
    pub last_class: Option<TickClass>,
    /// Panics observed in this child: contained ticks' and its task's.
    pub panics: u64,
}

impl Beat {
    /// A tick has begun and not ended. For a tick loop that is a tick in
    /// progress (or one that will never end, if the task died inside it);
    /// for a listener it is the serving state.
    #[must_use]
    pub const fn in_flight(&self) -> bool {
        self.ticks_started > self.ticks_finished
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::{ReconcileReport, ReconcileResult};

    #[test]
    fn a_heartbeat_that_never_beat_says_never() {
        let beat = Heartbeat::new().snapshot();
        assert_eq!(beat.ticks_started, 0);
        assert_eq!(beat.ticks_finished, 0);
        assert_eq!(beat.last_start, None);
        assert_eq!(beat.last_end, None);
        assert_eq!(
            beat.last_class, None,
            "no tick has ended, so there is no class — not a default one"
        );
        assert!(!beat.in_flight());
    }

    #[tokio::test(start_paused = true)]
    async fn begin_and_end_record_times_counts_and_class() {
        let hb = Heartbeat::new();
        tokio::time::advance(Duration::from_secs(5)).await;
        hb.begin();
        let mid = hb.snapshot();
        assert!(mid.in_flight(), "a begun tick is in flight until it ends");
        assert_eq!(mid.last_end, None);

        tokio::time::advance(Duration::from_secs(2)).await;
        hb.end(TickClass::Transient);
        let after = hb.snapshot();
        assert!(!after.in_flight());
        assert_eq!((after.ticks_started, after.ticks_finished), (1, 1));
        assert_eq!(after.last_class, Some(TickClass::Transient));
        let (start, end) = (after.last_start.unwrap(), after.last_end.unwrap());
        assert_eq!(end - start, Duration::from_secs(2));
    }

    #[test]
    fn every_class_round_trips_through_its_stored_code() {
        for class in TickClass::ALL {
            assert_ne!(class.code(), 0, "{class:?}: 0 is reserved for never");
            assert_eq!(TickClass::from_code(class.code()), Some(*class));
        }
        let past = u8::try_from(TickClass::ALL.len() + 1).unwrap();
        assert_eq!(TickClass::from_code(0), None);
        assert_eq!(TickClass::from_code(past), None);
    }

    #[test]
    fn panics_are_counted() {
        let hb = Heartbeat::new();
        hb.record_panic();
        hb.record_panic();
        assert_eq!(hb.snapshot().panics, 2);
    }

    #[test]
    fn a_tick_result_classifies_by_outcome_and_failure_class() {
        let ok = |r| Ok(ReconcileOutcome::new(ReconcileReport::default(), r));
        assert_eq!(TickClass::of(&ok(ReconcileResult::Done)), TickClass::Done);
        assert_eq!(
            TickClass::of(&ok(ReconcileResult::Requeue(Duration::from_secs(1)))),
            TickClass::Requeued
        );
        assert_eq!(
            TickClass::of(&Err(ControllerError::Store(
                engenho_store::StoreError::ClientWriteFailed("no leader".into())
            ))),
            TickClass::Transient
        );
        assert_eq!(
            TickClass::of(&Err(ControllerError::InvalidResource("bad".into()))),
            TickClass::Declarative
        );
    }

    /// A contained panic classifies as a panic, not as the Declarative
    /// failure its retry class shares — a bug is never read as a malformed
    /// declaration.
    #[test]
    fn a_contained_panic_is_its_own_class() {
        let panicked = Err(ControllerError::Panicked(crate::PanicMessage::Opaque));
        assert_eq!(TickClass::of(&panicked), TickClass::Panicked);
    }

    /// Ending a tick as `Panicked` counts the panic; no other class does.
    #[test]
    fn a_tick_that_ended_in_a_panic_is_counted_as_one() {
        for class in TickClass::ALL {
            let hb = Heartbeat::new();
            hb.begin();
            hb.end(*class);
            let want = u64::from(*class == TickClass::Panicked);
            assert_eq!(hb.snapshot().panics, want, "{class:?}");
        }
    }
}
