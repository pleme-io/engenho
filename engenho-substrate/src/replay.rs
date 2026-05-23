//! replay — typed cursor over event streams + máquina state reconstruction.
//!
//! Per the research brief — seventh inventive primitive. Composes
//! máquina + linhagem-aberta: an event stream can be replayed
//! through a `StateMachine` to deterministically reconstruct any
//! historical state — operators debug by bisecting cursors.
//!
//! ## Surface
//!
//!   - `ReplayCursor<E>` — typed cursor (position + bounds + skip /
//!     rewind / peek / reset)
//!   - `replay_into<M>` — drives a `MachineRunner<M>` through an
//!     event stream; returns count consumed or first machine error
//!   - `replay_bisect` — split-then-step pattern for finding the
//!     event that triggered a specific state predicate
//!
//! Two key properties:
//!   - Cursor positions form a deterministic sequence given the
//!     event Vec; no hidden state mutation
//!   - Replay through `MachineRunner` is deterministic by máquina's
//!     own determinism contract — same events, same `FrozenClock`,
//!     byte-identical state and history

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::maquina::{MachineError, MachineRunner, StateMachine};
use crate::named::Named;

/// Typed cursor over an event stream.
///
/// Lock-free; `next` uses `fetch_add` so multi-producer scenarios
/// each get unique positions (one-shot delivery semantics).
pub struct ReplayCursor<E: Clone> {
    events: Arc<[E]>,
    pos: AtomicUsize,
    name: &'static str,
}

impl<E: Clone> ReplayCursor<E> {
    /// New cursor over the given event sequence. Position starts at 0.
    pub fn new<I: IntoIterator<Item = E>>(name: &'static str, events: I) -> Self {
        let v: Vec<E> = events.into_iter().collect();
        Self {
            events: v.into(),
            pos: AtomicUsize::new(0),
            name,
        }
    }

    /// Advance and return the next event, or `None` if done. Atomic.
    pub fn next(&self) -> Option<E> {
        let p = self.pos.fetch_add(1, Ordering::AcqRel);
        if p >= self.events.len() {
            // Saturate back to len so position() reports accurately.
            self.pos.store(self.events.len(), Ordering::Release);
            None
        } else {
            Some(self.events[p].clone())
        }
    }

    /// Peek at the next event without advancing.
    pub fn peek(&self) -> Option<E> {
        let p = self.pos.load(Ordering::Acquire);
        self.events.get(p).cloned()
    }

    /// Reset to position 0.
    pub fn reset(&self) {
        self.pos.store(0, Ordering::Release);
    }

    /// Current position (0..=len).
    pub fn position(&self) -> usize {
        self.pos.load(Ordering::Acquire).min(self.events.len())
    }

    /// Total event count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Remaining events from the current position.
    pub fn remaining(&self) -> usize {
        self.len().saturating_sub(self.position())
    }

    /// True if cursor is exhausted.
    pub fn is_done(&self) -> bool {
        self.remaining() == 0
    }

    /// True if cursor has not been advanced.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Skip forward up to `n` positions. Returns the actual count skipped
    /// (clamped to remaining).
    pub fn skip(&self, n: usize) -> usize {
        let curr = self.position();
        let target = (curr + n).min(self.events.len());
        let delta = target - curr;
        self.pos.store(target, Ordering::Release);
        delta
    }

    /// Rewind up to `n` positions. Returns the actual count rewound
    /// (clamped to current position).
    pub fn rewind(&self, n: usize) -> usize {
        let curr = self.position();
        let target = curr.saturating_sub(n);
        let delta = curr - target;
        self.pos.store(target, Ordering::Release);
        delta
    }
}

impl<E: Clone> Named for ReplayCursor<E> {
    fn name(&self) -> &'static str {
        self.name
    }
}

/// Drive a `MachineRunner<M>` through every remaining event in
/// `cursor`. Stops on first machine error OR when cursor is
/// exhausted; returns the number of events actually applied.
///
/// # Errors
/// Returns `MachineError<M::Err>` from the first step that fails;
/// remaining events are NOT consumed (cursor stops at the failing
/// position).
pub fn replay_into<M: StateMachine>(
    runner: &mut MachineRunner<M>,
    cursor: &ReplayCursor<M::Event>,
) -> Result<usize, MachineError<M::Err>>
where
    M::Event: Clone,
{
    let mut applied = 0;
    while let Some(event) = cursor.peek() {
        runner.step(event)?;
        // Only advance after the step succeeds, so failures leave the
        // cursor on the offending event for inspection.
        cursor.next();
        applied += 1;
    }
    Ok(applied)
}

/// Bisect: replay through `cursor` until `predicate(state)` becomes
/// true. Returns the (position, `transition_count`) at which the
/// predicate fired, or `None` if it never did.
///
/// # Errors
/// Returns `MachineError<M::Err>` if any intermediate step fails.
pub fn replay_until<M: StateMachine>(
    runner: &mut MachineRunner<M>,
    cursor: &ReplayCursor<M::Event>,
    predicate: impl Fn(&M::State) -> bool,
) -> Result<Option<usize>, MachineError<M::Err>>
where
    M::Event: Clone,
{
    if predicate(runner.state()) {
        return Ok(Some(0));
    }
    let mut steps = 0;
    while let Some(event) = cursor.peek() {
        runner.step(event)?;
        cursor.next();
        steps += 1;
        if predicate(runner.state()) {
            return Ok(Some(steps));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::impl_error_kind;
    use crate::maquina::{MachineRunner, StateMachine};
    use crate::relogio::FrozenClock;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use thiserror::Error;

    // ── Cursor primitive tests ──────────────────────────────────

    #[test]
    fn empty_cursor_returns_none() {
        let c: ReplayCursor<u32> = ReplayCursor::new("test", vec![]);
        assert_eq!(c.next(), None);
        assert!(c.is_done());
        assert!(c.is_empty());
        assert_eq!(c.len(), 0);
        assert_eq!(c.remaining(), 0);
    }

    #[test]
    fn next_advances_position() {
        let c = ReplayCursor::new("test", vec![1, 2, 3]);
        assert_eq!(c.position(), 0);
        assert_eq!(c.next(), Some(1));
        assert_eq!(c.position(), 1);
        assert_eq!(c.next(), Some(2));
        assert_eq!(c.next(), Some(3));
        assert_eq!(c.next(), None);
        assert_eq!(c.position(), 3);
    }

    #[test]
    fn peek_does_not_advance() {
        let c = ReplayCursor::new("test", vec![1, 2, 3]);
        assert_eq!(c.peek(), Some(1));
        assert_eq!(c.peek(), Some(1));
        assert_eq!(c.position(), 0);
        let _ = c.next();
        assert_eq!(c.peek(), Some(2));
    }

    #[test]
    fn reset_returns_to_zero() {
        let c = ReplayCursor::new("test", vec![1, 2, 3]);
        let _ = c.next();
        let _ = c.next();
        c.reset();
        assert_eq!(c.position(), 0);
        assert_eq!(c.next(), Some(1));
    }

    #[test]
    fn skip_clamps_to_len() {
        let c = ReplayCursor::new("test", vec![1, 2, 3]);
        let skipped = c.skip(10);
        assert_eq!(skipped, 3);
        assert_eq!(c.position(), 3);
        assert!(c.is_done());
    }

    #[test]
    fn rewind_clamps_to_zero() {
        let c = ReplayCursor::new("test", vec![1, 2, 3]);
        c.skip(2);
        let rewound = c.rewind(5);
        assert_eq!(rewound, 2);
        assert_eq!(c.position(), 0);
    }

    #[test]
    fn remaining_decreases_with_next() {
        let c = ReplayCursor::new("test", vec![10, 20, 30, 40]);
        assert_eq!(c.remaining(), 4);
        let _ = c.next();
        assert_eq!(c.remaining(), 3);
        c.skip(2);
        assert_eq!(c.remaining(), 1);
        let _ = c.next();
        assert_eq!(c.remaining(), 0);
    }

    #[test]
    fn named_works() {
        let c: ReplayCursor<u32> = ReplayCursor::new("ev-stream", vec![]);
        assert_eq!(c.name(), "ev-stream");
    }

    // ── replay_into with a Sum machine ──────────────────────────

    #[derive(Default)]
    struct SumMachine;
    crate::define_named!(SumMachine, "sum");

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    enum SumState {
        Active(i64),
        Capped,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    enum SumEvent {
        Add(i64),
    }

    #[derive(Debug, Clone, Error)]
    enum SumErr {
        #[error("overflow")]
        Overflow,
    }
    impl_error_kind! {
        SumErr {
            Overflow => "overflow",
        }
    }

    impl StateMachine for SumMachine {
        type State = SumState;
        type Event = SumEvent;
        type Effect = i64;
        type Err = SumErr;

        fn initial() -> SumState {
            SumState::Active(0)
        }

        fn step(state: &SumState, event: &SumEvent) -> Result<(SumState, i64), SumErr> {
            match (state, event) {
                (SumState::Active(n), SumEvent::Add(by)) => {
                    let next = n.checked_add(*by).ok_or(SumErr::Overflow)?;
                    Ok((SumState::Active(next), next))
                }
                (SumState::Capped, _) => Err(SumErr::Overflow),
            }
        }

        fn is_terminal(state: &SumState) -> bool {
            matches!(state, SumState::Capped)
        }
    }

    #[test]
    fn replay_into_consumes_all_events() {
        let mut runner = MachineRunner::<SumMachine>::new(Arc::new(FrozenClock::at(0)));
        let cursor = ReplayCursor::new(
            "test",
            vec![SumEvent::Add(1), SumEvent::Add(2), SumEvent::Add(3)],
        );
        let applied = replay_into(&mut runner, &cursor).unwrap();
        assert_eq!(applied, 3);
        assert_eq!(runner.state(), &SumState::Active(6));
        assert!(cursor.is_done());
    }

    #[test]
    fn replay_into_stops_on_machine_error() {
        let mut runner = MachineRunner::<SumMachine>::new(Arc::new(FrozenClock::at(0)));
        runner.step(SumEvent::Add(i64::MAX - 1)).unwrap();
        let cursor = ReplayCursor::new(
            "test",
            vec![SumEvent::Add(1), SumEvent::Add(100), SumEvent::Add(200)],
        );
        let err = replay_into(&mut runner, &cursor).unwrap_err();
        match err {
            MachineError::Step(SumErr::Overflow) => {}
            _ => panic!("expected Overflow"),
        }
        // Cursor stopped at the offending event (Add(100)); Add(1) consumed first.
        assert_eq!(cursor.position(), 1);
    }

    #[test]
    fn replay_until_finds_predicate() {
        let mut runner = MachineRunner::<SumMachine>::new(Arc::new(FrozenClock::at(0)));
        let cursor = ReplayCursor::new(
            "test",
            vec![
                SumEvent::Add(1),
                SumEvent::Add(2),
                SumEvent::Add(3),
                SumEvent::Add(4),
                SumEvent::Add(5),
            ],
        );
        // Stop when sum reaches 6.
        let found = replay_until(
            &mut runner,
            &cursor,
            |s| matches!(s, SumState::Active(n) if *n >= 6),
        )
        .unwrap();
        assert_eq!(found, Some(3)); // 1+2+3 = 6 at step 3
        assert_eq!(runner.state(), &SumState::Active(6));
        assert_eq!(cursor.position(), 3);
    }

    #[test]
    fn replay_until_returns_none_when_predicate_never_fires() {
        let mut runner = MachineRunner::<SumMachine>::new(Arc::new(FrozenClock::at(0)));
        let cursor = ReplayCursor::new("test", vec![SumEvent::Add(1), SumEvent::Add(2)]);
        let found = replay_until(
            &mut runner,
            &cursor,
            |s| matches!(s, SumState::Active(n) if *n >= 1000),
        )
        .unwrap();
        assert_eq!(found, None);
        // Cursor exhausted.
        assert!(cursor.is_done());
    }

    #[test]
    fn replay_until_zero_steps_when_predicate_already_true() {
        let mut runner = MachineRunner::<SumMachine>::new(Arc::new(FrozenClock::at(0)));
        let cursor = ReplayCursor::new("test", vec![SumEvent::Add(1)]);
        let found = replay_until(&mut runner, &cursor, |_| true).unwrap();
        assert_eq!(found, Some(0));
        // Cursor unaffected.
        assert_eq!(cursor.position(), 0);
    }
}
