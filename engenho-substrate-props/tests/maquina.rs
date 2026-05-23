//! Property: máquina StateMachine + MachineRunner invariants.

use engenho_substrate::{
    define_named, impl_error_kind, FrozenClock, MachineRunner, StateMachine,
};
use proptest::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

// ── Counter machine: increments u32, terminal at MAX ─────────

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CounterState {
    Active(u32),
    Capped,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CounterEvent {
    Inc(u32),
    Reset,
}

#[derive(Debug, Clone, Error)]
pub enum CounterErr {
    #[error("overflow")]
    Overflow,
    #[error("invalid: {0}")]
    Invalid(String),
}

impl_error_kind! {
    CounterErr {
        Overflow => "overflow",
        (Invalid(_)) => "invalid",
    }
}

#[derive(Default)]
pub struct CounterMachine;
define_named!(CounterMachine, "counter");

const CAP: u32 = 100;

impl StateMachine for CounterMachine {
    type State = CounterState;
    type Event = CounterEvent;
    type Effect = u32;  // current count after step
    type Err = CounterErr;

    fn initial() -> CounterState {
        CounterState::Active(0)
    }

    fn step(state: &CounterState, event: &CounterEvent) -> Result<(CounterState, u32), CounterErr> {
        match (state, event) {
            (CounterState::Active(n), CounterEvent::Inc(by)) => {
                let next = n.saturating_add(*by);
                if next >= CAP {
                    Ok((CounterState::Capped, CAP))
                } else {
                    Ok((CounterState::Active(next), next))
                }
            }
            (CounterState::Active(_), CounterEvent::Reset) => {
                Ok((CounterState::Active(0), 0))
            }
            (CounterState::Capped, _) => Err(CounterErr::Invalid("capped".into())),
        }
    }

    fn is_terminal(state: &CounterState) -> bool {
        matches!(state, CounterState::Capped)
    }
}

fn make_runner() -> MachineRunner<CounterMachine> {
    MachineRunner::new(Arc::new(FrozenClock::at(0)))
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(256),
        ..ProptestConfig::default()
    })]

    /// Pure step: same (state, event) → same (state', effect).
    #[test]
    fn step_is_pure_and_deterministic(
        n in 0u32..CAP,
        by in 0u32..50,
    ) {
        let state = CounterState::Active(n);
        let event = CounterEvent::Inc(by);
        let (s1, e1) = CounterMachine::step(&state, &event).unwrap();
        let (s2, e2) = CounterMachine::step(&state, &event).unwrap();
        prop_assert_eq!(s1, s2);
        prop_assert_eq!(e1, e2);
    }

    /// Successful step increments history exactly by 1.
    #[test]
    fn successful_step_appends_one_history_entry(
        by in 1u32..30,
    ) {
        let mut r = make_runner();
        let prev_count = r.step_count();
        r.step(CounterEvent::Inc(by)).unwrap();
        prop_assert_eq!(r.step_count(), prev_count + 1);
    }

    /// Failed step leaves state + history unchanged.
    #[test]
    fn failed_step_doesnt_mutate(
        _seed in any::<u8>(),
    ) {
        let mut r = make_runner();
        // Drive to capped.
        r.step(CounterEvent::Inc(CAP)).unwrap();
        let cap_count = r.step_count();
        let cap_state = r.state().clone();
        // Now any step is invalid.
        let _ = r.step(CounterEvent::Inc(1)).unwrap_err();
        prop_assert_eq!(r.step_count(), cap_count);
        prop_assert_eq!(r.state(), &cap_state);
    }

    /// Terminal state is reached after enough increments.
    #[test]
    fn terminal_reached_after_cap_increment(
        increments in proptest::collection::vec(1u32..50, 3..32),
    ) {
        let mut r = make_runner();
        let mut total = 0u32;
        for inc in &increments {
            if r.is_terminal() {
                break;
            }
            r.step(CounterEvent::Inc(*inc)).unwrap();
            total = total.saturating_add(*inc);
        }
        // Either we hit the cap (total >= CAP → terminal) OR we didn't.
        if total >= CAP {
            prop_assert!(r.is_terminal());
        } else {
            prop_assert!(!r.is_terminal());
        }
    }

    /// Reset returns to initial state + empties history.
    #[test]
    fn reset_returns_to_initial(
        n_steps in 0usize..16,
    ) {
        let mut r = make_runner();
        for _ in 0..n_steps {
            if r.is_terminal() {
                break;
            }
            let _ = r.step(CounterEvent::Inc(1));
        }
        r.reset();
        prop_assert_eq!(r.state(), &CounterState::Active(0));
        prop_assert_eq!(r.step_count(), 0);
    }

    /// History is append-only — never truncated by success.
    #[test]
    fn history_is_append_only(
        increments in proptest::collection::vec(1u32..10, 1..8),
    ) {
        let mut r = make_runner();
        let mut expected_count = 0;
        for inc in &increments {
            if r.is_terminal() {
                break;
            }
            let _ = r.step(CounterEvent::Inc(*inc));
            expected_count += 1;
        }
        prop_assert_eq!(r.step_count(), expected_count);
        // Every history entry's `to` matches the next entry's `from`.
        let h = r.history();
        for i in 1..h.len() {
            prop_assert_eq!(&h[i - 1].to, &h[i].from);
        }
    }

    /// TransitionRecord.at is monotonic when clock advances.
    #[test]
    fn transition_at_is_monotonic_under_advancing_clock(
        n_steps in 2usize..16,
        step_ms in 1u64..100,
    ) {
        let clock = Arc::new(FrozenClock::at(0));
        let mut r = MachineRunner::<CounterMachine>::new(clock.clone());
        for _ in 0..n_steps {
            if r.is_terminal() {
                break;
            }
            r.step(CounterEvent::Inc(1)).unwrap();
            clock.advance(step_ms);
        }
        let h = r.history();
        for i in 1..h.len() {
            prop_assert!(h[i].at > h[i - 1].at);
        }
    }
}
