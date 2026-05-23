//! máquina — typed FSM primitive.
//!
//! Per the research brief — MEDIUM-leverage inventive primitive.
//! The substrate has multiple ad-hoc state machines (Plantio
//! stage state, Transient lifecycle, QuorumOutcome enum, search
//! Ensaio status) hand-rolled with match statements + bare
//! enums. máquina folds them into one typed surface.
//!
//! ## Surface
//!
//!   - `StateMachine` trait — pure step function
//!     `(state, event) -> (state, effect)`
//!   - `MachineRunner<M>` — drives steps, holds current state,
//!     records every transition into a typed history Vec
//!   - `TransitionRecord<M>` — typed (from, event, to, at: Instant)
//!     for replay/audit
//!
//! ## What this collapses
//!
//! Today: every controller hand-rolls its FSM with bespoke enum
//! matching, scattered transition logic, no transition history.
//!
//! Tomorrow: every controller's FSM lifts into a `MachineRunner`,
//! gains free transition recording + replay + fingerprinting via
//! the existing trait surfaces.
//!
//! ## Composition
//!
//!   - `Named` supertrait → every machine has telemetry name
//!   - `relógio::Instant` for transition timestamps
//!   - `Fingerprint` over State + Event (caller supplies)
//!   - `ErrorKind` on M::Err

use serde::{Deserialize, Serialize};

use crate::relogio::Instant;

/// Typed finite-state-machine surface. Implementers supply:
///   - `State`: the machine's state (Clone + Serialize + Eq)
///   - `Event`: input events (Clone + Serialize)
///   - `Effect`: side-effects the step requests be performed
///     by the runner's consumer (telemetry, log line, action)
///   - `Err`: error type — must implement ErrorKind
///
/// The step function is PURE — no I/O, no side effects beyond
/// returning the Effect for the runner to dispatch. This is what
/// makes replay possible.
pub trait StateMachine: crate::named::Named + Default + Send + Sync {
    /// The state type — must be cloneable + comparable + serdeable.
    type State: Clone + Eq + Serialize + Send + Sync;

    /// The event type — input that drives transitions.
    type Event: Clone + Serialize + Send + Sync;

    /// The effect type — side-effects the runner's consumer
    /// dispatches (logging, telemetry, downstream calls).
    type Effect: Clone + Send + Sync;

    /// The error type — typed step failures (invalid event for
    /// current state, etc.).
    type Err: crate::error_kind::ErrorKind + Send + Sync + std::fmt::Debug;

    /// Initial state when the runner spawns.
    fn initial() -> Self::State;

    /// Pure step: given a state + event, return the next state +
    /// effect. Pure — no I/O, no side effects.
    ///
    /// # Errors
    /// [`Self::Err`] when the event isn't valid for the current state.
    fn step(
        state: &Self::State,
        event: &Self::Event,
    ) -> Result<(Self::State, Self::Effect), Self::Err>;

    /// True if `state` is a terminal state. The runner refuses to
    /// step from a terminal state.
    fn is_terminal(state: &Self::State) -> bool;
}

/// One typed record of a transition. Serializable for replay +
/// fingerprintable for cross-node attestation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitionRecord<S, E> {
    /// State the machine was in before the event.
    pub from: S,
    /// Event that drove the transition.
    pub event: E,
    /// State the machine ended up in.
    pub to: S,
    /// When the transition happened (clock-injected).
    pub at: Instant,
}

/// Runner that owns one machine + drives steps + accumulates
/// the transition history.
pub struct MachineRunner<M: StateMachine> {
    state: M::State,
    history: Vec<TransitionRecord<M::State, M::Event>>,
    clock: std::sync::Arc<dyn crate::relogio::Clock>,
    name: &'static str,
}

impl<M: StateMachine> MachineRunner<M> {
    /// New runner with the machine's initial state + a clock
    /// (operator picks `WallClock` / `FrozenClock` / etc).
    #[must_use]
    pub fn new(clock: std::sync::Arc<dyn crate::relogio::Clock>) -> Self {
        Self {
            state: M::initial(),
            history: Vec::new(),
            clock,
            name: <M as MachineNamed>::name_static(),
        }
    }

    /// New runner starting from an explicit state. Useful for
    /// reconstruction / replay tests.
    #[must_use]
    pub fn from_state(
        state: M::State,
        clock: std::sync::Arc<dyn crate::relogio::Clock>,
    ) -> Self {
        Self {
            state,
            history: Vec::new(),
            clock,
            name: <M as MachineNamed>::name_static(),
        }
    }

    /// Drive one step. Records the transition + returns the
    /// effect for the caller to dispatch.
    ///
    /// # Errors
    /// [`MachineError`] wrapping `M::Err` if the step fails OR
    /// `MachineError::Terminal` if attempted from a terminal state.
    pub fn step(&mut self, event: M::Event) -> Result<M::Effect, MachineError<M::Err>> {
        if M::is_terminal(&self.state) {
            return Err(MachineError::Terminal);
        }
        let (next_state, effect) =
            M::step(&self.state, &event).map_err(MachineError::Step)?;
        let from = self.state.clone();
        self.history.push(TransitionRecord {
            from,
            event,
            to: next_state.clone(),
            at: self.clock.now(),
        });
        self.state = next_state;
        Ok(effect)
    }

    /// Current state (snapshot).
    #[must_use]
    pub fn state(&self) -> &M::State {
        &self.state
    }

    /// True when current state is terminal.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        M::is_terminal(&self.state)
    }

    /// Full transition history (cloneable for replay).
    #[must_use]
    pub fn history(&self) -> &[TransitionRecord<M::State, M::Event>] {
        &self.history
    }

    /// Machine's telemetry name.
    #[must_use]
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Step count (number of transitions recorded).
    #[must_use]
    pub fn step_count(&self) -> usize {
        self.history.len()
    }

    /// Reset to initial state + clear history. Useful for tests.
    pub fn reset(&mut self) {
        self.state = M::initial();
        self.history.clear();
    }
}

/// Errors a machine runner can surface.
#[derive(Debug, thiserror::Error)]
pub enum MachineError<E: crate::error_kind::ErrorKind + std::fmt::Debug> {
    /// Wrapped step error from the machine's typed err.
    #[error("step: {0:?}")]
    Step(E),
    /// Step attempted from a terminal state.
    #[error("terminal: machine cannot step from a terminal state")]
    Terminal,
}

impl<E: crate::error_kind::ErrorKind + std::fmt::Debug> crate::error_kind::ErrorKind
    for MachineError<E>
{
    fn kind(&self) -> &'static str {
        match self {
            Self::Step(_) => "step",
            Self::Terminal => "terminal",
        }
    }
}

/// Helper trait so `MachineRunner::new` can read the machine's
/// telemetry name without an instance. Auto-implemented for any
/// `Named + Default` type.
pub trait MachineNamed: crate::named::Named {
    /// Static name lookup — no instance needed.
    fn name_static() -> &'static str;
}

impl<T: crate::named::Named + Default> MachineNamed for T {
    fn name_static() -> &'static str {
        let instance = Self::default();
        instance.name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relogio::FrozenClock;
    use std::sync::Arc;
    use thiserror::Error;

    // ── Sample machine: door (Open / Closed / Locked) ──────────

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub enum DoorState {
        Open,
        Closed,
        Locked,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub enum DoorEvent {
        OpenIt,
        CloseIt,
        LockIt,
        UnlockIt,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum DoorEffect {
        Creak,
        Slam,
        Click,
    }

    #[derive(Debug, Clone, Error)]
    pub enum DoorErr {
        #[error("invalid transition from {state:?} on {event:?}")]
        Invalid { state: String, event: String },
    }

    crate::impl_error_kind! {
        DoorErr {
            { Invalid { .. } } => "invalid_transition",
        }
    }

    #[derive(Default)]
    pub struct DoorMachine;

    crate::define_named!(DoorMachine, "door");

    impl StateMachine for DoorMachine {
        type State = DoorState;
        type Event = DoorEvent;
        type Effect = DoorEffect;
        type Err = DoorErr;

        fn initial() -> DoorState {
            DoorState::Closed
        }

        fn step(state: &DoorState, event: &DoorEvent) -> Result<(DoorState, DoorEffect), DoorErr> {
            match (state, event) {
                (DoorState::Closed, DoorEvent::OpenIt) => Ok((DoorState::Open, DoorEffect::Creak)),
                (DoorState::Open, DoorEvent::CloseIt) => Ok((DoorState::Closed, DoorEffect::Slam)),
                (DoorState::Closed, DoorEvent::LockIt) => Ok((DoorState::Locked, DoorEffect::Click)),
                (DoorState::Locked, DoorEvent::UnlockIt) => Ok((DoorState::Closed, DoorEffect::Click)),
                _ => Err(DoorErr::Invalid {
                    state: format!("{state:?}"),
                    event: format!("{event:?}"),
                }),
            }
        }

        fn is_terminal(_state: &DoorState) -> bool {
            false  // door FSM has no terminals
        }
    }

    fn make_runner() -> MachineRunner<DoorMachine> {
        MachineRunner::new(Arc::new(FrozenClock::at(100)))
    }

    #[test]
    fn initial_state_is_closed() {
        let r = make_runner();
        assert_eq!(r.state(), &DoorState::Closed);
    }

    #[test]
    fn step_advances_state() {
        let mut r = make_runner();
        let effect = r.step(DoorEvent::OpenIt).unwrap();
        assert_eq!(effect, DoorEffect::Creak);
        assert_eq!(r.state(), &DoorState::Open);
    }

    #[test]
    fn step_records_transition() {
        let mut r = make_runner();
        r.step(DoorEvent::OpenIt).unwrap();
        assert_eq!(r.step_count(), 1);
        let h = r.history();
        assert_eq!(h[0].from, DoorState::Closed);
        assert_eq!(h[0].event, DoorEvent::OpenIt);
        assert_eq!(h[0].to, DoorState::Open);
    }

    #[test]
    fn invalid_transition_returns_error() {
        let mut r = make_runner();
        // Closed + UnlockIt is invalid.
        let err = r.step(DoorEvent::UnlockIt).unwrap_err();
        match err {
            MachineError::Step(_) => {}
            _ => panic!("expected step error"),
        }
        // State unchanged + no history.
        assert_eq!(r.state(), &DoorState::Closed);
        assert_eq!(r.step_count(), 0);
    }

    #[test]
    fn machine_error_kind_is_stable() {
        use crate::error_kind::ErrorKind;
        let e: MachineError<DoorErr> = MachineError::Step(DoorErr::Invalid {
            state: "x".into(),
            event: "y".into(),
        });
        assert_eq!(e.kind(), "step");
        let t: MachineError<DoorErr> = MachineError::Terminal;
        assert_eq!(t.kind(), "terminal");
    }

    #[test]
    fn full_state_walk() {
        let mut r = make_runner();
        r.step(DoorEvent::OpenIt).unwrap();
        assert_eq!(r.state(), &DoorState::Open);
        r.step(DoorEvent::CloseIt).unwrap();
        assert_eq!(r.state(), &DoorState::Closed);
        r.step(DoorEvent::LockIt).unwrap();
        assert_eq!(r.state(), &DoorState::Locked);
        r.step(DoorEvent::UnlockIt).unwrap();
        assert_eq!(r.state(), &DoorState::Closed);
        assert_eq!(r.step_count(), 4);
    }

    #[test]
    fn reset_clears_history_and_state() {
        let mut r = make_runner();
        r.step(DoorEvent::OpenIt).unwrap();
        r.step(DoorEvent::CloseIt).unwrap();
        r.reset();
        assert_eq!(r.state(), &DoorState::Closed);
        assert_eq!(r.step_count(), 0);
    }

    #[test]
    fn from_state_starts_at_explicit_state() {
        let r: MachineRunner<DoorMachine> =
            MachineRunner::from_state(DoorState::Locked, Arc::new(FrozenClock::at(0)));
        assert_eq!(r.state(), &DoorState::Locked);
    }

    #[test]
    fn name_passes_through() {
        let r = make_runner();
        assert_eq!(r.name(), "door");
    }

    #[test]
    fn frozen_clock_yields_same_instant_for_all_transitions() {
        let mut r = make_runner();
        r.step(DoorEvent::OpenIt).unwrap();
        r.step(DoorEvent::CloseIt).unwrap();
        let h = r.history();
        // Same FrozenClock instance → both transitions at the same Instant.
        assert_eq!(h[0].at, h[1].at);
    }

    #[test]
    fn step_with_advancing_clock_records_distinct_instants() {
        let clock = Arc::new(FrozenClock::at(100));
        let mut r = MachineRunner::<DoorMachine>::new(clock.clone());
        r.step(DoorEvent::OpenIt).unwrap();
        clock.advance(10);
        r.step(DoorEvent::CloseIt).unwrap();
        let h = r.history();
        assert!(h[1].at > h[0].at);
    }

    // ── Terminal-state machine ──────────────────────────────────

    #[derive(Default)]
    pub struct OneShotMachine;
    crate::define_named!(OneShotMachine, "one-shot");

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub enum OneShotState {
        Pending,
        Done,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct OneShotEvent;

    impl StateMachine for OneShotMachine {
        type State = OneShotState;
        type Event = OneShotEvent;
        type Effect = ();
        type Err = DoorErr;

        fn initial() -> OneShotState {
            OneShotState::Pending
        }

        fn step(_state: &OneShotState, _event: &OneShotEvent) -> Result<(OneShotState, ()), DoorErr> {
            Ok((OneShotState::Done, ()))
        }

        fn is_terminal(state: &OneShotState) -> bool {
            matches!(state, OneShotState::Done)
        }
    }

    #[test]
    fn step_from_terminal_returns_terminal_error() {
        let mut r: MachineRunner<OneShotMachine> =
            MachineRunner::new(Arc::new(FrozenClock::at(0)));
        r.step(OneShotEvent).unwrap(); // Pending -> Done
        assert!(r.is_terminal());
        let err = r.step(OneShotEvent).unwrap_err();
        match err {
            MachineError::Terminal => {}
            _ => panic!("expected Terminal"),
        }
        // No transition recorded for the failed terminal step.
        assert_eq!(r.step_count(), 1);
    }

    #[test]
    fn transition_record_round_trips_via_serde() {
        let r = TransitionRecord {
            from: DoorState::Closed,
            event: DoorEvent::OpenIt,
            to: DoorState::Open,
            at: Instant::new(100, 5),
        };
        let bytes = serde_json::to_vec(&r).unwrap();
        let back: TransitionRecord<DoorState, DoorEvent> =
            serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, r);
    }
}
