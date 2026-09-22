use std::collections::BTreeSet;
use std::num::NonZeroU32;

use engenho_substrate::maquina::StateMachine;
use proptest::prelude::*;

use super::*;

fn t(secs: i64) -> Timestamp {
    Timestamp::parse("2026-09-22T00:00:00Z")
        .expect("literal")
        .after(Duration::from_secs(
            u64::try_from(secs).expect("non-negative"),
        ))
}

fn one() -> NonZeroU32 {
    NonZeroU32::MIN
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "every call site builds its event inline; taking it by value keeps them readable"
)]
fn step(m: &Lifecycle, e: LifecycleEvent) -> (Lifecycle, LifecycleEffect) {
    DaemonLifecycle::step(m, &e).unwrap_or_else(|r| panic!("{} refused: {r}", e.name()))
}

fn refused(m: &Lifecycle, e: &LifecycleEvent) -> RefusedBecause {
    match DaemonLifecycle::step(m, e) {
        Ok((next, _)) => panic!("{} accepted into {}", e.name(), next.state.name()),
        Err(r) => r.reason,
    }
}

fn failed(phase: BootPhase, class: FailureClass, store: StoreOutcome, at: i64) -> LifecycleEvent {
    LifecycleEvent::BootFailed {
        report: FailureReport {
            phase,
            error: "boom".into(),
            at: t(at),
        },
        class,
        store,
    }
}

fn booted(at: i64) -> LifecycleEvent {
    LifecycleEvent::Booted {
        at: t(at),
        apiserver_addr: "127.0.0.1:6443".into(),
    }
}

fn running() -> Lifecycle {
    let (m, _) = step(&Lifecycle::at(t(0)), LifecycleEvent::Start { at: t(0) });
    step(&m, booted(1)).0
}

#[test]
fn startup_starts_the_first_boot_at_the_first_phase() {
    let (m, effect) = step(&Lifecycle::at(t(0)), LifecycleEvent::Start { at: t(0) });
    assert_eq!(effect, LifecycleEffect::StartBoot { attempt: one() });
    assert_eq!(
        m.state,
        LifecycleState::Booting {
            attempt: one(),
            phase: BootPhase::FIRST,
            since: t(0),
        }
    );
}

#[test]
fn a_boot_walks_its_phases_then_runs_in_sync() {
    let (mut m, _) = step(&Lifecycle::at(t(0)), LifecycleEvent::Start { at: t(0) });
    for (i, phase) in BootPhase::ALL.iter().enumerate() {
        let at = t(i64::try_from(i).expect("small"));
        m = step(&m, LifecycleEvent::Phase { phase: *phase, at }).0;
        assert!(matches!(m.state, LifecycleState::Booting { phase: p, .. } if p == *phase));
    }
    let (m, effect) = step(&m, booted(20));
    assert_eq!(effect, LifecycleEffect::None);
    assert_eq!(
        m.state,
        LifecycleState::Running {
            attempt: one(),
            since: t(20),
            apiserver_addr: "127.0.0.1:6443".into(),
            pending: PendingApply::InSync,
        }
    );
    assert_eq!(m.streak(), 0);
}

#[test]
fn backoff_failures_double_their_delay_and_retry_when_due() {
    let (m, _) = step(&Lifecycle::at(t(0)), LifecycleEvent::Start { at: t(0) });
    let bind = || {
        failed(
            BootPhase::BindApiserver,
            FailureClass::Backoff,
            StoreOutcome::Released,
            5,
        )
    };
    let (m, _) = step(&m, bind());
    let LifecycleState::Failed { retry, attempt, .. } = &m.state else {
        panic!("{:?}", m.state)
    };
    assert_eq!(*attempt, one());
    assert_eq!(
        *retry,
        RetryClass::Backoff {
            next_retry_at: t(6),
            delay_ms: 1000,
        }
    );
    assert_eq!(m.epoch(), 1, "the failed boot released the store");

    let (m, effect) = step(&m, LifecycleEvent::RetryDue { at: t(6) });
    assert_eq!(
        effect,
        LifecycleEffect::StartBoot {
            attempt: NonZeroU32::new(2).expect("two"),
        }
    );
    let (m, _) = step(&m, bind());
    assert!(matches!(
        m.state,
        LifecycleState::Failed {
            retry: RetryClass::Backoff { delay_ms: 2000, .. },
            ..
        }
    ));
    // A boot that reaches Running ends the streak.
    let (m, _) = step(&m, LifecycleEvent::Retry { at: t(7) });
    let (m, _) = step(&m, booted(8));
    assert_eq!(m.streak(), 0);
}

#[test]
fn a_held_failure_waits_for_a_change_not_a_timer() {
    let (m, _) = step(&Lifecycle::at(t(0)), LifecycleEvent::Start { at: t(0) });
    let (m, _) = step(
        &m,
        failed(
            BootPhase::ResolveConfig,
            FailureClass::Hold,
            StoreOutcome::Released,
            1,
        ),
    );
    assert!(matches!(
        m.state,
        LifecycleState::Failed {
            retry: RetryClass::Hold,
            ..
        }
    ));
    assert_eq!(
        refused(&m, &LifecycleEvent::RetryDue { at: t(2) }),
        RefusedBecause::Unexpected,
        "a held failure has no timer to be due"
    );
    let (_, effect) = step(&m, LifecycleEvent::DeclaredChanged { at: t(3) });
    assert!(matches!(effect, LifecycleEffect::StartBoot { .. }));
    let (_, effect) = step(&m, LifecycleEvent::Retry { at: t(3) });
    assert!(matches!(effect, LifecycleEffect::StartBoot { .. }));
}

#[test]
fn stopping_a_running_runtime_drains_it_then_rests_in_a_new_epoch() {
    let (m, effect) = step(&running(), LifecycleEvent::Stop { at: t(2) });
    assert_eq!(effect, LifecycleEffect::ShutdownRuntime);
    assert_eq!(
        m.state,
        LifecycleState::Draining {
            attempt: one(),
            then: AfterDrain::Stop,
        }
    );
    let (m, effect) = step(
        &m,
        LifecycleEvent::Drained {
            at: t(3),
            store: StoreOutcome::Released,
        },
    );
    assert_eq!(effect, LifecycleEffect::None);
    assert_eq!(
        m.state,
        LifecycleState::Stopped {
            reason: StopReason::OperatorRequest,
            epoch: 1,
            since: t(3),
        }
    );
    // And a stopped runtime starts again as the next attempt.
    let (_, effect) = step(&m, LifecycleEvent::Start { at: t(4) });
    assert_eq!(
        effect,
        LifecycleEffect::StartBoot {
            attempt: NonZeroU32::new(2).expect("two"),
        }
    );
}

#[test]
fn stopping_a_boot_cancels_it_and_its_unwind_completes_the_stop() {
    let (m, _) = step(&Lifecycle::at(t(0)), LifecycleEvent::Start { at: t(0) });
    let (m, effect) = step(&m, LifecycleEvent::Stop { at: t(1) });
    assert_eq!(effect, LifecycleEffect::CancelBoot);
    // Late progress from the boot is absorbed.
    let (m, _) = step(
        &m,
        LifecycleEvent::Phase {
            phase: BootPhase::OpenStore,
            at: t(1),
        },
    );
    let (m, _) = step(
        &m,
        failed(
            BootPhase::OpenStore,
            FailureClass::Hold,
            StoreOutcome::Released,
            2,
        ),
    );
    assert!(matches!(
        m.state,
        LifecycleState::Stopped {
            reason: StopReason::OperatorRequest,
            ..
        }
    ));
    assert_eq!(m.streak(), 0, "a cancelled boot is not a failure streak");
}

#[test]
fn a_boot_that_finishes_before_it_sees_the_stop_is_shut_down() {
    let (m, _) = step(&Lifecycle::at(t(0)), LifecycleEvent::Start { at: t(0) });
    let (m, _) = step(&m, LifecycleEvent::Stop { at: t(1) });
    let (m, effect) = step(&m, booted(2));
    assert_eq!(effect, LifecycleEffect::ShutdownRuntime);
    assert!(matches!(m.state, LifecycleState::Draining { .. }));
}

#[test]
fn restart_drains_then_boots_the_next_attempt() {
    let (m, effect) = step(&running(), LifecycleEvent::Restart { at: t(2) });
    assert_eq!(effect, LifecycleEffect::ShutdownRuntime);
    let (m, effect) = step(
        &m,
        LifecycleEvent::Drained {
            at: t(3),
            store: StoreOutcome::Released,
        },
    );
    assert_eq!(
        effect,
        LifecycleEffect::StartBoot {
            attempt: NonZeroU32::new(2).expect("two"),
        }
    );
    assert_eq!(m.epoch(), 1);
}

#[test]
fn a_store_that_is_still_held_wedges_and_only_exit_leaves() {
    let (m, _) = step(&running(), LifecycleEvent::Stop { at: t(2) });
    let (m, _) = step(
        &m,
        LifecycleEvent::Drained {
            at: t(3),
            store: StoreOutcome::Held {
                cause: "2 strong refs".into(),
            },
        },
    );
    assert!(matches!(m.state, LifecycleState::Wedged { .. }));
    for event in [
        LifecycleEvent::Start { at: t(4) },
        LifecycleEvent::Retry { at: t(4) },
        LifecycleEvent::Stop { at: t(4) },
        LifecycleEvent::Restart { at: t(4) },
        LifecycleEvent::DeclaredChanged { at: t(4) },
    ] {
        assert_eq!(
            refused(&m, &event),
            RefusedBecause::Wedged,
            "{}",
            event.name()
        );
    }
    let (m, effect) = step(
        &m,
        LifecycleEvent::Exit {
            intent: ExitIntent::Relaunch,
        },
    );
    assert_eq!(effect, LifecycleEffect::Exit(ExitIntent::Relaunch));
    assert!(DaemonLifecycle::is_terminal(&m));
}

#[test]
fn exiting_while_running_drains_first_and_exits_even_if_the_store_is_held() {
    let (m, effect) = step(
        &running(),
        LifecycleEvent::Exit {
            intent: ExitIntent::Halt,
        },
    );
    assert_eq!(effect, LifecycleEffect::ShutdownRuntime);
    let (m, effect) = step(
        &m,
        LifecycleEvent::Drained {
            at: t(3),
            store: StoreOutcome::Held {
                cause: "leak".into(),
            },
        },
    );
    assert_eq!(effect, LifecycleEffect::Exit(ExitIntent::Halt));
    assert_eq!(
        m.state,
        LifecycleState::Exiting {
            intent: ExitIntent::Halt,
        }
    );
}

#[test]
fn an_exit_during_a_stop_takes_over_the_drain() {
    let (m, _) = step(&running(), LifecycleEvent::Stop { at: t(2) });
    let (m, effect) = step(
        &m,
        LifecycleEvent::Exit {
            intent: ExitIntent::Relaunch,
        },
    );
    assert_eq!(
        effect,
        LifecycleEffect::None,
        "the drain is already under way"
    );
    let (_, effect) = step(
        &m,
        LifecycleEvent::Drained {
            at: t(3),
            store: StoreOutcome::Released,
        },
    );
    assert_eq!(effect, LifecycleEffect::Exit(ExitIntent::Relaunch));
}

#[test]
fn a_hold_marker_at_startup_rests_without_booting() {
    let (m, effect) = step(
        &Lifecycle::at(t(0)),
        LifecycleEvent::HeldAtStartup { at: t(0) },
    );
    assert_eq!(effect, LifecycleEffect::None);
    assert_eq!(
        m.state,
        LifecycleState::Stopped {
            reason: StopReason::HeldAtStartup,
            epoch: 0,
            since: t(0),
        }
    );
}

#[test]
fn stopping_a_failed_boot_rests_in_the_failures_epoch() {
    let (m, _) = step(&Lifecycle::at(t(0)), LifecycleEvent::Start { at: t(0) });
    let (m, _) = step(
        &m,
        failed(
            BootPhase::BindApiserver,
            FailureClass::Backoff,
            StoreOutcome::Released,
            1,
        ),
    );
    let (m, effect) = step(&m, LifecycleEvent::Stop { at: t(2) });
    assert_eq!(effect, LifecycleEffect::None);
    assert!(matches!(m.state, LifecycleState::Stopped { epoch: 1, .. }));
}

#[test]
fn commands_are_refused_with_the_apis_reasons() {
    let at = t(9);
    let run = running();
    assert_eq!(
        refused(&run, &LifecycleEvent::Start { at }),
        RefusedBecause::RuntimeRunning
    );
    assert_eq!(
        refused(&run, &LifecycleEvent::Retry { at }),
        RefusedBecause::RuntimeNotFailed
    );

    let (booting, _) = step(&Lifecycle::at(t(0)), LifecycleEvent::Start { at: t(0) });
    assert_eq!(
        refused(&booting, &LifecycleEvent::Start { at }),
        RefusedBecause::LifecycleBusy
    );

    let (stopped, _) = step(
        &Lifecycle::at(t(0)),
        LifecycleEvent::HeldAtStartup { at: t(0) },
    );
    assert_eq!(
        refused(&stopped, &LifecycleEvent::Stop { at }),
        RefusedBecause::RuntimeNotRunning
    );
    assert_eq!(
        refused(&stopped, &LifecycleEvent::Restart { at }),
        RefusedBecause::RuntimeNotRunning
    );

    let (failed_m, _) = step(
        &booting,
        failed(
            BootPhase::OpenStore,
            FailureClass::Hold,
            StoreOutcome::Released,
            1,
        ),
    );
    assert_eq!(
        refused(&failed_m, &LifecycleEvent::Start { at }),
        RefusedBecause::RuntimeNotStopped
    );

    let (draining, _) = step(&run, LifecycleEvent::Stop { at });
    assert_eq!(
        refused(&draining, &LifecycleEvent::Stop { at }),
        RefusedBecause::LifecycleBusy
    );
}

#[test]
fn the_wire_shapes_are_the_control_apis() {
    let draining = LifecycleState::Draining {
        attempt: one(),
        then: AfterDrain::Exit {
            intent: ExitIntent::Relaunch,
        },
    };
    assert_eq!(
        serde_json::to_value(&draining).expect("serialize"),
        serde_json::json!({"state": "draining", "attempt": 1, "then": "exit", "intent": "relaunch"})
    );
    let stop = LifecycleState::Draining {
        attempt: one(),
        then: AfterDrain::Stop,
    };
    assert_eq!(
        serde_json::to_value(&stop).expect("serialize"),
        serde_json::json!({"state": "draining", "attempt": 1, "then": "stop"})
    );
    let failed_state = LifecycleState::Failed {
        attempt: one(),
        report: FailureReport {
            phase: BootPhase::BindApiserver,
            error: "bind failed".into(),
            at: t(0),
        },
        retry: RetryClass::Backoff {
            next_retry_at: t(1),
            delay_ms: 1000,
        },
    };
    assert_eq!(
        serde_json::to_value(&failed_state).expect("serialize"),
        serde_json::json!({
            "state": "failed",
            "attempt": 1,
            "report": {"phase": "bind_apiserver", "error": "bind failed", "at": "2026-09-22T00:00:00.000Z"},
            "retry": {"class": "backoff", "next_retry_at": "2026-09-22T00:00:01.000Z", "delay_ms": 1000},
        })
    );
    for state in [draining, stop, failed_state] {
        let back: LifecycleState =
            serde_json::from_value(serde_json::to_value(&state).expect("serialize"))
                .expect("round trip");
        assert_eq!(back, state);
    }
    assert_eq!(ExitIntent::Halt.code(), 0);
    assert_eq!(ExitIntent::Relaunch.code(), 75);
}

/// Every state is reachable from startup.
#[test]
fn every_state_is_reachable() {
    let start = Lifecycle::at(t(0));
    let mut seen = BTreeSet::new();
    let mut walk = |events: Vec<LifecycleEvent>| {
        let mut m = start.clone();
        seen.insert(m.state.name());
        for e in events {
            m = step(&m, e).0;
            seen.insert(m.state.name());
        }
    };
    walk(vec![
        LifecycleEvent::Start { at: t(0) },
        booted(1),
        LifecycleEvent::Stop { at: t(2) },
        LifecycleEvent::Drained {
            at: t(3),
            store: StoreOutcome::Held { cause: "x".into() },
        },
        LifecycleEvent::Exit {
            intent: ExitIntent::Halt,
        },
    ]);
    walk(vec![
        LifecycleEvent::Start { at: t(0) },
        failed(
            BootPhase::OpenStore,
            FailureClass::Hold,
            StoreOutcome::Released,
            1,
        ),
        LifecycleEvent::Stop { at: t(2) },
    ]);
    let all: BTreeSet<&str> = [
        "resolving",
        "booting",
        "running",
        "draining",
        "stopped",
        "failed",
        "wedged",
        "exiting",
    ]
    .into_iter()
    .collect();
    assert_eq!(seen, all);
}

fn any_store() -> impl Strategy<Value = StoreOutcome> {
    prop_oneof![
        3 => Just(StoreOutcome::Released),
        1 => Just(StoreOutcome::Held { cause: "held".into() }),
    ]
}

fn any_event() -> impl Strategy<Value = LifecycleEvent> {
    let at = (0i64..10_000).prop_map(t);
    let phase = proptest::sample::select(BootPhase::ALL.to_vec());
    let class = prop_oneof![Just(FailureClass::Backoff), Just(FailureClass::Hold)];
    let intent = prop_oneof![Just(ExitIntent::Halt), Just(ExitIntent::Relaunch)];
    prop_oneof![
        at.clone()
            .prop_map(|at| LifecycleEvent::HeldAtStartup { at }),
        at.clone().prop_map(|at| LifecycleEvent::Start { at }),
        at.clone().prop_map(|at| LifecycleEvent::Retry { at }),
        at.clone().prop_map(|at| LifecycleEvent::RetryDue { at }),
        at.clone()
            .prop_map(|at| LifecycleEvent::DeclaredChanged { at }),
        (phase.clone(), at.clone()).prop_map(|(phase, at)| LifecycleEvent::Phase { phase, at }),
        at.clone().prop_map(|at| LifecycleEvent::Booted {
            at,
            apiserver_addr: "127.0.0.1:1".into()
        }),
        (phase, class, any_store(), at.clone()).prop_map(|(phase, class, store, at)| {
            LifecycleEvent::BootFailed {
                report: FailureReport {
                    phase,
                    error: "e".into(),
                    at,
                },
                class,
                store,
            }
        }),
        at.clone().prop_map(|at| LifecycleEvent::Stop { at }),
        at.clone().prop_map(|at| LifecycleEvent::Restart { at }),
        intent.prop_map(|intent| LifecycleEvent::Exit { intent }),
        (at, any_store()).prop_map(|(at, store)| LifecycleEvent::Drained { at, store }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// The store is never held at rest, and no boot starts over a held store.
    ///
    /// `held` is a ghost: whether, going by the outcomes the machine
    /// accepted, the store may be open. It becomes true when a boot starts
    /// and false only on an accepted outcome that says `Released`.
    #[test]
    fn the_store_is_never_held_at_rest(events in proptest::collection::vec(any_event(), 0..64)) {
        let mut m = Lifecycle::at(t(0));
        let mut held = false;
        let mut attempts = 0u32;
        let mut epoch = 0u64;
        for event in events {
            if DaemonLifecycle::is_terminal(&m) {
                break;
            }
            let Ok((next, effect)) = DaemonLifecycle::step(&m, &event) else {
                continue;
            };
            if let LifecycleEvent::BootFailed { store, .. } | LifecycleEvent::Drained { store, .. } = &event {
                held = matches!(store, StoreOutcome::Held { .. });
            }
            if let LifecycleEffect::StartBoot { attempt } = effect {
                prop_assert!(!held, "a boot started over a held store: {:?} -> {:?}", m.state, next.state);
                prop_assert_eq!(attempt.get(), next.attempts());
                held = true;
            }
            if matches!(next.state, LifecycleState::Wedged { .. }) {
                prop_assert!(held, "wedged with the store released");
            }
            if !next.state.may_hold_store() && !DaemonLifecycle::is_terminal(&next) {
                prop_assert!(!held, "{} with the store held", next.state.name());
            }
            prop_assert!(next.attempts() >= attempts && next.epoch() >= epoch);
            attempts = next.attempts();
            epoch = next.epoch();

            let wire = serde_json::to_value(&next.state).expect("serialize");
            prop_assert_eq!(wire["state"].as_str(), Some(next.state.name()));
            let back: LifecycleState = serde_json::from_value(wire).expect("round trip");
            prop_assert_eq!(&back, &next.state);
            // Parity with the control API: every reachable state is a valid
            // `LifecycleState` of the spec, and says the same thing there.
            let spec: engenho_control_types::types::LifecycleState =
                crate::control::wire(&next.state).expect("the spec's shape");
            let returned: LifecycleState = crate::control::wire(&spec).expect("and back");
            prop_assert_eq!(&returned, &next.state);
            m = next;
        }
    }
}
