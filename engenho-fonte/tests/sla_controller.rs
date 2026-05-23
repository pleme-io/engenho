//! Tests for the concrete SlaController — proves the typed
//! Spec/Snapshot/Drift pipeline classifies + decides correctly.

#![cfg(feature = "with-promessa")]

use engenho_fonte::{SlaController, SlaDrift, SlaSnapshot, SlaSpec};
use promessa_types::{Decision, PromessaTargetKind, Severity, TargetController, TypedAction};

#[test]
fn kind_is_sla() {
    assert_eq!(SlaController::KIND, PromessaTargetKind::Sla);
}

#[test]
fn diff_at_target_yields_zero_delta() {
    let c = SlaController::new();
    let spec = SlaSpec {
        target_pct: 99.99,
        window_seconds: 2_592_000,
    };
    let snap = SlaSnapshot {
        measured_pct: 99.99,
    };
    let drift = c.diff(&spec, &snap);
    assert_eq!(drift, SlaDrift { delta_pp: 0.0 });
}

#[test]
fn diff_above_target_yields_positive_delta() {
    let c = SlaController::new();
    let drift = c.diff(
        &SlaSpec {
            target_pct: 99.0,
            window_seconds: 0,
        },
        &SlaSnapshot { measured_pct: 99.5 },
    );
    assert!(drift.delta_pp > 0.0);
}

#[test]
fn classify_above_target_is_cosmetic() {
    let c = SlaController::new();
    assert_eq!(c.classify(&SlaDrift { delta_pp: 0.5 }), Severity::Cosmetic);
}

#[test]
fn classify_within_one_pp_is_functional() {
    let c = SlaController::new();
    assert_eq!(
        c.classify(&SlaDrift { delta_pp: -0.5 }),
        Severity::Functional
    );
}

#[test]
fn classify_beyond_one_pp_is_critical() {
    let c = SlaController::new();
    assert_eq!(c.classify(&SlaDrift { delta_pp: -3.0 }), Severity::Critical);
}

#[test]
fn decide_cosmetic_is_no_action() {
    let c = SlaController::new();
    let spec = SlaSpec {
        target_pct: 99.0,
        window_seconds: 0,
    };
    let d = c.decide(&spec, Severity::Cosmetic, &SlaDrift { delta_pp: 0.5 });
    assert_eq!(d, Decision::NoAction);
}

#[test]
fn decide_functional_is_alert() {
    let c = SlaController::new();
    let spec = SlaSpec {
        target_pct: 99.0,
        window_seconds: 0,
    };
    let d = c.decide(&spec, Severity::Functional, &SlaDrift { delta_pp: -0.5 });
    assert_eq!(d, Decision::Alert);
}

#[test]
fn decide_critical_is_auto_correct() {
    let c = SlaController::new();
    let spec = SlaSpec {
        target_pct: 99.0,
        window_seconds: 0,
    };
    let d = c.decide(&spec, Severity::Critical, &SlaDrift { delta_pp: -3.0 });
    match d {
        Decision::AutoCorrect(TypedAction::Noop) => {}
        other => panic!("expected AutoCorrect(Noop), got {other:?}"),
    }
}

#[test]
fn diff_is_pure_no_side_effects() {
    // Trait law `diff_pure` — same inputs, same outputs, no I/O.
    let c = SlaController::new();
    let spec = SlaSpec {
        target_pct: 99.9,
        window_seconds: 30,
    };
    let snap = SlaSnapshot { measured_pct: 99.5 };
    let d1 = c.diff(&spec, &snap);
    let d2 = c.diff(&spec, &snap);
    assert_eq!(d1, d2);
}
