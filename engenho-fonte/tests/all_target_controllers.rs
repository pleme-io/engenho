//! Tests for the 4 non-Sla TargetController impls — CostBudget,
//! Compliance, Security, CustomerKpi. Same shape verification as
//! v1.34's sla_controller.rs tests.

#![cfg(feature = "with-promessa")]

use engenho_fonte::{
    ComplianceController, ComplianceDrift, ComplianceSnapshot, ComplianceSpec,
    CostBudgetController, CostBudgetDrift, CostBudgetSnapshot, CostBudgetSpec,
    CustomerKpiController, CustomerKpiDrift, CustomerKpiSnapshot, CustomerKpiSpec,
    SecurityController, SecurityDrift, SecuritySnapshot, SecuritySpec,
};
use promessa_types::{Decision, PromessaTargetKind, Severity, TargetController, TypedAction};

// ── CostBudget ──────────────────────────────────────────────────

#[test]
fn cost_budget_kind() {
    assert_eq!(CostBudgetController::KIND, PromessaTargetKind::CostBudget);
}

#[test]
fn cost_budget_diff_at_target_yields_zero_overrun() {
    let c = CostBudgetController::new();
    let spec = CostBudgetSpec {
        target_usd: 5000.0,
        window_days: 30,
    };
    // Halfway through window, spent half budget → projected on-target.
    let snap = CostBudgetSnapshot {
        measured_usd: 2500.0,
        day_in_window: 15,
    };
    let drift = c.diff(&spec, &snap);
    assert!(drift.overrun_fraction.abs() < 1e-9);
}

#[test]
fn cost_budget_projected_overrun_classifies_critical() {
    let c = CostBudgetController::new();
    // Spent 20% of budget in 10% of window → 2x overrun.
    let drift = c.diff(
        &CostBudgetSpec {
            target_usd: 5000.0,
            window_days: 30,
        },
        &CostBudgetSnapshot {
            measured_usd: 1000.0,
            day_in_window: 3,
        },
    );
    assert_eq!(c.classify(&drift), Severity::Critical);
}

#[test]
fn cost_budget_decide_critical_is_auto_correct() {
    let c = CostBudgetController::new();
    let d = c.decide(
        &CostBudgetSpec {
            target_usd: 5000.0,
            window_days: 30,
        },
        Severity::Critical,
        &CostBudgetDrift {
            overrun_fraction: 0.5,
        },
    );
    assert_eq!(d, Decision::AutoCorrect(TypedAction::Noop));
}

// ── Compliance ──────────────────────────────────────────────────

#[test]
fn compliance_kind() {
    assert_eq!(ComplianceController::KIND, PromessaTargetKind::Compliance);
}

#[test]
fn compliance_perfect_pass_rate_zero_failures_is_cosmetic() {
    let c = ComplianceController::new();
    let drift = c.diff(
        &ComplianceSpec {
            baseline: "fedramp-high".into(),
            min_pass_rate: 0.95,
        },
        &ComplianceSnapshot {
            pass_rate: 1.0,
            failing_count: 0,
        },
    );
    assert_eq!(c.classify(&drift), Severity::Cosmetic);
}

#[test]
fn compliance_critical_requires_approval() {
    let c = ComplianceController::new();
    // Below min rate AND failing controls → Critical → RequireApproval
    let drift = ComplianceDrift {
        rate_gap: -0.10,
        failing_count: 5,
    };
    let d = c.decide(
        &ComplianceSpec {
            baseline: "pci-dss-4.0".into(),
            min_pass_rate: 0.95,
        },
        Severity::Critical,
        &drift,
    );
    assert_eq!(d, Decision::RequireApproval(TypedAction::Noop));
}

// ── Security ────────────────────────────────────────────────────

#[test]
fn security_kind() {
    assert_eq!(SecurityController::KIND, PromessaTargetKind::Security);
}

#[test]
fn security_within_spec_is_cosmetic() {
    let c = SecurityController::new();
    let drift = c.diff(
        &SecuritySpec {
            max_cve_age_hours: 24,
            max_critical_count: 0,
        },
        &SecuritySnapshot {
            oldest_cve_age_hours: 0,
            critical_count: 0,
        },
    );
    assert_eq!(c.classify(&drift), Severity::Cosmetic);
    assert_eq!(drift.age_overage_hours, 0);
    assert_eq!(drift.critical_overage, 0);
}

#[test]
fn security_critical_count_overage_classifies_critical() {
    let c = SecurityController::new();
    let drift = c.diff(
        &SecuritySpec {
            max_cve_age_hours: 24,
            max_critical_count: 0,
        },
        &SecuritySnapshot {
            oldest_cve_age_hours: 12,
            critical_count: 3,
        },
    );
    assert_eq!(c.classify(&drift), Severity::Critical);
    assert_eq!(drift.critical_overage, 3);
}

// ── CustomerKpi ─────────────────────────────────────────────────

#[test]
fn customer_kpi_kind() {
    assert_eq!(CustomerKpiController::KIND, PromessaTargetKind::CustomerKpi);
}

#[test]
fn customer_kpi_above_target_is_cosmetic() {
    let c = CustomerKpiController::new();
    let drift = c.diff(
        &CustomerKpiSpec {
            kpi_name: "nps".into(),
            target: 50.0,
            critical_floor: 30.0,
        },
        &CustomerKpiSnapshot { measured: 55.0 },
    );
    assert_eq!(c.classify(&drift), Severity::Cosmetic);
    assert!(drift.gap > 0.0);
}

#[test]
fn customer_kpi_below_floor_requires_approval() {
    let c = CustomerKpiController::new();
    let spec = CustomerKpiSpec {
        kpi_name: "nps".into(),
        target: 50.0,
        critical_floor: 30.0,
    };
    let drift = CustomerKpiDrift { gap: -25.0 }; // measured = 25, below floor 30
    let d = c.decide(&spec, Severity::Critical, &drift);
    assert_eq!(d, Decision::RequireApproval(TypedAction::Noop));
}

#[test]
fn customer_kpi_below_target_above_floor_critical_auto_corrects() {
    let c = CustomerKpiController::new();
    let spec = CustomerKpiSpec {
        kpi_name: "nps".into(),
        target: 50.0,
        critical_floor: 30.0,
    };
    let drift = CustomerKpiDrift { gap: -15.0 }; // measured = 35, above floor 30
    let d = c.decide(&spec, Severity::Critical, &drift);
    assert_eq!(d, Decision::AutoCorrect(TypedAction::Noop));
}

// ── Shape parity across all 5 controllers ───────────────────────

#[test]
fn all_five_controllers_have_unique_kind() {
    use std::collections::HashSet;
    let kinds: HashSet<PromessaTargetKind> = [
        engenho_fonte::SlaController::KIND,
        CostBudgetController::KIND,
        ComplianceController::KIND,
        SecurityController::KIND,
        CustomerKpiController::KIND,
    ]
    .into_iter()
    .collect();
    assert_eq!(
        kinds.len(),
        5,
        "every controller must register a unique kind"
    );
}
