//! CostBudgetController — concrete TargetController for the
//! CostBudget kind. Operator declares a target monthly spend; the
//! controller observes actual spend + classifies overrun by severity.
//!
//! Pure functions; gated `with-promessa`.

use promessa_types::{Decision, PromessaTargetKind, Severity, TargetController, TypedAction};
use serde::{Deserialize, Serialize};

/// CostBudget spec — target monthly USD spend over the budget window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostBudgetSpec {
    /// Target monthly spend in USD (e.g. 5000.0).
    pub target_usd: f64,
    /// Budget window in days (typical: 30).
    pub window_days: u32,
}

/// Observed snapshot — current spend so far in the window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostBudgetSnapshot {
    /// Current spend so far in the active window (USD).
    pub measured_usd: f64,
    /// Day index within the window (1..=window_days).
    pub day_in_window: u32,
}

/// Typed drift — projected end-of-window overrun as a fraction of
/// target (e.g. 0.1 = 10% over target).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CostBudgetDrift {
    /// Projected overrun fraction. Negative = under budget; positive
    /// = projected over budget.
    pub overrun_fraction: f64,
}

/// Concrete CostBudget controller.
#[derive(Debug, Default)]
pub struct CostBudgetController;

impl CostBudgetController {
    /// New controller.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl TargetController for CostBudgetController {
    type Spec = CostBudgetSpec;
    type Snapshot = CostBudgetSnapshot;
    type Drift = CostBudgetDrift;
    const KIND: PromessaTargetKind = PromessaTargetKind::CostBudget;

    fn diff(&self, spec: &Self::Spec, snapshot: &Self::Snapshot) -> Self::Drift {
        // Linear projection: rate = measured/day; projected_total =
        // rate * window_days. overrun = (projected - target) / target.
        let days = snapshot.day_in_window.max(1) as f64;
        let rate = snapshot.measured_usd / days;
        let projected = rate * f64::from(spec.window_days);
        let overrun = (projected - spec.target_usd) / spec.target_usd;
        CostBudgetDrift {
            overrun_fraction: overrun,
        }
    }

    fn classify(&self, drift: &Self::Drift) -> Severity {
        // Under budget → Cosmetic; up to 10% over → Functional;
        // beyond 10% → Critical.
        if drift.overrun_fraction <= 0.0 {
            Severity::Cosmetic
        } else if drift.overrun_fraction <= 0.10 {
            Severity::Functional
        } else {
            Severity::Critical
        }
    }

    fn decide(&self, _spec: &Self::Spec, severity: Severity, _drift: &Self::Drift) -> Decision {
        match severity {
            Severity::Cosmetic => Decision::NoAction,
            Severity::Functional => Decision::Alert,
            Severity::Critical => Decision::AutoCorrect(TypedAction::Noop),
        }
    }
}
