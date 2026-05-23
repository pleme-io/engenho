//! CustomerKpiController — concrete TargetController for the
//! CustomerKpi kind. Operator declares a target customer-facing KPI
//! (NPS, CSAT, retention, activation); the controller observes the
//! current value + classifies dip.

use promessa_types::{Decision, PromessaTargetKind, Severity, TargetController, TypedAction};
use serde::{Deserialize, Serialize};

/// CustomerKpi spec — KPI name + target value + minimum-acceptable
/// value below which we escalate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomerKpiSpec {
    /// KPI name (e.g. "nps", "csat", "30d-retention").
    pub kpi_name: String,
    /// Target value (e.g. 50.0 for NPS).
    pub target: f64,
    /// Minimum-acceptable value — below this is Critical.
    pub critical_floor: f64,
}

/// Observed snapshot — current KPI measurement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomerKpiSnapshot {
    /// Currently measured value.
    pub measured: f64,
}

/// Typed drift — gap from target.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CustomerKpiDrift {
    /// `measured - target` (positive = above target, negative = below).
    pub gap: f64,
}

/// Concrete CustomerKpi controller.
#[derive(Debug, Default)]
pub struct CustomerKpiController;

impl CustomerKpiController {
    /// New controller.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl TargetController for CustomerKpiController {
    type Spec = CustomerKpiSpec;
    type Snapshot = CustomerKpiSnapshot;
    type Drift = CustomerKpiDrift;
    const KIND: PromessaTargetKind = PromessaTargetKind::CustomerKpi;

    fn diff(&self, spec: &Self::Spec, snapshot: &Self::Snapshot) -> Self::Drift {
        CustomerKpiDrift {
            gap: snapshot.measured - spec.target,
        }
    }

    fn classify(&self, drift: &Self::Drift) -> Severity {
        // Above target → Cosmetic. Below target but above floor →
        // Functional. Below floor → Critical. (Floor is checked in
        // decide() since classify takes only the drift.)
        if drift.gap >= 0.0 {
            Severity::Cosmetic
        } else if drift.gap >= -10.0 {
            Severity::Functional
        } else {
            Severity::Critical
        }
    }

    fn decide(&self, spec: &Self::Spec, severity: Severity, drift: &Self::Drift) -> Decision {
        let measured = spec.target + drift.gap;
        match severity {
            Severity::Cosmetic => Decision::NoAction,
            Severity::Functional => Decision::Alert,
            Severity::Critical => {
                // Below floor → escalate (customer-facing impact
                // warrants human review); otherwise auto-correct
                // (typically a typed retention/activation campaign).
                if measured < spec.critical_floor {
                    Decision::RequireApproval(TypedAction::Noop)
                } else {
                    Decision::AutoCorrect(TypedAction::Noop)
                }
            }
        }
    }
}
