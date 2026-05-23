//! ComplianceController — concrete TargetController for the
//! Compliance kind. Operator declares a regulatory baseline (e.g.
//! "fedramp-high"); the controller observes the current attestation
//! pass rate + classifies drift.

use promessa_types::{Decision, PromessaTargetKind, Severity, TargetController, TypedAction};
use serde::{Deserialize, Serialize};

/// Compliance spec — baseline name + minimum pass rate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceSpec {
    /// Regulatory baseline (e.g. "fedramp-high", "pci-dss-4.0").
    pub baseline: String,
    /// Minimum pass rate (0.0–1.0). 1.0 = all controls must pass.
    pub min_pass_rate: f64,
}

/// Observed snapshot — current pass rate against the baseline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceSnapshot {
    /// Current pass rate (0.0–1.0).
    pub pass_rate: f64,
    /// Number of failing controls.
    pub failing_count: u32,
}

/// Typed drift — pass rate gap + failing-control count.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComplianceDrift {
    /// `pass_rate - min_pass_rate` (positive = above min,
    /// negative = below).
    pub rate_gap: f64,
    /// Number of failing controls — surfaced for the dashboard.
    pub failing_count: u32,
}

/// Concrete Compliance controller.
#[derive(Debug, Default)]
pub struct ComplianceController;

impl ComplianceController {
    /// New controller.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl TargetController for ComplianceController {
    type Spec = ComplianceSpec;
    type Snapshot = ComplianceSnapshot;
    type Drift = ComplianceDrift;
    const KIND: PromessaTargetKind = PromessaTargetKind::Compliance;

    fn diff(&self, spec: &Self::Spec, snapshot: &Self::Snapshot) -> Self::Drift {
        ComplianceDrift {
            rate_gap: snapshot.pass_rate - spec.min_pass_rate,
            failing_count: snapshot.failing_count,
        }
    }

    fn classify(&self, drift: &Self::Drift) -> Severity {
        // Above min + zero failing → Cosmetic; small dip → Functional;
        // any failing controls OR rate_gap < -0.05 → Critical.
        if drift.rate_gap >= 0.0 && drift.failing_count == 0 {
            Severity::Cosmetic
        } else if drift.rate_gap >= -0.05 && drift.failing_count <= 2 {
            Severity::Functional
        } else {
            Severity::Critical
        }
    }

    fn decide(&self, _spec: &Self::Spec, severity: Severity, _drift: &Self::Drift) -> Decision {
        match severity {
            Severity::Cosmetic => Decision::NoAction,
            Severity::Functional => Decision::Alert,
            // Compliance drift always escalates rather than
            // auto-correct — regulatory action needs human approval.
            Severity::Critical => Decision::RequireApproval(TypedAction::Noop),
        }
    }
}
