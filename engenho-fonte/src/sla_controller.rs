//! Concrete SlaController — a worked TargetController for the
//! Sla kind. Demonstrates the typed Spec/Snapshot/Drift integration
//! pattern; real per-kind controllers (CostBudget, Compliance,
//! Security, CustomerKpi) follow the same shape.
//!
//! Behind `with-promessa` (re-uses the cross-workspace promessa-types
//! dep).

use promessa_types::{Decision, PromessaTargetKind, Severity, TargetController, TypedAction};
use serde::{Deserialize, Serialize};

/// SLA promise spec: target availability percent over a window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlaSpec {
    /// Target availability — e.g. 99.99.
    pub target_pct: f64,
    /// Window in seconds (e.g. 2_592_000 for 30d).
    pub window_seconds: u64,
}

/// Observed snapshot — current measured availability percent over
/// the window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlaSnapshot {
    /// Currently measured availability over the window.
    pub measured_pct: f64,
}

/// Typed drift — observed deviation in percentage points (positive
/// = above target, negative = below).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SlaDrift {
    /// `measured - target` in percentage points.
    pub delta_pp: f64,
}

/// Concrete SLA controller — pure functions, no I/O.
#[derive(Debug, Default)]
pub struct SlaController;

impl SlaController {
    /// New controller.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl TargetController for SlaController {
    type Spec = SlaSpec;
    type Snapshot = SlaSnapshot;
    type Drift = SlaDrift;
    const KIND: PromessaTargetKind = PromessaTargetKind::Sla;

    fn diff(&self, spec: &Self::Spec, snapshot: &Self::Snapshot) -> Self::Drift {
        SlaDrift {
            delta_pp: snapshot.measured_pct - spec.target_pct,
        }
    }

    fn classify(&self, drift: &Self::Drift) -> Severity {
        // Above target → Cosmetic; within 1pp → Functional; below 1pp
        // → Critical. Mirrors promessa-types' 3-tier severity.
        if drift.delta_pp >= 0.0 {
            Severity::Cosmetic
        } else if drift.delta_pp.abs() <= 1.0 {
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
