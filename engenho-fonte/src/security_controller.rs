//! SecurityController — concrete TargetController for the Security
//! kind. Operator declares a max acceptable CVE age + max critical
//! count; the controller observes the cluster's current posture.

use promessa_types::{Decision, PromessaTargetKind, Severity, TargetController, TypedAction};
use serde::{Deserialize, Serialize};

/// Security spec — max acceptable age for any unpatched CVE +
/// max acceptable critical CVE count.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecuritySpec {
    /// Max age in hours for any unpatched CVE (typical: 24).
    pub max_cve_age_hours: u32,
    /// Max acceptable critical-severity CVE count.
    pub max_critical_count: u32,
}

/// Observed snapshot — cluster's current security posture.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecuritySnapshot {
    /// Age in hours of the oldest unpatched CVE (0 = no CVEs).
    pub oldest_cve_age_hours: u32,
    /// Current critical-severity CVE count.
    pub critical_count: u32,
}

/// Typed drift — both overage signals surface for downstream
/// dashboards.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SecurityDrift {
    /// Hours over the max age (0 if within spec).
    pub age_overage_hours: u32,
    /// Critical count above the limit (0 if within spec).
    pub critical_overage: u32,
}

/// Concrete Security controller.
#[derive(Debug, Default)]
pub struct SecurityController;

impl SecurityController {
    /// New controller.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl TargetController for SecurityController {
    type Spec = SecuritySpec;
    type Snapshot = SecuritySnapshot;
    type Drift = SecurityDrift;
    const KIND: PromessaTargetKind = PromessaTargetKind::Security;

    fn diff(&self, spec: &Self::Spec, snapshot: &Self::Snapshot) -> Self::Drift {
        SecurityDrift {
            age_overage_hours: snapshot
                .oldest_cve_age_hours
                .saturating_sub(spec.max_cve_age_hours),
            critical_overage: snapshot
                .critical_count
                .saturating_sub(spec.max_critical_count),
        }
    }

    fn classify(&self, drift: &Self::Drift) -> Severity {
        // No overage → Cosmetic; minor age overage (<24h) → Functional;
        // any critical overage OR age >24h over → Critical.
        if drift.age_overage_hours == 0 && drift.critical_overage == 0 {
            Severity::Cosmetic
        } else if drift.critical_overage == 0 && drift.age_overage_hours <= 24 {
            Severity::Functional
        } else {
            Severity::Critical
        }
    }

    fn decide(&self, _spec: &Self::Spec, severity: Severity, _drift: &Self::Drift) -> Decision {
        match severity {
            Severity::Cosmetic => Decision::NoAction,
            Severity::Functional => Decision::Alert,
            // Security drift auto-pins to new image (Noop placeholder
            // until the renovate-style action lands).
            Severity::Critical => Decision::AutoCorrect(TypedAction::Noop),
        }
    }
}
