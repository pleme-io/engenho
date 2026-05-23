//! R19 — PodDisruptionBudget controller.
//!
//! Tracks per-pod-set disruption budgets. The substrate's
//! voluntary-eviction path (R20+ drain controller) consults the
//! budget before evicting; this controller maintains the
//! budget's status (currently-healthy / desired-min / max-unavailable)
//! so consumers see live state.
//!
//! ## Reconcile rule
//!
//! For each PDB:
//!   * Find pods matching `spec.selector.matchLabels`
//!   * Count healthy pods (status.conditions[Ready]=True)
//!   * Compute allowed-disruptions from spec.minAvailable /
//!     spec.maxUnavailable
//!   * Patch status with current counts
//!
//! Decisions stay idempotent — status patches only fire when the
//! computed numbers change.

use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{
    command::{Reason, ResourceCommand},
    StoreMesh,
};
use serde_json::{json, Value};

use crate::controller::{Controller, ReconcileReport};
use crate::error::ControllerError;
use crate::selector::matches_labels;

/// PodDisruptionBudget controller.
pub struct PodDisruptionBudgetController {
    store: Arc<StoreMesh>,
    namespace: Option<String>,
}

impl PodDisruptionBudgetController {
    /// New controller.
    #[must_use]
    pub fn new(store: Arc<StoreMesh>, namespace: Option<String>) -> Self {
        Self { store, namespace }
    }

    fn min_available(pdb: &Value) -> Option<i64> {
        pdb.get("spec")
            .and_then(|s| s.get("minAvailable"))
            .and_then(|n| n.as_i64())
    }

    fn max_unavailable(pdb: &Value) -> Option<i64> {
        pdb.get("spec")
            .and_then(|s| s.get("maxUnavailable"))
            .and_then(|n| n.as_i64())
    }

    fn selector_match_labels(pdb: &Value) -> Option<&Value> {
        pdb.get("spec")
            .and_then(|s| s.get("selector"))
            .and_then(|sel| sel.get("matchLabels"))
    }

    fn pod_is_ready(pod: &Value) -> bool {
        pod.get("status")
            .and_then(|s| s.get("conditions"))
            .and_then(|c| c.as_array())
            .map(|conds| {
                conds.iter().any(|c| {
                    c.get("type").and_then(|t| t.as_str()) == Some("Ready")
                        && c.get("status").and_then(|s| s.as_str()) == Some("True")
                })
            })
            .unwrap_or(false)
    }

    /// Pure: compute disruptions-allowed from min_available /
    /// max_unavailable / healthy / total. Exposed for tests.
    #[must_use]
    pub fn compute_allowed_disruptions(
        min_available: Option<i64>,
        max_unavailable: Option<i64>,
        healthy: i64,
        total: i64,
    ) -> i64 {
        if let Some(min) = min_available {
            (healthy - min).max(0)
        } else if let Some(max) = max_unavailable {
            // disruptions_allowed = max_unavailable - (total - healthy)
            // i.e. how many more we can afford to lose
            let unhealthy = total - healthy;
            (max - unhealthy).max(0)
        } else {
            // No budget specified — full disruption allowed.
            healthy
        }
    }
}

#[async_trait]
impl Controller for PodDisruptionBudgetController {
    fn name(&self) -> &'static str {
        "pdb"
    }

    async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
        let pdbs = self
            .store
            .list("policy", "v1", "PodDisruptionBudget", self.namespace.as_deref())
            .await;
        let mut report = ReconcileReport::default();
        report.objects_examined = pdbs.len();

        for (pdb_key, pdb_value) in &pdbs {
            let Some(selector) = Self::selector_match_labels(pdb_value) else {
                report.objects_skipped += 1;
                continue;
            };
            let ns = pdb_key.namespace.as_deref();
            let pods = self.store.list("", "v1", "Pod", ns).await;
            let matching: Vec<&Value> = pods
                .iter()
                .filter_map(|(_, p)| {
                    if matches_labels(p, selector) {
                        Some(p)
                    } else {
                        None
                    }
                })
                .collect();
            let total = matching.len() as i64;
            let healthy = matching.iter().filter(|p| Self::pod_is_ready(p)).count() as i64;
            let allowed = Self::compute_allowed_disruptions(
                Self::min_available(pdb_value),
                Self::max_unavailable(pdb_value),
                healthy,
                total,
            );

            // Only patch if the status would change.
            let current_healthy = pdb_value
                .get("status")
                .and_then(|s| s.get("currentHealthy"))
                .and_then(|n| n.as_i64());
            let current_total = pdb_value
                .get("status")
                .and_then(|s| s.get("expectedPods"))
                .and_then(|n| n.as_i64());
            let current_allowed = pdb_value
                .get("status")
                .and_then(|s| s.get("disruptionsAllowed"))
                .and_then(|n| n.as_i64());
            if current_healthy == Some(healthy)
                && current_total == Some(total)
                && current_allowed == Some(allowed)
            {
                continue;
            }

            self.store
                .propose(ResourceCommand::Patch {
                    key: pdb_key.clone(),
                    patch: json!({
                        "status": {
                            "currentHealthy": healthy,
                            "expectedPods": total,
                            "disruptionsAllowed": allowed,
                        }
                    }),
                    reason: Reason::Controller,
                })
                .await
                .map_err(|e| ControllerError::Store(e.to_string()))?;
            report.objects_changed += 1;
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn min_available_reads_spec_field() {
        let pdb = json!({"spec": {"minAvailable": 2}});
        assert_eq!(PodDisruptionBudgetController::min_available(&pdb), Some(2));
    }

    #[test]
    fn max_unavailable_reads_spec_field() {
        let pdb = json!({"spec": {"maxUnavailable": 1}});
        assert_eq!(PodDisruptionBudgetController::max_unavailable(&pdb), Some(1));
    }

    #[test]
    fn selector_match_labels_extracts_spec_field() {
        let pdb = json!({"spec": {"selector": {"matchLabels": {"app": "x"}}}});
        let sel =
            PodDisruptionBudgetController::selector_match_labels(&pdb).unwrap();
        assert_eq!(sel.get("app").unwrap(), "x");
    }

    #[test]
    fn pod_is_ready_true_when_condition_true() {
        let p = json!({"status": {"conditions": [{"type": "Ready", "status": "True"}]}});
        assert!(PodDisruptionBudgetController::pod_is_ready(&p));
    }

    #[test]
    fn pod_is_ready_false_when_no_status() {
        let p = json!({"metadata": {"name": "x"}});
        assert!(!PodDisruptionBudgetController::pod_is_ready(&p));
    }

    #[test]
    fn compute_allowed_disruptions_min_available_floor() {
        // minAvailable=2, healthy=5 → allowed=3 (can lose up to 3)
        let d = PodDisruptionBudgetController::compute_allowed_disruptions(
            Some(2), None, 5, 5,
        );
        assert_eq!(d, 3);
        // minAvailable=2, healthy=2 → allowed=0 (already at floor)
        let d = PodDisruptionBudgetController::compute_allowed_disruptions(
            Some(2), None, 2, 2,
        );
        assert_eq!(d, 0);
        // minAvailable=2, healthy=1 → allowed=0 (already below; can't disrupt)
        let d = PodDisruptionBudgetController::compute_allowed_disruptions(
            Some(2), None, 1, 2,
        );
        assert_eq!(d, 0);
    }

    #[test]
    fn compute_allowed_disruptions_max_unavailable_ceiling() {
        // maxUnavailable=2, healthy=5/5 total → allowed=2
        let d = PodDisruptionBudgetController::compute_allowed_disruptions(
            None, Some(2), 5, 5,
        );
        assert_eq!(d, 2);
        // maxUnavailable=2, healthy=4/5 total (1 already unhealthy) → allowed=1
        let d = PodDisruptionBudgetController::compute_allowed_disruptions(
            None, Some(2), 4, 5,
        );
        assert_eq!(d, 1);
        // maxUnavailable=2, healthy=3/5 (2 already unhealthy) → allowed=0
        let d = PodDisruptionBudgetController::compute_allowed_disruptions(
            None, Some(2), 3, 5,
        );
        assert_eq!(d, 0);
    }

    #[test]
    fn compute_allowed_disruptions_no_budget_allows_all() {
        let d = PodDisruptionBudgetController::compute_allowed_disruptions(
            None, None, 5, 5,
        );
        assert_eq!(d, 5);
    }

    #[test]
    fn controller_name_is_stable() {
        struct F;
        #[async_trait]
        impl Controller for F {
            fn name(&self) -> &'static str { "pdb" }
            async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
                Ok(ReconcileReport::default())
            }
        }
        assert_eq!(F.name(), "pdb");
    }
}
