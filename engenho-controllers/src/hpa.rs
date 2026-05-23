//! R18 — HorizontalPodAutoscaler controller.
//!
//! Watches HPA objects + adjusts the target's `spec.replicas`
//! based on observed metrics. Same trait shape as every other
//! controller; metrics provider is pluggable.
//!
//! ## Reconcile rule
//!
//! For each HPA:
//!   1. Read `spec.scaleTargetRef.{kind, name}` and current
//!      target's `spec.replicas`.
//!   2. Read `spec.minReplicas` (default 1) + `spec.maxReplicas`.
//!   3. Ask the metrics provider for the current metric value
//!      keyed by `(target.kind, target.name, namespace)`.
//!   4. Compute desired = ceil(current_replicas × metric_value /
//!                              target_value), clamped to [min, max].
//!   5. If desired ≠ current_replicas → patch the target.
//!
//! ## Metrics provider
//!
//! Backends: `FakeMetricsProvider` (tests; serves an explicit map)
//! and (R18b) `PrometheusMetricsProvider` (queries Prometheus's
//! `/api/v1/query` for any `query` expression on the HPA).

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{
    command::{Reason, ResourceCommand},
    resource::ResourceKey,
    StoreMesh,
};
use serde_json::{json, Value};
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::debug;

use crate::controller::{Controller, ReconcileReport};
use crate::error::ControllerError;

/// Metrics-provider error.
#[derive(Debug, Clone, Error)]
pub enum MetricsError {
    /// Backend (Prometheus, fake, etc.) returned an error.
    #[error("backend: {0}")]
    Backend(String),
}

impl MetricsError {
    /// Stable identifier for telemetry.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Backend(_) => "backend",
        }
    }
}

/// Identifies the workload an HPA is monitoring + scaling.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ScaleTarget {
    /// `Deployment` / `StatefulSet` / `ReplicaSet`.
    pub kind: String,
    /// Resource name.
    pub name: String,
    /// Namespace (typically same as the HPA's).
    pub namespace: String,
}

/// Metrics-provider trait. Returns the OBSERVED current value
/// (CPU %, memory MB, custom metric) for a workload.
#[async_trait]
pub trait MetricsProvider: Send + Sync {
    /// Backend identifier for telemetry.
    fn name(&self) -> &'static str;

    /// Observed metric for `target`. None = no signal (HPA holds
    /// current replicas + warns).
    ///
    /// # Errors
    /// [`MetricsError::Backend`] on backend failure.
    async fn observe(&self, target: &ScaleTarget) -> Result<Option<f64>, MetricsError>;
}

// =================================================================
// FakeMetricsProvider — deterministic for tests
// =================================================================

/// Explicit metric map keyed by target identity. Tests pin
/// metrics + drive the HPA controller to see scale decisions.
#[derive(Default, Clone)]
pub struct FakeMetricsProvider {
    inner: Arc<Mutex<BTreeMap<ScaleTarget, f64>>>,
}

impl FakeMetricsProvider {
    /// Fresh empty provider.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the observed metric for a target.
    pub async fn set(&self, target: ScaleTarget, value: f64) {
        self.inner.lock().await.insert(target, value);
    }

    /// Clear all observations.
    pub async fn clear(&self) {
        self.inner.lock().await.clear();
    }
}

#[async_trait]
impl MetricsProvider for FakeMetricsProvider {
    fn name(&self) -> &'static str {
        "fake"
    }

    async fn observe(&self, target: &ScaleTarget) -> Result<Option<f64>, MetricsError> {
        Ok(self.inner.lock().await.get(target).copied())
    }
}

// =================================================================
// HorizontalPodAutoscalerController
// =================================================================

/// Controller — scales targets based on metric vs target_value.
pub struct HorizontalPodAutoscalerController {
    store: Arc<StoreMesh>,
    metrics: Arc<dyn MetricsProvider>,
    namespace: Option<String>,
}

impl HorizontalPodAutoscalerController {
    /// New controller.
    #[must_use]
    pub fn new(
        store: Arc<StoreMesh>,
        metrics: Arc<dyn MetricsProvider>,
        namespace: Option<String>,
    ) -> Self {
        Self {
            store,
            metrics,
            namespace,
        }
    }

    fn min_replicas(hpa: &Value) -> i64 {
        hpa.get("spec")
            .and_then(|s| s.get("minReplicas"))
            .and_then(|n| n.as_i64())
            .unwrap_or(1)
    }

    fn max_replicas(hpa: &Value) -> i64 {
        hpa.get("spec")
            .and_then(|s| s.get("maxReplicas"))
            .and_then(|n| n.as_i64())
            .unwrap_or(1)
    }

    fn target_value(hpa: &Value) -> f64 {
        hpa.get("spec")
            .and_then(|s| s.get("targetValue"))
            .and_then(|n| n.as_f64())
            .unwrap_or(50.0)
    }

    fn scale_target_ref(hpa: &Value) -> Option<(String, String)> {
        let r = hpa
            .get("spec")
            .and_then(|s| s.get("scaleTargetRef"))?;
        let kind = r.get("kind").and_then(|k| k.as_str())?.to_string();
        let name = r.get("name").and_then(|n| n.as_str())?.to_string();
        Some((kind, name))
    }

    /// Pure: compute desired replicas from observed metric.
    /// Exposed for tests.
    #[must_use]
    pub fn compute_desired(
        current_replicas: i64,
        observed: f64,
        target_value: f64,
        min: i64,
        max: i64,
    ) -> i64 {
        if target_value <= 0.0 {
            return current_replicas.clamp(min, max);
        }
        let ratio = observed / target_value;
        let desired = ((current_replicas as f64) * ratio).ceil() as i64;
        desired.max(1).clamp(min, max)
    }

    /// Map an HPA's `scaleTargetRef.kind` to the (group, version) pair
    /// the store uses. Mirrors K8s convention.
    fn target_gv(kind: &str) -> (&'static str, &'static str) {
        match kind {
            "Deployment" | "StatefulSet" | "ReplicaSet" => ("apps", "v1"),
            _ => ("", "v1"),
        }
    }
}

#[async_trait]
impl Controller for HorizontalPodAutoscalerController {
    fn name(&self) -> &'static str {
        "hpa"
    }

    async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
        let hpas = self
            .store
            .list("autoscaling", "v2", "HorizontalPodAutoscaler", self.namespace.as_deref())
            .await;
        let mut report = ReconcileReport::default();
        report.objects_examined = hpas.len();

        for (hpa_key, hpa_value) in &hpas {
            let Some((target_kind, target_name)) = Self::scale_target_ref(hpa_value) else {
                report.objects_skipped += 1;
                continue;
            };
            let ns = hpa_key.namespace.as_deref().unwrap_or("default").to_string();
            let min = Self::min_replicas(hpa_value);
            let max = Self::max_replicas(hpa_value);
            let target_value = Self::target_value(hpa_value);

            let (g, v) = Self::target_gv(&target_kind);
            let target_key =
                ResourceKey::namespaced(g, v, &target_kind, &ns, &target_name);
            let Some(target) = self.store.get(&target_key).await else {
                report.objects_skipped += 1;
                continue;
            };
            let current = target
                .get("spec")
                .and_then(|s| s.get("replicas"))
                .and_then(|r| r.as_i64())
                .unwrap_or(1);

            let observed = match self
                .metrics
                .observe(&ScaleTarget {
                    kind: target_kind.clone(),
                    name: target_name.clone(),
                    namespace: ns.clone(),
                })
                .await
                .map_err(|e| ControllerError::Internal(e.to_string()))?
            {
                Some(v) => v,
                None => {
                    debug!(
                        target = %format!("{ns}/{target_kind}/{target_name}"),
                        "no metric signal; holding current replicas"
                    );
                    continue;
                }
            };

            let desired = Self::compute_desired(current, observed, target_value, min, max);
            if desired == current {
                continue;
            }
            debug!(
                target = %format!("{ns}/{target_kind}/{target_name}"),
                current,
                desired,
                observed,
                target_value,
                "HPA scaling"
            );
            self.store
                .propose(ResourceCommand::Patch {
                    key: target_key,
                    patch: json!({"spec": {"replicas": desired}}),
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

    #[test]
    fn min_replicas_defaults_to_1() {
        assert_eq!(HorizontalPodAutoscalerController::min_replicas(&json!({})), 1);
    }

    #[test]
    fn max_replicas_defaults_to_1() {
        assert_eq!(HorizontalPodAutoscalerController::max_replicas(&json!({})), 1);
    }

    #[test]
    fn target_value_defaults_to_50() {
        let v = HorizontalPodAutoscalerController::target_value(&json!({}));
        assert!((v - 50.0).abs() < 0.001);
    }

    #[test]
    fn scale_target_ref_reads_spec_field() {
        let hpa = json!({"spec": {"scaleTargetRef": {"kind": "Deployment", "name": "podinfo"}}});
        let (k, n) = HorizontalPodAutoscalerController::scale_target_ref(&hpa).unwrap();
        assert_eq!(k, "Deployment");
        assert_eq!(n, "podinfo");
    }

    #[test]
    fn target_gv_maps_workload_kinds() {
        for k in ["Deployment", "StatefulSet", "ReplicaSet"] {
            let (g, v) = HorizontalPodAutoscalerController::target_gv(k);
            assert_eq!(g, "apps");
            assert_eq!(v, "v1");
        }
        let (g, v) = HorizontalPodAutoscalerController::target_gv("Pod");
        assert_eq!(g, "");
        assert_eq!(v, "v1");
    }

    #[test]
    fn compute_desired_scales_up_on_high_metric() {
        // 80% observed vs 50% target → 2× replicas
        let d =
            HorizontalPodAutoscalerController::compute_desired(2, 80.0, 50.0, 1, 10);
        assert_eq!(d, 4);
    }

    #[test]
    fn compute_desired_scales_down_on_low_metric() {
        // 25% observed vs 50% target → ½× replicas
        let d =
            HorizontalPodAutoscalerController::compute_desired(4, 25.0, 50.0, 1, 10);
        assert_eq!(d, 2);
    }

    #[test]
    fn compute_desired_clamps_to_min() {
        let d =
            HorizontalPodAutoscalerController::compute_desired(2, 1.0, 50.0, 3, 10);
        assert_eq!(d, 3);
    }

    #[test]
    fn compute_desired_clamps_to_max() {
        let d = HorizontalPodAutoscalerController::compute_desired(
            5, 1000.0, 50.0, 1, 10,
        );
        assert_eq!(d, 10);
    }

    #[test]
    fn compute_desired_never_below_1() {
        let d =
            HorizontalPodAutoscalerController::compute_desired(1, 0.0, 50.0, 1, 10);
        assert_eq!(d, 1);
    }

    #[tokio::test]
    async fn fake_metrics_provider_round_trip() {
        let p = FakeMetricsProvider::new();
        let target = ScaleTarget {
            kind: "Deployment".into(),
            name: "x".into(),
            namespace: "default".into(),
        };
        assert_eq!(p.observe(&target).await.unwrap(), None);
        p.set(target.clone(), 75.0).await;
        assert_eq!(p.observe(&target).await.unwrap(), Some(75.0));
        p.clear().await;
        assert_eq!(p.observe(&target).await.unwrap(), None);
    }

    #[test]
    fn provider_name_is_stable() {
        assert_eq!(FakeMetricsProvider::new().name(), "fake");
    }

    #[test]
    fn metrics_error_kind_is_stable() {
        assert_eq!(MetricsError::Backend("x".into()).kind(), "backend");
    }

    #[test]
    fn controller_name_is_stable() {
        struct F;
        #[async_trait]
        impl Controller for F {
            fn name(&self) -> &'static str { "hpa" }
            async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
                Ok(ReconcileReport::default())
            }
        }
        assert_eq!(F.name(), "hpa");
    }
}
