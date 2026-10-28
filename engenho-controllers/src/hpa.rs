//! R18 — HorizontalPodAutoscaler controller.
//!
//! Watches HPA objects + adjusts the target's `spec.replicas`
//! based on observed metrics. Same trait shape as every other
//! controller; metrics provider is pluggable.
//!
//! ## Reconcile rule
//!
//! For each HPA:
//!   1. Read `spec.scaleTargetRef.{apiVersion, kind, name}` and the
//!      target's `spec.replicas`.
//!   2. Read `spec.minReplicas` (default 1) + `spec.maxReplicas`.
//!   3. Read `spec.metrics`. Each `External` metric is asked of the
//!      metrics provider by name; with no `spec.metrics`, the provider is
//!      asked for the target's own value against `spec.targetValue`.
//!   4. Recommend replicas per metric (`AverageValue`: ⌈value / target⌉;
//!      `Value`: ⌈current × value / target⌉), take the largest, stabilize
//!      it over `spec.behavior` windows, clamp to [min, max].
//!   5. If desired ≠ current_replicas → patch the target.
//!
//! ## Metrics provider
//!
//! [`MetricsProvider`] is the seam; [`FakeMetricsProvider`] serves an
//! explicit map for tests, and [`StatusMetricsProvider`] reads declared
//! status fields from the store ([`PANGEA_LEAF_PENDING_WORK`]).

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand},
    resource::ResourceKey,
};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::{debug, warn};

use crate::controller::{Controller, ReconcileOutcome, ReconcileReport};
use crate::csi_provisioner::parse_quantity;
use crate::effect::Effect;
use crate::error::ControllerError;
use crate::meta::{DefaultedInt, REPLICAS, warn_unreadable};
use crate::reads::{DeclaresReads, Reads};

/// `autoscaling/v2` `spec.minReplicas`, defaulted to 1.
const MIN_REPLICAS: DefaultedInt = DefaultedInt::new(&["spec", "minReplicas"], 1);

/// `spec.maxReplicas` is required upstream; an absent one reads as 1 here.
const MAX_REPLICAS: DefaultedInt = DefaultedInt::new(&["spec", "maxReplicas"], 1);

const SCALE_UP_WINDOW: DefaultedInt = DefaultedInt::new(
    &["spec", "behavior", "scaleUp", "stabilizationWindowSeconds"],
    0,
);

const SCALE_DOWN_WINDOW: DefaultedInt = DefaultedInt::new(
    &[
        "spec",
        "behavior",
        "scaleDown",
        "stabilizationWindowSeconds",
    ],
    300,
);

const TOLERANCE: f64 = 0.1;

/// Metrics-provider error.
#[derive(Debug, Clone, Error)]
pub enum MetricsError {
    /// Backend (Prometheus, fake, etc.) returned an error.
    #[error("backend: {0}")]
    Backend(String),
    #[error("unreadable: {0}")]
    Unreadable(String),
}

engenho_substrate::impl_error_kind! {
    MetricsError {
        (Backend(_)) => "backend",
        (Unreadable(_)) => "unreadable",
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

    /// # Errors
    /// [`MetricsError`] when the source cannot be read.
    async fn external(&self, metric: &str) -> Result<Option<f64>, MetricsError>;
}

/// Explicit metric map keyed by target identity. Tests pin
/// metrics + drive the HPA controller to see scale decisions.
#[derive(Default, Clone)]
pub struct FakeMetricsProvider {
    inner: Arc<Mutex<BTreeMap<ScaleTarget, f64>>>,
    external: Arc<Mutex<BTreeMap<String, f64>>>,
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

    pub async fn set_external(&self, metric: &str, value: f64) {
        self.external.lock().await.insert(metric.to_owned(), value);
    }

    /// Clear all observations.
    pub async fn clear(&self) {
        self.inner.lock().await.clear();
        self.external.lock().await.clear();
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

    async fn external(&self, metric: &str) -> Result<Option<f64>, MetricsError> {
        Ok(self.external.lock().await.get(metric).copied())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StatusMetric {
    pub name: &'static str,
    pub group: &'static str,
    pub version: &'static str,
    pub kind: &'static str,
    pub object: &'static str,
    pub path: &'static [&'static str],
}

pub const PANGEA_LEAF_PENDING_WORK: StatusMetric = StatusMetric {
    name: "pangea_leaf_pending_work",
    group: "pangea.pleme.io",
    version: "v1alpha1",
    kind: "PangeaFleetStatus",
    object: "default",
    path: &["status", "leafWork", "pending"],
};

impl StatusMetric {
    fn key(self) -> ResourceKey {
        ResourceKey::cluster_scoped(self.group, self.version, self.kind, self.object)
    }

    fn read(self, object: &Value) -> Result<Option<f64>, MetricsError> {
        let unreadable = |at: &Value| {
            MetricsError::Unreadable(format!(
                "{}/{} {}: {} is {at}, expected a non-negative number",
                self.group,
                self.kind,
                self.object,
                self.path.join(".")
            ))
        };
        let mut at = object;
        for step in self.path {
            let Value::Object(map) = at else {
                return Err(unreadable(at));
            };
            match map.get(*step) {
                Some(Value::Null) | None => return Ok(None),
                Some(next) => at = next,
            }
        }
        match at.as_f64() {
            Some(n) if n.is_finite() && n >= 0.0 => Ok(Some(n)),
            _ => Err(unreadable(at)),
        }
    }
}

pub struct StatusMetricsProvider {
    store: Arc<StoreMesh>,
    metrics: Vec<StatusMetric>,
}

impl StatusMetricsProvider {
    #[must_use]
    pub fn new(store: Arc<StoreMesh>, metrics: Vec<StatusMetric>) -> Self {
        Self { store, metrics }
    }

    #[must_use]
    pub fn metrics(&self) -> &[StatusMetric] {
        &self.metrics
    }
}

#[async_trait]
impl MetricsProvider for StatusMetricsProvider {
    fn name(&self) -> &'static str {
        "status"
    }

    async fn observe(&self, _target: &ScaleTarget) -> Result<Option<f64>, MetricsError> {
        Ok(None)
    }

    async fn external(&self, metric: &str) -> Result<Option<f64>, MetricsError> {
        let Some(source) = self.metrics.iter().find(|m| m.name == metric).copied() else {
            return Ok(None);
        };
        match self.store.get(&source.key()).await {
            Some(object) => source.read(&object),
            None => Ok(None),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MetricTarget {
    AverageValue(f64),
    Value(f64),
}

#[derive(Clone, Debug, PartialEq)]
pub enum MetricSpec {
    TargetValue(f64),
    External(Vec<(String, MetricTarget)>),
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum MetricSpecError {
    #[error("spec.metrics[{index}] is type {found}; only External is served")]
    Unsupported { index: usize, found: String },
    #[error("spec.metrics[{index}]: {reason}")]
    Malformed { index: usize, reason: String },
}

fn quantity(value: &Value) -> Option<f64> {
    let n = match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => quantity_text(s.trim()),
        _ => None,
    };
    n.filter(|n| n.is_finite() && *n > 0.0)
}

fn quantity_text(s: &str) -> Option<f64> {
    if let Ok(n) = s.parse::<f64>() {
        return Some(n);
    }
    if let Some(milli) = s.strip_suffix('m') {
        return milli.parse::<f64>().ok().map(|n| n / 1000.0);
    }
    #[allow(clippy::cast_precision_loss)]
    parse_quantity(s).map(|n| n as f64)
}

impl MetricSpec {
    /// # Errors
    /// [`MetricSpecError`] naming the entry that cannot be served.
    pub fn of(hpa: &Value) -> Result<Self, MetricSpecError> {
        let spec = hpa.get("spec");
        let Some(metrics) = spec.and_then(|s| s.get("metrics")).filter(|m| !m.is_null()) else {
            let target = spec
                .and_then(|s| s.get("targetValue"))
                .and_then(Value::as_f64)
                .unwrap_or(50.0);
            return Ok(Self::TargetValue(target));
        };
        let malformed = |index: usize, reason: &str| MetricSpecError::Malformed {
            index,
            reason: reason.to_owned(),
        };
        let Some(entries) = metrics.as_array() else {
            return Err(malformed(0, "spec.metrics is not a list"));
        };
        if entries.is_empty() {
            return Err(malformed(0, "spec.metrics is empty"));
        }
        let mut out = Vec::with_capacity(entries.len());
        for (index, entry) in entries.iter().enumerate() {
            let kind = entry.get("type").and_then(Value::as_str).unwrap_or("");
            if kind != "External" {
                return Err(MetricSpecError::Unsupported {
                    index,
                    found: kind.to_owned(),
                });
            }
            let external = entry.get("external");
            let name = external
                .and_then(|e| e.get("metric"))
                .and_then(|m| m.get("name"))
                .and_then(Value::as_str)
                .ok_or_else(|| malformed(index, "external.metric.name is missing"))?;
            let target = external
                .and_then(|e| e.get("target"))
                .ok_or_else(|| malformed(index, "external.target is missing"))?;
            let target = match target.get("type").and_then(Value::as_str) {
                Some("AverageValue") => target
                    .get("averageValue")
                    .and_then(quantity)
                    .map(MetricTarget::AverageValue)
                    .ok_or_else(|| {
                        malformed(index, "target.averageValue is not a positive quantity")
                    })?,
                Some("Value") => target
                    .get("value")
                    .and_then(quantity)
                    .map(MetricTarget::Value)
                    .ok_or_else(|| malformed(index, "target.value is not a positive quantity"))?,
                _ => return Err(malformed(index, "target.type is not AverageValue or Value")),
            };
            out.push((name.to_owned(), target));
        }
        Ok(Self::External(out))
    }
}

#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
pub fn recommend(current: i64, observed: f64, target: MetricTarget) -> i64 {
    let current_f = current as f64;
    match target {
        MetricTarget::AverageValue(average) => {
            if current > 0 && (observed / (average * current_f) - 1.0).abs() <= TOLERANCE {
                return current;
            }
            (observed / average).ceil() as i64
        }
        MetricTarget::Value(value) => {
            let ratio = observed / value;
            if (ratio - 1.0).abs() <= TOLERANCE {
                return current;
            }
            (current_f * ratio).ceil() as i64
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Windows {
    pub up: Duration,
    pub down: Duration,
}

impl Windows {
    fn of(hpa: &Value) -> Result<Self, crate::meta::ShapeError> {
        let secs = |n: i64| Duration::from_secs(u64::try_from(n.max(0)).unwrap_or(0));
        Ok(Self {
            up: secs(SCALE_UP_WINDOW.read(hpa)?),
            down: secs(SCALE_DOWN_WINDOW.read(hpa)?),
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct Recommendations {
    seen: VecDeque<(Instant, i64)>,
}

impl Recommendations {
    pub fn stabilize(&mut self, now: Instant, raw: i64, current: i64, windows: Windows) -> i64 {
        self.seen.push_back((now, raw));
        let keep = windows.up.max(windows.down);
        while self
            .seen
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) > keep)
        {
            self.seen.pop_front();
        }
        let within = |window: Duration| {
            self.seen
                .iter()
                .filter(move |(at, _)| now.duration_since(*at) <= window)
                .map(|(_, r)| *r)
        };
        let up = within(windows.up).min().unwrap_or(raw);
        let down = within(windows.down).max().unwrap_or(raw);
        current.max(up).min(down)
    }
}

/// Controller — scales targets based on metric vs target_value.
pub struct HorizontalPodAutoscalerController {
    store: Arc<StoreMesh>,
    metrics: Arc<dyn MetricsProvider>,
    namespace: Option<String>,
    history: Mutex<BTreeMap<String, Recommendations>>,
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
            history: Mutex::new(BTreeMap::new()),
        }
    }

    fn scale_target_ref(hpa: &Value) -> Option<(String, String, String)> {
        let r = hpa.get("spec").and_then(|s| s.get("scaleTargetRef"))?;
        let api_version = r.get("apiVersion").and_then(|a| a.as_str())?.to_string();
        let kind = r.get("kind").and_then(|k| k.as_str())?.to_string();
        let name = r.get("name").and_then(|n| n.as_str())?.to_string();
        Some((api_version, kind, name))
    }

    /// Pure: compute desired replicas from observed metric.
    /// Exposed for tests.
    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
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

    #[must_use]
    pub fn target_gv(api_version: &str) -> (&str, &str) {
        match api_version.split_once('/') {
            Some((group, version)) => (group, version),
            None => ("", api_version),
        }
    }

    async fn raw_recommendation(
        &self,
        spec: &MetricSpec,
        target: &ScaleTarget,
        current: i64,
        min: i64,
        max: i64,
    ) -> Result<Option<i64>, MetricsError> {
        match spec {
            MetricSpec::TargetValue(target_value) => {
                Ok(self.metrics.observe(target).await?.map(|observed| {
                    Self::compute_desired(current, observed, *target_value, min, max)
                }))
            }
            MetricSpec::External(metrics) => {
                let mut best: Option<i64> = None;
                for (name, metric_target) in metrics {
                    let Some(observed) = self.metrics.external(name).await? else {
                        return Ok(None);
                    };
                    let r = recommend(current, observed, *metric_target);
                    best = Some(best.map_or(r, |b| b.max(r)));
                }
                Ok(best)
            }
        }
    }
}

impl DeclaresReads for HorizontalPodAutoscalerController {
    fn reads(&self) -> Reads {
        Reads::every()
    }
}

#[async_trait]
impl Controller for HorizontalPodAutoscalerController {
    fn name(&self) -> &'static str {
        "hpa"
    }

    #[allow(clippy::too_many_lines)]
    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let hpas = self
            .store
            .list(
                "autoscaling",
                "v2",
                "HorizontalPodAutoscaler",
                self.namespace.as_deref(),
            )
            .await;
        let mut report = ReconcileReport::default();
        report.objects_examined = hpas.len();
        let now = Instant::now();
        let mut history = self.history.lock().await;
        history.retain(|label, _| hpas.iter().any(|(key, _)| key.label() == *label));

        for (hpa_key, hpa_value) in &hpas {
            let Some((api_version, target_kind, target_name)) = Self::scale_target_ref(hpa_value)
            else {
                report.objects_skipped += 1;
                continue;
            };
            let ns = hpa_key
                .namespace
                .as_deref()
                .unwrap_or("default")
                .to_string();
            let (min, max, windows) = match (
                MIN_REPLICAS.read(hpa_value),
                MAX_REPLICAS.read(hpa_value),
                Windows::of(hpa_value),
            ) {
                (Ok(min), Ok(max), Ok(windows)) => (min, max, windows),
                (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
                    warn_unreadable("hpa", &hpa_key.label(), &e);
                    report.objects_skipped += 1;
                    continue;
                }
            };
            let spec = match MetricSpec::of(hpa_value) {
                Ok(spec) => spec,
                Err(e) => {
                    warn!(hpa = %hpa_key.label(), error = %e, "metric spec refused; target left alone");
                    report.objects_skipped += 1;
                    continue;
                }
            };

            let (g, v) = Self::target_gv(&api_version);
            let target_key = ResourceKey::namespaced(g, v, &target_kind, &ns, &target_name);
            let Some(target) = self.store.get(&target_key).await else {
                report.objects_skipped += 1;
                continue;
            };
            let current = match REPLICAS.read(&target) {
                Ok(current) => current,
                Err(e) => {
                    warn_unreadable("hpa", &target_key.label(), &e);
                    report.objects_skipped += 1;
                    continue;
                }
            };

            let scale_target = ScaleTarget {
                kind: target_kind.clone(),
                name: target_name.clone(),
                namespace: ns.clone(),
            };
            let raw = match self
                .raw_recommendation(&spec, &scale_target, current, min, max)
                .await
            {
                Ok(Some(raw)) => raw,
                Ok(None) => {
                    debug!(
                        target = %format!("{ns}/{target_kind}/{target_name}"),
                        "no metric signal; holding current replicas"
                    );
                    continue;
                }
                Err(e) => {
                    warn!(hpa = %hpa_key.label(), error = %e, "metric unreadable; holding current replicas");
                    report.objects_skipped += 1;
                    continue;
                }
            };

            let desired = history
                .entry(hpa_key.label())
                .or_default()
                .stabilize(now, raw, current, windows)
                .clamp(min, max);
            if desired == current {
                continue;
            }
            debug!(
                target = %format!("{ns}/{target_kind}/{target_name}"),
                current,
                desired,
                raw,
                "HPA scaling"
            );
            let applied = self
                .store
                .propose(ResourceCommand::patch(
                    target_key,
                    json!({"spec": {"replicas": desired}}),
                    Reason::Controller,
                ))
                .await?;
            report.record(Effect::of(applied.op));
        }
        Ok(report.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_replicas_defaults_to_1() {
        assert_eq!(MIN_REPLICAS.read(&json!({})), Ok(1));
    }

    #[test]
    fn max_replicas_defaults_to_1() {
        assert_eq!(MAX_REPLICAS.read(&json!({})), Ok(1));
    }

    #[test]
    fn a_malformed_bound_is_an_error_not_the_default() {
        assert!(
            MIN_REPLICAS
                .read(&json!({"spec": {"minReplicas": "2"}}))
                .is_err()
        );
        assert!(
            MAX_REPLICAS
                .read(&json!({"spec": {"maxReplicas": 9.5}}))
                .is_err()
        );
    }

    #[test]
    fn target_value_defaults_to_50() {
        assert_eq!(
            MetricSpec::of(&json!({})),
            Ok(MetricSpec::TargetValue(50.0))
        );
    }

    #[test]
    fn scale_target_ref_reads_spec_field() {
        let hpa = json!({"spec": {"scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "podinfo"}}});
        let (a, k, n) = HorizontalPodAutoscalerController::scale_target_ref(&hpa).unwrap();
        assert_eq!(a, "apps/v1");
        assert_eq!(k, "Deployment");
        assert_eq!(n, "podinfo");
        let no_api_version =
            json!({"spec": {"scaleTargetRef": {"kind": "Deployment", "name": "podinfo"}}});
        assert!(HorizontalPodAutoscalerController::scale_target_ref(&no_api_version).is_none());
    }

    #[test]
    fn target_gv_reads_the_targets_own_api_version() {
        let gv = HorizontalPodAutoscalerController::target_gv;
        assert_eq!(gv("apps/v1"), ("apps", "v1"));
        assert_eq!(gv("v1"), ("", "v1"));
        assert_eq!(gv("argoproj.io/v1alpha1"), ("argoproj.io", "v1alpha1"));
        assert_eq!(
            gv("pangea.pleme.io/v1alpha1"),
            ("pangea.pleme.io", "v1alpha1")
        );
    }

    #[test]
    fn compute_desired_scales_up_on_high_metric() {
        // 80% observed vs 50% target → 2× replicas
        let d = HorizontalPodAutoscalerController::compute_desired(2, 80.0, 50.0, 1, 10);
        assert_eq!(d, 4);
    }

    #[test]
    fn compute_desired_scales_down_on_low_metric() {
        // 25% observed vs 50% target → ½× replicas
        let d = HorizontalPodAutoscalerController::compute_desired(4, 25.0, 50.0, 1, 10);
        assert_eq!(d, 2);
    }

    #[test]
    fn compute_desired_clamps_to_min() {
        let d = HorizontalPodAutoscalerController::compute_desired(2, 1.0, 50.0, 3, 10);
        assert_eq!(d, 3);
    }

    #[test]
    fn compute_desired_clamps_to_max() {
        let d = HorizontalPodAutoscalerController::compute_desired(5, 1000.0, 50.0, 1, 10);
        assert_eq!(d, 10);
    }

    #[test]
    fn compute_desired_never_below_1() {
        let d = HorizontalPodAutoscalerController::compute_desired(1, 0.0, 50.0, 1, 10);
        assert_eq!(d, 1);
    }

    fn leaf_work_hpa(min: i64, max: i64, average: &Value) -> Value {
        json!({"spec": {
            "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "workers"},
            "minReplicas": min, "maxReplicas": max,
            "metrics": [{"type": "External", "external": {
                "metric": {"name": "pangea_leaf_pending_work"},
                "target": {"type": "AverageValue", "averageValue": average}
            }}]
        }})
    }

    #[test]
    fn metric_spec_reads_an_external_average_value() {
        assert_eq!(
            MetricSpec::of(&leaf_work_hpa(0, 4, &json!("600"))),
            Ok(MetricSpec::External(vec![(
                "pangea_leaf_pending_work".into(),
                MetricTarget::AverageValue(600.0)
            )]))
        );
        assert_eq!(
            MetricSpec::of(&leaf_work_hpa(0, 4, &json!(600))),
            MetricSpec::of(&leaf_work_hpa(0, 4, &json!("600")))
        );
        assert_eq!(
            MetricSpec::of(&leaf_work_hpa(0, 4, &json!("1500m"))),
            Ok(MetricSpec::External(vec![(
                "pangea_leaf_pending_work".into(),
                MetricTarget::AverageValue(1.5)
            )]))
        );
        assert_eq!(
            MetricSpec::of(&leaf_work_hpa(0, 4, &json!("1k"))),
            Ok(MetricSpec::External(vec![(
                "pangea_leaf_pending_work".into(),
                MetricTarget::AverageValue(1000.0)
            )]))
        );
    }

    #[test]
    fn metric_spec_refuses_what_it_cannot_serve() {
        let resource = json!({"spec": {"metrics": [{"type": "Resource", "resource": {}}]}});
        assert_eq!(
            MetricSpec::of(&resource),
            Err(MetricSpecError::Unsupported {
                index: 0,
                found: "Resource".into()
            })
        );
        assert!(matches!(
            MetricSpec::of(&leaf_work_hpa(0, 4, &json!("0"))),
            Err(MetricSpecError::Malformed { index: 0, .. })
        ));
        assert!(matches!(
            MetricSpec::of(&leaf_work_hpa(0, 4, &json!("lots"))),
            Err(MetricSpecError::Malformed { index: 0, .. })
        ));
        assert!(matches!(
            MetricSpec::of(&json!({"spec": {"metrics": []}})),
            Err(MetricSpecError::Malformed { .. })
        ));
    }

    #[test]
    fn average_value_is_work_per_replica() {
        let t = MetricTarget::AverageValue(600.0);
        assert_eq!(recommend(0, 0.0, t), 0);
        assert_eq!(recommend(0, 1.0, t), 1);
        assert_eq!(recommend(0, 1800.0, t), 3);
        assert_eq!(recommend(1, 1801.0, t), 4);
        assert_eq!(recommend(4, 600.0, t), 1);
    }

    #[test]
    fn average_value_holds_inside_the_tolerance() {
        let t = MetricTarget::AverageValue(600.0);
        assert_eq!(recommend(2, 1250.0, t), 2);
        assert_eq!(recommend(2, 1100.0, t), 2);
        assert_eq!(recommend(2, 1400.0, t), 3);
    }

    #[test]
    fn value_scales_by_ratio() {
        let t = MetricTarget::Value(100.0);
        assert_eq!(recommend(2, 200.0, t), 4);
        assert_eq!(recommend(4, 50.0, t), 2);
        assert_eq!(recommend(3, 105.0, t), 3);
    }

    fn windows(up: u64, down: u64) -> Windows {
        Windows {
            up: Duration::from_secs(up),
            down: Duration::from_secs(down),
        }
    }

    #[test]
    fn scale_up_is_immediate_by_default() {
        let t0 = Instant::now();
        let mut r = Recommendations::default();
        assert_eq!(r.stabilize(t0, 4, 1, windows(0, 300)), 4);
    }

    #[test]
    fn scale_down_holds_the_highest_recommendation_in_the_window() {
        let t0 = Instant::now();
        let w = windows(0, 300);
        let mut r = Recommendations::default();
        assert_eq!(r.stabilize(t0, 4, 1, w), 4);
        assert_eq!(r.stabilize(t0 + Duration::from_secs(60), 1, 4, w), 4);
        assert_eq!(r.stabilize(t0 + Duration::from_secs(299), 2, 4, w), 4);
        assert_eq!(r.stabilize(t0 + Duration::from_secs(301), 0, 4, w), 2);
        assert_eq!(r.stabilize(t0 + Duration::from_secs(700), 0, 2, w), 0);
    }

    #[test]
    fn scale_up_window_holds_the_lowest_recommendation() {
        let t0 = Instant::now();
        let w = windows(120, 0);
        let mut r = Recommendations::default();
        assert_eq!(r.stabilize(t0, 1, 1, w), 1);
        assert_eq!(r.stabilize(t0 + Duration::from_secs(30), 5, 1, w), 1);
        assert_eq!(r.stabilize(t0 + Duration::from_secs(121), 5, 1, w), 5);
    }

    #[test]
    fn windows_read_behavior_with_upstream_defaults() {
        assert_eq!(Windows::of(&json!({})), Ok(windows(0, 300)));
        let hpa = json!({"spec": {"behavior": {
            "scaleUp": {"stabilizationWindowSeconds": 30},
            "scaleDown": {"stabilizationWindowSeconds": 0}
        }}});
        assert_eq!(Windows::of(&hpa), Ok(windows(30, 0)));
    }

    #[test]
    fn leaf_work_reads_pending_as_a_non_negative_number() {
        let m = PANGEA_LEAF_PENDING_WORK;
        assert_eq!(
            m.read(&json!({"status": {"leafWork": {"pending": 1800}}}))
                .unwrap(),
            Some(1800.0)
        );
        assert_eq!(m.read(&json!({"status": {}})).unwrap(), None);
        assert_eq!(
            m.read(&json!({"status": {"leafWork": null}})).unwrap(),
            None
        );
        assert!(
            m.read(&json!({"status": {"leafWork": {"pending": -1}}}))
                .is_err()
        );
        assert!(
            m.read(&json!({"status": {"leafWork": {"pending": "9"}}}))
                .is_err()
        );
        assert!(m.read(&json!({"status": {"leafWork": 9}})).is_err());
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
        assert_eq!(MetricsError::Unreadable("x".into()).kind(), "unreadable");
    }

    #[test]
    fn controller_name_is_stable() {
        struct F;
        #[async_trait]
        impl Controller for F {
            fn name(&self) -> &'static str {
                "hpa"
            }
            async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
                Ok(ReconcileReport::default().into())
            }
        }
        assert_eq!(F.name(), "hpa");
    }
}
