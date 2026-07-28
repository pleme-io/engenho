//! R19 — PodDisruptionBudget controller.
//!
//! Tracks per-pod-set disruption budgets. The substrate's
//! voluntary-eviction path (the `/eviction` subresource — a SEPARATE
//! later brick) consults the budget's `status.disruptionsAllowed`
//! before allowing a voluntary pod delete; this controller maintains
//! that status (the number it reads) so the gate has live state.
//!
//! ## Reconcile rule
//!
//! For each `policy/v1 PodDisruptionBudget`:
//!   * Find pods in the PDB's namespace matching `spec.selector`
//!     (reuses [`crate::selector::matches_labels`] +
//!     [`crate::selector::selector_match_labels`] — the same
//!     selector logic the EndpointsController uses; no fork).
//!   * `expectedPods` = matched pods.
//!   * `currentHealthy` = matched pods that are Ready (status.conditions
//!     `Ready=True`).
//!   * `desiredHealthy` is resolved from the `minAvailable` / `maxUnavailable`
//!     [`IntOrString`] (mutually exclusive; int → that number, `"N%"` →
//!     `ceil(N/100 × expectedPods)`):
//!       - `minAvailable` set ⇒ `desiredHealthy = resolve(minAvailable)`.
//!       - `maxUnavailable` set ⇒ `desiredHealthy = expectedPods − resolve(maxUnavailable)`.
//!       - neither set ⇒ default `minAvailable` semantics (`desiredHealthy = 0`).
//!   * `disruptionsAllowed = max(0, currentHealthy − desiredHealthy)`.
//!   * `observedGeneration = metadata.generation`.
//!
//! Decisions stay idempotent — the status patch only fires when one of
//! the five computed numbers changes (no churn on a steady re-tick).

use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand},
};
use serde_json::{Value, json};

use crate::controller::{Controller, ReconcileOutcome, ReconcileReport};
use crate::error::ControllerError;
use crate::selector::{matches_labels, selector_match_labels};

/// The five status numbers a PDB reconcile computes. Carrying them in
/// one typed struct keeps the "compute" step (pure, unit-testable) and
/// the "did anything change?" comparison (idempotency) honest — a new
/// status field can't be silently dropped from the comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PdbStatus {
    /// `status.currentHealthy` — matched pods that are Ready.
    pub current_healthy: i64,
    /// `status.desiredHealthy` — minimum healthy pods the budget wants.
    pub desired_healthy: i64,
    /// `status.expectedPods` — matched pods (selector total).
    pub expected_pods: i64,
    /// `status.disruptionsAllowed` — `max(0, currentHealthy − desiredHealthy)`.
    pub disruptions_allowed: i64,
    /// `status.observedGeneration` — the PDB's `metadata.generation`.
    pub observed_generation: i64,
}

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

    fn min_available(pdb: &Value) -> Option<&Value> {
        pdb.get("spec").and_then(|s| s.get("minAvailable"))
    }

    fn max_unavailable(pdb: &Value) -> Option<&Value> {
        pdb.get("spec").and_then(|s| s.get("maxUnavailable"))
    }

    /// `metadata.generation` off the stored PDB (the store stamps it on
    /// every create/replace/patch). Absent (e.g. a hand-constructed test
    /// object) → 0.
    fn generation(pdb: &Value) -> i64 {
        pdb.get("metadata")
            .and_then(|m| m.get("generation"))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0)
    }

    /// True iff `status.conditions` carries `type=Ready,status=True`.
    /// A pod without a status (unbound / not yet reported by kubelet) is
    /// NOT ready — it never counts toward `currentHealthy`.
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

    /// Resolve a `minAvailable` / `maxUnavailable` [`IntOrString`] against a
    /// pod total. A bare integer JSON value resolves to itself; a string of
    /// the form `"N%"` resolves to `ceil(N/100 × total)` (K8s rounds the
    /// percentage UP for `minAvailable`/`maxUnavailable`); any other shape
    /// (malformed string, null, object) resolves to `None`.
    ///
    /// Exposed for tests — this is the int-or-percent parse the whole
    /// `desiredHealthy` computation rests on.
    #[must_use]
    pub fn resolve_int_or_percent(value: &Value, total: i64) -> Option<i64> {
        if let Some(n) = value.as_i64() {
            return Some(n);
        }
        let s = value.as_str()?;
        let pct_str = s.strip_suffix('%')?;
        let pct: i64 = pct_str.trim().parse().ok()?;
        // ceil(pct × total / 100) with non-negative inputs.
        let numer = pct.saturating_mul(total);
        Some(numer.div_euclid(100) + i64::from(numer.rem_euclid(100) != 0))
    }

    /// Pure: compute the five status numbers from the PDB's
    /// `minAvailable`/`maxUnavailable` (int-or-percent), the healthy +
    /// total matched-pod counts, and the PDB generation. Exposed for tests.
    ///
    /// `desiredHealthy`:
    ///   * `minAvailable` set ⇒ `resolve(minAvailable, total)`.
    ///   * else `maxUnavailable` set ⇒ `total − resolve(maxUnavailable, total)`.
    ///   * else (neither) ⇒ 0 (default `minAvailable` semantics).
    ///
    /// `disruptionsAllowed = max(0, currentHealthy − desiredHealthy)`
    /// (clamped ≥ 0). `desiredHealthy` is itself clamped ≥ 0 so a
    /// `maxUnavailable` larger than `total` can't drive it negative.
    #[must_use]
    pub fn compute_status(
        min_available: Option<&Value>,
        max_unavailable: Option<&Value>,
        healthy: i64,
        total: i64,
        generation: i64,
    ) -> PdbStatus {
        let desired_healthy = if let Some(min) = min_available {
            Self::resolve_int_or_percent(min, total).unwrap_or(0)
        } else if let Some(max) = max_unavailable {
            let allowed_unavailable = Self::resolve_int_or_percent(max, total).unwrap_or(0);
            total - allowed_unavailable
        } else {
            // Neither set — default minAvailable semantics (nothing required).
            0
        }
        .max(0);

        let disruptions_allowed = (healthy - desired_healthy).max(0);

        PdbStatus {
            current_healthy: healthy,
            desired_healthy,
            expected_pods: total,
            disruptions_allowed,
            observed_generation: generation,
        }
    }

    /// Read the five status numbers already on the stored PDB (for the
    /// idempotency comparison). A field absent → that arm of the compared
    /// `Option` is `None`, so the very first reconcile (no status yet)
    /// always differs and writes.
    fn current_status(pdb: &Value) -> Option<PdbStatus> {
        let status = pdb.get("status")?;
        let read = |k: &str| status.get(k).and_then(serde_json::Value::as_i64);
        Some(PdbStatus {
            current_healthy: read("currentHealthy")?,
            desired_healthy: read("desiredHealthy")?,
            expected_pods: read("expectedPods")?,
            disruptions_allowed: read("disruptionsAllowed")?,
            observed_generation: read("observedGeneration")?,
        })
    }
}

#[async_trait]
impl Controller for PodDisruptionBudgetController {
    fn name(&self) -> &'static str {
        "pdb"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let pdbs = self
            .store
            .list(
                "policy",
                "v1",
                "PodDisruptionBudget",
                self.namespace.as_deref(),
            )
            .await;
        let mut report = ReconcileReport::default();
        report.objects_examined = pdbs.len();

        for (pdb_key, pdb_value) in &pdbs {
            // A null selector matches no pods (K8s convention); our
            // `selector_match_labels` returns None for an absent selector,
            // which we treat as "nothing to budget" and skip.
            let Some(selector) = selector_match_labels(pdb_value) else {
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

            let desired = Self::compute_status(
                Self::min_available(pdb_value),
                Self::max_unavailable(pdb_value),
                healthy,
                total,
                Self::generation(pdb_value),
            );

            // Idempotent: skip the patch when the live status already
            // matches all five computed numbers.
            if Self::current_status(pdb_value) == Some(desired) {
                continue;
            }

            self.store
                .propose(ResourceCommand::patch(
                    pdb_key.clone(),
                    json!({
                        "status": {
                            "currentHealthy": desired.current_healthy,
                            "desiredHealthy": desired.desired_healthy,
                            "expectedPods": desired.expected_pods,
                            "disruptionsAllowed": desired.disruptions_allowed,
                            "observedGeneration": desired.observed_generation,
                        }
                    }),
                    Reason::Controller,
                ))
                .await?;
            report.objects_changed += 1;
        }
        Ok(report.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_store::resource::ResourceKey;
    use serde_json::json;

    // ── pure helpers ──────────────────────────────────────────────────

    #[test]
    fn min_available_reads_spec_field() {
        let pdb = json!({"spec": {"minAvailable": 2}});
        assert_eq!(
            PodDisruptionBudgetController::min_available(&pdb).and_then(serde_json::Value::as_i64),
            Some(2)
        );
    }

    #[test]
    fn max_unavailable_reads_spec_field() {
        let pdb = json!({"spec": {"maxUnavailable": 1}});
        assert_eq!(
            PodDisruptionBudgetController::max_unavailable(&pdb)
                .and_then(serde_json::Value::as_i64),
            Some(1)
        );
    }

    #[test]
    fn pod_is_ready_true_when_condition_true() {
        let p = json!({"status": {"conditions": [{"type": "Ready", "status": "True"}]}});
        assert!(PodDisruptionBudgetController::pod_is_ready(&p));
    }

    #[test]
    fn pod_is_ready_false_when_condition_false() {
        let p = json!({"status": {"conditions": [{"type": "Ready", "status": "False"}]}});
        assert!(!PodDisruptionBudgetController::pod_is_ready(&p));
    }

    #[test]
    fn pod_is_ready_false_when_no_status() {
        let p = json!({"metadata": {"name": "x"}});
        assert!(!PodDisruptionBudgetController::pod_is_ready(&p));
    }

    // ── int-or-percent IntOrString parse ──────────────────────────────

    #[test]
    fn resolve_int_or_percent_bare_int() {
        assert_eq!(
            PodDisruptionBudgetController::resolve_int_or_percent(&json!(2), 10),
            Some(2)
        );
    }

    #[test]
    fn resolve_int_or_percent_percent_ceils() {
        // 50% of 4 → ceil(2.0) = 2.
        assert_eq!(
            PodDisruptionBudgetController::resolve_int_or_percent(&json!("50%"), 4),
            Some(2)
        );
        // 50% of 3 → ceil(1.5) = 2.
        assert_eq!(
            PodDisruptionBudgetController::resolve_int_or_percent(&json!("50%"), 3),
            Some(2)
        );
        // 30% of 10 → ceil(3.0) = 3.
        assert_eq!(
            PodDisruptionBudgetController::resolve_int_or_percent(&json!("30%"), 10),
            Some(3)
        );
        // 100% of 5 → 5.
        assert_eq!(
            PodDisruptionBudgetController::resolve_int_or_percent(&json!("100%"), 5),
            Some(5)
        );
    }

    #[test]
    fn resolve_int_or_percent_malformed_is_none() {
        assert_eq!(
            PodDisruptionBudgetController::resolve_int_or_percent(&json!("abc"), 4),
            None
        );
        assert_eq!(
            PodDisruptionBudgetController::resolve_int_or_percent(&json!("50"), 4),
            None
        );
        assert_eq!(
            PodDisruptionBudgetController::resolve_int_or_percent(&json!(null), 4),
            None
        );
    }

    // ── compute_status (the brick's core math) ────────────────────────

    #[test]
    fn compute_status_min_available_int() {
        // minAvailable=2, 3 ready / 3 total → desired=2, allowed=1.
        let s = PodDisruptionBudgetController::compute_status(Some(&json!(2)), None, 3, 3, 7);
        assert_eq!(s.expected_pods, 3);
        assert_eq!(s.current_healthy, 3);
        assert_eq!(s.desired_healthy, 2);
        assert_eq!(s.disruptions_allowed, 1);
        assert_eq!(s.observed_generation, 7);
    }

    #[test]
    fn compute_status_min_available_percent() {
        // minAvailable="50%", 3 ready / 4 total → desired=ceil(2.0)=2, allowed=1.
        let s = PodDisruptionBudgetController::compute_status(Some(&json!("50%")), None, 3, 4, 1);
        assert_eq!(s.expected_pods, 4);
        assert_eq!(s.current_healthy, 3);
        assert_eq!(s.desired_healthy, 2);
        assert_eq!(s.disruptions_allowed, 1);
    }

    #[test]
    fn compute_status_max_unavailable_int() {
        // maxUnavailable=1, 3 ready / 3 total → desired = 3-1 = 2, allowed=1.
        let s = PodDisruptionBudgetController::compute_status(None, Some(&json!(1)), 3, 3, 1);
        assert_eq!(s.desired_healthy, 2);
        assert_eq!(s.disruptions_allowed, 1);
    }

    #[test]
    fn compute_status_max_unavailable_percent() {
        // maxUnavailable="25%" of 4 → ceil(1.0)=1; desired = 4-1 = 3.
        // 4 ready / 4 total → allowed = 4-3 = 1.
        let s = PodDisruptionBudgetController::compute_status(None, Some(&json!("25%")), 4, 4, 1);
        assert_eq!(s.desired_healthy, 3);
        assert_eq!(s.disruptions_allowed, 1);
    }

    #[test]
    fn compute_status_clamps_disruptions_allowed_at_zero() {
        // minAvailable=3, only 2 healthy → desired=3, allowed=max(0,2-3)=0.
        let s = PodDisruptionBudgetController::compute_status(Some(&json!(3)), None, 2, 3, 1);
        assert_eq!(s.desired_healthy, 3);
        assert_eq!(s.disruptions_allowed, 0);
        // currentHealthy == desiredHealthy → also 0.
        let s = PodDisruptionBudgetController::compute_status(Some(&json!(2)), None, 2, 2, 1);
        assert_eq!(s.disruptions_allowed, 0);
    }

    #[test]
    fn compute_status_max_unavailable_cannot_drive_desired_negative() {
        // maxUnavailable=5 but only 3 pods → desired = max(0, 3-5) = 0.
        let s = PodDisruptionBudgetController::compute_status(None, Some(&json!(5)), 3, 3, 1);
        assert_eq!(s.desired_healthy, 0);
        assert_eq!(s.disruptions_allowed, 3);
    }

    #[test]
    fn compute_status_neither_defaults_to_zero_desired() {
        let s = PodDisruptionBudgetController::compute_status(None, None, 5, 5, 1);
        assert_eq!(s.desired_healthy, 0);
        assert_eq!(s.disruptions_allowed, 5);
    }

    #[test]
    fn controller_name_is_stable() {
        struct F;
        #[async_trait]
        impl Controller for F {
            fn name(&self) -> &'static str {
                "pdb"
            }
            async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
                Ok(ReconcileReport::default().into())
            }
        }
        assert_eq!(F.name(), "pdb");
    }

    // ── live-store reconcile (mockable, no real cluster) ──────────────

    async fn live_store() -> Arc<StoreMesh> {
        use engenho_store::{InProcessRouter, default_config};
        use std::time::Duration;
        let router = InProcessRouter::new();
        let cfg = default_config("controllers-pdb").unwrap();
        let store = Arc::new(
            StoreMesh::start(1, "in-process://1".into(), router, cfg)
                .await
                .unwrap(),
        );
        store.initialize_singleton().await.unwrap();
        assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
        store
    }

    /// Seed a Pod with the given labels + readiness.
    async fn put_pod(store: &Arc<StoreMesh>, ns: &str, name: &str, labels: Value, ready: bool) {
        let conditions = if ready {
            json!([{"type": "Ready", "status": "True"}])
        } else {
            json!([{"type": "Ready", "status": "False"}])
        };
        store
            .propose(ResourceCommand::put(
                ResourceKey::namespaced("", "v1", "Pod", ns, name),
                json!({
                    "kind": "Pod", "apiVersion": "v1",
                    "metadata": {"name": name, "namespace": ns, "labels": labels},
                    "status": {"conditions": conditions}
                }),
                Reason::Operator,
            ))
            .await
            .unwrap();
    }

    /// Seed a PDB; returns its key.
    async fn put_pdb(store: &Arc<StoreMesh>, ns: &str, name: &str, spec: Value) -> ResourceKey {
        let key = ResourceKey::namespaced("policy", "v1", "PodDisruptionBudget", ns, name);
        store
            .propose(ResourceCommand::put(
                key.clone(),
                json!({
                    "kind": "PodDisruptionBudget", "apiVersion": "policy/v1",
                    "metadata": {"name": name, "namespace": ns},
                    "spec": spec
                }),
                Reason::Operator,
            ))
            .await
            .unwrap();
        key
    }

    fn status_of(pdb: &Value) -> (i64, i64, i64, i64) {
        let s = pdb.get("status").expect("status patched");
        (
            s["currentHealthy"].as_i64().unwrap(),
            s["desiredHealthy"].as_i64().unwrap(),
            s["expectedPods"].as_i64().unwrap(),
            s["disruptionsAllowed"].as_i64().unwrap(),
        )
    }

    #[tokio::test]
    async fn min_available_two_three_ready_pods() {
        let store = live_store().await;
        let key = put_pdb(
            &store,
            "ns1",
            "pdb",
            json!({"minAvailable": 2, "selector": {"matchLabels": {"app": "x"}}}),
        )
        .await;
        for n in ["p1", "p2", "p3"] {
            put_pod(&store, "ns1", n, json!({"app": "x"}), true).await;
        }

        let c = PodDisruptionBudgetController::new(store.clone(), None);
        let out = c.tick().await.unwrap();
        assert_eq!(out.objects_changed, 1);

        let pdb = store.get(&key).await.unwrap();
        // currentHealthy=3, desiredHealthy=2, expectedPods=3, disruptionsAllowed=1.
        assert_eq!(status_of(&pdb), (3, 2, 3, 1));
        // observedGeneration tracks metadata.generation (store-stamped).
        let generation = pdb["metadata"]["generation"].as_i64().unwrap();
        assert_eq!(
            pdb["status"]["observedGeneration"].as_i64().unwrap(),
            generation
        );
    }

    #[tokio::test]
    async fn min_available_percent_50_of_four() {
        let store = live_store().await;
        let key = put_pdb(
            &store,
            "ns1",
            "pdb",
            json!({"minAvailable": "50%", "selector": {"matchLabels": {"app": "x"}}}),
        )
        .await;
        // 4 matching pods, 3 ready.
        put_pod(&store, "ns1", "p1", json!({"app": "x"}), true).await;
        put_pod(&store, "ns1", "p2", json!({"app": "x"}), true).await;
        put_pod(&store, "ns1", "p3", json!({"app": "x"}), true).await;
        put_pod(&store, "ns1", "p4", json!({"app": "x"}), false).await;

        PodDisruptionBudgetController::new(store.clone(), None)
            .tick()
            .await
            .unwrap();

        let pdb = store.get(&key).await.unwrap();
        // desiredHealthy=ceil(0.5×4)=2, currentHealthy=3, expected=4, allowed=1.
        assert_eq!(status_of(&pdb), (3, 2, 4, 1));
    }

    #[tokio::test]
    async fn max_unavailable_one_three_ready_pods() {
        let store = live_store().await;
        let key = put_pdb(
            &store,
            "ns1",
            "pdb",
            json!({"maxUnavailable": 1, "selector": {"matchLabels": {"app": "x"}}}),
        )
        .await;
        for n in ["p1", "p2", "p3"] {
            put_pod(&store, "ns1", n, json!({"app": "x"}), true).await;
        }

        PodDisruptionBudgetController::new(store.clone(), None)
            .tick()
            .await
            .unwrap();

        let pdb = store.get(&key).await.unwrap();
        // desiredHealthy=3-1=2, currentHealthy=3, expected=3, allowed=1.
        assert_eq!(status_of(&pdb), (3, 2, 3, 1));
    }

    #[tokio::test]
    async fn disruptions_allowed_clamped_to_zero() {
        let store = live_store().await;
        // minAvailable=3 but only 2 ready → desired=3, currentHealthy=2,
        // disruptionsAllowed=max(0, 2-3)=0.
        let key = put_pdb(
            &store,
            "ns1",
            "pdb",
            json!({"minAvailable": 3, "selector": {"matchLabels": {"app": "x"}}}),
        )
        .await;
        put_pod(&store, "ns1", "p1", json!({"app": "x"}), true).await;
        put_pod(&store, "ns1", "p2", json!({"app": "x"}), true).await;
        put_pod(&store, "ns1", "p3", json!({"app": "x"}), false).await;

        PodDisruptionBudgetController::new(store.clone(), None)
            .tick()
            .await
            .unwrap();

        let pdb = store.get(&key).await.unwrap();
        // currentHealthy=2 (one unready), desired=3, expected=3, allowed=0.
        assert_eq!(status_of(&pdb), (2, 3, 3, 0));
    }

    #[tokio::test]
    async fn unready_pods_do_not_count_toward_current_healthy() {
        let store = live_store().await;
        let key = put_pdb(
            &store,
            "ns1",
            "pdb",
            json!({"minAvailable": 1, "selector": {"matchLabels": {"app": "x"}}}),
        )
        .await;
        // 3 matching, only 1 ready.
        put_pod(&store, "ns1", "p1", json!({"app": "x"}), true).await;
        put_pod(&store, "ns1", "p2", json!({"app": "x"}), false).await;
        put_pod(&store, "ns1", "p3", json!({"app": "x"}), false).await;

        PodDisruptionBudgetController::new(store.clone(), None)
            .tick()
            .await
            .unwrap();

        let pdb = store.get(&key).await.unwrap();
        // expected=3 (all match), currentHealthy=1 (only one ready).
        assert_eq!(status_of(&pdb), (1, 1, 3, 0));
    }

    #[tokio::test]
    async fn selector_only_counts_matching_pods() {
        let store = live_store().await;
        let key = put_pdb(
            &store,
            "ns1",
            "pdb",
            json!({"minAvailable": 1, "selector": {"matchLabels": {"app": "x"}}}),
        )
        .await;
        // 2 matching ready + 1 non-matching ready pod.
        put_pod(&store, "ns1", "p1", json!({"app": "x"}), true).await;
        put_pod(&store, "ns1", "p2", json!({"app": "x"}), true).await;
        put_pod(&store, "ns1", "other", json!({"app": "y"}), true).await;

        PodDisruptionBudgetController::new(store.clone(), None)
            .tick()
            .await
            .unwrap();

        let pdb = store.get(&key).await.unwrap();
        // expected=2 (only app=x), currentHealthy=2, desired=1, allowed=1.
        assert_eq!(status_of(&pdb), (2, 1, 2, 1));
    }

    #[tokio::test]
    async fn re_tick_is_idempotent_no_change() {
        let store = live_store().await;
        let key = put_pdb(
            &store,
            "ns1",
            "pdb",
            json!({"minAvailable": 2, "selector": {"matchLabels": {"app": "x"}}}),
        )
        .await;
        for n in ["p1", "p2", "p3"] {
            put_pod(&store, "ns1", n, json!({"app": "x"}), true).await;
        }

        let c = PodDisruptionBudgetController::new(store.clone(), None);
        let first = c.tick().await.unwrap();
        assert_eq!(first.objects_changed, 1);
        let rv_after_first = store.get(&key).await.unwrap()["metadata"]["resourceVersion"].clone();

        // Re-tick with no pod change: status already matches → NoChange.
        let second = c.tick().await.unwrap();
        assert_eq!(
            second.objects_changed, 0,
            "idempotent re-tick writes nothing"
        );
        let rv_after_second = store.get(&key).await.unwrap()["metadata"]["resourceVersion"].clone();
        assert_eq!(
            rv_after_first, rv_after_second,
            "no status churn on a steady re-tick"
        );
    }
}
