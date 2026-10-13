//! The `pods/eviction` subresource: policy/v1 `Eviction`.
//!
//! An eviction is a delete a `PodDisruptionBudget` may refuse. Upstream's
//! rules (`pkg/registry/core/pod/storage/eviction.go`, v1.34), which this
//! module states as pure decisions so the handler only reads and writes:
//!
//! * A pod that is Succeeded, Failed, Pending or already being deleted is
//!   deleted with no budget consulted.
//! * No budget selects it: deleted.
//! * More than one: refused (500), as upstream refuses.
//! * One: an unready pod the budget can spare (`unhealthyPodEvictionPolicy`)
//!   is deleted without spending the budget; a budget whose status lags its
//!   generation, or that allows no disruption, refuses with 429
//!   `TooManyRequests` and a `DisruptionBudget` cause; otherwise
//!   `disruptionsAllowed` is decremented, the pod is recorded in
//!   `disruptedPods`, and the pod is deleted.

use serde_json::{Value, json};

use crate::error::ApiError;

/// A decoded `Eviction`: the pod it names and the `deleteOptions` it carries.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Eviction {
    /// `deleteOptions.preconditions.uid`.
    pub uid: Option<String>,
    /// `deleteOptions.preconditions.resourceVersion`.
    pub resource_version: Option<String>,
    /// `deleteOptions.dryRun` names `All`.
    pub dry_run: bool,
}

impl Eviction {
    /// Decode the request body for the pod `name` in the path.
    ///
    /// # Errors
    ///
    /// [`ApiError::BadRequest`] for a body that is not a policy `Eviction`,
    /// or whose `metadata.name` names another pod.
    pub fn from_body(body: &Value, name: &str) -> Result<Self, ApiError> {
        let kind = body.get("kind").and_then(Value::as_str).unwrap_or("Eviction");
        let api_version = body
            .get("apiVersion")
            .and_then(Value::as_str)
            .unwrap_or("policy/v1");
        if kind != "Eviction" || !matches!(api_version, "policy/v1" | "policy/v1beta1") {
            return Err(ApiError::BadRequest(format!(
                "expected a policy/v1 Eviction, got {api_version} {kind}"
            )));
        }
        let named = body.pointer("/metadata/name").and_then(Value::as_str);
        if named.is_some_and(|n| n != name) {
            return Err(ApiError::BadRequest(
                "name in URL does not match name in Eviction object".into(),
            ));
        }
        let opts = body.get("deleteOptions");
        let pre = |f: &str| {
            opts.and_then(|o| o.pointer(&["/preconditions/", f].concat()))
                .and_then(Value::as_str)
                .map(str::to_owned)
        };
        let dry_run = opts
            .and_then(|o| o.get("dryRun"))
            .and_then(Value::as_array)
            .is_some_and(|d| d.iter().any(|v| v == "All"));
        Ok(Self {
            uid: pre("uid"),
            resource_version: pre("resourceVersion"),
            dry_run,
        })
    }
}

/// The 201 body of a granted eviction: `Status{status: Success}`.
#[must_use]
pub fn success_status(name: &str) -> Value {
    json!({
        "kind": "Status",
        "apiVersion": "v1",
        "metadata": {},
        "status": "Success",
        "details": {"name": name, "kind": "pods"},
    })
}

/// Whether the pod is deleted without consulting any budget.
#[must_use]
pub fn ignores_budgets(pod: &Value) -> bool {
    let phase = pod.pointer("/status/phase").and_then(Value::as_str);
    matches!(phase, Some("Succeeded" | "Failed" | "Pending"))
        || pod.pointer("/metadata/deletionTimestamp").is_some()
}

/// What one selecting budget allows.
#[derive(Debug, Clone, PartialEq)]
pub enum Decision {
    /// Delete the pod; the budget is not spent.
    Delete,
    /// Spend one disruption: merge this patch into the budget (with a CAS on
    /// its revision), then delete the pod.
    Spend(Value),
}

/// Judge `pod` against the one budget that selects it, at `now` (an
/// RFC3339 stamp recorded in `disruptedPods`).
///
/// # Errors
///
/// [`ApiError::DisruptionBudget`] (429) when the budget cannot spare the pod
/// now; [`ApiError::Forbidden`] when its `disruptionsAllowed` is negative.
pub fn decide(pod: &Value, pdb: &Value, now: &str) -> Result<Decision, ApiError> {
    let pdb_name = pdb
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let status_int = |f: &str| pdb.pointer(&["/status/", f].concat()).and_then(Value::as_i64);
    let current = status_int("currentHealthy").unwrap_or(0);
    let desired = status_int("desiredHealthy").unwrap_or(0);
    if !engenho_controllers::status::pod_is_ready(pod) {
        let policy = pdb
            .pointer("/spec/unhealthyPodEvictionPolicy")
            .and_then(Value::as_str)
            .unwrap_or("IfHealthyBudget");
        if policy == "AlwaysAllow" || (current >= desired && desired > 0) {
            return Ok(Decision::Delete);
        }
    }
    let generation = pdb.pointer("/metadata/generation").and_then(Value::as_i64);
    let observed = status_int("observedGeneration");
    if observed.is_none() || observed < generation {
        return Err(ApiError::DisruptionBudget(format!(
            "The disruption budget {pdb_name} is still being processed by the server."
        )));
    }
    let allowed = status_int("disruptionsAllowed").unwrap_or(0);
    if allowed < 0 {
        return Err(ApiError::Forbidden(
            "pdb disruptions allowed is negative".into(),
        ));
    }
    if allowed == 0 {
        return Err(ApiError::DisruptionBudget(format!(
            "The disruption budget {pdb_name} needs {desired} healthy pods and has {current} currently"
        )));
    }
    let pod_name = pod
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    Ok(Decision::Spend(json!({
        "status": {
            "disruptionsAllowed": allowed - 1,
            "disruptedPods": { pod_name: now },
        }
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pod(ready: bool) -> Value {
        json!({"metadata": {"name": "web-0"}, "status": {"phase": "Running",
            "conditions": [{"type": "Ready", "status": if ready { "True" } else { "False" }}]}})
    }

    fn pdb(allowed: i64, current: i64, desired: i64) -> Value {
        json!({"metadata": {"name": "web", "generation": 2},
            "status": {"observedGeneration": 2, "disruptionsAllowed": allowed,
                "currentHealthy": current, "desiredHealthy": desired}})
    }

    #[test]
    fn a_budget_with_room_is_spent_and_records_the_pod() {
        let d = decide(&pod(true), &pdb(1, 3, 2), "2026-10-13T12:00:00Z").unwrap();
        assert_eq!(
            d,
            Decision::Spend(json!({"status": {"disruptionsAllowed": 0,
                "disruptedPods": {"web-0": "2026-10-13T12:00:00Z"}}}))
        );
    }

    #[test]
    fn a_spent_budget_is_a_429_naming_it() {
        let err = decide(&pod(true), &pdb(0, 2, 2), "t").unwrap_err();
        assert!(matches!(&err, ApiError::DisruptionBudget(m) if m.contains("web needs 2 healthy pods and has 2")));
    }

    #[test]
    fn a_lagging_budget_is_a_429() {
        let mut lagging = pdb(5, 3, 1);
        lagging["metadata"]["generation"] = json!(3);
        assert!(matches!(
            decide(&pod(true), &lagging, "t"),
            Err(ApiError::DisruptionBudget(_))
        ));
        let unobserved = json!({"metadata": {"name": "web", "generation": 1}, "status": {}});
        assert!(matches!(
            decide(&pod(true), &unobserved, "t"),
            Err(ApiError::DisruptionBudget(_))
        ));
    }

    #[test]
    fn an_unready_pod_a_healthy_budget_can_spare_does_not_spend_it() {
        assert_eq!(decide(&pod(false), &pdb(0, 2, 2), "t").unwrap(), Decision::Delete);
        let mut always = pdb(0, 0, 2);
        always["spec"] = json!({"unhealthyPodEvictionPolicy": "AlwaysAllow"});
        assert_eq!(decide(&pod(false), &always, "t").unwrap(), Decision::Delete);
        assert!(decide(&pod(false), &pdb(0, 1, 2), "t").is_err());
    }

    #[test]
    fn terminal_pending_and_terminating_pods_skip_budgets() {
        for phase in ["Succeeded", "Failed", "Pending"] {
            assert!(ignores_budgets(&json!({"status": {"phase": phase}})));
        }
        assert!(ignores_budgets(
            &json!({"metadata": {"deletionTimestamp": "x"}, "status": {"phase": "Running"}})
        ));
        assert!(!ignores_budgets(&pod(true)));
    }

    #[test]
    fn the_body_must_name_the_pod_in_the_path() {
        let body = json!({"apiVersion": "policy/v1", "kind": "Eviction",
            "metadata": {"name": "web-0"},
            "deleteOptions": {"preconditions": {"uid": "u1"}, "dryRun": ["All"]}});
        let e = Eviction::from_body(&body, "web-0").unwrap();
        assert_eq!(e.uid.as_deref(), Some("u1"));
        assert!(e.dry_run);
        assert!(Eviction::from_body(&body, "web-1").is_err());
        assert!(Eviction::from_body(&json!({"kind": "Pod", "apiVersion": "v1"}), "web-0").is_err());
    }
}
