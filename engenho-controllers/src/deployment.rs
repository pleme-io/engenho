//! `DeploymentController` — reconciles Deployments into
//! ReplicaSets.
//!
//! K8s rule:
//!   * each Deployment owns 1..N ReplicaSets via ownerReferences
//!   * the CURRENT ReplicaSet matches the Deployment's
//!     `spec.template` (hashed for stability)
//!   * older ReplicaSets are kept around at `replicas=0` so
//!     `kubectl rollout undo` still works (revision history)
//!
//! R9.5 implementation (this file):
//!   1. For each Deployment, compute a template hash.
//!   2. Find owned ReplicaSets (via uid).
//!   3. If no owned RS has the current template hash, create one.
//!   4. Scale the new RS to `Deployment.spec.replicas`.
//!   5. Scale older owned RSes to 0 (revision history retained).
//!
//! Skips: status updates, paused rollouts, partial-rollout
//! strategies — those are R9.5b. The substrate's good enough to
//! prove the controller pattern compounds.

use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand},
    resource::ResourceKey,
};
use serde_json::{Value, json};

use crate::error::ControllerError;
use crate::meta::ObjectMeta;
use crate::owned_children::{ChildKind, OwnedChildrenReconciler, ParentGvk, ReconcileDelta};
use crate::owner::{owner_ref_for, set_owner_reference};
use crate::status::observed_generation;

pub struct DeploymentController {
    store: Arc<StoreMesh>,
    namespace: Option<String>,
}

impl DeploymentController {
    #[must_use]
    pub fn new(store: Arc<StoreMesh>, namespace: Option<String>) -> Self {
        Self { store, namespace }
    }

    /// Deterministic hash of `spec.template`. Production K8s uses
    /// a stable rsspec-hash; for R9.5 we use a BLAKE3 hex prefix
    /// of the canonical-JSON template bytes. Good enough for
    /// template-equality without external deps (we already pull
    /// blake3 via engenho-revoada).
    fn template_hash(d: &Value) -> Option<String> {
        let template = d.get("spec").and_then(|s| s.get("template"))?;
        let bytes = serde_json::to_vec(template).ok()?;
        // Use a simple FNV-1a so we don't pull blake3 just for this.
        // 8 hex chars is plenty for the typical 1-10 revision range.
        let mut hash: u64 = 0xcbf29ce484222325;
        for b in &bytes {
            hash ^= u64::from(*b);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        Some(format!("{hash:016x}").chars().take(10).collect())
    }

    /// Build a ReplicaSet object from a Deployment + chosen
    /// template hash. The RS's `spec.template` is the
    /// Deployment's; the RS gets the deployment's labels +
    /// a `pod-template-hash` label for kubectl-rollout-friendly
    /// debugging.
    fn build_replicaset_from(d: &Value, hash: &str) -> Option<(String, Value)> {
        let d_name = d.name()?;
        // The child ReplicaSet lives in the SAME namespace as its parent
        // Deployment — never the controller's scope namespace (an
        // all-namespace controller has none). A namespaced Deployment with
        // no metadata.namespace is impossible past admission, but default
        // defensively so the key + the object can never disagree.
        let d_namespace = d
            .namespace()
            .map_or_else(|| "default".to_string(), |c| c.to_owned());
        let template = d.get("spec").and_then(|s| s.get("template"))?.clone();
        let replicas = d.spec_i64("replicas", 1);
        let rs_name = format!("{d_name}-{hash}");
        let value = json!({
            "kind": "ReplicaSet",
            "apiVersion": "apps/v1",
            "metadata": {
                "name": rs_name,
                "namespace": d_namespace,
                "labels": {
                    "app.kubernetes.io/managed-by": "engenho-deployment-controller",
                    "pod-template-hash": hash
                }
            },
            "spec": {
                "replicas": replicas,
                "selector": d.get("spec").and_then(|s| s.get("selector")).cloned(),
                "template": template
            }
        });
        Some((rs_name, value))
    }

    fn rs_template_hash(rs: &Value) -> Option<String> {
        rs.get("metadata")
            .and_then(|m| m.get("labels"))
            .and_then(|l| l.get("pod-template-hash"))
            .and_then(|h| h.as_str())
            .map(String::from)
    }

    /// Read an i64 `status.<field>` off a ReplicaSet (the status the RS
    /// controller wrote), defaulting to 0 — Deployment status aggregates
    /// these across its current-template RS(es).
    fn rs_status_field(rs: &Value, field: &str) -> i64 {
        rs.get("status")
            .and_then(|s| s.get(field))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0)
    }
}

#[async_trait]
impl OwnedChildrenReconciler for DeploymentController {
    fn name(&self) -> &'static str {
        "deployment"
    }

    fn parent_gvk(&self) -> ParentGvk {
        ParentGvk::new("apps", "v1", "Deployment", "apps/v1")
    }

    fn child_kinds(&self) -> &'static [ChildKind] {
        const CHILD_KINDS: &[ChildKind] = &[ChildKind::new("apps", "v1", "ReplicaSet")];
        CHILD_KINDS
    }

    fn store(&self) -> &StoreMesh {
        &self.store
    }

    fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    async fn reconcile_one(
        &self,
        d_value: &Value,
        owned_rs: &[(ResourceKey, Value)],
    ) -> Result<ReconcileDelta, ControllerError> {
        // No template / owner-ref → nothing to do this tick (the parent
        // is freshly minted; the blanket already skipped no-uid parents).
        let Some(desired_hash) = Self::template_hash(d_value) else {
            return Ok(ReconcileDelta::none());
        };
        let Some(owner_ref) = owner_ref_for(d_value, "apps/v1", "Deployment") else {
            return Ok(ReconcileDelta::none());
        };

        let ns = self.namespace.as_deref();
        let desired_replicas = d_value.spec_i64("replicas", 1);
        let mut commands = Vec::new();

        // Scale stale RSes to 0 (revision history retained at replicas=0).
        for (rs_key, rs_value) in owned_rs {
            if Self::rs_template_hash(rs_value).as_deref() == Some(&desired_hash) {
                continue;
            }
            let current_replicas = rs_value
                .get("spec")
                .and_then(|s| s.get("replicas"))
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0);
            if current_replicas != 0 {
                commands.push(ResourceCommand::patch(
                    rs_key.clone(),
                    json!({"spec": {"replicas": 0}}),
                    Reason::Controller,
                ));
            }
        }

        // Ensure the current-template RS exists + has the right replica
        // count.
        let current = owned_rs
            .iter()
            .find(|(_, r)| Self::rs_template_hash(r).as_deref() == Some(&desired_hash));
        match current {
            Some((rs_key, rs_value)) => {
                let current_replicas = rs_value
                    .get("spec")
                    .and_then(|s| s.get("replicas"))
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0);
                if current_replicas != desired_replicas {
                    commands.push(ResourceCommand::patch(
                        rs_key.clone(),
                        json!({"spec": {"replicas": desired_replicas}}),
                        Reason::Controller,
                    ));
                }
            }
            None => {
                if let Some((rs_name, mut rs_value)) =
                    Self::build_replicaset_from(d_value, &desired_hash)
                {
                    set_owner_reference(&mut rs_value, owner_ref.clone());
                    // Key the RS under the PARENT Deployment's namespace —
                    // the same namespace the blanket gathers `owned_rs` from
                    // (by owner-ref). Using the controller's scope namespace
                    // (`ns`, None→"default") keyed the RS where the owned-RS
                    // query never looks → the controller never saw the RS it
                    // created → recreate-every-tick hot loop + status thrash.
                    let rs_ns = d_value
                        .namespace()
                        .map_or_else(|| ns.unwrap_or("default").to_string(), |c| c.to_owned());
                    let rs_key =
                        ResourceKey::namespaced("apps", "v1", "ReplicaSet", &rs_ns, &rs_name);
                    commands.push(ResourceCommand::Put {
                        key: rs_key,
                        value: rs_value,
                        expected: None,
                        reason: Reason::Controller,
                    });
                }
            }
        }

        Ok(ReconcileDelta::from_commands(commands))
    }

    fn compute_status(
        &self,
        d_value: &Value,
        owned_rs_after: &[(ResourceKey, Value)],
    ) -> Option<Value> {
        // Aggregate over CURRENT-template owned RS(es) — read the status
        // the RS controller wrote. `replicas`/`ready`/`available` SUM
        // across every current-template owned RS; `updatedReplicas` ==
        // current-template RS replica count (single template at M0.1, so
        // it equals `replicas`). Source of truth = the live RS status.
        let desired_hash = Self::template_hash(d_value)?;
        let current_rses: Vec<&Value> = owned_rs_after
            .iter()
            .filter(|(_, r)| Self::rs_template_hash(r).as_deref() == Some(&desired_hash))
            .map(|(_, r)| r)
            .collect();
        let sum = |field: &str| -> i64 {
            current_rses
                .iter()
                .map(|r| Self::rs_status_field(r, field))
                .sum()
        };
        let replicas = sum("replicas");
        let ready = sum("readyReplicas");
        let available = sum("availableReplicas");
        let updated = replicas;
        Some(json!({
            "replicas": replicas,
            "readyReplicas": ready,
            "availableReplicas": available,
            "updatedReplicas": updated,
            "observedGeneration": observed_generation(d_value),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_hash_is_deterministic() {
        let d1 = json!({"spec": {"template": {"spec": {"containers": [{"name": "x"}]}}}});
        let d2 = d1.clone();
        assert_eq!(
            DeploymentController::template_hash(&d1),
            DeploymentController::template_hash(&d2)
        );
    }

    #[test]
    fn template_hash_changes_with_template() {
        let d1 = json!({"spec": {"template": {"spec": {"containers": [{"image": "v1"}]}}}});
        let d2 = json!({"spec": {"template": {"spec": {"containers": [{"image": "v2"}]}}}});
        assert_ne!(
            DeploymentController::template_hash(&d1),
            DeploymentController::template_hash(&d2)
        );
    }

    #[test]
    fn template_hash_is_short_hex() {
        let d = json!({"spec": {"template": {"spec": {}}}});
        let h = DeploymentController::template_hash(&d).unwrap();
        assert_eq!(h.len(), 10);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn template_hash_none_for_missing_template() {
        let d = json!({"spec": {"replicas": 1}});
        assert!(DeploymentController::template_hash(&d).is_none());
    }

    #[test]
    fn build_replicaset_carries_replicas_and_selector() {
        let d = json!({
            "metadata": {"name": "podinfo"},
            "spec": {
                "replicas": 5,
                "selector": {"matchLabels": {"app": "podinfo"}},
                "template": {"metadata": {"labels": {"app": "podinfo"}}, "spec": {}}
            }
        });
        let (name, rs) = DeploymentController::build_replicaset_from(&d, "abcdef").unwrap();
        assert_eq!(name, "podinfo-abcdef");
        assert_eq!(rs.get("spec").unwrap().get("replicas").unwrap(), 5);
        let selector = rs.get("spec").unwrap().get("selector").unwrap();
        assert_eq!(
            selector.get("matchLabels").unwrap().get("app").unwrap(),
            "podinfo"
        );
        let labels = rs.get("metadata").unwrap().get("labels").unwrap();
        assert_eq!(labels.get("pod-template-hash").unwrap(), "abcdef");
    }

    #[test]
    fn rs_template_hash_reads_label() {
        let rs = json!({"metadata": {"labels": {"pod-template-hash": "deadbeef01"}}});
        assert_eq!(
            DeploymentController::rs_template_hash(&rs),
            Some("deadbeef01".into())
        );
    }

    #[test]
    fn rs_template_hash_none_when_label_missing() {
        let rs = json!({"metadata": {"labels": {}}});
        assert!(DeploymentController::rs_template_hash(&rs).is_none());
    }

    #[test]
    fn build_replicaset_from_inherits_the_deployments_namespace() {
        // The child ReplicaSet MUST land in the parent Deployment's
        // namespace — not "default". Regression test for the bug where a
        // Deployment in ns `team-a` produced an RS in `default`, which the
        // owned-RS query (in `team-a`) never saw → recreate-every-tick loop.
        let d = json!({
            "kind": "Deployment", "apiVersion": "apps/v1",
            "metadata": {"name": "web", "namespace": "team-a", "uid": "u1"},
            "spec": {
                "replicas": 3,
                "selector": {"matchLabels": {"app": "web"}},
                "template": {
                    "metadata": {"labels": {"app": "web"}},
                    "spec": {"containers": [{"name": "c", "image": "img"}]}
                }
            }
        });
        let (rs_name, rs) = DeploymentController::build_replicaset_from(&d, "hash01").unwrap();
        assert_eq!(rs_name, "web-hash01");
        assert_eq!(
            rs.get("metadata").unwrap().get("namespace").unwrap(),
            "team-a"
        );
        assert_eq!(rs.get("spec").unwrap().get("replicas").unwrap(), 3);
    }

    #[test]
    fn build_replicaset_from_defaults_namespace_when_parent_has_none() {
        let d = json!({
            "metadata": {"name": "web"},
            "spec": {"replicas": 1, "selector": {}, "template": {"spec": {"containers": []}}}
        });
        let (_, rs) = DeploymentController::build_replicaset_from(&d, "h").unwrap();
        assert_eq!(
            rs.get("metadata").unwrap().get("namespace").unwrap(),
            "default"
        );
    }
}
