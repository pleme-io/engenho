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
use tracing::debug;

use crate::controller::{Controller, ReconcileReport};
use crate::error::ControllerError;
use crate::owner::{OwnerReference, is_owned_by, set_owner_reference};

pub struct DeploymentController {
    store: Arc<StoreMesh>,
    namespace: Option<String>,
}

impl DeploymentController {
    #[must_use]
    pub fn new(store: Arc<StoreMesh>, namespace: Option<String>) -> Self {
        Self { store, namespace }
    }

    fn deployment_uid(d: &Value) -> Option<String> {
        d.get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(|u| u.as_str())
            .map(String::from)
    }

    fn deployment_name(d: &Value) -> Option<&str> {
        d.get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
    }

    fn desired_replicas(d: &Value) -> i64 {
        d.get("spec")
            .and_then(|s| s.get("replicas"))
            .and_then(|n| n.as_i64())
            .unwrap_or(1)
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

    fn owner_ref_for(d: &Value) -> Option<OwnerReference> {
        Some(OwnerReference {
            api_version: "apps/v1".into(),
            kind: "Deployment".into(),
            name: Self::deployment_name(d)?.to_string(),
            uid: Self::deployment_uid(d)?,
            controller: true,
            block_owner_deletion: true,
        })
    }

    /// Build a ReplicaSet object from a Deployment + chosen
    /// template hash. The RS's `spec.template` is the
    /// Deployment's; the RS gets the deployment's labels +
    /// a `pod-template-hash` label for kubectl-rollout-friendly
    /// debugging.
    fn build_replicaset_from(d: &Value, hash: &str) -> Option<(String, Value)> {
        let d_name = Self::deployment_name(d)?;
        let template = d.get("spec").and_then(|s| s.get("template"))?.clone();
        let replicas = Self::desired_replicas(d);
        let rs_name = format!("{d_name}-{hash}");
        let value = json!({
            "kind": "ReplicaSet",
            "apiVersion": "apps/v1",
            "metadata": {
                "name": rs_name,
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
}

#[async_trait]
impl Controller for DeploymentController {
    fn name(&self) -> &'static str {
        "deployment"
    }

    async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
        let deployments = self
            .store
            .list("apps", "v1", "Deployment", self.namespace.as_deref())
            .await;
        let mut report = ReconcileReport::default();
        report.objects_examined = deployments.len();

        for (d_key, d_value) in &deployments {
            let Some(uid) = Self::deployment_uid(d_value) else {
                report.objects_skipped += 1;
                continue;
            };
            let Some(desired_hash) = Self::template_hash(d_value) else {
                report.objects_skipped += 1;
                continue;
            };
            let Some(owner_ref) = Self::owner_ref_for(d_value) else {
                report.objects_skipped += 1;
                continue;
            };
            let ns = d_key.namespace.as_deref();
            let all_rs = self.store.list("apps", "v1", "ReplicaSet", ns).await;
            let owned_rs: Vec<&(ResourceKey, Value)> = all_rs
                .iter()
                .filter(|(_, r)| is_owned_by(r, &uid))
                .collect();

            // Look for a current-template RS.
            let current = owned_rs
                .iter()
                .find(|(_, r)| Self::rs_template_hash(r).as_deref() == Some(&desired_hash))
                .cloned();

            let desired_replicas = Self::desired_replicas(d_value);

            // Scale stale RSes to 0.
            for (rs_key, rs_value) in &owned_rs {
                let hash_matches =
                    Self::rs_template_hash(rs_value).as_deref() == Some(&desired_hash);
                if hash_matches {
                    continue;
                }
                let current_replicas = rs_value
                    .get("spec")
                    .and_then(|s| s.get("replicas"))
                    .and_then(|n| n.as_i64())
                    .unwrap_or(0);
                if current_replicas != 0 {
                    debug!(rs = %rs_key.label(), "scaling stale RS to 0");
                    self.store
                        .propose(ResourceCommand::Patch {
                            key: (*rs_key).clone(),
                            patch: json!({"spec": {"replicas": 0}}),
                            reason: Reason::Controller,
                        })
                        .await
                        .map_err(|e| ControllerError::Store(e.to_string()))?;
                    report.objects_changed += 1;
                }
            }

            // Ensure the current-template RS exists + has correct replica count.
            match current {
                Some((rs_key, rs_value)) => {
                    let current_replicas = rs_value
                        .get("spec")
                        .and_then(|s| s.get("replicas"))
                        .and_then(|n| n.as_i64())
                        .unwrap_or(0);
                    if current_replicas != desired_replicas {
                        self.store
                            .propose(ResourceCommand::Patch {
                                key: rs_key.clone(),
                                patch: json!({"spec": {"replicas": desired_replicas}}),
                                reason: Reason::Controller,
                            })
                            .await
                            .map_err(|e| ControllerError::Store(e.to_string()))?;
                        report.objects_changed += 1;
                    }
                }
                None => {
                    // Create the current-template RS.
                    let Some((rs_name, mut rs_value)) =
                        Self::build_replicaset_from(d_value, &desired_hash)
                    else {
                        report.objects_skipped += 1;
                        continue;
                    };
                    set_owner_reference(&mut rs_value, owner_ref.clone());
                    let rs_ns = ns.unwrap_or("default");
                    let rs_key =
                        ResourceKey::namespaced("apps", "v1", "ReplicaSet", rs_ns, &rs_name);
                    self.store
                        .propose(ResourceCommand::Put {
                            key: rs_key,
                            value: rs_value,
                            reason: Reason::Controller,
                        })
                        .await
                        .map_err(|e| ControllerError::Store(e.to_string()))?;
                    report.objects_changed += 1;
                }
            }
        }
        Ok(report)
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
}
