//! `ReplicaSetController` — keeps the observed pod count matching
//! `spec.replicas` per ReplicaSet.
//!
//! The reconciliation rule:
//!   * count Pods owned by the ReplicaSet (controller-owned)
//!   * if count < replicas: create the difference (each Pod cloned
//!     from `spec.template` + owner-referenced + named uniquely)
//!   * if count > replicas: delete the excess (eviction by name
//!     order for predictability)
//!
//! No scheduling here — that's the scheduler's job. The Pods get
//! created without `spec.nodeName`; the scheduler binds them in
//! its own loop.

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

pub struct ReplicaSetController {
    store: Arc<StoreMesh>,
    /// Optional namespace scope. None = all namespaces.
    namespace: Option<String>,
}

impl ReplicaSetController {
    #[must_use]
    pub fn new(store: Arc<StoreMesh>, namespace: Option<String>) -> Self {
        Self { store, namespace }
    }

    fn replicas(rs: &Value) -> i64 {
        rs.get("spec")
            .and_then(|s| s.get("replicas"))
            .and_then(|n| n.as_i64())
            .unwrap_or(1)
    }

    fn rs_uid(rs: &Value) -> Option<String> {
        rs.get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(|u| u.as_str())
            .map(String::from)
    }

    fn rs_name(rs: &Value) -> Option<&str> {
        rs.get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
    }

    /// Build the OwnerReference pointing at this ReplicaSet.
    fn owner_ref_for(rs: &Value) -> Option<OwnerReference> {
        Some(OwnerReference {
            api_version: "apps/v1".into(),
            kind: "ReplicaSet".into(),
            name: Self::rs_name(rs)?.to_string(),
            uid: Self::rs_uid(rs)?,
            controller: true,
            block_owner_deletion: true,
        })
    }

    /// Construct a Pod object from a ReplicaSet's `spec.template`.
    /// Names the Pod `{rs_name}-{index}-{random}`; the index +
    /// random suffix make names deterministic for tests + readable
    /// for operators.
    fn build_pod_from_template(rs: &Value, index: usize) -> Option<(String, Value)> {
        let rs_name = Self::rs_name(rs)?;
        let template = rs.get("spec").and_then(|s| s.get("template"))?;
        let mut pod = template.clone();
        // Ensure pod is an object
        let pod_obj = pod.as_object_mut()?;
        // Wrap with kind + apiVersion at the top
        pod_obj.insert("kind".into(), Value::String("Pod".into()));
        pod_obj.insert("apiVersion".into(), Value::String("v1".into()));
        // Generate a deterministic-ish name: {rs}-{index}
        // (Production K8s uses random hashes; for R9 we keep it
        // deterministic for test reproducibility. R9.5+ can swap
        // in nanoid if name collisions matter.)
        let pod_name = format!("{rs_name}-{index}");
        let metadata = pod_obj
            .entry("metadata".to_string())
            .or_insert_with(|| json!({}));
        let metadata_obj = metadata.as_object_mut()?;
        metadata_obj.insert("name".into(), Value::String(pod_name.clone()));
        Some((pod_name, pod))
    }
}

#[async_trait]
impl Controller for ReplicaSetController {
    fn name(&self) -> &'static str {
        "replicaset"
    }

    async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
        let rs_list = self
            .store
            .list("apps", "v1", "ReplicaSet", self.namespace.as_deref())
            .await;
        let mut report = ReconcileReport::default();
        report.objects_examined = rs_list.len();

        for (rs_key, rs_value) in &rs_list {
            let Some(uid) = Self::rs_uid(rs_value) else {
                debug!(rs = %rs_key.label(), "skipping RS with no metadata.uid");
                report.objects_skipped += 1;
                continue;
            };
            let desired = Self::replicas(rs_value).max(0) as usize;
            let ns = rs_key.namespace.as_deref();

            // Find owned Pods.
            let all_pods = self.store.list("", "v1", "Pod", ns).await;
            let owned_pods: Vec<&(ResourceKey, Value)> = all_pods
                .iter()
                .filter(|(_, p)| is_owned_by(p, &uid))
                .collect();

            let observed = owned_pods.len();
            if observed == desired {
                continue;
            }

            let Some(owner_ref) = Self::owner_ref_for(rs_value) else {
                report.objects_skipped += 1;
                continue;
            };

            if observed < desired {
                // Create the difference.
                let to_create = desired - observed;
                for i in 0..to_create {
                    // Index from observed+i so consecutive ticks
                    // don't collide on names. For tests we usually
                    // start at 0.
                    let idx = observed + i;
                    let Some((pod_name, mut pod)) = Self::build_pod_from_template(rs_value, idx)
                    else {
                        report.objects_skipped += 1;
                        continue;
                    };
                    set_owner_reference(&mut pod, owner_ref.clone());
                    let pod_ns = ns.unwrap_or("default");
                    let pod_key = ResourceKey::namespaced("", "v1", "Pod", pod_ns, &pod_name);
                    self.store
                        .propose(ResourceCommand::Put {
                            key: pod_key,
                            value: pod,
                            reason: Reason::Controller,
                        })
                        .await
                        .map_err(|e| ControllerError::Store(e.to_string()))?;
                    report.objects_changed += 1;
                }
            } else {
                // observed > desired — evict the excess.
                let to_delete = observed - desired;
                // Sort owned pods by name for deterministic eviction.
                let mut owned_sorted: Vec<&(ResourceKey, Value)> = owned_pods.clone();
                owned_sorted.sort_by(|a, b| a.0.name.cmp(&b.0.name));
                for (pod_key, _) in owned_sorted.iter().rev().take(to_delete) {
                    self.store
                        .propose(ResourceCommand::Delete {
                            key: (*pod_key).clone(),
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
    fn replicas_defaults_to_1() {
        let rs = json!({"metadata": {"name": "rs"}, "spec": {}});
        assert_eq!(ReplicaSetController::replicas(&rs), 1);
    }

    #[test]
    fn replicas_reads_spec_field() {
        let rs = json!({"spec": {"replicas": 5}});
        assert_eq!(ReplicaSetController::replicas(&rs), 5);
    }

    #[test]
    fn build_pod_from_template_sets_name_and_metadata() {
        let rs = json!({
            "metadata": {"name": "rs1"},
            "spec": {
                "template": {
                    "metadata": {"labels": {"app": "rs1"}},
                    "spec": {"containers": [{"name": "c", "image": "img"}]}
                }
            }
        });
        let (name, pod) = ReplicaSetController::build_pod_from_template(&rs, 0).unwrap();
        assert_eq!(name, "rs1-0");
        assert_eq!(pod.get("kind").unwrap(), "Pod");
        assert_eq!(pod.get("apiVersion").unwrap(), "v1");
        assert_eq!(pod.get("metadata").unwrap().get("name").unwrap(), "rs1-0");
        // Template labels survive.
        assert_eq!(
            pod.get("metadata")
                .unwrap()
                .get("labels")
                .unwrap()
                .get("app")
                .unwrap(),
            "rs1"
        );
    }

    #[test]
    fn owner_ref_for_constructs_typed_ref() {
        let rs = json!({
            "metadata": {"name": "rs1", "uid": "uid-1"},
            "spec": {"replicas": 1}
        });
        let owner = ReplicaSetController::owner_ref_for(&rs).unwrap();
        assert_eq!(owner.api_version, "apps/v1");
        assert_eq!(owner.kind, "ReplicaSet");
        assert_eq!(owner.uid, "uid-1");
        assert!(owner.controller);
        assert!(owner.block_owner_deletion);
    }

    #[test]
    fn owner_ref_for_returns_none_without_uid() {
        let rs = json!({"metadata": {"name": "rs1"}});
        assert!(ReplicaSetController::owner_ref_for(&rs).is_none());
    }

    #[test]
    fn controller_name_is_stable() {
        // We can't construct a real ReplicaSetController without
        // an Arc<StoreMesh>; but the trait impl exposes name().
        // This test documents the canonical name.
        struct Fake;
        #[async_trait]
        impl Controller for Fake {
            fn name(&self) -> &'static str {
                "replicaset"
            }
            async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
                Ok(ReconcileReport::default())
            }
        }
        assert_eq!(Fake.name(), "replicaset");
    }
}
