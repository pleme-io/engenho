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

use crate::error::ControllerError;
use crate::meta::ObjectMeta;
use crate::owned_children::{ChildKind, OwnedChildrenReconciler, ParentGvk, ReconcileDelta};
use crate::owner::{owner_ref_for, set_owner_reference};
use crate::status::{observed_generation, pod_is_ready};

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

    /// Construct a Pod object from a ReplicaSet's `spec.template`.
    /// Names the Pod `{rs_name}-{index}-{random}`; the index +
    /// random suffix make names deterministic for tests + readable
    /// for operators.
    fn build_pod_from_template(rs: &Value, index: usize) -> Option<(String, Value)> {
        let rs_name = rs.name()?;
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
impl OwnedChildrenReconciler for ReplicaSetController {
    fn name(&self) -> &'static str {
        "replicaset"
    }

    fn parent_gvk(&self) -> ParentGvk {
        ParentGvk::new("apps", "v1", "ReplicaSet", "apps/v1")
    }

    fn child_kinds(&self) -> &'static [ChildKind] {
        const CHILD_KINDS: &[ChildKind] = &[ChildKind::new("", "v1", "Pod")];
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
        rs_value: &Value,
        owned_pods: &[(ResourceKey, Value)],
    ) -> Result<ReconcileDelta, ControllerError> {
        let desired = rs_value.spec_i64("replicas", 1).max(0) as usize;
        let observed = owned_pods.len();

        // Fixpoint — nothing to create or evict.
        if observed == desired {
            return Ok(ReconcileDelta::none());
        }

        // Lazily build the owner ref; skip (empty delta) when the parent
        // has no uid/name yet (a freshly minted RS). The blanket already
        // skipped no-uid parents, but name may still be missing.
        let Some(owner_ref) = owner_ref_for(rs_value, "apps/v1", "ReplicaSet") else {
            return Ok(ReconcileDelta::none());
        };
        let ns = self.namespace.as_deref();
        let pod_ns = ns.unwrap_or("default");
        let mut commands = Vec::new();

        if observed < desired {
            // Create the difference. Index from observed+i so consecutive
            // ticks don't collide on names.
            let to_create = desired - observed;
            for i in 0..to_create {
                let idx = observed + i;
                let Some((pod_name, mut pod)) = Self::build_pod_from_template(rs_value, idx) else {
                    continue;
                };
                set_owner_reference(&mut pod, owner_ref.clone());
                let pod_key = ResourceKey::namespaced("", "v1", "Pod", pod_ns, &pod_name);
                commands.push(ResourceCommand::Put {
                    key: pod_key,
                    value: pod,
                    expected: None,
                    reason: Reason::Controller,
                });
            }
        } else {
            // observed > desired — evict the excess by name order (highest
            // names first) for deterministic eviction.
            let to_delete = observed - desired;
            let mut owned_sorted: Vec<&(ResourceKey, Value)> = owned_pods.iter().collect();
            owned_sorted.sort_by(|a, b| a.0.name.cmp(&b.0.name));
            for (pod_key, _) in owned_sorted.iter().rev().take(to_delete) {
                commands.push(ResourceCommand::Delete {
                    key: (*pod_key).clone(),
                    expected: None,
                    reason: Reason::Controller,
                });
            }
        }

        Ok(ReconcileDelta::from_commands(commands))
    }

    fn compute_status(
        &self,
        rs_value: &Value,
        owned_now: &[(ResourceKey, Value)],
    ) -> Option<Value> {
        // Status computed from the LIVE owned pods AFTER the reconcile
        // delta. readyReplicas counts Ready=True pods; availableReplicas
        // == readyReplicas at M0.1 (no minReadySeconds);
        // fullyLabeledReplicas == replicas.
        let replicas = i64::try_from(owned_now.len()).unwrap_or(i64::MAX);
        let ready =
            i64::try_from(owned_now.iter().filter(|(_, p)| pod_is_ready(p)).count())
                .unwrap_or(i64::MAX);
        Some(json!({
            "replicas": replicas,
            "readyReplicas": ready,
            "availableReplicas": ready,
            "fullyLabeledReplicas": replicas,
            "observedGeneration": observed_generation(rs_value),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replicas_defaults_to_1() {
        let rs = json!({"metadata": {"name": "rs"}, "spec": {}});
        assert_eq!(rs.spec_i64("replicas", 1), 1);
    }

    #[test]
    fn replicas_reads_spec_field() {
        let rs = json!({"spec": {"replicas": 5}});
        assert_eq!(rs.spec_i64("replicas", 1), 5);
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
        let owner = owner_ref_for(&rs, "apps/v1", "ReplicaSet").unwrap();
        assert_eq!(owner.api_version, "apps/v1");
        assert_eq!(owner.kind, "ReplicaSet");
        assert_eq!(owner.uid, "uid-1");
        assert!(owner.controller);
        assert!(owner.block_owner_deletion);
    }

    #[test]
    fn owner_ref_for_returns_none_without_uid() {
        let rs = json!({"metadata": {"name": "rs1"}});
        assert!(owner_ref_for(&rs, "apps/v1", "ReplicaSet").is_none());
    }

}
