//! R15 — StatefulSet controller.
//!
//! Like ReplicaSetController but with **ordered, persistent
//! identity**: pods get names `{sts}-0`, `{sts}-1`, ...
//! Replacement keeps the same name + PVC mapping.
//!
//! ## Reconcile rule
//!
//! For each StatefulSet:
//!   1. desired_replicas = spec.replicas (default 1)
//!   2. owned pods = pods with controller-ref UID matching
//!   3. for i in 0..desired_replicas:
//!         expected_name = "{sts}-{i}"
//!         if no pod with that name exists → create from template
//!   4. for each owned pod with ordinal >= desired_replicas:
//!         delete (scale-down evicts highest-ordinal first)
//!
//! Identity is preserved across restart: pod `web-2` always has
//! the same volume claims + (with R13 wiring) the same persistent
//! volume.

use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand},
    resource::ResourceKey,
};
use serde_json::{Value, json};
use tracing::debug;

use crate::error::ControllerError;
use crate::meta::ObjectMeta;
use crate::owned_children::{ChildKind, OwnedChildrenReconciler, ParentGvk, ReconcileDelta};
use crate::owner::{owner_ref_for, set_owner_reference};
use crate::status::{observed_generation, pod_is_ready};

/// StatefulSet controller — peer to ReplicaSetController with
/// ordered identity semantics.
pub struct StatefulSetController {
    store: Arc<StoreMesh>,
    namespace: Option<String>,
}

impl StatefulSetController {
    /// Construct with optional namespace scope.
    #[must_use]
    pub fn new(store: Arc<StoreMesh>, namespace: Option<String>) -> Self {
        Self { store, namespace }
    }

    /// Build a Pod from the StatefulSet template at ordinal `i`.
    /// The name is `{sts_name}-{i}` — ordered + persistent.
    fn build_pod(sts: &Value, ordinal: usize) -> Option<(String, Value)> {
        let sts_name = sts.name()?;
        let template = sts.get("spec").and_then(|s| s.get("template"))?;
        let mut pod = template.clone();
        let pod_obj = pod.as_object_mut()?;
        pod_obj.insert("kind".into(), Value::String("Pod".into()));
        pod_obj.insert("apiVersion".into(), Value::String("v1".into()));
        let pod_name = format!("{sts_name}-{ordinal}");
        let metadata = pod_obj
            .entry("metadata".to_string())
            .or_insert_with(|| json!({}));
        let m = metadata.as_object_mut()?;
        m.insert("name".into(), Value::String(pod_name.clone()));
        Some((pod_name, pod))
    }

    /// Extract the ordinal from "{sts}-{n}". Returns None for
    /// names that don't match.
    fn ordinal_of(pod_name: &str, sts_name: &str) -> Option<usize> {
        let prefix = format!("{sts_name}-");
        pod_name
            .strip_prefix(&prefix)
            .and_then(|s| s.parse::<usize>().ok())
    }
}

#[async_trait]
impl OwnedChildrenReconciler for StatefulSetController {
    fn name(&self) -> &'static str {
        "statefulset"
    }

    fn parent_gvk(&self) -> ParentGvk {
        ParentGvk::new("apps", "v1", "StatefulSet", "apps/v1")
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
        sts_value: &Value,
        owned: &[(ResourceKey, Value)],
    ) -> Result<ReconcileDelta, ControllerError> {
        // No name / owner-ref → no-op (parent freshly minted; blanket
        // already skipped no-uid parents).
        let Some(sts_name) = sts_value.name() else {
            return Ok(ReconcileDelta::none());
        };
        let Some(owner_ref) = owner_ref_for(sts_value, "apps/v1", "StatefulSet") else {
            return Ok(ReconcileDelta::none());
        };

        let desired = sts_value.spec_i64("replicas", 1).max(0) as usize;
        let ns = self.namespace.as_deref();
        let pod_ns = ns.unwrap_or("default");

        let existing_ordinals: std::collections::BTreeSet<usize> = owned
            .iter()
            .filter_map(|(k, _)| Self::ordinal_of(&k.name, sts_name))
            .collect();

        let mut commands = Vec::new();

        // Create missing ordinals 0..desired.
        for ordinal in 0..desired {
            if existing_ordinals.contains(&ordinal) {
                continue;
            }
            let Some((pod_name, mut pod)) = Self::build_pod(sts_value, ordinal) else {
                continue;
            };
            set_owner_reference(&mut pod, owner_ref.clone());
            let pod_key = ResourceKey::namespaced("", "v1", "Pod", pod_ns, &pod_name);
            debug!(sts = sts_name, ordinal, "creating ordered pod");
            commands.push(ResourceCommand::Put {
                key: pod_key,
                value: pod,
                expected: None,
                reason: Reason::Controller,
            });
        }

        // Scale-down: remove pods with ordinal >= desired.
        for (pod_key, _) in owned {
            if let Some(ord) = Self::ordinal_of(&pod_key.name, sts_name) {
                if ord >= desired {
                    debug!(sts = sts_name, pod = %pod_key.label(), "scaling down ordered pod");
                    commands.push(ResourceCommand::Delete {
                        key: pod_key.clone(),
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
        sts_value: &Value,
        owned_now: &[(ResourceKey, Value)],
    ) -> Option<Value> {
        // Computed from the LIVE owned pods after the reconcile.
        // `replicas` = owned pod count; `readyReplicas`/`availableReplicas`
        // = ready owned pods; `updatedReplicas`/`currentReplicas` ==
        // replicas (single revision at M0.1).
        let replicas = i64::try_from(owned_now.len()).unwrap_or(i64::MAX);
        let ready =
            i64::try_from(owned_now.iter().filter(|(_, p)| pod_is_ready(p)).count())
                .unwrap_or(i64::MAX);
        Some(json!({
            "replicas": replicas,
            "readyReplicas": ready,
            "availableReplicas": ready,
            "updatedReplicas": replicas,
            "currentReplicas": replicas,
            "observedGeneration": observed_generation(sts_value),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ordinal_of_parses_correct_format() {
        assert_eq!(StatefulSetController::ordinal_of("web-0", "web"), Some(0));
        assert_eq!(StatefulSetController::ordinal_of("web-12", "web"), Some(12));
        assert_eq!(StatefulSetController::ordinal_of("web", "web"), None);
        assert_eq!(StatefulSetController::ordinal_of("other-1", "web"), None);
        assert_eq!(StatefulSetController::ordinal_of("web-abc", "web"), None);
    }

    #[test]
    fn replicas_defaults_to_1() {
        let sts = json!({"metadata": {"name": "x"}, "spec": {}});
        assert_eq!(sts.spec_i64("replicas", 1), 1);
    }

    #[test]
    fn replicas_reads_spec_field() {
        let sts = json!({"spec": {"replicas": 3}});
        assert_eq!(sts.spec_i64("replicas", 1), 3);
    }

    #[test]
    fn build_pod_names_with_ordinal() {
        let sts = json!({
            "metadata": {"name": "web"},
            "spec": {
                "template": {
                    "metadata": {"labels": {"app": "web"}},
                    "spec": {"containers": [{"image": "nginx"}]}
                }
            }
        });
        for ord in [0usize, 1, 7] {
            let (name, pod) = StatefulSetController::build_pod(&sts, ord).unwrap();
            assert_eq!(name, format!("web-{ord}"));
            assert_eq!(pod.get("kind").unwrap(), "Pod");
            assert_eq!(pod.get("metadata").unwrap().get("name").unwrap(), &name);
        }
    }

    #[test]
    fn owner_ref_for_constructs_typed_ref() {
        let sts = json!({"metadata": {"name": "web", "uid": "u-1"}});
        let r = owner_ref_for(&sts, "apps/v1", "StatefulSet").unwrap();
        assert_eq!(r.kind, "StatefulSet");
        assert_eq!(r.api_version, "apps/v1");
        assert_eq!(r.uid, "u-1");
        assert!(r.controller);
    }

    #[test]
    fn owner_ref_none_without_uid() {
        let sts = json!({"metadata": {"name": "web"}});
        assert!(owner_ref_for(&sts, "apps/v1", "StatefulSet").is_none());
    }

}
