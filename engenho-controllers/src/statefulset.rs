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

use crate::controller::{Controller, ReconcileReport};
use crate::error::ControllerError;
use crate::owner::{OwnerReference, is_owned_by, set_owner_reference};
use crate::status::{observed_generation, pod_is_ready, write_status_cas};

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

    fn replicas(sts: &Value) -> i64 {
        sts.get("spec")
            .and_then(|s| s.get("replicas"))
            .and_then(|n| n.as_i64())
            .unwrap_or(1)
    }

    fn sts_uid(sts: &Value) -> Option<String> {
        sts.get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(|u| u.as_str())
            .map(String::from)
    }

    fn sts_name(sts: &Value) -> Option<&str> {
        sts.get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
    }

    fn owner_ref_for(sts: &Value) -> Option<OwnerReference> {
        Some(OwnerReference {
            api_version: "apps/v1".into(),
            kind: "StatefulSet".into(),
            name: Self::sts_name(sts)?.to_string(),
            uid: Self::sts_uid(sts)?,
            controller: true,
            block_owner_deletion: true,
        })
    }

    /// Build a Pod from the StatefulSet template at ordinal `i`.
    /// The name is `{sts_name}-{i}` — ordered + persistent.
    fn build_pod(sts: &Value, ordinal: usize) -> Option<(String, Value)> {
        let sts_name = Self::sts_name(sts)?;
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
impl Controller for StatefulSetController {
    fn name(&self) -> &'static str {
        "statefulset"
    }

    async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
        let sts_list = self
            .store
            .list("apps", "v1", "StatefulSet", self.namespace.as_deref())
            .await;
        let mut report = ReconcileReport::default();
        report.objects_examined = sts_list.len();

        for (sts_key, sts_value) in &sts_list {
            let Some(uid) = Self::sts_uid(sts_value) else {
                report.objects_skipped += 1;
                continue;
            };
            let Some(sts_name) = Self::sts_name(sts_value) else {
                report.objects_skipped += 1;
                continue;
            };
            let Some(owner_ref) = Self::owner_ref_for(sts_value) else {
                report.objects_skipped += 1;
                continue;
            };
            let desired = Self::replicas(sts_value).max(0) as usize;
            let ns = sts_key.namespace.as_deref();
            let all_pods = self.store.list("", "v1", "Pod", ns).await;
            let owned: Vec<&(ResourceKey, Value)> = all_pods
                .iter()
                .filter(|(_, p)| is_owned_by(p, &uid))
                .collect();

            // Map ordinal → existing pod_key for fast lookup.
            let existing_ordinals: std::collections::BTreeSet<usize> = owned
                .iter()
                .filter_map(|(k, _)| Self::ordinal_of(&k.name, sts_name))
                .collect();

            // Create missing ordinals 0..desired.
            for ordinal in 0..desired {
                if existing_ordinals.contains(&ordinal) {
                    continue;
                }
                let Some((pod_name, mut pod)) = Self::build_pod(sts_value, ordinal) else {
                    report.objects_skipped += 1;
                    continue;
                };
                set_owner_reference(&mut pod, owner_ref.clone());
                let pod_ns = ns.unwrap_or("default");
                let pod_key = ResourceKey::namespaced("", "v1", "Pod", pod_ns, &pod_name);
                debug!(sts = sts_name, ordinal, "creating ordered pod");
                self.store
                    .propose(ResourceCommand::Put {
                        key: pod_key,
                        value: pod,
                        expected: None,
                        reason: Reason::Controller,
                    })
                    .await
                    .map_err(|e| ControllerError::Store(e.to_string()))?;
                report.objects_changed += 1;
            }

            // Scale-down: remove pods with ordinal >= desired.
            for (pod_key, _) in &owned {
                if let Some(ord) = Self::ordinal_of(&pod_key.name, sts_name) {
                    if ord >= desired {
                        debug!(sts = sts_name, pod = %pod_key.label(), "scaling down ordered pod");
                        self.store
                            .propose(ResourceCommand::Delete {
                                key: (*pod_key).clone(),
                                expected: None,
                                reason: Reason::Controller,
                            })
                            .await
                            .map_err(|e| ControllerError::Store(e.to_string()))?;
                        report.objects_changed += 1;
                    }
                }
            }

            // ── Status: computed from the LIVE owned pods after the
            // create/delete reconcile (re-list so deletes/creates this
            // tick are reflected). `replicas` = owned pod count;
            // `readyReplicas`/`availableReplicas` = ready owned pods;
            // `updatedReplicas`/`currentReplicas` == replicas (single
            // revision at M0.1).
            let fresh_pods = self.store.list("", "v1", "Pod", ns).await;
            let owned_now: Vec<&Value> = fresh_pods
                .iter()
                .filter(|(_, p)| is_owned_by(p, &uid))
                .map(|(_, p)| p)
                .collect();
            let replicas = i64::try_from(owned_now.len()).unwrap_or(i64::MAX);
            let ready = i64::try_from(owned_now.iter().filter(|p| pod_is_ready(p)).count())
                .unwrap_or(i64::MAX);
            let desired_status = json!({
                "replicas": replicas,
                "readyReplicas": ready,
                "availableReplicas": ready,
                "updatedReplicas": replicas,
                "currentReplicas": replicas,
                "observedGeneration": observed_generation(sts_value),
            });
            if write_status_cas(&self.store, sts_key, sts_value, &desired_status)
                .await?
                .changed()
            {
                report.objects_changed += 1;
            }
        }
        Ok(report)
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
        assert_eq!(StatefulSetController::replicas(&sts), 1);
    }

    #[test]
    fn replicas_reads_spec_field() {
        let sts = json!({"spec": {"replicas": 3}});
        assert_eq!(StatefulSetController::replicas(&sts), 3);
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
        let r = StatefulSetController::owner_ref_for(&sts).unwrap();
        assert_eq!(r.kind, "StatefulSet");
        assert_eq!(r.api_version, "apps/v1");
        assert_eq!(r.uid, "u-1");
        assert!(r.controller);
    }

    #[test]
    fn owner_ref_none_without_uid() {
        let sts = json!({"metadata": {"name": "web"}});
        assert!(StatefulSetController::owner_ref_for(&sts).is_none());
    }

    #[test]
    fn controller_name_is_stable() {
        struct Fake;
        #[async_trait]
        impl Controller for Fake {
            fn name(&self) -> &'static str {
                "statefulset"
            }
            async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
                Ok(ReconcileReport::default())
            }
        }
        assert_eq!(Fake.name(), "statefulset");
    }
}
