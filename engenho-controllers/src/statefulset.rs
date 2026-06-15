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
    ///
    /// The Pod inherits the parent StatefulSet's namespace — the
    /// namespace the blanket gathers owned pods from. Never the
    /// controller's scope namespace (an all-namespace controller has
    /// none). Mirrors `ReplicaSetController::build_pod_from_template`;
    /// keying a pod under the controller scope ns instead of the
    /// parent's broke owned-pod gathering for namespaced parents (the
    /// same just-fixed bug as deployment→RS and replicaset→pod).
    ///
    /// `volumeClaimTemplates` (PVC provisioning) is scoped OUT at M0 —
    /// the PVC/storage subsystem is a separate engenho gap. BUT when the
    /// StatefulSet declares `spec.volumeClaimTemplates`, the pod's
    /// matching `spec.volumes[].persistentVolumeClaim.claimName` is set
    /// to the deterministic per-pod PVC name `{template}-{sts}-{ordinal}`
    /// (the K8s naming contract) so the wiring is CORRECT the moment PVCs
    /// land — no rename needed.
    fn build_pod(sts: &Value, ordinal: usize) -> Option<(String, Value)> {
        let sts_name = sts.name()?;
        let sts_namespace =
            sts.namespace().map_or_else(|| "default".to_string(), |c| c.to_owned());
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
        m.insert("namespace".into(), Value::String(sts_namespace));

        // Per-pod PVC volume wiring from volumeClaimTemplates (deterministic
        // name; provisioning itself is a separate gap — see doc above).
        Self::wire_pvc_volumes(sts, pod_obj, sts_name, ordinal);

        Some((pod_name, pod))
    }

    /// For each `spec.volumeClaimTemplates[i].metadata.name`, append a
    /// `spec.volumes` entry on the pod referencing the deterministic
    /// per-pod claim `{template}-{sts}-{ordinal}` (the K8s naming
    /// contract). PVC objects themselves are NOT created here (storage is
    /// a separate engenho gap) — only the pod-side reference, so the pod
    /// is correct once PVCs land. Idempotent: skips a volume name already
    /// present on the pod template.
    fn wire_pvc_volumes(
        sts: &Value,
        pod_obj: &mut serde_json::Map<String, Value>,
        sts_name: &str,
        ordinal: usize,
    ) {
        let Some(vcts) = sts
            .get("spec")
            .and_then(|s| s.get("volumeClaimTemplates"))
            .and_then(|v| v.as_array())
        else {
            return;
        };
        if vcts.is_empty() {
            return;
        }
        let spec = pod_obj
            .entry("spec".to_string())
            .or_insert_with(|| json!({}));
        let Some(spec_obj) = spec.as_object_mut() else {
            return;
        };
        let volumes = spec_obj
            .entry("volumes".to_string())
            .or_insert_with(|| Value::Array(Vec::new()));
        let Some(arr) = volumes.as_array_mut() else {
            return;
        };
        for vct in vcts {
            let Some(tmpl_name) = vct
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
            else {
                continue;
            };
            // Idempotence: a template that already declares this volume name
            // (operator pre-wired it) is left untouched.
            let already = arr.iter().any(|v| {
                v.get("name").and_then(|n| n.as_str()) == Some(tmpl_name)
            });
            if already {
                continue;
            }
            let claim_name = Self::pvc_claim_name(tmpl_name, sts_name, ordinal);
            arr.push(json!({
                "name": tmpl_name,
                "persistentVolumeClaim": { "claimName": claim_name }
            }));
        }
    }

    /// The deterministic per-pod PVC name for a volumeClaimTemplate:
    /// `{template}-{sts}-{ordinal}` (the K8s StatefulSet PVC naming
    /// contract). Pure + total — the name is correct the moment PVC
    /// provisioning lands.
    #[must_use]
    fn pvc_claim_name(template_name: &str, sts_name: &str, ordinal: usize) -> String {
        format!("{template_name}-{sts_name}-{ordinal}")
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
        // Pods go in the StatefulSet's OWN namespace (where owned-pod
        // gathering looks), not the controller scope ns — same fix as
        // deployment→RS and replicaset→pod. Keying under the scope ns
        // (None→"default") put pods where the owned-pod query never looked
        // → recreate-every-tick hot loop for namespaced StatefulSets.
        let pod_ns = sts_value.namespace().map_or_else(
            || self.namespace.as_deref().unwrap_or("default").to_string(),
            |c| c.to_owned(),
        );

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
            let pod_key = ResourceKey::namespaced("", "v1", "Pod", &pod_ns, &pod_name);
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
                    commands.push(ResourceCommand::delete(pod_key.clone(), Reason::Controller));
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
    use crate::controller::Controller; // brings `.tick()` (blanket via OwnedChildrenReconciler) into scope
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

    #[test]
    fn build_pod_inherits_the_statefulsets_namespace() {
        // The Pod MUST land in the parent STS's namespace — not the
        // controller scope ns. Regression test for the bug where a STS in
        // ns `db` produced pods in `default`, breaking owned-pod gathering.
        let sts = json!({
            "metadata": {"name": "vm", "namespace": "monitoring"},
            "spec": {"template": {
                "metadata": {"labels": {"app": "vm"}},
                "spec": {"containers": [{"name": "c", "image": "img"}]}
            }}
        });
        let (name, pod) = StatefulSetController::build_pod(&sts, 0).unwrap();
        assert_eq!(name, "vm-0");
        assert_eq!(pod.get("metadata").unwrap().get("namespace").unwrap(), "monitoring");
    }

    #[test]
    fn build_pod_defaults_namespace_when_parent_has_none() {
        let sts = json!({
            "metadata": {"name": "web"},
            "spec": {"template": {"spec": {"containers": []}}}
        });
        let (_, pod) = StatefulSetController::build_pod(&sts, 0).unwrap();
        assert_eq!(pod.get("metadata").unwrap().get("namespace").unwrap(), "default");
    }

    // ── volumeClaimTemplates → deterministic per-pod PVC volume name ───

    #[test]
    fn pvc_claim_name_follows_k8s_contract() {
        assert_eq!(
            StatefulSetController::pvc_claim_name("data", "vm", 2),
            "data-vm-2"
        );
    }

    #[test]
    fn build_pod_wires_pvc_volume_from_volume_claim_template() {
        // A STS with a volumeClaimTemplate gets a pod-side volume reference
        // to the deterministic per-pod PVC name `{template}-{sts}-{ordinal}`.
        // PVC provisioning itself is OUT of scope, but the reference is
        // correct the moment PVCs land.
        let sts = json!({
            "metadata": {"name": "vm", "namespace": "monitoring"},
            "spec": {
                "volumeClaimTemplates": [
                    {"metadata": {"name": "data"}, "spec": {"resources": {}}}
                ],
                "template": {
                    "metadata": {"labels": {"app": "vm"}},
                    "spec": {"containers": [{"name": "c", "image": "img"}]}
                }
            }
        });
        let (_, pod) = StatefulSetController::build_pod(&sts, 1).unwrap();
        let volumes = pod
            .get("spec")
            .and_then(|s| s.get("volumes"))
            .and_then(|v| v.as_array())
            .expect("pod has volumes wired from the template");
        assert_eq!(volumes.len(), 1);
        assert_eq!(volumes[0].get("name").unwrap(), "data");
        assert_eq!(
            volumes[0]
                .get("persistentVolumeClaim")
                .unwrap()
                .get("claimName")
                .unwrap(),
            "data-vm-1"
        );
    }

    #[test]
    fn build_pod_pvc_wiring_is_idempotent_with_pre_declared_volume() {
        // If the operator pre-declared the volume on the pod template, the
        // controller leaves it untouched (no duplicate).
        let sts = json!({
            "metadata": {"name": "vm"},
            "spec": {
                "volumeClaimTemplates": [{"metadata": {"name": "data"}}],
                "template": {"spec": {
                    "containers": [],
                    "volumes": [{"name": "data", "emptyDir": {}}]
                }}
            }
        });
        let (_, pod) = StatefulSetController::build_pod(&sts, 0).unwrap();
        let volumes = pod
            .get("spec")
            .and_then(|s| s.get("volumes"))
            .and_then(|v| v.as_array())
            .unwrap();
        assert_eq!(volumes.len(), 1, "pre-declared volume not duplicated");
        // The pre-declared emptyDir wins (operator override).
        assert!(volumes[0].get("emptyDir").is_some());
    }

    #[test]
    fn build_pod_no_volumes_without_volume_claim_templates() {
        let sts = json!({
            "metadata": {"name": "vm"},
            "spec": {"template": {"spec": {"containers": []}}}
        });
        let (_, pod) = StatefulSetController::build_pod(&sts, 0).unwrap();
        // No volumeClaimTemplates → no volumes injected.
        assert!(pod.get("spec").and_then(|s| s.get("volumes")).is_none());
    }

    // ── integration over the store: ordinal scale-up / scale-down ──────

    use engenho_store::command::Reason;
    use engenho_store::{InProcessRouter, default_config};
    use std::sync::Arc;
    use std::time::Duration;

    async fn test_store() -> Arc<StoreMesh> {
        let router = InProcessRouter::new();
        let cfg = default_config("controllers-statefulset").unwrap();
        let store = Arc::new(
            StoreMesh::start(1, "in-process://1".into(), router, cfg)
                .await
                .unwrap(),
        );
        store.initialize_singleton().await.unwrap();
        assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
        store
    }

    async fn seed_sts(store: &StoreMesh, ns: &str, name: &str, replicas: i64) -> String {
        let key = ResourceKey::namespaced("apps", "v1", "StatefulSet", ns, name);
        store
            .propose(ResourceCommand::put(
                key.clone(),
                json!({
                    "kind": "StatefulSet", "apiVersion": "apps/v1",
                    "metadata": {"name": name, "namespace": ns},
                    "spec": {
                        "replicas": replicas,
                        "template": {
                            "metadata": {"labels": {"app": name}},
                            "spec": {"containers": [{"name": "c", "image": "img"}]}
                        }
                    }
                }),
                Reason::Operator,
            ))
            .await
            .unwrap();
        store.get(&key).await.unwrap().uid().unwrap().to_string()
    }

    async fn owned_pod_names(store: &StoreMesh, ns: &str, uid: &str) -> Vec<String> {
        let pods = store.list("", "v1", "Pod", Some(ns)).await;
        let mut names: Vec<String> = pods
            .iter()
            .filter(|(_, p)| crate::owner::is_owned_by(p, uid))
            .map(|(k, _)| k.name.clone())
            .collect();
        names.sort();
        names
    }

    #[tokio::test]
    async fn ordinal_naming_and_namespace_inherited() {
        let store = test_store().await;
        let uid = seed_sts(&store, "monitoring", "vm", 3).await;
        let c = StatefulSetController { store: store.clone(), namespace: None };
        c.tick().await.unwrap();

        let names = owned_pod_names(&store, "monitoring", &uid).await;
        assert_eq!(names, vec!["vm-0", "vm-1", "vm-2"], "ordinal pod names");

        // All pods land in the STS namespace, not "default".
        let in_default = store.list("", "v1", "Pod", Some("default")).await;
        assert!(in_default.is_empty(), "no STS pods in default namespace");
    }

    #[tokio::test]
    async fn scale_up_adds_the_next_highest_ordinal() {
        let store = test_store().await;
        let uid = seed_sts(&store, "default", "vm", 1).await;
        let c = StatefulSetController { store: store.clone(), namespace: None };
        c.tick().await.unwrap();
        assert_eq!(owned_pod_names(&store, "default", &uid).await, vec!["vm-0"]);

        // Scale to 3 → adds vm-1, vm-2 (vm-0 untouched).
        store
            .propose(ResourceCommand::patch(
                ResourceKey::namespaced("apps", "v1", "StatefulSet", "default", "vm"),
                json!({"spec": {"replicas": 3}}),
                Reason::Operator,
            ))
            .await
            .unwrap();
        c.tick().await.unwrap();
        assert_eq!(
            owned_pod_names(&store, "default", &uid).await,
            vec!["vm-0", "vm-1", "vm-2"]
        );
    }

    #[tokio::test]
    async fn scale_down_removes_the_highest_ordinals() {
        let store = test_store().await;
        let uid = seed_sts(&store, "default", "vm", 3).await;
        let c = StatefulSetController { store: store.clone(), namespace: None };
        c.tick().await.unwrap();
        assert_eq!(
            owned_pod_names(&store, "default", &uid).await,
            vec!["vm-0", "vm-1", "vm-2"]
        );

        // Scale to 1 → removes vm-2, vm-1 (highest first), keeps vm-0.
        store
            .propose(ResourceCommand::patch(
                ResourceKey::namespaced("apps", "v1", "StatefulSet", "default", "vm"),
                json!({"spec": {"replicas": 1}}),
                Reason::Operator,
            ))
            .await
            .unwrap();
        c.tick().await.unwrap();
        assert_eq!(owned_pod_names(&store, "default", &uid).await, vec!["vm-0"]);
    }

    #[tokio::test]
    async fn statefulset_no_thrash_stable_across_ticks() {
        let store = test_store().await;
        let _uid = seed_sts(&store, "default", "vm", 2).await;
        let c = StatefulSetController { store: store.clone(), namespace: None };
        c.tick().await.unwrap();
        let rev_a = store.current_catalog().await.revision();
        for _ in 0..3 {
            c.tick().await.unwrap();
        }
        let rev_b = store.current_catalog().await.revision();
        assert_eq!(rev_a, rev_b, "converged StatefulSet must not thrash");
    }
}
