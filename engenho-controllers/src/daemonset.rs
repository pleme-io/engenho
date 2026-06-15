//! `DaemonSetController` — ensures exactly ONE Pod per schedulable
//! node, owned by the DaemonSet.
//!
//! ## The DaemonSet contract (distinct from ReplicaSet)
//!
//! A DaemonSet does NOT have a `spec.replicas` knob. Its desired pod
//! set is "one pod on every schedulable node" — node-pinned, scheduler-
//! bypassing. So unlike ReplicaSet/StatefulSet (which clone N pods the
//! scheduler then binds), the DaemonSet controller:
//!
//!   1. enumerates schedulable Nodes (cluster-scoped `Node` objects,
//!      skipping those with `spec.unschedulable == true`),
//!   2. for each node WITHOUT an owned pod, creates one pod from
//!      `spec.template` with `spec.nodeName` PRE-SET to that node (the
//!      scheduler never touches it — DaemonSet pods are node-pinned),
//!   3. for each owned pod on a node that no longer exists / is now
//!      unschedulable, deletes it (node removed → pod GC'd; owner-ref
//!      GC also covers DaemonSet deletion).
//!
//! Pod name: `{ds}-{node}` — deterministic (one pod per node, so the
//! node name is a natural unique key). Production K8s uses a hash
//! suffix; we keep it deterministic + readable for tests + operators,
//! the same way ReplicaSet uses `{rs}-{index}`.
//!
//! Self-heal: a deleted pod is recreated on its node on the next tick
//! (the node still exists + has no owned pod → step 2 fires).
//!
//! ## Why node enumeration lives in `reconcile_one`
//!
//! The shared [`OwnedChildrenReconciler`] blanket hands each parent its
//! owned children, but a DaemonSet's desired set is a function of the
//! NODE list (not the child count). The controller reads the cluster's
//! Nodes from `self.store()` inside `reconcile_one` — the one place the
//! per-parent delta is computed. Nodes are cluster-scoped, so the read
//! is `list("", "v1", "Node", None)` regardless of the DS's namespace.

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

/// DaemonSet controller — one node-pinned Pod per schedulable node.
pub struct DaemonSetController {
    store: Arc<StoreMesh>,
    namespace: Option<String>,
}

impl DaemonSetController {
    /// Construct with optional namespace scope (`None` = all namespaces).
    #[must_use]
    pub fn new(store: Arc<StoreMesh>, namespace: Option<String>) -> Self {
        Self { store, namespace }
    }

    /// A node's `metadata.name`, if present.
    fn node_name_of(node: &Value) -> Option<&str> {
        node.name()
    }

    /// True iff a node is schedulable — i.e. NOT cordoned
    /// (`spec.unschedulable != true`). Mirrors the self-registered
    /// node's `spec.unschedulable == false` shape the runtime stamps.
    fn node_is_schedulable(node: &Value) -> bool {
        !node
            .get("spec")
            .and_then(|s| s.get("unschedulable"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    }

    /// Build a node-pinned Pod from the DaemonSet template for `node`.
    /// The name is `{ds_name}-{node}`; `spec.nodeName` is PRE-SET to the
    /// node (the scheduler is bypassed — DaemonSet pods are node-pinned).
    ///
    /// The Pod lives in the SAME namespace as its parent DaemonSet — the
    /// namespace the blanket gathers owned pods from. Never the
    /// controller's scope namespace (an all-namespace controller has
    /// none). Mirrors `ReplicaSetController::build_pod_from_template`.
    fn build_pod_for_node(ds: &Value, node_name: &str) -> Option<(String, Value)> {
        let ds_name = ds.name()?;
        let ds_namespace =
            ds.namespace().map_or_else(|| "default".to_string(), |c| c.to_owned());
        let template = ds.get("spec").and_then(|s| s.get("template"))?;
        let mut pod = template.clone();
        let pod_obj = pod.as_object_mut()?;
        pod_obj.insert("kind".into(), Value::String("Pod".into()));
        pod_obj.insert("apiVersion".into(), Value::String("v1".into()));

        let pod_name = format!("{ds_name}-{node_name}");
        let metadata = pod_obj
            .entry("metadata".to_string())
            .or_insert_with(|| json!({}));
        let m = metadata.as_object_mut()?;
        m.insert("name".into(), Value::String(pod_name.clone()));
        m.insert("namespace".into(), Value::String(ds_namespace));

        // Node-pin: pre-set spec.nodeName so the scheduler never binds it.
        let spec = pod_obj
            .entry("spec".to_string())
            .or_insert_with(|| json!({}));
        if let Some(spec_obj) = spec.as_object_mut() {
            spec_obj.insert("nodeName".into(), Value::String(node_name.to_string()));
        }
        Some((pod_name, pod))
    }

    /// The node an owned pod is pinned to (`spec.nodeName`), if present.
    fn pod_node(pod: &Value) -> Option<&str> {
        pod.get("spec")
            .and_then(|s| s.get("nodeName"))
            .and_then(|n| n.as_str())
            .filter(|s| !s.is_empty())
    }
}

#[async_trait]
impl OwnedChildrenReconciler for DaemonSetController {
    fn name(&self) -> &'static str {
        "daemonset"
    }

    fn parent_gvk(&self) -> ParentGvk {
        ParentGvk::new("apps", "v1", "DaemonSet", "apps/v1")
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
        ds_value: &Value,
        owned: &[(ResourceKey, Value)],
    ) -> Result<ReconcileDelta, ControllerError> {
        // No name / owner-ref → no-op (parent freshly minted; the blanket
        // already skipped no-uid parents).
        let Some(_ds_name) = ds_value.name() else {
            return Ok(ReconcileDelta::none());
        };
        let Some(owner_ref) = owner_ref_for(ds_value, "apps/v1", "DaemonSet") else {
            return Ok(ReconcileDelta::none());
        };

        // Enumerate schedulable nodes (cluster-scoped). The desired set is
        // one pod per schedulable node — node-pinned.
        let nodes = self.store.list("", "v1", "Node", None).await;
        let schedulable: std::collections::BTreeSet<String> = nodes
            .iter()
            .filter(|(_, n)| Self::node_is_schedulable(n))
            .filter_map(|(_, n)| Self::node_name_of(n).map(String::from))
            .collect();

        // Pods go in the DS's OWN namespace (where owned-pod gathering
        // looks), not the controller scope ns.
        let pod_ns = ds_value.namespace().map_or_else(
            || self.namespace.as_deref().unwrap_or("default").to_string(),
            |c| c.to_owned(),
        );

        // The nodes already covered by an owned pod.
        let covered: std::collections::BTreeSet<String> = owned
            .iter()
            .filter_map(|(_, p)| Self::pod_node(p).map(String::from))
            .collect();

        let mut commands = Vec::new();

        // Create one pod per schedulable node NOT yet covered.
        for node_name in &schedulable {
            if covered.contains(node_name) {
                continue;
            }
            let Some((pod_name, mut pod)) = Self::build_pod_for_node(ds_value, node_name) else {
                continue;
            };
            set_owner_reference(&mut pod, owner_ref.clone());
            let pod_key = ResourceKey::namespaced("", "v1", "Pod", &pod_ns, &pod_name);
            debug!(node = %node_name, pod = %pod_name, "creating node-pinned daemon pod");
            commands.push(ResourceCommand::Put {
                key: pod_key,
                value: pod,
                expected: None,
                reason: Reason::Controller,
            });
        }

        // Delete owned pods whose node no longer exists / is unschedulable
        // (node removed → pod GC'd). Owner-ref GC covers DS deletion; this
        // covers the per-node case.
        for (pod_key, pod_value) in owned {
            let on_scheduled_node = Self::pod_node(pod_value)
                .is_some_and(|n| schedulable.contains(n));
            if !on_scheduled_node {
                debug!(pod = %pod_key.label(), "deleting daemon pod on missing/unschedulable node");
                commands.push(ResourceCommand::delete(pod_key.clone(), Reason::Controller));
            }
        }

        Ok(ReconcileDelta::from_commands(commands))
    }

    fn compute_status(
        &self,
        ds_value: &Value,
        owned_now: &[(ResourceKey, Value)],
    ) -> Option<Value> {
        // Status from the LIVE owned pods AFTER the reconcile.
        //   desiredNumberScheduled = # schedulable nodes is NOT re-derived
        //     here (the blanket re-lists owned children, not nodes); we
        //     report the OWNED-pod-derived counts, which after a converged
        //     tick equal the desired set. `desiredNumberScheduled` ==
        //     `currentNumberScheduled` == owned pod count (one per node);
        //     `numberReady` counts Ready=True owned pods.
        let scheduled = i64::try_from(owned_now.len()).unwrap_or(i64::MAX);
        let ready =
            i64::try_from(owned_now.iter().filter(|(_, p)| pod_is_ready(p)).count())
                .unwrap_or(i64::MAX);
        Some(json!({
            "desiredNumberScheduled": scheduled,
            "currentNumberScheduled": scheduled,
            "numberReady": ready,
            "numberAvailable": ready,
            "updatedNumberScheduled": scheduled,
            "observedGeneration": observed_generation(ds_value),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::Controller; // brings `.tick()` (blanket via OwnedChildrenReconciler) into scope
    use engenho_store::command::Reason;
    use engenho_store::{InProcessRouter, default_config};
    use serde_json::json;
    use std::time::Duration;

    async fn test_store() -> Arc<StoreMesh> {
        let router = InProcessRouter::new();
        let cfg = default_config("controllers-daemonset").unwrap();
        let store = Arc::new(
            StoreMesh::start(1, "in-process://1".into(), router, cfg)
                .await
                .unwrap(),
        );
        store.initialize_singleton().await.unwrap();
        assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
        store
    }

    /// Seed a schedulable Node into the store.
    async fn seed_node(store: &StoreMesh, name: &str, schedulable: bool) {
        let key = ResourceKey::cluster_scoped("", "v1", "Node", name);
        store
            .propose(ResourceCommand::put(
                key,
                json!({
                    "kind": "Node", "apiVersion": "v1",
                    "metadata": {"name": name},
                    "spec": {"unschedulable": !schedulable}
                }),
                Reason::Operator,
            ))
            .await
            .unwrap();
    }

    /// Seed a DaemonSet into the store (so it gets a uid the controller
    /// can owner-ref against).
    async fn seed_ds(store: &StoreMesh, ns: &str, name: &str) -> String {
        let key = ResourceKey::namespaced("apps", "v1", "DaemonSet", ns, name);
        store
            .propose(ResourceCommand::put(
                key.clone(),
                json!({
                    "kind": "DaemonSet", "apiVersion": "apps/v1",
                    "metadata": {"name": name, "namespace": ns},
                    "spec": {"template": {
                        "metadata": {"labels": {"app": name}},
                        "spec": {"containers": [{"name": "c", "image": "img"}]}
                    }}
                }),
                Reason::Operator,
            ))
            .await
            .unwrap();
        store.get(&key).await.unwrap().uid().unwrap().to_string()
    }

    // ── unit: pure helpers ────────────────────────────────────────────

    #[test]
    fn node_is_schedulable_default_and_cordon() {
        assert!(DaemonSetController::node_is_schedulable(&json!({"spec": {}})));
        assert!(DaemonSetController::node_is_schedulable(&json!({})));
        assert!(DaemonSetController::node_is_schedulable(
            &json!({"spec": {"unschedulable": false}})
        ));
        assert!(!DaemonSetController::node_is_schedulable(
            &json!({"spec": {"unschedulable": true}})
        ));
    }

    #[test]
    fn build_pod_for_node_pins_node_and_inherits_namespace() {
        let ds = json!({
            "metadata": {"name": "vector", "namespace": "observability"},
            "spec": {"template": {
                "metadata": {"labels": {"app": "vector"}},
                "spec": {"containers": [{"name": "c", "image": "img"}]}
            }}
        });
        let (name, pod) = DaemonSetController::build_pod_for_node(&ds, "node-A").unwrap();
        assert_eq!(name, "vector-node-A");
        assert_eq!(pod.get("kind").unwrap(), "Pod");
        // Namespace inherited from the parent DS — NOT "default".
        assert_eq!(
            pod.get("metadata").unwrap().get("namespace").unwrap(),
            "observability"
        );
        // Node-pinned: spec.nodeName pre-set (scheduler bypassed).
        assert_eq!(
            pod.get("spec").unwrap().get("nodeName").unwrap(),
            "node-A"
        );
        // Template labels survive.
        assert_eq!(
            pod.get("metadata").unwrap().get("labels").unwrap().get("app").unwrap(),
            "vector"
        );
    }

    #[test]
    fn build_pod_for_node_defaults_namespace_when_parent_has_none() {
        let ds = json!({
            "metadata": {"name": "ds"},
            "spec": {"template": {"spec": {"containers": []}}}
        });
        let (_, pod) = DaemonSetController::build_pod_for_node(&ds, "n1").unwrap();
        assert_eq!(pod.get("metadata").unwrap().get("namespace").unwrap(), "default");
    }

    // ── integration over the store ────────────────────────────────────

    #[tokio::test]
    async fn one_pod_per_schedulable_node_namespace_inherited() {
        let store = test_store().await;
        seed_node(&store, "node-A", true).await;
        seed_node(&store, "node-B", true).await;
        seed_node(&store, "node-C", false).await; // cordoned → no pod
        let uid = seed_ds(&store, "observability", "vector").await;

        let c = DaemonSetController { store: store.clone(), namespace: None };
        c.tick().await.unwrap();

        // Exactly 2 pods (one per schedulable node), both in the DS namespace,
        // both node-pinned + owned by the DS.
        let pods = store.list("", "v1", "Pod", Some("observability")).await;
        let owned: Vec<_> = pods
            .iter()
            .filter(|(_, p)| crate::owner::is_owned_by(p, &uid))
            .collect();
        assert_eq!(owned.len(), 2, "one pod per schedulable node (C cordoned)");

        for (key, pod) in &owned {
            assert_eq!(key.namespace.as_deref(), Some("observability"));
            let node = DaemonSetController::pod_node(pod).unwrap();
            assert!(node == "node-A" || node == "node-B", "pinned to a schedulable node");
        }
        // No pod on the cordoned node-C.
        assert!(
            !owned.iter().any(|(_, p)| DaemonSetController::pod_node(p) == Some("node-C")),
            "cordoned node must get no daemon pod"
        );
    }

    #[tokio::test]
    async fn self_heal_recreates_deleted_pod_on_its_node() {
        let store = test_store().await;
        seed_node(&store, "node-A", true).await;
        let uid = seed_ds(&store, "default", "ds").await;

        let c = DaemonSetController { store: store.clone(), namespace: None };
        c.tick().await.unwrap();
        let pod_key = ResourceKey::namespaced("", "v1", "Pod", "default", "ds-node-A");
        assert!(store.get(&pod_key).await.is_some(), "first tick creates the pod");

        // Operator/chaos deletes the pod.
        store
            .propose(ResourceCommand::delete(pod_key.clone(), Reason::Operator))
            .await
            .unwrap();
        assert!(store.get(&pod_key).await.is_none(), "pod gone");

        // Next tick recreates it on node-A.
        c.tick().await.unwrap();
        let pod = store.get(&pod_key).await.expect("pod recreated");
        assert!(crate::owner::is_owned_by(&pod, &uid));
        assert_eq!(DaemonSetController::pod_node(&pod), Some("node-A"));
    }

    #[tokio::test]
    async fn removed_node_gcs_its_pod() {
        let store = test_store().await;
        seed_node(&store, "node-A", true).await;
        seed_node(&store, "node-B", true).await;
        let _uid = seed_ds(&store, "default", "ds").await;

        let c = DaemonSetController { store: store.clone(), namespace: None };
        c.tick().await.unwrap();
        assert!(store.get(&ResourceKey::namespaced("", "v1", "Pod", "default", "ds-node-B")).await.is_some());

        // node-B disappears (drained / removed from the cluster).
        store
            .propose(ResourceCommand::delete(
                ResourceKey::cluster_scoped("", "v1", "Node", "node-B"),
                Reason::Operator,
            ))
            .await
            .unwrap();

        // Next tick deletes the orphaned pod on node-B (node A's stays).
        c.tick().await.unwrap();
        assert!(
            store.get(&ResourceKey::namespaced("", "v1", "Pod", "default", "ds-node-B")).await.is_none(),
            "pod on removed node must be GC'd"
        );
        assert!(
            store.get(&ResourceKey::namespaced("", "v1", "Pod", "default", "ds-node-A")).await.is_some(),
            "pod on surviving node stays"
        );
    }

    #[tokio::test]
    async fn no_thrash_stable_across_ticks() {
        let store = test_store().await;
        seed_node(&store, "node-A", true).await;
        seed_node(&store, "node-B", true).await;
        let _uid = seed_ds(&store, "default", "ds").await;

        let c = DaemonSetController { store: store.clone(), namespace: None };
        c.tick().await.unwrap();
        let rev_a = store.current_catalog().await.revision();
        // Several idle ticks: at the fixpoint nothing is proposed.
        for _ in 0..3 {
            c.tick().await.unwrap();
        }
        let rev_b = store.current_catalog().await.revision();
        assert_eq!(rev_a, rev_b, "converged DaemonSet must not thrash");
    }

    #[tokio::test]
    async fn status_counts_scheduled_pods() {
        let store = test_store().await;
        seed_node(&store, "node-A", true).await;
        seed_node(&store, "node-B", true).await;
        seed_ds(&store, "default", "ds").await;

        let c = DaemonSetController { store: store.clone(), namespace: None };
        c.tick().await.unwrap();

        let ds = store
            .get(&ResourceKey::namespaced("apps", "v1", "DaemonSet", "default", "ds"))
            .await
            .unwrap();
        let status = ds.get("status").expect("status written");
        assert_eq!(status.get("desiredNumberScheduled").unwrap(), 2);
        assert_eq!(status.get("currentNumberScheduled").unwrap(), 2);
        // Pods aren't Ready (no kubelet in this test) → numberReady == 0.
        assert_eq!(status.get("numberReady").unwrap(), 0);
    }
}
