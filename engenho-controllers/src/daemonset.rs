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
//! ## Updates (`spec.updateStrategy`)
//!
//! Every pod is stamped with the revision of the template it was built from
//! ([`crate::rollout`]). Under `RollingUpdate` (the default, as upstream,
//! with `maxUnavailable: 1`) an out-of-date pod is deleted and its node gets
//! a pod from the current template; out-of-date pods that are not Ready go at
//! once, Ready ones one budget's worth at a time. Under `OnDelete` a pod is
//! only replaced once something else deletes it. A pod in phase `Failed` is
//! deleted and replaced under either strategy, with a per-node backoff.
//!
//! This is what turns a template change — a new image after the daemon
//! restarts onto a new release — into replaced pods without an operator.
//! Before it, a node with a pod was covered whatever the pod was built from,
//! and a pod whose image had been garbage-collected waited forever.
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
use tracing::{debug, info};

use crate::error::ControllerError;
use crate::event_recorder::Reason as EventReason;
use crate::meta::{ObjectMeta, ShapeError};
use crate::owned_children::{
    OwnedChildrenReconciler, ParentGvk, ReconcileDelta, live_children, pod_from_template,
    template_object_mut,
};
use crate::owner::{OwnerReference, owner_ref_for};
use crate::reads::gvk;
use crate::rollout::{TemplateRevision, UpdateStrategy, pod_is_failed};
use crate::status::pod_is_ready;
use crate::sweep::{Sweep, impl_sweep_event_sink};
use engenho_substrate::Clock;
use engenho_types::kind::GroupVersionKind;

/// DaemonSet controller — one node-pinned Pod per schedulable node.
pub struct DaemonSetController {
    store: Arc<StoreMesh>,
    namespace: Option<String>,
    /// Per-DaemonSet isolation (`FailedCreate` on a malformed template).
    sweep: Sweep,
    /// The clock the failed-pod backoff reads.
    clock: Arc<dyn Clock>,
    /// How soon a node's failed pod may be deleted again.
    failed_backoff: FailedPodBackoff,
}

impl_sweep_event_sink!(DaemonSetController);

impl DaemonSetController {
    /// Construct with optional namespace scope (`None` = all namespaces).
    #[must_use]
    pub fn new(store: Arc<StoreMesh>, namespace: Option<String>) -> Self {
        Self {
            store,
            namespace,
            sweep: Sweep::new("daemonset-controller", EventReason::FailedCreate),
            clock: Arc::new(engenho_substrate::WallClock),
            failed_backoff: FailedPodBackoff::default(),
        }
    }

    /// Builder: the clock the failed-pod backoff reads (tests pin one).
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
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
    ///
    /// `Ok(None)` when the `DaemonSet` has no name or no template.
    ///
    /// # Errors
    ///
    /// [`ShapeError`] when the template (or its `spec`) has the wrong JSON
    /// shape. It used to skip such a template's `spec` silently, creating a
    /// pod the scheduler would then place on ANY node.
    fn build_pod_for_node(
        ds: &Value,
        node_name: &str,
        owner: OwnerReference,
    ) -> Result<Option<(String, Value)>, ShapeError> {
        let Some(ds_name) = ds.name() else {
            return Ok(None);
        };
        let ds_namespace = ds.namespace().unwrap_or("default");
        let pod_name = format!("{ds_name}-{node_name}");
        let Some(mut pod) = pod_from_template(ds, &pod_name, Some(ds_namespace), owner)? else {
            return Ok(None);
        };
        // Node-pin: pre-set spec.nodeName so the scheduler never binds it.
        template_object_mut(&mut pod, &["spec"])?.insert("nodeName".into(), Value::from(node_name));
        Ok(Some((pod_name, pod)))
    }

    /// The failed pods to delete this pass.
    ///
    /// Upstream's `podsShouldBeOnNode` deletes a daemon pod in phase `Failed`
    /// so the node gets a fresh one from the CURRENT template. engenho kept
    /// it forever: the node was covered by name. A native pod whose closure
    /// is gone fails as `ImageUnavailable`, so this is also what recovers a
    /// node from a garbage-collected image. Rate-limited per node, as
    /// upstream's `failedPodsBackoff` is, so a template that fails every
    /// time does not create and delete at the loop's speed; a node whose pod
    /// is Ready is forgiven.
    fn failed_deletions<'o>(
        &self,
        ds_name: &str,
        ds_uid: &str,
        owned: &'o [(ResourceKey, Value)],
        deleting: &std::collections::BTreeSet<&ResourceKey>,
    ) -> Vec<&'o ResourceKey> {
        let mut out = Vec::new();
        for (pod_key, pod_value) in live_children(owned) {
            let node = Self::pod_node(pod_value).unwrap_or_default();
            if pod_is_ready(pod_value) {
                self.failed_backoff.clear(ds_uid, node);
            }
            if deleting.contains(pod_key) || !pod_is_failed(pod_value) {
                continue;
            }
            if self
                .failed_backoff
                .admit(ds_uid, node, self.clock.unix_ms())
            {
                info!(
                    daemonset = %ds_name,
                    pod = %pod_key.label(),
                    node,
                    reason = pod_value
                        .pointer("/status/reason")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or(""),
                    "deleting failed daemon pod so its node gets a fresh one"
                );
                out.push(pod_key);
            }
        }
        out
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

    fn child_kinds(&self) -> &'static [GroupVersionKind] {
        const CHILD_KINDS: &[GroupVersionKind] = &[gvk("", "v1", "Pod")];
        CHILD_KINDS
    }

    /// The Nodes it places one pod on each of.
    fn also_reads(&self) -> &'static [GroupVersionKind] {
        const ALSO: &[GroupVersionKind] = &[gvk("", "v1", "Node")];
        ALSO
    }

    fn store(&self) -> &StoreMesh {
        &self.store
    }

    fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    fn sweep(&self) -> &Sweep {
        &self.sweep
    }

    async fn reconcile_one(
        &self,
        ds_value: &Value,
        owned: &[(ResourceKey, Value)],
    ) -> Result<ReconcileDelta, ControllerError> {
        // No name / owner-ref → no-op (parent freshly minted; the blanket
        // already skipped no-uid parents).
        let Some(ds_name) = ds_value.name() else {
            return Ok(ReconcileDelta::none());
        };
        let Some(owner_ref) = owner_ref_for(ds_value, "apps/v1", "DaemonSet") else {
            return Ok(ReconcileDelta::none());
        };
        // A strategy engenho cannot read refuses THIS DaemonSet (an Event on
        // it), before anything is created or deleted for it.
        let strategy = UpdateStrategy::of(ds_value)?;
        let revision = TemplateRevision::of(ds_value);
        let generation = crate::status::generation_of(ds_value).ok();

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

        // The nodes already covered by an owned pod. A Terminating pod
        // still covers its node: its replacement would take the same name
        // (`{ds}-{node}`), so it waits for the old one to go. Upstream names
        // daemon pods by hash and replaces at once. Deleting a Terminating
        // pod again is left out by the blanket (I3). A Failed pod covers its
        // node too, until the delete below has removed it.
        let covered: std::collections::BTreeSet<String> = owned
            .iter()
            .filter_map(|(_, p)| Self::pod_node(p).map(String::from))
            .collect();

        let mut commands = Vec::new();
        let mut deleting: std::collections::BTreeSet<&ResourceKey> =
            std::collections::BTreeSet::new();

        // Create one pod per schedulable node NOT yet covered, stamped with
        // the template revision it is built from.
        for node_name in &schedulable {
            if covered.contains(node_name) {
                continue;
            }
            let Some((pod_name, mut pod)) =
                Self::build_pod_for_node(ds_value, node_name, owner_ref.clone())?
            else {
                continue;
            };
            if let Some(rev) = &revision {
                rev.stamp(&mut pod, generation)
                    .map_err(|e| e.under(crate::owned_children::TEMPLATE))?;
            }
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
            let on_scheduled_node =
                Self::pod_node(pod_value).is_some_and(|n| schedulable.contains(n));
            if !on_scheduled_node {
                debug!(pod = %pod_key.label(), "deleting daemon pod on missing/unschedulable node");
                commands.push(ResourceCommand::delete(pod_key.clone(), Reason::Controller));
                deleting.insert(pod_key);
            }
        }

        // ── ★ A FAILED DAEMON POD IS REPLACED (see `failed_deletions`) ──────
        let ds_uid = ds_value.uid().unwrap_or(ds_name);
        for key in self.failed_deletions(ds_name, ds_uid, owned, &deleting) {
            commands.push(ResourceCommand::delete(key.clone(), Reason::Controller));
            deleting.insert(key);
        }

        // ── ★ ROLLING UPDATE ───────────────────────────────────────────────
        if let (
            Some(rev),
            UpdateStrategy::RollingUpdate {
                max_unavailable, ..
            },
        ) = (&revision, strategy)
        {
            let budget = max_unavailable.resolve(schedulable.len());
            for (pod_key, pod_value) in
                rolling_deletions(rev, budget, &schedulable, owned, &deleting)
            {
                info!(
                    daemonset = %ds_name,
                    pod = %pod_key.label(),
                    node = Self::pod_node(pod_value).unwrap_or_default(),
                    from = TemplateRevision::of_pod(pod_value).unwrap_or("<none>"),
                    to = rev.as_str(),
                    max_unavailable = budget,
                    "rolling update: replacing out-of-date daemon pod"
                );
                commands.push(ResourceCommand::delete(pod_key.clone(), Reason::Controller));
            }
        }

        Ok(ReconcileDelta::from_commands(commands))
    }

    fn compute_status(
        &self,
        ds_value: &Value,
        owned_now: &[(ResourceKey, Value)],
        observed_generation: i64,
    ) -> Option<Value> {
        // Status from the LIVE owned pods AFTER the reconcile.
        //   desiredNumberScheduled = # schedulable nodes is NOT re-derived
        //     here (the blanket re-lists owned children, not nodes); we
        //     report the OWNED-pod-derived counts, which after a converged
        //     tick equal the desired set. `desiredNumberScheduled` ==
        //     `currentNumberScheduled` == owned pod count (one per node);
        //     `numberReady` counts Ready=True owned pods;
        //     `updatedNumberScheduled` counts the pods built from the
        //     CURRENT template revision — the number `kubectl rollout status`
        //     waits on.
        let scheduled = i64::try_from(owned_now.len()).unwrap_or(i64::MAX);
        let ready = i64::try_from(owned_now.iter().filter(|(_, p)| pod_is_ready(p)).count())
            .unwrap_or(i64::MAX);
        let updated = TemplateRevision::of(ds_value).map_or(scheduled, |rev| {
            i64::try_from(owned_now.iter().filter(|(_, p)| rev.is_current(p)).count())
                .unwrap_or(i64::MAX)
        });
        Some(json!({
            "desiredNumberScheduled": scheduled,
            "currentNumberScheduled": scheduled,
            "numberReady": ready,
            "numberAvailable": ready,
            "numberUnavailable": scheduled - ready,
            "updatedNumberScheduled": updated,
            "observedGeneration": observed_generation,
        }))
    }
}

/// The out-of-date pods a `RollingUpdate` deletes this pass — upstream's
/// `DaemonSetsController.rollingUpdate`, with the budget counted per
/// schedulable node:
///
/// * a node whose pod is missing, Terminating or already being deleted is
///   UNAVAILABLE. Upstream does not count a node with no pod, because it
///   names the replacement by hash and creates it at once; engenho's
///   replacement takes the same name and waits for the old pod to go, so the
///   gap between delete and create is real downtime and is charged;
/// * a node whose pod is current and not Ready is unavailable;
/// * an out-of-date pod that is not Ready is deleted WHATEVER the budget —
///   it serves nothing, so replacing it costs nothing. This is the arm that
///   frees a node whose pod can never start (its image is gone);
/// * an out-of-date Ready pod is deleted only while the budget has room.
///
/// Candidates are taken in node order, so the choice is deterministic.
fn rolling_deletions<'o>(
    rev: &TemplateRevision,
    budget: usize,
    schedulable: &std::collections::BTreeSet<String>,
    owned: &'o [(ResourceKey, Value)],
    deleting: &std::collections::BTreeSet<&ResourceKey>,
) -> Vec<&'o (ResourceKey, Value)> {
    let mut unavailable = 0usize;
    let mut replace_now = Vec::new();
    let mut candidates = Vec::new();
    for node in schedulable {
        let on_node: Vec<&(ResourceKey, Value)> = owned
            .iter()
            .filter(|(_, p)| DaemonSetController::pod_node(p) == Some(node.as_str()))
            .collect();
        let live: Vec<&(ResourceKey, Value)> = on_node
            .iter()
            .copied()
            .filter(|(k, p)| !p.is_terminating() && !deleting.contains(k) && !pod_is_failed(p))
            .collect();
        let [pod] = live.as_slice() else {
            // No live pod (missing, Terminating, Failed or being deleted), or
            // more than one: the node is not serving a current pod.
            unavailable += 1;
            continue;
        };
        let (_, value) = pod;
        match (rev.is_current(value), pod_is_ready(value)) {
            (true, true) => {}
            (true, false) => unavailable += 1,
            (false, false) => replace_now.push(*pod),
            (false, true) => candidates.push(*pod),
        }
    }
    let room = budget.saturating_sub(unavailable + replace_now.len());
    replace_now.extend(candidates.into_iter().take(room));
    replace_now
}

/// Per-node backoff before a failed daemon pod is deleted again: upstream's
/// `failedPodsBackoff` (1 s doubling to 15 min), keyed by `DaemonSet` uid and
/// node. The first failure on a node is replaced at once; a node whose pod
/// comes up Ready is forgiven.
#[derive(Debug, Default)]
struct FailedPodBackoff {
    entries: std::sync::Mutex<std::collections::BTreeMap<(String, String), (u32, u64)>>,
}

impl FailedPodBackoff {
    const INITIAL_MS: u64 = 1_000;
    const MAX_MS: u64 = 15 * 60 * 1_000;

    /// Whether a failed pod on `node` may be deleted at `now_ms`; records the
    /// deletion when it may.
    fn admit(&self, ds_uid: &str, node: &str, now_ms: u64) -> bool {
        let Ok(mut entries) = self.entries.lock() else {
            return true;
        };
        let key = (ds_uid.to_string(), node.to_string());
        let admit = entries.get(&key).is_none_or(|&(n, last)| {
            let wait = Self::INITIAL_MS
                .saturating_mul(1u64 << (n.saturating_sub(1)).min(20))
                .min(Self::MAX_MS);
            now_ms.saturating_sub(last) >= wait
        });
        if admit {
            let n = entries.get(&key).map_or(0, |&(n, _)| n);
            entries.insert(key, (n.saturating_add(1), now_ms));
        }
        admit
    }

    fn clear(&self, ds_uid: &str, node: &str) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.remove(&(ds_uid.to_string(), node.to_string()));
        }
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

    fn ds_owner() -> OwnerReference {
        OwnerReference {
            api_version: "apps/v1".into(),
            kind: "DaemonSet".into(),
            name: "ds".into(),
            uid: "uid-ds".into(),
            controller: true,
            block_owner_deletion: true,
        }
    }

    // ── unit: pure helpers ────────────────────────────────────────────

    #[test]
    fn node_is_schedulable_default_and_cordon() {
        assert!(DaemonSetController::node_is_schedulable(
            &json!({"spec": {}})
        ));
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
        let (name, pod) = DaemonSetController::build_pod_for_node(&ds, "node-A", ds_owner())
            .unwrap()
            .unwrap();
        assert_eq!(name, "vector-node-A");
        assert_eq!(
            pod["status"]["phase"], "Pending",
            "a controller-built pod is born with status: {pod}"
        );
        assert_eq!(pod.get("kind").unwrap(), "Pod");
        // Namespace inherited from the parent DS — NOT "default".
        assert_eq!(
            pod.get("metadata").unwrap().get("namespace").unwrap(),
            "observability"
        );
        // Node-pinned: spec.nodeName pre-set (scheduler bypassed).
        assert_eq!(pod.get("spec").unwrap().get("nodeName").unwrap(), "node-A");
        // Template labels survive.
        assert_eq!(
            pod.get("metadata")
                .unwrap()
                .get("labels")
                .unwrap()
                .get("app")
                .unwrap(),
            "vector"
        );
    }

    #[test]
    fn build_pod_for_node_defaults_namespace_when_parent_has_none() {
        let ds = json!({
            "metadata": {"name": "ds"},
            "spec": {"template": {"spec": {"containers": []}}}
        });
        let (_, pod) = DaemonSetController::build_pod_for_node(&ds, "n1", ds_owner())
            .unwrap()
            .unwrap();
        assert_eq!(
            pod.get("metadata").unwrap().get("namespace").unwrap(),
            "default"
        );
    }

    // ── integration over the store ────────────────────────────────────

    #[tokio::test]
    async fn one_pod_per_schedulable_node_namespace_inherited() {
        let store = test_store().await;
        seed_node(&store, "node-A", true).await;
        seed_node(&store, "node-B", true).await;
        seed_node(&store, "node-C", false).await; // cordoned → no pod
        let uid = seed_ds(&store, "observability", "vector").await;

        let c = DaemonSetController::new(store.clone(), None);
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
            assert!(
                node == "node-A" || node == "node-B",
                "pinned to a schedulable node"
            );
        }
        // No pod on the cordoned node-C.
        assert!(
            !owned
                .iter()
                .any(|(_, p)| DaemonSetController::pod_node(p) == Some("node-C")),
            "cordoned node must get no daemon pod"
        );
    }

    #[tokio::test]
    async fn self_heal_recreates_deleted_pod_on_its_node() {
        let store = test_store().await;
        seed_node(&store, "node-A", true).await;
        let uid = seed_ds(&store, "default", "ds").await;

        let c = DaemonSetController::new(store.clone(), None);
        c.tick().await.unwrap();
        let pod_key = ResourceKey::namespaced("", "v1", "Pod", "default", "ds-node-A");
        assert!(
            store.get(&pod_key).await.is_some(),
            "first tick creates the pod"
        );

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

        let c = DaemonSetController::new(store.clone(), None);
        c.tick().await.unwrap();
        assert!(
            store
                .get(&ResourceKey::namespaced(
                    "",
                    "v1",
                    "Pod",
                    "default",
                    "ds-node-B"
                ))
                .await
                .is_some()
        );

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
            store
                .get(&ResourceKey::namespaced(
                    "",
                    "v1",
                    "Pod",
                    "default",
                    "ds-node-B"
                ))
                .await
                .is_none(),
            "pod on removed node must be GC'd"
        );
        assert!(
            store
                .get(&ResourceKey::namespaced(
                    "",
                    "v1",
                    "Pod",
                    "default",
                    "ds-node-A"
                ))
                .await
                .is_some(),
            "pod on surviving node stays"
        );
    }

    #[tokio::test]
    async fn no_thrash_stable_across_ticks() {
        let store = test_store().await;
        seed_node(&store, "node-A", true).await;
        seed_node(&store, "node-B", true).await;
        let _uid = seed_ds(&store, "default", "ds").await;

        let c = DaemonSetController::new(store.clone(), None);
        c.tick().await.unwrap();
        let rev_a = store.current_revision().await;
        // Several idle ticks: at the fixpoint nothing is proposed.
        for _ in 0..3 {
            c.tick().await.unwrap();
        }
        let rev_b = store.current_revision().await;
        assert_eq!(rev_a, rev_b, "converged DaemonSet must not thrash");
    }

    #[tokio::test]
    async fn status_counts_scheduled_pods() {
        let store = test_store().await;
        seed_node(&store, "node-A", true).await;
        seed_node(&store, "node-B", true).await;
        seed_ds(&store, "default", "ds").await;

        let c = DaemonSetController::new(store.clone(), None);
        c.tick().await.unwrap();

        let ds = store
            .get(&ResourceKey::namespaced(
                "apps",
                "v1",
                "DaemonSet",
                "default",
                "ds",
            ))
            .await
            .unwrap();
        let status = ds.get("status").expect("status written");
        assert_eq!(status.get("desiredNumberScheduled").unwrap(), 2);
        assert_eq!(status.get("currentNumberScheduled").unwrap(), 2);
        // Pods aren't Ready (no kubelet in this test) → numberReady == 0.
        assert_eq!(status.get("numberReady").unwrap(), 0);
    }
}
