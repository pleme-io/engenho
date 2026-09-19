//! `ReplicaSetController` — keeps the observed pod count matching
//! `spec.replicas` per ReplicaSet.
//!
//! The reconciliation rule:
//!   * count the LIVE Pods owned by the `ReplicaSet` (controller-owned,
//!     not Terminating — upstream's `FilterActivePods`)
//!   * if count < replicas: create the difference (each Pod cloned
//!     from `spec.template` + owner-referenced + named uniquely)
//!   * if count > replicas: delete the excess among the live pods
//!     (eviction by name order for predictability)
//!
//! A Terminating pod is on its way out: it is not counted, never chosen
//! for eviction, and still holds its name, so its replacement takes the
//! next free index rather than its own.
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
use crate::event_recorder::Reason as EventReason;
use crate::meta::{ObjectMeta, REPLICAS, ShapeError};
use crate::owned_children::{
    OwnedChildrenReconciler, ParentGvk, ReconcileDelta, live_children, pod_from_template,
};
use crate::owner::{OwnerReference, owner_ref_for};
use crate::reads::gvk;
use crate::status::pod_is_ready;
use crate::sweep::{Sweep, impl_sweep_event_sink};
use engenho_types::kind::GroupVersionKind;

pub struct ReplicaSetController {
    store: Arc<StoreMesh>,
    /// Optional namespace scope. None = all namespaces.
    namespace: Option<String>,
    /// Per-`ReplicaSet` isolation: a `ReplicaSet` whose template has the wrong
    /// shape gets a `FailedCreate` Event and the others still converge.
    sweep: Sweep,
}

impl_sweep_event_sink!(ReplicaSetController);

impl ReplicaSetController {
    #[must_use]
    pub fn new(store: Arc<StoreMesh>, namespace: Option<String>) -> Self {
        Self {
            store,
            namespace,
            sweep: Sweep::new("replicaset-controller", EventReason::FailedCreate),
        }
    }

    /// Construct a Pod object from a `ReplicaSet`'s `spec.template`,
    /// owned by `owner`. Names the Pod `{rs_name}-{index}`, deterministic
    /// for tests + readable for operators.
    ///
    /// `Ok(None)` when the `ReplicaSet` has no name or no template.
    ///
    /// # Errors
    ///
    /// [`ShapeError`] when the template has the wrong JSON shape.
    fn build_pod_from_template(
        rs: &Value,
        index: usize,
        owner: OwnerReference,
    ) -> Result<Option<(String, Value)>, ShapeError> {
        let Some(rs_name) = rs.name() else {
            return Ok(None);
        };
        // Pod lives in the SAME namespace as its parent ReplicaSet — the
        // namespace the blanket gathers owned pods from. Never the
        // controller's scope namespace (an all-namespace controller has none).
        let rs_namespace = rs.namespace().unwrap_or("default");
        // Generate a deterministic-ish name: {rs}-{index}
        // (Production K8s uses random hashes; for R9 we keep it
        // deterministic for test reproducibility. R9.5+ can swap
        // in nanoid if name collisions matter.)
        let pod_name = format!("{rs_name}-{index}");
        let pod = pod_from_template(rs, &pod_name, Some(rs_namespace), owner)?;
        Ok(pod.map(|pod| (pod_name, pod)))
    }
}

/// The set of `<rs>-<index>` suffix indices already in use among the owned
/// pods. A pod whose name is `<rs_name>-<n>` (with `<n>` a parseable usize)
/// contributes `n`; any other shape is ignored (it can't collide on an index).
/// `rs_name = None` (a freshly-minted RS with no name) yields the empty set.
fn used_indices(
    rs_name: Option<&str>,
    owned_pods: &[(ResourceKey, Value)],
) -> std::collections::BTreeSet<usize> {
    let mut used = std::collections::BTreeSet::new();
    let Some(rs_name) = rs_name else {
        return used;
    };
    let prefix = format!("{rs_name}-");
    for (key, _) in owned_pods {
        if let Some(suffix) = key.name.strip_prefix(&prefix) {
            if let Ok(n) = suffix.parse::<usize>() {
                used.insert(n);
            }
        }
    }
    used
}

/// The lowest `count` indices NOT in `used`, ascending. Fills freed slots
/// first (so a middle delete recreates the freed index, never clobbering a
/// surviving pod), then extends past the high-water mark.
fn free_indices(used: &std::collections::BTreeSet<usize>, count: usize) -> Vec<usize> {
    let mut out = Vec::with_capacity(count);
    let mut idx = 0usize;
    while out.len() < count {
        if !used.contains(&idx) {
            out.push(idx);
        }
        idx += 1;
    }
    out
}

#[async_trait]
impl OwnedChildrenReconciler for ReplicaSetController {
    fn name(&self) -> &'static str {
        "replicaset"
    }

    fn parent_gvk(&self) -> ParentGvk {
        ParentGvk::new("apps", "v1", "ReplicaSet", "apps/v1")
    }

    fn child_kinds(&self) -> &'static [GroupVersionKind] {
        const CHILD_KINDS: &[GroupVersionKind] = &[gvk("", "v1", "Pod")];
        CHILD_KINDS
    }

    fn also_reads(&self) -> &'static [GroupVersionKind] {
        &[]
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
        rs_value: &Value,
        owned_pods: &[(ResourceKey, Value)],
    ) -> Result<ReconcileDelta, ControllerError> {
        // A `spec.replicas` that is not an integer fails this ReplicaSet
        // (an Event on it) with nothing created or evicted — never the
        // default's count.
        let desired = REPLICAS.read(rs_value)?.max(0) as usize;
        // A Terminating pod is already going: counting it would leave the
        // set a replica short until its finalizers clear, and evicting it
        // again would be a delete that changes nothing while a live pod the
        // count needs gone stays (I3).
        let live: Vec<&(ResourceKey, Value)> = live_children(owned_pods).collect();
        let observed = live.len();

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
        // Pods go in the RS's OWN namespace (where owned-pod gathering
        // looks), not the controller scope ns — same fix as deployment→RS.
        let pod_ns = rs_value.namespace().map_or_else(
            || self.namespace.as_deref().unwrap_or("default").to_string(),
            |c| c.to_owned(),
        );
        let mut commands = Vec::new();

        if observed < desired {
            // Create the difference at the LOWEST FREE indices — NOT
            // `observed + i`. The old `observed + i` basis CLOBBERED a
            // surviving pod when a MIDDLE pod was deleted: deleting `<rs>-0`
            // left `observed=1`, so the recreate targeted index 1 → the name
            // `<rs>-1` COLLIDED with the surviving `<rs>-1` pod, and the
            // `Put { expected: None }` overwrote it (resetting its
            // scheduler-assigned `spec.nodeName` from the template → the
            // kubelet then orphaned + removed its container → churn). Filling
            // the lowest free index instead recreates `<rs>-0` (the freed
            // slot), so the survivor `<rs>-1` is never touched. Names stay
            // deterministic for tests; self-heal after a middle delete works.
            // Every owned pod, Terminating ones included: a Terminating pod
            // still holds its name until its finalizers clear.
            let used = used_indices(rs_value.name(), owned_pods);
            let free = free_indices(&used, desired - observed);
            for idx in free {
                let Some((pod_name, pod)) =
                    Self::build_pod_from_template(rs_value, idx, owner_ref.clone())?
                else {
                    continue;
                };
                let pod_key = ResourceKey::namespaced("", "v1", "Pod", &pod_ns, &pod_name);
                commands.push(ResourceCommand::Put {
                    key: pod_key,
                    value: pod,
                    expected: None,
                    reason: Reason::Controller,
                });
            }
        } else {
            // observed > desired — evict the excess live pods by name order
            // (highest names first) for deterministic eviction.
            let to_delete = observed - desired;
            let mut live_sorted = live;
            live_sorted.sort_by(|a, b| a.0.name.cmp(&b.0.name));
            for (pod_key, _) in live_sorted.iter().rev().take(to_delete) {
                commands.push(ResourceCommand::delete(
                    (*pod_key).clone(),
                    Reason::Controller,
                ));
            }
        }

        Ok(ReconcileDelta::from_commands(commands))
    }

    fn compute_status(
        &self,
        _rs_value: &Value,
        owned_now: &[(ResourceKey, Value)],
        observed_generation: i64,
    ) -> Option<Value> {
        // Status computed from the owned pods AFTER the reconcile delta,
        // counting only the live ones (upstream `calculateStatus` over
        // `FilterActivePods`): a Terminating pod is not a replica.
        // readyReplicas counts Ready=True pods; availableReplicas ==
        // readyReplicas at M0.1 (no minReadySeconds); fullyLabeledReplicas
        // == replicas.
        let live: Vec<&(ResourceKey, Value)> = live_children(owned_now).collect();
        let replicas = i64::try_from(live.len()).unwrap_or(i64::MAX);
        let ready =
            i64::try_from(live.iter().filter(|(_, p)| pod_is_ready(p)).count()).unwrap_or(i64::MAX);
        Some(json!({
            "replicas": replicas,
            "readyReplicas": ready,
            "availableReplicas": ready,
            "fullyLabeledReplicas": replicas,
            "observedGeneration": observed_generation,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replicas_defaults_to_1() {
        let rs = json!({"metadata": {"name": "rs"}, "spec": {}});
        assert_eq!(REPLICAS.read(&rs), Ok(1));
    }

    #[test]
    fn replicas_reads_spec_field() {
        let rs = json!({"spec": {"replicas": 5}});
        assert_eq!(REPLICAS.read(&rs), Ok(5));
    }

    fn rs_owner() -> OwnerReference {
        OwnerReference {
            api_version: "apps/v1".into(),
            kind: "ReplicaSet".into(),
            name: "rs1".into(),
            uid: "uid-rs1".into(),
            controller: true,
            block_owner_deletion: true,
        }
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
        let (name, pod) = ReplicaSetController::build_pod_from_template(&rs, 0, rs_owner())
            .unwrap()
            .unwrap();
        assert_eq!(name, "rs1-0");
        assert_eq!(pod.get("kind").unwrap(), "Pod");
        assert_eq!(pod.get("apiVersion").unwrap(), "v1");
        assert_eq!(pod.get("metadata").unwrap().get("name").unwrap(), "rs1-0");
        // No RS namespace → the pod defaults to "default".
        assert_eq!(
            pod.get("metadata").unwrap().get("namespace").unwrap(),
            "default"
        );
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
    fn build_pod_from_template_inherits_the_replicasets_namespace() {
        // The Pod MUST land in the parent RS's namespace — not "default".
        // Regression test for the bug where an RS in ns `team-b` produced
        // pods in `default`, breaking the owned-pod gathering + scheduling.
        let rs = json!({
            "metadata": {"name": "rs1", "namespace": "team-b"},
            "spec": {"template": {
                "metadata": {"labels": {"app": "rs1"}},
                "spec": {"containers": [{"name": "c", "image": "img"}]}
            }}
        });
        let (name, pod) = ReplicaSetController::build_pod_from_template(&rs, 2, rs_owner())
            .unwrap()
            .unwrap();
        assert_eq!(name, "rs1-2");
        assert_eq!(
            pod.get("metadata").unwrap().get("namespace").unwrap(),
            "team-b"
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

    // ── collision-free index allocation (middle-delete self-heal) ─────────

    fn owned(rs: &str, idx: usize) -> (ResourceKey, Value) {
        let name = format!("{rs}-{idx}");
        (
            ResourceKey::namespaced("", "v1", "Pod", "default", &name),
            json!({"metadata": {"name": name}}),
        )
    }

    #[test]
    fn used_indices_extracts_suffix_numbers() {
        let pods = vec![owned("rs1", 0), owned("rs1", 2), owned("rs1", 5)];
        let used = used_indices(Some("rs1"), &pods);
        assert_eq!(used.into_iter().collect::<Vec<_>>(), vec![0, 2, 5]);
    }

    #[test]
    fn used_indices_ignores_foreign_names() {
        // A pod whose name isn't `<rs>-<n>` contributes nothing.
        let pods = vec![owned("rs1", 1), owned("other", 0)];
        let used = used_indices(Some("rs1"), &pods);
        assert_eq!(used.into_iter().collect::<Vec<_>>(), vec![1]);
    }

    #[test]
    fn free_indices_fills_freed_middle_slot_first() {
        // The bug scenario: pods -0 deleted, survivor -1 in use → recreate
        // must target the FREED index 0, NOT collide on 1.
        let used: std::collections::BTreeSet<usize> = [1usize].into_iter().collect();
        assert_eq!(free_indices(&used, 1), vec![0]);
    }

    #[test]
    fn free_indices_extends_past_high_water_mark() {
        // No freed slots → extend upward from the lowest unused index.
        let used: std::collections::BTreeSet<usize> = [0usize, 1].into_iter().collect();
        assert_eq!(free_indices(&used, 2), vec![2, 3]);
    }

    #[test]
    fn free_indices_mixed_freed_and_extend() {
        // Freed 0 + survivors {1,3} → fill 0, then 2, then extend to 4.
        let used: std::collections::BTreeSet<usize> = [1usize, 3].into_iter().collect();
        assert_eq!(free_indices(&used, 3), vec![0, 2, 4]);
    }

    #[test]
    fn middle_delete_recreates_freed_index_not_survivor() {
        // End-to-end of the fix: RS desired=2, only `rs1-1` survives (the `-0`
        // was deleted). The recreate must allocate index 0 (the freed slot),
        // never re-Put `rs1-1` (which would clobber the survivor's nodeName).
        let surviving = vec![owned("rs1", 1)];
        let used = used_indices(Some("rs1"), &surviving);
        let free = free_indices(&used, 2 - surviving.len());
        assert_eq!(
            free,
            vec![0],
            "must recreate the freed index 0, not collide on 1"
        );
    }
}
