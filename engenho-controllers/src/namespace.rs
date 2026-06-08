//! `NamespaceController` — namespace-cascade (Background) deletion.
//!
//! The Viggy/controllers-not-runbooks realization of Kubernetes'
//! namespace teardown: deleting a Namespace does NOT synchronously remove
//! its contents in-request. Instead the namespace goes `Terminating` (the
//! finalizer gate in `engenho-store::state::apply_delete` stamps a
//! `deletionTimestamp` because the namespace carries the `kubernetes`
//! finalizer) and THIS controller drives the background sweep:
//!
//!   1. list every namespaced object in the Terminating namespace
//!      (across the typed served namespaced kinds — [`RESOURCE_CATALOG`]
//!      rows with `namespaced == true`) and DELETE each;
//!   2. set `status.phase = "Terminating"` so kubectl shows the phase;
//!   3. once the namespace is EMPTY of namespaced objects, clear the
//!      `kubernetes` finalizer via a typed `Patch` — which (per the
//!      store's finalizer-release rule) empties `metadata.finalizers` on a
//!      deletionTimestamp-bearing object and so the namespace itself is
//!      GC-removed.
//!
//! ## Why a raw [`Controller`] (not [`OwnedChildrenReconciler`])
//!
//! Like [`crate::gc::GcController`], the cascade has NO single parent kind
//! and does a CROSS-KIND sweep — it is "out of family" for the
//! single-parent owned-children reconciler. It implements the raw
//! `Controller` trait directly, modeled on `GcController`.
//!
//! ## Finalizer-awareness all the way down
//!
//! Each child delete is a normal `ResourceCommand::Delete`. If a CHILD
//! object itself bears finalizers, the store's gate gives IT the
//! Terminating treatment (deletionTimestamp set, kept until its
//! finalizers clear) — so the cascade is finalizer-aware end to end. The
//! controller re-ticks (WatchDriver fallback) until the namespace is
//! actually empty.
//!
//! ## Scope (v1)
//!
//! Cascade covers core + built-in namespaced kinds from
//! [`RESOURCE_CATALOG`]. Dynamic CRD-served namespaced CR kinds are the
//! next extension (fold the CrdController's registered served set into the
//! enumeration); documented as TYPED-DEFERRED, not silently dropped.

use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand, ResourceOp},
    resource::ResourceKey,
};
use engenho_types::generated_v1_34::{RESOURCE_CATALOG, ResourceDescriptor};
use serde_json::Value;
use tracing::{debug, info};

use crate::controller::{Controller, ReconcileOutcome, ReconcileReport};
use crate::error::ControllerError;
use crate::status::write_status_cas;

/// The Kubernetes namespace finalizer — gates a Namespace's removal until
/// its contents are gone. Stamped at create (apiserver) + cleared here.
const KUBERNETES_FINALIZER: &str = "kubernetes";

/// The typed catalog-reflection surface: every served NAMESPACED kind,
/// from [`RESOURCE_CATALOG`] where `namespaced == true`. The cascade
/// iterates this mechanically — adding a namespaced kind to the catalog
/// extends the cascade with no controller edit.
#[must_use]
pub fn namespaced_kinds() -> impl Iterator<Item = &'static ResourceDescriptor> {
    RESOURCE_CATALOG.iter().filter(|d| d.namespaced)
}

pub struct NamespaceController {
    store: Arc<StoreMesh>,
    /// Namespace scope filter (empty/None = all). Mirrors the other
    /// controllers' `namespace` knob; the cascade itself is per-namespace
    /// (it sweeps the Terminating namespace's own contents).
    #[allow(dead_code)]
    namespace: Option<String>,
}

impl NamespaceController {
    #[must_use]
    pub fn new(store: Arc<StoreMesh>, namespace: Option<String>) -> Self {
        Self { store, namespace }
    }

    /// Read `metadata.finalizers` off a Namespace object.
    fn finalizers_of(ns: &Value) -> Vec<String> {
        ns.get("metadata")
            .and_then(|m| m.get("finalizers"))
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// `true` iff `ns` is Terminating (has `metadata.deletionTimestamp`).
    fn is_terminating(ns: &Value) -> bool {
        ns.get("metadata")
            .and_then(|m| m.get("deletionTimestamp"))
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty())
    }

    /// Sweep one Terminating namespace: delete every namespaced object in
    /// it, set `status.phase = Terminating`, and — when empty — clear the
    /// `kubernetes` finalizer (which removes the namespace via the store's
    /// finalizer-release rule). Returns the number of mutations made.
    async fn sweep(&self, ns_name: &str, ns_obj: &Value) -> Result<usize, ControllerError> {
        let mut changed = 0usize;
        // Freeze ONE boundary clock read for THIS sweep — threaded into
        // every child delete so a finalizer-bearing child gets a Terminating
        // deletionTimestamp (the store's finalizer gate stamps it from this
        // REPLICATED scalar, never its own clock). A controller is the right
        // place to read the clock (it's the boundary, not the deterministic
        // store-apply path). Without this, a child with its OWN finalizer
        // would hit the gate's "finalizer + no timestamp ⇒ NoOp" branch and
        // the cascade would never drain it.
        let deletion_ts = engenho_types::time::now_rfc3339_utc();

        // 1. status.phase = Terminating (idempotent via write_status_cas).
        let ns_key = ResourceKey::cluster_scoped("", "v1", "Namespace", ns_name);
        let desired_status = serde_json::json!({ "phase": "Terminating" });
        if write_status_cas(&self.store, &ns_key, ns_obj, &desired_status)
            .await?
            .changed()
        {
            changed += 1;
        }

        // 2. Delete every namespaced object in this namespace across all
        //    served namespaced kinds.
        let mut remaining = 0usize;
        for d in namespaced_kinds() {
            let objs = self
                .store
                .list(d.group, d.version, d.kind, Some(ns_name))
                .await;
            for (key, _value) in objs {
                remaining += 1;
                debug!(
                    namespace = ns_name,
                    child = %key.label(),
                    "cascade-deleting namespaced object"
                );
                // GC-reason delete carrying the frozen sweep timestamp. If
                // the child bears its OWN finalizers, the store gate stamps
                // its deletionTimestamp (Terminating) from this replicated
                // scalar and keeps it until its finalizers clear; the next
                // tick re-counts it as still-remaining (cascade is
                // finalizer-aware all the way down). A no-finalizer child is
                // removed immediately.
                let out = self
                    .store
                    .propose(ResourceCommand::delete_at(
                        key,
                        None,
                        Reason::GarbageCollector,
                        Some(deletion_ts.clone()),
                    ))
                    .await?;
                // Only a real removal / Terminating stamp counts as a change.
                if matches!(out.op, ResourceOp::Deleted | ResourceOp::DeletionPending) {
                    changed += 1;
                }
            }
        }

        // 3. If the namespace is now EMPTY of namespaced objects, clear the
        //    kubernetes finalizer so the namespace itself is removed. We use
        //    the LIVE listed-this-tick `remaining` count; a non-zero count
        //    means we re-tick (WatchDriver fallback) until drained.
        if remaining == 0 {
            let current = Self::finalizers_of(ns_obj);
            if current.iter().any(|f| f == KUBERNETES_FINALIZER) {
                let next: Vec<String> = current
                    .into_iter()
                    .filter(|f| f != KUBERNETES_FINALIZER)
                    .collect();
                // Typed merge patch on metadata.finalizers — empties the
                // kubernetes finalizer on a deletionTimestamp-bearing
                // object, which the store converts into a removal.
                let patch = serde_json::json!({ "metadata": { "finalizers": next } });
                info!(
                    namespace = ns_name,
                    "namespace empty; clearing kubernetes finalizer (removal follows)"
                );
                let out = self
                    .store
                    .propose(ResourceCommand::patch(
                        ns_key,
                        patch,
                        Reason::GarbageCollector,
                    ))
                    .await?;
                if matches!(out.op, ResourceOp::Patched | ResourceOp::Deleted) {
                    changed += 1;
                }
            }
        } else {
            debug!(
                namespace = ns_name,
                remaining, "namespace still has objects; will re-tick"
            );
        }

        Ok(changed)
    }
}

#[async_trait]
impl Controller for NamespaceController {
    fn name(&self) -> &'static str {
        "namespace"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let mut report = ReconcileReport::default();

        // Enumerate Namespaces (cluster-scoped).
        let namespaces = self.store.list("", "v1", "Namespace", None).await;
        report.objects_examined = namespaces.len();

        for (key, ns_obj) in namespaces {
            // Only act on Terminating namespaces that still hold the
            // kubernetes finalizer (the ones we own the teardown of).
            if !Self::is_terminating(&ns_obj) {
                continue;
            }
            if !Self::finalizers_of(&ns_obj)
                .iter()
                .any(|f| f == KUBERNETES_FINALIZER)
            {
                continue;
            }
            let ns_name = &key.name;
            report.objects_changed += self.sweep(ns_name, &ns_obj).await?;
        }

        // Done — the WatchDriver fallback tick re-runs until each
        // Terminating namespace drains (mirrors gc.rs).
        Ok(report.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_store::{InProcessRouter, default_config};
    use serde_json::json;
    use std::time::Duration;

    /// Build a single-node in-memory StoreMesh (the controller-test rig the
    /// other reconciler tests use — see tests/r9_replicaset_controller.rs).
    async fn test_store() -> Arc<StoreMesh> {
        let router = InProcessRouter::new();
        let cfg = default_config("controllers-namespace").unwrap();
        let store = Arc::new(
            StoreMesh::start(1, "in-process://1".into(), router, cfg)
                .await
                .unwrap(),
        );
        store.initialize_singleton().await.unwrap();
        assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
        store
    }

    async fn put(store: &StoreMesh, key: ResourceKey, value: Value) {
        store
            .propose(ResourceCommand::put(key, value, Reason::Operator))
            .await
            .expect("put");
    }

    #[test]
    fn namespaced_kinds_filters_to_namespaced_only() {
        // The catalog-reflection surface enumerates ONLY namespaced rows.
        let kinds: Vec<&str> = namespaced_kinds().map(|d| d.kind).collect();
        assert!(kinds.contains(&"ConfigMap"));
        assert!(kinds.contains(&"Deployment"));
        assert!(kinds.contains(&"Pod"));
        // Cluster-scoped kinds are excluded.
        assert!(!kinds.contains(&"Namespace"));
        assert!(!kinds.contains(&"Node"));
        assert!(!kinds.contains(&"ClusterRole"));
        // Every enumerated descriptor is genuinely namespaced.
        assert!(namespaced_kinds().all(|d| d.namespaced));
    }

    #[tokio::test]
    async fn cascade_deletes_children_then_clears_finalizer() {
        let store = test_store().await;
        let c = NamespaceController::new(store.clone(), None);

        // Seed a Terminating namespace WITH the kubernetes finalizer.
        let ns_key = ResourceKey::cluster_scoped("", "v1", "Namespace", "demo");
        put(
            &store,
            ns_key.clone(),
            json!({
                "kind": "Namespace",
                "apiVersion": "v1",
                "metadata": {
                    "name": "demo",
                    "finalizers": [KUBERNETES_FINALIZER],
                    "deletionTimestamp": "2026-06-08T00:00:00Z"
                },
                "status": { "phase": "Active" }
            }),
        )
        .await;
        // + 1 ConfigMap and 1 Deployment in it.
        put(
            &store,
            ResourceKey::namespaced("", "v1", "ConfigMap", "demo", "a"),
            json!({"kind": "ConfigMap", "metadata": {"name": "a", "namespace": "demo"}, "data": {"x": "y"}}),
        )
        .await;
        put(
            &store,
            ResourceKey::namespaced("apps", "v1", "Deployment", "demo", "d"),
            json!({"kind": "Deployment", "metadata": {"name": "d", "namespace": "demo"}, "spec": {"replicas": 1}}),
        )
        .await;

        // First tick: status.phase=Terminating (1) + 2 child deletes = 3
        // changes; the namespace is NOT yet empty THIS tick (children were
        // listed as present), so the finalizer is not cleared.
        let out = c.tick().await.unwrap();
        assert!(out.objects_changed >= 2, "both children deleted");
        // Children are gone.
        assert!(
            store
                .get(&ResourceKey::namespaced("", "v1", "ConfigMap", "demo", "a"))
                .await
                .is_none(),
            "configmap cascade-deleted"
        );
        assert!(
            store
                .get(&ResourceKey::namespaced("apps", "v1", "Deployment", "demo", "d"))
                .await
                .is_none(),
            "deployment cascade-deleted"
        );

        // Second tick: namespace now empty → clear the kubernetes
        // finalizer, which removes the namespace itself.
        let out2 = c.tick().await.unwrap();
        assert!(out2.objects_changed >= 1, "finalizer cleared");
        assert!(
            store.get(&ns_key).await.is_none(),
            "namespace removed once finalizer cleared"
        );
    }

    #[tokio::test]
    async fn cascade_is_finalizer_aware_for_children() {
        // A child bearing its OWN finalizer gets the Terminating treatment
        // (deletionTimestamp stamped, kept) on the first sweep — the cascade
        // threads a frozen timestamp so the store gate stamps it. The
        // namespace is NOT removed while the child is still present. Once the
        // child's finalizer clears, the next sweep drains it + removes the ns.
        let store = test_store().await;
        let c = NamespaceController::new(store.clone(), None);
        let ns_key = ResourceKey::cluster_scoped("", "v1", "Namespace", "fin");
        put(
            &store,
            ns_key.clone(),
            json!({
                "kind": "Namespace",
                "metadata": {
                    "name": "fin",
                    "finalizers": [KUBERNETES_FINALIZER],
                    "deletionTimestamp": "2026-06-08T00:00:00Z"
                }
            }),
        )
        .await;
        let child_key = ResourceKey::namespaced("", "v1", "ConfigMap", "fin", "held");
        put(
            &store,
            child_key.clone(),
            json!({
                "kind": "ConfigMap",
                "metadata": {"name": "held", "namespace": "fin", "finalizers": ["example.com/hold"]},
                "data": {"x": "y"}
            }),
        )
        .await;

        // First sweep: the child is given a deletionTimestamp (Terminating),
        // NOT removed; the namespace stays (child still present).
        c.tick().await.unwrap();
        let child = store.get(&child_key).await.expect("child still present (Terminating)");
        assert!(
            child
                .get("metadata")
                .and_then(|m| m.get("deletionTimestamp"))
                .and_then(|d| d.as_str())
                .is_some(),
            "finalizer-bearing child got a deletionTimestamp (Terminating)"
        );
        assert!(store.get(&ns_key).await.is_some(), "ns held while child present");

        // Clear the child's finalizer → it is removed by the store rule.
        store
            .propose(ResourceCommand::patch(
                child_key.clone(),
                json!({"metadata": {"finalizers": []}}),
                Reason::Operator,
            ))
            .await
            .unwrap();
        assert!(store.get(&child_key).await.is_none(), "child removed after finalizer clear");

        // Next sweep: namespace now empty → finalizer cleared → removed.
        c.tick().await.unwrap();
        assert!(store.get(&ns_key).await.is_none(), "ns removed once empty");
    }

    #[tokio::test]
    async fn non_terminating_namespace_untouched() {
        let store = test_store().await;
        let c = NamespaceController::new(store.clone(), None);
        let ns_key = ResourceKey::cluster_scoped("", "v1", "Namespace", "live");
        put(
            &store,
            ns_key.clone(),
            json!({
                "kind": "Namespace",
                "metadata": { "name": "live", "finalizers": [KUBERNETES_FINALIZER] },
                "status": { "phase": "Active" }
            }),
        )
        .await;
        let out = c.tick().await.unwrap();
        assert_eq!(out.objects_changed, 0, "active namespace untouched");
        assert!(store.get(&ns_key).await.is_some(), "still present");
    }
}
