//! `PvcProtectionController` — upstream's pvc-protection controller.
//!
//! ★ THE DEFECT. Nothing held a claim that a pod was using. `kubectl
//! delete pvc` removed it at once, running pod or not, and once the binder
//! learned to reclaim (W9) that was no longer only an orphaned reference:
//! the claim's volume went `Released` and, under the default `Delete`
//! policy, its directory was removed underneath the pod still writing to it.
//!
//! ★ THE SHAPE. The `kubernetes.io/pvc-protection` finalizer, upstream's
//! mechanism and upstream's rules (`pkg/controller/volume/pvcprotection`):
//!
//!   * a live claim (no `deletionTimestamp`) without the finalizer gets it
//!     (`NeedToAddFinalizer`). Upstream's admission plugin stamps it at
//!     create; this controller is the backstop for every claim that
//!     arrived without it, which on engenho today is all of them;
//!   * a Terminating claim that still carries it (`IsDeletionCandidate`)
//!     keeps it for as long as a pod uses the claim, and loses it — which
//!     lets the store finish the delete — once none does;
//!   * a Terminating claim without it is someone else's to hold, and is
//!     never protected again: a finalizer is not added to an object whose
//!     deletion has started.
//!
//! "Uses" is upstream's `podUsesPVC`: a pod in the claim's namespace that
//! is SCHEDULED (the kubelet only ever sees scheduled pods, and a kubelet
//! does not start a pod whose claim is being deleted) and names the claim
//! through a `persistentVolumeClaim` volume, or through a generic
//! ephemeral volume whose claim `<pod>-<volume>` the pod controls, unless
//! that pod is already shut down (Terminating with a zero grace period).
//! A pod's phase does not matter: a Completed pod still holds its claim.
//!
//! "Unused" is decided twice, as upstream decides it twice (informer, then
//! a live apiserver list): the tick's pod list, and a fresh list of the
//! claim's namespace just before the release, so a pod created since the
//! tick began still holds the claim.
//!
//! Every write is a merge patch of `metadata.finalizers` at the revision
//! the claim was read at, so a concurrent writer's finalizer, or the
//! binder's bind, is never overwritten; a conflict is retried on the next
//! wake with the claim as it now stands.
//!
//! Tier-honest: protection is a controller, so there is a window between a
//! claim's create and this controller's first pass in which a delete is not
//! held. Upstream closes it with an admission plugin that stamps the
//! finalizer at create; engenho's apiserver does not yet.

use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand},
    resource::ResourceKey,
};
use serde_json::{Value, json};
use tracing::debug;

use crate::controller::{Controller, ReconcileOutcome};
use crate::effect::Effect;
use crate::error::ControllerError;
use crate::event_recorder::Reason as EventReason;
use crate::meta::{ObjectMeta, array_mut};
use crate::owner::is_owned_by;
use crate::reads::{DeclaresReads, Reads, gvk};
use crate::status::resource_version_of;
use crate::sweep::{ObjectOutcome, Sweep, impl_sweep_event_sink};

/// The finalizer that holds a claim while a pod uses it.
pub const PVC_PROTECTION_FINALIZER: &str = "kubernetes.io/pvc-protection";

/// Upstream's `source.component` for this controller.
const COMPONENT: &str = "pvc-protection-controller";

/// Where a claim's finalizers live.
const FINALIZERS: &[&str] = &["metadata", "finalizers"];

/// The report note for a Terminating claim a pod still uses.
const IN_USE: &str = "PVC is Terminating and a scheduled pod still uses it; the pvc-protection \
     finalizer holds it until no pod does";

/// The report note for a claim whose finalizers cannot be written at a
/// known revision.
const NO_REVISION: &str = "PVC carries no resourceVersion, so its finalizers cannot be written at \
     the revision it was read at; left for the next pass";

/// The report note for a claim listed without a namespace.
const NO_NAMESPACE: &str = "PVC has no namespace, so no pod can be asked whether it uses it; \
     left as it is";

/// What protection owes one claim, from its deletion state and whether it
/// carries the finalizer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Protection {
    /// Live and not protected yet: add the finalizer.
    Protect,
    /// Terminating and held by the finalizer: release it once no pod uses
    /// the claim.
    ReleaseWhenUnused,
    /// Live and protected, or Terminating without the finalizer.
    Settled,
}

impl Protection {
    fn of(claim: &Value) -> Self {
        match (
            claim.is_terminating(),
            claim.has_finalizer(PVC_PROTECTION_FINALIZER),
        ) {
            (false, false) => Self::Protect,
            (true, true) => Self::ReleaseWhenUnused,
            (false, true) | (true, false) => Self::Settled,
        }
    }
}

/// The one edit this controller makes to a claim's finalizers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FinalizerEdit {
    Add,
    Remove,
}

/// Does `pod` use the claim named `claim_name` (whose object is `claim`)?
///
/// Upstream's `podUsesPVC`; see the module docs. The caller has already
/// narrowed `pod` to the claim's namespace.
fn pod_uses_claim(pod: &Value, claim_name: &str, claim: &Value) -> bool {
    let spec = pod.get("spec");
    let scheduled = spec
        .and_then(|s| s.get("nodeName"))
        .and_then(Value::as_str)
        .is_some_and(|node| !node.is_empty());
    if !scheduled {
        return false;
    }
    let Some(volumes) = spec
        .and_then(|s| s.get("volumes"))
        .and_then(Value::as_array)
    else {
        return false;
    };
    volumes.iter().any(|volume| {
        names_claim(volume, claim_name)
            || (!is_shut_down(pod) && ephemeral_claim_is(pod, volume, claim_name, claim))
    })
}

/// A `persistentVolumeClaim` volume naming `claim_name`.
fn names_claim(volume: &Value, claim_name: &str) -> bool {
    volume
        .get("persistentVolumeClaim")
        .and_then(|p| p.get("claimName"))
        .and_then(Value::as_str)
        == Some(claim_name)
}

/// A generic ephemeral volume whose claim is `claim_name`: upstream names
/// that claim `<pod>-<volume>` and counts it only when the pod controls it
/// (`ephemeral.VolumeIsForPod`), so a claim that merely shares the name is
/// not held by this pod.
fn ephemeral_claim_is(pod: &Value, volume: &Value, claim_name: &str, claim: &Value) -> bool {
    if volume.get("ephemeral").is_none_or(Value::is_null) {
        return false;
    }
    let (Some(pod_name), Some(volume_name)) =
        (pod.name(), volume.get("name").and_then(Value::as_str))
    else {
        return false;
    };
    let named = claim_name
        .strip_prefix(pod_name)
        .and_then(|rest| rest.strip_prefix('-'))
        == Some(volume_name);
    named && pod.uid().is_some_and(|uid| is_owned_by(claim, uid))
}

/// Upstream's `podIsShutDown`: a pod Terminating with a zero grace period
/// has been through the kubelet (or was force-deleted) and only waits for
/// garbage collection.
fn is_shut_down(pod: &Value) -> bool {
    pod.is_terminating()
        && pod
            .get("metadata")
            .and_then(|m| m.get("deletionGracePeriodSeconds"))
            .and_then(Value::as_i64)
            == Some(0)
}

/// Does any pod in `pods` that lives in `namespace` use the claim?
fn in_use(pods: &[(ResourceKey, Value)], namespace: &str, claim_name: &str, claim: &Value) -> bool {
    pods.iter()
        .filter(|(key, _)| key.namespace.as_deref() == Some(namespace))
        .any(|(_, pod)| pod_uses_claim(pod, claim_name, claim))
}

/// The pvc-protection controller.
pub struct PvcProtectionController {
    store: Arc<StoreMesh>,
    namespace: Option<String>,
    /// Per-claim isolation: one claim's failure costs only that claim.
    sweep: Sweep,
}

impl_sweep_event_sink!(PvcProtectionController);

impl PvcProtectionController {
    /// A controller over the claims in `namespace` (every namespace when
    /// `None`).
    #[must_use]
    pub fn new(store: Arc<StoreMesh>, namespace: Option<String>) -> Self {
        Self {
            store,
            namespace,
            sweep: Sweep::new(COMPONENT, EventReason::FailedUpdate),
        }
    }

    /// Reconcile one claim against the pods listed this tick.
    async fn reconcile_claim(
        &self,
        key: &ResourceKey,
        claim: &Value,
        pods: &[(ResourceKey, Value)],
    ) -> Result<ObjectOutcome, ControllerError> {
        match Protection::of(claim) {
            Protection::Settled => Ok(ObjectOutcome::Unchanged),
            Protection::Protect => self.edit_finalizers(key, claim, FinalizerEdit::Add).await,
            Protection::ReleaseWhenUnused => {
                let Some(namespace) = key.namespace.as_deref() else {
                    return Ok(ObjectOutcome::skipped_because(NO_NAMESPACE));
                };
                if in_use(pods, namespace, &key.name, claim) {
                    return Ok(ObjectOutcome::skipped_because(IN_USE));
                }
                // The second look: a pod created since the tick's list was
                // taken holds the claim just the same.
                let fresh = self.store.list("", "v1", "Pod", Some(namespace)).await;
                if in_use(&fresh, namespace, &key.name, claim) {
                    return Ok(ObjectOutcome::skipped_because(IN_USE));
                }
                debug!(pvc = %key.label(), "no pod uses the claim; releasing pvc-protection");
                self.edit_finalizers(key, claim, FinalizerEdit::Remove)
                    .await
            }
        }
    }

    /// Add or remove the protection finalizer, keeping every other entry,
    /// at the revision the claim was read at.
    async fn edit_finalizers(
        &self,
        key: &ResourceKey,
        claim: &Value,
        edit: FinalizerEdit,
    ) -> Result<ObjectOutcome, ControllerError> {
        let Some(revision) = resource_version_of(claim) else {
            return Ok(ObjectOutcome::skipped_because(NO_REVISION));
        };
        let mut edited = claim.clone();
        let finalizers = array_mut(&mut edited, FINALIZERS)?;
        match edit {
            FinalizerEdit::Add => finalizers.push(Value::from(PVC_PROTECTION_FINALIZER)),
            FinalizerEdit::Remove => {
                finalizers.retain(|f| f.as_str() != Some(PVC_PROTECTION_FINALIZER));
            }
        }
        let patch = json!({ "metadata": { "finalizers": std::mem::take(finalizers) } });
        let applied = self
            .store
            .propose(ResourceCommand::patch_cas(
                key.clone(),
                patch,
                Some(revision),
                Reason::Controller,
            ))
            .await?;
        Ok(ObjectOutcome::from(Effect::of(applied.op)))
    }
}

/// Claims, and the pods that use them: a pod's deletion is what releases a
/// Terminating claim, so it has to wake the controller.
impl DeclaresReads for PvcProtectionController {
    fn reads(&self) -> Reads {
        Reads::of(&[gvk("", "v1", "PersistentVolumeClaim"), gvk("", "v1", "Pod")])
    }
}

#[async_trait]
impl Controller for PvcProtectionController {
    fn name(&self) -> &'static str {
        "pvc-protection"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let namespace = self.namespace.as_deref();
        let claims = self
            .store
            .list("", "v1", "PersistentVolumeClaim", namespace)
            .await;
        // Pods matter only to a claim waiting on its release; a tick with
        // none does not read them.
        let pods = if claims
            .iter()
            .any(|(_, c)| Protection::of(c) == Protection::ReleaseWhenUnused)
        {
            self.store.list("", "v1", "Pod", namespace).await
        } else {
            Vec::new()
        };
        let (this, pods) = (self, &pods);
        let report = self
            .sweep
            .run(&claims, |key, claim| async move {
                ObjectOutcome::settle(this.reconcile_claim(key, claim, pods).await)
            })
            .await?;
        Ok(report.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    async fn live_store() -> Arc<StoreMesh> {
        use engenho_store::{InProcessRouter, default_config};
        let router = InProcessRouter::new();
        let cfg = default_config("controllers-pvc-protection").unwrap();
        let store = Arc::new(
            StoreMesh::start(1, "in-process://1".into(), router, cfg)
                .await
                .unwrap(),
        );
        store.initialize_singleton().await.unwrap();
        assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
        store
    }

    fn claim_key(ns: &str, name: &str) -> ResourceKey {
        ResourceKey::namespaced("", "v1", "PersistentVolumeClaim", ns, name)
    }

    fn pod_key(ns: &str, name: &str) -> ResourceKey {
        ResourceKey::namespaced("", "v1", "Pod", ns, name)
    }

    async fn put(store: &StoreMesh, key: ResourceKey, value: Value) {
        store
            .propose(ResourceCommand::put(key, value, Reason::Operator))
            .await
            .unwrap();
    }

    async fn delete(store: &StoreMesh, key: ResourceKey) {
        store
            .propose(ResourceCommand::delete(key, Reason::Operator))
            .await
            .unwrap();
    }

    async fn put_claim(store: &StoreMesh, ns: &str, name: &str, finalizers: Value) {
        put(
            store,
            claim_key(ns, name),
            json!({"apiVersion": "v1", "kind": "PersistentVolumeClaim",
                   "metadata": {"name": name, "namespace": ns, "uid": (["uid-", name].concat()),
                                "finalizers": finalizers},
                   "spec": {"accessModes": ["ReadWriteOnce"],
                            "resources": {"requests": {"storage": "1Gi"}}}}),
        )
        .await;
    }

    /// A pod in `ns` mounting claim `claim`, on `node` (`None`: unscheduled).
    fn pod_mounting(ns: &str, name: &str, claim: &str, node: Option<&str>) -> Value {
        let mut spec = json!({"volumes": [
            {"name": "data", "persistentVolumeClaim": {"claimName": claim}}
        ]});
        if let Some(node) = node {
            spec["nodeName"] = json!(node);
        }
        json!({"apiVersion": "v1", "kind": "Pod",
               "metadata": {"name": name, "namespace": ns, "uid": (["uid-", name].concat())},
               "spec": spec})
    }

    fn controller(store: &Arc<StoreMesh>) -> PvcProtectionController {
        PvcProtectionController::new(store.clone(), None)
    }

    async fn finalizers(store: &StoreMesh, ns: &str, name: &str) -> Option<Value> {
        store
            .get(&claim_key(ns, name))
            .await
            .map(|c| c["metadata"]["finalizers"].clone())
    }

    // ── the pure predicates ─────────────────────────────────────────

    #[test]
    fn protection_follows_deletion_state_and_the_finalizer() {
        let claim = |ts: Option<&str>, fin: Value| json!({"metadata": {"deletionTimestamp": ts, "finalizers": fin}});
        let ts = Some("2026-09-19T00:00:00Z");
        assert_eq!(Protection::of(&claim(None, json!([]))), Protection::Protect);
        assert_eq!(
            Protection::of(&claim(None, json!(["example.com/x"]))),
            Protection::Protect
        );
        assert_eq!(
            Protection::of(&claim(None, json!([PVC_PROTECTION_FINALIZER]))),
            Protection::Settled
        );
        assert_eq!(
            Protection::of(&claim(ts, json!([PVC_PROTECTION_FINALIZER]))),
            Protection::ReleaseWhenUnused
        );
        assert_eq!(
            Protection::of(&claim(ts, json!(["example.com/x"]))),
            Protection::Settled,
            "a deletion that has started is never protected after the fact"
        );
    }

    #[test]
    fn only_a_scheduled_pod_naming_the_claim_uses_it() {
        let claim = json!({"metadata": {"name": "data"}});
        let scheduled = pod_mounting("ns1", "web", "data", Some("node-a"));
        assert!(pod_uses_claim(&scheduled, "data", &claim));
        assert!(!pod_uses_claim(&scheduled, "other", &claim));
        let unscheduled = pod_mounting("ns1", "web", "data", None);
        assert!(!pod_uses_claim(&unscheduled, "data", &claim));
        let mut empty_node = scheduled.clone();
        empty_node["spec"]["nodeName"] = json!("");
        assert!(!pod_uses_claim(&empty_node, "data", &claim));
        // A completed pod still holds its claim: phase is not consulted.
        let mut completed = scheduled;
        completed["status"] = json!({"phase": "Succeeded"});
        assert!(pod_uses_claim(&completed, "data", &claim));
    }

    #[test]
    fn an_ephemeral_claim_is_used_only_by_the_pod_that_controls_it() {
        let pod = json!({"metadata": {"name": "web", "uid": "uid-web"},
                         "spec": {"nodeName": "node-a",
                                  "volumes": [{"name": "scratch", "ephemeral": {
                                      "volumeClaimTemplate": {"spec": {}}}}]}});
        let owned = json!({"metadata": {"name": "web-scratch", "ownerReferences": [
            {"apiVersion": "v1", "kind": "Pod", "name": "web", "uid": "uid-web",
             "controller": true}]}});
        let foreign = json!({"metadata": {"name": "web-scratch", "ownerReferences": [
            {"apiVersion": "v1", "kind": "Pod", "name": "web", "uid": "uid-other",
             "controller": true}]}});
        assert!(pod_uses_claim(&pod, "web-scratch", &owned));
        assert!(
            !pod_uses_claim(&pod, "web-scratch", &foreign),
            "a claim of the same name the pod does not control is not its"
        );
        assert!(!pod_uses_claim(&pod, "web-other", &owned));

        let mut shut_down = pod.clone();
        shut_down["metadata"]["deletionTimestamp"] = json!("2026-09-19T00:00:00Z");
        shut_down["metadata"]["deletionGracePeriodSeconds"] = json!(0);
        assert!(
            !pod_uses_claim(&shut_down, "web-scratch", &owned),
            "a pod the kubelet is done with no longer holds its ephemeral claim"
        );
        let mut terminating = pod;
        terminating["metadata"]["deletionTimestamp"] = json!("2026-09-19T00:00:00Z");
        terminating["metadata"]["deletionGracePeriodSeconds"] = json!(30);
        assert!(pod_uses_claim(&terminating, "web-scratch", &owned));
    }

    // ── against a live store ────────────────────────────────────────

    /// A live claim gains the finalizer, and keeps every finalizer it had.
    #[tokio::test]
    async fn a_live_claim_gains_the_finalizer_beside_its_own() {
        let store = live_store().await;
        put_claim(&store, "ns1", "bare", Value::Null).await;
        put_claim(&store, "ns1", "held", json!(["example.com/hold"])).await;

        let outcome = controller(&store).tick().await.unwrap();

        assert_eq!(
            finalizers(&store, "ns1", "bare").await,
            Some(json!([PVC_PROTECTION_FINALIZER]))
        );
        assert_eq!(
            finalizers(&store, "ns1", "held").await,
            Some(json!(["example.com/hold", PVC_PROTECTION_FINALIZER]))
        );
        assert_eq!(outcome.sweep.unwrap().changed(), 2);

        let again = controller(&store).tick().await.unwrap();
        let sweep = again.sweep.unwrap();
        assert_eq!(
            (sweep.changed(), sweep.unchanged()),
            (0, 2),
            "a protected claim is left alone"
        );
    }

    /// ★ THE DEFECT. A claim a scheduled pod uses is deleted: it must stay,
    /// Terminating, for as long as the pod exists, and go once it does not.
    #[tokio::test]
    async fn a_claim_in_use_outlives_its_delete_until_the_pod_is_gone() {
        let store = live_store().await;
        put_claim(&store, "ns1", "data", Value::Null).await;
        put(
            &store,
            pod_key("ns1", "web"),
            pod_mounting("ns1", "web", "data", Some("node-a")),
        )
        .await;
        let c = controller(&store);
        c.tick().await.unwrap();

        delete(&store, claim_key("ns1", "data")).await;
        let held = c.tick().await.unwrap();

        let claim = store
            .get(&claim_key("ns1", "data"))
            .await
            .expect("a claim in use survives its delete");
        assert!(claim.is_terminating());
        assert!(claim.has_finalizer(PVC_PROTECTION_FINALIZER));
        assert_eq!(held.sweep.unwrap().note(), Some(IN_USE));

        delete(&store, pod_key("ns1", "web")).await;
        c.tick().await.unwrap();

        assert!(
            store.get(&claim_key("ns1", "data")).await.is_none(),
            "the claim goes once no pod uses it"
        );
    }

    /// Upstream counts only scheduled pods in the claim's own namespace.
    #[tokio::test]
    async fn an_unscheduled_or_foreign_pod_does_not_hold_the_claim() {
        let store = live_store().await;
        put_claim(&store, "ns1", "data", json!([PVC_PROTECTION_FINALIZER])).await;
        put(
            &store,
            pod_key("ns1", "pending"),
            pod_mounting("ns1", "pending", "data", None),
        )
        .await;
        put(
            &store,
            pod_key("ns2", "elsewhere"),
            pod_mounting("ns2", "elsewhere", "data", Some("node-a")),
        )
        .await;
        delete(&store, claim_key("ns1", "data")).await;
        assert!(store.get(&claim_key("ns1", "data")).await.is_some());

        controller(&store).tick().await.unwrap();

        assert!(store.get(&claim_key("ns1", "data")).await.is_none());
    }

    /// Releasing protection removes only this controller's finalizer; a
    /// claim someone else still holds stays Terminating.
    #[tokio::test]
    async fn release_keeps_every_other_finalizer() {
        let store = live_store().await;
        put_claim(
            &store,
            "ns1",
            "data",
            json!(["example.com/hold", PVC_PROTECTION_FINALIZER]),
        )
        .await;
        delete(&store, claim_key("ns1", "data")).await;

        controller(&store).tick().await.unwrap();

        let claim = store.get(&claim_key("ns1", "data")).await.unwrap();
        assert!(claim.is_terminating());
        assert_eq!(claim["metadata"]["finalizers"], json!(["example.com/hold"]));
    }

    /// A deletion that started before the claim was protected is not
    /// protected after the fact.
    #[tokio::test]
    async fn a_terminating_claim_is_never_given_the_finalizer() {
        let store = live_store().await;
        put_claim(&store, "ns1", "data", json!(["example.com/hold"])).await;
        put(
            &store,
            pod_key("ns1", "web"),
            pod_mounting("ns1", "web", "data", Some("node-a")),
        )
        .await;
        delete(&store, claim_key("ns1", "data")).await;

        controller(&store).tick().await.unwrap();

        assert_eq!(
            finalizers(&store, "ns1", "data").await,
            Some(json!(["example.com/hold"]))
        );
    }

    /// The finalizer write is at the revision read: a claim that moved
    /// since is not overwritten with the stale copy.
    #[tokio::test]
    async fn a_stale_claim_is_not_written() {
        let store = live_store().await;
        put_claim(&store, "ns1", "data", Value::Null).await;
        let stale = store.get(&claim_key("ns1", "data")).await.unwrap();
        put_claim(&store, "ns1", "data", json!(["example.com/late"])).await;

        let outcome = controller(&store)
            .edit_finalizers(&claim_key("ns1", "data"), &stale, FinalizerEdit::Add)
            .await
            .unwrap();

        assert!(
            matches!(outcome, ObjectOutcome::Skipped { .. }),
            "{outcome:?}"
        );
        assert_eq!(
            finalizers(&store, "ns1", "data").await,
            Some(json!(["example.com/late"]))
        );
    }
}
