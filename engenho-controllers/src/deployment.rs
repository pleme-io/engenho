//! `DeploymentController` — reconciles Deployments into
//! ReplicaSets.
//!
//! K8s rule:
//!   * each Deployment owns 1..N ReplicaSets via ownerReferences
//!   * the CURRENT `ReplicaSet` is the one whose `spec.template` EQUALS the
//!     Deployment's, ignoring the `pod-template-hash` label (upstream's
//!     `EqualIgnoreHash`, see [`crate::pod_template`]) — the hash only
//!     names a new one
//!   * older ReplicaSets are kept around at `replicas=0` so
//!     `kubectl rollout undo` still works (revision history)
//!
//! R9.5 implementation (this file):
//!   1. For each Deployment, normalize its template.
//!   2. Find owned ReplicaSets (via uid).
//!   3. Pick the current one: an owned RS running an equal template
//!      ([`current_replicaset`]). If there is none, create one, named by the
//!      template's hash.
//!   4. Scale the current RS to `Deployment.spec.replicas`.
//!   5. Scale every other owned RS to 0 (revision history retained).
//!
//! Skips: status updates, paused rollouts, partial-rollout
//! strategies — those are R9.5b. The substrate's good enough to
//! prove the controller pattern compounds.

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
use crate::meta::{DefaultedInt, ObjectMeta, REPLICAS, ShapeError, warn_unreadable};
use crate::owned_children::{ChildKind, OwnedChildrenReconciler, ParentGvk, ReconcileDelta};
use crate::owner::{owner_ref_for, set_owner_reference};
use crate::pod_template::{NormalizedTemplate, POD_TEMPLATE_HASH_LABEL, TemplateHash};
use crate::sweep::{Sweep, impl_sweep_event_sink};

/// The counts a `ReplicaSet`'s controller writes into its status, which a
/// Deployment's status sums. Absent is 0: a `ReplicaSet` the controller
/// has not reported on yet has observed no replicas.
const RS_STATUS_REPLICAS: DefaultedInt = DefaultedInt::new(&["status", "replicas"], 0);
const RS_STATUS_READY: DefaultedInt = DefaultedInt::new(&["status", "readyReplicas"], 0);
const RS_STATUS_AVAILABLE: DefaultedInt = DefaultedInt::new(&["status", "availableReplicas"], 0);

pub struct DeploymentController {
    store: Arc<StoreMesh>,
    namespace: Option<String>,
    /// Per-Deployment isolation (`ReplicaSetCreateError` on an Item
    /// failure). The `ReplicaSet` body is built fresh, so the owner-reference
    /// write cannot meet a wrong shape today; the failure path is the
    /// family's, shared through the blanket.
    sweep: Sweep,
}

impl_sweep_event_sink!(DeploymentController);

impl DeploymentController {
    #[must_use]
    pub fn new(store: Arc<StoreMesh>, namespace: Option<String>) -> Self {
        Self {
            store,
            namespace,
            sweep: Sweep::new("deployment-controller", EventReason::ReplicaSetCreateError),
        }
    }

    /// Build a ReplicaSet object from a Deployment + chosen
    /// template hash. The RS's `spec.template` is the
    /// Deployment's, and its `spec.replicas` is `replicas` — the
    /// Deployment's count, already read (a malformed one never gets
    /// here); the RS gets the deployment's labels +
    /// a `pod-template-hash` label for kubectl-rollout-friendly
    /// debugging.
    fn build_replicaset_from(
        d: &Value,
        hash: TemplateHash,
        replicas: i64,
    ) -> Option<(String, Value)> {
        let d_name = d.name()?;
        // The child ReplicaSet lives in the SAME namespace as its parent
        // Deployment — never the controller's scope namespace (an
        // all-namespace controller has none). A namespaced Deployment with
        // no metadata.namespace is impossible past admission, but default
        // defensively so the key + the object can never disagree.
        let d_namespace = d
            .namespace()
            .map_or_else(|| "default".to_string(), |c| c.to_owned());
        let template = d.get("spec").and_then(|s| s.get("template"))?.clone();
        let rs_name = format!("{d_name}-{hash}");
        let value = json!({
            "kind": "ReplicaSet",
            "apiVersion": "apps/v1",
            "metadata": {
                "name": rs_name,
                "namespace": d_namespace,
                "labels": {
                    "app.kubernetes.io/managed-by": "engenho-deployment-controller",
                    POD_TEMPLATE_HASH_LABEL: hash.to_string()
                }
            },
            "spec": {
                "replicas": replicas,
                "selector": d.get("spec").and_then(|s| s.get("selector")).cloned(),
                "template": template
            }
        });
        Some((rs_name, value))
    }

    /// The `(replicas, ready, available)` a `ReplicaSet`'s status reports
    /// — Deployment status sums these across its current-template RS(es).
    ///
    /// # Errors
    ///
    /// [`ShapeError::NotAnInteger`] when one of them is not an integer.
    fn rs_status_counts(rs: &Value) -> Result<(i64, i64, i64), ShapeError> {
        Ok((
            RS_STATUS_REPLICAS.read(rs)?,
            RS_STATUS_READY.read(rs)?,
            RS_STATUS_AVAILABLE.read(rs)?,
        ))
    }
}

#[async_trait]
impl OwnedChildrenReconciler for DeploymentController {
    fn name(&self) -> &'static str {
        "deployment"
    }

    fn parent_gvk(&self) -> ParentGvk {
        ParentGvk::new("apps", "v1", "Deployment", "apps/v1")
    }

    fn child_kinds(&self) -> &'static [ChildKind] {
        const CHILD_KINDS: &[ChildKind] = &[ChildKind::new("apps", "v1", "ReplicaSet")];
        CHILD_KINDS
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
        d_value: &Value,
        owned_rs: &[(ResourceKey, Value)],
    ) -> Result<ReconcileDelta, ControllerError> {
        // A `spec.replicas` that is not an integer fails this Deployment
        // (an Event on it) with nothing created or scaled — never scaled
        // to the default's 1.
        let desired_replicas = REPLICAS.read(d_value)?;
        // No template / owner-ref → nothing to do this tick (the parent
        // is freshly minted; the blanket already skipped no-uid parents).
        let Some(template) = NormalizedTemplate::of_spec_template(d_value) else {
            return Ok(ReconcileDelta::none());
        };
        let Some(owner_ref) = owner_ref_for(d_value, "apps/v1", "Deployment") else {
            return Ok(ReconcileDelta::none());
        };

        let ns = self.namespace.as_deref();
        let mut commands = Vec::new();
        let current = current_replicaset(&template, owned_rs);
        let current_key = current.map(|(k, _)| k);

        // Scale every owned RS but the current one to 0 (revision history
        // retained at replicas=0) — including a second RS running an equal
        // template, whose pods would otherwise run on top of the current
        // one's. An RS's absent count is the API's 1 — the count its own
        // controller runs — so an absent one is still scaled down. An RS
        // whose count is malformed is left alone: its controller does
        // nothing with it either, and says so on it.
        for (rs_key, rs_value) in owned_rs {
            if current_key == Some(rs_key) {
                continue;
            }
            let current_replicas = match REPLICAS.read(rs_value) {
                Ok(n) => n,
                Err(e) => {
                    warn_unreadable("deployment", &rs_key.label(), &e);
                    continue;
                }
            };
            if current_replicas != 0 {
                commands.push(ResourceCommand::patch(
                    rs_key.clone(),
                    json!({"spec": {"replicas": 0}}),
                    Reason::Controller,
                ));
            }
        }

        // Ensure the current-template RS exists + has the right replica
        // count.
        match current {
            Some((rs_key, rs_value)) => match REPLICAS.read(rs_value) {
                Ok(current_replicas) => {
                    if current_replicas != desired_replicas {
                        commands.push(ResourceCommand::patch(
                            rs_key.clone(),
                            json!({"spec": {"replicas": desired_replicas}}),
                            Reason::Controller,
                        ));
                    }
                }
                Err(e) => warn_unreadable("deployment", &rs_key.label(), &e),
            },
            None => {
                // The hash only NAMES the new RS. `None` is a template that
                // cannot be serialized, which a `Value` never is.
                if let Some((rs_name, mut rs_value)) = template
                    .naming_hash()
                    .and_then(|hash| Self::build_replicaset_from(d_value, hash, desired_replicas))
                {
                    set_owner_reference(&mut rs_value, owner_ref.clone())?;
                    // Key the RS under the PARENT Deployment's namespace —
                    // the same namespace the blanket gathers `owned_rs` from
                    // (by owner-ref). Using the controller's scope namespace
                    // (`ns`, None→"default") keyed the RS where the owned-RS
                    // query never looks → the controller never saw the RS it
                    // created → recreate-every-tick hot loop + status thrash.
                    let rs_ns = d_value
                        .namespace()
                        .map_or_else(|| ns.unwrap_or("default").to_string(), |c| c.to_owned());
                    let rs_key =
                        ResourceKey::namespaced("apps", "v1", "ReplicaSet", &rs_ns, &rs_name);
                    commands.push(ResourceCommand::Put {
                        key: rs_key,
                        value: rs_value,
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
        d_value: &Value,
        owned_rs_after: &[(ResourceKey, Value)],
        observed_generation: i64,
    ) -> Option<Value> {
        // Aggregate over CURRENT-template owned RS(es) — read the status
        // the RS controller wrote. `replicas`/`ready`/`available` SUM
        // across every current-template owned RS; `updatedReplicas` ==
        // current-template RS replica count (single template at M0.1, so
        // it equals `replicas`). Source of truth = the live RS status. A
        // count that cannot be read writes no status this pass: a sum
        // with a guessed term would be reported as observed.
        let template = NormalizedTemplate::of_spec_template(d_value)?;
        let (mut replicas, mut ready, mut available) = (0_i64, 0_i64, 0_i64);
        for (rs_key, rs) in owned_rs_after
            .iter()
            .filter(|(_, r)| runs_template(r, &template))
        {
            match Self::rs_status_counts(rs) {
                Ok((r, rd, av)) => {
                    replicas = replicas.saturating_add(r);
                    ready = ready.saturating_add(rd);
                    available = available.saturating_add(av);
                }
                Err(e) => {
                    warn_unreadable("deployment", &rs_key.label(), &e);
                    return None;
                }
            }
        }
        let updated = replicas;
        Some(json!({
            "replicas": replicas,
            "readyReplicas": ready,
            "availableReplicas": available,
            "updatedReplicas": updated,
            "observedGeneration": observed_generation,
        }))
    }
}

/// Whether `rs` runs `template`: its own `spec.template`, normalized,
/// equals it. The `pod-template-hash` label on either side is ignored, so
/// this holds whatever hash the RS was named by.
fn runs_template(rs: &Value, template: &NormalizedTemplate) -> bool {
    NormalizedTemplate::of_spec_template(rs).as_ref() == Some(template)
}

/// The owned `ReplicaSet` that is a Deployment's current one — upstream's
/// `FindNewReplicaSet`: an RS whose `spec.template` equals `template`,
/// ignoring the `pod-template-hash` label. `None` when no owned RS runs it,
/// the one case in which a new RS is created.
///
/// When more than one runs it (two RSes whose templates differed only in
/// bytes, both created under the old hash matcher), the one holding the
/// most replicas is current, and on a tie the one whose name sorts first.
/// Upstream picks the OLDEST instead; engenho has no gradual rollout, so
/// moving the pods from one RS to another running the same template would
/// be pure churn, and the RS already holding them is the one to keep. A
/// count that cannot be read ranks below every readable one.
#[must_use]
pub fn current_replicaset<'a>(
    template: &NormalizedTemplate,
    owned_rs: &'a [(ResourceKey, Value)],
) -> Option<&'a (ResourceKey, Value)> {
    owned_rs
        .iter()
        .filter(|(_, rs)| runs_template(rs, template))
        .max_by(|(key_a, a), (key_b, b)| {
            REPLICAS
                .read(a)
                .ok()
                .cmp(&REPLICAS.read(b).ok())
                .then_with(|| key_b.name.cmp(&key_a.name))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash_of(d: &Value) -> TemplateHash {
        NormalizedTemplate::of_spec_template(d)
            .and_then(|t| t.naming_hash())
            .expect("a Deployment with a template has a naming hash")
    }

    fn rs(
        name: &str,
        replicas: Option<i64>,
        hash_label: &str,
        template: &Value,
    ) -> (ResourceKey, Value) {
        let mut v = json!({
            "metadata": {"name": name, "labels": {"pod-template-hash": hash_label}},
            "spec": {"template": template}
        });
        if let Some(n) = replicas {
            v["spec"]["replicas"] = json!(n);
        }
        (
            ResourceKey::namespaced("apps", "v1", "ReplicaSet", "default", name),
            v,
        )
    }

    fn web(image: &str) -> Value {
        json!({
            "metadata": {"labels": {"app": "web"}},
            "spec": {"containers": [{"name": "c", "image": image}]}
        })
    }

    fn name_of(found: Option<&(ResourceKey, Value)>) -> Option<&str> {
        found.map(|(k, _)| k.name.as_str())
    }

    #[test]
    fn the_current_replicaset_is_found_by_template_whatever_its_hash_label() {
        let owned = vec![
            rs("web-old", Some(0), "aaaaaaaaaa", &web("web:1")),
            rs(
                "web-cur",
                Some(3),
                "not-a-hash-this-code-made",
                &web("web:2"),
            ),
        ];
        let t = NormalizedTemplate::of(&web("web:2"));
        assert_eq!(name_of(current_replicaset(&t, &owned)), Some("web-cur"));
    }

    #[test]
    fn a_template_no_owned_replicaset_runs_has_no_current_replicaset() {
        let owned = vec![rs("web-old", Some(3), "aaaaaaaaaa", &web("web:1"))];
        let t = NormalizedTemplate::of(&web("web:2"));
        assert!(current_replicaset(&t, &owned).is_none());
    }

    #[test]
    fn a_replicaset_with_no_template_runs_no_template() {
        let (key, mut v) = rs("web-bare", Some(3), "x", &web("web:1"));
        v["spec"]["template"] = Value::Null;
        let t = NormalizedTemplate::of(&Value::Null);
        assert!(current_replicaset(&t, &[(key, v)]).is_none());
    }

    #[test]
    fn of_two_replicasets_running_the_template_the_one_holding_the_pods_is_current() {
        // Two RSes whose templates differ only in bytes — what the hash
        // matcher left behind. The one running the pods stays current, so
        // switching matchers moves no pod.
        let mut byte_different = web("web:1");
        byte_different["spec"]["volumes"] = json!([]);
        let owned = vec![
            rs("web-aaaa", Some(0), "aaaa", &web("web:1")),
            rs("web-bbbb", Some(3), "bbbb", &byte_different),
        ];
        let t = NormalizedTemplate::of(&web("web:1"));
        assert_eq!(name_of(current_replicaset(&t, &owned)), Some("web-bbbb"));
    }

    #[test]
    fn a_tie_goes_to_the_name_that_sorts_first_in_any_order() {
        let a = rs("web-aaaa", Some(2), "aaaa", &web("web:1"));
        let b = rs("web-bbbb", Some(2), "bbbb", &web("web:1"));
        let t = NormalizedTemplate::of(&web("web:1"));
        let forward = vec![a.clone(), b.clone()];
        let backward = vec![b, a];
        assert_eq!(name_of(current_replicaset(&t, &forward)), Some("web-aaaa"));
        assert_eq!(name_of(current_replicaset(&t, &backward)), Some("web-aaaa"));
    }

    #[test]
    fn an_absent_count_is_the_api_one_and_outranks_zero() {
        let owned = vec![
            rs("web-aaaa", Some(0), "aaaa", &web("web:1")),
            rs("web-bbbb", None, "bbbb", &web("web:1")),
        ];
        let t = NormalizedTemplate::of(&web("web:1"));
        assert_eq!(name_of(current_replicaset(&t, &owned)), Some("web-bbbb"));
    }

    #[test]
    fn build_replicaset_carries_replicas_and_selector() {
        let d = json!({
            "metadata": {"name": "podinfo"},
            "spec": {
                "replicas": 5,
                "selector": {"matchLabels": {"app": "podinfo"}},
                "template": {"metadata": {"labels": {"app": "podinfo"}}, "spec": {}}
            }
        });
        let hash = hash_of(&d);
        let (name, rs) =
            DeploymentController::build_replicaset_from(&d, hash, REPLICAS.read(&d).unwrap())
                .unwrap();
        assert_eq!(name, format!("podinfo-{hash}"));
        assert_eq!(rs.get("spec").unwrap().get("replicas").unwrap(), 5);
        let selector = rs.get("spec").unwrap().get("selector").unwrap();
        assert_eq!(
            selector.get("matchLabels").unwrap().get("app").unwrap(),
            "podinfo"
        );
        let labels = rs.get("metadata").unwrap().get("labels").unwrap();
        assert_eq!(
            labels.get("pod-template-hash").unwrap(),
            &json!(hash.to_string())
        );
    }

    #[test]
    fn build_replicaset_from_inherits_the_deployments_namespace() {
        // The child ReplicaSet MUST land in the parent Deployment's
        // namespace — not "default". Regression test for the bug where a
        // Deployment in ns `team-a` produced an RS in `default`, which the
        // owned-RS query (in `team-a`) never saw → recreate-every-tick loop.
        let d = json!({
            "kind": "Deployment", "apiVersion": "apps/v1",
            "metadata": {"name": "web", "namespace": "team-a", "uid": "u1"},
            "spec": {
                "replicas": 3,
                "selector": {"matchLabels": {"app": "web"}},
                "template": {
                    "metadata": {"labels": {"app": "web"}},
                    "spec": {"containers": [{"name": "c", "image": "img"}]}
                }
            }
        });
        let hash = hash_of(&d);
        let (rs_name, rs) =
            DeploymentController::build_replicaset_from(&d, hash, REPLICAS.read(&d).unwrap())
                .unwrap();
        assert_eq!(rs_name, format!("web-{hash}"));
        assert_eq!(
            rs.get("metadata").unwrap().get("namespace").unwrap(),
            "team-a"
        );
        assert_eq!(rs.get("spec").unwrap().get("replicas").unwrap(), 3);
    }

    #[test]
    fn build_replicaset_from_defaults_namespace_when_parent_has_none() {
        let d = json!({
            "metadata": {"name": "web"},
            "spec": {"replicas": 1, "selector": {}, "template": {"spec": {"containers": []}}}
        });
        let (_, rs) = DeploymentController::build_replicaset_from(&d, hash_of(&d), 1).unwrap();
        assert_eq!(
            rs.get("metadata").unwrap().get("namespace").unwrap(),
            "default"
        );
    }
}
