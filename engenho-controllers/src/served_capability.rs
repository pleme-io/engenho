//! ADVERTISED-BUT-INERT KINDS — making a no-op say so.
//!
//! ★ THE GAP THIS CLOSES. Three kinds are in the catalog and therefore in
//! discovery, and nothing implements their behaviour: `APIService` (no
//! aggregation proxy), `FlowSchema` and `PriorityLevelConfiguration` (no
//! priority-and-fairness queueing). A client can discover them, create one,
//! get a `201`, and receive no semantics whatsoever.
//!
//! That is the one pattern this codebase otherwise refuses everywhere else.
//! `kotae` says an answer must name WHICH of four things happened; a
//! silently-inert kind answers `found` when the truth is `refused`.
//!
//! ★ WHY NOT SIMPLY STOP ADVERTISING THEM. Because the contract is the
//! product. Removing `APIService` from discovery makes engenho visibly not
//! a Kubernetes API server — every client that enumerates api groups sees a
//! hole — and the whole programme is to present the real surface first and
//! decide about the technology behind it afterwards. The honest move is to
//! keep the surface and make the ABSENCE queryable.
//!
//! ★ WHY `Available=False` ON APIService IS THE LOAD-BEARING HALF.
//! `Available` is not decoration: it is the field a client reads to decide
//! whether to ROUTE to an aggregated service. metrics-server registers an
//! APIService and expects the apiserver to proxy `metrics.k8s.io` to it.
//! Left with no status, its registration looks successful and every query
//! to that group silently goes nowhere — a broken feature that reports
//! itself healthy. `Available=False` turns that into `kubectl get
//! apiservices` showing exactly which group is not served, and why.
//!
//! ★ THE OTHER TWO GET AN ENGENHO-SCOPED CONDITION, NOT AN UPSTREAM ONE.
//! `FlowSchema` has an upstream `Dangling` condition meaning something
//! specific (it references a PriorityLevelConfiguration that does not
//! exist). Reusing it to mean "engenho has no APF" would be a lie in
//! upstream's own vocabulary — a client acting on `Dangling` would go fix a
//! reference that is perfectly correct. A condition type we own cannot be
//! misread as a claim we are not making.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};

use engenho_store::StoreMesh;
use engenho_store::command::{Reason, ResourceCommand};

use crate::controller::{Controller, ReconcileOutcome, ReconcileReport};
use crate::error::ControllerError;

/// The condition type engenho uses to declare that a kind's behaviour is
/// not implemented on this server.
///
/// Deliberately NOT an upstream condition type: see the module header.
pub const SERVED_CONDITION: &str = "engenho.io/Served";

/// Upstream's `APIService` availability condition. Real, and read by real
/// clients to decide routing — which is why it is set here and why the
/// value is `False`.
pub const AVAILABLE_CONDITION: &str = "Available";

/// A kind engenho advertises but does not implement.
///
/// A CLOSED enum: adding a fourth inert kind is a compile error at every
/// match, which is the only way this list stays honest as the catalog
/// grows. An open `&str` here would let a kind go inert silently — exactly
/// the failure this module exists to end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InertKind {
    /// Aggregation layer: no proxy to an aggregated apiserver.
    ApiService,
    /// Priority and fairness: no request queueing.
    FlowSchema,
    /// Priority and fairness: no concurrency shares.
    PriorityLevelConfiguration,
}

impl InertKind {
    /// Classify a kind name.
    ///
    /// Returns `None` for every kind engenho DOES implement — a served kind
    /// must never be stamped inert, which would be a worse lie than the
    /// silence this replaces.
    #[must_use]
    pub fn classify(kind: &str) -> Option<Self> {
        match kind {
            "APIService" => Some(Self::ApiService),
            "FlowSchema" => Some(Self::FlowSchema),
            "PriorityLevelConfiguration" => Some(Self::PriorityLevelConfiguration),
            _ => None,
        }
    }

    /// The kind's name, as it appears in the catalog.
    #[must_use]
    pub fn kind_name(self) -> &'static str {
        match self {
            Self::ApiService => "APIService",
            Self::FlowSchema => "FlowSchema",
            Self::PriorityLevelConfiguration => "PriorityLevelConfiguration",
        }
    }

    /// A machine-readable reason, in upstream's CamelCase convention.
    #[must_use]
    pub fn reason(self) -> &'static str {
        match self {
            Self::ApiService => "AggregationNotImplemented",
            Self::FlowSchema | Self::PriorityLevelConfiguration => {
                "PriorityAndFairnessNotImplemented"
            }
        }
    }

    /// A message naming what is missing and what follows from it, because
    /// the operator reading this needs to know the CONSEQUENCE, not just
    /// that a box is unchecked.
    #[must_use]
    pub fn message(self) -> &'static str {
        match self {
            Self::ApiService => {
                "engenho does not implement the aggregation layer: this \
                 APIService is stored and returned, but no request to its \
                 group is proxied to the backing service"
            }
            Self::FlowSchema => {
                "engenho does not implement priority and fairness: this \
                 FlowSchema is stored and returned, but no request is \
                 classified or queued by it"
            }
            Self::PriorityLevelConfiguration => {
                "engenho does not implement priority and fairness: this \
                 PriorityLevelConfiguration is stored and returned, but no \
                 concurrency share is enforced from it"
            }
        }
    }

    /// Whether this kind ALSO carries upstream's own condition.
    ///
    /// Only `APIService` does, and only because `Available` is the field a
    /// client reads to decide routing. Inventing upstream conditions for
    /// the other two would put false claims in upstream's vocabulary.
    #[must_use]
    pub fn upstream_condition(self) -> Option<&'static str> {
        match self {
            Self::ApiService => Some(AVAILABLE_CONDITION),
            Self::FlowSchema | Self::PriorityLevelConfiguration => None,
        }
    }

    /// The conditions to publish on this object's `status`.
    #[must_use]
    pub fn conditions(self, now: &str) -> Vec<Value> {
        let mut out = vec![json!({
            "type": SERVED_CONDITION,
            "status": "False",
            "reason": self.reason(),
            "message": self.message(),
            "lastTransitionTime": now,
        })];
        if let Some(upstream) = self.upstream_condition() {
            out.push(json!({
                "type": upstream,
                "status": "False",
                "reason": self.reason(),
                "message": self.message(),
                "lastTransitionTime": now,
            }));
        }
        out
    }
}

/// Whether an object already carries the truthful conditions.
///
/// Used to keep the stamping idempotent: re-writing an unchanged status on
/// every tick advances the store revision forever, which is the hot-loop
/// class this codebase guards against elsewhere and would reintroduce here.
#[must_use]
pub fn already_stamped(object: &Value, kind: InertKind) -> bool {
    let Some(conds) = object
        .get("status")
        .and_then(|s| s.get("conditions"))
        .and_then(Value::as_array)
    else {
        return false;
    };
    let has = |ty: &str| {
        conds.iter().any(|c| {
            c.get("type").and_then(Value::as_str) == Some(ty)
                && c.get("status").and_then(Value::as_str) == Some("False")
                && c.get("reason").and_then(Value::as_str) == Some(kind.reason())
        })
    };
    has(SERVED_CONDITION) && kind.upstream_condition().is_none_or(has)
}

/// The object with truthful conditions applied.
#[must_use]
pub fn stamped(object: &Value, kind: InertKind, now: &str) -> Value {
    let mut out = object.clone();
    out["status"] = json!({ "conditions": kind.conditions(now) });
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: &str = "2026-08-30T00:00:00Z";

    #[test]
    fn only_the_three_inert_kinds_classify() {
        for k in ["APIService", "FlowSchema", "PriorityLevelConfiguration"] {
            assert!(InertKind::classify(k).is_some(), "{k}");
        }
        // A served kind stamped inert would be a worse lie than the silence
        // this module replaces.
        for k in [
            "Pod",
            "Service",
            "Deployment",
            "Node",
            "CustomResourceDefinition",
        ] {
            assert!(InertKind::classify(k).is_none(), "{k} is served");
        }
    }

    #[test]
    fn classify_round_trips_the_kind_name() {
        for k in ["APIService", "FlowSchema", "PriorityLevelConfiguration"] {
            assert_eq!(InertKind::classify(k).unwrap().kind_name(), k);
        }
    }

    #[test]
    fn apiservice_carries_upstreams_available_and_the_others_do_not() {
        // The load-bearing asymmetry. `Available` is what a client reads to
        // decide whether to route to an aggregated service, so a
        // registration that is silently unreachable must say so THERE.
        let api = InertKind::ApiService.conditions(NOW);
        assert_eq!(api.len(), 2);
        assert!(api.iter().any(|c| c["type"] == AVAILABLE_CONDITION));
        assert!(api.iter().all(|c| c["status"] == "False"));

        // Reusing upstream's `Dangling` to mean "no APF" would send a
        // client to fix a reference that is perfectly correct.
        for k in [InertKind::FlowSchema, InertKind::PriorityLevelConfiguration] {
            let c = k.conditions(NOW);
            assert_eq!(c.len(), 1, "{k:?}");
            assert_eq!(c[0]["type"], SERVED_CONDITION);
            assert_ne!(c[0]["type"], "Dangling");
        }
    }

    #[test]
    fn every_message_names_the_consequence_not_just_the_absence() {
        // An operator needs to know what STOPS WORKING, not that a box is
        // unchecked. Each message says what is stored and what is not done.
        for k in [
            InertKind::ApiService,
            InertKind::FlowSchema,
            InertKind::PriorityLevelConfiguration,
        ] {
            let m = k.message();
            assert!(m.contains("stored and returned"), "{k:?}: {m}");
            assert!(m.contains("engenho does not implement"), "{k:?}: {m}");
        }
    }

    #[test]
    fn stamping_is_idempotent() {
        // Re-writing an unchanged status every tick advances the revision
        // forever — the same hot-loop class the node lease hit.
        for k in [
            InertKind::ApiService,
            InertKind::FlowSchema,
            InertKind::PriorityLevelConfiguration,
        ] {
            let obj = json!({ "kind": k.kind_name(), "metadata": { "name": "x" } });
            assert!(!already_stamped(&obj, k), "{k:?} starts unstamped");
            let once = stamped(&obj, k, NOW);
            assert!(already_stamped(&once, k), "{k:?} is stamped after one pass");
        }
    }

    #[test]
    fn a_partial_stamp_is_not_accepted_as_complete() {
        // An APIService carrying only the engenho condition is still
        // silently unroutable to a client reading `Available`.
        let partial = json!({
            "status": { "conditions": [{
                "type": SERVED_CONDITION,
                "status": "False",
                "reason": InertKind::ApiService.reason(),
            }]}
        });
        assert!(
            !already_stamped(&partial, InertKind::ApiService),
            "the Available half is missing"
        );
    }

    #[test]
    fn a_true_condition_does_not_count_as_stamped() {
        // Guards the direction that matters: something claiming the kind IS
        // served must never satisfy the check.
        let lying = json!({
            "status": { "conditions": [{
                "type": SERVED_CONDITION,
                "status": "True",
                "reason": InertKind::FlowSchema.reason(),
            }]}
        });
        assert!(!already_stamped(&lying, InertKind::FlowSchema));
    }
}

// =====================================================================
// THE PRODUCER
// =====================================================================

/// The API group each inert kind lives in. Kept beside the enum rather
/// than looked up, because the whole list is three entries and a lookup
/// that can miss would silently skip a kind — the failure this module
/// exists to end.
impl InertKind {
    /// `(group, version)` for this kind.
    #[must_use]
    pub fn group_version(self) -> (&'static str, &'static str) {
        match self {
            Self::ApiService => ("apiregistration.k8s.io", "v1"),
            Self::FlowSchema | Self::PriorityLevelConfiguration => {
                ("flowcontrol.apiserver.k8s.io", "v1")
            }
        }
    }

    /// Every inert kind. An array rather than an iterator over strings so
    /// the compiler checks the arity against the closed enum.
    #[must_use]
    pub const fn all() -> [Self; 3] {
        [
            Self::ApiService,
            Self::FlowSchema,
            Self::PriorityLevelConfiguration,
        ]
    }
}

/// Stamps truthful status conditions onto every advertised-but-inert
/// object, so a no-op is queryable instead of silent.
///
/// ★ THIS CONTROLLER IS THE PRODUCER. Without it the module above is a
/// well-tested vocabulary nobody emits — the exact shape that has now been
/// the root of six separate gaps in this codebase.
pub struct ServedCapabilityController {
    store: Arc<StoreMesh>,
}

impl ServedCapabilityController {
    /// New controller over `store`.
    #[must_use]
    pub fn new(store: Arc<StoreMesh>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Controller for ServedCapabilityController {
    fn name(&self) -> &'static str {
        "served-capability"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let mut report = ReconcileReport::default();
        let now = engenho_types::time::now_rfc3339_utc();

        for kind in InertKind::all() {
            let (group, version) = kind.group_version();
            let objects = self
                .store
                .list(group, version, kind.kind_name(), None)
                .await;
            report.objects_examined += objects.len();

            for (key, object) in objects {
                // Idempotent: an unchanged status rewritten every tick
                // advances the revision forever, which is the hot-loop
                // class guarded against elsewhere in this crate.
                if already_stamped(&object, kind) {
                    report.objects_skipped += 1;
                    continue;
                }
                let desired = stamped(&object, kind, &now);
                match self
                    .store
                    .propose(ResourceCommand::Put {
                        key,
                        value: desired,
                        expected: None,
                        reason: Reason::Controller,
                    })
                    .await
                {
                    Ok(_) => report.objects_changed += 1,
                    // Never fatal: failing to annotate an inert object must
                    // not stop the rest of the control plane. The next tick
                    // retries, and the object is no worse off than the
                    // silence this replaces.
                    Err(_) => report.objects_skipped += 1,
                }
            }
        }

        Ok(ReconcileOutcome::from(report))
    }
}

#[cfg(test)]
mod producer_tests {
    use super::*;
    use engenho_store::{InProcessRouter, ResourceKey, default_config};
    use std::time::Duration;

    async fn boot(name: &str) -> Arc<StoreMesh> {
        let router = InProcessRouter::new();
        let cfg = default_config(name).unwrap();
        let store = Arc::new(
            StoreMesh::start(1, "in-process://1".into(), router, cfg)
                .await
                .unwrap(),
        );
        store.initialize_singleton().await.unwrap();
        assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
        store
    }

    #[tokio::test]
    async fn an_apiservice_is_marked_unavailable_so_a_client_stops_routing() {
        let store = boot("served-cap").await;
        let key = ResourceKey::cluster_scoped(
            "apiregistration.k8s.io",
            "v1",
            "APIService",
            "v1beta1.metrics.k8s.io",
        );
        store
            .propose(ResourceCommand::Put {
                key: key.clone(),
                value: json!({
                    "apiVersion": "apiregistration.k8s.io/v1",
                    "kind": "APIService",
                    "metadata": { "name": "v1beta1.metrics.k8s.io" },
                    "spec": { "group": "metrics.k8s.io", "version": "v1beta1" }
                }),
                expected: None,
                reason: Reason::Operator,
            })
            .await
            .unwrap();

        let c = ServedCapabilityController::new(store.clone());
        c.tick().await.unwrap();

        let got = store.get(&key).await.expect("apiservice");
        let conds = got["status"]["conditions"].as_array().expect("conditions");
        // The load-bearing one: this is what metrics-server's registration
        // is judged by, and without it an unreachable service reports
        // itself healthy.
        let avail = conds
            .iter()
            .find(|c| c["type"] == AVAILABLE_CONDITION)
            .expect("Available condition");
        assert_eq!(avail["status"], "False");
        assert_eq!(avail["reason"], "AggregationNotImplemented");

        // And a second tick changes nothing.
        let before = store.current_catalog().await.revision();
        c.tick().await.unwrap();
        assert_eq!(
            store.current_catalog().await.revision(),
            before,
            "stamping is idempotent — a rewrite every tick is a hot loop"
        );

        // The controller holds a store clone; it must go first.
        drop(c);
        Arc::try_unwrap(store)
            .ok()
            .unwrap()
            .terminate()
            .await
            .unwrap();
    }
}
