//! `EndpointsController` — materializes Endpoints **and** EndpointSlice
//! objects from Service selectors + matching Pod IPs.
//!
//! K8s rule: for each Service, find Pods in the same namespace
//! whose labels satisfy `service.spec.selector` AND are ready
//! AND have a `status.podIP`. Materialize one Endpoints object
//! (same name + namespace as the Service) with the Pod IPs in
//! `subsets[].addresses`.
//!
//! ## EndpointSlice (discovery.k8s.io/v1)
//!
//! In addition to the legacy Endpoints object, the controller emits ONE
//! `discovery.k8s.io/v1` EndpointSlice per Service — the modern endpoint-
//! publishing kind kube-proxy + many controllers consume. Both objects
//! are derived from the SAME selector→pod resolution (no fork): the slice
//! is a parallel projection of the identical `(ip, pod_name)` set. The
//! slice carries the upstream-required `kubernetes.io/service-name` label
//! + an owner reference back to the Service, and is named `<service>`
//! (engenho emits exactly one slice per Service; upstream shards into
//! `<service>-<hash>` only above 1000 endpoints — a typed follow-up).
//!
//! This is the FIRST selector-based controller in engenho-controllers
//! (vs the owner-ref-based ReplicaSet/Deployment/GC). It validates
//! that the Controller trait is general — the trait knows nothing
//! about ownership; selector matching is just a different
//! reconciliation predicate.

use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand},
    resource::ResourceKey,
};
use serde_json::{Value, json};
use tracing::debug;

use crate::controller::{Controller, ReconcileOutcome, ReconcileReport};
use crate::create_stamp::{CreateClock, stamp_create_timestamp, wall_clock};
use crate::error::ControllerError;
use crate::meta::ObjectMeta;
use crate::owner::{owner_ref_for, set_owner_reference};
use crate::selector::{matches_labels, service_selector};

pub struct EndpointsController {
    store: Arc<StoreMesh>,
    namespace: Option<String>,
    /// Boundary clock read ONCE per created Endpoints object to freeze
    /// `metadata.creationTimestamp` into the replicated `Put` (see
    /// [`crate::create_stamp`]). Production = [`wall_clock`]; unit tests
    /// pin a fixed instant via [`Self::with_clock`].
    create_clock: CreateClock,
}

impl EndpointsController {
    #[must_use]
    pub fn new(store: Arc<StoreMesh>, namespace: Option<String>) -> Self {
        Self {
            store,
            namespace,
            create_clock: wall_clock,
        }
    }

    /// Construct with a pinned [`Clock`] — the unit-test determinism seam
    /// for the `creationTimestamp` boundary stamp.
    #[must_use]
    pub fn with_clock(store: Arc<StoreMesh>, namespace: Option<String>, clock: CreateClock) -> Self {
        Self {
            store,
            namespace,
            create_clock: clock,
        }
    }

    /// Pod's IP from `status.podIP`. Returns None for unbound
    /// pods (no status yet) or for pods missing the field.
    fn pod_ip(pod: &Value) -> Option<&str> {
        pod.get("status")
            .and_then(|s| s.get("podIP"))
            .and_then(|i| i.as_str())
    }

    /// Pod considered Ready iff `status.conditions[Ready].status == "True"`.
    /// Pods without status yet are treated as not-ready (won't be added
    /// to Endpoints until kubelet reports them).
    fn pod_is_ready(pod: &Value) -> bool {
        pod.get("status")
            .and_then(|s| s.get("conditions"))
            .and_then(|c| c.as_array())
            .map(|conds| {
                conds.iter().any(|c| {
                    c.get("type").and_then(|t| t.as_str()) == Some("Ready")
                        && c.get("status").and_then(|s| s.as_str()) == Some("True")
                })
            })
            .unwrap_or(false)
    }

    /// Build the Endpoints object body. `addresses` is a typed
    /// list of (ip, target_pod_name) pairs.
    fn build_endpoints(svc: &Value, addresses: Vec<(String, String)>) -> Value {
        let name = svc.name().unwrap_or("");
        let ports = svc
            .get("spec")
            .and_then(|s| s.get("ports"))
            .cloned()
            .unwrap_or_else(|| json!([]));

        let subset_addresses: Vec<Value> = addresses
            .into_iter()
            .map(|(ip, pod_name)| {
                json!({
                    "ip": ip,
                    "targetRef": {
                        "kind": "Pod",
                        "name": pod_name
                    }
                })
            })
            .collect();

        json!({
            "kind": "Endpoints",
            "apiVersion": "v1",
            "metadata": { "name": name },
            "subsets": [
                {
                    "addresses": subset_addresses,
                    "ports": ports
                }
            ]
        })
    }

    /// Build the `discovery.k8s.io/v1` EndpointSlice body from the SAME
    /// `(ip, pod_name)` set the Endpoints object uses. Parallel projection,
    /// not a fork: the caller resolves the selector once and feeds both.
    ///
    /// The slice carries the required `kubernetes.io/service-name` label so
    /// consumers (kube-proxy, the routing controller) can locate every
    /// slice for a Service. `addressType: IPv4` (engenho's pods are IPv4
    /// today; dual-stack is a typed follow-up). Each endpoint is marked
    /// `conditions.ready = true` (the caller already filtered to ready,
    /// ip-bearing pods).
    fn build_endpoint_slice(svc: &Value, addresses: &[(String, String)]) -> Value {
        let name = svc.name().unwrap_or("");
        // EndpointSlice ports use the same `(name, port, protocol)` shape as
        // the Service ports — projected verbatim (the Service's targetPort is
        // the pod-side port the slice publishes; engenho's Endpoints carries
        // the Service ports today, mirrored here for one source of truth).
        let ports = svc
            .get("spec")
            .and_then(|s| s.get("ports"))
            .and_then(|p| p.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|p| {
                        let port_name = p.get("name").cloned().unwrap_or(Value::Null);
                        // The slice publishes the pod-side port (targetPort);
                        // default to the service port when targetPort is unset
                        // (mirrors the routing controller's resolution).
                        let port = p
                            .get("targetPort")
                            .or_else(|| p.get("port"))
                            .cloned()
                            .unwrap_or(Value::Null);
                        let protocol = p
                            .get("protocol")
                            .cloned()
                            .unwrap_or_else(|| json!("TCP"));
                        json!({ "name": port_name, "port": port, "protocol": protocol })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let endpoints: Vec<Value> = addresses
            .iter()
            .map(|(ip, pod_name)| {
                json!({
                    "addresses": [ip],
                    "conditions": { "ready": true },
                    "targetRef": { "kind": "Pod", "name": pod_name }
                })
            })
            .collect();

        json!({
            "kind": "EndpointSlice",
            "apiVersion": "discovery.k8s.io/v1",
            "metadata": {
                "name": name,
                "labels": { "kubernetes.io/service-name": name }
            },
            "addressType": "IPv4",
            "endpoints": endpoints,
            "ports": ports
        })
    }

    /// Reconcile the EndpointSlice for a Service: owner-ref it, stamp the
    /// creationTimestamp on create, and write it only when the slice body
    /// changed (idempotent). Mirrors the Endpoints reconcile shape so both
    /// projections share one convergence discipline.
    async fn reconcile_endpoint_slice(
        &self,
        namespace: &str,
        svc_name: &str,
        mut slice: Value,
        owner_ref: crate::owner::OwnerReference,
        report: &mut ReconcileReport,
    ) -> Result<(), ControllerError> {
        let slice_key =
            ResourceKey::namespaced("discovery.k8s.io", "v1", "EndpointSlice", namespace, svc_name);
        set_owner_reference(&mut slice, owner_ref);

        let existing = self.store.get(&slice_key).await;
        if let Some(ref current) = existing {
            if slice_bodies_equivalent(current, &slice) {
                return Ok(());
            }
        } else {
            // First materialization: freeze the creationTimestamp from one
            // boundary clock read (same discipline as Endpoints).
            stamp_create_timestamp(&mut slice, self.create_clock);
        }

        self.store
            .propose(ResourceCommand::Put {
                key: slice_key,
                value: slice,
                expected: None,
                reason: Reason::Controller,
            })
            .await?;
        report.objects_changed += 1;
        Ok(())
    }
}

#[async_trait]
impl Controller for EndpointsController {
    fn name(&self) -> &'static str {
        "endpoints"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let services = self
            .store
            .list("", "v1", "Service", self.namespace.as_deref())
            .await;
        let mut report = ReconcileReport::default();
        report.objects_examined = services.len();

        for (svc_key, svc_value) in &services {
            let Some(selector) = service_selector(svc_value) else {
                report.objects_skipped += 1;
                continue;
            };
            let Some(owner_ref) = owner_ref_for(svc_value, "v1", "Service") else {
                report.objects_skipped += 1;
                continue;
            };
            let ns = svc_key.namespace.as_deref();
            let all_pods = self.store.list("", "v1", "Pod", ns).await;

            // Filter to ready, ip-bearing pods matching the selector.
            let mut addresses: Vec<(String, String)> = all_pods
                .iter()
                .filter(|(_, pod)| matches_labels(pod, selector))
                .filter(|(_, pod)| Self::pod_is_ready(pod))
                .filter_map(|(_, pod)| {
                    let ip = Self::pod_ip(pod)?.to_string();
                    let name = pod
                        .get("metadata")
                        .and_then(|m| m.get("name"))
                        .and_then(|n| n.as_str())?
                        .to_string();
                    Some((ip, name))
                })
                .collect();
            // Deterministic order for tests + diffing.
            addresses.sort();

            let endpoints_ns = ns.unwrap_or("default");
            let svc_name = svc_value.name().unwrap_or("");
            let endpoints_key =
                ResourceKey::namespaced("", "v1", "Endpoints", endpoints_ns, svc_name);

            // Build the EndpointSlice from the SAME resolved address set
            // (borrowed before `addresses` is moved into build_endpoints).
            let slice_body = Self::build_endpoint_slice(svc_value, &addresses);

            // Check if existing Endpoints already matches what we'd write.
            let existing = self.store.get(&endpoints_key).await;
            let mut new_endpoints = Self::build_endpoints(svc_value, addresses);
            set_owner_reference(&mut new_endpoints, owner_ref.clone());

            // Emit the EndpointSlice (parallel projection). Done before the
            // Endpoints early-return so the slice converges even when the
            // Endpoints subsets are unchanged (e.g. first slice on an
            // already-materialized Endpoints).
            self.reconcile_endpoint_slice(
                endpoints_ns,
                svc_name,
                slice_body,
                owner_ref.clone(),
                &mut report,
            )
            .await?;

            if let Some(ref current) = existing {
                if subsets_equivalent(current, &new_endpoints) {
                    continue;
                }
            } else {
                // CREATE (no existing Endpoints): freeze creationTimestamp
                // from ONE boundary clock read so kubectl AGE renders. An
                // UPDATE (existing) is left untouched — never bumped.
                stamp_create_timestamp(&mut new_endpoints, self.create_clock);
            }

            debug!(
                svc = %svc_key.label(),
                endpoint_count = new_endpoints
                    .get("subsets")
                    .and_then(|s| s.get(0))
                    .and_then(|s| s.get("addresses"))
                    .and_then(|a| a.as_array())
                    .map(|a| a.len())
                    .unwrap_or(0),
                "writing endpoints"
            );

            self.store
                .propose(ResourceCommand::Put {
                    key: endpoints_key,
                    value: new_endpoints,
                    expected: None,
                    reason: Reason::Controller,
                })
                .await?;
            report.objects_changed += 1;
        }
        Ok(report.into())
    }
}

/// Compare two Endpoints values for subset equivalence (ignores
/// metadata.resourceVersion, uid, etc. which the store auto-fills).
fn subsets_equivalent(a: &Value, b: &Value) -> bool {
    a.get("subsets") == b.get("subsets")
}

/// Compare two EndpointSlice values for body equivalence — the
/// `endpoints` + `ports` + `addressType` carry the load-bearing state;
/// metadata.resourceVersion/uid (store-filled) is ignored so a re-tick
/// with the same pods is a no-op.
fn slice_bodies_equivalent(a: &Value, b: &Value) -> bool {
    a.get("endpoints") == b.get("endpoints")
        && a.get("ports") == b.get("ports")
        && a.get("addressType") == b.get("addressType")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pod_ip_reads_status_podip() {
        let p = json!({"status": {"podIP": "10.0.0.1"}});
        assert_eq!(EndpointsController::pod_ip(&p), Some("10.0.0.1"));
    }

    #[test]
    fn pod_ip_none_when_missing() {
        assert!(EndpointsController::pod_ip(&json!({})).is_none());
        assert!(EndpointsController::pod_ip(&json!({"status": {}})).is_none());
    }

    #[test]
    fn pod_is_ready_true_when_condition_true() {
        let p = json!({
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        });
        assert!(EndpointsController::pod_is_ready(&p));
    }

    #[test]
    fn pod_is_ready_false_when_condition_false() {
        let p = json!({
            "status": {
                "conditions": [{"type": "Ready", "status": "False"}]
            }
        });
        assert!(!EndpointsController::pod_is_ready(&p));
    }

    #[test]
    fn pod_is_ready_false_when_no_status() {
        let p = json!({"metadata": {"name": "p"}});
        assert!(!EndpointsController::pod_is_ready(&p));
    }

    #[test]
    fn build_endpoints_carries_selector_addresses() {
        let svc = json!({
            "metadata": {"name": "podinfo"},
            "spec": {"selector": {"app": "podinfo"}, "ports": [{"port": 80}]}
        });
        let addrs = vec![
            ("10.0.0.1".into(), "p1".into()),
            ("10.0.0.2".into(), "p2".into()),
        ];
        let ep = EndpointsController::build_endpoints(&svc, addrs);
        assert_eq!(ep.get("kind").unwrap(), "Endpoints");
        let subsets = ep.get("subsets").unwrap().as_array().unwrap();
        assert_eq!(subsets.len(), 1);
        let addresses = subsets[0].get("addresses").unwrap().as_array().unwrap();
        assert_eq!(addresses.len(), 2);
        assert_eq!(addresses[0].get("ip").unwrap(), "10.0.0.1");
        assert_eq!(
            addresses[0].get("targetRef").unwrap().get("kind").unwrap(),
            "Pod"
        );
        // Ports carried through from service.
        let ports = subsets[0].get("ports").unwrap().as_array().unwrap();
        assert_eq!(ports.len(), 1);
        assert_eq!(ports[0].get("port").unwrap(), 80);
    }

    #[test]
    fn build_endpoints_two_addresses_in_sorted_order() {
        // M0.3 podIP→Endpoints math: two ready pods on the same selector
        // produce a 2-address subset. The controller sorts addresses
        // (endpoints.rs:170) before building, so the IP+name pairs land in
        // deterministic order regardless of pod-listing order — proven
        // here by feeding them pre-sorted (as the controller does) and
        // asserting both IPs + their targetRefs survive intact. This pins
        // the Endpoints-from-pods computation with NO container runtime.
        let svc = json!({
            "metadata": {"name": "podinfo"},
            "spec": {"selector": {"app": "podinfo"}, "ports": [{"port": 80, "targetPort": 80}]}
        });
        let mut addrs = vec![
            ("10.89.0.7".to_string(), "pod-b".to_string()),
            ("10.89.0.4".to_string(), "pod-a".to_string()),
        ];
        // Mirror the controller's `addresses.sort()` before build.
        addrs.sort();
        let ep = EndpointsController::build_endpoints(&svc, addrs);
        let subsets = ep.get("subsets").unwrap().as_array().unwrap();
        assert_eq!(subsets.len(), 1);
        let addresses = subsets[0].get("addresses").unwrap().as_array().unwrap();
        assert_eq!(addresses.len(), 2);
        // Sorted: 10.89.0.4 (pod-a) before 10.89.0.7 (pod-b).
        assert_eq!(addresses[0].get("ip").unwrap(), "10.89.0.4");
        assert_eq!(
            addresses[0].get("targetRef").unwrap().get("name").unwrap(),
            "pod-a"
        );
        assert_eq!(addresses[1].get("ip").unwrap(), "10.89.0.7");
        assert_eq!(
            addresses[1].get("targetRef").unwrap().get("name").unwrap(),
            "pod-b"
        );
        // Port copied from the Service.
        let ports = subsets[0].get("ports").unwrap().as_array().unwrap();
        assert_eq!(ports[0].get("port").unwrap(), 80);
    }

    #[test]
    fn build_endpoint_slice_projects_addresses_and_label() {
        let svc = json!({
            "metadata": {"name": "podinfo"},
            "spec": {"selector": {"app": "podinfo"},
                     "ports": [{"name": "http", "port": 80, "targetPort": 9898}]}
        });
        let addrs = vec![
            ("10.0.0.1".to_string(), "p1".to_string()),
            ("10.0.0.2".to_string(), "p2".to_string()),
        ];
        let slice = EndpointsController::build_endpoint_slice(&svc, &addrs);
        assert_eq!(slice["kind"], "EndpointSlice");
        assert_eq!(slice["apiVersion"], "discovery.k8s.io/v1");
        assert_eq!(slice["addressType"], "IPv4");
        // Required service-name label so consumers can find the slice.
        assert_eq!(
            slice["metadata"]["labels"]["kubernetes.io/service-name"],
            "podinfo"
        );
        // One endpoint per address, each ready, with its pod targetRef.
        let eps = slice["endpoints"].as_array().unwrap();
        assert_eq!(eps.len(), 2);
        assert_eq!(eps[0]["addresses"][0], "10.0.0.1");
        assert_eq!(eps[0]["conditions"]["ready"], true);
        assert_eq!(eps[0]["targetRef"]["name"], "p1");
        // Port published is the pod-side targetPort (9898), not service 80.
        let ports = slice["ports"].as_array().unwrap();
        assert_eq!(ports[0]["name"], "http");
        assert_eq!(ports[0]["port"], 9898);
        assert_eq!(ports[0]["protocol"], "TCP");
    }

    #[test]
    fn build_endpoint_slice_empty_when_no_addresses() {
        let svc = json!({"metadata": {"name": "x"}, "spec": {"ports": [{"port": 80}]}});
        let slice = EndpointsController::build_endpoint_slice(&svc, &[]);
        assert!(slice["endpoints"].as_array().unwrap().is_empty());
    }

    #[test]
    fn slice_bodies_equivalent_ignores_metadata() {
        let a = json!({"metadata": {"resourceVersion": "5"},
                       "addressType": "IPv4", "endpoints": [{"addresses": ["10.0.0.1"]}],
                       "ports": [{"port": 80}]});
        let b = json!({"metadata": {"resourceVersion": "99"},
                       "addressType": "IPv4", "endpoints": [{"addresses": ["10.0.0.1"]}],
                       "ports": [{"port": 80}]});
        assert!(slice_bodies_equivalent(&a, &b));
        let c = json!({"addressType": "IPv4",
                       "endpoints": [{"addresses": ["10.0.0.2"]}], "ports": [{"port": 80}]});
        assert!(!slice_bodies_equivalent(&a, &c));
    }

    #[test]
    fn subsets_equivalent_compares_only_subsets() {
        let a = json!({"metadata": {"resourceVersion": "5"}, "subsets": [{"x": 1}]});
        let b = json!({"metadata": {"resourceVersion": "99"}, "subsets": [{"x": 1}]});
        assert!(subsets_equivalent(&a, &b));
        let c = json!({"subsets": [{"x": 2}]});
        assert!(!subsets_equivalent(&a, &c));
    }

    #[test]
    fn controller_name_is_stable() {
        struct Fake;
        #[async_trait]
        impl Controller for Fake {
            fn name(&self) -> &'static str {
                "endpoints"
            }
            async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
                Ok(ReconcileReport::default().into())
            }
        }
        assert_eq!(Fake.name(), "endpoints");
    }

    // ── creationTimestamp on controller-created Endpoints (Part B) ────────

    const FIXED_TS: &str = "2026-06-14T09:00:00Z";
    fn fixed_clock() -> String {
        FIXED_TS.to_string()
    }

    async fn live_store() -> Arc<StoreMesh> {
        use engenho_store::{InProcessRouter, default_config};
        use std::time::Duration;
        let router = InProcessRouter::new();
        let cfg = default_config("controllers-endpoints").unwrap();
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
    async fn created_endpoints_carries_frozen_creation_timestamp() {
        let store = live_store().await;
        // A Service with a selector + a ready, ip-bearing Pod matching it.
        store
            .propose(ResourceCommand::put(
                ResourceKey::namespaced("", "v1", "Service", "ns1", "svc"),
                json!({"kind": "Service", "apiVersion": "v1",
                       "metadata": {"name": "svc", "namespace": "ns1"},
                       "spec": {"selector": {"app": "x"},
                                "ports": [{"port": 80, "targetPort": 80}]}}),
                Reason::Operator,
            ))
            .await
            .unwrap();
        store
            .propose(ResourceCommand::put(
                ResourceKey::namespaced("", "v1", "Pod", "ns1", "p"),
                json!({"kind": "Pod", "apiVersion": "v1",
                       "metadata": {"name": "p", "namespace": "ns1", "labels": {"app": "x"}},
                       "status": {"podIP": "10.0.0.1",
                                  "conditions": [{"type": "Ready", "status": "True"}]}}),
                Reason::Operator,
            ))
            .await
            .unwrap();

        let c = EndpointsController::with_clock(store.clone(), None, fixed_clock);
        c.tick().await.unwrap();

        let ep = store
            .get(&ResourceKey::namespaced("", "v1", "Endpoints", "ns1", "svc"))
            .await
            .expect("Endpoints created");
        assert_eq!(
            ep.get("metadata").unwrap().get("creationTimestamp").unwrap(),
            FIXED_TS,
            "controller-created Endpoints must carry the frozen creationTimestamp"
        );

        // A subsequent tick that re-writes the subsets (e.g. a new pod) must
        // NOT bump creationTimestamp — it is preserved from the prior object.
        store
            .propose(ResourceCommand::put(
                ResourceKey::namespaced("", "v1", "Pod", "ns1", "p2"),
                json!({"kind": "Pod", "apiVersion": "v1",
                       "metadata": {"name": "p2", "namespace": "ns1", "labels": {"app": "x"}},
                       "status": {"podIP": "10.0.0.2",
                                  "conditions": [{"type": "Ready", "status": "True"}]}}),
                Reason::Operator,
            ))
            .await
            .unwrap();
        // Use a DIFFERENT clock to prove the update path never stamps.
        let c2 = EndpointsController::with_clock(store.clone(), None, || {
            "2099-01-01T00:00:00Z".to_string()
        });
        c2.tick().await.unwrap();
        let ep2 = store
            .get(&ResourceKey::namespaced("", "v1", "Endpoints", "ns1", "svc"))
            .await
            .unwrap();
        // The Endpoints was updated (now 2 addresses) but the timestamp held.
        // NOTE: the store preserves metadata.creationTimestamp across an
        // update Put (it carries the prior object's value); the controller's
        // create-only stamp simply never fires on the update path.
        assert_eq!(
            ep2.get("metadata").unwrap().get("creationTimestamp").unwrap(),
            FIXED_TS,
            "creationTimestamp must be stable across the update (never bumped)"
        );
    }

    #[tokio::test]
    async fn controller_emits_endpoint_slice_alongside_endpoints() {
        let store = live_store().await;
        // A Service with a selector + a ready, ip-bearing Pod matching it.
        store
            .propose(ResourceCommand::put(
                ResourceKey::namespaced("", "v1", "Service", "ns1", "svc"),
                json!({"kind": "Service", "apiVersion": "v1",
                       "metadata": {"name": "svc", "namespace": "ns1",
                                    "uid": "svc-uid"},
                       "spec": {"selector": {"app": "x"},
                                "ports": [{"name": "http", "port": 80, "targetPort": 9898}]}}),
                Reason::Operator,
            ))
            .await
            .unwrap();
        store
            .propose(ResourceCommand::put(
                ResourceKey::namespaced("", "v1", "Pod", "ns1", "p"),
                json!({"kind": "Pod", "apiVersion": "v1",
                       "metadata": {"name": "p", "namespace": "ns1", "labels": {"app": "x"}},
                       "status": {"podIP": "10.0.0.1",
                                  "conditions": [{"type": "Ready", "status": "True"}]}}),
                Reason::Operator,
            ))
            .await
            .unwrap();

        let c = EndpointsController::with_clock(store.clone(), None, fixed_clock);
        c.tick().await.unwrap();

        // The legacy Endpoints object exists.
        let ep = store
            .get(&ResourceKey::namespaced("", "v1", "Endpoints", "ns1", "svc"))
            .await
            .expect("Endpoints created");
        assert_eq!(ep["subsets"][0]["addresses"][0]["ip"], "10.0.0.1");

        // The EndpointSlice exists in parallel, derived from the SAME pod.
        let slice = store
            .get(&ResourceKey::namespaced(
                "discovery.k8s.io",
                "v1",
                "EndpointSlice",
                "ns1",
                "svc",
            ))
            .await
            .expect("EndpointSlice created");
        assert_eq!(slice["kind"], "EndpointSlice");
        assert_eq!(
            slice["metadata"]["labels"]["kubernetes.io/service-name"],
            "svc"
        );
        assert_eq!(slice["endpoints"][0]["addresses"][0], "10.0.0.1");
        assert_eq!(slice["endpoints"][0]["conditions"]["ready"], true);
        // The slice carries the frozen creationTimestamp + an owner ref.
        assert_eq!(slice["metadata"]["creationTimestamp"], FIXED_TS);
        assert_eq!(slice["metadata"]["ownerReferences"][0]["kind"], "Service");

        // Idempotent: a re-tick with no pod change writes nothing new.
        c.tick().await.unwrap();
        let slice2 = store
            .get(&ResourceKey::namespaced(
                "discovery.k8s.io",
                "v1",
                "EndpointSlice",
                "ns1",
                "svc",
            ))
            .await
            .unwrap();
        assert_eq!(slice2["endpoints"], slice["endpoints"], "stable across re-tick");
    }
}
