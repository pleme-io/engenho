//! `EndpointsController` — materializes Endpoints objects from
//! Service selectors + matching Pod IPs.
//!
//! K8s rule: for each Service, find Pods in the same namespace
//! whose labels satisfy `service.spec.selector` AND are ready
//! AND have a `status.podIP`. Materialize one Endpoints object
//! (same name + namespace as the Service) with the Pod IPs in
//! `subsets[].addresses`.
//!
//! This is the FIRST selector-based controller in engenho-controllers
//! (vs the owner-ref-based ReplicaSet/Deployment/GC). It validates
//! that the Controller trait is general — the trait knows nothing
//! about ownership; selector matching is just a different
//! reconciliation predicate.

use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{
    command::{Reason, ResourceCommand},
    resource::ResourceKey,
    StoreMesh,
};
use serde_json::{json, Value};
use tracing::debug;

use crate::controller::{Controller, ReconcileReport};
use crate::error::ControllerError;
use crate::owner::{set_owner_reference, OwnerReference};
use crate::selector::{matches_labels, service_selector};

pub struct EndpointsController {
    store: Arc<StoreMesh>,
    namespace: Option<String>,
}

impl EndpointsController {
    #[must_use]
    pub fn new(store: Arc<StoreMesh>, namespace: Option<String>) -> Self {
        Self { store, namespace }
    }

    fn service_uid(svc: &Value) -> Option<String> {
        svc.get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(|u| u.as_str())
            .map(String::from)
    }

    fn service_name(svc: &Value) -> Option<&str> {
        svc.get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
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

    fn owner_ref_for(svc: &Value) -> Option<OwnerReference> {
        Some(OwnerReference {
            api_version: "v1".into(),
            kind: "Service".into(),
            name: Self::service_name(svc)?.to_string(),
            uid: Self::service_uid(svc)?,
            controller: true,
            block_owner_deletion: true,
        })
    }

    /// Build the Endpoints object body. `addresses` is a typed
    /// list of (ip, target_pod_name) pairs.
    fn build_endpoints(
        svc: &Value,
        addresses: Vec<(String, String)>,
    ) -> Value {
        let name = Self::service_name(svc).unwrap_or("");
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
}

#[async_trait]
impl Controller for EndpointsController {
    fn name(&self) -> &'static str {
        "endpoints"
    }

    async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
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
            let Some(owner_ref) = Self::owner_ref_for(svc_value) else {
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
            let endpoints_key = ResourceKey::namespaced(
                "",
                "v1",
                "Endpoints",
                endpoints_ns,
                Self::service_name(svc_value).unwrap_or(""),
            );

            // Check if existing Endpoints already matches what we'd write.
            let existing = self.store.get(&endpoints_key).await;
            let mut new_endpoints = Self::build_endpoints(svc_value, addresses);
            set_owner_reference(&mut new_endpoints, owner_ref.clone());

            if let Some(ref current) = existing {
                if subsets_equivalent(current, &new_endpoints) {
                    continue;
                }
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
                    reason: Reason::Controller,
                })
                .await
                .map_err(|e| ControllerError::Store(e.to_string()))?;
            report.objects_changed += 1;
        }
        Ok(report)
    }
}

/// Compare two Endpoints values for subset equivalence (ignores
/// metadata.resourceVersion, uid, etc. which the store auto-fills).
fn subsets_equivalent(a: &Value, b: &Value) -> bool {
    a.get("subsets") == b.get("subsets")
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
        let addrs = vec![("10.0.0.1".into(), "p1".into()), ("10.0.0.2".into(), "p2".into())];
        let ep = EndpointsController::build_endpoints(&svc, addrs);
        assert_eq!(ep.get("kind").unwrap(), "Endpoints");
        let subsets = ep.get("subsets").unwrap().as_array().unwrap();
        assert_eq!(subsets.len(), 1);
        let addresses = subsets[0].get("addresses").unwrap().as_array().unwrap();
        assert_eq!(addresses.len(), 2);
        assert_eq!(addresses[0].get("ip").unwrap(), "10.0.0.1");
        assert_eq!(addresses[0].get("targetRef").unwrap().get("kind").unwrap(), "Pod");
        // Ports carried through from service.
        let ports = subsets[0].get("ports").unwrap().as_array().unwrap();
        assert_eq!(ports.len(), 1);
        assert_eq!(ports[0].get("port").unwrap(), 80);
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
            fn name(&self) -> &'static str { "endpoints" }
            async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
                Ok(ReconcileReport::default())
            }
        }
        assert_eq!(Fake.name(), "endpoints");
    }
}
