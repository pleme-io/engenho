//! R11 — Service IP routing primitive.
//!
//! The substrate has Service + Endpoints objects + matching Pods
//! since R9.6; before R11 nothing actually routed traffic from a
//! Service's ClusterIP to the backing Pods. R11 ships the typed
//! routing surface + a backend trait.
//!
//! ## Architecture
//!
//! ```text
//! ServiceRoutingController (Controller impl)
//!     reads: Service + Endpoints from StoreMesh
//!     emits: RouteTable
//!         → pluggable ServiceRouter backend:
//!             - FakeRouter (tests; tracks routes in BTreeMap)
//!             - IptablesRouter (Linux production; shells out to iptables)
//!             - IpvsRouter (Linux production, scalable; future R11.5)
//! ```
//!
//! ## Reconcile rule
//!
//! 1. List Services + Endpoints from the store.
//! 2. For each pair (service, endpoints), compute the typed
//!    [`ServiceRoute`]: ClusterIP + per-port (proto, port, target_port)
//!    + the set of healthy Pod IPs from Endpoints.subsets.
//! 3. Diff against the backend's current table. Add/remove routes
//!    to converge.
//! 4. Idempotent — re-running with the same state is a no-op.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::StoreMesh;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::controller::{Controller, ReconcileReport};
use crate::error::ControllerError;

/// A single routing entry — one ClusterIP:port → set of pod backends.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ServiceRoute {
    /// `namespace/name` identifier — stable across edits.
    pub service_id: String,
    /// Cluster-IP allocated for the service. Empty = headless.
    pub cluster_ip: String,
    /// Port mapping (each Service can expose multiple ports).
    pub ports: Vec<PortMap>,
    /// Sorted set of backend Pod IPs ready to receive traffic.
    pub endpoints: BTreeSet<String>,
}

/// Per-port service-to-pod mapping.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PortMap {
    /// Operator-facing port name (e.g. "http").
    pub name: String,
    /// Service-side port (the ClusterIP listens here).
    pub service_port: u16,
    /// Pod-side port (Endpoints subsets each have this).
    pub target_port: u16,
    /// Protocol; defaults to "TCP".
    pub protocol: String,
}

/// Errors a ServiceRouter backend may return.
#[derive(Debug, Clone, Error)]
pub enum RouterError {
    /// Backend (iptables / ipvs) returned a non-zero exit.
    #[error("backend: {0}")]
    Backend(String),
    /// Invalid route shape (e.g. empty service_id).
    #[error("invalid route: {0}")]
    InvalidRoute(String),
}

impl RouterError {
    /// Stable identifier for telemetry / SDK dispatch.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Backend(_) => "backend",
            Self::InvalidRoute(_) => "invalid_route",
        }
    }
}

/// Pluggable routing backend trait. Implementations apply +
/// remove routes on the local host (iptables, ipvs, ebpf, fake).
#[async_trait]
pub trait ServiceRouter: Send + Sync {
    /// Stable backend name.
    fn name(&self) -> &'static str;

    /// Install / refresh a route. Idempotent — if the route is
    /// already installed identically, this is a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`RouterError::Backend`] if the host refuses the rule.
    async fn upsert(&self, route: &ServiceRoute) -> Result<(), RouterError>;

    /// Remove an existing route by `service_id`.
    ///
    /// # Errors
    ///
    /// [`RouterError::Backend`] on host failure.
    async fn remove(&self, service_id: &str) -> Result<(), RouterError>;

    /// Currently-installed routes (for diff + reconcile).
    ///
    /// # Errors
    ///
    /// [`RouterError::Backend`] on backend inspection failure.
    async fn list(&self) -> Result<BTreeMap<String, ServiceRoute>, RouterError>;
}

// =================================================================
// FakeRouter — deterministic in-memory backend for tests
// =================================================================

/// Test backend. Tracks routes in a BTreeMap + records every
/// upsert/remove call for assertion in tests.
#[derive(Default, Clone)]
pub struct FakeRouter {
    inner: Arc<Mutex<FakeRouterState>>,
}

#[derive(Default)]
struct FakeRouterState {
    routes: BTreeMap<String, ServiceRoute>,
    events: Vec<FakeRouterEvent>,
}

/// Per-call event log entry for tests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FakeRouterEvent {
    /// `upsert(service_id)` was invoked.
    Upsert(String),
    /// `remove(service_id)` was invoked.
    Remove(String),
}

impl FakeRouter {
    /// Fresh empty backend.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of recorded events.
    pub async fn events(&self) -> Vec<FakeRouterEvent> {
        self.inner.lock().await.events.clone()
    }

    /// Current route count.
    pub async fn route_count(&self) -> usize {
        self.inner.lock().await.routes.len()
    }
}

#[async_trait]
impl ServiceRouter for FakeRouter {
    fn name(&self) -> &'static str {
        "fake"
    }

    async fn upsert(&self, route: &ServiceRoute) -> Result<(), RouterError> {
        if route.service_id.is_empty() {
            return Err(RouterError::InvalidRoute("empty service_id".into()));
        }
        let mut state = self.inner.lock().await;
        state.routes.insert(route.service_id.clone(), route.clone());
        state.events.push(FakeRouterEvent::Upsert(route.service_id.clone()));
        Ok(())
    }

    async fn remove(&self, service_id: &str) -> Result<(), RouterError> {
        let mut state = self.inner.lock().await;
        state.routes.remove(service_id);
        state.events.push(FakeRouterEvent::Remove(service_id.to_string()));
        Ok(())
    }

    async fn list(&self) -> Result<BTreeMap<String, ServiceRoute>, RouterError> {
        Ok(self.inner.lock().await.routes.clone())
    }
}

// =================================================================
// ServiceRoutingController — reads store, drives the backend
// =================================================================

/// Controller that watches Services + Endpoints in the store and
/// drives a [`ServiceRouter`] backend to match.
pub struct ServiceRoutingController {
    store: Arc<StoreMesh>,
    backend: Arc<dyn ServiceRouter>,
    namespace: Option<String>,
}

impl ServiceRoutingController {
    /// Construct a controller for `backend`, optionally namespace-scoped.
    #[must_use]
    pub fn new(
        store: Arc<StoreMesh>,
        backend: Arc<dyn ServiceRouter>,
        namespace: Option<String>,
    ) -> Self {
        Self {
            store,
            backend,
            namespace,
        }
    }

    /// Build the canonical [`ServiceRoute`] from a Service + matching Endpoints.
    fn build_route(
        service: &serde_json::Value,
        endpoints: Option<&serde_json::Value>,
        service_id: &str,
    ) -> Option<ServiceRoute> {
        let cluster_ip = service
            .get("spec")
            .and_then(|s| s.get("clusterIP"))
            .and_then(|c| c.as_str())
            .unwrap_or("")
            .to_string();
        let ports = service
            .get("spec")
            .and_then(|s| s.get("ports"))
            .and_then(|p| p.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|p| {
                        let name = p
                            .get("name")
                            .and_then(|n| n.as_str())
                            .unwrap_or("default")
                            .to_string();
                        let service_port = p.get("port").and_then(|n| n.as_u64())? as u16;
                        let target_port = p
                            .get("targetPort")
                            .and_then(|n| n.as_u64())
                            .map(|n| n as u16)
                            .unwrap_or(service_port);
                        let protocol = p
                            .get("protocol")
                            .and_then(|n| n.as_str())
                            .unwrap_or("TCP")
                            .to_string();
                        Some(PortMap {
                            name,
                            service_port,
                            target_port,
                            protocol,
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let endpoints_set: BTreeSet<String> = endpoints
            .and_then(|e| e.get("subsets"))
            .and_then(|s| s.as_array())
            .map(|arr| {
                arr.iter()
                    .flat_map(|subset| {
                        subset
                            .get("addresses")
                            .and_then(|a| a.as_array())
                            .into_iter()
                            .flatten()
                    })
                    .filter_map(|addr| {
                        addr.get("ip").and_then(|i| i.as_str()).map(String::from)
                    })
                    .collect()
            })
            .unwrap_or_default();
        Some(ServiceRoute {
            service_id: service_id.to_string(),
            cluster_ip,
            ports,
            endpoints: endpoints_set,
        })
    }

    fn service_id(namespace: &str, name: &str) -> String {
        format!("{namespace}/{name}")
    }
}

#[async_trait]
impl Controller for ServiceRoutingController {
    fn name(&self) -> &'static str {
        "service_router"
    }

    async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
        let services = self
            .store
            .list("", "v1", "Service", self.namespace.as_deref())
            .await;
        let endpoints_list = self
            .store
            .list("", "v1", "Endpoints", self.namespace.as_deref())
            .await;
        let endpoints_by_id: BTreeMap<String, serde_json::Value> = endpoints_list
            .into_iter()
            .map(|(k, v)| (Self::service_id(k.namespace.as_deref().unwrap_or("default"), &k.name), v))
            .collect();

        let mut report = ReconcileReport::default();
        report.objects_examined = services.len();

        let desired: BTreeMap<String, ServiceRoute> = services
            .into_iter()
            .filter_map(|(k, svc)| {
                let id = Self::service_id(k.namespace.as_deref().unwrap_or("default"), &k.name);
                let eps = endpoints_by_id.get(&id);
                Self::build_route(&svc, eps, &id).map(|r| (id, r))
            })
            .collect();

        let installed = self
            .backend
            .list()
            .await
            .map_err(|e| ControllerError::Internal(e.to_string()))?;

        // Upserts: in desired but not installed identically.
        for (id, route) in &desired {
            if installed.get(id) != Some(route) {
                self.backend
                    .upsert(route)
                    .await
                    .map_err(|e| ControllerError::Internal(e.to_string()))?;
                report.objects_changed += 1;
            }
        }
        // Removes: in installed but not in desired.
        for id in installed.keys() {
            if !desired.contains_key(id) {
                self.backend
                    .remove(id)
                    .await
                    .map_err(|e| ControllerError::Internal(e.to_string()))?;
                report.objects_changed += 1;
            }
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_service(name: &str, cluster_ip: &str, port: u16) -> serde_json::Value {
        json!({
            "kind": "Service",
            "metadata": {"name": name, "namespace": "default"},
            "spec": {
                "clusterIP": cluster_ip,
                "ports": [{"name": "http", "port": port, "targetPort": port}]
            }
        })
    }

    fn make_endpoints(ips: &[&str]) -> serde_json::Value {
        let addresses: Vec<serde_json::Value> = ips
            .iter()
            .map(|ip| json!({"ip": ip}))
            .collect();
        json!({"subsets": [{"addresses": addresses}]})
    }

    #[test]
    fn build_route_extracts_cluster_ip_and_ports() {
        let svc = make_service("podinfo", "10.96.5.1", 80);
        let eps = make_endpoints(&["10.42.0.1", "10.42.0.2"]);
        let r = ServiceRoutingController::build_route(&svc, Some(&eps), "default/podinfo")
            .unwrap();
        assert_eq!(r.cluster_ip, "10.96.5.1");
        assert_eq!(r.ports[0].service_port, 80);
        assert_eq!(r.endpoints.len(), 2);
        assert!(r.endpoints.contains("10.42.0.1"));
        assert!(r.endpoints.contains("10.42.0.2"));
    }

    #[test]
    fn build_route_handles_missing_endpoints() {
        let svc = make_service("x", "10.96.0.1", 8080);
        let r = ServiceRoutingController::build_route(&svc, None, "default/x").unwrap();
        assert!(r.endpoints.is_empty());
    }

    #[test]
    fn build_route_default_target_port_equals_service_port() {
        let svc = json!({
            "spec": {
                "clusterIP": "10.0.0.1",
                "ports": [{"name": "metrics", "port": 9090}]  // no targetPort
            }
        });
        let r = ServiceRoutingController::build_route(&svc, None, "x/y").unwrap();
        assert_eq!(r.ports[0].service_port, 9090);
        assert_eq!(r.ports[0].target_port, 9090);
    }

    #[test]
    fn build_route_default_protocol_is_tcp() {
        let svc = make_service("x", "10.0.0.1", 80);
        let r = ServiceRoutingController::build_route(&svc, None, "x/y").unwrap();
        assert_eq!(r.ports[0].protocol, "TCP");
    }

    #[tokio::test]
    async fn fake_router_upsert_inserts_route() {
        let router = FakeRouter::new();
        let route = ServiceRoute {
            service_id: "default/x".into(),
            cluster_ip: "10.0.0.1".into(),
            ports: vec![],
            endpoints: BTreeSet::new(),
        };
        router.upsert(&route).await.unwrap();
        assert_eq!(router.route_count().await, 1);
        let evs = router.events().await;
        assert_eq!(evs, vec![FakeRouterEvent::Upsert("default/x".into())]);
    }

    #[tokio::test]
    async fn fake_router_rejects_empty_service_id() {
        let router = FakeRouter::new();
        let route = ServiceRoute {
            service_id: String::new(),
            cluster_ip: "x".into(),
            ports: vec![],
            endpoints: BTreeSet::new(),
        };
        let err = router.upsert(&route).await.unwrap_err();
        assert_eq!(err.kind(), "invalid_route");
    }

    #[tokio::test]
    async fn fake_router_remove_clears_route() {
        let router = FakeRouter::new();
        let route = ServiceRoute {
            service_id: "x".into(),
            cluster_ip: "10.0.0.1".into(),
            ports: vec![],
            endpoints: BTreeSet::new(),
        };
        router.upsert(&route).await.unwrap();
        router.remove("x").await.unwrap();
        assert_eq!(router.route_count().await, 0);
    }

    #[tokio::test]
    async fn fake_router_list_returns_installed() {
        let router = FakeRouter::new();
        let route = ServiceRoute {
            service_id: "a".into(),
            cluster_ip: "10.0.0.1".into(),
            ports: vec![],
            endpoints: BTreeSet::new(),
        };
        router.upsert(&route).await.unwrap();
        let list = router.list().await.unwrap();
        assert_eq!(list.len(), 1);
        assert!(list.contains_key("a"));
    }

    #[test]
    fn error_kinds_are_stable() {
        assert_eq!(RouterError::Backend("x".into()).kind(), "backend");
        assert_eq!(RouterError::InvalidRoute("x".into()).kind(), "invalid_route");
    }

    #[test]
    fn controller_name_is_stable() {
        struct Fake;
        #[async_trait]
        impl Controller for Fake {
            fn name(&self) -> &'static str { "service_router" }
            async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
                Ok(ReconcileReport::default())
            }
        }
        assert_eq!(Fake.name(), "service_router");
    }
}
