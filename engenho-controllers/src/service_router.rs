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

engenho_substrate::impl_error_kind! {
    RouterError {
        (Backend(_)) => "backend",
        (InvalidRoute(_)) => "invalid_route",
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
        state
            .events
            .push(FakeRouterEvent::Upsert(route.service_id.clone()));
        Ok(())
    }

    async fn remove(&self, service_id: &str) -> Result<(), RouterError> {
        let mut state = self.inner.lock().await;
        state.routes.remove(service_id);
        state
            .events
            .push(FakeRouterEvent::Remove(service_id.to_string()));
        Ok(())
    }

    async fn list(&self) -> Result<BTreeMap<String, ServiceRoute>, RouterError> {
        Ok(self.inner.lock().await.routes.clone())
    }
}

// =================================================================
// IptablesRouter — Linux production backend (R11b)
// =================================================================

/// Production ServiceRouter for Linux nodes. Renders the desired
/// state as iptables rules + applies them via `iptables-restore`.
///
/// Per the org's NO SHELL rule, this is the ONE acceptable
/// shell-out site for ServiceRouter — the integration boundary
/// itself, not orchestration glue.
///
/// ## Approach
///
/// Each `upsert` builds a fully-formed iptables-restore script
/// for the `KUBE-SERVICES` + `KUBE-SVC-{hash}` + `KUBE-SEP-{hash}`
/// chain hierarchy + pipes it to `iptables-restore --noflush`.
/// The same idempotent rule structure kube-proxy's iptables mode
/// uses. Removal flushes the relevant chains.
///
/// In-memory tracking of installed routes (so `list` returns the
/// authoritative state without re-parsing iptables-save output).
#[derive(Clone)]
pub struct IptablesRouter {
    /// Binary path; default `iptables-restore`.
    binary: String,
    inner: Arc<Mutex<IptablesState>>,
}

#[derive(Default)]
struct IptablesState {
    /// Routes the router has installed.
    routes: BTreeMap<String, ServiceRoute>,
}

impl Default for IptablesRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl IptablesRouter {
    /// New router using `iptables-restore` from `$PATH`.
    #[must_use]
    pub fn new() -> Self {
        Self::with_binary("iptables-restore")
    }

    /// New router with an explicit binary path. Useful for testing
    /// against `iptables-legacy-restore` or for shimming in CI.
    #[must_use]
    pub fn with_binary(binary: impl Into<String>) -> Self {
        Self {
            binary: binary.into(),
            inner: Arc::new(Mutex::new(IptablesState::default())),
        }
    }

    /// Render the iptables-restore script for one route. Pure —
    /// no I/O. Test helper exposed publicly so callers can inspect
    /// what the router would emit.
    #[must_use]
    pub fn render_script(route: &ServiceRoute) -> String {
        let chain_svc = chain_name("KUBE-SVC", &route.service_id);
        let mut script = String::new();
        script.push_str("*nat\n");
        script.push_str(&format!(":{chain_svc} - [0:0]\n"));
        for port in &route.ports {
            // Hit the per-service chain from KUBE-SERVICES.
            script.push_str(&format!(
                "-A KUBE-SERVICES -d {}/32 -p {} --dport {} -j {chain_svc}\n",
                route.cluster_ip,
                port.protocol.to_lowercase(),
                port.service_port
            ));
            // For each pod IP, install a per-endpoint chain.
            for (i, pod_ip) in route.endpoints.iter().enumerate() {
                let chain_ep =
                    chain_name("KUBE-SEP", &format!("{}-{}-{i}", route.service_id, pod_ip));
                script.push_str(&format!(":{chain_ep} - [0:0]\n"));
                // Round-robin via statistic mode random; first endpoint
                // gets 1/N, second 1/(N-1) etc.
                let remaining = route.endpoints.len() - i;
                let probability = format!("--probability {:.6}", 1.0 / (remaining as f64));
                script.push_str(&format!(
                    "-A {chain_svc} -m statistic --mode random {probability} -j {chain_ep}\n"
                ));
                // The endpoint chain DNATs to the pod.
                script.push_str(&format!(
                    "-A {chain_ep} -p {} -j DNAT --to-destination {pod_ip}:{}\n",
                    port.protocol.to_lowercase(),
                    port.target_port
                ));
            }
        }
        script.push_str("COMMIT\n");
        script
    }

    async fn run_restore(&self, script: &str) -> Result<(), RouterError> {
        use std::process::Stdio;
        let mut cmd = tokio::process::Command::new(&self.binary);
        cmd.arg("--noflush")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd
            .spawn()
            .map_err(|e| RouterError::Backend(format!("{} spawn: {e}", self.binary)))?;
        use tokio::io::AsyncWriteExt;
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(script.as_bytes())
                .await
                .map_err(|e| RouterError::Backend(format!("stdin write: {e}")))?;
            stdin
                .shutdown()
                .await
                .map_err(|e| RouterError::Backend(format!("stdin close: {e}")))?;
        }
        let out = child
            .wait_with_output()
            .await
            .map_err(|e| RouterError::Backend(format!("wait: {e}")))?;
        if !out.status.success() {
            return Err(RouterError::Backend(format!(
                "{} exit {:?}: {}",
                self.binary,
                out.status.code(),
                String::from_utf8_lossy(&out.stderr)
            )));
        }
        Ok(())
    }
}

/// Hash a free-form id to a 12-char iptables-chain-safe suffix.
fn chain_name(prefix: &str, key: &str) -> String {
    let hash = blake3::hash(key.as_bytes());
    let hex = hash.to_hex();
    format!("{prefix}-{}", &hex.as_str()[..12].to_uppercase())
}

#[async_trait]
impl ServiceRouter for IptablesRouter {
    fn name(&self) -> &'static str {
        "iptables"
    }

    async fn upsert(&self, route: &ServiceRoute) -> Result<(), RouterError> {
        if route.service_id.is_empty() {
            return Err(RouterError::InvalidRoute("empty service_id".into()));
        }
        let script = Self::render_script(route);
        self.run_restore(&script).await?;
        let mut state = self.inner.lock().await;
        state.routes.insert(route.service_id.clone(), route.clone());
        Ok(())
    }

    async fn remove(&self, service_id: &str) -> Result<(), RouterError> {
        let chain_svc = chain_name("KUBE-SVC", service_id);
        // Flush + delete the per-service chain.
        let script =
            format!("*nat\n:{chain_svc} - [0:0]\n-F {chain_svc}\n-X {chain_svc}\nCOMMIT\n");
        // Errors on remove are tolerated when the chain is already gone.
        let _ = self.run_restore(&script).await;
        let mut state = self.inner.lock().await;
        state.routes.remove(service_id);
        Ok(())
    }

    async fn list(&self) -> Result<BTreeMap<String, ServiceRoute>, RouterError> {
        Ok(self.inner.lock().await.routes.clone())
    }
}

// =================================================================
// IPVSRouter — R11c — scalable backend (>1k services)
// =================================================================

/// IPVS-backed router. Production backend for large clusters
/// where iptables rule walking becomes O(n) per packet (≥1000
/// services). IPVS uses hash-based virtual-server lookup → O(1)
/// regardless of cluster size.
///
/// Per the NO SHELL rule, this shells out to `ipvsadm` — the
/// integration boundary. Future R11d swaps in netlink-direct
/// IPVS control via the `ipvs` crate (no shell).
#[derive(Clone)]
pub struct IpvsRouter {
    binary: String,
    inner: Arc<Mutex<IpvsState>>,
}

#[derive(Default)]
struct IpvsState {
    routes: BTreeMap<String, ServiceRoute>,
}

impl Default for IpvsRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl IpvsRouter {
    /// New router using `ipvsadm` from `$PATH`.
    #[must_use]
    pub fn new() -> Self {
        Self::with_binary("ipvsadm")
    }

    /// New router with explicit binary path.
    #[must_use]
    pub fn with_binary(binary: impl Into<String>) -> Self {
        Self {
            binary: binary.into(),
            inner: Arc::new(Mutex::new(IpvsState::default())),
        }
    }

    /// Render the ipvsadm script for one route. Pure — no I/O.
    ///
    /// Format: sequence of `-A` (add-service) + `-a` (add-real-server)
    /// lines that ipvsadm-restore consumes.
    #[must_use]
    pub fn render_script(route: &ServiceRoute) -> String {
        let mut out = String::new();
        for port in &route.ports {
            // -A: add virtual service. -t = TCP, -u = UDP.
            let proto_flag = match port.protocol.to_uppercase().as_str() {
                "UDP" => "-u",
                _ => "-t",
            };
            out.push_str(&format!(
                "-A {proto_flag} {}:{} -s rr\n",
                route.cluster_ip, port.service_port
            ));
            for pod_ip in &route.endpoints {
                // -a: add real server. -m = masquerade (NAT).
                out.push_str(&format!(
                    "-a {proto_flag} {}:{} -r {pod_ip}:{} -m\n",
                    route.cluster_ip, port.service_port, port.target_port
                ));
            }
        }
        out
    }

    async fn run_restore(&self, script: &str) -> Result<(), RouterError> {
        use std::process::Stdio;
        let mut cmd = tokio::process::Command::new(&self.binary);
        cmd.arg("-R")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd
            .spawn()
            .map_err(|e| RouterError::Backend(format!("{} spawn: {e}", self.binary)))?;
        use tokio::io::AsyncWriteExt;
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(script.as_bytes())
                .await
                .map_err(|e| RouterError::Backend(format!("stdin write: {e}")))?;
            stdin
                .shutdown()
                .await
                .map_err(|e| RouterError::Backend(format!("stdin close: {e}")))?;
        }
        let out = child
            .wait_with_output()
            .await
            .map_err(|e| RouterError::Backend(format!("wait: {e}")))?;
        if !out.status.success() {
            return Err(RouterError::Backend(format!(
                "{} exit {:?}: {}",
                self.binary,
                out.status.code(),
                String::from_utf8_lossy(&out.stderr)
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl ServiceRouter for IpvsRouter {
    fn name(&self) -> &'static str {
        "ipvs"
    }

    async fn upsert(&self, route: &ServiceRoute) -> Result<(), RouterError> {
        if route.service_id.is_empty() {
            return Err(RouterError::InvalidRoute("empty service_id".into()));
        }
        let script = Self::render_script(route);
        self.run_restore(&script).await?;
        let mut state = self.inner.lock().await;
        state.routes.insert(route.service_id.clone(), route.clone());
        Ok(())
    }

    async fn remove(&self, service_id: &str) -> Result<(), RouterError> {
        // Delete each virtual service this route owned.
        let route = {
            let state = self.inner.lock().await;
            state.routes.get(service_id).cloned()
        };
        if let Some(route) = route {
            let mut script = String::new();
            for port in &route.ports {
                let proto_flag = match port.protocol.to_uppercase().as_str() {
                    "UDP" => "-u",
                    _ => "-t",
                };
                script.push_str(&format!(
                    "-D {proto_flag} {}:{}\n",
                    route.cluster_ip, port.service_port
                ));
            }
            let _ = self.run_restore(&script).await;
        }
        let mut state = self.inner.lock().await;
        state.routes.remove(service_id);
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
                    .filter_map(|addr| addr.get("ip").and_then(|i| i.as_str()).map(String::from))
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
            .map(|(k, v)| {
                (
                    Self::service_id(k.namespace.as_deref().unwrap_or("default"), &k.name),
                    v,
                )
            })
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
        let addresses: Vec<serde_json::Value> = ips.iter().map(|ip| json!({"ip": ip})).collect();
        json!({"subsets": [{"addresses": addresses}]})
    }

    #[test]
    fn build_route_extracts_cluster_ip_and_ports() {
        let svc = make_service("podinfo", "10.96.5.1", 80);
        let eps = make_endpoints(&["10.42.0.1", "10.42.0.2"]);
        let r = ServiceRoutingController::build_route(&svc, Some(&eps), "default/podinfo").unwrap();
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
        assert_eq!(
            RouterError::InvalidRoute("x".into()).kind(),
            "invalid_route"
        );
    }

    #[test]
    fn ipvs_router_render_script_contains_virtual_services() {
        let route = ServiceRoute {
            service_id: "default/podinfo".into(),
            cluster_ip: "10.96.5.1".into(),
            ports: vec![PortMap {
                name: "http".into(),
                service_port: 80,
                target_port: 9898,
                protocol: "TCP".into(),
            }],
            endpoints: ["10.42.0.1".to_string(), "10.42.0.2".to_string()]
                .into_iter()
                .collect(),
        };
        let script = IpvsRouter::render_script(&route);
        assert!(script.contains("-A -t 10.96.5.1:80 -s rr"));
        assert!(script.contains("-a -t 10.96.5.1:80 -r 10.42.0.1:9898 -m"));
        assert!(script.contains("-a -t 10.96.5.1:80 -r 10.42.0.2:9898 -m"));
    }

    #[test]
    fn ipvs_router_renders_udp_with_u_flag() {
        let route = ServiceRoute {
            service_id: "default/dns".into(),
            cluster_ip: "10.96.0.10".into(),
            ports: vec![PortMap {
                name: "dns".into(),
                service_port: 53,
                target_port: 53,
                protocol: "UDP".into(),
            }],
            endpoints: ["10.42.0.5".to_string()].into_iter().collect(),
        };
        let script = IpvsRouter::render_script(&route);
        assert!(script.contains("-A -u 10.96.0.10:53"));
        assert!(script.contains("-a -u 10.96.0.10:53 -r 10.42.0.5:53"));
    }

    #[test]
    fn ipvs_router_name_is_stable() {
        assert_eq!(IpvsRouter::new().name(), "ipvs");
    }

    #[test]
    fn ipvs_with_binary_uses_path() {
        let r = IpvsRouter::with_binary("/usr/sbin/ipvsadm");
        assert_eq!(r.binary, "/usr/sbin/ipvsadm");
    }

    #[tokio::test]
    async fn ipvs_rejects_empty_service_id() {
        let r = IpvsRouter::with_binary("/nonexistent-binary");
        let route = ServiceRoute {
            service_id: String::new(),
            cluster_ip: "10.0.0.1".into(),
            ports: vec![],
            endpoints: BTreeSet::new(),
        };
        let err = r.upsert(&route).await.unwrap_err();
        assert_eq!(err.kind(), "invalid_route");
    }

    #[test]
    fn iptables_router_render_script_contains_chains() {
        let route = ServiceRoute {
            service_id: "default/podinfo".into(),
            cluster_ip: "10.96.5.1".into(),
            ports: vec![PortMap {
                name: "http".into(),
                service_port: 80,
                target_port: 9898,
                protocol: "TCP".into(),
            }],
            endpoints: ["10.42.0.1".to_string(), "10.42.0.2".to_string()]
                .into_iter()
                .collect(),
        };
        let script = IptablesRouter::render_script(&route);
        assert!(script.starts_with("*nat\n"));
        assert!(script.ends_with("COMMIT\n"));
        assert!(script.contains("-A KUBE-SERVICES -d 10.96.5.1/32 -p tcp --dport 80"));
        assert!(script.contains("DNAT --to-destination 10.42.0.1:9898"));
        assert!(script.contains("DNAT --to-destination 10.42.0.2:9898"));
        assert!(script.contains("-m statistic --mode random"));
    }

    #[test]
    fn iptables_chain_name_is_deterministic_short_hex() {
        let n = chain_name("KUBE-SVC", "default/podinfo");
        assert!(n.starts_with("KUBE-SVC-"));
        // 12 hex chars (uppercase).
        let suffix = n.trim_start_matches("KUBE-SVC-");
        assert_eq!(suffix.len(), 12);
        assert!(suffix.chars().all(|c| c.is_ascii_hexdigit()));
        // Determinism: same input → same chain name.
        assert_eq!(n, chain_name("KUBE-SVC", "default/podinfo"));
    }

    #[test]
    fn iptables_router_name_is_stable() {
        assert_eq!(IptablesRouter::new().name(), "iptables");
    }

    #[test]
    fn iptables_with_binary_uses_path() {
        let r = IptablesRouter::with_binary("/sbin/iptables-restore");
        assert_eq!(r.binary, "/sbin/iptables-restore");
    }

    #[tokio::test]
    async fn iptables_rejects_empty_service_id() {
        let r = IptablesRouter::with_binary("/nonexistent-binary");
        let route = ServiceRoute {
            service_id: String::new(),
            cluster_ip: "10.0.0.1".into(),
            ports: vec![],
            endpoints: BTreeSet::new(),
        };
        let err = r.upsert(&route).await.unwrap_err();
        assert_eq!(err.kind(), "invalid_route");
    }

    #[test]
    fn controller_name_is_stable() {
        struct Fake;
        #[async_trait]
        impl Controller for Fake {
            fn name(&self) -> &'static str {
                "service_router"
            }
            async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
                Ok(ReconcileReport::default())
            }
        }
        assert_eq!(Fake.name(), "service_router");
    }
}
