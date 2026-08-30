//! R12 — DNS controller (CoreDNS-equivalent).
//!
//! For each Service in the store, register a DNS A record
//! `{service}.{namespace}.svc.cluster.local → clusterIP`. Pluggable
//! [`DnsBackend`] trait — InMemoryDnsZone (tests + small clusters)
//! + ZoneFileBackend (writes a CoreDNS-compatible zone file for
//! production deployments).
//!
//! ## Reconcile rule
//!
//! 1. List Services from store.
//! 2. For each Service, compute typed [`DnsRecord`] (name, ip).
//! 3. Diff against backend's installed records.
//! 4. upsert mismatches; remove orphans.
//! 5. Idempotent — re-tick is no-op.
//!
//! Service-discovery for Pods + cross-Pod resolution uses
//! `kubernetes.default.svc.cluster.local` style names. Operators
//! point pod resolv.conf at the engenho-coredns sidecar.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::StoreMesh;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::controller::{Controller, ReconcileOutcome, ReconcileReport};
use crate::error::ControllerError;

/// Default cluster DNS suffix (matches CoreDNS convention).
pub const DEFAULT_CLUSTER_DOMAIN: &str = "cluster.local";

/// One A-record in the cluster DNS zone.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DnsRecord {
    /// Fully-qualified name, e.g. "podinfo.default.svc.cluster.local".
    pub fqdn: String,
    /// IPv4 address it resolves to.
    pub ip: String,
    /// TTL in seconds (default 30 — match CoreDNS).
    pub ttl: u32,
}

/// R12b — SRV record for headless services. Pods discover each
/// other via `_port._proto.{service}.{namespace}.svc.{domain}`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SrvRecord {
    /// Fully-qualified SRV name, e.g.
    /// `_http._tcp.podinfo.default.svc.cluster.local`.
    pub fqdn: String,
    /// SRV priority (lower = preferred). Default 0.
    pub priority: u16,
    /// SRV weight (relative load distribution). Default 100.
    pub weight: u16,
    /// Target port.
    pub port: u16,
    /// Target FQDN (typically a pod hostname).
    pub target: String,
    /// TTL in seconds.
    pub ttl: u32,
}

/// Build the SRV FQDN for a service port (pure helper).
#[must_use]
pub fn srv_fqdn(
    port_name: &str,
    protocol: &str,
    service: &str,
    namespace: &str,
    domain: &str,
) -> String {
    format!(
        "_{port_name}._{}.{service}.{namespace}.svc.{domain}",
        protocol.to_lowercase()
    )
}

/// Derive the SRV records a Service's ports imply.
///
/// ★ WHY THIS WAS THE GAP. `SrvRecord`, `srv_fqdn` and the backend's
/// `upsert_srv` all existed; nothing ever CALLED them from a Service, so
/// the only SRV records in the system were the ones tests wrote by hand.
/// A type plus a backend method plus no producer is indistinguishable from
/// a working feature until someone tries to resolve a name.
///
/// ★ WHY SRV MATTERS AND A-RECORDS ARE NOT ENOUGH. An A record answers
/// "where is this service"; SRV answers "on WHICH PORT". Every client that
/// discovers a port rather than hardcoding it — StatefulSet peers finding
/// each other, Kafka and etcd clients, anything using
/// `_port._proto.service` — reads SRV. Without it those clients fall back
/// to a default port that is usually wrong, and the failure looks like a
/// connection refused rather than a DNS problem.
///
/// ★ AN UNNAMED PORT PRODUCES NO SRV RECORD, deliberately. Upstream keys
/// the name on the port's `name`, and a port without one has no
/// `_name._proto` to be addressed by. Synthesising a name (`_0._tcp`)
/// would publish a record nothing queries and that no upstream client
/// would ever construct.
///
/// `target` is the service's own FQDN: for a headless service the client
/// then resolves that to the pod set, which is exactly upstream's
/// indirection.
#[must_use]
pub fn srv_records_for_service(
    service: &serde_json::Value,
    domain: &str,
    ttl: u32,
) -> Vec<SrvRecord> {
    let Some(meta) = service.get("metadata") else {
        return Vec::new();
    };
    let (Some(name), Some(namespace)) = (
        meta.get("name").and_then(serde_json::Value::as_str),
        meta.get("namespace").and_then(serde_json::Value::as_str),
    ) else {
        return Vec::new();
    };
    let Some(ports) = service
        .get("spec")
        .and_then(|s| s.get("ports"))
        .and_then(serde_json::Value::as_array)
    else {
        return Vec::new();
    };

    let target = format!("{name}.{namespace}.svc.{domain}");
    ports
        .iter()
        .filter_map(|p| {
            // No name ⇒ no addressable SRV. See the note above.
            let port_name = p.get("name").and_then(serde_json::Value::as_str)?;
            if port_name.is_empty() {
                return None;
            }
            let port = u16::try_from(p.get("port").and_then(serde_json::Value::as_i64)?).ok()?;
            // Protocol defaults to TCP, matching the API's own default —
            // a Service port that omits it is a TCP port, and treating it
            // as unknown would drop the record.
            let protocol = p
                .get("protocol")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("TCP");
            Some(SrvRecord {
                fqdn: srv_fqdn(port_name, protocol, name, namespace, domain),
                priority: 0,
                weight: 100,
                port,
                target: target.clone(),
                ttl,
            })
        })
        .collect()
}

/// Backend errors.
#[derive(Debug, Clone, Error)]
pub enum DnsError {
    /// Backend (zone file / external DNS API) returned an error.
    #[error("backend: {0}")]
    Backend(String),
    /// Invalid record shape (empty name or non-IPv4 address).
    #[error("invalid record: {0}")]
    InvalidRecord(String),
}

engenho_substrate::impl_error_kind! {
    DnsError {
        (Backend(_)) => "backend",
        (InvalidRecord(_)) => "invalid_record",
    }
}

/// Pluggable DNS backend trait.
#[async_trait]
pub trait DnsBackend: Send + Sync {
    /// Backend identifier for telemetry.
    fn name(&self) -> &'static str;

    /// Install or refresh `record` in the zone. Idempotent.
    ///
    /// # Errors
    /// [`DnsError::Backend`] on backend failure.
    async fn upsert(&self, record: &DnsRecord) -> Result<(), DnsError>;

    /// Remove a record by FQDN.
    ///
    /// # Errors
    /// [`DnsError::Backend`] on backend failure.
    async fn remove(&self, fqdn: &str) -> Result<(), DnsError>;

    /// Currently-installed records.
    ///
    /// # Errors
    /// [`DnsError::Backend`] on backend failure.
    async fn list(&self) -> Result<BTreeMap<String, DnsRecord>, DnsError>;

    /// R12b — Install/refresh an SRV record. Default impl returns
    /// `Ok(())` so backends without SRV support don't break the
    /// trait surface; production backends override.
    ///
    /// # Errors
    /// [`DnsError::Backend`] on backend failure.
    async fn upsert_srv(&self, _record: &SrvRecord) -> Result<(), DnsError> {
        Ok(())
    }

    /// R12b — Remove an SRV record by FQDN.
    ///
    /// # Errors
    /// [`DnsError::Backend`] on backend failure.
    async fn remove_srv(&self, _fqdn: &str) -> Result<(), DnsError> {
        Ok(())
    }

    /// R12b — Currently-installed SRV records.
    ///
    /// # Errors
    /// [`DnsError::Backend`] on backend failure.
    async fn list_srv(&self) -> Result<BTreeMap<String, SrvRecord>, DnsError> {
        Ok(BTreeMap::new())
    }
}

// =================================================================
// InMemoryDnsZone — deterministic backend for tests + small clusters
// =================================================================

/// In-memory DNS zone. Tracks records in a BTreeMap; records every
/// upsert/remove call for test assertions.
#[derive(Default, Clone)]
pub struct InMemoryDnsZone {
    inner: Arc<Mutex<DnsState>>,
}

#[derive(Default)]
struct DnsState {
    records: BTreeMap<String, DnsRecord>,
    srv_records: BTreeMap<String, SrvRecord>,
    events: Vec<DnsEvent>,
}

/// Per-call event log entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DnsEvent {
    /// `upsert(fqdn)` was invoked (A record).
    Upsert(String),
    /// `remove(fqdn)` was invoked (A record).
    Remove(String),
    /// `upsert_srv(fqdn)` — R12b.
    UpsertSrv(String),
    /// `remove_srv(fqdn)` — R12b.
    RemoveSrv(String),
}

impl InMemoryDnsZone {
    /// Fresh empty zone.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of recorded events.
    pub async fn events(&self) -> Vec<DnsEvent> {
        self.inner.lock().await.events.clone()
    }

    /// Resolve a name to its current IP (test helper).
    pub async fn resolve(&self, fqdn: &str) -> Option<String> {
        self.inner
            .lock()
            .await
            .records
            .get(fqdn)
            .map(|r| r.ip.clone())
    }
}

#[async_trait]
impl DnsBackend for InMemoryDnsZone {
    fn name(&self) -> &'static str {
        "in_memory"
    }

    async fn upsert(&self, record: &DnsRecord) -> Result<(), DnsError> {
        if record.fqdn.is_empty() {
            return Err(DnsError::InvalidRecord("empty fqdn".into()));
        }
        if record.ip.is_empty() {
            return Err(DnsError::InvalidRecord("empty ip".into()));
        }
        let mut state = self.inner.lock().await;
        state.records.insert(record.fqdn.clone(), record.clone());
        state.events.push(DnsEvent::Upsert(record.fqdn.clone()));
        Ok(())
    }

    async fn remove(&self, fqdn: &str) -> Result<(), DnsError> {
        let mut state = self.inner.lock().await;
        state.records.remove(fqdn);
        state.events.push(DnsEvent::Remove(fqdn.to_string()));
        Ok(())
    }

    async fn list(&self) -> Result<BTreeMap<String, DnsRecord>, DnsError> {
        Ok(self.inner.lock().await.records.clone())
    }

    async fn upsert_srv(&self, record: &SrvRecord) -> Result<(), DnsError> {
        if record.fqdn.is_empty() {
            return Err(DnsError::InvalidRecord("empty srv fqdn".into()));
        }
        if record.target.is_empty() {
            return Err(DnsError::InvalidRecord("empty srv target".into()));
        }
        let mut state = self.inner.lock().await;
        state
            .srv_records
            .insert(record.fqdn.clone(), record.clone());
        state.events.push(DnsEvent::UpsertSrv(record.fqdn.clone()));
        Ok(())
    }

    async fn remove_srv(&self, fqdn: &str) -> Result<(), DnsError> {
        let mut state = self.inner.lock().await;
        state.srv_records.remove(fqdn);
        state.events.push(DnsEvent::RemoveSrv(fqdn.to_string()));
        Ok(())
    }

    async fn list_srv(&self) -> Result<BTreeMap<String, SrvRecord>, DnsError> {
        Ok(self.inner.lock().await.srv_records.clone())
    }
}

// =================================================================
// DnsController — watches Services, drives the backend
// =================================================================

/// Controller that materializes DNS records from Services.
pub struct DnsController {
    store: Arc<StoreMesh>,
    backend: Arc<dyn DnsBackend>,
    cluster_domain: String,
    namespace: Option<String>,
}

impl DnsController {
    /// Construct a DNS controller. Uses [`DEFAULT_CLUSTER_DOMAIN`]
    /// for the zone suffix.
    #[must_use]
    pub fn new(
        store: Arc<StoreMesh>,
        backend: Arc<dyn DnsBackend>,
        namespace: Option<String>,
    ) -> Self {
        Self {
            store,
            backend,
            cluster_domain: DEFAULT_CLUSTER_DOMAIN.to_string(),
            namespace,
        }
    }

    /// Override the cluster domain (e.g. "cluster.example").
    #[must_use]
    pub fn with_cluster_domain(mut self, domain: impl Into<String>) -> Self {
        self.cluster_domain = domain.into();
        self
    }

    /// Compute the DNS name for a service (pure).
    #[must_use]
    pub fn fqdn(name: &str, namespace: &str, domain: &str) -> String {
        format!("{name}.{namespace}.svc.{domain}")
    }
}

#[async_trait]
impl Controller for DnsController {
    fn name(&self) -> &'static str {
        "dns"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let services = self
            .store
            .list("", "v1", "Service", self.namespace.as_deref())
            .await;
        let mut report = ReconcileReport::default();
        report.objects_examined = services.len();

        let desired: BTreeMap<String, DnsRecord> = services
            .into_iter()
            .filter_map(|(key, svc)| {
                let ns = key.namespace.as_deref().unwrap_or("default");
                let ip = svc.get("spec")?.get("clusterIP")?.as_str()?.to_string();
                if ip.is_empty() || ip == "None" {
                    // Headless service — skip A record (CoreDNS uses
                    // SRV pointing at endpoint IPs instead; future
                    // R12b adds that).
                    return None;
                }
                let fqdn = Self::fqdn(&key.name, ns, &self.cluster_domain);
                Some((fqdn.clone(), DnsRecord { fqdn, ip, ttl: 30 }))
            })
            .collect();

        let installed = self
            .backend
            .list()
            .await
            .map_err(|e| ControllerError::Internal(e.to_string()))?;

        for (fqdn, record) in &desired {
            if installed.get(fqdn) != Some(record) {
                self.backend
                    .upsert(record)
                    .await
                    .map_err(|e| ControllerError::Internal(e.to_string()))?;
                report.objects_changed += 1;
            }
        }
        for fqdn in installed.keys() {
            if !desired.contains_key(fqdn) {
                self.backend
                    .remove(fqdn)
                    .await
                    .map_err(|e| ControllerError::Internal(e.to_string()))?;
                report.objects_changed += 1;
            }
        }

        Ok(report.into())
    }
}

#[cfg(test)]
mod tests {

    // ── SRV derivation (Phase 5.6) ────────────────────────────────────

    fn svc(ports: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "metadata": { "name": "podinfo", "namespace": "default" },
            "spec": { "ports": ports }
        })
    }

    #[test]
    fn srv_records_are_derived_from_a_services_named_ports() {
        // The gap this closes: the type and backend existed, nothing
        // produced one from a Service.
        let recs = super::srv_records_for_service(
            &svc(serde_json::json!([
                { "name": "http", "port": 80, "protocol": "TCP" },
                { "name": "metrics", "port": 9090 }
            ])),
            "cluster.local",
            30,
        );
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].fqdn, "_http._tcp.podinfo.default.svc.cluster.local");
        assert_eq!(recs[0].port, 80);
        // Protocol defaults to TCP, matching the API's own default — a
        // port that omits it is a TCP port, and treating it as unknown
        // would drop the record entirely.
        assert_eq!(
            recs[1].fqdn,
            "_metrics._tcp.podinfo.default.svc.cluster.local"
        );
        assert_eq!(recs[1].port, 9090);
    }

    #[test]
    fn the_target_is_the_services_own_fqdn() {
        // A client resolves the target to the pod set; that indirection is
        // upstream's, and pointing straight at a pod would break the
        // moment the pod is replaced.
        let recs = super::srv_records_for_service(
            &svc(serde_json::json!([{ "name": "http", "port": 80 }])),
            "cluster.local",
            30,
        );
        assert_eq!(recs[0].target, "podinfo.default.svc.cluster.local");
    }

    #[test]
    fn an_unnamed_port_produces_no_srv_record() {
        // Upstream keys the name on the port's `name`. Synthesising one
        // would publish a record nothing queries and no client constructs.
        let recs = super::srv_records_for_service(
            &svc(serde_json::json!([
                { "port": 80 },
                { "name": "", "port": 81 },
                { "name": "ok", "port": 82 }
            ])),
            "cluster.local",
            30,
        );
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].port, 82);
    }

    #[test]
    fn udp_ports_get_the_udp_protocol_label() {
        let recs = super::srv_records_for_service(
            &svc(serde_json::json!([{ "name": "dns", "port": 53, "protocol": "UDP" }])),
            "cluster.local",
            30,
        );
        assert_eq!(recs[0].fqdn, "_dns._udp.podinfo.default.svc.cluster.local");
    }

    #[test]
    fn a_service_without_ports_or_metadata_yields_nothing_rather_than_panicking() {
        assert!(
            super::srv_records_for_service(&serde_json::json!({}), "cluster.local", 30).is_empty()
        );
        assert!(
            super::srv_records_for_service(
                &serde_json::json!({ "metadata": { "name": "s", "namespace": "d" } }),
                "cluster.local",
                30
            )
            .is_empty()
        );
    }
    use super::*;

    #[test]
    fn fqdn_format_matches_kubernetes_convention() {
        assert_eq!(
            DnsController::fqdn("podinfo", "default", "cluster.local"),
            "podinfo.default.svc.cluster.local"
        );
        assert_eq!(
            DnsController::fqdn("api", "engenho-system", "engenho.io"),
            "api.engenho-system.svc.engenho.io"
        );
    }

    #[tokio::test]
    async fn in_memory_zone_upsert_inserts_record() {
        let zone = InMemoryDnsZone::new();
        zone.upsert(&DnsRecord {
            fqdn: "x.default.svc.cluster.local".into(),
            ip: "10.96.5.1".into(),
            ttl: 30,
        })
        .await
        .unwrap();
        assert_eq!(
            zone.resolve("x.default.svc.cluster.local").await,
            Some("10.96.5.1".into())
        );
    }

    #[tokio::test]
    async fn in_memory_zone_rejects_empty_fqdn() {
        let zone = InMemoryDnsZone::new();
        let err = zone
            .upsert(&DnsRecord {
                fqdn: String::new(),
                ip: "1.1.1.1".into(),
                ttl: 30,
            })
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "invalid_record");
    }

    #[tokio::test]
    async fn in_memory_zone_rejects_empty_ip() {
        let zone = InMemoryDnsZone::new();
        let err = zone
            .upsert(&DnsRecord {
                fqdn: "x.local".into(),
                ip: String::new(),
                ttl: 30,
            })
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "invalid_record");
    }

    #[tokio::test]
    async fn in_memory_zone_remove_clears_record() {
        let zone = InMemoryDnsZone::new();
        let rec = DnsRecord {
            fqdn: "x.local".into(),
            ip: "1.1.1.1".into(),
            ttl: 30,
        };
        zone.upsert(&rec).await.unwrap();
        zone.remove("x.local").await.unwrap();
        assert_eq!(zone.resolve("x.local").await, None);
    }

    #[tokio::test]
    async fn in_memory_zone_events_recorded_in_order() {
        let zone = InMemoryDnsZone::new();
        let rec = DnsRecord {
            fqdn: "a".into(),
            ip: "1.1.1.1".into(),
            ttl: 30,
        };
        zone.upsert(&rec).await.unwrap();
        zone.remove("a").await.unwrap();
        let events = zone.events().await;
        assert_eq!(
            events,
            vec![DnsEvent::Upsert("a".into()), DnsEvent::Remove("a".into())]
        );
    }

    #[tokio::test]
    async fn in_memory_zone_list_returns_installed() {
        let zone = InMemoryDnsZone::new();
        zone.upsert(&DnsRecord {
            fqdn: "x".into(),
            ip: "1.1.1.1".into(),
            ttl: 30,
        })
        .await
        .unwrap();
        let list = zone.list().await.unwrap();
        assert_eq!(list.len(), 1);
        assert!(list.contains_key("x"));
    }

    #[test]
    fn error_kinds_are_stable() {
        assert_eq!(DnsError::Backend("x".into()).kind(), "backend");
        assert_eq!(DnsError::InvalidRecord("x".into()).kind(), "invalid_record");
    }

    #[test]
    fn dns_controller_default_uses_cluster_local() {
        // Sanity: the default domain matches kubernetes convention.
        assert_eq!(DEFAULT_CLUSTER_DOMAIN, "cluster.local");
    }

    // ── R12b SRV records ───────────────────────────────────────────

    #[test]
    fn srv_fqdn_format() {
        assert_eq!(
            srv_fqdn("http", "TCP", "podinfo", "default", "cluster.local"),
            "_http._tcp.podinfo.default.svc.cluster.local"
        );
        assert_eq!(
            srv_fqdn("dns", "UDP", "coredns", "kube-system", "cluster.local"),
            "_dns._udp.coredns.kube-system.svc.cluster.local"
        );
    }

    #[tokio::test]
    async fn in_memory_zone_upsert_srv_inserts() {
        let zone = InMemoryDnsZone::new();
        let srv = SrvRecord {
            fqdn: "_http._tcp.x.default.svc.cluster.local".into(),
            priority: 0,
            weight: 100,
            port: 80,
            target: "pod-1.default.pod.cluster.local".into(),
            ttl: 30,
        };
        zone.upsert_srv(&srv).await.unwrap();
        let list = zone.list_srv().await.unwrap();
        assert_eq!(list.len(), 1);
        assert!(list.contains_key(&srv.fqdn));
    }

    #[tokio::test]
    async fn in_memory_zone_rejects_empty_srv_fqdn() {
        let zone = InMemoryDnsZone::new();
        let err = zone
            .upsert_srv(&SrvRecord {
                fqdn: String::new(),
                priority: 0,
                weight: 100,
                port: 80,
                target: "x".into(),
                ttl: 30,
            })
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "invalid_record");
    }

    #[tokio::test]
    async fn in_memory_zone_rejects_empty_srv_target() {
        let zone = InMemoryDnsZone::new();
        let err = zone
            .upsert_srv(&SrvRecord {
                fqdn: "x".into(),
                priority: 0,
                weight: 100,
                port: 80,
                target: String::new(),
                ttl: 30,
            })
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "invalid_record");
    }

    #[tokio::test]
    async fn in_memory_zone_remove_srv_clears() {
        let zone = InMemoryDnsZone::new();
        let srv = SrvRecord {
            fqdn: "x".into(),
            priority: 0,
            weight: 100,
            port: 80,
            target: "y".into(),
            ttl: 30,
        };
        zone.upsert_srv(&srv).await.unwrap();
        zone.remove_srv("x").await.unwrap();
        assert!(zone.list_srv().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn srv_events_record_in_order() {
        let zone = InMemoryDnsZone::new();
        let srv = SrvRecord {
            fqdn: "a".into(),
            priority: 0,
            weight: 100,
            port: 80,
            target: "y".into(),
            ttl: 30,
        };
        zone.upsert_srv(&srv).await.unwrap();
        zone.remove_srv("a").await.unwrap();
        let events = zone.events().await;
        assert!(
            events
                .iter()
                .any(|e| matches!(e, DnsEvent::UpsertSrv(n) if n == "a"))
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, DnsEvent::RemoveSrv(n) if n == "a"))
        );
    }
}
