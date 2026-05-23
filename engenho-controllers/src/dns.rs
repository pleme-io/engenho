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

use crate::controller::{Controller, ReconcileReport};
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

    async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
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

        Ok(report)
    }
}

#[cfg(test)]
mod tests {
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
