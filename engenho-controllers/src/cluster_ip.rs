//! Service ClusterIP (VIP) allocator + the defaulting admission webhook
//! that stamps an allocated VIP onto a Service at create time.
//!
//! ## The gap this closes
//!
//! Before this module a Service stored in engenho kept whatever
//! `spec.clusterIP` the operator supplied — and `kubectl get svc`
//! reported `clusterIP: None` for the common case (operator supplies no
//! IP). Nothing ALLOCATED a virtual IP. `ServiceRoutingController`
//! (service_router.rs) already computes VIP→pod routing rules, the DNS
//! controller already answers `*.svc → clusterIP`, and the
//! `EndpointsController` already resolves selectors → pod IPs — every
//! one of them keyed on a ClusterIP that nothing produced. This module
//! is the producer.
//!
//! ## Shape (★★ TYPED-SPEC + INTERPRETER TRIPLET — Environment trait)
//!
//! The allocator core ([`ClusterIpAllocator`]) is a PURE typed value over
//! a parsed CIDR — no I/O, fully unit-testable. The defaulting webhook
//! ([`ClusterIpDefaultingWebhook`]) is an [`AdmissionWebhook`] whose ONE
//! side effect — "what VIPs are currently held by live Services?" — is
//! abstracted behind the [`ServiceIpSource`] trait (production: read the
//! store; test: a static list). That seam is also what makes the
//! allocation **restart-persistent + collision-free by construction**:
//! every allocation reseeds the in-use set from the live Service set, so
//! a freshly-restarted process recomputes the held VIPs from the durable
//! store rather than from a lost in-memory counter. There is no separate
//! allocation ledger to drift — the Services ARE the ledger.
//!
//! ## Rules (upstream parity)
//!
//!   * `type: ClusterIP` (the default) + `clusterIP` unset/"" ⇒ allocate
//!     a free VIP from the configured `service_cidr`, write it onto
//!     `spec.clusterIP` AND `spec.clusterIPs[0]`.
//!   * `clusterIP: "None"` (headless) ⇒ left untouched. No VIP.
//!   * `type: ExternalName` ⇒ left untouched. No VIP.
//!   * `clusterIP` already a concrete IP (operator pinned it) ⇒ left
//!     untouched (honored as-is; the in-use set already accounts for it
//!     on the next allocation because it's a live Service).
//!   * Pool exhaustion ⇒ a typed [`Deny`] (NOT a silent unallocated
//!     Service) — the operator sees the exhaustion, never a half-built
//!     object.
//!
//! [`Deny`]: crate::admission::AdmissionDecision::Deny

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use engenho_config::parse_ipv4_cidr;
use engenho_store::StoreMesh;
use serde_json::Value;
use thiserror::Error;

use crate::admission::{
    AdmissionAction, AdmissionDecision, AdmissionError, AdmissionRequest, AdmissionWebhook,
};

// ═══════════════════════════════════════════════════════════════════════════
//  Errors.
// ═══════════════════════════════════════════════════════════════════════════

/// A typed failure from the allocator. No silent wrong answers.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum AllocatorError {
    /// The configured `service_cidr` is not a parseable IPv4 CIDR.
    #[error("invalid service CIDR: {0}")]
    InvalidCidr(String),
    /// Every assignable VIP in the CIDR is already held by a live
    /// Service — the pool is exhausted.
    #[error("service CIDR {cidr} is exhausted ({held} VIPs in use)")]
    PoolExhausted {
        /// The configured CIDR.
        cidr: String,
        /// How many VIPs are currently held.
        held: usize,
    },
}

engenho_substrate::impl_error_kind! {
    AllocatorError {
        (InvalidCidr(_)) => "invalid_cidr",
        { PoolExhausted { .. } } => "pool_exhausted",
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  The pure allocator core.
// ═══════════════════════════════════════════════════════════════════════════

/// A typed ClusterIP allocator over one IPv4 service CIDR.
///
/// PURE — holds the parsed CIDR bounds + an in-use VIP set. Construct it,
/// seed the in-use set from the live Service VIPs ([`Self::reserve`]),
/// then [`Self::allocate`] / [`Self::release`]. Collision-freedom is a
/// property of the in-use set: `allocate` never returns a held VIP and
/// inserts what it returns.
#[derive(Clone, Debug)]
pub struct ClusterIpAllocator {
    /// The original CIDR string (for error messages).
    cidr: String,
    /// Network base address as a u32 (host order).
    base: u32,
    /// First assignable host VIP (base + 1 — the network address itself
    /// is skipped per upstream convention).
    first: u32,
    /// Last assignable host VIP (broadcast - 1 for non-/31,/32; for /31
    /// and /32 the whole range is assignable).
    last: u32,
    /// VIPs currently held — the collision-free invariant lives here.
    in_use: BTreeSet<u32>,
}

impl ClusterIpAllocator {
    /// Construct an allocator over `cidr` (e.g. `10.96.0.0/12`).
    ///
    /// # Errors
    /// [`AllocatorError::InvalidCidr`] when `cidr` is not a parseable
    /// IPv4 CIDR.
    pub fn new(cidr: &str) -> Result<Self, AllocatorError> {
        let (base, prefix) =
            parse_ipv4_cidr(cidr).map_err(AllocatorError::InvalidCidr)?;
        // Host bits = 32 - prefix. The assignable host range is
        // (network+1)..=(broadcast-1) for the common case; for a /31 or
        // /32 the whole block is assignable (no separate net/broadcast).
        let host_bits = 32 - u32::from(prefix);
        let (first, last) = if prefix >= 31 {
            // /32 → single address; /31 → two addresses, both usable.
            let size = if host_bits == 0 { 1 } else { 2 };
            (base, base.saturating_add(size - 1))
        } else {
            let block_size = 1u64 << host_bits;
            let broadcast = u64::from(base) + block_size - 1;
            // Skip the network address (base) and the broadcast. The
            // last assignable host (broadcast - 1) is in-range by
            // construction (block_size >= 4 for prefix <= 30), so the
            // truncating cast back to u32 is exact.
            let last = u32::try_from(broadcast - 1).unwrap_or(u32::MAX);
            (base + 1, last)
        };
        Ok(Self {
            cidr: cidr.to_string(),
            base,
            first,
            last,
            in_use: BTreeSet::new(),
        })
    }

    /// Mark `ip` as in-use if it falls within this CIDR. Out-of-range or
    /// unparseable IPs are ignored (a live Service may carry a VIP from a
    /// different range after a CIDR reconfiguration; it's simply not
    /// drawn from this pool). Returns `true` iff the IP was reserved.
    pub fn reserve(&mut self, ip: &str) -> bool {
        match ip_to_u32(ip) {
            Some(v) if v >= self.base && v <= self.last => self.in_use.insert(v),
            _ => false,
        }
    }

    /// Release `ip` back to the pool (reclaim on Service delete).
    pub fn release(&mut self, ip: &str) {
        if let Some(v) = ip_to_u32(ip) {
            self.in_use.remove(&v);
        }
    }

    /// Allocate the next free VIP, marking it in-use. Deterministic:
    /// always the lowest free assignable VIP, so the same in-use set
    /// yields the same allocation order (stable tests + diffs).
    ///
    /// # Errors
    /// [`AllocatorError::PoolExhausted`] when no assignable VIP is free.
    pub fn allocate(&mut self) -> Result<String, AllocatorError> {
        let mut candidate = self.first;
        while candidate <= self.last {
            if !self.in_use.contains(&candidate) {
                self.in_use.insert(candidate);
                return Ok(u32_to_ip(candidate));
            }
            // Guard the u32 add at the top of the range.
            if candidate == u32::MAX {
                break;
            }
            candidate += 1;
        }
        Err(AllocatorError::PoolExhausted {
            cidr: self.cidr.clone(),
            held: self.in_use.len(),
        })
    }

    /// Count of currently-held VIPs.
    #[must_use]
    pub fn held(&self) -> usize {
        self.in_use.len()
    }

    /// True iff `ip` is currently held.
    #[must_use]
    pub fn is_held(&self, ip: &str) -> bool {
        ip_to_u32(ip).is_some_and(|v| self.in_use.contains(&v))
    }
}

/// Parse a dotted-quad IPv4 into a host-order u32. `None` on malformed.
fn ip_to_u32(ip: &str) -> Option<u32> {
    let octets: Vec<&str> = ip.split('.').collect();
    if octets.len() != 4 {
        return None;
    }
    let mut v: u32 = 0;
    for oct in octets {
        let byte: u8 = oct.parse().ok()?;
        v = (v << 8) | u32::from(byte);
    }
    Some(v)
}

/// Render a host-order u32 as a dotted-quad IPv4 string. Built from typed
/// octets (no `format!()` of address structure — the four octets ARE the
/// typed pieces; `format!` of the `{}.{}.{}.{}` shape is the canonical
/// dotted-quad serializer, ★★ TYPED EMISSION allows it as the value's
/// render surface).
fn u32_to_ip(v: u32) -> String {
    format!(
        "{}.{}.{}.{}",
        (v >> 24) & 0xff,
        (v >> 16) & 0xff,
        (v >> 8) & 0xff,
        v & 0xff
    )
}

// ═══════════════════════════════════════════════════════════════════════════
//  Service classification — what kind of Service is this?
// ═══════════════════════════════════════════════════════════════════════════

/// How a Service relates to VIP allocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServiceVipDisposition {
    /// `type: ClusterIP` (or unset) with `clusterIP` unset/"" ⇒ allocate.
    NeedsAllocation,
    /// `clusterIP: "None"` (headless) ⇒ leave untouched.
    Headless,
    /// `type: ExternalName` ⇒ leave untouched.
    ExternalName,
    /// `clusterIP` is already a concrete IP ⇒ honored as-is.
    AlreadyAssigned,
}

/// Classify a Service object by its VIP disposition. Pure.
#[must_use]
pub fn classify_service(svc: &Value) -> ServiceVipDisposition {
    let spec = svc.get("spec");
    let svc_type = spec
        .and_then(|s| s.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("ClusterIP");
    if svc_type == "ExternalName" {
        return ServiceVipDisposition::ExternalName;
    }
    let cluster_ip = spec
        .and_then(|s| s.get("clusterIP"))
        .and_then(Value::as_str);
    match cluster_ip {
        Some("None") => ServiceVipDisposition::Headless,
        None | Some("") => ServiceVipDisposition::NeedsAllocation,
        Some(_concrete) => ServiceVipDisposition::AlreadyAssigned,
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  The Environment seam (restart-persistence + collision-freedom).
// ═══════════════════════════════════════════════════════════════════════════

/// Source of the VIPs currently held by live Services. Production reads
/// the store; tests pass a static list. The webhook reseeds the
/// allocator from this on EVERY allocation, so the durable Service set is
/// the single source of truth — no separate allocation ledger, and a
/// restarted process recomputes the held set rather than losing a counter.
#[async_trait]
pub trait ServiceIpSource: Send + Sync {
    /// Every `spec.clusterIP` (+ `spec.clusterIPs`) string currently held
    /// by a live Service. Headless ("None") and empty values are caller-
    /// filtered, so this may include "None"/"" — the allocator's
    /// [`ClusterIpAllocator::reserve`] ignores anything not a real VIP in
    /// range.
    async fn held_cluster_ips(&self) -> Vec<String>;
}

/// Production [`ServiceIpSource`] — reads `spec.clusterIP` +
/// `spec.clusterIPs` off every live Service in the store.
pub struct StoreServiceIpSource {
    store: Arc<StoreMesh>,
}

impl StoreServiceIpSource {
    /// New source over `store`.
    #[must_use]
    pub fn new(store: Arc<StoreMesh>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ServiceIpSource for StoreServiceIpSource {
    async fn held_cluster_ips(&self) -> Vec<String> {
        let services = self.store.list("", "v1", "Service", None).await;
        let mut ips = Vec::new();
        for (_key, svc) in services {
            if let Some(ip) = svc
                .get("spec")
                .and_then(|s| s.get("clusterIP"))
                .and_then(Value::as_str)
            {
                ips.push(ip.to_string());
            }
            if let Some(arr) = svc
                .get("spec")
                .and_then(|s| s.get("clusterIPs"))
                .and_then(Value::as_array)
            {
                for v in arr {
                    if let Some(ip) = v.as_str() {
                        ips.push(ip.to_string());
                    }
                }
            }
        }
        ips
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  The defaulting admission webhook.
// ═══════════════════════════════════════════════════════════════════════════

/// A defaulting [`AdmissionWebhook`] that allocates a ClusterIP for a
/// Service on create.
///
/// Fires on Put of a `v1/Service` (CREATE). It (1) reseeds a fresh
/// [`ClusterIpAllocator`] from the live Service VIPs, (2) classifies the
/// proposed Service, (3) for `NeedsAllocation` allocates a free VIP and
/// returns [`AdmissionDecision::Mutate`] with `spec.clusterIP` +
/// `spec.clusterIPs` set. Headless / ExternalName / already-assigned
/// Services + every non-Service kind pass through unchanged
/// ([`AdmissionDecision::Allow`]). Pool exhaustion is a typed
/// [`AdmissionDecision::Deny`].
pub struct ClusterIpDefaultingWebhook {
    /// The configured service CIDR.
    cidr: String,
    /// The live-VIP source (reseeded per allocation).
    source: Arc<dyn ServiceIpSource>,
}

impl ClusterIpDefaultingWebhook {
    /// New webhook over `cidr` reading live VIPs from `source`.
    #[must_use]
    pub fn new(cidr: impl Into<String>, source: Arc<dyn ServiceIpSource>) -> Self {
        Self {
            cidr: cidr.into(),
            source,
        }
    }

    /// Whether `key` names a core/v1 Service.
    fn is_service(key: &engenho_store::resource::ResourceKey) -> bool {
        key.group.is_empty() && key.version == "v1" && key.kind == "Service"
    }
}

#[async_trait]
impl AdmissionWebhook for ClusterIpDefaultingWebhook {
    fn name(&self) -> &'static str {
        "cluster-ip-allocator"
    }

    async fn review(
        &self,
        request: &AdmissionRequest,
    ) -> Result<AdmissionDecision, AdmissionError> {
        // Only CREATE of a Service is in scope; everything else passes.
        if request.action != AdmissionAction::Put || !Self::is_service(&request.key) {
            return Ok(AdmissionDecision::Allow);
        }
        let Some(svc) = &request.value else {
            return Ok(AdmissionDecision::Allow);
        };

        match classify_service(svc) {
            ServiceVipDisposition::NeedsAllocation => {}
            // Headless / ExternalName / already-pinned: untouched.
            _ => return Ok(AdmissionDecision::Allow),
        }

        // Build a fresh allocator + reseed it from the live Service VIPs.
        // This is the restart-persistence + collision-freedom step: the
        // durable Service set is the ledger.
        let mut allocator = match ClusterIpAllocator::new(&self.cidr) {
            Ok(a) => a,
            Err(e) => {
                // A misconfigured CIDR is a typed backend error — the
                // chain's FailClosed/FailOpen mode governs it, never a
                // silent unallocated Service.
                return Err(AdmissionError::Backend(e.to_string()));
            }
        };
        for ip in self.source.held_cluster_ips().await {
            allocator.reserve(&ip);
        }

        let vip = match allocator.allocate() {
            Ok(ip) => ip,
            Err(e) => {
                // Pool exhausted → Deny (the operator sees it). NOT a
                // silently unallocated Service.
                return Ok(AdmissionDecision::Deny(e.to_string()));
            }
        };

        // Stamp the VIP onto a clone of the proposed object: spec.clusterIP
        // (legacy single) + spec.clusterIPs[0] (dual-stack-aware list).
        let mut mutated = svc.clone();
        let spec = mutated
            .as_object_mut()
            .and_then(|o| o.entry("spec").or_insert_with(|| Value::Object(Default::default())).as_object_mut());
        let Some(spec) = spec else {
            // A non-object spec is malformed — surface it, don't paper over.
            return Ok(AdmissionDecision::Deny(
                "Service has a non-object spec; cannot assign clusterIP".to_string(),
            ));
        };
        spec.insert("clusterIP".to_string(), Value::String(vip.clone()));
        spec.insert(
            "clusterIPs".to_string(),
            Value::Array(vec![Value::String(vip)]),
        );
        Ok(AdmissionDecision::Mutate(mutated))
    }
}

/// A static [`ServiceIpSource`] for tests — a fixed list of held VIPs.
#[derive(Clone, Default)]
pub struct StaticServiceIpSource {
    ips: Vec<String>,
}

impl StaticServiceIpSource {
    /// New source holding `ips`.
    #[must_use]
    pub fn new(ips: Vec<String>) -> Self {
        Self { ips }
    }
}

#[async_trait]
impl ServiceIpSource for StaticServiceIpSource {
    async fn held_cluster_ips(&self) -> Vec<String> {
        self.ips.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_store::resource::ResourceKey;
    use serde_json::json;

    // ── allocator core ───────────────────────────────────────────────

    #[test]
    fn allocate_starts_at_first_host() {
        let mut a = ClusterIpAllocator::new("10.96.0.0/12").unwrap();
        // Network address 10.96.0.0 is skipped; first host is 10.96.0.1.
        assert_eq!(a.allocate().unwrap(), "10.96.0.1");
    }

    #[test]
    fn two_allocations_are_distinct_and_sequential() {
        let mut a = ClusterIpAllocator::new("10.96.0.0/12").unwrap();
        let v1 = a.allocate().unwrap();
        let v2 = a.allocate().unwrap();
        assert_ne!(v1, v2, "collision-free");
        assert_eq!(v1, "10.96.0.1");
        assert_eq!(v2, "10.96.0.2");
    }

    #[test]
    fn reserve_skips_held_vips() {
        let mut a = ClusterIpAllocator::new("10.96.0.0/12").unwrap();
        a.reserve("10.96.0.1");
        a.reserve("10.96.0.2");
        // Next free is .3.
        assert_eq!(a.allocate().unwrap(), "10.96.0.3");
    }

    #[test]
    fn release_returns_vip_to_pool() {
        let mut a = ClusterIpAllocator::new("10.96.0.0/12").unwrap();
        let v1 = a.allocate().unwrap(); // .1
        assert_eq!(a.held(), 1);
        a.release(&v1);
        assert_eq!(a.held(), 0);
        // Re-allocates the freed VIP (lowest free).
        assert_eq!(a.allocate().unwrap(), "10.96.0.1");
    }

    #[test]
    fn reserve_ignores_out_of_range_and_non_vip() {
        let mut a = ClusterIpAllocator::new("10.96.0.0/24").unwrap();
        assert!(!a.reserve("None"));
        assert!(!a.reserve(""));
        assert!(!a.reserve("192.168.1.1")); // out of CIDR
        assert!(!a.reserve("not-an-ip"));
        assert_eq!(a.held(), 0);
    }

    #[test]
    fn small_cidr_exhausts_with_typed_error() {
        // /30 → 4 addresses; .0 net + .3 broadcast skipped ⇒ 2 assignable.
        let mut a = ClusterIpAllocator::new("10.0.0.0/30").unwrap();
        assert_eq!(a.allocate().unwrap(), "10.0.0.1");
        assert_eq!(a.allocate().unwrap(), "10.0.0.2");
        let err = a.allocate().unwrap_err();
        assert_eq!(err.kind(), "pool_exhausted");
    }

    #[test]
    fn invalid_cidr_is_typed_error() {
        let err = ClusterIpAllocator::new("nonsense").unwrap_err();
        assert_eq!(err.kind(), "invalid_cidr");
    }

    #[test]
    fn allocation_survives_restart_via_reseed() {
        // Simulate a restart: process 1 allocates two VIPs, they land on
        // live Services. Process 2 (fresh allocator) reseeds from those
        // live VIPs → never re-hands them out.
        let mut p1 = ClusterIpAllocator::new("10.96.0.0/16").unwrap();
        let held1 = p1.allocate().unwrap(); // .0.1
        let held2 = p1.allocate().unwrap(); // .0.2

        // Fresh allocator after "restart" — no in-memory counter survives.
        let mut p2 = ClusterIpAllocator::new("10.96.0.0/16").unwrap();
        p2.reserve(&held1);
        p2.reserve(&held2);
        let next = p2.allocate().unwrap();
        assert_ne!(next, held1);
        assert_ne!(next, held2);
        assert_eq!(next, "10.96.0.3", "recomputes from the durable set");
    }

    // ── classification ───────────────────────────────────────────────

    #[test]
    fn classify_default_service_needs_allocation() {
        let svc = json!({"spec": {"ports": [{"port": 80}]}});
        assert_eq!(classify_service(&svc), ServiceVipDisposition::NeedsAllocation);
        let svc2 = json!({"spec": {"type": "ClusterIP", "clusterIP": ""}});
        assert_eq!(classify_service(&svc2), ServiceVipDisposition::NeedsAllocation);
    }

    #[test]
    fn classify_headless_service() {
        let svc = json!({"spec": {"clusterIP": "None"}});
        assert_eq!(classify_service(&svc), ServiceVipDisposition::Headless);
    }

    #[test]
    fn classify_external_name_service() {
        let svc = json!({"spec": {"type": "ExternalName", "externalName": "db.example.com"}});
        assert_eq!(classify_service(&svc), ServiceVipDisposition::ExternalName);
    }

    #[test]
    fn classify_already_assigned() {
        let svc = json!({"spec": {"clusterIP": "10.96.5.5"}});
        assert_eq!(classify_service(&svc), ServiceVipDisposition::AlreadyAssigned);
    }

    // ── defaulting webhook ───────────────────────────────────────────

    fn svc_request(svc: Value) -> AdmissionRequest {
        AdmissionRequest {
            action: AdmissionAction::Put,
            key: ResourceKey::namespaced("", "v1", "Service", "default", "podinfo"),
            value: Some(svc),
            current: None,
            user_info: engenho_types::auth::UserInfo::default(),
        }
    }

    #[tokio::test]
    async fn webhook_allocates_vip_for_default_service() {
        let source = Arc::new(StaticServiceIpSource::new(vec![]));
        let wh = ClusterIpDefaultingWebhook::new("10.96.0.0/12", source);
        let req = svc_request(json!({
            "kind": "Service", "apiVersion": "v1",
            "metadata": {"name": "podinfo"},
            "spec": {"selector": {"app": "podinfo"}, "ports": [{"port": 80}]}
        }));
        match wh.review(&req).await.unwrap() {
            AdmissionDecision::Mutate(v) => {
                let ip = v["spec"]["clusterIP"].as_str().unwrap();
                assert_eq!(ip, "10.96.0.1");
                // clusterIPs[0] mirrors clusterIP (dual-stack-aware list).
                assert_eq!(v["spec"]["clusterIPs"][0], "10.96.0.1");
            }
            other => panic!("expected Mutate, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn webhook_skips_held_vips_collision_free() {
        // Two live Services already hold .1 and .2; the new Service gets .3.
        let source = Arc::new(StaticServiceIpSource::new(vec![
            "10.96.0.1".into(),
            "10.96.0.2".into(),
        ]));
        let wh = ClusterIpDefaultingWebhook::new("10.96.0.0/12", source);
        let req = svc_request(json!({
            "kind": "Service", "apiVersion": "v1",
            "metadata": {"name": "new"}, "spec": {"ports": [{"port": 80}]}
        }));
        match wh.review(&req).await.unwrap() {
            AdmissionDecision::Mutate(v) => {
                assert_eq!(v["spec"]["clusterIP"], "10.96.0.3");
            }
            other => panic!("expected Mutate, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn webhook_leaves_headless_untouched() {
        let source = Arc::new(StaticServiceIpSource::new(vec![]));
        let wh = ClusterIpDefaultingWebhook::new("10.96.0.0/12", source);
        let req = svc_request(json!({
            "kind": "Service", "apiVersion": "v1",
            "metadata": {"name": "headless"},
            "spec": {"clusterIP": "None", "selector": {"app": "x"}}
        }));
        // Headless → Allow (unchanged); clusterIP stays None.
        assert!(matches!(wh.review(&req).await.unwrap(), AdmissionDecision::Allow));
    }

    #[tokio::test]
    async fn webhook_leaves_external_name_untouched() {
        let source = Arc::new(StaticServiceIpSource::new(vec![]));
        let wh = ClusterIpDefaultingWebhook::new("10.96.0.0/12", source);
        let req = svc_request(json!({
            "kind": "Service", "apiVersion": "v1",
            "metadata": {"name": "ext"},
            "spec": {"type": "ExternalName", "externalName": "db.example.com"}
        }));
        assert!(matches!(wh.review(&req).await.unwrap(), AdmissionDecision::Allow));
    }

    #[tokio::test]
    async fn webhook_honors_operator_pinned_clusterip() {
        let source = Arc::new(StaticServiceIpSource::new(vec![]));
        let wh = ClusterIpDefaultingWebhook::new("10.96.0.0/12", source);
        let req = svc_request(json!({
            "kind": "Service", "apiVersion": "v1",
            "metadata": {"name": "pinned"},
            "spec": {"clusterIP": "10.96.7.7", "ports": [{"port": 80}]}
        }));
        // Already-assigned → Allow (honored as-is, never reallocated).
        assert!(matches!(wh.review(&req).await.unwrap(), AdmissionDecision::Allow));
    }

    #[tokio::test]
    async fn webhook_ignores_non_service_kinds() {
        let source = Arc::new(StaticServiceIpSource::new(vec![]));
        let wh = ClusterIpDefaultingWebhook::new("10.96.0.0/12", source);
        let req = AdmissionRequest {
            action: AdmissionAction::Put,
            key: ResourceKey::namespaced("", "v1", "Pod", "default", "p"),
            value: Some(json!({"kind": "Pod"})),
            current: None,
            user_info: engenho_types::auth::UserInfo::default(),
        };
        assert!(matches!(wh.review(&req).await.unwrap(), AdmissionDecision::Allow));
    }

    #[tokio::test]
    async fn webhook_denies_on_pool_exhaustion() {
        // A /30 with both assignable VIPs already held → Deny (not silent).
        let source = Arc::new(StaticServiceIpSource::new(vec![
            "10.0.0.1".into(),
            "10.0.0.2".into(),
        ]));
        let wh = ClusterIpDefaultingWebhook::new("10.0.0.0/30", source);
        let req = svc_request(json!({
            "kind": "Service", "apiVersion": "v1",
            "metadata": {"name": "doomed"}, "spec": {"ports": [{"port": 80}]}
        }));
        match wh.review(&req).await.unwrap() {
            AdmissionDecision::Deny(reason) => assert!(reason.contains("exhausted")),
            other => panic!("expected Deny, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn webhook_restart_persistence_via_store_reseed() {
        // The webhook reseeds from the source on EVERY call. Simulate a
        // restart by constructing a NEW webhook over a source reporting the
        // VIPs that landed on live Services — it never re-hands them out.
        let live_after_restart = Arc::new(StaticServiceIpSource::new(vec![
            "10.96.0.1".into(),
            "10.96.0.2".into(),
            "10.96.0.3".into(),
        ]));
        let wh = ClusterIpDefaultingWebhook::new("10.96.0.0/12", live_after_restart);
        let req = svc_request(json!({
            "kind": "Service", "apiVersion": "v1",
            "metadata": {"name": "post-restart"}, "spec": {"ports": [{"port": 80}]}
        }));
        match wh.review(&req).await.unwrap() {
            AdmissionDecision::Mutate(v) => {
                assert_eq!(v["spec"]["clusterIP"], "10.96.0.4");
            }
            other => panic!("expected Mutate, got {other:?}"),
        }
    }

    #[test]
    fn u32_ip_roundtrip() {
        assert_eq!(u32_to_ip(ip_to_u32("10.96.0.1").unwrap()), "10.96.0.1");
        assert_eq!(u32_to_ip(ip_to_u32("255.255.255.255").unwrap()), "255.255.255.255");
        assert_eq!(u32_to_ip(0), "0.0.0.0");
    }
}
