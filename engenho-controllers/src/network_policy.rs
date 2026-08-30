//! R17 — NetworkPolicy enforcement.
//!
//! Typed pluggable backend for installing L3/L4 firewall rules
//! that implement K8s NetworkPolicy semantics. Backends:
//!   * `FakeNetworkPolicyEnforcer` (tests; tracks rules in BTreeMap)
//!   * `CiliumNetworkPolicyAdapter` (renders CiliumNetworkPolicy CRD YAML;
//!     production via cilium-operator hot-reload)
//!   * `IptablesNetworkPolicyEnforcer` (R17b — direct iptables-rules
//!     production backend; deferred)

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;

/// One NetworkPolicy rule — a flat representation of the K8s
/// NetworkPolicy spec.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkPolicyRule {
    /// `namespace/name` of the source NetworkPolicy object.
    pub policy_id: String,
    /// Pod selector (matchLabels) — pods this rule applies to.
    pub pod_selector: BTreeMap<String, String>,
    /// Direction: ingress (incoming) or egress (outgoing).
    pub direction: Direction,
    /// Allowed peers — empty = deny all in that direction.
    pub allowed_peers: Vec<PeerSelector>,
    /// Allowed ports — empty = any port.
    pub allowed_ports: Vec<PortSpec>,
}

/// Direction of traffic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum Direction {
    /// Incoming traffic to selected pods.
    Ingress,
    /// Outgoing traffic from selected pods.
    Egress,
}

/// One allowed peer (pod selector, namespace selector, or CIDR).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerSelector {
    /// Match pods with these labels.
    PodSelector {
        /// Pod label match map.
        match_labels: BTreeMap<String, String>,
    },
    /// Match all pods in namespaces with these labels.
    NamespaceSelector {
        /// Namespace label match map.
        match_labels: BTreeMap<String, String>,
    },
    /// Match an IP range (e.g. "10.0.0.0/8").
    IpBlock {
        /// CIDR string.
        cidr: String,
        /// Optional exclusion CIDRs.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        except: Vec<String>,
    },
}

/// One allowed port range.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortSpec {
    /// Port number (or start of range).
    pub port: u16,
    /// Optional end of port range; None = single port.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_port: Option<u16>,
    /// Protocol: TCP / UDP / SCTP (default TCP).
    pub protocol: String,
}

/// NetworkPolicy backend errors.
#[derive(Debug, Clone, Error)]
pub enum NetworkPolicyError {
    /// Backend (Cilium, iptables, ebpf) returned an error.
    #[error("backend: {0}")]
    Backend(String),
    /// Invalid policy (e.g. empty policy_id).
    #[error("invalid policy: {0}")]
    InvalidPolicy(String),
}

engenho_substrate::impl_error_kind! {
    NetworkPolicyError {
        (Backend(_)) => "backend",
        (InvalidPolicy(_)) => "invalid_policy",
    }
}

/// Whether a backend actually INSTALLS the policy datapath, or only
/// computes it.
///
/// ★ THE SAME SPLIT `DatapathInstall` MAKES FOR kube-proxy, AND FOR THE
/// SAME REASON. A NetworkPolicy that is parsed, selected and tracked but
/// never installed in a kernel allows every packet it claims to deny. That
/// is not a partial implementation, it is the opposite of the stated
/// semantics — and it is the one failure shape where silence is a security
/// claim rather than a missing feature. Making the distinction a value on
/// the trait means a caller cannot hold an enforcer without being able to
/// ask which of the two it got.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyDatapath {
    /// Rules are computed, selected and observable, but NO packet filter
    /// is installed anywhere. The fail-safe state on a topology that
    /// cannot host the filter (engenho on darwin, pods inside a podman
    /// VM). Traffic is UNRESTRICTED; the policy is a plan, not a control.
    Computed,
    /// The backend installed the filter (iptables / eBPF / a Cilium CRD a
    /// running cilium-operator loads). Traffic is actually restricted.
    Installed,
}

/// Pluggable network-policy backend trait.
#[async_trait]
pub trait NetworkPolicyEnforcer: Send + Sync {
    /// Backend identifier for telemetry.
    fn name(&self) -> &'static str;

    /// Whether this backend installs the datapath or only computes it.
    ///
    /// Defaults to [`PolicyDatapath::Installed`] — the kernel backends —
    /// so a NEW backend that forgets to override is claimed to enforce and
    /// will be caught by the honesty test, rather than defaulting to the
    /// permissive answer that nobody notices.
    fn datapath(&self) -> PolicyDatapath {
        PolicyDatapath::Installed
    }

    /// Install / refresh a rule. Idempotent.
    ///
    /// # Errors
    /// [`NetworkPolicyError::Backend`] on backend failure.
    async fn upsert(&self, rule: &NetworkPolicyRule) -> Result<(), NetworkPolicyError>;

    /// Remove a rule by `policy_id`.
    ///
    /// # Errors
    /// [`NetworkPolicyError::Backend`] on backend failure.
    async fn remove(&self, policy_id: &str) -> Result<(), NetworkPolicyError>;

    /// Currently-installed rules.
    ///
    /// # Errors
    /// [`NetworkPolicyError::Backend`] on backend failure.
    async fn list(&self) -> Result<Vec<NetworkPolicyRule>, NetworkPolicyError>;
}

// =================================================================
// FakeNetworkPolicyEnforcer — deterministic backend for tests
// =================================================================

/// In-memory backend. Tracks rules in a BTreeMap + records events.
#[derive(Default, Clone)]
pub struct FakeNetworkPolicyEnforcer {
    inner: Arc<Mutex<FakeNpState>>,
}

#[derive(Default)]
struct FakeNpState {
    rules: BTreeMap<String, NetworkPolicyRule>,
    events: Vec<FakeNpEvent>,
}

/// Per-call event log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FakeNpEvent {
    /// `upsert(policy_id)`.
    Upsert(String),
    /// `remove(policy_id)`.
    Remove(String),
}

impl FakeNetworkPolicyEnforcer {
    /// Fresh empty backend.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of events.
    pub async fn events(&self) -> Vec<FakeNpEvent> {
        self.inner.lock().await.events.clone()
    }

    /// Count of installed rules.
    pub async fn rule_count(&self) -> usize {
        self.inner.lock().await.rules.len()
    }
}

#[async_trait]
impl NetworkPolicyEnforcer for FakeNetworkPolicyEnforcer {
    fn name(&self) -> &'static str {
        "fake"
    }

    /// A test double installs nothing, and neither does the darwin
    /// topology it stands in for.
    fn datapath(&self) -> PolicyDatapath {
        PolicyDatapath::Computed
    }

    async fn upsert(&self, rule: &NetworkPolicyRule) -> Result<(), NetworkPolicyError> {
        if rule.policy_id.is_empty() {
            return Err(NetworkPolicyError::InvalidPolicy("empty policy_id".into()));
        }
        let mut state = self.inner.lock().await;
        state.rules.insert(rule.policy_id.clone(), rule.clone());
        state
            .events
            .push(FakeNpEvent::Upsert(rule.policy_id.clone()));
        Ok(())
    }

    async fn remove(&self, policy_id: &str) -> Result<(), NetworkPolicyError> {
        let mut state = self.inner.lock().await;
        state.rules.remove(policy_id);
        state
            .events
            .push(FakeNpEvent::Remove(policy_id.to_string()));
        Ok(())
    }

    async fn list(&self) -> Result<Vec<NetworkPolicyRule>, NetworkPolicyError> {
        Ok(self.inner.lock().await.rules.values().cloned().collect())
    }
}

// =================================================================
// CiliumNetworkPolicyAdapter — renders Cilium CRD YAML
// =================================================================

/// Renders [`NetworkPolicyRule`] as CiliumNetworkPolicy CRD YAML +
/// writes to a configured directory. Cilium operator watches the
/// directory + hot-reloads.
///
/// No shell-out; pure Rust + std::fs.
#[derive(Clone)]
pub struct CiliumNetworkPolicyAdapter {
    output_dir: std::path::PathBuf,
    inner: Arc<Mutex<CiliumState>>,
}

#[derive(Default)]
struct CiliumState {
    rules: BTreeMap<String, NetworkPolicyRule>,
}

impl CiliumNetworkPolicyAdapter {
    /// New adapter writing to `output_dir`.
    #[must_use]
    pub fn new(output_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            output_dir: output_dir.into(),
            inner: Arc::new(Mutex::new(CiliumState::default())),
        }
    }

    /// Render the CiliumNetworkPolicy CRD YAML for one rule. Pure.
    ///
    /// Builds a typed [`engenho_types::egress::CiliumNetworkPolicy`]
    /// + renders it through `serde_yaml` (per the ★★ TYPED EMISSION
    /// rule — no `format!()` of YAML syntax).
    #[must_use]
    pub fn render_crd(rule: &NetworkPolicyRule) -> String {
        use engenho_types::egress::{CiliumDirection, CiliumNetworkPolicy};
        let name = rule.policy_id.replace('/', "-");
        let direction = match rule.direction {
            Direction::Ingress => CiliumDirection::Ingress,
            Direction::Egress => CiliumDirection::Egress,
        };
        CiliumNetworkPolicy::new(name, rule.pod_selector.clone(), direction).to_yaml()
    }

    fn rule_filename(rule: &NetworkPolicyRule) -> String {
        format!("{}.yaml", rule.policy_id.replace('/', "-"))
    }
}

#[async_trait]
impl NetworkPolicyEnforcer for CiliumNetworkPolicyAdapter {
    fn name(&self) -> &'static str {
        "cilium"
    }

    async fn upsert(&self, rule: &NetworkPolicyRule) -> Result<(), NetworkPolicyError> {
        if rule.policy_id.is_empty() {
            return Err(NetworkPolicyError::InvalidPolicy("empty policy_id".into()));
        }
        let yaml = Self::render_crd(rule);
        let file = self.output_dir.join(Self::rule_filename(rule));
        std::fs::create_dir_all(&self.output_dir).map_err(|e| {
            NetworkPolicyError::Backend(format!("mkdir {}: {e}", self.output_dir.display()))
        })?;
        std::fs::write(&file, yaml)
            .map_err(|e| NetworkPolicyError::Backend(format!("write {}: {e}", file.display())))?;
        self.inner
            .lock()
            .await
            .rules
            .insert(rule.policy_id.clone(), rule.clone());
        Ok(())
    }

    async fn remove(&self, policy_id: &str) -> Result<(), NetworkPolicyError> {
        let filename = format!("{}.yaml", policy_id.replace('/', "-"));
        let file = self.output_dir.join(filename);
        let _ = std::fs::remove_file(&file);
        self.inner.lock().await.rules.remove(policy_id);
        Ok(())
    }

    async fn list(&self) -> Result<Vec<NetworkPolicyRule>, NetworkPolicyError> {
        Ok(self.inner.lock().await.rules.values().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_rule() -> NetworkPolicyRule {
        let mut sel = BTreeMap::new();
        sel.insert("app".to_string(), "podinfo".to_string());
        NetworkPolicyRule {
            policy_id: "default/allow-frontend".into(),
            pod_selector: sel,
            direction: Direction::Ingress,
            allowed_peers: vec![PeerSelector::PodSelector {
                match_labels: {
                    let mut m = BTreeMap::new();
                    m.insert("app".to_string(), "frontend".to_string());
                    m
                },
            }],
            allowed_ports: vec![PortSpec {
                port: 80,
                end_port: None,
                protocol: "TCP".into(),
            }],
        }
    }

    // ── Fake ─────────────────────────────────────────────────

    #[tokio::test]
    async fn fake_upsert_inserts_rule() {
        let b = FakeNetworkPolicyEnforcer::new();
        b.upsert(&sample_rule()).await.unwrap();
        assert_eq!(b.rule_count().await, 1);
    }

    #[tokio::test]
    async fn fake_rejects_empty_policy_id() {
        let b = FakeNetworkPolicyEnforcer::new();
        let mut r = sample_rule();
        r.policy_id = String::new();
        let err = b.upsert(&r).await.unwrap_err();
        assert_eq!(err.kind(), "invalid_policy");
    }

    #[tokio::test]
    async fn fake_remove_clears_rule() {
        let b = FakeNetworkPolicyEnforcer::new();
        let r = sample_rule();
        b.upsert(&r).await.unwrap();
        b.remove(&r.policy_id).await.unwrap();
        assert_eq!(b.rule_count().await, 0);
    }

    #[tokio::test]
    async fn fake_events_record_in_order() {
        let b = FakeNetworkPolicyEnforcer::new();
        let r = sample_rule();
        b.upsert(&r).await.unwrap();
        b.remove(&r.policy_id).await.unwrap();
        let evs = b.events().await;
        assert_eq!(evs.len(), 2);
        assert!(matches!(&evs[0], FakeNpEvent::Upsert(_)));
        assert!(matches!(&evs[1], FakeNpEvent::Remove(_)));
    }

    #[tokio::test]
    async fn fake_list_returns_rules() {
        let b = FakeNetworkPolicyEnforcer::new();
        b.upsert(&sample_rule()).await.unwrap();
        assert_eq!(b.list().await.unwrap().len(), 1);
    }

    // ── Cilium ───────────────────────────────────────────────

    #[test]
    fn cilium_render_crd_contains_selector_and_direction() {
        let yaml = CiliumNetworkPolicyAdapter::render_crd(&sample_rule());
        assert!(yaml.contains("apiVersion: cilium.io/v2"));
        assert!(yaml.contains("kind: CiliumNetworkPolicy"));
        assert!(yaml.contains("name: default-allow-frontend"));
        assert!(yaml.contains("app: podinfo"));
        assert!(yaml.contains("ingress:"));
    }

    #[test]
    fn cilium_render_crd_egress_direction() {
        let mut r = sample_rule();
        r.direction = Direction::Egress;
        let yaml = CiliumNetworkPolicyAdapter::render_crd(&r);
        assert!(yaml.contains("egress:"));
        assert!(!yaml.contains("ingress:"));
    }

    #[tokio::test]
    async fn cilium_upsert_writes_file() {
        let dir = std::env::temp_dir().join(format!("engenho-np-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let b = CiliumNetworkPolicyAdapter::new(&dir);
        b.upsert(&sample_rule()).await.unwrap();
        let files: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .collect();
        assert_eq!(files.len(), 1);
        let content = std::fs::read_to_string(&files[0]).unwrap();
        assert!(content.contains("CiliumNetworkPolicy"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn cilium_remove_deletes_file() {
        let dir = std::env::temp_dir().join(format!("engenho-np-rm-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let b = CiliumNetworkPolicyAdapter::new(&dir);
        let r = sample_rule();
        b.upsert(&r).await.unwrap();
        b.remove(&r.policy_id).await.unwrap();
        let count = std::fs::read_dir(&dir).map(|i| i.count()).unwrap_or(0);
        assert_eq!(count, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn direction_serializes_pascal_case() {
        assert_eq!(
            serde_json::to_string(&Direction::Ingress).unwrap(),
            "\"Ingress\""
        );
    }

    #[test]
    fn peer_selector_serializes_snake_case() {
        let p = PeerSelector::IpBlock {
            cidr: "10.0.0.0/8".into(),
            except: vec![],
        };
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("\"ip_block\""));
    }

    #[test]
    fn error_kinds_are_stable() {
        assert_eq!(NetworkPolicyError::Backend("x".into()).kind(), "backend");
        assert_eq!(
            NetworkPolicyError::InvalidPolicy("x".into()).kind(),
            "invalid_policy"
        );
    }

    #[test]
    fn backend_names_are_stable() {
        assert_eq!(FakeNetworkPolicyEnforcer::new().name(), "fake");
        assert_eq!(CiliumNetworkPolicyAdapter::new("/tmp").name(), "cilium");
    }
}

// =================================================================
// ComputedNetworkPolicyEnforcer — the honest PRODUCTION backend for a
// topology that cannot host a packet filter.
// =================================================================

/// The production backend on a host with no filter to install into
/// (engenho on darwin, pods inside a podman VM).
///
/// ★ WHY THIS EXISTS SEPARATELY FROM `FakeNetworkPolicyEnforcer` WHEN THE
/// BEHAVIOUR IS IDENTICAL. It is not duplicated — the tracking is
/// DELEGATED to the same implementation. What differs is the only two
/// things that matter at a boundary: the `name()` that appears in an
/// operator-facing event and in logs, and the fact that wiring a struct
/// called `Fake…` into a production runtime is how a test double quietly
/// becomes the shipped answer. A backend an operator sees named `fake` in
/// a warning is a bug report; one named `computed` is the truth.
#[derive(Default, Clone)]
pub struct ComputedNetworkPolicyEnforcer {
    inner: FakeNetworkPolicyEnforcer,
}

impl ComputedNetworkPolicyEnforcer {
    /// Fresh backend.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl NetworkPolicyEnforcer for ComputedNetworkPolicyEnforcer {
    fn name(&self) -> &'static str {
        "computed"
    }

    /// Computed, always. This backend has no path to `Installed`, so the
    /// permissive answer cannot be reached by a configuration mistake.
    fn datapath(&self) -> PolicyDatapath {
        PolicyDatapath::Computed
    }

    async fn upsert(&self, rule: &NetworkPolicyRule) -> Result<(), NetworkPolicyError> {
        self.inner.upsert(rule).await
    }

    async fn remove(&self, policy_id: &str) -> Result<(), NetworkPolicyError> {
        self.inner.remove(policy_id).await
    }

    async fn list(&self) -> Result<Vec<NetworkPolicyRule>, NetworkPolicyError> {
        self.inner.list().await
    }
}

#[cfg(test)]
mod computed_backend_tests {
    use super::*;

    #[tokio::test]
    async fn the_production_computed_backend_tracks_but_never_claims_enforcement() {
        let e = ComputedNetworkPolicyEnforcer::new();
        assert_eq!(e.datapath(), PolicyDatapath::Computed);
        // The operator-facing name must not be the word "fake": that is the
        // whole reason this type is not the test double.
        assert_eq!(e.name(), "computed");
        e.upsert(&NetworkPolicyRule {
            policy_id: "ns/p#ingress:0".into(),
            pod_selector: BTreeMap::new(),
            direction: Direction::Ingress,
            allowed_peers: vec![],
            allowed_ports: vec![],
        })
        .await
        .unwrap();
        assert_eq!(e.list().await.unwrap().len(), 1, "rules are still tracked");
    }
}
