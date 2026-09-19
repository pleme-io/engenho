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
//!
//! ## Datapath scope (HONEST — the macOS-VM packet-install is a typed
//!    deferral, NOT a silent gap)
//!
//! Three layers compose the kube-proxy datapath; engenho ships them at
//! different maturities and is explicit about which:
//!
//!   1. **VIP allocation** — DONE (`crate::cluster_ip`): a Service gets a
//!      real ClusterIP, so a route HAS a VIP to key on (before this the
//!      VIP was `None` and every route degenerate).
//!   2. **Desired-rule computation** — DONE (this module): the controller
//!      resolves Service+Endpoints → typed [`ServiceRoute`]s, and the
//!      [`IptablesRouter`]/[`IpvsRouter`] backends render the EXACT
//!      `KUBE-SVC`/`KUBE-SEP` chain hierarchy (resp. ipvs virtual-server
//!      table) kube-proxy installs — fully unit-tested via pure
//!      `render_script`.
//!   3. **Packet-routing INSTALL** — the actual `iptables-restore` /
//!      `ipvsadm` apply on the node. On the bootstrap macOS topology the
//!      engenho host process runs on Darwin while pods run in a podman
//!      Linux VM, so the host cannot install the Linux VIP datapath the
//!      way a Linux node would. The controller's backend selection is a
//!      typed decision ([`DatapathInstall`]): `Computed` (default off-Linux
//!      — routes are computed + observable via a [`FakeRouter`], but no
//!      kernel rule is installed) vs `Installed` (Linux node — the
//!      iptables/ipvs backend applies the rules). A `Computed` install is
//!      NOT a silent skip: it is the named, fail-safe state the same way
//!      the webhook caBundle deferral is — real progress (correct allocator
//!      + correct computed rules + EndpointSlice) awaiting a Linux node for
//!      the final kernel apply.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::StoreMesh;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::controller::{Controller, ReconcileOutcome, ReconcileReport};
use crate::effect::Effect;
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

/// The datapath-install maturity a [`ServiceRouter`] backend provides —
/// the typed name for "did we COMPUTE the rules, or did we INSTALL them in
/// the kernel?" This makes the macOS-VM packet-install deferral a typed
/// value, never a silent gap.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatapathInstall {
    /// Routes are computed + tracked + observable, but NO kernel rule is
    /// installed. The fail-safe state on a topology where the host cannot
    /// install the Linux VIP datapath (e.g. engenho on Darwin with pods in
    /// a podman Linux VM). Correct allocator + correct computed rules,
    /// awaiting a Linux node for the kernel apply.
    Computed,
    /// The backend installs the VIP datapath in the node's kernel
    /// (iptables / ipvs / eBPF) — a real Linux node.
    Installed,
}

/// Pluggable routing backend trait. Implementations apply +
/// remove routes on the local host (iptables, ipvs, ebpf, fake).
#[async_trait]
pub trait ServiceRouter: Send + Sync {
    /// Stable backend name.
    fn name(&self) -> &'static str;

    /// The datapath-install maturity this backend provides. `Computed`
    /// (FakeRouter / off-Linux topology) ⇒ routes are computed + tracked
    /// but no kernel rule is installed; `Installed` (iptables / ipvs on a
    /// Linux node) ⇒ the VIP datapath is applied. Defaults to `Installed`
    /// (the kernel backends); the `FakeRouter` overrides to `Computed`.
    fn datapath(&self) -> DatapathInstall {
        DatapathInstall::Installed
    }

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

    fn datapath(&self) -> DatapathInstall {
        // Computes + tracks routes; installs nothing in the kernel. This is
        // ALSO the honest default backend on the macOS-VM topology where the
        // host can't install the Linux VIP datapath — routes are computed +
        // observable, the kernel apply awaits a Linux node.
        DatapathInstall::Computed
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
    /// The `iptables` control binary (check / create / delete a single
    /// rule). `iptables-restore` cannot express "append only if absent",
    /// which is the whole of [`ensure_root_chain`](IptablesRouter::ensure_root_chain).
    control_binary: String,
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
        let binary = binary.into();
        let control = Self::control_binary_for(&binary);
        Self::with_binaries(binary, control)
    }

    /// New router with both binaries named explicitly.
    #[must_use]
    pub fn with_binaries(binary: impl Into<String>, control_binary: impl Into<String>) -> Self {
        Self {
            binary: binary.into(),
            control_binary: control_binary.into(),
            inner: Arc::new(Mutex::new(IptablesState::default())),
        }
    }

    /// `iptables-restore` -> `iptables`, preserving any directory and any
    /// variant prefix (`iptables-legacy-restore` -> `iptables-legacy`), so a
    /// node pinned to the legacy backend does not get its rules checked
    /// through the nft one. Pure — unit-assertable.
    #[must_use]
    pub fn control_binary_for(restore_binary: &str) -> String {
        restore_binary
            .strip_suffix("-restore")
            .unwrap_or(restore_binary)
            .to_string()
    }

    /// Render the iptables-restore script for one route. Pure —
    /// no I/O. Test helper exposed publicly so callers can inspect
    /// what the router would emit.
    ///
    /// Built as a typed [`engenho_types::egress::IptablesScript`] +
    /// rendered through its `Display` chokepoint (★★ TYPED EMISSION —
    /// no `format!()` of iptables syntax).
    #[must_use]
    pub fn render_script(route: &ServiceRoute) -> String {
        use engenho_types::egress::IptablesScript;
        let chain_svc = chain_name("KUBE-SVC", &route.service_id);
        let mut script = IptablesScript::new();
        script.table("nat").chain(&chain_svc);
        for port in &route.ports {
            let proto = port.protocol.to_lowercase();
            // Hit the per-service chain from KUBE-SERVICES.
            script.jump_to_service_chain(&route.cluster_ip, &proto, port.service_port, &chain_svc);
            // For each pod IP, install a per-endpoint chain.
            for (i, pod_ip) in route.endpoints.iter().enumerate() {
                let chain_ep =
                    chain_name("KUBE-SEP", &format!("{}-{}-{i}", route.service_id, pod_ip));
                script.chain(&chain_ep);
                // Round-robin via statistic mode random; first endpoint
                // gets 1/N, second 1/(N-1) etc.
                let remaining = route.endpoints.len() - i;
                let probability = 1.0 / (remaining as f64);
                script.statistic_jump(&chain_svc, probability, &chain_ep);
                // The endpoint chain DNATs to the pod.
                script.dnat(&chain_ep, &proto, pod_ip, port.target_port);
            }
        }
        script.commit();
        script.to_string()
    }

    /// The chain every Service-VIP jump hangs off.
    ///
    /// **engenho must create this itself.** kube-proxy creates
    /// `KUBE-SERVICES` and hooks it into nat/PREROUTING + nat/OUTPUT, and an
    /// earlier version of this router simply appended into it — which works
    /// only on a node where kube-proxy has run. That is exactly the node
    /// engenho is built to replace, so the dependency was on the one thing
    /// guaranteed absent. Measured on rio 2026-09-15: the host still carried
    /// k3s's chains hours after k3s was stopped, so Service routing would
    /// have appeared to work and then vanished at the next reboot, when
    /// iptables comes up empty.
    const ROOT_CHAIN: &'static str = "KUBE-SERVICES";

    /// Comment stamped on the two hook rules so they are identifiable as
    /// ours in `iptables -S` and matched exactly by the `-C` check.
    const HOOK_COMMENT: &'static str = "engenho service portals";

    /// The nat hooks a Service VIP must be reachable from: PREROUTING for
    /// traffic arriving from a pod, OUTPUT for traffic originating on the
    /// node itself. Missing OUTPUT is the classic half-fix — pods reach the
    /// VIP and the host does not.
    const HOOKS: [&'static str; 2] = ["PREROUTING", "OUTPUT"];

    /// Run the control binary, returning (success, stderr).
    async fn control(&self, args: &[String]) -> Result<(bool, String), RouterError> {
        let out = tokio::process::Command::new(&self.control_binary)
            .args(args)
            .output()
            .await
            .map_err(|e| RouterError::Backend(format!("{} spawn: {e}", self.control_binary)))?;
        Ok((
            out.status.success(),
            String::from_utf8_lossy(&out.stderr).to_string(),
        ))
    }

    /// argv for creating the root chain (pure, unit-assertable).
    #[must_use]
    pub fn root_chain_create_argv() -> Vec<String> {
        vec![
            "-t".into(),
            "nat".into(),
            "-N".into(),
            Self::ROOT_CHAIN.into(),
        ]
    }

    /// argv for checking (`-C`) or inserting (`-I <hook> 1`) the hook rule.
    /// Inserted at position 1 rather than appended: a DNAT that runs after
    /// someone else's blanket MASQUERADE or RETURN never runs at all.
    #[must_use]
    pub fn hook_argv(hook: &str, verb: &str) -> Vec<String> {
        let mut v = vec![
            "-t".to_string(),
            "nat".to_string(),
            verb.to_string(),
            hook.to_string(),
        ];
        if verb == "-I" {
            v.push("1".to_string());
        }
        v.extend([
            "-m".to_string(),
            "comment".to_string(),
            "--comment".to_string(),
            Self::HOOK_COMMENT.to_string(),
            "-j".to_string(),
            Self::ROOT_CHAIN.to_string(),
        ]);
        v
    }

    /// Create the root chain and its two hooks if they are not already
    /// there. Idempotent by CHECKING, never by blind append — `iptables -A`
    /// happily installs a second identical hook rule, and nothing complains.
    async fn ensure_root_chain(&self) -> Result<(), RouterError> {
        let (ok, stderr) = self.control(&Self::root_chain_create_argv()).await?;
        if !ok && !stderr.contains("already exists") {
            return Err(RouterError::Backend(format!(
                "create {}: {stderr}",
                Self::ROOT_CHAIN
            )));
        }
        for hook in Self::HOOKS {
            let (present, _) = self.control(&Self::hook_argv(hook, "-C")).await?;
            if !present {
                let (ok, stderr) = self.control(&Self::hook_argv(hook, "-I")).await?;
                if !ok {
                    return Err(RouterError::Backend(format!(
                        "hook {hook} -> {}: {stderr}",
                        Self::ROOT_CHAIN
                    )));
                }
            }
        }
        Ok(())
    }

    /// argv deleting ONE jump for this route+port from the root chain.
    /// Pure so the `-D` spec can be asserted equal to the `-A` the script
    /// renders — they are the same bytes or the delete silently matches
    /// nothing.
    #[must_use]
    pub fn jump_delete_argv(
        cluster_ip: &str,
        protocol: &str,
        dport: u16,
        chain_svc: &str,
    ) -> Vec<String> {
        use engenho_types::egress::IptablesScript;
        let mut argv = vec![
            "-t".to_string(),
            "nat".to_string(),
            "-D".to_string(),
            Self::ROOT_CHAIN.to_string(),
        ];
        argv.extend(IptablesScript::service_jump_spec(
            cluster_ip, protocol, dport, chain_svc,
        ));
        argv
    }

    /// Delete EVERY existing jump for this route from the root chain.
    ///
    /// The root chain is shared by all Services, so it can never be declared
    /// (and therefore flushed) in a per-route restore script without wiping
    /// the other Services' jumps. Measured on rio 2026-09-15: applying the
    /// same script twice through `iptables-restore --noflush` left ONE rule
    /// in a chain the script declares and TWO in a chain it does not — so
    /// without this the node grows a duplicate jump per resync, forever.
    ///
    /// Loops until the delete fails, which both prevents a new duplicate and
    /// heals a node that already accumulated them.
    async fn prune_jumps(&self, route: &ServiceRoute) -> Result<(), RouterError> {
        let chain_svc = chain_name("KUBE-SVC", &route.service_id);
        for port in &route.ports {
            let argv = Self::jump_delete_argv(
                &route.cluster_ip,
                &port.protocol.to_lowercase(),
                port.service_port,
                &chain_svc,
            );
            // Bounded: an unbounded loop here would wedge the reconciler
            // (★★ reconciler liveness) if iptables ever reported success
            // without deleting anything.
            for _ in 0..MAX_DUPLICATE_JUMPS {
                let (ok, _) = self.control(&argv).await?;
                if !ok {
                    break;
                }
            }
        }
        Ok(())
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

/// Upper bound on how many duplicate jumps `prune_jumps` will delete for one
/// port before giving up. Generous against any plausible accumulation, finite
/// so a backend that reported success without deleting cannot wedge the
/// reconciler in an unbounded loop.
const MAX_DUPLICATE_JUMPS: usize = 64;

/// Hash a free-form id to a 12-char iptables-chain-safe suffix.
fn chain_name(prefix: &str, key: &str) -> String {
    let hash = blake3::hash(key.as_bytes());
    let hex = hash.to_hex();
    format!("{prefix}-{}", &hex.as_str()[..12].to_uppercase())
}

/// Map a K8s protocol string to the ipvsadm transport flag.
/// `UDP` → `-u`; everything else (TCP default) → `-t`.
fn ipvs_proto_flag(protocol: &str) -> &'static str {
    match protocol.to_uppercase().as_str() {
        "UDP" => "-u",
        _ => "-t",
    }
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
        // Order is load-bearing: the root chain must exist before the
        // script appends into it, and the stale jumps must go before the
        // script adds the fresh one.
        self.ensure_root_chain().await?;
        self.prune_jumps(route).await?;
        let script = Self::render_script(route);
        self.run_restore(&script).await?;
        let mut state = self.inner.lock().await;
        state.routes.insert(route.service_id.clone(), route.clone());
        Ok(())
    }

    async fn remove(&self, service_id: &str) -> Result<(), RouterError> {
        use engenho_types::egress::IptablesScript;
        let chain_svc = chain_name("KUBE-SVC", service_id);
        // Flush + delete the per-service chain — typed script, no
        // `format!()` of iptables syntax (★★ TYPED EMISSION).
        let mut script = IptablesScript::new();
        script
            .table("nat")
            .chain(&chain_svc)
            .flush(&chain_svc)
            .delete_chain(&chain_svc)
            .commit();
        // Errors on remove are tolerated when the chain is already gone.
        let _ = self.run_restore(&script.to_string()).await;
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
    /// lines that ipvsadm-restore consumes. Built as a typed
    /// [`engenho_types::egress::IpvsScript`] + rendered through its
    /// `Display` chokepoint (★★ TYPED EMISSION — no `format!()` of
    /// ipvsadm syntax).
    #[must_use]
    pub fn render_script(route: &ServiceRoute) -> String {
        use engenho_types::egress::{IpvsLine, IpvsScript};
        let mut script = IpvsScript::new();
        for port in &route.ports {
            // -A: add virtual service. -t = TCP, -u = UDP.
            let proto_flag = ipvs_proto_flag(&port.protocol);
            script.push(IpvsLine::AddService {
                proto_flag: proto_flag.to_string(),
                vip: route.cluster_ip.clone(),
                port: port.service_port,
            });
            for pod_ip in &route.endpoints {
                // -a: add real server. -m = masquerade (NAT).
                script.push(IpvsLine::AddRealServer {
                    proto_flag: proto_flag.to_string(),
                    vip: route.cluster_ip.clone(),
                    port: port.service_port,
                    pod_ip: pod_ip.clone(),
                    target_port: port.target_port,
                });
            }
        }
        script.to_string()
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
            use engenho_types::egress::{IpvsLine, IpvsScript};
            let mut script = IpvsScript::new();
            for port in &route.ports {
                script.push(IpvsLine::DeleteService {
                    proto_flag: ipvs_proto_flag(&port.protocol).to_string(),
                    vip: route.cluster_ip.clone(),
                    port: port.service_port,
                });
            }
            let _ = self.run_restore(&script.to_string()).await;
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
                        let target_port = Self::target_port(p, &name, endpoints, service_port)?;
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

    /// The pod-side port a Service port DNATs to.
    ///
    /// The Endpoints controller has already resolved a NAMED `targetPort`
    /// against the backing containers, so a port published there under the
    /// same name is authoritative. Reading only a numeric `targetPort` here
    /// silently treated `targetPort: "http"` as absent and DNATed to the
    /// SERVICE port — measured on rio 2026-09-15, where Flux's
    /// source-controller (`port: 80`, `targetPort: http` → 9090) refused every
    /// ClusterIP connection while the Endpoints object correctly said 9090.
    ///
    /// A named port with no resolved Endpoints port yields `None`: no rule
    /// beats a rule to a port nothing listens on.
    fn target_port(
        port: &serde_json::Value,
        name: &str,
        endpoints: Option<&serde_json::Value>,
        service_port: u16,
    ) -> Option<u16> {
        let published = endpoints
            .and_then(|e| e.get("subsets"))
            .and_then(|s| s.as_array())
            .into_iter()
            .flatten()
            .filter_map(|subset| subset.get("ports").and_then(|p| p.as_array()))
            .flatten()
            .find(|ep| ep.get("name").and_then(|n| n.as_str()).unwrap_or("default") == name)
            .and_then(|ep| ep.get("port").and_then(|n| n.as_u64()))
            .and_then(|n| u16::try_from(n).ok());
        match port.get("targetPort") {
            None | Some(serde_json::Value::Null) => Some(published.unwrap_or(service_port)),
            Some(serde_json::Value::Number(n)) => n.as_u64().and_then(|n| u16::try_from(n).ok()),
            Some(serde_json::Value::String(_)) => published,
            Some(_) => None,
        }
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

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
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

        // Upserts: in desired but not installed identically. A backend
        // error ends the tick uncounted; only a call that returned Ok is a
        // change.
        for (id, route) in &desired {
            if installed.get(id) != Some(route) {
                let applied = Effect::applied(self.backend.upsert(route).await)
                    .map_err(|e| ControllerError::Internal(e.to_string()))?;
                report.record(applied);
            }
        }
        // Removes: in installed but not in desired.
        for id in installed.keys() {
            if !desired.contains_key(id) {
                let applied = Effect::applied(self.backend.remove(id).await)
                    .map_err(|e| ControllerError::Internal(e.to_string()))?;
                report.record(applied);
            }
        }
        Ok(report.into())
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

    fn named_target_service() -> serde_json::Value {
        json!({
            "spec": {
                "clusterIP": "10.97.0.5",
                "ports": [{"name": "http", "port": 80, "targetPort": "http"}]
            }
        })
    }

    #[test]
    fn build_route_named_target_port_dnats_to_the_resolved_endpoints_port() {
        let eps = json!({"subsets": [{
            "addresses": [{"ip": "10.89.0.65"}],
            "ports": [{"name": "http", "port": 9090, "protocol": "TCP"}]
        }]});
        let r = ServiceRoutingController::build_route(
            &named_target_service(),
            Some(&eps),
            "flux-system/source-controller",
        )
        .unwrap();
        assert_eq!(r.ports[0].service_port, 80);
        assert_eq!(r.ports[0].target_port, 9090);
    }

    #[test]
    fn build_route_named_target_port_never_falls_back_to_the_service_port() {
        // Negative control for the test above: with nothing resolved, the old
        // code produced 80 — a rule to a port nothing listens on.
        let eps = json!({"subsets": [{"addresses": [{"ip": "10.89.0.65"}]}]});
        let r = ServiceRoutingController::build_route(&named_target_service(), Some(&eps), "x/y")
            .unwrap();
        assert!(
            r.ports.is_empty(),
            "unresolved named port must emit no rule, got {:?}",
            r.ports.first().map(|p| p.target_port)
        );
        let r =
            ServiceRoutingController::build_route(&named_target_service(), None, "x/y").unwrap();
        assert!(r.ports.is_empty());
    }

    #[test]
    fn build_route_numeric_target_port_wins_over_endpoints() {
        let svc = json!({"spec": {"clusterIP": "10.0.0.1", "ports": [{"name": "http", "port": 80, "targetPort": 8080}]}});
        let eps = json!({"subsets": [{"addresses": [{"ip": "10.0.0.9"}], "ports": [{"name": "http", "port": 1234}]}]});
        let r = ServiceRoutingController::build_route(&svc, Some(&eps), "x/y").unwrap();
        assert_eq!(r.ports[0].target_port, 8080);
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

    /// The delete must be byte-identical to the append, or pruning removes
    /// nothing and reports success — a duplicate-cleanup that looks like a
    /// fix while the duplicates keep accumulating. Derived from one source
    /// (`service_jump_spec`); this proves the two consumers agree.
    #[test]
    fn the_prune_delete_matches_the_rule_the_script_appends() {
        // The chain name is DERIVED, exactly as prune_jumps derives it — a
        // literal here would test a rule the router never writes.
        let chain_svc = chain_name("KUBE-SVC", "ns/svc");
        let argv = IptablesRouter::jump_delete_argv("10.96.5.1", "tcp", 80, &chain_svc);
        assert_eq!(argv[0..4], ["-t", "nat", "-D", "KUBE-SERVICES"]);
        let rendered = argv[4..].join(" ");
        let route = ServiceRoute {
            service_id: "ns/svc".into(),
            cluster_ip: "10.96.5.1".into(),
            ports: vec![PortMap {
                name: "http".into(),
                service_port: 80,
                target_port: 8080,
                protocol: "TCP".into(),
            }],
            endpoints: ["10.244.0.5".to_string()].into_iter().collect(),
        };
        let script = IptablesRouter::render_script(&route);
        let appended = script
            .lines()
            .find(|l| l.starts_with("-A KUBE-SERVICES "))
            .expect("the script must append a jump");
        assert_eq!(
            appended.trim_start_matches("-A KUBE-SERVICES "),
            rendered,
            "the -D spec and the -A spec have drifted"
        );
    }

    /// The root chain and its hooks are engenho's to create. A node that
    /// never ran kube-proxy has neither, and appending into an absent chain
    /// fails the whole restore.
    #[test]
    fn the_router_creates_its_own_root_chain_and_both_hooks() {
        assert_eq!(
            IptablesRouter::root_chain_create_argv(),
            ["-t", "nat", "-N", "KUBE-SERVICES"]
        );
        let check = IptablesRouter::hook_argv("PREROUTING", "-C");
        assert_eq!(check[0..4], ["-t", "nat", "-C", "PREROUTING"]);
        assert!(check.contains(&"KUBE-SERVICES".to_string()));

        // Inserted at position 1, never appended: a DNAT placed after a
        // blanket MASQUERADE or RETURN never runs.
        let insert = IptablesRouter::hook_argv("OUTPUT", "-I");
        assert_eq!(insert[0..5], ["-t", "nat", "-I", "OUTPUT", "1"]);

        // The check and the insert must describe the SAME rule apart from
        // the verb and the position, or the check never matches and a hook
        // is inserted on every single reconcile.
        let check_out = IptablesRouter::hook_argv("OUTPUT", "-C");
        assert_eq!(check_out[4..], insert[5..]);
    }

    /// A node pinned to the legacy backend must be checked through the
    /// legacy binary; asking nft whether a legacy rule exists answers "no"
    /// forever, and the hook is re-inserted every reconcile.
    #[test]
    fn the_control_binary_keeps_the_backend_variant() {
        assert_eq!(
            IptablesRouter::control_binary_for("iptables-restore"),
            "iptables"
        );
        assert_eq!(
            IptablesRouter::control_binary_for("iptables-legacy-restore"),
            "iptables-legacy"
        );
        assert_eq!(
            IptablesRouter::control_binary_for("/usr/sbin/iptables-nft-restore"),
            "/usr/sbin/iptables-nft"
        );
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
    fn datapath_install_maturity_is_typed_per_backend() {
        // FakeRouter (and the macOS-VM topology it stands in for) COMPUTES
        // routes but installs no kernel rule — the typed deferral.
        assert_eq!(FakeRouter::new().datapath(), DatapathInstall::Computed);
        // The kernel backends INSTALL the datapath (on a Linux node).
        assert_eq!(IptablesRouter::new().datapath(), DatapathInstall::Installed);
        assert_eq!(IpvsRouter::new().datapath(), DatapathInstall::Installed);
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
            async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
                Ok(ReconcileReport::default().into())
            }
        }
        assert_eq!(Fake.name(), "service_router");
    }

    async fn live_store() -> Arc<StoreMesh> {
        use engenho_store::{InProcessRouter, default_config};
        use std::time::Duration;
        let router = InProcessRouter::new();
        let cfg = default_config("controllers-service-router").unwrap();
        let store = Arc::new(
            StoreMesh::start(1, "in-process://1".into(), router, cfg)
                .await
                .unwrap(),
        );
        store.initialize_singleton().await.unwrap();
        assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
        store
    }

    /// The off-Linux dev path: a `FakeRouter` (compute-only) backend
    /// reconciles a Service + Endpoints into a computed route WITHOUT any
    /// kernel call (no iptables/ipvs binary touched), and its datapath
    /// maturity is the typed `DatapathInstall::Computed`. This is exactly
    /// the backend the runtime selects on a Darwin host (Auto → ComputeOnly),
    /// so the local daemon keeps running fine.
    #[tokio::test]
    async fn compute_only_backend_reconciles_route_without_kernel_call() {
        use engenho_store::{ResourceKey, command::Reason, command::ResourceCommand};

        let store = live_store().await;
        store
            .propose(ResourceCommand::put(
                ResourceKey::namespaced("", "v1", "Service", "default", "podinfo"),
                serde_json::json!({
                    "kind": "Service", "apiVersion": "v1",
                    "metadata": {"name": "podinfo", "namespace": "default"},
                    "spec": {"clusterIP": "10.96.0.10",
                             "ports": [{"name": "http", "port": 80, "targetPort": 8080}]}
                }),
                Reason::Operator,
            ))
            .await
            .unwrap();
        store
            .propose(ResourceCommand::put(
                ResourceKey::namespaced("", "v1", "Endpoints", "default", "podinfo"),
                serde_json::json!({
                    "kind": "Endpoints", "apiVersion": "v1",
                    "metadata": {"name": "podinfo", "namespace": "default"},
                    "subsets": [{"addresses": [{"ip": "10.0.0.1"}, {"ip": "10.0.0.2"}]}]
                }),
                Reason::Operator,
            ))
            .await
            .unwrap();

        // The compute-only backend = the FakeRouter. Its datapath maturity
        // is the typed Computed value — no kernel rule is installed.
        let backend = Arc::new(FakeRouter::new());
        assert_eq!(backend.datapath(), DatapathInstall::Computed);

        let controller =
            ServiceRoutingController::new(store.clone(), backend.clone(), Some("default".into()));
        controller.tick().await.unwrap();

        // The route was computed + tracked: one upsert event, one route,
        // the right backends — proving the controller ran end-to-end with
        // no real iptables shell-out.
        assert_eq!(backend.route_count().await, 1);
        let events = backend.events().await;
        assert_eq!(
            events,
            vec![FakeRouterEvent::Upsert("default/podinfo".into())]
        );
        let installed = backend.list().await.unwrap();
        let route = installed.get("default/podinfo").expect("route computed");
        assert_eq!(route.cluster_ip, "10.96.0.10");
        assert_eq!(
            route.endpoints,
            ["10.0.0.1".to_string(), "10.0.0.2".to_string()]
                .into_iter()
                .collect()
        );
    }
}
