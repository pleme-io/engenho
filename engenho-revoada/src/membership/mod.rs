//! Layer A — membership + failure detection.
//!
//! Wraps a [`chitchat`] gossip mesh: every engenho node runs a
//! `GossipMesh` endpoint; the scuttlebutt protocol propagates
//! per-node state and the phi-accrual failure detector decides when
//! a node is suspected vs. confirmed dead.
//!
//! The public surface here intentionally mirrors the shape we'd
//! want REGARDLESS of which gossip library backs it. If chitchat
//! gets replaced later (e.g. by a libp2p-based gossip) the trait
//! `MembershipObservable` is the integration point — Layers B
//! (consensus) and C (content sync) only see the trait.
//!
//! ## Typed surface
//!
//! [`GossipMesh::start`] takes a typed [`GossipConfig`], spawns the
//! chitchat server, and returns a handle that owns the gossip
//! loop. The handle exposes:
//!
//!   * `local_state` — read what this node currently advertises
//!   * `update_local_state` — atomically swap the advertised state
//!   * `peers` — snapshot of all known live peers + their last
//!     advertised state
//!   * `subscribe` — `tokio::sync::watch::Receiver` that streams
//!     `MembershipView` changes (drop in any task to react)
//!   * `shutdown` — graceful gossip termination
//!
//! ## Key-value serialization
//!
//! chitchat is a string-keyed KV store under the hood. The mesh
//! advertises a single key `"engenho.revoada.node"` whose value
//! is a serde_json encoding of [`NodeState`]. R5+ may break this
//! into smaller keys if delta efficiency matters; for the typical
//! ~few-hundred-byte NodeState the single-blob shape is simpler
//! and chitchat handles deltas at the message layer.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use chitchat::transport::UdpTransport;
use chitchat::{ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig, spawn_chitchat};
use engenho_types::primitives::Quantity;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, watch};

use crate::NodeId;
use crate::attestation::{NodeIdentity, verify_signature};

/// Operator-facing config to start a `GossipMesh`.
#[derive(Clone, Debug)]
pub struct GossipConfig {
    /// This node's ed25519 keypair. Signs every gossiped [`NodeState`];
    /// `node_id` is *derived* from it (`identity.node_id()`), so a config
    /// with a `node_id` it can't sign for is unrepresentable. Reuses the
    /// Layer-D attestation [`NodeIdentity`] — membership is its second
    /// consumer.
    pub identity: Arc<NodeIdentity>,
    /// This node's typed identity (= `identity.node_id()`, the ed25519 public key).
    pub node_id: NodeId,
    /// The UDP socket chitchat listens on + advertises.
    pub gossip_addr: SocketAddr,
    /// Initial peer addresses to bootstrap from. Can be empty for
    /// a singleton (first node in the mesh); for any node after the
    /// first, ≥1 reachable seed is required.
    pub seed_nodes: Vec<String>,
    /// Cluster identifier — nodes only gossip with peers on the
    /// same cluster_id. Different engenho clusters can share a
    /// network without merging.
    pub cluster_id: String,
    /// How often to send gossip messages. chitchat default 1s;
    /// we default to 500ms for faster failure detection at the
    /// cost of slightly more network chatter.
    pub gossip_interval: Duration,
    /// Phi-accrual threshold for "suspected dead". Lower = more
    /// jumpy; higher = slower to call a node dead. chitchat
    /// default 8.0.
    pub phi_threshold: f64,
    /// Grace period before a marked-dead node's state is GC'd.
    /// Default 30s.
    pub marked_for_deletion_grace_period: Duration,
    /// Initial NodeState to advertise. Updates flow through
    /// [`GossipMesh::update_local_state`] after start.
    pub initial_state: NodeState,
}

impl GossipConfig {
    /// Construct a config with sane defaults for an engenho node. `node_id` is
    /// derived from `identity` (the ed25519 public key), so it always matches the
    /// key that will sign this node's gossip.
    #[must_use]
    pub fn new(
        identity: Arc<NodeIdentity>,
        gossip_addr: SocketAddr,
        initial_state: NodeState,
    ) -> Self {
        let node_id = identity.node_id();
        Self {
            identity,
            node_id,
            gossip_addr,
            seed_nodes: Vec::new(),
            cluster_id: "engenho-default".to_string(),
            gossip_interval: Duration::from_millis(500),
            phi_threshold: 8.0,
            marked_for_deletion_grace_period: Duration::from_secs(30),
            initial_state,
        }
    }

    /// Builder helper to set seeds.
    #[must_use]
    pub fn with_seeds(mut self, seeds: Vec<String>) -> Self {
        self.seed_nodes = seeds;
        self
    }

    /// Builder helper to set cluster id.
    #[must_use]
    pub fn with_cluster_id(mut self, cluster_id: impl Into<String>) -> Self {
        self.cluster_id = cluster_id.into();
        self
    }
}

/// Per-node state gossiped across the mesh.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeState {
    pub node_id: NodeId,
    pub gossip_addr: String,
    /// Raft listen address — populated once Layer B is wired.
    /// `None` if this node isn't participating in the Raft group.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raft_addr: Option<String>,
    pub roles: BTreeSet<NodeRole>,
    pub capacity: NodeCapacity,
    pub k8s_version: String,
    pub uptime_sec: u64,
    /// Monotonic counter incremented on every confirmed role change.
    /// Lets stale gossip be detected during merge.
    pub membership_generation: u64,
}

impl NodeState {
    /// Deterministic canonical bytes for signing/verification. serde_json over a
    /// struct is field-order-stable and `BTreeSet<NodeRole>` serializes sorted, so
    /// the same `NodeState` always produces the same bytes — sign-then-verify holds
    /// across the gossip round-trip.
    fn canonical_bytes(&self) -> Result<Vec<u8>, MembershipError> {
        Ok(serde_json::to_vec(self)?)
    }
}

/// The signed wire envelope every gossiped [`NodeState`] travels in. Carries the
/// node's ed25519 signature (by its own identity key) over the state's canonical
/// bytes. On ingest the signature is verified against `state.node_id` (which *is*
/// the public key), so a peer cannot forge another node's state without that node's
/// secret key — the load-bearing property for an untrusted mesh. Unsigned or
/// wrongly-signed gossip is dropped at the deserialization boundary
/// (parse-time-rejected; it never enters a [`MembershipView`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SignedNodeState {
    state: NodeState,
    /// ed25519 signature (64 bytes) over `state.canonical_bytes()`.
    signature: Vec<u8>,
}

impl SignedNodeState {
    /// Sign `state` with `identity`. The signer MUST be the identity named by
    /// `state.node_id` (a node signs its own state under its own key); a debug
    /// assertion guards the misuse.
    fn sign(state: NodeState, identity: &NodeIdentity) -> Result<Self, MembershipError> {
        debug_assert_eq!(
            identity.node_id(),
            state.node_id,
            "a node must sign its own state under its own identity key"
        );
        let signature = identity.sign(&state.canonical_bytes()?).to_vec();
        Ok(Self { state, signature })
    }

    /// Verify and unwrap. Returns the inner [`NodeState`] only if the signature is a
    /// valid ed25519 signature, by `state.node_id`'s key, over the state's canonical
    /// bytes. Any failure (wrong length, bad key, bad signature, re-encode error)
    /// returns `None` — the entry is dropped, never trusted.
    fn into_verified(self) -> Option<NodeState> {
        let sig: [u8; 64] = self.signature.as_slice().try_into().ok()?;
        let bytes = self.state.canonical_bytes().ok()?;
        verify_signature(&self.state.node_id, &bytes, &sig).ok()?;
        Some(self.state)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    ApiServer,
    Etcd,
    Scheduler,
    ControllerManager,
    Worker,
    Quarantined,
    Observer,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCapacity {
    pub cpu: Quantity,
    pub memory: Quantity,
    pub storage: Quantity,
    pub pods: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeHealth {
    Healthy,
    Suspect,
    Dead,
}

/// Snapshot of the mesh's current membership view. Streamed via
/// [`GossipMesh::subscribe`].
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct MembershipView {
    pub members: Vec<MembershipEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MembershipEntry {
    pub node_id: NodeId,
    pub gossip_addr: String,
    pub state: NodeState,
}

#[derive(Debug, thiserror::Error)]
pub enum MembershipError {
    #[error("chitchat startup failed: {0}")]
    StartFailed(#[source] anyhow::Error),
    #[error("could not encode NodeState: {0}")]
    Encode(#[from] serde_json::Error),
    #[error("invalid config: {0}")]
    Invalid(String),
}

engenho_substrate::impl_error_kind! {
    MembershipError {
        (StartFailed(_)) => "start_failed",
        (Encode(_)) => "encode",
        (Invalid(_)) => "invalid",
    }
}

/// Wraps a running chitchat instance + a derived `MembershipView`
/// watcher that subscribers can clone to react to membership
/// changes.
pub struct GossipMesh {
    handle: ChitchatHandle,
    /// Background task continuously transforms chitchat's
    /// `BTreeMap<ChitchatId, chitchat::NodeState>` watcher into a
    /// typed `MembershipView`.
    view_tx: Arc<Mutex<watch::Sender<MembershipView>>>,
    view_rx: watch::Receiver<MembershipView>,
    local_node_id: NodeId,
    /// This node's signing identity — re-signs state on every
    /// [`GossipMesh::update_local_state`].
    identity: Arc<NodeIdentity>,
}

/// The chitchat KV key under which we publish the serde-JSON of `NodeState`.
const STATE_KEY: &str = "engenho.revoada.node";

impl GossipMesh {
    /// Construct + spawn the gossip server. Returns once chitchat
    /// has bound its UDP socket and the bridge task is running.
    ///
    /// Defaults to `WallClock` for the ChitchatId generation timestamp.
    /// Tests pinning the timestamp should use [`Self::start_with_clock`].
    ///
    /// # Errors
    ///
    /// Returns [`MembershipError::StartFailed`] if chitchat can't
    /// bind the socket or if any other startup step fails.
    pub async fn start(config: GossipConfig) -> Result<Self, MembershipError> {
        Self::start_with_clock(config, std::sync::Arc::new(engenho_substrate::WallClock)).await
    }

    /// Construct + spawn with an explicit typed `Clock` for the
    /// ChitchatId generation timestamp. Pattern #7 (concrete-first,
    /// trait-back) — production unchanged via `start()`; tests get
    /// determinism via `FrozenClock`. Closes the v0.62/v0.74
    /// membership backlog.
    ///
    /// # Errors
    /// Same as [`Self::start`].
    pub async fn start_with_clock(
        config: GossipConfig,
        clock: std::sync::Arc<dyn engenho_substrate::Clock>,
    ) -> Result<Self, MembershipError> {
        let chitchat_id = ChitchatId::new(
            config.node_id.to_hex(),
            clock.unix_secs(),
            config.gossip_addr,
        );

        let cc_config = ChitchatConfig {
            chitchat_id: chitchat_id.clone(),
            cluster_id: config.cluster_id.clone(),
            gossip_interval: config.gossip_interval,
            listen_addr: config.gossip_addr,
            seed_nodes: config.seed_nodes.clone(),
            failure_detector_config: FailureDetectorConfig {
                phi_threshold: config.phi_threshold,
                initial_interval: config.gossip_interval,
                ..Default::default()
            },
            marked_for_deletion_grace_period: config.marked_for_deletion_grace_period,
            catchup_callback: None,
            extra_liveness_predicate: None,
        };

        let signed_initial = SignedNodeState::sign(config.initial_state.clone(), &config.identity)?;
        let initial_blob = serde_json::to_string(&signed_initial)?;
        let initial_kvs = vec![(STATE_KEY.to_string(), initial_blob)];

        let transport = UdpTransport;
        let handle = spawn_chitchat(cc_config, initial_kvs, &transport)
            .await
            .map_err(MembershipError::StartFailed)?;

        let live_watcher = {
            let cc = handle.chitchat();
            let cc = cc.lock().await;
            cc.live_nodes_watcher()
        };

        let (view_tx, view_rx) = watch::channel(MembershipView::default());
        let view_tx_arc = Arc::new(Mutex::new(view_tx));
        let view_tx_clone = view_tx_arc.clone();

        // Bridge task: every time chitchat publishes a new live-nodes
        // map, decode it into a typed MembershipView and broadcast.
        tokio::spawn(async move {
            let mut rx = live_watcher;
            loop {
                let snapshot = rx.borrow().clone();
                let view = build_view(&snapshot);
                {
                    let sender = view_tx_clone.lock().await;
                    let _ = sender.send(view);
                }
                if rx.changed().await.is_err() {
                    break;
                }
            }
        });

        let _ = chitchat_id; // kept for traceability — chitchat_id is logged on its own
        Ok(Self {
            handle,
            view_tx: view_tx_arc,
            view_rx,
            local_node_id: config.node_id,
            identity: config.identity,
        })
    }

    /// Subscribe to membership changes. The returned watcher is
    /// borrow-able for the latest snapshot + can be `.changed().await`'d
    /// for the next update.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<MembershipView> {
        self.view_rx.clone()
    }

    /// Snapshot of the current membership view (synchronous).
    #[must_use]
    pub fn current_view(&self) -> MembershipView {
        self.view_rx.borrow().clone()
    }

    /// Read this node's currently-advertised state.
    pub async fn local_state(&self) -> Result<Option<NodeState>, MembershipError> {
        let cc = self.handle.chitchat();
        let cc = cc.lock().await;
        let chitchat_id = cc.self_chitchat_id().clone();
        if let Some(node) = cc.node_state(&chitchat_id) {
            if let Some(blob) = node.get(STATE_KEY) {
                let signed: SignedNodeState = serde_json::from_str(blob)?;
                // Our own state must verify under our own key.
                return Ok(signed.into_verified());
            }
        }
        Ok(None)
    }

    /// Atomically swap the advertised state. Triggers a gossip
    /// round so neighbors learn it within `gossip_interval`.
    ///
    /// # Errors
    ///
    /// Returns [`MembershipError::Encode`] if `state` doesn't
    /// serialize to JSON.
    pub async fn update_local_state(&self, state: &NodeState) -> Result<(), MembershipError> {
        let signed = SignedNodeState::sign(state.clone(), &self.identity)?;
        let blob = serde_json::to_string(&signed)?;
        let cc = self.handle.chitchat();
        let mut cc = cc.lock().await;
        cc.self_node_state().set(STATE_KEY, blob);
        Ok(())
    }

    /// All known live peers (excluding self, excluding dead).
    #[must_use]
    pub async fn peers(&self) -> Vec<MembershipEntry> {
        let view = self.current_view();
        view.members
            .into_iter()
            .filter(|m| m.node_id != self.local_node_id)
            .collect()
    }

    /// This node's typed identity.
    #[must_use]
    pub fn local_node_id(&self) -> NodeId {
        self.local_node_id
    }

    /// Wait for the live-nodes count (including self) to reach
    /// `target` or for `timeout` to elapse. Useful in bootstrap
    /// flows where R2 (consensus) waits for R1 (membership) to
    /// converge.
    pub async fn wait_for_members(
        &self,
        target: usize,
        timeout: Duration,
    ) -> Result<MembershipView, MembershipError> {
        let mut rx = self.view_rx.clone();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let view = rx.borrow().clone();
            if view.members.len() >= target {
                return Ok(view);
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(MembershipError::Invalid(format!(
                    "wait_for_members({target}) timed out at {} live members",
                    view.members.len()
                )));
            }
            // tokio::time::timeout works on Result so we wrap rx.changed.
            match tokio::time::timeout(remaining, rx.changed()).await {
                Ok(Ok(())) => continue,
                Ok(Err(_)) => {
                    return Err(MembershipError::Invalid("watch closed".into()));
                }
                Err(_) => {
                    return Err(MembershipError::Invalid(format!(
                        "wait_for_members({target}) timed out at {} live members",
                        view.members.len()
                    )));
                }
            }
        }
    }

    /// Gracefully shut down the gossip server. Idempotent.
    ///
    /// # Errors
    ///
    /// Returns the chitchat shutdown error if termination fails.
    pub async fn shutdown(self) -> Result<(), MembershipError> {
        let _ = self.view_tx.lock().await; // ensure no in-flight send
        self.handle
            .shutdown()
            .await
            .map_err(MembershipError::StartFailed)?;
        Ok(())
    }
}

/// Build a typed `MembershipView` from chitchat's raw snapshot.
fn build_view(
    snapshot: &std::collections::BTreeMap<ChitchatId, chitchat::NodeState>,
) -> MembershipView {
    let mut members = Vec::with_capacity(snapshot.len());
    for (cid, node) in snapshot {
        let Some(blob) = node.get(STATE_KEY) else {
            continue;
        };
        // Deserialize the signed envelope and verify the ed25519 signature against
        // the claimed node_id (= public key). Unsigned, wrongly-signed, or tampered
        // gossip is dropped here — it never enters the MembershipView.
        let Ok(signed) = serde_json::from_str::<SignedNodeState>(blob) else {
            continue;
        };
        let Some(state) = signed.into_verified() else {
            continue;
        };
        members.push(MembershipEntry {
            node_id: state.node_id,
            gossip_addr: cid.gossip_advertise_addr.to_string(),
            state,
        });
    }
    members.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    MembershipView { members }
}

// `unix_seconds()` helper deleted in v0.88 — replaced by
// `engenho_substrate::Clock::unix_secs()` plumbed through
// `GossipMesh::start_with_clock`.

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn node_role_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&NodeRole::ApiServer).unwrap(),
            "\"api_server\""
        );
        assert_eq!(
            serde_json::to_string(&NodeRole::ControllerManager).unwrap(),
            "\"controller_manager\""
        );
    }

    #[test]
    fn node_state_round_trips() {
        let state = NodeState {
            node_id: NodeId::new([1; 32]),
            gossip_addr: "192.168.64.10:7800".into(),
            raft_addr: Some("192.168.64.10:7900".into()),
            roles: [NodeRole::ApiServer, NodeRole::Etcd, NodeRole::Worker]
                .into_iter()
                .collect(),
            capacity: NodeCapacity {
                cpu: Quantity::from_str("4").unwrap(),
                memory: Quantity::from_str("8Gi").unwrap(),
                storage: Quantity::from_str("50Gi").unwrap(),
                pods: 32,
            },
            k8s_version: "v1.34.0".into(),
            uptime_sec: 3600,
            membership_generation: 7,
        };
        let json = serde_json::to_string(&state).unwrap();
        let back: NodeState = serde_json::from_str(&json).unwrap();
        assert_eq!(back, state);
    }

    #[test]
    fn gossip_config_builder() {
        let identity = Arc::new(NodeIdentity::from_seed([1; 32]));
        let node_id = identity.node_id();
        let cfg = GossipConfig::new(
            identity,
            "127.0.0.1:7800".parse().unwrap(),
            test_state(node_id),
        )
        .with_seeds(vec!["127.0.0.1:7801".into()])
        .with_cluster_id("test-cluster");
        assert_eq!(cfg.seed_nodes, vec!["127.0.0.1:7801".to_string()]);
        assert_eq!(cfg.cluster_id, "test-cluster");
        assert_eq!(cfg.phi_threshold, 8.0);
        // node_id is derived from the identity — they can never disagree.
        assert_eq!(cfg.node_id, node_id);
    }

    #[test]
    fn signed_state_round_trips_and_verifies() {
        let identity = NodeIdentity::from_seed([9; 32]);
        let state = test_state(identity.node_id());
        let signed = SignedNodeState::sign(state.clone(), &identity).unwrap();
        // A faithfully-signed envelope verifies and yields the original state.
        assert_eq!(signed.into_verified(), Some(state));
    }

    #[test]
    fn tampered_state_is_rejected() {
        let identity = NodeIdentity::from_seed([9; 32]);
        let mut signed =
            SignedNodeState::sign(test_state(identity.node_id()), &identity).unwrap();
        // Mutate the state after signing — the signature no longer matches.
        signed.state.uptime_sec = 999_999;
        assert_eq!(signed.into_verified(), None, "tampered state must be dropped");
    }

    #[test]
    fn forged_state_under_anothers_node_id_is_rejected() {
        // Attacker (key A) publishes a state CLAIMING to be victim (node_id B),
        // signing with their own key. Verification is against B's key, which the
        // attacker does not hold — so it is dropped.
        let attacker = NodeIdentity::from_seed([0xaa; 32]);
        let victim = NodeIdentity::from_seed([0xbb; 32]);
        let mut victim_state = test_state(victim.node_id());
        victim_state.roles = [NodeRole::ApiServer, NodeRole::Etcd].into_iter().collect();
        // Build the envelope by hand: claims node_id = victim, signed by attacker.
        let bytes = victim_state.canonical_bytes().unwrap();
        let forged = SignedNodeState {
            state: victim_state,
            signature: attacker.sign(&bytes).to_vec(),
        };
        assert_eq!(
            forged.into_verified(),
            None,
            "a state signed by the wrong key must not verify against the claimed node_id"
        );
    }

    #[test]
    fn malformed_signature_is_rejected() {
        let identity = NodeIdentity::from_seed([9; 32]);
        let bad = SignedNodeState {
            state: test_state(identity.node_id()),
            signature: vec![0u8; 10], // wrong length
        };
        assert_eq!(bad.into_verified(), None);
    }

    pub(crate) fn test_state(node_id: NodeId) -> NodeState {
        NodeState {
            node_id,
            gossip_addr: "127.0.0.1:0".into(),
            raft_addr: None,
            roles: [NodeRole::Worker].into_iter().collect(),
            capacity: NodeCapacity {
                cpu: Quantity::from_str("4").unwrap(),
                memory: Quantity::from_str("8Gi").unwrap(),
                storage: Quantity::from_str("50Gi").unwrap(),
                pods: 32,
            },
            k8s_version: "v1.34.0".into(),
            uptime_sec: 0,
            membership_generation: 0,
        }
    }
}
