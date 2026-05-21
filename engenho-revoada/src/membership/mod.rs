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
use chitchat::{
    spawn_chitchat, ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig,
};
use engenho_types::primitives::Quantity;
use serde::{Deserialize, Serialize};
use tokio::sync::{watch, Mutex};

use crate::NodeId;

/// Operator-facing config to start a `GossipMesh`.
#[derive(Clone, Debug)]
pub struct GossipConfig {
    /// This node's typed identity.
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
    /// Construct a config with sane defaults for an engenho node.
    #[must_use]
    pub fn new(
        node_id: NodeId,
        gossip_addr: SocketAddr,
        initial_state: NodeState,
    ) -> Self {
        Self {
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
}

/// The chitchat KV key under which we publish the serde-JSON of `NodeState`.
const STATE_KEY: &str = "engenho.revoada.node";

impl GossipMesh {
    /// Construct + spawn the gossip server. Returns once chitchat
    /// has bound its UDP socket and the bridge task is running.
    ///
    /// # Errors
    ///
    /// Returns [`MembershipError::StartFailed`] if chitchat can't
    /// bind the socket or if any other startup step fails.
    pub async fn start(config: GossipConfig) -> Result<Self, MembershipError> {
        let chitchat_id = ChitchatId::new(
            config.node_id.to_hex(),
            unix_seconds(),
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

        let initial_blob = serde_json::to_string(&config.initial_state)?;
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
                let state: NodeState = serde_json::from_str(blob)?;
                return Ok(Some(state));
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
        let blob = serde_json::to_string(state)?;
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
        let Ok(state) = serde_json::from_str::<NodeState>(blob) else {
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

fn unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

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
        let cfg = GossipConfig::new(
            NodeId::new([1; 32]),
            "127.0.0.1:7800".parse().unwrap(),
            test_state(NodeId::new([1; 32])),
        )
        .with_seeds(vec!["127.0.0.1:7801".into()])
        .with_cluster_id("test-cluster");
        assert_eq!(cfg.seed_nodes, vec!["127.0.0.1:7801".to_string()]);
        assert_eq!(cfg.cluster_id, "test-cluster");
        assert_eq!(cfg.phi_threshold, 8.0);
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
