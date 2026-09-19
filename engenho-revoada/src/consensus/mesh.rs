//! `RaftMesh` — the public wrapper that ties together
//! [`InMemoryStore`], [`InProcessRouter`], and openraft's `Raft<C>`
//! into a single ergonomic surface.
//!
//! At R2 the API is intentionally minimal:
//!
//!   * [`RaftMesh::start`] — boot a Raft node + register with a
//!     shared in-process router (the only network impl available
//!     this milestone)
//!   * [`RaftMesh::initialize`] — first-node bootstrap; commits
//!     the initial membership
//!   * [`RaftMesh::propose`] — submit a [`RoleAssignment`] to be
//!     committed via openraft's `client_write`
//!   * [`RaftMesh::current_shape`] — read the typed [`MeshShape`]
//!     after apply
//!   * [`RaftMesh::is_leader`] — query metrics for leadership state
//!
//! R3+ swaps the in-process router for a tonic-gRPC client and
//! exposes [`RaftMesh::add_voter`] / `remove_voter` for the
//! dynamic-membership policy engine.

use std::sync::Arc;
use std::time::Duration;

use engenho_substrate::{OwnedTask, TaskStop};
use openraft::raft::ClientWriteResponse;
use openraft::{BasicNode, Config, Raft};
use tokio::sync::mpsc;

use crate::attestation::{AttestationChain, NodeIdentity};
use crate::consensus::network::{InProcessRouter, RpcRequest};
use crate::consensus::store::InMemoryStore;
use crate::consensus::type_config::{ApplyResult, RaftNodeId, TypeConfig};
use crate::consensus::{MeshShape, RoleAssignment};

#[derive(Debug, thiserror::Error)]
pub enum RaftError {
    #[error("openraft config invalid: {0}")]
    ConfigInvalid(String),
    #[error("raft initialize failed: {0}")]
    InitializeFailed(String),
    #[error("client_write failed: {0}")]
    ClientWriteFailed(String),
    #[error("raft fatal: {0}")]
    Fatal(String),
}

engenho_substrate::impl_error_kind! {
    RaftError {
        (ConfigInvalid(_)) => "config_invalid",
        (InitializeFailed(_)) => "initialize_failed",
        (ClientWriteFailed(_)) => "client_write_failed",
        (Fatal(_)) => "fatal",
    }
}

/// Wrapping handle for a single engenho-revoada Raft node.
///
/// At R4.5 the attestation chain LIVES in the [`InMemoryStore`]
/// (the state machine), so every node — leader and follower —
/// builds its own auditor-verifiable chain by signing each
/// committed entry on apply. Per-node M-of-N attestation emerges
/// naturally from the parallel chains.
pub struct RaftMesh {
    raft: Raft<TypeConfig>,
    store: InMemoryStore,
    node_id: RaftNodeId,
    listen_addr: String,
    router: InProcessRouter,
    /// The inbound-RPC pump. It holds a `Raft` clone and this node's RPC
    /// receiver, so [`RaftMesh::terminate`] stops it and AWAITS it before
    /// shutting the core down; dropping the mesh aborts it.
    rpc_task: OwnedTask,
    /// Ed25519 keypair (clone — store owns the canonical copy).
    /// Exposed via `node_identity()` so external callers can sign
    /// detached payloads (e.g. saguão passport requests).
    identity: NodeIdentity,
}

impl RaftMesh {
    /// Boot a Raft node + register with the shared in-process router.
    ///
    /// Uses a freshly-generated [`NodeIdentity`] for attestation
    /// signing. Use [`start_with_identity`] to supply your own
    /// keypair (the common case in production where the identity
    /// is persisted via cofre).
    pub async fn start(
        node_id: RaftNodeId,
        listen_addr: String,
        router: InProcessRouter,
        config: Arc<Config>,
    ) -> Result<Self, RaftError> {
        Self::start_with_identity(
            node_id,
            listen_addr,
            router,
            config,
            NodeIdentity::generate(),
        )
        .await
    }

    /// Boot a Raft node with an operator-supplied identity.
    pub async fn start_with_identity(
        node_id: RaftNodeId,
        listen_addr: String,
        router: InProcessRouter,
        config: Arc<Config>,
        identity: NodeIdentity,
    ) -> Result<Self, RaftError> {
        // The store owns the identity now (R4.5) — apply() signs
        // every committed entry to this node's chain.
        let store = InMemoryStore::new(identity.clone());
        let log_store = store.clone();
        let state_machine = store.clone();

        let (tx_rpc, mut rx_rpc) = mpsc::channel::<RpcRequest>(256);

        let raft =
            Raft::<TypeConfig>::new(node_id, config, router.clone(), log_store, state_machine)
                .await
                .map_err(|e| RaftError::Fatal(e.to_string()))?;

        router.register(node_id, tx_rpc).await;

        // Spawn the request-handler loop that pumps incoming RPCs
        // from peers (via the router) into the local Raft instance.
        let raft_for_rpc = raft.clone();
        let rpc_task = OwnedTask::spawn(async move {
            while let Some(req) = rx_rpc.recv().await {
                match req {
                    RpcRequest::AppendEntries(rpc, reply) => {
                        if let Ok(resp) = raft_for_rpc.append_entries(rpc).await {
                            let _ = reply.send(resp);
                        }
                    }
                    RpcRequest::Vote(rpc, reply) => {
                        if let Ok(resp) = raft_for_rpc.vote(rpc).await {
                            let _ = reply.send(resp);
                        }
                    }
                    RpcRequest::InstallSnapshot(rpc, reply) => {
                        if let Ok(resp) = raft_for_rpc.install_snapshot(rpc).await {
                            let _ = reply.send(resp);
                        }
                    }
                }
            }
        });

        Ok(Self {
            raft,
            store,
            node_id,
            listen_addr,
            router,
            rpc_task,
            identity,
        })
    }

    /// First-time bootstrap: declare a singleton cluster with this
    /// node as the only voter. Subsequent voters join via
    /// `add_voter` after R3.
    pub async fn initialize_singleton(&self) -> Result<(), RaftError> {
        let mut members = std::collections::BTreeMap::new();
        members.insert(
            self.node_id,
            BasicNode {
                addr: self.listen_addr.clone(),
            },
        );
        self.raft
            .initialize(members)
            .await
            .map_err(|e| RaftError::InitializeFailed(e.to_string()))?;
        Ok(())
    }

    /// Bootstrap a multi-node cluster with the listed initial voters.
    pub async fn initialize_with_voters(
        &self,
        voters: Vec<(RaftNodeId, String)>,
    ) -> Result<(), RaftError> {
        let mut members = std::collections::BTreeMap::new();
        for (id, addr) in voters {
            members.insert(id, BasicNode { addr });
        }
        self.raft
            .initialize(members)
            .await
            .map_err(|e| RaftError::InitializeFailed(e.to_string()))?;
        Ok(())
    }

    /// Submit a typed `RoleAssignment` to be committed.
    ///
    /// At R4.5 chain-signing happens in the state machine's
    /// `apply()` on every node — leader AND followers. So whoever
    /// is leader at submit time, EVERY node's local chain grows
    /// when the commit applies. The auditor can verify ANY node's
    /// chain offline (each block signed by THAT node's identity).
    pub async fn propose(&self, cmd: RoleAssignment) -> Result<ApplyResult, RaftError> {
        let resp: ClientWriteResponse<TypeConfig> = self
            .raft
            .client_write(cmd)
            .await
            .map_err(|e| RaftError::ClientWriteFailed(e.to_string()))?;
        Ok(resp.data)
    }

    /// Snapshot of this node's local attestation chain. The chain
    /// is now owned by the state machine — RaftMesh forwards.
    pub fn attestation_chain(&self) -> &AttestationChain {
        self.store.attestation_chain()
    }

    /// This node's identity (public key only is exposed via
    /// `node_identity_public`).
    pub fn node_identity(&self) -> &NodeIdentity {
        &self.identity
    }

    /// Read-only snapshot of the typed mesh state.
    pub async fn current_shape(&self) -> MeshShape {
        self.store.current_shape().await
    }

    /// Is this node currently the leader?
    pub async fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    /// Wait for this node to become the leader OR for `timeout`
    /// to elapse. Returns `true` if leadership was acquired.
    pub async fn wait_for_leadership(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut rx = self.raft.metrics().clone();
        loop {
            if rx.borrow().current_leader == Some(self.node_id) {
                return true;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }
            if tokio::time::timeout(remaining, rx.changed()).await.is_err() {
                return false;
            }
        }
    }

    /// Wait until the state machine's `last_applied_index` reaches
    /// at least `target`, OR until `timeout` elapses. Used by tests
    /// to confirm a committed command has been applied.
    pub async fn wait_for_applied(&self, target: u64, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let shape = self.current_shape().await;
            if shape.last_applied_index >= target {
                return true;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(50.min(remaining.as_millis() as u64))).await;
        }
    }

    pub fn node_id(&self) -> RaftNodeId {
        self.node_id
    }

    /// Add a new voter to the cluster (called on the leader).
    pub async fn add_voter(&self, node_id: RaftNodeId, addr: String) -> Result<(), RaftError> {
        self.raft
            .add_learner(node_id, BasicNode { addr }, true)
            .await
            .map_err(|e| RaftError::ClientWriteFailed(format!("add_learner: {e}")))?;
        let mut new_members = self
            .raft
            .metrics()
            .borrow()
            .membership_config
            .voter_ids()
            .collect::<std::collections::BTreeSet<_>>();
        new_members.insert(node_id);
        self.raft
            .change_membership(new_members, false)
            .await
            .map_err(|e| RaftError::ClientWriteFailed(format!("change_membership: {e}")))?;
        Ok(())
    }

    /// Stop the Raft node + drop the router registration.
    ///
    /// On return the inbound-RPC pump is gone: its `Raft` clone and its RPC
    /// receiver have been dropped, so a peer still holding a route to this
    /// node sees it closed. Aborting the pump without awaiting it only held
    /// that by luck — when the core's own shutdown happened to yield — and a
    /// core that had already stopped (as after a fatal error) does not yield.
    pub async fn terminate(self) -> Result<(), RaftError> {
        self.router.deregister(self.node_id).await;
        if self.rpc_task.stop().await == TaskStop::Panicked {
            tracing::error!(
                node_id = self.node_id,
                "raft RPC pump had panicked before terminate; inbound RPCs were not being served"
            );
        }
        // openraft's `shutdown` is the canonical termination signal.
        let _ = self.raft.shutdown().await;
        Ok(())
    }
}

/// Default Raft Config suitable for tests + LAN clusters.
pub fn default_config(cluster_name: &str) -> Result<Arc<Config>, RaftError> {
    let cfg = Config {
        cluster_name: cluster_name.to_string(),
        heartbeat_interval: 250,
        election_timeout_min: 500,
        election_timeout_max: 1000,
        enable_tick: true,
        enable_heartbeat: true,
        enable_elect: true,
        ..Default::default()
    };
    let validated = cfg
        .validate()
        .map_err(|e| RaftError::ConfigInvalid(e.to_string()))?;
    Ok(Arc::new(validated))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Generous: only a pump that is never stopped exceeds it.
    const PUMP_GONE_WITHIN: Duration = Duration::from_secs(5);

    async fn started_mesh(router: &InProcessRouter, node_id: RaftNodeId) -> RaftMesh {
        let config = default_config("owned-rpc-pump").expect("valid test config");
        RaftMesh::start(node_id, "in-process://1".into(), router.clone(), config)
            .await
            .expect("mesh starts")
    }

    /// A peer that looked up this node's route before `terminate` must see
    /// that route closed the moment `terminate` returns. The core is stopped
    /// first, as after a fatal error: then shutdown has nothing to wait on,
    /// which is exactly when an un-awaited abort let `terminate` return with
    /// the pump still alive.
    #[tokio::test(flavor = "current_thread")]
    async fn terminate_closes_a_peers_route_even_when_the_core_had_already_stopped() {
        let router = InProcessRouter::new();
        let mesh = started_mesh(&router, 1).await;
        let peer_route = router.lookup(1).await.expect("node 1 is registered");

        mesh.raft.shutdown().await.expect("core stops");
        mesh.terminate().await.expect("terminate");

        assert!(
            peer_route.is_closed(),
            "terminate returned while the RPC pump still held this node's receiver"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminate_closes_a_peers_route_with_the_core_running() {
        let router = InProcessRouter::new();
        let mesh = started_mesh(&router, 1).await;
        let peer_route = router.lookup(1).await.expect("node 1 is registered");

        mesh.terminate().await.expect("terminate");

        assert!(
            peer_route.is_closed(),
            "terminate returned while the RPC pump still held this node's receiver"
        );
    }

    /// A mesh dropped without `terminate` must not leave its pump running
    /// forever, holding a `Raft` clone and serving a node nobody owns.
    #[tokio::test(flavor = "current_thread")]
    async fn dropping_a_mesh_stops_its_rpc_pump() {
        let router = InProcessRouter::new();
        let mesh = started_mesh(&router, 1).await;
        // The router keeps its own sender, so only stopping the pump can
        // close this route.
        let peer_route = router.lookup(1).await.expect("node 1 is registered");

        drop(mesh);

        assert!(
            tokio::time::timeout(PUMP_GONE_WITHIN, peer_route.closed())
                .await
                .is_ok(),
            "a dropped mesh left its RPC pump running"
        );
    }
}
