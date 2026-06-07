//! F2 — NATS-backed [`RaftNetwork`] sibling to the in-process router.
//!
//! Moves Raft RPC off the in-process `mpsc` channel onto NATS
//! subjects via [`engenho_teia::TeiaClient`]. Same `RaftNetworkFactory`
//! interface; runtime-pluggable.
//!
//! ## Subject grammar
//!
//! Per `engenho-teia`'s [`ClusterScope`]:
//!
//! ```text
//! engenho.{cluster}.raft.store.append.{target_node}
//! engenho.{cluster}.raft.store.vote.{target_node}
//! engenho.{cluster}.raft.store.snapshot.{target_node}
//! ```
//!
//! Each RPC encodes as a JSON message; the response rides NATS
//! request-reply (`nats request <subj> <payload>` waits for the
//! target's reply on the auto-generated inbox subject).
//!
//! ## Per-node subscriber
//!
//! Every node spawns a subscriber on
//! `engenho.{cluster}.raft.store.*.{my_node}` that decodes the JSON
//! envelope + replies in kind. That subscriber is **owned by the
//! Raft mesh** (see `StoreMesh::start_nats_listener`) — not
//! implemented in this commit, which ships the **client side**
//! (RaftNetwork + RaftNetworkFactory) + leaves the listener
//! wire-up for F2b.

use std::time::Duration;

use engenho_teia::subject::RaftGroup;
use engenho_teia::{ClusterScope, NodeId as TeiaNodeId, TeiaClient};
use openraft::BasicNode;
use openraft::error::{NetworkError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::{Deserialize, Serialize};

use crate::type_config::{RaftNodeId, TypeConfig};

/// Default RPC timeout — Raft expects sub-second responses for
/// healthy elections; 1s is a generous upper bound that still
/// lets phi-accrual fire on real network failures.
const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(1);

/// Translate a Raft `RaftNodeId` (u64) to teia's hex-string
/// [`TeiaNodeId`]. Zero-padded to 16 hex chars so the subject
/// shape is uniform regardless of magnitude.
fn raft_id_to_teia(id: RaftNodeId) -> TeiaNodeId {
    TeiaNodeId::from_hex(format!("{id:016x}"))
}

/// `RaftNetworkFactory` that hands out [`NatsRaftNetwork`] clients
/// pre-wired with the teia subject scope.
#[derive(Clone)]
pub struct NatsRaftNetworkFactory {
    client: TeiaClient,
    scope: ClusterScope,
    rpc_timeout: Duration,
}

impl NatsRaftNetworkFactory {
    /// Construct from a connected [`TeiaClient`]. Pulls the cluster
    /// scope off the client's own configuration.
    #[must_use]
    pub fn new(client: TeiaClient) -> Self {
        let scope = client.scope().clone();
        Self {
            client,
            scope,
            rpc_timeout: DEFAULT_RPC_TIMEOUT,
        }
    }

    /// Override the per-RPC timeout. Default is 1s.
    #[must_use]
    pub fn with_rpc_timeout(mut self, timeout: Duration) -> Self {
        self.rpc_timeout = timeout;
        self
    }
}

impl RaftNetworkFactory<TypeConfig> for NatsRaftNetworkFactory {
    type Network = NatsRaftNetwork;

    async fn new_client(&mut self, target: RaftNodeId, _node: &BasicNode) -> Self::Network {
        NatsRaftNetwork {
            target,
            target_teia: raft_id_to_teia(target),
            client: self.client.clone(),
            scope: self.scope.clone(),
            rpc_timeout: self.rpc_timeout,
        }
    }
}

/// Per-target Raft network handle. Holds a clone of the shared
/// teia client + the target node's subject suffix.
pub struct NatsRaftNetwork {
    target: RaftNodeId,
    target_teia: TeiaNodeId,
    client: TeiaClient,
    scope: ClusterScope,
    rpc_timeout: Duration,
}

impl NatsRaftNetwork {
    fn append_subject(&self) -> String {
        self.scope
            .raft_append(RaftGroup::Store, self.target_teia.clone())
    }

    fn vote_subject(&self) -> String {
        self.scope
            .raft_vote(RaftGroup::Store, self.target_teia.clone())
    }

    fn snapshot_subject(&self) -> String {
        self.scope
            .raft_snapshot(RaftGroup::Store, self.target_teia.clone())
    }
}

/// JSON envelope so the consumer side can pattern-match a single
/// type per subject. Mirrors `RpcRequest` in `network.rs` but
/// typed for serde.
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum NatsRpcEnvelope {
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<RaftNodeId>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
}

impl RaftNetwork<TypeConfig> for NatsRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<RaftNodeId>,
        RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>,
    > {
        let envelope = NatsRpcEnvelope::AppendEntries(rpc);
        self.client
            .request_json::<NatsRpcEnvelope, AppendEntriesResponse<RaftNodeId>>(
                self.append_subject(),
                &envelope,
                self.rpc_timeout,
            )
            .await
            .map_err(|e| {
                RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
                    "NATS append_entries to {} failed: {e}",
                    self.target
                ))))
            })
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<RaftNodeId>,
        RPCError<
            RaftNodeId,
            BasicNode,
            RaftError<RaftNodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        let envelope = NatsRpcEnvelope::InstallSnapshot(rpc);
        self.client
            .request_json::<NatsRpcEnvelope, InstallSnapshotResponse<RaftNodeId>>(
                self.snapshot_subject(),
                &envelope,
                self.rpc_timeout,
            )
            .await
            .map_err(|e| {
                RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
                    "NATS install_snapshot to {} failed: {e}",
                    self.target
                ))))
            })
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<RaftNodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<RaftNodeId>, RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>>
    {
        let envelope = NatsRpcEnvelope::Vote(rpc);
        self.client
            .request_json::<NatsRpcEnvelope, VoteResponse<RaftNodeId>>(
                self.vote_subject(),
                &envelope,
                self.rpc_timeout,
            )
            .await
            .map_err(|e| {
                RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
                    "NATS vote to {} failed: {e}",
                    self.target
                ))))
            })
    }
}

/// Make sure the cluster scope + target node + RPC kind all
/// roundtrip into the canonical engenho-teia subject.
///
/// Sanity tests only — RPC against a live NATS server is an
/// integration test that requires the listener side, deferred to
/// F2b. Confirms the typed wire shape is correct.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_id_to_teia_is_zero_padded() {
        assert_eq!(raft_id_to_teia(1).0, "0000000000000001");
        assert_eq!(raft_id_to_teia(42).0, "000000000000002a");
        assert_eq!(raft_id_to_teia(0xdead_beef).0, "00000000deadbeef");
    }

    #[test]
    fn append_subject_format() {
        let scope = ClusterScope::new("rio");
        let target = raft_id_to_teia(7);
        let s = scope.raft_append(RaftGroup::Store, target);
        assert_eq!(s, "engenho.rio.raft.store.append.0000000000000007");
    }

    #[test]
    fn vote_subject_format() {
        let scope = ClusterScope::new("rio");
        let target = raft_id_to_teia(7);
        let s = scope.raft_vote(RaftGroup::Store, target);
        assert_eq!(s, "engenho.rio.raft.store.vote.0000000000000007");
    }

    #[test]
    fn snapshot_subject_format() {
        let scope = ClusterScope::new("rio");
        let target = raft_id_to_teia(7);
        let s = scope.raft_snapshot(RaftGroup::Store, target);
        assert_eq!(s, "engenho.rio.raft.store.snapshot.0000000000000007");
    }

    #[test]
    fn envelope_serde_round_trip_vote() {
        use openraft::{CommittedLeaderId, LogId, Vote};
        let req = VoteRequest::<RaftNodeId>::new(
            Vote::new(3, 1),
            Some(LogId::new(CommittedLeaderId::new(3, 1), 100)),
        );
        let env = NatsRpcEnvelope::Vote(req.clone());
        let json = serde_json::to_string(&env).unwrap();
        let back: NatsRpcEnvelope = serde_json::from_str(&json).unwrap();
        match back {
            NatsRpcEnvelope::Vote(decoded) => assert_eq!(decoded, req),
            _ => panic!("wrong variant"),
        }
        // Tag is present.
        assert!(json.contains("\"kind\":\"Vote\""));
    }
}
