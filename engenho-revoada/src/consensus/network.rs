//! In-process [`RaftNetwork`] implementation for engenho-revoada.
//!
//! R2 ships an in-memory router that delivers RPCs between Raft
//! nodes running in the same process — sufficient to drive the
//! multi-node integration tests that prove the wiring is correct.
//! R3 (next milestone) replaces the in-process router with a
//! tonic-gRPC implementation for cross-process deployments.
//!
//! ## Architecture
//!
//! [`InProcessRouter`] holds a `HashMap<RaftNodeId, mpsc::Sender<...>>`
//! pointing to each running node's request handler. A
//! [`RaftNetworkFactory::new_client`] call returns a thin
//! [`InProcessNetwork`] handle that looks up the target node's
//! sender in the router and round-trips the RPC over a oneshot.
//!
//! The router is a `Send + Sync + 'static` `Arc<Mutex<...>>` so
//! every node can hold a clone + receive incoming RPCs from peers.

use std::collections::HashMap;
use std::sync::Arc;

use openraft::error::{NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::consensus::type_config::{RaftNodeId, TypeConfig};

/// One incoming RPC waiting to be handled by a Raft node.
pub enum RpcRequest {
    AppendEntries(
        AppendEntriesRequest<TypeConfig>,
        oneshot::Sender<AppendEntriesResponse<RaftNodeId>>,
    ),
    Vote(
        VoteRequest<RaftNodeId>,
        oneshot::Sender<VoteResponse<RaftNodeId>>,
    ),
    InstallSnapshot(
        InstallSnapshotRequest<TypeConfig>,
        oneshot::Sender<InstallSnapshotResponse<RaftNodeId>>,
    ),
}

#[derive(Clone, Default)]
pub struct InProcessRouter {
    nodes: Arc<Mutex<HashMap<RaftNodeId, mpsc::Sender<RpcRequest>>>>,
}

impl InProcessRouter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a node's inbound RPC sender. Called by `RaftMesh::start`
    /// to plug the local Raft into the shared router.
    pub async fn register(&self, node_id: RaftNodeId, sender: mpsc::Sender<RpcRequest>) {
        self.nodes.lock().await.insert(node_id, sender);
    }

    pub async fn deregister(&self, node_id: RaftNodeId) {
        self.nodes.lock().await.remove(&node_id);
    }

    async fn lookup(&self, node_id: RaftNodeId) -> Option<mpsc::Sender<RpcRequest>> {
        self.nodes.lock().await.get(&node_id).cloned()
    }
}

impl RaftNetworkFactory<TypeConfig> for InProcessRouter {
    type Network = InProcessNetwork;

    async fn new_client(&mut self, target: RaftNodeId, _node: &BasicNode) -> Self::Network {
        InProcessNetwork {
            target,
            router: self.clone(),
        }
    }
}

pub struct InProcessNetwork {
    target: RaftNodeId,
    router: InProcessRouter,
}

impl InProcessNetwork {
    fn map_send_err<E>(target: RaftNodeId) -> impl FnOnce(E) -> RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        move |e| {
            let _ = target;
            RPCError::Unreachable(Unreachable::new(&e))
        }
    }
}

impl RaftNetwork<TypeConfig> for InProcessNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<RaftNodeId>, RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>>
    {
        let sender = self.router.lookup(self.target).await.ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&std::io::Error::other(format!(
                "no in-process route to node {}",
                self.target
            ))))
        })?;
        let (tx, rx) = oneshot::channel();
        sender
            .send(RpcRequest::AppendEntries(rpc, tx))
            .await
            .map_err(Self::map_send_err(self.target))?;
        rx.await.map_err(|e| {
            RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
                "AppendEntries oneshot dropped: {e}"
            ))))
        })
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<RaftNodeId>,
        RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId, openraft::error::InstallSnapshotError>>,
    > {
        let sender = self.router.lookup(self.target).await.ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&std::io::Error::other(format!(
                "no in-process route to node {}",
                self.target
            ))))
        })?;
        let (tx, rx) = oneshot::channel();
        sender.send(RpcRequest::InstallSnapshot(rpc, tx)).await.map_err(|e| {
            RPCError::Unreachable(Unreachable::new(&std::io::Error::other(format!(
                "InstallSnapshot send to {}: {e}",
                self.target
            ))))
        })?;
        rx.await.map_err(|e| {
            RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
                "InstallSnapshot oneshot dropped: {e}"
            ))))
        })
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<RaftNodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<RaftNodeId>, RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>>
    {
        let sender = self.router.lookup(self.target).await.ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&std::io::Error::other(format!(
                "no in-process route to node {}",
                self.target
            ))))
        })?;
        let (tx, rx) = oneshot::channel();
        sender
            .send(RpcRequest::Vote(rpc, tx))
            .await
            .map_err(Self::map_send_err(self.target))?;
        rx.await.map_err(|e| {
            RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
                "Vote oneshot dropped: {e}"
            ))))
        })
    }
}
