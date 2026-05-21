//! In-process [`RaftNetwork`] for engenho-store's Raft group.
//!
//! Mirrors engenho-revoada::consensus::network — same pattern,
//! separate router (so engenho-revoada's role-assignment Raft +
//! engenho-store's resource Raft don't accidentally share an RPC
//! channel even though they're in the same process).

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

use crate::type_config::{RaftNodeId, TypeConfig};

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

impl RaftNetwork<TypeConfig> for InProcessNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<RaftNodeId>,
        RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>,
    > {
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
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
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
        sender
            .send(RpcRequest::InstallSnapshot(rpc, tx))
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&std::io::Error::other(format!(
                "InstallSnapshot send to {}: {e}",
                self.target
            )))))?;
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
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        rx.await.map_err(|e| {
            RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
                "Vote oneshot dropped: {e}"
            ))))
        })
    }
}
