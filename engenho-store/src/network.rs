//! In-process [`RaftNetwork`] for engenho-store's Raft group.
//!
//! Mirrors engenho-revoada::consensus::network — same pattern,
//! separate router (so engenho-revoada's role-assignment Raft +
//! engenho-store's resource Raft don't accidentally share an RPC
//! channel even though they're in the same process).

use std::collections::HashMap;
use std::sync::Arc;

use openraft::BasicNode;
use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::type_config::{RaftNodeId, TypeConfig};

pub type RpcReply<T, E = RaftError<RaftNodeId>> = oneshot::Sender<Result<T, E>>;

pub enum RpcRequest {
    AppendEntries(
        AppendEntriesRequest<TypeConfig>,
        RpcReply<AppendEntriesResponse<RaftNodeId>>,
    ),
    Vote(VoteRequest<RaftNodeId>, RpcReply<VoteResponse<RaftNodeId>>),
    InstallSnapshot(
        InstallSnapshotRequest<TypeConfig>,
        RpcReply<InstallSnapshotResponse<RaftNodeId>, RaftError<RaftNodeId, InstallSnapshotError>>,
    ),
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum RpcEnvelope {
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<RaftNodeId>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
}

impl RpcEnvelope {
    pub async fn deliver(self, inbox: &mpsc::Sender<RpcRequest>) -> std::io::Result<Vec<u8>> {
        fn encoded<T: serde::Serialize>(
            received: Result<T, oneshot::error::RecvError>,
        ) -> std::io::Result<Vec<u8>> {
            let answer = received.map_err(std::io::Error::other)?;
            serde_json::to_vec(&answer).map_err(std::io::Error::other)
        }
        let closed = |_| std::io::Error::other("the node's RPC inbox is closed");
        match self {
            Self::AppendEntries(rpc) => {
                let (tx, rx) = oneshot::channel();
                inbox
                    .send(RpcRequest::AppendEntries(rpc, tx))
                    .await
                    .map_err(closed)?;
                encoded(rx.await)
            }
            Self::Vote(rpc) => {
                let (tx, rx) = oneshot::channel();
                inbox
                    .send(RpcRequest::Vote(rpc, tx))
                    .await
                    .map_err(closed)?;
                encoded(rx.await)
            }
            Self::InstallSnapshot(rpc) => {
                let (tx, rx) = oneshot::channel();
                inbox
                    .send(RpcRequest::InstallSnapshot(rpc, tx))
                    .await
                    .map_err(closed)?;
                encoded(rx.await)
            }
        }
    }
}

pub trait MeshNetwork: RaftNetworkFactory<TypeConfig> + Clone + Send + Sync + 'static {
    fn register(
        &self,
        node_id: RaftNodeId,
        sender: mpsc::Sender<RpcRequest>,
    ) -> impl std::future::Future<Output = ()> + Send;

    fn deregister(&self, node_id: RaftNodeId) -> impl std::future::Future<Output = ()> + Send;
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

impl MeshNetwork for InProcessRouter {
    async fn register(&self, node_id: RaftNodeId, sender: mpsc::Sender<RpcRequest>) {
        InProcessRouter::register(self, node_id, sender).await;
    }

    async fn deregister(&self, node_id: RaftNodeId) {
        InProcessRouter::deregister(self, node_id).await;
    }
}

pub(crate) fn remote_answer<T, E>(
    target: RaftNodeId,
    answer: Result<T, E>,
) -> Result<T, RPCError<RaftNodeId, BasicNode, E>>
where
    E: std::error::Error,
{
    answer.map_err(|e| RPCError::RemoteError(RemoteError::new(target, e)))
}

fn answered<T, E>(
    target: RaftNodeId,
    kind: &str,
    received: Result<Result<T, E>, oneshot::error::RecvError>,
) -> Result<T, RPCError<RaftNodeId, BasicNode, E>>
where
    E: std::error::Error,
{
    let answer = received.map_err(|e| {
        RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
            "{kind} oneshot dropped: {e}"
        ))))
    })?;
    remote_answer(target, answer)
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
        answered(self.target, "AppendEntries", rx.await)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<RaftNodeId>,
        RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId, InstallSnapshotError>>,
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
            .map_err(|e| {
                RPCError::Unreachable(Unreachable::new(&std::io::Error::other(format!(
                    "InstallSnapshot send to {}: {e}",
                    self.target
                ))))
            })?;
        answered(self.target, "InstallSnapshot", rx.await)
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
        answered(self.target, "Vote", rx.await)
    }
}

#[cfg(test)]
mod envelope_tests {
    use std::collections::{BTreeMap, BTreeSet};

    use openraft::{BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, Membership, Vote};

    use super::RpcEnvelope;
    use crate::type_config::TypeConfig;

    #[test]
    fn an_append_carrying_a_membership_entry_round_trips_through_json() {
        let voters = BTreeSet::from([1, 2, 3]);
        let nodes: BTreeMap<u64, BasicNode> = voters
            .iter()
            .map(|id| (*id, BasicNode::new(format!("127.0.0.1:{}", 7000 + id))))
            .collect();
        let entry = Entry::<TypeConfig> {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
            payload: EntryPayload::Membership(Membership::new(vec![voters], nodes)),
        };
        let envelope = RpcEnvelope::AppendEntries(openraft::raft::AppendEntriesRequest {
            vote: Vote::new_committed(1, 1),
            prev_log_id: None,
            entries: vec![entry.clone()],
            leader_commit: None,
        });
        let wire = serde_json::to_vec(&envelope).unwrap();
        let back: RpcEnvelope = serde_json::from_slice(&wire)
            .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&wire)));
        let RpcEnvelope::AppendEntries(decoded) = back else {
            panic!("decoded as another kind");
        };
        assert_eq!(decoded.entries, vec![entry]);
    }
}
