use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use engenho_store::network::{InProcessNetwork, RpcRequest};
use engenho_store::{
    InProcessRouter, MeshNetwork, RaftNodeId, Reason, ResourceCommand, ResourceKey, StoreMesh,
    TypeConfig, default_config,
};
use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use tokio::sync::mpsc;

#[derive(Clone, Default)]
struct CountingRouter {
    inner: InProcessRouter,
    registered: Arc<AtomicUsize>,
    appends: Arc<AtomicUsize>,
    votes: Arc<AtomicUsize>,
}

impl MeshNetwork for CountingRouter {
    async fn register(&self, node_id: RaftNodeId, sender: mpsc::Sender<RpcRequest>) {
        self.registered.fetch_add(1, Ordering::SeqCst);
        self.inner.register(node_id, sender).await;
    }

    async fn deregister(&self, node_id: RaftNodeId) {
        self.inner.deregister(node_id).await;
    }
}

impl RaftNetworkFactory<TypeConfig> for CountingRouter {
    type Network = CountingNetwork;

    async fn new_client(&mut self, target: RaftNodeId, node: &BasicNode) -> Self::Network {
        CountingNetwork {
            inner: self.inner.new_client(target, node).await,
            appends: self.appends.clone(),
            votes: self.votes.clone(),
        }
    }
}

struct CountingNetwork {
    inner: InProcessNetwork,
    appends: Arc<AtomicUsize>,
    votes: Arc<AtomicUsize>,
}

impl RaftNetwork<TypeConfig> for CountingNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<RaftNodeId>,
        RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>,
    > {
        self.appends.fetch_add(1, Ordering::SeqCst);
        self.inner.append_entries(rpc, option).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<RaftNodeId>,
        RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId, InstallSnapshotError>>,
    > {
        self.inner.install_snapshot(rpc, option).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<RaftNodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<RaftNodeId>, RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>>
    {
        self.votes.fetch_add(1, Ordering::SeqCst);
        self.inner.vote(rpc, option).await
    }
}

fn pod_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

#[tokio::test]
async fn a_three_node_store_replicates_over_a_network_that_is_not_the_in_process_router() {
    let router = CountingRouter::default();
    let cfg = default_config("engenho-store-p1-generic").unwrap();
    let mut meshes: Vec<StoreMesh<CountingRouter>> = Vec::new();
    for id in 1..=3u64 {
        meshes.push(
            StoreMesh::start(
                id,
                format!("in-process://{id}"),
                router.clone(),
                cfg.clone(),
            )
            .await
            .unwrap(),
        );
    }
    assert_eq!(router.registered.load(Ordering::SeqCst), 3);

    meshes[0]
        .initialize_with_voters(
            (1..=3u64)
                .map(|id| (id, format!("in-process://{id}")))
                .collect(),
        )
        .await
        .unwrap();

    let mut leader = None;
    for _ in 0..60 {
        for (i, mesh) in meshes.iter().enumerate() {
            if mesh.is_leader().await {
                leader = Some(i);
            }
        }
        if leader.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let leader = leader.expect("no leader over the counting network");

    let written = meshes[leader]
        .propose(ResourceCommand::Put {
            key: pod_key("counted"),
            value: serde_json::json!({"spec": {"image": "img-counted"}}),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();

    for mesh in &meshes {
        assert!(
            mesh.wait_for_applied(written.applied_index, Duration::from_secs(5))
                .await
        );
        assert!(
            mesh.get(&pod_key("counted")).await.is_some(),
            "node{} did not receive the write",
            mesh.node_id()
        );
    }
    assert!(router.votes.load(Ordering::SeqCst) > 0);
    assert!(router.appends.load(Ordering::SeqCst) > 0);

    for mesh in meshes {
        mesh.terminate().await.unwrap();
    }
}
