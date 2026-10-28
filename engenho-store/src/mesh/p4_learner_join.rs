use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use engenho_control_types::pin::KeyMaterial;
use serde_json::json;

use crate::command::{Reason, ResourceCommand};
use crate::mesh::StoreMesh;
use crate::mesh_join::{JoinGate, JoinKey};
use crate::peer_table::PeerTable;
use crate::quic_network::{MeshAddr, QuicRouter};
use crate::resource::ResourceKey;
use crate::testing::fast_config;
use crate::type_config::RaftNodeId;

const CLUSTER: &str = "p4-join";
const BOUND: Duration = Duration::from_secs(10);

struct Member {
    mesh: StoreMesh,
    router: QuicRouter,
    addr: SocketAddr,
    dir: tempfile::TempDir,
}

fn gate(ca_key: &[u8]) -> JoinGate {
    JoinGate {
        cluster: CLUSTER.into(),
        key: JoinKey::derive(ca_key),
    }
}

impl Member {
    async fn start(gate: JoinGate) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let key = KeyMaterial::generate().unwrap();
        let id = key.spki().mesh_node_id();
        let table = Arc::new(PeerTable::open(dir.path().join(PeerTable::FILE)).unwrap());
        let router =
            QuicRouter::bind_joinable(([127, 0, 0, 1], 0).into(), key, table, gate).unwrap();
        let addr = router.local_addr().unwrap();
        let mesh = StoreMesh::start(
            id,
            MeshAddr::pinned(addr, router.spki()).to_string(),
            router.clone(),
            fast_config(CLUSTER).unwrap(),
        )
        .await
        .unwrap();
        router.follow(mesh.metrics()).await;
        Self {
            mesh,
            router,
            addr,
            dir,
        }
    }

    async fn form(gate: JoinGate) -> Self {
        let member = Self::start(gate).await;
        member.mesh.initialize_singleton().await.unwrap();
        assert!(member.mesh.wait_for_leadership(BOUND).await);
        member
    }

    async fn join(&self, seeds: &[&Member]) {
        let seeds: Vec<SocketAddr> = seeds.iter().map(|m| m.addr).collect();
        self.router.join(&seeds, self.addr, BOUND).await.unwrap();
    }

    fn id(&self) -> RaftNodeId {
        self.mesh.node_id()
    }

    async fn role_of(&self, node: RaftNodeId) -> Option<bool> {
        self.mesh
            .members()
            .await
            .into_iter()
            .find(|(id, _, _)| *id == node)
            .map(|(_, _, voter)| voter)
    }
}

fn config_map(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "ConfigMap", "default", name)
}

async fn write(on: &Member, name: &str) -> crate::revision::Revision {
    on.mesh
        .propose(ResourceCommand::put(
            config_map(name),
            json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": name, "namespace": "default"}}),
            Reason::Operator,
        ))
        .await
        .unwrap();
    on.mesh.current_revision().await
}

async fn reads(on: &Member, name: &str, at: crate::revision::Revision) -> bool {
    on.mesh.wait_for_revision(at, BOUND).await && on.mesh.get(&config_map(name)).await.is_some()
}

async fn leader_among(members: &[&Member]) -> Option<usize> {
    let deadline = tokio::time::Instant::now() + BOUND;
    while tokio::time::Instant::now() < deadline {
        for (i, m) in members.iter().enumerate() {
            if m.mesh.is_leader().await {
                return Some(i);
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_joins_replicates_as_a_learner_and_votes_only_once_promoted() {
    let ca_key = b"the shared cluster CA key";
    let a = Member::form(gate(ca_key)).await;
    let b = Member::start(gate(ca_key)).await;
    b.join(&[&a]).await;

    assert_eq!(
        a.role_of(b.id()).await,
        Some(false),
        "b joined as a learner"
    );
    let at = write(&a, "before-promotion").await;
    assert!(reads(&b, "before-promotion", at).await);
    let at = write(&b, "from-the-learner").await;
    assert!(
        reads(&a, "from-the-learner", at).await,
        "a learner's write was not forwarded to the leader"
    );
    assert!(!b.mesh.is_leader().await);
    assert!(b.mesh.is_member(b.id()).await);
    let recorded = PeerTable::open(b.dir.path().join(PeerTable::FILE)).unwrap();
    assert_eq!(recorded.get(a.id()).map(|p| p.spki), Some(a.router.spki()));

    let deadline = tokio::time::Instant::now() + BOUND;
    let leaders_table = a.dir.path().join(PeerTable::FILE);
    while PeerTable::open(&leaders_table)
        .unwrap()
        .get(b.id())
        .is_none()
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the leader never learned the learner from the membership"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    a.mesh.promote_voter(b.id()).await.unwrap();
    assert_eq!(a.role_of(b.id()).await, Some(true), "b votes once promoted");
    let at = write(&a, "after-promotion").await;
    assert!(reads(&b, "after-promotion", at).await);

    b.mesh.terminate().await.unwrap();
    a.mesh.terminate().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_join_through_a_follower_reaches_the_leader_and_the_cluster_survives_losing_it() {
    let ca_key = b"the shared cluster CA key";
    let a = Member::form(gate(ca_key)).await;
    let b = Member::start(gate(ca_key)).await;
    b.join(&[&a]).await;
    a.mesh.promote_voter(b.id()).await.unwrap();
    let c = Member::start(gate(ca_key)).await;
    c.join(&[&b]).await;
    assert_eq!(a.role_of(c.id()).await, Some(false), "c joined through b");
    a.mesh.promote_voter(c.id()).await.unwrap();
    let at = write(&a, "three-voters").await;
    assert!(reads(&c, "three-voters", at).await);

    a.mesh.terminate().await.unwrap();
    let survivors = [&b, &c];
    let leader = leader_among(&survivors)
        .await
        .expect("b and c elect a leader");
    let other = survivors[1 - leader];
    let at = write(survivors[leader], "after-the-leader-died").await;
    assert!(reads(other, "after-the-leader-died", at).await);

    b.mesh.terminate().await.unwrap();
    c.mesh.terminate().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_without_the_cluster_key_or_name_is_refused_and_never_becomes_a_member() {
    let a = Member::form(gate(b"the shared cluster CA key")).await;
    let stranger = Member::start(gate(b"some other CA key")).await;
    let refused = stranger
        .router
        .join(&[a.addr], stranger.addr, Duration::from_secs(1))
        .await;
    assert!(refused.is_err(), "{refused:?}");
    let mut renamed = gate(b"the shared cluster CA key");
    renamed.cluster = "another-cluster".into();
    let misnamed = Member::start(renamed).await;
    let refused = misnamed
        .router
        .join(&[a.addr], misnamed.addr, Duration::from_secs(1))
        .await
        .unwrap_err();
    assert!(refused.to_string().contains("refused"), "{refused}");
    for outsider in [stranger.id(), misnamed.id()] {
        assert_eq!(a.role_of(outsider).await, None);
    }
    stranger.mesh.terminate().await.unwrap();
    misnamed.mesh.terminate().await.unwrap();
    a.mesh.terminate().await.unwrap();
}
