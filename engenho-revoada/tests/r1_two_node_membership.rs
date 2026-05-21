//! R1 integration test — two `GossipMesh` instances on a local
//! tokio runtime discover each other through chitchat's gossip
//! protocol and converge on a typed `MembershipView`.
//!
//! This is the first PROVENANCE that revoada's distribution
//! substrate actually works end-to-end. Anything below this layer
//! is sub-test fluff; anything above (Raft, content sync,
//! attestation) sits on this.

use std::str::FromStr;
use std::time::Duration;

use engenho_revoada::membership::{
    GossipConfig, GossipMesh, NodeCapacity, NodeRole, NodeState,
};
use engenho_revoada::NodeId;
use engenho_types::primitives::Quantity;
use tokio::time::timeout;

/// Allocate two ports on `127.0.0.1` by binding then dropping
/// UdpSockets — the OS won't immediately reissue them so the
/// follow-up bind by chitchat succeeds. Test-only convenience.
fn pick_two_ports() -> (u16, u16) {
    use std::net::UdpSocket;
    let s1 = UdpSocket::bind("127.0.0.1:0").unwrap();
    let s2 = UdpSocket::bind("127.0.0.1:0").unwrap();
    let p1 = s1.local_addr().unwrap().port();
    let p2 = s2.local_addr().unwrap().port();
    drop(s1);
    drop(s2);
    (p1, p2)
}

fn state(node_id: NodeId, port: u16, roles: &[NodeRole]) -> NodeState {
    NodeState {
        node_id,
        gossip_addr: format!("127.0.0.1:{port}"),
        raft_addr: None,
        roles: roles.iter().copied().collect(),
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

#[tokio::test]
async fn two_nodes_discover_each_other_through_gossip() {
    let (p1, p2) = pick_two_ports();
    let id_a = NodeId::new([0xa1; 32]);
    let id_b = NodeId::new([0xb2; 32]);

    let mesh_a = GossipMesh::start(
        GossipConfig::new(
            id_a,
            format!("127.0.0.1:{p1}").parse().unwrap(),
            state(id_a, p1, &[NodeRole::ApiServer, NodeRole::Worker]),
        )
        .with_cluster_id("revoada-test-r1")
        .with_seeds(vec![]), // bootstrap node
    )
    .await
    .expect("mesh A start");

    let mesh_b = GossipMesh::start(
        GossipConfig::new(
            id_b,
            format!("127.0.0.1:{p2}").parse().unwrap(),
            state(id_b, p2, &[NodeRole::Worker]),
        )
        .with_cluster_id("revoada-test-r1")
        .with_seeds(vec![format!("127.0.0.1:{p1}")]),
    )
    .await
    .expect("mesh B start");

    // Both nodes should see exactly 2 live members within ~5
    // gossip intervals (chitchat default-config gossip_interval is
    // 500ms in our wrapper; first delta typically converges within
    // 1-2 intervals on localhost).
    let view_a = mesh_a
        .wait_for_members(2, Duration::from_secs(5))
        .await
        .expect("mesh A converges");
    let view_b = mesh_b
        .wait_for_members(2, Duration::from_secs(5))
        .await
        .expect("mesh B converges");

    assert_eq!(view_a.members.len(), 2);
    assert_eq!(view_b.members.len(), 2);

    // Both views must show the same NodeIds (order-independent via
    // build_view's BTreeMap-sorted output).
    let ids_a: Vec<_> = view_a.members.iter().map(|m| m.node_id).collect();
    let ids_b: Vec<_> = view_b.members.iter().map(|m| m.node_id).collect();
    assert_eq!(ids_a, ids_b);

    // The peer view must report each other's roles correctly.
    let b_from_a = view_a
        .members
        .iter()
        .find(|m| m.node_id == id_b)
        .expect("A sees B");
    assert!(b_from_a.state.roles.contains(&NodeRole::Worker));

    let a_from_b = view_b
        .members
        .iter()
        .find(|m| m.node_id == id_a)
        .expect("B sees A");
    assert!(a_from_b.state.roles.contains(&NodeRole::ApiServer));

    // Local-state read-back proves the chitchat KV is wired both ways.
    let local_a = mesh_a.local_state().await.unwrap().expect("A has state");
    assert_eq!(local_a.node_id, id_a);

    // Graceful shutdown — both meshes drop without panic.
    mesh_a.shutdown().await.unwrap();
    mesh_b.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_state_update_propagates_to_peer() {
    let (p1, p2) = pick_two_ports();
    let id_a = NodeId::new([0xc3; 32]);
    let id_b = NodeId::new([0xd4; 32]);

    let mesh_a = GossipMesh::start(
        GossipConfig::new(
            id_a,
            format!("127.0.0.1:{p1}").parse().unwrap(),
            state(id_a, p1, &[NodeRole::Observer]),
        )
        .with_cluster_id("revoada-test-r1-update"),
    )
    .await
    .unwrap();

    let mesh_b = GossipMesh::start(
        GossipConfig::new(
            id_b,
            format!("127.0.0.1:{p2}").parse().unwrap(),
            state(id_b, p2, &[NodeRole::Worker]),
        )
        .with_cluster_id("revoada-test-r1-update")
        .with_seeds(vec![format!("127.0.0.1:{p1}")]),
    )
    .await
    .unwrap();

    // Wait for initial convergence.
    mesh_a
        .wait_for_members(2, Duration::from_secs(5))
        .await
        .expect("converge");

    // Update A's role to ApiServer.
    let mut new_a = state(id_a, p1, &[NodeRole::ApiServer, NodeRole::Etcd]);
    new_a.membership_generation = 1;
    mesh_a.update_local_state(&new_a).await.unwrap();

    // B should observe the updated roles within a few gossip intervals.
    let mut rx_b = mesh_b.subscribe();
    let observed = timeout(Duration::from_secs(5), async {
        loop {
            let view = rx_b.borrow().clone();
            if let Some(a_view) = view.members.iter().find(|m| m.node_id == id_a) {
                if a_view.state.roles.contains(&NodeRole::Etcd) {
                    return a_view.state.clone();
                }
            }
            if rx_b.changed().await.is_err() {
                panic!("watch closed");
            }
        }
    })
    .await
    .expect("B observes updated A");

    assert!(observed.roles.contains(&NodeRole::ApiServer));
    assert!(observed.roles.contains(&NodeRole::Etcd));
    assert!(!observed.roles.contains(&NodeRole::Observer));
    assert_eq!(observed.membership_generation, 1);

    mesh_a.shutdown().await.unwrap();
    mesh_b.shutdown().await.unwrap();
}

#[tokio::test]
async fn single_node_with_no_seeds_starts_cleanly() {
    let (p1, _) = pick_two_ports();
    let id = NodeId::new([0xee; 32]);
    let mesh = GossipMesh::start(
        GossipConfig::new(
            id,
            format!("127.0.0.1:{p1}").parse().unwrap(),
            state(id, p1, &[NodeRole::ApiServer, NodeRole::Worker]),
        )
        .with_cluster_id("revoada-test-r1-singleton"),
    )
    .await
    .expect("singleton start");

    let view = mesh
        .wait_for_members(1, Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(view.members.len(), 1);
    assert_eq!(view.members[0].node_id, id);

    mesh.shutdown().await.unwrap();
}
