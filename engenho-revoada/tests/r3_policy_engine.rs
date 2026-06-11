//! R3 integration test — end-to-end policy engine.
//!
//! Wires:
//!   * R1 GossipMesh (real chitchat gossip)
//!   * R2 RaftMesh (real openraft consensus)
//!   * R3 PolicyEngine + AutoReplacementPolicy
//!
//! and proves: when the policy sees a gap between target topology
//! and current mesh state, it emits proposals + the proposals
//! actually commit through Raft, updating the typed MeshShape.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use engenho_revoada::NodeId;
use engenho_revoada::attestation::NodeIdentity;
use engenho_revoada::consensus::{InProcessRouter, RaftMesh, default_config};
use engenho_revoada::membership::{GossipConfig, GossipMesh, NodeCapacity, NodeRole, NodeState};
use engenho_revoada::policy::{
    AutoReplacementPolicy, PolicyEngine, PolicyEngineConfig, TargetTopology,
};
use engenho_types::primitives::Quantity;

fn pick_port() -> u16 {
    use std::net::UdpSocket;
    let s = UdpSocket::bind("127.0.0.1:0").unwrap();
    let port = s.local_addr().unwrap().port();
    drop(s);
    port
}

fn ns(node_id: NodeId, port: u16, roles: &[NodeRole]) -> NodeState {
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

/// The closing test of distributed engenho — single-node case:
/// gossip + raft + policy all running; policy notices target says
/// we want 1 ApiServer + nobody currently holds it; it commits
/// the promotion through Raft; the typed MeshShape reflects.
#[tokio::test]
async fn policy_engine_promotes_to_meet_target_topology() {
    // === Setup: 1 RaftMesh singleton ===
    let router = InProcessRouter::new();
    let cfg = default_config("revoada-test-r3").unwrap();
    let mesh = Arc::new(
        RaftMesh::start(1, "in-process://node-1".into(), router, cfg)
            .await
            .expect("raft start"),
    );
    mesh.initialize_singleton().await.expect("initialize");
    assert!(mesh.wait_for_leadership(Duration::from_secs(3)).await);

    // === Setup: 1 GossipMesh advertising itself ===
    let port = pick_port();
    let key = Arc::new(NodeIdentity::from_seed([0xa1; 32]));
    let node_id = key.node_id();
    let gossip = Arc::new(
        GossipMesh::start(
            GossipConfig::new(
                key.clone(),
                format!("127.0.0.1:{port}").parse().unwrap(),
                ns(node_id, port, &[NodeRole::Worker]),
            )
            .with_cluster_id("revoada-test-r3"),
        )
        .await
        .expect("gossip start"),
    );
    gossip
        .wait_for_members(1, Duration::from_secs(3))
        .await
        .expect("gossip converges");

    // === Initial state: MeshShape is empty (nobody holds ApiServer) ===
    let shape_before = mesh.current_shape().await;
    assert!(shape_before.holders(NodeRole::ApiServer).is_empty());

    // === Setup: PolicyEngine with target topology demanding 1 ApiServer ===
    let target = TargetTopology {
        api_servers: 1,
        etcd_replicas: 0,
        schedulers: 0,
        controller_managers: 0,
        min_workers: 0,
    };
    let engine = PolicyEngine::new(
        gossip.clone(),
        mesh.clone(),
        PolicyEngineConfig {
            audit_interval: Duration::from_secs(60),
            target,
        },
    )
    .with_policy(AutoReplacementPolicy);

    // === Tick: policy should propose 1 Promote ===
    let report = engine.tick().await.expect("tick");
    assert_eq!(report.proposals_seen, 1, "expected 1 proposal");
    assert_eq!(report.applied.len(), 1, "expected 1 successful Raft commit");
    assert!(
        report.errors.is_empty(),
        "policy errors: {:?}",
        report.errors
    );

    // === Verify the typed MeshShape now reflects the proposed role ===
    let shape_after = mesh.current_shape().await;
    let api_holders = shape_after.holders(NodeRole::ApiServer);
    assert_eq!(api_holders.len(), 1, "expected 1 ApiServer holder");
    assert!(
        api_holders.contains(&node_id),
        "expected node {node_id:?} to hold ApiServer, got {api_holders:?}"
    );

    // === Second tick should be a no-op (at target) ===
    let report_2 = engine.tick().await.expect("second tick");
    assert_eq!(
        report_2.proposals_seen, 0,
        "expected no proposals at target: {:?}",
        report_2.applied
    );
    assert!(report_2.applied.is_empty());

    // === Cleanup ===
    drop(engine);
    let mesh = Arc::try_unwrap(mesh).ok().expect("mesh single-owner");
    let gossip = Arc::try_unwrap(gossip).ok().expect("gossip single-owner");
    mesh.terminate().await.expect("raft terminate");
    gossip.shutdown().await.expect("gossip shutdown");
}

/// Multi-policy composition: register two policies; one fills
/// ApiServer, the other (a hypothetical no-op) doesn't propose
/// anything. Engine evaluates both in order.
#[tokio::test]
async fn policy_engine_handles_multiple_policies_in_order() {
    let router = InProcessRouter::new();
    let cfg = default_config("revoada-test-r3-multi").unwrap();
    let mesh = Arc::new(
        RaftMesh::start(2, "in-process://node-2".into(), router, cfg)
            .await
            .unwrap(),
    );
    mesh.initialize_singleton().await.unwrap();
    assert!(mesh.wait_for_leadership(Duration::from_secs(3)).await);

    let port = pick_port();
    let key = Arc::new(NodeIdentity::from_seed([0xb2; 32]));
    let node_id = key.node_id();
    let gossip = Arc::new(
        GossipMesh::start(
            GossipConfig::new(
                key.clone(),
                format!("127.0.0.1:{port}").parse().unwrap(),
                ns(node_id, port, &[]),
            )
            .with_cluster_id("revoada-test-r3-multi"),
        )
        .await
        .unwrap(),
    );
    gossip
        .wait_for_members(1, Duration::from_secs(3))
        .await
        .unwrap();

    let target = TargetTopology {
        api_servers: 1,
        etcd_replicas: 1,
        schedulers: 0,
        controller_managers: 0,
        min_workers: 0,
    };
    let engine = PolicyEngine::new(
        gossip.clone(),
        mesh.clone(),
        PolicyEngineConfig {
            audit_interval: Duration::from_secs(60),
            target,
        },
    )
    .with_policy(AutoReplacementPolicy);

    // Tick: target says 1 ApiServer + 1 Etcd. With 1 node, both
    // promotions land on the same NodeId (auto-promotion finds the
    // healthiest candidate not already holding the role).
    let report = engine.tick().await.unwrap();
    // 2 proposals: Promote ApiServer + Promote Etcd, both on our 1 node.
    assert_eq!(report.proposals_seen, 2);
    assert_eq!(report.applied.len(), 2);

    let shape = mesh.current_shape().await;
    assert!(shape.holders(NodeRole::ApiServer).contains(&node_id));
    assert!(shape.holders(NodeRole::Etcd).contains(&node_id));

    drop(engine);
    let mesh = Arc::try_unwrap(mesh).ok().unwrap();
    let gossip = Arc::try_unwrap(gossip).ok().unwrap();
    mesh.terminate().await.unwrap();
    gossip.shutdown().await.unwrap();
}
