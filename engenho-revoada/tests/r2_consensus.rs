//! R2 integration tests — openraft consensus over engenho-revoada's
//! typed `RoleAssignment` command set.
//!
//! These tests exercise the REAL openraft library — no mocking of
//! the Raft state machine, log storage, or RPC layer. The
//! `InProcessRouter` delivers RPCs over tokio channels so the tests
//! run in a single tokio runtime without binding TCP sockets.

use std::collections::BTreeSet;
use std::time::Duration;

use engenho_revoada::consensus::{
    default_config, InProcessRouter, RaftMesh, Reason, RoleAssignment,
};
use engenho_revoada::membership::NodeRole;
use engenho_revoada::NodeId;

#[tokio::test]
async fn single_node_raft_initializes_and_commits_promote() {
    let router = InProcessRouter::new();
    let cfg = default_config("revoada-test-r2-single").unwrap();

    let mesh = RaftMesh::start(1, "in-process://node-1".into(), router.clone(), cfg)
        .await
        .expect("mesh start");

    mesh.initialize_singleton()
        .await
        .expect("singleton initialize");

    // Singleton becomes leader within ~1 election cycle (≤ 1.5s with
    // our config: election_timeout_min=500ms).
    assert!(mesh.wait_for_leadership(Duration::from_secs(3)).await);

    let target = NodeId::new([0xa1; 32]);
    let mut roles = BTreeSet::new();
    roles.insert(NodeRole::ApiServer);
    roles.insert(NodeRole::Etcd);
    let cmd = RoleAssignment::Promote {
        node_id: target,
        roles,
        reason: Reason::Operator,
    };

    let result = mesh.propose(cmd).await.expect("propose");
    assert!(result.applied_index >= 1);

    // Wait for the apply to land in the state machine.
    assert!(mesh.wait_for_applied(result.applied_index, Duration::from_secs(3)).await);

    let shape = mesh.current_shape().await;
    assert_eq!(shape.holders(NodeRole::ApiServer).len(), 1);
    assert_eq!(shape.holders(NodeRole::Etcd).len(), 1);
    assert!(shape.assignments.contains_key(&target));

    mesh.terminate().await.expect("terminate");
}

#[tokio::test]
async fn promote_then_demote_state_machine_idempotent() {
    let router = InProcessRouter::new();
    let cfg = default_config("revoada-test-r2-promote-demote").unwrap();
    let mesh = RaftMesh::start(7, "in-process://node-7".into(), router, cfg)
        .await
        .unwrap();
    mesh.initialize_singleton().await.unwrap();
    assert!(mesh.wait_for_leadership(Duration::from_secs(3)).await);

    let target = NodeId::new([0xee; 32]);
    let mut roles_promote = BTreeSet::new();
    roles_promote.insert(NodeRole::ApiServer);
    roles_promote.insert(NodeRole::Etcd);
    mesh.propose(RoleAssignment::Promote {
        node_id: target,
        roles: roles_promote,
        reason: Reason::Operator,
    })
    .await
    .unwrap();

    let mut roles_demote = BTreeSet::new();
    roles_demote.insert(NodeRole::Etcd);
    let demote_result = mesh
        .propose(RoleAssignment::Demote {
            node_id: target,
            roles_relinquished: roles_demote,
            reason: Reason::Rebalance,
        })
        .await
        .unwrap();

    assert!(mesh.wait_for_applied(demote_result.applied_index, Duration::from_secs(3)).await);
    let shape = mesh.current_shape().await;
    // ApiServer survives the demote; Etcd was relinquished.
    assert_eq!(shape.holders(NodeRole::ApiServer).len(), 1);
    assert_eq!(shape.holders(NodeRole::Etcd).len(), 0);

    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn quarantine_then_restore_cycle() {
    let router = InProcessRouter::new();
    let cfg = default_config("revoada-test-r2-quarantine").unwrap();
    let mesh = RaftMesh::start(3, "in-process://node-3".into(), router, cfg)
        .await
        .unwrap();
    mesh.initialize_singleton().await.unwrap();
    assert!(mesh.wait_for_leadership(Duration::from_secs(3)).await);

    let target = NodeId::new([0xbb; 32]);
    let mut roles = BTreeSet::new();
    roles.insert(NodeRole::Worker);
    mesh.propose(RoleAssignment::Promote {
        node_id: target,
        roles,
        reason: Reason::Operator,
    })
    .await
    .unwrap();
    mesh.propose(RoleAssignment::Quarantine {
        node_id: target,
        reason: Reason::HealthDegraded,
    })
    .await
    .unwrap();
    let restore_result = mesh
        .propose(RoleAssignment::Restore { node_id: target })
        .await
        .unwrap();
    assert!(mesh.wait_for_applied(restore_result.applied_index, Duration::from_secs(3)).await);

    let shape = mesh.current_shape().await;
    assert!(shape.assignments[&target].contains(&NodeRole::Worker));
    assert!(!shape.assignments[&target].contains(&NodeRole::Quarantined));

    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn three_node_raft_replicates_promote_to_followers() {
    let router = InProcessRouter::new();
    let cfg = default_config("revoada-test-r2-three-node").unwrap();

    let mesh_1 = RaftMesh::start(1, "in-process://node-1".into(), router.clone(), cfg.clone())
        .await
        .unwrap();
    let mesh_2 = RaftMesh::start(2, "in-process://node-2".into(), router.clone(), cfg.clone())
        .await
        .unwrap();
    let mesh_3 = RaftMesh::start(3, "in-process://node-3".into(), router.clone(), cfg.clone())
        .await
        .unwrap();

    // Bootstrap the cluster: all three start as initial voters.
    mesh_1
        .initialize_with_voters(vec![
            (1, "in-process://node-1".into()),
            (2, "in-process://node-2".into()),
            (3, "in-process://node-3".into()),
        ])
        .await
        .expect("3-node initialize");

    // One of the three becomes leader within a few election cycles.
    // We don't know which one; assert at least one of them wins.
    let leader_wait = async {
        for _ in 0..40 {
            if mesh_1.is_leader().await
                || mesh_2.is_leader().await
                || mesh_3.is_leader().await
            {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    };
    assert!(leader_wait.await, "no leader elected within 4s");

    // Find the leader + propose through it.
    let target = NodeId::new([0xfe; 32]);
    let mut roles = BTreeSet::new();
    roles.insert(NodeRole::ApiServer);

    let leader = if mesh_1.is_leader().await {
        &mesh_1
    } else if mesh_2.is_leader().await {
        &mesh_2
    } else {
        &mesh_3
    };

    let result = leader
        .propose(RoleAssignment::Promote {
            node_id: target,
            roles,
            reason: Reason::Operator,
        })
        .await
        .expect("propose to leader");

    // All three nodes must converge on the same applied state.
    assert!(mesh_1.wait_for_applied(result.applied_index, Duration::from_secs(5)).await);
    assert!(mesh_2.wait_for_applied(result.applied_index, Duration::from_secs(5)).await);
    assert!(mesh_3.wait_for_applied(result.applied_index, Duration::from_secs(5)).await);

    let shape_1 = mesh_1.current_shape().await;
    let shape_2 = mesh_2.current_shape().await;
    let shape_3 = mesh_3.current_shape().await;
    assert_eq!(shape_1, shape_2, "1 vs 2 diverge");
    assert_eq!(shape_2, shape_3, "2 vs 3 diverge");
    assert_eq!(shape_1.holders(NodeRole::ApiServer).len(), 1);

    mesh_1.terminate().await.unwrap();
    mesh_2.terminate().await.unwrap();
    mesh_3.terminate().await.unwrap();
}
