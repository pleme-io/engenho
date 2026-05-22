//! R-TOPO.1 — TopologyReactor closing the loop between membership
//! and topology transitions. The reactor is the typed glue between
//! "phi-accrual flagged a node" and "Raft committed a transition";
//! these tests cover the full life-cycle of a formation under load.

use engenho_revoada::topology::{
    Cluster3MNW, MeshAllPeers, NodeId, NodeState, Pair, Phalanx, Quorum3M, Role,
    Solo, TopologyReactor, Transition,
};

fn ids(n: usize) -> Vec<NodeId> {
    (0..n).map(|i| NodeId::new(format!("n{i}"))).collect()
}

#[test]
fn reactor_admits_new_eligible_nodes() {
    let r = TopologyReactor::new(Box::new(Pair));
    let tx = r.observe_membership(&ids(3), &[]);
    let admits: Vec<&Transition> = tx
        .iter()
        .filter(|t| matches!(t, Transition::Admit(_)))
        .collect();
    assert_eq!(admits.len(), 3, "all three nodes should be admitted");
}

#[test]
fn reactor_does_not_double_admit_known_nodes() {
    let r = TopologyReactor::new(Box::new(Pair));
    let nodes = ids(2);
    let tx1 = r.observe_membership(&nodes, &[]);
    r.apply_transitions(&tx1);
    let tx2 = r.observe_membership(&nodes, &[]);
    let admits: Vec<&Transition> = tx2
        .iter()
        .filter(|t| matches!(t, Transition::Admit(_)))
        .collect();
    assert!(admits.is_empty(), "no new admits on re-observation");
}

#[test]
fn reactor_bootstrap_creates_initial_shape() {
    let r = TopologyReactor::new(Box::new(Quorum3M));
    let nodes = ids(5);
    let tx = r.observe_membership(&nodes, &[]);
    r.apply_transitions(&tx);
    let current = r.current();
    assert_eq!(current.nodes_with_role(Role::Master).len(), 3);
    assert_eq!(current.voting_count(), 3);
}

#[test]
fn reactor_bootstrap_skipped_if_below_min_nodes() {
    let r = TopologyReactor::new(Box::new(Quorum3M));
    let nodes = ids(2); // below min_nodes=3
    let tx = r.observe_membership(&nodes, &[]);
    r.apply_transitions(&tx);
    let current = r.current();
    assert_eq!(
        current.nodes_with_role(Role::Master).len(),
        0,
        "below min_nodes, no roles assigned"
    );
    assert_eq!(
        current
            .eligible()
            .into_iter()
            .filter(|n| current.get(n) == Some(NodeState::Standby))
            .count(),
        2,
        "both nodes admitted as standby"
    );
}

#[test]
fn reactor_reacts_to_phi_failure() {
    let r = TopologyReactor::new(Box::new(Quorum3M));
    let nodes = ids(5);
    // Bootstrap.
    let tx = r.observe_membership(&nodes, &[]);
    r.apply_transitions(&tx);
    assert_eq!(r.current().voting_count(), 3);

    // Simulate phi flagging the first master.
    let failed = vec![NodeId::new("n0")];
    let surviving: Vec<NodeId> = nodes.iter().skip(1).cloned().collect();
    let tx2 = r.observe_membership(&surviving, &failed);
    r.apply_transitions(&tx2);

    // After applying: still 3 voters, n0 marked Failed.
    let current = r.current();
    assert_eq!(current.voting_count(), 3, "quorum restored");
    assert_eq!(
        current.get(&NodeId::new("n0")),
        Some(NodeState::Failed),
        "n0 evicted"
    );
}

#[test]
fn reactor_handles_full_cluster_lifecycle() {
    // The plane-formation analogy in code: a cluster boots,
    // loses members, recovers them, loses more — and stays in
    // formation throughout.
    let r = TopologyReactor::new(Box::new(Phalanx));
    let initial = ids(10);

    // Boot.
    let tx = r.observe_membership(&initial, &[]);
    r.apply_transitions(&tx);
    let masters = r.current().nodes_with_role(Role::Master).len();
    assert_eq!(masters, Phalanx::target_masters(10));

    // Lose 3 nodes.
    let failed = vec![
        NodeId::new("n0"),
        NodeId::new("n1"),
        NodeId::new("n2"),
    ];
    let surviving: Vec<NodeId> = initial.iter().skip(3).cloned().collect();
    let tx = r.observe_membership(&surviving, &failed);
    r.apply_transitions(&tx);
    let masters_now = r.current().nodes_with_role(Role::Master).len();
    assert_eq!(masters_now, Phalanx::target_masters(7));

    // Lose 2 more.
    let failed2 = vec![NodeId::new("n3"), NodeId::new("n4")];
    let surviving2: Vec<NodeId> = surviving.iter().skip(2).cloned().collect();
    let tx = r.observe_membership(&surviving2, &failed2);
    r.apply_transitions(&tx);
    let masters_after = r.current().nodes_with_role(Role::Master).len();
    assert_eq!(masters_after, Phalanx::target_masters(5));
}

#[test]
fn reactor_apply_transition_individual_calls() {
    let r = TopologyReactor::new(Box::new(Solo));
    r.apply_transition(&Transition::Admit(NodeId::new("a")));
    r.apply_transition(&Transition::Promote(NodeId::new("a"), Role::Master));
    assert_eq!(
        r.current().get(&NodeId::new("a")),
        Some(NodeState::Active(Role::Master))
    );
    r.apply_transition(&Transition::Evict(NodeId::new("a")));
    assert_eq!(
        r.current().get(&NodeId::new("a")),
        Some(NodeState::Failed)
    );
}

#[test]
fn reactor_strategy_name_exposed() {
    let r = TopologyReactor::new(Box::new(Phalanx));
    assert_eq!(r.strategy_name(), "phalanx");
}

#[test]
fn reactor_with_initial_seeds_from_log_replay() {
    let mut seed = engenho_revoada::topology::RoleAssignment::new();
    seed.set(NodeId::new("n0"), NodeState::Active(Role::Master));
    seed.set(NodeId::new("n1"), NodeState::Active(Role::Master));
    let r = TopologyReactor::with_initial(Box::new(Pair), seed);
    assert_eq!(r.current().nodes_with_role(Role::Master).len(), 2);
}

#[test]
fn reactor_node_loss_with_no_active_masters_yet() {
    // Defensive: reactor has admitted nodes but never bootstrapped
    // (e.g. cluster below min_nodes). A failure on a Standby
    // shouldn't produce promotions — just an eviction.
    let r = TopologyReactor::new(Box::new(Quorum3M));
    let nodes = ids(2);
    let tx = r.observe_membership(&nodes, &[]);
    r.apply_transitions(&tx);
    // Now flag one as failed (it's a Standby).
    let failed = vec![NodeId::new("n0")];
    let surviving = vec![NodeId::new("n1")];
    let tx = r.observe_membership(&surviving, &failed);
    r.apply_transitions(&tx);
    // The strategy doesn't promote because there were no masters
    // to replace. Just evicts.
    assert_eq!(r.current().get(&NodeId::new("n0")), Some(NodeState::Failed));
}

#[test]
fn reactor_cross_strategy_switching() {
    // Operator switches strategy mid-flight (rare but supported).
    // The reactor is constructed anew with the prior assignment
    // as initial state. Test that the new strategy can compute
    // a meaningful transition set.
    let r1 = TopologyReactor::new(Box::new(Solo));
    let nodes = ids(5);
    let tx = r1.observe_membership(&nodes, &[]);
    r1.apply_transitions(&tx);
    let solo_state = r1.current();
    assert_eq!(solo_state.nodes_with_role(Role::Master).len(), 1);

    // Operator decides Solo isn't enough — switch to MeshAllPeers.
    let r2 = TopologyReactor::with_initial(Box::new(MeshAllPeers), solo_state);
    // The new reactor would now re-evaluate; we don't simulate
    // that here because the strategy switch logic lives in policy
    // (operator-driven). Just verify the reactor preserved state.
    let masters = r2.current().nodes_with_role(Role::Master).len();
    assert_eq!(
        masters, 1,
        "post-switch state is the pre-switch state until next observe_membership"
    );
}

#[test]
fn reactor_idempotency_double_observe_no_change() {
    let r = TopologyReactor::new(Box::new(Cluster3MNW));
    let nodes = ids(6);
    let tx = r.observe_membership(&nodes, &[]);
    r.apply_transitions(&tx);
    let snapshot = r.current();
    // Second observe with the same eligible set + no failures.
    let tx2 = r.observe_membership(&nodes, &[]);
    r.apply_transitions(&tx2);
    let snapshot2 = r.current();
    assert_eq!(snapshot, snapshot2, "idempotent re-observation");
}
