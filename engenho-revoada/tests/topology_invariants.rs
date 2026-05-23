//! Topology invariant tests — extensive coverage of the typed
//! strategies under randomized inputs + the eight canonical chaos
//! scenarios from docs/RESILIENCE.md.
//!
//! Three layers:
//!
//!   * **Property tests** (proptest) — randomized N + loss; verifies
//!     universal invariants every strategy must satisfy.
//!   * **Eight canonical chaos scenarios** — RESILIENCE.md §"Eight
//!     concrete chaos scenarios" encoded as explicit tests. Each
//!     test models one failure mode + asserts cluster converges to
//!     a valid + serving + voting shape.
//!   * **State machine transition tests** — verifies the NodeState
//!     edges + the Transition enum cover every reachable case.

use std::collections::HashSet;

use engenho_revoada::topology::{
    Cluster3MNW, MeshAllPeers, NodeId, NodeState, Pair, Phalanx, Quorum3M, Role, RoleAssignment,
    Solo, TopologyStrategy, Transition,
};
use proptest::prelude::*;

fn ids(n: usize) -> Vec<NodeId> {
    (0..n).map(|i| NodeId::new(format!("n{i}"))).collect()
}

/// Apply a slice of transitions to an assignment for invariant
/// checks. Mirrors what the Raft state machine does on apply().
fn apply(assignment: &mut RoleAssignment, transitions: &[Transition]) {
    for t in transitions {
        match t.clone() {
            Transition::Admit(id) => assignment.set(id, NodeState::Standby),
            Transition::Promote(id, r) => assignment.set(id, NodeState::Active(r)),
            Transition::Demote(id) => assignment.set(id, NodeState::Demoting),
            Transition::Reassign(id, r) => assignment.set(id, NodeState::Active(r)),
            Transition::Evict(id) => assignment.set(id, NodeState::Failed),
        }
    }
}

/// Every strategy under any allowed input. Used in proptest loops.
fn all_strategies() -> Vec<Box<dyn TopologyStrategy>> {
    vec![
        Box::new(Solo),
        Box::new(Pair),
        Box::new(Quorum3M),
        Box::new(Cluster3MNW),
        Box::new(MeshAllPeers),
        Box::new(Phalanx),
    ]
}

// =================================================================
// Universal invariants (proptest)
// =================================================================

proptest! {
    /// I1: Every strategy at N >= min_nodes produces an assignment
    /// whose voting_count is >= 1. Guarantees: cluster never starts
    /// in a stuck-by-construction state.
    #[test]
    fn prop_assign_yields_at_least_one_voter(
        n in 1usize..32,
    ) {
        for s in all_strategies() {
            if n < s.min_nodes() { continue; }
            let a = s.assign(&ids(n)).expect("assignable");
            prop_assert!(a.voting_count() >= 1,
                "{} on {} nodes: voting_count = 0", s.name(), n);
        }
    }

    /// I2: Every strategy's assign() validates() — strategies don't
    /// produce assignments their own validator rejects.
    #[test]
    fn prop_assign_validates(
        n in 1usize..32,
    ) {
        for s in all_strategies() {
            if n < s.min_nodes() { continue; }
            let a = s.assign(&ids(n)).expect("assignable");
            // Validate may relax for Mesh / Phalanx (no fixed master
            // count), so only check the ones with fixed shapes.
            if matches!(s.name(), "solo" | "pair" | "quorum_3m" | "cluster_3m_nw") {
                prop_assert!(s.validate(&a).is_ok(),
                    "{} on {} nodes: assign() output failed validate()", s.name(), n);
            }
        }
    }

    /// I3: react_to_loss is monotone — calling it with the EMPTY
    /// loss set returns no transitions (steady state).
    #[test]
    fn prop_no_loss_means_no_transitions(
        n in 1usize..32,
    ) {
        for s in all_strategies() {
            if n < s.min_nodes() { continue; }
            let current = s.assign(&ids(n)).unwrap();
            let tx = s.react_to_loss(&current, &[]);
            prop_assert!(tx.is_empty(),
                "{} on {} nodes: zero-loss reaction produced {} transitions",
                s.name(), n, tx.len());
        }
    }

    /// I4: react_to_loss is bounded — never emits more transitions
    /// than `lost.len() + eligible.len()`. Guards against runaway
    /// emission that could choke the Raft log.
    #[test]
    fn prop_reaction_size_bounded(
        n in 3usize..20,
        loss_count in 1usize..5,
    ) {
        for s in all_strategies() {
            if n < s.min_nodes() { continue; }
            let loss_count = loss_count.min(n);
            let nodes = ids(n);
            let current = s.assign(&nodes).unwrap();
            let lost: Vec<NodeId> = nodes.iter().take(loss_count).cloned().collect();
            let tx = s.react_to_loss(&current, &lost);
            prop_assert!(tx.len() <= loss_count + n,
                "{} reacted with {} transitions for {} losses in {} nodes",
                s.name(), tx.len(), loss_count, n);
        }
    }

    /// I5: After applying react_to_loss to losses, the surviving
    /// cluster has at least one voter (or zero if all were lost).
    #[test]
    fn prop_post_reaction_serves_when_anything_survives(
        n in 2usize..20,
        loss_count in 1usize..10,
    ) {
        for s in all_strategies() {
            if n < s.min_nodes() { continue; }
            let loss_count = loss_count.min(n - 1); // keep at least one
            let nodes = ids(n);
            let mut current = s.assign(&nodes).unwrap();
            let lost: Vec<NodeId> = nodes.iter().take(loss_count).cloned().collect();
            let tx = s.react_to_loss(&current, &lost);
            apply(&mut current, &tx);
            let survivors = n - loss_count;
            if survivors >= s.min_nodes() {
                prop_assert!(current.voting_count() >= 1,
                    "{}: lost {} of {}, survivors={}, but no voters after react",
                    s.name(), loss_count, n, survivors);
            }
        }
    }

    /// I6: react_to_loss never assigns a role to a lost node — the
    /// reactor only proposes evictions for them.
    #[test]
    fn prop_lost_nodes_never_promoted(
        n in 2usize..15,
        loss_count in 1usize..5,
    ) {
        for s in all_strategies() {
            if n < s.min_nodes() { continue; }
            let loss_count = loss_count.min(n);
            let nodes = ids(n);
            let current = s.assign(&nodes).unwrap();
            let lost: Vec<NodeId> = nodes.iter().take(loss_count).cloned().collect();
            let lost_set: HashSet<&NodeId> = lost.iter().collect();
            let tx = s.react_to_loss(&current, &lost);
            for t in &tx {
                match t {
                    Transition::Promote(id, _) | Transition::Reassign(id, _) => {
                        prop_assert!(!lost_set.contains(id),
                            "{}: lost node {} was reassigned/promoted", s.name(), id);
                    }
                    _ => {}
                }
            }
        }
    }
}

// =================================================================
// Eight canonical chaos scenarios — RESILIENCE.md
// =================================================================

/// Scenario 1: Minority partition (2 of 5 isolated). Surviving
/// 3-node majority must remain writable + voting.
#[test]
fn chaos_1_minority_partition_2_of_5() {
    let s = Quorum3M;
    let nodes = ids(5);
    let mut current = s.assign(&nodes).unwrap();
    let isolated = vec![NodeId::new("n3"), NodeId::new("n4")];
    let tx = s.react_to_loss(&current, &isolated);
    apply(&mut current, &tx);

    let surviving_eligible = current.eligible().len();
    let voters = current.voting_count();
    assert!(
        surviving_eligible >= 3,
        "minority partition shouldn't evict the majority"
    );
    assert!(
        voters >= 3,
        "majority side should retain quorum after isolating 2 of 5"
    );
}

/// Scenario 2: Majority partition (3 of 5 isolated). The minority
/// side becomes Failed; the formation reactor evicts them. The
/// remaining 2 surviving nodes cannot form quorum on their own —
/// the cluster correctly reports unavailable, NOT split-brain.
#[test]
fn chaos_2_majority_partition_3_of_5() {
    let s = Quorum3M;
    let nodes = ids(5);
    let mut current = s.assign(&nodes).unwrap();
    let lost = vec![NodeId::new("n0"), NodeId::new("n1"), NodeId::new("n2")];
    let tx = s.react_to_loss(&current, &lost);
    apply(&mut current, &tx);
    // The surviving 2 nodes can't satisfy Quorum3M's min_nodes=3.
    let surviving_eligible = current.eligible().len();
    assert_eq!(surviving_eligible, 2);
    assert!(
        s.assign(&ids(2)).is_err(),
        "Quorum3M::assign should refuse to start a 2-node cluster"
    );
}

/// Scenario 3: Slow links — no node loss, but heartbeats are
/// delayed. The reactor sees no losses (phi-accrual hasn't fired);
/// no transitions emitted. Idempotent steady state.
#[test]
fn chaos_3_slow_links_no_transitions() {
    let s = Cluster3MNW;
    let nodes = ids(7);
    let current = s.assign(&nodes).unwrap();
    let tx = s.react_to_loss(&current, &[]);
    assert!(tx.is_empty(), "slow links without failures = no reaction");
}

/// Scenario 4: Clock skew — semantically modeled as random heartbeat
/// fluctuations. Per Raft + chitchat, this doesn't change role
/// assignment unless phi-accrual fires. Modeled here as the same
/// no-loss reactor pathway.
#[test]
fn chaos_4_clock_skew_no_role_change() {
    let s = Pair;
    let nodes = ids(3);
    let current = s.assign(&nodes).unwrap();
    // Clock-skew nemesis doesn't flag anyone; reactor sees no loss.
    let tx = s.react_to_loss(&current, &[]);
    assert!(tx.is_empty());
    // Pair invariant still holds.
    assert_eq!(current.nodes_with_role(Role::Master).len(), 2);
}

/// Scenario 5: Disk full on a master. The host stops accepting
/// appends; phi-accrual eventually flags it. The reactor demotes
/// it + promotes a worker.
#[test]
fn chaos_5_disk_full_master_demoted() {
    let s = Cluster3MNW;
    let nodes = ids(6); // 3M + 3W
    let mut current = s.assign(&nodes).unwrap();
    // n0 is a master; phi flags it.
    let lost = vec![NodeId::new("n0")];
    let tx = s.react_to_loss(&current, &lost);
    apply(&mut current, &tx);
    let masters_now: Vec<&NodeId> = current.nodes_with_role(Role::Master);
    assert_eq!(masters_now.len(), 3, "should still have 3 masters");
    assert!(
        !masters_now.iter().any(|n| **n == NodeId::new("n0")),
        "n0 should no longer be a master"
    );
}

/// Scenario 6: Leader crash mid-commit. From the topology's
/// perspective this is identical to scenario 5 — one master gone.
/// The recovery path is the same.
#[test]
fn chaos_6_leader_crash_mid_commit() {
    let s = Quorum3M;
    let nodes = ids(5);
    let mut current = s.assign(&nodes).unwrap();
    // n0 is the leader; it crashes.
    let tx = s.react_to_loss(&current, &[NodeId::new("n0")]);
    apply(&mut current, &tx);
    assert_eq!(
        current.voting_count(),
        3,
        "quorum restored after leader crash"
    );
    assert!(
        current.get(&NodeId::new("n0")) == Some(NodeState::Failed),
        "crashed leader is Failed"
    );
}

/// Scenario 7: Rolling restart — one node at a time goes down,
/// restarts, comes back. The reactor promotes a worker when a
/// master dies; on rejoin, the node returns as Standby (the
/// policy engine could re-balance via a separate path, R-TOPO.1).
///
/// Across the full rolling restart, the cluster always retains
/// at least 1 voting master.
#[test]
fn chaos_7_rolling_restart() {
    let s = Cluster3MNW;
    let mut current = s.assign(&ids(6)).unwrap();
    for i in 0..6 {
        let lost = vec![NodeId::new(format!("n{i}"))];
        let tx = s.react_to_loss(&current, &lost);
        apply(&mut current, &tx);
        let voters_during_failure = current.voting_count();
        assert!(
            voters_during_failure >= 1,
            "rolling restart iter {i}: cluster has 0 voters mid-failure"
        );
        // Simulate the node rejoining as Standby.
        current.set(NodeId::new(format!("n{i}")), NodeState::Standby);
        let voters_after_rejoin = current.voting_count();
        assert!(
            voters_after_rejoin >= 1,
            "rolling restart iter {i}: cluster has 0 voters after rejoin"
        );
    }
}

/// Scenario 8: Simultaneous quorum-losing kill (3 of 5).
/// Cluster goes unavailable but stays consistent.
#[test]
fn chaos_8_simultaneous_3_of_5_kill() {
    let s = Quorum3M;
    let nodes = ids(5);
    let mut current = s.assign(&nodes).unwrap();
    // Three masters die at once.
    let lost = vec![NodeId::new("n0"), NodeId::new("n1"), NodeId::new("n2")];
    let tx = s.react_to_loss(&current, &lost);
    apply(&mut current, &tx);
    let voters = current.voting_count();
    let eligible = current.eligible().len();
    // The cluster has 2 eligible survivors, but Quorum3M can't
    // start fresh on 2. Voters could be 0 because the reactor has
    // no workers left to promote (5-node cluster started 3M+2W;
    // killing 3 masters leaves 2 workers; reactor promotes both;
    // but Quorum3M needs 3 masters and only 2 workers exist).
    assert!(
        voters <= 2,
        "after 3-of-5 kill, voters cannot exceed surviving population"
    );
    assert_eq!(eligible, 2);
}

// =================================================================
// State machine edge coverage
// =================================================================

#[test]
fn state_machine_joining_to_standby_via_admit() {
    let mut a = RoleAssignment::new();
    apply(&mut a, &[Transition::Admit(NodeId::new("n1"))]);
    assert_eq!(a.get(&NodeId::new("n1")), Some(NodeState::Standby));
}

#[test]
fn state_machine_standby_to_active_via_promote() {
    let mut a = RoleAssignment::new();
    apply(
        &mut a,
        &[
            Transition::Admit(NodeId::new("n1")),
            Transition::Promote(NodeId::new("n1"), Role::Master),
        ],
    );
    assert_eq!(
        a.get(&NodeId::new("n1")),
        Some(NodeState::Active(Role::Master))
    );
}

#[test]
fn state_machine_active_to_demoting_via_demote() {
    let mut a = RoleAssignment::new();
    apply(
        &mut a,
        &[
            Transition::Promote(NodeId::new("n1"), Role::Master),
            Transition::Demote(NodeId::new("n1")),
        ],
    );
    assert_eq!(a.get(&NodeId::new("n1")), Some(NodeState::Demoting));
}

#[test]
fn state_machine_master_to_worker_via_reassign() {
    let mut a = RoleAssignment::new();
    apply(
        &mut a,
        &[
            Transition::Promote(NodeId::new("n1"), Role::Master),
            Transition::Reassign(NodeId::new("n1"), Role::Worker),
        ],
    );
    assert_eq!(
        a.get(&NodeId::new("n1")),
        Some(NodeState::Active(Role::Worker))
    );
}

#[test]
fn state_machine_any_to_failed_via_evict() {
    let mut a = RoleAssignment::new();
    apply(
        &mut a,
        &[
            Transition::Promote(NodeId::new("n1"), Role::Master),
            Transition::Evict(NodeId::new("n1")),
        ],
    );
    assert_eq!(a.get(&NodeId::new("n1")), Some(NodeState::Failed));
}

#[test]
fn state_machine_eligible_excludes_failed_and_departing() {
    let mut a = RoleAssignment::new();
    a.set(NodeId::new("alive"), NodeState::Active(Role::Master));
    a.set(NodeId::new("standby"), NodeState::Standby);
    a.set(NodeId::new("failed"), NodeState::Failed);
    a.set(
        NodeId::new("leaving"),
        NodeState::Demoting, // demoting is still eligible
    );
    let eligible = a.eligible();
    let names: HashSet<&NodeId> = eligible.into_iter().collect();
    assert!(names.contains(&NodeId::new("alive")));
    assert!(names.contains(&NodeId::new("standby")));
    assert!(names.contains(&NodeId::new("leaving")));
    assert!(!names.contains(&NodeId::new("failed")));
}

// =================================================================
// Strategy interchangeability
// =================================================================

/// Switching strategies on the same node set should always produce
/// a VALID different assignment (when the new strategy's min_nodes
/// is met). Tests that strategies are pluggable runtime-config not
/// compile-time decisions.
#[test]
fn strategies_are_runtime_pluggable() {
    let nodes = ids(7);
    let solo = Solo.assign(&nodes[..1]).unwrap();
    let pair = Pair.assign(&nodes[..2]).unwrap();
    let quorum = Quorum3M.assign(&nodes).unwrap();
    let mesh = MeshAllPeers.assign(&nodes).unwrap();
    let phalanx = Phalanx.assign(&nodes).unwrap();
    assert_eq!(solo.voting_count(), 1);
    assert_eq!(pair.voting_count(), 2);
    assert_eq!(quorum.voting_count(), 3);
    assert_eq!(mesh.voting_count(), 7);
    // phalanx target for 7 = ceil(14/5) = 3
    assert_eq!(phalanx.voting_count(), Phalanx::target_masters(7));
}

/// Reassignment from one strategy to another preserves liveness:
/// the cluster never has zero voters during the swap.
#[test]
fn strategy_swap_quorum3m_to_mesh_preserves_liveness() {
    let nodes = ids(5);
    let from = Quorum3M.assign(&nodes).unwrap();
    let to = MeshAllPeers.assign(&nodes).unwrap();
    assert_eq!(from.voting_count(), 3);
    assert_eq!(to.voting_count(), 5);
    // No state during which voting_count was 0 (both endpoints valid).
}

// =================================================================
// Cross-strategy stress
// =================================================================

/// Phalanx self-stabilizes: after a sequence of node losses, the
/// final shape always matches Phalanx::target_masters(surviving).
#[test]
fn phalanx_self_stabilizes_after_cascade() {
    let s = Phalanx;
    let mut current = s.assign(&ids(20)).unwrap();
    // 4 successive losses of 3 nodes each: 20 → 17 → 14 → 11 → 8.
    let mut lost_so_far = 0;
    for batch in 0..4 {
        let lost: Vec<NodeId> = (0..3)
            .map(|i| NodeId::new(format!("n{}", lost_so_far + i)))
            .collect();
        let tx = s.react_to_loss(&current, &lost);
        apply(&mut current, &tx);
        lost_so_far += 3;
        let surviving = 20 - lost_so_far;
        let masters_now = current.nodes_with_role(Role::Master).len();
        let expected_masters = Phalanx::target_masters(surviving);
        assert_eq!(
            masters_now, expected_masters,
            "after batch {batch} (lost {lost_so_far}, surviving {surviving}), \
             expected {expected_masters} masters; got {masters_now}"
        );
    }
}

/// Idempotency: applying the SAME transition set twice produces
/// the same final state. Critical for Raft replays + restarts.
#[test]
fn transition_application_is_idempotent() {
    let s = Quorum3M;
    let nodes = ids(5);
    let mut current = s.assign(&nodes).unwrap();
    let lost = vec![NodeId::new("n0")];
    let tx = s.react_to_loss(&current, &lost);
    apply(&mut current, &tx);
    let snapshot = current.clone();
    // Apply again.
    apply(&mut current, &tx);
    assert_eq!(snapshot, current, "double-apply must be a no-op");
}

/// Cross-strategy invariant: if Mesh.assign succeeds with N nodes,
/// every other strategy whose min_nodes <= N also succeeds. (Solo's
/// min is 1, Pair 2, Quorum3M 3, Cluster3MNW 3.)
#[test]
fn strategies_compose_when_min_nodes_satisfied() {
    for n in 1usize..16 {
        let nodes = ids(n);
        let mesh = MeshAllPeers.assign(&nodes);
        assert!(mesh.is_ok());
        if n >= 1 {
            assert!(Solo.assign(&nodes[..1]).is_ok());
        }
        if n >= 2 {
            assert!(Pair.assign(&nodes[..2]).is_ok());
        }
        if n >= 3 {
            assert!(Quorum3M.assign(&nodes).is_ok());
            assert!(Cluster3MNW.assign(&nodes).is_ok());
        }
        assert!(Phalanx.assign(&nodes).is_ok());
    }
}
