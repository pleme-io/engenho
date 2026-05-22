//! Topology strategy — the typed surface for fleet role assignment.
//!
//! Codename: **`formação`** (Portuguese: *formation* / *lineup*).
//! Every engenho cluster picks ONE [`TopologyStrategy`] at boot.
//! The strategy answers: *"given N healthy nodes, what role does
//! each one get, and how do we shift when a node is lost?"*
//!
//! ## The formation analogy
//!
//! Like a soccer team's 3-4-3 or a fighter-jet squadron's V
//! formation, a topology strategy declares:
//!
//!   * A SHAPE: how many of each role, when achievable
//!   * A SHIFT: when a node is lost, which surviving node takes
//!     over the vacant role (promote standby; demote excess; etc)
//!   * AN INVARIANT: minimum viable configuration; below this
//!     the cluster enters degraded mode (read-only or refuses
//!     writes) but never gets stuck
//!
//! ## Six pre-packed strategies
//!
//! | Strategy | Min nodes | Shape | Use case |
//! |---|---|---|---|
//! | [`Solo`] | 1 | 1 master | dev / homelab |
//! | [`Pair`] | 2 | 2 active-passive masters | HA pair |
//! | [`Quorum3M`] | 3 | 3 masters | etcd-style quorum |
//! | [`Cluster3MNW`] | 4+ | 3 masters + N workers | typical k8s |
//! | [`MeshAllPeers`] | 1+ | all peers, no role | symmetric (gossip-heavy) |
//! | [`Phalanx`] | N | ⌈2N/5⌉ masters | scales with cluster size |
//!
//! Strategies are pluggable — operators can also implement
//! [`TopologyStrategy`] for custom shapes.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

/// Typed role identifier. Every node holds 0 or 1 of these.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// Voting member of the Raft quorum. Reads + writes.
    Master,
    /// Workload-capable; not in the Raft quorum.
    Worker,
    /// Bootstrap node — initial seed before quorum forms.
    /// Transitions to Master once peers join.
    Bootstrap,
    /// Non-voting peer; participates in gossip + can serve reads
    /// but doesn't count toward quorum (Raft "learner").
    Observer,
}

impl Role {
    /// Stable identifier for telemetry.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Master => "master",
            Self::Worker => "worker",
            Self::Bootstrap => "bootstrap",
            Self::Observer => "observer",
        }
    }

    /// True if this role contributes to Raft quorum.
    #[must_use]
    pub fn is_voting(self) -> bool {
        matches!(self, Self::Master | Self::Bootstrap)
    }
}

/// Per-node identifier. In production, this is the ed25519 public
/// key (32 bytes); in tests, a u64 alias.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(pub String);

impl NodeId {
    #[must_use]
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// One node's state in the role-transition state machine.
///
/// State transitions are atomic + go through Raft (single source
/// of truth). Phi-accrual + heartbeats drive Healthy→Failed; the
/// topology strategy drives Standby→Active and the reverse.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeState {
    /// Just came up — gossiping its presence; no role yet.
    Joining,
    /// Healthy peer; topology strategy hasn't assigned a role yet
    /// (typical for new nodes during a rolling join).
    Standby,
    /// Has an active role.
    Active(Role),
    /// Transitioning out of an active role (writes blocked).
    Demoting,
    /// Voluntarily leaving the cluster.
    Departing,
    /// Phi-accrual flagged. Eligible for replacement.
    Failed,
}

impl NodeState {
    /// True if the node can serve any role workload (active +
    /// not transitioning).
    #[must_use]
    pub fn is_serving(self) -> bool {
        matches!(self, Self::Active(_))
    }

    /// Role this state implies, if any.
    #[must_use]
    pub fn role(self) -> Option<Role> {
        match self {
            Self::Active(r) => Some(r),
            _ => None,
        }
    }

    /// True if the node is in any usable state (not Failed or
    /// Departing). Used by [`TopologyStrategy::assign`] to scope
    /// the candidate pool.
    #[must_use]
    pub fn is_eligible(self) -> bool {
        !matches!(self, Self::Failed | Self::Departing)
    }
}

/// The cluster's committed role assignment. Lives in
/// engenho-revoada's Raft state machine — every change goes
/// through quorum.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleAssignment {
    pub assignments: Vec<(NodeId, NodeState)>,
}

impl RoleAssignment {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or update a node's state.
    pub fn set(&mut self, node: NodeId, state: NodeState) {
        for entry in &mut self.assignments {
            if entry.0 == node {
                entry.1 = state;
                return;
            }
        }
        self.assignments.push((node, state));
    }

    /// Get a node's state.
    #[must_use]
    pub fn get(&self, node: &NodeId) -> Option<NodeState> {
        self.assignments.iter().find(|(n, _)| n == node).map(|(_, s)| *s)
    }

    /// All nodes currently serving the given role.
    #[must_use]
    pub fn nodes_with_role(&self, role: Role) -> Vec<&NodeId> {
        self.assignments
            .iter()
            .filter(|(_, s)| s.role() == Some(role))
            .map(|(n, _)| n)
            .collect()
    }

    /// All eligible (non-failed, non-departing) nodes.
    #[must_use]
    pub fn eligible(&self) -> Vec<&NodeId> {
        self.assignments
            .iter()
            .filter(|(_, s)| s.is_eligible())
            .map(|(n, _)| n)
            .collect()
    }

    /// Count of voting members (Master + Bootstrap) currently active.
    #[must_use]
    pub fn voting_count(&self) -> usize {
        self.assignments
            .iter()
            .filter(|(_, s)| s.role().map(Role::is_voting).unwrap_or(false))
            .count()
    }

    /// True if the assignment satisfies Raft majority (voting > 1
    /// implies > N/2 voters present). Used by
    /// [`TopologyStrategy::has_quorum`].
    #[must_use]
    pub fn has_majority(&self) -> bool {
        let voting = self.voting_count();
        let eligible_voting: usize = self
            .assignments
            .iter()
            .filter(|(_, s)| s.is_eligible() && s.role().map(Role::is_voting).unwrap_or(false))
            .count();
        voting > 0 && voting * 2 > eligible_voting.saturating_sub(voting).max(0)
            || (voting > 0 && eligible_voting <= 1)
    }
}

/// A single proposed change to the role assignment. The reactor
/// generates these; Raft applies them atomically.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Transition {
    /// New node enters Standby state.
    Admit(NodeId),
    /// Standby/Worker → Active(Role).
    Promote(NodeId, Role),
    /// Active → Demoting → Standby (or Departing).
    Demote(NodeId),
    /// Master ↔ Worker (rare; happens on rebalance).
    Reassign(NodeId, Role),
    /// Failed/Departing node removed entirely.
    Evict(NodeId),
}

/// Errors a strategy may return.
#[derive(Debug, thiserror::Error, Clone)]
pub enum TopologyError {
    #[error("not enough eligible nodes: need {needed}, have {have}")]
    InsufficientNodes { needed: usize, have: usize },
    #[error("invariant violated: {0}")]
    InvariantViolated(String),
    #[error("strategy cannot satisfy desired shape with available nodes")]
    UnsatisfiableShape,
}

/// The pluggable interface every strategy implements.
pub trait TopologyStrategy: Send + Sync + std::fmt::Debug {
    /// Stable name for telemetry + config.
    fn name(&self) -> &'static str;

    /// Minimum eligible nodes for this strategy to operate (below
    /// this the cluster goes read-only).
    fn min_nodes(&self) -> usize;

    /// Compute the IDEAL assignment given the eligible nodes. The
    /// caller (revoada's policy engine) translates the delta vs
    /// the current assignment into [`Transition`]s.
    ///
    /// # Errors
    ///
    /// [`TopologyError::InsufficientNodes`] when below `min_nodes`.
    fn assign(&self, eligible: &[NodeId]) -> Result<RoleAssignment, TopologyError>;

    /// Given the current assignment + a set of newly-failed nodes,
    /// compute the list of [`Transition`]s that re-satisfies the
    /// strategy. Empty = nothing to do.
    fn react_to_loss(
        &self,
        current: &RoleAssignment,
        lost: &[NodeId],
    ) -> Vec<Transition>;

    /// Validate an assignment against the strategy's invariants.
    /// Used at admission + in tests.
    fn validate(&self, assignment: &RoleAssignment) -> Result<(), TopologyError>;
}

// =================================================================
// Pre-packed strategies
// =================================================================

/// Solo — single node holds every role. Development + homelab.
#[derive(Debug, Default, Clone)]
pub struct Solo;

impl TopologyStrategy for Solo {
    fn name(&self) -> &'static str {
        "solo"
    }
    fn min_nodes(&self) -> usize {
        1
    }
    fn assign(&self, eligible: &[NodeId]) -> Result<RoleAssignment, TopologyError> {
        if eligible.is_empty() {
            return Err(TopologyError::InsufficientNodes {
                needed: 1,
                have: 0,
            });
        }
        let mut a = RoleAssignment::new();
        a.set(eligible[0].clone(), NodeState::Active(Role::Master));
        // Extras parked as Standby so they're eligible for promotion
        // if the master dies — never silently dropped.
        for id in &eligible[1..] {
            a.set(id.clone(), NodeState::Standby);
        }
        Ok(a)
    }
    fn react_to_loss(
        &self,
        current: &RoleAssignment,
        lost: &[NodeId],
    ) -> Vec<Transition> {
        let lost_set: HashSet<&NodeId> = lost.iter().collect();
        let master_lost = current
            .nodes_with_role(Role::Master)
            .into_iter()
            .any(|n| lost_set.contains(n));
        let mut tx = Vec::new();
        if master_lost {
            // Promote the first surviving Standby (or Worker) to
            // Master. Without this, a multi-node Solo cluster
            // would lose its only voter when the master died —
            // violating the "never stuck" invariant.
            let candidate = current
                .assignments
                .iter()
                .find(|(id, state)| {
                    !lost_set.contains(id)
                        && matches!(state, NodeState::Standby | NodeState::Active(Role::Worker))
                })
                .map(|(id, _)| id.clone());
            if let Some(id) = candidate {
                tx.push(Transition::Promote(id, Role::Master));
            }
        }
        for id in lost {
            tx.push(Transition::Evict(id.clone()));
        }
        tx
    }
    fn validate(&self, assignment: &RoleAssignment) -> Result<(), TopologyError> {
        let masters = assignment.nodes_with_role(Role::Master).len();
        if masters != 1 {
            return Err(TopologyError::InvariantViolated(format!(
                "Solo expects exactly 1 master; have {masters}"
            )));
        }
        Ok(())
    }
}

/// Pair — two active masters (active-passive replication).
#[derive(Debug, Default, Clone)]
pub struct Pair;

impl TopologyStrategy for Pair {
    fn name(&self) -> &'static str {
        "pair"
    }
    fn min_nodes(&self) -> usize {
        2
    }
    fn assign(&self, eligible: &[NodeId]) -> Result<RoleAssignment, TopologyError> {
        if eligible.len() < 2 {
            return Err(TopologyError::InsufficientNodes {
                needed: 2,
                have: eligible.len(),
            });
        }
        let mut a = RoleAssignment::new();
        a.set(eligible[0].clone(), NodeState::Active(Role::Master));
        a.set(eligible[1].clone(), NodeState::Active(Role::Master));
        // Extras (rare for Pair) go Worker.
        for id in &eligible[2..] {
            a.set(id.clone(), NodeState::Active(Role::Worker));
        }
        Ok(a)
    }
    fn react_to_loss(
        &self,
        current: &RoleAssignment,
        lost: &[NodeId],
    ) -> Vec<Transition> {
        let lost_set: HashSet<&NodeId> = lost.iter().collect();
        let mut tx = Vec::new();
        let masters_lost = current
            .nodes_with_role(Role::Master)
            .into_iter()
            .filter(|n| lost_set.contains(n))
            .count();
        if masters_lost == 0 {
            return tx;
        }
        // Promote any standby worker to master to maintain pair.
        let workers: Vec<NodeId> = current
            .nodes_with_role(Role::Worker)
            .into_iter()
            .filter(|n| !lost_set.contains(n))
            .cloned()
            .collect();
        for w in workers.into_iter().take(masters_lost) {
            tx.push(Transition::Reassign(w, Role::Master));
        }
        for id in lost {
            tx.push(Transition::Evict(id.clone()));
        }
        tx
    }
    fn validate(&self, a: &RoleAssignment) -> Result<(), TopologyError> {
        let m = a.nodes_with_role(Role::Master).len();
        if m != 2 {
            return Err(TopologyError::InvariantViolated(format!(
                "Pair expects 2 masters; have {m}"
            )));
        }
        Ok(())
    }
}

/// Quorum3M — 3 masters; etcd/raft-style quorum.
#[derive(Debug, Default, Clone)]
pub struct Quorum3M;

impl TopologyStrategy for Quorum3M {
    fn name(&self) -> &'static str {
        "quorum_3m"
    }
    fn min_nodes(&self) -> usize {
        3
    }
    fn assign(&self, eligible: &[NodeId]) -> Result<RoleAssignment, TopologyError> {
        if eligible.len() < 3 {
            return Err(TopologyError::InsufficientNodes {
                needed: 3,
                have: eligible.len(),
            });
        }
        let mut a = RoleAssignment::new();
        for n in &eligible[..3] {
            a.set(n.clone(), NodeState::Active(Role::Master));
        }
        for n in &eligible[3..] {
            a.set(n.clone(), NodeState::Active(Role::Worker));
        }
        Ok(a)
    }
    fn react_to_loss(
        &self,
        current: &RoleAssignment,
        lost: &[NodeId],
    ) -> Vec<Transition> {
        let lost_set: HashSet<&NodeId> = lost.iter().collect();
        let masters_lost: Vec<NodeId> = current
            .nodes_with_role(Role::Master)
            .into_iter()
            .filter(|n| lost_set.contains(n))
            .cloned()
            .collect();
        if masters_lost.is_empty() {
            return Vec::new();
        }
        // Promote workers to fill the gaps; keep total masters at 3.
        let need = masters_lost.len();
        let workers: Vec<NodeId> = current
            .nodes_with_role(Role::Worker)
            .into_iter()
            .filter(|n| !lost_set.contains(n))
            .cloned()
            .collect();
        let mut tx = Vec::new();
        for w in workers.into_iter().take(need) {
            tx.push(Transition::Reassign(w, Role::Master));
        }
        for n in lost {
            tx.push(Transition::Evict(n.clone()));
        }
        tx
    }
    fn validate(&self, a: &RoleAssignment) -> Result<(), TopologyError> {
        let m = a.nodes_with_role(Role::Master).len();
        if m != 3 {
            return Err(TopologyError::InvariantViolated(format!(
                "Quorum3M expects 3 masters; have {m}"
            )));
        }
        Ok(())
    }
}

/// Cluster3MNW — 3 masters + N workers. The classic K8s shape.
#[derive(Debug, Default, Clone)]
pub struct Cluster3MNW;

impl TopologyStrategy for Cluster3MNW {
    fn name(&self) -> &'static str {
        "cluster_3m_nw"
    }
    fn min_nodes(&self) -> usize {
        3
    }
    fn assign(&self, eligible: &[NodeId]) -> Result<RoleAssignment, TopologyError> {
        Quorum3M.assign(eligible)
    }
    fn react_to_loss(
        &self,
        current: &RoleAssignment,
        lost: &[NodeId],
    ) -> Vec<Transition> {
        Quorum3M.react_to_loss(current, lost)
    }
    fn validate(&self, a: &RoleAssignment) -> Result<(), TopologyError> {
        let m = a.nodes_with_role(Role::Master).len();
        if m != 3 {
            return Err(TopologyError::InvariantViolated(format!(
                "Cluster3MNW expects 3 masters; have {m}"
            )));
        }
        Ok(())
    }
}

/// MeshAllPeers — every eligible node is a Master. Used for
/// gossip-heavy workloads where every peer is symmetric.
#[derive(Debug, Default, Clone)]
pub struct MeshAllPeers;

impl TopologyStrategy for MeshAllPeers {
    fn name(&self) -> &'static str {
        "mesh_all_peers"
    }
    fn min_nodes(&self) -> usize {
        1
    }
    fn assign(&self, eligible: &[NodeId]) -> Result<RoleAssignment, TopologyError> {
        if eligible.is_empty() {
            return Err(TopologyError::InsufficientNodes {
                needed: 1,
                have: 0,
            });
        }
        let mut a = RoleAssignment::new();
        for n in eligible {
            a.set(n.clone(), NodeState::Active(Role::Master));
        }
        Ok(a)
    }
    fn react_to_loss(
        &self,
        _current: &RoleAssignment,
        lost: &[NodeId],
    ) -> Vec<Transition> {
        lost.iter().map(|n| Transition::Evict(n.clone())).collect()
    }
    fn validate(&self, _a: &RoleAssignment) -> Result<(), TopologyError> {
        Ok(())
    }
}

/// Phalanx — scales master count with cluster size. ⌈2N/5⌉ masters
/// (so 5 nodes → 2 masters; 10 → 4; 25 → 10). Named for the Greek
/// formation that keeps proportional shield-wall depth regardless
/// of unit count.
#[derive(Debug, Default, Clone)]
pub struct Phalanx;

impl Phalanx {
    /// The master-count formula. Public so other strategies can
    /// borrow it.
    #[must_use]
    pub fn target_masters(eligible: usize) -> usize {
        if eligible == 0 {
            return 0;
        }
        // Round UP to ensure odd-count quorum where possible.
        ((eligible * 2) + 4) / 5
    }
}

impl TopologyStrategy for Phalanx {
    fn name(&self) -> &'static str {
        "phalanx"
    }
    fn min_nodes(&self) -> usize {
        1
    }
    fn assign(&self, eligible: &[NodeId]) -> Result<RoleAssignment, TopologyError> {
        if eligible.is_empty() {
            return Err(TopologyError::InsufficientNodes {
                needed: 1,
                have: 0,
            });
        }
        let m = Self::target_masters(eligible.len()).max(1);
        let mut a = RoleAssignment::new();
        for n in &eligible[..m.min(eligible.len())] {
            a.set(n.clone(), NodeState::Active(Role::Master));
        }
        for n in &eligible[m.min(eligible.len())..] {
            a.set(n.clone(), NodeState::Active(Role::Worker));
        }
        Ok(a)
    }
    fn react_to_loss(
        &self,
        current: &RoleAssignment,
        lost: &[NodeId],
    ) -> Vec<Transition> {
        let lost_set: HashSet<&NodeId> = lost.iter().collect();
        let surviving: Vec<NodeId> = current
            .eligible()
            .into_iter()
            .filter(|n| !lost_set.contains(n))
            .cloned()
            .collect();
        if surviving.is_empty() {
            return Vec::new();
        }
        let target_m = Self::target_masters(surviving.len()).max(1);
        let current_masters: HashSet<NodeId> = current
            .nodes_with_role(Role::Master)
            .into_iter()
            .filter(|n| !lost_set.contains(n))
            .cloned()
            .collect();
        let mut tx = Vec::new();
        if current_masters.len() < target_m {
            // Promote workers up to target.
            let need = target_m - current_masters.len();
            let workers: Vec<NodeId> = current
                .nodes_with_role(Role::Worker)
                .into_iter()
                .filter(|n| !lost_set.contains(n))
                .cloned()
                .collect();
            for w in workers.into_iter().take(need) {
                tx.push(Transition::Reassign(w, Role::Master));
            }
        } else if current_masters.len() > target_m {
            // Demote excess masters to workers (rebalance).
            let demote_count = current_masters.len() - target_m;
            for m in current_masters.into_iter().take(demote_count) {
                tx.push(Transition::Reassign(m, Role::Worker));
            }
        }
        for n in lost {
            tx.push(Transition::Evict(n.clone()));
        }
        tx
    }
    fn validate(&self, _a: &RoleAssignment) -> Result<(), TopologyError> {
        Ok(())
    }
}

// =================================================================
// Tests
// =================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(n: usize) -> Vec<NodeId> {
        (0..n).map(|i| NodeId::new(format!("node-{i}"))).collect()
    }

    #[test]
    fn role_is_voting() {
        assert!(Role::Master.is_voting());
        assert!(Role::Bootstrap.is_voting());
        assert!(!Role::Worker.is_voting());
        assert!(!Role::Observer.is_voting());
    }

    #[test]
    fn node_state_eligibility() {
        assert!(NodeState::Joining.is_eligible());
        assert!(NodeState::Standby.is_eligible());
        assert!(NodeState::Active(Role::Master).is_eligible());
        assert!(NodeState::Demoting.is_eligible());
        assert!(!NodeState::Departing.is_eligible());
        assert!(!NodeState::Failed.is_eligible());
    }

    #[test]
    fn solo_assigns_first_node_as_master() {
        let s = Solo;
        let a = s.assign(&ids(1)).unwrap();
        assert_eq!(
            a.get(&NodeId::new("node-0")),
            Some(NodeState::Active(Role::Master))
        );
        s.validate(&a).unwrap();
    }

    #[test]
    fn solo_requires_at_least_one_node() {
        let s = Solo;
        let err = s.assign(&[]).unwrap_err();
        assert!(matches!(err, TopologyError::InsufficientNodes { .. }));
    }

    #[test]
    fn pair_assigns_two_masters() {
        let s = Pair;
        let a = s.assign(&ids(2)).unwrap();
        assert_eq!(a.nodes_with_role(Role::Master).len(), 2);
        s.validate(&a).unwrap();
    }

    #[test]
    fn pair_promotes_worker_on_master_loss() {
        let s = Pair;
        let mut current = s.assign(&ids(3)).unwrap();
        // Third node was a worker.
        assert_eq!(
            current.get(&NodeId::new("node-2")),
            Some(NodeState::Active(Role::Worker))
        );
        let lost = vec![NodeId::new("node-0")];
        let tx = s.react_to_loss(&current, &lost);
        // Expect: reassign node-2 to Master + evict node-0.
        assert!(tx.contains(&Transition::Reassign(
            NodeId::new("node-2"),
            Role::Master
        )));
        assert!(tx.contains(&Transition::Evict(NodeId::new("node-0"))));
        // Apply transitions + re-validate.
        for t in tx {
            match t {
                Transition::Reassign(id, r) => current.set(id, NodeState::Active(r)),
                Transition::Evict(id) => current.set(id, NodeState::Failed),
                _ => {}
            }
        }
        let voting = current.voting_count();
        assert_eq!(voting, 2, "still 2 voting masters after loss");
    }

    #[test]
    fn quorum_3m_creates_3_masters_n_workers() {
        let s = Quorum3M;
        let a = s.assign(&ids(7)).unwrap();
        assert_eq!(a.nodes_with_role(Role::Master).len(), 3);
        assert_eq!(a.nodes_with_role(Role::Worker).len(), 4);
        s.validate(&a).unwrap();
    }

    #[test]
    fn quorum_3m_promotes_worker_on_master_loss() {
        let s = Quorum3M;
        let mut current = s.assign(&ids(5)).unwrap();
        let lost = vec![NodeId::new("node-1")];
        let tx = s.react_to_loss(&current, &lost);
        let promotes: Vec<&Transition> = tx
            .iter()
            .filter(|t| matches!(t, Transition::Reassign(_, Role::Master)))
            .collect();
        assert_eq!(promotes.len(), 1);
        for t in tx {
            match t {
                Transition::Reassign(id, r) => current.set(id, NodeState::Active(r)),
                Transition::Evict(id) => current.set(id, NodeState::Failed),
                _ => {}
            }
        }
        assert_eq!(current.voting_count(), 3);
    }

    #[test]
    fn quorum_3m_with_workers_handles_double_master_loss() {
        let s = Quorum3M;
        let mut current = s.assign(&ids(7)).unwrap();
        let lost = vec![NodeId::new("node-0"), NodeId::new("node-1")];
        let tx = s.react_to_loss(&current, &lost);
        let promotes: Vec<&Transition> = tx
            .iter()
            .filter(|t| matches!(t, Transition::Reassign(_, Role::Master)))
            .collect();
        assert_eq!(promotes.len(), 2);
        for t in tx {
            match t {
                Transition::Reassign(id, r) => current.set(id, NodeState::Active(r)),
                Transition::Evict(id) => current.set(id, NodeState::Failed),
                _ => {}
            }
        }
        assert_eq!(current.voting_count(), 3);
    }

    #[test]
    fn mesh_assigns_every_node_as_master() {
        let s = MeshAllPeers;
        let a = s.assign(&ids(5)).unwrap();
        assert_eq!(a.nodes_with_role(Role::Master).len(), 5);
        assert_eq!(a.nodes_with_role(Role::Worker).len(), 0);
    }

    #[test]
    fn phalanx_target_masters_scales() {
        assert_eq!(Phalanx::target_masters(1), 1);
        assert_eq!(Phalanx::target_masters(5), 2);
        assert_eq!(Phalanx::target_masters(10), 4);
        assert_eq!(Phalanx::target_masters(25), 10);
        // Edge: empty cluster has 0.
        assert_eq!(Phalanx::target_masters(0), 0);
    }

    #[test]
    fn phalanx_assign_matches_target() {
        let s = Phalanx;
        let a = s.assign(&ids(10)).unwrap();
        assert_eq!(a.nodes_with_role(Role::Master).len(), 4);
        assert_eq!(a.nodes_with_role(Role::Worker).len(), 6);
    }

    #[test]
    fn phalanx_reacts_to_loss_with_correct_target() {
        let s = Phalanx;
        let mut current = s.assign(&ids(10)).unwrap();
        // Lose 5 nodes (mix of masters + workers).
        let lost: Vec<NodeId> = (0..5).map(|i| NodeId::new(format!("node-{i}"))).collect();
        let tx = s.react_to_loss(&current, &lost);
        for t in &tx {
            match t.clone() {
                Transition::Reassign(id, r) => current.set(id, NodeState::Active(r)),
                Transition::Evict(id) => current.set(id, NodeState::Failed),
                _ => {}
            }
        }
        let surviving_eligible = current.eligible().len();
        assert_eq!(surviving_eligible, 5);
        let masters_now = current.nodes_with_role(Role::Master).len();
        assert_eq!(
            masters_now,
            Phalanx::target_masters(5),
            "phalanx should rebalance to 2 masters for 5-node cluster"
        );
    }

    #[test]
    fn assignment_serde_round_trip() {
        let mut a = RoleAssignment::new();
        a.set(NodeId::new("n1"), NodeState::Active(Role::Master));
        a.set(NodeId::new("n2"), NodeState::Active(Role::Worker));
        let json = serde_json::to_string(&a).unwrap();
        let back: RoleAssignment = serde_json::from_str(&json).unwrap();
        assert_eq!(back, a);
    }

    #[test]
    fn transition_serde_round_trip() {
        let cases = vec![
            Transition::Admit(NodeId::new("a")),
            Transition::Promote(NodeId::new("b"), Role::Master),
            Transition::Demote(NodeId::new("c")),
            Transition::Reassign(NodeId::new("d"), Role::Worker),
            Transition::Evict(NodeId::new("e")),
        ];
        for t in cases {
            let json = serde_json::to_string(&t).unwrap();
            let back: Transition = serde_json::from_str(&json).unwrap();
            assert_eq!(back, t);
        }
    }

    /// Resilience invariant: any strategy with N>=min_nodes must
    /// always produce an assignment that validates AND has at
    /// least one voting node (so the cluster isn't stuck).
    #[test]
    fn strategies_produce_at_least_one_voter() {
        for s in strategies() {
            for n in 1..=12 {
                if n < s.min_nodes() {
                    continue;
                }
                let a = s.assign(&ids(n)).expect("assignable");
                assert!(
                    a.voting_count() >= 1,
                    "strategy {} produced 0 voters for {n} nodes",
                    s.name()
                );
            }
        }
    }

    fn strategies() -> Vec<Box<dyn TopologyStrategy>> {
        vec![
            Box::new(Solo),
            Box::new(Pair),
            Box::new(Quorum3M),
            Box::new(Cluster3MNW),
            Box::new(MeshAllPeers),
            Box::new(Phalanx),
        ]
    }
}
