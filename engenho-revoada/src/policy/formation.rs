//! `FormationPolicy` — R-TOPO.1b glue between the typed topology
//! layer (R-TOPO.0 / R-TOPO.1) and the existing R3 policy engine.
//!
//! Wraps a [`TopologyReactor`] + implements the [`Policy`] trait.
//! Translates `topology::Transition` (Admit/Promote/Demote/Reassign/Evict)
//! into `consensus::RoleAssignment` (Demote/Promote with NodeRole sets)
//! so the formation-shift logic flows through the same commit path
//! every other policy uses.
//!
//! ## Translation rules
//!
//! | topology::Transition | consensus::RoleAssignment |
//! |---|---|
//! | Admit(n) | (none — gossip already discovered the node) |
//! | Promote(n, Master) | Promote(n, {ApiServer, Etcd, Scheduler, ControllerManager}) |
//! | Promote(n, Worker) | (none — workers are non-control-plane; no consensus op) |
//! | Promote(n, Observer) | (none — non-voting; pending future support) |
//! | Demote(n) | Demote(n, current_roles, Voluntary) |
//! | Reassign(n, Master) | Promote(n, {control-plane set}) |
//! | Reassign(n, Worker) | Demote(n, control-plane_roles, OperatorRebalance) |
//! | Evict(n) | Demote(n, current_roles, ReplacingFailed) |
//!
//! ## Why both policies in one engine
//!
//! `FormationPolicy` and `AutoReplacementPolicy` are NOT mutually
//! exclusive — operators run BOTH:
//!   * FormationPolicy decides cluster-shape (how many masters at all)
//!   * AutoReplacementPolicy fills the per-component role gaps
//! Together they form the two-tier shape declaration the user's
//! formation directive asks for.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;

use crate::consensus::{MeshShape, Reason, RoleAssignment};
use crate::membership::{MembershipView, NodeRole};
use crate::policy::{Policy, TargetTopology};
use crate::topology::{NodeId as TopologyNodeId, Role, TopologyReactor, Transition};
use crate::NodeId;

/// Policy that drives cluster shape via a [`TopologyReactor`].
///
/// Construct with a reactor + the engine's NodeId vocabulary
/// translator. Register on the [`crate::policy::PolicyEngine`] like
/// any other policy.
pub struct FormationPolicy {
    reactor: Arc<TopologyReactor>,
}

impl FormationPolicy {
    #[must_use]
    pub fn new(reactor: Arc<TopologyReactor>) -> Self {
        Self { reactor }
    }

    /// Translate a `consensus::NodeId` to the `topology::NodeId` string
    /// shape. Today this is hex-encoded ed25519; future R-TOPO.6 may
    /// switch to base58 — the conversion is centralized here.
    fn to_topology_id(id: &NodeId) -> TopologyNodeId {
        TopologyNodeId::new(id.to_hex())
    }

    /// Translate `topology::NodeId` (hex string) back to the typed
    /// `consensus::NodeId`. Returns None on parse failure (defensive).
    fn from_topology_id(tid: &TopologyNodeId) -> Option<NodeId> {
        NodeId::from_hex(&tid.0).ok()
    }

    /// The four control-plane roles a Master holds.
    fn master_role_set() -> BTreeSet<NodeRole> {
        let mut s = BTreeSet::new();
        s.insert(NodeRole::ApiServer);
        s.insert(NodeRole::Etcd);
        s.insert(NodeRole::Scheduler);
        s.insert(NodeRole::ControllerManager);
        s
    }

    /// Determine the role-set a node currently holds in consensus.
    fn current_roles(consensus: &MeshShape, node: &NodeId) -> BTreeSet<NodeRole> {
        consensus
            .assignments
            .get(node)
            .cloned()
            .unwrap_or_default()
    }
}

#[async_trait]
impl Policy for FormationPolicy {
    fn name(&self) -> &'static str {
        "formation"
    }

    async fn evaluate(
        &self,
        membership: &MembershipView,
        consensus: &MeshShape,
        _target: &TargetTopology,
    ) -> Vec<RoleAssignment> {
        // Build the typed membership snapshot.
        let alive: BTreeSet<NodeId> = membership.members.iter().map(|m| m.node_id).collect();
        let known_in_consensus: BTreeSet<NodeId> =
            consensus.assignments.keys().copied().collect();

        let eligible_topo: Vec<TopologyNodeId> =
            alive.iter().map(Self::to_topology_id).collect();
        let failed_topo: Vec<TopologyNodeId> = known_in_consensus
            .iter()
            .filter(|n| !alive.contains(n))
            .map(Self::to_topology_id)
            .collect();

        let transitions = self.reactor.observe_membership(&eligible_topo, &failed_topo);

        // Translate topology transitions → consensus role-assignments.
        let mut proposals = Vec::new();
        for t in transitions {
            match t {
                Transition::Admit(_) => {
                    // Admission is membership-layer concern; no consensus op.
                }
                Transition::Promote(tid, role) => {
                    let Some(node) = Self::from_topology_id(&tid) else { continue };
                    match role {
                        Role::Master | Role::Bootstrap => {
                            proposals.push(RoleAssignment::Promote {
                                node_id: node,
                                roles: Self::master_role_set(),
                                reason: Reason::ReplacingFailed,
                            });
                        }
                        Role::Worker | Role::Observer => {
                            // Worker/Observer have no quorum-affecting roles;
                            // no consensus op needed (workload scheduler
                            // handles workers separately).
                        }
                    }
                }
                Transition::Demote(tid) => {
                    let Some(node) = Self::from_topology_id(&tid) else { continue };
                    let current = Self::current_roles(consensus, &node);
                    if current.is_empty() {
                        continue;
                    }
                    proposals.push(RoleAssignment::Demote {
                        node_id: node,
                        roles_relinquished: current,
                        reason: Reason::Operator,
                    });
                }
                Transition::Reassign(tid, new_role) => {
                    let Some(node) = Self::from_topology_id(&tid) else { continue };
                    let current = Self::current_roles(consensus, &node);
                    match new_role {
                        Role::Master | Role::Bootstrap => {
                            // Was Worker, becoming Master: PROMOTE.
                            proposals.push(RoleAssignment::Promote {
                                node_id: node,
                                roles: Self::master_role_set(),
                                reason: Reason::Rebalance,
                            });
                        }
                        Role::Worker | Role::Observer => {
                            // Was Master, demoting: DEMOTE all control-plane roles.
                            if !current.is_empty() {
                                proposals.push(RoleAssignment::Demote {
                                    node_id: node,
                                    roles_relinquished: current,
                                    reason: Reason::Rebalance,
                                });
                            }
                        }
                    }
                }
                Transition::Evict(tid) => {
                    let Some(node) = Self::from_topology_id(&tid) else { continue };
                    let current = Self::current_roles(consensus, &node);
                    if current.is_empty() {
                        continue;
                    }
                    proposals.push(RoleAssignment::Demote {
                        node_id: node,
                        roles_relinquished: current,
                        reason: Reason::ReplacingFailed,
                    });
                }
            }
        }
        proposals
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_translation_round_trips() {
        let original = NodeId::from_hex("ab").unwrap();
        let tid = FormationPolicy::to_topology_id(&original);
        let back = FormationPolicy::from_topology_id(&tid).unwrap();
        assert_eq!(original, back);
    }

    #[test]
    fn master_role_set_contains_all_four_control_plane_roles() {
        let s = FormationPolicy::master_role_set();
        assert_eq!(s.len(), 4);
        assert!(s.contains(&NodeRole::ApiServer));
        assert!(s.contains(&NodeRole::Etcd));
        assert!(s.contains(&NodeRole::Scheduler));
        assert!(s.contains(&NodeRole::ControllerManager));
    }

    #[test]
    fn policy_name_is_stable() {
        let reactor = Arc::new(TopologyReactor::new(Box::new(crate::topology::Pair)));
        let p = FormationPolicy::new(reactor);
        assert_eq!(p.name(), "formation");
    }

    #[test]
    fn current_roles_returns_empty_for_unknown_node() {
        let consensus = MeshShape::default();
        let node = NodeId::from_hex("ff").unwrap();
        assert!(FormationPolicy::current_roles(&consensus, &node).is_empty());
    }
}
