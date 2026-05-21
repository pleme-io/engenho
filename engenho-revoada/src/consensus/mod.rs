//! Layer B — role-assignment consensus via Raft.
//!
//! The mesh's control-plane nodes form a Raft group. The leader
//! serializes typed [`RoleAssignment`] commands; followers apply
//! them in log order to a deterministic state machine that owns
//! the canonical "who-runs-what" view.
//!
//! Joint consensus (openraft's native dynamic-membership protocol)
//! makes shifts atomic — a single Raft commit transitions the mesh
//! from "old shape" to "new shape" without an intermediate state.
//!
//! R0: typed surface + state machine model only. R2 wires
//! `databendlabs/openraft` (already a tatara-engine dep) and runs
//! a real Raft group over a chitchat-discovered peer set.

pub mod role_assignment;

pub use role_assignment::{Reason, RoleAssignment};

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

use crate::membership::NodeRole;
use crate::NodeId;

/// Deterministic state machine over the Raft log. Every committed
/// [`RoleAssignment`] mutates this; followers and leader converge to
/// the same `MeshShape` by replay.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshShape {
    pub assignments: BTreeMap<NodeId, BTreeSet<NodeRole>>,
    /// Raft term + index of the most recent commit applied.
    pub last_applied_term: u64,
    pub last_applied_index: u64,
}

impl MeshShape {
    /// Apply a single committed assignment. Pure function.
    pub fn apply(&mut self, cmd: &RoleAssignment, term: u64, index: u64) {
        match cmd {
            RoleAssignment::Promote { node_id, roles, .. } => {
                let entry = self.assignments.entry(*node_id).or_default();
                for &r in roles {
                    entry.insert(r);
                }
            }
            RoleAssignment::Demote {
                node_id,
                roles_relinquished,
                ..
            } => {
                if let Some(entry) = self.assignments.get_mut(node_id) {
                    for r in roles_relinquished {
                        entry.remove(r);
                    }
                    if entry.is_empty() {
                        self.assignments.remove(node_id);
                    }
                }
            }
            RoleAssignment::Quarantine { node_id, .. } => {
                let entry = self.assignments.entry(*node_id).or_default();
                entry.insert(NodeRole::Quarantined);
            }
            RoleAssignment::Restore { node_id } => {
                if let Some(entry) = self.assignments.get_mut(node_id) {
                    entry.remove(&NodeRole::Quarantined);
                }
            }
        }
        self.last_applied_term = term;
        self.last_applied_index = index;
    }

    /// Read-only query: which nodes currently hold the given role?
    #[must_use]
    pub fn holders(&self, role: NodeRole) -> BTreeSet<NodeId> {
        self.assignments
            .iter()
            .filter_map(|(id, roles)| if roles.contains(&role) { Some(*id) } else { None })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nid(b: u8) -> NodeId {
        NodeId::new([b; 32])
    }

    #[test]
    fn promote_then_demote_is_idempotent() {
        let mut shape = MeshShape::default();
        let promote = RoleAssignment::Promote {
            node_id: nid(1),
            roles: [NodeRole::ApiServer, NodeRole::Etcd].into_iter().collect(),
            reason: Reason::Operator,
        };
        shape.apply(&promote, 1, 1);
        assert_eq!(shape.holders(NodeRole::ApiServer).len(), 1);
        assert_eq!(shape.holders(NodeRole::Etcd).len(), 1);

        let demote = RoleAssignment::Demote {
            node_id: nid(1),
            roles_relinquished: [NodeRole::ApiServer].into_iter().collect(),
            reason: Reason::ScalingDown,
        };
        shape.apply(&demote, 1, 2);
        assert_eq!(shape.holders(NodeRole::ApiServer).len(), 0);
        // Etcd role still held.
        assert_eq!(shape.holders(NodeRole::Etcd).len(), 1);
    }

    #[test]
    fn quarantine_then_restore() {
        let mut shape = MeshShape::default();
        shape.apply(
            &RoleAssignment::Promote {
                node_id: nid(2),
                roles: [NodeRole::Worker].into_iter().collect(),
                reason: Reason::Operator,
            },
            1,
            1,
        );
        shape.apply(
            &RoleAssignment::Quarantine {
                node_id: nid(2),
                reason: Reason::HealthDegraded,
            },
            1,
            2,
        );
        assert!(shape.assignments[&nid(2)].contains(&NodeRole::Quarantined));
        shape.apply(&RoleAssignment::Restore { node_id: nid(2) }, 1, 3);
        assert!(!shape.assignments[&nid(2)].contains(&NodeRole::Quarantined));
        // Worker role survived.
        assert!(shape.assignments[&nid(2)].contains(&NodeRole::Worker));
    }

    #[test]
    fn last_applied_advances_monotonically() {
        let mut shape = MeshShape::default();
        shape.apply(
            &RoleAssignment::Promote {
                node_id: nid(1),
                roles: BTreeSet::new(),
                reason: Reason::Operator,
            },
            5,
            42,
        );
        assert_eq!(shape.last_applied_term, 5);
        assert_eq!(shape.last_applied_index, 42);
    }
}
