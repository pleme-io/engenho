//! Typed `RoleAssignment` — the closed-enum command set that Layer B
//! serializes through the Raft log. Each variant is an atomic
//! mesh-shape mutation. Adding a variant requires updating the state
//! machine in `super::MeshShape::apply` (compile-exhaustive).

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

use crate::membership::NodeRole;
use crate::NodeId;

/// One mesh-shape mutation. All variants emit the same Raft-log
/// shape; the state machine in `MeshShape` is deterministic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RoleAssignment {
    /// Add the listed roles to a node's role set. Idempotent — if
    /// the node already holds them, no-op.
    Promote {
        node_id: NodeId,
        roles: BTreeSet<NodeRole>,
        reason: Reason,
    },
    /// Remove the listed roles from a node. If the resulting set is
    /// empty, the node is removed from `assignments` entirely.
    Demote {
        node_id: NodeId,
        roles_relinquished: BTreeSet<NodeRole>,
        reason: Reason,
    },
    /// Mark a node as quarantined — receives no new pods.
    Quarantine { node_id: NodeId, reason: Reason },
    /// Lift quarantine.
    Restore { node_id: NodeId },
}

/// Why this assignment was proposed. Telemetry + audit chain anchor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    /// Phi-accrual detector marked the prior holder dead; promoting
    /// a replacement.
    ReplacingFailed,
    /// Capacity-driven scale-up.
    ScalingUp,
    /// Operator drained a node for maintenance.
    ScalingDown,
    /// Health-check degraded enough to quarantine without removing.
    HealthDegraded,
    /// Explicit operator command (typically via the future
    /// `mesh_propose_shift` MCP tool, gated on saguão passport).
    Operator,
    /// Re-balance: too many control-plane components co-located.
    Rebalance,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_assignment_serializes_tagged() {
        let cmd = RoleAssignment::Promote {
            node_id: NodeId::new([1; 32]),
            roles: [NodeRole::ApiServer].into_iter().collect(),
            reason: Reason::ReplacingFailed,
        };
        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("\"kind\":\"promote\""));
        assert!(json.contains("\"reason\":\"replacing_failed\""));
        assert!(json.contains("\"api_server\""));
        let back: RoleAssignment = serde_json::from_str(&json).unwrap();
        assert_eq!(back, cmd);
    }

    #[test]
    fn every_variant_round_trips() {
        let nid = NodeId::new([7; 32]);
        let cases: Vec<RoleAssignment> = vec![
            RoleAssignment::Promote {
                node_id: nid,
                roles: [NodeRole::Worker].into_iter().collect(),
                reason: Reason::ScalingUp,
            },
            RoleAssignment::Demote {
                node_id: nid,
                roles_relinquished: [NodeRole::Etcd].into_iter().collect(),
                reason: Reason::Rebalance,
            },
            RoleAssignment::Quarantine {
                node_id: nid,
                reason: Reason::HealthDegraded,
            },
            RoleAssignment::Restore { node_id: nid },
        ];
        for cmd in cases {
            let json = serde_json::to_string(&cmd).unwrap();
            let back: RoleAssignment = serde_json::from_str(&json).unwrap();
            assert_eq!(back, cmd);
        }
    }
}
