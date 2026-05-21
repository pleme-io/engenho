//! Layer A — membership + failure detection.
//!
//! Wraps a chitchat gossip mesh. Every engenho node runs a chitchat
//! endpoint; the gossip protocol propagates per-node state and the
//! phi-accrual failure detector decides when a node is suspected
//! vs. confirmed dead.
//!
//! R0: typed surface only — concrete implementation lands at R1 by
//! wrapping `quickwit-oss/chitchat` (already used by tatara-engine).

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

use crate::NodeId;
use engenho_types::primitives::Quantity;

/// Per-node state gossiped across the mesh. Each node maintains its
/// own row; chitchat reconciles updates across the cluster.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeState {
    pub node_id: NodeId,
    pub gossip_addr: String,
    pub roles: BTreeSet<NodeRole>,
    pub capacity: NodeCapacity,
    pub k8s_version: String,
    pub uptime_sec: u64,
    /// Monotonic counter incremented on every confirmed role change.
    /// Lets stale gossip be detected during merge.
    pub membership_generation: u64,
}

/// Closed enum of node roles. A node can hold multiple
/// (e.g. a small cluster's first node is `ControlPlane + Etcd +
/// Scheduler + Worker`). The mesh's policy decides how to split.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    /// Runs kube-apiserver-equivalent (engenho-apiserver).
    ApiServer,
    /// Runs etcd-equivalent (engenho-store with the leader replica).
    Etcd,
    /// Runs kube-scheduler-equivalent (engenho-scheduler).
    Scheduler,
    /// Runs kube-controller-manager-equivalent (engenho-controllers).
    ControllerManager,
    /// Runs the kubelet — hosts Pods.
    Worker,
    /// Quarantined — receives no new pods until restored.
    Quarantined,
    /// Observer — gossip participant, no role responsibility.
    /// Used during bootstrap or graceful decommission.
    Observer,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCapacity {
    pub cpu: Quantity,
    pub memory: Quantity,
    pub storage: Quantity,
    /// Pod-count budget (typically `min(110, memory/256Mi)`).
    pub pods: u32,
}

/// Health classification derived from chitchat's phi-accrual detector.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeHealth {
    /// Recent heartbeat; phi value below suspect threshold.
    Healthy,
    /// Phi above suspect threshold but below dead threshold.
    Suspect,
    /// Phi above dead threshold OR grace period exceeded.
    Dead,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn node_role_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&NodeRole::ApiServer).unwrap(),
            "\"api_server\""
        );
        assert_eq!(
            serde_json::to_string(&NodeRole::ControllerManager).unwrap(),
            "\"controller_manager\""
        );
    }

    #[test]
    fn node_state_round_trips() {
        let state = NodeState {
            node_id: NodeId::new([1; 32]),
            gossip_addr: "192.168.64.10:7800".into(),
            roles: [NodeRole::ApiServer, NodeRole::Etcd, NodeRole::Worker]
                .into_iter()
                .collect(),
            capacity: NodeCapacity {
                cpu: Quantity::from_str("4").unwrap(),
                memory: Quantity::from_str("8Gi").unwrap(),
                storage: Quantity::from_str("50Gi").unwrap(),
                pods: 32,
            },
            k8s_version: "v1.34.0".into(),
            uptime_sec: 3600,
            membership_generation: 7,
        };
        let json = serde_json::to_string(&state).unwrap();
        let back: NodeState = serde_json::from_str(&json).unwrap();
        assert_eq!(back, state);
    }
}
