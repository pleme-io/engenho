//! `TypeConfig` — wires openraft's generic surface to revoada's
//! typed domain.
//!
//! - `D` = [`RoleAssignment`] (the command we replicate)
//! - `R` = `ApplyResult` (the state-machine response)
//! - `NodeId` = `u64` (small + serde-friendly; maps onto revoada's
//!   32-byte [`crate::NodeId`] via the mesh-shape resolver layer)
//! - `Node` = [`openraft::BasicNode`] (carries the Raft listen addr)
//! - `SnapshotData` = `std::io::Cursor<Vec<u8>>` (the openraft idiom)

use openraft::BasicNode;

use crate::consensus::RoleAssignment;

/// Openraft NodeId — `u64`. Distinct from [`crate::NodeId`] (the
/// ed25519 pubkey we gossip); the mapping is owned by `RaftMesh`.
pub type RaftNodeId = u64;

/// The state-machine reply written back to the proposing client.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ApplyResult {
    /// Last-applied log index after this command was processed.
    pub applied_index: u64,
    /// Last-applied Raft term.
    pub applied_term: u64,
}

openraft::declare_raft_types!(
    /// engenho-revoada's openraft type configuration.
    pub TypeConfig:
        D            = RoleAssignment,
        R            = ApplyResult,
        NodeId       = RaftNodeId,
        Node         = BasicNode,
        Entry        = openraft::Entry<TypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);
