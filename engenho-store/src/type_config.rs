//! openraft `TypeConfig` binding for engenho-store's Raft group.
//!
//! Mirrors the shape in engenho-revoada::consensus::type_config
//! but binds to engenho-store's resource-command domain.

use openraft::BasicNode;

use crate::command::ResourceCommand;
use crate::command::ResourceOp;

pub type RaftNodeId = u64;

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ApplyResult {
    /// Last-applied Raft log index after this command was processed.
    pub applied_index: u64,
    pub applied_term: u64,
    /// What the catalog did with this command.
    pub op: ResourceOp,
    /// The global MVCC revision stamped onto this mutation's
    /// `metadata.resourceVersion`. `0` for a no-op (no revision was
    /// consumed). Decoupled from `applied_index` (the Raft log
    /// index) — see [`crate::revision`]. Consumers use this for
    /// list-then-watch resume + optimistic concurrency.
    pub revision: u64,
}

openraft::declare_raft_types!(
    /// engenho-store's openraft type configuration.
    pub TypeConfig:
        D            = ResourceCommand,
        R            = ApplyResult,
        NodeId       = RaftNodeId,
        Node         = BasicNode,
        Entry        = openraft::Entry<TypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);
