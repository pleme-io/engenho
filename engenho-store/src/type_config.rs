//! openraft `TypeConfig` binding for engenho-store's Raft group.
//!
//! Mirrors the shape in engenho-revoada::consensus::type_config
//! but binds to engenho-store's resource-command domain.

use openraft::BasicNode;

use crate::command::LoggedCommand;
use crate::command::ResourceOp;

pub type RaftNodeId = u64;

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ApplyResult {
    /// Last-applied Raft log index after this command was processed.
    pub applied_index: u64,
    pub applied_term: u64,
    /// What the catalog did with this command.
    pub op: ResourceOp,
    /// The catalog's global MVCC revision once this command was applied.
    /// For a committed change it is the revision stamped onto the
    /// mutation's `metadata.resourceVersion`. For an outcome that consumed
    /// none (a no-op, a refusal, or [`ResourceOp::Unchanged`]) it is the
    /// revision the catalog already stood at — NOT the object's own
    /// resourceVersion, which an unchanged write leaves where it was; read
    /// the object for that. Decoupled from `applied_index` (the Raft log
    /// index) — see [`crate::revision`]. Consumers use this for
    /// list-then-watch resume + optimistic concurrency.
    pub revision: u64,
    /// The typed patch-interpreter error message when `op ==
    /// [`ResourceOp::PatchRejected`]`; `None` otherwise. Carried so the
    /// apiserver renders the correct typed Status (415 for server-side
    /// apply, 422/400 for a bad patch body) rather than a generic error.
    #[serde(default)]
    pub patch_error: Option<String>,
}

openraft::declare_raft_types!(
    /// engenho-store's openraft type configuration.
    pub TypeConfig:
        // A command plus the apply rules it was proposed under (T3.5).
        D            = LoggedCommand,
        R            = ApplyResult,
        NodeId       = RaftNodeId,
        Node         = BasicNode,
        Entry        = openraft::Entry<TypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);
