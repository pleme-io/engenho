//! Typed events that flow through the convergence pipeline.
//!
//! Each event is a small Send+Sync typed value with serde support so
//! mirante can broadcast them + tameshi can content-address them.

use engenho_sui_typescape::TypescapeValue;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Why a change was surfaced. Matches the shape shikumi-notify uses
/// (created / modified / removed) plus an initial-load variant for
/// the boot sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    /// Initial load — the Watcher's first tick after construction.
    Initial,
    /// The watched source was created (didn't exist before).
    Created,
    /// The watched source was modified in place.
    Modified,
    /// The watched source was removed (cluster should drain the
    /// resources that derived from it).
    Removed,
}

/// A surfaced change to the watched declaration. The Watcher emits
/// these; the Evaluator consumes them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Change {
    /// Stable identifier for the source — file path, URL, in-memory
    /// key. Used for logging + deduplication.
    pub source: Arc<str>,
    /// What happened to the source.
    pub kind: ChangeKind,
    /// Raw source text (the tlisp form). The Evaluator parses + types
    /// this into a [`TypescapeValue`].
    pub source_text: Arc<str>,
    /// Monotone revision — incremented per change by the Watcher.
    /// Used by the Proposer to fence stale proposals.
    pub revision: u64,
}

/// The Evaluator's typed output: the parsed + type-checked
/// representation of the declaration plus enough provenance to chain
/// attestations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Decision {
    /// The original change that triggered evaluation.
    pub change: Change,
    /// The typed value the source evaluated to. Send+Sync via
    /// `TypescapeValue`'s `Arc<...>` payload.
    pub typed: TypescapeValue,
}

/// The terminal event after the change has propagated all the way
/// through propose → attest → publish. Carries every stage's typed
/// identity so consumers can reason about end-to-end provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outcome {
    /// The originating change's revision.
    pub revision: u64,
    /// The Proposer's typed commit identifier.
    pub proposal_id: u64,
    /// The Attester's typed receipt identifier (BLAKE3 hex, abridged
    /// for log lines — full hash lives in the chain).
    pub receipt_id: Arc<str>,
    /// Wall-clock ms-since-epoch when the outcome was finalized.
    pub finalized_at_ms: u64,
}
