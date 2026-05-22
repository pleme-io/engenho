//! # engenho-revoada
//!
//! The **distribution layer** above `engenho` (the K8s runtime).
//! Lets engenho-managed clusters dynamically shift their control-plane
//! / worker topology in response to node failures, capacity changes,
//! or operator directives — all without external orchestration.
//!
//! Read [`docs/DISTRIBUTED.md`](../../docs/DISTRIBUTED.md) for the
//! full design. This crate's public surface mirrors that doc's
//! four-layer architecture:
//!
//! ```text
//! engenho_revoada::
//!     membership      ← Layer A: chitchat gossip + phi-accrual
//!     consensus       ← Layer B: openraft for RoleAssignment commands
//!     content         ← Layer C: iroh P2P workload sync (BLAKE3-keyed)
//!     attestation     ← Layer D: tameshi BLAKE3 chain of role shifts
//! ```
//!
//! ## Status
//!
//! **R0** — design frozen + typed surface scaffolded. Module bodies
//! ship as types-only stubs; library deps (chitchat / openraft / iroh)
//! get wired in subsequent R1–R5 phases. Tests cover the typed enums
//! + invariants of the wire shapes.
//!
//! ## Naming
//!
//! Portuguese *revoada* — a murmuration of starlings. The flock shifts
//! formation continuously, each bird following local rules, the global
//! pattern emerging without central coordination. Matches engenho-mesh's
//! gossip-elected, Raft-committed, eventually-consistent role-shifting
//! dance.

#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod attestation;
pub mod cluster;
pub mod consensus;
pub mod content;
pub mod fabric;
pub mod face;
pub mod membership;
pub mod policy;
pub mod topology;

pub use topology::{
    Cluster3MNW, MeshAllPeers, NodeId as TopologyNodeId, NodeState, Pair, Phalanx,
    Quorum3M, Role, RoleAssignment, Solo, TopologyError, TopologyReactor,
    TopologyStrategy, Transition,
};

pub use fabric::{
    AttestationConfig, ConsensusConfig, ConsensusKind, ContentConfig, FabricFace,
    FabricStrategy, FabricStrategyError, FaceKind, MembershipConfig, PlacementPolicy,
    ReconciliationCadence,
};
pub use face::{Face, FaceError, KubernetesFace, NomadFace, PureRaftFace, instantiate as instantiate_face};
pub use cluster::{Cluster, ClusterCoherenceError, ClusterDeclaration, ClusterRuntimeError};

use serde::{Deserialize, Serialize};

/// Stable node identifier — ed25519 public key bytes, hex-encoded
/// for serde wire form. Used across all four layers as the canonical
/// "which engenho instance is this" reference.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(pub [u8; 32]);

impl NodeId {
    /// Construct from raw 32 bytes (the ed25519 public key).
    #[must_use]
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// 64-char hex string for log lines + telemetry.
    #[must_use]
    pub fn to_hex(&self) -> String {
        let mut out = String::with_capacity(64);
        for b in self.0 {
            out.push_str(&format!("{b:02x}"));
        }
        out
    }

    /// Parse a NodeId from its hex representation. Accepts any
    /// length 1..=64; shorter values left-zero-pad so test-friendly
    /// shorthand like "ab" round-trips through translation layers.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the input contains non-hex characters or
    /// exceeds 64 chars.
    pub fn from_hex(s: &str) -> Result<Self, String> {
        if s.is_empty() || s.len() > 64 {
            return Err(format!("hex must be 1..=64 chars, got {}", s.len()));
        }
        let padded = format!("{s:0>64}");
        let mut bytes = [0u8; 32];
        for (i, byte) in bytes.iter_mut().enumerate() {
            let pair = &padded[i * 2..i * 2 + 2];
            *byte = u8::from_str_radix(pair, 16).map_err(|e| e.to_string())?;
        }
        Ok(Self(bytes))
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // First 12 hex chars — short enough to read, long enough to disambiguate.
        for b in &self.0[..6] {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_hex_round_trips() {
        let id = NodeId::new([0xab; 32]);
        let hex = id.to_hex();
        assert_eq!(hex.len(), 64);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn node_id_display_is_short_prefix() {
        let id = NodeId::new([0xab; 32]);
        assert_eq!(id.to_string(), "abababababab");
    }

    #[test]
    fn node_id_serde_round_trips() {
        let id = NodeId::new([0xcd; 32]);
        let json = serde_json::to_string(&id).unwrap();
        let back: NodeId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);
    }
}
