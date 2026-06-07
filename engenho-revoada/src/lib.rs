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
pub mod audit;
pub mod backend;
pub mod backend_fs;
pub mod backends;
pub mod cluster;
pub mod config_bridge;
pub mod consensus;
pub mod content;
pub mod fabric;
pub mod face;
pub mod face_store;
pub mod federation;
pub mod format;
pub mod membership;
pub mod policy;
pub mod topology;

pub use audit::{
    AuditEvent, AuditLog, AuditingBackend, FileAuditLog, InMemoryAuditLog, NoopAuditLog, VerbKind,
};
pub use backend::{
    StoreBackend, StubBackend, kube_apiserver_stub, nomad_http_stub, raft_stub,
    supervised_systemd_stub, systemd_dbus_stub,
};
pub use backend_fs::FileSystemBackend;
pub use backends::{
    KubeApiServerBackend, KubeApiServerConfig, NomadHttpBackend, NomadHttpConfig, RaftBackend,
    RaftConfig, SupervisedSystemdBackend, SupervisedSystemdConfig, SystemdDbusBackend,
    SystemdDbusConfig,
};

pub use format::{
    AdapterError, FormatAdapter, HclAdapter, K8sJsonAdapter, K8sYamlAdapter,
    NativePassthroughAdapter, encode_envelope,
};

pub use federation::{FederatedEventKind, FederatedFabric, FederationError, RoutingPolicy};

pub use topology::{
    Cluster3MNW, MeshAllPeers, NodeId as TopologyNodeId, NodeState, Pair, Phalanx, Quorum3M, Role,
    RoleAssignment, Solo, TopologyError, TopologyReactor, TopologyStrategy, Transition,
};

pub use cluster::{
    Cluster, ClusterBuilder, ClusterBuilderError, ClusterCoherenceError, ClusterDeclaration,
    ClusterHealth, ClusterRuntimeError,
};
pub use fabric::{
    AttestationConfig, ConsensusConfig, ConsensusKind, ContentConfig, FabricFace, FabricStrategy,
    FabricStrategyError, FaceKind, MembershipConfig, PlacementPolicy, ReconciliationCadence,
};
pub use face::{
    BareMetalSupervisorFace, Face, FaceError, FaceWatchEvent, FaceWatchEventKind, FaceWatchStream,
    KubernetesFace, NomadFace, PureRaftFace, ResourceFormat, ResourceRef, SystemdFace,
    instantiate as instantiate_face,
};

engenho_substrate::define_hash_newtype! {
    #[derive(Copy)]
    #[serde(transparent)]
    /// Stable node identifier — ed25519 public key bytes, hex-encoded
    /// for serde wire form. Used across all four layers as the canonical
    /// "which engenho instance is this" reference.
    ///
    /// `Display` is the first 6 bytes (12 hex chars) — short enough to
    /// read, long enough to disambiguate. `from_hex` left-zero-pads
    /// `1..=64`-char input so shorthand like `"ab"` round-trips through
    /// translation layers.
    NodeId { display = prefix(6), from_hex = padded }
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
