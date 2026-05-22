//! C5 — per-resource consistency-tier annotations.
//!
//! Per `docs/CONSISTENCY-FABRIC.md`, engenho's data plane has four
//! transports (Strong / EventualGossip / DurableStream / Content).
//! Most resources default to Strong (Raft) at admission; this module
//! provides the typed surface for callers to opt resources into
//! other tiers via the canonical annotation:
//!
//! ```text
//! metadata.annotations.engenho.io/consistency-tier: eventual_gossip
//! ```
//!
//! ## Design notes
//!
//! - The enum is **closed** — every variant maps to a concrete
//!   transport. Adding a new transport is a typed change (one new
//!   variant + exhaustive match).
//! - Default is [`ConsistencyTier::Strong`] — the safe choice for
//!   any resource that controllers expect to read linearizably.
//! - At admission time (R7.5c), the apiserver reads the annotation,
//!   parses it through [`ConsistencyTier::from_annotation`], and
//!   stores the typed variant alongside the resource in
//!   `ResourceValue::consistency_tier` (R7.5c schema extension).
//! - At write time (post-R7.5c), the apiserver consults the typed
//!   variant + routes the propose through the correct transport
//!   (Raft for Strong; gossip topic for EventualGossip; etc.).
//!
//! ## Invariant
//!
//! `ConsistencyTier::Strong.is_strong()` is `true`; every other
//! variant is `false`. The substrate enforces "if you're not Strong,
//! you opted in deliberately — controllers must handle staleness
//! explicitly."

use serde::{Deserialize, Serialize};

/// The canonical annotation key consumed by admission.
pub const CONSISTENCY_TIER_ANNOTATION: &str = "engenho.io/consistency-tier";

/// Typed consistency tier — one variant per transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsistencyTier {
    /// Linearizable Raft commits. Default. Reads see latest committed
    /// state. Used for resources that controllers expect to read
    /// linearizably (Pod.spec, Service, Endpoints, Secret, RBAC).
    Strong,

    /// Eventually-consistent via chitchat gossip. 1-3s convergence.
    /// Used for soft data: per-node metrics, observed capacity,
    /// node liveness signals, cluster membership.
    EventualGossip,

    /// Durable streamed via NATS JetStream. At-least-once with
    /// cursor replay. Used for audit logs, attestation receipts,
    /// watch event archives, anything that must be REPLAYABLE.
    DurableStream,

    /// Content-addressed via iroh or NATS Object. BLAKE3 hash →
    /// P2P resolve. Used for large blobs (image layers, large
    /// ConfigMap data, Helm chart tarballs). Deduplicated.
    Content,
}

impl Default for ConsistencyTier {
    fn default() -> Self {
        Self::Strong
    }
}

impl ConsistencyTier {
    /// Parse the annotation value. Returns [`ConsistencyTier::Strong`]
    /// on unknown values — invalid annotations don't promote a write
    /// to a weaker tier by accident.
    ///
    /// Accepts snake_case (canonical), kebab-case, or PascalCase
    /// so YAML authors don't have to remember the exact case.
    #[must_use]
    pub fn from_annotation(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "strong" => Self::Strong,
            "eventual_gossip" | "eventualgossip" | "gossip" => Self::EventualGossip,
            "durable_stream" | "durablestream" | "stream" => Self::DurableStream,
            "content" | "content_addressed" | "contentaddressed" => Self::Content,
            _ => Self::Strong, // safe default for unknown
        }
    }

    /// Stable canonical string used in annotation writes + telemetry.
    #[must_use]
    pub fn as_canonical(self) -> &'static str {
        match self {
            Self::Strong => "strong",
            Self::EventualGossip => "eventual_gossip",
            Self::DurableStream => "durable_stream",
            Self::Content => "content",
        }
    }

    /// Returns true for [`Self::Strong`] only. Useful for admission
    /// + early-out checks in the propose path.
    #[must_use]
    pub fn is_strong(self) -> bool {
        matches!(self, Self::Strong)
    }

    /// Returns true if writes through this tier require quorum.
    #[must_use]
    pub fn requires_quorum(self) -> bool {
        matches!(self, Self::Strong)
    }

    /// Returns true if writes through this tier persist a durable
    /// record (Raft log OR JetStream stream). Strong + DurableStream
    /// both qualify; gossip is in-memory + best-effort.
    #[must_use]
    pub fn persists(self) -> bool {
        matches!(self, Self::Strong | Self::DurableStream)
    }
}

/// Look up the consistency tier from a resource's
/// `metadata.annotations` map. Returns [`ConsistencyTier::Strong`]
/// when the annotation is absent.
#[must_use]
pub fn tier_from_metadata(annotations: Option<&serde_json::Value>) -> ConsistencyTier {
    let Some(annotations) = annotations else {
        return ConsistencyTier::Strong;
    };
    let Some(value) = annotations.get(CONSISTENCY_TIER_ANNOTATION) else {
        return ConsistencyTier::Strong;
    };
    let Some(s) = value.as_str() else {
        return ConsistencyTier::Strong;
    };
    ConsistencyTier::from_annotation(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn default_is_strong() {
        assert_eq!(ConsistencyTier::default(), ConsistencyTier::Strong);
    }

    #[test]
    fn from_annotation_canonical() {
        assert_eq!(
            ConsistencyTier::from_annotation("strong"),
            ConsistencyTier::Strong
        );
        assert_eq!(
            ConsistencyTier::from_annotation("eventual_gossip"),
            ConsistencyTier::EventualGossip
        );
        assert_eq!(
            ConsistencyTier::from_annotation("durable_stream"),
            ConsistencyTier::DurableStream
        );
        assert_eq!(
            ConsistencyTier::from_annotation("content"),
            ConsistencyTier::Content
        );
    }

    #[test]
    fn from_annotation_kebab_case() {
        assert_eq!(
            ConsistencyTier::from_annotation("eventual-gossip"),
            ConsistencyTier::EventualGossip
        );
        assert_eq!(
            ConsistencyTier::from_annotation("durable-stream"),
            ConsistencyTier::DurableStream
        );
    }

    #[test]
    fn from_annotation_case_insensitive() {
        assert_eq!(
            ConsistencyTier::from_annotation("EventualGossip"),
            ConsistencyTier::EventualGossip
        );
        assert_eq!(
            ConsistencyTier::from_annotation("DURABLE_STREAM"),
            ConsistencyTier::DurableStream
        );
    }

    #[test]
    fn from_annotation_aliases() {
        assert_eq!(
            ConsistencyTier::from_annotation("gossip"),
            ConsistencyTier::EventualGossip
        );
        assert_eq!(
            ConsistencyTier::from_annotation("stream"),
            ConsistencyTier::DurableStream
        );
        assert_eq!(
            ConsistencyTier::from_annotation("content_addressed"),
            ConsistencyTier::Content
        );
    }

    #[test]
    fn from_annotation_unknown_defaults_to_strong() {
        assert_eq!(
            ConsistencyTier::from_annotation("weird-unknown"),
            ConsistencyTier::Strong
        );
        assert_eq!(
            ConsistencyTier::from_annotation(""),
            ConsistencyTier::Strong
        );
    }

    #[test]
    fn as_canonical_round_trips() {
        for t in [
            ConsistencyTier::Strong,
            ConsistencyTier::EventualGossip,
            ConsistencyTier::DurableStream,
            ConsistencyTier::Content,
        ] {
            assert_eq!(ConsistencyTier::from_annotation(t.as_canonical()), t);
        }
    }

    #[test]
    fn is_strong_only_for_strong() {
        assert!(ConsistencyTier::Strong.is_strong());
        assert!(!ConsistencyTier::EventualGossip.is_strong());
        assert!(!ConsistencyTier::DurableStream.is_strong());
        assert!(!ConsistencyTier::Content.is_strong());
    }

    #[test]
    fn requires_quorum_only_for_strong() {
        assert!(ConsistencyTier::Strong.requires_quorum());
        assert!(!ConsistencyTier::EventualGossip.requires_quorum());
        assert!(!ConsistencyTier::DurableStream.requires_quorum());
        assert!(!ConsistencyTier::Content.requires_quorum());
    }

    #[test]
    fn persists_for_strong_and_durable_stream() {
        assert!(ConsistencyTier::Strong.persists());
        assert!(ConsistencyTier::DurableStream.persists());
        assert!(!ConsistencyTier::EventualGossip.persists());
        // Content is durably stored externally (CAS hash); the
        // engenho-store layer doesn't track it as durable.
        assert!(!ConsistencyTier::Content.persists());
    }

    #[test]
    fn tier_from_metadata_annotations_map() {
        let annotations = json!({"engenho.io/consistency-tier": "eventual_gossip"});
        assert_eq!(
            tier_from_metadata(Some(&annotations)),
            ConsistencyTier::EventualGossip
        );
    }

    #[test]
    fn tier_from_metadata_returns_default_when_absent() {
        let annotations = json!({"other": "value"});
        assert_eq!(
            tier_from_metadata(Some(&annotations)),
            ConsistencyTier::Strong
        );
        assert_eq!(tier_from_metadata(None), ConsistencyTier::Strong);
    }

    #[test]
    fn serde_round_trip() {
        let t = ConsistencyTier::DurableStream;
        let s = serde_json::to_string(&t).unwrap();
        assert_eq!(s, "\"durable_stream\"");
        let back: ConsistencyTier = serde_json::from_str(&s).unwrap();
        assert_eq!(back, t);
    }
}
