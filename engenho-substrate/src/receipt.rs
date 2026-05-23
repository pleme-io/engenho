//! MaterializationReceipt — typed proof that one node successfully
//! materialized one artifact at a specific moment.
//!
//! The substrate's confirmation primitive. Any controller, build
//! backend, or fabric tier emits a receipt after a successful
//! operation; receipts are content-addressed (BLAKE3 over their
//! canonical bytes) so they're idempotent + dedupable across
//! gossip / Raft / storage.
//!
//! ## Wire shape
//!
//! ```text
//! ReceiptKind { Drv, Nar, Realisation, BuildResult, Materialization(ShapeId) }
//! MaterializationReceipt {
//!     kind: ReceiptKind,
//!     subject: BLAKE3,        // the thing being attested
//!     emitter: NodeId,         // who's claiming
//!     emitted_at: u64,         // unix seconds
//!     evidence_hash: BLAKE3,   // BLAKE3 over the claim's payload
//! }
//! ```
//!
//! Receipts compose with `QuorumTracker` (next module) to express
//! "K-of-N nodes have confirmed this materialization."

use serde::{Deserialize, Serialize};

/// What's being attested. Extends as the substrate grows.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptKind {
    /// A Drv was materialized into the cache.
    Drv,
    /// A NAR blob landed at this node.
    Nar,
    /// A Realisation entry was recorded.
    Realisation,
    /// A BuildBackend completed a build.
    BuildResult,
    /// A higher-level Shape materialization (OCI image / NixOS
    /// closure / qcow2 / wasm bundle) — payload identifies which
    /// shape via a free-form tag (typically a shape name).
    Shape(String),
}

/// Opaque node identifier — 32 raw bytes (typically BLAKE3 of the
/// node's public key, or whatever the operator's identity layer
/// produces).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeId(pub [u8; 32]);

impl NodeId {
    /// Construct from raw bytes.
    #[must_use]
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Derive from arbitrary bytes via BLAKE3.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    /// Hex representation (64 chars).
    #[must_use]
    pub fn to_hex(&self) -> String {
        hex_encode(&self.0)
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// Typed receipt — one node's claim that one thing was materialized.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializationReceipt {
    /// What the receipt is for.
    pub kind: ReceiptKind,
    /// Subject hash — identifies the materialized thing (drv hash
    /// for Drv, nar hash for Nar, deterministic shape hash for Shape).
    pub subject: [u8; 32],
    /// Who emitted the receipt.
    pub emitter: NodeId,
    /// Unix-seconds timestamp.
    pub emitted_at: u64,
    /// BLAKE3 over the canonical wire bytes of the evidence
    /// (typically the materialized artifact itself, or its
    /// catalog entry). Lets verifiers re-derive without trusting
    /// the emitter.
    pub evidence_hash: [u8; 32],
}

impl MaterializationReceipt {
    /// New receipt — caller supplies all typed fields.
    #[must_use]
    pub fn new(
        kind: ReceiptKind,
        subject: [u8; 32],
        emitter: NodeId,
        emitted_at: u64,
        evidence_hash: [u8; 32],
    ) -> Self {
        Self {
            kind,
            subject,
            emitter,
            emitted_at,
            evidence_hash,
        }
    }

    /// Content-address of the receipt itself — BLAKE3 over the
    /// canonical-bytes serialization. Idempotent: two receipts
    /// with identical fields produce identical ids.
    #[must_use]
    pub fn id(&self) -> [u8; 32] {
        let bytes = serde_json::to_vec(self).expect("receipt serializes");
        *blake3::hash(&bytes).as_bytes()
    }

    /// Helper for constructing a Drv receipt for the canonical case.
    #[must_use]
    pub fn for_drv(
        drv_hash: [u8; 32],
        emitter: NodeId,
        emitted_at: u64,
        evidence_hash: [u8; 32],
    ) -> Self {
        Self::new(
            ReceiptKind::Drv,
            drv_hash,
            emitter,
            emitted_at,
            evidence_hash,
        )
    }

    /// Helper for constructing a Nar receipt.
    #[must_use]
    pub fn for_nar(
        nar_hash: [u8; 32],
        emitter: NodeId,
        emitted_at: u64,
        evidence_hash: [u8; 32],
    ) -> Self {
        Self::new(
            ReceiptKind::Nar,
            nar_hash,
            emitter,
            emitted_at,
            evidence_hash,
        )
    }

    /// True if two receipts attest to the SAME subject (regardless
    /// of emitter / timestamp). Used by `QuorumTracker` to count
    /// confirmations.
    #[must_use]
    pub fn same_subject(&self, other: &Self) -> bool {
        self.kind == other.kind && self.subject == other.subject
    }

    /// True if the evidence hashes agree. False = nodes disagree
    /// about what they materialized — surface as a hard fault.
    #[must_use]
    pub fn agrees_with(&self, other: &Self) -> bool {
        self.same_subject(other) && self.evidence_hash == other.evidence_hash
    }
}

use crate::hex::hex_encode;

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(emitter: NodeId, ts: u64) -> MaterializationReceipt {
        MaterializationReceipt::for_drv([1u8; 32], emitter, ts, [2u8; 32])
    }

    #[test]
    fn node_id_hex_is_64_chars() {
        let n = NodeId::from_bytes(b"engenho");
        assert_eq!(n.to_hex().len(), 64);
    }

    #[test]
    fn node_id_display_matches_hex() {
        let n = NodeId::from_bytes(b"x");
        assert_eq!(format!("{n}"), n.to_hex());
    }

    #[test]
    fn receipt_round_trips_via_serde() {
        let r = sample(NodeId::from_bytes(b"a"), 100);
        let bytes = serde_json::to_vec(&r).unwrap();
        let back: MaterializationReceipt = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn receipt_id_is_deterministic() {
        let r1 = sample(NodeId::from_bytes(b"a"), 100);
        let r2 = sample(NodeId::from_bytes(b"a"), 100);
        assert_eq!(r1.id(), r2.id());
    }

    #[test]
    fn different_emitters_yield_different_ids() {
        let r1 = sample(NodeId::from_bytes(b"a"), 100);
        let r2 = sample(NodeId::from_bytes(b"b"), 100);
        assert_ne!(r1.id(), r2.id());
    }

    #[test]
    fn same_subject_ignores_emitter_and_timestamp() {
        let r1 = sample(NodeId::from_bytes(b"a"), 100);
        let r2 = sample(NodeId::from_bytes(b"b"), 200);
        assert!(r1.same_subject(&r2));
    }

    #[test]
    fn agrees_with_requires_matching_evidence() {
        let r1 = sample(NodeId::from_bytes(b"a"), 100);
        let mut r2 = sample(NodeId::from_bytes(b"b"), 100);
        assert!(r1.agrees_with(&r2));
        // Mutate evidence hash → disagree.
        r2.evidence_hash[0] ^= 0xff;
        assert!(!r1.agrees_with(&r2));
        // Subject still matches though.
        assert!(r1.same_subject(&r2));
    }

    #[test]
    fn receipt_kinds_serialize_snake_case() {
        let json = serde_json::to_string(&ReceiptKind::BuildResult).unwrap();
        assert_eq!(json, "\"build_result\"");
        let json = serde_json::to_string(&ReceiptKind::Shape("oci".into())).unwrap();
        assert_eq!(json, "{\"shape\":\"oci\"}");
    }

    #[test]
    fn for_drv_constructor_sets_kind() {
        let r = MaterializationReceipt::for_drv([3u8; 32], NodeId::from_bytes(b"x"), 42, [4u8; 32]);
        assert_eq!(r.kind, ReceiptKind::Drv);
        assert_eq!(r.subject, [3u8; 32]);
        assert_eq!(r.emitted_at, 42);
    }

    #[test]
    fn for_nar_constructor_sets_kind() {
        let r = MaterializationReceipt::for_nar([5u8; 32], NodeId::from_bytes(b"x"), 42, [6u8; 32]);
        assert_eq!(r.kind, ReceiptKind::Nar);
    }

    #[test]
    fn shape_receipt_carries_tag() {
        let r = MaterializationReceipt::new(
            ReceiptKind::Shape("nixos-closure".into()),
            [0u8; 32],
            NodeId::from_bytes(b"x"),
            0,
            [0u8; 32],
        );
        match r.kind {
            ReceiptKind::Shape(tag) => assert_eq!(tag, "nixos-closure"),
            _ => panic!("expected Shape"),
        }
    }
}
