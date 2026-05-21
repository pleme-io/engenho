//! Layer D — attested role transitions.
//!
//! Every Raft-committed [`RoleAssignment`] is wrapped in a typed
//! `RoleAttestationBlock` and appended to the mesh's attestation
//! chain. The chain is BLAKE3-linked (same hashing as tameshi);
//! operators can verify the chain offline by walking from the
//! genesis block to the head and checking every block's
//! `prev_hash == hash(prior_block)` + signature.
//!
//! R0: typed surface + chain-walk verification logic only. R4 wires
//! `tameshi-types` for the concrete BLAKE3 hashing + ed25519
//! signature machinery.

use serde::{Deserialize, Serialize};

use crate::consensus::RoleAssignment;
use crate::NodeId;

/// One block in the role attestation chain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleAttestationBlock {
    /// BLAKE3 hash of the prior block's canonical encoding.
    /// Genesis block has `[0; 32]`.
    pub prev_hash: [u8; 32],
    /// The committed assignment.
    pub assignment: RoleAssignment,
    /// Unix milliseconds when the Raft commit happened.
    pub committed_at_ms: u64,
    /// Raft term + index of the commit.
    pub raft_term: u64,
    pub raft_log_index: u64,
    /// The leader's ed25519 signature over the canonical encoding
    /// of (prev_hash, assignment, committed_at_ms, raft_term,
    /// raft_log_index). 64 bytes; `Vec<u8>` at the wire because
    /// serde derives top out at `[u8; 32]` without serde_bytes.
    pub leader_signature: Vec<u8>,
    /// The leader node's identity.
    pub leader: NodeId,
    /// Co-signers (typically `floor(N/2) + 1` of control-plane
    /// nodes, matching the Raft quorum). R4 wires the multi-sig.
    pub witness_signatures: Vec<WitnessSignature>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WitnessSignature {
    pub witness: NodeId,
    pub signature: Vec<u8>,
}

impl RoleAttestationBlock {
    /// Canonical bytes the leader signs. Stable serialization
    /// (BTreeMap/BTreeSet derived) so multiple readers compute the
    /// same hash. R4 implementation will use a stable canonical
    /// codec; this scaffold uses serde_json as a placeholder.
    #[must_use]
    pub fn canonical_bytes_for_signing(&self) -> Vec<u8> {
        // R4 swaps for a deterministic codec (e.g. CBOR with sorted
        // map keys). serde_json::to_vec is sufficient for R0 invariant
        // tests because we control both sides.
        #[derive(Serialize)]
        struct SigningPayload<'a> {
            prev_hash: &'a [u8; 32],
            assignment: &'a RoleAssignment,
            committed_at_ms: u64,
            raft_term: u64,
            raft_log_index: u64,
        }
        serde_json::to_vec(&SigningPayload {
            prev_hash: &self.prev_hash,
            assignment: &self.assignment,
            committed_at_ms: self.committed_at_ms,
            raft_term: self.raft_term,
            raft_log_index: self.raft_log_index,
        })
        .expect("serde_json on owned types never fails")
    }
}

/// Chain-walk verifier. Walks blocks in order; for each block
/// computes `hash(prior_block.canonical_bytes_for_signing())` and
/// asserts it equals `block.prev_hash`.
///
/// R0 stub: returns the first violation (or None if chain is
/// internally consistent). Does NOT verify signatures — that lands
/// at R4 when we wire ed25519-dalek.
#[must_use]
pub fn verify_chain_links(blocks: &[RoleAttestationBlock]) -> Option<usize> {
    if blocks.is_empty() {
        return None;
    }
    // Genesis block: prev_hash must be [0; 32].
    if blocks[0].prev_hash != [0; 32] {
        return Some(0);
    }
    for i in 1..blocks.len() {
        // The prev_hash on block i must equal a hash of block (i-1)'s
        // canonical bytes. R4 plugs in BLAKE3; R0 uses a stub.
        let computed = stub_hash(&blocks[i - 1].canonical_bytes_for_signing());
        if blocks[i].prev_hash != computed {
            return Some(i);
        }
    }
    None
}

/// R0 stub: a deterministic non-BLAKE3 hash so the typed surface
/// works without pulling blake3 into the dep tree yet. R4 swaps in
/// the real BLAKE3 (via tameshi-types).
fn stub_hash(bytes: &[u8]) -> [u8; 32] {
    // FNV-1a 256-bit-truncated stand-in. Sufficient for R0 invariant
    // tests; NOT cryptographic. R4 replaces.
    let mut out = [0u8; 32];
    let mut hash: u64 = 0xcbf29ce484222325;
    for (idx, &b) in bytes.iter().enumerate() {
        hash = hash.wrapping_mul(0x100000001b3);
        hash ^= u64::from(b);
        out[idx % 32] ^= ((hash >> ((idx % 8) * 8)) & 0xff) as u8;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::{Reason, RoleAssignment};
    use crate::membership::NodeRole;
    use std::collections::BTreeSet;

    fn promote_block(prev: [u8; 32], index: u64) -> RoleAttestationBlock {
        RoleAttestationBlock {
            prev_hash: prev,
            assignment: RoleAssignment::Promote {
                node_id: NodeId::new([1; 32]),
                roles: [NodeRole::ApiServer].into_iter().collect::<BTreeSet<_>>(),
                reason: Reason::Operator,
            },
            committed_at_ms: 1_700_000_000_000 + index,
            raft_term: 1,
            raft_log_index: index,
            leader_signature: vec![0; 64],
            leader: NodeId::new([2; 32]),
            witness_signatures: vec![],
        }
    }

    #[test]
    fn empty_chain_is_valid() {
        assert!(verify_chain_links(&[]).is_none());
    }

    #[test]
    fn single_genesis_block_is_valid() {
        let blocks = vec![promote_block([0; 32], 1)];
        assert!(verify_chain_links(&blocks).is_none());
    }

    #[test]
    fn genesis_with_nonzero_prev_is_rejected() {
        let blocks = vec![promote_block([1; 32], 1)];
        assert_eq!(verify_chain_links(&blocks), Some(0));
    }

    #[test]
    fn properly_linked_chain_validates() {
        let b0 = promote_block([0; 32], 1);
        let h0 = stub_hash(&b0.canonical_bytes_for_signing());
        let b1 = promote_block(h0, 2);
        let h1 = stub_hash(&b1.canonical_bytes_for_signing());
        let b2 = promote_block(h1, 3);
        let blocks = vec![b0, b1, b2];
        assert!(verify_chain_links(&blocks).is_none());
    }

    #[test]
    fn broken_link_is_detected_at_the_right_index() {
        let b0 = promote_block([0; 32], 1);
        let h0 = stub_hash(&b0.canonical_bytes_for_signing());
        let mut b1 = promote_block(h0, 2);
        // Sabotage b1's prev_hash:
        b1.prev_hash = [0xff; 32];
        let blocks = vec![b0, b1];
        assert_eq!(verify_chain_links(&blocks), Some(1));
    }
}
