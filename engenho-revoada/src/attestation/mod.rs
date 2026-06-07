//! Layer D — attested role transitions via BLAKE3 chain + ed25519
//! signatures.
//!
//! R4 — wired with **real cryptography**. Every Raft-committed
//! [`RoleAssignment`] is wrapped in a [`RoleAttestationBlock`]
//! that:
//!
//!   1. Carries a `prev_hash` linking to the prior block's BLAKE3
//!      hash. Genesis has `[0; 32]`.
//!   2. Is signed by the proposing leader's ed25519 keypair (the
//!      keypair whose public key IS the leader's [`NodeId`]).
//!   3. Optionally carries co-witness signatures from other voters
//!      (R4.5 wires this; today only leader signs).
//!
//! The [`AttestationChain`] is the typed log of these blocks. It
//! provides:
//!
//!   * `append(...)` — atomic block emission (signing + linking)
//!   * `verify(...)` — full chain walk: BLAKE3 linkage + signature
//!     verification for every block
//!   * `head_hash()` — for nodes joining late
//!   * `iter()` — for export / introspection
//!
//! Verification is **standalone** — anyone with the chain bytes
//! + the participating nodes' public keys ([`NodeId`]s) can
//! `verify` without any other engenho infrastructure.

pub mod identity;

pub use identity::{NodeIdentity, verify_signature};

use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::NodeId;
use crate::consensus::RoleAssignment;

/// One block in the role attestation chain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleAttestationBlock {
    /// BLAKE3 hash of the prior block's canonical bytes. Genesis = `[0; 32]`.
    pub prev_hash: [u8; 32],
    /// The committed assignment.
    pub assignment: RoleAssignment,
    /// Unix milliseconds when the Raft commit was applied.
    pub committed_at_ms: u64,
    /// Raft term of the commit.
    pub raft_term: u64,
    /// Raft log index of the commit.
    pub raft_log_index: u64,
    /// Proposing leader's [`NodeId`] (the ed25519 public key).
    pub leader: NodeId,
    /// Leader's ed25519 signature over canonical bytes
    /// (`canonical_bytes_for_signing`).
    pub leader_signature: Vec<u8>,
    /// Optional co-signers (R4.5 wires real multi-sig; today
    /// always empty).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub witness_signatures: Vec<WitnessSignature>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WitnessSignature {
    pub witness: NodeId,
    pub signature: Vec<u8>,
}

impl RoleAttestationBlock {
    /// Canonical bytes the leader signs. Stable serialization so
    /// reproducers (e.g. `kensa verify`) compute the same hash.
    #[must_use]
    pub fn canonical_bytes_for_signing(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct SigningPayload<'a> {
            prev_hash: &'a [u8; 32],
            assignment: &'a RoleAssignment,
            committed_at_ms: u64,
            raft_term: u64,
            raft_log_index: u64,
            leader: &'a NodeId,
        }
        serde_json::to_vec(&SigningPayload {
            prev_hash: &self.prev_hash,
            assignment: &self.assignment,
            committed_at_ms: self.committed_at_ms,
            raft_term: self.raft_term,
            raft_log_index: self.raft_log_index,
            leader: &self.leader,
        })
        .expect("serde_json on owned typed payload never fails")
    }

    /// BLAKE3 hash of this block's canonical signing payload —
    /// the value the NEXT block's `prev_hash` must equal.
    #[must_use]
    pub fn blake3_hash(&self) -> [u8; 32] {
        let bytes = self.canonical_bytes_for_signing();
        *blake3::hash(&bytes).as_bytes()
    }
}

/// Errors from chain operations.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AttestationError {
    #[error("block at index {index}: prev_hash does not match prior block's hash")]
    BrokenLink { index: usize },
    #[error("block at index {index}: leader signature failed verification")]
    InvalidLeaderSignature { index: usize },
    #[error("block at index {index}: witness signature failed verification")]
    InvalidWitnessSignature { index: usize },
    #[error("genesis block must have prev_hash = [0; 32]")]
    InvalidGenesis,
    #[error("invalid public key bytes for node {0}")]
    InvalidPublicKey(NodeId),
    #[error("bad signature from {0}")]
    BadSignature(NodeId),
    #[error("chain is empty")]
    Empty,
}

engenho_substrate::impl_error_kind! {
    AttestationError {
        { BrokenLink { .. } } => "broken_link",
        { InvalidLeaderSignature { .. } } => "invalid_leader_signature",
        { InvalidWitnessSignature { .. } } => "invalid_witness_signature",
        InvalidGenesis => "invalid_genesis",
        (InvalidPublicKey(_)) => "invalid_public_key",
        (BadSignature(_)) => "bad_signature",
        Empty => "empty",
    }
}

/// In-memory append-only chain of attestation blocks.
#[derive(Clone, Default)]
pub struct AttestationChain {
    inner: Arc<Mutex<Vec<RoleAttestationBlock>>>,
}

impl AttestationChain {
    pub fn new() -> Self {
        Self::default()
    }

    /// Atomic append of a signed block. Signs `assignment` with
    /// `identity` after linking to the current head.
    #[allow(clippy::too_many_arguments)]
    pub fn append(
        &self,
        identity: &NodeIdentity,
        assignment: RoleAssignment,
        committed_at_ms: u64,
        raft_term: u64,
        raft_log_index: u64,
    ) -> RoleAttestationBlock {
        let mut guard = self.inner.lock().unwrap();
        let prev_hash = guard
            .last()
            .map(RoleAttestationBlock::blake3_hash)
            .unwrap_or([0; 32]);
        let mut block = RoleAttestationBlock {
            prev_hash,
            assignment,
            committed_at_ms,
            raft_term,
            raft_log_index,
            leader: identity.node_id(),
            leader_signature: Vec::with_capacity(64),
            witness_signatures: Vec::new(),
        };
        let sig = identity.sign(&block.canonical_bytes_for_signing());
        block.leader_signature = sig.to_vec();
        guard.push(block.clone());
        block
    }

    /// Append an externally-constructed block. Useful when a
    /// follower receives a block from the leader and wants to
    /// store it locally — the follower has already verified.
    pub fn append_external(&self, block: RoleAttestationBlock) {
        self.inner.lock().unwrap().push(block);
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }

    pub fn head_hash(&self) -> Option<[u8; 32]> {
        self.inner
            .lock()
            .unwrap()
            .last()
            .map(RoleAttestationBlock::blake3_hash)
    }

    /// Snapshot of the chain — clones the blocks for read-only
    /// export.
    pub fn snapshot(&self) -> Vec<RoleAttestationBlock> {
        self.inner.lock().unwrap().clone()
    }

    /// Full chain verification — BLAKE3 linkage + every leader
    /// signature.
    ///
    /// # Errors
    ///
    /// Returns the first violation encountered.
    pub fn verify(&self) -> Result<(), AttestationError> {
        let blocks = self.snapshot();
        verify_chain(&blocks)
    }
}

/// Standalone chain verification — usable from `kensa verify`,
/// an auditor's offline tool, or anywhere that has the chain
/// bytes + public keys.
pub fn verify_chain(blocks: &[RoleAttestationBlock]) -> Result<(), AttestationError> {
    if blocks.is_empty() {
        return Ok(());
    }
    if blocks[0].prev_hash != [0; 32] {
        return Err(AttestationError::InvalidGenesis);
    }
    for (i, block) in blocks.iter().enumerate() {
        // Linkage (i > 0)
        if i > 0 {
            let expected = blocks[i - 1].blake3_hash();
            if block.prev_hash != expected {
                return Err(AttestationError::BrokenLink { index: i });
            }
        }
        // Leader signature
        let sig_bytes = block.leader_signature.as_slice();
        if sig_bytes.len() != 64 {
            return Err(AttestationError::InvalidLeaderSignature { index: i });
        }
        let mut sig_arr = [0u8; 64];
        sig_arr.copy_from_slice(sig_bytes);
        let canonical = block.canonical_bytes_for_signing();
        verify_signature(&block.leader, &canonical, &sig_arr)
            .map_err(|_| AttestationError::InvalidLeaderSignature { index: i })?;
        // Witness signatures (R4.5: today empty)
        for ws in &block.witness_signatures {
            let ws_sig_bytes = ws.signature.as_slice();
            if ws_sig_bytes.len() != 64 {
                return Err(AttestationError::InvalidWitnessSignature { index: i });
            }
            let mut ws_arr = [0u8; 64];
            ws_arr.copy_from_slice(ws_sig_bytes);
            verify_signature(&ws.witness, &canonical, &ws_arr)
                .map_err(|_| AttestationError::InvalidWitnessSignature { index: i })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::Reason;
    use crate::membership::NodeRole;
    use std::collections::BTreeSet;

    fn promote_cmd(seed: u8) -> RoleAssignment {
        let mut roles = BTreeSet::new();
        roles.insert(NodeRole::ApiServer);
        RoleAssignment::Promote {
            node_id: NodeId::new([seed; 32]),
            roles,
            reason: Reason::Operator,
        }
    }

    #[test]
    fn empty_chain_verifies() {
        let chain = AttestationChain::new();
        assert!(chain.verify().is_ok());
        assert_eq!(chain.len(), 0);
        assert!(chain.is_empty());
        assert_eq!(chain.head_hash(), None);
    }

    #[test]
    fn single_block_chain_verifies() {
        let chain = AttestationChain::new();
        let id = NodeIdentity::from_seed([1; 32]);
        let block = chain.append(&id, promote_cmd(0xa1), 1, 1, 1);
        assert_eq!(block.prev_hash, [0; 32]);
        assert_eq!(block.leader, id.node_id());
        assert_eq!(block.leader_signature.len(), 64);
        assert_eq!(chain.len(), 1);
        chain.verify().expect("single block verifies");
    }

    #[test]
    fn three_block_chain_verifies() {
        let chain = AttestationChain::new();
        let id = NodeIdentity::from_seed([2; 32]);
        let b0 = chain.append(&id, promote_cmd(1), 100, 1, 1);
        let b1 = chain.append(&id, promote_cmd(2), 200, 1, 2);
        let b2 = chain.append(&id, promote_cmd(3), 300, 1, 3);
        assert_eq!(chain.len(), 3);
        // Linkage holds.
        assert_eq!(b1.prev_hash, b0.blake3_hash());
        assert_eq!(b2.prev_hash, b1.blake3_hash());
        chain.verify().expect("three block chain verifies");
    }

    #[test]
    fn head_hash_advances_after_each_append() {
        let chain = AttestationChain::new();
        let id = NodeIdentity::from_seed([3; 32]);
        let h0 = chain.head_hash();
        chain.append(&id, promote_cmd(1), 100, 1, 1);
        let h1 = chain.head_hash();
        chain.append(&id, promote_cmd(2), 200, 1, 2);
        let h2 = chain.head_hash();
        assert_eq!(h0, None);
        assert!(h1.is_some());
        assert!(h2.is_some());
        assert_ne!(h1, h2);
    }

    #[test]
    fn external_block_appends_without_re_signing() {
        let chain_a = AttestationChain::new();
        let chain_b = AttestationChain::new();
        let id = NodeIdentity::from_seed([4; 32]);
        let block = chain_a.append(&id, promote_cmd(7), 100, 1, 1);
        chain_b.append_external(block);
        assert_eq!(chain_b.len(), 1);
        chain_b.verify().expect("external append still verifies");
    }

    #[test]
    fn tampered_block_field_breaks_signature() {
        let chain = AttestationChain::new();
        let id = NodeIdentity::from_seed([5; 32]);
        chain.append(&id, promote_cmd(1), 100, 1, 1);
        // Reach in + mutate a field.
        {
            let mut guard = chain.inner.lock().unwrap();
            guard[0].committed_at_ms = 999;
        }
        let err = chain.verify().unwrap_err();
        assert_eq!(err, AttestationError::InvalidLeaderSignature { index: 0 });
    }

    #[test]
    fn broken_prev_hash_is_detected() {
        let chain = AttestationChain::new();
        let id = NodeIdentity::from_seed([6; 32]);
        chain.append(&id, promote_cmd(1), 100, 1, 1);
        chain.append(&id, promote_cmd(2), 200, 1, 2);
        // Sabotage block 1's prev_hash.
        {
            let mut guard = chain.inner.lock().unwrap();
            guard[1].prev_hash = [0xff; 32];
        }
        let err = chain.verify().unwrap_err();
        assert_eq!(err, AttestationError::BrokenLink { index: 1 });
    }

    #[test]
    fn nonzero_genesis_prev_hash_is_rejected() {
        let chain = AttestationChain::new();
        let id = NodeIdentity::from_seed([7; 32]);
        chain.append(&id, promote_cmd(1), 100, 1, 1);
        {
            let mut guard = chain.inner.lock().unwrap();
            guard[0].prev_hash = [0x42; 32];
        }
        let err = chain.verify().unwrap_err();
        assert_eq!(err, AttestationError::InvalidGenesis);
    }

    #[test]
    fn forged_signature_with_wrong_length_is_rejected() {
        let chain = AttestationChain::new();
        let id = NodeIdentity::from_seed([8; 32]);
        chain.append(&id, promote_cmd(1), 100, 1, 1);
        {
            let mut guard = chain.inner.lock().unwrap();
            // Truncate the signature so it's the wrong length.
            guard[0].leader_signature.truncate(32);
        }
        let err = chain.verify().unwrap_err();
        assert_eq!(err, AttestationError::InvalidLeaderSignature { index: 0 });
    }

    #[test]
    fn leader_swap_invalidates_signature() {
        let chain = AttestationChain::new();
        let id_alice = NodeIdentity::from_seed([0x0a; 32]);
        let id_bob = NodeIdentity::from_seed([0x0b; 32]);
        chain.append(&id_alice, promote_cmd(1), 100, 1, 1);
        // Mutate the leader field to Bob's NodeId — Alice's
        // signature no longer verifies against Bob's pubkey AND
        // the canonical bytes (which include leader=Bob) are
        // different anyway.
        {
            let mut guard = chain.inner.lock().unwrap();
            guard[0].leader = id_bob.node_id();
        }
        let err = chain.verify().unwrap_err();
        assert_eq!(err, AttestationError::InvalidLeaderSignature { index: 0 });
    }

    #[test]
    fn forged_witness_signature_is_rejected() {
        let chain = AttestationChain::new();
        let id_leader = NodeIdentity::from_seed([0x10; 32]);
        let id_witness = NodeIdentity::from_seed([0x11; 32]);
        chain.append(&id_leader, promote_cmd(1), 100, 1, 1);
        // Now inject a witness signature whose bytes are bogus.
        {
            let mut guard = chain.inner.lock().unwrap();
            guard[0].witness_signatures.push(WitnessSignature {
                witness: id_witness.node_id(),
                signature: vec![0xab; 64], // bogus, doesn't actually sign anything
            });
        }
        let err = chain.verify().unwrap_err();
        assert_eq!(err, AttestationError::InvalidWitnessSignature { index: 0 });
    }

    #[test]
    fn legitimate_witness_signature_verifies() {
        let chain = AttestationChain::new();
        let id_leader = NodeIdentity::from_seed([0x20; 32]);
        let id_witness = NodeIdentity::from_seed([0x21; 32]);
        chain.append(&id_leader, promote_cmd(1), 100, 1, 1);
        // Co-sign the same canonical bytes with witness's key.
        {
            let mut guard = chain.inner.lock().unwrap();
            let canonical = guard[0].canonical_bytes_for_signing();
            let sig = id_witness.sign(&canonical);
            guard[0].witness_signatures.push(WitnessSignature {
                witness: id_witness.node_id(),
                signature: sig.to_vec(),
            });
        }
        chain.verify().expect("legitimate witness sig verifies");
    }

    #[test]
    fn standalone_verify_chain_works_without_attestation_chain() {
        // Auditor scenario: only has the block bytes + leader pubkey.
        let id = NodeIdentity::from_seed([0x30; 32]);
        let chain = AttestationChain::new();
        chain.append(&id, promote_cmd(1), 100, 1, 1);
        chain.append(&id, promote_cmd(2), 200, 1, 2);
        let blocks = chain.snapshot();
        verify_chain(&blocks).expect("standalone verifies");
    }

    #[test]
    fn block_serde_round_trips() {
        let id = NodeIdentity::from_seed([0x40; 32]);
        let chain = AttestationChain::new();
        let block = chain.append(&id, promote_cmd(7), 999, 5, 42);
        let json = serde_json::to_string(&block).unwrap();
        let back: RoleAttestationBlock = serde_json::from_str(&json).unwrap();
        assert_eq!(back, block);
        // The deserialized block still verifies signatures.
        verify_chain(&[back]).expect("deserialized block verifies");
    }

    use proptest::prelude::*;

    proptest! {
        /// Property: any chain built by appending N random
        /// assignments verifies under standalone `verify_chain`.
        #[test]
        fn arb_chain_verifies(
            seed in 0u8..255,
            count in 1usize..10,
            seeds in proptest::collection::vec(0u8..255, 1..10),
        ) {
            let id = NodeIdentity::from_seed([seed; 32]);
            let chain = AttestationChain::new();
            for (i, s) in seeds.iter().take(count).enumerate() {
                chain.append(&id, promote_cmd(*s), 100 + i as u64, 1, i as u64 + 1);
            }
            let blocks = chain.snapshot();
            verify_chain(&blocks).expect("appended chain verifies");
        }

        /// Property: tampering with ANY field of ANY block in a
        /// random chain results in either a broken-link or
        /// invalid-signature error — never silent acceptance.
        #[test]
        fn arb_tampered_chain_always_rejected(
            seed in 0u8..255,
            target_idx in 0usize..3,
            field_choice in 0usize..3,
        ) {
            let id = NodeIdentity::from_seed([seed; 32]);
            let chain = AttestationChain::new();
            for i in 0..3 {
                chain.append(&id, promote_cmd(i as u8), 100 + i, 1, i as u64 + 1);
            }
            let mut blocks = chain.snapshot();
            // Tamper with one block's chosen field.
            match field_choice {
                0 => blocks[target_idx].committed_at_ms = 9999,
                1 => blocks[target_idx].raft_log_index = 9999,
                _ => {
                    // Mutate first byte of prev_hash if not genesis
                    if target_idx > 0 {
                        blocks[target_idx].prev_hash[0] ^= 0xff;
                    } else {
                        // Genesis: tamper assignment instead
                        blocks[target_idx].committed_at_ms = 9999;
                    }
                }
            }
            let result = verify_chain(&blocks);
            prop_assert!(result.is_err(), "tampered chain must NOT verify: {result:?}");
        }
    }

    #[test]
    fn blake3_hash_is_deterministic_across_appends() {
        let id = NodeIdentity::from_seed([0x50; 32]);
        let chain_1 = AttestationChain::new();
        let chain_2 = AttestationChain::new();
        // Two chains with the same identity + assignments + timestamps
        // must produce identical hashes (the linchpin of distributed
        // verification).
        let b1a = chain_1.append(&id, promote_cmd(1), 100, 1, 1);
        let b2a = chain_2.append(&id, promote_cmd(1), 100, 1, 1);
        // Signatures differ (Ed25519 is deterministic in dalek 2 — so
        // they should be equal too).
        assert_eq!(b1a.blake3_hash(), b2a.blake3_hash());
        assert_eq!(b1a.leader_signature, b2a.leader_signature);
    }
}
