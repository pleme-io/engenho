//! `NodeIdentity` — the operator-owned ed25519 keypair that
//! signs attestation blocks.
//!
//! Per the LEAN.md contract, the canonical [`crate::NodeId`] IS
//! the ed25519 public key bytes. `NodeIdentity` holds the
//! corresponding signing key (private + public) so this node can
//! sign committed assignments.
//!
//! In R4 the identity is in-memory (constructed at process start
//! via [`NodeIdentity::generate`]). R5+ persists the signing key
//! via cofre so it survives restarts.

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;

use crate::NodeId;

#[derive(Clone)]
pub struct NodeIdentity {
    signing: SigningKey,
}

impl NodeIdentity {
    /// Generate a fresh keypair from the OS RNG. Use for tests +
    /// node bootstrap; production nodes should persist + reload
    /// via cofre.
    #[must_use]
    pub fn generate() -> Self {
        let signing = SigningKey::generate(&mut OsRng);
        Self { signing }
    }

    /// Construct from a known 32-byte seed (deterministic — for
    /// tests where reproducible identity matters).
    #[must_use]
    pub fn from_seed(seed: [u8; 32]) -> Self {
        Self {
            signing: SigningKey::from_bytes(&seed),
        }
    }

    /// The 32-byte public key as [`NodeId`].
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        let pk: VerifyingKey = self.signing.verifying_key();
        NodeId::new(pk.to_bytes())
    }

    /// Sign canonical bytes. The signature is 64 bytes wide
    /// (Ed25519's fixed output).
    #[must_use]
    pub fn sign(&self, bytes: &[u8]) -> [u8; 64] {
        self.signing.sign(bytes).to_bytes()
    }

    /// Raw signing-key bytes — used by tests + persistence.
    /// Production callers must keep this secret.
    #[must_use]
    pub fn signing_key_bytes(&self) -> [u8; 32] {
        self.signing.to_bytes()
    }
}

impl std::fmt::Debug for NodeIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // NEVER print the signing key — only the public node id.
        write!(f, "NodeIdentity(node_id={})", self.node_id())
    }
}

/// Verify a 64-byte signature against the canonical bytes a
/// `NodeId` (= ed25519 public key) was meant to sign.
///
/// # Errors
///
/// Returns [`AttestationError::InvalidPublicKey`] if `node_id`'s
/// bytes don't form a valid ed25519 public key (only happens on
/// bit-corruption), and [`AttestationError::BadSignature`] if the
/// signature doesn't verify.
pub fn verify_signature(
    node_id: &NodeId,
    bytes: &[u8],
    signature: &[u8; 64],
) -> Result<(), super::AttestationError> {
    let pk = VerifyingKey::from_bytes(&node_id.0)
        .map_err(|_| super::AttestationError::InvalidPublicKey(*node_id))?;
    let sig = Signature::from_bytes(signature);
    pk.verify(bytes, &sig)
        .map_err(|_| super::AttestationError::BadSignature(*node_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_produces_distinct_identities() {
        let a = NodeIdentity::generate();
        let b = NodeIdentity::generate();
        assert_ne!(a.node_id(), b.node_id());
    }

    #[test]
    fn from_seed_is_deterministic() {
        let seed = [0xab; 32];
        let a = NodeIdentity::from_seed(seed);
        let b = NodeIdentity::from_seed(seed);
        assert_eq!(a.node_id(), b.node_id());
        assert_eq!(a.signing_key_bytes(), b.signing_key_bytes());
    }

    #[test]
    fn sign_and_verify_round_trip() {
        let id = NodeIdentity::from_seed([1; 32]);
        let msg = b"engenho-revoada attestation payload";
        let sig = id.sign(msg);
        verify_signature(&id.node_id(), msg, &sig).expect("signature verifies");
    }

    #[test]
    fn tampered_message_fails_verification() {
        let id = NodeIdentity::from_seed([2; 32]);
        let msg = b"original payload";
        let sig = id.sign(msg);
        let tampered = b"different payload";
        let err = verify_signature(&id.node_id(), tampered, &sig).unwrap_err();
        assert!(matches!(
            err,
            super::super::AttestationError::BadSignature(_)
        ));
    }

    #[test]
    fn signature_from_wrong_key_fails_verification() {
        let alice = NodeIdentity::from_seed([3; 32]);
        let bob = NodeIdentity::from_seed([4; 32]);
        let msg = b"bob's message";
        // Bob signs; we try to verify with Alice's pubkey.
        let sig = bob.sign(msg);
        let err = verify_signature(&alice.node_id(), msg, &sig).unwrap_err();
        assert!(matches!(
            err,
            super::super::AttestationError::BadSignature(_)
        ));
    }

    #[test]
    fn debug_does_not_leak_signing_key() {
        let id = NodeIdentity::from_seed([5; 32]);
        let debug_str = format!("{id:?}");
        // The hex of the signing key bytes (each byte is 05) must not appear.
        let signing_hex = "0505";
        assert!(
            !debug_str.contains(signing_hex),
            "signing key leaked in Debug output: {debug_str}"
        );
        // But the node id (the pubkey) is fine to print.
        assert!(debug_str.contains("node_id="));
    }
}
