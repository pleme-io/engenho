//! selo — typed capability tokens.
//!
//! Per the research brief — ninth and final inventive primitive.
//! Capability-as-data: `(subject, capability, expires_at)` MAC'd
//! with a shared secret. Holder of a valid `Selo` is authorized
//! to perform `capability` as `subject` until `expires_at`.
//!
//! Composes with:
//!   - `relógio::Instant` for expiry — same `Instant` type used by
//!     orçamento + mirante; one clock, one expiry semantics
//!   - `BLAKE3` keyed-hash for MAC — same hash used by linhagem-aberta
//!     + tameshi; one signature primitive across the substrate
//!   - `ErrorKind` for typed errors (`Expired` / `InvalidSignature` /
//!     `SubjectMismatch` / `CapabilityMismatch`)
//!
//! ## Determinism contract
//!
//!   - Same (subject, capability, `expires_at`, secret) → same
//!     signature, byte-identical
//!   - Verify is constant-time-safe via BLAKE3's `verify` (no
//!     early-out on first byte mismatch)
//!
//! ## Surface
//!
//!   - `Selo` — the token itself; Serialize/Deserialize for transport
//!   - `SeloIssuer` — holds the secret; mints + verifies Selos
//!   - `SeloError` — typed verification errors

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::relogio::Instant;
use crate::risca::Risca;

/// Errors raised by [`SeloIssuer::verify`].
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum SeloError {
    /// `now` is at or after `selo.expires_at`.
    #[error("expired: now_packed={now_packed}, expires_packed={expires_packed}")]
    Expired {
        /// Packed Instant for "now" as observed by verifier.
        now_packed: u64,
        /// Packed Instant the selo was minted to expire at.
        expires_packed: u64,
    },
    /// MAC didn't verify — selo was tampered with OR was minted by a
    /// different issuer.
    #[error("invalid signature")]
    InvalidSignature,
    /// `selo.subject` doesn't match the expected subject the caller
    /// asserted.
    #[error("subject mismatch: expected={expected:?}, got={got:?}")]
    SubjectMismatch {
        /// What the verifier expected.
        expected: String,
        /// What the selo carried.
        got: String,
    },
    /// `selo.capability` doesn't match the expected capability.
    #[error("capability mismatch: expected={expected:?}, got={got:?}")]
    CapabilityMismatch {
        /// What the verifier expected.
        expected: String,
        /// What the selo carried.
        got: String,
    },
}

crate::impl_error_kind! {
    SeloError {
        { Expired { .. } } => "expired",
        InvalidSignature => "invalid_signature",
        { SubjectMismatch { .. } } => "subject_mismatch",
        { CapabilityMismatch { .. } } => "capability_mismatch",
    }
}

/// A signed capability token. `signature` is a BLAKE3 keyed-hash
/// over the canonical encoding of (subject, capability, `expires_at`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Selo {
    /// Subject the capability is granted to (e.g. user ID or service name).
    pub subject: String,
    /// What the subject is authorized to do.
    pub capability: String,
    /// Expiry instant — selo is invalid at or after this.
    pub expires_at: Instant,
    /// MAC over the canonical encoding of the other fields.
    pub signature: [u8; 32],
}

/// Mints + verifies Selos using a shared 32-byte secret.
///
/// The secret is wrapped in [`Risca`] so it can NEVER leak via
/// `Debug` / `Display` / `Serialize` — accidental observability
/// of the issuer key is a type error. Only `compute_mac` reaches
/// the inner bytes via the verbose `expose_secret()` call, marking
/// every exposure as a deliberate, grep-able code-review event.
pub struct SeloIssuer {
    secret: Risca<[u8; 32]>,
}

impl SeloIssuer {
    /// New issuer with the given secret. Operators generate this
    /// secret via a cofre primitive in production.
    #[must_use]
    pub const fn new(secret: [u8; 32]) -> Self {
        Self {
            secret: Risca::new(secret),
        }
    }

    /// Mint a new selo. Always succeeds — minting is local + cheap.
    #[must_use]
    pub fn issue(&self, subject: &str, capability: &str, expires_at: Instant) -> Selo {
        let signature = self.compute_mac(subject, capability, expires_at);
        Selo {
            subject: subject.to_string(),
            capability: capability.to_string(),
            expires_at,
            signature,
        }
    }

    /// Verify a selo. Checks (in order): signature, expiry, subject
    /// match, capability match. Returns first error encountered.
    ///
    /// # Errors
    /// - [`SeloError::InvalidSignature`] if MAC doesn't match
    /// - [`SeloError::Expired`] if `now >= selo.expires_at`
    /// - [`SeloError::SubjectMismatch`] if subjects differ
    /// - [`SeloError::CapabilityMismatch`] if capabilities differ
    #[allow(clippy::similar_names)]
    pub fn verify(
        &self,
        selo: &Selo,
        expected_subject: &str,
        expected_capability: &str,
        now: Instant,
    ) -> Result<(), SeloError> {
        // Constant-time MAC verification via BLAKE3.
        let expected = self.compute_mac(&selo.subject, &selo.capability, selo.expires_at);
        if !ct_eq_32(&expected, &selo.signature) {
            return Err(SeloError::InvalidSignature);
        }
        if !selo.expires_at.causally_after(&now) && selo.expires_at != now_strictly_after(&now) {
            // Equivalent to now >= expires_at; using Instant ordering.
            if now >= selo.expires_at {
                return Err(SeloError::Expired {
                    now_packed: now.to_packed(),
                    expires_packed: selo.expires_at.to_packed(),
                });
            }
        }
        if selo.subject != expected_subject {
            return Err(SeloError::SubjectMismatch {
                expected: expected_subject.to_string(),
                got: selo.subject.clone(),
            });
        }
        if selo.capability != expected_capability {
            return Err(SeloError::CapabilityMismatch {
                expected: expected_capability.to_string(),
                got: selo.capability.clone(),
            });
        }
        Ok(())
    }

    fn compute_mac(&self, subject: &str, capability: &str, expires_at: Instant) -> [u8; 32] {
        let mut h = blake3::Hasher::new_keyed(self.secret.expose_secret());
        h.update(b"selo/v1/");
        h.update((subject.len() as u64).to_le_bytes().as_slice());
        h.update(subject.as_bytes());
        h.update(b"|");
        h.update((capability.len() as u64).to_le_bytes().as_slice());
        h.update(capability.as_bytes());
        h.update(b"|");
        h.update(&expires_at.to_packed().to_le_bytes());
        *h.finalize().as_bytes()
    }
}

/// Constant-time 32-byte equality check (defense against timing leaks
/// on signature comparison).
fn ct_eq_32(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff: u8 = 0;
    for i in 0..32 {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

/// Strict-after helper for Instant comparison. Returns an Instant
/// that is the smallest one strictly greater than `t`.
const fn now_strictly_after(t: &Instant) -> Instant {
    Instant {
        physical_ms: t.physical_ms,
        logical: t.logical.wrapping_add(1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn issuer(secret_byte: u8) -> SeloIssuer {
        SeloIssuer::new([secret_byte; 32])
    }

    fn at(ms: u64) -> Instant {
        Instant::from_ms(ms)
    }

    #[test]
    fn issue_then_verify_succeeds() {
        let iss = issuer(1);
        let selo = iss.issue("alice", "read:foo", at(1000));
        let res = iss.verify(&selo, "alice", "read:foo", at(500));
        assert!(res.is_ok());
    }

    #[test]
    fn verify_after_expiry_errors() {
        let iss = issuer(1);
        let selo = iss.issue("alice", "read:foo", at(1000));
        let err = iss
            .verify(&selo, "alice", "read:foo", at(1500))
            .unwrap_err();
        assert_eq!(err.kind(), "expired");
    }

    #[test]
    fn verify_at_exact_expiry_errors() {
        let iss = issuer(1);
        let selo = iss.issue("alice", "read:foo", at(1000));
        let err = iss
            .verify(&selo, "alice", "read:foo", at(1000))
            .unwrap_err();
        assert_eq!(err.kind(), "expired");
    }

    #[test]
    fn verify_with_wrong_subject_errors() {
        let iss = issuer(1);
        let selo = iss.issue("alice", "read:foo", at(1000));
        let err = iss.verify(&selo, "bob", "read:foo", at(500)).unwrap_err();
        assert_eq!(err.kind(), "subject_mismatch");
    }

    #[test]
    fn verify_with_wrong_capability_errors() {
        let iss = issuer(1);
        let selo = iss.issue("alice", "read:foo", at(1000));
        let err = iss
            .verify(&selo, "alice", "write:foo", at(500))
            .unwrap_err();
        assert_eq!(err.kind(), "capability_mismatch");
    }

    #[test]
    fn verify_with_wrong_secret_errors_invalid_signature() {
        let iss1 = issuer(1);
        let iss2 = issuer(2);
        let selo = iss1.issue("alice", "read:foo", at(1000));
        let err = iss2
            .verify(&selo, "alice", "read:foo", at(500))
            .unwrap_err();
        assert_eq!(err.kind(), "invalid_signature");
    }

    #[test]
    fn tampered_subject_invalidates_signature() {
        let iss = issuer(1);
        let mut selo = iss.issue("alice", "read:foo", at(1000));
        selo.subject = "mallory".to_string();
        let err = iss
            .verify(&selo, "mallory", "read:foo", at(500))
            .unwrap_err();
        assert_eq!(err.kind(), "invalid_signature");
    }

    #[test]
    fn tampered_capability_invalidates_signature() {
        let iss = issuer(1);
        let mut selo = iss.issue("alice", "read:foo", at(1000));
        selo.capability = "write:foo".to_string();
        let err = iss
            .verify(&selo, "alice", "write:foo", at(500))
            .unwrap_err();
        assert_eq!(err.kind(), "invalid_signature");
    }

    #[test]
    fn tampered_expiry_invalidates_signature() {
        let iss = issuer(1);
        let mut selo = iss.issue("alice", "read:foo", at(1000));
        selo.expires_at = at(99_999_999);
        let err = iss.verify(&selo, "alice", "read:foo", at(500)).unwrap_err();
        assert_eq!(err.kind(), "invalid_signature");
    }

    #[test]
    fn selo_serde_round_trips() {
        let iss = issuer(7);
        let selo = iss.issue("alice", "read:foo", at(1000));
        let json = serde_json::to_string(&selo).unwrap();
        let back: Selo = serde_json::from_str(&json).unwrap();
        assert_eq!(back, selo);
        assert!(iss.verify(&back, "alice", "read:foo", at(500)).is_ok());
    }

    #[test]
    fn issue_is_deterministic() {
        let iss = issuer(1);
        let s1 = iss.issue("alice", "read:foo", at(1000));
        let s2 = iss.issue("alice", "read:foo", at(1000));
        assert_eq!(s1.signature, s2.signature);
    }

    #[test]
    fn different_subjects_get_different_signatures() {
        let iss = issuer(1);
        let s1 = iss.issue("alice", "read:foo", at(1000));
        let s2 = iss.issue("bob", "read:foo", at(1000));
        assert_ne!(s1.signature, s2.signature);
    }

    #[test]
    fn ct_eq_32_works() {
        let a = [42u8; 32];
        let b = [42u8; 32];
        let mut c = [42u8; 32];
        c[31] = 41;
        assert!(ct_eq_32(&a, &b));
        assert!(!ct_eq_32(&a, &c));
    }

    #[test]
    fn error_kinds_stable() {
        assert_eq!(
            SeloError::Expired {
                now_packed: 0,
                expires_packed: 0
            }
            .kind(),
            "expired"
        );
        assert_eq!(SeloError::InvalidSignature.kind(), "invalid_signature");
        assert_eq!(
            SeloError::SubjectMismatch {
                expected: "a".into(),
                got: "b".into()
            }
            .kind(),
            "subject_mismatch"
        );
        assert_eq!(
            SeloError::CapabilityMismatch {
                expected: "x".into(),
                got: "y".into()
            }
            .kind(),
            "capability_mismatch"
        );
    }
}
