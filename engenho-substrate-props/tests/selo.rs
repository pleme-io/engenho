//! Property: selo Selo + SeloIssuer invariants.

use engenho_substrate::{Instant, Selo, SeloError, SeloIssuer};
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;

fn at(ms: u64) -> Instant {
    Instant::from_ms(ms)
}

proptest_with_env! {
    /// Issue + verify with matching params + same issuer always succeeds
    /// (when not yet expired).
    #[test]
    fn issue_verify_round_trip_succeeds(
        secret in any::<[u8; 32]>(),
        subj in "[a-zA-Z0-9]{1,30}",
        cap in "[a-zA-Z0-9:_-]{1,30}",
        exp in 1000u64..1_000_000,
        verify_at in 0u64..999,
    ) {
        let iss = SeloIssuer::new(secret);
        let selo = iss.issue(&subj, &cap, at(exp));
        let res = iss.verify(&selo, &subj, &cap, at(verify_at));
        prop_assert!(res.is_ok(), "verify failed: {res:?}");
    }

    /// Different secrets always produce different signatures
    /// (BLAKE3 keyed-hash collision is astronomically unlikely).
    #[test]
    fn different_secrets_different_signatures(
        s1 in any::<[u8; 32]>(),
        s2 in any::<[u8; 32]>(),
        subj in "[a-zA-Z]{1,20}",
        cap in "[a-zA-Z]{1,20}",
        exp in 1000u64..1_000_000,
    ) {
        prop_assume!(s1 != s2);
        let iss1 = SeloIssuer::new(s1);
        let iss2 = SeloIssuer::new(s2);
        let selo1 = iss1.issue(&subj, &cap, at(exp));
        let selo2 = iss2.issue(&subj, &cap, at(exp));
        prop_assert_ne!(selo1.signature, selo2.signature);
    }

    /// Issuing the same selo twice produces byte-identical signatures.
    #[test]
    fn issue_is_deterministic(
        secret in any::<[u8; 32]>(),
        subj in "[a-zA-Z0-9]{1,30}",
        cap in "[a-zA-Z0-9:_-]{1,30}",
        exp in 0u64..1_000_000,
    ) {
        let iss = SeloIssuer::new(secret);
        let s1 = iss.issue(&subj, &cap, at(exp));
        let s2 = iss.issue(&subj, &cap, at(exp));
        prop_assert_eq!(s1.signature, s2.signature);
    }

    /// Verifying with the wrong issuer always fails with InvalidSignature.
    #[test]
    fn wrong_issuer_returns_invalid_signature(
        s1 in any::<[u8; 32]>(),
        s2 in any::<[u8; 32]>(),
        subj in "[a-zA-Z]{1,20}",
        cap in "[a-zA-Z]{1,20}",
    ) {
        prop_assume!(s1 != s2);
        let iss1 = SeloIssuer::new(s1);
        let iss2 = SeloIssuer::new(s2);
        let selo = iss1.issue(&subj, &cap, at(1000));
        let err = iss2.verify(&selo, &subj, &cap, at(0)).unwrap_err();
        prop_assert_eq!(err, SeloError::InvalidSignature);
    }

    /// Verifying past expiry always errors with Expired.
    #[test]
    fn past_expiry_errors_expired(
        secret in any::<[u8; 32]>(),
        subj in "[a-zA-Z]{1,20}",
        cap in "[a-zA-Z]{1,20}",
        exp in 100u64..10_000,
        excess in 0u64..10_000,
    ) {
        let iss = SeloIssuer::new(secret);
        let selo = iss.issue(&subj, &cap, at(exp));
        match iss.verify(&selo, &subj, &cap, at(exp + excess)) {
            Err(SeloError::Expired { .. }) => {}
            other => prop_assert!(false, "expected Expired, got {other:?}"),
        }
    }

    /// Tampering with any field of a selo invalidates its signature.
    #[test]
    fn tampered_field_invalidates_signature(
        secret in any::<[u8; 32]>(),
        subj1 in "[a-zA-Z]{1,20}",
        subj2 in "[a-zA-Z]{1,20}",
        cap in "[a-zA-Z]{1,20}",
        exp in 1000u64..10_000,
    ) {
        prop_assume!(subj1 != subj2);
        let iss = SeloIssuer::new(secret);
        let mut selo = iss.issue(&subj1, &cap, at(exp));
        selo.subject = subj2.clone();
        match iss.verify(&selo, &subj2, &cap, at(0)) {
            Err(SeloError::InvalidSignature) => {}
            other => prop_assert!(false, "expected InvalidSignature, got {other:?}"),
        }
    }

    /// Selo serde round-trips byte-perfectly.
    #[test]
    fn selo_serde_round_trip_preserves_signature(
        secret in any::<[u8; 32]>(),
        subj in "[a-zA-Z]{1,20}",
        cap in "[a-zA-Z]{1,20}",
        exp in 1000u64..10_000,
    ) {
        let iss = SeloIssuer::new(secret);
        let selo: Selo = iss.issue(&subj, &cap, at(exp));
        let json = serde_json::to_string(&selo).unwrap();
        let back: Selo = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(back.signature, selo.signature);
        prop_assert_eq!(&back.subject, &selo.subject);
        prop_assert_eq!(&back.capability, &selo.capability);
        prop_assert_eq!(back.expires_at, selo.expires_at);
    }
}
