//! Property: verifier_impls invariants (HashEqualityVerifier + SmokeTestVerifier).

use engenho_substrate::{
    BytesAccessor, HashEqualityVerifier, NarHash, NodeId, SmokeBuilder, SmokeTestVerifier,
    Verificacao, Verifier, VerifyError,
};
use engenho_substrate_props::{block_on, proptest_with_env};
use proptest::prelude::*;
use std::sync::Arc;

fn emitter(b: u8) -> NodeId {
    NodeId::from_bytes(&[b; 32])
}

proptest_with_env! {
    /// HashEqualityVerifier: returns Failed when accessor returns no bytes.
    #[test]
    fn hash_equality_no_bytes_fails(subject in any::<[u8; 32]>(), emitter_b in any::<u8>()) {
        block_on(async {
            let accessor: BytesAccessor = Arc::new(|_| Box::pin(async { Ok(None) }));
            let v = HashEqualityVerifier::default_named(accessor);
            let verificacao = Verificacao::HashEquality {
                expected: NarHash::new([0u8; 32]),
            };
            let err = v
                .verify(&verificacao, subject, emitter(emitter_b), 0)
                .await
                .unwrap_err();
            assert!(matches!(err, VerifyError::Failed(_)));
        });
    }

    /// HashEqualityVerifier: matching expected → Ok.
    #[test]
    fn hash_equality_match_succeeds(
        bytes in proptest::collection::vec(any::<u8>(), 0..128),
        subject in any::<[u8; 32]>(),
        emitter_b in any::<u8>(),
    ) {
        block_on(async {
            let want = NarHash::from_bytes(&bytes);
            let bytes_clone = bytes.clone();
            let accessor: BytesAccessor = Arc::new(move |_| {
                let bytes = bytes_clone.clone();
                Box::pin(async move { Ok(Some(bytes)) })
            });
            let v = HashEqualityVerifier::default_named(accessor);
            let verificacao = Verificacao::HashEquality { expected: want };
            let receipt = v
                .verify(&verificacao, subject, emitter(emitter_b), 0)
                .await
                .unwrap();
            assert_eq!(receipt.verifier, "hash-equality");
        });
    }

    /// HashEqualityVerifier: hash mismatch → Failed.
    #[test]
    fn hash_equality_mismatch_fails(
        bytes in proptest::collection::vec(any::<u8>(), 1..32),
        wrong_expected in any::<[u8; 32]>(),
        subject in any::<[u8; 32]>(),
    ) {
        let actual = NarHash::from_bytes(&bytes);
        prop_assume!(actual != NarHash::new(wrong_expected));
        block_on(async {
            let bytes_clone = bytes.clone();
            let accessor: BytesAccessor = Arc::new(move |_| {
                let bytes = bytes_clone.clone();
                Box::pin(async move { Ok(Some(bytes)) })
            });
            let v = HashEqualityVerifier::default_named(accessor);
            let verificacao = Verificacao::HashEquality {
                expected: NarHash::new(wrong_expected),
            };
            let err = v
                .verify(&verificacao, subject, emitter(0), 0)
                .await
                .unwrap_err();
            assert!(matches!(err, VerifyError::Failed(_)));
        });
    }

    /// HashEqualityVerifier: rejects non-HashEquality variants.
    #[test]
    fn hash_equality_rejects_non_hash_variants(subject in any::<[u8; 32]>()) {
        block_on(async {
            let accessor: BytesAccessor = Arc::new(|_| Box::pin(async { Ok(None) }));
            let v = HashEqualityVerifier::default_named(accessor);
            let verificacao = Verificacao::CrossNodeAgreement { quorum: 2 };
            let err = v
                .verify(&verificacao, subject, emitter(0), 0)
                .await
                .unwrap_err();
            assert!(matches!(err, VerifyError::Unsupported(_)));
        });
    }

    /// HashEqualityVerifier: accessor error propagates.
    #[test]
    fn hash_equality_accessor_error_propagates(subject in any::<[u8; 32]>()) {
        block_on(async {
            let accessor: BytesAccessor = Arc::new(|_| {
                Box::pin(async { Err(VerifyError::Backend("accessor io".into())) })
            });
            let v = HashEqualityVerifier::default_named(accessor);
            let verificacao = Verificacao::HashEquality {
                expected: NarHash::new([0u8; 32]),
            };
            let err = v
                .verify(&verificacao, subject, emitter(0), 0)
                .await
                .unwrap_err();
            assert!(matches!(err, VerifyError::Backend(_)));
        });
    }

    /// SmokeTestVerifier: build success → Ok.
    #[test]
    fn smoke_test_passes_on_build_success(subject in any::<[u8; 32]>()) {
        block_on(async {
            let builder: SmokeBuilder = Arc::new(|_| Box::pin(async { Ok(()) }));
            let v = SmokeTestVerifier::default_named(builder);
            let verificacao = Verificacao::SmokeTest {
                drv_hash_hex: "abc123".into(),
            };
            let res = v.verify(&verificacao, subject, emitter(0), 0).await;
            assert!(res.is_ok());
        });
    }

    /// SmokeTestVerifier: build failure → Failed.
    #[test]
    fn smoke_test_fails_on_build_failure(subject in any::<[u8; 32]>()) {
        block_on(async {
            let builder: SmokeBuilder = Arc::new(|_| {
                Box::pin(async { Err(VerifyError::Failed("build broke".into())) })
            });
            let v = SmokeTestVerifier::default_named(builder);
            let verificacao = Verificacao::SmokeTest {
                drv_hash_hex: "abc123".into(),
            };
            let err = v
                .verify(&verificacao, subject, emitter(0), 0)
                .await
                .unwrap_err();
            assert!(matches!(err, VerifyError::Failed(_)));
        });
    }

    /// SmokeTestVerifier: rejects non-SmokeTest variants.
    #[test]
    fn smoke_test_rejects_non_smoke_variants(subject in any::<[u8; 32]>()) {
        block_on(async {
            let builder: SmokeBuilder = Arc::new(|_| Box::pin(async { Ok(()) }));
            let v = SmokeTestVerifier::default_named(builder);
            let verificacao = Verificacao::HashEquality {
                expected: NarHash::new([0u8; 32]),
            };
            let err = v
                .verify(&verificacao, subject, emitter(0), 0)
                .await
                .unwrap_err();
            assert!(matches!(err, VerifyError::Unsupported(_)));
        });
    }
}
