//! Property: verifier_impls invariants (HashEqualityVerifier + SmokeTestVerifier).

use engenho_substrate::{
    BytesAccessor, HashEqualityVerifier, IndependentRebuild, IndependentVerifier, NarHash,
    SignerCheck, SmokeBuilder, SmokeTestVerifier, TameshiVerifier, Verificacao, Verifier,
    VerifyError,
};
use engenho_substrate_props::helpers::sample_emitter as emitter;
use engenho_substrate_props::{block_on, proptest_with_env};
use proptest::prelude::*;
use std::sync::Arc;

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

    /// IndependentVerifier: rebuild Ok → Ok, receipt carries the
    /// verifier name. Evidence hash determinism is covered by the
    /// faithful-nodes-converge property below.
    #[test]
    fn independent_rebuild_ok_succeeds(subject in any::<[u8; 32]>(), b in any::<u8>()) {
        block_on(async {
            let payload = vec![b; 16];
            let rebuild: IndependentRebuild = Arc::new(move |_| {
                let bytes = payload.clone();
                Box::pin(async move { Ok(bytes) })
            });
            let v = IndependentVerifier::default_named(rebuild);
            let verificacao = Verificacao::Independent {
                backend: "test-backend".into(),
            };
            let receipt = v.verify(&verificacao, subject, emitter(0), 0).await.unwrap();
            assert_eq!(receipt.verifier, "independent");
            assert_eq!(receipt.receipt.subject, subject);
        });
    }

    /// IndependentVerifier: rebuild error propagates.
    #[test]
    fn independent_rebuild_err_propagates(subject in any::<[u8; 32]>()) {
        block_on(async {
            let rebuild: IndependentRebuild =
                Arc::new(|_| Box::pin(async { Err(VerifyError::Backend("rebuild fail".into())) }));
            let v = IndependentVerifier::default_named(rebuild);
            let verificacao = Verificacao::Independent {
                backend: "test-backend".into(),
            };
            let err = v.verify(&verificacao, subject, emitter(0), 0).await.unwrap_err();
            assert!(matches!(err, VerifyError::Backend(_)));
        });
    }

    /// IndependentVerifier: rejects non-Independent variants.
    #[test]
    fn independent_rejects_non_independent_variants(subject in any::<[u8; 32]>()) {
        block_on(async {
            let rebuild: IndependentRebuild = Arc::new(|_| Box::pin(async { Ok(vec![]) }));
            let v = IndependentVerifier::default_named(rebuild);
            let verificacao = Verificacao::HashEquality {
                expected: NarHash::new([0u8; 32]),
            };
            let err = v.verify(&verificacao, subject, emitter(0), 0).await.unwrap_err();
            assert!(matches!(err, VerifyError::Unsupported(_)));
        });
    }

    /// IndependentVerifier: faithful nodes converge on identical evidence
    /// (same rebuilt bytes → same BLAKE3 → same evidence). Catches
    /// dissent-detection bugs.
    #[test]
    fn independent_faithful_nodes_converge(
        subject in any::<[u8; 32]>(),
        b in any::<u8>(),
    ) {
        block_on(async {
            let payload = vec![b; 32];
            let p1 = payload.clone();
            let p2 = payload.clone();
            let rebuild_a: IndependentRebuild = Arc::new(move |_| {
                let bytes = p1.clone();
                Box::pin(async move { Ok(bytes) })
            });
            let rebuild_b: IndependentRebuild = Arc::new(move |_| {
                let bytes = p2.clone();
                Box::pin(async move { Ok(bytes) })
            });
            let va = IndependentVerifier::default_named(rebuild_a);
            let vb = IndependentVerifier::default_named(rebuild_b);
            let verificacao = Verificacao::Independent {
                backend: "backend-a".into(),
            };
            let ra = va.verify(&verificacao, subject, emitter(0), 0).await.unwrap();
            let rb = vb.verify(&verificacao, subject, emitter(0), 0).await.unwrap();
            // Faithful nodes producing the same rebuilt bytes get
            // identical evidence_hash (QuorumTracker convergence).
            assert_eq!(ra.receipt.evidence_hash, rb.receipt.evidence_hash);
        });
    }

    /// TameshiVerifier: check Ok → Ok, receipt carries verifier name
    /// + subject.
    #[test]
    fn tameshi_signer_ok_succeeds(subject in any::<[u8; 32]>(), b in any::<u8>()) {
        block_on(async {
            let sig = vec![b; 64];
            let check: SignerCheck = Arc::new(move |_, _| {
                let bytes = sig.clone();
                Box::pin(async move { Ok(bytes) })
            });
            let v = TameshiVerifier::default_named(check);
            let verificacao = Verificacao::TameshiSigned {
                signer: "did:web:tameshi.io".into(),
            };
            let receipt = v.verify(&verificacao, subject, emitter(0), 0).await.unwrap();
            assert_eq!(receipt.verifier, "tameshi");
            assert_eq!(receipt.receipt.subject, subject);
        });
    }

    /// TameshiVerifier: check error propagates.
    #[test]
    fn tameshi_signer_err_propagates(subject in any::<[u8; 32]>()) {
        block_on(async {
            let check: SignerCheck =
                Arc::new(|_, _| Box::pin(async { Err(VerifyError::Failed("nope".into())) }));
            let v = TameshiVerifier::default_named(check);
            let verificacao = Verificacao::TameshiSigned {
                signer: "did:web:tameshi.io".into(),
            };
            let err = v.verify(&verificacao, subject, emitter(0), 0).await.unwrap_err();
            assert!(matches!(err, VerifyError::Failed(_)));
        });
    }

    /// TameshiVerifier: rejects non-TameshiSigned variants.
    #[test]
    fn tameshi_rejects_non_tameshi_variants(subject in any::<[u8; 32]>()) {
        block_on(async {
            let check: SignerCheck = Arc::new(|_, _| Box::pin(async { Ok(vec![]) }));
            let v = TameshiVerifier::default_named(check);
            let verificacao = Verificacao::HashEquality {
                expected: NarHash::new([0u8; 32]),
            };
            let err = v.verify(&verificacao, subject, emitter(0), 0).await.unwrap_err();
            assert!(matches!(err, VerifyError::Unsupported(_)));
        });
    }

    /// TameshiVerifier: signer string passed verbatim to the check closure.
    #[test]
    fn tameshi_signer_string_passes_through(
        subject in any::<[u8; 32]>(),
        signer_name in "[a-z]{3,16}:[a-z0-9.]{3,32}",
    ) {
        let observed = Arc::new(std::sync::Mutex::new(String::new()));
        let observed_clone = observed.clone();
        block_on(async {
            let check: SignerCheck = Arc::new(move |signer, _| {
                let observed = observed_clone.clone();
                Box::pin(async move {
                    *observed.lock().unwrap() = signer;
                    Ok(vec![1u8; 8])
                })
            });
            let v = TameshiVerifier::default_named(check);
            let verificacao = Verificacao::TameshiSigned {
                signer: signer_name.clone(),
            };
            v.verify(&verificacao, subject, emitter(0), 0).await.unwrap();
        });
        assert_eq!(*observed.lock().unwrap(), signer_name);
    }
}
