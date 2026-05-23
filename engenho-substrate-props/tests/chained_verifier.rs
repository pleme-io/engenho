//! Property: ChainedVerifier first-failure-denies + last-success-returns.

use engenho_substrate::{
    ChainedVerifier, FakeVerifier, NarHash, Verificacao, Verifier, VerifyError,
};
use engenho_substrate_props::helpers::sample_emitter as emitter;
use engenho_substrate_props::{block_on, proptest_with_env};
use proptest::prelude::*;
use std::sync::Arc;

fn sample_verificacao() -> Verificacao {
    Verificacao::HashEquality {
        expected: NarHash::new([1u8; 32]),
    }
}

proptest_with_env! {
    /// Empty chain always returns Backend error.
    #[test]
    fn empty_chain_errors_backend(emitter_b in any::<u8>(), emitted_at in 0u64..1_000_000) {
        block_on(async {
            let chain = ChainedVerifier::default_named(vec![]);
            let err = chain
                .verify(&sample_verificacao(), [0u8; 32], emitter(emitter_b), emitted_at)
                .await
                .unwrap_err();
            assert!(matches!(err, VerifyError::Backend(_)));
        });
    }

    /// All-pass chain returns Ok.
    #[test]
    fn all_pass_chain_returns_ok(n in 1usize..6, emitter_b in any::<u8>()) {
        block_on(async {
            let verifiers: Vec<Arc<dyn Verifier>> = (0..n)
                .map(|_| Arc::new(FakeVerifier::new()) as Arc<dyn Verifier>)
                .collect();
            let chain = ChainedVerifier::default_named(verifiers);
            let res = chain
                .verify(&sample_verificacao(), [0u8; 32], emitter(emitter_b), 0)
                .await;
            assert!(res.is_ok());
        });
    }

    /// chain.len() matches verifier count.
    #[test]
    fn len_matches_verifier_count(n in 0usize..8) {
        let verifiers: Vec<Arc<dyn Verifier>> = (0..n)
            .map(|_| Arc::new(FakeVerifier::new()) as Arc<dyn Verifier>)
            .collect();
        let chain = ChainedVerifier::default_named(verifiers);
        assert_eq!(chain.len(), n);
        assert_eq!(chain.is_empty(), n == 0);
    }

    /// First failure in the chain denies — Failed error.
    #[test]
    fn first_failure_denies(
        before_pass in 0usize..4,
        emitter_b in any::<u8>(),
    ) {
        block_on(async {
            let mut verifiers: Vec<Arc<dyn Verifier>> = Vec::new();
            for _ in 0..before_pass {
                verifiers.push(Arc::new(FakeVerifier::new()));
            }
            // The N+1-th verifier fails.
            let failing = Arc::new(FakeVerifier::new());
            failing
                .fail_next(VerifyError::Failed("simulated".into()))
                .await;
            verifiers.push(failing);
            // Append one more that would pass — proves it's never reached.
            verifiers.push(Arc::new(FakeVerifier::new()));
            let chain = ChainedVerifier::default_named(verifiers);
            let err = chain
                .verify(&sample_verificacao(), [0u8; 32], emitter(emitter_b), 0)
                .await
                .unwrap_err();
            // Wrapped as Failed("{name}: {inner}").
            assert!(matches!(err, VerifyError::Failed(_)));
        });
    }

    /// Name is stable across chain construction.
    #[test]
    fn name_is_stable(_seed in any::<u8>()) {
        let chain = ChainedVerifier::new("custom-name", vec![]);
        assert_eq!(chain.name(), "custom-name");
        let default_chain = ChainedVerifier::default_named(vec![]);
        assert_eq!(default_chain.name(), "chained");
    }
}
