//! Property: SearchRng determinism + stream isolation.
//!
//! The substrate's pesquisa layer depends on SearchRng being
//! deterministic given (seed, stream_tag) — this is the
//! replay-verifiability anchor for every search.

use engenho_substrate::SearchRng;
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;

proptest_with_env! {
    /// Same (seed, stream_tag) → identical N-element u64 output.
    #[test]
    fn same_seed_same_stream_identical_output(
        seed_bytes in any::<[u8; 32]>(),
        stream in proptest::collection::vec(any::<u8>(), 0..64),
        n_calls in 1usize..64,
    ) {
        let mut r1 = SearchRng::new(&seed_bytes, &stream);
        let mut r2 = SearchRng::new(&seed_bytes, &stream);
        for _ in 0..n_calls {
            prop_assert_eq!(r1.next_u64(), r2.next_u64());
        }
    }

    /// Different streams → diverge within K samples (statistical).
    #[test]
    fn different_streams_diverge_within_k_samples(
        seed_bytes in any::<[u8; 32]>(),
        s1 in proptest::collection::vec(any::<u8>(), 1..32),
        s2 in proptest::collection::vec(any::<u8>(), 1..32),
    ) {
        prop_assume!(s1 != s2);
        let mut r1 = SearchRng::new(&seed_bytes, &s1);
        let mut r2 = SearchRng::new(&seed_bytes, &s2);
        // Within 16 samples, at least one must differ
        // (probability of all 16 colliding is ~2^-1024).
        let mut diverged = false;
        for _ in 0..16 {
            if r1.next_u64() != r2.next_u64() {
                diverged = true;
                break;
            }
        }
        prop_assert!(diverged);
    }

    /// Different seeds → diverge within K samples.
    #[test]
    fn different_seeds_diverge_within_k_samples(
        s1 in any::<[u8; 32]>(),
        s2 in any::<[u8; 32]>(),
        stream in proptest::collection::vec(any::<u8>(), 0..16),
    ) {
        prop_assume!(s1 != s2);
        let mut r1 = SearchRng::new(&s1, &stream);
        let mut r2 = SearchRng::new(&s2, &stream);
        let mut diverged = false;
        for _ in 0..16 {
            if r1.next_u64() != r2.next_u64() {
                diverged = true;
                break;
            }
        }
        prop_assert!(diverged);
    }

    /// next_below(max) is always < max for max > 0.
    #[test]
    fn next_below_is_strictly_less_than_bound(
        seed_bytes in any::<[u8; 32]>(),
        stream in proptest::collection::vec(any::<u8>(), 0..32),
        max in 1u64..1_000_000,
        n_calls in 1usize..32,
    ) {
        let mut r = SearchRng::new(&seed_bytes, &stream);
        for _ in 0..n_calls {
            let v = r.next_below(max);
            prop_assert!(v < max);
        }
    }

    /// next_below(0) always returns 0.
    #[test]
    fn next_below_zero_returns_zero(
        seed_bytes in any::<[u8; 32]>(),
        stream in proptest::collection::vec(any::<u8>(), 0..32),
        n_calls in 1usize..16,
    ) {
        let mut r = SearchRng::new(&seed_bytes, &stream);
        for _ in 0..n_calls {
            prop_assert_eq!(r.next_below(0), 0);
        }
    }
}
