//! Property: Linhagem fingerprint determinism + chain divergence.

use engenho_substrate::{GeracaoId, Linhagem, SearchId};
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;

fn gen_id_strategy() -> impl Strategy<Value = GeracaoId> {
    any::<[u8; 32]>().prop_map(GeracaoId)
}

proptest_with_env! {
    /// Same search_id + same generation chain → identical fingerprint.
    #[test]
    fn fingerprint_deterministic(
        search in "[a-z]{1,16}",
        gens in proptest::collection::vec(gen_id_strategy(), 0..16),
    ) {
        let mut l1 = Linhagem::new(SearchId::new(&search));
        let mut l2 = Linhagem::new(SearchId::new(&search));
        for g in &gens {
            l1.extend(*g);
            l2.extend(*g);
        }
        prop_assert_eq!(l1.fingerprint(), l2.fingerprint());
    }

    /// Different search_id → different fingerprint.
    #[test]
    fn fingerprint_diverges_per_search(
        s1 in "[a-z]{1,16}",
        s2 in "[a-z]{1,16}",
        gens in proptest::collection::vec(gen_id_strategy(), 0..16),
    ) {
        prop_assume!(s1 != s2);
        let mut l1 = Linhagem::new(SearchId::new(s1));
        let mut l2 = Linhagem::new(SearchId::new(s2));
        for g in &gens {
            l1.extend(*g);
            l2.extend(*g);
        }
        prop_assert_ne!(l1.fingerprint(), l2.fingerprint());
    }

    /// Different chain → different fingerprint.
    #[test]
    fn fingerprint_diverges_per_chain(
        search in "[a-z]{1,16}",
        g1 in proptest::collection::vec(gen_id_strategy(), 1..16),
        g2 in proptest::collection::vec(gen_id_strategy(), 1..16),
    ) {
        prop_assume!(g1 != g2);
        let mut l1 = Linhagem::new(SearchId::new(&search));
        let mut l2 = Linhagem::new(SearchId::new(&search));
        for g in &g1 {
            l1.extend(*g);
        }
        for g in &g2 {
            l2.extend(*g);
        }
        prop_assert_ne!(l1.fingerprint(), l2.fingerprint());
    }

    /// head() returns the last-appended generation.
    #[test]
    fn head_returns_last_extended(
        search in "[a-z]{1,16}",
        gens in proptest::collection::vec(gen_id_strategy(), 1..16),
    ) {
        let mut l = Linhagem::new(SearchId::new(search));
        for g in &gens {
            l.extend(*g);
        }
        prop_assert_eq!(l.head().copied(), gens.last().copied());
    }

    /// len() matches extension count.
    #[test]
    fn len_matches_extensions(
        search in "[a-z]{1,16}",
        gens in proptest::collection::vec(gen_id_strategy(), 0..32),
    ) {
        let mut l = Linhagem::new(SearchId::new(search));
        for g in &gens {
            l.extend(*g);
        }
        prop_assert_eq!(l.len(), gens.len());
        prop_assert_eq!(l.is_empty(), gens.is_empty());
    }
}
