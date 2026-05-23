//! Property: EnsaioId::derive is deterministic + diverges
//! across every input field.

use engenho_substrate::{EnsaioId, SearchId};
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;

proptest_with_env! {
    /// Same inputs → byte-identical EnsaioId.
    #[test]
    fn derive_is_deterministic(
        search in "[a-z]{1,16}",
        gen_idx in 0u64..1_000_000,
        genotype in proptest::collection::vec(any::<u8>(), 0..256),
        lineage_root in any::<[u8; 32]>(),
    ) {
        let sid = SearchId::new(search);
        let id1 = EnsaioId::derive(&sid, gen_idx, &genotype, &lineage_root);
        let id2 = EnsaioId::derive(&sid, gen_idx, &genotype, &lineage_root);
        prop_assert_eq!(id1, id2);
    }

    /// Different search_id → different EnsaioId (overwhelmingly).
    #[test]
    fn diverges_per_search(
        s1 in "[a-z]{1,16}",
        s2 in "[a-z]{1,16}",
        gen_idx in 0u64..1_000_000,
        genotype in proptest::collection::vec(any::<u8>(), 0..256),
        lineage_root in any::<[u8; 32]>(),
    ) {
        prop_assume!(s1 != s2);
        let id1 = EnsaioId::derive(&SearchId::new(s1), gen_idx, &genotype, &lineage_root);
        let id2 = EnsaioId::derive(&SearchId::new(s2), gen_idx, &genotype, &lineage_root);
        prop_assert_ne!(id1, id2);
    }

    /// Different generation_idx → different EnsaioId.
    #[test]
    fn diverges_per_generation(
        search in "[a-z]{1,16}",
        g1 in 0u64..1_000_000,
        g2 in 0u64..1_000_000,
        genotype in proptest::collection::vec(any::<u8>(), 0..256),
        lineage_root in any::<[u8; 32]>(),
    ) {
        prop_assume!(g1 != g2);
        let sid = SearchId::new(search);
        let id1 = EnsaioId::derive(&sid, g1, &genotype, &lineage_root);
        let id2 = EnsaioId::derive(&sid, g2, &genotype, &lineage_root);
        prop_assert_ne!(id1, id2);
    }

    /// Different genotype bytes → different EnsaioId.
    #[test]
    fn diverges_per_genotype(
        search in "[a-z]{1,16}",
        gen_idx in 0u64..1_000_000,
        g1 in proptest::collection::vec(any::<u8>(), 1..256),
        g2 in proptest::collection::vec(any::<u8>(), 1..256),
        lineage_root in any::<[u8; 32]>(),
    ) {
        prop_assume!(g1 != g2);
        let sid = SearchId::new(search);
        let id1 = EnsaioId::derive(&sid, gen_idx, &g1, &lineage_root);
        let id2 = EnsaioId::derive(&sid, gen_idx, &g2, &lineage_root);
        prop_assert_ne!(id1, id2);
    }

    /// Different lineage_root → different EnsaioId.
    #[test]
    fn diverges_per_lineage_root(
        search in "[a-z]{1,16}",
        gen_idx in 0u64..1_000_000,
        genotype in proptest::collection::vec(any::<u8>(), 0..256),
        r1 in any::<[u8; 32]>(),
        r2 in any::<[u8; 32]>(),
    ) {
        prop_assume!(r1 != r2);
        let sid = SearchId::new(search);
        let id1 = EnsaioId::derive(&sid, gen_idx, &genotype, &r1);
        let id2 = EnsaioId::derive(&sid, gen_idx, &genotype, &r2);
        prop_assert_ne!(id1, id2);
    }

    /// Hex representation is always 64 lowercase hex characters.
    #[test]
    fn hex_is_64_lowercase_hex(
        search in "[a-z]{1,16}",
        gen_idx in 0u64..1_000_000,
        genotype in proptest::collection::vec(any::<u8>(), 0..256),
        lineage_root in any::<[u8; 32]>(),
    ) {
        let id = EnsaioId::derive(&SearchId::new(search), gen_idx, &genotype, &lineage_root);
        let hex = id.to_hex();
        prop_assert_eq!(hex.len(), 64);
        for c in hex.chars() {
            prop_assert!(c.is_ascii_hexdigit() && (c.is_ascii_digit() || c.is_ascii_lowercase()));
        }
    }
}
