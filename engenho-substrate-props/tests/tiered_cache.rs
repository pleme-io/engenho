//! Property: TieredCache walks tiers in order + promotes on hit.

use engenho_substrate::{
    DerivationCacheBackend, DrvHash, MemoryDerivationCache, PromotionPolicy, TieredCache,
};
use engenho_substrate_props::helpers::{sample_drv as drv, sample_nar as nar};
use engenho_substrate_props::{block_on, proptest_with_env};
use proptest::prelude::*;
use std::sync::Arc;

fn tier_chain(
    n: usize,
) -> (
    Vec<Arc<MemoryDerivationCache>>,
    Vec<Arc<dyn DerivationCacheBackend>>,
) {
    let concretes: Vec<Arc<MemoryDerivationCache>> = (0..n)
        .map(|_| Arc::new(MemoryDerivationCache::new()))
        .collect();
    let dyns: Vec<Arc<dyn DerivationCacheBackend>> = concretes
        .iter()
        .map(|t| t.clone() as Arc<dyn DerivationCacheBackend>)
        .collect();
    (concretes, dyns)
}

proptest_with_env! {
    /// Empty tier list: every get returns None; every put silently succeeds.
    #[test]
    fn empty_tier_list_is_no_op(hash_b in any::<u8>()) {
        block_on(async {
            let cache = TieredCache::new(vec![]);
            let got = cache.get_drv(&DrvHash::new([hash_b; 32])).await.unwrap();
            assert!(got.is_none());
            // put_drv also succeeds (no tiers to write to).
            cache.put_drv(&drv(hash_b)).await.unwrap();
        });
    }

    /// Single-tier cache: get/put round-trip preserves identity.
    #[test]
    fn single_tier_round_trips(hash_b in any::<u8>()) {
        block_on(async {
            let inner = Arc::new(MemoryDerivationCache::new());
            let dyns: Vec<Arc<dyn DerivationCacheBackend>> = vec![inner.clone()];
            let cache = TieredCache::new(dyns);
            let d = drv(hash_b);
            cache.put_drv(&d).await.unwrap();
            let got = cache.get_drv(&DrvHash::new([hash_b; 32])).await.unwrap();
            assert_eq!(got, Some(d));
        });
    }

    /// Hit on a lower tier promotes the value to all higher tiers
    /// (Eager policy).
    #[test]
    fn hit_promotes_to_higher_tiers(
        hash_b in any::<u8>(),
        n in 2usize..5,
    ) {
        block_on(async {
            let (concretes, dyns) = tier_chain(n);
            let cache = TieredCache::with_promotion(dyns, PromotionPolicy::Eager);
            // Pre-seed only the LOWEST tier with the drv.
            let d = drv(hash_b);
            concretes[n - 1].put_drv(&d).await.unwrap();
            // Initially the higher tiers are empty.
            for c in &concretes[..n - 1] {
                let none = c.get_drv(&DrvHash::new([hash_b; 32])).await.unwrap();
                assert!(none.is_none());
            }
            // A get on the tiered cache hits the lowest tier + promotes.
            let got = cache.get_drv(&DrvHash::new([hash_b; 32])).await.unwrap();
            assert_eq!(got, Some(d.clone()));
            // Now every tier (including higher ones) has the value.
            for (i, c) in concretes.iter().enumerate() {
                let val = c.get_drv(&DrvHash::new([hash_b; 32])).await.unwrap();
                assert_eq!(val, Some(d.clone()), "tier {i} missing after promotion");
            }
        });
    }

    /// Higher tier hits without consulting lower tiers (efficiency).
    #[test]
    fn higher_tier_hit_short_circuits(hash_b in any::<u8>(), n in 2usize..4) {
        block_on(async {
            let (concretes, dyns) = tier_chain(n);
            let cache = TieredCache::new(dyns);
            // Pre-seed the HIGHEST tier (L0) only.
            let d = drv(hash_b);
            concretes[0].put_drv(&d).await.unwrap();
            let got = cache.get_drv(&DrvHash::new([hash_b; 32])).await.unwrap();
            assert_eq!(got, Some(d));
            // Lower tiers still have NOTHING (they were never written).
            for c in &concretes[1..] {
                let none = c.get_drv(&DrvHash::new([hash_b; 32])).await.unwrap();
                assert!(none.is_none());
            }
        });
    }

    /// put_drv writes ONLY to L0 (the fastest tier). Production
    /// deployments add a separate reconcile loop that fans out to
    /// cluster + federation tiers. Lower tiers are read-only from
    /// the cache's perspective at write time.
    #[test]
    fn put_drv_writes_only_to_l0(hash_b in any::<u8>(), n in 2usize..5) {
        block_on(async {
            let (concretes, dyns) = tier_chain(n);
            let cache = TieredCache::new(dyns);
            let d = drv(hash_b);
            cache.put_drv(&d).await.unwrap();
            // L0 has it.
            assert_eq!(
                concretes[0].get_drv(&DrvHash::new([hash_b; 32])).await.unwrap(),
                Some(d.clone())
            );
            // Lower tiers are untouched.
            for (i, c) in concretes.iter().enumerate().skip(1) {
                let val = c.get_drv(&DrvHash::new([hash_b; 32])).await.unwrap();
                assert!(
                    val.is_none(),
                    "tier {i} unexpectedly has drv after put — write-to-local was supposed to skip it"
                );
            }
        });
    }

    /// Miss across every tier returns None.
    #[test]
    fn complete_miss_returns_none(hash_b in any::<u8>(), n in 1usize..6) {
        block_on(async {
            let (_, dyns) = tier_chain(n);
            let cache = TieredCache::new(dyns);
            let got = cache.get_drv(&DrvHash::new([hash_b; 32])).await.unwrap();
            assert!(got.is_none());
        });
    }

    /// tier_count + tier_name accessors match the underlying Vec.
    #[test]
    fn tier_accessors_match_input(n in 0usize..8) {
        let (_, dyns) = tier_chain(n);
        let cache = TieredCache::new(dyns);
        assert_eq!(cache.tier_count(), n);
        for i in 0..n {
            assert_eq!(cache.tier_name(i), Some("memory"));
        }
        assert_eq!(cache.tier_name(n), None);
    }

    /// NarBlob round-trip through TieredCache (analogous to drv).
    #[test]
    fn nar_blob_round_trips_through_tiered(payload_b in any::<u8>(), n in 1usize..4) {
        block_on(async {
            let (_, dyns) = tier_chain(n);
            let cache = TieredCache::new(dyns);
            let blob = nar(payload_b);
            cache.put_nar(&blob).await.unwrap();
            let got = cache.get_nar(&blob.hash).await.unwrap();
            assert_eq!(got, Some(blob));
        });
    }
}
