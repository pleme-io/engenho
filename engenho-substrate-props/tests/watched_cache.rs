//! Property: WatchedCache event emission + subscriber semantics.

use engenho_substrate::{
    CacheEvent, DerivationCacheBackend, DrvHash, MemoryDerivationCache, NarHash, Realisation,
    WatchedCache,
};
use engenho_substrate_props::helpers::{sample_drv as drv, sample_nar as nar_blob};
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;
use std::sync::Arc;

proptest_with_env! {
    /// Every put_drv emits one CacheEvent::DrvPut to active subscribers.
    #[test]
    fn put_drv_emits_event(hashes in proptest::collection::vec(any::<u8>(), 1..6)) {
        engenho_substrate_props::block_on(async {
            let inner: Arc<dyn DerivationCacheBackend> = Arc::new(MemoryDerivationCache::new());
            let cache = WatchedCache::new(inner);
            let mut rx = cache.subscribe(64);
            for h in &hashes {
                cache.put_drv(&drv(*h)).await.unwrap();
            }
            let mut count = 0;
            while let Ok(ev) = rx.try_recv() {
                if matches!(ev, CacheEvent::DrvUpserted(_)) {
                    count += 1;
                }
            }
            // Distinct hashes produce distinct put events.
            let unique = hashes.iter().copied().collect::<std::collections::BTreeSet<_>>().len();
            assert!(count >= unique, "fewer DrvPut events ({count}) than distinct drvs ({unique})");
    });
    }

    /// Every put_nar emits a CacheEvent::NarPut.
    #[test]
    fn put_nar_emits_event(payload_byte in any::<u8>()) {
        engenho_substrate_props::block_on(async {
            let inner: Arc<dyn DerivationCacheBackend> = Arc::new(MemoryDerivationCache::new());
            let cache = WatchedCache::new(inner);
            let mut rx = cache.subscribe(64);
            cache.put_nar(&nar_blob(payload_byte)).await.unwrap();
            let ev = rx.try_recv().unwrap();
            assert!(matches!(ev, CacheEvent::NarUpserted(_)));
    });
    }

    /// subscriber_count tracks subscribe/drop cycle.
    #[test]
    fn subscriber_count_tracks_lifecycle(n in 0usize..8) {
        engenho_substrate_props::block_on(async {
            let inner: Arc<dyn DerivationCacheBackend> = Arc::new(MemoryDerivationCache::new());
            let cache = WatchedCache::new(inner);
            let baseline = cache.subscriber_count();
            let receivers: Vec<_> = (0..n).map(|_| cache.subscribe(16)).collect();
            assert_eq!(cache.subscriber_count(), baseline + n);
            drop(receivers);
            assert_eq!(cache.subscriber_count(), baseline);
    });
    }

    /// put_drv with no subscribers doesn't error (broadcast best-effort).
    #[test]
    fn put_drv_no_subscribers_ok(hash in any::<u8>()) {
        engenho_substrate_props::block_on(async {
            let inner: Arc<dyn DerivationCacheBackend> = Arc::new(MemoryDerivationCache::new());
            let cache = WatchedCache::new(inner);
            // No subscribe() call.
            let res = cache.put_drv(&drv(hash)).await;
            assert!(res.is_ok());
    });
    }

    /// get_drv delegates to inner — round-trip preserves identity.
    #[test]
    fn get_drv_delegates_to_inner(hash in any::<u8>()) {
        engenho_substrate_props::block_on(async {
            let inner: Arc<dyn DerivationCacheBackend> = Arc::new(MemoryDerivationCache::new());
            let cache = WatchedCache::new(inner);
            let d = drv(hash);
            cache.put_drv(&d).await.unwrap();
            let got = cache.get_drv(&DrvHash::new([hash; 32])).await.unwrap();
            assert_eq!(got, Some(d));
    });
    }

    /// put_realisation emits an event.
    #[test]
    fn put_realisation_emits_event(hash in any::<u8>(), output_byte in any::<u8>()) {
        engenho_substrate_props::block_on(async {
            let inner: Arc<dyn DerivationCacheBackend> = Arc::new(MemoryDerivationCache::new());
            let cache = WatchedCache::new(inner);
            let mut rx = cache.subscribe(8);
            let r = Realisation {
                drv_hash: DrvHash::new([hash; 32]),
                output_name: "out".into(),
                output_path: engenho_substrate::OutputPath::new("/nix/store/x"),
                nar_hash: Some(NarHash::new([output_byte; 32])),
            };
            cache.put_realisation(&r).await.unwrap();
            let ev = rx.try_recv().unwrap();
            assert!(matches!(ev, CacheEvent::RealisationUpserted { .. }));
    });
    }
}
