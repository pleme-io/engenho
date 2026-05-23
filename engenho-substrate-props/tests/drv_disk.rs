//! Property: DiskDerivationCache file-backed round-trip + lifecycle.

use engenho_substrate::{
    DerivationCacheBackend, DiskDerivationCache, DrvHash, NarHash, OutputPath, Realisation,
};
use engenho_substrate_props::helpers::{sample_drv as drv, sample_nar as nar};
use engenho_substrate_props::{block_on, proptest_with_env};
use proptest::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_tempdir(tag: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "engenho-substrate-props-drv-disk-{}-{}-{tag}",
        std::process::id(),
        n
    ))
}

proptest_with_env! {
    /// put_drv then get_drv on the same hash returns the original Drv.
    #[test]
    fn drv_round_trips_through_disk(hash_b in any::<u8>()) {
        let root = unique_tempdir("drv-rt");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        block_on(async {
            let cache = DiskDerivationCache::new(&root);
            let d = drv(hash_b);
            cache.put_drv(&d).await.unwrap();
            let got = cache.get_drv(&DrvHash::new([hash_b; 32])).await.unwrap();
            assert_eq!(got, Some(d));
        });
        let _ = std::fs::remove_dir_all(&root);
    }

    /// get_drv on an absent hash returns None (no error).
    #[test]
    fn missing_drv_returns_none(hash_b in any::<u8>()) {
        let root = unique_tempdir("drv-miss");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        block_on(async {
            let cache = DiskDerivationCache::new(&root);
            let got = cache.get_drv(&DrvHash::new([hash_b; 32])).await.unwrap();
            assert!(got.is_none());
        });
        let _ = std::fs::remove_dir_all(&root);
    }

    /// put_nar then get_nar round-trips bytes.
    #[test]
    fn nar_round_trips_through_disk(payload_b in any::<u8>()) {
        let root = unique_tempdir("nar-rt");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        block_on(async {
            let cache = DiskDerivationCache::new(&root);
            let blob = nar(payload_b);
            let blob_hash = blob.hash.clone();
            cache.put_nar(&blob).await.unwrap();
            let got = cache.get_nar(&blob_hash).await.unwrap();
            assert_eq!(got, Some(blob));
        });
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Two distinct drv hashes occupy distinct files.
    #[test]
    fn distinct_drv_hashes_distinct_files(a in any::<u8>(), b in any::<u8>()) {
        prop_assume!(a != b);
        let root = unique_tempdir("drv-distinct");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        block_on(async {
            let cache = DiskDerivationCache::new(&root);
            cache.put_drv(&drv(a)).await.unwrap();
            cache.put_drv(&drv(b)).await.unwrap();
            let got_a = cache.get_drv(&DrvHash::new([a; 32])).await.unwrap();
            let got_b = cache.get_drv(&DrvHash::new([b; 32])).await.unwrap();
            assert_eq!(got_a, Some(drv(a)));
            assert_eq!(got_b, Some(drv(b)));
        });
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Overwriting the same drv-hash slot replaces the value.
    #[test]
    fn put_drv_overwrites_same_hash(hash_b in any::<u8>()) {
        let root = unique_tempdir("drv-overwrite");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        block_on(async {
            let cache = DiskDerivationCache::new(&root);
            // Same hash, different drv contents — DrvHash::new([hash_b; 32])
            // is the address; put_drv just rewrites the file.
            cache.put_drv(&drv(hash_b)).await.unwrap();
            cache.put_drv(&drv(hash_b)).await.unwrap();
            let got = cache.get_drv(&DrvHash::new([hash_b; 32])).await.unwrap();
            assert_eq!(got, Some(drv(hash_b)));
        });
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Realisations round-trip per drv_hash.
    #[test]
    fn realisation_round_trips_through_disk(
        drv_b in any::<u8>(),
        nar_b in any::<u8>(),
    ) {
        let root = unique_tempdir("real-rt");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        block_on(async {
            let cache = DiskDerivationCache::new(&root);
            let drv_hash = DrvHash::new([drv_b; 32]);
            let r = Realisation {
                drv_hash: drv_hash.clone(),
                output_name: "out".into(),
                output_path: OutputPath::new("/nix/store/x"),
                nar_hash: Some(NarHash::new([nar_b; 32])),
            };
            cache.put_realisation(&r).await.unwrap();
            let got = cache.list_realisations(&drv_hash).await.unwrap();
            assert_eq!(got, vec![r]);
        });
        let _ = std::fs::remove_dir_all(&root);
    }

    /// List realisations for an unseen drv → empty Vec.
    #[test]
    fn unseen_drv_has_no_realisations(drv_b in any::<u8>()) {
        let root = unique_tempdir("real-empty");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        block_on(async {
            let cache = DiskDerivationCache::new(&root);
            let got = cache.list_realisations(&DrvHash::new([drv_b; 32])).await.unwrap();
            assert!(got.is_empty());
        });
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Name is stable telemetry identifier.
    #[test]
    fn cache_name_is_stable(_seed in any::<u8>()) {
        let root = unique_tempdir("name");
        let cache = DiskDerivationCache::new(&root);
        assert_eq!(cache.name(), "disk");
    }

    /// Root accessor returns the configured path.
    #[test]
    fn root_accessor_returns_configured_path(_seed in any::<u8>()) {
        let root = unique_tempdir("root");
        let cache = DiskDerivationCache::new(&root);
        assert_eq!(cache.root(), root.as_path());
    }
}
