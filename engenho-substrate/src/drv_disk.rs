//! Disk persistence for derivations + realisations via MagicBlob.
//!
//! Wires the typed `Drv` value through the substrate's existing
//! `MagicBlob` + `write_atomic` primitives. Disk format:
//!
//! ```text
//! drv-{drv_hash}.bin       → MagicBlob<Drv>
//! realisation-{drv_hash}.bin → MagicBlob<Vec<Realisation>>
//! nar-{nar_hash}.bin        → MagicBlob<NarBlob>
//! ```
//!
//! Each file is BLAKE3-hash-checked, version-stamped, fsync-anchored
//! atomic write. Same pattern as `engenho-store::CatalogSnapshot`.

use std::path::{Path, PathBuf};

use async_trait::async_trait;

use crate::derivation::{
    CacheError, DerivationCacheBackend, Drv, DrvHash, NarBlob, NarHash, Realisation,
};
use crate::magic_blob::{MagicBlob, MagicBlobError};

const MAGIC_DRV_V1: &[u8] = b"engenho-drv v1\n";
const MAGIC_NAR_V1: &[u8] = b"engenho-nar v1\n";
const MAGIC_REALISATIONS_V1: &[u8] = b"engenho-realisations v1\n";

/// Local-disk derivation cache. Files laid out under `root`.
#[derive(Clone, Debug)]
pub struct DiskDerivationCache {
    root: PathBuf,
}

impl DiskDerivationCache {
    /// New cache rooted at `root`. Directory created on first write.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn drv_path(&self, hash: &DrvHash) -> PathBuf {
        self.root.join(format!("drv-{}.bin", hash.to_hex()))
    }

    fn nar_path(&self, hash: &NarHash) -> PathBuf {
        self.root.join(format!("nar-{}.bin", hash.to_hex()))
    }

    fn realisations_path(&self, drv_hash: &DrvHash) -> PathBuf {
        self.root
            .join(format!("realisations-{}.bin", drv_hash.to_hex()))
    }

    /// Root directory the cache reads + writes under.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }
}

fn map_err(e: MagicBlobError) -> CacheError {
    match e {
        MagicBlobError::HashMismatch => CacheError::HashMismatch {
            requested: "claimed".into(),
            actual: "computed".into(),
        },
        MagicBlobError::Io(s) | MagicBlobError::Encode(s) | MagicBlobError::Decode(s) => {
            CacheError::Backend(s)
        }
        MagicBlobError::BadMagic => CacheError::Backend("bad_magic".into()),
        MagicBlobError::Truncated => CacheError::Backend("truncated".into()),
    }
}

#[async_trait]
impl DerivationCacheBackend for DiskDerivationCache {
    fn name(&self) -> &'static str {
        "disk"
    }

    async fn get_drv(&self, hash: &DrvHash) -> Result<Option<Drv>, CacheError> {
        let path = self.drv_path(hash);
        if !path.exists() {
            return Ok(None);
        }
        let drv: Drv = MagicBlob::<Drv>::load_from(MAGIC_DRV_V1, &path).map_err(map_err)?;
        Ok(Some(drv))
    }

    async fn put_drv(&self, drv: &Drv) -> Result<(), CacheError> {
        let blob = MagicBlob {
            magic: MAGIC_DRV_V1,
            value: drv.clone(),
        };
        blob.save_to(&self.drv_path(&drv.drv_hash)).map_err(map_err)
    }

    async fn get_nar(&self, hash: &NarHash) -> Result<Option<NarBlob>, CacheError> {
        let path = self.nar_path(hash);
        if !path.exists() {
            return Ok(None);
        }
        let blob: NarBlob =
            MagicBlob::<NarBlob>::load_from(MAGIC_NAR_V1, &path).map_err(map_err)?;
        // Re-verify the blob's claimed hash matches its bytes —
        // defense in depth on top of MagicBlob's outer hash.
        let actual = NarHash::from_bytes(&blob.bytes);
        if &actual != hash {
            return Err(CacheError::HashMismatch {
                requested: hash.to_hex(),
                actual: actual.to_hex(),
            });
        }
        Ok(Some(blob))
    }

    async fn put_nar(&self, blob: &NarBlob) -> Result<(), CacheError> {
        let actual = NarHash::from_bytes(&blob.bytes);
        if actual != blob.hash {
            return Err(CacheError::HashMismatch {
                requested: blob.hash.to_hex(),
                actual: actual.to_hex(),
            });
        }
        let mb = MagicBlob {
            magic: MAGIC_NAR_V1,
            value: blob.clone(),
        };
        mb.save_to(&self.nar_path(&blob.hash)).map_err(map_err)
    }

    async fn list_realisations(&self, drv_hash: &DrvHash) -> Result<Vec<Realisation>, CacheError> {
        let path = self.realisations_path(drv_hash);
        if !path.exists() {
            return Ok(Vec::new());
        }
        MagicBlob::<Vec<Realisation>>::load_from(MAGIC_REALISATIONS_V1, &path).map_err(map_err)
    }

    async fn put_realisation(&self, realisation: &Realisation) -> Result<(), CacheError> {
        let path = self.realisations_path(&realisation.drv_hash);
        let mut existing: Vec<Realisation> = if path.exists() {
            MagicBlob::<Vec<Realisation>>::load_from(MAGIC_REALISATIONS_V1, &path)
                .map_err(map_err)?
        } else {
            Vec::new()
        };
        existing.retain(|r| r.output_name != realisation.output_name);
        existing.push(realisation.clone());
        let blob = MagicBlob {
            magic: MAGIC_REALISATIONS_V1,
            value: existing,
        };
        blob.save_to(&path).map_err(map_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::derivation::OutputPath;

    fn temp_root(suffix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("engenho-drv-disk-{}-{suffix}", std::process::id()))
    }

    #[tokio::test]
    async fn put_get_drv_round_trip_on_disk() {
        let root = temp_root("drv");
        let _ = std::fs::remove_dir_all(&root);
        let cache = DiskDerivationCache::new(&root);
        let drv = Drv::synthetic(DrvHash::from_bytes(b"d"), "x86_64-linux");
        cache.put_drv(&drv).await.unwrap();
        let got = cache.get_drv(&drv.drv_hash).await.unwrap();
        assert_eq!(got, Some(drv));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn get_drv_missing_returns_none() {
        let root = temp_root("missing");
        let _ = std::fs::remove_dir_all(&root);
        let cache = DiskDerivationCache::new(&root);
        assert!(
            cache
                .get_drv(&DrvHash::from_bytes(b"nothing"))
                .await
                .unwrap()
                .is_none()
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn put_get_nar_round_trip_on_disk() {
        let root = temp_root("nar");
        let _ = std::fs::remove_dir_all(&root);
        let cache = DiskDerivationCache::new(&root);
        let blob = NarBlob::from_bytes(b"hello-nar".to_vec());
        cache.put_nar(&blob).await.unwrap();
        let got = cache.get_nar(&blob.hash).await.unwrap();
        assert_eq!(got, Some(blob));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn realisations_persist_and_replace_by_output_name() {
        let root = temp_root("real");
        let _ = std::fs::remove_dir_all(&root);
        let cache = DiskDerivationCache::new(&root);
        let drv_hash = DrvHash::from_bytes(b"d");
        cache
            .put_realisation(&Realisation {
                drv_hash: drv_hash.clone(),
                output_name: "out".into(),
                output_path: OutputPath::new("/nix/store/v1"),
                nar_hash: None,
            })
            .await
            .unwrap();
        cache
            .put_realisation(&Realisation {
                drv_hash: drv_hash.clone(),
                output_name: "dev".into(),
                output_path: OutputPath::new("/nix/store/dev"),
                nar_hash: None,
            })
            .await
            .unwrap();
        let list = cache.list_realisations(&drv_hash).await.unwrap();
        assert_eq!(list.len(), 2);
        // Now replace "out".
        cache
            .put_realisation(&Realisation {
                drv_hash: drv_hash.clone(),
                output_name: "out".into(),
                output_path: OutputPath::new("/nix/store/v2"),
                nar_hash: None,
            })
            .await
            .unwrap();
        let list = cache.list_realisations(&drv_hash).await.unwrap();
        assert_eq!(list.len(), 2);
        let out = list.iter().find(|r| r.output_name == "out").unwrap();
        assert_eq!(out.output_path.as_str(), "/nix/store/v2");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn cache_name_is_stable() {
        let cache = DiskDerivationCache::new("/tmp");
        assert_eq!(cache.name(), "disk");
    }

    #[tokio::test]
    async fn put_nar_rejects_hash_mismatch() {
        let root = temp_root("badnar");
        let _ = std::fs::remove_dir_all(&root);
        let cache = DiskDerivationCache::new(&root);
        let bad = NarBlob {
            hash: NarHash::from_bytes(b"different"),
            size: 5,
            bytes: b"hello".to_vec(),
        };
        let err = cache.put_nar(&bad).await.unwrap_err();
        assert_eq!(err.kind(), "hash_mismatch");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn surviving_restart_via_reopen() {
        let root = temp_root("restart");
        let _ = std::fs::remove_dir_all(&root);
        {
            let cache = DiskDerivationCache::new(&root);
            for tag in [b"a".as_ref(), b"b", b"c"] {
                cache
                    .put_drv(&Drv::synthetic(DrvHash::from_bytes(tag), "x86_64-linux"))
                    .await
                    .unwrap();
            }
        }
        // New cache instance, same root — same data.
        let cache2 = DiskDerivationCache::new(&root);
        for tag in [b"a".as_ref(), b"b", b"c"] {
            assert!(
                cache2
                    .get_drv(&DrvHash::from_bytes(tag))
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        let _ = std::fs::remove_dir_all(&root);
    }
}
