//! Tiered derivation cache — walks an ordered list of backends,
//! promoting hits to higher (faster) tiers.
//!
//! ## Composition shape
//!
//! ```text
//! TieredCache { tiers: [L0 memory, L1 disk, L2 cluster, L3 federation] }
//!   get_drv(h):
//!     for each tier in order:
//!       if hit -> promote to all higher tiers, return
//!     return None
//!   put_drv(d):
//!     write to L0 + L1 (the local tiers); cluster + federation are
//!     reconcile-driven, not write-through.
//! ```
//!
//! ## Why "promote on read" + "write to local only"
//!
//! Symmetric with how every other tiered system (CPU caches, CDNs,
//! ZFS L2ARC, S3 + CloudFront) works. Higher tiers are eventually-
//! consistent reflections of authoritative lower tiers; writes are
//! cheap locally; reads pull data closer to the consumer.
//!
//! Cross-tier promotion is best-effort: a promote-failure to an
//! upper tier doesn't fail the read. The trait contract still holds
//! (returned bytes hash-verified by every backend's `get_nar`).

use std::sync::Arc;

use async_trait::async_trait;

use crate::derivation::{
    CacheError, DerivationCacheBackend, Drv, DrvHash, NarBlob, NarHash, Realisation,
};
use crate::promotion::{PromotionContext, PromotionGate, PromotionPolicy};

/// Composed cache: walks tiers in order, returns first hit,
/// promotes the hit to all higher (faster) tiers per the
/// configured [`PromotionPolicy`].
///
/// Construct with [`TieredCache::new`] (eager promotion default)
/// or [`TieredCache::with_promotion`] for a custom policy.
/// Tiers are FASTEST (L0) first, SLOWEST (Ln) last — load-bearing.
pub struct TieredCache {
    tiers: Vec<Arc<dyn DerivationCacheBackend>>,
    gate: Arc<PromotionGate>,
}

impl TieredCache {
    /// New tiered cache with eager promotion (default).
    /// Empty vec → cache is a no-op (every get returns None, every
    /// put succeeds silently).
    #[must_use]
    pub fn new(tiers: Vec<Arc<dyn DerivationCacheBackend>>) -> Self {
        Self {
            tiers,
            gate: Arc::new(PromotionGate::new(PromotionPolicy::Eager)),
        }
    }

    /// New tiered cache with a specific promotion policy.
    #[must_use]
    pub fn with_promotion(
        tiers: Vec<Arc<dyn DerivationCacheBackend>>,
        policy: PromotionPolicy,
    ) -> Self {
        Self {
            tiers,
            gate: Arc::new(PromotionGate::new(policy)),
        }
    }

    /// Total tiers (snapshot helper).
    fn total(&self) -> usize {
        self.tiers.len()
    }

    /// Number of tiers in this cache.
    #[must_use]
    pub fn tier_count(&self) -> usize {
        self.tiers.len()
    }

    /// Backend name of tier `i` (telemetry helper).
    #[must_use]
    pub fn tier_name(&self, i: usize) -> Option<&'static str> {
        self.tiers.get(i).map(|t| t.name())
    }
}

#[async_trait]
impl DerivationCacheBackend for TieredCache {
    fn name(&self) -> &'static str {
        "tiered"
    }

    async fn get_drv(&self, hash: &DrvHash) -> Result<Option<Drv>, CacheError> {
        let total = self.total();
        for (idx, tier) in self.tiers.iter().enumerate() {
            if let Some(drv) = tier.get_drv(hash).await? {
                // Ask the promotion gate.
                let ctx = PromotionContext { source_tier: idx, total_tiers: total };
                if let Some(promote_to) = self.gate.decide(ctx) {
                    for higher in &self.tiers[..promote_to] {
                        let _ = higher.put_drv(&drv).await;
                    }
                }
                return Ok(Some(drv));
            }
        }
        Ok(None)
    }

    async fn put_drv(&self, drv: &Drv) -> Result<(), CacheError> {
        // Write-to-local: only the fastest tier (L0). Production
        // deployments add a separate reconcile loop that fans out
        // to cluster + federation. Single-tier setups still work
        // because L0 IS the storage.
        if let Some(local) = self.tiers.first() {
            local.put_drv(drv).await?;
        }
        Ok(())
    }

    async fn get_nar(&self, hash: &NarHash) -> Result<Option<NarBlob>, CacheError> {
        let total = self.total();
        for (idx, tier) in self.tiers.iter().enumerate() {
            // Get from this tier. HashMismatch propagates — corrupt
            // cache is a halting fault, not a fallback trigger.
            if let Some(blob) = tier.get_nar(hash).await? {
                let ctx = PromotionContext { source_tier: idx, total_tiers: total };
                if let Some(promote_to) = self.gate.decide(ctx) {
                    for higher in &self.tiers[..promote_to] {
                        let _ = higher.put_nar(&blob).await;
                    }
                }
                return Ok(Some(blob));
            }
        }
        Ok(None)
    }

    async fn put_nar(&self, blob: &NarBlob) -> Result<(), CacheError> {
        if let Some(local) = self.tiers.first() {
            local.put_nar(blob).await?;
        }
        Ok(())
    }

    async fn list_realisations(
        &self,
        drv_hash: &DrvHash,
    ) -> Result<Vec<Realisation>, CacheError> {
        // Merge realisations across tiers: a realisation can exist
        // anywhere, and the lattice union is the correct answer
        // (every output-name distinct, latest wins).
        let mut merged: std::collections::BTreeMap<String, Realisation> =
            std::collections::BTreeMap::new();
        for tier in &self.tiers {
            for r in tier.list_realisations(drv_hash).await? {
                merged.insert(r.output_name.clone(), r);
            }
        }
        Ok(merged.into_values().collect())
    }

    async fn put_realisation(
        &self,
        realisation: &Realisation,
    ) -> Result<(), CacheError> {
        // Write to L0 only; promotion to upper tiers via reconcile.
        if let Some(local) = self.tiers.first() {
            local.put_realisation(realisation).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::derivation::MemoryDerivationCache;
    use crate::derivation::OutputPath;

    fn sample_drv(tag: &[u8]) -> Drv {
        Drv::synthetic(DrvHash::from_bytes(tag), "x86_64-linux")
    }

    fn arc<T: DerivationCacheBackend + 'static>(t: T) -> Arc<dyn DerivationCacheBackend> {
        Arc::new(t)
    }

    #[tokio::test]
    async fn empty_tiered_cache_returns_none() {
        let c = TieredCache::new(vec![]);
        let h = DrvHash::from_bytes(b"x");
        assert!(c.get_drv(&h).await.unwrap().is_none());
        assert_eq!(c.tier_count(), 0);
    }

    #[tokio::test]
    async fn tier_count_and_names() {
        let l0 = arc(MemoryDerivationCache::new());
        let l1 = arc(MemoryDerivationCache::new());
        let c = TieredCache::new(vec![l0, l1]);
        assert_eq!(c.tier_count(), 2);
        assert_eq!(c.tier_name(0), Some("memory"));
        assert_eq!(c.tier_name(1), Some("memory"));
        assert_eq!(c.tier_name(2), None);
        assert_eq!(c.name(), "tiered");
    }

    #[tokio::test]
    async fn get_drv_walks_tiers_until_hit() {
        let l0 = Arc::new(MemoryDerivationCache::new());
        let l1 = Arc::new(MemoryDerivationCache::new());
        let drv = sample_drv(b"a");
        l1.put_drv(&drv).await.unwrap();  // only in L1
        let c = TieredCache::new(vec![l0.clone(), l1.clone()]);
        let got = c.get_drv(&drv.drv_hash).await.unwrap();
        assert_eq!(got, Some(drv.clone()));
        // Promotion: L0 should now have it too.
        assert_eq!(l0.get_drv(&drv.drv_hash).await.unwrap(), Some(drv));
    }

    #[tokio::test]
    async fn get_drv_short_circuits_at_first_hit() {
        let l0 = Arc::new(MemoryDerivationCache::new());
        let l1 = Arc::new(MemoryDerivationCache::new());
        let drv = sample_drv(b"hit-l0");
        l0.put_drv(&drv).await.unwrap();
        // L1 deliberately not populated.
        let c = TieredCache::new(vec![l0.clone(), l1.clone()]);
        let got = c.get_drv(&drv.drv_hash).await.unwrap();
        assert_eq!(got, Some(drv));
        // L1 should not have been written to (no promotion needed).
        assert_eq!(l1.drv_count().await, 0);
    }

    #[tokio::test]
    async fn put_drv_writes_to_local_only() {
        let l0 = Arc::new(MemoryDerivationCache::new());
        let l1 = Arc::new(MemoryDerivationCache::new());
        let c = TieredCache::new(vec![l0.clone(), l1.clone()]);
        c.put_drv(&sample_drv(b"q")).await.unwrap();
        assert_eq!(l0.drv_count().await, 1);
        assert_eq!(l1.drv_count().await, 0);
    }

    #[tokio::test]
    async fn get_nar_walks_and_promotes() {
        let l0 = Arc::new(MemoryDerivationCache::new());
        let l1 = Arc::new(MemoryDerivationCache::new());
        let blob = NarBlob::from_bytes(b"nar-bytes".to_vec());
        l1.put_nar(&blob).await.unwrap();
        let c = TieredCache::new(vec![l0.clone(), l1.clone()]);
        let got = c.get_nar(&blob.hash).await.unwrap();
        assert_eq!(got, Some(blob.clone()));
        // Promotion: L0 should have it.
        assert_eq!(l0.get_nar(&blob.hash).await.unwrap(), Some(blob));
    }

    #[tokio::test]
    async fn list_realisations_merges_across_tiers() {
        let l0 = Arc::new(MemoryDerivationCache::new());
        let l1 = Arc::new(MemoryDerivationCache::new());
        let drv_hash = DrvHash::from_bytes(b"d");
        // L0 has "out", L1 has "dev"; merge should produce both.
        l0.put_realisation(&Realisation {
            drv_hash: drv_hash.clone(),
            output_name: "out".into(),
            output_path: OutputPath::new("/nix/store/a-out"),
            nar_hash: None,
        })
        .await
        .unwrap();
        l1.put_realisation(&Realisation {
            drv_hash: drv_hash.clone(),
            output_name: "dev".into(),
            output_path: OutputPath::new("/nix/store/b-dev"),
            nar_hash: None,
        })
        .await
        .unwrap();
        let c = TieredCache::new(vec![l0, l1]);
        let list = c.list_realisations(&drv_hash).await.unwrap();
        assert_eq!(list.len(), 2);
        assert!(list.iter().any(|r| r.output_name == "out"));
        assert!(list.iter().any(|r| r.output_name == "dev"));
    }

    #[tokio::test]
    async fn list_realisations_lower_tier_loses_to_higher_on_same_output_name() {
        let l0 = Arc::new(MemoryDerivationCache::new());
        let l1 = Arc::new(MemoryDerivationCache::new());
        let drv_hash = DrvHash::from_bytes(b"d");
        // Both tiers have "out" but with DIFFERENT output paths.
        l0.put_realisation(&Realisation {
            drv_hash: drv_hash.clone(),
            output_name: "out".into(),
            output_path: OutputPath::new("/nix/store/l0-out"),
            nar_hash: None,
        })
        .await
        .unwrap();
        l1.put_realisation(&Realisation {
            drv_hash: drv_hash.clone(),
            output_name: "out".into(),
            output_path: OutputPath::new("/nix/store/l1-out"),
            nar_hash: None,
        })
        .await
        .unwrap();
        // Merge walks L0 first then L1; later insert wins (L1's path).
        // Operators wiring real tiers should make L0 authoritative; the
        // merge semantics here are "lattice union, latest wins" — that
        // matches the eventual-consistency story across tiers.
        let c = TieredCache::new(vec![l0, l1]);
        let list = c.list_realisations(&drv_hash).await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].output_path.as_str(), "/nix/store/l1-out");
    }

    #[tokio::test]
    async fn put_realisation_writes_to_local_only() {
        let l0 = Arc::new(MemoryDerivationCache::new());
        let l1 = Arc::new(MemoryDerivationCache::new());
        let drv_hash = DrvHash::from_bytes(b"d");
        let c = TieredCache::new(vec![l0.clone(), l1.clone()]);
        c.put_realisation(&Realisation {
            drv_hash: drv_hash.clone(),
            output_name: "out".into(),
            output_path: OutputPath::new("/nix/store/a-out"),
            nar_hash: None,
        })
        .await
        .unwrap();
        assert_eq!(l0.list_realisations(&drv_hash).await.unwrap().len(), 1);
        assert_eq!(l1.list_realisations(&drv_hash).await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn miss_in_all_tiers_returns_none() {
        let l0 = Arc::new(MemoryDerivationCache::new());
        let l1 = Arc::new(MemoryDerivationCache::new());
        let c = TieredCache::new(vec![l0, l1]);
        let h = DrvHash::from_bytes(b"never-seen");
        assert!(c.get_drv(&h).await.unwrap().is_none());
        assert!(c.get_nar(&NarHash::from_bytes(b"never")).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn with_promotion_lazy_disables_promote_on_drv_read() {
        let l0 = Arc::new(MemoryDerivationCache::new());
        let l1 = Arc::new(MemoryDerivationCache::new());
        let drv = sample_drv(b"lazy-drv");
        l1.put_drv(&drv).await.unwrap();
        let c = TieredCache::with_promotion(vec![l0.clone(), l1.clone()], PromotionPolicy::Lazy);
        let got = c.get_drv(&drv.drv_hash).await.unwrap();
        assert_eq!(got, Some(drv.clone()));
        // L0 should NOT have been written to — promotion suppressed.
        assert_eq!(l0.drv_count().await, 0);
    }

    #[tokio::test]
    async fn with_promotion_lazy_disables_promote_on_nar_read() {
        let l0 = Arc::new(MemoryDerivationCache::new());
        let l1 = Arc::new(MemoryDerivationCache::new());
        let blob = NarBlob::from_bytes(b"lazy-nar".to_vec());
        l1.put_nar(&blob).await.unwrap();
        let c = TieredCache::with_promotion(vec![l0.clone(), l1.clone()], PromotionPolicy::Lazy);
        let got = c.get_nar(&blob.hash).await.unwrap();
        assert_eq!(got, Some(blob));
        assert_eq!(l0.nar_count().await, 0);
    }

    #[tokio::test]
    async fn with_promotion_eager_still_promotes() {
        let l0 = Arc::new(MemoryDerivationCache::new());
        let l1 = Arc::new(MemoryDerivationCache::new());
        let drv = sample_drv(b"eager-drv");
        l1.put_drv(&drv).await.unwrap();
        let c = TieredCache::with_promotion(vec![l0.clone(), l1.clone()], PromotionPolicy::Eager);
        c.get_drv(&drv.drv_hash).await.unwrap();
        // Eager preserves the original behavior.
        assert_eq!(l0.drv_count().await, 1);
    }

    #[tokio::test]
    async fn with_promotion_sample_rate_promotes_one_in_n() {
        let l0 = Arc::new(MemoryDerivationCache::new());
        let l1 = Arc::new(MemoryDerivationCache::new());
        // Pre-populate L1 with several distinct drvs.
        let drvs: Vec<Drv> = (0u8..6).map(|i| sample_drv(&[b'k', i])).collect();
        for d in &drvs {
            l1.put_drv(d).await.unwrap();
        }
        let c = TieredCache::with_promotion(
            vec![l0.clone(), l1.clone()],
            PromotionPolicy::SampleRate(3),
        );
        // 6 reads at rate 3 → exactly 2 promotions.
        for d in &drvs {
            c.get_drv(&d.drv_hash).await.unwrap();
        }
        assert_eq!(l0.drv_count().await, 2);
    }
}
