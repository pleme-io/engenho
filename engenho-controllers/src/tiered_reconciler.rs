//! TieredCacheReconciler — proactive promotion across cache tiers.
//!
//! Complement to TieredCache's on-read promotion: this controller
//! sweeps lower-tier entries that haven't yet reached upper tiers
//! + fans them out. Useful when the operator's PromotionPolicy is
//! Lazy or SampleRate (which deliberately skip on-read promotion).
//!
//! ## Reconcile rule
//!
//! For a configured set of (drv_hash, output_name) of interest:
//!   1. Walk all tiers via separate handles (NOT through the
//!      tiered facade — we need per-tier visibility).
//!   2. For each item present in tier `i` but absent in tier `j<i`,
//!      copy the bytes from `i` → `j`.
//!   3. Report per-tier promotion counts via ReconcileReport.
//!
//! ## Configuring "items of interest"
//!
//! The controller doesn't sweep the whole drv-space (that's
//! O(cluster-wide). Instead it consults a [`PromotionScope`]
//! trait that yields drv hashes worth promoting (e.g. from a
//! DerivationCR list, from a watched-cache subscription, from
//! a static config of "always-cache-locally" pins).

use std::sync::Arc;

use async_trait::async_trait;
use engenho_substrate::{DerivationCacheBackend, DrvHash};

use crate::controller::{Controller, ReconcileReport};
use crate::error::ControllerError;

/// Source of "items of interest" for proactive promotion.
#[async_trait]
pub trait PromotionScope: Send + Sync {
    /// Drv hashes the controller should keep promoted to high tiers.
    /// Returning an empty Vec is fine (controller is a no-op).
    ///
    /// # Errors
    /// Implementations may surface I/O or backend errors.
    async fn drv_hashes_of_interest(&self) -> Result<Vec<DrvHash>, ControllerError>;
}

/// Static scope — fixed list of pins.
pub struct StaticPromotionScope {
    pins: Vec<DrvHash>,
}

impl StaticPromotionScope {
    /// New scope from a list of drv hashes.
    #[must_use]
    pub fn new(pins: Vec<DrvHash>) -> Self {
        Self { pins }
    }
}

#[async_trait]
impl PromotionScope for StaticPromotionScope {
    async fn drv_hashes_of_interest(&self) -> Result<Vec<DrvHash>, ControllerError> {
        Ok(self.pins.clone())
    }
}

/// Reconciler controller.
pub struct TieredCacheReconciler {
    /// Tiers ordered fastest-to-slowest (same convention as TieredCache).
    tiers: Vec<Arc<dyn DerivationCacheBackend>>,
    scope: Arc<dyn PromotionScope>,
}

impl TieredCacheReconciler {
    /// New reconciler.
    #[must_use]
    pub fn new(
        tiers: Vec<Arc<dyn DerivationCacheBackend>>,
        scope: Arc<dyn PromotionScope>,
    ) -> Self {
        Self { tiers, scope }
    }

    /// Tier count.
    #[must_use]
    pub fn tier_count(&self) -> usize {
        self.tiers.len()
    }
}

#[async_trait]
impl Controller for TieredCacheReconciler {
    fn name(&self) -> &'static str {
        "tiered-cache-reconciler"
    }

    async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
        let mut report = ReconcileReport::default();
        if self.tiers.len() < 2 {
            // No promotion possible with < 2 tiers.
            return Ok(report);
        }
        let pins = self.scope.drv_hashes_of_interest().await?;
        report.objects_examined = pins.len();

        for hash in &pins {
            // Find the highest tier index that holds this drv.
            // (highest = LARGEST idx = slowest tier; we promote
            // toward idx 0 = fastest.)
            let mut authoritative_idx: Option<usize> = None;
            for (idx, tier) in self.tiers.iter().enumerate().rev() {
                let exists = tier
                    .get_drv(hash)
                    .await
                    .map_err(|e| ControllerError::Internal(e.to_string()))?
                    .is_some();
                if exists {
                    authoritative_idx = Some(idx);
                    break;
                }
            }
            // If found, walk down to all faster tiers + promote where missing.
            if let Some(src_idx) = authoritative_idx {
                let drv = self.tiers[src_idx]
                    .get_drv(hash)
                    .await
                    .map_err(|e| ControllerError::Internal(e.to_string()))?
                    .expect("just verified existence");
                // Also fetch realisations to promote them.
                let realisations = self.tiers[src_idx]
                    .list_realisations(hash)
                    .await
                    .map_err(|e| ControllerError::Internal(e.to_string()))?;

                for dst_idx in 0..src_idx {
                    // Promote drv if missing.
                    let exists = self.tiers[dst_idx]
                        .get_drv(hash)
                        .await
                        .map_err(|e| ControllerError::Internal(e.to_string()))?
                        .is_some();
                    if !exists {
                        self.tiers[dst_idx]
                            .put_drv(&drv)
                            .await
                            .map_err(|e| ControllerError::Internal(e.to_string()))?;
                        report.objects_changed += 1;
                    }
                    // Promote realisations (idempotent — backend dedupes by output_name).
                    for r in &realisations {
                        self.tiers[dst_idx]
                            .put_realisation(r)
                            .await
                            .map_err(|e| ControllerError::Internal(e.to_string()))?;
                    }
                }
            }
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_substrate::{Drv, MemoryDerivationCache, OutputPath, Realisation};

    fn arc_mem() -> Arc<dyn DerivationCacheBackend> {
        Arc::new(MemoryDerivationCache::new())
    }

    fn sample_drv(tag: &[u8]) -> Drv {
        Drv::synthetic(DrvHash::from_bytes(tag), "x86_64-linux")
    }

    #[tokio::test]
    async fn empty_scope_no_changes() {
        let r = TieredCacheReconciler::new(
            vec![arc_mem(), arc_mem()],
            Arc::new(StaticPromotionScope::new(vec![])),
        );
        let report = r.tick().await.unwrap();
        assert_eq!(report.objects_changed, 0);
    }

    #[tokio::test]
    async fn single_tier_no_promotion() {
        let only = arc_mem();
        only.put_drv(&sample_drv(b"x")).await.unwrap();
        let r = TieredCacheReconciler::new(
            vec![only],
            Arc::new(StaticPromotionScope::new(vec![DrvHash::from_bytes(b"x")])),
        );
        let report = r.tick().await.unwrap();
        assert_eq!(report.objects_changed, 0);
    }

    #[tokio::test]
    async fn promotes_drv_from_slow_to_fast_tier() {
        let l0 = arc_mem();
        let l1 = arc_mem();
        let drv = sample_drv(b"promote-me");
        // Only in L1.
        l1.put_drv(&drv).await.unwrap();
        let r = TieredCacheReconciler::new(
            vec![l0.clone(), l1.clone()],
            Arc::new(StaticPromotionScope::new(vec![drv.drv_hash.clone()])),
        );
        let report = r.tick().await.unwrap();
        assert_eq!(report.objects_changed, 1);
        // L0 now has the drv too.
        assert!(l0.get_drv(&drv.drv_hash).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn idempotent_when_already_present_everywhere() {
        let l0 = arc_mem();
        let l1 = arc_mem();
        let drv = sample_drv(b"x");
        l0.put_drv(&drv).await.unwrap();
        l1.put_drv(&drv).await.unwrap();
        let r = TieredCacheReconciler::new(
            vec![l0, l1],
            Arc::new(StaticPromotionScope::new(vec![drv.drv_hash])),
        );
        let report = r.tick().await.unwrap();
        assert_eq!(report.objects_changed, 0);
    }

    #[tokio::test]
    async fn promotes_realisations_alongside_drvs() {
        let l0 = arc_mem();
        let l1 = arc_mem();
        let drv = sample_drv(b"d");
        l1.put_drv(&drv).await.unwrap();
        l1.put_realisation(&Realisation {
            drv_hash: drv.drv_hash.clone(),
            output_name: "out".into(),
            output_path: OutputPath::new("/nix/store/a"),
            nar_hash: None,
        })
        .await
        .unwrap();
        let r = TieredCacheReconciler::new(
            vec![l0.clone(), l1.clone()],
            Arc::new(StaticPromotionScope::new(vec![drv.drv_hash.clone()])),
        );
        let _ = r.tick().await.unwrap();
        let l0_rs = l0.list_realisations(&drv.drv_hash).await.unwrap();
        assert_eq!(l0_rs.len(), 1);
        assert_eq!(l0_rs[0].output_name, "out");
    }

    #[tokio::test]
    async fn three_tier_promotion_walks_down_from_authoritative() {
        let l0 = arc_mem();
        let l1 = arc_mem();
        let l2 = arc_mem();
        let drv = sample_drv(b"three-tier");
        // Only in L2 (slowest).
        l2.put_drv(&drv).await.unwrap();
        let r = TieredCacheReconciler::new(
            vec![l0.clone(), l1.clone(), l2.clone()],
            Arc::new(StaticPromotionScope::new(vec![drv.drv_hash.clone()])),
        );
        let report = r.tick().await.unwrap();
        // Both L0 and L1 promoted = 2 changes.
        assert_eq!(report.objects_changed, 2);
        assert!(l0.get_drv(&drv.drv_hash).await.unwrap().is_some());
        assert!(l1.get_drv(&drv.drv_hash).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn missing_drv_in_any_tier_silently_skipped() {
        let l0 = arc_mem();
        let l1 = arc_mem();
        // Nothing in either tier.
        let r = TieredCacheReconciler::new(
            vec![l0.clone(), l1.clone()],
            Arc::new(StaticPromotionScope::new(vec![DrvHash::from_bytes(
                b"void",
            )])),
        );
        let report = r.tick().await.unwrap();
        assert_eq!(report.objects_changed, 0);
        assert!(
            l0.get_drv(&DrvHash::from_bytes(b"void"))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn tier_count_helper() {
        let r = TieredCacheReconciler::new(
            vec![arc_mem(), arc_mem(), arc_mem()],
            Arc::new(StaticPromotionScope::new(vec![])),
        );
        assert_eq!(r.tier_count(), 3);
    }

    #[tokio::test]
    async fn controller_name_is_stable() {
        let r = TieredCacheReconciler::new(
            vec![arc_mem()],
            Arc::new(StaticPromotionScope::new(vec![])),
        );
        assert_eq!(r.name(), "tiered-cache-reconciler");
    }
}
