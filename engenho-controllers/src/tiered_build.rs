//! TieredBuildBackend — composes N `BuildBackend`s; tries each in
//! order, first success wins. Analog to `TieredCache` but for the
//! build path.
//!
//! ## Use-cases
//!
//!   * Try a fast local builder first (FakeBuildBackend for tests,
//!     SuiBuildBackend on-node for production), fall back to a
//!     remote queue (NatsBuildBackend) for unsupported systems
//!     or oversized workloads.
//!   * Try multiple cache mirrors (each backed by a different
//!     content tier) — first hit wins, others not queried.
//!   * Multi-arch fan-out: per-system builders, the controller
//!     filters to the matching ones before assembling the tier list.
//!
//! ## Semantics
//!
//!   * `build()` walks tiers in order; first `Ok` is returned.
//!   * Errors from a tier are collected; if EVERY tier fails, the
//!     last error is returned (callers can introspect to see what
//!     went wrong).
//!   * Empty tier list → `BuildError::Backend("no tiers")`.

use std::sync::Arc;

use async_trait::async_trait;
use engenho_substrate::Drv;

use crate::drv_build::{BuildBackend, BuildError, BuildResult};

/// Composed build backend.
pub struct TieredBuildBackend {
    tiers: Vec<Arc<dyn BuildBackend>>,
}

impl TieredBuildBackend {
    /// New tiered backend. Pass tiers in priority order (highest
    /// priority first).
    #[must_use]
    pub fn new(tiers: Vec<Arc<dyn BuildBackend>>) -> Self {
        Self { tiers }
    }

    /// Number of tiers.
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
impl BuildBackend for TieredBuildBackend {
    fn name(&self) -> &'static str {
        "tiered"
    }

    async fn build(&self, drv: &Drv) -> Result<BuildResult, BuildError> {
        if self.tiers.is_empty() {
            return Err(BuildError::Backend("no tiers configured".into()));
        }
        let mut last_err: Option<BuildError> = None;
        for tier in &self.tiers {
            match tier.build(drv).await {
                Ok(r) => return Ok(r),
                Err(e) => {
                    last_err = Some(e);
                }
            }
        }
        // All tiers failed.
        Err(last_err.unwrap_or_else(|| BuildError::Backend("all tiers failed".into())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::drv_build::FakeBuildBackend;
    use engenho_substrate::DrvHash;

    fn sample_drv() -> Drv {
        Drv::synthetic(DrvHash::from_bytes(b"x"), "x86_64-linux")
    }

    fn fake_arc() -> Arc<dyn BuildBackend> {
        Arc::new(FakeBuildBackend::new())
    }

    #[tokio::test]
    async fn empty_tiers_returns_error() {
        let t = TieredBuildBackend::new(vec![]);
        let err = t.build(&sample_drv()).await.unwrap_err();
        assert_eq!(err.kind(), "backend");
    }

    #[tokio::test]
    async fn first_tier_succeeds_short_circuits() {
        let h1 = Arc::new(FakeBuildBackend::new());
        let h2 = Arc::new(FakeBuildBackend::new());
        let t = TieredBuildBackend::new(vec![h1.clone(), h2.clone()]);
        let drv = sample_drv();
        let _ = t.build(&drv).await.unwrap();
        assert_eq!(h1.builds().await.len(), 1);
        assert_eq!(h2.builds().await.len(), 0, "h2 not queried on h1 success");
    }

    #[tokio::test]
    async fn falls_through_on_first_tier_failure() {
        let h1 = Arc::new(FakeBuildBackend::new());
        let h2 = Arc::new(FakeBuildBackend::new());
        h1.fail_next("h1 dead").await;
        let t = TieredBuildBackend::new(vec![h1.clone(), h2.clone()]);
        let _ = t.build(&sample_drv()).await.unwrap();
        assert_eq!(h1.builds().await.len(), 1);
        assert_eq!(h2.builds().await.len(), 1, "h2 reached on h1 failure");
    }

    #[tokio::test]
    async fn all_tiers_fail_returns_last_error() {
        let h1 = Arc::new(FakeBuildBackend::new());
        let h2 = Arc::new(FakeBuildBackend::new());
        h1.fail_next("h1 down").await;
        h2.fail_next("h2 down").await;
        let t = TieredBuildBackend::new(vec![h1.clone(), h2.clone()]);
        let err = t.build(&sample_drv()).await.unwrap_err();
        // Last tier's error is what's returned.
        assert!(err.to_string().contains("h2 down"));
        assert_eq!(err.kind(), "backend");
    }

    #[tokio::test]
    async fn tier_metadata_helpers() {
        let t = TieredBuildBackend::new(vec![fake_arc(), fake_arc()]);
        assert_eq!(t.tier_count(), 2);
        assert_eq!(t.tier_name(0), Some("fake"));
        assert_eq!(t.tier_name(2), None);
        assert_eq!(t.name(), "tiered");
    }

    #[tokio::test]
    async fn three_tier_walk_stops_at_second_success() {
        let h1 = Arc::new(FakeBuildBackend::new());
        let h2 = Arc::new(FakeBuildBackend::new());
        let h3 = Arc::new(FakeBuildBackend::new());
        h1.fail_next("h1 fail").await;
        let t = TieredBuildBackend::new(vec![h1.clone(), h2.clone(), h3.clone()]);
        let _ = t.build(&sample_drv()).await.unwrap();
        assert_eq!(h1.builds().await.len(), 1);
        assert_eq!(h2.builds().await.len(), 1);
        assert_eq!(h3.builds().await.len(), 0, "h3 not queried after h2 succeeds");
    }
}
