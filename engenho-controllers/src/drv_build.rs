//! DrvBuildController — the consumer that watches `DerivationCR`
//! phase=Pending + drives a pluggable build backend.
//!
//! Pairs with [`crate::drv::DrvController`] (which sets the phase
//! from cache state). DrvBuildController acts on the missing-output
//! case: take the drv, run it through a `BuildBackend`, capture
//! realisations + NAR blob, push back into the cache.
//!
//! ## Pluggable backends
//!
//!   * `FakeBuildBackend` — deterministic for tests; produces
//!     synthetic realisations
//!   * `SuiBuildBackend` — future; shells out to sui's build path
//!     (or directly invokes sui's typed Build API once available)

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand},
};
use engenho_substrate::{
    DerivationCacheBackend, Drv, DrvHash, NarBlob, NarHash, OutputPath, Realisation,
};
use serde_json::json;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::debug;

use crate::controller::{Controller, ReconcileOutcome, ReconcileReport};
use crate::drv::DrvController;
use crate::error::ControllerError;

/// One build's output bundle — realisations + the NAR blobs
/// referenced by them (typically one NAR per realisation).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuildResult {
    /// One realisation per declared output name.
    pub realisations: Vec<Realisation>,
    /// NAR blobs the realisations point at.
    pub nars: Vec<NarBlob>,
}

/// Build backend errors.
#[derive(Debug, Clone, Error)]
pub enum BuildError {
    /// Backend (sui, remote builder, queue) failed.
    #[error("backend: {0}")]
    Backend(String),
    /// Drv was structurally invalid (no outputs declared, etc.).
    #[error("invalid drv: {0}")]
    InvalidDrv(String),
}

engenho_substrate::impl_error_kind! {
    BuildError {
        (Backend(_)) => "backend",
        (InvalidDrv(_)) => "invalid_drv",
    }
}

/// Pluggable build backend trait.
#[async_trait]
pub trait BuildBackend: Send + Sync {
    /// Backend identifier for telemetry.
    fn name(&self) -> &'static str;

    /// Build the drv. Returns realisations + NAR blobs.
    ///
    /// # Errors
    /// [`BuildError::Backend`] on backend failure;
    /// [`BuildError::InvalidDrv`] for malformed drvs.
    async fn build(&self, drv: &Drv) -> Result<BuildResult, BuildError>;
}

// =================================================================
// FakeBuildBackend — deterministic, no real building
// =================================================================

/// Deterministic test backend. Produces synthetic realisations
/// + NAR blobs based on the drv hash. Records every build call.
#[derive(Default, Clone)]
pub struct FakeBuildBackend {
    inner: Arc<Mutex<FakeBuildState>>,
}

#[derive(Default)]
struct FakeBuildState {
    builds: Vec<DrvHash>,
    /// If set, the next build returns this error instead.
    fail_with: Option<String>,
}

impl FakeBuildBackend {
    /// Fresh backend.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of build calls.
    pub async fn builds(&self) -> Vec<DrvHash> {
        self.inner.lock().await.builds.clone()
    }

    /// Make the next build call return BuildError::Backend.
    pub async fn fail_next(&self, msg: impl Into<String>) {
        self.inner.lock().await.fail_with = Some(msg.into());
    }
}

#[async_trait]
impl BuildBackend for FakeBuildBackend {
    fn name(&self) -> &'static str {
        "fake"
    }

    async fn build(&self, drv: &Drv) -> Result<BuildResult, BuildError> {
        let mut state = self.inner.lock().await;
        state.builds.push(drv.drv_hash.clone());
        if let Some(msg) = state.fail_with.take() {
            return Err(BuildError::Backend(msg));
        }
        // Synthetic outputs: one per declared, or "out" if none declared.
        let outputs: Vec<String> = if drv.outputs.is_empty() {
            vec!["out".into()]
        } else {
            drv.outputs.keys().cloned().collect()
        };
        let mut realisations = Vec::with_capacity(outputs.len());
        let mut nars = Vec::with_capacity(outputs.len());
        for name in outputs {
            let synthetic_bytes = format!("nar:{}:{name}", drv.drv_hash.to_hex()).into_bytes();
            let nar = NarBlob::from_bytes(synthetic_bytes);
            realisations.push(Realisation {
                drv_hash: drv.drv_hash.clone(),
                output_name: name.clone(),
                output_path: OutputPath::new(format!(
                    "/nix/store/{}-{name}",
                    &drv.drv_hash.to_hex()[..16]
                )),
                nar_hash: Some(nar.hash.clone()),
            });
            nars.push(nar);
        }
        Ok(BuildResult { realisations, nars })
    }
}

// =================================================================
// DrvBuildController
// =================================================================

/// Controller that drives Pending derivations through a BuildBackend.
pub struct DrvBuildController {
    store: Arc<StoreMesh>,
    cache: Arc<dyn DerivationCacheBackend>,
    builder: Arc<dyn BuildBackend>,
    namespace: Option<String>,
}

impl DrvBuildController {
    /// New controller.
    #[must_use]
    pub fn new(
        store: Arc<StoreMesh>,
        cache: Arc<dyn DerivationCacheBackend>,
        builder: Arc<dyn BuildBackend>,
        namespace: Option<String>,
    ) -> Self {
        Self {
            store,
            cache,
            builder,
            namespace,
        }
    }

    /// Push a [`BuildResult`] into the cache. Pure helper exposed
    /// for tests that don't want to drive a full controller tick.
    ///
    /// # Errors
    /// Propagates [`engenho_substrate::CacheError`] as backend errors.
    pub async fn ingest(
        cache: &Arc<dyn DerivationCacheBackend>,
        result: &BuildResult,
    ) -> Result<(), ControllerError> {
        for nar in &result.nars {
            cache
                .put_nar(nar)
                .await
                .map_err(|e| ControllerError::Internal(e.to_string()))?;
        }
        for r in &result.realisations {
            cache
                .put_realisation(r)
                .await
                .map_err(|e| ControllerError::Internal(e.to_string()))?;
        }
        Ok(())
    }
}

#[async_trait]
impl Controller for DrvBuildController {
    fn name(&self) -> &'static str {
        "drv-build"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let drs = self
            .store
            .list("engenho.io", "v1", "Derivation", self.namespace.as_deref())
            .await;
        let mut report = ReconcileReport::default();
        report.objects_examined = drs.len();

        for (cr_key, cr_value) in &drs {
            // Only act on Pending phase.
            let phase = cr_value
                .get("status")
                .and_then(|s| s.get("phase"))
                .and_then(|p| p.as_str());
            if phase != Some("Pending") {
                continue;
            }
            let hash_str = cr_value
                .get("spec")
                .and_then(|s| s.get("drvHash"))
                .and_then(|h| h.as_str());
            let Some(hash_str) = hash_str else {
                report.objects_skipped += 1;
                continue;
            };
            let Some(hash) = DrvController::parse_drv_hash(hash_str) else {
                report.objects_skipped += 1;
                continue;
            };

            // Read the drv itself from the cache.
            let drv = match self
                .cache
                .get_drv(&hash)
                .await
                .map_err(|e| ControllerError::Internal(e.to_string()))?
            {
                Some(d) => d,
                None => {
                    debug!(drv_hash = %hash_str, "DrvBuild: drv not in cache");
                    continue;
                }
            };

            // Build.
            let result = match self.builder.build(&drv).await {
                Ok(r) => r,
                Err(e) => {
                    // Surface the failure into the CR status.
                    self.store
                        .propose(ResourceCommand::patch(
                            cr_key.clone(),
                            json!({
                                "status": {
                                    "phase": "BuildFailed",
                                    "message": e.to_string(),
                                }
                            }),
                            Reason::Controller,
                        ))
                        .await?;
                    report.objects_changed += 1;
                    continue;
                }
            };

            // Ingest into the cache.
            Self::ingest(&self.cache, &result).await?;
            report.objects_changed += 1;
        }
        Ok(report.into())
    }
}

/// Build-result helpers for downstream consumers.
impl BuildResult {
    /// Empty result.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            realisations: Vec::new(),
            nars: Vec::new(),
        }
    }

    /// Construct a single-output result for tests / smoke flows.
    #[must_use]
    pub fn single_output(
        drv_hash: DrvHash,
        output_name: impl Into<String>,
        output_path: OutputPath,
        nar_bytes: Vec<u8>,
    ) -> Self {
        let nar = NarBlob::from_bytes(nar_bytes);
        let realisation = Realisation {
            drv_hash,
            output_name: output_name.into(),
            output_path,
            nar_hash: Some(nar.hash.clone()),
        };
        Self {
            realisations: vec![realisation],
            nars: vec![nar],
        }
    }

    /// Map output name → NAR hash for a quick lookup.
    #[must_use]
    pub fn nar_index(&self) -> BTreeMap<String, NarHash> {
        self.realisations
            .iter()
            .filter_map(|r| {
                r.nar_hash
                    .as_ref()
                    .map(|h| (r.output_name.clone(), h.clone()))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_substrate::MemoryDerivationCache;

    fn sample_drv() -> Drv {
        Drv::synthetic(DrvHash::from_bytes(b"sample"), "x86_64-linux")
    }

    fn cache_arc(c: MemoryDerivationCache) -> Arc<dyn DerivationCacheBackend> {
        Arc::new(c)
    }

    #[tokio::test]
    async fn fake_backend_records_build_calls() {
        let b = FakeBuildBackend::new();
        let drv = sample_drv();
        b.build(&drv).await.unwrap();
        assert_eq!(b.builds().await.len(), 1);
        assert_eq!(b.builds().await[0], drv.drv_hash);
    }

    #[tokio::test]
    async fn fake_backend_produces_single_out_when_no_outputs_declared() {
        let b = FakeBuildBackend::new();
        let r = b.build(&sample_drv()).await.unwrap();
        assert_eq!(r.realisations.len(), 1);
        assert_eq!(r.realisations[0].output_name, "out");
        assert_eq!(r.nars.len(), 1);
    }

    #[tokio::test]
    async fn fake_backend_respects_declared_outputs() {
        let mut drv = sample_drv();
        drv.outputs.insert("out".into(), OutputPath::new(""));
        drv.outputs.insert("dev".into(), OutputPath::new(""));
        let b = FakeBuildBackend::new();
        let r = b.build(&drv).await.unwrap();
        assert_eq!(r.realisations.len(), 2);
        let names: Vec<_> = r.realisations.iter().map(|r| &r.output_name).collect();
        assert!(names.iter().any(|n| n.as_str() == "out"));
        assert!(names.iter().any(|n| n.as_str() == "dev"));
    }

    #[tokio::test]
    async fn fake_backend_nar_hashes_match_payload() {
        let b = FakeBuildBackend::new();
        let r = b.build(&sample_drv()).await.unwrap();
        for nar in &r.nars {
            let actual = NarHash::from_bytes(&nar.bytes);
            assert_eq!(actual, nar.hash);
        }
    }

    #[tokio::test]
    async fn fake_backend_fail_next_surfaces_error() {
        let b = FakeBuildBackend::new();
        b.fail_next("oops").await;
        let err = b.build(&sample_drv()).await.unwrap_err();
        assert_eq!(err.kind(), "backend");
    }

    #[tokio::test]
    async fn ingest_writes_nars_and_realisations() {
        let cache = cache_arc(MemoryDerivationCache::new());
        let drv = sample_drv();
        let result = BuildResult::single_output(
            drv.drv_hash.clone(),
            "out",
            OutputPath::new("/nix/store/abc-out"),
            b"hello-nar".to_vec(),
        );
        DrvBuildController::ingest(&cache, &result).await.unwrap();
        let nar = cache.get_nar(&result.nars[0].hash).await.unwrap();
        assert!(nar.is_some());
        let list = cache.list_realisations(&drv.drv_hash).await.unwrap();
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn build_result_single_output_builds_typed_realisation() {
        let drv_hash = DrvHash::from_bytes(b"x");
        let r = BuildResult::single_output(
            drv_hash.clone(),
            "out",
            OutputPath::new("/nix/store/a-out"),
            b"nar-bytes".to_vec(),
        );
        assert_eq!(r.realisations.len(), 1);
        assert_eq!(r.realisations[0].drv_hash, drv_hash);
        assert_eq!(r.realisations[0].output_name, "out");
        assert_eq!(r.nars.len(), 1);
        assert_eq!(r.realisations[0].nar_hash.as_ref(), Some(&r.nars[0].hash));
    }

    #[test]
    fn build_result_empty_is_zero_sized() {
        let r = BuildResult::empty();
        assert!(r.realisations.is_empty());
        assert!(r.nars.is_empty());
    }

    #[test]
    fn build_result_nar_index_maps_output_to_hash() {
        let drv_hash = DrvHash::from_bytes(b"x");
        let r = BuildResult::single_output(
            drv_hash,
            "out",
            OutputPath::new("/nix/store/a"),
            b"nar".to_vec(),
        );
        let idx = r.nar_index();
        assert_eq!(idx.len(), 1);
        assert!(idx.contains_key("out"));
    }

    #[test]
    fn build_error_kinds_are_stable() {
        assert_eq!(BuildError::Backend("x".into()).kind(), "backend");
        assert_eq!(BuildError::InvalidDrv("x".into()).kind(), "invalid_drv");
    }

    #[test]
    fn fake_backend_name_is_stable() {
        assert_eq!(FakeBuildBackend::new().name(), "fake");
    }

    #[test]
    fn controller_name_is_stable() {
        struct F;
        #[async_trait]
        impl Controller for F {
            fn name(&self) -> &'static str {
                "drv-build"
            }
            async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
                Ok(ReconcileReport::default().into())
            }
        }
        assert_eq!(F.name(), "drv-build");
    }
}
