//! BuildBackendRoceiro — production Roceiro that composes
//! BuildBackend + DerivationCacheBackend + Verifier into one
//! materializer surface.
//!
//! ## Reconcile rule (per Stage on per Node)
//!
//! 1. Build the drv referenced by the stage's shape via BuildBackend.
//!    (For now the stage's `shape` is the witness, and the actual
//!    drv hash is the substrate's existing typed primitive — we
//!    derive a synthetic drv per stage so the test path doesn't
//!    require a full sui integration. Production wires the real
//!    Drv via Stage extension fields.)
//! 2. Ingest the BuildResult's NARs + realisations into the cache.
//! 3. Run every Verificacao in stage.verify through the Verifier.
//! 4. Emit a typed MaterializationReceipt the ledger consumes.
//!
//! ## Bridging Stage → Drv
//!
//! This commit ships the composition layer with a synthetic
//! drv-per-stage (BLAKE3 of stage_id). Future Stage extension:
//! a `drv_hash: Option<DrvHash>` field that the operator pins
//! explicitly. Until then, every test uses the synthetic.

use std::sync::Arc;

use async_trait::async_trait;
use engenho_substrate::{
    DerivationCacheBackend, Drv, DrvHash, MaterializationReceipt, NodeId, ReceiptKind, Stage,
    Verifier,
};

use crate::drv_build::BuildBackend;
use crate::roceiro::{Roceiro, RoceiroError};

/// Production Roceiro — composes three existing trait families.
pub struct BuildBackendRoceiro {
    name: &'static str,
    build_backend: Arc<dyn BuildBackend>,
    cache: Arc<dyn DerivationCacheBackend>,
    verifier: Arc<dyn Verifier>,
}

impl BuildBackendRoceiro {
    /// New roceiro with the given build / cache / verifier backends.
    /// `name` is the telemetry label (e.g. "sui-build-podman-cache").
    #[must_use]
    pub fn new(
        name: &'static str,
        build_backend: Arc<dyn BuildBackend>,
        cache: Arc<dyn DerivationCacheBackend>,
        verifier: Arc<dyn Verifier>,
    ) -> Self {
        Self {
            name,
            build_backend,
            cache,
            verifier,
        }
    }

    /// Convenience constructor with the default name "build-backend".
    #[must_use]
    pub fn default_named(
        build_backend: Arc<dyn BuildBackend>,
        cache: Arc<dyn DerivationCacheBackend>,
        verifier: Arc<dyn Verifier>,
    ) -> Self {
        Self::new("build-backend", build_backend, cache, verifier)
    }

    /// Derive the synthetic drv for a stage (transitional — replaced
    /// when Stage gets an explicit `drv_hash` field).
    #[must_use]
    pub fn synthetic_drv(stage: &Stage) -> Drv {
        let hash = DrvHash::from_bytes(stage.id.as_str().as_bytes());
        Drv::synthetic(hash, "x86_64-linux")
    }
}

#[async_trait]
impl Roceiro for BuildBackendRoceiro {
    fn name(&self) -> &'static str {
        self.name
    }

    async fn materialize(
        &self,
        stage: &Stage,
        node: NodeId,
    ) -> Result<MaterializationReceipt, RoceiroError> {
        let drv = Self::synthetic_drv(stage);

        // 1. Build the drv.
        let result = self
            .build_backend
            .build(&drv)
            .await
            .map_err(|e| RoceiroError::Backend(format!("build: {e}")))?;

        // 2. Ingest NARs + realisations into the cache.
        for nar in &result.nars {
            self.cache
                .put_nar(nar)
                .await
                .map_err(|e| RoceiroError::Backend(format!("cache put_nar: {e}")))?;
        }
        for r in &result.realisations {
            self.cache
                .put_realisation(r)
                .await
                .map_err(|e| RoceiroError::Backend(format!("cache put_realisation: {e}")))?;
        }

        // 3. Run each Verificacao in stage.verify; first failure
        //    short-circuits with VerificationDenied.
        let subject = *blake3::hash(stage.id.as_str().as_bytes()).as_bytes();
        for verificacao in &stage.verify {
            self.verifier
                .verify(verificacao, subject, node, 0)
                .await
                .map_err(|e| RoceiroError::VerificationDenied {
                    stage: stage.id.clone(),
                    detail: e.to_string(),
                })?;
        }

        // 4. Emit the typed MaterializationReceipt. Evidence hashes
        //    the realisations' nar_hashes so identical materialization
        //    across nodes produces identical receipts (faithful nodes
        //    agree; QuorumTracker reaches Reached).
        let mut composed: Vec<u8> = Vec::new();
        for r in &result.realisations {
            composed.extend_from_slice(r.output_name.as_bytes());
            if let Some(h) = &r.nar_hash {
                composed.extend_from_slice(&h.0);
            }
        }
        let evidence = *blake3::hash(&composed).as_bytes();
        Ok(MaterializationReceipt::new(
            ReceiptKind::Shape(stage.shape.tag()),
            subject,
            node,
            0,
            evidence,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::drv_build::FakeBuildBackend;
    use engenho_substrate::{FakeVerifier, MemoryDerivationCache, Verificacao, WorkloadShape};

    fn n(b: u8) -> NodeId {
        NodeId::new([b; 32])
    }

    fn stage(id: &str) -> Stage {
        Stage::pinned(id, WorkloadShape::OciImage, n(1))
    }

    fn arc_build() -> Arc<dyn BuildBackend> {
        Arc::new(FakeBuildBackend::new())
    }

    fn arc_cache() -> Arc<dyn DerivationCacheBackend> {
        Arc::new(MemoryDerivationCache::new())
    }

    fn arc_verifier() -> Arc<dyn Verifier> {
        Arc::new(FakeVerifier::new())
    }

    #[tokio::test]
    async fn materialize_succeeds_for_simple_stage() {
        let r = BuildBackendRoceiro::default_named(arc_build(), arc_cache(), arc_verifier());
        let receipt = r.materialize(&stage("x"), n(1)).await.unwrap();
        match receipt.kind {
            ReceiptKind::Shape(tag) => assert_eq!(tag, "oci_image"),
            _ => panic!("expected Shape variant"),
        }
        assert_eq!(receipt.emitter, n(1));
    }

    #[tokio::test]
    async fn build_failure_surfaces_as_backend_error() {
        let bad = Arc::new(FakeBuildBackend::new());
        bad.fail_next("kaboom").await;
        let r = BuildBackendRoceiro::default_named(bad, arc_cache(), arc_verifier());
        let err = r.materialize(&stage("x"), n(1)).await.unwrap_err();
        assert_eq!(err.kind(), "backend");
        assert!(err.to_string().contains("build"));
    }

    #[tokio::test]
    async fn verifier_failure_surfaces_as_verification_denied() {
        let v = Arc::new(FakeVerifier::new());
        v.set_policy("hash_equality", false).await;
        let mut st = stage("x");
        st.verify.push(Verificacao::HashEquality {
            expected: engenho_substrate::NarHash::from_bytes(b"x"),
        });
        let r =
            BuildBackendRoceiro::default_named(arc_build(), arc_cache(), v as Arc<dyn Verifier>);
        let err = r.materialize(&st, n(1)).await.unwrap_err();
        assert_eq!(err.kind(), "verification_denied");
    }

    #[tokio::test]
    async fn cache_receives_nars_and_realisations() {
        let cache = Arc::new(MemoryDerivationCache::new());
        let r = BuildBackendRoceiro::default_named(arc_build(), cache.clone(), arc_verifier());
        r.materialize(&stage("x"), n(1)).await.unwrap();
        // FakeBuildBackend produces one synthetic NAR per output;
        // default = 1 ("out") since no outputs declared.
        assert_eq!(cache.nar_count().await, 1);
    }

    #[tokio::test]
    async fn faithful_nodes_produce_identical_receipts() {
        let r = BuildBackendRoceiro::default_named(arc_build(), arc_cache(), arc_verifier());
        let r1 = r.materialize(&stage("x"), n(1)).await.unwrap();
        let r2 = r.materialize(&stage("x"), n(2)).await.unwrap();
        // Faithful build → identical evidence on different nodes.
        // (Quorum reaches Reached, not Dissent.)
        assert_eq!(r1.evidence_hash, r2.evidence_hash);
        assert_eq!(r1.subject, r2.subject);
        assert_ne!(r1.emitter, r2.emitter);
    }

    #[tokio::test]
    async fn no_verify_predicates_skips_verifier() {
        // Verifier set to deny everything — but stage has no
        // Verificacao entries, so no calls.
        let v = Arc::new(FakeVerifier::new());
        v.set_policy("hash_equality", false).await;
        let r = BuildBackendRoceiro::default_named(
            arc_build(),
            arc_cache(),
            v.clone() as Arc<dyn Verifier>,
        );
        // Stage with no verify predicates.
        let _ = r.materialize(&stage("x"), n(1)).await.unwrap();
        // Verifier never called.
        assert_eq!(v.calls().await.len(), 0);
    }

    #[tokio::test]
    async fn multiple_verify_predicates_all_evaluated() {
        let v = Arc::new(FakeVerifier::new());
        let mut st = stage("x");
        st.verify.push(Verificacao::HashEquality {
            expected: engenho_substrate::NarHash::from_bytes(b"a"),
        });
        st.verify
            .push(Verificacao::CrossNodeAgreement { quorum: 3 });
        st.verify.push(Verificacao::TameshiSigned {
            signer: "engenho-pki".into(),
        });
        let r = BuildBackendRoceiro::default_named(
            arc_build(),
            arc_cache(),
            v.clone() as Arc<dyn Verifier>,
        );
        r.materialize(&st, n(1)).await.unwrap();
        // Every predicate should have been verified.
        assert_eq!(v.calls().await.len(), 3);
    }

    #[tokio::test]
    async fn synthetic_drv_uses_stage_id_as_hash_source() {
        let s = stage("hello");
        let drv = BuildBackendRoceiro::synthetic_drv(&s);
        let expected = DrvHash::from_bytes(b"hello");
        assert_eq!(drv.drv_hash, expected);
    }

    #[tokio::test]
    async fn name_passes_through_from_constructor() {
        let r =
            BuildBackendRoceiro::new("custom-roceiro", arc_build(), arc_cache(), arc_verifier());
        assert_eq!(r.name(), "custom-roceiro");
    }

    #[tokio::test]
    async fn default_name_is_build_backend() {
        let r = BuildBackendRoceiro::default_named(arc_build(), arc_cache(), arc_verifier());
        assert_eq!(r.name(), "build-backend");
    }
}
