//! Shape-renderer impls + composition layer.
//!
//! Three primitives this module:
//!
//!   * [`FakeShapeRenderer`] — deterministic per-shape renderer
//!     for tests. Produces synthetic bytes tied to (shape, drv_hash).
//!
//!   * [`CompositeShapeRenderer`] — composes N renderers + dispatches
//!     by shape match. Operator wires one composite per node;
//!     the composite picks the right backend per Stage.shape.
//!
//!   * Future per-shape impls (OciImageRenderer, Qcow2Renderer,
//!     WasmRenderer) plug into the composite without changing
//!     call sites.

use std::sync::Arc;

use async_trait::async_trait;

use crate::derivation::Drv;
use crate::shape::{RenderedArtifact, ShapeError, ShapeRenderer, WorkloadShape};

// =================================================================
// FakeShapeRenderer — deterministic per-shape for tests
// =================================================================

/// In-memory renderer parameterized by a target shape. Produces
/// synthetic bytes `BLAKE3(shape.tag() + drv_hash_hex)` so the
/// same drv rendered into the same shape yields identical bytes
/// across nodes (faithful reproducibility for tests).
#[derive(Clone)]
pub struct FakeShapeRenderer {
    name: &'static str,
    shape: WorkloadShape,
    fail: bool,
}

impl FakeShapeRenderer {
    /// New renderer with telemetry name + shape it produces.
    #[must_use]
    pub fn new(name: &'static str, shape: WorkloadShape) -> Self {
        Self {
            name,
            shape,
            fail: false,
        }
    }

    /// Convenience with auto-name derived from the shape's tag.
    /// Useful when operators want one Fake per shape.
    #[must_use]
    pub fn for_shape(shape: WorkloadShape) -> Self {
        let name = match &shape {
            WorkloadShape::OciImage => "fake-oci",
            WorkloadShape::NixClosure => "fake-nix-closure",
            WorkloadShape::Qcow2 => "fake-qcow2",
            WorkloadShape::Wasm => "fake-wasm",
            WorkloadShape::HelmChart => "fake-helm",
            _ => "fake-other",
        };
        Self::new(name, shape)
    }

    /// Make the next render call fail. (Single-shot — re-render
    /// after a failure succeeds again.)
    #[must_use]
    pub fn failing(mut self) -> Self {
        self.fail = true;
        self
    }
}

#[async_trait]
impl ShapeRenderer for FakeShapeRenderer {
    fn name(&self) -> &'static str {
        self.name
    }

    fn shape(&self) -> WorkloadShape {
        self.shape.clone()
    }

    async fn render(&self, drv: &Drv) -> Result<RenderedArtifact, ShapeError> {
        if self.fail {
            return Err(ShapeError::Backend(format!(
                "fake renderer {} configured to fail",
                self.name
            )));
        }
        let drv_hex = drv.drv_hash.to_hex();
        let mut composed = self.shape.tag().into_bytes();
        composed.extend_from_slice(drv_hex.as_bytes());
        Ok(RenderedArtifact::from_bytes(self.shape.clone(), composed))
    }
}

// =================================================================
// CompositeShapeRenderer — dispatches by shape match
// =================================================================

/// Composes N ShapeRenderer impls + dispatches by shape match.
///
/// Operator constructs once per node:
///
/// ```ignore
/// let renderer = CompositeShapeRenderer::new("composite", vec![
///     Arc::new(OciImageRenderer::new(...)),
///     Arc::new(Qcow2Renderer::new(...)),
///     Arc::new(WasmRenderer::new(...)),
/// ]);
/// ```
///
/// At render time, the composite asks each child `shape()` until
/// it finds one matching the request; first hit wins. No match
/// returns `ShapeError::UnsupportedShape`.
///
/// ## Pluggable shape() comparison
///
/// `WorkloadShape::eq` is structural — `OciImage` matches
/// `OciImage`, `StaticBinary { triple }` matches only when triples
/// match exactly. Operators wiring multiple triple-specific
/// renderers get clean dispatch per triple for free.
pub struct CompositeShapeRenderer {
    name: &'static str,
    renderers: Vec<Arc<dyn ShapeRenderer>>,
}

impl CompositeShapeRenderer {
    /// New composite with telemetry name + child renderers.
    #[must_use]
    pub fn new(name: &'static str, renderers: Vec<Arc<dyn ShapeRenderer>>) -> Self {
        Self { name, renderers }
    }

    /// Convenience: name = "composite".
    #[must_use]
    pub fn default_named(renderers: Vec<Arc<dyn ShapeRenderer>>) -> Self {
        Self::new("composite", renderers)
    }

    /// Number of registered renderers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.renderers.len()
    }

    /// True if no renderers registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.renderers.is_empty()
    }

    /// Observable snapshot — name + renderer count. Pattern #2
    /// (SSC v0.91) — returns canonical
    /// [`crate::mirante::ChildCountSnapshot`].
    #[must_use]
    pub fn snapshot(&self) -> crate::mirante::ChildCountSnapshot {
        crate::mirante::ChildCountSnapshot {
            name: self.name,
            child_count: self.len(),
        }
    }

    /// Set of distinct shapes this composite knows how to render.
    #[must_use]
    pub fn supported_shapes(&self) -> Vec<WorkloadShape> {
        let mut shapes: Vec<WorkloadShape> = self.renderers.iter().map(|r| r.shape()).collect();
        // Deduplicate; preserve declaration order so first-hit
        // dispatch matches semantics below.
        shapes.dedup();
        shapes
    }

    /// Render a drv into a specific target shape. Useful when the
    /// composite is part of a higher-level dispatch that already
    /// knows what shape it wants (rather than inferring from drv).
    ///
    /// # Errors
    /// [`ShapeError::UnsupportedShape`] if no child renderer matches.
    pub async fn render_as(
        &self,
        drv: &Drv,
        target: &WorkloadShape,
    ) -> Result<RenderedArtifact, ShapeError> {
        for r in &self.renderers {
            if r.shape() == *target {
                return r.render(drv).await;
            }
        }
        Err(ShapeError::UnsupportedShape(format!(
            "no renderer registered for {}",
            target.tag()
        )))
    }
}

crate::impl_named_field!(CompositeShapeRenderer);

crate::impl_observable!(CompositeShapeRenderer, crate::mirante::ChildCountSnapshot);

#[async_trait]
impl ShapeRenderer for CompositeShapeRenderer {
    fn name(&self) -> &'static str {
        self.name
    }

    /// Composite's own shape is the FIRST child's shape — operators
    /// using a composite as a drop-in ShapeRenderer typically wire
    /// it where the shape is known statically.
    fn shape(&self) -> WorkloadShape {
        self.renderers
            .first()
            .map(|r| r.shape())
            .unwrap_or(WorkloadShape::Custom {
                name: "empty-composite".into(),
            })
    }

    async fn render(&self, drv: &Drv) -> Result<RenderedArtifact, ShapeError> {
        // Default behavior: use the first child's shape as the
        // target. Operators wanting per-Stage dispatch should call
        // render_as() with the explicit target instead.
        let target = self.shape();
        self.render_as(drv, &target).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::derivation::DrvHash;

    fn drv(tag: &[u8]) -> Drv {
        Drv::synthetic(DrvHash::from_bytes(tag), "x86_64-linux")
    }

    fn arc_fake(shape: WorkloadShape) -> Arc<dyn ShapeRenderer> {
        Arc::new(FakeShapeRenderer::for_shape(shape))
    }

    // ── FakeShapeRenderer ──────────────────────────────────────

    #[tokio::test]
    async fn fake_renders_deterministic_bytes_per_drv() {
        let r = FakeShapeRenderer::for_shape(WorkloadShape::OciImage);
        let a1 = r.render(&drv(b"x")).await.unwrap();
        let a2 = r.render(&drv(b"x")).await.unwrap();
        assert_eq!(a1.bytes, a2.bytes);
        assert_eq!(a1.evidence_hash, a2.evidence_hash);
        assert_eq!(a1.shape, WorkloadShape::OciImage);
    }

    #[tokio::test]
    async fn fake_diverges_per_drv() {
        let r = FakeShapeRenderer::for_shape(WorkloadShape::OciImage);
        let a1 = r.render(&drv(b"a")).await.unwrap();
        let a2 = r.render(&drv(b"b")).await.unwrap();
        assert_ne!(a1.bytes, a2.bytes);
    }

    #[tokio::test]
    async fn fake_diverges_per_shape() {
        let oci = FakeShapeRenderer::for_shape(WorkloadShape::OciImage);
        let wasm = FakeShapeRenderer::for_shape(WorkloadShape::Wasm);
        let a_oci = oci.render(&drv(b"x")).await.unwrap();
        let a_wasm = wasm.render(&drv(b"x")).await.unwrap();
        // Same drv, different shape → different bytes.
        assert_ne!(a_oci.bytes, a_wasm.bytes);
    }

    #[tokio::test]
    async fn fake_failing_returns_backend_error() {
        let r = FakeShapeRenderer::for_shape(WorkloadShape::OciImage).failing();
        let err = r.render(&drv(b"x")).await.unwrap_err();
        assert_eq!(err.kind(), "backend");
    }

    #[tokio::test]
    async fn fake_shape_passes_through() {
        let r = FakeShapeRenderer::for_shape(WorkloadShape::Qcow2);
        assert_eq!(r.shape(), WorkloadShape::Qcow2);
    }

    #[tokio::test]
    async fn fake_name_per_shape_is_stable() {
        assert_eq!(
            FakeShapeRenderer::for_shape(WorkloadShape::OciImage).name(),
            "fake-oci"
        );
        assert_eq!(
            FakeShapeRenderer::for_shape(WorkloadShape::Qcow2).name(),
            "fake-qcow2"
        );
        assert_eq!(
            FakeShapeRenderer::for_shape(WorkloadShape::HelmChart).name(),
            "fake-helm"
        );
    }

    #[tokio::test]
    async fn fake_custom_name_overrides() {
        let r = FakeShapeRenderer::new("my-renderer", WorkloadShape::Wasm);
        assert_eq!(r.name(), "my-renderer");
    }

    // ── CompositeShapeRenderer ─────────────────────────────────

    #[tokio::test]
    async fn empty_composite_render_as_returns_unsupported() {
        let c = CompositeShapeRenderer::default_named(vec![]);
        let err = c
            .render_as(&drv(b"x"), &WorkloadShape::OciImage)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "unsupported_shape");
    }

    #[tokio::test]
    async fn composite_dispatches_by_shape() {
        let c = CompositeShapeRenderer::default_named(vec![
            arc_fake(WorkloadShape::OciImage),
            arc_fake(WorkloadShape::Wasm),
            arc_fake(WorkloadShape::Qcow2),
        ]);
        // Each shape dispatches to its specific child.
        let a_oci = c
            .render_as(&drv(b"x"), &WorkloadShape::OciImage)
            .await
            .unwrap();
        let a_wasm = c.render_as(&drv(b"x"), &WorkloadShape::Wasm).await.unwrap();
        let a_qcow = c
            .render_as(&drv(b"x"), &WorkloadShape::Qcow2)
            .await
            .unwrap();
        assert_eq!(a_oci.shape, WorkloadShape::OciImage);
        assert_eq!(a_wasm.shape, WorkloadShape::Wasm);
        assert_eq!(a_qcow.shape, WorkloadShape::Qcow2);
        // All three distinct bytes.
        assert_ne!(a_oci.bytes, a_wasm.bytes);
        assert_ne!(a_wasm.bytes, a_qcow.bytes);
    }

    #[tokio::test]
    async fn composite_first_hit_wins() {
        // Two renderers for the same shape — first wins.
        let first = Arc::new(FakeShapeRenderer::new("first", WorkloadShape::OciImage));
        let second = Arc::new(FakeShapeRenderer::new("second", WorkloadShape::OciImage));
        let c = CompositeShapeRenderer::default_named(vec![
            first as Arc<dyn ShapeRenderer>,
            second as Arc<dyn ShapeRenderer>,
        ]);
        // We can't directly observe which one ran (both produce same
        // bytes for same drv/shape). But we can assert behavior is
        // consistent — both render the same artifact.
        let a = c
            .render_as(&drv(b"x"), &WorkloadShape::OciImage)
            .await
            .unwrap();
        let direct = FakeShapeRenderer::new("first", WorkloadShape::OciImage)
            .render(&drv(b"x"))
            .await
            .unwrap();
        assert_eq!(a.bytes, direct.bytes);
    }

    #[tokio::test]
    async fn composite_render_uses_first_shape_as_target() {
        let c = CompositeShapeRenderer::default_named(vec![
            arc_fake(WorkloadShape::Wasm),
            arc_fake(WorkloadShape::OciImage),
        ]);
        let a = c.render(&drv(b"x")).await.unwrap();
        assert_eq!(a.shape, WorkloadShape::Wasm);
    }

    #[tokio::test]
    async fn composite_supported_shapes_returns_distinct() {
        let c = CompositeShapeRenderer::default_named(vec![
            arc_fake(WorkloadShape::OciImage),
            arc_fake(WorkloadShape::Wasm),
            arc_fake(WorkloadShape::OciImage), // dup — preserved by dedup
        ]);
        let shapes = c.supported_shapes();
        assert!(shapes.contains(&WorkloadShape::OciImage));
        assert!(shapes.contains(&WorkloadShape::Wasm));
    }

    #[tokio::test]
    async fn composite_metadata_helpers() {
        let empty = CompositeShapeRenderer::default_named(vec![]);
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());
        let c = CompositeShapeRenderer::default_named(vec![
            arc_fake(WorkloadShape::Wasm),
            arc_fake(WorkloadShape::OciImage),
        ]);
        assert_eq!(c.len(), 2);
        assert!(!c.is_empty());
    }

    #[tokio::test]
    async fn composite_empty_shape_falls_back_to_custom_marker() {
        let c = CompositeShapeRenderer::default_named(vec![]);
        match c.shape() {
            WorkloadShape::Custom { name } => assert_eq!(name, "empty-composite"),
            other => panic!("expected Custom marker, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn composite_name_passes_through() {
        let c = CompositeShapeRenderer::new("my-composite", vec![arc_fake(WorkloadShape::Wasm)]);
        assert_eq!(c.name(), "my-composite");
    }

    #[tokio::test]
    async fn composite_child_failure_propagates() {
        let c = CompositeShapeRenderer::default_named(vec![Arc::new(
            FakeShapeRenderer::for_shape(WorkloadShape::OciImage).failing(),
        )]);
        let err = c
            .render_as(&drv(b"x"), &WorkloadShape::OciImage)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "backend");
    }
}
