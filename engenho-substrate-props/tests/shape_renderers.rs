//! Property: FakeShapeRenderer + CompositeShapeRenderer invariants.

use engenho_substrate::{
    CompositeShapeRenderer, Drv, DrvHash, FakeShapeRenderer, ShapeError, ShapeRenderer,
    WorkloadShape,
};
use engenho_substrate_props::{block_on, proptest_with_env};
use proptest::prelude::*;
use std::sync::Arc;

fn drv(b: u8) -> Drv {
    Drv::synthetic(DrvHash::new([b; 32]), "x86_64-linux")
}

fn shape_strategy() -> impl Strategy<Value = WorkloadShape> {
    prop_oneof![
        Just(WorkloadShape::OciImage),
        Just(WorkloadShape::NixClosure),
        Just(WorkloadShape::Qcow2),
    ]
}

proptest_with_env! {
    /// FakeShapeRenderer.render returns an artifact when not failing.
    #[test]
    fn fake_renders_when_not_failing(hash_b in any::<u8>(), shape in shape_strategy()) {
        block_on(async {
            let r = FakeShapeRenderer::for_shape(shape.clone());
            let artifact = r.render(&drv(hash_b)).await.unwrap();
            assert_eq!(artifact.shape, shape);
        });
    }

    /// FakeShapeRenderer.failing() makes render() error.
    #[test]
    fn fake_failing_renderer_errors(hash_b in any::<u8>(), shape in shape_strategy()) {
        block_on(async {
            let r = FakeShapeRenderer::for_shape(shape).failing();
            let res = r.render(&drv(hash_b)).await;
            assert!(res.is_err());
        });
    }

    /// FakeShapeRenderer.shape() returns the configured shape.
    #[test]
    fn fake_shape_returns_configured(shape in shape_strategy()) {
        let r = FakeShapeRenderer::for_shape(shape.clone());
        assert_eq!(r.shape(), shape);
    }

    /// Empty composite reports zero len + is_empty.
    #[test]
    fn empty_composite_invariants(_seed in any::<u8>()) {
        let c = CompositeShapeRenderer::default_named(vec![]);
        assert_eq!(c.len(), 0);
        assert!(c.is_empty());
        assert!(c.supported_shapes().is_empty());
    }

    /// Composite.len() matches the input Vec count.
    #[test]
    fn composite_len_matches_input(n in 0usize..6, shape in shape_strategy()) {
        let renderers: Vec<Arc<dyn ShapeRenderer>> = (0..n)
            .map(|_| Arc::new(FakeShapeRenderer::for_shape(shape.clone())) as Arc<dyn ShapeRenderer>)
            .collect();
        let c = CompositeShapeRenderer::default_named(renderers);
        assert_eq!(c.len(), n);
        assert_eq!(c.is_empty(), n == 0);
    }

    /// Composite delegates to the first matching child for render_as.
    #[test]
    fn composite_render_as_dispatches_to_matching_shape(
        hash_b in any::<u8>(),
        target in shape_strategy(),
    ) {
        block_on(async {
            // Build a composite with one renderer for each of the 3 shapes.
            let renderers: Vec<Arc<dyn ShapeRenderer>> = vec![
                Arc::new(FakeShapeRenderer::for_shape(WorkloadShape::OciImage)),
                Arc::new(FakeShapeRenderer::for_shape(WorkloadShape::NixClosure)),
                Arc::new(FakeShapeRenderer::for_shape(WorkloadShape::Qcow2)),
            ];
            let c = CompositeShapeRenderer::default_named(renderers);
            let artifact = c.render_as(&drv(hash_b), &target).await.unwrap();
            assert_eq!(artifact.shape, target);
        });
    }

    /// Composite.render_as returns UnsupportedShape for unknown targets.
    #[test]
    fn composite_render_as_unknown_target_errors(hash_b in any::<u8>()) {
        block_on(async {
            // Composite has only OciImage renderer.
            let renderers: Vec<Arc<dyn ShapeRenderer>> = vec![
                Arc::new(FakeShapeRenderer::for_shape(WorkloadShape::OciImage)),
            ];
            let c = CompositeShapeRenderer::default_named(renderers);
            // Asking for Qcow2 → no match → UnsupportedShape.
            let custom = WorkloadShape::Custom {
                name: "qcow2-unknown".into(),
            };
            let err = c.render_as(&drv(hash_b), &custom).await.unwrap_err();
            assert!(matches!(err, ShapeError::UnsupportedShape(_)));
        });
    }

    /// Composite default render() uses first child's shape as target.
    #[test]
    fn composite_default_render_uses_first_child_shape(
        hash_b in any::<u8>(),
        first_shape in shape_strategy(),
        second_shape in shape_strategy(),
    ) {
        block_on(async {
            let renderers: Vec<Arc<dyn ShapeRenderer>> = vec![
                Arc::new(FakeShapeRenderer::for_shape(first_shape.clone())),
                Arc::new(FakeShapeRenderer::for_shape(second_shape)),
            ];
            let c = CompositeShapeRenderer::default_named(renderers);
            // Composite's own shape() == first child's shape.
            assert_eq!(c.shape(), first_shape);
            // render() targets that shape via render_as.
            let artifact = c.render(&drv(hash_b)).await.unwrap();
            assert_eq!(artifact.shape, first_shape);
        });
    }

    /// supported_shapes deduplicates + preserves declaration order.
    #[test]
    fn supported_shapes_dedups(hash_b in any::<u8>()) {
        // Three renderers with shapes: Oci, Oci, NixClosure — dedup → [Oci, NixClosure]
        let renderers: Vec<Arc<dyn ShapeRenderer>> = vec![
            Arc::new(FakeShapeRenderer::for_shape(WorkloadShape::OciImage)),
            Arc::new(FakeShapeRenderer::for_shape(WorkloadShape::OciImage)),
            Arc::new(FakeShapeRenderer::for_shape(WorkloadShape::NixClosure)),
        ];
        let _ = drv(hash_b); // unused but exercises the strategy
        let c = CompositeShapeRenderer::default_named(renderers);
        let shapes = c.supported_shapes();
        assert_eq!(shapes.len(), 2);
        assert_eq!(shapes[0], WorkloadShape::OciImage);
        assert_eq!(shapes[1], WorkloadShape::NixClosure);
    }
}
