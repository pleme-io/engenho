//! Property: WorkloadShape::shape_hash is deterministic + diverges
//! per (shape, drv_hash_hex) pair.

use engenho_substrate::WorkloadShape;
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;

fn shape_strategy() -> impl Strategy<Value = WorkloadShape> {
    prop_oneof![
        Just(WorkloadShape::OciImage),
        Just(WorkloadShape::NixClosure),
        Just(WorkloadShape::Qcow2),
        Just(WorkloadShape::Wasm),
        Just(WorkloadShape::HelmChart),
        "[a-z0-9_-]{1,32}".prop_map(|name| WorkloadShape::Custom { name }),
        "[a-z0-9_-]{4,32}".prop_map(|triple| WorkloadShape::StaticBinary { triple }),
    ]
}

fn drv_hex_strategy() -> impl Strategy<Value = String> {
    "[0-9a-f]{64}".prop_map(|s| s)
}

proptest_with_env! {
    /// Same shape + same drv_hash_hex → byte-identical shape_hash.
    #[test]
    fn shape_hash_is_deterministic(
        shape in shape_strategy(),
        drv_hex in drv_hex_strategy(),
    ) {
        let h1 = shape.shape_hash(&drv_hex);
        let h2 = shape.shape_hash(&drv_hex);
        prop_assert_eq!(h1, h2);
    }

    /// Different shape, same drv_hex → different shape_hash.
    #[test]
    fn shape_hash_diverges_per_shape(
        s1 in shape_strategy(),
        s2 in shape_strategy(),
        drv_hex in drv_hex_strategy(),
    ) {
        prop_assume!(s1.tag() != s2.tag());
        prop_assert_ne!(s1.shape_hash(&drv_hex), s2.shape_hash(&drv_hex));
    }

    /// Same shape, different drv_hex → different shape_hash.
    #[test]
    fn shape_hash_diverges_per_drv(
        shape in shape_strategy(),
        d1 in drv_hex_strategy(),
        d2 in drv_hex_strategy(),
    ) {
        prop_assume!(d1 != d2);
        prop_assert_ne!(shape.shape_hash(&d1), shape.shape_hash(&d2));
    }

    /// Tag is always snake_case OR snake_case:param (sanity).
    #[test]
    fn tag_is_snake_case_or_parametric(shape in shape_strategy()) {
        let tag = shape.tag();
        for c in tag.chars() {
            prop_assert!(
                c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == ':' || c == '-',
                "tag {tag} contains invalid char {c}"
            );
        }
    }

    /// Round-trip via serde_json preserves equality.
    #[test]
    fn serde_round_trip(shape in shape_strategy()) {
        let bytes = serde_json::to_vec(&shape).unwrap();
        let back: WorkloadShape = serde_json::from_slice(&bytes).unwrap();
        prop_assert_eq!(back, shape);
    }
}
