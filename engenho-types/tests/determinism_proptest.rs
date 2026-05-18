//! Determinism property tests — the §VI.4 contract surfaced as proptest.
//!
//! Per theory/ENGENHO.md §VI.4, all SSA-merge / serialization paths must
//! be byte-deterministic given identical inputs. Upstream Go's
//! strategic-merge-patch has known order-dependent edge cases driven by
//! Go map iteration order; engenho avoids this by `BTreeMap`-everywhere.
//!
//! These proptest cases generate arbitrary `ObjectMeta` instances,
//! serialize them, and assert the result is order-stable. Failing this
//! test means a hand-edit somewhere swapped `BTreeMap` for `HashMap` —
//! a regression that L0 (the compiler) cannot catch, but proptest does.

use std::collections::BTreeMap;

use engenho_types::meta::ObjectMeta;
use proptest::collection::btree_map;
use proptest::prelude::*;

/// Arbitrary ObjectMeta strategy — generates valid instances over the
/// shape every K8s namespaced resource carries.
fn arb_object_meta() -> impl Strategy<Value = ObjectMeta> {
    (
        ".{0,253}",                                          // name
        proptest::option::of(".{1,63}"),                     // namespace
        ".{0,32}",                                           // resource_version
        proptest::option::of(".{32,36}"),                    // uid
        btree_map(".{1,32}", ".{0,63}", 0..8),               // labels
        btree_map(".{1,32}", ".{0,1024}", 0..8),             // annotations
        proptest::collection::vec(".{1,253}", 0..4),         // finalizers
        proptest::option::of(0i64..1_000_000i64),            // generation
    )
        .prop_map(|(name, ns, rv, uid, labels, annotations, finalizers, generation)| {
            let mut m = ObjectMeta {
                name,
                namespace: ns,
                resource_version: rv,
                uid,
                labels: BTreeMap::new(),
                annotations: BTreeMap::new(),
                finalizers,
                generation,
                ..Default::default()
            };
            m.labels = labels;
            m.annotations = annotations;
            m
        })
}

proptest! {
    /// Serialize → deserialize → reserialize → byte-equal.
    ///
    /// If any internal map switched from BTreeMap to HashMap, the second
    /// serialization would differ from the first by key ordering. The
    /// equality check fails immediately.
    #[test]
    fn object_meta_serialization_is_byte_deterministic(meta in arb_object_meta()) {
        let first  = serde_json::to_vec(&meta).expect("first serialize");
        let back: ObjectMeta = serde_json::from_slice(&first).expect("deserialize");
        let second = serde_json::to_vec(&back).expect("second serialize");
        prop_assert_eq!(first, second);
    }

    /// Serialize → deserialize → equal — the round-trip identity.
    ///
    /// A weaker check than byte-determinism, but catches semantic loss
    /// (a field accidentally dropped on serialize, or a default re-emitted
    /// where it should be skipped).
    #[test]
    fn object_meta_round_trips_through_json(meta in arb_object_meta()) {
        let json = serde_json::to_string(&meta).expect("serialize");
        let back: ObjectMeta = serde_json::from_str(&json).expect("deserialize");
        prop_assert_eq!(meta, back);
    }

    /// Label / annotation order is monotonically ascending in the
    /// serialized form — locked via the canonical `serde_json::Value` path.
    ///
    /// Strategy: parse the serialized output back as a `serde_json::Value`,
    /// extract the labels object, and verify the keys appear in ascending
    /// order. `serde_json::Value::Object` preserves insertion order (via
    /// `serde_json::Map` when the `preserve_order` feature isn't on, the
    /// crate uses `BTreeMap`-equivalent ordering anyway via `IndexMap`'s
    /// default). Either way, ordering reflects what the serializer
    /// emitted — which for a `BTreeMap` source must be ascending.
    #[test]
    fn label_keys_serialize_in_ascending_order(meta in arb_object_meta()) {
        let json = serde_json::to_string(&meta).expect("serialize");
        let value: serde_json::Value = serde_json::from_str(&json).expect("re-parse");

        if let Some(labels) = value.get("labels").and_then(|v| v.as_object()) {
            let keys: Vec<&str> = labels.keys().map(String::as_str).collect();
            for window in keys.windows(2) {
                prop_assert!(
                    window[0] <= window[1],
                    "label keys not in ascending order: {:?} then {:?}",
                    window[0], window[1]
                );
            }
        }
        if let Some(annotations) = value.get("annotations").and_then(|v| v.as_object()) {
            let keys: Vec<&str> = annotations.keys().map(String::as_str).collect();
            for window in keys.windows(2) {
                prop_assert!(
                    window[0] <= window[1],
                    "annotation keys not in ascending order: {:?} then {:?}",
                    window[0], window[1]
                );
            }
        }
    }
}
