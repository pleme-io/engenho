//! Verify engenho-revoada registers PlacementPolicy into the
//! fleet-wide DispatcherCatalog. engenho-revoada is the fabric
//! crate (distribution + placement) — sixth consumer class
//! adopting gen-platform's typed-dispatcher catamorphism.
//!
//! PlacementPolicy = where a workload may land. Five typed
//! variants: ZoneAware / RackAware / LatencyAware / Spread /
//! None. The substrate's typed shadow now spans cluster
//! placement decisions in addition to code supply, OTP
//! hot-upgrades, sandbox capabilities, secret materialization,
//! and scheduler failure handling.

use engenho_revoada::PlacementPolicy;
use gen_platform::{TypedDispatcherTrait, catalog};

#[test]
fn placement_policy_registers_into_fleet_catalog() {
    let entry = catalog::by_label("engenho.placement-policy")
        .expect("engenho-revoada must register PlacementPolicy");
    assert_eq!(entry.label, "engenho.placement-policy");
    assert_eq!((entry.variant_count)(), 5);
}

#[test]
fn variant_kinds_kebab() {
    let kinds = PlacementPolicy::variant_kinds();
    assert_eq!(
        kinds,
        vec![
            "zone-aware",
            "rack-aware",
            "latency-aware",
            "spread",
            "none"
        ]
    );
}

#[test]
fn variant_fields_surfaced() {
    let fields = PlacementPolicy::variant_fields();
    assert_eq!(
        fields,
        vec![
            ("zone-aware", vec!["min_zones"]),
            ("rack-aware", vec!["min_racks"]),
            ("latency-aware", vec!["max_p99_ms"]),
            ("spread", vec![]),
            ("none", vec![]),
        ]
    );
}

#[test]
fn variant_count_via_trait() {
    assert_eq!(PlacementPolicy::variant_count(), 5);
}
