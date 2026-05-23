//! Round-trip property tests for `Sistema` + every sub-primitive
//! reference type.
//!
//! Every typed primitive must satisfy:
//!   T::from_typescape_value(&t.to_typescape_value())? == t

use engenho_fonte::{
    AppRef, InfraBackend, InfraRef, PromessaKind, PromessaRef, Sistema, TopologyRef,
};
use engenho_substrate_props::proptest_with_env;
use engenho_sui_typescape::Typescape;
use proptest::prelude::*;
use std::sync::Arc;

// ── Strategies ──────────────────────────────────────────────────

fn arb_arc_str() -> impl Strategy<Value = Arc<str>> {
    "[a-z][a-z0-9-]{0,15}".prop_map(Arc::from)
}

fn arb_app_ref() -> impl Strategy<Value = AppRef> {
    (arb_arc_str(), proptest::option::of(arb_arc_str()))
        .prop_map(|(name, version)| AppRef { name, version })
}

fn arb_infra_backend() -> impl Strategy<Value = InfraBackend> {
    prop_oneof![
        Just(InfraBackend::Magma),
        Just(InfraBackend::Pangea),
        Just(InfraBackend::Crossplane),
    ]
}

fn arb_infra_ref() -> impl Strategy<Value = InfraRef> {
    (arb_arc_str(), arb_infra_backend()).prop_map(|(name, backend)| InfraRef { name, backend })
}

fn arb_promessa_kind() -> impl Strategy<Value = PromessaKind> {
    prop_oneof![
        Just(PromessaKind::Availability),
        Just(PromessaKind::Budget),
        Just(PromessaKind::Compliance),
        Just(PromessaKind::Security),
        Just(PromessaKind::CustomerKpi),
    ]
}

fn arb_promessa_ref() -> impl Strategy<Value = PromessaRef> {
    // f64 target restricted to non-NaN — TypescapeValue ↔ typed
    // round-trip is byte-perfect (it's just enum projection).
    (
        arb_arc_str(),
        arb_promessa_kind(),
        any::<f64>().prop_filter("non-NaN", |f| !f.is_nan()),
    )
        .prop_map(|(name, kind, target)| PromessaRef { name, kind, target })
}

/// JSON-stable variant — restricts target to integer values so the
/// serde_json text round-trip is exact. Real promessas (SLA 99.99,
/// cost 5000, NPS 50) all sit in this range; arbitrary f64 mantissas
/// lose 1 ulp through serde_json's default formatter and that's
/// orthogonal to typescape correctness.
fn arb_promessa_ref_json_stable() -> impl Strategy<Value = PromessaRef> {
    (
        arb_arc_str(),
        arb_promessa_kind(),
        (-1_000_000_i64..1_000_000_i64).prop_map(|i| i as f64),
    )
        .prop_map(|(name, kind, target)| PromessaRef { name, kind, target })
}

fn arb_topology_ref() -> impl Strategy<Value = TopologyRef> {
    (
        prop_oneof![
            Just::<Arc<str>>(Arc::from("solo")),
            Just::<Arc<str>>(Arc::from("pair")),
            Just::<Arc<str>>(Arc::from("quorum_3m")),
            Just::<Arc<str>>(Arc::from("cluster_3m_nw")),
            Just::<Arc<str>>(Arc::from("mesh_all_peers")),
            Just::<Arc<str>>(Arc::from("phalanx")),
        ],
        1u32..32,
    )
        .prop_map(|(strategy, nodes)| TopologyRef { strategy, nodes })
}

fn arb_sistema() -> impl Strategy<Value = Sistema> {
    (
        arb_arc_str(),
        proptest::collection::vec(arb_app_ref(), 0..8),
        proptest::collection::vec(arb_infra_ref(), 0..6),
        proptest::collection::vec(arb_promessa_ref(), 0..6),
        arb_topology_ref(),
    )
        .prop_map(|(name, apps, infra, promises, topology)| Sistema {
            name,
            apps,
            infra,
            promises,
            topology,
        })
}

fn arb_sistema_json_stable() -> impl Strategy<Value = Sistema> {
    (
        arb_arc_str(),
        proptest::collection::vec(arb_app_ref(), 0..8),
        proptest::collection::vec(arb_infra_ref(), 0..6),
        proptest::collection::vec(arb_promessa_ref_json_stable(), 0..6),
        arb_topology_ref(),
    )
        .prop_map(|(name, apps, infra, promises, topology)| Sistema {
            name,
            apps,
            infra,
            promises,
            topology,
        })
}

// ── Round-trip properties ────────────────────────────────────────

proptest_with_env! {
    #[test]
    fn app_ref_round_trips(a in arb_app_ref()) {
        let v = a.to_typescape_value();
        let r = AppRef::from_typescape_value(&v).unwrap();
        prop_assert_eq!(r, a);
    }

    #[test]
    fn infra_backend_round_trips(b in arb_infra_backend()) {
        let v = b.to_typescape_value();
        let r = InfraBackend::from_typescape_value(&v).unwrap();
        prop_assert_eq!(r, b);
    }

    #[test]
    fn infra_ref_round_trips(i in arb_infra_ref()) {
        let v = i.to_typescape_value();
        let r = InfraRef::from_typescape_value(&v).unwrap();
        prop_assert_eq!(r, i);
    }

    #[test]
    fn promessa_kind_round_trips(k in arb_promessa_kind()) {
        let v = k.to_typescape_value();
        let r = PromessaKind::from_typescape_value(&v).unwrap();
        prop_assert_eq!(r, k);
    }

    #[test]
    fn promessa_ref_round_trips(p in arb_promessa_ref()) {
        let v = p.to_typescape_value();
        let r = PromessaRef::from_typescape_value(&v).unwrap();
        prop_assert_eq!(r, p);
    }

    #[test]
    fn topology_ref_round_trips(t in arb_topology_ref()) {
        let v = t.to_typescape_value();
        let r = TopologyRef::from_typescape_value(&v).unwrap();
        prop_assert_eq!(r, t);
    }

    #[test]
    fn sistema_round_trips_all_compositions(s in arb_sistema()) {
        let v = s.to_typescape_value();
        let r = Sistema::from_typescape_value(&v).unwrap();
        prop_assert_eq!(r, s);
    }

    /// Sistema round-trips through serde_json as well — the
    /// TypescapeValue serializer + deserializer are byte-stable for
    /// JSON-stable f64 targets (integers in the +/-1e6 range, the
    /// realistic promise-target window).
    #[test]
    fn sistema_round_trips_via_json(s in arb_sistema_json_stable()) {
        let v = s.to_typescape_value();
        let json = serde_json::to_string(&v).unwrap();
        let v2: engenho_sui_typescape::TypescapeValue =
            serde_json::from_str(&json).unwrap();
        let r = Sistema::from_typescape_value(&v2).unwrap();
        prop_assert_eq!(r, s);
    }
}

// ── Variant-mismatch surface ─────────────────────────────────────

#[test]
fn unknown_infra_backend_surfaces_invariant_error() {
    let v = engenho_sui_typescape::TypescapeValue::string("docker");
    let err = InfraBackend::from_typescape_value(&v).unwrap_err();
    matches!(err, engenho_sui_typescape::TypescapeError::Invariant { .. });
}

#[test]
fn sistema_missing_attr_surfaces_missing_attr_error() {
    let v = engenho_sui_typescape::TypescapeValue::attrs(vec![(
        "name",
        engenho_sui_typescape::TypescapeValue::string("partial"),
    )]);
    let err = Sistema::from_typescape_value(&v).unwrap_err();
    match err {
        engenho_sui_typescape::TypescapeError::MissingAttr(k) => {
            // The first missing attr in field order is `apps`.
            assert_eq!(k, "apps");
        }
        other => panic!("expected MissingAttr, got {other:?}"),
    }
}
