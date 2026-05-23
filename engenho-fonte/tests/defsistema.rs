//! Tests for the defsistema authoring surface.

use engenho_fonte::{
    AppRef, InfraBackend, InfraRef, PromessaKind, PromessaRef, Sistema, SistemaBuilder,
    TopologyRef, parse_json, to_authoring_form,
};

#[test]
fn builder_constructs_typed_sistema() {
    let s = SistemaBuilder::new("rio")
        .app("podinfo", Some("6.4.1"))
        .app("lilitu", None::<String>)
        .infra("rio-net", InfraBackend::Magma)
        .infra("rio-dns", InfraBackend::Pangea)
        .promessa("sla", PromessaKind::Availability, 99.99)
        .topology("quorum_3m", 3)
        .build();
    assert_eq!(&*s.name, "rio");
    assert_eq!(s.apps.len(), 2);
    assert_eq!(s.infra.len(), 2);
    assert_eq!(s.promises.len(), 1);
    assert_eq!(s.topology.nodes, 3);
}

#[test]
fn parse_json_round_trips_via_builder() {
    let s1 = SistemaBuilder::new("a")
        .app("x", None::<String>)
        .topology("solo", 1)
        .build();

    let json = r#"{
        "name": "a",
        "apps": [{"name": "x", "version": null}],
        "infra": [],
        "promises": [],
        "topology": {"strategy": "solo", "nodes": 1}
    }"#;
    let s2 = parse_json(json).unwrap();
    assert_eq!(s1, s2);
}

#[test]
fn parse_json_surfaces_typed_error_on_malformed() {
    let err = parse_json("not json {").unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("fonte/eval"), "got: {msg}");
}

#[test]
fn to_authoring_form_emits_canonical_defsistema() {
    let s = SistemaBuilder::new("rio")
        .app("podinfo", Some("6.4.1"))
        .infra("net", InfraBackend::Magma)
        .promessa("sla", PromessaKind::Availability, 99.99)
        .topology("solo", 1)
        .build();
    let lisp = to_authoring_form(&s);
    assert!(lisp.contains("(defsistema \"rio\""));
    assert!(lisp.contains("(appref \"podinfo\" :version \"6.4.1\")"));
    assert!(lisp.contains("(inframagma \"net\")"));
    assert!(lisp.contains(":kind :availability"));
    assert!(lisp.contains(":target 99.99"));
    assert!(lisp.contains(":nodes 1"));
}

#[test]
fn to_authoring_form_empty_sistema_renders_minimal() {
    let s = SistemaBuilder::new("empty").topology("solo", 1).build();
    let lisp = to_authoring_form(&s);
    assert!(lisp.contains("(defsistema \"empty\""));
    assert!(lisp.contains(":topology"));
    assert!(!lisp.contains(":apps"));
    assert!(!lisp.contains(":infra"));
    assert!(!lisp.contains(":promises"));
}

#[test]
fn all_three_backends_render_with_distinct_keywords() {
    let s = SistemaBuilder::new("multi")
        .infra("a", InfraBackend::Magma)
        .infra("b", InfraBackend::Pangea)
        .infra("c", InfraBackend::Crossplane)
        .topology("solo", 1)
        .build();
    let lisp = to_authoring_form(&s);
    assert!(lisp.contains("(inframagma \"a\")"));
    assert!(lisp.contains("(infrapangea \"b\")"));
    assert!(lisp.contains("(infracrossplane \"c\")"));
}

#[test]
fn fluent_builder_chains_typed_struct_fields() {
    // Verify the typed Sistema constructed from the builder matches
    // a hand-rolled Sistema literal.
    let built = SistemaBuilder::new("x").app("a", None::<String>).build();
    let hand = Sistema {
        name: "x".into(),
        apps: vec![AppRef {
            name: "a".into(),
            version: None,
        }],
        infra: vec![],
        promises: vec![],
        topology: TopologyRef {
            strategy: "solo".into(),
            nodes: 1,
        },
    };
    assert_eq!(built, hand);
    // Suppress unused-trait-bound warning if Sistema's fields drift.
    let _: PromessaRef = PromessaRef {
        name: "noop".into(),
        kind: PromessaKind::Availability,
        target: 0.0,
    };
    let _: InfraRef = InfraRef {
        name: "noop".into(),
        backend: InfraBackend::Magma,
    };
}
