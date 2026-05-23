//! Tests for the (defsistema) TataraDomain registration —
//! parse_tlisp round-trips against to_authoring_form so the lisp
//! surface stays bidirectional.

#![cfg(feature = "with-tatara-lisp")]

use engenho_fonte::{
    InfraBackend, PromessaKind, Sistema, SistemaBuilder, parse_tlisp, to_authoring_form,
};
use tatara_lisp::TataraDomain;

#[test]
fn defsistema_keyword_is_registered() {
    assert_eq!(Sistema::KEYWORD, "defsistema");
}

#[test]
fn minimal_defsistema_parses() {
    let src = r#"(defsistema "rio" :topology (topology "solo" :nodes 1))"#;
    let s = parse_tlisp(src).unwrap();
    assert_eq!(&*s.name, "rio");
    assert!(s.apps.is_empty());
    assert!(s.infra.is_empty());
    assert!(s.promises.is_empty());
    assert_eq!(&*s.topology.strategy, "solo");
    assert_eq!(s.topology.nodes, 1);
}

#[test]
fn full_defsistema_with_every_section_parses() {
    let src = r#"(defsistema "rio-cluster"
  :apps     ((appref "podinfo" :version "6.4.1") (appref "lilitu"))
  :infra    ((inframagma "rio-net") (infrapangea "rio-dns") (infracrossplane "rio-xp"))
  :promises ((promessaref "sla"  :kind :availability :target 99.99)
             (promessaref "cost" :kind :budget       :target 5000))
  :topology (topology "quorum_3m" :nodes 3))"#;
    let s = parse_tlisp(src).unwrap();
    assert_eq!(&*s.name, "rio-cluster");
    assert_eq!(s.apps.len(), 2);
    assert_eq!(&*s.apps[0].name, "podinfo");
    assert_eq!(s.apps[0].version.as_deref(), Some("6.4.1"));
    assert!(s.apps[1].version.is_none());
    assert_eq!(s.infra.len(), 3);
    assert_eq!(s.infra[0].backend, InfraBackend::Magma);
    assert_eq!(s.infra[1].backend, InfraBackend::Pangea);
    assert_eq!(s.infra[2].backend, InfraBackend::Crossplane);
    assert_eq!(s.promises.len(), 2);
    assert_eq!(s.promises[0].kind, PromessaKind::Availability);
    assert!((s.promises[0].target - 99.99).abs() < 1e-9);
    assert_eq!(s.promises[1].kind, PromessaKind::Budget);
    assert_eq!(&*s.topology.strategy, "quorum_3m");
    assert_eq!(s.topology.nodes, 3);
}

#[test]
fn unknown_kwarg_surfaces_typed_error() {
    let src = r#"(defsistema "rio" :unknown 1 :topology (topology "solo" :nodes 1))"#;
    let err = parse_tlisp(src).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("unknown") || msg.contains("Unknown"),
        "got: {msg}"
    );
}

#[test]
fn malformed_infra_head_surfaces_typed_error() {
    let src = r#"(defsistema "rio"
  :infra ((infraunknown "x"))
  :topology (topology "solo" :nodes 1))"#;
    let err = parse_tlisp(src).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("infra"), "got: {msg}");
}

#[test]
fn unknown_promessa_kind_surfaces_typed_error() {
    let src = r#"(defsistema "rio"
  :promises ((promessaref "x" :kind :unknown-kind :target 1.0))
  :topology (topology "solo" :nodes 1))"#;
    let err = parse_tlisp(src).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("unknown promessa kind"), "got: {msg}");
}

#[test]
fn round_trip_authoring_form_parses_back() {
    // Build a Sistema, render to authoring form, parse it back, assert equal.
    let s = SistemaBuilder::new("rio")
        .app("podinfo", Some("6.4.1"))
        .infra("net", InfraBackend::Magma)
        .promessa("sla", PromessaKind::Availability, 99.99)
        .topology("solo", 1)
        .build();
    let rendered = to_authoring_form(&s);
    let parsed = parse_tlisp(&rendered).expect(&rendered);
    assert_eq!(parsed, s);
}
