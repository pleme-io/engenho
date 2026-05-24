//! Verification: parse_nix (gated `with-sui-eval`) parses a typed
//! Sistema from a Nix expression — proves the sui-bytecode upstream
//! fix unblocks the full Sistema-from-Nix pipeline.

#![cfg(feature = "with-sui-eval")]

use engenho_fonte::{InfraBackend, PromessaKind, parse_nix};

#[test]
fn parse_nix_loads_full_sistema_attrset() {
    let nix = r#"{
        name = "rio-cluster";
        apps = [
            { name = "podinfo"; version = "6.4.1"; }
            { name = "lilitu";  version = null; }
        ];
        infra = [
            { name = "rio-net"; backend = "magma"; }
            { name = "rio-dns"; backend = "pangea"; }
        ];
        promises = [
            { name = "sla";  kind = "availability"; target = 99.99; }
            { name = "cost"; kind = "budget";       target = 5000.0; }
        ];
        topology = { strategy = "quorum_3m"; nodes = 3; };
    }"#;
    let s = parse_nix(nix).expect("parse_nix should succeed");
    assert_eq!(&*s.name, "rio-cluster");
    assert_eq!(s.apps.len(), 2);
    assert_eq!(&*s.apps[0].name, "podinfo");
    assert_eq!(s.apps[0].version.as_deref(), Some("6.4.1"));
    assert!(s.apps[1].version.is_none());
    assert_eq!(s.infra.len(), 2);
    assert_eq!(s.infra[0].backend, InfraBackend::Magma);
    assert_eq!(s.infra[1].backend, InfraBackend::Pangea);
    assert_eq!(s.promises.len(), 2);
    assert_eq!(s.promises[0].kind, PromessaKind::Availability);
    assert_eq!(s.promises[1].kind, PromessaKind::Budget);
    assert_eq!(&*s.topology.strategy, "quorum_3m");
    assert_eq!(s.topology.nodes, 3);
}

#[test]
fn parse_nix_with_let_binding_works() {
    // sui's lazy evaluation: let-bindings are thunks; parse_nix
    // forces them via TypescapeValue's bridge.
    let nix = r#"
        let
            cluster = "rio";
        in {
            name = cluster;
            apps = [];
            infra = [];
            promises = [];
            topology = { strategy = "solo"; nodes = 1; };
        }
    "#;
    let s = parse_nix(nix).expect("parse_nix should resolve let-bindings");
    assert_eq!(&*s.name, "rio");
}

#[test]
fn parse_nix_with_function_application_works() {
    let nix = r#"
        let
            makeTopology = nodes: { strategy = "quorum_3m"; inherit nodes; };
        in {
            name = "rio";
            apps = [];
            infra = [];
            promises = [];
            topology = makeTopology 3;
        }
    "#;
    let s = parse_nix(nix).expect("parse_nix should resolve function application");
    assert_eq!(s.topology.nodes, 3);
}
