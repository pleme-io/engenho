//! The fence around a binary made of mocks.
//!
//! `engenho-fonte` wires an in-process attester, publisher, proposer
//! and remediation handler. Cargo must build it only when the caller
//! names the `mock-universe` feature, so a plain `cargo build`,
//! `cargo install` or `cargo run --bin engenho-fonte` never yields a
//! process that looks like a converging daemon.
//!
//! Cargo enforces the fence when it reads the manifest; this test pins
//! the three manifest facts it depends on. It is a gate over the
//! manifest, not a type.

use toml::Value;

const FEATURE: &str = "mock-universe";

fn manifest() -> Value {
    let text = include_str!("../Cargo.toml");
    toml::from_str(text).expect("engenho-fonte-cli/Cargo.toml parses")
}

#[test]
fn every_binary_requires_the_mock_universe_feature() {
    let manifest = manifest();
    let bins = manifest
        .get("bin")
        .and_then(Value::as_array)
        .expect("engenho-fonte-cli declares its [[bin]] explicitly");
    assert!(!bins.is_empty(), "no [[bin]] table");
    for bin in bins {
        let name = bin.get("name").and_then(Value::as_str).unwrap_or("?");
        let required: Vec<&str> = bin
            .get("required-features")
            .and_then(Value::as_array)
            .map(|features| features.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        assert!(
            required.contains(&FEATURE),
            "[[bin]] {name} must carry required-features = [\"{FEATURE}\"], has {required:?}"
        );
    }
}

#[test]
fn the_mock_universe_feature_is_declared() {
    let manifest = manifest();
    let declared = manifest
        .get("features")
        .and_then(|features| features.get(FEATURE))
        .is_some();
    assert!(declared, "[features] must declare {FEATURE}");
}

#[test]
fn the_mock_universe_feature_is_never_on_by_default() {
    let manifest = manifest();
    let default: Vec<&str> = manifest
        .get("features")
        .and_then(|features| features.get("default"))
        .and_then(Value::as_array)
        .map(|features| features.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    assert!(
        !default.contains(&FEATURE),
        "{FEATURE} in default features turns the fence off: {default:?}"
    );
}
