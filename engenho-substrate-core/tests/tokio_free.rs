//! ★ T5.6: the core is tokio-free, and the proof is its dependency set.
//!
//! A module of this crate can only `use` a crate this manifest names, so
//! "no module here reaches an async runtime" is exactly "no async runtime is
//! a library dependency". This test reads the manifest back and fails the
//! day one appears, under its own name or renamed with `package = ...`.
//! Dev-dependencies are not checked: a test may drive the core from an
//! executor without the library ever seeing one.
//!
//! This is the DIRECT ban — what this manifest itself names. The core could
//! satisfy it and still carry a runtime by taking one edge to a workspace
//! crate that has it, so `engenho/tests/shipped_closure.rs` bans the same
//! crates transitively over the whole closure. Two questions, two tests.

use toml::Value;

const MANIFEST: &str = include_str!("../Cargo.toml");

/// What the core must not link, and why. The runtime crates are T5.6's
/// carve: this half of the substrate is the one no executor reaches.
/// `serde_yaml` is the same line drawn at config — parsing a config format
/// is engenho-config's job, and the core sits below it.
const FORBIDDEN: [(&str, &str); 7] = [
    ("tokio", "an async runtime"),
    ("async-trait", "exists to drive an async runtime"),
    ("futures", "exists to drive an async runtime"),
    ("async-std", "an async runtime"),
    ("smol", "an async runtime"),
    ("tokio-util", "exists to drive an async runtime"),
    (
        "serde_yaml",
        "a config format; that is engenho-config's layer",
    ),
];

/// Every crate the library links: `[dependencies]`, `[build-dependencies]`
/// and each `[target.*.dependencies]`, by the package name the entry
/// resolves to.
fn library_dependencies(manifest: &Value) -> Vec<String> {
    let mut tables: Vec<&Value> = ["dependencies", "build-dependencies"]
        .iter()
        .filter_map(|k| manifest.get(*k))
        .collect();
    if let Some(targets) = manifest.get("target").and_then(Value::as_table) {
        for target in targets.values() {
            tables.extend(
                ["dependencies", "build-dependencies"]
                    .iter()
                    .filter_map(|k| target.get(*k)),
            );
        }
    }
    let mut names = Vec::new();
    for table in tables {
        let table = table.as_table().expect("a dependency table is a table");
        for (key, spec) in table {
            let package = spec
                .get("package")
                .and_then(Value::as_str)
                .unwrap_or(key.as_str());
            names.push(package.to_owned());
        }
    }
    names
}

#[test]
fn the_core_links_no_async_runtime_and_no_config_format() {
    let manifest: Value = toml::from_str(MANIFEST).expect("Cargo.toml parses");
    let deps = library_dependencies(&manifest);
    assert!(
        !deps.is_empty(),
        "read no dependencies at all; the manifest shape changed under this test"
    );
    let carried: Vec<(&str, &str)> = FORBIDDEN
        .iter()
        .copied()
        .filter(|(name, _)| deps.iter().any(|d| d == name))
        .collect();
    assert!(
        carried.is_empty(),
        "engenho-substrate-core links {carried:?}; move the module that needs it to engenho-substrate, the leaf"
    );
}

/// The reader above must see a renamed runtime too, or the gate is vacuous.
#[test]
fn a_renamed_runtime_is_still_seen() {
    let manifest: Value = toml::from_str(
        r#"
        [dependencies]
        rt = { package = "tokio", version = "1" }
        [target.'cfg(unix)'.dependencies]
        futures = "0.3"
        "#,
    )
    .unwrap();
    let deps = library_dependencies(&manifest);
    assert_eq!(deps, vec!["tokio".to_owned(), "futures".to_owned()]);
}
