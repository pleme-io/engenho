//! ★ T5.6: the core is tokio-free, and the proof is its dependency set.
//!
//! A module of this crate can only `use` a crate this manifest names, so
//! "no module here reaches an async runtime" is exactly "no async runtime is
//! a library dependency". This test reads the manifest back and fails the
//! day one appears, under its own name or renamed with `package = ...`.
//! Dev-dependencies are not checked: a test may drive the core from an
//! executor without the library ever seeing one.

use toml::Value;

const MANIFEST: &str = include_str!("../Cargo.toml");

/// Crates that are, or exist to drive, an async runtime.
const ASYNC_RUNTIME: [&str; 6] = [
    "tokio",
    "async-trait",
    "futures",
    "async-std",
    "smol",
    "tokio-util",
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
fn the_core_links_no_async_runtime() {
    let manifest: Value = toml::from_str(MANIFEST).expect("Cargo.toml parses");
    let deps = library_dependencies(&manifest);
    assert!(
        !deps.is_empty(),
        "read no dependencies at all; the manifest shape changed under this test"
    );
    let runtime: Vec<&String> = deps
        .iter()
        .filter(|d| ASYNC_RUNTIME.contains(&d.as_str()))
        .collect();
    assert!(
        runtime.is_empty(),
        "engenho-substrate-core must stay tokio-free; move the module that needs {runtime:?} to engenho-substrate"
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
