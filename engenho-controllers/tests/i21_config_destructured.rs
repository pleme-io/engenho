//! I21 (plan T5.8): every `*Config` struct engenho-controllers defines,
//! and every `engenho_config::*Config` it consumes, is destructured where
//! it is consumed, with no `..`.
//!
//! ## What is a type, and what is only a gate
//!
//! * The destructure is the type-level part. At the consuming site every
//!   field is bound by name, so a field added to the struct does not
//!   compile there (E0027) until someone decides what the consumer does
//!   with it. A config knob that nothing reads cannot ship silently.
//! * That the destructure EXISTS is only this test: a lexical CI gate over
//!   the crate's shipped sources. Field access (`config.debounce`) reads
//!   what it wants and says nothing about the rest; replacing a
//!   destructure with it compiles, and this test is what goes red.
//! * A field discarded as `field: _` still compiles. Only review catches
//!   that, which is why each discard in this crate carries its reason.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Every `.rs` file under `dir`, recursively, in a stable order.
fn rust_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

/// The shipped part of a source file: its unit-test module is cut off (a
/// destructure in a test does not bind the production consumer), and line
/// comments are dropped (a destructure in prose binds nothing).
fn shipped(source: &str) -> String {
    let body = source
        .find("\n#[cfg(test)]\nmod tests")
        .map_or(source, |at| &source[..at]);
    body.lines()
        .map(|line| line.find("//").map_or(line, |at| &line[..at]))
        .collect::<Vec<_>>()
        .join("\n")
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// The identifier starting at `s`'s first byte.
fn ident(s: &str) -> &str {
    let end = s.find(|c: char| !is_ident_char(c)).unwrap_or(s.len());
    &s[..end]
}

/// Names of the `*Config` structs `source` defines.
fn defined_configs(source: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for (at, _) in source.match_indices("struct ") {
        let before = source[..at].chars().next_back();
        if before.is_some_and(is_ident_char) {
            continue;
        }
        let name = ident(&source[at + "struct ".len()..]);
        if name.ends_with("Config") {
            out.insert(name.to_string());
        }
    }
    out
}

/// Names of the `engenho_config::*Config` types `source` names.
fn foreign_configs(source: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for (at, _) in source.match_indices("engenho_config::") {
        let name = ident(&source[at + "engenho_config::".len()..]);
        if name.ends_with("Config") {
            out.insert(name.to_string());
        }
    }
    out
}

/// The body of each `let [path::]Name { … } =` pattern in `source`, from
/// the brace after `Name` to its match (nested braces included).
fn destructures<'a>(source: &'a str, name: &str) -> Vec<&'a str> {
    let mut out = Vec::new();
    for (at, _) in source.match_indices("let ") {
        let rest = &source[at + "let ".len()..];
        // An optional path, then the name, then `{`.
        let path_len = rest
            .find(|c: char| !(is_ident_char(c) || c == ':'))
            .unwrap_or(rest.len());
        let path = &rest[..path_len];
        if path.rsplit("::").next() != Some(name) {
            continue;
        }
        let after = rest[path_len..].trim_start();
        if !after.starts_with('{') {
            continue;
        }
        let mut depth = 0usize;
        let mut close = None;
        for (i, c) in after.char_indices() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        close = Some(i);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(close) = close else { continue };
        let tail = after[close + 1..].trim_start();
        if tail.starts_with('=') && !tail.starts_with("==") {
            out.push(&after[..=close]);
        }
    }
    out
}

/// Whether `source` destructures `name` somewhere with no `..` rest.
fn destructured_exhaustively(source: &str, name: &str) -> bool {
    destructures(source, name)
        .iter()
        .any(|pattern| !pattern.contains(".."))
}

fn crate_sources() -> String {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    rust_files(&src)
        .iter()
        .map(|p| shipped(&std::fs::read_to_string(p).unwrap()))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn every_config_struct_is_destructured_where_it_is_consumed() {
    let sources = crate_sources();
    let configs: BTreeSet<String> = defined_configs(&sources)
        .into_iter()
        .chain(foreign_configs(&sources))
        .collect();

    // A scan that sees nothing passes vacuously: the driver's config is
    // defined here, so a blind scanner is a red test, not a green one.
    assert!(
        configs.contains("WatchDriverConfig"),
        "the scan found no WatchDriverConfig definition, so it read nothing: {configs:?}"
    );

    let missing: Vec<&String> = configs
        .iter()
        .filter(|name| !destructured_exhaustively(&sources, name))
        .collect();
    assert!(
        missing.is_empty(),
        "these config structs are consumed without a `let Name {{ .. }} = ` destructure that \
         names every field (no `..`), so a field added to one would compile unread: {missing:?}"
    );
}

/// The gate's own red run: the scanner rejects the shapes it exists to
/// catch, and accepts the one it exists to require.
#[test]
fn the_scanner_tells_an_exhaustive_destructure_from_the_shapes_it_rejects() {
    let exhaustive = "let FooConfig { a, b: Bar { c, d }, e: _ } = config;";
    let rest = "let FooConfig { a, .. } = config;";
    let field_access = "let a = config.a; let FooConfig = 1;";
    let struct_literal = "let x = FooConfig { a, ..Default::default() };";
    let comparison = "let FooConfig { a } == b;";
    let other_name = "let BarFooConfig { a } = config;";
    let pathed = "let crate::m::FooConfig { a } = config;";

    assert!(destructured_exhaustively(exhaustive, "FooConfig"));
    assert!(destructured_exhaustively(pathed, "FooConfig"));
    for rejected in [rest, field_access, struct_literal, comparison, other_name] {
        assert!(
            !destructured_exhaustively(rejected, "FooConfig"),
            "accepted a non-exhaustive shape: {rejected}"
        );
    }

    // Prose and unit tests do not count as the consumer.
    let in_comment = "// let FooConfig { a } = config;\nfn f() {}";
    assert!(!destructured_exhaustively(
        &shipped(in_comment),
        "FooConfig"
    ));
    let in_test = "fn f() {}\n#[cfg(test)]\nmod tests {\n let FooConfig { a } = c;\n}";
    assert!(!destructured_exhaustively(&shipped(in_test), "FooConfig"));

    assert_eq!(
        defined_configs("pub struct FooConfig {}\nstruct NotAConfigHolder;\nstruct BazConfig;"),
        BTreeSet::from(["BazConfig".to_string(), "FooConfig".to_string()])
    );
    assert_eq!(
        foreign_configs("impl From<&engenho_config::ControllersConfig> for X {}"),
        BTreeSet::from(["ControllersConfig".to_string()])
    );
}
