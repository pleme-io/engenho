//! ★ sub-core-01: one home for the "enum + `ALL` from one variant list"
//! shape, and a census that stops a fourth from being written.
//!
//! The shape was derived three separate times before this crate held it —
//! `engenho-controllers`, the scheduler's `filter_plugins!` (which added
//! `name()`), and a test-local copy in `engenho-apiserver`. None of them
//! coordinated, which is what makes the shape forced rather than merely
//! convenient, and `engenho-config` could reach none of them, so
//! `KubeletBackendKind` still carries a hand-written list.
//!
//! This test walks every `.rs` file in the workspace, reads back the name of
//! every declarative macro each one defines, and holds two facts:
//!
//!   1. **The positive control.** `engenho-substrate-core/src/closed_enum.rs`
//!      defines `closed_enum`. Without this, a walk that found nothing — a
//!      renamed file, a changed layout, a skip rule that ate the tree —
//!      would satisfy every other assertion by finding zero of everything,
//!      which is an empty result read as a verdict.
//!   2. **The ceiling.** Every OTHER definition of `closed_enum` or
//!      `filter_plugins` is one of the three in [`KNOWN_UNMIGRATED`]. A new
//!      one fails here.
//!
//! The ceiling is deliberately one-sided: a row disappearing is the
//! migration landing, and must not turn this red. Emptying the list is the
//! deferred half of sub-core-01 — those three crates belong to other lanes.
//!
//! Tier: a gate, not a type. Nothing stops a fourth copy being *written*;
//! this fails the build when one is. The shape is only unrepresentable for a
//! crate that uses the macro, where `ALL` and the variant list are one list.

use std::path::{Path, PathBuf};

/// Definitions that predate this crate's home for the macro, each with the
/// crate that must be edited to retire it. Every one is outside sub-core-01's
/// file boundary, so the migration is deferred, not forgotten.
const KNOWN_UNMIGRATED: &[(&str, &str)] = &[
    ("engenho-controllers/src/closed_enum.rs", "closed_enum"),
    ("engenho-scheduler/src/filter.rs", "filter_plugins"),
    (
        "engenho-apiserver/tests/w1_one_write_pipeline.rs",
        "closed_enum",
    ),
];

/// The macro names that must have exactly one home.
const CENSUSED: [&str; 2] = ["closed_enum", "filter_plugins"];

/// Where the one home is.
const HOME: &str = "engenho-substrate-core/src/closed_enum.rs";

/// Directories the walk never enters: build output, git's object store, and
/// anything vendored — none of them is workspace source anyone edits.
const SKIP_DIRS: [&str; 4] = ["target", ".git", "vendor", "node_modules"];

/// The workspace root: this crate's manifest directory's parent.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the crate directory has a parent")
        .to_path_buf()
}

/// Every `.rs` file under `dir`, recursively.
fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if path.is_dir() {
            if !SKIP_DIRS.contains(&name.as_ref()) {
                rust_files(&path, out);
            }
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// The keyword that opens a declarative-macro definition, assembled at
/// runtime so that THIS file contains no text the scan below would match.
/// A scan whose own source is a hit reports itself and teaches nothing.
fn definition_keyword() -> String {
    let mut kw = String::from("macro_");
    kw.push_str("rules");
    kw
}

/// The name of every declarative macro `src` defines.
///
/// Reads the name rather than matching a fixed string, so a definition
/// written with unusual spacing is still seen: the scan skips whitespace
/// around the `!` instead of assuming one space.
fn macros_defined(src: &str) -> Vec<String> {
    let kw = definition_keyword();
    let mut names = Vec::new();
    let mut rest = src;
    while let Some(at) = rest.find(&kw) {
        let after = &rest[at + kw.len()..];
        rest = after;
        let Some(after) = after.trim_start().strip_prefix('!') else {
            continue;
        };
        let name: String = after
            .trim_start()
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if !name.is_empty() {
            names.push(name);
        }
    }
    names
}

/// Every (workspace-relative path, macro name) pair for the censused macros.
fn census() -> Vec<(String, String)> {
    let root = workspace_root();
    let mut files = Vec::new();
    rust_files(&root, &mut files);
    assert!(
        files.len() > 100,
        "walked {} .rs files; the workspace layout changed under this test",
        files.len()
    );

    let mut found = Vec::new();
    for file in files {
        let Ok(src) = std::fs::read_to_string(&file) else {
            continue;
        };
        for name in macros_defined(&src) {
            if CENSUSED.contains(&name.as_str()) {
                let rel = file
                    .strip_prefix(&root)
                    .unwrap_or(&file)
                    .to_string_lossy()
                    .replace('\\', "/");
                found.push((rel, name));
            }
        }
    }
    found
}

/// The positive control. Every other assertion here is about what the scan
/// does NOT find, and an absence only means something once the scan is known
/// to find what is there.
#[test]
fn the_scan_finds_the_macro_in_its_home() {
    let found = census();
    assert!(
        found
            .iter()
            .any(|(path, name)| path == HOME && name == "closed_enum"),
        "the scan did not find closed_enum in {HOME}; it found {found:?}"
    );
}

/// The scan reads a name it is not looking for, so a renamed copy of the
/// shape is still a hit rather than silently absent.
#[test]
fn the_scan_reads_the_name_and_not_a_fixed_string() {
    let kw = definition_keyword();
    let src = format!("{kw}!closed_enum {{ }}\n{kw}  !  filter_plugins {{ }}\nfn f() {{}}");
    assert_eq!(macros_defined(&src), vec!["closed_enum", "filter_plugins"]);
}

/// The ceiling: no definition beyond the home and the three known copies.
#[test]
fn no_crate_defines_a_fourth_copy_of_the_shape() {
    let found = census();
    let unexpected: Vec<&(String, String)> = found
        .iter()
        .filter(|(path, name)| {
            !(path == HOME && name == "closed_enum")
                && !KNOWN_UNMIGRATED
                    .iter()
                    .any(|(p, n)| *p == path && *n == name)
        })
        .collect();
    assert!(
        unexpected.is_empty(),
        "{unexpected:?} define the enum-plus-ALL shape again; use \
         engenho_substrate_core::closed_enum! (its #[named] arm also \
         generates name()) instead of writing a fourth copy"
    );
}
