//! T0.10 — `engenho census`, run as the built binary.
//!
//! The census library is pinned in engenho-runtime (`src/census/tests.rs`,
//! `tests/t0_10_census.rs`). This file pins the CLI contract around it: the
//! catalog is printable, a report goes to stdout alone, and a census that
//! could not read its source fails with a non-zero exit and prints no report
//! — so a script cannot mistake "could not look" for "found nothing".

use std::process::{Command, Output};

use engenho_runtime::census::Predicate;

fn engenho(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_engenho"))
        .args(args)
        .output()
        .expect("run the engenho binary")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// `census --list` prints one line per predicate, name first, and exits 0.
#[test]
fn census_list_prints_the_whole_catalog() {
    let out = engenho(&["census", "--list"]);
    assert!(out.status.success(), "stderr: {}", text(&out.stderr));
    let stdout = text(&out.stdout);
    assert_eq!(stdout.lines().count(), Predicate::ALL.len(), "{stdout}");
    for p in Predicate::ALL {
        assert!(
            stdout.lines().any(|l| l.starts_with(p.name())),
            "the catalog omits {p}: {stdout}"
        );
    }
}

/// A name outside the catalog is refused before any source is touched.
#[test]
fn census_refuses_an_unknown_predicate() {
    let out = engenho(&[
        "census",
        "--predicate",
        "everything",
        "--data-dir",
        "/nonexistent",
    ]);
    assert!(!out.status.success());
    assert!(out.stdout.is_empty(), "stdout: {}", text(&out.stdout));
    let stderr = text(&out.stderr);
    assert!(stderr.contains("no predicate \"everything\""), "{stderr}");
}

/// A data directory with no store fails the census: non-zero, no report.
#[test]
fn census_over_a_directory_without_a_store_fails_without_a_report() {
    let empty = tempfile::tempdir().expect("tempdir");
    let dir = empty.path().to_str().expect("a UTF-8 temp path");
    let out = engenho(&[
        "census",
        "--predicate",
        "stored-would-reject",
        "--data-dir",
        dir,
    ]);
    assert!(!out.status.success(), "stdout: {}", text(&out.stdout));
    assert!(
        out.stdout.is_empty(),
        "a census that could not read printed a report: {}",
        text(&out.stdout)
    );
    let stderr = text(&out.stderr);
    assert!(stderr.contains("no store directory"), "{stderr}");
}
