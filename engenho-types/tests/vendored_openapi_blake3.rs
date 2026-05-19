//! Vendored OpenAPI v3 BLAKE3 manifest verification.
//!
//! Per theory/ENGENHO.md §VI.1, every byte the forge-gen → engenho-types
//! pipeline consumes is BLAKE3-attested. This test reads the manifest
//! and **recomputes** the BLAKE3 of every vendored file, asserting the
//! on-disk content matches the manifest declaration byte-for-byte.
//!
//! Failure mode: someone replaced an upstream schema without updating
//! the manifest. The diff surfaces here, in CI, at L1 of the test pyramid
//! (theory/ENGENHO.md §V.2 — unit tests).
//!
//! This is the seed verification that locks the input substrate before
//! M0.0.3 wires up the generator itself. After M0.0.3, the generator
//! consumes these schemas; before M0.0.3, only this test reads them.
//! The hash chain is identical in both phases.

use std::fs;
use std::path::PathBuf;

const VENDOR_DIR: &str = "vendor/openapi/v1.34.0";

#[derive(Debug, serde::Deserialize)]
struct Manifest {
    files: Vec<ManifestEntry>,
}

#[derive(Debug, serde::Deserialize)]
struct ManifestEntry {
    path:   String,
    blake3: String,
    bytes:  u64,
}

fn manifest_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(VENDOR_DIR).join("MANIFEST.yaml")
}

fn vendor_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(VENDOR_DIR).join(name)
}

fn load_manifest() -> Manifest {
    let raw = fs::read_to_string(manifest_path()).expect("read MANIFEST.yaml");
    serde_yaml::from_str(&raw).expect("parse MANIFEST.yaml")
}

#[test]
fn manifest_exists_and_parses() {
    let _ = load_manifest();
}

#[test]
fn every_vendored_file_present_with_declared_size() {
    let manifest = load_manifest();
    for entry in &manifest.files {
        let p = vendor_path(&entry.path);
        let meta = fs::metadata(&p).unwrap_or_else(|e| panic!("stat {}: {e}", p.display()));
        assert_eq!(
            meta.len(),
            entry.bytes,
            "byte-count drift for {} — manifest says {} bytes, on-disk is {} bytes",
            entry.path,
            entry.bytes,
            meta.len()
        );
    }
}

/// The load-bearing determinism check.
///
/// Recomputes the BLAKE3 of every vendored file and asserts it equals
/// the manifest's declared hash. Any byte-level tampering — including
/// re-saves that change line endings, accidental edits, or upstream
/// schema swaps — surfaces here.
///
/// This test is the M0 instantiation of theory/ENGENHO.md §VI.1's
/// "generated source is bit-reproducible" contract. At M0.0.3 the same
/// test gates forge-gen's output (regenerate from these schemas → same
/// generated source byte-for-byte).
#[test]
fn every_vendored_file_blake3_matches_manifest() {
    let manifest = load_manifest();
    for entry in &manifest.files {
        let p = vendor_path(&entry.path);
        let bytes = fs::read(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()));
        let actual = blake3::hash(&bytes).to_hex().to_string();
        assert_eq!(
            actual, entry.blake3,
            "BLAKE3 drift for {} — manifest declares {}, on-disk is {}",
            entry.path, entry.blake3, actual,
        );
    }
}

#[test]
fn manifest_declares_kubernetes_1_34() {
    // Tightening invariant: engenho ships ONLY when Sonobuoy
    // --mode=certified-conformance passes on the 1.34 surface with
    // zero skips (theory/ENGENHO.md §III.1 + §XIII Ship Gate). A
    // manifest bump to a different minor must be a deliberate edit
    // reviewed alongside the corresponding conformance-pass evidence.
    //
    // History: M0.0 vendored 1.32 (the N-2 at session start on
    // 2026-05-18); bumped to 1.34 on 2026-05-19 once we proved the
    // k3s-bridge runs 1.34 in production locally + decided engenho
    // chases the current stable surface rather than N-2.
    let raw = fs::read_to_string(manifest_path()).expect("read MANIFEST.yaml");
    assert!(
        raw.contains("kubernetes_version: 1.34"),
        "vendored manifest must target k8s 1.34 per ENGENHO.md §III.1"
    );
}

#[test]
fn all_vendored_files_have_valid_blake3_format() {
    let manifest = load_manifest();
    for entry in &manifest.files {
        assert_eq!(entry.blake3.len(), 64, "blake3 for {} not 64 hex chars", entry.path);
        assert!(
            entry.blake3.chars().all(|c| c.is_ascii_hexdigit()),
            "blake3 for {} contains non-hex chars",
            entry.path
        );
    }
}
