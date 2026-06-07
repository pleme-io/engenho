//! Vendored Kubernetes protobuf (.proto) BLAKE3 manifest verification.
//!
//! Per theory/ENGENHO.md §VI.1, every byte the protobuf codec consumes
//! is BLAKE3-attested — the same discipline as the vendored OpenAPI
//! schemas in engenho-types. This test reads `vendor/proto/MANIFEST.yaml`
//! and recomputes the BLAKE3 of every vendored `.proto`, asserting the
//! on-disk content matches the manifest byte-for-byte. Any upstream
//! schema swap or accidental edit surfaces here, in CI.

use std::fs;
use std::path::PathBuf;

const VENDOR_DIR: &str = "vendor/proto";

#[derive(Debug, serde::Deserialize)]
struct Manifest {
    files: Vec<ManifestEntry>,
}

#[derive(Debug, serde::Deserialize)]
struct ManifestEntry {
    path: String,
    blake3: String,
    bytes: u64,
}

fn manifest_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(VENDOR_DIR)
        .join("MANIFEST.yaml")
}

fn vendor_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(VENDOR_DIR)
        .join(name)
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
fn every_vendored_proto_present_with_declared_size() {
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

/// The load-bearing determinism check — the inputs to the codec
/// descriptor compilation are attested.
#[test]
fn every_vendored_proto_blake3_matches_manifest() {
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
    let raw = fs::read_to_string(manifest_path()).expect("read MANIFEST.yaml");
    assert!(
        raw.contains("kubernetes_version: 1.34"),
        "vendored proto manifest must target k8s 1.34 (matches engenho-types openapi vendor)"
    );
}

#[test]
fn all_vendored_proto_have_valid_blake3_format() {
    let manifest = load_manifest();
    for entry in &manifest.files {
        assert_eq!(
            entry.blake3.len(),
            64,
            "blake3 for {} not 64 hex chars",
            entry.path
        );
        assert!(
            entry.blake3.chars().all(|c| c.is_ascii_hexdigit()),
            "blake3 for {} contains non-hex chars",
            entry.path
        );
    }
}
