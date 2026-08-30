//! # engenho-kube-codegen
//!
//! Reads vendored Kubernetes OpenAPI v3 JSON + emits typed Rust source
//! into `engenho-types/src/generated_v1_34/`. Per theory/ENGENHO.md
//! §IV the load-bearing invariant: no hand-authored K8s resource
//! types — every kind is generated bit-reproducibly from upstream.
//!
//! ## What's generated today (M0.0.1 first pass)
//!
//! A "thin kind" representation: each generated struct carries:
//!   * `metadata: engenho_types::meta::ObjectMeta`
//!   * `spec:     serde_json::Value`   (typed expansion is M0.0.4)
//!   * `status:   Option<serde_json::Value>`
//!
//! Plus a `KubeResource` impl with the correct
//! `GroupVersionKind` / `GroupVersionResource` / `Scope`.
//!
//! ## Future (M0.0.4)
//!
//! Recursive `$ref` expansion through the OpenAPI spec produces
//! fully-typed `PodSpec`, `Container`, `Volume`, etc. Hand-rolled
//! ObjectMeta also becomes generator output at that point.
//!
//! ## Determinism contract
//!
//! `kube-codegen --check` (callable from CI) must regenerate
//! byte-identical source from the same input. Implementation:
//!   * Sort iteration deterministic (BTreeMap, never HashMap).
//!   * Emitted source has a fixed header comment.
//!   * Field ordering: required-then-optional, then alphabetical.

#![warn(missing_docs)]

pub mod catalog;
pub mod emit;
pub mod emit_typed;
pub mod openapi;
pub mod types;

pub use catalog::{KIND_CATALOG, KindEntry, Subresource};
pub use emit::{emit_catalog, emit_kind, emit_kind_typed, emit_module, emit_shared_module};
pub use emit_typed::{SchemaView, shared_substructs};
pub use openapi::{KindShape, OpenApiDoc};

/// Format emitted Rust source, deterministically and without a subprocess.
///
/// ## Why this exists
///
/// The generator emitted UNFORMATTED source while the committed files were
/// formatted. Measured 2026-08-30: `--check` reported 46 drifting files
/// against a pristine tree, and **40 of them differed by formatting alone**
/// — reformatted by `16d0fd5`, a workspace-wide `cargo fmt` that swept 149
/// files including this generated directory.
///
/// A determinism gate that is permanently red is not a gate. Once
/// `--check` could never pass, nobody could see the SIX real differences
/// hiding behind the forty cosmetic ones — including three hand-authored
/// files (`rbac_v1/policy.rs`, `apps_v1/deployment_spec.rs`,
/// `apps_v1/replicaset_spec.rs`) living inside a directory whose header
/// says GENERATED — DO NOT EDIT. Regenerating deletes them, which is
/// exactly how a routine regen broke `engenho-apiserver`'s authz module.
///
/// ## Why prettyplease and not rustfmt
///
/// `--check` compares bytes, so the formatter must be pinned by Cargo, not
/// by whatever `rustfmt` is on the operator's PATH. A subprocess would also
/// be a shell-out in a codebase that forbids them. `prettyplease` is a
/// library, versioned in the lockfile, identical on every machine.
///
/// # Errors
///
/// Returns the parse error if `src` is not valid Rust. This fails LOUDLY
/// on purpose: a generator emitting unparseable source is a defect, and
/// silently writing it out would move the failure to whoever builds next.
pub fn format_source(src: &str) -> anyhow::Result<String> {
    let parsed = syn::parse_file(src)
        .map_err(|e| anyhow::anyhow!("generated source is not valid Rust: {e}"))?;
    Ok(prettyplease::unparse(&parsed))
}
