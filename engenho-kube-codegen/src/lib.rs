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
pub mod openapi;
pub mod types;

pub use catalog::{KIND_CATALOG, KindEntry};
pub use emit::{emit_kind, emit_module};
pub use openapi::{KindShape, OpenApiDoc};
