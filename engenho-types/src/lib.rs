//! # engenho-types
//!
//! The typed Kubernetes resource catalog for engenho.
//!
//! Per `pleme-io/theory/ENGENHO.md` §II.1, every Kubernetes kind is a
//! `#[derive(KubeResource, TataraDomain)]` struct mechanically emitted by
//! `forge-gen` from upstream OpenAPI v3 (Pillar 12 — generation over
//! composition). The non-negotiable rule of this crate: **no hand-authored
//! resource types**. If you reach for a hand-written struct, you are
//! violating the prime directive — extend the generator instead.
//!
//! ## Crate layout (target)
//!
//! ```text
//! engenho-types/
//! ├── src/
//! │   ├── lib.rs                 (this file — module roots + invariants)
//! │   ├── kind.rs                (trait KubeResource — the typed contract)
//! │   ├── meta.rs                (ObjectMeta, ListMeta, TypeMeta)
//! │   ├── api.rs                 (GroupVersionKind, GroupVersionResource)
//! │   ├── core_v1/               (generated — Pod, Service, Namespace, …)
//! │   ├── apps_v1/               (generated — Deployment, ReplicaSet, …)
//! │   ├── rbac_v1/               (generated — Role, ClusterRole, …)
//! │   ├── networking_v1/         (generated — NetworkPolicy, Ingress, …)
//! │   ├── storage_v1/            (generated — StorageClass, …)
//! │   ├── coordination_v1/       (generated — Lease, …)
//! │   ├── apiextensions_v1/      (generated — CustomResourceDefinition)
//! │   └── …                      (~16 generated kind groups total)
//! ├── vendor/
//! │   └── openapi/v1.34.0/       (BLAKE3-attested upstream schemas)
//! └── tests/
//!     ├── openapi_roundtrip.rs   (each kind ↔ openapi-spec/v3/*.json)
//!     ├── bit_repro.rs           (forge-gen regenerate == tree source)
//!     └── lisp_authoring.rs      (TataraDomain round-trip per kind)
//! ```
//!
//! ## Status — M0.0
//!
//! Crate scaffold only. The first kind (Pod) lands in M0.0.1 as a
//! hand-authored target shape, validated by `tests/openapi_roundtrip.rs`
//! against `vendor/openapi/v1.34.0/api__v1_openapi.json` — that hand-author
//! is **the generator's bullseye**, the byte-for-byte target that
//! `forge-gen --backend kube-resource` must emit by M0.0.3. The kind itself
//! is then deleted from the tree and replaced by the generator's output.
//!
//! ## Ship gate (theory/ENGENHO.md §XIII)
//!
//! Engenho ships ONLY when Sonobuoy `--mode=certified-conformance`
//! passes on Kubernetes 1.34 with zero skips. Until M4 lands, the kasou
//! + kikai bridge runs k3s v1.34 in production locally and supplies the
//! operator kubectl experience while engenho catches up.

#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod api;
pub mod auth;
pub mod client;
pub mod consistency_tier;
pub mod error;
pub mod generated_v1_34;
pub mod informer;
pub mod kind;
pub mod meta;
pub mod nomad_v1;
pub mod patch;
pub mod primitives;
pub mod reconciler;
pub mod translator;
pub mod watch;

pub use consistency_tier::{CONSISTENCY_TIER_ANNOTATION, ConsistencyTier, tier_from_metadata};

// Generated kind modules land below this line. M0.0.1 introduces core_v1::Pod
// as the proof-of-pipeline; M0.0.3 replaces it with generator output; M0.0.4
// scales to ~150 kinds across ~16 groups.

#[cfg(test)]
mod tests {
    #[test]
    fn workspace_compiles() {
        // Placeholder. M0.0 trivially passes; meaningful tests come with
        // the first generated kind. The point of this test is to detect
        // workspace breakage during scaffolding, not to verify behaviour.
    }
}
