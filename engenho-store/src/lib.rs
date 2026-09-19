//! # engenho-store
//!
//! The K8s resource store — engenho's etcd-equivalent. A
//! Raft-replicated key-value catalog of K8s resources keyed by
//! `(GroupVersionKind, namespace, name)`.
//!
//! ## Architecture
//!
//! Sibling to `engenho-revoada::consensus::RaftMesh` but with a
//! DIFFERENT state machine + command set. The two Raft groups
//! coexist in an engenho cluster:
//!
//!   * **revoada Raft** — commits typed `RoleAssignment`s
//!     (cluster shape: which node holds which control-plane role).
//!   * **engenho-store Raft** — commits typed `ResourceCommand`s
//!     (K8s resource CRUD: which pods/services/configmaps exist).
//!
//! Both reuse the openraft + InMemoryStore + InProcessRouter
//! pattern but instantiate their own state machine per their
//! domain. R6.5 may extract a shared `pleme-io/openraft-mem` crate
//! once a third Raft-using site appears.
//!
//! ## Layers reused
//!
//!   * openraft for replication (Layer B)
//!   * Per-node BLAKE3+ed25519 attestation chain via apply-time
//!     signing (Layer D) — same code path as revoada but for
//!     resource ops instead of role ops
//!
//! ## What this enables
//!
//! Once engenho-store ships, engenho-apiserver becomes a thin
//! wrapper that translates K8s API REST calls into `ResourceCommand`
//! Raft proposals. Workers and CLI clients see a stock K8s API;
//! the underlying store is the distributed substrate.
//!
//! ## The catalog is sealed (T3.2b)
//!
//! The state machine's catalog is crate-private, and this crate denies
//! `private_interfaces`, so no public signature can return or take it.
//! Reading one integer used to clone every resource plus the 8192-entry
//! watch-replay ring; now a reader outside the crate gets a scalar, one
//! key, a scoped list or page, or a visitor under one guard — see
//! [`StoreMesh`]'s read surface.
//!
//! The type cannot be named from outside:
//!
//! ```compile_fail,E0603
//! let _ = engenho_store::state::ResourceCatalog::default();
//! ```
//!
//! and the method that handed it out is gone:
//!
//! ```compile_fail,E0599
//! async fn read(mesh: &engenho_store::StoreMesh) {
//!     let _ = mesh.current_catalog().await;
//! }
//! ```
//!
//! while its replacements are reachable. This is the positive control for
//! both blocks above — the same shapes, compiling — and it is what keeps them
//! from being vacuous: stable rustdoc does not check a `compile_fail` block's
//! error code (only nightly does), so on stable a block proves only that it
//! fails to compile, not why.
//!
//! ```
//! async fn read(mesh: &engenho_store::StoreMesh) -> (engenho_store::Revision, u64) {
//!     (mesh.current_revision().await, mesh.last_applied_index().await)
//! }
//! let _ = engenho_store::state::DEFAULT_HISTORY_CAPACITY;
//! ```

#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
// `StorageError<NodeId>` is openraft's own large error type (its
// source notes "StorageError is 136 bytes, try to reduce the size").
// Every openraft storage-trait method returns it by contract — we
// can't box it without breaking the trait signatures.
#![allow(clippy::result_large_err)]
// Pervasive pure-style pedantic noise across this crate (and the
// fleet). Allowed at the crate root the same way
// `module_name_repetitions` is — keeps the substantive lints loud
// while not gating on doc-prose formatting.
#![allow(clippy::doc_markdown)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::must_use_candidate)]
// `is_leader` keeps its `async fn` signature for API symmetry with
// the rest of StoreMesh's async surface (callers `.await` it across
// the fleet); the millis-cast in `wait_for_applied` is a bounded
// timeout value. Both predate item-2 and are out of its scope.
#![allow(clippy::unused_async)]
#![allow(clippy::cast_possible_truncation)]
// ★ T3.2b: the catalog seal. `state::ResourceCatalog` is `pub(crate)`; with
// this denied, a `pub fn` that returns or takes it is a compile error rather
// than a quiet way to hand the whole catalog and its replay ring back out.
#![deny(private_interfaces)]

pub mod command;
pub mod data_dir_lock;
pub mod drv_committal;
pub mod fjall_store;
pub mod mesh;
pub mod nats_listener;
pub mod nats_network;
pub mod network;
pub mod owned_task;
pub mod pagination;
pub mod patch_apply;
pub mod resource;
pub mod revision;
pub mod ssa;
pub mod state;
pub mod store;
pub mod type_config;
pub mod watch;
pub mod watch_backend;
/// The watch-replay ring, its compaction floor, the head revision and the
/// ring's capacity as one sealed value (T3.3). Crate-private, like the
/// catalog that holds it.
mod watch_history;

/// The catalog-level suites that were integration tests until T3.2b sealed
/// the catalog: they drive `ResourceCatalog` directly, which only code
/// inside the crate can name.
#[cfg(test)]
mod catalog_tests;

pub use command::{ApplySemantics, LoggedCommand, Reason, ResourceCommand, ResourceOp};
pub use drv_committal::{
    DRV_GROUP, DRV_KIND, DRV_VERSION, delete_drv_command, drv_resource_key, put_drv_command,
    render_drv_resource,
};
pub use fjall_store::{FjallStore, Flushed, IMAGE_GATE, ImageInconsistency, ImageTripwire};
pub use mesh::{MeshFlushed, Quiesced, StoreError, StoreMesh, default_config};
pub use nats_listener::NatsListener;
pub use nats_network::{NatsRaftNetwork, NatsRaftNetworkFactory, NatsRpcEnvelope};
pub use network::InProcessRouter;
pub use owned_task::{OwnedTask, TaskStop};
pub use pagination::{ContinueInvalid, ContinueToken, ListPage, PageAtRevision};
pub use patch_apply::{
    Gvk, JsonPath, ListMergeStrategy, MockPatchEnv, OpenApiPatchEnv, PatchBody, PatchDirective,
    PatchError, PatchSchemaEnv, apply as apply_patch_algorithm,
};
pub use resource::{ListScope, ResourceKey, ResourceValue};
pub use revision::{Change, ChangeKind, CompactedTooOld, Revision, VersionMeta};
pub use ssa::{ApplyConflicts, Conflict, FieldSet, PathElement, SsaOutcome, apply_ssa};
pub use state::{ApplyOutcome, DEFAULT_HISTORY_CAPACITY, check_precondition, unchanged};
pub use store::InMemoryStore;
pub use type_config::{ApplyResult, RaftNodeId, TypeConfig};
pub use watch::{WatchEvent, WatchEventKind};
pub use watch_backend::{WatchGone, WatchOpts, WatchSignal, WatchStream};
