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

#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod command;
pub mod mesh;
pub mod nats_listener;
pub mod nats_network;
pub mod network;
pub mod persistence;
pub mod persistent_log;
pub mod resource;
pub mod state;
pub mod store;
pub mod type_config;
pub mod watch;

pub use command::{Reason, ResourceCommand, ResourceOp};
pub use mesh::{default_config, StoreError, StoreMesh};
pub use nats_listener::NatsListener;
pub use nats_network::{NatsRaftNetwork, NatsRaftNetworkFactory, NatsRpcEnvelope};
pub use persistence::{CatalogSnapshot, SnapshotError};
pub use persistent_log::{PersistentLog, PersistentLogError};
pub use network::InProcessRouter;
pub use resource::{ResourceKey, ResourceValue};
pub use state::{ResourceCatalog, ResourceCatalogSnapshot};
pub use store::InMemoryStore;
pub use type_config::{ApplyResult, RaftNodeId, TypeConfig};
pub use watch::{WatchEvent, WatchEventKind};
