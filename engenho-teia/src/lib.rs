//! # engenho-teia
//!
//! The fabric layer — one NATS-backed transport that carries
//! every cross-process byte in engenho. Per `docs/FABRIC.md`,
//! teia is the third-site extraction of the messaging pattern
//! that previously lived twice (engenho-revoada's InProcessRouter
//! + engenho-store's InProcessRouter).
//!
//! ## Surface
//!
//! - [`TeiaConfig`] — typed NATS connection config: servers,
//!   optional JWT credentials, leaf-node remotes for federation.
//! - [`subject::ClusterScope`] — typed subject builder. Subjects
//!   are constructed from primitives, never string-formatted ad-hoc.
//! - `TeiaClient` — wraps `async_nats::Client` with the typed
//!   subject + payload encoding. **Only under the `teia-nats` feature.**
//!
//! ## Fenced off (docs/IMPROVEMENT-PLAN.md §5.1)
//!
//! NATS is not engenho's fabric: a NATS server is a second process on every
//! node, a sidecar under another name. engenho-config's `fabric` is
//! `in_binary`. This crate stays as a typed draft; with default features it
//! is the subject grammar and the connection config and nothing that dials
//! a socket. `teia-nats` (off by default) adds the client and async-nats.
//!
//! ## Channels (per FABRIC.md)
//!
//! 1. Raft RPC          `engenho.{cluster}.raft.{group}.append.{target_node}`
//! 2. Watch streams     `engenho.{cluster}.watch.{gvk}.{namespace}.{name}`
//! 3. Content store     `engenho.{cluster}.content.{bucket}` (NATS Object)
//! 4. Attestation       `engenho.{cluster}.attestation.{node_id}.{index}`
//! 5. Observability     `engenho.{cluster}.health.{service}` (consumed by Vector)
//!
//! ## Phase
//!
//! F1 — typed surface + client connection (this crate).
//! F2-F5 — real impls (RaftTransport, WatchPub/Sub, ContentStore,
//! AttestationPub) ratchet on top.

#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

#[cfg(feature = "teia-nats")]
pub mod client;
pub mod config;
pub mod error;
pub mod subject;

#[cfg(feature = "teia-nats")]
pub use client::TeiaClient;
pub use config::{LeafNodeRemote, TeiaConfig};
pub use error::TeiaError;
pub use subject::{ClusterScope, Gvk, NodeId, Subject};
