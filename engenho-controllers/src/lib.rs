//! # engenho-controllers
//!
//! The K8s controller suite for engenho. Hosts:
//!
//!   * [`Controller`] trait — the second-site extraction of the
//!     reconcile-loop shape (first site: `engenho-scheduler`'s
//!     `Scheduler`). Future controllers all implement this.
//!   * [`replicaset::ReplicaSetController`] — first concrete
//!     impl. Watches ReplicaSets, creates/deletes Pods so the
//!     observed replica count matches `spec.replicas`.
//!   * [`runtime::ControllerRuntime`] — runs N controllers on a
//!     shared tokio scheduler with per-controller intervals.
//!
//! ## Why this is its own crate (not part of engenho-scheduler)
//!
//! The scheduler is one controller. The controllers crate is the
//! HOME for all the others. Per the prime directive, the SHARED
//! trait + runtime live here; engenho-scheduler keeps its
//! `Scheduler` impl and will optionally implement [`Controller`]
//! at R9.5 (cheap mechanical edit).
//!
//! ## Owner references
//!
//! K8s controllers track ownership via `metadata.ownerReferences`.
//! [`owner::set_owner_reference`] is the typed helper. Garbage
//! collection of orphaned children is R9.7.

#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod controller;
pub mod error;
pub mod owner;
pub mod replicaset;
pub mod runtime;

pub use controller::{Controller, ReconcileReport};
pub use error::ControllerError;
pub use owner::{controlling_owner, is_owned_by, set_owner_reference, OwnerReference};
pub use replicaset::ReplicaSetController;
pub use runtime::{ControllerRuntime, RuntimeConfig};
