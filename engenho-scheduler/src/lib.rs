//! # engenho-scheduler
//!
//! The **first real K8s controller** on the engenho substrate.
//! Reconciles pending Pods (those without `spec.nodeName`) by
//! picking a Node via a typed [`SchedulingStrategy`] trait + patching
//! the Pod through [`engenho_store::StoreMesh`].
//!
//! ## Why this matters
//!
//! Until R8, the substrate had stores + servers but no production
//! consumer. engenho-scheduler is the proof that:
//!
//!   1. `StoreMesh` is a usable foundation for K8s controllers.
//!   2. The reconcile-loop pattern (poll → decide → patch) works
//!      cleanly on top of typed resource commands.
//!   3. Future controllers (Deployment, ReplicaSet, Service,
//!      Endpoints, GC, etc.) follow this exact shape.
//!
//! ## Architecture
//!
//! ```text
//!   StoreMesh::list("", "v1", "Pod", None)
//!         ↓
//!   filter: spec.nodeName missing OR empty
//!         ↓
//!   for each pending pod:
//!     candidates ← StoreMesh::list("", "v1", "Node", None)
//!     filter: not Quarantined, not unschedulable
//!     SchedulingStrategy::pick(pod, candidates) → NodeBinding
//!         ↓
//!     StoreMesh::propose(Patch { key=pod_key, patch={"spec":{"nodeName":"node-X"}}})
//! ```
//!
//! The strategy is pluggable — `RoundRobinStrategy` is the default;
//! production can swap in `BinPackStrategy`, `AffinityStrategy`, etc.

#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod config_bridge;
pub mod error;
pub mod fit;
pub mod scheduler;
pub mod strategy;

pub use config_bridge::make_scheduling_strategy;
pub use error::SchedulerError;
pub use fit::{NodeResources, PodRequests, fits, free_on_node, node_allocatable, pod_requests};
pub use scheduler::{Scheduler, TickReport};
pub use strategy::{RoundRobinStrategy, SchedulingStrategy};
