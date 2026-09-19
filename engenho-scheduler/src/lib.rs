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
//!   StoreMesh::list("", "v1", "Pod", scope)   scope ← scheduler.namespace
//!         ↓
//!   filter: spec.nodeName missing OR empty
//!         ↓
//!   candidates ← StoreMesh::list("", "v1", "Node", None)
//!     each Node's Ready derived from its Lease (ObservedNode::project)
//!         ↓
//!   for each pending pod:
//!     filter(pod, nodes): every FilterPlugin, in order
//!       NodeReady → Cordon → NodeName → NodeSelector
//!         → TaintToleration → Resources
//!         ↓
//!     Feasible(set)     → SchedulingStrategy::pick(pod, &set) -> String
//!                         StoreMesh::propose(Patch {"spec":{"nodeName":"node-X"}})
//!     Infeasible(why)   → PodScheduled=False / Unschedulable, message = why
//!     NoNodesObserved   → nothing written
//! ```
//!
//! The strategy is pluggable — `RoundRobinStrategy` is the default;
//! production can swap in `BinPackStrategy`, `AffinityStrategy`, etc.

#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod affinity;
pub mod config_bridge;
pub mod error;
pub mod filter;
pub mod fit;
pub mod ledger;
pub mod observed;
pub mod predicates;
pub mod preemption;
pub mod scheduler;
pub mod scope;
pub mod strategy;

pub use config_bridge::{ConfiguredScheduler, make_scheduling_strategy};
pub use error::SchedulerError;
pub use filter::{
    Candidate, Diagnosis, Feasible, FilterPlugin, Filtered, Rejection, admit, filter,
};
pub use fit::{NodeResources, PodRequests, fits, node_allocatable, pod_requests};
pub use ledger::{CapacityHold, Headroom, NodeLedger, holds_capacity};
pub use observed::ObservedNode;
pub use scheduler::{Scheduler, TickReport};
pub use scope::{NamespaceScope, ScopedNamespace};
pub use strategy::{RoundRobinStrategy, SchedulingStrategy};
