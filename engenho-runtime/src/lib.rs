//! # engenho-runtime
//!
//! The single-node assembly layer. [`Runtime`] is the ONE struct that
//! boots the entire single-node engenho control + data plane in ONE
//! process over a durable [`engenho_store::StoreMesh`], so a POSTed
//! `apps/v1` Deployment converges to a running container. The `engenho`
//! binary is a thin launcher over [`Runtime::start`].
//!
//! ## Why a lib crate (not just `main.rs`)
//!
//! The convergence proof — boot every subsystem, POST a Deployment over
//! real HTTP, watch the chain converge autonomously — must be an
//! integration test that doesn't go through `main`. A lib crate makes
//! [`Runtime`] integration-testable; the binary depends on it.
//!
//! ## What gets assembled (the spine)
//!
//! ```text
//!   ONE StoreMesh (durable fjall, or ephemeral in tests)
//!        │   ← every subsystem holds a clone; the apiserver translates
//!        │     HTTP→ResourceCommand proposals, controllers read via
//!        │     list/get/watch + write via propose. Same catalog.
//!        ├── ApiServer            (HTTP K8s API; handlers_from_catalog)
//!        ├── Node/<node_name>     (self-registered at boot; schedulable)
//!        ├── DeploymentController (Deployment → ReplicaSet)
//!        ├── ReplicaSetController (ReplicaSet → Pod)
//!        ├── EndpointsController  (Service → Endpoints)
//!        ├── GcController         (orphan owner-ref GC)
//!        ├── Scheduler            (pending Pod → spec.nodeName)
//!        └── Kubelet              (bound Pod → container via backend)
//! ```
//!
//! Each controller/scheduler/kubelet is wrapped in a
//! [`engenho_controllers::WatchDriver`] with a per-controller
//! [`engenho_controllers::KindFilter`] so the chain converges in ms,
//! with a periodic fallback as the safety net. The filter is derived from
//! the kinds the controller declares it reads
//! ([`engenho_controllers::DeclaresReads`], T1.7), never written by hand.
//!
//! ## Owned children (T2.6)
//!
//! Every long-lived task the runtime starts — each driver, the :10250 and
//! :2379 listeners, the node lease — is a [`Child`] of one closed catalog,
//! spawned into one [`Children`] set. A child's task cannot complete
//! normally (its output is `Infallible`), so a task that ends has panicked
//! or been aborted; [`Runtime::next_dead_child`] reports it, marked Dead and
//! logged at ERROR, and `main` watches for it beside its stop signal. There
//! is no respawn.
//!
//! ## Panics (T2.7)
//!
//! A panic in a driver's tick is contained only when the catalog says the
//! driver is [`TickState::Stateless`]: it is counted and the fallback
//! re-ticks it. A [`TickState::Stateful`] driver's panic ends it, and it is
//! Dead. A listener whose serve ends, or panics, binds again on a growing
//! backoff. Every panic in the process, caught or not, is counted by the
//! hook behind [`PanicCounter`], which [`Runtime::start`] installs.
//!
//! ## Every fault, every child (W6)
//!
//! [`Child::supervision`] declares what the supervisor does when a
//! [`Fault`] — a panic, a hang, an error return, a bind failure — strikes
//! each child ([`Supervision`]), derived from the catalog's rows. A test
//! matrix strikes every child with every fault and holds the runtime to it.
//!
//! ## The node lease (T1.3c)
//!
//! [`Child::NodeLease`] renews this node's Lease every renew interval while,
//! and only while, the kubelet's liveness row (the one `/livez` renders) is
//! alive. A kubelet whose tick wedges past the stuck window, or whose task
//! dies, stops heartbeating, and the node reads `NotReady` one grace period
//! later; a long tick inside the window (an image pull) keeps it Ready.
//!
//! ## The container runtime's health (W8)
//!
//! A [`Relister`] lists every container the runtime holds through the
//! [`Relist`] seam and writes the outcome into a [`RelistLedger`], whose one
//! judgement is a [`RuntimeHealth`]: healthy while the last successful
//! relist began within [`RELIST_THRESHOLD`] (upstream's PLEG health). Built
//! over that ledger, the node lease renews only while the runtime is healthy
//! too. `pending-runtime-relist`: the kubelet's `ContainerRuntime` cannot
//! relist yet, so the relister is [`Dormant`] and the runtime builds the
//! lease over [`RuntimeHealthSource::Unobserved`], renewing by the kubelet
//! alone.
//!
//! ## Health (T2.8)
//!
//! `/livez`, `/healthz`, `/readyz` and the runtime's `/metrics` families are
//! read from one [`Health`], built before the apiserver binds and handed the
//! spawned children: each child is judged from its heartbeat and its task
//! handle (never asserted), each driver's ticks are counted by how they
//! ended, and a propose-rate detector flags a controller that lands writes
//! in almost every second ([`ProposeRate::Continuous`]). The stuck-tick
//! threshold a driver logs BLOCKED at and the one liveness judges against
//! are one value ([`Windows`]).
//!
//! ## Dormant controllers (T5.11)
//!
//! Every type that implements [`engenho_controllers::Controller`] is either
//! the controller behind a catalog [`Driver`] or a [`Dormant`] row, which
//! names the type and says why nothing runs it ([`DormantReason`]). A row
//! names its type through the compiler; that no controller type is in
//! neither is a test over the workspace's source — a CI gate, not a type.
//!
//! ## The census (T0.10)
//!
//! [`census`] runs one named check from a closed catalog, with no side
//! effects, over a running apiserver (LIST) or over a copy of a node's data
//! directory (booted privately; the directory given is never written), and
//! reports what it matched with counts by kind and reason. Each check calls
//! the function the rule it gates ships. `engenho census` is its CLI.
//!
//! ## Boot order (strict)
//!
//! 1. `config.validate()`
//! 2. StoreMesh start (durable `start_or_resume`, or ephemeral) +
//!    `wait_for_leadership` — leadership MUST precede any `propose`
//! 3. register `Node/<node_name>` (the missing brick; no other code
//!    does this, and the scheduler hard-requires a schedulable Node)
//! 4. `ApiServer::start`
//! 5. spawn controllers / scheduler / kubelet drivers
//!
//! ## Shutdown (the tricky bit)
//!
//! [`Runtime::shutdown`] aborts + awaits every child task (so the
//! controller/scheduler/kubelet tasks drop their `Arc<StoreMesh>`
//! clones), shuts the apiserver down (2s grace, severs open watches —
//! which drops the handler clones), THEN `Arc::try_unwrap`s the store
//! and calls `terminate` (which consumes `StoreMesh` and needs the sole
//! strong ref). The Runtime holds the last clone; once the tasks +
//! handlers drop theirs, the unwrap succeeds.

#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod census;
pub mod etcd_facade;

mod boot_config;
mod child;
mod dormant;
mod error;
#[cfg(test)]
mod fault_matrix;
mod health;
#[cfg(test)]
mod impl_census;
mod node_lease;
mod node_registration;
mod panics;
#[cfg(test)]
mod read_census;
mod rebind;
mod runtime;
mod runtime_health;
#[cfg(test)]
mod testing;

pub use boot_config::{PkiField, Unhonoured};
pub use child::{
    Child, ChildHandle, ChildState, Children, DeadChild, DeathCause, Driver, Fault, Listener,
    Supervision, TickState, Wiring,
};
pub use dormant::{Dormant, DormantReason};
pub use error::{RuntimeError, ShutdownStage};
pub use etcd_facade::MeshEtcdStore;
pub use health::{CONTINUOUS_AFTER, Health, ProposeRate, ProposeWindow, Pulse, SPAN, Windows};
pub use node_registration::NodeRegistrationError;
pub use panics::PanicCounter;
pub use runtime::Runtime;
pub use runtime_health::{
    RELIST_PERIOD, RELIST_THRESHOLD, Relist, RelistFault, RelistLedger, Relisted, Relister,
    RuntimeHealth, RuntimeHealthSource, RuntimeSight, Staleness,
};
