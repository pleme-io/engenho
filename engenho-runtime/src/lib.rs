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
//! with a periodic fallback as the safety net.
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
//! [`Runtime::shutdown`] aborts + awaits every driver JoinHandle (so
//! the controller/scheduler/kubelet tasks drop their `Arc<StoreMesh>`
//! clones), shuts the apiserver down (2s grace, severs open watches —
//! which drops the handler clones), THEN `Arc::try_unwrap`s the store
//! and calls `terminate` (which consumes `StoreMesh` and needs the sole
//! strong ref). The Runtime holds the last clone; once the tasks +
//! handlers drop theirs, the unwrap succeeds.

#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod etcd_facade;

mod error;
mod runtime;

pub use error::RuntimeError;
pub use etcd_facade::MeshEtcdStore;
pub use runtime::Runtime;
