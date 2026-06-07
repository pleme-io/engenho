//! # engenho-kubelet
//!
//! The per-node controller that closes the gap between "Pod object
//! exists in the store" and "container is executing on host." Until
//! R10, the substrate had a complete control plane + typed catalog
//! + watch streams + scheduler — but nothing actually ran a
//! container. R10 ships that.
//!
//! ## Surface
//!
//!   * [`ContainerRuntime`] — pluggable backend trait
//!   * [`Kubelet`] — per-node reconcile loop, impl `Controller`
//!   * Backends:
//!       * [`backend::FakeBackend`] — in-memory, deterministic, for tests
//!       * [`backend::PodmanBackend`] — shell-out to `podman` for local Mac/Linux
//!       * (future R10.5) CriBackend, RunwasiBackend
//!
//! ## Reconcile loop
//!
//! 1. List all Pods whose `spec.nodeName == self.node_name`.
//! 2. For each Pod:
//!    - If not already running locally, start a container via the
//!      backend and capture container_id.
//!    - If running, ensure backend reports healthy + update
//!      `status.podIP` + `status.conditions[Ready]=True` via patch.
//!    - If Pod is missing from the store (cleanup), kill the
//!      container.
//!
//! ## Container intent translation
//!
//! Reuses `engenho_types::translator::WorkloadIntent` so an
//! operator can express a Pod, ReplicaSet, or full Deployment
//! through any face and the kubelet's container spec is derived
//! consistently. Per-pod the kubelet reads `spec.containers[0]`
//! directly (Pod is the lowest-level unit; no further translation).

#![warn(clippy::pedantic)]
#![warn(missing_docs)]
#![allow(clippy::module_name_repetitions)]

pub mod backend;
pub mod config_bridge;
pub mod error;
pub mod kubelet;
pub mod volume;

pub use backend::{ContainerRuntime, ContainerStatus, FakeBackend, PodmanBackend, PullPolicy};
pub use config_bridge::make_container_runtime;
pub use error::KubeletError;
pub use kubelet::Kubelet;
pub use volume::{
    AccessMode, FakeVolumeBackend, FakeVolumeEvent, HostPathVolumeBackend, MountedVolume,
    VolumeError, VolumeRuntime, VolumeSpec,
};
