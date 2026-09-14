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
pub mod backoff;
pub mod config_bridge;
pub mod cri;
pub mod cri_backend;
pub mod csi_materializer;
pub mod error;
pub mod exec_channel;
pub mod exec_session;
pub mod kubelet;
pub mod lifecycle;
/// Node lease + readiness derivation.
///
/// ── ★ MOVED TO `engenho-controllers` 2026-09-14, RE-EXPORTED HERE ─────────
/// It lived in this crate because the kubelet was its only consumer. The
/// apiserver now DERIVES a Node's `Ready` condition from the lease at READ
/// time, and `engenho-apiserver` does not — and should not — depend on
/// `engenho-kubelet`. Both depend on `engenho-controllers`, so the derivation
/// moved there rather than being copied, which would have left two definitions
/// of `GRACE_PERIOD` free to disagree about when a node is stale.
///
/// Re-exported so every existing `engenho_kubelet::node_lease::…` path still
/// resolves (★★ MODULARIZE, DON'T DELETE).
pub use engenho_controllers::node_lease;
pub mod pod_volume;
pub mod podman_api;
pub mod probe;
pub mod server;
pub mod volume;

pub use backend::{
    ContainerRuntime, ContainerStatus, ExecOutcome, FakeBackend, FakeNetProber, HttpProbeTarget,
    LogOptions, NetProber, PodmanBackend, ProbeIoError, PullPolicy, TcpProbeTarget, TokioNetProber,
};
pub use config_bridge::{make_container_runtime, make_container_runtime_with_apiserver};
pub use csi_materializer::{
    CsiRegistrarController, CsiVolumeMaterializer, DriverCsiProvisioner, DriverTable,
    RegisteredDriver,
};
pub use error::KubeletError;
pub use kubelet::{Kubelet, SaRefreshReport, TestClock};
pub use lifecycle::{
    ContainerObservation, ContainerState, ContainerStatusOut, RestartPolicy, reconcile_pod_phase,
};
pub use pod_volume::{
    FakeVolumeMaterializer, MountSource, NoServiceAccountProjection, PodVolumeSource,
    PodmanVolumeMaterializer, ResolvedMount, SA_MOUNT_PATH, ServiceAccountProjector,
    VolumeMaterializer, VolumeResolveError, resolve_pod_volumes,
};
pub use probe::{
    HttpScheme, ProbeHandler, ProbeKind, ProbeObservation, ProbeParseError, ProbePort,
    ProbeRuntime, ProbeSpec, ProbeTiming, ProbeVerdict, aggregate_container_readiness,
    fold_probe_observation, run_handler,
};
pub use volume::{
    AccessMode, FakeVolumeBackend, FakeVolumeEvent, HostPathVolumeBackend, MountedVolume,
    VolumeError, VolumeRuntime, VolumeSpec,
};
