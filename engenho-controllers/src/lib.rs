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
pub mod deployment;
pub mod endpoints;
pub mod error;
pub mod gc;
pub mod owner;
pub mod replicaset;
pub mod runtime;
pub mod admission;
pub mod crd;
pub mod dns;
pub mod drv;
pub mod drv_build;
pub mod hpa;
pub mod ingress;
pub mod job;
pub mod network_policy;
pub mod pdb;
pub mod selector;
pub mod service_router;
pub mod statefulset;
pub mod watch_driver;

pub use controller::{Controller, ReconcileReport};
pub use deployment::DeploymentController;
pub use endpoints::EndpointsController;
pub use error::ControllerError;
pub use gc::GcController;
pub use owner::{controlling_owner, is_owned_by, set_owner_reference, OwnerReference};
pub use replicaset::ReplicaSetController;
pub use runtime::{ControllerRuntime, RuntimeConfig};
pub use selector::{matches_labels, selector_match_labels, service_selector};
pub use dns::{
    srv_fqdn, DnsBackend, DnsController, DnsError, DnsEvent, DnsRecord,
    InMemoryDnsZone, SrvRecord, DEFAULT_CLUSTER_DOMAIN,
};
pub use ingress::{
    FakeIngressBackend, FakeIngressEvent, IngressBackend, IngressController,
    IngressError, IngressRoute, NginxIngressBackend, PathType, TraefikIngressBackend,
};
pub use job::{Clock, CronJobController, FixedClock, JobController, SystemClock};
pub use network_policy::{
    CiliumNetworkPolicyAdapter, Direction, FakeNetworkPolicyEnforcer, FakeNpEvent,
    NetworkPolicyEnforcer, NetworkPolicyError, NetworkPolicyRule, PeerSelector, PortSpec,
};
pub use admission::{
    AdmissionAction, AdmissionChain, AdmissionDecision, AdmissionError, AdmissionMode,
    AdmissionRequest, AdmissionWebhook, FakeAdmissionWebhook,
};
pub use crd::{CrdController, CrdEntry, CrdError, CrdRegistry, CrdScope};
pub use drv::DrvController;
pub use drv_build::{
    BuildBackend, BuildError, BuildResult, DrvBuildController, FakeBuildBackend,
};
pub use hpa::{
    FakeMetricsProvider, HorizontalPodAutoscalerController, MetricsError,
    MetricsProvider, ScaleTarget,
};
pub use pdb::PodDisruptionBudgetController;
pub use statefulset::StatefulSetController;
pub use service_router::{
    FakeRouter, FakeRouterEvent, IptablesRouter, IpvsRouter, PortMap, RouterError,
    ServiceRoute, ServiceRouter, ServiceRoutingController,
};
pub use watch_driver::{KindFilter, WatchDriver, WatchDriverConfig};
