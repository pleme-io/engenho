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

pub mod admission;
pub mod admission_webhook;
pub mod attestation;
pub mod build_backend_roceiro;
pub mod cluster_ip;
pub mod controller;
pub mod crd;
pub mod crd_validator;
pub mod create_stamp;
pub mod cron;
pub mod daemonset;
pub mod deployment;
pub mod dns;
pub mod drv;
pub mod drv_build;
pub mod endpoints;
pub mod error;
pub mod event_driven;
pub mod gc;
pub mod hpa;
pub mod ingress;
pub mod job;
pub mod meta;
pub mod namespace;
pub mod network_policy;
pub mod owned_children;
pub mod owner;
pub mod pdb;
pub mod plantio;
pub mod plantio_pipeline;
pub mod pv_binder;
pub mod replicaset;
pub mod roceiro;
pub mod runtime;
pub mod selector;
pub mod service_router;
pub mod statefulset;
pub mod status;
pub mod store_ledger;
pub mod store_resolver;
pub mod tiered_build;
pub mod tiered_reconciler;
pub mod watch_driver;

pub use admission::{
    AdmissionAction, AdmissionChain, AdmissionDecision, AdmissionError, AdmissionMode,
    AdmissionRequest, AdmissionWebhook, FakeAdmissionWebhook,
};
pub use admission_webhook::{
    FailurePolicy, MockWebhookCaller, MutatingWebhook, MutatingWebhookPlugin, Pluralizer,
    ReinvocationPolicy, RuleWithOperations, ServiceRef, StaticConfigSource, WebhookCaller,
    WebhookClientConfig, WebhookConfigSource, WebhookError, build_admission_review,
    parse_mutating_config, read_admission_review, rules_match,
};
pub use attestation::{
    FakeSignatureVerifier, SignatureVerifier, TameshiAttestationWebhook, VerifierError,
    tameshi_attestation_webhook,
};
pub use build_backend_roceiro::BuildBackendRoceiro;
pub use controller::{Controller, ReconcileOutcome, ReconcileReport, ReconcileResult};
pub use crd::{
    CrdController, CrdEntry, CrdError, CrdHandlerSpec, CrdNames, CrdScope, CrdSpec, CrdVersion,
    DynamicHandlerSink,
};
pub use crd_validator::{CrdValidationWebhook, ValidationRegistry, crd_validation_webhook};
pub use create_stamp::{
    CreateClock, creation_timestamp_is_unset, stamp_create_timestamp, wall_clock,
};
pub use daemonset::DaemonSetController;
pub use deployment::DeploymentController;
pub use dns::{
    DEFAULT_CLUSTER_DOMAIN, DnsBackend, DnsController, DnsError, DnsEvent, DnsRecord,
    InMemoryDnsZone, SrvRecord, srv_fqdn,
};
pub use drv::DrvController;
pub use drv_build::{BuildBackend, BuildError, BuildResult, DrvBuildController, FakeBuildBackend};
pub use endpoints::EndpointsController;
pub use error::ControllerError;
pub use event_driven::EventDrivenController;
pub use gc::GcController;
pub use hpa::{
    FakeMetricsProvider, HorizontalPodAutoscalerController, MetricsError, MetricsProvider,
    ScaleTarget,
};
pub use ingress::{
    FakeIngressBackend, FakeIngressEvent, IngressBackend, IngressController, IngressError,
    IngressRoute, NginxIngressBackend, PathType, TraefikIngressBackend,
};
pub use job::{
    Clock, ConcurrencyPolicy, CronJobController, JobController, SystemClock, WallClock, fixed_clock,
};
// Backwards-compat alias for any external consumer that referenced the
// old name. Substrate's FrozenClock is the canonical replacement.
pub use engenho_substrate::FrozenClock;
#[deprecated(
    since = "0.1.4",
    note = "use engenho_substrate::FrozenClock directly (ms precision)"
)]
pub type FixedClock = engenho_substrate::FrozenClock;
pub use meta::ObjectMeta;
pub use namespace::{NamespaceController, namespaced_kinds};
pub use network_policy::{
    CiliumNetworkPolicyAdapter, Direction, FakeNetworkPolicyEnforcer, FakeNpEvent,
    NetworkPolicyEnforcer, NetworkPolicyError, NetworkPolicyRule, PeerSelector, PortSpec,
};
pub use owned_children::{ChildKind, OwnedChildrenReconciler, ParentGvk, ReconcileDelta};
pub use owner::{
    OwnerReference, controlling_owner, is_owned_by, owner_ref_for, set_owner_reference,
};
pub use pdb::PodDisruptionBudgetController;
pub use plantio::{NodeResolver, PlantioController, StaticNodeResolver};
pub use plantio_pipeline::{
    LedgerChoice, LedgerWrappers, NodeResolverChoice, PipelineConfig, PlantioPipeline,
    RoceiroChoice, bootstrap_pipeline,
};
pub use pv_binder::{
    ENGENHO_LOCAL_PATH_PROVISIONER, FakeProvisionerEnv, HostProvisionerEnv, LOCAL_PATH_PROVISIONER,
    ProvisionerEnv, PvBinderController,
};
pub use replicaset::ReplicaSetController;
pub use roceiro::{FakeRoceiro, Roceiro, RoceiroError};
pub use runtime::{ControllerRuntime, RuntimeConfig};
pub use selector::{matches_labels, selector_match_labels, service_selector};
pub use service_router::{
    FakeRouter, FakeRouterEvent, IptablesRouter, IpvsRouter, PortMap, RouterError, ServiceRoute,
    ServiceRouter, ServiceRoutingController,
};
pub use statefulset::StatefulSetController;
pub use status::{
    StatusWriteOutcome, generation_of, observed_generation, pod_is_ready, resource_version_of,
    write_status_cas,
};
pub use store_ledger::{DEFAULT_RECEIPT_NAMESPACE, StoreBackedLedger};
pub use store_resolver::StoreBackedNodeResolver;
pub use tiered_build::TieredBuildBackend;
pub use tiered_reconciler::{PromotionScope, StaticPromotionScope, TieredCacheReconciler};
pub use watch_driver::{KindFilter, WatchDriver, WatchDriverConfig};
