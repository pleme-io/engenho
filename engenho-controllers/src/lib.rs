//! # engenho-controllers
//!
//! The K8s controller suite for engenho. Hosts:
//!
//!   * [`Controller`] trait — the second-site extraction of the
//!     reconcile-loop shape (first site: `engenho-scheduler`'s
//!     `Scheduler`, which implements it too). Every controller here
//!     implements it.
//!   * [`replicaset::ReplicaSetController`] — first concrete
//!     impl. Watches ReplicaSets, creates/deletes Pods so the
//!     observed replica count matches `spec.replicas`.
//!   * [`WatchDriver`] — the event loop that drives one controller from
//!     store events, a requeue slot and a fallback timer. The daemon runs
//!     each controller it spawns under one.
//!   * [`runtime::ControllerRuntime`] — runs N controllers on a
//!     shared tokio scheduler with per-controller intervals, for
//!     embedders and tests; the daemon does not use it.
//!
//! ## Why this is its own crate (not part of engenho-scheduler)
//!
//! The scheduler is one controller. The controllers crate is the
//! HOME for all the others. Per the prime directive, the SHARED
//! trait + drivers live here; engenho-scheduler keeps its
//! `Scheduler` and implements [`Controller`] for it.
//!
//! ## Owner references
//!
//! K8s controllers track ownership via `metadata.ownerReferences`.
//! [`owner::set_owner_reference`] is the typed helper; [`gc`] collects
//! the dependents whose owners are gone.

// A doc link to an item that does not exist is a comment naming code that
// is not there (plan class D). This makes `cargo doc` refuse one. Tier: a
// gate only where rustdoc runs; plain builds and tests never read it.
#![deny(rustdoc::broken_intra_doc_links)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod admission;
pub mod admission_webhook;
pub mod attestation;
pub mod build_backend_roceiro;
mod closed_enum;
pub mod cluster_ip;
pub mod cni_status;
pub mod condition;
pub mod contain;
pub mod controller;
pub mod crd;
pub mod crd_conversion;
pub mod crd_validator;
pub mod create_stamp;
pub mod cron;
pub mod csi_provisioner;
pub mod curve;
pub mod daemonset;
pub mod deployment;
pub mod dns;
pub mod drv;
pub mod drv_build;
pub mod effect;
pub mod endpoints;
pub mod error;
pub mod event_driven;
pub mod event_recorder;
pub mod gc;
pub mod heartbeat;
pub mod hpa;
pub mod ingress;
pub mod job;
pub mod meta;
pub mod namespace;
pub mod network_policy;
pub mod network_policy_controller;
pub mod node_lease;
pub mod node_port;
pub mod owned_children;
pub mod owner;
pub mod pdb;
pub mod plantio;
pub mod plantio_pipeline;
pub mod pod_scheduling;
pub mod pod_template;
pub mod pv_binder;
pub mod pvc_protection;
pub mod reads;
pub mod replicaset;
pub mod roceiro;
pub mod runtime;
pub mod selector;
pub mod served_capability;
pub mod service_router;
pub mod statefulset;
pub mod status;
pub mod store_ledger;
pub mod store_resolver;
pub mod sweep;
pub mod tiered_build;
pub mod tiered_reconciler;
pub mod volume_snapshot;
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
pub use contain::{PanicMessage, TickState};
pub use controller::{
    Controller, ControllerType, ReconcileOutcome, ReconcileReport, ReconcileResult,
};
pub use crd::{
    CrdController, CrdEntry, CrdError, CrdHandlerSpec, CrdNames, CrdScope, CrdSpec, CrdVersion,
    DynamicHandlerSink,
};
pub use crd_validator::{CrdValidationWebhook, ValidationRegistry, crd_validation_webhook};
pub use create_stamp::{
    CreateClock, creation_timestamp_is_unset, stamp_create_timestamp, wall_clock,
};
pub use daemonset::DaemonSetController;
pub use deployment::{DeploymentController, current_replicaset};
pub use dns::{
    DEFAULT_CLUSTER_DOMAIN, DnsBackend, DnsController, DnsError, DnsEvent, DnsRecord,
    InMemoryDnsZone, SrvRecord, srv_fqdn,
};
pub use drv::DrvController;
pub use drv_build::{BuildBackend, BuildError, BuildResult, DrvBuildController, FakeBuildBackend};
pub use effect::{Effect, Landed, Refusal};
pub use endpoints::EndpointsController;
pub use error::{ControllerError, ErrorScope};
pub use event_driven::EventDrivenController;
pub use gc::GcController;
pub use heartbeat::{Beat, Heartbeat, TickClass};
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
pub use condition::{ConditionStatus, ConditionUpsert, DesiredCondition, upsert_condition};
pub use curve::{Curve, Streak};
pub use meta::{
    Container, DefaultedInt, FieldPath, JsonKind, ObjectMeta, REPLICAS, ShapeError, array_mut,
    int_at, object_mut,
};
pub use namespace::{NamespaceController, namespaced_kinds};
pub use network_policy::{
    CiliumNetworkPolicyAdapter, Direction, FakeNetworkPolicyEnforcer, FakeNpEvent,
    NetworkPolicyEnforcer, NetworkPolicyError, NetworkPolicyRule, PeerSelector, PortSpec,
};
pub use owned_children::{OwnedChildrenReconciler, ParentGvk, ReconcileDelta};
pub use owner::{
    OwnerReference, controlling_owner, is_owned_by, owner_ref_for, set_owner_reference,
};
pub use pdb::PodDisruptionBudgetController;
pub use plantio::{NodeResolver, PlantioController, StaticNodeResolver};
pub use plantio_pipeline::{
    LedgerChoice, LedgerWrappers, NodeResolverChoice, PipelineConfig, PlantioPipeline,
    RoceiroChoice,
};
pub use pod_scheduling::{
    BindOutcome, Binding, DEFAULT_SCHEDULER, PodSchedulingState, Schedulable, bind_cas,
    mark_unschedulable_cas,
};
pub use pod_template::{NormalizedTemplate, POD_TEMPLATE_HASH_LABEL, TemplateHash};
pub use pv_binder::{
    ClaimUid, ENGENHO_LOCAL_PATH_PROVISIONER, FakeProvisionerEnv, HostProvisionerEnv,
    LOCAL_PATH_PROVISIONER, LocalPathDir, NoVolumeIdentity, ProvisionerEnv, PvBinderController,
    PvName, ReclaimError, ReclaimPolicy, Unreclaimable,
};
pub use pvc_protection::{PVC_PROTECTION_FINALIZER, PvcProtectionController};
pub use reads::{DeclaresReads, Reads, gvk};
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
    CasEnv, GENERATION, StatusEdit, StatusEditOutcome, StatusWriteOutcome, StoreCasEnv,
    edit_status_cas, generation_of, pod_is_ready, resource_version_of, upsert_condition_cas,
    write_status_cas,
};
pub use store_ledger::{DEFAULT_RECEIPT_NAMESPACE, StoreBackedLedger};
pub use store_resolver::StoreBackedNodeResolver;
pub use sweep::{ItemFailure, ObjectOutcome, Sweep, SweepAbort, SweepReport};
pub use tiered_build::TieredBuildBackend;
pub use tiered_reconciler::{PromotionScope, StaticPromotionScope, TieredCacheReconciler};
pub use watch_driver::{
    ConsecutiveFailures, KindFilter, RESUBSCRIBE, TRANSIENT_RETRY, WatchDriver, WatchDriverConfig,
    next_wake,
};
