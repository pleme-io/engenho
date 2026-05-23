//! # engenho-fonte
//!
//! The **live source-of-truth reconciler**. Takes a typed declaration
//! (a `(defsistema …)` tlisp form, a tatara-script, any `TypescapeValue`
//! crossing the bridge from `engenho-sui-typescape`) and drives the
//! cluster — across consensus, attestation, reconciliation, observation
//! — to *be* what the declaration *says*.
//!
//! Named for Portuguese **fonte** ("source", "spring", "font"): this
//! crate is where every change to a system declaration originates,
//! before any subsystem on either side sees it.
//!
//! ## The five typed roles
//!
//! ```text
//!  Watcher      → Evaluator    → Proposer     → Attester    → Publisher
//!  ----------     -----------    -----------    ----------    ---------
//!  shikumi        sui            revoada        tameshi       mirante
//!  notify         (defsistema…)  Raft propose   +sekiban      ObservationChannel
//!                                                 +kensa
//!  ChangeEvent    TypescapeValue ProposalId     Receipt       Snapshot
//! ```
//!
//! Each role is a typed [`async_trait`] with mock impls shipped by
//! default. Consumer wires them under a [`Conduit`] supervisor that
//! runs the seven-beat Viggy convergence tick:
//!
//!   Observe → Diff → Classify → Decide → Act → Attest → Tick
//!
//! ## What's in this crate (M0)
//!
//!   - The five typed traits + their typed error enums.
//!   - Mock implementations (`MockWatcher`, `MockEvaluator`, …)
//!     suitable for property tests + integration tests.
//!   - The [`Conduit`] supervisor — a `shigoto`-shaped DAG wiring the
//!     five traits end-to-end.
//!   - The [`Change`] / [`Decision`] / [`Outcome`] typed event types
//!     that flow through the pipeline.
//!
//! ## What's NOT in this crate yet
//!
//!   - Real shikumi-notify integration (M1.1 — feature `with-shikumi`)
//!   - Real sui evaluator integration (M1.2 — feature `with-sui-eval`)
//!   - Real engenho-revoada Face-bound Proposer (M1.3 — feature `with-revoada`)
//!   - Real tameshi attestation chain (M1.4 — feature `with-tameshi`)
//!   - Real mirante channel publish (M1.5 — feature `with-mirante`)
//!
//! Each integration ships as a focused PR adding one feature flag +
//! one impl. The mock universe stays the always-on default so
//! cross-substrate tests never depend on a real cluster.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod anomaly;
mod attester;
#[cfg(feature = "with-caixa")]
mod caixa_app_reconciler;
mod change;
#[cfg(feature = "with-promessa")]
mod compliance_controller;
mod conduit;
mod controller;
#[cfg(feature = "with-promessa")]
mod cost_budget_controller;
#[cfg(feature = "with-promessa")]
mod customer_kpi_controller;
mod defsistema;
mod error;
mod evaluator;
#[cfg(feature = "with-revoada")]
mod face_federated_watcher;
mod federation;
mod gossip;
mod kube_app_reconciler;
mod linhagem_chain;
mod mirante_publisher;
#[cfg(feature = "with-tameshi")]
mod outcome_chain;
#[cfg(feature = "with-promessa")]
mod promessa_reconciler;
mod proposer;
mod provacao_conduit;
mod provisioning;
mod publisher;
mod remediation;
#[cfg(feature = "with-revoada")]
mod revoada;
#[cfg(feature = "with-promessa")]
mod security_controller;
#[cfg(feature = "with-shigoto")]
mod shigoto_provisioning;
#[cfg(feature = "with-shikumi")]
mod shikumi;
mod sistema;
#[cfg(feature = "with-promessa")]
mod sla_controller;
mod subprocess_stage;
#[cfg(feature = "with-sui-eval")]
mod sui_evaluator;
#[cfg(feature = "with-tameshi")]
mod tameshi_attester;
#[cfg(feature = "with-tatara-lisp")]
mod tatara_lisp_domain;
mod typed_config_stage;
#[cfg(feature = "with-sui-eval")]
mod typed_nix_stage;
mod watcher;

pub use anomaly::{AnomalyChain, AnomalyEntry, AnomalyEvent, MockAnomalyChain};
pub use attester::{Attester, MockAttester, Receipt};
#[cfg(feature = "with-caixa")]
pub use caixa_app_reconciler::CaixaAppReconciler;
pub use change::{Change, ChangeKind, Decision, Outcome};
#[cfg(feature = "with-promessa")]
pub use compliance_controller::{
    ComplianceController, ComplianceDrift, ComplianceSnapshot, ComplianceSpec,
};
pub use conduit::Conduit;
pub use controller::{
    AppReconciler, InfraReconciler, MockAppReconciler, MockInfraReconciler, MockPromessaReconciler,
    MockTopologyReconciler, PromessaReconciler, SystemController, TopologyReconciler,
    mock_system_controller,
};
#[cfg(feature = "with-promessa")]
pub use cost_budget_controller::{
    CostBudgetController, CostBudgetDrift, CostBudgetSnapshot, CostBudgetSpec,
};
#[cfg(feature = "with-promessa")]
pub use customer_kpi_controller::{
    CustomerKpiController, CustomerKpiDrift, CustomerKpiSnapshot, CustomerKpiSpec,
};
#[cfg(feature = "with-sui-eval")]
pub use defsistema::parse_nix;
pub use defsistema::{SistemaBuilder, parse_json, to_authoring_form};
pub use error::{FonteError, FonteResult};
pub use evaluator::{Evaluator, MockEvaluator};
#[cfg(feature = "with-revoada")]
pub use face_federated_watcher::FaceFederatedWatcher;
pub use federation::{FederatedWatcher, FederationBroker};
pub use gossip::GossipBroker;
pub use kube_app_reconciler::KubeAppReconciler;
pub use linhagem_chain::LinhagemAnomalyChain;
pub use mirante_publisher::MirantePublisher;
#[cfg(feature = "with-tameshi")]
pub use outcome_chain::{OutcomeChainRecorder, TameshiOutcomeChain};
#[cfg(feature = "with-promessa")]
pub use promessa_reconciler::{PromessaIntent, PromessaTargetReconciler, promessa_kind_to_target};
pub use proposer::{MockProposer, ProposalId, Proposer};
pub use provacao_conduit::{FonteFault, ProvacaoConduit};
pub use provisioning::{
    MockProvisioningStage, ProvisioningController, ProvisioningReport, ProvisioningStage,
    StageKind, mock_provisioning_controller,
};
pub use publisher::{MockPublisher, Publisher};
pub use remediation::{
    AnomalyHandler, AnomalyRouter, MockAnomalyHandler, RemediationPolicy, mock_anomaly_router,
};
#[cfg(feature = "with-revoada")]
pub use revoada::{FaceGossipBroker, RevoadaProposer};
#[cfg(feature = "with-promessa")]
pub use security_controller::{SecurityController, SecurityDrift, SecuritySnapshot, SecuritySpec};
#[cfg(feature = "with-shigoto")]
pub use shigoto_provisioning::{
    ShigotoProvisioningController, StageDag, StageDagBuilder, stage_kind_to_job_id,
};
#[cfg(feature = "with-shikumi")]
pub use shikumi::ShikumiWatcher;
pub use sistema::{
    AppRef, InfraBackend, InfraRef, PromessaKind, PromessaRef, Sistema, TopologyRef,
};
#[cfg(feature = "with-promessa")]
pub use sla_controller::{SlaController, SlaDrift, SlaSnapshot, SlaSpec};
pub use subprocess_stage::{
    SubprocessInvocation, SubprocessStage, magma_cloud_stage, pangea_networking_stage,
};
#[cfg(feature = "with-sui-eval")]
pub use sui_evaluator::SuiEvaluator;
#[cfg(feature = "with-tameshi")]
pub use tameshi_attester::TameshiAttester;
#[cfg(feature = "with-tatara-lisp")]
pub use tatara_lisp_domain::parse_tlisp;
pub use typed_config_stage::TypedConfigStage;
#[cfg(feature = "with-sui-eval")]
pub use typed_nix_stage::TypedNixStage;
pub use watcher::{MockWatcher, Watcher};
