//! The `SystemController` — a typed `Proposer` impl that decomposes
//! a `Sistema` decision into per-sub-primitive reconciles.
//!
//! This is the *fan-out* point of the live-config loop: a single
//! `(defsistema …)` change becomes N parallel typed reconciliations
//! (one per app, infra, promise, topology slot). Each sub-reconciler
//! reports a typed outcome; the SystemController aggregates them into
//! a single ProposalId returned to the Conduit.
//!
//! ## Real wiring (M3.1+)
//!
//! The four `Mock<…>Reconciler` types in this module ship as the
//! always-on default so tests can drive the convergence loop without
//! a real cluster. Behind feature flags, real reconcilers will plug
//! in:
//!
//!   - `MockAppReconciler` → `engenho-controllers::CaixaReconciler`
//!     (consumes `caixa-renderer` to materialize Deployment/Service)
//!   - `MockInfraReconciler` → `magma` / `pangea-operator` /
//!     `crossplane`-backed reconciler per InfraBackend
//!   - `MockPromessaReconciler` → viggy's per-promessa controller
//!     (engenho's PromessaCR + AnomalyChain)
//!   - `MockTopologyReconciler` → revoada's `FabricFace` shape
//!     reconciler (Raft topology shift)
//!
//! Pattern #7 (concrete-first, trait-back constructors): each sub-
//! reconciler is a small typed trait; the SystemController takes them
//! as `Arc<dyn Trait>`.

use crate::{
    AnomalyChain, AnomalyEvent, AppRef, Decision, FonteError, FonteResult, InfraRef, PromessaRef,
    ProposalId, Proposer, Sistema, TopologyRef,
};
use async_trait::async_trait;
use engenho_sui_typescape::Typescape;
use futures::future::join_all;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

// ── Sub-reconciler traits (one per sub-primitive kind) ──────────

/// Reconciles a single workload reference (caixa app).
#[async_trait]
pub trait AppReconciler: Send + Sync {
    /// Bring the app reference's desired shape into reality. Returns
    /// `Ok(())` when the reconcile reaches a steady state.
    async fn reconcile_app(&self, app: &AppRef) -> FonteResult<()>;
}

/// Reconciles a single infrastructure reference (magma/pangea/crossplane).
#[async_trait]
pub trait InfraReconciler: Send + Sync {
    /// Apply / observe the infra primitive's desired state.
    async fn reconcile_infra(&self, infra: &InfraRef) -> FonteResult<()>;
}

/// Reconciles a viggy promessa reference.
#[async_trait]
pub trait PromessaReconciler: Send + Sync {
    /// Configure the per-promessa controller to chase `target`.
    async fn reconcile_promessa(&self, promessa: &PromessaRef) -> FonteResult<()>;
}

/// Reconciles a revoada topology reference (the cluster's fabric
/// face shape).
#[async_trait]
pub trait TopologyReconciler: Send + Sync {
    /// Shift the cluster topology toward `topology`.
    async fn reconcile_topology(&self, topology: &TopologyRef) -> FonteResult<()>;
}

// ── SystemController — composes the four reconcilers + impls Proposer ──

/// The system-level controller. Implements [`Proposer`]: every
/// `Decision` flowing through the Conduit is decoded into a `Sistema`,
/// fanned out to the four sub-reconcilers, and aggregated into a
/// single proposal id.
pub struct SystemController {
    apps: Arc<dyn AppReconciler>,
    infra: Arc<dyn InfraReconciler>,
    promises: Arc<dyn PromessaReconciler>,
    topology: Arc<dyn TopologyReconciler>,
    anomalies: Option<Arc<dyn AnomalyChain>>,
    next_id: AtomicU64,
    last_applied: Mutex<Option<Sistema>>,
}

impl SystemController {
    /// Build from four typed sub-reconcilers. Anomaly detection is
    /// opt-in via [`Self::with_anomaly_chain`].
    #[must_use]
    pub fn new(
        apps: Arc<dyn AppReconciler>,
        infra: Arc<dyn InfraReconciler>,
        promises: Arc<dyn PromessaReconciler>,
        topology: Arc<dyn TopologyReconciler>,
    ) -> Self {
        Self {
            apps,
            infra,
            promises,
            topology,
            anomalies: None,
            next_id: AtomicU64::new(0),
            last_applied: Mutex::new(None),
        }
    }

    /// Enable typed drift detection. After every reconcile, the diff
    /// against `last_applied` (or an empty Sistema for the first
    /// reconcile) is recorded into the chain as a sequence of typed
    /// [`AnomalyEvent`]s. Real wiring to viggy's AnomalyController
    /// ships behind feature flags later.
    #[must_use]
    pub fn with_anomaly_chain(mut self, chain: Arc<dyn AnomalyChain>) -> Self {
        self.anomalies = Some(chain);
        self
    }

    /// Read-only snapshot of the last applied Sistema. Returns
    /// `None` before the first proposal lands.
    pub fn last_applied(&self) -> Option<Sistema> {
        self.last_applied
            .lock()
            .expect("controller poisoned")
            .clone()
    }
}

#[async_trait]
impl Proposer for SystemController {
    async fn propose(&self, decision: &Decision) -> FonteResult<ProposalId> {
        let sistema = Sistema::from_typescape_value(&decision.typed)?;
        // Fan out to the four sub-reconcilers concurrently. Each
        // returns FonteResult<()>; the first error short-circuits
        // the proposal (the Conduit surfaces the error upstream so
        // the Watcher's next change retries from a clean state).
        let app_futs = sistema
            .apps
            .iter()
            .map(|a| self.apps.reconcile_app(a))
            .collect::<Vec<_>>();
        let infra_futs = sistema
            .infra
            .iter()
            .map(|i| self.infra.reconcile_infra(i))
            .collect::<Vec<_>>();
        let promise_futs = sistema
            .promises
            .iter()
            .map(|p| self.promises.reconcile_promessa(p))
            .collect::<Vec<_>>();
        let topology_fut = self.topology.reconcile_topology(&sistema.topology);

        // Drive all in parallel.
        let (app_res, infra_res, promise_res, topo_res) = futures::join!(
            join_all(app_futs),
            join_all(infra_futs),
            join_all(promise_futs),
            topology_fut,
        );

        // Collect errors. If anything failed, refuse to advance the
        // proposal id — the Sistema is not in steady state yet.
        for r in app_res.into_iter().chain(infra_res).chain(promise_res) {
            r?;
        }
        topo_res?;

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);

        // Diff + record anomalies BEFORE swapping last_applied so the
        // chain sees the transition from prev → next, not next → next.
        let prev = self
            .last_applied
            .lock()
            .expect("controller poisoned")
            .clone();
        if let Some(chain) = &self.anomalies {
            let events = match &prev {
                Some(p) => AnomalyEvent::diff(p, &sistema),
                None => AnomalyEvent::diff(&empty_sistema(&sistema.name), &sistema),
            };
            if !events.is_empty() {
                chain.record(decision.change.revision, events).await?;
            }
        }
        *self.last_applied.lock().expect("controller poisoned") = Some(sistema);
        Ok(id)
    }
}

/// An empty Sistema with the given name — used as the synthetic
/// `prev` for the first reconcile so AnomalyEvent::diff treats every
/// declared sub-primitive as an addition (not a noop).
fn empty_sistema(name: &Arc<str>) -> Sistema {
    Sistema {
        name: name.clone(),
        apps: Vec::new(),
        infra: Vec::new(),
        promises: Vec::new(),
        topology: TopologyRef {
            strategy: "solo".into(),
            nodes: 0,
        },
    }
}

// ── Mock reconcilers (always available) ─────────────────────────

/// Mock that records every reconcile invocation in a Vec.
#[derive(Debug, Default)]
pub struct MockAppReconciler {
    log: Mutex<Vec<AppRef>>,
}

impl MockAppReconciler {
    /// New mock.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    /// Read the recorded reconcile log.
    pub fn log(&self) -> Vec<AppRef> {
        self.log.lock().expect("mock poisoned").clone()
    }
}

#[async_trait]
impl AppReconciler for MockAppReconciler {
    async fn reconcile_app(&self, app: &AppRef) -> FonteResult<()> {
        self.log.lock().expect("mock poisoned").push(app.clone());
        Ok(())
    }
}

/// Mock infra reconciler.
#[derive(Debug, Default)]
pub struct MockInfraReconciler {
    log: Mutex<Vec<InfraRef>>,
}

impl MockInfraReconciler {
    /// New mock.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    /// Read the recorded reconcile log.
    pub fn log(&self) -> Vec<InfraRef> {
        self.log.lock().expect("mock poisoned").clone()
    }
}

#[async_trait]
impl InfraReconciler for MockInfraReconciler {
    async fn reconcile_infra(&self, infra: &InfraRef) -> FonteResult<()> {
        self.log.lock().expect("mock poisoned").push(infra.clone());
        Ok(())
    }
}

/// Mock promessa reconciler.
#[derive(Debug, Default)]
pub struct MockPromessaReconciler {
    log: Mutex<Vec<PromessaRef>>,
}

impl MockPromessaReconciler {
    /// New mock.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    /// Read the recorded reconcile log.
    pub fn log(&self) -> Vec<PromessaRef> {
        self.log.lock().expect("mock poisoned").clone()
    }
}

#[async_trait]
impl PromessaReconciler for MockPromessaReconciler {
    async fn reconcile_promessa(&self, promessa: &PromessaRef) -> FonteResult<()> {
        self.log
            .lock()
            .expect("mock poisoned")
            .push(promessa.clone());
        Ok(())
    }
}

/// Mock topology reconciler.
#[derive(Debug, Default)]
pub struct MockTopologyReconciler {
    log: Mutex<Vec<TopologyRef>>,
}

impl MockTopologyReconciler {
    /// New mock.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    /// Read the recorded reconcile log.
    pub fn log(&self) -> Vec<TopologyRef> {
        self.log.lock().expect("mock poisoned").clone()
    }
}

#[async_trait]
impl TopologyReconciler for MockTopologyReconciler {
    async fn reconcile_topology(&self, topology: &TopologyRef) -> FonteResult<()> {
        self.log
            .lock()
            .expect("mock poisoned")
            .push(topology.clone());
        Ok(())
    }
}

/// Convenience: build a SystemController whose sub-reconcilers are
/// all Mocks. Returns the controller plus handles to every mock so
/// tests can read their logs.
#[must_use]
pub fn mock_system_controller() -> (
    Arc<MockAppReconciler>,
    Arc<MockInfraReconciler>,
    Arc<MockPromessaReconciler>,
    Arc<MockTopologyReconciler>,
    SystemController,
) {
    let apps = Arc::new(MockAppReconciler::new());
    let infra = Arc::new(MockInfraReconciler::new());
    let promises = Arc::new(MockPromessaReconciler::new());
    let topology = Arc::new(MockTopologyReconciler::new());
    let ctrl = SystemController::new(
        apps.clone(),
        infra.clone(),
        promises.clone(),
        topology.clone(),
    );
    (apps, infra, promises, topology, ctrl)
}

// `FonteError` is re-exported for callers; suppress unused-import
// warning when no error-handling demo lives in this module.
#[allow(dead_code)]
const _: Option<FonteError> = None;
