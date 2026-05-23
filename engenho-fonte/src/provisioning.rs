//! M5: ProvisioningController — declarative cluster bootstrap.
//!
//! A `(defsistema "new-cluster" …)` form for a cluster that DOESN'T
//! EXIST YET. The ProvisioningController takes the typed Sistema
//! declaration and walks the typed bootstrap stages:
//!
//!   1. **Cloud allocation** (magma): provision the underlying VMs /
//!      pods / network the cluster sits on.
//!   2. **DNS + ingress** (pangea): wire the cluster's public
//!      endpoints (auth.<cluster>.<location>.<domain>, etc).
//!   3. **engenho install**: bring up apiserver, etcd, scheduler,
//!      kubelet on the allocated nodes.
//!   4. **caixa boot**: deploy the first apps declared in the
//!      Sistema.
//!   5. **viggy promessa**: register every declared promessa with
//!      its target controller so the convergence loop starts chasing
//!      promises from minute zero.
//!   6. **Federation join** (revoada): if this cluster joins an
//!      existing federation, register with the broker so other
//!      peers see it.
//!
//! Each stage is a typed `ProvisioningStage` impl. The controller
//! runs them via a shigoto-shaped DAG (linear today; parallel
//! stages where the typed DAG allows). Mock-driven by default;
//! real backends (`with-magma`, `with-pangea`, `with-engenho-install`,
//! `with-caixa-boot`) slot in behind feature flags.
//!
//! Pattern #7 (concrete-first, trait-back constructors): each stage
//! is a typed trait; the controller takes them as `Arc<dyn Stage>`.

use crate::{FonteResult, Sistema};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::Mutex;

/// Typed identifier of a single provisioning stage. Used for
/// telemetry + DAG ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageKind {
    /// Cloud VM / pod allocation (magma layer).
    Cloud,
    /// DNS + ingress + load balancer wiring (pangea layer).
    Networking,
    /// engenho apiserver/etcd/scheduler/kubelet install.
    EngenhoInstall,
    /// caixa boot — deploy first declared apps.
    CaixaBoot,
    /// viggy promessa registration.
    PromessaRegister,
    /// Revoada federation join.
    FederationJoin,
}

impl StageKind {
    /// The canonical stage order. Each stage's prerequisites are
    /// every prior entry in this list (linear DAG today; future
    /// versions may parallelize Cloud + Networking once magma +
    /// pangea decouple).
    #[must_use]
    pub const fn canonical_order() -> &'static [StageKind] {
        &[
            Self::Cloud,
            Self::Networking,
            Self::EngenhoInstall,
            Self::CaixaBoot,
            Self::PromessaRegister,
            Self::FederationJoin,
        ]
    }
}

/// One provisioning stage. Real impls (M5.1+) wrap magma / pangea /
/// engenho-install / etc. Mock impls record (stage, sistema-name)
/// tuples for assertion.
#[async_trait]
pub trait ProvisioningStage: Send + Sync {
    /// Telemetry identifier.
    fn kind(&self) -> StageKind;

    /// Drive this stage of cluster bootstrap. Returns once the stage
    /// reaches a steady state OR errors.
    async fn provision(&self, sistema: &Sistema) -> FonteResult<()>;
}

/// Typed provisioning result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProvisioningReport {
    /// Sistema name that was provisioned.
    pub sistema_name: Arc<str>,
    /// Stages that ran successfully, in canonical order.
    pub stages_completed: Vec<StageKind>,
    /// Any stages that failed; convergence halted at the first
    /// failure.
    pub stage_failed: Option<(StageKind, String)>,
}

/// M5 ProvisioningController. Holds one Stage per StageKind; on
/// provision_cluster(sistema) walks canonical_order() and invokes
/// each in turn. First failure halts the chain.
pub struct ProvisioningController {
    cloud: Arc<dyn ProvisioningStage>,
    networking: Arc<dyn ProvisioningStage>,
    engenho_install: Arc<dyn ProvisioningStage>,
    caixa_boot: Arc<dyn ProvisioningStage>,
    promessa_register: Arc<dyn ProvisioningStage>,
    federation_join: Arc<dyn ProvisioningStage>,
}

impl ProvisioningController {
    /// Build the controller from six typed stages.
    #[must_use]
    pub fn new(
        cloud: Arc<dyn ProvisioningStage>,
        networking: Arc<dyn ProvisioningStage>,
        engenho_install: Arc<dyn ProvisioningStage>,
        caixa_boot: Arc<dyn ProvisioningStage>,
        promessa_register: Arc<dyn ProvisioningStage>,
        federation_join: Arc<dyn ProvisioningStage>,
    ) -> Self {
        Self {
            cloud,
            networking,
            engenho_install,
            caixa_boot,
            promessa_register,
            federation_join,
        }
    }

    /// Provision a new cluster from a typed Sistema declaration.
    /// Walks canonical_order(); halts at first failure.
    pub async fn provision_cluster(&self, sistema: &Sistema) -> FonteResult<ProvisioningReport> {
        let mut completed = Vec::with_capacity(StageKind::canonical_order().len());
        for stage_kind in StageKind::canonical_order() {
            let stage = self.stage(*stage_kind);
            match stage.provision(sistema).await {
                Ok(()) => completed.push(*stage_kind),
                Err(e) => {
                    return Ok(ProvisioningReport {
                        sistema_name: sistema.name.clone(),
                        stages_completed: completed,
                        stage_failed: Some((*stage_kind, format!("{e}"))),
                    });
                }
            }
        }
        Ok(ProvisioningReport {
            sistema_name: sistema.name.clone(),
            stages_completed: completed,
            stage_failed: None,
        })
    }

    fn stage(&self, kind: StageKind) -> &Arc<dyn ProvisioningStage> {
        match kind {
            StageKind::Cloud => &self.cloud,
            StageKind::Networking => &self.networking,
            StageKind::EngenhoInstall => &self.engenho_install,
            StageKind::CaixaBoot => &self.caixa_boot,
            StageKind::PromessaRegister => &self.promessa_register,
            StageKind::FederationJoin => &self.federation_join,
        }
    }
}

// ── Mock stage (always available) ───────────────────────────────

/// Mock provisioning stage — records every (kind, sistema_name)
/// tuple, optionally fails on a configured kind for failure-path
/// testing.
#[derive(Debug)]
pub struct MockProvisioningStage {
    kind: StageKind,
    log: Mutex<Vec<Arc<str>>>,
    fails: Mutex<bool>,
}

impl MockProvisioningStage {
    /// New mock that always succeeds.
    #[must_use]
    pub fn new(kind: StageKind) -> Self {
        Self {
            kind,
            log: Mutex::new(Vec::new()),
            fails: Mutex::new(false),
        }
    }

    /// Configure this mock to fail on the NEXT provision call.
    pub fn fail_next(&self) {
        *self.fails.lock().expect("mock stage poisoned") = true;
    }

    /// Read the log of sistema names this stage provisioned.
    pub fn log(&self) -> Vec<Arc<str>> {
        self.log.lock().expect("mock stage poisoned").clone()
    }
}

#[async_trait]
impl ProvisioningStage for MockProvisioningStage {
    fn kind(&self) -> StageKind {
        self.kind
    }

    async fn provision(&self, sistema: &Sistema) -> FonteResult<()> {
        let mut fails = self.fails.lock().expect("mock stage poisoned");
        if *fails {
            *fails = false;
            return Err(crate::FonteError::Propose(format!(
                "mock {:?} configured to fail",
                self.kind
            )));
        }
        self.log
            .lock()
            .expect("mock stage poisoned")
            .push(sistema.name.clone());
        Ok(())
    }
}

/// Convenience: build a ProvisioningController whose six stages are
/// all Mocks. Tests inspect each Mock's `.log()` to verify which
/// stages fired for which Sistema.
#[must_use]
#[allow(clippy::type_complexity)]
pub fn mock_provisioning_controller() -> (
    Arc<MockProvisioningStage>,
    Arc<MockProvisioningStage>,
    Arc<MockProvisioningStage>,
    Arc<MockProvisioningStage>,
    Arc<MockProvisioningStage>,
    Arc<MockProvisioningStage>,
    ProvisioningController,
) {
    let cloud = Arc::new(MockProvisioningStage::new(StageKind::Cloud));
    let networking = Arc::new(MockProvisioningStage::new(StageKind::Networking));
    let engenho_install = Arc::new(MockProvisioningStage::new(StageKind::EngenhoInstall));
    let caixa_boot = Arc::new(MockProvisioningStage::new(StageKind::CaixaBoot));
    let promessa_register = Arc::new(MockProvisioningStage::new(StageKind::PromessaRegister));
    let federation_join = Arc::new(MockProvisioningStage::new(StageKind::FederationJoin));
    let ctrl = ProvisioningController::new(
        cloud.clone(),
        networking.clone(),
        engenho_install.clone(),
        caixa_boot.clone(),
        promessa_register.clone(),
        federation_join.clone(),
    );
    (
        cloud,
        networking,
        engenho_install,
        caixa_boot,
        promessa_register,
        federation_join,
        ctrl,
    )
}
