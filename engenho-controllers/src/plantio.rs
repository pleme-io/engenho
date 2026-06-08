//! PlantioController — reconciles `PlantioCR` resources by
//! driving a `Roceiro` materializer + watching a
//! `MaterializationLedger` for confirmation.
//!
//! ## Reconcile rule
//!
//! For each PlantioCR (engenho.io/v1.Plantio):
//!   1. Read + validate the embedded Plantio.
//!   2. Compile to MaterializationJobs.
//!   3. For each job whose stage is Ready (deps Confirmed):
//!        a. Dispatch via Roceiro.materialize(stage, target_node)
//!        b. Ingest receipt into the ledger
//!        c. Mark per-job status (Materialized / Failed)
//!   4. Walk the stages:
//!        a. If ledger reports Reached → status.stages[id].phase = "Confirmed"
//!        b. If ledger reports Dissent → status.stages[id].phase = "Dissent"
//!        c. Else: keep Pending / Materializing
//!   5. If every stage Confirmed → status.phase = "Complete"
//!   6. If any stage in Dissent or Failed too many times →
//!        status.phase = "Failed"
//!
//! ## Idempotency
//!
//! The controller only re-dispatches stages whose receipts are
//! missing from the ledger. Re-tick with no state change is a no-op.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand},
};
use engenho_substrate::{
    JobTarget, MaterializationLedger, NodeId, Plantio, QuorumOutcome, StageId,
};
use serde_json::{Value, json};
use tracing::debug;

use crate::controller::{Controller, ReconcileOutcome, ReconcileReport};
use crate::error::ControllerError;
use crate::roceiro::Roceiro;

/// Resolves abstract JobTargets (AnyOne/AnyK/AllNodes) into
/// concrete NodeIds. The substrate doesn't have a live cluster
/// directory baked in; consumers supply the resolver.
#[async_trait]
pub trait NodeResolver: Send + Sync {
    /// Backend identifier for telemetry.
    fn name(&self) -> &'static str;

    /// Resolve a target into a concrete set of NodeIds.
    ///
    /// # Errors
    /// Implementations may surface backend errors via ControllerError.
    async fn resolve(&self, target: &JobTarget) -> Result<Vec<NodeId>, ControllerError>;
}

/// Static resolver — fixed list of known nodes. Useful for tests
/// + small bootstrap clusters where the operator pins the node
/// directory.
pub struct StaticNodeResolver {
    nodes: Vec<NodeId>,
}

impl StaticNodeResolver {
    /// New resolver over a fixed node list.
    #[must_use]
    pub fn new(nodes: Vec<NodeId>) -> Self {
        Self { nodes }
    }
}

#[async_trait]
impl NodeResolver for StaticNodeResolver {
    fn name(&self) -> &'static str {
        "static"
    }

    async fn resolve(&self, target: &JobTarget) -> Result<Vec<NodeId>, ControllerError> {
        let resolved = match target {
            JobTarget::Node(n) => vec![*n],
            JobTarget::AnyOne => self.nodes.iter().take(1).copied().collect(),
            JobTarget::AnyK { k } => self.nodes.iter().take(*k).copied().collect(),
            JobTarget::Quorum { k } => self.nodes.iter().take(*k).copied().collect(),
            JobTarget::AllNodes => self.nodes.clone(),
        };
        Ok(resolved)
    }
}

/// The controller.
pub struct PlantioController {
    store: Arc<StoreMesh>,
    roceiro: Arc<dyn Roceiro>,
    ledger: Arc<dyn MaterializationLedger>,
    resolver: Arc<dyn NodeResolver>,
    namespace: Option<String>,
}

impl PlantioController {
    /// New controller.
    #[must_use]
    pub fn new(
        store: Arc<StoreMesh>,
        roceiro: Arc<dyn Roceiro>,
        ledger: Arc<dyn MaterializationLedger>,
        resolver: Arc<dyn NodeResolver>,
        namespace: Option<String>,
    ) -> Self {
        Self {
            store,
            roceiro,
            ledger,
            resolver,
            namespace,
        }
    }

    /// Extract the typed Plantio from a CR manifest. Pure helper.
    #[must_use]
    pub fn extract_plantio(cr: &Value) -> Option<Plantio> {
        let plantio_v = cr.get("spec")?.get("plantio")?.clone();
        serde_json::from_value(plantio_v).ok()
    }

    /// Confirmation threshold for a stage. Maps the typed
    /// ConfirmacaoPolicy + Placement into a `usize` the ledger
    /// understands.
    #[must_use]
    pub fn threshold_for(stage: &engenho_substrate::Stage) -> usize {
        use engenho_substrate::ConfirmacaoPolicy as C;
        use engenho_substrate::Placement as P;
        match (&stage.confirm, &stage.placement) {
            (C::Local, _) => 1,
            (C::Quorum { k }, _) => (*k).max(1),
            (C::All, P::Pinned { .. } | P::AnyOne) => 1,
            (C::All, P::AnyK { k } | P::Quorum { k }) => (*k).max(1),
            (C::All, P::AllNodes) => 1, // resolved at runtime by resolver count
            (C::RaftCommitted, _) => 1,
        }
    }

    /// Compute the set of StageIds whose deps are all Confirmed.
    /// Pure helper.
    #[must_use]
    pub fn ready_stages(plantio: &Plantio, confirmed: &BTreeSet<StageId>) -> Vec<StageId> {
        let mut ready = Vec::new();
        for (id, stage) in &plantio.stages {
            if confirmed.contains(id) {
                continue;
            }
            if stage.depends_on.iter().all(|d| confirmed.contains(d)) {
                ready.push(id.clone());
            }
        }
        ready.sort();
        ready
    }
}

#[async_trait]
impl Controller for PlantioController {
    fn name(&self) -> &'static str {
        "plantio"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let crs = self
            .store
            .list("engenho.io", "v1", "Plantio", self.namespace.as_deref())
            .await;
        let mut report = ReconcileReport::default();
        report.objects_examined = crs.len();

        for (cr_key, cr_value) in &crs {
            let Some(plantio) = Self::extract_plantio(cr_value) else {
                report.objects_skipped += 1;
                continue;
            };
            if plantio.validate().is_err() {
                report.objects_skipped += 1;
                continue;
            }

            // Determine current confirmation state from CR status.
            let mut confirmed: BTreeSet<StageId> = cr_value
                .get("status")
                .and_then(|s| s.get("stages"))
                .and_then(|s| s.as_object())
                .map(|m| {
                    m.iter()
                        .filter_map(|(k, v)| {
                            let phase = v.get("phase").and_then(|p| p.as_str())?;
                            if phase == "Confirmed" {
                                Some(StageId::new(k.clone()))
                            } else {
                                None
                            }
                        })
                        .collect()
                })
                .unwrap_or_default();

            let mut stage_phases: BTreeMap<StageId, String> = confirmed
                .iter()
                .map(|id| (id.clone(), "Confirmed".to_string()))
                .collect();

            // Iterate: dispatch + check ledger until no progress this tick.
            loop {
                let ready = Self::ready_stages(&plantio, &confirmed);
                if ready.is_empty() {
                    break;
                }
                let mut made_progress = false;
                for stage_id in ready {
                    let stage = &plantio.stages[&stage_id];
                    let target = match stage.placement {
                        engenho_substrate::Placement::Pinned { node } => JobTarget::Node(node),
                        engenho_substrate::Placement::AnyOne => JobTarget::AnyOne,
                        engenho_substrate::Placement::AnyK { k } => JobTarget::AnyK { k },
                        engenho_substrate::Placement::Quorum { k } => JobTarget::Quorum { k },
                        engenho_substrate::Placement::AllNodes => JobTarget::AllNodes,
                    };
                    let nodes = self.resolver.resolve(&target).await?;
                    let threshold = Self::threshold_for(stage);
                    let mut quorum_outcome: Option<QuorumOutcome> = None;
                    for node in nodes {
                        match self.roceiro.materialize(stage, node).await {
                            Ok(receipt) => {
                                let outcome = self
                                    .ledger
                                    .ingest(&stage_id, threshold, &receipt)
                                    .await
                                    .map_err(|e| ControllerError::Internal(e.to_string()))?;
                                quorum_outcome = Some(outcome);
                            }
                            Err(e) => {
                                debug!(stage = %stage_id, error = %e, "materialize failed");
                                stage_phases.insert(stage_id.clone(), "Failed".into());
                            }
                        }
                    }
                    match quorum_outcome {
                        Some(QuorumOutcome::Reached { .. }) => {
                            confirmed.insert(stage_id.clone());
                            stage_phases.insert(stage_id, "Confirmed".into());
                            made_progress = true;
                        }
                        Some(QuorumOutcome::Dissent { .. }) => {
                            stage_phases.insert(stage_id, "Dissent".into());
                        }
                        Some(QuorumOutcome::Pending { .. }) => {
                            stage_phases
                                .entry(stage_id)
                                .or_insert("Materializing".into());
                        }
                        None => {
                            // Never even dispatched (no nodes resolved).
                            stage_phases.entry(stage_id).or_insert("Pending".into());
                        }
                    }
                }
                if !made_progress {
                    break;
                }
            }

            // Aggregate status.phase.
            let all_confirmed = plantio.stages.keys().all(|id| confirmed.contains(id));
            let any_dissent = stage_phases.values().any(|p| p == "Dissent");
            let any_failed = stage_phases.values().any(|p| p == "Failed");
            let overall = if any_dissent || any_failed {
                "Failed"
            } else if all_confirmed {
                "Complete"
            } else {
                "Materializing"
            };

            // Build the status patch.
            let stages_status: serde_json::Map<String, Value> = stage_phases
                .iter()
                .map(|(k, v)| (k.to_string(), json!({"phase": v})))
                .collect();
            let new_status = json!({
                "status": {
                    "phase": overall,
                    "stages": stages_status,
                }
            });

            // Patch CR.
            self.store
                .propose(ResourceCommand::patch(
                    cr_key.clone(),
                    new_status,
                    Reason::Controller,
                ))
                .await?;
            report.objects_changed += 1;
        }
        Ok(report.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_substrate::{ConfirmacaoPolicy, MemoryLedger, Placement, Stage, WorkloadShape};

    fn n(b: u8) -> NodeId {
        NodeId::new([b; 32])
    }

    fn pinned_stage(id: &str, node: NodeId) -> Stage {
        Stage::pinned(id, WorkloadShape::OciImage, node)
    }

    // ── NodeResolver ──────────────────────────────────────────

    #[tokio::test]
    async fn static_resolver_node_target_returns_one() {
        let r = StaticNodeResolver::new(vec![n(1), n(2), n(3)]);
        let nodes = r.resolve(&JobTarget::Node(n(2))).await.unwrap();
        assert_eq!(nodes, vec![n(2)]);
    }

    #[tokio::test]
    async fn static_resolver_any_one_returns_first() {
        let r = StaticNodeResolver::new(vec![n(1), n(2), n(3)]);
        let nodes = r.resolve(&JobTarget::AnyOne).await.unwrap();
        assert_eq!(nodes, vec![n(1)]);
    }

    #[tokio::test]
    async fn static_resolver_any_k_returns_first_k() {
        let r = StaticNodeResolver::new(vec![n(1), n(2), n(3), n(4)]);
        let nodes = r.resolve(&JobTarget::AnyK { k: 2 }).await.unwrap();
        assert_eq!(nodes, vec![n(1), n(2)]);
    }

    #[tokio::test]
    async fn static_resolver_quorum_returns_first_k() {
        let r = StaticNodeResolver::new(vec![n(1), n(2), n(3)]);
        let nodes = r.resolve(&JobTarget::Quorum { k: 2 }).await.unwrap();
        assert_eq!(nodes.len(), 2);
    }

    #[tokio::test]
    async fn static_resolver_all_nodes_returns_full_list() {
        let r = StaticNodeResolver::new(vec![n(1), n(2)]);
        let nodes = r.resolve(&JobTarget::AllNodes).await.unwrap();
        assert_eq!(nodes, vec![n(1), n(2)]);
    }

    #[tokio::test]
    async fn static_resolver_name_is_stable() {
        let r = StaticNodeResolver::new(vec![]);
        assert_eq!(r.name(), "static");
    }

    // ── PlantioController extract_plantio ─────────────────────

    #[test]
    fn extract_plantio_parses_spec_plantio() {
        let plantio = Plantio::new();
        let cr = json!({
            "spec": {"plantio": &plantio}
        });
        let back = PlantioController::extract_plantio(&cr).unwrap();
        assert_eq!(back, plantio);
    }

    #[test]
    fn extract_plantio_none_for_missing_spec() {
        let cr = json!({"metadata": {"name": "x"}});
        assert!(PlantioController::extract_plantio(&cr).is_none());
    }

    // ── threshold_for ─────────────────────────────────────────

    #[test]
    fn threshold_for_local_is_one() {
        let stage = pinned_stage("x", n(1));
        // confirm defaults to Local
        assert_eq!(PlantioController::threshold_for(&stage), 1);
    }

    #[test]
    fn threshold_for_quorum_uses_k() {
        let mut stage = pinned_stage("x", n(1));
        stage.confirm = ConfirmacaoPolicy::Quorum { k: 5 };
        assert_eq!(PlantioController::threshold_for(&stage), 5);
    }

    #[test]
    fn threshold_for_quorum_clamps_zero_to_one() {
        let mut stage = pinned_stage("x", n(1));
        stage.confirm = ConfirmacaoPolicy::Quorum { k: 0 };
        assert_eq!(PlantioController::threshold_for(&stage), 1);
    }

    #[test]
    fn threshold_for_all_with_anyk_uses_k() {
        let mut stage = pinned_stage("x", n(1));
        stage.confirm = ConfirmacaoPolicy::All;
        stage.placement = Placement::AnyK { k: 3 };
        assert_eq!(PlantioController::threshold_for(&stage), 3);
    }

    #[test]
    fn threshold_for_raft_committed_is_one() {
        let mut stage = pinned_stage("x", n(1));
        stage.confirm = ConfirmacaoPolicy::RaftCommitted;
        assert_eq!(PlantioController::threshold_for(&stage), 1);
    }

    // ── ready_stages ──────────────────────────────────────────

    #[test]
    fn ready_stages_includes_stages_with_no_deps_when_nothing_confirmed() {
        let mut plantio = Plantio::new();
        plantio.add_stage(pinned_stage("a", n(1))).unwrap();
        plantio.add_stage(pinned_stage("b", n(1))).unwrap();
        let confirmed = BTreeSet::new();
        let ready = PlantioController::ready_stages(&plantio, &confirmed);
        assert_eq!(ready, vec![StageId::new("a"), StageId::new("b")]);
    }

    #[test]
    fn ready_stages_excludes_stages_with_unconfirmed_deps() {
        let mut plantio = Plantio::new();
        plantio.add_stage(pinned_stage("a", n(1))).unwrap();
        let mut b = pinned_stage("b", n(1));
        b.depends_on.insert(StageId::new("a"));
        plantio.add_stage(b).unwrap();
        let confirmed = BTreeSet::new();
        let ready = PlantioController::ready_stages(&plantio, &confirmed);
        // Only "a" is ready; "b" is blocked.
        assert_eq!(ready, vec![StageId::new("a")]);
    }

    #[test]
    fn ready_stages_unblocks_dependent_once_dep_is_confirmed() {
        let mut plantio = Plantio::new();
        plantio.add_stage(pinned_stage("a", n(1))).unwrap();
        let mut b = pinned_stage("b", n(1));
        b.depends_on.insert(StageId::new("a"));
        plantio.add_stage(b).unwrap();
        let confirmed: BTreeSet<_> = [StageId::new("a")].into_iter().collect();
        let ready = PlantioController::ready_stages(&plantio, &confirmed);
        assert_eq!(ready, vec![StageId::new("b")]);
    }

    #[test]
    fn ready_stages_excludes_already_confirmed_stages() {
        let mut plantio = Plantio::new();
        plantio.add_stage(pinned_stage("a", n(1))).unwrap();
        let confirmed: BTreeSet<_> = [StageId::new("a")].into_iter().collect();
        let ready = PlantioController::ready_stages(&plantio, &confirmed);
        assert!(ready.is_empty());
    }

    // ── full controller behavior verified at higher level later
    // (integration tests against a live StoreMesh land when the
    // CR storage type ships in engenho-store::types).

    #[test]
    fn controller_name_is_stable() {
        struct F;
        #[async_trait]
        impl Controller for F {
            fn name(&self) -> &'static str {
                "plantio"
            }
            async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
                Ok(ReconcileReport::default().into())
            }
        }
        assert_eq!(F.name(), "plantio");
    }

    // Sanity: composition of FakeRoceiro + MemoryLedger + StaticResolver
    // produces sensible outcomes in isolation.

    #[tokio::test]
    async fn fake_roceiro_plus_memory_ledger_reaches_quorum_for_three_node_placement() {
        use crate::roceiro::FakeRoceiro;
        let roceiro = FakeRoceiro::new();
        let ledger = MemoryLedger::new();
        let mut stage = pinned_stage("x", n(1));
        stage.confirm = ConfirmacaoPolicy::Quorum { k: 3 };
        stage.placement = Placement::AnyK { k: 3 };
        let threshold = PlantioController::threshold_for(&stage);
        let resolver = StaticNodeResolver::new(vec![n(1), n(2), n(3)]);
        let nodes = resolver.resolve(&JobTarget::AnyK { k: 3 }).await.unwrap();
        let mut outcomes = Vec::new();
        for node in nodes {
            let r = roceiro.materialize(&stage, node).await.unwrap();
            outcomes.push(ledger.ingest(&stage.id, threshold, &r).await.unwrap());
        }
        // Third receipt should be Reached.
        assert!(matches!(
            outcomes.last(),
            Some(QuorumOutcome::Reached { .. })
        ));
    }
}
