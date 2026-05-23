//! ShigotoProvisioningController — ProvisioningController wired
//! through shigoto-dag for typed topological ordering + parallel
//! waves.
//!
//! v1.33's plain ProvisioningController walks StageKind::canonical_order()
//! linearly. ShigotoProvisioningController instead computes the DAG of
//! stages via shigoto_dag::Dag, then drives each topological wave —
//! independent stages can fire concurrently when their typed
//! dependency edges allow.
//!
//! For the current 6-stage canonical DAG, every stage depends on the
//! previous (linear chain) — so ShigotoProvisioningController produces
//! the same ordering as the linear controller. The leverage shows
//! when operators wire ADDITIONAL stages (e.g. parallel Cloud + DNS
//! once magma + pangea decouple) — they only declare the typed edges,
//! and shigoto handles the topology.
//!
//! Gated `with-shigoto`.

use crate::{FonteResult, ProvisioningReport, ProvisioningStage, Sistema, StageKind};
use shigoto_dag::Dag;
use shigoto_types::{JobId, JobKindId, JobScope, JobSubject};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// Convert a StageKind into a typed JobId for the shigoto DAG.
#[must_use]
pub fn stage_kind_to_job_id(kind: StageKind) -> JobId {
    let slug = match kind {
        StageKind::Cloud => "fonte.provision.cloud",
        StageKind::Networking => "fonte.provision.networking",
        StageKind::EngenhoInstall => "fonte.provision.engenho-install",
        StageKind::CaixaBoot => "fonte.provision.caixa-boot",
        StageKind::PromessaRegister => "fonte.provision.promessa-register",
        StageKind::FederationJoin => "fonte.provision.federation-join",
    };
    JobId {
        scope: JobScope::Global,
        kind: JobKindId::new(slug.to_string()),
        subject: JobSubject::None,
    }
}

/// Builder that constructs a typed shigoto DAG over StageKind nodes
/// with operator-declared edges.
pub struct StageDagBuilder {
    dag: Dag,
    kind_to_id: BTreeMap<StageKind, JobId>,
}

impl Default for StageDagBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl StageDagBuilder {
    /// New empty builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            dag: Dag::new(),
            kind_to_id: BTreeMap::new(),
        }
    }

    /// Add a stage as a node in the DAG.
    #[must_use]
    pub fn stage(mut self, kind: StageKind) -> Self {
        let id = stage_kind_to_job_id(kind);
        self.dag.ensure_node(id.clone());
        self.kind_to_id.insert(kind, id);
        self
    }

    /// Declare that `child` depends on `parent` (parent must
    /// complete before child fires).
    #[must_use]
    pub fn depends_on(mut self, child: StageKind, parent: StageKind) -> Self {
        let p_id = stage_kind_to_job_id(parent);
        let c_id = stage_kind_to_job_id(child);
        self.dag.ensure_node(p_id.clone());
        self.dag.ensure_node(c_id.clone());
        self.kind_to_id.insert(parent, p_id.clone());
        self.kind_to_id.insert(child, c_id.clone());
        self.dag.add_edge(p_id, c_id);
        self
    }

    /// Finalize. Returns the typed DAG.
    #[must_use]
    pub fn build(self) -> StageDag {
        StageDag {
            dag: self.dag,
            kind_to_id: self.kind_to_id,
        }
    }
}

/// Typed DAG over StageKind nodes with operator-declared edges.
pub struct StageDag {
    dag: Dag,
    kind_to_id: BTreeMap<StageKind, JobId>,
}

impl StageDag {
    /// The canonical 6-stage linear DAG matching v1.33's
    /// ProvisioningController order. Operators override for
    /// per-cluster shapes (e.g. parallel Cloud + Networking once
    /// the backends decouple).
    #[must_use]
    pub fn canonical_linear() -> Self {
        let kinds = StageKind::canonical_order();
        let mut builder = StageDagBuilder::new();
        for k in kinds {
            builder = builder.stage(*k);
        }
        for pair in kinds.windows(2) {
            builder = builder.depends_on(pair[1], pair[0]);
        }
        builder.build()
    }

    /// Compute the topological waves. Stages within the same wave
    /// can fire concurrently. Operators get parallel execution per
    /// wave for free.
    pub fn waves(&self) -> Result<Vec<Vec<StageKind>>, shigoto_dag::DagError> {
        let id_waves = self.dag.waves(None)?;
        // JobId is Hash + Eq but not Ord — use a HashMap.
        let id_to_kind: HashMap<JobId, StageKind> = self
            .kind_to_id
            .iter()
            .map(|(k, v)| (v.clone(), *k))
            .collect();
        Ok(id_waves
            .into_iter()
            .map(|wave| {
                wave.iter()
                    .filter_map(|id| id_to_kind.get(id).copied())
                    .collect::<Vec<_>>()
            })
            .collect())
    }
}

/// ProvisioningController that walks the typed shigoto DAG. Each
/// wave's stages fire concurrently via futures::join_all; waves
/// fire sequentially.
pub struct ShigotoProvisioningController {
    dag: StageDag,
    stages: BTreeMap<StageKind, Arc<dyn ProvisioningStage>>,
}

impl ShigotoProvisioningController {
    /// Build with a typed DAG + a stage impl per StageKind.
    #[must_use]
    pub fn new(dag: StageDag, stages: BTreeMap<StageKind, Arc<dyn ProvisioningStage>>) -> Self {
        Self { dag, stages }
    }

    /// Provision a cluster by walking the DAG's waves. Each wave's
    /// stages fire concurrently. First failure in a wave halts the
    /// next wave; in-progress stages in the failing wave still
    /// complete.
    pub async fn provision_cluster(&self, sistema: &Sistema) -> FonteResult<ProvisioningReport> {
        let waves = self
            .dag
            .waves()
            .map_err(|e| crate::FonteError::Propose(format!("shigoto DAG cycle: {e:?}")))?;
        let mut completed = Vec::new();
        for wave in waves {
            // Fire every stage in this wave concurrently.
            let futs = wave.iter().map(|kind| {
                let stage = self.stages.get(kind).cloned();
                async move {
                    match stage {
                        Some(s) => (*kind, s.provision(sistema).await),
                        None => (
                            *kind,
                            Err(crate::FonteError::Propose(format!(
                                "no stage registered for {kind:?}"
                            ))),
                        ),
                    }
                }
            });
            let results = futures::future::join_all(futs).await;

            // If any stage in this wave failed, return early.
            for (kind, result) in results {
                match result {
                    Ok(()) => completed.push(kind),
                    Err(e) => {
                        return Ok(ProvisioningReport {
                            sistema_name: sistema.name.clone(),
                            stages_completed: completed,
                            stage_failed: Some((kind, format!("{e}"))),
                        });
                    }
                }
            }
        }
        Ok(ProvisioningReport {
            sistema_name: sistema.name.clone(),
            stages_completed: completed,
            stage_failed: None,
        })
    }
}
