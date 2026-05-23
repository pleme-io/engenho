//! roça — typed materialization-DAG primitive.
//!
//! `Plantio` (Portuguese: *planting*; the cleared field becomes a
//! planted field) is the substrate's typed DAG-value of stages.
//! Each `Stage` names a shape, its prerequisites, its verifiers,
//! its confirmation policy, and where to materialize it.
//!
//! ## Pure-data layer
//!
//! This module ships ONLY the typed values + topology helpers.
//! No controller, no scheduler, no runtime. The DAG compiles to
//! a list of `(StageId, NodeId)` materialization jobs the next
//! layer (`PlantioController`) dispatches.
//!
//! ## Composition with prior primitives
//!
//! - `Stage.shape: WorkloadShape` — from `crate::shape`
//! - `Stage.verify: Vec<Verificacao>` — from `crate::verifier`
//! - `Stage.confirm: ConfirmacaoPolicy` — references `QuorumTracker`
//!   (the next-round controller composes them)
//! - `Stage.depends_on: BTreeSet<StageId>` — typed topology
//! - `Stage.placement: Placement` — typed scheduler hint
//!
//! ## Topology validation
//!
//! `Plantio::validate()` returns `Ok(())` only when:
//!   * Every `depends_on` reference points to a known stage.
//!   * The DAG is acyclic (deterministic Kahn-style sort succeeds).
//!   * No two stages share a `StageId`.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::receipt::NodeId;
use crate::shape::WorkloadShape;
use crate::verifier::Verificacao;

/// Stable stage identifier within a Plantio. Operator-chosen
/// (typically human-readable like "build-image" or "deploy-pod").
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StageId(pub String);

impl StageId {
    /// Construct from a string.
    #[must_use]
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Borrow as `&str`.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for StageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Where a stage should be materialized.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Placement {
    /// Exactly one specific node.
    Pinned {
        /// Target node identity.
        node: NodeId,
    },
    /// Any one node — first available wins.
    AnyOne,
    /// Any K nodes — load-spreading without agreement requirement.
    AnyK {
        /// How many nodes (≥1).
        k: usize,
    },
    /// Quorum — K nodes, BLAKE3-agreement required (typed Dissent
    /// from `QuorumTracker` if disagreement detected).
    Quorum {
        /// Quorum threshold.
        k: usize,
    },
    /// Every node in the cluster.
    AllNodes,
}

impl Placement {
    /// Minimum nodes required for this placement to be satisfied.
    /// `AllNodes` returns `None` (depends on dynamic cluster size).
    #[must_use]
    pub fn min_nodes(&self) -> Option<usize> {
        match self {
            Self::Pinned { .. } | Self::AnyOne => Some(1),
            Self::AnyK { k } | Self::Quorum { k } => Some((*k).max(1)),
            Self::AllNodes => None,
        }
    }

    /// True if the placement requires agreement across nodes (i.e.
    /// `Quorum` or `AllNodes`).
    #[must_use]
    pub fn requires_agreement(&self) -> bool {
        matches!(self, Self::Quorum { .. } | Self::AllNodes)
    }
}

/// How the DAG decides a stage is confirmed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ConfirmacaoPolicy {
    /// Local-only — this node's own receipt suffices.
    Local,
    /// K receipts gossiped via chitchat must reach this node.
    Quorum {
        /// Receipt threshold.
        k: usize,
    },
    /// Every placement target must produce a receipt.
    All,
    /// Strong consistency — Raft commit required.
    RaftCommitted,
}

impl Default for ConfirmacaoPolicy {
    fn default() -> Self {
        Self::Local
    }
}

/// One stage of the materialization DAG.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stage {
    /// Stable identity within the Plantio.
    pub id: StageId,
    /// What shape this stage produces.
    pub shape: WorkloadShape,
    /// Stages that must reach `Confirmed` before this one starts.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub depends_on: BTreeSet<StageId>,
    /// Verification predicates — ALL must pass before Confirmed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub verify: Vec<Verificacao>,
    /// How the DAG knows the stage is confirmed.
    #[serde(default)]
    pub confirm: ConfirmacaoPolicy,
    /// Where to materialize.
    pub placement: Placement,
}

impl Stage {
    /// Minimal constructor — single-node Pinned placement.
    #[must_use]
    pub fn pinned(id: impl Into<String>, shape: WorkloadShape, node: NodeId) -> Self {
        Self {
            id: StageId::new(id),
            shape,
            depends_on: BTreeSet::new(),
            verify: Vec::new(),
            confirm: ConfirmacaoPolicy::Local,
            placement: Placement::Pinned { node },
        }
    }
}

/// Validation errors for a Plantio.
#[derive(Debug, Clone, Error)]
pub enum PlantioError {
    /// A `depends_on` reference points at an unknown StageId.
    #[error("unknown dependency: stage {stage} depends on {dependency}")]
    UnknownDependency {
        /// Stage that has the dangling reference.
        stage: StageId,
        /// The reference that doesn't resolve.
        dependency: StageId,
    },
    /// The DAG contains a cycle.
    #[error("cycle detected (remaining stages: {remaining:?})")]
    Cycle {
        /// Stages that couldn't be topologically sorted.
        remaining: Vec<StageId>,
    },
    /// A stage was added twice under the same ID.
    #[error("duplicate stage id: {0}")]
    DuplicateId(StageId),
}

crate::impl_error_kind! {
    PlantioError {
        { UnknownDependency { .. } } => "unknown_dependency",
        { Cycle { .. } } => "cycle",
        (DuplicateId(_)) => "duplicate_id",
    }
}

crate::impl_fingerprint!(Plantio);

/// Typed materialization DAG. Pure value — no runtime state.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Plantio {
    /// Stages keyed by ID. BTreeMap preserves deterministic ordering.
    pub stages: BTreeMap<StageId, Stage>,
}

impl Plantio {
    /// Empty Plantio.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a stage. Errors on duplicate ID.
    ///
    /// # Errors
    /// [`PlantioError::DuplicateId`] if a stage with the same ID
    /// is already present.
    pub fn add_stage(&mut self, stage: Stage) -> Result<&mut Self, PlantioError> {
        if self.stages.contains_key(&stage.id) {
            return Err(PlantioError::DuplicateId(stage.id));
        }
        self.stages.insert(stage.id.clone(), stage);
        Ok(self)
    }

    /// Count of stages.
    #[must_use]
    pub fn len(&self) -> usize {
        self.stages.len()
    }

    /// True if empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    /// BLAKE3 fingerprint over canonical JSON. Operators commit
    /// this to attest a particular Plantio was deployed.
    /// Same Plantio (down to stage insertion order via BTreeMap)
    /// always produces the same fingerprint.
    ///
    /// Generated via [`crate::Fingerprint`].
    #[must_use]
    pub fn fingerprint(&self) -> [u8; 32] {
        <Self as crate::Fingerprint>::fingerprint(self)
    }

    /// Observable snapshot — name + stage count. Pattern #2 SSC v0.96:
    /// `Plantio` joins TieredCache + CompositeShapeRenderer +
    /// ChainedVerifier as a 4th `ChildCountSnapshot` consumer.
    /// Dashboards can show "this Plantio has N stages."
    #[must_use]
    pub fn snapshot(&self) -> crate::mirante::ChildCountSnapshot {
        crate::mirante::ChildCountSnapshot {
            name: "plantio",
            child_count: self.len(),
        }
    }

    /// Validate topology: every depends_on resolves + the DAG is
    /// acyclic. Empty Plantio is trivially valid.
    ///
    /// # Errors
    /// [`PlantioError::UnknownDependency`] or [`PlantioError::Cycle`].
    pub fn validate(&self) -> Result<(), PlantioError> {
        // Check every dependency resolves.
        for stage in self.stages.values() {
            for dep in &stage.depends_on {
                if !self.stages.contains_key(dep) {
                    return Err(PlantioError::UnknownDependency {
                        stage: stage.id.clone(),
                        dependency: dep.clone(),
                    });
                }
            }
        }
        // Kahn's algorithm — surfaces cycles by leaving them in `remaining`.
        let _ = self.topo_sort()?;
        Ok(())
    }

    /// Topological sort. Returns stages in dependency order;
    /// each stage appears AFTER everything it depends_on.
    ///
    /// # Errors
    /// [`PlantioError::Cycle`] if the DAG is cyclic;
    /// [`PlantioError::UnknownDependency`] for dangling refs.
    pub fn topo_sort(&self) -> Result<Vec<StageId>, PlantioError> {
        // Validate dependencies first.
        for stage in self.stages.values() {
            for dep in &stage.depends_on {
                if !self.stages.contains_key(dep) {
                    return Err(PlantioError::UnknownDependency {
                        stage: stage.id.clone(),
                        dependency: dep.clone(),
                    });
                }
            }
        }
        // Compute in-degree per stage.
        let mut in_degree: BTreeMap<StageId, usize> =
            self.stages.keys().map(|k| (k.clone(), 0usize)).collect();
        for stage in self.stages.values() {
            for dep in &stage.depends_on {
                // depends_on means dep → stage. Stage's in-degree
                // increments by the count of its deps.
                *in_degree.entry(stage.id.clone()).or_insert(0) += 1;
                // Touch dep so we don't accidentally drop it.
                let _ = dep;
            }
        }
        // Seed queue with zero-in-degree nodes (BTree iteration =
        // deterministic order).
        let mut queue: VecDeque<StageId> = in_degree
            .iter()
            .filter(|(_, d)| **d == 0)
            .map(|(k, _)| k.clone())
            .collect();
        let mut sorted = Vec::with_capacity(self.stages.len());
        while let Some(next) = queue.pop_front() {
            sorted.push(next.clone());
            // Decrement in-degree of stages that depend on `next`.
            for stage in self.stages.values() {
                if stage.depends_on.contains(&next) {
                    if let Some(d) = in_degree.get_mut(&stage.id) {
                        *d = d.saturating_sub(1);
                        if *d == 0 {
                            queue.push_back(stage.id.clone());
                        }
                    }
                }
            }
        }
        if sorted.len() != self.stages.len() {
            let remaining: Vec<StageId> = in_degree
                .into_iter()
                .filter(|(_, d)| *d > 0)
                .map(|(k, _)| k)
                .collect();
            return Err(PlantioError::Cycle { remaining });
        }
        Ok(sorted)
    }

    /// "Compile" the Plantio into a flat list of materialization
    /// jobs ready for a downstream scheduler. Each job is a
    /// (stage_id, target_node) pair; placement fan-out happens
    /// here — `AllNodes` requires the caller to supply the live
    /// cluster size.
    ///
    /// Returns an empty Vec on an empty Plantio. Caller is
    /// responsible for the actual fan-out across `AllNodes` (the
    /// substrate doesn't have a live cluster directory here —
    /// that's `PlantioController`'s job next round).
    ///
    /// # Errors
    /// Propagates [`PlantioError`] from validation.
    pub fn compile_jobs(&self) -> Result<Vec<MaterializationJob>, PlantioError> {
        let sorted = self.topo_sort()?;
        let mut jobs = Vec::new();
        for id in &sorted {
            let stage = &self.stages[id];
            match &stage.placement {
                Placement::Pinned { node } => jobs.push(MaterializationJob {
                    stage_id: id.clone(),
                    target: JobTarget::Node(*node),
                }),
                Placement::AnyOne => jobs.push(MaterializationJob {
                    stage_id: id.clone(),
                    target: JobTarget::AnyOne,
                }),
                Placement::AnyK { k } => jobs.push(MaterializationJob {
                    stage_id: id.clone(),
                    target: JobTarget::AnyK { k: (*k).max(1) },
                }),
                Placement::Quorum { k } => jobs.push(MaterializationJob {
                    stage_id: id.clone(),
                    target: JobTarget::Quorum { k: (*k).max(1) },
                }),
                Placement::AllNodes => jobs.push(MaterializationJob {
                    stage_id: id.clone(),
                    target: JobTarget::AllNodes,
                }),
            }
        }
        Ok(jobs)
    }
}

crate::define_named!(Plantio, "plantio");
crate::impl_observable!(Plantio, crate::mirante::ChildCountSnapshot);

/// One unit of work emitted by `Plantio::compile_jobs`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializationJob {
    /// Stage this job materializes.
    pub stage_id: StageId,
    /// Where the job targets.
    pub target: JobTarget,
}

/// Job-side projection of `Placement`. Distinguishes "I need a
/// specific node" from "the scheduler picks". Single-node concrete;
/// multi-node abstract until the scheduler resolves them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JobTarget {
    /// Concrete node — runs there.
    Node(NodeId),
    /// Scheduler picks any one node.
    AnyOne,
    /// Scheduler picks K nodes.
    AnyK {
        /// Count of nodes.
        k: usize,
    },
    /// Scheduler picks K nodes with agreement.
    Quorum {
        /// Quorum threshold.
        k: usize,
    },
    /// Scheduler fans out to every cluster node.
    AllNodes,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(b: u8) -> NodeId {
        NodeId::new([b; 32])
    }

    fn s(id: &str) -> StageId {
        StageId::new(id)
    }

    fn simple_stage(id: &str, deps: &[&str]) -> Stage {
        let mut st = Stage::pinned(id, WorkloadShape::OciImage, n(1));
        st.depends_on = deps.iter().map(|d| s(d)).collect();
        st
    }

    // ── StageId ────────────────────────────────────────────────

    #[test]
    fn stage_id_display_matches_str() {
        assert_eq!(format!("{}", s("build")), "build");
    }

    #[test]
    fn stage_id_round_trips_via_serde() {
        let id = s("x");
        let bytes = serde_json::to_vec(&id).unwrap();
        let back: StageId = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, id);
    }

    // ── Placement / Confirmacao ────────────────────────────────

    #[test]
    fn placement_min_nodes_matrix() {
        assert_eq!(Placement::Pinned { node: n(1) }.min_nodes(), Some(1));
        assert_eq!(Placement::AnyOne.min_nodes(), Some(1));
        assert_eq!(Placement::AnyK { k: 5 }.min_nodes(), Some(5));
        assert_eq!(Placement::Quorum { k: 3 }.min_nodes(), Some(3));
        assert_eq!(Placement::AllNodes.min_nodes(), None);
    }

    #[test]
    fn placement_requires_agreement_only_for_quorum_and_all() {
        assert!(Placement::Quorum { k: 1 }.requires_agreement());
        assert!(Placement::AllNodes.requires_agreement());
        assert!(!Placement::Pinned { node: n(1) }.requires_agreement());
        assert!(!Placement::AnyOne.requires_agreement());
        assert!(!Placement::AnyK { k: 5 }.requires_agreement());
    }

    #[test]
    fn placement_zero_k_clamps_to_one_via_min_nodes() {
        assert_eq!(Placement::AnyK { k: 0 }.min_nodes(), Some(1));
        assert_eq!(Placement::Quorum { k: 0 }.min_nodes(), Some(1));
    }

    #[test]
    fn confirmacao_default_is_local() {
        assert_eq!(ConfirmacaoPolicy::default(), ConfirmacaoPolicy::Local);
    }

    #[test]
    fn confirmacao_serializes_with_tag() {
        let json = serde_json::to_string(&ConfirmacaoPolicy::Quorum { k: 3 }).unwrap();
        assert!(json.contains("\"kind\":\"quorum\""));
    }

    // ── Stage ──────────────────────────────────────────────────

    #[test]
    fn pinned_constructor_sets_defaults() {
        let st = Stage::pinned("x", WorkloadShape::OciImage, n(1));
        assert_eq!(st.id, s("x"));
        assert_eq!(st.confirm, ConfirmacaoPolicy::Local);
        assert!(st.depends_on.is_empty());
        assert!(st.verify.is_empty());
        assert!(matches!(st.placement, Placement::Pinned { .. }));
    }

    #[test]
    fn stage_round_trips_via_serde() {
        let st = simple_stage("x", &["a", "b"]);
        let bytes = serde_json::to_vec(&st).unwrap();
        let back: Stage = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, st);
    }

    // ── Plantio basic ──────────────────────────────────────────

    #[test]
    fn empty_plantio_is_valid() {
        let p = Plantio::new();
        assert!(p.is_empty());
        assert_eq!(p.len(), 0);
        p.validate().unwrap();
        assert!(p.compile_jobs().unwrap().is_empty());
    }

    #[test]
    fn add_stage_rejects_duplicate_id() {
        let mut p = Plantio::new();
        p.add_stage(simple_stage("x", &[])).unwrap();
        let err = p.add_stage(simple_stage("x", &[])).unwrap_err();
        assert_eq!(err.kind(), "duplicate_id");
    }

    #[test]
    fn validate_rejects_unknown_dependency() {
        let mut p = Plantio::new();
        p.add_stage(simple_stage("x", &["nope"])).unwrap();
        let err = p.validate().unwrap_err();
        assert_eq!(err.kind(), "unknown_dependency");
    }

    // ── Plantio topo_sort ──────────────────────────────────────

    #[test]
    fn topo_sort_orders_a_then_b_when_b_depends_on_a() {
        let mut p = Plantio::new();
        p.add_stage(simple_stage("a", &[])).unwrap();
        p.add_stage(simple_stage("b", &["a"])).unwrap();
        let order = p.topo_sort().unwrap();
        assert_eq!(order, vec![s("a"), s("b")]);
    }

    #[test]
    fn topo_sort_chain_of_three() {
        let mut p = Plantio::new();
        p.add_stage(simple_stage("c", &["b"])).unwrap();
        p.add_stage(simple_stage("b", &["a"])).unwrap();
        p.add_stage(simple_stage("a", &[])).unwrap();
        let order = p.topo_sort().unwrap();
        assert_eq!(order, vec![s("a"), s("b"), s("c")]);
    }

    #[test]
    fn topo_sort_diamond_works() {
        // a → {b, c} → d
        let mut p = Plantio::new();
        p.add_stage(simple_stage("a", &[])).unwrap();
        p.add_stage(simple_stage("b", &["a"])).unwrap();
        p.add_stage(simple_stage("c", &["a"])).unwrap();
        p.add_stage(simple_stage("d", &["b", "c"])).unwrap();
        let order = p.topo_sort().unwrap();
        // a first, d last; b/c in either order.
        assert_eq!(order[0], s("a"));
        assert_eq!(order[3], s("d"));
        let middle: BTreeSet<_> = order[1..3].iter().collect();
        assert!(middle.contains(&s("b")));
        assert!(middle.contains(&s("c")));
    }

    #[test]
    fn topo_sort_detects_cycle() {
        // a → b → a (cycle)
        let mut p = Plantio::new();
        p.add_stage(simple_stage("a", &["b"])).unwrap();
        p.add_stage(simple_stage("b", &["a"])).unwrap();
        let err = p.topo_sort().unwrap_err();
        assert_eq!(err.kind(), "cycle");
        if let PlantioError::Cycle { remaining } = err {
            // Both stages remain unprocessed.
            assert_eq!(remaining.len(), 2);
        }
    }

    // ── Plantio compile_jobs ───────────────────────────────────

    #[test]
    fn compile_jobs_emits_one_job_per_stage_in_topo_order() {
        let mut p = Plantio::new();
        p.add_stage(simple_stage("a", &[])).unwrap();
        p.add_stage(simple_stage("b", &["a"])).unwrap();
        let jobs = p.compile_jobs().unwrap();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].stage_id, s("a"));
        assert_eq!(jobs[1].stage_id, s("b"));
    }

    #[test]
    fn compile_jobs_projects_placement_to_job_target() {
        let mut p = Plantio::new();
        let mut st_pinned = Stage::pinned("p", WorkloadShape::OciImage, n(1));
        st_pinned.placement = Placement::Pinned { node: n(7) };
        p.add_stage(st_pinned).unwrap();
        let mut st_any = Stage::pinned("any", WorkloadShape::OciImage, n(1));
        st_any.placement = Placement::AnyOne;
        p.add_stage(st_any).unwrap();
        let mut st_quorum = Stage::pinned("q", WorkloadShape::OciImage, n(1));
        st_quorum.placement = Placement::Quorum { k: 3 };
        p.add_stage(st_quorum).unwrap();
        let mut st_all = Stage::pinned("all", WorkloadShape::OciImage, n(1));
        st_all.placement = Placement::AllNodes;
        p.add_stage(st_all).unwrap();
        let jobs = p.compile_jobs().unwrap();
        let targets: BTreeMap<StageId, JobTarget> =
            jobs.into_iter().map(|j| (j.stage_id, j.target)).collect();
        assert_eq!(targets[&s("p")], JobTarget::Node(n(7)));
        assert_eq!(targets[&s("any")], JobTarget::AnyOne);
        assert_eq!(targets[&s("q")], JobTarget::Quorum { k: 3 });
        assert_eq!(targets[&s("all")], JobTarget::AllNodes);
    }

    #[test]
    fn compile_jobs_propagates_cycle_error() {
        let mut p = Plantio::new();
        p.add_stage(simple_stage("a", &["b"])).unwrap();
        p.add_stage(simple_stage("b", &["a"])).unwrap();
        let err = p.compile_jobs().unwrap_err();
        assert_eq!(err.kind(), "cycle");
    }

    // ── Plantio serde ──────────────────────────────────────────

    #[test]
    fn plantio_round_trips_via_serde() {
        let mut p = Plantio::new();
        p.add_stage(simple_stage("a", &[])).unwrap();
        p.add_stage(simple_stage("b", &["a"])).unwrap();
        let bytes = serde_json::to_vec(&p).unwrap();
        let back: Plantio = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, p);
    }

    // ── PlantioError ───────────────────────────────────────────

    #[test]
    fn error_kinds_are_stable() {
        let dup = PlantioError::DuplicateId(s("x"));
        assert_eq!(dup.kind(), "duplicate_id");
        let unk = PlantioError::UnknownDependency {
            stage: s("a"),
            dependency: s("b"),
        };
        assert_eq!(unk.kind(), "unknown_dependency");
        let cyc = PlantioError::Cycle { remaining: vec![] };
        assert_eq!(cyc.kind(), "cycle");
    }
}
