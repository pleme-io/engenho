//! Property: Plantio topology invariants hold for every
//! well-formed input.

use engenho_substrate::{NodeId, Placement, Plantio, Stage, StageId, WorkloadShape};
use proptest::prelude::*;
// plantio_topo.rs uses unwrap_or(128) — keeps its own ProptestConfig.
use std::collections::{BTreeMap, BTreeSet};

/// Generate a Plantio with N stages where stage `i` may depend on
/// any subset of stages `0..i` — this guarantees DAG (no cycles).
fn acyclic_plantio_strategy(max_n: usize) -> impl Strategy<Value = Plantio> {
    (1usize..max_n)
        .prop_flat_map(|n| {
            // For each stage i, pick a subset of {0..i} as deps.
            let dep_maps = (0..n)
                .map(|i| {
                    proptest::collection::vec(0..(i.max(1)), 0..i.max(1).min(4)).prop_map(
                        move |deps| {
                            let mut set: BTreeSet<usize> = deps.into_iter().collect();
                            set.remove(&i);
                            set
                        },
                    )
                })
                .collect::<Vec<_>>();
            (Just(n), dep_maps)
        })
        .prop_map(|(_n, dep_sets)| {
            let mut plantio = Plantio::new();
            let node = NodeId::new([0u8; 32]);
            for (i, deps) in dep_sets.into_iter().enumerate() {
                let mut stage =
                    Stage::pinned(format!("stage-{i:03}"), WorkloadShape::OciImage, node);
                stage.depends_on = deps
                    .into_iter()
                    .map(|d| StageId::new(format!("stage-{d:03}")))
                    .collect();
                stage.placement = Placement::Pinned { node };
                plantio.add_stage(stage).unwrap();
            }
            plantio
        })
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(128),
        ..ProptestConfig::default()
    })]

    /// Every acyclic Plantio passes validate().
    #[test]
    fn acyclic_plantio_validates(plantio in acyclic_plantio_strategy(16)) {
        prop_assert!(plantio.validate().is_ok());
    }

    /// topo_sort produces stages in dependency order — every
    /// stage appears AFTER everything it depends_on.
    #[test]
    fn topo_sort_respects_dependency_order(plantio in acyclic_plantio_strategy(16)) {
        let order = plantio.topo_sort().unwrap();
        let position: BTreeMap<StageId, usize> = order
            .iter()
            .enumerate()
            .map(|(i, id)| (id.clone(), i))
            .collect();
        for (id, stage) in &plantio.stages {
            let my_pos = position[id];
            for dep in &stage.depends_on {
                let dep_pos = position[dep];
                prop_assert!(
                    dep_pos < my_pos,
                    "stage {} (pos {}) depends on {} (pos {})",
                    id,
                    my_pos,
                    dep,
                    dep_pos
                );
            }
        }
    }

    /// topo_sort produces every stage exactly once.
    #[test]
    fn topo_sort_returns_all_stages(plantio in acyclic_plantio_strategy(16)) {
        let order = plantio.topo_sort().unwrap();
        prop_assert_eq!(order.len(), plantio.len());
        let unique: BTreeSet<StageId> = order.into_iter().collect();
        prop_assert_eq!(unique.len(), plantio.len());
        for id in plantio.stages.keys() {
            prop_assert!(unique.contains(id));
        }
    }

    /// Cycle injection produces PlantioError::Cycle.
    #[test]
    fn cycle_injection_detected(
        plantio in acyclic_plantio_strategy(8),
        a_idx in 0usize..8,
        b_idx in 0usize..8,
    ) {
        prop_assume!(plantio.len() >= 2);
        let stage_ids: Vec<StageId> = plantio.stages.keys().cloned().collect();
        let a = stage_ids[a_idx % stage_ids.len()].clone();
        let b = stage_ids[b_idx % stage_ids.len()].clone();
        prop_assume!(a != b);
        let mut p2 = plantio.clone();
        // Force A → B and B → A (cycle).
        p2.stages.get_mut(&a).unwrap().depends_on.insert(b.clone());
        p2.stages.get_mut(&b).unwrap().depends_on.insert(a.clone());
        // validate() must catch the cycle.
        let result = p2.validate();
        prop_assert!(result.is_err());
        if let Err(e) = result {
            prop_assert_eq!(e.kind(), "cycle");
        }
    }

    /// compile_jobs emits exactly one job per stage.
    #[test]
    fn compile_jobs_one_per_stage(plantio in acyclic_plantio_strategy(16)) {
        let jobs = plantio.compile_jobs().unwrap();
        prop_assert_eq!(jobs.len(), plantio.len());
    }

    /// Plantio serde round-trips.
    #[test]
    fn plantio_serde_round_trip(plantio in acyclic_plantio_strategy(8)) {
        let bytes = serde_json::to_vec(&plantio).unwrap();
        let back: Plantio = serde_json::from_slice(&bytes).unwrap();
        prop_assert_eq!(back, plantio);
    }
}
