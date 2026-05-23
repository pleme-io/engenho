//! Property: Placement → JobTarget projection preserves identity
//! across every Placement variant.

use engenho_substrate::{JobTarget, NodeId, Placement, Plantio, Stage, WorkloadShape};
use proptest::prelude::*;
// placement.rs uses unwrap_or(128) (not 256) for its default cases so
// it doesn't migrate to the standard proptest_with_env! macro.

fn placement_strategy() -> impl Strategy<Value = Placement> {
    prop_oneof![
        any::<[u8; 32]>().prop_map(|b| Placement::Pinned {
            node: NodeId::new(b),
        }),
        Just(Placement::AnyOne),
        (1usize..16).prop_map(|k| Placement::AnyK { k }),
        (1usize..16).prop_map(|k| Placement::Quorum { k }),
        Just(Placement::AllNodes),
    ]
}

fn job_target_for(placement: &Placement) -> JobTarget {
    match placement {
        Placement::Pinned { node } => JobTarget::Node(*node),
        Placement::AnyOne => JobTarget::AnyOne,
        Placement::AnyK { k } => JobTarget::AnyK { k: *k },
        Placement::Quorum { k } => JobTarget::Quorum { k: *k },
        Placement::AllNodes => JobTarget::AllNodes,
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(128),
        ..ProptestConfig::default()
    })]

    /// compile_jobs projects each Placement to its matching JobTarget.
    #[test]
    fn compile_jobs_projects_placement_to_job_target(
        placement in placement_strategy(),
    ) {
        let mut p = Plantio::new();
        let mut stage = Stage::pinned(
            "x",
            WorkloadShape::OciImage,
            NodeId::new([0u8; 32]),
        );
        stage.placement = placement.clone();
        p.add_stage(stage).unwrap();
        let jobs = p.compile_jobs().unwrap();
        prop_assert_eq!(jobs.len(), 1);
        prop_assert_eq!(jobs[0].target.clone(), job_target_for(&placement));
    }

    /// placement.min_nodes() is consistent with the JobTarget shape.
    #[test]
    fn min_nodes_consistent_with_target(placement in placement_strategy()) {
        let target = job_target_for(&placement);
        let min_nodes = placement.min_nodes();
        // AllNodes is dynamic → None.
        // Everything else is Some(k≥1).
        match (target, min_nodes) {
            (JobTarget::AllNodes, None) => {}
            (_, Some(n)) => prop_assert!(n >= 1),
            other => panic!("unexpected (target, min_nodes) combo: {other:?}"),
        }
    }

    /// requires_agreement matches the typed enum match.
    #[test]
    fn requires_agreement_only_for_quorum_and_all(placement in placement_strategy()) {
        let expected = matches!(placement, Placement::Quorum { .. } | Placement::AllNodes);
        prop_assert_eq!(placement.requires_agreement(), expected);
    }

    /// Placement serde round-trips.
    #[test]
    fn placement_serde_round_trip(placement in placement_strategy()) {
        let bytes = serde_json::to_vec(&placement).unwrap();
        let back: Placement = serde_json::from_slice(&bytes).unwrap();
        prop_assert_eq!(back, placement);
    }
}
