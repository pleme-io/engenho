//! Property-based invariant tests. Each proptest hammers a typed
//! boundary with hundreds of random inputs to catch corners no
//! static test covers. Failures here mean a typed invariant was
//! reachable in an unintended way.

use std::sync::Arc;

use engenho_revoada::face::{Face, ResourceFormat, ResourceRef};
use engenho_revoada::topology::{Cluster3MNW, Phalanx, Quorum3M, Solo};
use engenho_revoada::{
    Cluster, ClusterDeclaration, FabricFace, FabricStrategy, FaceKind, FederatedFabric,
    FormatAdapter, K8sJsonAdapter, K8sYamlAdapter, ReconciliationCadence, RoutingPolicy,
    encode_envelope,
};

use proptest::prelude::*;

// ── Helpers ──────────────────────────────────────────────────────

fn dns_label() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9-]{0,15}"
}

fn yaml_pod(name: &str, ns: &str) -> Vec<u8> {
    format!("apiVersion: v1\nkind: Pod\nmetadata:\n  name: {name}\n  namespace: {ns}\nspec: {{}}\n")
        .into_bytes()
}

fn json_pod(name: &str, ns: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": name, "namespace": ns },
        "spec": {}
    }))
    .unwrap()
}

fn raft_cluster(name: &str) -> Cluster {
    Cluster::builder()
        .strategy(FabricStrategy::prescribed_homelab())
        .face_pure_raft(name)
        .topology(Quorum3M)
        .start()
        .unwrap()
}

// ── FabricStrategy invariants ────────────────────────────────────

proptest! {
    /// ReconciliationCadence::new rejects zero EXACTLY, accepts
    /// every other u32.
    #[test]
    fn cadence_accepts_nonzero_rejects_zero(ms in 0u32..1_000_000) {
        let r = ReconciliationCadence::new(ms);
        if ms == 0 {
            prop_assert!(r.is_err());
        } else {
            prop_assert!(r.is_ok());
            prop_assert_eq!(r.unwrap().millis(), ms);
        }
    }

    /// Strategy liveness: quorum 1 or 2 always fail; quorum >= 3
    /// passes IFF quorum is odd.
    #[test]
    fn strategy_liveness_quorum_must_be_odd_and_at_least_three(
        quorum in 1u32..20,
    ) {
        let mut s = FabricStrategy::prescribed_homelab();
        s.consensus.kind = engenho_revoada::ConsensusKind::OpenRaft {
            quorum_size: quorum,
            election_timeout_ms: 150,
            snapshot_interval_entries: 1000,
        };
        let result = s.prove_liveness();
        if quorum < 3 {
            prop_assert!(result.is_err(),
                "quorum {} should fail (too small)", quorum);
        } else if quorum % 2 == 0 {
            prop_assert!(result.is_err(),
                "quorum {} should fail (even)", quorum);
        } else {
            prop_assert!(result.is_ok(),
                "quorum {} should pass (odd, >= 3)", quorum);
        }
    }
}

// ── ResourceRef + ResourceFormat round-trips ────────────────────

proptest! {
    /// Any namespaced ResourceRef can be encoded into a CBOR
    /// envelope + decoded back without loss.
    #[test]
    fn resource_ref_envelope_round_trip(
        kind in dns_label(),
        name in dns_label(),
        namespace in dns_label(),
        payload in proptest::collection::vec(any::<u8>(), 0..256),
    ) {
        let r = ResourceRef::namespaced(&kind, &name, &namespace);
        let env = encode_envelope(&r, &payload).unwrap();

        // Round-trip through the InMemoryStore via PureRaftFace's
        // apply + get path.
        let face = engenho_revoada::PureRaftFace::from_declaration(&FabricFace {
            name: "rt".into(),
            kind: FaceKind::PureRaft,
        })
        .unwrap();
        face.apply_resource(ResourceFormat::Native, &env).unwrap();
        let got = face.get_resource(&r, ResourceFormat::Native).unwrap();
        prop_assert_eq!(got, env);
    }
}

// ── Format adapters extract consistent refs ─────────────────────

proptest! {
    /// YAML and JSON adapters extract byte-identical ResourceRef
    /// for equivalent manifests.
    #[test]
    fn yaml_json_adapters_agree_on_extracted_ref(
        name in dns_label(),
        ns in dns_label(),
    ) {
        let yaml_body = yaml_pod(&name, &ns);
        let json_body = json_pod(&name, &ns);
        let yaml_ref = K8sYamlAdapter
            .extract_ref(ResourceFormat::Yaml, &yaml_body)
            .unwrap();
        let json_ref = K8sJsonAdapter
            .extract_ref(ResourceFormat::Json, &json_body)
            .unwrap();
        prop_assert_eq!(yaml_ref, json_ref);
    }
}

// ── Snapshot determinism + round-trip ────────────────────────────

proptest! {
    /// Apply N pods (1..32), snapshot, restore into a fresh
    /// cluster, verify identical resource count.
    #[test]
    fn snapshot_restore_preserves_count(
        names in proptest::collection::vec(dns_label(), 1..32),
    ) {
        let cluster = raft_cluster("snap-test");
        let unique_names: std::collections::HashSet<_> =
            names.iter().cloned().collect();
        for name in &unique_names {
            cluster.apply(ResourceFormat::Yaml, &yaml_pod(name, "default")).unwrap();
        }
        let expected_count = unique_names.len();
        let snap = cluster.snapshot().unwrap();
        let fresh = raft_cluster("snap-fresh");
        fresh.restore(&snap).unwrap();
        prop_assert_eq!(fresh.health().resource_count, expected_count);
    }

    /// Two clusters with equivalent state produce byte-identical
    /// snapshots — content-addressed backup foundation.
    #[test]
    fn snapshot_determinism_byte_identical_for_equivalent_state(
        names in proptest::collection::vec(dns_label(), 1..16),
    ) {
        let c1 = raft_cluster("c1");
        let c2 = raft_cluster("c2");
        let unique: std::collections::HashSet<_> = names.iter().cloned().collect();
        // Apply in different orders — snapshot must still match.
        let unique_vec: Vec<_> = unique.iter().collect();
        for n in &unique_vec {
            c1.apply(ResourceFormat::Yaml, &yaml_pod(n, "default")).unwrap();
        }
        for n in unique_vec.iter().rev() {
            c2.apply(ResourceFormat::Yaml, &yaml_pod(n, "default")).unwrap();
        }
        prop_assert_eq!(c1.snapshot().unwrap(), c2.snapshot().unwrap());
    }
}

// ── Last-writer-wins ─────────────────────────────────────────────

proptest! {
    /// N applies to the same ref → last value wins.
    #[test]
    fn last_writer_wins_for_repeated_applies(
        n in 1usize..8,
    ) {
        let cluster = raft_cluster("lww");
        let mut last = Vec::new();
        for i in 0..n {
            let body = format!(
                "apiVersion: v1\nkind: Pod\nmetadata:\n  name: x\n  namespace: default\nspec:\n  containers:\n    - image: v{i}\n"
            )
            .into_bytes();
            cluster.apply(ResourceFormat::Yaml, &body).unwrap();
            last = body;
        }
        let r = ResourceRef::namespaced("Pod", "x", "default");
        let got = cluster.get(&r, ResourceFormat::Yaml).unwrap();
        prop_assert_eq!(got, last);
        prop_assert_eq!(cluster.health().resource_count, 1);
    }
}

// ── Federation routing ──────────────────────────────────────────

proptest! {
    /// NamespacePrefix policy: every apply lands on the routed
    /// member; non-routed members stay empty.
    #[test]
    fn federation_namespace_routing_is_deterministic(
        names in proptest::collection::vec(dns_label(), 1..8),
    ) {
        let mut map = std::collections::HashMap::new();
        map.insert("alpha".to_string(), 0);
        map.insert("beta".to_string(), 1);
        let policy = RoutingPolicy::NamespacePrefix {
            map,
            default_member: None,
        };
        let federation = FederatedFabric::new(
            vec![
                Arc::new(raft_cluster("alpha-cluster")),
                Arc::new(raft_cluster("beta-cluster")),
            ],
            policy,
        )
        .unwrap();

        let unique: std::collections::HashSet<_> = names.iter().cloned().collect();
        for n in &unique {
            let r_alpha = ResourceRef::namespaced("Pod", n, "alpha");
            let r_beta = ResourceRef::namespaced("Pod", n, "beta");
            federation
                .apply(&r_alpha, ResourceFormat::Yaml, &yaml_pod(n, "alpha"))
                .unwrap();
            federation
                .apply(&r_beta, ResourceFormat::Yaml, &yaml_pod(n, "beta"))
                .unwrap();
        }
        prop_assert_eq!(federation.members()[0].health().resource_count, unique.len());
        prop_assert_eq!(federation.members()[1].health().resource_count, unique.len());
    }
}

// ── Topology min_nodes invariant ────────────────────────────────

proptest! {
    /// For every pre-packed topology, ClusterDeclaration::new
    /// rejects a 3-quorum strategy iff topology.min_nodes() < 3.
    #[test]
    fn topology_quorum_coherence_holds_uniformly(
        which in 0u8..4,
    ) {
        let topology: Box<dyn engenho_revoada::topology::TopologyStrategy> = match which {
            0 => Box::new(Solo),
            1 => Box::new(Quorum3M),
            2 => Box::new(Cluster3MNW),
            _ => Box::new(Phalanx),
        };
        let min = topology.min_nodes();
        let result = ClusterDeclaration::new(
            FabricStrategy::prescribed_homelab(), // quorum=3
            FabricFace {
                name: "t".into(),
                kind: FaceKind::PureRaft,
            },
            topology,
        );
        if min >= 3 {
            prop_assert!(result.is_ok(), "topology min={} should pass", min);
        } else {
            prop_assert!(result.is_err(), "topology min={} should fail", min);
        }
    }
}
