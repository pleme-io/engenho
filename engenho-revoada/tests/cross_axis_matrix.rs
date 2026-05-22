//! Cross-axis matrix tests — every supported (face × format ×
//! topology) combination is constructible + applies + gets
//! cleanly. Catches any axis-coupling regression.

use engenho_revoada::face::{Face, ResourceFormat, ResourceRef};
use engenho_revoada::topology::{
    Cluster3MNW, MeshAllPeers, Pair, Phalanx, Quorum3M, Solo, TopologyStrategy,
};
use engenho_revoada::{
    Cluster, ClusterDeclaration, FabricFace, FabricStrategy, FaceKind,
};

fn yaml_pod(name: &str, ns: &str) -> Vec<u8> {
    format!(
        "apiVersion: v1\nkind: Pod\nmetadata:\n  name: {name}\n  namespace: {ns}\nspec: {{}}\n"
    )
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

fn native_pod(name: &str, ns: &str) -> Vec<u8> {
    let r = ResourceRef::namespaced("Pod", name, ns);
    engenho_revoada::encode_envelope(&r, b"payload").unwrap()
}

/// All 5 face declarations.
fn all_face_decls() -> Vec<(&'static str, FabricFace)> {
    vec![
        (
            "pure-raft",
            FabricFace {
                name: "raft".into(),
                kind: FaceKind::PureRaft,
            },
        ),
        ("kubernetes", FabricFace::prescribed_kubernetes_v1_34()),
        (
            "nomad",
            FabricFace {
                name: "nomad".into(),
                kind: FaceKind::Nomad {
                    version: "1.7".into(),
                },
            },
        ),
        (
            "systemd",
            FabricFace {
                name: "systemd".into(),
                kind: FaceKind::Systemd { user_units: false },
            },
        ),
        (
            "bms",
            FabricFace {
                name: "bms".into(),
                kind: FaceKind::BareMetalSupervisor,
            },
        ),
    ]
}

/// Topologies that satisfy the prescribed-strategy 3-quorum
/// requirement (min_nodes >= 3). Phalanx has min_nodes=1 (scales
/// per-cluster-size) so it's excluded from this set.
fn quorum3_compatible_topologies() -> Vec<(&'static str, Box<dyn TopologyStrategy>)> {
    vec![
        ("quorum-3m", Box::new(Quorum3M)),
        ("cluster-3m-nw", Box::new(Cluster3MNW)),
    ]
}

fn ok_strategy() -> FabricStrategy {
    FabricStrategy::prescribed_homelab()
}

// ── Face × Format ────────────────────────────────────────────────

#[test]
fn matrix_face_x_format_yaml_apply_get_round_trips() {
    for (face_label, decl) in all_face_decls() {
        let cluster = Cluster::builder()
            .strategy(ok_strategy())
            .face(decl)
            .topology(Quorum3M)
            .start()
            .unwrap();
        let yaml = yaml_pod("nginx", "default");
        cluster
            .apply(ResourceFormat::Yaml, &yaml)
            .unwrap_or_else(|e| panic!("{face_label}/Yaml apply: {e}"));
        let r = ResourceRef::namespaced("Pod", "nginx", "default");
        let got = cluster
            .get(&r, ResourceFormat::Yaml)
            .unwrap_or_else(|e| panic!("{face_label}/Yaml get: {e}"));
        assert_eq!(got, yaml, "{face_label}/Yaml round-trip");
    }
}

#[test]
fn matrix_face_x_format_json_apply_get_round_trips() {
    for (face_label, decl) in all_face_decls() {
        let cluster = Cluster::builder()
            .strategy(ok_strategy())
            .face(decl)
            .topology(Quorum3M)
            .start()
            .unwrap();
        let json = json_pod("nginx", "default");
        cluster
            .apply(ResourceFormat::Json, &json)
            .unwrap_or_else(|e| panic!("{face_label}/Json apply: {e}"));
        let r = ResourceRef::namespaced("Pod", "nginx", "default");
        let got = cluster
            .get(&r, ResourceFormat::Json)
            .unwrap_or_else(|e| panic!("{face_label}/Json get: {e}"));
        assert_eq!(got, json, "{face_label}/Json round-trip");
    }
}

#[test]
fn matrix_face_x_format_native_apply_get_round_trips() {
    for (face_label, decl) in all_face_decls() {
        let cluster = Cluster::builder()
            .strategy(ok_strategy())
            .face(decl)
            .topology(Quorum3M)
            .start()
            .unwrap();
        let native = native_pod("nginx", "default");
        cluster
            .apply(ResourceFormat::Native, &native)
            .unwrap_or_else(|e| panic!("{face_label}/Native apply: {e}"));
        let r = ResourceRef::namespaced("Pod", "nginx", "default");
        let got = cluster
            .get(&r, ResourceFormat::Native)
            .unwrap_or_else(|e| panic!("{face_label}/Native get: {e}"));
        assert_eq!(got, native, "{face_label}/Native round-trip");
    }
}

// ── Face × Topology coherence ────────────────────────────────────

#[test]
fn matrix_face_x_topology_quorum_compatible_constructs() {
    // Every (face, topology) combination with topology.min_nodes
    // >= 3 must construct + start cleanly.
    for (face_label, decl) in all_face_decls() {
        for (topo_label, topology) in quorum3_compatible_topologies() {
            let result = Cluster::builder()
                .strategy(ok_strategy())
                .face(decl.clone())
                .topology_boxed(topology)
                .start();
            assert!(
                result.is_ok(),
                "{face_label}+{topo_label} should build: {:?}",
                result.err(),
            );
        }
    }
}

#[test]
fn matrix_solo_pair_topologies_fail_against_3_quorum() {
    // Solo + Pair both have min_nodes < 3. The 3-quorum strategy
    // must reject them regardless of face.
    for (face_label, decl) in all_face_decls() {
        for (topo_label, topology) in [
            ("solo", Box::new(Solo) as Box<dyn TopologyStrategy>),
            ("pair", Box::new(Pair)),
        ] {
            let result = ClusterDeclaration::new(ok_strategy(), decl.clone(), topology);
            assert!(
                result.is_err(),
                "{face_label}+{topo_label} should fail coherence",
            );
        }
    }
}

// ── Face × Format × Topology — the full 3-axis matrix ───────────

#[test]
fn matrix_full_3_axis_quorum_compatible_works() {
    let formats = [
        ResourceFormat::Yaml,
        ResourceFormat::Json,
        ResourceFormat::Native,
    ];
    for (face_label, decl) in all_face_decls() {
        for (topo_label, topology) in quorum3_compatible_topologies() {
            let cluster = Cluster::builder()
                .strategy(ok_strategy())
                .face(decl.clone())
                .topology_boxed(topology)
                .start()
                .unwrap_or_else(|e| panic!("{face_label}+{topo_label}: build: {e}"));

            for format in formats {
                let body = match format {
                    ResourceFormat::Yaml => yaml_pod("p", "default"),
                    ResourceFormat::Json => json_pod("p", "default"),
                    ResourceFormat::Native => native_pod("p", "default"),
                    ResourceFormat::Hcl => continue, // K8s vocabulary doesn't have HCL
                };
                cluster
                    .apply(format, &body)
                    .unwrap_or_else(|e| panic!(
                        "{face_label}+{topo_label}+{format:?}: apply: {e}"
                    ));
                let r = ResourceRef::namespaced("Pod", "p", "default");
                let got = cluster
                    .get(&r, format)
                    .unwrap_or_else(|e| panic!(
                        "{face_label}+{topo_label}+{format:?}: get: {e}"
                    ));
                assert_eq!(
                    got,
                    body,
                    "{face_label}+{topo_label}+{format:?}: round-trip",
                );
                cluster.delete(&r).unwrap();
            }
        }
    }
}

// ── Phalanx + MeshAllPeers structural checks ────────────────────

#[test]
fn mesh_all_peers_min_nodes_is_low_so_quorum3_check_fires() {
    // MeshAllPeers.min_nodes = 1, so against a 3-quorum strategy
    // it MUST fail the coherence check.
    let result = ClusterDeclaration::new(
        ok_strategy(),
        FabricFace {
            name: "test".into(),
            kind: FaceKind::PureRaft,
        },
        Box::new(MeshAllPeers),
    );
    assert!(result.is_err(), "MeshAllPeers + 3-quorum should fail");
}

// ── Helper: ClusterBuilder needs a boxed-topology hook ──────────

trait BuilderBoxedTopo {
    fn topology_boxed(self, topology: Box<dyn TopologyStrategy>) -> Self;
}

impl BuilderBoxedTopo for engenho_revoada::ClusterBuilder {
    fn topology_boxed(self, topology: Box<dyn TopologyStrategy>) -> Self {
        // Box::new is auto-coerced; pass directly via the generic
        // topology() since the bound is T: TopologyStrategy + 'static
        // and Box<dyn T> does NOT satisfy that.
        //
        // Workaround: a tiny wrapper that owns the box + impls the
        // trait by forwarding.
        struct Forward(Box<dyn TopologyStrategy>);
        impl std::fmt::Debug for Forward {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "Forward(<dyn>)")
            }
        }
        impl TopologyStrategy for Forward {
            fn name(&self) -> &'static str {
                "boxed-forward"
            }
            fn min_nodes(&self) -> usize {
                self.0.min_nodes()
            }
            fn assign(
                &self,
                eligible: &[engenho_revoada::topology::NodeId],
            ) -> Result<
                engenho_revoada::topology::RoleAssignment,
                engenho_revoada::topology::TopologyError,
            > {
                self.0.assign(eligible)
            }
            fn react_to_loss(
                &self,
                current: &engenho_revoada::topology::RoleAssignment,
                lost: &[engenho_revoada::topology::NodeId],
            ) -> Vec<engenho_revoada::topology::Transition> {
                self.0.react_to_loss(current, lost)
            }
            fn validate(
                &self,
                assignment: &engenho_revoada::topology::RoleAssignment,
            ) -> Result<(), engenho_revoada::topology::TopologyError> {
                self.0.validate(assignment)
            }
        }
        self.topology(Forward(topology))
    }
}
