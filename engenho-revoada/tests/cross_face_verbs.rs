//! Cross-face verb-consistency tests.
//!
//! The 5-verb contract on the Face trait promises identical
//! operator-facing behavior across every concrete impl (PureRaft /
//! Kubernetes / Nomad / Systemd / BareMetalSupervisor). This test
//! suite generates K8s-shaped YAML manifests + asserts every face
//! round-trips them identically. If any face's verb dispatch
//! diverges from the contract, the proptest cases catch it.
//!
//! Lives in `tests/` (integration tests) so it runs against the
//! public API exactly the way an operator would.

use engenho_revoada::face::{Face, ResourceFormat, ResourceRef};
use engenho_revoada::{
    BareMetalSupervisorFace, FabricFace, FaceKind, KubernetesFace, NomadFace, PureRaftFace,
    SystemdFace,
};
use engenho_substrate_props::proptest_with_env;

use proptest::prelude::*;

/// Build one face instance for each [`FaceKind`] variant.
fn all_faces() -> Vec<(&'static str, Box<dyn Face>)> {
    vec![
        (
            "pure-raft",
            Box::new(
                PureRaftFace::from_declaration(&FabricFace {
                    name: "pure-raft".into(),
                    kind: FaceKind::PureRaft,
                })
                .unwrap(),
            ),
        ),
        (
            "kubernetes",
            Box::new(
                KubernetesFace::from_declaration(&FabricFace::prescribed_kubernetes_v1_34())
                    .unwrap(),
            ),
        ),
        (
            "nomad",
            Box::new(
                NomadFace::from_declaration(&FabricFace {
                    name: "nomad".into(),
                    kind: FaceKind::Nomad {
                        version: "1.7".into(),
                    },
                })
                .unwrap(),
            ),
        ),
        (
            "systemd",
            Box::new(
                SystemdFace::from_declaration(&FabricFace {
                    name: "systemd".into(),
                    kind: FaceKind::Systemd { user_units: false },
                })
                .unwrap(),
            ),
        ),
        (
            "bms",
            Box::new(
                BareMetalSupervisorFace::from_declaration(&FabricFace {
                    name: "bms".into(),
                    kind: FaceKind::BareMetalSupervisor,
                })
                .unwrap(),
            ),
        ),
    ]
}

fn k8s_yaml(kind: &str, name: &str, namespace: &str) -> Vec<u8> {
    format!(
        "apiVersion: v1\nkind: {kind}\nmetadata:\n  name: {name}\n  namespace: {namespace}\nspec: {{}}\n"
    )
    .into_bytes()
}

// ── Static cross-face proofs ───────────────────────────────────────

#[test]
fn every_face_round_trips_a_pod_yaml_manifest() {
    let yaml = k8s_yaml("Pod", "nginx", "default");
    let r = ResourceRef::namespaced("Pod", "nginx", "default");
    for (label, face) in all_faces() {
        face.apply_resource(ResourceFormat::Yaml, &yaml)
            .unwrap_or_else(|e| panic!("{label}: apply failed: {e}"));
        let back = face
            .get_resource(&r, ResourceFormat::Yaml)
            .unwrap_or_else(|e| panic!("{label}: get failed: {e}"));
        assert_eq!(back, yaml, "{label}: YAML round-trip diverged");
    }
}

#[test]
fn every_face_round_trips_a_pod_json_manifest() {
    let json = serde_json::to_vec(&serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "nginx", "namespace": "default" },
        "spec": {}
    }))
    .unwrap();
    let r = ResourceRef::namespaced("Pod", "nginx", "default");
    for (label, face) in all_faces() {
        face.apply_resource(ResourceFormat::Json, &json)
            .unwrap_or_else(|e| panic!("{label}: apply failed: {e}"));
        let back = face
            .get_resource(&r, ResourceFormat::Json)
            .unwrap_or_else(|e| panic!("{label}: get failed: {e}"));
        assert_eq!(back, json, "{label}: JSON round-trip diverged");
    }
}

#[test]
fn every_face_list_matches_apply_count() {
    let yaml_a = k8s_yaml("Pod", "a", "default");
    let yaml_b = k8s_yaml("Pod", "b", "default");
    let yaml_c = k8s_yaml("Pod", "c", "other");
    for (label, face) in all_faces() {
        face.apply_resource(ResourceFormat::Yaml, &yaml_a).unwrap();
        face.apply_resource(ResourceFormat::Yaml, &yaml_b).unwrap();
        face.apply_resource(ResourceFormat::Yaml, &yaml_c).unwrap();
        let listed = face
            .list_resources("Pod", Some("default"), ResourceFormat::Yaml)
            .unwrap_or_else(|e| panic!("{label}: list failed: {e}"));
        assert_eq!(
            listed.len(),
            2,
            "{label}: expected 2 Pods in default ns, got {}",
            listed.len(),
        );
    }
}

#[test]
fn every_face_delete_then_get_errors_uniformly() {
    let yaml = k8s_yaml("Pod", "nginx", "default");
    let r = ResourceRef::namespaced("Pod", "nginx", "default");
    for (label, face) in all_faces() {
        face.apply_resource(ResourceFormat::Yaml, &yaml).unwrap();
        face.delete_resource(&r).unwrap();
        let got = face.get_resource(&r, ResourceFormat::Yaml);
        assert!(
            got.is_err(),
            "{label}: get after delete should error, got {got:?}"
        );
    }
}

// ── Property-based cross-face round-trip ──────────────────────────

proptest_with_env! {
    /// For any K8s-shape (kind, name, namespace) triple within a
    /// reasonable character set, every face apply/get round-trips
    /// the YAML manifest byte-identically.
    ///
    /// Strings are constrained to lowercase + digits + hyphen
    /// (K8s DNS-1123 label shape) so the YAML emitter doesn't
    /// quote them and the round-trip stays comparable.
    #[test]
    fn proptest_every_face_round_trips_any_pod_yaml(
        name in "[a-z][a-z0-9-]{0,15}",
        namespace in "[a-z][a-z0-9-]{0,15}",
    ) {
        let yaml = k8s_yaml("Pod", &name, &namespace);
        let r = ResourceRef::namespaced("Pod", name.clone(), namespace.clone());
        for (label, face) in all_faces() {
            prop_assert!(
                face.apply_resource(ResourceFormat::Yaml, &yaml).is_ok(),
                "{}: apply failed for name={:?} ns={:?}", label, name, namespace,
            );
            let back = face
                .get_resource(&r, ResourceFormat::Yaml)
                .unwrap_or_else(|e| panic!("{}: get failed: {}", label, e));
            prop_assert_eq!(
                back,
                yaml.clone(),
                "{}: YAML diverged for {} / {}",
                label,
                name,
                namespace,
            );
        }
    }

    /// Apply N times to the same ref, last value wins. Every face
    /// honors the last-writer-wins contract.
    #[test]
    fn proptest_every_face_apply_is_last_writer_wins(
        n in 1usize..8,
    ) {
        let r = ResourceRef::namespaced("Pod", "nginx", "default");
        for (label, face) in all_faces() {
            // Apply n different versions of the same Pod (differ
            // in spec body content via a comment).
            let mut last = Vec::new();
            for i in 0..n {
                let yaml = format!(
                    "apiVersion: v1\nkind: Pod\nmetadata:\n  name: nginx\n  namespace: default\nspec:\n  containers:\n    - name: c\n      image: nginx:v{i}\n"
                )
                .into_bytes();
                face.apply_resource(ResourceFormat::Yaml, &yaml).unwrap();
                last = yaml;
            }
            let back = face.get_resource(&r, ResourceFormat::Yaml).unwrap();
            prop_assert_eq!(back, last, "{}: last-writer-wins violated", label);
        }
    }
}
