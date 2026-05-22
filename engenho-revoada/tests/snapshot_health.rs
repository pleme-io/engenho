//! Integration tests for Snapshot/Restore + ClusterHealth — the
//! observability + state-capture primitives shipped on top of the
//! 5-verb face contract.

use engenho_revoada::face::{Face, ResourceFormat, ResourceRef};
use engenho_revoada::topology::Quorum3M;
use engenho_revoada::{
    BareMetalSupervisorFace, Cluster, FabricFace, FabricStrategy, FaceKind, KubernetesFace,
    NomadFace, PureRaftFace, SystemdFace,
};

fn yaml_manifest(name: &str, ns: &str) -> Vec<u8> {
    format!(
        "apiVersion: v1\nkind: Pod\nmetadata:\n  name: {name}\n  namespace: {ns}\nspec: {{}}\n"
    )
    .into_bytes()
}

fn raft_face() -> PureRaftFace {
    PureRaftFace::from_declaration(&FabricFace {
        name: "test-raft".into(),
        kind: FaceKind::PureRaft,
    })
    .unwrap()
}

// ── Snapshot / Restore on PureRaftFace ────────────────────────────

#[test]
fn snapshot_then_restore_preserves_every_resource() {
    let face = raft_face();
    face.apply_resource(ResourceFormat::Yaml, &yaml_manifest("a", "default"))
        .unwrap();
    face.apply_resource(ResourceFormat::Yaml, &yaml_manifest("b", "default"))
        .unwrap();
    face.apply_resource(ResourceFormat::Yaml, &yaml_manifest("c", "other"))
        .unwrap();
    let snap = face.snapshot().unwrap();

    let restored = raft_face();
    restored.restore(&snap).unwrap();
    assert_eq!(restored.resource_count(), 3);

    // Each resource is byte-identical via get.
    let r_a = ResourceRef::namespaced("Pod", "a", "default");
    let r_b = ResourceRef::namespaced("Pod", "b", "default");
    let r_c = ResourceRef::namespaced("Pod", "c", "other");
    assert_eq!(
        restored.get_resource(&r_a, ResourceFormat::Yaml).unwrap(),
        yaml_manifest("a", "default"),
    );
    assert_eq!(
        restored.get_resource(&r_b, ResourceFormat::Yaml).unwrap(),
        yaml_manifest("b", "default"),
    );
    assert_eq!(
        restored.get_resource(&r_c, ResourceFormat::Yaml).unwrap(),
        yaml_manifest("c", "other"),
    );
}

#[test]
fn snapshot_is_deterministic_byte_identical() {
    // Two equivalent stores produce byte-identical snapshots —
    // foundation for content-addressed backup naming.
    let face_a = raft_face();
    let face_b = raft_face();
    for (n, ns) in &[("a", "default"), ("b", "default"), ("c", "other")] {
        face_a
            .apply_resource(ResourceFormat::Yaml, &yaml_manifest(n, ns))
            .unwrap();
        face_b
            .apply_resource(ResourceFormat::Yaml, &yaml_manifest(n, ns))
            .unwrap();
    }
    assert_eq!(face_a.snapshot().unwrap(), face_b.snapshot().unwrap());
}

#[test]
fn snapshot_then_restore_to_self_preserves_state() {
    let face = raft_face();
    face.apply_resource(ResourceFormat::Yaml, &yaml_manifest("a", "default"))
        .unwrap();
    let snap = face.snapshot().unwrap();
    let count_before = face.resource_count();
    face.restore(&snap).unwrap();
    assert_eq!(face.resource_count(), count_before);
}

#[test]
fn restore_replaces_all_existing_state() {
    let face = raft_face();
    face.apply_resource(ResourceFormat::Yaml, &yaml_manifest("a", "default"))
        .unwrap();
    let snap = face.snapshot().unwrap();
    face.apply_resource(ResourceFormat::Yaml, &yaml_manifest("z", "other"))
        .unwrap();
    assert_eq!(face.resource_count(), 2);
    face.restore(&snap).unwrap();
    assert_eq!(face.resource_count(), 1);
    let r_z = ResourceRef::namespaced("Pod", "z", "other");
    assert!(face.get_resource(&r_z, ResourceFormat::Yaml).is_err());
}

#[test]
fn empty_snapshot_restores_to_empty_store() {
    let face = raft_face();
    let empty_snap = face.snapshot().unwrap();
    face.apply_resource(ResourceFormat::Yaml, &yaml_manifest("a", "default"))
        .unwrap();
    assert_eq!(face.resource_count(), 1);
    face.restore(&empty_snap).unwrap();
    assert_eq!(face.resource_count(), 0);
}

#[test]
fn malformed_snapshot_returns_err() {
    let face = raft_face();
    let err = face.restore(b"not cbor at all").unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("restore") || msg.contains("cbor"), "msg: {msg}");
}

#[test]
fn snapshot_works_uniformly_across_all_five_faces() {
    let yaml = yaml_manifest("nginx", "default");
    let cases: Vec<Box<dyn Face>> = vec![
        Box::new(
            PureRaftFace::from_declaration(&FabricFace {
                name: "p".into(),
                kind: FaceKind::PureRaft,
            })
            .unwrap(),
        ),
        Box::new(
            KubernetesFace::from_declaration(&FabricFace::prescribed_kubernetes_v1_34())
                .unwrap(),
        ),
        Box::new(
            NomadFace::from_declaration(&FabricFace {
                name: "n".into(),
                kind: FaceKind::Nomad {
                    version: "1.7".into(),
                },
            })
            .unwrap(),
        ),
        Box::new(
            SystemdFace::from_declaration(&FabricFace {
                name: "s".into(),
                kind: FaceKind::Systemd { user_units: false },
            })
            .unwrap(),
        ),
        Box::new(
            BareMetalSupervisorFace::from_declaration(&FabricFace {
                name: "b".into(),
                kind: FaceKind::BareMetalSupervisor,
            })
            .unwrap(),
        ),
    ];
    for face in cases {
        face.apply_resource(ResourceFormat::Yaml, &yaml).unwrap();
        assert_eq!(face.resource_count(), 1, "{}: expected 1", face.name());
        let snap = face
            .snapshot()
            .unwrap_or_else(|e| panic!("{}: snapshot failed: {e}", face.name()));
        // Restore into the same face — should be a no-op state-wise.
        face.restore(&snap)
            .unwrap_or_else(|e| panic!("{}: restore failed: {e}", face.name()));
        assert_eq!(face.resource_count(), 1, "{}: after restore", face.name());
    }
}

// ── ClusterHealth ────────────────────────────────────────────────

fn ok_cluster() -> Cluster {
    Cluster::builder()
        .strategy(FabricStrategy::prescribed_homelab())
        .face_pure_raft("test-cluster")
        .topology(Quorum3M)
        .start()
        .unwrap()
}

#[test]
fn health_reflects_running_state_and_face_kind() {
    let cluster = ok_cluster();
    let h = cluster.health();
    assert_eq!(h.name, "test-cluster");
    assert_eq!(h.kind, FaceKind::PureRaft);
    assert!(h.cluster_running);
    assert!(h.face_running);
    assert_eq!(h.resource_count, 0);
    assert_eq!(h.subscriber_count, 0);
    assert_eq!(h.strategy_name, "homelab-3node");
    // Topology name varies per impl but should be non-empty.
    assert!(!h.topology_name.is_empty());
}

#[test]
fn health_resource_count_grows_with_applies() {
    let cluster = ok_cluster();
    assert_eq!(cluster.health().resource_count, 0);
    cluster
        .apply(ResourceFormat::Yaml, &yaml_manifest("a", "default"))
        .unwrap();
    assert_eq!(cluster.health().resource_count, 1);
    cluster
        .apply(ResourceFormat::Yaml, &yaml_manifest("b", "default"))
        .unwrap();
    assert_eq!(cluster.health().resource_count, 2);
}

#[test]
fn health_subscriber_count_grows_with_watches() {
    let cluster = ok_cluster();
    assert_eq!(cluster.health().subscriber_count, 0);
    let _w1 = cluster
        .watch("Pod", None, ResourceFormat::Yaml)
        .unwrap();
    assert_eq!(cluster.health().subscriber_count, 1);
    let _w2 = cluster
        .watch("Pod", None, ResourceFormat::Yaml)
        .unwrap();
    assert_eq!(cluster.health().subscriber_count, 2);
}

#[test]
fn health_check_passes_on_running_cluster() {
    let cluster = ok_cluster();
    cluster.health().check().expect("running cluster healthy");
}

#[test]
fn health_check_fails_after_shutdown() {
    let cluster = ok_cluster();
    cluster.shutdown().unwrap();
    let h = cluster.health();
    let err = h.check().unwrap_err();
    assert!(err.contains("not running"), "err: {err}");
}

#[test]
fn health_serializes_to_json_for_telemetry() {
    let cluster = ok_cluster();
    cluster
        .apply(ResourceFormat::Yaml, &yaml_manifest("nginx", "default"))
        .unwrap();
    let h = cluster.health();
    let json = serde_json::to_string(&h).unwrap();
    assert!(json.contains("\"name\":\"test-cluster\""));
    assert!(json.contains("\"resource_count\":1"));
    assert!(json.contains("\"face_running\":true"));
}

// ── Cluster snapshot + restore wire-through ──────────────────────

#[test]
fn cluster_snapshot_then_restore_round_trips() {
    let cluster = ok_cluster();
    cluster
        .apply(ResourceFormat::Yaml, &yaml_manifest("a", "default"))
        .unwrap();
    cluster
        .apply(ResourceFormat::Yaml, &yaml_manifest("b", "default"))
        .unwrap();
    let snap = cluster.snapshot().unwrap();

    // Build a fresh cluster + restore into it.
    let restored = ok_cluster();
    restored.restore(&snap).unwrap();
    assert_eq!(restored.health().resource_count, 2);
}

#[test]
fn cluster_snapshot_is_deterministic_across_equivalent_clusters() {
    let cluster_a = ok_cluster();
    let cluster_b = ok_cluster();
    for (n, ns) in &[("a", "default"), ("b", "default")] {
        cluster_a
            .apply(ResourceFormat::Yaml, &yaml_manifest(n, ns))
            .unwrap();
        cluster_b
            .apply(ResourceFormat::Yaml, &yaml_manifest(n, ns))
            .unwrap();
    }
    assert_eq!(cluster_a.snapshot().unwrap(), cluster_b.snapshot().unwrap());
}
