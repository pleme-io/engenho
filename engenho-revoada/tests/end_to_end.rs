//! End-to-end operator-flow integration tests. Each exercises 3+
//! substrate layers together; failures here mean cross-layer
//! contracts diverged.

use std::sync::Arc;

use engenho_revoada::face::{Face, ResourceFormat, ResourceRef};
use engenho_revoada::topology::{Cluster3MNW, Quorum3M};
use engenho_revoada::{
    Cluster, FabricFace, FabricStrategy, FaceKind, FederatedFabric, RoutingPolicy,
};

fn yaml(name: &str, ns: &str) -> Vec<u8> {
    format!("apiVersion: v1\nkind: Pod\nmetadata:\n  name: {name}\n  namespace: {ns}\nspec: {{}}\n")
        .into_bytes()
}

fn build_cluster(face_name: &str, kind: FaceKind) -> Cluster {
    Cluster::builder()
        .strategy(FabricStrategy::prescribed_homelab())
        .face(FabricFace {
            name: face_name.into(),
            kind,
        })
        .topology(Quorum3M)
        .start()
        .unwrap()
}

// ── Full operator flow on a single cluster ────────────────────────

#[test]
fn e2e_single_cluster_full_crudw_flow() {
    let cluster = build_cluster("e2e-single", FaceKind::PureRaft);

    // Apply 3 pods, 1 in another namespace.
    cluster
        .apply(ResourceFormat::Yaml, &yaml("a", "default"))
        .unwrap();
    cluster
        .apply(ResourceFormat::Yaml, &yaml("b", "default"))
        .unwrap();
    cluster
        .apply(ResourceFormat::Yaml, &yaml("c", "other"))
        .unwrap();

    // Health reflects accurate state.
    let h = cluster.health();
    assert_eq!(h.resource_count, 3);
    assert!(h.cluster_running);

    // List filtered by namespace.
    let in_default = cluster
        .list("Pod", Some("default"), ResourceFormat::Yaml)
        .unwrap();
    assert_eq!(in_default.len(), 2);
    let all = cluster.list("Pod", None, ResourceFormat::Yaml).unwrap();
    assert_eq!(all.len(), 3);

    // Watch streams the existing state as Added.
    let mut w = cluster.watch("Pod", None, ResourceFormat::Yaml).unwrap();
    for _ in 0..3 {
        let _ = w.next_event().unwrap().expect("Added event");
    }

    // Delete + verify get errors.
    let r = ResourceRef::namespaced("Pod", "a", "default");
    cluster.delete(&r).unwrap();
    assert!(cluster.get(&r, ResourceFormat::Yaml).is_err());
    assert_eq!(cluster.health().resource_count, 2);

    // Snapshot → restore into fresh cluster → identical state.
    let snap = cluster.snapshot().unwrap();
    let fresh = build_cluster("e2e-fresh", FaceKind::PureRaft);
    fresh.restore(&snap).unwrap();
    assert_eq!(fresh.health().resource_count, 2);
}

#[test]
fn e2e_snapshot_round_trip_across_face_kinds() {
    // Apply on one face kind; snapshot; restore into a face of a
    // different kind. The store contract is face-kind-agnostic
    // (under the InMemoryStore backend) so this should work.
    let raft = build_cluster("raft", FaceKind::PureRaft);
    raft.apply(ResourceFormat::Yaml, &yaml("a", "default"))
        .unwrap();
    raft.apply(ResourceFormat::Yaml, &yaml("b", "default"))
        .unwrap();

    let snap = raft.snapshot().unwrap();

    let k8s = Cluster::builder()
        .strategy(FabricStrategy::prescribed_homelab())
        .face_kubernetes_prescribed()
        .topology(Quorum3M)
        .start()
        .unwrap();
    k8s.restore(&snap).unwrap();
    assert_eq!(k8s.health().resource_count, 2);

    // The kind on the receiving cluster's health reflects the
    // RECEIVING face, not the source — snapshot is data, not
    // identity.
    let h = k8s.health();
    assert!(matches!(h.kind, FaceKind::Kubernetes { .. }));
}

// ── Federation flows ─────────────────────────────────────────────

#[test]
fn e2e_federation_namespace_routing_with_multi_apply() {
    let mut map = std::collections::HashMap::new();
    map.insert("team-a".to_string(), 0);
    map.insert("team-b".to_string(), 1);
    map.insert("team-c".to_string(), 2);
    let policy = RoutingPolicy::NamespacePrefix {
        map,
        default_member: Some(0),
    };

    let federation = FederatedFabric::new(
        vec![
            Arc::new(build_cluster("us-east", FaceKind::PureRaft)),
            Arc::new(build_cluster("us-west", FaceKind::PureRaft)),
            Arc::new(build_cluster("eu-central", FaceKind::PureRaft)),
        ],
        policy,
    )
    .unwrap();

    // Apply 1 pod per team — each routes to its own cluster.
    for (ns, name) in &[("team-a", "alpha"), ("team-b", "beta"), ("team-c", "gamma")] {
        let r = ResourceRef::namespaced("Pod", *name, *ns);
        federation
            .apply(&r, ResourceFormat::Yaml, &yaml(name, ns))
            .unwrap();
    }

    // Each member has exactly its own routed pod.
    assert_eq!(federation.members()[0].health().resource_count, 1);
    assert_eq!(federation.members()[1].health().resource_count, 1);
    assert_eq!(federation.members()[2].health().resource_count, 1);
}

#[test]
fn e2e_federation_list_aggregates_across_members() {
    let federation = FederatedFabric::new(
        vec![
            Arc::new(build_cluster("a", FaceKind::PureRaft)),
            Arc::new(build_cluster("b", FaceKind::PureRaft)),
            Arc::new(build_cluster("c", FaceKind::PureRaft)),
        ],
        RoutingPolicy::First,
    )
    .unwrap();

    // Write directly to each member (bypass routing for setup).
    for (idx, name) in ["a-pod", "b-pod", "c-pod"].iter().enumerate() {
        federation.members()[idx]
            .apply(ResourceFormat::Yaml, &yaml(name, "default"))
            .unwrap();
    }

    let all = federation
        .list("Pod", Some("default"), ResourceFormat::Yaml)
        .unwrap();
    assert_eq!(all.len(), 3);
}

#[test]
fn e2e_federation_watch_fans_events_across_members() {
    let federation = FederatedFabric::new(
        vec![
            Arc::new(build_cluster("alpha", FaceKind::PureRaft)),
            Arc::new(build_cluster("beta", FaceKind::PureRaft)),
        ],
        RoutingPolicy::First,
    )
    .unwrap();

    let mut watch = federation
        .watch("Pod", Some("default"), ResourceFormat::Yaml)
        .unwrap();

    federation.members()[0]
        .apply(ResourceFormat::Yaml, &yaml("from-alpha", "default"))
        .unwrap();
    federation.members()[1]
        .apply(ResourceFormat::Yaml, &yaml("from-beta", "default"))
        .unwrap();

    let mut got = Vec::new();
    for _ in 0..2 {
        let ev = watch.next_event().unwrap().expect("event");
        got.push(ev.body);
    }
    assert_eq!(got.len(), 2);
}

// ── Heterogeneous face federation ────────────────────────────────

#[test]
fn e2e_heterogeneous_face_federation_all_5_kinds() {
    // One cluster per face kind, all federated together. Any
    // operator can call verbs against any member via the unified
    // FederatedFabric handle.
    let federation = FederatedFabric::new(
        vec![
            Arc::new(build_cluster("raft", FaceKind::PureRaft)),
            Arc::new(
                Cluster::builder()
                    .strategy(FabricStrategy::prescribed_homelab())
                    .face_kubernetes_prescribed()
                    .topology(Quorum3M)
                    .start()
                    .unwrap(),
            ),
            Arc::new(build_cluster(
                "nomad",
                FaceKind::Nomad {
                    version: "1.7".into(),
                },
            )),
            Arc::new(build_cluster(
                "systemd",
                FaceKind::Systemd { user_units: false },
            )),
            Arc::new(build_cluster("bms", FaceKind::BareMetalSupervisor)),
        ],
        RoutingPolicy::First,
    )
    .unwrap();

    // Direct apply on each member (bypass routing).
    for (idx, member) in federation.members().iter().enumerate() {
        let pod_name = format!("pod-{idx}");
        member
            .apply(ResourceFormat::Yaml, &yaml(&pod_name, "default"))
            .unwrap();
    }

    // List aggregates across all 5 face kinds uniformly.
    let all = federation
        .list("Pod", Some("default"), ResourceFormat::Yaml)
        .unwrap();
    assert_eq!(all.len(), 5);
}

// ── Format mixing within one cluster ─────────────────────────────

#[test]
fn e2e_mixed_format_apply_each_round_trips_in_its_format() {
    let cluster = build_cluster("mixed", FaceKind::PureRaft);

    // Apply YAML, JSON, and Native envelope at distinct refs.
    let yaml_body = yaml("yaml-pod", "default");
    cluster.apply(ResourceFormat::Yaml, &yaml_body).unwrap();

    let json_body = serde_json::to_vec(&serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "json-pod", "namespace": "default" },
        "spec": {}
    }))
    .unwrap();
    cluster.apply(ResourceFormat::Json, &json_body).unwrap();

    use engenho_revoada::encode_envelope;
    let native_ref = ResourceRef::namespaced("Pod", "native-pod", "default");
    let native = encode_envelope(&native_ref, b"native-payload").unwrap();
    cluster.apply(ResourceFormat::Native, &native).unwrap();

    assert_eq!(cluster.health().resource_count, 3);

    // Each round-trips through its own format.
    let r_yaml = ResourceRef::namespaced("Pod", "yaml-pod", "default");
    let r_json = ResourceRef::namespaced("Pod", "json-pod", "default");
    let r_native = ResourceRef::namespaced("Pod", "native-pod", "default");
    assert_eq!(
        cluster.get(&r_yaml, ResourceFormat::Yaml).unwrap(),
        yaml_body
    );
    assert_eq!(
        cluster.get(&r_json, ResourceFormat::Json).unwrap(),
        json_body
    );
    assert_eq!(
        cluster.get(&r_native, ResourceFormat::Native).unwrap(),
        native
    );
}

// ── RAII Drop under various lifecycle states ─────────────────────

#[test]
fn e2e_drop_after_apply_releases_resources_cleanly() {
    let initial = {
        let cluster = build_cluster("drop-test", FaceKind::PureRaft);
        cluster
            .apply(ResourceFormat::Yaml, &yaml("a", "default"))
            .unwrap();
        cluster.health().resource_count
        // cluster drops here
    };
    assert_eq!(initial, 1);
    // New cluster has empty state — drop doesn't leak between
    // instances (they share no global state).
    let fresh = build_cluster("drop-test-2", FaceKind::PureRaft);
    assert_eq!(fresh.health().resource_count, 0);
}

#[test]
fn e2e_topology_swap_via_new_cluster() {
    // Operators "swap topology" by tearing down + building anew.
    // Verify both topologies survive the same declaration shape.
    let cluster_3m = Cluster::builder()
        .strategy(FabricStrategy::prescribed_homelab())
        .face_pure_raft("c1")
        .topology(Quorum3M)
        .start()
        .unwrap();
    cluster_3m
        .apply(ResourceFormat::Yaml, &yaml("a", "default"))
        .unwrap();
    let snap = cluster_3m.snapshot().unwrap();
    drop(cluster_3m);

    let cluster_3mnw = Cluster::builder()
        .strategy(FabricStrategy::prescribed_homelab())
        .face_pure_raft("c1")
        .topology(Cluster3MNW)
        .start()
        .unwrap();
    cluster_3mnw.restore(&snap).unwrap();
    assert_eq!(cluster_3mnw.health().resource_count, 1);
}
