//! Backend integration tests — every R5/R6 backend exposes
//! identical operator-facing semantics. Catches any backend that
//! diverges from the trait contract.

use engenho_revoada::backend::StoreBackend;
use engenho_revoada::face::{Face, ResourceFormat, ResourceRef};
use engenho_revoada::face_store::InMemoryStore;
use engenho_revoada::{
    FileSystemBackend, KubeApiServerBackend, KubeApiServerConfig, NomadHttpBackend,
    NomadHttpConfig, RaftBackend, RaftConfig, SupervisedSystemdBackend, SupervisedSystemdConfig,
    SystemdDbusBackend, SystemdDbusConfig,
};

fn yaml(name: &str, ns: &str) -> Vec<u8> {
    format!("apiVersion: v1\nkind: Pod\nmetadata:\n  name: {name}\n  namespace: {ns}\nspec: {{}}\n")
        .into_bytes()
}

/// One shipping backend per face kind + the in-memory default +
/// the filesystem backend = 7 backends total.
fn all_shipping_backends(dir: &std::path::Path) -> Vec<(&'static str, Box<dyn StoreBackend>)> {
    vec![
        ("in-memory", Box::new(InMemoryStore::new("mem"))),
        (
            "filesystem",
            Box::new(FileSystemBackend::open(dir, "fs").unwrap()),
        ),
        (
            "openraft",
            Box::new(RaftBackend::new(RaftConfig {
                node_id: 1,
                peers: vec![],
                log_dir: dir.into(),
            })),
        ),
        (
            "kube-apiserver",
            Box::new(KubeApiServerBackend::new(KubeApiServerConfig {
                endpoint: "https://k:6443".into(),
                kubeconfig: None,
                bearer_token: None,
                api_version: "1.34".into(),
            })),
        ),
        (
            "nomad-http",
            Box::new(NomadHttpBackend::new(NomadHttpConfig {
                address: "http://n:4646".into(),
                token: None,
                region: "global".into(),
            })),
        ),
        (
            "systemd-dbus",
            Box::new(SystemdDbusBackend::new(SystemdDbusConfig {
                user_units: false,
                auto_reload: false,
            })),
        ),
        (
            "supervised-systemd",
            Box::new(SupervisedSystemdBackend::new(SupervisedSystemdConfig {
                hostname: "h".into(),
                runtime: "podman".into(),
            })),
        ),
    ]
}

#[test]
fn every_backend_round_trips_yaml_apply_get() {
    let dir = tempfile::tempdir().unwrap();
    for (label, backend) in all_shipping_backends(dir.path()) {
        let body = yaml("nginx", "default");
        backend
            .apply(ResourceFormat::Yaml, &body)
            .unwrap_or_else(|e| panic!("{label}: apply: {e}"));
        let r = ResourceRef::namespaced("Pod", "nginx", "default");
        let got = backend
            .get(&r, ResourceFormat::Yaml)
            .unwrap_or_else(|e| panic!("{label}: get: {e}"));
        assert_eq!(got, body, "{label}: YAML round-trip diverged");
    }
}

#[test]
fn every_backend_lists_correctly_after_three_applies() {
    let dir = tempfile::tempdir().unwrap();
    for (label, backend) in all_shipping_backends(dir.path()) {
        backend
            .apply(ResourceFormat::Yaml, &yaml("a", "default"))
            .unwrap();
        backend
            .apply(ResourceFormat::Yaml, &yaml("b", "default"))
            .unwrap();
        backend
            .apply(ResourceFormat::Yaml, &yaml("c", "other"))
            .unwrap();
        let listed = backend
            .list("Pod", Some("default"), ResourceFormat::Yaml)
            .unwrap();
        assert_eq!(listed.len(), 2, "{label}: list filtered count");
    }
}

#[test]
fn every_backend_delete_then_get_errors() {
    let dir = tempfile::tempdir().unwrap();
    for (label, backend) in all_shipping_backends(dir.path()) {
        backend
            .apply(ResourceFormat::Yaml, &yaml("nginx", "default"))
            .unwrap();
        let r = ResourceRef::namespaced("Pod", "nginx", "default");
        backend.delete(&r).unwrap();
        assert!(
            backend.get(&r, ResourceFormat::Yaml).is_err(),
            "{label}: get after delete should error",
        );
    }
}

#[test]
fn every_backend_watch_streams_apply_event() {
    let dir = tempfile::tempdir().unwrap();
    for (label, backend) in all_shipping_backends(dir.path()) {
        let mut watch = backend
            .watch("Pod", Some("default"), ResourceFormat::Yaml)
            .unwrap_or_else(|e| panic!("{label}: watch open: {e}"));
        backend
            .apply(ResourceFormat::Yaml, &yaml("nginx", "default"))
            .unwrap();
        let _ = watch
            .next_event()
            .unwrap_or_else(|e| panic!("{label}: watch read: {e}"));
    }
}

#[test]
fn every_backend_snapshot_restore_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    for (label, backend) in all_shipping_backends(dir.path()) {
        backend
            .apply(ResourceFormat::Yaml, &yaml("a", "default"))
            .unwrap();
        backend
            .apply(ResourceFormat::Yaml, &yaml("b", "default"))
            .unwrap();
        let snap = backend
            .snapshot()
            .unwrap_or_else(|e| panic!("{label}: snapshot: {e}"));

        // Restore into self (idempotent).
        backend
            .restore(&snap)
            .unwrap_or_else(|e| panic!("{label}: restore: {e}"));
        assert_eq!(backend.resource_count(), 2, "{label}: count after restore");
    }
}

#[test]
fn snapshot_from_in_memory_restores_into_every_other_backend() {
    // Cross-backend snapshot interop — the killer feature of the
    // common CBOR codec.
    let mem = InMemoryStore::new("source");
    mem.apply(ResourceFormat::Yaml, &yaml("a", "default"))
        .unwrap();
    mem.apply(ResourceFormat::Yaml, &yaml("b", "default"))
        .unwrap();
    let snap = mem.snapshot().unwrap();

    let dir = tempfile::tempdir().unwrap();
    // Restore into each non-in-memory backend.
    for (label, backend) in all_shipping_backends(dir.path()).into_iter().skip(1) {
        backend.restore(&snap).unwrap();
        assert_eq!(
            backend.resource_count(),
            2,
            "{label}: restore from in-memory snapshot lost data",
        );
    }
}

#[test]
fn every_backend_handles_all_three_formats() {
    let dir = tempfile::tempdir().unwrap();
    let formats = [
        ResourceFormat::Yaml,
        ResourceFormat::Json,
        ResourceFormat::Native,
    ];
    for (label, backend) in all_shipping_backends(dir.path()) {
        for format in formats {
            let body: Vec<u8> = match format {
                ResourceFormat::Yaml => yaml("p", "default"),
                ResourceFormat::Json => serde_json::to_vec(&serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": { "name": "p", "namespace": "default" },
                    "spec": {}
                }))
                .unwrap(),
                ResourceFormat::Native => {
                    let r = ResourceRef::namespaced("Pod", "p", "default");
                    engenho_revoada::encode_envelope(&r, b"payload").unwrap()
                }
                ResourceFormat::Hcl => continue,
            };
            backend
                .apply(format, &body)
                .unwrap_or_else(|e| panic!("{label}+{format:?}: apply: {e}"));
            let r = ResourceRef::namespaced("Pod", "p", "default");
            let got = backend
                .get(&r, format)
                .unwrap_or_else(|e| panic!("{label}+{format:?}: get: {e}"));
            assert_eq!(got, body, "{label}+{format:?}: round-trip");
            backend.delete(&r).unwrap();
        }
    }
}

#[test]
fn every_backend_named_identity_matches_expected() {
    let dir = tempfile::tempdir().unwrap();
    let expected = [
        "in-memory",
        "filesystem",
        "openraft",
        "kube-apiserver",
        "nomad-http",
        "systemd-dbus",
        "supervised-systemd",
    ];
    let backends = all_shipping_backends(dir.path());
    assert_eq!(backends.len(), expected.len());
    for ((_label, backend), expected_name) in backends.iter().zip(expected.iter()) {
        assert_eq!(backend.name(), *expected_name);
    }
}
