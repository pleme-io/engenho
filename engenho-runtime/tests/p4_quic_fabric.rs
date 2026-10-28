#![cfg(feature = "mesh-quic")]

mod support;

use std::path::{Path, PathBuf};

use engenho_config::{EngenhoConfig, Fabric, KubeletBackendKind, SharedCluster};
use engenho_control_types::pin::KeyMaterial;
use engenho_runtime::{Runtime, RuntimeError, Unhonoured};
use engenho_store::{MeshAddr, Reason, ResourceCommand, ResourceKey};
use shikumi::TieredConfig;

fn shared_ca(dir: &Path) -> (PathBuf, PathBuf) {
    engenho_apiserver::load_or_generate_ca(dir).unwrap();
    let pki = engenho_apiserver::PkiFile::dir(dir);
    (pki.join("ca.crt"), pki.join("ca.key"))
}

fn node(data_dir: &Path, ca: &(PathBuf, PathBuf)) -> EngenhoConfig {
    let mut cfg = EngenhoConfig::prescribed_default();
    cfg.cluster.shared = Some(SharedCluster {
        name: "aldeia-quic".into(),
        ca_cert_path: ca.0.clone(),
        ca_key_path: ca.1.clone(),
        identity_key_path: data_dir.join("mesh").join("node.key"),
        seeds: Vec::new(),
    });
    cfg.fabric = Fabric::Quic {
        bind: "127.0.0.1:0".parse().unwrap(),
    };
    cfg.runtime.listen_addr = "127.0.0.1:0".into();
    cfg.runtime.kubelet_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.etcd_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.kubeconfig_publish_path = String::new();
    support::place(&mut cfg, data_dir);
    cfg.runtime.durable = true;
    cfg.runtime.node_name = "quic-node".into();
    cfg.runtime.kubelet_backend = KubeletBackendKind::Fake;
    cfg.runtime.leadership_timeout_seconds = 10;
    cfg
}

fn config_map(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "ConfigMap", "default", name)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shared_member_on_the_quic_fabric_forms_over_its_pinned_mesh_address_and_resumes() {
    let (ca_dir, dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let ca = shared_ca(ca_dir.path());
    let runtime = Runtime::start(node(dir.path(), &ca)).await.unwrap();
    let store = runtime.store();
    let addr: MeshAddr = store.listen_addr().parse().unwrap();
    let key = KeyMaterial::from_pem(
        &std::fs::read_to_string(dir.path().join("mesh").join("node.key")).unwrap(),
    )
    .unwrap();
    assert_eq!(addr.pin, Some(key.spki()));
    assert_eq!(addr.socket.ip().to_string(), "127.0.0.1");
    assert_ne!(
        addr.socket.port(),
        0,
        "the advertised port is the bound one"
    );
    assert_eq!(store.node_id(), key.spki().mesh_node_id());
    store
        .propose(ResourceCommand::put(
            config_map("over-quic"),
            serde_json::json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "over-quic", "namespace": "default"}}),
            Reason::Operator,
        ))
        .await
        .unwrap();
    drop(store);
    runtime.shutdown().await.unwrap();

    let again = Runtime::start(node(dir.path(), &ca)).await.unwrap();
    assert!(again.store().get(&config_map("over-quic")).await.is_some());
    assert_eq!(again.store().node_id(), key.spki().mesh_node_id());
    again.shutdown().await.unwrap();
}

#[tokio::test]
async fn the_quic_fabric_without_a_shared_identity_is_refused_before_anything_opens() {
    let dir = tempfile::tempdir().unwrap();
    let ca_dir = tempfile::tempdir().unwrap();
    let mut cfg = node(dir.path(), &shared_ca(ca_dir.path()));
    cfg.cluster.shared = None;
    match Runtime::start(cfg).await {
        Err(RuntimeError::Unhonoured(Unhonoured::MeshWithoutIdentity)) => {}
        Err(other) => panic!("refused for another reason: {other}"),
        Ok(_) => panic!("a quic fabric booted with no identity"),
    }
    assert!(!dir.path().join("store").exists());
}
