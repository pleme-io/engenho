mod support;

use std::path::{Path, PathBuf};

use engenho_config::{EngenhoConfig, KubeletBackendKind, SharedCluster};
use engenho_control_types::pin::KeyMaterial;
use engenho_runtime::{Runtime, RuntimeError};
use shikumi::TieredConfig;

struct SharedCa {
    _dir: tempfile::TempDir,
    cert: PathBuf,
    key: PathBuf,
}

fn shared_ca() -> SharedCa {
    let dir = tempfile::tempdir().unwrap();
    engenho_apiserver::load_or_generate_ca(dir.path()).unwrap();
    let pki = engenho_apiserver::PkiFile::dir(dir.path());
    SharedCa {
        cert: pki.join("ca.crt"),
        key: pki.join("ca.key"),
        _dir: dir,
    }
}

fn node(data_dir: &Path, shared: Option<&SharedCa>) -> EngenhoConfig {
    let mut cfg = EngenhoConfig::prescribed_default();
    cfg.cluster.name = "engenho-host-name".into();
    cfg.cluster.shared = shared.map(|ca| SharedCluster {
        name: "aldeia-p4".into(),
        ca_cert_path: ca.cert.clone(),
        ca_key_path: ca.key.clone(),
        identity_key_path: data_dir.join("mesh").join("node.key"),
    });
    cfg.runtime.listen_addr = "127.0.0.1:0".into();
    cfg.runtime.kubelet_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.etcd_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.kubeconfig_publish_path = String::new();
    support::place(&mut cfg, data_dir);
    cfg.runtime.durable = true;
    cfg.runtime.node_name = "p4-node".into();
    cfg.runtime.kubelet_backend = KubeletBackendKind::Fake;
    cfg.runtime.leadership_timeout_seconds = 10;
    cfg
}

fn kubeconfig(data_dir: &Path) -> serde_yaml::Value {
    serde_yaml::from_str(&std::fs::read_to_string(data_dir.join("kubeconfig")).unwrap()).unwrap()
}

fn ca_data(data_dir: &Path) -> String {
    kubeconfig(data_dir)["clusters"][0]["cluster"]["certificate-authority-data"]
        .as_str()
        .expect("the kubeconfig embeds a CA")
        .to_owned()
}

fn cluster_of(data_dir: &Path) -> String {
    kubeconfig(data_dir)["clusters"][0]["name"]
        .as_str()
        .expect("the kubeconfig names its cluster")
        .to_owned()
}

fn identity_id(data_dir: &Path) -> u64 {
    let pem = std::fs::read_to_string(data_dir.join("mesh").join("node.key")).unwrap();
    KeyMaterial::from_pem(&pem).unwrap().spki().mesh_node_id()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_the_block_a_node_is_raft_node_1_under_its_own_ca_and_name() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = Runtime::start(node(dir.path(), None)).await.unwrap();
    assert_eq!(runtime.store().node_id(), 1);
    assert!(!dir.path().join("mesh").exists());
    assert!(
        engenho_apiserver::PkiFile::dir(dir.path())
            .join("ca.key")
            .exists()
    );
    assert_eq!(cluster_of(dir.path()), "engenho-host-name");
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn members_of_a_shared_cluster_share_its_ca_and_name_and_hold_distinct_stable_ids() {
    let ca = shared_ca();
    let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let first = Runtime::start(node(a.path(), Some(&ca))).await.unwrap();
    let second = Runtime::start(node(b.path(), Some(&ca))).await.unwrap();

    let (id_a, id_b) = (first.store().node_id(), second.store().node_id());
    assert_eq!(id_a, identity_id(a.path()));
    assert_eq!(id_b, identity_id(b.path()));
    assert_ne!(id_a, 1);
    assert_ne!(id_a, id_b);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(a.path().join("mesh").join("node.key"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    assert_eq!(ca_data(a.path()), ca_data(b.path()));
    assert_eq!(cluster_of(a.path()), "aldeia-p4");
    assert!(
        !engenho_apiserver::PkiFile::dir(a.path())
            .join("ca.key")
            .exists()
    );
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();

    let again = Runtime::start(node(a.path(), Some(&ca))).await.unwrap();
    assert_eq!(again.store().node_id(), id_a, "the id survives a restart");
    again.shutdown().await.unwrap();

    let own = tempfile::tempdir().unwrap();
    let alone = Runtime::start(node(own.path(), None)).await.unwrap();
    assert_ne!(ca_data(own.path()), ca_data(a.path()));
    alone.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_store_formed_as_its_own_cluster_is_refused_under_a_shared_identity() {
    let ca = shared_ca();
    let dir = tempfile::tempdir().unwrap();
    Runtime::start(node(dir.path(), None))
        .await
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    match Runtime::start(node(dir.path(), Some(&ca))).await {
        Err(RuntimeError::NotAMember { node_id }) => assert_eq!(node_id, identity_id(dir.path())),
        Err(other) => panic!("refused for another reason: {other}"),
        Ok(_) => panic!("a store formed as node 1 booted under a shared identity"),
    }
}
