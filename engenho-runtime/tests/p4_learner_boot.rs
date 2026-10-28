#![cfg(feature = "mesh-quic")]

mod support;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use engenho_config::{
    EngenhoConfig, Fabric, KubeletBackendKind, SharedCluster, TopologyStrategyKind,
};
use engenho_runtime::Runtime;
use engenho_store::{MeshAddr, Reason, ResourceCommand, ResourceKey};
use shikumi::TieredConfig;

fn ca(dir: &Path) -> (PathBuf, PathBuf) {
    engenho_apiserver::load_or_generate_ca(dir).unwrap();
    let pki = engenho_apiserver::PkiFile::dir(dir);
    (pki.join("ca.crt"), pki.join("ca.key"))
}

fn node(
    data_dir: &Path,
    ca: &(PathBuf, PathBuf),
    name: &str,
    seeds: Vec<SocketAddr>,
) -> EngenhoConfig {
    let mut cfg = EngenhoConfig::prescribed_default();
    cfg.cluster.shared = Some(SharedCluster {
        name: "aldeia-learner".into(),
        ca_cert_path: ca.0.clone(),
        ca_key_path: ca.1.clone(),
        identity_key_path: data_dir.join("mesh").join("node.key"),
        seeds,
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
    cfg.runtime.node_name = name.into();
    cfg.runtime.kubelet_backend = KubeletBackendKind::Fake;
    cfg.runtime.leadership_timeout_seconds = 10;
    cfg.runtime.tls.enabled = false;
    cfg
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_with_seeds_boots_as_a_learner_serves_what_the_leader_wrote_and_its_own_writes_reach_the_leader()
 {
    let (ca_dir, one, two) = (
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    );
    let ca = ca(ca_dir.path());
    let first = Runtime::start(node(one.path(), &ca, "node-one", Vec::new()))
        .await
        .unwrap();
    let seed: MeshAddr = first.store().listen_addr().parse().unwrap();

    let mut joining = node(two.path(), &ca, "node-two", vec![seed.socket]);
    joining.revoada.topology.strategy = TopologyStrategyKind::Quorum3M;
    joining.revoada.topology.min_nodes = 3;
    let second = Runtime::start(joining).await.unwrap();
    let learner = second.store();
    assert!(!learner.is_leader().await);
    assert!(first.store().members().await.contains(&(
        learner.node_id(),
        learner.listen_addr().to_owned(),
        false
    )));

    let key = ResourceKey::namespaced("", "v1", "ConfigMap", "default", "from-the-leader");
    first
        .store()
        .propose(ResourceCommand::put(
            key.clone(),
            serde_json::json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "from-the-leader", "namespace": "default"}, "data": {"k": "v"}}),
            Reason::Operator,
        ))
        .await
        .unwrap();
    let at = first.store().current_revision().await;
    assert!(learner.wait_for_revision(at, Duration::from_secs(10)).await);

    let token =
        std::fs::read_to_string(engenho_apiserver::PkiFile::AdminToken.path(two.path())).unwrap();
    let served: serde_json::Value = reqwest::Client::new()
        .get(format!(
            "http://{}/api/v1/namespaces/default/configmaps/from-the-leader",
            second.local_addr()
        ))
        .bearer_auth(token.trim())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(served["data"]["k"], "v", "{served}");
    let registered = first
        .store()
        .get(&ResourceKey::cluster_scoped("", "v1", "Node", "node-two"))
        .await
        .expect("the learner's Node registration did not reach the leader");
    assert!(
        registered.pointer("/status/capacity/cpu").is_some()
            && registered.pointer("/status/nodeInfo").is_some(),
        "the learner's boot did not register its host-owned Node fields: {registered}"
    );
    let lease = ResourceKey::namespaced(
        "coordination.k8s.io",
        "v1",
        "Lease",
        "kube-node-lease",
        "node-two",
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while first.store().get(&lease).await.is_none() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the learner's node lease never reached the leader"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let created = reqwest::Client::new()
        .post(format!(
            "http://{}/api/v1/namespaces/default/configmaps",
            second.local_addr()
        ))
        .bearer_auth(token.trim())
        .json(&serde_json::json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "from-the-learner", "namespace": "default"}, "data": {"k": "learner"}}))
        .send()
        .await
        .unwrap();
    assert!(
        created.status().is_success(),
        "{}",
        created.text().await.unwrap()
    );
    let theirs = ResourceKey::namespaced("", "v1", "ConfigMap", "default", "from-the-learner");
    assert_eq!(
        first.store().get(&theirs).await.unwrap()["data"]["k"],
        "learner",
        "a write to the learner's apiserver is not the leader's"
    );

    drop(learner);
    second.shutdown().await.unwrap();
    first.shutdown().await.unwrap();
}
