#![cfg(feature = "mesh-quic")]

mod support;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use engenho_config::{EngenhoConfig, Fabric, KubeletBackendKind, SharedCluster};
use engenho_runtime::Runtime;
use engenho_store::{MeshAddr, Reason, ResourceCommand, ResourceKey, Revision};
use shikumi::TieredConfig;

const BOUND: Duration = Duration::from_secs(15);

struct Node {
    runtime: Runtime,
    dir: tempfile::TempDir,
}

impl Node {
    async fn start(ca: &(PathBuf, PathBuf), name: &str, seeds: &[&Node]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let seeds = seeds.iter().map(|n| n.mesh()).collect();
        let runtime = Runtime::start(config(dir.path(), ca, name, seeds))
            .await
            .unwrap();
        Self { runtime, dir }
    }

    fn mesh(&self) -> SocketAddr {
        self.runtime
            .store()
            .listen_addr()
            .parse::<MeshAddr>()
            .unwrap()
            .socket
    }

    fn id(&self) -> u64 {
        self.runtime.store().node_id()
    }

    async fn voter(&self, on: &Node) -> Option<bool> {
        on.runtime
            .store()
            .members()
            .await
            .into_iter()
            .find(|(id, _, _)| *id == self.id())
            .map(|(_, _, voter)| voter)
    }

    async fn write(&self, name: &str) -> Revision {
        let store = self.runtime.store();
        store
            .propose(ResourceCommand::put(
                config_map(name),
                serde_json::json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": name, "namespace": "default"}, "data": {"written": name}}),
                Reason::Operator,
            ))
            .await
            .unwrap();
        store.current_revision().await
    }

    async fn serves(&self, name: &str, at: Revision) -> bool {
        if !self.runtime.store().wait_for_revision(at, BOUND).await {
            return false;
        }
        let token =
            std::fs::read_to_string(engenho_apiserver::PkiFile::AdminToken.path(self.dir.path()))
                .unwrap();
        let served: serde_json::Value = reqwest::Client::new()
            .get(format!(
                "http://{}/api/v1/namespaces/default/configmaps/{name}",
                self.runtime.local_addr()
            ))
            .bearer_auth(token.trim())
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        served["data"]["written"] == name
    }
}

fn ca(dir: &Path) -> (PathBuf, PathBuf) {
    engenho_apiserver::load_or_generate_ca(dir).unwrap();
    let pki = engenho_apiserver::PkiFile::dir(dir);
    (pki.join("ca.crt"), pki.join("ca.key"))
}

fn config(
    data_dir: &Path,
    ca: &(PathBuf, PathBuf),
    name: &str,
    seeds: Vec<SocketAddr>,
) -> EngenhoConfig {
    let mut cfg = EngenhoConfig::prescribed_default();
    cfg.cluster.shared = Some(SharedCluster {
        name: "aldeia-e2e".into(),
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
    cfg.runtime.leadership_timeout_seconds = 15;
    cfg.runtime.tls.enabled = false;
    cfg
}

fn config_map(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "ConfigMap", "default", name)
}

async fn leader_among<'a>(nodes: &[&'a Node]) -> Option<&'a Node> {
    let deadline = tokio::time::Instant::now() + BOUND;
    while tokio::time::Instant::now() < deadline {
        for node in nodes {
            if node.runtime.store().is_leader().await {
                return Some(node);
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_daemons_one_cluster_a_learner_serves_the_leaders_write_and_is_promoted() {
    let ca_dir = tempfile::tempdir().unwrap();
    let ca = ca(ca_dir.path());
    let one = Node::start(&ca, "node-one", &[]).await;
    let two = Node::start(&ca, "node-two", &[&one]).await;
    assert_eq!(
        two.voter(&one).await,
        Some(false),
        "node two joined as a learner"
    );

    let at = one.write("one-wrote").await;
    assert!(
        two.serves("one-wrote", at).await,
        "node two serves node one's write"
    );

    one.runtime.store().promote_voter(two.id()).await.unwrap();
    assert_eq!(two.voter(&one).await, Some(true));
    let at = one.write("after-promotion").await;
    assert!(two.serves("after-promotion", at).await);

    two.runtime.shutdown().await.unwrap();
    one.runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_daemons_one_cluster_survive_the_leader_being_killed() {
    let ca_dir = tempfile::tempdir().unwrap();
    let ca = ca(ca_dir.path());
    let one = Node::start(&ca, "node-one", &[]).await;
    let two = Node::start(&ca, "node-two", &[&one]).await;
    one.runtime.store().promote_voter(two.id()).await.unwrap();
    let three = Node::start(&ca, "node-three", &[&two]).await;
    assert_eq!(
        three.voter(&one).await,
        Some(false),
        "node three joined through node two"
    );
    one.runtime.store().promote_voter(three.id()).await.unwrap();
    let at = one.write("three-voters").await;
    assert!(two.serves("three-voters", at).await);
    assert!(three.serves("three-voters", at).await);

    let Node {
        runtime,
        dir: _kept,
    } = one;
    runtime.shutdown().await.unwrap();

    let leader = leader_among(&[&two, &three])
        .await
        .expect("nodes two and three elect a leader without node one");
    let other = if leader.id() == two.id() {
        &three
    } else {
        &two
    };
    let at = leader.write("after-node-one-died").await;
    assert!(other.serves("after-node-one-died", at).await);

    two.runtime.shutdown().await.unwrap();
    three.runtime.shutdown().await.unwrap();
}
