use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{ApiServer, handlers_from_catalog};
use engenho_controllers::{Controller, DeploymentController, ReplicaSetController};
use engenho_store::{InProcessRouter, StoreMesh, default_config};
use serde_json::{Value, json};

const DEPLOYMENT: &str = "/apis/apps/v1/namespaces/default/deployments/web";

async fn boot() -> (Arc<StoreMesh>, ApiServer) {
    let cfg = default_config("a-changed-pod-template-rolls-out").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), InProcessRouter::new(), cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    let server = ApiServer::start(
        "127.0.0.1:0".parse().unwrap(),
        handlers_from_catalog(store.clone()),
        None,
    )
    .await
    .unwrap();
    (store, server)
}

fn rendered(checksum: &str) -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"labels": {"app": "web"}, "name": "web", "namespace": "default"},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "web"}},
            "template": {
                "metadata": {
                    "annotations": {"checksum/config": checksum},
                    "labels": {"app": "web"}
                },
                "spec": {"containers": [{
                    "env": [{"name": "A", "value": "1"}],
                    "image": "nginx:1.27",
                    "name": "main",
                    "ports": [{"containerPort": 80, "name": "http"}]
                }]}
            }
        }
    })
}

async fn patch(server: &ApiServer, content_type: &str, query: &str, body: &Value) -> Value {
    let resp = reqwest::Client::new()
        .patch(format!("http://{}{DEPLOYMENT}{query}", server.local_addr()))
        .header("Content-Type", content_type)
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::OK, "{body}");
    body
}

async fn apply(server: &ApiServer, body: &Value) -> Value {
    patch(
        server,
        "application/apply-patch+yaml",
        "?fieldManager=helm-controller&force=true",
        body,
    )
    .await
}

async fn get(server: &ApiServer) -> Value {
    reqwest::get(format!("http://{}{DEPLOYMENT}", server.local_addr()))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn roll(store: &Arc<StoreMesh>) {
    let deployments = DeploymentController::new(store.clone(), Some("default".into()));
    let replicasets = ReplicaSetController::new(store.clone(), Some("default".into()));
    for _ in 0..3 {
        deployments.tick().await.unwrap();
        replicasets.tick().await.unwrap();
    }
}

async fn live_hash(store: &StoreMesh) -> String {
    let mut out: Vec<(String, i64)> = store
        .list("apps", "v1", "ReplicaSet", Some("default"))
        .await
        .into_iter()
        .filter(|(_, v)| v["metadata"].get("deletionTimestamp").is_none())
        .map(|(_, v)| {
            (
                v["metadata"]["labels"]["pod-template-hash"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                v["spec"]["replicas"].as_i64().unwrap_or(1),
            )
        })
        .collect();
    out.sort();
    let live: Vec<String> = out
        .into_iter()
        .filter(|(_, replicas)| *replicas > 0)
        .map(|(hash, _)| hash)
        .collect();
    assert_eq!(live.len(), 1, "one scaled ReplicaSet: {live:?}");
    live[0].clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pod_template_annotation_change_bumps_generation_and_rolls_a_new_replicaset() {
    let (store, server) = boot().await;

    let born = apply(&server, &rendered("one")).await;
    assert_eq!(born["metadata"]["generation"], json!(1));
    roll(&store).await;
    let first = live_hash(&store).await;

    let upgraded = apply(&server, &rendered("two")).await;
    assert_eq!(
        upgraded["spec"]["template"]["metadata"]["annotations"]["checksum/config"],
        json!("two"),
        "the apply answers the applied template"
    );
    let stored = get(&server).await;
    assert_eq!(
        stored["spec"]["template"]["metadata"]["annotations"]["checksum/config"],
        json!("two"),
        "the applied template annotation is stored"
    );
    assert_eq!(stored["metadata"]["generation"], json!(2));
    assert_eq!(
        stored["spec"]["template"]["spec"]["containers"][0]["ports"],
        json!([{"containerPort": 80, "name": "http"}]),
        "the port is matched by its default protocol, not duplicated"
    );
    roll(&store).await;
    let second = live_hash(&store).await;
    assert_ne!(second, first, "a new ReplicaSet runs the new template");

    patch(
        &server,
        "application/strategic-merge-patch+json",
        "",
        &json!({"spec": {"template": {"metadata": {"annotations": {"checksum/config": "three"}}}}}),
    )
    .await;
    let stored = get(&server).await;
    assert_eq!(stored["metadata"]["generation"], json!(3));
    roll(&store).await;
    let third = live_hash(&store).await;
    assert!(
        third != first && third != second,
        "the strategic patch rolls a third ReplicaSet"
    );
    let owners: Vec<String> = store
        .list("", "v1", "Pod", Some("default"))
        .await
        .into_iter()
        .filter(|(_, v)| v["metadata"].get("deletionTimestamp").is_none())
        .filter_map(|(_, v)| {
            v["metadata"]["ownerReferences"][0]["name"]
                .as_str()
                .map(str::to_string)
        })
        .collect();
    assert_eq!(
        owners,
        [format!("web-{third}")],
        "the one pod runs the newest template"
    );

    server.shutdown().await.unwrap();
}
