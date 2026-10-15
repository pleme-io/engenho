use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{ApiServer, ResourceHandler, StoreBackedHandler};
use engenho_store::{InProcessRouter, StoreMesh, default_config};
use serde_json::{Value, json};

async fn boot() -> (Arc<StoreMesh>, ApiServer) {
    let router = InProcessRouter::new();
    let cfg = default_config("apiserver-uid").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    let deploy: Arc<dyn ResourceHandler> = Arc::new(
        StoreBackedHandler::for_kind(store.clone(), "Deployment")
            .expect("Deployment is a cataloged kind"),
    );
    let server = ApiServer::start("127.0.0.1:0".parse().unwrap(), vec![deploy], None)
        .await
        .unwrap();
    (store, server)
}

fn deployment(name: &str, uid: Option<&str>) -> Value {
    let mut metadata = json!({ "name": name, "namespace": "default" });
    if let Some(uid) = uid {
        metadata["uid"] = json!(uid);
    }
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": metadata,
        "spec": {
            "replicas": 1,
            "selector": { "matchLabels": { "app": name } },
            "template": {
                "metadata": { "labels": { "app": name } },
                "spec": { "containers": [{ "name": "c", "image": "nginx:1.27" }] }
            }
        }
    })
}

async fn create(addr: &str, client: &reqwest::Client, body: &Value) -> Value {
    let resp = client
        .post(format!("http://{addr}/apis/apps/v1/namespaces/default/deployments"))
        .json(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    resp.json().await.unwrap()
}

async fn get(addr: &str, client: &reqwest::Client, name: &str) -> Value {
    client
        .get(format!(
            "http://{addr}/apis/apps/v1/namespaces/default/deployments/{name}"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

fn uid_of(v: &Value) -> &str {
    v["metadata"]["uid"].as_str().unwrap_or("")
}

#[tokio::test]
async fn a_create_whose_body_carries_an_empty_uid_is_given_one() {
    let (_s, server) = boot().await;
    let addr = server.local_addr().to_string();
    let client = reqwest::Client::new();
    let created = create(&addr, &client, &deployment("empty", Some(""))).await;
    assert!(!uid_of(&created).is_empty(), "{}", created["metadata"]);
    assert_eq!(uid_of(&get(&addr, &client, "empty").await), uid_of(&created));
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_create_never_keeps_a_uid_the_client_chose() {
    let (_s, server) = boot().await;
    let addr = server.local_addr().to_string();
    let client = reqwest::Client::new();
    let a = create(&addr, &client, &deployment("a", Some("chosen"))).await;
    let b = create(&addr, &client, &deployment("b", Some("chosen"))).await;
    assert_ne!(uid_of(&a), "chosen");
    assert_ne!(uid_of(&a), uid_of(&b));
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_update_whose_body_carries_an_empty_uid_keeps_the_stored_one() {
    let (_s, server) = boot().await;
    let addr = server.local_addr().to_string();
    let client = reqwest::Client::new();
    let created = create(&addr, &client, &deployment("web", None)).await;
    let mut body = deployment("web", Some(""));
    body["metadata"]["resourceVersion"] = created["metadata"]["resourceVersion"].clone();
    body["spec"]["replicas"] = json!(2);
    let resp = client
        .put(format!(
            "http://{addr}/apis/apps/v1/namespaces/default/deployments/web"
        ))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let got = get(&addr, &client, "web").await;
    assert_eq!(got["spec"]["replicas"], 2);
    assert_eq!(uid_of(&got), uid_of(&created));
    server.shutdown().await.unwrap();
}
