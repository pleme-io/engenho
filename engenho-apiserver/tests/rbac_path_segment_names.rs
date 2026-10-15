use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{ApiServer, ResourceHandler, StoreBackedHandler};
use engenho_store::{InProcessRouter, StoreMesh, default_config};
use serde_json::json;

#[tokio::test]
async fn a_cluster_role_named_with_colons_is_created_and_served() {
    let router = InProcessRouter::new();
    let store = Arc::new(
        StoreMesh::start(
            1,
            "in-process://1".into(),
            router,
            default_config("apiserver-rbac-names").unwrap(),
        )
        .await
        .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    let handlers: Vec<Arc<dyn ResourceHandler>> = ["ClusterRole", "ConfigMap"]
        .into_iter()
        .map(|k| {
            Arc::new(StoreBackedHandler::for_kind(store.clone(), k).expect("cataloged"))
                as Arc<dyn ResourceHandler>
        })
        .collect();
    let server = ApiServer::start("127.0.0.1:0".parse().unwrap(), handlers, None)
        .await
        .unwrap();
    let addr = server.local_addr().to_string();
    let client = reqwest::Client::new();
    let base = format!("http://{addr}/apis/rbac.authorization.k8s.io/v1/clusterroles");

    let name = "monitoring:monitoring:vmagent-rio";
    let resp = client
        .post(&base)
        .json(&json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRole",
            "metadata": { "name": name },
            "rules": [{ "apiGroups": [""], "resources": ["nodes"], "verbs": ["get"] }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED, "{}", resp.text().await.unwrap());
    let got: serde_json::Value = client
        .get(format!("{base}/{name}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(got["metadata"]["name"], name);

    let resp = client
        .post(&base)
        .json(&json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRole",
            "metadata": { "name": ".." }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);

    let resp = client
        .post(format!("http://{addr}/api/v1/namespaces/default/configmaps"))
        .json(&json!({ "apiVersion": "v1", "kind": "ConfigMap", "metadata": { "name": "a:b" } }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    server.shutdown().await.unwrap();
}
