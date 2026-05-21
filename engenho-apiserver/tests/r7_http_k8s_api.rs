//! R7 integration tests — kubectl-style HTTP requests hit the
//! ApiServer; ApiServer routes through the ResourceHandler trait
//! into a real StoreMesh; the K8s resource catalog actually grows.

use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{ApiServer, ResourceHandler, StoreBackedHandler};
use engenho_store::{default_config, InProcessRouter, StoreMesh};

async fn boot_store_and_server() -> (Arc<StoreMesh>, ApiServer) {
    let router = InProcessRouter::new();
    let cfg = default_config("apiserver-r7").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);

    let pod_handler: Arc<dyn ResourceHandler> = Arc::new(StoreBackedHandler::for_core_kind(
        store.clone(),
        "Pod",
        true,
    ));
    let cm_handler: Arc<dyn ResourceHandler> = Arc::new(StoreBackedHandler::for_core_kind(
        store.clone(),
        "ConfigMap",
        true,
    ));
    let ns_handler: Arc<dyn ResourceHandler> = Arc::new(StoreBackedHandler::for_core_kind(
        store.clone(),
        "Namespace",
        false,
    ));

    let server = ApiServer::start(
        "127.0.0.1:0".parse().unwrap(),
        vec![pod_handler, cm_handler, ns_handler],
    )
    .await
    .unwrap();

    (store, server)
}

#[tokio::test]
async fn create_then_get_pod_via_http() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let body = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "podinfo" },
        "spec": { "containers": [{"name": "app", "image": "podinfo:6"}] }
    });
    // POST
    let resp = client
        .post(format!("http://{addr}/api/v1/namespaces/default/pods"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let created: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(created.get("metadata").unwrap().get("name").unwrap(), "podinfo");
    let rv = created
        .get("metadata")
        .unwrap()
        .get("resourceVersion")
        .unwrap()
        .as_str()
        .unwrap();
    assert!(!rv.is_empty());

    // GET
    let resp = client
        .get(format!(
            "http://{addr}/api/v1/namespaces/default/pods/podinfo"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let pod: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(pod.get("kind").unwrap(), "Pod");
    assert_eq!(pod.get("apiVersion").unwrap(), "v1");
    assert_eq!(
        pod.get("spec")
            .unwrap()
            .get("containers")
            .unwrap()
            .get(0)
            .unwrap()
            .get("image")
            .unwrap(),
        "podinfo:6"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn list_pods_returns_pod_list() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    // Create three pods.
    for name in ["a", "b", "c"] {
        let body = serde_json::json!({
            "metadata": { "name": name },
            "spec": {}
        });
        client
            .post(format!("http://{addr}/api/v1/namespaces/default/pods"))
            .json(&body)
            .send()
            .await
            .unwrap();
    }

    // LIST
    let resp = client
        .get(format!("http://{addr}/api/v1/namespaces/default/pods"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let list: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(list.get("kind").unwrap(), "PodList");
    assert_eq!(list.get("apiVersion").unwrap(), "v1");
    let items = list.get("items").unwrap().as_array().unwrap();
    assert_eq!(items.len(), 3);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn patch_pod_merges_into_existing() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let body = serde_json::json!({
        "metadata": { "name": "p" },
        "spec": { "replicas": 1, "image": "v1" }
    });
    client
        .post(format!("http://{addr}/api/v1/namespaces/default/pods"))
        .json(&body)
        .send()
        .await
        .unwrap();

    // PATCH the image only
    let patch = serde_json::json!({"spec": {"image": "v2"}});
    let resp = client
        .patch(format!(
            "http://{addr}/api/v1/namespaces/default/pods/p"
        ))
        .json(&patch)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let patched: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(patched.get("spec").unwrap().get("image").unwrap(), "v2");
    // replicas survived the merge
    assert_eq!(patched.get("spec").unwrap().get("replicas").unwrap(), 1);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn delete_pod_removes_it_from_store() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let body = serde_json::json!({"metadata": {"name": "delete-me"}, "spec": {}});
    client
        .post(format!("http://{addr}/api/v1/namespaces/default/pods"))
        .json(&body)
        .send()
        .await
        .unwrap();

    let resp = client
        .delete(format!(
            "http://{addr}/api/v1/namespaces/default/pods/delete-me"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Subsequent GET should 404.
    let resp = client
        .get(format!(
            "http://{addr}/api/v1/namespaces/default/pods/delete-me"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn create_conflict_when_pod_already_exists() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let body = serde_json::json!({"metadata": {"name": "p"}, "spec": {}});
    let first = client
        .post(format!("http://{addr}/api/v1/namespaces/default/pods"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), reqwest::StatusCode::CREATED);

    let second = client
        .post(format!("http://{addr}/api/v1/namespaces/default/pods"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), reqwest::StatusCode::CONFLICT);
    let err: serde_json::Value = second.json().await.unwrap();
    assert_eq!(err.get("kind").unwrap(), "Status");
    assert_eq!(err.get("status").unwrap(), "Failure");
    assert_eq!(err.get("reason").unwrap(), "AlreadyExists");

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn get_missing_pod_returns_404() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "http://{addr}/api/v1/namespaces/default/pods/ghost"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    let err: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(err.get("reason").unwrap(), "NotFound");
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn cluster_scoped_namespace_kind_round_trips() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let body = serde_json::json!({"metadata": {"name": "engenho-test"}, "spec": {}});
    let resp = client
        .post(format!("http://{addr}/api/v1/namespaces"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .get(format!("http://{addr}/api/v1/namespaces/engenho-test"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let ns: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(ns.get("kind").unwrap(), "Namespace");

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn openapi_spec_is_served_at_canonical_paths() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    // Both /openapi.json and /openapi/v3 must serve the spec.
    for path in ["/openapi.json", "/openapi/v3"] {
        let resp = client
            .get(format!("http://{addr}{path}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        let spec: serde_json::Value = resp.json().await.unwrap();
        // openapi version 3.x
        let v = spec.get("openapi").unwrap().as_str().unwrap();
        assert!(v.starts_with("3."), "{path} returned non-v3 spec: {v}");
        // info.title is our title
        assert_eq!(
            spec.get("info").unwrap().get("title").unwrap(),
            "engenho-apiserver — K8s REST API"
        );
        // The 10 K8s REST paths are documented
        let paths = spec.get("paths").unwrap().as_object().unwrap();
        assert!(paths.contains_key("/api/v1/namespaces/{ns}/{plural}/{name}"));
        assert!(paths.contains_key("/api/v1/namespaces/{ns}/{plural}"));
        assert!(paths.contains_key("/api/v1/{plural}/{name}"));
        assert!(paths.contains_key("/api/v1/{plural}"));
        // The 4 typed schemas are exported
        let schemas = spec
            .get("components")
            .unwrap()
            .get("schemas")
            .unwrap()
            .as_object()
            .unwrap();
        for name in ["K8sResource", "K8sResourceList", "K8sStatus", "K8sPatch"] {
            assert!(schemas.contains_key(name), "missing schema {name} at {path}");
        }
    }

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn unknown_kind_returns_404() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{addr}/api/v1/namespaces/default/widgets"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    server.shutdown().await.unwrap();
}
