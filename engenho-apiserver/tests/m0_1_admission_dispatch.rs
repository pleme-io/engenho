//! M0.1 item 8 (Part D) — admission dispatch on the write path.
//!
//! The apiserver dispatches the existing [`AdmissionChain`] on every
//! create / patch / delete BEFORE proposing to the store:
//!
//!   * a `Mutate` webhook rewrites the stored object;
//!   * a `Deny` webhook rejects the write with a typed 403 Forbidden +
//!     NOTHING is committed;
//!   * the no-admission `handlers_from_catalog` path is unaffected
//!     (regression guard).

use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{ApiServer, ResourceHandler, StoreBackedHandler, handlers_from_catalog};
use engenho_controllers::{
    AdmissionChain, AdmissionDecision, AdmissionMode, FakeAdmissionWebhook,
};
use engenho_store::{InProcessRouter, ResourceKey, StoreMesh, default_config};

async fn boot_store() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("apiserver-admission").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

fn pod_body() -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "podinfo" },
        "spec": { "containers": [{"name": "app", "image": "podinfo:6"}] }
    })
}

fn pod_key() -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", "podinfo")
}

#[tokio::test]
async fn admission_mutate_injects_label_into_stored_object() {
    let store = boot_store().await;

    // Defaulting webhook: inject a default label into the Pod body.
    let hook = Arc::new(FakeAdmissionWebhook::new("defaulter"));
    hook.set_decision(
        &pod_key().label(),
        AdmissionDecision::Mutate(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "podinfo", "labels": { "injected-by": "admission" } },
            "spec": { "containers": [{"name": "app", "image": "podinfo:6"}] }
        })),
    )
    .await;
    let chain = Arc::new(AdmissionChain::new(vec![hook], AdmissionMode::FailClosed));

    let handler: Arc<dyn ResourceHandler> =
        Arc::new(StoreBackedHandler::for_core_kind(store.clone(), "Pod", true).with_admission(chain));
    let server = ApiServer::start("127.0.0.1:0".parse().unwrap(), vec![handler])
        .await
        .unwrap();
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("http://{addr}/api/v1/namespaces/default/pods"))
        .json(&pod_body())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // The stored object carries the admission-injected label.
    let stored = store.get(&pod_key()).await.expect("pod committed");
    assert_eq!(
        stored
            .get("metadata")
            .and_then(|m| m.get("labels"))
            .and_then(|l| l.get("injected-by"))
            .and_then(|v| v.as_str()),
        Some("admission"),
        "admission Mutate must rewrite the committed object"
    );

    server.shutdown().await.unwrap();
    drop(store);
}

#[tokio::test]
async fn admission_deny_returns_403_and_commits_nothing() {
    let store = boot_store().await;

    let hook = Arc::new(FakeAdmissionWebhook::new("policy"));
    hook.set_decision(
        &pod_key().label(),
        AdmissionDecision::Deny("pods must declare resource requests".into()),
    )
    .await;
    let chain = Arc::new(AdmissionChain::new(vec![hook], AdmissionMode::FailClosed));

    let handler: Arc<dyn ResourceHandler> =
        Arc::new(StoreBackedHandler::for_core_kind(store.clone(), "Pod", true).with_admission(chain));
    let server = ApiServer::start("127.0.0.1:0".parse().unwrap(), vec![handler])
        .await
        .unwrap();
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("http://{addr}/api/v1/namespaces/default/pods"))
        .json(&pod_body())
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "admission Deny → HTTP 403 Forbidden"
    );
    // The denied object was NEVER committed.
    assert!(
        store.get(&pod_key()).await.is_none(),
        "nothing committed on a denied create"
    );
    // The Status body carries reason "Forbidden" + the deny message.
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body.get("kind").unwrap(), "Status");
    assert_eq!(body.get("reason").unwrap(), "Forbidden");
    let msg = body.get("message").unwrap().as_str().unwrap();
    assert!(
        msg.contains("policy") && msg.contains("resource requests"),
        "deny message carries the webhook name + reason: {msg}"
    );

    server.shutdown().await.unwrap();
    drop(store);
}

#[tokio::test]
async fn no_admission_path_is_unaffected() {
    // Regression guard: handlers_from_catalog (no admission) admits a
    // plain Pod create exactly as before.
    let store = boot_store().await;
    let handlers = handlers_from_catalog(store.clone());
    let server = ApiServer::start("127.0.0.1:0".parse().unwrap(), handlers)
        .await
        .unwrap();
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("http://{addr}/api/v1/namespaces/default/pods"))
        .json(&pod_body())
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::CREATED,
        "no-admission create still succeeds"
    );
    assert!(
        store.get(&pod_key()).await.is_some(),
        "object committed via the no-admission path"
    );

    server.shutdown().await.unwrap();
    drop(store);
}
