//! End-to-end: a `MutatingWebhookConfiguration` + a webhook returning a
//! base64 RFC6902 JSONPatch → the apiserver calls it on Pod CREATE → the
//! injected sidecar + init container land in the PERSISTED Pod.
//!
//! This is the mesh-enablement proof: `lareira-enxerto` (tatara-mesh's sidecar
//! injector) is a `MutatingWebhookConfiguration`; this test shows engenho
//! hosts it. The injected init container then runs via the kubelet's
//! init-container driver (commit a7810a6) — the loop closes.
//!
//! The HTTP call-out is the mock [`MockWebhookCaller`] (Environment-trait
//! discipline — no real webhook server). The config source is the REAL
//! store-backed [`StoreWebhookConfigSource`], so the config is read out of the
//! same mesh the apiserver writes to: applying the config via the API IS what
//! activates the webhook.

use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::webhook_admission::{StoreWebhookConfigSource, catalog_pluralizer};
use engenho_apiserver::{ApiServer, ResourceHandler, StoreBackedHandler, handlers_from_catalog};
use engenho_controllers::admission_webhook::{MockWebhookCaller, MutatingWebhookPlugin};
use engenho_controllers::{AdmissionChain, AdmissionMode};
use engenho_store::{InProcessRouter, ResourceKey, StoreMesh, default_config};

async fn boot_store() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("apiserver-mwh-e2e").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

const ENDPOINT: &str = "https://lareira-enxerto.mesh.svc/mutate";

fn mwc_body() -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": { "name": "lareira-enxerto" },
        "webhooks": [{
            "name": "inject.tatara-mesh.io",
            "clientConfig": { "url": ENDPOINT },
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": [""],
                "apiVersions": ["v1"],
                "resources": ["pods"]
            }],
            "failurePolicy": "Fail",
            "timeoutSeconds": 5
        }]
    })
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

fn mwc_key() -> ResourceKey {
    ResourceKey::cluster_scoped(
        "admissionregistration.k8s.io",
        "v1",
        "MutatingWebhookConfiguration",
        "lareira-enxerto",
    )
}

/// The injector's JSONPatch: append the aresta proxy sidecar + a SPIRE-agent
/// init container — exactly what a service-mesh injector does.
fn injector_ops() -> serde_json::Value {
    serde_json::json!([
        {"op": "add", "path": "/spec/containers/-",
         "value": {"name": "aresta-proxy", "image": "ghcr.io/pleme-io/aresta:1.0"}},
        {"op": "add", "path": "/spec/initContainers",
         "value": [{"name": "spire-agent-init", "image": "ghcr.io/pleme-io/spire-agent:1.0"}]}
    ])
}

#[tokio::test]
async fn mutating_webhook_injects_sidecar_into_persisted_pod() {
    let store = boot_store().await;

    // The mock webhook server returns the injector's JSONPatch.
    let caller = Arc::new(MockWebhookCaller::new());
    caller
        .set_response(
            ENDPOINT,
            MockWebhookCaller::json_patch_review("inject-uid", &injector_ops()),
        )
        .await;

    // The REAL store-backed config source + the mutating plugin in the chain.
    let plugin = Arc::new(MutatingWebhookPlugin::new(
        Arc::new(StoreWebhookConfigSource::new(store.clone())),
        caller.clone(),
        catalog_pluralizer(),
    ));
    let chain = Arc::new(AdmissionChain::new(vec![plugin], AdmissionMode::FailClosed));

    // Pod handler (admission-dispatched) + a plain MWC handler so we can apply
    // the config object into the store via the API.
    let pod_handler: Arc<dyn ResourceHandler> = Arc::new(
        StoreBackedHandler::for_core_kind(store.clone(), "Pod", true).with_admission(chain),
    );
    let mwc_handler: Arc<dyn ResourceHandler> = Arc::new(
        StoreBackedHandler::for_kind(store.clone(), "MutatingWebhookConfiguration")
            .expect("MutatingWebhookConfiguration is cataloged"),
    );
    let server = ApiServer::start(
        "127.0.0.1:0".parse().unwrap(),
        vec![pod_handler, mwc_handler],
        None,
    )
    .await
    .unwrap();
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    // 1) kubectl apply the MutatingWebhookConfiguration → stored.
    let resp = client
        .post(format!(
            "http://{addr}/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations"
        ))
        .json(&mwc_body())
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::CREATED,
        "MutatingWebhookConfiguration apply must succeed"
    );
    assert!(store.get(&mwc_key()).await.is_some(), "MWC committed");

    // 2) POST a plain Pod → the apiserver calls the webhook + applies the patch.
    let resp = client
        .post(format!("http://{addr}/api/v1/namespaces/default/pods"))
        .json(&pod_body())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 3) The PERSISTED Pod carries the injected sidecar + init container.
    let stored = store.get(&pod_key()).await.expect("pod committed");
    let containers = stored["spec"]["containers"].as_array().unwrap();
    assert_eq!(containers.len(), 2, "aresta proxy sidecar injected");
    assert!(
        containers.iter().any(|c| c["name"] == "aresta-proxy"),
        "the aresta proxy sidecar is in the persisted Pod"
    );
    let inits = stored["spec"]["initContainers"]
        .as_array()
        .expect("init container injected");
    assert_eq!(inits.len(), 1);
    assert_eq!(
        inits[0]["name"], "spire-agent-init",
        "the SPIRE-agent init container is in the persisted Pod — the kubelet \
         init-container driver (a7810a6) runs it: mesh loop closed"
    );

    // The webhook was actually called once, with a CREATE AdmissionReview for pods.
    let calls = caller.calls().await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, ENDPOINT);
    assert_eq!(calls[0].2["request"]["operation"], "CREATE");
    assert_eq!(calls[0].2["request"]["resource"]["resource"], "pods");
    assert_eq!(calls[0].2["request"]["object"]["metadata"]["name"], "podinfo");

    server.shutdown().await.unwrap();
    drop(store);
}

#[tokio::test]
async fn no_webhook_config_means_plain_pod_unchanged() {
    // Regression guard: with the mutating plugin in the chain but NO
    // MutatingWebhookConfiguration applied, a Pod create is unchanged + the
    // webhook is never called.
    let store = boot_store().await;
    let caller = Arc::new(MockWebhookCaller::new());
    let plugin = Arc::new(MutatingWebhookPlugin::new(
        Arc::new(StoreWebhookConfigSource::new(store.clone())),
        caller.clone(),
        catalog_pluralizer(),
    ));
    let chain = Arc::new(AdmissionChain::new(vec![plugin], AdmissionMode::FailClosed));
    let handler: Arc<dyn ResourceHandler> = Arc::new(
        StoreBackedHandler::for_core_kind(store.clone(), "Pod", true).with_admission(chain),
    );
    let server = ApiServer::start("127.0.0.1:0".parse().unwrap(), vec![handler], None)
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
    let stored = store.get(&pod_key()).await.expect("pod committed");
    assert_eq!(
        stored["spec"]["containers"].as_array().unwrap().len(),
        1,
        "no MWC applied → Pod unchanged"
    );
    assert_eq!(caller.calls().await.len(), 0, "no webhook called");

    server.shutdown().await.unwrap();
    drop(store);
}

#[tokio::test]
async fn failure_policy_fail_rejects_pod_when_webhook_errors() {
    let store = boot_store().await;
    let caller = Arc::new(MockWebhookCaller::new());
    caller.set_error(ENDPOINT, "connection refused").await;
    let plugin = Arc::new(MutatingWebhookPlugin::new(
        Arc::new(StoreWebhookConfigSource::new(store.clone())),
        caller,
        catalog_pluralizer(),
    ));
    let chain = Arc::new(AdmissionChain::new(vec![plugin], AdmissionMode::FailClosed));
    let pod_handler: Arc<dyn ResourceHandler> = Arc::new(
        StoreBackedHandler::for_core_kind(store.clone(), "Pod", true).with_admission(chain),
    );
    let mwc_handler: Arc<dyn ResourceHandler> = Arc::new(
        StoreBackedHandler::for_kind(store.clone(), "MutatingWebhookConfiguration").unwrap(),
    );
    let server = ApiServer::start(
        "127.0.0.1:0".parse().unwrap(),
        vec![pod_handler, mwc_handler],
        None,
    )
    .await
    .unwrap();
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    // Apply the failurePolicy=Fail config.
    client
        .post(format!(
            "http://{addr}/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations"
        ))
        .json(&mwc_body())
        .send()
        .await
        .unwrap();

    // The webhook errors → failurePolicy=Fail → 403 + nothing committed.
    let resp = client
        .post(format!("http://{addr}/api/v1/namespaces/default/pods"))
        .json(&pod_body())
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "failurePolicy=Fail + webhook error → 403"
    );
    assert!(
        store.get(&pod_key()).await.is_none(),
        "nothing committed when the webhook denies the create"
    );

    server.shutdown().await.unwrap();
    drop(store);
}

/// Sanity: the no-admission cataloged path still serves the new
/// admissionregistration kinds (kubectl apply works without any webhook plugin).
#[tokio::test]
async fn admissionregistration_kinds_are_served() {
    let store = boot_store().await;
    let handlers = handlers_from_catalog(store.clone());
    let server = ApiServer::start("127.0.0.1:0".parse().unwrap(), handlers, None)
        .await
        .unwrap();
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    for plural in [
        "mutatingwebhookconfigurations",
        "validatingwebhookconfigurations",
    ] {
        let body = serde_json::json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": if plural.starts_with("mutating") {
                "MutatingWebhookConfiguration"
            } else {
                "ValidatingWebhookConfiguration"
            },
            "metadata": { "name": "x" },
            "webhooks": []
        });
        let resp = client
            .post(format!(
                "http://{addr}/apis/admissionregistration.k8s.io/v1/{plural}"
            ))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::CREATED,
            "{plural} must be served + applyable"
        );
    }

    server.shutdown().await.unwrap();
    drop(store);
}
