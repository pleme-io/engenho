//! R7.10 server-side-apply integration tests — `application/apply-patch+yaml`
//! routes through the real apiserver → `decode_patch` (Apply) +
//! `?fieldManager=`/`?force=` extraction → `ResourceCommand::apply_ssa` →
//! the store's `apply_ssa` interpreter (managedFields ownership + conflict
//! detection + force + field-removal, reusing the strategic associative-list
//! merge). Pins the full SSA surface end-to-end against a live `StoreMesh`:
//!
//!   1. APPLY CREATES + records ownership (managedFields).
//!   2. RE-APPLY updates (same manager, no conflict).
//!   3. FIELD REMOVAL — dropping a field on re-apply removes it.
//!   4. CONFLICT — second manager differing value → 409 + details.causes.
//!   5. FORCE — `?force=true` overrides + transfers ownership → 200.
//!   6. MISSING fieldManager → typed 400.
//!   7. Deployment containers merged by name (strategic list-merge reused).

use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{ApiServer, ResourceHandler, StoreBackedHandler};
use engenho_store::{InProcessRouter, StoreMesh, default_config};

async fn boot() -> (Arc<StoreMesh>, ApiServer) {
    let router = InProcessRouter::new();
    let cfg = default_config("apiserver-r7-10").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);

    let cm: Arc<dyn ResourceHandler> = Arc::new(
        StoreBackedHandler::for_kind(store.clone(), "ConfigMap")
            .expect("ConfigMap is a cataloged kind"),
    );
    let deploy: Arc<dyn ResourceHandler> = Arc::new(
        StoreBackedHandler::for_kind(store.clone(), "Deployment")
            .expect("Deployment is a cataloged kind"),
    );

    let server = ApiServer::start("127.0.0.1:0".parse().unwrap(), vec![cm, deploy], None)
        .await
        .unwrap();
    (store, server)
}

fn cm_url(addr: &str, name: &str) -> String {
    format!("http://{addr}/api/v1/namespaces/default/configmaps/{name}")
}

async fn apply(
    client: &reqwest::Client,
    url: &str,
    manager: &str,
    force: bool,
    body: &str,
) -> reqwest::Response {
    client
        .patch(format!("{url}?fieldManager={manager}&force={force}"))
        .header("Content-Type", "application/apply-patch+yaml")
        .body(body.to_string())
        .send()
        .await
        .unwrap()
}

async fn get_json(client: &reqwest::Client, url: &str) -> serde_json::Value {
    client.get(url).send().await.unwrap().json().await.unwrap()
}

/// 1+2. Apply CREATES (the object doesn't exist) recording ownership, then
/// re-apply by the same manager UPDATES with no conflict.
#[tokio::test]
async fn apply_creates_records_ownership_then_reapply_updates() {
    let (_s, server) = boot().await;
    let addr = server.local_addr().to_string();
    let client = reqwest::Client::new();
    let url = cm_url(&addr, "cm1");

    // CREATE via apply.
    let resp = apply(
        &client,
        &url,
        "mgr",
        false,
        r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm1"},"data":{"a":"1"}}"#,
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "apply creates");

    let got = get_json(&client, &url).await;
    assert_eq!(got["data"]["a"], "1");
    let mf = got["metadata"]["managedFields"].as_array().unwrap();
    assert!(
        mf.iter()
            .any(|e| e["manager"] == "mgr" && e["operation"] == "Apply"),
        "managedFields records the apply manager"
    );

    // RE-APPLY same manager: add data.b, no conflict.
    let resp = apply(
        &client,
        &url,
        "mgr",
        false,
        r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm1"},"data":{"a":"2","b":"3"}}"#,
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "re-apply updates");
    let got = get_json(&client, &url).await;
    assert_eq!(got["data"]["a"], "2");
    assert_eq!(got["data"]["b"], "3");
    server.shutdown().await.unwrap();
}

/// 3. FIELD REMOVAL — dropping data.b on re-apply removes it from the object.
#[tokio::test]
async fn apply_dropping_field_removes_it() {
    let (_s, server) = boot().await;
    let addr = server.local_addr().to_string();
    let client = reqwest::Client::new();
    let url = cm_url(&addr, "cm2");

    apply(
        &client,
        &url,
        "mgr",
        false,
        r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm2"},"data":{"a":"1","b":"2"}}"#,
    )
    .await;

    let resp = apply(
        &client,
        &url,
        "mgr",
        false,
        r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm2"},"data":{"a":"1"}}"#,
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let got = get_json(&client, &url).await;
    assert_eq!(got["data"]["a"], "1");
    assert!(
        got["data"].get("b").is_none(),
        "data.b removed on re-apply that drops it"
    );
    server.shutdown().await.unwrap();
}

/// 4. CONFLICT — a second manager applying data.a with a DIFFERENT value
/// gets a 409 with reason "Conflict" + a details.causes array; the object is
/// NOT silently overwritten.
#[tokio::test]
async fn second_manager_conflict_returns_409_with_causes() {
    let (_s, server) = boot().await;
    let addr = server.local_addr().to_string();
    let client = reqwest::Client::new();
    let url = cm_url(&addr, "cm3");

    apply(
        &client,
        &url,
        "mgr-a",
        false,
        r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm3"},"data":{"a":"1"}}"#,
    )
    .await;

    let resp = apply(
        &client,
        &url,
        "mgr-b",
        false,
        r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm3"},"data":{"a":"2"}}"#,
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::CONFLICT,
        "unforced conflict → 409"
    );
    let status: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(status["reason"], "Conflict");
    let causes = status["details"]["causes"].as_array().unwrap();
    assert!(!causes.is_empty(), "409 carries details.causes");
    assert_eq!(causes[0]["type"], "FieldManagerConflict");
    assert_eq!(causes[0]["field"], ".data.a");

    // The object is UNTOUCHED — no silent overwrite.
    let got = get_json(&client, &url).await;
    assert_eq!(got["data"]["a"], "1", "object byte-identical after 409");
    server.shutdown().await.unwrap();
}

/// 5. FORCE — `?force=true` overrides the conflict + transfers ownership.
#[tokio::test]
async fn force_overrides_conflict_and_transfers_ownership() {
    let (_s, server) = boot().await;
    let addr = server.local_addr().to_string();
    let client = reqwest::Client::new();
    let url = cm_url(&addr, "cm4");

    apply(
        &client,
        &url,
        "mgr-a",
        false,
        r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm4"},"data":{"a":"1"}}"#,
    )
    .await;

    let resp = apply(
        &client,
        &url,
        "mgr-b",
        true, // force
        r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm4"},"data":{"a":"2"}}"#,
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "force → 200");
    let got = get_json(&client, &url).await;
    assert_eq!(got["data"]["a"], "2", "force applied the value");
    let mf = got["metadata"]["managedFields"].as_array().unwrap();
    assert!(
        mf.iter().any(|e| e["manager"] == "mgr-b"),
        "mgr-b now owns the field"
    );
    assert!(
        mf.iter().all(|e| e["manager"] != "mgr-a"),
        "mgr-a emptied → dropped"
    );
    server.shutdown().await.unwrap();
}

/// 6. A missing `?fieldManager=` on an apply is a typed 400 (matching
/// upstream) — never a silent default manager.
#[tokio::test]
async fn apply_without_field_manager_is_bad_request() {
    let (_s, server) = boot().await;
    let addr = server.local_addr().to_string();
    let client = reqwest::Client::new();
    let url = cm_url(&addr, "cm5");

    let resp = client
        .patch(&url) // no ?fieldManager=
        .header("Content-Type", "application/apply-patch+yaml")
        .body(
            r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm5"},"data":{"a":"1"}}"#,
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "missing fieldManager → 400"
    );
    server.shutdown().await.unwrap();
}

/// 7. Deployment apply merges containers BY NAME (reuses the strategic
/// associative-list merge) — applying nginx leaves the sidecar untouched.
#[tokio::test]
async fn apply_deployment_merges_containers_by_name() {
    let (_s, server) = boot().await;
    let addr = server.local_addr().to_string();
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/apis/apps/v1/namespaces/default/deployments/web");

    // Seed a two-container Deployment via POST.
    client
        .post(format!(
            "http://{addr}/apis/apps/v1/namespaces/default/deployments"
        ))
        .json(&serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "web"},
            "spec": {"replicas": 1, "template": {"spec": {"containers": [
                {"name": "nginx", "image": "nginx:1.27"},
                {"name": "sidecar", "image": "envoy:1.30"}
            ]}}}
        }))
        .send()
        .await
        .unwrap();

    let resp = apply(
        &client,
        &url,
        "deployer",
        false,
        r#"{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"web"},"spec":{"template":{"spec":{"containers":[{"name":"nginx","image":"nginx:1.28"}]}}}}"#,
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let got = get_json(&client, &url).await;
    let containers = got["spec"]["template"]["spec"]["containers"]
        .as_array()
        .unwrap();
    assert_eq!(containers.len(), 2, "merge-by-key: no container lost");
    let nginx = containers.iter().find(|c| c["name"] == "nginx").unwrap();
    let sidecar = containers.iter().find(|c| c["name"] == "sidecar").unwrap();
    assert_eq!(nginx["image"], "nginx:1.28", "nginx bumped");
    assert_eq!(sidecar["image"], "envoy:1.30", "SIDECAR UNCHANGED");
    server.shutdown().await.unwrap();
}
