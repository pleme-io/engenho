//! R7.9 integration tests — PATCH with each `Content-Type` routes through the
//! real apiserver → `decode_patch` → typed `ResourceCommand::Patch` → store
//! interpreter to the CORRECT algorithm. The previous server erased the media
//! type at the apiserver boundary (`decode_patch_body` → bare `Value`) and ran
//! EVERY patch as an RFC7396 merge — silently corrupting json-patch (ops
//! dropped) and strategic-merge (`kubectl set image` replaced the whole
//! container list). These tests pin the four content-type paths end-to-end
//! against a live `StoreMesh`:
//!
//!   * `application/merge-patch+json`            → RFC 7396 merge
//!   * `application/json-patch+json`             → RFC 6902 ops executed
//!   * `application/strategic-merge-patch+json`  → list-merge-by-key
//!   * (no Content-Type, kubectl default)        → strategic
//!   * `application/apply-patch+yaml` (SSA)      → field-managed apply
//!     (full SSA coverage lives in r7_10_server_side_apply.rs)

use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{ApiServer, ResourceHandler, StoreBackedHandler};
use engenho_store::{InProcessRouter, StoreMesh, default_config};

async fn boot() -> (Arc<StoreMesh>, ApiServer) {
    let router = InProcessRouter::new();
    let cfg = default_config("apiserver-r7-9").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);

    // apps/v1 Deployment — its OpenAPI schema carries the
    // `x-kubernetes-patch-merge-key: name` on `spec.template.spec.containers`,
    // so the production `OpenApiPatchEnv` resolves the list-merge-by-key
    // strategy used by the `kubectl set image` test below.
    let deploy: Arc<dyn ResourceHandler> = Arc::new(
        StoreBackedHandler::for_kind(store.clone(), "Deployment")
            .expect("Deployment is a cataloged kind"),
    );

    let server = ApiServer::start("127.0.0.1:0".parse().unwrap(), vec![deploy], None)
        .await
        .unwrap();
    (store, server)
}

/// POST a two-container Deployment `web` (nginx + sidecar) and return its addr.
async fn seed_two_container_deploy(addr: &str, client: &reqwest::Client) {
    let body = serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": "web" },
        "spec": {
            "replicas": 1,
            "template": { "spec": { "containers": [
                {"name": "nginx",   "image": "nginx:1.27"},
                {"name": "sidecar", "image": "envoy:1.30"}
            ] } }
        }
    });
    let resp = client
        .post(format!(
            "http://{addr}/apis/apps/v1/namespaces/default/deployments"
        ))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
}

async fn get_deploy(addr: &str, client: &reqwest::Client) -> serde_json::Value {
    client
        .get(format!(
            "http://{addr}/apis/apps/v1/namespaces/default/deployments/web"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// `application/merge-patch+json` → RFC 7396: replicas updated, rest preserved.
#[tokio::test]
async fn merge_patch_content_type_runs_rfc7396() {
    let (_s, server) = boot().await;
    let addr = server.local_addr().to_string();
    let client = reqwest::Client::new();
    seed_two_container_deploy(&addr, &client).await;

    let resp = client
        .patch(format!(
            "http://{addr}/apis/apps/v1/namespaces/default/deployments/web"
        ))
        .header("Content-Type", "application/merge-patch+json")
        .body(r#"{"spec":{"replicas":3}}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let got = get_deploy(&addr, &client).await;
    assert_eq!(got["spec"]["replicas"], 3, "RFC7396 set replicas");
    // The container list is untouched by a merge that didn't name it.
    assert_eq!(
        got["spec"]["template"]["spec"]["containers"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    server.shutdown().await.unwrap();
}

/// `application/json-patch+json` → RFC 6902: the `replace` op actually executes
/// (the previous server deep-merged the op object into the resource and
/// dropped it — a silent wrong Ok).
#[tokio::test]
async fn json_patch_content_type_executes_ops() {
    let (_s, server) = boot().await;
    let addr = server.local_addr().to_string();
    let client = reqwest::Client::new();
    seed_two_container_deploy(&addr, &client).await;

    let resp = client
        .patch(format!(
            "http://{addr}/apis/apps/v1/namespaces/default/deployments/web"
        ))
        .header("Content-Type", "application/json-patch+json")
        .body(r#"[{"op":"replace","path":"/spec/replicas","value":5}]"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let got = get_deploy(&addr, &client).await;
    assert_eq!(got["spec"]["replicas"], 5, "RFC6902 replace executed");
    // CRUCIALLY: the literal op object was NOT deep-merged in. There is no
    // `op`/`path`/`value` key floating in the resource.
    assert!(got.get("op").is_none(), "op object not merged into resource");
    assert!(got.get("path").is_none());
    server.shutdown().await.unwrap();
}

/// `application/json-patch+json` `remove` op removes just the targeted field.
#[tokio::test]
async fn json_patch_remove_op_executes() {
    let (_s, server) = boot().await;
    let addr = server.local_addr().to_string();
    let client = reqwest::Client::new();

    // Seed with two labels so we can prove `remove` deletes only one.
    let body = serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": "web", "labels": {"env": "prod", "team": "x"} },
        "spec": {}
    });
    client
        .post(format!(
            "http://{addr}/apis/apps/v1/namespaces/default/deployments"
        ))
        .json(&body)
        .send()
        .await
        .unwrap();

    let resp = client
        .patch(format!(
            "http://{addr}/apis/apps/v1/namespaces/default/deployments/web"
        ))
        .header("Content-Type", "application/json-patch+json")
        .body(r#"[{"op":"remove","path":"/metadata/labels/env"}]"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let got = get_deploy(&addr, &client).await;
    assert!(got["metadata"]["labels"].get("env").is_none(), "env removed");
    assert_eq!(got["metadata"]["labels"]["team"], "x", "team preserved");
    server.shutdown().await.unwrap();
}

/// THE list-merge-by-key proof: `application/strategic-merge-patch+json` patches
/// ONLY the named container (nginx) — the sidecar is untouched. This is the
/// `kubectl set image` case; under the old unconditional-merge server the whole
/// `containers` list was replaced and the sidecar vanished.
#[tokio::test]
async fn strategic_merge_content_type_merges_container_by_name() {
    let (_s, server) = boot().await;
    let addr = server.local_addr().to_string();
    let client = reqwest::Client::new();
    seed_two_container_deploy(&addr, &client).await;

    let resp = client
        .patch(format!(
            "http://{addr}/apis/apps/v1/namespaces/default/deployments/web"
        ))
        .header("Content-Type", "application/strategic-merge-patch+json")
        .body(
            r#"{"spec":{"template":{"spec":{"containers":[{"name":"nginx","image":"nginx:1.28"}]}}}}"#,
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let got = get_deploy(&addr, &client).await;
    let containers = got["spec"]["template"]["spec"]["containers"]
        .as_array()
        .unwrap();
    assert_eq!(containers.len(), 2, "no container added or removed");
    let nginx = containers.iter().find(|c| c["name"] == "nginx").unwrap();
    let sidecar = containers.iter().find(|c| c["name"] == "sidecar").unwrap();
    assert_eq!(nginx["image"], "nginx:1.28", "nginx image bumped");
    assert_eq!(sidecar["image"], "envoy:1.30", "SIDECAR UNCHANGED");
    server.shutdown().await.unwrap();
}

/// No `Content-Type` (kubectl's default patch) → strategic. Same list-merge
/// semantics as the explicit strategic-merge-patch+json case.
#[tokio::test]
async fn no_content_type_defaults_to_strategic() {
    let (_s, server) = boot().await;
    let addr = server.local_addr().to_string();
    let client = reqwest::Client::new();
    seed_two_container_deploy(&addr, &client).await;

    // reqwest's `.body(String)` does not set a Content-Type; the server
    // resolves an empty media type to the strategic default.
    let resp = client
        .patch(format!(
            "http://{addr}/apis/apps/v1/namespaces/default/deployments/web"
        ))
        .body(
            r#"{"spec":{"template":{"spec":{"containers":[{"name":"nginx","image":"nginx:1.29"}]}}}}"#
                .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let got = get_deploy(&addr, &client).await;
    let containers = got["spec"]["template"]["spec"]["containers"]
        .as_array()
        .unwrap();
    assert_eq!(containers.len(), 2, "default=strategic merges by key");
    let sidecar = containers.iter().find(|c| c["name"] == "sidecar").unwrap();
    assert_eq!(sidecar["image"], "envoy:1.30", "sidecar untouched under default");
    server.shutdown().await.unwrap();
}

/// `application/apply-patch+yaml` (server-side apply) now MERGES + records
/// field ownership (the 415 stub is gone). A fieldManager-bearing apply on
/// an existing object updates the named field, records managedFields, and
/// leaves un-applied fields intact. (Full SSA scenarios — conflict / force /
/// removal / create — live in r7_10_server_side_apply.rs.)
#[tokio::test]
async fn server_side_apply_content_type_merges_and_records_ownership() {
    let (_s, server) = boot().await;
    let addr = server.local_addr().to_string();
    let client = reqwest::Client::new();
    seed_two_container_deploy(&addr, &client).await;

    let resp = client
        .patch(format!(
            "http://{addr}/apis/apps/v1/namespaces/default/deployments/web?fieldManager=test"
        ))
        .header("Content-Type", "application/apply-patch+yaml")
        .body(r#"{"spec":{"replicas":9}}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "SSA applies, no 415");

    let got = get_deploy(&addr, &client).await;
    assert_eq!(got["spec"]["replicas"], 9, "apply set replicas");
    // managedFields records the manager owning .spec.replicas.
    let mf = got["metadata"]["managedFields"].as_array().unwrap();
    assert!(
        mf.iter().any(|e| e["manager"] == "test" && e["operation"] == "Apply"),
        "managedFields records the apply manager"
    );
    // The container list (not applied) is untouched.
    assert_eq!(
        got["spec"]["template"]["spec"]["containers"]
            .as_array()
            .unwrap()
            .len(),
        2,
        "un-applied containers preserved"
    );
    server.shutdown().await.unwrap();
}
