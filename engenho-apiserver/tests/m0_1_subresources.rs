//! `/status` + `/scale` subresources — catalog-declared, dispatched through
//! the real coords → handler path, in-process over the real `ApiServer`.
//!
//! Covers:
//!   1. `/scale` GET projects an autoscaling/v1 Scale off the parent.
//!   2. `/scale` PUT writes ONLY `spec.replicas` (status untouched).
//!   3. `/scale` PATCH (Scale-shaped merge) updates `spec.replicas`.
//!   4. `/status` PUT preserves `spec` (status-only write).
//!   5. `/status` PATCH preserves `spec` (a /status patch naming spec is
//!      ignored).
//!   6. A kind WITHOUT scale (ConfigMap) → typed 404 on `/scale`.
//!   7. Discovery advertises `deployments/scale` + `deployments/status`
//!      under apps/v1, `pods/status` (but NOT `pods/scale`) under /api/v1.
//!   8. POST/DELETE on a subresource → typed 4xx (no stub Ok).

use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{ApiServer, handlers_from_catalog};
use engenho_store::{InProcessRouter, StoreMesh, default_config};

async fn boot() -> (Arc<StoreMesh>, ApiServer) {
    let router = InProcessRouter::new();
    let cfg = default_config("apiserver-subresources").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    let handlers = handlers_from_catalog(store.clone());
    let server = ApiServer::start("127.0.0.1:0".parse().unwrap(), handlers, None)
        .await
        .unwrap();
    (store, server)
}

/// POST a 3-replica Deployment `web` into default; return its base URL.
async fn create_web(addr: std::net::SocketAddr, client: &reqwest::Client) -> String {
    let base = format!("http://{addr}/apis/apps/v1/namespaces/default/deployments");
    let body = serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": "web" },
        "spec": { "replicas": 3, "selector": { "matchLabels": { "app": "web" } } }
    });
    let resp = client.post(&base).json(&body).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    base
}

async fn get_json(client: &reqwest::Client, url: &str) -> serde_json::Value {
    client.get(url).send().await.unwrap().json().await.unwrap()
}

fn replicas(v: &serde_json::Value) -> i64 {
    v.get("spec")
        .and_then(|s| s.get("replicas"))
        .and_then(|r| r.as_i64())
        .unwrap_or(-1)
}

#[tokio::test]
async fn scale_get_projects_autoscaling_v1_scale() {
    let (_store, server) = boot().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();
    let base = create_web(addr, &client).await;

    let scale = get_json(&client, &format!("{base}/web/scale")).await;
    assert_eq!(scale.get("apiVersion").unwrap(), "autoscaling/v1");
    assert_eq!(scale.get("kind").unwrap(), "Scale");
    assert_eq!(scale.get("spec").unwrap().get("replicas").unwrap(), 3);
    // selector projected as a label string.
    assert_eq!(
        scale
            .get("status")
            .unwrap()
            .get("selector")
            .and_then(|s| s.as_str()),
        Some("app=web")
    );
    // The Scale's rv IS the parent's rv (CAS round-trips).
    let parent = get_json(&client, &format!("{base}/web")).await;
    assert_eq!(
        scale.get("metadata").unwrap().get("resourceVersion"),
        parent.get("metadata").unwrap().get("resourceVersion")
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn scale_put_writes_only_spec_replicas() {
    let (_store, server) = boot().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();
    let base = create_web(addr, &client).await;

    // PUT /scale with replicas=5.
    let scale_body = serde_json::json!({
        "apiVersion": "autoscaling/v1",
        "kind": "Scale",
        "metadata": { "name": "web", "namespace": "default" },
        "spec": { "replicas": 5 }
    });
    let resp = client
        .put(format!("{base}/web/scale"))
        .json(&scale_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let returned: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(returned.get("kind").unwrap(), "Scale");
    assert_eq!(returned.get("spec").unwrap().get("replicas").unwrap(), 5);

    // The parent's spec.replicas is now 5.
    let parent = get_json(&client, &format!("{base}/web")).await;
    assert_eq!(
        replicas(&parent),
        5,
        "scale PUT updated parent spec.replicas"
    );
    // The selector (other spec content) is preserved.
    assert_eq!(
        parent
            .get("spec")
            .unwrap()
            .get("selector")
            .unwrap()
            .get("matchLabels")
            .unwrap()
            .get("app")
            .unwrap(),
        "web",
        "scale PUT preserved the rest of spec"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn scale_patch_updates_spec_replicas() {
    let (_store, server) = boot().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();
    let base = create_web(addr, &client).await;

    let patch = serde_json::json!({ "spec": { "replicas": 7 } });
    let resp = client
        .patch(format!("{base}/web/scale"))
        .header("Content-Type", "application/merge-patch+json")
        .json(&patch)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let returned: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(returned.get("spec").unwrap().get("replicas").unwrap(), 7);

    let parent = get_json(&client, &format!("{base}/web")).await;
    assert_eq!(replicas(&parent), 7);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn status_put_preserves_spec() {
    let (_store, server) = boot().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();
    let base = create_web(addr, &client).await;

    // PUT /status carrying a stale spec.replicas — it MUST be ignored.
    let status_body = serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": "web", "namespace": "default" },
        "spec": { "replicas": 99 },
        "status": { "observedGeneration": 7, "replicas": 3, "readyReplicas": 3 }
    });
    let resp = client
        .put(format!("{base}/web/status"))
        .json(&status_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let parent = get_json(&client, &format!("{base}/web")).await;
    // spec.replicas is STILL 3 (the /status PUT did NOT touch spec).
    assert_eq!(
        replicas(&parent),
        3,
        "status PUT must NOT clobber spec.replicas"
    );
    // status.observedGeneration was written.
    assert_eq!(
        parent
            .get("status")
            .unwrap()
            .get("observedGeneration")
            .unwrap(),
        7
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn status_patch_ignores_spec() {
    let (_store, server) = boot().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();
    let base = create_web(addr, &client).await;

    // A /status merge patch naming both spec and status — spec is dropped.
    let patch = serde_json::json!({
        "spec": { "replicas": 42 },
        "status": { "observedGeneration": 9 }
    });
    let resp = client
        .patch(format!("{base}/web/status"))
        .header("Content-Type", "application/merge-patch+json")
        .json(&patch)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let parent = get_json(&client, &format!("{base}/web")).await;
    assert_eq!(replicas(&parent), 3, "status PATCH must ignore spec");
    assert_eq!(
        parent
            .get("status")
            .unwrap()
            .get("observedGeneration")
            .unwrap(),
        9
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn main_put_preserves_status() {
    // The REVERSE of `status_put_preserves_spec`: a MAIN-object PUT on a
    // status-bearing kind MUST NOT change `.status` (k8s writes status only
    // through `/status`). engenho's `replace()` preserves the live status for
    // any kind declaring a Status subresource.
    let (_store, server) = boot().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();
    let base = create_web(addr, &client).await;

    // Seed a status via /status (so the live object HAS a status to protect).
    let status_body = serde_json::json!({
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": { "name": "web", "namespace": "default" },
        "status": { "observedGeneration": 5, "replicas": 3 }
    });
    let resp = client
        .put(format!("{base}/web/status"))
        .json(&status_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // MAIN PUT: change spec.replicas (a real spec edit) AND carry a bogus
    // status. The spec edit MUST land; the status edit MUST be dropped.
    let main_body = serde_json::json!({
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": { "name": "web", "namespace": "default" },
        "spec": { "replicas": 8, "selector": { "matchLabels": { "app": "web" } } },
        "status": { "observedGeneration": 999, "replicas": 999 }
    });
    let resp = client
        .put(format!("{base}/web"))
        .json(&main_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let parent = get_json(&client, &format!("{base}/web")).await;
    // The spec edit landed.
    assert_eq!(replicas(&parent), 8, "main PUT applied the spec edit");
    // The status edit was DROPPED — the live status (observedGeneration 5)
    // survives, NOT the 999 the client tried to write through the main object.
    assert_eq!(
        parent
            .get("status")
            .unwrap()
            .get("observedGeneration")
            .unwrap(),
        5,
        "main PUT must NOT clobber status (status is /status-only)"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn scale_on_kind_without_scale_is_typed_404() {
    let (_store, server) = boot().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    // Create a ConfigMap, then GET /scale on it — ConfigMap declares no
    // scale subresource → typed 404 K8s Status (NOT panic / 500).
    let cm_base = format!("http://{addr}/api/v1/namespaces/default/configmaps");
    let cm = serde_json::json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": { "name": "x" }, "data": { "k": "v" }
    });
    let resp = client.post(&cm_base).json(&cm).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .get(format!("{cm_base}/x/scale"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "configmaps/x/scale must be a typed 404"
    );
    let status: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(status.get("kind").unwrap(), "Status");
    assert_eq!(status.get("status").unwrap(), "Failure");

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn discovery_advertises_subresource_rows() {
    let (_store, server) = boot().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    // apps/v1: deployments + deployments/scale + deployments/status.
    let apps: serde_json::Value = get_json(&client, &format!("http://{addr}/apis/apps/v1")).await;
    let names: Vec<&str> = apps
        .get("resources")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r.get("name").unwrap().as_str().unwrap())
        .collect();
    assert!(names.contains(&"deployments"));
    assert!(names.contains(&"deployments/scale"), "scale row advertised");
    assert!(
        names.contains(&"deployments/status"),
        "status row advertised"
    );

    // The scale row carries autoscaling/v1/Scale.
    let scale_row = apps
        .get("resources")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r.get("name").unwrap() == "deployments/scale")
        .unwrap();
    assert_eq!(scale_row.get("kind").unwrap(), "Scale");
    assert_eq!(scale_row.get("group").unwrap(), "autoscaling");
    assert_eq!(scale_row.get("version").unwrap(), "v1");
    let verbs: Vec<&str> = scale_row
        .get("verbs")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(verbs.contains(&"get") && verbs.contains(&"patch") && verbs.contains(&"update"));

    // The status row inherits the parent's GV (group/version OMITTED).
    let status_row = apps
        .get("resources")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r.get("name").unwrap() == "deployments/status")
        .unwrap();
    assert_eq!(status_row.get("kind").unwrap(), "Deployment");
    assert!(status_row.get("group").is_none(), "status row inherits GV");

    // /api/v1: pods/status present, pods/scale absent (Pod has no scale).
    let core: serde_json::Value = get_json(&client, &format!("http://{addr}/api/v1")).await;
    let core_names: Vec<&str> = core
        .get("resources")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r.get("name").unwrap().as_str().unwrap())
        .collect();
    assert!(
        core_names.contains(&"pods/status"),
        "pods/status advertised"
    );
    assert!(
        !core_names.contains(&"pods/scale"),
        "pods/scale must NOT be advertised (Pod has no scale)"
    );
    // configmaps has NO subresource rows.
    assert!(!core_names.contains(&"configmaps/status"));
    assert!(!core_names.contains(&"configmaps/scale"));

    // Rows sort stable: deployments < deployments/scale < deployments/status.
    let dep_idx = names.iter().position(|n| *n == "deployments").unwrap();
    let scale_idx = names
        .iter()
        .position(|n| *n == "deployments/scale")
        .unwrap();
    let status_idx = names
        .iter()
        .position(|n| *n == "deployments/status")
        .unwrap();
    assert!(
        dep_idx < scale_idx && scale_idx < status_idx,
        "rows sorted by name"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn post_and_delete_on_subresource_are_typed_rejections() {
    let (_store, server) = boot().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();
    let base = create_web(addr, &client).await;

    // POST /scale → typed BadRequest (no subresource supports create).
    let resp = client
        .post(format!("{base}/web/scale"))
        .json(&serde_json::json!({"spec": {"replicas": 1}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    // DELETE /status → typed BadRequest.
    let resp = client
        .delete(format!("{base}/web/status"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    server.shutdown().await.unwrap();
}
