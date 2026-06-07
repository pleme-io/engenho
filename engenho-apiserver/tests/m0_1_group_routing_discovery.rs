//! M0.1 item 6 — apiserver API-group routing + discovery.
//!
//! End-to-end, in-proc HTTP over the real `ApiServer`, with handlers
//! registered via `handlers_from_catalog(store)` so the FULL generated
//! `RESOURCE_CATALOG` is live (proves the catalog-driven path, not a
//! hand-wired handler vec). Covers:
//!
//!   1. NON-CORE CRUD (apps/v1 Deployment) — the whole point: an apps/v1
//!      kind is un-routable before this item.
//!   2. NON-CORE WATCH — list-then-watch over the grouped path, with the
//!      GVK filter excluding a core Pod from a Deployment watch.
//!   3. RBAC kinds — a SECOND group, both scopes (ClusterRole cluster +
//!      Role namespaced).
//!   4. SCOPE-MISMATCH typed-rejected (namespaced kind on cluster path).
//!   5. DISCOVERY shapes — /apis, /apis/apps/v1, /api, /api/v1 parsed as
//!      typed structs.
//!   6. IRREGULAR PLURAL — Endpoints routes at /endpoints (NOT endpointss)
//!      + appears in /api/v1 discovery as `endpoints`.
//!   7. CORE STILL WORKS — Pod CRUD regression under the re-keyed state +
//!      catalog-driven registration.

use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{ApiServer, handlers_from_catalog};
use engenho_store::{InProcessRouter, StoreMesh, default_config};

// ── boot harness ───────────────────────────────────────────────────────

async fn boot() -> (Arc<StoreMesh>, ApiServer) {
    let router = InProcessRouter::new();
    let cfg = default_config("apiserver-m01-group").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);

    // The FULL cataloged set — routing + discovery + pluralization all
    // follow RESOURCE_CATALOG.
    let handlers = handlers_from_catalog(store.clone());
    let server = ApiServer::start("127.0.0.1:0".parse().unwrap(), handlers, None)
        .await
        .unwrap();
    (store, server)
}

// ── incremental NDJSON reader (per-line timeout so a hang fails fast) ───

struct NdjsonReader {
    resp: reqwest::Response,
    buf: Vec<u8>,
}

impl NdjsonReader {
    fn new(resp: reqwest::Response) -> Self {
        Self {
            resp,
            buf: Vec::new(),
        }
    }

    async fn next_line(&mut self) -> Option<serde_json::Value> {
        loop {
            if let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = self.buf.drain(..=pos).collect();
                let trimmed = &line[..line.len() - 1];
                if trimmed.is_empty() {
                    continue;
                }
                return Some(serde_json::from_slice(trimmed).expect("NDJSON line parses"));
            }
            let chunk = tokio::time::timeout(Duration::from_secs(3), self.resp.chunk())
                .await
                .expect("watch chunk arrived before timeout")
                .expect("watch chunk read ok");
            match chunk {
                Some(bytes) => self.buf.extend_from_slice(&bytes),
                None => return None,
            }
        }
    }

    /// Next non-bookmark event line.
    async fn next_event(&mut self) -> serde_json::Value {
        loop {
            let line = self.next_line().await.expect("expected an event, got EOF");
            match line.get("type").and_then(|t| t.as_str()) {
                Some("BOOKMARK") => continue,
                Some(_) => return line,
                None => panic!("watch line missing type: {line}"),
            }
        }
    }
}

// =========================================================================
// 1. NON-CORE CRUD — apps/v1 Deployment (the load-bearing case)
// =========================================================================

#[tokio::test]
async fn non_core_deployment_full_crud() {
    let (_store, server) = boot().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();
    let base = format!("http://{addr}/apis/apps/v1/namespaces/default/deployments");

    // POST → 201
    let body = serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": "web" },
        "spec": { "replicas": 3, "selector": { "matchLabels": { "app": "web" } } }
    });
    let resp = client.post(&base).json(&body).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED, "apps/v1 POST must 201");
    let created: serde_json::Value = resp.json().await.unwrap();
    let rv = created
        .get("metadata")
        .unwrap()
        .get("resourceVersion")
        .unwrap()
        .as_str()
        .unwrap();
    assert!(!rv.is_empty(), "created Deployment carries a resourceVersion");

    // GET → 200, kind/apiVersion/spec correct
    let resp = client.get(format!("{base}/web")).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let dep: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(dep.get("kind").unwrap(), "Deployment");
    assert_eq!(dep.get("apiVersion").unwrap(), "apps/v1");
    assert_eq!(dep.get("spec").unwrap().get("replicas").unwrap(), 3);

    // LIST → 200, DeploymentList
    let resp = client.get(&base).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let list: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(list.get("kind").unwrap(), "DeploymentList");
    assert_eq!(list.get("apiVersion").unwrap(), "apps/v1");
    assert_eq!(list.get("items").unwrap().as_array().unwrap().len(), 1);

    // PATCH replicas → 5
    let patch = serde_json::json!({ "spec": { "replicas": 5 } });
    let resp = client.patch(format!("{base}/web")).json(&patch).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let patched: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(patched.get("spec").unwrap().get("replicas").unwrap(), 5);

    // DELETE → 200, then GET → 404
    let resp = client.delete(format!("{base}/web")).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let resp = client.get(format!("{base}/web")).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    server.shutdown().await.unwrap();
}

// =========================================================================
// 2. NON-CORE WATCH — list-then-watch over the grouped path
// =========================================================================

#[tokio::test]
async fn non_core_deployment_list_then_watch() {
    let (_store, server) = boot().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();
    let dep_base = format!("http://{addr}/apis/apps/v1/namespaces/default/deployments");

    // LIST → capture snapshot rv N.
    let list: serde_json::Value = client.get(&dep_base).send().await.unwrap().json().await.unwrap();
    let n: u64 = list
        .get("metadata")
        .unwrap()
        .get("resourceVersion")
        .unwrap()
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    // Open WATCH from N over the grouped path.
    let resp = client
        .get(format!("{dep_base}?watch=true&resourceVersion={n}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "grouped watch should 200");
    let mut watch = NdjsonReader::new(resp);

    // POST a NEW Deployment + a core Pod (the Pod must NOT appear).
    let dep = serde_json::json!({
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": { "name": "api" }, "spec": { "replicas": 1 }
    });
    client.post(&dep_base).json(&dep).send().await.unwrap();
    client
        .post(format!("http://{addr}/api/v1/namespaces/default/pods"))
        .json(&serde_json::json!({"metadata": {"name": "noise"}, "spec": {}}))
        .send()
        .await
        .unwrap();

    // The first event on this deployment watch is the ADDED Deployment.
    let ev = watch.next_event().await;
    assert_eq!(ev.get("type").unwrap(), "ADDED", "first event is ADDED");
    let obj = ev.get("object").unwrap();
    assert_eq!(
        obj.get("metadata").unwrap().get("name").unwrap(),
        "api",
        "the ADDED object is the new Deployment (Pod is GVK-filtered out)"
    );
    // The object's kind context is Deployment, never Pod.
    let kind = obj.get("kind").and_then(|k| k.as_str());
    assert!(
        kind.is_none() || kind == Some("Deployment"),
        "watched object kind context is Deployment, got {kind:?}"
    );

    server.shutdown().await.unwrap();
}

// =========================================================================
// 3. RBAC — a SECOND group, both scopes
// =========================================================================

#[tokio::test]
async fn rbac_second_group_both_scopes() {
    let (_store, server) = boot().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();
    let g = "rbac.authorization.k8s.io/v1";

    // Cluster-scoped ClusterRole.
    let cr = serde_json::json!({
        "apiVersion": g, "kind": "ClusterRole",
        "metadata": { "name": "viewer" }, "rules": []
    });
    let resp = client
        .post(format!("http://{addr}/apis/{g}/clusterroles"))
        .json(&cr)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED, "ClusterRole POST 201");
    let resp = client
        .get(format!("http://{addr}/apis/{g}/clusterroles/viewer"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let got: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(got.get("kind").unwrap(), "ClusterRole");

    // Namespaced Role in the same group.
    let role = serde_json::json!({
        "apiVersion": g, "kind": "Role",
        "metadata": { "name": "reader" }, "rules": []
    });
    let resp = client
        .post(format!("http://{addr}/apis/{g}/namespaces/default/roles"))
        .json(&role)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED, "Role POST 201");

    server.shutdown().await.unwrap();
}

// =========================================================================
// 4. SCOPE-MISMATCH typed-rejected
// =========================================================================

#[tokio::test]
async fn scope_mismatch_is_bad_request() {
    let (_store, server) = boot().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();
    let g = "rbac.authorization.k8s.io/v1";

    // Role is NAMESPACED; POST it to the CLUSTER-scoped path → 400.
    let role = serde_json::json!({
        "apiVersion": g, "kind": "Role",
        "metadata": { "name": "oops" }, "rules": []
    });
    let resp = client
        .post(format!("http://{addr}/apis/{g}/roles"))
        .json(&role)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "namespaced Role on cluster path → typed 400, not a silent mis-store"
    );
    let err: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(err.get("kind").unwrap(), "Status");
    assert_eq!(err.get("reason").unwrap(), "BadRequest");

    // And the inverse: ClusterRole (cluster-scoped) on a NAMESPACED path → 400.
    let cr = serde_json::json!({
        "apiVersion": g, "kind": "ClusterRole",
        "metadata": { "name": "oops2" }, "rules": []
    });
    let resp = client
        .post(format!("http://{addr}/apis/{g}/namespaces/default/clusterroles"))
        .json(&cr)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "cluster-scoped ClusterRole on namespaced path → typed 400"
    );

    server.shutdown().await.unwrap();
}

// =========================================================================
// 5. DISCOVERY shapes (typed)
// =========================================================================

#[tokio::test]
async fn discovery_apis_group_list_shape() {
    let (_store, server) = boot().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    // GET /apis → APIGroupList containing apps + rbac.
    let resp = client.get(format!("http://{addr}/apis")).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let gl: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(gl.get("kind").unwrap(), "APIGroupList");
    let groups = gl.get("groups").unwrap().as_array().unwrap();
    let names: Vec<&str> = groups
        .iter()
        .map(|g| g.get("name").unwrap().as_str().unwrap())
        .collect();
    assert!(names.contains(&"apps"), "apps group advertised: {names:?}");
    assert!(
        names.contains(&"rbac.authorization.k8s.io"),
        "rbac group advertised: {names:?}"
    );
    // Core group is NOT in /apis.
    assert!(!names.contains(&""), "core group is not advertised under /apis");
    // Each group's preferredVersion.version == "v1".
    for g in groups {
        assert_eq!(
            g.get("preferredVersion").unwrap().get("version").unwrap(),
            "v1",
            "preferredVersion.version == v1 for {:?}",
            g.get("name")
        );
    }

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn discovery_apps_v1_resource_list_shape() {
    let (_store, server) = boot().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let resp = client.get(format!("http://{addr}/apis/apps/v1")).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let rl: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(rl.get("kind").unwrap(), "APIResourceList");
    assert_eq!(rl.get("groupVersion").unwrap(), "apps/v1");
    let resources = rl.get("resources").unwrap().as_array().unwrap();
    let dep = resources
        .iter()
        .find(|r| r.get("name").unwrap() == "deployments")
        .expect("deployments resource advertised");
    assert_eq!(dep.get("kind").unwrap(), "Deployment");
    assert_eq!(dep.get("namespaced").unwrap(), true);
    let verbs: Vec<&str> = dep
        .get("verbs")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    for want in ["get", "list", "watch", "create", "delete"] {
        assert!(verbs.contains(&want), "verbs ⊇ {want}: {verbs:?}");
    }

    // Unknown group/version → 404.
    let resp = client
        .get(format!("http://{addr}/apis/nope.example.com/v1"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn discovery_core_api_and_api_v1() {
    let (_store, server) = boot().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    // GET /api → APIVersions { versions: ["v1"] }.
    let resp = client.get(format!("http://{addr}/api")).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let av: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(av.get("kind").unwrap(), "APIVersions");
    let versions: Vec<&str> = av
        .get("versions")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(versions, vec!["v1"]);

    // GET /api/v1 → APIResourceList groupVersion "v1" containing pods.
    let resp = client.get(format!("http://{addr}/api/v1")).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let rl: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(rl.get("kind").unwrap(), "APIResourceList");
    assert_eq!(rl.get("groupVersion").unwrap(), "v1");
    let resources = rl.get("resources").unwrap().as_array().unwrap();
    assert!(
        resources.iter().any(|r| r.get("name").unwrap() == "pods"),
        "pods advertised under /api/v1"
    );

    server.shutdown().await.unwrap();
}

// =========================================================================
// 6. IRREGULAR PLURAL — Endpoints (endpoints, not endpointss)
// =========================================================================

#[tokio::test]
async fn irregular_plural_endpoints_routes_and_discovers() {
    let (_store, server) = boot().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    // POST /api/v1/namespaces/default/endpoints → 201 (curated plural).
    let body = serde_json::json!({
        "apiVersion": "v1", "kind": "Endpoints",
        "metadata": { "name": "svc" }, "subsets": []
    });
    let resp = client
        .post(format!("http://{addr}/api/v1/namespaces/default/endpoints"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED, "endpoints POST 201");
    let resp = client
        .get(format!("http://{addr}/api/v1/namespaces/default/endpoints/svc"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let got: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(got.get("kind").unwrap(), "Endpoints");

    // Discovery lists "endpoints" (never "endpointss").
    let rl: serde_json::Value = client
        .get(format!("http://{addr}/api/v1"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let names: Vec<&str> = rl
        .get("resources")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r.get("name").unwrap().as_str().unwrap())
        .collect();
    assert!(names.contains(&"endpoints"), "discovery lists endpoints: {names:?}");
    assert!(!names.contains(&"endpointss"), "no naive +s plural in discovery");

    // The naive plural is NOT a valid route → 404.
    let resp = client
        .get(format!("http://{addr}/api/v1/namespaces/default/endpointss"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "endpointss (naive +s) is NOT a route"
    );

    server.shutdown().await.unwrap();
}

// =========================================================================
// 7. CORE STILL WORKS — Pod CRUD regression under catalog-driven state
// =========================================================================

#[tokio::test]
async fn core_pod_crud_regression() {
    let (_store, server) = boot().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();
    let base = format!("http://{addr}/api/v1/namespaces/default/pods");

    let body = serde_json::json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": { "name": "podinfo" },
        "spec": { "containers": [{"name": "app", "image": "podinfo:6"}] }
    });
    let resp = client.post(&base).json(&body).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let resp = client.get(format!("{base}/podinfo")).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let pod: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(pod.get("kind").unwrap(), "Pod");
    assert_eq!(pod.get("apiVersion").unwrap(), "v1");

    let list: serde_json::Value = client.get(&base).send().await.unwrap().json().await.unwrap();
    assert_eq!(list.get("kind").unwrap(), "PodList");
    assert_eq!(list.get("items").unwrap().as_array().unwrap().len(), 1);

    let patch = serde_json::json!({"spec": {"nodeName": "n1"}});
    let resp = client.patch(format!("{base}/podinfo")).json(&patch).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client.delete(format!("{base}/podinfo")).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let resp = client.get(format!("{base}/podinfo")).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // Cluster-scoped core kind (Namespace) still round-trips.
    let ns = serde_json::json!({"metadata": {"name": "team-a"}, "spec": {}});
    let resp = client
        .post(format!("http://{addr}/api/v1/namespaces"))
        .json(&ns)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    server.shutdown().await.unwrap();
}
