//! R7 integration tests — kubectl-style HTTP requests hit the
//! ApiServer; ApiServer routes through the ResourceHandler trait
//! into a real StoreMesh; the K8s resource catalog actually grows.

use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{ApiServer, ResourceHandler, StoreBackedHandler};
use engenho_store::{InProcessRouter, StoreMesh, default_config};

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
    // apps/v1 Deployment — used by the DELETE protobuf round-trip test
    // (its GVK resolves cleanly in the kube-proto pool, unlike Status).
    let deploy_handler: Arc<dyn ResourceHandler> = Arc::new(
        StoreBackedHandler::for_kind(store.clone(), "Deployment")
            .expect("Deployment is a cataloged kind"),
    );

    let server = ApiServer::start(
        "127.0.0.1:0".parse().unwrap(),
        vec![pod_handler, cm_handler, ns_handler, deploy_handler],
        None,
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
    assert_eq!(
        created.get("metadata").unwrap().get("name").unwrap(),
        "podinfo"
    );
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
            "spec": { "containers": [ { "name": "c", "image": "busybox:1.36" } ] }
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
        // `replicas`/`image` are not PodSpec fields — they are merge
        // probes, kept because this test is about PATCH semantics. The
        // container list is what makes the object VALID; without it the
        // create is a 422 and the patch then 404s.
        "spec": {
            "containers": [ { "name": "c", "image": "busybox:1.36" } ],
            "replicas": 1,
            "image": "v1"
        }
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
        .patch(format!("http://{addr}/api/v1/namespaces/default/pods/p"))
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

    let body = serde_json::json!({"metadata": {"name": "delete-me"}, "spec": { "containers": [ { "name": "c", "image": "busybox:1.36" } ] }});
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
    // The DELETE wire returns a NON-empty typed body (the deleted object).
    // An empty 200 crashes kubectl's `json.Unmarshal([]byte{})`.
    let body = resp.bytes().await.unwrap();
    assert!(
        !body.is_empty(),
        "DELETE 200 must carry a non-empty body (empty crashes kubectl)"
    );

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

/// DELETE of an existing object with `Accept: application/json` returns
/// HTTP 200 with a NON-empty body that parses to the deleted object
/// (apiVersion/kind/metadata.name present). This is the bug the empty-body
/// DELETE caused: kubectl ran `json.Unmarshal([]byte{})` → "unexpected end
/// of JSON input". The body MUST be the typed object now.
#[tokio::test]
async fn delete_pod_returns_object_json() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let body = serde_json::json!({"metadata": {"name": "byebye"}, "spec": { "containers": [ { "name": "c", "image": "busybox:1.36" } ] }});
    client
        .post(format!("http://{addr}/api/v1/namespaces/default/pods"))
        .json(&body)
        .send()
        .await
        .unwrap();

    let resp = client
        .delete(format!(
            "http://{addr}/api/v1/namespaces/default/pods/byebye"
        ))
        .header("Accept", "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let raw = resp.bytes().await.unwrap();
    assert!(!raw.is_empty(), "DELETE body must not be empty");
    let obj: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(
        obj.get("metadata").unwrap().get("name").unwrap(),
        "byebye",
        "DELETE returns the deleted object"
    );
    assert_eq!(obj.get("kind").unwrap(), "Pod");
    assert_eq!(obj.get("apiVersion").unwrap(), "v1");

    server.shutdown().await.unwrap();
}

/// DELETE of an existing apps/v1 Deployment with
/// `Accept: application/vnd.kubernetes.protobuf,application/json` returns
/// HTTP 200 with `Content-Type: application/vnd.kubernetes.protobuf` and a
/// non-empty body that round-trips through the kube-proto decoder back to
/// the deleted object. Proves the protobuf branch of `render_object` fires
/// for DELETE (the object GVK resolves cleanly in the proto pool).
#[tokio::test]
async fn delete_returns_object_protobuf() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let body = serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": "web" },
        "spec": {}
    });
    let created = client
        .post(format!(
            "http://{addr}/apis/apps/v1/namespaces/default/deployments"
        ))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), reqwest::StatusCode::CREATED);

    let resp = client
        .delete(format!(
            "http://{addr}/apis/apps/v1/namespaces/default/deployments/web"
        ))
        .header(
            "Accept",
            "application/vnd.kubernetes.protobuf,application/json",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("application/vnd.kubernetes.protobuf"),
        "DELETE honors the protobuf Accept for an existing object"
    );
    let raw = resp.bytes().await.unwrap();
    assert!(!raw.is_empty(), "protobuf DELETE body must not be empty");
    let decoded =
        engenho_kube_proto::decode_protobuf(&raw).expect("DELETE protobuf body round-trips");
    assert_eq!(
        decoded.get("metadata").unwrap().get("name").unwrap(),
        "web",
        "decoded protobuf is the deleted Deployment"
    );
    assert_eq!(decoded.get("kind").unwrap(), "Deployment");
    assert_eq!(decoded.get("apiVersion").unwrap(), "apps/v1");

    server.shutdown().await.unwrap();
}

/// DELETE of a never-created name returns HTTP 200 with a non-empty body
/// that parses to a `metav1.Status{status:"Success"}` (idempotent no-op,
/// matching the store's NoOp semantics). Proves the no-object branch is
/// typed and non-empty, not an empty 200 that crashes kubectl.
#[tokio::test]
async fn delete_absent_returns_status_success_json() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let resp = client
        .delete(format!(
            "http://{addr}/api/v1/namespaces/default/configmaps/ghost"
        ))
        .header("Accept", "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let raw = resp.bytes().await.unwrap();
    assert!(
        !raw.is_empty(),
        "absent-DELETE body must not be empty (empty crashes kubectl)"
    );
    let obj: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(obj.get("kind").unwrap(), "Status");
    assert_eq!(obj.get("status").unwrap(), "Success");
    assert_eq!(
        obj.get("details").unwrap().get("name").unwrap(),
        "ghost",
        "Status-Success names the target"
    );
    assert_eq!(
        obj.get("details").unwrap().get("kind").unwrap(),
        "ConfigMap"
    );

    server.shutdown().await.unwrap();
}

/// DELETE with a stale `?resourceVersion=` precondition still returns the
/// typed 409 Conflict — unchanged by the DELETE-body fix.
#[tokio::test]
async fn delete_with_stale_resource_version_conflicts() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let body = serde_json::json!({"metadata": {"name": "cas"}, "spec": { "containers": [ { "name": "c", "image": "busybox:1.36" } ] }});
    let created = client
        .post(format!("http://{addr}/api/v1/namespaces/default/pods"))
        .json(&body)
        .send()
        .await
        .unwrap();
    let created: serde_json::Value = created.json().await.unwrap();
    let rv: u64 = created
        .get("metadata")
        .unwrap()
        .get("resourceVersion")
        .unwrap()
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    // A resourceVersion that cannot match the live object's mod_revision
    // (well above it — no future delete will bump the pod's rv here).
    let stale = rv + 1000;

    let resp = client
        .delete(format!(
            "http://{addr}/api/v1/namespaces/default/pods/cas?resourceVersion={stale}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    let obj: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(obj.get("kind").unwrap(), "Status");
    assert_eq!(obj.get("reason").unwrap(), "Conflict");

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn create_conflict_when_pod_already_exists() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let body = serde_json::json!({"metadata": {"name": "p"}, "spec": { "containers": [ { "name": "c", "image": "busybox:1.36" } ] }});
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

    // `/openapi.json` serves the utoipa-derived description of engenho's OWN
    // REST surface (SDK/codegen consumers) — unchanged.
    let resp = client
        .get(format!("http://{addr}/openapi.json"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let spec: serde_json::Value = resp.json().await.unwrap();
    let v = spec.get("openapi").unwrap().as_str().unwrap();
    assert!(
        v.starts_with("3."),
        "/openapi.json returned non-v3 spec: {v}"
    );
    assert_eq!(
        spec.get("info").unwrap().get("title").unwrap(),
        "engenho-apiserver — K8s REST API"
    );
    let paths = spec.get("paths").unwrap().as_object().unwrap();
    assert!(paths.contains_key("/api/v1/namespaces/{ns}/{plural}/{name}"));
    assert!(paths.contains_key("/api/v1/namespaces/{ns}/{plural}"));
    assert!(paths.contains_key("/api/v1/{plural}/{name}"));
    assert!(paths.contains_key("/api/v1/{plural}"));
    let schemas = spec
        .get("components")
        .unwrap()
        .get("schemas")
        .unwrap()
        .as_object()
        .unwrap();
    for name in ["K8sResource", "K8sResourceList", "K8sStatus", "K8sPatch"] {
        assert!(schemas.contains_key(name), "missing schema {name}");
    }

    // `/openapi/v3` now serves the K8s OpenAPI-v3 DISCOVERY INDEX (what
    // kubectl apply --validate + explain consume), NOT the engenho-own
    // utoipa stub. It is a `{ paths: { <group-key>: { serverRelativeURL }}}`
    // index over exactly the cataloged groups.
    let resp = client
        .get(format!("http://{addr}/openapi/v3"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let idx: serde_json::Value = resp.json().await.unwrap();
    let idx_paths = idx.get("paths").unwrap().as_object().unwrap();
    for key in [
        "api/v1",
        "apis/apps/v1",
        "apis/rbac.authorization.k8s.io/v1",
    ] {
        let item = idx_paths
            .get(key)
            .unwrap_or_else(|| panic!("discovery index missing key {key}"));
        assert!(
            item.get("serverRelativeURL").is_some(),
            "index entry {key} has a serverRelativeURL"
        );
    }
    // It is the discovery index, NOT the utoipa stub.
    assert!(
        idx.get("info").is_none(),
        "/openapi/v3 is the K8s discovery index (no utoipa info block)"
    );

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

/// PUT on the MAIN object replaces it (the kubectl `replace`/update verb).
/// engenho used to reject this with a typed 400; it now does a real
/// optimistic-concurrency replace via the store's `ResourceCommand::Put`.
/// A body with no `resourceVersion` is an unconditional replace; the
/// server-owned `creationTimestamp` is preserved from the live object.
/// (This is the divergence engenho-diff caught against k3s and flipped to
/// parity.)
#[tokio::test]
async fn put_replaces_existing_pod_main_object() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let body = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "rp" },
        "spec": { "containers": [{"name": "a", "image": "img:1"}] }
    });
    let created = client
        .post(format!("http://{addr}/api/v1/namespaces/default/pods"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), reqwest::StatusCode::CREATED);
    let created: serde_json::Value = created.json().await.unwrap();
    let created_ts = created["metadata"]["creationTimestamp"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(!created_ts.is_empty());

    // PUT (replace) with a new image, no resourceVersion → unconditional.
    let put = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "rp" },
        "spec": { "containers": [{"name": "a", "image": "img:2"}] }
    });
    let resp = client
        .put(format!("http://{addr}/api/v1/namespaces/default/pods/rp"))
        .json(&put)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let obj: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(obj["kind"], "Pod");
    assert_eq!(obj["apiVersion"], "v1");
    assert_eq!(obj["spec"]["containers"][0]["image"], "img:2");
    // creationTimestamp is server-owned + preserved across the replace.
    assert_eq!(obj["metadata"]["creationTimestamp"], created_ts);
    // The stored object reflects the replace on a follow-up GET.
    let got = client
        .get(format!("http://{addr}/api/v1/namespaces/default/pods/rp"))
        .send()
        .await
        .unwrap();
    let got: serde_json::Value = got.json().await.unwrap();
    assert_eq!(got["spec"]["containers"][0]["image"], "img:2");

    server.shutdown().await.unwrap();
}

/// PUT (replace) of a never-created name is a typed 404 — `replace` requires
/// the object to already exist (unlike an apply, which upserts).
#[tokio::test]
async fn put_replace_absent_pod_is_404() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let put = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "ghost" },
        "spec": {}
    });
    let resp = client
        .put(format!(
            "http://{addr}/api/v1/namespaces/default/pods/ghost"
        ))
        .json(&put)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    let err: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(err["reason"], "NotFound");

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn deletecollection_configmaps_removes_all_and_returns_list() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();
    let base = format!("http://{addr}/api/v1/namespaces/default/configmaps");

    // Create three configmaps.
    for name in ["a", "b", "c"] {
        let body = serde_json::json!({"metadata": {"name": name}, "data": {"k": "v"}});
        let resp = client.post(&base).json(&body).send().await.unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    }

    // DELETE the collection (no object name) → deletecollection.
    let resp = client.delete(&base).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let list: serde_json::Value = resp.json().await.unwrap();
    // The wire contract is the <Kind>List of the deleted pre-images.
    assert_eq!(list.get("kind").unwrap(), "ConfigMapList");
    assert_eq!(list.get("apiVersion").unwrap(), "v1");
    let items = list.get("items").unwrap().as_array().unwrap();
    assert_eq!(items.len(), 3, "the three matched configmaps are returned");
    // List items are TypeMeta-less (the envelope carries the GVK).
    assert!(items[0].get("kind").is_none());
    assert!(items[0].get("apiVersion").is_none());

    // The collection is now empty.
    let resp = client.get(&base).send().await.unwrap();
    let after: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        after.get("items").unwrap().as_array().unwrap().len(),
        0,
        "deletecollection removed every object"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn deletecollection_on_namespaces_is_rejected() {
    // Namespaces have no CollectionDeleter — a collection-path DELETE is a
    // typed BadRequest, and discovery never advertises the verb for them.
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let resp = client
        .delete(format!("http://{addr}/api/v1/namespaces"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    server.shutdown().await.unwrap();
}
