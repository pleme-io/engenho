//! M0.4 protobuf write-path codec — content negotiation at the HTTP
//! boundary.
//!
//! kubectl's typed clientset (`kubectl create configmap`, …) speaks
//! `application/vnd.kubernetes.protobuf` and treats a 415 as terminal
//! (no fall-back-to-JSON). This test proves the apiserver:
//!
//!   1. accepts a real magic-prefixed `runtime.Unknown`-framed ConfigMap
//!      protobuf body on POST → 201 Created, AND returns a protobuf body
//!      (Accept: protobuf) that decodes back to the created object;
//!   2. rejects a junk Content-Type with a proper K8s `Status` JSON body
//!      (reason "UnsupportedMediaType", code 415) — NOT axum's built-in
//!      plain-text JsonRejection;
//!   3. still serves the JSON path (Content-Type: application/json) →
//!      201, regression-proving the JSON-Value pipeline is untouched.

use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{ApiServer, handlers_from_catalog};
use engenho_kube_proto::{CONTENT_TYPE_PROTOBUF, Gvk, decode_protobuf, encode_response};
use engenho_store::{InProcessRouter, StoreMesh, default_config};
use serde_json::json;

async fn boot() -> (Arc<StoreMesh>, ApiServer) {
    let router = InProcessRouter::new();
    let cfg = default_config("apiserver-m04-proto").unwrap();
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

/// The EXACT 67 bytes captured live from kubectl v1.34.3
/// `create configmap demo --from-literal=hello=engenho`.
const GOLDEN_CONFIGMAP: &[u8] = &[
    0x6b, 0x38, 0x73, 0x00, 0x0a, 0x0f, 0x0a, 0x02, 0x76, 0x31, 0x12, 0x09, 0x43, 0x6f, 0x6e, 0x66,
    0x69, 0x67, 0x4d, 0x61, 0x70, 0x12, 0x28, 0x0a, 0x14, 0x0a, 0x04, 0x64, 0x65, 0x6d, 0x6f, 0x12,
    0x00, 0x1a, 0x00, 0x22, 0x00, 0x2a, 0x00, 0x32, 0x00, 0x38, 0x00, 0x42, 0x00, 0x12, 0x10, 0x0a,
    0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x12, 0x07, 0x65, 0x6e, 0x67, 0x65, 0x6e, 0x68, 0x6f, 0x1a,
    0x00, 0x22, 0x00,
];

#[tokio::test]
async fn protobuf_create_returns_201_and_protobuf_body() {
    let (_store, server) = boot().await;
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/api/v1/namespaces/default/configmaps"))
        .header("Content-Type", CONTENT_TYPE_PROTOBUF)
        // typed clientset Accept: protobuf first, json fallback.
        .header(
            "Accept",
            format!("{CONTENT_TYPE_PROTOBUF},application/json"),
        )
        .body(GOLDEN_CONFIGMAP.to_vec())
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        reqwest::StatusCode::CREATED,
        "protobuf POST must return 201 Created"
    );
    // The response Content-Type is protobuf (Accept honored).
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(
        ct.starts_with(CONTENT_TYPE_PROTOBUF),
        "Accept: protobuf must yield a protobuf response, got {ct:?}"
    );
    // Decode the protobuf response body back into JSON and verify.
    let body = resp.bytes().await.unwrap();
    let v = decode_protobuf(&body).expect("response decodes as protobuf");
    assert_eq!(v["metadata"]["name"], "demo");
    assert_eq!(v["data"]["hello"], "engenho");
    assert_eq!(v["kind"], "ConfigMap");
}

#[tokio::test]
async fn protobuf_create_then_json_get_reads_back() {
    let (_store, server) = boot().await;
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::new();

    // Create via protobuf.
    let resp = client
        .post(format!("{base}/api/v1/namespaces/default/configmaps"))
        .header("Content-Type", CONTENT_TYPE_PROTOBUF)
        .header("Accept", "application/json")
        .body(GOLDEN_CONFIGMAP.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    // Accept: json → JSON body even on a protobuf create.
    let created: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(created["data"]["hello"], "engenho");

    // GET it back as JSON.
    let got: serde_json::Value = client
        .get(format!("{base}/api/v1/namespaces/default/configmaps/demo"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(got["data"]["hello"], "engenho");
    assert_eq!(got["metadata"]["name"], "demo");
}

#[tokio::test]
async fn protobuf_create_then_protobuf_get_reads_back() {
    let (_store, server) = boot().await;
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::new();

    client
        .post(format!("{base}/api/v1/namespaces/default/configmaps"))
        .header("Content-Type", CONTENT_TYPE_PROTOBUF)
        .header("Accept", CONTENT_TYPE_PROTOBUF)
        .body(GOLDEN_CONFIGMAP.to_vec())
        .send()
        .await
        .unwrap();

    // GET with Accept: protobuf → protobuf body.
    let resp = client
        .get(format!("{base}/api/v1/namespaces/default/configmaps/demo"))
        .header("Accept", CONTENT_TYPE_PROTOBUF)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body = resp.bytes().await.unwrap();
    let v = decode_protobuf(&body).expect("GET protobuf body decodes");
    assert_eq!(v["data"]["hello"], "engenho");
}

#[tokio::test]
async fn junk_content_type_returns_415_k8s_status_json() {
    let (_store, server) = boot().await;
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/api/v1/namespaces/default/configmaps"))
        .header("Content-Type", "application/x-totally-bogus")
        .body(b"whatever".to_vec())
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "junk content-type must be 415"
    );
    // The body is a proper K8s Status JSON object — NOT axum's plain text.
    let body: serde_json::Value = resp
        .json()
        .await
        .expect("415 body must be JSON (a K8s Status), not plain text");
    assert_eq!(body["kind"], "Status");
    assert_eq!(body["apiVersion"], "v1");
    assert_eq!(body["status"], "Failure");
    assert_eq!(body["code"], 415);
    assert_eq!(
        body["reason"], "UnsupportedMediaType",
        "415 reason must be UnsupportedMediaType in the JSON body"
    );
}

#[tokio::test]
async fn json_create_still_works() {
    // Regression: the JSON-Value pipeline is untouched. A JSON POST (the
    // `kubectl create -f` / dynamic-client path) still returns 201.
    let (_store, server) = boot().await;
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::new();

    let body = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": "jcm" },
        "data": { "k": "v" }
    });
    let resp = client
        .post(format!("{base}/api/v1/namespaces/default/configmaps"))
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let created: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(created["data"]["k"], "v");
}

#[tokio::test]
async fn protobuf_deployment_create_round_trips() {
    // A non-core (apps/v1) kind through the grouped route + protobuf codec.
    let (_store, server) = boot().await;
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::new();

    let gvk = Gvk::new("apps/v1", "Deployment");
    let dep = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": "web" },
        "spec": { "replicas": 2 }
    });
    let bytes = encode_response(&gvk, &dep).expect("encode deployment protobuf");

    let resp = client
        .post(format!(
            "{base}/apis/apps/v1/namespaces/default/deployments"
        ))
        .header("Content-Type", CONTENT_TYPE_PROTOBUF)
        .header("Accept", CONTENT_TYPE_PROTOBUF)
        .body(bytes.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let body = resp.bytes().await.unwrap();
    let v = decode_protobuf(&body).expect("deployment response decodes");
    assert_eq!(v["metadata"]["name"], "web");
    assert_eq!(v["kind"], "Deployment");
}
