//! THE KUBELET HTTP SURFACE, DRIVEN THROUGH THE REAL KUBELET.
//!
//! ★ WHY THIS IS SEPARATE FROM `server.rs`'s own tests. Those drive a
//! `FakeApi` defined in the same module, so they prove the ROUTER. They
//! were green the entire time `KubeletApi` had no production implementor
//! at all — the surface existed as a type and not as a port. This file
//! drives `impl KubeletApi for Kubelet`, so it fails if that impl is
//! removed, and it is the only thing that would have noticed.
//!
//! Invariants:
//!   P1 /containerLogs returns the real container's stdout
//!   P2 an unknown container is a typed refusal naming all three parts,
//!      never an empty 200
//!   P3 /pods lists what this kubelet actually manages
//!   P4 /exec runs argv in the real container and frames the output

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use engenho_controllers::Controller;
use engenho_kubelet::server::{KubeletApi, KubeletServer};
use engenho_kubelet::{FakeBackend, Kubelet};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::json;
use tower::ServiceExt;

async fn boot(name: &str) -> (Arc<StoreMesh>, Arc<FakeBackend>, Arc<Kubelet>) {
    let router = InProcessRouter::new();
    let cfg = default_config(name).unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Arc::new(Kubelet::new(store.clone(), backend.clone(), "node-A"));
    (store, backend, kubelet)
}

async fn put_pod(store: &StoreMesh, name: &str) {
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::namespaced("", "v1", "Pod", "default", name),
            value: json!({
                "kind": "Pod",
                "apiVersion": "v1",
                "metadata": { "name": name },
                "spec": {
                    "nodeName": "node-A",
                    "restartPolicy": "Always",
                    "containers": [ { "name": "app", "image": "busybox" } ]
                }
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

async fn get(app: axum::Router, uri: &str) -> (StatusCode, String) {
    let res = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

#[tokio::test]
async fn p1_p2_p3_the_real_kubelet_serves_logs_and_pods() {
    let (store, backend, kubelet) = boot("http-producer").await;
    put_pod(&store, "web").await;
    // Seeded BEFORE the tick: the fake consumes the seed at container
    // START, so seeding afterwards silently leaves the default in place —
    // which is a test that asserts nothing about the resolver.
    //
    // The deterministic backend name is the KUBELET's (`<ns>_<pod>_<cname>`),
    // not the test's, so a resolver that found some other container — or
    // none — could not produce this content.
    backend
        .seed_log("default_web_app", "line one\nline two\n")
        .await;
    kubelet.tick().await.unwrap();

    let api: Arc<dyn KubeletApi> = kubelet.clone();
    let app = {
        let api = api.clone();
        move || KubeletServer::new(api.clone()).routes()
    };

    // P1
    let (status, body) = get(app(), "/containerLogs/default/web/app").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("line one"), "real container stdout: {body}");

    // P2 — a container this kubelet does not run. The message must name
    // all three parts: on a multi-node cluster the overwhelmingly common
    // cause is asking the WRONG kubelet, and a bare "not found" sends the
    // operator hunting a deleted pod.
    let (status, body) = get(app(), "/containerLogs/default/web/sidecar").await;
    assert_ne!(status, StatusCode::OK, "an unknown container is not a 200");
    assert!(body.contains("sidecar") && body.contains("web"), "{body}");
    assert!(body.contains("this node"), "names the node scope: {body}");

    // P3
    let (status, body) = get(app(), "/pods").await;
    assert_eq!(status, StatusCode::OK);
    let list: serde_json::Value = serde_json::from_str(&body).expect("PodList JSON");
    assert_eq!(list["kind"], "PodList", "{list}");
    let names: Vec<&str> = list["items"]
        .as_array()
        .unwrap_or_else(|| panic!("items is an array: {list}"))
        .iter()
        .filter_map(|p| p["metadata"]["name"].as_str())
        .collect();
    assert_eq!(names, vec!["web"], "lists what this kubelet manages");

    // Every Arc<Kubelet> clone must go before the mesh can be unwrapped —
    // the trait object holds one too.
    drop(app);
    drop(api);
    drop(kubelet);
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}

#[tokio::test]
async fn p4_exec_reaches_the_real_container() {
    let (store, backend, kubelet) = boot("http-exec").await;
    put_pod(&store, "web").await;
    kubelet.tick().await.unwrap();

    backend
        .set_default_exec(engenho_kubelet::ExecOutcome {
            exit_code: 0,
            stdout: "it ran\n".into(),
            stderr: String::new(),
        })
        .await;

    let out = KubeletApi::exec(
        kubelet.as_ref(),
        "default",
        "web",
        "app",
        &["echo".to_string(), "hi".to_string()],
    )
    .await
    .expect("exec reached the container");
    assert_eq!(out.stdout, "it ran\n");
    assert_eq!(out.exit_code, 0);

    // And the same typed refusal for an unknown container — exec must not
    // silently run somewhere else.
    let err = KubeletApi::exec(
        kubelet.as_ref(),
        "default",
        "web",
        "nope",
        &["echo".to_string()],
    )
    .await
    .expect_err("unknown container is refused");
    assert!(err.contains("nope"), "{err}");

    drop(kubelet);
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}
