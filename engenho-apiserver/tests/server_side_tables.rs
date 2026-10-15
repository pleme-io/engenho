use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{ApiServer, ResourceHandler, StoreBackedHandler};
use engenho_store::command::{Reason, ResourceCommand};
use engenho_store::{InProcessRouter, ResourceKey, StoreMesh, default_config};
use serde_json::{Value, json};

const TABLE: &str = "application/json;as=Table;v=v1;g=meta.k8s.io,application/json";

async fn boot() -> (Arc<StoreMesh>, ApiServer) {
    let store = Arc::new(
        StoreMesh::start(
            1,
            "in-process://1".into(),
            InProcessRouter::new(),
            default_config("server-side-tables").unwrap(),
        )
        .await
        .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    let handlers: Vec<Arc<dyn ResourceHandler>> = vec![
        Arc::new(StoreBackedHandler::for_core_kind(store.clone(), "Node", false).unwrap()),
        Arc::new(StoreBackedHandler::for_core_kind(store.clone(), "Pod", true).unwrap()),
        Arc::new(
            StoreBackedHandler::new(
                store.clone(),
                "kustomize.toolkit.fluxcd.io",
                "v1",
                "Kustomization",
                "kustomizations",
                true,
            )
            .with_printer_columns(&[
                json!({"name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp"}),
                json!({"name": "Ready", "type": "string", "jsonPath": ".status.conditions[?(@.type==\"Ready\")].status"}),
            ]),
        ),
    ];
    let server = ApiServer::start("127.0.0.1:0".parse().unwrap(), handlers, None)
        .await
        .unwrap();
    (store, server)
}

async fn put(store: &StoreMesh, key: ResourceKey, value: Value) {
    store
        .propose(ResourceCommand::Put {
            key,
            value,
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

async fn table(server: &ApiServer, path: &str) -> Value {
    let resp = reqwest::Client::new()
        .get(format!("http://{}{path}", server.local_addr()))
        .header(reqwest::header::ACCEPT, TABLE)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "{path}: {}", resp.status());
    resp.json().await.unwrap()
}

fn names(t: &Value) -> Vec<&str> {
    t["columnDefinitions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect()
}

fn days_ago(days: i64) -> String {
    (chrono::Utc::now() - chrono::Duration::days(days))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

#[tokio::test]
async fn kubectl_get_prints_upstreams_columns_with_ages_rendered() {
    let (store, server) = boot().await;
    put(
        &store,
        ResourceKey::cluster_scoped("", "v1", "Node", "ryn"),
        json!({
            "metadata": {"name": "ryn", "creationTimestamp": days_ago(44)},
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}],
                "nodeInfo": {"kubeletVersion": "v1.34.0+engenho.0.53.133"}
            }
        }),
    )
    .await;
    put(
        &store,
        ResourceKey::namespaced("", "v1", "Pod", "default", "web"),
        json!({
            "metadata": {"name": "web", "namespace": "default", "creationTimestamp": days_ago(3)},
            "spec": {"nodeName": "ryn", "containers": [{"name": "c", "image": "i"}]},
            "status": {"phase": "Running", "podIPs": [{"ip": "10.42.0.9"}],
                "containerStatuses": [{"name": "c", "ready": true, "restartCount": 0, "state": {"running": {}}}]}
        }),
    )
    .await;
    put(
        &store,
        ResourceKey::namespaced("kustomize.toolkit.fluxcd.io", "v1", "Kustomization", "flux-system", "infra"),
        json!({
            "metadata": {"name": "infra", "namespace": "flux-system", "creationTimestamp": days_ago(8)},
            "status": {"conditions": [{"type": "Ready", "status": "True"}]}
        }),
    )
    .await;

    let nodes = table(&server, "/api/v1/nodes").await;
    assert_eq!(
        names(&nodes)[..5],
        ["Name", "Status", "Roles", "Age", "Version"],
        "{nodes}"
    );
    assert_eq!(
        nodes["rows"][0]["cells"].as_array().unwrap()[..5],
        [
            json!("ryn"),
            json!("Ready"),
            json!("<none>"),
            json!("44d"),
            json!("v1.34.0+engenho.0.53.133")
        ],
        "{nodes}"
    );

    let pods = table(&server, "/api/v1/namespaces/default/pods").await;
    assert_eq!(
        names(&pods)[..7],
        ["Name", "Ready", "Status", "Restarts", "Age", "IP", "Node"],
        "{pods}"
    );
    assert_eq!(
        pods["rows"][0]["cells"].as_array().unwrap()[..7],
        [
            json!("web"),
            json!("1/1"),
            json!("Running"),
            json!("0"),
            json!("3d"),
            json!("10.42.0.9"),
            json!("ryn")
        ],
        "{pods}"
    );

    let one = table(&server, "/api/v1/namespaces/default/pods/web").await;
    assert_eq!(one["rows"].as_array().unwrap().len(), 1);
    assert_eq!(one["rows"][0]["cells"][4], json!("3d"));

    let ks = table(
        &server,
        "/apis/kustomize.toolkit.fluxcd.io/v1/namespaces/flux-system/kustomizations",
    )
    .await;
    assert_eq!(names(&ks), ["Name", "Age", "Ready"], "{ks}");
    assert_eq!(
        ks["rows"][0]["cells"],
        json!(["infra", "8d", "True"]),
        "{ks}"
    );
}
