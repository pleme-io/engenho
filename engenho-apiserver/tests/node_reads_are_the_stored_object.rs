use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{ApiServer, ResourceHandler, StoreBackedHandler};
use engenho_controllers::node_lease::{lease_key, lease_value};
use engenho_store::command::{Reason, ResourceCommand};
use engenho_store::{InProcessRouter, ResourceKey, StoreMesh, default_config};
use serde_json::{Value, json};

async fn boot() -> (Arc<StoreMesh>, ApiServer) {
    let store = Arc::new(
        StoreMesh::start(
            1,
            "in-process://1".into(),
            InProcessRouter::new(),
            default_config("node-reads").unwrap(),
        )
        .await
        .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    let nodes: Arc<dyn ResourceHandler> = Arc::new(
        StoreBackedHandler::for_core_kind(store.clone(), "Node", false).expect("Node is cataloged"),
    );
    let server = ApiServer::start("127.0.0.1:0".parse().unwrap(), vec![nodes], None)
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

async fn get(client: &reqwest::Client, url: &str) -> Value {
    let resp = client.get(url).send().await.unwrap();
    assert!(resp.status().is_success(), "{url}: {}", resp.status());
    resp.json().await.unwrap()
}

fn ready_status(node: &Value) -> Value {
    node["status"]["conditions"]
        .as_array()
        .and_then(|cs| cs.iter().find(|c| c["type"] == "Ready"))
        .map(|c| c["status"].clone())
        .unwrap_or(Value::Null)
}

#[tokio::test]
async fn a_node_read_serves_the_stored_object_whatever_its_lease_says() {
    let (store, server) = boot().await;
    let node_key = ResourceKey::cluster_scoped("", "v1", "Node", "node-A");
    put(
        &store,
        node_key.clone(),
        json!({
            "kind": "Node",
            "apiVersion": "v1",
            "metadata": { "name": "node-A" },
            "status": { "conditions": [{
                "type": "Ready", "status": "True", "reason": "KubeletReady",
                "lastHeartbeatTime": "2026-10-15T12:00:00Z",
                "lastTransitionTime": "2026-10-15T12:00:00Z"
            }]}
        }),
    )
    .await;
    put(
        &store,
        lease_key("node-A"),
        lease_value("node-A", "2020-01-01T00:00:00Z", 0),
    )
    .await;

    let addr = server.local_addr();
    let client = reqwest::Client::new();
    let one = format!("http://{addr}/api/v1/nodes/node-A");
    let all = format!("http://{addr}/api/v1/nodes");
    let stored = store.get(&node_key).await.unwrap();

    let first = get(&client, &one).await;
    let listed = get(&client, &all).await;
    assert_eq!(
        first["metadata"]["resourceVersion"],
        stored["metadata"]["resourceVersion"]
    );
    assert_eq!(
        ready_status(&first),
        json!("True"),
        "a GET serves the stored condition, not one derived at read time: {first}"
    );
    assert_eq!(
        ready_status(&listed["items"][0]),
        json!("True"),
        "a LIST serves the stored condition too: {listed}"
    );

    put(
        &store,
        lease_key("node-A"),
        lease_value("node-A", &engenho_types::time::now_rfc3339_utc(), 1),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let second = get(&client, &one).await;
    assert_eq!(
        second["metadata"]["resourceVersion"], first["metadata"]["resourceVersion"],
        "nothing wrote the Node"
    );
    assert_eq!(
        second, first,
        "one resourceVersion, one content: a watch cache keyed on the revision \
         would never see the difference"
    );
}
