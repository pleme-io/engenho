use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::params::DryRun;
use engenho_apiserver::{ObjectBody, ResourceHandler, StoreBackedHandler};
use engenho_store::{InProcessRouter, StoreMesh, default_config};
use engenho_types::auth::UserInfo;
use engenho_types::generated_v1_34::Subresource;
use serde_json::{Value, json};

const NS: &str = "default";
const GROUP: &str = "example.com";
const KIND: &str = "Environment";
const STATUS_ONLY: &[Subresource] = &[Subresource::Status];

async fn environments() -> StoreBackedHandler {
    let router = InProcessRouter::new();
    let cfg = default_config("status-put-replaces").expect("store config");
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .expect("store starts"),
    );
    store.initialize_singleton().await.expect("singleton init");
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    StoreBackedHandler::new(store, GROUP, "v1", KIND, "environments", true)
        .with_subresources(STATUS_ONLY)
}

fn environment(spec: Value, status: Option<Value>) -> Value {
    let mut obj = json!({
        "apiVersion": "example.com/v1",
        "kind": KIND,
        "metadata": {"name": "core1", "namespace": NS},
        "spec": spec,
    });
    if let Some(status) = status {
        obj["status"] = status;
    }
    obj
}

fn body(value: Value) -> ObjectBody {
    ObjectBody::normalize(GROUP, KIND, value).expect("a well-shaped body")
}

#[tokio::test]
async fn a_status_put_replaces_the_whole_status_and_a_main_put_drops_omitted_spec_fields() {
    let h = environments().await;
    h.create(
        Some(NS),
        body(environment(json!({"size": "s", "region": "r"}), None)),
        &UserInfo::default(),
        DryRun::Off,
    )
    .await
    .expect("environment created");

    h.put_status(
        Some(NS),
        "core1",
        environment(json!({}), Some(json!({"phase": "a", "held": ["x"]}))),
    )
    .await
    .expect("first status put");
    h.put_status(
        Some(NS),
        "core1",
        environment(json!({"size": "ignored"}), Some(json!({"phase": "b"}))),
    )
    .await
    .expect("second status put");

    let stored = h.get(Some(NS), "core1").await.expect("get");
    assert_eq!(
        stored["status"],
        json!({"phase": "b"}),
        "a /status PUT replaces .status: held must be gone"
    );
    assert_eq!(
        stored["spec"],
        json!({"size": "s", "region": "r"}),
        "a /status PUT ignores .spec"
    );

    h.put_status(Some(NS), "core1", environment(json!({}), None))
        .await
        .expect("status put with no status");
    let stored = h.get(Some(NS), "core1").await.expect("get");
    assert!(
        stored.get("status").is_none(),
        "a /status PUT with no .status clears it: {stored}"
    );

    h.put_status(
        Some(NS),
        "core1",
        environment(json!({}), Some(json!({"phase": "c", "held": ["y"]}))),
    )
    .await
    .expect("status put before main put");
    h.replace(
        Some(NS),
        "core1",
        body(environment(
            json!({"size": "m"}),
            Some(json!({"phase": "ignored"})),
        )),
        &UserInfo::default(),
        DryRun::Off,
    )
    .await
    .expect("main put");

    let stored = h.get(Some(NS), "core1").await.expect("get");
    assert_eq!(
        stored["spec"],
        json!({"size": "m"}),
        "a main PUT replaces .spec: region must be gone"
    );
    assert_eq!(
        stored["status"],
        json!({"phase": "c", "held": ["y"]}),
        "a main PUT keeps the stored .status"
    );
}
