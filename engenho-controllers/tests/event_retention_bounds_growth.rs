use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::{Controller, GcController};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::json;

async fn boot_store() -> Arc<StoreMesh> {
    let cfg = default_config("event-retention").expect("config");
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), InProcessRouter::new(), cfg)
            .await
            .expect("store"),
    );
    store.initialize_singleton().await.expect("init");
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

async fn put_event(store: &StoreMesh, name: &str, age: chrono::Duration) {
    let stamp = engenho_types::time::to_rfc3339_utc(chrono::Utc::now() - age);
    let key = ResourceKey::namespaced("", "v1", "Event", "default", name);
    let value = json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {"name": name, "namespace": "default"},
        "involvedObject": {"kind": "Pod", "namespace": "default", "name": "p"},
        "reason": "BackOff",
        "firstTimestamp": stamp,
        "lastTimestamp": stamp,
        "count": 1,
    });
    store
        .propose(ResourceCommand::put(key, value, Reason::Controller))
        .await
        .expect("put event");
}

#[tokio::test]
async fn gc_holds_the_event_count_to_what_the_ttl_admits() {
    let store = boot_store().await;
    let gc = GcController::new(store.clone(), None);
    let fresh_per_round = 50;
    let stale_per_round = 400;
    for round in 0..6 {
        for i in 0..stale_per_round {
            put_event(
                &store,
                &format!("stale-{round}-{i}"),
                chrono::Duration::hours(2),
            )
            .await;
        }
        for i in 0..fresh_per_round {
            put_event(
                &store,
                &format!("fresh-{round}-{i}"),
                chrono::Duration::minutes(5),
            )
            .await;
        }
        gc.tick().await.expect("gc tick");
        let events = store.list("", "v1", "Event", None).await;
        let stale = events
            .iter()
            .filter(|(k, _)| k.name.starts_with("stale-"))
            .count();
        assert_eq!(
            stale, 0,
            "round {round}: events past the ttl survived a gc tick"
        );
        assert_eq!(
            events.len(),
            fresh_per_round * (round + 1),
            "round {round}: an event inside the ttl was collected"
        );
    }
}
