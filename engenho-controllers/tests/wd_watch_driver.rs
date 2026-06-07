//! WatchDriver integration — proves event-driven reconcile actually
//! fires when a Put hits the store.

use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::{
    Controller, ControllerError, KindFilter, ReconcileOutcome, ReconcileReport, WatchDriver,
    WatchDriverConfig,
};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::json;
use tokio::sync::Mutex;

/// Counter controller — records how many ticks it received.
#[derive(Default)]
struct TickCounter {
    count: Mutex<usize>,
}

#[async_trait::async_trait]
impl Controller for TickCounter {
    fn name(&self) -> &'static str {
        "tick_counter"
    }
    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let mut c = self.count.lock().await;
        *c += 1;
        Ok(ReconcileReport::default().into())
    }
}

async fn boot() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("wd-test").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

#[tokio::test]
async fn driver_ticks_on_matching_event() {
    let store = boot().await;
    let counter = Arc::new(TickCounter::default());

    let driver = WatchDriver::new(
        // For the trait dispatch to find tick_counter, we wrap a clone-able Arc.
        TickCounterRef {
            inner: counter.clone(),
        },
        store.clone(),
        WatchDriverConfig {
            filter: KindFilter::kind("Pod"),
            debounce: Duration::from_millis(20),
            fallback_interval: Duration::from_secs(3600),
        },
    );
    let handle = driver.spawn();

    // Brief wait for the driver to subscribe.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Fire a Pod event.
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::namespaced("", "v1", "Pod", "default", "p1"),
            value: json!({"spec": {}}),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();

    // Wait long enough for debounce + tick.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let count = *counter.count.lock().await;
    assert!(count >= 1, "expected at least 1 tick; got {count}");
    handle.abort();
    let _ = handle.await; // ensure the task has fully dropped its Arc clone
    // Best-effort terminate — if the abort race left an extra clone
    // somewhere, the test's tokio runtime will drop everything when
    // it shuts down anyway.
    if let Ok(mesh) = Arc::try_unwrap(store) {
        let _ = mesh.terminate().await;
    }
}

#[tokio::test]
async fn driver_filter_skips_irrelevant_events() {
    let store = boot().await;
    let counter = Arc::new(TickCounter::default());

    let driver = WatchDriver::new(
        TickCounterRef {
            inner: counter.clone(),
        },
        store.clone(),
        WatchDriverConfig {
            filter: KindFilter::kind("Pod"), // only Pods
            debounce: Duration::from_millis(20),
            fallback_interval: Duration::from_secs(3600),
        },
    );
    let handle = driver.spawn();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Fire a NODE event — should NOT wake the Pod-filtered driver.
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::cluster_scoped("", "v1", "Node", "node-1"),
            value: json!({"spec": {}}),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        *counter.count.lock().await,
        0,
        "Pod-filtered driver should NOT tick on Node events"
    );
    handle.abort();
    let _ = handle.await; // ensure the task has fully dropped its Arc clone
    // Best-effort terminate — if the abort race left an extra clone
    // somewhere, the test's tokio runtime will drop everything when
    // it shuts down anyway.
    if let Ok(mesh) = Arc::try_unwrap(store) {
        let _ = mesh.terminate().await;
    }
}

#[tokio::test]
async fn driver_coalesces_burst_events_into_one_tick() {
    let store = boot().await;
    let counter = Arc::new(TickCounter::default());

    let driver = WatchDriver::new(
        TickCounterRef {
            inner: counter.clone(),
        },
        store.clone(),
        WatchDriverConfig {
            filter: KindFilter::All,
            debounce: Duration::from_millis(100),
            fallback_interval: Duration::from_secs(3600),
        },
    );
    let handle = driver.spawn();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Fire 5 events in a tight burst.
    for i in 0..5 {
        store
            .propose(ResourceCommand::Put {
                key: ResourceKey::namespaced("", "v1", "Pod", "default", &format!("p{i}")),
                value: json!({"spec": {}}),
                expected: None,
                reason: Reason::Operator,
            })
            .await
            .unwrap();
    }

    // Allow debounce window + tick.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Coalescing means we should see fewer ticks than events. In
    // practice with a 100ms window + Raft commit pacing, we expect
    // typically 1-2 ticks for 5 events.
    let count = *counter.count.lock().await;
    assert!(
        (1..=5).contains(&count),
        "expected 1-5 coalesced ticks for 5 events; got {count}"
    );
    assert!(
        count < 5,
        "expected coalescing; got {count} ticks for 5 events"
    );

    handle.abort();
    let _ = handle.await;
    if let Ok(mesh) = Arc::try_unwrap(store) {
        let _ = mesh.terminate().await;
    }
}

#[tokio::test]
async fn driver_fallback_timer_ticks_with_no_events() {
    let store = boot().await;
    let counter = Arc::new(TickCounter::default());

    let driver = WatchDriver::new(
        TickCounterRef {
            inner: counter.clone(),
        },
        store.clone(),
        WatchDriverConfig {
            filter: KindFilter::All,
            debounce: Duration::from_millis(20),
            fallback_interval: Duration::from_millis(150),
        },
    );
    let handle = driver.spawn();

    // No events; just wait for the fallback timer to fire twice.
    tokio::time::sleep(Duration::from_millis(400)).await;

    let count = *counter.count.lock().await;
    assert!(
        count >= 2,
        "fallback timer should have fired ≥2 times; got {count}"
    );

    handle.abort();
    let _ = handle.await;
    if let Ok(mesh) = Arc::try_unwrap(store) {
        let _ = mesh.terminate().await;
    }
}

/// Newtype wrapper so the driver can clone the controller across tasks
/// while keeping the shared Mutex-protected counter alive.
struct TickCounterRef {
    inner: Arc<TickCounter>,
}

#[async_trait::async_trait]
impl Controller for TickCounterRef {
    fn name(&self) -> &'static str {
        "tick_counter"
    }
    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        self.inner.tick().await
    }
}
