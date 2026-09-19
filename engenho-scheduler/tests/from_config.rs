//! T5.8: every `SchedulerConfig` field reaches the running scheduler.
//!
//! Before T5.8, `scheduler.namespace` and `scheduler.tick_interval_seconds`
//! were validated at boot and read by nothing: the runtime scopes the
//! scheduler by `controllers.namespace` and drives it on the controllers'
//! fallback tick, and does so until it builds the scheduler through
//! `Scheduler::from_config`. These tests build it that way and check each
//! field's effect against a real `StoreMesh`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use engenho_config::{SchedulerConfig, SchedulerStrategyKind};
use engenho_controllers::{KindFilter, WatchDriverConfig};
use engenho_scheduler::{RoundRobinStrategy, Scheduler, SchedulerError};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::{Value, json};

async fn boot_store() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("scheduler-from-config").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

/// Terminate the mesh when this test holds its last handle. A driver task
/// that was aborted may still be dropping its clone; the process exit
/// reclaims the mesh in that case.
async fn teardown(store: Arc<StoreMesh>) {
    if let Ok(mesh) = Arc::try_unwrap(store) {
        mesh.terminate().await.unwrap();
    }
}

fn config(namespace: &str, tick_interval_seconds: u32) -> SchedulerConfig {
    SchedulerConfig {
        strategy: SchedulerStrategyKind::RoundRobin,
        namespace: namespace.to_owned(),
        tick_interval_seconds,
    }
}

/// A heartbeating, sized, Ready node.
async fn put_node(store: &StoreMesh, name: &str) {
    common::put_fresh_lease(store, name).await;
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::cluster_scoped("", "v1", "Node", name),
            value: json!({
                "kind": "Node",
                "apiVersion": "v1",
                "metadata": { "name": name },
                "spec": { "unschedulable": false },
                "status": {
                    "capacity": { "cpu": "4", "memory": "8Gi" },
                    "allocatable": { "cpu": "4", "memory": "8Gi" },
                    "conditions": [{ "type": "Ready", "status": "True" }]
                }
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

fn pod_key(namespace: &str, name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", namespace, name)
}

async fn put_pending_pod(store: &StoreMesh, namespace: &str, name: &str) {
    store
        .propose(ResourceCommand::Put {
            key: pod_key(namespace, name),
            value: json!({
                "kind": "Pod",
                "apiVersion": "v1",
                "metadata": { "name": name, "namespace": namespace },
                "spec": { "containers": [{ "name": "main", "image": "podinfo:6" }] }
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

async fn get_pod(store: &StoreMesh, namespace: &str, name: &str) -> Value {
    store
        .get(&pod_key(namespace, name))
        .await
        .expect("pod exists")
}

fn bound_node(pod: &Value) -> Option<&str> {
    pod.pointer("/spec/nodeName")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

/// One node, and one pending pod in each of `team-a` and `team-b`.
async fn two_namespaces() -> Arc<StoreMesh> {
    let store = boot_store().await;
    put_node(&store, "node-1").await;
    put_pending_pod(&store, "team-a", "pa").await;
    put_pending_pod(&store, "team-b", "pb").await;
    store
}

#[tokio::test]
async fn the_configured_namespace_is_the_only_one_scheduled() {
    let store = two_namespaces().await;

    let configured = Scheduler::from_config(store.clone(), &config("team-a", 5)).unwrap();
    let report = configured.scheduler().tick().await.unwrap();
    drop(configured);

    assert_eq!(report.pending_pods, 1, "only team-a's pod is in scope");
    assert_eq!(
        bound_node(&get_pod(&store, "team-a", "pa").await),
        Some("node-1")
    );
    // Out of scope means untouched: no binding and no condition.
    let other = get_pod(&store, "team-b", "pb").await;
    assert_eq!(bound_node(&other), None, "team-b is out of scope");
    assert!(
        other.pointer("/status/conditions").is_none(),
        "nothing is written to an out-of-scope pod: {other}"
    );

    teardown(store).await;
}

#[tokio::test]
async fn an_empty_configured_namespace_schedules_every_namespace() {
    let store = two_namespaces().await;

    let configured = Scheduler::from_config(store.clone(), &config("", 5)).unwrap();
    let report = configured.scheduler().tick().await.unwrap();
    drop(configured);

    assert_eq!(report.bound.len(), 2, "both namespaces are in scope");
    for (namespace, name) in [("team-a", "pa"), ("team-b", "pb")] {
        assert_eq!(
            bound_node(&get_pod(&store, namespace, name).await),
            Some("node-1"),
            "{namespace}/{name}"
        );
    }

    teardown(store).await;
}

#[tokio::test]
async fn a_scheduler_given_an_empty_namespace_is_not_scoped_to_nothing() {
    // `Some("")` used to reach the store as a filter for the namespace
    // named "", which holds no pod: every tick succeeded and placed nothing.
    let store = two_namespaces().await;

    let sched = Scheduler::new(
        store.clone(),
        RoundRobinStrategy::new(),
        Some(String::new()),
    );
    let report = sched.tick().await.unwrap();
    drop(sched);

    assert_eq!(report.pending_pods, 2);
    assert_eq!(report.bound.len(), 2);

    teardown(store).await;
}

#[tokio::test]
async fn the_configured_tick_is_the_fallback_interval() {
    let store = boot_store().await;
    let configured = Scheduler::from_config(store.clone(), &config("", 7)).unwrap();
    assert_eq!(configured.fallback_interval(), Duration::from_secs(7));
    drop(configured);
    teardown(store).await;
}

#[tokio::test]
async fn a_zero_tick_interval_is_refused() {
    let store = boot_store().await;
    match Scheduler::from_config(store.clone(), &config("", 0)) {
        Err(SchedulerError::ZeroTickInterval) => {}
        Err(other) => panic!("expected ZeroTickInterval, got {other:?}"),
        Ok(c) => panic!(
            "a zero tick must be refused, got a fallback of {:?}",
            c.fallback_interval()
        ),
    }
    teardown(store).await;
}

#[tokio::test]
async fn an_unimplemented_strategy_is_refused() {
    let store = boot_store().await;
    let cfg = SchedulerConfig {
        strategy: SchedulerStrategyKind::BinPack,
        ..config("", 5)
    };
    match Scheduler::from_config(store.clone(), &cfg) {
        Err(SchedulerError::UnsupportedStrategy { requested }) => {
            assert_eq!(requested, SchedulerStrategyKind::BinPack);
        }
        Err(other) => panic!("expected UnsupportedStrategy, got {other:?}"),
        Ok(_) => panic!("BinPack must be refused, never run as round-robin"),
    }
    teardown(store).await;
}

#[tokio::test]
async fn the_driver_ticks_on_the_configured_interval() {
    // The base driver config wakes on no event and falls back once an
    // hour. Only the configured 1s fallback can bind this pod in time.
    let store = boot_store().await;
    put_node(&store, "node-1").await;
    put_pending_pod(&store, "default", "p").await;

    let configured = Scheduler::from_config(store.clone(), &config("", 1)).unwrap();
    let base = WatchDriverConfig {
        filter: KindFilter::Kinds(Vec::new()),
        fallback_interval: Duration::from_secs(3600),
        ..WatchDriverConfig::default()
    };
    // T2.6: a driver is a future that never returns (`run -> Infallible`);
    // the caller owns the task.
    let handle = tokio::spawn(configured.into_watch_driver(base).run());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut landed = None;
    while tokio::time::Instant::now() < deadline {
        landed = bound_node(&get_pod(&store, "default", "p").await).map(str::to_owned);
        if landed.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    handle.abort();
    let _ = handle.await;

    assert_eq!(
        landed.as_deref(),
        Some("node-1"),
        "the configured 1s fallback tick never ran the scheduler"
    );

    teardown(store).await;
}
