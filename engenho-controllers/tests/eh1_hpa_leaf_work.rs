use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use engenho_controllers::{
    Controller, HorizontalPodAutoscalerController, PANGEA_LEAF_PENDING_WORK, StatusMetricsProvider,
};
use engenho_store::command::{Reason, ResourceCommand};
use engenho_store::{InProcessRouter, ResourceKey, StoreMesh, default_config};

async fn boot(name: &str) -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config(name).unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

async fn shutdown(store: Arc<StoreMesh>) {
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}

async fn put(store: &StoreMesh, key: &ResourceKey, value: Value) {
    store
        .propose(ResourceCommand::put(key.clone(), value, Reason::Operator))
        .await
        .unwrap();
}

fn fleet_status_key() -> ResourceKey {
    ResourceKey::cluster_scoped(
        "pangea.pleme.io",
        "v1alpha1",
        "PangeaFleetStatus",
        "default",
    )
}

async fn set_pending(store: &StoreMesh, pending: f64) {
    put(
        store,
        &fleet_status_key(),
        json!({
            "apiVersion": "pangea.pleme.io/v1alpha1", "kind": "PangeaFleetStatus",
            "metadata": {"name": "default"},
            "status": {"leafWork": {"pending": pending}}
        }),
    )
    .await;
}

fn workers(name: &str, replicas: i64) -> Value {
    json!({
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": name, "namespace": "pangea-system"},
        "spec": {
            "replicas": replicas,
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": {"app": name}},
                "spec": {"containers": [{"name": "worker", "image": "pangea-operator:1"}]}
            }
        }
    })
}

fn leaf_work_hpa(name: &str, target: &str, scale_down_window: Option<i64>) -> Value {
    let mut hpa = json!({
        "apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
        "metadata": {"name": name, "namespace": "pangea-system"},
        "spec": {
            "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": target},
            "minReplicas": 0, "maxReplicas": 4,
            "metrics": [{"type": "External", "external": {
                "metric": {"name": "pangea_leaf_pending_work"},
                "target": {"type": "AverageValue", "averageValue": "600"}
            }}]
        }
    });
    if let Some(window) = scale_down_window {
        hpa["spec"]["behavior"] = json!({"scaleDown": {"stabilizationWindowSeconds": window}});
    }
    hpa
}

fn deployment_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("apps", "v1", "Deployment", "pangea-system", name)
}

fn hpa_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced(
        "autoscaling",
        "v2",
        "HorizontalPodAutoscaler",
        "pangea-system",
        name,
    )
}

async fn replicas(store: &StoreMesh, name: &str) -> Value {
    store.get(&deployment_key(name)).await.unwrap()["spec"]["replicas"].clone()
}

#[tokio::test]
async fn leaf_work_scales_an_apps_v1_deployment_and_scale_down_waits_for_its_window() {
    let store = boot("eh1-hpa").await;
    put(&store, &deployment_key("held"), workers("held", 0)).await;
    put(&store, &deployment_key("quick"), workers("quick", 0)).await;
    put(
        &store,
        &hpa_key("held"),
        leaf_work_hpa("held", "held", None),
    )
    .await;
    put(
        &store,
        &hpa_key("quick"),
        leaf_work_hpa("quick", "quick", Some(0)),
    )
    .await;
    let metrics = Arc::new(StatusMetricsProvider::new(
        store.clone(),
        vec![PANGEA_LEAF_PENDING_WORK],
    ));
    let hpa = HorizontalPodAutoscalerController::new(store.clone(), metrics, None);

    hpa.tick().await.unwrap();
    assert_eq!(replicas(&store, "held").await, json!(0));
    assert_eq!(replicas(&store, "quick").await, json!(0));

    set_pending(&store, 1800.0).await;
    let out = hpa.tick().await.unwrap();
    assert_eq!(out.objects_changed, 2);
    assert_eq!(replicas(&store, "held").await, json!(3));
    assert_eq!(replicas(&store, "quick").await, json!(3));

    set_pending(&store, 60_000.0).await;
    hpa.tick().await.unwrap();
    assert_eq!(replicas(&store, "held").await, json!(4));

    set_pending(&store, 0.0).await;
    hpa.tick().await.unwrap();
    assert_eq!(replicas(&store, "held").await, json!(4));
    assert_eq!(replicas(&store, "quick").await, json!(0));

    drop(hpa);
    shutdown(store).await;
}

#[tokio::test]
async fn an_unreadable_leaf_work_holds_the_target() {
    let store = boot("eh1-hpa-unreadable").await;
    put(&store, &deployment_key("w"), workers("w", 2)).await;
    put(&store, &hpa_key("w"), leaf_work_hpa("w", "w", Some(0))).await;
    put(
        &store,
        &fleet_status_key(),
        json!({"metadata": {"name": "default"}, "status": {"leafWork": {"pending": "many"}}}),
    )
    .await;
    let metrics = Arc::new(StatusMetricsProvider::new(
        store.clone(),
        vec![PANGEA_LEAF_PENDING_WORK],
    ));
    let hpa = HorizontalPodAutoscalerController::new(store.clone(), metrics, None);
    let out = hpa.tick().await.unwrap();
    assert_eq!((out.objects_changed, out.objects_skipped), (0, 1));
    assert_eq!(replicas(&store, "w").await, json!(2));
    drop(hpa);
    shutdown(store).await;
}
