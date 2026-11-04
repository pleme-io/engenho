use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use engenho_controllers::{Controller, JobController, is_owned_by};
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

#[tokio::test]
async fn an_unscoped_controller_keys_a_job_pod_in_the_jobs_own_namespace() {
    let store = boot("job-pod-namespace").await;
    let key = ResourceKey::namespaced("batch", "v1", "Job", "flux-system", "token");
    store
        .propose(ResourceCommand::put(
            key.clone(),
            json!({
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {"name": "token", "namespace": "flux-system"},
                "spec": {"template": {"spec": {"containers": [{"name": "c", "image": "i"}]}}}
            }),
            Reason::Operator,
        ))
        .await
        .unwrap();
    let uid = store.get(&key).await.unwrap()["metadata"]["uid"]
        .as_str()
        .unwrap()
        .to_string();

    let controller = JobController::new(store.clone(), None);
    controller.tick().await.unwrap();
    controller.tick().await.unwrap();

    let at_home = store.list("", "v1", "Pod", Some("flux-system")).await;
    let owned: Vec<_> = at_home
        .iter()
        .filter(|(_, pod)| is_owned_by(pod, &uid))
        .collect();
    assert_eq!(owned.len(), 1, "{at_home:?}");
    assert_eq!(owned[0].0.name, "token-0");
    assert!(
        store
            .list("", "v1", "Pod", Some("default"))
            .await
            .is_empty()
    );

    drop(controller);
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}
