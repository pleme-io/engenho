use std::sync::Arc;
use std::time::Duration;

use engenho_store::{
    DeletionGeneration, InProcessRouter, ResourceKey, ResourceOp, StoreMesh, WatchEventKind,
    WatchSignal,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::{Value, json};

async fn boot(rule: Option<DeletionGeneration>) -> Arc<StoreMesh> {
    let cfg = default_config("deletion-generation").unwrap();
    let mesh = StoreMesh::start(1, "in-process://1".into(), InProcessRouter::new(), cfg)
        .await
        .unwrap();
    let mesh = match rule {
        Some(rule) => mesh.with_deletion_generation(rule),
        None => mesh,
    };
    let store = Arc::new(mesh);
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

fn helm_chart() -> ResourceKey {
    ResourceKey::namespaced(
        "source.toolkit.fluxcd.io",
        "v1",
        "HelmChart",
        "flux-system",
        "podinfo",
    )
}

fn generation(v: &Value) -> i64 {
    v["metadata"]["generation"].as_i64().expect("generation")
}

async fn next_event(stream: &mut engenho_store::WatchStream) -> engenho_store::WatchEvent {
    loop {
        match tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("signal arrived")
        {
            Some(Ok(WatchSignal::Event(ev))) => return ev,
            Some(Ok(WatchSignal::Bookmark(_))) => {}
            Some(Err(g)) => panic!("unexpected WatchGone: {g:?}"),
            None => panic!("stream closed unexpectedly"),
        }
    }
}

async fn create_held_chart(store: &StoreMesh) -> i64 {
    store
        .propose(ResourceCommand::put(
            helm_chart(),
            json!({
                "metadata": {"finalizers": ["finalizers.fluxcd.io"]},
                "spec": {"chart": "podinfo", "interval": "1m"},
            }),
            Reason::Operator,
        ))
        .await
        .unwrap();
    generation(&store.get(&helm_chart()).await.expect("created"))
}

#[tokio::test]
async fn a_delete_held_by_a_finalizer_bumps_generation_and_a_watch_sees_it_modified() {
    let store = boot(None).await;
    let before = create_held_chart(&store).await;
    assert_eq!(before, 1);
    let mut watch = store.watch().await.unwrap();

    let out = store
        .propose(ResourceCommand::delete(helm_chart(), Reason::Operator))
        .await
        .unwrap();
    assert_eq!(out.op, ResourceOp::DeletionPending);

    let ev = next_event(&mut watch).await;
    assert_eq!(ev.kind, WatchEventKind::Modified);
    assert!(ev.object["metadata"]["deletionTimestamp"].is_string());
    assert!(
        generation(&ev.object) > before,
        "the Terminating MODIFIED event carries generation {} (was {before}); a GenerationChangedPredicate drops it",
        generation(&ev.object)
    );
    assert_eq!(generation(&ev.object), before + 1);

    let again = store
        .propose(ResourceCommand::delete(helm_chart(), Reason::Operator))
        .await
        .unwrap();
    assert_eq!(again.op, ResourceOp::NoOp);
    assert_eq!(
        generation(&store.get(&helm_chart()).await.unwrap()),
        before + 1
    );

    drop(watch);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn kept_deletion_generation_leaves_a_terminating_objects_generation_as_it_was() {
    let store = boot(Some(DeletionGeneration::Kept)).await;
    let before = create_held_chart(&store).await;

    let out = store
        .propose(ResourceCommand::delete(helm_chart(), Reason::Operator))
        .await
        .unwrap();
    assert_eq!(out.op, ResourceOp::DeletionPending);
    let live = store.get(&helm_chart()).await.unwrap();
    assert!(live["metadata"]["deletionTimestamp"].is_string());
    assert_eq!(generation(&live), before);

    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}
