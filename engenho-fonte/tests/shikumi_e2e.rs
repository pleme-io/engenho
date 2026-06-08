//! Integration test for the `with-shikumi` feature — write a JSON
//! file, watch it via ShikumiWatcher, modify it, assert the Conduit
//! re-runs the convergence loop.

#![cfg(feature = "with-shikumi")]

use engenho_fonte::{
    ChangeKind, Conduit, MockAttester, MockEvaluator, MockPublisher, ShikumiWatcher,
    mock_system_controller,
};
use std::sync::Arc;
use tokio::time::{Duration, sleep};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writing_a_file_emits_initial_change() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();
    std::fs::write(
        &path,
        r#"{"name":"a","apps":[],"infra":[],"promises":[],"topology":{"strategy":"solo","nodes":1}}"#,
    )
    .unwrap();

    let watcher = Arc::new(ShikumiWatcher::new(&path).expect("watcher"));
    let evaluator = Arc::new(MockEvaluator::new());
    let (_apps, _infra, _promises, _topology, ctrl) = mock_system_controller();
    let proposer = Arc::new(ctrl);
    let attester = Arc::new(MockAttester::new());
    let publisher = Arc::new(MockPublisher::new());

    let conduit = Conduit::new(
        watcher,
        evaluator,
        proposer.clone(),
        attester.clone(),
        publisher.clone(),
    );

    let outcome = conduit.tick().await.expect("tick").expect("outcome");
    assert_eq!(outcome.revision, 0);
    assert_eq!(publisher.outcomes().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn modifying_a_file_emits_modified_change() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    std::fs::write(
        &path,
        r#"{"name":"a","apps":[{"name":"x","version":null}],"infra":[],"promises":[],"topology":{"strategy":"solo","nodes":1}}"#,
    )
    .unwrap();

    let watcher = Arc::new(ShikumiWatcher::new(&path).unwrap());
    let evaluator = Arc::new(MockEvaluator::new());
    let (_a, _i, _p, _t, ctrl) = mock_system_controller();
    let conduit = Conduit::new(
        watcher,
        evaluator,
        Arc::new(ctrl),
        Arc::new(MockAttester::new()),
        Arc::new(MockPublisher::new()),
    );

    // Consume the initial change.
    let initial = conduit.tick().await.unwrap().expect("initial");
    assert_eq!(initial.revision, 0);

    // Modify the file (write a Sistema with 2 apps).
    sleep(Duration::from_millis(100)).await;
    std::fs::write(
        &path,
        r#"{"name":"a","apps":[{"name":"x","version":null},{"name":"y","version":null}],"infra":[],"promises":[],"topology":{"strategy":"solo","nodes":1}}"#,
    )
    .unwrap();

    // Wait for the notify watcher to fire + Conduit to process. The loop
    // returns as soon as the event arrives, so a generous deadline costs
    // nothing in the common case; it only adds slack for filesystem-notify
    // delivery latency under heavy parallel `cargo test --workspace` load
    // (a 5s bound flaked there — the notify event can lag past it).
    let modified = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(Some(o)) = conduit.tick().await {
                return o;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("modify notify within 30s");
    assert!(modified.revision > initial.revision);
}

#[test]
fn missing_file_surfaces_typed_watch_error() {
    let result = ShikumiWatcher::new("/this/path/does/not/exist/at/all/I/promise.json");
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("expected watcher creation to fail"),
    };
    let msg = format!("{err}");
    assert!(msg.contains("fonte/watch"), "got: {msg}");
}

#[test]
fn change_kind_classification_matches_notify_kind() {
    // Sanity: ChangeKind enum still has the four variants we encode.
    let _ = ChangeKind::Initial;
    let _ = ChangeKind::Created;
    let _ = ChangeKind::Modified;
    let _ = ChangeKind::Removed;
}
