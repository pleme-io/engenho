use std::time::{Duration, Instant};

use crate::data_dir_lock::DataDirLock;
use crate::mesh::{StoreBackend, StoreMesh, default_config};
use crate::network::InProcessRouter;

const LATE: Duration = Duration::from_millis(300);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminate_returns_only_once_a_late_store_clone_has_let_the_directory_go() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().join("store");
    let mesh = StoreMesh::start_durable(
        1,
        "in-process://1".into(),
        InProcessRouter::new(),
        default_config("terminate-releases").expect("config"),
        &path,
    )
    .await
    .expect("start");
    let StoreBackend::Fjall(store) = &mesh.store else {
        panic!("a durable mesh is fjall-backed");
    };
    let late = store.clone();
    tokio::spawn(async move {
        tokio::time::sleep(LATE).await;
        drop(late);
    });
    let began = Instant::now();
    mesh.terminate().await.expect("terminate");
    assert!(
        began.elapsed() >= LATE,
        "terminate returned after {:?}, before the late clone let go",
        began.elapsed()
    );
    DataDirLock::acquire(&path).expect("the directory is free once terminate returns");
}
