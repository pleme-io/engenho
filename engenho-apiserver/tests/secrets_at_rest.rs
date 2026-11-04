use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{ApiError, ResourceHandler, Selectors, StoreBackedHandler};
use engenho_store::command::{Reason, ResourceCommand};
use engenho_store::{
    InProcessRouter, KEY_FILE, ResourceKey, StoreMesh, default_config, keys_dir_for,
};
use serde_json::json;

const TOKEN: &str = "api-canary-5e1d";

async fn boot(path: &Path) -> (Arc<StoreMesh>, StoreBackedHandler) {
    let store = Arc::new(
        StoreMesh::start_durable(
            1,
            "in-process://1".into(),
            InProcessRouter::new(),
            default_config("secrets-api").unwrap(),
            path,
        )
        .await
        .unwrap(),
    );
    store.initialize_if_fresh().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(5)).await);
    let handler = StoreBackedHandler::for_core_kind(store.clone(), "Secret", true).unwrap();
    (store, handler)
}

async fn stop(store: Arc<StoreMesh>, handler: StoreBackedHandler) {
    let _ = store.flush().await.unwrap();
    drop(handler);
    Arc::into_inner(store).unwrap().terminate().await.unwrap();
}

async fn seal_one(path: &Path) {
    let (store, handler) = boot(path).await;
    store
        .propose(ResourceCommand::put(
            ResourceKey::namespaced("", "v1", "Secret", "default", "a"),
            json!({"metadata": {"name": "a", "namespace": "default"}, "stringData": {"token": TOKEN}}),
            Reason::Operator,
        ))
        .await
        .unwrap();
    stop(store, handler).await;
}

#[tokio::test]
async fn the_api_returns_a_decrypted_secret_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    seal_one(&path).await;

    let (store, handler) = boot(&path).await;
    let got = handler.get(Some("default"), "a").await.unwrap();
    assert_eq!(got["stringData"]["token"], TOKEN);
    let (items, _) = handler
        .list_at(Some("default"), &Selectors::default())
        .await
        .unwrap();
    assert_eq!(items[0]["stringData"]["token"], TOKEN);
    stop(store, handler).await;
}

#[tokio::test]
async fn a_lost_key_answers_a_storage_error_naming_the_cause_never_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    seal_one(&path).await;
    std::fs::remove_file(keys_dir_for(&path).join(KEY_FILE)).unwrap();

    let (store, handler) = boot(&path).await;
    let refused = handler.get(Some("default"), "a").await.unwrap_err();
    let ApiError::StorageError(reason) = &refused else {
        panic!("a sealed Secret read as {refused:?}");
    };
    assert!(reason.contains("is missing"), "{reason}");
    assert!(reason.contains(KEY_FILE), "{reason}");

    let listed = handler
        .list_at(Some("default"), &Selectors::default())
        .await;
    assert!(
        matches!(listed, Err(ApiError::StorageError(_))),
        "{listed:?}"
    );
    let paged = handler
        .list_page(Some("default"), &Selectors::default(), 10, None)
        .await;
    assert!(matches!(paged, Err(ApiError::StorageError(_))), "{paged:?}");

    let (other, _) = handler
        .list_at(Some("elsewhere"), &Selectors::default())
        .await
        .expect("a namespace with no unreadable Secret still lists");
    assert!(other.is_empty());
    assert!(matches!(
        handler.get(Some("default"), "absent").await,
        Err(ApiError::NotFound(_))
    ));
    stop(store, handler).await;
}
