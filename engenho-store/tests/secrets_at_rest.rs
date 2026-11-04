use std::path::{Path, PathBuf};
use std::time::Duration;

use engenho_store::{
    FjallStore, InProcessRouter, KEY_FILE, ListScope, ReadConsistency, ReadRefused, Reason,
    ResourceCommand, ResourceKey, SealFault, SealWrites, StoreError, StoreMesh, default_config,
    keys_dir_for,
};
use openraft::RaftSnapshotBuilder as _;
use openraft::storage::RaftStateMachine as _;
use serde_json::{Value, json};

const TOKEN: &str = "plaintext-canary-3f9a7c1e";
const VISIBLE: &str = "configmap-control-canary-7d2b";
const BOUND: Duration = Duration::from_secs(10);

fn secret(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Secret", "default", name)
}

fn config_map(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "ConfigMap", "default", name)
}

fn secret_value(name: &str) -> Value {
    json!({"metadata": {"name": name, "namespace": "default"}, "stringData": {"token": TOKEN}})
}

fn config_value(name: &str) -> Value {
    json!({"metadata": {"name": name, "namespace": "default"}, "data": {"note": VISIBLE}})
}

async fn start(path: &Path, writes: SealWrites) -> StoreMesh {
    let mesh = StoreMesh::start_durable_with(
        1,
        "in-process://1".into(),
        InProcessRouter::new(),
        default_config("secrets-at-rest").unwrap(),
        path,
        writes,
    )
    .await
    .unwrap();
    mesh.initialize_if_fresh().await.unwrap();
    assert!(mesh.wait_for_leadership(BOUND).await);
    mesh
}

async fn put(mesh: &StoreMesh, key: ResourceKey, value: Value) -> Result<(), StoreError> {
    mesh.propose(ResourceCommand::put(key, value, Reason::Operator))
        .await
        .map(|_| ())
}

async fn stop(mesh: StoreMesh) {
    let _ = mesh.flush().await.unwrap();
    mesh.terminate().await.unwrap();
}

fn files_under(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                found.push(path);
            }
        }
    }
    found
}

fn disk_holds(root: &Path, needle: &str) -> bool {
    files_under(root).iter().any(|file| {
        std::fs::read(file)
            .unwrap()
            .windows(needle.len())
            .any(|window| window == needle.as_bytes())
    })
}

fn raw_catalog(path: &Path) -> Vec<u8> {
    let keyspace = fjall::Config::new(path).open().unwrap();
    let catalog = keyspace
        .open_partition("catalog", fjall::PartitionCreateOptions::default())
        .unwrap();
    catalog.get(b"catalog").unwrap().unwrap().to_vec()
}

fn stored_value(catalog: &[u8], key: &ResourceKey) -> Value {
    let blob: Value = serde_json::from_slice(catalog).unwrap();
    let want = serde_json::to_value(key).unwrap();
    blob["resources"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row[0] == want)
        .map(|row| row[1][0].clone())
        .unwrap()
}

fn key_file(path: &Path) -> PathBuf {
    keys_dir_for(path).join(KEY_FILE)
}

fn store_path(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().join("store")
}

#[tokio::test]
async fn a_secret_round_trips_through_a_store_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let mesh = start(&path, SealWrites::Seal).await;
    put(&mesh, secret("a"), secret_value("a")).await.unwrap();
    stop(mesh).await;

    let mesh = start(&path, SealWrites::Seal).await;
    let value = mesh.get(&secret("a")).await.expect("the secret survives");
    assert_eq!(value["stringData"]["token"], TOKEN);
    let listed = mesh.list("", "v1", "Secret", Some("default")).await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].1["stringData"]["token"], TOKEN);
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn a_replay_of_a_sealed_log_entry_restores_the_secret() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let mesh = start(&path, SealWrites::Seal).await;
    for index in 0..3 {
        put(&mesh, secret(&format!("s{index}")), secret_value("s"))
            .await
            .unwrap();
    }
    mesh.terminate().await.unwrap();

    let mesh = start(&path, SealWrites::Seal).await;
    for index in 0..3 {
        let value = mesh.get(&secret(&format!("s{index}"))).await.unwrap();
        assert_eq!(value["stringData"]["token"], TOKEN);
    }
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn the_raw_bytes_on_disk_do_not_hold_the_plaintext() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let mesh = start(&path, SealWrites::Seal).await;
    put(&mesh, secret("a"), secret_value("a")).await.unwrap();
    put(&mesh, config_map("c"), config_value("c"))
        .await
        .unwrap();
    stop(mesh).await;

    assert!(
        disk_holds(&path, VISIBLE),
        "the walker must see an unsealed kind, or this test proves nothing"
    );
    assert!(
        !disk_holds(&path, TOKEN),
        "a Secret reached the disk in plaintext"
    );

    let stored = stored_value(&raw_catalog(&path), &secret("a"));
    assert!(stored.as_str().unwrap().starts_with("engenho-sealed:v1:"));
}

#[tokio::test]
async fn a_snapshot_is_sealed_at_rest_and_plain_to_the_next_node() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("leader");
    let learner_path = dir.path().join("learner");
    let mesh = start(&leader_path, SealWrites::Seal).await;
    put(&mesh, secret("a"), secret_value("a")).await.unwrap();
    stop(mesh).await;

    let mut leader = FjallStore::open(&leader_path).unwrap();
    let built = leader
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    let (meta, wire) = (built.meta, built.snapshot.into_inner());
    assert!(
        wire.windows(TOKEN.len()).any(|w| w == TOKEN.as_bytes()),
        "the snapshot a peer receives is opened, so its own key can seal it"
    );
    drop(leader);
    assert!(!disk_holds(&leader_path, TOKEN));

    let mut leader = FjallStore::open(&leader_path).unwrap();
    let served = leader.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(served.snapshot.into_inner(), wire);
    drop(leader);

    let mut learner = FjallStore::open(&learner_path).unwrap();
    learner
        .install_snapshot(&meta, Box::new(std::io::Cursor::new(wire)))
        .await
        .unwrap();
    assert_eq!(
        learner.get_resource(&secret("a")).await.unwrap()["stringData"]["token"],
        TOKEN
    );
    drop(learner);
    assert_ne!(
        std::fs::read(key_file(&leader_path)).unwrap(),
        std::fs::read(key_file(&learner_path)).unwrap(),
        "each node generates its own key"
    );
    assert!(!disk_holds(&learner_path, TOKEN));

    let learner = FjallStore::open(&learner_path).unwrap();
    assert_eq!(
        learner.get_resource(&secret("a")).await.unwrap()["stringData"]["token"],
        TOKEN
    );
}

#[tokio::test]
async fn a_follower_with_its_own_key_replicates_and_stores_no_plaintext() {
    let dir = tempfile::tempdir().unwrap();
    let (first_path, second_path) = (dir.path().join("first"), dir.path().join("second"));
    let router = InProcessRouter::new();
    let first = StoreMesh::start_durable(
        1,
        "in-process://1".into(),
        router.clone(),
        default_config("secrets-at-rest").unwrap(),
        &first_path,
    )
    .await
    .unwrap();
    let second = StoreMesh::start_durable(
        2,
        "in-process://2".into(),
        router.clone(),
        default_config("secrets-at-rest").unwrap(),
        &second_path,
    )
    .await
    .unwrap();
    first
        .initialize_with_voters(vec![
            (1, "in-process://1".into()),
            (2, "in-process://2".into()),
        ])
        .await
        .unwrap();
    assert!(first.wait_for_leadership(BOUND).await || second.wait_for_leadership(BOUND).await);
    let leader = if first.is_leader().await {
        &first
    } else {
        &second
    };
    let result = leader
        .propose(ResourceCommand::put(
            secret("a"),
            secret_value("a"),
            Reason::Operator,
        ))
        .await
        .unwrap();
    for node in [&first, &second] {
        assert!(
            node.wait_for_revision(engenho_store::Revision(result.revision), BOUND)
                .await
        );
        assert_eq!(
            node.get(&secret("a")).await.unwrap()["stringData"]["token"],
            TOKEN
        );
    }
    let _ = first.flush().await.unwrap();
    let _ = second.flush().await.unwrap();
    first.terminate().await.unwrap();
    second.terminate().await.unwrap();

    assert_ne!(
        std::fs::read(key_file(&first_path)).unwrap(),
        std::fs::read(key_file(&second_path)).unwrap()
    );
    assert!(!disk_holds(&first_path, TOKEN));
    assert!(!disk_holds(&second_path, TOKEN));
}

#[tokio::test]
async fn a_legacy_plaintext_value_reads_and_converts_on_the_next_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let mesh = start(&path, SealWrites::Plain).await;
    put(&mesh, secret("old"), secret_value("old"))
        .await
        .unwrap();
    stop(mesh).await;
    assert!(
        !key_file(&path).exists(),
        "a store that seals nothing generates no key"
    );
    let legacy = raw_catalog(&path);
    assert_eq!(
        stored_value(&legacy, &secret("old"))["stringData"]["token"],
        TOKEN
    );

    let store = FjallStore::open(&path).unwrap();
    assert_eq!(
        store.get_resource(&secret("old")).await.unwrap()["stringData"]["token"],
        TOKEN,
        "a legacy value reads as itself"
    );
    drop(store);
    assert_eq!(raw_catalog(&path), legacy, "opening rewrites nothing");

    let mesh = start(&path, SealWrites::Seal).await;
    assert_eq!(
        mesh.get(&secret("old")).await.unwrap()["stringData"]["token"],
        TOKEN
    );
    put(&mesh, config_map("next"), config_value("next"))
        .await
        .unwrap();
    stop(mesh).await;

    let converted = raw_catalog(&path);
    assert!(
        stored_value(&converted, &secret("old"))
            .as_str()
            .unwrap()
            .starts_with("engenho-sealed:v1:")
    );
    assert!(
        !converted
            .windows(TOKEN.len())
            .any(|w| w == TOKEN.as_bytes()),
        "the next image write converts the value"
    );

    let mesh = start(&path, SealWrites::Seal).await;
    assert_eq!(
        mesh.get(&secret("old")).await.unwrap()["stringData"]["token"],
        TOKEN
    );
    mesh.terminate().await.unwrap();
}

async fn seal_then_stop(path: &Path) -> Value {
    let mesh = start(path, SealWrites::Seal).await;
    put(&mesh, secret("a"), secret_value("a")).await.unwrap();
    put(&mesh, config_map("c"), config_value("c"))
        .await
        .unwrap();
    stop(mesh).await;
    stored_value(&raw_catalog(path), &secret("a"))
}

async fn assert_refused_and_unchanged(path: &Path, sealed: &Value, expect: &str) {
    let mesh = start(path, SealWrites::Seal).await;

    let fault = mesh
        .secret_fault(&secret("a"))
        .await
        .expect("a typed fault");
    assert_eq!(fault.kind(), expect);
    assert!(
        mesh.get(&secret("a")).await.is_none(),
        "no ciphertext as data"
    );
    assert!(
        mesh.list("", "v1", "Secret", Some("default"))
            .await
            .is_empty(),
        "no ciphertext in a list"
    );
    assert_eq!(
        mesh.get_consistent(&secret("a"), ReadConsistency::Latest, BOUND)
            .await,
        Err(ReadRefused::SecretUnreadable { fault })
    );
    assert_eq!(
        mesh.secret_faults(ListScope::new("", "v1", "Secret", Some("default")))
            .await,
        vec![(secret("a"), fault)]
    );

    assert_eq!(
        mesh.get(&config_map("c")).await.unwrap()["data"]["note"],
        VISIBLE,
        "a sibling of another kind still reads"
    );

    let overwrite = put(&mesh, secret("a"), json!({"stringData": {"token": "new"}})).await;
    assert!(
        matches!(overwrite, Err(StoreError::SecretUnreadable { .. })),
        "got {overwrite:?}"
    );
    let delete = mesh
        .propose(ResourceCommand::delete(secret("a"), Reason::Operator))
        .await;
    assert!(
        matches!(delete, Err(StoreError::SecretUnreadable { .. })),
        "got {delete:?}"
    );
    let fresh = put(&mesh, secret("new"), secret_value("new")).await;
    assert!(
        matches!(fresh, Err(StoreError::SecretUnreadable { .. })),
        "a new Secret has no key to seal under: {fresh:?}"
    );
    put(&mesh, config_map("still-writes"), config_value("x"))
        .await
        .expect("an unsealed kind still writes");
    stop(mesh).await;

    assert_eq!(
        &stored_value(&raw_catalog(path), &secret("a")),
        sealed,
        "the stored value is byte for byte what it was"
    );
}

#[tokio::test]
async fn a_wrong_key_is_a_typed_failure_and_leaves_the_bytes_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let sealed = seal_then_stop(&path).await;
    let right_key = std::fs::read(key_file(&path)).unwrap();

    let wrong: Vec<u8> = right_key.iter().map(|byte| byte ^ 0xA5).collect();
    std::fs::write(key_file(&path), &wrong).unwrap();
    assert_refused_and_unchanged(&path, &sealed, "wrong_key").await;
    assert_eq!(
        std::fs::read(key_file(&path)).unwrap(),
        wrong,
        "the key file is never rewritten"
    );

    std::fs::write(key_file(&path), &right_key).unwrap();
    let mesh = start(&path, SealWrites::Seal).await;
    assert_eq!(
        mesh.get(&secret("a")).await.unwrap()["stringData"]["token"],
        TOKEN,
        "the right key opens the untouched value"
    );
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn a_missing_key_is_a_typed_failure_and_is_never_regenerated() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let sealed = seal_then_stop(&path).await;
    let right_key = std::fs::read(key_file(&path)).unwrap();

    std::fs::remove_file(key_file(&path)).unwrap();
    assert_refused_and_unchanged(&path, &sealed, "key_missing").await;
    assert!(!key_file(&path).exists(), "a stranding key was generated");

    std::fs::write(key_file(&path), &right_key).unwrap();
    let mesh = start(&path, SealWrites::Seal).await;
    assert_eq!(
        mesh.get(&secret("a")).await.unwrap()["stringData"]["token"],
        TOKEN
    );
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn a_malformed_key_is_a_typed_failure() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let sealed = seal_then_stop(&path).await;
    std::fs::write(key_file(&path), b"truncated").unwrap();
    assert_refused_and_unchanged(&path, &sealed, "key_unusable").await;
    assert_eq!(std::fs::read(key_file(&path)).unwrap(), b"truncated");
}

#[tokio::test]
async fn a_value_copied_to_another_key_fails_alone() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let mesh = start(&path, SealWrites::Seal).await;
    put(&mesh, secret("a"), secret_value("a")).await.unwrap();
    put(&mesh, secret("b"), secret_value("b")).await.unwrap();
    stop(mesh).await;

    {
        let keyspace = fjall::Config::new(&path).open().unwrap();
        let catalog = keyspace
            .open_partition("catalog", fjall::PartitionCreateOptions::default())
            .unwrap();
        let bytes = catalog.get(b"catalog").unwrap().unwrap();
        let mut blob: Value = serde_json::from_slice(&bytes).unwrap();
        let from = stored_value(&bytes, &secret("a"));
        let target = serde_json::to_value(secret("b")).unwrap();
        let rows = blob["resources"].as_array_mut().unwrap();
        let row = rows.iter_mut().find(|row| row[0] == target).unwrap();
        row[1][0] = from;
        catalog
            .insert(b"catalog", serde_json::to_vec(&blob).unwrap())
            .unwrap();
        keyspace.persist(fjall::PersistMode::SyncAll).unwrap();
    }

    let store = FjallStore::open(&path).unwrap();
    assert_eq!(
        store.get_resource(&secret("a")).await.unwrap()["stringData"]["token"],
        TOKEN,
        "the value at its own key still opens"
    );
    assert!(
        store.get_resource(&secret("b")).await.is_none(),
        "a moved value never opens"
    );
    let mesh = start_after(store, &path).await;
    assert!(matches!(
        mesh.secret_fault(&secret("b")).await,
        Some(SealFault::Damaged { .. })
    ));
    mesh.terminate().await.unwrap();
}

async fn start_after(store: FjallStore, path: &Path) -> StoreMesh {
    drop(store);
    start(path, SealWrites::Seal).await
}

#[tokio::test]
async fn encryption_off_writes_plaintext_and_converts_sealed_values_back() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let sealed = seal_then_stop(&path).await;
    assert!(sealed.is_string());

    let mesh = start(&path, SealWrites::Plain).await;
    assert_eq!(
        mesh.get(&secret("a")).await.unwrap()["stringData"]["token"],
        TOKEN,
        "off still reads what was sealed"
    );
    put(&mesh, config_map("off"), config_value("off"))
        .await
        .unwrap();
    stop(mesh).await;
    assert_eq!(
        stored_value(&raw_catalog(&path), &secret("a"))["stringData"]["token"],
        TOKEN
    );
}

#[tokio::test]
async fn encryption_off_with_a_lost_key_still_keeps_sealed_values_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let sealed = seal_then_stop(&path).await;
    std::fs::remove_file(key_file(&path)).unwrap();

    let mesh = start(&path, SealWrites::Plain).await;
    assert_eq!(
        mesh.secret_fault(&secret("a")).await.unwrap().kind(),
        "key_missing"
    );
    put(&mesh, config_map("off"), config_value("off"))
        .await
        .unwrap();
    stop(mesh).await;
    assert_eq!(stored_value(&raw_catalog(&path), &secret("a")), sealed);
    assert!(!key_file(&path).exists());
}
