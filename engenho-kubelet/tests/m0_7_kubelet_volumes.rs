//! M0.7 — kubelet volumes (emptyDir + configMap + secret mounts;
//! missing-source Pending).
//!
//! Proves the kubelet-volumes brick against the MOCK seams (FakeBackend +
//! FakeVolumeMaterializer) — the materializer trait IS the testability
//! contract, so ZERO real podman / real filesystem:
//!
//!   (a) configMap `data:{greeting:hello}` → the materializer received
//!       `greeting → b"hello"`; the container's ContainerSpec.mounts carries
//!       a ResolvedMount at the right mountPath, read-only.
//!   (b) secret base64 `data` → the materializer received the DECODED bytes
//!       (proves decode happened before materialize).
//!   (c) emptyDir referenced by 2 containers → the SAME NamedVolume
//!       MountSource is stamped on BOTH containers' mounts (shared).
//!   (d) missing non-optional configMap → resolve fails; the pod is written
//!       Pending with `containerStatuses[].state.waiting.reason ==
//!       "ConfigMapNotFound"` AND NO backend.start call (FakeBackend events
//!       carry no Start).
//!   (e) no-volume pod → ContainerSpec.mounts empty + run_argv byte-identical
//!       to before this brick (regression).
//!   (f) pure run_argv unit test: a spec with 2 ResolvedMounts → exact
//!       `-v src:path[:ro]` argv in deterministic order, WITHOUT podman.

use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::Controller;
use engenho_kubelet::backend::{ContainerSpec, FakeEvent, PodmanBackend};
use engenho_kubelet::pod_volume::{MountSource, ResolvedMount};
use engenho_kubelet::{FakeBackend, FakeVolumeMaterializer, Kubelet, VolumeMaterializer};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};

async fn delete_pod(store: &StoreMesh, name: &str) {
    store
        .propose(ResourceCommand::delete(pod_key(name), Reason::Operator))
        .await
        .unwrap();
}
use serde_json::{Value, json};

async fn boot_store(name: &str) -> Arc<StoreMesh> {
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

fn pod_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

async fn put(store: &StoreMesh, key: ResourceKey, value: Value) {
    store
        .propose(ResourceCommand::Put {
            key,
            value,
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

fn b64(s: &str) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
}

fn waiting_reason(pod: &Value, idx: usize) -> Option<String> {
    pod.get("status")?
        .get("containerStatuses")?
        .as_array()?
        .get(idx)?
        .get("state")?
        .get("waiting")?
        .get("reason")?
        .as_str()
        .map(String::from)
}

fn phase(pod: &Value) -> Option<String> {
    pod.get("status")?.get("phase")?.as_str().map(String::from)
}

fn count_starts(events: &[FakeEvent]) -> usize {
    events
        .iter()
        .filter(|e| matches!(e, FakeEvent::Start(_)))
        .count()
}

async fn teardown(store: Arc<StoreMesh>, kubelet: Kubelet) {
    drop(kubelet);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

// ── (a) configMap → files + read-only mount ────────────────────────────────

#[tokio::test]
async fn configmap_volume_materializes_files_and_mounts_read_only() {
    let store = boot_store("vol-configmap").await;
    let backend = Arc::new(FakeBackend::new());
    let mat = Arc::new(FakeVolumeMaterializer::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A")
        .with_volume_materializer(mat.clone() as Arc<dyn VolumeMaterializer>);

    put(
        &store,
        ResourceKey::namespaced("", "v1", "ConfigMap", "default", "cfg"),
        json!({
            "kind": "ConfigMap", "apiVersion": "v1",
            "metadata": { "name": "cfg" },
            "data": { "greeting": "hello" }
        }),
    )
    .await;
    put(
        &store,
        pod_key("p1"),
        json!({
            "kind": "Pod", "apiVersion": "v1",
            "metadata": { "name": "p1" },
            "spec": {
                "nodeName": "node-A",
                "volumes": [ { "name": "cfg-vol", "configMap": { "name": "cfg" } } ],
                "containers": [ {
                    "name": "main", "image": "busybox",
                    "volumeMounts": [ { "name": "cfg-vol", "mountPath": "/etc/cfg" } ]
                } ]
            }
        }),
    )
    .await;

    kubelet.tick().await.unwrap();

    // The materializer was asked to write greeting → b"hello".
    let files = mat.files_for("cfg-vol").await.unwrap();
    assert_eq!(files.get("greeting"), Some(&b"hello".to_vec()));

    // The started container's spec carries a read-only ResolvedMount at the
    // right mountPath (FakeBackend records it ON ContainerSpec — zero plumbing).
    let containers = backend.containers().await;
    assert_eq!(containers.len(), 1);
    let (id, _) = &containers[0];
    let spec = backend.spec_of(id).await.unwrap();
    assert_eq!(spec.mounts.len(), 1);
    assert_eq!(spec.mounts[0].mount_path, "/etc/cfg");
    assert!(
        spec.mounts[0].read_only,
        "configMap mount defaults read-only"
    );
    assert!(matches!(
        spec.mounts[0].source,
        MountSource::Materialized(_)
    ));

    teardown(store, kubelet).await;
}

// ── (b) secret base64-decoded before materialize ───────────────────────────

#[tokio::test]
async fn secret_volume_decodes_base64_before_materialize() {
    let store = boot_store("vol-secret").await;
    let backend = Arc::new(FakeBackend::new());
    let mat = Arc::new(FakeVolumeMaterializer::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A")
        .with_volume_materializer(mat.clone() as Arc<dyn VolumeMaterializer>);

    put(
        &store,
        ResourceKey::namespaced("", "v1", "Secret", "default", "sec"),
        json!({
            "kind": "Secret", "apiVersion": "v1",
            "metadata": { "name": "sec" },
            "data": { "token": b64("s3cr3t-value") }
        }),
    )
    .await;
    put(
        &store,
        pod_key("p1"),
        json!({
            "kind": "Pod", "apiVersion": "v1",
            "metadata": { "name": "p1" },
            "spec": {
                "nodeName": "node-A",
                "volumes": [ { "name": "sec-vol", "secret": { "secretName": "sec" } } ],
                "containers": [ {
                    "name": "main", "image": "busybox",
                    "volumeMounts": [ { "name": "sec-vol", "mountPath": "/etc/sec" } ]
                } ]
            }
        }),
    )
    .await;

    kubelet.tick().await.unwrap();

    // The materializer saw the DECODED bytes, proving the decode happened.
    let files = mat.files_for("sec-vol").await.unwrap();
    assert_eq!(files.get("token"), Some(&b"s3cr3t-value".to_vec()));

    teardown(store, kubelet).await;
}

// ── (c) emptyDir shared across 2 containers ────────────────────────────────

#[tokio::test]
async fn empty_dir_named_volume_shared_across_containers() {
    let store = boot_store("vol-emptydir").await;
    let backend = Arc::new(FakeBackend::new());
    let mat = Arc::new(FakeVolumeMaterializer::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A")
        .with_volume_materializer(mat.clone() as Arc<dyn VolumeMaterializer>);

    put(
        &store,
        pod_key("p1"),
        json!({
            "kind": "Pod", "apiVersion": "v1",
            "metadata": { "name": "p1" },
            "spec": {
                "nodeName": "node-A",
                "volumes": [ { "name": "scratch", "emptyDir": {} } ],
                "containers": [
                    {
                        "name": "writer", "image": "busybox",
                        "volumeMounts": [ { "name": "scratch", "mountPath": "/data" } ]
                    },
                    {
                        "name": "reader", "image": "busybox",
                        "volumeMounts": [ { "name": "scratch", "mountPath": "/data" } ]
                    }
                ]
            }
        }),
    )
    .await;

    kubelet.tick().await.unwrap();

    // emptyDir ensured exactly once for the pod.
    assert_eq!(mat.ensured_empty_dirs().await, vec!["scratch".to_string()]);

    // BOTH containers carry the SAME NamedVolume MountSource (shared).
    let containers = backend.containers().await;
    assert_eq!(containers.len(), 2);
    let mut sources = Vec::new();
    for (id, _) in &containers {
        let spec = backend.spec_of(id).await.unwrap();
        assert_eq!(spec.mounts.len(), 1);
        assert_eq!(spec.mounts[0].mount_path, "/data");
        // emptyDir defaults read-write.
        assert!(!spec.mounts[0].read_only);
        sources.push(spec.mounts[0].source.clone());
    }
    assert_eq!(
        sources[0], sources[1],
        "both containers share one named volume"
    );
    assert!(matches!(sources[0], MountSource::NamedVolume(_)));

    teardown(store, kubelet).await;
}

// ── (c2) emptyDir named volume is reaped on pod delete ─────────────────────

#[tokio::test]
async fn empty_dir_named_volume_reaped_on_pod_delete() {
    let store = boot_store("vol-emptydir-reap").await;
    let backend = Arc::new(FakeBackend::new());
    let mat = Arc::new(FakeVolumeMaterializer::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A")
        .with_volume_materializer(mat.clone() as Arc<dyn VolumeMaterializer>);

    put(
        &store,
        pod_key("p1"),
        json!({
            "kind": "Pod", "apiVersion": "v1",
            "metadata": { "name": "p1" },
            "spec": {
                "nodeName": "node-A",
                "volumes": [
                    { "name": "scratch", "emptyDir": {} },
                    // A configMap-less optional vol: its (empty) materialized
                    // directory is removed as a directory, never reaped as a
                    // named volume.
                    { "name": "cfg-vol", "configMap": { "name": "absent", "optional": true } }
                ],
                "containers": [ {
                    "name": "main", "image": "busybox",
                    "volumeMounts": [
                        { "name": "scratch", "mountPath": "/data" },
                        { "name": "cfg-vol", "mountPath": "/etc/cfg" }
                    ]
                } ]
            }
        }),
    )
    .await;

    // Start tick: emptyDir ensured, container started, nothing reaped yet.
    kubelet.tick().await.unwrap();
    assert_eq!(mat.ensured_empty_dirs().await, vec!["scratch".to_string()]);
    assert!(
        mat.removed_empty_dirs().await.is_empty(),
        "no reap before delete"
    );
    assert_eq!(count_starts(&backend.events().await), 1);

    assert!(
        mat.removed_materialized().await.is_empty(),
        "no materialized directory removed before delete"
    );

    // Hard-delete the pod → next tick reaps the orphan's containers, its
    // emptyDir named volume, AND the directory the configMap was materialized
    // into — each through its own teardown, never one mistaken for the other.
    delete_pod(&store, "p1").await;
    kubelet.tick().await.unwrap();

    assert_eq!(
        mat.removed_empty_dirs().await,
        vec!["scratch".to_string()],
        "exactly the emptyDir volume is reaped on pod delete"
    );
    // ★ T2.10: this directory was never removed before. The LocalPod comment
    // promised a "pod-dir GC" that did not exist.
    assert_eq!(
        mat.removed_materialized().await,
        vec![std::path::PathBuf::from("/fake/cfg-vol")],
        "the materialized configMap directory is removed on pod delete"
    );

    teardown(store, kubelet).await;
}

// ── (d) missing non-optional configMap → Pending + no start ────────────────

#[tokio::test]
async fn missing_configmap_keeps_pod_pending_no_start() {
    let store = boot_store("vol-missing").await;
    let backend = Arc::new(FakeBackend::new());
    let mat = Arc::new(FakeVolumeMaterializer::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A")
        .with_volume_materializer(mat.clone() as Arc<dyn VolumeMaterializer>);

    // Pod references a configMap that does NOT exist in the store.
    put(
        &store,
        pod_key("p1"),
        json!({
            "kind": "Pod", "apiVersion": "v1",
            "metadata": { "name": "p1" },
            "spec": {
                "nodeName": "node-A",
                "volumes": [ { "name": "cfg-vol", "configMap": { "name": "absent" } } ],
                "containers": [ {
                    "name": "main", "image": "busybox",
                    "volumeMounts": [ { "name": "cfg-vol", "mountPath": "/etc/cfg" } ]
                } ]
            }
        }),
    )
    .await;

    kubelet.tick().await.unwrap();

    // Pod is Pending with the typed waiting reason; NO container started.
    let pod = store.get(&pod_key("p1")).await.unwrap();
    assert_eq!(phase(&pod).as_deref(), Some("Pending"));
    assert_eq!(
        waiting_reason(&pod, 0).as_deref(),
        Some("ConfigMapNotFound")
    );
    assert_eq!(
        count_starts(&backend.events().await),
        0,
        "no backend start on missing source"
    );

    // Now CREATE the configMap → next tick resolves + the pod proceeds.
    put(
        &store,
        ResourceKey::namespaced("", "v1", "ConfigMap", "default", "absent"),
        json!({
            "kind": "ConfigMap", "apiVersion": "v1",
            "metadata": { "name": "absent" },
            "data": { "k": "v" }
        }),
    )
    .await;
    kubelet.tick().await.unwrap();
    let pod = store.get(&pod_key("p1")).await.unwrap();
    assert_eq!(
        phase(&pod).as_deref(),
        Some("Running"),
        "pod converges once source exists"
    );
    assert_eq!(count_starts(&backend.events().await), 1);

    teardown(store, kubelet).await;
}

// ── (e) no-volume pod is unchanged ─────────────────────────────────────────

#[tokio::test]
async fn no_volume_pod_runs_with_empty_mounts() {
    let store = boot_store("vol-none").await;
    let backend = Arc::new(FakeBackend::new());
    let mat = Arc::new(FakeVolumeMaterializer::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A")
        .with_volume_materializer(mat.clone() as Arc<dyn VolumeMaterializer>);

    put(
        &store,
        pod_key("p1"),
        json!({
            "kind": "Pod", "apiVersion": "v1",
            "metadata": { "name": "p1" },
            "spec": {
                "nodeName": "node-A",
                "containers": [ { "name": "main", "image": "busybox" } ]
            }
        }),
    )
    .await;

    kubelet.tick().await.unwrap();

    let pod = store.get(&pod_key("p1")).await.unwrap();
    assert_eq!(phase(&pod).as_deref(), Some("Running"));
    // The container's spec has NO mounts + the materializer was never asked.
    let containers = backend.containers().await;
    assert_eq!(containers.len(), 1);
    let (id, _) = &containers[0];
    let spec = backend.spec_of(id).await.unwrap();
    assert!(
        spec.mounts.is_empty(),
        "no-volume pod produces empty mounts"
    );
    assert!(mat.ensured_empty_dirs().await.is_empty());

    teardown(store, kubelet).await;
}

// ── (f) pure run_argv: -v src:path[:ro] in order, no podman ─────────────────

#[test]
fn run_argv_emits_volume_flags_in_order() {
    let backend = PodmanBackend::new();
    let spec = ContainerSpec {
        name: "default_p1_main".into(),
        image: "busybox".into(),
        mounts: vec![
            ResolvedMount {
                source: MountSource::UserHostPath("/home/u/vols/cfg".into()),
                mount_path: "/etc/cfg".into(),
                read_only: true,
                sub_path: None,
            },
            ResolvedMount {
                source: MountSource::NamedVolume("engenho-empty-default_p1_scratch".into()),
                mount_path: "/data".into(),
                read_only: false,
                sub_path: None,
            },
        ],
        ..Default::default()
    };
    let argv = backend.run_argv(&spec);

    // The two `-v` pairs appear in spec.mounts order, AFTER `--name <name>`
    // and BEFORE the image. A read-only host path → `:ro`; NamedVolume rw → none.
    let name_idx = argv.iter().position(|a| a == "default_p1_main").unwrap();
    let cfg_idx = argv
        .iter()
        .position(|a| a == "/home/u/vols/cfg:/etc/cfg:ro")
        .unwrap();
    let scratch_idx = argv
        .iter()
        .position(|a| a == "engenho-empty-default_p1_scratch:/data")
        .unwrap();
    let image_idx = argv.iter().position(|a| a == "busybox").unwrap();

    // Both are -v values immediately preceded by a "-v" flag.
    assert_eq!(argv[cfg_idx - 1], "-v");
    assert_eq!(argv[scratch_idx - 1], "-v");
    // Ordering: name < cfg < scratch < image (deterministic, unit-assertable).
    assert!(name_idx < cfg_idx, "mounts come after --name");
    assert!(cfg_idx < scratch_idx, "mounts preserve spec.mounts order");
    assert!(scratch_idx < image_idx, "mounts come before the image");
}

// ── T2.10: cleanup obligations are values ───────────────────────────────────

/// ★ T2.10, against the real filesystem: deleting a pod removes the
/// directory its secret was materialized into, and leaves the directory its
/// `hostPath` named exactly as it was.
///
/// Before T2.10 both were one `MountSource::HostDir` arm, and neither was
/// ever removed: every secret a pod had mounted stayed on the node's disk
/// after the pod was gone, behind a comment promising a "pod-dir GC" that did
/// not exist. Removing "every HostDir" instead would have deleted the
/// user's data — which is why the fix is a type, not a loop.
#[tokio::test]
async fn deleting_a_pod_removes_its_materialized_secret_and_never_its_host_path() {
    let store = boot_store("vol-teardown-real").await;
    let backend = Arc::new(FakeBackend::new());
    let data_root = tempfile::tempdir().expect("tempdir");
    let user_root = tempfile::tempdir().expect("tempdir");
    let user_dir = user_root.path().join("pgdata");
    std::fs::create_dir_all(&user_dir).unwrap();
    std::fs::write(user_dir.join("PG_VERSION"), b"16").unwrap();

    let mat =
        Arc::new(engenho_kubelet::PodmanVolumeMaterializer::new().with_data_root(data_root.path()));
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A")
        .with_volume_materializer(mat as Arc<dyn VolumeMaterializer>)
        .with_host_path_policy(engenho_kubelet::pod_volume::HostPathPolicy::allowing([
            user_root.path(),
        ]));

    put(
        &store,
        ResourceKey::namespaced("", "v1", "Secret", "default", "creds"),
        json!({
            "kind": "Secret", "apiVersion": "v1",
            "metadata": { "name": "creds", "namespace": "default" },
            "data": { "password": b64("hunter2") }
        }),
    )
    .await;
    put(
        &store,
        pod_key("db"),
        json!({
            "kind": "Pod", "apiVersion": "v1",
            "metadata": { "name": "db", "namespace": "default" },
            "spec": {
                "nodeName": "node-A",
                "volumes": [
                    { "name": "creds", "secret": { "secretName": "creds" } },
                    { "name": "data", "hostPath": { "path": user_dir.to_string_lossy() } }
                ],
                "containers": [ {
                    "name": "main", "image": "busybox",
                    "volumeMounts": [
                        { "name": "creds", "mountPath": "/etc/creds" },
                        { "name": "data", "mountPath": "/var/lib/data" }
                    ]
                } ]
            }
        }),
    )
    .await;

    kubelet.tick().await.unwrap();
    assert_eq!(count_starts(&backend.events().await), 1, "control: started");
    let secret_dir = data_root.path().join("default_db").join("creds");
    assert_eq!(
        std::fs::read(secret_dir.join("password")).unwrap(),
        b"hunter2",
        "control: the secret was materialized"
    );

    delete_pod(&store, "db").await;
    kubelet.tick().await.unwrap();

    assert!(
        !secret_dir.exists(),
        "the materialized secret must not outlive its pod"
    );
    assert!(
        !data_root.path().join("default_db").exists(),
        "nor the pod's now-empty directory"
    );
    assert!(
        user_dir.join("PG_VERSION").exists(),
        "a hostPath is the user's data and is never removed"
    );
    assert_eq!(
        std::fs::read(user_dir.join("PG_VERSION")).unwrap(),
        b"16",
        "nor rewritten"
    );

    teardown(store, kubelet).await;
}

/// A container whose stop is still in flight (the native backend's answer
/// while its process is inside the grace period) holds the pod's teardown:
/// no volume is removed from under a process that may still be using it,
/// the kubelet asks to come back soon rather than waiting for a sweep, and
/// once the process is reaped everything is discharged.
#[tokio::test]
async fn volumes_wait_for_a_container_still_inside_its_grace_period() {
    use engenho_controllers::ReconcileResult;
    use engenho_kubelet::ContainerRuntime;

    let store = boot_store("vol-teardown-unreaped").await;
    let backend = Arc::new(FakeBackend::new());
    let mat = Arc::new(FakeVolumeMaterializer::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A")
        .with_volume_materializer(mat.clone() as Arc<dyn VolumeMaterializer>);

    put(
        &store,
        pod_key("p1"),
        json!({
            "kind": "Pod", "apiVersion": "v1",
            "metadata": { "name": "p1" },
            "spec": {
                "nodeName": "node-A",
                "volumes": [
                    { "name": "scratch", "emptyDir": {} },
                    { "name": "cfg-vol", "configMap": { "name": "absent", "optional": true } }
                ],
                "containers": [ {
                    "name": "main", "image": "busybox",
                    "volumeMounts": [
                        { "name": "scratch", "mountPath": "/data" },
                        { "name": "cfg-vol", "mountPath": "/etc/cfg" }
                    ]
                } ]
            }
        }),
    )
    .await;
    kubelet.tick().await.unwrap();
    let (cid, _) = backend.containers().await.remove(0);

    backend.hold_unreaped(&cid).await;
    delete_pod(&store, "p1").await;
    let outcome = kubelet.tick().await.unwrap();

    assert!(
        backend.status(&cid).await.unwrap().is_some(),
        "an unreaped container's record stays"
    );
    assert!(
        mat.removed_empty_dirs().await.is_empty() && mat.removed_materialized().await.is_empty(),
        "no volume is torn down while a container that mounts it is still stopping"
    );
    assert!(
        matches!(
            outcome.result,
            ReconcileResult::Requeue(_) | ReconcileResult::RequeueWithProgress(_)
        ),
        "the kubelet must come back soon to finish, got {:?}",
        outcome.result
    );

    backend.reap(&cid).await;
    kubelet.tick().await.unwrap();

    assert_eq!(
        backend.status(&cid).await.unwrap(),
        None,
        "removed once reaped"
    );
    assert_eq!(mat.removed_empty_dirs().await, vec!["scratch".to_string()]);
    assert_eq!(
        mat.removed_materialized().await,
        vec![std::path::PathBuf::from("/fake/cfg-vol")]
    );

    teardown(store, kubelet).await;
}
