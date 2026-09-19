//! One pod's failure costs only that pod, and a pod that cannot be given its
//! ServiceAccount credentials is held `Pending` with a Warning.
//!
//! ## What was broken
//!
//! The kubelet's tick walked every bound pod in a hand loop whose every step
//! ended in `?`. The ServiceAccount projection's write was one of those steps,
//! mapped to `ControllerError::Internal`: a pod whose credential directory
//! could not be written failed the WHOLE tick, so every pod after it in key
//! order was neither started nor observed — on that tick and on every retry,
//! for as long as the one directory kept failing. The other half of the same
//! step, a token that could not be minted, did the opposite and was as bad: a
//! silent skip with no status and no Event, so the pod showed a `nodeName`
//! and nothing else, even though the projector's contract says the failure
//! "surfaces as the pod-Pending reason, never a silent skip".
//!
//! ## What these tests pin
//!
//!   (a) a pod whose credentials cannot be written does not stop the pods
//!       after it, and the tick counts every bound pod once;
//!   (b) that pod is `Pending` with the reason on every container, a Warning
//!       Event on it naming the cause, nothing started, and a requeue armed;
//!   (c) a token that cannot be minted is held the same way, not skipped
//!       silently;
//!   (d) the Warning follows the retry curve, not the loop: a tick that comes
//!       back before the pod is due does not announce it again;
//!   (e) a held pod starts once its credentials can be written.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use engenho_controllers::event_recorder::{CollectingEventSink, EventRecord, Reason, Severity};
use engenho_controllers::{Controller, ReconcileResult};
use engenho_kubelet::{
    FakeBackend, FakeVolumeMaterializer, Kubelet, MaterializedDir, MountSource,
    ServiceAccountProjector, TestClock, VolumeMaterializer, VolumeResolveError,
};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason as WriteReason, ResourceCommand},
    default_config,
};
use serde_json::{Value, json};

/// The volume the kubelet writes a pod's ServiceAccount credentials under.
const SA_VOLUME: &str = "kube-api-access";

/// A projector that mints a token, or refuses while `refusing` is set.
#[derive(Default)]
struct Projector {
    refusing: AtomicBool,
}

#[async_trait]
impl ServiceAccountProjector for Projector {
    async fn project(
        &self,
        namespace: &str,
        _service_account: &str,
        _pod_name: &str,
        _pod_uid: &str,
    ) -> Result<Option<BTreeMap<String, Vec<u8>>>, String> {
        if self.refusing.load(Ordering::SeqCst) {
            return Err("signing key unavailable (test)".into());
        }
        let mut files = BTreeMap::new();
        files.insert("token".to_string(), b"token".to_vec());
        files.insert("ca.crt".to_string(), b"ca".to_vec());
        files.insert("namespace".to_string(), namespace.as_bytes().to_vec());
        Ok(Some(files))
    }
}

/// A materializer that cannot write ONE pod's ServiceAccount credentials
/// while `refusing` is set, and behaves like the fake for everything else.
struct Materializer {
    inner: FakeVolumeMaterializer,
    pod: &'static str,
    refusing: AtomicBool,
}

impl Materializer {
    fn refusing_for(pod: &'static str) -> Self {
        Self {
            inner: FakeVolumeMaterializer::new(),
            pod,
            refusing: AtomicBool::new(true),
        }
    }
}

#[async_trait]
impl VolumeMaterializer for Materializer {
    fn name(&self) -> &'static str {
        "refusing-one-pod"
    }

    async fn materialize_files(
        &self,
        namespace: &str,
        pod: &str,
        volume: &str,
        files: &BTreeMap<String, Vec<u8>>,
    ) -> Result<MountSource, VolumeResolveError> {
        if pod == self.pod && volume == SA_VOLUME && self.refusing.load(Ordering::SeqCst) {
            return Err(VolumeResolveError::Materialize(
                "no space left on device (test)".into(),
            ));
        }
        self.inner
            .materialize_files(namespace, pod, volume, files)
            .await
    }

    async fn ensure_empty_dir(
        &self,
        namespace: &str,
        pod: &str,
        volume: &str,
    ) -> Result<MountSource, VolumeResolveError> {
        self.inner.ensure_empty_dir(namespace, pod, volume).await
    }

    async fn remove_empty_dir(
        &self,
        namespace: &str,
        pod: &str,
        volume: &str,
    ) -> Result<(), VolumeResolveError> {
        self.inner.remove_empty_dir(namespace, pod, volume).await
    }

    async fn remove_materialized(&self, dir: &MaterializedDir) -> Result<(), VolumeResolveError> {
        self.inner.remove_materialized(dir).await
    }
}

struct Node {
    store: Arc<StoreMesh>,
    backend: Arc<FakeBackend>,
    events: Arc<CollectingEventSink>,
    kubelet: Kubelet,
}

impl Node {
    async fn boot(
        tag: &str,
        projector: Arc<Projector>,
        materializer: Arc<dyn VolumeMaterializer>,
        clock: &TestClock,
    ) -> Self {
        let router = InProcessRouter::new();
        let cfg = default_config(tag).unwrap();
        let store = Arc::new(
            StoreMesh::start(1, "in-process://1".into(), router, cfg)
                .await
                .unwrap(),
        );
        store.initialize_singleton().await.unwrap();
        assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
        let backend = Arc::new(FakeBackend::new());
        let events = Arc::new(CollectingEventSink::new());
        let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A")
            .with_volume_materializer(materializer)
            .with_sa_projector(projector as Arc<dyn ServiceAccountProjector>)
            .with_event_sink(events.clone())
            .with_clock(clock.as_clock());
        Self {
            store,
            backend,
            events,
            kubelet,
        }
    }

    async fn put_pod(&self, name: &str) {
        self.store
            .propose(ResourceCommand::Put {
                key: key(name),
                value: json!({
                    "kind": "Pod", "apiVersion": "v1",
                    "metadata": { "name": name, "namespace": "default",
                                  "uid": (["uid-", name].concat()) },
                    "spec": {
                        "nodeName": "node-A",
                        "serviceAccountName": "sa-1",
                        "containers": [ { "name": "main", "image": "busybox" } ]
                    }
                }),
                expected: None,
                reason: WriteReason::Operator,
            })
            .await
            .unwrap();
    }

    async fn pod(&self, name: &str) -> Value {
        self.store.get(&key(name)).await.expect("the pod is stored")
    }

    /// Start attempts for the pod's one container, by its backend name.
    async fn starts(&self, pod: &str) -> usize {
        self.backend
            .start_attempts(&["default_", pod, "_main"].concat())
            .await
    }

    /// Every Warning recorded since the last call.
    fn warnings(&self) -> Vec<EventRecord> {
        self.events
            .drain()
            .into_iter()
            .filter(|e| e.reason.severity() == Severity::Warning)
            .collect()
    }

    async fn teardown(self) {
        drop(self.kubelet);
        let mesh = Arc::try_unwrap(self.store).ok().unwrap();
        mesh.terminate().await.unwrap();
    }
}

fn key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

fn waiting_reason(pod: &Value) -> Option<&str> {
    pod.pointer("/status/containerStatuses/0/state/waiting/reason")
        .and_then(Value::as_str)
}

// ── (a) one pod's failure does not stop the pods after it ──────────────────

#[tokio::test]
async fn a_pod_whose_credentials_cannot_be_written_does_not_stop_the_pods_after_it() {
    let clock = TestClock::new();
    let node = Node::boot(
        "i7-isolation",
        Arc::new(Projector::default()),
        Arc::new(Materializer::refusing_for("a-unwritable")),
        &clock,
    )
    .await;
    // Key order puts the failing pod FIRST, so a tick that stops at it
    // never reaches the healthy one.
    node.put_pod("a-unwritable").await;
    node.put_pod("b-healthy").await;

    let outcome = node
        .kubelet
        .tick()
        .await
        .expect("one pod's credentials failing must not fail the tick");

    assert_eq!(
        node.starts("b-healthy").await,
        1,
        "the pod after the failing one in key order was started"
    );
    assert_eq!(
        node.starts("a-unwritable").await,
        0,
        "no container starts without the credentials it mounts"
    );
    let swept = outcome.sweep.expect("the tick reports its per-pod tally");
    assert_eq!(swept.examined(), 2, "every bound pod was reached once");
    assert_eq!(
        swept.failed(),
        0,
        "a pod held for its credentials is held, not failed"
    );

    node.teardown().await;
}

// ── (b) held Pending, with the reason and a Warning ────────────────────────

#[tokio::test]
async fn a_pod_whose_credentials_cannot_be_written_is_pending_with_a_warning() {
    let clock = TestClock::new();
    let node = Node::boot(
        "i7-unwritable",
        Arc::new(Projector::default()),
        Arc::new(Materializer::refusing_for("a-unwritable")),
        &clock,
    )
    .await;
    node.put_pod("a-unwritable").await;

    let outcome = node.kubelet.tick().await.expect("the tick succeeds");

    let pod = node.pod("a-unwritable").await;
    assert_eq!(pod["status"]["phase"], "Pending", "{pod}");
    assert_eq!(
        waiting_reason(&pod),
        Some("VolumeMaterializeError"),
        "the container says why it is waiting: {pod}"
    );
    let warnings = node.warnings();
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    let w = &warnings[0];
    assert_eq!(w.reason, Reason::Failed);
    assert_eq!(w.involved.kind, "Pod");
    assert_eq!(w.involved.name, "a-unwritable");
    assert!(
        w.message.contains(SA_VOLUME) && w.message.contains("no space left on device"),
        "the Warning names the volume and the cause: {}",
        w.message
    );
    assert!(
        matches!(outcome.result, ReconcileResult::Requeue(_)),
        "the held pod is come back to on its retry curve: {:?}",
        outcome.result
    );

    node.teardown().await;
}

// ── (c) a token that cannot be minted is held, never skipped silently ──────

#[tokio::test]
async fn a_token_that_cannot_be_minted_leaves_the_pod_pending_with_a_warning() {
    let clock = TestClock::new();
    let projector = Arc::new(Projector::default());
    projector.refusing.store(true, Ordering::SeqCst);
    let node = Node::boot(
        "i7-unminted",
        projector,
        Arc::new(FakeVolumeMaterializer::new()),
        &clock,
    )
    .await;
    node.put_pod("p1").await;

    node.kubelet.tick().await.expect("the tick succeeds");

    let pod = node.pod("p1").await;
    assert_eq!(
        pod["status"]["phase"], "Pending",
        "a pod that cannot get its identity says so in its status: {pod}"
    );
    assert_eq!(
        waiting_reason(&pod),
        Some("ServiceAccountTokenUnavailable"),
        "{pod}"
    );
    assert_eq!(node.starts("p1").await, 0, "nothing started");
    let warnings = node.warnings();
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(
        warnings[0].message.contains("sa-1")
            && warnings[0].message.contains("signing key unavailable"),
        "the Warning names the ServiceAccount and the cause: {}",
        warnings[0].message
    );

    node.teardown().await;
}

// ── (d) the Warning follows the retry curve, not the loop ──────────────────

#[tokio::test]
async fn the_warning_is_repeated_when_the_pod_is_due_not_on_every_tick() {
    let clock = TestClock::new();
    let projector = Arc::new(Projector::default());
    projector.refusing.store(true, Ordering::SeqCst);
    let node = Node::boot(
        "i7-cadence",
        projector,
        Arc::new(FakeVolumeMaterializer::new()),
        &clock,
    )
    .await;
    node.put_pod("p1").await;

    node.kubelet.tick().await.unwrap();
    assert_eq!(node.warnings().len(), 1, "the first failure is announced");

    // Woken again before the pod is due (a write, another pod's timer): the
    // same failure is not a new Event — each one is a store write.
    node.kubelet.tick().await.unwrap();
    assert_eq!(
        node.warnings().len(),
        0,
        "a tick before the pod is due does not announce it again"
    );

    // Past the wait the curve owes: announced again.
    clock.advance(Duration::from_secs(5));
    node.kubelet.tick().await.unwrap();
    assert_eq!(
        node.warnings().len(),
        1,
        "a failure still there when the pod is due is announced again"
    );

    node.teardown().await;
}

// ── (e) a held pod starts once it can ──────────────────────────────────────

#[tokio::test]
async fn a_held_pod_starts_once_its_credentials_can_be_written() {
    let clock = TestClock::new();
    let materializer = Arc::new(Materializer::refusing_for("p1"));
    let node = Node::boot(
        "i7-recovers",
        Arc::new(Projector::default()),
        materializer.clone(),
        &clock,
    )
    .await;
    node.put_pod("p1").await;

    node.kubelet.tick().await.expect("the tick succeeds");
    assert_eq!(node.starts("p1").await, 0, "premise: held");

    materializer.refusing.store(false, Ordering::SeqCst);
    node.kubelet.tick().await.expect("the tick succeeds");

    assert_eq!(node.starts("p1").await, 1, "the held pod started");
    let pod = node.pod("p1").await;
    assert_eq!(pod["status"]["phase"], "Running", "{pod}");
    assert!(
        materializer.inner.files_for(SA_VOLUME).await.is_some(),
        "its credentials were written before it started"
    );

    node.teardown().await;
}
