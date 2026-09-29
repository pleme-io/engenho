//! A native pod's closure is fetched before its first start, rooted for as
//! long as a pod bound here names it, and — when it cannot exist — the pod
//! fails with a typed reason instead of waiting forever.
//!
//! Measured on a native node when the daemon restarted onto a new release:
//! garbage collection removed the old release's closure, which eight pods
//! still named; nothing had rooted it. The pods sat `ContainerCreating` for
//! ~5.5 h, the spawn retried on the start curve forever, and no controller
//! replaced them because nothing said they could never start.
//!
//! These cases drive the kubelet with an injected store. The same seam is
//! exercised against a real `nix-store` and a real targeted GC in
//! `native_closure_roots_survive_gc.rs`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use engenho_controllers::Controller;
use engenho_kubelet::kubelet::TestClock;
use engenho_kubelet::{ClosureState, ClosureStore, FakeBackend, Kubelet, RootName};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::{Value, json};

const PRESENT: &str = "/nix/store/q91n8aaaaaaaaaaaaaaaaaaaaaaaaaaa-rust_engenho-0.53.119";
const GONE: &str = "/nix/store/p27v2aaaaaaaaaaaaaaaaaaaaaaaaaaa-rust_engenho-0.53.118";

/// A store holding `present`, and a record of every root.
#[derive(Default)]
struct FakeStore {
    present: Mutex<Vec<PathBuf>>,
    roots: Mutex<BTreeMap<RootName, PathBuf>>,
    ensures: Mutex<Vec<PathBuf>>,
}

#[async_trait::async_trait]
impl ClosureStore for FakeStore {
    fn name(&self) -> &'static str {
        "fake"
    }
    async fn ensure(&self, root: &RootName, closure: &Path) -> ClosureState {
        self.ensures.lock().unwrap().push(closure.to_path_buf());
        if !self.is_present(closure) {
            return ClosureState::Unavailable {
                path: closure.to_path_buf(),
                detail: "no substituter has it".into(),
            };
        }
        self.roots
            .lock()
            .unwrap()
            .insert(root.clone(), closure.to_path_buf());
        ClosureState::Rooted
    }
    fn is_present(&self, closure: &Path) -> bool {
        self.present.lock().unwrap().iter().any(|p| p == closure)
    }
    fn roots(&self) -> std::io::Result<BTreeMap<RootName, PathBuf>> {
        Ok(self.roots.lock().unwrap().clone())
    }
    fn release(&self, root: &RootName) -> std::io::Result<()> {
        self.roots.lock().unwrap().remove(root);
        Ok(())
    }
}

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

fn key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

async fn put_pod(store: &StoreMesh, name: &str, closure: &str) {
    store
        .propose(ResourceCommand::put(
            key(name),
            json!({"kind": "Pod", "apiVersion": "v1", "metadata": {"name": name},
                   "spec": {"nodeName": "node-A", "restartPolicy": "Always",
                            "containers": [{"name": "app", "image": format!("nix:{closure}")}]}}),
            Reason::Operator,
        ))
        .await
        .unwrap();
}

fn kubelet(
    store: &Arc<StoreMesh>,
    backend: &Arc<FakeBackend>,
    closures: &Arc<FakeStore>,
) -> Kubelet {
    Kubelet::new(store.clone(), backend.clone(), "node-A")
        .with_clock(TestClock::new().as_clock())
        .with_closure_store(closures.clone())
}

#[tokio::test]
async fn a_pod_whose_closure_cannot_exist_fails_as_image_unavailable_naming_the_path() {
    let store = boot_store("closures-unavailable").await;
    let backend = Arc::new(FakeBackend::new());
    let closures = Arc::new(FakeStore::default());
    let k = kubelet(&store, &backend, &closures);
    put_pod(&store, "agent", GONE).await;

    k.tick().await.unwrap();

    let p = store.get(&key("agent")).await.unwrap();
    assert_eq!(p.pointer("/status/phase"), Some(&json!("Failed")), "{p}");
    assert_eq!(
        p.pointer("/status/reason"),
        Some(&json!("ImageUnavailable")),
        "{p}"
    );
    let waiting = p
        .pointer("/status/containerStatuses/0/state/waiting")
        .cloned()
        .unwrap_or(Value::Null);
    assert_eq!(waiting["reason"], "ImageUnavailable", "{p}");
    assert!(
        waiting["message"]
            .as_str()
            .is_some_and(|m| m.contains(GONE)),
        "the message names the path: {p}"
    );
    assert_eq!(
        backend.start_attempts("default_agent_app").await,
        0,
        "no spawn of a program that does not exist"
    );
    assert_eq!(
        closures.ensures.lock().unwrap().len(),
        1,
        "the fetch was tried first"
    );

    // Terminal: later ticks neither retry nor rewrite it.
    k.tick().await.unwrap();
    assert_eq!(closures.ensures.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_started_pods_closure_is_rooted_and_released_with_the_pod() {
    let store = boot_store("closures-rooted").await;
    let backend = Arc::new(FakeBackend::new());
    let closures = Arc::new(FakeStore::default());
    closures
        .present
        .lock()
        .unwrap()
        .push(PathBuf::from(PRESENT));
    let k = kubelet(&store, &backend, &closures);
    put_pod(&store, "agent", PRESENT).await;

    k.tick().await.unwrap();
    assert_eq!(backend.start_attempts("default_agent_app").await, 1);
    let roots = closures.roots().unwrap();
    assert_eq!(
        roots.values().collect::<Vec<_>>(),
        vec![&PathBuf::from(PRESENT)]
    );

    store
        .propose(ResourceCommand::delete(key("agent"), Reason::Operator))
        .await
        .unwrap();
    k.tick().await.unwrap();
    assert!(
        closures.roots().unwrap().is_empty(),
        "no pod names it: released"
    );
}

/// Roots are reconciled from the pods bound here, not from starts: a root no
/// pod names (its pod went while the daemon was down) is released, and so is
/// a terminal pod's. The same pass roots a running pod's closure the first
/// tick it sees it, which is what protects an OLD closure between a daemon
/// restart and the rollout that replaces its pods.
#[tokio::test]
async fn roots_are_reconciled_from_the_pods_bound_here_not_from_starts() {
    let store = boot_store("closures-reconciled").await;
    let backend = Arc::new(FakeBackend::new());
    let closures = Arc::new(FakeStore::default());
    closures
        .present
        .lock()
        .unwrap()
        .push(PathBuf::from(PRESENT));
    // A stale root no pod names, left by a pod that went while the daemon
    // was down.
    closures
        .roots
        .lock()
        .unwrap()
        .insert(RootName::new("dead-uid", "app"), PathBuf::from(GONE));
    put_pod(&store, "agent", PRESENT).await;
    store
        .propose(ResourceCommand::patch(
            key("agent"),
            json!({"status": {"phase": "Succeeded"}}),
            Reason::Operator,
        ))
        .await
        .unwrap();
    let k = kubelet(&store, &backend, &closures);

    k.tick().await.unwrap();
    let roots = closures.roots().unwrap();
    assert!(
        !roots.contains_key(&RootName::new("dead-uid", "app")),
        "the stale root is released: {roots:?}"
    );
    assert!(
        roots.is_empty(),
        "a terminal pod needs its closure no longer: {roots:?}"
    );
}
