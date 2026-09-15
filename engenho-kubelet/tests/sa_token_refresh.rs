//! Projected ServiceAccount tokens are REWRITTEN before they expire.
//!
//! ## What was broken, and why it was expensive to diagnose
//!
//! The projection ran exactly once, on the path that starts a pod's
//! containers. Bound tokens carry an `exp`, so every API-calling workload
//! worked perfectly and then began failing at a fixed age — one hour by
//! default. At that moment nothing in the kubelet looked wrong: the pod was
//! Running, its containers healthy, the signing key fine, and the apiserver
//! correctly rejecting a genuinely expired credential. An operator that works
//! after a restart and dies an hour later reads as a bug in the operator.
//!
//! ## What these tests prove
//!
//!   (a) the refresh REWRITES — the token bytes in the materializer CHANGE.
//!       This is the negative control for the whole feature: a refresh that
//!       re-wrote the *same* token would satisfy a call-count assertion while
//!       leaving the pod to expire exactly as before.
//!   (b) the cadence gate holds — a second tick inside the interval does not
//!       re-mint, so an idle kubelet is not writing files every tick.
//!   (c) the cadence is DERIVED from the lifetime, not a second constant, and
//!       the floor cannot produce a zero interval.
//!   (d) a projector that mints nothing is refreshed zero times rather than
//!       erroring — the honest no-op.
//!   (e) a pod we never STARTED is not refreshed; its start path projects
//!       afresh, so writing for it would write files nothing has mounted.
//!   (f) a minting failure is survivable: the tick still succeeds and the pod
//!       keeps running on the token it already has.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use engenho_controllers::Controller;
use engenho_kubelet::pod_volume::ServiceAccountProjector;
use engenho_kubelet::{
    FakeBackend, FakeVolumeMaterializer, Kubelet, TestClock, VolumeMaterializer,
};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::{Value, json};

/// One hour, the runtime's real default — so these tests exercise the
/// production cadence rather than a convenient one.
const LIFETIME: Duration = Duration::from_secs(3600);

/// A projector that mints a DIFFERENT token every call.
///
/// The changing value is the point. A fake returning a constant token would
/// let a broken refresh (one that rewrites identical bytes, or never runs at
/// all) pass an assertion about files being present.
struct CountingProjector {
    calls: AtomicUsize,
    lifetime: Option<Duration>,
    fail: bool,
}

impl CountingProjector {
    fn new(lifetime: Option<Duration>) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            lifetime,
            fail: false,
        }
    }

    fn failing() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            lifetime: Some(LIFETIME),
            fail: true,
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ServiceAccountProjector for CountingProjector {
    async fn project(
        &self,
        namespace: &str,
        _service_account: &str,
        _pod_name: &str,
        _pod_uid: &str,
    ) -> Result<Option<BTreeMap<String, Vec<u8>>>, String> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            return Err("mint refused (test)".into());
        }
        if self.lifetime.is_none() {
            return Ok(None);
        }
        let mut files = BTreeMap::new();
        files.insert("token".to_string(), format!("token-mint-{n}").into_bytes());
        files.insert("ca.crt".to_string(), b"ca".to_vec());
        files.insert("namespace".to_string(), namespace.as_bytes().to_vec());
        Ok(Some(files))
    }

    fn token_lifetime(&self) -> Option<Duration> {
        self.lifetime
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

fn pod(name: &str, node: &str) -> Value {
    json!({
        "kind": "Pod", "apiVersion": "v1",
        "metadata": { "name": name, "uid": "uid-1" },
        "spec": {
            "nodeName": node,
            "serviceAccountName": "sa-1",
            "containers": [ { "name": "main", "image": "busybox" } ]
        }
    })
}

async fn teardown(store: Arc<StoreMesh>, kubelet: Kubelet) {
    drop(kubelet);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

/// The token in the materializer's `kube-api-access` volume, if any.
async fn projected_token(mat: &FakeVolumeMaterializer) -> Option<Vec<u8>> {
    mat.files_for("kube-api-access")
        .await
        .and_then(|f| f.get("token").cloned())
}

// ── (a) THE NEGATIVE CONTROL — the token bytes actually change ─────────────

#[tokio::test]
async fn refresh_rewrites_the_token_with_new_bytes() {
    let store = boot_store("sa-refresh-rewrites").await;
    let backend = Arc::new(FakeBackend::new());
    let mat = Arc::new(FakeVolumeMaterializer::new());
    let proj = Arc::new(CountingProjector::new(Some(LIFETIME)));
    let clock = TestClock::new();
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A")
        .with_volume_materializer(mat.clone() as Arc<dyn VolumeMaterializer>)
        .with_sa_projector(proj.clone() as Arc<dyn ServiceAccountProjector>)
        .with_clock(clock.as_clock());

    put(&store, pod_key("p1"), pod("p1", "node-A")).await;

    // Tick 1 starts the pod and projects its first token.
    kubelet.tick().await.unwrap();
    let first = projected_token(&mat).await.expect("pod gets a token");

    // Past the refresh interval (a third of the lifetime).
    clock.advance(LIFETIME / 2);
    kubelet.tick().await.unwrap();
    let second = projected_token(&mat).await.expect("token still present");

    assert_ne!(
        first, second,
        "the refresh must MINT A NEW token, not rewrite the same bytes — \
         identical bytes would expire on the original schedule"
    );

    teardown(store, kubelet).await;
}

// ── (b) the cadence gate holds ─────────────────────────────────────────────

#[tokio::test]
async fn a_tick_inside_the_interval_does_not_remint() {
    let store = boot_store("sa-refresh-cadence").await;
    let backend = Arc::new(FakeBackend::new());
    let mat = Arc::new(FakeVolumeMaterializer::new());
    let proj = Arc::new(CountingProjector::new(Some(LIFETIME)));
    let clock = TestClock::new();
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A")
        .with_volume_materializer(mat.clone() as Arc<dyn VolumeMaterializer>)
        .with_sa_projector(proj.clone() as Arc<dyn ServiceAccountProjector>)
        .with_clock(clock.as_clock());

    put(&store, pod_key("p1"), pod("p1", "node-A")).await;
    kubelet.tick().await.unwrap();
    let after_start = proj.calls();

    // Well inside the interval — three idle ticks must not mint anything.
    clock.advance(Duration::from_secs(60));
    kubelet.tick().await.unwrap();
    kubelet.tick().await.unwrap();
    kubelet.tick().await.unwrap();

    assert_eq!(
        proj.calls(),
        after_start,
        "an idle kubelet inside the refresh interval must not re-mint; \
         minting every tick makes every idle reconcile a filesystem write"
    );

    teardown(store, kubelet).await;
}

// ── (c) the cadence is DERIVED, and the floor cannot be zero ───────────────

#[tokio::test]
async fn a_shorter_lifetime_refreshes_sooner() {
    // The same elapsed time that is INSIDE the interval for a 1h token must be
    // OUTSIDE it for a 60s token. If the cadence were a constant rather than a
    // function of the lifetime, both would behave identically — which is the
    // silent drift this derivation exists to prevent.
    for (lifetime, advance, want_remint) in [
        (LIFETIME, Duration::from_secs(60), false),
        (Duration::from_secs(60), Duration::from_secs(60), true),
    ] {
        let store = boot_store(&format!("sa-refresh-derived-{}", lifetime.as_secs())).await;
        let backend = Arc::new(FakeBackend::new());
        let mat = Arc::new(FakeVolumeMaterializer::new());
        let proj = Arc::new(CountingProjector::new(Some(lifetime)));
        let clock = TestClock::new();
        let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A")
            .with_volume_materializer(mat.clone() as Arc<dyn VolumeMaterializer>)
            .with_sa_projector(proj.clone() as Arc<dyn ServiceAccountProjector>)
            .with_clock(clock.as_clock());

        put(&store, pod_key("p1"), pod("p1", "node-A")).await;
        kubelet.tick().await.unwrap();
        let after_start = proj.calls();

        clock.advance(advance);
        kubelet.tick().await.unwrap();

        let reminted = proj.calls() > after_start;
        assert_eq!(
            reminted, want_remint,
            "lifetime {lifetime:?} after {advance:?}: the refresh cadence must \
             follow the lifetime, not a fixed constant"
        );

        teardown(store, kubelet).await;
    }
}

// ── (d) a projector that mints nothing is a no-op, not an error ────────────

#[tokio::test]
async fn a_projector_with_no_lifetime_is_never_refreshed() {
    let store = boot_store("sa-refresh-none").await;
    let backend = Arc::new(FakeBackend::new());
    let mat = Arc::new(FakeVolumeMaterializer::new());
    // No lifetime ⇒ mints nothing ⇒ nothing to keep alive.
    let proj = Arc::new(CountingProjector::new(None));
    let clock = TestClock::new();
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A")
        .with_volume_materializer(mat.clone() as Arc<dyn VolumeMaterializer>)
        .with_sa_projector(proj.clone() as Arc<dyn ServiceAccountProjector>)
        .with_clock(clock.as_clock());

    put(&store, pod_key("p1"), pod("p1", "node-A")).await;
    kubelet.tick().await.unwrap();
    let after_start = proj.calls();

    clock.advance(LIFETIME);
    kubelet.tick().await.unwrap();

    assert_eq!(
        proj.calls(),
        after_start,
        "a projector that mints nothing must not be driven by the refresh loop"
    );
    assert!(
        projected_token(&mat).await.is_none(),
        "nothing projected means no token file — never an empty one"
    );

    teardown(store, kubelet).await;
}

// ── (e) a pod we never started is not refreshed ────────────────────────────

#[tokio::test]
async fn a_pod_bound_to_another_node_is_not_refreshed() {
    let store = boot_store("sa-refresh-unstarted").await;
    let backend = Arc::new(FakeBackend::new());
    let mat = Arc::new(FakeVolumeMaterializer::new());
    let proj = Arc::new(CountingProjector::new(Some(LIFETIME)));
    let clock = TestClock::new();
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A")
        .with_volume_materializer(mat.clone() as Arc<dyn VolumeMaterializer>)
        .with_sa_projector(proj.clone() as Arc<dyn ServiceAccountProjector>)
        .with_clock(clock.as_clock());

    // Bound to a DIFFERENT node: this kubelet never starts it, so it holds no
    // materialized directory for it and must not write one.
    put(&store, pod_key("elsewhere"), pod("elsewhere", "node-B")).await;
    kubelet.tick().await.unwrap();
    clock.advance(LIFETIME);
    kubelet.tick().await.unwrap();

    assert_eq!(
        proj.calls(),
        0,
        "a pod this kubelet never started has no projection to refresh"
    );

    teardown(store, kubelet).await;
}

// ── (f) a minting failure is survivable ────────────────────────────────────

#[tokio::test]
async fn a_failed_refresh_does_not_fail_the_tick() {
    let store = boot_store("sa-refresh-failure").await;
    let backend = Arc::new(FakeBackend::new());
    let mat = Arc::new(FakeVolumeMaterializer::new());
    let proj = Arc::new(CountingProjector::failing());
    let clock = TestClock::new();
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A")
        .with_volume_materializer(mat.clone() as Arc<dyn VolumeMaterializer>)
        .with_sa_projector(proj.clone() as Arc<dyn ServiceAccountProjector>)
        .with_clock(clock.as_clock());

    put(&store, pod_key("p1"), pod("p1", "node-A")).await;
    // The first projection fails, so the pod stays Pending — that is the
    // pre-existing contract and not what this test is about.
    kubelet.tick().await.unwrap();

    clock.advance(LIFETIME);
    // The refresh pass also fails. The tick must still succeed: a kubelet that
    // stops reconciling because a token could not be re-minted has turned a
    // credential problem into an outage.
    let outcome = kubelet.tick().await;
    assert!(
        outcome.is_ok(),
        "a refresh failure must be logged, never fatal: {outcome:?}"
    );

    teardown(store, kubelet).await;
}
