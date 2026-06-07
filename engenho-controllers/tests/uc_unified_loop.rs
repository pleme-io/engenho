//! Controller-loop unification — proves the typed `ReconcileResult` is
//! LOAD-BEARING (propagated, never swallowed) and the owned-children
//! blanket impl is behavior-preserving.
//!
//! Three properties:
//!   1. A controller that doesn't opt into requeue returns
//!      `ReconcileOutcome.result == Done` (drivers behave as before).
//!   2. A controller that requests `Requeue{after}` gets a one-shot
//!      re-tick fired at `after` — NOT a silent warn-and-wait. The
//!      WatchDriver's `arm_requeue` schedules the extra tick.
//!   3. A controller that returns a Transient `ControllerError::Store`
//!      is RETRIED (the loop schedules a re-tick), while a Declarative
//!      error is surfaced + NOT targeted-retried — proven via the typed
//!      classifier on `ControllerError`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use engenho_controllers::{
    Controller, ControllerError, KindFilter, ReconcileOutcome, ReconcileReport, ReconcileResult,
    ReplicaSetController, WatchDriver, WatchDriverConfig,
};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::json;

async fn boot() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("uc-unified-loop").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

// ──────────────────────────────────────────────────────────────────────
// (1) Owned-children controller defaults result == Done — same behavior
//     as the pre-unification counter-only loop.
// ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn owned_children_tick_defaults_result_to_done() {
    let store = boot().await;
    // A 2-replica ReplicaSet still reconciles to 2 Pods, and the typed
    // outcome carries result == Done (no requeue opt-in).
    let rs_key = ResourceKey::namespaced("apps", "v1", "ReplicaSet", "default", "web");
    store
        .propose(ResourceCommand::Put {
            key: rs_key,
            value: json!({
                "kind": "ReplicaSet", "apiVersion": "apps/v1",
                "metadata": {"name": "web"},
                "spec": {"replicas": 2, "selector": {"matchLabels": {"app": "web"}},
                         "template": {"metadata": {"labels": {"app": "web"}}, "spec": {}}}
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();

    let ctrl = ReplicaSetController::new(store.clone(), Some("default".into()));
    let outcome = ctrl.tick().await.unwrap();
    // Behavior-preserving counts: 2 pods + 1 status write.
    assert_eq!(outcome.report.objects_examined, 1);
    assert_eq!(outcome.report.objects_changed, 3);
    // The typed requeue decision defaults to Done.
    assert_eq!(outcome.result, ReconcileResult::Done);
    assert_eq!(outcome.result.requeue_after(), None);

    drop(ctrl);
    let mesh = Arc::try_unwrap(store).ok().expect("only owner");
    mesh.terminate().await.unwrap();
}

// ──────────────────────────────────────────────────────────────────────
// (2) A Requeue{after} is propagated by the driver into ONE extra tick,
//     not swallowed. The controller returns Requeue on its first tick
//     and Done after; the WatchDriver's arm_requeue fires the re-tick.
// ──────────────────────────────────────────────────────────────────────

/// Controller that requeues itself once: tick #1 returns
/// `Requeue(20ms)`, tick #2+ returns `Done`. Records every tick.
#[derive(Default)]
struct RequeueOnce {
    ticks: AtomicUsize,
}

#[async_trait::async_trait]
impl Controller for RequeueOnce {
    fn name(&self) -> &'static str {
        "requeue_once"
    }
    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let n = self.ticks.fetch_add(1, Ordering::SeqCst);
        let result = if n == 0 {
            ReconcileResult::Requeue(Duration::from_millis(20))
        } else {
            ReconcileResult::Done
        };
        Ok(ReconcileOutcome::new(ReconcileReport::default(), result))
    }
}

/// Arc-sharing wrapper so the test keeps a handle to the tick counter
/// while the driver owns the controller.
struct RequeueOnceRef {
    inner: Arc<RequeueOnce>,
}

#[async_trait::async_trait]
impl Controller for RequeueOnceRef {
    fn name(&self) -> &'static str {
        "requeue_once"
    }
    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        self.inner.tick().await
    }
}

#[tokio::test]
async fn requeue_result_arms_one_shot_re_tick_not_swallowed() {
    let store = boot().await;
    let ctrl = Arc::new(RequeueOnce::default());

    let driver = WatchDriver::new(
        RequeueOnceRef {
            inner: ctrl.clone(),
        },
        store.clone(),
        WatchDriverConfig {
            filter: KindFilter::kind("Pod"),
            debounce: Duration::from_millis(10),
            // Long fallback so the SECOND tick can ONLY come from the
            // requeue timer — never the blind fallback. If requeue were
            // swallowed, ticks would stay at 1.
            fallback_interval: Duration::from_secs(3600),
        },
    );
    let handle = driver.spawn();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Fire ONE Pod event → tick #1 (returns Requeue(20ms)).
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::namespaced("", "v1", "Pod", "default", "p1"),
            value: json!({"spec": {}}),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();

    // The single event drives tick #1; the requeue timer (20ms) then
    // drives tick #2. Wait well past both.
    tokio::time::sleep(Duration::from_millis(250)).await;

    let ticks = ctrl.ticks.load(Ordering::SeqCst);
    assert!(
        ticks >= 2,
        "requeue must arm a second tick (got {ticks}); the typed result was swallowed if this is 1"
    );

    handle.abort();
    let _ = handle.await;
    if let Ok(mesh) = Arc::try_unwrap(store) {
        let _ = mesh.terminate().await;
    }
}

// ──────────────────────────────────────────────────────────────────────
// (3) Typed error classification: Transient is retried; Declarative is
//     surfaced + NOT targeted-retried. Asserted directly on the typed
//     ControllerError classifier the loop consults (so the test is
//     deterministic + needs no real failing store).
// ──────────────────────────────────────────────────────────────────────

#[test]
fn store_error_classifies_transient_and_retries() {
    // A store/transport failure is Transient by the fleet classifier →
    // the loop schedules a targeted retry at ~1s instead of waiting the
    // flat 30s fallback.
    let e = ControllerError::Store(engenho_store::StoreError::ClientWriteFailed(
        "connection refused".into(),
    ));
    assert_eq!(e.classify(), shigoto_types::failure::FailureKind::Transient);
    assert_eq!(e.retry_after(), Some(Duration::from_secs(1)));
}

#[test]
fn invalid_resource_classifies_declarative_and_does_not_retry() {
    // A malformed declaration is Declarative → surfaced, NO targeted
    // retry (retry_after == None). The prior loop retried this forever.
    let e = ControllerError::InvalidResource("missing the attribute `template`".into());
    assert_eq!(
        e.classify(),
        shigoto_types::failure::FailureKind::Declarative
    );
    assert_eq!(e.retry_after(), None);
}

#[test]
fn internal_error_with_declarative_signature_is_surfaced() {
    // An Internal error whose message carries a documented Declarative
    // signature is classified Declarative (consumes the fleet classifier,
    // not a fork) → surfaced, not blind-retried.
    let e = ControllerError::Internal("schema validation failed: unknown attribute".into());
    assert_eq!(
        e.classify(),
        shigoto_types::failure::FailureKind::Declarative
    );
    assert_eq!(e.retry_after(), None);
}
