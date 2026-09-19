//! T2.5 — conditions and binds written at the revision read, against a real
//! in-memory `StoreMesh`.
//!
//!   * `upsert_condition_cas` writes a condition once, then proposes nothing
//!     while it still holds, keeps `lastTransitionTime` across a rewording,
//!     and leaves every other condition in place;
//!   * on a conflict it reads again and retries exactly once, on top of the
//!     concurrent write;
//!   * `bind_cas` binds only the pod as it was classified: one that moved is
//!     refused and stays unbound;
//!   * `mark_unschedulable_cas` stands down for a pod bound meanwhile;
//!   * driven by a `WatchDriver`, a controller that marks one unschedulable
//!     pod every tick ticks at most three times in two seconds. The
//!     scheduler that rewrote its condition every tick did 38 (2026-09-18).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use engenho_controllers::{
    BindOutcome, CasEnv, ConditionStatus, Controller, ControllerError, DEFAULT_SCHEDULER,
    DesiredCondition, Effect, KindFilter, PodSchedulingState, ReconcileOutcome, ReconcileReport,
    Refusal, Schedulable, StatusEditOutcome, StoreCasEnv, TickState, WatchDriver,
    WatchDriverConfig, bind_cas, mark_unschedulable_cas, upsert_condition_cas,
};
use engenho_store::{
    ApplyResult, InProcessRouter, ResourceKey, Revision, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::{Value, json};

const T1: &str = "2026-09-19T10:00:00Z";
const T2: &str = "2026-09-19T11:00:00Z";
const T3: &str = "2026-09-19T12:00:00Z";

async fn boot() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("t2-5-conditions").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

async fn teardown(store: Arc<StoreMesh>) {
    if let Ok(mesh) = Arc::try_unwrap(store) {
        let _ = mesh.terminate().await;
    }
}

fn pod_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

async fn put(store: &StoreMesh, key: &ResourceKey, value: Value) {
    store
        .propose(ResourceCommand::Put {
            key: key.clone(),
            value,
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

async fn operator_patch(store: &StoreMesh, key: &ResourceKey, patch: Value) {
    store
        .propose(ResourceCommand::patch(key.clone(), patch, Reason::Operator))
        .await
        .unwrap();
}

async fn read(store: &StoreMesh, key: &ResourceKey) -> Value {
    store.get(key).await.unwrap()
}

fn rv(object: &Value) -> String {
    object["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn conditions(object: &Value) -> Vec<Value> {
    object
        .pointer("/status/conditions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn pod_scheduled(object: &Value) -> Option<Value> {
    conditions(object)
        .into_iter()
        .find(|c| c["type"] == "PodScheduled")
}

fn unschedulable(message: &str) -> DesiredCondition {
    DesiredCondition {
        condition_type: "PodScheduled",
        status: ConditionStatus::False,
        reason: "Unschedulable",
        message: message.to_owned(),
    }
}

fn schedulable(key: &ResourceKey, pod: &Value) -> Schedulable {
    match PodSchedulingState::of(key, pod, DEFAULT_SCHEDULER) {
        PodSchedulingState::Schedulable(pod) => pod,
        other => panic!("expected Schedulable, got {other:?}"),
    }
}

/// A [`CasEnv`] over the store that makes a concurrent write in front of
/// each of its next `races` patches, and counts the patches it forwards.
struct Racing<'a> {
    store: &'a StoreMesh,
    inner: StoreCasEnv<'a>,
    races: AtomicUsize,
    race: fn(usize) -> Value,
    patches: AtomicUsize,
}

impl<'a> Racing<'a> {
    fn new(store: &'a StoreMesh, races: usize, race: fn(usize) -> Value) -> Self {
        Self {
            store,
            inner: StoreCasEnv::new(store, Reason::Scheduler),
            races: AtomicUsize::new(races),
            race,
            patches: AtomicUsize::new(0),
        }
    }

    fn patches(&self) -> usize {
        self.patches.load(Ordering::SeqCst)
    }
}

/// A concurrent writer that relabels the pod: new content every time.
fn relabel(n: usize) -> Value {
    json!({"metadata": {"labels": {"race": n.to_string()}}})
}

#[async_trait::async_trait]
impl CasEnv for Racing<'_> {
    async fn get(&self, key: &ResourceKey) -> Option<Value> {
        self.inner.get(key).await
    }

    async fn patch_at(
        &self,
        key: &ResourceKey,
        patch: Value,
        pinned: Revision,
    ) -> Result<ApplyResult, ControllerError> {
        let n = self.patches.fetch_add(1, Ordering::SeqCst) + 1;
        if self
            .races
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok()
        {
            operator_patch(self.store, key, (self.race)(n)).await;
        }
        self.inner.patch_at(key, patch, pinned).await
    }
}

// ── upsert_condition_cas ───────────────────────────────────────────────────

/// The hot-loop defense, end to end: the second assertion of the same
/// condition proposes nothing, so the pod's revision does not move and no
/// watch event fires.
#[tokio::test]
async fn a_condition_already_carried_is_not_proposed_again() {
    let store = boot().await;
    let key = pod_key("stuck");
    put(&store, &key, json!({"spec": {}})).await;
    let env = StoreCasEnv::new(&store, Reason::Scheduler);

    let first = upsert_condition_cas(&env, &key, &unschedulable("0/1 nodes"), T1)
        .await
        .unwrap();
    assert!(
        matches!(first, StatusEditOutcome::Proposed(Effect::Written(_))),
        "{first:?}"
    );
    let written = read(&store, &key).await;
    assert_eq!(
        pod_scheduled(&written).unwrap()["lastTransitionTime"],
        T1,
        "the first write records when the condition began"
    );

    let second = upsert_condition_cas(&env, &key, &unschedulable("0/1 nodes"), T2)
        .await
        .unwrap();
    assert_eq!(second, StatusEditOutcome::Unchanged);
    assert!(!second.changed());
    assert_eq!(
        rv(&read(&store, &key).await),
        rv(&written),
        "nothing proposed, so the revision did not move"
    );
    teardown(store).await;
}

/// A reworded condition is rewritten in place: its transition time is
/// carried, and the conditions other writers set stay where they were.
#[tokio::test]
async fn a_rewording_keeps_the_transition_time_and_every_other_condition() {
    let store = boot().await;
    let key = pod_key("reworded");
    let initialized = json!({"type": "Initialized", "status": "True"});
    put(
        &store,
        &key,
        json!({"spec": {}, "status": {"conditions": [initialized.clone()]}}),
    )
    .await;
    let env = StoreCasEnv::new(&store, Reason::Scheduler);

    upsert_condition_cas(&env, &key, &unschedulable("0/1 nodes"), T1)
        .await
        .unwrap();
    let outcome = upsert_condition_cas(&env, &key, &unschedulable("0/2 nodes"), T2)
        .await
        .unwrap();
    assert!(outcome.changed(), "{outcome:?}");

    assert_eq!(
        conditions(&read(&store, &key).await),
        vec![
            initialized,
            json!({"type": "PodScheduled", "status": "False", "reason": "Unschedulable",
                   "message": "0/2 nodes", "lastTransitionTime": T1}),
        ]
    );

    // A status flip is a transition.
    let scheduled = DesiredCondition {
        condition_type: "PodScheduled",
        status: ConditionStatus::True,
        reason: "Scheduled",
        message: String::new(),
    };
    upsert_condition_cas(&env, &key, &scheduled, T3)
        .await
        .unwrap();
    assert_eq!(
        pod_scheduled(&read(&store, &key).await).unwrap()["lastTransitionTime"],
        T3
    );
    teardown(store).await;
}

/// A conflict is retried once, on the object as re-read: the concurrent
/// writer's change and the condition both survive.
#[tokio::test]
async fn a_conflict_is_retried_once_on_top_of_the_concurrent_write() {
    let store = boot().await;
    let key = pod_key("raced");
    put(&store, &key, json!({"spec": {}})).await;
    let env = Racing::new(&store, 1, relabel);

    let outcome = upsert_condition_cas(&env, &key, &unschedulable("0/1 nodes"), T1)
        .await
        .unwrap();

    assert!(
        matches!(outcome, StatusEditOutcome::Proposed(Effect::Written(_))),
        "the retry lands: {outcome:?}"
    );
    assert_eq!(env.patches(), 2, "one conflicted attempt, one retry");
    let pod = read(&store, &key).await;
    assert_eq!(pod["metadata"]["labels"]["race"], "1", "the racer's write");
    assert_eq!(pod_scheduled(&pod).unwrap()["message"], "0/1 nodes");
    teardown(store).await;
}

/// Exactly one retry: a writer that moves the object in front of every
/// attempt gets a conflict back, after two attempts, and nothing written.
#[tokio::test]
async fn a_second_conflict_is_returned_after_exactly_two_attempts() {
    let store = boot().await;
    let key = pod_key("contended");
    put(&store, &key, json!({"spec": {}})).await;
    let env = Racing::new(&store, usize::MAX, relabel);

    let outcome = upsert_condition_cas(&env, &key, &unschedulable("0/1 nodes"), T1)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        StatusEditOutcome::Proposed(Effect::Rejected(Refusal::Conflict))
    );
    assert_eq!(env.patches(), 2);
    assert_eq!(pod_scheduled(&read(&store, &key).await), None);
    teardown(store).await;
}

/// The retry re-runs the edit: when the concurrent writer already set the
/// same condition, there is nothing left to write.
#[tokio::test]
async fn a_retry_that_finds_the_condition_already_written_proposes_nothing() {
    let store = boot().await;
    let key = pod_key("beaten");
    put(&store, &key, json!({"spec": {}})).await;
    fn same_condition(_: usize) -> Value {
        json!({"status": {"conditions": [
            {"type": "PodScheduled", "status": "False", "reason": "Unschedulable",
             "message": "0/1 nodes", "lastTransitionTime": "2026-09-19T09:00:00Z"}
        ]}})
    }
    let env = Racing::new(&store, 1, same_condition);

    let outcome = upsert_condition_cas(&env, &key, &unschedulable("0/1 nodes"), T1)
        .await
        .unwrap();

    assert_eq!(outcome, StatusEditOutcome::Unchanged);
    assert_eq!(
        env.patches(),
        1,
        "the retry found it done and proposed nothing"
    );
    teardown(store).await;
}

#[tokio::test]
async fn nothing_is_proposed_for_an_absent_object() {
    let store = boot().await;
    let env = Racing::new(&store, 0, relabel);
    let outcome = upsert_condition_cas(&env, &pod_key("ghost"), &unschedulable("m"), T1)
        .await
        .unwrap();
    assert_eq!(outcome, StatusEditOutcome::Absent);
    assert_eq!(env.patches(), 0);
    teardown(store).await;
}

// ── bind_cas ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_bind_at_the_revision_read_lands_and_names_pod_and_node() {
    let store = boot().await;
    let key = pod_key("placeable");
    put(&store, &key, json!({"spec": {}})).await;
    let pod = schedulable(&key, &read(&store, &key).await);

    let env = StoreCasEnv::new(&store, Reason::Scheduler);
    let outcome = bind_cas(&env, pod, "n1".to_owned()).await.unwrap();

    let BindOutcome::Bound(binding) = outcome else {
        panic!("expected Bound, got {outcome:?}");
    };
    assert_eq!(binding.pod_key(), &key);
    assert_eq!(binding.node_name(), "n1");
    assert_eq!(read(&store, &key).await["spec"]["nodeName"], "n1");
    teardown(store).await;
}

/// The node was chosen for the pod as it was read. A pod that changed
/// since (here a new nodeSelector) is not bound, and no Binding is minted.
#[tokio::test]
async fn a_pod_that_moved_since_it_was_read_is_not_bound() {
    let store = boot().await;
    let key = pod_key("moved");
    put(&store, &key, json!({"spec": {}})).await;
    let pod = schedulable(&key, &read(&store, &key).await);
    operator_patch(
        &store,
        &key,
        json!({"spec": {"nodeSelector": {"gpu": "true"}}}),
    )
    .await;

    let env = StoreCasEnv::new(&store, Reason::Scheduler);
    let outcome = bind_cas(&env, pod, "n1".to_owned()).await.unwrap();

    assert_eq!(outcome, BindOutcome::Refused(Refusal::Conflict));
    assert!(!outcome.effect().landed());
    assert_eq!(read(&store, &key).await["spec"].get("nodeName"), None);
    teardown(store).await;
}

// ── mark_unschedulable_cas ─────────────────────────────────────────────────

#[tokio::test]
async fn marking_writes_the_condition_and_a_pending_phase_once() {
    let store = boot().await;
    let key = pod_key("unplaceable");
    put(&store, &key, json!({"spec": {}})).await;
    let env = StoreCasEnv::new(&store, Reason::Scheduler);
    let pod = schedulable(&key, &read(&store, &key).await);

    let first = mark_unschedulable_cas(&env, &pod, DEFAULT_SCHEDULER, "0/1 nodes".into(), T1)
        .await
        .unwrap();
    assert!(first.changed(), "{first:?}");
    let marked = read(&store, &key).await;
    assert_eq!(marked["status"]["phase"], "Pending");
    assert_eq!(
        pod_scheduled(&marked).unwrap(),
        json!({"type": "PodScheduled", "status": "False", "reason": "Unschedulable",
               "message": "0/1 nodes", "lastTransitionTime": T1})
    );

    let again = mark_unschedulable_cas(&env, &pod, DEFAULT_SCHEDULER, "0/1 nodes".into(), T2)
        .await
        .unwrap();
    assert_eq!(again, StatusEditOutcome::Unchanged);
    assert_eq!(rv(&read(&store, &key).await), rv(&marked));
    teardown(store).await;
}

/// The one retry must not mark a pod that was bound in the meantime: the
/// re-read pod is no longer ours to mark.
#[tokio::test]
async fn a_pod_bound_while_it_was_being_marked_is_left_alone() {
    let store = boot().await;
    let key = pod_key("bound-meanwhile");
    put(&store, &key, json!({"spec": {}})).await;
    let pod = schedulable(&key, &read(&store, &key).await);
    fn bind_elsewhere(_: usize) -> Value {
        json!({"spec": {"nodeName": "n2"}})
    }
    let env = Racing::new(&store, 1, bind_elsewhere);

    let outcome = mark_unschedulable_cas(&env, &pod, DEFAULT_SCHEDULER, "0/1 nodes".into(), T1)
        .await
        .unwrap();

    assert_eq!(outcome, StatusEditOutcome::Superseded);
    assert_eq!(env.patches(), 1);
    let now = read(&store, &key).await;
    assert_eq!(now["spec"]["nodeName"], "n2");
    assert_eq!(pod_scheduled(&now), None);
    teardown(store).await;
}

// ── driven: the scheduler's loop, reduced to its writes ────────────────────

/// Marks every schedulable pod unschedulable, every tick, at a fresh
/// instant: the scheduler's tick with no node that fits.
struct Marker {
    store: Arc<StoreMesh>,
    ticks: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Controller for Marker {
    fn name(&self) -> &'static str {
        "t2_5_marker"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        self.ticks.fetch_add(1, Ordering::SeqCst);
        let env = StoreCasEnv::new(&self.store, Reason::Scheduler);
        let now = engenho_types::time::now_micro_time_utc();
        let mut report = ReconcileReport::default();
        for (key, pod) in self.store.list("", "v1", "Pod", None).await {
            if let PodSchedulingState::Schedulable(pod) =
                PodSchedulingState::of(&key, &pod, DEFAULT_SCHEDULER)
            {
                let outcome = mark_unschedulable_cas(
                    &env,
                    &pod,
                    DEFAULT_SCHEDULER,
                    "0/1 nodes are available: 1 node(s) didn't match Pod's node selector.".into(),
                    &now,
                )
                .await?;
                report.record(outcome.effect());
            }
        }
        Ok(report.into())
    }
}

/// Plan T2.5's gate: one unschedulable pod, a watch-driven controller that
/// re-derives its condition every tick, at most 3 ticks in 2 s. The pod's
/// creation wakes the first tick, which writes the condition; that write's
/// event wakes one more, which finds it current and writes nothing, and the
/// loop goes quiet.
#[tokio::test]
async fn one_unschedulable_pod_ticks_at_most_three_times_in_two_seconds() {
    let store = boot().await;
    let key = pod_key("stuck");

    let ticks = Arc::new(AtomicUsize::new(0));
    let driver = WatchDriver::new(
        Marker {
            store: store.clone(),
            ticks: ticks.clone(),
        },
        store.clone(),
        WatchDriverConfig {
            filter: KindFilter::kind("Pod"),
            // The production debounce (WatchDriverConfig::default()).
            debounce: Duration::from_millis(50),
            fallback_interval: Duration::from_secs(3600),
            stuck_tick_after: Duration::from_secs(120),
            tick_state: TickState::Stateful,
        },
    );
    let handle = tokio::spawn(driver.run());
    // Let the driver subscribe; the driver ticks on events, not at start.
    tokio::time::sleep(Duration::from_millis(50)).await;
    put(
        &store,
        &key,
        json!({"spec": {"nodeSelector": {"gpu": "true"}}}),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    handle.abort();
    let _ = handle.await;

    let count = ticks.load(Ordering::SeqCst);
    let pod = read(&store, &key).await;
    assert!(
        pod_scheduled(&pod).is_some(),
        "the controller marked the pod (the loop ran): {pod}"
    );
    assert!(
        count <= 3,
        "one unschedulable pod woke its controller {count} times in 2 s (at most 3)"
    );
    teardown(store).await;
}
