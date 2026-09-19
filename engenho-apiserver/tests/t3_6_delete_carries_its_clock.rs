//! T3.6 (apiserver half) — a DELETE carries its clock, whatever the object
//! looked like when the apiserver read it.
//!
//! The apiserver used to freeze a `deletionTimestamp` into the Delete command
//! ONLY when its own pre-read of the object showed finalizers
//! (`prior.filter(object_has_finalizers)`). That read and the store's apply are
//! two different moments. A finalizer that lands between them turned the
//! DELETE into a silent `NoOp`: the store saw finalizers, had no timestamp to
//! stamp, and kept the object live — while the apiserver answered success.
//! Upstream decides inside the storage update itself, so the delete always
//! takes effect: the object is removed, or it goes Terminating.
//!
//! The race is staged deterministically, not by hoping for a scheduler
//! interleaving: on the current-thread runtime, a future polled by hand runs
//! until its first `Pending` and no other task runs until the test itself
//! yields. So the test
//!
//!   1. polls the finalizer write once — it is handed to Raft, not applied;
//!   2. asserts the store still shows the object WITHOUT that write, so the
//!      DELETE's read necessarily predates it;
//!   3. polls the DELETE once — it reads, then proposes behind the write;
//!   4. drives both to completion (Raft applies the write, then the delete).
//!
//! Step 2 is the guard against a vacuous pass: if a runtime change let Raft
//! apply the write before the DELETE read, that assertion fails instead of the
//! test going green without exercising the gap.
//!
//! Tier: a behaviour test (a gate), not a type. The store-side destination —
//! `ResourceCommand::delete_at` taking a required `String` so a clockless
//! delete does not compile — belongs to the store crate.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use engenho_apiserver::params::DryRun;
use engenho_apiserver::{ApiError, ResourceHandler, StoreBackedHandler};
use engenho_store::{
    ApplyResult, InProcessRouter, Reason, ResourceCommand, ResourceKey, ResourceOp, StoreMesh,
    default_config,
};
use engenho_types::auth::UserInfo;
use serde_json::{Value, json};

const NS: &str = "default";
const FINALIZER: &str = "example.com/hold";

async fn boot_store(name: &str) -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config(name).expect("store config");
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .expect("store starts"),
    );
    store.initialize_singleton().await.expect("singleton init");
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

fn configmaps(store: &Arc<StoreMesh>) -> StoreBackedHandler {
    StoreBackedHandler::for_core_kind(store.clone(), "ConfigMap", true)
        .expect("ConfigMap is a cataloged core/v1 namespaced kind")
}

fn cm_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "ConfigMap", NS, name)
}

fn configmap(name: &str, finalizers: &[&str]) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": name, "namespace": NS, "finalizers": finalizers },
        "data": { "k": "v" }
    })
}

fn deletion_timestamp(v: &Value) -> Option<&str> {
    v.pointer("/metadata/deletionTimestamp")
        .and_then(Value::as_str)
}

fn finalizers(v: &Value) -> Vec<&str> {
    v.pointer("/metadata/finalizers")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default()
}

/// Poll `fut` exactly once with the current task's context and report whether
/// it finished. Nothing else on the current-thread runtime runs meanwhile.
async fn poll_once<F: Future + Unpin>(fut: &mut F) -> Poll<F::Output> {
    std::future::poll_fn(|cx| Poll::Ready(Pin::new(&mut *fut).poll(cx))).await
}

/// Stage the race: `write` is handed to Raft first, the DELETE of `name` reads
/// the store before `write` applies and proposes behind it. Returns both
/// outcomes once Raft has applied them in that order.
async fn delete_racing(
    store: &Arc<StoreMesh>,
    h: &StoreBackedHandler,
    name: &str,
    write: ResourceCommand,
    live_before_write: Option<&Value>,
) -> (ApplyResult, Value) {
    let user = UserInfo::default();
    let mut write = Box::pin(store.propose(write));
    assert!(
        poll_once(&mut write).await.is_pending(),
        "the concurrent write must be in flight, not applied, when the DELETE reads"
    );
    assert_eq!(
        store.get(&cm_key(name)).await.as_ref(),
        live_before_write,
        "the store must not show the concurrent write yet — otherwise the \
         DELETE's read would see it and the race would not be exercised"
    );
    let mut delete = h.delete(Some(NS), name, &user, DryRun::Off);
    assert!(
        poll_once(&mut delete).await.is_pending(),
        "the DELETE must have read and proposed, then be waiting on Raft"
    );
    let (written, deleted) = tokio::join!(write, delete);
    (
        written.expect("the concurrent write commits"),
        deleted.expect("the DELETE succeeds"),
    )
}

// ── Regression: the delete is decided at apply time, not at read time ──────

/// A finalizer added between the apiserver's read and its proposal. Before
/// T3.6 the DELETE was committed clockless, the store saw the finalizer, kept
/// the object live, and the client was told the delete succeeded.
#[tokio::test]
async fn a_finalizer_added_after_the_read_still_sends_the_object_terminating() {
    let store = boot_store("t36-finalizer-race").await;
    let h = configmaps(&store);
    let user = UserInfo::default();
    h.create(Some(NS), configmap("racer", &[]), &user, DryRun::Off)
        .await
        .expect("create a finalizer-free ConfigMap");
    let live = store.get(&cm_key("racer")).await.expect("created");
    assert!(finalizers(&live).is_empty(), "starts finalizer-free");

    let add_finalizer = ResourceCommand::patch(
        cm_key("racer"),
        json!({ "metadata": { "finalizers": [FINALIZER] } }),
        Reason::Controller,
    );
    let (patched, body) = delete_racing(&store, &h, "racer", add_finalizer, Some(&live)).await;
    assert_eq!(
        patched.op,
        ResourceOp::Patched,
        "the finalizer write applied first (the DELETE did not remove the object ahead of it)"
    );

    let after = store
        .get(&cm_key("racer"))
        .await
        .expect("the finalizer holds the object");
    assert!(
        deletion_timestamp(&after).is_some(),
        "an accepted DELETE on a finalizer-bearing object must leave it Terminating, \
         never live: {after}"
    );
    assert_eq!(finalizers(&after), vec![FINALIZER]);
    assert!(
        deletion_timestamp(&body).is_some(),
        "the DELETE response is the Terminating object, as upstream returns it: {body}"
    );
}

/// The object did not exist when the apiserver read it, and was created with a
/// finalizer before the delete applied. Before T3.6 the DELETE answered
/// `Status: Success` while the object stayed live.
#[tokio::test]
async fn an_object_created_with_a_finalizer_after_the_read_still_goes_terminating() {
    let store = boot_store("t36-create-race").await;
    let h = configmaps(&store);

    let create = ResourceCommand::put(
        cm_key("late"),
        configmap("late", &[FINALIZER]),
        Reason::Controller,
    );
    let (created, body) = delete_racing(&store, &h, "late", create, None).await;
    assert_eq!(created.op, ResourceOp::Created);

    let after = store
        .get(&cm_key("late"))
        .await
        .expect("the finalizer holds the object");
    assert!(
        deletion_timestamp(&after).is_some(),
        "the DELETE applied after the create, so the object must be Terminating: {after}"
    );
    assert_eq!(
        body.get("kind").and_then(Value::as_str),
        Some("ConfigMap"),
        "the DELETE did take effect, so it answers with the Terminating object, \
         not a Status for an absent name: {body}"
    );
    assert!(deletion_timestamp(&body).is_some(), "{body}");
}

// ── Preservation: the clock now always travels; nothing else changes ──────

/// A finalizer-free object is still removed immediately, even though its
/// Delete now carries a timestamp.
#[tokio::test]
async fn a_finalizer_free_delete_still_removes_the_object_immediately() {
    let store = boot_store("t36-plain").await;
    let h = configmaps(&store);
    let user = UserInfo::default();
    h.create(Some(NS), configmap("plain", &[]), &user, DryRun::Off)
        .await
        .expect("create");

    let body = h
        .delete(Some(NS), "plain", &user, DryRun::Off)
        .await
        .expect("delete");
    assert_eq!(
        body.pointer("/metadata/name").and_then(Value::as_str),
        Some("plain")
    );
    assert!(
        store.get(&cm_key("plain")).await.is_none(),
        "removed, not held Terminating"
    );
    assert!(matches!(
        h.get(Some(NS), "plain").await,
        Err(ApiError::NotFound(_))
    ));
}

/// A repeated DELETE on a Terminating object carries a fresh clock but does
/// not restamp it: the first `deletionTimestamp` stands, and emptying the
/// finalizers removes the object.
#[tokio::test]
async fn a_repeated_delete_keeps_the_first_deletion_timestamp() {
    let store = boot_store("t36-repeat").await;
    let h = configmaps(&store);
    let user = UserInfo::default();
    h.create(
        Some(NS),
        configmap("held", &[FINALIZER]),
        &user,
        DryRun::Off,
    )
    .await
    .expect("create");

    let first = h
        .delete(Some(NS), "held", &user, DryRun::Off)
        .await
        .expect("first delete");
    let stamped = deletion_timestamp(&first)
        .expect("Terminating after the first delete")
        .to_string();
    // The RFC3339 clock has one-second resolution; cross a second so a
    // restamp would be visible.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    h.delete(Some(NS), "held", &user, DryRun::Off)
        .await
        .expect("second delete");
    let live = store.get(&cm_key("held")).await.expect("still held");
    assert_eq!(deletion_timestamp(&live), Some(stamped.as_str()));

    store
        .propose(ResourceCommand::patch(
            cm_key("held"),
            json!({ "metadata": { "finalizers": [] } }),
            Reason::Controller,
        ))
        .await
        .expect("release the finalizer");
    assert!(
        store.get(&cm_key("held")).await.is_none(),
        "releasing the last finalizer on a Terminating object removes it"
    );
}
