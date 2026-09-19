//! I3 — a writer leaves a Terminating child alone.
//!
//! Since a delete carries its clock (store T3.6), a child that bears a
//! finalizer goes Terminating instead of vanishing, and stays in the store
//! until the finalizer clears. Every child writer here was built when that
//! could not happen, and each met it one of three ways:
//!
//! * **re-delete** — the writer deleted it again on every tick. The store
//!   answers `NoOp` (no revision, no event), so the count stayed honest
//!   (T1.8), but each tick still cost a Raft entry per Terminating child;
//! * **overwrite** — the writer names children deterministically, so a
//!   parent recreated under the same name while its old children waited on
//!   finalizers `Put` a fresh child over the Terminating one. The new body
//!   carries no `deletionTimestamp`: the store replaced a Terminating object
//!   with a live one, owned by someone else;
//! * **miscount** — a `ReplicaSet` counted its Terminating pod as a replica,
//!   so it ran a replica short, and on scale-down it evicted a LIVE pod in
//!   its place.
//!
//! "Left alone" is measured where it happens: the Terminating object's
//! bytes, and the Raft log. A re-delete leaves the catalog byte-identical,
//! so only `last_applied_index` sees it.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use engenho_controllers::job::FrozenClock;
use engenho_controllers::meta::ObjectMeta;
use engenho_controllers::{
    Controller, CronJobController, DaemonSetController, DeploymentController, GcController,
    JobController, NamespaceController, OwnerReference, ReplicaSetController,
    StatefulSetController, is_owned_by, set_owner_reference,
};
use engenho_store::command::{Reason, ResourceCommand, ResourceOp};
use engenho_store::{InProcessRouter, ResourceKey, StoreMesh, default_config};

const HOLD: &str = "example.com/hold";

async fn boot(tag: &str) -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config(tag).unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

async fn put(store: &StoreMesh, key: &ResourceKey, value: Value) -> Value {
    store
        .propose(ResourceCommand::put(key.clone(), value, Reason::Operator))
        .await
        .unwrap();
    store.get(key).await.expect("stored")
}

async fn patch(store: &StoreMesh, key: &ResourceKey, body: Value) {
    store
        .propose(ResourceCommand::patch(key.clone(), body, Reason::Operator))
        .await
        .unwrap();
}

/// Give the object at `key` a finalizer, then delete it: it goes
/// Terminating and stays. Returns it as it now stands.
async fn hold_and_delete(store: &StoreMesh, key: &ResourceKey) -> Value {
    patch(store, key, json!({"metadata": {"finalizers": [HOLD]}})).await;
    let applied = store
        .propose(ResourceCommand::delete(key.clone(), Reason::Operator))
        .await
        .unwrap();
    assert_eq!(applied.op, ResourceOp::DeletionPending, "{}", key.label());
    let terminating = store.get(key).await.expect("held by its finalizer");
    assert!(terminating.is_terminating(), "{terminating}");
    terminating
}

/// Clear the finalizer on a Terminating object: the store removes it.
async fn release(store: &StoreMesh, key: &ResourceKey) {
    patch(store, key, json!({"metadata": {"finalizers": []}})).await;
    assert!(store.get(key).await.is_none(), "{} released", key.label());
}

/// Tick `c` three more times and assert it proposed nothing at all: no
/// Raft entry (a NoOp delete is one), and no change reported.
async fn proposes_nothing(store: &StoreMesh, c: &impl Controller, why: &str) {
    let before = store.last_applied_index().await;
    for _ in 0..3 {
        let out = c.tick().await.unwrap();
        assert_eq!(out.objects_changed, 0, "{why}: a change was reported");
    }
    assert_eq!(
        store.last_applied_index().await,
        before,
        "{why}: the controller proposed a write"
    );
}

fn pod(ns: &str, name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", ns, name)
}

fn uid(v: &Value) -> String {
    v.uid().expect("uid").to_owned()
}

fn template(app: &str) -> Value {
    json!({
        "metadata": {"labels": {"app": app}},
        "spec": {"containers": [{"name": "c", "image": "img"}]}
    })
}

fn replicaset(name: &str, replicas: i64) -> Value {
    json!({
        "kind": "ReplicaSet", "apiVersion": "apps/v1",
        "metadata": {"name": name, "namespace": "default"},
        "spec": {
            "replicas": replicas,
            "selector": {"matchLabels": {"app": name}},
            "template": template(name)
        }
    })
}

fn statefulset(name: &str, replicas: i64) -> Value {
    json!({
        "kind": "StatefulSet", "apiVersion": "apps/v1",
        "metadata": {"name": name, "namespace": "default"},
        "spec": {"replicas": replicas, "template": template(name)}
    })
}

fn daemonset(name: &str) -> Value {
    json!({
        "kind": "DaemonSet", "apiVersion": "apps/v1",
        "metadata": {"name": name, "namespace": "default"},
        "spec": {"template": template(name)}
    })
}

fn deployment(name: &str) -> Value {
    json!({
        "kind": "Deployment", "apiVersion": "apps/v1",
        "metadata": {"name": name, "namespace": "default"},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": name}},
            "template": template(name)
        }
    })
}

fn job(name: &str) -> Value {
    json!({
        "kind": "Job", "apiVersion": "batch/v1",
        "metadata": {"name": name, "namespace": "default"},
        "spec": {"completions": 1, "parallelism": 1, "template": template(name)}
    })
}

async fn node(store: &StoreMesh, name: &str) -> ResourceKey {
    let key = ResourceKey::cluster_scoped("", "v1", "Node", name);
    put(
        store,
        &key,
        json!({"kind": "Node", "apiVersion": "v1", "metadata": {"name": name}}),
    )
    .await;
    key
}

fn rs_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("apps", "v1", "ReplicaSet", "default", name)
}

// ── ReplicaSet: counts and evicts only live pods ─────────────────────────

/// A Terminating pod is not a replica: the set creates its replacement at
/// the next free index, and the Terminating pod keeps its name and bytes.
#[tokio::test]
async fn a_replicaset_replaces_a_terminating_pod_and_leaves_it_alone() {
    let store = boot("i3-rs-replace").await;
    put(&store, &rs_key("web"), replicaset("web", 2)).await;
    let c = ReplicaSetController::new(store.clone(), None);
    c.tick().await.unwrap();

    let terminating = hold_and_delete(&store, &pod("default", "web-0")).await;
    c.tick().await.unwrap();

    let replacement = store
        .get(&pod("default", "web-2"))
        .await
        .expect("the Terminating pod was replaced at the next free index");
    assert!(!replacement.is_terminating());
    assert!(store.get(&pod("default", "web-1")).await.is_some());
    assert_eq!(
        store.get(&pod("default", "web-0")).await.as_ref(),
        Some(&terminating),
        "the Terminating pod is untouched"
    );
    proposes_nothing(&store, &c, "a converged set with a Terminating pod").await;
}

/// Scale-down: the evicted pod goes Terminating, and it is not deleted
/// again on every tick after; the status counts only the live replica.
#[tokio::test]
async fn a_replicaset_does_not_delete_its_terminating_pod_again() {
    let store = boot("i3-rs-scale-down").await;
    put(&store, &rs_key("web"), replicaset("web", 2)).await;
    let c = ReplicaSetController::new(store.clone(), None);
    c.tick().await.unwrap();
    for name in ["web-0", "web-1"] {
        patch(
            &store,
            &pod("default", name),
            json!({"metadata": {"finalizers": [HOLD]}}),
        )
        .await;
    }

    patch(&store, &rs_key("web"), json!({"spec": {"replicas": 1}})).await;
    c.tick().await.unwrap();
    let evicted = store
        .get(&pod("default", "web-1"))
        .await
        .expect("held by its finalizer");
    assert!(evicted.is_terminating(), "the highest name was evicted");
    assert!(
        !store
            .get(&pod("default", "web-0"))
            .await
            .unwrap()
            .is_terminating()
    );
    c.tick().await.unwrap(); // status settles

    proposes_nothing(&store, &c, "a Terminating pod past the desired count").await;
    assert_eq!(store.get(&pod("default", "web-1")).await.unwrap(), evicted);
    let rs = store.get(&rs_key("web")).await.unwrap();
    assert_eq!(
        rs["status"]["replicas"],
        json!(1),
        "a Terminating pod is not a replica: {rs}"
    );
}

/// With a Terminating pod among three, scaling to two evicts nothing: the
/// two live pods ARE the desired set. Counting the Terminating pod evicted
/// a live one and left the set a replica short.
#[tokio::test]
async fn a_replicaset_never_evicts_a_live_pod_in_place_of_a_terminating_one() {
    let store = boot("i3-rs-evict").await;
    put(&store, &rs_key("web"), replicaset("web", 3)).await;
    let c = ReplicaSetController::new(store.clone(), None);
    c.tick().await.unwrap();
    hold_and_delete(&store, &pod("default", "web-0")).await;

    patch(&store, &rs_key("web"), json!({"spec": {"replicas": 2}})).await;
    c.tick().await.unwrap();

    for name in ["web-1", "web-2"] {
        let p = store
            .get(&pod("default", name))
            .await
            .unwrap_or_else(|| panic!("live pod {name} was evicted"));
        assert!(!p.is_terminating(), "live pod {name} was evicted");
    }
}

// ── StatefulSet / DaemonSet: a Terminating pod is not deleted again ──────

#[tokio::test]
async fn a_statefulset_does_not_delete_its_terminating_pod_again() {
    let store = boot("i3-sts").await;
    let sts = ResourceKey::namespaced("apps", "v1", "StatefulSet", "default", "db");
    put(&store, &sts, statefulset("db", 2)).await;
    let c = StatefulSetController::new(store.clone(), None);
    c.tick().await.unwrap();
    patch(
        &store,
        &pod("default", "db-1"),
        json!({"metadata": {"finalizers": [HOLD]}}),
    )
    .await;

    patch(&store, &sts, json!({"spec": {"replicas": 1}})).await;
    c.tick().await.unwrap();
    let evicted = store.get(&pod("default", "db-1")).await.unwrap();
    assert!(evicted.is_terminating(), "ordinal 1 was scaled away");
    c.tick().await.unwrap(); // status settles

    proposes_nothing(&store, &c, "a Terminating ordinal past the desired count").await;
    assert_eq!(store.get(&pod("default", "db-1")).await.unwrap(), evicted);
}

#[tokio::test]
async fn a_daemonset_does_not_delete_its_terminating_pod_again() {
    let store = boot("i3-ds").await;
    node(&store, "node-a").await;
    let gone = node(&store, "node-b").await;
    let ds = ResourceKey::namespaced("apps", "v1", "DaemonSet", "default", "agent");
    put(&store, &ds, daemonset("agent")).await;
    let c = DaemonSetController::new(store.clone(), None);
    c.tick().await.unwrap();
    patch(
        &store,
        &pod("default", "agent-node-b"),
        json!({"metadata": {"finalizers": [HOLD]}}),
    )
    .await;

    store
        .propose(ResourceCommand::delete(gone, Reason::Operator))
        .await
        .unwrap();
    c.tick().await.unwrap();
    let evicted = store.get(&pod("default", "agent-node-b")).await.unwrap();
    assert!(evicted.is_terminating(), "its node is gone");
    c.tick().await.unwrap(); // status settles

    proposes_nothing(&store, &c, "a Terminating pod on a removed node").await;
    assert_eq!(
        store.get(&pod("default", "agent-node-b")).await.unwrap(),
        evicted
    );
}

// ── every templated writer: a held name is never overwritten ─────────────

/// A parent deleted and recreated under the same name while its old child
/// waits on a finalizer. The recreated parent wants the same child name;
/// the Terminating child keeps it, untouched, until the finalizer clears,
/// and then the new parent creates its own.
async fn a_held_name_is_never_overwritten<C: Controller>(
    store: &StoreMesh,
    c: &C,
    parent: &ResourceKey,
    parent_value: Value,
    child: &ResourceKey,
) {
    put(store, parent, parent_value.clone()).await;
    c.tick().await.unwrap();
    assert!(
        store.get(child).await.is_some(),
        "{} created",
        child.label()
    );

    patch(store, child, json!({"metadata": {"finalizers": [HOLD]}})).await;
    store
        .propose(ResourceCommand::delete(parent.clone(), Reason::Operator))
        .await
        .unwrap();
    let terminating = hold_and_delete(store, child).await;

    let reborn = uid(&put(store, parent, parent_value).await);
    assert_ne!(reborn, terminating["metadata"]["ownerReferences"][0]["uid"]);
    c.tick().await.unwrap();
    c.tick().await.unwrap();
    assert_eq!(
        store.get(child).await.as_ref(),
        Some(&terminating),
        "{}: the Terminating child was overwritten",
        child.label()
    );

    release(store, child).await;
    c.tick().await.unwrap();
    let fresh = store
        .get(child)
        .await
        .expect("created once the name is free");
    assert!(is_owned_by(&fresh, &reborn), "{fresh}");
    assert!(!fresh.is_terminating(), "{fresh}");
}

#[tokio::test]
async fn a_replicaset_never_overwrites_a_terminating_pod() {
    let store = boot("i3-held-rs").await;
    let c = ReplicaSetController::new(store.clone(), None);
    a_held_name_is_never_overwritten(
        &store,
        &c,
        &rs_key("web"),
        replicaset("web", 1),
        &pod("default", "web-0"),
    )
    .await;
}

#[tokio::test]
async fn a_statefulset_never_overwrites_a_terminating_pod() {
    let store = boot("i3-held-sts").await;
    let c = StatefulSetController::new(store.clone(), None);
    a_held_name_is_never_overwritten(
        &store,
        &c,
        &ResourceKey::namespaced("apps", "v1", "StatefulSet", "default", "db"),
        statefulset("db", 1),
        &pod("default", "db-0"),
    )
    .await;
}

#[tokio::test]
async fn a_daemonset_never_overwrites_a_terminating_pod() {
    let store = boot("i3-held-ds").await;
    node(&store, "node-a").await;
    let c = DaemonSetController::new(store.clone(), None);
    a_held_name_is_never_overwritten(
        &store,
        &c,
        &ResourceKey::namespaced("apps", "v1", "DaemonSet", "default", "agent"),
        daemonset("agent"),
        &pod("default", "agent-node-a"),
    )
    .await;
}

#[tokio::test]
async fn a_job_never_overwrites_a_terminating_pod() {
    let store = boot("i3-held-job").await;
    let c = JobController::new(store.clone(), None);
    a_held_name_is_never_overwritten(
        &store,
        &c,
        &ResourceKey::namespaced("batch", "v1", "Job", "default", "pi"),
        job("pi"),
        &pod("default", "pi-0"),
    )
    .await;
}

#[tokio::test]
async fn a_deployment_never_overwrites_a_terminating_replicaset() {
    let store = boot("i3-held-deploy").await;
    let c = DeploymentController::new(store.clone(), None);
    let parent = ResourceKey::namespaced("apps", "v1", "Deployment", "default", "api");
    // The ReplicaSet is named by the template's hash: learn it from the
    // first incarnation.
    put(&store, &parent, deployment("api")).await;
    c.tick().await.unwrap();
    let rs = store
        .list("apps", "v1", "ReplicaSet", Some("default"))
        .await;
    let [(child, _)] = rs.as_slice() else {
        panic!("one ReplicaSet: {rs:?}");
    };
    store
        .propose(ResourceCommand::delete(parent.clone(), Reason::Operator))
        .await
        .unwrap();
    store
        .propose(ResourceCommand::delete(child.clone(), Reason::Operator))
        .await
        .unwrap();

    a_held_name_is_never_overwritten(&store, &c, &parent, deployment("api"), child).await;
}

// ── raw controllers: cronjob, namespace, gc ──────────────────────────────

/// `Replace` deletes the active Job before creating the next. A Job
/// already Terminating was deleted before: it gets no second delete, and
/// the new Job is still created.
#[tokio::test]
async fn a_cronjob_replace_does_not_delete_a_terminating_job_again() {
    let store = boot("i3-cronjob").await;
    let cj_key = ResourceKey::namespaced("batch", "v1", "CronJob", "default", "tick");
    let cj = put(
        &store,
        &cj_key,
        json!({
            "kind": "CronJob", "apiVersion": "batch/v1",
            "metadata": {
                "name": "tick", "namespace": "default",
                "creationTimestamp": "1970-01-01T00:00:00Z"
            },
            "spec": {
                "schedule": "* * * * *",
                "concurrencyPolicy": "Replace",
                "jobTemplate": {"spec": {"template": template("tick")}}
            }
        }),
    )
    .await;
    let old = ResourceKey::namespaced("batch", "v1", "Job", "default", "tick-60");
    let mut old_job = json!({
        "kind": "Job", "apiVersion": "batch/v1",
        "metadata": {"name": "tick-60", "namespace": "default"},
        "status": {"active": 1}
    });
    set_owner_reference(
        &mut old_job,
        OwnerReference {
            api_version: "batch/v1".into(),
            kind: "CronJob".into(),
            name: "tick".into(),
            uid: uid(&cj),
            controller: true,
            block_owner_deletion: true,
        },
    )
    .unwrap();
    put(&store, &old, old_job).await;
    let terminating = hold_and_delete(&store, &old).await;

    let c = CronJobController::new(store.clone(), Arc::new(FrozenClock::at(120_000)), None);
    let before = store.last_applied_index().await;
    let out = c.tick().await.unwrap();

    assert_eq!(out.objects_changed, 1, "the CronJob fired");
    assert!(
        store
            .get(&ResourceKey::namespaced(
                "batch", "v1", "Job", "default", "tick-120"
            ))
            .await
            .is_some(),
        "the next Job was created"
    );
    assert_eq!(store.get(&old).await.unwrap(), terminating);
    assert_eq!(
        store.last_applied_index().await - before,
        2,
        "two entries: the new Job and the CronJob's status, no second delete"
    );
}

/// The cascade stamps a finalizer-bearing child Terminating once, then
/// waits for it: it is not deleted again on every tick, and the namespace
/// stays until it goes.
#[tokio::test]
async fn a_namespace_cascade_does_not_delete_a_terminating_child_again() {
    let store = boot("i3-namespace").await;
    let ns = ResourceKey::cluster_scoped("", "v1", "Namespace", "gone");
    put(
        &store,
        &ns,
        json!({"kind": "Namespace", "metadata": {"name": "gone", "finalizers": ["kubernetes"]}}),
    )
    .await;
    store
        .propose(ResourceCommand::delete(ns.clone(), Reason::Operator))
        .await
        .unwrap();
    let held = ResourceKey::namespaced("", "v1", "ConfigMap", "gone", "held");
    put(
        &store,
        &held,
        json!({"kind": "ConfigMap", "metadata": {"name": "held", "finalizers": [HOLD]}}),
    )
    .await;

    let c = NamespaceController::new(store.clone(), None);
    c.tick().await.unwrap();
    let terminating = store.get(&held).await.expect("held by its finalizer");
    assert!(terminating.is_terminating(), "the cascade deleted it once");

    proposes_nothing(&store, &c, "a namespace waiting on a Terminating child").await;
    assert_eq!(store.get(&held).await.unwrap(), terminating);
    assert!(store.get(&ns).await.is_some(), "the namespace waits");

    release(&store, &held).await;
    c.tick().await.unwrap();
    assert!(store.get(&ns).await.is_none(), "empty, so it goes");
}

/// gc deletes a finalizer-bearing orphan once (Terminating: a real
/// change) and then leaves it to its finalizer: no second delete, nothing
/// counted.
#[tokio::test]
async fn gc_does_not_delete_a_terminating_orphan_again() {
    let store = boot("i3-gc").await;
    let orphan = pod("default", "orphan");
    let mut value = json!({
        "kind": "Pod", "apiVersion": "v1",
        "metadata": {"name": "orphan", "finalizers": [HOLD]}
    });
    set_owner_reference(
        &mut value,
        OwnerReference {
            api_version: "apps/v1".into(),
            kind: "ReplicaSet".into(),
            name: "never-stored".into(),
            uid: "uid-never-stored".into(),
            controller: true,
            block_owner_deletion: true,
        },
    )
    .unwrap();
    put(&store, &orphan, value).await;

    let gc = GcController::new(store.clone(), None);
    let first = gc.tick().await.unwrap();
    assert_eq!(
        first.objects_changed, 1,
        "the Terminating stamp is a change"
    );
    let terminating = store.get(&orphan).await.unwrap();
    assert!(terminating.is_terminating());

    proposes_nothing(&store, &gc, "gc over a Terminating orphan").await;
    assert_eq!(store.get(&orphan).await.unwrap(), terminating);
}
