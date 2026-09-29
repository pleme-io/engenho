//! R16 — a template change reaches the pods of a `DaemonSet` and a
//! `StatefulSet`.
//!
//! Both controllers keep pods under stable names (`{ds}-{node}`,
//! `{sts}-{ordinal}`) and used to decide a pod's fate by that name alone, so
//! a template change reached no running pod. Measured on a native node when
//! the daemon restarted onto a new release: every `DaemonSet` was re-rendered
//! with the new closure as its image, the existing pods kept the old one, the
//! old closure was garbage-collected, and the pods sat `ContainerCreating`
//! for five and a half hours. Deleting them by hand was the only way out.
//!
//! Upstream's contract, which these cases pin:
//!
//! * every pod carries `controller-revision-hash`, the revision of the
//!   template it was built from;
//! * `updateStrategy` defaults to `RollingUpdate` with `maxUnavailable: 1`;
//! * an out-of-date pod that is not Ready is replaced at once (it serves
//!   nothing), a Ready one only within the budget;
//! * `OnDelete` replaces nothing by itself;
//! * a `Failed` daemon pod is replaced.
//!
//! Only public API and literal label names are used, so the file compiles
//! against the controllers before the fix too — where it is red.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use engenho_controllers::{Controller, DaemonSetController, StatefulSetController};
use engenho_store::command::{Reason, ResourceCommand};
use engenho_store::{InProcessRouter, ResourceKey, StoreMesh, default_config};

const OLD: &str = "nix:/nix/store/p27v2aaaaaaaaaaaaaaaaaaaaaaaaaaa-rust_engenho-0.53.118";
const NEW: &str = "nix:/nix/store/q91n8aaaaaaaaaaaaaaaaaaaaaaaaaaa-rust_engenho-0.53.119";
const REV: &str = "controller-revision-hash";

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

async fn put(store: &StoreMesh, key: &ResourceKey, value: Value) {
    store
        .propose(ResourceCommand::put(key.clone(), value, Reason::Operator))
        .await
        .unwrap();
}

async fn patch(store: &StoreMesh, key: &ResourceKey, body: Value) {
    store
        .propose(ResourceCommand::patch(key.clone(), body, Reason::Operator))
        .await
        .unwrap();
}

fn pod_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

fn ds_key() -> ResourceKey {
    ResourceKey::namespaced("apps", "v1", "DaemonSet", "default", "agent")
}

fn sts_key() -> ResourceKey {
    ResourceKey::namespaced("apps", "v1", "StatefulSet", "default", "db")
}

async fn node(store: &StoreMesh, name: &str) {
    put(
        store,
        &ResourceKey::cluster_scoped("", "v1", "Node", name),
        json!({"kind": "Node", "apiVersion": "v1", "metadata": {"name": name}, "spec": {}}),
    )
    .await;
}

fn daemonset(image: &str, strategy: Option<Value>) -> Value {
    let mut ds = json!({
        "kind": "DaemonSet", "apiVersion": "apps/v1",
        "metadata": {"name": "agent", "namespace": "default"},
        "spec": {"template": {
            "metadata": {"labels": {"app": "agent"}},
            "spec": {"containers": [{"name": "c", "image": image}]}
        }}
    });
    if let Some(s) = strategy {
        ds["spec"]["updateStrategy"] = s;
    }
    ds
}

fn statefulset(image: &str, replicas: i64, strategy: Option<Value>) -> Value {
    let mut sts = json!({
        "kind": "StatefulSet", "apiVersion": "apps/v1",
        "metadata": {"name": "db", "namespace": "default"},
        "spec": {"replicas": replicas, "template": {
            "metadata": {"labels": {"app": "db"}},
            "spec": {"containers": [{"name": "c", "image": image}]}
        }}
    });
    if let Some(s) = strategy {
        sts["spec"]["updateStrategy"] = s;
    }
    sts
}

async fn image_of(store: &StoreMesh, name: &str) -> Option<String> {
    store.get(&pod_key(name)).await.map(|p| {
        p.pointer("/spec/containers/0/image")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    })
}

async fn mark_ready(store: &StoreMesh, name: &str) {
    patch(
        store,
        &pod_key(name),
        json!({"status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}}),
    )
    .await;
}

async fn images(store: &StoreMesh, names: &[&str]) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for n in names {
        out.push(image_of(store, n).await);
    }
    out
}

const DS_PODS: [&str; 3] = ["agent-n1", "agent-n2", "agent-n3"];

async fn ds_at_old(tag: &str, strategy: Option<Value>) -> (Arc<StoreMesh>, DaemonSetController) {
    let store = boot(tag).await;
    for n in ["n1", "n2", "n3"] {
        node(&store, n).await;
    }
    put(&store, &ds_key(), daemonset(OLD, strategy)).await;
    let c = DaemonSetController::new(store.clone(), None);
    c.tick().await.unwrap();
    assert_eq!(
        images(&store, &DS_PODS).await,
        vec![Some(OLD.to_string()); 3],
        "one pod per node from the first template"
    );
    (store, c)
}

/// Re-put the DaemonSet with a new image, keeping its uid (a put over an
/// existing object is an update).
async fn roll_ds_to_new(store: &StoreMesh, strategy: Option<Value>) {
    put(store, &ds_key(), daemonset(NEW, strategy)).await;
}

// ── DaemonSet ────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_created_daemon_pod_carries_its_template_revision() {
    let (store, _c) = ds_at_old("r16-ds-label", None).await;
    let pod = store.get(&pod_key("agent-n1")).await.unwrap();
    let rev = pod
        .pointer("/metadata/labels")
        .and_then(|l| l.get(REV))
        .and_then(Value::as_str);
    assert!(rev.is_some_and(|r| !r.is_empty()), "{pod}");
    assert!(
        pod.pointer("/metadata/labels/pod-template-generation")
            .is_some(),
        "{pod}"
    );
}

/// The incident: the pods never became Ready (their image is gone), so the
/// rollout replaces all of them at once — an unavailable pod serves nothing.
#[tokio::test]
async fn out_of_date_pods_that_are_not_ready_are_replaced_at_once() {
    let (store, c) = ds_at_old("r16-ds-unready", None).await;
    roll_ds_to_new(&store, None).await;

    c.tick().await.unwrap(); // deletes every out-of-date, unready pod
    c.tick().await.unwrap(); // recreates each node's pod from the current template
    assert_eq!(
        images(&store, &DS_PODS).await,
        vec![Some(NEW.to_string()); 3],
        "every node runs the current template without an operator"
    );
}

/// Ready pods go one budget's worth at a time — `maxUnavailable: 1` by
/// default — and the next goes only once the replacement is Ready.
#[tokio::test]
async fn ready_pods_roll_one_at_a_time_by_default() {
    let (store, c) = ds_at_old("r16-ds-ready", None).await;
    for p in DS_PODS {
        mark_ready(&store, p).await;
    }
    roll_ds_to_new(&store, None).await;

    let mut seen_down = Vec::new();
    for _ in 0..3 {
        c.tick().await.unwrap(); // one old pod deleted
        let now = images(&store, &DS_PODS).await;
        let down = now.iter().filter(|i| i.is_none()).count();
        assert_eq!(down, 1, "exactly one pod down at a time: {now:?}");
        seen_down.push(now);
        c.tick().await.unwrap(); // its replacement created from the new template
        c.tick().await.unwrap(); // budget exhausted: the replacement is not Ready yet
        let now = images(&store, &DS_PODS).await;
        assert!(now.iter().all(Option::is_some), "{now:?}");
        let fresh: Vec<&str> = DS_PODS
            .iter()
            .zip(&now)
            .filter(|(_, i)| i.as_deref() == Some(NEW))
            .map(|(n, _)| *n)
            .collect();
        for p in fresh {
            mark_ready(&store, p).await;
        }
    }
    assert_eq!(
        images(&store, &DS_PODS).await,
        vec![Some(NEW.to_string()); 3]
    );
}

/// The daemon restarting onto a new release is the case that matters: the
/// re-rendered `DaemonSet` reaches a controller that was just constructed and
/// holds no memory of the old pods. The rollout needs none — revisions are on
/// the pods — so a fresh controller after every step still replaces exactly
/// one Ready pod at a time, with no operator action.
#[tokio::test]
async fn a_controller_built_after_a_restart_rolls_one_pod_at_a_time() {
    let (store, first) = ds_at_old("r16-ds-restart", None).await;
    for p in DS_PODS {
        mark_ready(&store, p).await;
    }
    drop(first);
    roll_ds_to_new(&store, None).await;
    for step in 1..=3 {
        let c = DaemonSetController::new(store.clone(), None);
        c.tick().await.unwrap();
        let now = images(&store, &DS_PODS).await;
        assert_eq!(
            now.iter().filter(|i| i.is_none()).count(),
            1,
            "step {step}: {now:?}"
        );
        let c = DaemonSetController::new(store.clone(), None);
        c.tick().await.unwrap();
        for (p, img) in DS_PODS.iter().zip(images(&store, &DS_PODS).await) {
            if img.as_deref() == Some(NEW) {
                mark_ready(&store, p).await;
            }
        }
        let fresh = images(&store, &DS_PODS)
            .await
            .iter()
            .filter(|i| i.as_deref() == Some(NEW))
            .count();
        assert_eq!(fresh, step, "{step} replaced so far");
    }
}

#[tokio::test]
async fn max_unavailable_two_replaces_two_at_once() {
    let strategy = json!({"type": "RollingUpdate", "rollingUpdate": {"maxUnavailable": 2}});
    let (store, c) = ds_at_old("r16-ds-two", Some(strategy.clone())).await;
    for p in DS_PODS {
        mark_ready(&store, p).await;
    }
    roll_ds_to_new(&store, Some(strategy)).await;
    c.tick().await.unwrap();
    let now = images(&store, &DS_PODS).await;
    assert_eq!(now.iter().filter(|i| i.is_none()).count(), 2, "{now:?}");
}

#[tokio::test]
async fn on_delete_replaces_only_what_something_else_deleted() {
    let strategy = json!({"type": "OnDelete"});
    let (store, c) = ds_at_old("r16-ds-ondelete", Some(strategy.clone())).await;
    roll_ds_to_new(&store, Some(strategy)).await;
    c.tick().await.unwrap();
    c.tick().await.unwrap();
    assert_eq!(
        images(&store, &DS_PODS).await,
        vec![Some(OLD.to_string()); 3],
        "OnDelete never deletes a pod by itself"
    );
    store
        .propose(ResourceCommand::delete(
            pod_key("agent-n2"),
            Reason::Operator,
        ))
        .await
        .unwrap();
    c.tick().await.unwrap();
    assert_eq!(
        images(&store, &DS_PODS).await,
        vec![
            Some(OLD.to_string()),
            Some(NEW.to_string()),
            Some(OLD.to_string())
        ],
        "a deleted pod comes back from the current template"
    );
}

/// A daemon pod in phase `Failed` is deleted, and its node gets a fresh one.
#[tokio::test]
async fn a_failed_daemon_pod_is_replaced() {
    let (store, c) = ds_at_old("r16-ds-failed", None).await;
    for p in DS_PODS {
        mark_ready(&store, p).await;
    }
    let before = store.get(&pod_key("agent-n1")).await.unwrap();
    patch(
        &store,
        &pod_key("agent-n1"),
        json!({"status": {"phase": "Failed", "reason": "ImageUnavailable", "conditions": []}}),
    )
    .await;
    c.tick().await.unwrap();
    assert!(
        store.get(&pod_key("agent-n1")).await.is_none(),
        "failed pod deleted"
    );
    c.tick().await.unwrap();
    let after = store.get(&pod_key("agent-n1")).await.expect("replaced");
    assert_ne!(
        after.pointer("/metadata/uid"),
        before.pointer("/metadata/uid"),
        "a new pod, not the failed one"
    );
}

/// Pods made before revisions existed carry no label: they are out of date,
/// as upstream reads them, and roll once after an upgrade.
#[tokio::test]
async fn a_pod_with_no_revision_label_is_rolled() {
    let (store, c) = ds_at_old("r16-ds-legacy", None).await;
    for p in DS_PODS {
        let mut pod = store.get(&pod_key(p)).await.unwrap();
        pod["metadata"]["labels"] = json!({"app": "agent"});
        put(&store, &pod_key(p), pod).await;
    }
    c.tick().await.unwrap();
    let now = images(&store, &DS_PODS).await;
    assert_eq!(
        now.iter().filter(|i| i.is_none()).count(),
        3,
        "unlabelled and unready: replaced at once {now:?}"
    );
}

#[tokio::test]
async fn status_counts_the_pods_on_the_current_revision() {
    let (store, c) = ds_at_old("r16-ds-status", None).await;
    for p in DS_PODS {
        mark_ready(&store, p).await;
    }
    roll_ds_to_new(&store, None).await;
    c.tick().await.unwrap();
    c.tick().await.unwrap();
    let ds = store.get(&ds_key()).await.unwrap();
    assert_eq!(
        ds.pointer("/status/updatedNumberScheduled"),
        Some(&json!(1)),
        "{ds}"
    );
}

#[tokio::test]
async fn an_unknown_update_strategy_touches_no_pod() {
    let (store, c) = ds_at_old("r16-ds-bad", None).await;
    roll_ds_to_new(&store, Some(json!({"type": "Recreate"}))).await;
    c.tick().await.unwrap();
    assert_eq!(
        images(&store, &DS_PODS).await,
        vec![Some(OLD.to_string()); 3]
    );
}

// ── StatefulSet ──────────────────────────────────────────────────────────

const STS_PODS: [&str; 3] = ["db-0", "db-1", "db-2"];

async fn sts_at_old(tag: &str, strategy: Option<Value>) -> (Arc<StoreMesh>, StatefulSetController) {
    let store = boot(tag).await;
    put(&store, &sts_key(), statefulset(OLD, 3, strategy)).await;
    let c = StatefulSetController::new(store.clone(), None);
    c.tick().await.unwrap();
    assert_eq!(
        images(&store, &STS_PODS).await,
        vec![Some(OLD.to_string()); 3]
    );
    for p in STS_PODS {
        mark_ready(&store, p).await;
    }
    (store, c)
}

/// Upstream's `RollingUpdate` for a `StatefulSet`: highest ordinal first, one
/// at a time, the next only once every pod is Ready again.
#[tokio::test]
async fn a_statefulset_rolls_from_the_highest_ordinal_one_at_a_time() {
    let (store, c) = sts_at_old("r16-sts-roll", None).await;
    put(&store, &sts_key(), statefulset(NEW, 3, None)).await;

    for expected_new in [
        &["db-2"][..],
        &["db-2", "db-1"][..],
        &["db-2", "db-1", "db-0"][..],
    ] {
        c.tick().await.unwrap(); // delete the next out-of-date ordinal
        c.tick().await.unwrap(); // recreate it from the current template
        c.tick().await.unwrap(); // the replacement is not Ready: nothing else moves
        for p in STS_PODS {
            let img = image_of(&store, p).await;
            let want = if expected_new.contains(&p) { NEW } else { OLD };
            assert_eq!(img.as_deref(), Some(want), "{p} after {expected_new:?}");
        }
        for p in expected_new {
            mark_ready(&store, p).await;
        }
    }
}

#[tokio::test]
async fn a_statefulset_partition_holds_the_ordinals_below_it() {
    let strategy = json!({"type": "RollingUpdate", "rollingUpdate": {"partition": 2}});
    let (store, c) = sts_at_old("r16-sts-partition", Some(strategy.clone())).await;
    put(&store, &sts_key(), statefulset(NEW, 3, Some(strategy))).await;
    for _ in 0..4 {
        c.tick().await.unwrap();
        if let Some(Some(img)) = images(&store, &["db-2"]).await.first()
            && img == NEW
        {
            mark_ready(&store, "db-2").await;
        }
    }
    assert_eq!(
        images(&store, &STS_PODS).await,
        vec![
            Some(OLD.to_string()),
            Some(OLD.to_string()),
            Some(NEW.to_string())
        ]
    );
}

#[tokio::test]
async fn a_statefulset_on_delete_replaces_nothing_by_itself() {
    let strategy = json!({"type": "OnDelete"});
    let (store, c) = sts_at_old("r16-sts-ondelete", Some(strategy.clone())).await;
    put(&store, &sts_key(), statefulset(NEW, 3, Some(strategy))).await;
    c.tick().await.unwrap();
    c.tick().await.unwrap();
    assert_eq!(
        images(&store, &STS_PODS).await,
        vec![Some(OLD.to_string()); 3]
    );
}

/// Upstream waits on an unhealthy pod during a rollout, so a pod whose image
/// can never start would hold the set forever. What frees it is the pod going
/// `Failed` (a native pod with no closure does: `ImageUnavailable`): a failed
/// ordinal is deleted and recreated from the current template.
#[tokio::test]
async fn a_failed_statefulset_pod_is_recreated_from_the_current_template() {
    let store = boot("r16-sts-failed").await;
    put(&store, &sts_key(), statefulset(OLD, 2, None)).await;
    let c = StatefulSetController::new(store.clone(), None);
    c.tick().await.unwrap();
    put(&store, &sts_key(), statefulset(NEW, 2, None)).await;
    c.tick().await.unwrap(); // db-1 (highest, out of date) deleted
    c.tick().await.unwrap(); // db-1 recreated at NEW; db-0 waits on it
    assert_eq!(
        images(&store, &["db-0", "db-1"]).await,
        vec![Some(OLD.to_string()), Some(NEW.to_string())]
    );
    patch(
        &store,
        &pod_key("db-0"),
        json!({"status": {"phase": "Failed", "reason": "ImageUnavailable"}}),
    )
    .await;
    c.tick().await.unwrap(); // the failed ordinal is deleted
    c.tick().await.unwrap(); // and recreated
    assert_eq!(
        images(&store, &["db-0", "db-1"]).await,
        vec![Some(NEW.to_string()); 2],
    );
}

// ── Recorded gaps (docs/QUALIFICATION.md), red until fixed ───────────────

/// Upstream's Deployment `RollingUpdate` (default `maxSurge: 25%`,
/// `maxUnavailable: 25%`) keeps three of four old pods while the first new
/// one comes up. engenho scales every old `ReplicaSet` to zero at once, so a
/// template change takes the whole Deployment down together. Not the
/// stuck-forever defect (old pods ARE replaced), but not upstream either.
#[tokio::test]
#[ignore = "gap: docs/QUALIFICATION.md row 9 (Deployment ignores maxSurge/maxUnavailable)"]
async fn gap_a_deployment_rollout_keeps_old_pods_within_max_unavailable() {
    use engenho_controllers::DeploymentController;
    let store = boot("r16-deploy-gap").await;
    let key = ResourceKey::namespaced("apps", "v1", "Deployment", "default", "web");
    let deploy = |image: &str| {
        json!({"kind": "Deployment", "apiVersion": "apps/v1",
               "metadata": {"name": "web", "namespace": "default"},
               "spec": {"replicas": 4, "selector": {"matchLabels": {"app": "web"}},
                        "template": {"metadata": {"labels": {"app": "web"}},
                                     "spec": {"containers": [{"name": "c", "image": image}]}}}})
    };
    put(&store, &key, deploy(OLD)).await;
    let c = DeploymentController::new(store.clone(), None);
    c.tick().await.unwrap();
    put(&store, &key, deploy(NEW)).await;
    c.tick().await.unwrap();
    let rs = store
        .list("apps", "v1", "ReplicaSet", Some("default"))
        .await;
    let old_replicas: i64 = rs
        .iter()
        .filter(|(_, r)| r.pointer("/spec/template/spec/containers/0/image") == Some(&json!(OLD)))
        .filter_map(|(_, r)| r.pointer("/spec/replicas").and_then(Value::as_i64))
        .sum();
    assert_eq!(
        old_replicas, 3,
        "25% of 4 may be unavailable: three old replicas stay"
    );
}

/// Upstream's `ReplicaSet` counts only ACTIVE pods (`FilterActivePods`
/// excludes `Failed` and `Succeeded`), so a failed replica is replaced.
/// engenho counts a `Failed` pod as a replica and never replaces it. Left
/// unfixed on purpose until a terminated-pod GC exists: replacing without
/// one would accumulate a failed pod per retry.
#[tokio::test]
#[ignore = "gap: docs/QUALIFICATION.md row 10 (ReplicaSet counts Failed pods as replicas)"]
async fn gap_a_replicaset_replaces_a_failed_replica() {
    use engenho_controllers::ReplicaSetController;
    let store = boot("r16-rs-gap").await;
    let key = ResourceKey::namespaced("apps", "v1", "ReplicaSet", "default", "web");
    put(
        &store,
        &key,
        json!({"kind": "ReplicaSet", "apiVersion": "apps/v1",
               "metadata": {"name": "web", "namespace": "default"},
               "spec": {"replicas": 1, "template": {"metadata": {"labels": {"app": "web"}},
                        "spec": {"containers": [{"name": "c", "image": OLD}]}}}}),
    )
    .await;
    let c = ReplicaSetController::new(store.clone(), None);
    c.tick().await.unwrap();
    let pods = store.list("", "v1", "Pod", Some("default")).await;
    assert_eq!(pods.len(), 1, "premise");
    patch(&store, &pods[0].0, json!({"status": {"phase": "Failed"}})).await;
    c.tick().await.unwrap();
    let active = store
        .list("", "v1", "Pod", Some("default"))
        .await
        .into_iter()
        .filter(|(_, p)| p.pointer("/status/phase") != Some(&json!("Failed")))
        .count();
    assert_eq!(active, 1, "a failed replica is replaced");
}
