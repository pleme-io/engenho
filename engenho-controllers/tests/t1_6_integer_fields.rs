//! T1.6 — an integer field has three states: absent, an integer, or
//! something else.
//!
//! Before T1.6 every controller read a count with
//! `.as_i64().unwrap_or(default)`, which read the third state as the first.
//! A Deployment declaring `replicas: "3"` was read as `replicas: 1` and its
//! ReplicaSet was scaled to one pod, with nothing said to anyone.
//!
//! * **I1** a count that is not an integer is not the default: the object
//!   that declares it gets nothing written for it, and a Warning Event on
//!   it names the field. The object after it in the list still converges.
//! * **I2** an absent count is the API default — for a child the parent
//!   reads, too: a stale ReplicaSet with no declared count runs the API's
//!   one pod, so it is scaled down like any other.
//! * **I3** a count that cannot be read never feeds a write computed from
//!   it — a status sum, an HPA clamp.
//!
//! Every test lists the malformed object FIRST (keys sort by name), so "the
//! next object still converges" is proved by the object after it.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use engenho_controllers::event_recorder::CollectingEventSink;
use engenho_controllers::{
    Controller, DeploymentController, FakeMetricsProvider, HorizontalPodAutoscalerController,
    JobController, ReplicaSetController, ScaleTarget, is_owned_by,
};
use engenho_store::command::{Reason, ResourceCommand};
use engenho_store::{InProcessRouter, ResourceKey, StoreMesh, default_config};

async fn boot(name: &str) -> Arc<StoreMesh> {
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

async fn shutdown(store: Arc<StoreMesh>) {
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}

/// Store `value` at `key` and return the uid the store stamped.
async fn put(store: &StoreMesh, key: &ResourceKey, value: Value) -> String {
    store
        .propose(ResourceCommand::put(key.clone(), value, Reason::Operator))
        .await
        .unwrap();
    store.get(key).await.expect("stored")["metadata"]["uid"]
        .as_str()
        .expect("the store stamps a uid")
        .to_string()
}

fn deployment_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("apps", "v1", "Deployment", "default", name)
}

fn rs_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("apps", "v1", "ReplicaSet", "default", name)
}

fn template(name: &str) -> Value {
    json!({
        "metadata": { "labels": { "app": name } },
        "spec": { "containers": [{ "name": "main", "image": "podinfo:6" }] }
    })
}

/// A Deployment whose `spec.replicas` is `replicas`, or has none when
/// `replicas` is `None`.
fn deployment(name: &str, replicas: Option<Value>) -> Value {
    let mut d = json!({
        "kind": "Deployment",
        "apiVersion": "apps/v1",
        "metadata": { "name": name, "namespace": "default" },
        "spec": {
            "selector": { "matchLabels": { "app": name } },
            "template": template(name)
        }
    });
    if let Some(replicas) = replicas {
        d["spec"]["replicas"] = replicas;
    }
    d
}

async fn owned(store: &StoreMesh, group: &str, kind: &str, uid: &str) -> Vec<(ResourceKey, Value)> {
    store
        .list(group, "v1", kind, Some("default"))
        .await
        .into_iter()
        .filter(|(_, v)| is_owned_by(v, uid))
        .collect()
}

fn assert_one_event_naming(events: &CollectingEventSink, object: &str, field_message: &str) {
    let recorded = events.drain();
    assert_eq!(recorded.len(), 1, "{recorded:?}");
    assert_eq!(recorded[0].involved.name, object);
    assert!(
        recorded[0].message.contains(field_message),
        "{}",
        recorded[0].message
    );
}

/// I1, the worked defect. A Deployment already running three replicas is
/// edited to `replicas: "3"`. On HEAD before T1.6 the string read as the
/// default, and the ReplicaSet was patched down to one.
#[tokio::test]
async fn a_deployment_declaring_replicas_as_a_string_is_not_scaled_to_1() {
    let store = boot("t16-deploy-scale").await;
    let key = deployment_key("web");
    let uid = put(&store, &key, deployment("web", Some(json!(3)))).await;

    let events = Arc::new(CollectingEventSink::new());
    let dc = DeploymentController::new(store.clone(), Some("default".into()))
        .with_event_sink(events.clone());
    dc.tick().await.unwrap();
    let rses = owned(&store, "apps", "ReplicaSet", &uid).await;
    assert_eq!(rses.len(), 1);
    assert_eq!(rses[0].1["spec"]["replicas"], json!(3));

    // Same template (so the same current ReplicaSet), count now quoted.
    put(&store, &key, deployment("web", Some(json!("3")))).await;
    let out = dc
        .tick()
        .await
        .expect("a malformed Deployment is not the tick's failure");

    let rses = owned(&store, "apps", "ReplicaSet", &uid).await;
    assert_eq!(rses.len(), 1, "no second ReplicaSet");
    assert_eq!(
        rses[0].1["spec"]["replicas"],
        json!(3),
        "the ReplicaSet was left at its count, not scaled to the default's 1"
    );
    let sweep = out.sweep.expect("the blanket reports through its sweep");
    assert_eq!((sweep.failed(), sweep.changed()), (1, 0));
    assert_one_event_naming(
        &events,
        "web",
        "spec.replicas is not an integer (found a string)",
    );

    drop(dc);
    shutdown(store).await;
}

/// I1 on a fresh Deployment: nothing is created for it and no status is
/// written, and the Deployment listed after it converges. On HEAD the
/// malformed one got a ReplicaSet of one.
#[tokio::test]
async fn a_new_deployment_with_a_malformed_count_gets_nothing_and_the_next_converges() {
    let store = boot("t16-deploy-new").await;
    let bad = put(
        &store,
        &deployment_key("a-bad"),
        deployment("a-bad", Some(json!(2.5))),
    )
    .await;
    let clean = put(
        &store,
        &deployment_key("b-clean"),
        deployment("b-clean", Some(json!(2))),
    )
    .await;

    let events = Arc::new(CollectingEventSink::new());
    let dc = DeploymentController::new(store.clone(), Some("default".into()))
        .with_event_sink(events.clone());
    let out = dc.tick().await.unwrap();

    assert!(
        owned(&store, "apps", "ReplicaSet", &bad).await.is_empty(),
        "no ReplicaSet is created from a count that cannot be read"
    );
    let sweep = out.sweep.expect("the blanket reports through its sweep");
    assert_eq!((sweep.failed(), sweep.changed()), (1, 1));
    assert!(
        store.get(&deployment_key("a-bad")).await.unwrap()["status"].is_null(),
        "no status is written for a Deployment whose count cannot be read"
    );
    let clean_rs = owned(&store, "apps", "ReplicaSet", &clean).await;
    assert_eq!(clean_rs.len(), 1);
    assert_eq!(clean_rs[0].1["spec"]["replicas"], json!(2));
    assert_one_event_naming(
        &events,
        "a-bad",
        "spec.replicas is not an integer (found a number)",
    );

    // Once per resourceVersion, as for every Declarative failure.
    dc.tick().await.unwrap();
    assert!(events.drain().is_empty());

    drop(dc);
    shutdown(store).await;
}

/// I2 — absent is the API default of one, not zero and not an error.
#[tokio::test]
async fn an_absent_count_is_the_api_default_of_one() {
    let store = boot("t16-deploy-absent").await;
    let uid = put(&store, &deployment_key("web"), deployment("web", None)).await;
    let dc = DeploymentController::new(store.clone(), Some("default".into()));
    let out = dc.tick().await.unwrap();
    assert_eq!(out.sweep.expect("sweep").failed(), 0);
    let rses = owned(&store, "apps", "ReplicaSet", &uid).await;
    assert_eq!(rses.len(), 1);
    assert_eq!(rses[0].1["spec"]["replicas"], json!(1));

    drop(dc);
    shutdown(store).await;
}

/// An owned ReplicaSet of `deployment_uid` that is NOT the current
/// template's, carrying `replicas` (or no count). Its template is an older
/// revision's: since T4.10 a ReplicaSet is matched by its template, not by
/// its `pod-template-hash` label, so a stale label alone no longer makes it
/// stale.
fn stale_rs(name: &str, deployment_uid: &str, replicas: Option<Value>) -> Value {
    let mut older_revision = template("web");
    older_revision["spec"]["containers"][0]["image"] = json!("podinfo:5");
    let mut rs = json!({
        "kind": "ReplicaSet",
        "apiVersion": "apps/v1",
        "metadata": {
            "name": name,
            "namespace": "default",
            "labels": { "pod-template-hash": "stale00000" },
            "ownerReferences": [{
                "apiVersion": "apps/v1", "kind": "Deployment", "name": "web",
                "uid": deployment_uid, "controller": true, "blockOwnerDeletion": true
            }]
        },
        "spec": { "selector": { "matchLabels": { "app": "web" } }, "template": older_revision }
    });
    if let Some(replicas) = replicas {
        rs["spec"]["replicas"] = replicas;
    }
    rs
}

/// I2 for a child: a stale ReplicaSet with no declared count runs the
/// API's one pod, so the Deployment scales it to 0. On HEAD the absent
/// count read as 0 and the old revision kept its pod.
#[tokio::test]
async fn a_stale_replicaset_with_no_declared_count_is_scaled_down() {
    let store = boot("t16-stale-absent").await;
    let uid = put(
        &store,
        &deployment_key("web"),
        deployment("web", Some(json!(2))),
    )
    .await;
    put(&store, &rs_key("web-old"), stale_rs("web-old", &uid, None)).await;

    let dc = DeploymentController::new(store.clone(), Some("default".into()));
    dc.tick().await.unwrap();
    assert_eq!(
        store.get(&rs_key("web-old")).await.unwrap()["spec"]["replicas"],
        json!(0)
    );

    drop(dc);
    shutdown(store).await;
}

/// I1 for a child: a stale ReplicaSet whose count is malformed is left as
/// it is (its own controller does nothing with it either, and says so on
/// it), while the Deployment's own work — its current ReplicaSet — goes on.
#[tokio::test]
async fn a_stale_replicaset_with_a_malformed_count_is_left_alone() {
    let store = boot("t16-stale-bad").await;
    let uid = put(
        &store,
        &deployment_key("web"),
        deployment("web", Some(json!(2))),
    )
    .await;
    put(
        &store,
        &rs_key("web-old"),
        stale_rs("web-old", &uid, Some(json!("2"))),
    )
    .await;

    let dc = DeploymentController::new(store.clone(), Some("default".into()));
    let out = dc.tick().await.unwrap();
    assert_eq!(out.sweep.expect("sweep").failed(), 0);
    assert_eq!(
        store.get(&rs_key("web-old")).await.unwrap()["spec"]["replicas"],
        json!("2"),
        "nothing is written to an object from a count that cannot be read"
    );
    let rses = owned(&store, "apps", "ReplicaSet", &uid).await;
    assert!(
        rses.iter()
            .any(|(k, v)| k.name != "web-old" && v["spec"]["replicas"] == json!(2)),
        "the current ReplicaSet was still created: {rses:?}"
    );

    drop(dc);
    shutdown(store).await;
}

/// I3 — a Deployment's status sums its ReplicaSets' status counts. One
/// that cannot be read writes no status, rather than a sum with a guessed
/// term. On HEAD `readyReplicas: "3"` read as 0 and the sum was written.
#[tokio::test]
async fn a_child_status_count_that_cannot_be_read_writes_no_deployment_status() {
    let store = boot("t16-status-sum").await;
    let key = deployment_key("web");
    let uid = put(&store, &key, deployment("web", Some(json!(3)))).await;
    let dc = DeploymentController::new(store.clone(), Some("default".into()));
    dc.tick().await.unwrap();
    let before = store.get(&key).await.unwrap()["status"].clone();
    assert_eq!(before["replicas"], json!(0), "no RS status yet: {before}");

    let (rs, _) = owned(&store, "apps", "ReplicaSet", &uid)
        .await
        .pop()
        .expect("the current ReplicaSet");
    store
        .propose(ResourceCommand::patch(
            rs,
            json!({"status": {"replicas": 3, "readyReplicas": "3", "availableReplicas": 3}}),
            Reason::Operator,
        ))
        .await
        .unwrap();

    dc.tick().await.unwrap();
    assert_eq!(
        store.get(&key).await.unwrap()["status"],
        before,
        "the status was not rewritten from a count that cannot be read"
    );

    drop(dc);
    shutdown(store).await;
}

/// I1 through the ReplicaSet controller: no pod for a quoted count. On
/// HEAD the ReplicaSet got one pod.
#[tokio::test]
async fn a_replicaset_declaring_replicas_as_a_string_creates_no_pods() {
    let store = boot("t16-rs").await;
    let rs = |name: &str, replicas: Value| {
        json!({
            "kind": "ReplicaSet", "apiVersion": "apps/v1",
            "metadata": { "name": name, "namespace": "default" },
            "spec": { "replicas": replicas,
                      "selector": { "matchLabels": { "app": name } },
                      "template": template(name) }
        })
    };
    let bad = put(&store, &rs_key("a-bad"), rs("a-bad", json!("3"))).await;
    let clean = put(&store, &rs_key("b-clean"), rs("b-clean", json!(2))).await;

    let events = Arc::new(CollectingEventSink::new());
    let rc = ReplicaSetController::new(store.clone(), Some("default".into()))
        .with_event_sink(events.clone());
    rc.tick().await.unwrap();

    assert!(owned(&store, "", "Pod", &bad).await.is_empty());
    assert_eq!(owned(&store, "", "Pod", &clean).await.len(), 2);
    assert_one_event_naming(
        &events,
        "a-bad",
        "spec.replicas is not an integer (found a string)",
    );

    drop(rc);
    shutdown(store).await;
}

/// I1 through the Job controller: no pod for a quoted completion count.
/// On HEAD the Job ran one pod toward a count it never declared.
#[tokio::test]
async fn a_job_declaring_completions_as_a_string_creates_no_pods() {
    let store = boot("t16-job").await;
    let job_key = |name: &str| ResourceKey::namespaced("batch", "v1", "Job", "default", name);
    let job = |name: &str, completions: Value| {
        json!({
            "kind": "Job", "apiVersion": "batch/v1",
            "metadata": { "name": name, "namespace": "default" },
            "spec": { "completions": completions, "template": template(name) }
        })
    };
    let bad = put(&store, &job_key("a-bad"), job("a-bad", json!("2"))).await;
    let clean = put(&store, &job_key("b-clean"), job("b-clean", json!(1))).await;

    let events = Arc::new(CollectingEventSink::new());
    let jc =
        JobController::new(store.clone(), Some("default".into())).with_event_sink(events.clone());
    jc.tick().await.unwrap();

    assert!(owned(&store, "", "Pod", &bad).await.is_empty());
    assert_eq!(owned(&store, "", "Pod", &clean).await.len(), 1);
    assert_one_event_naming(
        &events,
        "a-bad",
        "spec.completions is not an integer (found a string)",
    );

    drop(jc);
    shutdown(store).await;
}

/// I3 through the HPA: a `minReplicas` that cannot be read leaves the
/// target alone instead of clamping by the default. On HEAD the quoted
/// bound read as 1 and the target was scaled.
#[tokio::test]
async fn an_hpa_whose_bound_cannot_be_read_leaves_its_target_alone() {
    let store = boot("t16-hpa").await;
    let target = deployment_key("web");
    put(&store, &target, deployment("web", Some(json!(2)))).await;
    put(
        &store,
        &ResourceKey::namespaced(
            "autoscaling",
            "v2",
            "HorizontalPodAutoscaler",
            "default",
            "web",
        ),
        json!({
            "kind": "HorizontalPodAutoscaler", "apiVersion": "autoscaling/v2",
            "metadata": { "name": "web", "namespace": "default" },
            "spec": {
                "scaleTargetRef": { "kind": "Deployment", "name": "web" },
                "minReplicas": "5", "maxReplicas": 10, "targetValue": 50.0
            }
        }),
    )
    .await;
    let metrics = Arc::new(FakeMetricsProvider::new());
    metrics
        .set(
            ScaleTarget {
                kind: "Deployment".into(),
                name: "web".into(),
                namespace: "default".into(),
            },
            80.0,
        )
        .await;

    let hpa = HorizontalPodAutoscalerController::new(store.clone(), metrics, None);
    let out = hpa.tick().await.unwrap();
    assert_eq!(
        store.get(&target).await.unwrap()["spec"]["replicas"],
        json!(2),
        "the target is not scaled by a clamp nobody declared"
    );
    assert_eq!((out.objects_changed, out.objects_skipped), (0, 1));

    drop(hpa);
    shutdown(store).await;
}
