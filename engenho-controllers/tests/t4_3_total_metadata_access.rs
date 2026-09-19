//! T4.3 — total metadata access: a wrong-shaped object costs that object.
//!
//! Before T4.3 every writer into a stored object's metadata reached the map
//! through `.expect(..)`. A ReplicaSet whose `spec.template.metadata
//! .ownerReferences` was `null` panicked the replicaset controller on its
//! first pod, and a NetworkPolicy whose `metadata.annotations` was `null`
//! panicked the NetworkPolicy controller. Either way every object after the
//! bad one in the list was never reconciled again.
//!
//! * **M1** `null` is the empty case: the object is reconciled, not a panic.
//! * **M2** any other wrong type skips THAT object with a Warning Event on
//!   it naming the field, and the sweep continues to the next object.
//! * **M3** the Event is given once per resourceVersion, not once per tick.
//!
//! Every test lists the malformed object FIRST (keys sort by name), so "the
//! sweep continues" is proved by the object after it.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use engenho_controllers::event_recorder::{CollectingEventSink, Reason, Severity};
use engenho_controllers::network_policy::FakeNetworkPolicyEnforcer;
use engenho_controllers::network_policy_controller::{
    ENFORCEMENT_ANNOTATION, NetworkPolicyController,
};
use engenho_controllers::{Controller, DaemonSetController, ReplicaSetController, is_owned_by};
use engenho_store::command::{Reason as CommandReason, ResourceCommand};
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

async fn put(store: &StoreMesh, key: &ResourceKey, value: Value) -> String {
    store
        .propose(ResourceCommand::Put {
            key: key.clone(),
            value,
            expected: None,
            reason: CommandReason::Operator,
        })
        .await
        .unwrap();
    store.get(key).await.expect("stored")["metadata"]["uid"]
        .as_str()
        .expect("the store stamps a uid")
        .to_string()
}

fn rs_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("apps", "v1", "ReplicaSet", "default", name)
}

/// A two-replica ReplicaSet whose template carries `template_metadata`.
fn replicaset(name: &str, template_metadata: Value) -> Value {
    json!({
        "kind": "ReplicaSet",
        "apiVersion": "apps/v1",
        "metadata": { "name": name, "namespace": "default" },
        "spec": {
            "replicas": 2,
            "selector": { "matchLabels": { "app": name } },
            "template": {
                "metadata": template_metadata,
                "spec": { "containers": [{ "name": "main", "image": "podinfo:6" }] }
            }
        }
    })
}

async fn owned_pods(store: &StoreMesh, uid: &str) -> Vec<Value> {
    store
        .list("", "v1", "Pod", Some("default"))
        .await
        .into_iter()
        .map(|(_, pod)| pod)
        .filter(|pod| is_owned_by(pod, uid))
        .collect()
}

/// M1 — the generic owned-children path. A template whose ownerReferences
/// is `null` gets its pods, each owned by the ReplicaSet; the ReplicaSet
/// listed after it converges too. On HEAD before T4.3 the tick panicked
/// with "ownerReferences must be array".
#[tokio::test]
async fn a_null_template_owner_references_is_reconciled_not_a_panic() {
    let store = boot("t43-rs-null").await;
    let null_uid = put(
        &store,
        &rs_key("a-null"),
        replicaset(
            "a-null",
            json!({ "labels": { "app": "a-null" }, "ownerReferences": null }),
        ),
    )
    .await;
    let clean_uid = put(
        &store,
        &rs_key("b-clean"),
        replicaset("b-clean", json!({ "labels": { "app": "b-clean" } })),
    )
    .await;

    let events = Arc::new(CollectingEventSink::new());
    let c = ReplicaSetController::new(store.clone(), Some("default".into()))
        .with_event_sink(events.clone());
    let out = c.tick().await.expect("the tick completes");

    let sweep = out.sweep.expect("the blanket reports through its sweep");
    assert_eq!(
        (sweep.examined(), sweep.failed()),
        (2, 0),
        "null is the empty case, not a failure"
    );
    let pods = owned_pods(&store, &null_uid).await;
    assert_eq!(
        pods.len(),
        2,
        "the null-ownerReferences template got its pods"
    );
    for pod in &pods {
        assert_eq!(
            pod["metadata"]["ownerReferences"].as_array().map(Vec::len),
            Some(1),
            "exactly the ReplicaSet's reference, written into the healed array: {pod}"
        );
    }
    assert_eq!(owned_pods(&store, &clean_uid).await.len(), 2);
    assert!(
        events.drain().is_empty(),
        "nothing failed, nothing announced"
    );

    drop(c);
    shutdown(store).await;
}

/// M2 + M3 — a template whose ownerReferences is the wrong type creates no
/// pods, says why on the ReplicaSet (the path is the one the operator
/// declared, under spec.template), and does not stop the next ReplicaSet.
/// The Event is not repeated while the ReplicaSet is unchanged.
#[tokio::test]
async fn a_wrong_typed_template_owner_references_skips_that_replicaset_with_an_event() {
    let store = boot("t43-rs-wrong").await;
    let bad_uid = put(
        &store,
        &rs_key("a-bad"),
        replicaset(
            "a-bad",
            json!({ "labels": { "app": "a-bad" }, "ownerReferences": "oops" }),
        ),
    )
    .await;
    let clean_uid = put(
        &store,
        &rs_key("b-clean"),
        replicaset("b-clean", json!({ "labels": { "app": "b-clean" } })),
    )
    .await;

    let events = Arc::new(CollectingEventSink::new());
    let c = ReplicaSetController::new(store.clone(), Some("default".into()))
        .with_event_sink(events.clone());
    let out = c
        .tick()
        .await
        .expect("one malformed ReplicaSet is not the whole tick's failure");

    let sweep = out.sweep.expect("the blanket reports through its sweep");
    assert_eq!((sweep.failed(), sweep.changed()), (1, 1));
    assert!(owned_pods(&store, &bad_uid).await.is_empty());
    assert_eq!(
        owned_pods(&store, &clean_uid).await.len(),
        2,
        "the ReplicaSet after the malformed one still converges"
    );

    let recorded = events.drain();
    assert_eq!(recorded.len(), 1, "{recorded:?}");
    let ev = &recorded[0];
    assert_eq!(ev.reason, Reason::FailedCreate);
    assert_eq!(ev.reason.severity(), Severity::Warning);
    assert_eq!(ev.involved.kind, "ReplicaSet");
    assert_eq!(ev.involved.name, "a-bad");
    assert_eq!(ev.component, "replicaset-controller");
    assert!(
        ev.message
            .contains("spec.template.metadata.ownerReferences is a string, expected an array"),
        "{}",
        ev.message
    );

    // M3 — the same declaration next tick is the same failure: no new Event.
    c.tick().await.unwrap();
    assert!(events.drain().is_empty(), "once per resourceVersion");

    drop(c);
    shutdown(store).await;
}

/// M2 through a controller-specific write: a DaemonSet whose template
/// `spec` is not an object used to get an UNPINNED pod (the nodeName write
/// was skipped silently, so the scheduler could place it anywhere). It now
/// gets no pod and an Event; the DaemonSet after it is unaffected.
#[tokio::test]
async fn a_daemonset_with_a_non_object_template_spec_gets_no_unpinned_pod() {
    let store = boot("t43-ds").await;
    store
        .propose(ResourceCommand::put(
            ResourceKey::cluster_scoped("", "v1", "Node", "node-a"),
            json!({ "kind": "Node", "apiVersion": "v1", "metadata": { "name": "node-a" },
                    "spec": { "unschedulable": false } }),
            CommandReason::Operator,
        ))
        .await
        .unwrap();
    let ds = |name: &str, template_spec: Value| {
        json!({
            "kind": "DaemonSet", "apiVersion": "apps/v1",
            "metadata": { "name": name, "namespace": "default" },
            "spec": { "template": { "metadata": { "labels": { "app": name } },
                                    "spec": template_spec } }
        })
    };
    let ds_key = |name: &str| ResourceKey::namespaced("apps", "v1", "DaemonSet", "default", name);
    let bad_uid = put(&store, &ds_key("a-bad"), ds("a-bad", json!("oops"))).await;
    let clean_uid = put(
        &store,
        &ds_key("b-clean"),
        ds(
            "b-clean",
            json!({ "containers": [{ "name": "c", "image": "img" }] }),
        ),
    )
    .await;

    let events = Arc::new(CollectingEventSink::new());
    let c = DaemonSetController::new(store.clone(), None).with_event_sink(events.clone());
    c.tick().await.expect("the tick completes");

    assert!(
        owned_pods(&store, &bad_uid).await.is_empty(),
        "no pod rather than a pod pinned to nothing"
    );
    let clean = owned_pods(&store, &clean_uid).await;
    assert_eq!(clean.len(), 1);
    assert_eq!(clean[0]["spec"]["nodeName"], "node-a");
    let recorded = events.drain();
    assert_eq!(recorded.len(), 1, "{recorded:?}");
    assert_eq!(recorded[0].involved.name, "a-bad");
    assert!(
        recorded[0]
            .message
            .contains("spec.template.spec is a string, expected an object"),
        "{}",
        recorded[0].message
    );

    drop(c);
    shutdown(store).await;
}

fn np_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("networking.k8s.io", "v1", "NetworkPolicy", "ns", name)
}

fn policy(name: &str, annotations: Option<Value>) -> Value {
    let mut metadata = json!({ "name": name, "namespace": "ns" });
    if let Some(a) = annotations {
        metadata["annotations"] = a;
    }
    json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": metadata,
        "spec": { "podSelector": {}, "policyTypes": ["Ingress"] },
    })
}

fn verdict(v: &Value) -> Option<&str> {
    v["metadata"]["annotations"][ENFORCEMENT_ANNOTATION].as_str()
}

/// M1 for the NetworkPolicy controller: `annotations: null` is annotated,
/// and the clean policy after it is annotated too. On HEAD before T4.3 the
/// tick panicked with "annotations is an object".
#[tokio::test]
async fn a_null_annotations_policy_is_annotated_and_so_is_the_next() {
    let store = boot("t43-np-null").await;
    put(
        &store,
        &np_key("a-null"),
        policy("a-null", Some(Value::Null)),
    )
    .await;
    put(&store, &np_key("b-clean"), policy("b-clean", None)).await;

    let c = NetworkPolicyController::new(store.clone(), Arc::new(FakeNetworkPolicyEnforcer::new()));
    c.tick().await.expect("the tick completes");

    for name in ["a-null", "b-clean"] {
        let got = store.get(&np_key(name)).await.expect("policy");
        assert_eq!(verdict(&got), Some("Computed"), "{name}: {got}");
    }

    drop(c);
    shutdown(store).await;
}

/// M2 for the NetworkPolicy controller: annotations of the wrong type are
/// left exactly as declared, the policy gets a `FailedUpdate` Event naming
/// the field, and the clean policy after it is still annotated.
#[tokio::test]
async fn a_wrong_typed_annotations_policy_is_skipped_and_the_next_is_annotated() {
    let store = boot("t43-np-wrong").await;
    put(
        &store,
        &np_key("a-bad"),
        policy("a-bad", Some(json!("oops"))),
    )
    .await;
    put(&store, &np_key("b-clean"), policy("b-clean", None)).await;

    let events = Arc::new(CollectingEventSink::new());
    let c = NetworkPolicyController::new(store.clone(), Arc::new(FakeNetworkPolicyEnforcer::new()))
        .with_event_sink(events.clone());
    let out = c.tick().await.expect("the tick completes");
    assert_eq!(out.sweep.expect("sweep").failed(), 1);

    let bad = store.get(&np_key("a-bad")).await.expect("policy");
    assert_eq!(bad["metadata"]["annotations"], "oops", "left as declared");
    let clean = store.get(&np_key("b-clean")).await.expect("policy");
    assert_eq!(verdict(&clean), Some("Computed"));

    let failed: Vec<_> = events
        .drain()
        .into_iter()
        .filter(|e| e.reason == Reason::FailedUpdate)
        .collect();
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert_eq!(failed[0].involved.name, "a-bad");
    assert_eq!(failed[0].component, "network-policy-controller");
    assert!(
        failed[0]
            .message
            .contains("metadata.annotations is a string, expected an object"),
        "{}",
        failed[0].message
    );

    drop(c);
    shutdown(store).await;
}
