//! T4.10 — a Deployment finds its `ReplicaSet` by comparing normalized
//! templates (upstream's `EqualIgnoreHash`), not by the hash of the raw
//! template bytes.
//!
//! Under the hash matcher, any change to a template's BYTES that left its
//! meaning alone — a `null` the apiserver starts dropping (T4.4), an `[]` a
//! client starts omitting, a new hash function — found no `ReplicaSet` for
//! the Deployment, created a new one and scaled the running one to zero: a
//! rollout of identical pods, for every Deployment the change touched.
//!
//! * **M1** switching matchers creates no `ReplicaSet` and moves no pod: the
//!   state the hash matcher left behind is already converged.
//! * **M2** an equivalent but byte-different template creates no
//!   `ReplicaSet` (red on the hash matcher).
//! * **M3** the hash only names: an owned `ReplicaSet` running the template
//!   is current whatever hash its label carries (red on the hash matcher),
//!   and its status is the Deployment's.
//! * **M4** a real template edit still rolls out, including the edits a
//!   too-eager normalization would swallow; rolling back to an equivalent
//!   template reuses the old `ReplicaSet` (red on the hash matcher).
//! * **M5** two `ReplicaSet`s running the same template leave the pods
//!   where they are: the one holding them stays current, the other goes to
//!   zero.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use engenho_controllers::owner::{owner_ref_for, set_owner_reference};
use engenho_controllers::{Controller, DeploymentController, is_owned_by};
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

fn deployment_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("apps", "v1", "Deployment", "default", name)
}

fn rs_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("apps", "v1", "ReplicaSet", "default", name)
}

async fn put(store: &StoreMesh, key: &ResourceKey, value: Value) -> Value {
    store
        .propose(ResourceCommand::put(key.clone(), value, Reason::Operator))
        .await
        .unwrap();
    store.get(key).await.expect("stored")
}

fn template(image: &str) -> Value {
    json!({
        "metadata": { "labels": { "app": "web" } },
        "spec": { "containers": [{ "name": "main", "image": image }] }
    })
}

fn deployment(replicas: i64, template: Value) -> Value {
    json!({
        "kind": "Deployment",
        "apiVersion": "apps/v1",
        "metadata": { "name": "web", "namespace": "default" },
        "spec": {
            "replicas": replicas,
            "selector": { "matchLabels": { "app": "web" } },
            "template": template
        }
    })
}

/// Store the Deployment (a re-Put keeps the uid) and return it as stored.
async fn put_deployment(store: &StoreMesh, replicas: i64, template: Value) -> Value {
    put(
        store,
        &deployment_key("web"),
        deployment(replicas, template),
    )
    .await
}

/// An owned `ReplicaSet` as a controller left it: `template` verbatim,
/// `hash_label` as its `pod-template-hash`, named after it.
async fn put_owned_rs(
    store: &StoreMesh,
    parent: &Value,
    hash_label: &str,
    replicas: i64,
    template: &Value,
) -> String {
    let name = format!("web-{hash_label}");
    let mut rs = json!({
        "kind": "ReplicaSet",
        "apiVersion": "apps/v1",
        "metadata": {
            "name": name,
            "namespace": "default",
            "labels": {
                "app.kubernetes.io/managed-by": "engenho-deployment-controller",
                "pod-template-hash": hash_label
            }
        },
        "spec": {
            "replicas": replicas,
            "selector": { "matchLabels": { "app": "web" } },
            "template": template
        }
    });
    let owner = owner_ref_for(parent, "apps/v1", "Deployment").expect("stored parent has a uid");
    set_owner_reference(&mut rs, owner).unwrap();
    put(store, &rs_key(&name), rs).await;
    name
}

/// The retired matcher's hash, reproduced so a test can build the exact
/// state it left: FNV-1a over the RAW template's compact JSON, the first 10
/// of 16 hex digits.
fn retired_hash(template: &Value) -> String {
    let bytes = serde_json::to_vec(template).unwrap();
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in &bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    format!("{h:016x}").chars().take(10).collect()
}

/// `(name, spec.replicas)` of every `ReplicaSet` the Deployment owns,
/// sorted by name.
async fn owned_rs(store: &StoreMesh, parent: &Value) -> Vec<(String, i64)> {
    let uid = parent["metadata"]["uid"].as_str().expect("uid");
    let mut out: Vec<(String, i64)> = store
        .list("apps", "v1", "ReplicaSet", Some("default"))
        .await
        .into_iter()
        .filter(|(_, v)| is_owned_by(v, uid))
        .map(|(k, v)| {
            (
                k.name,
                v["spec"]["replicas"].as_i64().expect("an integer count"),
            )
        })
        .collect();
    out.sort();
    out
}

/// One named edit to a template.
type Edit = fn(&mut Value);

fn controller(store: &Arc<StoreMesh>) -> DeploymentController {
    DeploymentController::new(store.clone(), Some("default".into()))
}

/// M1. The state the hash matcher left — a current RS named and labelled
/// by the RAW template's hash, and an older revision at zero — is already
/// converged under template matching. The current template carries the
/// byte-quirks a normalization strips (`annotations: null`, `env: []`), so
/// a matcher that merely re-hashed the normalized form would miss it.
#[tokio::test]
async fn switching_matchers_creates_no_replicaset_and_moves_no_pod() {
    let store = boot("t410-switch").await;
    let old = template("web:1");
    let mut current = template("web:2");
    current["metadata"]["annotations"] = Value::Null;
    current["spec"]["containers"][0]["env"] = json!([]);
    let d = put_deployment(&store, 3, current).await;
    // Hash and copy the template AS STORED — what the old controller read.
    let stored = d["spec"]["template"].clone();
    let old_rs = put_owned_rs(&store, &d, &retired_hash(&old), 0, &old).await;
    let cur_rs = put_owned_rs(&store, &d, &retired_hash(&stored), 3, &stored).await;
    let before = owned_rs(&store, &d).await;

    let dc = controller(&store);
    dc.tick().await.unwrap();
    dc.tick().await.unwrap();

    let after = owned_rs(&store, &d).await;
    assert_eq!(after, before, "no ReplicaSet created, none rescaled");
    assert!(after.contains(&(cur_rs, 3)) && after.contains(&(old_rs, 0)));

    drop(dc);
    shutdown(store).await;
}

/// M2, the worked defect. A Deployment is re-stored with a template that
/// means the same thing in different bytes. Each variant is one a real
/// writer produces: `kubectl get -o yaml` round-trips `creationTimestamp:
/// null`; T4.4 will drop `annotations: null`; clients differ on emitting
/// `[]` and `{}`; an upstream-written template may carry the hash label.
/// On the hash matcher the first variant already created a second
/// ReplicaSet and scaled the running one to zero.
#[tokio::test]
async fn an_equivalent_but_byte_different_template_creates_no_replicaset() {
    let store = boot("t410-equivalent").await;
    let d = put_deployment(&store, 3, template("web:1")).await;
    let dc = controller(&store);
    dc.tick().await.unwrap();
    let converged = owned_rs(&store, &d).await;
    assert_eq!(converged.len(), 1);
    assert_eq!(converged[0].1, 3);

    let variants: Vec<(&str, Edit)> = vec![
        ("creationTimestamp: null", |t| {
            t["metadata"]["creationTimestamp"] = Value::Null;
        }),
        ("annotations: null", |t| {
            t["metadata"]["annotations"] = Value::Null;
        }),
        ("annotations: {}", |t| {
            t["metadata"]["annotations"] = json!({});
        }),
        ("volumes: []", |t| {
            t["spec"]["volumes"] = json!([]);
        }),
        ("env: []", |t| {
            t["spec"]["containers"][0]["env"] = json!([]);
        }),
        ("nodeSelector: {}", |t| {
            t["spec"]["nodeSelector"] = json!({});
        }),
        ("command: null", |t| {
            t["spec"]["containers"][0]["command"] = Value::Null;
        }),
        ("a pod-template-hash label", |t| {
            t["metadata"]["labels"]["pod-template-hash"] = json!("0123456789");
        }),
    ];
    let mut equivalent = template("web:1");
    for (what, edit) in variants {
        edit(&mut equivalent);
        put_deployment(&store, 3, equivalent.clone()).await;
        dc.tick().await.unwrap();
        assert_eq!(
            owned_rs(&store, &d).await,
            converged,
            "an equivalent template ({what}) created or rescaled a ReplicaSet"
        );
    }

    drop(dc);
    shutdown(store).await;
}

/// M3. The owned ReplicaSet runs the Deployment's exact template but was
/// named by some other hash — a changed hash function, or an RS written by
/// upstream's controller. It is the current one: scaled to the Deployment's
/// count, nothing created, and the Deployment's status is its status.
#[tokio::test]
async fn the_hash_only_names_a_replicaset_running_the_template_is_current() {
    let store = boot("t410-foreign-hash").await;
    let d = put_deployment(&store, 3, template("web:1")).await;
    let name = put_owned_rs(&store, &d, "5f7c9d8b6x", 2, &template("web:1")).await;
    store
        .propose(ResourceCommand::patch(
            rs_key(&name),
            json!({"status": {"replicas": 2, "readyReplicas": 2, "availableReplicas": 2}}),
            Reason::Operator,
        ))
        .await
        .unwrap();

    let dc = controller(&store);
    dc.tick().await.unwrap();

    assert_eq!(
        owned_rs(&store, &d).await,
        vec![(name, 3)],
        "the RS running the template is scaled, and no other is created"
    );
    let status = store.get(&deployment_key("web")).await.unwrap()["status"].clone();
    assert_eq!(status["replicas"], json!(2), "{status}");
    assert_eq!(status["readyReplicas"], json!(2), "{status}");
    assert_eq!(status["updatedReplicas"], json!(2), "{status}");

    drop(dc);
    shutdown(store).await;
}

/// M4. Real edits still roll out — one new ReplicaSet each, the running one
/// scaled to zero. Two of them are edits a too-eager normalization would
/// swallow: `kubectl rollout restart` (one annotation) and adding
/// `emptyDir: {}` (an empty object that IS the volume's source).
#[tokio::test]
async fn a_real_template_edit_still_rolls_out() {
    let store = boot("t410-real-edit").await;
    let base = template("web:1");
    let d = put_deployment(&store, 2, base.clone()).await;
    let dc = controller(&store);
    dc.tick().await.unwrap();
    assert_eq!(owned_rs(&store, &d).await.len(), 1);

    let mut restarted = base.clone();
    restarted["metadata"]["annotations"] =
        json!({"kubectl.kubernetes.io/restartedAt": "2026-09-19T00:00:00Z"});
    let mut no_source = restarted.clone();
    no_source["spec"]["volumes"] = json!([{"name": "scratch"}]);
    let mut empty_dir = restarted.clone();
    empty_dir["spec"]["volumes"] = json!([{"name": "scratch", "emptyDir": {}}]);

    for (n, edited) in [restarted, no_source, empty_dir].into_iter().enumerate() {
        put_deployment(&store, 2, edited).await;
        dc.tick().await.unwrap();
        let counts: Vec<i64> = owned_rs(&store, &d).await.iter().map(|(_, r)| *r).collect();
        let mut sorted = counts.clone();
        sorted.sort_unstable();
        let mut expected = vec![0; n + 1];
        expected.push(2);
        assert_eq!(
            sorted, expected,
            "edit {n}: one new RS at 2, every other at 0"
        );
    }

    drop(dc);
    shutdown(store).await;
}

/// M4, rollback. A → B → A-in-other-bytes reuses A's ReplicaSet, as
/// `kubectl rollout undo` expects: no third ReplicaSet. On the hash matcher
/// the byte difference made it a stranger and created one.
#[tokio::test]
async fn rolling_back_to_an_equivalent_template_reuses_its_replicaset() {
    let store = boot("t410-rollback").await;
    let d = put_deployment(&store, 2, template("web:1")).await;
    let dc = controller(&store);
    dc.tick().await.unwrap();
    let first = owned_rs(&store, &d).await;
    assert_eq!(first.len(), 1);
    let a_name = first[0].0.clone();

    put_deployment(&store, 2, template("web:2")).await;
    dc.tick().await.unwrap();
    assert_eq!(owned_rs(&store, &d).await.len(), 2);

    let mut a_again = template("web:1");
    a_again["metadata"]["annotations"] = Value::Null;
    a_again["spec"]["volumes"] = json!([]);
    put_deployment(&store, 2, a_again).await;
    dc.tick().await.unwrap();

    let after = owned_rs(&store, &d).await;
    assert_eq!(after.len(), 2, "no third ReplicaSet: {after:?}");
    for (name, replicas) in &after {
        let expected = if *name == a_name { 2 } else { 0 };
        assert_eq!(*replicas, expected, "{name}: {after:?}");
    }

    drop(dc);
    shutdown(store).await;
}

/// M5. Two owned ReplicaSets run the same template in different bytes,
/// both holding pods. The one holding more stays current at the
/// Deployment's count; the other goes to zero, so the pods total the
/// Deployment's count. Nothing is created.
#[tokio::test]
async fn two_replicasets_running_the_template_leave_the_pods_where_they_are() {
    let store = boot("t410-duplicate").await;
    let plain = template("web:1");
    let mut quirky = template("web:1");
    quirky["spec"]["containers"][0]["args"] = json!([]);
    let d = put_deployment(&store, 3, plain.clone()).await;
    let holding = put_owned_rs(&store, &d, &retired_hash(&quirky), 3, &quirky).await;
    let spare = put_owned_rs(&store, &d, &retired_hash(&plain), 1, &plain).await;

    let dc = controller(&store);
    dc.tick().await.unwrap();

    let mut expected = vec![(holding, 3), (spare, 0)];
    expected.sort();
    assert_eq!(owned_rs(&store, &d).await, expected);

    drop(dc);
    shutdown(store).await;
}
