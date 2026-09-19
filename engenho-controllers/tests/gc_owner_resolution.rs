//! The gc controller resolves a dependent's owner by that owner's OWN
//! apiVersion + kind, and fails closed.
//!
//! Measured on ryn 2026-09-18: `pitr-lab/mysql-0`, owned by a StatefulSet,
//! was deleted ~10 times per SECOND for days, because gc built its set of
//! live owner UIDs from two hardcoded kinds — Deployment and ReplicaSet —
//! and read "uid not in my set" as "orphan". The statefulset controller
//! recreated the pod, the scheduler bound it, gc deleted it again. Three
//! controllers ran flat out, each reporting `changed=1` every tick, and
//! the resulting event storm overflowed the kubelet's watch buffer.
//!
//! These tests pin the rule in both directions: an owner gc was never
//! taught about keeps its dependent, and a genuinely absent owner still
//! loses it.

use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::{Controller, GcController, OwnerReference, set_owner_reference};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::{Value, json};

async fn boot_store(tag: &str) -> Arc<StoreMesh> {
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
        .propose(ResourceCommand::Put {
            key: key.clone(),
            value,
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    store.get(key).await.expect("stored")
}

fn uid_of(v: &Value) -> String {
    v.get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|u| u.as_str())
        .expect("uid assigned")
        .to_string()
}

/// A StatefulSet is a kind gc never enumerated. Its pod must survive.
#[tokio::test]
async fn a_pod_owned_by_a_statefulset_is_not_an_orphan() {
    let store = boot_store("gc-sts").await;

    let sts_key = ResourceKey::namespaced("apps", "v1", "StatefulSet", "pitr-lab", "mysql");
    let sts = put(
        &store,
        &sts_key,
        json!({
            "kind": "StatefulSet",
            "apiVersion": "apps/v1",
            "metadata": { "name": "mysql" },
            "spec": { "replicas": 1 }
        }),
    )
    .await;

    let pod_key = ResourceKey::namespaced("", "v1", "Pod", "pitr-lab", "mysql-0");
    let mut pod = json!({
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": { "name": "mysql-0" },
        "spec": { "containers": [{ "name": "mysql", "image": "mysql:8.0" }] }
    });
    set_owner_reference(
        &mut pod,
        OwnerReference {
            api_version: "apps/v1".into(),
            kind: "StatefulSet".into(),
            name: "mysql".into(),
            uid: uid_of(&sts),
            controller: true,
            block_owner_deletion: true,
        },
    )
    .unwrap();
    put(&store, &pod_key, pod).await;

    let gc = GcController::new(store.clone(), None);
    let outcome = gc.tick().await.expect("gc tick");

    assert_eq!(
        outcome.report.objects_changed, 0,
        "gc deleted a StatefulSet's pod as an orphan; that is the ryn busy-loop"
    );
    assert!(
        store.get(&pod_key).await.is_some(),
        "mysql-0 must survive a gc tick while its StatefulSet exists"
    );
}

/// The other direction, so the guard above is not vacuous: an owner that
/// is genuinely gone still orphans its dependent.
#[tokio::test]
async fn a_pod_whose_owner_does_not_exist_is_still_collected() {
    let store = boot_store("gc-absent").await;

    let pod_key = ResourceKey::namespaced("", "v1", "Pod", "pitr-lab", "ghost-0");
    let mut pod = json!({
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": { "name": "ghost-0" },
        "spec": { "containers": [{ "name": "c", "image": "x" }] }
    });
    set_owner_reference(
        &mut pod,
        OwnerReference {
            api_version: "apps/v1".into(),
            kind: "StatefulSet".into(),
            name: "never-existed".into(),
            uid: "uid-that-was-never-stored".into(),
            controller: true,
            block_owner_deletion: true,
        },
    )
    .unwrap();
    put(&store, &pod_key, pod).await;

    let gc = GcController::new(store.clone(), None);
    let outcome = gc.tick().await.expect("gc tick");

    assert_eq!(
        outcome.report.objects_changed, 1,
        "an absent owner must still orphan its dependent"
    );
}

/// An owner recreated under the same name is a different object, and the
/// dependent belongs to the generation that died.
#[tokio::test]
async fn a_uid_mismatch_on_a_live_name_still_orphans() {
    let store = boot_store("gc-uid").await;

    let sts_key = ResourceKey::namespaced("apps", "v1", "StatefulSet", "pitr-lab", "mysql");
    put(
        &store,
        &sts_key,
        json!({
            "kind": "StatefulSet",
            "apiVersion": "apps/v1",
            "metadata": { "name": "mysql" },
            "spec": { "replicas": 1 }
        }),
    )
    .await;

    let pod_key = ResourceKey::namespaced("", "v1", "Pod", "pitr-lab", "mysql-0");
    let mut pod = json!({
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": { "name": "mysql-0" },
        "spec": { "containers": [{ "name": "mysql", "image": "mysql:8.0" }] }
    });
    set_owner_reference(
        &mut pod,
        OwnerReference {
            api_version: "apps/v1".into(),
            kind: "StatefulSet".into(),
            name: "mysql".into(),
            uid: "uid-of-a-previous-incarnation".into(),
            controller: true,
            block_owner_deletion: true,
        },
    )
    .unwrap();
    put(&store, &pod_key, pod).await;

    let gc = GcController::new(store.clone(), None);
    let outcome = gc.tick().await.expect("gc tick");

    assert_eq!(
        outcome.report.objects_changed, 1,
        "a live name under a different uid is a dead generation's dependent"
    );
}

// ── I19: every owner reference, the served catalog, the foreground finalizer ─
//
// Each rule below is a row of upstream's garbage collector (v1.34.0), checked
// in full by tests/oracle_gc.rs. These pin the same rules through the whole
// controller over a real store.

fn reference(
    api_version: &str,
    kind: &str,
    name: &str,
    uid: &str,
    controller: bool,
) -> OwnerReference {
    OwnerReference {
        api_version: api_version.into(),
        kind: kind.into(),
        name: name.into(),
        uid: uid.into(),
        controller,
        block_owner_deletion: controller,
    }
}

/// A pod in `ns` naming `owners`, in order.
async fn owned_pod(
    store: &StoreMesh,
    ns: &str,
    name: &str,
    owners: &[OwnerReference],
) -> ResourceKey {
    let key = ResourceKey::namespaced("", "v1", "Pod", ns, name);
    let mut pod = json!({
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": { "name": name },
        "spec": { "containers": [{ "name": "c", "image": "x" }] }
    });
    for owner in owners {
        set_owner_reference(&mut pod, owner.clone()).unwrap();
    }
    put(store, &key, pod).await;
    key
}

async fn statefulset(store: &StoreMesh, ns: &str, name: &str, finalizers: &[&str]) -> Value {
    let key = ResourceKey::namespaced("apps", "v1", "StatefulSet", ns, name);
    put(
        store,
        &key,
        json!({
            "kind": "StatefulSet",
            "apiVersion": "apps/v1",
            "metadata": { "name": name, "finalizers": finalizers },
            "spec": { "replicas": 1 }
        }),
    )
    .await
}

/// Delete `key` the way the apiserver does: a finalizer-bearing object goes
/// Terminating and stays.
async fn begin_deletion(store: &StoreMesh, key: &ResourceKey) -> Value {
    store
        .propose(ResourceCommand::delete(key.clone(), Reason::Operator))
        .await
        .unwrap();
    let terminating = store.get(key).await.expect("held by its finalizer");
    assert!(
        terminating["metadata"]["deletionTimestamp"].is_string(),
        "{terminating}"
    );
    terminating
}

fn owner_uids(value: &Value) -> Vec<String> {
    value["metadata"]["ownerReferences"]
        .as_array()
        .map(|refs| {
            refs.iter()
                .filter_map(|r| r["uid"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

async fn tick(store: &Arc<StoreMesh>) -> usize {
    GcController::new(store.clone(), None)
        .tick()
        .await
        .expect("gc tick")
        .report
        .objects_changed
}

/// Upstream classifies EVERY ownerReference: a pod whose controller is gone
/// but whose other owner lives is kept, and only the dead reference goes.
#[tokio::test]
async fn a_live_second_owner_keeps_the_pod_and_the_dead_reference_goes() {
    let store = boot_store("gc-all-owners").await;
    let live = statefulset(&store, "ns", "live", &[]).await;
    let pod = owned_pod(
        &store,
        "ns",
        "p",
        &[
            reference("apps/v1", "ReplicaSet", "gone", "uid-never-stored", true),
            reference("apps/v1", "StatefulSet", "live", &uid_of(&live), false),
        ],
    )
    .await;

    assert_eq!(
        tick(&store).await,
        1,
        "one write: the dead reference is removed"
    );
    let after = store
        .get(&pod)
        .await
        .expect("a pod with a live owner is never deleted");
    assert_eq!(owner_uids(&after), [uid_of(&live)], "{after}");

    assert_eq!(
        tick(&store).await,
        0,
        "a pod whose every owner is solid needs nothing"
    );
}

/// No solid owner among several: the pod is deleted.
#[tokio::test]
async fn a_pod_whose_every_owner_is_gone_is_deleted() {
    let store = boot_store("gc-all-gone").await;
    let pod = owned_pod(
        &store,
        "ns",
        "p",
        &[
            reference("apps/v1", "ReplicaSet", "gone", "uid-a", true),
            reference("apps/v1", "StatefulSet", "also-gone", "uid-b", false),
        ],
    )
    .await;
    assert_eq!(tick(&store).await, 1);
    assert!(store.get(&pod).await.is_none());
}

/// A well-formed apiVersion nothing serves is unresolvable, never absent:
/// the pod stays, whether or not the owner exists at a served version.
#[tokio::test]
async fn an_unserved_api_version_never_deletes_the_pod() {
    let store = boot_store("gc-unserved").await;
    let deploy_key = ResourceKey::namespaced("apps", "v1", "Deployment", "ns", "web");
    let deploy = put(
        &store,
        &deploy_key,
        json!({"kind": "Deployment", "apiVersion": "apps/v1", "metadata": {"name": "web"}}),
    )
    .await;
    let of_live = owned_pod(
        &store,
        "ns",
        "of-live",
        &[reference(
            "extensions/v1beta1",
            "Deployment",
            "web",
            &uid_of(&deploy),
            true,
        )],
    )
    .await;
    let of_gone = owned_pod(
        &store,
        "ns",
        "of-gone",
        &[reference(
            "extensions/v1beta1",
            "Deployment",
            "gone",
            "uid-gone",
            true,
        )],
    )
    .await;
    let before = (store.get(&of_live).await, store.get(&of_gone).await);

    assert_eq!(tick(&store).await, 0);
    assert_eq!(
        (store.get(&of_live).await, store.get(&of_gone).await),
        before
    );
}

/// One reference that cannot be resolved pins the pod, even beside a
/// verifiably dangling one: no delete, no patch.
#[tokio::test]
async fn an_unresolvable_reference_pins_the_pod_beside_a_dangling_one() {
    let store = boot_store("gc-first-error").await;
    let pod = owned_pod(
        &store,
        "ns",
        "p",
        &[
            reference("apps/v1", "ReplicaSet", "gone", "uid-gone", true),
            reference("test/v1", "invalid0", "invalid", "invalid-0", false),
        ],
    )
    .await;
    let before = store.get(&pod).await;
    assert_eq!(tick(&store).await, 0);
    assert_eq!(store.get(&pod).await, before);
}

/// An owner being deleted in the foreground waits for its dependents: it
/// does not keep them. Alone, it gets its pod deleted; beside a live owner,
/// its reference is dropped so it can finish.
#[tokio::test]
async fn a_foreground_deleting_owner_does_not_keep_its_pods() {
    let store = boot_store("gc-foreground").await;
    let fg_key = ResourceKey::namespaced("apps", "v1", "StatefulSet", "ns", "fg");
    let fg = statefulset(&store, "ns", "fg", &["foregroundDeletion"]).await;
    begin_deletion(&store, &fg_key).await;
    let live = statefulset(&store, "ns", "live", &[]).await;
    let fg_ref = reference("apps/v1", "StatefulSet", "fg", &uid_of(&fg), true);

    let alone = owned_pod(&store, "ns", "alone", std::slice::from_ref(&fg_ref)).await;
    let beside = owned_pod(
        &store,
        "ns",
        "beside",
        &[
            fg_ref,
            reference("apps/v1", "StatefulSet", "live", &uid_of(&live), false),
        ],
    )
    .await;

    assert_eq!(tick(&store).await, 2, "one delete, one reference removed");
    assert!(
        store.get(&alone).await.is_none(),
        "a waiting owner alone is not solid"
    );
    let kept = store.get(&beside).await.expect("the live owner keeps it");
    assert_eq!(owner_uids(&kept), [uid_of(&live)], "{kept}");
}

/// The other direction: a Terminating owner WITHOUT the foreground finalizer
/// is solid (it may be orphaning its dependents), so its pod stays.
#[tokio::test]
async fn a_terminating_owner_without_the_foreground_finalizer_keeps_its_pod() {
    let store = boot_store("gc-orphaning").await;
    let key = ResourceKey::namespaced("apps", "v1", "StatefulSet", "ns", "orphaning");
    let owner = statefulset(&store, "ns", "orphaning", &["orphan"]).await;
    begin_deletion(&store, &key).await;
    let pod = owned_pod(
        &store,
        "ns",
        "p",
        &[reference(
            "apps/v1",
            "StatefulSet",
            "orphaning",
            &uid_of(&owner),
            true,
        )],
    )
    .await;
    assert_eq!(tick(&store).await, 0);
    assert!(store.get(&pod).await.is_some());
}

async fn widget_crd(store: &StoreMesh) {
    let key = ResourceKey::cluster_scoped(
        "apiextensions.k8s.io",
        "v1",
        "CustomResourceDefinition",
        "widgets.example.com",
    );
    put(
        store,
        &key,
        json!({
            "kind": "CustomResourceDefinition",
            "apiVersion": "apiextensions.k8s.io/v1",
            "metadata": {"name": "widgets.example.com"},
            "spec": {
                "group": "example.com",
                "scope": "Namespaced",
                "names": {"kind": "Widget", "plural": "widgets"},
                "versions": [
                    {"name": "v1", "served": true, "storage": true},
                    {"name": "v2", "served": true}
                ]
            }
        }),
    )
    .await;
}

/// engenho stores a custom resource under the version it was written at.
/// A reference through another SERVED version finds it there, so the pod
/// stays; with the owner gone at every served version, the pod goes.
#[tokio::test]
async fn a_custom_owner_stored_under_another_served_version_keeps_its_pod() {
    let store = boot_store("gc-crd-versions").await;
    widget_crd(&store).await;
    let widget = put(
        &store,
        &ResourceKey::namespaced("example.com", "v1", "Widget", "ns", "w"),
        json!({"kind": "Widget", "apiVersion": "example.com/v1", "metadata": {"name": "w"}}),
    )
    .await;
    let kept = owned_pod(
        &store,
        "ns",
        "kept",
        &[reference(
            "example.com/v2",
            "Widget",
            "w",
            &uid_of(&widget),
            true,
        )],
    )
    .await;
    let orphan = owned_pod(
        &store,
        "ns",
        "orphan",
        &[reference(
            "example.com/v2",
            "Widget",
            "gone",
            "uid-gone",
            true,
        )],
    )
    .await;

    assert_eq!(tick(&store).await, 1);
    assert!(store.get(&kept).await.is_some(), "the owner lives under v1");
    assert!(store.get(&orphan).await.is_none(), "positive control");
}

/// A kind with no CRD serving it is unresolvable: its dependents stay.
#[tokio::test]
async fn a_custom_owner_kind_nothing_serves_never_deletes_the_pod() {
    let store = boot_store("gc-crd-absent").await;
    let pod = owned_pod(
        &store,
        "ns",
        "p",
        &[reference(
            "example.com/v1",
            "Widget",
            "gone",
            "uid-gone",
            true,
        )],
    )
    .await;
    assert_eq!(tick(&store).await, 0);
    assert!(store.get(&pod).await.is_some());
}

/// A cluster-scoped owner is looked up cluster-wide (a mirror pod names its
/// Node), and an all-lowercase kind resolves, as client-go's mapper does.
#[tokio::test]
async fn a_cluster_scoped_or_lowercase_owner_resolves() {
    let store = boot_store("gc-scope").await;
    let node = put(
        &store,
        &ResourceKey::cluster_scoped("", "v1", "Node", "n1"),
        json!({"kind": "Node", "apiVersion": "v1", "metadata": {"name": "n1"}}),
    )
    .await;
    let sts = statefulset(&store, "ns", "db", &[]).await;
    let mirror = owned_pod(
        &store,
        "ns",
        "mirror",
        &[reference("v1", "Node", "n1", &uid_of(&node), true)],
    )
    .await;
    let lower = owned_pod(
        &store,
        "ns",
        "lower",
        &[reference(
            "apps/v1",
            "statefulset",
            "db",
            &uid_of(&sts),
            true,
        )],
    )
    .await;
    let gone = owned_pod(
        &store,
        "ns",
        "gone",
        &[reference("v1", "Node", "n2", "uid-n2", true)],
    )
    .await;

    assert_eq!(tick(&store).await, 1);
    assert!(
        store.get(&mirror).await.is_some(),
        "its Node exists cluster-wide"
    );
    assert!(
        store.get(&lower).await.is_some(),
        "`statefulset` resolves to StatefulSet"
    );
    assert!(store.get(&gone).await.is_none(), "positive control");
}
