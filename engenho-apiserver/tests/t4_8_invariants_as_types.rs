//! T4.8 — apiserver invariants carried by types, not by `expect`.
//!
//! Three invariants used to be asserted at runtime:
//!
//!   * an object write's admitted body is `Some` (three
//!     `expect("…preserves Some…")` on create / replace / patch);
//!   * a resolved subresource has an instance name (an `expect` in the GET
//!     dispatch);
//!   * `for_core_kind` names a cataloged core kind in the caller's scope (a
//!     `panic!` plus a `debug_assert_eq!`).
//!
//! Each is now a type: `admit_object` returns a `Value`, `admit_delete`
//! returns `()`, the resolved subresource carries its name, and
//! `for_core_kind` returns `Option`. These tests pin the behaviour of every
//! path that used to lean on those asserts.

use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::params::DryRun;
use engenho_apiserver::{
    ApiError, ApiServer, ResourceHandler, StoreBackedHandler, handlers_from_catalog,
};
use engenho_controllers::{
    AdmissionAction, AdmissionChain, AdmissionDecision, AdmissionMode, FakeAdmissionWebhook,
};
use engenho_store::{InProcessRouter, ResourceKey, StoreMesh, default_config};
use engenho_types::auth::UserInfo;
use engenho_types::patch::PatchType;

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

fn pod_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

fn pod(name: &str, from: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": name, "labels": { "from": from } },
        "spec": { "containers": [{ "name": "app", "image": "podinfo:6" }] }
    })
}

fn label_from(v: &serde_json::Value) -> Option<&str> {
    v.pointer("/metadata/labels/from").and_then(|l| l.as_str())
}

/// A Pod handler whose admission chain is ONE fake webhook the test steers.
fn admitted_pod_handler(
    store: &Arc<StoreMesh>,
    hook: &Arc<FakeAdmissionWebhook>,
) -> StoreBackedHandler {
    let chain = Arc::new(AdmissionChain::new(
        vec![hook.clone()],
        AdmissionMode::FailClosed,
    ));
    StoreBackedHandler::for_core_kind(store.clone(), "Pod", true)
        .expect("Pod is a cataloged core/v1 namespaced kind")
        .with_admission(chain)
}

// ── for_core_kind: a value, never a panic ──────────────────────────────

#[tokio::test]
async fn for_core_kind_answers_none_for_an_uncataloged_kind() {
    let store = boot_store("t48-uncataloged").await;
    assert!(
        StoreBackedHandler::for_core_kind(store, "NotAKind", true).is_none(),
        "an uncataloged kind is None — it used to panic"
    );
}

#[tokio::test]
async fn for_core_kind_answers_none_when_the_caller_scope_disagrees_with_the_catalog() {
    let store = boot_store("t48-scope").await;
    // Pod is namespaced; claiming cluster scope is a caller error.
    assert!(StoreBackedHandler::for_core_kind(store.clone(), "Pod", false).is_none());
    // Namespace is cluster-scoped; claiming namespaced is a caller error.
    assert!(StoreBackedHandler::for_core_kind(store, "Namespace", true).is_none());
}

#[tokio::test]
async fn for_core_kind_answers_none_for_a_kind_outside_the_core_group() {
    let store = boot_store("t48-named-group").await;
    // Deployment is cataloged, but in apps — not core/v1.
    assert!(StoreBackedHandler::for_core_kind(store, "Deployment", true).is_none());
}

/// POSITIVE CONTROL for the three `None` tests above: a cataloged core kind
/// in its catalog scope still builds, with the catalog's plural and scope.
#[tokio::test]
async fn for_core_kind_builds_a_cataloged_core_kind_in_its_own_scope() {
    let store = boot_store("t48-core").await;
    let ep = StoreBackedHandler::for_core_kind(store.clone(), "Endpoints", true)
        .expect("Endpoints is cataloged in core/v1, namespaced");
    assert_eq!(ep.plural(), "endpoints");
    assert!(ep.namespaced());
    assert_eq!(ep.group(), "");
    let ns = StoreBackedHandler::for_core_kind(store, "Namespace", false)
        .expect("Namespace is cataloged in core/v1, cluster-scoped");
    assert!(!ns.namespaced());
}

// ── admit_object: the admitted body is the body that is written ────────

#[tokio::test]
async fn create_under_allow_writes_the_body_it_was_given() {
    let store = boot_store("t48-create-allow").await;
    let hook = Arc::new(FakeAdmissionWebhook::new("allow-all"));
    let h = admitted_pod_handler(&store, &hook);

    h.create(
        Some("default"),
        pod("p", "client"),
        &UserInfo::default(),
        DryRun::Off,
    )
    .await
    .expect("an allowed create succeeds");

    let stored = store.get(&pod_key("p")).await.expect("pod committed");
    assert_eq!(label_from(&stored), Some("client"));
    assert_eq!(
        hook.calls().await,
        vec![(AdmissionAction::Put, pod_key("p").label())],
        "a create is reviewed exactly once, as a Put"
    );
}

#[tokio::test]
async fn replace_under_mutate_writes_the_admitted_body() {
    let store = boot_store("t48-replace-mutate").await;
    let hook = Arc::new(FakeAdmissionWebhook::new("defaulter"));
    let h = admitted_pod_handler(&store, &hook);
    h.create(
        Some("default"),
        pod("p", "seed"),
        &UserInfo::default(),
        DryRun::Off,
    )
    .await
    .expect("seed create");

    hook.set_decision(
        &pod_key("p").label(),
        AdmissionDecision::Mutate(pod("p", "admission")),
    )
    .await;
    let out = h
        .replace(
            Some("default"),
            "p",
            pod("p", "client"),
            &UserInfo::default(),
            DryRun::Off,
        )
        .await
        .expect("a mutated replace succeeds");

    assert_eq!(label_from(&out), Some("admission"), "response body");
    let stored = store.get(&pod_key("p")).await.expect("pod still present");
    assert_eq!(
        label_from(&stored),
        Some("admission"),
        "the webhook's body is what a replace writes, not the client's"
    );
}

#[tokio::test]
async fn patch_under_mutate_applies_the_admitted_patch() {
    let store = boot_store("t48-patch-mutate").await;
    let hook = Arc::new(FakeAdmissionWebhook::new("defaulter"));
    let h = admitted_pod_handler(&store, &hook);
    h.create(
        Some("default"),
        pod("p", "seed"),
        &UserInfo::default(),
        DryRun::Off,
    )
    .await
    .expect("seed create");

    hook.set_decision(
        &pod_key("p").label(),
        AdmissionDecision::Mutate(serde_json::json!({
            "metadata": { "labels": { "from": "admission" } }
        })),
    )
    .await;
    h.patch(
        Some("default"),
        "p",
        serde_json::json!({ "metadata": { "labels": { "from": "client" } } }),
        PatchType::Merge,
        None,
        &UserInfo::default(),
        DryRun::Off,
    )
    .await
    .expect("a mutated patch succeeds");

    let stored = store.get(&pod_key("p")).await.expect("pod still present");
    assert_eq!(
        label_from(&stored),
        Some("admission"),
        "the webhook's patch is what gets applied, not the client's"
    );
    assert_eq!(
        hook.calls().await.last(),
        Some(&(AdmissionAction::Patch, pod_key("p").label())),
        "a patch is reviewed as a Patch"
    );
}

#[tokio::test]
async fn patch_under_deny_is_forbidden_and_changes_nothing() {
    let store = boot_store("t48-patch-deny").await;
    let hook = Arc::new(FakeAdmissionWebhook::new("policy"));
    let h = admitted_pod_handler(&store, &hook);
    h.create(
        Some("default"),
        pod("p", "seed"),
        &UserInfo::default(),
        DryRun::Off,
    )
    .await
    .expect("seed create");

    hook.set_decision(
        &pod_key("p").label(),
        AdmissionDecision::Deny("labels are frozen".into()),
    )
    .await;
    let err = h
        .patch(
            Some("default"),
            "p",
            serde_json::json!({ "metadata": { "labels": { "from": "client" } } }),
            PatchType::Merge,
            None,
            &UserInfo::default(),
            DryRun::Off,
        )
        .await
        .expect_err("a denied patch fails");
    assert!(
        matches!(&err, ApiError::Forbidden(reason) if reason.contains("labels are frozen")),
        "a Deny is a typed 403 carrying the reason: {err:?}"
    );
    let stored = store.get(&pod_key("p")).await.expect("pod still present");
    assert_eq!(label_from(&stored), Some("seed"));
}

// ── admit_delete: a delete is judged, never asked for a body ───────────

#[tokio::test]
async fn delete_under_deny_is_forbidden_and_the_object_survives() {
    let store = boot_store("t48-delete-deny").await;
    let hook = Arc::new(FakeAdmissionWebhook::new("policy"));
    let h = admitted_pod_handler(&store, &hook);
    h.create(
        Some("default"),
        pod("p", "seed"),
        &UserInfo::default(),
        DryRun::Off,
    )
    .await
    .expect("seed create");

    hook.set_decision(
        &pod_key("p").label(),
        AdmissionDecision::Deny("pods here are pinned".into()),
    )
    .await;
    let err = h
        .delete(Some("default"), "p", &UserInfo::default(), DryRun::Off)
        .await
        .expect_err("a denied delete fails");
    assert!(
        matches!(&err, ApiError::Forbidden(reason) if reason.contains("pinned")),
        "a Deny on delete is a typed 403: {err:?}"
    );
    assert!(
        store.get(&pod_key("p")).await.is_some(),
        "a denied delete removes nothing"
    );
    assert_eq!(
        hook.calls().await.last(),
        Some(&(AdmissionAction::Delete, pod_key("p").label())),
        "the delete was reviewed, as a Delete"
    );
}

/// A `Mutate` answer to a delete has no body to rewrite; it admits the
/// delete rather than being mistaken for a refusal or an error.
#[tokio::test]
async fn delete_under_mutate_is_admitted_and_removes_the_object() {
    let store = boot_store("t48-delete-mutate").await;
    let hook = Arc::new(FakeAdmissionWebhook::new("defaulter"));
    let h = admitted_pod_handler(&store, &hook);
    h.create(
        Some("default"),
        pod("p", "seed"),
        &UserInfo::default(),
        DryRun::Off,
    )
    .await
    .expect("seed create");

    hook.set_decision(
        &pod_key("p").label(),
        AdmissionDecision::Mutate(pod("p", "admission")),
    )
    .await;
    h.delete(Some("default"), "p", &UserInfo::default(), DryRun::Off)
        .await
        .expect("a Mutate admits the delete");
    assert!(
        store.get(&pod_key("p")).await.is_none(),
        "the admitted delete removed the pod"
    );
}

// ── the subresource GET dispatch: the name travels with the variant ────

/// `GET …/pods/<name>/status` serves THAT instance. With two pods present, a
/// dispatch that lost track of which name the subresource belongs to would
/// answer with the wrong object or a 404.
#[tokio::test]
async fn a_status_get_serves_the_instance_the_path_names() {
    let store = boot_store("t48-status-get").await;
    let server = ApiServer::start(
        "127.0.0.1:0".parse().expect("loopback addr"),
        handlers_from_catalog(store.clone()),
        None,
    )
    .await
    .expect("apiserver starts");
    let base = format!(
        "http://{}/api/v1/namespaces/default/pods",
        server.local_addr()
    );
    let client = reqwest::Client::new();
    for name in ["first", "second"] {
        let resp = client
            .post(&base)
            .json(&pod(name, name))
            .send()
            .await
            .expect("create request");
        assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    }

    for name in ["second", "first"] {
        let resp = client
            .get(format!("{base}/{name}/status"))
            .send()
            .await
            .expect("status request");
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "{name}/status");
        let body: serde_json::Value = resp.json().await.expect("status body is JSON");
        assert_eq!(
            body.pointer("/metadata/name").and_then(|n| n.as_str()),
            Some(name),
            "{name}/status serves {name}"
        );
    }

    server.shutdown().await.expect("apiserver stops");
}
