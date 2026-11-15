use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::field_validation::PatchFields;
use engenho_apiserver::params::DryRun;
use engenho_apiserver::{ApiError, ObjectBody, ResourceHandler, StoreBackedHandler};
use engenho_controllers::{Controller, NamespaceController};
use engenho_store::{InProcessRouter, ResourceKey, StoreMesh, default_config};
use engenho_types::auth::UserInfo;
use engenho_types::patch::PatchType;
use serde_json::{Value, json};

const NS: &str = "env-core1";
const HOLD: &str = "finalizers.example.com/uninstall";

async fn boot_store() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("namespace-deletion").expect("store config");
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .expect("store starts"),
    );
    store.initialize_singleton().await.expect("singleton init");
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

struct Api {
    namespaces: StoreBackedHandler,
    crds: StoreBackedHandler,
    secrets: StoreBackedHandler,
    pods: StoreBackedHandler,
    configmaps: StoreBackedHandler,
    widgets: StoreBackedHandler,
}

impl Api {
    fn new(store: &Arc<StoreMesh>) -> Self {
        let kind = |k: &str| {
            StoreBackedHandler::for_kind(store.clone(), k)
                .expect("a cataloged kind")
                .with_namespace_lifecycle()
        };
        Self {
            namespaces: kind("Namespace"),
            crds: kind("CustomResourceDefinition"),
            secrets: kind("Secret"),
            pods: kind("Pod"),
            configmaps: kind("ConfigMap"),
            widgets: StoreBackedHandler::new(
                store.clone(),
                "example.com",
                "v1",
                "Widget",
                "widgets",
                true,
            )
            .with_namespace_lifecycle(),
        }
    }
}

fn body(group: &str, kind: &str, value: Value) -> ObjectBody {
    ObjectBody::normalize(group, kind, value).expect("a well-shaped body")
}

async fn create(
    h: &StoreBackedHandler,
    namespace: Option<&str>,
    group: &str,
    kind: &str,
    value: Value,
) -> Result<Value, ApiError> {
    h.create(
        namespace,
        body(group, kind, value),
        &UserInfo::default(),
        DryRun::Off,
    )
    .await
}

fn widget_crd() -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"plural": "widgets", "singular": "widget", "kind": "Widget", "listKind": "WidgetList"},
            "versions": [{
                "name": "v1", "served": true, "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}
            }]
        }
    })
}

async fn populate(api: &Api) {
    create(
        &api.namespaces,
        None,
        "",
        "Namespace",
        json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS}}),
    )
    .await
    .expect("namespace created");
    create(
        &api.secrets,
        Some(NS),
        "",
        "Secret",
        json!({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "environment-seed", "namespace": NS}, "stringData": {"seed": "s"}}),
    )
    .await
    .expect("secret created");
    create(
        &api.pods,
        Some(NS),
        "",
        "Pod",
        json!({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "database-schema-0", "namespace": NS},
               "spec": {"restartPolicy": "Never", "containers": [{"name": "schema", "image": "busybox"}]}}),
    )
    .await
    .expect("pod created");
    create(
        &api.widgets,
        Some(NS),
        "example.com",
        "Widget",
        json!({"apiVersion": "example.com/v1", "kind": "Widget",
               "metadata": {"name": "release", "namespace": NS, "finalizers": [HOLD]}}),
    )
    .await
    .expect("widget created");
}

fn namespaced(group: &str, kind: &str, name: &str) -> ResourceKey {
    ResourceKey::namespaced(group, "v1", kind, NS, name)
}

fn namespace_key() -> ResourceKey {
    ResourceKey::cluster_scoped("", "v1", "Namespace", NS)
}

async fn contents(store: &StoreMesh) -> Vec<String> {
    let mut held = Vec::new();
    for (group, kind) in [
        ("", "Secret"),
        ("", "Pod"),
        ("", "ConfigMap"),
        ("example.com", "Widget"),
    ] {
        for (key, _) in store.list(group, "v1", kind, Some(NS)).await {
            held.push(key.label());
        }
    }
    held
}

#[tokio::test]
async fn deleting_a_namespace_deletes_every_object_in_it_first_and_honours_their_finalizers() {
    let store = boot_store().await;
    let api = Api::new(&store);
    let user = UserInfo::default();
    create(
        &api.crds,
        None,
        "apiextensions.k8s.io",
        "CustomResourceDefinition",
        widget_crd(),
    )
    .await
    .expect("crd created");
    populate(&api).await;
    assert_eq!(contents(&store).await.len(), 3);

    api.namespaces
        .delete(None, NS, &user, DryRun::Off)
        .await
        .expect("namespace delete accepted");
    let terminating = store
        .get(&namespace_key())
        .await
        .expect("the namespace stays while it holds objects");
    assert!(
        terminating
            .pointer("/metadata/deletionTimestamp")
            .and_then(Value::as_str)
            .is_some(),
        "the namespace is Terminating: {terminating}"
    );

    let refused = create(
        &api.configmaps,
        Some(NS),
        "",
        "ConfigMap",
        json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "late", "namespace": NS}}),
    )
    .await
    .expect_err("a create into a Terminating namespace is refused");
    assert!(
        matches!(refused, ApiError::Forbidden(ref m) if m.contains("because it is being terminated")),
        "{refused:?}"
    );

    let controller = NamespaceController::new(store.clone(), None);
    controller.tick().await.expect("sweep");
    assert!(
        store
            .get(&namespaced("", "Secret", "environment-seed"))
            .await
            .is_none()
    );
    assert!(
        store
            .get(&namespaced("", "Pod", "database-schema-0"))
            .await
            .is_none()
    );
    let widget = store
        .get(&namespaced("example.com", "Widget", "release"))
        .await
        .expect("the widget waits on its own finalizer");
    assert!(
        widget.pointer("/metadata/deletionTimestamp").is_some(),
        "the CRD object was deleted: {widget}"
    );
    assert_eq!(
        store
            .get(&namespace_key())
            .await
            .expect("namespace held by the widget")
            .pointer("/status/phase"),
        Some(&json!("Terminating"))
    );

    controller.tick().await.expect("sweep");
    assert!(
        store.get(&namespace_key()).await.is_some(),
        "the namespace waits while a contained object holds a finalizer"
    );

    api.widgets
        .patch(
            Some(NS),
            "release",
            json!({"metadata": {"finalizers": null}}),
            PatchType::Merge,
            None,
            &user,
            DryRun::Off,
            &mut PatchFields::unasked(),
        )
        .await
        .expect("the widget's controller releases it");
    assert!(
        contents(&store).await.is_empty(),
        "every contained object is gone"
    );
    assert!(
        store.get(&namespace_key()).await.is_some(),
        "the namespace outlives its contents"
    );

    controller.tick().await.expect("sweep");
    assert!(
        store.get(&namespace_key()).await.is_none(),
        "empty, so it goes"
    );

    create(
        &api.namespaces,
        None,
        "",
        "Namespace",
        json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS}}),
    )
    .await
    .expect("namespace recreated");
    assert_eq!(
        contents(&store).await,
        Vec::<String>::new(),
        "a namespace recreated with the same name is empty"
    );
}
