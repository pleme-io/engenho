//! T4.4 — a POST or PUT body is normalized at the border; a PATCH body never
//! is.
//!
//! Upstream decodes a create/update body into its Go type before storing it,
//! so `metadata.labels: null` is stored as absent and a label value of `null`
//! is stored as `""`. engenho stored the JSON as it arrived, and a stored
//! `annotations: null` is what crashed controllers that read annotations as a
//! map (T4.3). These tests drive the real router over HTTP:
//!
//!   * PUT and POST store the normalized object, and answer with it;
//!   * a mis-shaped `metadata` is upstream's 400 and nothing is stored;
//!   * a mutating webhook cannot put the nulls back (a mis-shaped webhook
//!     body is upstream's 500);
//!   * a merge patch's `null` still DELETES — the patch body is never
//!     normalized, because there `null` means "remove this field" (edge 9).

use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{ApiServer, ResourceHandler, StoreBackedHandler};
use engenho_controllers::{AdmissionChain, AdmissionDecision, AdmissionMode, FakeAdmissionWebhook};
use engenho_store::{InProcessRouter, ResourceKey, StoreMesh, default_config};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

const CONFIGMAPS: &str = "api/v1/namespaces/default/configmaps";
const DEPLOYMENTS: &str = "apis/apps/v1/namespaces/default/deployments";
const PODS: &str = "api/v1/namespaces/default/pods";

struct Cluster {
    _server: ApiServer,
    base: String,
    client: Client,
    hook: Arc<FakeAdmissionWebhook>,
}

impl Cluster {
    async fn boot(name: &str) -> Self {
        let router = InProcessRouter::new();
        let cfg = default_config(name).expect("store config");
        let store = Arc::new(
            StoreMesh::start(1, "in-process://1".into(), router, cfg)
                .await
                .expect("store starts"),
        );
        store.initialize_singleton().await.expect("singleton init");
        assert!(store.wait_for_leadership(Duration::from_secs(3)).await);

        let hook = Arc::new(FakeAdmissionWebhook::new("mutator"));
        let chain = Arc::new(AdmissionChain::new(
            vec![hook.clone()],
            AdmissionMode::FailClosed,
        ));
        let handlers: Vec<Arc<dyn ResourceHandler>> = vec![
            Arc::new(StoreBackedHandler::for_kind(store.clone(), "ConfigMap").expect("cataloged")),
            Arc::new(StoreBackedHandler::for_kind(store.clone(), "Deployment").expect("cataloged")),
            Arc::new(
                StoreBackedHandler::for_kind(store.clone(), "Pod")
                    .expect("cataloged")
                    .with_admission(chain),
            ),
        ];
        let server = ApiServer::start("127.0.0.1:0".parse().expect("addr"), handlers, None)
            .await
            .expect("server starts");
        let base = ["http://", &server.local_addr().to_string(), "/"].concat();
        Self {
            _server: server,
            base,
            client: Client::new(),
            hook,
        }
    }

    fn url(&self, collection: &str, name: Option<&str>) -> String {
        match name {
            Some(n) => [&self.base, collection, "/", n].concat(),
            None => [&self.base, collection].concat(),
        }
    }

    async fn post(&self, collection: &str, body: &Value) -> (StatusCode, Value) {
        let resp = self
            .client
            .post(self.url(collection, None))
            .json(body)
            .send()
            .await
            .expect("POST sent");
        (resp.status(), resp.json().await.expect("JSON body"))
    }

    async fn put(&self, collection: &str, name: &str, body: &Value) -> (StatusCode, Value) {
        let resp = self
            .client
            .put(self.url(collection, Some(name)))
            .json(body)
            .send()
            .await
            .expect("PUT sent");
        (resp.status(), resp.json().await.expect("JSON body"))
    }

    async fn merge_patch(&self, collection: &str, name: &str, patch: &Value) -> StatusCode {
        self.client
            .patch(self.url(collection, Some(name)))
            .header("Content-Type", "application/merge-patch+json")
            .body(patch.to_string())
            .send()
            .await
            .expect("PATCH sent")
            .status()
    }

    async fn get(&self, collection: &str, name: &str) -> (StatusCode, Value) {
        let resp = self
            .client
            .get(self.url(collection, Some(name)))
            .send()
            .await
            .expect("GET sent");
        (resp.status(), resp.json().await.expect("JSON body"))
    }
}

fn configmap(metadata: Value) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": metadata,
        "data": { "k": "v" }
    })
}

fn meta(object: &Value) -> &serde_json::Map<String, Value> {
    object
        .get("metadata")
        .and_then(Value::as_object)
        .expect("the object has a metadata object")
}

/// The four fields upstream stores as absent when they arrive `null`.
const NULLABLE: [&str; 4] = ["labels", "annotations", "ownerReferences", "finalizers"];

#[tokio::test]
async fn a_put_stores_null_metadata_fields_as_absent() {
    let c = Cluster::boot("t44-put-nulls").await;
    let (status, _) = c
        .post(
            CONFIGMAPS,
            &configmap(json!({"name": "cm", "labels": {"a": "1"}})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, answered) = c
        .put(
            CONFIGMAPS,
            "cm",
            &configmap(json!({
                "name": "cm",
                "labels": null,
                "annotations": null,
                "ownerReferences": null,
                "finalizers": null
            })),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{answered}");
    let (_, stored) = c.get(CONFIGMAPS, "cm").await;
    for field in NULLABLE {
        assert_eq!(meta(&answered).get(field), None, "PUT answer: {field}");
        assert_eq!(meta(&stored).get(field), None, "stored: {field}");
    }
    assert_eq!(stored["data"], json!({"k": "v"}), "the rest is written");
}

#[tokio::test]
async fn a_put_stores_a_null_label_value_as_the_empty_string() {
    let c = Cluster::boot("t44-put-entry").await;
    let (status, _) = c.post(CONFIGMAPS, &configmap(json!({"name": "cm"}))).await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, answered) = c
        .put(
            CONFIGMAPS,
            "cm",
            &configmap(json!({
                "name": "cm",
                "labels": {"keep": "v", "blank": null},
                "annotations": {"note": null}
            })),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{answered}");
    let (_, stored) = c.get(CONFIGMAPS, "cm").await;
    assert_eq!(
        stored["metadata"]["labels"],
        json!({"keep": "v", "blank": ""})
    );
    assert_eq!(stored["metadata"]["annotations"], json!({"note": ""}));
}

#[tokio::test]
async fn a_post_normalizes_a_deployments_pod_template() {
    let c = Cluster::boot("t44-post-template").await;
    let (status, answered) = c
        .post(
            DEPLOYMENTS,
            &json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "web", "annotations": null},
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "web"}},
                    "template": {
                        "metadata": {
                            "labels": {"app": "web"},
                            "annotations": null,
                            "finalizers": null
                        },
                        "spec": {"containers": [{"name": "nginx", "image": "nginx:1.27"}]}
                    }
                }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{answered}");
    let (_, stored) = c.get(DEPLOYMENTS, "web").await;
    assert_eq!(meta(&stored).get("annotations"), None);
    assert_eq!(
        stored["spec"]["template"]["metadata"],
        json!({"labels": {"app": "web"}}),
        "the template's null fields are absent, its labels kept"
    );
}

#[tokio::test]
async fn mis_shaped_metadata_is_upstreams_400_and_nothing_is_stored() {
    let c = Cluster::boot("t44-mis-shaped").await;

    let (status, answer) = c
        .post(
            CONFIGMAPS,
            &configmap(json!({"name": "numeric", "labels": {"a": 5}})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{answer}");
    assert_eq!(answer["reason"], "BadRequest");
    assert_eq!(
        answer["message"],
        "invalid request: ConfigMap in version \"v1\" cannot be handled as a ConfigMap: \
         metadata.labels[a]: expected string, got number"
    );
    let (status, _) = c.get(CONFIGMAPS, "numeric").await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "a refused POST stores nothing"
    );

    let (status, _) = c
        .post(
            CONFIGMAPS,
            &configmap(json!({"name": "cm", "labels": {"a": "1"}})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, answer) = c
        .put(
            CONFIGMAPS,
            "cm",
            &configmap(json!({"name": "cm", "ownerReferences": {}})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{answer}");
    let message = answer["message"].as_str().unwrap_or_default();
    assert!(
        message.ends_with("metadata.ownerReferences: expected array, got object"),
        "{message}"
    );
    let (_, stored) = c.get(CONFIGMAPS, "cm").await;
    assert_eq!(
        stored["metadata"]["labels"],
        json!({"a": "1"}),
        "PUT refused"
    );
    assert_eq!(meta(&stored).get("ownerReferences"), None);
}

#[tokio::test]
async fn a_merge_patch_null_still_deletes() {
    let c = Cluster::boot("t44-merge-null").await;
    let (status, _) = c
        .post(
            CONFIGMAPS,
            &configmap(json!({
                "name": "cm",
                "labels": {"a": "1", "b": "2"},
                "annotations": {"note": "x"}
            })),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    // `kubectl label cm a-` and `kubectl annotate --overwrite` reduce to
    // exactly these two nulls.
    let status = c
        .merge_patch(
            CONFIGMAPS,
            "cm",
            &json!({"metadata": {"labels": {"a": null}, "annotations": null}}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let (_, stored) = c.get(CONFIGMAPS, "cm").await;
    assert_eq!(
        stored["metadata"]["labels"],
        json!({"b": "2"}),
        "label `a` is deleted, not set to \"\""
    );
    assert_eq!(
        meta(&stored).get("annotations"),
        None,
        "`annotations: null` deletes every annotation"
    );
}

fn pod_key() -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", "p")
}

fn pod(metadata: Value) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": metadata,
        "spec": {"containers": [{"name": "app", "image": "podinfo:6"}]}
    })
}

#[tokio::test]
async fn a_mutating_webhook_cannot_put_the_nulls_back() {
    let c = Cluster::boot("t44-webhook-nulls").await;
    c.hook
        .set_decision(
            &pod_key().label(),
            AdmissionDecision::Mutate(pod(json!({
                "name": "p",
                "labels": null,
                "annotations": {"injected": null},
                "finalizers": null
            }))),
        )
        .await;
    let (status, answered) = c.post(PODS, &pod(json!({"name": "p"}))).await;
    assert_eq!(status, StatusCode::CREATED, "{answered}");
    let (_, stored) = c.get(PODS, "p").await;
    assert_eq!(meta(&stored).get("labels"), None);
    assert_eq!(meta(&stored).get("finalizers"), None);
    assert_eq!(stored["metadata"]["annotations"], json!({"injected": ""}));
}

#[tokio::test]
async fn a_webhook_that_mis_shapes_metadata_is_upstreams_500() {
    let c = Cluster::boot("t44-webhook-shape").await;
    c.hook
        .set_decision(
            &pod_key().label(),
            AdmissionDecision::Mutate(pod(json!({"name": "p", "finalizers": "x"}))),
        )
        .await;
    let (status, answer) = c.post(PODS, &pod(json!({"name": "p"}))).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{answer}");
    assert_eq!(answer["reason"], "InternalError");
    let (status, _) = c.get(PODS, "p").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "nothing stored");
}
