use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::openapi_v2::pb;
use engenho_apiserver::{ApiServer, ResourceHandler, StoreBackedHandler, handlers_from_catalog};
use engenho_store::{InProcessRouter, StoreMesh, default_config};
use prost::Message;
use serde_json::{Value, json};

const CLIENT_GO_ACCEPT: &str = "application/com.github.proto-openapi.spec.v2@v1.0+protobuf";

async fn boot() -> ApiServer {
    let cfg = default_config("apiserver-openapi-v2").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), InProcessRouter::new(), cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    let mut handlers = handlers_from_catalog(store.clone());
    let widgets: Arc<dyn ResourceHandler> = Arc::new(
        StoreBackedHandler::new(store, "example.com", "v1", "Widget", "widgets", true)
            .with_crd_schema(json!({
                "type": "object",
                "properties": {
                    "spec": {
                        "type": "object",
                        "required": ["size"],
                        "properties": {
                            "size": {"type": "integer"},
                            "labels": {"type": "array"},
                            "values": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}
                        }
                    }
                }
            })),
    );
    handlers.push(widgets);
    ApiServer::start("127.0.0.1:0".parse().unwrap(), handlers, None)
        .await
        .unwrap()
}

async fn get(server: &ApiServer, accept: &str) -> reqwest::Response {
    reqwest::Client::new()
        .get(format!("http://{}/openapi/v2", server.local_addr()))
        .header(reqwest::header::ACCEPT, accept)
        .send()
        .await
        .unwrap()
}

fn gvk_of(schema: &pb::Schema) -> Option<Value> {
    schema
        .vendor_extension
        .iter()
        .find(|e| e.name == "x-kubernetes-group-version-kind")
        .and_then(|e| e.value.as_ref())
        .map(|v| serde_json::from_str(&v.yaml).unwrap())
}

fn assert_kube_openapi_parses(
    name: &str,
    schema: &Value,
    definitions: &serde_json::Map<String, Value>,
) {
    if let Some(target) = schema.get("$ref").and_then(Value::as_str) {
        let model = target
            .strip_prefix("#/definitions/")
            .expect("a definitions ref");
        assert!(
            definitions.contains_key(model),
            "{name}: dangling ref {target}"
        );
        assert!(
            schema.get("type").is_none() && schema.get("properties").is_none(),
            "{name}: ref with siblings"
        );
        return;
    }
    if schema.get("type").and_then(Value::as_str) == Some("array") {
        assert!(
            schema.get("items").is_some_and(Value::is_object),
            "{name}: array without one item schema"
        );
    }
    if let Some(t) = schema.get("type") {
        assert!(t.is_string(), "{name}: type must be one string");
    }
    for child in schema
        .get("properties")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|p| p.values())
        .chain(schema.get("items"))
        .chain(schema.get("additionalProperties").filter(|a| a.is_object()))
    {
        assert_kube_openapi_parses(name, child, definitions);
    }
}

#[tokio::test]
async fn openapi_v2_serves_built_in_and_crd_definitions_with_gvk_over_protobuf_and_json() {
    let server = boot().await;

    let resp = get(&server, CLIENT_GO_ACCEPT).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers()[reqwest::header::CONTENT_TYPE],
        "application/com.github.proto-openapi.spec.v2.v1.0+protobuf"
    );
    let doc =
        pb::Document::decode(resp.bytes().await.unwrap()).expect("a gnostic openapi_v2.Document");
    assert_eq!(doc.swagger, "2.0");
    let definitions = doc.definitions.expect("definitions").additional_properties;
    let find = |name: &str| {
        definitions
            .iter()
            .find(|d| d.name == name)
            .and_then(|d| d.value.clone())
            .unwrap_or_else(|| panic!("no definition {name}"))
    };
    let deployment = find("io.k8s.api.apps.v1.Deployment");
    assert_eq!(
        gvk_of(&deployment),
        Some(json!([{"group": "apps", "kind": "Deployment", "version": "v1"}]))
    );
    let spec = deployment
        .properties
        .as_ref()
        .and_then(|p| p.additional_properties.iter().find(|p| p.name == "spec"))
        .and_then(|p| p.value.as_ref())
        .expect("Deployment.spec");
    assert_eq!(spec.xref, "#/definitions/io.k8s.api.apps.v1.DeploymentSpec");
    let widget = find("com.example.v1.Widget");
    assert_eq!(
        gvk_of(&widget),
        Some(json!([{"group": "example.com", "kind": "Widget", "version": "v1"}]))
    );
    let widget_fields: Vec<&str> = widget
        .properties
        .as_ref()
        .expect("Widget properties")
        .additional_properties
        .iter()
        .map(|p| p.name.as_str())
        .collect();
    for field in ["apiVersion", "kind", "metadata", "spec"] {
        assert!(widget_fields.contains(&field), "Widget declares {field}");
    }

    let resp = get(&server, "application/json").await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers()[reqwest::header::CONTENT_TYPE],
        "application/json"
    );
    let doc: Value = resp.json().await.unwrap();
    assert_eq!(doc["swagger"], "2.0");
    let definitions = doc["definitions"].as_object().expect("definitions");
    assert_eq!(
        definitions["io.k8s.api.apps.v1.Deployment"]["x-kubernetes-group-version-kind"],
        json!([{"group": "apps", "kind": "Deployment", "version": "v1"}])
    );
    let widget = &definitions["com.example.v1.Widget"];
    assert_eq!(
        widget["x-kubernetes-group-version-kind"],
        json!([{"group": "example.com", "kind": "Widget", "version": "v1"}])
    );
    assert!(
        widget["properties"]["spec"]["properties"]["labels"]
            .get("type")
            .is_none()
    );
    assert!(
        widget["properties"]["spec"]["properties"]["values"]
            .get("type")
            .is_none()
    );
    for (name, schema) in definitions {
        assert_kube_openapi_parses(name, schema, definitions);
    }

    let resp = get(
        &server,
        "application/com.github.proto-openapi.spec.v2.v1.0+protobuf, application/json",
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers()[reqwest::header::CONTENT_TYPE],
        "application/com.github.proto-openapi.spec.v2.v1.0+protobuf"
    );

    let resp = get(&server, "application/vnd.kubernetes.protobuf").await;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_ACCEPTABLE);

    server.shutdown().await.unwrap();
}
