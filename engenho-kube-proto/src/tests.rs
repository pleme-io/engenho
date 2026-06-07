//! Codec unit tests — golden-vector round-trip + per-kind matrix.

use super::*;
use serde_json::json;

/// The EXACT 67 bytes captured live from kubectl v1.34.3
/// `create configmap demo --from-literal=hello=engenho`.
const GOLDEN_CONFIGMAP: &[u8] = &[
    0x6b, 0x38, 0x73, 0x00, // magic "k8s\0"
    0x0a, 0x0f, 0x0a, 0x02, 0x76, 0x31, 0x12, 0x09, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x4d, 0x61,
    0x70, // Unknown.typeMeta = {apiVersion="v1", kind="ConfigMap"}
    0x12, 0x28, // Unknown.raw, len 40
    0x0a, 0x14, 0x0a, 0x04, 0x64, 0x65, 0x6d, 0x6f, 0x12, 0x00, 0x1a, 0x00, 0x22, 0x00, 0x2a, 0x00,
    0x32, 0x00, 0x38, 0x00, 0x42, 0x00, // ObjectMeta {name="demo", ...empty...}
    0x12, 0x10, 0x0a, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x12, 0x07, 0x65, 0x6e, 0x67, 0x65, 0x6e,
    0x68, 0x6f, 0x1a, 0x00, 0x22, 0x00, // data {hello: engenho}
];

#[test]
fn golden_configmap_decodes_to_expected_json() {
    let v = decode_protobuf(GOLDEN_CONFIGMAP).expect("decode golden ConfigMap");
    assert_eq!(v["apiVersion"], "v1", "apiVersion injected from wrapper");
    assert_eq!(v["kind"], "ConfigMap", "kind injected from wrapper");
    assert_eq!(
        v["metadata"]["name"], "demo",
        "metadata.name read from ObjectMeta field 1"
    );
    assert_eq!(
        v["data"]["hello"], "engenho",
        "data.hello read from the ConfigMap data map"
    );
}

#[test]
fn decode_request_dispatches_on_content_type() {
    // protobuf content-type → the codec path.
    let v = decode_request(CONTENT_TYPE_PROTOBUF, GOLDEN_CONFIGMAP)
        .expect("protobuf content-type decodes");
    assert_eq!(v["data"]["hello"], "engenho");

    // json content-type → straight serde_json parse.
    let body = br#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"j"}}"#;
    let v = decode_request(CONTENT_TYPE_JSON, body).expect("json content-type parses");
    assert_eq!(v["metadata"]["name"], "j");
}

#[test]
fn bad_magic_is_a_typed_error_not_a_panic() {
    let mut bad = GOLDEN_CONFIGMAP.to_vec();
    bad[0] = 0xff; // corrupt the magic
    let err = decode_protobuf(&bad).unwrap_err();
    assert!(matches!(err, CodecError::BadMagic), "got {err:?}");

    // Too short to even hold the magic.
    let err = decode_protobuf(&[0x6b, 0x38]).unwrap_err();
    assert!(matches!(err, CodecError::BadMagic), "got {err:?}");
}

#[test]
fn uncataloged_kind_is_a_typed_error() {
    let gvk = Gvk::new("v1", "NoSuchKind");
    let err = message_for_gvk(&gvk).unwrap_err();
    assert!(matches!(err, CodecError::UncatalogedKind { .. }), "got {err:?}");

    let gvk = Gvk::new("made.up/v9", "Pod");
    let err = message_for_gvk(&gvk).unwrap_err();
    assert!(matches!(err, CodecError::UncatalogedKind { .. }), "got {err:?}");
}

#[test]
fn content_type_negotiation_helpers() {
    assert!(is_protobuf_content_type(CONTENT_TYPE_PROTOBUF));
    assert!(is_protobuf_content_type(
        "application/vnd.kubernetes.protobuf; charset=utf-8"
    ));
    assert!(!is_protobuf_content_type(CONTENT_TYPE_JSON));
    assert!(!is_protobuf_content_type("application/yaml"));

    // kubectl typed clientset Accept header → protobuf wins.
    assert!(response_wants_protobuf(
        "application/vnd.kubernetes.protobuf,application/json"
    ));
    // dynamic/unstructured client Accept header → JSON.
    assert!(!response_wants_protobuf("application/json"));
}

#[test]
fn golden_configmap_round_trips_through_encode() {
    // decode → encode → decode must preserve the object.
    let v = decode_protobuf(GOLDEN_CONFIGMAP).expect("decode");
    let gvk = Gvk::new("v1", "ConfigMap");
    let bytes = encode_response(&gvk, &v).expect("encode");
    // The re-encoded bytes carry the magic + Unknown wrapper.
    assert_eq!(&bytes[..4], &PROTOBUF_MAGIC);
    let back = decode_protobuf(&bytes).expect("re-decode");
    assert_eq!(back["metadata"]["name"], "demo");
    assert_eq!(back["data"]["hello"], "engenho");
    assert_eq!(back["apiVersion"], "v1");
    assert_eq!(back["kind"], "ConfigMap");
}

/// One row per cataloged kind. `min` is a minimal valid object body for
/// that kind (just enough to round-trip through the proto). Per the
/// CATALOG REFLECTION rule: the build fails when a cataloged kind has no
/// descriptor.
struct MatrixRow {
    api_version: &'static str,
    kind: &'static str,
    body: fn() -> serde_json::Value,
}

fn meta(name: &str) -> serde_json::Value {
    json!({ "metadata": { "name": name } })
}

const MATRIX: &[MatrixRow] = &[
    MatrixRow { api_version: "v1", kind: "Pod", body: || meta("p") },
    MatrixRow { api_version: "v1", kind: "Service", body: || meta("s") },
    MatrixRow { api_version: "v1", kind: "ConfigMap", body: || json!({"metadata":{"name":"cm"},"data":{"k":"v"}}) },
    MatrixRow { api_version: "v1", kind: "Secret", body: || json!({"metadata":{"name":"sec"},"type":"Opaque"}) },
    MatrixRow { api_version: "v1", kind: "Namespace", body: || meta("ns") },
    MatrixRow { api_version: "v1", kind: "ServiceAccount", body: || meta("sa") },
    MatrixRow { api_version: "v1", kind: "Node", body: || meta("node") },
    MatrixRow { api_version: "v1", kind: "PersistentVolume", body: || meta("pv") },
    MatrixRow { api_version: "v1", kind: "PersistentVolumeClaim", body: || meta("pvc") },
    MatrixRow { api_version: "v1", kind: "Endpoints", body: || meta("ep") },
    MatrixRow { api_version: "apps/v1", kind: "Deployment", body: || meta("dep") },
    MatrixRow { api_version: "apps/v1", kind: "ReplicaSet", body: || meta("rs") },
    MatrixRow { api_version: "apps/v1", kind: "StatefulSet", body: || meta("ss") },
    MatrixRow { api_version: "apps/v1", kind: "DaemonSet", body: || meta("ds") },
    MatrixRow { api_version: "rbac.authorization.k8s.io/v1", kind: "Role", body: || meta("role") },
    MatrixRow { api_version: "rbac.authorization.k8s.io/v1", kind: "ClusterRole", body: || meta("crole") },
    MatrixRow { api_version: "rbac.authorization.k8s.io/v1", kind: "RoleBinding", body: || json!({"metadata":{"name":"rb"},"roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"Role","name":"role"}}) },
    MatrixRow { api_version: "rbac.authorization.k8s.io/v1", kind: "ClusterRoleBinding", body: || json!({"metadata":{"name":"crb"},"roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"ClusterRole","name":"crole"}}) },
];

#[test]
fn every_cataloged_kind_has_a_descriptor() {
    // CATALOG REFLECTION: a cataloged kind with no proto descriptor fails
    // the build. Aggregate all failures before asserting.
    let mut failures = vec![];
    for row in MATRIX {
        let gvk = Gvk::new(row.api_version, row.kind);
        if let Err(e) = message_for_gvk(&gvk) {
            failures.push(format!("{}/{}: {e}", row.api_version, row.kind));
        }
    }
    assert!(
        failures.is_empty(),
        "{} cataloged kinds have no descriptor:\n  - {}",
        failures.len(),
        failures.join("\n  - ")
    );
}

#[test]
fn matrix_covers_all_eighteen_cataloged_kinds() {
    assert_eq!(MATRIX.len(), 18, "the codec matrix must cover all 18 cataloged kinds");
}

#[test]
fn every_cataloged_kind_value_encode_decode_identity() {
    // Value -> encode -> decode -> Value identity on the load-bearing
    // fields (metadata.name + kind + apiVersion). Aggregate failures.
    let mut failures = vec![];
    for row in MATRIX {
        let gvk = Gvk::new(row.api_version, row.kind);
        let body = (row.body)();
        let want_name = body["metadata"]["name"].clone();
        match encode_response(&gvk, &body).and_then(|b| decode_protobuf(&b)) {
            Ok(back) => {
                if back["metadata"]["name"] != want_name {
                    failures.push(format!(
                        "{}/{}: name drift want={want_name} got={}",
                        row.api_version, row.kind, back["metadata"]["name"]
                    ));
                }
                if back["kind"] != row.kind {
                    failures.push(format!(
                        "{}/{}: kind drift got={}",
                        row.api_version, row.kind, back["kind"]
                    ));
                }
                if back["apiVersion"] != row.api_version {
                    failures.push(format!(
                        "{}/{}: apiVersion drift got={}",
                        row.api_version, row.kind, back["apiVersion"]
                    ));
                }
            }
            Err(e) => failures.push(format!("{}/{}: {e}", row.api_version, row.kind)),
        }
    }
    assert!(
        failures.is_empty(),
        "{} kinds failed encode/decode identity:\n  - {}",
        failures.len(),
        failures.join("\n  - ")
    );
}

#[test]
fn json_inner_content_type_is_parsed_directly() {
    // Build an Unknown wrapper with contentType=application/json and the
    // raw being literal JSON — the codec must parse it directly without
    // touching the per-kind descriptor.
    let inner = br#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"j"},"data":{"a":"b"}}"#;
    let unknown_desc = unknown_descriptor().unwrap();
    let mut unknown = DynamicMessage::new(unknown_desc.clone());
    let tm_field = unknown_desc.get_field_by_name("typeMeta").unwrap();
    let tm_desc = tm_field.kind().as_message().cloned().unwrap();
    let mut tm = DynamicMessage::new(tm_desc);
    tm.set_field_by_name("apiVersion", prost_reflect::Value::String("v1".into()));
    tm.set_field_by_name("kind", prost_reflect::Value::String("ConfigMap".into()));
    unknown.set_field_by_name("typeMeta", prost_reflect::Value::Message(tm));
    unknown.set_field_by_name("raw", prost_reflect::Value::Bytes(Bytes::from(inner.to_vec())));
    unknown.set_field_by_name(
        "contentType",
        prost_reflect::Value::String("application/json".into()),
    );
    let mut buf = PROTOBUF_MAGIC.to_vec();
    unknown.encode(&mut buf).unwrap();

    let v = decode_protobuf(&buf).expect("json-inner decodes");
    assert_eq!(v["metadata"]["name"], "j");
    assert_eq!(v["data"]["a"], "b");
}
