//! W3 (plan T4.7): the protobuf codec is driven by the descriptors, and its
//! JSON is the JSON upstream serves.
//!
//! ## The defect this pins
//!
//! The codec bridged protobuf to JSON through prost-reflect's generic proto3
//! mapping, which knows nothing of the hand-written Go marshallers behind the
//! Kubernetes JSON form. So a protobuf write stored
//! `renewTime: {"seconds": "1726000015", "nanos": …}`, `cpu: {"string": …}`,
//! `targetPort: {"type": "1", …}` and `generation: "3"`; an `env[].valueFrom
//! .configMapKeyRef` came back as `{"localObjectReference": {"name": …}}`;
//! and a protobuf READ of any Deployment that declares resources failed,
//! because `"500m"` is not a `Quantity` message. A protobuf read of a kind
//! with no vendored message (batch, networking, discovery, storage, …) was a
//! 400 even though every client-go client also accepts JSON.
//!
//! ## The oracle
//!
//! The wire bytes below are built by a protobuf writer in THIS file, from the
//! upstream Go marshallers at v1.34.0 (`time_proto.go` writes a `Time` as
//! `{seconds, nanos: 0}`, `micro_time_proto.go` truncates to the microsecond,
//! `intstr` writes all three fields, `json:",inline"` embeds are nested
//! messages on the wire), never from the transcoder or prost-reflect. Each
//! golden is checked in both directions: decode(golden) is the upstream JSON,
//! and encode(that JSON) is the golden.

use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::proto_transcode::{
    self, FieldProblem, INLINE, KnownLossy, Special, TranscodeError,
};
use engenho_apiserver::{ApiServer, handlers_from_catalog};
use engenho_kube_proto::{CONTENT_TYPE_PROTOBUF, Gvk, message_for_gvk};
use engenho_store::{InProcessRouter, StoreMesh, default_config};
use engenho_types::generated_v1_34::RESOURCE_CATALOG;
use serde_json::{Map, Value, json};

// ── an independent protobuf writer ─────────────────────────────────────────

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
mod wire {
    fn varint(mut v: u64, out: &mut Vec<u8>) {
        while v >= 0x80 {
            out.push((v & 0x7f) as u8 | 0x80);
            v >>= 7;
        }
        out.push(v as u8);
    }

    fn key(field: u32, wire_type: u8, out: &mut Vec<u8>) {
        varint((u64::from(field) << 3) | u64::from(wire_type), out);
    }

    /// A varint field (int32, int64, bool): two's complement, as Go writes.
    pub fn int(field: u32, v: i64) -> Vec<u8> {
        let mut out = Vec::new();
        key(field, 0, &mut out);
        varint(v as u64, &mut out);
        out
    }

    pub fn bytes(field: u32, b: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        key(field, 2, &mut out);
        varint(b.len() as u64, &mut out);
        out.extend_from_slice(b);
        out
    }

    pub fn string(field: u32, s: &str) -> Vec<u8> {
        bytes(field, s.as_bytes())
    }

    pub fn msg(field: u32, parts: &[Vec<u8>]) -> Vec<u8> {
        bytes(field, &parts.concat())
    }

    /// One `map<string, V>` entry: key 1, value 2.
    pub fn entry(field: u32, k: &str, value_field_2: Vec<u8>) -> Vec<u8> {
        msg(field, &[string(1, k), value_field_2])
    }

    /// `metav1.Time`: `Time.ProtoTime` sets only Seconds; gogo writes the
    /// zero Nanos too.
    pub fn time(field: u32, seconds: i64) -> Vec<u8> {
        msg(field, &[int(1, seconds), int(2, 0)])
    }

    /// `metav1.MicroTime`: seconds, and nanos truncated to the microsecond.
    pub fn micro_time(field: u32, seconds: i64, nanos: i64) -> Vec<u8> {
        msg(field, &[int(1, seconds), int(2, nanos)])
    }

    pub fn quantity(field: u32, s: &str) -> Vec<u8> {
        msg(field, &[string(1, s)])
    }

    /// `IntOrString` writes type, intVal and strVal whichever arm is live.
    pub fn int_arm(field: u32, v: i64) -> Vec<u8> {
        msg(field, &[int(1, 0), int(2, v), string(3, "")])
    }

    pub fn string_arm(field: u32, s: &str) -> Vec<u8> {
        msg(field, &[int(1, 1), int(2, 0), string(3, s)])
    }

    /// `RawExtension` / `FieldsV1`: field 1 is the raw JSON.
    pub fn raw(field: u32, json: &str) -> Vec<u8> {
        msg(field, &[bytes(1, json.as_bytes())])
    }

    fn unknown(api_version: &str, kind: &str, object: &[Vec<u8>]) -> Vec<u8> {
        let mut out = b"k8s\0".to_vec();
        out.extend(msg(1, &[string(1, api_version), string(2, kind)]));
        out.extend(bytes(2, &object.concat()));
        out
    }

    /// `k8s\0` + `runtime.Unknown` as upstream's serializer writes it,
    /// empty contentEncoding and contentType included.
    pub fn go_frame(api_version: &str, kind: &str, object: &[Vec<u8>]) -> Vec<u8> {
        let mut out = unknown(api_version, kind, object);
        out.extend(string(3, ""));
        out.extend(string(4, ""));
        out
    }

    /// The same frame without the two empty strings: what engenho writes.
    /// A decoder reads both the same.
    pub fn engenho_frame(api_version: &str, kind: &str, object: &[Vec<u8>]) -> Vec<u8> {
        unknown(api_version, kind, object)
    }
}

use wire::{entry, int, int_arm, micro_time, msg, quantity, raw, string, string_arm, time};

/// 2024-09-10T20:26:40Z
const S_ACQUIRE: i64 = 1_726_000_000;
/// 2024-09-10T20:26:55Z
const S_RENEW: i64 = 1_726_000_015;
/// 2025-09-19T10:00:00Z
const S_CREATED: i64 = 1_758_276_000;

/// One upstream-shaped object: its GVK, its wire object, its JSON.
struct Golden {
    api_version: &'static str,
    kind: &'static str,
    object: Vec<Vec<u8>>,
    json: Value,
}

impl Golden {
    fn gvk(&self) -> Gvk {
        Gvk::new(self.api_version, self.kind)
    }
}

/// `coordination.k8s.io/v1` Lease: MicroTime, int32.
fn lease(renew_nanos: i64) -> Golden {
    Golden {
        api_version: "coordination.k8s.io/v1",
        kind: "Lease",
        object: vec![
            msg(1, &[string(1, "leader"), string(3, "kube-system")]),
            msg(
                2,
                &[
                    string(1, "node-a"),
                    int(2, 15),
                    micro_time(3, S_ACQUIRE, 0),
                    micro_time(4, S_RENEW, renew_nanos),
                    int(5, 2),
                ],
            ),
        ],
        json: json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": { "name": "leader", "namespace": "kube-system" },
            "spec": {
                "holderIdentity": "node-a",
                "leaseDurationSeconds": 15,
                "acquireTime": "2024-09-10T20:26:40.000000Z",
                "renewTime": "2024-09-10T20:26:55.123456Z",
                "leaseTransitions": 2
            }
        }),
    }
}

/// `apps/v1` Deployment: Time, int64, a set-to-zero pointer, Quantity maps,
/// both IntOrString arms, and three inline embeds (Volume.volumeSource,
/// ConfigMapKeySelector.localObjectReference, Probe.handler).
fn deployment() -> Golden {
    let container = msg(
        2,
        &[
            string(1, "app"),
            string(2, "nginx"),
            msg(
                7,
                &[
                    string(1, "MODE"),
                    msg(
                        3,
                        &[msg(
                            3,
                            &[msg(1, &[string(1, "app-config")]), string(2, "mode")],
                        )],
                    ),
                ],
            ),
            msg(
                8,
                &[
                    entry(1, "cpu", quantity(2, "500m")),
                    entry(2, "memory", quantity(2, "64Mi")),
                ],
            ),
            msg(
                10,
                &[
                    msg(
                        1,
                        &[msg(2, &[string(1, "/healthz"), string_arm(2, "http")])],
                    ),
                    int(4, 5),
                ],
            ),
        ],
    );
    let volume = msg(
        1,
        &[
            string(1, "scratch"),
            msg(2, &[msg(2, &[quantity(2, "1Gi")])]),
        ],
    );
    Golden {
        api_version: "apps/v1",
        kind: "Deployment",
        object: vec![
            msg(1, &[string(1, "web"), int(7, 3), time(8, S_CREATED)]),
            msg(
                2,
                &[
                    int(1, 0),
                    msg(3, &[msg(2, &[volume, container])]),
                    msg(
                        4,
                        &[
                            string(1, "RollingUpdate"),
                            msg(2, &[int_arm(1, 1), string_arm(2, "25%")]),
                        ],
                    ),
                ],
            ),
        ],
        json: json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "web",
                "generation": 3,
                "creationTimestamp": "2025-09-19T10:00:00Z"
            },
            "spec": {
                "replicas": 0,
                "template": { "spec": {
                    "volumes": [ { "name": "scratch", "emptyDir": { "sizeLimit": "1Gi" } } ],
                    "containers": [ {
                        "name": "app",
                        "image": "nginx",
                        "env": [ {
                            "name": "MODE",
                            "valueFrom": { "configMapKeyRef": { "name": "app-config", "key": "mode" } }
                        } ],
                        "resources": {
                            "limits": { "cpu": "500m" },
                            "requests": { "memory": "64Mi" }
                        },
                        "livenessProbe": {
                            "httpGet": { "path": "/healthz", "port": "http" },
                            "periodSeconds": 5
                        }
                    } ]
                } },
                "strategy": {
                    "type": "RollingUpdate",
                    "rollingUpdate": { "maxUnavailable": 1, "maxSurge": "25%" }
                }
            }
        }),
    }
}

/// `v1` Service: IntOrString in both arms inside a repeated message.
fn service() -> Golden {
    Golden {
        api_version: "v1",
        kind: "Service",
        object: vec![
            msg(1, &[string(1, "api")]),
            msg(
                2,
                &[
                    msg(1, &[string(1, "http"), int(3, 80), string_arm(4, "http")]),
                    msg(1, &[string(1, "grpc"), int(3, 9090), int_arm(4, 9091)]),
                ],
            ),
        ],
        json: json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": { "name": "api" },
            "spec": { "ports": [
                { "name": "http", "port": 80, "targetPort": "http" },
                { "name": "grpc", "port": 9090, "targetPort": 9091 }
            ] }
        }),
    }
}

/// `apps/v1` ControllerRevision: RawExtension, FieldsV1, int64, a Time
/// inside a repeated message.
fn controller_revision() -> Golden {
    Golden {
        api_version: "apps/v1",
        kind: "ControllerRevision",
        object: vec![
            msg(
                1,
                &[
                    string(1, "web-5d8f"),
                    msg(
                        17,
                        &[
                            string(1, "kubectl"),
                            string(2, "Apply"),
                            string(3, "apps/v1"),
                            time(4, S_CREATED),
                            string(6, "FieldsV1"),
                            raw(7, r#"{"f:data":{}}"#),
                        ],
                    ),
                ],
            ),
            raw(2, r#"{"spec":{"template":{"$patch":"replace"}}}"#),
            int(3, 7),
        ],
        json: json!({
            "apiVersion": "apps/v1",
            "kind": "ControllerRevision",
            "metadata": {
                "name": "web-5d8f",
                "managedFields": [ {
                    "manager": "kubectl",
                    "operation": "Apply",
                    "apiVersion": "apps/v1",
                    "time": "2025-09-19T10:00:00Z",
                    "fieldsType": "FieldsV1",
                    "fieldsV1": { "f:data": {} }
                } ]
            },
            "data": { "spec": { "template": { "$patch": "replace" } } },
            "revision": 7
        }),
    }
}

/// `authentication.k8s.io/v1` TokenReview: a bool, and `ExtraValue`, a Go
/// `[]string` go-to-protobuf masks as `{items}`.
fn token_review() -> Golden {
    Golden {
        api_version: "authentication.k8s.io/v1",
        kind: "TokenReview",
        object: vec![msg(
            3,
            &[
                int(1, 1),
                msg(
                    2,
                    &[
                        string(1, "alice"),
                        entry(
                            4,
                            "scopes",
                            msg(2, &[string(1, "read"), string(1, "write")]),
                        ),
                    ],
                ),
            ],
        )],
        json: json!({
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenReview",
            "status": {
                "authenticated": true,
                "user": { "username": "alice", "extra": { "scopes": ["read", "write"] } }
            }
        }),
    }
}

/// Every golden, in both directions: one per special type, and the inline
/// embeds a Deployment carries.
fn goldens() -> Vec<Golden> {
    vec![
        lease(123_456_000),
        deployment(),
        service(),
        controller_revision(),
        token_review(),
    ]
}

/// The exact 67 bytes kubectl v1.34.3 sent for
/// `kubectl create configmap demo --from-literal=hello=engenho`.
const KUBECTL_CONFIGMAP: &[u8] = &[
    0x6b, 0x38, 0x73, 0x00, 0x0a, 0x0f, 0x0a, 0x02, 0x76, 0x31, 0x12, 0x09, 0x43, 0x6f, 0x6e, 0x66,
    0x69, 0x67, 0x4d, 0x61, 0x70, 0x12, 0x28, 0x0a, 0x14, 0x0a, 0x04, 0x64, 0x65, 0x6d, 0x6f, 0x12,
    0x00, 0x1a, 0x00, 0x22, 0x00, 0x2a, 0x00, 0x32, 0x00, 0x38, 0x00, 0x42, 0x00, 0x12, 0x10, 0x0a,
    0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x12, 0x07, 0x65, 0x6e, 0x67, 0x65, 0x6e, 0x68, 0x6f, 0x1a,
    0x00, 0x22, 0x00,
];

// ── the upstream oracle, both directions ────────────────────────────────────

/// Decode: bytes laid out as upstream's Go marshallers lay them out become
/// the JSON upstream serves. Against the old codec every row failed: times
/// came out as `{"seconds": "…"}` objects, int64s as strings, quantities as
/// `{"string": …}`, inline embeds nested.
#[test]
fn upstream_shaped_bytes_decode_to_upstream_json() {
    let mut failures = Vec::new();
    for golden in goldens() {
        let frame = wire::go_frame(golden.api_version, golden.kind, &golden.object);
        match proto_transcode::decode(&frame) {
            Ok(decoded) => {
                if decoded.value != golden.json {
                    failures.push(format!(
                        "{}: decoded\n  {}\nwant\n  {}",
                        golden.kind, decoded.value, golden.json
                    ));
                }
                if !decoded.losses.is_empty() {
                    failures.push(format!("{}: losses {:?}", golden.kind, decoded.losses));
                }
            }
            Err(e) => failures.push(format!("{}: {e}", golden.kind)),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Encode: upstream JSON becomes the bytes upstream's marshallers write,
/// field for field. The old codec could not encode the Deployment at all
/// (`"500m"` is not a `Quantity` message).
#[test]
fn upstream_json_encodes_to_upstream_shaped_bytes() {
    let mut failures = Vec::new();
    for golden in goldens() {
        let want = wire::engenho_frame(golden.api_version, golden.kind, &golden.object);
        match proto_transcode::encode(&golden.gvk(), &golden.json) {
            Ok(encoded) => {
                if encoded.bytes.as_ref() != want.as_slice() {
                    failures.push(format!(
                        "{}: encoded {:02x?}\n want {:02x?}",
                        golden.kind,
                        encoded.bytes.as_ref(),
                        want
                    ));
                }
                if !encoded.losses.is_empty() {
                    failures.push(format!("{}: losses {:?}", golden.kind, encoded.losses));
                }
            }
            Err(e) => failures.push(format!("{}: {e}", golden.kind)),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// MicroTime keeps microseconds and drops the rest, as
/// `MicroTime.Unmarshal` does.
#[test]
fn micro_time_truncates_to_the_microsecond() {
    let golden = lease(123_456_789);
    let decoded = proto_transcode::decode(&wire::go_frame(
        golden.api_version,
        golden.kind,
        &golden.object,
    ))
    .expect("decodes");
    assert_eq!(
        decoded.value.pointer("/spec/renewTime"),
        Some(&json!("2024-09-10T20:26:55.123456Z"))
    );
}

/// kubectl's real bytes: every non-pointer ObjectMeta field Go wrote comes
/// back as its zero value, `generation` as a NUMBER, and the empty `Time`
/// message as `null` (the old codec gave `"0"` and `{}`).
#[test]
fn kubectls_configmap_decodes_with_numbers_and_a_null_time() {
    let decoded = proto_transcode::decode(KUBECTL_CONFIGMAP).expect("kubectl's bytes decode");
    assert_eq!(
        decoded.value,
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "demo",
                "generateName": "",
                "namespace": "",
                "selfLink": "",
                "uid": "",
                "resourceVersion": "",
                "generation": 0,
                "creationTimestamp": null
            },
            "data": { "hello": "engenho" }
        })
    );
    assert!(decoded.losses.is_empty());
}

/// A `Time`'s nanos are not read (`Time.Unmarshal` keeps whole seconds), and
/// Go's zero time is `null` both ways.
#[test]
fn time_is_whole_seconds_and_the_zero_time_is_null() {
    let with_nanos = wire::go_frame(
        "v1",
        "ConfigMap",
        &[msg(
            1,
            &[string(1, "t"), msg(8, &[int(1, S_CREATED), int(2, 999)])],
        )],
    );
    let decoded = proto_transcode::decode(&with_nanos).expect("decodes");
    assert_eq!(
        decoded.value.pointer("/metadata/creationTimestamp"),
        Some(&json!("2025-09-19T10:00:00Z"))
    );

    let zero = json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": { "name": "t", "creationTimestamp": "0001-01-01T00:00:00Z" }
    });
    let encoded = proto_transcode::encode(&Gvk::new("v1", "ConfigMap"), &zero).expect("encodes");
    assert_eq!(
        encoded.bytes.as_ref(),
        wire::engenho_frame("v1", "ConfigMap", &[msg(1, &[string(1, "t"), msg(8, &[])])])
            .as_slice(),
        "Go writes the zero time as an empty message"
    );
    let back = proto_transcode::decode(&encoded.bytes).expect("decodes");
    assert_eq!(
        back.value.pointer("/metadata/creationTimestamp"),
        Some(&Value::Null)
    );
}

// ── what the old codec stored ───────────────────────────────────────────────

/// Objects written through the old codec carry proto3 shapes. They must stay
/// readable over protobuf: a Lease a leader-election loop wrote before this
/// change is read by the same loop after it.
#[test]
fn stored_proto3_shapes_encode_as_the_values_they_denote() {
    let canonical = deployment().json;
    let mut legacy = canonical.clone();
    legacy["metadata"]["generation"] = json!("3");
    legacy["metadata"]["creationTimestamp"] = json!({ "seconds": "1758276000", "nanos": 0 });
    legacy["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"]["cpu"] =
        json!({ "string": "500m" });
    legacy["spec"]["strategy"]["rollingUpdate"]["maxSurge"] =
        json!({ "type": "1", "intVal": 0, "strVal": "25%" });
    legacy["spec"]["strategy"]["rollingUpdate"]["maxUnavailable"] =
        json!({ "type": "0", "intVal": 1, "strVal": "" });
    let gvk = Gvk::new("apps/v1", "Deployment");
    let want = proto_transcode::encode(&gvk, &canonical).expect("canonical encodes");
    let got = proto_transcode::encode(&gvk, &legacy).expect("legacy shapes encode");
    assert_eq!(got.bytes, want.bytes);
    assert!(got.losses.is_empty());

    let mut lease_legacy = lease(123_456_000).json;
    lease_legacy["spec"]["renewTime"] = json!({ "seconds": "1726000015", "nanos": 123_456_000 });
    let lease_gvk = Gvk::new("coordination.k8s.io/v1", "Lease");
    assert_eq!(
        proto_transcode::encode(&lease_gvk, &lease_legacy)
            .expect("legacy lease encodes")
            .bytes,
        proto_transcode::encode(&lease_gvk, &lease(123_456_000).json)
            .expect("canonical lease encodes")
            .bytes,
    );
}

// ── refusals ────────────────────────────────────────────────────────────────

#[test]
fn an_impossible_int_or_string_type_is_refused() {
    let frame = wire::go_frame(
        "v1",
        "Service",
        &[msg(
            2,
            &[msg(
                1,
                &[int(3, 80), msg(4, &[int(1, 2), int(2, 0), string(3, "")])],
            )],
        )],
    );
    let err = proto_transcode::decode(&frame).expect_err("type 2 has no JSON form");
    assert!(
        matches!(
            err,
            TranscodeError::Field {
                problem: FieldProblem::IntOrStringType(2),
                ..
            }
        ),
        "{err}"
    );
    assert_eq!(
        err.to_string(),
        "spec.ports[0].targetPort: impossible IntOrString type 2"
    );
}

#[test]
fn an_embedded_document_that_is_not_json_is_refused() {
    let frame = wire::go_frame("apps/v1", "ControllerRevision", &[raw(2, "k8s\0not json")]);
    let err = proto_transcode::decode(&frame).expect_err("RawExtension must be JSON");
    assert!(
        matches!(
            err,
            TranscodeError::Field {
                problem: FieldProblem::NotJson(_),
                ..
            }
        ),
        "{err}"
    );
}

#[test]
fn a_kind_with_no_message_is_uncataloged_both_ways() {
    let frame = wire::go_frame(
        "networking.k8s.io/v1",
        "IngressClass",
        &[msg(1, &[string(1, "x")])],
    );
    assert!(matches!(
        proto_transcode::decode(&frame),
        Err(TranscodeError::Uncataloged { .. })
    ));
    assert!(matches!(
        proto_transcode::encode(
            &Gvk::new("networking.k8s.io/v1", "IngressClass"),
            &json!({ "metadata": { "name": "x" } })
        ),
        Err(TranscodeError::Uncataloged { .. })
    ));
}

#[test]
fn integers_are_integers() {
    let gvk = Gvk::new("apps/v1", "Deployment");
    let fraction = json!({ "metadata": { "name": "d" }, "spec": { "replicas": 1.5 } });
    let err = proto_transcode::encode(&gvk, &fraction).expect_err("1.5 is not an int32");
    assert!(
        matches!(
            err,
            TranscodeError::Field {
                problem: FieldProblem::NotAnInteger(_),
                ..
            }
        ),
        "{err}"
    );
    let wide = json!({ "metadata": { "name": "d" }, "spec": { "strategy": {
        "rollingUpdate": { "maxSurge": 4_294_967_296_i64 } } } });
    let err = proto_transcode::encode(&gvk, &wide).expect_err("an IntOrString int is an int32");
    assert!(
        matches!(
            err,
            TranscodeError::Field {
                problem: FieldProblem::OutOfRange(_),
                ..
            }
        ),
        "{err}"
    );
}

// ── the KnownLossy ratchet ──────────────────────────────────────────────────

/// How many classes of loss exist. Only ever lowered: a new class is a new
/// variant, which must raise this in the same diff and give a producer below.
const KNOWN_LOSSY_RATCHET: usize = 3;

/// An input that produces each class, so no variant is dead. Exhaustive: a
/// new variant does not compile here until it has one.
fn produce(loss: KnownLossy) -> proto_transcode::Losses {
    let configmap = Gvk::new("v1", "ConfigMap");
    match loss {
        KnownLossy::UnknownJsonField => {
            proto_transcode::encode(
                &configmap,
                &json!({ "metadata": { "name": "x" }, "data": {}, "notAField": 1 }),
            )
            .expect("encodes")
            .losses
        }
        KnownLossy::TimeSubsecond => proto_transcode::encode(
            &configmap,
            &json!({ "metadata": { "name": "x", "creationTimestamp": "2025-09-19T10:00:00.5Z" } }),
        )
        .expect("encodes")
        .losses,
        KnownLossy::UnknownWireField => {
            // kubectl's ConfigMap (the raw object is bytes 23..63 of its
            // frame) plus a field 99 no v1.34 message defines.
            let mut object = KUBECTL_CONFIGMAP[23..63].to_vec();
            object.extend(string(99, "from a newer client"));
            proto_transcode::decode(&wire::go_frame("v1", "ConfigMap", &[object]))
                .expect("decodes")
                .losses
        }
    }
}

#[test]
fn known_lossy_ratchet() {
    assert_eq!(
        KnownLossy::ALL.len(),
        KNOWN_LOSSY_RATCHET,
        "a new class of loss must raise KNOWN_LOSSY_RATCHET in the same diff; \
         a removed one must lower it"
    );
    for loss in KnownLossy::ALL {
        let losses = produce(loss);
        assert!(
            losses.contains(loss),
            "{loss:?} is catalogued but its producer did not produce it: {losses:?}"
        );
    }
}

/// A time with a fraction loses it on the wire, and the loss is reported.
#[test]
fn a_subsecond_time_is_a_reported_loss() {
    let losses = produce(KnownLossy::TimeSubsecond);
    assert_eq!(
        losses.iter().collect::<Vec<_>>(),
        vec![KnownLossy::TimeSubsecond]
    );
}

// ── every served kind, both directions ──────────────────────────────────────

fn container() -> Value {
    json!({
        "name": "app",
        "image": "nginx:1.27",
        "ports": [ { "name": "http", "containerPort": 8080, "protocol": "TCP" } ],
        "env": [
            { "name": "A", "value": "1" },
            { "name": "MODE", "valueFrom": { "configMapKeyRef": { "name": "cfg", "key": "mode" } } },
            { "name": "PW", "valueFrom": { "secretKeyRef": { "name": "sec", "key": "pw", "optional": true } } }
        ],
        "envFrom": [ { "configMapRef": { "name": "cfg" } }, { "secretRef": { "name": "sec" } } ],
        "resources": {
            "limits": { "cpu": "500m", "memory": "128Mi" },
            "requests": { "cpu": "100m" }
        },
        "livenessProbe": { "httpGet": { "path": "/healthz", "port": "http" }, "periodSeconds": 10 },
        "readinessProbe": { "tcpSocket": { "port": 8080 } },
        "startupProbe": { "exec": { "command": ["true"] }, "failureThreshold": 30 },
        "securityContext": { "runAsUser": 1000, "runAsNonRoot": true }
    })
}

fn pod_spec() -> Value {
    json!({
        "containers": [ container() ],
        "ephemeralContainers": [ { "name": "debug", "image": "busybox", "targetContainerName": "app" } ],
        "volumes": [
            { "name": "scratch", "emptyDir": { "sizeLimit": "1Gi" } },
            { "name": "cfg", "configMap": { "name": "cfg", "items": [ { "key": "k", "path": "p" } ] } },
            { "name": "proj", "projected": { "sources": [
                { "configMap": { "name": "cfg" } },
                { "secret": { "name": "sec" } },
                { "serviceAccountToken": { "path": "token", "expirationSeconds": 3600 } }
            ] } }
        ],
        "terminationGracePeriodSeconds": 30,
        "tolerations": [ { "key": "k", "operator": "Exists", "effect": "NoSchedule", "tolerationSeconds": 60 } ]
    })
}

fn template() -> Value {
    json!({ "metadata": { "labels": { "app": "demo" } }, "spec": pod_spec() })
}

fn selector() -> Value {
    json!({ "matchLabels": { "app": "demo" } })
}

fn condition(kind: &str) -> Value {
    json!({
        "type": kind, "status": "True",
        "lastTransitionTime": "2025-09-19T10:01:00Z",
        "reason": "Fine", "message": "all good"
    })
}

/// The fields beyond `metadata` a kind's row exercises. A kind with no row
/// is exercised through `metadata` alone.
fn payload(kind: &str) -> Option<Value> {
    Some(match kind {
        "Pod" => json!({
            "spec": pod_spec(),
            "status": {
                "phase": "Running",
                "startTime": "2025-09-19T10:00:00Z",
                "podIP": "10.0.0.7",
                "conditions": [ condition("Ready") ],
                "containerStatuses": [ {
                    "name": "app", "ready": true, "restartCount": 1,
                    "image": "nginx:1.27", "imageID": "sha256:abc",
                    "state": { "running": { "startedAt": "2025-09-19T10:00:05Z" } }
                } ]
            }
        }),
        "Deployment" => json!({
            "spec": {
                "replicas": 0,
                "selector": selector(),
                "template": template(),
                "strategy": { "type": "RollingUpdate",
                    "rollingUpdate": { "maxUnavailable": 1, "maxSurge": "25%" } }
            },
            "status": {
                "observedGeneration": 2, "replicas": 1,
                "conditions": [ {
                    "type": "Available", "status": "True",
                    "lastUpdateTime": "2025-09-19T10:01:00Z",
                    "lastTransitionTime": "2025-09-19T10:01:00Z"
                } ]
            }
        }),
        "ReplicaSet" | "ReplicationController" => json!({
            "spec": {
                "replicas": 2,
                "selector": if kind == "ReplicaSet" { selector() } else { json!({ "app": "demo" }) },
                "template": template()
            },
            "status": { "replicas": 2, "observedGeneration": 1 }
        }),
        "StatefulSet" => json!({
            "spec": {
                "replicas": 1,
                "serviceName": "db",
                "selector": selector(),
                "template": template(),
                "updateStrategy": { "type": "RollingUpdate",
                    "rollingUpdate": { "partition": 0, "maxUnavailable": "50%" } },
                "volumeClaimTemplates": [ {
                    "metadata": { "name": "data" },
                    "spec": { "accessModes": ["ReadWriteOnce"],
                        "resources": { "requests": { "storage": "1Gi" } } }
                } ]
            }
        }),
        "DaemonSet" => json!({
            "spec": {
                "selector": selector(),
                "template": template(),
                "updateStrategy": { "type": "RollingUpdate",
                    "rollingUpdate": { "maxUnavailable": "10%", "maxSurge": 0 } }
            }
        }),
        "ControllerRevision" => json!({
            "data": { "spec": { "template": { "$patch": "replace" } } },
            "revision": 3
        }),
        "PodTemplate" => json!({ "template": template() }),
        "Service" => json!({
            "spec": {
                "type": "ClusterIP",
                "clusterIP": "10.96.0.10",
                "selector": { "app": "demo" },
                "ports": [
                    { "name": "http", "port": 80, "targetPort": "http", "protocol": "TCP" },
                    { "name": "grpc", "port": 9090, "targetPort": 9091 }
                ]
            }
        }),
        "PersistentVolumeClaim" => json!({
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": { "requests": { "storage": "1Gi" } },
                "storageClassName": "local"
            },
            "status": { "phase": "Bound", "capacity": { "storage": "1Gi" } }
        }),
        "PersistentVolume" => json!({
            "spec": {
                "capacity": { "storage": "5Gi" },
                "hostPath": { "path": "/data" },
                "accessModes": ["ReadWriteOnce"],
                "persistentVolumeReclaimPolicy": "Retain"
            }
        }),
        "Node" => json!({
            "spec": { "taints": [ { "key": "k", "effect": "NoSchedule",
                "timeAdded": "2025-09-19T10:00:00Z" } ] },
            "status": {
                "capacity": { "cpu": "4", "memory": "8Gi", "pods": "110" },
                "allocatable": { "cpu": "3800m", "memory": "7Gi" },
                "conditions": [ {
                    "type": "Ready", "status": "True",
                    "lastHeartbeatTime": "2025-09-19T10:01:00Z",
                    "lastTransitionTime": "2025-09-19T10:00:00Z"
                } ]
            }
        }),
        "Lease" => json!({
            "spec": {
                "holderIdentity": "node-a",
                "leaseDurationSeconds": 15,
                "acquireTime": "2024-09-10T20:26:40.000000Z",
                "renewTime": "2024-09-10T20:26:55.123456Z",
                "leaseTransitions": 2
            }
        }),
        "Event" => json!({
            "involvedObject": { "kind": "Pod", "name": "p", "namespace": "default" },
            "reason": "Started", "message": "started", "type": "Normal",
            "source": { "component": "kubelet" },
            "firstTimestamp": "2025-09-19T10:00:00Z",
            "lastTimestamp": "2025-09-19T10:01:00Z",
            "count": 2,
            "eventTime": "2025-09-19T10:00:00.250000Z",
            "series": { "count": 2, "lastObservedTime": "2025-09-19T10:01:00.500000Z" },
            "action": "Start",
            "reportingComponent": "kubelet",
            "reportingInstance": "node-a"
        }),
        "ResourceQuota" => json!({
            "spec": { "hard": { "pods": "10", "requests.cpu": "4" } },
            "status": { "hard": { "pods": "10" }, "used": { "pods": "3" } }
        }),
        "LimitRange" => json!({
            "spec": { "limits": [ {
                "type": "Container",
                "max": { "cpu": "2" },
                "default": { "cpu": "500m" },
                "defaultRequest": { "cpu": "100m" }
            } ] }
        }),
        "Secret" => json!({
            "type": "Opaque",
            "data": { "password": "aHVudGVyMg==" },
            "immutable": true
        }),
        "ConfigMap" => json!({
            "data": { "k": "v" },
            "binaryData": { "bin": "AAEC" }
        }),
        "Endpoints" => json!({
            "subsets": [ {
                "addresses": [ { "ip": "10.0.0.7",
                    "targetRef": { "kind": "Pod", "name": "p", "namespace": "default" } } ],
                "ports": [ { "name": "http", "port": 8080, "protocol": "TCP" } ]
            } ]
        }),
        "ServiceAccount" => json!({
            "secrets": [ { "name": "token" } ],
            "automountServiceAccountToken": false
        }),
        "Namespace" => json!({
            "spec": { "finalizers": ["kubernetes"] },
            "status": { "phase": "Active" }
        }),
        "Role" => json!({
            "rules": [ { "apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"] } ]
        }),
        "ClusterRole" => json!({
            "rules": [ { "nonResourceURLs": ["/healthz"], "verbs": ["get"] } ],
            "aggregationRule": { "clusterRoleSelectors": [ selector() ] }
        }),
        "RoleBinding" | "ClusterRoleBinding" => json!({
            "roleRef": { "apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "view" },
            "subjects": [ { "kind": "User", "apiGroup": "rbac.authorization.k8s.io", "name": "alice" } ]
        }),
        "SubjectAccessReview" => json!({
            "spec": {
                "resourceAttributes": { "namespace": "default", "verb": "get", "resource": "pods" },
                "user": "alice",
                "groups": ["dev"],
                "extra": { "scopes": ["read"] },
                "uid": "u-1"
            },
            "status": { "allowed": true, "reason": "rbac" }
        }),
        "SelfSubjectAccessReview" => json!({
            "spec": { "nonResourceAttributes": { "path": "/healthz", "verb": "get" } },
            "status": { "allowed": false, "denied": true }
        }),
        "SelfSubjectRulesReview" => json!({
            "spec": { "namespace": "default" },
            "status": {
                "resourceRules": [ { "verbs": ["get"], "apiGroups": [""], "resources": ["pods"] } ],
                "nonResourceRules": [ { "verbs": ["get"], "nonResourceURLs": ["/api"] } ],
                "incomplete": false
            }
        }),
        "TokenReview" => json!({
            "spec": { "token": "t", "audiences": ["api"] },
            "status": {
                "authenticated": true,
                "user": { "username": "alice", "uid": "u-1", "groups": ["dev"],
                    "extra": { "scopes": ["read", "write"] } },
                "audiences": ["api"]
            }
        }),
        _ => return None,
    })
}

/// Every kind the router serves that has a vendored message.
fn covered_kinds() -> Vec<&'static engenho_types::generated_v1_34::ResourceDescriptor> {
    RESOURCE_CATALOG
        .iter()
        .filter(|d| message_for_gvk(&Gvk::new(d.api_version, d.kind)).is_ok())
        .collect()
}

fn object_for(d: &engenho_types::generated_v1_34::ResourceDescriptor) -> Value {
    let mut metadata = json!({
        "name": "demo",
        "uid": "5f0c9c3e-0000-4000-8000-000000000001",
        "resourceVersion": "12",
        "generation": 2,
        "creationTimestamp": "2025-09-19T10:00:00Z",
        "deletionTimestamp": "2025-09-19T10:05:00Z",
        "deletionGracePeriodSeconds": 30,
        "labels": { "app": "demo" },
        "annotations": { "note": "x" },
        "ownerReferences": [ {
            "apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "rs",
            "uid": "5f0c9c3e-0000-4000-8000-000000000002",
            "controller": true, "blockOwnerDeletion": true
        } ],
        "finalizers": ["example.com/guard"],
        "managedFields": [ {
            "manager": "kubectl", "operation": "Update", "apiVersion": d.api_version,
            "time": "2025-09-19T10:01:00Z", "fieldsType": "FieldsV1",
            "fieldsV1": { "f:metadata": { "f:labels": { "f:app": {} } } }
        } ]
    });
    if d.namespaced {
        metadata["namespace"] = json!("default");
    }
    let mut object = Map::new();
    object.insert("apiVersion".into(), json!(d.api_version));
    object.insert("kind".into(), json!(d.kind));
    object.insert("metadata".into(), metadata);
    if let Some(Value::Object(extra)) = payload(d.kind) {
        object.extend(extra);
    }
    Value::Object(object)
}

/// Encoding then decoding what engenho serves returns it unchanged, for
/// every served kind with a message and for its list. An inline embed that
/// came back nested, a time that came back an object, an int64 that came
/// back a string, or a field the message lacks all fail it.
#[test]
fn every_covered_kind_round_trips_exactly_as_object_and_list() {
    let mut failures = Vec::new();
    let mut cells = 0;
    let mut listless = Vec::new();
    for d in covered_kinds() {
        let object = object_for(d);
        let gvk = Gvk::new(d.api_version, d.kind);
        cells += 1;
        match proto_transcode::encode(&gvk, &object)
            .and_then(|e| Ok((e.losses, proto_transcode::decode(&e.bytes)?)))
        {
            Ok((encode_losses, decoded)) => {
                if !encode_losses.is_empty() || !decoded.losses.is_empty() {
                    failures.push(format!(
                        "{}: losses {encode_losses:?} / {:?}",
                        d.kind, decoded.losses
                    ));
                }
                if decoded.value != object {
                    failures.push(format!(
                        "{}: object came back\n  {}\nwant\n  {object}",
                        d.kind, decoded.value
                    ));
                }
            }
            Err(e) => failures.push(format!("{}: {e}", d.kind)),
        }

        // The list: items carry TypeMeta in engenho's JSON, and the wire
        // carries it once, in the wrapper (so it is consumed, not lost).
        let list_kind = [d.kind, "List"].concat();
        if message_for_gvk(&Gvk::new(d.api_version, list_kind.as_str())).is_err() {
            listless.push(d.kind);
            continue;
        }
        cells += 1;
        let list = json!({
            "apiVersion": d.api_version,
            "kind": list_kind,
            "metadata": { "resourceVersion": "42", "continue": "c1", "remainingItemCount": 1 },
            "items": [ object.clone() ]
        });
        let mut item = object.clone();
        if let Value::Object(map) = &mut item {
            map.remove("apiVersion");
            map.remove("kind");
        }
        let want = json!({
            "apiVersion": d.api_version,
            "kind": list_kind,
            "metadata": { "resourceVersion": "42", "continue": "c1", "remainingItemCount": 1 },
            "items": [ item ]
        });
        match proto_transcode::encode(&Gvk::new(d.api_version, list_kind.as_str()), &list)
            .and_then(|e| Ok((e.losses, proto_transcode::decode(&e.bytes)?)))
        {
            Ok((encode_losses, decoded)) => {
                if !encode_losses.is_empty() || !decoded.losses.is_empty() {
                    failures.push(format!(
                        "{list_kind}: losses {encode_losses:?} / {:?}",
                        decoded.losses
                    ));
                }
                if decoded.value != want {
                    failures.push(format!(
                        "{list_kind}: list came back\n  {}\nwant\n  {want}",
                        decoded.value
                    ));
                }
            }
            Err(e) => failures.push(format!("{list_kind}: {e}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {cells} cells not exact:\n{}",
        failures.len(),
        failures.join("\n")
    );
    // Reviews are create-only: upstream defines no list for them.
    listless.sort_unstable();
    assert_eq!(
        listless,
        [
            "SelfSubjectAccessReview",
            "SelfSubjectRulesReview",
            "SubjectAccessReview",
            "TokenReview"
        ]
    );
    assert_eq!(
        cells,
        29 + 25,
        "every covered kind, and every list upstream defines"
    );
}

/// A payload row for a kind that is not covered is a stale row: the matrix
/// would silently stop exercising what it names.
#[test]
fn every_payload_row_names_a_covered_kind() {
    let covered: Vec<&str> = covered_kinds().iter().map(|d| d.kind).collect();
    for kind in [
        "Pod",
        "Deployment",
        "ReplicaSet",
        "ReplicationController",
        "StatefulSet",
        "DaemonSet",
        "ControllerRevision",
        "PodTemplate",
        "Service",
        "PersistentVolumeClaim",
        "PersistentVolume",
        "Node",
        "Lease",
        "Event",
        "ResourceQuota",
        "LimitRange",
        "Secret",
        "ConfigMap",
        "Endpoints",
        "ServiceAccount",
        "Namespace",
        "Role",
        "ClusterRole",
        "RoleBinding",
        "ClusterRoleBinding",
        "SubjectAccessReview",
        "SelfSubjectAccessReview",
        "SelfSubjectRulesReview",
        "TokenReview",
    ] {
        assert!(payload(kind).is_some(), "{kind} lost its payload row");
        assert!(
            covered.contains(&kind),
            "{kind} has a payload row but no message"
        );
    }
}

/// The coverage census, pinned: 29 served kinds have a vendored message and
/// 23 do not. The 22 are answered in JSON when the client takes JSON (every
/// client-go client does) and refused with 415 as a protobuf request body.
/// Vendoring a group into engenho-kube-proto moves kinds from the second
/// list to the first, and this test is where that is recorded.
#[test]
fn the_protobuf_coverage_census() {
    let mut uncovered: Vec<String> = RESOURCE_CATALOG
        .iter()
        .filter(|d| message_for_gvk(&Gvk::new(d.api_version, d.kind)).is_err())
        .map(|d| [d.api_version, "/", d.kind].concat())
        .collect();
    uncovered.sort();
    assert_eq!(covered_kinds().len(), 29);
    assert_eq!(
        uncovered,
        [
            "admissionregistration.k8s.io/v1/MutatingWebhookConfiguration",
            "admissionregistration.k8s.io/v1/ValidatingWebhookConfiguration",
            "apiextensions.k8s.io/v1/CustomResourceDefinition",
            "apiregistration.k8s.io/v1/APIService",
            "authorization.k8s.io/v1/LocalSubjectAccessReview",
            "autoscaling/v2/HorizontalPodAutoscaler",
            "batch/v1/CronJob",
            "batch/v1/Job",
            "certificates.k8s.io/v1/CertificateSigningRequest",
            "discovery.k8s.io/v1/EndpointSlice",
            "flowcontrol.apiserver.k8s.io/v1/FlowSchema",
            "flowcontrol.apiserver.k8s.io/v1/PriorityLevelConfiguration",
            "networking.k8s.io/v1/Ingress",
            "networking.k8s.io/v1/IngressClass",
            "networking.k8s.io/v1/NetworkPolicy",
            "node.k8s.io/v1/RuntimeClass",
            "policy/v1/PodDisruptionBudget",
            "scheduling.k8s.io/v1/PriorityClass",
            "storage.k8s.io/v1/CSIDriver",
            "storage.k8s.io/v1/CSINode",
            "storage.k8s.io/v1/CSIStorageCapacity",
            "storage.k8s.io/v1/StorageClass",
            "storage.k8s.io/v1/VolumeAttachment",
        ]
    );
}

// ── the descriptor-side gates ───────────────────────────────────────────────

/// Every message reachable from a covered kind (or its list).
fn reachable_messages() -> Vec<prost_reflect::MessageDescriptor> {
    let mut seen = std::collections::BTreeSet::new();
    let mut out = Vec::new();
    let mut stack: Vec<prost_reflect::MessageDescriptor> = Vec::new();
    for d in covered_kinds() {
        for kind in [d.kind.to_string(), [d.kind, "List"].concat()] {
            if let Ok(desc) = message_for_gvk(&Gvk::new(d.api_version, kind)) {
                stack.push(desc);
            }
        }
    }
    while let Some(desc) = stack.pop() {
        if !seen.insert(desc.full_name().to_owned()) {
            continue;
        }
        for field in desc.fields() {
            if let Some(inner) = field.kind().as_message() {
                stack.push(inner.clone());
            }
        }
        out.push(desc);
    }
    out
}

/// A Go `[]string` named type is masked as a message holding only
/// `repeated string items`. Every one reachable must be `Special::StringList`,
/// or its bare-array JSON becomes `{"items": […]}`.
#[test]
fn every_reachable_masked_string_list_is_special() {
    let mut masked_seen = Vec::new();
    for desc in reachable_messages() {
        let fields: Vec<_> = desc.fields().collect();
        let masked = fields.len() == 1
            && fields[0].name() == "items"
            && fields[0].is_list()
            && matches!(fields[0].kind(), prost_reflect::Kind::String);
        if masked {
            assert_eq!(
                Special::of(&desc),
                Some(Special::StringList),
                "{} is a masked []string the transcoder does not know",
                desc.full_name()
            );
            masked_seen.push(desc.full_name().to_owned());
        }
    }
    // Positive control: the shape test finds the two it must find.
    masked_seen.sort();
    assert_eq!(
        masked_seen,
        [
            "k8s.io.api.authentication.v1.ExtraValue",
            "k8s.io.api.authorization.v1.ExtraValue"
        ]
    );
}

/// Every apimachinery message reachable from a covered kind either has its
/// own JSON form ([`Special`]) or is a plain struct whose proto3 JSON IS its
/// Kubernetes JSON. A newly reachable apimachinery message fails here until
/// someone reads its Go marshaller and files it.
#[test]
fn every_reachable_apimachinery_message_is_classified() {
    const PLAIN: [&str; 7] = [
        "k8s.io.apimachinery.pkg.apis.meta.v1.ObjectMeta",
        "k8s.io.apimachinery.pkg.apis.meta.v1.ListMeta",
        "k8s.io.apimachinery.pkg.apis.meta.v1.OwnerReference",
        "k8s.io.apimachinery.pkg.apis.meta.v1.ManagedFieldsEntry",
        "k8s.io.apimachinery.pkg.apis.meta.v1.LabelSelector",
        "k8s.io.apimachinery.pkg.apis.meta.v1.LabelSelectorRequirement",
        "k8s.io.apimachinery.pkg.apis.meta.v1.Condition",
    ];
    let mut unclassified = Vec::new();
    let mut plain_seen = std::collections::BTreeSet::new();
    for desc in reachable_messages() {
        let name = desc.full_name();
        if PLAIN.contains(&name) {
            plain_seen.insert(name.to_owned());
        }
        // A map field's synthetic entry message is not a Go type.
        if name.starts_with("k8s.io.apimachinery.")
            && !desc.is_map_entry()
            && Special::of(&desc).is_none()
            && !PLAIN.contains(&name)
        {
            unclassified.push(name.to_owned());
        }
    }
    assert!(unclassified.is_empty(), "unclassified: {unclassified:?}");
    // Exact, not a superset: an entry nothing reaches is a claim about
    // nothing.
    let stale: Vec<_> = PLAIN.iter().filter(|p| !plain_seen.contains(**p)).collect();
    assert!(
        stale.is_empty(),
        "PLAIN entries no covered kind reaches: {stale:?}"
    );
}

/// Every special message exists in the vendored pool, so a rename upstream
/// cannot turn an arm into dead code that silently stops matching.
#[test]
fn every_special_message_is_in_the_pool() {
    let pool = message_for_gvk(&Gvk::new("v1", "Namespace"))
        .expect("the pool")
        .parent_pool()
        .clone();
    for special in Special::ALL {
        for name in special.messages() {
            assert!(
                pool.get_message_by_name(name).is_some(),
                "{special:?}: {name} is not vendored"
            );
        }
    }
}

/// Every inline embed names a singular message field that exists: the table
/// is transcribed from Go source, and this is what keeps it pointed at the
/// descriptors.
#[test]
fn every_inline_embed_is_a_message_field_in_the_pool() {
    let pool = message_for_gvk(&Gvk::new("v1", "Namespace"))
        .expect("the pool")
        .parent_pool()
        .clone();
    assert_eq!(INLINE.len(), 11, "the v1.34.0 json:\",inline\" census");
    for (message, field) in INLINE {
        let desc = pool
            .get_message_by_name(message)
            .unwrap_or_else(|| panic!("{message} is not vendored"));
        let f = desc
            .get_field_by_name(field)
            .unwrap_or_else(|| panic!("{message} has no field {field}"));
        assert!(
            f.kind().as_message().is_some() && !f.is_list(),
            "{message}.{field}"
        );
    }
}

// ── over HTTP ───────────────────────────────────────────────────────────────

async fn boot() -> (Arc<StoreMesh>, ApiServer) {
    let router = InProcessRouter::new();
    let cfg = default_config("apiserver-w3-proto").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    let handlers = handlers_from_catalog(store.clone());
    let server = ApiServer::start("127.0.0.1:0".parse().unwrap(), handlers, None)
        .await
        .unwrap();
    (store, server)
}

const PROTOBUF_OR_JSON: &str = "application/vnd.kubernetes.protobuf,application/json";

fn content_type(resp: &reqwest::Response) -> String {
    resp.headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}

fn warnings(resp: &reqwest::Response) -> Vec<String> {
    resp.headers()
        .get_all("warning")
        .iter()
        .filter_map(|v| v.to_str().ok().map(str::to_owned))
        .collect()
}

async fn get_json(client: &reqwest::Client, url: &str) -> Value {
    let resp = client
        .get(url)
        .header("Accept", "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "GET {url}");
    resp.json().await.unwrap()
}

/// Reading an object over protobuf gives exactly what reading it over JSON
/// gives: the equality with the JSON path.
async fn protobuf_read_equals_json_read(client: &reqwest::Client, url: &str) -> Value {
    let json_body = get_json(client, url).await;
    let resp = client
        .get(url)
        .header("Accept", PROTOBUF_OR_JSON)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert!(
        content_type(&resp).starts_with(CONTENT_TYPE_PROTOBUF),
        "an exact object is served as protobuf, got {}",
        content_type(&resp)
    );
    let decoded = proto_transcode::decode(&resp.bytes().await.unwrap()).expect("decodes");
    assert!(decoded.losses.is_empty(), "{:?}", decoded.losses);
    assert_eq!(decoded.value, json_body, "protobuf read != JSON read");
    json_body
}

/// A Deployment written by a protobuf client is stored as upstream JSON:
/// quantities are strings, IntOrStrings are bare, inline embeds are flat,
/// `generation` is a number. Reading it back over protobuf equals reading
/// it over JSON (the old codec 400'd: `"500m"` is not a Quantity message).
#[tokio::test]
async fn a_protobuf_deployment_is_stored_as_upstream_json() {
    let (_store, server) = boot().await;
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::new();
    let golden = deployment();

    let resp = client
        .post(format!(
            "{base}/apis/apps/v1/namespaces/default/deployments"
        ))
        .header("Content-Type", CONTENT_TYPE_PROTOBUF)
        .header("Accept", PROTOBUF_OR_JSON)
        .body(wire::go_frame(
            golden.api_version,
            golden.kind,
            &golden.object,
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::CREATED,
        "{:?}",
        resp.text().await
    );

    let url = format!("{base}/apis/apps/v1/namespaces/default/deployments/web");
    let stored = protobuf_read_equals_json_read(&client, &url).await;
    let c = &stored["spec"]["template"]["spec"]["containers"][0];
    assert_eq!(c["resources"]["limits"]["cpu"], "500m");
    assert_eq!(
        c["env"][0]["valueFrom"]["configMapKeyRef"],
        json!({ "name": "app-config", "key": "mode" })
    );
    assert_eq!(c["livenessProbe"]["httpGet"]["port"], "http");
    assert_eq!(
        stored["spec"]["template"]["spec"]["volumes"][0]["emptyDir"]["sizeLimit"],
        "1Gi"
    );
    assert_eq!(
        stored["spec"]["strategy"]["rollingUpdate"],
        json!({ "maxUnavailable": 1, "maxSurge": "25%" })
    );
    assert_eq!(stored["spec"]["replicas"], 0);
    assert!(stored["metadata"]["generation"].is_number(), "{stored}");
}

/// Leader election: client-go writes Leases over protobuf. `renewTime` is
/// stored as a MicroTime string, and reads back over protobuf equal to JSON.
#[tokio::test]
async fn a_protobuf_lease_keeps_its_micro_time() {
    let (_store, server) = boot().await;
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::new();
    let golden = lease(123_456_000);

    let resp = client
        .post(format!(
            "{base}/apis/coordination.k8s.io/v1/namespaces/kube-system/leases"
        ))
        .header("Content-Type", CONTENT_TYPE_PROTOBUF)
        .header("Accept", PROTOBUF_OR_JSON)
        .body(wire::go_frame(
            golden.api_version,
            golden.kind,
            &golden.object,
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::CREATED,
        "{:?}",
        resp.text().await
    );

    let url = format!("{base}/apis/coordination.k8s.io/v1/namespaces/kube-system/leases/leader");
    let stored = protobuf_read_equals_json_read(&client, &url).await;
    assert_eq!(stored["spec"]["renewTime"], "2024-09-10T20:26:55.123456Z");
    assert_eq!(stored["spec"]["acquireTime"], "2024-09-10T20:26:40.000000Z");
}

/// A kind with no vendored message is answered in JSON when the client takes
/// JSON (every client-go client does), and 406 when it takes only protobuf.
/// The old codec answered 400 to both, so a Go client listing EndpointSlices,
/// Jobs or Ingresses over protobuf could not list them at all.
#[tokio::test]
async fn an_uncovered_kind_falls_back_to_json() {
    let (_store, server) = boot().await;
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/apis/networking.k8s.io/v1/ingressclasses"))
        .header("Content-Type", "application/json")
        .json(&json!({
            "apiVersion": "networking.k8s.io/v1", "kind": "IngressClass",
            "metadata": { "name": "nginx" },
            "spec": { "controller": "k8s.io/ingress-nginx" }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::CREATED,
        "{:?}",
        resp.text().await
    );

    for url in [
        format!("{base}/apis/networking.k8s.io/v1/ingressclasses/nginx"),
        format!("{base}/apis/networking.k8s.io/v1/ingressclasses"),
    ] {
        let resp = client
            .get(&url)
            .header("Accept", PROTOBUF_OR_JSON)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "GET {url}");
        assert!(
            content_type(&resp).starts_with("application/json"),
            "GET {url}: {}",
            content_type(&resp)
        );
        let body: Value = resp.json().await.unwrap();
        assert!(
            body["kind"] == "IngressClass" || body["kind"] == "IngressClassList",
            "{body}"
        );

        let resp = client
            .get(&url)
            .header("Accept", CONTENT_TYPE_PROTOBUF)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::NOT_ACCEPTABLE,
            "GET {url}"
        );
        let status: Value = resp.json().await.unwrap();
        assert_eq!(status["reason"], "NotAcceptable", "{status}");
    }
}

/// A protobuf request body for a kind with no message is 415, as upstream
/// answers a custom resource's: the client sent a media type this kind does
/// not take. The old codec said 400, a malformed request.
#[tokio::test]
async fn a_protobuf_body_for_an_uncovered_kind_is_415() {
    let (_store, server) = boot().await;
    let base = format!("http://{}", server.local_addr());
    let resp = reqwest::Client::new()
        .post(format!("{base}/apis/networking.k8s.io/v1/ingressclasses"))
        .header("Content-Type", CONTENT_TYPE_PROTOBUF)
        .body(wire::go_frame(
            "networking.k8s.io/v1",
            "IngressClass",
            &[msg(1, &[string(1, "nginx")])],
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE);
    let status: Value = resp.json().await.unwrap();
    assert_eq!(status["reason"], "UnsupportedMediaType", "{status}");
}

/// An object whose protobuf would lose data is served as JSON when the
/// client takes JSON, and as protobuf with a warning naming the loss when
/// it takes only protobuf.
#[tokio::test]
async fn a_lossy_object_is_served_as_json_when_json_is_accepted() {
    let (_store, server) = boot().await;
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::new();

    let resp = client
        .post(format!(
            "{base}/apis/apps/v1/namespaces/default/deployments"
        ))
        .header("Content-Type", "application/json")
        .json(&json!({
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": { "name": "frac" },
            "spec": {
                "selector": selector(),
                "template": {
                    "metadata": { "labels": { "app": "demo" },
                        "creationTimestamp": "2025-09-19T10:00:00.5Z" },
                    "spec": { "containers": [ { "name": "app", "image": "nginx" } ] }
                }
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::CREATED,
        "{:?}",
        resp.text().await
    );
    let url = format!("{base}/apis/apps/v1/namespaces/default/deployments/frac");
    let json_body = get_json(&client, &url).await;
    assert_eq!(
        json_body["spec"]["template"]["metadata"]["creationTimestamp"], "2025-09-19T10:00:00.5Z",
        "precondition: the store keeps the fraction"
    );

    let resp = client
        .get(&url)
        .header("Accept", PROTOBUF_OR_JSON)
        .send()
        .await
        .unwrap();
    assert!(
        content_type(&resp).starts_with("application/json"),
        "{}",
        content_type(&resp)
    );
    assert_eq!(resp.json::<Value>().await.unwrap(), json_body);

    let resp = client
        .get(&url)
        .header("Accept", CONTENT_TYPE_PROTOBUF)
        .send()
        .await
        .unwrap();
    assert!(
        content_type(&resp).starts_with(CONTENT_TYPE_PROTOBUF),
        "{}",
        content_type(&resp)
    );
    let named = warnings(&resp);
    assert!(
        named
            .iter()
            .any(|w| w.contains(&KnownLossy::TimeSubsecond.to_string())),
        "the loss must be named: {named:?}"
    );
}

/// A protobuf body carrying a field the vendored descriptors do not define
/// is stored without it, and the client is told.
#[tokio::test]
async fn a_dropped_wire_field_is_a_warning_on_create() {
    let (_store, server) = boot().await;
    let base = format!("http://{}", server.local_addr());
    let mut inner = KUBECTL_CONFIGMAP[23..63].to_vec();
    inner.extend(string(99, "from a newer client"));
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/namespaces/default/configmaps"))
        .header("Content-Type", CONTENT_TYPE_PROTOBUF)
        .header("Accept", "application/json")
        .body(wire::go_frame("v1", "ConfigMap", &[inner]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let named = warnings(&resp);
    assert!(
        named
            .iter()
            .any(|w| w.contains(&KnownLossy::UnknownWireField.to_string())),
        "the loss must be named: {named:?}"
    );
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["hello"], "engenho");
}

/// The JSON fallback is chosen from `Accept`, never assumed: `*/*` and
/// `application/*` take JSON too.
#[tokio::test]
async fn wildcards_admit_the_json_fallback() {
    let (_store, server) = boot().await;
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::new();
    client
        .post(format!("{base}/apis/networking.k8s.io/v1/ingressclasses"))
        .json(
            &json!({ "apiVersion": "networking.k8s.io/v1", "kind": "IngressClass",
            "metadata": { "name": "nginx" } }),
        )
        .send()
        .await
        .unwrap();
    for accept in [
        "application/vnd.kubernetes.protobuf, */*",
        "application/vnd.kubernetes.protobuf;q=1.0, application/*;q=0.5",
    ] {
        let resp = client
            .get(format!(
                "{base}/apis/networking.k8s.io/v1/ingressclasses/nginx"
            ))
            .header("Accept", accept)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "Accept: {accept}");
        assert!(
            content_type(&resp).starts_with("application/json"),
            "Accept: {accept}"
        );
    }
}

/// The oracle writer reproduces kubectl's captured bytes exactly, so the
/// goldens above are what Go writes and not what this file believes.
#[test]
fn the_writer_frames_like_kubectl() {
    let rebuilt = wire::go_frame(
        "v1",
        "ConfigMap",
        &[
            msg(
                1,
                &[
                    string(1, "demo"),
                    string(2, ""),
                    string(3, ""),
                    string(4, ""),
                    string(5, ""),
                    string(6, ""),
                    int(7, 0),
                    msg(8, &[]),
                ],
            ),
            entry(2, "hello", string(2, "engenho")),
        ],
    );
    assert_eq!(
        rebuilt, KUBECTL_CONFIGMAP,
        "the oracle writer must reproduce kubectl's bytes"
    );
}
