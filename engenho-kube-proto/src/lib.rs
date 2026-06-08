//! Kubernetes protobuf wire codec for the engenho apiserver HTTP boundary.
//!
//! kubectl's typed clientset (the imperative `kubectl create configmap`,
//! `kubectl create secret`, `kubectl create deployment`, … generators)
//! negotiates `application/vnd.kubernetes.protobuf` ONCE at client
//! construction and never renegotiates: a 415 from the server is a
//! terminal error, NOT a fall-back-to-JSON trigger (verified empirically —
//! see the crate-level docs in the apiserver). So the apiserver MUST speak
//! the protobuf wire format to serve the typed-clientset write path that
//! CNCF conformance exercises heavily.
//!
//! The wire format (verified live against kubectl v1.34.3):
//!
//! ```text
//! [ 6b 38 73 00 ]  4-byte MAGIC = "k8s\0"
//! [ protobuf    ]  runtime.Unknown {
//!                    field 1 typeMeta = TypeMeta { apiVersion, kind }
//!                    field 2 raw      = <the object, protobuf-encoded
//!                                        per its own generated.proto>
//!                    field 3 contentEncoding (absent/"" = none)
//!                    field 4 contentType    (absent/"" = the raw is
//!                                            itself protobuf; "application/json"
//!                                            = the raw is literally JSON)
//!                  }
//! ```
//!
//! This crate exposes two TOTAL functions over the boundary:
//!
//!   * [`decode_request`] — `(content_type, bytes) -> serde_json::Value`.
//!     Strips + verifies the magic, decodes the `Unknown` wrapper, selects
//!     the per-kind message by the embedded `TypeMeta {apiVersion,kind}`,
//!     and converts the raw object to the EXACT `serde_json::Value` the
//!     JSON-Value handler pipeline already expects.
//!   * [`encode_response`] — `(gvk, value) -> Bytes`. Builds the per-kind
//!     `DynamicMessage` from the read-back Value, wraps it in
//!     `runtime.Unknown`, and prepends the magic.
//!
//! Both are driven by the SAME vendored descriptors (`build.rs` →
//! `protox` → `FileDescriptorSet` → [`prost_reflect::DescriptorPool`]),
//! so the JSON field names come from the proto's `json_name`
//! (== K8s camelCase) and are never hand-maintained. The downstream
//! store / admission / MVCC / read paths see only `serde_json::Value`
//! and are untouched.

use std::sync::OnceLock;

use bytes::Bytes;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use prost_reflect::{DeserializeOptions, SerializeOptions};

/// The 4-byte magic prefix every K8s protobuf payload carries: `"k8s\0"`.
pub const PROTOBUF_MAGIC: [u8; 4] = [0x6b, 0x38, 0x73, 0x00];

/// The `Content-Type` / `Accept` token for the K8s protobuf serializer.
pub const CONTENT_TYPE_PROTOBUF: &str = "application/vnd.kubernetes.protobuf";

/// The `Content-Type` token for JSON.
pub const CONTENT_TYPE_JSON: &str = "application/json";

/// The full-name of the `runtime.Unknown` wrapper message.
const UNKNOWN_FQ: &str = "k8s.io.apimachinery.pkg.runtime.Unknown";

/// The encoded `FileDescriptorSet`, compiled by `build.rs` (protox).
static DESCRIPTOR_SET_BYTES: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/k8s_descriptors.bin"));

/// Typed codec errors. Every error is a typed value — no panic, no
/// `unimplemented!()`. The apiserver maps these to a proper K8s `Status`
/// body (a 400 BadRequest for malformed / unknown input).
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// The 4-byte magic prefix was missing or wrong.
    #[error("not a kubernetes protobuf payload: bad magic (expected k8s\\0)")]
    BadMagic,
    /// The `runtime.Unknown` wrapper failed to decode.
    #[error("malformed runtime.Unknown wrapper: {0}")]
    BadWrapper(String),
    /// The wrapper carried no `TypeMeta {apiVersion,kind}` — we cannot
    /// select a message descriptor without it.
    #[error("protobuf payload missing TypeMeta (apiVersion/kind); cannot select decoder")]
    MissingTypeMeta,
    /// The `(apiVersion, kind)` is not one of the cataloged kinds, so no
    /// descriptor exists to decode/encode the object.
    #[error("uncataloged kind for protobuf codec: apiVersion={api_version:?} kind={kind:?}")]
    UncatalogedKind { api_version: String, kind: String },
    /// The inner object's protobuf bytes failed to decode against the
    /// selected per-kind descriptor.
    #[error("failed to decode {kind} object body: {source}")]
    DecodeObject {
        kind: String,
        source: prost::DecodeError,
    },
    /// The `DynamicMessage -> serde_json::Value` conversion failed.
    #[error("failed to convert {kind} protobuf to JSON: {source}")]
    ToJson {
        kind: String,
        source: serde_json::Error,
    },
    /// The `serde_json::Value -> DynamicMessage` conversion failed (encode).
    #[error("failed to convert {kind} JSON to protobuf: {source}")]
    FromJson {
        kind: String,
        source: serde_json::Error,
    },
    /// The contentType field declared a JSON inner body that failed to
    /// parse as JSON.
    #[error("runtime.Unknown declared contentType=application/json but raw is not valid JSON: {0}")]
    BadInnerJson(serde_json::Error),
    /// A descriptor that should exist (the `Unknown` wrapper, or a
    /// cataloged kind) was absent from the compiled pool — a build-time
    /// invariant violation surfaced at runtime rather than a silent wrong
    /// answer.
    #[error("descriptor pool is missing required message {0} (build/vendor drift)")]
    MissingDescriptor(String),
}

/// A Group/Version/Kind triple — the apiserver's key for selecting the
/// per-kind message descriptor. `api_version` is the K8s wire form
/// (`"v1"` for core, `"<group>/<version>"` otherwise) so it matches the
/// value carried in `TypeMeta.apiVersion`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Gvk {
    /// `"v1"` or `"apps/v1"` or `"rbac.authorization.k8s.io/v1"`.
    pub api_version: String,
    /// `"ConfigMap"`, `"Deployment"`, …
    pub kind: String,
}

impl Gvk {
    /// Construct from the K8s wire `(apiVersion, kind)`.
    #[must_use]
    pub fn new(api_version: impl Into<String>, kind: impl Into<String>) -> Self {
        Self {
            api_version: api_version.into(),
            kind: kind.into(),
        }
    }
}

/// The process-wide descriptor pool, decoded once from the embedded
/// `FileDescriptorSet`.
fn pool() -> &'static DescriptorPool {
    static POOL: OnceLock<DescriptorPool> = OnceLock::new();
    POOL.get_or_init(|| {
        DescriptorPool::decode(DESCRIPTOR_SET_BYTES)
            .expect("decode embedded k8s FileDescriptorSet (built by protox in build.rs)")
    })
}

/// Map a K8s `apiVersion` to the protobuf package prefix the
/// go-to-protobuf message lives under. `(apiVersion, kind)` →
/// `<package>.<kind>` (e.g. `v1` + `ConfigMap` →
/// `k8s.io.api.core.v1.ConfigMap`).
///
/// Returns `None` for an apiVersion outside the cataloged groups so the
/// caller raises a typed [`CodecError::UncatalogedKind`] (never a panic).
fn package_for_api_version(api_version: &str) -> Option<&'static str> {
    match api_version {
        "v1" => Some("k8s.io.api.core.v1"),
        "apps/v1" => Some("k8s.io.api.apps.v1"),
        "rbac.authorization.k8s.io/v1" => Some("k8s.io.api.rbac.v1"),
        // The SubjectAccessReview family (kubectl `auth can-i` posts these as
        // application/vnd.kubernetes.protobuf).
        "authorization.k8s.io/v1" => Some("k8s.io.api.authorization.v1"),
        _ => None,
    }
}

/// Resolve the per-kind [`MessageDescriptor`] for a GVK, asserting the
/// kind is one of the 18 cataloged kinds (a descriptor must exist in the
/// pool). The pair (cataloged set + pool membership) is exercised by the
/// crate's matrix test.
///
/// # Errors
///
/// [`CodecError::UncatalogedKind`] when `gvk.api_version` is outside the
/// cataloged groups OR the `<package>.<kind>` message is absent from the
/// compiled pool.
pub fn message_for_gvk(gvk: &Gvk) -> Result<MessageDescriptor, CodecError> {
    let pkg = package_for_api_version(&gvk.api_version).ok_or_else(|| {
        CodecError::UncatalogedKind {
            api_version: gvk.api_version.clone(),
            kind: gvk.kind.clone(),
        }
    })?;
    let full_name = format!("{pkg}.{}", gvk.kind);
    pool()
        .get_message_by_name(&full_name)
        .ok_or(CodecError::UncatalogedKind {
            api_version: gvk.api_version.clone(),
            kind: gvk.kind.clone(),
        })
}

/// The `runtime.Unknown` wrapper descriptor.
fn unknown_descriptor() -> Result<MessageDescriptor, CodecError> {
    pool()
        .get_message_by_name(UNKNOWN_FQ)
        .ok_or_else(|| CodecError::MissingDescriptor(UNKNOWN_FQ.to_string()))
}

/// serde options that emit the proto `json_name` (== K8s camelCase) and
/// DO emit default/zero values exactly as the JSON-Value pipeline
/// already round-trips them. (K8s JSON is `omitempty`-shaped, but proto3
/// JSON defaults to skipping defaults; for the create body the handler
/// only reads `metadata.name` + the kind-specific payload, all of which
/// are present, so the default-skip behavior is harmless and matches what
/// kubectl sends.)
fn serialize_options() -> SerializeOptions {
    SerializeOptions::new().use_proto_field_name(false)
}

/// serde options that read the proto `json_name` (== K8s camelCase) and
/// tolerate unknown fields (so a kubectl JSON body carrying a field the
/// proto doesn't model is not a hard error).
fn deserialize_options() -> DeserializeOptions {
    DeserializeOptions::new().deny_unknown_fields(false)
}

/// Decode an HTTP request body into the `serde_json::Value` the handler
/// pipeline expects.
///
/// `content_type` is the request `Content-Type` header.
///
///   * `application/json` (or a JSON family type) → the body is parsed
///     as JSON directly (the existing path; callers may also use
///     `serde_json::from_slice` themselves — this branch exists so a
///     single boundary function handles both).
///   * `application/vnd.kubernetes.protobuf` → strip the magic, decode
///     the `runtime.Unknown` wrapper, select the per-kind message by the
///     embedded `TypeMeta`, decode the raw object, convert to JSON Value.
///     If the wrapper's `contentType` is `application/json`, the raw is
///     literally JSON and is parsed directly.
///
/// # Errors
///
/// A typed [`CodecError`] for any malformed / unknown / undecodable
/// input — never a panic. The caller (apiserver router) maps the error
/// to a proper K8s `Status` body.
pub fn decode_request(content_type: &str, bytes: &[u8]) -> Result<serde_json::Value, CodecError> {
    if is_protobuf_content_type(content_type) {
        decode_protobuf(bytes)
    } else {
        // JSON (or JSON-family) body: parse directly.
        serde_json::from_slice(bytes).map_err(CodecError::BadInnerJson)
    }
}

/// Decode a magic-prefixed `runtime.Unknown`-framed K8s protobuf payload
/// into a `serde_json::Value`.
///
/// # Errors
///
/// See [`decode_request`].
pub fn decode_protobuf(bytes: &[u8]) -> Result<serde_json::Value, CodecError> {
    let body = strip_magic(bytes)?;

    // Decode the runtime.Unknown wrapper dynamically.
    let unknown_desc = unknown_descriptor()?;
    let unknown = DynamicMessage::decode(unknown_desc, body)
        .map_err(|e| CodecError::BadWrapper(e.to_string()))?;

    // Pull TypeMeta {apiVersion, kind} out of field 1.
    let (api_version, kind) = read_type_meta(&unknown)?;
    let gvk = Gvk::new(api_version, kind);

    // The inner object bytes (field 2 `raw`).
    let raw = unknown
        .get_field_by_name("raw")
        .map(|v| v.as_bytes().cloned().unwrap_or_default())
        .unwrap_or_default();

    // contentType (field 4): EXPLICITLY "application/json" means `raw` is
    // literal JSON; absent/"" means `raw` is protobuf per the per-kind
    // descriptor (the common kubectl case). Note: empty is NOT JSON here —
    // unlike the OUTER request Content-Type, an empty inner contentType is
    // the default-protobuf signal, so we test for an explicit JSON media
    // type only.
    let content_type = unknown
        .get_field_by_name("contentType")
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default();

    if content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .eq_ignore_ascii_case(CONTENT_TYPE_JSON)
    {
        // The raw is literally JSON — parse it directly.
        return serde_json::from_slice(&raw).map_err(CodecError::BadInnerJson);
    }

    // Select the per-kind message descriptor and decode the raw object.
    let msg_desc = message_for_gvk(&gvk)?;
    let dynamic =
        DynamicMessage::decode(msg_desc, raw.as_ref()).map_err(|source| CodecError::DecodeObject {
            kind: gvk.kind.clone(),
            source,
        })?;

    // DynamicMessage -> serde_json::Value via the descriptor json_name
    // (== K8s camelCase).
    let mut value = json_value_from_dynamic(&dynamic, &gvk.kind)?;

    // The protobuf object's TypeMeta is carried in the WRAPPER, not the
    // inner message — re-inject apiVersion + kind so the handler (which
    // reads metadata.name + the payload, and the store which read-backs)
    // sees a complete object exactly as the JSON path produces.
    if let serde_json::Value::Object(map) = &mut value {
        map.entry("apiVersion".to_string())
            .or_insert_with(|| serde_json::Value::String(gvk.api_version.clone()));
        map.entry("kind".to_string())
            .or_insert_with(|| serde_json::Value::String(gvk.kind.clone()));
    }

    Ok(value)
}

/// Encode a read-back `serde_json::Value` (the object the handler
/// returned) into a magic-prefixed `runtime.Unknown`-framed K8s protobuf
/// response body for the given GVK.
///
/// # Errors
///
/// A typed [`CodecError`] when the value cannot be coerced into the
/// per-kind message (e.g. a field shape the proto rejects) — never a
/// panic.
pub fn encode_response(gvk: &Gvk, value: &serde_json::Value) -> Result<Bytes, CodecError> {
    // Build the per-kind DynamicMessage from the JSON Value.
    let msg_desc = message_for_gvk(gvk)?;
    let serialized = value.to_string();
    let mut de = serde_json::Deserializer::from_str(&serialized);
    let dynamic = DynamicMessage::deserialize_with_options(msg_desc, &mut de, &deserialize_options())
        .map_err(|source| CodecError::FromJson {
            kind: gvk.kind.clone(),
            source,
        })?;
    de.end().map_err(|source| CodecError::FromJson {
        kind: gvk.kind.clone(),
        source,
    })?;
    let raw = dynamic.encode_to_vec();

    // Wrap in runtime.Unknown { typeMeta {apiVersion, kind}, raw }.
    let unknown_desc = unknown_descriptor()?;
    let mut unknown = DynamicMessage::new(unknown_desc.clone());

    // typeMeta field is itself a message (TypeMeta).
    let type_meta_field = unknown_desc
        .get_field_by_name("typeMeta")
        .ok_or_else(|| CodecError::MissingDescriptor(format!("{UNKNOWN_FQ}.typeMeta")))?;
    let type_meta_desc = type_meta_field
        .kind()
        .as_message()
        .cloned()
        .ok_or_else(|| CodecError::MissingDescriptor(format!("{UNKNOWN_FQ}.typeMeta (message)")))?;
    let mut type_meta = DynamicMessage::new(type_meta_desc);
    type_meta.set_field_by_name(
        "apiVersion",
        prost_reflect::Value::String(gvk.api_version.clone()),
    );
    type_meta.set_field_by_name("kind", prost_reflect::Value::String(gvk.kind.clone()));
    unknown.set_field_by_name("typeMeta", prost_reflect::Value::Message(type_meta));

    unknown.set_field_by_name("raw", prost_reflect::Value::Bytes(Bytes::from(raw)));
    // contentType left "" (absent) — the raw is protobuf.

    let mut out = Vec::with_capacity(4 + unknown.encoded_len());
    out.extend_from_slice(&PROTOBUF_MAGIC);
    unknown
        .encode(&mut out)
        .map_err(|e| CodecError::BadWrapper(e.to_string()))?;
    Ok(Bytes::from(out))
}

/// `true` iff `content_type` selects the K8s protobuf serializer.
#[must_use]
pub fn is_protobuf_content_type(content_type: &str) -> bool {
    // Strip any `; charset=...` suffix and compare the media type.
    let media = content_type.split(';').next().unwrap_or("").trim();
    media.eq_ignore_ascii_case(CONTENT_TYPE_PROTOBUF)
}

/// Parse the `Accept` header into the response codec to use.
///
/// kubectl's typed clientset sends `Accept: application/vnd.kubernetes.protobuf,application/json`
/// — protobuf FIRST. We honor that: if protobuf is acceptable we encode
/// protobuf; otherwise JSON. The dynamic/unstructured client sends
/// `Accept: application/json` and gets JSON.
#[must_use]
pub fn response_wants_protobuf(accept: &str) -> bool {
    accept.split(',').any(|part| {
        let media = part.split(';').next().unwrap_or("").trim();
        media.eq_ignore_ascii_case(CONTENT_TYPE_PROTOBUF)
    })
}

// ── internals ──────────────────────────────────────────────────────────

/// Strip + verify the 4-byte magic, returning the protobuf body.
fn strip_magic(bytes: &[u8]) -> Result<&[u8], CodecError> {
    if bytes.len() < 4 || bytes[..4] != PROTOBUF_MAGIC {
        return Err(CodecError::BadMagic);
    }
    Ok(&bytes[4..])
}

/// Read `(apiVersion, kind)` out of the wrapper's `typeMeta` sub-message.
fn read_type_meta(unknown: &DynamicMessage) -> Result<(String, String), CodecError> {
    let tm = unknown
        .get_field_by_name("typeMeta")
        .ok_or(CodecError::MissingTypeMeta)?;
    let tm = tm.as_message().ok_or(CodecError::MissingTypeMeta)?;
    let api_version = tm
        .get_field_by_name("apiVersion")
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default();
    let kind = tm
        .get_field_by_name("kind")
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default();
    if api_version.is_empty() || kind.is_empty() {
        return Err(CodecError::MissingTypeMeta);
    }
    Ok((api_version, kind))
}

/// Convert a [`DynamicMessage`] to a `serde_json::Value` using the
/// descriptor `json_name` (== K8s camelCase).
fn json_value_from_dynamic(
    dynamic: &DynamicMessage,
    kind: &str,
) -> Result<serde_json::Value, CodecError> {
    let mut buf = Vec::new();
    let mut ser = serde_json::Serializer::new(&mut buf);
    dynamic
        .serialize_with_options(&mut ser, &serialize_options())
        .map_err(|source| CodecError::ToJson {
            kind: kind.to_string(),
            source,
        })?;
    serde_json::from_slice(&buf).map_err(|source| CodecError::ToJson {
        kind: kind.to_string(),
        source,
    })
}

#[cfg(test)]
mod tests;
