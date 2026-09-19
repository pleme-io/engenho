//! The protobuf wire, transcoded by its descriptors (plan T4.7).
//!
//! kubectl's typed clientset, Helm, and every client-go leader-election loop
//! speak `application/vnd.kubernetes.protobuf`. The request and response
//! bodies are `k8s\0` + a `runtime.Unknown` whose `raw` is the object encoded
//! by its go-to-protobuf message. The rest of engenho speaks the JSON an
//! upstream apiserver serves, so every protobuf body crosses one transcoder.
//!
//! ## Why the generic proto3-JSON mapping is wrong here
//!
//! The Kubernetes JSON form is not the proto3 JSON mapping of these messages.
//! Some types carry a hand-written Go marshaller whose JSON has nothing to do
//! with their message shape, the proto3 mapping renders every 64-bit integer
//! as a string, and Go's `json:",inline"` flattens a struct the wire nests:
//!
//! | message | wire | upstream JSON |
//! |---|---|---|
//! | `meta.v1.Time` | `{seconds, nanos: 0}` | `"2006-01-02T15:04:05Z"`, zero is `null` |
//! | `meta.v1.MicroTime` | `{seconds, nanos}` | `"2006-01-02T15:04:05.000000Z"`, zero is `null` |
//! | `resource.Quantity` | `{string}` | `"500m"` |
//! | `intstr.IntOrString` | `{type, intVal, strVal}` | `8080` or `"http"` |
//! | `runtime.RawExtension` | `{raw}` | the raw bytes, which are JSON |
//! | `meta.v1.FieldsV1` | `{Raw}` | the raw bytes, which are JSON |
//! | `ExtraValue`, `Verbs` | `{items}` | the bare array |
//! | any `int64` | varint | a JSON number |
//! | an [`INLINE`] embed, e.g. `Probe.handler` | a nested message | the embed's fields, in the parent |
//!
//! The codec this replaces bridged through prost-reflect's proto3 mapping, so
//! a protobuf write stored `renewTime: {"seconds": "1726…", "nanos": …}`,
//! `cpu: {"string": "500m"}`, `generation: "0"` and
//! `configMapKeyRef: {"localObjectReference": {"name": …}}`, and a protobuf
//! read of a Deployment whose containers declare resources failed outright
//! (`"500m"` is not a `Quantity` message). Each arm below follows the
//! upstream Go marshaller at v1.34.0 (`time_proto.go`,
//! `micro_time_proto.go`, `quantity_proto.go`, `intstr.go`, `extension.go`,
//! `helpers.go`); the types are the closed [`Special`] enum, and every match
//! over it, over [`Kind`] and over the wire [`Wire`] value is exhaustive with
//! no `_` arm. The descriptors cannot say which fields are inline embeds, so
//! [`INLINE`] is the census of upstream's `json:",inline"` tags.
//!
//! ## What is exact, and what is not
//!
//! Encoding then decoding a value engenho would serve returns it unchanged,
//! for every served kind that has a descriptor
//! (`tests/w3_protobuf_codec.rs`). Three classes cannot be exact, and each is
//! a [`KnownLossy`] variant the transcoder reports when it happens. The enum
//! is closed and its size is pinned by a ratchet test, so a new class of loss
//! is a reviewed change, never a silent one:
//!
//! - [`KnownLossy::UnknownJsonField`]: a JSON field the kind's message does
//!   not define has nowhere to go on the wire.
//! - [`KnownLossy::TimeSubsecond`]: upstream's `Time.ProtoTime` writes whole
//!   seconds, so a `Time` with a fraction loses it.
//! - [`KnownLossy::UnknownWireField`]: a field number the vendored v1.34
//!   descriptors do not define (a newer client) is dropped, as an upstream
//!   v1.34 apiserver drops it.
//!
//! The router serves JSON instead of a lossy protobuf body whenever the
//! client's `Accept` admits JSON, which every client-go client's does.
//!
//! Not losses, and so not variants: `null` and an absent field are the same
//! Go value; a JSON-number `Quantity` is sent as its decimal text, which Go
//! parses to the same quantity; a decoded body keeps the zero values Go wrote
//! for non-pointer fields (`generateName: ""`), which the descriptor cannot
//! tell apart from pointer fields and which Go reads back identically.
//!
//! ## Stored shapes from the old codec
//!
//! Objects written through the old codec are still in the store. The encoder
//! reads their proto3 shapes (`{"seconds": "…", "nanos": …}` for a time,
//! `{"string": …}` for a quantity, the three-field `IntOrString` object, a
//! decimal string for an integer) as the values they denote, so a Lease a
//! leader-election loop wrote last week stays readable over protobuf.

use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::str::FromStr;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use chrono::DateTime;
use engenho_kube_proto::{CONTENT_TYPE_JSON, CONTENT_TYPE_PROTOBUF, Gvk, PROTOBUF_MAGIC};
use prost::Message as _;
use prost_reflect::{
    DescriptorPool, DynamicMessage, EnumDescriptor, FieldDescriptor, Kind, MapKey,
    MessageDescriptor, ReflectMessage as _, Value as Wire,
};
use serde_json::{Map, Number, Value};

use crate::error::ApiError;
use crate::object_body::{FieldPath, JsonKind};

/// The wrapper every protobuf body is framed in.
const UNKNOWN: &str = "k8s.io.apimachinery.pkg.runtime.Unknown";

/// The wrapper's `typeMeta`.
const TYPE_META: &str = "k8s.io.apimachinery.pkg.runtime.TypeMeta";

/// Go's zero `time.Time`, 0001-01-01T00:00:00Z, in Unix seconds. Its JSON is
/// `null` and its protobuf is an empty message.
const GO_ZERO_TIME_UNIX: i64 = -62_135_596_800;

/// A top-level object's JSON carries its `TypeMeta` inline; the wire carries
/// it in the `runtime.Unknown` wrapper, never in the kind's message.
const TYPE_META_KEYS: [&str; 2] = ["apiVersion", "kind"];

// ── the types whose JSON is not their message ───────────────────────────────

/// A message whose Kubernetes JSON is not its proto3 JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Special {
    /// `metav1.Time`: whole seconds, RFC 3339.
    Time,
    /// `metav1.MicroTime`: microseconds, RFC 3339 with six fraction digits.
    MicroTime,
    /// `resource.Quantity`: its text.
    Quantity,
    /// `intstr.IntOrString`: a JSON number or a JSON string.
    IntOrString,
    /// `runtime.RawExtension`: the embedded JSON document.
    RawExtension,
    /// `metav1.FieldsV1`: the embedded JSON field set.
    FieldsV1,
    /// A Go `[]string` named type (`ExtraValue`, `Verbs`), which
    /// go-to-protobuf masks as `{repeated string items}`: the bare array.
    StringList,
}

impl Special {
    /// Every special message.
    pub const ALL: [Self; 7] = [
        Self::Time,
        Self::MicroTime,
        Self::Quantity,
        Self::IntOrString,
        Self::RawExtension,
        Self::FieldsV1,
        Self::StringList,
    ];

    /// The full protobuf names of the messages this type is.
    #[must_use]
    pub const fn messages(self) -> &'static [&'static str] {
        match self {
            Self::Time => &["k8s.io.apimachinery.pkg.apis.meta.v1.Time"],
            Self::MicroTime => &["k8s.io.apimachinery.pkg.apis.meta.v1.MicroTime"],
            Self::Quantity => &["k8s.io.apimachinery.pkg.api.resource.Quantity"],
            Self::IntOrString => &["k8s.io.apimachinery.pkg.util.intstr.IntOrString"],
            Self::RawExtension => &["k8s.io.apimachinery.pkg.runtime.RawExtension"],
            Self::FieldsV1 => &["k8s.io.apimachinery.pkg.apis.meta.v1.FieldsV1"],
            Self::StringList => &[
                "k8s.io.api.authentication.v1.ExtraValue",
                "k8s.io.api.authorization.v1.ExtraValue",
                "k8s.io.apimachinery.pkg.apis.meta.v1.Verbs",
            ],
        }
    }

    /// The special type a message descriptor names, if any.
    #[must_use]
    pub fn of(desc: &MessageDescriptor) -> Option<Self> {
        let name = desc.full_name();
        Self::ALL
            .into_iter()
            .find(|special| special.messages().contains(&name))
    }
}

/// Every Go struct embedded with `json:",inline"` in the vendored API groups:
/// its fields are the parent's in JSON, and a nested message on the wire.
/// `(parent message, proto field name)`.
///
/// The descriptors cannot say this (go-to-protobuf leaves no mark), so it is
/// the census of `json:",inline"` in upstream v1.34.0's `types.go` for
/// core/v1, apps/v1, rbac/v1, authorization/v1, authentication/v1,
/// coordination/v1 and meta/v1, `TypeMeta` excluded (the wrapper carries it).
/// Vendoring another group means re-taking it:
/// `awk '/^type [A-Za-z]+ struct/{s=$2} /json:",inline"/ && !/TypeMeta/{print s": "$0}' types.go`.
pub const INLINE: [(&str, &str); 11] = [
    ("k8s.io.api.core.v1.Volume", "volumeSource"),
    (
        "k8s.io.api.core.v1.PersistentVolumeSpec",
        "persistentVolumeSource",
    ),
    (
        "k8s.io.api.core.v1.SecretProjection",
        "localObjectReference",
    ),
    (
        "k8s.io.api.core.v1.ConfigMapVolumeSource",
        "localObjectReference",
    ),
    (
        "k8s.io.api.core.v1.ConfigMapProjection",
        "localObjectReference",
    ),
    (
        "k8s.io.api.core.v1.ConfigMapKeySelector",
        "localObjectReference",
    ),
    (
        "k8s.io.api.core.v1.SecretKeySelector",
        "localObjectReference",
    ),
    (
        "k8s.io.api.core.v1.ConfigMapEnvSource",
        "localObjectReference",
    ),
    ("k8s.io.api.core.v1.SecretEnvSource", "localObjectReference"),
    ("k8s.io.api.core.v1.Probe", "handler"),
    (
        "k8s.io.api.core.v1.EphemeralContainer",
        "ephemeralContainerCommon",
    ),
];

/// Whether a field is a Go inline embed ([`INLINE`]).
fn is_inline(field: &FieldDescriptor) -> bool {
    let parent = field.parent_message();
    INLINE
        .iter()
        .any(|(message, name)| parent.full_name() == *message && field.name() == *name)
}

/// The message an inline field embeds.
fn inline_message(
    field: &FieldDescriptor,
    at: &At<'_>,
) -> Result<MessageDescriptor, TranscodeError> {
    field.kind().as_message().cloned().ok_or_else(|| {
        at.problem(FieldProblem::Descriptor(
            "an inline embed that is not a message",
        ))
    })
}

/// Whether a JSON key names a field of `desc` once its inline embeds are
/// flattened into it.
fn accepts_key(desc: &MessageDescriptor, key: &str) -> bool {
    desc.fields().any(|field| {
        if is_inline(&field) {
            field
                .kind()
                .as_message()
                .is_some_and(|inner| accepts_key(inner, key))
        } else {
            field.json_name() == key
        }
    })
}

/// `IntOrString.type`: upstream's `intstr.Type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IntOrStringType {
    Int,
    String,
}

impl IntOrStringType {
    const fn wire(self) -> i64 {
        match self {
            Self::Int => 0,
            Self::String => 1,
        }
    }

    fn from_wire(value: i64) -> Option<Self> {
        [Self::Int, Self::String]
            .into_iter()
            .find(|t| t.wire() == value)
    }
}

/// How finely a time type renders.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Precision {
    Seconds,
    Micros,
}

// ── losses ──────────────────────────────────────────────────────────────────

/// A transcode the codec performs but cannot make exact. Closed: a new class
/// of loss is a new variant, and `tests/w3_protobuf_codec.rs` pins how many
/// there are.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KnownLossy {
    /// Encode: a JSON field the kind's message does not define. Dropped.
    UnknownJsonField,
    /// Encode: a `metav1.Time` with a fraction of a second. The wire carries
    /// whole seconds, as upstream's `Time.ProtoTime` writes them.
    TimeSubsecond,
    /// Decode: a field number the vendored descriptors do not define.
    /// Dropped, as an upstream v1.34 apiserver drops it.
    UnknownWireField,
}

impl KnownLossy {
    /// Every class of loss.
    pub const ALL: [Self; 3] = [
        Self::UnknownJsonField,
        Self::TimeSubsecond,
        Self::UnknownWireField,
    ];
}

impl fmt::Display for KnownLossy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnknownJsonField => {
                "a field the kind's protobuf message does not define was left out of the protobuf encoding"
            }
            Self::TimeSubsecond => {
                "a metav1.Time carried a fraction of a second, which its protobuf encoding drops"
            }
            Self::UnknownWireField => {
                "the protobuf body carried a field the server's v1.34 descriptors do not define; it was dropped"
            }
        })
    }
}

/// The classes of loss one transcode ran into.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Losses(BTreeSet<KnownLossy>);

impl Losses {
    fn insert(&mut self, loss: KnownLossy) {
        self.0.insert(loss);
    }

    /// No loss: the transcode was exact.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether this class of loss happened.
    #[must_use]
    pub fn contains(&self, loss: KnownLossy) -> bool {
        self.0.contains(&loss)
    }

    /// The classes that happened, in a stable order.
    pub fn iter(&self) -> impl Iterator<Item = KnownLossy> + '_ {
        self.0.iter().copied()
    }
}

// ── errors ──────────────────────────────────────────────────────────────────

/// What a JSON value was expected to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expected {
    Object,
    Array,
    Boolean,
    String,
    Base64,
    Number,
    Int32,
    Int64,
    Uint32,
    Uint64,
    EnumValue,
    Time,
    MicroTime,
    Quantity,
    IntOrString,
}

impl fmt::Display for Expected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Object => "an object",
            Self::Array => "an array",
            Self::Boolean => "a boolean",
            Self::String => "a string",
            Self::Base64 => "a base64 string",
            Self::Number => "a number",
            Self::Int32 => "a 32-bit integer",
            Self::Int64 => "a 64-bit integer",
            Self::Uint32 => "an unsigned 32-bit integer",
            Self::Uint64 => "an unsigned 64-bit integer",
            Self::EnumValue => "an enum value name or number",
            Self::Time => "an RFC 3339 time",
            Self::MicroTime => "an RFC 3339 time with microseconds",
            Self::Quantity => "a quantity string or number",
            Self::IntOrString => "an integer or a string",
        })
    }
}

/// What is wrong at one field.
#[derive(Debug, thiserror::Error)]
pub enum FieldProblem {
    /// The JSON value has the wrong kind.
    #[error("expected {expected}, found {found}")]
    Shape { expected: Expected, found: JsonKind },
    /// A number that is not an integer where an integer belongs.
    #[error("expected {0}, found a number with a fraction")]
    NotAnInteger(Expected),
    /// An integer outside the field's range, or a string that is not one.
    #[error("value does not fit {0}")]
    OutOfRange(Expected),
    /// A NaN or an infinity, which JSON cannot carry.
    #[error("a non-finite floating-point value has no JSON form")]
    NonFinite,
    /// Bytes that are not base64.
    #[error("invalid base64: {0}")]
    Base64(#[source] base64::DecodeError),
    /// A time that is not RFC 3339.
    #[error("invalid time: {0}")]
    Time(#[source] chrono::ParseError),
    /// A time outside what a timestamp can hold.
    #[error("time out of range: {seconds}s {nanos}ns")]
    TimeRange { seconds: i64, nanos: i64 },
    /// An embedded JSON document (`RawExtension`, `FieldsV1`) that is not JSON.
    #[error("embedded document is not JSON: {0}")]
    NotJson(#[source] serde_json::Error),
    /// An `IntOrString.type` other than int (0) or string (1). Upstream's
    /// `MarshalJSON` fails the same way.
    #[error("impossible IntOrString type {0}")]
    IntOrStringType(i64),
    /// An enum value name the enum does not define.
    #[error("unknown enum value {0:?}")]
    UnknownEnumValue(String),
    /// A map key that does not parse as the map's key type.
    #[error("map key {0:?} does not parse as the map's key type")]
    MapKey(String),
    /// A repeated value where the descriptor admits only one.
    #[error("a list or map where a single value belongs")]
    NestedRepeated,
    /// The descriptor and the transcoder disagree: a build or vendoring defect.
    #[error("descriptor defect: {0}")]
    Descriptor(&'static str),
}

/// Why a body could not be transcoded.
#[derive(Debug, thiserror::Error)]
pub enum TranscodeError {
    /// The 4-byte `k8s\0` prefix is missing.
    #[error("not a kubernetes protobuf payload: bad magic (expected k8s\\0)")]
    BadMagic,
    /// The `runtime.Unknown` wrapper does not decode.
    #[error("malformed runtime.Unknown wrapper: {0}")]
    BadWrapper(#[source] prost::DecodeError),
    /// The wrapper names no `apiVersion` or no `kind`.
    #[error("protobuf payload is missing TypeMeta (apiVersion and kind)")]
    MissingTypeMeta,
    /// The wrapper declares a content encoding; upstream never sends one.
    #[error("runtime.Unknown contentEncoding {0:?} is not supported")]
    ContentEncoding(String),
    /// The wrapper declares an inner content type that is neither protobuf
    /// nor JSON.
    #[error("runtime.Unknown contentType {0:?} is not supported")]
    InnerContentType(String),
    /// The wrapper declares a JSON body that is not JSON.
    #[error("runtime.Unknown declared a JSON body that is not valid JSON: {0}")]
    InnerJson(#[source] serde_json::Error),
    /// No vendored message for this kind: the protobuf form is not served.
    #[error("no protobuf serialization for apiVersion={api_version:?} kind={kind:?}")]
    Uncataloged { api_version: String, kind: String },
    /// A message the transcoder requires is absent from the pool.
    #[error("descriptor pool is missing message {0}")]
    MissingDescriptor(&'static str),
    /// A field the transcoder sets is absent from its message, or of another type.
    #[error("descriptor pool message {message} has no usable field {field}")]
    MissingField {
        message: String,
        field: &'static str,
    },
    /// The object's bytes do not decode against the kind's message.
    #[error("{kind}: undecodable protobuf object: {source}")]
    Decode {
        kind: String,
        #[source]
        source: prost::DecodeError,
    },
    /// The wrapper does not encode.
    #[error("protobuf encoding failed: {0}")]
    Encode(#[source] prost::EncodeError),
    /// One field does not transcode.
    #[error("{path}: {problem}")]
    Field {
        path: FieldPath,
        problem: FieldProblem,
    },
}

impl TranscodeError {
    /// The HTTP answer for a REQUEST body that does not decode: a kind with
    /// no protobuf form is upstream's 415 (as for a custom resource), and
    /// anything else is a malformed request.
    #[must_use]
    pub fn into_request_error(self) -> ApiError {
        match self {
            Self::Uncataloged { .. } => ApiError::UnsupportedMediaType(self.to_string()),
            Self::BadMagic
            | Self::BadWrapper(_)
            | Self::MissingTypeMeta
            | Self::ContentEncoding(_)
            | Self::InnerContentType(_)
            | Self::InnerJson(_)
            | Self::Decode { .. }
            | Self::Field { .. } => ApiError::BadRequest(self.to_string()),
            Self::MissingDescriptor(_) | Self::MissingField { .. } | Self::Encode(_) => {
                ApiError::Internal(self.to_string())
            }
        }
    }

    /// The HTTP answer for a RESPONSE that cannot be served as protobuf to a
    /// client whose `Accept` names nothing else: a kind with no protobuf form
    /// is 406, and a stored object that does not encode is the server's fault.
    #[must_use]
    pub fn into_response_error(self) -> ApiError {
        match self {
            Self::Uncataloged { .. } => ApiError::NotAcceptable(self.to_string()),
            Self::BadMagic
            | Self::BadWrapper(_)
            | Self::MissingTypeMeta
            | Self::ContentEncoding(_)
            | Self::InnerContentType(_)
            | Self::InnerJson(_)
            | Self::Decode { .. }
            | Self::Field { .. }
            | Self::MissingDescriptor(_)
            | Self::MissingField { .. }
            | Self::Encode(_) => ApiError::Internal(self.to_string()),
        }
    }
}

// ── the path to a field, materialized only on error ─────────────────────────

/// Where the walk is: a stack-linked path, so the happy path allocates none.
#[derive(Clone, Copy)]
enum At<'a> {
    Root,
    Field(&'a At<'a>, &'a str),
    Key(&'a At<'a>, &'a str),
    Index(&'a At<'a>, usize),
}

impl<'a> At<'a> {
    fn field(&'a self, name: &'a str) -> Self {
        At::Field(self, name)
    }

    fn key(&'a self, key: &'a str) -> Self {
        At::Key(self, key)
    }

    fn index(&'a self, index: usize) -> Self {
        At::Index(self, index)
    }

    fn path(&self) -> FieldPath {
        match self {
            At::Root => FieldPath::default(),
            At::Field(parent, name) => parent.path().named(name),
            At::Key(parent, key) => parent.path().key(key),
            At::Index(parent, index) => parent.path().index(*index),
        }
    }

    fn problem(&self, problem: FieldProblem) -> TranscodeError {
        TranscodeError::Field {
            path: self.path(),
            problem,
        }
    }

    fn shape(&self, expected: Expected, found: &Value) -> TranscodeError {
        self.problem(FieldProblem::Shape {
            expected,
            found: JsonKind::of(found),
        })
    }
}

/// Whether a message is an object whose JSON carries `TypeMeta` inline: the
/// top-level kind, or an item of a top-level list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Level {
    Object,
    Nested,
}

// ── decode: protobuf -> JSON ────────────────────────────────────────────────

/// A decoded protobuf request body.
#[derive(Debug, Clone, PartialEq)]
pub struct Decoded {
    /// The object as upstream's JSON renders it, `apiVersion` and `kind`
    /// included.
    pub value: Value,
    /// What the transcode could not carry.
    pub losses: Losses,
}

/// Decode a `k8s\0`-framed protobuf body into the object's Kubernetes JSON.
///
/// # Errors
///
/// A typed [`TranscodeError`] for a bad frame, a kind with no descriptor, an
/// undecodable object or a field that has no JSON form.
pub fn decode(bytes: &[u8]) -> Result<Decoded, TranscodeError> {
    let body = bytes
        .strip_prefix(PROTOBUF_MAGIC.as_slice())
        .ok_or(TranscodeError::BadMagic)?;
    let pool = pool()?;
    let unknown_desc = pool
        .get_message_by_name(UNKNOWN)
        .ok_or(TranscodeError::MissingDescriptor(UNKNOWN))?;
    let unknown = DynamicMessage::decode(unknown_desc, body).map_err(TranscodeError::BadWrapper)?;

    let gvk = frame_type_meta(&unknown)?;
    let encoding = string_field(&unknown, "contentEncoding").unwrap_or_default();
    if !encoding.is_empty() {
        return Err(TranscodeError::ContentEncoding(encoding));
    }
    let raw = bytes_field(&unknown, "raw").unwrap_or_default();
    let inner = string_field(&unknown, "contentType").unwrap_or_default();
    let inner_media = inner.split(';').next().unwrap_or_default().trim();

    if inner_media.eq_ignore_ascii_case(CONTENT_TYPE_JSON) {
        // The wrapper says the object inside is literally JSON.
        let value = serde_json::from_slice(&raw).map_err(TranscodeError::InnerJson)?;
        return Ok(Decoded {
            value,
            losses: Losses::default(),
        });
    }
    if !(inner_media.is_empty() || inner_media.eq_ignore_ascii_case(CONTENT_TYPE_PROTOBUF)) {
        return Err(TranscodeError::InnerContentType(inner));
    }

    let desc = descriptor_for(&gvk)?;
    let object =
        DynamicMessage::decode(desc, raw.as_ref()).map_err(|source| TranscodeError::Decode {
            kind: gvk.kind.clone(),
            source,
        })?;
    let mut losses = Losses::default();
    let mut value = message_to_json(&object, &At::Root, &mut losses)?;
    if let Value::Object(map) = &mut value {
        map.insert("apiVersion".to_owned(), Value::String(gvk.api_version));
        map.insert("kind".to_owned(), Value::String(gvk.kind));
    }
    Ok(Decoded { value, losses })
}

fn message_to_json(
    msg: &DynamicMessage,
    at: &At<'_>,
    losses: &mut Losses,
) -> Result<Value, TranscodeError> {
    if msg.unknown_fields().next().is_some() {
        losses.insert(KnownLossy::UnknownWireField);
    }
    if let Some(special) = Special::of(&msg.descriptor()) {
        return special_to_json(special, msg, at);
    }
    let mut out = Map::new();
    let mut inlined = Vec::new();
    for (field, value) in msg.fields() {
        if is_inline(&field) {
            // Flattened after the parent's own fields: in Go's JSON the
            // shallower field wins a name both declare.
            let Wire::Message(inner) = value else {
                return Err(at.problem(FieldProblem::Descriptor(
                    "an inline embed whose value is not a message",
                )));
            };
            let Value::Object(fields) = message_to_json(inner, at, losses)? else {
                return Err(at.problem(FieldProblem::Descriptor(
                    "an inline embed whose JSON is not an object",
                )));
            };
            inlined.push(fields);
            continue;
        }
        let name = field.json_name();
        let at = at.field(name);
        let json = match value {
            Wire::List(items) => {
                let kind = field.kind();
                let mut array = Vec::with_capacity(items.len());
                for (i, item) in items.iter().enumerate() {
                    array.push(single_to_json(&kind, item, &at.index(i), losses)?);
                }
                Value::Array(array)
            }
            Wire::Map(entries) => map_to_json(&field, entries, &at, losses)?,
            single @ (Wire::Bool(_)
            | Wire::I32(_)
            | Wire::I64(_)
            | Wire::U32(_)
            | Wire::U64(_)
            | Wire::F32(_)
            | Wire::F64(_)
            | Wire::String(_)
            | Wire::Bytes(_)
            | Wire::EnumNumber(_)
            | Wire::Message(_)) => single_to_json(&field.kind(), single, &at, losses)?,
        };
        out.insert(name.to_owned(), json);
    }
    for fields in inlined {
        for (name, json) in fields {
            out.entry(name).or_insert(json);
        }
    }
    Ok(Value::Object(out))
}

fn map_to_json(
    field: &FieldDescriptor,
    entries: &HashMap<MapKey, Wire>,
    at: &At<'_>,
    losses: &mut Losses,
) -> Result<Value, TranscodeError> {
    let value_kind = field
        .kind()
        .as_message()
        .map(|entry| entry.map_entry_value_field().kind())
        .ok_or_else(|| {
            at.problem(FieldProblem::Descriptor(
                "a map field whose kind is not a map entry",
            ))
        })?;
    let mut out = Map::new();
    for (key, value) in entries {
        let key = map_key_text(key);
        let json = single_to_json(&value_kind, value, &at.key(&key), losses)?;
        out.insert(key.into_owned(), json);
    }
    Ok(Value::Object(out))
}

fn map_key_text(key: &MapKey) -> Cow<'_, str> {
    match key {
        MapKey::Bool(b) => Cow::Owned(b.to_string()),
        MapKey::I32(n) => Cow::Owned(n.to_string()),
        MapKey::I64(n) => Cow::Owned(n.to_string()),
        MapKey::U32(n) => Cow::Owned(n.to_string()),
        MapKey::U64(n) => Cow::Owned(n.to_string()),
        MapKey::String(s) => Cow::Borrowed(s),
    }
}

fn single_to_json(
    kind: &Kind,
    value: &Wire,
    at: &At<'_>,
    losses: &mut Losses,
) -> Result<Value, TranscodeError> {
    match value {
        Wire::Bool(b) => Ok(Value::Bool(*b)),
        Wire::I32(n) => Ok(Value::from(*n)),
        Wire::I64(n) => Ok(Value::from(*n)),
        Wire::U32(n) => Ok(Value::from(*n)),
        Wire::U64(n) => Ok(Value::from(*n)),
        Wire::F32(x) => finite(f64::from(*x), at),
        Wire::F64(x) => finite(*x, at),
        Wire::String(s) => Ok(Value::String(s.clone())),
        Wire::Bytes(b) => Ok(Value::String(BASE64.encode(b))),
        Wire::EnumNumber(n) => Ok(enum_to_json(kind.as_enum(), *n)),
        Wire::Message(m) => message_to_json(m, at, losses),
        Wire::List(_) | Wire::Map(_) => Err(at.problem(FieldProblem::NestedRepeated)),
    }
}

fn finite(x: f64, at: &At<'_>) -> Result<Value, TranscodeError> {
    Number::from_f64(x)
        .map(Value::Number)
        .ok_or_else(|| at.problem(FieldProblem::NonFinite))
}

fn enum_to_json(desc: Option<&EnumDescriptor>, number: i32) -> Value {
    desc.and_then(|e| e.get_value(number))
        .map_or(Value::from(number), |v| Value::String(v.name().to_owned()))
}

fn special_to_json(
    special: Special,
    msg: &DynamicMessage,
    at: &At<'_>,
) -> Result<Value, TranscodeError> {
    match special {
        Special::Time => time_to_json(msg, Precision::Seconds, at),
        Special::MicroTime => time_to_json(msg, Precision::Micros, at),
        // A zero Quantity renders "0" (upstream `Quantity.MarshalJSON`).
        Special::Quantity => Ok(Value::String(
            string_field(msg, "string").unwrap_or_else(|| "0".to_owned()),
        )),
        Special::IntOrString => {
            // Absent fields are Go's zero values: type Int, intVal 0.
            let wire_type = i64_field(msg, "type").unwrap_or(0);
            match IntOrStringType::from_wire(wire_type) {
                Some(IntOrStringType::Int) => {
                    Ok(Value::from(i32_field(msg, "intVal").unwrap_or(0)))
                }
                Some(IntOrStringType::String) => Ok(Value::String(
                    string_field(msg, "strVal").unwrap_or_default(),
                )),
                None => Err(at.problem(FieldProblem::IntOrStringType(wire_type))),
            }
        }
        Special::RawExtension => embedded_to_json(msg, "raw", at),
        Special::FieldsV1 => embedded_to_json(msg, "Raw", at),
        // "items, if empty, will result in an empty slice" (the go-to-protobuf
        // comment on the masked type).
        Special::StringList => match present(msg, "items") {
            None => Ok(Value::Array(Vec::new())),
            Some(items) => match items.as_ref() {
                Wire::List(values) => values
                    .iter()
                    .enumerate()
                    .map(|(i, item)| {
                        item.as_str()
                            .map(|s| Value::String(s.to_owned()))
                            .ok_or_else(|| {
                                at.index(i).problem(FieldProblem::Descriptor(
                                    "a masked []string whose item is not a string",
                                ))
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(Value::Array),
                Wire::Bool(_)
                | Wire::I32(_)
                | Wire::I64(_)
                | Wire::U32(_)
                | Wire::U64(_)
                | Wire::F32(_)
                | Wire::F64(_)
                | Wire::String(_)
                | Wire::Bytes(_)
                | Wire::EnumNumber(_)
                | Wire::Message(_)
                | Wire::Map(_) => Err(at.problem(FieldProblem::Descriptor(
                    "a masked []string whose items field is not repeated",
                ))),
            },
        },
    }
}

/// `Time.Unmarshal` / `MicroTime.Unmarshal`: no bytes is the zero time, whose
/// JSON is `null`; `Time` keeps whole seconds; `MicroTime` truncates to the
/// microsecond.
fn time_to_json(
    msg: &DynamicMessage,
    precision: Precision,
    at: &At<'_>,
) -> Result<Value, TranscodeError> {
    if msg.fields().next().is_none() {
        return Ok(Value::Null);
    }
    let seconds = i64_field(msg, "seconds").unwrap_or(0);
    let nanos = match precision {
        Precision::Seconds => 0,
        Precision::Micros => {
            let nanos = i32_field(msg, "nanos").unwrap_or(0);
            nanos - nanos % 1_000
        }
    };
    if seconds == GO_ZERO_TIME_UNIX && nanos == 0 {
        return Ok(Value::Null);
    }
    let out_of_range = || {
        at.problem(FieldProblem::TimeRange {
            seconds,
            nanos: i64::from(nanos),
        })
    };
    let nanos = u32::try_from(nanos).map_err(|_| out_of_range())?;
    let instant = DateTime::from_timestamp(seconds, nanos)
        .filter(|_| nanos < 1_000_000_000)
        .ok_or_else(out_of_range)?;
    Ok(Value::String(match precision {
        Precision::Seconds => engenho_types::time::to_rfc3339_utc(instant),
        Precision::Micros => engenho_types::time::to_micro_time_utc(instant),
    }))
}

/// `RawExtension.MarshalJSON` / `FieldsV1.MarshalJSON`: no bytes is `null`,
/// and bytes are the JSON document itself.
fn embedded_to_json(
    msg: &DynamicMessage,
    field: &str,
    at: &At<'_>,
) -> Result<Value, TranscodeError> {
    match bytes_field(msg, field) {
        None => Ok(Value::Null),
        Some(raw) => serde_json::from_slice(&raw).map_err(|e| at.problem(FieldProblem::NotJson(e))),
    }
}

// ── encode: JSON -> protobuf ────────────────────────────────────────────────

/// An encoded protobuf response body.
#[derive(Debug, Clone, PartialEq)]
pub struct Encoded {
    /// `k8s\0` + the `runtime.Unknown` frame.
    pub bytes: Bytes,
    /// What the transcode could not carry.
    pub losses: Losses,
}

/// Encode an object's Kubernetes JSON as the `k8s\0`-framed protobuf body of
/// `gvk`. A list is encoded under its `<Kind>List` GVK.
///
/// # Errors
///
/// [`TranscodeError::Uncataloged`] when `gvk` has no vendored message, or a
/// typed field error when the value does not fit the message.
pub fn encode(gvk: &Gvk, value: &Value) -> Result<Encoded, TranscodeError> {
    let desc = descriptor_for(gvk)?;
    let mut losses = Losses::default();
    let object = json_to_message(&desc, value, Level::Object, &At::Root, &mut losses)?;
    let pool = desc.parent_pool();

    let mut type_meta = DynamicMessage::new(
        pool.get_message_by_name(TYPE_META)
            .ok_or(TranscodeError::MissingDescriptor(TYPE_META))?,
    );
    set_named(
        &mut type_meta,
        "apiVersion",
        Wire::String(gvk.api_version.clone()),
    )?;
    set_named(&mut type_meta, "kind", Wire::String(gvk.kind.clone()))?;

    let mut unknown = DynamicMessage::new(
        pool.get_message_by_name(UNKNOWN)
            .ok_or(TranscodeError::MissingDescriptor(UNKNOWN))?,
    );
    set_named(&mut unknown, "typeMeta", Wire::Message(type_meta))?;
    set_named(
        &mut unknown,
        "raw",
        Wire::Bytes(Bytes::from(object.encode_to_vec())),
    )?;

    let mut out = Vec::with_capacity(PROTOBUF_MAGIC.len() + unknown.encoded_len());
    out.extend_from_slice(&PROTOBUF_MAGIC);
    unknown.encode(&mut out).map_err(TranscodeError::Encode)?;
    Ok(Encoded {
        bytes: Bytes::from(out),
        losses,
    })
}

fn json_to_message(
    desc: &MessageDescriptor,
    value: &Value,
    level: Level,
    at: &At<'_>,
    losses: &mut Losses,
) -> Result<DynamicMessage, TranscodeError> {
    if let Some(special) = Special::of(desc) {
        return special_from_json(special, desc, value, at, losses);
    }
    let Value::Object(object) = value else {
        return Err(at.shape(Expected::Object, value));
    };
    let items_are_objects = level == Level::Object && is_list_envelope(desc);
    let inline_fields: Vec<FieldDescriptor> = desc.fields().filter(is_inline).collect();
    let mut inline_objects: Vec<Map<String, Value>> = vec![Map::new(); inline_fields.len()];
    let mut msg = DynamicMessage::new(desc.clone());
    for (key, item) in object {
        let own = desc
            .get_field_by_json_name(key)
            .filter(|field| !is_inline(field));
        let Some(field) = own else {
            // A key of an inline embed's struct travels in that embed.
            if let Some(slot) = inline_fields.iter().position(|field| {
                field
                    .kind()
                    .as_message()
                    .is_some_and(|inner| accepts_key(inner, key))
            }) {
                inline_objects[slot].insert(key.clone(), item.clone());
            } else if level == Level::Nested || !TYPE_META_KEYS.contains(&key.as_str()) {
                losses.insert(KnownLossy::UnknownJsonField);
            }
            continue;
        };
        // `null` is Go's nil: the field is unset.
        if item.is_null() {
            continue;
        }
        let at = at.field(key);
        let child = if items_are_objects && field.json_name() == "items" {
            Level::Object
        } else {
            Level::Nested
        };
        let wire = json_to_field(&field, item, child, &at, losses)?;
        msg.try_set_field(&field, wire).map_err(|_| {
            at.problem(FieldProblem::Descriptor(
                "a value of the wrong type for its field",
            ))
        })?;
    }
    for (field, fields) in inline_fields.iter().zip(inline_objects) {
        if fields.is_empty() {
            continue;
        }
        let inner = json_to_message(
            &inline_message(field, at)?,
            &Value::Object(fields),
            Level::Nested,
            at,
            losses,
        )?;
        msg.try_set_field(field, Wire::Message(inner))
            .map_err(|_| {
                at.problem(FieldProblem::Descriptor(
                    "an inline embed of the wrong type",
                ))
            })?;
    }
    Ok(msg)
}

/// A `<Kind>List`: `metadata` is a `ListMeta` and `items` are objects.
fn is_list_envelope(desc: &MessageDescriptor) -> bool {
    desc.get_field_by_json_name("metadata")
        .and_then(|f| f.kind().as_message().map(|m| m.name() == "ListMeta"))
        .unwrap_or(false)
        && desc
            .get_field_by_json_name("items")
            .is_some_and(|f| f.is_list())
}

fn json_to_field(
    field: &FieldDescriptor,
    value: &Value,
    level: Level,
    at: &At<'_>,
    losses: &mut Losses,
) -> Result<Wire, TranscodeError> {
    if field.is_map() {
        let Value::Object(entries) = value else {
            return Err(at.shape(Expected::Object, value));
        };
        let entry = field.kind().as_message().cloned().ok_or_else(|| {
            at.problem(FieldProblem::Descriptor(
                "a map field whose kind is not a map entry",
            ))
        })?;
        let key_field = entry.map_entry_key_field();
        let value_field = entry.map_entry_value_field();
        let mut out = HashMap::with_capacity(entries.len());
        for (key, item) in entries {
            let at = at.key(key);
            let wire_key = map_key_from_text(&key_field.kind(), key, &at)?;
            // A null map entry is Go's zero value for the element.
            let wire_value = if item.is_null() {
                Wire::default_value_for_field(&value_field)
            } else {
                json_to_single(&value_field.kind(), item, Level::Nested, &at, losses)?
            };
            out.insert(wire_key, wire_value);
        }
        Ok(Wire::Map(out))
    } else if field.is_list() {
        let Value::Array(items) = value else {
            return Err(at.shape(Expected::Array, value));
        };
        let kind = field.kind();
        let mut out = Vec::with_capacity(items.len());
        for (i, item) in items.iter().enumerate() {
            // A null element is Go's zero value for the element.
            out.push(if item.is_null() {
                Wire::default_value(&kind)
            } else {
                json_to_single(&kind, item, level, &at.index(i), losses)?
            });
        }
        Ok(Wire::List(out))
    } else {
        json_to_single(&field.kind(), value, level, at, losses)
    }
}

fn map_key_from_text(kind: &Kind, text: &str, at: &At<'_>) -> Result<MapKey, TranscodeError> {
    let bad = || at.problem(FieldProblem::MapKey(text.to_owned()));
    match kind {
        Kind::String => Ok(MapKey::String(text.to_owned())),
        Kind::Bool => text.parse().map(MapKey::Bool).map_err(|_| bad()),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => {
            text.parse().map(MapKey::I32).map_err(|_| bad())
        }
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => {
            text.parse().map(MapKey::I64).map_err(|_| bad())
        }
        Kind::Uint32 | Kind::Fixed32 => text.parse().map(MapKey::U32).map_err(|_| bad()),
        Kind::Uint64 | Kind::Fixed64 => text.parse().map(MapKey::U64).map_err(|_| bad()),
        Kind::Double | Kind::Float | Kind::Bytes | Kind::Message(_) | Kind::Enum(_) => Err(at
            .problem(FieldProblem::Descriptor(
                "a map key kind protobuf does not allow",
            ))),
    }
}

fn json_to_single(
    kind: &Kind,
    value: &Value,
    level: Level,
    at: &At<'_>,
    losses: &mut Losses,
) -> Result<Wire, TranscodeError> {
    match kind {
        Kind::Double => float(value, at).map(Wire::F64),
        Kind::Float => float(value, at)
            .and_then(|x| single_precision(x, at))
            .map(Wire::F32),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => {
            integer(value, Expected::Int32, at).map(Wire::I32)
        }
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => {
            integer(value, Expected::Int64, at).map(Wire::I64)
        }
        Kind::Uint32 | Kind::Fixed32 => integer(value, Expected::Uint32, at).map(Wire::U32),
        Kind::Uint64 | Kind::Fixed64 => integer(value, Expected::Uint64, at).map(Wire::U64),
        Kind::Bool => value
            .as_bool()
            .map(Wire::Bool)
            .ok_or_else(|| at.shape(Expected::Boolean, value)),
        Kind::String => value
            .as_str()
            .map(|s| Wire::String(s.to_owned()))
            .ok_or_else(|| at.shape(Expected::String, value)),
        Kind::Bytes => {
            let text = value
                .as_str()
                .ok_or_else(|| at.shape(Expected::Base64, value))?;
            BASE64
                .decode(text)
                .map(|b| Wire::Bytes(Bytes::from(b)))
                .map_err(|e| at.problem(FieldProblem::Base64(e)))
        }
        Kind::Enum(desc) => enum_from_json(desc, value, at),
        Kind::Message(desc) => json_to_message(desc, value, level, at, losses).map(Wire::Message),
    }
}

fn float(value: &Value, at: &At<'_>) -> Result<f64, TranscodeError> {
    match value {
        Value::Number(n) => n
            .as_f64()
            .ok_or_else(|| at.problem(FieldProblem::OutOfRange(Expected::Number))),
        Value::Null | Value::Bool(_) | Value::String(_) | Value::Array(_) | Value::Object(_) => {
            Err(at.shape(Expected::Number, value))
        }
    }
}

fn single_precision(x: f64, at: &At<'_>) -> Result<f32, TranscodeError> {
    if x.is_finite() && x.abs() <= f64::from(f32::MAX) {
        // In range by the check above; the narrowing only rounds.
        #[allow(clippy::cast_possible_truncation)]
        Ok(x as f32)
    } else {
        Err(at.problem(FieldProblem::OutOfRange(Expected::Number)))
    }
}

/// An integer field: a JSON integer, or a decimal string (the proto3 JSON
/// form, and the form the old codec stored every 64-bit integer in).
fn integer<T>(value: &Value, expected: Expected, at: &At<'_>) -> Result<T, TranscodeError>
where
    T: TryFrom<i64> + TryFrom<u64> + FromStr,
{
    match value {
        Value::Number(n) if n.is_f64() => Err(at.problem(FieldProblem::NotAnInteger(expected))),
        Value::Number(n) => n
            .as_i64()
            .and_then(|i| <T as TryFrom<i64>>::try_from(i).ok())
            .or_else(|| {
                n.as_u64()
                    .and_then(|u| <T as TryFrom<u64>>::try_from(u).ok())
            })
            .ok_or_else(|| at.problem(FieldProblem::OutOfRange(expected))),
        Value::String(s) => s
            .parse()
            .map_err(|_| at.problem(FieldProblem::OutOfRange(expected))),
        Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => {
            Err(at.shape(expected, value))
        }
    }
}

fn enum_from_json(
    desc: &EnumDescriptor,
    value: &Value,
    at: &At<'_>,
) -> Result<Wire, TranscodeError> {
    match value {
        Value::String(name) => desc
            .get_value_by_name(name)
            .map(|v| Wire::EnumNumber(v.number()))
            .ok_or_else(|| at.problem(FieldProblem::UnknownEnumValue(name.clone()))),
        Value::Number(_) => integer(value, Expected::EnumValue, at).map(Wire::EnumNumber),
        Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => {
            Err(at.shape(Expected::EnumValue, value))
        }
    }
}

fn special_from_json(
    special: Special,
    desc: &MessageDescriptor,
    value: &Value,
    at: &At<'_>,
    losses: &mut Losses,
) -> Result<DynamicMessage, TranscodeError> {
    let mut msg = DynamicMessage::new(desc.clone());
    match special {
        Special::Time => {
            // Upstream's `Time.ProtoTime` writes seconds and a zero nanos.
            if let Some((seconds, nanos)) = instant(value, Expected::Time, at)? {
                if nanos != 0 {
                    losses.insert(KnownLossy::TimeSubsecond);
                }
                set_named(&mut msg, "seconds", Wire::I64(seconds))?;
                set_named(&mut msg, "nanos", Wire::I32(0))?;
            }
        }
        Special::MicroTime => {
            // `MicroTime.ProtoMicroTime` truncates to the microsecond.
            if let Some((seconds, nanos)) = instant(value, Expected::MicroTime, at)? {
                let micros = i32::try_from(nanos - nanos % 1_000).map_err(|_| {
                    at.problem(FieldProblem::TimeRange {
                        seconds,
                        nanos: i64::from(nanos),
                    })
                })?;
                set_named(&mut msg, "seconds", Wire::I64(seconds))?;
                set_named(&mut msg, "nanos", Wire::I32(micros))?;
            }
        }
        Special::Quantity => {
            set_named(&mut msg, "string", Wire::String(quantity_text(value, at)?))?;
        }
        Special::IntOrString => {
            // Upstream writes all three fields, whichever arm is live.
            let (wire_type, int_val, str_val) = int_or_string(value, at)?;
            set_named(&mut msg, "type", Wire::I64(wire_type.wire()))?;
            set_named(&mut msg, "intVal", Wire::I32(int_val))?;
            set_named(&mut msg, "strVal", Wire::String(str_val))?;
        }
        Special::RawExtension => {
            set_named(&mut msg, "raw", Wire::Bytes(embedded_bytes(value, at)?))?;
        }
        Special::FieldsV1 => {
            set_named(&mut msg, "Raw", Wire::Bytes(embedded_bytes(value, at)?))?;
        }
        Special::StringList => {
            let Value::Array(items) = value else {
                return Err(at.shape(Expected::Array, value));
            };
            let mut out = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                let text = item
                    .as_str()
                    .ok_or_else(|| at.index(i).shape(Expected::String, item))?;
                out.push(Wire::String(text.to_owned()));
            }
            set_named(&mut msg, "items", Wire::List(out))?;
        }
    }
    Ok(msg)
}

/// A time as `(seconds, nanos)`, or `None` for the zero time (whose protobuf
/// is an empty message). Reads the RFC 3339 wire string, and the
/// `{seconds, nanos}` object the old codec stored.
fn instant(
    value: &Value,
    expected: Expected,
    at: &At<'_>,
) -> Result<Option<(i64, u32)>, TranscodeError> {
    let (seconds, nanos) = match value {
        Value::Null => return Ok(None),
        Value::String(text) => {
            let parsed = DateTime::parse_from_rfc3339(text)
                .map_err(|e| at.problem(FieldProblem::Time(e)))?;
            (parsed.timestamp(), parsed.timestamp_subsec_nanos())
        }
        Value::Object(legacy) => {
            if legacy.is_empty() {
                return Ok(None);
            }
            let seconds = match legacy.get("seconds") {
                None => 0,
                Some(v) => integer::<i64>(v, Expected::Int64, &at.field("seconds"))?,
            };
            let nanos = match legacy.get("nanos") {
                None => 0,
                Some(v) => integer::<u32>(v, Expected::Uint32, &at.field("nanos"))?,
            };
            (seconds, nanos)
        }
        Value::Bool(_) | Value::Number(_) | Value::Array(_) => {
            return Err(at.shape(expected, value));
        }
    };
    if nanos >= 1_000_000_000 {
        // A leap second, or a stored object's out-of-range nanos: a protobuf
        // Timestamp's nanos is below one second.
        return Err(at.problem(FieldProblem::TimeRange {
            seconds,
            nanos: i64::from(nanos),
        }));
    }
    if seconds == GO_ZERO_TIME_UNIX && nanos == 0 {
        return Ok(None);
    }
    Ok(Some((seconds, nanos)))
}

/// A quantity's text: the wire string, a JSON number's decimal text (which
/// upstream's `Quantity.UnmarshalJSON` also accepts), or the old codec's
/// `{"string": …}`.
fn quantity_text(value: &Value, at: &At<'_>) -> Result<String, TranscodeError> {
    match value {
        Value::String(text) => Ok(text.clone()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Object(legacy) => match legacy.get("string") {
            Some(Value::String(text)) => Ok(text.clone()),
            Some(
                other @ (Value::Null
                | Value::Bool(_)
                | Value::Number(_)
                | Value::Array(_)
                | Value::Object(_)),
            ) => Err(at.field("string").shape(Expected::String, other)),
            None => Err(at.shape(Expected::Quantity, value)),
        },
        Value::Null | Value::Bool(_) | Value::Array(_) => Err(at.shape(Expected::Quantity, value)),
    }
}

/// `IntOrString.UnmarshalJSON`: a JSON string is the string arm, anything
/// else the int arm. Also reads the old codec's three-field object.
fn int_or_string(
    value: &Value,
    at: &At<'_>,
) -> Result<(IntOrStringType, i32, String), TranscodeError> {
    match value {
        Value::String(text) => Ok((IntOrStringType::String, 0, text.clone())),
        Value::Number(_) => Ok((
            IntOrStringType::Int,
            integer(value, Expected::Int32, at)?,
            String::new(),
        )),
        Value::Object(legacy) => {
            let wire_type = match legacy.get("type") {
                None => 0,
                Some(v) => integer::<i64>(v, Expected::Int64, &at.field("type"))?,
            };
            let int_val = match legacy.get("intVal") {
                None => 0,
                Some(v) => integer::<i32>(v, Expected::Int32, &at.field("intVal"))?,
            };
            let str_val = match legacy.get("strVal") {
                None => String::new(),
                Some(v) => v
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| at.field("strVal").shape(Expected::String, v))?,
            };
            let wire_type = IntOrStringType::from_wire(wire_type)
                .ok_or_else(|| at.problem(FieldProblem::IntOrStringType(wire_type)))?;
            Ok((wire_type, int_val, str_val))
        }
        Value::Null | Value::Bool(_) | Value::Array(_) => {
            Err(at.shape(Expected::IntOrString, value))
        }
    }
}

/// `RawExtension.UnmarshalJSON` / `FieldsV1.UnmarshalJSON`: the raw bytes are
/// the JSON document.
fn embedded_bytes(value: &Value, at: &At<'_>) -> Result<Bytes, TranscodeError> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|e| at.problem(FieldProblem::NotJson(e)))
}

// ── descriptors and field access ────────────────────────────────────────────

/// The vendored descriptor pool. engenho-kube-proto keeps it private, but
/// every descriptor it hands out carries it, and `v1/Namespace` is in every
/// vendored set.
fn pool() -> Result<DescriptorPool, TranscodeError> {
    engenho_kube_proto::message_for_gvk(&Gvk::new("v1", "Namespace"))
        .map(|desc| desc.parent_pool().clone())
        .map_err(|_| TranscodeError::MissingDescriptor("k8s.io.api.core.v1.Namespace"))
}

/// The message for a GVK, or [`TranscodeError::Uncataloged`].
///
/// # Errors
///
/// [`TranscodeError::Uncataloged`] when no vendored message is the kind.
pub fn descriptor_for(gvk: &Gvk) -> Result<MessageDescriptor, TranscodeError> {
    engenho_kube_proto::message_for_gvk(gvk).map_err(|_| TranscodeError::Uncataloged {
        api_version: gvk.api_version.clone(),
        kind: gvk.kind.clone(),
    })
}

fn frame_type_meta(unknown: &DynamicMessage) -> Result<Gvk, TranscodeError> {
    let type_meta = unknown
        .get_field_by_name("typeMeta")
        .and_then(|v| v.as_message().cloned())
        .ok_or(TranscodeError::MissingTypeMeta)?;
    let api_version = string_field(&type_meta, "apiVersion").unwrap_or_default();
    let kind = string_field(&type_meta, "kind").unwrap_or_default();
    if api_version.is_empty() || kind.is_empty() {
        return Err(TranscodeError::MissingTypeMeta);
    }
    Ok(Gvk::new(api_version, kind))
}

fn present<'m>(msg: &'m DynamicMessage, name: &str) -> Option<Cow<'m, Wire>> {
    if msg.has_field_by_name(name) {
        msg.get_field_by_name(name)
    } else {
        None
    }
}

fn string_field(msg: &DynamicMessage, name: &str) -> Option<String> {
    present(msg, name).and_then(|v| v.as_str().map(str::to_owned))
}

fn bytes_field(msg: &DynamicMessage, name: &str) -> Option<Bytes> {
    present(msg, name).and_then(|v| v.as_bytes().cloned())
}

fn i64_field(msg: &DynamicMessage, name: &str) -> Option<i64> {
    present(msg, name).and_then(|v| v.as_i64())
}

fn i32_field(msg: &DynamicMessage, name: &str) -> Option<i32> {
    present(msg, name).and_then(|v| v.as_i32())
}

fn set_named(
    msg: &mut DynamicMessage,
    field: &'static str,
    value: Wire,
) -> Result<(), TranscodeError> {
    let message = msg.descriptor();
    let missing = || TranscodeError::MissingField {
        message: message.full_name().to_owned(),
        field,
    };
    let desc = message.get_field_by_name(field).ok_or_else(missing)?;
    msg.try_set_field(&desc, value).map_err(|_| missing())
}
