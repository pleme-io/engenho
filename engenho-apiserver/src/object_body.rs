//! The body of a write that IS the stored object — POST and PUT, never PATCH.
//!
//! Upstream never stores the bytes a client sent. It decodes them into a Go
//! struct and stores that struct, so a JSON `null` in `metadata.labels` comes
//! out the other side as a nil map, which serializes as absent. engenho stores
//! the decoded `serde_json::Value` as it arrived, so a `labels: null` was
//! stored as `null` — and every controller that later read labels as a map had
//! to cope with a value upstream can never produce (T4.3 is the crash that
//! came from it).
//!
//! [`ObjectBody::normalize`] is the part of that decode engenho needs: the
//! [`ObjectMeta`](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#ObjectMeta)
//! fields controllers read, at the root and inside the pod template of the
//! built-in kinds that embed one.
//!
//! * `labels`, `annotations`, `ownerReferences`, `finalizers` that are `null`
//!   are dropped (a nil map or slice);
//! * a `labels`/`annotations` ENTRY that is `null` becomes `""` —
//!   `encoding/json` leaves a `map[string]string` element it cannot fill at
//!   the element's zero value, so upstream stores `{"a": ""}`, not `{}`;
//! * a `metadata` that is `null` is dropped (an embedded struct left at its
//!   zero value);
//! * anything of the wrong JSON kind — a `metadata` that is not an object, a
//!   `labels` that is not a map, a label value that is a number, an
//!   `ownerReferences` that is not an array of objects, a `finalizers` that is
//!   not an array of strings — is refused with a [`MetaShapeError`].
//!
//! ★ WHY NEVER A PATCH BODY. In a merge patch, `null` is not "absent", it is
//! "delete this": `{"metadata":{"labels":{"x":null}}}` removes label `x`, and
//! `{"metadata":{"labels":null}}` removes every label. Normalizing a patch body
//! would turn both into no-ops (or, for the entry, into setting `x` to `""`).
//! So the router builds an `ObjectBody` only from a POST or PUT body, and
//! `ResourceHandler::create`/`replace` take this type while `patch` takes a
//! raw `Value`.
//!
//! Tier, stated plainly:
//! * an un-normalized body reaching `create` or `replace` is
//!   **unrepresentable** — the only constructor normalizes;
//! * a patch body being normalized is only **caught** — nothing in the type
//!   system stops a caller normalizing a patch `Value` and unwrapping it again;
//!   `tests/t4_4_normalize_at_the_border.rs` pins that merge-patch `null`
//!   still deletes;
//! * a mutating webhook's body is re-normalized after admission (upstream
//!   decodes the patched object the same way), so an admitted body is covered
//!   too — by the handler calling `normalize` again, not by a type;
//! * since the one write pipeline (T4.5, `handler/write_plan.rs`), the object
//!   a PATCH or server-side apply would STORE — the merged candidate, not the
//!   patch — is normalized too, so no write through the apiserver stores the
//!   nulls. Objects stored before these landed, and objects controllers write
//!   straight to the store, can still carry them: readers still need T4.3's
//!   total access.
//!
//! Status codes follow upstream, not a guess: a body upstream cannot decode is
//! `transformDecodeError` → `errors.NewBadRequest` (400,
//! `k8s.io/apiserver/pkg/endpoints/handlers/rest.go`), and a webhook-patched
//! object that does not decode is `apierrors.NewInternalError` (500,
//! `admission/plugin/webhook/mutating/dispatcher.go`).

use std::borrow::Cow;
use std::fmt;

use serde_json::{Map, Value};

use crate::error::ApiError;

/// A POST or PUT body after border normalization: the object that will be
/// stored.
///
/// The only constructor is [`ObjectBody::normalize`], so holding one means the
/// body passed it. `ResourceHandler::create` and `replace` take this type.
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectBody(Value);

impl ObjectBody {
    /// Normalize `body`, a POST or PUT body for `group`/`kind` (see the module
    /// docs for what changes and what is refused). Idempotent: normalizing an
    /// `ObjectBody`'s value again returns the same value.
    ///
    /// # Errors
    ///
    /// A [`MetaShapeError`] naming the first field of the wrong JSON kind.
    pub fn normalize(group: &str, kind: &str, body: Value) -> Result<Self, MetaShapeError> {
        let mut body = body;
        let Value::Object(root) = &mut body else {
            return Err(MetaShapeError {
                path: FieldPath::default(),
                expected: JsonKind::Object,
                found: JsonKind::of(&body),
            });
        };
        normalize_meta_slot(root, &[])?;
        for parent in embedded_meta_parents(group, kind) {
            if let Some(container) = object_at(root, parent) {
                normalize_meta_slot(container, parent)?;
            }
        }
        Ok(Self(body))
    }

    /// The normalized body.
    #[must_use]
    pub fn as_value(&self) -> &Value {
        &self.0
    }

    /// The normalized body, by value.
    #[must_use]
    pub fn into_value(self) -> Value {
        self.0
    }
}

/// A field of the wrong JSON kind inside an `ObjectMeta`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{path}: expected {expected}, got {found}")]
pub struct MetaShapeError {
    /// Where the field is, in upstream's field-path syntax.
    pub path: FieldPath,
    /// The JSON kind the field must have.
    pub expected: JsonKind,
    /// The JSON kind it had.
    pub found: JsonKind,
}

impl MetaShapeError {
    /// The client sent this body: upstream's 400 for a body it cannot decode.
    #[must_use]
    pub fn request_error(&self, version: &str, kind: &str) -> ApiError {
        ApiError::BadRequest(
            Undecodable {
                version,
                kind,
                error: self,
            }
            .to_string(),
        )
    }

    /// A mutating webhook produced this body: upstream's 500 for a patched
    /// object it cannot decode. The client's request was fine.
    #[must_use]
    pub fn admission_error(&self, version: &str, kind: &str) -> ApiError {
        ApiError::Internal(
            Undecodable {
                version,
                kind,
                error: self,
            }
            .to_string(),
        )
    }
}

/// Upstream's `transformDecodeError` sentence, around why a body could not
/// be decoded: a [`MetaShapeError`], or a strict decoding error
/// ([`crate::field_validation`]).
pub(crate) struct Undecodable<'a> {
    pub(crate) version: &'a str,
    pub(crate) kind: &'a str,
    pub(crate) error: &'a dyn fmt::Display,
}

impl fmt::Display for Undecodable<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{kind} in version \"{version}\" cannot be handled as a {kind}: {error}",
            kind = self.kind,
            version = self.version,
            error = self.error,
        )
    }
}

/// The kind of a JSON value, as a field error names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonKind {
    Null,
    Boolean,
    Number,
    String,
    Array,
    Object,
}

impl JsonKind {
    /// The kind of `value`.
    #[must_use]
    pub fn of(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(_) => Self::Boolean,
            Value::Number(_) => Self::Number,
            Value::String(_) => Self::String,
            Value::Array(_) => Self::Array,
            Value::Object(_) => Self::Object,
        }
    }

    fn admits(self, value: &Value) -> bool {
        Self::of(value) == self
    }
}

impl fmt::Display for JsonKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Null => "null",
            Self::Boolean => "boolean",
            Self::Number => "number",
            Self::String => "string",
            Self::Array => "array",
            Self::Object => "object",
        })
    }
}

/// A path into a request body, rendered the way upstream's `field.Path` is:
/// `spec.template.metadata.labels[app]`, `metadata.ownerReferences[0]`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FieldPath(Vec<Segment>);

/// A field name is borrowed when the border names it in source and owned when
/// it comes from a runtime schema: the protobuf transcoder
/// ([`crate::proto_transcode`]) reports paths through descriptors, whose names
/// live in the descriptor pool.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    Field(Cow<'static, str>),
    Key(String),
    Index(usize),
}

impl FieldPath {
    fn fields(names: &[&'static str]) -> Self {
        Self(
            names
                .iter()
                .map(|name| Segment::Field(Cow::Borrowed(*name)))
                .collect(),
        )
    }

    fn field(mut self, name: &'static str) -> Self {
        self.0.push(Segment::Field(Cow::Borrowed(name)));
        self
    }

    /// Descend into a field whose name is not known at compile time.
    pub(crate) fn named(mut self, name: &str) -> Self {
        self.0.push(Segment::Field(Cow::Owned(name.to_owned())));
        self
    }

    pub(crate) fn key(mut self, key: &str) -> Self {
        self.0.push(Segment::Key(key.to_owned()));
        self
    }

    pub(crate) fn index(mut self, index: usize) -> Self {
        self.0.push(Segment::Index(index));
        self
    }
}

impl fmt::Display for FieldPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            return f.write_str("(root)");
        }
        for (i, segment) in self.0.iter().enumerate() {
            match segment {
                Segment::Field(name) if i == 0 => f.write_str(name)?,
                Segment::Field(name) => write!(f, ".{name}")?,
                Segment::Key(key) => write!(f, "[{key}]")?,
                Segment::Index(index) => write!(f, "[{index}]")?,
            }
        }
        Ok(())
    }
}

/// The `ObjectMeta` fields that are string-to-string maps.
const STRING_MAP_FIELDS: [&str; 2] = ["labels", "annotations"];

/// The `ObjectMeta` fields that are arrays, with the kind each item must be.
const ARRAY_FIELDS: [(&str, JsonKind); 2] = [
    ("ownerReferences", JsonKind::Object),
    ("finalizers", JsonKind::String),
];

/// Where a built-in kind embeds a second `ObjectMeta`: the path to the object
/// holding that `metadata`. A closed list — each is a `PodTemplateSpec` or a
/// `JobTemplateSpec`, whose `metadata` upstream decodes as `ObjectMeta`.
///
/// A custom resource has none: its `spec.template` is whatever its schema
/// says, and upstream coerces an embedded `metadata` there only when the
/// schema marks it `x-kubernetes-embedded-resource`.
fn embedded_meta_parents(group: &str, kind: &str) -> &'static [&'static [&'static str]] {
    const SPEC_TEMPLATE: &[&[&str]] = &[&["spec", "template"]];
    match (group, kind) {
        ("apps", "Deployment" | "ReplicaSet" | "DaemonSet" | "StatefulSet")
        | ("batch", "Job")
        | ("", "ReplicationController") => SPEC_TEMPLATE,
        ("", "PodTemplate") => &[&["template"]],
        ("batch", "CronJob") => &[
            &["spec", "jobTemplate"],
            &["spec", "jobTemplate", "spec", "template"],
        ],
        _ => &[],
    }
}

/// The object at `path` under `root`, when every step is an object.
fn object_at<'v>(
    root: &'v mut Map<String, Value>,
    path: &[&str],
) -> Option<&'v mut Map<String, Value>> {
    let mut node = root;
    for step in path {
        node = node.get_mut(*step)?.as_object_mut()?;
    }
    Some(node)
}

/// Normalize the `metadata` held by `container`, which sits at `at`.
fn normalize_meta_slot(
    container: &mut Map<String, Value>,
    at: &[&'static str],
) -> Result<(), MetaShapeError> {
    match container.get_mut("metadata") {
        None => Ok(()),
        Some(Value::Object(meta)) => normalize_object_meta(meta, at),
        Some(Value::Null) => {
            container.remove("metadata");
            Ok(())
        }
        Some(other) => Err(MetaShapeError {
            path: FieldPath::fields(at).field("metadata"),
            expected: JsonKind::Object,
            found: JsonKind::of(other),
        }),
    }
}

/// Normalize one `ObjectMeta` object in place.
fn normalize_object_meta(
    meta: &mut Map<String, Value>,
    at: &[&'static str],
) -> Result<(), MetaShapeError> {
    let path = |field: &'static str| FieldPath::fields(at).field("metadata").field(field);
    for field in STRING_MAP_FIELDS {
        match meta.get_mut(field) {
            None => {}
            Some(Value::Null) => {
                meta.remove(field);
            }
            Some(Value::Object(map)) => {
                for (key, value) in map.iter_mut() {
                    match value {
                        Value::String(_) => {}
                        Value::Null => *value = Value::String(String::new()),
                        other => {
                            return Err(MetaShapeError {
                                path: path(field).key(key),
                                expected: JsonKind::String,
                                found: JsonKind::of(other),
                            });
                        }
                    }
                }
            }
            Some(other) => {
                return Err(MetaShapeError {
                    path: path(field),
                    expected: JsonKind::Object,
                    found: JsonKind::of(other),
                });
            }
        }
    }
    for (field, item_kind) in ARRAY_FIELDS {
        match meta.get_mut(field) {
            None => {}
            Some(Value::Null) => {
                meta.remove(field);
            }
            Some(Value::Array(items)) => {
                if let Some((index, item)) = items
                    .iter()
                    .enumerate()
                    .find(|(_, item)| !item_kind.admits(item))
                {
                    return Err(MetaShapeError {
                        path: path(field).index(index),
                        expected: item_kind,
                        found: JsonKind::of(item),
                    });
                }
            }
            Some(other) => {
                return Err(MetaShapeError {
                    path: path(field),
                    expected: JsonKind::Array,
                    found: JsonKind::of(other),
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn normalized(group: &str, kind: &str, body: Value) -> Value {
        ObjectBody::normalize(group, kind, body)
            .expect("a well-shaped body normalizes")
            .into_value()
    }

    fn refused(group: &str, kind: &str, body: Value) -> String {
        ObjectBody::normalize(group, kind, body)
            .expect_err("a mis-shaped body is refused")
            .to_string()
    }

    #[test]
    fn null_meta_fields_are_dropped_at_the_root() {
        let out = normalized(
            "",
            "ConfigMap",
            json!({"metadata": {
                "name": "c",
                "labels": null,
                "annotations": null,
                "ownerReferences": null,
                "finalizers": null
            }}),
        );
        assert_eq!(out, json!({"metadata": {"name": "c"}}));
    }

    #[test]
    fn a_null_map_entry_becomes_the_empty_string() {
        let out = normalized(
            "",
            "ConfigMap",
            json!({"metadata": {"name": "c", "labels": {"a": null, "b": "x"}}}),
        );
        assert_eq!(out["metadata"]["labels"], json!({"a": "", "b": "x"}));
    }

    #[test]
    fn a_pod_template_is_normalized_for_the_kinds_that_embed_one() {
        for (group, kind, parent) in [
            ("apps", "Deployment", "/spec/template"),
            ("apps", "ReplicaSet", "/spec/template"),
            ("apps", "DaemonSet", "/spec/template"),
            ("apps", "StatefulSet", "/spec/template"),
            ("batch", "Job", "/spec/template"),
            ("", "ReplicationController", "/spec/template"),
        ] {
            let out = normalized(
                group,
                kind,
                json!({"metadata": {"name": "d"}, "spec": {"template": {
                    "metadata": {"labels": {"app": "d"}, "annotations": null}
                }}}),
            );
            let meta = out
                .pointer(&[parent, "/metadata"].concat())
                .unwrap_or_else(|| panic!("{kind}: template metadata kept"));
            assert_eq!(meta, &json!({"labels": {"app": "d"}}), "{kind}");
        }
    }

    #[test]
    fn a_cron_job_normalizes_both_embedded_templates() {
        let out = normalized(
            "batch",
            "CronJob",
            json!({"metadata": {"name": "c"}, "spec": {"jobTemplate": {
                "metadata": {"labels": null},
                "spec": {"template": {"metadata": {"finalizers": null}}}
            }}}),
        );
        assert_eq!(out["spec"]["jobTemplate"]["metadata"], json!({}));
        assert_eq!(
            out["spec"]["jobTemplate"]["spec"]["template"]["metadata"],
            json!({})
        );
    }

    #[test]
    fn a_custom_resource_template_is_left_to_its_schema() {
        let body = json!({"metadata": {"name": "w"}, "spec": {"template": {
            "metadata": {"labels": null}
        }}});
        assert_eq!(normalized("example.com", "Widget", body.clone()), body);
    }

    #[test]
    fn wrong_kinds_are_refused_by_path() {
        let cases = [
            (json!("x"), "(root): expected object, got string"),
            (
                json!({"metadata": "x"}),
                "metadata: expected object, got string",
            ),
            (
                json!({"metadata": {"labels": "x"}}),
                "metadata.labels: expected object, got string",
            ),
            (
                json!({"metadata": {"annotations": {"a": 5}}}),
                "metadata.annotations[a]: expected string, got number",
            ),
            (
                json!({"metadata": {"ownerReferences": {}}}),
                "metadata.ownerReferences: expected array, got object",
            ),
            (
                json!({"metadata": {"ownerReferences": [{}, "x"]}}),
                "metadata.ownerReferences[1]: expected object, got string",
            ),
            (
                json!({"metadata": {"finalizers": "x"}}),
                "metadata.finalizers: expected array, got string",
            ),
            (
                json!({"metadata": {"finalizers": [true]}}),
                "metadata.finalizers[0]: expected string, got boolean",
            ),
        ];
        for (body, want) in cases {
            assert_eq!(refused("", "ConfigMap", body), want);
        }
        assert_eq!(
            refused(
                "apps",
                "Deployment",
                json!({"spec": {"template": {"metadata": {"labels": {"a": []}}}}})
            ),
            "spec.template.metadata.labels[a]: expected string, got array"
        );
    }

    #[test]
    fn normalizing_twice_changes_nothing() {
        let once = normalized(
            "apps",
            "Deployment",
            json!({"metadata": {"name": "d", "labels": {"a": null}, "finalizers": null},
                   "spec": {"template": {"metadata": null}}}),
        );
        assert_eq!(normalized("apps", "Deployment", once.clone()), once);
    }

    #[test]
    fn the_request_error_is_upstreams_400_sentence() {
        let e = ObjectBody::normalize("", "ConfigMap", json!({"metadata": 1}))
            .expect_err("refused")
            .request_error("v1", "ConfigMap");
        assert!(matches!(&e, ApiError::BadRequest(_)), "{e:?}");
        assert_eq!(
            e.to_string(),
            "invalid request: ConfigMap in version \"v1\" cannot be handled as a \
             ConfigMap: metadata: expected object, got number"
        );
    }
}
