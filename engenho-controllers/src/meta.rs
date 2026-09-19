//! `ObjectMeta` accessor extension trait.
//!
//! Every workload controller (ReplicaSet / Deployment / StatefulSet /
//! Endpoints / Job / GC) reads the same handful of fields off the
//! opaque `serde_json::Value` it lists from the store:
//!
//!   * `metadata.uid`  — the object's stable identity (owner refs)
//!   * `metadata.name` — the object's name (child naming, owner refs)
//!   * `metadata.namespace` — scope
//!   * `spec.<key>` as an `i64` — replica/completion/parallelism counts
//!
//! Before this trait each controller hand-copied byte-identical
//! `*_uid` / `*_name` / `*_replicas` accessors. Per the Prime
//! Directive the shape is solved ONCE here: [`ObjectMeta`] is the
//! single accessor surface every controller consumes. Adding a new
//! controller no longer re-derives these four accessors.
//!
//! The trait is implemented for [`serde_json::Value`] (so it is also
//! available through any `&Value` deref); call sites read
//! `obj.uid()` / `obj.name()` / `obj.namespace()` /
//! `obj.spec_i64("replicas", 1)` directly.
//!
//! ## Writing: [`object_mut`] and [`array_mut`] are total
//!
//! Reading an opaque object is already total (`get` returns `Option`).
//! Writing was not: a controller reached a nested map through
//! `.as_object_mut().expect(..)` or `serde_json`'s `IndexMut`, and both
//! PANIC on a value of the wrong JSON type. Stored objects are declared by
//! users, so the wrong type is ordinary input, not a broken invariant. The
//! owner-reference writer was the worked case: a `ReplicaSet` whose
//! `spec.template.metadata.ownerReferences` was `null` panicked the whole
//! controller task on its first pod, and every `ReplicaSet` after it in the
//! list stopped converging.
//!
//! The two accessors decide every shape in one place:
//!
//!   * absent, or `null` — treated as empty: `{}` or `[]` is inserted and
//!     returned. Upstream's Go types decode `null` to a nil map or slice,
//!     and every upstream writer initializes a nil one before writing, so
//!     this is the same answer, not a new leniency;
//!   * the expected container — returned as-is;
//!   * anything else — [`ShapeError::Wrong`], naming the dotted path and the
//!     JSON type found there. Nothing is written.
//!
//! [`JsonKind`] has no `Null` arm, because null is never reported: it is the
//! empty case.

use std::fmt;

use serde_json::{Map, Value};

/// Borrowed accessors over a K8s object's `metadata` + `spec`.
///
/// Implemented for [`serde_json::Value`]. The returned `&str` borrows
/// from the object, matching the byte-for-byte behavior of the
/// per-controller accessors this trait replaces.
pub trait ObjectMeta {
    /// `metadata.uid` — the object's stable identity. `None` when the
    /// field is absent (a freshly minted object the apiserver has not
    /// stamped yet).
    fn uid(&self) -> Option<&str>;

    /// `metadata.name` — the object's name. `None` when absent.
    fn name(&self) -> Option<&str>;

    /// `metadata.namespace` — the object's namespace scope. `None`
    /// for cluster-scoped objects or before the field is set.
    fn namespace(&self) -> Option<&str>;

    /// `spec.<key>` interpreted as an `i64`, falling back to `default`
    /// when the field is absent or not an integer. Mirrors the
    /// per-controller `replicas` / `desired_replicas` / `completions`
    /// / `parallelism` accessors (each was `…unwrap_or(<default>)`).
    fn spec_i64(&self, key: &str, default: i64) -> i64;
}

impl ObjectMeta for Value {
    fn uid(&self) -> Option<&str> {
        self.get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(|u| u.as_str())
    }

    fn name(&self) -> Option<&str> {
        self.get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
    }

    fn namespace(&self) -> Option<&str> {
        self.get("metadata")
            .and_then(|m| m.get("namespace"))
            .and_then(|n| n.as_str())
    }

    fn spec_i64(&self, key: &str, default: i64) -> i64 {
        self.get("spec")
            .and_then(|s| s.get(key))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(default)
    }
}

/// The container a write needs at a path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Container {
    Object,
    Array,
}

impl fmt::Display for Container {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Object => "an object",
            Self::Array => "an array",
        })
    }
}

/// The JSON type found where a [`Container`] was needed.
///
/// There is no `Null` arm: the accessors treat null as empty and never
/// report it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonKind {
    Bool,
    Number,
    String,
    Array,
    Object,
}

impl fmt::Display for JsonKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Bool => "a boolean",
            Self::Number => "a number",
            Self::String => "a string",
            Self::Array => "an array",
            Self::Object => "an object",
        })
    }
}

/// A dotted path into an object, as the operator reads it.
///
/// Segments are static field names. A value a controller built by cloning
/// part of its parent (a Pod from `spec.template`) reports its paths
/// relative to that parent through [`ShapeError::under`], so the message
/// names the field the operator actually has to fix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldPath(Vec<&'static str>);

impl FieldPath {
    /// The path's segments, outermost first. Empty for the object itself.
    #[must_use]
    pub fn segments(&self) -> &[&'static str] {
        &self.0
    }
}

impl fmt::Display for FieldPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut segments = self.0.iter();
        let Some(first) = segments.next() else {
            return f.write_str("the object itself");
        };
        f.write_str(first)?;
        for segment in segments {
            f.write_str(".")?;
            f.write_str(segment)?;
        }
        Ok(())
    }
}

/// A stored object does not have the shape a write needs.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ShapeError {
    /// `path` holds `found` where `expected` is required.
    #[error("{path} is {found}, expected {expected}")]
    Wrong {
        path: FieldPath,
        expected: Container,
        found: JsonKind,
    },
}

impl ShapeError {
    /// Re-anchor this error under `prefix`: the value it was found in is
    /// the one at `prefix` in the object the operator declared.
    #[must_use]
    pub fn under(self, prefix: &'static [&'static str]) -> Self {
        match self {
            Self::Wrong {
                path,
                expected,
                found,
            } => Self::Wrong {
                path: FieldPath(prefix.iter().chain(path.segments()).copied().collect()),
                expected,
                found,
            },
        }
    }

    /// The path the error names.
    #[must_use]
    pub fn path(&self) -> &FieldPath {
        match self {
            Self::Wrong { path, .. } => path,
        }
    }

    fn wrong(path: &'static [&'static str], expected: Container, found: JsonKind) -> Self {
        Self::Wrong {
            path: FieldPath(path.to_vec()),
            expected,
            found,
        }
    }
}

/// The map at `path` inside `value`, creating every absent or null step as
/// `{}`.
///
/// `path` is relative to `value`; the empty path is `value` itself.
///
/// # Errors
///
/// [`ShapeError::Wrong`] naming the first step (or `value` itself) that
/// holds something other than an object or null. Nothing is written at or
/// below that step.
pub fn object_mut<'v>(
    value: &'v mut Value,
    path: &'static [&'static str],
) -> Result<&'v mut Map<String, Value>, ShapeError> {
    match path.split_last() {
        None => object_slot(value, path),
        Some((last, parent)) => object_slot(
            object_mut(value, parent)?
                .entry(*last)
                .or_insert(Value::Null),
            path,
        ),
    }
}

/// The array at `path` inside `value`, creating it as `[]` when absent or
/// null, and every absent or null step above it as `{}`.
///
/// # Errors
///
/// [`ShapeError::Wrong`] naming the first step that holds something other
/// than an object (above the leaf) or an array (at the leaf), or null.
pub fn array_mut<'v>(
    value: &'v mut Value,
    path: &'static [&'static str],
) -> Result<&'v mut Vec<Value>, ShapeError> {
    match path.split_last() {
        None => array_slot(value, path),
        Some((last, parent)) => array_slot(
            object_mut(value, parent)?
                .entry(*last)
                .or_insert(Value::Null),
            path,
        ),
    }
}

/// `slot` as a map; null becomes `{}`.
fn object_slot<'v>(
    slot: &'v mut Value,
    path: &'static [&'static str],
) -> Result<&'v mut Map<String, Value>, ShapeError> {
    let found = match slot {
        Value::Object(map) => return Ok(map),
        Value::Null => {
            *slot = Value::Object(Map::new());
            return object_slot(slot, path);
        }
        Value::Bool(_) => JsonKind::Bool,
        Value::Number(_) => JsonKind::Number,
        Value::String(_) => JsonKind::String,
        Value::Array(_) => JsonKind::Array,
    };
    Err(ShapeError::wrong(path, Container::Object, found))
}

/// `slot` as an array; null becomes `[]`.
fn array_slot<'v>(
    slot: &'v mut Value,
    path: &'static [&'static str],
) -> Result<&'v mut Vec<Value>, ShapeError> {
    let found = match slot {
        Value::Array(items) => return Ok(items),
        Value::Null => {
            *slot = Value::Array(Vec::new());
            return array_slot(slot, path);
        }
        Value::Bool(_) => JsonKind::Bool,
        Value::Number(_) => JsonKind::Number,
        Value::String(_) => JsonKind::String,
        Value::Object(_) => JsonKind::Object,
    };
    Err(ShapeError::wrong(path, Container::Array, found))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const OWNER_REFS: &[&str] = &["metadata", "ownerReferences"];
    const ANNOTATIONS: &[&str] = &["metadata", "annotations"];

    #[test]
    fn absent_steps_are_created_empty() {
        let mut obj = json!({"kind": "Pod"});
        array_mut(&mut obj, OWNER_REFS).unwrap().push(json!(1));
        assert_eq!(
            obj,
            json!({"kind": "Pod", "metadata": {"ownerReferences": [1]}})
        );
    }

    #[test]
    fn null_is_empty_at_every_step() {
        let mut obj = json!({"metadata": null});
        object_mut(&mut obj, ANNOTATIONS)
            .unwrap()
            .insert("a".into(), json!("b"));
        assert_eq!(obj, json!({"metadata": {"annotations": {"a": "b"}}}));

        let mut obj = json!({"metadata": {"ownerReferences": null}});
        assert!(array_mut(&mut obj, OWNER_REFS).unwrap().is_empty());
        assert_eq!(obj, json!({"metadata": {"ownerReferences": []}}));

        let mut root = Value::Null;
        object_mut(&mut root, &[]).unwrap();
        assert_eq!(root, json!({}));
    }

    #[test]
    fn an_existing_container_is_returned_untouched() {
        let mut obj = json!({"metadata": {"annotations": {"keep": "me"}}});
        let anns = object_mut(&mut obj, ANNOTATIONS).unwrap();
        assert_eq!(anns.get("keep"), Some(&json!("me")));
    }

    #[test]
    fn the_wrong_type_at_the_leaf_is_named_and_left_alone() {
        let mut obj = json!({"metadata": {"ownerReferences": "x"}});
        let err = array_mut(&mut obj, OWNER_REFS).unwrap_err();
        assert_eq!(
            err,
            ShapeError::Wrong {
                path: FieldPath(vec!["metadata", "ownerReferences"]),
                expected: Container::Array,
                found: JsonKind::String,
            }
        );
        assert_eq!(
            err.to_string(),
            "metadata.ownerReferences is a string, expected an array"
        );
        assert_eq!(obj, json!({"metadata": {"ownerReferences": "x"}}));
    }

    #[test]
    fn the_wrong_type_above_the_leaf_is_named_at_that_step() {
        let mut obj = json!({"metadata": [1]});
        let err = object_mut(&mut obj, ANNOTATIONS).unwrap_err();
        assert_eq!(err.path().segments(), ["metadata"]);
        assert_eq!(err.to_string(), "metadata is an array, expected an object");
        assert_eq!(obj, json!({"metadata": [1]}));

        let mut not_an_object = json!("scalar");
        assert_eq!(
            object_mut(&mut not_an_object, ANNOTATIONS)
                .unwrap_err()
                .to_string(),
            "the object itself is a string, expected an object"
        );
    }

    #[test]
    fn every_non_null_kind_is_reported_as_itself() {
        for (value, found) in [
            (json!(true), JsonKind::Bool),
            (json!(1), JsonKind::Number),
            (json!("s"), JsonKind::String),
            (json!([]), JsonKind::Array),
        ] {
            let mut obj = json!({"metadata": {"annotations": value}});
            assert!(
                matches!(
                    object_mut(&mut obj, ANNOTATIONS),
                    Err(ShapeError::Wrong { found: f, expected: Container::Object, .. }) if f == found
                ),
                "{found:?}"
            );
        }
        let mut obj = json!({"metadata": {"ownerReferences": {}}});
        assert!(matches!(
            array_mut(&mut obj, OWNER_REFS),
            Err(ShapeError::Wrong {
                found: JsonKind::Object,
                expected: Container::Array,
                ..
            })
        ));
    }

    #[test]
    fn under_re_anchors_the_path_in_the_parent() {
        let mut pod = json!({"metadata": {"ownerReferences": 7}});
        let err = array_mut(&mut pod, OWNER_REFS)
            .unwrap_err()
            .under(&["spec", "template"]);
        assert_eq!(
            err.to_string(),
            "spec.template.metadata.ownerReferences is a number, expected an array"
        );
    }

    #[test]
    fn uid_reads_metadata_uid() {
        let obj = json!({"metadata": {"uid": "u-1"}});
        assert_eq!(obj.uid(), Some("u-1"));
    }

    #[test]
    fn uid_none_when_absent() {
        let obj = json!({"metadata": {"name": "x"}});
        assert_eq!(obj.uid(), None);
        let obj = json!({"spec": {}});
        assert_eq!(obj.uid(), None);
    }

    #[test]
    fn name_reads_metadata_name() {
        let obj = json!({"metadata": {"name": "podinfo"}});
        assert_eq!(obj.name(), Some("podinfo"));
    }

    #[test]
    fn name_none_when_absent() {
        let obj = json!({"metadata": {"uid": "u"}});
        assert_eq!(obj.name(), None);
    }

    #[test]
    fn namespace_reads_metadata_namespace() {
        let obj = json!({"metadata": {"namespace": "kube-system"}});
        assert_eq!(obj.namespace(), Some("kube-system"));
    }

    #[test]
    fn namespace_none_when_absent() {
        let obj = json!({"metadata": {"name": "x"}});
        assert_eq!(obj.namespace(), None);
    }

    #[test]
    fn spec_i64_reads_value() {
        let obj = json!({"spec": {"replicas": 3}});
        assert_eq!(obj.spec_i64("replicas", 1), 3);
    }

    #[test]
    fn spec_i64_falls_back_when_absent() {
        let obj = json!({"spec": {}});
        assert_eq!(obj.spec_i64("replicas", 1), 1);
        let obj = json!({"metadata": {}});
        assert_eq!(obj.spec_i64("completions", 7), 7);
    }

    #[test]
    fn spec_i64_falls_back_when_not_integer() {
        // A non-integer (string / float) yields the default, matching
        // the per-controller `as_i64().unwrap_or(default)` behavior.
        let obj = json!({"spec": {"replicas": "two"}});
        assert_eq!(obj.spec_i64("replicas", 1), 1);
        let obj = json!({"spec": {"parallelism": 2.5}});
        assert_eq!(obj.spec_i64("parallelism", 1), 1);
    }

    #[test]
    fn accessors_return_borrowed_str() {
        // The returned &str borrows from the object (no allocation) —
        // call sites that need an owned String do `.map(String::from)`.
        let obj = json!({"metadata": {"uid": "u", "name": "n", "namespace": "ns"}});
        let _: Option<&str> = obj.uid();
        let _: Option<&str> = obj.name();
        let _: Option<&str> = obj.namespace();
    }
}
