//! `ObjectMeta` accessor extension trait.
//!
//! Every workload controller (ReplicaSet / Deployment / StatefulSet /
//! Endpoints / Job / GC) reads the same handful of fields off the
//! opaque `serde_json::Value` it lists from the store:
//!
//!   * `metadata.uid`  — the object's stable identity (owner refs)
//!   * `metadata.name` — the object's name (child naming, owner refs)
//!   * `metadata.namespace` — scope
//!
//! Before this trait each controller hand-copied byte-identical
//! `*_uid` / `*_name` accessors. Per the Prime Directive the shape is
//! solved ONCE here: [`ObjectMeta`] is the single accessor surface every
//! controller consumes. Adding a new controller no longer re-derives
//! these accessors.
//!
//! The trait is implemented for [`serde_json::Value`] (so it is also
//! available through any `&Value` deref); call sites read
//! `obj.uid()` / `obj.name()` / `obj.namespace()` directly.
//!
//! ## Reading an integer: [`DefaultedInt`] and [`int_at`] (T1.6)
//!
//! A count (`spec.replicas`, `spec.completions`, `metadata.generation`)
//! has three states — absent, an integer, or something else — and the
//! old `spec_i64(key, default)` read the third as the first: a Deployment
//! declaring `replicas: "3"` was scaled to one pod. Both readers go
//! through [`engenho_types::SpecInt`] and keep the third state an error:
//!
//!   * [`int_at`] — `Ok(None)` absent, `Ok(Some(n))` an integer,
//!     `Err(`[`ShapeError::NotAnInteger`]`)` anything else. For a field
//!     whose absence means "off" (`startingDeadlineSeconds`).
//!   * [`DefaultedInt::read`] — the same, with absence replaced by the
//!     field's API default, declared once next to the field's path
//!     ([`REPLICAS`]). For a count the apiserver would have defaulted.
//!
//! A malformed integer is a Declarative, Item-scoped [`ShapeError`]: the
//! object it belongs to gets nothing written for it this pass and a
//! Warning (an Event through the sweep, or [`warn_unreadable`] where
//! there is no sweep), and every other object is reconciled as usual.
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

use engenho_types::SpecInt;
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

    /// `metadata.deletionTimestamp` — present once a delete was accepted
    /// for an object that finalizers still hold (Terminating). `None` when
    /// absent, `null` or empty: an empty stamp is no stamp.
    fn deletion_timestamp(&self) -> Option<&str>;

    /// Whether the object is Terminating: a delete was accepted and its
    /// finalizers hold it (a [`Self::deletion_timestamp`] is present).
    ///
    /// A Terminating object is on its way out. A controller never writes
    /// it again as a child: re-creating over it would clear a
    /// `deletionTimestamp` nothing may clear, and deleting it again is the
    /// store's `NoOp`, one Raft entry per tick.
    fn is_terminating(&self) -> bool {
        self.deletion_timestamp().is_some()
    }

    /// Whether `metadata.finalizers` names `finalizer`. An absent, `null`
    /// or non-list field names none.
    fn has_finalizer(&self, finalizer: &str) -> bool;
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

    fn deletion_timestamp(&self) -> Option<&str> {
        self.get("metadata")
            .and_then(|m| m.get("deletionTimestamp"))
            .and_then(Value::as_str)
            .filter(|ts| !ts.is_empty())
    }

    fn has_finalizer(&self, finalizer: &str) -> bool {
        self.get("metadata")
            .and_then(|m| m.get("finalizers"))
            .and_then(Value::as_array)
            .is_some_and(|all| all.iter().any(|f| f.as_str() == Some(finalizer)))
    }
}

/// `apps/v1` `spec.replicas` (Deployment, `ReplicaSet`, `StatefulSet`):
/// the apiserver defaults an absent count to 1.
pub const REPLICAS: DefaultedInt = DefaultedInt::new(&["spec", "replicas"], 1);

/// An integer field whose absence means its API default.
///
/// The default is declared once, beside the path, so every controller
/// reading the field applies the same one; the old `spec_i64(key,
/// default)` took the default at each call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DefaultedInt {
    path: &'static [&'static str],
    absent: i64,
}

impl DefaultedInt {
    /// The field at `path`, reading as `absent` when missing or `null`.
    #[must_use]
    pub const fn new(path: &'static [&'static str], absent: i64) -> Self {
        Self { path, absent }
    }

    /// The field's path, outermost first.
    #[must_use]
    pub const fn path(self) -> &'static [&'static str] {
        self.path
    }

    /// The field's value in `object`: the integer it holds, or the API
    /// default when it is missing or `null`.
    ///
    /// # Errors
    ///
    /// [`ShapeError::NotAnInteger`] when the field holds anything else —
    /// never the default.
    pub fn read(self, object: &Value) -> Result<i64, ShapeError> {
        Ok(int_at(object, self.path)?.unwrap_or(self.absent))
    }
}

/// The integer at `path` in `object`, in its three states: `Ok(None)`
/// when missing or `null`, `Ok(Some(n))` when it holds an integer.
///
/// # Errors
///
/// [`ShapeError::NotAnInteger`] naming `path` when the field — or a step
/// above it that is not an object — holds anything else.
pub fn int_at(object: &Value, path: &'static [&'static str]) -> Result<Option<i64>, ShapeError> {
    match SpecInt::at(object, path) {
        SpecInt::Absent => Ok(None),
        SpecInt::Int(n) => Ok(Some(n)),
        // `SpecInt::at` reads `null` as Absent, so a `null` here is a
        // `SpecInt` built by hand; it means what Absent means.
        SpecInt::Malformed(found) => JsonKind::of(found).map_or(Ok(None), |found| {
            Err(ShapeError::NotAnInteger {
                path: FieldPath(path.to_vec()),
                found,
            })
        }),
    }
}

/// Say that a field `object` declares cannot be read (`error` names it),
/// so nothing is written from it this pass — the structured warning where
/// there is no [`crate::sweep::Sweep`] to raise an Event on the object.
pub fn warn_unreadable(controller: &'static str, object: &dyn fmt::Display, error: &ShapeError) {
    tracing::warn!(
        controller,
        object = %object,
        field = %error.path(),
        error = %error,
        "a field this object declares cannot be read; nothing is written from it this pass"
    );
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

impl JsonKind {
    /// The kind of `value`; `None` for `null`, which is never reported.
    #[must_use]
    pub const fn of(value: &Value) -> Option<Self> {
        match value {
            Value::Null => None,
            Value::Bool(_) => Some(Self::Bool),
            Value::Number(_) => Some(Self::Number),
            Value::String(_) => Some(Self::String),
            Value::Array(_) => Some(Self::Array),
            Value::Object(_) => Some(Self::Object),
        }
    }
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

/// A stored object does not have the shape a controller needs: a write
/// needs a container, or a read needs an integer.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ShapeError {
    /// `path` holds `found` where `expected` is required.
    #[error("{path} is {found}, expected {expected}")]
    Wrong {
        path: FieldPath,
        expected: Container,
        found: JsonKind,
    },
    /// `path` cannot be read as an integer: `found` stands there, or at a
    /// step above it that is not an object. `a number` here is one with a
    /// fraction or beyond the `i64` range.
    #[error("{path} is not an integer (found {found})")]
    NotAnInteger { path: FieldPath, found: JsonKind },
}

impl ShapeError {
    /// Re-anchor this error under `prefix`: the value it was found in is
    /// the one at `prefix` in the object the operator declared.
    #[must_use]
    pub fn under(self, prefix: &'static [&'static str]) -> Self {
        let anchor =
            |path: FieldPath| FieldPath(prefix.iter().chain(path.segments()).copied().collect());
        match self {
            Self::Wrong {
                path,
                expected,
                found,
            } => Self::Wrong {
                path: anchor(path),
                expected,
                found,
            },
            Self::NotAnInteger { path, found } => Self::NotAnInteger {
                path: anchor(path),
                found,
            },
        }
    }

    /// The path the error names.
    #[must_use]
    pub fn path(&self) -> &FieldPath {
        match self {
            Self::Wrong { path, .. } | Self::NotAnInteger { path, .. } => path,
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
    fn deletion_timestamp_reads_a_stamp_and_nothing_else() {
        let ts = "2026-01-01T00:00:00Z";
        assert_eq!(
            json!({"metadata": {"deletionTimestamp": ts}}).deletion_timestamp(),
            Some(ts)
        );
        for unstamped in [
            json!({"metadata": {}}),
            json!({"metadata": {"deletionTimestamp": null}}),
            json!({"metadata": {"deletionTimestamp": ""}}),
            json!({}),
        ] {
            assert_eq!(unstamped.deletion_timestamp(), None, "{unstamped}");
            assert!(!unstamped.is_terminating(), "{unstamped}");
        }
        assert!(json!({"metadata": {"deletionTimestamp": ts}}).is_terminating());
    }

    #[test]
    fn has_finalizer_matches_a_named_entry_only() {
        let obj = json!({"metadata": {"finalizers": ["orphan", "foregroundDeletion"]}});
        assert!(obj.has_finalizer("foregroundDeletion"));
        assert!(obj.has_finalizer("orphan"));
        assert!(!obj.has_finalizer("kubernetes"));
        for none in [
            json!({"metadata": {}}),
            json!({"metadata": {"finalizers": null}}),
            json!({"metadata": {"finalizers": "foregroundDeletion"}}),
        ] {
            assert!(!none.has_finalizer("foregroundDeletion"), "{none}");
        }
    }

    #[test]
    fn a_declared_integer_is_read() {
        let obj = json!({"spec": {"replicas": 3}});
        assert_eq!(REPLICAS.read(&obj), Ok(3));
        assert_eq!(int_at(&obj, &["spec", "replicas"]), Ok(Some(3)));
        assert_eq!(REPLICAS.read(&json!({"spec": {"replicas": 0}})), Ok(0));
    }

    #[test]
    fn an_absent_or_null_integer_is_the_api_default() {
        for obj in [
            json!({"spec": {}}),
            json!({"spec": {"replicas": null}}),
            json!({"spec": null}),
            json!({"metadata": {}}),
        ] {
            assert_eq!(REPLICAS.read(&obj), Ok(1), "{obj}");
            assert_eq!(int_at(&obj, REPLICAS.path()), Ok(None), "{obj}");
        }
        let seven = DefaultedInt::new(&["spec", "completions"], 7);
        assert_eq!(seven.read(&json!({"spec": {}})), Ok(7));
    }

    /// T1.6 — `"3"` is not absent and is not 1. The old `spec_i64` read it
    /// as the default and a Deployment declaring it was scaled to one pod.
    #[test]
    fn a_quoted_integer_is_an_error_naming_the_field_not_the_default() {
        let obj = json!({"spec": {"replicas": "3"}});
        let err = REPLICAS.read(&obj).unwrap_err();
        assert_eq!(
            err,
            ShapeError::NotAnInteger {
                path: FieldPath(vec!["spec", "replicas"]),
                found: JsonKind::String,
            }
        );
        assert_eq!(
            err.to_string(),
            "spec.replicas is not an integer (found a string)"
        );
        assert_eq!(int_at(&obj, REPLICAS.path()), Err(err));
    }

    #[test]
    fn every_non_integer_is_an_error_never_a_value() {
        for (value, found) in [
            (json!("two"), JsonKind::String),
            (json!(2.5), JsonKind::Number),
            (json!(3.0), JsonKind::Number),
            (json!(u64::MAX), JsonKind::Number),
            (json!(true), JsonKind::Bool),
            (json!([3]), JsonKind::Array),
            (json!({"n": 3}), JsonKind::Object),
        ] {
            let obj = json!({"spec": {"replicas": value}});
            assert!(
                matches!(
                    REPLICAS.read(&obj),
                    Err(ShapeError::NotAnInteger { found: f, .. }) if f == found
                ),
                "{obj}"
            );
        }
    }

    #[test]
    fn a_non_object_above_the_integer_is_an_error_too() {
        let obj = json!({"spec": "oops"});
        assert_eq!(
            REPLICAS.read(&obj).unwrap_err().to_string(),
            "spec.replicas is not an integer (found a string)"
        );
    }

    #[test]
    fn null_has_no_reported_kind() {
        assert_eq!(JsonKind::of(&Value::Null), None);
        assert_eq!(JsonKind::of(&json!(1)), Some(JsonKind::Number));
    }

    #[test]
    fn not_an_integer_re_anchors_under_a_parent() {
        let err = REPLICAS
            .read(&json!({"spec": {"replicas": "3"}}))
            .unwrap_err()
            .under(&["spec", "template"]);
        assert_eq!(
            err.path().segments(),
            ["spec", "template", "spec", "replicas"]
        );
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
