//! Finding unknown and duplicate fields, the way upstream's strict JSON
//! decoder does (`sigs.k8s.io/json`, `internal/golang/encoding/json`).
//!
//! Two walks, because the two findings need different inputs:
//!
//! * [`scan`] reads the request's own BYTES. A duplicate key only exists
//!   there: `serde_json::Value`, like a Go map, keeps the last value and
//!   forgets the first. For an object body it finds unknown fields in the
//!   same pass, so the findings come out in document order, as upstream's
//!   one decode reports them.
//! * [`unknown_paths`] walks a decoded [`Value`] — the object a PATCH
//!   would store, which never existed as bytes.
//!
//! What upstream reports, and so what these report:
//!
//! * inside a struct, a key with no field is `unknown field "<path>"` and
//!   its value is not looked into; a known field given twice is
//!   `duplicate field "<path>"`, and an unknown one given twice is still
//!   one unknown field;
//! * inside a map, any key given twice is a duplicate; no key is unknown;
//! * inside `RawExtension` and `FieldsV1`, nothing: they keep their bytes;
//! * inside an untyped value (`interface{}`, a patch body), any key given
//!   twice is a duplicate;
//! * the same finding once, and at most [`MAX_FINDINGS`] of them.
//!
//! A path is rendered as upstream renders it: keys joined by `.`, list
//! positions as `[i]`, map keys as keys (`metadata.labels.app`).

use std::collections::HashSet;
use std::fmt;

use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::Value;

use super::schema::{Row, Shape, ShapeId};

/// Upstream's cap on accumulated strict errors (`saveStrictError`).
pub const MAX_FINDINGS: usize = 100;

/// One step of a [`StrictPath`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Step {
    Key(String),
    Index(usize),
}

/// A position in a request body, rendered as `sigs.k8s.io/json` renders a
/// strict error's path: `spec.containers[0].image`, `[0].op`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct StrictPath(Vec<Step>);

impl StrictPath {
    fn push_key(&mut self, key: &str) {
        self.0.push(Step::Key(key.to_owned()));
    }

    fn push_index(&mut self, index: usize) {
        self.0.push(Step::Index(index));
    }

    fn pop(&mut self) {
        self.0.pop();
    }

    fn child(&self, key: &str) -> Self {
        let mut path = self.clone();
        path.push_key(key);
        path
    }

    /// Whether the last key is a strategic-merge directive
    /// (`k8s.io/apimachinery/pkg/util/strategicpatch/patch.go`): an
    /// instruction to the merge, never a field of any kind.
    pub(crate) fn names_merge_directive(&self) -> bool {
        matches!(
            self.0.last(),
            Some(Step::Key(key)) if key == "$patch"
                || key == "$retainKeys"
                || key.starts_with("$setElementOrder/")
                || key.starts_with("$deleteFromPrimitiveList/")
        )
    }

    /// Remove the field this path names from `value`. A path that does not
    /// lead to an object key removes nothing.
    pub(crate) fn remove_from(&self, value: &mut Value) {
        let Some((Step::Key(last), parents)) = self.0.split_last() else {
            return;
        };
        let mut node = value;
        for step in parents {
            let next = match (step, node) {
                (Step::Key(key), Value::Object(map)) => map.get_mut(key),
                (Step::Index(index), Value::Array(items)) => items.get_mut(*index),
                _ => None,
            };
            let Some(next) = next else {
                return;
            };
            node = next;
        }
        if let Value::Object(map) = node {
            map.remove(last);
        }
    }
}

impl fmt::Display for StrictPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, step) in self.0.iter().enumerate() {
            match step {
                Step::Key(key) if i == 0 => f.write_str(key)?,
                Step::Key(key) => write!(f, ".{key}")?,
                Step::Index(index) => write!(f, "[{index}]")?,
            }
        }
        Ok(())
    }
}

/// Which of upstream's two strict errors a finding is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FindingKind {
    /// A key the Go type has no field for. The field is dropped.
    Unknown,
    /// A key given twice. The last value is kept.
    Duplicate,
}

/// What the finding was made in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Origin {
    /// The object, or a merge-style patch of it.
    Object,
    /// A JSON patch's operation list; upstream prefixes these `json patch `.
    JsonPatch,
}

/// One strict decoding error.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Finding {
    kind: FindingKind,
    path: StrictPath,
    origin: Origin,
}

impl Finding {
    /// Which error this is.
    #[must_use]
    pub fn kind(&self) -> FindingKind {
        self.kind
    }

    /// Where it is.
    #[must_use]
    pub fn path(&self) -> &StrictPath {
        &self.path
    }
}

impl fmt::Display for Finding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.origin == Origin::JsonPatch {
            f.write_str("json patch ")?;
        }
        f.write_str(match self.kind {
            FindingKind::Unknown => "unknown field ",
            FindingKind::Duplicate => "duplicate field ",
        })?;
        write!(f, "{}", super::GoQuoted(&self.path))
    }
}

/// Strict decoding errors in the order found, each once, at most
/// [`MAX_FINDINGS`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Findings {
    items: Vec<Finding>,
}

impl Findings {
    fn record(&mut self, finding: Finding) {
        if self.items.len() < MAX_FINDINGS && !self.items.contains(&finding) {
            self.items.push(finding);
        }
    }

    /// Append `other`'s findings after this one's.
    pub(crate) fn extend(&mut self, other: Findings) {
        for finding in other.items {
            self.record(finding);
        }
    }

    /// Every finding, in order.
    #[must_use]
    pub fn as_slice(&self) -> &[Finding] {
        &self.items
    }

    /// Whether nothing was found.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

/// What a body is decoded into, for a [`scan`].
#[derive(Clone, Copy, Debug)]
pub(crate) enum Target<'s> {
    /// A kind's own type: unknown and duplicate fields.
    Kind(Row<'s>),
    /// `interface{}` / `map[string]interface{}`: duplicate fields only.
    Untyped,
    /// A JSON patch operation list (`[]jsonPatchOp`).
    JsonPatch,
}

/// Where the scanner stands.
#[derive(Clone, Copy, Debug)]
enum At {
    Typed(ShapeId),
    Untyped,
    Raw,
    JsonPatchOps,
    JsonPatchOp,
}

/// Scan a JSON document's bytes for strict decoding errors.
///
/// # Errors
///
/// The JSON parse error when `raw` is not JSON. Every caller has already
/// parsed the same bytes, so this does not happen on a live request.
pub(crate) fn scan(raw: &[u8], target: Target<'_>) -> Result<Findings, serde_json::Error> {
    let (row, at, origin) = match target {
        Target::Kind(row) => (Some(row), At::Typed(row.root()), Origin::Object),
        Target::Untyped => (None, At::Untyped, Origin::Object),
        Target::JsonPatch => (None, At::JsonPatchOps, Origin::JsonPatch),
    };
    let mut scanner = Scanner {
        row,
        origin,
        path: StrictPath::default(),
        found: Findings::default(),
    };
    let mut de = serde_json::Deserializer::from_slice(raw);
    Node {
        at,
        scanner: &mut scanner,
    }
    .deserialize(&mut de)?;
    de.end()?;
    Ok(scanner.found)
}

struct Scanner<'s> {
    row: Option<Row<'s>>,
    origin: Origin,
    path: StrictPath,
    found: Findings,
}

impl<'s> Scanner<'s> {
    fn shape(&self, id: ShapeId) -> &'s Shape {
        match self.row {
            Some(row) => row.shape(id),
            None => &Shape::Raw,
        }
    }

    fn found(&mut self, kind: FindingKind, key: &str) {
        let finding = Finding {
            kind,
            path: self.path.child(key),
            origin: self.origin,
        };
        self.found.record(finding);
    }

    /// Enter `key` of an object at `at`: record what it is, return where
    /// its value stands.
    fn enter(&mut self, at: At, key: &str, seen: &mut HashSet<String>) -> At {
        let mut duplicate_check = |scanner: &mut Self| {
            if !seen.insert(key.to_owned()) {
                scanner.found(FindingKind::Duplicate, key);
            }
        };
        match at {
            At::Raw | At::JsonPatchOps => At::Raw,
            At::Untyped => {
                duplicate_check(self);
                At::Untyped
            }
            At::JsonPatchOp => match key {
                "op" | "path" | "from" => {
                    duplicate_check(self);
                    At::Raw
                }
                "value" => {
                    duplicate_check(self);
                    At::Untyped
                }
                _ => {
                    self.found(FindingKind::Unknown, key);
                    At::Raw
                }
            },
            At::Typed(id) => match self.shape(id) {
                Shape::Struct(fields) => {
                    if let Some(&field) = fields.get(key) {
                        duplicate_check(self);
                        At::Typed(field)
                    } else {
                        self.found(FindingKind::Unknown, key);
                        At::Raw
                    }
                }
                &Shape::Map(values) => {
                    duplicate_check(self);
                    At::Typed(values)
                }
                Shape::List(_) | Shape::Leaf | Shape::Raw => At::Raw,
            },
        }
    }

    /// Where an element of a list at `at` stands.
    fn element(&self, at: At) -> At {
        match at {
            At::Untyped => At::Untyped,
            At::JsonPatchOps => At::JsonPatchOp,
            At::Typed(id) => match self.shape(id) {
                &Shape::List(items) => At::Typed(items),
                Shape::Struct(_) | Shape::Map(_) | Shape::Leaf | Shape::Raw => At::Raw,
            },
            At::Raw | At::JsonPatchOp => At::Raw,
        }
    }
}

struct Node<'a, 's> {
    at: At,
    scanner: &'a mut Scanner<'s>,
}

impl<'de> DeserializeSeed<'de> for Node<'_, '_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Node<'_, '_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON value")
    }

    fn visit_bool<E>(self, _: bool) -> Result<(), E> {
        Ok(())
    }

    fn visit_i64<E>(self, _: i64) -> Result<(), E> {
        Ok(())
    }

    fn visit_u64<E>(self, _: u64) -> Result<(), E> {
        Ok(())
    }

    fn visit_f64<E>(self, _: f64) -> Result<(), E> {
        Ok(())
    }

    fn visit_str<E>(self, _: &str) -> Result<(), E> {
        Ok(())
    }

    fn visit_unit<E>(self) -> Result<(), E> {
        Ok(())
    }

    fn visit_none<E>(self) -> Result<(), E> {
        Ok(())
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        let Node { at, scanner } = self;
        let element = scanner.element(at);
        let mut index = 0;
        loop {
            scanner.path.push_index(index);
            let more = seq.next_element_seed(Node {
                at: element,
                scanner: &mut *scanner,
            })?;
            scanner.path.pop();
            if more.is_none() {
                return Ok(());
            }
            index += 1;
        }
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let Node { at, scanner } = self;
        let mut seen = HashSet::new();
        while let Some(key) = map.next_key::<String>()? {
            let value_at = scanner.enter(at, &key, &mut seen);
            scanner.path.push_key(&key);
            map.next_value_seed(Node {
                at: value_at,
                scanner: &mut *scanner,
            })?;
            scanner.path.pop();
        }
        Ok(())
    }
}

/// Every unknown field of `value` under `row`'s schema, in the value's own
/// key order, however many there are: the fields a decode into the Go type
/// drops.
pub(crate) fn unknown_paths(row: Row<'_>, value: &Value) -> Vec<StrictPath> {
    let mut found = Vec::new();
    let mut path = StrictPath::default();
    walk(row, row.root(), value, &mut path, &mut found);
    found
}

/// `paths` as unknown-field findings, capped as upstream caps them.
pub(crate) fn unknown_findings(paths: &[StrictPath]) -> Findings {
    let mut found = Findings::default();
    for path in paths {
        found.record(Finding {
            kind: FindingKind::Unknown,
            path: path.clone(),
            origin: Origin::Object,
        });
    }
    found
}

fn walk(
    row: Row<'_>,
    id: ShapeId,
    value: &Value,
    path: &mut StrictPath,
    found: &mut Vec<StrictPath>,
) {
    match (row.shape(id), value) {
        (Shape::Struct(fields), Value::Object(map)) => {
            for (key, child) in map {
                match fields.get(key.as_str()) {
                    Some(&field) => {
                        path.push_key(key);
                        walk(row, field, child, path, found);
                        path.pop();
                    }
                    None => found.push(path.child(key)),
                }
            }
        }
        (&Shape::Map(values), Value::Object(map)) => {
            for (key, child) in map {
                path.push_key(key);
                walk(row, values, child, path, found);
                path.pop();
            }
        }
        (&Shape::List(items), Value::Array(elements)) => {
            for (index, child) in elements.iter().enumerate() {
                path.push_index(index);
                walk(row, items, child, path, found);
                path.pop();
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::super::schema::row;
    use super::*;

    fn rendered(found: &Findings) -> Vec<String> {
        found.as_slice().iter().map(ToString::to_string).collect()
    }

    fn pod() -> Row<'static> {
        row("", "v1", "Pod").expect("Pod is schema-backed")
    }

    #[test]
    fn unknown_and_duplicate_fields_come_out_in_document_order() {
        let raw = br#"{"apiVersion":"v1","kind":"Pod",
            "metadata":{"name":"p","name":"q","bogus":1,"labels":{"a":"1","a":"2"}},
            "spec":{"containers":[{"name":"c","image":"i","imagePullPolicyy":"x"}]},
            "spec":{}}"#;
        let found = scan(raw, Target::Kind(pod())).expect("json");
        assert_eq!(
            rendered(&found),
            [
                r#"duplicate field "metadata.name""#,
                r#"unknown field "metadata.bogus""#,
                r#"duplicate field "metadata.labels.a""#,
                r#"unknown field "spec.containers[0].imagePullPolicyy""#,
                r#"duplicate field "spec""#,
            ]
        );
    }

    #[test]
    fn an_unknown_field_is_reported_once_and_not_looked_into() {
        let raw = br#"{"extra":{"a":1,"a":2},"extra":3}"#;
        let found = scan(raw, Target::Kind(pod())).expect("json");
        assert_eq!(rendered(&found), [r#"unknown field "extra""#]);
    }

    #[test]
    fn a_raw_extension_keeps_its_bytes() {
        let revision = row("apps", "v1", "ControllerRevision").expect("schema-backed");
        let raw = br#"{"data":{"x":1,"x":2,"anything":{}},"revision":1}"#;
        assert!(scan(raw, Target::Kind(revision)).expect("json").is_empty());
    }

    #[test]
    fn an_untyped_patch_reports_every_duplicate_and_no_unknown() {
        let raw = br#"{"spec":{"x":1,"x":2},"whatever":[{"k":1,"k":1}]}"#;
        assert_eq!(
            rendered(&scan(raw, Target::Untyped).expect("json")),
            [
                r#"duplicate field "spec.x""#,
                r#"duplicate field "whatever[0].k""#
            ]
        );
    }

    #[test]
    fn a_json_patch_is_checked_as_an_operation_list() {
        let raw = br#"[{"op":"add","path":"/a","value":{"k":1,"k":2},"op":"add","extra":1}]"#;
        assert_eq!(
            rendered(&scan(raw, Target::JsonPatch).expect("json")),
            [
                r#"json patch duplicate field "[0].value.k""#,
                r#"json patch duplicate field "[0].op""#,
                r#"json patch unknown field "[0].extra""#,
            ]
        );
    }

    #[test]
    fn findings_stop_at_the_upstream_cap() {
        let mut raw = String::from("{");
        for i in 0..(MAX_FINDINGS + 20) {
            if i > 0 {
                raw.push(',');
            }
            raw.push_str("\"f");
            raw.push_str(&i.to_string());
            raw.push_str("\":1");
        }
        raw.push('}');
        let found = scan(raw.as_bytes(), Target::Kind(pod())).expect("json");
        assert_eq!(found.as_slice().len(), MAX_FINDINGS);
    }

    #[test]
    fn a_decoded_value_is_walked_for_unknown_fields() {
        let value = serde_json::json!({
            "metadata": {"name": "p", "labels": {"x": "y"}, "nope": true},
            "spec": {"containers": [{"name": "c"}, {"name": "d", "wat": 1}]},
        });
        assert_eq!(
            rendered(&unknown_findings(&unknown_paths(pod(), &value))),
            [
                r#"unknown field "metadata.nope""#,
                r#"unknown field "spec.containers[1].wat""#
            ]
        );
    }

    #[test]
    fn removing_a_path_drops_exactly_that_field() {
        let mut value = serde_json::json!({
            "spec": {"containers": [{"name": "c"}, {"name": "d", "wat": 1}]},
        });
        for path in unknown_paths(pod(), &value) {
            path.remove_from(&mut value);
        }
        assert_eq!(
            value,
            serde_json::json!({"spec": {"containers": [{"name": "c"}, {"name": "d"}]}})
        );
    }
}
