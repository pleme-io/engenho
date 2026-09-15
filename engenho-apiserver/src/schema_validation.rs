//! Structural-schema validation for custom resources.
//!
//! Measured 2026-08-28: a CRD declaring `spec.size: {type: integer}` accepted a
//! CR carrying `spec.size: "NOT-AN-INT"` with **201**. `crd.rs` was explicit
//! that this was coming — its `CrdEntry::schema` field is documented as "the
//! opaque openAPIV3Schema for this version (validation DEFERRED)" — so the
//! schema was already captured, just never applied.
//!
//! A CRD's whole promise is that its schema is enforced. An unvalidated CR is
//! worse than an unschema'd one: every controller downstream was written
//! against the declared types and will panic, mis-branch, or silently coerce
//! on data the apiserver swore could not exist.
//!
//! ## Scope — read this before assuming coverage
//!
//! This implements **type checking** over a structural schema: `type`,
//! `properties`, `items`, and `required`. That is the subset that catches the
//! measured defect and the overwhelming bulk of real schema violations.
//!
//! It does **not** implement: `format`, `pattern`, `enum`, numeric bounds
//! (`minimum`/`maximum`), string/array length bounds, `oneOf`/`anyOf`/`allOf`,
//! `x-kubernetes-validations` (CEL), or pruning of unknown fields.
//! **Defaulting IS implemented** — see [`apply_defaults`], added 2026-09-15
//! after a missing default crashed a controller in a loop.
//! Each is a real upstream behaviour and each is absent here. The honest
//! framing is that a CR which passes this has *correctly-typed* fields, not a
//! *valid* object.
//!
//! Deliberately hand-rolled rather than pulling a JSON-Schema crate: upstream's
//! structural-schema dialect is a constrained subset with Kubernetes-specific
//! extensions (`x-kubernetes-preserve-unknown-fields`, `x-kubernetes-int-or-string`),
//! and a general validator would need configuring into agreement with it anyway
//! — while adding a dependency whose disagreements with upstream would surface
//! as mysterious rejections. When the remaining keywords land, revisit that
//! trade rather than treating it as settled.
//!
//! ## Tier
//!
//! **parse-time-rejected** at the create boundary. Not unrepresentable: the
//! store still accepts any `Value`, so this is a check that must be CALLED.

use serde_json::Value;

/// One validation failure, addressed by its JSON path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaViolation {
    /// Dotted path to the offending field (`spec.size`).
    pub path: String,
    /// What the schema required.
    pub expected: String,
    /// What the object carried.
    pub found: String,
}

/// The JSON type name of a value, as the schema dialect spells it.
fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "integer"
            } else {
                "number"
            }
        }
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Whether `value` satisfies the schema's declared `type`.
///
/// `integer` is accepted where `number` is required — every integer is a
/// number, and rejecting that would fail objects upstream accepts. The reverse
/// is NOT true: a fractional value under `type: integer` is a violation, which
/// is the case a naive `is_number()` check would wave through.
fn type_matches(schema_type: &str, value: &Value) -> bool {
    match schema_type {
        "number" => value.is_number(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        other => other == type_name(value),
    }
}

/// Validate `value` against a structural `schema`, collecting every violation
/// rather than stopping at the first — an operator fixing a manifest wants the
/// whole list, not one error per apply cycle.
#[must_use]
pub fn validate(schema: &Value, value: &Value) -> Vec<SchemaViolation> {
    let mut out = Vec::new();
    walk(schema, value, "", &mut out);
    out
}

/// Apply the schema's `default` values to every absent field of `value`,
/// in place.
///
/// **A missing default is not a cosmetic gap — it is a crash.** Kubernetes
/// applies structural-schema defaults when an object is decoded, so every
/// controller in the ecosystem is written as though a defaulted field is
/// always present, and reads it without a nil check. Measured on rio
/// 2026-09-15: `GitRepository.spec.timeout` carries `default: 60s` in Flux's
/// CRD, engenho stored the object without it, and source-controller v1.8.5
/// dereferenced the nil `*metav1.Duration` at
/// `gitrepository_controller.go:1007` — panicking on every reconcile, forever,
/// with the GitRepository stuck reporting "building artifact". No error
/// pointed at the apiserver.
///
/// Runs BEFORE validation, as upstream does: a default must be able to satisfy
/// a `required` field, which it cannot if validation has already rejected the
/// object.
///
/// ## Two deliberate boundaries
///
/// **Only descends into fields that are PRESENT.** If `spec` itself is absent
/// and carries no `default` of its own, its children are not defaulted —
/// upstream behaves the same way, and inventing a `spec: {}` to hang defaults
/// on would fabricate an object the author never wrote.
///
/// **`null` counts as absent.** Upstream prunes an explicit null on a
/// non-nullable field and then defaults it; we do not implement pruning, so
/// treating null as absent reaches the same end state for the case that
/// matters and never leaves a nil where a controller expects a value.
pub fn apply_defaults(schema: &Value, value: &mut Value) {
    let Some(obj) = schema.as_object() else {
        return;
    };

    // Same two escape hatches validation honours: a free-form subtree is the
    // author asking us not to touch it, and int-or-string has no sub-schema
    // to default from.
    if obj
        .get("x-kubernetes-preserve-unknown-fields")
        .and_then(Value::as_bool)
        == Some(true)
        || obj
            .get("x-kubernetes-int-or-string")
            .and_then(Value::as_bool)
            == Some(true)
    {
        return;
    }

    if let Some(props) = obj.get("properties").and_then(Value::as_object)
        && let Some(map) = value.as_object_mut()
    {
        for (k, sub_schema) in props {
            let absent = map.get(k).is_none_or(Value::is_null);
            if absent {
                if let Some(d) = sub_schema.get("default") {
                    map.insert(k.clone(), d.clone());
                }
                // Whether or not a default landed, an absent field has no
                // children to descend into.
                continue;
            }
            if let Some(sub_value) = map.get_mut(k) {
                apply_defaults(sub_schema, sub_value);
            }
        }
    }

    if let Some(items) = obj.get("items")
        && let Some(arr) = value.as_array_mut()
    {
        for element in arr.iter_mut() {
            apply_defaults(items, element);
        }
    }
}

fn walk(schema: &Value, value: &Value, path: &str, out: &mut Vec<SchemaViolation>) {
    let Some(obj) = schema.as_object() else {
        return;
    };

    // `x-kubernetes-preserve-unknown-fields` marks a subtree as intentionally
    // free-form. Descending into it would reject exactly the data the author
    // asked us not to police.
    if obj
        .get("x-kubernetes-preserve-unknown-fields")
        .and_then(Value::as_bool)
        == Some(true)
    {
        return;
    }
    // `x-kubernetes-int-or-string` is upstream's escape hatch for fields like
    // `targetPort`; both types are legal by construction.
    if obj
        .get("x-kubernetes-int-or-string")
        .and_then(Value::as_bool)
        == Some(true)
    {
        return;
    }

    // A null value is how a field is absent-but-present; `required` below is
    // what makes absence an error, so do not double-report here.
    if value.is_null() {
        return;
    }

    if let Some(t) = obj.get("type").and_then(Value::as_str)
        && !type_matches(t, value)
    {
        out.push(SchemaViolation {
            path: if path.is_empty() {
                "<root>".into()
            } else {
                path.into()
            },
            expected: t.to_string(),
            found: type_name(value).to_string(),
        });
        // A type mismatch makes the children meaningless — recursing would
        // bury the real error under noise from a subtree that was never going
        // to match.
        return;
    }

    // required
    if let (Some(req), Some(map)) = (
        obj.get("required").and_then(Value::as_array),
        value.as_object(),
    ) {
        for r in req.iter().filter_map(Value::as_str) {
            if !map.contains_key(r) {
                out.push(SchemaViolation {
                    path: child_path(path, r),
                    expected: "required field".into(),
                    found: "absent".into(),
                });
            }
        }
    }

    // properties — only those the schema declares; unknown fields are ignored
    // here because PRUNING them is upstream's separate behaviour and is not
    // implemented (see the module header).
    if let (Some(props), Some(map)) = (
        obj.get("properties").and_then(Value::as_object),
        value.as_object(),
    ) {
        for (k, sub_schema) in props {
            if let Some(sub_value) = map.get(k) {
                walk(sub_schema, sub_value, &child_path(path, k), out);
            }
        }
    }

    // items — every element against the same schema.
    if let (Some(items), Some(arr)) = (obj.get("items"), value.as_array()) {
        for (i, el) in arr.iter().enumerate() {
            walk(items, el, &index_path(path, i), out);
        }
    }
}

fn child_path(parent: &str, key: &str) -> String {
    if parent.is_empty() {
        key.to_string()
    } else {
        [parent, ".", key].concat()
    }
}

fn index_path(parent: &str, i: usize) -> String {
    let mut s = parent.to_string();
    s.push('[');
    s.push_str(itoa(i).as_str());
    s.push(']');
    s
}

/// Small integer render — avoids `format!()` per ★★ TYPED EMISSION.
fn itoa(mut n: usize) -> String {
    if n == 0 {
        return "0".to_string();
    }
    let mut buf = Vec::new();
    while n > 0 {
        buf.push(b'0' + u8::try_from(n % 10).unwrap_or(0));
        n /= 10;
    }
    buf.reverse();
    String::from_utf8(buf).unwrap_or_default()
}

impl core::fmt::Display for SchemaViolation {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "{}: expected {}, found {}",
            self.path, self.expected, self.found
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn widget_schema() -> Value {
        json!({
            "type": "object",
            "properties": {
                "spec": {
                    "type": "object",
                    "required": ["size"],
                    "properties": {
                        "size": { "type": "integer" },
                        "name": { "type": "string" },
                        "tags": { "type": "array", "items": { "type": "string" } },
                    }
                }
            }
        })
    }

    /// THE measured defect: a string where the schema declares an integer.
    #[test]
    fn the_measured_defect_is_caught() {
        let v = json!({ "spec": { "size": "NOT-AN-INT" } });
        let violations = validate(&widget_schema(), &v);
        assert_eq!(violations.len(), 1, "got: {violations:?}");
        assert_eq!(violations[0].path, "spec.size");
        assert_eq!(violations[0].expected, "integer");
        assert_eq!(violations[0].found, "string");
    }

    #[test]
    fn a_valid_object_passes() {
        let v = json!({ "spec": { "size": 3, "name": "w", "tags": ["a", "b"] } });
        assert!(validate(&widget_schema(), &v).is_empty());
    }

    /// integer is accepted for `number`; a FRACTION is not accepted for
    /// `integer` — the case a naive `is_number()` check waves through.
    #[test]
    fn integer_number_asymmetry_is_respected() {
        let num = json!({ "type": "number" });
        assert!(
            validate(&num, &json!(3)).is_empty(),
            "an integer IS a number"
        );
        assert!(validate(&num, &json!(3.5)).is_empty());

        let int = json!({ "type": "integer" });
        assert!(validate(&int, &json!(3)).is_empty());
        assert_eq!(
            validate(&int, &json!(3.5)).len(),
            1,
            "a fraction is NOT an integer"
        );
    }

    #[test]
    fn required_fields_are_enforced() {
        let v = json!({ "spec": { "name": "w" } });
        let violations = validate(&widget_schema(), &v);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].path, "spec.size");
        assert_eq!(violations[0].expected, "required field");
    }

    #[test]
    fn array_items_are_validated_positionally() {
        let v = json!({ "spec": { "size": 1, "tags": ["ok", 7, "fine"] } });
        let violations = validate(&widget_schema(), &v);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].path, "spec.tags[1]", "the INDEX is named");
        assert_eq!(violations[0].found, "integer");
    }

    /// Every violation is collected — an operator fixing a manifest wants the
    /// whole list, not one error per apply cycle.
    #[test]
    fn all_violations_are_collected() {
        let v = json!({ "spec": { "size": "x", "name": 1, "tags": "not-an-array" } });
        let violations = validate(&widget_schema(), &v);
        assert_eq!(violations.len(), 3, "got: {violations:?}");
    }

    /// A type mismatch does not recurse — one clear error, not a cascade.
    #[test]
    fn a_type_mismatch_does_not_bury_itself_in_child_noise() {
        let v = json!({ "spec": "this should be an object" });
        let violations = validate(&widget_schema(), &v);
        assert_eq!(violations.len(), 1, "got: {violations:?}");
        assert_eq!(violations[0].path, "spec");
    }

    /// Free-form subtrees are respected, not policed.
    #[test]
    fn preserve_unknown_fields_is_not_policed() {
        let schema = json!({
            "type": "object",
            "properties": {
                "blob": { "x-kubernetes-preserve-unknown-fields": true }
            }
        });
        let v = json!({ "blob": { "anything": [1, "two", {"three": true}] } });
        assert!(validate(&schema, &v).is_empty());
    }

    #[test]
    fn int_or_string_accepts_both() {
        let schema = json!({
            "type": "object",
            "properties": { "port": { "x-kubernetes-int-or-string": true } }
        });
        assert!(validate(&schema, &json!({ "port": 80 })).is_empty());
        assert!(validate(&schema, &json!({ "port": "http" })).is_empty());
    }

    /// Fields the schema does not declare are IGNORED — pruning is upstream's
    /// separate behaviour and is deliberately not implemented here.
    #[test]
    fn undeclared_fields_are_ignored_not_rejected() {
        let v = json!({ "spec": { "size": 1 }, "extra": "not in the schema" });
        assert!(
            validate(&widget_schema(), &v).is_empty(),
            "unknown-field PRUNING is not implemented; do not read this as validation"
        );
    }

    #[test]
    fn violations_render_actionably() {
        let v = json!({ "spec": { "size": "x" } });
        let s = validate(&widget_schema(), &v)[0].to_string();
        assert!(s.contains("spec.size") && s.contains("integer"), "got: {s}");
    }
}

#[cfg(test)]
mod defaulting {
    use super::apply_defaults;
    use serde_json::{Value, json};

    /// The exact object that crash-looped source-controller on rio.
    #[test]
    fn a_gitrepository_gains_the_timeout_its_crd_promises() {
        let schema = json!({
            "type": "object",
            "properties": {
                "spec": {
                    "type": "object",
                    "properties": {
                        "url": {"type": "string"},
                        "interval": {"type": "string"},
                        "timeout": {"type": "string", "default": "60s"}
                    }
                }
            }
        });
        let mut obj =
            json!({"spec": {"url": "https://github.com/pleme-io/k8s", "interval": "1m0s"}});
        apply_defaults(&schema, &mut obj);
        assert_eq!(obj["spec"]["timeout"], json!("60s"));
        // Defaulting must not disturb what the author DID write.
        assert_eq!(obj["spec"]["interval"], json!("1m0s"));
    }

    /// Negative control: with no `default` in the schema there is nothing to
    /// add, so a green result above cannot come from the walk inventing
    /// fields.
    #[test]
    fn a_schema_without_defaults_adds_nothing() {
        let schema = json!({
            "type": "object",
            "properties": {"spec": {"type": "object", "properties": {"timeout": {"type": "string"}}}}
        });
        let mut obj = json!({"spec": {"url": "u"}});
        let before = obj.clone();
        apply_defaults(&schema, &mut obj);
        assert_eq!(obj, before);
    }

    /// An author's explicit value always wins — a default that overwrote it
    /// would silently discard configuration.
    #[test]
    fn an_explicit_value_is_never_overwritten() {
        let schema = json!({
            "type": "object",
            "properties": {"timeout": {"type": "string", "default": "60s"}}
        });
        let mut obj = json!({"timeout": "5m"});
        apply_defaults(&schema, &mut obj);
        assert_eq!(obj["timeout"], json!("5m"));
    }

    /// An explicit null is how a client spells "absent" through a merge
    /// patch; leaving it null hands a controller the nil this whole change
    /// exists to prevent.
    #[test]
    fn an_explicit_null_is_treated_as_absent() {
        let schema = json!({
            "type": "object",
            "properties": {"timeout": {"type": "string", "default": "60s"}}
        });
        let mut obj = json!({"timeout": Value::Null});
        apply_defaults(&schema, &mut obj);
        assert_eq!(obj["timeout"], json!("60s"));
    }

    /// An ABSENT parent stays absent. Upstream does not fabricate a `spec`
    /// to hang defaults on, and neither may we — a spec the author never
    /// wrote is not a default, it is an invention.
    #[test]
    fn an_absent_parent_object_is_not_fabricated() {
        let schema = json!({
            "type": "object",
            "properties": {
                "spec": {
                    "type": "object",
                    "properties": {"timeout": {"type": "string", "default": "60s"}}
                }
            }
        });
        let mut obj = json!({"metadata": {"name": "x"}});
        apply_defaults(&schema, &mut obj);
        assert!(obj.get("spec").is_none(), "got: {obj}");
    }

    /// Array elements are objects too, and a default inside `items` is the
    /// common shape for a container list.
    #[test]
    fn defaults_reach_inside_array_elements() {
        let schema = json!({
            "type": "object",
            "properties": {
                "containers": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "protocol": {"type": "string", "default": "TCP"}
                        }
                    }
                }
            }
        });
        let mut obj = json!({"containers": [{"name": "a"}, {"name": "b", "protocol": "UDP"}]});
        apply_defaults(&schema, &mut obj);
        assert_eq!(obj["containers"][0]["protocol"], json!("TCP"));
        assert_eq!(obj["containers"][1]["protocol"], json!("UDP"));
    }

    /// A free-form subtree is the author asking the apiserver not to touch
    /// it. Writing a default in there would corrupt data the schema
    /// deliberately declines to describe.
    #[test]
    fn a_preserve_unknown_subtree_is_left_alone() {
        let schema = json!({
            "type": "object",
            "properties": {
                "values": {
                    "x-kubernetes-preserve-unknown-fields": true,
                    "properties": {"replicas": {"default": 1}}
                }
            }
        });
        let mut obj = json!({"values": {"anything": true}});
        apply_defaults(&schema, &mut obj);
        assert!(obj["values"].get("replicas").is_none(), "got: {obj}");
    }

    /// Defaulting must run BEFORE validation, or a defaulted field cannot
    /// satisfy `required` — this pins the ordering the handler relies on.
    #[test]
    fn a_default_can_satisfy_a_required_field() {
        let schema = json!({
            "type": "object",
            "required": ["timeout"],
            "properties": {"timeout": {"type": "string", "default": "60s"}}
        });
        let mut obj = json!({});
        assert!(
            !super::validate(&schema, &obj).is_empty(),
            "negative control: undefaulted, this object must be invalid"
        );
        apply_defaults(&schema, &mut obj);
        assert!(super::validate(&schema, &obj).is_empty(), "got: {obj}");
    }
}
