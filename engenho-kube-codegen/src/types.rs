//! OpenAPI v3 schema → Rust type mapping (the M0.0.4 typed-emission core).
//!
//! This is brick 1 of the typed-emission engine: a pure function that maps a
//! single OpenAPI property schema to the Rust type a generated field should
//! carry, and collects every `$ref`'d schema key it encounters so the emitter
//! can later emit those referenced kinds as transitive typed sub-structs.
//!
//! It does NOT yet wire into `emit.rs` — that integration (which flips the
//! generated `spec`/`status` from opaque `serde_json::Value` to typed structs)
//! is the next brick. Keeping the mapping a standalone, unit-tested primitive
//! means the wider `engenho-types` generated tree + the `--check` determinism
//! gate are untouched by this brick.

use serde_json::Value;
use std::collections::BTreeSet;

/// The Rust type an OpenAPI property maps to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RustType {
    /// A primitive: `String`, `i64`, `i32`, `bool`, `f64`.
    Scalar(&'static str),
    /// A reference to another generated kind/sub-struct (e.g. `PodSpec`,
    /// `ObjectReference`). The String is the bare Rust type name.
    Ref(String),
    /// `Vec<inner>` — an OpenAPI `array`.
    Vec(Box<RustType>),
    /// `std::collections::BTreeMap<String, inner>` — an OpenAPI `object`
    /// with `additionalProperties`.
    Map(Box<RustType>),
    /// `serde_json::Value` — the safe fallback for shapes the typed mapper
    /// does not (yet) model precisely (e.g. `x-kubernetes-int-or-string`,
    /// untyped free-form objects, `anyOf`/`oneOf`).
    Json,
}

impl RustType {
    /// Render this type as the Rust source token a field declaration uses.
    /// `Ref` names are emitted bare — the emitter is responsible for the
    /// corresponding `use`/module path.
    #[must_use]
    pub fn render(&self) -> String {
        match self {
            RustType::Scalar(s) => (*s).to_string(),
            RustType::Ref(name) => name.clone(),
            RustType::Vec(inner) => format!("Vec<{}>", inner.render()),
            RustType::Map(inner) => {
                format!("std::collections::BTreeMap<String, {}>", inner.render())
            }
            RustType::Json => "serde_json::Value".to_string(),
        }
    }

    /// True if this type (transitively) bottoms out in a `Ref` — i.e. it
    /// pulls in a sub-struct the emitter must also generate.
    #[must_use]
    pub fn has_ref(&self) -> bool {
        match self {
            RustType::Ref(_) => true,
            RustType::Vec(inner) | RustType::Map(inner) => inner.has_ref(),
            RustType::Scalar(_) | RustType::Json => false,
        }
    }
}

/// `io.k8s.api.core.v1.PodSpec` → `PodSpec`. The Rust type name is the last
/// dotted segment of the OpenAPI schema key.
#[must_use]
pub fn type_name_from_ref(ref_str: &str) -> String {
    // `$ref` values are `#/components/schemas/<key>`; the key is dotted.
    let key = ref_str.rsplit('/').next().unwrap_or(ref_str);
    key.rsplit('.').next().unwrap_or(key).to_string()
}

/// Map an OpenAPI property schema to a [`RustType`], inserting every
/// referenced schema KEY (the full dotted `io.k8s.…` key, not the bare name)
/// into `refs` so the caller can transitively emit those sub-structs.
///
/// Handles the property shapes K8s actually uses: `$ref`, single-`$ref`
/// `allOf` (how K8s wraps a typed object with a description/default),
/// `string`/`integer`/`number`/`boolean` scalars (honoring `int32`/`int64`
/// format), `array` (→ `Vec`), and `object` with `additionalProperties`
/// (→ `BTreeMap`). Everything else falls back to [`RustType::Json`] — a
/// safe, always-compiling default the later bricks tighten.
#[must_use]
pub fn map_schema(schema: &Value, refs: &mut BTreeSet<String>) -> RustType {
    // 1. Direct $ref.
    if let Some(r) = schema.get("$ref").and_then(Value::as_str) {
        return ref_type(r, refs);
    }
    // 2. allOf: [{ $ref }] — K8s wraps a typed object that also carries a
    //    description/default. Take the first $ref'd member.
    if let Some(all_of) = schema.get("allOf").and_then(Value::as_array) {
        if let Some(r) = all_of
            .iter()
            .find_map(|m| m.get("$ref").and_then(Value::as_str))
        {
            return ref_type(r, refs);
        }
    }
    // 3. Typed scalars / arrays / maps.
    match schema.get("type").and_then(Value::as_str) {
        Some("string") => RustType::Scalar("String"),
        Some("boolean") => RustType::Scalar("bool"),
        Some("number") => RustType::Scalar("f64"),
        Some("integer") => match schema.get("format").and_then(Value::as_str) {
            Some("int32") => RustType::Scalar("i32"),
            _ => RustType::Scalar("i64"),
        },
        Some("array") => {
            let inner = schema
                .get("items")
                .map_or(RustType::Json, |items| map_schema(items, refs));
            RustType::Vec(Box::new(inner))
        }
        Some("object") => match schema.get("additionalProperties") {
            // `additionalProperties: { <schema> }` → typed map value.
            Some(ap) if ap.is_object() => RustType::Map(Box::new(map_schema(ap, refs))),
            // `additionalProperties: true` / object with inline `properties`
            // (a nameless nested object) → untyped for now.
            _ => RustType::Json,
        },
        // No `type` and no `$ref`/`allOf` — `x-kubernetes-int-or-string`,
        // `anyOf`/`oneOf`, or genuinely free-form. Safe fallback.
        _ => RustType::Json,
    }
}

/// Resolve a `$ref` string into a [`RustType::Ref`], recording the full
/// schema key in `refs`.
fn ref_type(ref_str: &str, refs: &mut BTreeSet<String>) -> RustType {
    let key = ref_str.rsplit('/').next().unwrap_or(ref_str).to_string();
    refs.insert(key);
    RustType::Ref(type_name_from_ref(ref_str))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn map(v: Value) -> (RustType, BTreeSet<String>) {
        let mut refs = BTreeSet::new();
        let t = map_schema(&v, &mut refs);
        (t, refs)
    }

    #[test]
    fn scalars() {
        assert_eq!(map(json!({"type": "string"})).0, RustType::Scalar("String"));
        assert_eq!(map(json!({"type": "boolean"})).0, RustType::Scalar("bool"));
        assert_eq!(map(json!({"type": "number"})).0, RustType::Scalar("f64"));
        assert_eq!(map(json!({"type": "integer"})).0, RustType::Scalar("i64"));
        assert_eq!(
            map(json!({"type": "integer", "format": "int32"})).0,
            RustType::Scalar("i32")
        );
    }

    #[test]
    fn direct_ref_records_key_and_names_type() {
        let (t, refs) = map(json!({"$ref": "#/components/schemas/io.k8s.api.core.v1.PodSpec"}));
        assert_eq!(t, RustType::Ref("PodSpec".into()));
        assert!(refs.contains("io.k8s.api.core.v1.PodSpec"));
        assert_eq!(t.render(), "PodSpec");
        assert!(t.has_ref());
    }

    #[test]
    fn allof_single_ref_unwraps() {
        // How K8s expresses `metadata` (a typed ObjectMeta with a default).
        let (t, refs) = map(json!({
            "allOf": [{"$ref": "#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"}],
            "default": {},
            "description": "Standard object metadata."
        }));
        assert_eq!(t, RustType::Ref("ObjectMeta".into()));
        assert!(refs.contains("io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"));
    }

    #[test]
    fn array_of_ref_is_vec_and_collects_inner_ref() {
        // ServiceAccount.imagePullSecrets shape.
        let (t, refs) = map(json!({
            "type": "array",
            "items": {"$ref": "#/components/schemas/io.k8s.api.core.v1.LocalObjectReference"}
        }));
        assert_eq!(t, RustType::Vec(Box::new(RustType::Ref("LocalObjectReference".into()))));
        assert_eq!(t.render(), "Vec<LocalObjectReference>");
        assert!(refs.contains("io.k8s.api.core.v1.LocalObjectReference"));
        assert!(t.has_ref());
    }

    #[test]
    fn array_of_scalar_is_vec_no_ref() {
        let (t, refs) = map(json!({"type": "array", "items": {"type": "string"}}));
        assert_eq!(t, RustType::Vec(Box::new(RustType::Scalar("String"))));
        assert_eq!(t.render(), "Vec<String>");
        assert!(refs.is_empty());
        assert!(!t.has_ref());
    }

    #[test]
    fn object_with_typed_additional_properties_is_map() {
        // `labels`/`annotations`-style: object → BTreeMap<String, V>.
        let (t, _) = map(json!({"type": "object", "additionalProperties": {"type": "string"}}));
        assert_eq!(t, RustType::Map(Box::new(RustType::Scalar("String"))));
        assert_eq!(t.render(), "std::collections::BTreeMap<String, String>");
    }

    #[test]
    fn int_or_string_and_freeform_fall_back_to_json() {
        // x-kubernetes-int-or-string (no `type`).
        assert_eq!(map(json!({"x-kubernetes-int-or-string": true})).0, RustType::Json);
        // additionalProperties: true.
        assert_eq!(map(json!({"type": "object", "additionalProperties": true})).0, RustType::Json);
        // genuinely empty.
        assert_eq!(map(json!({})).0, RustType::Json);
        assert_eq!(RustType::Json.render(), "serde_json::Value");
        assert!(!RustType::Json.has_ref());
    }

    #[test]
    fn nested_array_of_map_renders_and_propagates_ref() {
        let (t, refs) = map(json!({
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": {"$ref": "#/components/schemas/io.k8s.api.core.v1.ResourceClaim"}
            }
        }));
        assert_eq!(
            t.render(),
            "Vec<std::collections::BTreeMap<String, ResourceClaim>>"
        );
        assert!(t.has_ref());
        assert!(refs.contains("io.k8s.api.core.v1.ResourceClaim"));
    }

    #[test]
    fn type_name_from_ref_takes_last_segment() {
        assert_eq!(
            type_name_from_ref("#/components/schemas/io.k8s.api.core.v1.PodSpec"),
            "PodSpec"
        );
        assert_eq!(type_name_from_ref("io.k8s.api.apps.v1.DeploymentStatus"), "DeploymentStatus");
    }
}
