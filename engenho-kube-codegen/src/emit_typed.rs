//! Typed-struct emission engine (M0.0.4 brick 2).
//!
//! Brick 1 ([`crate::types`]) maps ONE property schema to a [`RustType`].
//! This module composes that into whole `struct`s: it emits a kind (or a
//! referenced sub-struct) as typed Rust fields, then recursively emits every
//! `$ref`'d sub-struct it pulls in — the transitive closure — from a merged
//! OpenAPI schema map. apimachinery types engenho-types hand-provides
//! (`ObjectMeta`, …) are REFERENCED, never re-emitted. Anything the mapper
//! can't model bottoms out at `serde_json::Value`, so output always compiles.
//!
//! Like brick 1 this is a standalone, unit-tested primitive — it does not yet
//! drive the on-disk generator (that wiring + regenerate is the next brick),
//! so the generated tree + `--check` are untouched here.

use std::collections::{BTreeMap, BTreeSet};

use crate::types::{RustType, map_schema, type_name_from_ref};

/// apimachinery / engenho-types-provided types: referenced by generated
/// code, never emitted (they are hand-authored in engenho-types). Keyed by
/// the BARE Rust name (last dotted segment of the OpenAPI key).
const PROVIDED: &[&str] = &[
    "ObjectMeta",
    "ListMeta",
    "TypeMeta",
];

/// Field-name shapes the kind struct handles structurally rather than as
/// data fields: `apiVersion`/`kind` are TypeMeta (carried separately by the
/// wire envelope, not stored), and `metadata` is the provided `ObjectMeta`.
fn is_envelope_field(name: &str) -> bool {
    matches!(name, "apiVersion" | "kind")
}

/// camelCase → snake_case for Rust field names (serde `rename` preserves the
/// wire name). `automountServiceAccountToken` → `automount_service_account_token`.
#[must_use]
pub fn snake_case(camel: &str) -> String {
    let mut out = String::with_capacity(camel.len() + 4);
    for (i, c) in camel.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i != 0 {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// First paragraph of an OpenAPI description, line-trimmed into `///` rustdoc
/// (no 4-space indent that rustdoc would treat as a code block).
fn rustdoc(desc: &str, indent: &str) -> String {
    if desc.is_empty() {
        return String::new();
    }
    let first = desc.split("\n\n").next().unwrap_or(desc);
    first
        .lines()
        .map(|l| {
            let t = l.trim_start();
            if t.is_empty() {
                format!("{indent}///")
            } else {
                format!("{indent}/// {t}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Emit the field lines of a struct body from an OpenAPI schema's
/// `properties` + `required`, collecting referenced schema KEYS into `refs`.
/// `metadata` → the provided `ObjectMeta`; `apiVersion`/`kind` are skipped.
#[must_use]
pub fn emit_fields(
    properties: &BTreeMap<String, serde_json::Value>,
    required: &[String],
    refs: &mut BTreeSet<String>,
) -> String {
    let mut out = String::new();
    for (name, schema) in properties {
        if is_envelope_field(name) {
            continue;
        }
        let field = snake_case(name);
        let doc = schema
            .get("description")
            .and_then(serde_json::Value::as_str)
            .map(|d| format!("{}\n", rustdoc(d, "    ")))
            .unwrap_or_default();

        if name == "metadata" {
            // Provided ObjectMeta — referenced, with the project's standard
            // empty-meta skip. Record nothing in refs (hand-authored).
            out.push_str(&doc);
            out.push_str("    #[serde(default, skip_serializing_if = \"is_empty_meta\")]\n");
            out.push_str("    pub metadata: crate::meta::ObjectMeta,\n");
            continue;
        }

        let ty = map_schema(schema, refs);
        let is_required = required.iter().any(|r| r == name);
        let rename = if field == *name {
            String::new()
        } else {
            format!(", rename = \"{name}\"")
        };
        // Optional (non-required) scalars/refs become Option<T>; collections
        // (Vec/Map) skip when empty; required stays bare.
        let (decl_ty, skip) = match (&ty, is_required) {
            (RustType::Vec(_), _) => (ty.render(), ", skip_serializing_if = \"Vec::is_empty\""),
            (RustType::Map(_), _) => (
                ty.render(),
                ", skip_serializing_if = \"std::collections::BTreeMap::is_empty\"",
            ),
            (_, true) => (ty.render(), ""),
            (_, false) => (format!("Option<{}>", ty.render()), ", skip_serializing_if = \"Option::is_none\""),
        };
        out.push_str(&doc);
        out.push_str(&format!("    #[serde(default{rename}{skip})]\n"));
        out.push_str(&format!("    pub {field}: {decl_ty},\n"));
    }
    out
}

/// Emit a single `pub struct {name}` from a schema, returning its source +
/// the set of referenced schema keys (for transitive emission).
#[must_use]
pub fn emit_struct(name: &str, shape: &SchemaView) -> (String, BTreeSet<String>) {
    let mut refs = BTreeSet::new();
    let fields = emit_fields(&shape.properties, &shape.required, &mut refs);
    let doc = rustdoc(&shape.description, "");
    let doc = if doc.is_empty() {
        format!("/// `{name}` — generated typed struct.")
    } else {
        doc
    };
    let body = format!(
        "{doc}\n#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]\npub struct {name} {{\n{fields}}}\n",
    );
    (body, refs)
}

/// Compute the transitive closure of sub-structs to emit for a set of seed
/// schema keys, in deterministic (sorted) order, skipping PROVIDED types and
/// keys absent from the merged schema (those were Json-fallback'd already).
#[must_use]
pub fn transitive_structs(
    seeds: &BTreeSet<String>,
    schemas: &BTreeMap<String, SchemaView>,
) -> Vec<(String, SchemaView)> {
    let mut queue: Vec<String> = seeds.iter().cloned().collect();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut collected: BTreeMap<String, SchemaView> = BTreeMap::new();
    while let Some(key) = queue.pop() {
        if !seen.insert(key.clone()) {
            continue;
        }
        let name = type_name_from_ref(&key);
        if PROVIDED.contains(&name.as_str()) {
            continue;
        }
        let Some(shape) = schemas.get(&key) else {
            continue; // unresolved → was Json-fallback'd at the field
        };
        // Discover this sub-struct's own refs.
        let mut sub_refs = BTreeSet::new();
        let _ = emit_fields(&shape.properties, &shape.required, &mut sub_refs);
        for r in sub_refs {
            if !seen.contains(&r) {
                queue.push(r);
            }
        }
        collected.insert(name, shape.clone());
    }
    // BTreeMap → sorted by Rust type name (deterministic emission order).
    collected.into_iter().collect()
}

/// A view of an OpenAPI schema body — the subset the emitter needs. Mirrors
/// `openapi::KindShape` but lives here so the engine is unit-testable without
/// the full parser.
#[derive(Debug, Clone, Default)]
pub struct SchemaView {
    pub description: String,
    pub properties: BTreeMap<String, serde_json::Value>,
    pub required: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn view(props: serde_json::Value, required: &[&str]) -> SchemaView {
        let properties = props
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        SchemaView {
            description: "A test kind.".into(),
            properties,
            required: required.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    #[test]
    fn snake_case_handles_k8s_camel() {
        assert_eq!(snake_case("automountServiceAccountToken"), "automount_service_account_token");
        assert_eq!(snake_case("metadata"), "metadata");
        assert_eq!(snake_case("clusterIP"), "cluster_i_p");
    }

    #[test]
    fn envelope_and_metadata_fields_special_cased() {
        let mut refs = BTreeSet::new();
        let f = emit_fields(
            &view(
                json!({
                    "apiVersion": {"type": "string"},
                    "kind": {"type": "string"},
                    "metadata": {"allOf": [{"$ref": "#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"}]}
                }),
                &[],
            )
            .properties,
            &[],
            &mut refs,
        );
        assert!(!f.contains("pub api_version")); // apiVersion skipped
        assert!(!f.contains("pub kind")); // kind skipped
        assert!(f.contains("pub metadata: crate::meta::ObjectMeta"));
    }

    #[test]
    fn serviceaccount_shape_types_correctly() {
        let mut refs = BTreeSet::new();
        let f = emit_fields(
            &view(
                json!({
                    "automountServiceAccountToken": {"type": "boolean"},
                    "imagePullSecrets": {"type": "array", "items": {"$ref": "#/components/schemas/io.k8s.api.core.v1.LocalObjectReference"}},
                    "secrets": {"type": "array", "items": {"$ref": "#/components/schemas/io.k8s.api.core.v1.ObjectReference"}}
                }),
                &[],
            )
            .properties,
            &[],
            &mut refs,
        );
        // optional bool → Option<bool> + rename
        assert!(f.contains("rename = \"automountServiceAccountToken\""));
        assert!(f.contains("pub automount_service_account_token: Option<bool>"));
        // arrays → Vec, skip-if-empty, rename
        assert!(f.contains("pub image_pull_secrets: Vec<LocalObjectReference>"));
        assert!(f.contains("skip_serializing_if = \"Vec::is_empty\""));
        assert!(f.contains("pub secrets: Vec<ObjectReference>"));
        // both refs collected for transitive emission
        assert!(refs.contains("io.k8s.api.core.v1.LocalObjectReference"));
        assert!(refs.contains("io.k8s.api.core.v1.ObjectReference"));
    }

    #[test]
    fn required_field_is_bare_not_option() {
        let mut refs = BTreeSet::new();
        let f = emit_fields(
            &view(json!({"replicas": {"type": "integer", "format": "int32"}}), &["replicas"]).properties,
            &["replicas".to_string()],
            &mut refs,
        );
        assert!(f.contains("pub replicas: i32"));
        assert!(!f.contains("Option<i32>"));
    }

    #[test]
    fn emit_struct_wraps_fields_in_decl() {
        let (src, refs) = emit_struct(
            "LocalObjectReference",
            &view(json!({"name": {"type": "string"}}), &[]),
        );
        assert!(src.contains("pub struct LocalObjectReference {"));
        assert!(src.contains("pub name: Option<String>"));
        assert!(src.contains("#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]"));
        assert!(refs.is_empty());
    }

    #[test]
    fn transitive_closure_walks_refs_skips_provided_and_missing() {
        let mut schemas = BTreeMap::new();
        // A → B (present), A → ObjectMeta (provided), A → Z (missing)
        schemas.insert(
            "io.k8s.api.core.v1.A".to_string(),
            view(
                json!({
                    "b": {"$ref": "#/components/schemas/io.k8s.api.core.v1.B"},
                    "meta": {"$ref": "#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"},
                    "z": {"$ref": "#/components/schemas/io.k8s.api.core.v1.Z"}
                }),
                &[],
            ),
        );
        schemas.insert(
            "io.k8s.api.core.v1.B".to_string(),
            view(json!({"name": {"type": "string"}}), &[]),
        );
        let mut seeds = BTreeSet::new();
        seeds.insert("io.k8s.api.core.v1.A".to_string());
        let structs = transitive_structs(&seeds, &schemas);
        let names: Vec<&str> = structs.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"A"));
        assert!(names.contains(&"B")); // transitively pulled
        assert!(!names.contains(&"ObjectMeta")); // provided → skipped
        assert!(!names.contains(&"Z")); // missing → skipped (Json-fallback'd)
    }
}
