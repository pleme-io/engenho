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
const PROVIDED: &[&str] = &["ObjectMeta", "ListMeta", "TypeMeta"];

/// Field-type overrides for prose-only enums: upstream types these as a plain
/// `string` but the value set is closed (described only in prose, no `enum`
/// array). The generator can't derive the enum, so it REFERENCES a curated
/// hand-authored enum from `engenho_types::curated_enums`. Tuple:
/// (owning OpenAPI schema key, wire field name, Rust type to emit verbatim).
/// The override suppresses `$ref` collection for that field.
const FIELD_OVERRIDES: &[(&str, &str, &str)] = &[
    (
        "io.k8s.api.core.v1.PodStatus",
        "phase",
        "Option<crate::curated_enums::PodPhase>",
    ),
    (
        "io.k8s.api.core.v1.Secret",
        "type",
        "Option<crate::curated_enums::SecretType>",
    ),
];

/// Kubernetes types that marshal as a SCALAR, not as an object.
///
/// Their OpenAPI schemas carry no `properties` — the wire form is defined by
/// hand-written Go `MarshalJSON`/`UnmarshalJSON`, which the schema cannot
/// express. A generic emitter therefore produces `pub struct Quantity {}`,
/// which is syntactically fine and **rejects every real value**:
///
/// ```text
/// invalid type: string "1Gi", expected struct Quantity
/// ```
///
/// Measured 2026-08-30 against the reference pangea deployment: an
/// `emptyDir` with `sizeLimit: 1Gi` failed to parse, so the Postgres pod
/// stayed Pending forever. All five of these were empty structs, and each
/// one silently breaks a whole family of manifests — every resource
/// request/limit (`Quantity`), every `targetPort: http` or
/// `maxUnavailable: 25%` (`IntOrString`), every nested timestamp (`Time`,
/// `MicroTime`), every webhook payload and CRD default (`RawExtension`).
///
/// The override is by SCHEMA KEY, not by bare name, so a same-named type in
/// another group cannot be caught by accident.
const SCALAR_TYPE_OVERRIDES: &[(&str, &str)] = &[
    (
        "io.k8s.apimachinery.pkg.api.resource.Quantity",
        "/// `Quantity` — a fixed-point number on the wire as a STRING (`\"1Gi\"`,\n\
         /// `\"100m\"`, `\"1.5\"`). Kept as the literal wire text rather than\n\
         /// parsed: comparing quantities needs suffix-aware arithmetic, and a\n\
         /// lossy round-trip through f64 would change bytes we must echo back\n\
         /// verbatim in a GET.\n\
         #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]\n\
         #[serde(transparent)]\n\
         pub struct Quantity(pub String);\n",
    ),
    (
        "io.k8s.apimachinery.pkg.util.intstr.IntOrString",
        "/// `IntOrString` — an int OR a string on the wire. `targetPort: 8080`\n\
         /// and `targetPort: http` are both legal, as are `maxUnavailable: 1`\n\
         /// and `maxUnavailable: \"25%\"`.\n\
         #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]\n\
         #[serde(untagged)]\n\
         pub enum IntOrString {\n\
         \x20   Int(i64),\n\
         \x20   Str(String),\n\
         }\n\
         \n\
         impl Default for IntOrString {\n\
         \x20   fn default() -> Self {\n\
         \x20       Self::Int(0)\n\
         \x20   }\n\
         }\n",
    ),
    (
        "io.k8s.apimachinery.pkg.apis.meta.v1.Time",
        "/// `Time` — RFC3339 on the wire, as a string.\n\
         #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]\n\
         #[serde(transparent)]\n\
         pub struct Time(pub String);\n",
    ),
    (
        "io.k8s.apimachinery.pkg.apis.meta.v1.MicroTime",
        "/// `MicroTime` — RFC3339 with microseconds, on the wire as a string.\n\
         #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]\n\
         #[serde(transparent)]\n\
         pub struct MicroTime(pub String);\n",
    ),
    (
        "io.k8s.apimachinery.pkg.runtime.RawExtension",
        "/// `RawExtension` — arbitrary JSON held verbatim. Used for webhook\n\
         /// payloads and CRD defaults, where the shape is not knowable here.\n\
         #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]\n\
         #[serde(transparent)]\n\
         pub struct RawExtension(pub serde_json::Value);\n",
    ),
];

/// The hand-written body for a scalar-marshalled type, if `owner_key` names one.
#[must_use]
pub fn scalar_type_override(owner_key: &str) -> Option<&'static str> {
    SCALAR_TYPE_OVERRIDES
        .iter()
        .find(|(k, _)| *k == owner_key)
        .map(|(_, body)| *body)
}

/// Look up a field-type override for `(owner_key, wire_field)`.
fn field_override(owner_key: &str, wire_field: &str) -> Option<&'static str> {
    FIELD_OVERRIDES
        .iter()
        .find(|(o, f, _)| *o == owner_key && *f == wire_field)
        .map(|(_, _, t)| *t)
}

/// Field-name shapes a TOP-LEVEL KIND handles structurally rather than as
/// data fields: `apiVersion`/`kind` are TypeMeta (carried separately by the
/// wire envelope, not stored), and `metadata` is the provided `ObjectMeta`.
///
/// ★ ONLY TRUE FOR A TOP-LEVEL KIND. On a nested struct these are ordinary
/// data: `RoleRef.kind` is `"Role"` or `"ClusterRole"`, `Subject.kind` is
/// `"User"`/`"Group"`/`"ServiceAccount"`, and `ObjectReference` carries BOTH
/// `apiVersion` and `kind` as real fields. Stripping them there silently
/// deletes load-bearing data — which is why `emit_fields` now takes
/// [`EnvelopePolicy`] instead of deciding for itself.
///
/// A purely structural test does not work: "has apiVersion AND kind" would
/// correctly classify `RoleRef` and then wrongly strip `ObjectReference`.
/// The caller knows whether it is emitting a kind; the callee cannot.
fn is_envelope_field(name: &str) -> bool {
    matches!(name, "apiVersion" | "kind")
}

/// Emit fields for a TOP-LEVEL KIND (TypeMeta stripped).
///
/// Kept as the name every existing kind-emitter call site already uses, so
/// the policy is opt-in at the two places that actually needed it rather
/// than a mechanical edit across the file.
#[must_use]
pub fn emit_fields(
    owner_key: &str,
    properties: &BTreeMap<String, serde_json::Value>,
    required: &[String],
    refs: &mut BTreeSet<String>,
) -> String {
    emit_fields_with(
        owner_key,
        properties,
        required,
        refs,
        EnvelopePolicy::StripTypeMeta,
    )
}

/// Whether `apiVersion`/`kind` are envelope (a top-level kind) or data (a
/// nested struct).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvelopePolicy {
    /// Top-level kind: strip TypeMeta, it is carried by the wire envelope.
    StripTypeMeta,
    /// Nested struct: `apiVersion`/`kind` are ordinary data fields.
    KeepAllFields,
}

/// Rust keywords that can be raw identifiers (`r#type`). K8s hits `type`,
/// `continue`, `ref`, `enum`, … as field names; without this they'd be
/// syntax errors in the generated struct.
const RAW_KEYWORDS: &[&str] = &[
    "as", "break", "const", "continue", "dyn", "else", "enum", "extern", "false", "fn", "for",
    "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref", "return",
    "static", "struct", "trait", "true", "type", "unsafe", "use", "where", "while", "async",
    "await", "box", "do", "final", "macro", "override", "priv", "typeof", "unsized", "virtual",
    "yield", "try", "gen",
];

/// Keywords that CANNOT be raw identifiers — must be suffixed with `_`.
const NON_RAW_KEYWORDS: &[&str] = &["self", "Self", "super", "crate"];

/// Map a wire field name to (Rust identifier, Some(wire-name) if a serde
/// `rename` is needed). Handles camelCase→snake_case AND Rust-keyword
/// collisions (raw `r#kw` where legal, `kw_` otherwise) — the generated
/// struct always parses, the wire name is always preserved.
#[must_use]
pub fn field_ident(wire: &str) -> (String, Option<String>) {
    let snake = snake_case(wire);
    if NON_RAW_KEYWORDS.contains(&snake.as_str()) {
        (format!("{snake}_"), Some(wire.to_string()))
    } else if RAW_KEYWORDS.contains(&snake.as_str()) {
        (format!("r#{snake}"), Some(wire.to_string()))
    } else if snake == wire {
        (snake, None)
    } else {
        (snake.clone(), Some(wire.to_string()))
    }
}

/// camelCase → snake_case for Rust field names (serde `rename` preserves the
/// wire name). `automountServiceAccountToken` → `automount_service_account_token`.
#[must_use]
pub fn snake_case(camel: &str) -> String {
    let chars: Vec<char> = camel.chars().collect();
    let mut out = String::with_capacity(camel.len() + 4);
    for i in 0..chars.len() {
        let c = chars[i];
        if c.is_ascii_uppercase() {
            // A word boundary precedes this uppercase when the previous char
            // is lower/digit (`pod|IP`), OR an acronym run ends at a following
            // lowercase (`IP|Address`). This keeps acronyms intact: `podIP` →
            // `pod_ip`, `clusterIP` → `cluster_ip`, `IPAddress` → `ip_address`.
            let prev_lower_or_digit =
                i > 0 && (chars[i - 1].is_ascii_lowercase() || chars[i - 1].is_ascii_digit());
            // ...EXCEPT when the "following lowercase" is a lone trailing
            // `s`, which pluralises the acronym rather than starting a word.
            // `nonResourceURLs` is one word plus a plural, not `...UR` + `Ls`.
            //
            // This bug produced `non_resource_ur_ls`, and rather than being
            // fixed it was worked around by hand-writing
            // `generated_v1_34/rbac_v1/policy.rs` INSIDE a directory headed
            // GENERATED — DO NOT EDIT. That single workaround made the
            // --check determinism gate permanently red, which hid 40 files
            // of formatting drift and two more hand-authored files behind
            // it, and made a routine regeneration delete code the apiserver
            // needs to compile. One mangled identifier, three files of
            // debt, one dead gate.
            let plural_s_at_end = i + 1 == chars.len() - 1 && chars[i + 1] == 's';
            let acronym_end = i > 0
                && chars[i - 1].is_ascii_uppercase()
                && i + 1 < chars.len()
                && chars[i + 1].is_ascii_lowercase()
                && !plural_s_at_end;
            if prev_lower_or_digit || acronym_end {
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
pub fn emit_fields_with(
    owner_key: &str,
    properties: &BTreeMap<String, serde_json::Value>,
    required: &[String],
    refs: &mut BTreeSet<String>,
    envelope: EnvelopePolicy,
) -> String {
    let mut out = String::new();
    for (name, schema) in properties {
        if envelope == EnvelopePolicy::StripTypeMeta && is_envelope_field(name) {
            continue;
        }
        let (field, rename_to) = field_ident(name);
        let doc = schema
            .get("description")
            .and_then(serde_json::Value::as_str)
            .map(|d| format!("{}\n", rustdoc(d, "    ")))
            .unwrap_or_default();

        // Curated-enum override: emit the referenced type verbatim, collect no
        // ref. Every override type here is `Option<…>` → skip-if-none.
        if let Some(ovr) = field_override(owner_key, name) {
            let rename = rename_to
                .map(|w| format!(", rename = \"{w}\""))
                .unwrap_or_default();
            out.push_str(&doc);
            out.push_str(&format!(
                "    #[serde(default{rename}, skip_serializing_if = \"Option::is_none\")]\n"
            ));
            out.push_str(&format!("    pub {field}: {ovr},\n"));
            continue;
        }

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
        let rename = rename_to
            .map(|w| format!(", rename = \"{w}\""))
            .unwrap_or_default();
        // Optional (non-required) scalars/refs become Option<T>; collections
        // (Vec/Map) skip when empty; required stays bare.
        let (decl_ty, skip) = match (&ty, is_required) {
            (RustType::Vec(_), _) => (ty.render(), ", skip_serializing_if = \"Vec::is_empty\""),
            (RustType::Map(_), _) => (
                ty.render(),
                ", skip_serializing_if = \"std::collections::BTreeMap::is_empty\"",
            ),
            (_, true) => (ty.render(), ""),
            (_, false) => (
                format!("Option<{}>", ty.render()),
                ", skip_serializing_if = \"Option::is_none\"",
            ),
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
pub fn emit_struct(name: &str, owner_key: &str, shape: &SchemaView) -> (String, BTreeSet<String>) {
    // A scalar-marshalled Kubernetes type is emitted verbatim: its schema
    // has no properties, so the generic path below would produce an empty
    // struct that rejects every real value. See SCALAR_TYPE_OVERRIDES.
    if let Some(body) = scalar_type_override(owner_key) {
        return (body.to_string(), BTreeSet::new());
    }
    let mut refs = BTreeSet::new();
    // A sub-struct is NOT a kind: `RoleRef.kind` and `Subject.kind` are
    // data, and stripping them is what made hand-written replacements
    // necessary in the generated directory.
    let fields = emit_fields_with(
        owner_key,
        &shape.properties,
        &shape.required,
        &mut refs,
        EnvelopePolicy::KeepAllFields,
    );
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
) -> Vec<(String, String, SchemaView)> {
    let mut queue: Vec<String> = seeds.iter().cloned().collect();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    // Keyed by Rust type name → (full schema key, shape). BTreeMap →
    // deterministic, sorted-by-name emission order.
    let mut collected: BTreeMap<String, (String, SchemaView)> = BTreeMap::new();
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
        // Discover this sub-struct's own refs (overrides suppress some).
        let mut sub_refs = BTreeSet::new();
        let _ = emit_fields(&key, &shape.properties, &shape.required, &mut sub_refs);
        for r in sub_refs {
            if !seen.contains(&r) {
                queue.push(r);
            }
        }
        collected.insert(name, (key, shape.clone()));
    }
    collected
        .into_iter()
        .map(|(name, (key, shape))| (name, key, shape))
        .collect()
}

/// Collect the global, deduplicated sub-struct closure across every catalog
/// kind (`kind_keys` = their OpenAPI keys), excluding the kinds themselves
/// (`kind_names`). Shared sub-structs (`PodSpec`, `LabelSelector`,
/// `PodTemplateSpec`, …) are emitted ONCE into a shared module rather than
/// duplicated per kind — so a single canonical type is referenced everywhere
/// (Deployment's `PodTemplateSpec` IS Pod's `PodSpec`'s container, not a
/// distinct copy). Deterministic, sorted by Rust type name.
#[must_use]
pub fn shared_substructs(
    kind_keys: &[&str],
    kind_names: &[&str],
    schemas: &BTreeMap<String, SchemaView>,
) -> Vec<(String, String, SchemaView)> {
    let mut seeds = BTreeSet::new();
    for k in kind_keys {
        if let Some(v) = schemas.get(*k) {
            let mut refs = BTreeSet::new();
            let _ = emit_fields(k, &v.properties, &v.required, &mut refs);
            seeds.extend(refs);
        }
    }
    transitive_structs(&seeds, schemas)
        .into_iter()
        .filter(|(name, _, _)| !kind_names.contains(&name.as_str()))
        .collect()
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
mod snake_case_acronym_tests {
    use super::snake_case;

    /// An acronym followed by a PLURAL `s` is one word, not two.
    ///
    /// REGRESSION: `nonResourceURLs` produced `non_resource_ur_ls`. The
    /// wire name was right (`#[serde(rename)]` carries it), so the defect
    /// was invisible on the wire and fatal in Rust — `authz` writes
    /// `non_resource_urls` and could not compile against the generated
    /// type. It was worked around with a hand-written file inside the
    /// generated directory instead of being fixed here.
    #[test]
    fn an_acronym_plus_plural_s_is_one_word() {
        assert_eq!(snake_case("nonResourceURLs"), "non_resource_urls");
        assert_eq!(snake_case("externalIPs"), "external_ips");
        assert_eq!(snake_case("clusterIPs"), "cluster_ips");
        assert_eq!(
            snake_case("serverAddressByClientCIDRs"),
            "server_address_by_client_cidrs"
        );
    }

    /// The original acronym rule must still hold — an acronym followed by a
    /// real WORD still splits.
    #[test]
    fn an_acronym_plus_a_word_still_splits() {
        assert_eq!(snake_case("IPAddress"), "ip_address");
        assert_eq!(snake_case("TLSConfig"), "tls_config");
        assert_eq!(snake_case("HTTPSPort"), "https_port");
    }

    /// And the ordinary cases are unchanged.
    #[test]
    fn ordinary_camel_case_is_unchanged() {
        assert_eq!(snake_case("podIP"), "pod_ip");
        assert_eq!(snake_case("nodeName"), "node_name");
        assert_eq!(snake_case("apiGroups"), "api_groups");
        assert_eq!(snake_case("resourceNames"), "resource_names");
        assert_eq!(snake_case("verbs"), "verbs");
    }
}

#[cfg(test)]
mod scalar_override_tests {
    use super::{SCALAR_TYPE_OVERRIDES, SchemaView, emit_struct, scalar_type_override};

    /// Every Kubernetes type that marshals as a scalar is overridden.
    ///
    /// Their schemas have no `properties`, so the generic emitter produces
    /// `pub struct X {}` — which compiles and rejects every real value with
    /// `invalid type: string "1Gi", expected struct Quantity`.
    #[test]
    fn all_five_scalar_types_are_overridden() {
        for key in [
            "io.k8s.apimachinery.pkg.api.resource.Quantity",
            "io.k8s.apimachinery.pkg.util.intstr.IntOrString",
            "io.k8s.apimachinery.pkg.apis.meta.v1.Time",
            "io.k8s.apimachinery.pkg.apis.meta.v1.MicroTime",
            "io.k8s.apimachinery.pkg.runtime.RawExtension",
        ] {
            assert!(
                scalar_type_override(key).is_some(),
                "{key} must be overridden or it emits an empty struct"
            );
        }
    }

    /// The emitted bodies carry the right wire representation, and NONE of
    /// them is an empty struct.
    #[test]
    fn no_override_emits_an_empty_struct() {
        for (key, body) in SCALAR_TYPE_OVERRIDES {
            assert!(
                !body.contains("{}"),
                "{key} still emits an empty struct: {body}"
            );
            assert!(
                body.contains("(pub ") || body.contains("pub enum"),
                "{key} must be a newtype or an enum: {body}"
            );
        }
    }

    /// Quantity keeps the literal wire text. Parsing to a number would be a
    /// lossy round-trip, and a GET must echo the submitted bytes back.
    #[test]
    fn quantity_is_a_transparent_string_newtype() {
        let body = scalar_type_override("io.k8s.apimachinery.pkg.api.resource.Quantity").unwrap();
        assert!(body.contains("pub struct Quantity(pub String);"));
        assert!(body.contains("#[serde(transparent)]"));
    }

    /// IntOrString must accept BOTH forms — `targetPort: 8080` and
    /// `targetPort: http` are each legal in the same field.
    #[test]
    fn int_or_string_is_an_untagged_enum_over_both_forms() {
        let body = scalar_type_override("io.k8s.apimachinery.pkg.util.intstr.IntOrString").unwrap();
        assert!(body.contains("#[serde(untagged)]"));
        assert!(body.contains("Int(i64)"));
        assert!(body.contains("Str(String)"));
    }

    /// The override fires from emit_struct, and takes precedence over the
    /// generic property-driven path.
    #[test]
    fn emit_struct_routes_through_the_override() {
        let empty = SchemaView::default();
        let (src, refs) = emit_struct(
            "Quantity",
            "io.k8s.apimachinery.pkg.api.resource.Quantity",
            &empty,
        );
        assert!(src.contains("pub struct Quantity(pub String);"), "{src}");
        assert!(refs.is_empty(), "an override references no other schema");

        // A normal type with no properties is still an empty struct — the
        // override is keyed, not a blanket rule.
        let (plain, _) = emit_struct("SomeThing", "io.k8s.api.core.v1.SomeThing", &empty);
        assert!(plain.contains("pub struct SomeThing {"), "{plain}");
    }
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
        assert_eq!(
            snake_case("automountServiceAccountToken"),
            "automount_service_account_token"
        );
        assert_eq!(snake_case("metadata"), "metadata");
        // Acronyms stay intact (K8s podIP/clusterIP/hostIP/IPAddress).
        assert_eq!(snake_case("clusterIP"), "cluster_ip");
        assert_eq!(snake_case("podIP"), "pod_ip");
        assert_eq!(snake_case("hostIP"), "host_ip");
        assert_eq!(snake_case("IPAddress"), "ip_address");
    }

    #[test]
    fn rust_keyword_fields_become_raw_or_suffixed_with_rename() {
        // `type` (K8s ServicePort.protocol-ish, EndpointPort, etc.) → r#type.
        assert_eq!(
            field_ident("type"),
            ("r#type".to_string(), Some("type".to_string()))
        );
        // `continue` → r#continue.
        assert_eq!(
            field_ident("continue"),
            ("r#continue".to_string(), Some("continue".to_string()))
        );
        // non-raw-able keyword → suffixed.
        assert_eq!(
            field_ident("self"),
            ("self_".to_string(), Some("self".to_string()))
        );
        // plain field → no rename.
        assert_eq!(field_ident("name"), ("name".to_string(), None));
        // camelCase → rename.
        assert_eq!(
            field_ident("nodeName"),
            ("node_name".to_string(), Some("nodeName".to_string()))
        );
    }

    #[test]
    fn field_override_emits_curated_enum_not_string() {
        // Secret.type is `string` in OpenAPI but overridden to the curated enum.
        let mut refs = BTreeSet::new();
        let f = emit_fields(
            "io.k8s.api.core.v1.Secret",
            &view(json!({"type": {"type": "string"}}), &[]).properties,
            &[],
            &mut refs,
        );
        assert!(f.contains("pub r#type: Option<crate::curated_enums::SecretType>"));
        assert!(f.contains("rename = \"type\""));
        // override suppresses ref collection (it's a string in OpenAPI anyway).
        assert!(refs.is_empty());
        // Without the matching owner, no override → plain String.
        let mut refs2 = BTreeSet::new();
        let g = emit_fields(
            "io.k8s.api.core.v1.NotSecret",
            &view(json!({"type": {"type": "string"}}), &[]).properties,
            &[],
            &mut refs2,
        );
        assert!(g.contains("pub r#type: Option<String>"));
    }

    #[test]
    fn keyword_field_emitted_as_raw_ident() {
        let mut refs = BTreeSet::new();
        let f = emit_fields(
            "",
            &view(json!({"type": {"type": "string"}}), &[]).properties,
            &[],
            &mut refs,
        );
        assert!(f.contains("rename = \"type\""));
        assert!(f.contains("pub r#type: Option<String>"));
    }

    #[test]
    fn envelope_and_metadata_fields_special_cased() {
        let mut refs = BTreeSet::new();
        let f = emit_fields(
            "",
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
            "",
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
            "",
            &view(
                json!({"replicas": {"type": "integer", "format": "int32"}}),
                &["replicas"],
            )
            .properties,
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
            "io.k8s.api.core.v1.LocalObjectReference",
            &view(json!({"name": {"type": "string"}}), &[]),
        );
        assert!(src.contains("pub struct LocalObjectReference {"));
        assert!(src.contains("pub name: Option<String>"));
        assert!(
            src.contains("#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]")
        );
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
        let names: Vec<&str> = structs.iter().map(|(n, _, _)| n.as_str()).collect();
        assert!(names.contains(&"A"));
        assert!(names.contains(&"B")); // transitively pulled
        assert!(!names.contains(&"ObjectMeta")); // provided → skipped
        assert!(!names.contains(&"Z")); // missing → skipped (Json-fallback'd)
    }
}
