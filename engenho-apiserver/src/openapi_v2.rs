use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

use prost::Message;
use serde_json::{Map, Value, json};

use crate::accept;

pub const CONTENT_TYPE_PROTOBUF: &str =
    "application/com.github.proto-openapi.spec.v2.v1.0+protobuf";

const SUBTYPE_PROTOBUF: &str = "com.github.proto-openapi.spec.v2.v1.0+protobuf";
const SUBTYPE_PROTOBUF_DEPRECATED: &str = "com.github.proto-openapi.spec.v2@v1.0+protobuf";
const OBJECT_META: &str = "io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta";
const GVK_EXTENSION: &str = "x-kubernetes-group-version-kind";
const TYPES: [&str; 6] = ["object", "array", "string", "integer", "number", "boolean"];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encoding {
    Json,
    Protobuf,
}

impl Encoding {
    #[must_use]
    pub const fn content_type(self) -> &'static str {
        match self {
            Self::Json => "application/json",
            Self::Protobuf => CONTENT_TYPE_PROTOBUF,
        }
    }
}

#[must_use]
pub fn negotiate(accept_header: Option<&str>) -> Option<Encoding> {
    let header = match accept_header {
        Some(h) if !h.is_empty() => h,
        _ => "*/*",
    };
    let offered = [
        ("json", Encoding::Json),
        (SUBTYPE_PROTOBUF_DEPRECATED, Encoding::Protobuf),
        (SUBTYPE_PROTOBUF, Encoding::Protobuf),
    ];
    accept::ranked(header).iter().find_map(|range| {
        offered.iter().find_map(|&(subtype, encoding)| {
            let kind = range.kind == "*" || range.kind.eq_ignore_ascii_case("application");
            let sub = range.subtype == "*" || range.subtype.eq_ignore_ascii_case(subtype);
            (kind && sub).then_some(encoding)
        })
    })
}

pub struct CustomResource<'a> {
    pub group: &'a str,
    pub version: &'a str,
    pub kind: &'a str,
    pub schema: &'a Value,
}

static BUILT_IN: LazyLock<BTreeMap<String, Value>> = LazyLock::new(built_in);

fn built_in() -> BTreeMap<String, Value> {
    let docs: Vec<Value> = engenho_types::openapi_v3::SERVED
        .iter()
        .filter_map(|d| serde_json::from_str(d.body).ok())
        .collect();
    let mut components: BTreeMap<&str, &Value> = BTreeMap::new();
    for doc in &docs {
        if let Some(schemas) = doc
            .pointer("/components/schemas")
            .and_then(Value::as_object)
        {
            for (name, schema) in schemas {
                components.entry(name.as_str()).or_insert(schema);
            }
        }
    }
    let known: BTreeSet<&str> = components.keys().copied().collect();
    components
        .into_iter()
        .map(|(name, schema)| (name.to_owned(), lower(schema, &known)))
        .collect()
}

#[must_use]
pub fn definition_name(group: &str, version: &str, kind: &str) -> String {
    let mut parts: Vec<&str> = group.split('.').filter(|p| !p.is_empty()).rev().collect();
    parts.push(version);
    parts.push(kind);
    parts.join(".")
}

fn custom_resource_definition(cr: &CustomResource<'_>) -> Value {
    let mut out = if flag(cr.schema, "x-kubernetes-preserve-unknown-fields") {
        Map::from_iter([("type".to_owned(), json!("object"))])
    } else {
        let mut out = lower_node(cr.schema, &BTreeSet::new());
        add_type_meta(&mut out);
        out
    };
    out.insert(
        GVK_EXTENSION.to_owned(),
        json!([{ "group": cr.group, "kind": cr.kind, "version": cr.version }]),
    );
    Value::Object(out)
}

fn add_type_meta(out: &mut Map<String, Value>) {
    let properties = out
        .entry("properties")
        .or_insert_with(|| Value::Object(Map::new()));
    if let Value::Object(properties) = properties {
        properties.insert(
            "metadata".to_owned(),
            json!({ "$ref": (["#/definitions/", OBJECT_META].concat()) }),
        );
        properties.insert("apiVersion".to_owned(), json!({ "type": "string" }));
        properties.insert("kind".to_owned(), json!({ "type": "string" }));
    }
}

fn flag(schema: &Value, key: &str) -> bool {
    schema.get(key).and_then(Value::as_bool) == Some(true)
}

fn reference(schema: &Map<String, Value>) -> Option<&str> {
    let target = match schema.get("$ref") {
        Some(r) => r,
        None => match schema.get("allOf").and_then(Value::as_array)?.as_slice() {
            [only] => only.get("$ref")?,
            _ => return None,
        },
    };
    let target = target.as_str()?;
    target
        .strip_prefix("#/components/schemas/")
        .or_else(|| target.strip_prefix("#/definitions/"))
}

fn lower(schema: &Value, known: &BTreeSet<&str>) -> Value {
    Value::Object(lower_node(schema, known))
}

fn lower_node(schema: &Value, known: &BTreeSet<&str>) -> Map<String, Value> {
    let Some(node) = schema.as_object() else {
        return Map::new();
    };
    let mut out = Map::new();
    for (key, value) in node {
        let kept = key == "description"
            || key == "format"
            || (key.starts_with("x-kubernetes-") && key != "x-kubernetes-validations");
        if kept {
            out.insert(key.clone(), value.clone());
        }
    }
    if let Some(target) = reference(node) {
        if known.contains(target) {
            out.insert(
                "$ref".to_owned(),
                Value::String(["#/definitions/", target].concat()),
            );
        }
        return out;
    }
    if flag(schema, "nullable") {
        return out;
    }
    let preserve = flag(schema, "x-kubernetes-preserve-unknown-fields");
    let mut ty = node
        .get("type")
        .and_then(Value::as_str)
        .filter(|t| TYPES.contains(t));
    if preserve && ty == Some("object") {
        ty = None;
    }
    let properties = node.get("properties").and_then(Value::as_object);
    if !preserve {
        if let Some(properties) = properties {
            let lowered: Map<String, Value> = properties
                .iter()
                .map(|(name, s)| (name.clone(), lower(s, known)))
                .collect();
            out.insert("properties".to_owned(), Value::Object(lowered));
        }
        if let Some(items) = node.get("items").filter(|i| i.is_object()) {
            out.insert("items".to_owned(), lower(items, known));
        }
    }
    if ty == Some("array") && !out.contains_key("items") {
        ty = None;
    }
    if let Some(ty) = ty {
        out.insert("type".to_owned(), Value::String(ty.to_owned()));
    }
    let nullable_values = match node.get("additionalProperties") {
        Some(Value::Bool(b)) => {
            out.insert("additionalProperties".to_owned(), Value::Bool(*b));
            false
        }
        Some(values @ Value::Object(_)) => {
            out.insert("additionalProperties".to_owned(), lower(values, known));
            flag(values, "nullable")
        }
        _ => false,
    };
    if let Some(required) = node.get("required").and_then(Value::as_array)
        && !nullable_values
    {
        let nullable_field = |f: &str| {
            properties
                .and_then(|p| p.get(f))
                .is_some_and(|s| flag(s, "nullable"))
        };
        let kept: Vec<Value> = required
            .iter()
            .filter_map(Value::as_str)
            .filter(|f| !nullable_field(f))
            .map(|f| Value::String(f.to_owned()))
            .collect();
        if !kept.is_empty() {
            out.insert("required".to_owned(), Value::Array(kept));
        }
    }
    if flag(schema, "x-kubernetes-embedded-resource") && !preserve {
        add_type_meta(&mut out);
        let mut required: Vec<Value> = out
            .get("required")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for field in ["kind", "apiVersion"] {
            if !required.iter().any(|r| r == field) {
                required.push(Value::String(field.to_owned()));
            }
        }
        out.insert("required".to_owned(), Value::Array(required));
    }
    out
}

fn custom_definitions(custom: &[CustomResource<'_>]) -> BTreeMap<String, Value> {
    custom
        .iter()
        .map(|cr| {
            (
                definition_name(cr.group, cr.version, cr.kind),
                custom_resource_definition(cr),
            )
        })
        .collect()
}

#[must_use]
pub fn json_document(git_version: &str, custom: &[CustomResource<'_>]) -> Value {
    let mut definitions = BUILT_IN.clone();
    definitions.extend(custom_definitions(custom));
    json!({
        "swagger": "2.0",
        "info": { "title": "Kubernetes", "version": git_version },
        "paths": {},
        "definitions": definitions,
    })
}

static BUILT_IN_PB: LazyLock<Vec<pb::NamedSchema>> = LazyLock::new(|| {
    BUILT_IN
        .iter()
        .map(|(name, schema)| named_schema(name, schema))
        .collect()
});

fn named_schema(name: &str, schema: &Value) -> pb::NamedSchema {
    pb::NamedSchema {
        name: name.to_owned(),
        value: Some(to_pb(schema)),
    }
}

#[must_use]
pub fn protobuf_document(git_version: &str, custom: &[CustomResource<'_>]) -> Vec<u8> {
    let custom = custom_definitions(custom);
    let mut definitions: Vec<pb::NamedSchema> = BUILT_IN_PB
        .iter()
        .filter(|d| !custom.contains_key(&d.name))
        .cloned()
        .collect();
    definitions.extend(
        custom
            .iter()
            .map(|(name, schema)| named_schema(name, schema)),
    );
    definitions.sort_by(|a, b| a.name.cmp(&b.name));
    pb::Document {
        swagger: "2.0".to_owned(),
        info: Some(pb::Info {
            title: "Kubernetes".to_owned(),
            version: git_version.to_owned(),
        }),
        paths: Some(pb::Paths::default()),
        definitions: Some(pb::Definitions {
            additional_properties: definitions,
        }),
    }
    .encode_to_vec()
}

fn to_pb(schema: &Value) -> pb::Schema {
    let text = |key: &str| {
        schema
            .get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    let vendor_extension = schema
        .as_object()
        .map(|o| {
            o.iter()
                .filter(|(k, _)| k.starts_with("x-"))
                .map(|(k, v)| pb::NamedAny {
                    name: k.clone(),
                    value: Some(pb::Any {
                        yaml: v.to_string(),
                    }),
                })
                .collect()
        })
        .unwrap_or_default();
    pb::Schema {
        xref: text("$ref"),
        format: text("format"),
        description: text("description"),
        required: schema
            .get("required")
            .and_then(Value::as_array)
            .map(|r| {
                r.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default(),
        additional_properties: match schema.get("additionalProperties") {
            Some(Value::Bool(b)) => Some(Box::new(pb::AdditionalPropertiesItem {
                oneof: Some(pb::additional_properties_item::Oneof::Boolean(*b)),
            })),
            Some(values @ Value::Object(_)) => Some(Box::new(pb::AdditionalPropertiesItem {
                oneof: Some(pb::additional_properties_item::Oneof::Schema(Box::new(
                    to_pb(values),
                ))),
            })),
            _ => None,
        },
        r#type: schema
            .get("type")
            .and_then(Value::as_str)
            .map(|t| pb::TypeItem {
                value: vec![t.to_owned()],
            }),
        items: schema.get("items").map(|items| pb::ItemsItem {
            schema: vec![to_pb(items)],
        }),
        properties: schema
            .get("properties")
            .and_then(Value::as_object)
            .map(|p| pb::Properties {
                additional_properties: p.iter().map(|(name, s)| named_schema(name, s)).collect(),
            }),
        vendor_extension,
    }
}

pub mod pb {
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Document {
        #[prost(string, tag = "1")]
        pub swagger: String,
        #[prost(message, optional, tag = "2")]
        pub info: Option<Info>,
        #[prost(message, optional, tag = "8")]
        pub paths: Option<Paths>,
        #[prost(message, optional, tag = "9")]
        pub definitions: Option<Definitions>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Info {
        #[prost(string, tag = "1")]
        pub title: String,
        #[prost(string, tag = "2")]
        pub version: String,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Paths {
        #[prost(message, repeated, tag = "1")]
        pub vendor_extension: Vec<NamedAny>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Definitions {
        #[prost(message, repeated, tag = "1")]
        pub additional_properties: Vec<NamedSchema>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct NamedSchema {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(message, optional, tag = "2")]
        pub value: Option<Schema>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Schema {
        #[prost(string, tag = "1")]
        pub xref: String,
        #[prost(string, tag = "2")]
        pub format: String,
        #[prost(string, tag = "4")]
        pub description: String,
        #[prost(string, repeated, tag = "19")]
        pub required: Vec<String>,
        #[prost(message, optional, boxed, tag = "21")]
        pub additional_properties: Option<Box<AdditionalPropertiesItem>>,
        #[prost(message, optional, tag = "22")]
        pub r#type: Option<TypeItem>,
        #[prost(message, optional, tag = "23")]
        pub items: Option<ItemsItem>,
        #[prost(message, optional, tag = "25")]
        pub properties: Option<Properties>,
        #[prost(message, repeated, tag = "31")]
        pub vendor_extension: Vec<NamedAny>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct TypeItem {
        #[prost(string, repeated, tag = "1")]
        pub value: Vec<String>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ItemsItem {
        #[prost(message, repeated, tag = "1")]
        pub schema: Vec<Schema>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Properties {
        #[prost(message, repeated, tag = "1")]
        pub additional_properties: Vec<NamedSchema>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct AdditionalPropertiesItem {
        #[prost(oneof = "additional_properties_item::Oneof", tags = "1, 2")]
        pub oneof: Option<additional_properties_item::Oneof>,
    }

    pub mod additional_properties_item {
        #[derive(Clone, PartialEq, prost::Oneof)]
        pub enum Oneof {
            #[prost(message, tag = "1")]
            Schema(Box<super::Schema>),
            #[prost(bool, tag = "2")]
            Boolean(bool),
        }
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct NamedAny {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(message, optional, tag = "2")]
        pub value: Option<Any>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Any {
        #[prost(string, tag = "2")]
        pub yaml: String,
    }
}
