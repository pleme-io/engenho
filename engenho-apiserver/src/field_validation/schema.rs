//! The field shape of every schema-backed kind, lowered once from the
//! vendored upstream `OpenAPI` v3 documents.
//!
//! Upstream decides "unknown field" by decoding into the kind's Go struct:
//! a key the struct has no field for is unknown, a key of a Go map is never
//! unknown, and a type with its own `UnmarshalJSON` (`RawExtension`,
//! `FieldsV1`) swallows whatever is inside it. The `OpenAPI` documents are
//! generated from those same structs, so each Go type has one component
//! schema here and the three cases read straight off it:
//!
//! | Go type                         | schema                                   | [`Shape`]  |
//! |---------------------------------|------------------------------------------|------------|
//! | struct                          | `properties`                             | `Struct`   |
//! | `map[string]T`                  | `additionalProperties`                   | `Map`      |
//! | slice                           | `type: array`, `items`                   | `List`     |
//! | string, int, bool, `Quantity`, `IntOrString`, `Time` | a scalar `type` or a scalar `oneOf` | `Leaf` |
//! | `RawExtension`, `FieldsV1`, `Patch` | `type: object` with neither          | `Raw`      |
//!
//! That the table is right for the served kinds is not argued, it is
//! measured: `tests/w2_typed_border.rs` walks every row's upstream roundtrip
//! fixture (k8s.io/api `testdata/HEAD`, which fills every Go field) and
//! finds no unknown field, then plants an unknown key in every struct of
//! the fixture and finds each one.
//!
//! Anything the lowering does not recognise becomes [`Shape::Raw`]: it can
//! only make a check miss an unknown field, never invent one.

use std::collections::HashMap;
use std::sync::LazyLock;

use serde_json::{Map, Value};

/// Index of a [`Shape`] in [`Schemas`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ShapeId(usize);

/// What a JSON value at one position of a kind may hold, as the Go decoder
/// sees it.
#[derive(Clone, Debug)]
pub(crate) enum Shape {
    /// A Go struct: exactly these keys, each with its own shape.
    Struct(HashMap<Box<str>, ShapeId>),
    /// A Go map: any key, every value of one shape.
    Map(ShapeId),
    /// A Go slice.
    List(ShapeId),
    /// A scalar. An object or array here is a type error, not an unknown
    /// field, so nothing inside one is examined.
    Leaf,
    /// A type that decodes its own bytes. Nothing inside is examined.
    Raw,
}

/// Every lowered shape, and the root shape of each served kind.
#[derive(Debug, Default)]
pub(crate) struct Schemas {
    shapes: Vec<Shape>,
    components: HashMap<String, ShapeId>,
    roots: HashMap<(String, String, String), ShapeId>,
}

/// The shapes of the kinds the apiserver serves with a vendored schema.
/// Built on first use; the documents are part of the binary.
pub(crate) static VENDORED: LazyLock<Schemas> = LazyLock::new(Schemas::vendored);

/// One kind's schema: where its root shape is.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Row<'s> {
    schemas: &'s Schemas,
    root: ShapeId,
}

impl<'s> Row<'s> {
    /// The root shape.
    pub(crate) fn root(self) -> ShapeId {
        self.root
    }

    /// The shape behind `id`.
    pub(crate) fn shape(self, id: ShapeId) -> &'s Shape {
        // Every `ShapeId` is handed out by `Schemas::push`, so it indexes
        // `shapes`; `Raw` is the answer that can only make a check miss.
        self.schemas.shapes.get(id.0).unwrap_or(&Shape::Raw)
    }
}

/// The schema of `group/version, kind`, when a vendored document declares
/// it. `None` for every kind served without one: the opaque catalog rows
/// and every custom resource.
#[must_use]
pub(crate) fn row(group: &str, version: &str, kind: &str) -> Option<Row<'static>> {
    VENDORED.row(group, version, kind)
}

impl Schemas {
    fn vendored() -> Self {
        let mut schemas = Self::default();
        for doc in engenho_types::openapi_v3::SERVED {
            match serde_json::from_str::<Value>(doc.body) {
                Ok(parsed) => schemas.add_document(&parsed),
                Err(error) => tracing::error!(
                    group = doc.group,
                    version = doc.version,
                    %error,
                    "a vendored OpenAPI document does not parse; its kinds get no \
                     unknown-field detection"
                ),
            }
        }
        schemas
    }

    fn row(&self, group: &str, version: &str, kind: &str) -> Option<Row<'_>> {
        let key = (group.to_owned(), version.to_owned(), kind.to_owned());
        self.roots.get(&key).map(|&root| Row {
            schemas: self,
            root,
        })
    }

    /// Lower every kind one document declares. A component already lowered
    /// from an earlier document is reused: every document is cut from the
    /// same release, so a shared type (`ObjectMeta`) is the same in each.
    fn add_document(&mut self, doc: &Value) {
        let Some(components) = doc
            .pointer("/components/schemas")
            .and_then(Value::as_object)
        else {
            return;
        };
        for (name, schema) in components {
            let Some(gvks) = schema
                .get("x-kubernetes-group-version-kind")
                .and_then(Value::as_array)
            else {
                continue;
            };
            let root = self.component(name, components);
            for gvk in gvks {
                let field = |f: &str| gvk.get(f).and_then(Value::as_str).unwrap_or_default();
                self.roots
                    .entry((
                        field("group").to_owned(),
                        field("version").to_owned(),
                        field("kind").to_owned(),
                    ))
                    .or_insert(root);
            }
        }
    }

    fn push(&mut self, shape: Shape) -> ShapeId {
        self.shapes.push(shape);
        ShapeId(self.shapes.len() - 1)
    }

    /// The shape of the component `name`. Registered before it is lowered,
    /// so a type that refers to itself terminates.
    fn component(&mut self, name: &str, components: &Map<String, Value>) -> ShapeId {
        if let Some(&id) = self.components.get(name) {
            return id;
        }
        let id = self.push(Shape::Raw);
        self.components.insert(name.to_owned(), id);
        let shape = match components.get(name) {
            Some(schema) => match reference(schema) {
                Some(target) => {
                    let target = self.component(target, components);
                    self.shapes.get(target.0).cloned().unwrap_or(Shape::Raw)
                }
                None => self.lower(schema, components),
            },
            None => Shape::Raw,
        };
        if let Some(slot) = self.shapes.get_mut(id.0) {
            *slot = shape;
        }
        id
    }

    /// The shape of a schema node that may be a reference.
    fn node(&mut self, schema: &Value, components: &Map<String, Value>) -> ShapeId {
        if let Some(name) = reference(schema) {
            return self.component(name, components);
        }
        let shape = self.lower(schema, components);
        self.push(shape)
    }

    /// The shape of a schema node that is not a reference.
    fn lower(&mut self, schema: &Value, components: &Map<String, Value>) -> Shape {
        if schema
            .get("x-kubernetes-preserve-unknown-fields")
            .and_then(Value::as_bool)
            == Some(true)
        {
            return Shape::Raw;
        }
        if schema
            .get("x-kubernetes-int-or-string")
            .and_then(Value::as_bool)
            == Some(true)
        {
            return Shape::Leaf;
        }
        // `Quantity` and `IntOrString` are a `oneOf` of scalars. A union
        // that could hold an object is not modelled: Raw.
        if let Some(branches) = schema
            .get("oneOf")
            .or_else(|| schema.get("anyOf"))
            .and_then(Value::as_array)
        {
            let scalar = branches.iter().all(|b| {
                matches!(
                    b.get("type").and_then(Value::as_str),
                    Some("string" | "integer" | "number" | "boolean")
                )
            });
            return if scalar { Shape::Leaf } else { Shape::Raw };
        }
        let properties = schema.get("properties").and_then(Value::as_object);
        let values = schema.get("additionalProperties");
        match (
            schema.get("type").and_then(Value::as_str),
            properties,
            values,
        ) {
            (Some("array"), _, _) => {
                let items = match schema.get("items") {
                    Some(items) => self.node(items, components),
                    None => self.push(Shape::Raw),
                };
                Shape::List(items)
            }
            (Some("string" | "integer" | "number" | "boolean"), _, _) => Shape::Leaf,
            (Some("object") | None, Some(properties), None) => {
                let mut fields = HashMap::with_capacity(properties.len());
                for (field, schema) in properties {
                    let id = self.node(schema, components);
                    fields.insert(field.as_str().into(), id);
                }
                Shape::Struct(fields)
            }
            (Some("object") | None, None, Some(values)) => {
                let id = match values {
                    Value::Object(_) => self.node(values, components),
                    _ => self.push(Shape::Raw),
                };
                Shape::Map(id)
            }
            _ => Shape::Raw,
        }
    }
}

/// The component a schema node names, through `$ref` or the one-element
/// `allOf` the documents wrap a described reference in.
fn reference(schema: &Value) -> Option<&str> {
    let target = match schema.get("$ref") {
        Some(r) => r,
        None => match schema.get("allOf").and_then(Value::as_array)?.as_slice() {
            [only] => only.get("$ref")?,
            _ => return None,
        },
    };
    target.as_str()?.strip_prefix("#/components/schemas/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_served_catalog_row_has_a_schema() {
        for d in engenho_types::generated_v1_34::RESOURCE_CATALOG {
            let found = row(d.group, d.version, d.kind).is_some();
            assert_eq!(
                found, !d.opaque,
                "{}/{} {}: a schema-backed row resolves, an opaque one does not",
                d.group, d.version, d.kind
            );
        }
    }

    #[test]
    fn the_three_go_decoding_cases_lower_to_three_shapes() {
        let pod = row("", "v1", "Pod").expect("Pod is served with a schema");
        let Shape::Struct(fields) = pod.shape(pod.root()) else {
            panic!("a Pod is a struct");
        };
        let meta = fields.get("metadata").copied().expect("metadata");
        let Shape::Struct(meta) = pod.shape(meta) else {
            panic!("ObjectMeta is a struct");
        };
        for field in [
            "ownerReferences",
            "generateName",
            "selfLink",
            "managedFields",
        ] {
            assert!(meta.contains_key(field), "ObjectMeta declares {field}");
        }
        let labels = meta.get("labels").copied().expect("labels");
        assert!(
            matches!(pod.shape(labels), Shape::Map(_)),
            "labels is a map"
        );
        let managed = meta.get("managedFields").copied().expect("managedFields");
        let Shape::List(entry) = pod.shape(managed) else {
            panic!("managedFields is a list");
        };
        let Shape::Struct(entry) = pod.shape(*entry) else {
            panic!("a managedFields entry is a struct");
        };
        let fields_v1 = entry.get("fieldsV1").copied().expect("fieldsV1");
        assert!(
            matches!(pod.shape(fields_v1), Shape::Raw),
            "FieldsV1 is raw"
        );
    }

    #[test]
    fn a_quantity_is_a_scalar_that_admits_a_number_or_a_string() {
        let quota = row("", "v1", "ResourceQuota").expect("ResourceQuota is served");
        let Shape::Struct(root) = quota.shape(quota.root()) else {
            panic!("struct");
        };
        let Shape::Struct(spec) = quota.shape(root["spec"]) else {
            panic!("struct");
        };
        let Shape::Map(value) = quota.shape(spec["hard"]) else {
            panic!("a ResourceList is a map");
        };
        assert!(matches!(quota.shape(*value), Shape::Leaf));
    }
}
