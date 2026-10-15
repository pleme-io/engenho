//! Server-side printing — `meta.k8s.io/v1` `Table` conversion.
//!
//! Measured 2026-08-28: a request carrying
//! `Accept: application/json;as=Table;v=1;g=meta.k8s.io` got a plain
//! `NodeList` back, and `grep -rn 'columnDefinitions|as=Table|includeObject'`
//! over the whole workspace returned **nothing**. Table conversion did not
//! exist.
//!
//! It is not a nicety. `kubectl get` and k9s both ask for a Table and let the
//! SERVER decide the columns — that is what makes `kubectl get pods` render
//! the same columns against any cluster, and it is why k9s could not draw a
//! useful row for engenho even once objects existed.
//!
//! ## Columns
//!
//! Each kind's columns and cells come from [`crate::printer::Printer`]: one
//! data row per built-in kind upstream prints (`printers/internalversion`),
//! a CRD's own `additionalPrinterColumns`, and `NAME`/`AGE` for anything
//! else. `AGE` and every other duration cell is rendered on the server as
//! kubectl's `duration.HumanDuration` ("44d"), as upstream sends it.

use serde::Serialize;
use serde_json::Value;

/// What the client asked to be embedded in each row's `object` field.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum IncludeObject {
    /// `includeObject=None` — no object, just cells.
    None,
    /// `includeObject=Metadata` (the default) — a `PartialObjectMetadata`.
    #[default]
    Metadata,
    /// `includeObject=Object` — the whole object.
    Object,
}

impl IncludeObject {
    /// Parse the `includeObject` query parameter. Unknown values fall back to
    /// the upstream default rather than erroring, matching upstream's
    /// tolerance here.
    #[must_use]
    pub fn parse(raw: Option<&str>) -> Self {
        match raw {
            Some(v) if v.eq_ignore_ascii_case("None") => Self::None,
            Some(v) if v.eq_ignore_ascii_case("Object") => Self::Object,
            _ => Self::Metadata,
        }
    }
}

pub use crate::printer::{ColumnDefinition, Printer};

/// One row: the cells, plus optionally the object they came from.
#[derive(Serialize, Clone, Debug)]
pub struct TableRow {
    /// Cell values, positionally matching `columnDefinitions`.
    pub cells: Vec<Value>,
    /// Row conditions (`Completed` for a finished pod).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Value>,
    /// The source object, per `includeObject`. Omitted for `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<Value>,
}

/// A `meta.k8s.io/v1` Table.
#[derive(Serialize, Clone, Debug)]
pub struct Table {
    /// Always `"Table"`.
    pub kind: &'static str,
    /// Always `"meta.k8s.io/v1"`.
    #[serde(rename = "apiVersion")]
    pub api_version: &'static str,
    /// Carries the source list's `resourceVersion` so a client can watch on.
    pub metadata: Value,
    /// The columns, in order.
    #[serde(rename = "columnDefinitions")]
    pub column_definitions: Vec<ColumnDefinition>,
    /// One row per object.
    pub rows: Vec<TableRow>,
}

/// Project an object (or a List) into the `meta.k8s.io/v1` metadata-only
/// shape a client requested via `Accept: …;as=PartialObjectMetadataList`.
///
/// **This is a CONVERSION, not a hint.** A client that asks for
/// `PartialObjectMetadataList` installs a decoder for exactly that kind, so
/// serving it the full List is not a generous superset — it is undecodable.
/// Measured on rio 2026-09-15: engenho ignored the `as=` parameter, Flux's
/// kustomize-controller received a normal list, and its metadata-only cache
/// failed to start with `unable to decode returned object as
/// PartialObjectMetadataList`, so the controller never reconciled anything.
/// Nothing in that chain named the apiserver.
///
/// A List becomes a `PartialObjectMetadataList` (each item stripped to its
/// metadata); a single object becomes a `PartialObjectMetadata`.
#[must_use]
pub fn to_partial_object_metadata(value: &Value) -> Value {
    let is_list = value
        .get("kind")
        .and_then(Value::as_str)
        .is_some_and(|k| k.ends_with("List"))
        || value.get("items").is_some_and(Value::is_array);

    if !is_list {
        return partial_object_metadata(value);
    }

    let items: Vec<Value> = value
        .get("items")
        .and_then(Value::as_array)
        .map(|arr| arr.iter().map(partial_object_metadata).collect())
        .unwrap_or_default();

    serde_json::json!({
        "kind": "PartialObjectMetadataList",
        "apiVersion": "meta.k8s.io/v1",
        // The list-level metadata carries resourceVersion, which is what a
        // reflector resumes its watch from. Dropping it would make every
        // metadata-only informer re-list from scratch on each sync.
        "metadata": value.get("metadata").cloned().unwrap_or_else(|| serde_json::json!({})),
        "items": items,
    })
}

/// Build the `PartialObjectMetadata` upstream embeds for
/// `includeObject=Metadata` — the object's `metadata` and nothing else, which
/// is the whole point of asking for a Table rather than a List.
fn partial_object_metadata(obj: &Value) -> Value {
    serde_json::json!({
        "kind": "PartialObjectMetadata",
        "apiVersion": "meta.k8s.io/v1",
        "metadata": obj.get("metadata").cloned().unwrap_or_else(|| serde_json::json!({})),
    })
}

fn row_for(obj: &Value, include: IncludeObject, printer: &Printer, now: i64) -> TableRow {
    TableRow {
        cells: printer.cells(obj, now),
        conditions: printer.conditions(obj),
        object: match include {
            IncludeObject::None => None,
            IncludeObject::Metadata => Some(partial_object_metadata(obj)),
            IncludeObject::Object => Some(obj.clone()),
        },
    }
}

/// Convert a List **or** a single object into a Table, with `printer`'s
/// columns and every duration cell measured against `now` (unix seconds).
///
/// A single-object GET is a one-row Table upstream, not an error — `kubectl get
/// pod foo` asks for exactly that.
#[must_use]
pub fn to_table(value: &Value, include: IncludeObject, printer: &Printer, now: i64) -> Table {
    let items = value.get("items").and_then(|i| i.as_array());
    let (rows, metadata) = match items {
        Some(items) => (
            items
                .iter()
                .map(|o| row_for(o, include, printer, now))
                .collect(),
            value
                .get("metadata")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({})),
        ),
        None => (
            vec![row_for(value, include, printer, now)],
            value.pointer("/metadata/resourceVersion").map_or_else(
                || serde_json::json!({}),
                |rv| serde_json::json!({ "resourceVersion": rv }),
            ),
        ),
    };
    Table {
        kind: "Table",
        api_version: "meta.k8s.io/v1",
        metadata,
        column_definitions: printer.columns(),
        rows,
    }
}

/// Whether an `Accept` header asks for a Table.
///
/// kubectl sends
/// `application/json;as=Table;v=1;g=meta.k8s.io,application/json`, so the
/// check is per media-range: the Table range must be found before falling back
/// to plain JSON. Matching on the whole header string would make any request
/// listing a Table alternative render as a Table.
#[must_use]
pub fn accept_wants_table(accept: &str) -> bool {
    accept.split(',').any(|range| {
        let lower = range.to_ascii_lowercase();
        lower.contains("as=table") && lower.contains("g=meta.k8s.io")
    })
}

/// Whether any `Accept` range asks for the metadata-only projection.
///
/// Matches both `as=PartialObjectMetadataList` (a list) and
/// `as=PartialObjectMetadata` (a single object); `to_partial_object_metadata`
/// picks the shape from the payload rather than from the header, so one
/// predicate covers both. Both are gated on `g=meta.k8s.io` exactly as the
/// Table check is — `as=` without the group is not this conversion.
#[must_use]
pub fn accept_wants_partial_metadata(accept: &str) -> bool {
    accept.split(',').any(|range| {
        let lower = range.to_ascii_lowercase();
        lower.contains("as=partialobjectmetadata") && lower.contains("g=meta.k8s.io")
    })
}

#[cfg(test)]
mod tests {

    use super::*;

    fn list() -> Value {
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMapList",
            "metadata": { "resourceVersion": "42" },
            "items": [
                { "metadata": { "name": "a", "creationTimestamp": "2026-08-28T21:00:00Z" } },
                { "metadata": { "name": "b", "creationTimestamp": "2026-08-28T21:05:00Z" } },
            ]
        })
    }

    #[test]
    fn accept_detection_is_per_media_range() {
        assert!(accept_wants_table(
            "application/json;as=Table;v=1;g=meta.k8s.io,application/json"
        ));
        assert!(accept_wants_table(
            "application/json;as=Table;v=1;g=meta.k8s.io"
        ));
        assert!(!accept_wants_table("application/json"));
        assert!(!accept_wants_table(""));
        // `as=Table` without the meta.k8s.io group is not a Table request.
        assert!(!accept_wants_table("application/json;as=Table;v=1"));
    }

    const NOW: i64 = 1_787_950_920;

    fn table(value: &Value, include: IncludeObject) -> Table {
        to_table(value, include, &Printer::for_kind("", "ConfigMap"), NOW)
    }

    #[test]
    fn a_list_becomes_one_row_per_item() {
        let t = table(&list(), IncludeObject::Metadata);
        assert_eq!(t.kind, "Table");
        assert_eq!(t.api_version, "meta.k8s.io/v1");
        assert_eq!(t.rows.len(), 2);
        assert_eq!(t.column_definitions.len(), 3);
        assert_eq!(t.rows[0].cells[0], serde_json::json!("a"));
        assert_eq!(t.rows[0].cells[2], serde_json::json!("2m"));
    }

    /// List metadata must survive, or a paging client is stranded.
    #[test]
    fn list_metadata_is_carried_through() {
        let t = table(&list(), IncludeObject::Metadata);
        assert_eq!(
            t.metadata.get("resourceVersion"),
            Some(&serde_json::json!("42"))
        );
    }

    /// A single object is a ONE-ROW table, not an error.
    #[test]
    fn a_single_object_is_a_one_row_table() {
        let obj = serde_json::json!({
            "metadata": { "name": "solo", "creationTimestamp": "2026-08-28T21:00:00Z" }
        });
        let t = table(&obj, IncludeObject::Metadata);
        assert_eq!(t.rows.len(), 1);
        assert_eq!(t.rows[0].cells[0], serde_json::json!("solo"));
    }

    #[test]
    fn include_object_none_omits_the_object() {
        let t = table(&list(), IncludeObject::None);
        assert!(t.rows[0].object.is_none());
    }

    #[test]
    fn include_object_metadata_embeds_partial_object_metadata() {
        let t = table(&list(), IncludeObject::Metadata);
        let o = t.rows[0].object.as_ref().expect("metadata embedded");
        assert_eq!(
            o.get("kind"),
            Some(&serde_json::json!("PartialObjectMetadata"))
        );
        assert!(o.get("metadata").is_some());
    }

    #[test]
    fn include_object_object_embeds_the_whole_object() {
        let t = table(&list(), IncludeObject::Object);
        let o = t.rows[0].object.as_ref().expect("object embedded");
        assert_eq!(
            o.get("metadata").and_then(|m| m.get("name")),
            Some(&serde_json::json!("a"))
        );
        assert_eq!(o.get("kind"), None, "the raw object, not wrapped");
    }

    #[test]
    fn include_object_parses_case_insensitively_with_an_upstream_default() {
        assert_eq!(IncludeObject::parse(Some("None")), IncludeObject::None);
        assert_eq!(IncludeObject::parse(Some("none")), IncludeObject::None);
        assert_eq!(IncludeObject::parse(Some("Object")), IncludeObject::Object);
        assert_eq!(IncludeObject::parse(None), IncludeObject::Metadata);
        assert_eq!(
            IncludeObject::parse(Some("nonsense")),
            IncludeObject::Metadata,
            "unknown values fall back to the default rather than erroring"
        );
    }

    /// An empty list is an empty Table WITH its columns — a client must still
    /// be able to draw the header row.
    #[test]
    fn an_empty_list_still_carries_columns() {
        let empty = serde_json::json!({
            "apiVersion": "v1", "kind": "ConfigMapList",
            "metadata": {}, "items": []
        });
        let t = table(&empty, IncludeObject::Metadata);
        assert!(t.rows.is_empty());
        assert_eq!(
            t.column_definitions.len(),
            3,
            "an empty table still defines its columns, or the client cannot \
             render a header"
        );
    }
}

#[cfg(test)]
mod partial_metadata {
    use super::{accept_wants_partial_metadata, to_partial_object_metadata};
    use serde_json::json;

    /// The exact Accept controller-runtime's METADATA cache sends, and the
    /// one engenho ignored — answering with a protobuf-encoded full list, so
    /// the client's JSON decoder met the `k8s\0` magic and reported
    /// `invalid character 'k'`.
    #[test]
    fn the_metadata_client_accept_is_recognised() {
        assert!(accept_wants_partial_metadata(
            "application/json;as=PartialObjectMetadataList;g=meta.k8s.io;v=v1,application/json"
        ));
        assert!(accept_wants_partial_metadata(
            "application/vnd.kubernetes.protobuf;as=PartialObjectMetadataList;g=meta.k8s.io;v=v1,application/json"
        ));
        // Single-object form, used by a metadata GET.
        assert!(accept_wants_partial_metadata(
            "application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1"
        ));
    }

    /// Negative controls. `as=` without the meta.k8s.io group is a different
    /// conversion, and an ordinary Accept must not be hijacked — otherwise
    /// every normal list would silently lose its objects.
    #[test]
    fn an_ordinary_accept_is_not_hijacked() {
        assert!(!accept_wants_partial_metadata("application/json"));
        assert!(!accept_wants_partial_metadata(
            "application/vnd.kubernetes.protobuf,application/json"
        ));
        assert!(!accept_wants_partial_metadata(
            "application/json;as=Table;g=meta.k8s.io;v=v1"
        ));
        // `as=` naming the right kind but the WRONG group is not this.
        assert!(!accept_wants_partial_metadata(
            "application/json;as=PartialObjectMetadataList;g=example.com;v=v1"
        ));
    }

    #[test]
    fn a_list_becomes_a_partial_object_metadata_list() {
        let list = json!({
            "apiVersion": "v1",
            "kind": "SecretList",
            "metadata": {"resourceVersion": "172654"},
            "items": [
                {"apiVersion":"v1","kind":"Secret","metadata":{"name":"a","namespace":"flux-system"},"data":{"k":"dg=="}},
                {"apiVersion":"v1","kind":"Secret","metadata":{"name":"b","namespace":"flux-system"},"data":{"k":"dg=="}}
            ]
        });
        let out = to_partial_object_metadata(&list);
        assert_eq!(out["kind"], json!("PartialObjectMetadataList"));
        assert_eq!(out["apiVersion"], json!("meta.k8s.io/v1"));
        // The list-level resourceVersion is what a reflector resumes from;
        // dropping it makes every metadata informer re-list on each sync.
        assert_eq!(out["metadata"]["resourceVersion"], json!("172654"));
        assert_eq!(out["items"].as_array().unwrap().len(), 2);
        assert_eq!(out["items"][0]["kind"], json!("PartialObjectMetadata"));
        assert_eq!(out["items"][0]["metadata"]["name"], json!("a"));
        // ★ The point of the projection: the body is GONE. A metadata-only
        // cache exists so a controller never holds Secret payloads in memory.
        assert!(out["items"][0].get("data").is_none(), "got: {out}");
    }

    #[test]
    fn a_single_object_becomes_a_partial_object_metadata() {
        let obj = json!({
            "apiVersion": "v1", "kind": "Secret",
            "metadata": {"name": "flux-system"},
            "data": {"password": "aHVudGVyMg=="}
        });
        let out = to_partial_object_metadata(&obj);
        assert_eq!(out["kind"], json!("PartialObjectMetadata"));
        assert_eq!(out["metadata"]["name"], json!("flux-system"));
        assert!(
            out.get("data").is_none(),
            "the payload must not survive: {out}"
        );
    }

    /// An empty list is still a list — it must not collapse into a single
    /// PartialObjectMetadata, which the client cannot decode into a list.
    #[test]
    fn an_empty_list_stays_a_list() {
        let out = to_partial_object_metadata(&json!({
            "apiVersion": "v1", "kind": "SecretList",
            "metadata": {}, "items": []
        }));
        assert_eq!(out["kind"], json!("PartialObjectMetadataList"));
        assert_eq!(out["items"].as_array().unwrap().len(), 0);
    }
}
