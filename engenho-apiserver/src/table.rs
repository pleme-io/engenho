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
//! ## What this implements, and what it does not
//!
//! Upstream ships a per-kind printer (`printers/internalversion`) giving Pods
//! their READY/STATUS/RESTARTS columns, Deployments their UP-TO-DATE/AVAILABLE,
//! and so on. Those column sets live in Go source, not in any schema engenho
//! vendors, so they cannot be *derived* — they would have to be transcribed,
//! and a transcribed table is a hand-list that drifts.
//!
//! So this implements upstream's **default** table — `NAME` and `AGE`, read
//! from `metadata` — which upstream itself falls back to for any kind without a
//! registered printer. It is correct for every kind, derived rather than
//! transcribed, and it is the difference between k9s showing a usable list and
//! showing nothing. Per-kind columns are a follow-up that should arrive as a
//! GENERATED table (from a pinned extraction of upstream's printers, or from a
//! CRD's own `additionalPrinterColumns`), never as hand-written match arms.
//!
//! Stated plainly so nobody reads a green test as full server-side printing:
//! **columns are the generic pair, not upstream's per-kind sets.**

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

/// One `meta.k8s.io/v1` column definition.
#[derive(Serialize, Clone, Debug)]
pub struct ColumnDefinition {
    /// Human column name (`NAME`, `AGE`).
    pub name: &'static str,
    /// OpenAPI type (`string`, `date`).
    #[serde(rename = "type")]
    pub type_: &'static str,
    /// OpenAPI format; empty when none.
    pub format: &'static str,
    /// What the column means, surfaced by `kubectl explain`-style clients.
    pub description: &'static str,
    /// 0 shows in the default view; higher values are wide-only.
    pub priority: i32,
}

/// One row: the cells, plus optionally the object they came from.
#[derive(Serialize, Clone, Debug)]
pub struct TableRow {
    /// Cell values, positionally matching `columnDefinitions`.
    pub cells: Vec<Value>,
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

/// Upstream's default column pair, used for any kind with no registered
/// per-kind printer. Deliberately `const`: these are not configurable, and a
/// call site that wanted to vary them would be reintroducing the hand-list
/// this module exists to avoid.
const DEFAULT_COLUMNS: &[ColumnDefinition] = &[
    ColumnDefinition {
        name: "Name",
        type_: "string",
        format: "name",
        description: "Name must be unique within a namespace.",
        priority: 0,
    },
    ColumnDefinition {
        name: "Age",
        type_: "date",
        format: "",
        description: "CreationTimestamp is a timestamp representing the server time when this \
                      object was created.",
        priority: 0,
    },
];

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

/// One row from one object.
fn row_for(obj: &Value, include: IncludeObject) -> TableRow {
    let meta = obj.get("metadata");
    let name = meta
        .and_then(|m| m.get("name"))
        .cloned()
        .unwrap_or(Value::Null);
    // AGE is rendered by the CLIENT from creationTimestamp — the server sends
    // the timestamp, not a pre-formatted duration, so a client that has been
    // open for an hour still shows a correct age without re-fetching.
    let age = meta
        .and_then(|m| m.get("creationTimestamp"))
        .cloned()
        .unwrap_or(Value::Null);
    TableRow {
        cells: vec![name, age],
        object: match include {
            IncludeObject::None => None,
            IncludeObject::Metadata => Some(partial_object_metadata(obj)),
            IncludeObject::Object => Some(obj.clone()),
        },
    }
}

/// Convert a List **or** a single object into a Table.
///
/// A single-object GET is a one-row Table upstream, not an error — `kubectl get
/// pod foo` asks for exactly that.
#[must_use]
pub fn to_table(value: &Value, include: IncludeObject) -> Table {
    let items = value.get("items").and_then(|i| i.as_array());
    let (rows, metadata) = match items {
        // A List: one row per item, and carry the list's own metadata
        // (resourceVersion, continue) so a paging client is not stranded.
        Some(items) => (
            items.iter().map(|o| row_for(o, include)).collect(),
            value
                .get("metadata")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({})),
        ),
        // A single object: one row. Its own metadata would be the OBJECT's,
        // which is not list metadata, so send an empty block rather than
        // something a client would misread as a continue token.
        None => (vec![row_for(value, include)], serde_json::json!({})),
    };
    Table {
        kind: "Table",
        api_version: "meta.k8s.io/v1",
        metadata,
        column_definitions: DEFAULT_COLUMNS.to_vec(),
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

    #[test]
    fn a_list_becomes_one_row_per_item() {
        let t = to_table(&list(), IncludeObject::Metadata);
        assert_eq!(t.kind, "Table");
        assert_eq!(t.api_version, "meta.k8s.io/v1");
        assert_eq!(t.rows.len(), 2);
        assert_eq!(t.column_definitions.len(), 2);
        assert_eq!(t.rows[0].cells[0], serde_json::json!("a"));
        assert_eq!(
            t.rows[0].cells[1],
            serde_json::json!("2026-08-28T21:00:00Z"),
            "AGE carries the TIMESTAMP; the client renders the duration"
        );
    }

    /// List metadata must survive, or a paging client is stranded.
    #[test]
    fn list_metadata_is_carried_through() {
        let t = to_table(&list(), IncludeObject::Metadata);
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
        let t = to_table(&obj, IncludeObject::Metadata);
        assert_eq!(t.rows.len(), 1);
        assert_eq!(t.rows[0].cells[0], serde_json::json!("solo"));
    }

    #[test]
    fn include_object_none_omits_the_object() {
        let t = to_table(&list(), IncludeObject::None);
        assert!(t.rows[0].object.is_none());
    }

    #[test]
    fn include_object_metadata_embeds_partial_object_metadata() {
        let t = to_table(&list(), IncludeObject::Metadata);
        let o = t.rows[0].object.as_ref().expect("metadata embedded");
        assert_eq!(
            o.get("kind"),
            Some(&serde_json::json!("PartialObjectMetadata"))
        );
        assert!(o.get("metadata").is_some());
    }

    #[test]
    fn include_object_object_embeds_the_whole_object() {
        let t = to_table(&list(), IncludeObject::Object);
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
        let t = to_table(&empty, IncludeObject::Metadata);
        assert!(t.rows.is_empty());
        assert_eq!(
            t.column_definitions.len(),
            2,
            "an empty table still defines its columns, or the client cannot \
             render a header"
        );
    }
}
