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

/// A printer column declared by a CRD's `additionalPrinterColumns`.
///
/// ★ WHY THIS IS THE RIGHT SOURCE and hand-written per-kind arms are not.
/// Upstream's built-in column sets live in Go source that engenho vendors
/// no schema for, so they can only be TRANSCRIBED — and a transcribed
/// table is a hand-list that drifts from upstream silently, showing a
/// plausible-but-wrong column set. A CRD's columns are different: the
/// author DECLARED them in the CRD, engenho already stores that CRD, so
/// they are DERIVED. No drift is possible because there is one source.
///
/// This covers the kinds operators most often stare at — Flux
/// Kustomizations, Cilium policies, Pangea resources — every one of which
/// ships printer columns that were being discarded.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OwnedColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub format: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub description: String,
    pub priority: i32,
}

/// Extract the printer columns a CRD version declares.
///
/// Returns `None` when the CRD declares none, so the caller falls back to
/// the default pair rather than rendering an empty header row.
#[must_use]
pub fn crd_printer_columns(version: &Value) -> Option<Vec<(OwnedColumn, String)>> {
    let cols = version.get("additionalPrinterColumns")?.as_array()?;
    if cols.is_empty() {
        return None;
    }
    let out: Vec<(OwnedColumn, String)> = cols
        .iter()
        .filter_map(|c| {
            // `jsonPath` is required by the CRD schema; a column without
            // one cannot be rendered and is dropped rather than emitted
            // with a null cell, which would misalign every following
            // column in the row.
            let path = c.get("jsonPath")?.as_str()?.to_string();
            Some((
                OwnedColumn {
                    name: c.get("name")?.as_str()?.to_string(),
                    type_: c
                        .get("type")
                        .and_then(Value::as_str)
                        .unwrap_or("string")
                        .to_string(),
                    format: c
                        .get("format")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    description: c
                        .get("description")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    priority: c
                        .get("priority")
                        .and_then(Value::as_i64)
                        .and_then(|p| i32::try_from(p).ok())
                        .unwrap_or(0),
                },
                path,
            ))
        })
        .collect();
    (!out.is_empty()).then_some(out)
}

/// Resolve one `jsonPath` cell value against an object.
///
/// ★ A DELIBERATELY NARROW SUBSET. CRD printer columns use simple dotted
/// paths (`.status.phase`, `.spec.replicas`) in practice; the full JSONPath
/// grammar includes filters and wildcards that would need a real evaluator.
/// Anything outside the subset yields `null` — an EMPTY CELL, which renders
/// as `<none>` exactly as upstream does for an unresolvable path, rather
/// than dropping the column and misaligning the row.
#[must_use]
pub fn resolve_json_path(obj: &Value, path: &str) -> Value {
    let mut cur = obj;
    for seg in path.trim_start_matches('.').split('.') {
        if seg.is_empty() {
            continue;
        }
        match cur.get(seg) {
            Some(next) => cur = next,
            None => return Value::Null,
        }
    }
    cur.clone()
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

    // ── CRD additionalPrinterColumns (Phase 5.7) ──────────────────────

    fn crd_version_with_columns() -> serde_json::Value {
        serde_json::json!({
            "name": "v1",
            "additionalPrinterColumns": [
                { "name": "Ready", "type": "string", "jsonPath": ".status.conditions[0].status" },
                { "name": "Phase", "type": "string", "jsonPath": ".status.phase",
                  "description": "current phase" },
                { "name": "Replicas", "type": "integer", "jsonPath": ".spec.replicas",
                  "priority": 1 }
            ]
        })
    }

    #[test]
    fn a_crds_declared_columns_are_derived_not_transcribed() {
        // These are the kinds operators stare at — Flux, Cilium, Pangea —
        // and their columns were being discarded entirely.
        let cols = super::crd_printer_columns(&crd_version_with_columns()).expect("declared");
        let names: Vec<&str> = cols.iter().map(|(c, _)| c.name.as_str()).collect();
        assert_eq!(names, vec!["Ready", "Phase", "Replicas"]);
        assert_eq!(cols[1].0.description, "current phase");
        assert_eq!(cols[2].0.priority, 1, "priority is honoured, not flattened");
        assert_eq!(cols[2].0.type_, "integer");
    }

    #[test]
    fn a_crd_with_no_columns_falls_back_rather_than_rendering_an_empty_header() {
        assert!(super::crd_printer_columns(&serde_json::json!({ "name": "v1" })).is_none());
        assert!(
            super::crd_printer_columns(&serde_json::json!({ "additionalPrinterColumns": [] }))
                .is_none(),
            "an empty array must fall back, not produce a headerless table"
        );
    }

    #[test]
    fn a_column_without_a_json_path_is_dropped_not_emitted_null() {
        // Emitting it with a null cell would misalign every FOLLOWING
        // column in the row, which is worse than omitting one.
        let v = serde_json::json!({
            "additionalPrinterColumns": [
                { "name": "Broken", "type": "string" },
                { "name": "Fine", "type": "string", "jsonPath": ".status.phase" }
            ]
        });
        let cols = super::crd_printer_columns(&v).expect("one survives");
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].0.name, "Fine");
    }

    #[test]
    fn dotted_json_paths_resolve_and_a_missing_one_is_an_empty_cell() {
        let obj = serde_json::json!({
            "spec": { "replicas": 3 },
            "status": { "phase": "Ready" }
        });
        assert_eq!(super::resolve_json_path(&obj, ".status.phase"), "Ready");
        assert_eq!(super::resolve_json_path(&obj, ".spec.replicas"), 3);
        // Leading dot optional — CRDs write it both ways.
        assert_eq!(super::resolve_json_path(&obj, "status.phase"), "Ready");
        // Unresolvable renders as <none> upstream, so null is right; the
        // column must NOT be dropped at render time or the row misaligns.
        assert_eq!(
            super::resolve_json_path(&obj, ".status.nope"),
            serde_json::Value::Null
        );
        assert_eq!(
            super::resolve_json_path(&obj, ".status.conditions[0].status"),
            serde_json::Value::Null,
            "the filter/index grammar is outside the subset and yields an \
             empty cell rather than a wrong one"
        );
    }
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
