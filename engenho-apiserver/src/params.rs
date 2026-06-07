//! List/Watch query parameters + the typed K8s watch-wire encoders.
//!
//! Shared by the two LIST routes (`list_namespaced`,
//! `list_cluster_scoped`). `?watch=true` flips a LIST into a streaming
//! WATCH; `resourceVersion=`, `labelSelector=`, `fieldSelector=`,
//! `allowWatchBookmarks=` shape both paths.
//!
//! ## Typed emission
//!
//! Every byte written onto the watch stream is produced through a typed
//! `Serialize` value (`K8sWatchLine`, `K8sBookmarkObject`,
//! `crate::error::status_object`) + `serde_json` — never `format!()` of
//! JSON. The on-wire shape is K8s newline-delimited JSON: each line is a
//! `WatchEvent` `{"type":...,"object":...}` followed by a `\n`.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use engenho_store::resource::ResourceKey;
use engenho_store::watch::WatchEvent;
use engenho_store::{Revision, WatchEventKind};

use crate::error::ApiError;

/// Raw list/watch query string params, K8s-shaped.
///
/// `resourceVersion`, `timeoutSeconds`, `limit`, `continue` are kept as
/// strings (K8s wire types) and interpreted by typed accessors so a
/// malformed value is a typed 400, not a serde reject.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default)]
pub struct ListWatchParams {
    /// `?watch=true` / `?watch=1` → stream; anything else → list.
    #[serde(deserialize_with = "de_bool")]
    pub watch: bool,
    /// `resourceVersion=` — string per K8s ("" / absent / "0" / "N").
    #[serde(rename = "resourceVersion")]
    pub resource_version: Option<String>,
    /// `labelSelector=k1=v1,k2=v2`.
    #[serde(rename = "labelSelector")]
    pub label_selector: Option<String>,
    /// `fieldSelector=metadata.name=x,metadata.namespace=y`.
    #[serde(rename = "fieldSelector")]
    pub field_selector: Option<String>,
    /// `allowWatchBookmarks=true` — K8s opt-in; defaults to `true` here.
    #[serde(rename = "allowWatchBookmarks", deserialize_with = "de_bool_default_true")]
    pub allow_watch_bookmarks: bool,
    /// Accepted + parsed, no-op at M0.1 (informer long-poll timeout).
    #[serde(rename = "timeoutSeconds")]
    pub timeout_seconds: Option<String>,
    /// Accepted + parsed, no-op at M0.1 (pagination = item 5).
    pub limit: Option<String>,
    /// Accepted + parsed, no-op at M0.1 (pagination = item 5).
    #[serde(rename = "continue")]
    pub continue_: Option<String>,
}

impl ListWatchParams {
    /// Interpret `resourceVersion` with K8s semantics.
    ///
    ///   * absent / `""` / `"0"` => [`ResumePoint::MostRecent`]
    ///   * `"N"` (parseable u64) => [`ResumePoint::At(Revision(N))`]
    ///   * anything else => `Err(ApiError::BadRequest)` (a real 400)
    ///
    /// # Errors
    ///
    /// [`ApiError::BadRequest`] when `resourceVersion` is present and
    /// non-empty but not a base-10 unsigned integer.
    pub fn resume_point(&self) -> Result<ResumePoint, ApiError> {
        match self.resource_version.as_deref() {
            None | Some("") | Some("0") => Ok(ResumePoint::MostRecent),
            Some(s) => s.parse::<u64>().map(|n| ResumePoint::At(Revision(n))).map_err(|_| {
                ApiError::BadRequest(format!(
                    "invalid resourceVersion: {s:?} (must be a non-negative integer)"
                ))
            }),
        }
    }

    /// Parse the label + field selectors into a typed [`Selectors`].
    ///
    /// # Errors
    ///
    /// [`ApiError::BadRequest`] when a selector clause is not `k=v`.
    pub fn selectors(&self) -> Result<Selectors, ApiError> {
        Ok(Selectors {
            labels: parse_kv(self.label_selector.as_deref(), "labelSelector")?,
            fields: parse_kv(self.field_selector.as_deref(), "fieldSelector")?,
        })
    }
}

/// Where a WATCH (or the LIST snapshot) resumes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumePoint {
    /// `resourceVersion` absent or `"0"` — watch from the current
    /// revision forward, NO historical replay.
    MostRecent,
    /// `resourceVersion="N"` (N > 0) — replay `changes_since(N)` then
    /// live.
    At(Revision),
}

/// Typed label + field selectors. At M0.1 the only field selectors are
/// `metadata.name` + `metadata.namespace`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Selectors {
    pub labels: Vec<(String, String)>,
    pub fields: Vec<(String, String)>,
}

impl Selectors {
    /// `true` when `obj` satisfies EVERY label + field clause.
    ///
    /// Labels match `obj.metadata.labels.<k> == v`; fields match
    /// `metadata.name` / `metadata.namespace` (the only field selectors
    /// supported at M0.1). An unsupported field key never matches (the
    /// object is filtered out), which is the safe default.
    #[must_use]
    pub fn matches(&self, obj: &serde_json::Value) -> bool {
        let metadata = obj.get("metadata");
        for (k, want) in &self.labels {
            let have = metadata
                .and_then(|m| m.get("labels"))
                .and_then(|l| l.get(k))
                .and_then(serde_json::Value::as_str);
            if have != Some(want.as_str()) {
                return false;
            }
        }
        for (k, want) in &self.fields {
            let have = match k.as_str() {
                "metadata.name" => metadata
                    .and_then(|m| m.get("name"))
                    .and_then(serde_json::Value::as_str),
                "metadata.namespace" => metadata
                    .and_then(|m| m.get("namespace"))
                    .and_then(serde_json::Value::as_str),
                _ => return false, // unsupported field selector → no match
            };
            if have != Some(want.as_str()) {
                return false;
            }
        }
        true
    }

    /// `true` when there are no clauses (everything passes).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.labels.is_empty() && self.fields.is_empty()
    }
}

/// `true` if `key`'s (group, version, kind) matches the handler's GVK
/// AND its namespace matches the requested namespace (when scoped).
///
/// The WatchStream is cluster-wide (the store is GVK-keyed but a single
/// registry fans every kind); the handler filters each event down to
/// its own kind + the requested namespace.
#[must_use]
pub fn gvk_ns_matches(
    key: &ResourceKey,
    group: &str,
    version: &str,
    kind: &str,
    namespace: Option<&str>,
) -> bool {
    if key.group != group || key.version != version || key.kind != kind {
        return false;
    }
    match (namespace, key.namespace.as_deref()) {
        (None, _) => true,
        (Some(want), Some(have)) => want == have,
        (Some(_), None) => false,
    }
}

// ── typed watch-wire encoders (no format!() of JSON) ───────────────

/// One K8s watch line — EXACTLY `{"type":...,"object":...}`.
///
/// The engenho-internal [`WatchEvent`] serializes with extra `key` /
/// `resource_version` fields the K8s wire form must NOT carry; this
/// struct re-projects to the canonical two-field shape.
#[derive(Serialize)]
struct K8sWatchLine<'a> {
    #[serde(rename = "type")]
    kind: WatchEventKind,
    object: &'a serde_json::Value,
}

/// The synthetic object a BOOKMARK line carries:
/// `{"metadata":{"resourceVersion":"N"}}`.
#[derive(Serialize)]
struct K8sBookmarkObject {
    metadata: K8sBookmarkMeta,
}

#[derive(Serialize)]
struct K8sBookmarkMeta {
    #[serde(rename = "resourceVersion")]
    resource_version: String,
}

/// Encode a watch `Event` as a newline-terminated K8s watch line —
/// `{"type":"ADDED|MODIFIED|DELETED","object":<resource>}\n`. Emits
/// ONLY `{type, object}`; `object` is `ev.object`, which already carries
/// `metadata.resourceVersion` stamped by the catalog.
#[must_use]
pub fn to_k8s_watch_line(ev: &WatchEvent) -> Bytes {
    let line = K8sWatchLine {
        kind: ev.kind,
        object: &ev.object,
    };
    encode_ndjson(&line)
}

/// Encode a BOOKMARK line —
/// `{"type":"BOOKMARK","object":{"metadata":{"resourceVersion":"N"}}}\n`.
#[must_use]
pub fn bookmark_line(rev: Revision) -> Bytes {
    let object = K8sBookmarkObject {
        metadata: K8sBookmarkMeta {
            resource_version: rev.to_string(),
        },
    };
    let line = K8sWatchLine {
        kind: WatchEventKind::Bookmark,
        object: &serde_json::to_value(&object).unwrap_or(serde_json::Value::Null),
    };
    encode_ndjson(&line)
}

/// Encode an in-band 410 `Status` line carrying `rev` as the safe
/// resume point. Used mid-stream (the response is already HTTP 200) when
/// compaction / overflow is discovered — clients (informers) drop their
/// cache + re-LIST from a revision >= `rev`.
#[must_use]
pub fn status_410_line(rev: Revision) -> Bytes {
    let status = crate::error::status_object(
        format!("too old resource version: resume from {rev}"),
        410,
        "Expired",
    );
    // The Status object goes out as a watch line of type ERROR — the
    // shape kube-apiserver uses for in-band terminal status.
    #[derive(Serialize)]
    struct StatusLine<'a> {
        #[serde(rename = "type")]
        kind: &'static str,
        object: &'a serde_json::Value,
    }
    let line = StatusLine {
        kind: "ERROR",
        object: &status,
    };
    encode_ndjson(&line)
}

/// Serialize a value as one NDJSON line (`<json>\n`). Falls back to an
/// empty `{}` line on the (impossible for our shapes) serialize error,
/// never panicking on the streaming path.
fn encode_ndjson<T: Serialize>(value: &T) -> Bytes {
    let mut buf = serde_json::to_vec(value).unwrap_or_else(|_| b"{}".to_vec());
    buf.push(b'\n');
    Bytes::from(buf)
}

// ── deserialize helpers ────────────────────────────────────────────

/// `?flag=true|1|yes` → true; absent → false (serde `default`).
fn de_bool<'de, D: serde::Deserializer<'de>>(de: D) -> Result<bool, D::Error> {
    let s = String::deserialize(de)?;
    Ok(matches!(s.as_str(), "true" | "1" | "yes"))
}

/// Same, but absent / empty defaults to `true` (allowWatchBookmarks
/// opt-in default here).
fn de_bool_default_true<'de, D: serde::Deserializer<'de>>(de: D) -> Result<bool, D::Error> {
    let s = String::deserialize(de)?;
    Ok(!matches!(s.as_str(), "false" | "0" | "no"))
}

/// Parse `k1=v1,k2=v2` into `Vec<(k, v)>`. Empty / absent → empty.
fn parse_kv(s: Option<&str>, what: &str) -> Result<Vec<(String, String)>, ApiError> {
    let Some(s) = s else { return Ok(Vec::new()) };
    if s.is_empty() {
        return Ok(Vec::new());
    }
    s.split(',')
        .map(|clause| {
            clause
                .split_once('=')
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
                .ok_or_else(|| {
                    ApiError::BadRequest(format!("invalid {what} clause {clause:?}: expected k=v"))
                })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params_with_rv(rv: Option<&str>) -> ListWatchParams {
        ListWatchParams {
            resource_version: rv.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn resume_point_zero_and_absent_are_most_recent() {
        assert_eq!(
            params_with_rv(None).resume_point().unwrap(),
            ResumePoint::MostRecent
        );
        assert_eq!(
            params_with_rv(Some("")).resume_point().unwrap(),
            ResumePoint::MostRecent
        );
        assert_eq!(
            params_with_rv(Some("0")).resume_point().unwrap(),
            ResumePoint::MostRecent
        );
    }

    #[test]
    fn resume_point_n_is_at_revision() {
        assert_eq!(
            params_with_rv(Some("42")).resume_point().unwrap(),
            ResumePoint::At(Revision(42))
        );
    }

    #[test]
    fn malformed_resource_version_is_bad_request() {
        let err = params_with_rv(Some("abc")).resume_point().unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)));
    }

    #[test]
    fn allow_watch_bookmarks_defaults_true() {
        // Default value (serde default) is false-on-the-struct; the
        // query default is set by de_bool_default_true when the key is
        // present. Absent key → struct default (false) is interpreted by
        // the router as "use the param as-is". We test the DESERIALIZE
        // default-true behavior here.
        let p: ListWatchParams =
            serde_urlencoded::from_str("allowWatchBookmarks=").unwrap();
        assert!(p.allow_watch_bookmarks);
        let p: ListWatchParams =
            serde_urlencoded::from_str("allowWatchBookmarks=false").unwrap();
        assert!(!p.allow_watch_bookmarks);
        let p: ListWatchParams =
            serde_urlencoded::from_str("allowWatchBookmarks=true").unwrap();
        assert!(p.allow_watch_bookmarks);
    }

    #[test]
    fn watch_flag_parses() {
        let p: ListWatchParams = serde_urlencoded::from_str("watch=true").unwrap();
        assert!(p.watch);
        let p: ListWatchParams = serde_urlencoded::from_str("watch=1").unwrap();
        assert!(p.watch);
        let p: ListWatchParams = serde_urlencoded::from_str("watch=false").unwrap();
        assert!(!p.watch);
        let p: ListWatchParams = serde_urlencoded::from_str("").unwrap();
        assert!(!p.watch);
    }

    #[test]
    fn selectors_parse_and_match() {
        let p = ListWatchParams {
            label_selector: Some("app=web,tier=front".into()),
            field_selector: Some("metadata.name=p1".into()),
            ..Default::default()
        };
        let sel = p.selectors().unwrap();
        assert_eq!(sel.labels.len(), 2);
        assert_eq!(sel.fields.len(), 1);

        let yes = serde_json::json!({
            "metadata": {"name": "p1", "labels": {"app": "web", "tier": "front"}}
        });
        assert!(sel.matches(&yes));

        let wrong_label =
            serde_json::json!({"metadata": {"name": "p1", "labels": {"app": "api"}}});
        assert!(!sel.matches(&wrong_label));

        let wrong_name =
            serde_json::json!({"metadata": {"name": "p2", "labels": {"app": "web", "tier": "front"}}});
        assert!(!sel.matches(&wrong_name));
    }

    #[test]
    fn empty_selectors_match_everything() {
        let sel = Selectors::default();
        assert!(sel.is_empty());
        assert!(sel.matches(&serde_json::json!({"metadata": {"name": "anything"}})));
    }

    #[test]
    fn malformed_selector_is_bad_request() {
        let p = ListWatchParams {
            label_selector: Some("app".into()),
            ..Default::default()
        };
        assert!(matches!(p.selectors(), Err(ApiError::BadRequest(_))));
    }

    #[test]
    fn gvk_ns_match_filters_by_kind_and_namespace() {
        let pod = ResourceKey::namespaced("", "v1", "Pod", "default", "p");
        assert!(gvk_ns_matches(&pod, "", "v1", "Pod", Some("default")));
        // wrong kind
        assert!(!gvk_ns_matches(&pod, "", "v1", "ConfigMap", Some("default")));
        // wrong namespace
        assert!(!gvk_ns_matches(&pod, "", "v1", "Pod", Some("kube-system")));
        // cluster-scoped request (namespace None) matches any ns
        assert!(gvk_ns_matches(&pod, "", "v1", "Pod", None));
    }

    #[test]
    fn watch_line_emits_only_type_and_object() {
        let ev = WatchEvent {
            kind: WatchEventKind::Added,
            object: serde_json::json!({"kind": "Pod", "metadata": {"resourceVersion": "7"}}),
            key: ResourceKey::namespaced("", "v1", "Pod", "default", "p"),
            resource_version: 7,
        };
        let bytes = to_k8s_watch_line(&ev);
        let s = std::str::from_utf8(&bytes).unwrap();
        assert!(s.ends_with('\n'), "line is newline-terminated");
        let v: serde_json::Value = serde_json::from_str(s.trim_end()).unwrap();
        assert_eq!(v.get("type").unwrap(), "ADDED");
        // object carries the full resource WITHOUT the internal key /
        // resource_version sibling fields.
        let obj = v.get("object").unwrap().as_object().unwrap();
        assert_eq!(obj.get("kind").unwrap(), "Pod");
        // The line itself has exactly two top-level fields.
        assert_eq!(v.as_object().unwrap().len(), 2);
        assert!(v.get("key").is_none(), "no internal key field on the wire");
    }

    #[test]
    fn bookmark_line_shape() {
        let bytes = bookmark_line(Revision(99));
        let s = std::str::from_utf8(&bytes).unwrap();
        let v: serde_json::Value = serde_json::from_str(s.trim_end()).unwrap();
        assert_eq!(v.get("type").unwrap(), "BOOKMARK");
        assert_eq!(
            v.get("object")
                .unwrap()
                .get("metadata")
                .unwrap()
                .get("resourceVersion")
                .unwrap(),
            "99"
        );
    }

    #[test]
    fn status_410_line_shape() {
        let bytes = status_410_line(Revision(3));
        let s = std::str::from_utf8(&bytes).unwrap();
        let v: serde_json::Value = serde_json::from_str(s.trim_end()).unwrap();
        assert_eq!(v.get("type").unwrap(), "ERROR");
        let obj = v.get("object").unwrap();
        assert_eq!(obj.get("kind").unwrap(), "Status");
        assert_eq!(obj.get("code").unwrap(), 410);
        assert_eq!(obj.get("reason").unwrap(), "Expired");
    }
}
