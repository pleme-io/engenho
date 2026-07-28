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
use engenho_store::{ContinueToken, Revision, WatchEventKind};

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
    #[serde(
        rename = "allowWatchBookmarks",
        deserialize_with = "de_bool_default_true"
    )]
    pub allow_watch_bookmarks: bool,
    /// Accepted + parsed, no-op at M0.1 (informer long-poll timeout).
    #[serde(rename = "timeoutSeconds")]
    pub timeout_seconds: Option<String>,
    /// `limit=N` — page size (item 5). Parsed by [`Self::limit`]; `0` /
    /// absent = unbounded.
    pub limit: Option<String>,
    /// `continue=<opaque token>` — the page cursor (item 5). Decoded +
    /// integrity-verified by [`Self::continue_token`]; invalid → 410.
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
            Some(s) => s
                .parse::<u64>()
                .map(|n| ResumePoint::At(Revision(n)))
                .map_err(|_| {
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
    /// [`ApiError::BadRequest`] when a selector clause is malformed (a bad
    /// label requirement grammar / a field clause that is not `k=v` / `k!=v`).
    pub fn selectors(&self) -> Result<Selectors, ApiError> {
        Ok(Selectors {
            labels: parse_label_selector(self.label_selector.as_deref())?,
            fields: parse_field_selector(self.field_selector.as_deref())?,
        })
    }

    /// Interpret `limit` with K8s semantics: absent / `""` / `"0"` => 0
    /// (unbounded); `"N"` => N; anything else => a real 400.
    ///
    /// # Errors
    ///
    /// [`ApiError::BadRequest`] when `limit` is present and non-empty but
    /// not a base-10 unsigned integer.
    pub fn limit(&self) -> Result<usize, ApiError> {
        match self.limit.as_deref() {
            None | Some("") | Some("0") => Ok(0),
            Some(s) => s.parse::<usize>().map_err(|_| {
                ApiError::BadRequest(format!(
                    "invalid limit: {s:?} (must be a non-negative integer)"
                ))
            }),
        }
    }

    /// Decode + integrity-verify the `continue` token. Absent / empty =>
    /// `None` (first page). A present-but-invalid/expired/corrupt token
    /// => [`ApiError::Gone`] (HTTP 410 / Expired), the K8s contract for a
    /// stale continue cursor.
    ///
    /// # Errors
    ///
    /// [`ApiError::Gone`] when the token fails to decode or its integrity
    /// digest / version tag don't verify.
    pub fn continue_token(&self) -> Result<Option<ContinueToken>, ApiError> {
        match self.continue_.as_deref() {
            None | Some("") => Ok(None),
            Some(s) => ContinueToken::decode(s).map(Some).map_err(|e| {
                ApiError::Gone(format!("invalid or expired continue token: {}", e.reason))
            }),
        }
    }

    /// Interpret `resourceVersion` as a DELETE precondition
    /// (`Preconditions.resourceVersion`, K8s `?resourceVersion=N` on
    /// DELETE): absent / `""` / `"0"` => `None` (unconditional delete);
    /// `"N"` => `Some(Revision(N))`; anything else => a real 400.
    ///
    /// # Errors
    ///
    /// [`ApiError::BadRequest`] when `resourceVersion` is present and
    /// non-empty but not a base-10 unsigned integer.
    pub fn precondition(&self) -> Result<Option<Revision>, ApiError> {
        match self.resource_version.as_deref() {
            None | Some("") | Some("0") => Ok(None),
            Some(s) => s.parse::<u64>().map(|n| Some(Revision(n))).map_err(|_| {
                ApiError::BadRequest(format!(
                    "invalid resourceVersion precondition: {s:?} (must be a non-negative integer)"
                ))
            }),
        }
    }
}

/// The server-side-apply query params (`?fieldManager=` + `?force=`),
/// parsed off the PATCH request when the Content-Type resolves to
/// [`engenho_types::patch::PatchType::Apply`].
///
/// `fieldManager` is REQUIRED on an apply request — a missing/empty one is
/// a typed 422/400 ([`ApplyOptions::from_params`]), matching upstream
/// kube-apiserver. `force` defaults to false.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default)]
pub struct ApplyParams {
    /// `?fieldManager=<name>` — the manager's identity (REQUIRED for apply).
    #[serde(rename = "fieldManager")]
    pub field_manager: Option<String>,
    /// `?force=true|1|yes` — take ownership of conflicting fields.
    #[serde(deserialize_with = "de_bool")]
    pub force: bool,
}

/// The validated, typed server-side-apply options threaded from the router
/// into the handler. Construction enforces the `fieldManager` requirement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyOptions {
    /// The field-manager identity (non-empty by construction).
    pub manager: String,
    /// Whether to force-override conflicting fields.
    pub force: bool,
}

impl ApplyOptions {
    /// Validate the raw [`ApplyParams`] into typed [`ApplyOptions`]. A
    /// missing / empty `fieldManager` is a typed
    /// [`ApiError::BadRequest`] (the 400/422 kube-apiserver returns for an
    /// apply request without a fieldManager) — never a silent default.
    ///
    /// # Errors
    ///
    /// [`ApiError::BadRequest`] when `fieldManager` is absent or empty.
    pub fn from_params(p: &ApplyParams) -> Result<Self, ApiError> {
        let manager = p
            .field_manager
            .as_deref()
            .map(str::trim)
            .filter(|m| !m.is_empty())
            .ok_or_else(|| {
                ApiError::BadRequest(
                    "fieldManager is required for an apply patch (Content-Type \
                     application/apply-patch+yaml)"
                        .to_string(),
                )
            })?
            .to_string();
        Ok(Self {
            manager,
            force: p.force,
        })
    }
}

/// Read the optimistic-concurrency precondition from an inbound resource
/// BODY (create / patch): `metadata.resourceVersion`.
///
///   * absent => `None` (unconditional — K8s semantics for absent rv).
///   * `"N"` => `Some(Revision(N))`.
///   * present-but-malformed => a real 400.
///
/// Uses the SAME `Revision`-parse shape as [`ListWatchParams::precondition`].
///
/// # Errors
///
/// [`ApiError::BadRequest`] when `metadata.resourceVersion` is present
/// but not a base-10 unsigned integer.
pub fn body_precondition(body: &serde_json::Value) -> Result<Option<Revision>, ApiError> {
    let rv = body.get("metadata").and_then(|m| m.get("resourceVersion"));
    match rv {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(s)) if s.is_empty() => Ok(None),
        Some(serde_json::Value::String(s)) => {
            s.parse::<u64>().map(|n| Some(Revision(n))).map_err(|_| {
                ApiError::BadRequest(format!(
                    "invalid metadata.resourceVersion: {s:?} (must be a non-negative integer)"
                ))
            })
        }
        // K8s resourceVersion is a string on the wire; a non-string is a
        // malformed body.
        Some(other) => Err(ApiError::BadRequest(format!(
            "metadata.resourceVersion must be a string, got {other}"
        ))),
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

/// One typed label-selector requirement — the FULL K8s label selector
/// grammar (equality-based `=`/`==`/`!=` + set-based `in`/`notin`/exists/
/// not-exists). Before this typed model the parser handled ONLY `k=v`, so
/// `k!=v`, `k in (a,b)`, `!k`, and bare-`k` (exists) were mis-parsed or
/// rejected (a `,` inside `(a,b)` split the clause and 400'd) — diverging from
/// every apiserver a controller / kubectl talks to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelRequirement {
    /// `k=v` / `k==v` — label `k` present and equal to `v`.
    Equals(String, String),
    /// `k!=v` — label `k` absent OR present-but-not-`v` (k8s includes
    /// objects LACKING the key).
    NotEquals(String, String),
    /// `k in (a,b,…)` — label `k` present and IN the set.
    In(String, Vec<String>),
    /// `k notin (a,b,…)` — label `k` absent OR present-but-NOT-in the set.
    NotIn(String, Vec<String>),
    /// `k` — label `k` present (any value).
    Exists(String),
    /// `!k` — label `k` absent.
    NotExists(String),
}

impl LabelRequirement {
    /// `true` when `labels` (the object's `metadata.labels`) satisfies this
    /// requirement. A missing labels map is treated as "no labels".
    fn satisfied_by(&self, labels: Option<&serde_json::Value>) -> bool {
        let have = |k: &str| {
            labels
                .and_then(|l| l.get(k))
                .and_then(serde_json::Value::as_str)
        };
        match self {
            Self::Equals(k, v) => have(k) == Some(v.as_str()),
            Self::NotEquals(k, v) => have(k) != Some(v.as_str()),
            Self::In(k, set) => have(k).is_some_and(|h| set.iter().any(|s| s == h)),
            Self::NotIn(k, set) => have(k).is_none_or(|h| !set.iter().any(|s| s == h)),
            Self::Exists(k) => have(k).is_some(),
            Self::NotExists(k) => have(k).is_none(),
        }
    }
}

/// One typed field-selector requirement. K8s field selectors are equality-
/// based only (`=`/`==`/`!=`); the SUPPORTED keys are `metadata.name` +
/// `metadata.namespace` (the two every kind honors). An unsupported field key
/// (`status.phase`, `spec.nodeName`, …) needs per-kind field-selector
/// registration engenho does not yet have — such a requirement matches nothing
/// (the object is filtered out), a divergence recorded in the diff harness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldRequirement {
    /// `k=v` / `k==v`.
    Equals(String, String),
    /// `k!=v`.
    NotEquals(String, String),
}

impl FieldRequirement {
    fn key(&self) -> &str {
        match self {
            Self::Equals(k, _) | Self::NotEquals(k, _) => k,
        }
    }

    /// `true` when `obj` satisfies this field requirement. Returns `false`
    /// for an unsupported field key (filter the object out — the safe
    /// default until per-kind field-selector registration lands).
    fn satisfied_by(&self, obj: &serde_json::Value) -> bool {
        let metadata = obj.get("metadata");
        let have = match self.key() {
            "metadata.name" => metadata
                .and_then(|m| m.get("name"))
                .and_then(serde_json::Value::as_str),
            "metadata.namespace" => metadata
                .and_then(|m| m.get("namespace"))
                .and_then(serde_json::Value::as_str),
            _ => return false, // unsupported field key → no match
        };
        match self {
            Self::Equals(_, v) => have == Some(v.as_str()),
            Self::NotEquals(_, v) => have != Some(v.as_str()),
        }
    }
}

/// Typed label + field selectors (the full label grammar; equality field
/// selectors on `metadata.name`/`metadata.namespace`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Selectors {
    pub labels: Vec<LabelRequirement>,
    pub fields: Vec<FieldRequirement>,
}

impl Selectors {
    /// `true` when `obj` satisfies EVERY label + field requirement (the
    /// requirements are ANDed, matching k8s).
    #[must_use]
    pub fn matches(&self, obj: &serde_json::Value) -> bool {
        let labels = obj.get("metadata").and_then(|m| m.get("labels"));
        self.labels.iter().all(|r| r.satisfied_by(labels))
            && self.fields.iter().all(|r| r.satisfied_by(obj))
    }

    /// `true` when there are no requirements (everything passes).
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

/// Split a selector string into its top-level clauses on `,`, treating commas
/// INSIDE `(...)` as part of a set-based value list (so `k in (a,b),m=n` splits
/// into `["k in (a,b)", "m=n"]`, NOT `["k in (a", "b)", "m=n"]`). This is the
/// load-bearing fix for the old naive `split(',')` that shattered a set-based
/// clause and 400'd it.
fn split_top_level(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut cur = String::new();
    for c in s.chars() {
        match c {
            '(' => {
                depth += 1;
                cur.push(c);
            }
            ')' => {
                depth -= 1;
                cur.push(c);
            }
            ',' if depth == 0 => out.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    out.push(cur);
    out
}

/// Parse a `labelSelector=` string into typed [`LabelRequirement`]s (the full
/// K8s grammar). Empty / absent → empty. A malformed clause → a typed 400.
///
/// # Errors
///
/// [`ApiError::BadRequest`] on a malformed clause (empty key, a set clause
/// with no `in`/`notin` operator, …).
fn parse_label_selector(s: Option<&str>) -> Result<Vec<LabelRequirement>, ApiError> {
    let Some(s) = s.filter(|s| !s.is_empty()) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for clause in split_top_level(s) {
        if let Some(req) = parse_label_clause(clause.trim())? {
            out.push(req);
        }
    }
    Ok(out)
}

fn parse_label_clause(c: &str) -> Result<Option<LabelRequirement>, ApiError> {
    if c.is_empty() {
        return Ok(None);
    }
    // not-exists: `!key`.
    if let Some(key) = c.strip_prefix('!') {
        let key = key.trim();
        if key.is_empty() {
            return Err(bad_selector("labelSelector", c, "empty key after '!'"));
        }
        return Ok(Some(LabelRequirement::NotExists(key.to_string())));
    }
    // set-based: `key in (a,b)` / `key notin (a,b)`.
    if let Some(open) = c.find('(') {
        let close = c
            .rfind(')')
            .filter(|&r| r > open)
            .ok_or_else(|| bad_selector("labelSelector", c, "unterminated '(' set"))?;
        let head = c[..open].trim();
        let inner = &c[open + 1..close];
        let values: Vec<String> = inner
            .split(',')
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .collect();
        let mut toks = head.split_whitespace();
        let key = toks
            .next()
            .filter(|k| !k.is_empty())
            .ok_or_else(|| bad_selector("labelSelector", c, "empty key before set"))?;
        let op = toks.next().unwrap_or("");
        if toks.next().is_some() {
            return Err(bad_selector("labelSelector", c, "extra tokens before set"));
        }
        return match op {
            "in" => Ok(Some(LabelRequirement::In(key.to_string(), values))),
            "notin" => Ok(Some(LabelRequirement::NotIn(key.to_string(), values))),
            _ => Err(bad_selector(
                "labelSelector",
                c,
                "set requires 'in' or 'notin'",
            )),
        };
    }
    // equality-based: `k!=v` / `k==v` / `k=v` (check `!=`/`==` before `=`).
    if let Some((k, v)) = c.split_once("!=") {
        return Ok(Some(LabelRequirement::NotEquals(
            trim_key(k, c, "labelSelector")?,
            v.trim().to_string(),
        )));
    }
    if let Some((k, v)) = c.split_once("==") {
        return Ok(Some(LabelRequirement::Equals(
            trim_key(k, c, "labelSelector")?,
            v.trim().to_string(),
        )));
    }
    if let Some((k, v)) = c.split_once('=') {
        return Ok(Some(LabelRequirement::Equals(
            trim_key(k, c, "labelSelector")?,
            v.trim().to_string(),
        )));
    }
    // bare key → exists.
    Ok(Some(LabelRequirement::Exists(c.to_string())))
}

/// Parse a `fieldSelector=` string into typed [`FieldRequirement`]s (equality
/// only). Empty / absent → empty; a malformed clause → a typed 400.
///
/// # Errors
///
/// [`ApiError::BadRequest`] on a clause that is not `k=v` / `k==v` / `k!=v`.
fn parse_field_selector(s: Option<&str>) -> Result<Vec<FieldRequirement>, ApiError> {
    let Some(s) = s.filter(|s| !s.is_empty()) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for clause in split_top_level(s) {
        let c = clause.trim();
        if c.is_empty() {
            continue;
        }
        if let Some((k, v)) = c.split_once("!=") {
            out.push(FieldRequirement::NotEquals(
                trim_key(k, c, "fieldSelector")?,
                v.trim().to_string(),
            ));
        } else if let Some((k, v)) = c.split_once("==") {
            out.push(FieldRequirement::Equals(
                trim_key(k, c, "fieldSelector")?,
                v.trim().to_string(),
            ));
        } else if let Some((k, v)) = c.split_once('=') {
            out.push(FieldRequirement::Equals(
                trim_key(k, c, "fieldSelector")?,
                v.trim().to_string(),
            ));
        } else {
            return Err(bad_selector("fieldSelector", c, "expected k=v / k!=v"));
        }
    }
    Ok(out)
}

/// Trim a selector clause's key + reject an empty one.
fn trim_key(k: &str, clause: &str, what: &str) -> Result<String, ApiError> {
    let k = k.trim();
    if k.is_empty() {
        return Err(bad_selector(what, clause, "empty key"));
    }
    Ok(k.to_string())
}

fn bad_selector(what: &str, clause: &str, why: &str) -> ApiError {
    ApiError::BadRequest(format!("invalid {what} clause {clause:?}: {why}"))
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
    fn limit_parses_with_k8s_semantics() {
        let p = |s: Option<&str>| ListWatchParams {
            limit: s.map(str::to_string),
            ..Default::default()
        };
        assert_eq!(p(None).limit().unwrap(), 0);
        assert_eq!(p(Some("")).limit().unwrap(), 0);
        assert_eq!(p(Some("0")).limit().unwrap(), 0);
        assert_eq!(p(Some("25")).limit().unwrap(), 25);
        assert!(matches!(
            p(Some("nope")).limit(),
            Err(ApiError::BadRequest(_))
        ));
    }

    #[test]
    fn continue_token_accessor_round_trips_and_rejects_garbage() {
        let token = ContinueToken::new(
            Revision(9),
            ResourceKey::namespaced("", "v1", "Pod", "default", "p3"),
        );
        let encoded = token.encode();
        let p = ListWatchParams {
            continue_: Some(encoded),
            ..Default::default()
        };
        assert_eq!(p.continue_token().unwrap(), Some(token));

        // Absent / empty → None.
        assert_eq!(ListWatchParams::default().continue_token().unwrap(), None);

        // Garbage → Gone (410).
        let bad = ListWatchParams {
            continue_: Some("not-a-token".into()),
            ..Default::default()
        };
        assert!(matches!(bad.continue_token(), Err(ApiError::Gone(_))));
    }

    #[test]
    fn delete_precondition_parses() {
        let p = |s: Option<&str>| ListWatchParams {
            resource_version: s.map(str::to_string),
            ..Default::default()
        };
        assert_eq!(p(None).precondition().unwrap(), None);
        assert_eq!(p(Some("")).precondition().unwrap(), None);
        assert_eq!(p(Some("0")).precondition().unwrap(), None);
        assert_eq!(p(Some("7")).precondition().unwrap(), Some(Revision(7)));
        assert!(matches!(
            p(Some("x")).precondition(),
            Err(ApiError::BadRequest(_))
        ));
    }

    #[test]
    fn body_precondition_reads_metadata_resource_version() {
        // Absent → None.
        assert_eq!(
            body_precondition(&serde_json::json!({"metadata": {"name": "p"}})).unwrap(),
            None
        );
        // String "5" → Some(5).
        assert_eq!(
            body_precondition(&serde_json::json!({"metadata": {"resourceVersion": "5"}})).unwrap(),
            Some(Revision(5))
        );
        // Empty string → None.
        assert_eq!(
            body_precondition(&serde_json::json!({"metadata": {"resourceVersion": ""}})).unwrap(),
            None
        );
        // Malformed string → BadRequest.
        assert!(matches!(
            body_precondition(&serde_json::json!({"metadata": {"resourceVersion": "abc"}})),
            Err(ApiError::BadRequest(_))
        ));
        // Non-string → BadRequest.
        assert!(matches!(
            body_precondition(&serde_json::json!({"metadata": {"resourceVersion": 5}})),
            Err(ApiError::BadRequest(_))
        ));
    }

    #[test]
    fn apply_params_parse_field_manager_and_force() {
        let p: ApplyParams = serde_urlencoded::from_str("fieldManager=test&force=true").unwrap();
        let opts = ApplyOptions::from_params(&p).unwrap();
        assert_eq!(opts.manager, "test");
        assert!(opts.force);

        // force absent → false.
        let p: ApplyParams = serde_urlencoded::from_str("fieldManager=test").unwrap();
        let opts = ApplyOptions::from_params(&p).unwrap();
        assert!(!opts.force);

        // force=false explicit.
        let p: ApplyParams = serde_urlencoded::from_str("fieldManager=test&force=false").unwrap();
        assert!(!ApplyOptions::from_params(&p).unwrap().force);
    }

    #[test]
    fn apply_options_requires_field_manager() {
        // Absent fieldManager → BadRequest (422/400).
        let p: ApplyParams = serde_urlencoded::from_str("force=true").unwrap();
        assert!(matches!(
            ApplyOptions::from_params(&p),
            Err(ApiError::BadRequest(_))
        ));
        // Empty fieldManager → BadRequest.
        let p: ApplyParams = serde_urlencoded::from_str("fieldManager=").unwrap();
        assert!(matches!(
            ApplyOptions::from_params(&p),
            Err(ApiError::BadRequest(_))
        ));
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
        let p: ListWatchParams = serde_urlencoded::from_str("allowWatchBookmarks=").unwrap();
        assert!(p.allow_watch_bookmarks);
        let p: ListWatchParams = serde_urlencoded::from_str("allowWatchBookmarks=false").unwrap();
        assert!(!p.allow_watch_bookmarks);
        let p: ListWatchParams = serde_urlencoded::from_str("allowWatchBookmarks=true").unwrap();
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

        let wrong_label = serde_json::json!({"metadata": {"name": "p1", "labels": {"app": "api"}}});
        assert!(!sel.matches(&wrong_label));

        let wrong_name = serde_json::json!({"metadata": {"name": "p2", "labels": {"app": "web", "tier": "front"}}});
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
        // An empty KEY is malformed (a bare `app` is now a valid Exists
        // selector — see `label_selector_full_grammar`).
        let p = ListWatchParams {
            label_selector: Some("=web".into()),
            ..Default::default()
        };
        assert!(matches!(p.selectors(), Err(ApiError::BadRequest(_))));
        // A set clause with a bad operator is malformed.
        let p = ListWatchParams {
            label_selector: Some("k blah (a,b)".into()),
            ..Default::default()
        };
        assert!(matches!(p.selectors(), Err(ApiError::BadRequest(_))));
    }

    /// The FULL K8s label-selector grammar the typed parser + matcher now
    /// honor (equality `=`/`==`/`!=` + set-based `in`/`notin`/exists/
    /// not-exists), matching how kubectl + every controller select objects.
    #[test]
    fn label_selector_full_grammar() {
        let parse = |s: &str| {
            ListWatchParams {
                label_selector: Some(s.into()),
                ..Default::default()
            }
            .selectors()
            .unwrap()
        };
        let web = serde_json::json!({"metadata": {"labels": {"tier": "web", "env": "prod"}}});
        let api = serde_json::json!({"metadata": {"labels": {"tier": "api"}}});
        let bare = serde_json::json!({"metadata": {"name": "x"}}); // no labels

        // exists / not-exists.
        assert!(parse("tier").matches(&web));
        assert!(!parse("tier").matches(&bare));
        assert!(parse("!tier").matches(&bare));
        assert!(!parse("!tier").matches(&web));

        // set-based in / notin (the comma inside (...) is NOT a clause split).
        assert!(parse("tier in (web,api)").matches(&web));
        assert!(parse("tier in (web,api)").matches(&api));
        assert!(!parse("tier notin (web)").matches(&web));
        assert!(parse("tier notin (web)").matches(&api));
        // notin includes objects LACKING the key (k8s semantics).
        assert!(parse("tier notin (web)").matches(&bare));

        // inequality includes objects LACKING the key.
        assert!(parse("tier!=web").matches(&api));
        assert!(parse("tier!=web").matches(&bare));
        assert!(!parse("tier!=web").matches(&web));

        // ANDed multi-clause spanning a set + an equality.
        let sel = parse("tier in (web,api),env=prod");
        assert_eq!(sel.labels.len(), 2);
        assert!(sel.matches(&web)); // tier=web ∈ set AND env=prod
        assert!(!sel.matches(&api)); // api lacks env=prod
    }

    /// Field selectors: equality on the two core keys; `!=` supported; an
    /// unsupported key filters the object out (per-kind registration pending).
    #[test]
    fn field_selector_grammar() {
        let parse = |s: &str| {
            ListWatchParams {
                field_selector: Some(s.into()),
                ..Default::default()
            }
            .selectors()
            .unwrap()
        };
        let p1 = serde_json::json!({"metadata": {"name": "p1", "namespace": "ns1"}});
        assert!(parse("metadata.name=p1").matches(&p1));
        assert!(!parse("metadata.name=p2").matches(&p1));
        assert!(parse("metadata.name!=p2").matches(&p1));
        assert!(parse("metadata.namespace=ns1").matches(&p1));
        // Unsupported field key → filtered out (no match).
        assert!(!parse("status.phase=Running").matches(&p1));
    }

    #[test]
    fn gvk_ns_match_filters_by_kind_and_namespace() {
        let pod = ResourceKey::namespaced("", "v1", "Pod", "default", "p");
        assert!(gvk_ns_matches(&pod, "", "v1", "Pod", Some("default")));
        // wrong kind
        assert!(!gvk_ns_matches(
            &pod,
            "",
            "v1",
            "ConfigMap",
            Some("default")
        ));
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
