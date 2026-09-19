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

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use engenho_store::resource::ResourceKey;
use engenho_store::watch::WatchEvent;
use engenho_store::{ContinueToken, Revision, WatchEventKind};

use crate::error::ApiError;
use crate::watch_end::Compacted;

/// Raw list/watch query string params, K8s-shaped.
///
/// `resourceVersion`, `timeoutSeconds`, `limit`, `continue` are kept as
/// strings (K8s wire types) and interpreted by typed accessors so a
/// malformed value is a typed 400, not a serde reject.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default)]
pub struct ListWatchParams {
    // No `watch` field. Whether a GET streams is decided ONCE, by
    // `crate::coords::RequestInfo` in the request-info layer, and the
    // dispatcher reads `RequestInfo::is_watch` — the verb authz judged. A
    // second read here is how a list-only grant used to open a stream
    // (`?watch=yes`: authz saw `list`, this struct saw `true`).
    /// `resourceVersion=` — string per K8s ("" / absent / "0" / "N").
    #[serde(rename = "resourceVersion")]
    pub resource_version: Option<String>,
    /// `labelSelector=k1=v1,k2=v2`.
    #[serde(rename = "labelSelector")]
    pub label_selector: Option<String>,
    /// `fieldSelector=metadata.name=x,metadata.namespace=y`.
    #[serde(rename = "fieldSelector")]
    pub field_selector: Option<String>,
    /// `allowWatchBookmarks=true` — K8s opt-in. Absent is `false`, upstream's
    /// default (the struct's `serde(default)`); a present key is `true`
    /// unless its value is `false`, `0` or `no`, so an empty value is `true`.
    #[serde(
        rename = "allowWatchBookmarks",
        deserialize_with = "de_bool_default_true"
    )]
    pub allow_watch_bookmarks: bool,
    /// `sendInitialEvents=` — Kubernetes 1.27 **streaming lists**. `true`:
    /// the server replays current state as `ADDED` events, then (when the
    /// client asked for bookmarks) emits a BOOKMARK annotated
    /// [`INITIAL_EVENTS_END_ANNOTATION`]; the client never issues a separate
    /// LIST.
    ///
    /// Three states, because upstream draws the line at presence: absent
    /// lets [`Self::watch_initial_events`] default it (a watch from `""` or
    /// `"0"` gets the snapshot), and `false` is not the same as absent (a
    /// watch from `"0"` with `false` starts now with no state). Read it only
    /// through [`Self::watch_initial_events`] and [`Self::validate_list`].
    ///
    /// This is the DEFAULT path for `kube-rs`'s `watcher` under
    /// `Config::streaming_lists()`. Until 2026-08-08 engenho parsed no such
    /// param and silently ignored it, so a streaming-list client received a
    /// lone bookmark, zero objects, and never left its initializing state.
    #[serde(rename = "sendInitialEvents", deserialize_with = "de_present_bool")]
    pub send_initial_events: Option<bool>,
    /// `resourceVersionMatch=` — `NotOlderThan` or `Exact`. On a watch it is
    /// legal only as `NotOlderThan` alongside `sendInitialEvents`, and the
    /// snapshot is then taken at the store's current revision, which is not
    /// older than any `resourceVersion` the store has reached; one ahead of
    /// the snapshot is refused in-band with a 410
    /// ([`crate::watch_start::WatchRefusal`]). On a LIST, any
    /// `resourceVersion` ahead of the store waits for it
    /// ([`crate::list_floor`]); `Exact` is validated but served as
    /// `NotOlderThan` (T3.9b).
    #[serde(rename = "resourceVersionMatch")]
    pub resource_version_match: Option<String>,
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

    /// The `resourceVersionMatch` the client sent, with an empty value read
    /// as none (upstream tests `len(match) > 0`).
    fn version_match(&self) -> Option<&str> {
        self.resource_version_match
            .as_deref()
            .filter(|m| !m.is_empty())
    }

    /// Whether the client sent a `continue` token.
    fn has_continue(&self) -> bool {
        self.continue_.as_deref().is_some_and(|c| !c.is_empty())
    }

    /// Validate a WATCH's options and decide what it sends before its first
    /// change. This is kube-apiserver v1.34's `SetListOptionsDefaults`
    /// (`WatchList` is on by default) followed by `validateWatchOptions`,
    /// evaluated once, here:
    ///
    ///   * a watch that names neither `sendInitialEvents` nor
    ///     `resourceVersionMatch`, from `resourceVersion` absent, `""` or
    ///     `"0"`, is defaulted to `sendInitialEvents=true` +
    ///     `NotOlderThan`: it begins with a synthetic `ADDED` for every
    ///     object, as the API docs promise for both;
    ///   * `sendInitialEvents` (either value) requires
    ///     `resourceVersionMatch=NotOlderThan`, and `resourceVersionMatch`
    ///     requires `sendInitialEvents`;
    ///   * the snapshot ends with an `initial-events-end` BOOKMARK only when
    ///     the client asked for bookmarks.
    ///
    /// # Errors
    ///
    /// [`InvalidListOptions`] listing every rule the options break, which
    /// the router answers with a 422 before any watch exists.
    pub fn watch_initial_events(&self) -> Result<InitialEvents, InvalidListOptions> {
        let legacy = matches!(self.resource_version.as_deref(), None | Some("" | "0"));
        let (send, version_match) = match (self.send_initial_events, self.version_match()) {
            (None, None) if legacy => (Some(true), Some(NOT_OLDER_THAN)),
            given => given,
        };
        let mut violations = Vec::new();
        if send.is_some() && version_match != Some(NOT_OLDER_THAN) {
            violations.push(OptionViolation::InitialEventsNeedNotOlderThan);
        }
        if let Some(m) = version_match {
            if send.is_none() {
                violations.push(OptionViolation::MatchNeedsInitialEvents);
            }
            if m != NOT_OLDER_THAN {
                violations.push(OptionViolation::WatchMatchNotSupported(m.to_owned()));
            }
            if self.has_continue() {
                violations.push(OptionViolation::MatchWithContinue);
            }
        }
        InvalidListOptions::check(violations)?;
        Ok(match send {
            Some(true) => InitialEvents::Snapshot {
                end_bookmark: self.allow_watch_bookmarks,
            },
            Some(false) | None => InitialEvents::None,
        })
    }

    /// Validate a LIST's options: kube-apiserver v1.34's
    /// `ValidateListOptions` for a request that is not a watch.
    ///
    /// # Errors
    ///
    /// [`InvalidListOptions`] listing every rule the options break.
    pub fn validate_list(&self) -> Result<(), InvalidListOptions> {
        let mut violations = Vec::new();
        if let Some(m) = self.version_match() {
            if self.resource_version.as_deref().is_none_or(str::is_empty) {
                violations.push(OptionViolation::MatchNeedsResourceVersion);
            }
            if self.has_continue() {
                violations.push(OptionViolation::MatchWithContinue);
            }
            if m != EXACT && m != NOT_OLDER_THAN {
                violations.push(OptionViolation::ListMatchNotSupported(m.to_owned()));
            }
            if m == EXACT && self.resource_version.as_deref() == Some("0") {
                violations.push(OptionViolation::ExactAtZero);
            }
        }
        if self.send_initial_events.is_some() {
            violations.push(OptionViolation::InitialEventsOnList);
        }
        InvalidListOptions::check(violations)
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

    /// `timeoutSeconds` as a typed duration. Absent / empty / `"0"` =>
    /// `None`, meaning "no server-side deadline, stream until the client
    /// goes away" — which is also K8s's meaning for an absent value.
    ///
    /// K8s treats this as the interval after which the server CLOSES the
    /// watch cleanly, so the client re-LISTs and re-WATCHes. Honouring it
    /// is what keeps a long-running client from having to rely on its own
    /// read timeout: before this accessor existed the field was parsed and
    /// read by nobody, so `?timeoutSeconds=N` was silently discarded and a
    /// kube-rs controller churned on `hyper::Error(Body, Kind(TimedOut))`
    /// instead (~28/hour, measured against a live engenho 2026-09-06).
    ///
    /// # Errors
    ///
    /// [`ApiError::BadRequest`] when the value is present but not a
    /// non-negative integer — a typed 400 rather than a serde reject, the
    /// same shape as [`Self::limit`].
    pub fn timeout(&self) -> Result<Option<std::time::Duration>, ApiError> {
        match self.timeout_seconds.as_deref() {
            None | Some("") | Some("0") => Ok(None),
            Some(s) => s
                .parse::<u64>()
                .map(|n| Some(std::time::Duration::from_secs(n)))
                .map_err(|_| {
                    ApiError::BadRequest(format!(
                        "invalid timeoutSeconds: {s:?} (must be a non-negative integer)"
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
/// `?dryRun=` — whether a write is EVALUATED but never persisted.
///
/// # Why this is a typed two-variant enum and not a `bool`
///
/// Kubernetes defines exactly one legal value, `All`. Anything else is a 400,
/// **not** a silent "false" — and that distinction is the whole safety
/// property. Until 2026-08-09 engenho did not parse this parameter at all
/// (`rg dry_run` over the apiserver and store returned ZERO hits), so a
/// `?dryRun=All` write was COMMITTED: measured, `kubectl create --dry-run=server`
/// persisted the pod, and the follow-up real create failed `AlreadyExists`.
/// The same hole meant a dry-run DELETE really deleted.
///
/// An unparseable value returning `Off` would rebuild exactly that hole for
/// anyone who typos `?dryRun=true`, so [`DryRun::parse`] refuses instead.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DryRun {
    /// Persist normally.
    #[default]
    Off,
    /// Run every check, persist NOTHING, return the object that would have
    /// been written.
    All,
}

impl DryRun {
    /// Interpret the raw `?dryRun=` value.
    ///
    /// Absent or empty → [`DryRun::Off`]. `All` → [`DryRun::All`]. Anything
    /// else is a typed 400 — never a silent downgrade to a real write.
    ///
    /// # Errors
    ///
    /// [`ApiError::BadRequest`] for any value other than `All`.
    pub fn parse(raw: Option<&str>) -> Result<Self, ApiError> {
        match raw {
            None | Some("") => Ok(Self::Off),
            Some("All") => Ok(Self::All),
            Some(other) => Err(ApiError::BadRequest(format!(
                "invalid dryRun value {other:?}: the only supported value is \"All\""
            ))),
        }
    }

    /// True when nothing may be persisted.
    #[must_use]
    pub fn is_dry(self) -> bool {
        matches!(self, Self::All)
    }

    /// Resolve dry-run for a **DELETE**, which carries it in the request BODY
    /// rather than (only) the query string.
    ///
    /// `kubectl delete --dry-run=server` sends `DeleteOptions` as the body and
    /// **no** `?dryRun=` at all — measured with `-v=8`:
    ///
    /// ```text
    /// Request Body: {"propagationPolicy":"Background","dryRun":["All"]}
    /// url="https://…/api/v1/namespaces/dr2/configmaps/keep"
    /// ```
    ///
    /// A query-parameter-only implementation therefore looks correct, compiles,
    /// passes its unit tests, and **still really deletes** — measured exactly
    /// that way on 2026-08-09 before this existed. `DeleteOptions.dryRun` is a
    /// `[]string`, so `["All"]` is the shape, and per K8s any entry other than
    /// `All` is invalid.
    ///
    /// Either source may set it; the query string is honoured too because a
    /// raw client may use it.
    ///
    /// # Errors
    ///
    /// [`ApiError::BadRequest`] when either source carries a value that is not
    /// `All`. A malformed body is NOT an error here — DELETE bodies are
    /// optional and a non-`DeleteOptions` body simply carries no dry-run.
    pub fn for_delete(query: Option<&str>, body: &[u8]) -> Result<Self, ApiError> {
        if Self::parse(query)?.is_dry() {
            return Ok(Self::All);
        }
        let Ok(v) = serde_json::from_slice::<serde_json::Value>(body) else {
            return Ok(Self::Off);
        };
        let Some(entries) = v.get("dryRun").and_then(serde_json::Value::as_array) else {
            return Ok(Self::Off);
        };
        let mut out = Self::Off;
        for e in entries {
            let raw = e.as_str().unwrap_or_default();
            if Self::parse(Some(raw))?.is_dry() {
                out = Self::All;
            }
        }
        Ok(out)
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default)]
pub struct ApplyParams {
    /// `?fieldManager=<name>` — the manager's identity (REQUIRED for apply).
    #[serde(rename = "fieldManager")]
    pub field_manager: Option<String>,
    /// `?force=true|1|yes` — take ownership of conflicting fields.
    #[serde(deserialize_with = "de_bool")]
    pub force: bool,
    /// `?dryRun=All` — evaluate, persist nothing. Interpreted by
    /// [`DryRun::parse`], which REFUSES any value other than `All`.
    #[serde(rename = "dryRun")]
    pub dry_run: Option<String>,
    /// `?fieldValidation=Ignore|Warn|Strict` — interpreted by
    /// [`crate::field_validation::Directive::parse`], which refuses any other
    /// value as upstream does.
    #[serde(rename = "fieldValidation")]
    pub field_validation: Option<String>,
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
    /// `resourceVersion` absent, `""` or `"0"` — from the store's current
    /// revision. Whether the watch first replays the current state as
    /// `ADDED` is not this value's call: [`InitialEvents`] decides it.
    MostRecent,
    /// `resourceVersion="N"` (N > 0) — replay `changes_since(N)` then
    /// live. A WATCH at an `N` the store has not reached, or has compacted
    /// away, is refused in-band with a 410
    /// ([`crate::watch_start::WatchRefusal`]); a LIST at an `N` the store
    /// has not reached waits for it ([`crate::list_floor`]).
    At(Revision),
}

impl ResumePoint {
    /// The refusal for a resume point the store has not reached, or `None`
    /// when a store at `current` can serve it. The one comparison both a
    /// WATCH ([`crate::watch_start::WatchRefusal::ahead_of`]) and a LIST
    /// ([`crate::list_floor`]) make.
    ///
    /// `MostRecent` is never ahead: it means "from wherever the store is
    /// now". An explicit `At(current)` is servable too.
    #[must_use]
    pub fn ahead_of(self, current: Revision) -> Option<TooLargeResourceVersion> {
        match self {
            Self::At(requested) if requested > current => {
                Some(TooLargeResourceVersion { requested, current })
            }
            Self::At(_) | Self::MostRecent => None,
        }
    }
}

/// A `resourceVersion` the store has not reached. Built only by
/// [`ResumePoint::ahead_of`], so `requested > current` always holds.
///
/// The text is kube-apiserver's (`storage.NewTooLargeResourceVersionError`).
/// A LIST renders it as upstream does, a 504 `Timeout` whose cause is
/// `ResourceVersionTooLarge`; a WATCH renders it as an in-band 410, a
/// deliberate deviation ([`crate::watch_start`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Too large resource version: {requested}, current: {current}")]
pub struct TooLargeResourceVersion {
    requested: Revision,
    current: Revision,
}

impl TooLargeResourceVersion {
    /// The revision the client named.
    #[must_use]
    pub fn requested(&self) -> Revision {
        self.requested
    }

    /// Where the store stood.
    #[must_use]
    pub fn current(&self) -> Revision {
        self.current
    }
}

/// `resourceVersionMatch=NotOlderThan`.
const NOT_OLDER_THAN: &str = "NotOlderThan";
/// `resourceVersionMatch=Exact`.
const EXACT: &str = "Exact";

/// What a WATCH sends before its first change, as
/// [`ListWatchParams::watch_initial_events`] decides it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitialEvents {
    /// Nothing: the watch streams the changes after its resume point.
    None,
    /// A synthetic `ADDED` for every object in a snapshot taken at the
    /// store's current revision, then the changes after that revision.
    Snapshot {
        /// Whether a BOOKMARK annotated [`INITIAL_EVENTS_END_ANNOTATION`]
        /// closes the snapshot: only for a client that asked for bookmarks.
        end_bookmark: bool,
    },
}

/// How a violated option is reported: upstream's `field.ErrorType`, for the
/// two types list/watch validation uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViolationKind {
    /// `field.Forbidden`.
    Forbidden,
    /// `field.NotSupported`.
    NotSupported,
}

/// One rule of kube-apiserver v1.34's `ValidateListOptions`
/// (`apimachinery/pkg/apis/meta/internalversion/validation`) that a request
/// broke. Each variant carries upstream's field and text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptionViolation {
    /// `sendInitialEvents` (either value) without
    /// `resourceVersionMatch=NotOlderThan`.
    InitialEventsNeedNotOlderThan,
    /// `resourceVersionMatch` on a watch that sets no `sendInitialEvents`.
    MatchNeedsInitialEvents,
    /// A watch's `resourceVersionMatch` other than `NotOlderThan`.
    WatchMatchNotSupported(String),
    /// `resourceVersionMatch` together with `continue`.
    MatchWithContinue,
    /// A LIST's `resourceVersionMatch` without a `resourceVersion`.
    MatchNeedsResourceVersion,
    /// A LIST's `resourceVersionMatch` other than `Exact` or `NotOlderThan`.
    ListMatchNotSupported(String),
    /// `resourceVersionMatch=Exact` with `resourceVersion=0`.
    ExactAtZero,
    /// `sendInitialEvents` on a LIST.
    InitialEventsOnList,
}

impl OptionViolation {
    /// The query parameter at fault.
    #[must_use]
    pub fn field(&self) -> &'static str {
        match self {
            Self::InitialEventsOnList => "sendInitialEvents",
            _ => "resourceVersionMatch",
        }
    }

    /// Forbidden or not supported.
    #[must_use]
    pub fn kind(&self) -> ViolationKind {
        match self {
            Self::WatchMatchNotSupported(_) | Self::ListMatchNotSupported(_) => {
                ViolationKind::NotSupported
            }
            _ => ViolationKind::Forbidden,
        }
    }

    /// For a not-supported value, the values that are.
    #[must_use]
    pub fn supported_values(&self) -> &'static [&'static str] {
        match self {
            Self::WatchMatchNotSupported(_) => &[NOT_OLDER_THAN],
            Self::ListMatchNotSupported(_) => &[EXACT, NOT_OLDER_THAN, ""],
            _ => &[],
        }
    }

    /// For a forbidden combination, upstream's detail text.
    #[must_use]
    pub fn forbidden_detail(&self) -> Option<&'static str> {
        match self {
            Self::InitialEventsNeedNotOlderThan => {
                Some("sendInitialEvents requires setting resourceVersionMatch to NotOlderThan")
            }
            Self::MatchNeedsInitialEvents => Some(
                "resourceVersionMatch is forbidden for watch unless sendInitialEvents is provided",
            ),
            Self::MatchWithContinue => {
                Some("resourceVersionMatch is forbidden when continue is provided")
            }
            Self::MatchNeedsResourceVersion => {
                Some("resourceVersionMatch is forbidden unless resourceVersion is provided")
            }
            Self::ExactAtZero => {
                Some("resourceVersionMatch \"exact\" is forbidden for resourceVersion \"0\"")
            }
            Self::InitialEventsOnList => Some("sendInitialEvents is forbidden for list"),
            Self::WatchMatchNotSupported(_) | Self::ListMatchNotSupported(_) => None,
        }
    }
}

/// Upstream's `field.Error.Error()`: `<field>: Forbidden: <detail>`, or
/// `<field>: Unsupported value: "<value>": supported values: "<a>", "<b>"`.
impl std::fmt::Display for OptionViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WatchMatchNotSupported(value) | Self::ListMatchNotSupported(value) => {
                write!(
                    f,
                    "{}: Unsupported value: {value:?}: supported values: ",
                    self.field()
                )?;
                for (i, supported) in self.supported_values().iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{supported:?}")?;
                }
                Ok(())
            }
            _ => write!(
                f,
                "{}: Forbidden: {}",
                self.field(),
                self.forbidden_detail().unwrap_or_default()
            ),
        }
    }
}

/// A request whose list/watch options break at least one of upstream's
/// rules. Never empty: [`Self::check`] is the only constructor, and it
/// returns `Ok` for no violations. Rendered as kube-apiserver renders
/// `errors.NewInvalid(ListOptions.meta.k8s.io, "", errs)`, a 422 `Invalid`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidListOptions(Vec<OptionViolation>);

impl InvalidListOptions {
    /// `Ok` when nothing was violated, otherwise the violations in the order
    /// upstream checks them.
    fn check(violations: Vec<OptionViolation>) -> Result<(), Self> {
        if violations.is_empty() {
            Ok(())
        } else {
            Err(Self(violations))
        }
    }

    /// Every rule broken, in upstream's order.
    #[must_use]
    pub fn violations(&self) -> &[OptionViolation] {
        &self.0
    }
}

/// `ListOptions.meta.k8s.io "" is invalid: <e>` for one violation, and
/// `... is invalid: [<e1>, <e2>]` for several (`utilerrors.NewAggregate`).
impl std::fmt::Display for InvalidListOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ListOptions.meta.k8s.io \"\" is invalid: ")?;
        match self.0.as_slice() {
            [one] => write!(f, "{one}"),
            many => {
                f.write_str("[")?;
                for (i, violation) in many.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{violation}")?;
                }
                f.write_str("]")
            }
        }
    }
}

impl std::error::Error for InvalidListOptions {}

impl From<InvalidListOptions> for ApiError {
    fn from(invalid: InvalidListOptions) -> Self {
        Self::Invalid(invalid.to_string())
    }
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
/// based only (`=`/`==`/`!=`).
///
/// ★ THE SUPPORTED SET IS AN ALLOWLIST, AND IT MATTERS WHICH FIELDS ARE ON
/// IT. `metadata.name`/`metadata.namespace` alone are not enough for a
/// working cluster: a kubelet lists its OWN pods with
/// `spec.nodeName=<node>`, and controllers and operators filter on
/// `status.phase`. Without those two, every such client silently receives
/// an empty list.
///
/// ★ AN UNSUPPORTED KEY FILTERS TO NOTHING, WHICH IS THE DANGEROUS
/// DIRECTION and is a KNOWN DIVERGENCE from upstream, which returns 400
/// for a field it has not registered. `--field-selector status.phase!=Running`
/// against an unsupported field yields an EMPTY list that reads exactly
/// like "no pods are failing" — a false all-clear. It is left as a
/// divergence rather than silently widened because matching-everything
/// would be equally wrong in the opposite direction; closing it properly
/// needs per-kind field registration, which needs the kind at parse time.
/// Recorded in the diff harness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldRequirement {
    /// `k=v` / `k==v`.
    Equals(String, String),
    /// `k!=v`.
    NotEquals(String, String),
}

/// Field-selector keys engenho honours, mirroring what upstream registers.
///
/// Deliberately a short explicit list rather than "any dotted path": an
/// open resolver would make a typo (`status.phse`) silently match nothing,
/// which is the same false all-clear this type's doc warns about, only
/// harder to notice because it looks like a supported field.
pub const SUPPORTED_FIELD_SELECTORS: &[&str] = &[
    // Honoured by every kind.
    "metadata.name",
    "metadata.namespace",
    // Pod — the two a working cluster cannot do without. A kubelet lists
    // its own pods by nodeName; controllers and operators filter on phase.
    "spec.nodeName",
    "status.phase",
    "spec.restartPolicy",
    "spec.schedulerName",
    "spec.serviceAccountName",
    "status.podIP",
    "status.nominatedNodeName",
    // Node.
    "spec.unschedulable",
    // Secret — `kubectl get secrets --field-selector type=...`.
    "type",
    // Event — how kubectl describe finds an object's events.
    "involvedObject.kind",
    "involvedObject.name",
    "involvedObject.namespace",
    "involvedObject.uid",
    "reason",
];

/// Resolve a dotted path to a string, coercing the scalar kinds a field
/// selector can legally compare against.
///
/// Booleans are stringified because `spec.unschedulable=true` arrives as
/// the STRING "true" on the wire while the object holds a JSON bool —
/// comparing them without coercion would never match, which is precisely
/// how a supported field can still behave as if unsupported.
fn resolve_path<'a>(obj: &'a serde_json::Value, path: &str) -> Option<std::borrow::Cow<'a, str>> {
    let mut cur = obj;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    match cur {
        serde_json::Value::String(s) => Some(std::borrow::Cow::Borrowed(s.as_str())),
        serde_json::Value::Bool(b) => Some(std::borrow::Cow::Owned(b.to_string())),
        serde_json::Value::Number(n) => Some(std::borrow::Cow::Owned(n.to_string())),
        _ => None,
    }
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
        let key = self.key();
        if !SUPPORTED_FIELD_SELECTORS.contains(&key) {
            return false; // unsupported field key → no match (see the type doc)
        }
        // Every supported key is a dotted path into the object, so one
        // resolver serves them all — a per-field `match` arm would drift
        // from the allowlist the moment a field is added to one and not
        // the other.
        let have = resolve_path(obj, key);
        let have = have.as_deref();
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

/// The `(apiVersion, kind)` a watch stream stamps onto every object it
/// emits.
///
/// A watch object is serialized **standalone**, so — unlike a LIST item,
/// whose TypeMeta the `<Kind>List` envelope carries — it MUST carry its own
/// `apiVersion`/`kind`. This is not cosmetic: `kube_core::watch::Bookmark`
/// holds a `#[serde(flatten)] TypeMeta`, so a bookmark without `apiVersion`
/// fails to deserialize with `missing field 'apiVersion'` and takes the whole
/// stream down with it. Measured 2026-08-08 against banken.
#[derive(Debug, Clone, Copy)]
pub struct WatchGvk<'a> {
    /// `"v1"` for the core group, `"<group>/<version>"` otherwise.
    pub api_version: &'a str,
    /// The singular kind — `"Pod"`, `"ConfigMap"`, …
    pub kind: &'a str,
}

/// The annotation kube-apiserver sets on the final initial-events BOOKMARK
/// to tell a streaming-list client that the replay is complete. A client
/// (kube-rs `watcher`, client-go reflector) stays in its "initializing"
/// state until it sees this, so omitting it means the informer never
/// becomes ready even when every object was delivered.
pub const INITIAL_EVENTS_END_ANNOTATION: &str = "k8s.io/initial-events-end";

/// The synthetic object a BOOKMARK line carries:
/// `{"kind":..,"apiVersion":..,"metadata":{"resourceVersion":"N"}}`.
#[derive(Serialize)]
struct K8sBookmarkObject<'a> {
    kind: &'a str,
    #[serde(rename = "apiVersion")]
    api_version: &'a str,
    metadata: K8sBookmarkMeta,
}

#[derive(Serialize)]
struct K8sBookmarkMeta {
    #[serde(rename = "resourceVersion")]
    resource_version: String,
    /// Omitted entirely unless this is the initial-events terminator —
    /// an empty annotations map on every ordinary bookmark would be noise
    /// on the wire that kube-apiserver does not emit either.
    #[serde(skip_serializing_if = "Option::is_none")]
    annotations: Option<BTreeMap<&'static str, &'static str>>,
}

/// Encode a watch `Event` as a newline-terminated K8s watch line —
/// `{"type":"ADDED|MODIFIED|DELETED","object":<resource>}\n`. Emits
/// ONLY `{type, object}`; `object` is `ev.object`, which already carries
/// `metadata.resourceVersion` stamped by the catalog.
#[must_use]
pub fn to_k8s_watch_line(ev: &WatchEvent, gvk: WatchGvk<'_>) -> Bytes {
    event_line(ev.kind, &ev.object, gvk)
}

/// Encode one `{"type":..,"object":..}` line, stamping `gvk` onto the object
/// when it does not already carry its own TypeMeta.
///
/// Shared by the live-stream path and the `sendInitialEvents` replay, so the
/// two cannot drift into emitting different shapes for the same object — the
/// replay is exactly what the live stream would have said.
#[must_use]
pub fn event_line(kind: WatchEventKind, object: &serde_json::Value, gvk: WatchGvk<'_>) -> Bytes {
    let stamped = stamp_type_meta(object, gvk);
    let line = K8sWatchLine {
        kind,
        object: &stamped,
    };
    encode_ndjson(&line)
}

/// Add `kind` + `apiVersion` to `v` if absent, leaving an object that already
/// declares its own TypeMeta untouched (a stored create/PUT body carries one).
fn stamp_type_meta(v: &serde_json::Value, gvk: WatchGvk<'_>) -> serde_json::Value {
    let mut out = v.clone();
    if let Some(obj) = out.as_object_mut() {
        obj.entry("kind".to_string())
            .or_insert_with(|| serde_json::Value::String(gvk.kind.to_owned()));
        obj.entry("apiVersion".to_string())
            .or_insert_with(|| serde_json::Value::String(gvk.api_version.to_owned()));
    }
    out
}

/// Encode a BOOKMARK line —
/// `{"type":"BOOKMARK","object":{"metadata":{"resourceVersion":"N"}}}\n`.
#[must_use]
pub fn bookmark_line(rev: Revision, gvk: WatchGvk<'_>, initial_events_end: bool) -> Bytes {
    let object = K8sBookmarkObject {
        kind: gvk.kind,
        api_version: gvk.api_version,
        metadata: K8sBookmarkMeta {
            resource_version: rev.to_string(),
            annotations: initial_events_end.then(|| {
                let mut m = BTreeMap::new();
                m.insert(INITIAL_EVENTS_END_ANNOTATION, "true");
                m
            }),
        },
    };
    let line = K8sWatchLine {
        kind: WatchEventKind::Bookmark,
        object: &serde_json::to_value(&object).unwrap_or(serde_json::Value::Null),
    };
    encode_ndjson(&line)
}

/// Encode the in-band `ERROR` line that ends a watch whose history was
/// compacted mid-stream: `Status{code: 410, reason: "Expired"}` with
/// kube-apiserver's `too old resource version: <requested> (<compacted>)`.
/// The response is already HTTP 200, so the end is carried in-band; the
/// client drops its cache and re-LISTs.
///
/// Takes a [`Compacted`], which only `WatchGone::CompactedTooOld` builds: an
/// overflow is not a compaction, and ends another way
/// ([`crate::watch_end`]).
#[must_use]
pub fn status_410_line(compacted: Compacted) -> Bytes {
    error_line(&crate::error::status_object(
        compacted.to_string(),
        410,
        "Expired",
    ))
}

/// Encode a `Status` object as a watch line of type `ERROR`, the shape
/// kube-apiserver uses for an in-band terminal status. Every in-band end of a
/// watch goes through here: the mid-stream 410 above, the no-progress 429
/// ([`crate::watch_end::NoProgress::status_line`]), a refused start or
/// resume ([`crate::watch_start::WatchRefusal::status_line`]) and an error
/// met while a watch resumes.
#[must_use]
pub(crate) fn error_line(status: &serde_json::Value) -> Bytes {
    #[derive(Serialize)]
    struct StatusLine<'a> {
        #[serde(rename = "type")]
        kind: &'static str,
        object: &'a serde_json::Value,
    }
    encode_ndjson(&StatusLine {
        kind: "ERROR",
        object: status,
    })
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

/// The ONE truth table for a boolean query flag: `true`, `1` or `yes` (the
/// value already percent-decoded) is set; anything else, including an empty
/// value, is not. Shared by every flag here and by the `watch` read in
/// [`crate::coords::RequestInfo::parse`], so a flag cannot mean one thing to
/// authz and another to dispatch.
#[must_use]
pub(crate) fn query_flag(value: &str) -> bool {
    matches!(value, "true" | "1" | "yes")
}

/// `?flag=true|1|yes` → true; absent → false (serde `default`).
fn de_bool<'de, D: serde::Deserializer<'de>>(de: D) -> Result<bool, D::Error> {
    let s = String::deserialize(de)?;
    Ok(query_flag(&s))
}

/// A flag whose ABSENCE means something of its own: present → `Some` of the
/// [`query_flag`] truth table, absent → `None` (serde `default`; this runs
/// only for a present key).
fn de_present_bool<'de, D: serde::Deserializer<'de>>(de: D) -> Result<Option<bool>, D::Error> {
    let s = String::deserialize(de)?;
    Ok(Some(query_flag(&s)))
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

    // ── field selectors (Phase 5.8) ───────────────────────────────────

    fn pod_on(node: &str, phase: &str) -> serde_json::Value {
        serde_json::json!({
            "metadata": { "name": "p", "namespace": "default" },
            "spec": { "nodeName": node, "schedulerName": "default-scheduler" },
            "status": { "phase": phase, "podIP": "10.0.0.1" }
        })
    }

    fn req(sel: &str) -> Vec<super::FieldRequirement> {
        super::parse_field_selector(Some(sel)).expect("parses")
    }

    fn matches(sel: &str, obj: &serde_json::Value) -> bool {
        req(sel).iter().all(|r| r.satisfied_by(obj))
    }

    #[test]
    fn a_kubelet_can_list_its_own_pods_by_node_name() {
        // Without this every kubelet silently receives an empty list.
        let p = pod_on("cid", "Running");
        assert!(matches("spec.nodeName=cid", &p));
        assert!(!matches("spec.nodeName=other", &p));
        assert!(matches("spec.nodeName!=other", &p));
    }

    #[test]
    fn controllers_can_filter_on_status_phase() {
        let running = pod_on("cid", "Running");
        let failed = pod_on("cid", "Failed");
        assert!(matches("status.phase=Running", &running));
        assert!(!matches("status.phase=Running", &failed));
        // The negated form is what an operator uses to find broken pods.
        assert!(matches("status.phase!=Running", &failed));
        assert!(!matches("status.phase!=Running", &running));
    }

    #[test]
    fn requirements_are_anded() {
        let p = pod_on("cid", "Running");
        assert!(matches("spec.nodeName=cid,status.phase=Running", &p));
        assert!(!matches("spec.nodeName=cid,status.phase=Failed", &p));
    }

    #[test]
    fn a_non_string_scalar_is_coerced_before_comparison() {
        // `spec.unschedulable=true` arrives as the STRING "true" while the
        // object holds a JSON bool. Without coercion a SUPPORTED field
        // behaves exactly as if it were unsupported.
        let node = serde_json::json!({
            "metadata": { "name": "cid" },
            "spec": { "unschedulable": true }
        });
        assert!(matches("spec.unschedulable=true", &node));
        assert!(!matches("spec.unschedulable=false", &node));
    }

    #[test]
    fn an_unsupported_key_matches_nothing_and_that_is_the_known_divergence() {
        // Upstream returns 400 here. engenho filters to empty, which reads
        // as a false all-clear — pinned so the behaviour is deliberate and
        // visible rather than discovered.
        let p = pod_on("cid", "Running");
        assert!(!matches("status.phse=Running", &p), "typo must not match");
        assert!(
            !matches("spec.madeUpField!=x", &p),
            "even the negated form matches nothing — the divergence is \
             symmetric, which is why widening it would be equally wrong"
        );
    }

    #[test]
    fn the_allowlist_and_the_resolver_cannot_drift() {
        // Every advertised key must actually resolve on an object that has
        // it — an entry in the list with no working path would advertise a
        // field that silently matches nothing.
        for key in super::SUPPORTED_FIELD_SELECTORS {
            assert!(
                !key.is_empty() && !key.starts_with('.') && !key.ends_with('.'),
                "malformed allowlist entry: {key}"
            );
        }
        let p = pod_on("cid", "Running");
        for key in [
            "metadata.name",
            "metadata.namespace",
            "spec.nodeName",
            "status.phase",
        ] {
            assert!(
                matches(&format!("{key}!=__definitely_not__"), &p),
                "{key} must resolve on a pod"
            );
        }
    }
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
    fn query_flag_truth_table() {
        for set in ["true", "1", "yes"] {
            assert!(query_flag(set), "{set:?} sets a flag");
        }
        for unset in ["false", "0", "no", "", "TRUE", "on"] {
            assert!(!query_flag(unset), "{unset:?} does not set a flag");
        }
    }

    #[test]
    fn list_watch_params_ignore_the_watch_key() {
        // `watch` is read once, by RequestInfo; the list/watch params accept
        // the key (every watch request carries it) without holding a copy.
        let p: ListWatchParams =
            serde_urlencoded::from_str("watch=true&labelSelector=app%3Dweb").unwrap();
        assert_eq!(p.label_selector.as_deref(), Some("app=web"));
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
        let bytes = to_k8s_watch_line(&ev, test_gvk());
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

    /// The core-group Pod GVK every watch-wire test stamps with.
    fn test_gvk() -> WatchGvk<'static> {
        WatchGvk {
            api_version: "v1",
            kind: "Pod",
        }
    }

    /// A watch object is serialized standalone, so it MUST carry its own
    /// TypeMeta — `kube_core::watch::Bookmark` flattens a `TypeMeta`, and a
    /// bookmark without `apiVersion` fails client deserialization outright.
    #[test]
    fn every_watch_object_carries_its_type_meta() {
        // An object with no TypeMeta of its own gets the stream's stamped on.
        let ev = WatchEvent {
            kind: WatchEventKind::Added,
            object: serde_json::json!({"metadata": {"resourceVersion": "7"}}),
            key: ResourceKey::namespaced("", "v1", "Pod", "default", "p"),
            resource_version: 7,
        };
        let bytes = to_k8s_watch_line(&ev, test_gvk());
        let v: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(&bytes).unwrap().trim_end()).unwrap();
        let obj = v.get("object").unwrap();
        assert_eq!(obj.get("apiVersion").unwrap(), "v1");
        assert_eq!(obj.get("kind").unwrap(), "Pod");

        // And so does a BOOKMARK — the case that actually broke kube-rs.
        let bytes = bookmark_line(Revision(99), test_gvk(), false);
        let v: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(&bytes).unwrap().trim_end()).unwrap();
        let obj = v.get("object").unwrap();
        assert_eq!(obj.get("apiVersion").unwrap(), "v1");
        assert_eq!(obj.get("kind").unwrap(), "Pod");
    }

    /// Only the initial-events terminator carries the annotation; an
    /// ordinary periodic bookmark must NOT, or a client would treat every
    /// bookmark as "the replay finished".
    #[test]
    fn only_the_initial_events_terminator_is_annotated() {
        let terminator = bookmark_line(Revision(5), test_gvk(), true);
        let v: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(&terminator).unwrap().trim_end()).unwrap();
        assert_eq!(
            v.pointer("/object/metadata/annotations")
                .and_then(|a| a.get(INITIAL_EVENTS_END_ANNOTATION))
                .and_then(serde_json::Value::as_str),
            Some("true"),
        );

        let ordinary = bookmark_line(Revision(5), test_gvk(), false);
        let v: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(&ordinary).unwrap().trim_end()).unwrap();
        assert!(
            v.pointer("/object/metadata/annotations").is_none(),
            "an ordinary bookmark must carry no annotations at all",
        );
    }

    /// `dryRun` accepts EXACTLY `All`. A typo must be a 400, never a silent
    /// real write — that silence is the whole defect this type closes.
    /// DELETE carries dry-run in the BODY. A query-only implementation looks
    /// right, compiles, passes its own unit tests — and really deletes.
    /// Measured exactly that way before `for_delete` existed.
    #[test]
    fn a_delete_reads_dry_run_from_the_delete_options_body() {
        // The literal body kubectl -v=8 showed.
        let body = br#"{"propagationPolicy":"Background","dryRun":["All"]}"#;
        assert!(
            DryRun::for_delete(None, body).unwrap().is_dry(),
            "DeleteOptions.dryRun MUST be honoured — the query string is empty \
             on a kubectl dry-run delete",
        );

        // An ordinary delete body carries no dryRun.
        let plain = br#"{"propagationPolicy":"Background"}"#;
        assert!(!DryRun::for_delete(None, plain).unwrap().is_dry());

        // A DELETE with no body at all is legal and is not a dry run.
        assert!(!DryRun::for_delete(None, b"").unwrap().is_dry());
        // Nor is a non-DeleteOptions body an error — DELETE bodies are optional.
        assert!(!DryRun::for_delete(None, b"not json").unwrap().is_dry());

        // The query string still works for a raw client.
        assert!(DryRun::for_delete(Some("All"), b"").unwrap().is_dry());

        // And a bogus value in EITHER source is still refused.
        assert!(DryRun::for_delete(Some("true"), b"").is_err());
        assert!(DryRun::for_delete(None, br#"{"dryRun":["true"]}"#).is_err());
    }

    #[test]
    fn dry_run_accepts_only_all_and_refuses_typos() {
        assert_eq!(DryRun::parse(None).unwrap(), DryRun::Off);
        assert_eq!(DryRun::parse(Some("")).unwrap(), DryRun::Off);
        assert_eq!(DryRun::parse(Some("All")).unwrap(), DryRun::All);
        assert!(DryRun::parse(Some("All")).unwrap().is_dry());
        assert!(!DryRun::parse(None).unwrap().is_dry());

        // The dangerous cases: anything else REFUSES rather than falling back
        // to Off, which would persist a write the operator asked to rehearse.
        for typo in ["true", "all", "ALL", "1", "yes", "None"] {
            let err = DryRun::parse(Some(typo)).expect_err(
                "a non-`All` dryRun value must be refused, not treated as a real write",
            );
            assert!(
                matches!(err, ApiError::BadRequest(_)),
                "{typo:?} must be a 400, got {err:?}"
            );
        }
    }

    #[test]
    fn send_initial_events_is_parsed() {
        let p: ListWatchParams = serde_urlencoded::from_str(
            "watch=true&sendInitialEvents=true&resourceVersionMatch=NotOlderThan",
        )
        .expect("streaming-list params parse");
        assert_eq!(
            p.send_initial_events,
            Some(true),
            "sendInitialEvents=true must parse"
        );
        assert_eq!(p.resource_version_match.as_deref(), Some("NotOlderThan"));

        let p: ListWatchParams =
            serde_urlencoded::from_str("sendInitialEvents=false").expect("explicit false parses");
        assert_eq!(p.send_initial_events, Some(false), "false is not absent");

        let p: ListWatchParams =
            serde_urlencoded::from_str("watch=true").expect("plain watch params parse");
        assert_eq!(
            p.send_initial_events, None,
            "absent sendInitialEvents stays absent, for watch_initial_events to default"
        );
    }

    fn initial(query: &str) -> Result<InitialEvents, InvalidListOptions> {
        serde_urlencoded::from_str::<ListWatchParams>(query)
            .expect("params parse")
            .watch_initial_events()
    }

    /// kube-apiserver v1.34 defaults a watch from `""`/`"0"` that names
    /// neither option to `sendInitialEvents=true` + `NotOlderThan`
    /// (`SetListOptionsDefaults`), so it begins with the current state.
    #[test]
    fn a_legacy_watch_is_defaulted_to_a_snapshot() {
        for rv in ["", "resourceVersion=", "resourceVersion=0"] {
            assert_eq!(
                initial(rv),
                Ok(InitialEvents::Snapshot {
                    end_bookmark: false
                }),
                "{rv:?}: no allowWatchBookmarks, no end bookmark (upstream's default is false)"
            );
            let bookmarks = format!("{rv}&allowWatchBookmarks=true");
            assert_eq!(
                initial(&bookmarks),
                Ok(InitialEvents::Snapshot { end_bookmark: true }),
                "{bookmarks:?}: the end bookmark for a client that asked"
            );
        }
        assert_eq!(
            initial("resourceVersion=7"),
            Ok(InitialEvents::None),
            "a watch from an explicit revision replays changes, not state"
        );
        assert_eq!(
            initial("resourceVersion=0&sendInitialEvents=false&resourceVersionMatch=NotOlderThan"),
            Ok(InitialEvents::None),
            "an explicit false is honoured: from now, no state"
        );
        assert_eq!(
            initial(
                "resourceVersion=7&sendInitialEvents=true&resourceVersionMatch=NotOlderThan\
                 &allowWatchBookmarks=true"
            ),
            Ok(InitialEvents::Snapshot { end_bookmark: true }),
            "kube-rs's streaming list, from a revision"
        );
    }

    fn rendered(result: Result<(), InvalidListOptions>) -> String {
        result.map_or_else(|e| e.to_string(), |()| "valid".to_owned())
    }

    #[test]
    fn watch_options_break_upstreams_rules_with_upstreams_text() {
        assert_eq!(
            rendered(initial("sendInitialEvents=true").map(drop)),
            "ListOptions.meta.k8s.io \"\" is invalid: resourceVersionMatch: Forbidden: \
             sendInitialEvents requires setting resourceVersionMatch to NotOlderThan"
        );
        assert_eq!(
            rendered(initial("sendInitialEvents=false").map(drop)),
            "ListOptions.meta.k8s.io \"\" is invalid: resourceVersionMatch: Forbidden: \
             sendInitialEvents requires setting resourceVersionMatch to NotOlderThan",
            "either value of sendInitialEvents needs NotOlderThan"
        );
        assert_eq!(
            rendered(initial("resourceVersionMatch=NotOlderThan").map(drop)),
            "ListOptions.meta.k8s.io \"\" is invalid: resourceVersionMatch: Forbidden: \
             resourceVersionMatch is forbidden for watch unless sendInitialEvents is provided"
        );
        assert_eq!(
            rendered(initial("sendInitialEvents=true&resourceVersionMatch=Exact").map(drop)),
            "ListOptions.meta.k8s.io \"\" is invalid: [resourceVersionMatch: Forbidden: \
             sendInitialEvents requires setting resourceVersionMatch to NotOlderThan, \
             resourceVersionMatch: Unsupported value: \"Exact\": supported values: \
             \"NotOlderThan\"]"
        );
    }

    #[test]
    fn list_options_break_upstreams_rules() {
        let list = |query: &str| {
            rendered(
                serde_urlencoded::from_str::<ListWatchParams>(query)
                    .expect("params parse")
                    .validate_list(),
            )
        };
        assert_eq!(
            list("resourceVersion=5&resourceVersionMatch=Exact"),
            "valid"
        );
        assert_eq!(
            list("resourceVersion=5&resourceVersionMatch=NotOlderThan"),
            "valid"
        );
        assert_eq!(
            list("sendInitialEvents=true"),
            "ListOptions.meta.k8s.io \"\" is invalid: sendInitialEvents: Forbidden: \
             sendInitialEvents is forbidden for list"
        );
        assert_eq!(
            list("resourceVersionMatch=NotOlderThan"),
            "ListOptions.meta.k8s.io \"\" is invalid: resourceVersionMatch: Forbidden: \
             resourceVersionMatch is forbidden unless resourceVersion is provided"
        );
        assert_eq!(
            list("resourceVersion=0&resourceVersionMatch=Exact"),
            "ListOptions.meta.k8s.io \"\" is invalid: resourceVersionMatch: Forbidden: \
             resourceVersionMatch \"exact\" is forbidden for resourceVersion \"0\""
        );
        assert_eq!(
            list("resourceVersion=5&resourceVersionMatch=Newest"),
            "ListOptions.meta.k8s.io \"\" is invalid: resourceVersionMatch: Unsupported \
             value: \"Newest\": supported values: \"Exact\", \"NotOlderThan\", \"\""
        );
    }

    #[test]
    fn only_a_resume_point_past_the_store_is_too_large() {
        let too_large = ResumePoint::At(Revision(9))
            .ahead_of(Revision(8))
            .expect("one past the store is ahead");
        assert_eq!(
            (too_large.requested(), too_large.current()),
            (Revision(9), Revision(8))
        );
        assert_eq!(
            too_large.to_string(),
            "Too large resource version: 9, current: 8"
        );
        assert_eq!(ResumePoint::At(Revision(8)).ahead_of(Revision(8)), None);
        assert_eq!(ResumePoint::MostRecent.ahead_of(Revision(0)), None);
    }

    #[test]
    fn bookmark_line_shape() {
        let bytes = bookmark_line(Revision(99), test_gvk(), false);
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
        let compacted = Compacted::try_from(engenho_store::WatchGone::CompactedTooOld {
            requested: Revision(3),
            compacted: Revision(6),
        })
        .unwrap();
        let bytes = status_410_line(compacted);
        let s = std::str::from_utf8(&bytes).unwrap();
        let v: serde_json::Value = serde_json::from_str(s.trim_end()).unwrap();
        assert_eq!(v.get("type").unwrap(), "ERROR");
        let obj = v.get("object").unwrap();
        assert_eq!(obj.get("kind").unwrap(), "Status");
        assert_eq!(obj.get("code").unwrap(), 410);
        assert_eq!(obj.get("reason").unwrap(), "Expired");
        assert_eq!(
            obj.get("message").unwrap(),
            "too old resource version: 3 (6)"
        );
    }
}
