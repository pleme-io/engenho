//! What client-go (v1.34.0) and kube-rs (4.2.0) do with a watch's end.
//!
//! A model of the two decisions that matter to a server: which `Status` a
//! client reads out of what it received, and whether it then re-watches the
//! same `resourceVersion` or relists. Each function names the upstream line
//! it follows. The oracle tables check the model row by row
//! (`oracle_watch_429.rs`, and the client rows of `oracle_watch_410.rs`), so
//! the model is pinned to upstream before anything is concluded from it
//! about engenho.
//!
//! Timing (backoff schedules, jitter, retry windows) is not modelled: it
//! decides how soon a client comes back, never where it comes back to.

use serde_json::Value;

/// A `metav1.Status` as a client decodes it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Status {
    pub status: String,
    pub code: i64,
    pub reason: String,
    pub message: String,
    /// `details` was present (client-go distinguishes nil from empty).
    pub details: bool,
    /// Each cause's type (`reason` on the wire, `type` in some fixtures).
    pub cause_types: Vec<String>,
    /// Each cause's message.
    pub cause_messages: Vec<String>,
    pub retry_after_seconds: i64,
}

impl Status {
    /// A Status from a code and a reason, nothing else.
    pub fn of(code: i64, reason: &str) -> Self {
        Self {
            status: "Failure".to_owned(),
            code,
            reason: reason.to_owned(),
            ..Self::default()
        }
    }

    /// A Status read from its JSON, wire form or fixture form.
    pub fn from_json(v: &Value) -> Self {
        let text = |key: &str| v[key].as_str().unwrap_or_default().to_owned();
        let details = &v["details"];
        let causes = details["causes"].as_array().cloned().unwrap_or_default();
        Self {
            // Read as sent: client-go checks `status: Failure` before it lets a
            // body Status replace an HTTP error (request.go L1506).
            status: text("status"),
            code: v["code"].as_i64().unwrap_or(0),
            reason: text("reason"),
            message: text("message"),
            details: details.is_object(),
            cause_types: causes
                .iter()
                .filter_map(|c| c["reason"].as_str().or_else(|| c["type"].as_str()))
                .map(str::to_owned)
                .collect(),
            cause_messages: causes
                .iter()
                .filter_map(|c| c["message"].as_str())
                .map(str::to_owned)
                .collect(),
            retry_after_seconds: details["retryAfterSeconds"].as_i64().unwrap_or(0),
        }
    }
}

pub mod client_go {
    //! client-go and apimachinery v1.34.0.

    use super::Status;
    use serde_json::Value;

    /// `knownReasons`, errors.go L47-68. `""` (Unknown) is not in it.
    const KNOWN_REASONS: [&str; 19] = [
        "Unauthorized",
        "Forbidden",
        "NotFound",
        "AlreadyExists",
        "Conflict",
        "Gone",
        "Invalid",
        "ServerTimeout",
        "StoreReadError",
        "Timeout",
        "TooManyRequests",
        "BadRequest",
        "MethodNotAllowed",
        "NotAcceptable",
        "RequestEntityTooLarge",
        "UnsupportedMediaType",
        "InternalError",
        "Expired",
        "ServiceUnavailable",
    ];

    fn known(reason: &str) -> bool {
        KNOWN_REASONS.contains(&reason)
    }

    /// errors.go L724: the code fallback ignores a known reason.
    pub fn is_too_many_requests(s: &Status) -> bool {
        s.reason == "TooManyRequests" || s.code == 429
    }

    /// errors.go L573.
    pub fn is_gone(s: &Status) -> bool {
        s.reason == "Gone" || (!known(&s.reason) && s.code == 410)
    }

    /// errors.go L587: reason only.
    pub fn is_resource_expired(s: &Status) -> bool {
        s.reason == "Expired"
    }

    /// errors.go L710.
    pub fn is_internal_error(s: &Status) -> bool {
        s.reason == "InternalError" || (!known(&s.reason) && s.code == 500)
    }

    /// errors.go L688.
    pub fn is_timeout(s: &Status) -> bool {
        s.reason == "Timeout" || (!known(&s.reason) && s.code == 504)
    }

    /// errors.go L702: reason only.
    pub fn is_server_timeout(s: &Status) -> bool {
        s.reason == "ServerTimeout"
    }

    /// reflector.go L1040.
    pub fn is_expired_error(s: &Status) -> bool {
        is_resource_expired(s) || is_gone(s)
    }

    /// reflector.go L1048-1076.
    pub fn is_too_large_resource_version(s: &Status) -> bool {
        if s.cause_types.iter().any(|t| t == "ResourceVersionTooLarge") {
            return true;
        }
        if !is_timeout(s) || !s.details {
            return false;
        }
        s.cause_messages
            .iter()
            .any(|m| m == "Too large resource version")
            || s.message.contains("Too large resource version")
    }

    /// The predicate named `name`, by its Go name.
    pub fn predicate(name: &str, s: &Status) -> Option<bool> {
        Some(match name {
            "IsTooManyRequests" => is_too_many_requests(s),
            "IsGone" => is_gone(s),
            "IsResourceExpired" => is_resource_expired(s),
            "IsInternalError" => is_internal_error(s),
            "IsTimeout" => is_timeout(s),
            "IsServerTimeout" => is_server_timeout(s),
            "isExpiredError" => is_expired_error(s),
            _ => return None,
        })
    }

    /// `apierrors.FromObject`, errors.go L123-142: the Status an in-band
    /// `ERROR` event carries, or `None` for an `UnexpectedObjectError`.
    pub fn from_object(object: &Value) -> Option<Status> {
        let api_version = object["apiVersion"].as_str().unwrap_or_default();
        (object["kind"] == "Status" && matches!(api_version, "v1" | "meta.k8s.io/v1"))
            .then(|| Status::from_json(object))
    }

    /// `NewGenericServerResponse`, errors.go L436-492: the reason client-go
    /// gives an HTTP error whose body is not a Status.
    pub fn generic_reason(code: i64) -> &'static str {
        match code {
            409 => "Conflict",
            404 => "NotFound",
            400 => "BadRequest",
            401 => "Unauthorized",
            403 => "Forbidden",
            406 => "NotAcceptable",
            415 => "UnsupportedMediaType",
            405 => "MethodNotAllowed",
            422 => "Invalid",
            503 => "ServiceUnavailable",
            504 => "Timeout",
            429 => "TooManyRequests",
            c if c >= 500 => "InternalError",
            _ => "",
        }
    }

    /// `retryAfterSeconds`, request.go L1378: `strconv.Atoi` of the header.
    pub fn retry_after_header(header: Option<&str>) -> Option<i64> {
        header.and_then(|h| h.parse::<i64>().ok())
    }

    /// `checkWait`, with_retry.go L309: a 429 or any 5xx, with a numeric
    /// `Retry-After`, is retried inside the REST client after that many
    /// seconds.
    pub fn check_wait(code: i64, retry_after: Option<&str>) -> Option<i64> {
        if code == 429 || code >= 500 {
            retry_after_header(retry_after)
        } else {
            None
        }
    }

    /// `NewRequest` maxRetries (request.go L166).
    pub const MAX_RETRIES: usize = 10;

    /// A watch's HTTP attempts when every attempt answers `code` with
    /// `retry_after`, and the sleep before each retry.
    pub fn rest_attempts(code: i64, retry_after: Option<&str>) -> (usize, Option<i64>) {
        match check_wait(code, retry_after) {
            Some(secs) => (1 + MAX_RETRIES, Some(secs.max(0))),
            None => (1, None),
        }
    }

    /// `Result.Error`, request.go L1489-1511: a JSON body that decodes to a
    /// `Status` with `status: Failure` replaces the error built from the
    /// HTTP code.
    pub fn result_error(code: i64, body: Option<&Value>) -> Status {
        if let Some(status) = body.map(Status::from_json)
            && status.status == "Failure"
        {
            return status;
        }
        let mut s = Status::of(code, generic_reason(code));
        s.details = true;
        s.cause_types = vec!["UnexpectedServerResponse".to_owned()];
        s
    }

    /// What the reflector does next.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Next {
        /// `watch()` returns; `BackoffUntil` backs off; `ListAndWatch` LISTs
        /// again, at the last seen `resourceVersion` first.
        Relist,
        /// Back off, then WATCH again from the last seen `resourceVersion`.
        BackoffRewatch,
        /// WATCH again at once from the last seen `resourceVersion`.
        Rewatch,
    }

    /// How a watch stream ended, as the reflector's `handleWatch` sees it.
    #[derive(Clone, Debug)]
    pub enum Ended {
        /// An in-band `ERROR` event carrying this object.
        Error(Value),
        /// The stream closed. `events` counts bookmarks and objects of the
        /// watched type; `short` is under one second since the request.
        Closed { events: usize, short: bool },
    }

    /// The watch path's switch, reflector.go L548-571, and
    /// `handleAnyWatch`'s very-short-watch rule (L975-978). The
    /// internal-error retry window is off in every informer.
    pub fn after_watch(ended: &Ended) -> (Next, &'static str) {
        let status = match ended {
            Ended::Closed {
                events: 0,
                short: true,
            } => return (Next::Relist, "default"),
            Ended::Closed { .. } => return (Next::Rewatch, "none"),
            Ended::Error(object) => from_object(object),
        };
        match status {
            Some(s) if is_expired_error(&s) => (Next::Relist, "isExpiredError"),
            Some(s) if is_too_many_requests(&s) => (Next::BackoffRewatch, "IsTooManyRequests"),
            _ => (Next::Relist, "default"),
        }
    }

    /// A watch-START error: `isWatchErrorRetriable` (reflector.go L1081,
    /// ECONNREFUSED or 429) backs off and re-watches; anything else is
    /// returned from `ListAndWatch`, which relists after a backoff.
    pub fn after_watch_start_error(s: &Status) -> Next {
        if is_too_many_requests(s) {
            Next::BackoffRewatch
        } else {
            Next::Relist
        }
    }

    /// The WatchList path's order, reflector.go L712-727: 429 first, then
    /// expired (which retries at once with `resourceVersion=""`).
    pub fn after_watch_list(s: &Status) -> (Next, &'static str, bool) {
        if is_too_many_requests(s) {
            (Next::BackoffRewatch, "isWatchErrorRetriable", false)
        } else if is_expired_error(s) {
            (Next::Rewatch, "isExpiredError", true)
        } else {
            (Next::Relist, "fallbackToList", false)
        }
    }

    /// `relistResourceVersion`, reflector.go L1002-1018.
    pub fn relist_rv(last_sync: &str, unavailable: bool) -> String {
        if unavailable {
            String::new()
        } else if last_sync.is_empty() {
            "0".to_owned()
        } else {
            last_sync.to_owned()
        }
    }

    /// The reflector's page size for a LIST at `rv` (reflector.go L607-620,
    /// default `WatchListPageSize` 0): paged only from `""` or `"0"`.
    pub fn list_limit(rv: &str) -> u64 {
        if rv.is_empty() || rv == "0" { 500 } else { 0 }
    }

    /// What a server answered one LIST with.
    #[derive(Clone, Debug)]
    pub enum Listed {
        /// A list at this `resourceVersion`.
        At(String),
        /// An error.
        Refused(Status),
    }

    /// One reflector LIST, reflector.go L604-633: a 410 or a TooLarge answer
    /// sets `isLastSyncResourceVersionUnavailable` and LISTs again at once
    /// with `resourceVersion=""`. Returns the `resourceVersion` of every
    /// LIST call and the last answer.
    pub fn list_calls(
        last_sync: &str,
        mut answer: impl FnMut(&str) -> Listed,
    ) -> (Vec<String>, Listed) {
        let mut calls = Vec::new();
        let mut unavailable = false;
        loop {
            let rv = relist_rv(last_sync, unavailable);
            calls.push(rv.clone());
            match answer(&rv) {
                Listed::Refused(e)
                    if !unavailable
                        && (is_expired_error(&e) || is_too_large_resource_version(&e)) =>
                {
                    unavailable = true;
                }
                done => return (calls, done),
            }
        }
    }

    /// The pager's continue-page fallback, pager.go L114-124: a continue
    /// page answered with `IsResourceExpired` becomes a full LIST (limit 0)
    /// at the original `resourceVersion`. `IsGone` does not qualify.
    pub fn pager_falls_back(s: &Status) -> bool {
        is_resource_expired(s)
    }

    /// The watch event types client-go's decoder accepts
    /// (rest/watch/decoder.go); any other is synthesized as an in-band 500
    /// `ClientWatchDecoding`.
    pub fn decodes_event_type(kind: &str) -> bool {
        matches!(
            kind,
            "ADDED" | "MODIFIED" | "DELETED" | "BOOKMARK" | "ERROR"
        )
    }

    /// `minWatchTimeout` (reflector.go): a watch's `timeoutSeconds` is drawn
    /// from [5 min, 10 min).
    pub const WATCH_TIMEOUT_RANGE: [u64; 2] = [300, 600];
}

pub mod kube_rs {
    //! kube-rs 4.2.0: `kube-runtime/src/watcher.rs` `step_trampolined`,
    //! `kube-client` `request_events` and its retry layer.

    use std::fmt;

    /// The watcher's state.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum State {
        /// Nothing listed: the next poll LISTs (default `ListSemantic::
        /// MostRecent`, no `resourceVersion`) or opens a streaming list at
        /// `"0"`.
        Empty,
        /// Listed; the next poll WATCHes from `rv`.
        InitListed { rv: String },
        /// Streaming from `rv`.
        Watching { rv: String },
        /// A streaming list before its `initial-events-end` bookmark.
        InitialWatch,
    }

    /// What reached the watcher from one poll of its stream.
    #[derive(Clone, Debug)]
    pub enum Item {
        /// An object at `rv`.
        Object { rv: String },
        /// A BOOKMARK at `rv`; `end` when annotated initial-events-end.
        Bookmark { rv: String, end: bool },
        /// An in-band `ERROR` event with this `code`.
        Error { code: i64 },
        /// A line that is not a watch event (the body of an HTTP error),
        /// parsed as a Status: `Err(Error::Api)` (client/mod.rs
        /// `request_events` L340-400, no status check).
        ApiError { code: i64 },
        /// The stream ended.
        End,
    }

    /// What the watcher emitted.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Emitted {
        Nothing,
        Apply,
        InitApply,
        InitDone,
        WatchError,
        WatchFailed,
        WatchStartFailed,
        InitialListFailed,
    }

    impl Emitted {
        /// The fixture's spelling.
        pub fn spelled(self) -> &'static str {
            match self {
                Self::Nothing => "",
                Self::Apply => "Ok(Event::Apply)",
                Self::InitApply => "Ok(Event::InitApply)",
                Self::InitDone => "Ok(Event::InitDone)",
                Self::WatchError => "Err(Error::WatchError)",
                Self::WatchFailed => "Err(Error::WatchFailed)",
                Self::WatchStartFailed => "Err(Error::WatchStartFailed)",
                Self::InitialListFailed => "Err(Error::InitialListFailed)",
            }
        }
    }

    /// One step of `step_trampolined` from `state` on `item`, watcher.rs
    /// L599-714. Only `code == 410` in-band relists; nothing else does.
    pub fn step(state: &State, item: &Item) -> (Emitted, State) {
        match (state, item) {
            (State::Watching { .. }, Item::Object { rv }) => {
                (Emitted::Apply, State::Watching { rv: rv.clone() })
            }
            (State::Watching { .. }, Item::Bookmark { rv, .. }) => {
                (Emitted::Nothing, State::Watching { rv: rv.clone() })
            }
            (State::Watching { .. } | State::InitialWatch, Item::Error { code: 410 }) => {
                (Emitted::WatchError, State::Empty)
            }
            (s @ (State::Watching { .. } | State::InitialWatch), Item::Error { .. }) => {
                (Emitted::WatchError, s.clone())
            }
            (s @ (State::Watching { .. } | State::InitialWatch), Item::ApiError { .. }) => {
                (Emitted::WatchFailed, s.clone())
            }
            (State::Watching { rv }, Item::End) => {
                (Emitted::Nothing, State::InitListed { rv: rv.clone() })
            }
            (State::InitialWatch, Item::Object { .. }) => (Emitted::InitApply, State::InitialWatch),
            (State::InitialWatch, Item::Bookmark { rv, end: true }) => {
                (Emitted::InitDone, State::Watching { rv: rv.clone() })
            }
            (State::InitialWatch, Item::Bookmark { end: false, .. }) => {
                (Emitted::Nothing, State::InitialWatch)
            }
            (State::InitialWatch, Item::End) => (Emitted::Nothing, State::Empty),
            (s, _) => (Emitted::Nothing, s.clone()),
        }
    }

    /// `InitListed`'s watch start (watcher.rs L635-655): a transport error
    /// stays `InitListed` at the same `rv`. An HTTP error status is not a
    /// start error: `api.watch` returns a stream (see [`Item::ApiError`]).
    pub fn start_failed(state: &State) -> (Emitted, State) {
        (Emitted::WatchStartFailed, state.clone())
    }

    /// `InitPage`'s list error (watcher.rs L578-585): back to `Empty`.
    pub fn list_failed() -> (Emitted, State) {
        (Emitted::InitialListFailed, State::Empty)
    }

    /// The HTTP retry layer, `retry.rs` `is_retryable_status` L114.
    pub fn http_retries(code: i64) -> bool {
        matches!(code, 429 | 503 | 504)
    }

    /// Retries per request (`retry.rs`).
    pub const MAX_RETRIES: usize = 15;

    /// Whether a watch whose `resourceVersion` the server refuses the same
    /// way every time is abandoned for a relist, driven through [`step`]:
    /// the server answers each watch from `rv` with `answer` (the items of
    /// one response), and the watcher is followed for `rounds` watches.
    pub fn leaves_a_dead_rv(rv: &str, answer: &[Item], rounds: usize) -> bool {
        let mut state = State::InitListed { rv: rv.to_owned() };
        for _ in 0..rounds {
            state = match state {
                State::InitListed { rv } => State::Watching { rv },
                other => other,
            };
            for item in answer {
                state = step(&state, item).1;
                if state == State::Empty {
                    return true;
                }
            }
        }
        false
    }

    /// The fixture's `Watching{rv:'57'}` / `InitListed{rv:'57'}` spelling.
    pub struct Short<'a>(pub &'a State);

    impl fmt::Display for Short<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self.0 {
                State::Empty => f.write_str("Empty"),
                State::InitListed { rv } => write!(f, "InitListed{{rv:'{rv}'}}"),
                State::Watching { rv } => write!(f, "Watching{{rv:'{rv}'}}"),
                State::InitialWatch => f.write_str("InitialWatch"),
            }
        }
    }

    /// The fixture's `Watching{resource_version: "500"}` spelling.
    pub struct Long<'a>(pub &'a State);

    impl fmt::Display for Long<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self.0 {
                State::Empty => f.write_str("Empty"),
                State::InitListed { rv } => write!(f, "InitListed{{resource_version: \"{rv}\"}}"),
                State::Watching { rv } => write!(f, "Watching{{resource_version: \"{rv}\"}}"),
                State::InitialWatch => f.write_str("InitialWatch"),
            }
        }
    }

    /// The query kube-rs sends for a streaming list's initial watch
    /// (kube-core `request.rs` test `watch_streaming_list`).
    pub const STREAMING_LIST_QUERY: &str = "watch=true&timeoutSeconds=290&allowWatchBookmarks=true\
        &sendInitialEvents=true&resourceVersionMatch=NotOlderThan&resourceVersion=0";

    /// The query kube-rs sends to resume a watch (`WatchPhase::Resumed`):
    /// no `sendInitialEvents`.
    pub fn resumed_query(rv: &str) -> String {
        let mut q = String::from("timeoutSeconds=290&allowWatchBookmarks=true&resourceVersion=");
        q.push_str(rv);
        q
    }
}
