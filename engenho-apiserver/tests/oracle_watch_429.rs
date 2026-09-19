//! How client-go (v1.34.0) and kube-rs (4.2.0) react to the statuses a watch
//! can end with, against upstream's table, and what that means for the
//! statuses engenho emits (T0.7, T3.7).
//!
//! Two tests:
//!
//! * `watch_client_rows_agree_with_upstream` runs the `watch-429-clients`
//!   table. The client rows check the model in `watch_oracle::clients`
//!   against upstream, row by row; the three server rows check engenho
//!   itself, over HTTP. Timing rows (backoff schedules, jitter, retry
//!   windows) and transport rows (EOF, connection refused) are out of scope:
//!   no status a server sends changes them.
//! * `no_watch_end_engenho_emits_leaves_a_client_on_a_dead_resource_version`
//!   takes every way engenho refuses or ends a watch, as the wire carries it,
//!   and drives both (now pinned) client models through it. The property:
//!   a client told a `resourceVersion` engenho will never serve leaves it,
//!   and a client told to retry at the same `resourceVersion` is at one
//!   engenho serves. kube-rs relists only on an IN-BAND `ERROR` with code
//!   410; anything else, an HTTP 410 included, re-watches the same
//!   revision, so a refusal rendered any other way would wedge
//!   pangea-operator (INTEGRATION-NOTES, T3.9a).

mod watch_oracle;

use std::time::Duration;

use engenho_apiserver::params::WatchGvk;
use engenho_apiserver::{ListWatchParams, WatchProgress};
use engenho_oracle::{Answer, Case, OutOfScope, Vector, assert_table};
use engenho_store::{Revision, WatchGone};
use serde_json::{Map, Value, json};
use watch_oracle::clients::Status;
use watch_oracle::clients::client_go::{self as cg, Ended, Listed, Next};
use watch_oracle::clients::kube_rs::{self as kr, Item, Short, State};
use watch_oracle::cluster::{Bookmarks, Cluster, Http, Wire, rv_of};
use watch_oracle::says;

/// Rows no server status decides.
const OUT_OF_SCOPE: &[OutOfScope] = &[
    OutOfScope::case(
        "rest_watch_transport_eof_exhausts_then_empty_watch",
        "a transport EOF carries no status from the server; which status engenho sends cannot \
         change it",
    ),
    OutOfScope::case(
        "rest_watch_connection_refused_not_retried_by_rest_but_retriable_by_reflector",
        "a refused connection carries no status from the server",
    ),
    OutOfScope::case(
        "reflector_inband_internal_error_retry_window_no_backoff",
        "the internal-error retry window is a client option no informer sets \
         (MaxInternalErrorRetryDuration = 0); it is timing, and the off-by-default row is checked",
    ),
    OutOfScope::case(
        "reflector_watchlist_disabled_by_default_in_v1_34",
        "a client-go feature-gate default: nothing a server sends decides it",
    ),
    OutOfScope::case(
        "stream_unexpected_eof_is_silent_close",
        "a truncated stream carries no status from the server",
    ),
    OutOfScope::case(
        "reflector_backoff_schedule_defaults",
        "backoff timing decides how soon a client comes back, never where",
    ),
    OutOfScope::case(
        "reflector_backoff_reset_after_quiet_period",
        "backoff timing decides how soon a client comes back, never where",
    ),
    OutOfScope::case(
        "reflector_backoff_state_shared_by_429_and_relist",
        "backoff timing decides how soon a client comes back, never where",
    ),
    OutOfScope::case(
        "reflector_init_conn_backoff_bounds_from_test",
        "backoff timing decides how soon a client comes back, never where",
    ),
    OutOfScope::case(
        "apiserver_unready_watch_cache_returns_http_429",
        "engenho has no watch cache that can be unready: a watch reads the store, which serves \
         as soon as the apiserver does, so it never sheds a watch with a 429",
    ),
    OutOfScope::case(
        "kube_rs_http_retry_sleep_formula",
        "backoff timing decides how soon a client comes back, never where",
    ),
    OutOfScope::case(
        "kube_rs_default_backoff_schedule",
        "backoff timing decides how soon a client comes back, never where",
    ),
    OutOfScope::case(
        "kube_rs_backoff_resets_on_any_ok_item",
        "backoff timing decides how soon a client comes back, never where",
    ),
    OutOfScope::case(
        "kube_rs_reset_timer_backoff_test",
        "backoff timing decides how soon a client comes back, never where",
    ),
    OutOfScope::case(
        "kube_rs_raw_watcher_has_no_backoff",
        "whether a watcher backs off is the caller's wiring (default_backoff, Controller); it \
         changes how fast a retry loops, not whether it relists",
    ),
];

/// engenho's answers to the server rows, observed once.
struct Live {
    /// A LIST at 100 against a store at 90.
    too_large: Http,
    /// A watch from a revision below the compaction floor.
    compacted: Wire,
}

impl Live {
    async fn observe() -> Self {
        let (too_large, compacted) = tokio::join!(
            async {
                let c = Cluster::boot(Bookmarks::Production).await;
                c.advance_to(90).await;
                c.list("resourceVersion=100").await
            },
            async {
                let c = Cluster::reloaded_at(5, Bookmarks::Production).await;
                c.watch("resourceVersion=3").await
            }
        );
        Self {
            too_large,
            compacted,
        }
    }
}

/// An in-band `ERROR` object carrying `status`, as the wire would.
fn error_object(code: i64, reason: &str) -> Value {
    json!({"kind": "Status", "apiVersion": "v1", "status": "Failure", "code": code, "reason": reason})
}

/// The client-go predicates `expected` names, over `s`.
fn predicates(expected: &Value, s: &Status) -> Value {
    let mut out = Map::new();
    for key in expected.as_object().map(Map::keys).into_iter().flatten() {
        if let Some(b) = cg::predicate(key, s) {
            out.insert(key.clone(), Value::Bool(b));
        }
    }
    Value::Object(out)
}

/// One ListAndWatch run over a sequence of watch calls: backoffs inside it,
/// how many watch calls it made, and the error it returned.
enum Call {
    StartError(Status),
    InBand(Value),
    /// An already-stopped watcher: closed at once with nothing.
    Stopped,
}

fn list_and_watch(calls: &[Call]) -> (usize, usize, Option<Status>) {
    let mut backoffs = 0;
    for (i, call) in calls.iter().enumerate() {
        let next = match call {
            Call::StartError(s) => match cg::after_watch_start_error(s) {
                Next::Relist => return (backoffs, i + 1, Some(s.clone())),
                next => next,
            },
            Call::InBand(object) => cg::after_watch(&Ended::Error(object.clone())).0,
            Call::Stopped => {
                cg::after_watch(&Ended::Closed {
                    events: 0,
                    short: true,
                })
                .0
            }
        };
        match next {
            Next::BackoffRewatch => backoffs += 1,
            Next::Rewatch => {}
            Next::Relist => return (backoffs, i + 1, None),
        }
    }
    (backoffs, calls.len(), None)
}

#[allow(clippy::too_many_lines)]
fn answer(case: &Case, live: &Live) -> Answer {
    let input = &case.input;
    let expected = &case.expected;
    let v = match case.name.as_str() {
        name if name.starts_with("classify_") => {
            predicates(expected, &Status::from_json(&input["status"]))
        }
        "generic_http_error_mapping_non_status_body" => {
            let mut out = Map::new();
            for code in input["http_status_codes"].as_array().into_iter().flatten() {
                let code = code.as_i64().unwrap_or(0);
                let s = cg::result_error(code, None);
                let key = code.to_string();
                let mut row = Map::new();
                for k in expected[&key]
                    .as_object()
                    .map(Map::keys)
                    .into_iter()
                    .flatten()
                {
                    if k == "reason" {
                        row.insert(k.clone(), Value::String(s.reason.clone()));
                    } else if let Some(b) = cg::predicate(k, &s) {
                        row.insert(k.clone(), Value::Bool(b));
                    }
                }
                out.insert(key, Value::Object(row));
            }
            Value::Object(out)
        }
        "inband_error_event_object_not_a_status" => {
            let object = &input["watch_events"][0]["object"];
            let (next, branch) = cg::after_watch(&Ended::Error(object.clone()));
            json!({
                "error_type": if cg::from_object(object).is_none() { "UnexpectedObjectError" } else { "StatusError" },
                "reflector_branch": says(branch == "default", "default (log 'Warning: watch ended with error')", branch),
                "next_action": says(next == Next::Relist, "return nil from watch(); backoff; relist", next),
            })
        }
        "inband_error_unstructured_status_wrong_apiversion" => {
            let object = &input["watch_events"][0]["object"];
            let status = cg::from_object(object);
            let next = cg::after_watch(&Ended::Error(object.clone())).0;
            json!({
                "error_type": if status.is_none() { "UnexpectedObjectError" } else { "StatusError" },
                "IsTooManyRequests": status.as_ref().is_some_and(cg::is_too_many_requests),
                "next_action": says(next == Next::Relist, "relist (default branch), NOT backoff+rewatch", next),
            })
        }
        "rest_watch_http_429_numeric_retry_after_retried_inside_rest_client" => {
            let (attempts, sleep) = cg::rest_attempts(429, Some("2"));
            let err = cg::result_error(429, None);
            let next = cg::after_watch_start_error(&err);
            json!({
                "http_attempts": attempts,
                "sleep_before_each_retry_s": sleep,
                "error_surfaced_to_reflector": {"code": err.code, "IsTooManyRequests": cg::is_too_many_requests(&err)},
                "reflector_action": says(next == Next::BackoffRewatch, "backoffManager.Backoff() then re-watch at LastSyncResourceVersion; no LIST", next),
            })
        }
        "rest_watch_http_429_without_retry_after_not_retried" => {
            let (attempts, _) = cg::rest_attempts(429, None);
            let err = cg::result_error(429, None);
            let next = cg::after_watch_start_error(&err);
            json!({
                "http_attempts": attempts,
                "error_surfaced_to_reflector": {"code": err.code},
                "reflector_action": says(next == Next::BackoffRewatch, "backoff, re-watch, no LIST", next),
            })
        }
        "rest_watch_http_429_http_date_retry_after_not_retried" => {
            let header = "Wed, 21 Oct 2015 07:28:00 GMT";
            let (attempts, _) = cg::rest_attempts(429, Some(header));
            json!({
                "http_attempts": attempts,
                "details.retryAfterSeconds": cg::retry_after_header(Some(header)).unwrap_or(0),
            })
        }
        "rest_watch_retry_after_zero_and_negative" => {
            let tries = ["0", "-1"].map(|h| cg::rest_attempts(429, Some(h)));
            json!({
                "retried": tries.iter().all(|(attempts, _)| *attempts > 1),
                "sleep_s": tries.iter().filter_map(|(_, s)| *s).max(),
            })
        }
        "rest_watch_5xx_with_retry_after_retried_410_is_not" => {
            let mut out = Map::new();
            for code in [504, 503, 500, 410] {
                let verdict = if cg::check_wait(code, Some("1")).is_some() {
                    "retried"
                } else {
                    "not retried; error surfaced immediately"
                };
                out.insert(code.to_string(), Value::String(verdict.to_owned()));
            }
            Value::Object(out)
        }
        "rest_watch_body_status_overrides_http_code" => {
            let err = cg::result_error(
                input["http_status"].as_i64().unwrap_or(0),
                Some(&input["body"]),
            );
            let next = cg::after_watch_start_error(&err);
            json!({
                "returned_error": {"code": err.code, "reason": err.reason},
                "IsTooManyRequests": cg::is_too_many_requests(&err),
                "reflector_action": says(
                    next == Next::Relist && cg::is_expired_error(&err),
                    "Watch returns err -> ListAndWatch returns err -> DefaultWatchErrorHandler (isExpiredError, V(4)) -> backoff -> LIST",
                    next,
                ),
            })
        }
        "rest_watch_body_status_not_failure_keeps_http_code" => {
            let err = cg::result_error(
                input["http_status"].as_i64().unwrap_or(0),
                Some(&input["body"]),
            );
            json!({"returned_error": {"code": err.code, "reason": err.reason}})
        }
        "rest_body_retry_after_seconds_without_header_is_ignored" => {
            let (attempts, _) = cg::rest_attempts(429, None);
            let err = cg::result_error(429, Some(&input["body"]));
            let next = cg::after_watch_start_error(&err);
            json!({
                "http_attempts": attempts,
                "reflector_wait": says(next == Next::BackoffRewatch, "backoffManager value, not 5s", next),
            })
        }
        "reflector_establishment_429_then_inband_429" => {
            let calls = [
                Call::StartError(Status {
                    reason: "TooManyRequests".to_owned(),
                    ..Status::default()
                }),
                Call::InBand(error_object(429, "TooManyRequests")),
                Call::Stopped,
            ];
            let (backoffs, _, error) = list_and_watch(&calls);
            json!({"backoffManager_Backoff_calls": backoffs, "ListAndWatch_error": error.map(|e| e.reason)})
        }
        "reflector_establishment_429_x4_no_relist" => {
            let too_many = || Call::StartError(Status::of(429, "TooManyRequests"));
            let calls = [
                too_many(),
                too_many(),
                too_many(),
                too_many(),
                Call::Stopped,
            ];
            let (_, watches, _) = list_and_watch(&calls);
            // One ListAndWatch run: one LIST, however many watches.
            json!({"list_calls": 1, "watch_calls": watches})
        }
        "reflector_inband_429_rewatch_from_last_event_rv" => {
            let events = input["watch_events"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            let mut last_sync = input["list_rv"].as_str().unwrap_or("").to_owned();
            let mut backoffs = 0;
            let mut lists = 1;
            for e in &events {
                if e["type"] == "ERROR" {
                    let object = error_object(
                        e["status"]["code"].as_i64().unwrap_or(0),
                        e["status"]["reason"].as_str().unwrap_or(""),
                    );
                    match cg::after_watch(&Ended::Error(object)).0 {
                        Next::BackoffRewatch => backoffs += 1,
                        Next::Relist => lists += 1,
                        Next::Rewatch => {}
                    }
                } else if let Some(rv) = e["rv"].as_str() {
                    last_sync = rv.to_owned();
                }
            }
            json!({
                "store_contains_rv_12": last_sync == "12",
                "backoff_calls": backoffs,
                "list_calls": lists,
                "next_watch_options": {
                    "resourceVersion": last_sync,
                    "allowWatchBookmarks": true,
                    "timeoutSeconds_range": cg::WATCH_TIMEOUT_RANGE,
                },
                // An in-band error ends the watch, not ListAndWatch: the
                // handler runs only for an error ListAndWatch returns.
                "watchErrorHandler_called": lists > 1,
            })
        }
        "reflector_inband_429_retry_after_seconds_ignored" => {
            let next = cg::after_watch(&Ended::Error(error_object(429, "TooManyRequests"))).0;
            json!({"wait": says(
                next == Next::BackoffRewatch,
                "backoffManager.Backoff() only (first: [0.8s,1.6s)), retryAfterSeconds unused",
                next,
            )})
        }
        "reflector_inband_410_relists_from_last_sync_rv_after_backoff" => {
            let last = input["watch_events"][0]["rv"].as_str().unwrap_or("");
            let next = cg::after_watch(&Ended::Error(error_object(410, "Expired"))).0;
            let relist = cg::relist_rv(last, false);
            json!({
                "watch_returns": null,
                "ListAndWatch_returns": null,
                "watchErrorHandler_called": false,
                "isLastSyncResourceVersionUnavailable": false,
                "BackoffUntil_sleep_before_relist": next == Next::Relist,
                "next_list_options": {"resourceVersion": relist, "limit": cg::list_limit(&relist)},
            })
        }
        "reflector_relist_after_410_list_also_410_falls_back_to_rv_empty" => {
            let (mut calls, first) = cg::list_calls("", |rv| {
                if rv == "0" {
                    Listed::At("10".to_owned())
                } else {
                    Listed::Refused(Status::of(0, ""))
                }
            });
            let Listed::At(last) = first else {
                panic!("the first LIST succeeds: {first:?}");
            };
            let (second, _) = cg::list_calls(&last, |rv| match rv {
                "10" => Listed::Refused(Status::of(410, "Expired")),
                "" => Listed::At("11".to_owned()),
                _ => Listed::Refused(Status::of(0, "")),
            });
            calls.extend(second);
            json!({"list_call_rvs": calls, "backoff_between_rv10_and_rv_empty": false})
        }
        "reflector_relist_too_large_rv_three_error_shapes" => {
            let by_type = Status::from_json(&json!({
                "code": 504, "reason": "Timeout",
                "details": {"causes": [{"reason": "ResourceVersionTooLarge"}]}
            }));
            let by_cause_message = Status::from_json(&json!({
                "code": 504, "reason": "Timeout",
                "details": {"causes": [{"message": "Too large resource version"}]}
            }));
            let by_message = Status::from_json(&json!({
                "code": 504, "reason": "Timeout", "message": "Timeout: Too large resource version",
                "details": {"retryAfterSeconds": 1}
            }));
            let mut fresh = ["30", "40", "50"].into_iter();
            let mut calls = Vec::new();
            let mut last = String::new();
            for refusal in [
                None,
                Some(by_type),
                Some(by_cause_message),
                Some(by_message),
            ] {
                let (run, listed) = cg::list_calls(&last, |rv| {
                    if rv.is_empty() {
                        fresh.next().map_or_else(
                            || Listed::Refused(Status::default()),
                            |r| Listed::At(r.to_owned()),
                        )
                    } else if rv == "0" {
                        Listed::At("20".to_owned())
                    } else {
                        Listed::Refused(refusal.clone().unwrap_or_default())
                    }
                });
                calls.extend(run);
                last = match listed {
                    Listed::At(rv) => rv,
                    Listed::Refused(_) => String::new(),
                };
            }
            json!({"list_call_rvs": calls})
        }
        "reflector_list_continue_page_expired_full_list_at_same_rv"
        | "reflector_list_continue_page_gone_reason_skips_pager_fallback" => {
            // The continue page of a paged LIST at 10 answered 410 with this
            // reason.
            let reason = if case.name.contains("expired") {
                "Expired"
            } else {
                "Gone"
            };
            let refusal = Status::of(410, reason);
            let falls_back = cg::pager_falls_back(&refusal);
            let sets_unavailable = !falls_back && cg::is_expired_error(&refusal);
            if falls_back {
                // Run 1 LISTs at "0"; run 2 pages from the last seen 10, its
                // continue page carries no resourceVersion, and the pager's
                // fallback re-LISTs whole at 10.
                let mut calls = vec![cg::relist_rv("", false), cg::relist_rv("10", false)];
                calls.push(String::new());
                calls.push(cg::relist_rv("10", sets_unavailable));
                json!({
                    "list_call_rvs": calls,
                    "isLastSyncResourceVersionUnavailable_set": sets_unavailable,
                })
            } else {
                json!({
                    "pager_full_list_fallback": falls_back,
                    "reflector_sets_unavailable": sets_unavailable,
                    "next_list": {"resourceVersion": cg::relist_rv("10", sets_unavailable), "limit": 4},
                })
            }
        }
        "reflector_inband_internal_error_retries_off_by_default" => {
            let (next, branch) = cg::after_watch(&Ended::Error(error_object(500, "InternalError")));
            json!({
                "internal_error_retries": 0,
                "action": says(next == Next::Relist && branch == "default", "default branch -> return nil -> backoff -> LIST", (next, branch)),
            })
        }
        "reflector_inband_504_timeout_relists_at_last_sync_rv" => {
            let status = &input["watch_events"][0]["status"];
            let mut object = status.clone();
            object["kind"] = json!("Status");
            object["apiVersion"] = json!("v1");
            let (next, branch) = cg::after_watch(&Ended::Error(object));
            let last = input["last_sync_rv"].as_str().unwrap_or("");
            let too_large = Status::from_json(status);
            let (calls, _) = cg::list_calls(last, |rv| {
                if rv == last {
                    Listed::Refused(too_large.clone())
                } else {
                    Listed::At("58".to_owned())
                }
            });
            json!({
                "branch": branch,
                "backoff_before_relist": next == Next::Relist,
                "next_list_rv": cg::relist_rv(last, false),
                "then_if_list_too_large": {"next_list_rv": calls.get(1)},
            })
        }
        "reflector_http_504_at_establishment_is_listandwatch_error" => {
            let err = cg::result_error(504, None);
            json!({
                "isWatchErrorRetriable": cg::is_too_many_requests(&err),
                "ListAndWatch_returns_error": cg::after_watch_start_error(&err) == Next::Relist,
            })
        }
        "reflector_http_410_at_establishment_relists_not_from_rv_empty" => {
            let err = cg::result_error(
                410,
                Some(
                    &json!({"kind": "Status", "apiVersion": "v1", "status": "Failure", "code": 410, "reason": "Expired"}),
                ),
            );
            let last = input["last_sync_rv"].as_str().unwrap_or("");
            json!({
                "ListAndWatch_returns_error": cg::after_watch_start_error(&err) == Next::Relist,
                "watchErrorHandler": says(cg::is_expired_error(&err), "isExpiredError -> V(4) log only", &err),
                // DefaultWatchErrorHandler does not set the unavailable flag.
                "next_list_rv": cg::relist_rv(last, false),
            })
        }
        "reflector_precedence_expired_checked_before_429_on_watch_path" => {
            let (next, branch) = cg::after_watch(&Ended::Error(error_object(429, "Expired")));
            json!({
                "branch": branch,
                "action": says(next == Next::Relist, "return nil -> backoff -> LIST at lastSyncRV", next),
                "rewatch_without_list": next != Next::Relist,
            })
        }
        "reflector_precedence_429_checked_before_expired_on_watchlist_path" => {
            let (next, branch, unavailable) = cg::after_watch_list(&Status::of(429, "Expired"));
            json!({
                "branch": branch,
                "action": says(next == Next::BackoffRewatch, "stop watcher, backoff (not interruptible by stop), retry watch-list at same RV", next),
                "isLastSyncResourceVersionUnavailable": unavailable,
            })
        }
        "reflector_watchlist_410_retries_immediately_with_rv_empty" => {
            let (next, _, unavailable) = cg::after_watch_list(&Status::of(410, "Expired"));
            json!({
                "backoff": next == Next::BackoffRewatch,
                "isLastSyncResourceVersionUnavailable": unavailable,
                "next_watch_options": {
                    "resourceVersion": cg::relist_rv("57", unavailable),
                    "sendInitialEvents": true,
                    "resourceVersionMatch": "NotOlderThan",
                },
            })
        }
        "reflector_watchlist_other_error_falls_back_to_list" => {
            let (next, branch, _) = cg::after_watch_list(&Status::of(504, "Timeout"));
            json!({
                "fallbackToList": branch == "fallbackToList",
                "action": says(next == Next::Relist, "LIST then WATCH in the same ListAndWatch call", next),
            })
        }
        "reflector_normal_watch_close_rewatches_without_backoff" => {
            let (next, _) = cg::after_watch(&Ended::Closed {
                events: 1,
                short: false,
            });
            json!({
                "handleWatch_error": null,
                "backoff": next == Next::BackoffRewatch,
                "list": next == Next::Relist,
                "next": says(next == Next::Rewatch, "immediate WATCH at LastSyncResourceVersion", next),
            })
        }
        "reflector_very_short_empty_watch_relists"
        | "reflector_short_watch_with_only_a_bookmark_is_not_very_short"
        | "reflector_short_watch_with_only_wrong_type_objects_is_very_short" => {
            // `handleAnyWatch` counts bookmarks and objects of the watched
            // type, and nothing else.
            let wanted = input["expected_type"].as_str().unwrap_or("Pod");
            let events = input["watch_events"].as_array().map_or(0, |es| {
                es.iter()
                    .filter(|e| {
                        e["type"] == "BOOKMARK"
                            || e["object_kind"].as_str().is_none_or(|k| k == wanted)
                    })
                    .count()
            });
            let (next, branch) = cg::after_watch(&Ended::Closed {
                events,
                short: true,
            });
            let error = (next == Next::Relist).then_some("VeryShortWatchError");
            match case.name.as_str() {
                "reflector_very_short_empty_watch_relists" => json!({
                    "error": error,
                    "branch": branch,
                    "action": says(next == Next::Relist, "return nil -> backoff -> LIST", next),
                }),
                "reflector_short_watch_with_only_a_bookmark_is_not_very_short" => json!({
                    "eventCount": events,
                    "error": error,
                    "next": says(next == Next::Rewatch, "immediate WATCH at rv 60", next),
                }),
                _ => json!({
                    "eventCount": events,
                    "error": error,
                    "action": says(next == Next::Relist, "backoff -> LIST", next),
                }),
            }
        }
        "stream_decode_error_becomes_inband_500" => {
            let kind = input["watch_stream_line"]["type"].as_str().unwrap_or("");
            let synthesized = (!cg::decodes_event_type(kind)).then(|| {
                json!({"kind": "Status", "apiVersion": "v1", "status": "Failure", "code": 500,
                       "reason": "InternalError", "details": {"causes": [{"reason": "ClientWatchDecoding"}]}})
            });
            let next = synthesized
                .clone()
                .map(|object| cg::after_watch(&Ended::Error(object)).0);
            json!({
                "synthesized_event": synthesized.map(|s| json!({
                    "type": "ERROR",
                    "status": {"code": s["code"], "reason": s["reason"], "details.causes[-1].type": "ClientWatchDecoding"},
                })),
                "reflector_action_default_config": says(
                    next == Some(Next::Relist),
                    "relist (IsInternalError but MaxInternalErrorRetryDuration=0)",
                    next,
                ),
            })
        }
        // ── engenho itself ──
        "apiserver_mirrors_retry_after_seconds_into_header" => {
            let body = Status::from_json(&live.too_large.body);
            let mirrored = body.retry_after_seconds > 0
                && live.too_large.retry_after.as_deref()
                    == Some(body.retry_after_seconds.to_string().as_str());
            json!({"applies_to_any_code_when_retryAfterSeconds_gt_0": mirrored && live.too_large.status != 429})
        }
        "apiserver_post_admission_watch_errors_are_inband" => {
            let w = &live.compacted;
            let single_status = matches!(w.lines.as_slice(), [only] if only["type"] == "ERROR"
                && cg::from_object(&only["object"]).is_some());
            json!({
                "http_status": w.status,
                "events": [says(single_status, "single ERROR event carrying the Status", &w.lines)],
                "then": says(w.ended, "stream closed", "still open"),
            })
        }
        "apiserver_too_large_rv_error_shape" => {
            let b = &live.too_large.body;
            let causes: Vec<Value> = b["details"]["causes"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|c| json!({"type": c["reason"]}))
                .collect();
            json!({
                "code": b["code"],
                "reason": b["reason"],
                "message": b["message"],
                "details": {"retryAfterSeconds": b["details"]["retryAfterSeconds"], "causes": causes},
            })
        }
        // ── kube-rs ──
        "kube_rs_inband_410_resets_to_empty_and_relists_at_most_recent" => {
            let (emitted, state) = kr::step(
                &State::Watching { rv: "57".into() },
                &Item::Error { code: 410 },
            );
            json!({
                "emitted": emitted.spelled(),
                "next_state": Short(&state).to_string(),
                "next_poll": says(state == State::Empty, "Ok(Event::Init) then LIST with resourceVersion unset (ListSemantic::MostRecent), limit 500", &state),
            })
        }
        "kube_rs_inband_429_keeps_stream_then_rewatches_same_rv" => {
            let watching = State::Watching { rv: "57".into() };
            let (emitted, after_error) = kr::step(&watching, &Item::Error { code: 429 });
            let (_, after_end) = kr::step(&after_error, &Item::End);
            json!({
                "emitted": [emitted.spelled()],
                "state_after_error": says(after_error == watching, "Watching{rv:'57', same stream}", &after_error),
                "state_after_end": Short(&after_end).to_string(),
                "relist": after_error == State::Empty || after_end == State::Empty,
            })
        }
        "kube_rs_relist_decision_uses_code_only" => {
            let statuses = input["inband_statuses"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            let kube_rs: Vec<bool> = statuses
                .iter()
                .map(|s| {
                    let code = s["code"].as_i64().unwrap_or(0);
                    kr::step(&State::Watching { rv: "57".into() }, &Item::Error { code }).1
                        == State::Empty
                })
                .collect();
            let client_go: Vec<bool> = statuses
                .iter()
                .map(|s| {
                    let object = error_object(
                        s["code"].as_i64().unwrap_or(0),
                        s["reason"].as_str().unwrap_or(""),
                    );
                    cg::after_watch(&Ended::Error(object)).0 == Next::Relist
                })
                .collect();
            json!({"relist": kube_rs, "client_go_relist_for_same_inputs": client_go})
        }
        "kube_rs_http_level_410_never_relists" => {
            // `api.watch` returns a stream whatever the HTTP status; its first
            // line is the error body, parsed as a Status.
            let (emitted, state) = kr::step(
                &State::Watching { rv: "57".into() },
                &Item::ApiError { code: 410 },
            );
            let (_, after_end) = kr::step(&state, &Item::End);
            json!({
                "emitted": emitted.spelled(),
                "state_after_end": Short(&after_end).to_string(),
                // Whether the watcher ever leaves 57, answered this way on
                // every watch.
                "relist": kr::leaves_a_dead_rv("57", &[Item::ApiError { code: 410 }, Item::End], 8),
            })
        }
        "kube_rs_watch_start_transport_error_stays_initlisted" => {
            let (emitted, state) = kr::start_failed(&State::InitListed { rv: "57".into() });
            json!({
                "emitted": emitted.spelled(),
                "next_state": Short(&state).to_string(),
                "relist": state == State::Empty,
            })
        }
        "kube_rs_http_retry_layer_statuses" => {
            let mut retried = Map::new();
            for code in [429, 503, 504, 500, 502, 410] {
                retried.insert(code.to_string(), Value::Bool(kr::http_retries(code)));
            }
            json!({"retried_by_http_layer": retried, "max_retries_per_request": kr::MAX_RETRIES})
        }
        "kube_rs_http_429_after_retry_exhaustion" => {
            let attempts = if kr::http_retries(429) {
                1 + kr::MAX_RETRIES
            } else {
                1
            };
            let (emitted, state) = kr::step(
                &State::Watching { rv: "57".into() },
                &Item::ApiError { code: 429 },
            );
            let (_, after_end) = kr::step(&state, &Item::End);
            json!({
                "http_attempts": attempts,
                "emitted": says(emitted == kr::Emitted::WatchFailed, "Err(Error::WatchFailed(Error::Api(429)))", emitted),
                "state_after_end": Short(&after_end).to_string(),
                "relist": after_end == State::Empty,
            })
        }
        "kube_rs_list_error_restarts_listing" => {
            let (emitted, state) = kr::list_failed();
            json!({
                "emitted": emitted.spelled(),
                "next_state": Short(&state).to_string(),
                "next_poll": says(state == State::Empty, "Ok(Event::Init), list restarts from page 1 (continue token dropped)", &state),
            })
        }
        "kube_rs_streaming_list_nonfatal_error_then_close_restarts_initial" => {
            let (_, after_error) = kr::step(&State::InitialWatch, &Item::Error { code: 429 });
            let (_, after_end) = kr::step(&after_error, &Item::End);
            json!({
                "state_after_error": says(after_error == State::InitialWatch, "InitialWatch (same stream)", &after_error),
                "state_after_end": Short(&after_end).to_string(),
                "next": says(after_end == State::Empty, "new initial watch with sendInitialEvents at version '0'", &after_end),
            })
        }
        _ => return Answer::NotChecked,
    };
    Answer::Checked(v)
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_client_rows_agree_with_upstream() {
    let live = Live::observe().await;
    let table = Vector::Watch429Clients.load();
    let report = assert_table(&table, OUT_OF_SCOPE, &[], |case| answer(case, &live));
    assert_eq!(
        report.checked + report.out_of_scope,
        table.cases.len(),
        "every row is checked or out of scope"
    );
    assert_eq!(report.out_of_scope, OUT_OF_SCOPE.len());
    eprintln!("watch-429-clients: {report:?}");
}

// ─────────────────────────────────────────────────────────────────────────
// engenho's side: no watch end it emits leaves a client on a dead revision.
// ─────────────────────────────────────────────────────────────────────────

/// A watch's lines as kube-rs's watcher receives them.
fn kube_rs_items(wire: &Wire) -> Vec<Item> {
    let mut items: Vec<Item> = wire
        .lines
        .iter()
        .map(|line| {
            let rv = rv_of(line).map_or_else(String::new, |r| r.to_string());
            match line["type"].as_str() {
                Some("ERROR") => Item::Error {
                    code: line["object"]["code"].as_i64().unwrap_or(0),
                },
                Some("BOOKMARK") => Item::Bookmark {
                    rv,
                    end: line["object"]["metadata"]["annotations"]["k8s.io/initial-events-end"]
                        == "true",
                },
                _ => Item::Object { rv },
            }
        })
        .collect();
    if wire.status != 200 {
        // An HTTP error: kube-rs parses the body as one Status line.
        items = vec![Item::ApiError {
            code: i64::from(wire.status),
        }];
    }
    if wire.ended {
        items.push(Item::End);
    }
    items
}

/// Assert `wire` is one of engenho's refusals, as both clients must read it,
/// and that both leave the refused revision.
fn assert_refusal_is_left(what: &str, rv: &str, wire: &Wire) {
    assert_eq!(
        wire.status, 200,
        "{what}: a watch refusal is never an HTTP status: {wire:?}"
    );
    assert!(
        wire.ended,
        "{what}: the refusal closes the stream: {wire:?}"
    );
    let [only] = wire.lines.as_slice() else {
        panic!("{what}: exactly one line: {wire:?}");
    };
    assert_eq!(only["type"], "ERROR", "{what}: {only}");
    assert!(cg::decodes_event_type("ERROR"));
    let status = cg::from_object(&only["object"])
        .unwrap_or_else(|| panic!("{what}: client-go decodes the object as a Status: {only}"));
    assert_eq!(
        cg::after_watch(&Ended::Error(only["object"].clone())).0,
        Next::Relist,
        "{what}: client-go relists after {status:?}"
    );
    assert!(
        kr::leaves_a_dead_rv(rv, &kube_rs_items(wire), 4),
        "{what}: kube-rs keeps re-watching {rv} after {status:?}; it relists only on an in-band \
         ERROR with code 410"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn no_watch_end_engenho_emits_leaves_a_client_on_a_dead_resource_version() {
    // A store reloaded at 20: history below 20 is gone, and it stands at 20.
    let c = Cluster::reloaded_at(20, Bookmarks::Production).await;

    // ── the three refusals: compacted, ahead, and a streaming list ahead ──
    for (what, rv, query) in [
        ("compacted", "7", "resourceVersion=7"),
        ("ahead of the store", "90", "resourceVersion=90"),
        (
            "a streaming list ahead of the store",
            "90",
            "resourceVersion=90&sendInitialEvents=true&resourceVersionMatch=NotOlderThan\
             &allowWatchBookmarks=true",
        ),
    ] {
        let first = c.watch(query).await;
        assert_refusal_is_left(what, rv, &first);
        // Dead: the same watch is refused the same way again.
        let again = c.watch(query).await;
        assert_eq!(
            first.lines, again.lines,
            "{what}: the revision stays refused"
        );
    }

    // ── client-go's relist after each refusal converges on engenho ──
    // It LISTs at the refused revision first, and drops to "" only on a 410
    // or a TooLarge answer. engenho serves a compacted revision from current
    // state (not older than asked), and answers one ahead with the 504
    // TooLarge after its wait.
    let compacted = c.list("resourceVersion=7").await;
    assert_eq!(compacted.status, 200, "{compacted:?}");
    let ahead = c.list("resourceVersion=90").await;
    let too_large = Status::from_json(&ahead.body);
    assert_eq!(ahead.status, 504, "{ahead:?}");
    assert!(cg::is_too_large_resource_version(&too_large), "{ahead:?}");
    let fresh = c.list("resourceVersion=").await;
    assert_eq!(fresh.status, 200, "{fresh:?}");
    let (calls, listed) = cg::list_calls("90", |rv| match rv {
        "90" => Listed::Refused(too_large.clone()),
        "" => Listed::At(
            fresh.body["metadata"]["resourceVersion"]
                .as_str()
                .unwrap_or("")
                .to_owned(),
        ),
        _ => Listed::Refused(Status::default()),
    });
    assert_eq!(calls, ["90", ""], "client-go leaves 90 after engenho's 504");
    let Listed::At(listed) = listed else {
        panic!("the relist at \"\" succeeds: {listed:?}");
    };
    assert_eq!(listed, "20");
    // kube-rs lists with no resourceVersion at all (ListSemantic::MostRecent).

    // ── the in-band 429: kube-rs re-watches the same revision, so it must
    // be one engenho serves. The 429 ends a watch whose overflow held nothing
    // past the revision the stream opened at, which is the revision the
    // client watched from and engenho accepted.
    let start = Revision(c.current().await);
    let end = WatchProgress::new(start, false).after(&WatchGone::Overflow {
        capacity: 1,
        last_seen: start,
    });
    let engenho_apiserver::AfterGone::End(end) = end else {
        panic!("an overflow with nothing past the start ends the watch: {end:?}");
    };
    let line = end
        .final_line(WatchGvk {
            api_version: "v1",
            kind: "Pod",
        })
        .expect("the no-progress end has a line");
    let line: Value = serde_json::from_slice(line.trim_ascii_end()).expect("one JSON line");
    let code = line["object"]["code"].as_i64().expect("a status code");
    assert_eq!(code, 429, "{line}");
    let kept = kr::step(
        &State::Watching {
            rv: start.to_string(),
        },
        &Item::Error { code },
    )
    .1;
    assert_eq!(
        kept,
        State::Watching {
            rv: start.to_string()
        },
        "kube-rs keeps the revision"
    );
    let status = cg::from_object(&line["object"]).expect("client-go decodes it");
    assert_eq!(cg::after_watch_start_error(&status), Next::BackoffRewatch);
    let mut rewatch = c.open(&kr::resumed_query(&start.to_string())).await;
    assert_eq!(rewatch.status, 200);
    let lines = rewatch.lines_for(Duration::from_millis(300)).await;
    assert!(
        lines.iter().all(|l| l["type"] != "ERROR") && !rewatch.ended,
        "a watch from the 429's revision is served: {lines:?}"
    );

    // ── kube-rs's own requests are never refused at the HTTP level ──
    // A watch-start error (400, 422) would keep kube-rs on its revision
    // forever, so its two query shapes must both be accepted.
    let mut streaming = c.open(kr::STREAMING_LIST_QUERY).await;
    assert_eq!(
        streaming.status, 200,
        "kube-rs's streaming list is a valid watch"
    );
    let initial = streaming
        .lines_until(Duration::from_secs(5), |l| {
            l["object"]["metadata"]["annotations"]["k8s.io/initial-events-end"] == "true"
        })
        .await;
    let mut state = State::InitialWatch;
    for item in kube_rs_items(&Wire {
        status: 200,
        lines: initial,
        ended: false,
    }) {
        state = kr::step(&state, &item).1;
    }
    assert_eq!(
        state,
        State::Watching { rv: "20".into() },
        "kube-rs's streaming list reaches InitDone at the store's revision"
    );
    let parsed: ListWatchParams = serde_urlencoded::from_str(&kr::resumed_query("20"))
        .expect("kube-rs's resume query parses");
    assert!(
        parsed.watch_initial_events().is_ok(),
        "kube-rs's resumed watch is valid"
    );
}
