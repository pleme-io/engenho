//! Watch 410, too-large revisions, bookmarks and watch ends against
//! kube-apiserver v1.34's watch cache (`watch-410-bookmark`), row by row,
//! over HTTP against a real store (T0.7, T3.9a, T3.7).
//!
//! ## How a row becomes an engenho situation
//!
//! Upstream's rows describe a per-kind watch cache: a ring of that kind's
//! events, a relist revision, evictions. engenho has no cache: a watch reads
//! the store, whose replay ring is shared by every kind and whose revisions
//! are dense. The harness (`watch_oracle::cluster`) builds the situation a
//! client could observe:
//!
//! * "the cache relisted at R" is a store reloaded at R (its floor and its
//!   revision are R, its ring empty; T3.3);
//! * "the oldest buffered event is at O" is a store whose history below O is
//!   gone: reloaded at O-1, upstream's own `oldest-1` boundary;
//! * "an event at N" is a Pod written at exactly revision N, with ConfigMap
//!   writes (which the pod watch filters out) filling the revisions between.
//!
//! What is then compared is what a client sees: the watch's HTTP status, its
//! lines, and whether it ended. Rows about how upstream's cache is built
//! (capacity, resizing, channel sizes, time budgets, bookmark scheduling,
//! draining) have no engenho counterpart and are out of scope, each with the
//! reason.
//!
//! ## Where engenho differs on purpose
//!
//! A watch whose revision is ahead of the store is refused at once with an
//! in-band 410 (T3.9a, `watch_start.rs`); upstream accepts a plain one and
//! makes a watch-list wait 3 s for a 504. Every row about a watch ahead of
//! the store is a declared deviation. A LIST ahead of the store waits and
//! answers upstream's 504 (`list_floor.rs`), and its row agrees.

mod watch_oracle;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use engenho_apiserver::{ListWatchParams, ResumePoint, ViolationKind, WatchProgress};
use engenho_oracle::{Answer, Case, Deviation, OutOfScope, Vector, assert_table};
use engenho_store::{Revision, WatchGone};
use futures::StreamExt;
use serde_json::{Map, Value, json};
use watch_oracle::clients::Status;
use watch_oracle::clients::client_go::{self as cg, Ended, Listed, Next};
use watch_oracle::clients::kube_rs::{self as kr, Item, Long, State};
use watch_oracle::cluster::{Bookmarks, Cluster, FAST_BOOKMARKS, Wire, line_name, rv_of};
use watch_oracle::says;

/// Kinds and rows no engenho code answers.
const OUT_OF_SCOPE: &[OutOfScope] = &[
    OutOfScope::kind(
        "cacher.Watch -> watch_cache.getAllEventsSinceLocked",
        "an uninitialized watch cache: engenho serves no watch before its store is initialized \
         and leading, and has no cache to be uninitialized",
    ),
    OutOfScope::kind(
        "api_object_versioner.ParseResourceVersion + cacher.getWatchCacheResourceVersion",
        "how upstream routes \"\" and \"0\" to a cache freshness wait; engenho parses both to \
         ResumePoint::MostRecent and reads its own store, and their client-visible difference \
         is checked by the routing rows",
    ),
    OutOfScope::kind(
        "cacher.Watch (ResilientWatchCacheInitialization GA in 1.34)",
        "an unready watch cache: engenho has none, so it never sheds a watch with a 429",
    ),
    OutOfScope::kind(
        "util.calculateRetryAfterForUnreadyCache",
        "the retry-after of an unready watch cache, which engenho does not have",
    ),
    OutOfScope::kind(
        "cacher.getWatchCacheResourceVersion + waitUntilFreshAndBlock",
        "the etcd revision a cache waits for; engenho's store is the source, not a cache behind it",
    ),
    OutOfScope::kind(
        "cacher.getWatchCacheResourceVersion",
        "the revision a watch cache waits for before it serves; engenho's store needs no wait",
    ),
    OutOfScope::kind(
        "cacher.getBookmarkAfterResourceVersionLockedFunc",
        "the threshold a cache watcher waits for before its end bookmark; engenho sends the end \
         bookmark at the snapshot's revision in the same response, and when it is sent at all \
         is checked by watchlist.initial_events_end_bookmark_matrix",
    ),
    OutOfScope::kind(
        "delegator.Watch",
        "the WatchList feature gate: engenho has no gate; sendInitialEvents is always honoured",
    ),
    OutOfScope::kind(
        "cacheWatcher.nonblockingAdd",
        "a cache watcher's bookmark-after-rv gate; engenho has no such state",
    ),
    OutOfScope::kind(
        "cacheWatcher.nextBookmarkTime (frequency 60s)",
        "per-watcher bookmark scheduling; engenho's store bookmarks each watcher on a fixed \
         cadence (5 s) whenever the revision moved, with no deadline-2s bookmark",
    ),
    OutOfScope::kind(
        "cacheWatcher.nextBookmarkTime + kube-core WatchParams default",
        "per-watcher bookmark scheduling against kube-rs's 290 s timeout; engenho's cadence is \
         fixed (5 s), so the last bookmark before the deadline is at most 5 s old",
    ),
    OutOfScope::kind(
        "watcherBookmarkTimeBuckets",
        "the cacher's one-second bookmark buckets; engenho has no buckets",
    ),
    OutOfScope::kind(
        "watcherBookmarkTimeBuckets + cacheWatcher",
        "the cacher's bookmark buckets; engenho has none",
    ),
    OutOfScope::kind(
        "watchCache.suggestedWatchChannelSize",
        "per-watcher channel sizing; engenho's is one bounded channel of 1024 \
         (WATCH_CHANNEL_CAPACITY)",
    ),
    OutOfScope::kind(
        "timeBudgetImpl",
        "the dispatch time budget; engenho never blocks on a watcher (try_send), so it has none",
    ),
    OutOfScope::kind(
        "timeBudgetImpl (test instance: maxBudget 200ms, refresh 50ms)",
        "the dispatch time budget; engenho has none",
    ),
    OutOfScope::kind(
        "cacher.dispatchEvent + cacheWatcher.add",
        "the shared timer that closes blocked watchers; engenho never blocks: a full channel is a \
         typed overflow the router answers (watch_end.rs)",
    ),
    OutOfScope::kind(
        "cacheWatcher.add closeFunc",
        "graceful draining by bookmark state; engenho's watcher has no bookmark state, and an \
         overflow delivers everything buffered before it ends (WatchStream drain-then-Gone)",
    ),
    OutOfScope::kind(
        "cacheWatcher",
        "a cache watcher's input/result buffers and draining; engenho's watcher is one channel \
         that always drains before it reports an overflow",
    ),
    OutOfScope::kind(
        "cacheWatcher.add",
        "draining a watcher that waits for its bookmark; engenho has no such wait",
    ),
    OutOfScope::kind(
        "cacheWatcher.processInterval + watchCacheInterval",
        "a replay interval invalidated by the ring wrapping under it; engenho captures a replay \
         under the store's lock in one piece, so it cannot be invalidated",
    ),
    OutOfScope::kind(
        "cacher.terminateAllWatchers",
        "the cacher stopping (reflector failure); engenho's watches end when the store drops, \
         with a clean close",
    ),
    OutOfScope::kind(
        "cacher.forgetWatcher",
        "cacher bookkeeping; engenho's store reaps a watcher whose receiver dropped",
    ),
    OutOfScope::kind(
        "watch_cache.capacityUpperBound",
        "per-kind ring sizing; engenho's ring is one DEFAULT_HISTORY_CAPACITY (8192) across all \
         kinds",
    ),
    OutOfScope::kind(
        "watch_cache.resizeCacheLocked (DefaultEventFreshDuration=75s)",
        "per-kind ring resizing; engenho's ring does not resize",
    ),
    OutOfScope::case(
        "error_form.registration_race_yields_immediate_close",
        "a race between two readiness checks of a cache; engenho registers a watch under the \
         store's catalog lock in one step",
    ),
    OutOfScope::case(
        "watchlist.storage_without_progress_notify_is_500_error_event",
        "an etcd feature (RequestWatchProgress) a watch cache needs; engenho's store is not etcd",
    ),
    OutOfScope::case(
        "bookmark.periodic_tick_interval",
        "the cacher's jittered 1 s tick; engenho's store ticks every 50 ms and bookmarks each \
         watcher on its own cadence",
    ),
    OutOfScope::case(
        "bookmark.dispatch_with_concurrent_stop_does_not_hang",
        "a cacheWatcher Stop() racing the dispatcher; engenho's watcher is a channel the store \
         reaps when the receiver drops",
    ),
    OutOfScope::case(
        "bookmark.never_blocks_and_never_kills_a_watcher",
        "a bookmark meeting a full channel is engenho-store's decision (tick_bookmarks skips it \
         and keeps the watcher, the same rule); over HTTP the router drains the channel into \
         the socket, so a full channel cannot be held still from here",
    ),
];

/// Rows engenho answers differently on purpose, and where that is decided.
const DEVIATIONS: &[Deviation] = &[
    Deviation {
        case: "too_old.conservative_floor_ignores_rv_gaps",
        why: "engenho's replay ring is one dense sequence across every kind, and its floor is \
              the newest revision it evicted (ResourceCatalog::push_history), so a watch that \
              lost nothing is served: the replay from 52 is complete. Upstream's per-kind cache \
              does not remember the last evicted revision and refuses from its oldest held one.",
    },
    Deviation {
        case: "future_rv.plain_watch_ahead_of_cache_is_accepted_without_504",
        why: "T3.9a (watch_start.rs): a watch ahead of the store is refused at once with an \
              in-band 410, never attached and left silent",
    },
    Deviation {
        case: "future_rv.watchlist_succeeds_when_cache_catches_up_within_timeout",
        why: "T3.9a (watch_start.rs): engenho refuses a watch-list ahead of the store at once \
              rather than waiting up to 3 s for it",
    },
    Deviation {
        case: "future_rv.events_up_to_requested_rv_are_suppressed",
        why: "T3.9a (watch_start.rs): the watch at 1010 is ahead of the store at 1000, which \
              engenho refuses with an in-band 410; the suppression of events at or below a \
              watch's revision is checked by future_rv.event_exactly_at_requested_rv_is_not_delivered",
    },
    Deviation {
        case: "future_rv.periodic_bookmarks_suppressed_until_cache_passes_rv",
        why: "T3.9a (watch_start.rs): a watch ahead of the store is refused at once, so no \
              bookmark is ever due",
    },
    Deviation {
        case: "future_rv.watchlist_blocks_3s_then_504_too_large",
        why: "T3.9a (watch_start.rs): engenho refuses a watch-list ahead of the store at once \
              with an in-band 410 Expired, which both clients relist on; upstream waits 3 s and \
              sends 504, which kube-rs keeps re-watching",
    },
    Deviation {
        case: "bookmark.multiple_bookmarks_monotonic",
        why: "engenho-store sends a watcher one bookmark per revision the store moved to \
              (WatcherRegistry::tick_bookmarks suppresses a repeat at the same revision), not \
              one per period: a quiet watch gets no keepalive bookmarks and ends at its \
              timeoutSeconds instead. Every bookmark it does send is monotonic.",
    },
    Deviation {
        case: "event_shape.deleted_via_selector_transition_carries_transition_rv",
        why: "NOT a design choice: pending I4 (T3.7-relabel). A change that takes an object out \
              of a watch's selector is filtered out, not sent as DELETED from the prior state; \
              the store's watch event does not carry the prior object yet",
    },
    Deviation {
        case: "server_timeout.randomized_when_unset",
        why: "engenho puts no server-chosen deadline on a watch that names no timeoutSeconds \
              (r7_8 watch_without_timeout_seconds_stays_open); kube-rs (290 s) and client-go's \
              reflector (5-10 min) always name one. At a named timeout the end is the same clean \
              close.",
    },
];

// ── reading engenho's answers ────────────────────────────────────────────

/// An `ERROR` line's object, if the watch ended with one.
fn error_of(w: &Wire) -> Option<&Value> {
    w.lines
        .iter()
        .find(|l| l["type"] == "ERROR")
        .map(|l| &l["object"])
}

/// The event lines: neither an ERROR nor a BOOKMARK.
fn events_of(w: &Wire) -> Vec<&Value> {
    w.lines
        .iter()
        .filter(|l| l["type"] != "ERROR" && l["type"] != "BOOKMARK")
        .collect()
}

/// The status fields upstream's rows compare.
fn status_fields(o: &Value) -> Value {
    json!({"status": o["status"], "code": o["code"], "reason": o["reason"], "message": o["message"]})
}

/// A watch read to its end, answered in the keys a history row's `expected`
/// uses. Events are `{type, rv}`; `prev_object` is `null` for an ADDED line
/// (it asserts no prior state), and a template naming `prev_rv`, which no
/// wire carries, leaves `events` unchecked.
fn history_answer(w: &Wire, expected: &Value) -> Value {
    let error = error_of(w);
    let events = events_of(w);
    let template = expected["events"][0].as_object();
    let mut out = Map::new();
    for key in expected.as_object().map(Map::keys).into_iter().flatten() {
        let v = match key.as_str() {
            "result" | "watch_result" => {
                json!(if error.is_some() { "too_old" } else { "ok" })
            }
            "status" => error.map_or(Value::Null, status_fields),
            "status_code" => error.map_or(Value::Null, |o| o["code"].clone()),
            "reason" => error.map_or(Value::Null, |o| o["reason"].clone()),
            "message" => error.map_or(Value::Null, |o| o["message"].clone()),
            "message_contains" => {
                let message = error.and_then(|o| o["message"].as_str()).unwrap_or("");
                let wanted = expected[key].as_str().unwrap_or("");
                json!(if message.contains(wanted) {
                    wanted
                } else {
                    message
                })
            }
            "events" if template.is_some_and(|t| t.contains_key("prev_rv")) => continue,
            "events" => Value::Array(
                events
                    .iter()
                    .map(|l| {
                        let mut e = json!({"type": l["type"], "rv": rv_of(l)});
                        if template.is_some_and(|t| t.contains_key("prev_object")) {
                            e["prev_object"] = if l["type"] == "ADDED" {
                                Value::Null
                            } else {
                                json!("not on the wire")
                            };
                        }
                        e
                    })
                    .collect(),
            ),
            "events_rvs" => json!(events.iter().map(|l| rv_of(l)).collect::<Vec<_>>()),
            _ => continue,
        };
        out.insert(key.clone(), v);
    }
    Value::Object(out)
}

/// How a watch answered, in the fixture's outcome vocabulary.
fn outcome(w: &Wire) -> &'static str {
    if w.status != 200 {
        "direct_error"
    } else if error_of(w).is_some() {
        "error_event"
    } else if w.lines.is_empty() && w.ended {
        "immediate_close"
    } else {
        "established"
    }
}

/// `name` for a Pod whose first write is at `rev`.
fn pod_name(rev: u64) -> String {
    let mut name = String::from("pod-");
    name.push_str(&rev.to_string());
    name
}

/// A store whose history holds nothing at or below `floor` (a reload
/// there; a fresh store for 0), with one Pod created at each of `pods`.
async fn history(floor: u64, pods: &[u64], bookmarks: Bookmarks) -> Cluster {
    let c = if floor == 0 {
        Cluster::boot(bookmarks).await
    } else {
        Cluster::reloaded_at(floor, bookmarks).await
    };
    for rev in pods {
        c.pod_at(&pod_name(*rev), json!({}), *rev).await;
    }
    c
}

/// A plain watch from `from`, no bookmarks, closed by the server after 1 s.
fn plain_from(from: u64) -> String {
    let mut q = String::from("resourceVersion=");
    q.push_str(&from.to_string());
    q.push_str("&allowWatchBookmarks=false&timeoutSeconds=1");
    q
}

/// The query for a row's options: `allowWatchBookmarks`, and
/// `sendInitialEvents` with the `NotOlderThan` a valid request needs.
fn options_query(rv: &str, allow: &Value, send: &Value) -> String {
    let mut q = String::from("resourceVersion=");
    q.push_str(rv);
    if let Some(allow) = allow.as_bool() {
        q.push_str(if allow {
            "&allowWatchBookmarks=true"
        } else {
            "&allowWatchBookmarks=false"
        });
    }
    if let Some(send) = send.as_bool() {
        q.push_str(if send {
            "&sendInitialEvents=true"
        } else {
            "&sendInitialEvents=false"
        });
        q.push_str("&resourceVersionMatch=NotOlderThan");
    }
    q
}

/// A line in the matrix rows' notation: `ADDED pod-1`, or a bookmark as
/// `{type, object_metadata}`.
fn matrix_event(line: &Value) -> Value {
    if line["type"] == "BOOKMARK" {
        json!({"type": "BOOKMARK", "object_metadata": line["object"]["metadata"]})
    } else {
        let mut s = line["type"].as_str().unwrap_or("?").to_owned();
        s.push(' ');
        s.push_str(line_name(line));
        Value::String(s)
    }
}

/// A line in the selector row's notation: `MODIFIED pod{foo}@1002`.
fn selector_event(line: &Value) -> String {
    let labels = &line["object"]["metadata"]["labels"];
    let present: Vec<&str> = ["foo", "bar"]
        .into_iter()
        .filter(|l| labels.get(*l).is_some())
        .collect();
    let mut s = line["type"].as_str().unwrap_or("?").to_owned();
    s.push_str(" pod{");
    s.push_str(&present.join(","));
    s.push_str("}@");
    s.push_str(&rv_of(line).map_or_else(String::new, |r| r.to_string()));
    s
}

/// The fixture's spelling of a validation violation.
fn violation_text(v: &engenho_apiserver::OptionViolation) -> String {
    let mut s = match v.kind() {
        ViolationKind::Forbidden => String::from("Forbidden "),
        ViolationKind::NotSupported => String::from("NotSupported "),
    };
    s.push_str(v.field());
    s.push_str(": ");
    match v.kind() {
        ViolationKind::Forbidden => s.push_str(v.forbidden_detail().unwrap_or_default()),
        ViolationKind::NotSupported => {
            s.push_str("supported values ");
            s.push_str(&v.supported_values().join(", "));
        }
    }
    s
}

#[allow(clippy::too_many_lines)]
async fn answer(case: &Case) -> Answer {
    let input = &case.input;
    let expected = &case.expected;
    let v = match case.name.as_str() {
        // ── the floor: too old → in-band 410, the boundary itself served ──
        "too_old.empty_buffer_after_relist.rv_below_list_rv" => {
            let c = history(9, &[], Bookmarks::Production).await;
            history_answer(&c.watch(&plain_from(8)).await, expected)
        }
        "too_old.empty_buffer_after_relist.rv_equal_list_rv_is_accepted" => {
            let c = history(9, &[], Bookmarks::Production).await;
            history_answer(&c.watch(&plain_from(9)).await, expected)
        }
        "too_old.after_relist.event_after_list_rv_is_delivered" => {
            let c = history(9, &[12], Bookmarks::Production).await;
            history_answer(&c.watch(&plain_from(9)).await, expected)
        }
        "too_old.no_relist.boundary_is_first_buffered_rv_minus_one" => {
            let c = history(2, &[3], Bookmarks::Production).await;
            history_answer(&c.watch(&plain_from(1)).await, expected)
        }
        "too_old.no_relist.rv_exactly_oldest_minus_one_is_accepted" => {
            let c = history(2, &[3], Bookmarks::Production).await;
            history_answer(&c.watch(&plain_from(2)).await, expected)
        }
        "too_old.not_full.modified_events_carry_prev_object" => {
            let c = Cluster::boot(Bookmarks::Production).await;
            for rev in [3, 4, 5] {
                c.pod_at("pod", json!({"v": rev.to_string()}), rev).await;
            }
            history_answer(&c.watch(&plain_from(3)).await, expected)
        }
        "too_old.full_buffer.evicted_prefix_is_gone" => {
            let c = history(4, &[5, 6, 7, 8, 9], Bookmarks::Production).await;
            history_answer(&c.watch(&plain_from(3)).await, expected)
        }
        "too_old.full_buffer.oldest_minus_one_replays_whole_buffer" => {
            let c = history(4, &[5, 6, 7, 8, 9], Bookmarks::Production).await;
            history_answer(&c.watch(&plain_from(4)).await, expected)
        }
        "too_old.eviction_since_relist_invalidates_list_rv_floor" => {
            // Evicted through 39: the ring holds 40 and 50.
            let c = history(39, &[40, 50], Bookmarks::Production).await;
            history_answer(&c.watch(&plain_from(15)).await, expected)
        }
        "too_old.conservative_floor_ignores_rv_gaps" => {
            // The newest evicted revision is 50; engenho's ring is dense, so
            // 51..59 are retained (ConfigMaps) before the pods at 60, 61.
            let c = history(50, &[60, 61], Bookmarks::Production).await;
            let mut per_rv = Map::new();
            for from in ["52", "58", "59", "60"] {
                let w = c.watch(&plain_from(from.parse().unwrap_or_default())).await;
                let verdict = if error_of(&w).is_some() {
                    String::from("too_old")
                } else {
                    let rvs: Vec<String> = events_of(&w)
                        .iter()
                        .filter_map(|l| rv_of(l))
                        .map(|r| r.to_string())
                        .collect();
                    let mut s = String::from("ok events [");
                    s.push_str(&rvs.join(","));
                    s.push(']');
                    s
                };
                per_rv.insert(from.to_owned(), Value::String(verdict));
            }
            json!({"per_rv": per_rv})
        }
        "too_old.relist_empties_buffer" => {
            let c = history(200, &[], Bookmarks::Production).await;
            history_answer(&c.watch(&plain_from(150)).await, expected)
        }
        "error_form.too_old_is_in_stream_error_event_not_http_410" => {
            let c = history(5, &[], Bookmarks::Production).await;
            let w = c.watch(&plain_from(4)).await;
            let error = error_of(&w);
            json!({
                "outcome": outcome(&w),
                "http_status": w.status,
                "event": error.map(|o| json!({
                    "type": "ERROR",
                    "object": {"status": o["status"], "code": o["code"], "reason": o["reason"]},
                })),
                "then": says(w.ended && w.lines.len() == 1, "stream closes", (&w.lines, w.ended)),
            })
        }
        "watchlist.old_rv_with_send_initial_events_never_410" => {
            let c = history(499, &[500, 501], Bookmarks::Production).await;
            let w = c
                .watch(
                    "resourceVersion=10&sendInitialEvents=true&resourceVersionMatch=NotOlderThan\
                     &allowWatchBookmarks=true&timeoutSeconds=1",
                )
                .await;
            let added: Vec<&str> = events_of(&w).iter().map(|l| line_name(l)).collect();
            let end = w.of_type("BOOKMARK").first().and_then(|l| rv_of(l));
            let whole = added == ["pod-500", "pod-501"] && end == Some(c.current().await);
            json!({
                "result": if error_of(&w).is_some() { "too_old" } else { "ok" },
                "events": says(whole, "ADDED for every object currently in the store, each at cache RV", (&added, end)),
            })
        }
        "routing.rv_zero_send_initial_events_false_starts_now_without_state" => {
            let c = Cluster::boot(Bookmarks::Production).await;
            c.pod_at("existing", json!({}), 300).await;
            let mut open = c
                .open(
                    "resourceVersion=0&sendInitialEvents=false&resourceVersionMatch=NotOlderThan\
                     &allowWatchBookmarks=false",
                )
                .await;
            c.pod_at("next", json!({}), 301).await;
            let lines = open
                .lines_until(Duration::from_secs(5), |l| rv_of(l) == Some(301))
                .await;
            let (before, after): (Vec<&Value>, Vec<&Value>) = lines
                .iter()
                .partition(|l| rv_of(l).is_none_or(|r| r <= 300));
            json!({
                "initial_events": before.iter().map(|l| l["type"].clone()).collect::<Vec<_>>(),
                "subsequent": says(
                    after.first().and_then(|l| rv_of(l)) == Some(301),
                    "only events with rv > 300",
                    &lines,
                ),
            })
        }
        // ── error forms ──
        "error_form.unparseable_rv_is_direct_error" => {
            // The fixture's `error` is upstream's storage-layer value, which
            // no wire carries; only the direct (non-200) answer is compared.
            let c = Cluster::boot(Bookmarks::Production).await;
            json!({"outcome": outcome(&c.watch("resourceVersion=abc").await)})
        }
        // ── ahead of the store: engenho refuses at once (deviations) ──
        "future_rv.plain_watch_ahead_of_cache_is_accepted_without_504" => {
            let c = history(5, &[], Bookmarks::Production).await;
            json!({"outcome": outcome(&c.watch(&plain_from(6)).await)})
        }
        "future_rv.watchlist_succeeds_when_cache_catches_up_within_timeout" => {
            let c = Cluster::boot(Bookmarks::Production).await;
            c.advance_to(100).await;
            let w = c
                .watch(
                    "resourceVersion=105&sendInitialEvents=true\
                     &resourceVersionMatch=NotOlderThan&allowWatchBookmarks=true&timeoutSeconds=1",
                )
                .await;
            json!({"outcome": outcome(&w)})
        }
        "future_rv.events_up_to_requested_rv_are_suppressed" => {
            let c = Cluster::boot(Bookmarks::Production).await;
            c.pod_at("foo", json!({}), 1000).await;
            let w = c.watch(&plain_from(1010)).await;
            json!({"first_event_rv": events_of(&w).first().and_then(|l| rv_of(l))})
        }
        "future_rv.periodic_bookmarks_suppressed_until_cache_passes_rv" => {
            let c = Cluster::boot(Bookmarks::Fast).await;
            c.advance_to(1500).await;
            let w = c
                .watch("resourceVersion=2000&allowWatchBookmarks=true&timeoutSeconds=1")
                .await;
            let delivered: Vec<Value> = w
                .lines
                .iter()
                .map(|l| json!({"type": l["type"], "rv": rv_of(l), "code": l["object"]["code"]}))
                .collect();
            json!({"delivered": delivered})
        }
        "future_rv.watchlist_blocks_3s_then_504_too_large" => {
            let c = Cluster::boot(Bookmarks::Production).await;
            c.advance_to(100).await;
            let t0 = Instant::now();
            let w = c
                .watch(
                    "resourceVersion=105&sendInitialEvents=true\
                     &resourceVersionMatch=NotOlderThan&allowWatchBookmarks=true&timeoutSeconds=5",
                )
                .await;
            json!({
                "outcome": outcome(&w),
                "http_status": w.status,
                "block_seconds": t0.elapsed().as_secs(),
                "event": error_of(&w).map(|o| json!({"type": "ERROR", "object": {
                    "status": o["status"], "code": o["code"], "reason": o["reason"],
                    "message": o["message"], "details": o["details"],
                }})),
            })
        }
        // ── the one comparison every watch passes: suppress rv <= requested ──
        "future_rv.event_exactly_at_requested_rv_is_not_delivered" => {
            let c = history(0, &[49, 50, 51], Bookmarks::Production).await;
            let w = c.watch(&plain_from(50)).await;
            json!({"delivered_rvs": events_of(&w).iter().map(|l| rv_of(l)).collect::<Vec<_>>()})
        }
        // ── a LIST ahead of the store waits, then 504 TooLarge (agrees) ──
        "future_rv.list_timeout_is_also_too_large" => {
            let c = Cluster::boot(Bookmarks::Production).await;
            c.advance_to(3).await;
            let s = Status::from_json(&c.list("resourceVersion=4").await.body);
            // Server side, storage.IsTooLargeResourceVersion: IsTimeout AND
            // the ResourceVersionTooLarge cause.
            let too_large =
                cg::is_timeout(&s) && s.cause_types.iter().any(|t| t == "ResourceVersionTooLarge");
            json!({"is_timeout": cg::is_timeout(&s), "is_too_large_resource_version": too_large})
        }
        "watchlist.rv_zero_skips_wait" => {
            let c = Cluster::boot(Bookmarks::Production).await;
            c.advance_to(5).await;
            let t0 = Instant::now();
            let mut open = c
                .open(
                    "resourceVersion=0&sendInitialEvents=true&resourceVersionMatch=NotOlderThan\
                     &allowWatchBookmarks=true",
                )
                .await;
            let first = open.next_line(Duration::from_secs(5)).await;
            let parsed: ListWatchParams =
                serde_urlencoded::from_str("resourceVersion=0").unwrap_or_default();
            let point = parsed
                .resume_point()
                .unwrap_or(ResumePoint::At(Revision(0)));
            json!({
                "waits": first.is_none() || t0.elapsed() >= Duration::from_secs(1),
                // "0" is MostRecent, which no store revision is behind.
                "can_return_504": point.ahead_of(Revision::ZERO).is_some(),
            })
        }
        // ── initial events and their end bookmark ──
        "watchlist.empty_store_still_gets_end_bookmark" => {
            let c = Cluster::boot(Bookmarks::Production).await;
            c.advance_to(100).await;
            let w = c
                .watch(
                    "resourceVersion=&sendInitialEvents=true&resourceVersionMatch=NotOlderThan\
                     &allowWatchBookmarks=true&timeoutSeconds=1",
                )
                .await;
            json!({"events": w.lines.iter().map(matrix_event).collect::<Vec<_>>()})
        }
        "watchlist.initial_events_end_bookmark_matrix" => {
            let c = Cluster::boot(Bookmarks::Production).await;
            c.advance_to(100).await;
            for (name, rev) in [("pod-1", 101), ("pod-2", 102), ("pod-3", 103)] {
                c.pod_at(name, json!({}), rev).await;
            }
            let rv = input["watch_rv"].as_str().unwrap_or("100");
            let mut rows = Vec::new();
            for row in expected["rows"].as_array().into_iter().flatten() {
                let (allow, send) = (&row["allow_watch_bookmarks"], &row["send_initial_events"]);
                let mut q = options_query(rv, allow, send);
                q.push_str("&timeoutSeconds=1");
                let w = c.watch(&q).await;
                rows.push(json!({
                    "allow_watch_bookmarks": allow,
                    "send_initial_events": send,
                    "events": w.lines.iter().map(matrix_event).collect::<Vec<_>>(),
                }));
            }
            json!({"rows": rows})
        }
        "watchlist.bookmark_object_is_empty_shell_with_rv" => {
            let c = Cluster::boot(Bookmarks::Production).await;
            c.pod_at("pod-1", json!({"app": "x"}), 103).await;
            let w = c
                .watch(
                    "resourceVersion=0&sendInitialEvents=true&resourceVersionMatch=NotOlderThan\
                     &allowWatchBookmarks=true&timeoutSeconds=1",
                )
                .await;
            let bookmark = w.of_type("BOOKMARK").first().map(|l| l["object"].clone());
            let shell = bookmark.as_ref().is_some_and(|o| {
                let top_ok = o.as_object().is_some_and(|m| {
                    m.keys()
                        .all(|k| ["kind", "apiVersion", "metadata"].contains(&k.as_str()))
                });
                let meta_ok = o["metadata"].as_object().is_some_and(|m| {
                    m.contains_key("resourceVersion")
                        && m.keys()
                            .all(|k| ["resourceVersion", "annotations"].contains(&k.as_str()))
                });
                top_ok && meta_ok && o["kind"] == "Pod"
            });
            json!({"object": says(
                shell,
                "c.newFunc() (zero-value object of the resource kind) with only metadata.resourceVersion set (+ initial-events-end annotation when applicable)",
                bookmark,
            )})
        }
        "routing.rv_unset_watch_is_served_from_cache_like_rv_zero" => {
            let c = history(0, &[1, 2], Bookmarks::Production).await;
            let mut open = c.open("").await;
            let initial = open
                .lines_until(Duration::from_secs(5), |l| line_name(l) == "pod-2")
                .await;
            c.pod_at("pod-3", json!({}), 3).await;
            let live = open
                .lines_until(Duration::from_secs(5), |l| line_name(l) == "pod-3")
                .await;
            let names = |ls: &[Value]| {
                ls.iter()
                    .filter(|l| l["type"] == "ADDED")
                    .map(|l| line_name(l).to_owned())
                    .collect::<Vec<_>>()
            };
            let served = names(&initial) == ["pod-1", "pod-2"] && names(&live) == ["pod-3"];
            json!({"initial_events": says(
                served,
                "synthetic ADDED for every object in the watch cache store, then live events",
                (names(&initial), names(&live)),
            )})
        }
        "routing.rv_zero_initial_events_use_cache_rv_not_object_rv_for_dedup" => {
            let c = history(0, &[9, 10], Bookmarks::Production).await;
            let requested = input["requested_rv"].as_u64().unwrap_or(9);
            let mut q = String::from("resourceVersion=");
            q.push_str(&requested.to_string());
            q.push_str(
                "&sendInitialEvents=true&resourceVersionMatch=NotOlderThan&allowWatchBookmarks=true",
            );
            let mut open = c.open(&q).await;
            let initial = open
                .lines_until(Duration::from_secs(5), |l| l["type"] == "BOOKMARK")
                .await;
            let end_rv = initial.last().and_then(rv_of);
            c.pod_at("pod-11", json!({}), 11).await;
            let after = open
                .lines_until(Duration::from_secs(5), |l| rv_of(l) == Some(11))
                .await;
            json!({
                "watcher_rv_after_interval": end_rv,
                "delivered_after_init": after
                    .iter()
                    .filter(|l| l["type"] != "BOOKMARK")
                    .map(|l| json!({"rv": rv_of(l)}))
                    .collect::<Vec<_>>(),
            })
        }
        // ── validation ──
        "validation.watch_options_matrix" => {
            let mut rows = Vec::new();
            for row in expected["rows"].as_array().into_iter().flatten() {
                // Every key but `errors` describes the request.
                let mut q = String::new();
                if let Some(send) = row["send_initial_events"].as_bool() {
                    q.push_str(if send {
                        "sendInitialEvents=true"
                    } else {
                        "sendInitialEvents=false"
                    });
                }
                if let Some(m) = row["resource_version_match"].as_str() {
                    q.push_str("&resourceVersionMatch=");
                    q.push_str(m);
                }
                if let Some(allow) = row["allow_watch_bookmarks"].as_bool() {
                    q.push_str(if allow {
                        "&allowWatchBookmarks=true"
                    } else {
                        "&allowWatchBookmarks=false"
                    });
                }
                let params: ListWatchParams =
                    serde_urlencoded::from_str(q.trim_start_matches('&')).unwrap_or_default();
                let verdict = if row["list"] == true {
                    params.validate_list()
                } else {
                    params.watch_initial_events().map(drop)
                };
                let errors: Vec<String> = verdict
                    .err()
                    .map(|e| e.violations().iter().map(violation_text).collect())
                    .unwrap_or_default();
                let mut out = row.as_object().cloned().unwrap_or_default();
                out.insert("errors".into(), json!(errors));
                rows.push(Value::Object(out));
            }
            json!({"rows": rows})
        }
        // ── bookmarks ──
        "bookmark.not_requested_never_sent" | "bookmark.requested_is_sent_and_monotonic" => {
            let c = Cluster::boot(Bookmarks::Fast).await;
            let allow = case.name.ends_with("monotonic");
            let mut open = c
                .open(if allow {
                    "resourceVersion=0&allowWatchBookmarks=true&timeoutSeconds=2"
                } else {
                    "resourceVersion=0&allowWatchBookmarks=false&timeoutSeconds=2"
                })
                .await;
            for i in 1..=10u64 {
                c.pod_at(&pod_name(i), json!({}), i).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let lines = open.lines_for(Duration::from_secs(5)).await;
            let bookmarks = lines.iter().filter(|l| l["type"] == "BOOKMARK").count();
            let mut newest = 0;
            let mut monotonic = true;
            for l in &lines {
                let rv = rv_of(l).unwrap_or(0);
                if l["type"] == "BOOKMARK" {
                    monotonic &= rv >= newest;
                }
                newest = newest.max(rv);
            }
            if allow {
                json!({"bookmark_rv_ge_last_observed_rv": bookmarks > 0 && monotonic})
            } else {
                json!({"bookmark_events": bookmarks})
            }
        }
        "bookmark.rv_equal_to_watcher_rv_is_dropped_for_plain_watch" => {
            let c = Cluster::boot(Bookmarks::Fast).await;
            c.advance_to(100).await;
            let mut open = c.open("resourceVersion=100&allowWatchBookmarks=true").await;
            // Long enough for the store's first heartbeat, at 100, to be due.
            tokio::time::sleep(FAST_BOOKMARKS * 4).await;
            c.advance_to(101).await;
            let lines = open
                .lines_until(Duration::from_secs(5), |l| {
                    l["type"] == "BOOKMARK" && rv_of(l).is_some_and(|r| r >= 101)
                })
                .await;
            json!({"delivered": lines.iter().filter(|l| l["type"] == "BOOKMARK").map(rv_of).collect::<Vec<_>>()})
        }
        "bookmark.rv_equal_is_delivered_once_for_watchlist" => {
            let c = Cluster::boot(Bookmarks::Fast).await;
            c.advance_to(100).await;
            let mut open = c
                .open(
                    "resourceVersion=100&sendInitialEvents=true&resourceVersionMatch=NotOlderThan\
                     &allowWatchBookmarks=true",
                )
                .await;
            let lines = open.lines_for(FAST_BOOKMARKS * 6).await;
            let delivered: Vec<Value> = lines
                .iter()
                .filter(|l| l["type"] == "BOOKMARK")
                .map(|l| json!({
                    "type": "BOOKMARK",
                    "rv": rv_of(l),
                    "annotations": l["object"]["metadata"]["annotations"].as_object().cloned().unwrap_or_default(),
                }))
                .collect();
            json!({"delivered": delivered})
        }
        "bookmark.storage_bookmarks_not_forwarded_only_advance_rv" => {
            let c = Cluster::boot(Bookmarks::Fast).await;
            c.pod_at("pod", json!({}), 1000).await;
            let mut open = c
                .open("resourceVersion=1000&allowWatchBookmarks=true")
                .await;
            // Changes this watch filters out move the store to 2000.
            c.advance_to(2000).await;
            let lines = open
                .lines_until(Duration::from_secs(5), |l| {
                    l["type"] == "BOOKMARK" && rv_of(l).is_some_and(|r| r >= 2000)
                })
                .await;
            let last = lines.iter().rev().find(|l| l["type"] == "BOOKMARK");
            json!({"eventually": last.map(|l| json!({"type": "BOOKMARK", "rv": rv_of(l)}))})
        }
        "bookmark.multiple_bookmarks_monotonic" => {
            let c = Cluster::boot(Bookmarks::Fast).await;
            c.pod_at("pod", json!({}), 100).await;
            let mut open = c
                .open("resourceVersion=100&allowWatchBookmarks=true&timeoutSeconds=2")
                .await;
            c.pod_at("pod-101", json!({}), 101).await;
            // Twenty bookmark periods; upstream's row counts five.
            let lines = open.lines_for(Duration::from_secs(5)).await;
            let mut newest = 0;
            let mut monotonic = true;
            let mut bookmarks = 0;
            for l in &lines {
                let rv = rv_of(l).unwrap_or(0);
                if l["type"] == "BOOKMARK" {
                    bookmarks += 1;
                    monotonic &= rv >= newest;
                }
                newest = newest.max(rv);
            }
            json!({
                "bookmarks_within_5s": if bookmarks >= 2 { String::from(">= 2") } else { bookmarks.to_string() },
                "each_bookmark_rv_ge_previous_event_rv": monotonic,
            })
        }
        "bookmark.initial_events_end_annotation_only_on_first_matching_bookmark" => {
            let c = Cluster::boot(Bookmarks::Fast).await;
            c.pod_at("pod-5", json!({}), 5).await;
            c.advance_to(10).await;
            let mut list = c
                .open(
                    "resourceVersion=10&sendInitialEvents=true&resourceVersionMatch=NotOlderThan\
                     &allowWatchBookmarks=true",
                )
                .await;
            let initial = list
                .lines_until(Duration::from_secs(5), |l| l["type"] == "BOOKMARK")
                .await;
            let end = initial.last().filter(|l| l["type"] == "BOOKMARK");
            let mut plain = c.open("resourceVersion=10&allowWatchBookmarks=true").await;
            c.pod_at("pod-15", json!({}), 15).await;
            let later = plain
                .lines_until(Duration::from_secs(5), |l| {
                    l["type"] == "BOOKMARK" && rv_of(l).is_some_and(|r| r >= 15)
                })
                .await;
            let annotated = later
                .iter()
                .filter(|l| l["type"] == "BOOKMARK")
                .any(|l| l["object"]["metadata"].get("annotations").is_some());
            json!({
                "bookmark_event": end.map(|l| json!({"type": "BOOKMARK", "metadata": l["object"]["metadata"]})),
                "plain_watch_bookmarks_annotated": annotated,
            })
        }
        // ── slow and fast watchers ──
        "slow_watcher.not_going_back_in_time" | "slow_watcher.fast_watcher_not_blocked_by_slow" => {
            let last = if case.name.ends_with("in_time") {
                1099
            } else {
                1049
            };
            let c = Cluster::boot(Bookmarks::Production).await;
            c.pod_at(&pod_name(1000), json!({}), 1000).await;
            // w1 never reads.
            let _slow = c
                .open("resourceVersion=999&allowWatchBookmarks=false")
                .await;
            let mut fast = c
                .open("resourceVersion=999&allowWatchBookmarks=false")
                .await;
            for rev in 1001..=last {
                c.pod_at(&pod_name(rev), json!({}), rev).await;
            }
            let lines = fast
                .lines_until(Duration::from_secs(10), |l| rv_of(l) == Some(last))
                .await;
            let rvs: Vec<u64> = lines.iter().filter_map(rv_of).collect();
            if last == 1099 {
                json!({"w2_rvs_non_decreasing": rvs.windows(2).all(|w| w[0] <= w[1]) && rvs.last() == Some(&last)})
            } else {
                json!({"w2_added_events": lines.iter().filter(|l| l["type"] == "ADDED").count()})
            }
        }
        "slow_watcher.termination_is_clean_eof_no_error_event" => {
            // A watcher that fell behind after it was sent something: the
            // end the router takes (watch_end.rs), with and without
            // bookmarks.
            let ends: Vec<Option<Value>> = [false, true]
                .into_iter()
                .map(|bookmarks| {
                    let mut progress = WatchProgress::new(Revision(10), bookmarks);
                    progress.delivered(Revision(12));
                    match progress.after(&WatchGone::Overflow {
                        capacity: 1024,
                        last_seen: Revision(15),
                    }) {
                        engenho_apiserver::AfterGone::End(end) => end
                            .final_line(engenho_apiserver::params::WatchGvk {
                                api_version: "v1",
                                kind: "Pod",
                            })
                            .map(|b| {
                                serde_json::from_slice(b.trim_ascii_end()).unwrap_or(Value::Null)
                            }),
                        engenho_apiserver::AfterGone::Resume(_) => Some(json!({"type": "RESUME"})),
                    }
                })
                .collect();
            let error = ends
                .iter()
                .flatten()
                .any(|l| l["type"] == "ERROR" || l["type"] == "RESUME");
            json!({"outcome": if error { "error_event" } else { "clean_eof" }, "error_event": error})
        }
        // ── event shape ──
        "event_shape.deleted_via_selector_transition_carries_transition_rv" => {
            let c = Cluster::boot(Bookmarks::Production).await;
            c.pod_at("pod", json!({"foo": "true", "bar": "true"}), 1001)
                .await;
            c.pod_at("pod", json!({"foo": "true"}), 1002).await;
            c.pod_at("pod", json!({}), 1003).await;
            c.delete_pod_at("pod", 1004).await;
            let mut out = Map::new();
            for (watcher, selector) in [
                ("all", ""),
                ("foo", "&labelSelector=foo=true"),
                ("bar", "&labelSelector=bar=true"),
            ] {
                let mut q = plain_from(1000);
                q.push_str(selector);
                let w = c.watch(&q).await;
                out.insert(
                    watcher.to_owned(),
                    json!(
                        events_of(&w)
                            .iter()
                            .map(|l| selector_event(l))
                            .collect::<Vec<_>>()
                    ),
                );
            }
            Value::Object(out)
        }
        // ── the watch deadline ──
        "server_timeout.randomized_when_unset" => {
            let c = Cluster::boot(Bookmarks::Production).await;
            let bounded = c.watch(&plain_from(0)).await;
            let mut unbounded = c.open("allowWatchBookmarks=false").await;
            let _ = unbounded.lines_for(Duration::from_millis(1500)).await;
            json!({
                "timeout": says(unbounded.ended, "T * (1 + rand[0,1)) => [T, 2T)", "none: open until the client leaves"),
                "at_timeout": says(
                    bounded.ended && error_of(&bounded).is_none(),
                    "clean_eof, no ERROR event",
                    (&bounded.lines, bounded.ended),
                ),
            })
        }
        // ── the clients ──
        "client.kube_rs.error_410_resets_to_relist" => {
            let (emitted, state) = kr::step(
                &State::Watching { rv: "500".into() },
                &Item::Error { code: 410 },
            );
            json!({
                "emits": says(emitted == kr::Emitted::WatchError, "Err(Error::WatchError(status))", emitted),
                "next_state": says(state == State::Empty, "Empty (full relist; default ListSemantic::MostRecent => list with resourceVersion unset)", &state),
            })
        }
        "client.kube_rs.non_410_error_keeps_position" => {
            let watching = State::Watching { rv: "500".into() };
            let after_error = kr::step(&watching, &Item::Error { code: 504 }).1;
            let after_end = kr::step(&after_error, &Item::End).1;
            json!({
                "after_error": Long(&after_error).to_string(),
                "after_stream_end": says(
                    after_end == State::InitListed { rv: "500".into() }
                        && !kr::resumed_query("500").contains("sendInitialEvents"),
                    "InitListed{resource_version: \"500\"} => watch(rv=\"500\") without sendInitialEvents",
                    &after_end,
                ),
            })
        }
        "client.kube_rs.bookmark_advances_resume_rv_silently" => {
            let (emitted, state) = kr::step(
                &State::Watching { rv: "500".into() },
                &Item::Bookmark {
                    rv: "900".into(),
                    end: false,
                },
            );
            json!({
                "emits": (emitted != kr::Emitted::Nothing).then(|| emitted.spelled()),
                "next_state": Long(&state).to_string(),
            })
        }
        "client.kube_rs.clean_eof_resumes_from_last_rv" => {
            let state = kr::step(&State::Watching { rv: "900".into() }, &Item::End).1;
            json!({"next_state": Long(&state).to_string()})
        }
        "client.kube_rs.streaming_list_initial_watch" => {
            // What kube-rs sends, and what engenho does with it.
            let c = Cluster::boot(Bookmarks::Production).await;
            c.advance_to(7).await;
            let mut open = c.open(kr::STREAMING_LIST_QUERY).await;
            let lines = open
                .lines_until(Duration::from_secs(5), |l| {
                    l["type"] == "BOOKMARK" || l["type"] == "ERROR"
                })
                .await;
            let end = lines.last().filter(|l| {
                l["object"]["metadata"]["annotations"]["k8s.io/initial-events-end"] == "true"
            });
            let served = open.status == 200 && end.and_then(rv_of) == Some(7);
            json!({
                "initial_query": kr::STREAMING_LIST_QUERY,
                "server_side_consequence": says(
                    served,
                    "requiredRV=0 so the cacher never blocks this request and never returns 504; bookmarkAfterRV = cache RV at registration",
                    &lines,
                ),
                "resumed_watch_sends_send_initial_events": kr::resumed_query("7").contains("sendInitialEvents"),
            })
        }
        "client.kube_rs.initial_watch_bookmark_handling" => {
            let (first_emitted, first) = kr::step(
                &State::InitialWatch,
                &Item::Bookmark {
                    rv: "50".into(),
                    end: false,
                },
            );
            let (second_emitted, second) = kr::step(
                &first,
                &Item::Bookmark {
                    rv: "60".into(),
                    end: true,
                },
            );
            json!({
                "first": says(first == State::InitialWatch && first_emitted == kr::Emitted::Nothing, "ignored, stay InitialWatch", (&first, first_emitted)),
                "second": says(
                    second_emitted == kr::Emitted::InitDone && second == State::Watching { rv: "60".into() },
                    "emit InitDone; next_state Watching{resource_version: \"60\"}",
                    (&second, second_emitted),
                ),
            })
        }
        "client.kube_rs.initial_watch_eof_restarts_from_scratch" => {
            let state = kr::step(&State::InitialWatch, &Item::End).1;
            json!({"next_state": says(state == State::Empty, "Empty (not InitListed)", &state)})
        }
        "client.client_go.watch_410_relists_at_last_rv_first" => {
            let object =
                json!({"kind": "Status", "apiVersion": "v1", "code": 410, "reason": "Expired"});
            let (next, _) = cg::after_watch(&Ended::Error(object));
            let last = input["last_sync_rv"].as_str().unwrap_or("");
            let (calls, _) = cg::list_calls(last, |rv| {
                if rv == last {
                    Listed::Refused(Status::of(410, "Expired"))
                } else {
                    Listed::At(String::from("501"))
                }
            });
            json!({
                "isLastSyncResourceVersionUnavailable": false,
                "next_list_rv": says(
                    next == Next::Relist && calls.first().map(String::as_str) == Some("500"),
                    "500 (NotOlderThan semantics, may be served from watch cache)",
                    (&next, &calls),
                ),
                "if_that_list_returns_410_or_too_large": says(
                    calls.get(1).is_some_and(String::is_empty),
                    "retry list immediately with resourceVersion=\"\" (quorum read)",
                    &calls,
                ),
            })
        }
        "client.client_go.too_large_detection" => {
            let mut out = Map::new();
            for e in input["errors"].as_array().into_iter().flatten() {
                let id = e["id"].as_str().unwrap_or("?").to_owned();
                out.insert(
                    id,
                    Value::Bool(cg::is_too_large_resource_version(&Status::from_json(e))),
                );
            }
            json!({"is_too_large": out})
        }
        _ => return Answer::NotChecked,
    };
    Answer::Checked(v)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn watch_410_rows_agree_with_upstream() {
    let table = Vector::Watch410Bookmark.load();
    let answers: HashMap<String, Answer> = futures::stream::iter(table.cases.clone())
        .map(|case| async move {
            let a = answer(&case).await;
            (case.name, a)
        })
        .buffer_unordered(8)
        .collect()
        .await;
    let report = assert_table(&table, OUT_OF_SCOPE, DEVIATIONS, |case| {
        answers
            .get(&case.name)
            .cloned()
            .unwrap_or(Answer::NotChecked)
    });
    assert_eq!(
        report.checked + report.out_of_scope,
        table.cases.len(),
        "every row is checked or out of scope"
    );
    eprintln!("watch-410-bookmark: {report:?}");
}
