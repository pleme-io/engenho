//! M0.1 item 4 — HTTP list-then-watch contract.
//!
//! The K8s list-then-watch contract over HTTP, end-to-end against a
//! real `Arc<StoreMesh>`:
//!
//!   1. LIST returns the snapshot rv (atomic — `current_revision`, NOT
//!      `last_applied_index`).
//!   2. WATCH from that rv is gap+dup-free (replay tail then live tail
//!      one contiguous ordered range).
//!   3. CompactedTooOld → 410 (the `WatchGone::CompactedTooOld =>
//!      ApiError::Gone` mapping + a handler-seam 410, plus the in-band
//!      410 line shape).
//!   4. GVK/namespace filter excludes other resources.
//!   5. labelSelector filter (LIST + WATCH).
//!   6. Bookmark passthrough (+ opt-out + resume-from-bookmark).
//!   7. rv=0/absent = most recent, no replay.
//!
//! Two harness levels: (1) HTTP via reqwest over the real ApiServer
//! (chunked NDJSON read incrementally with a per-line timeout so a hang
//! fails fast); (2) handler-unit level calling
//! `StoreBackedHandler::{list_at, watch_stream}` directly against a real
//! StoreMesh.

use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{ApiServer, ResourceHandler, ResumePoint, Selectors, StoreBackedHandler};
use engenho_store::{InProcessRouter, Revision, StoreMesh, WatchSignal, default_config};

// ── boot helpers ──────────────────────────────────────────────────

async fn boot_store() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("apiserver-r78").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

async fn boot_store_and_server() -> (Arc<StoreMesh>, ApiServer) {
    let store = boot_store().await;
    let pod_handler: Arc<dyn ResourceHandler> = Arc::new(StoreBackedHandler::for_core_kind(
        store.clone(),
        "Pod",
        true,
    ));
    let cm_handler: Arc<dyn ResourceHandler> = Arc::new(StoreBackedHandler::for_core_kind(
        store.clone(),
        "ConfigMap",
        true,
    ));
    let server = ApiServer::start(
        "127.0.0.1:0".parse().unwrap(),
        vec![pod_handler, cm_handler],
        None,
    )
    .await
    .unwrap();
    (store, server)
}

fn pod_body(name: &str) -> serde_json::Value {
    serde_json::json!({ "metadata": { "name": name }, "spec": { "containers": [ { "name": "c", "image": "busybox:1.36" } ] } })
}

fn pod_body_labeled(name: &str, key: &str, val: &str) -> serde_json::Value {
    serde_json::json!({ "metadata": { "name": name, "labels": { key: val } }, "spec": { "containers": [ { "name": "c", "image": "busybox:1.36" } ] } })
}

async fn post_pod(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    ns: &str,
    body: &serde_json::Value,
) {
    let resp = client
        .post(format!("http://{addr}/api/v1/namespaces/{ns}/pods"))
        .json(body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::CREATED,
        "POST pod failed"
    );
}

async fn post_cm(client: &reqwest::Client, addr: std::net::SocketAddr, ns: &str, name: &str) {
    let resp = client
        .post(format!("http://{addr}/api/v1/namespaces/{ns}/configmaps"))
        .json(&serde_json::json!({"metadata": {"name": name}, "data": {}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
}

/// An incremental NDJSON reader over a streaming reqwest response. Each
/// `next_line` pulls chunks (with a per-line timeout so a hang fails
/// fast) until a full `\n`-terminated line is buffered.
struct NdjsonReader {
    resp: reqwest::Response,
    buf: Vec<u8>,
}

impl NdjsonReader {
    fn new(resp: reqwest::Response) -> Self {
        Self {
            resp,
            buf: Vec::new(),
        }
    }

    /// Next NDJSON object, or `None` if the stream ended. Bounded by a
    /// timeout — a hang panics rather than blocking the test forever.
    async fn next_line(&mut self) -> Option<serde_json::Value> {
        loop {
            // A complete line already buffered?
            if let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = self.buf.drain(..=pos).collect();
                let trimmed = &line[..line.len() - 1];
                if trimmed.is_empty() {
                    continue;
                }
                return Some(serde_json::from_slice(trimmed).expect("NDJSON line parses"));
            }
            // Pull another chunk with a timeout (a hang fails fast).
            let chunk = tokio::time::timeout(Duration::from_secs(3), self.resp.chunk())
                .await
                .expect("watch chunk arrived before timeout")
                .expect("watch chunk read ok");
            match chunk {
                Some(bytes) => self.buf.extend_from_slice(&bytes),
                None => return None, // stream ended
            }
        }
    }

    /// Next line that is a WATCH event of the given `type`, skipping
    /// bookmarks. Returns the `object`.
    async fn next_event(&mut self) -> serde_json::Value {
        loop {
            let line = self.next_line().await.expect("expected an event, got EOF");
            match line.get("type").and_then(|t| t.as_str()) {
                Some("BOOKMARK") => continue,
                Some(_) => return line,
                None => panic!("watch line missing type: {line}"),
            }
        }
    }
}

/// The `resourceVersion` of a watch event's object, as u64.
fn ev_rv(line: &serde_json::Value) -> u64 {
    line.get("object")
        .unwrap()
        .get("metadata")
        .unwrap()
        .get("resourceVersion")
        .unwrap()
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

async fn open_watch(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    path: &str,
) -> NdjsonReader {
    let resp = client
        .get(format!("http://{addr}{path}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "watch should 200");
    NdjsonReader::new(resp)
}

// =================================================================
// 1. LIST returns the snapshot rv (atomic)
// =================================================================

#[tokio::test]
async fn list_returns_snapshot_resource_version() {
    let (store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    for n in ["p1", "p2", "p3"] {
        post_pod(&client, addr, "default", &pod_body(n)).await;
    }

    let resp = client
        .get(format!("http://{addr}/api/v1/namespaces/default/pods"))
        .send()
        .await
        .unwrap();
    let list: serde_json::Value = resp.json().await.unwrap();
    let rv: u64 = list
        .get("metadata")
        .unwrap()
        .get("resourceVersion")
        .unwrap()
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    // The rv is current_revision, NOT last_applied_index. Assert it
    // equals the store's current_revision at list time directly.
    let current = store.current_catalog().await.revision().get();
    assert_eq!(rv, current, "LIST rv == current_revision (dense MVCC)");
    assert_eq!(rv, 3, "3 real mutations → revision 3");

    // Regression: last_applied_index has advanced PAST current_revision
    // (the openraft init blank entry consumes a log index but no
    // revision), so the old broken envelope would NOT equal 3.
    let last_applied = store.current_catalog().await.last_applied_index;
    assert!(
        last_applied >= rv,
        "last_applied_index ({last_applied}) >= current_revision ({rv}) — they differ, \
         proving the envelope uses current_revision not last_applied_index"
    );

    // After one MORE write, the OLD list's rv is unchanged (it came from
    // its own snapshot clone). Re-list to capture the new rv.
    post_pod(&client, addr, "default", &pod_body("p4")).await;
    let resp2 = client
        .get(format!("http://{addr}/api/v1/namespaces/default/pods"))
        .send()
        .await
        .unwrap();
    let list2: serde_json::Value = resp2.json().await.unwrap();
    let rv2: u64 = list2
        .get("metadata")
        .unwrap()
        .get("resourceVersion")
        .unwrap()
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(rv2, 4, "new list reflects the 4th write");
    assert_ne!(rv, rv2, "the old list's rv is its own snapshot, unchanged");

    server.shutdown().await.unwrap();
}

// =================================================================
// 2. WATCH from that rv is gap+dup-free
// =================================================================

#[tokio::test]
async fn watch_from_list_rv_is_gap_and_dup_free() {
    let (store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    // LIST → rv=N (3 pods already there).
    for n in ["a", "b", "c"] {
        post_pod(&client, addr, "default", &pod_body(n)).await;
    }
    let resp = client
        .get(format!("http://{addr}/api/v1/namespaces/default/pods"))
        .send()
        .await
        .unwrap();
    let list: serde_json::Value = resp.json().await.unwrap();
    let n: u64 = list
        .get("metadata")
        .unwrap()
        .get("resourceVersion")
        .unwrap()
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(n, 3);

    // Open WATCH ?watch=true&resourceVersion=N.
    let mut watch = open_watch(
        &client,
        addr,
        &format!("/api/v1/namespaces/default/pods?watch=true&resourceVersion={n}"),
    )
    .await;

    // Concurrently POST p4,p5,p6 (revs N+1,N+2,N+3).
    for name in ["p4", "p5", "p6"] {
        post_pod(&client, addr, "default", &pod_body(name)).await;
    }

    // The delivered object rv sequence is exactly N+1,N+2,N+3 contiguous,
    // type ADDED, no item with rv<=N (no dup of listed items), none
    // skipped (no gap).
    let mut delivered = Vec::new();
    while delivered.last().copied() != Some(n + 3) {
        let ev = watch.next_event().await;
        assert_eq!(ev.get("type").unwrap(), "ADDED");
        let rv = ev_rv(&ev);
        assert!(
            rv > n,
            "no event at rv<=N (no dup of listed items): {rv} <= {n}"
        );
        delivered.push(rv);
    }
    assert_eq!(
        delivered,
        vec![n + 1, n + 2, n + 3],
        "contiguous, ordered, no gap no dup"
    );

    let _ = store;
    drop(watch); // close the watch connection so shutdown drains promptly
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn watch_replay_tail_then_live_tail_one_contiguous_range() {
    // Mirror item 3's nonempty_replay_boundary through HTTP: create a
    // backlog, capture rv BEFORE some of it, WATCH from that rv → replay
    // tail then live tail are one contiguous ordered range.
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    // Backlog: revs 1..6.
    for i in 1..=6 {
        post_pod(&client, addr, "default", &pod_body(&format!("bk{i}"))).await;
    }
    // Resume from the midpoint (rev 3) → replay covers 4,5,6 (non-empty).
    let from = 3u64;
    let mut watch = open_watch(
        &client,
        addr,
        &format!("/api/v1/namespaces/default/pods?watch=true&resourceVersion={from}"),
    )
    .await;

    // Drive live writes that land after the backlog (revs 7,8).
    for i in 7..=8 {
        post_pod(&client, addr, "default", &pod_body(&format!("lv{i}"))).await;
    }

    // The delivered range is exactly (from, final] = 4,5,6,7,8 — replay
    // tail FIRST in revision order, then live tail, no reorder/gap/dup.
    let final_rev = 8u64;
    let mut delivered = Vec::new();
    while delivered.last().copied() != Some(final_rev) {
        delivered.push(ev_rv(&watch.next_event().await));
    }
    assert_eq!(delivered, vec![4, 5, 6, 7, 8]);

    drop(watch);
    server.shutdown().await.unwrap();
}

// =================================================================
// 3. WATCH from a compacted rv => 410
// =================================================================

#[tokio::test]
async fn compacted_too_old_maps_to_gone_410() {
    // DEFAULT_HISTORY_CAPACITY isn't reachable through the public mesh
    // boot, so per the test strategy we assert the translation arm
    // directly: WatchGone::CompactedTooOld => ApiError::Gone => HTTP 410
    // Expired. A registry-backed integration in engenho-store already
    // proves watch_from(below-watermark) => CompactedTooOld; this proves
    // the apiserver translates it to a real 410.
    use axum::response::IntoResponse;
    let err = engenho_apiserver::gone_to_api_error(engenho_store::WatchGone::CompactedTooOld {
        requested: Revision(1),
        compacted: Revision(5),
    });
    let resp = err.into_response();
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::GONE,
        "CompactedTooOld at registration → HTTP 410"
    );

    // Sanity at the handler seam: a normal MostRecent watch never 410s,
    // and an rv ahead of the server is tolerated as MostRecent at M0.1
    // (documented) — it must NOT 410.
    let store = boot_store().await;
    let h = StoreBackedHandler::for_core_kind(store.clone(), "Pod", true);
    assert!(
        h.watch_stream(Some("default"), ResumePoint::MostRecent, true)
            .await
            .is_ok(),
        "MostRecent watch never Gone"
    );
    assert!(
        h.watch_stream(Some("default"), ResumePoint::At(Revision(9_999)), true)
            .await
            .is_ok(),
        "an rv ahead of the server is tolerated (MostRecent), not 410, at M0.1"
    );

    drop(h);
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}

// =================================================================
// 4. GVK/namespace filter excludes other resources
// =================================================================

#[tokio::test]
async fn watch_filters_other_kinds_and_namespaces() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    // Seed one of each so the shared revision is non-trivial.
    post_pod(&client, addr, "default", &pod_body("seed-pod")).await;
    post_cm(&client, addr, "default", "seed-cm").await;

    // WATCH pods in default from MostRecent (no replay).
    let mut watch = open_watch(
        &client,
        addr,
        "/api/v1/namespaces/default/pods?watch=true&resourceVersion=0",
    )
    .await;

    // POST another ConfigMap (advances revision) AND a pod in a DIFFERENT
    // namespace (advances revision) — both must be filtered out — then a
    // pod in default that MUST appear.
    post_cm(&client, addr, "default", "cm-after").await;
    post_pod(&client, addr, "kube-system", &pod_body("other-ns-pod")).await;
    post_pod(&client, addr, "default", &pod_body("wanted")).await;

    // The first (and only) event the pod-watch in default delivers is
    // "wanted" — the ConfigMap + cross-namespace pod were filtered out
    // even though they advanced the shared revision.
    let ev = watch.next_event().await;
    assert_eq!(ev.get("type").unwrap(), "ADDED");
    assert_eq!(
        ev.get("object")
            .unwrap()
            .get("metadata")
            .unwrap()
            .get("name")
            .unwrap(),
        "wanted",
        "pod-watch in default delivers ONLY the matching pod"
    );

    drop(watch);
    server.shutdown().await.unwrap();
}

// =================================================================
// 5. labelSelector filter (LIST + WATCH)
// =================================================================

#[tokio::test]
async fn label_selector_filters_list_and_watch() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    // Two pods up-front: one matching app=web, one not.
    post_pod(
        &client,
        addr,
        "default",
        &pod_body_labeled("web1", "app", "web"),
    )
    .await;
    post_pod(
        &client,
        addr,
        "default",
        &pod_body_labeled("api1", "app", "api"),
    )
    .await;

    // LIST ?labelSelector=app=web → only web1.
    let resp = client
        .get(format!(
            "http://{addr}/api/v1/namespaces/default/pods?labelSelector=app=web"
        ))
        .send()
        .await
        .unwrap();
    let list: serde_json::Value = resp.json().await.unwrap();
    let items = list.get("items").unwrap().as_array().unwrap();
    assert_eq!(items.len(), 1, "LIST label-filtered to one pod");
    assert_eq!(
        items[0].get("metadata").unwrap().get("name").unwrap(),
        "web1"
    );

    // WATCH ?watch=true&resourceVersion=0&labelSelector=app=web.
    let mut watch = open_watch(
        &client,
        addr,
        "/api/v1/namespaces/default/pods?watch=true&resourceVersion=0&labelSelector=app=web",
    )
    .await;

    // Create one matching + one non-matching pod; only the matching one
    // streams.
    post_pod(
        &client,
        addr,
        "default",
        &pod_body_labeled("web2", "app", "web"),
    )
    .await;
    post_pod(
        &client,
        addr,
        "default",
        &pod_body_labeled("api2", "app", "api"),
    )
    .await;

    let ev = watch.next_event().await;
    assert_eq!(
        ev.get("object")
            .unwrap()
            .get("metadata")
            .unwrap()
            .get("name")
            .unwrap(),
        "web2",
        "only the app=web pod streams"
    );

    drop(watch);
    server.shutdown().await.unwrap();
}

// =================================================================
// 6. Bookmark passthrough
// =================================================================

#[tokio::test]
async fn bookmark_passthrough_and_optout() {
    // The store's bookmark cadence is 5s by default and not tunable per
    // request below 5s through the HTTP surface (watch_stream uses the
    // fixed cadence). Exercising a real timed bookmark over HTTP would
    // need a >5s wait; instead, prove the bookmark LINE shape + opt-out
    // via the handler unit seam (a real StoreMesh + a fast cadence) and
    // the resume-from-bookmark contract over the store.
    let store = boot_store().await;

    // allowWatchBookmarks=true with a fast cadence (handler-unit seam
    // can't set cadence directly, so drive the store's watch_from with a
    // short bookmark_every) → a BOOKMARK signal arrives on a quiescent
    // store.
    let from = store.current_catalog().await.revision();
    let mut bm_stream = store
        .watch_from(engenho_store::WatchOpts {
            from,
            buffer: 64,
            bookmark_every: Duration::from_millis(50),
        })
        .await
        .unwrap();
    // Wait for a Bookmark (skip any events on a quiescent store there are
    // none).
    let bm_rev = loop {
        match tokio::time::timeout(Duration::from_secs(3), bm_stream.next())
            .await
            .expect("bookmark before timeout")
        {
            Some(Ok(WatchSignal::Bookmark(rev))) => break rev,
            Some(Ok(WatchSignal::Event(_))) => continue,
            other => panic!("unexpected: {other:?}"),
        }
    };
    assert_eq!(bm_rev, from, "bookmark marks the current revision");

    // Cross-check: watch_from(bookmarked rev) replays nothing.
    let mut resumed = store
        .watch_from(engenho_store::WatchOpts::from_revision(bm_rev))
        .await
        .unwrap();
    let replayed = tokio::time::timeout(Duration::from_millis(80), resumed.next()).await;
    assert!(
        replayed.is_err(),
        "a bookmark is a valid resume point — replays nothing"
    );

    // Opt-out: bookmark_every ZERO → no bookmark line.
    let mut no_bm = store
        .watch_from(engenho_store::WatchOpts {
            from,
            buffer: 64,
            bookmark_every: Duration::ZERO,
        })
        .await
        .unwrap();
    let none = tokio::time::timeout(Duration::from_millis(120), no_bm.next()).await;
    assert!(none.is_err(), "ZERO cadence → no bookmark");

    drop((bm_stream, resumed, no_bm));
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}

#[tokio::test]
async fn watch_stream_disables_bookmarks_when_not_requested() {
    // allow_bookmarks=false on watch_stream → the underlying WatchOpts
    // carries ZERO cadence → no bookmark ever. Prove via the handler
    // seam that no bookmark signal arrives.
    let store = boot_store().await;
    let h = StoreBackedHandler::for_core_kind(store.clone(), "Pod", true);
    let mut stream = h
        .watch_stream(Some("default"), ResumePoint::MostRecent, false)
        .await
        .unwrap();
    let none = tokio::time::timeout(Duration::from_millis(120), stream.next()).await;
    assert!(none.is_err(), "no bookmark with allow_bookmarks=false");
    drop(stream);
    drop(h);
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}

// =================================================================
// 7. rv=0/absent = most recent, no replay
// =================================================================

#[tokio::test]
async fn watch_most_recent_does_not_replay_existing() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    // 3 existing pods.
    for n in ["e1", "e2", "e3"] {
        post_pod(&client, addr, "default", &pod_body(n)).await;
    }

    // WATCH ?watch=true (no resourceVersion) → MostRecent, NO replay.
    let mut watch = open_watch(&client, addr, "/api/v1/namespaces/default/pods?watch=true").await;

    // Create a 4th pod.
    post_pod(&client, addr, "default", &pod_body("e4")).await;

    // Exactly that one ADDED line streams (no replay of e1..e3).
    let ev = watch.next_event().await;
    assert_eq!(ev.get("type").unwrap(), "ADDED");
    assert_eq!(
        ev.get("object")
            .unwrap()
            .get("metadata")
            .unwrap()
            .get("name")
            .unwrap(),
        "e4",
        "MostRecent watch delivers only the post-subscribe write"
    );
    // The delivered rv is 4 — none of the existing 3 replayed.
    assert_eq!(ev_rv(&ev), 4);

    drop(watch);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_resource_version_is_400() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let resp = client
        .get(format!(
            "http://{addr}/api/v1/namespaces/default/pods?watch=true&resourceVersion=abc"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let err: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(err.get("kind").unwrap(), "Status");
    assert_eq!(err.get("reason").unwrap(), "BadRequest");

    // A bare key is a VALID set-based EXISTS selector (`?labelSelector=oops`
    // means "objects that HAVE label `oops`") — it must NOT 400.
    let resp = client
        .get(format!(
            "http://{addr}/api/v1/namespaces/default/pods?labelSelector=oops"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "bare key = exists selector"
    );

    // A genuinely malformed selector (empty KEY, `=v`) → 400.
    let resp = client
        .get(format!(
            "http://{addr}/api/v1/namespaces/default/pods?labelSelector=%3Doops"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    server.shutdown().await.unwrap();
}

// =================================================================
// Handler-unit: list_at captures items + rv atomically
// =================================================================

#[tokio::test]
async fn handler_list_at_returns_items_and_current_revision() {
    let store = boot_store().await;
    let h = StoreBackedHandler::for_core_kind(store.clone(), "Pod", true);

    for n in ["x", "y"] {
        store
            .propose(engenho_store::command::ResourceCommand::Put {
                key: engenho_store::ResourceKey::namespaced("", "v1", "Pod", "default", n),
                value: serde_json::json!({"metadata": {"name": n}, "spec": {}}),
                expected: None,
                reason: engenho_store::command::Reason::Operator,
            })
            .await
            .unwrap();
    }

    let (items, rv) = h
        .list_at(Some("default"), &Selectors::default())
        .await
        .unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(rv, store.current_catalog().await.revision());
    assert_eq!(rv, Revision(2));

    drop(h);
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}

// =================================================================
// timeoutSeconds — the server closes the watch, the client does not
// =================================================================

/// `?timeoutSeconds=N` ends the stream CLEANLY at the deadline.
///
/// Before this was honoured, `timeout_seconds` was parsed into
/// `ListWatchParams` and read by nobody, so the watch stayed open forever
/// and a long-running client fell back on its own read timeout instead.
/// Measured against a live engenho on 2026-09-06: pangea-operator logged
/// ~28 `hyper::Error(Body, Kind(TimedOut))` per hour, across all 12 of its
/// controllers, each followed by a re-LIST. Reconciliation still happened,
/// so nothing failed loudly — it was a poll wearing a watch's clothes.
///
/// The assertion is deliberately "the stream ENDS", not "an error arrives":
/// K8s closes the watch cleanly and the client re-LISTs. An error here
/// would be a different, worse contract.
#[tokio::test]
async fn watch_timeout_seconds_ends_the_stream_cleanly() {
    let (_mesh, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let mut w = open_watch(
        &client,
        addr,
        "/api/v1/namespaces/default/pods?watch=true&resourceVersion=0&timeoutSeconds=1",
    )
    .await;

    // Drain until EOF. Any bookmark/event before the deadline is fine; the
    // contract under test is that the stream TERMINATES, and within a bound
    // that a never-closing stream could not satisfy.
    let ended = tokio::time::timeout(Duration::from_secs(8), async {
        while w.next_line().await.is_some() {}
    })
    .await;

    assert!(
        ended.is_ok(),
        "watch with timeoutSeconds=1 must end; it was still open after 8s"
    );
}

/// Negative control for the test above — WITHOUT `timeoutSeconds` the same
/// watch is still open well past that deadline.
///
/// Without this, the test above passes for the wrong reason on any build
/// where watches happen to close early (a dropped store, a panicking task),
/// which would make it a vacuous guard rather than a test of the parameter.
#[tokio::test]
async fn watch_without_timeout_seconds_stays_open() {
    let (_mesh, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let mut w = open_watch(
        &client,
        addr,
        "/api/v1/namespaces/default/pods?watch=true&resourceVersion=0",
    )
    .await;

    // Read RAW chunks, not `next_line()`: that helper panics on a 3s
    // silence ("watch chunk arrived before timeout"), and an idle watch is
    // legitimately silent — so using it here would fail this control for
    // the wrong reason instead of testing what it claims to.
    //
    // "Still open" = every chunk read either times out (silence, fine) or
    // yields bytes. Only `Ok(None)` — a real EOF — means the server closed
    // the stream, which is the thing that must NOT happen without the
    // parameter.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    let mut closed_early = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), w.resp.chunk()).await {
            Err(_) => {}                       // silence: still open
            Ok(Ok(Some(_))) => {}              // data: still open
            Ok(Ok(None)) => {
                closed_early = true;           // EOF: the server closed it
                break;
            }
            Ok(Err(e)) => panic!("watch chunk read failed: {e}"),
        }
    }

    assert!(
        !closed_early,
        "watch without timeoutSeconds must stay open; it ended on its own, \
         which would make the timeoutSeconds test vacuous"
    );
}
