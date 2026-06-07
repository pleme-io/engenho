//! M0.1 item 5 — optimistic concurrency (CAS) + range pagination,
//! end-to-end over the real `ApiServer` (reqwest, in-proc `StoreMesh`).
//!
//! No network/FS beyond the loopback TCP listener; satisfies the
//! Environment-trait mockability spirit (the store is in-process).
//!
//!   CAS:
//!     A. PUT/PATCH with the CURRENT resourceVersion → 200 + new rv.
//!     B. Re-PUT with the STALE rv → 409 reason "Conflict"; object
//!        UNCHANGED.
//!     C. PUT/PATCH with NO resourceVersion → unconditional success.
//!     D. DELETE ?resourceVersion=<stale> → 409, object present;
//!        DELETE ?resourceVersion=<current> → OK, gone.
//!
//!   Pagination:
//!     E. limit=3 over 7 → 3 + continue + remainingItemCount==4; follow
//!        twice → 3 then 1, last has no continue; union == all 7.
//!     F. limit=100 over 7 → 7, no continue / remainingItemCount.
//!     G. continue=<garbage> → 410 reason "Expired".
//!     H. continue token round-trips through the wire (URL-encoded).

use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{ApiServer, ResourceHandler, StoreBackedHandler};
use engenho_store::{InProcessRouter, StoreMesh, default_config};

// ── boot helpers ──────────────────────────────────────────────────

async fn boot_store() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("apiserver-m01-cas-page").unwrap();
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
    let pod_handler: Arc<dyn ResourceHandler> =
        Arc::new(StoreBackedHandler::for_core_kind(store.clone(), "Pod", true));
    let server = ApiServer::start("127.0.0.1:0".parse().unwrap(), vec![pod_handler], None)
        .await
        .unwrap();
    (store, server)
}

fn pod_body(name: &str) -> serde_json::Value {
    serde_json::json!({ "metadata": { "name": name }, "spec": {} })
}

fn rv_of(obj: &serde_json::Value) -> String {
    obj.get("metadata")
        .unwrap()
        .get("resourceVersion")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string()
}

async fn post_pod(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    ns: &str,
    body: &serde_json::Value,
) -> serde_json::Value {
    let resp = client
        .post(format!("http://{addr}/api/v1/namespaces/{ns}/pods"))
        .json(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED, "POST pod failed");
    resp.json().await.unwrap()
}

async fn get_pod(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    ns: &str,
    name: &str,
) -> reqwest::Response {
    client
        .get(format!("http://{addr}/api/v1/namespaces/{ns}/pods/{name}"))
        .send()
        .await
        .unwrap()
}

// =================================================================
// CAS — A + B + C
// =================================================================

#[tokio::test]
async fn cas_put_current_rv_succeeds_stale_conflicts_absent_unconditional() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    // Create pod; capture rv N.
    let created = post_pod(&client, addr, "default", &pod_body("cas-pod")).await;
    let n = rv_of(&created);

    // A. PUT (via PATCH route, since we have no PUT route; the K8s PUT
    // semantics for update map onto our merge Patch with the inbound rv).
    // PATCH with metadata.resourceVersion = N → 200 + new rv > N.
    let patch_ok = serde_json::json!({
        "metadata": { "resourceVersion": n },
        "spec": { "image": "v2" }
    });
    let resp = client
        .patch(format!("http://{addr}/api/v1/namespaces/default/pods/cas-pod"))
        .json(&patch_ok)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "current-rv PATCH succeeds");
    let updated: serde_json::Value = resp.json().await.unwrap();
    let n2 = rv_of(&updated);
    assert_ne!(n2, n, "rv advanced");
    assert!(n2.parse::<u64>().unwrap() > n.parse::<u64>().unwrap());

    // B. Re-PATCH with the STALE N → 409, reason "Conflict"; object
    // UNCHANGED (still at n2).
    let patch_stale = serde_json::json!({
        "metadata": { "resourceVersion": n },
        "spec": { "image": "v3" }
    });
    let resp = client
        .patch(format!("http://{addr}/api/v1/namespaces/default/pods/cas-pod"))
        .json(&patch_stale)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT, "stale rv → 409");
    let status: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(status.get("kind").unwrap(), "Status");
    assert_eq!(
        status.get("reason").unwrap(),
        "Conflict",
        "optimistic-concurrency reason is Conflict, NOT AlreadyExists"
    );
    // Object unchanged: still at n2, image still v2.
    let got: serde_json::Value = get_pod(&client, addr, "default", "cas-pod")
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(rv_of(&got), n2, "object unchanged after conflict");
    assert_eq!(got.get("spec").unwrap().get("image").unwrap(), "v2");

    // C. PATCH with NO resourceVersion → unconditional success.
    let patch_uncond = serde_json::json!({ "spec": { "image": "v4" } });
    let resp = client
        .patch(format!("http://{addr}/api/v1/namespaces/default/pods/cas-pod"))
        .json(&patch_uncond)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "no-rv PATCH is unconditional");
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v.get("spec").unwrap().get("image").unwrap(), "v4");

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn cas_conflict_reason_distinct_from_already_exists() {
    // The create-already-exists path uses reason "AlreadyExists"; the
    // optimistic-concurrency path uses reason "Conflict". Both are 409 —
    // assert the two distinct reasons over the wire.
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let created = post_pod(&client, addr, "default", &pod_body("dup")).await;
    let _ = &created;

    // POST again (no rv) → AlreadyExists 409.
    let resp = client
        .post(format!("http://{addr}/api/v1/namespaces/default/pods"))
        .json(&pod_body("dup"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    let s: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(s.get("reason").unwrap(), "AlreadyExists");

    // PATCH with a stale rv → Conflict 409.
    let resp = client
        .patch(format!("http://{addr}/api/v1/namespaces/default/pods/dup"))
        .json(&serde_json::json!({"metadata": {"resourceVersion": "999"}, "spec": {"x": 1}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    let s: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(s.get("reason").unwrap(), "Conflict");

    server.shutdown().await.unwrap();
}

// =================================================================
// CAS — D (delete precondition)
// =================================================================

#[tokio::test]
async fn cas_delete_precondition() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let created = post_pod(&client, addr, "default", &pod_body("del-pod")).await;
    let n = rv_of(&created);

    // D1. DELETE ?resourceVersion=<stale> → 409 Conflict, object present.
    let resp = client
        .delete(format!(
            "http://{addr}/api/v1/namespaces/default/pods/del-pod?resourceVersion=999"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT, "stale delete precondition → 409");
    let s: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(s.get("reason").unwrap(), "Conflict");
    assert_eq!(
        get_pod(&client, addr, "default", "del-pod").await.status(),
        reqwest::StatusCode::OK,
        "object still present after failed precondition delete"
    );

    // D2. DELETE ?resourceVersion=<current> → OK, gone.
    let resp = client
        .delete(format!(
            "http://{addr}/api/v1/namespaces/default/pods/del-pod?resourceVersion={n}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "matching precondition delete → OK");
    assert_eq!(
        get_pod(&client, addr, "default", "del-pod").await.status(),
        reqwest::StatusCode::NOT_FOUND,
        "object gone after successful delete"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn unconditional_delete_still_works() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    post_pod(&client, addr, "default", &pod_body("plain")).await;
    let resp = client
        .delete(format!("http://{addr}/api/v1/namespaces/default/pods/plain"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        get_pod(&client, addr, "default", "plain").await.status(),
        reqwest::StatusCode::NOT_FOUND
    );

    server.shutdown().await.unwrap();
}

// =================================================================
// Pagination — E + F + G + H
// =================================================================

fn list_at(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    query: &str,
) -> reqwest::RequestBuilder {
    client.get(format!("http://{addr}/api/v1/namespaces/default/pods?{query}"))
}

fn item_names(list: &serde_json::Value) -> Vec<String> {
    list.get("items")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|i| {
            i.get("metadata")
                .unwrap()
                .get("name")
                .unwrap()
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect()
}

fn continue_of(list: &serde_json::Value) -> Option<String> {
    list.get("metadata")
        .unwrap()
        .get("continue")
        .and_then(|c| c.as_str())
        .map(str::to_string)
}

fn remaining_of(list: &serde_json::Value) -> Option<i64> {
    list.get("metadata")
        .unwrap()
        .get("remainingItemCount")
        .and_then(serde_json::Value::as_i64)
}

#[tokio::test]
async fn pagination_limit_continue_series_covers_all_no_dup() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    // 7 pods (zero-padded so name order is total order).
    for i in 0..7 {
        post_pod(&client, addr, "default", &pod_body(&format!("pp{i}"))).await;
    }

    // E. First page limit=3 → 3 items, continue set, remainingItemCount==4.
    let p1: serde_json::Value = list_at(&client, addr, "limit=3")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let names1 = item_names(&p1);
    assert_eq!(names1.len(), 3, "first page has 3");
    assert_eq!(remaining_of(&p1), Some(4), "remainingItemCount == 4");
    let cont1 = continue_of(&p1).expect("continue set on first page");

    // H. The continue token survives URL-encoding round-trip.
    let p2: serde_json::Value = list_at(&client, addr, &format!("limit=3&continue={cont1}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let names2 = item_names(&p2);
    assert_eq!(names2.len(), 3, "second page has 3");
    let cont2 = continue_of(&p2).expect("continue set on second page");

    // Final page → 1 item, no continue.
    let p3: serde_json::Value = list_at(&client, addr, &format!("limit=3&continue={cont2}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let names3 = item_names(&p3);
    assert_eq!(names3.len(), 1, "last page has 1");
    assert!(continue_of(&p3).is_none(), "no continue on last page");
    assert!(remaining_of(&p3).is_none(), "no remainingItemCount on last page");

    // Union == all 7, no dup, in order.
    let mut union: Vec<String> = Vec::new();
    union.extend(names1);
    union.extend(names2);
    union.extend(names3);
    let expected: Vec<String> = (0..7).map(|i| format!("pp{i}")).collect();
    assert_eq!(union, expected, "series covers all 7 in order, no dup, no gap");

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn pagination_exact_multiple_no_trailing_empty_page() {
    // 6 pods, limit=3 → exactly two full pages; the second page must NOT
    // carry a continue (no spurious empty third page over the wire).
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    for i in 0..6 {
        post_pod(&client, addr, "default", &pod_body(&format!("m{i}"))).await;
    }
    let p1: serde_json::Value = list_at(&client, addr, "limit=3")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(item_names(&p1).len(), 3);
    let cont = continue_of(&p1).expect("more after first page");

    let p2: serde_json::Value = list_at(&client, addr, &format!("limit=3&continue={cont}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(item_names(&p2).len(), 3, "second page is full");
    assert!(continue_of(&p2).is_none(), "no continue on the exact-fill final page");

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn pagination_limit_exceeds_set_no_continue() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    for i in 0..7 {
        post_pod(&client, addr, "default", &pod_body(&format!("q{i}"))).await;
    }
    // F. limit=100 → all 7, no continue / remainingItemCount.
    let list: serde_json::Value = list_at(&client, addr, "limit=100")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(item_names(&list).len(), 7);
    assert!(continue_of(&list).is_none(), "no continue when limit > set");
    assert!(remaining_of(&list).is_none(), "no remainingItemCount when limit > set");

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn pagination_with_label_selector_over_fetches_and_pages_matches_only() {
    // The load-bearing selector-vs-limit interaction: `limit` counts
    // items AFTER selector filtering. Interleave matching + non-matching
    // pods so the handler MUST over-fetch store pages to fill a page.
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    // 10 pods: even-indexed match app=web, odd-indexed app=api.
    for i in 0..10 {
        let app = if i % 2 == 0 { "web" } else { "api" };
        let body = serde_json::json!({
            "metadata": { "name": format!("s{i:02}"), "labels": { "app": app } },
            "spec": {}
        });
        post_pod(&client, addr, "default", &body).await;
    }
    // 5 web pods total: s00,s02,s04,s06,s08.

    // Page the web pods at limit=2 → must collect 2 MATCHES per page even
    // though every other store item is rejected.
    let mut collected: Vec<String> = Vec::new();
    let mut query = "limit=2&labelSelector=app=web".to_string();
    loop {
        let page: serde_json::Value = list_at(&client, addr, &query)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let names = item_names(&page);
        // Every returned item must match the selector.
        for n in &names {
            let idx: usize = n.trim_start_matches('s').parse().unwrap();
            assert_eq!(idx % 2, 0, "only app=web pods are returned: {n}");
        }
        collected.extend(names);
        match continue_of(&page) {
            Some(c) => query = format!("limit=2&labelSelector=app=web&continue={c}"),
            None => break,
        }
        assert!(collected.len() <= 5, "no overrun");
    }
    assert_eq!(
        collected,
        vec!["s00", "s02", "s04", "s06", "s08"],
        "all 5 web pods paged, in order, no dup, no gap"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn pagination_garbage_continue_is_410_expired() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    post_pod(&client, addr, "default", &pod_body("only")).await;

    // G. continue=<garbage> → 410 Gone, reason "Expired".
    let resp = list_at(&client, addr, "limit=3&continue=deadbeefgarbage")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::GONE, "garbage continue → 410");
    let s: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(s.get("kind").unwrap(), "Status");
    assert_eq!(s.get("reason").unwrap(), "Expired");

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn pagination_invalid_limit_is_400() {
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    let resp = list_at(&client, addr, "limit=notanumber")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST, "malformed limit → 400");

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn pagination_continue_excludes_pre_cursor_late_insert() {
    // Start a limit=3 series; insert a pod that sorts BEFORE the cursor
    // between pages; the continued pages (resuming strictly after the
    // cursor) must NOT surface it. This proves CURSOR exclusion
    // (`Bound::Excluded(last_key)`), NOT MVCC snapshot isolation: it
    // holds for any cursor scheme and does not depend on revision pinning.
    //
    // NOTE: M0.1 pagination does NOT isolate against a POST-cursor
    // mid-pagination insert — a pod inserted AFTER the cursor between
    // pages WOULD surface on a later page (the LIST reads the live store
    // each call; the envelope `resourceVersion` is a label, not a
    // read-isolation revision). Destination = revision-indexed historical
    // reads (deferred). See engenho-store::pagination module docs.
    let (_store, server) = boot_store_and_server().await;
    let addr = server.local_addr();
    let client = reqwest::Client::new();

    // pods r1..r5.
    for i in 1..=5 {
        post_pod(&client, addr, "default", &pod_body(&format!("r{i}"))).await;
    }
    let p1: serde_json::Value = list_at(&client, addr, "limit=3")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(item_names(&p1), vec!["r1", "r2", "r3"]);
    let cont = continue_of(&p1).unwrap();

    // Insert "r0" (sorts before the cursor r3).
    post_pod(&client, addr, "default", &pod_body("r0")).await;

    // Resume from the cursor → r4, r5; r0 (pre-cursor) does NOT appear.
    let p2: serde_json::Value = list_at(&client, addr, &format!("limit=3&continue={cont}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let names2 = item_names(&p2);
    assert!(!names2.contains(&"r0".to_string()), "pre-cursor late insert not paged in series");
    assert_eq!(names2, vec!["r4", "r5"], "continued range is contiguous after cursor");

    server.shutdown().await.unwrap();
}
