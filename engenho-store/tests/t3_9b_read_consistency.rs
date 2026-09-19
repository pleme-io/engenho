//! T3.9b, mesh level — the reads a LIST or GET makes through `StoreMesh`
//! honour its `ReadConsistency`, on both backends.
//!
//! | behaviour | why it is not an implementation detail |
//! |---|---|
//! | a read naming a revision the replica has not reached waits for it, and is served once it arrives | a replica that lags (a follower, a store mid-replay) is upstream's cache behind etcd: kube-apiserver waits up to 3 s rather than failing a read that would succeed a moment later |
//! | a read naming a revision the replica does not reach within the wait is `TooLarge{requested, current}`, after the wait | upstream's 504; before T3.9b the store served its older present as if it were the newer revision the client had already seen |
//! | an exact read serves the catalog as it was at that revision, GET and LIST alike | `resourceVersionMatch=Exact` promises that past |
//! | after a restart, an exact read of history the store did not keep is `Expired`, the present still answers | the replay ring is never persisted; answering with the present would be a 200 carrying the wrong revision |
//! | an exact read of the past clones only what it returns | a past read runs under the lock `apply` takes; rewinding the whole keyspace to serve one scope is the clone class that wedged Flux on rio |
//!
//! The catalog-level model check (every revision, scope, page size and key
//! against the present captured at that revision) is
//! `src/catalog_tests/t3_9b_read_consistency.rs`.

mod support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use engenho_store::{
    InProcessRouter, ListScope, ReadConsistency, ReadRefused, Reason, ResourceCommand, ResourceKey,
    Revision, StoreMesh, default_config,
};
use serde_json::json;
use support::{bytes_allocated_by, durable_mesh, memory_mesh, put};

fn pod(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

fn pods() -> ListScope<'static> {
    ListScope::new("", "v1", "Pod", Some("default"))
}

fn marker(value: &serde_json::Value) -> &str {
    value["spec"]["marker"].as_str().unwrap_or("")
}

async fn delete(mesh: &StoreMesh, key: ResourceKey) {
    mesh.propose(ResourceCommand::Delete {
        key,
        expected: None,
        reason: Reason::Operator,
        deletion_timestamp: None,
    })
    .await
    .expect("propose delete");
}

/// Long enough that a missing wait cannot pass by luck, short enough that
/// the suite does not crawl.
const PATIENCE: Duration = Duration::from_secs(10);
const SHORT_WAIT: Duration = Duration::from_millis(300);

// ── waits for a revision on its way ──────────────────────────────────

/// A write lands while the read waits: the read is served, at a revision
/// at least the one it named, with the write in it.
async fn a_read_ahead_of_the_replica_waits_for_it(mesh: Arc<StoreMesh>) {
    put(&mesh, pod("before"), json!({ "spec": { "marker": "b" } })).await;
    let head = mesh.current_revision().await;
    let named = Revision(head.get() + 1);

    let writer = {
        let mesh = Arc::clone(&mesh);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            put(&mesh, pod("arrives"), json!({ "spec": { "marker": "a" } })).await;
        })
    };
    let page = mesh
        .list_page_consistent(
            pods(),
            None,
            0,
            ReadConsistency::NotOlderThan(named),
            PATIENCE,
        )
        .await
        .expect("the named revision arrives within the wait, so the read is served");
    assert!(
        page.revision >= named,
        "served at {} for a read not older than {named}",
        page.revision
    );
    assert!(
        page.items.iter().any(|(k, _)| *k == pod("arrives")),
        "the write the read waited for is in it"
    );
    writer.await.expect("writer");

    // The same for one key, waiting on a revision still to come.
    let named = Revision(mesh.current_revision().await.get() + 1);
    let writer = {
        let mesh = Arc::clone(&mesh);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            put(&mesh, pod("before"), json!({ "spec": { "marker": "b2" } })).await;
        })
    };
    let got = mesh
        .get_consistent(
            &pod("before"),
            ReadConsistency::NotOlderThan(named),
            PATIENCE,
        )
        .await
        .expect("served once the revision arrives")
        .expect("the key exists");
    assert_eq!(
        marker(&got),
        "b2",
        "a GET not older than {named} sees the write that made {named}"
    );
    writer.await.expect("writer");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_ahead_of_a_memory_replica_waits_for_it() {
    a_read_ahead_of_the_replica_waits_for_it(memory_mesh("t3-9b-wait-memory").await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_ahead_of_a_durable_replica_waits_for_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    a_read_ahead_of_the_replica_waits_for_it(durable_mesh(dir.path(), "t3-9b-wait-durable").await)
        .await;
}

// ── refuses a revision that never comes ──────────────────────────────

async fn a_revision_that_never_comes_is_too_large_after_the_wait(mesh: &StoreMesh) {
    put(mesh, pod("only"), json!({})).await;
    let head = mesh.current_revision().await;
    let ahead = Revision(head.get() + 5);
    let too_large = ReadRefused::TooLarge {
        requested: ahead,
        current: head,
    };

    for consistency in [
        ReadConsistency::NotOlderThan(ahead),
        ReadConsistency::Exact(ahead),
    ] {
        let started = Instant::now();
        assert_eq!(
            mesh.list_page_consistent(pods(), None, 0, consistency, SHORT_WAIT)
                .await
                .err(),
            Some(too_large),
            "LIST {consistency:?} over a store at {head}"
        );
        assert!(
            started.elapsed() >= SHORT_WAIT,
            "the refusal came after {:?}, before the {SHORT_WAIT:?} the read was given",
            started.elapsed()
        );
        assert_eq!(
            mesh.get_consistent(&pod("only"), consistency, SHORT_WAIT)
                .await
                .err(),
            Some(too_large),
            "GET {consistency:?} over a store at {head}"
        );
    }

    // What the store has reached is still served, without waiting.
    let started = Instant::now();
    let page = mesh
        .list_page_consistent(
            pods(),
            None,
            0,
            ReadConsistency::NotOlderThan(head),
            PATIENCE,
        )
        .await
        .expect("the store is at the named revision");
    assert_eq!(page.revision, head);
    assert!(
        started.elapsed() < PATIENCE,
        "a read the store can serve does not sit out the wait"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_memory_mesh_refuses_a_revision_it_never_reaches() {
    let mesh = memory_mesh("t3-9b-ahead-memory").await;
    a_revision_that_never_comes_is_too_large_after_the_wait(&mesh).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_durable_mesh_refuses_a_revision_it_never_reaches() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mesh = durable_mesh(dir.path(), "t3-9b-ahead-durable").await;
    a_revision_that_never_comes_is_too_large_after_the_wait(&mesh).await;
}

// ── serves the past it names ─────────────────────────────────────────

async fn an_exact_read_serves_the_past(mesh: &StoreMesh) {
    put(
        mesh,
        pod("modified"),
        json!({ "spec": { "marker": "then" } }),
    )
    .await;
    put(mesh, pod("deleted"), json!({ "spec": { "marker": "was" } })).await;
    let then = mesh.current_revision().await;

    put(
        mesh,
        pod("modified"),
        json!({ "spec": { "marker": "now" } }),
    )
    .await;
    delete(mesh, pod("deleted")).await;
    put(mesh, pod("created"), json!({ "spec": { "marker": "new" } })).await;

    let page = mesh
        .list_page_consistent(pods(), None, 0, ReadConsistency::Exact(then), PATIENCE)
        .await
        .expect("the revision is retained");
    assert_eq!(page.revision, then);
    let seen: Vec<(&str, &str)> = page
        .items
        .iter()
        .map(|(k, v)| (k.name.as_str(), marker(v)))
        .collect();
    assert_eq!(
        seen,
        [("deleted", "was"), ("modified", "then")],
        "the list as it was at {then}"
    );

    assert_eq!(
        mesh.get_consistent(&pod("modified"), ReadConsistency::Exact(then), PATIENCE)
            .await
            .expect("served")
            .map(|v| marker(&v).to_owned()),
        Some("then".to_owned())
    );
    assert_eq!(
        mesh.get_consistent(&pod("created"), ReadConsistency::Exact(then), PATIENCE)
            .await,
        Ok(None),
        "a key created after {then} did not exist at it"
    );

    let now = mesh
        .list_page_consistent(pods(), None, 0, ReadConsistency::Latest, PATIENCE)
        .await
        .expect("a latest read is never refused");
    assert_eq!(now.revision, mesh.current_revision().await);
    let seen: Vec<(&str, &str)> = now
        .items
        .iter()
        .map(|(k, v)| (k.name.as_str(), marker(v)))
        .collect();
    assert_eq!(seen, [("created", "new"), ("modified", "now")]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_memory_mesh_serves_the_past_it_is_asked_for() {
    let mesh = memory_mesh("t3-9b-exact-memory").await;
    an_exact_read_serves_the_past(&mesh).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_durable_mesh_serves_the_past_it_is_asked_for() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mesh = durable_mesh(dir.path(), "t3-9b-exact-durable").await;
    an_exact_read_serves_the_past(&mesh).await;
}

// ── a restart keeps no history ───────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn after_a_restart_the_history_it_did_not_keep_is_expired() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let (mesh, fresh) = StoreMesh::start_or_resume(
            1,
            "in-process://1".into(),
            InProcessRouter::new(),
            default_config("t3-9b-restart-1").expect("config"),
            dir.path(),
        )
        .await
        .expect("start");
        assert!(fresh);
        assert!(mesh.wait_for_leadership(support::LEADERSHIP).await);
        for marker in ["one", "two", "three"] {
            put(&mesh, pod("p"), json!({ "spec": { "marker": marker } })).await;
        }
        mesh.terminate().await.expect("terminate");
    }

    let (mesh, fresh) = StoreMesh::start_or_resume(
        1,
        "in-process://1".into(),
        InProcessRouter::new(),
        default_config("t3-9b-restart-2").expect("config"),
        dir.path(),
    )
    .await
    .expect("resume");
    assert!(!fresh);
    assert!(mesh.wait_for_leadership(support::LEADERSHIP).await);

    let head = mesh.current_revision().await;
    let floor = mesh.compacted_revision().await;
    assert!(
        floor > Revision::ZERO,
        "precondition: the restarted store kept no history from revision 0 (floor {floor})"
    );
    let gone = Revision(floor.get() - 1);
    let expired = ReadRefused::Expired {
        requested: gone,
        compacted: floor,
    };
    assert_eq!(
        mesh.list_page_consistent(pods(), None, 0, ReadConsistency::Exact(gone), PATIENCE)
            .await
            .err(),
        Some(expired),
        "an exact LIST at {gone} after a restart that kept history only from {floor}"
    );
    assert_eq!(
        mesh.get_consistent(&pod("p"), ReadConsistency::Exact(gone), PATIENCE)
            .await
            .err(),
        Some(expired)
    );

    let present = mesh
        .list_page_consistent(pods(), None, 0, ReadConsistency::Exact(head), PATIENCE)
        .await
        .expect("the head is always readable exactly");
    assert_eq!(present.revision, head);
    assert_eq!(marker(&present.items[0].1), "three");
    assert_eq!(
        mesh.list_page_consistent(
            pods(),
            None,
            0,
            ReadConsistency::NotOlderThan(gone),
            PATIENCE,
        )
        .await
        .map(|p| p.revision),
        Ok(head),
        "the present is not older than a revision the store no longer holds"
    );
    mesh.terminate().await.expect("terminate");
}

// ── a past read clones only what it returns ──────────────────────────

/// One ConfigMap, then a hundred 16 KiB Secrets, then the ConfigMap again:
/// an exact read of the ConfigMap scope before its rewrite must cost the
/// ConfigMap, not the Secrets.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_exact_past_read_clones_only_what_it_returns() {
    let mesh = memory_mesh("t3-9b-alloc").await;
    let configmap = ResourceKey::namespaced("", "v1", "ConfigMap", "default", "one");
    put(&mesh, configmap.clone(), json!({ "data": { "k": "old" } })).await;
    let body = "A".repeat(16 * 1024);
    for i in 0..100 {
        put(
            &mesh,
            ResourceKey::namespaced("", "v1", "Secret", "default", format!("s{i}")),
            json!({ "data": { "payload": body } }),
        )
        .await;
    }
    let before_rewrite = mesh.current_revision().await;
    put(&mesh, configmap.clone(), json!({ "data": { "k": "new" } })).await;

    let (secrets, bulk_bytes) = bytes_allocated_by(mesh.list_page_consistent(
        ListScope::new("", "v1", "Secret", Some("default")),
        None,
        0,
        ReadConsistency::Latest,
        PATIENCE,
    ))
    .await;
    assert_eq!(secrets.expect("latest").items.len(), 100);
    assert!(
        bulk_bytes > 1024 * 1024,
        "positive control: reading the hundred Secrets allocated only {bulk_bytes} bytes — \
         the counting allocator or the seed is broken, so the bound below would prove nothing"
    );

    let (page, past_bytes) = bytes_allocated_by(mesh.list_page_consistent(
        ListScope::new("", "v1", "ConfigMap", Some("default")),
        None,
        0,
        ReadConsistency::Exact(before_rewrite),
        PATIENCE,
    ))
    .await;
    let page = page.expect("retained");
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].1["data"]["k"], "old");
    assert!(
        past_bytes < 64 * 1024,
        "an exact read of one ConfigMap allocated {past_bytes} bytes against {bulk_bytes} for \
         the Secrets: the past read is rewinding more than its own scope"
    );
}
