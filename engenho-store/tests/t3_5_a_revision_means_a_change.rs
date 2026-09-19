//! T3.5 — a revision means a change, end to end.
//!
//! The state machine's gate is pinned in `state::a_revision_means_a_change`
//! and in the `r9_mvcc_revision` generator. This file pins the two things
//! those cannot see:
//!
//! | behaviour | why it is not an implementation detail |
//! |---|---|
//! | a proposal through the mesh answers `Unchanged`, keeps the stored resourceVersion, and a watcher's next event is the next REAL change | a client's resourceVersion and a watcher's event stream are the contract; the gate is only how it is kept |
//! | an entry keeps its apply rules through the log's own encoding: a marked entry replays as V1, an unmarked one as V0 | replay runs whatever binary opens the log; a marker lost in the encoding would renumber every revision after it |

use std::time::Duration;

use engenho_store::command::{LoggedCommand, Reason, ResourceCommand, ResourceOp};
use engenho_store::{
    FjallStore, InProcessRouter, ResourceKey, Revision, StoreMesh, TypeConfig, WatchEventKind,
    WatchOpts, WatchSignal, default_config,
};
use openraft::storage::RaftStateMachine as _;
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
use serde_json::{Value, json};

const NODE: u64 = 1;
const ADDR: &str = "in-process://1";
const LEADERSHIP: Duration = Duration::from_secs(10);

fn cm() -> ResourceKey {
    ResourceKey::namespaced("", "v1", "ConfigMap", "default", "settings")
}

fn put(data: &Value) -> ResourceCommand {
    ResourceCommand::put(
        cm(),
        json!({"metadata": {"name": "settings", "namespace": "default"}, "data": data}),
        Reason::Controller,
    )
}

fn stored_rv(value: &Value) -> &str {
    value["metadata"]["resourceVersion"]
        .as_str()
        .expect("a stored object carries its resourceVersion")
}

/// Identical writes through `mesh` answer `Unchanged`, and the watcher's next
/// event is the change that follows them.
async fn identical_proposals_are_invisible(mesh: &StoreMesh) {
    let created = mesh.propose(put(&json!({"k": "v"}))).await.expect("create");
    assert_eq!(created.op, ResourceOp::Created);
    let at = created.revision;

    let mut watch = mesh
        .watch_from(WatchOpts::live_tail(Revision(at), 16))
        .await
        .expect("watch from the create");

    for (what, cmd) in [
        ("a verbatim re-Put", put(&json!({"k": "v"}))),
        (
            "a merge patch of the current value",
            ResourceCommand::patch(cm(), json!({"data": {"k": "v"}}), Reason::Controller),
        ),
    ] {
        let out = mesh.propose(cmd).await.expect(what);
        assert_eq!(out.op, ResourceOp::Unchanged, "{what}");
        assert_eq!(out.revision, at, "{what} consumed a revision");
    }
    let stored = mesh.get(&cm()).await.expect("still stored");
    assert_eq!(stored_rv(&stored), at.to_string(), "resourceVersion moved");

    let changed = mesh.propose(put(&json!({"k": "w"}))).await.expect("change");
    assert_eq!(changed.op, ResourceOp::Replaced);
    assert_eq!(
        changed.revision,
        at + 1,
        "the next real change takes the next revision"
    );

    let next = tokio::time::timeout(Duration::from_secs(5), watch.next())
        .await
        .expect("the change reaches the watcher")
        .expect("the stream is open")
        .expect("no terminal error");
    let WatchSignal::Event(event) = next else {
        panic!("expected an event, got {next:?}");
    };
    assert_eq!(event.kind, WatchEventKind::Modified);
    assert_eq!(
        event.resource_version,
        at + 1,
        "the watcher's first event after the identical writes is the real change"
    );
    assert_eq!(event.object["data"]["k"], "w");
}

#[tokio::test]
async fn an_identical_proposal_is_unchanged_and_wakes_no_watcher_memory() {
    let mesh = StoreMesh::start(
        NODE,
        ADDR.into(),
        InProcessRouter::new(),
        default_config("t35-memory").expect("config"),
    )
    .await
    .expect("start");
    mesh.initialize_singleton().await.expect("initialize");
    assert!(mesh.wait_for_leadership(LEADERSHIP).await, "leader");

    identical_proposals_are_invisible(&mesh).await;
    mesh.terminate().await.expect("terminate");
}

#[tokio::test]
async fn an_identical_proposal_is_unchanged_and_wakes_no_watcher_durable() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mesh = StoreMesh::start_durable(
        NODE,
        ADDR.into(),
        InProcessRouter::new(),
        default_config("t35-durable").expect("config"),
        &tmp.path().join("store"),
    )
    .await
    .expect("start");
    mesh.initialize_singleton().await.expect("initialize");
    assert!(mesh.wait_for_leadership(LEADERSHIP).await, "leader");

    identical_proposals_are_invisible(&mesh).await;
    mesh.terminate().await.expect("terminate");
}

/// `cmds` as the `log` partition stores them — one JSON document per
/// `Entry<TypeConfig>` — with `unmark` applied to each entry's payload.
fn encoded_log(cmds: &[ResourceCommand], unmark: bool) -> Vec<String> {
    cmds.iter()
        .zip(1u64..)
        .map(|(cmd, index)| {
            let entry: Entry<TypeConfig> = Entry {
                log_id: LogId {
                    leader_id: CommittedLeaderId::new(1, NODE),
                    index,
                },
                payload: EntryPayload::Normal(LoggedCommand::proposed(cmd.clone())),
            };
            let mut doc = serde_json::to_value(&entry).expect("encode entry");
            if unmark {
                // What a pre-T3.5 binary wrote: the command, and no marker.
                let payload = doc["payload"]["Normal"]
                    .as_object_mut()
                    .expect("a normal payload is the command's object");
                assert!(
                    payload.remove("semantics").is_some(),
                    "the marker is logged"
                );
            }
            doc.to_string()
        })
        .collect()
}

/// Decode `log` and apply it to a fresh durable store in one batch, as a boot
/// replay does. Returns each entry's (op, revision).
async fn replay(log: &[String]) -> Vec<(ResourceOp, u64)> {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut store = FjallStore::open(tmp.path().join("store")).expect("open");
    let entries: Vec<Entry<TypeConfig>> = log
        .iter()
        .map(|line| serde_json::from_str(line).expect("the log line decodes"))
        .collect();
    store
        .apply(entries)
        .await
        .expect("replay")
        .into_iter()
        .map(|r| (r.op, r.revision))
        .collect()
}

#[tokio::test]
async fn a_logged_entry_keeps_its_apply_rules_through_the_log_encoding() {
    let writes = [
        put(&json!({"k": "v"})),
        put(&json!({"k": "v"})),
        ResourceCommand::patch(cm(), json!({"data": {"k": "v"}}), Reason::Controller),
    ];

    assert_eq!(
        replay(&encoded_log(&writes, false)).await,
        [
            (ResourceOp::Created, 1),
            (ResourceOp::Unchanged, 1),
            (ResourceOp::Unchanged, 1),
        ],
        "an entry this binary logged replays under the rules it was proposed with"
    );
    assert_eq!(
        replay(&encoded_log(&writes, true)).await,
        [
            (ResourceOp::Created, 1),
            (ResourceOp::Replaced, 2),
            (ResourceOp::Patched, 3),
        ],
        "an entry logged before the marker replays exactly as it did then"
    );
}
