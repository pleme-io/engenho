//! T3.1 — the restart oracle: **a restart is a kill.**
//!
//! Nothing in engenho gets to assume it was stopped politely. A process can
//! vanish between any two instructions, and the next boot has to rebuild,
//! from what is on disk alone, a store that tells the truth about every write
//! it acknowledged. These cases pin that, one way of dying each.
//!
//! | case | how the process ends | what must hold on the next boot |
//! |---|---|---|
//! | 1 | dropped mid persist-window | every ack present, the revision unchanged, and no watch resumes into a strict subset of what happened |
//! | 2 | dropped after a snapshot + purge past a stale catalog blob | the node boots at all, with every ack |
//! | 3 | SIGKILL of a child process mid-propose | every ack present at the revision it was acknowledged at |
//! | 4 | a clean stop (`StoreMesh::terminate`) | nothing is left to replay, the revision is continuous |
//! | 4b | killed right after `StoreMesh::flush`, with no terminate | the same as case 4: the flush alone is the clean stop's durability |
//! | 5 | — (replay compatibility) | a log recorded by a released binary replays through this tree to the same revisions and catalog bytes |
//!
//! ## How a "kill" is produced in-process
//!
//! Each store lifetime runs on its own tokio runtime ([`lifetime`]). Dropping
//! the runtime drops every task it spawned — the raft core, the state-machine
//! worker, the rpc loop — and with them every handle on the store. Nothing is
//! stopped, flushed or asked to finish. (fjall's own `Drop` may still flush its
//! journal; case 3 is the real SIGKILL, where nothing gets to run at all.)
//!
//! ## Why cases 1 and 2 drive `FjallStore::apply` themselves
//!
//! The defect they pin depends on HOW applies are batched relative to the
//! catalog blob's persist window, and a live raft decides that batching on its
//! own. So the log is written by a real openraft instance
//! ([`commit_to_durable_log`]) whose state machine is a throwaway in-memory one;
//! the durable store's own state machine is then driven through its public
//! `RaftStateMachine` methods with exactly the batching the scenario names.
//! The reopen is the real boot path: `StoreMesh::start_durable`, which runs
//! `Raft::new` and its replay.
//!
//! ## Tier, stated plainly
//!
//! This is a gate (a test), not a type. Cases 1, 2 and 4 were red at the commit
//! that introduced them and are `#[ignore]`d with the id of the item that turns
//! each green; that item un-ignores it. Cases 3 and 5 are green and run always;
//! case 4 went green with T2.9-store (`terminate` flushes last) and 4b came
//! with it; case 1 went green with T3.3 (the floor on load is the current
//! revision).

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use engenho_store::command::{ApplyMeta, PatchType, TxnCompare, TxnOp};
use engenho_store::{
    ApplyResult, FjallStore, Flushed, InMemoryStore, InProcessRouter, MeshFlushed, Reason,
    ResourceCommand, ResourceKey, Revision, StoreError, StoreMesh, TypeConfig, WatchGone,
    WatchOpts, WatchSignal, default_config,
};
use openraft::storage::{RaftLogStorage as _, RaftStateMachine as _};
use openraft::{
    BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, Membership, Raft, RaftLogReader as _,
    RaftSnapshotBuilder as _,
};
use serde_json::{Value, json};

const NODE: u64 = 1;
const ADDR: &str = "in-process://1";
const LEADERSHIP: Duration = Duration::from_secs(10);

// ── shared harness ────────────────────────────────────────────────────────

/// One process lifetime. Everything `f` spawns lives on this runtime, and
/// dropping the runtime at the end drops every task — the raft core, the
/// state-machine worker, the rpc loop — and with them every handle on the
/// store. For everything engenho owns, the end of a lifetime is a kill.
fn lifetime<F: Future>(f: F) -> F::Output {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build the runtime for one store lifetime");
    let out = rt.block_on(f);
    drop(rt);
    out
}

/// A fresh store path. The data-dir lock is `<store>.lock`, a sibling, so the
/// store lives one level down and the lock is cleaned up with the tempdir.
fn store_dir() -> (tempfile::TempDir, PathBuf) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("store");
    (tmp, dir)
}

fn pod(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

fn put_pod(name: &str) -> ResourceCommand {
    ResourceCommand::put(
        pod(name),
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": name, "namespace": "default"},
            "spec": {"containers": [{"name": "c", "image": "img:1"}]}
        }),
        Reason::Operator,
    )
}

fn names(prefix: &str, n: usize) -> Vec<String> {
    (0..n).map(|i| format!("{prefix}-{i}")).collect()
}

/// A write the store acknowledged: the proposer got `Ok` for it, carrying
/// the revision it was committed at.
#[derive(Clone, Debug)]
struct Ack {
    name: String,
    revision: u64,
}

/// Commit one Put per name into the durable LOG at `dir` through a real
/// openraft instance, applying to a throwaway in-memory state machine.
///
/// The durable store's own state machine (its catalog blob and persisted
/// `last_applied`) is left untouched, so the caller can drive
/// `FjallStore::apply` with exactly the batching its scenario names. Every
/// returned [`Ack`] is a write raft committed and answered.
async fn commit_to_durable_log(dir: &Path, names: &[String]) -> Vec<Ack> {
    let log = FjallStore::open(dir).expect("open the durable log");
    let cfg = default_config("restart-oracle-writer").expect("raft config");
    let raft =
        Raft::<TypeConfig>::new(NODE, cfg, InProcessRouter::new(), log, InMemoryStore::new())
            .await
            .expect("raft over the durable log");
    raft.initialize(BTreeMap::from([(NODE, BasicNode { addr: ADDR.into() })]))
        .await
        .expect("initialize the single voter");
    raft.wait(Some(LEADERSHIP))
        .current_leader(NODE, "writer leadership")
        .await
        .expect("the writer becomes leader");
    let mut acks = Vec::with_capacity(names.len());
    for name in names {
        let resp = raft
            .client_write(put_pod(name))
            .await
            .expect("an acknowledged write");
        acks.push(Ack {
            name: name.clone(),
            revision: resp.data.revision,
        });
    }
    raft.shutdown().await.expect("writer shutdown");
    acks
}

/// Every entry in the durable log, split at the first client write: the raft
/// bookkeeping openraft wrote first (membership, the leader's blank), then the
/// writes in commit order.
async fn read_log(store: &FjallStore) -> (Vec<Entry<TypeConfig>>, Vec<Entry<TypeConfig>>) {
    let mut handle = store.clone();
    let mut reader = handle.get_log_reader().await;
    let mut entries = reader
        .try_get_log_entries(..)
        .await
        .expect("read the durable log");
    let first_write = entries
        .iter()
        .position(|e| matches!(e.payload, EntryPayload::Normal(_)))
        .unwrap_or(entries.len());
    let writes = entries.split_off(first_write);
    (entries, writes)
}

/// What the next boot starts from, observed by opening the directory raw —
/// before any raft replay runs over it.
#[derive(Debug)]
struct DurableImage {
    /// The revision the persisted catalog blob hydrates to.
    revision: u64,
    /// The persisted `last_applied` the next boot replays from.
    last_applied: Option<LogId<u64>>,
    /// The last entry in the durable log.
    last_log: Option<LogId<u64>>,
}

fn durable_image(dir: &Path) -> DurableImage {
    lifetime(async {
        let mut store = FjallStore::open(dir).expect("reopen the directory raw");
        let revision = store.current_revision().await.get();
        let (last_applied, _) = store.applied_state().await.expect("applied state");
        let last_log = store.get_log_state().await.expect("log state").last_log_id;
        DurableImage {
            revision,
            last_applied,
            last_log,
        }
    })
}

/// The real boot path: `StoreMesh::start_durable` runs `Raft::new`, which
/// replays whatever the durable image says is committed but not applied.
async fn boot(dir: &Path, cluster: &str) -> Result<StoreMesh, StoreError> {
    StoreMesh::start_durable(
        NODE,
        ADDR.into(),
        InProcessRouter::new(),
        default_config(cluster)?,
        dir,
    )
    .await
}

/// Every acknowledged write is present, at the revision it was acknowledged
/// at. A write that is present but renumbered is as wrong as a missing one:
/// its resourceVersion is what every client holds.
async fn assert_every_ack_survived(mesh: &StoreMesh, acks: &[Ack]) {
    let mut missing = Vec::new();
    let mut renumbered = Vec::new();
    for ack in acks {
        match mesh.get(&pod(&ack.name)).await {
            None => missing.push(ack.name.as_str()),
            Some(obj) => {
                let rv = obj
                    .pointer("/metadata/resourceVersion")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                if rv.as_deref() != Some(ack.revision.to_string().as_str()) {
                    renumbered.push((ack.name.as_str(), ack.revision, rv));
                }
            }
        }
    }
    assert!(
        missing.is_empty() && renumbered.is_empty(),
        "{} of {} acknowledged writes did not survive the restart: missing {missing:?}; \
         renumbered (name, acked revision, resourceVersion after reopen) {renumbered:?}",
        missing.len() + renumbered.len(),
        acks.len(),
    );
}

/// Drain a watch's replay: every event revision it delivers before going
/// quiet. The replay is enqueued at registration, so a short quiet period
/// means it is complete.
async fn replayed_revisions(mut stream: engenho_store::WatchStream) -> Vec<u64> {
    let mut revs = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_millis(100), stream.next()).await {
            Ok(Some(Ok(WatchSignal::Event(ev)))) => revs.push(ev.resource_version),
            Ok(Some(Ok(WatchSignal::Bookmark(_)))) => {}
            Ok(Some(Err(gone))) => panic!("the replay itself ended with {gone}"),
            Ok(None) | Err(_) => return revs,
        }
    }
}

/// For every resume point up to `head`, the store either refuses it
/// (`CompactedTooOld`, the 410 a client relists on) or replays EXACTLY the
/// revisions after it. An `Ok` replay with revisions missing is the silent
/// defect: the client believes it is caught up and is not.
async fn assert_no_partial_replay(mesh: &StoreMesh, head: u64) {
    for from in 0..=head {
        match mesh
            .watch_from(WatchOpts::live_tail(Revision(from), 4096))
            .await
        {
            Err(WatchGone::CompactedTooOld { .. }) => {}
            Err(other) => panic!("watch_from({from}) failed with an unexpected {other}"),
            Ok(stream) => {
                let got = replayed_revisions(stream).await;
                let want: Vec<u64> = (from + 1..=head).collect();
                assert_eq!(
                    got, want,
                    "watch_from({from}) returned Ok with revisions {got:?}, a strict subset of \
                     {want:?}: the store cannot replay what the durable catalog absorbed before \
                     the restart, so it must answer CompactedTooOld instead"
                );
            }
        }
    }
}

// ── case 1: dropped mid persist-window ─────────────────────────────────────

/// One persisted apply, then three batched applies inside the same persist
/// window, then the process is gone. The catalog blob holds the first write
/// only; the next boot replays the other three from the log.
#[test]
fn case1_drop_mid_persist_window_keeps_every_ack_and_never_replays_a_subset() {
    let (_tmp, dir) = store_dir();
    let acks = lifetime(commit_to_durable_log(&dir, &names("c1", 4)));

    let applied_head = lifetime(async {
        let mut store = FjallStore::open(&dir).expect("open the durable store");
        let (raft_head, writes) = read_log(&store).await;
        let (first, rest) = writes.split_at(1);
        // The persisted apply: raft's bookkeeping plus the first write.
        let mut batch = raft_head;
        batch.extend_from_slice(first);
        store.apply(batch).await.expect("the persisted apply");
        // Three batched applies, all inside the same persist window.
        for entry in rest {
            store
                .apply(vec![entry.clone()])
                .await
                .expect("a batched apply");
        }
        store.current_revision().await.get()
        // Dropped here: no terminate, no flush.
    });
    let last_ack = acks.last().expect("four acks").revision;
    assert_eq!(
        applied_head, last_ack,
        "HARNESS: the same log produced different revisions in two state machines"
    );

    let image = durable_image(&dir);
    assert!(
        image.revision < applied_head,
        "HARNESS PRECONDITION: the scenario must leave the catalog blob behind the applied \
         state (blob at {}, applied {applied_head}), or the reopen replays nothing and this \
         case proves nothing",
        image.revision
    );

    lifetime(async {
        let mesh = boot(&dir, "restart-oracle-c1")
            .await
            .expect("reopen via StoreMesh::start_durable");
        assert!(mesh.wait_for_leadership(LEADERSHIP).await);
        assert_every_ack_survived(&mesh, &acks).await;
        assert_eq!(
            mesh.current_revision().await.get(),
            applied_head,
            "current_revision changed across the restart"
        );
        assert_no_partial_replay(&mesh, applied_head).await;
        mesh.terminate().await.expect("terminate");
    });
}

// ── case 2: snapshot + purge past a stale catalog blob ─────────────────────

/// Applies in the persist window. Below `CATALOG_PERSIST_EVERY` (64), so only
/// the first apply of the process lands the blob.
const C2_APPLIES: usize = 40;
/// Entries per apply. The plan's production shape is 40 × 50 with
/// `purge(1500)`; this keeps the same ratio at a tenth of the size, since every
/// entry costs a real fsync to write.
const C2_ENTRIES_PER_APPLY: usize = 5;
/// Entries the purge keeps: fewer than the blob is behind, which is the whole
/// defect (openraft keeps 1,000 by default, while the blob can fall further
/// behind under a burst).
const C2_KEEP: usize = 50;

#[test]
#[ignore = "red until T3.4 (the durable image is never older than a snapshot): the reopen fails \
            in Raft::new with 'Failed to get log entries, expected index'"]
fn case2_snapshot_and_purge_past_a_stale_blob_still_boots() {
    let (_tmp, dir) = store_dir();
    let acks = lifetime(commit_to_durable_log(
        &dir,
        &names("c2", C2_APPLIES * C2_ENTRIES_PER_APPLY),
    ));

    lifetime(async {
        let mut store = FjallStore::open(&dir).expect("open the durable store");
        let (raft_head, writes) = read_log(&store).await;
        assert_eq!(writes.len(), C2_APPLIES * C2_ENTRIES_PER_APPLY);
        let last = writes.last().expect("writes").log_id;
        store.apply(raft_head).await.expect("the persisted apply");
        for chunk in writes.chunks(C2_ENTRIES_PER_APPLY) {
            store.apply(chunk.to_vec()).await.expect("a batched apply");
        }
        let mut builder = store.get_snapshot_builder().await;
        builder.build_snapshot().await.expect("build_snapshot");
        store
            .save_committed(Some(last))
            .await
            .expect("commit the snapshot's position");
        let purge_upto = writes[writes.len() - 1 - C2_KEEP].log_id;
        store.purge(purge_upto).await.expect("purge");
        // Dropped here: no terminate, no flush.
    });

    lifetime(async {
        let mesh = match boot(&dir, "restart-oracle-c2").await {
            Ok(mesh) => mesh,
            Err(e) => panic!(
                "the node cannot boot after a snapshot and a purge past its catalog blob: {e}"
            ),
        };
        assert!(mesh.wait_for_leadership(LEADERSHIP).await);
        assert_every_ack_survived(&mesh, &acks).await;
        let head = acks.last().expect("acks").revision;
        assert_eq!(mesh.current_revision().await.get(), head);
        let next = mesh
            .propose(put_pod("c2-after-boot"))
            .await
            .expect("the rebooted node takes a write");
        assert_eq!(next.revision, head + 1, "the revision is not continuous");
        mesh.terminate().await.expect("terminate");
    });
}

// ── case 3: SIGKILL mid-propose ────────────────────────────────────────────

/// Set in the child's environment: the store path it proposes into. Its
/// presence is what makes this test binary act as the child.
const CHILD_DIR_ENV: &str = "ENGENHO_RESTART_ORACLE_CHILD_DIR";
/// The token the child prefixes each acknowledgement line with.
const ACK_MARK: &str = "restart-oracle-ack";
/// This test's own name: the child re-runs the test binary filtered to it.
const CASE3: &str = "case3_sigkill_mid_propose_loses_no_acknowledged_write";
/// Concurrent proposers in the child, so a proposal is always in flight.
const CHILD_PROPOSERS: usize = 4;
/// Acknowledgements the parent waits for before the kill: past two
/// 64-apply persist boundaries.
const KILL_AFTER_ACKS: usize = 150;
/// Upper bound on the child's life if the parent never kills it.
const CHILD_WRITE_BOUND: usize = 5_000;

#[test]
fn case3_sigkill_mid_propose_loses_no_acknowledged_write() {
    if let Some(dir) = std::env::var_os(CHILD_DIR_ENV) {
        propose_until_killed(Path::new(&dir));
        return;
    }

    let (_tmp, dir) = store_dir();
    let exe = std::env::current_exe().expect("the test binary's own path");
    let mut child = Command::new(exe)
        .args([CASE3, "--exact", "--nocapture", "--test-threads", "1"])
        .env(CHILD_DIR_ENV, &dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the proposer child");
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");

    let (tx, rx) = mpsc::channel::<Ack>();
    let ack_reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Some(ack) = parse_ack(&line)
                && tx.send(ack).is_err()
            {
                return;
            }
        }
    });
    let stderr_reader = std::thread::spawn(move || {
        let mut text = String::new();
        let _ = BufReader::new(stderr).read_to_string(&mut text);
        text
    });

    let deadline = Instant::now() + Duration::from_secs(120);
    let mut acks = Vec::new();
    while acks.len() < KILL_AFTER_ACKS {
        match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(ack) => acks.push(ack),
            Err(_) => break,
        }
    }
    let acked_before_kill = acks.len();
    // The child's proposers never pause between writes, so the kill lands
    // with proposals in flight.
    child.kill().expect("SIGKILL the proposer");
    let status = child.wait().expect("reap the proposer");
    // Acks the child wrote before it died but the parent had not read yet are
    // acknowledgements all the same.
    acks.extend(rx.iter());
    ack_reader.join().expect("ack reader");
    let child_stderr = stderr_reader.join().expect("stderr reader");

    assert!(
        acked_before_kill >= KILL_AFTER_ACKS,
        "HARNESS: the child acknowledged only {acked_before_kill} writes before the deadline \
         (status {status}); its stderr:\n{child_stderr}"
    );
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;
        assert_eq!(
            status.signal(),
            Some(libc::SIGKILL),
            "the child must have died by SIGKILL, not exited on its own (status {status}); \
             its stderr:\n{child_stderr}"
        );
    }

    lifetime(async {
        let mesh = boot(&dir, "restart-oracle-c3")
            .await
            .expect("reopen after SIGKILL");
        assert!(
            mesh.is_initialized().await,
            "the killed store reopens as initialized"
        );
        assert!(mesh.wait_for_leadership(LEADERSHIP).await);
        assert_every_ack_survived(&mesh, &acks).await;
        let highest = acks.iter().map(|a| a.revision).max().unwrap_or(0);
        assert!(
            mesh.current_revision().await.get() >= highest,
            "the reopened revision is below an acknowledged one"
        );
        mesh.terminate().await.expect("terminate");
    });
}

/// `<ACK_MARK> <name> <revision>` anywhere on a line (libtest may print its
/// own `test … ` prefix on the same line under `--nocapture`).
fn parse_ack(line: &str) -> Option<Ack> {
    let (_, rest) = line.split_once(ACK_MARK)?;
    let mut fields = rest.split_whitespace();
    let name = fields.next()?.to_owned();
    let revision = fields.next()?.parse().ok()?;
    Some(Ack { name, revision })
}

/// The child: propose from several tasks at once, announcing each write only
/// after the store acknowledged it, until the parent kills the process.
fn propose_until_killed(dir: &Path) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("child runtime");
    rt.block_on(async {
        let (mesh, _fresh) = StoreMesh::start_or_resume(
            NODE,
            ADDR.into(),
            InProcessRouter::new(),
            default_config("restart-oracle-c3").expect("raft config"),
            dir,
        )
        .await
        .expect("child store");
        assert!(mesh.wait_for_leadership(LEADERSHIP).await);
        let mesh = Arc::new(mesh);
        let mut proposers = tokio::task::JoinSet::new();
        for p in 0..CHILD_PROPOSERS {
            let mesh = Arc::clone(&mesh);
            proposers.spawn(async move {
                for i in 0..CHILD_WRITE_BOUND {
                    let name = format!("c3-p{p}-{i}");
                    match mesh.propose(put_pod(&name)).await {
                        Ok(res) => {
                            let mut out = std::io::stdout().lock();
                            let _ = writeln!(out, "{ACK_MARK} {name} {}", res.revision);
                            let _ = out.flush();
                        }
                        Err(e) => {
                            eprintln!("child: propose {name} failed: {e}");
                            return;
                        }
                    }
                }
            });
        }
        while proposers.join_next().await.is_some() {}
    });
}

// ── case 4: a clean stop ───────────────────────────────────────────────────

/// How a case-4 lifetime ends after its writes.
#[derive(Clone, Copy, Debug)]
enum CleanStop {
    /// `StoreMesh::terminate`, which flushes as its last step.
    Terminate,
    /// `StoreMesh::flush`, then the lifetime ends with no terminate: the stop
    /// path's flush ran, and nothing after it did (a leaked `Arc` that makes
    /// `terminate` unreachable, or a kill right after the flush).
    FlushThenKill,
}

#[test]
fn case4_clean_stop_leaves_nothing_to_replay() {
    clean_stop_leaves_nothing_to_replay(CleanStop::Terminate, "restart-oracle-c4");
}

#[test]
fn case4b_flush_then_kill_leaves_nothing_to_replay() {
    clean_stop_leaves_nothing_to_replay(CleanStop::FlushThenKill, "restart-oracle-c4b");
}

/// Three writes inside one persist window, then `stop`. The durable image
/// must already hold all three — nothing left in the log for the next boot to
/// replay — and the next boot continues the revision.
fn clean_stop_leaves_nothing_to_replay(stop: CleanStop, cluster: &str) {
    let (_tmp, dir) = store_dir();
    let acks = lifetime(async {
        let (mesh, fresh) = StoreMesh::start_or_resume(
            NODE,
            ADDR.into(),
            InProcessRouter::new(),
            default_config(cluster).expect("raft config"),
            &dir,
        )
        .await
        .expect("first boot");
        assert!(fresh);
        assert!(mesh.wait_for_leadership(LEADERSHIP).await);
        let mut acks = Vec::new();
        for name in names("c4", 3) {
            let res = mesh.propose(put_pod(&name)).await.expect("write");
            acks.push(Ack {
                name,
                revision: res.revision,
            });
        }
        match stop {
            CleanStop::Terminate => mesh.terminate().await.expect("the clean stop"),
            CleanStop::FlushThenKill => {
                let flushed = mesh.flush().await.expect("the stop path's flush");
                assert!(
                    matches!(flushed, MeshFlushed::Durable(Flushed::Persisted { .. })),
                    "HARNESS PRECONDITION: the writes must still be waiting on the persist \
                     cadence when flush runs, or this case proves nothing about flush: \
                     {flushed:?}"
                );
                // Dropped here with the runtime: no terminate.
            }
        }
        acks
    });
    let head = acks.last().expect("acks").revision;

    let image = durable_image(&dir);
    assert_eq!(
        image.last_applied, image.last_log,
        "a clean stop left log entries for the next boot to replay: {image:?}"
    );
    assert_eq!(
        image.revision, head,
        "after a clean stop the durable catalog is behind the last acknowledged write"
    );

    lifetime(async {
        let mesh = boot(&dir, "restart-oracle-c4")
            .await
            .expect("reopen after a clean stop");
        assert!(mesh.wait_for_leadership(LEADERSHIP).await);
        assert_every_ack_survived(&mesh, &acks).await;
        assert_eq!(mesh.current_revision().await.get(), head);
        let next = mesh
            .propose(put_pod("c4-after-boot"))
            .await
            .expect("write after the clean restart");
        assert_eq!(next.revision, head + 1, "the revision is not continuous");
        mesh.terminate().await.expect("terminate");
    });
}

/// The in-memory backend has no durable image, and `flush` says so instead of
/// reporting a write it never made.
#[test]
fn flush_of_an_in_memory_mesh_is_ephemeral() {
    lifetime(async {
        let mesh = StoreMesh::start(
            NODE,
            ADDR.into(),
            InProcessRouter::new(),
            default_config("restart-oracle-memory").expect("raft config"),
        )
        .await
        .expect("start the in-memory mesh");
        assert_eq!(mesh.flush().await.expect("flush"), MeshFlushed::Ephemeral);
        mesh.terminate().await.expect("terminate");
    });
}

// ── case 5: replay compatibility ───────────────────────────────────────────
//
// A fixture is one directory per release under `tests/fixtures/replay/`:
//
//   * `log.jsonl`    — one `Entry<TypeConfig>` per line, the exact encoding the
//                      `log` partition stores;
//   * `results.json` — the `ApplyResult` the release returned for each entry;
//   * `catalog.json` — the release's snapshot bytes after the whole log.
//
// Forward: every fixture replays through THIS tree's durable state machine to
// the same revisions and ops and the same catalog bytes. Applied in chunks of
// 64 (openraft's replay chunk) while it was recorded one entry per apply, so
// the check also pins that batching does not change the outcome.
//
// Recording (at a release tag, from that tag's own tree):
//   ENGENHO_RECORD_REPLAY_FIXTURE=v0.53.118 CARGO_PROFILE_DEV_DEBUG=0 \
//     cargo test -p engenho-store --test restart_is_a_kill -- --ignored record_replay_fixture
//
// Backward (a log written by the working tree replayed by the previous
// release) needs the previous release's binary, so it runs outside this
// file: record from the working tree with `ENGENHO_REPLAY_FIXTURE_DIR`
// pointing at a scratch directory, then run the PREVIOUS tag's case 5 with
// the same `ENGENHO_REPLAY_FIXTURE_DIR`. Possible from the first release that
// contains this file; v0.53.118 predates it.

const FIXTURE_LOG: &str = "log.jsonl";
const FIXTURE_RESULTS: &str = "results.json";
const FIXTURE_CATALOG: &str = "catalog.json";
const RECORD_ENV: &str = "ENGENHO_RECORD_REPLAY_FIXTURE";
/// Overrides where fixtures are read from and recorded to (the backward run).
const FIXTURE_DIR_ENV: &str = "ENGENHO_REPLAY_FIXTURE_DIR";
/// openraft's replay chunk size (`reapply_committed`).
const REPLAY_CHUNK: usize = 64;

fn fixtures_root() -> PathBuf {
    std::env::var_os(FIXTURE_DIR_ENV).map_or_else(
        || Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/replay"),
        PathBuf::from,
    )
}

/// The fields of an [`ApplyResult`] a replay must reproduce. `patch_error`'s
/// TEXT is a diagnostic for the caller, not replicated state, so only its
/// presence is compared.
fn replay_outcome(r: &ApplyResult) -> (u64, u64, engenho_store::ResourceOp, u64, bool) {
    (
        r.applied_index,
        r.applied_term,
        r.op,
        r.revision,
        r.patch_error.is_some(),
    )
}

/// The catalog bytes a snapshot of `store` carries.
async fn snapshot_bytes(store: &FjallStore) -> Vec<u8> {
    let mut handle = store.clone();
    let mut builder = handle.get_snapshot_builder().await;
    let snap = builder.build_snapshot().await.expect("build_snapshot");
    (*snap.snapshot).into_inner()
}

#[test]
fn case5_recorded_release_logs_replay_forward_to_identical_state() {
    let mut releases: Vec<PathBuf> = std::fs::read_dir(fixtures_root())
        .expect("the replay fixture directory")
        .map(|e| e.expect("fixture entry").path())
        .filter(|p| p.is_dir())
        .collect();
    releases.sort();
    assert!(
        !releases.is_empty(),
        "no recorded release under {} — the replay gate would check nothing",
        fixtures_root().display()
    );
    for release in &releases {
        replay_forward(release);
    }
}

fn replay_forward(release: &Path) {
    let label = release.display();
    let log_text = std::fs::read_to_string(release.join(FIXTURE_LOG)).expect("fixture log");
    let entries: Vec<Entry<TypeConfig>> = log_text
        .lines()
        .enumerate()
        .map(|(n, line)| {
            serde_json::from_str(line).unwrap_or_else(|e| {
                panic!("{label}: log line {n} no longer decodes in this tree: {e}")
            })
        })
        .collect();
    let recorded: Vec<ApplyResult> = serde_json::from_slice(
        &std::fs::read(release.join(FIXTURE_RESULTS)).expect("fixture results"),
    )
    .expect("fixture results decode");
    let recorded_catalog = std::fs::read(release.join(FIXTURE_CATALOG)).expect("fixture catalog");
    assert_eq!(
        entries.len(),
        recorded.len(),
        "{label}: fixture log and results disagree in length"
    );

    let (_tmp, dir) = store_dir();
    let (results, catalog) = lifetime(async {
        let mut store = FjallStore::open(&dir).expect("a fresh durable store");
        let mut results = Vec::with_capacity(entries.len());
        for chunk in entries.chunks(REPLAY_CHUNK) {
            results.extend(store.apply(chunk.to_vec()).await.expect("replay apply"));
        }
        let catalog = snapshot_bytes(&store).await;
        (results, catalog)
    });
    assert_eq!(
        results.len(),
        recorded.len(),
        "{label}: the replay returned a different number of results than it applied entries"
    );

    for (n, (now, then)) in results.iter().zip(&recorded).enumerate() {
        assert_eq!(
            replay_outcome(now),
            replay_outcome(then),
            "{label}: entry {n} replays differently in this tree.\n  entry: {}\n  \
             recorded (index, term, op, revision, rejected): {:?}\n  \
             now: {:?}",
            serde_json::to_string(&entries[n]).unwrap_or_default(),
            replay_outcome(then),
            replay_outcome(now),
        );
    }
    if catalog != recorded_catalog {
        let same_state = serde_json::from_slice::<Value>(&catalog).ok()
            == serde_json::from_slice::<Value>(&recorded_catalog).ok();
        panic!(
            "{label}: the replayed catalog's bytes differ from the release's ({} vs {} bytes; \
             equal as JSON values: {same_state})",
            catalog.len(),
            recorded_catalog.len()
        );
    }
}

/// The recorder. Not a check: it writes a release's fixture from the tree it
/// runs in, so it must run from that release's own tag.
#[test]
#[ignore = "fixture recorder, not a check: run from a release tag with \
            ENGENHO_RECORD_REPLAY_FIXTURE=<tag> ... -- --ignored record_replay_fixture"]
fn record_replay_fixture() {
    let Some(tag) = std::env::var_os(RECORD_ENV) else {
        panic!("{RECORD_ENV} must name the release tag being recorded");
    };
    let out = fixtures_root().join(tag);
    std::fs::create_dir_all(&out).expect("create the fixture directory");

    let (_tmp, dir) = store_dir();
    let (log, results, catalog) = lifetime(async {
        let mut rec = Recorder {
            store: FjallStore::open(&dir).expect("a fresh durable store"),
            log: Vec::new(),
            results: Vec::new(),
            term: 0,
        };
        replay_scenario(&mut rec).await;
        let catalog = snapshot_bytes(&rec.store).await;
        (rec.log, rec.results, catalog)
    });

    let mut log_text = String::new();
    for entry in &log {
        log_text.push_str(&serde_json::to_string(entry).expect("encode entry"));
        log_text.push('\n');
    }
    std::fs::write(out.join(FIXTURE_LOG), log_text).expect("write log");
    let mut results_text = serde_json::to_string_pretty(&results).expect("encode results");
    results_text.push('\n');
    std::fs::write(out.join(FIXTURE_RESULTS), results_text).expect("write results");
    std::fs::write(out.join(FIXTURE_CATALOG), catalog).expect("write catalog");
}

/// Writes entries into the log it records AND applies each one to a real
/// durable state machine as it goes, so the scenario can aim its CAS
/// preconditions at revisions the store actually returned.
struct Recorder {
    store: FjallStore,
    log: Vec<Entry<TypeConfig>>,
    results: Vec<ApplyResult>,
    term: u64,
}

impl Recorder {
    async fn push(&mut self, payload: EntryPayload<TypeConfig>) -> ApplyResult {
        let index = self.log.len() as u64;
        let entry = Entry {
            log_id: LogId {
                leader_id: CommittedLeaderId::new(self.term, NODE),
                index,
            },
            payload,
        };
        let mut out = self
            .store
            .apply(vec![entry.clone()])
            .await
            .expect("record apply");
        self.log.push(entry);
        let result = out.pop().expect("one result per entry");
        self.results.push(result.clone());
        result
    }

    async fn membership(&mut self) {
        let voters = BTreeSet::from([NODE]);
        let nodes = BTreeMap::from([(NODE, BasicNode { addr: ADDR.into() })]);
        self.push(EntryPayload::Membership(Membership::new(
            vec![voters],
            nodes,
        )))
        .await;
    }

    async fn elect(&mut self, term: u64) {
        self.term = term;
        self.push(EntryPayload::Blank).await;
    }

    async fn write(&mut self, cmd: ResourceCommand) -> ApplyResult {
        self.push(EntryPayload::Normal(cmd)).await
    }
}

fn cm(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "ConfigMap", "default", name)
}

fn cm_body(name: &str, data: &Value) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": name, "namespace": "default"},
        "data": data
    })
}

fn deploy() -> ResourceKey {
    ResourceKey::namespaced("apps", "v1", "Deployment", "default", "web")
}

fn ssa(manager: &str, force: bool) -> ApplyMeta {
    ApplyMeta {
        manager: manager.into(),
        force,
        time: "2026-09-19T00:00:00Z".into(),
    }
}

/// The recorded scenario: every command shape the state machine
/// distinguishes, including the outcomes that consume no revision.
async fn replay_scenario(rec: &mut Recorder) {
    let op = Reason::Operator;
    rec.membership().await;
    rec.elect(1).await;

    // Put: create, identical rewrite, change, CAS hit, CAS miss.
    let created = rec
        .write(ResourceCommand::put(
            cm("a"),
            cm_body("a", &json!({"k": "v1"})),
            op,
        ))
        .await;
    rec.write(ResourceCommand::put(
        cm("a"),
        cm_body("a", &json!({"k": "v1"})),
        op,
    ))
    .await;
    let changed = rec
        .write(ResourceCommand::put(
            cm("a"),
            cm_body("a", &json!({"k": "v2"})),
            op,
        ))
        .await;
    rec.write(ResourceCommand::Put {
        key: cm("a"),
        value: cm_body("a", &json!({"k": "v3"})),
        expected: Some(Revision(changed.revision)),
        reason: op,
    })
    .await;
    rec.write(ResourceCommand::Put {
        key: cm("a"),
        value: cm_body("a", &json!({"k": "stale"})),
        expected: Some(Revision(created.revision)),
        reason: op,
    })
    .await;

    // Patch: merge add + remove, json-patch rejected + applied, identical
    // merge, patch of a missing object.
    rec.write(ResourceCommand::patch(
        cm("a"),
        json!({"data": {"k2": "x"}}),
        Reason::Controller,
    ))
    .await;
    rec.write(ResourceCommand::patch(
        cm("a"),
        json!({"data": {"k2": null}}),
        Reason::Controller,
    ))
    .await;
    rec.write(ResourceCommand::patch_typed(
        cm("a"),
        json!([{"op": "test", "path": "/data/k", "value": "not-this"}]),
        PatchType::Json,
        op,
    ))
    .await;
    rec.write(ResourceCommand::patch_typed(
        cm("a"),
        json!([{"op": "add", "path": "/data/k3", "value": "y"}]),
        PatchType::Json,
        op,
    ))
    .await;
    rec.write(ResourceCommand::patch(
        cm("a"),
        json!({"data": {"k3": "y"}}),
        Reason::Controller,
    ))
    .await;
    rec.write(ResourceCommand::patch(
        cm("missing"),
        json!({"data": {"x": "1"}}),
        op,
    ))
    .await;

    // Strategic merge: list-merge by key, then a `$patch: delete`; then a
    // status write with a CAS hit and a CAS miss.
    rec.write(ResourceCommand::put(
        deploy(),
        json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "web", "namespace": "default", "labels": {"app": "web"}},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "web"}},
                "template": {
                    "metadata": {"labels": {"app": "web"}},
                    "spec": {"containers": [
                        {"name": "app", "image": "app:v1", "ports": [{"containerPort": 8080}]},
                        {"name": "sidecar", "image": "sidecar:v1"}
                    ]}
                }
            }
        }),
        op,
    ))
    .await;
    rec.write(ResourceCommand::patch_typed(
        deploy(),
        json!({"spec": {"template": {"spec": {"containers": [
            {"name": "app", "image": "app:v2"}
        ]}}}}),
        PatchType::Strategic,
        op,
    ))
    .await;
    let pruned = rec
        .write(ResourceCommand::patch_typed(
            deploy(),
            json!({"spec": {"template": {"spec": {"containers": [
                {"name": "sidecar", "$patch": "delete"}
            ]}}}}),
            PatchType::Strategic,
            op,
        ))
        .await;
    rec.write(ResourceCommand::patch_cas(
        deploy(),
        json!({"status": {"replicas": 1, "readyReplicas": 1}}),
        Some(Revision(pruned.revision)),
        Reason::Controller,
    ))
    .await;
    rec.write(ResourceCommand::patch_cas(
        deploy(),
        json!({"status": {"replicas": 0}}),
        Some(Revision(pruned.revision)),
        Reason::Controller,
    ))
    .await;

    // Server-side apply: create, a conflict, a forced takeover, a second
    // manager's disjoint field.
    rec.write(ResourceCommand::apply_ssa(
        cm("ssa"),
        cm_body("ssa", &json!({"a": "1"})),
        ssa("kubectl", false),
        None,
        op,
    ))
    .await;
    rec.write(ResourceCommand::apply_ssa(
        cm("ssa"),
        cm_body("ssa", &json!({"a": "2"})),
        ssa("controller", false),
        None,
        Reason::Controller,
    ))
    .await;
    rec.write(ResourceCommand::apply_ssa(
        cm("ssa"),
        cm_body("ssa", &json!({"a": "2"})),
        ssa("controller", true),
        None,
        Reason::Controller,
    ))
    .await;
    rec.write(ResourceCommand::apply_ssa(
        cm("ssa"),
        cm_body("ssa", &json!({"b": "1"})),
        ssa("kubectl", false),
        None,
        op,
    ))
    .await;

    // A new term, then a membership re-commit.
    rec.elect(2).await;
    rec.membership().await;

    // Finalizers: a stamped delete goes Terminating, a second delete is a
    // no-op, clearing the finalizers removes; an unstamped delete of a
    // finalizer-bearing object.
    for name in ["fin", "fin-unstamped"] {
        rec.write(ResourceCommand::put(
            pod(name),
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": name, "namespace": "default",
                             "finalizers": ["example.com/hold"]},
                "spec": {"containers": [{"name": "c", "image": "img:1"}]}
            }),
            op,
        ))
        .await;
    }
    for _ in 0..2 {
        rec.write(ResourceCommand::delete_at(
            pod("fin"),
            None,
            op,
            Some("2026-09-19T00:00:01Z".into()),
        ))
        .await;
    }
    rec.write(ResourceCommand::patch(
        pod("fin"),
        json!({"metadata": {"finalizers": null}}),
        Reason::Controller,
    ))
    .await;
    rec.write(ResourceCommand::delete(
        pod("fin-unstamped"),
        Reason::GarbageCollector,
    ))
    .await;

    // Cluster-scoped create; deletes: CAS miss, CAS hit, not found.
    rec.write(ResourceCommand::put(
        ResourceKey::cluster_scoped("", "v1", "Namespace", "team-a"),
        json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "team-a"}}),
        op,
    ))
    .await;
    let live = rec
        .write(ResourceCommand::put(
            cm("a"),
            cm_body("a", &json!({"k": "final"})),
            op,
        ))
        .await;
    rec.write(ResourceCommand::delete_at(
        cm("a"),
        Some(Revision(created.revision)),
        op,
        None,
    ))
    .await;
    rec.write(ResourceCommand::delete_at(
        cm("a"),
        Some(Revision(live.revision)),
        op,
        None,
    ))
    .await;
    rec.write(ResourceCommand::delete(cm("a"), op)).await;

    // Transactions: both branches, and an unconditional one.
    let txn = rec
        .write(ResourceCommand::Txn {
            compares: vec![TxnCompare::NotExists { key: cm("t1") }],
            success: vec![
                TxnOp::Put {
                    key: cm("t1"),
                    value: cm_body("t1", &json!({"n": "1"})),
                },
                TxnOp::Put {
                    key: cm("t2"),
                    value: cm_body("t2", &json!({"n": "2"})),
                },
            ],
            failure: vec![],
            reason: op,
        })
        .await;
    rec.write(ResourceCommand::Txn {
        compares: vec![TxnCompare::ModRevisionEq {
            key: cm("t1"),
            revision: Revision(txn.revision + 100),
        }],
        success: vec![TxnOp::Delete {
            key: cm("t1"),
            deletion_timestamp: None,
        }],
        failure: vec![TxnOp::Delete {
            key: cm("t2"),
            deletion_timestamp: None,
        }],
        reason: op,
    })
    .await;
    rec.write(ResourceCommand::Txn {
        compares: vec![],
        success: vec![TxnOp::Put {
            key: cm("t1"),
            value: cm_body("t1", &json!({"n": "1b"})),
        }],
        failure: vec![],
        reason: op,
    })
    .await;

    // Volume: enough plain writes that revisions and log indices diverge.
    for i in 0..24 {
        rec.write(put_pod(&format!("bulk-{i}"))).await;
    }
    for i in 0..8 {
        rec.write(ResourceCommand::patch(
            pod(&format!("bulk-{i}")),
            json!({"metadata": {"labels": {"round": "2"}}}),
            Reason::Controller,
        ))
        .await;
    }
}
