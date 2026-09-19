//! T1.8 — a reported change is a write that landed.
//!
//! Before T1.8 every count site proposed a command, `?`-ed the transport
//! error and added one, without reading the `ResourceOp` the store returned.
//! These tests drive a real in-memory store and datapath doubles to the two
//! cases that difference is about:
//!
//! * **E1** a write the store answers `NoOp` (nothing changed) is not a change;
//! * **E2** a datapath removal the backend refused is not a change.
//!
//! Each has a positive control in the same test, so a controller that stops
//! counting altogether fails too.
//!
//! * **E3** a gate, not a type: no source file in the workspace writes
//!   `objects_changed` except through `ReconcileReport::record` and the sweep
//!   conversion, and the sites not yet migrated stay under a ceiling.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};

use engenho_controllers::network_policy::{
    Direction, FakeNetworkPolicyEnforcer, NetworkPolicyEnforcer, NetworkPolicyError,
    NetworkPolicyRule, PolicyDatapath,
};
use engenho_controllers::network_policy_controller::NetworkPolicyController;
use engenho_controllers::{Controller, GcController, OwnerReference, set_owner_reference};
use engenho_store::command::{Reason, ResourceCommand};
use engenho_store::{InProcessRouter, ResourceKey, StoreMesh, default_config};

async fn boot(tag: &str) -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config(tag).unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

async fn put(store: &StoreMesh, key: &ResourceKey, value: Value) {
    store
        .propose(ResourceCommand::Put {
            key: key.clone(),
            value,
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

/// A pod controlled by a ReplicaSet that was never stored: an orphan.
fn orphan_pod(name: &str, finalizers: &[&str]) -> Value {
    let mut pod = json!({
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": { "name": name, "finalizers": finalizers },
        "spec": { "containers": [{ "name": "c", "image": "x" }] }
    });
    set_owner_reference(
        &mut pod,
        OwnerReference {
            api_version: "apps/v1".into(),
            kind: "ReplicaSet".into(),
            name: "gone".into(),
            uid: "uid-never-stored".into(),
            controller: true,
            block_owner_deletion: true,
        },
    )
    .unwrap();
    pod
}

/// E1. A delete the store answers `NoOp` is not a change.
///
/// Since store T3.6 every delete carries a clock, so gc's first delete of a
/// finalizer-bearing orphan stamps `deletionTimestamp` (Terminating): a real
/// change, counted once. A later delete of that object, for as long as the
/// finalizer holds, would be the store's `NoOp`; that repeat used to count as
/// a change on every tick. gc no longer proposes it at all (I3).
#[tokio::test]
async fn a_write_the_store_answered_noop_is_not_a_change() {
    let store = boot("t1-8-gc-noop").await;
    let held = ResourceKey::namespaced("", "v1", "Pod", "ns", "held");
    let free = ResourceKey::namespaced("", "v1", "Pod", "ns", "free");
    put(&store, &held, orphan_pod("held", &["example.com/hold"])).await;
    put(&store, &free, orphan_pod("free", &[])).await;

    let gc = GcController::new(store.clone(), None);
    let outcome = gc.tick().await.expect("gc tick");

    assert_eq!(outcome.report.objects_examined, 2);
    assert!(
        store.get(&free).await.is_none(),
        "positive control: the finalizer-free orphan was deleted"
    );
    let terminating = store.get(&held).await.expect("held survives its finalizer");
    assert!(
        terminating["metadata"]["deletionTimestamp"].is_string(),
        "the finalizer-bearing orphan is Terminating, not gone: {terminating}"
    );
    assert_eq!(
        outcome.report.objects_changed, 2,
        "both deletes landed: one removal, one Terminating stamp"
    );

    // The next tick leaves the Terminating orphan to its finalizer: gc
    // proposes no second delete (I3, `i3_terminating_children.rs` pins the
    // Raft log), and nothing is counted.
    let again = gc.tick().await.expect("gc tick");
    assert_eq!(store.get(&held).await.expect("held"), terminating);
    assert_eq!(
        again.report.objects_changed, 0,
        "a NoOp repeated every tick is not a change every tick"
    );
}

fn stale_rule() -> NetworkPolicyRule {
    NetworkPolicyRule {
        policy_id: "ns/deleted#0".into(),
        pod_selector: std::collections::BTreeMap::new(),
        direction: Direction::Ingress,
        allowed_peers: Vec::new(),
        allowed_ports: Vec::new(),
    }
}

/// An enforcer still holding a deleted policy's rule, which refuses to
/// remove it.
struct Stuck;

#[async_trait]
impl NetworkPolicyEnforcer for Stuck {
    fn name(&self) -> &'static str {
        "stuck"
    }
    fn datapath(&self) -> PolicyDatapath {
        PolicyDatapath::Computed
    }
    async fn upsert(&self, _rule: &NetworkPolicyRule) -> Result<(), NetworkPolicyError> {
        Ok(())
    }
    async fn remove(&self, _policy_id: &str) -> Result<(), NetworkPolicyError> {
        Err(NetworkPolicyError::Backend("rule busy".into()))
    }
    async fn list(&self) -> Result<Vec<NetworkPolicyRule>, NetworkPolicyError> {
        Ok(vec![stale_rule()])
    }
}

/// E2. The reap of a deleted policy's rule used to be
/// `let _ = remove(..); reaped += 1`: a removal the enforcer refused was
/// reported as a change, while the filter stayed installed.
#[tokio::test]
async fn a_backend_removal_that_failed_is_not_a_change() {
    let store = boot("t1-8-np-reap").await;

    // Positive control: an enforcer that does remove the stale rule.
    let working = Arc::new(FakeNetworkPolicyEnforcer::new());
    working.upsert(&stale_rule()).await.unwrap();
    let c = NetworkPolicyController::new(store.clone(), working.clone());
    let reaped = c.tick().await.expect("tick");
    assert_eq!(working.rule_count().await, 0, "the stale rule was removed");
    assert_eq!(
        reaped.report.objects_changed, 1,
        "a removal that landed is a change"
    );

    // The enforcer refuses: the rule is still there, and nothing changed.
    let c = NetworkPolicyController::new(store.clone(), Arc::new(Stuck));
    let refused = c
        .tick()
        .await
        .expect("a refused removal does not fail the tick");
    assert_eq!(
        refused.report.objects_changed, 0,
        "a removal the enforcer refused is not a change"
    );
}

// ── E3: the legacy counter path, gated ──────────────────────────────────

/// The only lines in any workspace `src/` that may write
/// `ReconcileReport::objects_changed`: the field itself, the one increment
/// in `record` (which reads an `Effect`), and the sweep conversion (whose
/// count is of `Changed(Landed)` outcomes).
const ALLOWED: [(&str, &str); 3] = [
    (
        "engenho-controllers/src/controller.rs",
        "pub objects_changed: usize,",
    ),
    (
        "engenho-controllers/src/controller.rs",
        "Effect::Written(_) => self.objects_changed += 1,",
    ),
    (
        "engenho-controllers/src/sweep.rs",
        "objects_changed: s.changed,",
    ),
];

/// Bare writes that predate T1.8, in crates outside the controllers lane.
/// Each file may keep at most this many; a new one fails the gate. Lower a
/// ceiling when a site moves to `record(Effect)`. At zero everywhere the
/// field goes private, which makes this whole gate a compile error, and
/// this test is deleted.
const NOT_YET_MIGRATED: [(&str, usize); 3] = [
    ("engenho-kubelet/src/kubelet.rs", 8),
    ("engenho-kubelet/src/csi_materializer.rs", 2),
    ("engenho-scheduler/src/scheduler.rs", 1),
];

const FIELD: &str = "objects_changed";

/// True iff this line assigns the field, compound-assigns it, or sets it
/// in a struct literal. Reads (`, `, `)`, `==`, `>=`) are not writes. Text
/// after `//` is ignored, so a write that follows a `//` inside a string
/// literal on the same line is missed: this is a text gate over
/// rustfmt-shaped code, not a parser.
fn writes_the_field(line: &str) -> bool {
    let code = line.split("//").next().unwrap_or_default();
    code.match_indices(FIELD).any(|(at, _)| {
        let ident = |c: char| c.is_alphanumeric() || c == '_';
        let before = code[..at].chars().next_back();
        let rest = &code[at + FIELD.len()..];
        if before.is_some_and(ident) || rest.chars().next().is_some_and(ident) {
            return false;
        }
        let rest = rest.trim_start();
        ["+=", "-=", "*="].iter().any(|op| rest.starts_with(op))
            || (rest.starts_with('=') && !rest.starts_with("==") && !rest.starts_with("=>"))
            || (rest.starts_with(':') && !rest.starts_with("::"))
    })
}

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("readable source dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Every bare write of the field under each crate's `src/`, keyed by the
/// file's workspace-relative path, as `line: text`.
fn bare_writes(root: &Path) -> BTreeMap<String, Vec<String>> {
    let mut crates: Vec<PathBuf> = fs::read_dir(root)
        .expect("workspace root")
        .map(|e| e.expect("dir entry").path().join("src"))
        .filter(|src| src.is_dir())
        .collect();
    crates.sort();
    let mut found: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for src in crates {
        let mut files = Vec::new();
        rust_sources(&src, &mut files);
        for file in files {
            let rel = file
                .strip_prefix(root)
                .expect("under the root")
                .to_string_lossy()
                .replace('\\', "/");
            let text = fs::read_to_string(&file).expect("utf-8 source");
            for (n, line) in text.lines().enumerate() {
                let trimmed = line.trim();
                let allowed = ALLOWED
                    .iter()
                    .any(|&(path, ok)| path == rel && ok == trimmed);
                if writes_the_field(line) && !allowed {
                    found
                        .entry(rel.clone())
                        .or_default()
                        .push(format!("{}: {trimmed}", n + 1));
                }
            }
        }
    }
    found
}

#[test]
fn the_gate_reads_writes_and_not_reads() {
    for write in [
        "report.objects_changed += 1;",
        "report.objects_changed = 3;",
        "objects_changed: report.bound.len(),",
        "self.objects_changed -= 1;",
    ] {
        assert!(writes_the_field(write), "{write}");
    }
    for read in [
        "assert_eq!(out.objects_changed, 1);",
        "if self.objects_changed > 0 || x {",
        "assert!(out.objects_changed >= 2);",
        "changed = self.objects_changed,",
        "x.objects_changed == 0",
        "// report.objects_changed += 1;",
        "report.objects_changed_total += 1;",
        "my_objects_changed = 2;",
    ] {
        assert!(!writes_the_field(read), "{read}");
    }
}

/// E3. Every count in the workspace goes through an `Effect`, except the
/// ceilinged sites above. CI-caught, not a type: `objects_changed` is still
/// a public field.
#[test]
fn no_count_in_the_workspace_bypasses_effect() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the crate sits in the workspace root");
    let found = bare_writes(root);

    // Positive control: the gate sees the ceilinged sites, so an empty
    // result means "no bypass", not "read nothing".
    assert!(
        found.contains_key("engenho-kubelet/src/kubelet.rs"),
        "the gate must see the kubelet's known writes; found {found:#?}"
    );

    let ceilings: BTreeMap<&str, usize> = NOT_YET_MIGRATED.into_iter().collect();
    for (file, sites) in &found {
        let ceiling = ceilings.get(file.as_str()).copied().unwrap_or(0);
        assert!(
            sites.len() <= ceiling,
            "{file} writes objects_changed without an Effect ({} sites, at most {ceiling} \
             allowed); count it with ReconcileReport::record(Effect::..):\n{}",
            sites.len(),
            sites.join("\n"),
        );
    }
}
