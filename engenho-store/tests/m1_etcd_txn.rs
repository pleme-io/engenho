//! M1 — atomic multi-key transaction invariants (`ResourceCommand::Txn`).
//!
//! All tests are cargo-only: pure functions over the in-memory state
//! machine. No network, no filesystem, no Raft.
//!
//! `Txn` exists for the etcd v3 façade — every other command touches one
//! key, which is all engenho's own apiserver ever needed. The properties
//! below are the ones a consumer can OBSERVE on the etcd wire, so a
//! divergence is not cosmetic:
//!
//!   * T1 all-or-nothing — a failing compare applies no op from either
//!     branch's success side, and leaves the catalog byte-identical.
//!   * T2 one transaction, one revision — every key a Txn mutates shares
//!     the same `mod_revision`, and `current_revision` advances by exactly
//!     one however many keys were touched.
//!   * T3 branch selection — all compares must hold; an empty compare list
//!     is vacuously true (etcd's unconditional Txn).
//!   * T4 no-op neutrality — an empty or fully-no-op branch advances
//!     neither `current_revision` nor `history`, the same law every
//!     single-key path obeys.
//!   * T5 history completeness — every mutated key appears in `history`,
//!     so a watcher resuming from before the Txn sees ALL of it, never a
//!     torn half.

use engenho_store::command::{Reason, ResourceCommand, TxnCompare, TxnOp};
use engenho_store::revision::Revision;
use engenho_store::{ResourceCatalog, ResourceKey, ResourceValue};

fn pod_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

fn pod(name: &str) -> ResourceValue {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": name, "namespace": "default" }
    })
}

fn txn(compares: Vec<TxnCompare>, success: Vec<TxnOp>, failure: Vec<TxnOp>) -> ResourceCommand {
    ResourceCommand::Txn {
        compares,
        success,
        failure,
        reason: Reason::Operator,
    }
}

fn put_op(name: &str) -> TxnOp {
    TxnOp::Put {
        key: pod_key(name),
        value: pod(name),
    }
}

/// Apply one command at the next Raft index.
fn apply(
    cat: &mut ResourceCatalog,
    cmd: &ResourceCommand,
    index: u64,
) -> engenho_store::state::ApplyOutcome {
    cat.apply(cmd, 1, index)
}

// ── T2 + T5 ───────────────────────────────────────────────────────────

#[test]
fn t2_one_transaction_is_one_revision_across_every_key() {
    let mut cat = ResourceCatalog::default();
    let before = cat.revision();

    let out = apply(
        &mut cat,
        &txn(vec![], vec![put_op("a"), put_op("b"), put_op("c")], vec![]),
        1,
    );

    // Exactly one revision consumed, however many keys were touched.
    assert_eq!(cat.revision().get(), before.get() + 1);

    // Every key carries THAT revision as its mod_revision — this is the
    // property an etcd client reads off `KeyValue.mod_revision`.
    let rev = cat.revision();
    for name in ["a", "b", "c"] {
        let (_, meta) = cat
            .get_with_meta(&pod_key(name))
            .unwrap_or_else(|| panic!("{name} must exist"));
        assert_eq!(
            meta.mod_revision, rev,
            "{name} must share the transaction's revision"
        );
    }

    // T5: all three changes are reported, and all at the same revision.
    let all: Vec<&engenho_store::revision::Change> =
        out.change.iter().chain(out.extra_changes.iter()).collect();
    assert_eq!(all.len(), 3, "every mutated key must be reported");
    assert!(all.iter().all(|c| c.revision == rev));
}

#[test]
fn t5_a_watcher_resuming_before_the_txn_sees_all_of_it() {
    let mut cat = ResourceCatalog::default();
    let before = cat.revision();
    apply(
        &mut cat,
        &txn(vec![], vec![put_op("a"), put_op("b")], vec![]),
        1,
    );
    let changes = cat.changes_since(before).expect("within history");
    assert_eq!(
        changes.len(),
        2,
        "a torn half-transaction is the failure this guards"
    );
    assert!(changes.iter().all(|c| c.revision == cat.revision()));
}

// ── T1 + T3 ───────────────────────────────────────────────────────────

#[test]
fn t1_a_failing_compare_mutates_nothing_and_consumes_no_revision() {
    let mut cat = ResourceCatalog::default();
    // Seed one key so the compare has something to be wrong about.
    apply(
        &mut cat,
        &ResourceCommand::put(pod_key("seed"), pod("seed"), Reason::Operator),
        1,
    );
    let rev_before = cat.revision();

    // Compare demands a mod_revision that is NOT the live one.
    let out = apply(
        &mut cat,
        &txn(
            vec![TxnCompare::ModRevisionEq {
                key: pod_key("seed"),
                revision: Revision(9_999),
            }],
            vec![put_op("a"), put_op("b")],
            vec![],
        ),
        2,
    );

    assert!(out.change.is_none(), "no change on a failed compare");
    assert!(out.extra_changes.is_empty());
    assert_eq!(cat.revision(), rev_before, "no revision consumed");
    assert!(
        cat.get(&pod_key("a")).is_none(),
        "success branch must not run"
    );
    assert!(cat.get(&pod_key("b")).is_none());
}

#[test]
fn t3_all_compares_must_hold_and_an_empty_list_is_vacuously_true() {
    let mut cat = ResourceCatalog::default();
    apply(
        &mut cat,
        &ResourceCommand::put(pod_key("seed"), pod("seed"), Reason::Operator),
        1,
    );
    let seed_rev = cat.revision();

    // Two compares, both true ⇒ success branch.
    apply(
        &mut cat,
        &txn(
            vec![
                TxnCompare::ModRevisionEq {
                    key: pod_key("seed"),
                    revision: seed_rev,
                },
                TxnCompare::NotExists {
                    key: pod_key("absent"),
                },
            ],
            vec![put_op("ok")],
            vec![put_op("nope")],
        ),
        2,
    );
    assert!(cat.get(&pod_key("ok")).is_some());
    assert!(cat.get(&pod_key("nope")).is_none());

    // One true, one false ⇒ FAILURE branch. ALL must hold, not any.
    apply(
        &mut cat,
        &txn(
            vec![
                TxnCompare::NotExists {
                    key: pod_key("still-absent"),
                },
                TxnCompare::NotExists {
                    key: pod_key("seed"), // false — it exists
                },
            ],
            vec![put_op("wrong")],
            vec![put_op("right")],
        ),
        3,
    );
    assert!(
        cat.get(&pod_key("wrong")).is_none(),
        "ALL compares must hold"
    );
    assert!(cat.get(&pod_key("right")).is_some());
}

#[test]
fn t3_not_exists_is_the_create_compare_kube_apiserver_emits() {
    let mut cat = ResourceCatalog::default();
    // First create wins.
    apply(
        &mut cat,
        &txn(
            vec![TxnCompare::NotExists { key: pod_key("x") }],
            vec![put_op("x")],
            vec![],
        ),
        1,
    );
    assert!(cat.get(&pod_key("x")).is_some());

    // The identical transaction replayed must now take the failure branch —
    // this is exactly how an apiserver detects AlreadyExists.
    let rev = cat.revision();
    let out = apply(
        &mut cat,
        &txn(
            vec![TxnCompare::NotExists { key: pod_key("x") }],
            vec![put_op("x")],
            vec![],
        ),
        2,
    );
    assert!(out.change.is_none());
    assert_eq!(cat.revision(), rev, "a refused create consumes no revision");
}

// ── T4 ────────────────────────────────────────────────────────────────

#[test]
fn t4_an_empty_branch_is_a_noop_not_a_silent_success() {
    let mut cat = ResourceCatalog::default();
    let rev = cat.revision();
    // Compares fail, and the failure branch is empty.
    let out = apply(
        &mut cat,
        &txn(
            vec![TxnCompare::ModRevisionEq {
                key: pod_key("nothing"),
                revision: Revision(1),
            }],
            vec![put_op("a")],
            vec![],
        ),
        1,
    );
    assert!(out.change.is_none());
    assert_eq!(cat.revision(), rev, "no mutation must mean no revision");
    assert!(
        cat.changes_since(rev).expect("in history").is_empty(),
        "and no history entry"
    );
}

#[test]
fn a_txn_delete_shares_the_transactions_revision() {
    let mut cat = ResourceCatalog::default();
    apply(
        &mut cat,
        &ResourceCommand::put(pod_key("gone"), pod("gone"), Reason::Operator),
        1,
    );
    let before = cat.revision();

    let out = apply(
        &mut cat,
        &txn(
            vec![],
            vec![
                put_op("kept"),
                TxnOp::Delete {
                    key: pod_key("gone"),
                    deletion_timestamp: None,
                },
            ],
            vec![],
        ),
        2,
    );

    assert_eq!(cat.revision().get(), before.get() + 1);
    assert!(cat.get(&pod_key("gone")).is_none(), "delete applied");
    assert!(cat.get(&pod_key("kept")).is_some(), "put applied");
    let n = out.change.iter().count() + out.extra_changes.len();
    assert_eq!(n, 2, "both the put and the delete are reported");
}
