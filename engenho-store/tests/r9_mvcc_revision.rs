//! R9 — MVCC revision model invariants for [`ResourceCatalog`].
//!
//! All tests are cargo-only: pure functions over the in-memory state
//! machine. No network, no filesystem, no Raft.
//!
//! Covers the load-bearing properties:
//!
//!   * I1 strict monotonicity — stamped revisions are strictly
//!     increasing with no gaps in `history`, across any interleaving.
//!   * I2 no-op neutrality — delete-not-found / patch-missing advance
//!     neither `current_revision` nor `history`.
//!   * I5 changes_since correctness + the CompactedTooOld boundary.
//!   * I6 determinism — identical command sequence ⇒ byte-identical
//!     serde output.

use engenho_store::command::{Reason, ResourceCommand};
use engenho_store::revision::{ChangeKind, Revision};
use engenho_store::{ResourceCatalog, ResourceKey, ResourceValue};
use proptest::prelude::*;

fn pod_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

fn put_cmd(key: ResourceKey, value: ResourceValue) -> ResourceCommand {
    ResourceCommand::put(key, value, Reason::Operator)
}

fn patch_cmd(key: ResourceKey, patch: ResourceValue) -> ResourceCommand {
    ResourceCommand::patch(key, patch, Reason::Operator)
}

fn delete_cmd(key: ResourceKey) -> ResourceCommand {
    ResourceCommand::delete(key, Reason::Operator)
}

// One generated op kind. 4 keys + 3 verbs keeps the state space dense
// enough to hit creates, replaces, patches-on-existing, patches-on-
// missing, deletes, and delete-not-found in the same run.
#[derive(Clone, Debug)]
enum Op {
    Put(usize),
    Patch(usize),
    Delete(usize),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    let key = 0usize..4;
    prop_oneof![
        key.clone().prop_map(Op::Put),
        key.clone().prop_map(Op::Patch),
        key.prop_map(Op::Delete),
    ]
}

fn apply_op(cat: &mut ResourceCatalog, op: &Op, index: u64) -> engenho_store::ApplyOutcome {
    let key = match op {
        Op::Put(k) | Op::Patch(k) | Op::Delete(k) => pod_key(&format!("p{k}")),
    };
    let cmd = match op {
        Op::Put(_) => put_cmd(key, serde_json::json!({"spec": {"i": index}})),
        Op::Patch(_) => patch_cmd(key, serde_json::json!({"spec": {"patched": index}})),
        Op::Delete(_) => delete_cmd(key),
    };
    cat.apply(&cmd, 1, index)
}

proptest! {
    /// I1 + I2 + the current_revision == non-noop-count property.
    ///
    /// Across any arbitrary interleaving of Put/Patch/Delete on any
    /// keys: the sequence of stamped revisions in `history` is
    /// strictly increasing by exactly 1 with no gaps, and
    /// `current_revision == count of non-NoOp ops`.
    #[test]
    fn revision_sequence_is_dense_and_monotonic(ops in prop::collection::vec(op_strategy(), 0..200)) {
        let mut cat = ResourceCatalog::default();
        let mut real_mutations = 0u64;
        let mut prev_revision = 0u64;

        for (i, op) in ops.iter().enumerate() {
            let raft_index = (i as u64) + 1;
            let outcome = apply_op(&mut cat, op, raft_index);
            match outcome.change {
                Some(change) => {
                    real_mutations += 1;
                    // I1: each real mutation advances by exactly 1.
                    prop_assert_eq!(change.revision.0, prev_revision + 1);
                    prev_revision = change.revision.0;
                    // The catalog's global counter tracks the stamp.
                    prop_assert_eq!(cat.revision(), change.revision);
                    // I3: for a Put (create/replace/patch), the live
                    // key's mod_revision == the change's revision. For
                    // a Delete the change carries the REMOVED key's
                    // pre-delete metadata (its last mod_revision), so
                    // that equality does not apply to the tombstone.
                    if change.kind == ChangeKind::Put {
                        prop_assert_eq!(change.version_meta.mod_revision, change.revision);
                    } else {
                        prop_assert!(change.version_meta.mod_revision < change.revision);
                    }
                }
                None => {
                    // I2: a no-op advances nothing.
                    prop_assert_eq!(outcome.op, engenho_store::ResourceOp::NoOp);
                    prop_assert_eq!(cat.revision().0, prev_revision);
                }
            }
        }

        // current_revision == count of non-NoOp ops.
        prop_assert_eq!(cat.revision(), Revision(real_mutations));

        // The history ring (when not compacted) is a dense, strictly
        // increasing 1..=real_mutations sequence with no gaps.
        prop_assert_eq!(cat.history.len() as u64, real_mutations);
        for (pos, change) in cat.history.iter().enumerate() {
            prop_assert_eq!(change.revision.0, (pos as u64) + 1);
        }
    }

    /// I6 determinism: two catalogs applying the identical command
    /// sequence produce byte-identical serde output.
    #[test]
    fn determinism_byte_identical_serde(ops in prop::collection::vec(op_strategy(), 0..120)) {
        let mut a = ResourceCatalog::default();
        let mut b = ResourceCatalog::default();
        for (i, op) in ops.iter().enumerate() {
            let raft_index = (i as u64) + 1;
            apply_op(&mut a, op, raft_index);
            apply_op(&mut b, op, raft_index);
        }
        let bytes_a = serde_json::to_vec(&a).unwrap();
        let bytes_b = serde_json::to_vec(&b).unwrap();
        prop_assert_eq!(bytes_a, bytes_b);
        // And the catalogs compare equal on the converged state.
        prop_assert_eq!(a, b);
    }
}

#[test]
fn noop_neutrality_explicit() {
    // I2: a delete-not-found and a patch-missing leave revision +
    // history untouched, regardless of surrounding real mutations.
    let mut cat = ResourceCatalog::default();
    let k = pod_key("real");
    cat.apply(&put_cmd(k.clone(), serde_json::json!({"spec": {}})), 1, 1); // rev 1
    assert_eq!(cat.revision(), Revision(1));
    assert_eq!(cat.history.len(), 1);

    // patch-missing
    let o = cat.apply(&patch_cmd(pod_key("ghost"), serde_json::json!({"x": 1})), 1, 2);
    assert_eq!(o.op, engenho_store::ResourceOp::NoOp);
    assert!(o.change.is_none());
    assert_eq!(cat.revision(), Revision(1));
    assert_eq!(cat.history.len(), 1);

    // delete-not-found
    let o = cat.apply(&delete_cmd(pod_key("ghost")), 1, 3);
    assert_eq!(o.op, engenho_store::ResourceOp::NoOp);
    assert!(o.change.is_none());
    assert_eq!(cat.revision(), Revision(1));
    assert_eq!(cat.history.len(), 1);
}

#[test]
fn changes_since_tail_in_order() {
    // I5: 5 puts on distinct keys (revs 1..5); changes_since(2) → 3,4,5.
    let mut cat = ResourceCatalog::default();
    for i in 1..=5u64 {
        cat.apply(
            &put_cmd(pod_key(&format!("p{i}")), serde_json::json!({"i": i})),
            1,
            i,
        );
    }
    let tail = cat.changes_since(Revision(2)).unwrap();
    let revs: Vec<u64> = tail.iter().map(|c| c.revision.0).collect();
    assert_eq!(revs, vec![3, 4, 5]);
    // Order is revision order.
    assert!(tail.windows(2).all(|w| w[0].revision < w[1].revision));
}

#[test]
fn changes_since_below_compacted_is_typed_error() {
    // I5: tiny capacity forces compaction; below-watermark → typed
    // CompactedTooOld (the 410 Gone equivalent).
    let mut cat = ResourceCatalog::with_history_capacity(2);
    for i in 1..=6u64 {
        cat.apply(
            &put_cmd(pod_key(&format!("p{i}")), serde_json::json!({"i": i})),
            1,
            i,
        );
    }
    // Only revs 5,6 retained; compacted at rev 4.
    assert_eq!(cat.history.len(), 2);
    assert_eq!(cat.compacted_revision(), Revision(4));

    let err = cat
        .changes_since(Revision(2))
        .expect_err("below compaction watermark must error");
    assert_eq!(err.requested, Revision(2));
    assert_eq!(err.compacted, Revision(4));
    assert_eq!(err.kind(), "compacted_too_old");

    // From the watermark (or above) is honored.
    assert_eq!(cat.changes_since(Revision(4)).unwrap().len(), 2);
    assert_eq!(cat.changes_since(Revision(5)).unwrap().len(), 1);
}

#[test]
fn delete_change_is_a_tombstone_with_prior() {
    // I4: the Delete change is a Delete-kind change whose value +
    // prior both carry the last-known object (never Null).
    let mut cat = ResourceCatalog::default();
    let k = pod_key("doomed");
    cat.apply(
        &put_cmd(
            k.clone(),
            serde_json::json!({"spec": {"image": "podinfo:6", "replicas": 3}}),
        ),
        1,
        1,
    );
    let outcome = cat.apply(&delete_cmd(k.clone()), 1, 2);
    let change = outcome.change.expect("delete of existing key emits a change");
    assert_eq!(change.kind, ChangeKind::Delete);
    assert!(!change.value.is_null());
    assert_eq!(
        change.value.get("spec").unwrap().get("image").unwrap(),
        "podinfo:6"
    );
    let prior = change.prior.expect("delete prior is the last-known object");
    assert_eq!(prior.get("spec").unwrap().get("replicas").unwrap(), 3);
}

#[test]
fn create_mod_version_semantics() {
    // I3: create_revision stable across replaces+patches; recreate
    // after delete yields a NEW create_revision; version +1/mutation.
    let mut cat = ResourceCatalog::default();
    let k = pod_key("svc");

    cat.apply(&put_cmd(k.clone(), serde_json::json!({"spec": {"v": 1}})), 1, 1); // rev 1
    let (_, m) = cat.get_with_meta(&k).unwrap();
    assert_eq!(m.create_revision, Revision(1));
    assert_eq!(m.version, 1);

    cat.apply(&put_cmd(k.clone(), serde_json::json!({"spec": {"v": 2}})), 1, 2); // rev 2 (replace)
    let (_, m) = cat.get_with_meta(&k).unwrap();
    assert_eq!(m.create_revision, Revision(1), "create_revision stable across replace");
    assert_eq!(m.mod_revision, Revision(2));
    assert_eq!(m.version, 2);

    cat.apply(&patch_cmd(k.clone(), serde_json::json!({"spec": {"w": 9}})), 1, 3); // rev 3 (patch)
    let (_, m) = cat.get_with_meta(&k).unwrap();
    assert_eq!(m.create_revision, Revision(1), "create_revision stable across patch");
    assert_eq!(m.mod_revision, Revision(3));
    assert_eq!(m.version, 3);

    cat.apply(&delete_cmd(k.clone()), 1, 4); // rev 4
    cat.apply(&put_cmd(k.clone(), serde_json::json!({"spec": {"v": 100}})), 1, 5); // rev 5 (recreate)
    let (_, m) = cat.get_with_meta(&k).unwrap();
    assert_eq!(m.create_revision, Revision(5), "recreate yields NEW create_revision");
    assert_eq!(m.mod_revision, Revision(5));
    assert_eq!(m.version, 1, "version resets on recreate");
}
