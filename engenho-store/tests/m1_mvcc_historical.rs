//! M1 — historical (point-in-time) reads and explicit compaction.
//!
//! Cargo-only: pure functions over the in-memory state machine. No
//! network, no filesystem, no Raft.
//!
//! These close the two gaps that made etcd `Range{revision: N}` and
//! `Compact` unimplementable. Invariants:
//!
//!   * H1 value exactness — the value at a past revision is byte-identical
//!     to what a reader at that revision would have seen, for creates,
//!     modifications and deletions alike.
//!   * H2 creation boundary — a key created AFTER the requested revision
//!     is absent at it, not present-with-an-old-value.
//!   * H3 resurrection — a key deleted after the requested revision is
//!     PRESENT at it. This is the direction a naive "read the live map"
//!     implementation silently gets wrong.
//!   * H4 the compaction boundary is refused, not approximated.
//!   * H5 metadata fidelity is declared — exact where the change at or
//!     before the revision is retained, an explicit floor otherwise. A
//!     guessed mod_revision is worse than a declared one.
//!   * C1 compact advances the watermark and only ever forwards.
//!   * C2 compaction reclaims HISTORY, never DATA — every live key is
//!     still readable at the current revision afterwards.

use engenho_store::command::{Reason, ResourceCommand};
use engenho_store::revision::Revision;
use engenho_store::state::MetaFidelity;
use engenho_store::{ResourceCatalog, ResourceKey, ResourceValue};

fn key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

fn pod(name: &str, marker: &str) -> ResourceValue {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": name, "namespace": "default" },
        "spec": { "marker": marker }
    })
}

fn put(cat: &mut ResourceCatalog, name: &str, marker: &str, idx: u64) -> Revision {
    cat.apply(
        &ResourceCommand::put(key(name), pod(name, marker), Reason::Operator),
        1,
        idx,
    );
    cat.revision()
}

fn del(cat: &mut ResourceCatalog, name: &str, idx: u64) -> Revision {
    cat.apply(
        &ResourceCommand::delete(key(name), Reason::Operator),
        1,
        idx,
    );
    cat.revision()
}

fn marker_at(cat: &ResourceCatalog, name: &str, rev: Revision) -> Option<String> {
    cat.state_at(rev)
        .expect("within the retained window")
        .get(&key(name))
        .map(|e| e.value["spec"]["marker"].as_str().unwrap_or("").to_string())
}

// ── H1 ────────────────────────────────────────────────────────────────

#[test]
fn h1_a_past_value_is_the_value_a_reader_then_would_have_seen() {
    let mut cat = ResourceCatalog::default();
    let r1 = put(&mut cat, "p", "one", 1);
    let r2 = put(&mut cat, "p", "two", 2);
    let r3 = put(&mut cat, "p", "three", 3);

    assert_eq!(marker_at(&cat, "p", r1).as_deref(), Some("one"));
    assert_eq!(marker_at(&cat, "p", r2).as_deref(), Some("two"));
    assert_eq!(marker_at(&cat, "p", r3).as_deref(), Some("three"));
    // And the live read still agrees with the newest revision.
    assert_eq!(cat.get(&key("p")).unwrap()["spec"]["marker"], "three");
}

// ── H2 ────────────────────────────────────────────────────────────────

#[test]
fn h2_a_key_created_after_the_revision_is_absent_at_it() {
    let mut cat = ResourceCatalog::default();
    let before = put(&mut cat, "early", "x", 1);
    put(&mut cat, "late", "y", 2);

    let at = cat.state_at(before).expect("retained");
    assert!(at.contains_key(&key("early")));
    assert!(
        !at.contains_key(&key("late")),
        "a key created later must not appear at an earlier revision"
    );
}

// ── H3 — the direction a live-map read gets wrong ─────────────────────

#[test]
fn h3_a_key_deleted_later_is_still_present_at_the_earlier_revision() {
    let mut cat = ResourceCatalog::default();
    let alive = put(&mut cat, "doomed", "still-here", 1);
    del(&mut cat, "doomed", 2);

    assert!(
        cat.get(&key("doomed")).is_none(),
        "gone from the live map, as expected"
    );
    assert_eq!(
        marker_at(&cat, "doomed", alive).as_deref(),
        Some("still-here"),
        "but a reader at the earlier revision must still see it"
    );
}

// ── H4 ────────────────────────────────────────────────────────────────

#[test]
fn h4_a_revision_below_the_watermark_is_refused_not_approximated() {
    // A tiny ring forces the boundary without waiting.
    let mut cat = ResourceCatalog::with_history_capacity(2);
    put(&mut cat, "a", "1", 1);
    put(&mut cat, "b", "2", 2);
    put(&mut cat, "c", "3", 3);
    put(&mut cat, "d", "4", 4);

    let compacted = cat.compacted_revision;
    assert!(compacted.get() > 0, "the ring must have evicted something");

    let err = cat
        .state_at(Revision(compacted.get() - 1))
        .expect_err("a read below the watermark must be refused");
    assert_eq!(err.compacted, compacted);
}

// ── H5 ────────────────────────────────────────────────────────────────

#[test]
fn h5_metadata_fidelity_is_declared_not_guessed() {
    let mut cat = ResourceCatalog::default();
    let r1 = put(&mut cat, "p", "one", 1);
    put(&mut cat, "p", "two", 2);

    let e = cat
        .get_at(&key("p"), r1)
        .expect("retained")
        .expect("present");
    // The change AT r1 is retained, so the metadata is exact.
    assert_eq!(e.fidelity, MetaFidelity::Exact);
    assert_eq!(e.meta.mod_revision, r1);
}

// ── C1 / C2 ───────────────────────────────────────────────────────────

#[test]
fn c1_compact_advances_the_watermark_and_only_forwards() {
    let mut cat = ResourceCatalog::default();
    put(&mut cat, "a", "1", 1);
    let mid = put(&mut cat, "b", "2", 2);
    put(&mut cat, "c", "3", 3);

    assert_eq!(cat.compact(mid), mid, "watermark moves to the target");
    assert_eq!(cat.compacted_revision, mid);

    // Backwards is a no-op, not a rewind: rewinding would promise history
    // that has already been dropped.
    assert_eq!(cat.compact(Revision(1)), mid);
    assert_eq!(cat.compacted_revision, mid);

    // Beyond the present is clamped — you cannot compact away revisions
    // that do not exist yet.
    let now = cat.revision();
    assert_eq!(cat.compact(Revision(now.get() + 100)), now);
}

#[test]
fn c2_compaction_reclaims_history_never_data() {
    let mut cat = ResourceCatalog::default();
    put(&mut cat, "a", "1", 1);
    put(&mut cat, "b", "2", 2);
    let now = put(&mut cat, "c", "3", 3);

    cat.compact(now);

    // Every live key is still readable at the CURRENT revision — the live
    // map keeps one version per key and compaction never touches it.
    for name in ["a", "b", "c"] {
        assert!(
            cat.get(&key(name)).is_some(),
            "{name} must survive compaction"
        );
    }
    assert!(cat.state_at(now).is_ok(), "the present is always readable");

    // Only the ability to read BEFORE the watermark is gone.
    assert!(cat.state_at(Revision(1)).is_err());
}
