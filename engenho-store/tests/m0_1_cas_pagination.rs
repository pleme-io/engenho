//! M0.1 item 5 — optimistic concurrency (CAS) + range pagination.
//!
//! Pure-function tests over the in-memory [`ResourceCatalog`] (no
//! network, no filesystem, no Raft). CAS and pagination are the two
//! halves of one invariant: a store mutation/read is a function of
//! (snapshot revision, expected revision) — the revision is the
//! optimistic-concurrency and consistent-pagination token alike.
//!
//! Load-bearing properties asserted here:
//!
//!   * CAS conflict leaves the catalog BYTE-IDENTICAL (no mutation, no
//!     revision advance, no history entry) — proven via Clone + PartialEq
//!     and serde_json byte-equality.
//!   * CAS success advances the revision + the per-key mod_revision.
//!   * Pagination is gap/dup-free + stable across the page series.
//!   * The continue token round-trips + rejects tamper/truncation/
//!     wrong-version.
//!   * The page series is pinned to the token's snapshot revision.

use engenho_store::command::{Reason, ResourceCommand, ResourceOp};
use engenho_store::pagination::ContinueToken;
use engenho_store::revision::Revision;
use engenho_store::{ResourceCatalog, ResourceKey, ResourceValue};

fn pod_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

fn put_with(key: ResourceKey, value: ResourceValue, expected: Option<Revision>) -> ResourceCommand {
    ResourceCommand::Put {
        key,
        value,
        expected,
        reason: Reason::Operator,
    }
}

fn patch_with(key: ResourceKey, patch: ResourceValue, expected: Option<Revision>) -> ResourceCommand {
    ResourceCommand::Patch {
        key,
        patch,
        expected,
        reason: Reason::Operator,
    }
}

fn delete_with(key: ResourceKey, expected: Option<Revision>) -> ResourceCommand {
    ResourceCommand::Delete {
        key,
        expected,
        reason: Reason::Operator,
    }
}

/// Apply an unconditional Put helper (sets up a key at a known rev).
fn seed_pod(cat: &mut ResourceCatalog, name: &str, idx: u64) {
    let out = cat.apply(
        &put_with(pod_key(name), serde_json::json!({"spec": {}}), None),
        1,
        idx,
    );
    assert_eq!(out.op, ResourceOp::Created);
}

// =================================================================
// CAS — optimistic concurrency
// =================================================================

#[test]
fn cas_stale_rv_conflicts_and_leaves_state_unchanged() {
    // For Put, Patch, AND Delete: a stale expected rv → Conflict, and the
    // catalog is byte-identical before and after.
    for variant in ["put", "patch", "delete"] {
        let mut cat = ResourceCatalog::default();
        let k = pod_key("podinfo");
        seed_pod(&mut cat, "podinfo", 1); // rev 1, mod_revision 1
        assert_eq!(cat.revision(), Revision(1));

        // Snapshot the resource-state-relevant projection. NOTE: a
        // conflicting apply still advances `last_applied_index` (the Raft
        // log position — the entry WAS applied, it just changed nothing),
        // which is correct + required for read-after-write/replay. The
        // "byte-identical" guarantee the spec asserts is over the RESOURCE
        // STATE: resources + current_revision + history. We snapshot those
        // before, and normalize last_applied to compare the full serde
        // bytes too.
        let snapshot_resources = cat.resources.clone();
        let snapshot_history: Vec<_> = cat.history.iter().cloned().collect();
        let snapshot_rev = cat.revision();
        // A clone with last_applied pre-advanced to the index the conflict
        // will land on — everything else must match byte-for-byte.
        let mut expected_after = cat.clone();
        expected_after.last_applied_index = 2;
        expected_after.last_applied_term = 1;
        let expected_bytes = serde_json::to_vec(&expected_after).unwrap();

        let cmd = match variant {
            "put" => put_with(k.clone(), serde_json::json!({"spec": {"x": 1}}), Some(Revision(99))),
            "patch" => patch_with(k.clone(), serde_json::json!({"spec": {"x": 1}}), Some(Revision(99))),
            "delete" => delete_with(k.clone(), Some(Revision(99))),
            _ => unreachable!(),
        };
        let out = cat.apply(&cmd, 1, 2);

        assert_eq!(out.op, ResourceOp::Conflict, "{variant}: stale rv → Conflict");
        assert!(out.change.is_none(), "{variant}: conflict carries no Change");
        assert_eq!(cat.revision(), snapshot_rev, "{variant}: revision UNCHANGED");
        assert_eq!(cat.revision(), Revision(1), "{variant}: revision still 1");
        assert_eq!(cat.resources, snapshot_resources, "{variant}: resources UNCHANGED");
        assert_eq!(
            cat.history.iter().cloned().collect::<Vec<_>>(),
            snapshot_history,
            "{variant}: history UNCHANGED (no new entry)"
        );
        // Byte-identical resource state (last_applied normalized away — it
        // is the Raft position, not resource state).
        assert_eq!(
            serde_json::to_vec(&cat).unwrap(),
            expected_bytes,
            "{variant}: catalog byte-identical modulo Raft position"
        );
    }
}

#[test]
fn cas_current_rv_succeeds_and_advances() {
    let mut cat = ResourceCatalog::default();
    let k = pod_key("podinfo");
    seed_pod(&mut cat, "podinfo", 1); // rev 1, mod_revision 1

    // Put with expected = current mod_revision (1) → succeeds, advances.
    let out = cat.apply(
        &put_with(k.clone(), serde_json::json!({"spec": {"x": 1}}), Some(Revision(1))),
        1,
        2,
    );
    assert_eq!(out.op, ResourceOp::Replaced);
    assert_eq!(cat.revision(), Revision(2));
    let (_, meta) = cat.get_with_meta(&k).unwrap();
    assert_eq!(meta.mod_revision, Revision(2));

    // Patch with the NEW current mod_revision (2) → succeeds → rev 3.
    let out = cat.apply(
        &patch_with(k.clone(), serde_json::json!({"spec": {"y": 2}}), Some(Revision(2))),
        1,
        3,
    );
    assert_eq!(out.op, ResourceOp::Patched);
    assert_eq!(cat.revision(), Revision(3));

    // Delete with the NEW current mod_revision (3) → succeeds → rev 4.
    let out = cat.apply(&delete_with(k.clone(), Some(Revision(3))), 1, 4);
    assert_eq!(out.op, ResourceOp::Deleted);
    assert_eq!(cat.revision(), Revision(4));
    assert!(cat.get(&k).is_none());
}

#[test]
fn cas_absent_rv_is_unconditional() {
    // expected = None on an existing key succeeds + advances regardless of
    // the stored mod_revision.
    let mut cat = ResourceCatalog::default();
    let k = pod_key("podinfo");
    seed_pod(&mut cat, "podinfo", 1);
    cat.apply(&put_with(k.clone(), serde_json::json!({"spec": {"x": 1}}), None), 1, 2); // rev 2
    cat.apply(&put_with(k.clone(), serde_json::json!({"spec": {"x": 2}}), None), 1, 3); // rev 3
    assert_eq!(cat.revision(), Revision(3));
    let (_, meta) = cat.get_with_meta(&k).unwrap();
    assert_eq!(meta.mod_revision, Revision(3));
}

#[test]
fn cas_expected_on_missing_key_conflicts() {
    // expected = Some(_) on an ABSENT key → Conflict (cannot CAS a
    // non-existent object), NOT a create — for all three variants. The
    // revision stays at 0 and the catalog stays empty.
    for variant in ["put", "patch", "delete"] {
        let mut cat = ResourceCatalog::default();
        let k = pod_key("ghost");
        assert_eq!(cat.revision(), Revision(0));

        let cmd = match variant {
            "put" => put_with(k.clone(), serde_json::json!({"spec": {}}), Some(Revision(1))),
            "patch" => patch_with(k.clone(), serde_json::json!({"spec": {}}), Some(Revision(1))),
            "delete" => delete_with(k.clone(), Some(Revision(1))),
            _ => unreachable!(),
        };
        let out = cat.apply(&cmd, 1, 1);
        assert_eq!(out.op, ResourceOp::Conflict, "{variant}: CAS on missing key → Conflict");
        assert!(out.change.is_none());
        assert_eq!(cat.revision(), Revision(0), "{variant}: revision unchanged");
        assert!(cat.is_empty(), "{variant}: catalog stays empty (no create)");
    }
}

#[test]
fn cas_conflict_is_deterministic() {
    // Two catalogs replay the IDENTICAL command sequence (including a
    // conflicting CAS); the resulting catalogs are byte-identical
    // (replica-divergence guard — the CAS decision is deterministic).
    let seq = [
        put_with(pod_key("a"), serde_json::json!({"spec": {"v": 1}}), None), // rev 1
        put_with(pod_key("a"), serde_json::json!({"spec": {"v": 2}}), Some(Revision(1))), // rev 2 ok
        // Conflicting CAS: expected the now-stale rev 1 → Conflict, no advance.
        put_with(pod_key("a"), serde_json::json!({"spec": {"v": 3}}), Some(Revision(1))),
        put_with(pod_key("b"), serde_json::json!({"spec": {"v": 9}}), None), // rev 3
        delete_with(pod_key("a"), Some(Revision(99))), // Conflict (stale) → no advance
    ];

    let mut cat1 = ResourceCatalog::default();
    let mut cat2 = ResourceCatalog::default();
    let mut ops1 = Vec::new();
    let mut ops2 = Vec::new();
    for (i, cmd) in seq.iter().enumerate() {
        ops1.push(cat1.apply(cmd, 1, (i + 1) as u64).op);
        ops2.push(cat2.apply(cmd, 1, (i + 1) as u64).op);
    }
    assert_eq!(ops1, ops2, "op sequence identical across replicas");
    assert_eq!(
        ops1,
        vec![
            ResourceOp::Created,
            ResourceOp::Replaced,
            ResourceOp::Conflict,
            ResourceOp::Created,
            ResourceOp::Conflict,
        ]
    );
    assert_eq!(
        serde_json::to_vec(&cat1).unwrap(),
        serde_json::to_vec(&cat2).unwrap(),
        "byte-identical catalogs (no replica divergence)"
    );
    assert_eq!(cat1.revision(), Revision(3), "only 3 real mutations advanced");
}

// =================================================================
// Pagination — range paging
// =================================================================

/// Insert `n` pods named p00..p{n-1} (zero-padded so key order == numeric
/// order) into a fresh catalog.
fn cat_with_pods(n: usize) -> ResourceCatalog {
    let mut cat = ResourceCatalog::default();
    for i in 0..n {
        let name = format!("p{i:02}");
        cat.apply(
            &put_with(pod_key(&name), serde_json::json!({"i": i}), None),
            1,
            (i + 1) as u64,
        );
    }
    cat
}

fn page_names(page: &engenho_store::pagination::ListPage<'_>) -> Vec<String> {
    page.items.iter().map(|(k, _)| k.name.clone()).collect()
}

#[test]
fn pagination_stable_ordered_no_dup_no_gap() {
    // 10 pods; page with limit=3 repeatedly threading `next`; assert all
    // 10 covered in key order with zero dup + zero gap.
    let cat = cat_with_pods(10);
    let mut collected: Vec<String> = Vec::new();
    let mut after: Option<ResourceKey> = None;

    loop {
        let page = cat.list_page("", "v1", "Pod", Some("default"), after.as_ref(), 3);
        collected.extend(page_names(&page));
        match page.next {
            Some(k) => after = Some(k),
            None => break,
        }
        assert!(collected.len() <= 10, "no overrun");
    }

    // Full set, in order, no dup, no gap.
    let mut expected: Vec<String> = (0..10).map(|i| format!("p{i:02}")).collect();
    assert_eq!(collected.len(), 10, "exactly 10 items across the series");
    assert_eq!(collected, expected, "key order, no dup, no gap");
    expected.sort();
    assert_eq!(collected, expected, "already sorted");
}

#[test]
fn pagination_limit_exceeds_set_returns_all_no_continue() {
    let cat = cat_with_pods(10);
    let page = cat.list_page("", "v1", "Pod", Some("default"), None, 100);
    assert_eq!(page.items.len(), 10);
    assert!(page.next.is_none(), "limit > set → no continue");
    assert_eq!(page.remaining, 0);
}

#[test]
fn pagination_remaining_count_accurate() {
    let cat = cat_with_pods(10);
    let page = cat.list_page("", "v1", "Pod", Some("default"), None, 3);
    assert_eq!(page.items.len(), 3);
    assert_eq!(page.remaining, 7, "10 - 3 = 7 remaining after first page");
    assert!(page.next.is_some());
    assert_eq!(page.next.as_ref().unwrap().name, "p02", "next cursor = last emitted key");
}

#[test]
fn pagination_exact_multiple_has_no_trailing_empty_page() {
    // 6 items, limit 3 → two full pages; the SECOND page must have
    // next == None (no spurious trailing empty page).
    let cat = cat_with_pods(6);
    let page1 = cat.list_page("", "v1", "Pod", Some("default"), None, 3);
    assert_eq!(page_names(&page1), vec!["p00", "p01", "p02"]);
    assert_eq!(page1.remaining, 3);
    let cursor = page1.next.clone().expect("more after first page");

    let page2 = cat.list_page("", "v1", "Pod", Some("default"), Some(&cursor), 3);
    assert_eq!(page_names(&page2), vec!["p03", "p04", "p05"]);
    assert_eq!(page2.remaining, 0, "nothing after the exact-fill final page");
    assert!(page2.next.is_none(), "no continuation on the exact-fill final page");
}

#[test]
fn pagination_limit_zero_is_unbounded() {
    let cat = cat_with_pods(5);
    let page = cat.list_page("", "v1", "Pod", Some("default"), None, 0);
    assert_eq!(page.items.len(), 5, "limit 0 = all items");
    assert!(page.next.is_none());
    assert_eq!(page.remaining, 0);
}

#[test]
fn continue_token_round_trips_and_rejects_tamper() {
    let token = ContinueToken::new(Revision(7), pod_key("p03"));
    let encoded = token.encode();
    let back = ContinueToken::decode(&encoded).unwrap();
    assert_eq!(back, token, "encode → decode is identity");

    // Flip one hex nibble in the body region → ContinueInvalid.
    let mut chars: Vec<char> = encoded.chars().collect();
    let pos = 22; // past version(2) + digest(16) hex chars
    chars[pos] = if chars[pos] == 'a' { 'b' } else { 'a' };
    let flipped: String = chars.into_iter().collect();
    assert!(ContinueToken::decode(&flipped).is_err(), "flipped nibble rejected");

    // Truncate → ContinueInvalid.
    assert!(ContinueToken::decode(&encoded[..encoded.len() - 6]).is_err(), "truncation rejected");

    // Wrong version tag → ContinueInvalid (reason bad_version).
    let mut raw_token = ContinueToken::new(Revision(1), pod_key("p00"));
    let _ = &mut raw_token;
    let with_bad_tag = {
        // Construct: take a valid token, decode the hex, bump byte 0,
        // re-hex (digest will still match the body, but version tag is
        // wrong → bad_version fires first).
        let valid = ContinueToken::new(Revision(1), pod_key("p00")).encode();
        let mut bytes = hex_decode(&valid);
        bytes[0] = bytes[0].wrapping_add(7);
        hex_encode(&bytes)
    };
    let err = ContinueToken::decode(&with_bad_tag).unwrap_err();
    assert_eq!(err.reason, "bad_version");
}

// Local hex helpers mirroring the store's internal map (kept here so the
// test doesn't depend on private fns).
fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(s: &str) -> Vec<u8> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len() / 2);
    let nib = |c: u8| match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        _ => panic!("non-hex in test fixture"),
    };
    for pair in b.chunks_exact(2) {
        out.push((nib(pair[0]) << 4) | nib(pair[1]));
    }
    out
}

#[test]
fn pagination_series_pinned_to_snapshot_rev() {
    // Start a limit=3 series, capture the snapshot rev, insert a new pod
    // BETWEEN pages, and assert the continued pages (read at the token's
    // snapshot rev via the mesh's list_page_at_revision in the HTTP tests)
    // still reflect a consistent key set. Here at the catalog layer we
    // assert: a continue token captured at snapshot rv N, and that token's
    // snapshot_rev never changes as we thread it; the new pod that sorts
    // INTO an already-passed range does not retroactively appear.
    let mut cat = cat_with_pods(6); // p00..p05, rev 6
    let snapshot_rev = cat.revision();
    assert_eq!(snapshot_rev, Revision(6));

    // First page: p00,p01,p02. Build a continue token pinned to snapshot.
    let page1 = cat.list_page("", "v1", "Pod", Some("default"), None, 3);
    assert_eq!(page_names(&page1), vec!["p00", "p01", "p02"]);
    let cursor = page1.next.clone().unwrap();
    let token = ContinueToken::new(snapshot_rev, cursor);

    // Insert a NEW pod that sorts BEFORE the cursor ("p00a") — it would
    // appear on page 1 if we re-listed from the start, but a continue from
    // the cursor (strictly after p02) must NOT surface it.
    cat.apply(
        &put_with(pod_key("p00a"), serde_json::json!({"new": true}), None),
        1,
        7,
    );
    // The token's snapshot_rev is immutable across the series.
    let decoded = ContinueToken::decode(&token.encode()).unwrap();
    assert_eq!(decoded.snapshot_rev, snapshot_rev, "snapshot rev pinned");

    // Resume strictly after the cursor → p03,p04,p05 (the inserted p00a,
    // which sorts before the cursor, is NOT in the continued range).
    let page2 = cat.list_page(
        "",
        "v1",
        "Pod",
        Some("default"),
        Some(&decoded.last_key),
        3,
    );
    let names = page_names(&page2);
    assert!(!names.contains(&"p00a".to_string()), "pre-cursor insert not retroactively paged");
    assert_eq!(names, vec!["p03", "p04", "p05"], "continued range is contiguous after cursor");
    assert!(page2.next.is_none(), "last page has no continuation");
}
