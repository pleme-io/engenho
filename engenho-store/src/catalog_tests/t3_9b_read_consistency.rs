//! T3.9b, catalog level — a read under a `ReadConsistency` answers what the
//! store was at the revision it names, or refuses with a typed reason.
//!
//! | behaviour | why it is not an implementation detail |
//! |---|---|
//! | an exact read of revision `r` returns what a read of the present returned when the store WAS at `r`, for every scope, page size and key | a client that lists at `resourceVersionMatch=Exact` (or pages a series) is promised that past; serving the present is a 200 carrying the wrong data |
//! | a page series read at one exact revision is one snapshot, whatever lands between its pages | the continue-token series is a single LIST to the client; a write between pages must not tear it |
//! | a revision the store has not reached is `TooLarge{requested, current}`, for not-older-than and exact alike | upstream's 504; before T3.9b the store served its older present as if it were newer |
//! | an exact revision below the compaction floor is `Expired{requested, compacted}` | upstream's 410; the changes that would rebuild it are gone, so any answer would be a guess |
//! | not-older-than and latest reads are served from the present, floor or no floor | the present is not older than any past |
//!
//! The model is the store itself: after every revision the present is
//! captured through the latest read, and a later exact read of that revision
//! must reproduce it. No second implementation of rewinding exists to agree
//! with by accident.

use std::collections::BTreeMap;

use proptest::prelude::*;

use crate::command::{Reason, ResourceCommand};
use crate::read::{ReadConsistency, ReadRefused};
use crate::resource::{ListScope, ResourceKey, ResourceValue};
use crate::revision::Revision;
use crate::state::ResourceCatalog;

/// Kinds × namespaces × names: two scopes of each shape overlap nowhere but
/// share key prefixes, so a rewind that leaks one scope's changes into
/// another shows up.
fn universe() -> Vec<ResourceKey> {
    let mut keys = Vec::new();
    for kind in ["Pod", "ConfigMap"] {
        for ns in ["a", "b"] {
            for name in ["x", "y", "z"] {
                keys.push(ResourceKey::namespaced("", "v1", kind, ns, name));
            }
        }
    }
    keys.push(ResourceKey::cluster_scoped("", "v1", "Pod", "cluster-wide"));
    keys
}

fn scopes() -> [ListScope<'static>; 5] {
    [
        ListScope::new("", "v1", "Pod", None),
        ListScope::new("", "v1", "Pod", Some("a")),
        ListScope::new("", "v1", "Pod", Some("b")),
        ListScope::new("", "v1", "ConfigMap", None),
        ListScope::new("", "v1", "ConfigMap", Some("b")),
    ]
}

#[derive(Clone, Debug)]
enum Op {
    Put(usize, u8),
    Delete(usize),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (0usize..13, 0u8..4).prop_map(|(k, m)| Op::Put(k, m)),
        1 => (0usize..13).prop_map(Op::Delete),
    ]
}

fn apply(cat: &mut ResourceCatalog, keys: &[ResourceKey], op: &Op, index: u64) {
    let cmd = match op {
        Op::Put(k, marker) => ResourceCommand::put(
            keys[*k].clone(),
            serde_json::json!({ "metadata": { "name": keys[*k].name }, "spec": { "marker": marker } }),
            Reason::Operator,
        ),
        Op::Delete(k) => ResourceCommand::delete(keys[*k].clone(), Reason::Operator),
    };
    cat.apply(&cmd, 1, index);
}

/// The whole present, through the latest read of each scope in the universe.
fn present(cat: &ResourceCatalog) -> BTreeMap<ResourceKey, ResourceValue> {
    let mut all = BTreeMap::new();
    for kind in ["Pod", "ConfigMap"] {
        let page = cat
            .read_page(
                ListScope::new("", "v1", kind, None),
                None,
                0,
                ReadConsistency::Latest,
            )
            .expect("a latest read is never refused");
        all.extend(page.items);
    }
    all
}

fn in_scope(
    state: &BTreeMap<ResourceKey, ResourceValue>,
    scope: ListScope<'_>,
) -> Vec<(ResourceKey, ResourceValue)> {
    state
        .iter()
        .filter(|(k, _)| scope.contains(k))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Every page of `scope` at `consistency`, `limit` at a time, concatenated;
/// each page must report `revision`.
fn paged(
    cat: &ResourceCatalog,
    scope: ListScope<'_>,
    limit: usize,
    consistency: ReadConsistency,
    revision: Revision,
) -> Vec<(ResourceKey, ResourceValue)> {
    let mut out = Vec::new();
    let mut after: Option<ResourceKey> = None;
    loop {
        let page = cat
            .read_page(scope, after.as_ref(), limit, consistency)
            .expect("a served revision serves every page");
        assert_eq!(
            page.revision, revision,
            "every page reports the revision read"
        );
        out.extend(page.items);
        match page.next {
            Some(next) => after = Some(next),
            None => return out,
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn every_read_answers_the_revision_it_names_or_refuses(
        capacity in 1usize..10,
        ops in prop::collection::vec(op(), 1..40),
    ) {
        let keys = universe();
        let mut cat = ResourceCatalog::with_history_capacity(capacity);
        // history[r] = the present when the store was at revision r.
        let mut history = vec![present(&cat)];
        for (i, op) in ops.iter().enumerate() {
            apply(&mut cat, &keys, op, i as u64 + 1);
            if cat.revision().get() as usize == history.len() {
                history.push(present(&cat));
            }
            prop_assert_eq!(
                cat.revision().get() as usize + 1,
                history.len(),
                "a revision advances by one per real mutation"
            );
        }
        let head = cat.revision();
        let floor = cat.compacted_revision();

        for rv in 0..=head.get() + 2 {
            let at = Revision(rv);
            for scope in scopes() {
                // Exact.
                let exact = cat.read_page(scope, None, 0, ReadConsistency::Exact(at));
                if at > head {
                    prop_assert_eq!(
                        exact.err(),
                        Some(ReadRefused::TooLarge { requested: at, current: head })
                    );
                } else if at < floor {
                    prop_assert_eq!(
                        exact.err(),
                        Some(ReadRefused::Expired { requested: at, compacted: floor })
                    );
                } else {
                    let want = in_scope(&history[rv as usize], scope);
                    let page = exact.expect("served");
                    prop_assert_eq!(page.revision, at);
                    prop_assert_eq!((page.next, page.remaining), (None, 0));
                    prop_assert_eq!(
                        &page.items, &want,
                        "exact read of {:?} at {} (head {}, floor {})", scope, at, head, floor
                    );
                    for limit in 1..=3 {
                        prop_assert_eq!(
                            paged(&cat, scope, limit, ReadConsistency::Exact(at), at),
                            want.clone(),
                            "exact pages of {} at {}", limit, at
                        );
                    }
                }

                // Not older than: the present, once the store has reached it.
                let not_older = cat.read_page(scope, None, 0, ReadConsistency::NotOlderThan(at));
                if at > head {
                    prop_assert_eq!(
                        not_older.err(),
                        Some(ReadRefused::TooLarge { requested: at, current: head })
                    );
                } else {
                    let page = not_older.expect("served");
                    prop_assert_eq!(page.revision, head);
                    prop_assert_eq!(page.items, in_scope(&history[head.get() as usize], scope));
                }
            }

            // One key at a time.
            for key in &keys {
                let one = cat.read_one(key, ReadConsistency::Exact(at));
                if at > head {
                    prop_assert_eq!(
                        one.err(),
                        Some(ReadRefused::TooLarge { requested: at, current: head })
                    );
                } else if at < floor {
                    prop_assert_eq!(
                        one.err(),
                        Some(ReadRefused::Expired { requested: at, compacted: floor })
                    );
                } else {
                    prop_assert_eq!(
                        one.expect("served"),
                        history[rv as usize].get(key).cloned(),
                        "exact get of {:?} at {}", key, at
                    );
                }
            }
        }
    }
}

fn pod(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

fn put(cat: &mut ResourceCatalog, name: &str, marker: &str, index: u64) -> Revision {
    cat.apply(
        &ResourceCommand::put(
            pod(name),
            serde_json::json!({ "metadata": { "name": name }, "spec": { "marker": marker } }),
            Reason::Operator,
        ),
        1,
        index,
    );
    cat.revision()
}

fn marker(value: &ResourceValue) -> &str {
    value["spec"]["marker"].as_str().unwrap_or("")
}

fn pods() -> ListScope<'static> {
    ListScope::new("", "v1", "Pod", Some("default"))
}

/// The past is the past for every kind of change after it: a modified key
/// reads its old value, a deleted key is back, a created key is absent.
#[test]
fn an_exact_read_of_the_past_undoes_every_later_change() {
    let mut cat = ResourceCatalog::default();
    put(&mut cat, "kept", "old", 1);
    put(&mut cat, "deleted-later", "was-here", 2);
    let then = put(&mut cat, "modified-later", "before", 3);

    put(&mut cat, "modified-later", "after", 4);
    cat.apply(
        &ResourceCommand::delete(pod("deleted-later"), Reason::Operator),
        1,
        5,
    );
    put(&mut cat, "created-later", "new", 6);

    let page = cat
        .read_page(pods(), None, 0, ReadConsistency::Exact(then))
        .expect("revision 3 is retained");
    let seen: Vec<(&str, &str)> = page
        .items
        .iter()
        .map(|(k, v)| (k.name.as_str(), marker(v)))
        .collect();
    assert_eq!(
        seen,
        [
            ("deleted-later", "was-here"),
            ("kept", "old"),
            ("modified-later", "before"),
        ],
        "revision 3 as it was: nothing after it shows"
    );
    assert_eq!(page.revision, then, "the page reports the revision it read");

    assert_eq!(
        cat.read_one(&pod("created-later"), ReadConsistency::Exact(then)),
        Ok(None),
        "a key created after the revision did not exist at it"
    );
    assert_eq!(
        cat.read_one(&pod("modified-later"), ReadConsistency::Exact(then))
            .map(|v| v.map(|v| marker(&v).to_owned())),
        Ok(Some("before".to_owned()))
    );
}

/// A page series read at one exact revision is one snapshot: an insert after
/// the cursor, a delete after it and a modification after it, all landing
/// between two pages, leave the second page as it was.
#[test]
fn a_page_series_at_one_exact_revision_is_one_snapshot() {
    let mut cat = ResourceCatalog::default();
    for (i, name) in ["a", "b", "c", "d"].into_iter().enumerate() {
        put(&mut cat, name, "v1", i as u64 + 1);
    }
    let first_page_at = cat.revision();
    let whole = cat
        .read_page(pods(), None, 0, ReadConsistency::Exact(first_page_at))
        .expect("the head is always readable exactly");

    let first = cat
        .read_page(pods(), None, 2, ReadConsistency::Exact(first_page_at))
        .expect("served");
    assert_eq!(first.next, Some(pod("b")));

    // Between the pages: an insert after the cursor, a delete after it, a
    // modification after it.
    put(&mut cat, "bb", "v1", 5);
    cat.apply(&ResourceCommand::delete(pod("c"), Reason::Operator), 1, 6);
    put(&mut cat, "d", "v2", 7);
    assert!(
        cat.revision() > first_page_at,
        "precondition: the store moved on"
    );

    let second = cat
        .read_page(
            pods(),
            first.next.as_ref(),
            2,
            ReadConsistency::Exact(first_page_at),
        )
        .expect("the first page's revision is still retained");
    assert_eq!(second.revision, first_page_at);
    assert_eq!((second.next, second.remaining), (None, 0));

    let series: Vec<_> = first.items.into_iter().chain(second.items).collect();
    assert_eq!(
        series, whole.items,
        "the series is the list at the first page's revision, not a mix of two"
    );
}

/// A revision the store has not reached is refused for every read that
/// names one, and the refusal says where the store is.
#[test]
fn a_revision_the_store_has_not_reached_is_too_large() {
    let mut cat = ResourceCatalog::default();
    let head = put(&mut cat, "only", "v1", 1);
    let ahead = Revision(head.get() + 1);
    let too_large = ReadRefused::TooLarge {
        requested: ahead,
        current: head,
    };
    for consistency in [
        ReadConsistency::NotOlderThan(ahead),
        ReadConsistency::Exact(ahead),
    ] {
        assert_eq!(
            cat.read_page(pods(), None, 0, consistency).err(),
            Some(too_large),
            "{consistency:?} over a store at {head}"
        );
        assert_eq!(
            cat.read_one(&pod("only"), consistency).err(),
            Some(too_large),
            "{consistency:?} of one key over a store at {head}"
        );
    }
}

/// History the ring no longer holds is refused for an exact read, and only
/// for an exact read: the present still answers not-older-than.
#[test]
fn an_exact_revision_below_the_floor_is_expired() {
    let mut cat = ResourceCatalog::with_history_capacity(1);
    put(&mut cat, "p", "one", 1);
    let gone = cat.revision();
    put(&mut cat, "p", "two", 2);
    put(&mut cat, "p", "three", 3);
    let floor = cat.compacted_revision();
    assert!(gone < floor, "precondition: revision {gone} was evicted");

    let expired = ReadRefused::Expired {
        requested: gone,
        compacted: floor,
    };
    assert_eq!(
        cat.read_page(pods(), None, 0, ReadConsistency::Exact(gone))
            .err(),
        Some(expired)
    );
    assert_eq!(
        cat.read_one(&pod("p"), ReadConsistency::Exact(gone)).err(),
        Some(expired)
    );

    let page = cat
        .read_page(pods(), None, 0, ReadConsistency::NotOlderThan(gone))
        .expect("the present is not older than a compacted revision");
    assert_eq!(page.revision, cat.revision());
    assert_eq!(marker(&page.items[0].1), "three");
}
