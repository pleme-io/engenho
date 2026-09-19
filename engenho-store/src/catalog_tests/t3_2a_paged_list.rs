//! T3.2a, catalog level — a scope's range is exactly the scope, and pages
//! concatenate to the unpaged list.
//!
//! | behaviour | pinned by |
//! |---|---|
//! | a scope's range is exactly the scope: nothing of another kind or namespace is visited | `a_scope_range_yields_exactly_its_keys` |
//! | pages concatenate to the unpaged list, for every limit | `pages_concatenate_to_the_unpaged_list` |
//! | a cursor from anywhere (a continue token's key comes from the client) resumes strictly after it and never panics | `a_cursor_from_anywhere_resumes_strictly_after_it` |
//!
//! The mesh-level half (a page allocates only its items; the mesh's pages
//! concatenate to its list on both backends) stays an integration test in
//! `tests/t3_2a_paged_list.rs`: it needs nothing but the public surface.
//! This half drives the catalog directly, which only the crate can name
//! since T3.2b sealed it.
//!
//! The adversarial alphabet below puts keys right next to a scope's edges:
//! kinds `Pod\0` and `Pod ` sort immediately after `Pod`, namespace
//! `default\0` immediately after `default`, and `None` (cluster-scoped)
//! before every namespace.

use std::collections::BTreeMap;

use crate::command::{Reason, ResourceCommand};
use crate::state::ResourceCatalog;
use crate::{ListScope, ResourceKey};
use proptest::prelude::*;
use proptest::sample::select;
use serde_json::json;

// =================================================================
// The adversarial key alphabet
// =================================================================

const GROUPS: &[&str] = &["", "apps"];
const VERSIONS: &[&str] = &["v1", "v1beta1"];
const KINDS: &[&str] = &["Po", "Pod", "Pod\0", "Pod\0\0", "Pod ", "PodX", "Pods"];
const NAMESPACES: &[Option<&str>] = &[
    None,
    Some(""),
    Some("\0"),
    Some("de"),
    Some("default"),
    Some("default\0"),
    Some("default-a"),
    Some("defaulu"),
];

fn name() -> impl Strategy<Value = String> {
    prop::collection::vec(select(vec!['\0', 'a', 'b']), 0..3).prop_map(String::from_iter)
}

fn key() -> impl Strategy<Value = ResourceKey> {
    (
        select(GROUPS),
        select(VERSIONS),
        select(KINDS),
        select(NAMESPACES),
        name(),
    )
        .prop_map(|(group, version, kind, namespace, name)| ResourceKey {
            group: group.to_owned(),
            version: version.to_owned(),
            kind: kind.to_owned(),
            namespace: namespace.map(str::to_owned),
            name,
        })
}

/// A LIST's scope as plain strings, so the oracle below never goes
/// through the type under test.
#[derive(Clone, Copy, Debug)]
struct Want {
    group: &'static str,
    version: &'static str,
    kind: &'static str,
    namespace: Option<&'static str>,
}

impl Want {
    fn scope(self) -> ListScope<'static> {
        ListScope::new(self.group, self.version, self.kind, self.namespace)
    }

    /// The oracle: the brute-force filter over every key that the unpaged
    /// LIST has always meant. `namespace: None` spans every namespace and
    /// the cluster-scoped keys.
    fn selects(self, k: &ResourceKey) -> bool {
        k.group == self.group
            && k.version == self.version
            && k.kind == self.kind
            && match self.namespace {
                None => true,
                Some(ns) => k.namespace.as_deref() == Some(ns),
            }
    }
}

fn want() -> impl Strategy<Value = Want> {
    (
        select(GROUPS),
        select(VERSIONS),
        select(KINDS),
        select(NAMESPACES),
    )
        .prop_map(|(group, version, kind, namespace)| Want {
            group,
            version,
            kind,
            namespace,
        })
}

fn catalog_of(keys: &[ResourceKey]) -> ResourceCatalog {
    let mut cat = ResourceCatalog::default();
    for (i, k) in keys.iter().enumerate() {
        cat.apply(
            &ResourceCommand::Put {
                key: k.clone(),
                value: json!({ "i": i }),
                expected: None,
                reason: Reason::Operator,
            },
            1,
            i as u64 + 1,
        );
    }
    cat
}

/// Every key of `cat` the oracle selects, in key order, strictly after
/// `after` when given.
fn oracle(cat: &ResourceCatalog, w: Want, after: Option<&ResourceKey>) -> Vec<ResourceKey> {
    cat.resources
        .keys()
        .filter(|k| w.selects(k))
        .filter(|k| after.is_none_or(|a| *k > a))
        .cloned()
        .collect()
}

// =================================================================
// Catalog-level properties
// =================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// The range IS the scope: it yields every key the oracle selects and
    /// nothing else, so serving a LIST never walks another kind. An upper
    /// bound left `Unbounded` yields the keys that sort after the scope and
    /// fails here, whatever filter a caller puts after it.
    #[test]
    fn a_scope_range_yields_exactly_its_keys(
        keys in prop::collection::vec(key(), 0..40),
        w in want(),
        cursor in prop::option::of(key()),
    ) {
        let map: BTreeMap<ResourceKey, usize> =
            keys.iter().cloned().enumerate().map(|(i, k)| (k, i)).collect();
        let yielded: Vec<ResourceKey> =
            w.scope().range(&map, cursor.as_ref()).map(|(k, _)| k.clone()).collect();
        let expected: Vec<ResourceKey> = map
            .keys()
            .filter(|k| w.selects(k))
            .filter(|k| cursor.as_ref().is_none_or(|c| *k > c))
            .cloned()
            .collect();
        prop_assert_eq!(yielded, expected);
    }

    /// Threading `next` through `list_page` visits the unpaged list exactly
    /// once, in order, for every limit; each page reports how many items
    /// follow it, and continues iff any do. The owned page a backend hands
    /// out matches the borrowed one and carries the catalog's revision.
    #[test]
    fn pages_concatenate_to_the_unpaged_list(
        keys in prop::collection::vec(key(), 0..40),
        w in want(),
        limit in 1usize..7,
    ) {
        let cat = catalog_of(&keys);
        let unpaged: Vec<ResourceKey> = cat
            .list(w.group, w.version, w.kind, w.namespace)
            .into_iter()
            .map(|(k, _)| k.clone())
            .collect();
        prop_assert_eq!(&unpaged, &oracle(&cat, w, None), "the unpaged list is the oracle's");

        let mut paged: Vec<ResourceKey> = Vec::new();
        let mut after: Option<ResourceKey> = None;
        for _ in 0..=unpaged.len() {
            let page = cat.list_page(w.group, w.version, w.kind, w.namespace, after.as_ref(), limit);
            let owned = cat.list_page_at_revision(w.scope(), after.as_ref(), limit);
            prop_assert_eq!(owned.revision, cat.revision());
            prop_assert_eq!(
                owned.items.iter().map(|(k, v)| (k, v)).collect::<Vec<_>>(),
                page.items.clone()
            );
            prop_assert_eq!(&owned.next, &page.next);
            prop_assert_eq!(owned.remaining, page.remaining);

            prop_assert!(page.items.len() <= limit, "a page never exceeds its limit");
            let emitted: Vec<ResourceKey> = page.items.iter().map(|(k, _)| (*k).clone()).collect();
            paged.extend(emitted.iter().cloned());
            let follow = oracle(&cat, w, emitted.last().or(after.as_ref())).len() as u64;
            prop_assert_eq!(page.remaining, follow, "remaining counts what follows the page");
            prop_assert_eq!(page.next.is_some(), follow > 0, "a page continues iff items follow");
            match page.next {
                Some(k) => {
                    prop_assert_eq!(Some(&k), emitted.last(), "the cursor is the last emitted key");
                    after = Some(k);
                }
                None => break,
            }
        }
        prop_assert_eq!(paged, unpaged);
    }

    /// A continue token's key comes from the client: it may name another
    /// kind, another namespace, or a key past the scope. Any of them reads
    /// the scope's keys strictly after it, never panicking.
    #[test]
    fn a_cursor_from_anywhere_resumes_strictly_after_it(
        keys in prop::collection::vec(key(), 0..40),
        w in want(),
        cursor in key(),
    ) {
        let cat = catalog_of(&keys);
        let page = cat.list_page(w.group, w.version, w.kind, w.namespace, Some(&cursor), 0);
        let got: Vec<ResourceKey> = page.items.iter().map(|(k, _)| (*k).clone()).collect();
        prop_assert_eq!(got, oracle(&cat, w, Some(&cursor)));
        prop_assert!(page.next.is_none());
        prop_assert_eq!(page.remaining, 0);
    }
}
