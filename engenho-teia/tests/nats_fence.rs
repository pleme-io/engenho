//! The NATS fence (docs/IMPROVEMENT-PLAN.md §5.1, T5.2).
//!
//! NATS is not engenho's fabric, so a default build of engenho-teia must not
//! be able to reach `async-nats`: it is optional, and only the `teia-nats`
//! feature switches it on. These tests read this crate's own manifest and
//! walk its `[features]` table the way cargo does, so they fail the moment
//! either half of the fence is undone: `async-nats` made unconditional again,
//! or `teia-nats` (or anything that enables it) added to `default`.
//!
//! Tier: CI-caught. This is a gate over the manifest, not a type. It pins
//! engenho-teia's own contract only; whether the engenho binary's closure is
//! free of async-nats also depends on engenho-store making engenho-teia
//! optional (the integration half of T5.2).

use std::collections::BTreeSet;

use toml::{Table, Value};

const MANIFEST: &str = include_str!("../Cargo.toml");
const NATS_CLIENT: &str = "async-nats";
const NATS_FEATURE: &str = "teia-nats";

fn manifest() -> Table {
    MANIFEST
        .parse::<Table>()
        .expect("engenho-teia/Cargo.toml parses as TOML")
}

fn table<'a>(m: &'a Table, key: &str) -> Option<&'a Table> {
    m.get(key).and_then(Value::as_table)
}

/// Dependencies that are always linked, whatever features are on.
fn unconditional_dependencies(m: &Table) -> BTreeSet<String> {
    table(m, "dependencies")
        .into_iter()
        .flatten()
        .filter(|(_, spec)| {
            !spec
                .as_table()
                .and_then(|t| t.get("optional"))
                .and_then(Value::as_bool)
                .unwrap_or(false)
        })
        .map(|(name, _)| name.clone())
        .collect()
}

/// Every optional dependency the `roots` features switch on, following
/// feature-to-feature edges transitively. Mirrors cargo's grammar:
/// `dep:x` enables `x`; `x/f` enables `x` (while `x?/f` does not); a bare
/// name is a feature when `[features]` declares it, otherwise the implicit
/// feature of the optional dependency `x`.
fn optional_dependencies_enabled_by(m: &Table, roots: &[&str]) -> BTreeSet<String> {
    let features = table(m, "features");
    let mut stack: Vec<String> = roots.iter().map(|r| (*r).to_owned()).collect();
    let mut visited = BTreeSet::new();
    let mut enabled = BTreeSet::new();
    while let Some(feature) = stack.pop() {
        if !visited.insert(feature.clone()) {
            continue;
        }
        let members = features
            .and_then(|f| f.get(&feature))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str);
        for member in members {
            if let Some(dep) = member.strip_prefix("dep:") {
                enabled.insert(dep.to_owned());
            } else if let Some((head, _)) = member.split_once('/') {
                if !head.ends_with('?') {
                    enabled.insert(head.to_owned());
                }
            } else if features.is_some_and(|f| f.contains_key(member)) {
                stack.push(member.to_owned());
            } else {
                enabled.insert(member.to_owned());
            }
        }
    }
    enabled
}

#[test]
fn async_nats_is_not_an_unconditional_dependency() {
    let m = manifest();
    assert!(
        !unconditional_dependencies(&m).contains(NATS_CLIENT),
        "{NATS_CLIENT} is linked into every build of engenho-teia again; \
         it must be `optional = true` and reachable only through `{NATS_FEATURE}`"
    );
}

#[test]
fn default_features_do_not_reach_async_nats() {
    let m = manifest();
    let reached = optional_dependencies_enabled_by(&m, &["default"]);
    assert!(
        !reached.contains(NATS_CLIENT),
        "the default feature set reaches {NATS_CLIENT} (enabled: {reached:?}); \
         `{NATS_FEATURE}` must stay off by default"
    );
}

/// Positive control: the walk above is not vacuous. The same walk from
/// `teia-nats` does reach the client, so an empty answer for `default`
/// means the fence holds, not that the walk sees nothing.
#[test]
fn teia_nats_is_the_feature_that_reaches_async_nats() {
    let m = manifest();
    let reached = optional_dependencies_enabled_by(&m, &[NATS_FEATURE]);
    assert!(
        reached.contains(NATS_CLIENT),
        "`{NATS_FEATURE}` must enable {NATS_CLIENT}; it enables {reached:?}"
    );
}
