//! Property: linhagem-aberta DAG invariants.

use engenho_substrate::{Fingerprint, LineageGraph, impl_fingerprint};
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sample {
    pub id: u64,
    pub note: String,
}
impl_fingerprint!(Sample);

fn s(id: u64) -> Sample {
    Sample {
        id,
        note: format!("v{id}"),
    }
}

/// Build a linear chain of `len` Samples, return graph + last fingerprint.
fn chain(len: usize) -> (LineageGraph<Sample>, Vec<[u8; 32]>) {
    let mut g = LineageGraph::new();
    let mut fps = Vec::with_capacity(len);
    let mut prev: BTreeSet<[u8; 32]> = BTreeSet::new();
    for i in 0..len {
        let fp = g.append(s(i as u64), prev.clone()).unwrap();
        fps.push(fp);
        prev = std::iter::once(fp).collect();
    }
    (g, fps)
}

proptest_with_env! {
    /// Same value → same fingerprint (deterministic content addressing).
    #[test]
    fn fingerprint_is_deterministic(id in 0u64..1000) {
        let a = s(id);
        let b = s(id);
        prop_assert_eq!(a.fingerprint(), b.fingerprint());
    }

    /// Distinct values → distinct fingerprints (BLAKE3 collision-resistant
    /// in practice; this is testing serde determinism).
    #[test]
    fn distinct_values_distinct_fingerprints(
        id1 in 0u64..500,
        id2 in 500u64..1000,
    ) {
        let a = s(id1);
        let b = s(id2);
        prop_assert_ne!(a.fingerprint(), b.fingerprint());
    }

    /// Empty causes → node is registered as a root.
    #[test]
    fn empty_causes_makes_root(id in 0u64..1000) {
        let mut g = LineageGraph::new();
        let fp = g.append(s(id), BTreeSet::new()).unwrap();
        prop_assert!(g.roots.contains(&fp));
        prop_assert_eq!(g.len(), 1);
    }

    /// Appending a duplicate value errors and leaves graph unchanged.
    #[test]
    fn duplicate_append_is_no_op(id in 0u64..1000) {
        let mut g = LineageGraph::new();
        g.append(s(id), BTreeSet::new()).unwrap();
        let snapshot_len = g.len();
        let snapshot_hash = g.root_hash();
        let err = g.append(s(id), BTreeSet::new()).unwrap_err();
        prop_assert_eq!(err.to_string(), format!("duplicate node: {}", hex8(&s(id).fingerprint())));
        prop_assert_eq!(g.len(), snapshot_len);
        prop_assert_eq!(g.root_hash(), snapshot_hash);
    }

    /// Unknown cause → error, graph unchanged.
    #[test]
    fn unknown_cause_errors(id in 0u64..1000, fake_cause in any::<[u8; 32]>()) {
        let mut g = LineageGraph::new();
        g.append(s(0), BTreeSet::new()).unwrap();
        let snapshot_len = g.len();
        let unknown: BTreeSet<_> = std::iter::once(fake_cause).collect();
        // Filter out the (extremely unlikely) collision case.
        if g.contains(&fake_cause) {
            return Ok(());
        }
        let err = g.append(s(id + 1), unknown).unwrap_err();
        prop_assert!(err.to_string().starts_with("unknown cause"));
        prop_assert_eq!(g.len(), snapshot_len);
    }

    /// ancestors() includes the target itself.
    #[test]
    fn ancestors_includes_target(len in 1usize..16) {
        let (g, fps) = chain(len);
        let target = fps[len - 1];
        let ancestors = g.ancestors(target).unwrap();
        prop_assert!(ancestors.contains(&target));
    }

    /// ancestors() of the leaf in a length-n chain returns exactly n nodes.
    #[test]
    fn ancestors_of_leaf_in_chain_is_full_chain(len in 1usize..16) {
        let (g, fps) = chain(len);
        let target = fps[len - 1];
        let ancestors = g.ancestors(target).unwrap();
        prop_assert_eq!(ancestors.len(), len);
    }

    /// Topological order: for every node, all its causes appear before it.
    #[test]
    fn ancestors_are_topologically_ordered(len in 2usize..16) {
        let (g, _fps) = chain(len);
        // Take leaf, get ancestors, check ordering.
        let leaf = *g.nodes.keys().last().unwrap();
        let ancestors = g.ancestors(leaf).unwrap();
        let positions: std::collections::BTreeMap<_, _> = ancestors
            .iter()
            .enumerate()
            .map(|(i, fp)| (*fp, i))
            .collect();
        for (fp, node) in &g.nodes {
            if let Some(&my_pos) = positions.get(fp) {
                for cause in &node.causes {
                    if let Some(&cause_pos) = positions.get(cause) {
                        prop_assert!(cause_pos < my_pos);
                    }
                }
            }
        }
    }

    /// topo_sort returns every node in the graph.
    #[test]
    fn topo_sort_covers_every_node(len in 1usize..16) {
        let (g, _) = chain(len);
        let sorted = g.topo_sort();
        prop_assert_eq!(sorted.len(), g.len());
        let unique: BTreeSet<_> = sorted.iter().copied().collect();
        prop_assert_eq!(unique.len(), g.len());
    }

    /// root_hash is deterministic — two identical graphs produce identical hashes.
    #[test]
    fn root_hash_deterministic_across_graphs(len in 1usize..16) {
        let (g1, _) = chain(len);
        let (g2, _) = chain(len);
        prop_assert_eq!(g1.root_hash(), g2.root_hash());
    }

    /// proof_of(target).root == graph.root_hash().
    #[test]
    fn proof_root_matches_graph_root(len in 1usize..16) {
        let (g, fps) = chain(len);
        let target = fps[len - 1];
        let proof = g.proof_of(target).unwrap();
        prop_assert_eq!(proof.root, g.root_hash());
    }

    /// proof_of(target).path is exactly ancestors(target).
    #[test]
    fn proof_path_matches_ancestors(len in 1usize..16) {
        let (g, fps) = chain(len);
        let target = fps[len - 1];
        let proof = g.proof_of(target).unwrap();
        let ancestors = g.ancestors(target).unwrap();
        prop_assert_eq!(proof.path, ancestors);
    }

    /// Adding a node only grows the graph.
    #[test]
    fn append_strictly_grows_node_count(
        ids in proptest::collection::vec(0u64..2000, 1..16),
    ) {
        let mut g = LineageGraph::new();
        let mut last_len = 0;
        let mut seen: BTreeSet<u64> = BTreeSet::new();
        for id in ids {
            if seen.insert(id) {
                let _fp = g.append(s(id), BTreeSet::new()).unwrap();
                prop_assert_eq!(g.len(), last_len + 1);
                last_len += 1;
            }
        }
    }
}

fn hex8(bytes: &[u8; 32]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(8);
    for &b in &bytes[..4] {
        write!(out, "{b:02x}").unwrap();
    }
    out
}
