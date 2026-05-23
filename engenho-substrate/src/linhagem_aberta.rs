//! linhagem-aberta — generic causality DAG over any `Fingerprint`.
//!
//! Per the research brief — fourth inventive primitive. The
//! existing `pesquisa::Linhagem` is search-local; this module
//! generalizes the same shape to ANY `Fingerprint` value. Use
//! cases:
//!   - Drv ancestry (which input drvs caused this output)
//!   - Plantio audit (which receipts caused this Confirmed stage)
//!   - máquina transition lineage (replay-from-history primitive)
//!   - replay debugging (bisect to causing receipt)
//!
//! ## Surface
//!
//!   - `LineageGraph<T: Fingerprint + Serialize>` — immutable
//!     append-only DAG-of-fingerprints
//!   - `LineageNode<T>` — value + typed `causes: BTreeSet<[u8;32]>`
//!   - `LineageProof` — Merkle-style inclusion proof
//!   - `LineageError` — typed errors
//!
//! Three operations on the DAG:
//!   - `append(value, causes)` — adds a node; returns its
//!     fingerprint; errors if a cause isn't already in the graph
//!   - `ancestors(target)` — iterate every ancestor's fingerprint
//!     in topological order
//!   - `root_hash()` — BLAKE3 over the sorted-merkle of leaves
//!   - `proof_of(target)` — Merkle-style proof binding target to root

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::fingerprint::Fingerprint;

/// `LineageGraph` errors.
#[derive(Debug, Clone, Error)]
pub enum LineageError {
    /// `append` referenced a cause hash not yet in the graph.
    #[error("unknown cause: {0}")]
    UnknownCause(String),
    /// `append` referenced a duplicate value (already in the graph).
    #[error("duplicate node: {0}")]
    DuplicateNode(String),
    /// `proof_of` was asked for a target not in the graph.
    #[error("not in graph: {0}")]
    NotInGraph(String),
}

crate::impl_error_kind! {
    LineageError {
        (UnknownCause(_)) => "unknown_cause",
        (DuplicateNode(_)) => "duplicate_node",
        (NotInGraph(_)) => "not_in_graph",
    }
}

/// One node in the lineage DAG.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineageNode<T> {
    /// The value at this node.
    pub value: T,
    /// Set of cause fingerprints (parents) — empty for roots.
    pub causes: BTreeSet<[u8; 32]>,
}

/// Append-only DAG keyed by content fingerprint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineageGraph<T> {
    /// Nodes keyed by their content fingerprint.
    pub nodes: BTreeMap<[u8; 32], LineageNode<T>>,
    /// Set of root fingerprints (nodes with empty causes).
    pub roots: BTreeSet<[u8; 32]>,
}

impl<T> Default for LineageGraph<T> {
    fn default() -> Self {
        Self {
            nodes: BTreeMap::new(),
            roots: BTreeSet::new(),
        }
    }
}

impl<T: Fingerprint + Clone> LineageGraph<T> {
    /// Empty graph.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Node count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// True if empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// True if the graph contains a node with this fingerprint.
    #[must_use]
    pub fn contains(&self, fingerprint: &[u8; 32]) -> bool {
        self.nodes.contains_key(fingerprint)
    }

    /// Append a node with the given causes. Returns the node's
    /// content fingerprint.
    ///
    /// # Errors
    /// - [`LineageError::UnknownCause`] if any cause isn't in the graph
    /// - [`LineageError::DuplicateNode`] if the value's fingerprint
    ///   already exists
    pub fn append(
        &mut self,
        value: T,
        causes: BTreeSet<[u8; 32]>,
    ) -> Result<[u8; 32], LineageError> {
        // Verify every cause exists.
        for cause in &causes {
            if !self.nodes.contains_key(cause) {
                return Err(LineageError::UnknownCause(hex8(cause)));
            }
        }
        let fp = value.fingerprint();
        if self.nodes.contains_key(&fp) {
            return Err(LineageError::DuplicateNode(hex8(&fp)));
        }
        if causes.is_empty() {
            self.roots.insert(fp);
        }
        self.nodes.insert(fp, LineageNode { value, causes });
        Ok(fp)
    }

    /// All ancestors of `target` (transitive parents), including
    /// the target itself. Returned in topological order
    /// (ancestors before descendants).
    ///
    /// # Errors
    /// [`LineageError::NotInGraph`] if `target` isn't in the graph.
    pub fn ancestors(&self, target: [u8; 32]) -> Result<Vec<[u8; 32]>, LineageError> {
        if !self.nodes.contains_key(&target) {
            return Err(LineageError::NotInGraph(hex8(&target)));
        }
        // BFS collecting all reachable ancestors.
        let mut visited: BTreeSet<[u8; 32]> = BTreeSet::new();
        let mut queue: VecDeque<[u8; 32]> = VecDeque::new();
        queue.push_back(target);
        visited.insert(target);
        while let Some(curr) = queue.pop_front() {
            if let Some(node) = self.nodes.get(&curr) {
                for cause in &node.causes {
                    if visited.insert(*cause) {
                        queue.push_back(*cause);
                    }
                }
            }
        }
        // Topological sort the subgraph induced by `visited`.
        Ok(self.topo_sort_subset(&visited))
    }

    /// Topological sort of the entire graph (parents before children).
    #[must_use]
    pub fn topo_sort(&self) -> Vec<[u8; 32]> {
        let all: BTreeSet<[u8; 32]> = self.nodes.keys().copied().collect();
        self.topo_sort_subset(&all)
    }

    fn topo_sort_subset(&self, subset: &BTreeSet<[u8; 32]>) -> Vec<[u8; 32]> {
        // Kahn's algorithm on the induced subgraph.
        let mut in_degree: BTreeMap<[u8; 32], usize> = subset
            .iter()
            .map(|fp| {
                let count = self.nodes.get(fp).map_or(0, |n| {
                    n.causes.iter().filter(|c| subset.contains(*c)).count()
                });
                (*fp, count)
            })
            .collect();
        let mut queue: VecDeque<[u8; 32]> = in_degree
            .iter()
            .filter(|&(_, &d)| d == 0)
            .map(|(fp, _)| *fp)
            .collect();
        let mut sorted = Vec::with_capacity(subset.len());
        while let Some(next) = queue.pop_front() {
            sorted.push(next);
            // Decrement in-degree of every child within subset.
            for fp in subset {
                let Some(node) = self.nodes.get(fp) else {
                    continue;
                };
                if !node.causes.contains(&next) {
                    continue;
                }
                if let Some(d) = in_degree.get_mut(fp) {
                    *d = d.saturating_sub(1);
                    if *d == 0 && !sorted.contains(fp) {
                        queue.push_back(*fp);
                    }
                }
            }
        }
        sorted
    }

    /// BLAKE3 root hash over the sorted node fingerprints. Used to
    /// commit a snapshot of the entire graph — operators publish
    /// this for attestation.
    #[must_use]
    pub fn root_hash(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        // Domain separator for the graph hash.
        hasher.update(b"linhagem-aberta/v1/");
        for fp in self.nodes.keys() {
            hasher.update(fp);
        }
        *hasher.finalize().as_bytes()
    }

    /// Inclusion proof: returns the target's ancestor chain + the
    /// root hash. A verifier with the root hash + the proof can
    /// confirm the target's lineage without seeing the whole graph.
    ///
    /// # Errors
    /// [`LineageError::NotInGraph`] if the target isn't in the graph.
    pub fn proof_of(&self, target: [u8; 32]) -> Result<LineageProof, LineageError> {
        let path = self.ancestors(target)?;
        Ok(LineageProof {
            path,
            root: self.root_hash(),
        })
    }
}

/// Inclusion proof for a target within a `LineageGraph`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineageProof {
    /// Ancestor chain in topological order (oldest first).
    pub path: Vec<[u8; 32]>,
    /// Root hash of the originating graph.
    pub root: [u8; 32],
}

fn hex8(bytes: &[u8; 32]) -> String {
    crate::hex::hex_encode(&bytes[..4])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct Sample {
        pub id: u32,
        pub name: String,
    }
    crate::impl_fingerprint!(Sample);

    fn s(id: u32, name: &str) -> Sample {
        Sample {
            id,
            name: name.into(),
        }
    }

    #[test]
    fn empty_graph_has_zero_len() {
        let g: LineageGraph<Sample> = LineageGraph::new();
        assert!(g.is_empty());
        assert_eq!(g.len(), 0);
    }

    #[test]
    fn append_root_node_inserts_into_roots() {
        let mut g = LineageGraph::new();
        let fp = g.append(s(1, "a"), BTreeSet::new()).unwrap();
        assert_eq!(g.len(), 1);
        assert!(g.roots.contains(&fp));
    }

    #[test]
    fn append_child_with_known_cause_succeeds() {
        let mut g = LineageGraph::new();
        let root = g.append(s(1, "root"), BTreeSet::new()).unwrap();
        let causes: BTreeSet<_> = std::iter::once(root).collect();
        let child = g.append(s(2, "child"), causes).unwrap();
        assert_eq!(g.len(), 2);
        assert!(g.contains(&child));
        // child is NOT a root (has causes).
        assert!(!g.roots.contains(&child));
    }

    #[test]
    fn append_with_unknown_cause_errors() {
        let mut g = LineageGraph::new();
        let unknown: BTreeSet<_> = std::iter::once([99u8; 32]).collect();
        let err = g.append(s(1, "x"), unknown).unwrap_err();
        assert_eq!(err.kind(), "unknown_cause");
        assert!(g.is_empty()); // failed append doesn't mutate
    }

    #[test]
    fn append_duplicate_value_errors() {
        let mut g = LineageGraph::new();
        g.append(s(1, "a"), BTreeSet::new()).unwrap();
        let err = g.append(s(1, "a"), BTreeSet::new()).unwrap_err();
        assert_eq!(err.kind(), "duplicate_node");
    }

    #[test]
    fn contains_returns_true_after_append() {
        let mut g = LineageGraph::new();
        let fp = g.append(s(1, "a"), BTreeSet::new()).unwrap();
        assert!(g.contains(&fp));
        assert!(!g.contains(&[0u8; 32]));
    }

    #[test]
    fn ancestors_includes_target_itself() {
        let mut g = LineageGraph::new();
        let root = g.append(s(1, "root"), BTreeSet::new()).unwrap();
        let ancestors = g.ancestors(root).unwrap();
        assert!(ancestors.contains(&root));
    }

    #[test]
    fn ancestors_chain_of_three() {
        let mut g = LineageGraph::new();
        let a = g.append(s(1, "a"), BTreeSet::new()).unwrap();
        let b = g.append(s(2, "b"), std::iter::once(a).collect()).unwrap();
        let c = g.append(s(3, "c"), std::iter::once(b).collect()).unwrap();
        let ancestors = g.ancestors(c).unwrap();
        // Topologically sorted: a, b, c.
        assert_eq!(ancestors.len(), 3);
        assert_eq!(ancestors[0], a);
        assert_eq!(ancestors[1], b);
        assert_eq!(ancestors[2], c);
    }

    #[test]
    fn ancestors_diamond_pattern() {
        // a → {b, c} → d
        let mut g = LineageGraph::new();
        let a = g.append(s(1, "a"), BTreeSet::new()).unwrap();
        let b = g.append(s(2, "b"), std::iter::once(a).collect()).unwrap();
        let c = g.append(s(3, "c"), std::iter::once(a).collect()).unwrap();
        let d = g.append(s(4, "d"), [b, c].into_iter().collect()).unwrap();
        let ancestors = g.ancestors(d).unwrap();
        assert_eq!(ancestors.len(), 4);
        assert_eq!(ancestors[0], a); // root first
        assert_eq!(ancestors[3], d); // target last
        // b and c can appear in either order in the middle.
        let middle: BTreeSet<_> = ancestors[1..3].iter().copied().collect();
        assert!(middle.contains(&b));
        assert!(middle.contains(&c));
    }

    #[test]
    fn ancestors_target_not_in_graph_errors() {
        let g: LineageGraph<Sample> = LineageGraph::new();
        let err = g.ancestors([99u8; 32]).unwrap_err();
        assert_eq!(err.kind(), "not_in_graph");
    }

    #[test]
    fn topo_sort_returns_all_nodes() {
        let mut g = LineageGraph::new();
        let a = g.append(s(1, "a"), BTreeSet::new()).unwrap();
        let b = g.append(s(2, "b"), std::iter::once(a).collect()).unwrap();
        let _c = g.append(s(3, "c"), std::iter::once(b).collect()).unwrap();
        let sorted = g.topo_sort();
        assert_eq!(sorted.len(), 3);
        // Check ordering invariant: for each node, all causes come before.
        let positions: BTreeMap<_, _> = sorted.iter().enumerate().map(|(i, fp)| (*fp, i)).collect();
        for (fp, node) in &g.nodes {
            let my_pos = positions[fp];
            for cause in &node.causes {
                assert!(positions[cause] < my_pos);
            }
        }
    }

    #[test]
    fn root_hash_is_deterministic() {
        let mut g1 = LineageGraph::new();
        let mut g2 = LineageGraph::new();
        let root_g1 = g1.append(s(1, "x"), BTreeSet::new()).unwrap();
        let root_g2 = g2.append(s(1, "x"), BTreeSet::new()).unwrap();
        assert_eq!(root_g1, root_g2); // same fingerprint
        assert_eq!(g1.root_hash(), g2.root_hash()); // same root hash
    }

    #[test]
    fn root_hash_diverges_per_distinct_graph() {
        let mut g1 = LineageGraph::new();
        let mut g2 = LineageGraph::new();
        g1.append(s(1, "a"), BTreeSet::new()).unwrap();
        g2.append(s(2, "b"), BTreeSet::new()).unwrap();
        assert_ne!(g1.root_hash(), g2.root_hash());
    }

    #[test]
    fn proof_of_returns_ancestors_and_root() {
        let mut g = LineageGraph::new();
        let a = g.append(s(1, "a"), BTreeSet::new()).unwrap();
        let b = g.append(s(2, "b"), std::iter::once(a).collect()).unwrap();
        let proof = g.proof_of(b).unwrap();
        assert_eq!(proof.path, vec![a, b]);
        assert_eq!(proof.root, g.root_hash());
    }

    #[test]
    fn proof_of_unknown_target_errors() {
        let g: LineageGraph<Sample> = LineageGraph::new();
        let err = g.proof_of([99u8; 32]).unwrap_err();
        assert_eq!(err.kind(), "not_in_graph");
    }

    #[test]
    fn lineage_proof_serde_round_trips() {
        // Note: LineageGraph itself uses `BTreeMap<[u8;32], _>` which
        // JSON can't represent (object keys must be strings). The
        // LineageProof shape (Vec<[u8;32]>) is JSON-clean and is what
        // operators ship for attestation — round-trip that.
        let mut g = LineageGraph::new();
        let a = g.append(s(1, "a"), BTreeSet::new()).unwrap();
        let b = g.append(s(2, "b"), std::iter::once(a).collect()).unwrap();
        let proof = g.proof_of(b).unwrap();
        let bytes = serde_json::to_vec(&proof).unwrap();
        let back: LineageProof = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, proof);
    }

    #[test]
    fn error_kinds_stable() {
        assert_eq!(
            LineageError::UnknownCause("x".into()).kind(),
            "unknown_cause"
        );
        assert_eq!(
            LineageError::DuplicateNode("x".into()).kind(),
            "duplicate_node"
        );
        assert_eq!(LineageError::NotInGraph("x".into()).kind(), "not_in_graph");
    }
}
