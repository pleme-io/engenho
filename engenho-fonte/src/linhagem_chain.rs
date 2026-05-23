//! AnomalyChain backed by engenho-substrate's [`LineageGraph`].
//!
//! [`MockAnomalyChain`](crate::MockAnomalyChain) ships its own
//! BLAKE3-linked Vec — fine for tests, but the substrate already has
//! a typed content-addressed Lineage DAG built specifically for this
//! kind of append-only proof structure. `LinhagemAnomalyChain` swaps
//! the Vec for `LineageGraph<AnomalyEntry>` so:
//!
//!   - Each event is content-addressed (BLAKE3 fingerprint via the
//!     `Fingerprint` trait — same shape as Drv / receipts /
//!     materializations).
//!   - The chain becomes a typed DAG (one event can have multiple
//!     causes, mirroring revoada's federation: a peer's drift event
//!     references the broker's announcing event as a cause).
//!   - Cross-substrate reuse: every other consumer of `LineageGraph<T>`
//!     (Plantio / chained_verifier / tiered_cache / etc.) speaks the
//!     same shape — operator dashboards can render every typed lineage
//!     identically.

use crate::{AnomalyChain, AnomalyEntry, AnomalyEvent, FonteResult};
use async_trait::async_trait;
use engenho_substrate::fingerprint::Fingerprint;
use engenho_substrate::linhagem_aberta::LineageGraph;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;

/// AnomalyChain backed by `LineageGraph<AnomalyEntry>`. The graph's
/// content-addressed DAG IS the chain — each appended event becomes
/// a node fingerprinted by BLAKE3 of its canonical serde-JSON form.
pub struct LinhagemAnomalyChain {
    graph: Mutex<LineageGraph<AnomalyEntry>>,
    last_fp: Mutex<Option<[u8; 32]>>,
    next_ms: Mutex<u64>,
}

impl Default for LinhagemAnomalyChain {
    fn default() -> Self {
        Self::new()
    }
}

impl LinhagemAnomalyChain {
    /// New empty chain.
    #[must_use]
    pub fn new() -> Self {
        Self {
            graph: Mutex::new(LineageGraph::new()),
            last_fp: Mutex::new(None),
            next_ms: Mutex::new(0),
        }
    }

    /// Borrow the underlying graph for assertion in tests.
    pub fn with_graph<R>(&self, f: impl FnOnce(&LineageGraph<AnomalyEntry>) -> R) -> R {
        f(&self.graph.lock().expect("linhagem chain poisoned"))
    }

    /// Number of nodes currently in the graph.
    pub fn len(&self) -> usize {
        self.graph.lock().expect("linhagem chain poisoned").len()
    }

    /// True if no events have been chained yet.
    pub fn is_empty(&self) -> bool {
        self.graph
            .lock()
            .expect("linhagem chain poisoned")
            .is_empty()
    }
}

// AnomalyEntry implements Fingerprint by canonical-serializing
// itself then BLAKE3-hashing — same shape as Drv / receipts elsewhere.
impl Fingerprint for AnomalyEntry {
    fn fingerprint(&self) -> [u8; 32] {
        let canonical = serde_json::to_string(self).expect("AnomalyEntry serializes");
        *blake3::hash(canonical.as_bytes()).as_bytes()
    }
}

#[async_trait]
impl AnomalyChain for LinhagemAnomalyChain {
    async fn record(
        &self,
        revision: u64,
        events: Vec<AnomalyEvent>,
    ) -> FonteResult<Option<Arc<str>>> {
        if events.is_empty() {
            return Ok(None);
        }
        let mut graph = self.graph.lock().expect("linhagem chain poisoned");
        let mut last = self.last_fp.lock().expect("linhagem chain poisoned");
        let mut next_ms = self.next_ms.lock().expect("linhagem chain poisoned");
        let mut last_id_arc: Option<Arc<str>> = None;
        for event in events {
            // Each event references the previous fingerprint as its
            // sole cause. First event has no causes (root node).
            let mut causes = BTreeSet::new();
            if let Some(fp) = *last {
                causes.insert(fp);
            }
            let sealed_at_ms = *next_ms;
            *next_ms += 1;
            // Compute the entry's typed id field (matches MockAnomalyChain's
            // shape — 16-char BLAKE3 hex of the canonical form). The
            // graph stores AnomalyEntry; appending fingerprints them
            // BLAKE3-style independently of `id`.
            let provisional = AnomalyEntry {
                id: Arc::from("placeholder"),
                prev_id: last
                    .map(|fp| {
                        let hex = hex_8(&fp);
                        Arc::from(hex.as_str())
                    })
                    .unwrap_or_else(|| Arc::from("0000000000000000")),
                revision,
                event: event.clone(),
                sealed_at_ms,
            };
            let id_hex = hex_8(&provisional.fingerprint());
            let entry = AnomalyEntry {
                id: Arc::from(id_hex.as_str()),
                prev_id: provisional.prev_id.clone(),
                revision: provisional.revision,
                event: provisional.event.clone(),
                sealed_at_ms: provisional.sealed_at_ms,
            };
            let fp = graph
                .append(entry, causes)
                .map_err(|e| crate::FonteError::Attest(format!("linhagem append: {e:?}")))?;
            *last = Some(fp);
            last_id_arc = Some(Arc::from(id_hex.as_str()));
        }
        Ok(last_id_arc)
    }
}

fn hex_8(fp: &[u8; 32]) -> String {
    fp[..8].iter().fold(String::new(), |mut acc, b| {
        use std::fmt::Write;
        let _ = write!(&mut acc, "{b:02x}");
        acc
    })
}
