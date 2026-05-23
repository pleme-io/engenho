//! Typed AnomalyChain — drift detection + remediation routing.
//!
//! When the [`SystemController`](crate::SystemController) reconciles a
//! `Sistema` decision, the diff against `last_applied` is a typed
//! sequence of [`AnomalyEvent`]s — never plain strings, never log
//! lines. Each event names the sub-primitive that drifted + the
//! typed delta (added / removed / changed).
//!
//! The events flow through an [`AnomalyChain`] — a BLAKE3-linked
//! append-only log that mirrors tameshi's chain shape. Real wiring
//! to viggy's [`AnomalyController`] ships behind feature flags later;
//! the mock-driven [`MockAnomalyChain`] satisfies the contract here
//! and in fonte's own tests.
//!
//! ## Why a typed chain (not a metric or alert)
//!
//! The Viggy Method (`pleme-io/theory/CONTINUOUS-SOLUTION-MACHINE.md`)
//! is built on the premise that drift is a *typed value* the
//! substrate proves something about — not a numeric symptom waiting
//! for a human to triage. AnomalyChain is the typed surface for that
//! claim; remediation policy (NoOp / Alert / AutoCorrect / Escalate)
//! routes against the typed kind, not against string-match patterns.

use crate::{AppRef, FonteResult, InfraRef, PromessaRef, Sistema, TopologyRef};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::Mutex;

/// What kind of drift the substrate observed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AnomalyEvent {
    /// An app was added to the desired Sistema.
    AppAdded(AppRef),
    /// An app was removed from the desired Sistema.
    AppRemoved(AppRef),
    /// An app's pinned version changed (same name).
    AppVersionChanged {
        /// App name.
        name: Arc<str>,
        /// Previous pinned version (None = latest-tracking).
        from: Option<Arc<str>>,
        /// New pinned version.
        to: Option<Arc<str>>,
    },
    /// An infra unit was added.
    InfraAdded(InfraRef),
    /// An infra unit was removed.
    InfraRemoved(InfraRef),
    /// A promessa was added.
    PromessaAdded(PromessaRef),
    /// A promessa was removed.
    PromessaRemoved(PromessaRef),
    /// A promessa's target value shifted.
    PromessaTargetChanged {
        /// Promessa name.
        name: Arc<str>,
        /// Previous target.
        from: f64,
        /// New target.
        to: f64,
    },
    /// The cluster topology shape changed (strategy or node count).
    TopologyChanged {
        /// Previous topology.
        from: TopologyRef,
        /// New topology.
        to: TopologyRef,
    },
}

impl AnomalyEvent {
    /// Diff two Sistemas. Returns the typed sequence of drift events
    /// that took the cluster from `prev` to `next`.
    ///
    /// Pure function — no side effects, no allocation beyond the
    /// returned Vec. Order: removals first, then additions, then
    /// changes — matches the operator's mental model of "what got
    /// pulled, what got added, what shifted in place."
    #[must_use]
    pub fn diff(prev: &Sistema, next: &Sistema) -> Vec<Self> {
        let mut events = Vec::new();
        events.extend(diff_apps(&prev.apps, &next.apps));
        events.extend(diff_infra(&prev.infra, &next.infra));
        events.extend(diff_promises(&prev.promises, &next.promises));
        if prev.topology != next.topology {
            events.push(Self::TopologyChanged {
                from: prev.topology.clone(),
                to: next.topology.clone(),
            });
        }
        events
    }
}

fn diff_apps(prev: &[AppRef], next: &[AppRef]) -> Vec<AnomalyEvent> {
    let mut out = Vec::new();
    // Removals
    for p in prev {
        if !next.iter().any(|n| n.name == p.name) {
            out.push(AnomalyEvent::AppRemoved(p.clone()));
        }
    }
    // Additions
    for n in next {
        if !prev.iter().any(|p| p.name == n.name) {
            out.push(AnomalyEvent::AppAdded(n.clone()));
        }
    }
    // Version changes
    for n in next {
        if let Some(p) = prev.iter().find(|p| p.name == n.name)
            && p.version != n.version
        {
            out.push(AnomalyEvent::AppVersionChanged {
                name: n.name.clone(),
                from: p.version.clone(),
                to: n.version.clone(),
            });
        }
    }
    out
}

fn diff_infra(prev: &[InfraRef], next: &[InfraRef]) -> Vec<AnomalyEvent> {
    let mut out = Vec::new();
    for p in prev {
        if !next.iter().any(|n| n.name == p.name) {
            out.push(AnomalyEvent::InfraRemoved(p.clone()));
        }
    }
    for n in next {
        if !prev.iter().any(|p| p.name == n.name) {
            out.push(AnomalyEvent::InfraAdded(n.clone()));
        }
    }
    out
}

fn diff_promises(prev: &[PromessaRef], next: &[PromessaRef]) -> Vec<AnomalyEvent> {
    let mut out = Vec::new();
    for p in prev {
        if !next.iter().any(|n| n.name == p.name) {
            out.push(AnomalyEvent::PromessaRemoved(p.clone()));
        }
    }
    for n in next {
        if !prev.iter().any(|p| p.name == n.name) {
            out.push(AnomalyEvent::PromessaAdded(n.clone()));
        }
    }
    for n in next {
        if let Some(p) = prev.iter().find(|p| p.name == n.name)
            && (p.target - n.target).abs() > f64::EPSILON
        {
            out.push(AnomalyEvent::PromessaTargetChanged {
                name: n.name.clone(),
                from: p.target,
                to: n.target,
            });
        }
    }
    out
}

// ── AnomalyChain trait + mock impl ──────────────────────────────

/// One chained entry. `prev_id` links to the previous entry's `id`;
/// the first entry's `prev_id` is the all-zero sentinel.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnomalyEntry {
    /// BLAKE3 hex (16-char prefix) of this entry's canonical form.
    pub id: Arc<str>,
    /// `id` of the previous entry, or the all-zero sentinel for the
    /// chain's first entry.
    pub prev_id: Arc<str>,
    /// The Sistema revision that triggered this drift detection.
    pub revision: u64,
    /// The typed drift event.
    pub event: AnomalyEvent,
    /// Monotone ms-since-construction (deterministic in tests).
    pub sealed_at_ms: u64,
}

/// Append-only typed log of [`AnomalyEvent`]s.
///
/// Real impls (M3.5+) chain entries into tameshi's HeartbeatChain +
/// route through viggy's AnomalyController per typed RemediationPolicy.
/// [`MockAnomalyChain`] is the always-on default.
#[async_trait]
pub trait AnomalyChain: Send + Sync {
    /// Append every event in `events` to the chain under the given
    /// Sistema revision. Each event becomes a chained entry; the
    /// last entry's id is returned for chain-fence assertions.
    async fn record(
        &self,
        revision: u64,
        events: Vec<AnomalyEvent>,
    ) -> FonteResult<Option<Arc<str>>>;
}

/// In-memory chain with BLAKE3 linkage. Deterministic clock
/// (monotone ms-since-construction).
#[derive(Debug)]
pub struct MockAnomalyChain {
    state: Mutex<MockAnomalyChainState>,
}

#[derive(Debug, Default)]
struct MockAnomalyChainState {
    chain: Vec<AnomalyEntry>,
    next_ms: u64,
}

impl Default for MockAnomalyChain {
    fn default() -> Self {
        Self::new()
    }
}

impl MockAnomalyChain {
    /// New empty chain.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new(MockAnomalyChainState::default()),
        }
    }

    /// Read the chain (clone) for assertion in tests.
    pub fn entries(&self) -> Vec<AnomalyEntry> {
        self.state
            .lock()
            .expect("mock chain poisoned")
            .chain
            .clone()
    }

    /// Validate the chain integrity: every entry's `prev_id` equals
    /// the previous entry's `id`, with the first entry pointing to
    /// the all-zero sentinel.
    pub fn validate_chain(&self) -> bool {
        let entries = self.entries();
        let mut prev: Arc<str> = Arc::from(ZERO_PREV);
        for e in &entries {
            if e.prev_id != prev {
                return false;
            }
            prev = e.id.clone();
        }
        true
    }
}

#[async_trait]
impl AnomalyChain for MockAnomalyChain {
    async fn record(
        &self,
        revision: u64,
        events: Vec<AnomalyEvent>,
    ) -> FonteResult<Option<Arc<str>>> {
        if events.is_empty() {
            return Ok(None);
        }
        let mut state = self.state.lock().expect("mock chain poisoned");
        let mut last_id: Option<Arc<str>> = None;
        for event in events {
            let prev_id: Arc<str> = state
                .chain
                .last()
                .map_or_else(|| Arc::from(ZERO_PREV), |e| e.id.clone());
            let canonical = serde_json::json!({
                "prev_id": prev_id.as_ref(),
                "revision": revision,
                "event": &event,
            })
            .to_string();
            let hash = blake3::hash(canonical.as_bytes()).to_hex();
            let id: Arc<str> = Arc::from(&hash[..16]);
            let sealed_at_ms = state.next_ms;
            state.next_ms += 1;
            let entry = AnomalyEntry {
                id: id.clone(),
                prev_id,
                revision,
                event,
                sealed_at_ms,
            };
            state.chain.push(entry);
            last_id = Some(id);
        }
        Ok(last_id)
    }
}

const ZERO_PREV: &str = "0000000000000000";
