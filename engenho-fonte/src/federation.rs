//! `FederatedConduit` — sketch of cross-cluster Sistema federation.
//!
//! Multiple Conduits in distinct clusters / regions share one
//! Sistema desired state. The federation primitive is just a typed
//! mpsc broker on top of the existing Conduit: every conduit's
//! Outcome (after publish) gets fanned out to every peer's Watcher,
//! so a change in one cluster propagates as a Change on every other
//! cluster's loop.
//!
//! Always-on (no Cargo feature flag) — the M0 wiring uses in-process
//! channels. Real cross-cluster transport behind future
//! `with-revoada-p2p` feature flag will swap the in-process broker
//! for a gossip-shaped `engenho-revoada::Face`.
//!
//! ## Apply rule
//!
//! - Operator publishes a Sistema change on cluster A.
//! - A's Conduit ticks → publishes Outcome → MirantePublisher fires.
//! - Federation broker observes A's outcome → pushes equivalent
//!   Change to every peer's Watcher (kind = ChangeKind::Modified,
//!   revision monotone across peers).
//! - Peer Conduits tick → their SystemControllers diff vs local
//!   last_applied → emit AnomalyEvents → reconcile.
//!
//! Eventual consistency: a change in any cluster converges every
//! peer cluster within one round-trip of the broker.

use crate::{Change, ChangeKind, FonteResult, Watcher};
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::broadcast;

/// In-process federation broker. Holds a `broadcast::Sender<Change>`
/// that every federated Watcher subscribes to.
///
/// In production (M4.1+), `FederationBroker::new_revoada(face)`
/// will swap the broadcast channel for revoada's P2P face — same
/// shape, different transport.
#[derive(Debug)]
pub struct FederationBroker {
    tx: broadcast::Sender<Change>,
    revision: Arc<AtomicU64>,
}

impl Default for FederationBroker {
    fn default() -> Self {
        Self::new(32)
    }
}

impl FederationBroker {
    /// New broker with a bounded broadcast capacity. Typical: 32 —
    /// human-paced editing rarely overflows a 32-slot queue.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        Self {
            tx,
            revision: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Announce a Sistema change to every federated peer. Returns
    /// the assigned monotone federation revision.
    pub fn announce(&self, source: Arc<str>, source_text: Arc<str>) -> u64 {
        let revision = self.revision.fetch_add(1, Ordering::SeqCst);
        let change = Change {
            source,
            kind: ChangeKind::Modified,
            source_text,
            revision,
        };
        let _ = self.tx.send(change);
        revision
    }

    /// Subscribe a peer to the federation broker. The returned
    /// [`FederatedWatcher`] wraps `broadcast::Receiver<Change>` as
    /// a typed [`Watcher`].
    #[must_use]
    pub fn subscribe(&self) -> FederatedWatcher {
        FederatedWatcher {
            rx: TokioMutex::new(self.tx.subscribe()),
        }
    }
}

/// A Watcher backed by a federation broker. Each `next()` awaits the
/// next federated Change.
pub struct FederatedWatcher {
    rx: TokioMutex<broadcast::Receiver<Change>>,
}

#[async_trait]
impl Watcher for FederatedWatcher {
    async fn next(&self) -> FonteResult<Option<Change>> {
        let mut rx = self.rx.lock().await;
        match rx.recv().await {
            Ok(change) => Ok(Some(change)),
            // Lagged: a peer fell behind by more than `capacity` and
            // missed messages. Returning None lets the conduit
            // continue (slow subscribers re-converge on next change).
            Err(broadcast::error::RecvError::Lagged(_)) => Ok(None),
            // Closed: broker dropped; peer should shut down.
            Err(broadcast::error::RecvError::Closed) => Ok(None),
        }
    }
}
