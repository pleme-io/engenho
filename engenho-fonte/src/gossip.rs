//! Typed gossip broker abstraction — generalize FederationBroker so
//! the underlying transport can swap from broadcast → revoada P2P
//! face → chitchat → iroh without changing peer consumers.
//!
//! ## Why a trait
//!
//! v1.25's [`FederationBroker`](crate::FederationBroker) is in-process
//! `tokio::sync::broadcast` — perfect for tests + single-process
//! multi-conduit. Production federation needs:
//!
//!   - cross-host transport (chitchat gossip / iroh P2P)
//!   - lossless delivery for typed Sistema diffs (revoada Raft Face)
//!   - cross-region disaster recovery (S3-backed snapshot transport)
//!
//! All three speak the *same shape*: "broadcast a Sistema change
//! source-text; subscribers receive typed Changes." That's a typed
//! trait, not a struct.
//!
//! ## Transports today
//!
//!   - [`crate::FederationBroker`] — `broadcast::Sender` (in-process).
//!   - `FaceGossipBroker` (sketch — see below) — wraps a revoada
//!     `Face::apply_resource` for announce + `Face::watch_resources`
//!     for subscribe. Behind `with-revoada` once the watch stream
//!     surface is finalized for typed Sistema envelopes (M4.1+).
//!
//! Future transports:
//!   - ChitchatGossipBroker — wraps chitchat's `Cluster::set_kv`.
//!   - IrohGossipBroker — wraps an iroh `Topic` channel.
//!   - S3GossipBroker — periodic snapshot pull from a typed prefix.

use crate::FonteResult;
use async_trait::async_trait;
use std::sync::Arc;

/// Typed gossip broker — every peer publishes typed Sistema source
/// text via [`announce`](Self::announce); subscribers receive typed
/// [`Change`]s via a transport-specific Watcher.
#[async_trait]
pub trait GossipBroker: Send + Sync {
    /// Broadcast a Sistema change to every peer. Returns the
    /// transport-assigned revision (monotone within the broker).
    async fn announce(&self, source: Arc<str>, source_text: Arc<str>) -> FonteResult<u64>;
}

// ── Adapter: GossipBroker for the existing FederationBroker ─────

#[async_trait]
impl GossipBroker for crate::FederationBroker {
    async fn announce(&self, source: Arc<str>, source_text: Arc<str>) -> FonteResult<u64> {
        Ok(self.announce_with_revision(source, source_text))
    }
}

// ── Future: FaceGossipBroker stub (M4.1+) ────────────────────────
//
// Sketch behind `with-revoada-gossip` (not yet enabled):
//
// ```ignore
// pub struct FaceGossipBroker { face: Arc<dyn Face>, rev: AtomicU64 }
// #[async_trait]
// impl GossipBroker for FaceGossipBroker {
//     async fn announce(&self, source: Arc<str>, text: Arc<str>) -> FonteResult<u64> {
//         // YAML envelope: same Pod-with-annotation shape as
//         // RevoadaProposer; replace with native (defsistema) envelope
//         // once revoada's FormatAdapter learns it.
//         let yaml = sistema_envelope(source.as_ref(), text.as_ref());
//         self.face.apply_resource(ResourceFormat::Yaml, yaml.as_bytes())?;
//         Ok(self.rev.fetch_add(1, Ordering::SeqCst))
//     }
// }
// ```
//
// The Watcher mirror is `FaceWatchBroker` — a tokio task pumping
// `Face::watch_resources(...)::next_event()` into a typed Change
// channel. Operators wire it the same way they wire
// FederatedWatcher in v1.25.
