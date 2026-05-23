//! mirante — typed live-state observation channel.
//!
//! Per the research brief — operator-dashboard primitive.
//! Different from `BroadcastLedger` (event-stream, every event
//! delivered). Different from logs (typed S, not text).
//!
//! Every substrate primitive that has observable state publishes
//! a typed `Snapshot` via `ObservationChannel`. Operators wire one
//! channel per primitive; a single `Mirante` registry surfaces them
//! all for dashboards / TUIs / health-check endpoints.
//!
//! ## Surface
//!
//!   - `Observable` trait — `fn snapshot(&self) -> Self::Snapshot`
//!   - `ObservationChannel<S>` — typed `watch::Sender` wrapper
//!   - `Mirante` registry — keyed by `&'static str` name
//!
//! ## Composition
//!
//!   - `Named` supertrait on Observable
//!   - `Fingerprint` over Snapshot for cross-node attestation
//!   - `relógio::Instant` for last-changed timestamps
//!   - `Serialize` on Snapshot so registry can render to JSON

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Serialize;
use tokio::sync::{RwLock, watch};

use crate::named::Named;
use crate::relogio::{Clock, Instant};

/// Universal observable surface. Every substrate primitive that
/// has live state worth observing implements this trait.
pub trait Observable: Named + Send + Sync {
    /// The snapshot type — must be cloneable + serializable +
    /// shareable across tasks.
    type Snapshot: Clone + Serialize + Send + Sync + 'static;

    /// Capture the current state (cheap; no I/O). Operators call
    /// this once per channel publish.
    fn snapshot(&self) -> Self::Snapshot;
}

/// Typed live-state channel. Wraps `tokio::sync::watch` + tracks
/// the last-changed Instant.
pub struct ObservationChannel<S: Clone + Send + Sync + 'static> {
    tx: watch::Sender<S>,
    rx: watch::Receiver<S>,
    last_changed: Arc<RwLock<Instant>>,
    clock: Arc<dyn Clock>,
}

impl<S: Clone + Send + Sync + 'static> ObservationChannel<S> {
    /// New channel with an initial snapshot + clock for timestamps.
    #[must_use]
    pub fn new(initial: S, clock: Arc<dyn Clock>) -> Self {
        let (tx, rx) = watch::channel(initial);
        let last_changed = Arc::new(RwLock::new(clock.now()));
        Self {
            tx,
            rx,
            last_changed,
            clock,
        }
    }

    /// Publish a new snapshot. The current value is replaced (last-
    /// value-only — older snapshots are NOT queued).
    pub fn publish(&self, next: S) {
        let _ = self.tx.send(next);
        let now = self.clock.now();
        if let Ok(mut guard) = self.last_changed.try_write() {
            *guard = now;
        }
    }

    /// Current snapshot (cheap; reads `watch::Receiver::borrow()`).
    #[must_use]
    pub fn current(&self) -> S {
        self.rx.borrow().clone()
    }

    /// Subscribe to changes — every publish wakes the receiver.
    /// (Last-value-only: a slow subscriber sees the LATEST value,
    /// not every intermediate.)
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<S> {
        self.rx.clone()
    }

    /// When the channel was last published.
    pub async fn last_changed(&self) -> Instant {
        *self.last_changed.read().await
    }

    /// Currently-active subscriber count.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

/// Type-erased view onto an `ObservationChannel<S>` for the
/// `Mirante` registry. Operators reach for the concrete channel
/// when they know the type; the registry exposes the universal
/// "current snapshot as JSON" surface.
pub trait AnyChannel: Send + Sync {
    /// Channel name (telemetry identifier).
    fn name(&self) -> &'static str;

    /// Current snapshot rendered as JSON. Used by the registry's
    /// `snapshot_all()` for dashboard rendering.
    fn current_json(&self) -> serde_json::Value;

    /// Last-changed Instant (cheap).
    fn last_changed_packed(&self) -> u64;
}

impl<S: Clone + Serialize + Send + Sync + 'static> AnyChannel
    for (&'static str, Arc<ObservationChannel<S>>)
{
    fn name(&self) -> &'static str {
        self.0
    }

    fn current_json(&self) -> serde_json::Value {
        serde_json::to_value(self.1.current()).unwrap_or(serde_json::Value::Null)
    }

    fn last_changed_packed(&self) -> u64 {
        // Use try_read to avoid blocking from sync context.
        match self.1.last_changed.try_read() {
            Ok(guard) => guard.to_packed(),
            Err(_) => 0,
        }
    }
}

/// The mirante registry — operator wires one per process. Every
/// observable primitive registers its channel; dashboards iterate
/// the registry to render live state.
#[derive(Default)]
pub struct Mirante {
    channels: BTreeMap<&'static str, Arc<dyn AnyChannel>>,
}

impl Mirante {
    /// Fresh empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a typed channel under `name`. Replaces any prior
    /// channel with the same name.
    pub fn register<S: Clone + Serialize + Send + Sync + 'static>(
        &mut self,
        name: &'static str,
        channel: Arc<ObservationChannel<S>>,
    ) {
        let any: Arc<dyn AnyChannel> = Arc::new((name, channel));
        self.channels.insert(name, any);
    }

    /// Names of all registered channels.
    #[must_use]
    pub fn list(&self) -> Vec<&'static str> {
        self.channels.keys().copied().collect()
    }

    /// Count of registered channels.
    #[must_use]
    pub fn len(&self) -> usize {
        self.channels.len()
    }

    /// True if no channels registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }

    /// Snapshot every channel as JSON. Useful for one-shot
    /// dashboard rendering.
    #[must_use]
    pub fn snapshot_all(&self) -> BTreeMap<String, serde_json::Value> {
        self.channels
            .iter()
            .map(|(name, ch)| ((*name).to_string(), ch.current_json()))
            .collect()
    }

    /// Last-changed timestamps for every channel.
    #[must_use]
    pub fn last_changed_all(&self) -> BTreeMap<String, u64> {
        self.channels
            .iter()
            .map(|(name, ch)| ((*name).to_string(), ch.last_changed_packed()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relogio::FrozenClock;
    use serde::Deserialize;

    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct CacheStats {
        hits: u64,
        misses: u64,
    }

    fn frozen_clock(t: u64) -> Arc<dyn Clock> {
        Arc::new(FrozenClock::at(t))
    }

    #[tokio::test]
    async fn channel_publish_and_current_round_trip() {
        let ch = ObservationChannel::new(CacheStats { hits: 0, misses: 0 }, frozen_clock(100));
        assert_eq!(ch.current(), CacheStats { hits: 0, misses: 0 });
        ch.publish(CacheStats {
            hits: 10,
            misses: 1,
        });
        assert_eq!(
            ch.current(),
            CacheStats {
                hits: 10,
                misses: 1
            }
        );
    }

    #[tokio::test]
    async fn channel_subscribe_receives_latest() {
        let ch = ObservationChannel::new(0u32, frozen_clock(0));
        let mut rx = ch.subscribe();
        ch.publish(42);
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), 42);
    }

    #[tokio::test]
    async fn channel_subscriber_count_tracks_lifetimes() {
        let ch = ObservationChannel::new(0u32, frozen_clock(0));
        assert_eq!(ch.subscriber_count(), 1); // internal rx counts
        let _rx2 = ch.subscribe();
        assert_eq!(ch.subscriber_count(), 2);
        drop(_rx2);
        assert_eq!(ch.subscriber_count(), 1);
    }

    #[tokio::test]
    async fn channel_last_changed_advances_on_publish() {
        let clock = Arc::new(FrozenClock::at(100));
        let ch = ObservationChannel::new(0u32, clock.clone());
        let t0 = ch.last_changed().await;
        clock.advance(50);
        ch.publish(1);
        // Allow a tick for write lock acquisition.
        tokio::task::yield_now().await;
        let t1 = ch.last_changed().await;
        assert!(t1 > t0);
    }

    #[tokio::test]
    async fn channel_publishes_are_last_value_only() {
        let ch = ObservationChannel::new(0u32, frozen_clock(0));
        ch.publish(1);
        ch.publish(2);
        ch.publish(3);
        // No queue — only the latest matters.
        assert_eq!(ch.current(), 3);
    }

    // ── Mirante registry ───────────────────────────────────────

    #[tokio::test]
    async fn mirante_register_and_list() {
        let mut m = Mirante::new();
        let cache_ch = Arc::new(ObservationChannel::new(
            CacheStats { hits: 0, misses: 0 },
            frozen_clock(0),
        ));
        let count_ch = Arc::new(ObservationChannel::new(0u32, frozen_clock(0)));
        m.register("cache", cache_ch);
        m.register("count", count_ch);
        let names = m.list();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"cache"));
        assert!(names.contains(&"count"));
        assert_eq!(m.len(), 2);
        assert!(!m.is_empty());
    }

    #[tokio::test]
    async fn mirante_register_replaces_by_name() {
        let mut m = Mirante::new();
        m.register(
            "x",
            Arc::new(ObservationChannel::new(0u32, frozen_clock(0))),
        );
        m.register(
            "x",
            Arc::new(ObservationChannel::new(99u32, frozen_clock(0))),
        );
        let snap = m.snapshot_all();
        assert_eq!(snap["x"], serde_json::json!(99));
    }

    #[tokio::test]
    async fn mirante_snapshot_all_returns_json() {
        let mut m = Mirante::new();
        let cache_ch = Arc::new(ObservationChannel::new(
            CacheStats {
                hits: 10,
                misses: 2,
            },
            frozen_clock(0),
        ));
        m.register("cache", cache_ch);
        let snap = m.snapshot_all();
        assert_eq!(snap["cache"]["hits"], 10);
        assert_eq!(snap["cache"]["misses"], 2);
    }

    #[tokio::test]
    async fn mirante_reflects_publish_after_register() {
        let mut m = Mirante::new();
        let ch = Arc::new(ObservationChannel::new(0u32, frozen_clock(0)));
        m.register("x", ch.clone());
        ch.publish(42);
        let snap = m.snapshot_all();
        assert_eq!(snap["x"], serde_json::json!(42));
    }

    #[tokio::test]
    async fn mirante_empty_default() {
        let m = Mirante::new();
        assert!(m.is_empty());
        assert_eq!(m.len(), 0);
        assert!(m.list().is_empty());
        assert!(m.snapshot_all().is_empty());
    }

    #[tokio::test]
    async fn mirante_last_changed_all_returns_packed() {
        let clock = Arc::new(FrozenClock::at(100));
        let ch = Arc::new(ObservationChannel::new(0u32, clock.clone()));
        let mut m = Mirante::new();
        m.register("x", ch.clone());
        clock.advance(50);
        ch.publish(1);
        tokio::task::yield_now().await;
        let lc = m.last_changed_all();
        assert!(lc["x"] > 0);
    }

    // ── Observable trait via newtype ───────────────────────────

    #[derive(Default)]
    pub struct FakeCache {
        stats: std::sync::Mutex<CacheStats>,
    }

    crate::define_named!(FakeCache, "fake-cache");

    impl Observable for FakeCache {
        type Snapshot = CacheStats;
        fn snapshot(&self) -> CacheStats {
            self.stats.lock().unwrap().clone()
        }
    }

    #[tokio::test]
    async fn observable_implementer_returns_snapshot() {
        let cache = FakeCache::default();
        cache.stats.lock().unwrap().hits = 5;
        let snap = cache.snapshot();
        assert_eq!(snap.hits, 5);
        assert_eq!(cache.name(), "fake-cache");
    }
}
