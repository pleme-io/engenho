//! WatchedCache — wraps any `DerivationCacheBackend` + broadcasts
//! typed change events to all subscribers.
//!
//! Same pattern as engenho-store's WatchEvent — gives consumers an
//! event-driven path so DrvController / DrvBuildController can
//! react immediately to cache changes instead of polling.
//!
//! Subscribers receive [`CacheEvent`]s on a tokio broadcast channel.
//! Backed by `tokio::sync::broadcast` so multiple consumers see the
//! same stream; if a slow consumer falls behind, it gets `Lagged`
//! errors (standard broadcast semantics) — no event is silently
//! dropped to other consumers.
//!
//! ## Wrapping
//!
//! ```ignore
//! let inner = MemoryDerivationCache::new();
//! let watched = WatchedCache::new(Arc::new(inner));
//! let mut rx = watched.subscribe(64);
//! watched.put_drv(&drv).await?;
//! // rx now receives CacheEvent::DrvUpserted(drv_hash)
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::broadcast;

use crate::derivation::{
    CacheError, DerivationCacheBackend, Drv, DrvHash, NarBlob, NarHash, Realisation,
};

/// One typed cache change event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CacheEvent {
    /// Drv inserted (or replaced) under this hash.
    DrvUpserted(DrvHash),
    /// NAR inserted under this hash.
    NarUpserted(NarHash),
    /// Realisation inserted for this drv.
    RealisationUpserted {
        /// Drv this realisation applies to.
        drv_hash: DrvHash,
        /// Output name.
        output_name: String,
    },
}

/// Wrapped cache that emits [`CacheEvent`]s on every write.
pub struct WatchedCache {
    inner: Arc<dyn DerivationCacheBackend>,
    sender: broadcast::Sender<CacheEvent>,
}

impl WatchedCache {
    /// Wrap `inner` with a watch channel of capacity 128.
    /// (Capacity controls how many events a slow consumer can fall
    /// behind before getting `Lagged`.)
    #[must_use]
    pub fn new(inner: Arc<dyn DerivationCacheBackend>) -> Self {
        Self::with_capacity(inner, 128)
    }

    /// Wrap `inner` with an explicit channel capacity.
    #[must_use]
    pub fn with_capacity(inner: Arc<dyn DerivationCacheBackend>, capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { inner, sender }
    }

    /// Subscribe to cache events. `capacity` parameter is the
    /// per-subscriber receive backlog — not the channel capacity
    /// (set at construction).
    pub fn subscribe(&self, _capacity: usize) -> broadcast::Receiver<CacheEvent> {
        self.sender.subscribe()
    }

    /// Number of currently-active subscribers.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }

    /// Borrow the wrapped backend.
    #[must_use]
    pub fn inner(&self) -> &Arc<dyn DerivationCacheBackend> {
        &self.inner
    }
}

/// Backwards-compat alias for the per-wrapper snapshot type that
/// shipped in v0.86. Migrated to the canonical
/// [`crate::mirante::SubscriberSnapshot`] in v0.87 per the TSR
/// extraction. Pattern #6 (deprecation alias).
#[deprecated(
    since = "0.1.4",
    note = "use engenho_substrate::SubscriberSnapshot directly (v0.87)"
)]
pub type WatchedCacheSnapshot = crate::mirante::SubscriberSnapshot;

crate::define_named!(WatchedCache, "watched");

impl crate::mirante::Observable for WatchedCache {
    type Snapshot = crate::mirante::SubscriberSnapshot;

    fn snapshot(&self) -> Self::Snapshot {
        crate::mirante::SubscriberSnapshot {
            name: "watched",
            subscriber_count: self.subscriber_count(),
        }
    }
}

#[async_trait]
impl DerivationCacheBackend for WatchedCache {
    fn name(&self) -> &'static str {
        "watched"
    }

    async fn get_drv(&self, hash: &DrvHash) -> Result<Option<Drv>, CacheError> {
        self.inner.get_drv(hash).await
    }

    async fn put_drv(&self, drv: &Drv) -> Result<(), CacheError> {
        self.inner.put_drv(drv).await?;
        // Send is fallible only when there are no subscribers; we
        // ignore that — events are best-effort by design.
        let _ = self
            .sender
            .send(CacheEvent::DrvUpserted(drv.drv_hash.clone()));
        Ok(())
    }

    async fn get_nar(&self, hash: &NarHash) -> Result<Option<NarBlob>, CacheError> {
        self.inner.get_nar(hash).await
    }

    async fn put_nar(&self, blob: &NarBlob) -> Result<(), CacheError> {
        self.inner.put_nar(blob).await?;
        let _ = self.sender.send(CacheEvent::NarUpserted(blob.hash.clone()));
        Ok(())
    }

    async fn list_realisations(&self, drv_hash: &DrvHash) -> Result<Vec<Realisation>, CacheError> {
        self.inner.list_realisations(drv_hash).await
    }

    async fn put_realisation(&self, realisation: &Realisation) -> Result<(), CacheError> {
        self.inner.put_realisation(realisation).await?;
        let _ = self.sender.send(CacheEvent::RealisationUpserted {
            drv_hash: realisation.drv_hash.clone(),
            output_name: realisation.output_name.clone(),
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::derivation::{MemoryDerivationCache, OutputPath};

    fn sample_drv(tag: &[u8]) -> Drv {
        Drv::synthetic(DrvHash::from_bytes(tag), "x86_64-linux")
    }

    fn watched_memory() -> WatchedCache {
        WatchedCache::new(Arc::new(MemoryDerivationCache::new()))
    }

    #[tokio::test]
    async fn put_drv_emits_drv_upserted_event() {
        let w = watched_memory();
        let mut rx = w.subscribe(16);
        let drv = sample_drv(b"x");
        w.put_drv(&drv).await.unwrap();
        let event = rx.recv().await.unwrap();
        assert_eq!(event, CacheEvent::DrvUpserted(drv.drv_hash));
    }

    #[tokio::test]
    async fn put_nar_emits_nar_upserted_event() {
        let w = watched_memory();
        let mut rx = w.subscribe(16);
        let blob = NarBlob::from_bytes(b"hello".to_vec());
        w.put_nar(&blob).await.unwrap();
        let event = rx.recv().await.unwrap();
        assert_eq!(event, CacheEvent::NarUpserted(blob.hash));
    }

    #[tokio::test]
    async fn put_realisation_emits_event() {
        let w = watched_memory();
        let mut rx = w.subscribe(16);
        let drv_hash = DrvHash::from_bytes(b"d");
        w.put_realisation(&Realisation {
            drv_hash: drv_hash.clone(),
            output_name: "out".into(),
            output_path: OutputPath::new("/nix/store/a-out"),
            nar_hash: None,
        })
        .await
        .unwrap();
        let event = rx.recv().await.unwrap();
        assert_eq!(
            event,
            CacheEvent::RealisationUpserted {
                drv_hash,
                output_name: "out".into()
            }
        );
    }

    #[tokio::test]
    async fn get_operations_do_not_emit_events() {
        let inner = Arc::new(MemoryDerivationCache::new());
        let drv = sample_drv(b"r");
        inner.put_drv(&drv).await.unwrap();
        let w = WatchedCache::new(inner);
        let mut rx = w.subscribe(16);
        // Read doesn't emit.
        let _ = w.get_drv(&drv.drv_hash).await.unwrap();
        // Confirm no event by checking try_recv non-blocking.
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn multiple_subscribers_all_receive() {
        let w = watched_memory();
        let mut rx1 = w.subscribe(16);
        let mut rx2 = w.subscribe(16);
        assert_eq!(w.subscriber_count(), 2);
        w.put_drv(&sample_drv(b"a")).await.unwrap();
        let e1 = rx1.recv().await.unwrap();
        let e2 = rx2.recv().await.unwrap();
        assert_eq!(e1, e2);
    }

    #[tokio::test]
    async fn writes_with_no_subscribers_dont_fail() {
        let w = watched_memory();
        // No subscribers — write must succeed regardless.
        w.put_drv(&sample_drv(b"no-subs")).await.unwrap();
    }

    #[tokio::test]
    async fn wrapped_backend_writes_actually_persist() {
        let inner = Arc::new(MemoryDerivationCache::new());
        let w = WatchedCache::new(inner.clone());
        let drv = sample_drv(b"persist");
        w.put_drv(&drv).await.unwrap();
        // Underlying backend has the drv.
        assert_eq!(inner.get_drv(&drv.drv_hash).await.unwrap(), Some(drv));
    }

    #[tokio::test]
    async fn subscriber_count_decreases_when_receiver_dropped() {
        let w = watched_memory();
        let rx = w.subscribe(16);
        assert_eq!(w.subscriber_count(), 1);
        drop(rx);
        assert_eq!(w.subscriber_count(), 0);
    }

    #[tokio::test]
    async fn backend_name_is_stable() {
        let w = watched_memory();
        assert_eq!(w.name(), "watched");
    }
}
