//! EventDrivenController — generic adapter that ticks a wrapped
//! Controller on every received event.
//!
//! Bridges any tokio broadcast channel (engenho-store WatchEvent,
//! engenho-substrate CacheEvent, custom event sources) into the
//! Controller trait surface — controllers no longer need to poll
//! when the upstream can notify.
//!
//! ## Reconcile semantics
//!
//!   * `tick()` on EventDrivenController = drain pending events +
//!     issue ONE downstream `tick()` per event (or one per batch
//!     when `coalesce: true`).
//!   * Lagged subscribers (broadcast capacity overflow) → ignored;
//!     one extra tick scheduled to catch up.
//!   * Closed channels → returns Ok with the count of events
//!     processed before close. Subsequent ticks no-op until the
//!     receiver is resubscribed.
//!
//! ## When to use
//!
//!   * Wrap DrvController so it ticks on CacheEvent::RealisationUpserted
//!     instead of running on every interval.
//!   * Wrap any Controller whose upstream emits typed events.
//!
//! ## When NOT to use
//!
//!   * Controllers whose state isn't event-derived (TickEvery is
//!     fine for those).
//!   * Cross-cluster federation paths where event order isn't
//!     guaranteed (use a reconcile loop with full-list reads).

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{Mutex, broadcast};

use crate::controller::{Controller, ReconcileOutcome, ReconcileReport};
use crate::error::ControllerError;

/// Generic wrapper that drains an event channel + issues ticks.
pub struct EventDrivenController<E: Clone + Send + 'static> {
    inner: Arc<dyn Controller>,
    receiver: Mutex<broadcast::Receiver<E>>,
    coalesce: bool,
}

impl<E: Clone + Send + 'static> EventDrivenController<E> {
    /// Wrap `inner` with an event-driven trigger. `coalesce=true`
    /// drains the entire pending queue + issues ONE downstream
    /// tick (efficient when events redundantly trigger the same
    /// work); `coalesce=false` issues one tick per event.
    #[must_use]
    pub fn new(
        inner: Arc<dyn Controller>,
        receiver: broadcast::Receiver<E>,
        coalesce: bool,
    ) -> Self {
        Self {
            inner,
            receiver: Mutex::new(receiver),
            coalesce,
        }
    }

    /// Wrap with default coalescing enabled (recommended for most
    /// controllers — saves redundant reconcile work).
    #[must_use]
    pub fn coalesced(inner: Arc<dyn Controller>, receiver: broadcast::Receiver<E>) -> Self {
        Self::new(inner, receiver, true)
    }
}

#[async_trait]
impl<E: Clone + Send + Sync + 'static> Controller for EventDrivenController<E> {
    fn name(&self) -> &'static str {
        // Wrapped — surface the inner controller's name for telemetry.
        self.inner.name()
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let mut rx = self.receiver.lock().await;
        // Drain pending events.
        let mut drained = 0usize;
        let mut had_event = false;
        loop {
            match rx.try_recv() {
                Ok(_) => {
                    had_event = true;
                    drained += 1;
                    if !self.coalesce {
                        // Per-event mode: issue tick now, keep draining.
                        drop(rx);
                        let _ = self.inner.tick().await?;
                        rx = self.receiver.lock().await;
                    }
                }
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(broadcast::error::TryRecvError::Lagged(_)) => {
                    // Caller fell behind capacity → force a sync.
                    had_event = true;
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    break;
                }
            }
        }
        drop(rx);
        if had_event && self.coalesce {
            // Single downstream tick for the entire drained batch. Preserve
            // the inner controller's requeue decision while bumping the
            // examined count to reflect the coalesced batch size.
            let outcome = self.inner.tick().await?;
            let result = outcome.result;
            let mut report = outcome.report;
            report.objects_examined = report.objects_examined.max(drained);
            return Ok(ReconcileOutcome::new(report, result));
        }
        if !had_event {
            // No events — no work to do.
            return Ok(ReconcileReport::default().into());
        }
        // Per-event mode: at least one tick was already issued.
        Ok(ReconcileReport {
            objects_examined: drained,
            ..ReconcileReport::default()
        }
        .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Test controller that counts tick invocations.
    #[derive(Default)]
    struct CountingController {
        ticks: AtomicUsize,
    }

    #[async_trait]
    impl Controller for CountingController {
        fn name(&self) -> &'static str {
            "counting"
        }
        async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
            self.ticks.fetch_add(1, Ordering::SeqCst);
            Ok(ReconcileReport::default().into())
        }
    }

    #[tokio::test]
    async fn no_events_means_no_downstream_tick() {
        let inner = Arc::new(CountingController::default());
        let (_tx, rx) = broadcast::channel::<u32>(16);
        let outer = EventDrivenController::coalesced(inner.clone(), rx);
        let _ = outer.tick().await.unwrap();
        assert_eq!(inner.ticks.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn coalesced_collapses_multiple_events_to_one_tick() {
        let inner = Arc::new(CountingController::default());
        let (tx, rx) = broadcast::channel::<u32>(16);
        let outer = EventDrivenController::coalesced(inner.clone(), rx);
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        tx.send(3).unwrap();
        let report = outer.tick().await.unwrap();
        assert_eq!(inner.ticks.load(Ordering::SeqCst), 1);
        assert_eq!(report.objects_examined, 3);
    }

    #[tokio::test]
    async fn per_event_mode_ticks_once_per_event() {
        let inner = Arc::new(CountingController::default());
        let (tx, rx) = broadcast::channel::<u32>(16);
        let outer = EventDrivenController::new(inner.clone(), rx, false);
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        tx.send(3).unwrap();
        let _ = outer.tick().await.unwrap();
        assert_eq!(inner.ticks.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn wrapped_name_passes_through() {
        let inner = Arc::new(CountingController::default());
        let (_tx, rx) = broadcast::channel::<u32>(16);
        let outer = EventDrivenController::coalesced(inner, rx);
        assert_eq!(outer.name(), "counting");
    }

    #[tokio::test]
    async fn lagged_subscriber_forces_sync_tick() {
        let inner = Arc::new(CountingController::default());
        // Tiny channel; fill past capacity to trigger Lagged.
        let (tx, rx) = broadcast::channel::<u32>(2);
        let outer = EventDrivenController::coalesced(inner.clone(), rx);
        // Send 5 events into a capacity-2 channel — subscriber
        // sees Lagged on first recv.
        for i in 0..5 {
            tx.send(i).unwrap();
        }
        let _ = outer.tick().await.unwrap();
        // At least one downstream tick fired.
        assert!(inner.ticks.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn closed_channel_means_no_tick() {
        let inner = Arc::new(CountingController::default());
        let (tx, rx) = broadcast::channel::<u32>(16);
        let outer = EventDrivenController::coalesced(inner.clone(), rx);
        drop(tx); // close sender
        let _ = outer.tick().await.unwrap();
        assert_eq!(inner.ticks.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn second_tick_after_drain_is_noop() {
        let inner = Arc::new(CountingController::default());
        let (tx, rx) = broadcast::channel::<u32>(16);
        let outer = EventDrivenController::coalesced(inner.clone(), rx);
        tx.send(1).unwrap();
        let _ = outer.tick().await.unwrap();
        assert_eq!(inner.ticks.load(Ordering::SeqCst), 1);
        // No new events → no new ticks.
        let _ = outer.tick().await.unwrap();
        assert_eq!(inner.ticks.load(Ordering::SeqCst), 1);
    }
}
