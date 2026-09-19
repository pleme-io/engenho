//! FaceFederatedWatcher — Watcher that pumps a revoada
//! `Face::watch_resources()` stream into a typed [`Change`] channel.
//!
//! Completes the FaceGossipBroker symmetry: every peer's announce()
//! lands in the shared Face as a Pod-shape envelope; every peer's
//! FaceFederatedWatcher reads back the watch events + emits typed
//! Changes into its local Conduit.
//!
//! ## The pump is owned, and stoppable without an event
//!
//! A face watch stream is synchronous, so the pump runs on the blocking
//! pool as an [`OwnedTask`]. A running blocking closure cannot be aborted;
//! it can only be asked. So every wait the pump makes is bounded by
//! [`FaceFederatedWatcher::STOP_POLL`] — the wait for the next event and the
//! wait for room in the channel — and between them it reads its stop signal. A pump that waited
//! on the stream without a bound kept the face subscription (and a
//! blocking-pool thread) until the next event for its kind, which for a
//! quiet kind is never.
//!
//! Gated `with-revoada`.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use crate::{Change, ChangeKind, FonteError, FonteResult, Watcher};
use async_trait::async_trait;
use engenho_revoada::face::{
    Face, FaceWatchEvent, FaceWatchEventKind, FaceWatchStream, ResourceFormat, WatchPoll,
};
use engenho_substrate::{OwnedTask, StopSignal, TaskStop};
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

/// Watcher backed by a revoada Face's watch stream. Pumps events
/// from the face's typed `watch_resources()` API into a tokio mpsc
/// channel; the Watcher trait's `next()` awaits the channel.
///
/// One owned pump per FaceFederatedWatcher. [`FaceFederatedWatcher::stop`]
/// ends it and waits until it has released the face subscription; dropping
/// the watcher asks it to end (within [`Self::STOP_POLL`]) without waiting.
pub struct FaceFederatedWatcher {
    rx: TokioMutex<mpsc::Receiver<Change>>,
    pump: OwnedTask,
}

impl FaceFederatedWatcher {
    /// The longest the pump waits — for an event, or for room in the channel
    /// — before it reads its stop signal again. So also the longest
    /// [`stop`](Self::stop) waits for a pump that is doing nothing.
    pub const STOP_POLL: Duration = Duration::from_millis(50);

    /// Subscribe to a face's watch stream for a given resource kind
    /// + namespace. Pumps watch events into a typed Change channel
    /// (capacity 32 — human-paced editing).
    ///
    /// # Errors
    ///
    /// Returns `FonteError::Watch` if the face rejects the watch
    /// subscription (e.g. unsupported format, face not started).
    ///
    /// # Panics
    ///
    /// Outside a tokio runtime, as `tokio::task::spawn_blocking` does.
    pub fn subscribe(
        face: Arc<dyn Face>,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> FonteResult<Self> {
        let stream = face
            .watch_resources(kind, namespace, format)
            .map_err(|e| FonteError::Watch(format!("face watch_resources: {e}")))?;

        let (tx, rx) = mpsc::channel::<Change>(32);
        let source: Arc<str> = format!("face/{}", face.name()).into();
        let pump = OwnedTask::spawn_blocking(move |stop| pump(stream, &tx, &source, &stop));

        Ok(Self {
            rx: TokioMutex::new(rx),
            pump,
        })
    }

    /// End the pump and wait for it. On return the face watch stream has
    /// been dropped, so the face releases the subscription at its next
    /// event.
    ///
    /// [`TaskStop::Returned`] means the stream had already ended on its own
    /// (the face shut down); [`TaskStop::Panicked`] means nothing was being
    /// pumped for some time before this call.
    pub async fn stop(&self) -> TaskStop {
        self.pump.stop().await
    }
}

/// The pump: move events from `stream` to `tx` until the stream ends, the
/// watcher is gone, or the owner asks it to stop. Every wait is bounded by
/// [`FaceFederatedWatcher::STOP_POLL`].
fn pump(
    mut stream: Box<dyn FaceWatchStream>,
    tx: &mpsc::Sender<Change>,
    source: &Arc<str>,
    stop: &StopSignal,
) {
    let mut revision: u64 = 0;
    while !stop.is_requested() {
        match stream.poll_event(FaceFederatedWatcher::STOP_POLL) {
            Ok(WatchPoll::Event(event)) => {
                let change = change_from(event, source, revision);
                revision = revision.wrapping_add(1);
                if deliver(tx, change, stop).is_break() {
                    return;
                }
            }
            Ok(WatchPoll::Idle) => {}
            Ok(WatchPoll::Ended) => return,
            // Transport hiccup: retry after one bounded pause, so a failing
            // stream is still stoppable.
            Err(_) => std::thread::sleep(FaceFederatedWatcher::STOP_POLL),
        }
    }
}

fn change_from(event: FaceWatchEvent, source: &Arc<str>, revision: u64) -> Change {
    let kind = match event.kind {
        FaceWatchEventKind::Added => ChangeKind::Created,
        FaceWatchEventKind::Modified => ChangeKind::Modified,
        FaceWatchEventKind::Deleted => ChangeKind::Removed,
        // Reset = re-fetch state from scratch. Treat as a Modified
        // (operator re-runs last-applied config).
        FaceWatchEventKind::Reset => ChangeKind::Modified,
    };
    Change {
        source: Arc::clone(source),
        kind,
        source_text: Arc::from(String::from_utf8_lossy(&event.body).into_owned()),
        revision,
    }
}

/// Hand `change` to the watcher, waiting for room at most
/// [`FaceFederatedWatcher::STOP_POLL`] at a time. `Break` once there is no
/// one to deliver to: the watcher dropped its receiver, or the owner asked
/// the pump to stop while the channel was full.
fn deliver(tx: &mpsc::Sender<Change>, mut change: Change, stop: &StopSignal) -> ControlFlow<()> {
    loop {
        match tx.try_send(change) {
            Ok(()) => return ControlFlow::Continue(()),
            Err(TrySendError::Closed(_)) => return ControlFlow::Break(()),
            Err(TrySendError::Full(back)) => {
                if stop.is_requested() {
                    return ControlFlow::Break(());
                }
                change = back;
                std::thread::sleep(FaceFederatedWatcher::STOP_POLL);
            }
        }
    }
}

#[async_trait]
impl Watcher for FaceFederatedWatcher {
    async fn next(&self) -> FonteResult<Option<Change>> {
        Ok(self.rx.lock().await.recv().await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_revoada::{FabricFace, FaceKind, PureRaftFace};

    /// Far longer than any stop that ends: only a pump that never reads its
    /// stop signal, or waits on something without a bound, exceeds it.
    const STOP_BOUND: Duration = Duration::from_secs(5);

    /// The watcher's channel capacity, as `subscribe` builds it.
    const CHANNEL_CAPACITY: usize = 32;

    fn started_face(name: &str) -> Arc<dyn Face> {
        let face = PureRaftFace::from_declaration(&FabricFace {
            name: name.into(),
            kind: FaceKind::PureRaft,
        })
        .expect("PureRaft face declares");
        face.start().expect("face starts");
        Arc::new(face)
    }

    fn pod(name: &str) -> Vec<u8> {
        format!(
            "apiVersion: v1\nkind: Pod\nmetadata:\n  name: {name}\n  namespace: default\nspec:\n  containers:\n    - name: c\n      image: nginx\n"
        )
        .into_bytes()
    }

    fn watch_pods(face: &Arc<dyn Face>) -> FaceFederatedWatcher {
        FaceFederatedWatcher::subscribe(
            Arc::clone(face),
            "Pod",
            Some("default"),
            ResourceFormat::Yaml,
        )
        .expect("a started PureRaft face watches Pods")
    }

    /// After `stop`, the face must find the subscription dead at its next
    /// broadcast and prune it. A pump still parked on the stream keeps it.
    fn assert_subscription_released(face: &Arc<dyn Face>) {
        face.apply_resource(ResourceFormat::Yaml, &pod("probe-after-stop"))
            .expect("probe apply");
        assert_eq!(
            face.subscriber_count(),
            0,
            "the pump still held the face watch stream after stop returned"
        );
    }

    async fn stop_within_bound(watcher: &FaceFederatedWatcher, why: &str) -> TaskStop {
        tokio::time::timeout(STOP_BOUND, watcher.stop())
            .await
            .unwrap_or_else(|_| panic!("stop did not return within {STOP_BOUND:?}: {why}"))
    }

    #[tokio::test]
    async fn the_pump_turns_face_events_into_changes() {
        let face = started_face("ffw-pumps");
        let watcher = watch_pods(&face);

        face.apply_resource(ResourceFormat::Yaml, &pod("web"))
            .expect("apply");
        let change = tokio::time::timeout(STOP_BOUND, watcher.next())
            .await
            .expect("an applied Pod reaches the watcher")
            .expect("next")
            .expect("the stream is open");

        assert_eq!(change.kind, ChangeKind::Created);
        assert_eq!(&*change.source, "face/ffw-pumps");
        assert_eq!(change.revision, 0);
        // The body is whatever envelope the face emits; only its presence is
        // this watcher's promise.
        assert!(!change.source_text.is_empty());
    }

    /// The defect: a pump waiting on the stream without a bound could not see
    /// a stop until the next event, and a quiet kind has none.
    #[tokio::test]
    async fn stop_ends_an_idle_pump_and_releases_the_subscription() {
        let face = started_face("ffw-stop-idle");
        let watcher = watch_pods(&face);
        assert_eq!(face.subscriber_count(), 1, "precondition: subscribed");
        face.apply_resource(ResourceFormat::Yaml, &pod("only"))
            .expect("apply");
        // Delivered, so the pump is RUNNING (one not yet started is simply
        // prevented from starting) and back waiting on a quiet stream.
        let first = tokio::time::timeout(STOP_BOUND, watcher.next())
            .await
            .expect("the pump delivers")
            .expect("next");
        assert!(first.is_some(), "precondition: the pump is running");

        let outcome = stop_within_bound(&watcher, "the pump never read its stop signal").await;

        assert_eq!(outcome, TaskStop::Cancelled);
        assert_subscription_released(&face);
        assert_eq!(watcher.stop().await, TaskStop::AlreadyStopped);
    }

    /// Nobody reads, so the pump fills the channel from the 40 replayed Pods
    /// and waits for room. That wait must be bounded too.
    #[tokio::test]
    async fn stop_ends_a_pump_waiting_on_a_full_channel() {
        let face = started_face("ffw-stop-full");
        for i in 0..40 {
            face.apply_resource(ResourceFormat::Yaml, &pod(&format!("p{i}")))
                .expect("apply");
        }
        let watcher = watch_pods(&face);
        // Precondition, observed rather than slept for: the channel is full,
        // so the pump holds the 33rd change and is waiting for room.
        tokio::time::timeout(STOP_BOUND, async {
            while watcher.rx.lock().await.len() < CHANNEL_CAPACITY {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("precondition: the pump filled the channel");

        let outcome = stop_within_bound(
            &watcher,
            "the pump waited on a full channel without a bound",
        )
        .await;

        assert_eq!(outcome, TaskStop::Cancelled);
        assert_subscription_released(&face);
    }
}
