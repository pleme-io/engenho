//! FaceFederatedWatcher — Watcher that pumps a revoada
//! `Face::watch_resources()` stream into a typed [`Change`] channel.
//!
//! Completes the FaceGossipBroker symmetry: every peer's announce()
//! lands in the shared Face as a Pod-shape envelope; every peer's
//! FaceFederatedWatcher reads back the watch events + emits typed
//! Changes into its local Conduit.
//!
//! Gated `with-revoada`.

use crate::{Change, ChangeKind, FonteError, FonteResult, Watcher};
use async_trait::async_trait;
use engenho_revoada::face::{Face, FaceWatchEventKind, ResourceFormat};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Watcher backed by a revoada Face's watch stream. Pumps events
/// from the face's typed `watch_resources()` API into a tokio mpsc
/// channel; the Watcher trait's `next()` awaits the channel.
///
/// One pumper task per FaceFederatedWatcher; dropping the watcher
/// signals the task to exit on the next stream tick.
pub struct FaceFederatedWatcher {
    rx: TokioMutex<mpsc::Receiver<Change>>,
    _pump: JoinHandle<()>,
}

impl FaceFederatedWatcher {
    /// Subscribe to a face's watch stream for a given resource kind
    /// + namespace. Pumps watch events into a typed Change channel
    /// (capacity 32 — human-paced editing).
    ///
    /// # Errors
    ///
    /// Returns `FonteError::Watch` if the face rejects the watch
    /// subscription (e.g. unsupported format, face not started).
    pub fn subscribe(
        face: Arc<dyn Face>,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> FonteResult<Self> {
        let mut stream = face
            .watch_resources(kind, namespace, format)
            .map_err(|e| FonteError::Watch(format!("face watch_resources: {e}")))?;

        let (tx, rx) = mpsc::channel::<Change>(32);
        let revision = Arc::new(AtomicU64::new(0));
        let source: Arc<str> = format!("face/{}", face.name()).into();

        let cb_source = source.clone();
        let cb_rev = revision.clone();
        let pump = tokio::task::spawn_blocking(move || {
            // Synchronous next_event loop — runs until the stream
            // ends or the channel closes.
            loop {
                match stream.next_event() {
                    Ok(Some(ev)) => {
                        let kind_map = match ev.kind {
                            FaceWatchEventKind::Added => ChangeKind::Created,
                            FaceWatchEventKind::Modified => ChangeKind::Modified,
                            FaceWatchEventKind::Deleted => ChangeKind::Removed,
                            // Reset = re-fetch state from scratch.
                            // Treat as a Modified (operator re-runs
                            // last-applied config).
                            FaceWatchEventKind::Reset => ChangeKind::Modified,
                        };
                        let rev = cb_rev.fetch_add(1, Ordering::SeqCst);
                        let change = Change {
                            source: cb_source.clone(),
                            kind: kind_map,
                            source_text: Arc::from(String::from_utf8_lossy(&ev.body).into_owned()),
                            revision: rev,
                        };
                        // try_send fails when the receiver is dropped —
                        // exit cleanly.
                        if tx.blocking_send(change).is_err() {
                            return;
                        }
                    }
                    Ok(None) => return,
                    Err(_) => {
                        // Transport hiccup — sleep + retry. Real
                        // implementation would back off; for tests
                        // an immediate retry is fine.
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                }
            }
        });

        Ok(Self {
            rx: TokioMutex::new(rx),
            _pump: pump,
        })
    }
}

#[async_trait]
impl Watcher for FaceFederatedWatcher {
    async fn next(&self) -> FonteResult<Option<Change>> {
        Ok(self.rx.lock().await.recv().await)
    }
}
