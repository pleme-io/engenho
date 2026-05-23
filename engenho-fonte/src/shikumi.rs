//! `with-shikumi` — real [`Watcher`] driven by `shikumi::ConfigWatcher`.
//!
//! `ShikumiWatcher::new(path)` registers a notify-based callback that
//! re-reads the file on every Modify event + pushes a typed [`Change`]
//! into an internal channel. The Watcher trait's `next()` awaits the
//! channel.
//!
//! Behind the `with-shikumi` Cargo feature. Adds shikumi as a
//! cross-workspace path dep — fonte still builds without it.

use crate::{Change, ChangeKind, FonteError, FonteResult, Watcher};
use async_trait::async_trait;
use shikumi::ConfigWatcher;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;

/// `Watcher` impl that emits a [`Change`] every time the watched
/// file is modified, created, or removed on disk.
pub struct ShikumiWatcher {
    rx: tokio::sync::Mutex<mpsc::Receiver<Change>>,
    // Owns the underlying ConfigWatcher; dropping this struct stops
    // the file-watch.
    _watcher: ConfigWatcher,
}

impl ShikumiWatcher {
    /// Build a ShikumiWatcher pinned to `path`. Reads the file once
    /// to emit the `Initial` change, then forwards subsequent Modify
    /// / Create / Remove events as typed [`Change`]s on a bounded
    /// mpsc channel (capacity 32 — sufficient for human-paced
    /// editing; production tuning M1.1.1).
    ///
    /// # Errors
    ///
    /// Returns `FonteError::Watch` if the initial read fails or the
    /// notify watcher cannot be created.
    pub fn new(path: impl AsRef<Path>) -> FonteResult<Self> {
        let path = path.as_ref().to_path_buf();
        let (tx, rx) = mpsc::channel::<Change>(32);
        let revision = Arc::new(AtomicU64::new(0));
        let source: Arc<str> = path.display().to_string().into();

        // Push the initial-read change.
        let initial = read_change(&path, &source, &revision, ChangeKind::Initial)?;
        let _ = tx.try_send(initial);

        // Notify callback: classify event kind, re-read, push.
        let cb_source = source.clone();
        let cb_path = path.clone();
        let cb_tx = tx.clone();
        let cb_revision = revision.clone();
        let watcher_path = path.clone();
        let last_seen: Arc<Mutex<Option<u64>>> = Arc::new(Mutex::new(None));
        let cb_last = last_seen.clone();
        let watcher = ConfigWatcher::watch(&watcher_path, move |event: notify::Event| {
            use notify::EventKind;
            let kind = match event.kind {
                EventKind::Create(_) => ChangeKind::Created,
                EventKind::Modify(_) => ChangeKind::Modified,
                EventKind::Remove(_) => ChangeKind::Removed,
                _ => return,
            };
            // Coalesce identical revisions if notify fires twice (some
            // OSes emit Modify(Metadata) + Modify(Data) per save).
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            {
                let mut last = cb_last.lock().expect("shikumi-watcher poisoned");
                if let Some(prev) = *last
                    && now.saturating_sub(prev) < 50
                {
                    return;
                }
                *last = Some(now);
            }
            match read_change(&cb_path, &cb_source, &cb_revision, kind) {
                Ok(change) => {
                    let _ = cb_tx.try_send(change);
                }
                Err(_) => {
                    // File transiently absent — caller's next read
                    // will re-emit when the file reappears.
                }
            }
        })
        .map_err(|e| FonteError::Watch(format!("shikumi watch: {e}")))?;

        Ok(Self {
            rx: tokio::sync::Mutex::new(rx),
            _watcher: watcher,
        })
    }
}

fn read_change(
    path: &PathBuf,
    source: &Arc<str>,
    revision: &Arc<AtomicU64>,
    kind: ChangeKind,
) -> FonteResult<Change> {
    let source_text = match kind {
        // On Remove, we synthesize an empty body so downstream sees
        // a deletion event.
        ChangeKind::Removed => Arc::from(""),
        _ => {
            let body = std::fs::read_to_string(path)
                .map_err(|e| FonteError::Watch(format!("read {}: {e}", path.display())))?;
            Arc::from(body)
        }
    };
    let rev = revision.fetch_add(1, Ordering::SeqCst);
    Ok(Change {
        source: source.clone(),
        kind,
        source_text,
        revision: rev,
    })
}

#[async_trait]
impl Watcher for ShikumiWatcher {
    async fn next(&self) -> FonteResult<Option<Change>> {
        Ok(self.rx.lock().await.recv().await)
    }
}
