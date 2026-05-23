//! The `Watcher` role — emits typed [`Change`]s when the source moves.

use crate::{Change, ChangeKind, FonteResult};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Source of typed changes feeding the convergence pipeline.
///
/// Real implementations (M1.1+) wrap shikumi-notify, a periodic
/// polling cycle, or an inbound API endpoint. The mock provided by
/// [`MockWatcher`] is sufficient for unit + integration tests.
#[async_trait]
pub trait Watcher: Send + Sync {
    /// Block until the next change is available, then return it.
    ///
    /// On graceful shutdown the trait returns `Ok(None)` — the
    /// supervisor treats it as the end-of-stream signal.
    async fn next(&self) -> FonteResult<Option<Change>>;
}

// ── Mock impl (always available) ─────────────────────────────────

/// In-memory `Watcher` backed by a `VecDeque`. Tests `push()` changes
/// before driving the supervisor; `next()` pops one per call.
#[derive(Debug, Default)]
pub struct MockWatcher {
    queue: Mutex<std::collections::VecDeque<Change>>,
}

impl MockWatcher {
    /// New empty mock.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Enqueue a change so the next `next()` call returns it.
    pub async fn push(&self, change: Change) {
        self.queue.lock().await.push_back(change);
    }

    /// Convenience: enqueue an `Initial`-kind change with `revision = 0`.
    pub async fn push_initial(
        &self,
        source: impl Into<Arc<str>>,
        source_text: impl Into<Arc<str>>,
    ) {
        self.push(Change {
            source: source.into(),
            kind: ChangeKind::Initial,
            source_text: source_text.into(),
            revision: 0,
        })
        .await;
    }
}

#[async_trait]
impl Watcher for MockWatcher {
    async fn next(&self) -> FonteResult<Option<Change>> {
        Ok(self.queue.lock().await.pop_front())
    }
}
