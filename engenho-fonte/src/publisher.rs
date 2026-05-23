//! The `Publisher` role — broadcasts the convergence [`Outcome`] so
//! mirante (or any other observer) sees it.

use crate::{FonteResult, Outcome};
use async_trait::async_trait;
use std::sync::Mutex;

/// Typed Publisher. Surfaces the terminal [`Outcome`] to subscribers.
#[async_trait]
pub trait Publisher: Send + Sync {
    /// Publish the outcome.
    async fn publish(&self, outcome: &Outcome) -> FonteResult<()>;
}

// ── Mock impl (always available) ─────────────────────────────────

/// In-memory `Publisher` that appends outcomes to a Vec. Tests
/// inspect `outcomes()` to verify the pipeline reached the publisher
/// for every change.
#[derive(Debug)]
pub struct MockPublisher {
    seen: Mutex<Vec<Outcome>>,
}

impl Default for MockPublisher {
    fn default() -> Self {
        Self::new()
    }
}

impl MockPublisher {
    /// New empty publisher.
    #[must_use]
    pub fn new() -> Self {
        Self {
            seen: Mutex::new(Vec::new()),
        }
    }

    /// Borrow the published outcomes.
    pub fn outcomes(&self) -> Vec<Outcome> {
        self.seen.lock().expect("mock publisher poisoned").clone()
    }
}

#[async_trait]
impl Publisher for MockPublisher {
    async fn publish(&self, outcome: &Outcome) -> FonteResult<()> {
        self.seen
            .lock()
            .expect("mock publisher poisoned")
            .push(outcome.clone());
        Ok(())
    }
}
