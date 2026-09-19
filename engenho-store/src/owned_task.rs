//! [`OwnedTask`] — a background task that one value owns and stops by
//! abort-THEN-await.
//!
//! ## Why the await is the whole point
//!
//! `JoinHandle::abort` only *requests* cancellation. The task's future, and
//! every `Arc` it captured or upgraded, is dropped later, the next time a
//! worker gets to it. So a task parked mid-`.await` while holding a strong
//! reference still holds it after `abort()` returns, for an unbounded time on
//! a busy runtime.
//!
//! The store has exactly that shape: [`crate::watch_backend::BookmarkTicker`]
//! upgrades its `Weak` and holds the strong reference across
//! `tick_once().await`, which waits on the catalog lock. Aborting it without
//! awaiting left the store's inner state alive past the stop — for the fjall
//! backend that includes the data-directory lock, so a reopen straight after
//! could be refused as "held". Awaiting the handle after the abort is the only
//! point at which "the task is gone" is a fact rather than a request.
//!
//! ## What is and is not guaranteed
//!
//! * On return from [`OwnedTask::stop`] the task's future has been dropped.
//!   That holds for a second, concurrent `stop` too: the slot lock is held
//!   across the await, so the second caller cannot return before the first
//!   has seen the task end.
//! * Dropping an `OwnedTask` without stopping it aborts the task and does NOT
//!   wait. That is the fallback for owners that are simply dropped; an owner
//!   that must know its task is gone calls `stop`.
//! * Nothing here stops a *different* task from upgrading a `Weak` to the same
//!   state. That is a residual, caught by tests, not prevented by a type.

use std::future::Future;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// How an [`OwnedTask`] ended when it was stopped.
///
/// Four outcomes, four variants: a panic is never reported as a clean stop,
/// and "someone else already stopped it" is never reported as "I stopped it".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStop {
    /// It was still running; it was aborted and awaited, so its future (and
    /// everything the future held) has been dropped.
    Cancelled,
    /// It had already returned on its own before the abort could land.
    Returned,
    /// It had panicked before the stop. It is gone, but it had not been doing
    /// its job for some time — a finding, not a clean stop.
    Panicked,
    /// An earlier `stop` already took it. Reported only once that earlier
    /// stop has finished awaiting it.
    AlreadyStopped,
}

/// A spawned background task owned by one value and stopped by abort-then-
/// await. See the [module docs](self) for why the await matters.
pub struct OwnedTask {
    /// `Some` until the first [`OwnedTask::stop`] has awaited the task. The
    /// async lock is held ACROSS that await, so a concurrent `stop` waits for
    /// it rather than returning `AlreadyStopped` while the task still runs.
    slot: Mutex<Option<JoinHandle<()>>>,
}

impl OwnedTask {
    /// Spawn `future` on the current tokio runtime and own it.
    ///
    /// # Panics
    ///
    /// Outside a tokio runtime, exactly as `tokio::spawn` does.
    #[must_use = "dropping an OwnedTask aborts its task at once"]
    pub fn spawn<F>(future: F) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Self {
            slot: Mutex::new(Some(tokio::spawn(future))),
        }
    }

    /// Abort the task, then wait for it to end. On return its future has been
    /// dropped, with every reference it held.
    ///
    /// Idempotent: every call after the first returns
    /// [`TaskStop::AlreadyStopped`], and only after the first has finished.
    ///
    /// Cancel-safe: if this future is dropped mid-await, the handle stays in
    /// the slot (already aborted) and the next `stop` awaits it again.
    pub async fn stop(&self) -> TaskStop {
        let mut slot = self.slot.lock().await;
        let Some(handle) = slot.as_mut() else {
            return TaskStop::AlreadyStopped;
        };
        handle.abort();
        let joined = handle.await;
        // Cleared only after the await resolved: a JoinHandle that has
        // returned Ready must never be polled again.
        *slot = None;
        match joined {
            Ok(()) => TaskStop::Returned,
            Err(e) if e.is_cancelled() => TaskStop::Cancelled,
            Err(_) => TaskStop::Panicked,
        }
    }
}

impl Drop for OwnedTask {
    fn drop(&mut self) {
        // `get_mut` needs no lock: `&mut self` proves no `stop` is running.
        if let Some(handle) = self.slot.get_mut() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// A task parked forever while holding a clone of `held`.
    fn parked_holding(held: &Arc<()>) -> OwnedTask {
        let in_task = Arc::clone(held);
        OwnedTask::spawn(async move {
            std::future::pending::<()>().await;
            drop(in_task);
        })
    }

    async fn stop_then_count(task: &OwnedTask, held: &Arc<()>) -> (TaskStop, usize) {
        let outcome = task.stop().await;
        (outcome, Arc::strong_count(held))
    }

    /// The defect this type exists for: a task parked mid-await holding a
    /// strong reference. After `stop` returns, that reference is gone.
    #[tokio::test]
    async fn stop_returns_only_after_the_task_dropped_what_it_held() {
        let held = Arc::new(());
        let task = parked_holding(&held);
        assert_eq!(
            Arc::strong_count(&held),
            2,
            "precondition: the task holds a ref"
        );

        assert_eq!(task.stop().await, TaskStop::Cancelled);
        assert_eq!(
            Arc::strong_count(&held),
            1,
            "stop returned while the aborted task still held its reference"
        );
    }

    /// A second `stop` racing the first must not report the task gone before
    /// it is gone.
    #[tokio::test]
    async fn a_concurrent_second_stop_waits_for_the_first() {
        let held = Arc::new(());
        let task = parked_holding(&held);

        let (a, b) = tokio::join!(stop_then_count(&task, &held), stop_then_count(&task, &held));

        let mut outcomes = [a.0, b.0];
        outcomes.sort_by_key(|o| *o == TaskStop::AlreadyStopped);
        assert_eq!(
            outcomes,
            [TaskStop::Cancelled, TaskStop::AlreadyStopped],
            "exactly one stop cancels; the other reports it already stopped"
        );
        assert_eq!(a.1, 1, "first stop returned before the task was gone");
        assert_eq!(b.1, 1, "second stop returned before the task was gone");
    }

    #[tokio::test]
    async fn a_task_that_returned_on_its_own_is_reported_returned() {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let task = OwnedTask::spawn(async move {
            let _ = done_tx.send(());
        });
        // The send and the return happen in the same poll, so once this
        // resolves the task has completed.
        let _ = done_rx.await;
        assert_eq!(task.stop().await, TaskStop::Returned);
        assert_eq!(task.stop().await, TaskStop::AlreadyStopped);
    }

    #[tokio::test]
    async fn a_task_that_panicked_is_reported_panicked_not_cancelled() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
        let task = OwnedTask::spawn(async move {
            let _ = started_tx.send(());
            panic!("owned-task test: deliberate panic");
        });
        // The panic unwinds in the same poll as the send, so once this
        // resolves the task has already panicked.
        let _ = started_rx.await;
        assert_eq!(task.stop().await, TaskStop::Panicked);
    }
}
