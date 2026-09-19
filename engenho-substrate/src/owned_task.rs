//! [`OwnedTask`] — a background task that one value owns and stops by
//! request-THEN-await.
//!
//! ## Why the await is the whole point
//!
//! `JoinHandle::abort` only *requests* cancellation. The task's future, and
//! every `Arc` it captured or upgraded, is dropped later, the next time a
//! worker gets to it. So a task parked mid-`.await` while holding a strong
//! reference still holds it after `abort()` returns, for an unbounded time on
//! a busy runtime — and for no time at all only by luck, when the caller
//! happens to yield to the scheduler before it looks.
//!
//! The first site that paid for this was the store's bookmark ticker: it
//! upgraded its `Weak` and held the strong reference across an await on the
//! catalog lock, so aborting it without awaiting left the store's inner state
//! (and, on fjall, the data-directory lock) alive past the stop. Awaiting the
//! handle after the abort is the only point at which "the task is gone" is a
//! fact rather than a request. Promoted here so every crate that owns a task
//! stops it the same way.
//!
//! ## Two kinds of task, one stop
//!
//! * [`OwnedTask::spawn`] owns an async task. `stop` aborts it and awaits it.
//! * [`OwnedTask::spawn_blocking`] owns a closure on the blocking pool. A
//!   running closure cannot be aborted, so `stop` raises the closure's
//!   [`StopSignal`] and awaits its return. The closure MUST poll the signal
//!   and bound every wait it makes; one that blocks without bound makes `stop`
//!   wait without bound. That obligation is a convention the closure keeps,
//!   not a type — the tests of each blocking owner are what catch a breach.
//!
//! ## What is and is not guaranteed
//!
//! * On return from [`OwnedTask::stop`] the task has ended: an async task's
//!   future has been dropped, a blocking closure has returned. That holds for
//!   a second, concurrent `stop` too: the slot lock is held across the await,
//!   so the second caller cannot return before the first has seen the end.
//! * Dropping an `OwnedTask` without stopping it aborts the task (and raises a
//!   blocking closure's signal) and does NOT wait. That is the fallback for
//!   owners that are simply dropped; an owner that must know its task is gone
//!   calls `stop`.
//! * Nothing here stops a *different* task from upgrading a `Weak` to the same
//!   state. That is a residual, caught by tests, not prevented by a type.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// How an [`OwnedTask`] ended when it was stopped.
///
/// Four outcomes, four variants: a panic is never reported as a clean stop,
/// and "someone else already stopped it" is never reported as "I stopped it".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStop {
    /// It was still running and ended because it was stopped: an async task
    /// was aborted and awaited (its future, and everything it held, has been
    /// dropped); a blocking closure saw its [`StopSignal`] and returned.
    Cancelled,
    /// It had already returned on its own before the stop could land.
    Returned,
    /// It had panicked before the stop. It is gone, but it had not been doing
    /// its job for some time — a finding, not a clean stop.
    Panicked,
    /// An earlier `stop` already took it. Reported only once that earlier
    /// stop has finished awaiting it.
    AlreadyStopped,
}

/// No stop has been requested.
const RUNNING: u8 = 0;
/// The owner asked the closure to stop; the closure has not looked yet.
const REQUESTED: u8 = 1;
/// The closure read the request. It is ending because it was asked to.
const OBSERVED: u8 = 2;

/// The stop request a blocking [`OwnedTask`] closure polls.
///
/// Only the owner can raise it; the closure can only read it. Reading it once
/// it is raised is what makes the stop a [`TaskStop::Cancelled`] rather than
/// a [`TaskStop::Returned`].
#[derive(Debug)]
pub struct StopSignal {
    state: Arc<AtomicU8>,
}

impl StopSignal {
    fn new() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(RUNNING)),
        }
    }

    /// The owner's handle to the same request. Private: a closure that could
    /// clone its signal could not raise it anyway, and has no need to.
    fn share(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }

    /// Has the owner asked this task to stop? The closure calls this between
    /// bounded waits and returns promptly once it answers `true`.
    #[must_use]
    pub fn is_requested(&self) -> bool {
        match self
            .state
            .compare_exchange(REQUESTED, OBSERVED, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => true,
            Err(now) => now == OBSERVED,
        }
    }

    /// Raise the request. Idempotent; never lowers an `OBSERVED`.
    fn request(&self) {
        let _ =
            self.state
                .compare_exchange(RUNNING, REQUESTED, Ordering::AcqRel, Ordering::Acquire);
    }

    fn was_observed(&self) -> bool {
        self.state.load(Ordering::Acquire) == OBSERVED
    }
}

/// A spawned background task owned by one value and stopped by
/// request-then-await. See the [module docs](self) for why the await matters.
pub struct OwnedTask {
    /// `Some` until the first [`OwnedTask::stop`] has awaited the task. The
    /// async lock is held ACROSS that await, so a concurrent `stop` waits for
    /// it rather than returning `AlreadyStopped` while the task still runs.
    slot: Mutex<Option<JoinHandle<()>>>,
    /// `Some` exactly for a blocking task, whose closure cannot be aborted
    /// once it runs and so has to be asked.
    signal: Option<StopSignal>,
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
            signal: None,
        }
    }

    /// Run `work` on the current runtime's blocking pool and own it.
    ///
    /// `work` receives the [`StopSignal`] it must poll: `stop` cannot abort a
    /// running closure, only ask it, then wait for it to return. Bound every
    /// wait inside `work` (a receive with a timeout, never a bare `recv`), or
    /// `stop` inherits the unbounded wait.
    ///
    /// # Panics
    ///
    /// Outside a tokio runtime, exactly as `tokio::task::spawn_blocking` does.
    #[must_use = "dropping an OwnedTask asks its closure to stop at once"]
    pub fn spawn_blocking<F>(work: F) -> Self
    where
        F: FnOnce(StopSignal) + Send + 'static,
    {
        let signal = StopSignal::new();
        let for_work = signal.share();
        Self {
            slot: Mutex::new(Some(tokio::task::spawn_blocking(move || work(for_work)))),
            signal: Some(signal),
        }
    }

    /// Stop the task, then wait for it to end. On return an async task's
    /// future has been dropped, and a blocking closure has returned, with
    /// every reference either held.
    ///
    /// Idempotent: every call after the first returns
    /// [`TaskStop::AlreadyStopped`], and only after the first has finished.
    ///
    /// Cancel-safe: if this future is dropped mid-await, the handle stays in
    /// the slot (already asked to stop) and the next `stop` awaits it again.
    pub async fn stop(&self) -> TaskStop {
        let mut slot = self.slot.lock().await;
        let Some(handle) = slot.as_mut() else {
            return TaskStop::AlreadyStopped;
        };
        if let Some(signal) = &self.signal {
            signal.request();
        }
        // For a blocking closure this only prevents one that has not started
        // from starting; a running one ends through the signal.
        handle.abort();
        let joined = handle.await;
        // Cleared only after the await resolved: a JoinHandle that has
        // returned Ready must never be polled again.
        *slot = None;
        match joined {
            Ok(()) if self.signal.as_ref().is_some_and(StopSignal::was_observed) => {
                TaskStop::Cancelled
            }
            Ok(()) => TaskStop::Returned,
            Err(e) if e.is_cancelled() => TaskStop::Cancelled,
            Err(_) => TaskStop::Panicked,
        }
    }
}

impl Drop for OwnedTask {
    fn drop(&mut self) {
        if let Some(signal) = &self.signal {
            signal.request();
        }
        // `get_mut` needs no lock: `&mut self` proves no `stop` is running.
        if let Some(handle) = self.slot.get_mut() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Long enough that only a stop which never returns can exceed it.
    const STOP_BOUND: Duration = Duration::from_secs(5);

    /// How long a test closure keeps waiting for a stop request that never
    /// comes. Past [`STOP_BOUND`], so the assertion fails first; finite, so a
    /// failing test ends (the runtime's shutdown waits for blocking closures)
    /// instead of hanging the suite.
    const LEAK_GUARD: Duration = Duration::from_secs(10);

    /// Park a blocking closure until it is asked to stop, polling every
    /// millisecond, or until [`LEAK_GUARD`] runs out.
    fn park_until_stopped(stop: &StopSignal) {
        let parked = std::time::Instant::now();
        while !stop.is_requested() && parked.elapsed() < LEAK_GUARD {
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// A task parked forever while holding a clone of `held`.
    fn parked_holding(held: &Arc<()>) -> OwnedTask {
        let in_task = Arc::clone(held);
        OwnedTask::spawn(async move {
            std::future::pending::<()>().await;
            drop(in_task);
        })
    }

    /// A RUNNING blocking closure holding a clone of `held` until it is asked
    /// to stop, polling the request every millisecond. Returns only once the
    /// closure has started: one that has not can be aborted, which is not the
    /// case these tests are about.
    fn blocking_holding(held: &Arc<()>) -> OwnedTask {
        let in_task = Arc::clone(held);
        let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
        let task = OwnedTask::spawn_blocking(move |stop| {
            let _ = started_tx.send(());
            park_until_stopped(&stop);
            drop(in_task);
        });
        assert!(
            started_rx.recv_timeout(STOP_BOUND).is_ok(),
            "precondition: the closure started"
        );
        task
    }

    async fn bounded_stop(task: &OwnedTask) -> TaskStop {
        tokio::time::timeout(STOP_BOUND, task.stop())
            .await
            .unwrap_or_else(|_| panic!("stop did not return within {STOP_BOUND:?}"))
    }

    async fn stop_then_count(task: &OwnedTask, held: &Arc<()>) -> (TaskStop, usize) {
        let outcome = bounded_stop(task).await;
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

    /// A running blocking closure cannot be aborted. `stop` must still end it
    /// — by asking — and return only once it has returned.
    #[tokio::test]
    async fn stop_ends_a_running_blocking_closure_and_waits_for_its_return() {
        let held = Arc::new(());
        let task = blocking_holding(&held);

        assert_eq!(bounded_stop(&task).await, TaskStop::Cancelled);
        assert_eq!(
            Arc::strong_count(&held),
            1,
            "stop returned while the blocking closure still held its reference"
        );
        assert_eq!(task.stop().await, TaskStop::AlreadyStopped);
    }

    /// A closure that never looked at the signal ended on its own; saying it
    /// was cancelled would claim a stop that did not happen.
    #[tokio::test]
    async fn a_blocking_closure_that_returned_on_its_own_is_reported_returned() {
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let task = OwnedTask::spawn_blocking(move |_stop| {
            let _ = done_tx.send(());
        });
        let _ = done_rx.recv_timeout(STOP_BOUND);
        assert_eq!(bounded_stop(&task).await, TaskStop::Returned);
    }

    #[tokio::test]
    async fn a_blocking_closure_that_panicked_is_reported_panicked() {
        let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
        let task = OwnedTask::spawn_blocking(move |_stop| {
            let _ = started_tx.send(());
            panic!("owned-task test: deliberate blocking panic");
        });
        // Started, so the abort in `stop` cannot pre-empt the panic.
        assert!(started_rx.recv_timeout(STOP_BOUND).is_ok());
        assert_eq!(bounded_stop(&task).await, TaskStop::Panicked);
    }

    /// The drop fallback does not wait, but it must still ask: a dropped
    /// owner may not leave its RUNNING closure running forever. (A closure
    /// that had not started yet is prevented from starting by the abort, so
    /// the test waits for this one to start first.)
    #[tokio::test]
    async fn dropping_a_blocking_task_asks_its_closure_to_stop() {
        let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
        let (ended_tx, ended_rx) = std::sync::mpsc::channel::<()>();
        let task = OwnedTask::spawn_blocking(move |stop| {
            let _ = started_tx.send(());
            park_until_stopped(&stop);
            let _ = ended_tx.send(());
        });
        assert!(
            started_rx.recv_timeout(STOP_BOUND).is_ok(),
            "precondition: the closure started"
        );
        drop(task);
        assert!(
            ended_rx.recv_timeout(STOP_BOUND).is_ok(),
            "a dropped blocking task's closure was never asked to stop"
        );
    }
}
