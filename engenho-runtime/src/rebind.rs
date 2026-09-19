//! A listener that stops serving binds again, on a growing backoff (T2.7).
//!
//! ## What this replaces
//!
//! T2.6 made each listener (:10250, :2379) a catalog child whose task
//! cannot end. When its bind failed or its server returned, it recorded
//! `Halted` and parked forever: the task stayed Running, the port stayed
//! closed, and only a restart of the whole daemon brought it back. A port
//! that was taken for a few seconds at boot — the previous process still
//! releasing it — cost `kubectl logs` and every etcd client for the life of
//! the process.
//!
//! ## What it does now
//!
//! [`serve_rebinding`] runs one bind-and-serve attempt at a time, forever.
//! After each attempt ends it records `Halted` (`Panicked` if the attempt
//! panicked), waits the next step of
//! [`REBIND`] (the same [`Curve`] every retry in the controllers reads), and
//! tries again. A port that stays taken is retried forever but never
//! hot-looped. An attempt that served for at least the curve's cap made
//! progress, so the failure after it starts the curve from the base again —
//! upstream's `BackoffUntil` resets on a run that made progress, the same
//! way.
//!
//! The heartbeat tells a reader which of the two states the listener is in:
//! in flight while an attempt runs (binding, then serving), and `Halted`
//! with nothing in flight while it waits to rebind.
//!
//! ## A panic in an attempt (W6)
//!
//! An attempt that panicked used to end the listener's task: the child was
//! Dead, nothing respawns a Dead child, and the port stayed closed for the
//! life of the process — the very outcome the rebind exists to prevent,
//! reached by a different road. The fault-injection matrix found it. An
//! attempt is now contained the way a Stateless tick is: its panic is
//! counted (it ends `Panicked`, which the heartbeat counts), and the
//! listener backs off and binds again like any attempt that ended.
//!
//! Containing it is sound because an attempt holds nothing across attempts:
//! `serve` builds each one afresh from clones, and what a listener serves
//! (the kubelet, the store) it reaches only from request tasks, which its
//! server spawns and tokio already isolates. A panic that comes back on
//! every attempt is retried on the curve, never hot-looped, and counted
//! each time.

use std::convert::Infallible;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::task::Poll;

use engenho_controllers::{Curve, Heartbeat, PanicMessage, Streak, TickClass};
use tracing::{error, warn};

use crate::child::Listener;

/// How long a listener whose serve ended waits before binding again: 1 s
/// doubling to 60 s.
pub(crate) const REBIND: Curve = Curve::from_millis::<1_000, 60_000>();

/// Run `serve` — one bind-and-serve attempt of `listener` — again every time
/// it ends or panics, waiting the next step of [`REBIND`] in between. It
/// never returns, and a panic in an attempt does not end it.
///
/// `serve` owns its logging: it says why its bind failed or its server
/// stopped. This says only that the listener is not serving and when it will
/// try again.
pub(crate) async fn serve_rebinding<F, Fut>(
    listener: Listener,
    beat: Arc<Heartbeat>,
    mut serve: F,
) -> Infallible
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    let mut misses = Streak::new(REBIND);
    loop {
        beat.begin();
        let began = tokio::time::Instant::now();
        let ended = match contained(serve()).await {
            Ok(()) => TickClass::Halted,
            Err(panic) => {
                error!(
                    listener = listener.name(),
                    %panic,
                    "listener's serve PANICKED and was contained; it holds nothing across \
                     attempts, so it binds again after the backoff"
                );
                TickClass::Panicked
            }
        };
        // `Panicked` is counted as a panic by the heartbeat itself.
        beat.end(ended);
        if began.elapsed() >= REBIND.cap() {
            misses.reset();
        }
        let after = misses.miss();
        warn!(
            listener = listener.name(),
            after_ms = u64::try_from(after.as_millis()).unwrap_or(u64::MAX),
            "listener is not serving; binding again after the backoff"
        );
        tokio::time::sleep(after).await;
    }
}

/// Run `fut` to completion, turning a panic in any poll of it into `Err`.
/// After a panic the future is never polled again.
///
/// `AssertUnwindSafe` is the caller's claim: here, that a listener's attempt
/// holds nothing across attempts (see the module docs).
///
/// `pending-dedup: contained` — this is
/// `engenho_controllers::contain::contained`, line for line, which is
/// `pub(crate)` in its crate. Export it there and this copy is deleted.
async fn contained<F: Future>(fut: F) -> Result<F::Output, PanicMessage> {
    let mut fut = std::pin::pin!(fut);
    std::future::poll_fn(move |cx| {
        match std::panic::catch_unwind(AssertUnwindSafe(|| fut.as_mut().poll(cx))) {
            Ok(poll) => poll.map(Ok),
            Err(payload) => Poll::Ready(Err(PanicMessage::of(payload.as_ref()))),
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use tokio::time::Instant;

    use super::*;

    /// A fake listener: every attempt records when it began, then serves for
    /// the duration `serves_for` gives that attempt's index (zero is a bind
    /// that fails at once).
    struct Attempts {
        began: Mutex<Vec<Instant>>,
        serves_for: fn(usize) -> Duration,
    }

    impl Attempts {
        fn new(serves_for: fn(usize) -> Duration) -> Arc<Self> {
            Arc::new(Self {
                began: Mutex::new(Vec::new()),
                serves_for,
            })
        }

        async fn attempt(self: Arc<Self>) {
            let index = {
                let mut began = self.began.lock().unwrap();
                began.push(Instant::now());
                began.len() - 1
            };
            tokio::time::sleep((self.serves_for)(index)).await;
        }

        /// The gap between the start of each attempt and the next.
        fn gaps(&self) -> Vec<Duration> {
            let began = self.began.lock().unwrap();
            began.windows(2).map(|w| w[1] - w[0]).collect()
        }
    }

    /// Run the loop over `attempts` for `run_for` of virtual time.
    async fn run_for(attempts: &Arc<Attempts>, beat: &Arc<Heartbeat>, run_for: Duration) {
        let task = tokio::spawn(serve_rebinding(Listener::KubeletHttp, beat.clone(), {
            let attempts = attempts.clone();
            move || attempts.clone().attempt()
        }));
        tokio::time::sleep(run_for).await;
        task.abort();
        let _ = task.await;
    }

    /// A port that stays taken: each bind fails at once. The listener binds
    /// again forever, at gaps that start at the base, grow at least 1.8x a
    /// step, and hold at the cap — never hot-looped, never abandoned.
    #[tokio::test(start_paused = true)]
    async fn a_listener_that_cannot_bind_retries_on_a_growing_curve() {
        let attempts = Attempts::new(|_| Duration::ZERO);
        let beat = Arc::new(Heartbeat::new());
        run_for(&attempts, &beat, Duration::from_secs(600)).await;
        let gaps = attempts.gaps();

        assert_eq!(gaps.first().copied(), Some(REBIND.base()), "{gaps:?}");
        assert!(gaps.len() >= 8, "ten minutes held only {gaps:?}");
        for pair in gaps.windows(2) {
            let (a, b) = (pair[0], pair[1]);
            assert!(b <= REBIND.cap(), "{b:?} is past the cap: {gaps:?}");
            if a < REBIND.cap() {
                assert!(
                    b == REBIND.cap() || b.as_micros() * 10 >= a.as_micros() * 18,
                    "{a:?} then {b:?}: the rebind gap did not grow toward the cap: {gaps:?}"
                );
            } else {
                assert_eq!(b, REBIND.cap(), "past the cap the gap holds: {gaps:?}");
            }
        }
        assert!(
            gaps.iter().filter(|g| **g == REBIND.cap()).count() >= 3,
            "ten minutes of failed binds never reached the cap: {gaps:?}"
        );

        let snap = beat.snapshot();
        assert_eq!(
            usize::try_from(snap.ticks_started).unwrap(),
            gaps.len() + 1,
            "every attempt is a beat: {snap:?}"
        );
        assert_eq!(snap.last_class, Some(TickClass::Halted), "{snap:?}");
    }

    /// Four failed binds walk the curve out to 8 s; the fifth attempt binds
    /// and serves for the whole cap. When that serve ends, the listener has
    /// made progress: it binds again after the BASE, not after 16 s.
    #[tokio::test(start_paused = true)]
    async fn a_serve_that_stayed_up_starts_the_curve_again() {
        let attempts = Attempts::new(|i| if i == 4 { REBIND.cap() } else { Duration::ZERO });
        let beat = Arc::new(Heartbeat::new());
        run_for(&attempts, &beat, Duration::from_secs(120)).await;
        let gaps = attempts.gaps();

        assert_eq!(
            gaps.get(..4),
            Some(&[1, 2, 4, 8].map(Duration::from_secs)[..]),
            "{gaps:?}"
        );
        assert_eq!(
            gaps.get(4).copied(),
            Some(REBIND.cap() + REBIND.base()),
            "after serving for the cap the next bind waits only the base: {gaps:?}"
        );
    }

    /// While an attempt runs the listener's beat is in flight; while it
    /// waits to rebind it is not, and its last class is `Halted`.
    #[tokio::test(start_paused = true)]
    async fn the_heartbeat_says_serving_or_waiting() {
        let attempts = Attempts::new(|i| {
            if i == 0 {
                Duration::from_secs(10)
            } else {
                Duration::ZERO
            }
        });
        let beat = Arc::new(Heartbeat::new());
        let task = tokio::spawn(serve_rebinding(Listener::EtcdFacade, beat.clone(), {
            let attempts = attempts.clone();
            move || attempts.clone().attempt()
        }));

        tokio::time::sleep(Duration::from_secs(5)).await;
        let serving = beat.snapshot();
        // The first serve ends at 10 s; the rebind is due at 11 s.
        tokio::time::sleep(Duration::from_millis(5_500)).await;
        let waiting = beat.snapshot();
        task.abort();
        let _ = task.await;

        assert!(serving.in_flight(), "{serving:?}");
        assert_eq!(serving.last_class, None, "{serving:?}");
        assert!(!waiting.in_flight(), "{waiting:?}");
        assert_eq!(waiting.last_class, Some(TickClass::Halted), "{waiting:?}");
    }

    /// An attempt that panics does not end the listener: the panic is
    /// counted, the heartbeat says `Panicked` with nothing in flight, and
    /// the listener binds again on the same curve as a failed bind — the
    /// port is not lost for the life of the process.
    #[tokio::test(start_paused = true)]
    async fn a_serve_that_panics_is_counted_and_binds_again() {
        let beat = Arc::new(Heartbeat::new());
        let attempts = Attempts::new(|_| Duration::ZERO);
        let task = tokio::spawn(serve_rebinding(Listener::EtcdFacade, beat.clone(), {
            let attempts = attempts.clone();
            move || {
                let attempts = attempts.clone();
                async move {
                    attempts.attempt().await;
                    panic!("the serve attempt tripped over something");
                }
            }
        }));

        // Attempts at 0 s, 1 s and 3 s; at 3.5 s it waits for the one at 7 s.
        tokio::time::sleep(Duration::from_millis(3_500)).await;
        let waiting = beat.snapshot();
        let ended = task.is_finished();
        task.abort();
        let _ = task.await;

        assert!(!ended, "a panicking attempt ended the listener's task");
        assert_eq!(
            attempts.gaps(),
            [REBIND.base(), REBIND.base() * 2],
            "a panicking attempt is retried on the rebind curve"
        );
        assert_eq!(waiting.panics, 3, "every panic is counted: {waiting:?}");
        assert_eq!(waiting.ticks_started, 3, "{waiting:?}");
        assert!(!waiting.in_flight(), "{waiting:?}");
        assert_eq!(waiting.last_class, Some(TickClass::Panicked), "{waiting:?}");
    }
}
