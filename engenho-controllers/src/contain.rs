//! Panic containment for a driver's tick (T2.7).
//!
//! ## What a panic in a tick used to do
//!
//! A `tick()` that panicked unwound through the driver loop and ended the
//! driver's task. T2.6 made that visible — the runtime's child set sees the
//! task end and marks the child Dead — but a Dead child is never respawned,
//! so one bad object that tripped an `unwrap` retired a whole controller for
//! the life of the process. Every other object of that kind stopped
//! converging with it.
//!
//! ## What it does now, and why only for some children
//!
//! What a panic may do depends on what the tick leaves behind, which is the
//! child's [`TickState`]:
//!
//! * **Stateless** — the tick re-reads everything it needs from the store
//!   every time, so nothing it half-did survives into the next tick except
//!   what the store holds, and the store is consistent by construction. The
//!   driver contains the panic ([`contained`]), turns it into
//!   [`crate::ControllerError::Panicked`] and carries on. That error class
//!   gets no targeted retry: the next event or the fallback re-ticks it. A
//!   panic caused by bad data therefore costs one counted tick per fallback
//!   interval, not the controller. This is what controller-runtime's
//!   `RecoverPanic` relies on, and for the same reason.
//! * **Stateful** — the tick holds in-memory state across ticks: a map it
//!   half-updated, a std `Mutex` the panic poisoned, a tokio lock guarding a
//!   torn value. Re-ticking over that is acting on state that is no longer
//!   true. The driver does not contain the panic; it ends the child's task,
//!   and the runtime marks it Dead and logs it at ERROR. `/livez` reports it
//!   only once a liveness source reads the children (T2.8); until then the
//!   ERROR line is the notice. For the kubelet, Dead is the park.
//!
//! When in doubt a child is Stateful: the wrong answer in that direction
//! costs availability, the wrong answer in the other costs correctness.
//!
//! ## Tier
//!
//! Mitigated, not unrepresentable. A contained panic is still a bug; it is
//! counted (in the driver's heartbeat, and by the runtime's process-wide
//! panic hook) so it is seen, and it is re-ticked so it costs one tick
//! instead of one controller. Whether a child is Stateless is a judgement
//! recorded in the runtime's catalog, not something the compiler checks.

use std::any::Any;
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::task::Poll;

/// Whether a child's tick leaves state behind that the next tick relies on.
///
/// Decides what a panic inside the tick does; see the [module
/// docs](self). The runtime's child catalog declares one per child, and the
/// [`crate::WatchDriver`] acts on it through
/// [`crate::WatchDriverConfig::tick_state`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TickState {
    /// Re-reads everything from the store every tick. A panic in a tick is
    /// contained, counted and re-ticked by the next event or the fallback.
    Stateless,
    /// Holds in-memory state across ticks. A panic in a tick is not
    /// contained: it ends the child, which its owner marks Dead.
    Stateful,
}

/// What a panic said, as far as its payload lets anyone tell.
///
/// A panic payload is `Box<dyn Any + Send>`. `panic!("…")` with no
/// arguments carries a `&'static str`, a formatted `panic!` carries a
/// `String`, and `std::panic::panic_any` can carry anything at all — which
/// is not text, and is said to be not text rather than printed as nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PanicMessage {
    /// The payload was text.
    Text(String),
    /// The payload was some other type.
    Opaque,
}

impl PanicMessage {
    /// Read a panic payload.
    #[must_use]
    pub fn of(payload: &(dyn Any + Send)) -> Self {
        payload
            .downcast_ref::<&str>()
            .map(|text| (*text).to_owned())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .map_or(Self::Opaque, Self::Text)
    }
}

impl fmt::Display for PanicMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text(text) => f.write_str(text),
            Self::Opaque => f.write_str("(the panic payload is not text)"),
        }
    }
}

/// Run `fut` to completion, turning a panic in any poll of it into
/// `Err`.
///
/// The future is polled inside [`std::panic::catch_unwind`] each time it is
/// polled, so a panic after an `.await` is caught as surely as one before
/// the first. After a panic the future is never polled again.
///
/// `AssertUnwindSafe` is the whole question, and the answer is the caller's:
/// only a [`TickState::Stateless`] tick may be run through this, because
/// only its state is all re-read from the store on the next tick.
pub(crate) async fn contained<F: Future>(fut: F) -> Result<F::Output, PanicMessage> {
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
    use super::*;

    #[tokio::test]
    async fn a_future_that_completes_is_returned_as_is() {
        assert_eq!(contained(async { 7 }).await, Ok(7));
    }

    #[tokio::test]
    async fn a_panic_before_the_first_await_is_caught_with_its_text() {
        let got = contained(async {
            panic!("bad object");
        })
        .await;
        assert_eq!(got, Err::<(), _>(PanicMessage::Text("bad object".into())));
    }

    /// The panic lands on a LATER poll, after the future has already
    /// returned `Pending` once: containment covers every poll, not only the
    /// first.
    #[tokio::test]
    async fn a_panic_after_an_await_is_caught_too() {
        let code = 42;
        let got = contained(async move {
            tokio::task::yield_now().await;
            panic!("index {code} out of range");
        })
        .await;
        assert_eq!(
            got,
            Err::<(), _>(PanicMessage::Text("index 42 out of range".into()))
        );
    }

    #[test]
    fn a_payload_that_is_not_text_is_said_to_be_opaque() {
        let payload: Box<dyn Any + Send> = Box::new(17_u32);
        assert_eq!(PanicMessage::of(payload.as_ref()), PanicMessage::Opaque);
        assert_ne!(PanicMessage::Opaque.to_string(), "");
    }

    #[test]
    fn both_text_payload_shapes_are_read() {
        let literal: Box<dyn Any + Send> = Box::new("literal");
        let formatted: Box<dyn Any + Send> = Box::new(String::from("formatted"));
        assert_eq!(
            PanicMessage::of(literal.as_ref()),
            PanicMessage::Text("literal".into())
        );
        assert_eq!(
            PanicMessage::of(formatted.as_ref()),
            PanicMessage::Text("formatted".into())
        );
    }
}
