//! A LIST that names a `resourceVersion` is served from state at least that
//! fresh, or not at all.
//!
//! `?resourceVersion=N` on a LIST means "not older than N" (and `Exact`
//! means "at N"). Until this module the router ignored it and answered from
//! whatever revision the store stood at, so a LIST at an `N` the store had
//! not reached returned OLDER data than the client asked for, with a
//! `resourceVersion` below the one it named. client-go relists at the last
//! revision it saw after a watch ends, so that is the request a client whose
//! watch was refused makes next.
//!
//! kube-apiserver v1.34 waits for its cache to reach `N` (`blockTimeout`, 3
//! s) and then answers `504 Timeout` with the cause `ResourceVersionTooLarge`
//! (`storage.NewTooLargeResourceVersionError`,
//! `watchCache.WaitUntilFreshAndGetList`). client-go's reflector recognises
//! that cause and relists with no `resourceVersion`; kube-rs lists with none
//! by default. [`await_revision`] does the same: it waits up to
//! [`LIST_WAIT_FOR_REVISION`] for the store to reach `N`, and otherwise
//! returns the [`TooLargeResourceVersion`] the router renders as that 504.
//!
//! ## What is sealed, and how
//!
//! The "not ahead" judgement is [`ResumePoint::ahead_of`], the comparison a
//! WATCH refusal makes too, and [`TooLargeResourceVersion`] exists only as
//! its answer. The wait is a poll of [`ResourceHandler::current_revision`]
//! (a scalar read), not a notification: it adds at most
//! [`MAX_POLL_PAUSE`] to a LIST that catches up, and costs nothing to a LIST
//! at a revision the store has reached. That the router calls it before
//! reading is a test (`tests/oracle_watch_410.rs`,
//! `future_rv.list_timeout_is_also_too_large`), not a type.

use std::time::Duration;

use engenho_store::Revision;

use crate::handler::ResourceHandler;
use crate::params::{ResumePoint, TooLargeResourceVersion};

/// How long a LIST waits for the store to reach the `resourceVersion` it
/// named: kube-apiserver's `blockTimeout`.
pub const LIST_WAIT_FOR_REVISION: Duration = Duration::from_secs(3);

/// `details.retryAfterSeconds` on the 504: kube-apiserver's
/// `resourceVersionTooHighRetrySeconds`.
pub const TOO_LARGE_RETRY_AFTER_SECONDS: u32 = 1;

/// The first pause between two reads of the store's revision; each pause
/// doubles up to [`MAX_POLL_PAUSE`].
const FIRST_POLL_PAUSE: Duration = Duration::from_millis(5);

/// The longest pause between two reads of the store's revision.
pub const MAX_POLL_PAUSE: Duration = Duration::from_millis(50);

/// Wait until the store behind `handler` has reached `floor`, for at most
/// `within`.
///
/// # Errors
///
/// [`TooLargeResourceVersion`] naming where the store stood when `within`
/// ran out.
pub async fn await_revision(
    handler: &dyn ResourceHandler,
    floor: Revision,
    within: Duration,
) -> Result<(), TooLargeResourceVersion> {
    let deadline = tokio::time::Instant::now() + within;
    let mut pause = FIRST_POLL_PAUSE;
    loop {
        let current = handler.current_revision().await;
        let Some(too_large) = ResumePoint::At(floor).ahead_of(current) else {
            return Ok(());
        };
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            return Err(too_large);
        }
        tokio::time::sleep(pause.min(left)).await;
        pause = (pause * 2).min(MAX_POLL_PAUSE);
    }
}
