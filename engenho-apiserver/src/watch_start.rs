//! Whether a WATCH can start at the revision its client named (T3.9a).
//!
//! A watch opens FROM a revision: "I have seen everything up to N, send me
//! what comes after." The store can honour that only while N lies inside the
//! history it holds, at or above its compaction floor and at or below its
//! current revision. Outside that window the watch is REFUSED, and the
//! refusal is a value ([`WatchRefusal`]), not an [`ApiError`](crate::ApiError):
//!
//!   * **ahead of the store** (N > current). Until T3.9a the store attached
//!     such a watch at its current revision without a word, so a client
//!     whose resourceVersion came from a history the store no longer holds
//!     (a restore, a replay that renumbered revisions) kept a cache that was
//!     silently stale.
//!   * **compacted** (N below the floor), as the store reports it.
//!
//! Registration fails for nothing else. A replay too large for the watcher's
//! buffer is reported on the stream, after what fitted has been delivered, and
//! [`crate::watch_end`] decides whether that watch ends or resumes. It is
//! never a refusal and never a 410. A resume that the store refuses (history
//! compacted while the watch filtered its way forward) ends with this module's
//! 410, as a client's own re-watch from there would.
//!
//! ## In-band, never an HTTP status
//!
//! A refusal is rendered as HTTP 200 carrying one `ERROR` watch line whose
//! object is `Status{code: 410, reason: "Expired"}`, followed by a clean
//! close. That is the shape kube-apiserver's watch cache uses for a too-old
//! revision. It is deliberately NOT an HTTP 410, because of how kube-rs, the
//! client pangea-operator uses, reacts to each. From kube-runtime `watcher.rs`
//! (0.99, and still in 4.2.0):
//!
//!   * any watch-START error, whatever its HTTP status, leaves the watcher in
//!     `InitListed { resource_version }`, and it re-issues the SAME watch.
//!     Against a revision the store will never serve, it does that forever;
//!   * only an in-band `WatchEvent::Error` with `code == 410` resets the
//!     watcher to relist.
//!
//! client-go reads the same in-band 410 as an expired resourceVersion and
//! relists too, so one shape serves both clients.
//!
//! ## Ahead of the store: a deliberate deviation
//!
//! kube-apiserver v1.34 does not refuse a watch whose revision is ahead of
//! its cache. A plain watch is accepted at once and sends nothing until the
//! cache passes the revision; only a `sendInitialEvents` watch waits (3 s)
//! and then ends with an in-band `504 Timeout` (cause
//! `ResourceVersionTooLarge`). engenho refuses both at once with the 410
//! above (T3.9a): it reads the store itself rather than a cache fed from it,
//! so a revision it has not reached is more likely one from a history it no
//! longer holds (a restore, a replay that renumbered revisions) than one it
//! is about to reach, and accepting it would leave that client's cache
//! silently stale. The cost falls on a replica that is merely behind: its
//! client relists where upstream's would have waited. The oracle rows that
//! disagree are declared deviations in `tests/oracle_watch_410.rs`. A LIST
//! ahead of the store, by contrast, waits and answers upstream's 504
//! ([`crate::list_floor`]).
//!
//! [`WatchRefusal`] has no `IntoResponse` and no conversion into
//! [`ApiError`](crate::ApiError), so the router cannot render one as an HTTP status by
//! accident, and [`WatchStart`] makes the router handle the refusal before
//! it can reach a stream.

use bytes::Bytes;
use engenho_store::{Revision, WatchGone, WatchStream};

use crate::error::status_object;
use crate::params::{ResumePoint, TooLargeResourceVersion, error_line};
use crate::watch_end::Compacted;

/// What opening a watch produced.
///
/// Returned by [`crate::ResourceHandler::watch_stream`]. The `Result` around
/// it carries an [`ApiError`](crate::ApiError) only when the handler could not open a watch at
/// all. A resume point the store cannot serve is [`Self::Refused`], which the
/// router ends in-band.
pub enum WatchStart {
    /// The store holds every revision after the resume point.
    Streaming(WatchStream),
    /// The store cannot serve from the resume point. The watch ends with
    /// [`WatchRefusal::status_line`] and nothing else.
    Refused(WatchRefusal),
}

/// Why a watch cannot start at the revision its client named.
///
/// The reasons are a private enum, so there are only two ways to hold one:
/// [`Self::ahead_of`], which returns `None` for any revision the store has
/// reached, and `From<Compacted>`, which carries the store's own verdict.
/// Code outside this module cannot build a refusal for a revision the store
/// can serve. That seal is a constructor, not a type: a handler that never
/// calls [`Self::ahead_of`] still compiles, and the tests over
/// [`crate::StoreBackedHandler`] are what catch it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(transparent)]
pub struct WatchRefusal(Refusal);

/// The reasons behind a [`WatchRefusal`]. Each message is the text
/// kube-apiserver uses for the same condition, so a client or an operator
/// matching on it reads engenho the same way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
enum Refusal {
    /// The client named a revision the store has not reached. The same fact
    /// a LIST reports as a 504 ([`crate::list_floor`]).
    #[error(transparent)]
    AheadOfStore(TooLargeResourceVersion),
    /// The client named a revision below the compaction floor.
    #[error(transparent)]
    Compacted(Compacted),
}

impl WatchRefusal {
    /// The refusal for a resume point the store has not reached, or `None`
    /// when the store can serve it ([`ResumePoint::ahead_of`]).
    ///
    /// `ResumePoint::MostRecent` is never ahead: it means "from wherever the
    /// store is now". An explicit `At(current)` is servable too: it starts
    /// at the store's revision and replays nothing.
    #[must_use]
    pub fn ahead_of(from: ResumePoint, current: Revision) -> Option<Self> {
        from.ahead_of(current)
            .map(|too_large| Self(Refusal::AheadOfStore(too_large)))
    }

    /// The one line a refused watch sends before it closes: an `ERROR` event
    /// whose object is `Status{code: 410, reason: "Expired"}`.
    #[must_use]
    pub fn status_line(&self) -> Bytes {
        error_line(&status_object(self.to_string(), 410, "Expired"))
    }
}

impl WatchRefusal {
    /// The refusal for the store's own verdict at registration, or the
    /// verdict back when it is not a refusal.
    ///
    /// Two verdicts refuse a watch: history below the compaction floor, and a
    /// resume point past the store's revision. The store judges the second
    /// under the lock its registration holds, so a rewind (a restore, a
    /// snapshot install) between the handler's own read and the attachment is
    /// still refused rather than served. The ahead case goes through
    /// [`Self::ahead_of`], the one comparison, so the store and the handler
    /// cannot disagree about what "ahead" means. An overflow is not a refusal
    /// and comes back unchanged.
    ///
    /// # Errors
    ///
    /// The verdict itself, when it is not a refusal.
    pub fn from_watch_gone(gone: WatchGone) -> Result<Self, WatchGone> {
        match gone {
            WatchGone::CompactedTooOld { .. } => Compacted::try_from(gone).map(Self::from),
            WatchGone::AheadOfStore { requested, current } => {
                Self::ahead_of(ResumePoint::At(requested), current).ok_or(gone)
            }
            WatchGone::Overflow { .. } => Err(gone),
        }
    }
}

impl From<Compacted> for WatchRefusal {
    fn from(compacted: Compacted) -> Self {
        Self(Refusal::Compacted(compacted))
    }
}

#[cfg(test)]
mod tests {
    use engenho_store::WatchGone;

    use super::*;

    fn line_json(refusal: &WatchRefusal) -> serde_json::Value {
        let bytes = refusal.status_line();
        assert_eq!(bytes.last(), Some(&b'\n'), "one NDJSON line");
        serde_json::from_slice(&bytes[..bytes.len() - 1]).unwrap()
    }

    /// The store's own ahead-of verdict, judged under its registration lock,
    /// ends the watch exactly as the handler's pre-read does: an in-band 410
    /// with upstream's text.
    #[test]
    fn the_stores_ahead_verdict_is_the_same_in_band_410() {
        let gone = WatchGone::AheadOfStore {
            requested: Revision(9999),
            current: Revision(8),
        };
        let refusal = WatchRefusal::from_watch_gone(gone).expect("ahead of the store is a refusal");
        let line = line_json(&refusal);
        assert_eq!(line["type"], "ERROR");
        assert_eq!(line["object"]["code"], 410);
        assert_eq!(line["object"]["reason"], "Expired");
        assert_eq!(
            line["object"]["message"],
            "Too large resource version: 9999, current: 8"
        );
    }

    /// An overflow is not a refusal: it comes back for `watch_end` to decide.
    #[test]
    fn an_overflow_is_not_a_refusal() {
        let gone = WatchGone::Overflow {
            capacity: 2,
            last_seen: Revision(5),
        };
        assert_eq!(WatchRefusal::from_watch_gone(gone.clone()), Err(gone));
    }

    #[test]
    fn a_resume_point_past_the_store_is_refused() {
        let refusal = WatchRefusal::ahead_of(ResumePoint::At(Revision(9)), Revision(8));
        assert!(refusal.is_some(), "one past the store is ahead");
    }

    #[test]
    fn a_resume_point_the_store_has_reached_is_servable() {
        assert_eq!(
            WatchRefusal::ahead_of(ResumePoint::At(Revision(8)), Revision(8)),
            None,
            "At(current) starts at the store's revision and replays nothing"
        );
        assert_eq!(
            WatchRefusal::ahead_of(ResumePoint::At(Revision(3)), Revision(8)),
            None
        );
        assert_eq!(
            WatchRefusal::ahead_of(ResumePoint::MostRecent, Revision(0)),
            None,
            "MostRecent means wherever the store is now"
        );
    }

    /// kube-rs decodes an `ERROR` line into `ErrorResponse{status, message,
    /// reason, code}` and relists only on `code == 410`; client-go reads
    /// reason `Expired` as an expired resourceVersion.
    #[test]
    fn an_ahead_refusal_is_an_in_band_410_expired_naming_both_revisions() {
        let refusal =
            WatchRefusal::ahead_of(ResumePoint::At(Revision(9_999)), Revision(8)).unwrap();
        let line = line_json(&refusal);
        assert_eq!(line["type"], "ERROR");
        let status = &line["object"];
        assert_eq!(status["kind"], "Status");
        assert_eq!(status["status"], "Failure");
        assert_eq!(status["code"], 410);
        assert_eq!(status["reason"], "Expired");
        assert_eq!(
            status["message"],
            "Too large resource version: 9999, current: 8"
        );
    }

    #[test]
    fn a_compacted_registration_is_an_in_band_410_expired() {
        let compacted = Compacted::try_from(WatchGone::CompactedTooOld {
            requested: Revision(2),
            compacted: Revision(5),
        })
        .unwrap();
        let refusal = WatchRefusal::from(compacted);
        let line = line_json(&refusal);
        assert_eq!(line["type"], "ERROR");
        assert_eq!(line["object"]["code"], 410);
        assert_eq!(line["object"]["reason"], "Expired");
        assert_eq!(line["object"]["message"], "too old resource version: 2 (5)");
        assert_eq!(
            refusal.status_line(),
            crate::params::status_410_line(compacted),
            "a compaction reads the same refused at registration or met mid-stream"
        );
    }
}
