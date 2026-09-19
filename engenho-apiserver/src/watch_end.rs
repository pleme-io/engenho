//! How a streaming WATCH ends, or carries on, when the store stops serving
//! it (T3.7).
//!
//! Once a watch is streaming, the response is HTTP 200 and every end is said
//! in-band. The store stops a stream for one of two reasons ([`WatchGone`]),
//! and [`WatchProgress::after`] turns each into exactly one [`AfterGone`]:
//!
//!   * **compacted.** History the watcher still needed is gone. The watch
//!     ends with `ERROR Status{410, Expired}` ([`Compacted`]) and the client
//!     relists.
//!   * **overflowed after the client progressed.** The per-watcher buffer
//!     filled, but the client can resume past the revision the watch started
//!     from. The watch ends with a `BOOKMARK` at the store's `last_seen`
//!     (when the client asked for bookmarks) and a clean close, which is what
//!     kube-apiserver's cacher does with a watcher that stopped keeping up.
//!     The client re-watches from its newest resourceVersion and relists
//!     nothing ([`Progressed`]). Without bookmarks that is the last event it
//!     was sent; if the filtered history after it overflows the next watch,
//!     that watch resumes as below.
//!   * **overflowed before the client progressed, with the store past where
//!     the stream opened.** Every revision up to `last_seen` reached the
//!     router and was filtered out, and the client did not ask for
//!     bookmarks, so nothing on the wire can move it past them. Ending the
//!     watch cannot help, whatever the last line says. The store replays
//!     every kind from the resume point into the one buffer, so which change
//!     overflows it is decided by history alone: a client that re-watches
//!     from where it stands meets the same filtered prefix and overflows at
//!     the same change, after any backoff. A 429 here would be a retry loop
//!     that only a compaction ends. So the router resumes the store watch
//!     itself at `last_seen` ([`Resume`]), and the client sees nothing. The
//!     store's overflow contract makes that resume gap-free and
//!     duplicate-free: it delivers every signal up to `last_seen` before it
//!     reports the overflow.
//!   * **overflowed with nothing past where the stream opened.** The buffer
//!     filled without one signal newer than the stream's opening revision.
//!     History did not decide that overflow, timing did, so a retry can go
//!     differently. The watch ends with
//!     `ERROR Status{429, TooManyRequests, details.retryAfterSeconds}`
//!     ([`NoProgress`]).
//!
//! An overflow is never a 410. Before T3.7 it was, and a 410 sends the client
//! to relist, the most expensive recovery there is, for a condition a resume
//! answers.
//!
//! ## Why a chain of resumes ends
//!
//! A [`Resume`] exists only for a `last_seen` past the revision the current
//! store stream opened at, and [`WatchProgress::resumed`] moves that revision
//! up to it. Each resume therefore opens strictly past the one before. The
//! chain stops when the router catches up with the store, or when the next
//! opening revision has been compacted away: the store refuses that open, and
//! the watch ends with a 410 ([`Compacted`]). What is not bounded is how many
//! resumes a watch takes while writes never let up. Each one costs a replay
//! read under the store's lock, as a client's own re-watch would, and the
//! router yields its worker between them.
//!
//! ## How reachable the 429 is
//!
//! With the apiserver's 1024-slot buffer, and bookmarks sent only to clients
//! that asked for them, the 429 cannot happen. Every event the store enqueues
//! is newer than the stream's opening revision. For a client that asked for
//! bookmarks, at most one bookmark (at that revision) is not, so a buffer of
//! two or more that fills always holds something newer. It takes a one-slot
//! buffer and a client that asked for bookmarks. The 429 stays as the honest
//! end of that branch, not a path a client is expected to meet. How the
//! clients handle it (client-go's reflector backs off on a watch 429 and
//! re-watches; kube-rs's watcher surfaces it as an error and re-watches when
//! the stream closes) is read from their source and not yet pinned by a
//! ported client table (T0.7).
//!
//! ## What is sealed, and how
//!
//! * [`crate::params::status_410_line`] takes a [`Compacted`], and a
//!   `Compacted` can be built only from `WatchGone::CompactedTooOld`. A 410
//!   built from an overflow's `last_seen` does not compile:
//!
//!   ```compile_fail,E0308
//!   let last_seen = engenho_store::Revision(7);
//!   let _ = engenho_apiserver::status_410_line(last_seen);
//!   ```
//!
//! * The in-band 429 is a method of [`NoProgress`], and the router re-opens
//!   the store watch only at a [`Resume`]. Both have private fields, so each
//!   exists only as what [`WatchProgress::after`] returned:
//!
//!   ```compile_fail,E0451
//!   let _ = engenho_apiserver::NoProgress { start: engenho_store::Revision(7) };
//!   ```
//!
//!   ```compile_fail,E0451
//!   let _ = engenho_apiserver::Resume { at: engenho_store::Revision(7) };
//!   ```
//!
//! These are constructor seals, not types. What stays a test is the router's
//! side of the bargain: that it records every line it forwards with
//! [`WatchProgress::delivered`] and none it filters out, and that it re-opens
//! the store watch at [`Resume::at`] and then records it with
//! [`WatchProgress::resumed`].

use bytes::Bytes;
use engenho_store::{Revision, WatchGone};

use crate::error::too_many_requests_object;
use crate::params::{WatchGvk, bookmark_line, error_line, status_410_line};

/// `retryAfterSeconds` on a no-progress watch end. One second is what
/// kube-apiserver's max-in-flight filter puts in `Retry-After` when it
/// sheds a request. Neither client waits exactly this long (each uses its
/// own backoff); it says "come back soon", not "come back never".
pub const NO_PROGRESS_RETRY_AFTER_SECONDS: u32 = 1;

/// What a streaming watch has told its client so far, and where the store
/// stream behind it opened.
///
/// The router builds one when the stream opens, records each line it
/// forwards, and asks it what to do when the store stops serving the watch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchProgress {
    /// The revision the client has seen everything up to when the watch
    /// opened: where it would resume if the watch closed right away.
    start: Revision,
    /// The newest revision the client has been sent, by an event or a
    /// bookmark. Starts at `start`.
    delivered: Revision,
    /// The revision the current store stream opened at. Starts at `start`;
    /// each [`Resume`] moves it up to the overflow's `last_seen`.
    opened_at: Revision,
    /// Whether the client asked for bookmarks (`allowWatchBookmarks`).
    bookmarks: bool,
}

impl WatchProgress {
    /// A watch that opens at `start` for a client that did (`bookmarks`) or
    /// did not ask for bookmarks.
    #[must_use]
    pub fn new(start: Revision, bookmarks: bool) -> Self {
        Self {
            start,
            delivered: start,
            opened_at: start,
            bookmarks,
        }
    }

    /// Whether the client asked for bookmarks: the router forwards the
    /// store's periodic bookmarks only then, and ends a progressed overflow
    /// with one only then.
    #[must_use]
    pub fn bookmarks(&self) -> bool {
        self.bookmarks
    }

    /// Record that a line carrying `rev` (an event or a bookmark) went to the
    /// client. A line the router filtered out is not recorded: the client
    /// never saw its revision and cannot resume from it.
    pub fn delivered(&mut self, rev: Revision) {
        if rev > self.delivered {
            self.delivered = rev;
        }
    }

    /// What the router does now that the store stopped serving this watch.
    ///
    /// The store delivers every buffered signal before it reports an
    /// overflow, so by the time the router asks, it has processed everything
    /// up to the overflow's `last_seen`: forwarded or filtered, nothing is
    /// pending. That is what makes a bookmark at `last_seen`, and a resume
    /// from it, gap-free.
    #[must_use]
    pub fn after(&self, gone: &WatchGone) -> AfterGone {
        match *gone {
            WatchGone::CompactedTooOld {
                requested,
                compacted,
            } => AfterGone::End(WatchEnd::Compacted(Compacted {
                requested,
                compacted,
            })),
            WatchGone::Overflow { last_seen, .. } => {
                // Where the client resumes once the watch closes: at
                // `last_seen` when a bookmark tells it so, otherwise at the
                // newest revision it was sent.
                let resume = if self.bookmarks {
                    last_seen.max(self.delivered)
                } else {
                    self.delivered
                };
                if resume > self.start {
                    AfterGone::End(WatchEnd::Progressed(Progressed {
                        resume,
                        bookmark: self.bookmarks,
                    }))
                } else if last_seen > self.opened_at {
                    AfterGone::Resume(Resume { at: last_seen })
                } else {
                    AfterGone::End(WatchEnd::NoProgress(NoProgress { start: self.start }))
                }
            }
        }
    }

    /// Record that the router re-opened the store watch at `resume`. The
    /// next resume has to open past it.
    pub fn resumed(&mut self, resume: &Resume) {
        if resume.at > self.opened_at {
            self.opened_at = resume.at;
        }
    }
}

/// What the router does when the store stops serving a streaming watch, as
/// [`WatchProgress::after`] decides it.
#[derive(Debug, PartialEq, Eq)]
pub enum AfterGone {
    /// Re-open the store watch at [`Resume::at`] and keep streaming on the
    /// same response. The client sees nothing.
    Resume(Resume),
    /// The watch ends with [`WatchEnd::final_line`], if it has one, and the
    /// body closes.
    End(WatchEnd),
}

/// An overflow the router answers by re-opening the store watch past the
/// history it has already filtered, instead of ending the watch. Built only
/// by [`WatchProgress::after`], and only for a `last_seen` past the revision
/// the current store stream opened at, and handed back through
/// [`WatchProgress::resumed`] once the store watch is open there.
#[derive(Debug, PartialEq, Eq)]
pub struct Resume {
    at: Revision,
}

impl Resume {
    /// Where the store watch re-opens: the overflow's `last_seen`, the
    /// newest revision the router has processed.
    #[must_use]
    pub fn at(&self) -> Revision {
        self.at
    }
}

/// How a streaming watch ends, as [`WatchProgress::after`] decides it. The
/// payloads have private fields, so an end cannot claim a progress or a
/// compaction that did not happen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchEnd {
    /// History the watcher needed was compacted: `ERROR 410 Expired`, and the
    /// client relists.
    Compacted(Compacted),
    /// Overflowed, and the client resumes past its start: a `BOOKMARK` at the
    /// resume point when bookmarks were requested, then a clean close.
    Progressed(Progressed),
    /// Overflowed with nothing newer than the stream's opening revision in
    /// the buffer: `ERROR 429 TooManyRequests` with `retryAfterSeconds`.
    NoProgress(NoProgress),
}

impl WatchEnd {
    /// The last line of the watch, or `None` when it ends with a clean close
    /// and nothing else (a progressed overflow for a client that did not ask
    /// for bookmarks: it resumes from the last event it was sent).
    ///
    /// `gvk` stamps the bookmark's object, which a kube-rs client refuses to
    /// decode without `apiVersion` and `kind`.
    #[must_use]
    pub fn final_line(&self, gvk: WatchGvk<'_>) -> Option<Bytes> {
        match self {
            Self::Compacted(compacted) => Some(status_410_line(*compacted)),
            Self::Progressed(progressed) => progressed
                .bookmark
                .then(|| bookmark_line(progressed.resume, gvk, false)),
            Self::NoProgress(no_progress) => Some(no_progress.status_line()),
        }
    }
}

/// A watch the store could not serve because history it needed was
/// compacted. Built only from `WatchGone::CompactedTooOld`, so its revision
/// is a compaction floor and never an overflow's `last_seen`.
///
/// The message is kube-apiserver's text for the same condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("too old resource version: {requested} ({compacted})")]
pub struct Compacted {
    requested: Revision,
    compacted: Revision,
}

impl Compacted {
    /// The revision the watch wanted to resume after.
    #[must_use]
    pub fn requested(&self) -> Revision {
        self.requested
    }

    /// The store's compaction floor: the lowest revision it still holds.
    #[must_use]
    pub fn compacted(&self) -> Revision {
        self.compacted
    }
}

impl TryFrom<WatchGone> for Compacted {
    /// Anything but a compaction comes back unchanged.
    type Error = WatchGone;

    fn try_from(gone: WatchGone) -> Result<Self, Self::Error> {
        match gone {
            WatchGone::CompactedTooOld {
                requested,
                compacted,
            } => Ok(Self {
                requested,
                compacted,
            }),
            other @ WatchGone::Overflow { .. } => Err(other),
        }
    }
}

/// An overflow after which the client resumes past its start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Progressed {
    resume: Revision,
    bookmark: bool,
}

impl Progressed {
    /// Where the client resumes: the bookmark's revision when one is sent,
    /// otherwise the newest revision it was sent.
    #[must_use]
    pub fn resume(&self) -> Revision {
        self.resume
    }

    /// Whether the watch ends with a bookmark at [`Self::resume`].
    #[must_use]
    pub fn bookmark(&self) -> bool {
        self.bookmark
    }
}

/// An overflow that left nothing to move past: the client was sent nothing
/// newer than its start, and the buffer held nothing newer than the store
/// stream's opening revision. Exists only inside the `WatchEnd::NoProgress`
/// that [`WatchProgress::after`] returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "watch fell behind before anything after resource version {start} was delivered; \
     retry the watch from {start}"
)]
pub struct NoProgress {
    start: Revision,
}

impl NoProgress {
    /// The revision the watch started from, which is where the client
    /// retries.
    #[must_use]
    pub fn start(&self) -> Revision {
        self.start
    }

    /// The one line this watch ends with: an `ERROR` event whose object is
    /// `Status{code: 429, reason: "TooManyRequests",
    /// details.retryAfterSeconds}`.
    #[must_use]
    pub fn status_line(&self) -> Bytes {
        error_line(&too_many_requests_object(
            self.to_string(),
            NO_PROGRESS_RETRY_AFTER_SECONDS,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GVK: WatchGvk<'static> = WatchGvk {
        api_version: "v1",
        kind: "ConfigMap",
    };

    fn overflow(last_seen: u64) -> WatchGone {
        WatchGone::Overflow {
            capacity: 4,
            last_seen: Revision(last_seen),
        }
    }

    fn json(line: &Bytes) -> serde_json::Value {
        assert_eq!(line.last(), Some(&b'\n'), "one NDJSON line");
        serde_json::from_slice(&line[..line.len() - 1]).unwrap()
    }

    /// The end `after` decided, where the test expects the watch to end.
    fn end(after: AfterGone) -> WatchEnd {
        match after {
            AfterGone::End(end) => end,
            AfterGone::Resume(resume) => panic!("expected an end, got {resume:?}"),
        }
    }

    /// The resume `after` decided, where the test expects the watch to go on.
    fn resume(after: AfterGone) -> Resume {
        match after {
            AfterGone::Resume(resume) => resume,
            AfterGone::End(end) => panic!("expected a resume, got {end:?}"),
        }
    }

    #[test]
    fn an_overflow_after_delivered_events_is_progress() {
        let mut progress = WatchProgress::new(Revision(10), false);
        progress.delivered(Revision(12));
        match end(progress.after(&overflow(15))) {
            WatchEnd::Progressed(p) => {
                assert_eq!(
                    p.resume(),
                    Revision(12),
                    "no bookmark: resume at the last event"
                );
                assert!(!p.bookmark());
            }
            other => panic!("expected Progressed, got {other:?}"),
        }
    }

    /// The store's `last_seen` covers events the router filtered out. With
    /// bookmarks, a bookmark there moves the client past them, so the
    /// watch progressed even though no event reached the client.
    #[test]
    fn with_bookmarks_the_stores_last_seen_is_progress() {
        let progress = WatchProgress::new(Revision(10), true);
        match end(progress.after(&overflow(15))) {
            WatchEnd::Progressed(p) => {
                assert_eq!(p.resume(), Revision(15));
                assert!(p.bookmark());
            }
            other => panic!("expected Progressed, got {other:?}"),
        }
    }

    /// Without bookmarks nothing can carry the client past the filtered
    /// revisions, and a re-watch from 10 would replay them into the same
    /// overflow. The router resumes the store watch at `last_seen` instead.
    #[test]
    fn without_bookmarks_filtered_history_is_resumed_at_last_seen() {
        let progress = WatchProgress::new(Revision(10), false);
        assert_eq!(resume(progress.after(&overflow(15))).at(), Revision(15));
    }

    /// Each resume opens strictly past the one before, which is what ends a
    /// chain of them.
    #[test]
    fn a_resume_moves_the_opening_revision_so_the_next_one_is_further_on() {
        let mut progress = WatchProgress::new(Revision(10), false);
        let first = resume(progress.after(&overflow(15)));
        progress.resumed(&first);
        assert!(
            matches!(
                end(progress.after(&overflow(15))),
                WatchEnd::NoProgress(np) if np.start() == Revision(10)
            ),
            "an overflow with nothing past 15 is not a second resume at 15"
        );
        assert_eq!(resume(progress.after(&overflow(20))).at(), Revision(20));
    }

    /// Over a grid of overflows: a resume happens only for a client that can
    /// be told nothing (no bookmarks, nothing delivered), and only strictly
    /// past the opening revision; a 429 only when nothing passed it.
    #[test]
    fn a_resume_is_only_ever_past_the_opening_revision() {
        for bookmarks in [false, true] {
            for delivered in [10, 12] {
                for last_seen in 5..=20 {
                    let mut progress = WatchProgress::new(Revision(10), bookmarks);
                    progress.delivered(Revision(delivered));
                    match progress.after(&overflow(last_seen)) {
                        AfterGone::Resume(r) => {
                            assert!(!bookmarks && delivered == 10, "{r:?}");
                            assert!(last_seen > 10, "{r:?}");
                            assert_eq!(r.at(), Revision(last_seen));
                        }
                        AfterGone::End(WatchEnd::NoProgress(np)) => {
                            assert!(delivered == 10 && last_seen <= 10, "{np:?}");
                        }
                        AfterGone::End(WatchEnd::Progressed(p)) => {
                            assert!(p.resume() > Revision(10), "{p:?}");
                        }
                        AfterGone::End(WatchEnd::Compacted(c)) => {
                            panic!("an overflow is never a compaction: {c:?}")
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn an_overflow_at_the_start_revision_is_no_progress_even_with_bookmarks() {
        let mut progress = WatchProgress::new(Revision(10), true);
        progress.delivered(Revision(10)); // a bookmark at the start moves nothing
        assert!(matches!(
            end(progress.after(&overflow(10))),
            WatchEnd::NoProgress(_)
        ));
    }

    #[test]
    fn a_progressed_end_with_bookmarks_is_a_bookmark_at_last_seen() {
        let end = end(WatchProgress::new(Revision(10), true).after(&overflow(15)));
        let line = json(&end.final_line(GVK).expect("a bookmark line"));
        assert_eq!(line["type"], "BOOKMARK");
        assert_eq!(line["object"]["metadata"]["resourceVersion"], "15");
        assert_eq!(line["object"]["kind"], "ConfigMap");
        assert_eq!(line["object"]["apiVersion"], "v1");
        assert!(
            line["object"]["metadata"].get("annotations").is_none(),
            "not an initial-events-end bookmark: {line}"
        );
    }

    #[test]
    fn a_progressed_end_without_bookmarks_is_a_clean_close() {
        let mut progress = WatchProgress::new(Revision(10), false);
        progress.delivered(Revision(11));
        assert_eq!(end(progress.after(&overflow(15))).final_line(GVK), None);
    }

    /// client-go's `apierrors.IsTooManyRequests` keys on code 429 / reason
    /// `TooManyRequests`, and `SuggestsClientDelay` on
    /// `details.retryAfterSeconds`. kube-rs decodes the same object into
    /// `ErrorResponse`, which needs `status`, `message`, `reason`, `code`.
    #[test]
    fn a_no_progress_end_is_an_in_band_429_with_retry_after() {
        let end = end(WatchProgress::new(Revision(10), false).after(&overflow(10)));
        let line = json(&end.final_line(GVK).expect("a status line"));
        assert_eq!(line["type"], "ERROR");
        let status = &line["object"];
        assert_eq!(status["kind"], "Status");
        assert_eq!(status["apiVersion"], "v1");
        assert_eq!(status["status"], "Failure");
        assert_eq!(status["code"], 429);
        assert_eq!(status["reason"], "TooManyRequests");
        assert_eq!(
            status["details"]["retryAfterSeconds"],
            NO_PROGRESS_RETRY_AFTER_SECONDS
        );
        assert_eq!(
            status["message"],
            "watch fell behind before anything after resource version 10 was delivered; \
             retry the watch from 10"
        );
    }

    #[test]
    fn a_compacted_end_is_an_in_band_410_naming_the_compaction_floor() {
        let end = end(
            WatchProgress::new(Revision(3), true).after(&WatchGone::CompactedTooOld {
                requested: Revision(3),
                compacted: Revision(9),
            }),
        );
        let line = json(&end.final_line(GVK).expect("a status line"));
        assert_eq!(line["type"], "ERROR");
        assert_eq!(line["object"]["code"], 410);
        assert_eq!(line["object"]["reason"], "Expired");
        assert_eq!(line["object"]["message"], "too old resource version: 3 (9)");
        assert!(line["object"].get("details").is_none(), "{line}");
    }

    /// No overflow ends with a 410, with or without progress.
    #[test]
    fn no_overflow_end_is_a_410() {
        for bookmarks in [false, true] {
            for (delivered, last_seen) in [(10, 10), (10, 15), (12, 15)] {
                let mut progress = WatchProgress::new(Revision(10), bookmarks);
                progress.delivered(Revision(delivered));
                let AfterGone::End(end) = progress.after(&overflow(last_seen)) else {
                    continue; // a resume sends no line at all
                };
                assert!(!matches!(end, WatchEnd::Compacted(_)), "{end:?}");
                if let Some(line) = end.final_line(GVK) {
                    assert_ne!(json(&line)["object"]["code"], 410, "{end:?}");
                }
            }
        }
    }

    #[test]
    fn only_a_compaction_converts_to_compacted() {
        assert_eq!(
            Compacted::try_from(WatchGone::CompactedTooOld {
                requested: Revision(1),
                compacted: Revision(5),
            })
            .map(|c| (c.requested(), c.compacted())),
            Ok((Revision(1), Revision(5)))
        );
        assert_eq!(Compacted::try_from(overflow(7)), Err(overflow(7)));
    }
}
