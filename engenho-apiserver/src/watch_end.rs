//! How a streaming WATCH ends when the store stops serving it (T3.7).
//!
//! Once a watch is streaming, the response is HTTP 200 and every end is said
//! in-band. The store stops a stream for one of two reasons ([`WatchGone`]),
//! and [`WatchProgress::end`] turns each into exactly one [`WatchEnd`]:
//!
//!   * **compacted.** History the watcher still needed is gone. The watch
//!     ends with `ERROR Status{410, Expired}` ([`Compacted`]) and the client
//!     relists.
//!   * **overflowed after progress.** The per-watcher buffer filled, but the
//!     client can resume past the revision the watch started from. The watch
//!     ends with a `BOOKMARK` at the store's `last_seen` (when the client
//!     asked for bookmarks) and a clean close, which is what kube-apiserver's
//!     cacher does with a watcher that stopped keeping up. The client
//!     re-watches from its newest resourceVersion and relists nothing.
//!   * **overflowed with no progress.** Nothing past the start revision
//!     reached the client, so a re-watch from the same point would walk
//!     straight back into the same overflow. The watch ends with
//!     `ERROR Status{429, TooManyRequests, details.retryAfterSeconds}`
//!     ([`NoProgress`]). client-go's reflector backs off on a watch 429 and
//!     resumes from its resourceVersion; kube-rs's watcher surfaces it as an
//!     error (a backoff, where one is attached, applies) and resumes from its
//!     resourceVersion when the stream closes. Both are read from the
//!     clients' source, not yet pinned by a ported client table (T0.7); if
//!     either client fails its row, the plan's fallback is an HTTP 429 on
//!     that client's next watch.
//!
//! An overflow is never a 410. Before T3.7 it was, and a 410 sends the client
//! to relist, the most expensive recovery there is, for a condition a resume
//! answers.
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
//! * The in-band 429 is a method of [`NoProgress`], and a `NoProgress` exists
//!   only as the payload of the `WatchEnd::NoProgress` that
//!   [`WatchProgress::end`] returns when the client made no progress. Its
//!   fields are private, so it cannot be built by hand:
//!
//!   ```compile_fail,E0451
//!   let _ = engenho_apiserver::NoProgress { start: engenho_store::Revision(7) };
//!   ```
//!
//! Both are constructor seals. What stays a test is the router's side of the
//! bargain: that it records every line it forwards with
//! [`WatchProgress::delivered`] and none it filters out.

use bytes::Bytes;
use engenho_store::{Revision, WatchGone};

use crate::error::too_many_requests_object;
use crate::params::{WatchGvk, bookmark_line, error_line, status_410_line};

/// `retryAfterSeconds` on a no-progress watch end. One second is what
/// kube-apiserver's max-in-flight filter puts in `Retry-After` when it
/// sheds a request. Neither client waits exactly this long (each uses its
/// own backoff); it says "come back soon", not "come back never".
pub const NO_PROGRESS_RETRY_AFTER_SECONDS: u32 = 1;

/// What a streaming watch has told its client so far.
///
/// The router builds one when the stream opens, records each line it
/// forwards, and asks it how the watch ends when the store stops serving it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchProgress {
    /// The revision the client has seen everything up to when the watch
    /// opened: where it would resume if the watch closed right away.
    start: Revision,
    /// The newest revision the client has been sent, by an event or a
    /// bookmark. Starts at `start`.
    delivered: Revision,
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

    /// How this watch ends, given why the store stopped serving it.
    ///
    /// The store delivers every buffered signal before it reports an
    /// overflow, so by the time the router asks, it has processed everything
    /// up to the overflow's `last_seen`: forwarded or filtered, nothing is
    /// pending. That is what makes a bookmark at `last_seen` true.
    #[must_use]
    pub fn end(&self, gone: &WatchGone) -> WatchEnd {
        match *gone {
            WatchGone::CompactedTooOld {
                requested,
                compacted,
            } => WatchEnd::Compacted(Compacted {
                requested,
                compacted,
            }),
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
                    WatchEnd::Progressed(Progressed {
                        resume,
                        bookmark: self.bookmarks,
                    })
                } else {
                    WatchEnd::NoProgress(NoProgress { start: self.start })
                }
            }
        }
    }
}

/// How a streaming watch ends, as [`WatchProgress::end`] decides it. The
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
    /// Overflowed before anything past the start reached the client:
    /// `ERROR 429 TooManyRequests` with `retryAfterSeconds`.
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

/// An overflow before anything past the watch's start revision reached the
/// client. Exists only inside the `WatchEnd::NoProgress` that
/// [`WatchProgress::end`] returns.
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

    #[test]
    fn an_overflow_after_delivered_events_is_progress() {
        let mut progress = WatchProgress::new(Revision(10), false);
        progress.delivered(Revision(12));
        match progress.end(&overflow(15)) {
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
        match progress.end(&overflow(15)) {
            WatchEnd::Progressed(p) => {
                assert_eq!(p.resume(), Revision(15));
                assert!(p.bookmark());
            }
            other => panic!("expected Progressed, got {other:?}"),
        }
    }

    /// Without bookmarks the client resumes from the last event it was sent;
    /// revisions the store passed but the router filtered do not count.
    #[test]
    fn without_bookmarks_filtered_revisions_are_not_progress() {
        let progress = WatchProgress::new(Revision(10), false);
        match progress.end(&overflow(15)) {
            WatchEnd::NoProgress(np) => assert_eq!(np.start(), Revision(10)),
            other => panic!("expected NoProgress, got {other:?}"),
        }
    }

    #[test]
    fn an_overflow_at_the_start_revision_is_no_progress_even_with_bookmarks() {
        let mut progress = WatchProgress::new(Revision(10), true);
        progress.delivered(Revision(10)); // a bookmark at the start moves nothing
        assert!(matches!(
            progress.end(&overflow(10)),
            WatchEnd::NoProgress(_)
        ));
    }

    #[test]
    fn a_progressed_end_with_bookmarks_is_a_bookmark_at_last_seen() {
        let end = WatchProgress::new(Revision(10), true).end(&overflow(15));
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
        assert_eq!(progress.end(&overflow(15)).final_line(GVK), None);
    }

    /// client-go's `apierrors.IsTooManyRequests` keys on code 429 / reason
    /// `TooManyRequests`, and `SuggestsClientDelay` on
    /// `details.retryAfterSeconds`. kube-rs decodes the same object into
    /// `ErrorResponse`, which needs `status`, `message`, `reason`, `code`.
    #[test]
    fn a_no_progress_end_is_an_in_band_429_with_retry_after() {
        let end = WatchProgress::new(Revision(10), false).end(&overflow(10));
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
        let end = WatchProgress::new(Revision(3), true).end(&WatchGone::CompactedTooOld {
            requested: Revision(3),
            compacted: Revision(9),
        });
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
                let end = progress.end(&overflow(last_seen));
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
