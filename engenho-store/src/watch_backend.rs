//! Resumable, gap-free, never-silently-lossy watch backend.
//!
//! ## Destination
//!
//! A watch the apiserver can hand straight to kubectl `--watch` and
//! controller informers, with the same atomic list-then-watch
//! contract etcd gives Kubernetes:
//!
//!   * **Resumable** — a consumer resumes from any retained
//!     [`Revision`] and receives EVERY change strictly after it, in
//!     revision order ([`WatchOpts::from`]).
//!   * **Gap-free + dup-free at the replay→live boundary** —
//!     subscription registers UNDER THE SAME catalog lock that
//!     `ResourceCatalog::apply` + `push_history` run under, so
//!     "I captured replay up to revision R" and "I am attached to the
//!     live tail" are one atomic act. No change committed during
//!     subscription is missed or doubled.
//!   * **Never silently lossy** — the per-watcher buffer is the ONLY
//!     place loss is possible, and loss there is a typed terminal
//!     [`WatchGone::Overflow`] carrying a resume token, never a silent
//!     `broadcast::RecvError::Lagged`.
//!
//! ## Why this replaces the broadcast fan-out
//!
//! The legacy `tokio::sync::broadcast` path (one `Sender` per backend)
//! had two structural defects:
//!
//!   1. **Silent loss on Lagged** — a slow consumer's ring overran and
//!      `recv()` returned `Err(Lagged(n))`; for a kubectl `--watch` or
//!      a controller informer this is fatal — silently-dropped events
//!      leave the consumer's cache permanently inconsistent.
//!   2. **No resume + a gap/dup window** — `watch_subscribe()` only
//!      attached to the live tail; even a "snapshot then subscribe"
//!      caller raced concurrent applies (broadcast `send` happened
//!      AFTER the lock dropped).
//!
//! Both are fixed here: per-watcher bounded `mpsc` channels (isolated
//! backpressure → typed Gone, never a global silent drop) + the
//! under-the-lock atomic handoff (closes the race window entirely).
//!
//! ## Replay is fed UNDER THE LOCK, before the sender is reachable
//!
//! The replay→live ordering is made STRUCTURAL, not timing-dependent.
//! [`WatcherRegistry::register_captured`] enqueues the entire replay
//! (in revision order) into the per-watcher channel BEFORE the handle
//! is pushed onto the registry — so before `fan_change` can ever reach
//! it. Because both `register_captured` and `fan_change` run under the
//! SAME catalog lock, every live event (`revision > boundary`) is
//! necessarily enqueued AFTER the whole replay. The channel is FIFO,
//! so insertion order == delivery order == revision order. There is no
//! separate feeder task and no second producer racing the apply path.
//!
//! A replay larger than the per-watcher buffer (e.g. a watcher resuming
//! from far behind, where the retained ring up to 8192 entries doesn't
//! fit a 1024-slot buffer) overflows DURING the replay feed and arms a
//! typed [`WatchGone::Overflow`] carrying the highest replay revision
//! actually enqueued — the consumer then re-lists + resumes, exactly
//! the same contract a compacted resume point gets. Never a silent
//! drop, never a block on the lock.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::revision::{Change, CompactedTooOld, Revision};
use crate::state::ResourceCatalog;
use crate::watch::{WatchEvent, WatchEventKind};

/// Default per-watcher buffer — mirrors the legacy broadcast capacity
/// so the migration is behaviorally familiar.
pub const WATCH_CHANNEL_CAPACITY: usize = 1024;

/// Default bookmark cadence.
pub const DEFAULT_BOOKMARK_EVERY: Duration = Duration::from_secs(5);

/// Typed terminal reason a watch stream ended (replaces the silent
/// `broadcast::RecvError::Lagged`). A consumer that receives a
/// [`WatchGone`] re-establishes the watch from the carried resume
/// point — it never silently misses events.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum WatchGone {
    /// The requested resume revision has been compacted away (the 410
    /// Gone equivalent). The consumer must re-list + resume from the
    /// fresh list revision.
    #[error("requested revision {requested} compacted (lowest retained {compacted})")]
    CompactedTooOld {
        requested: Revision,
        compacted: Revision,
    },
    /// The watcher overflowed its per-watcher buffer. `last_seen` is
    /// the highest revision actually delivered — the consumer resumes
    /// gap-free via `watch_from(last_seen)` WITHOUT re-listing.
    #[error("watcher overflowed its buffer (capacity {capacity}); resume from last_seen {last_seen}")]
    Overflow {
        capacity: usize,
        last_seen: Revision,
    },
}

engenho_substrate::impl_error_kind! {
    WatchGone {
        { CompactedTooOld { .. } } => "compacted_too_old",
        { Overflow { .. } } => "overflow",
    }
}

impl From<CompactedTooOld> for WatchGone {
    fn from(e: CompactedTooOld) -> Self {
        WatchGone::CompactedTooOld {
            requested: e.requested,
            compacted: e.compacted,
        }
    }
}

/// One streamed item — typed, never a naked broadcast value.
#[derive(Clone, Debug, PartialEq)]
pub enum WatchSignal {
    /// An Added / Modified / Deleted change. `resource_version` on the
    /// inner [`WatchEvent`] equals the [`Change::revision`].
    Event(WatchEvent),
    /// A periodic progress marker — a safe resume revision even when no
    /// events have flowed. Lets a quiescent consumer checkpoint so a
    /// later `watch_from(bookmarked_rev)` replays nothing and never
    /// returns `CompactedTooOld`.
    Bookmark(Revision),
}

impl WatchSignal {
    /// The revision this signal carries (event revision or bookmark
    /// revision). Used to track `last_seen` + ordering.
    #[must_use]
    pub fn revision(&self) -> Revision {
        match self {
            WatchSignal::Event(ev) => Revision(ev.resource_version),
            WatchSignal::Bookmark(rev) => *rev,
        }
    }

    /// `true` for [`WatchSignal::Event`].
    #[must_use]
    pub fn is_event(&self) -> bool {
        matches!(self, WatchSignal::Event(_))
    }

    /// `true` for [`WatchSignal::Bookmark`].
    #[must_use]
    pub fn is_bookmark(&self) -> bool {
        matches!(self, WatchSignal::Bookmark(_))
    }
}

/// The shared terminal-Gone slot. The producer side (apply / feeder)
/// arms it exactly once on overflow; the [`WatchStream`] observes it
/// promptly (via [`GoneSlot::armed`] in its `select!`) AND on a clean
/// channel close.
///
/// The `Notify` wakes a parked `WatchStream::next()` the instant the
/// slot is armed — otherwise an overflowed watcher whose channel still
/// has a live replay-sender clone would park forever waiting for a
/// close that never comes.
#[derive(Debug, Default)]
struct GoneSlot {
    gone: std::sync::Mutex<Option<WatchGone>>,
    notify: tokio::sync::Notify,
}

impl GoneSlot {
    /// Arm the terminal reason iff not already armed, then wake any
    /// parked stream. Idempotent — "converted exactly once".
    fn arm(&self, reason: WatchGone) {
        {
            let mut g = self.gone.lock().expect("gone slot poisoned");
            if g.is_none() {
                *g = Some(reason);
            }
        }
        self.notify.notify_waiters();
    }

    /// `true` if a terminal reason has been armed (without consuming it).
    fn armed(&self) -> bool {
        self.gone.lock().expect("gone slot poisoned").is_some()
    }

    fn take(&self) -> Option<WatchGone> {
        self.gone.lock().expect("gone slot poisoned").take()
    }
}

/// Options controlling a [`watch_from`](WatcherRegistry::watch_from)
/// subscription.
#[derive(Clone, Debug)]
pub struct WatchOpts {
    /// Resume point. `Revision::ZERO` means "from the beginning of
    /// retained history" (i.e. every change still in the ring).
    pub from: Revision,
    /// Per-watcher bounded-channel capacity. The ONLY place loss is
    /// possible — and there it is a typed [`WatchGone::Overflow`].
    pub buffer: usize,
    /// Bookmark cadence. `Duration::ZERO` disables bookmarks for this
    /// watcher.
    pub bookmark_every: Duration,
}

impl Default for WatchOpts {
    fn default() -> Self {
        Self {
            from: Revision::ZERO,
            buffer: WATCH_CHANNEL_CAPACITY,
            bookmark_every: DEFAULT_BOOKMARK_EVERY,
        }
    }
}

impl WatchOpts {
    /// Construct opts resuming from `from` with default buffer +
    /// bookmark cadence.
    #[must_use]
    pub fn from_revision(from: Revision) -> Self {
        Self {
            from,
            ..Self::default()
        }
    }

    /// Live-tail-only opts (no replay, no bookmarks) — used by the
    /// `watch()` compatibility shim.
    #[must_use]
    pub fn live_tail(from: Revision, buffer: usize) -> Self {
        Self {
            from,
            buffer: buffer.max(1),
            bookmark_every: Duration::ZERO,
        }
    }
}

/// A registered live watcher. Lives inside the catalog-lock-protected
/// registry; the apply path fans events to it via non-blocking
/// `try_send`.
struct WatcherHandle {
    /// The live event/bookmark sender. Bounded; `try_send` is
    /// non-blocking so the apply path never awaits a slow consumer.
    tx: mpsc::Sender<WatchSignal>,
    /// The replay high-water revision captured under the lock at
    /// subscription. The live fan-out drops any straggler
    /// `revision <= boundary` (belt-and-suspenders against
    /// re-delivery; with the lock held there should be none, but the
    /// tag makes dup-freedom an invariant, not a timing accident).
    boundary: Revision,
    /// Highest revision actually enqueued to this watcher — the
    /// accurate resume point for [`WatchGone::Overflow`].
    last_seen: Revision,
    /// Buffer capacity — carried so the overflow Gone reports it.
    capacity: usize,
    /// Bookmark cadence. `ZERO` disables bookmarks.
    bookmark_every: Duration,
    /// When this watcher last received a bookmark (or was registered).
    /// Gates per-watcher cadence against the store-level ticker.
    last_bookmark_at: tokio::time::Instant,
    /// The revision of the last bookmark actually delivered to this
    /// watcher (`None` = none yet). Suppresses repeated same-revision
    /// bookmarks while still emitting the FIRST heartbeat at the
    /// current revision (even on a quiescent store) + a fresh one each
    /// time the revision advances.
    last_bookmarked_rev: Option<Revision>,
    /// Terminal-Gone slot shared with the [`WatchStream`].
    gone: Arc<GoneSlot>,
    /// `true` once overflow has been armed — the handle is dead + will
    /// be reaped on the next sweep.
    overflowed: bool,
}

impl WatcherHandle {
    /// Enqueue one signal via non-blocking `try_send`.
    ///
    /// Returns `HandleSendOutcome` so the caller (registry) can reap
    /// closed/overflowed handles. On `Full` the handle is armed with a
    /// typed [`WatchGone::Overflow`] — never a silent drop, never a
    /// block.
    fn try_enqueue(&mut self, signal: WatchSignal) -> HandleSendOutcome {
        if self.overflowed {
            return HandleSendOutcome::Dead;
        }
        let rev = signal.revision();
        match self.tx.try_send(signal) {
            Ok(()) => {
                if rev > self.last_seen {
                    self.last_seen = rev;
                }
                HandleSendOutcome::Sent
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                // Bounded channel full → typed terminal Gone. The
                // consumer resumes from last_seen gap-free.
                self.gone.arm(WatchGone::Overflow {
                    capacity: self.capacity,
                    last_seen: self.last_seen,
                });
                self.overflowed = true;
                HandleSendOutcome::Overflowed
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // Consumer dropped the stream — reap the handle.
                HandleSendOutcome::Closed
            }
        }
    }
}

/// What happened when the registry tried to push a signal to a handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HandleSendOutcome {
    /// Delivered.
    Sent,
    /// Buffer was full → handle armed with `Overflow` + marked dead.
    Overflowed,
    /// Receiver dropped → reap the handle.
    Closed,
    /// Handle was already overflowed (dead) → reap.
    Dead,
}

impl HandleSendOutcome {
    /// `true` when the handle should be reaped from the registry.
    fn should_reap(self) -> bool {
        matches!(self, Self::Overflowed | Self::Closed | Self::Dead)
    }
}

/// The set of live watchers. One instance lives inside each backend's
/// catalog-lock-protected `Inner` / `MaterializedState`, so registering
/// a watcher + capturing the replay boundary + fanning live events all
/// happen under the SAME lock the catalog `apply` holds — that is the
/// crux that makes the replay→live handoff gap-free + dup-free.
#[derive(Default)]
pub struct WatcherRegistry {
    handles: Vec<WatcherHandle>,
}

impl WatcherRegistry {
    /// Construct an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of live watchers — telemetry + the `watch_subscriber_count`
    /// surface.
    #[must_use]
    pub fn len(&self) -> usize {
        self.handles.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    /// Register a new watcher UNDER THE CALLER'S CATALOG LOCK.
    ///
    /// `catalog` is the live catalog (already locked by the caller).
    /// `opts` is the subscription request.
    ///
    /// On success returns the consumer-facing [`WatchStream`]. The
    /// replay (every change with `revision > opts.from`, in revision
    /// order) has ALREADY been enqueued into the stream's channel by the
    /// time this returns — see [`Self::register_captured`].
    ///
    /// The replay is captured + fed here, under the lock, at the same
    /// instant the live sender becomes reachable by `fan_change` — so:
    ///
    ///   * every change with `revision <= boundary` is enqueued FIRST,
    ///     in revision order, before the handle is pushed onto the
    ///     registry,
    ///   * every change with `revision > boundary` reaches this watcher
    ///     via the live fan-out — and only AFTER the replay, because
    ///     `fan_change` cannot see the handle until this call returns
    ///     and runs under the SAME lock,
    ///   * nothing in between — no gap, no dup, no reorder.
    ///
    /// A rejected watch (`from` below compaction) allocates NO channel
    /// — it returns `Err(WatchGone::CompactedTooOld)` before any sender
    /// is registered.
    ///
    /// # Errors
    ///
    /// [`WatchGone::CompactedTooOld`] when `opts.from` is below the
    /// catalog's compaction watermark.
    pub fn register(
        &mut self,
        catalog: &ResourceCatalog,
        opts: &WatchOpts,
    ) -> Result<WatchStream, WatchGone> {
        // (a) Capture replay + boundary BEFORE allocating anything. A
        // rejected watch costs nothing. Both reads happen here so the
        // caller can do them under the catalog lock atomically with the
        // sender registration in `register_captured`.
        let replay = catalog.changes_since(opts.from).map_err(WatchGone::from)?;
        let boundary = catalog.revision();
        Ok(self.register_captured(replay, boundary, opts))
    }

    /// Register a watcher from an ALREADY-CAPTURED replay + boundary,
    /// feeding the replay into the channel BEFORE the handle is live.
    ///
    /// Lets callers that can't split-borrow the catalog + the registry
    /// from one lock guard (e.g. via a `tokio::MutexGuard`'s `Deref`)
    /// read `changes_since` / `revision` first, bind the owned `replay`
    /// + Copy `boundary`, then call this with only `&mut self`. The
    /// reads + this call MUST happen under the SAME catalog lock for the
    /// handoff to be atomic.
    ///
    /// ## Why the replay is fed here (not on a spawned task)
    ///
    /// The replay is `try_send`-pushed into the per-watcher channel
    /// IN REVISION ORDER, then the handle is appended to the registry.
    /// Only after this returns can `fan_change` (which runs under the
    /// same lock) reach the handle — so every live event lands strictly
    /// after the whole replay. Insertion order == delivery order ==
    /// revision order, by construction. There is no second producer and
    /// no timing window.
    ///
    /// If the replay is larger than `opts.buffer`, the channel fills
    /// mid-feed; we arm [`WatchGone::Overflow`] carrying the highest
    /// replay revision actually enqueued (the resume point). The handle
    /// is still registered (already overflowed) so it is reaped on the
    /// next `fan_change` / `tick_bookmarks` sweep; the stream drains its
    /// buffered prefix then surfaces the single terminal Gone. A
    /// far-behind watcher thus re-lists, the same contract a compacted
    /// resume point gets — never a silent drop, never a block.
    pub fn register_captured(
        &mut self,
        replay: Vec<Change>,
        boundary: Revision,
        opts: &WatchOpts,
    ) -> WatchStream {
        // Allocate the bounded channel + build the handle.
        let capacity = opts.buffer.max(1);
        let (tx, rx) = mpsc::channel::<WatchSignal>(capacity);
        let gone = Arc::new(GoneSlot::default());
        // last_seen starts at the resume point: the consumer has
        // logically "seen" everything up to `from`.
        let last_seen = opts.from;
        let mut handle = WatcherHandle {
            tx,
            boundary,
            last_seen,
            capacity,
            bookmark_every: opts.bookmark_every,
            last_bookmark_at: tokio::time::Instant::now(),
            last_bookmarked_rev: None,
            gone: gone.clone(),
            overflowed: false,
        };

        // Feed the replay FIRST, in revision order, through the same
        // FIFO channel the live path uses — under the lock, before the
        // handle is reachable by `fan_change`. `try_enqueue` accounts
        // `last_seen` per signal and arms `Overflow` (capturing the
        // resume point) if the buffer fills mid-replay. We stop feeding
        // on the first non-delivered outcome (overflow / closed) — there
        // is no point pushing more into a dead channel.
        for change in replay {
            let ev = watch_event_from_change(&change);
            if handle.try_enqueue(WatchSignal::Event(ev)) != HandleSendOutcome::Sent {
                break;
            }
        }

        // Register the handle (now carrying the buffered replay) so the
        // live fan-out can reach it. Anything `fan_change` enqueues from
        // here lands strictly after the replay already in the channel.
        self.handles.push(handle);

        WatchStream {
            rx,
            gone,
            last_seen,
            terminal_emitted: false,
            draining_for_gone: false,
        }
    }

    /// Fan one committed change to every live watcher whose
    /// `boundary < change.revision`. Called UNDER THE CATALOG LOCK by
    /// the apply path, right after `push_history` has run.
    ///
    /// Builds the typed [`WatchEvent`] from the [`Change`] + the
    /// resource op (Added vs Modified is decided by whether this Put
    /// was a create).
    ///
    /// Non-blocking: each delivery is `try_send`. Full → typed Gone +
    /// reap (never a silent drop, never a block). Closed → reap.
    pub fn fan_change(&mut self, change: &Change) {
        let ev = watch_event_from_change(change);
        let boundary_floor = change.revision;
        self.handles.retain_mut(|h| {
            // Dup-freedom invariant: a watcher only receives events
            // strictly past its replay boundary.
            if h.boundary >= boundary_floor {
                return true;
            }
            !h.try_enqueue(WatchSignal::Event(ev.clone())).should_reap()
        });
    }

    /// Tick bookmarks: for every live watcher that is DUE (cadence
    /// non-zero AND `now - last_bookmark_at >= bookmark_every`) AND not
    /// already bookmarked at `current` (`last_bookmarked_rev !=
    /// Some(current)`), enqueue a `Bookmark(current)`. Called UNDER THE
    /// CATALOG LOCK by the store-level ticker, which passes
    /// `now = tokio::time::Instant::now()`.
    ///
    /// The `last_bookmarked_rev` gate (rather than `last_seen <
    /// current`) means: a quiescent store STILL emits the first
    /// heartbeat bookmark at the current revision (even when the
    /// watcher subscribed at that revision), but repeated same-revision
    /// bookmarks are suppressed; a fresh bookmark fires each time the
    /// revision advances. Consecutive bookmarks are non-decreasing.
    ///
    /// Bookmarks are best-effort: a Full channel SKIPS the bookmark
    /// (it's redundant with the pending events) and is NOT counted as
    /// overflow.
    pub fn tick_bookmarks(&mut self, current: Revision, now: tokio::time::Instant) {
        self.handles.retain_mut(|h| {
            if h.overflowed {
                return false; // reap already-dead handles
            }
            let due = !h.bookmark_every.is_zero()
                && now.duration_since(h.last_bookmark_at) >= h.bookmark_every;
            if !due || h.last_bookmarked_rev == Some(current) {
                return true;
            }
            match h.tx.try_send(WatchSignal::Bookmark(current)) {
                Ok(()) => {
                    if current > h.last_seen {
                        h.last_seen = current;
                    }
                    h.last_bookmarked_rev = Some(current);
                    h.last_bookmark_at = now;
                    true
                }
                // Full → skip the bookmark (redundant), keep the handle.
                Err(mpsc::error::TrySendError::Full(_)) => true,
                // Closed → reap.
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            }
        });
    }

    /// Reap any handles whose receivers have been dropped. Cheap O(n)
    /// sweep; safe to call opportunistically.
    pub fn reap_closed(&mut self) {
        self.handles
            .retain(|h| !h.overflowed && !h.tx.is_closed());
    }
}

/// Build the typed [`WatchEvent`] for a committed [`Change`].
///
/// Added vs Modified for a `Put` is derived from the change itself:
/// `change.prior.is_none()` means a first-create (Added); `Some(_)`
/// means a replace/patch (Modified) — the EXACT same distinction
/// `ResourceCatalog::apply` makes when it returns `ResourceOp::Created`
/// vs `Replaced`. A `Delete` is always `Deleted`. The event's `object`
/// is the change's post-image (the tombstone for a delete — never
/// Null), and `resource_version` is the global MVCC revision.
///
/// Deriving from the Change (not a threaded `was_create` flag) means
/// replay + live take the same exact path with no extra state — the
/// boundary handoff can't disagree on Added/Modified for the same
/// revision.
#[must_use]
pub fn watch_event_from_change(change: &Change) -> WatchEvent {
    let kind = match change.kind {
        crate::revision::ChangeKind::Put => {
            if change.prior.is_none() {
                WatchEventKind::Added
            } else {
                WatchEventKind::Modified
            }
        }
        crate::revision::ChangeKind::Delete => WatchEventKind::Deleted,
    };
    WatchEvent {
        kind,
        object: change.value.clone(),
        key: change.key.clone(),
        resource_version: change.revision.get(),
    }
}

/// The consumer-facing watch stream. Yields typed [`WatchSignal`]s; on
/// terminal loss / compaction yields `Err(WatchGone)` exactly once,
/// then ends.
#[derive(Debug)]
pub struct WatchStream {
    rx: mpsc::Receiver<WatchSignal>,
    gone: Arc<GoneSlot>,
    last_seen: Revision,
    terminal_emitted: bool,
    /// Once the gone slot is observed armed, we switch to "drain then
    /// emit Gone" mode: deliver every still-buffered event FIRST (so
    /// the consumer gets the in-order prefix it was promised), then
    /// surface the single terminal `Err`.
    draining_for_gone: bool,
}

impl WatchStream {
    /// Next signal:
    ///
    ///   * `Some(Ok(signal))` — an Event or Bookmark.
    ///   * `Some(Err(gone))` — the typed terminal reason, emitted
    ///     exactly once (after the in-order prefix of buffered events).
    ///   * `None` — the stream is fully drained AND closed (the store
    ///     was dropped, or after the single terminal `Err`).
    ///
    /// Never surfaces `broadcast::RecvError::Lagged` — overflow is the
    /// typed [`WatchGone::Overflow`].
    pub async fn next(&mut self) -> Option<Result<WatchSignal, WatchGone>> {
        if self.terminal_emitted {
            return None;
        }

        // If Gone was armed before we got here (e.g. armed while the
        // consumer wasn't polling), switch to drain mode now — don't
        // park on a `notified()` whose `notify_waiters` already fired.
        if !self.draining_for_gone && self.gone.armed() {
            self.draining_for_gone = true;
        }

        // Drain-then-Gone mode: deliver buffered events first, then the
        // terminal. `try_recv` is non-blocking — we never park here.
        if self.draining_for_gone {
            return Some(self.next_while_draining());
        }

        // Normal mode: park on the channel OR a gone-armed wake,
        // whichever fires first.
        loop {
            tokio::select! {
                biased;
                recvd = self.rx.recv() => match recvd {
                    Some(signal) => return Some(Ok(self.account(signal))),
                    None => {
                        // Channel closed: armed terminal Gone (producer
                        // dropped the sender after arming) or a clean
                        // store-dropped close.
                        return self.close_or_gone();
                    }
                },
                () = self.gone.notify.notified() => {
                    // Gone was armed while we were parked. Switch to
                    // drain mode + deliver the buffered prefix first.
                    if self.gone.armed() {
                        self.draining_for_gone = true;
                        return Some(self.next_while_draining());
                    }
                    // Spurious wake (no arm yet) — loop + re-park.
                }
            }
        }
    }

    /// Non-blocking peek: like [`Self::next`] but returns immediately.
    ///
    ///   * `Some(Ok(signal))` — a buffered Event or Bookmark.
    ///   * `Some(Err(gone))` — the terminal reason (after the buffered
    ///     prefix is drained), emitted exactly once.
    ///   * `None` — nothing buffered right now (the stream may still be
    ///     live), OR the stream is fully drained + closed.
    ///
    /// Used by consumers that coalesce a burst (drain whatever is queued
    /// without parking). `None` does NOT mean "closed" in the
    /// non-draining case — it means "empty for now"; use [`Self::next`]
    /// to park for the next signal.
    pub fn try_next(&mut self) -> Option<Result<WatchSignal, WatchGone>> {
        if self.terminal_emitted {
            return None;
        }
        if !self.draining_for_gone && self.gone.armed() {
            self.draining_for_gone = true;
        }
        if self.draining_for_gone {
            return Some(self.next_while_draining());
        }
        match self.rx.try_recv() {
            Ok(signal) => Some(Ok(self.account(signal))),
            Err(mpsc::error::TryRecvError::Empty) => None,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                // All senders gone: surface an armed Gone once, else
                // signal closed by returning None.
                self.close_or_gone()
            }
        }
    }

    /// In drain-then-Gone mode: deliver one buffered event, or — when
    /// the buffer is empty — the single terminal Gone.
    fn next_while_draining(&mut self) -> Result<WatchSignal, WatchGone> {
        match self.rx.try_recv() {
            Ok(signal) => Ok(self.account(signal)),
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                // Buffer drained → emit the terminal reason exactly once.
                let gone = self
                    .gone
                    .take()
                    .unwrap_or(WatchGone::Overflow {
                        capacity: 0,
                        last_seen: self.last_seen,
                    });
                self.terminal_emitted = true;
                Err(gone)
            }
        }
    }

    /// On a clean channel close: surface an armed Gone (once) or `None`.
    fn close_or_gone(&mut self) -> Option<Result<WatchSignal, WatchGone>> {
        if let Some(gone) = self.gone.take() {
            self.terminal_emitted = true;
            Some(Err(gone))
        } else {
            None
        }
    }

    /// Track `last_seen` as each signal flows past.
    fn account(&mut self, signal: WatchSignal) -> WatchSignal {
        let rev = signal.revision();
        if rev > self.last_seen {
            self.last_seen = rev;
        }
        signal
    }

    /// The highest revision this stream has delivered — the resume
    /// point after a [`WatchGone`].
    #[must_use]
    pub fn last_seen(&self) -> Revision {
        self.last_seen
    }
}

/// The minimum granularity at which the store-level bookmark ticker
/// wakes. Per-watcher cadence (`WatchOpts::bookmark_every`) is gated
/// inside [`WatcherRegistry::tick_bookmarks`]; this is just how often
/// the ticker checks. A watcher with a 50ms cadence is served within
/// one granularity tick of becoming due.
pub const BOOKMARK_TICK_GRANULARITY: Duration = Duration::from_millis(50);

/// A backend whose catalog + watcher registry can be ticked for
/// bookmarks under one lock. Both [`crate::store::InMemoryStore`] and
/// [`crate::fjall_store::FjallStore`] implement this; the store-level
/// ticker drives it without caring which backend it is.
///
/// `tick_once` acquires the backend's catalog lock, reads the current
/// revision, and calls `registry.tick_bookmarks(rev, now)` — all under
/// the SAME lock the apply path holds, so a bookmark + a concurrent
/// apply can't interleave on the registry.
pub trait BookmarkTickable: Send + Sync + 'static {
    /// Tick bookmarks once under the catalog lock. Returns the future
    /// to await (the lock is async).
    fn tick_once(&self) -> impl std::future::Future<Output = ()> + Send;
}

/// A handle to the store-level bookmark ticker task. Dropping it aborts
/// the ticker (the watchers themselves continue to receive live
/// events; only periodic bookmarking stops).
pub struct BookmarkTicker {
    handle: tokio::task::JoinHandle<()>,
}

impl BookmarkTicker {
    /// Spawn a single store-level bookmark ticker driving `weak`. The
    /// ticker wakes every [`BOOKMARK_TICK_GRANULARITY`], upgrades the
    /// `Weak`, and ticks; when the upgrade fails (the store was
    /// dropped) it exits cleanly — no leaked task.
    ///
    /// One ticker per store (not one per watcher).
    #[must_use]
    pub fn spawn<T: BookmarkTickable>(weak: std::sync::Weak<T>) -> Self {
        let handle = tokio::spawn(async move {
            let mut tick = tokio::time::interval(BOOKMARK_TICK_GRANULARITY);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let Some(strong) = weak.upgrade() else {
                    return; // store dropped — stop ticking.
                };
                strong.tick_once().await;
                drop(strong); // don't keep the store alive across the sleep.
            }
        });
        Self { handle }
    }
}

impl Drop for BookmarkTicker {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::ResourceKey;
    use crate::revision::{ChangeKind, VersionMeta};

    fn pod_key(name: &str) -> ResourceKey {
        ResourceKey::namespaced("", "v1", "Pod", "default", name)
    }

    fn change_at(rev: u64, name: &str, kind: ChangeKind) -> Change {
        Change {
            revision: Revision(rev),
            key: pod_key(name),
            kind,
            value: serde_json::json!({"spec": {"r": rev}}),
            prior: None,
            version_meta: VersionMeta::created_at(Revision(rev)),
        }
    }

    // ── WatchGone From + kind() ────────────────────────────────────

    #[test]
    fn watch_gone_from_compacted_maps_fields() {
        let e = CompactedTooOld {
            requested: Revision(2),
            compacted: Revision(5),
        };
        let g: WatchGone = e.into();
        assert_eq!(
            g,
            WatchGone::CompactedTooOld {
                requested: Revision(2),
                compacted: Revision(5),
            }
        );
        assert_eq!(g.kind(), "compacted_too_old");
    }

    #[test]
    fn watch_gone_overflow_kind() {
        let g = WatchGone::Overflow {
            capacity: 4,
            last_seen: Revision(7),
        };
        assert_eq!(g.kind(), "overflow");
        // ErrorKind trait dispatch also works.
        fn poly<E: engenho_substrate::error_kind::ErrorKind>(e: &E) -> &'static str {
            e.kind()
        }
        assert_eq!(poly(&g), "overflow");
    }

    // ── WatchSignal helpers ────────────────────────────────────────

    fn change_with_prior(rev: u64, name: &str) -> Change {
        Change {
            revision: Revision(rev),
            key: pod_key(name),
            kind: ChangeKind::Put,
            value: serde_json::json!({"spec": {"r": rev}}),
            prior: Some(serde_json::json!({"spec": {"r": rev - 1}})),
            version_meta: VersionMeta::created_at(Revision(rev)).bumped_at(Revision(rev)),
        }
    }

    #[test]
    fn watch_signal_revision_and_predicates() {
        let ev = WatchSignal::Event(watch_event_from_change(&change_at(3, "p", ChangeKind::Put)));
        assert_eq!(ev.revision(), Revision(3));
        assert!(ev.is_event());
        assert!(!ev.is_bookmark());

        let bm = WatchSignal::Bookmark(Revision(9));
        assert_eq!(bm.revision(), Revision(9));
        assert!(bm.is_bookmark());
        assert!(!bm.is_event());
    }

    #[test]
    fn watch_event_from_change_kinds() {
        // No prior → Added.
        let added = watch_event_from_change(&change_at(1, "p", ChangeKind::Put));
        assert_eq!(added.kind, WatchEventKind::Added);
        assert_eq!(added.resource_version, 1);

        // Prior present → Modified.
        let modified = watch_event_from_change(&change_with_prior(2, "p"));
        assert_eq!(modified.kind, WatchEventKind::Modified);

        let deleted = watch_event_from_change(&change_at(3, "p", ChangeKind::Delete));
        assert_eq!(deleted.kind, WatchEventKind::Deleted);
    }

    // ── Registry register → replay + boundary ──────────────────────

    #[tokio::test]
    async fn register_returns_replay_tail_and_no_dup_live() {
        let mut cat = ResourceCatalog::default();
        for i in 1..=5u64 {
            cat.apply(
                &crate::command::ResourceCommand::Put {
                    key: pod_key(&format!("p{i}")),
                    value: serde_json::json!({"i": i}),
                    reason: crate::command::Reason::Operator,
                },
                1,
                i,
            );
        }
        let mut reg = WatcherRegistry::new();
        // Register from rev2 → replay {3,4,5} is enqueued UNDER this call,
        // before the handle is reachable by fan_change. No feeder task,
        // no replay-sender tuple.
        let mut stream = reg
            .register(&cat, &WatchOpts::from_revision(Revision(2)))
            .unwrap();

        // IMMEDIATELY fan a live change at rev 6 — boundary captured at
        // register == 5, so rev 6 > boundary → delivered AFTER the replay
        // by construction (replay already sits in the FIFO ahead of it).
        let live = change_at(6, "p6", ChangeKind::Put);
        reg.fan_change(&live);

        // Drain: 3,4,5 (replay) then 6 (live), contiguous, no dup, no
        // reorder — even though fan_change ran before we polled once.
        let mut got = Vec::new();
        for _ in 0..4 {
            if let Some(Ok(WatchSignal::Event(ev))) = stream.next().await {
                got.push(ev.resource_version);
            }
        }
        assert_eq!(got, vec![3, 4, 5, 6]);
    }

    /// Structural ordering guard at the registry layer: the replay is
    /// enqueued by `register_captured` BEFORE `fan_change` can reach the
    /// handle, so a live event committed at boundary+1 can NEVER overtake
    /// not-yet-fed replay revisions. This is the unit-level mirror of the
    /// store-level boundary-ordering regression test — it FAILS with the
    /// old `tokio::spawn(feed)` path (delivery [6,3,4,5]) and PASSES now.
    #[tokio::test]
    async fn replay_precedes_live_even_when_fan_runs_before_first_poll() {
        let mut cat = ResourceCatalog::default();
        for i in 1..=5u64 {
            cat.apply(
                &crate::command::ResourceCommand::Put {
                    key: pod_key(&format!("p{i}")),
                    value: serde_json::json!({"i": i}),
                    reason: crate::command::Reason::Operator,
                },
                1,
                i,
            );
        }
        let mut reg = WatcherRegistry::new();
        // Capture replay {3,4,5} + boundary 5, then register_captured.
        let replay = cat.changes_since(Revision(2)).unwrap();
        let boundary = cat.revision();
        assert_eq!(
            replay.iter().map(|c| c.revision.0).collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
        let mut stream = reg.register_captured(replay, boundary, &WatchOpts::from_revision(Revision(2)));

        // Fan the live event at boundary+1 (== 6) WITHOUT polling first.
        reg.fan_change(&change_at(6, "p6", ChangeKind::Put));

        // Exact contiguous ordered range — no reorder, no gap, no dup.
        let mut got = Vec::new();
        for _ in 0..4 {
            match stream.next().await {
                Some(Ok(WatchSignal::Event(ev))) => got.push(ev.resource_version),
                other => panic!("unexpected signal: {other:?}"),
            }
        }
        assert_eq!(got, vec![3, 4, 5, 6]);
    }

    /// A replay larger than the per-watcher buffer overflows DURING the
    /// under-the-lock feed: the consumer gets the in-order prefix then a
    /// typed `WatchGone::Overflow` whose `last_seen` == the highest
    /// replay revision actually enqueued (the resume point). Never a
    /// silent drop, never a block on the lock.
    #[tokio::test]
    async fn oversized_replay_overflows_with_typed_gone_under_lock() {
        let mut cat = ResourceCatalog::default();
        for i in 1..=10u64 {
            cat.apply(
                &crate::command::ResourceCommand::Put {
                    key: pod_key(&format!("p{i}")),
                    value: serde_json::json!({"i": i}),
                    reason: crate::command::Reason::Operator,
                },
                1,
                i,
            );
        }
        let mut reg = WatcherRegistry::new();
        // Replay = revs 1..=10 (from ZERO), buffer = 3 → overflow mid-feed.
        let mut stream = reg
            .register(&cat, &WatchOpts::live_tail(Revision::ZERO, 3))
            .unwrap();
        // The overflowed handle is reaped on the next sweep.
        reg.fan_change(&change_at(11, "p11", ChangeKind::Put));
        assert_eq!(reg.len(), 0, "overflowed handle reaped");

        // Drain the in-order prefix then exactly one terminal Overflow.
        let mut prefix = Vec::new();
        let gone = loop {
            match stream.next().await {
                Some(Ok(WatchSignal::Event(ev))) => prefix.push(ev.resource_version),
                Some(Ok(WatchSignal::Bookmark(_))) => {}
                Some(Err(g)) => break g,
                None => panic!("stream closed without surfacing Overflow"),
            }
        };
        assert!(!prefix.is_empty(), "delivered an in-order prefix: {prefix:?}");
        let mut sorted = prefix.clone();
        sorted.sort_unstable();
        assert_eq!(prefix, sorted, "prefix is in revision order");
        match gone {
            WatchGone::Overflow { capacity, last_seen } => {
                assert_eq!(capacity, 3);
                assert_eq!(
                    last_seen.get(),
                    *prefix.last().unwrap(),
                    "last_seen == highest replay revision actually enqueued"
                );
            }
            other => panic!("expected Overflow, got {other:?}"),
        }
        assert!(stream.next().await.is_none(), "stream ends after the terminal");
    }

    #[tokio::test]
    async fn register_below_compaction_errors_and_allocates_nothing() {
        let mut cat = ResourceCatalog::with_history_capacity(2);
        for i in 1..=5u64 {
            cat.apply(
                &crate::command::ResourceCommand::Put {
                    key: pod_key(&format!("p{i}")),
                    value: serde_json::json!({"i": i}),
                    reason: crate::command::Reason::Operator,
                },
                1,
                i,
            );
        }
        // compacted at rev 3.
        let mut reg = WatcherRegistry::new();
        let err = reg
            .register(&cat, &WatchOpts::from_revision(Revision(1)))
            .unwrap_err();
        assert_eq!(
            err,
            WatchGone::CompactedTooOld {
                requested: Revision(1),
                compacted: Revision(3),
            }
        );
        // No channel registered.
        assert_eq!(reg.len(), 0);

        // From exactly the watermark succeeds; the replay (revs 4,5) is
        // enqueued into the stream — drain it to prove 2 events landed.
        let mut stream = reg
            .register(&cat, &WatchOpts::from_revision(Revision(3)))
            .unwrap();
        assert_eq!(reg.len(), 1);
        let mut replayed = Vec::new();
        while let Some(Ok(WatchSignal::Event(ev))) = stream.try_next() {
            replayed.push(ev.resource_version);
        }
        assert_eq!(replayed, vec![4, 5]);
    }

    // ── Overflow → typed Gone, isolated per-watcher ────────────────

    #[tokio::test]
    async fn overflow_arms_typed_gone_and_isolates_watchers() {
        let cat = ResourceCatalog::default();
        let mut reg = WatcherRegistry::new();
        // Slow watcher: buffer 2, never drained.
        let mut slow = reg
            .register(&cat, &WatchOpts::live_tail(Revision(0), 2))
            .unwrap();
        // Fast watcher: ample buffer, will be drained.
        let mut fast = reg
            .register(&cat, &WatchOpts::live_tail(Revision(0), 64))
            .unwrap();

        // Fan 10 events. Slow overflows after filling 2; fast holds all.
        for i in 1..=10u64 {
            reg.fan_change(&change_at(i, &format!("p{i}"), ChangeKind::Put));
        }

        // Slow: delivers a prefix (>=2 events) then terminal Overflow.
        let mut slow_revs = Vec::new();
        let mut slow_gone = None;
        while let Some(item) = slow.next().await {
            match item {
                Ok(WatchSignal::Event(ev)) => slow_revs.push(ev.resource_version),
                Ok(WatchSignal::Bookmark(_)) => {}
                Err(g) => {
                    slow_gone = Some(g);
                    break;
                }
            }
        }
        assert!(slow_revs.len() >= 2, "slow got a prefix: {slow_revs:?}");
        match slow_gone.expect("slow terminated with Gone") {
            WatchGone::Overflow { capacity, last_seen } => {
                assert_eq!(capacity, 2);
                assert_eq!(last_seen.get(), *slow_revs.last().unwrap());
            }
            other => panic!("expected Overflow, got {other:?}"),
        }
        // After the terminal, stream ends.
        assert!(slow.next().await.is_none());

        // Fast: receives ALL 10 in order — overflow was isolated.
        let mut fast_revs = Vec::new();
        for _ in 0..10 {
            match fast.next().await {
                Some(Ok(WatchSignal::Event(ev))) => fast_revs.push(ev.resource_version),
                other => panic!("fast unexpected: {other:?}"),
            }
        }
        assert_eq!(fast_revs, (1..=10).collect::<Vec<_>>());
    }

    // ── Closed receiver → handle reaped, no panic ──────────────────

    #[tokio::test]
    async fn closed_receiver_is_reaped_on_next_fan() {
        let cat = ResourceCatalog::default();
        let mut reg = WatcherRegistry::new();
        let stream = reg
            .register(&cat, &WatchOpts::live_tail(Revision(0), 8))
            .unwrap();
        assert_eq!(reg.len(), 1);
        drop(stream); // consumer goes away — the only receiver closes
        // Next fan reaps the closed handle.
        reg.fan_change(&change_at(1, "p", ChangeKind::Put));
        assert_eq!(reg.len(), 0);
    }

    // ── Bookmark tick advances last_seen + skips when redundant ────

    #[tokio::test]
    async fn bookmark_tick_delivers_then_advances() {
        let cat = ResourceCatalog::default();
        let mut reg = WatcherRegistry::new();
        let mut stream = reg
            .register(
                &cat,
                &WatchOpts {
                    from: Revision(0),
                    buffer: 8,
                    bookmark_every: Duration::from_millis(10),
                },
            )
            .unwrap();
        let base = tokio::time::Instant::now();
        // `now` far past the cadence → due. Tick a bookmark at rev 5.
        reg.tick_bookmarks(Revision(5), base + Duration::from_secs(1));
        match stream.next().await {
            Some(Ok(WatchSignal::Bookmark(rev))) => assert_eq!(rev, Revision(5)),
            other => panic!("expected bookmark, got {other:?}"),
        }
        // last_seen now 5 → ticking at 5 again delivers nothing.
        reg.tick_bookmarks(Revision(5), base + Duration::from_secs(2));
        // Tick at 7 (well past cadence) delivers a new bookmark.
        reg.tick_bookmarks(Revision(7), base + Duration::from_secs(3));
        match stream.next().await {
            Some(Ok(WatchSignal::Bookmark(rev))) => assert_eq!(rev, Revision(7)),
            other => panic!("expected bookmark 7, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn bookmark_gated_by_cadence() {
        // Two ticks closer together than the cadence: only the first
        // delivers.
        let cat = ResourceCatalog::default();
        let mut reg = WatcherRegistry::new();
        let mut stream = reg
            .register(
                &cat,
                &WatchOpts {
                    from: Revision(0),
                    buffer: 8,
                    bookmark_every: Duration::from_secs(5),
                },
            )
            .unwrap();
        let base = tokio::time::Instant::now();
        // First tick is due (10s past register).
        reg.tick_bookmarks(Revision(3), base + Duration::from_secs(10));
        // Second tick is only 1s later (< 5s cadence) → NOT due.
        reg.tick_bookmarks(Revision(4), base + Duration::from_secs(11));
        // Only the first bookmark (rev 3) arrives.
        match stream.next().await {
            Some(Ok(WatchSignal::Bookmark(rev))) => assert_eq!(rev, Revision(3)),
            other => panic!("expected bookmark 3, got {other:?}"),
        }
        let res = tokio::time::timeout(Duration::from_millis(50), stream.next()).await;
        assert!(res.is_err(), "second tick was within cadence — no bookmark");
    }

    #[tokio::test]
    async fn bookmark_disabled_when_cadence_zero() {
        let cat = ResourceCatalog::default();
        let mut reg = WatcherRegistry::new();
        let mut stream = reg
            .register(&cat, &WatchOpts::live_tail(Revision(0), 8))
            .unwrap();
        reg.tick_bookmarks(Revision(5), tokio::time::Instant::now() + Duration::from_secs(60));
        // Nothing delivered — cadence ZERO disables bookmarks.
        let res = tokio::time::timeout(Duration::from_millis(50), stream.next()).await;
        assert!(res.is_err(), "no bookmark should arrive with ZERO cadence");
    }

    // ── Store-dropped clean close → None, not Err ──────────────────

    #[tokio::test]
    async fn store_dropped_yields_none_not_err() {
        let cat = ResourceCatalog::default();
        let mut reg = WatcherRegistry::new();
        let mut stream = reg
            .register(&cat, &WatchOpts::live_tail(Revision(0), 8))
            .unwrap();
        // Drop the registry (== store dropped). The sole sender closes
        // WITHOUT arming a Gone → clean None.
        drop(reg);
        assert!(stream.next().await.is_none());
    }
}
