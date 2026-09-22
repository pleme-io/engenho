//! A bounded, sequenced ring a reader pages through with a cursor — and can
//! wait on.
//!
//! The control plane's streams (events, logs) are long-polls, not
//! server-sent events: a reader asks for everything after its cursor, and if
//! nothing is there yet, may wait up to a budget for the next item. Items
//! carry a sequence number that only grows; when a reader's cursor has
//! fallen out of the ring, the page says so ([`Page::truncated`]) instead of
//! quietly resuming from whatever is oldest.

use std::collections::VecDeque;
use std::num::NonZeroU64;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use tokio::sync::watch;

use crate::boot::Timestamp;

/// One item with its place in the stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sequenced<T> {
    /// Its sequence number (1, 2, …).
    pub seq: NonZeroU64,
    /// When it was pushed.
    pub at: Timestamp,
    /// The item.
    pub item: T,
}

/// One page of a ring.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page<T> {
    /// The items, oldest first.
    pub items: Vec<Sequenced<T>>,
    /// Where to continue from.
    pub next_cursor: u64,
    /// `Some(oldest)` when items after the reader's cursor had already left
    /// the ring: the reader missed some; `oldest` is the cursor the ring
    /// still has everything after.
    pub truncated: Option<u64>,
}

/// A bounded ring of sequenced items.
#[derive(Debug)]
pub struct Ring<T> {
    state: Mutex<VecDeque<Sequenced<T>>>,
    cap: usize,
    /// The last sequence number pushed; readers wait on it.
    last: watch::Sender<u64>,
}

impl<T: Clone> Ring<T> {
    /// A ring holding at most `cap` items.
    #[must_use]
    pub fn new(cap: usize) -> Self {
        Self {
            state: Mutex::new(VecDeque::with_capacity(cap.min(1024))),
            cap: cap.max(1),
            last: watch::channel(0).0,
        }
    }

    /// Push an item; its sequence number.
    pub fn push(&self, item: T) -> NonZeroU64 {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let seq = NonZeroU64::new(*self.last.borrow() + 1).unwrap_or(NonZeroU64::MIN);
        if state.len() == self.cap {
            state.pop_front();
        }
        state.push_back(Sequenced {
            seq,
            at: Timestamp::now(),
            item,
        });
        self.last.send_replace(seq.get());
        seq
    }

    /// The last sequence number pushed (0 before the first push).
    #[must_use]
    pub fn last_seq(&self) -> u64 {
        *self.last.borrow()
    }

    /// Items after `after` that `keep` accepts, at most `limit`.
    #[must_use]
    pub fn page(&self, after: u64, limit: usize, keep: impl Fn(&T) -> bool) -> Page<T> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let last = *self.last.borrow();
        let oldest = state.front().map_or(last, |s| s.seq.get() - 1);
        let truncated = (after < oldest && after < last).then_some(oldest);
        let mut items = Vec::new();
        let mut scanned = after;
        for entry in state.iter().filter(|s| s.seq.get() > after) {
            if items.len() == limit {
                break;
            }
            scanned = entry.seq.get();
            if keep(&entry.item) {
                items.push(entry.clone());
            }
        }
        Page {
            items,
            next_cursor: scanned,
            truncated,
        }
    }

    /// [`Self::page`], waiting up to `wait` for an item after `after` when
    /// there is none yet.
    pub async fn wait_page(
        &self,
        after: u64,
        limit: usize,
        wait: Duration,
        keep: impl Fn(&T) -> bool,
    ) -> Page<T> {
        let page = self.page(after, limit, &keep);
        if !page.items.is_empty() || wait.is_zero() {
            return page;
        }
        let mut rx = self.last.subscribe();
        let deadline = tokio::time::Instant::now() + wait;
        let mut cursor = page.next_cursor;
        loop {
            let arrived =
                tokio::time::timeout_at(deadline, rx.wait_for(|last| *last > cursor)).await;
            let page = self.page(cursor, limit, &keep);
            if !page.items.is_empty() || arrived.is_err() {
                return page;
            }
            // Only filtered-out items arrived: keep waiting past them.
            cursor = page.next_cursor;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_page_resumes_from_its_cursor_and_says_when_it_missed_items() {
        let ring = Ring::new(3);
        for n in 1..=5 {
            ring.push(n);
        }
        let all = ring.page(0, 10, |_| true);
        assert_eq!(
            all.items.iter().map(|s| s.item).collect::<Vec<_>>(),
            [3, 4, 5]
        );
        assert_eq!(all.truncated, Some(2), "items 1 and 2 were lost");
        assert_eq!(all.next_cursor, 5);

        let tail = ring.page(3, 10, |_| true);
        assert_eq!(
            tail.items.iter().map(|s| s.item).collect::<Vec<_>>(),
            [4, 5]
        );
        assert_eq!(tail.truncated, None);

        let odd = ring.page(0, 10, |n| n % 2 == 1);
        assert_eq!(odd.items.iter().map(|s| s.item).collect::<Vec<_>>(), [3, 5]);
        assert_eq!(
            odd.next_cursor, 5,
            "the cursor passes what the filter dropped"
        );

        assert!(ring.page(5, 10, |_| true).items.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_reader_waits_for_the_next_item_or_its_budget() {
        let ring = std::sync::Arc::new(Ring::new(8));
        let empty = ring
            .wait_page(0, 10, Duration::from_millis(50), |_| true)
            .await;
        assert!(empty.items.is_empty());

        let writer = std::sync::Arc::clone(&ring);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            writer.push("even");
            writer.push("wanted");
        });
        let page = ring
            .wait_page(0, 10, Duration::from_secs(5), |s: &&str| *s == "wanted")
            .await;
        assert_eq!(
            page.items.iter().map(|s| s.item).collect::<Vec<_>>(),
            ["wanted"]
        );
    }
}
