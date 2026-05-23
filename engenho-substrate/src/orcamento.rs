//! orçamento — typed resource budget / token bucket.
//!
//! Per the research brief — fifth inventive primitive. Token bucket
//! is the canonical rate-limit primitive; orçamento is the typed
//! flavor that composes with the substrate:
//!
//!   - `relógio::Clock` for monotonic refill timing
//!   - `relógio::Instant` returned in `Exhausted { replenish_at }`
//!     so callers know when to retry
//!   - `Named` for telemetry identifier
//!   - `mirante::Observable` via `BudgetSnapshot` for dashboard wire-up
//!
//! Token-bucket invariants:
//!   - `current` ∈ [0, capacity] at all times (clamped, never overflows)
//!   - Refill is computed lazily on every `try_consume` / `available`
//!     call from `now - last_refill` × `refill_per_sec / 1000` ms
//!   - Concurrent `try_consume` is safe via `fetch_update` CAS loop
//!
//! ## Surface
//!
//!   - `Budget` — the token bucket itself
//!   - `BudgetSnapshot` — Observable snapshot for mirante
//!   - `BudgetError::Exhausted { available, requested, replenish_at }`

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::named::Named;
use crate::relogio::{Clock, Instant};

/// Budget errors.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum BudgetError {
    /// Not enough tokens available to satisfy the request.
    #[error(
        "exhausted: available={available}, requested={requested}, \
         replenish_at={replenish_at:?}"
    )]
    Exhausted {
        /// Tokens currently available.
        available: u64,
        /// Tokens the caller asked for.
        requested: u64,
        /// When enough tokens will be available, if `requested` ≤ `capacity`.
        replenish_at: Option<Instant>,
    },
    /// Request exceeds the budget's total capacity — will never be satisfiable.
    #[error("over-capacity: requested={requested}, capacity={capacity}")]
    OverCapacity {
        /// What the caller asked for.
        requested: u64,
        /// The bucket's max.
        capacity: u64,
    },
}

crate::impl_error_kind! {
    BudgetError {
        { Exhausted { .. } } => "exhausted",
        { OverCapacity { .. } } => "over_capacity",
    }
}

/// Snapshot of a budget's state — published into a mirante channel
/// for live dashboards.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BudgetSnapshot {
    /// Tokens currently available.
    pub available: u64,
    /// Total capacity (max tokens).
    pub capacity: u64,
    /// Refill rate (tokens per second).
    pub refill_per_sec: u64,
    /// When the last refill calc ran (packed Instant).
    pub last_refill_packed: u64,
}

/// Typed token-bucket budget. Thread-safe; lock-free for `try_consume`
/// (CAS loop) and `available` (single load).
pub struct Budget {
    name: &'static str,
    capacity: u64,
    refill_per_sec: u64,
    current: AtomicU64,
    last_refill_ms: AtomicU64,
    clock: Arc<dyn Clock>,
}

impl Budget {
    /// New budget starting full.
    ///
    /// # Errors
    /// Panics if `capacity == 0` (a zero-capacity budget can never be
    /// satisfied; reject at construction so callers don't ship broken
    /// configs).
    ///
    /// # Panics
    /// On `capacity == 0`.
    #[must_use]
    pub fn new(
        name: &'static str,
        capacity: u64,
        refill_per_sec: u64,
        clock: Arc<dyn Clock>,
    ) -> Self {
        assert!(capacity > 0, "Budget capacity must be > 0");
        let now = clock.now();
        Self {
            name,
            capacity,
            refill_per_sec,
            current: AtomicU64::new(capacity),
            last_refill_ms: AtomicU64::new(now.physical_ms),
            clock,
        }
    }

    /// Try to consume `n` tokens. Returns remaining tokens on success.
    ///
    /// # Errors
    /// - [`BudgetError::OverCapacity`] if `n > capacity` (un-satisfiable)
    /// - [`BudgetError::Exhausted`] if not enough tokens right now;
    ///   includes a `replenish_at: Instant` telling the caller when
    ///   to retry
    pub fn try_consume(&self, n: u64) -> Result<u64, BudgetError> {
        if n > self.capacity {
            return Err(BudgetError::OverCapacity {
                requested: n,
                capacity: self.capacity,
            });
        }
        self.refill();
        // CAS loop: try to subtract n from current.
        loop {
            let curr = self.current.load(Ordering::Acquire);
            if curr < n {
                return Err(BudgetError::Exhausted {
                    available: curr,
                    requested: n,
                    replenish_at: self.time_to_refill(n),
                });
            }
            let next = curr - n;
            if self
                .current
                .compare_exchange(curr, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(next);
            }
        }
    }

    /// Currently-available tokens (after a lazy refill).
    pub fn available(&self) -> u64 {
        self.refill();
        self.current.load(Ordering::Acquire)
    }

    /// Capacity (max tokens).
    #[must_use]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Refill rate (tokens per second).
    #[must_use]
    pub fn refill_per_sec(&self) -> u64 {
        self.refill_per_sec
    }

    /// Time at which `requested` tokens will be available. `None` if
    /// already available OR `refill_per_sec` == 0 (never).
    #[must_use]
    pub fn time_to_refill(&self, requested: u64) -> Option<Instant> {
        let curr = self.current.load(Ordering::Acquire);
        if curr >= requested {
            return None;
        }
        if self.refill_per_sec == 0 {
            return None;
        }
        let needed = requested - curr;
        // Conservative ceiling: ms = (needed * 1000 + refill_per_sec - 1) / refill_per_sec
        let ms = needed
            .saturating_mul(1000)
            .saturating_add(self.refill_per_sec.saturating_sub(1))
            / self.refill_per_sec;
        let last = self.last_refill_ms.load(Ordering::Acquire);
        Some(Instant::from_ms(last.saturating_add(ms)))
    }

    /// Snapshot for mirante publish.
    #[must_use]
    pub fn snapshot(&self) -> BudgetSnapshot {
        let available = self.current.load(Ordering::Acquire);
        BudgetSnapshot {
            available,
            capacity: self.capacity,
            refill_per_sec: self.refill_per_sec,
            last_refill_packed: Instant::from_ms(self.last_refill_ms.load(Ordering::Acquire))
                .to_packed(),
        }
    }

    /// Lazy refill: compute elapsed since `last_refill_ms` × rate, add
    /// to current (clamped to capacity), update `last_refill_ms`.
    fn refill(&self) {
        if self.refill_per_sec == 0 {
            return;
        }
        let now_ms = self.clock.now().physical_ms;
        let last = self.last_refill_ms.load(Ordering::Acquire);
        if now_ms <= last {
            return;
        }
        let elapsed_ms = now_ms - last;
        let new_tokens = (elapsed_ms.saturating_mul(self.refill_per_sec)) / 1000;
        if new_tokens == 0 {
            return;
        }
        // CAS update current = min(current + new_tokens, capacity).
        loop {
            let curr = self.current.load(Ordering::Acquire);
            let refilled = curr.saturating_add(new_tokens).min(self.capacity);
            if self
                .current
                .compare_exchange(curr, refilled, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                // Advance last_refill_ms past the credit we just added.
                let consumed_ms = new_tokens.saturating_mul(1000) / self.refill_per_sec.max(1);
                self.last_refill_ms
                    .store(last.saturating_add(consumed_ms), Ordering::Release);
                return;
            }
        }
    }
}

impl Named for Budget {
    fn name(&self) -> &'static str {
        self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relogio::FrozenClock;

    fn budget(cap: u64, rate: u64, t0: u64) -> (Budget, Arc<FrozenClock>) {
        let clock = Arc::new(FrozenClock::at(t0));
        let b = Budget::new("test", cap, rate, clock.clone());
        (b, clock)
    }

    #[test]
    fn new_budget_starts_full() {
        let (b, _) = budget(100, 10, 0);
        assert_eq!(b.available(), 100);
        assert_eq!(b.capacity(), 100);
        assert_eq!(b.refill_per_sec(), 10);
        assert_eq!(b.name(), "test");
    }

    #[test]
    fn try_consume_succeeds_when_enough() {
        let (b, _) = budget(100, 0, 0);
        let remaining = b.try_consume(40).unwrap();
        assert_eq!(remaining, 60);
        assert_eq!(b.available(), 60);
    }

    #[test]
    fn try_consume_fails_when_exhausted() {
        let (b, _) = budget(50, 0, 0);
        b.try_consume(50).unwrap();
        let err = b.try_consume(1).unwrap_err();
        match err {
            BudgetError::Exhausted {
                available,
                requested,
                ..
            } => {
                assert_eq!(available, 0);
                assert_eq!(requested, 1);
            }
            _ => panic!("expected Exhausted, got {err:?}"),
        }
    }

    #[test]
    fn try_consume_over_capacity_errors() {
        let (b, _) = budget(100, 0, 0);
        let err = b.try_consume(200).unwrap_err();
        assert_eq!(err.kind(), "over_capacity");
    }

    #[test]
    fn refill_adds_tokens_over_time() {
        let (b, clock) = budget(100, 10, 0); // 10 tokens/sec
        b.try_consume(100).unwrap();
        assert_eq!(b.available(), 0);
        clock.advance(2000); // 2 sec → 20 tokens
        assert_eq!(b.available(), 20);
    }

    #[test]
    fn refill_clamps_to_capacity() {
        let (b, clock) = budget(100, 100, 0); // 100 tokens/sec
        b.try_consume(10).unwrap();
        assert_eq!(b.available(), 90);
        clock.advance(10_000); // 10 sec → would add 1000, clamp to 100
        assert_eq!(b.available(), 100);
    }

    #[test]
    fn zero_refill_rate_never_refills() {
        let (b, clock) = budget(100, 0, 0);
        b.try_consume(50).unwrap();
        clock.advance(100_000); // 100 sec
        assert_eq!(b.available(), 50);
    }

    #[test]
    fn time_to_refill_returns_some_when_short() {
        let (b, _) = budget(100, 10, 0);
        b.try_consume(100).unwrap();
        let replenish = b.time_to_refill(5);
        assert!(replenish.is_some());
        // 5 tokens at 10/sec = 500ms.
        assert_eq!(replenish.unwrap().physical_ms, 500);
    }

    #[test]
    fn time_to_refill_returns_none_when_available() {
        let (b, _) = budget(100, 10, 0);
        let replenish = b.time_to_refill(50);
        assert!(replenish.is_none());
    }

    #[test]
    fn time_to_refill_returns_none_when_rate_zero() {
        let (b, _) = budget(100, 0, 0);
        b.try_consume(100).unwrap();
        let replenish = b.time_to_refill(1);
        assert!(replenish.is_none());
    }

    #[test]
    fn exhausted_error_includes_replenish_at() {
        let (b, _) = budget(100, 10, 1000);
        b.try_consume(100).unwrap();
        let err = b.try_consume(5).unwrap_err();
        match err {
            BudgetError::Exhausted { replenish_at, .. } => {
                assert!(replenish_at.is_some());
                assert!(replenish_at.unwrap().physical_ms >= 1500);
            }
            _ => panic!("expected Exhausted"),
        }
    }

    #[test]
    fn snapshot_reflects_current_state() {
        let (b, _) = budget(100, 5, 1000);
        b.try_consume(30).unwrap();
        let snap = b.snapshot();
        assert_eq!(snap.available, 70);
        assert_eq!(snap.capacity, 100);
        assert_eq!(snap.refill_per_sec, 5);
    }

    #[test]
    fn snapshot_serializes_to_json() {
        let (b, _) = budget(100, 5, 0);
        let snap = b.snapshot();
        let json = serde_json::to_string(&snap).unwrap();
        assert!(json.contains("\"available\":100"));
        assert!(json.contains("\"capacity\":100"));
    }

    #[test]
    fn consume_after_refill_works() {
        let (b, clock) = budget(10, 10, 0);
        b.try_consume(10).unwrap();
        assert_eq!(b.available(), 0);
        clock.advance(1000); // refill to full
        let remaining = b.try_consume(5).unwrap();
        assert_eq!(remaining, 5);
    }

    #[test]
    fn error_kinds_stable() {
        assert_eq!(
            BudgetError::Exhausted {
                available: 0,
                requested: 1,
                replenish_at: None
            }
            .kind(),
            "exhausted"
        );
        assert_eq!(
            BudgetError::OverCapacity {
                requested: 100,
                capacity: 50,
            }
            .kind(),
            "over_capacity"
        );
    }

    #[test]
    #[should_panic(expected = "Budget capacity must be > 0")]
    fn zero_capacity_panics() {
        let _ = budget(0, 10, 0);
    }
}
