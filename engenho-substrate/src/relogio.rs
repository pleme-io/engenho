//! relógio — typed deterministic clock + Hybrid Logical Clock (HLC).
//!
//! Per the research brief — the HIGHEST-leverage inventive
//! primitive. Every substrate site that currently calls
//! `std::time::SystemTime::now()` or accepts a raw `u64`
//! timestamp gains a typed `Clock` trait + `Instant` value.
//! The substrate becomes deterministic-at-the-type-level for
//! tests (via `FrozenClock`) AND causally-ordered cross-node
//! for federation (via `HlcClock` merge semantics).
//!
//! ## What ships
//!
//!   - `Instant` value (48-bit physical ms + 16-bit logical counter)
//!     with total ordering + causally_after + serde + Fingerprint
//!   - `Clock` trait — universal time surface, supertraits `Named`
//!   - `WallClock` — production: `SystemTime::now()` based
//!   - `FrozenClock` — tests: returns a fixed Instant; `advance()` mutates
//!   - `LogicalClock` — deterministic counter; no wall-time at all
//!   - `HlcClock` — HLC merge semantics for cross-node ordering
//!
//! ## Composition
//!
//!   - `Named` supertrait → every clock has telemetry name
//!   - `Fingerprint` over `Instant` → deterministic bytes
//!   - `Hex` over Instant.to_bytes() for log display
//!   - Future: `MaterializationReceipt.emitted_at: Instant` (next round)

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::named::Named;

/// Typed instant: 48-bit physical milliseconds since unix epoch +
/// 16-bit logical counter. Packs into 64 bits, totally ordered.
///
/// Constraints:
///   - `physical_ms < 2^48` (~8908 years from epoch — plenty)
///   - `logical < 2^16` (65k ticks per ms in worst-case bursts)
///
/// Tie-breaking: lexicographic on (physical_ms, logical).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Instant {
    /// Physical milliseconds since UNIX epoch.
    pub physical_ms: u64,
    /// Logical counter — increments when wall-clock doesn't advance.
    pub logical: u16,
}

impl Instant {
    /// Construct an Instant.
    #[must_use]
    pub fn new(physical_ms: u64, logical: u16) -> Self {
        Self {
            physical_ms,
            logical,
        }
    }

    /// The zero instant — useful for empty receipts + initial states.
    #[must_use]
    pub const fn zero() -> Self {
        Self {
            physical_ms: 0,
            logical: 0,
        }
    }

    /// Pack into 64 bits: 48-bit physical | 16-bit logical.
    /// `physical_ms` above 2^48 is silently saturated.
    #[must_use]
    pub fn to_packed(&self) -> u64 {
        let phys = self.physical_ms & ((1u64 << 48) - 1);
        (phys << 16) | u64::from(self.logical)
    }

    /// Unpack from 64-bit packed form.
    #[must_use]
    pub fn from_packed(packed: u64) -> Self {
        let logical = (packed & 0xFFFF) as u16;
        let physical_ms = packed >> 16;
        Self {
            physical_ms,
            logical,
        }
    }

    /// Canonical 8-byte representation (big-endian packed).
    #[must_use]
    pub fn to_bytes(&self) -> [u8; 8] {
        self.to_packed().to_be_bytes()
    }

    /// Total order: returns true if `self` is strictly after `other`
    /// in the (physical_ms, logical) lexicographic order.
    #[must_use]
    pub fn causally_after(&self, other: &Self) -> bool {
        (self.physical_ms, self.logical) > (other.physical_ms, other.logical)
    }

    /// Tick: produce the next Instant at-or-after `now` that is
    /// strictly greater than `previous`. Used by HLC merge.
    ///
    /// If `now > previous`, returns `now` (wall-clock advanced).
    /// If `now <= previous`, returns `previous + 1 logical`.
    #[must_use]
    pub fn tick(now: Self, previous: Self) -> Self {
        if now.causally_after(&previous) || now == previous {
            if now == previous {
                // Tie → bump logical counter on `now`.
                Self {
                    physical_ms: now.physical_ms,
                    logical: now.logical.saturating_add(1),
                }
            } else {
                now
            }
        } else {
            // Wall clock went backward (NTP correction, sleep) →
            // keep `previous.physical_ms` + bump logical.
            Self {
                physical_ms: previous.physical_ms,
                logical: previous.logical.saturating_add(1),
            }
        }
    }
}

impl PartialOrd for Instant {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Instant {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.physical_ms, self.logical).cmp(&(other.physical_ms, other.logical))
    }
}

impl std::fmt::Display for Instant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{:05}", self.physical_ms, self.logical)
    }
}

crate::impl_fingerprint!(Instant);

/// Universal clock surface. Implementers supply `now()`;
/// HLC-aware implementers additionally implement `tick(observed)`.
pub trait Clock: Named + Send + Sync {
    /// Current instant per this clock's notion of time.
    fn now(&self) -> Instant;

    /// HLC merge: given an observed Instant from elsewhere, return
    /// the next local Instant that is strictly after both `self.now()`
    /// AND `observed`. Default implementation is wall-clock-based;
    /// HLC-aware clocks override.
    fn tick(&self, observed: Instant) -> Instant {
        let now = self.now();
        Instant::tick(now.max(observed), observed)
    }
}

// =================================================================
// WallClock — production
// =================================================================

/// Production clock backed by `std::time::SystemTime::now()`.
/// Returns Instant with current ms-since-epoch + logical 0.
/// Use [`HlcClock`] for monotonicity within a process.
#[derive(Default, Clone, Copy)]
pub struct WallClock;

crate::define_named!(WallClock, "wall");

impl Clock for WallClock {
    fn now(&self) -> Instant {
        let physical_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Instant {
            physical_ms,
            logical: 0,
        }
    }
}

// =================================================================
// FrozenClock — tests
// =================================================================

/// Test clock — returns a fixed Instant; mutable via `advance()`.
/// Replaces the per-site `42`/`100` test timestamp pattern with
/// one typed primitive.
pub struct FrozenClock {
    physical_ms: AtomicU64,
    logical: AtomicU64, // u64 for atomic; cast to u16 on read
}

crate::define_named!(FrozenClock, "frozen");

impl FrozenClock {
    /// New clock pinned at `(physical_ms, 0)`.
    #[must_use]
    pub fn at(physical_ms: u64) -> Self {
        Self {
            physical_ms: AtomicU64::new(physical_ms),
            logical: AtomicU64::new(0),
        }
    }

    /// Advance physical wallclock by `ms`. Resets logical to 0.
    pub fn advance(&self, ms: u64) {
        self.physical_ms.fetch_add(ms, Ordering::SeqCst);
        self.logical.store(0, Ordering::SeqCst);
    }

    /// Bump the logical counter (simulates same-millisecond tick).
    pub fn tick_logical(&self) {
        self.logical.fetch_add(1, Ordering::SeqCst);
    }

    /// Reset to (0, 0).
    pub fn reset(&self) {
        self.physical_ms.store(0, Ordering::SeqCst);
        self.logical.store(0, Ordering::SeqCst);
    }
}

impl Default for FrozenClock {
    fn default() -> Self {
        Self::at(0)
    }
}

impl Clock for FrozenClock {
    fn now(&self) -> Instant {
        Instant {
            physical_ms: self.physical_ms.load(Ordering::SeqCst),
            logical: self.logical.load(Ordering::SeqCst) as u16,
        }
    }
}

// =================================================================
// LogicalClock — pure counter, no wall time
// =================================================================

/// Deterministic logical-only clock. Every `now()` call returns
/// (0, counter) with counter incrementing. Used when ordering
/// matters but wallclock is irrelevant (search trial ordering,
/// test fixture pinning).
#[derive(Default)]
pub struct LogicalClock {
    counter: AtomicU64,
}

crate::define_named!(LogicalClock, "logical");

impl LogicalClock {
    /// New clock starting at counter 0.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Current counter without advancing.
    pub fn peek(&self) -> u64 {
        self.counter.load(Ordering::SeqCst)
    }
}

impl Clock for LogicalClock {
    fn now(&self) -> Instant {
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        Instant {
            physical_ms: 0,
            logical: (n & 0xFFFF) as u16,
        }
    }
}

// =================================================================
// HlcClock — Hybrid Logical Clock with monotonicity guarantees
// =================================================================

/// HLC clock — wraps a wall-clock source + ensures monotonicity
/// across `now()` calls + correctly merges `observed` instants
/// from peer nodes.
pub struct HlcClock {
    wall: WallClock,
    last: std::sync::Mutex<Instant>,
}

crate::define_named!(HlcClock, "hlc");

impl Default for HlcClock {
    fn default() -> Self {
        Self::new()
    }
}

impl HlcClock {
    /// New HLC starting at the current wall time.
    #[must_use]
    pub fn new() -> Self {
        let wall = WallClock;
        let initial = wall.now();
        Self {
            wall,
            last: std::sync::Mutex::new(initial),
        }
    }
}

impl Clock for HlcClock {
    fn now(&self) -> Instant {
        let wall_now = self.wall.now();
        let mut last = self.last.lock().unwrap();
        *last = Instant::tick(wall_now, *last);
        *last
    }

    fn tick(&self, observed: Instant) -> Instant {
        let wall_now = self.wall.now();
        let mut last = self.last.lock().unwrap();
        // HLC merge: take max of (wall_now, observed, last) + bump logical
        let candidate = wall_now.max(observed).max(*last);
        *last = if candidate == *last
            || candidate == observed
            || candidate == wall_now
        {
            Instant {
                physical_ms: candidate.physical_ms,
                logical: candidate.logical.saturating_add(1),
            }
        } else {
            candidate
        };
        *last
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Instant: ordering, packing, ticking ────────────────────

    #[test]
    fn instant_total_order_lexicographic() {
        let a = Instant::new(100, 5);
        let b = Instant::new(100, 6);
        let c = Instant::new(101, 0);
        assert!(b > a);
        assert!(c > b);
        assert!(c > a);
    }

    #[test]
    fn instant_zero_is_minimal() {
        assert!(Instant::new(1, 0) > Instant::zero());
        assert!(Instant::new(0, 1) > Instant::zero());
    }

    #[test]
    fn instant_pack_unpack_round_trip() {
        for i in [
            Instant::zero(),
            Instant::new(1, 0),
            Instant::new(0, 1),
            Instant::new(1000, 5),
            Instant::new(u64::MAX >> 16, u16::MAX),
        ] {
            assert_eq!(Instant::from_packed(i.to_packed()), i);
        }
    }

    #[test]
    fn instant_to_bytes_is_eight() {
        let i = Instant::new(100, 5);
        assert_eq!(i.to_bytes().len(), 8);
    }

    #[test]
    fn instant_causally_after_strict() {
        let a = Instant::new(10, 0);
        let b = Instant::new(10, 0);
        assert!(!a.causally_after(&b));
        assert!(!b.causally_after(&a));
    }

    #[test]
    fn instant_tick_advances_wall_clock_when_strictly_after() {
        let prev = Instant::new(100, 5);
        let now = Instant::new(200, 0);
        let next = Instant::tick(now, prev);
        assert_eq!(next, now);
    }

    #[test]
    fn instant_tick_bumps_logical_on_tie() {
        let prev = Instant::new(100, 5);
        let now = Instant::new(100, 5);
        let next = Instant::tick(now, prev);
        assert_eq!(next, Instant::new(100, 6));
    }

    #[test]
    fn instant_tick_bumps_logical_when_wall_goes_backward() {
        let prev = Instant::new(200, 3);
        let now = Instant::new(100, 0); // wall regressed
        let next = Instant::tick(now, prev);
        assert_eq!(next.physical_ms, 200);
        assert_eq!(next.logical, 4);
    }

    #[test]
    fn instant_display_renders_dot_separated() {
        let i = Instant::new(1234, 42);
        assert_eq!(format!("{i}"), "1234.00042");
    }

    #[test]
    fn instant_serde_round_trips() {
        let i = Instant::new(1234, 56);
        let bytes = serde_json::to_vec(&i).unwrap();
        let back: Instant = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, i);
    }

    #[test]
    fn instant_fingerprint_deterministic() {
        use crate::Fingerprint;
        let i = Instant::new(1, 2);
        assert_eq!(i.fingerprint(), i.fingerprint());
    }

    // ── WallClock ──────────────────────────────────────────────

    #[test]
    fn wallclock_now_returns_nonzero_recent_time() {
        let w = WallClock;
        let now = w.now();
        // ~1.7e12 ms since epoch = ~2023+. Sanity, not exact.
        assert!(now.physical_ms > 1_500_000_000_000);
        assert_eq!(now.logical, 0);
    }

    #[test]
    fn wallclock_named() {
        assert_eq!(WallClock.name(), "wall");
    }

    // ── FrozenClock ───────────────────────────────────────────

    #[test]
    fn frozen_clock_returns_fixed_instant() {
        let c = FrozenClock::at(1000);
        assert_eq!(c.now(), Instant::new(1000, 0));
        // Multiple calls stay frozen.
        assert_eq!(c.now(), c.now());
    }

    #[test]
    fn frozen_clock_advance_mutates_physical() {
        let c = FrozenClock::at(100);
        c.advance(50);
        assert_eq!(c.now(), Instant::new(150, 0));
    }

    #[test]
    fn frozen_clock_tick_logical_bumps_counter() {
        let c = FrozenClock::at(100);
        c.tick_logical();
        c.tick_logical();
        assert_eq!(c.now(), Instant::new(100, 2));
    }

    #[test]
    fn frozen_clock_reset_zeroes_state() {
        let c = FrozenClock::at(100);
        c.advance(50);
        c.tick_logical();
        c.reset();
        assert_eq!(c.now(), Instant::zero());
    }

    #[test]
    fn frozen_clock_named() {
        assert_eq!(FrozenClock::at(0).name(), "frozen");
    }

    // ── LogicalClock ──────────────────────────────────────────

    #[test]
    fn logical_clock_advances_on_now() {
        let c = LogicalClock::new();
        let a = c.now();
        let b = c.now();
        assert!(b > a);
    }

    #[test]
    fn logical_clock_peek_doesnt_advance() {
        let c = LogicalClock::new();
        let _ = c.now(); // counter = 1
        let p1 = c.peek();
        let p2 = c.peek();
        assert_eq!(p1, p2);
    }

    #[test]
    fn logical_clock_named() {
        assert_eq!(LogicalClock::new().name(), "logical");
    }

    // ── HlcClock ──────────────────────────────────────────────

    #[test]
    fn hlc_clock_now_is_monotonic() {
        let c = HlcClock::new();
        let a = c.now();
        let b = c.now();
        // Each call advances logical at minimum.
        assert!(b > a);
    }

    #[test]
    fn hlc_clock_tick_merges_with_observed() {
        let c = HlcClock::new();
        let observed = Instant::new(u64::MAX >> 16, 0); // far future
        let merged = c.tick(observed);
        // Local clock catches up to observed + bumps.
        assert!(merged > observed);
    }

    #[test]
    fn hlc_clock_named() {
        assert_eq!(HlcClock::new().name(), "hlc");
    }

    // ── Polymorphic dispatch ──────────────────────────────────

    #[test]
    fn clock_trait_object_dispatch() {
        let clocks: Vec<Box<dyn Clock>> = vec![
            Box::new(WallClock),
            Box::new(FrozenClock::at(100)),
            Box::new(LogicalClock::new()),
            Box::new(HlcClock::new()),
        ];
        for c in &clocks {
            let _ = c.now();
            let _ = c.name();
        }
    }
}
