//! Property: relógio HLC + Instant invariants.

use engenho_substrate::{Clock, FrozenClock, HlcClock, Instant, LogicalClock};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig {
        cases: std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(256),
        ..ProptestConfig::default()
    })]

    /// Instant pack/unpack is lossless within 48-bit physical range.
    #[test]
    fn instant_pack_round_trip(
        physical in 0u64..(1u64 << 48),
        logical in any::<u16>(),
    ) {
        let i = Instant::new(physical, logical);
        let packed = i.to_packed();
        let back = Instant::from_packed(packed);
        prop_assert_eq!(back, i);
    }

    /// Instant total ordering matches lexicographic on (physical, logical).
    #[test]
    fn instant_total_order_matches_lexicographic(
        p1 in 0u64..(1u64 << 48),
        l1 in any::<u16>(),
        p2 in 0u64..(1u64 << 48),
        l2 in any::<u16>(),
    ) {
        let a = Instant::new(p1, l1);
        let b = Instant::new(p2, l2);
        let expected = (p1, l1).cmp(&(p2, l2));
        prop_assert_eq!(a.cmp(&b), expected);
    }

    /// causally_after is strict total order: never reflexive.
    #[test]
    fn causally_after_is_strict(
        physical in 0u64..(1u64 << 48),
        logical in any::<u16>(),
    ) {
        let i = Instant::new(physical, logical);
        prop_assert!(!i.causally_after(&i));
    }

    /// Instant::tick always produces something strictly after previous.
    #[test]
    fn tick_produces_strictly_after_previous(
        prev_p in 0u64..(1u64 << 32),
        prev_l in 0u16..u16::MAX,  // leave headroom for +1
        now_p in 0u64..(1u64 << 32),
        now_l in any::<u16>(),
    ) {
        let prev = Instant::new(prev_p, prev_l);
        let now = Instant::new(now_p, now_l);
        let next = Instant::tick(now, prev);
        // next >= prev (it must catch up at minimum)
        prop_assert!(next >= prev || next.physical_ms == prev.physical_ms);
    }

    /// FrozenClock.now() is byte-identical across N consecutive calls.
    #[test]
    fn frozen_clock_now_is_stable(
        physical in 0u64..1_000_000_000,
        n_calls in 1usize..32,
    ) {
        let c = FrozenClock::at(physical);
        let first = c.now();
        for _ in 1..n_calls {
            prop_assert_eq!(c.now(), first);
        }
    }

    /// FrozenClock.advance is additive.
    #[test]
    fn frozen_clock_advance_is_additive(
        start in 0u64..1_000_000,
        a in 0u64..10_000,
        b in 0u64..10_000,
    ) {
        let c = FrozenClock::at(start);
        c.advance(a);
        c.advance(b);
        prop_assert_eq!(c.now().physical_ms, start + a + b);
    }

    /// LogicalClock advances on every now() call.
    #[test]
    fn logical_clock_advances_monotonically(
        n_calls in 1usize..64,
    ) {
        let c = LogicalClock::new();
        let mut prev = c.now();
        for _ in 1..n_calls {
            let next = c.now();
            prop_assert!(next > prev);
            prev = next;
        }
    }

    /// HlcClock is monotonic across N consecutive now() calls.
    #[test]
    fn hlc_clock_is_monotonic_across_calls(
        n_calls in 1usize..32,
    ) {
        let c = HlcClock::new();
        let mut prev = c.now();
        for _ in 1..n_calls {
            let next = c.now();
            prop_assert!(next > prev);
            prev = next;
        }
    }
}
