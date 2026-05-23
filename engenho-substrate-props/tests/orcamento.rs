//! Property: orçamento Budget invariants.

use engenho_substrate::{Budget, BudgetError, FrozenClock};
use proptest::prelude::*;
use std::sync::Arc;

fn budget(cap: u64, rate: u64) -> (Budget, Arc<FrozenClock>) {
    let clock = Arc::new(FrozenClock::at(0));
    let b = Budget::new("test", cap, rate, clock.clone());
    (b, clock)
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(256),
        ..ProptestConfig::default()
    })]

    /// Fresh budget always starts at capacity.
    #[test]
    fn fresh_budget_starts_at_capacity(cap in 1u64..1_000_000, rate in 0u64..10_000) {
        let (b, _) = budget(cap, rate);
        prop_assert_eq!(b.available(), cap);
        prop_assert_eq!(b.capacity(), cap);
    }

    /// try_consume(n) where n ≤ capacity always reduces by exactly n
    /// when available.
    #[test]
    fn consume_reduces_exactly_when_available(
        cap in 100u64..1000,
        n in 1u64..100,
    ) {
        let (b, _) = budget(cap, 0);
        let before = b.available();
        let after = b.try_consume(n).unwrap();
        prop_assert_eq!(after, before - n);
    }

    /// try_consume(n) where n > capacity always errors with OverCapacity.
    #[test]
    fn over_capacity_always_errors(
        cap in 1u64..100,
        excess in 1u64..1000,
    ) {
        let (b, _) = budget(cap, 0);
        let err = b.try_consume(cap + excess).unwrap_err();
        prop_assert_eq!(err.to_string(), format!("over-capacity: requested={}, capacity={cap}", cap + excess));
    }

    /// Refill rate 0 ⇒ no tokens ever return.
    #[test]
    fn zero_rate_means_no_refill(
        cap in 10u64..1000,
        time_ms in 1u64..1_000_000,
    ) {
        let (b, clock) = budget(cap, 0);
        b.try_consume(cap).unwrap();
        clock.advance(time_ms);
        prop_assert_eq!(b.available(), 0);
    }

    /// Available is bounded by [0, capacity] always.
    #[test]
    fn available_is_always_bounded(
        cap in 1u64..1000,
        rate in 1u64..1000,
        time_ms in 0u64..1_000_000,
        consumes in proptest::collection::vec(1u64..10, 0..10),
    ) {
        let (b, clock) = budget(cap, rate);
        for n in &consumes {
            let _ = b.try_consume(*n);
        }
        clock.advance(time_ms);
        let avail = b.available();
        prop_assert!(avail <= cap, "available {avail} > capacity {cap}");
    }

    /// After full refill window, available reaches capacity (when rate > 0).
    #[test]
    fn full_refill_window_restores_capacity(
        cap in 10u64..1000,
        rate in 1u64..100,
    ) {
        let (b, clock) = budget(cap, rate);
        b.try_consume(cap).unwrap();
        // Need cap/rate seconds to refill, give 2x safety margin.
        let refill_ms = (cap * 1000 / rate) * 2;
        clock.advance(refill_ms);
        prop_assert_eq!(b.available(), cap);
    }

    /// time_to_refill returns None when enough already available.
    #[test]
    fn time_to_refill_none_when_available(
        cap in 10u64..1000,
        n in 1u64..10,
    ) {
        let (b, _) = budget(cap, 100);
        prop_assert!(b.time_to_refill(n).is_none());
    }

    /// time_to_refill returns None when rate is zero (will never refill).
    #[test]
    fn time_to_refill_none_when_zero_rate(cap in 10u64..1000) {
        let (b, _) = budget(cap, 0);
        b.try_consume(cap).unwrap();
        prop_assert!(b.time_to_refill(1).is_none());
    }

    /// Exhausted error carries available + requested fields correctly.
    /// Constrain requested ≤ cap so we don't trip OverCapacity.
    #[test]
    fn exhausted_error_fields_correct(
        cap in 10u64..100,
        ratio in 1u64..10,
    ) {
        // Pick requested as cap/ratio (always ≤ cap).
        let requested = (cap / ratio).max(1);
        let (b, _) = budget(cap, 0);
        b.try_consume(cap).unwrap();
        match b.try_consume(requested) {
            Err(BudgetError::Exhausted {
                available,
                requested: req,
                ..
            }) => {
                prop_assert_eq!(available, 0);
                prop_assert_eq!(req, requested);
            }
            other => prop_assert!(false, "expected Exhausted, got {other:?}"),
        }
    }

    /// Snapshot.available always matches available() reading.
    #[test]
    fn snapshot_matches_available(
        cap in 10u64..1000,
        rate in 0u64..100,
        consume in 0u64..10,
    ) {
        let (b, _) = budget(cap, rate);
        if consume > 0 && consume <= cap {
            let _ = b.try_consume(consume);
        }
        let snap = b.snapshot();
        let direct = b.available();
        // Allow small drift since both internally trigger refill; under
        // a FrozenClock the values must equal exactly.
        prop_assert_eq!(snap.available, direct);
    }
}
