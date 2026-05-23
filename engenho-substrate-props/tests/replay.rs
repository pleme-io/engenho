//! Property: replay ReplayCursor invariants.

use engenho_substrate::ReplayCursor;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig {
        cases: std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(256),
        ..ProptestConfig::default()
    })]

    /// New cursor has position 0, len matches input, remaining == len.
    #[test]
    fn new_cursor_invariants(events in proptest::collection::vec(any::<u32>(), 0..50)) {
        let c = ReplayCursor::new("t", events.clone());
        prop_assert_eq!(c.position(), 0);
        prop_assert_eq!(c.len(), events.len());
        prop_assert_eq!(c.remaining(), events.len());
        prop_assert_eq!(c.is_empty(), events.is_empty());
        if events.is_empty() {
            prop_assert!(c.is_done());
        } else {
            prop_assert!(!c.is_done());
        }
    }

    /// next() through every event returns them in order, then None.
    #[test]
    fn next_returns_events_in_order(events in proptest::collection::vec(any::<u32>(), 1..50)) {
        let c = ReplayCursor::new("t", events.clone());
        for (i, expected) in events.iter().enumerate() {
            let got = c.next().unwrap();
            prop_assert_eq!(&got, expected);
            prop_assert_eq!(c.position(), i + 1);
        }
        prop_assert!(c.next().is_none());
        prop_assert!(c.is_done());
    }

    /// peek() returns the same value next() would, without advancing.
    #[test]
    fn peek_matches_next_without_advancing(events in proptest::collection::vec(any::<u32>(), 1..30)) {
        let c = ReplayCursor::new("t", events.clone());
        for _ in 0..events.len() {
            let peeked = c.peek();
            let pos_before = c.position();
            let consumed = c.next();
            prop_assert_eq!(peeked, consumed);
            prop_assert_eq!(c.position(), pos_before + 1);
        }
    }

    /// position is always bounded by [0, len].
    #[test]
    fn position_always_bounded(
        events in proptest::collection::vec(any::<u32>(), 0..30),
        ops in proptest::collection::vec(0u8..3, 0..50),
        skip_amt in 0usize..10,
        rewind_amt in 0usize..10,
    ) {
        let c = ReplayCursor::new("t", events.clone());
        for op in ops {
            match op {
                0 => { let _ = c.next(); }
                1 => { c.skip(skip_amt); }
                _ => { c.rewind(rewind_amt); }
            }
            let p = c.position();
            prop_assert!(p <= events.len(), "position {p} exceeds len {}", events.len());
        }
    }

    /// reset() always returns position to 0.
    #[test]
    fn reset_returns_to_zero(events in proptest::collection::vec(any::<u32>(), 0..30)) {
        let c = ReplayCursor::new("t", events);
        c.skip(10);
        c.reset();
        prop_assert_eq!(c.position(), 0);
    }

    /// skip(n) advances by min(n, remaining); reports actual count.
    #[test]
    fn skip_clamps_to_remaining(
        events in proptest::collection::vec(any::<u32>(), 1..30),
        n in 0usize..50,
    ) {
        let c = ReplayCursor::new("t", events.clone());
        let before = c.position();
        let actual = c.skip(n);
        let expected = n.min(events.len() - before);
        prop_assert_eq!(actual, expected);
        prop_assert_eq!(c.position(), before + actual);
    }

    /// rewind(n) decreases position by min(n, current); reports count.
    #[test]
    fn rewind_clamps_to_current_position(
        events in proptest::collection::vec(any::<u32>(), 1..30),
        skip_amt in 0usize..30,
        rewind_amt in 0usize..50,
    ) {
        let c = ReplayCursor::new("t", events);
        c.skip(skip_amt);
        let before = c.position();
        let actual = c.rewind(rewind_amt);
        let expected = rewind_amt.min(before);
        prop_assert_eq!(actual, expected);
        prop_assert_eq!(c.position(), before - actual);
    }

    /// remaining + position == len at all times.
    #[test]
    fn remaining_plus_position_equals_len(
        events in proptest::collection::vec(any::<u32>(), 0..30),
        ops in proptest::collection::vec(0u8..3, 0..30),
    ) {
        let c = ReplayCursor::new("t", events.clone());
        for op in ops {
            match op {
                0 => { let _ = c.next(); }
                1 => { c.skip(3); }
                _ => { c.rewind(2); }
            }
            prop_assert_eq!(c.position() + c.remaining(), c.len());
        }
    }
}
