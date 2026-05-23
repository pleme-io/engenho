//! Property: BackoffConfig delay invariants.

use engenho_substrate::BackoffConfig;
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;
use std::time::Duration;

proptest_with_env! {
    /// no_retry config has max_attempts == 1.
    #[test]
    fn no_retry_has_one_attempt(_seed in any::<u8>()) {
        assert_eq!(BackoffConfig::no_retry().max_attempts, 1);
    }

    /// Attempt 0 and 1 always have zero delay (the initial attempt is
    /// free; no_retry never gets to attempt 2).
    #[test]
    fn first_attempt_is_free(
        initial_ms in 1u64..1000,
        multiplier in 1.0_f64..10.0,
        max_ms in 100u64..10_000,
    ) {
        let cfg = BackoffConfig {
            max_attempts: 5,
            initial_delay: Duration::from_millis(initial_ms),
            multiplier,
            max_delay: Duration::from_millis(max_ms),
        };
        assert_eq!(cfg.delay_for(0), Duration::ZERO);
        assert_eq!(cfg.delay_for(1), Duration::ZERO);
    }

    /// Attempt 2 onward is bounded above by max_delay.
    #[test]
    fn delay_capped_at_max(
        initial_ms in 1u64..1000,
        multiplier in 1.0_f64..10.0,
        max_ms in 100u64..1000,
        attempt in 2u32..30,
    ) {
        let cfg = BackoffConfig {
            max_attempts: 100,
            initial_delay: Duration::from_millis(initial_ms),
            multiplier,
            max_delay: Duration::from_millis(max_ms),
        };
        let d = cfg.delay_for(attempt);
        // Allow a tiny FP epsilon (the cap is computed via f64 multiply).
        assert!(d <= Duration::from_millis(max_ms + 1),
            "attempt {attempt} delay {d:?} exceeded max {max_ms}ms");
    }

    /// With multiplier >= 1.0, delay is monotonically non-decreasing
    /// across attempts (until it hits the cap).
    #[test]
    fn delay_monotone_non_decreasing(
        initial_ms in 1u64..100,
        multiplier in 1.0_f64..3.0,
        max_ms in 10_000u64..1_000_000,
    ) {
        let cfg = BackoffConfig {
            max_attempts: 100,
            initial_delay: Duration::from_millis(initial_ms),
            multiplier,
            max_delay: Duration::from_millis(max_ms),
        };
        let d2 = cfg.delay_for(2);
        let d3 = cfg.delay_for(3);
        let d4 = cfg.delay_for(4);
        assert!(d3 >= d2, "delay[3]={d3:?} < delay[2]={d2:?}");
        assert!(d4 >= d3, "delay[4]={d4:?} < delay[3]={d3:?}");
    }

    /// Constant multiplier (1.0) → constant delay across attempts.
    #[test]
    fn unit_multiplier_constant_delay(initial_ms in 10u64..500) {
        let cfg = BackoffConfig {
            max_attempts: 100,
            initial_delay: Duration::from_millis(initial_ms),
            multiplier: 1.0,
            max_delay: Duration::from_secs(60), // far above any computed delay
        };
        let d2 = cfg.delay_for(2);
        let d3 = cfg.delay_for(3);
        let d10 = cfg.delay_for(10);
        assert_eq!(d2, d3);
        assert_eq!(d3, d10);
    }

    /// delay_for is a pure function — same args → same result.
    #[test]
    fn delay_for_is_pure(initial_ms in 1u64..1000, attempt in 0u32..10) {
        let cfg = BackoffConfig {
            max_attempts: 10,
            initial_delay: Duration::from_millis(initial_ms),
            multiplier: 2.0,
            max_delay: Duration::from_secs(60),
        };
        let d1 = cfg.delay_for(attempt);
        let d2 = cfg.delay_for(attempt);
        assert_eq!(d1, d2);
    }
}
