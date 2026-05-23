//! Property: provação Provacao injector invariants.

use engenho_substrate::{Clock, FrozenClock, Policy, Provacao, impl_error_kind};
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
enum Fault {
    #[error("timeout")]
    Timeout,
    #[error("denied")]
    Denied,
}

impl_error_kind! {
    Fault {
        Timeout => "timeout",
        Denied => "denied",
    }
}

fn clock(t: u64) -> Arc<dyn Clock> {
    Arc::new(FrozenClock::at(t))
}

proptest_with_env! {
    /// Empty provacao never produces a fault.
    #[test]
    fn empty_never_faults(seed in any::<u64>(), calls in 1usize..50) {
        let p = Provacao::<Fault>::new("test", seed);
        let c = clock(0);
        for _ in 0..calls {
            prop_assert!(p.maybe_fault(c.as_ref()).is_none());
        }
    }

    /// Probability p=0 NEVER fires (regardless of seed).
    #[test]
    fn probability_zero_never_fires(seed in any::<u64>(), calls in 1usize..50) {
        let p = Provacao::<Fault>::new("test", seed).with_policy(Policy::Probability {
            fault: Fault::Timeout,
            p: 0.0,
        });
        let c = clock(0);
        for _ in 0..calls {
            prop_assert!(p.maybe_fault(c.as_ref()).is_none());
        }
    }

    /// Probability p=1 ALWAYS fires (regardless of seed).
    #[test]
    fn probability_one_always_fires(seed in any::<u64>(), calls in 1usize..50) {
        let p = Provacao::<Fault>::new("test", seed).with_policy(Policy::Probability {
            fault: Fault::Timeout,
            p: 1.0,
        });
        let c = clock(0);
        for _ in 0..calls {
            prop_assert_eq!(p.maybe_fault(c.as_ref()), Some(Fault::Timeout));
        }
    }

    /// Same seed → same fault sequence (replayability).
    #[test]
    fn same_seed_same_sequence(
        seed in any::<u64>(),
        p in 0.0_f64..1.0,
        calls in 1usize..30,
    ) {
        let p1 = Provacao::<Fault>::new("a", seed).with_policy(Policy::Probability {
            fault: Fault::Timeout,
            p,
        });
        let p2 = Provacao::<Fault>::new("a", seed).with_policy(Policy::Probability {
            fault: Fault::Timeout,
            p,
        });
        let c = clock(0);
        for _ in 0..calls {
            prop_assert_eq!(p1.maybe_fault(c.as_ref()), p2.maybe_fault(c.as_ref()));
        }
    }

    /// EveryNth fires exactly `calls / n` times across `calls` invocations.
    #[test]
    fn every_nth_fires_count_matches(
        n in 1u64..10,
        calls in 1usize..50,
    ) {
        let p = Provacao::<Fault>::new("test", 0).with_policy(Policy::EveryNth {
            fault: Fault::Denied,
            n,
        });
        let c = clock(0);
        let fired = (0..calls)
            .filter(|_| p.maybe_fault(c.as_ref()).is_some())
            .count();
        // Calls 1..=calls; fires on multiples of n.
        let expected = calls as u64 / n;
        prop_assert_eq!(fired as u64, expected);
    }

    /// TimeWindow inside the window always fires; outside never.
    #[test]
    fn time_window_inside_fires_outside_doesnt(
        start in 100u64..1000,
        len in 100u64..1000,
        sample_offset in -500i64..1500,
    ) {
        let end = start + len;
        let policy_start = start;
        let policy_end = end;
        let sample_t = (start as i64 + sample_offset).max(0) as u64;
        let c = Arc::new(FrozenClock::at(sample_t));
        let p = Provacao::<Fault>::new("test", 0).with_policy(Policy::TimeWindow {
            fault: Fault::Timeout,
            start_ms: policy_start,
            end_ms: policy_end,
        });
        let got = p.maybe_fault(c.as_ref() as &dyn Clock);
        let want = if sample_t >= policy_start && sample_t < policy_end {
            Some(Fault::Timeout)
        } else {
            None
        };
        prop_assert_eq!(got, want);
    }

    /// reset_counters returns EveryNth to initial state.
    #[test]
    fn reset_counters_restores_every_nth(n in 1u64..10, pre_calls in 1usize..20) {
        let p = Provacao::<Fault>::new("test", 0).with_policy(Policy::EveryNth {
            fault: Fault::Denied,
            n,
        });
        let c = clock(0);
        for _ in 0..pre_calls {
            let _ = p.maybe_fault(c.as_ref());
        }
        p.reset_counters();
        // First call after reset should fire iff n == 1.
        let first = p.maybe_fault(c.as_ref());
        if n == 1 {
            prop_assert_eq!(first, Some(Fault::Denied));
        } else {
            prop_assert_eq!(first, None);
        }
    }

    /// First-policy-wins: when policy[0] fires p=1, policy[1] is never consulted.
    #[test]
    fn first_policy_wins(seed in any::<u64>()) {
        let p = Provacao::<Fault>::new("test", seed)
            .with_policy(Policy::Probability {
                fault: Fault::Timeout,
                p: 1.0,
            })
            .with_policy(Policy::Probability {
                fault: Fault::Denied,
                p: 1.0,
            });
        let c = clock(0);
        for _ in 0..20 {
            prop_assert_eq!(p.maybe_fault(c.as_ref()), Some(Fault::Timeout));
        }
    }

    /// Different seeds → eventually divergent sequences (statistical, but
    /// with 50 calls at p=0.5 it's astronomically unlikely to match
    /// every time by chance unless seeds collide).
    #[test]
    fn different_seeds_diverge_with_high_probability(
        seed1 in 0u64..1_000_000,
        seed2 in 1_000_000u64..2_000_000,
    ) {
        let p1 = Provacao::<Fault>::new("a", seed1).with_policy(Policy::Probability {
            fault: Fault::Timeout,
            p: 0.5,
        });
        let p2 = Provacao::<Fault>::new("a", seed2).with_policy(Policy::Probability {
            fault: Fault::Timeout,
            p: 0.5,
        });
        let c = clock(0);
        let seq1: Vec<_> = (0..50).map(|_| p1.maybe_fault(c.as_ref())).collect();
        let seq2: Vec<_> = (0..50).map(|_| p2.maybe_fault(c.as_ref())).collect();
        // Probability of full match by chance is 2^-50 ≈ 10^-15.
        prop_assert!(seq1 != seq2);
    }
}
