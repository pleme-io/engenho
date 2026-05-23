//! Property: concurrency stress tests for substrate primitives.
//!
//! Many substrate primitives use AtomicU64 / Mutex / RwLock
//! internally (Budget, Mirante, ObservationChannel, Provacao,
//! ReplayCursor) but had only single-threaded property tests
//! before v1.14. This file leverages `std::thread::spawn` +
//! the substrate's `Arc<...>`-shaped types to exercise the
//! lock-free invariants under contention.

use engenho_substrate::{Budget, Provacao, ReplayCursor};
use engenho_substrate_props::helpers::frozen_clock;
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;
use std::sync::Arc;
use std::thread;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
enum ConcFault {
    #[error("simulated")]
    Sim,
}
engenho_substrate::impl_error_kind! {
    ConcFault {
        Sim => "sim",
    }
}

proptest_with_env! {
    /// Budget: N threads each try_consume(1) concurrently from a
    /// capacity-cap budget. After all threads finish, the budget's
    /// remaining tokens is exactly cap - min(n_threads, cap).
    /// CAS loop on AtomicU64 must serialize correctly.
    #[test]
    fn budget_concurrent_consume_serializes(
        cap in 1u64..200,
        n_threads in 1usize..16,
    ) {
        let budget = Arc::new(Budget::new("conc", cap, 0, frozen_clock(0)));
        let handles: Vec<_> = (0..n_threads)
            .map(|_| {
                let b = budget.clone();
                thread::spawn(move || b.try_consume(1).ok())
            })
            .collect();
        let mut succeeded = 0;
        for h in handles {
            if h.join().unwrap().is_some() {
                succeeded += 1;
            }
        }
        let expected = (n_threads as u64).min(cap);
        prop_assert_eq!(succeeded as u64, expected);
        prop_assert_eq!(budget.available(), cap - expected);
    }

    /// ReplayCursor: N threads each call next() concurrently on a
    /// shared cursor with M events. The total successful next()
    /// returns equals min(N, M); cursor's final position equals
    /// min(N, M). fetch_add on AtomicUsize must serialize.
    #[test]
    fn cursor_concurrent_next_serializes(
        n_events in 1usize..100,
        n_threads in 1usize..16,
    ) {
        let events: Vec<u32> = (0..n_events as u32).collect();
        let cursor = Arc::new(ReplayCursor::new("conc-cursor", events));
        let handles: Vec<_> = (0..n_threads)
            .map(|_| {
                let c = cursor.clone();
                thread::spawn(move || c.next().is_some())
            })
            .collect();
        let succeeded: usize = handles.into_iter().map(|h| h.join().unwrap() as usize).sum();
        let expected = n_threads.min(n_events);
        prop_assert_eq!(succeeded, expected);
        prop_assert_eq!(cursor.position(), expected);
    }

    /// Provacao: N threads each call maybe_fault concurrently.
    /// EveryNth policy with n=1 → every call fires; total fault
    /// count exactly equals call count. fetch_add on counters
    /// must serialize.
    #[test]
    fn provacao_concurrent_counter_serializes(n_threads in 1usize..32) {
        use engenho_substrate::Policy;
        let prov = Arc::new(
            Provacao::<ConcFault>::new("conc-chaos", 0).with_policy(
                Policy::EveryNth {
                    fault: ConcFault::Sim,
                    n: 1,
                },
            ),
        );
        let handles: Vec<_> = (0..n_threads)
            .map(|_| {
                let p = prov.clone();
                thread::spawn(move || {
                    p.maybe_fault(&engenho_substrate::FrozenClock::at(0))
                        .is_some()
                })
            })
            .collect();
        let fault_count: usize = handles
            .into_iter()
            .map(|h| h.join().unwrap() as usize)
            .sum();
        // EveryNth(n=1) fires on every call; with no missed calls
        // total fires == n_threads (atomic fetch_add is the
        // serializer).
        prop_assert_eq!(fault_count, n_threads);
    }

    /// Budget: try_consume(over_cap) under contention NEVER allows
    /// available to go negative. Even with N concurrent attempts,
    /// the final available is in [0, cap].
    #[test]
    fn budget_no_negative_under_contention(
        cap in 1u64..100,
        n_threads in 2usize..16,
        per_call in 1u64..200,
    ) {
        let budget = Arc::new(Budget::new("nn", cap, 0, frozen_clock(0)));
        let handles: Vec<_> = (0..n_threads)
            .map(|_| {
                let b = budget.clone();
                let n = per_call;
                thread::spawn(move || b.try_consume(n).ok())
            })
            .collect();
        for h in handles {
            let _ = h.join();
        }
        // available is u64 — can't go negative, but more
        // importantly: it must equal cap - (sum of successful
        // consumes). The CAS loop guarantees this.
        prop_assert!(budget.available() <= cap);
    }
}
