//! provação — typed deterministic failure injection.
//!
//! Per the research brief — sixth inventive primitive. Chaos
//! engineering minus the chaos: faults are typed values from the
//! operator's error enum, injection policies are typed values too,
//! and the whole thing is fully replayable given a u64 seed.
//!
//! Two axes of typing:
//!   - Faults: parameterized by `F: ErrorKind + Clone` — the
//!     operator's own error variant slots in directly. No erasure.
//!   - Policies: three typed strategies (`Probability` / `EveryNth` /
//!     `TimeWindow`); composable in a `Vec` — every call walks the list
//!     and returns the first injected fault.
//!
//! ## Determinism contract
//!
//!   - Same seed + same `Policy` sequence + same call sequence → same
//!     fault sequence, byte-identical, across runs, across machines.
//!   - `SplitMix64` inline (no external `rand` dep, no syscall).
//!   - `TimeWindow` checks the operator-supplied `Clock` — typically a
//!     `FrozenClock` under test so even time-based faults are
//!     replayable.
//!
//! ## Composition
//!
//!   - `Named` for telemetry attribution
//!   - `ErrorKind` bound on F so callers can `?` straight through
//!   - `relógio::Clock` for `TimeWindow`
//!   - `máquina` pairing: wrap `MachineRunner::step` in `maybe_fault` to
//!     get deterministic chaos on FSM transitions

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error_kind::ErrorKind;
use crate::relogio::Clock;

/// One injection policy. Operators compose many via `with_policy`.
pub enum Policy<F: ErrorKind + Clone> {
    /// Inject `fault` with probability `p ∈ [0.0, 1.0]`. Each
    /// `maybe_fault` call rolls a fresh u64 from the PRNG; if
    /// `roll / u64::MAX < p`, the fault fires.
    Probability {
        /// The fault to inject.
        fault: F,
        /// Probability of injection per call (0.0 = never, 1.0 = always).
        p: f64,
    },
    /// Inject `fault` every `n`th call (counter-based, deterministic).
    /// First call fires on the `n`th invocation, then every `n` after.
    EveryNth {
        /// The fault to inject.
        fault: F,
        /// Fire on call `n`, `2n`, `3n`, …
        n: u64,
    },
    /// Inject `fault` during the time window `[start_ms, end_ms)` as
    /// observed by a Clock. Outside the window: no fault from this
    /// policy.
    TimeWindow {
        /// The fault to inject.
        fault: F,
        /// Start of the window (inclusive), `physical_ms` from Clock.
        start_ms: u64,
        /// End of the window (exclusive), `physical_ms` from Clock.
        end_ms: u64,
    },
}

/// Per-policy mutable state (counter + scratch). Indexed parallel
/// to the policy Vec.
#[derive(Default)]
struct PolicyState {
    /// Counter for `EveryNth`.
    counter: AtomicU64,
}

/// Typed deterministic fault injector. One per logical "operation
/// site" the operator wants to perturb.
pub struct Provacao<F: ErrorKind + Clone> {
    name: &'static str,
    rng_state: Mutex<u64>,
    policies: Vec<Policy<F>>,
    states: Vec<PolicyState>,
}

impl<F: ErrorKind + Clone> Provacao<F> {
    /// New injector with the given name and seed.
    #[must_use]
    pub fn new(name: &'static str, seed: u64) -> Self {
        Self {
            name,
            rng_state: Mutex::new(seed),
            policies: Vec::new(),
            states: Vec::new(),
        }
    }

    /// Attach a policy. Multiple calls compose; later policies are
    /// consulted only if earlier ones do not fire.
    #[must_use]
    pub fn with_policy(mut self, policy: Policy<F>) -> Self {
        self.policies.push(policy);
        self.states.push(PolicyState::default());
        self
    }

    /// Number of attached policies.
    #[must_use]
    pub fn policy_count(&self) -> usize {
        self.policies.len()
    }

    /// Sample. Returns the first fired fault, or `None` if no policy
    /// triggered on this call. Requires a Clock for time-windowed
    /// policies; pass `&FrozenClock::at(0)` if you don't care.
    pub fn maybe_fault(&self, clock: &dyn Clock) -> Option<F> {
        for (policy, state) in self.policies.iter().zip(self.states.iter()) {
            match policy {
                Policy::Probability { fault, p } => {
                    if self.roll_probability(*p) {
                        return Some(fault.clone());
                    }
                }
                Policy::EveryNth { fault, n } => {
                    let n = (*n).max(1);
                    let prev = state.counter.fetch_add(1, Ordering::AcqRel);
                    let next = prev + 1;
                    if next % n == 0 {
                        return Some(fault.clone());
                    }
                }
                Policy::TimeWindow {
                    fault,
                    start_ms,
                    end_ms,
                } => {
                    let now_ms = clock.now().physical_ms;
                    if now_ms >= *start_ms && now_ms < *end_ms {
                        return Some(fault.clone());
                    }
                }
            }
        }
        None
    }

    /// Reset every per-policy counter to 0. Does NOT reset the RNG
    /// (which is naturally streaming). Use `set_seed` to reset RNG.
    pub fn reset_counters(&self) {
        for s in &self.states {
            s.counter.store(0, Ordering::Release);
        }
    }

    /// Replace the RNG seed (mid-run reseed). Combined with
    /// `reset_counters()` this restores the injector to a known
    /// initial state.
    pub fn set_seed(&self, seed: u64) {
        if let Ok(mut guard) = self.rng_state.lock() {
            *guard = seed;
        }
    }

    /// Roll a uniform u64 from the `SplitMix64` stream, normalize to
    /// [0.0, 1.0), compare to p. Returns true if a uniform sample
    /// falls below p.
    fn roll_probability(&self, p: f64) -> bool {
        if p <= 0.0 {
            return false;
        }
        if p >= 1.0 {
            return true;
        }
        let roll = self.next_u64();
        // f64 has 52 bits of mantissa; keep top 53 bits for full prec.
        #[allow(clippy::cast_precision_loss)]
        let normalized = (roll >> 11) as f64 / ((1u64 << 53) as f64);
        normalized < p
    }

    fn next_u64(&self) -> u64 {
        let mut guard = self
            .rng_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard = guard.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *guard;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

crate::impl_named_field_generic!(Provacao, F: ErrorKind + Clone);

/// Provacao impl Observable (v1.00 SSC) — exposes
/// `ChildCountSnapshot { name, child_count: policy_count }`. Joins
/// TieredCache + CompositeShapeRenderer + ChainedVerifier + Plantio
/// as the 5th ChildCountSnapshot consumer. The Send + Sync + 'static
/// supertraits on `F` come from `Observable: Named + Send + Sync`
/// (pattern #11 check #6, codified v0.99).
impl<F: ErrorKind + Clone + Send + Sync + 'static> crate::mirante::Observable for Provacao<F> {
    type Snapshot = crate::mirante::ChildCountSnapshot;

    fn snapshot(&self) -> Self::Snapshot {
        crate::mirante::ChildCountSnapshot {
            name: <Self as crate::named::Named>::name(self),
            child_count: self.policy_count(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::named::Named;
    use crate::relogio::FrozenClock;
    use std::sync::Arc;
    use thiserror::Error;

    #[derive(Debug, Clone, PartialEq, Eq, Error)]
    enum TestFault {
        #[error("timeout")]
        Timeout,
        #[error("denied")]
        Denied,
        #[error("corrupted")]
        Corrupted,
    }

    crate::impl_error_kind! {
        TestFault {
            Timeout => "timeout",
            Denied => "denied",
            Corrupted => "corrupted",
        }
    }

    fn clock(t: u64) -> Arc<FrozenClock> {
        Arc::new(FrozenClock::at(t))
    }

    #[test]
    fn empty_provacao_never_faults() {
        let p = Provacao::<TestFault>::new("test", 42);
        let c = clock(0);
        for _ in 0..100 {
            assert_eq!(p.maybe_fault(c.as_ref()), None);
        }
    }

    #[test]
    fn named_returns_name() {
        let p = Provacao::<TestFault>::new("chaos", 0);
        assert_eq!(p.name(), "chaos");
    }

    #[test]
    fn probability_zero_never_fires() {
        let p = Provacao::<TestFault>::new("test", 42).with_policy(Policy::Probability {
            fault: TestFault::Timeout,
            p: 0.0,
        });
        let c = clock(0);
        for _ in 0..100 {
            assert_eq!(p.maybe_fault(c.as_ref()), None);
        }
    }

    #[test]
    fn probability_one_always_fires() {
        let p = Provacao::<TestFault>::new("test", 42).with_policy(Policy::Probability {
            fault: TestFault::Timeout,
            p: 1.0,
        });
        let c = clock(0);
        for _ in 0..100 {
            assert_eq!(p.maybe_fault(c.as_ref()), Some(TestFault::Timeout));
        }
    }

    #[test]
    fn probability_is_deterministic_per_seed() {
        let p1 = Provacao::<TestFault>::new("test", 12345).with_policy(Policy::Probability {
            fault: TestFault::Timeout,
            p: 0.5,
        });
        let p2 = Provacao::<TestFault>::new("test", 12345).with_policy(Policy::Probability {
            fault: TestFault::Timeout,
            p: 0.5,
        });
        let c = clock(0);
        for _ in 0..50 {
            assert_eq!(p1.maybe_fault(c.as_ref()), p2.maybe_fault(c.as_ref()));
        }
    }

    #[test]
    fn every_nth_fires_at_multiples_of_n() {
        let p = Provacao::<TestFault>::new("test", 0).with_policy(Policy::EveryNth {
            fault: TestFault::Denied,
            n: 3,
        });
        let c = clock(0);
        let results: Vec<_> = (0..9).map(|_| p.maybe_fault(c.as_ref())).collect();
        assert_eq!(results[0], None);
        assert_eq!(results[1], None);
        assert_eq!(results[2], Some(TestFault::Denied)); // 3rd
        assert_eq!(results[3], None);
        assert_eq!(results[4], None);
        assert_eq!(results[5], Some(TestFault::Denied)); // 6th
        assert_eq!(results[8], Some(TestFault::Denied)); // 9th
    }

    #[test]
    fn every_nth_zero_treated_as_one() {
        // Defensive: n=0 would div-by-zero. We coerce to n=1 (every call).
        let p = Provacao::<TestFault>::new("test", 0).with_policy(Policy::EveryNth {
            fault: TestFault::Corrupted,
            n: 0,
        });
        let c = clock(0);
        assert_eq!(p.maybe_fault(c.as_ref()), Some(TestFault::Corrupted));
    }

    #[test]
    fn time_window_fires_only_inside_range() {
        let c = Arc::new(FrozenClock::at(100));
        let p = Provacao::<TestFault>::new("test", 0).with_policy(Policy::TimeWindow {
            fault: TestFault::Timeout,
            start_ms: 200,
            end_ms: 500,
        });
        assert_eq!(p.maybe_fault(c.as_ref()), None); // before
        c.advance(150); // now 250 (inside)
        assert_eq!(p.maybe_fault(c.as_ref()), Some(TestFault::Timeout));
        c.advance(300); // now 550 (after)
        assert_eq!(p.maybe_fault(c.as_ref()), None);
    }

    #[test]
    fn time_window_at_boundary_inclusive_start() {
        let c = Arc::new(FrozenClock::at(200));
        let p = Provacao::<TestFault>::new("test", 0).with_policy(Policy::TimeWindow {
            fault: TestFault::Timeout,
            start_ms: 200,
            end_ms: 300,
        });
        assert_eq!(p.maybe_fault(c.as_ref()), Some(TestFault::Timeout));
    }

    #[test]
    fn time_window_at_boundary_exclusive_end() {
        let c = Arc::new(FrozenClock::at(300));
        let p = Provacao::<TestFault>::new("test", 0).with_policy(Policy::TimeWindow {
            fault: TestFault::Timeout,
            start_ms: 200,
            end_ms: 300,
        });
        assert_eq!(p.maybe_fault(c.as_ref()), None);
    }

    #[test]
    fn multiple_policies_first_fired_wins() {
        let p = Provacao::<TestFault>::new("test", 0)
            .with_policy(Policy::Probability {
                fault: TestFault::Timeout,
                p: 1.0, // always fires
            })
            .with_policy(Policy::Probability {
                fault: TestFault::Denied,
                p: 1.0, // would also always fire, but first wins
            });
        let c = clock(0);
        assert_eq!(p.maybe_fault(c.as_ref()), Some(TestFault::Timeout));
    }

    #[test]
    fn reset_counters_resets_every_nth() {
        let p = Provacao::<TestFault>::new("test", 0).with_policy(Policy::EveryNth {
            fault: TestFault::Denied,
            n: 2,
        });
        let c = clock(0);
        p.maybe_fault(c.as_ref()); // 1
        let fault1 = p.maybe_fault(c.as_ref()); // 2 → fires
        assert_eq!(fault1, Some(TestFault::Denied));
        p.reset_counters();
        let none = p.maybe_fault(c.as_ref()); // count=1 again
        assert_eq!(none, None);
        let fires = p.maybe_fault(c.as_ref()); // count=2 → fires
        assert_eq!(fires, Some(TestFault::Denied));
    }

    #[test]
    fn set_seed_reproduces_sequence() {
        let p = Provacao::<TestFault>::new("test", 123).with_policy(Policy::Probability {
            fault: TestFault::Timeout,
            p: 0.5,
        });
        let c = clock(0);
        let first: Vec<_> = (0..20).map(|_| p.maybe_fault(c.as_ref())).collect();
        p.set_seed(123);
        let second: Vec<_> = (0..20).map(|_| p.maybe_fault(c.as_ref())).collect();
        assert_eq!(first, second);
    }

    #[test]
    fn policy_count_reflects_with_policy() {
        let p = Provacao::<TestFault>::new("test", 0);
        assert_eq!(p.policy_count(), 0);
        let p = p.with_policy(Policy::Probability {
            fault: TestFault::Timeout,
            p: 0.5,
        });
        assert_eq!(p.policy_count(), 1);
        let p = p
            .with_policy(Policy::EveryNth {
                fault: TestFault::Denied,
                n: 3,
            })
            .with_policy(Policy::TimeWindow {
                fault: TestFault::Corrupted,
                start_ms: 0,
                end_ms: 100,
            });
        assert_eq!(p.policy_count(), 3);
    }
}
