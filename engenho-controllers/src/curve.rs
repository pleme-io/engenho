//! `Curve` — the one shape every retry delay in the controllers takes: a
//! floor that doubles per step up to a ceiling.
//!
//! ★ WHY ONE TYPE. Before this, each loop grew its own delay by hand: the
//! watch driver's re-subscribe had a private `grow`, a Transient reconcile
//! error retried at a FLAT 1 s forever, and the kubelet's crash backoff is
//! a third hand-written doubling. A flat retry is the defect the flat one
//! shipped: a controller whose store was down retried every second for as
//! long as the outage lasted, each attempt a log line and a store round-trip.
//! Upstream's workqueue rate limiter grows the delay on every consecutive
//! failure and forgets on success; [`Streak`] is that, and [`Curve`] is the
//! shape it reads.
//!
//! ★ A BACKWARDS CURVE DOES NOT COMPILE. A curve whose cap does not exceed
//! its base is either flat (cap == base, the defect above) or a curve that
//! never reaches its base. A zero base never grows at all: `0 × 2ⁿ` is a
//! hot loop at every step. [`Curve::from_millis`] takes both bounds as const
//! generics and asserts `0 < base < cap` in an inline `const` block, so the
//! check runs when the constructor is instantiated — in a `const` item or
//! in ordinary runtime code alike — and a bad pair is a build error, never
//! a runtime panic. The fields are private, so there is no other way in.
//!
//! Tier-honest: the assertion is a post-monomorphization error. `cargo
//! build` and `cargo test` (doctests included) reject a bad pair; `cargo
//! check` alone does not instantiate the constructor and may not. The
//! `compile_fail` doctests below name E0080, which only nightly rustdoc
//! checks; each differs from the passing example only in its bounds, and
//! deleting the assertion turns all three red.

use std::time::Duration;

/// A delay that starts at `base`, doubles on each step, and stops at `cap`.
///
/// Built only through [`Curve::from_millis`], which rejects `cap <= base`
/// (and a zero base) at compile time:
///
/// ```
/// use std::time::Duration;
/// use engenho_controllers::curve::Curve;
///
/// const RETRY: Curve = Curve::from_millis::<1_000, 60_000>();
/// assert_eq!(RETRY.delay(0), Duration::from_secs(1));
/// assert_eq!(RETRY.delay(1), Duration::from_secs(2));
/// assert_eq!(RETRY.delay(40), Duration::from_secs(60));
/// ```
///
/// A cap below the base does not compile:
///
/// ```compile_fail,E0080
/// use engenho_controllers::curve::Curve;
///
/// const BACKWARDS: Curve = Curve::from_millis::<60_000, 1_000>();
/// ```
///
/// Nor does a flat curve (`cap == base`), even built at runtime rather than
/// in a `const` item:
///
/// ```compile_fail,E0080
/// use engenho_controllers::curve::Curve;
///
/// let flat = Curve::from_millis::<1_000, 1_000>();
/// let _ = flat.delay(3);
/// ```
///
/// Nor a zero base, which would never grow:
///
/// ```compile_fail,E0080
/// use engenho_controllers::curve::Curve;
///
/// const STUCK: Curve = Curve::from_millis::<0, 30_000>();
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Curve {
    base_ms: u64,
    cap_ms: u64,
}

impl Curve {
    /// A curve from `BASE_MS` doubling to `CAP_MS`, both in milliseconds.
    ///
    /// Fails to compile unless `0 < BASE_MS < CAP_MS`.
    #[must_use]
    pub const fn from_millis<const BASE_MS: u64, const CAP_MS: u64>() -> Self {
        const {
            assert!(
                BASE_MS > 0 && CAP_MS > BASE_MS,
                "a Curve must grow: it needs 0 < base < cap"
            );
        }
        Self {
            base_ms: BASE_MS,
            cap_ms: CAP_MS,
        }
    }

    /// The first delay on the curve.
    #[must_use]
    pub const fn base(&self) -> Duration {
        Duration::from_millis(self.base_ms)
    }

    /// The delay the curve never exceeds.
    #[must_use]
    pub const fn cap(&self) -> Duration {
        Duration::from_millis(self.cap_ms)
    }

    /// The delay at step `n`: `base × 2ⁿ`, capped.
    ///
    /// `n` counts from zero, so `delay(0)` is `base`. A step too large to
    /// represent saturates at the cap — never wraps to a small delay, which
    /// would be a hot loop that appears only after a long run of failures,
    /// exactly when it matters.
    #[must_use]
    pub const fn delay(&self, n: u32) -> Duration {
        let ms = match 1u64.checked_shl(n) {
            Some(factor) => match self.base_ms.checked_mul(factor) {
                Some(ms) if ms < self.cap_ms => ms,
                _ => self.cap_ms,
            },
            None => self.cap_ms,
        };
        Duration::from_millis(ms)
    }
}

/// A run of consecutive misses, read against a [`Curve`].
///
/// Each [`miss`](Self::miss) owes the next delay on the curve — `base` for
/// the first miss in a row, doubling to `cap` — and [`reset`](Self::reset),
/// on progress, starts the next run from `base` again. The count and the
/// curve it indexes live together, so no caller can grow one without the
/// other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Streak {
    curve: Curve,
    misses: u32,
}

impl Streak {
    /// An empty streak on `curve`: the next miss owes `curve.base()`.
    #[must_use]
    pub const fn new(curve: Curve) -> Self {
        Self { curve, misses: 0 }
    }

    /// Count one miss and return the delay it owes.
    pub fn miss(&mut self) -> Duration {
        let owed = self.curve.delay(self.misses);
        self.misses = self.misses.saturating_add(1);
        owed
    }

    /// Progress was made: the next miss starts from the base again.
    pub fn reset(&mut self) {
        self.misses = 0;
    }

    /// How many misses in a row so far.
    #[must_use]
    pub const fn misses(&self) -> u32 {
        self.misses
    }

    /// The curve this streak is read against.
    #[must_use]
    pub const fn curve(&self) -> Curve {
        self.curve
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const C: Curve = Curve::from_millis::<100, 30_000>();

    #[test]
    fn each_step_doubles_until_the_cap_then_holds() {
        let steps: Vec<Duration> = (0..20).map(|n| C.delay(n)).collect();
        assert_eq!(steps[0], C.base());
        for pair in steps.windows(2) {
            let (a, b) = (pair[0], pair[1]);
            if a < C.cap() {
                assert!(
                    b.as_millis() * 10 >= a.as_millis() * 18 || b == C.cap(),
                    "{a:?} -> {b:?}: below the cap a step must grow at least 1.8x"
                );
            } else {
                assert_eq!(b, C.cap(), "past the cap the delay holds");
            }
        }
        assert_eq!(*steps.last().unwrap(), C.cap());
    }

    #[test]
    fn a_huge_step_saturates_at_the_cap_rather_than_wrapping() {
        for n in [31, 32, 63, 64, 65, u32::MAX] {
            assert_eq!(C.delay(n), C.cap(), "step {n}");
        }
    }

    #[test]
    fn a_streak_grows_per_miss_and_restarts_from_the_base_on_reset() {
        let mut s = Streak::new(C);
        assert_eq!(s.miss(), C.delay(0));
        assert_eq!(s.miss(), C.delay(1));
        assert_eq!(s.miss(), C.delay(2));
        assert_eq!(s.misses(), 3);
        s.reset();
        assert_eq!(s.misses(), 0);
        assert_eq!(s.miss(), C.base(), "a reset streak owes the base again");
    }

    #[test]
    fn a_streak_count_saturates_instead_of_wrapping_to_the_base() {
        let mut s = Streak {
            curve: C,
            misses: u32::MAX,
        };
        assert_eq!(s.miss(), C.cap());
        assert_eq!(s.misses(), u32::MAX);
        assert_eq!(s.miss(), C.cap());
    }
}
