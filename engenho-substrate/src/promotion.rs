//! Promotion policy — pluggable strategy controlling how
//! `TieredCache` propagates a hit from a lower tier to higher
//! tiers.
//!
//! ## Strategies
//!
//!   * [`PromotionPolicy::Eager`] — promote on every hit (default;
//!     what `TieredCache::get_*` did before this module existed).
//!   * [`PromotionPolicy::Lazy`] — never promote on read; rely on
//!     a reconcile loop to fan out (cheaper on hot caches; trades
//!     freshness for I/O).
//!   * [`PromotionPolicy::SampleRate(n)`] — promote 1-in-N hits;
//!     bounded I/O even under contention.
//!   * [`PromotionPolicy::OnlyTo(layer)`] — promote only to tier
//!     index `≤ layer`; useful when L0 fills fast but L1 is cheap.
//!
//! ## Composition
//!
//! `TieredCache::with_promotion(policy)` wraps the cache with a
//! typed policy. The default (Eager) preserves the old behavior;
//! consumers opt into the others when warranted.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Promotion strategy applied on every `get_*` hit.
#[derive(Clone, Debug)]
pub enum PromotionPolicy {
    /// Promote to every higher tier on every hit.
    Eager,
    /// Never promote on read.
    Lazy,
    /// Promote 1-in-`rate` hits; if `rate == 0` or `rate == 1`,
    /// behaves like `Eager`.
    SampleRate(u64),
    /// Promote only to tiers with index ≤ `max_tier`. (Tiers are
    /// 0-indexed from fastest; `OnlyTo(0)` ≡ promote only to L0.)
    OnlyTo(usize),
}

impl Default for PromotionPolicy {
    fn default() -> Self {
        Self::Eager
    }
}

/// Decision context for a single hit.
#[derive(Debug, Clone, Copy)]
pub struct PromotionContext {
    /// Index of the tier the hit came from (0 = fastest).
    pub source_tier: usize,
    /// Total tier count.
    pub total_tiers: usize,
}

/// Stateful promoter — owns the counter for `SampleRate`.
#[derive(Default)]
pub struct PromotionGate {
    counter: AtomicU64,
    policy: PromotionPolicy,
}

impl PromotionGate {
    /// New gate with the given policy.
    #[must_use]
    pub fn new(policy: PromotionPolicy) -> Self {
        Self {
            counter: AtomicU64::new(0),
            policy,
        }
    }

    /// Construct an `Arc` — useful for `TieredCache` consumers.
    #[must_use]
    pub fn arc(policy: PromotionPolicy) -> Arc<Self> {
        Arc::new(Self::new(policy))
    }

    /// Should this hit promote? Returns the highest tier index to
    /// promote to (exclusive — i.e. `Some(2)` = promote to tiers 0
    /// and 1). `None` = don't promote.
    pub fn decide(&self, ctx: PromotionContext) -> Option<usize> {
        if ctx.source_tier == 0 {
            return None; // already at fastest tier
        }
        match self.policy {
            PromotionPolicy::Eager => Some(ctx.source_tier),
            PromotionPolicy::Lazy => None,
            PromotionPolicy::SampleRate(rate) => {
                if rate <= 1 {
                    return Some(ctx.source_tier);
                }
                let n = self.counter.fetch_add(1, Ordering::Relaxed);
                if n % rate == 0 {
                    Some(ctx.source_tier)
                } else {
                    None
                }
            }
            PromotionPolicy::OnlyTo(max) => {
                let target = ctx.source_tier.min(max + 1);
                if target == 0 { None } else { Some(target) }
            }
        }
    }

    /// Current policy (telemetry helper).
    #[must_use]
    pub fn policy(&self) -> &PromotionPolicy {
        &self.policy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(source: usize, total: usize) -> PromotionContext {
        PromotionContext {
            source_tier: source,
            total_tiers: total,
        }
    }

    #[test]
    fn eager_promotes_to_source_tier() {
        let g = PromotionGate::new(PromotionPolicy::Eager);
        assert_eq!(g.decide(ctx(0, 3)), None); // no promotion needed
        assert_eq!(g.decide(ctx(1, 3)), Some(1));
        assert_eq!(g.decide(ctx(2, 3)), Some(2));
    }

    #[test]
    fn lazy_never_promotes() {
        let g = PromotionGate::new(PromotionPolicy::Lazy);
        for s in 0..5 {
            assert_eq!(g.decide(ctx(s, 5)), None);
        }
    }

    #[test]
    fn sample_rate_one_acts_like_eager() {
        let g = PromotionGate::new(PromotionPolicy::SampleRate(1));
        assert_eq!(g.decide(ctx(1, 3)), Some(1));
        assert_eq!(g.decide(ctx(2, 3)), Some(2));
    }

    #[test]
    fn sample_rate_zero_acts_like_eager() {
        let g = PromotionGate::new(PromotionPolicy::SampleRate(0));
        assert_eq!(g.decide(ctx(1, 3)), Some(1));
    }

    #[test]
    fn sample_rate_three_promotes_one_in_three() {
        let g = PromotionGate::new(PromotionPolicy::SampleRate(3));
        let mut promoted = 0;
        for _ in 0..9 {
            if g.decide(ctx(1, 2)).is_some() {
                promoted += 1;
            }
        }
        // 9 attempts / rate 3 → exactly 3 promotions.
        assert_eq!(promoted, 3);
    }

    #[test]
    fn source_tier_zero_never_promotes() {
        for policy in [
            PromotionPolicy::Eager,
            PromotionPolicy::Lazy,
            PromotionPolicy::SampleRate(2),
            PromotionPolicy::OnlyTo(5),
        ] {
            let g = PromotionGate::new(policy);
            assert_eq!(g.decide(ctx(0, 3)), None);
        }
    }

    #[test]
    fn only_to_caps_promotion_target() {
        let g = PromotionGate::new(PromotionPolicy::OnlyTo(0));
        // Source tier 2 → only promote up to tier 0+1=1 → so tier 0.
        // Actually OnlyTo(0) means "only promote into tier ≤ 0",
        // returning Some(1) means "fill tiers [0..1)" — tier 0 only.
        assert_eq!(g.decide(ctx(2, 3)), Some(1));
    }

    #[test]
    fn only_to_above_source_uses_source() {
        let g = PromotionGate::new(PromotionPolicy::OnlyTo(5));
        // Source tier 1, OnlyTo(5) → min(1, 6) = 1 → Some(1).
        assert_eq!(g.decide(ctx(1, 3)), Some(1));
    }

    #[test]
    fn default_is_eager() {
        let g = PromotionGate::default();
        assert!(matches!(g.policy(), PromotionPolicy::Eager));
    }

    #[test]
    fn arc_constructor_wraps_policy() {
        let g = PromotionGate::arc(PromotionPolicy::Lazy);
        assert!(matches!(g.policy(), PromotionPolicy::Lazy));
    }

    #[test]
    fn policy_round_trips_via_arc() {
        let p = PromotionPolicy::SampleRate(10);
        let g = PromotionGate::new(p);
        if let PromotionPolicy::SampleRate(n) = g.policy() {
            assert_eq!(*n, 10);
        } else {
            panic!("policy mismatch");
        }
    }
}
