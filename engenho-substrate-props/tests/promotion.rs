//! Property: PromotionPolicy decisions match their declared semantics.

use engenho_substrate::{PromotionContext, PromotionGate, PromotionPolicy};
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;

fn ctx(source: usize, total: usize) -> PromotionContext {
    PromotionContext {
        source_tier: source,
        total_tiers: total.max(1),
    }
}

proptest_with_env! {
    /// Eager always promotes to source_tier (when source > 0).
    #[test]
    fn eager_always_promotes_above_l0(
        source in 1usize..16,
        total in 1usize..16,
    ) {
        let g = PromotionGate::new(PromotionPolicy::Eager);
        let decision = g.decide(ctx(source, total));
        prop_assert_eq!(decision, Some(source));
    }

    /// Lazy never promotes regardless of source.
    #[test]
    fn lazy_never_promotes(
        source in 0usize..16,
        total in 1usize..16,
    ) {
        let g = PromotionGate::new(PromotionPolicy::Lazy);
        prop_assert_eq!(g.decide(ctx(source, total)), None);
    }

    /// SampleRate(n) promotes roughly 1-in-n hits.
    #[test]
    fn sample_rate_promotes_one_in_n(
        rate in 2u64..16,
        attempts in 32usize..256,
    ) {
        let g = PromotionGate::new(PromotionPolicy::SampleRate(rate));
        let mut promoted = 0;
        for _ in 0..attempts {
            if g.decide(ctx(1, 2)).is_some() {
                promoted += 1;
            }
        }
        // attempts / rate promotions expected — exact for SampleRate
        // since it's not random, it's a counter-driven stride.
        prop_assert_eq!(promoted, attempts / rate as usize + (if attempts % rate as usize > 0 { 1 } else { 0 }));
    }

    /// Source tier 0 never promotes regardless of policy.
    #[test]
    fn source_tier_zero_never_promotes(
        rate in 1u64..16,
        max_tier in 0usize..16,
        total in 1usize..16,
    ) {
        for policy in [
            PromotionPolicy::Eager,
            PromotionPolicy::Lazy,
            PromotionPolicy::SampleRate(rate),
            PromotionPolicy::OnlyTo(max_tier),
        ] {
            let g = PromotionGate::new(policy);
            prop_assert_eq!(g.decide(ctx(0, total)), None);
        }
    }

    /// OnlyTo(max) caps promotion at max+1.
    #[test]
    fn only_to_caps_promotion(
        max_tier in 0usize..16,
        source in 1usize..32,
        total in 1usize..16,
    ) {
        let g = PromotionGate::new(PromotionPolicy::OnlyTo(max_tier));
        let decision = g.decide(ctx(source, total));
        if let Some(promote_to) = decision {
            prop_assert!(promote_to <= source);
            prop_assert!(promote_to <= max_tier + 1);
        }
    }
}
