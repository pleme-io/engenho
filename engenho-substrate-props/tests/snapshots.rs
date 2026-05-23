//! Property: snapshot types serialize to expected JSON shape +
//! structural equality invariants.
//!
//! NB: the canonical snapshot types use `name: &'static str` which
//! serializes fine to JSON but doesn't deserialize back (would lose
//! the 'static guarantee — would need `String` or `Cow<'static, str>`
//! to round-trip). These tests verify the serialization SHAPE only.
//! The deserialize gap is a substrate design choice (snapshots are
//! display-only artifacts for dashboards) — codifying that contract.

use engenho_substrate::{
    BudgetSnapshot, ChildCountSnapshot, MiranteSnapshot, ReplayCursorSnapshot, SubscriberSnapshot,
};
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;

proptest_with_env! {
    /// SubscriberSnapshot serializes to {"name": ..., "subscriber_count": ...}
    #[test]
    fn subscriber_snapshot_serialize_shape(count in 0usize..1_000_000) {
        let snap = SubscriberSnapshot {
            name: "test",
            subscriber_count: count,
        };
        let v = serde_json::to_value(&snap).unwrap();
        prop_assert_eq!(&v["name"], &serde_json::json!("test"));
        prop_assert_eq!(&v["subscriber_count"], &serde_json::json!(count));
    }

    /// ChildCountSnapshot serializes to {"name": ..., "child_count": ...}
    #[test]
    fn child_count_snapshot_serialize_shape(count in 0usize..1_000_000) {
        let snap = ChildCountSnapshot {
            name: "test-wrapper",
            child_count: count,
        };
        let v = serde_json::to_value(&snap).unwrap();
        prop_assert_eq!(&v["name"], &serde_json::json!("test-wrapper"));
        prop_assert_eq!(&v["child_count"], &serde_json::json!(count));
    }

    /// MiranteSnapshot serializes channel_count + channel_names + name.
    #[test]
    fn mirante_snapshot_serialize_shape(
        channel_count in 0usize..32,
        names in proptest::collection::vec("[a-z][a-z0-9_-]{0,12}", 0..8),
    ) {
        let snap = MiranteSnapshot {
            name: "mirante",
            channel_count,
            channel_names: names.clone(),
        };
        let v = serde_json::to_value(&snap).unwrap();
        prop_assert_eq!(&v["name"], &serde_json::json!("mirante"));
        prop_assert_eq!(&v["channel_count"], &serde_json::json!(channel_count));
        prop_assert_eq!(&v["channel_names"], &serde_json::json!(names));
    }

    /// ReplayCursorSnapshot serializes all 5 fields.
    #[test]
    fn replay_cursor_snapshot_serialize_shape(
        position in 0usize..1000,
        len in 0usize..1000,
    ) {
        let pos = position.min(len);
        let snap = ReplayCursorSnapshot {
            name: "cursor",
            position: pos,
            len,
            remaining: len - pos,
            is_done: pos == len,
        };
        let v = serde_json::to_value(&snap).unwrap();
        prop_assert_eq!(&v["name"], &serde_json::json!("cursor"));
        prop_assert_eq!(&v["position"], &serde_json::json!(pos));
        prop_assert_eq!(&v["len"], &serde_json::json!(len));
        prop_assert_eq!(&v["remaining"], &serde_json::json!(len - pos));
        prop_assert_eq!(&v["is_done"], &serde_json::json!(pos == len));
    }

    /// BudgetSnapshot has owned fields (no &'static str) — full
    /// round-trip works.
    #[test]
    fn budget_snapshot_full_roundtrip(
        available in 0u64..10_000,
        capacity in 0u64..10_000,
        refill in 0u64..1000,
        packed in 0u64..u64::MAX,
    ) {
        let snap = BudgetSnapshot {
            available,
            capacity,
            refill_per_sec: refill,
            last_refill_packed: packed,
        };
        let json = serde_json::to_string(&snap).unwrap();
        let back: BudgetSnapshot = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(back, snap);
    }

    /// SubscriberSnapshot structural equality — same fields → ==.
    #[test]
    fn subscriber_snapshot_eq_is_structural(count in 0usize..1000) {
        let a = SubscriberSnapshot {
            name: "x",
            subscriber_count: count,
        };
        let b = SubscriberSnapshot {
            name: "x",
            subscriber_count: count,
        };
        prop_assert_eq!(a, b);
    }

    /// ChildCountSnapshot distinct counts → distinct snapshots.
    #[test]
    fn child_count_snapshot_distinct_counts_distinct(
        a in 0usize..500,
        b in 500usize..1000,
    ) {
        let s1 = ChildCountSnapshot {
            name: "w",
            child_count: a,
        };
        let s2 = ChildCountSnapshot {
            name: "w",
            child_count: b,
        };
        prop_assert_ne!(s1, s2);
    }
}
