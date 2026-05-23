//! Property: mirante ObservationChannel + Mirante registry invariants.

use engenho_substrate::{Clock, FrozenClock, Mirante, ObservationChannel};
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;
use std::sync::Arc;

fn frozen(t: u64) -> Arc<dyn Clock> {
    Arc::new(FrozenClock::at(t))
}

proptest_with_env! {
    /// Sequence of publishes: current() always equals the last published value.
    #[test]
    fn current_equals_last_published(
        values in proptest::collection::vec(any::<u32>(), 1..32),
    ) {
        let ch = ObservationChannel::new(0u32, frozen(0));
        let mut last = 0u32;
        for v in &values {
            ch.publish(*v);
            last = *v;
        }
        prop_assert_eq!(ch.current(), last);
    }

    /// Subscriber count grows + shrinks correctly with subscribe/drop.
    #[test]
    fn subscriber_count_tracks_lifecycle(
        subs in 0usize..8,
    ) {
        let ch = ObservationChannel::new(0u32, frozen(0));
        let baseline = ch.subscriber_count();
        let receivers: Vec<_> = (0..subs).map(|_| ch.subscribe()).collect();
        prop_assert_eq!(ch.subscriber_count(), baseline + subs);
        drop(receivers);
        prop_assert_eq!(ch.subscriber_count(), baseline);
    }

    /// publishes are last-value-only: after N publishes, current is the Nth.
    #[test]
    fn publishes_are_last_value_only(
        values in proptest::collection::vec(any::<u32>(), 1..64),
    ) {
        let ch = ObservationChannel::new(0u32, frozen(0));
        let last = *values.last().unwrap();
        for v in &values {
            ch.publish(*v);
        }
        prop_assert_eq!(ch.current(), last);
    }

    /// Mirante registry: register/list count parity.
    #[test]
    fn registry_list_count_parity(n in 1usize..8) {
        let mut m = Mirante::new();
        for i in 0..n {
            let name: &'static str = Box::leak(format!("ch{i}").into_boxed_str());
            let ch = Arc::new(ObservationChannel::new(0u32, frozen(0)));
            m.register(name, ch);
        }
        prop_assert_eq!(m.len(), n);
        prop_assert_eq!(m.list().len(), n);
        prop_assert!(!m.is_empty());
    }

    /// snapshot_all() reflects the latest published value for each channel.
    #[test]
    fn snapshot_all_reflects_latest_publish(
        values in proptest::collection::vec(any::<u32>(), 1..8),
    ) {
        let mut m = Mirante::new();
        let mut chans: Vec<(&'static str, Arc<ObservationChannel<u32>>)> = Vec::new();
        for (i, v) in values.iter().enumerate() {
            let name: &'static str = Box::leak(format!("ch{i}").into_boxed_str());
            let ch = Arc::new(ObservationChannel::new(0u32, frozen(0)));
            ch.publish(*v);
            m.register(name, ch.clone());
            chans.push((name, ch));
        }
        let snap = m.snapshot_all();
        for (name, ch) in &chans {
            let want = ch.current();
            let got = snap.get(*name).unwrap();
            prop_assert_eq!(got, &serde_json::json!(want));
        }
    }

    /// Re-registering the same name replaces; len stays bounded.
    #[test]
    fn re_register_same_name_replaces(replays in 1usize..16) {
        let mut m = Mirante::new();
        for _ in 0..replays {
            let ch = Arc::new(ObservationChannel::new(0u32, frozen(0)));
            m.register("x", ch);
        }
        prop_assert_eq!(m.len(), 1);
        prop_assert_eq!(m.list(), vec!["x"]);
    }

    /// Empty registry: len == 0, is_empty true, snapshot_all empty.
    #[test]
    fn empty_registry_invariants(_seed in any::<u8>()) {
        let m = Mirante::new();
        prop_assert!(m.is_empty());
        prop_assert_eq!(m.len(), 0);
        prop_assert!(m.snapshot_all().is_empty());
        prop_assert!(m.last_changed_all().is_empty());
    }
}
