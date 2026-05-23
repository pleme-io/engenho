//! Property: contract tests for engenho_substrate_props::helpers
//! functions. The helpers shipped without dedicated tests; adoption
//! sites prove they work at specific shapes but the helper's
//! contract should be machine-checked.

use engenho_substrate::Observable;
use engenho_substrate_props::helpers::fresh_mirante_publishing;
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;
use serde::{Deserialize, Serialize};

// Synthetic Observable for contract testing.

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct FakeObsSnapshot {
    pub n: u64,
}

struct FakeObs {
    n: u64,
}

engenho_substrate::define_named!(FakeObs, "fake-obs");

impl Observable for FakeObs {
    type Snapshot = FakeObsSnapshot;
    fn snapshot(&self) -> Self::Snapshot {
        FakeObsSnapshot { n: self.n }
    }
}

proptest_with_env! {
    /// fresh_mirante_publishing returns a Mirante with exactly 1
    /// registered channel under the given name.
    #[test]
    fn fresh_mirante_publishing_registers_one_channel(n in 0u64..1_000_000) {
        let obs = FakeObs { n };
        let m = fresh_mirante_publishing("test-channel", &obs);
        let names = m.list();
        prop_assert_eq!(names.len(), 1);
        prop_assert_eq!(names[0], "test-channel");
    }

    /// The registered channel's current snapshot in `snapshot_all()`
    /// matches the observable's `Observable::snapshot()` byte-for-byte.
    #[test]
    fn fresh_mirante_publishing_snapshot_matches_observable(n in 0u64..1_000_000) {
        let obs = FakeObs { n };
        let direct: FakeObsSnapshot = Observable::snapshot(&obs);
        let m = fresh_mirante_publishing("x", &obs);
        let all = m.snapshot_all();
        let via_mirante: FakeObsSnapshot =
            serde_json::from_value(all["x"].clone()).unwrap();
        prop_assert_eq!(direct, via_mirante);
    }

    /// The returned Mirante is itself Observable + its MiranteSnapshot
    /// reports channel_count = 1 + the channel_name we registered.
    #[test]
    fn fresh_mirante_publishing_mirante_is_observable(n in 0u64..1_000) {
        let obs = FakeObs { n };
        let m = fresh_mirante_publishing("k", &obs);
        let m_snap = Observable::snapshot(&m);
        prop_assert_eq!(m_snap.name, "mirante");
        prop_assert_eq!(m_snap.channel_count, 1);
        prop_assert_eq!(&m_snap.channel_names[0], &"k".to_string());
    }

    /// Different names produce distinct registry keys.
    #[test]
    fn fresh_mirante_publishing_name_round_trips(name in "[a-z][a-z0-9_-]{0,12}") {
        let obs = FakeObs { n: 0 };
        // Box::leak to give the helper a 'static str.
        let static_name: &'static str = Box::leak(name.clone().into_boxed_str());
        let m = fresh_mirante_publishing(static_name, &obs);
        let names = m.list();
        prop_assert_eq!(names[0], static_name);
        prop_assert!(m.snapshot_all().contains_key(&name));
    }
}
