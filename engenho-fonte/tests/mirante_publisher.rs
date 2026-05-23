//! Tests for the real MirantePublisher — every Outcome flows through
//! engenho-substrate's typed ObservationChannel; subscribers see
//! exactly the latest snapshot.

use engenho_fonte::{
    Change, ChangeKind, Conduit, MirantePublisher, MockAttester, MockEvaluator, MockWatcher,
    mock_system_controller,
};
use engenho_substrate::relogio::{Clock, FrozenClock};
use std::sync::Arc;

#[tokio::test]
async fn outcome_flows_through_real_mirante_channel() {
    let watcher = Arc::new(MockWatcher::new());
    let evaluator = Arc::new(MockEvaluator::new());
    let (_a, _i, _p, _t, ctrl) = mock_system_controller();
    let proposer = Arc::new(ctrl);
    let attester = Arc::new(MockAttester::new());
    let clock: Arc<dyn Clock> = Arc::new(FrozenClock::at(0));
    let publisher = Arc::new(MirantePublisher::new(clock));

    let conduit = Conduit::new(
        watcher.clone(),
        evaluator,
        proposer,
        attester,
        publisher.clone(),
    );

    // Subscribe BEFORE the change lands so we observe it.
    let mut sub = publisher.channel().subscribe();

    watcher
        .push(Change {
            source: "rio".into(),
            kind: ChangeKind::Initial,
            source_text: r#"{"name":"rio","apps":[],"infra":[],"promises":[],"topology":{"strategy":"solo","nodes":1}}"#.into(),
            revision: 42,
        })
        .await;

    let outcome = conduit.tick().await.unwrap().expect("outcome");
    assert_eq!(outcome.revision, 42);

    // The channel's current snapshot is the outcome.
    sub.changed().await.expect("changed");
    let current = sub.borrow().clone();
    assert_eq!(current.revision, 42);
}

#[tokio::test]
async fn registry_has_outcome_channel_registered() {
    let clock: Arc<dyn Clock> = Arc::new(FrozenClock::at(0));
    let publisher = MirantePublisher::new(clock);
    let registered: Vec<&'static str> = publisher.with_mirante(|m| m.list());
    assert_eq!(registered, vec!["fonte.outcome"]);
}

#[tokio::test]
async fn n_outcomes_keep_last_value_only() {
    let watcher = Arc::new(MockWatcher::new());
    let evaluator = Arc::new(MockEvaluator::new());
    let (_a, _i, _p, _t, ctrl) = mock_system_controller();
    let clock: Arc<dyn Clock> = Arc::new(FrozenClock::at(0));
    let publisher = Arc::new(MirantePublisher::new(clock));

    let conduit = Conduit::new(
        watcher.clone(),
        evaluator,
        Arc::new(ctrl),
        Arc::new(MockAttester::new()),
        publisher.clone(),
    );

    for i in 1..=5u64 {
        watcher
            .push(Change {
                source: "x".into(),
                kind: ChangeKind::Initial,
                source_text: format!(
                    r#"{{"name":"x","apps":[],"infra":[],"promises":[],"topology":{{"strategy":"solo","nodes":{i}}}}}"#
                )
                .into(),
                revision: i,
            })
            .await;
    }
    conduit.drain().await.unwrap();

    let current = publisher.channel().current();
    // Last-value-only — channel holds the latest outcome.
    assert_eq!(current.revision, 5);
}
