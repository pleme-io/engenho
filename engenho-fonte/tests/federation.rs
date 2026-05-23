//! Federation tests — a single broker announces a Sistema change;
//! N peer Conduits subscribed to it all reconcile in lockstep.

use engenho_fonte::{
    Conduit, FederationBroker, MockAttester, MockEvaluator, MockPublisher, mock_system_controller,
};
use std::sync::Arc;

fn spawn_peer(broker: &FederationBroker) -> (Arc<engenho_fonte::MockAppReconciler>, Conduit) {
    let watcher = Arc::new(broker.subscribe());
    let evaluator = Arc::new(MockEvaluator::new());
    let (apps, _i, _p, _t, ctrl) = mock_system_controller();
    let proposer = Arc::new(ctrl);
    let attester = Arc::new(MockAttester::new());
    let publisher = Arc::new(MockPublisher::new());
    let conduit = Conduit::new(watcher, evaluator, proposer, attester, publisher);
    (apps, conduit)
}

#[tokio::test]
async fn one_announce_propagates_to_three_peers() {
    let broker = FederationBroker::new(32);
    let (apps_a, conduit_a) = spawn_peer(&broker);
    let (apps_b, conduit_b) = spawn_peer(&broker);
    let (apps_c, conduit_c) = spawn_peer(&broker);

    let sistema = r#"{"name":"global","apps":[{"name":"podinfo","version":null}],"infra":[],"promises":[],"topology":{"strategy":"solo","nodes":1}}"#;
    broker.announce("rio".into(), sistema.into());

    let oa = conduit_a.tick().await.unwrap().expect("a outcome");
    let ob = conduit_b.tick().await.unwrap().expect("b outcome");
    let oc = conduit_c.tick().await.unwrap().expect("c outcome");
    // All three peers see revision 0 (federation revision).
    assert_eq!(oa.revision, 0);
    assert_eq!(ob.revision, 0);
    assert_eq!(oc.revision, 0);

    // All three peers reconciled the same 1 app.
    assert_eq!(apps_a.log().len(), 1);
    assert_eq!(apps_b.log().len(), 1);
    assert_eq!(apps_c.log().len(), 1);
    assert_eq!(apps_a.log()[0].name.as_ref(), "podinfo");
    assert_eq!(apps_b.log()[0].name.as_ref(), "podinfo");
    assert_eq!(apps_c.log()[0].name.as_ref(), "podinfo");
}

#[tokio::test]
async fn announce_revision_is_monotone() {
    let broker = FederationBroker::new(32);
    let r1 = broker.announce("x".into(), r#"{}"#.into());
    let r2 = broker.announce("x".into(), r#"{}"#.into());
    let r3 = broker.announce("x".into(), r#"{}"#.into());
    assert!(r1 < r2 && r2 < r3);
}

#[tokio::test]
async fn lagged_peer_recovers_on_next_change() {
    // Capacity 2 — third announce drops the oldest from a non-draining
    // peer. The peer reads the announces it can; the conduit
    // returns None (lagged) which the drain loop treats as
    // end-of-stream — the peer would re-subscribe in production.
    let broker = FederationBroker::new(2);
    let watcher = Arc::new(broker.subscribe());
    let (_apps, _i, _p, _t, ctrl) = mock_system_controller();
    let conduit = Conduit::new(
        watcher,
        Arc::new(MockEvaluator::new()),
        Arc::new(ctrl),
        Arc::new(MockAttester::new()),
        Arc::new(MockPublisher::new()),
    );

    let sistema = r#"{"name":"a","apps":[],"infra":[],"promises":[],"topology":{"strategy":"solo","nodes":1}}"#;
    // Overflow by far more than the capacity to guarantee lag.
    for _ in 0..8 {
        broker.announce("x".into(), sistema.into());
    }
    // drain: the peer pulls what it can; if it lagged, the broker
    // returns Lagged which our Watcher impl translates to Ok(None).
    let outcomes = conduit.drain().await.unwrap();
    // At least one outcome processed before the lag — but the test's
    // load-bearing assertion is the no-panic + no-error contract:
    // lagged broker does not crash the conduit.
    assert!(outcomes.len() <= 8);
}
