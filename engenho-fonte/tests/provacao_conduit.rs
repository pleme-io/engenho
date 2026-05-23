//! Tests for ProvacaoConduit — chaos-gated Conduit + typed FonteFault
//! injection into the Viggy convergence loop.

use engenho_fonte::{
    Change, ChangeKind, Conduit, FonteError, FonteFault, MockAttester, MockEvaluator,
    MockPublisher, MockWatcher, ProvacaoConduit, mock_system_controller,
};
use engenho_substrate::provacao::{Policy, Provacao};
use engenho_substrate::relogio::{Clock, FrozenClock};
use std::sync::Arc;

fn build_conduit() -> (Arc<MockWatcher>, Conduit) {
    let watcher = Arc::new(MockWatcher::new());
    let evaluator = Arc::new(MockEvaluator::new());
    let (_a, _i, _p, _t, ctrl) = mock_system_controller();
    let conduit = Conduit::new(
        watcher.clone(),
        evaluator,
        Arc::new(ctrl),
        Arc::new(MockAttester::new()),
        Arc::new(MockPublisher::new()),
    );
    (watcher, conduit)
}

#[tokio::test]
async fn no_chaos_means_normal_tick() {
    let (watcher, conduit) = build_conduit();
    let provacao: Arc<Provacao<FonteFault>> = Arc::new(Provacao::new("test", 0));
    let clock: Arc<dyn Clock> = Arc::new(FrozenClock::at(0));
    let chaos = ProvacaoConduit::new(conduit, provacao, clock);

    watcher
        .push(Change {
            source: "rio".into(),
            kind: ChangeKind::Initial,
            source_text: r#"{"name":"rio","apps":[],"infra":[],"promises":[],"topology":{"strategy":"solo","nodes":1}}"#.into(),
            revision: 1,
        })
        .await;
    let out = chaos.tick().await.unwrap();
    assert!(out.is_some());
}

#[tokio::test]
async fn every_nth_policy_fires_typed_fault() {
    let (watcher, conduit) = build_conduit();
    let provacao: Arc<Provacao<FonteFault>> =
        Arc::new(Provacao::new("test", 0).with_policy(Policy::EveryNth {
            fault: FonteFault::ProposeFault,
            n: 1, // every call fires
        }));
    let clock: Arc<dyn Clock> = Arc::new(FrozenClock::at(0));
    let chaos = ProvacaoConduit::new(conduit, provacao, clock);

    watcher
        .push(Change {
            source: "rio".into(),
            kind: ChangeKind::Initial,
            source_text: "null".into(),
            revision: 1,
        })
        .await;
    let err = chaos.tick().await.unwrap_err();
    match err {
        FonteError::Propose(msg) => {
            assert!(msg.contains("provacao"), "got: {msg}");
        }
        other => panic!("expected Propose, got {other:?}"),
    }
}

#[tokio::test]
async fn every_other_call_fires_via_n_2() {
    let (watcher, conduit) = build_conduit();
    let provacao: Arc<Provacao<FonteFault>> =
        Arc::new(Provacao::new("test", 0).with_policy(Policy::EveryNth {
            fault: FonteFault::AttestFault,
            n: 2,
        }));
    let clock: Arc<dyn Clock> = Arc::new(FrozenClock::at(0));
    let chaos = ProvacaoConduit::new(conduit, provacao, clock);

    for i in 1..=4u64 {
        watcher
            .push(Change {
                source: "x".into(),
                kind: ChangeKind::Initial,
                source_text: r#"{"name":"x","apps":[],"infra":[],"promises":[],"topology":{"strategy":"solo","nodes":1}}"#.into(),
                revision: i,
            })
            .await;
    }
    // n=2: calls 2 + 4 fire; calls 1 + 3 succeed.
    let r1 = chaos.tick().await;
    let r2 = chaos.tick().await;
    let r3 = chaos.tick().await;
    let r4 = chaos.tick().await;
    assert!(r1.is_ok(), "call 1 should succeed: {r1:?}");
    assert!(r2.is_err(), "call 2 should fail: {r2:?}");
    assert!(r3.is_ok(), "call 3 should succeed: {r3:?}");
    assert!(r4.is_err(), "call 4 should fail: {r4:?}");
}

#[tokio::test]
async fn every_fault_kind_maps_to_typed_fonte_error() {
    let pairs = vec![
        (FonteFault::WatchFault, "fonte/watch"),
        (FonteFault::EvalFault, "fonte/eval"),
        (FonteFault::ProposeFault, "fonte/propose"),
        (FonteFault::AttestFault, "fonte/attest"),
        (FonteFault::PublishFault, "fonte/publish"),
    ];
    for (fault, prefix) in pairs {
        let err = fault.into_fonte_error();
        let msg = format!("{err}");
        assert!(msg.starts_with(prefix), "{prefix} expected, got: {msg}");
    }
}
