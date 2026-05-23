//! Tests for the LinhagemAnomalyChain — proves the substrate's
//! LineageGraph<AnomalyEntry> backs the chain identically to
//! MockAnomalyChain's Vec semantics, plus typed graph queries.

use engenho_fonte::{AnomalyChain, AnomalyEvent, AppRef, LinhagemAnomalyChain, TopologyRef};

#[tokio::test]
async fn record_appends_one_node_per_event() {
    let chain = LinhagemAnomalyChain::new();
    let events = vec![
        AnomalyEvent::AppAdded(AppRef {
            name: "a".into(),
            version: None,
        }),
        AnomalyEvent::AppAdded(AppRef {
            name: "b".into(),
            version: None,
        }),
    ];
    chain.record(1, events).await.unwrap();
    assert_eq!(chain.len(), 2);
}

#[tokio::test]
async fn record_empty_is_noop() {
    let chain = LinhagemAnomalyChain::new();
    let id = chain.record(1, vec![]).await.unwrap();
    assert!(id.is_none());
    assert!(chain.is_empty());
}

#[tokio::test]
async fn chain_is_a_typed_dag_with_one_root() {
    let chain = LinhagemAnomalyChain::new();
    chain
        .record(
            1,
            vec![
                AnomalyEvent::AppAdded(AppRef {
                    name: "x".into(),
                    version: None,
                }),
                AnomalyEvent::TopologyChanged {
                    from: TopologyRef {
                        strategy: "solo".into(),
                        nodes: 1,
                    },
                    to: TopologyRef {
                        strategy: "solo".into(),
                        nodes: 2,
                    },
                },
            ],
        )
        .await
        .unwrap();
    chain.with_graph(|g| {
        assert_eq!(g.len(), 2);
        assert_eq!(g.roots.len(), 1, "exactly one root expected");
    });
}

#[tokio::test]
async fn topological_order_matches_insertion_order() {
    let chain = LinhagemAnomalyChain::new();
    chain
        .record(
            1,
            vec![
                AnomalyEvent::AppAdded(AppRef {
                    name: "first".into(),
                    version: None,
                }),
                AnomalyEvent::AppAdded(AppRef {
                    name: "second".into(),
                    version: None,
                }),
                AnomalyEvent::AppAdded(AppRef {
                    name: "third".into(),
                    version: None,
                }),
            ],
        )
        .await
        .unwrap();
    chain.with_graph(|g| {
        let order = g.topo_sort();
        assert_eq!(order.len(), 3);
        // The first node is a root (no causes); the rest each have
        // the previous as a cause. topo_sort returns ancestors first.
        let mut nodes: Vec<&AnomalyEvent> = order
            .iter()
            .map(|fp| &g.nodes.get(fp).unwrap().value.event)
            .collect();
        nodes.reverse(); // we want insertion order
        nodes.reverse(); // back to topo order
        match nodes[0] {
            AnomalyEvent::AppAdded(a) => assert_eq!(&*a.name, "first"),
            other => panic!("expected first AppAdded, got {other:?}"),
        }
    });
}

// ── Integration with SystemController ───────────────────────────

#[tokio::test]
async fn linhagem_chain_records_drift_from_controller() {
    use engenho_fonte::{
        Change, ChangeKind, Conduit, MockAttester, MockEvaluator, MockPublisher, MockWatcher,
        mock_system_controller,
    };
    use std::sync::Arc;

    let watcher = Arc::new(MockWatcher::new());
    let evaluator = Arc::new(MockEvaluator::new());
    let (_a, _i, _p, _t, ctrl) = mock_system_controller();
    let linhagem = Arc::new(LinhagemAnomalyChain::new());
    let ctrl = ctrl.with_anomaly_chain(linhagem.clone());

    let conduit = Conduit::new(
        watcher.clone(),
        evaluator,
        Arc::new(ctrl),
        Arc::new(MockAttester::new()),
        Arc::new(MockPublisher::new()),
    );

    watcher
        .push(Change {
            source: "rio".into(),
            kind: ChangeKind::Initial,
            source_text: r#"{"name":"rio","apps":[{"name":"x","version":null}],"infra":[],"promises":[],"topology":{"strategy":"solo","nodes":1}}"#.into(),
            revision: 1,
        })
        .await;

    conduit.tick().await.unwrap();
    // First reconcile diffs vs synthetic-empty: 1 AppAdded + 1
    // TopologyChanged = 2 events chained.
    assert_eq!(linhagem.len(), 2);
}
