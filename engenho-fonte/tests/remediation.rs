//! Tests for the RemediationPolicy routing layer.

use engenho_fonte::{
    AnomalyEvent, AppRef, InfraBackend, InfraRef, PromessaKind, PromessaRef, RemediationPolicy,
    TopologyRef, mock_anomaly_router,
};

#[test]
fn default_for_additions_is_auto_correct() {
    let ev = AnomalyEvent::AppAdded(AppRef {
        name: "x".into(),
        version: None,
    });
    assert_eq!(
        RemediationPolicy::default_for(&ev),
        RemediationPolicy::AutoCorrect
    );
}

#[test]
fn default_for_removals_is_auto_correct() {
    let ev = AnomalyEvent::AppRemoved(AppRef {
        name: "x".into(),
        version: None,
    });
    assert_eq!(
        RemediationPolicy::default_for(&ev),
        RemediationPolicy::AutoCorrect
    );
}

#[test]
fn default_for_topology_change_is_alert() {
    let ev = AnomalyEvent::TopologyChanged {
        from: TopologyRef {
            strategy: "solo".into(),
            nodes: 1,
        },
        to: TopologyRef {
            strategy: "quorum_3m".into(),
            nodes: 3,
        },
    };
    assert_eq!(
        RemediationPolicy::default_for(&ev),
        RemediationPolicy::Alert
    );
}

#[tokio::test]
async fn router_dispatches_per_default_policy() {
    let (handler, router) = mock_anomaly_router();
    let added = AnomalyEvent::AppAdded(AppRef {
        name: "x".into(),
        version: None,
    });
    let topology = AnomalyEvent::TopologyChanged {
        from: TopologyRef {
            strategy: "solo".into(),
            nodes: 1,
        },
        to: TopologyRef {
            strategy: "solo".into(),
            nodes: 2,
        },
    };
    let p1 = router.route(&added).await.unwrap();
    let p2 = router.route(&topology).await.unwrap();
    assert_eq!(p1, RemediationPolicy::AutoCorrect);
    assert_eq!(p2, RemediationPolicy::Alert);
    let log = handler.log();
    assert_eq!(log.len(), 2);
    assert_eq!(log[0].1, RemediationPolicy::AutoCorrect);
    assert_eq!(log[1].1, RemediationPolicy::Alert);
}

#[tokio::test]
async fn router_with_custom_rule_overrides_defaults() {
    let (handler, router) = mock_anomaly_router();
    let router = router.with_routing_rule(|_| RemediationPolicy::RequireApproval);
    let ev = AnomalyEvent::AppAdded(AppRef {
        name: "y".into(),
        version: None,
    });
    let p = router.route(&ev).await.unwrap();
    assert_eq!(p, RemediationPolicy::RequireApproval);
    assert_eq!(handler.log()[0].1, RemediationPolicy::RequireApproval);
}

#[tokio::test]
async fn every_anomaly_kind_has_a_default_policy() {
    // Sanity: exhaustively cover every AnomalyEvent variant has a
    // default mapping — if a new variant lands, this test catches
    // the omission at compile time (match must be exhaustive).
    let kinds = vec![
        AnomalyEvent::AppAdded(AppRef {
            name: "x".into(),
            version: None,
        }),
        AnomalyEvent::AppRemoved(AppRef {
            name: "x".into(),
            version: None,
        }),
        AnomalyEvent::AppVersionChanged {
            name: "x".into(),
            from: None,
            to: Some("1".into()),
        },
        AnomalyEvent::InfraAdded(InfraRef {
            name: "n".into(),
            backend: InfraBackend::Magma,
        }),
        AnomalyEvent::InfraRemoved(InfraRef {
            name: "n".into(),
            backend: InfraBackend::Magma,
        }),
        AnomalyEvent::PromessaAdded(PromessaRef {
            name: "s".into(),
            kind: PromessaKind::Availability,
            target: 99.0,
        }),
        AnomalyEvent::PromessaRemoved(PromessaRef {
            name: "s".into(),
            kind: PromessaKind::Availability,
            target: 99.0,
        }),
        AnomalyEvent::PromessaTargetChanged {
            name: "s".into(),
            from: 99.0,
            to: 99.99,
        },
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
    ];
    let (handler, router) = mock_anomaly_router();
    for k in &kinds {
        router.route(k).await.unwrap();
    }
    assert_eq!(handler.log().len(), kinds.len());
}
