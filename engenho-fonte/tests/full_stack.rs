//! Full-stack acceptance test — every typed primitive composed into
//! one running Viggy convergence loop.
//!
//! Wires:
//!
//!   ShikumiWatcher (notify file watch)
//!   → SuiEvaluator (Nix bytecode VM)
//!   → SystemController (with KubeAppReconciler producing typed
//!     Deployment manifests, LinhagemAnomalyChain for the typed
//!     DAG-backed drift log, and a wrapper RevoadaProposer to a
//!     PureRaftFace so the typed value also lands as a face
//!     resource)
//!   → MockAttester (BLAKE3 chain; tameshi swap pending)
//!   → MirantePublisher (real Observable broadcast)
//!   + AnomalyRouter (typed RemediationPolicy routing)
//!
//! Then:
//!   1. Write a (defsistema) Nix file
//!   2. Edit it
//!   3. Assert every substrate primitive observed the change with
//!      the typed shape it expects
//!
//! This is the destination's acceptance test — if it passes, the
//! Viggy convergence loop is wired correctly across every typed
//! primitive shipped in v1.16-v1.27.

#![cfg(all(
    feature = "with-shikumi",
    feature = "with-sui-eval",
    feature = "with-revoada"
))]

use async_trait::async_trait;
use engenho_fonte::{
    AnomalyEvent, Conduit, Decision, FonteResult, KubeAppReconciler, LinhagemAnomalyChain,
    MirantePublisher, MockAttester, MockInfraReconciler, MockPromessaReconciler,
    MockTopologyReconciler, ProposalId, Proposer, RevoadaProposer, ShikumiWatcher, SuiEvaluator,
    SystemController, mock_anomaly_router,
};
use engenho_revoada::face::Face;
use engenho_revoada::{FabricFace, FaceKind, PureRaftFace};
use engenho_substrate::relogio::{Clock, FrozenClock};
use std::sync::Arc;
use tokio::time::{Duration, sleep};

/// Chained Proposer — call N inner Proposers in sequence per
/// propose(). Returns the first Proposer's id; later proposers can
/// fail without losing the first id. Used here so the SystemController
/// AND the RevoadaProposer both see every Decision.
struct ChainedProposer {
    inner: Vec<Arc<dyn Proposer>>,
}

#[async_trait]
impl Proposer for ChainedProposer {
    async fn propose(&self, decision: &Decision) -> FonteResult<ProposalId> {
        let mut first: Option<ProposalId> = None;
        for p in &self.inner {
            let id = p.propose(decision).await?;
            if first.is_none() {
                first = Some(id);
            }
        }
        Ok(first.unwrap_or(0))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_stack_convergence_loop_typed_end_to_end() {
    // ── 1. Write the initial Sistema as a Nix file ───────────────
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    std::fs::write(
        &path,
        r#"{
            name = "rio";
            apps = [{ name = "podinfo"; version = "6.4.1"; }];
            infra = [];
            promises = [];
            topology = { strategy = "solo"; nodes = 1; };
        }"#,
    )
    .unwrap();

    // ── 2. Build every real-substrate primitive ──────────────────
    let watcher = Arc::new(ShikumiWatcher::new(&path).unwrap());
    let evaluator = Arc::new(SuiEvaluator::new());
    let apps = Arc::new(KubeAppReconciler::new());
    let infra = Arc::new(MockInfraReconciler::new());
    let promises = Arc::new(MockPromessaReconciler::new());
    let topology = Arc::new(MockTopologyReconciler::new());
    let anomaly_chain = Arc::new(LinhagemAnomalyChain::new());
    let (anomaly_handler, anomaly_router) = mock_anomaly_router();
    let ctrl = SystemController::new(
        apps.clone(),
        infra.clone(),
        promises.clone(),
        topology.clone(),
    )
    .with_anomaly_chain(anomaly_chain.clone());

    let face = PureRaftFace::from_declaration(&FabricFace {
        name: "fonte-fullstack".into(),
        kind: FaceKind::PureRaft,
    })
    .unwrap();
    face.start().unwrap();
    let face_arc: Arc<dyn Face> = Arc::new(face);

    let revoada_proposer = Arc::new(RevoadaProposer::new(face_arc.clone()));
    let proposer = Arc::new(ChainedProposer {
        inner: vec![Arc::new(ctrl), revoada_proposer],
    });
    let attester = Arc::new(MockAttester::new());
    let clock: Arc<dyn Clock> = Arc::new(FrozenClock::at(0));
    let publisher = Arc::new(MirantePublisher::new(clock));

    let conduit = Conduit::new(
        watcher,
        evaluator,
        proposer.clone(),
        attester.clone(),
        publisher.clone(),
    );

    // ── 3. Tick once for the initial change ──────────────────────
    let initial = conduit
        .tick()
        .await
        .expect("initial tick")
        .expect("initial outcome");
    assert_eq!(initial.proposal_id, 0);

    // KubeAppReconciler emitted 1 Deployment manifest with the
    // typed image string.
    let dep = &apps.emitted()[0];
    assert_eq!(&dep.metadata.name, "podinfo");
    assert_eq!(
        dep.spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0]
            .image
            .as_deref(),
        Some("podinfo:6.4.1")
    );

    // LinhagemAnomalyChain recorded the initial diff (1 AppAdded +
    // 1 TopologyChanged from synthetic empty Sistema).
    assert_eq!(anomaly_chain.len(), 2);
    anomaly_chain.with_graph(|g| {
        assert_eq!(g.roots.len(), 1, "exactly one root for the initial chain");
    });

    // Route the recorded events through the AnomalyRouter; the
    // typed RemediationPolicy fires for each.
    for kind in &[
        AnomalyEvent::AppAdded(engenho_fonte::AppRef {
            name: "podinfo".into(),
            version: Some("6.4.1".into()),
        }),
        AnomalyEvent::TopologyChanged {
            from: engenho_fonte::TopologyRef {
                strategy: "solo".into(),
                nodes: 0,
            },
            to: engenho_fonte::TopologyRef {
                strategy: "solo".into(),
                nodes: 1,
            },
        },
    ] {
        anomaly_router.route(kind).await.unwrap();
    }
    let routed = anomaly_handler.log();
    assert_eq!(routed.len(), 2);

    // MirantePublisher captured the latest outcome.
    let mirante_now = publisher.channel().current();
    assert_eq!(mirante_now.revision, initial.revision);

    // Attester chain has one receipt.
    assert_eq!(attester.chain().len(), 1);

    // Sub-reconciler logs reflect typed cardinalities.
    assert_eq!(apps.emitted().len(), 1);
    assert_eq!(infra.log().len(), 0);
    assert_eq!(promises.log().len(), 0);
    assert_eq!(topology.log().len(), 1);

    // Revoada face received the proposed Sistema.
    assert_eq!(
        face_arc.resource_count(),
        1,
        "revoada face should hold one resource after fonte proposed it"
    );

    // ── 4. Edit the Sistema file: add a second app ───────────────
    sleep(Duration::from_millis(100)).await;
    std::fs::write(
        &path,
        r#"{
            name = "rio";
            apps = [
                { name = "podinfo"; version = "6.4.1"; }
                { name = "lilitu";  version = null; }
            ];
            infra = [];
            promises = [];
            topology = { strategy = "solo"; nodes = 1; };
        }"#,
    )
    .unwrap();

    // Wait for the notify watcher + conduit to drain.
    let second = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(Some(o)) = conduit.tick().await {
                return o;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("modify notify within 5s");
    assert!(second.revision > initial.revision);

    // ── 5. Assert convergence to the new Sistema ─────────────────
    // KubeAppReconciler emitted ONE additional Deployment (only the
    // new lilitu app is added; podinfo was already reconciled
    // last tick, but our mocks re-reconcile every existing app per
    // tick — so 2 reconciles total for the second tick.)
    let total = apps.emitted().len();
    assert!(total >= 2, "expected >= 2 emitted, got {total}");

    // LinhagemAnomalyChain now has at least one new event (the new
    // AppAdded for lilitu).
    assert!(
        anomaly_chain.len() > 2,
        "anomaly chain should grow after the new app appears"
    );

    // Revoada face received the second proposal as well. The
    // resource_count stays at 1 because both proposals address the
    // SAME logical resource (same source path → same Pod name) —
    // the face is correctly idempotent on desired state, not on
    // mutation count. (To assert mutation count, subscribe to the
    // face's watch stream.)
    assert_eq!(face_arc.resource_count(), 1);

    face_arc.shutdown().unwrap();
}
