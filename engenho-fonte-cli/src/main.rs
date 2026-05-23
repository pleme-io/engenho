//! `engenho-fonte` daemon — the operator-facing live-config loop.
//!
//! ## What it does
//!
//! 1. Watches a `(defsistema …)` Nix file via shikumi-notify
//!    (`ShikumiWatcher`).
//! 2. Evaluates each change via sui's Nix bytecode VM
//!    (`SuiEvaluator`).
//! 3. Materializes the typed Sistema through the four mock sub-
//!    reconcilers, then routes the resulting AnomalyEvents through
//!    the typed RemediationPolicy.
//! 4. Chains every transition via the in-process `MockAttester` +
//!    `MockAnomalyChain` (real tameshi wiring lands behind a future
//!    `with-tameshi` feature flag).
//! 5. Optionally posts each Sistema into a revoada `PureRaftFace`
//!    (when `--with-revoada` is on) so the typed change becomes a
//!    persistent record in the cluster's typed face inventory.
//!
//! Each emitted change re-tickets the conduit so the loop converges
//! to whatever the file currently says.
//!
//! ## Usage
//!
//! ```bash
//! engenho-fonte --file ./sistemas/rio.nix
//! engenho-fonte --file ./sistemas/rio.nix --with-revoada
//! engenho-fonte --file ./sistemas/rio.nix --log-level debug
//! ```

use clap::Parser;
use engenho_fonte::{
    Conduit, MockAnomalyChain, MockAttester, MockPublisher, ShikumiWatcher, SuiEvaluator,
    mock_anomaly_router, mock_system_controller,
};
use engenho_revoada::face::Face;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::time::{Duration, sleep};

#[derive(Parser, Debug)]
#[command(version, about = "engenho-fonte — live-config convergence loop")]
struct Cli {
    /// Path to the (defsistema …) Nix file to watch.
    #[arg(long, short = 'f')]
    file: PathBuf,

    /// Enable the revoada FabricFace Proposer (PureRaftFace by
    /// default). When off, the loop uses the SystemController +
    /// mock sub-reconcilers only.
    #[arg(long, default_value_t = false)]
    with_revoada: bool,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(format!("info,engenho_fonte={}", cli.log_level)));
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    tracing::info!(file = %cli.file.display(), with_revoada = cli.with_revoada, "engenho-fonte starting");

    let watcher = Arc::new(ShikumiWatcher::new(&cli.file)?);
    let evaluator = Arc::new(SuiEvaluator::new());
    let attester = Arc::new(MockAttester::new());
    let publisher = Arc::new(MockPublisher::new());
    let anomaly_chain = Arc::new(MockAnomalyChain::new());
    let (anomaly_handler, anomaly_router) = mock_anomaly_router();
    let (apps, infra, promises, topology, ctrl) = mock_system_controller();
    let ctrl = ctrl.with_anomaly_chain(anomaly_chain.clone());

    // Optionally wire revoada FabricFace.
    if cli.with_revoada {
        use engenho_revoada::{FabricFace, FaceKind, PureRaftFace};
        let face = PureRaftFace::from_declaration(&FabricFace {
            name: "fonte".into(),
            kind: FaceKind::PureRaft,
        })
        .ok_or("PureRaftFace::from_declaration returned None")?;
        face.start()?;
        let face_arc: Arc<dyn Face> = Arc::new(face);
        let proposer = Arc::new(engenho_fonte::RevoadaProposer::new(face_arc.clone()));
        run_loop(
            Conduit::new(watcher, evaluator, proposer, attester, publisher),
            anomaly_chain,
            anomaly_router,
            anomaly_handler,
            apps,
            infra,
            promises,
            topology,
            Some(face_arc),
        )
        .await
    } else {
        let proposer = Arc::new(ctrl);
        run_loop(
            Conduit::new(watcher, evaluator, proposer, attester, publisher),
            anomaly_chain,
            anomaly_router,
            anomaly_handler,
            apps,
            infra,
            promises,
            topology,
            None,
        )
        .await
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_loop(
    conduit: Conduit,
    anomaly_chain: Arc<MockAnomalyChain>,
    anomaly_router: engenho_fonte::AnomalyRouter,
    anomaly_handler: Arc<engenho_fonte::MockAnomalyHandler>,
    apps: Arc<engenho_fonte::MockAppReconciler>,
    infra: Arc<engenho_fonte::MockInfraReconciler>,
    promises: Arc<engenho_fonte::MockPromessaReconciler>,
    topology: Arc<engenho_fonte::MockTopologyReconciler>,
    face: Option<Arc<dyn Face>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Shutdown handler: ctrl-c sets the cancel flag.
    let cancel = tokio_util_cancel::CancellationFlag::new();
    let cancel_for_signal = cancel.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("ctrl-c received, draining conduit then shutting down");
        cancel_for_signal.set();
    });

    let mut last_chain_len = 0usize;
    while !cancel.is_set() {
        match conduit.tick().await {
            Ok(Some(outcome)) => {
                tracing::info!(
                    revision = outcome.revision,
                    proposal_id = outcome.proposal_id,
                    receipt = %outcome.receipt_id,
                    "convergence tick completed"
                );

                // Route any newly chained anomalies.
                let entries = anomaly_chain.entries();
                for entry in &entries[last_chain_len..] {
                    if let Err(e) = anomaly_router.route(&entry.event).await {
                        tracing::warn!(?e, "anomaly routing failed");
                    }
                }
                last_chain_len = entries.len();

                tracing::info!(
                    apps = apps.log().len(),
                    infra = infra.log().len(),
                    promises = promises.log().len(),
                    topology = topology.log().len(),
                    anomalies = entries.len(),
                    routed = anomaly_handler.log().len(),
                    face_resources = face.as_ref().map(|f| f.resource_count()).unwrap_or(0),
                    "sub-reconciler + chain state"
                );
            }
            Ok(None) => {
                // No change pending; idle.
                sleep(Duration::from_millis(200)).await;
            }
            Err(e) => {
                tracing::error!(?e, "conduit tick failed; backing off");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    if let Some(face) = face {
        let _ = face.shutdown();
    }
    tracing::info!("engenho-fonte shut down cleanly");
    Ok(())
}

// Minimal cancellation flag (one bool, two methods). Could use
// tokio-util's CancellationToken but that's another dep — this is
// 8 lines.
mod tokio_util_cancel {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Clone, Default)]
    pub struct CancellationFlag(Arc<AtomicBool>);

    impl CancellationFlag {
        pub fn new() -> Self {
            Self::default()
        }
        pub fn set(&self) {
            self.0.store(true, Ordering::SeqCst);
        }
        pub fn is_set(&self) -> bool {
            self.0.load(Ordering::SeqCst)
        }
    }
}
