//! `engenho-fonte` — the fonte convergence loop run as a process, over
//! a mock universe.
//!
//! ## What it is today
//!
//! A harness, not a daemon that converges anything. It watches a
//! `(defsistema …)` Nix file ([`ShikumiWatcher`], real) and evaluates
//! each change with sui ([`SuiEvaluator`], real). Everything after that
//! is an in-process stand-in:
//!
//! - the proposer is a `SystemController` over four `Mock*Reconciler`s,
//!   or, with `--with-revoada`, a single-node `PureRaftFace` whose store
//!   is an in-memory map;
//! - the attester is `MockAttester`, the publisher `MockPublisher`;
//! - drift goes to a `MockAnomalyChain` and is routed to a
//!   `MockAnomalyHandler`.
//!
//! Nothing it records survives the process.
//!
//! So the binary is fenced. Cargo builds it only with
//! `--features mock-universe`, and at startup it resolves the
//! [`Universe`] of every slot it wired and logs it. Each tick is logged
//! in the words of that universe: under `mock`, a tick is never
//! reported as a convergence.
//!
//! ## Usage
//!
//! ```bash
//! cargo run -p engenho-fonte-cli --features mock-universe -- --file ./sistemas/rio.nix
//! engenho-fonte --file ./sistemas/rio.nix --with-revoada
//! engenho-fonte --file ./sistemas/rio.nix --log-level debug
//! ```

#[cfg(not(feature = "mock-universe"))]
compile_error!(
    "engenho-fonte wires in-process mock roles; build it with `--features mock-universe` to accept that"
);

mod universe;

use clap::Parser;
use engenho_fonte::{
    AnomalyRouter, Conduit, FonteError, MockAnomalyChain, MockAnomalyHandler, MockAppReconciler,
    MockAttester, MockInfraReconciler, MockPromessaReconciler, MockPublisher,
    MockTopologyReconciler, Outcome, Proposer, RevoadaProposer, ShikumiWatcher, SuiEvaluator,
    mock_anomaly_router, mock_system_controller,
};
use engenho_revoada::face::{Face, FaceError};
use engenho_revoada::{FabricFace, FaceKind, PureRaftFace};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use tokio::time::{Duration, sleep};
use universe::{Universe, Wiring, of};

#[derive(Parser, Debug)]
#[command(
    version,
    about = "engenho-fonte — the fonte convergence loop over a mock universe"
)]
struct Cli {
    /// Path to the (defsistema …) Nix file to watch.
    #[arg(long, short = 'f')]
    file: PathBuf,

    /// Propose into an in-process, single-node revoada PureRaftFace
    /// instead of the SystemController over mock sub-reconcilers.
    /// Either way the proposer is a mock.
    #[arg(long, default_value_t = false)]
    with_revoada: bool,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, default_value = "info")]
    log_level: String,
}

/// Why the daemon could not be wired.
#[derive(Debug, thiserror::Error)]
enum StartError {
    /// The declaration file could not be read or watched.
    #[error("engenho-fonte/wire: {0}")]
    Watch(FonteError),
    /// A `PureRaft` declaration did not construct a `PureRaftFace`.
    #[error("engenho-fonte/wire: the revoada face declaration is not PureRaft")]
    FaceDeclaration,
    /// The revoada face refused to start.
    #[error("engenho-fonte/wire: revoada face start: {0}")]
    FaceStart(FaceError),
}

/// The four mock sub-reconcilers behind the `SystemController`, kept so
/// the per-tick state line can report what each one recorded.
struct Reconcilers {
    apps: Arc<MockAppReconciler>,
    infra: Arc<MockInfraReconciler>,
    promises: Arc<MockPromessaReconciler>,
    topology: Arc<MockTopologyReconciler>,
}

/// Everything [`wire`] built, and the universe it resolved from it.
struct Daemon {
    conduit: Conduit,
    wiring: Wiring,
    reconcilers: Reconcilers,
    anomaly_chain: Arc<MockAnomalyChain>,
    anomaly_router: AnomalyRouter,
    anomaly_handler: Arc<MockAnomalyHandler>,
    face: Option<Arc<dyn Face>>,
    /// How many anomaly-chain entries have been routed so far.
    routed: usize,
}

/// What one pass of the loop did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    /// The conduit carried one change through to an outcome.
    Ticked,
    /// No change was pending.
    Idle,
    /// The conduit returned an error; the loop backs off.
    Failed,
}

/// Build every role, and resolve each slot's universe from the concrete
/// type placed in it.
fn wire(file: &Path, with_revoada: bool) -> Result<Daemon, StartError> {
    let watcher = ShikumiWatcher::new(file).map_err(StartError::Watch)?;
    let evaluator = SuiEvaluator::new();
    let attester = MockAttester::new();
    let publisher = MockPublisher::new();
    let anomaly_chain = Arc::new(MockAnomalyChain::new());
    let (anomaly_handler, anomaly_router) = mock_anomaly_router();
    let (apps, infra, promises, topology, ctrl) = mock_system_controller();

    let (proposer, proposer_universe, face): (Arc<dyn Proposer>, Universe, Option<Arc<dyn Face>>) =
        if with_revoada {
            let face = PureRaftFace::from_declaration(&FabricFace {
                name: "fonte".into(),
                kind: FaceKind::PureRaft,
            })
            .ok_or(StartError::FaceDeclaration)?;
            face.start().map_err(StartError::FaceStart)?;
            let universe = of(&face);
            let face: Arc<dyn Face> = Arc::new(face);
            (
                Arc::new(RevoadaProposer::new(face.clone())),
                universe,
                Some(face),
            )
        } else {
            let universe =
                Universe::resolve([of(&*apps), of(&*infra), of(&*promises), of(&*topology)]);
            (
                Arc::new(ctrl.with_anomaly_chain(anomaly_chain.clone())),
                universe,
                None,
            )
        };

    let wiring = Wiring {
        watcher: of(&watcher),
        evaluator: of(&evaluator),
        proposer: proposer_universe,
        attester: of(&attester),
        publisher: of(&publisher),
        anomaly_chain: of(&*anomaly_chain),
        remediation: of(&*anomaly_handler),
    };
    let conduit = Conduit::new(
        Arc::new(watcher),
        Arc::new(evaluator),
        proposer,
        Arc::new(attester),
        Arc::new(publisher),
    );
    Ok(Daemon {
        conduit,
        wiring,
        reconcilers: Reconcilers {
            apps,
            infra,
            promises,
            topology,
        },
        anomaly_chain,
        anomaly_router,
        anomaly_handler,
        face,
        routed: 0,
    })
}

impl Daemon {
    /// Log the universe resolved for every slot. Under `mock` this is a
    /// warning: the process is a harness, and no tick it runs is a
    /// convergence.
    fn announce(&self, file: &Path) {
        let universe = self.wiring.universe();
        let Wiring {
            watcher,
            evaluator,
            proposer,
            attester,
            publisher,
            anomaly_chain,
            remediation,
        } = self.wiring;
        match universe {
            Universe::Mock => tracing::warn!(
                file = %file.display(),
                %universe,
                %watcher,
                %evaluator,
                %proposer,
                %attester,
                %publisher,
                %anomaly_chain,
                %remediation,
                "engenho-fonte starting in a mock universe: every slot marked mock is an \
                 in-process stand-in, so no tick is a convergence"
            ),
            Universe::Real => tracing::info!(
                file = %file.display(),
                %universe,
                %watcher,
                %evaluator,
                %proposer,
                %attester,
                %publisher,
                %anomaly_chain,
                %remediation,
                "engenho-fonte starting"
            ),
        }
    }

    /// Run one tick of the conduit and report it in the words of the
    /// resolved universe.
    async fn step(&mut self) -> Step {
        match self.conduit.tick().await {
            Ok(Some(outcome)) => {
                log_tick(self.wiring.universe(), &outcome);
                self.route_anomalies().await;
                tracing::info!(
                    apps = self.reconcilers.apps.log().len(),
                    infra = self.reconcilers.infra.log().len(),
                    promises = self.reconcilers.promises.log().len(),
                    topology = self.reconcilers.topology.log().len(),
                    anomalies = self.routed,
                    routed = self.anomaly_handler.log().len(),
                    face_resources = self.face.as_ref().map_or(0, |f| f.resource_count()),
                    "sub-reconciler + chain state"
                );
                Step::Ticked
            }
            Ok(None) => Step::Idle,
            Err(e) => {
                tracing::error!(error = %e, "conduit tick failed; backing off");
                Step::Failed
            }
        }
    }

    /// Route every anomaly-chain entry not routed yet.
    async fn route_anomalies(&mut self) {
        let entries = self.anomaly_chain.entries();
        for entry in entries.iter().skip(self.routed) {
            if let Err(e) = self.anomaly_router.route(&entry.event).await {
                tracing::warn!(error = %e, "anomaly routing failed");
            }
        }
        self.routed = entries.len();
    }
}

/// One line per tick, worded by the universe it ran in. A mock tick
/// only passed a declaration through stand-ins, so it is never called
/// a convergence.
fn log_tick(universe: Universe, outcome: &Outcome) {
    match universe {
        Universe::Mock => tracing::info!(
            %universe,
            revision = outcome.revision,
            proposal_id = outcome.proposal_id,
            receipt = %outcome.receipt_id,
            "tick handled by a mock universe; not a convergence"
        ),
        Universe::Real => tracing::info!(
            %universe,
            revision = outcome.revision,
            proposal_id = outcome.proposal_id,
            receipt = %outcome.receipt_id,
            "tick proposed, attested and published the declaration"
        ),
    }
}

async fn run(mut daemon: Daemon) {
    // Shutdown handler: ctrl-c sets the cancel flag.
    let cancel = tokio_util_cancel::CancellationFlag::new();
    let cancel_for_signal = cancel.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("ctrl-c received, draining conduit then shutting down");
        cancel_for_signal.set();
    });

    while !cancel.is_set() {
        match daemon.step().await {
            Step::Ticked => {}
            Step::Idle => sleep(Duration::from_millis(200)).await,
            Step::Failed => sleep(Duration::from_secs(1)).await,
        }
    }

    if let Some(face) = &daemon.face
        && let Err(e) = face.shutdown()
    {
        tracing::warn!(error = %e, "revoada face shutdown failed");
    }
    tracing::info!("engenho-fonte shut down cleanly");
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::new(format!("info,engenho_fonte={}", cli.log_level))
    });
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    match wire(&cli.file, cli.with_revoada) {
        Ok(daemon) => {
            daemon.announce(&cli.file);
            run(daemon).await;
            ExitCode::SUCCESS
        }
        Err(e) => {
            tracing::error!(error = %e, "engenho-fonte could not start");
            ExitCode::FAILURE
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::Mutex;

    /// A Sistema small enough to read, valid enough for sui to type.
    const SISTEMA: &str = r#"{
        name = "rio";
        apps = [ { name = "podinfo"; version = null; } ];
        infra = [];
        promises = [];
        topology = { strategy = "solo"; nodes = 1; };
    }"#;

    /// What the binary wires today, slot by slot.
    const WIRED_TODAY: Wiring = Wiring {
        watcher: Universe::Real,
        evaluator: Universe::Real,
        proposer: Universe::Mock,
        attester: Universe::Mock,
        publisher: Universe::Mock,
        anomaly_chain: Universe::Mock,
        remediation: Universe::Mock,
    };

    fn declaration() -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().expect("tempfile");
        file.write_all(SISTEMA.as_bytes()).expect("write sistema");
        file
    }

    /// Every log line emitted while `f` runs, as plain text.
    fn captured<T>(f: impl FnOnce() -> T) -> (T, String) {
        let sink = Arc::new(Mutex::new(Vec::<u8>::new()));
        let writer = sink.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .with_writer(move || SinkWriter(writer.clone()))
            .finish();
        let out = tracing::subscriber::with_default(subscriber, f);
        let text = String::from_utf8_lossy(&sink.lock().expect("sink")).into_owned();
        (out, text)
    }

    struct SinkWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for SinkWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("sink").extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn the_binary_wires_a_mock_universe() {
        let file = declaration();
        let daemon = wire(file.path(), false).expect("wire");
        assert_eq!(daemon.wiring, WIRED_TODAY);
        assert_eq!(daemon.wiring.universe(), Universe::Mock);
    }

    #[test]
    fn with_revoada_the_proposer_is_still_a_mock() {
        let file = declaration();
        let daemon = wire(file.path(), true).expect("wire");
        assert_eq!(daemon.wiring, WIRED_TODAY);
        assert_eq!(daemon.wiring.universe(), Universe::Mock);
        if let Some(face) = &daemon.face {
            face.shutdown().expect("face shutdown");
        }
    }

    #[test]
    fn startup_warns_that_the_universe_is_mock() {
        let file = declaration();
        let daemon = wire(file.path(), false).expect("wire");
        let ((), log) = captured(|| daemon.announce(file.path()));
        assert!(log.contains("WARN"), "startup under mock must warn: {log}");
        assert!(log.contains("universe=mock"), "{log}");
        for slot in [
            "proposer=mock",
            "attester=mock",
            "publisher=mock",
            "watcher=real",
        ] {
            assert!(log.contains(slot), "missing {slot}: {log}");
        }
        assert!(log.contains("no tick is a convergence"), "{log}");
    }

    #[test]
    fn a_mock_tick_is_never_logged_as_a_convergence() {
        let file = declaration();
        let mut daemon = wire(file.path(), false).expect("wire");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let (step, log) = captured(|| runtime.block_on(daemon.step()));
        assert_eq!(step, Step::Ticked, "the declaration must tick: {log}");
        assert_eq!(daemon.reconcilers.apps.log().len(), 1, "{log}");
        assert!(
            log.contains("tick handled by a mock universe; not a convergence"),
            "{log}"
        );
        assert!(log.contains("universe=mock"), "{log}");
        assert!(!log.contains("convergence tick completed"), "{log}");
    }

    #[test]
    fn a_missing_declaration_is_a_typed_start_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("absent.nix");
        match wire(&missing, false) {
            Err(StartError::Watch(FonteError::Watch(_))) => {}
            Err(other) => panic!("wrong error: {other}"),
            Ok(_) => panic!("wired a daemon over a file that does not exist"),
        }
    }
}
