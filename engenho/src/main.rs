//! engenho — typed, attested, Rust-native Kubernetes runtime.
//!
//! Thin launcher over [`engenho_runtime::Runtime`] (M0.1 item 7). All
//! the assembly lives in the `engenho-runtime` lib crate so it stays
//! integration-testable without going through `main`; this binary just
//! wires the process.
//!
//! ## Subcommands
//!
//! * `engenho` (no args) — run the daemon: init tracing → place the control
//!   state (`data_dir/control/`) → a [`Supervisor`] boots the Runtime and
//!   stays up above it, retrying a failed boot on backoff (or holding it
//!   until the declared config changes) → SIGTERM or SIGINT (see
//!   [`StopCause`]) drains the runtime → exit 0. An exit requested with
//!   relaunch exits 75 for the service manager to start a fresh process.
//!   On boot (TLS-enabled) the runtime writes `data_dir/kubeconfig`.
//! * `engenho daemon` — explicit alias for the bare no-arg form. Runs
//!   the EXACT same `run_daemon` path. This is the verb the substrate
//!   `mkModuleTrio` factory invokes (`daemonSubcommand = "daemon"`) when
//!   it generates the systemd / launchd unit; the bare form stays
//!   working for back-compat and interactive use.
//! * `engenho kubeconfig [--data-dir <d>] [--server <url>]` — print the
//!   kubeconfig for the persisted cluster CA to stdout. Use this to
//!   re-emit a kubeconfig after the daemon is up, or to point kubectl at
//!   a non-loopback address.
//! * `engenho census --predicate <name> --kubeconfig <path>` or
//!   `engenho census --predicate <name> --data-dir <dir>` — run one named
//!   check from the census catalog (plan T0.10) over a running apiserver or
//!   over a copy of a node's data directory, read-only, and print what it
//!   matched with counts by kind and reason. `engenho census --list` prints
//!   the catalog. See [`engenho_runtime::census`].
//! * `engenho --help` / `-h` / `help` — print the usage summary.
//! * `engenho --version` / `-V` / `version` — print the version.
//!
//! ## Why `--help` is a subcommand and not a flag
//!
//! The bare no-arg form boots the daemon, so argv[1] is the ONLY
//! dispatch position and `--help` classifies there like any other verb.
//! Before this existed, `engenho --help` fell through to the unknown-verb
//! arm and printed an `anyhow` error plus a stack backtrace — i.e. the
//! first two commands anyone runs against a fresh install both looked
//! like a crash. Keep both spellings: `--help` is what a stranger types,
//! `help` is what someone used to subcommand-style CLIs types.

// The daemon's stop contract IS a unix signal: systemd and launchd stop a
// unit with SIGTERM, a terminal with SIGINT. A build with no way to receive
// SIGTERM would be a daemon every service-manager stop crashes, so there is
// no such build. (Every CI lane is ubuntu or macOS.)
#[cfg(not(unix))]
compile_error!("the engenho daemon is unix-only: its stop path is SIGTERM/SIGINT (see StopCause)");

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use engenho_apiserver::load_or_generate_ca;
use engenho_config::{
    ConfigError, ConfigTier, EngenhoConfig, OverrideLayer, SocketDefaults, TieredConfig,
    render_provenance,
};
use engenho_control_server::{
    AuditLog, AuthorizedSet, ControlIdentity, GrantPolicy, Pins, RemoteListener, Router, identity,
    serve_uds,
};
use engenho_kube_client::{emit_kubeconfig, emit_kubeconfig_with_admin};
use engenho_runtime::census::{self, ApiSource, Catalog, DataDirSource, Predicate};
use engenho_runtime::control::{
    DaemonControl, DaemonControlParts, LogLayer, OverrideStore, RemoteFacts, SocketFacts,
};
use engenho_runtime::lifecycle::{
    ConfigSource, ControlBootstrap, ControlDir, ExitIntent, ResolvedConfig, Supervisor,
    SupervisorConfig,
};
use engenho_serve::stop_channel;
use tokio::signal::unix::{Signal, SignalKind, signal};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod ctl;
mod remote;

/// The verb list, written ONCE.
///
/// `help_text` renders it and the unknown-verb error joins it, so the
/// usage summary and the error can never disagree about what engenho
/// accepts. `subcommand_list_matches_parse` asserts every name here is
/// actually dispatched by [`Command::parse`], which closes the other
/// direction: a verb added to the match arms without a row here fails
/// the suite rather than becoming silently undiscoverable.
const SUBCOMMAND_NAMES: [&str; 7] = [
    "daemon",
    "ctl",
    "remote",
    "kubeconfig",
    "config-show",
    "config-diff",
    "census",
];

/// The parsed top-level command. Splitting the argv classification out of
/// `main` keeps it unit-testable without booting the daemon or touching
/// the process environment — `Command::parse` is a pure function over the
/// argument stream.
#[derive(Debug, PartialEq, Eq)]
enum Command {
    /// Boot the daemon — the bare no-arg form AND the explicit `daemon`
    /// verb both resolve here, so `engenho` == `engenho daemon`.
    Daemon,
    /// `kubeconfig [flags...]` — carries the trailing flags verbatim for
    /// `run_kubeconfig` to parse.
    Kubeconfig(Vec<String>),
    /// `config-show [tier]` — print the resolved config (and, for the
    /// `default` tier, its per-leaf provenance). The optional tier arg
    /// overrides `$ENGENHO_TIER`.
    ConfigShow(Option<String>),
    /// `config-diff <from> <to>` — unified diff between two resolved tiers.
    ConfigDiff(String, String),
    /// `census …` — one named census check, or the catalog.
    Census(CensusCommand),
    /// `ctl …` — talk to the running daemon over its control socket; the
    /// arguments are parsed by [`ctl::CtlCommand::parse`].
    Ctl(Vec<String>),
    /// `remote …` — this machine's keys for remote daemons ([`remote::run`]).
    Remote(Vec<String>),
    /// `--help` / `-h` / `help` — print usage to stdout and exit 0.
    Help,
    /// `--version` / `-V` / `version` — print the version to stdout and exit 0.
    Version,
}

impl Command {
    /// Classify `engenho`'s argv (already skipping argv[0]).
    ///
    /// * no args → [`Command::Daemon`]
    /// * `daemon` → [`Command::Daemon`] (explicit alias — same path)
    /// * `kubeconfig …` → [`Command::Kubeconfig`] with the remaining args
    /// * `config-show [tier]` → [`Command::ConfigShow`]
    /// * `config-diff <from> <to>` → [`Command::ConfigDiff`]
    /// * `census …` → [`Command::Census`]
    /// * `--help` / `-h` / `help` → [`Command::Help`]
    /// * `--version` / `-V` / `version` → [`Command::Version`]
    /// * anything else → an error naming the supported verbs
    fn parse(mut args: impl Iterator<Item = String>) -> anyhow::Result<Self> {
        match args.next().as_deref() {
            None | Some("daemon") => Ok(Command::Daemon),
            Some("--help" | "-h" | "help") => Ok(Command::Help),
            Some("--version" | "-V" | "version") => Ok(Command::Version),
            Some("kubeconfig") => Ok(Command::Kubeconfig(args.collect())),
            Some("config-show") => Ok(Command::ConfigShow(args.next())),
            Some("census") => Ok(Command::Census(CensusCommand::parse(args)?)),
            Some("ctl") => Ok(Command::Ctl(args.collect())),
            Some("remote") => Ok(Command::Remote(args.collect())),
            Some("config-diff") => match (args.next(), args.next()) {
                (Some(from), Some(to)) => Ok(Command::ConfigDiff(from, to)),
                _ => Err(anyhow::anyhow!(
                    "config-diff requires two tier args: config-diff <from> <to> (bare|discovered|default|<yaml-path>)"
                )),
            },
            Some(other) => Err(anyhow::anyhow!(
                "unknown subcommand {other:?} (supported: {}) — run `engenho --help` for usage",
                SUBCOMMAND_NAMES.join(", ")
            )),
        }
    }
}

/// Why the daemon is stopping: one arm per signal [`StopSignals`] subscribes
/// to, and nothing else.
///
/// ★ WHY SIGTERM IS HERE. It is the signal `systemctl stop` and
/// `launchctl kickstart -k` send. The daemon used to subscribe to SIGINT
/// alone (`tokio::signal::ctrl_c`), so SIGTERM met the default disposition:
/// the process died on the signal, `Runtime::shutdown` never ran and the
/// store was never terminated — every service-manager stop was a crash.
///
/// Every mapping below is an exhaustive match, so a new arm cannot be added
/// without naming its kernel signal and its log name. Not covered, by
/// decision: SIGHUP and SIGQUIT are not subscribed and keep their default
/// dispositions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopCause {
    /// SIGINT — ctrl-c at an interactive terminal.
    Interrupt,
    /// SIGTERM — a service manager stopping the unit.
    Terminate,
}

impl StopCause {
    /// The kernel signal this cause arrives as.
    fn kind(self) -> SignalKind {
        match self {
            Self::Interrupt => SignalKind::interrupt(),
            Self::Terminate => SignalKind::terminate(),
        }
    }

    /// The conventional signal name — what an operator greps the log for.
    const fn signal_name(self) -> &'static str {
        match self {
            Self::Interrupt => "SIGINT",
            Self::Terminate => "SIGTERM",
        }
    }

    /// Register this cause's signal with tokio. From this call on, for the
    /// rest of the process, the signal no longer kills it — tokio never
    /// restores the default disposition — and each delivery is recorded on
    /// the returned stream.
    fn subscribe(self) -> Result<Signal, SignalSubscribeError> {
        signal(self.kind()).map_err(|source| SignalSubscribeError {
            cause: self,
            source,
        })
    }
}

impl fmt::Display for StopCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.signal_name())
    }
}

/// Registering a stop signal failed, so the daemon refuses to boot: a daemon
/// that could not hear its stop signal would die on it instead.
#[derive(Debug)]
struct SignalSubscribeError {
    cause: StopCause,
    source: std::io::Error,
}

impl fmt::Display for SignalSubscribeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cannot subscribe to {} (the daemon would die on it instead of stopping cleanly)",
            self.cause
        )
    }
}

impl std::error::Error for SignalSubscribeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// The daemon's stop signals, subscribed once before boot and held until the
/// stop, so neither is ever fatal while the daemon runs.
///
/// Subscribing BEFORE `Runtime::start` means a stop requested during boot is
/// recorded and honoured the moment boot completes, through the one clean
/// path (`Runtime::shutdown` needs a booted runtime). A boot that never
/// completes is escalated by the service manager's kill timeout, as it was
/// before.
struct StopSignals {
    interrupt: Signal,
    terminate: Signal,
}

impl StopSignals {
    fn subscribe() -> Result<Self, SignalSubscribeError> {
        Ok(Self {
            interrupt: StopCause::Interrupt.subscribe()?,
            terminate: StopCause::Terminate.subscribe()?,
        })
    }

    /// The next stop signal to arrive.
    ///
    /// Cancel-safe, because `Signal::recv` is: `run_daemon` selects on it in
    /// a loop, and a signal delivered while another branch runs stays
    /// recorded on its stream until the next call.
    ///
    /// A stream that can no longer deliver (`recv` yields `None`, which
    /// happens only once tokio's signal driver is gone) is NOT read as a
    /// stop: its arm is disabled, and with both gone this pends forever —
    /// the absence of a signal is not a request to stop.
    async fn next(&mut self) -> StopCause {
        tokio::select! {
            Some(()) = self.interrupt.recv() => StopCause::Interrupt,
            Some(()) = self.terminate.recv() => StopCause::Terminate,
            else => std::future::pending().await,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Minimal arg parsing — the binary has exactly two optional verbs
    // (`daemon`, an explicit alias for the bare form, and `kubeconfig`).
    // We avoid pulling clap for two verbs (the daemon path stays the
    // default).
    match Command::parse(std::env::args().skip(1))? {
        Command::Daemon => {
            let intent = run_daemon().await?;
            tracing::info!(?intent, code = intent.code(), "engenho exiting");
            std::process::exit(intent.code())
        }
        Command::Kubeconfig(flags) => run_kubeconfig(flags.into_iter()),
        Command::ConfigShow(tier) => run_config_show(tier),
        Command::ConfigDiff(from, to) => run_config_diff(&from, &to),
        Command::Census(census) => run_census(census).await,
        Command::Ctl(args) => std::process::exit(i32::from(ctl::run(args).await)),
        Command::Remote(args) => std::process::exit(i32::from(remote::run(&args))),
        Command::Help => {
            print!("{}", help_text());
            Ok(())
        }
        Command::Version => {
            println!("engenho {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
    }
}

/// Run the engenho daemon: a [`Supervisor`] that stays up above the runtime,
/// booting it, retrying it and stopping it, until SIGTERM, SIGINT or an
/// exit request ends the process. Returns how it should end.
async fn run_daemon() -> anyhow::Result<ExitIntent> {
    // 1. Tracing — env-filtered, info default for our crates: to stdout, and
    //    into the ring the control plane serves (`engenho ctl logs list`).
    let directives = std::env::var("RUST_LOG")
        .unwrap_or_else(|_| "engenho=info,engenho_runtime=info,engenho_store=info".into());
    let filter = || EnvFilter::try_new(&directives).unwrap_or_else(|_| EnvFilter::new("info"));
    let (log_layer, logs) = LogLayer::new();
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(filter()))
        .with(log_layer.with_filter(filter()))
        .init();

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        "engenho — typed, attested, Rust-native Kubernetes runtime"
    );

    // Subscribe to the stop signals before anything durable happens, so
    // from here on neither SIGTERM nor SIGINT can kill the process mid-boot.
    let mut stop = StopSignals::subscribe()?;

    // 2. Where the daemon keeps its own state and where its control socket
    //    is, decided even when the config is broken — that is the case the
    //    supervisor stays up (and the socket stays reachable) for.
    let bootstrap = ControlBootstrap::discover();
    let euid = engenho_control_server::daemon_euid();
    let socket_config = bootstrap.control.socket.clone();
    let socket_path = socket_config.resolved_path(&SocketDefaults::for_process(euid));
    tracing::info!(
        data_dir = %bootstrap.data_dir.display(),
        source = ?bootstrap.data_dir_source,
        socket = %socket_path.display(),
        "control state"
    );

    // 3. The supervisor. Each boot resolves the config afresh, inside its
    //    own phase — the declared file with the override tier folded over
    //    it — so a config that does not resolve is a failed boot the daemon
    //    reports and retries, not a dead process.
    let data_dir = bootstrap.data_dir.clone();
    let overrides = Arc::new(OverrideStore::open(ControlDir::under(&data_dir).root()));
    let source = ConfigSource::layered(resolve_declared_config, overrides);
    let (supervisor, handle) = Supervisor::new(SupervisorConfig {
        data_dir: data_dir.clone(),
        source: source.clone(),
        backend: None,
        declared: bootstrap.declared.clone(),
    })?;

    // 4. The remote listener's own identity and the pins it admits — the
    //    identity made whether or not remote control is enabled, so
    //    `engenho ctl control show` names the pin a client needs before it
    //    is turned on. Neither depends on anything a boot creates.
    let identity =
        ControlIdentity::load_or_create(&ControlDir::under(&data_dir).root().join(identity::DIR))
            .map(Arc::new)
            .map_err(|e| e.to_string());
    if let Err(why) = &identity {
        tracing::warn!(error = %why, "no control identity: remote control is unavailable");
    }
    let remote_config = bootstrap.control.remote.clone();
    let pins = Pins::new(
        AuthorizedSet::from_config(&remote_config.authorized_clients).unwrap_or_default(),
    );
    let (remote_state, remote_watch) = RemoteListener::channel();

    // 5. The local control socket, bound before the first boot: fatal if
    //    it cannot be, since it is the recovery path.
    let audit = Arc::new(AuditLog::open(
        &data_dir.join(ControlDir::NAME).join("audit"),
    )?);
    let control = DaemonControl::new(DaemonControlParts {
        supervisor: handle.clone(),
        source,
        data_dir,
        data_dir_source: bootstrap.data_dir_source,
        declared: bootstrap.declared,
        socket: SocketFacts {
            path: socket_path.clone(),
            access: socket_config.access,
            group_tier: socket_config.group_tier,
        },
        remote: RemoteFacts {
            state: remote_watch,
            identity: identity
                .as_ref()
                .map(|id| (id.spki(), id.created_at()))
                .map_err(Clone::clone),
            pins: pins.clone(),
        },
        logs,
        audit: Arc::clone(&audit),
        git_rev: option_env!("ENGENHO_GIT_REV")
            .unwrap_or("unknown")
            .to_owned(),
    });
    let bound = engenho_control_server::bind(&socket_path, socket_config.access, euid)?;
    let router = Arc::new(Router::new(
        Arc::new(control),
        GrantPolicy::new(euid, socket_config.access, socket_config.group_tier),
        audit,
    ));
    let (control_stop, control_signal) = stop_channel();
    let serving = tokio::spawn(serve_uds(
        bound.listener,
        Arc::clone(&router),
        control_signal.clone(),
        engenho_control_server::GRACE,
    ));
    tracing::info!(socket = %socket_path.display(), "control socket serving");

    // 6. The remote listener: never fatal — what keeps it from serving is its
    //    state, which `engenho ctl control show` reports. Its pins follow the
    //    declared file.
    let remote = RemoteListener::spawn(
        &remote_config,
        identity,
        pins.clone(),
        router,
        control_signal,
        remote_state,
    );
    tokio::spawn(follow_pins(handle.declared_changes(), pins));

    // 7. A stop signal is an exit request: the supervisor drains the runtime
    //    (if it is up) and the process exits 0. The signal streams live
    //    OUTSIDE the loop, so a signal that lands while a request is in
    //    flight stays recorded and is not lost.
    tokio::spawn(async move {
        loop {
            let cause = stop.next().await;
            tracing::info!(signal = %cause, "shutdown signal received");
            if let Err(err) = handle.exit(ExitIntent::Halt).await {
                tracing::warn!(signal = %cause, error = %err, "exit request not taken");
            }
        }
    });

    let intent = supervisor.run().await;
    // The exit request's own answer is on its way out: drain, then close.
    control_stop.stop();
    if let Err(err) = serving.await {
        tracing::warn!(error = %err, "the control socket's server ended abnormally");
    }
    remote.stopped().await;
    drop(bound.guard);
    Ok(intent)
}

/// Keep the remote listener's pins what the declared file says: a client
/// added or revoked there is admitted or refused from its next request,
/// without a restart. (Turning the listener on or moving it takes one.)
async fn follow_pins(mut changes: tokio::sync::watch::Receiver<u64>, pins: Pins) {
    while changes.changed().await.is_ok() {
        let remote = ControlBootstrap::discover().control.remote;
        match AuthorizedSet::from_config(&remote.authorized_clients) {
            Ok(set) => {
                tracing::info!(
                    clients = remote.authorized_clients.len(),
                    "remote control pins reloaded"
                );
                pins.replace(set);
            }
            Err(why) => {
                tracing::warn!(error = %why, "the declared remote pins do not parse; keeping those in force");
            }
        }
    }
}

/// The config one boot runs on, via the sealed progressive-discovery fold
/// (bare → discovered[`DiscoveryLayer`] → `prescribed_default` → declared
/// file → override tier), each effective leaf carrying typed provenance.
fn resolve_declared_config(
    overrides: Option<&OverrideLayer>,
) -> Result<ResolvedConfig, ConfigError> {
    let (config, provenance) = EngenhoConfig::resolve_progressively_with(overrides)?.into_parts();
    tracing::info!(
        cluster = %config.cluster.name,
        node = %config.runtime.node_name,
        listen = %config.runtime.listen_addr,
        durable = config.runtime.durable,
        tls = config.runtime.tls.enabled,
        "loaded config"
    );
    // Surface provenance: which tiers contributed, and where the node name
    // came from (Discovered when the host reported a name, else the Default
    // fallback). `engenho config-show` prints the full per-leaf breakdown.
    let node_name_tier = provenance
        .provenance_of(&["runtime", "node_name"])
        .map_or("?", |p| p.tier().as_str());
    tracing::info!(
        leaves = provenance.len(),
        tiers = %provenance
            .contributing_tiers()
            .iter()
            .map(|t| t.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        node_name_from = node_name_tier,
        "config provenance",
    );
    Ok(ResolvedConfig {
        config,
        provenance: Some(provenance),
    })
}

/// `engenho kubeconfig [--data-dir <d>] [--server <url>]` — load the
/// persisted cluster CA from `<data_dir>/pki/ca.crt` and print a
/// kubeconfig to stdout. `--server` overrides the default
/// `https://127.0.0.1:<listen_port>` (use it to point at a non-loopback
/// address). The CA is the SAME one the running daemon's server cert
/// chains to, so the emitted kubeconfig verifies the live server.
fn run_kubeconfig(args: impl Iterator<Item = String>) -> anyhow::Result<()> {
    // Resolve config first so the data_dir + cluster name + listen port
    // defaults come from the operator's discovered config.
    let config = EngenhoConfig::discover()?;

    let mut data_dir: Option<PathBuf> = None;
    let mut server: Option<String> = None;
    let mut args = args.peekable();
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--data-dir" => {
                data_dir = Some(PathBuf::from(args.next().ok_or_else(|| {
                    anyhow::anyhow!("--data-dir requires a path argument")
                })?));
            }
            "--server" => {
                server = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--server requires a URL argument"))?,
                );
            }
            other => {
                return Err(anyhow::anyhow!(
                    "unknown flag {other:?} (supported: --data-dir, --server)"
                ));
            }
        }
    }

    let data_dir = data_dir.unwrap_or_else(|| config.runtime.data_dir.clone());
    // The persisted CA. `load_or_generate_ca` LOADS when the CA already
    // exists (the daemon minted it at first boot); it only generates if
    // absent — handing out the kubeconfig before the daemon's first boot.
    let ca = load_or_generate_ca(&data_dir).map_err(|e| anyhow::anyhow!("load cluster CA: {e}"))?;

    // Default server URL: loopback + the configured listen port. The
    // operator can override with --server for a non-loopback address.
    let server_url = server.unwrap_or_else(|| default_server_url(&config));

    // If the daemon already minted + persisted the admin client cert (it does
    // at first TLS boot), emit a kubeconfig that authenticates as the admin
    // identity (→ `kubectl auth whoami` = engenho-admin). Otherwise fall back
    // to the anonymous-token kubeconfig (pre-first-boot / plaintext).
    let pki = data_dir.join("pki");
    let admin_cert = std::fs::read(pki.join("admin.crt")).ok();
    let admin_key = std::fs::read(pki.join("admin.key")).ok();
    let yaml = match (admin_cert, admin_key) {
        (Some(cert), Some(key)) => emit_kubeconfig_with_admin(
            &config.cluster.name,
            &server_url,
            ca.cert_pem().as_bytes(),
            &cert,
            &key,
        ),
        _ => emit_kubeconfig(&config.cluster.name, &server_url, ca.cert_pem().as_bytes()),
    }
    .map_err(|e| anyhow::anyhow!("emit kubeconfig: {e}"))?;
    print!("{yaml}");
    Ok(())
}

/// `https://127.0.0.1:<port>` where `<port>` is the configured
/// `listen_addr`'s port (or 6443 if it can't be parsed). Loopback because
/// `127.0.0.1` is always a server-cert SAN.
fn default_server_url(config: &EngenhoConfig) -> String {
    let port = config
        .runtime
        .listen_addr
        .rsplit(':')
        .next()
        .and_then(|p| p.parse::<u16>().ok())
        .filter(|p| *p != 0)
        .unwrap_or(6443);
    let mut url = String::from("https://127.0.0.1:");
    url.push_str(&port.to_string());
    url
}

/// `engenho config-show [bare|discovered|default|<yaml-path>]` — resolve the
/// named config tier and print its YAML to stdout. With no arg the tier comes
/// from `$ENGENHO_TIER` (default: `default`). The `default` tier resolves
/// through the sealed progressive fold and additionally prints a per-leaf
/// provenance summary (which tier produced each effective value).
fn run_config_show(tier_arg: Option<String>) -> anyhow::Result<()> {
    let tier = match tier_arg {
        Some(s) => ConfigTier::from_str_or_default(&s),
        None => ConfigTier::from_env("ENGENHO_TIER"),
    };
    match tier {
        ConfigTier::Default => {
            // The rich default: the progressive fold with typed provenance,
            // the override tier included — found where the daemon keeps it,
            // under the data directory the bootstrap decides.
            let bootstrap = ControlBootstrap::discover();
            let overrides = OverrideStore::open(ControlDir::under(&bootstrap.data_dir).root());
            let layer = match overrides.layer() {
                Ok(layer) => Some(layer),
                Err(err) => {
                    eprintln!("engenho: without the override tier: {err}");
                    None
                }
            };
            let resolution = EngenhoConfig::resolve_progressively_with(layer.as_ref())?;
            print!("{}", resolution.value().to_yaml()?);
            print!("{}", render_provenance(resolution.provenance()));
        }
        other => {
            // Bare / Discovered / Custom(path): a single tier, no fold.
            print!("{}", EngenhoConfig::resolve_tier(other).to_yaml()?);
        }
    }
    Ok(())
}

/// `engenho config-diff <from> <to>` — resolve two config tiers
/// (`bare|discovered|default|<yaml-path>`) and print a unified diff of their
/// YAML (shikumi `ConfigDiff`). Answers "what changes between these tiers?".
fn run_config_diff(from: &str, to: &str) -> anyhow::Result<()> {
    let from_cfg = EngenhoConfig::resolve_tier(ConfigTier::from_str_or_default(from));
    let to_cfg = EngenhoConfig::resolve_tier(ConfigTier::from_str_or_default(to));
    // diff_against(baseline): from → to.
    print!("{}", to_cfg.diff_against(&from_cfg).render_unified());
    Ok(())
}

/// `engenho census …`, parsed: the catalog, or one predicate over one
/// source. The predicate is a catalog row by construction — `--predicate`
/// is looked up, never evaluated.
#[derive(Debug, PartialEq, Eq)]
enum CensusCommand {
    /// `--list`: print the catalog.
    List,
    /// `--predicate <name>` over one source.
    Run {
        /// The catalog row to run.
        predicate: Predicate,
        /// Where to read.
        source: CensusSource,
    },
}

/// Where a census reads: exactly one of the two, so "both" and "neither"
/// are parse errors, not run-time choices.
#[derive(Debug, PartialEq, Eq)]
enum CensusSource {
    /// `--kubeconfig <path>`: LIST through the apiserver it names.
    Apiserver(PathBuf),
    /// `--data-dir <dir>`: a copy of a stopped node's data directory.
    DataDir(PathBuf),
}

/// Why `engenho census …` could not be parsed.
#[derive(Debug, PartialEq, Eq)]
enum CensusUsage {
    /// A flag that takes a value came last.
    MissingValue(&'static str),
    /// `--predicate` named no catalog row.
    UnknownPredicate(String),
    /// A flag the census does not take.
    UnknownFlag(String),
    /// A flag given twice.
    Repeated(&'static str),
    /// No `--predicate` and no `--list`.
    NoPredicate,
    /// Neither `--kubeconfig` nor `--data-dir`.
    NoSource,
    /// Both `--kubeconfig` and `--data-dir`.
    TwoSources,
    /// `--list` with another flag.
    ListTakesNothing,
}

impl fmt::Display for CensusUsage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingValue(flag) => write!(f, "census: {flag} needs a value"),
            Self::UnknownPredicate(name) => {
                write!(f, "census: no predicate {name:?} in the catalog (")?;
                for (i, p) in Predicate::ALL.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    f.write_str(p.name())?;
                }
                f.write_str(") — run `engenho census --list`")
            }
            Self::UnknownFlag(flag) => write!(
                f,
                "census: unknown flag {flag:?} (supported: --predicate, --kubeconfig, --data-dir, --list)"
            ),
            Self::Repeated(flag) => write!(f, "census: {flag} given twice"),
            Self::NoPredicate => {
                f.write_str("census: name a check with --predicate <name>, or run `census --list`")
            }
            Self::NoSource => f.write_str(
                "census: name a source: --kubeconfig <path> (a running apiserver) or \
                 --data-dir <dir> (a copy of a stopped node's data directory)",
            ),
            Self::TwoSources => {
                f.write_str("census: --kubeconfig and --data-dir are two sources; name one")
            }
            Self::ListTakesNothing => f.write_str("census: --list takes no other flag"),
        }
    }
}

impl std::error::Error for CensusUsage {}

impl CensusCommand {
    /// Parse the arguments after `census`.
    fn parse(mut args: impl Iterator<Item = String>) -> Result<Self, CensusUsage> {
        let mut list = false;
        let mut predicate: Option<Predicate> = None;
        let mut kubeconfig: Option<PathBuf> = None;
        let mut data_dir: Option<PathBuf> = None;
        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--list" => list = true,
                "--predicate" => {
                    let name = args
                        .next()
                        .ok_or(CensusUsage::MissingValue("--predicate"))?;
                    let p =
                        Predicate::from_name(&name).ok_or(CensusUsage::UnknownPredicate(name))?;
                    if predicate.replace(p).is_some() {
                        return Err(CensusUsage::Repeated("--predicate"));
                    }
                }
                "--kubeconfig" => {
                    let path = args
                        .next()
                        .ok_or(CensusUsage::MissingValue("--kubeconfig"))?;
                    if kubeconfig.replace(PathBuf::from(path)).is_some() {
                        return Err(CensusUsage::Repeated("--kubeconfig"));
                    }
                }
                "--data-dir" => {
                    let path = args.next().ok_or(CensusUsage::MissingValue("--data-dir"))?;
                    if data_dir.replace(PathBuf::from(path)).is_some() {
                        return Err(CensusUsage::Repeated("--data-dir"));
                    }
                }
                _ => return Err(CensusUsage::UnknownFlag(flag)),
            }
        }
        match (list, predicate, kubeconfig, data_dir) {
            (true, None, None, None) => Ok(Self::List),
            (true, ..) => Err(CensusUsage::ListTakesNothing),
            (false, None, ..) => Err(CensusUsage::NoPredicate),
            (false, Some(_), None, None) => Err(CensusUsage::NoSource),
            (false, Some(_), Some(_), Some(_)) => Err(CensusUsage::TwoSources),
            (false, Some(predicate), Some(path), None) => Ok(Self::Run {
                predicate,
                source: CensusSource::Apiserver(path),
            }),
            (false, Some(predicate), None, Some(dir)) => Ok(Self::Run {
                predicate,
                source: CensusSource::DataDir(dir),
            }),
        }
    }
}

/// `engenho census …` — print the catalog, or run one predicate over one
/// source and print its report. Read-only either way: an apiserver is only
/// listed, and a data directory is copied before its store is booted.
///
/// A census that ran prints its report and exits 0 whatever it counted —
/// zero matches is a finding. One that could not read its source exits
/// non-zero and prints nothing on stdout.
async fn run_census(command: CensusCommand) -> anyhow::Result<()> {
    let (predicate, source) = match command {
        CensusCommand::List => {
            print!("{Catalog}");
            return Ok(());
        }
        CensusCommand::Run { predicate, source } => (predicate, source),
    };
    // Diagnostics go to stderr, so stdout carries the report alone.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .init();
    let report = match source {
        CensusSource::Apiserver(kubeconfig) => {
            let source = ApiSource::from_kubeconfig(&kubeconfig).await?;
            census::run(predicate, &source).await?
        }
        CensusSource::DataDir(dir) => {
            let source = DataDirSource::open(&dir).await?;
            let report = census::run(predicate, &source).await;
            let closed = source.close().await;
            let report = report?;
            closed?;
            report
        }
    };
    print!("{report}");
    Ok(())
}

/// The usage summary printed by `engenho --help`.
///
/// Kept as a pure function returning a `String` so a test can assert on
/// its content without capturing stdout. The trailing example is
/// deliberate: a stranger's next question after "what is this" is "how do
/// I see it work", and the answer should be one copyable line rather than
/// a link.
fn help_text() -> String {
    format!(
        "\
engenho {version} — a Rust-native Kubernetes runtime.

Runs the control plane as a single binary: API server, scheduler, controllers
and kubelet in one process. Real kubectl drives it.

USAGE:
    engenho [SUBCOMMAND]

SUBCOMMANDS:
    daemon                    Boot the runtime. This is the default when no
                              subcommand is given, so bare `engenho` runs it.
    ctl <RESOURCE> <VERB>     Talk to the running daemon over its control
                              socket: its lifecycle, boots, init state,
                              config, children, PKI, store, logs and audit.
                              `ctl --list` prints every resource and verb;
                              `ctl --remote <NAME>` talks to another machine's.
    remote list | keygen <NAME> | fingerprint <NAME>
                              This machine's keys for remote daemons, and the
                              daemons remotes.yaml names.
    kubeconfig [FLAGS]        Print a kubeconfig for the persisted cluster CA.
                              Flags: --data-dir <dir>, --server <url>
    config-show [TIER]        Print the resolved config and each leaf's
                              provenance. TIER overrides $ENGENHO_TIER.
    config-diff <FROM> <TO>   Unified diff between two resolved config tiers.
                              Tiers: bare | discovered | default | <yaml-path>
    census --predicate <NAME> (--kubeconfig <PATH> | --data-dir <DIR>)
                              Count the live objects a planned rule would
                              refuse, read-only, over a running apiserver or a
                              copy of a stopped node's data directory.
                              `census --list` prints every predicate.

OPTIONS:
    -h, --help                Print this message.
    -V, --version             Print the version.

GETTING STARTED:
    engenho &                                    # boot the runtime
    engenho kubeconfig > /tmp/engenho.kubeconfig # write a kubeconfig
    KUBECONFIG=/tmp/engenho.kubeconfig kubectl get --raw /api

Docs: https://github.com/pleme-io/engenho
",
        version = env!("CARGO_PKG_VERSION"),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        CensusCommand, CensusSource, CensusUsage, Command, Predicate, StopCause, StopSignals,
    };

    fn parse(argv: &[&str]) -> anyhow::Result<Command> {
        Command::parse(argv.iter().map(|s| (*s).to_string()))
    }

    /// Each stop cause is logged under its conventional signal name, so the
    /// log says WHICH signal stopped the daemon rather than just that one did.
    #[test]
    fn stop_cause_is_logged_by_signal_name() {
        assert_eq!(StopCause::Interrupt.to_string(), "SIGINT");
        assert_eq!(StopCause::Terminate.to_string(), "SIGTERM");
    }

    /// A delivered SIGTERM is read as [`StopCause::Terminate`] and a SIGINT
    /// as [`StopCause::Interrupt`] — and, because both are subscribed first,
    /// raising either one does not kill this test process.
    ///
    /// One test on purpose: tokio fans a signal out to every subscriber in
    /// the process, so two signal tests running concurrently would read each
    /// other's signals.
    #[tokio::test]
    async fn each_subscribed_signal_is_read_as_its_own_cause() {
        use nix::sys::signal::{Signal, raise};
        use std::time::Duration;

        let mut stop = StopSignals::subscribe().expect("subscribe the stop signals");
        for (signal, cause) in [
            (Signal::SIGTERM, StopCause::Terminate),
            (Signal::SIGINT, StopCause::Interrupt),
        ] {
            raise(signal).expect("raise the signal");
            let read = tokio::time::timeout(Duration::from_secs(5), stop.next())
                .await
                .expect("a raised stop signal is delivered");
            assert_eq!(read, cause, "{signal:?} was read as the wrong cause");
        }
    }

    /// The bare no-arg form boots the daemon.
    #[test]
    fn bare_no_args_is_daemon() {
        assert_eq!(parse(&[]).unwrap(), Command::Daemon);
    }

    /// Every spelling of help classifies as [`Command::Help`].
    ///
    /// REGRESSION: `--help` and `--version` used to fall through to the
    /// unknown-subcommand arm, so both printed an `anyhow` error and a
    /// stack backtrace. They are the first two things anyone runs against
    /// an unfamiliar binary, and both looked like a crash.
    #[test]
    fn every_help_spelling_is_help() {
        for argv in [&["--help"], &["-h"], &["help"]] {
            assert_eq!(parse(argv).unwrap(), Command::Help, "argv {argv:?}");
        }
    }

    /// Every spelling of version classifies as [`Command::Version`].
    #[test]
    fn every_version_spelling_is_version() {
        for argv in [&["--version"], &["-V"], &["version"]] {
            assert_eq!(parse(argv).unwrap(), Command::Version, "argv {argv:?}");
        }
    }

    /// Neither help nor version may be mistaken for the daemon — booting a
    /// control plane because someone asked for usage is the worst possible
    /// reading of that argv.
    #[test]
    fn help_and_version_never_boot_the_daemon() {
        for argv in [
            &["--help"],
            &["-h"],
            &["help"],
            &["--version"],
            &["-V"],
            &["version"],
        ] {
            assert_ne!(parse(argv).unwrap(), Command::Daemon, "argv {argv:?}");
        }
    }

    /// The help text names every verb `Command::parse` accepts.
    ///
    /// This is the anti-drift row: adding a subcommand without listing it
    /// here fails, so the usage summary cannot silently fall behind the
    /// dispatch table the way the README fell behind the code.
    #[test]
    fn help_text_names_every_subcommand() {
        let help = super::help_text();
        for verb in super::SUBCOMMAND_NAMES {
            assert!(
                help.contains(verb),
                "help text omits the {verb:?} subcommand"
            );
        }
        for flag in ["--help", "--version"] {
            assert!(help.contains(flag), "help text omits {flag:?}");
        }
    }

    /// The help text carries the version and a runnable first command.
    #[test]
    fn help_text_is_actionable() {
        let help = super::help_text();
        assert!(
            help.contains(env!("CARGO_PKG_VERSION")),
            "help omits the version"
        );
        assert!(help.contains("kubectl"), "help gives no working next step");
    }

    /// An unknown verb names the supported verbs AND points at `--help`.
    #[test]
    fn unknown_subcommand_points_at_help() {
        let err = parse(&["frobnicate"]).unwrap_err().to_string();
        assert!(
            err.contains("--help"),
            "unknown-verb error does not mention --help: {err}"
        );
    }

    /// THE ANTI-DRIFT ROW. Every verb in `SUBCOMMAND_NAMES` is really
    /// dispatched, and the error message really lists all of them.
    ///
    /// Adding a match arm without a const row, or a const row without an
    /// arm, fails here — which is what keeps `--help` and the error from
    /// falling behind the dispatch table the way the README fell behind
    /// the code.
    #[test]
    fn subcommand_list_matches_parse() {
        let err = parse(&["frobnicate"]).unwrap_err().to_string();
        for verb in super::SUBCOMMAND_NAMES {
            // The verb must be RECOGNISED, not necessarily complete:
            // `config-diff` alone is a legitimate arity error. What may
            // never happen is an advertised verb reported as unknown.
            if let Err(e) = parse(&[verb]) {
                let msg = e.to_string();
                assert!(
                    !msg.contains("unknown subcommand"),
                    "{verb:?} is advertised but Command::parse rejects it as unknown: {msg}"
                );
            }
            assert!(
                err.contains(verb),
                "unknown-verb error omits the advertised verb {verb:?}: {err}"
            );
        }
    }

    /// `engenho daemon` is an explicit alias that resolves to the SAME
    /// daemon path as the bare form — this is the verb the substrate
    /// `mkModuleTrio` factory wires into the systemd/launchd unit.
    #[test]
    fn daemon_subcommand_is_daemon() {
        assert_eq!(parse(&["daemon"]).unwrap(), Command::Daemon);
    }

    /// `engenho` and `engenho daemon` classify identically — back-compat
    /// with the bare form is preserved alongside the explicit verb.
    #[test]
    fn bare_and_daemon_subcommand_agree() {
        assert_eq!(parse(&[]).unwrap(), parse(&["daemon"]).unwrap());
    }

    /// `kubeconfig` carries its trailing flags through verbatim.
    #[test]
    fn kubeconfig_subcommand_carries_flags() {
        assert_eq!(
            parse(&["kubeconfig", "--data-dir", "/var/lib/engenho-rio"]).unwrap(),
            Command::Kubeconfig(vec![
                "--data-dir".to_string(),
                "/var/lib/engenho-rio".to_string(),
            ]),
        );
    }

    /// An unrecognized verb is an error naming the supported verbs.
    #[test]
    fn unknown_subcommand_errors() {
        let err = parse(&["frobnicate"]).unwrap_err().to_string();
        assert!(
            err.contains("frobnicate"),
            "error should name the bad verb: {err}"
        );
        assert!(err.contains("daemon"), "error should list `daemon`: {err}");
        assert!(
            err.contains("kubeconfig"),
            "error should list `kubeconfig`: {err}"
        );
        assert!(
            err.contains("config-show"),
            "error should list `config-show`: {err}"
        );
        assert!(
            err.contains("config-diff"),
            "error should list `config-diff`: {err}"
        );
    }

    /// `config-show` with no tier arg honors `$ENGENHO_TIER` at run time.
    #[test]
    fn config_show_without_tier() {
        assert_eq!(parse(&["config-show"]).unwrap(), Command::ConfigShow(None));
    }

    /// `config-show <tier>` carries the tier selector through.
    #[test]
    fn config_show_with_tier() {
        assert_eq!(
            parse(&["config-show", "bare"]).unwrap(),
            Command::ConfigShow(Some("bare".to_string())),
        );
    }

    /// `config-diff <from> <to>` carries both tier selectors through.
    #[test]
    fn config_diff_carries_two_tiers() {
        assert_eq!(
            parse(&["config-diff", "bare", "default"]).unwrap(),
            Command::ConfigDiff("bare".to_string(), "default".to_string()),
        );
    }

    /// `config-diff` with fewer than two args is a usage error.
    #[test]
    fn config_diff_requires_two_args() {
        assert!(parse(&["config-diff", "bare"]).is_err());
        assert!(parse(&["config-diff"]).is_err());
    }

    fn census(argv: &[&str]) -> Result<CensusCommand, CensusUsage> {
        CensusCommand::parse(argv.iter().map(|s| (*s).to_string()))
    }

    /// `census` dispatches to the census; each predicate runs over exactly
    /// one named source, whichever order the flags come in.
    #[test]
    fn census_takes_one_catalog_predicate_over_one_source() {
        assert_eq!(
            parse(&["census", "--list"]).unwrap(),
            Command::Census(CensusCommand::List)
        );
        for p in Predicate::ALL {
            assert_eq!(
                census(&["--predicate", p.name(), "--data-dir", "/copy"]).unwrap(),
                CensusCommand::Run {
                    predicate: *p,
                    source: CensusSource::DataDir("/copy".into()),
                }
            );
        }
        assert_eq!(
            census(&["--kubeconfig", "/kc", "--predicate", "node-overcommitted"]).unwrap(),
            CensusCommand::Run {
                predicate: Predicate::NodeOvercommitted,
                source: CensusSource::Apiserver("/kc".into()),
            }
        );
    }

    /// A predicate outside the catalog is refused, and the refusal names the
    /// catalog: there is no string the census would evaluate instead.
    #[test]
    fn census_refuses_a_predicate_outside_the_catalog() {
        let err = census(&["--predicate", "spec.replicas > 3", "--data-dir", "/d"]).unwrap_err();
        assert_eq!(
            err,
            CensusUsage::UnknownPredicate("spec.replicas > 3".into())
        );
        let msg = err.to_string();
        for p in Predicate::ALL {
            assert!(msg.contains(p.name()), "the refusal omits {p}: {msg}");
        }
    }

    /// Neither source, both sources, a missing value, an unknown flag, a
    /// repeated flag and `--list` with company are each their own refusal.
    #[test]
    fn census_refuses_every_malformed_invocation() {
        let p = "deployment-would-roll";
        for (argv, want) in [
            (vec!["--predicate", p], CensusUsage::NoSource),
            (
                vec!["--predicate", p, "--kubeconfig", "/k", "--data-dir", "/d"],
                CensusUsage::TwoSources,
            ),
            (vec!["--data-dir", "/d"], CensusUsage::NoPredicate),
            (vec![], CensusUsage::NoPredicate),
            (
                vec!["--predicate"],
                CensusUsage::MissingValue("--predicate"),
            ),
            (
                vec!["--predicate", p, "--data-dir"],
                CensusUsage::MissingValue("--data-dir"),
            ),
            (
                vec!["--predicate", p, "--predicate", p, "--data-dir", "/d"],
                CensusUsage::Repeated("--predicate"),
            ),
            (
                vec!["--frobnicate"],
                CensusUsage::UnknownFlag("--frobnicate".into()),
            ),
            (
                vec!["--list", "--data-dir", "/d"],
                CensusUsage::ListTakesNothing,
            ),
        ] {
            assert_eq!(census(&argv).unwrap_err(), want, "argv {argv:?}");
        }
    }
}
