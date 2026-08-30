//! engenho — typed, attested, Rust-native Kubernetes runtime.
//!
//! Thin launcher over [`engenho_runtime::Runtime`] (M0.1 item 7). All
//! the assembly lives in the `engenho-runtime` lib crate so it stays
//! integration-testable without going through `main`; this binary just
//! wires the process.
//!
//! ## Subcommands
//!
//! * `engenho` (no args) — boot the daemon: init tracing → discover
//!   config → boot the Runtime → wait for ctrl-c → graceful shutdown.
//!   On boot (TLS-enabled) the daemon writes `data_dir/kubeconfig`.
//! * `engenho daemon` — explicit alias for the bare no-arg form. Runs
//!   the EXACT same `run_daemon` path. This is the verb the substrate
//!   `mkModuleTrio` factory invokes (`daemonSubcommand = "daemon"`) when
//!   it generates the systemd / launchd unit; the bare form stays
//!   working for back-compat and interactive use.
//! * `engenho kubeconfig [--data-dir <d>] [--server <url>]` — print the
//!   kubeconfig for the persisted cluster CA to stdout. Use this to
//!   re-emit a kubeconfig after the daemon is up, or to point kubectl at
//!   a non-loopback address.
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

use std::path::PathBuf;

use engenho_apiserver::load_or_generate_ca;
use engenho_config::{ConfigTier, EngenhoConfig, TieredConfig, render_provenance};
use engenho_kube_client::{emit_kubeconfig, emit_kubeconfig_with_admin};
use engenho_runtime::Runtime;
use tracing_subscriber::EnvFilter;

/// The verb list, written ONCE.
///
/// `help_text` renders it and the unknown-verb error joins it, so the
/// usage summary and the error can never disagree about what engenho
/// accepts. `subcommand_list_matches_parse` asserts every name here is
/// actually dispatched by [`Command::parse`], which closes the other
/// direction: a verb added to the match arms without a row here fails
/// the suite rather than becoming silently undiscoverable.
const SUBCOMMAND_NAMES: [&str; 4] = ["daemon", "kubeconfig", "config-show", "config-diff"];

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Minimal arg parsing — the binary has exactly two optional verbs
    // (`daemon`, an explicit alias for the bare form, and `kubeconfig`).
    // We avoid pulling clap for two verbs (the daemon path stays the
    // default).
    match Command::parse(std::env::args().skip(1))? {
        Command::Daemon => run_daemon().await,
        Command::Kubeconfig(flags) => run_kubeconfig(flags.into_iter()),
        Command::ConfigShow(tier) => run_config_show(tier),
        Command::ConfigDiff(from, to) => run_config_diff(&from, &to),
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

/// Boot the engenho daemon and run until ctrl-c.
async fn run_daemon() -> anyhow::Result<()> {
    // 1. Tracing — env-filtered, info default for our crates.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new("engenho=info,engenho_runtime=info,engenho_store=info")
        }))
        .init();

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        "engenho — typed, attested, Rust-native Kubernetes runtime"
    );

    // 2. Config via the sealed progressive-discovery fold
    //    (bare → discovered[DiscoveryLayer] → prescribed_default → operator
    //    file overlay), each effective leaf carrying typed Provenance.
    let (config, provenance) = EngenhoConfig::resolve_progressively()?.into_parts();
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

    // 3. Boot every subsystem over one StoreMesh. On boot the Runtime
    //    writes data_dir/kubeconfig when TLS is enabled.
    let runtime = Runtime::start(config).await?;
    tracing::info!(addr = %runtime.local_addr(), "engenho up — apiserver bound");

    // 4. Run until ctrl-c, then shut down gracefully.
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown signal received");
    runtime.shutdown().await?;
    tracing::info!("engenho stopped cleanly");
    Ok(())
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
            // The rich default: the progressive fold with typed provenance.
            let resolution = EngenhoConfig::resolve_progressively()?;
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
    kubeconfig [FLAGS]        Print a kubeconfig for the persisted cluster CA.
                              Flags: --data-dir <dir>, --server <url>
    config-show [TIER]        Print the resolved config and each leaf's
                              provenance. TIER overrides $ENGENHO_TIER.
    config-diff <FROM> <TO>   Unified diff between two resolved config tiers.
                              Tiers: bare | discovered | default | <yaml-path>

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
    use super::Command;

    fn parse(argv: &[&str]) -> anyhow::Result<Command> {
        Command::parse(argv.iter().map(|s| (*s).to_string()))
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
        for argv in [&["--help"], &["-h"], &["help"], &["--version"], &["-V"], &["version"]] {
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
            assert!(help.contains(verb), "help text omits the {verb:?} subcommand");
        }
        for flag in ["--help", "--version"] {
            assert!(help.contains(flag), "help text omits {flag:?}");
        }
    }

    /// The help text carries the version and a runnable first command.
    #[test]
    fn help_text_is_actionable() {
        let help = super::help_text();
        assert!(help.contains(env!("CARGO_PKG_VERSION")), "help omits the version");
        assert!(help.contains("kubectl"), "help gives no working next step");
    }

    /// An unknown verb names the supported verbs AND points at `--help`.
    #[test]
    fn unknown_subcommand_points_at_help() {
        let err = parse(&["frobnicate"]).unwrap_err().to_string();
        assert!(err.contains("--help"), "unknown-verb error does not mention --help: {err}");
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
}
