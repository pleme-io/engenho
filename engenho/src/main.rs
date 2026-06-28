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

use std::path::PathBuf;

use engenho_apiserver::load_or_generate_ca;
use engenho_config::EngenhoConfig;
use engenho_kube_client::{emit_kubeconfig, emit_kubeconfig_with_admin};
use engenho_runtime::Runtime;
use tracing_subscriber::EnvFilter;

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
}

impl Command {
    /// Classify `engenho`'s argv (already skipping argv[0]).
    ///
    /// * no args → [`Command::Daemon`]
    /// * `daemon` → [`Command::Daemon`] (explicit alias — same path)
    /// * `kubeconfig …` → [`Command::Kubeconfig`] with the remaining args
    /// * anything else → an error naming the supported verbs
    fn parse(mut args: impl Iterator<Item = String>) -> anyhow::Result<Self> {
        match args.next().as_deref() {
            None | Some("daemon") => Ok(Command::Daemon),
            Some("kubeconfig") => Ok(Command::Kubeconfig(args.collect())),
            Some(other) => Err(anyhow::anyhow!(
                "unknown subcommand {other:?} (supported: daemon [or no args] to boot the daemon, kubeconfig)"
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
    }
}

/// Boot the engenho daemon and run until ctrl-c.
async fn run_daemon() -> anyhow::Result<()> {
    // 1. Tracing — env-filtered, info default for our crates.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                EnvFilter::new("engenho=info,engenho_runtime=info,engenho_store=info")
            }),
        )
        .init();

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        "engenho — typed, attested, Rust-native Kubernetes runtime"
    );

    // 2. Config via the shikumi discovery cascade
    //    ($ENGENHO_CONFIG → XDG → /etc → prescribed_default).
    let config = EngenhoConfig::discover()?;
    tracing::info!(
        cluster = %config.cluster.name,
        node = %config.runtime.node_name,
        listen = %config.runtime.listen_addr,
        durable = config.runtime.durable,
        tls = config.runtime.tls.enabled,
        "loaded config"
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
    let ca =
        load_or_generate_ca(&data_dir).map_err(|e| anyhow::anyhow!("load cluster CA: {e}"))?;

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
        assert!(err.contains("frobnicate"), "error should name the bad verb: {err}");
        assert!(err.contains("daemon"), "error should list `daemon`: {err}");
        assert!(err.contains("kubeconfig"), "error should list `kubeconfig`: {err}");
    }
}
