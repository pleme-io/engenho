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
//! * `engenho kubeconfig [--data-dir <d>] [--server <url>]` — print the
//!   kubeconfig for the persisted cluster CA to stdout. Use this to
//!   re-emit a kubeconfig after the daemon is up, or to point kubectl at
//!   a non-loopback address.

use std::path::PathBuf;

use engenho_apiserver::load_or_generate_ca;
use engenho_config::EngenhoConfig;
use engenho_kube_client::emit_kubeconfig;
use engenho_runtime::Runtime;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Minimal arg parsing — the binary has exactly one optional
    // subcommand (`kubeconfig`); everything else is the daemon. We avoid
    // pulling clap for a single verb (the daemon path stays the default).
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("kubeconfig") => run_kubeconfig(args),
        Some(other) => Err(anyhow::anyhow!(
            "unknown subcommand {other:?} (supported: kubeconfig, or no args to boot the daemon)"
        )),
        None => run_daemon().await,
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

    let yaml = emit_kubeconfig(&config.cluster.name, &server_url, ca.cert_pem().as_bytes())
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
