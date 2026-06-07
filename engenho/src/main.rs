//! engenho — typed, attested, Rust-native Kubernetes runtime.
//!
//! Thin launcher over [`engenho_runtime::Runtime`] (M0.1 item 7). All
//! the assembly lives in the `engenho-runtime` lib crate so it stays
//! integration-testable without going through `main`; this binary just
//! wires the process: init tracing → discover config → boot the
//! Runtime → wait for ctrl-c → graceful shutdown.

use engenho_config::EngenhoConfig;
use engenho_runtime::Runtime;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
        "loaded config"
    );

    // 3. Boot every subsystem over one StoreMesh.
    let runtime = Runtime::start(config).await?;
    tracing::info!(addr = %runtime.local_addr(), "engenho up — apiserver bound");

    // 4. Run until ctrl-c, then shut down gracefully.
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown signal received");
    runtime.shutdown().await?;
    tracing::info!("engenho stopped cleanly");
    Ok(())
}
