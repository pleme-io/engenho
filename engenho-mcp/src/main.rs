//! engenho-mcp stdio entrypoint — mirrors the zoekt-mcp / shinryu-mcp
//! shape. All tracing goes to stderr because stdout owns the MCP
//! JSON-RPC protocol.
//!
//! `engenho-mcp [--allow-mutate]`: without the flag the server observes;
//! with it, it also offers this machine's engenho's mutate-tier control
//! operations. Nothing offers the destructive ones.

use std::sync::Arc;

use anyhow::Result;
use rmcp::ServiceExt;
use rmcp::transport::stdio;
use tracing_subscriber::EnvFilter;

use engenho_mcp::{Authority, EngenhoMcp, Grant, KikaiClusterReader};

/// The authority the arguments grant. Anything but `--allow-mutate` is
/// refused rather than ignored: a mistyped flag must not quietly launch a
/// server with less (or more) than was meant.
fn authority(args: impl IntoIterator<Item = String>) -> Result<Authority> {
    let mut authority = Authority::Observe;
    for arg in args {
        match arg.as_str() {
            "--allow-mutate" => {
                authority = Authority::LocalMutate {
                    granted_by: Grant::LaunchFlag,
                };
            }
            other => anyhow::bail!("unknown argument {other:?}; engenho-mcp takes --allow-mutate"),
        }
    }
    Ok(authority)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("engenho_mcp=info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let authority = authority(std::env::args().skip(1))?;
    tracing::info!(?authority, "engenho-mcp starting (stdio transport)");

    let reader = KikaiClusterReader::from_env()
        .map_err(|e| anyhow::anyhow!("failed to initialise cluster reader: {e}"))?;
    let server = EngenhoMcp::new(Arc::new(reader), authority);

    let service = server
        .serve(stdio())
        .await
        .map_err(|e| anyhow::anyhow!("serve: {e}"))?;
    service
        .waiting()
        .await
        .map_err(|e| anyhow::anyhow!("waiting: {e}"))?;

    tracing::info!("engenho-mcp exiting");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_mutate_flag_is_accepted() {
        let args = |a: &[&str]| a.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
        assert_eq!(authority(args(&[])).unwrap(), Authority::Observe);
        assert_eq!(
            authority(args(&["--allow-mutate"])).unwrap(),
            Authority::LocalMutate {
                granted_by: Grant::LaunchFlag
            }
        );
        assert!(authority(args(&["--allow-mutat"])).is_err());
        assert!(authority(args(&["--allow-destructive"])).is_err());
    }
}
