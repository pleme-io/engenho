//! engenho-mcp stdio entrypoint — mirrors the zoekt-mcp / shinryu-mcp
//! shape. All tracing goes to stderr because stdout owns the MCP
//! JSON-RPC protocol.

use std::sync::Arc;

use anyhow::Result;
use rmcp::ServiceExt;
use rmcp::transport::stdio;
use tracing_subscriber::EnvFilter;

use engenho_mcp::{EngenhoMcp, KikaiClusterReader};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("engenho_mcp=info")),
        )
        .with_writer(std::io::stderr)
        .init();

    tracing::info!("engenho-mcp starting (stdio transport)");

    let reader = KikaiClusterReader::from_env()
        .map_err(|e| anyhow::anyhow!("failed to initialise cluster reader: {e}"))?;
    let server = EngenhoMcp::new(Arc::new(reader));

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
