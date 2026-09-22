//! The control tools against a real control socket: a call reaches the
//! daemon's router and comes back in the daemon's own words, capped at the
//! ceiling the server was launched with; no daemon is blind, never empty.

use std::path::Path;
use std::sync::Arc;

use engenho_config::{GroupTier, SocketAccess};
use engenho_control_server::{GRACE, GrantPolicy, NoAudit, Router, bind, daemon_euid, serve_uds};
use engenho_control_types::ops::Unserved;
use engenho_control_types::{AuthorityTier, CATALOG, OperationId, OperationSpec};
use engenho_mcp::control::call;
use engenho_serve::stop_channel;
use rmcp::model::{CallToolResult, JsonObject};

fn row(id: OperationId) -> &'static OperationSpec {
    CATALOG.iter().find(|r| r.id == id).expect("in the catalog")
}

fn text(result: &CallToolResult) -> String {
    result
        .content
        .iter()
        .filter_map(|c| c.as_text().map(|t| t.text.clone()))
        .collect()
}

async fn at(socket: &Path, id: OperationId, ceiling: AuthorityTier) -> CallToolResult {
    call(socket, row(id), JsonObject::new(), ceiling).await
}

#[tokio::test]
async fn a_tool_call_reaches_the_daemon_capped_at_the_launch() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let socket = tmp.path().join("ctl.sock");
    let bound = bind(&socket, SocketAccess::Owner, daemon_euid()).expect("bind");
    // Behind the real grant: the socket's owner is entitled to everything,
    // so only the ceiling the call carries can hold it back.
    let router = Arc::new(Router::new(
        Arc::new(Unserved),
        GrantPolicy::new(daemon_euid(), SocketAccess::Owner, GroupTier::Observe),
        Arc::new(NoAudit),
    ));
    let (stop, signal) = stop_channel();
    let serving = tokio::spawn(serve_uds(bound.listener, router, signal, GRACE));

    // Observe reaches the handler: `Unserved` answers every operation with
    // a refusal of its own.
    let shown = at(&socket, OperationId::GetRuntime, AuthorityTier::Observe).await;
    assert_eq!(shown.is_error, Some(true));
    assert!(text(&shown).contains("\"unsupported\""), "{}", text(&shown));

    // A mutate operation under an observe ceiling is refused by the daemon's
    // grant, before any handler — the cap is the daemon's, not the tool list's.
    let stopped = at(&socket, OperationId::StopRuntime, AuthorityTier::Observe).await;
    assert!(
        text(&stopped).contains("\"insufficient_authority\""),
        "{}",
        text(&stopped)
    );
    // Under a mutate ceiling it passes the grant and reaches the handler.
    let allowed = at(&socket, OperationId::StopRuntime, AuthorityTier::Mutate).await;
    assert!(
        text(&allowed).contains("\"unsupported\""),
        "{}",
        text(&allowed)
    );

    // Nothing listening: blind, never an empty success.
    let nowhere = at(
        &tmp.path().join("nothing.sock"),
        OperationId::GetRuntime,
        AuthorityTier::Observe,
    )
    .await;
    assert_eq!(nowhere.is_error, Some(true));
    assert!(text(&nowhere).contains("\"blind\""), "{}", text(&nowhere));

    stop.stop();
    let _ = serving.await;
}
