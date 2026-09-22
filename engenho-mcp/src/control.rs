//! The daemon's control plane as MCP tools, generated from the spec's
//! operation catalog.
//!
//! Every operation an agent may call is one tool, `control_<resource>_<verb>`
//! (the operation's `engenho ctl` spelling), taking the operation's path,
//! query and header parameters as named arguments and its body as `body`,
//! and calling this machine's engenho over its control socket — the same
//! socket resolution, request rendering and response the `engenho ctl`
//! client uses. Nothing about any one operation is written here: a new
//! operation in the spec is a new tool, or deliberately none.
//!
//! **Which operations become tools** is the [`Authority`] the server was
//! launched with, over the catalog's tier column:
//!
//! * observe operations, always;
//! * mutate operations, only with `--allow-mutate` ([`Authority::LocalMutate`]);
//! * destructive operations, never — the confirmation handshake proves the
//!   operation is aimed at the right cluster, not that a human typed it;
//! * sensitive operations, never.
//!
//! The daemon enforces the same cap on its own: every call says
//! `Engenho-Actor: agent` and `Engenho-Ceiling: <tier>`, so a tool list
//! that were somehow wider would still be refused at the daemon.

use std::path::Path;
use std::sync::Arc;

use engenho_control_client::{ClientError, ControlClient, render, resolve_socket};
use engenho_control_types::wire::HttpParts;
use engenho_control_types::{AuthorityTier, CATALOG, ControlError, OperationSpec, ParamLocation};
use rmcp::handler::server::router::tool::ToolRoute;
use rmcp::handler::server::tool::ToolCallContext;
use rmcp::model::{CallToolResult, Content, JsonObject, Tool};

use crate::server::EngenhoMcp;
use crate::writer::Authority;

/// The actor every control call declares.
pub const ACTOR: &str = "agent";

/// The operations `authority` offers as tools.
pub fn exposed(authority: &Authority) -> impl Iterator<Item = &'static OperationSpec> + '_ {
    CATALOG.iter().filter(move |row| offered(row, authority))
}

/// Whether `row` is a tool under `authority`.
#[must_use]
pub fn offered(row: &OperationSpec, authority: &Authority) -> bool {
    !row.sensitive
        && match row.tier {
            AuthorityTier::Observe => true,
            AuthorityTier::Mutate => authority.control_ceiling() == AuthorityTier::Mutate,
            AuthorityTier::Destructive => false,
        }
}

/// `row`'s tool name: its `engenho ctl` spelling.
#[must_use]
pub fn tool_name(row: &OperationSpec) -> String {
    [
        "control_",
        row.cli.resource,
        "_",
        &row.cli.verb.replace('-', "_"),
    ]
    .concat()
}

/// `row`'s input: each parameter by name (the path ones required), and the
/// body as one object when it takes one. The daemon's own parser checks
/// the values.
#[must_use]
pub fn input_schema(row: &OperationSpec) -> JsonObject {
    let mut properties = serde_json::Map::new();
    let mut required = Vec::new();
    for param in row.params {
        let place = match param.location {
            ParamLocation::Path => {
                required.push(serde_json::Value::String(param.name.to_owned()));
                "path"
            }
            ParamLocation::Query => "query",
            ParamLocation::Header => "header",
        };
        let description = [place, " parameter `", param.name, "` of ", row.id.as_str()].concat();
        properties.insert(
            param.name.to_owned(),
            serde_json::json!({ "type": "string", "description": description }),
        );
    }
    if row.body {
        let description = [
            "the request body of ",
            row.id.as_str(),
            ", as the control API's spec shapes it",
        ]
        .concat();
        properties.insert(
            "body".into(),
            serde_json::json!({ "type": "object", "description": description }),
        );
    }
    let mut schema = serde_json::Map::new();
    schema.insert("type".into(), "object".into());
    schema.insert("properties".into(), serde_json::Value::Object(properties));
    schema.insert("required".into(), serde_json::Value::Array(required));
    schema
}

/// `row` as a tool.
#[must_use]
pub fn route(row: &'static OperationSpec) -> ToolRoute<EngenhoMcp> {
    let tier = row.tier.to_string();
    let description = [
        row.id.as_str(),
        " (",
        tier.as_str(),
        ") — `engenho ctl ",
        row.cli.resource,
        " ",
        row.cli.verb,
        "` on this machine's engenho, over its control socket",
    ]
    .concat();
    let tool = Tool::new(tool_name(row), description, Arc::new(input_schema(row)));
    ToolRoute::new_dyn(tool, move |ctx: ToolCallContext<'_, EngenhoMcp>| {
        let ceiling = ctx.service.authority().control_ceiling();
        let arguments = ctx.arguments.unwrap_or_default();
        // Found the way `engenho ctl` finds it, at each call: a daemon that
        // restarts elsewhere is followed.
        let socket = resolve_socket(None).path;
        Box::pin(async move { Ok(call(&socket, row, arguments, ceiling).await) })
    })
}

/// The HTTP pieces `arguments` name for `row`.
fn parts(row: &OperationSpec, mut arguments: JsonObject) -> HttpParts {
    let mut parts = HttpParts::default();
    let text = |v: serde_json::Value| match v {
        serde_json::Value::String(s) => s,
        other => other.to_string(),
    };
    for param in row.params {
        let Some(value) = arguments.remove(param.name) else {
            continue;
        };
        let pair = (param.name.to_owned(), text(value));
        match param.location {
            ParamLocation::Path => parts.path_params.push(pair),
            ParamLocation::Query => parts.query.push(pair),
            ParamLocation::Header => parts.headers.push(pair),
        }
    }
    if row.body {
        parts.body = Some(
            arguments
                .remove("body")
                .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new())),
        );
    }
    parts
}

/// Call `row` with `arguments` on the daemon at `socket`, capped at
/// `ceiling`.
pub async fn call(
    socket: &Path,
    row: &OperationSpec,
    arguments: JsonObject,
    ceiling: AuthorityTier,
) -> CallToolResult {
    let request = match render(row.id, &parts(row, arguments)) {
        Ok(request) => request,
        Err(invalid) => {
            return refused(&ControlError::refused(
                engenho_control_types::types::RefusalReason::InvalidValue,
                invalid.to_string(),
            ));
        }
    };
    let client = match ControlClient::uds(socket) {
        Ok(client) => client.with_actor(ACTOR).with_ceiling(ceiling),
        Err(err) => return failed(&err),
    };
    match client.send(row.id, request).await {
        Ok(reply) => CallToolResult::success(vec![Content::text(
            String::from_utf8_lossy(&reply.body).into_owned(),
        )]),
        Err(ClientError::Control(err)) => refused(&err),
        Err(err) => failed(&err),
    }
}

/// The daemon's refusal or blindness, in its own words: the spec's body.
fn refused(err: &ControlError) -> CallToolResult {
    let body = match err {
        ControlError::Refused(refusal) => serde_json::to_string_pretty(refusal),
        ControlError::Blind(blind) => serde_json::to_string_pretty(blind),
    };
    CallToolResult::error(vec![Content::text(body.unwrap_or_else(|e| e.to_string()))])
}

/// No answer from the daemon: blind, never an empty result.
fn failed(err: &ClientError) -> CallToolResult {
    CallToolResult::error(vec![Content::text(
        serde_json::json!({
            "outcome": "blind",
            "reason": "runtime_unavailable",
            "because": err.to_string(),
        })
        .to_string(),
    )])
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::writer::Grant;

    fn names(authority: &Authority) -> BTreeSet<String> {
        exposed(authority).map(tool_name).collect()
    }

    /// No destructive or sensitive operation is ever a tool, whatever the
    /// server was launched with.
    #[test]
    fn destructive_and_sensitive_operations_are_never_tools() {
        let mutate = Authority::LocalMutate {
            granted_by: Grant::LaunchFlag,
        };
        for authority in [Authority::Observe, mutate] {
            for row in exposed(&authority) {
                assert_ne!(row.tier, AuthorityTier::Destructive, "{}", row.id.as_str());
                assert!(!row.sensitive, "{}", row.id.as_str());
            }
        }
    }

    /// Observe operations are always tools; mutate ones only when launched
    /// to allow them; the spelling is the CLI's.
    #[test]
    fn the_launch_decides_the_mutate_tools() {
        let observe = names(&Authority::Observe);
        let mutate = names(&Authority::LocalMutate {
            granted_by: Grant::LaunchFlag,
        });
        for row in CATALOG.iter().filter(|r| !r.sensitive) {
            let name = tool_name(row);
            match row.tier {
                AuthorityTier::Observe => {
                    assert!(observe.contains(&name) && mutate.contains(&name), "{name}");
                }
                AuthorityTier::Mutate => {
                    assert!(!observe.contains(&name) && mutate.contains(&name), "{name}");
                }
                AuthorityTier::Destructive => assert!(!mutate.contains(&name), "{name}"),
            }
        }
        assert!(observe.contains("control_runtime_show"));
        assert!(mutate.contains("control_children_restart"));
        assert_eq!(
            mutate.len(),
            exposed(&Authority::LocalMutate {
                granted_by: Grant::LaunchFlag
            })
            .count(),
            "two tools share a name"
        );
    }

    /// Arguments become the operation's HTTP pieces by the catalog's
    /// parameter locations, and render through the daemon's own parser.
    #[test]
    fn arguments_render_through_the_daemons_parser() {
        let row = CATALOG
            .iter()
            .find(|r| r.id == engenho_control_types::OperationId::GetChild)
            .expect("getChild");
        let mut arguments = JsonObject::new();
        arguments.insert("child".into(), "kubelet".into());
        let rendered = render(row.id, &parts(row, arguments)).expect("renders");
        assert!(
            rendered.path.ends_with("/children/kubelet"),
            "{}",
            rendered.path
        );

        let mut bad = JsonObject::new();
        bad.insert("child".into(), "no-such-child".into());
        assert!(render(row.id, &parts(row, bad)).is_err());

        let schema = input_schema(row);
        assert_eq!(schema["required"], serde_json::json!(["child"]));
    }
}
