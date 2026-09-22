//! engenho-control-client — talks to a running engenho's control API.
//!
//! * [`ControlClient::call`] — one typed operation: the spec's request in,
//!   its response (or its typed refusal) out.
//! * [`ControlClient::send`] — any operation by id, already rendered: what
//!   the table-driven `engenho ctl` and the MCP control tools use, with
//!   [`render`] building the request through the daemon's own parser.
//! * [`resolve_socket`] — where the local daemon's socket is, found the way
//!   the daemon placed it.
//! * [`ControlClient::remote`] + [`remote`] — a daemon on another machine,
//!   over mTLS: this client's key, the daemon's pin, from `remotes.yaml`.

#![forbid(unsafe_code)]

pub mod remote;

use std::path::{Path, PathBuf};

use bytes::Bytes;
use engenho_config::{EngenhoConfig, SYSTEM_SOCKET_PATH, SocketDefaults};
use engenho_control_types::pin::{KeyMaterial, client_config};
use engenho_control_types::types::{Blind, Refusal};
use engenho_control_types::wire::{HttpParts, HttpRequest, OperationRequest};
use engenho_control_types::{
    AuthorityTier, ControlError, MediaType, Operation, OperationId, OperationVisitor, visit,
};

pub use remote::{RemoteEndpoint, RemoteError, RemotesConfig};

/// The environment variable that names the socket outright.
pub const SOCKET_ENV: &str = "ENGENHO_CONTROL_SOCKET";

/// An error and every cause under it, `: `-joined — a TLS pin failure is
/// three levels down a transport error, and it is the part that says why.
fn chain(err: &dyn std::error::Error) -> String {
    let mut out = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        source = cause.source();
    }
    out
}

/// Why a call did not produce a response body.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The daemon answered with a refusal or a blind.
    #[error(transparent)]
    Control(#[from] ControlError),
    /// The daemon could not be reached.
    #[error("cannot reach the engenho daemon at {endpoint}: {}", chain(source))]
    Unreachable {
        /// Where it was looked for.
        endpoint: String,
        /// Why.
        source: reqwest::Error,
    },
    /// The daemon answered with something the spec does not allow.
    #[error("unexpected answer from the engenho daemon: {0}")]
    Protocol(String),
}

/// A raw answer: what [`ControlClient::send`] returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    /// The status.
    pub status: u16,
    /// The body.
    pub body: Bytes,
}

/// A connection to one daemon's control API.
#[derive(Debug, Clone)]
pub struct ControlClient {
    http: reqwest::Client,
    base: String,
    endpoint: String,
    actor: Option<String>,
    ceiling: Option<AuthorityTier>,
}

impl ControlClient {
    /// Talk to the daemon on the socket at `path`.
    ///
    /// # Errors
    ///
    /// [`ClientError::Protocol`] if the HTTP client cannot be built.
    pub fn uds(path: &Path) -> Result<Self, ClientError> {
        let http = reqwest::Client::builder()
            .unix_socket(path)
            .build()
            .map_err(|e| ClientError::Protocol(format!("build the HTTP client: {e}")))?;
        Ok(Self {
            http,
            // The host is not resolved over a socket; it names the daemon.
            base: "http://engenho".into(),
            endpoint: path.display().to_string(),
            actor: None,
            ceiling: None,
        })
    }

    /// Talk to the remote daemon `endpoint` names, over TLS 1.3: presenting
    /// `key`, and trusting only a server whose key is one of the endpoint's
    /// pins.
    ///
    /// # Errors
    ///
    /// [`ClientError::Protocol`] if the TLS or HTTP client cannot be built.
    pub fn remote(endpoint: &RemoteEndpoint, key: &KeyMaterial) -> Result<Self, ClientError> {
        let tls = client_config(key, endpoint.server_spki.clone())
            .map_err(|e| ClientError::Protocol(format!("build the TLS client: {e}")))?;
        let http = reqwest::Client::builder()
            .use_preconfigured_tls(tls)
            .build()
            .map_err(|e| ClientError::Protocol(format!("build the HTTP client: {e}")))?;
        Ok(Self {
            http,
            base: ["https://", &endpoint.address].concat(),
            endpoint: endpoint.address.clone(),
            actor: None,
            ceiling: None,
        })
    }

    /// Declare what kind of caller this is (`human`, `agent:<label>`,
    /// `automation:<label>`). Recorded; never authority.
    #[must_use]
    pub fn with_actor(mut self, actor: impl Into<String>) -> Self {
        self.actor = Some(actor.into());
        self
    }

    /// Never act above `tier`, whatever this caller is granted.
    #[must_use]
    pub const fn with_ceiling(mut self, tier: AuthorityTier) -> Self {
        self.ceiling = Some(tier);
        self
    }

    /// Where this client connects.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Call operation `O`.
    ///
    /// # Errors
    ///
    /// [`ClientError::Control`] for the daemon's refusal or blind answer; the
    /// other variants when no answer arrived or it was malformed.
    pub async fn call<O: Operation>(&self, req: &O::Request) -> Result<O::Response, ClientError> {
        let reply = self.send(O::ID, req.to_http()).await?;
        let row = O::ID.spec();
        let value = match row.response_media {
            MediaType::Json => serde_json::from_slice(&reply.body)
                .map_err(|e| ClientError::Protocol(format!("{}: {e}", O::ID.as_str())))?,
            MediaType::Yaml => serde_json::Value::String(
                String::from_utf8(reply.body.to_vec())
                    .map_err(|e| ClientError::Protocol(format!("{}: {e}", O::ID.as_str())))?,
            ),
        };
        serde_json::from_value(value)
            .map_err(|e| ClientError::Protocol(format!("{}: {e}", O::ID.as_str())))
    }

    /// Send a rendered request for operation `id`. A success status returns
    /// the body; an error status is parsed into the spec's error body.
    ///
    /// # Errors
    ///
    /// As [`Self::call`].
    pub async fn send(&self, id: OperationId, req: HttpRequest) -> Result<Reply, ClientError> {
        let row = id.spec();
        let method = reqwest::Method::from_bytes(req.method.as_str().as_bytes())
            .map_err(|e| ClientError::Protocol(e.to_string()))?;
        let mut builder = self
            .http
            .request(method, format!("{}{}", self.base, req.path))
            .query(&req.query);
        for (name, value) in &req.headers {
            builder = builder.header(*name, value);
        }
        if let Some(actor) = &self.actor {
            builder = builder.header("engenho-actor", actor);
        }
        if let Some(ceiling) = self.ceiling {
            builder = builder.header("engenho-ceiling", ceiling.to_string());
        }
        if let Some(body) = &req.body {
            builder = builder.json(body);
        }
        let response = builder
            .send()
            .await
            .map_err(|source| ClientError::Unreachable {
                endpoint: self.endpoint.clone(),
                source,
            })?;
        let status = response.status().as_u16();
        let body = response
            .bytes()
            .await
            .map_err(|e| ClientError::Protocol(format!("read the body: {e}")))?;
        if status == row.success_status {
            return Ok(Reply { status, body });
        }
        Err(error_body(status, &body))
    }
}

/// An operation's HTTP pieces its request type refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{}: {detail}", op.as_str())]
pub struct Unrenderable {
    /// The operation.
    pub op: OperationId,
    /// Why.
    pub detail: String,
}

/// Render `parts` as operation `id`'s request, through the generated request
/// type the daemon parses it into — so a malformed argument is refused here,
/// by the daemon's own parser, before anything is sent. What `engenho ctl`
/// and the MCP control tools both build their requests with.
///
/// # Errors
///
/// [`Unrenderable`].
pub fn render(id: OperationId, parts: &HttpParts) -> Result<HttpRequest, Unrenderable> {
    visit(id, Render(parts))
}

struct Render<'a>(&'a HttpParts);

impl OperationVisitor for Render<'_> {
    type Output = Result<HttpRequest, Unrenderable>;

    fn visit<O: Operation>(self) -> Self::Output {
        O::Request::from_http(self.0)
            .map(|req| req.to_http())
            .map_err(|bad| Unrenderable {
                op: O::ID,
                detail: bad.to_string(),
            })
    }
}

/// The spec's error body for a non-success answer.
fn error_body(status: u16, body: &[u8]) -> ClientError {
    if let Ok(refusal) = serde_json::from_slice::<Refusal>(body) {
        return ClientError::Control(ControlError::Refused(refusal));
    }
    if let Ok(blind) = serde_json::from_slice::<Blind>(body) {
        return ClientError::Control(ControlError::Blind(blind));
    }
    ClientError::Protocol(format!(
        "status {status} with a body that is neither a refusal nor a blind: {}",
        String::from_utf8_lossy(body)
    ))
}

/// Where the local daemon's socket was found, and how.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SocketResolution {
    /// The socket to connect to.
    pub path: PathBuf,
    /// Every place considered, in order.
    pub considered: Vec<PathBuf>,
}

/// Find the local daemon's socket: `explicit`, else `$ENGENHO_CONTROL_SOCKET`,
/// else the first that exists of: the declared config's
/// `control.socket.path`, this user's default, the system default. With none
/// existing, the first candidate (so the error names where it looked).
#[must_use]
pub fn resolve_socket(explicit: Option<PathBuf>) -> SocketResolution {
    if let Some(path) = explicit.or_else(|| std::env::var_os(SOCKET_ENV).map(PathBuf::from)) {
        return SocketResolution {
            considered: vec![path.clone()],
            path,
        };
    }
    let defaults = SocketDefaults::for_process(nix::unistd::geteuid().as_raw());
    let mut considered: Vec<PathBuf> = Vec::new();
    for candidate in [
        declared_socket_path(),
        Some(defaults.default_path()),
        Some(PathBuf::from(SYSTEM_SOCKET_PATH)),
    ]
    .into_iter()
    .flatten()
    {
        if !considered.contains(&candidate) {
            considered.push(candidate);
        }
    }
    let path = considered
        .iter()
        .find(|p| p.exists())
        .or_else(|| considered.first())
        .cloned()
        .unwrap_or_else(|| PathBuf::from(SYSTEM_SOCKET_PATH));
    SocketResolution { path, considered }
}

/// `control.socket.path` from the declared config file, read leniently: a
/// config the daemon cannot boot on still names its socket.
fn declared_socket_path() -> Option<PathBuf> {
    let file = EngenhoConfig::discover_path()?;
    let doc: serde_yaml::Value = serde_yaml::from_str(&std::fs::read_to_string(file).ok()?).ok()?;
    doc.get("control")?
        .get("socket")?
        .get("path")?
        .as_str()
        .map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_explicit_socket_wins() {
        let r = resolve_socket(Some(PathBuf::from("/x/ctl.sock")));
        assert_eq!(r.path, PathBuf::from("/x/ctl.sock"));
        assert_eq!(r.considered, vec![PathBuf::from("/x/ctl.sock")]);
    }

    #[test]
    fn an_error_status_is_read_as_the_specs_error_body() {
        let refusal = br#"{"outcome":"refused","reason":"runtime_running","because":"it runs"}"#;
        assert!(matches!(
            error_body(409, refusal),
            ClientError::Control(ControlError::Refused(_))
        ));
        let blind = br#"{"outcome":"blind","reason":"internal","because":"x"}"#;
        assert!(matches!(
            error_body(503, blind),
            ClientError::Control(ControlError::Blind(_))
        ));
        assert!(matches!(error_body(500, b"oops"), ClientError::Protocol(_)));
    }

    #[tokio::test]
    async fn a_missing_daemon_is_unreachable_not_a_protocol_error() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let client = ControlClient::uds(&tmp.path().join("nothing.sock")).expect("client");
        let err = client
            .call::<engenho_control_types::ops::Hello>(&engenho_control_types::ops::HelloRequest {})
            .await
            .expect_err("nothing listens");
        assert!(matches!(err, ClientError::Unreachable { .. }), "{err}");
    }
}
