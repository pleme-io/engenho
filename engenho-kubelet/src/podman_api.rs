//! PODMAN, SPOKEN AS A PROTOCOL — the libpod REST API over its unix socket.
//!
//! ── ★ WHY THIS REPLACES SHELLING OUT ───────────────────────────────────────
//! [`crate::backend::PodmanBackend`] drives podman by building an argv,
//! spawning a process, and parsing its stdout. That works, and it makes the
//! runtime seam **a text format nobody versions**. A renamed flag, a reworded
//! error, a locale that formats a number differently, a podman release that
//! adds a column to `inspect` — each is a silent behaviour change with no
//! compiler and no schema between it and the kubelet. `cri.rs` already states
//! this case for CRI; it applies with equal force to podman, which is the
//! runtime actually in use.
//!
//! It is also a direct violation of the fleet's NO SHELL law, and of CONTAIN
//! THE C's sharper form: *speak the protocol, do not link the library* — and
//! do not shell to its CLI either, which is the same coupling with a worse
//! type system.
//!
//! podman ships exactly the surface that removes it. `podman.socket` serves
//! the **libpod REST API** — the same HTTP API the `podman --remote` client
//! speaks — as JSON over a unix socket. Every operation this kubelet performs
//! has an endpoint, so the subprocess buys nothing.
//!
//! ── WHAT THIS IS AND IS NOT ────────────────────────────────────────────────
//! It is: an HTTP/1.1 client over `tokio::net::UnixStream`, driven by `hyper`,
//! with typed request and response bodies. No `Command`, no argv, no stdout
//! parsing, no shell, no C library linked — hyper and serde are pure Rust and
//! the socket is a kernel object, not a vendored client.
//!
//! It is NOT a reimplementation of podman. engenho CALLS a runtime; it does
//! not become one. The same division `cri.rs` draws.
//!
//! ── ★ WHY NOT CRI, WHICH IS ALREADY WRITTEN ────────────────────────────────
//! `cri.rs` is the better long-term seam — containerd, CRI-O and youki all
//! speak it, and a distribution locked to one runtime is not a distribution.
//! But **podman does not implement CRI**. Selecting CRI means also installing
//! containerd or CRI-O; measured on plo 2026-09-05, the only runtime present is
//! podman and the only socket is `/run/podman/podman.sock`.
//!
//! So the two are siblings, not rivals: CRI substitutes the RUNTIME, this
//! removes the SUBPROCESS. Shipping this first fixes the seam that is actually
//! in the path today without making a runtime migration a prerequisite.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::backend::{ContainerSpec, PullPolicy};
use crate::error::KubeletError;

/// Where podman serves its API, in the order upstream itself looks.
///
/// NOT a constant. Rootful podman, rootless podman and every distribution's
/// packaging put the socket somewhere different, and a hardcoded path produces
/// a kubelet that works on exactly one machine — the same reasoning `cri.rs`
/// records for CRI endpoints.
///
/// Rootless comes FIRST when `XDG_RUNTIME_DIR` is set, because a user session
/// that has its own podman means the rootful socket (if it even exists) belongs
/// to a different container store, and picking it would leave the kubelet
/// managing containers nobody can see.
#[must_use]
pub fn default_socket_candidates() -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(xdg) = std::env::var("XDG_RUNTIME_DIR")
        && !xdg.is_empty()
    {
        out.push(PathBuf::from(xdg).join("podman/podman.sock"));
    }
    out.push(PathBuf::from("/run/podman/podman.sock"));
    out.push(PathBuf::from("/var/run/podman/podman.sock"));
    out
}

/// The first candidate that exists, or `None`.
///
/// Existence, not connectability: a socket whose server is down still names the
/// right store, and failing later with a connect error is more useful than
/// silently choosing a different one.
#[must_use]
pub fn discover_socket() -> Option<PathBuf> {
    default_socket_candidates().into_iter().find(|p| p.exists())
}

/// A libpod endpoint — the unix socket the API is served on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint(PathBuf);

impl Endpoint {
    /// Wrap an explicit socket path.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self(path.into())
    }

    /// Discover the socket, or fail with a message naming every path tried.
    ///
    /// # Errors
    ///
    /// [`KubeletError::Backend`] when no candidate exists. The error lists the
    /// candidates rather than saying "not found": on a machine where podman is
    /// installed but its socket unit is not enabled, the fix is
    /// `systemctl enable --now podman.socket`, and that is only obvious once
    /// you can see which paths were checked.
    pub fn discover() -> Result<Self, KubeletError> {
        discover_socket().map(Self).ok_or_else(|| {
            let tried = default_socket_candidates()
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ");
            KubeletError::Backend(format!(
                "no podman API socket found (tried: {tried}). If podman is installed but the \
                 socket is not being served, enable it — `systemctl enable --now podman.socket` \
                 for rootful, or `systemctl --user enable --now podman.socket` for rootless"
            ))
        })
    }

    /// The socket path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.0
    }
}

/// engenho's default container network.
///
/// The same literal `PodmanNetwork::default()` uses in the CLI backend. Named
/// here rather than imported because it is `PodmanNetwork::Named`'s inner
/// value, not a public constant — and duplicating a literal is better than
/// exporting a type whose shape this backend does not otherwise need. A test
/// pins the two together so they cannot drift.
pub const ENGENHO_NETWORK: &str = "engenho-net";

/// libpod's API version prefix.
///
/// Pinned rather than floating. podman serves versioned paths and keeps old
/// versions working; asking for a version is how a client states the contract
/// it was written against. `4.0.0` is the compatibility floor podman 4.x and
/// 5.x both honour, so this does not chase the installed version.
pub const API_VERSION: &str = "v4.0.0";

/// Body of `POST /libpod/networks/create`.
///
/// A typed struct rather than an inline `json!` so the wire shape is declared
/// once and a field rename is a compile error, matching every other request
/// type in this module.
#[derive(Debug, serde::Serialize)]
struct NetworkCreateRequest {
    name: String,
}

/// Build a libpod path: `/{API_VERSION}/libpod/{tail}`.
#[must_use]
pub fn libpod_path(tail: &str) -> String {
    let mut s = String::with_capacity(API_VERSION.len() + tail.len() + 10);
    s.push('/');
    s.push_str(API_VERSION);
    s.push_str("/libpod/");
    s.push_str(tail.trim_start_matches('/'));
    s
}

// ── Request bodies ─────────────────────────────────────────────────────────
//
// Typed structs rather than hand-built JSON. A misspelled key in a
// `serde_json::json!` literal is accepted by the compiler and rejected — or
// worse, IGNORED — by podman, which is the same failure class as a misspelled
// CLI flag and would defeat the point of moving off the CLI.

/// libpod's container-create request (the subset engenho sets).
///
/// podman's `SpecGenerator` has well over a hundred fields; every one omitted
/// here takes podman's own default, which is what the CLI would also have done.
/// Fields are `skip_serializing_if` so an unset value is ABSENT rather than
/// `null` — podman distinguishes the two for several options.
#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct CreateRequest {
    /// Container name.
    pub name: String,
    /// Image reference.
    pub image: String,
    /// Entrypoint override — podman's `command`, the CLI's trailing args.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    /// Environment, as a map (the CLI's repeated `-e k=v`).
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    /// Networks to attach, keyed by name.
    #[serde(rename = "Networks", skip_serializing_if = "BTreeMap::is_empty")]
    pub networks: BTreeMap<String, PerNetworkOptions>,
    /// Bind mounts.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub mounts: Vec<Mount>,
    /// Pull policy for the image.
    #[serde(rename = "pull_policy", skip_serializing_if = "Option::is_none")]
    pub pull_policy: Option<String>,
    /// Remove the container when it exits. Always false: the kubelet reads exit
    /// codes off stopped containers, and a self-removing container makes a
    /// terminal status unobservable.
    pub remove: bool,
    /// Full host privileges.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub privileged: bool,
    /// Read-only root filesystem.
    #[serde(
        rename = "read_only_filesystem",
        skip_serializing_if = "std::ops::Not::not"
    )]
    pub read_only_filesystem: bool,
    /// Set `no_new_privs` on the container process.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub no_new_privileges: bool,
    /// Capabilities to add.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub cap_add: Vec<String>,
    /// Capabilities to drop.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub cap_drop: Vec<String>,
    /// `uid` or `uid:gid` to run as.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

/// Per-network attachment options — carries the DNS aliases.
#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct PerNetworkOptions {
    /// DNS names this container answers to on that network.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
}

/// One mount, in libpod's `Mounts` shape.
///
/// `Type` is `bind` for a host path and `volume` for a named podman volume —
/// the same distinction the CLI backend makes between `-v /host/path:/dst` and
/// `-v volname:/dst`, where podman infers the kind from whether the source
/// looks like a path. Over the API there is no inference: the kind is a field,
/// which is one fewer thing to get wrong.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Mount {
    /// In-container path.
    #[serde(rename = "Destination")]
    pub destination: String,
    /// Host path.
    #[serde(rename = "Source")]
    pub source: String,
    /// Always `bind` here — engenho resolves volumes to host paths before this
    /// layer, so podman never has to know about the Kubernetes volume kinds.
    #[serde(rename = "Type")]
    pub mount_type: String,
    /// `ro` / `rw` and friends.
    #[serde(rename = "Options")]
    pub options: Vec<String>,
}

/// libpod's create response.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct CreateResponse {
    /// The new container's id.
    #[serde(rename = "Id")]
    pub id: String,
    /// Non-fatal warnings podman attached.
    #[serde(rename = "Warnings", default)]
    pub warnings: Vec<String>,
}

/// The slice of `GET /containers/{id}/json` engenho reads.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct InspectResponse {
    /// Container id.
    #[serde(rename = "Id")]
    pub id: String,
    /// Runtime state.
    #[serde(rename = "State")]
    pub state: InspectState,
    /// Network attachments, keyed by network name.
    #[serde(rename = "NetworkSettings", default)]
    pub network_settings: Option<NetworkSettings>,
}

/// The state block of an inspect response.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Default)]
pub struct InspectState {
    /// podman's state string: `created`, `running`, `exited`, …
    #[serde(rename = "Status", default)]
    pub status: String,
    /// Whether it is running right now.
    #[serde(rename = "Running", default)]
    pub running: bool,
    /// Exit code — meaningful only once terminal.
    #[serde(rename = "ExitCode", default)]
    pub exit_code: i32,
}

/// Network settings, for the pod IP.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Default)]
pub struct NetworkSettings {
    /// Per-network detail, keyed by network name.
    #[serde(rename = "Networks", default)]
    pub networks: BTreeMap<String, NetworkDetail>,
    /// The legacy top-level address, used when `Networks` is empty.
    #[serde(rename = "IPAddress", default)]
    pub ip_address: String,
}

/// One network's attachment detail.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Default)]
pub struct NetworkDetail {
    /// This container's address on that network.
    #[serde(rename = "IPAddress", default)]
    pub ip_address: String,
}

impl InspectResponse {
    /// The container's address, preferring a named network over the legacy
    /// top-level field.
    ///
    /// Returns `None` rather than an empty string for "no address yet": a
    /// container in `created` has no IP, and an empty string presented as an
    /// address is the kind of value that reaches a Pod status and looks real.
    #[must_use]
    pub fn pod_ip(&self) -> Option<String> {
        let settings = self.network_settings.as_ref()?;
        for detail in settings.networks.values() {
            if !detail.ip_address.is_empty() {
                return Some(detail.ip_address.clone());
            }
        }
        if settings.ip_address.is_empty() {
            None
        } else {
            Some(settings.ip_address.clone())
        }
    }
}

/// Map engenho's [`PullPolicy`] onto libpod's `pull_policy` string.
///
/// The same mapping [`crate::backend::PodmanBackend`] makes for `--pull`, kept
/// deliberately identical so switching backends cannot change WHEN a registry
/// is contacted. That would be a behaviour change disguised as a refactor.
#[must_use]
pub fn pull_policy_value(policy: PullPolicy) -> &'static str {
    match policy {
        // `IfNotPresent` is `missing`, NOT `newer`: `newer` is a registry
        // round-trip on every start. Corrected in the CLI backend 2026-08-30
        // and carried here rather than re-derived.
        PullPolicy::Never => "never",
        PullPolicy::Missing | PullPolicy::IfNotPresent => "missing",
        PullPolicy::Always => "always",
    }
}

/// Translate one resolved mount into libpod's shape.
///
/// Mirrors [`crate::backend::PodmanBackend`]'s argv rendering exactly —
/// `HostDir` and `PvcHostDir` are bind mounts of a host path, `NamedVolume` is
/// a podman volume — because the two backends must place the same bytes at the
/// same container path. A mount that differs between backends is a workload
/// that behaves differently depending on how the kubelet was configured, which
/// is the worst kind of difference: invisible until something reads a file.
fn to_libpod_mount(m: &crate::pod_volume::ResolvedMount) -> Mount {
    use crate::pod_volume::MountSource;
    let (source, mount_type) = match &m.source {
        MountSource::HostDir(p) => (p.display().to_string(), "bind"),
        MountSource::PvcHostDir { path, .. } => (path.display().to_string(), "bind"),
        MountSource::NamedVolume(n) => (n.clone(), "volume"),
    };
    // `ro` / `rw` are the same option strings the CLI appends after the second
    // colon. Emitting `rw` explicitly rather than omitting it keeps the
    // request self-describing — an absent option reads as "unspecified", and a
    // reader should not have to know podman's default to know what was asked.
    let options = vec![if m.read_only { "ro" } else { "rw" }.to_string()];
    Mount {
        destination: m.mount_path.clone(),
        source,
        mount_type: mount_type.to_string(),
        options,
    }
}

/// Build the create request for a [`ContainerSpec`].
///
/// Pure — no I/O — so the request body is unit-testable without a podman.
/// That is most of the value of moving off argv: the thing that used to be an
/// unverifiable string vector is now a typed value with an equality test.
#[must_use]
pub fn create_request(spec: &ContainerSpec, network: Option<&str>) -> CreateRequest {
    let c = &spec.confinement;
    let mut networks = BTreeMap::new();
    if let Some(net) = network {
        networks.insert(
            net.to_string(),
            PerNetworkOptions {
                aliases: spec.network_aliases.clone(),
            },
        );
    }
    CreateRequest {
        name: spec.name.clone(),
        image: spec.image.clone(),
        command: spec.command.clone(),
        env: spec.env.clone(),
        networks,
        mounts: spec.mounts.iter().map(to_libpod_mount).collect(),
        pull_policy: spec.pull_policy.map(|p| pull_policy_value(p).to_string()),
        remove: false,
        // ── ★ CONFINEMENT IS RENDERED, NOT MERELY CARRIED ─────────────────
        // A disposition read from the Pod and then not sent is the dropped-
        // mounts defect with worse consequences: the manifest says
        // `readOnlyRootFilesystem: true`, the type faithfully records it, and
        // the container still gets a writable root.
        privileged: c.privileged,
        read_only_filesystem: c.read_only_root_fs,
        no_new_privileges: c.no_new_privileges,
        cap_add: c.cap_add.clone(),
        cap_drop: c.cap_drop.clone(),
        // podman takes `uid:gid` as one string. Built here rather than in
        // Confinement so the typed side keeps two numbers and only the WIRE
        // sees the concatenation.
        user: match (c.run_as_user, c.run_as_group) {
            (Some(u), Some(g)) => Some(format!("{u}:{g}")),
            (Some(u), None) => Some(u.to_string()),
            // A gid with no uid is not expressible as podman's `user` string,
            // and inventing uid 0 to carry it would silently run as root.
            (None, _) => None,
        },
    }
}

// ── The multiplexed stream ─────────────────────────────────────────────────

/// Which stream a frame's payload belongs to.
///
/// The wire values are Docker's and libpod speaks the same framing, so these
/// are fixed by the protocol rather than chosen here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamKind {
    /// 0 — stdin echoed back (only in attach; never in logs or exec output).
    Stdin,
    /// 1 — stdout.
    Stdout,
    /// 2 — stderr.
    Stderr,
}

impl StreamKind {
    fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Stdin),
            1 => Some(Self::Stdout),
            2 => Some(Self::Stderr),
            _ => None,
        }
    }
}

/// stdout and stderr, separated.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Demuxed {
    /// Everything that arrived on stdout, in order.
    pub stdout: Vec<u8>,
    /// Everything that arrived on stderr, in order.
    pub stderr: Vec<u8>,
}

/// Split a libpod/Docker multiplexed stream into stdout and stderr.
///
/// ── THE FRAME ──────────────────────────────────────────────────────────────
/// ```text
///  0        1        2        3        4                                8
///  +--------+--------+--------+--------+--------------------------------+
///  |  kind  |          zero padding    |     payload length (BE u32)    |
///  +--------+--------+--------+--------+--------------------------------+
///  |                      payload, `length` bytes                       |
///  +--------------------------------------------------------------------+
/// ```
///
/// ── ★ WHY THIS IS A SEPARATE PURE FUNCTION ─────────────────────────────────
/// It is the only part of exec/logs with any logic in it, and every way it can
/// be wrong is SILENT. A misread length yields plausible-looking output that is
/// subtly truncated or interleaved; a header mistaken for payload puts control
/// bytes in the middle of a log line. None of that errors, and none of it is
/// visible in an integration test that only asserts the output "contains" a
/// word. So it is pure, and it is tested against bytes rather than against a
/// running podman.
///
/// # Errors
///
/// [`KubeletError::Backend`] on a truncated frame or an unknown stream kind.
/// A short frame is REFUSED rather than returned as a partial read: silently
/// returning what arrived is how a caller concludes a command produced no
/// output when in fact the stream was cut.
pub fn demux(mut buf: &[u8]) -> Result<Demuxed, KubeletError> {
    let mut out = Demuxed::default();
    while !buf.is_empty() {
        if buf.len() < 8 {
            return Err(KubeletError::Backend(format!(
                "truncated podman stream: {} trailing byte(s), need an 8-byte frame header",
                buf.len()
            )));
        }
        let kind = StreamKind::from_byte(buf[0]).ok_or_else(|| {
            KubeletError::Backend(format!(
                "unknown podman stream kind {} — the stream is not framed (a TTY-allocated \
                 container emits a RAW stream with no headers; ask for it without a TTY)",
                buf[0]
            ))
        })?;
        let len = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
        let body = &buf[8..];
        if body.len() < len {
            return Err(KubeletError::Backend(format!(
                "truncated podman frame: header declares {len} bytes, {} available",
                body.len()
            )));
        }
        match kind {
            StreamKind::Stdout => out.stdout.extend_from_slice(&body[..len]),
            StreamKind::Stderr => out.stderr.extend_from_slice(&body[..len]),
            // stdin echo carries nothing a kubelet wants; dropping it is not a
            // loss, and it never appears in logs or exec output anyway.
            StreamKind::Stdin => {}
        }
        buf = &body[len..];
    }
    Ok(out)
}

// ── The transport ──────────────────────────────────────────────────────────

/// A libpod client: HTTP/1.1 over the unix socket, no subprocess.
///
/// One connection PER REQUEST rather than a pooled client, deliberately. The
/// kubelet's call rate is a handful of requests per pod per reconcile tick, a
/// unix-socket connect is microseconds, and a pool would add a failure mode
/// (a half-closed connection surviving a podman restart) that costs more than
/// the connect it saves. `podman --remote` does the same thing.
#[derive(Clone, Debug)]
pub struct PodmanApi {
    endpoint: Endpoint,
}

impl PodmanApi {
    /// A client for an explicit endpoint.
    #[must_use]
    pub fn new(endpoint: Endpoint) -> Self {
        Self { endpoint }
    }

    /// A client for the discovered socket.
    ///
    /// # Errors
    ///
    /// [`KubeletError::Backend`] when no socket exists — see [`Endpoint::discover`].
    pub fn discover() -> Result<Self, KubeletError> {
        Ok(Self::new(Endpoint::discover()?))
    }

    /// The endpoint this client talks to.
    #[must_use]
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Send one request and read the whole response.
    ///
    /// # Errors
    ///
    /// [`KubeletError::Backend`] on connect, protocol or read failure. Every
    /// message names the socket, because the two failures that actually happen
    /// — the socket not being served, and it belonging to a different user's
    /// podman — are indistinguishable without it.
    async fn send(
        &self,
        method: hyper::Method,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<(hyper::StatusCode, Vec<u8>), KubeletError> {
        use http_body_util::{BodyExt as _, Full};
        use hyper::body::Bytes;

        let sock = self.endpoint.path();
        let stream = tokio::net::UnixStream::connect(sock).await.map_err(|e| {
            KubeletError::Backend(format!("connect to podman API at {}: {e}", sock.display()))
        })?;

        let (mut sender, conn) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
                .await
                .map_err(|e| {
                    KubeletError::Backend(format!(
                        "podman API handshake at {}: {e}",
                        sock.display()
                    ))
                })?;

        // The connection future must be driven for the request to progress. It
        // ends when the response is complete; a dropped handle would cancel the
        // request mid-flight.
        tokio::spawn(async move {
            let _ = conn.await;
        });

        let has_body = body.is_some();
        let mut req = hyper::Request::builder()
            .method(method)
            .uri(path)
            // libpod ignores Host for a unix socket, but HTTP/1.1 requires it
            // and some proxies in front of a socket do not.
            .header(hyper::header::HOST, "d");
        if has_body {
            req = req.header(hyper::header::CONTENT_TYPE, "application/json");
        }
        let req = req
            .body(Full::new(Bytes::from(body.unwrap_or_default())))
            .map_err(|e| KubeletError::Backend(format!("build podman request {path}: {e}")))?;

        let resp = sender.send_request(req).await.map_err(|e| {
            KubeletError::Backend(format!("podman API {path} at {}: {e}", sock.display()))
        })?;
        let status = resp.status();
        let bytes = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| KubeletError::Backend(format!("read podman response {path}: {e}")))?
            .to_bytes()
            .to_vec();
        Ok((status, bytes))
    }

    /// `POST /libpod/networks/create` — idempotent.
    ///
    /// engenho attaches every container it creates to [`ENGENHO_NETWORK`], but
    /// until now NOTHING in production ever created it: `ensure_network` existed
    /// on the CLI backend and in the M0.3 integration test, which makes its own
    /// network before it runs. So the tests passed while a real single-node
    /// deployment could not start ANY pod, failing with
    ///
    ///   podman create …: HTTP 500: unable to find network with name or ID
    ///   engenho-net: network not found
    ///
    /// Measured on a live single-node engenho 2026-09-06: every pod stuck
    /// Pending on exactly that, with `podman network ls` showing no such
    /// network. This is the gap the podman-API backend inherited when it
    /// replaced the CLI one — it carried the network REFERENCE across and left
    /// the network CREATION behind.
    ///
    /// # Errors
    ///
    /// [`KubeletError::Backend`] on transport failure or a non-2xx status.
    /// **409 Conflict is success**: podman returns it when the network already
    /// exists, which is the steady state on every tick after the first.
    pub async fn ensure_network(&self, name: &str) -> Result<(), KubeletError> {
        let body = serde_json::to_vec(&NetworkCreateRequest {
            name: name.to_string(),
        })
        .map_err(|e| KubeletError::Backend(format!("encode network create: {e}")))?;
        let (status, bytes) = self
            .send(
                hyper::Method::POST,
                &libpod_path("networks/create"),
                Some(body),
            )
            .await?;
        if status.is_success() || status == hyper::StatusCode::CONFLICT {
            return Ok(());
        }
        Err(KubeletError::Backend(format!(
            "podman network create {name}: HTTP {status}: {}",
            String::from_utf8_lossy(&bytes)
        )))
    }

    /// `POST /libpod/containers/create`.
    ///
    /// # Errors
    ///
    /// [`KubeletError::Backend`] on transport failure or a non-2xx status, with
    /// podman's own error body included — that body is the diagnosis (`image not
    /// known`, `name already in use`) and discarding it is how a runtime error
    /// becomes "create failed".
    pub async fn create(&self, req: &CreateRequest) -> Result<CreateResponse, KubeletError> {
        let body = serde_json::to_vec(req)
            .map_err(|e| KubeletError::Backend(format!("encode create request: {e}")))?;
        let (status, bytes) = self
            .send(
                hyper::Method::POST,
                &libpod_path("containers/create"),
                Some(body),
            )
            .await?;
        if !status.is_success() {
            return Err(KubeletError::Backend(format!(
                "podman create {}: HTTP {status}: {}",
                req.name,
                String::from_utf8_lossy(&bytes)
            )));
        }
        serde_json::from_slice(&bytes)
            .map_err(|e| KubeletError::Backend(format!("decode create response: {e}")))
    }

    /// `POST /libpod/containers/{id}/start`.
    ///
    /// # Errors
    ///
    /// [`KubeletError::Backend`] on transport failure or a non-2xx status.
    /// **304 is success**: podman returns Not Modified when the container is
    /// already running, and treating that as an error would make every
    /// reconcile tick after the first one fail.
    pub async fn start(&self, id: &str) -> Result<(), KubeletError> {
        let (status, bytes) = self
            .send(
                hyper::Method::POST,
                &libpod_path(&format!("containers/{id}/start")),
                None,
            )
            .await?;
        if status.is_success() || status == hyper::StatusCode::NOT_MODIFIED {
            return Ok(());
        }
        Err(KubeletError::Backend(format!(
            "podman start {id}: HTTP {status}: {}",
            String::from_utf8_lossy(&bytes)
        )))
    }

    /// `GET /libpod/containers/{id}/json`, or `None` when absent.
    ///
    /// # Errors
    ///
    /// [`KubeletError::Backend`] on transport failure or an unexpected status.
    /// A 404 is `Ok(None)`, NOT an error: "this container does not exist" is a
    /// normal answer to the kubelet's question and the reconciler acts on it.
    pub async fn inspect(&self, id: &str) -> Result<Option<InspectResponse>, KubeletError> {
        let (status, bytes) = self
            .send(
                hyper::Method::GET,
                &libpod_path(&format!("containers/{id}/json")),
                None,
            )
            .await?;
        if status == hyper::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(KubeletError::Backend(format!(
                "podman inspect {id}: HTTP {status}: {}",
                String::from_utf8_lossy(&bytes)
            )));
        }
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| KubeletError::Backend(format!("decode inspect {id}: {e}")))
    }

    /// `POST /libpod/containers/{id}/stop`.
    ///
    /// # Errors
    ///
    /// [`KubeletError::Backend`] on transport failure or an unexpected status.
    /// 304 (already stopped) and 404 (already gone) are both success — stopping
    /// is idempotent from the reconciler's point of view, and it re-issues stop
    /// on every tick until the pod object disappears.
    pub async fn stop(&self, id: &str) -> Result<(), KubeletError> {
        let (status, bytes) = self
            .send(
                hyper::Method::POST,
                &libpod_path(&format!("containers/{id}/stop")),
                None,
            )
            .await?;
        if status.is_success()
            || status == hyper::StatusCode::NOT_MODIFIED
            || status == hyper::StatusCode::NOT_FOUND
        {
            return Ok(());
        }
        Err(KubeletError::Backend(format!(
            "podman stop {id}: HTTP {status}: {}",
            String::from_utf8_lossy(&bytes)
        )))
    }

    /// `POST /libpod/containers/{id}/exec` — create an exec session.
    ///
    /// # Errors
    ///
    /// [`KubeletError::Backend`] on transport failure or a non-2xx status.
    async fn exec_create(&self, id: &str, argv: &[String]) -> Result<String, KubeletError> {
        // `Tty: false` is load-bearing, not a default: a TTY-allocated exec
        // returns a RAW stream with no frame headers, which `demux` would
        // correctly refuse. Asking for no TTY is what makes the output
        // separable into stdout and stderr at all.
        let body = serde_json::to_vec(&serde_json::json!({
            "AttachStdout": true,
            "AttachStderr": true,
            "AttachStdin": false,
            "Tty": false,
            "Cmd": argv,
        }))
        .map_err(|e| KubeletError::Backend(format!("encode exec create: {e}")))?;
        let (status, bytes) = self
            .send(
                hyper::Method::POST,
                &libpod_path(&format!("containers/{id}/exec")),
                Some(body),
            )
            .await?;
        if !status.is_success() {
            return Err(KubeletError::Backend(format!(
                "podman exec create {id}: HTTP {status}: {}",
                String::from_utf8_lossy(&bytes)
            )));
        }
        #[derive(Deserialize)]
        struct ExecCreated {
            #[serde(rename = "Id")]
            id: String,
        }
        serde_json::from_slice::<ExecCreated>(&bytes)
            .map(|c| c.id)
            .map_err(|e| KubeletError::Backend(format!("decode exec create: {e}")))
    }

    /// `POST /libpod/exec/{exec_id}/start` — run it and collect the output.
    ///
    /// # Errors
    ///
    /// [`KubeletError::Backend`] on transport failure, a non-2xx status, or an
    /// unframed stream.
    async fn exec_start(&self, exec_id: &str) -> Result<Demuxed, KubeletError> {
        // `Detach: false` so the response body IS the output stream. The whole
        // body is buffered, which is correct for the kubelet's uses (probes and
        // short commands) and would be wrong for an interactive session — noted
        // rather than discovered.
        let body = serde_json::to_vec(&serde_json::json!({ "Detach": false, "Tty": false }))
            .map_err(|e| KubeletError::Backend(format!("encode exec start: {e}")))?;
        let (status, bytes) = self
            .send(
                hyper::Method::POST,
                &libpod_path(&format!("exec/{exec_id}/start")),
                Some(body),
            )
            .await?;
        if !status.is_success() {
            return Err(KubeletError::Backend(format!(
                "podman exec start {exec_id}: HTTP {status}: {}",
                String::from_utf8_lossy(&bytes)
            )));
        }
        demux(&bytes)
    }

    /// `GET /libpod/exec/{exec_id}/json` — the exit code, after the run.
    ///
    /// # Errors
    ///
    /// [`KubeletError::Backend`] on transport failure, a non-2xx status, or an
    /// exec that reports itself still running.
    async fn exec_exit_code(&self, exec_id: &str) -> Result<i32, KubeletError> {
        let (status, bytes) = self
            .send(
                hyper::Method::GET,
                &libpod_path(&format!("exec/{exec_id}/json")),
                None,
            )
            .await?;
        if !status.is_success() {
            return Err(KubeletError::Backend(format!(
                "podman exec inspect {exec_id}: HTTP {status}: {}",
                String::from_utf8_lossy(&bytes)
            )));
        }
        #[derive(Deserialize)]
        struct ExecInspect {
            #[serde(rename = "ExitCode")]
            exit_code: Option<i32>,
            #[serde(rename = "Running", default)]
            running: bool,
        }
        let i: ExecInspect = serde_json::from_slice(&bytes)
            .map_err(|e| KubeletError::Backend(format!("decode exec inspect: {e}")))?;
        // ★ A still-running exec has NO exit code, and defaulting it to 0 would
        // report success for a command that has not finished — which a liveness
        // probe reads as healthy. Refuse instead.
        if i.running {
            return Err(KubeletError::Backend(format!(
                "podman exec {exec_id} is still running after its output stream closed; \
                 refusing to report an exit code it does not have"
            )));
        }
        i.exit_code.ok_or_else(|| {
            KubeletError::Backend(format!(
                "podman exec {exec_id} finished with no exit code — cannot distinguish \
                 success from failure"
            ))
        })
    }

    /// `GET /libpod/containers/{id}/logs` — the container's output.
    ///
    /// # Errors
    ///
    /// [`KubeletError::Backend`] on transport failure, a non-2xx status, or an
    /// unframed stream. A 404 is an ERROR, not empty output — the trait's own
    /// contract says a missing container must be typed, "never a silently-empty
    /// success".
    pub async fn logs(
        &self,
        id: &str,
        opts: &crate::backend::LogOptions,
    ) -> Result<Demuxed, KubeletError> {
        let mut q = String::from("stdout=true&stderr=true");
        if let Some(n) = opts.tail {
            q.push_str(&format!("&tail={n}"));
        }
        if opts.timestamps {
            q.push_str("&timestamps=true");
        }
        let (status, bytes) = self
            .send(
                hyper::Method::GET,
                &libpod_path(&format!("containers/{id}/logs?{q}")),
                None,
            )
            .await?;
        if !status.is_success() {
            return Err(KubeletError::Backend(format!(
                "podman logs {id}: HTTP {status}: {}",
                String::from_utf8_lossy(&bytes)
            )));
        }
        demux(&bytes)
    }

    /// `DELETE /libpod/containers/{id}?force=true`.
    ///
    /// # Errors
    ///
    /// [`KubeletError::Backend`] on transport failure or an unexpected status.
    /// 404 is success, for the same idempotence reason as `stop`.
    ///
    /// ★ This is also the fix for a measured CLI-backend failure: `podman rm`
    /// exits 0 while removing nothing when the store record is missing, so
    /// neither the exit code nor the absence of an exception reveals it — only
    /// the stderr text. Over the API a failure is a STATUS CODE, which cannot
    /// be mistaken for success.
    pub async fn remove(&self, id: &str) -> Result<(), KubeletError> {
        let (status, bytes) = self
            .send(
                hyper::Method::DELETE,
                &libpod_path(&format!("containers/{id}?force=true")),
                None,
            )
            .await?;
        if status.is_success() || status == hyper::StatusCode::NOT_FOUND {
            return Ok(());
        }
        Err(KubeletError::Backend(format!(
            "podman remove {id}: HTTP {status}: {}",
            String::from_utf8_lossy(&bytes)
        )))
    }
}

// ── The ContainerRuntime implementation ────────────────────────────────────

/// The naturalized podman backend: every operation an API call.
///
/// Drop-in for [`crate::backend::PodmanBackend`] — same trait, same semantics,
/// no subprocess. The differences that matter are all in the failure modes:
///
/// * A failure is a STATUS CODE, not a parsed error string. `podman rm` exiting
///   0 while removing nothing (measured 2026-09-01, and undetectable from the
///   exit code) has no analogue here.
/// * "Already running" / "already gone" are distinguishable from "failed",
///   because 304 and 404 are distinct from 500. On the CLI they are all
///   non-zero exits with prose.
/// * A podman upgrade that renames a flag cannot silently change behaviour.
///   The API is versioned; the CLI is not.
#[derive(Clone, Debug)]
pub struct PodmanApiBackend {
    api: PodmanApi,
    network: Option<String>,
}

impl PodmanApiBackend {
    /// A backend on an explicit endpoint.
    #[must_use]
    pub fn new(api: PodmanApi, network: Option<String>) -> Self {
        Self { api, network }
    }

    /// A backend on the discovered socket, attached to engenho's network.
    ///
    /// # Errors
    ///
    /// [`KubeletError::Backend`] when no podman socket exists.
    pub fn discover() -> Result<Self, KubeletError> {
        Ok(Self::new(
            PodmanApi::discover()?,
            Some(ENGENHO_NETWORK.to_string()),
        ))
    }

    /// The endpoint in use — for startup logging, so an operator can see WHICH
    /// podman store this kubelet is driving without guessing.
    #[must_use]
    pub fn endpoint_path(&self) -> &Path {
        self.api.endpoint().path()
    }
}

#[async_trait::async_trait]
impl crate::backend::ContainerRuntime for PodmanApiBackend {
    fn name(&self) -> &'static str {
        "podman-api"
    }

    async fn start(
        &self,
        spec: &ContainerSpec,
    ) -> Result<crate::backend::ContainerStatus, KubeletError> {
        // Ensure the network BEFORE the create that depends on it. Idempotent
        // (409 = already exists), so the cost after the first pod is one round
        // trip on a unix socket. Doing it here rather than at construction
        // keeps `discover()` infallible-by-IO and means a network deleted out
        // from under a running kubelet is repaired on the next start instead of
        // wedging until restart.
        if let Some(net) = self.network.as_deref() {
            self.api.ensure_network(net).await?;
        }

        let req = create_request(spec, self.network.as_deref());
        let created = self.api.create(&req).await?;
        self.api.start(&created.id).await?;

        // Read back rather than assume. `start` returning 2xx means podman
        // accepted the request; the ADDRESS is assigned during startup, and a
        // status synthesised from the request would report an IP the container
        // does not have.
        let status = self.api.inspect(&created.id).await?;
        Ok(match status {
            Some(i) => {
                let pod_ip = i.pod_ip();
                crate::backend::ContainerStatus {
                    container_id: i.id,
                    running: i.state.running,
                    pod_ip,
                    exit_code: if i.state.running {
                        None
                    } else {
                        Some(i.state.exit_code)
                    },
                }
            }
            // Created and started, then gone before the read: a container that
            // exits instantly. Report it as not-running with no exit code
            // rather than inventing one — the reconciler's next tick will see
            // the real terminal state.
            None => crate::backend::ContainerStatus {
                container_id: created.id,
                running: false,
                pod_ip: None,
                exit_code: None,
            },
        })
    }

    async fn status(
        &self,
        container_id: &str,
    ) -> Result<Option<crate::backend::ContainerStatus>, KubeletError> {
        Ok(self.api.inspect(container_id).await?.map(|i| {
            let pod_ip = i.pod_ip();
            crate::backend::ContainerStatus {
                container_id: i.id,
                running: i.state.running,
                pod_ip,
                // An exit code is only meaningful once the container is
                // terminal. Reporting 0 for a RUNNING container is how a
                // healthy workload gets read as a clean exit and restarted.
                exit_code: if i.state.running {
                    None
                } else {
                    Some(i.state.exit_code)
                },
            }
        }))
    }

    async fn stop(&self, container_id: &str) -> Result<(), KubeletError> {
        self.api.stop(container_id).await
    }

    async fn remove(&self, container_id: &str) -> Result<(), KubeletError> {
        self.api.remove(container_id).await
    }

    async fn logs(
        &self,
        container_id: &str,
        opts: &crate::backend::LogOptions,
    ) -> Result<String, KubeletError> {
        // Interleave stdout and stderr the way `kubectl logs` presents them:
        // one text blob, stdout first. The two are collected separately because
        // the WIRE separates them; joining is a presentation choice made here
        // rather than a distinction lost on the way in.
        let out = self.api.logs(container_id, opts).await?;
        let mut s = String::from_utf8_lossy(&out.stdout).into_owned();
        if !out.stderr.is_empty() {
            s.push_str(&String::from_utf8_lossy(&out.stderr));
        }
        Ok(s)
    }

    async fn exec(
        &self,
        container_id: &str,
        argv: &[String],
    ) -> Result<crate::backend::ExecOutcome, KubeletError> {
        // Three calls, and the third is the one that matters. libpod's exec is
        // create -> start -> inspect: the START response carries the OUTPUT but
        // no status, so an implementation that stops there has to invent an exit
        // code. Inventing 0 is how a failing liveness probe reads as healthy,
        // which is the whole reason this backend refused to implement exec at
        // all rather than fake it.
        let exec_id = self.api.exec_create(container_id, argv).await?;
        let streams = self.api.exec_start(&exec_id).await?;
        let exit_code = self.api.exec_exit_code(&exec_id).await?;
        Ok(crate::backend::ExecOutcome {
            exit_code,
            stdout: String::from_utf8_lossy(&streams.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&streams.stderr).into_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn libpod_paths_are_versioned_and_single_slashed() {
        assert_eq!(
            libpod_path("containers/create"),
            "/v4.0.0/libpod/containers/create"
        );
        // A caller writing a leading slash is the obvious mistake and must not
        // produce `//`, which some proxies normalise and some do not.
        assert_eq!(
            libpod_path("/containers/create"),
            "/v4.0.0/libpod/containers/create"
        );
    }

    #[test]
    fn the_socket_search_prefers_rootless_when_a_user_session_exists() {
        // Whichever socket is chosen decides which container STORE the kubelet
        // manages. Choosing the rootful one inside a user session leaves it
        // driving containers that user cannot see.
        let candidates = default_socket_candidates();
        assert!(
            candidates.contains(&PathBuf::from("/run/podman/podman.sock")),
            "the rootful socket must always be a candidate: {candidates:?}"
        );
    }

    #[test]
    fn pull_policy_mapping_matches_the_cli_backend_exactly() {
        // If these ever diverge, switching backends silently changes when a
        // registry is contacted — a behaviour change wearing a refactor's
        // clothes.
        assert_eq!(pull_policy_value(PullPolicy::Never), "never");
        assert_eq!(pull_policy_value(PullPolicy::Missing), "missing");
        assert_eq!(
            pull_policy_value(PullPolicy::IfNotPresent),
            "missing",
            "IfNotPresent is `missing`, never `newer` — `newer` is a registry \
             round-trip on every start (corrected in the CLI backend 2026-08-30)"
        );
        assert_eq!(pull_policy_value(PullPolicy::Always), "always");
    }

    #[test]
    fn a_create_request_carries_name_image_env_command_and_aliases() {
        let mut env = BTreeMap::new();
        env.insert("KUBECONFIG".to_string(), "/etc/kube/config".to_string());
        let spec = ContainerSpec {
            name: "operator-0".to_string(),
            image: "ghcr.io/pleme-io/pangea-operator:x".to_string(),
            env,
            command: vec!["/bin/operator".to_string(), "--serve".to_string()],
            pull_policy: Some(PullPolicy::Never),
            network_aliases: vec!["operator".to_string()],
            mounts: Vec::new(),
            confinement: crate::backend::Confinement::default(),
        };
        let req = create_request(&spec, Some("engenho-net"));

        assert_eq!(req.name, "operator-0");
        assert_eq!(req.command, vec!["/bin/operator", "--serve"]);
        assert_eq!(
            req.env.get("KUBECONFIG").map(String::as_str),
            Some("/etc/kube/config")
        );
        assert_eq!(req.pull_policy.as_deref(), Some("never"));
        assert_eq!(
            req.networks.get("engenho-net").map(|n| n.aliases.clone()),
            Some(vec!["operator".to_string()]),
            "network aliases are what make pod-to-pod DNS work; losing them is \
             silent until one workload cannot resolve another"
        );
    }

    // ── The multiplexed stream ────────────────────────────────────────────
    //
    // ★ Every way this can be wrong is SILENT. A misread length yields output
    // that is plausibly-shaped and subtly truncated; a header mistaken for
    // payload puts control bytes mid-line. An integration test asserting the
    // output "contains" a word passes through all of it. So these test BYTES.

    fn frame(kind: u8, payload: &[u8]) -> Vec<u8> {
        let mut v = vec![kind, 0, 0, 0];
        v.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        v.extend_from_slice(payload);
        v
    }

    #[test]
    fn stdout_and_stderr_are_separated_not_concatenated() {
        let mut s = frame(1, b"out-a");
        s.extend(frame(2, b"err-a"));
        s.extend(frame(1, b"out-b"));
        let d = demux(&s).expect("well-formed stream");
        assert_eq!(d.stdout, b"out-aout-b");
        assert_eq!(d.stderr, b"err-a");
    }

    #[test]
    fn frames_keep_their_order_within_a_stream() {
        // Interleaving must not reorder either side. A HashMap-shaped
        // implementation passes the previous test and fails this one.
        let mut s = Vec::new();
        for i in 0..8u8 {
            s.extend(frame(1, &[b'0' + i]));
            s.extend(frame(2, &[b'a' + i]));
        }
        let d = demux(&s).unwrap();
        assert_eq!(d.stdout, b"01234567");
        assert_eq!(d.stderr, b"abcdefgh");
    }

    #[test]
    fn a_length_is_big_endian_not_little() {
        // 256 bytes is the discriminating length: BE encodes [0,0,1,0] and LE
        // [0,1,0,0], so a byte-order mistake reads 1 byte instead of 256 and
        // then desynchronises the whole stream. A 1-byte payload cannot tell
        // the two apart, which is why this uses 256.
        let payload = vec![b'x'; 256];
        let d = demux(&frame(1, &payload)).expect("256-byte frame");
        assert_eq!(d.stdout.len(), 256);
    }

    #[test]
    fn an_empty_stream_is_empty_output_not_an_error() {
        let d = demux(&[]).expect("no output is a valid outcome");
        assert!(d.stdout.is_empty() && d.stderr.is_empty());
    }

    #[test]
    fn a_zero_length_frame_is_valid_and_consumes_its_header() {
        // podman emits these. Treating a 0-length frame as end-of-stream would
        // silently drop everything after it.
        let mut s = frame(1, b"");
        s.extend(frame(1, b"after"));
        assert_eq!(demux(&s).unwrap().stdout, b"after");
    }

    #[test]
    fn a_truncated_payload_is_refused_not_returned_partial() {
        // Returning what arrived is how a caller concludes a command produced
        // no output when the stream was actually cut.
        let mut s = frame(1, b"twelve chars");
        s.truncate(s.len() - 4);
        let e = demux(&s).expect_err("a short frame must be refused");
        assert!(
            e.to_string().contains("truncated"),
            "the error must say the stream was cut: {e}"
        );
    }

    #[test]
    fn a_trailing_partial_header_is_refused() {
        let mut s = frame(1, b"ok");
        s.extend_from_slice(&[1, 0, 0]);
        assert!(demux(&s).is_err(), "3 trailing bytes cannot be a frame");
    }

    #[test]
    fn a_raw_tty_stream_is_refused_with_the_reason() {
        // A TTY-allocated container emits an UNFRAMED stream. Parsing it as
        // frames yields garbage lengths and arbitrary output; the error has to
        // name the cause, because "invalid stream" sends the reader to the
        // wrong place entirely.
        let e = demux(b"hello from a tty").expect_err("raw stream must be refused");
        let msg = e.to_string();
        assert!(
            msg.contains("TTY"),
            "the error must name the TTY cause: {msg}"
        );
    }

    #[test]
    fn stdin_echo_is_dropped_without_disturbing_the_others() {
        let mut s = frame(0, b"echoed input");
        s.extend(frame(1, b"real"));
        let d = demux(&s).unwrap();
        assert_eq!(d.stdout, b"real");
        assert!(d.stderr.is_empty());
    }

    // ── Confinement ───────────────────────────────────────────────────────
    //
    // ★ These exist because `securityContext` was read NOWHERE before
    // 2026-09-06. A Pod could declare runAsNonRoot, cap-drop-ALL and
    // readOnlyRootFilesystem and receive none of them, silently — the same
    // shape as the dropped mounts, one layer more dangerous.

    fn hardened() -> ContainerSpec {
        ContainerSpec {
            name: "c".to_string(),
            image: "img".to_string(),
            confinement: crate::backend::Confinement {
                privileged: false,
                read_only_root_fs: true,
                no_new_privileges: true,
                cap_drop: vec!["ALL".to_string()],
                cap_add: vec!["NET_BIND_SERVICE".to_string()],
                run_as_user: Some(65534),
                run_as_group: Some(65534),
            },
            ..ContainerSpec::default()
        }
    }

    #[test]
    fn a_hardened_pod_reaches_the_wire_hardened() {
        let r = create_request(&hardened(), None);
        assert!(
            r.read_only_filesystem,
            "readOnlyRootFilesystem was declared"
        );
        assert!(
            r.no_new_privileges,
            "allowPrivilegeEscalation:false was declared"
        );
        assert_eq!(r.cap_drop, vec!["ALL".to_string()]);
        assert_eq!(r.cap_add, vec!["NET_BIND_SERVICE".to_string()]);
        assert_eq!(r.user.as_deref(), Some("65534:65534"));
        assert!(!r.privileged);
    }

    #[test]
    fn the_kubernetes_default_adds_nothing_to_the_wire() {
        // A Pod that said nothing must produce the same request as before this
        // field existed — otherwise every existing workload changes behaviour.
        let spec = ContainerSpec {
            name: "c".to_string(),
            image: "img".to_string(),
            ..ContainerSpec::default()
        };
        let json = serde_json::to_string(&create_request(&spec, None)).unwrap();
        for absent in [
            "privileged",
            "read_only_filesystem",
            "no_new_privileges",
            "cap_add",
            "cap_drop",
            "user",
        ] {
            assert!(
                !json.contains(absent),
                "a default disposition must not appear on the wire: {absent} in {json}"
            );
        }
    }

    #[test]
    fn a_gid_without_a_uid_is_omitted_rather_than_run_as_root() {
        // podman's `user` is one string. Carrying a lone gid would mean
        // inventing a uid, and the obvious invention is 0 — which would run the
        // container AS ROOT while the Pod was asking to be constrained.
        let spec = ContainerSpec {
            name: "c".to_string(),
            image: "img".to_string(),
            confinement: crate::backend::Confinement {
                run_as_group: Some(1000),
                ..crate::backend::Confinement::default()
            },
            ..ContainerSpec::default()
        };
        assert_eq!(create_request(&spec, None).user, None);
    }

    #[test]
    fn a_uid_alone_is_carried_without_a_colon() {
        let spec = ContainerSpec {
            name: "c".to_string(),
            image: "img".to_string(),
            confinement: crate::backend::Confinement {
                run_as_user: Some(1000),
                ..crate::backend::Confinement::default()
            },
            ..ContainerSpec::default()
        };
        assert_eq!(create_request(&spec, None).user.as_deref(), Some("1000"));
    }

    // ── Mounts ────────────────────────────────────────────────────────────
    //
    // ★ These exist because the first version of this backend set
    // `mounts: Vec::new()` unconditionally while ContainerSpec carried them,
    // so EVERY volume — ConfigMap, Secret, emptyDir, PVC — was silently
    // dropped. The container started, so nothing failed; the workload simply
    // could not read a file it was promised. That is the exact shape this
    // whole module was written to eliminate, reintroduced by the module
    // itself, and no test caught it because none asserted on mounts at all.

    #[test]
    fn a_host_dir_mount_reaches_the_request_as_a_bind() {
        let spec = ContainerSpec {
            name: "c".to_string(),
            image: "img".to_string(),
            mounts: vec![crate::pod_volume::ResolvedMount {
                source: crate::pod_volume::MountSource::HostDir("/host/cm".into()),
                mount_path: "/etc/config".to_string(),
                read_only: true,
                sub_path: None,
            }],
            ..ContainerSpec::default()
        };
        let req = create_request(&spec, None);
        assert_eq!(req.mounts.len(), 1, "the mount must not be dropped");
        let m = &req.mounts[0];
        assert_eq!(m.destination, "/etc/config");
        assert_eq!(m.source, "/host/cm");
        assert_eq!(m.mount_type, "bind");
        assert!(
            m.options.contains(&"ro".to_string()),
            "a ConfigMap/Secret mount is read-only in Kubernetes semantics; got {:?}",
            m.options
        );
    }

    #[test]
    fn a_named_volume_is_a_volume_not_a_bind() {
        // podman infers bind-vs-volume from whether the CLI source looks like a
        // path. Over the API it is an explicit field, so it must be set — a
        // named volume sent as `bind` would create a host directory named after
        // the volume instead of using the volume.
        let spec = ContainerSpec {
            name: "c".to_string(),
            image: "img".to_string(),
            mounts: vec![crate::pod_volume::ResolvedMount {
                source: crate::pod_volume::MountSource::NamedVolume("scratch".to_string()),
                mount_path: "/scratch".to_string(),
                read_only: false,
                sub_path: None,
            }],
            ..ContainerSpec::default()
        };
        let m = &create_request(&spec, None).mounts[0];
        assert_eq!(m.mount_type, "volume");
        assert_eq!(m.source, "scratch");
        assert!(m.options.contains(&"rw".to_string()));
    }

    #[test]
    fn a_pvc_host_dir_is_a_bind_of_its_path() {
        let spec = ContainerSpec {
            name: "c".to_string(),
            image: "img".to_string(),
            mounts: vec![crate::pod_volume::ResolvedMount {
                source: crate::pod_volume::MountSource::PvcHostDir {
                    path: "/mnt/pv-7".into(),
                    read_only: false,
                },
                mount_path: "/data".to_string(),
                read_only: false,
                sub_path: None,
            }],
            ..ContainerSpec::default()
        };
        let m = &create_request(&spec, None).mounts[0];
        assert_eq!(m.mount_type, "bind");
        assert_eq!(m.source, "/mnt/pv-7");
    }

    #[test]
    fn every_mount_survives_translation() {
        // The count assertion is the one that would have caught the original
        // defect. A per-field assertion on mounts[0] passes happily while
        // mounts 1..n are dropped.
        let mk = |n: u8| crate::pod_volume::ResolvedMount {
            source: crate::pod_volume::MountSource::HostDir(format!("/h/{n}").into()),
            mount_path: format!("/c/{n}"),
            read_only: false,
            sub_path: None,
        };
        let spec = ContainerSpec {
            name: "c".to_string(),
            image: "img".to_string(),
            mounts: vec![mk(1), mk(2), mk(3)],
            ..ContainerSpec::default()
        };
        assert_eq!(create_request(&spec, None).mounts.len(), 3);
    }

    #[test]
    fn a_created_container_never_self_removes() {
        // `remove: true` would delete the container on exit, which makes a
        // terminal exit code unobservable — and the kubelet's whole restart
        // decision reads that exit code.
        let spec = ContainerSpec {
            name: "c".to_string(),
            image: "img".to_string(),
            ..ContainerSpec::default()
        };
        assert!(!create_request(&spec, None).remove);
    }

    #[test]
    fn an_absent_pull_policy_is_omitted_rather_than_null() {
        // podman distinguishes an unset field from an explicit null on several
        // options, so an omitted policy must take podman's default rather than
        // assert one.
        let spec = ContainerSpec {
            name: "c".to_string(),
            image: "img".to_string(),
            ..ContainerSpec::default()
        };
        let json = serde_json::to_string(&create_request(&spec, None)).unwrap();
        assert!(
            !json.contains("pull_policy"),
            "an unset pull policy must not appear in the body: {json}"
        );
    }

    #[test]
    fn a_container_with_no_address_reports_none_not_empty_string() {
        let r = InspectResponse {
            id: "abc".to_string(),
            state: InspectState {
                status: "created".to_string(),
                running: false,
                exit_code: 0,
            },
            network_settings: Some(NetworkSettings::default()),
        };
        assert_eq!(
            r.pod_ip(),
            None,
            "an empty string presented as an address reaches Pod status and \
             looks like a real value"
        );
    }

    #[test]
    fn a_named_network_address_wins_over_the_legacy_field() {
        let mut networks = BTreeMap::new();
        networks.insert(
            "engenho-net".to_string(),
            NetworkDetail {
                ip_address: "10.89.1.7".to_string(),
            },
        );
        let r = InspectResponse {
            id: "abc".to_string(),
            state: InspectState::default(),
            network_settings: Some(NetworkSettings {
                networks,
                ip_address: "172.17.0.2".to_string(),
            }),
        };
        assert_eq!(r.pod_ip().as_deref(), Some("10.89.1.7"));
    }

    #[test]
    fn inspect_deserialises_from_podmans_actual_field_casing() {
        // Podman capitalises these; serde would silently leave every field at
        // its Default if the rename attributes were wrong, producing a
        // container that always reads as not-running with exit code 0 — a
        // crash-loop that looks like a workload bug.
        let raw = r#"{
            "Id": "deadbeef",
            "State": {"Status": "running", "Running": true, "ExitCode": 0},
            "NetworkSettings": {"Networks": {"engenho-net": {"IPAddress": "10.89.1.9"}}}
        }"#;
        let r: InspectResponse = serde_json::from_str(raw).expect("inspect must parse");
        assert_eq!(r.id, "deadbeef");
        assert!(r.state.running);
        assert_eq!(r.pod_ip().as_deref(), Some("10.89.1.9"));
    }
}
