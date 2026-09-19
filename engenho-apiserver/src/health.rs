//! Health + version endpoints — the probes kubectl / client-go / kubeadm
//! / controllers run BEFORE they will trust a server.
//!
//!   * `GET /version` → the `version.Info` JSON kubectl negotiates
//!     against. Absent → kubectl prints "couldn't get current server API
//!     group list" and refuses to talk to the server. This is the
//!     load-bearing reason a real kubectl rejected engenho before M0.4.
//!   * `GET /livez`   → is every child of this process alive?
//!   * `GET /healthz` → the legacy name for the same question.
//!   * `GET /readyz`  → may this node take traffic? (`kubectl wait`,
//!     kubeadm and load balancers poll it.)
//!
//! ★ HEALTH IS DERIVED FROM OBSERVATION, NEVER A CONSTANT. These three
//! used to return the fixed string `"ok"`. On plo (2026-09-06) a
//! controller retried hot enough to peg a core and hang every API read
//! while `/healthz` said ok. Each endpoint now aggregates named checks
//! read from a [`LivenessSource`] the runtime installs:
//!
//!   * `/livez`, `/healthz`: one check per child, passing only when the
//!     child is [`Liveness::Alive`]. Unknown, Stalled and Dead all fail.
//!   * `/readyz`: a linearizable store read that answers within
//!     [`DEFAULT_STORE_READ_TIMEOUT`] (`store`), one check per child that
//!     passes once the child has been observed at all, and the node not
//!     draining (`shutdown`). It deliberately does NOT ask "am I the
//!     leader": that would make every follower unready.
//!
//! ★ NOTHING OBSERVED IS NEVER OK. A router with no source installed fails
//! every endpoint on one `liveness-source` check, and a source reporting no
//! children fails on a `children` check. A [`Report`] with no checks cannot
//! pass either. So the constant answer cannot come back by forgetting to
//! wire something.
//!
//! ★ THE BODY IS UPSTREAM'S. `k8s.io/apiserver/pkg/server/healthz`: a
//! passing endpoint answers `ok`, or with `?verbose` one `[+]<check> ok`
//! line per check and `<endpoint> check passed`; a failing one always
//! answers 500 with every line, `[-]<check> failed: reason withheld` for
//! the failures, and `<endpoint> check failed`. The reason is withheld from
//! the body because these paths are pre-authz; it goes to the log.
//!
//! Tier: "unwired or empty never renders ok" is structural in [`gather`],
//! the only producer of a [`Report`]. Whether the runtime's source reflects
//! its real children is the runtime's (integration) responsibility.
//!
//! `/version` is a typed serde struct (`VersionInfo`) — NEVER
//! `serde_json::json!()` of an ad-hoc map (per the ★★ TYPED EMISSION
//! rule). The version is sourced from the SINGLE
//! [`engenho_types::KUBE_VERSION`] anchor so `/version`, discovery, and
//! the vendored OpenAPI surface can never drift.

use std::fmt;
use std::time::Duration;

use axum::Json;
use axum::extract::{RawQuery, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use engenho_store::{Revision, StoreError};
pub use engenho_substrate::freshness::Liveness;
use engenho_types::{KUBE_VERSION, KUBE_VERSION_MAJOR, KUBE_VERSION_MINOR};

use crate::router::RouterState;

/// Mirror of upstream `k8s.io/apimachinery/pkg/version.Info` — the JSON
/// body served at `GET /version`. Field names are camelCase per the K8s
/// wire contract (serde `rename` where Rust idiom diverges).
///
/// We are not a Go server, so `goVersion` is empty and `compiler` is
/// `rustc`; everything kubectl actually negotiates on (`major`, `minor`,
/// `gitVersion`) is sourced from [`engenho_types::KUBE_VERSION`].
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct VersionInfo {
    /// Major version (`"1"`).
    pub major: &'static str,
    /// Minor version (`"34"`).
    pub minor: &'static str,
    /// `vMAJOR.MINOR.PATCH` — what kubectl version-skew-checks against.
    #[serde(rename = "gitVersion")]
    pub git_version: &'static str,
    /// Commit the build was cut from. Empty until we wire a build-time
    /// stamp (no `unimplemented!()` — an empty string is a truthful
    /// "unknown commit", which kubectl tolerates).
    #[serde(rename = "gitCommit")]
    pub git_commit: &'static str,
    /// Working-tree state at build time. `clean` for released builds.
    #[serde(rename = "gitTreeState")]
    pub git_tree_state: &'static str,
    /// Build timestamp. Empty until a build-time stamp is wired.
    #[serde(rename = "buildDate")]
    pub build_date: &'static str,
    /// Go runtime version — empty: engenho is Rust, not Go.
    #[serde(rename = "goVersion")]
    pub go_version: &'static str,
    /// Compiler that produced the binary.
    pub compiler: &'static str,
    /// `<os>/<arch>` of the running binary.
    pub platform: &'static str,
}

impl VersionInfo {
    /// The version this build reports. Sources `major` / `minor` /
    /// `gitVersion` from the single [`engenho_types::KUBE_VERSION`]
    /// anchor; `platform` from the compile-time target triple.
    #[must_use]
    pub fn current() -> Self {
        Self {
            major: KUBE_VERSION_MAJOR,
            minor: KUBE_VERSION_MINOR,
            git_version: KUBE_VERSION,
            git_commit: "",
            git_tree_state: "clean",
            build_date: "",
            go_version: "",
            compiler: "rustc",
            // `<os>/<arch>` — the K8s `version.Info.platform` shape. The
            // pairs are enumerated as static literals (no `format!`, per
            // the ★★ TYPED EMISSION rule) keyed on the compile-time
            // target the binary was built for. Note rust's arch spelling
            // (`x86_64`/`aarch64`) differs from Go's (`amd64`/`arm64`);
            // kubectl only DISPLAYS platform (never parses it), so the
            // rust spelling is truthful + harmless.
            platform: match (std::env::consts::OS, std::env::consts::ARCH) {
                ("linux", "x86_64") => "linux/x86_64",
                ("linux", "aarch64") => "linux/aarch64",
                ("macos", "x86_64") => "darwin/x86_64",
                ("macos", "aarch64") => "darwin/aarch64",
                // Any target we haven't enumerated: a truthful fallback
                // (kubectl only displays it). Engenho's ship targets are
                // linux + darwin on x86_64 + aarch64.
                _ => "unknown/unknown",
            },
        }
    }
}

// ── axum route handlers ────────────────────────────────────────────────

/// `GET /version` → the typed [`VersionInfo`]. 200.
pub async fn version() -> impl IntoResponse {
    Json(VersionInfo::current())
}

// ── derived health: /livez, /healthz, /readyz ──────────────────────────

/// How long `/readyz` waits for the store read before failing its `store`
/// check: two seconds, the default of upstream's `--etcd-readycheck-timeout`.
/// The runtime may install another through
/// [`crate::router::RouterState::with_readyz_store_read_timeout`].
pub const DEFAULT_STORE_READ_TIMEOUT: Duration = Duration::from_secs(2);

/// The check every endpoint fails when no [`LivenessSource`] is installed.
pub const UNWIRED_CHECK: &str = "liveness-source";
/// The check every endpoint fails when the source reports no children.
pub const CHILDREN_CHECK: &str = "children";
/// `/readyz`'s linearizable store read.
pub const STORE_CHECK: &str = "store";
/// `/readyz`'s drain check, under upstream's name for it.
pub const SHUTDOWN_CHECK: &str = "shutdown";

/// One child's liveness, as a [`LivenessSource`] reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChildLiveness {
    /// The child's check name: the `<name>` in `[+]<name> ok`. A
    /// `&'static str`, so a check name comes from code (the runtime's
    /// closed child catalog), never from a request or an object.
    pub name: &'static str,
    /// What was observed of it, judged by [`Liveness::judge`].
    pub liveness: Liveness,
}

/// Whether this node has begun draining.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainState {
    /// Taking traffic.
    Serving,
    /// Shutting down: `/readyz` fails its `shutdown` check so load
    /// balancers stop routing here before the listener closes.
    Draining,
}

/// What the health endpoints read.
///
/// Implemented by the runtime over its supervised children and its store;
/// [`FixedLiveness`] is the test double. Installed with
/// [`crate::router::RouterState::with_liveness_source`].
#[async_trait::async_trait]
pub trait LivenessSource: Send + Sync {
    /// Every supervised child, judged now, in the order to render them.
    fn children(&self) -> Vec<ChildLiveness>;

    /// Whether this node is draining.
    fn drain_state(&self) -> DrainState;

    /// One LINEARIZABLE store read: one that observes every write committed
    /// before it began (a Raft read-index), not a local state-machine read,
    /// which a partitioned follower would answer from stale state.
    /// `/readyz` bounds it with a timeout, so an implementation may block.
    ///
    /// # Errors
    ///
    /// The store's own [`StoreError`] when the read cannot be served.
    async fn linearizable_read(&self) -> Result<Revision, StoreError>;
}

/// The three derived-health endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endpoint {
    /// `GET /livez`.
    Livez,
    /// `GET /healthz`.
    Healthz,
    /// `GET /readyz`.
    Readyz,
}

impl Endpoint {
    /// The name upstream prints in `<name> check passed`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Livez => "livez",
            Self::Healthz => "healthz",
            Self::Readyz => "readyz",
        }
    }
}

/// Why one check failed. Logged, never put in the (pre-authz) body.
#[derive(Debug, thiserror::Error)]
pub enum CheckFailure {
    /// No [`LivenessSource`] is installed in this server.
    #[error("no liveness source is installed in this server")]
    Unwired,
    /// The source reported no children at all.
    #[error("the liveness source reported no children")]
    NoChildren,
    /// The child has not reported since it was spawned.
    #[error("never observed since it was spawned")]
    Unobserved,
    /// The child is running but quiet past its window.
    #[error("stalled: last observed at {since}")]
    Stalled {
        /// When it last reported.
        since: engenho_substrate::Instant,
    },
    /// The child's task has ended.
    #[error("dead: its task has ended")]
    Dead,
    /// The store refused the read.
    #[error("store read failed: {0}")]
    StoreRead(StoreError),
    /// The store did not answer in time.
    #[error("store read did not answer within {0:?}")]
    StoreReadTimedOut(Duration),
    /// The node is draining.
    #[error("the node is draining")]
    Draining,
}

/// One named check and its verdict.
#[derive(Debug)]
pub struct Check {
    /// The check's name.
    pub name: &'static str,
    /// `Ok` when it passed.
    pub verdict: Result<(), CheckFailure>,
}

/// One endpoint's checks. Built only by [`gather`].
#[derive(Debug)]
pub struct Report {
    endpoint: Endpoint,
    checks: Vec<Check>,
}

impl Report {
    /// The endpoint this report answers.
    #[must_use]
    pub fn endpoint(&self) -> Endpoint {
        self.endpoint
    }

    /// The checks, in rendered order.
    #[must_use]
    pub fn checks(&self) -> &[Check] {
        &self.checks
    }

    /// Whether every check passed. A report with no checks has observed
    /// nothing, so it does not pass.
    #[must_use]
    pub fn passed(&self) -> bool {
        !self.checks.is_empty() && self.checks.iter().all(|c| c.verdict.is_ok())
    }

    /// 200 when [`Self::passed`], otherwise 500 (upstream's status).
    #[must_use]
    pub fn status(&self) -> StatusCode {
        if self.passed() {
            StatusCode::OK
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }

    /// The response body: `ok` for a pass without `?verbose`, otherwise
    /// every check line and the verdict line. A failure is always verbose.
    #[must_use]
    pub fn body(&self, verbose: bool) -> String {
        if self.passed() && !verbose {
            return String::from("ok");
        }
        VerboseBody(self).to_string()
    }
}

/// Upstream's verbose body: one `[+]`/`[-]` line per check, then the verdict.
struct VerboseBody<'a>(&'a Report);

impl fmt::Display for VerboseBody<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for check in &self.0.checks {
            match check.verdict {
                Ok(()) => writeln!(f, "[+]{} ok", check.name)?,
                Err(_) => writeln!(f, "[-]{} failed: reason withheld", check.name)?,
            }
        }
        let outcome = if self.0.passed() { "passed" } else { "failed" };
        writeln!(f, "{} check {outcome}", self.0.endpoint.name())
    }
}

/// Liveness: only an Alive child passes.
fn judge_alive(liveness: Liveness) -> Result<(), CheckFailure> {
    match liveness {
        Liveness::Alive => Ok(()),
        Liveness::Unknown => Err(CheckFailure::Unobserved),
        Liveness::Stalled { since } => Err(CheckFailure::Stalled { since }),
        Liveness::Dead => Err(CheckFailure::Dead),
    }
}

/// Readiness: a child passes once it has been observed at all. A stalled
/// or dead child shows on `/livez`; it does not stop this node serving.
fn judge_observed(liveness: Liveness) -> Result<(), CheckFailure> {
    match liveness {
        Liveness::Unknown => Err(CheckFailure::Unobserved),
        Liveness::Alive | Liveness::Stalled { .. } | Liveness::Dead => Ok(()),
    }
}

fn child_checks(
    source: &dyn LivenessSource,
    judge: fn(Liveness) -> Result<(), CheckFailure>,
) -> Vec<Check> {
    let children = source.children();
    if children.is_empty() {
        return vec![Check {
            name: CHILDREN_CHECK,
            verdict: Err(CheckFailure::NoChildren),
        }];
    }
    children
        .into_iter()
        .map(|c| Check {
            name: c.name,
            verdict: judge(c.liveness),
        })
        .collect()
}

async fn store_check(source: &dyn LivenessSource, timeout: Duration) -> Check {
    let verdict = match tokio::time::timeout(timeout, source.linearizable_read()).await {
        Ok(Ok(_revision)) => Ok(()),
        Ok(Err(e)) => Err(CheckFailure::StoreRead(e)),
        Err(_elapsed) => Err(CheckFailure::StoreReadTimedOut(timeout)),
    };
    Check {
        name: STORE_CHECK,
        verdict,
    }
}

/// Gather `endpoint`'s checks from `source`. The only producer of a
/// [`Report`]: no source, or a source with no children, yields a failing
/// check, never an empty pass.
pub async fn gather(
    endpoint: Endpoint,
    source: Option<&dyn LivenessSource>,
    store_read_timeout: Duration,
) -> Report {
    let Some(source) = source else {
        return Report {
            endpoint,
            checks: vec![Check {
                name: UNWIRED_CHECK,
                verdict: Err(CheckFailure::Unwired),
            }],
        };
    };
    let checks = match endpoint {
        Endpoint::Livez | Endpoint::Healthz => child_checks(source, judge_alive),
        Endpoint::Readyz => {
            let mut checks = vec![store_check(source, store_read_timeout).await];
            checks.extend(child_checks(source, judge_observed));
            checks.push(Check {
                name: SHUTDOWN_CHECK,
                verdict: match source.drain_state() {
                    DrainState::Serving => Ok(()),
                    DrainState::Draining => Err(CheckFailure::Draining),
                },
            });
            checks
        }
    };
    Report { endpoint, checks }
}

/// Upstream reads `?verbose` as a KEY: present with any value, or none.
fn wants_verbose(query: Option<&str>) -> bool {
    query.is_some_and(|q| {
        q.split('&')
            .any(|pair| pair.split_once('=').map_or(pair, |(key, _)| key) == "verbose")
    })
}

async fn serve(endpoint: Endpoint, state: &RouterState, query: Option<&str>) -> Response {
    let report = gather(
        endpoint,
        state.liveness.as_deref(),
        state.readyz_store_read_timeout,
    )
    .await;
    for check in report.checks() {
        if let Err(failure) = &check.verdict {
            tracing::warn!(
                endpoint = endpoint.name(),
                check = check.name,
                reason = %failure,
                "health check failed"
            );
        }
    }
    (
        report.status(),
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        report.body(wants_verbose(query)),
    )
        .into_response()
}

/// `GET /livez`: every child alive.
pub async fn livez(State(state): State<RouterState>, RawQuery(query): RawQuery) -> Response {
    serve(Endpoint::Livez, &state, query.as_deref()).await
}

/// `GET /healthz`: the legacy name for `/livez`.
pub async fn healthz(State(state): State<RouterState>, RawQuery(query): RawQuery) -> Response {
    serve(Endpoint::Healthz, &state, query.as_deref()).await
}

/// `GET /readyz`: a linearizable store read, every child observed, not
/// draining.
pub async fn readyz(State(state): State<RouterState>, RawQuery(query): RawQuery) -> Response {
    serve(Endpoint::Readyz, &state, query.as_deref()).await
}

// ── test double ────────────────────────────────────────────────────────

/// What [`FixedLiveness`]'s store read does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FixedStoreRead {
    /// Answers at this revision.
    Answers(Revision),
    /// Returns a [`StoreError`].
    Fails,
    /// Never answers.
    Hangs,
}

/// A [`LivenessSource`] that reports fixed answers: the test double for
/// the runtime's source.
#[derive(Debug, Clone)]
pub struct FixedLiveness {
    children: Vec<ChildLiveness>,
    drain: DrainState,
    store: FixedStoreRead,
}

impl FixedLiveness {
    /// These children, serving, with a store read that answers.
    #[must_use]
    pub fn new(children: Vec<ChildLiveness>) -> Self {
        Self {
            children,
            drain: DrainState::Serving,
            store: FixedStoreRead::Answers(Revision(1)),
        }
    }

    /// One [`Liveness::Alive`] child per name, serving, store answering.
    #[must_use]
    pub fn all_alive(names: &[&'static str]) -> Self {
        Self::new(
            names
                .iter()
                .map(|name| ChildLiveness {
                    name,
                    liveness: Liveness::Alive,
                })
                .collect(),
        )
    }

    /// Report `drain` as the drain state.
    #[must_use]
    pub fn with_drain_state(mut self, drain: DrainState) -> Self {
        self.drain = drain;
        self
    }

    /// Make the store read behave as `store`.
    #[must_use]
    pub fn with_store_read(mut self, store: FixedStoreRead) -> Self {
        self.store = store;
        self
    }
}

#[async_trait::async_trait]
impl LivenessSource for FixedLiveness {
    fn children(&self) -> Vec<ChildLiveness> {
        self.children.clone()
    }

    fn drain_state(&self) -> DrainState {
        self.drain
    }

    async fn linearizable_read(&self) -> Result<Revision, StoreError> {
        match self.store {
            FixedStoreRead::Answers(revision) => Ok(revision),
            FixedStoreRead::Fails => Err(StoreError::Fatal(String::from(
                "FixedLiveness: the store read is set to fail",
            ))),
            FixedStoreRead::Hangs => std::future::pending().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_info_reports_vendored_surface() {
        let v = VersionInfo::current();
        assert_eq!(v.major, "1");
        assert_eq!(v.minor, "34");
        assert_eq!(v.git_version, "v1.34.0");
        // The drift guard: /version MUST equal the single KUBE_VERSION
        // anchor so it can never diverge from the generated_v1_34 surface.
        assert_eq!(v.git_version, engenho_types::KUBE_VERSION);
        assert_eq!(v.compiler, "rustc");
    }

    #[test]
    fn version_info_serializes_camel_case() {
        let v = VersionInfo::current();
        let json = serde_json::to_value(&v).unwrap();
        // camelCase field names per the K8s version.Info wire contract.
        assert_eq!(json.get("major").unwrap(), "1");
        assert_eq!(json.get("minor").unwrap(), "34");
        assert_eq!(json.get("gitVersion").unwrap(), "v1.34.0");
        assert_eq!(json.get("gitTreeState").unwrap(), "clean");
        assert_eq!(json.get("compiler").unwrap(), "rustc");
        // These keys (not snake_case) MUST be present for client-go.
        assert!(json.get("gitCommit").is_some());
        assert!(json.get("buildDate").is_some());
        assert!(json.get("goVersion").is_some());
        assert!(json.get("platform").is_some());
        // No snake_case leakage.
        assert!(json.get("git_version").is_none());
    }

    #[test]
    fn platform_is_os_slash_arch() {
        let p = VersionInfo::current().platform;
        assert!(p.contains('/'), "platform {p:?} must be <os>/<arch>");
    }

    // ── derived health ────────────────────────────────────────────────

    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::Request;
    use engenho_substrate::Instant;

    fn child(name: &'static str, liveness: Liveness) -> ChildLiveness {
        ChildLiveness { name, liveness }
    }

    fn stalled() -> Liveness {
        Liveness::Stalled {
            since: Instant::from_ms(1_000),
        }
    }

    /// The full router (request-info, authn and authz layers included), so
    /// these go through the same path a probe does.
    fn app(source: FixedLiveness) -> axum::Router {
        crate::router::build(
            RouterState::new(vec![])
                .with_liveness_source(Arc::new(source))
                .with_readyz_store_read_timeout(Duration::from_millis(50)),
        )
    }

    async fn get(app: &axum::Router, uri: &str) -> (StatusCode, String) {
        use tower::ServiceExt as _;
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn every_child_alive_passes_livez_and_healthz_with_upstreams_bodies() {
        let app = app(FixedLiveness::all_alive(&["deployment", "kubelet"]));
        for endpoint in ["livez", "healthz"] {
            let (status, body) = get(&app, &["/", endpoint].concat()).await;
            assert_eq!(
                (status, body.as_str()),
                (StatusCode::OK, "ok"),
                "{endpoint}"
            );
            let (status, body) = get(&app, &["/", endpoint, "?verbose"].concat()).await;
            assert_eq!(status, StatusCode::OK, "{endpoint}?verbose");
            assert_eq!(
                body,
                [
                    "[+]deployment ok\n[+]kubelet ok\n",
                    endpoint,
                    " check passed\n"
                ]
                .concat()
            );
        }
    }

    #[tokio::test]
    async fn an_unknown_child_fails_livez_and_healthz() {
        let app = app(FixedLiveness::new(vec![
            child("deployment", Liveness::Alive),
            child("kubelet", Liveness::Unknown),
        ]));
        for endpoint in ["livez", "healthz"] {
            let (status, body) = get(&app, &["/", endpoint].concat()).await;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{endpoint}");
            // A failure is always verbose, and the reason is withheld.
            assert_eq!(
                body,
                [
                    "[+]deployment ok\n[-]kubelet failed: reason withheld\n",
                    endpoint,
                    " check failed\n"
                ]
                .concat()
            );
        }
    }

    #[tokio::test]
    async fn a_stalled_child_fails_livez() {
        let app = app(FixedLiveness::new(vec![
            child("deployment", Liveness::Alive),
            child("scheduler", stalled()),
        ]));
        let (status, body) = get(&app, "/livez").await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            body.contains("[-]scheduler failed: reason withheld\n"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn a_dead_child_fails_livez() {
        let app = app(FixedLiveness::new(vec![
            child("deployment", Liveness::Alive),
            child("kubelet", Liveness::Dead),
        ]));
        let (status, body) = get(&app, "/livez").await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            body.contains("[-]kubelet failed: reason withheld\n"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn an_unwired_router_is_red_on_every_endpoint() {
        let app = crate::router::build(RouterState::new(vec![]));
        for endpoint in ["livez", "healthz", "readyz"] {
            let (status, body) = get(&app, &["/", endpoint].concat()).await;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{endpoint}");
            assert_eq!(
                body,
                [
                    "[-]liveness-source failed: reason withheld\n",
                    endpoint,
                    " check failed\n"
                ]
                .concat()
            );
        }
    }

    #[tokio::test]
    async fn a_source_that_reports_no_children_is_not_ok() {
        let app = app(FixedLiveness::new(vec![]));
        for endpoint in ["livez", "healthz", "readyz"] {
            let (status, body) = get(&app, &["/", endpoint].concat()).await;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{endpoint}");
            assert!(
                body.contains("[-]children failed: reason withheld\n"),
                "{endpoint}: {body}"
            );
        }
    }

    #[tokio::test]
    async fn readyz_passes_on_a_store_read_observed_children_and_serving() {
        // A stalled or dead child is red on /livez, but it has booted: it
        // does not stop this node serving.
        let app = app(FixedLiveness::new(vec![
            child("deployment", Liveness::Alive),
            child("scheduler", stalled()),
            child("kubelet", Liveness::Dead),
        ]));
        let (status, body) = get(&app, "/readyz").await;
        assert_eq!((status, body.as_str()), (StatusCode::OK, "ok"));
        let (_, body) = get(&app, "/readyz?verbose").await;
        assert_eq!(
            body,
            "[+]store ok\n[+]deployment ok\n[+]scheduler ok\n[+]kubelet ok\n\
             [+]shutdown ok\nreadyz check passed\n"
        );
    }

    #[tokio::test]
    async fn readyz_fails_while_any_child_is_unknown() {
        let app = app(FixedLiveness::new(vec![
            child("deployment", Liveness::Alive),
            child("kubelet", Liveness::Unknown),
        ]));
        let (status, body) = get(&app, "/readyz").await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            body.contains("[-]kubelet failed: reason withheld\n"),
            "{body}"
        );
        assert!(body.contains("[+]store ok\n"), "{body}");
    }

    #[tokio::test]
    async fn readyz_fails_when_the_store_read_fails_or_does_not_answer_in_time() {
        for store in [FixedStoreRead::Fails, FixedStoreRead::Hangs] {
            let app = app(FixedLiveness::all_alive(&["deployment"]).with_store_read(store));
            let (status, body) = get(&app, "/readyz").await;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{store:?}");
            assert!(
                body.starts_with("[-]store failed: reason withheld\n"),
                "{store:?}: {body}"
            );
            // Liveness never reads the store: a hung store is a readiness
            // problem, not a reason to restart the process.
            let (status, _) = get(&app, "/livez").await;
            assert_eq!(status, StatusCode::OK, "{store:?}");
        }
    }

    #[tokio::test]
    async fn readyz_fails_while_draining() {
        let app =
            app(FixedLiveness::all_alive(&["deployment"]).with_drain_state(DrainState::Draining));
        let (status, body) = get(&app, "/readyz").await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            body.contains("[-]shutdown failed: reason withheld\n"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn verbose_is_read_as_a_key_whatever_its_value() {
        let app = app(FixedLiveness::all_alive(&["deployment"]));
        for query in ["?verbose", "?verbose=", "?verbose=false", "?x=1&verbose"] {
            let (_, body) = get(&app, &["/livez", query].concat()).await;
            assert_eq!(body, "[+]deployment ok\nlivez check passed\n", "{query}");
        }
        let (_, body) = get(&app, "/livez?verbosely").await;
        assert_eq!(body, "ok");
    }

    #[tokio::test]
    async fn the_failure_reason_is_typed_for_the_log() {
        let source = FixedLiveness::new(vec![
            child("a", Liveness::Unknown),
            child("b", stalled()),
            child("c", Liveness::Dead),
        ])
        .with_store_read(FixedStoreRead::Hangs)
        .with_drain_state(DrainState::Draining);
        let report = gather(Endpoint::Livez, Some(&source), Duration::from_millis(10)).await;
        let reasons: Vec<String> = report
            .checks()
            .iter()
            .map(|c| c.verdict.as_ref().map_err(ToString::to_string).unwrap_err())
            .collect();
        assert_eq!(
            reasons,
            [
                "never observed since it was spawned",
                "stalled: last observed at 1000.00000",
                "dead: its task has ended",
            ]
        );
        let report = gather(Endpoint::Readyz, Some(&source), Duration::from_millis(10)).await;
        let first = report
            .checks()
            .first()
            .map(|c| (c.name, c.verdict.is_err()));
        assert_eq!(first, Some((STORE_CHECK, true)));
        assert!(matches!(
            report.checks().last().map(|c| &c.verdict),
            Some(Err(CheckFailure::Draining))
        ));
        assert!(matches!(
            report.checks().first().map(|c| &c.verdict),
            Some(Err(CheckFailure::StoreReadTimedOut(_)))
        ));
    }

    #[test]
    fn a_report_with_no_checks_never_passes() {
        let empty = Report {
            endpoint: Endpoint::Livez,
            checks: vec![],
        };
        assert!(!empty.passed());
        assert_eq!(empty.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
