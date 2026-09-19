//! W7 — the backend conformance matrix: every backend kind the kubelet can be
//! configured with, held to one `ContainerRuntime` contract.
//!
//! ## The rows
//!
//! [`conformance_matrix!`] declares one row per [`KubeletBackendKind`] and
//! generates, from that ONE token list, both the exhaustive `match` that
//! answers "what does this kind promise" and [`EVERY_KIND`], the list the
//! matrix walks. So:
//!
//! - a kind added to the enum without a row does not compile (E0004 — the
//!   `match` has no wildcard, and `#[deny(unreachable_patterns)]` makes a
//!   duplicated row a compile error too);
//! - a kind that has a row is walked, because the list and the arms are the
//!   same tokens.
//!
//! Tier, stated: a compile error in THIS TEST TARGET. `cargo build` of the
//! library does not see it; `cargo test` and `cargo clippy --all-targets` do.
//!
//! A row is either [`Row::Refused`] — construction must refuse, with the
//! reasons the row names (CRI since T5.9) — or [`Row::Runs`], a [`Contract`]
//! the built runtime is held to, clause by clause ([`Clause`]).
//!
//! ## A runtime that is not here is RECORDED, never skipped
//!
//! podman and the libpod socket exist on rio and plo, not on a laptop. A row
//! whose runtime is absent gets the verdict [`Verdict::Unavailable`], carrying
//! what was probed and what was found. It is printed with the table, written
//! to `$CARGO_TARGET_TMPDIR/backend-conformance.txt`, and fails the matrix
//! wherever it is required:
//!
//! ```text
//! ENGENHO_CONFORMANCE_REQUIRE=podman_api,podman cargo test -p engenho-kubelet --test backend_conformance
//! ENGENHO_CONFORMANCE_REQUIRE=all                ...
//! ```
//!
//! A selector that names no backend kind is refused rather than read as
//! "require nothing". A row that needs nothing (the fake) has no
//! unavailable path at all.

#![allow(
    clippy::disallowed_methods,
    reason = "holds each runtime to its own contract directly; no kubelet, so no start curve"
)]

use std::fmt;
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use engenho_kubelet::KubeletError;
use engenho_kubelet::backend::{
    ContainerRuntime, ContainerSpec, ContainerStatus, PodIdentity, PullPolicy, Readoption,
};
use engenho_kubelet::config_bridge::{
    BackendRefused, KubeletBackendKind, make_container_runtime,
    make_container_runtime_with_apiserver,
};
use engenho_kubelet::cri_backend::CriGap;
use engenho_kubelet::pod_volume::{MountSource, ResolvedMount};
use engenho_kubelet::podman_api;

// ── The matrix ─────────────────────────────────────────────────────────────

/// Declare the rows. Generates [`EVERY_KIND`], [`row`] and [`selector`] from
/// the same list, so no kind can have a row and not be walked.
macro_rules! conformance_matrix {
    ( $( $variant:ident as $selector:literal => $row:expr ),+ $(,)? ) => {
        /// Every backend kind, in row order. Generated from the rows.
        const EVERY_KIND: &[KubeletBackendKind] = &[ $( KubeletBackendKind::$variant ),+ ];

        /// What `kind` promises.
        #[deny(unreachable_patterns)]
        fn row(kind: KubeletBackendKind) -> Row {
            match kind {
                $( KubeletBackendKind::$variant => $row, )+
            }
        }

        /// The name `engenho-config` selects `kind` by (`runtime.kubelet_backend`).
        #[deny(unreachable_patterns)]
        fn selector(kind: KubeletBackendKind) -> &'static str {
            match kind {
                $( KubeletBackendKind::$variant => $selector, )+
            }
        }
    };
}

conformance_matrix! {
    // T5.9: refused until the CRI backend sets mounts and pod IPs.
    Cri as "cri" => Row::Refused {
        names: &[CriGap::Mounts, CriGap::PodIp],
    },
    // rio and plo.
    PodmanApi as "podman_api" => Row::Runs(Contract {
        runtime: "podman-api",
        needs: Needs::PodmanSocket,
        readoption: Readoption::AdoptsRunning,
        pod_ip: PodIp::PodNetwork,
        mounts: Mounts::Honoured,
    }),
    // The fallback when the socket is not served.
    Podman as "podman" => Row::Runs(Contract {
        runtime: "podman",
        needs: Needs::PodmanCli,
        readoption: Readoption::Cannot,
        pod_ip: PodIp::PodNetwork,
        mounts: Mounts::Honoured,
    }),
    Fake as "fake" => Row::Runs(Contract {
        runtime: "fake",
        needs: Needs::Nothing,
        readoption: Readoption::Cannot,
        pod_ip: PodIp::PodNetwork,
        mounts: Mounts::NotObservable(
            "the fake runs no process, so nothing writes through a mount; it keeps the \
             mounts as spec data",
        ),
    }),
    // ryn.
    Native as "native" => Row::Runs(Contract {
        runtime: "native",
        needs: Needs::NixClosure,
        readoption: Readoption::Cannot,
        pod_ip: PodIp::HostNetwork,
        mounts: Mounts::Honoured,
    }),
}

/// What one backend kind promises.
#[derive(Clone, Copy, Debug)]
enum Row {
    /// Both constructors refuse it, and the refusal names each of `names`.
    Refused { names: &'static [CriGap] },
    /// It builds, and the runtime holds the contract.
    Runs(Contract),
}

/// The contract a built runtime is held to. What every runtime must do is
/// fixed ([`Clause`]); a row says only what legitimately differs.
#[derive(Clone, Copy, Debug)]
struct Contract {
    /// `ContainerRuntime::name` of the runtime the constructors must build —
    /// so a constructor that falls back to a different runtime is caught.
    runtime: &'static str,
    /// What must exist on the host for this runtime to run anything.
    needs: Needs,
    /// What `readoption()` must declare. `AdoptsRunning` is also observed: a
    /// fresh instance must hand the running container back.
    readoption: Readoption,
    /// Where a running container is reached.
    pod_ip: PodIp,
    /// Whether a workload writing through a mount is observable on the host.
    mounts: Mounts,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Needs {
    /// Nothing: the row always runs.
    Nothing,
    /// `nix` realising `nixpkgs#coreutils`.
    NixClosure,
    /// A reachable podman, driven through its CLI.
    PodmanCli,
    /// A libpod API socket.
    PodmanSocket,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PodIp {
    /// An address of its own: neither loopback nor unspecified.
    PodNetwork,
    /// The host's loopback — a host process is a `hostNetwork` pod, and
    /// upstream publishes `podIP = hostIP` for one.
    HostNetwork,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mounts {
    /// A file the workload writes at a mount's path appears at its host path.
    Honoured,
    /// Nothing can be observed, and why.
    NotObservable(&'static str),
}

// ── Clauses, verdicts, and why a row was not run ───────────────────────────

/// One promise of the `ContainerRuntime` contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Clause {
    /// Both constructors refuse, with one reason naming what the row names.
    Refusal,
    /// Both constructors build the row's runtime, not a fallback.
    Construction,
    /// `readoption()` declares what the row says.
    DeclaredReadoption,
    /// An id the runtime never had: `status` is `None`, `stop` and `remove`
    /// succeed.
    UnknownIsAbsent,
    /// `start` returns a running container with an id.
    Start,
    /// `status` of that id is the same container, running.
    Status,
    /// The running container reports the address the row says.
    PodIp,
    /// `AdoptsRunning`: a fresh instance's `start` hands the running
    /// container back instead of starting a second.
    Readoption,
    /// `stop` ends it, and stopping it again succeeds.
    Stop,
    /// `remove` drops it once reaped; it is then absent, and removing or
    /// stopping it again succeeds.
    Remove,
    /// A file written through a mount reaches the host.
    Mounts,
}

impl fmt::Display for Clause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Refusal => "refusal",
            Self::Construction => "construction",
            Self::DeclaredReadoption => "declared readoption",
            Self::UnknownIsAbsent => "unknown id is absent",
            Self::Start => "start",
            Self::Status => "status",
            Self::PodIp => "pod IP",
            Self::Readoption => "readoption",
            Self::Stop => "stop",
            Self::Remove => "remove",
            Self::Mounts => "mounts",
        })
    }
}

/// How a clause came out, short of a violation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Held,
    /// The row says this runtime cannot show it, and why.
    NotObservable(&'static str),
    /// Declared, and there is nothing to observe, and why.
    NotProbed(&'static str),
}

/// A clause that failed, and what was seen instead.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Violation {
    clause: Clause,
    observed: String,
}

impl Violation {
    fn new(clause: Clause, observed: impl Into<String>) -> Self {
        Self {
            clause,
            observed: observed.into(),
        }
    }
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.clause, self.observed)
    }
}

/// Why a row's runtime could not be run on this host.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Unavailable {
    NixClosure {
        attr: &'static str,
        detail: String,
    },
    PodmanUnreachable {
        detail: String,
    },
    ImageUnobtainable {
        image: String,
        detail: String,
    },
    NoPodmanSocket {
        tried: Vec<PathBuf>,
        /// What the constructor built instead — a fallback worth recording.
        built_instead: Result<&'static str, BackendRefused>,
    },
}

impl fmt::Display for Unavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NixClosure { attr, detail } => {
                write!(f, "nix could not realise {attr}: {detail}")
            }
            Self::PodmanUnreachable { detail } => write!(f, "podman is not reachable: {detail}"),
            Self::ImageUnobtainable { image, detail } => write!(
                f,
                "the conformance image {image} is not in the local store and could not be \
                 pulled: {detail}"
            ),
            Self::NoPodmanSocket {
                tried,
                built_instead,
            } => {
                f.write_str("no libpod socket (tried ")?;
                for (i, p) in tried.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{}", p.display())?;
                }
                match built_instead {
                    Ok(name) => write!(f, "); its constructor built `{name}` instead"),
                    Err(refused) => write!(f, "); its constructor refused: {refused}"),
                }
            }
        }
    }
}

/// What the matrix found for one backend kind.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Verdict {
    /// Refused at construction, as its row says.
    Refused(BackendRefused),
    /// Its runtime is not on this host. Fails the matrix only where required.
    Unavailable(Unavailable),
    /// Every clause held, or is recorded as not observable / not probed.
    Conforms(Vec<(Clause, Outcome)>),
    /// A clause failed. Clauses after it were not run.
    Violates {
        held: Vec<(Clause, Outcome)>,
        broke: Violation,
    },
}

// ── Which unavailable rows fail the matrix ─────────────────────────────────

/// The variable naming the rows that must run here.
const REQUIRE_ENV: &str = "ENGENHO_CONFORMANCE_REQUIRE";

/// Which rows must not be [`Verdict::Unavailable`] on this host.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Required {
    Nothing,
    All,
    Only(Vec<KubeletBackendKind>),
}

/// A required selector that names no backend kind.
#[derive(Clone, Debug, PartialEq, Eq)]
struct UnknownSelector {
    given: String,
}

impl fmt::Display for UnknownSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{REQUIRE_ENV} names `{}`, which is no backend kind; legal: all",
            self.given
        )?;
        for kind in EVERY_KIND {
            write!(f, ", {}", selector(*kind))?;
        }
        Ok(())
    }
}

impl Required {
    /// Parse `all` or a comma-separated list of selectors. Absent or blank
    /// requires nothing; a selector naming no kind is an error, never ignored.
    fn parse(raw: Option<&str>) -> Result<Self, UnknownSelector> {
        let Some(raw) = raw.map(str::trim).filter(|r| !r.is_empty()) else {
            return Ok(Self::Nothing);
        };
        if raw == "all" {
            return Ok(Self::All);
        }
        let mut kinds = Vec::new();
        for given in raw.split(',').map(str::trim) {
            let kind = EVERY_KIND
                .iter()
                .copied()
                .find(|k| selector(*k) == given)
                .ok_or_else(|| UnknownSelector {
                    given: given.to_string(),
                })?;
            kinds.push(kind);
        }
        Ok(Self::Only(kinds))
    }

    fn includes(&self, kind: KubeletBackendKind) -> bool {
        match self {
            Self::Nothing => false,
            Self::All => true,
            Self::Only(kinds) => kinds.contains(&kind),
        }
    }
}

/// A verdict that fails the matrix.
#[derive(Debug, PartialEq, Eq)]
enum Failure<'a> {
    Violated {
        kind: KubeletBackendKind,
        violation: &'a Violation,
    },
    RequiredButUnavailable {
        kind: KubeletBackendKind,
        why: &'a Unavailable,
    },
}

impl fmt::Display for Failure<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Violated { kind, violation } => {
                write!(f, "{} violates {violation}", selector(*kind))
            }
            Self::RequiredButUnavailable { kind, why } => write!(
                f,
                "{} is required by {REQUIRE_ENV} but unavailable: {why}",
                selector(*kind)
            ),
        }
    }
}

/// Every verdict that fails the matrix under `required`.
fn judge<'a>(
    results: &'a [(KubeletBackendKind, Verdict)],
    required: &Required,
) -> Vec<Failure<'a>> {
    results
        .iter()
        .filter_map(|(kind, verdict)| match verdict {
            Verdict::Violates { broke, .. } => Some(Failure::Violated {
                kind: *kind,
                violation: broke,
            }),
            Verdict::Unavailable(why) if required.includes(*kind) => {
                Some(Failure::RequiredButUnavailable { kind: *kind, why })
            }
            Verdict::Unavailable(_) | Verdict::Refused(_) | Verdict::Conforms(_) => None,
        })
        .collect()
}

/// The matrix as a table: one line per kind.
struct Table<'a> {
    results: &'a [(KubeletBackendKind, Verdict)],
    required: &'a Required,
}

impl fmt::Display for Table<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "backend conformance matrix (W7): {} kinds, required here: {:?}",
            self.results.len(),
            self.required
        )?;
        for (kind, verdict) in self.results {
            write!(f, "  {:<11} ", selector(*kind))?;
            match verdict {
                Verdict::Refused(refused) => writeln!(f, "refused      {refused}")?,
                Verdict::Unavailable(why) => writeln!(f, "UNAVAILABLE  {why}")?,
                Verdict::Conforms(clauses) => {
                    f.write_str("conforms     ")?;
                    write_clauses(f, clauses)?;
                    writeln!(f)?;
                }
                Verdict::Violates { held, broke } => {
                    write!(f, "VIOLATES     {broke} | before it: ")?;
                    write_clauses(f, held)?;
                    writeln!(f)?;
                }
            }
        }
        Ok(())
    }
}

fn write_clauses(f: &mut fmt::Formatter<'_>, clauses: &[(Clause, Outcome)]) -> fmt::Result {
    let mut first = true;
    for (clause, outcome) in clauses {
        if !first {
            f.write_str("; ")?;
        }
        first = false;
        match outcome {
            Outcome::Held => write!(f, "{clause} held")?,
            Outcome::NotObservable(why) => write!(f, "{clause} not observable ({why})")?,
            Outcome::NotProbed(why) => write!(f, "{clause} not probed ({why})")?,
        }
    }
    Ok(())
}

// ── Running a row ──────────────────────────────────────────────────────────

/// How long any runtime gets to settle a state change. podman's own stop
/// escalates to SIGKILL after 10 s, and busybox's `sleep` as PID 1 ignores
/// SIGTERM, so it has to cover that.
const SETTLE: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(25);

/// Namespace of every conformance container.
const NAMESPACE: &str = "engenho-conformance";

/// The podman rows' image, overridable because a node's cache is its own.
const IMAGE_ENV: &str = "ENGENHO_CONFORMANCE_IMAGE";
const DEFAULT_IMAGE: &str = "docker.io/library/busybox:latest";

fn podman_image() -> String {
    std::env::var(IMAGE_ENV).unwrap_or_else(|_| DEFAULT_IMAGE.to_string())
}

/// What a row's containers run from.
struct Fixture {
    image: String,
}

/// Provision what `needs` names, or say why it is not here.
async fn provision(kind: KubeletBackendKind, needs: Needs) -> Result<Fixture, Unavailable> {
    match needs {
        Needs::Nothing => Ok(Fixture {
            image: "conformance.invalid/fake:0".to_string(),
        }),
        Needs::NixClosure => {
            let attr = "nixpkgs#coreutils";
            let path = realise(attr).map_err(|detail| Unavailable::NixClosure { attr, detail })?;
            let mut image = String::from("nix:");
            image.push_str(&path.to_string_lossy());
            Ok(Fixture { image })
        }
        Needs::PodmanCli => {
            let image = podman_image();
            let exists = podman(&["image", "exists", &image])
                .map_err(|detail| Unavailable::PodmanUnreachable { detail })?;
            match exists.status.code() {
                Some(0) => {}
                // `image exists` answers 1 for absent; anything else is podman
                // failing to answer at all.
                Some(1) => {
                    let pulled = podman(&["pull", &image])
                        .map_err(|detail| Unavailable::PodmanUnreachable { detail })?;
                    if !pulled.status.success() {
                        return Err(Unavailable::ImageUnobtainable {
                            image,
                            detail: first_line(&pulled.stderr),
                        });
                    }
                }
                _ => {
                    return Err(Unavailable::PodmanUnreachable {
                        detail: first_line(&exists.stderr),
                    });
                }
            }
            Ok(Fixture { image })
        }
        Needs::PodmanSocket => {
            if podman_api::discover_socket().is_none() {
                return Err(Unavailable::NoPodmanSocket {
                    tried: podman_api::default_socket_candidates(),
                    built_instead: make_container_runtime(kind, None).map(|rt| rt.name()),
                });
            }
            let image = podman_image();
            let api =
                podman_api::PodmanApi::discover().map_err(|e| Unavailable::PodmanUnreachable {
                    detail: e.to_string(),
                })?;
            api.ensure_image(&image, Some(PullPolicy::IfNotPresent))
                .await
                .map_err(|e| Unavailable::ImageUnobtainable {
                    image: image.clone(),
                    detail: e.to_string(),
                })?;
            Ok(Fixture { image })
        }
    }
}

/// Realise `attr` and return the output that has a `bin/`.
fn realise(attr: &str) -> Result<PathBuf, String> {
    let out = std::process::Command::new("nix")
        .args(["build", "--no-link", "--print-out-paths", attr])
        .output()
        .map_err(|e| e.to_string())?;
    if !out.status.success() {
        return Err(first_line(&out.stderr));
    }
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::trim)
        .map(PathBuf::from)
        .find(|p| p.join("bin").is_dir())
        .ok_or_else(|| "no output has a bin/ directory".to_string())
}

fn podman(args: &[&str]) -> Result<std::process::Output, String> {
    std::process::Command::new("podman")
        .args(args)
        .output()
        .map_err(|e| e.to_string())
}

fn first_line(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("(no output)")
        .to_string()
}

/// Evaluate `kind` against its row.
async fn evaluate(kind: KubeletBackendKind) -> Verdict {
    match row(kind) {
        Row::Refused { names } => evaluate_refusal(kind, names),
        Row::Runs(contract) => evaluate_contract(kind, contract).await,
    }
}

fn evaluate_refusal(kind: KubeletBackendKind, names: &[CriGap]) -> Verdict {
    let violates = |observed: String| Verdict::Violates {
        held: Vec::new(),
        broke: Violation::new(Clause::Refusal, observed),
    };
    let Some(refused) = engenho_kubelet::config_bridge::construction_refusal(kind) else {
        let built = make_container_runtime(kind, None).map(|rt| rt.name());
        return violates(format!(
            "`{}` is no longer refused (the constructor gives {built:?}); give it a Runs row",
            selector(kind)
        ));
    };
    let boot = make_container_runtime_with_apiserver(
        kind,
        Some("/opt/podman/bin/podman"),
        Some(("10.43.0.1".to_string(), 443)),
    );
    for (constructor, built) in [
        ("make_container_runtime", make_container_runtime(kind, None)),
        ("make_container_runtime_with_apiserver", boot),
    ] {
        match built {
            Err(r) if r == refused => {}
            Err(r) => {
                return violates(format!(
                    "{constructor} refused with `{r}` but refusal() says `{refused}`"
                ));
            }
            Ok(rt) => return violates(format!("{constructor} built `{}`", rt.name())),
        }
    }
    let missing = match refused {
        BackendRefused::CriIncomplete { missing } => missing,
    };
    for gap in names {
        if !missing.contains(*gap) {
            return violates(format!("the refusal does not name {gap:?}: `{refused}`"));
        }
    }
    Verdict::Refused(refused)
}

async fn evaluate_contract(kind: KubeletBackendKind, contract: Contract) -> Verdict {
    if let Some(refused) = engenho_kubelet::config_bridge::construction_refusal(kind) {
        return Verdict::Violates {
            held: Vec::new(),
            broke: Violation::new(
                Clause::Construction,
                format!("refused at construction: {refused}"),
            ),
        };
    }
    let fixture = match provision(kind, contract.needs).await {
        Ok(fixture) => fixture,
        Err(why) => return Verdict::Unavailable(why),
    };
    let rt = match construct(kind, contract) {
        Ok(rt) => rt,
        Err(broke) => {
            return Verdict::Violates {
                held: Vec::new(),
                broke,
            };
        }
    };
    let mut run = Run {
        kind,
        contract,
        fixture,
        rt,
        started: Vec::new(),
        held: vec![(Clause::Construction, Outcome::Held)],
    };
    let outcome = run.clauses().await;
    run.clean_up().await;
    match outcome {
        Ok(()) => Verdict::Conforms(run.held),
        Err(broke) => Verdict::Violates {
            held: run.held,
            broke,
        },
    }
}

/// Build through both constructors and check each built the row's runtime.
fn construct(
    kind: KubeletBackendKind,
    contract: Contract,
) -> Result<Arc<dyn ContainerRuntime>, Violation> {
    let rt = make_container_runtime(kind, None)
        .map_err(|r| Violation::new(Clause::Construction, format!("refused: {r}")))?;
    let boot = make_container_runtime_with_apiserver(kind, None, Some(("10.43.0.1".into(), 443)))
        .map_err(|r| {
        Violation::new(
            Clause::Construction,
            format!("the boot constructor refused: {r}"),
        )
    })?;
    for (constructor, built) in [("make_container_runtime", &rt), ("boot constructor", &boot)] {
        if built.name() != contract.runtime {
            return Err(Violation::new(
                Clause::Construction,
                format!(
                    "{constructor} built `{}`, not `{}`",
                    built.name(),
                    contract.runtime
                ),
            ));
        }
    }
    Ok(rt)
}

/// One row being held to its contract.
struct Run {
    kind: KubeletBackendKind,
    contract: Contract,
    fixture: Fixture,
    rt: Arc<dyn ContainerRuntime>,
    /// Containers this run started and has not removed.
    started: Vec<String>,
    held: Vec<(Clause, Outcome)>,
}

impl Run {
    async fn clauses(&mut self) -> Result<(), Violation> {
        self.declared_readoption()?;
        self.unknown_is_absent().await?;
        let long = self.spec("long", &["sleep", "300"], Vec::new());
        let id = self.start_and_status(&long).await?;
        self.pod_ip(&id).await?;
        self.readoption(&long, &id).await?;
        self.stop(&id).await?;
        self.remove(&id).await?;
        self.mounts().await
    }

    fn spec(&self, container: &str, command: &[&str], mounts: Vec<ResolvedMount>) -> ContainerSpec {
        let pod = format!("conformance-{}", std::process::id());
        let container = format!("{}-{container}", selector(self.kind));
        ContainerSpec {
            name: format!("{NAMESPACE}_{pod}_{container}"),
            image: self.fixture.image.clone(),
            command: command.iter().map(|s| (*s).to_string()).collect(),
            pull_policy: Some(PullPolicy::Never),
            mounts,
            pod: PodIdentity {
                namespace: NAMESPACE.to_string(),
                uid: format!("{pod}-uid"),
                name: pod,
                container_name: container,
                init: false,
            },
            ..ContainerSpec::default()
        }
    }

    fn declared_readoption(&mut self) -> Result<(), Violation> {
        let declared = self.rt.readoption();
        if declared != self.contract.readoption {
            return Err(Violation::new(
                Clause::DeclaredReadoption,
                format!(
                    "declares {declared:?}, the row says {:?}",
                    self.contract.readoption
                ),
            ));
        }
        self.held.push((Clause::DeclaredReadoption, Outcome::Held));
        Ok(())
    }

    async fn unknown_is_absent(&mut self) -> Result<(), Violation> {
        let never = format!("{NAMESPACE}-never-started-{}", std::process::id());
        let broke = |what: String| Violation::new(Clause::UnknownIsAbsent, what);
        match self.rt.status(&never).await {
            Ok(None) => {}
            Ok(Some(s)) => return Err(broke(format!("status of `{never}` is {s:?}"))),
            Err(e) => return Err(broke(format!("status of `{never}` failed: {e}"))),
        }
        self.rt
            .stop(&never)
            .await
            .map_err(|e| broke(format!("stop of `{never}` failed: {e}")))?;
        self.rt
            .remove(&never)
            .await
            .map_err(|e| broke(format!("remove of `{never}` failed: {e}")))?;
        self.held.push((Clause::UnknownIsAbsent, Outcome::Held));
        Ok(())
    }

    async fn start_and_status(&mut self, spec: &ContainerSpec) -> Result<String, Violation> {
        let started = self
            .rt
            .start(spec)
            .await
            .map_err(|e| Violation::new(Clause::Start, format!("start failed: {e}")))?;
        if started.container_id.is_empty() {
            return Err(Violation::new(Clause::Start, "start returned an empty id"));
        }
        let id = started.container_id.clone();
        self.started.push(id.clone());
        if !started.is_running() {
            return Err(Violation::new(
                Clause::Start,
                format!("a `sleep 300` started as {:?}", started.state),
            ));
        }
        self.held.push((Clause::Start, Outcome::Held));

        match self.rt.status(&id).await {
            Ok(Some(s)) if s.container_id == id && s.is_running() => {}
            other => {
                return Err(Violation::new(
                    Clause::Status,
                    format!("status of the running `{id}` is {other:?}"),
                ));
            }
        }
        self.held.push((Clause::Status, Outcome::Held));
        Ok(id)
    }

    async fn pod_ip(&mut self, id: &str) -> Result<(), Violation> {
        let broke = |what: String| Violation::new(Clause::PodIp, what);
        // The address may be assigned after start returns; poll for it.
        let observed = settle(self.rt.as_ref(), id, |s| {
            s.is_some_and(|s| s.pod_ip.is_some())
        })
        .await
        .map_err(|e| broke(format!("status failed: {e}")))?;
        let Some(raw) = observed.and_then(|s| s.pod_ip) else {
            return Err(broke(format!(
                "a running container reported no address within {SETTLE:?}"
            )));
        };
        let ip: IpAddr = raw
            .parse()
            .map_err(|e| broke(format!("`{raw}` is not an address: {e}")))?;
        match self.contract.pod_ip {
            PodIp::PodNetwork if ip.is_loopback() || ip.is_unspecified() => {
                return Err(broke(format!("{ip} is not an address of its own")));
            }
            PodIp::HostNetwork if ip != IpAddr::V4(Ipv4Addr::LOCALHOST) => {
                return Err(broke(format!(
                    "{ip}, where a host process answers on 127.0.0.1"
                )));
            }
            PodIp::PodNetwork | PodIp::HostNetwork => {}
        }
        self.held.push((Clause::PodIp, Outcome::Held));
        Ok(())
    }

    async fn readoption(&mut self, spec: &ContainerSpec, id: &str) -> Result<(), Violation> {
        let broke = |what: String| Violation::new(Clause::Readoption, what);
        match self.contract.readoption {
            Readoption::AdoptsRunning => {
                // A kubelet restart: a new runtime instance, the same spec.
                let fresh = make_container_runtime(self.kind, None)
                    .map_err(|r| broke(format!("a second instance was refused: {r}")))?;
                let again = fresh
                    .start(spec)
                    .await
                    .map_err(|e| broke(format!("a fresh instance's start failed: {e}")))?;
                if again.container_id != id {
                    self.started.push(again.container_id.clone());
                    return Err(broke(format!(
                        "a fresh instance started `{}` beside the running `{id}`",
                        again.container_id
                    )));
                }
                if !again.is_running() {
                    return Err(broke(format!("adopted `{id}` as {:?}", again.state)));
                }
                match self.rt.status(id).await {
                    Ok(Some(s)) if s.is_running() => {}
                    other => {
                        return Err(broke(format!(
                            "after the adoption the original is {other:?}"
                        )));
                    }
                }
                self.held.push((Clause::Readoption, Outcome::Held));
            }
            Readoption::Cannot => self.held.push((
                Clause::Readoption,
                Outcome::NotProbed(
                    "declares Cannot: the kubelet never asks it to adopt, so there is \
                     nothing to observe",
                ),
            )),
        }
        Ok(())
    }

    async fn stop(&mut self, id: &str) -> Result<(), Violation> {
        let broke = |what: String| Violation::new(Clause::Stop, what);
        self.rt
            .stop(id)
            .await
            .map_err(|e| broke(format!("stop failed: {e}")))?;
        let after = settle(self.rt.as_ref(), id, |s| {
            !s.is_some_and(ContainerStatus::is_running)
        })
        .await
        .map_err(|e| broke(format!("status after stop failed: {e}")))?;
        match after {
            Some(s) if s.is_running() => {
                return Err(broke(format!("still running {SETTLE:?} after stop")));
            }
            Some(_) => {}
            None => return Err(broke("stop dropped the container's record".to_string())),
        }
        self.rt
            .stop(id)
            .await
            .map_err(|e| broke(format!("stopping it again failed: {e}")))?;
        self.held.push((Clause::Stop, Outcome::Held));
        Ok(())
    }

    async fn remove(&mut self, id: &str) -> Result<(), Violation> {
        let broke = |what: String| Violation::new(Clause::Remove, what);
        remove_once_reaped(self.rt.as_ref(), id)
            .await
            .map_err(|e| broke(format!("remove failed: {e}")))?;
        self.started.retain(|s| s != id);
        match self.rt.status(id).await {
            Ok(None) => {}
            other => {
                return Err(broke(format!("after remove, status is {other:?}")));
            }
        }
        self.rt
            .remove(id)
            .await
            .map_err(|e| broke(format!("removing it again failed: {e}")))?;
        self.rt
            .stop(id)
            .await
            .map_err(|e| broke(format!("stopping it once removed failed: {e}")))?;
        self.held.push((Clause::Remove, Outcome::Held));
        Ok(())
    }

    async fn mounts(&mut self) -> Result<(), Violation> {
        let broke = |what: String| Violation::new(Clause::Mounts, what);
        if let Mounts::NotObservable(why) = self.contract.mounts {
            self.held
                .push((Clause::Mounts, Outcome::NotObservable(why)));
            return Ok(());
        }
        // One path on both sides: a native process has no mount namespace, so
        // that is the only mount it can honour, and podman honours it too.
        let root = std::env::temp_dir().join(format!(
            "engenho-conformance-{}-{}",
            std::process::id(),
            selector(self.kind)
        ));
        let _ = std::fs::remove_dir_all(&root);
        let dir = root.join("vol");
        std::fs::create_dir_all(&dir)
            .map_err(|e| broke(format!("cannot create {}: {e}", dir.display())))?;
        let marker = dir.join("marker");
        let mount = ResolvedMount {
            source: MountSource::UserHostPath(dir.clone()),
            mount_path: dir.to_string_lossy().into_owned(),
            read_only: false,
            sub_path: None,
        };
        let spec = self.spec("mount", &["touch", &marker.to_string_lossy()], vec![mount]);
        let started = self
            .rt
            .start(&spec)
            .await
            .map_err(|e| broke(format!("a container with a mount would not start: {e}")))?;
        self.started.push(started.container_id.clone());

        let reached = wait_for(&marker).await;
        let retired = retire(self.rt.as_ref(), &started.container_id).await;
        let _ = std::fs::remove_dir_all(&root);
        if !reached {
            return Err(broke(format!(
                "the workload touched {} through the mount; it never reached the host",
                marker.display()
            )));
        }
        retired.map_err(|e| broke(format!("retiring the mount container failed: {e}")))?;
        self.started.retain(|s| *s != started.container_id);
        self.held.push((Clause::Mounts, Outcome::Held));
        Ok(())
    }

    /// Best effort: whatever a violation left running goes.
    async fn clean_up(&mut self) {
        for id in std::mem::take(&mut self.started) {
            if let Err(e) = retire(self.rt.as_ref(), &id).await {
                eprintln!("conformance clean-up of `{id}` failed: {e}");
            }
        }
    }
}

/// Poll `status(id)` until `done` holds or [`SETTLE`] runs out; the last
/// answer either way.
async fn settle(
    rt: &dyn ContainerRuntime,
    id: &str,
    done: impl Fn(Option<&ContainerStatus>) -> bool,
) -> Result<Option<ContainerStatus>, KubeletError> {
    let deadline = Instant::now() + SETTLE;
    loop {
        let now = rt.status(id).await?;
        if done(now.as_ref()) || Instant::now() >= deadline {
            return Ok(now);
        }
        tokio::time::sleep(POLL).await;
    }
}

/// `remove`, retried while the runtime says the process is not reaped yet.
async fn remove_once_reaped(rt: &dyn ContainerRuntime, id: &str) -> Result<(), KubeletError> {
    let deadline = Instant::now() + SETTLE;
    loop {
        match rt.remove(id).await {
            Err(KubeletError::NotReaped { .. }) if Instant::now() < deadline => {
                tokio::time::sleep(POLL).await;
            }
            done => return done,
        }
    }
}

/// Stop `id`, wait for it to end, and remove it.
async fn retire(rt: &dyn ContainerRuntime, id: &str) -> Result<(), KubeletError> {
    rt.stop(id).await?;
    settle(rt, id, |s| !s.is_some_and(ContainerStatus::is_running)).await?;
    remove_once_reaped(rt, id).await
}

async fn wait_for(path: &Path) -> bool {
    let deadline = Instant::now() + SETTLE;
    while Instant::now() < deadline {
        if path.exists() {
            return true;
        }
        tokio::time::sleep(POLL).await;
    }
    path.exists()
}

// ── The matrix test ────────────────────────────────────────────────────────

/// ★ Every backend kind, held to the contract its row states — or refused,
/// or recorded as unavailable on this host. A kind with no row does not
/// compile; a violation fails; an unavailable row fails where required.
#[tokio::test]
async fn every_backend_kind_holds_its_contract_or_is_recorded() {
    let required = Required::parse(std::env::var(REQUIRE_ENV).ok().as_deref())
        .unwrap_or_else(|e| panic!("{e}"));

    // The native backend writes container logs under this directory; keep
    // them out of the operator's real one (~/.local/share/engenho).
    let logs =
        std::env::temp_dir().join(format!("engenho-conformance-logs-{}", std::process::id()));
    // SAFETY: set before any runtime is built, from the only test in this
    // binary that reads the environment or spawns a process.
    unsafe { std::env::set_var("ENGENHO_NATIVE_LOG_DIR", &logs) };

    let mut results = Vec::new();
    for kind in EVERY_KIND {
        results.push((*kind, evaluate(*kind).await));
    }
    let _ = std::fs::remove_dir_all(&logs);

    let table = Table {
        results: &results,
        required: &required,
    }
    .to_string();
    eprintln!("{table}");
    let receipt = Path::new(env!("CARGO_TARGET_TMPDIR")).join("backend-conformance.txt");
    if let Err(e) = std::fs::write(&receipt, &table) {
        eprintln!("could not write {}: {e}", receipt.display());
    }

    let failures = judge(&results, &required);
    assert!(
        failures.is_empty(),
        "{table}\n{} failure(s):\n{}",
        failures.len(),
        failures
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    );
}

// ── The matrix's own rules ─────────────────────────────────────────────────

fn unreachable_podman() -> Verdict {
    Verdict::Unavailable(Unavailable::PodmanUnreachable {
        detail: "Cannot connect to Podman".to_string(),
    })
}

#[test]
fn an_unavailable_backend_is_recorded_and_fails_only_where_required() {
    let results = [(KubeletBackendKind::Podman, unreachable_podman())];

    assert_eq!(judge(&results, &Required::Nothing), Vec::new());
    assert_eq!(
        judge(&results, &Required::parse(Some("native")).expect("a kind")),
        Vec::new(),
        "requiring another kind does not require this one"
    );
    for required in [
        Required::All,
        Required::parse(Some("podman")).expect("a kind"),
    ] {
        let failures = judge(&results, &required);
        assert!(
            matches!(
                failures.as_slice(),
                [Failure::RequiredButUnavailable {
                    kind: KubeletBackendKind::Podman,
                    ..
                }]
            ),
            "{required:?} must fail an unavailable podman row: {failures:?}"
        );
    }
}

#[test]
fn a_violation_fails_the_matrix_whether_or_not_its_backend_is_required() {
    let broke = Violation::new(Clause::PodIp, "no address");
    let results = [(
        KubeletBackendKind::Native,
        Verdict::Violates {
            held: vec![(Clause::Start, Outcome::Held)],
            broke: broke.clone(),
        },
    )];
    for required in [Required::Nothing, Required::All] {
        assert_eq!(
            judge(&results, &required),
            vec![Failure::Violated {
                kind: KubeletBackendKind::Native,
                violation: &broke,
            }]
        );
    }
}

#[test]
fn a_refusal_or_a_conforming_row_fails_nothing() {
    let refused = engenho_kubelet::config_bridge::construction_refusal(KubeletBackendKind::Cri)
        .expect("cri is refused");
    let results = [
        (KubeletBackendKind::Cri, Verdict::Refused(refused)),
        (
            KubeletBackendKind::Fake,
            Verdict::Conforms(vec![(Clause::Start, Outcome::Held)]),
        ),
    ];
    assert_eq!(judge(&results, &Required::All), Vec::new());
}

#[test]
fn a_required_selector_that_names_no_kind_is_refused_not_ignored() {
    assert_eq!(Required::parse(None), Ok(Required::Nothing));
    assert_eq!(Required::parse(Some("  ")), Ok(Required::Nothing));
    assert_eq!(Required::parse(Some("all")), Ok(Required::All));
    assert_eq!(
        Required::parse(Some("podman_api, native")),
        Ok(Required::Only(vec![
            KubeletBackendKind::PodmanApi,
            KubeletBackendKind::Native
        ]))
    );
    let typo = Required::parse(Some("podman,podmn")).expect_err("a typo requires nothing");
    assert_eq!(typo.given, "podmn");
    let shown = typo.to_string();
    for kind in EVERY_KIND {
        assert!(shown.contains(selector(*kind)), "lists {kind:?}: {shown}");
    }
}

/// A row's selector is what `runtime.kubelet_backend` says in a node's
/// config, and it names the same kind there.
#[test]
fn every_row_is_named_by_the_selector_its_config_uses() {
    for kind in EVERY_KIND {
        let configured: engenho_config::KubeletBackendKind =
            serde_json::from_value(serde_json::Value::from(selector(*kind)))
                .unwrap_or_else(|e| panic!("`{}` is no config selector: {e}", selector(*kind)));
        assert_eq!(format!("{configured:?}"), format!("{kind:?}"));
    }
}

/// The row that needs nothing has no way to be unavailable, so the fake is
/// always held to the whole contract.
#[tokio::test]
async fn a_row_that_needs_nothing_always_runs() {
    for kind in EVERY_KIND {
        if let Row::Runs(contract) = row(*kind)
            && contract.needs == Needs::Nothing
        {
            assert!(
                provision(*kind, contract.needs).await.is_ok(),
                "{kind:?} needs nothing, so it cannot be unavailable"
            );
        }
    }
    assert!(
        EVERY_KIND
            .iter()
            .any(|k| matches!(row(*k), Row::Runs(c) if c.needs == Needs::Nothing)),
        "at least one row must run on every host"
    );
}
