//! Pluggable container runtime backends.
//!
//! The substrate's `ContainerRuntime` trait is THE seam between
//! kubelet (substrate-side) + the host's actual container runner
//! (podman, containerd via CRI, runwasi for wasm). Backend impls
//! receive a typed `ContainerSpec` + return a typed
//! `ContainerStatus`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::error::KubeletError;

/// Spec describing what the kubelet wants the backend to run.
/// Distilled from `engenho_types::core_v1::Pod.spec.containers[0]`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerSpec {
    /// Logical container name (namespace/podname/container).
    pub name: String,
    /// Container image (e.g. "stefanprodan/podinfo:6.5.4").
    pub image: String,
    /// Environment variables.
    pub env: BTreeMap<String, String>,
    /// Command to run inside the container; empty = image default.
    pub command: Vec<String>,
    /// Per-pod DNS aliases (`--network-alias`) the kubelet computed from
    /// the Services whose selector matches this Pod's labels (M0.3
    /// cluster-DNS brick). Each alias is a Service-name form — `<svc>`,
    /// `<svc>.<ns>`, `<svc>.<ns>.svc.<cluster_domain>` — fed to podman's
    /// aardvark-dns so a pod on the shared user-defined network resolves
    /// Service names to backend pod IPs (headless multi-A, no ClusterIP).
    ///
    /// Empty = the Pod matched no Service (or the backend is on the
    /// ambient network, where aliases are meaningless). Carried on
    /// `ContainerSpec` (not a separate `start` arg) so the
    /// [`ContainerRuntime::start`] signature is unchanged and
    /// [`FakeBackend`] needs no new method — aliases are just data.
    ///
    /// ## Start-time only (accepted M0.3 limitation)
    ///
    /// Aliases are computed once, at pod-start, because `--network-alias`
    /// is a `podman run` flag that cannot be added to a running container
    /// without recreate. A Service created AFTER a pod starts does NOT
    /// retroactively alias that pod until it is recreated. The
    /// EndpointsController (reconciled) keeps Service→Endpoints current
    /// for any consumer reading Endpoints; this alias path is the
    /// convenience DNS layer. The M0.4 `engenho-dns` authority (hickory +
    /// KubernetesAuthority owning ClusterIP A-records + SRV) removes the
    /// limitation entirely — named here so this interim is a conscious
    /// shortest-correct step toward that destination, not the easy path.
    pub network_aliases: Vec<String>,
}

/// Status the backend reports back.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerStatus {
    /// Opaque backend handle identifying the running container.
    pub container_id: String,
    /// Whether the container is currently up.
    pub running: bool,
    /// Pod-network IP assigned to the container (None until network setup).
    pub pod_ip: Option<String>,
    /// Optional exit code if the container has terminated.
    pub exit_code: Option<i32>,
}

impl ContainerStatus {
    /// Convenience constructor for tests / fakes.
    #[must_use]
    pub fn running(container_id: impl Into<String>, pod_ip: impl Into<String>) -> Self {
        Self {
            container_id: container_id.into(),
            running: true,
            pod_ip: Some(pod_ip.into()),
            exit_code: None,
        }
    }
}

/// Typed options for [`ContainerRuntime::logs`] — the `kubectl logs` knobs.
///
/// Typed (NOT a bare bool / string bag) so each option is a named field the
/// podman `logs_argv` builder maps deterministically. M0.3 ships `tail`
/// (kubectl `--tail`); `timestamps` / `since` are forward-compat fields the
/// builder honors when set (off by default) so adding the kubectl flags later
/// is a one-line argv addition, not a new method.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogOptions {
    /// `--tail N` — return only the last `N` lines. `None` = all lines.
    pub tail: Option<u32>,
    /// `--timestamps` — prefix each line with an RFC3339 timestamp. Default
    /// `false`. Forward-compat (kubectl `--timestamps`).
    pub timestamps: bool,
}

impl LogOptions {
    /// Options requesting all lines (the default).
    #[must_use]
    pub fn all() -> Self {
        Self::default()
    }

    /// Options requesting only the last `n` lines.
    #[must_use]
    pub fn tail(n: u32) -> Self {
        Self {
            tail: Some(n),
            ..Self::default()
        }
    }
}

/// The pluggable container runtime trait. Pure runtime —
/// host-effecting; no I/O in trait shape, but every method
/// performs side-effecting work on the host.
#[async_trait]
pub trait ContainerRuntime: Send + Sync {
    /// Stable identifier for telemetry.
    fn name(&self) -> &'static str;

    /// Start a container matching `spec`. Returns the new
    /// container's status (post-start; pod_ip may take a moment
    /// to materialize, callers should poll via `status` if None).
    ///
    /// # Errors
    ///
    /// Returns [`KubeletError::Backend`] if the runtime refused
    /// to start (image not found, OOM, etc).
    async fn start(&self, spec: &ContainerSpec) -> Result<ContainerStatus, KubeletError>;

    /// Look up status of a previously-started container. Returns
    /// `None` if not tracked.
    ///
    /// # Errors
    ///
    /// Returns [`KubeletError::Backend`] on backend inspection failure.
    async fn status(&self, container_id: &str) -> Result<Option<ContainerStatus>, KubeletError>;

    /// Stop a running container.
    ///
    /// # Errors
    ///
    /// Returns [`KubeletError::Backend`] if the backend cannot stop.
    async fn stop(&self, container_id: &str) -> Result<(), KubeletError>;

    /// Drop the container's record; equivalent to `docker rm`.
    ///
    /// # Errors
    ///
    /// Returns [`KubeletError::Backend`] on failure.
    async fn remove(&self, container_id: &str) -> Result<(), KubeletError>;

    /// Stream a container's stdout/stderr log. The `kubectl logs <pod>
    /// [-c <container>]` surface — the apiserver's Pod `/log` subresource
    /// asks the kubelet which asks this method.
    ///
    /// [`FakeBackend`] returns a per-container in-memory buffer (seeded at
    /// `start`) so tests assert exact stdout; [`PodmanBackend`] runs
    /// `podman logs [--tail N] <id>` via the pure [`PodmanBackend::logs_argv`]
    /// builder.
    ///
    /// # Errors
    ///
    /// [`KubeletError::Backend`] if the backend cannot read the log (no such
    /// container, podman spawn failure). A not-found container surfaces a
    /// typed error — never a silently-empty success.
    async fn logs(
        &self,
        container_id: &str,
        opts: &LogOptions,
    ) -> Result<String, KubeletError>;
}

// =================================================================
// FakeBackend — in-memory, deterministic, for tests
// =================================================================

/// Deterministic fake runtime for integration tests. Records every
/// (start / stop / remove) call + lets tests inspect state.
#[derive(Default, Clone)]
pub struct FakeBackend {
    inner: Arc<Mutex<FakeState>>,
}

#[derive(Default)]
struct FakeState {
    next_id: u64,
    containers: BTreeMap<String, ContainerStatus>,
    /// Last seen spec per container_id, kept for assertions.
    specs: BTreeMap<String, ContainerSpec>,
    /// Operations log so tests can assert backend invocations.
    pub events: Vec<FakeEvent>,
    /// Per-container stdout buffer keyed by container_id. Seeded at `start`
    /// from the spec name (deterministic) so [`ContainerRuntime::logs`]
    /// returns a known string a test can assert exactly; overridable per
    /// container name via [`FakeBackend::seed_log`].
    logs: BTreeMap<String, String>,
    /// Pre-seeded log content keyed by CONTAINER NAME (the `spec.name`). When
    /// `start` runs for a spec whose name has a seeded entry, that content is
    /// copied into `logs[container_id]`; otherwise a default
    /// `"<name> log line\n"` is used. Lets a test pin exact stdout (e.g.
    /// `hello-engenho`) before the container starts.
    seeded_logs_by_name: BTreeMap<String, String>,
}

/// Operation log entry for `FakeBackend`. Tests assert this shape
/// to prove the kubelet drove the backend correctly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FakeEvent {
    /// `start(spec)` was invoked; spec name captured.
    Start(String),
    /// `stop(container_id)`.
    Stop(String),
    /// `remove(container_id)`.
    Remove(String),
}

impl FakeBackend {
    /// Fresh empty backend.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of all containers currently tracked.
    pub async fn containers(&self) -> Vec<(String, ContainerStatus)> {
        let state = self.inner.lock().await;
        state
            .containers
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Snapshot of the operations log (for test assertions).
    pub async fn events(&self) -> Vec<FakeEvent> {
        self.inner.lock().await.events.clone()
    }

    /// Count of currently-running containers.
    pub async fn running_count(&self) -> usize {
        self.inner
            .lock()
            .await
            .containers
            .values()
            .filter(|s| s.running)
            .count()
    }

    /// Test hook: simulate a container exiting ON ITS OWN with
    /// `exit_code` (distinct from an operator-initiated [`stop`]). Flips
    /// the tracked container to `running == false` + records the exit
    /// code, WITHOUT emitting a [`FakeEvent::Stop`] — modeling a process
    /// that terminated by itself rather than being told to. The kubelet's
    /// running-status poll then observes the terminated container on its
    /// next tick.
    ///
    /// No-op (silently) if `container_id` isn't tracked — mirrors a
    /// best-effort host observation.
    ///
    /// [`stop`]: ContainerRuntime::stop
    pub async fn set_exit(&self, container_id: &str, exit_code: i32) {
        let mut state = self.inner.lock().await;
        if let Some(s) = state.containers.get_mut(container_id) {
            s.running = false;
            s.exit_code = Some(exit_code);
        }
    }

    /// Test hook: pre-seed the exact log content a container of name
    /// `container_name` (the `spec.name`) will report. Must be called BEFORE
    /// the container starts; at `start` the content is copied into the
    /// container's log buffer keyed by its assigned `container_id`. Lets a
    /// test assert `kubectl logs` returns a known string (e.g.
    /// `"hello-engenho\n"`).
    pub async fn seed_log(&self, container_name: &str, content: impl Into<String>) {
        let mut state = self.inner.lock().await;
        state
            .seeded_logs_by_name
            .insert(container_name.to_string(), content.into());
    }

    /// The current log buffer for a container_id (test inspection helper).
    pub async fn log_of(&self, container_id: &str) -> Option<String> {
        self.inner.lock().await.logs.get(container_id).cloned()
    }
}

#[async_trait]
impl ContainerRuntime for FakeBackend {
    fn name(&self) -> &'static str {
        "fake"
    }

    async fn start(&self, spec: &ContainerSpec) -> Result<ContainerStatus, KubeletError> {
        let mut state = self.inner.lock().await;
        state.next_id += 1;
        let container_id = format!("fake-{:08x}", state.next_id);
        let pod_ip = format!("10.42.0.{}", state.next_id % 250);
        let status = ContainerStatus::running(container_id.clone(), pod_ip);
        state
            .containers
            .insert(container_id.clone(), status.clone());
        // Seed the log buffer: pre-seeded content for this container NAME if a
        // test set it (FakeBackend::seed_log), else a deterministic default
        // derived from the name so logs() always returns a non-empty,
        // assertable string.
        let log_content = state
            .seeded_logs_by_name
            .get(&spec.name)
            .cloned()
            .unwrap_or_else(|| format!("{} log line\n", spec.name));
        state.logs.insert(container_id.clone(), log_content);
        state.specs.insert(container_id, spec.clone());
        state.events.push(FakeEvent::Start(spec.name.clone()));
        Ok(status)
    }

    async fn status(&self, container_id: &str) -> Result<Option<ContainerStatus>, KubeletError> {
        Ok(self
            .inner
            .lock()
            .await
            .containers
            .get(container_id)
            .cloned())
    }

    async fn stop(&self, container_id: &str) -> Result<(), KubeletError> {
        let mut state = self.inner.lock().await;
        if let Some(s) = state.containers.get_mut(container_id) {
            s.running = false;
            s.exit_code = Some(0);
        }
        state.events.push(FakeEvent::Stop(container_id.to_string()));
        Ok(())
    }

    async fn remove(&self, container_id: &str) -> Result<(), KubeletError> {
        let mut state = self.inner.lock().await;
        state.containers.remove(container_id);
        state.specs.remove(container_id);
        state.logs.remove(container_id);
        state
            .events
            .push(FakeEvent::Remove(container_id.to_string()));
        Ok(())
    }

    async fn logs(
        &self,
        container_id: &str,
        opts: &LogOptions,
    ) -> Result<String, KubeletError> {
        let state = self.inner.lock().await;
        let buf = state.logs.get(container_id).ok_or_else(|| {
            // No buffer → no such container. Typed error, never an empty Ok.
            KubeletError::Backend(format!("no such container for logs: {container_id}"))
        })?;
        // Honor --tail by returning only the last N lines (mirrors podman's
        // `--tail` line semantics) so the fake's behavior matches the real
        // backend for the test assertions.
        match opts.tail {
            Some(n) if n > 0 => {
                let lines: Vec<&str> = buf.lines().collect();
                let start = lines.len().saturating_sub(n as usize);
                let tail = lines[start..].join("\n");
                // Preserve a trailing newline if the buffer had one.
                if buf.ends_with('\n') && !tail.is_empty() {
                    Ok(format!("{tail}\n"))
                } else {
                    Ok(tail)
                }
            }
            // tail=0 returns nothing (podman --tail 0); None returns all.
            Some(_) => Ok(String::new()),
            None => Ok(buf.clone()),
        }
    }
}

// =================================================================
// PodmanBackend — shells out to `podman` for local Mac/Linux
// =================================================================

/// `--pull` policy passed to `podman run`. Typed so the backend has ONE
/// extensible knob for image-pull behavior instead of a hard-coded flag.
///
/// M0.2 defaults to [`PullPolicy::Never`] because the only image the
/// real-container path uses (`busybox`) is pre-cached, and the host's
/// container credential helper is broken (a registry contact would fail).
/// The other variants keep the backend forward-compatible without a
/// second code path: when a cached miss is acceptable, the operator
/// selects [`PullPolicy::Missing`] / [`PullPolicy::IfNotPresent`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PullPolicy {
    /// Never contact a registry; the image MUST already be in local
    /// storage. Maps to `--pull never`. The M0.2 default.
    #[default]
    Never,
    /// Pull only when the image is absent from local storage. Maps to
    /// `--pull missing` (podman's "pull if not in store" mode).
    Missing,
    /// Alias for [`PullPolicy::Missing`] phrased the K8s way; pulls only
    /// when the image isn't already present locally. Maps to
    /// `--pull newer` so a stale cached layer is refreshed when the
    /// registry advertises a newer one.
    IfNotPresent,
}

impl PullPolicy {
    /// The `--pull` argument value this policy maps to.
    #[must_use]
    pub fn flag_value(self) -> &'static str {
        match self {
            PullPolicy::Never => "never",
            PullPolicy::Missing => "missing",
            PullPolicy::IfNotPresent => "newer",
        }
    }
}

/// Podman network the backend attaches every container to. Typed so
/// the network-membership knob is a closed enum (mirroring [`PullPolicy`])
/// rather than a bare string threaded through the run path.
///
/// M0.3 defaults to [`PodmanNetwork::Named`]`("engenho-net")`: a single
/// shared user-defined network every engenho pod joins so containers
/// reach each other by IP (pod-to-pod L3 connectivity). On podman 5.x /
/// netavark a user-defined network is reachable container-to-container
/// AND surfaces a per-network IP under
/// `NetworkSettings.Networks.<net>.IPAddress` — which is what the
/// kubelet records as `status.podIP`. The legacy top-level
/// `NetworkSettings.IPAddress` field is empty on modern netavark, so a
/// shared named network is load-bearing for the podIP→Endpoints path.
///
/// [`PodmanNetwork::Default`] keeps the backend forward-compatible: an
/// operator who wants podman's ambient default network (no `--network`
/// flag) selects it. No `format!()` for the flag value — the typed
/// method returns the network name directly (per ★★ TYPED EMISSION).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "name")]
pub enum PodmanNetwork {
    /// Attach every container to a named user-defined network. The
    /// M0.3 default (`"engenho-net"`); [`PodmanBackend::ensure_network`]
    /// idempotently creates it before the first `start`.
    Named(String),
    /// Use podman's ambient default network — emit NO `--network` flag.
    /// Forward-compat escape hatch; the per-network IP extraction still
    /// works because the inspect template ranges over ALL networks.
    Default,
}

impl Default for PodmanNetwork {
    fn default() -> Self {
        Self::Named("engenho-net".to_string())
    }
}

impl PodmanNetwork {
    /// The network NAME this variant attaches to, or `None` for
    /// [`PodmanNetwork::Default`] (no explicit network). Used to decide
    /// whether `run_argv` emits a `--network <name>` flag and whether
    /// `ensure_network` has work to do. Typed accessor — no `format!()`.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        match self {
            PodmanNetwork::Named(n) => Some(n.as_str()),
            PodmanNetwork::Default => None,
        }
    }
}

/// Real podman backend. Shells out to the `podman` CLI on the host.
/// Designed for local engenho-managed clusters running on Darwin
/// (podman machine) + Linux nodes. Per the org's NO SHELL rule,
/// this is the ONE acceptable shell-out site — invoking `podman`
/// is the *integration boundary*, not orchestration glue.
///
/// ## Reliability invariants (M0.2)
///
///   * **Pull policy** — every `podman run` carries an explicit
///     `--pull <policy>` flag (default [`PullPolicy::Never`]) so the
///     backend never falls back to podman's ambient policy, which would
///     contact a registry through a possibly-broken credential helper.
///   * **`REGISTRY_AUTH_FILE`** — the backend NEVER hardcodes an auth
///     path. By default it inherits the spawned process env (tokio
///     `Command` inherits the parent env), so an operator/test that
///     exports `REGISTRY_AUTH_FILE` pointing at a valid (possibly empty
///     `{"auths":{}}`) authfile bypasses the broken machine credential
///     helper. [`registry_auth_file`] is an optional typed override that
///     sets the env var explicitly (deterministic for tests).
///   * **Deterministic naming** — the container name is built
///     deterministically by the kubelet (`<namespace>_<pod>`), so
///     status/stop/remove by-name is possible in a future item. `start`
///     surfaces a podman name-conflict as a recognizable typed error
///     (stderr contains "already in use") rather than a generic failure.
///
/// ## Networking invariants (M0.3)
///
///   * **Shared network membership** — every `podman run` carries
///     `--network <name>` (default `engenho-net`) so all engenho pods
///     join ONE user-defined network + reach each other by IP.
///     [`ensure_network`] idempotently creates it (exists-then-create)
///     before the first `start`.
///   * **Per-network IP extraction** — `inspect` reads the per-network
///     `NetworkSettings.Networks.<net>.IPAddress` (ranged over all
///     networks), NOT the empty legacy top-level `NetworkSettings.IPAddress`
///     field, so the kubelet records a real `status.podIP` → the
///     EndpointsController populates Service Endpoints with real IPs.
///   * **Prompt IP surfacing** — after a successful `run`, `start`
///     re-inspects so it can return `pod_ip: Some(..)` immediately,
///     closing the one-tick empty-Endpoints window.
///
/// [`registry_auth_file`]: PodmanBackend::registry_auth_file
/// [`ensure_network`]: PodmanBackend::ensure_network
pub struct PodmanBackend {
    /// Binary name or absolute path of the podman CLI (default `podman`).
    pub binary: String,
    /// `--pull` policy for `podman run` (default [`PullPolicy::Never`]).
    pub pull_policy: PullPolicy,
    /// Optional explicit `REGISTRY_AUTH_FILE` value. `None` (the default)
    /// inherits the spawned process env — production reads the operator's
    /// ambient `REGISTRY_AUTH_FILE`. `Some(path)` forces it explicitly so
    /// a test is deterministic without depending on ambient env.
    pub registry_auth_file: Option<PathBuf>,
    /// Shared podman network every container joins (default
    /// [`PodmanNetwork::Named`]`("engenho-net")`). Makes pod-to-pod
    /// L3 connectivity + per-network IP extraction work.
    pub network: PodmanNetwork,
}

impl Default for PodmanBackend {
    fn default() -> Self {
        Self {
            binary: "podman".to_string(),
            pull_policy: PullPolicy::Never,
            registry_auth_file: None,
            network: PodmanNetwork::default(),
        }
    }
}

impl PodmanBackend {
    /// New backend using the host's `podman` from `$PATH`, pull policy
    /// [`PullPolicy::Never`], and `REGISTRY_AUTH_FILE` inherited from the
    /// process env.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// New backend with an explicit podman binary path. Other fields keep
    /// their defaults (pull Never + inherit auth env).
    #[must_use]
    pub fn with_binary(binary: impl Into<String>) -> Self {
        Self {
            binary: binary.into(),
            ..Self::default()
        }
    }

    /// Builder: set the `--pull` policy.
    #[must_use]
    pub fn with_pull_policy(mut self, pull_policy: PullPolicy) -> Self {
        self.pull_policy = pull_policy;
        self
    }

    /// Builder: set an explicit `REGISTRY_AUTH_FILE` override. `None`
    /// inherits the process env (the default).
    #[must_use]
    pub fn with_registry_auth_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.registry_auth_file = Some(path.into());
        self
    }

    /// Builder: set the shared [`PodmanNetwork`] every container joins.
    /// Defaults to `Named("engenho-net")`; pass [`PodmanNetwork::Default`]
    /// to use podman's ambient network (no `--network` flag).
    #[must_use]
    pub fn with_network(mut self, network: PodmanNetwork) -> Self {
        self.network = network;
        self
    }

    // ── Pure typed argv builders (unit-testable without podman) ─────────
    //
    // Each builder emits the argv AFTER the binary, so a test can assert
    // the exact ContainerSpec → podman mapping without spawning a process.
    // The async methods below construct their `Command` from these vecs.

    /// `podman run` argv for `spec`, honoring `pull_policy` + `network` +
    /// per-pod `network_aliases`:
    /// `["run","-d", ("--network",<net>)?, ("--network-alias",<alias>)*, "--pull",<policy>,"--name",<name>, (-e k=v)*, <image>, (cmd)*]`.
    ///
    /// The `--network <name>` flag is emitted immediately after `-d` when
    /// the backend's [`PodmanNetwork`] names a network (the M0.3 default
    /// `engenho-net`); [`PodmanNetwork::Default`] omits it (ambient
    /// network).
    ///
    /// Each entry of `spec.network_aliases` becomes a `--network-alias
    /// <alias>` pair, emitted immediately AFTER the `--network <net>` pair
    /// and BEFORE `--pull` (deterministic, unit-assertable position).
    /// aardvark-dns (which already runs for a user-defined network) then
    /// resolves those alias names to this container's IP, so a pod resolves
    /// a Service name to the backend pod IPs (headless multi-A). Aliases
    /// are emitted ONLY when [`PodmanNetwork::name`] is `Some(_)`: on the
    /// ambient [`PodmanNetwork::Default`] network there is no aardvark-dns
    /// alias resolution, so aliases would be silently meaningless — we omit
    /// them entirely rather than emit a flag that does nothing.
    #[must_use]
    pub fn run_argv(&self, spec: &ContainerSpec) -> Vec<String> {
        let mut argv = vec!["run".to_string(), "-d".to_string()];
        // Shared-network membership: pod-to-pod L3 connectivity + a
        // per-network IP the kubelet records as status.podIP.
        if let Some(net) = self.network.name() {
            argv.push("--network".to_string());
            argv.push(net.to_string());
            // Service-name DNS aliases for aardvark-dns. ONLY on a named
            // user network (aardvark-dns resolves --network-alias names
            // there); on the ambient network they're meaningless, so the
            // `if let Some` guard already gates this block.
            for alias in &spec.network_aliases {
                argv.push("--network-alias".to_string());
                argv.push(alias.clone());
            }
        }
        argv.push("--pull".to_string());
        argv.push(self.pull_policy.flag_value().to_string());
        argv.push("--name".to_string());
        argv.push(spec.name.clone());
        // BTreeMap iteration is sorted → deterministic env ordering.
        for (k, v) in &spec.env {
            argv.push("-e".to_string());
            argv.push(format!("{k}={v}"));
        }
        argv.push(spec.image.clone());
        for arg in &spec.command {
            argv.push(arg.clone());
        }
        argv
    }

    /// `podman inspect` argv for `id` — the exact pipe template the
    /// status parser ([`parse_inspect`]) consumes.
    ///
    /// The third field reads the **per-network** IP by ranging over
    /// `NetworkSettings.Networks` — NOT the legacy top-level
    /// `NetworkSettings.IPAddress`, which is EMPTY on modern netavark
    /// (podman 5.x) when the container is on a user-defined network. Each
    /// engenho container is attached to exactly one network (`engenho-net`),
    /// so the range yields exactly that network's `IPAddress`. Ranging
    /// (rather than hard-coding the network name) keeps the template robust
    /// if [`PodmanNetwork`] is reconfigured. Empty (no networks / unbound)
    /// → `parse_inspect` yields `pod_ip = None`.
    ///
    /// [`parse_inspect`]: PodmanBackend::parse_inspect
    #[must_use]
    pub fn inspect_argv(id: &str) -> Vec<String> {
        vec![
            "inspect".to_string(),
            "--format".to_string(),
            "{{.State.Running}}|{{.State.ExitCode}}|{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}".to_string(),
            id.to_string(),
        ]
    }

    /// `podman network exists <name>` argv — exits 0 iff the network is
    /// already present. Used by [`ensure_network`] to make creation
    /// idempotent.
    ///
    /// [`ensure_network`]: PodmanBackend::ensure_network
    #[must_use]
    pub fn network_exists_argv(name: &str) -> Vec<String> {
        vec![
            "network".to_string(),
            "exists".to_string(),
            name.to_string(),
        ]
    }

    /// `podman network create <name>` argv — creates the shared
    /// user-defined network. A name-conflict (race / leftover) is treated
    /// as success by [`ensure_network`] (idempotent), mirroring the
    /// run-path "already in use" handling.
    #[must_use]
    pub fn network_create_argv(name: &str) -> Vec<String> {
        vec![
            "network".to_string(),
            "create".to_string(),
            name.to_string(),
        ]
    }

    /// `podman stop` argv for `id`.
    #[must_use]
    pub fn stop_argv(id: &str) -> Vec<String> {
        vec!["stop".to_string(), id.to_string()]
    }

    /// `podman rm -f` argv for `id`. `-f` force-stops then removes;
    /// the kubelet already does stop-then-remove so `-f` is redundant
    /// but harmless (and makes remove idempotent on a still-running id).
    #[must_use]
    pub fn rm_argv(id: &str) -> Vec<String> {
        vec!["rm".to_string(), "-f".to_string(), id.to_string()]
    }

    /// `podman logs [--tail N] [--timestamps] <id>` argv for a container's
    /// stdout/stderr. Pure (no podman) so the argv is unit-testable.
    ///
    /// `--tail N` is emitted only when [`LogOptions::tail`] is `Some(n)`;
    /// `--timestamps` only when [`LogOptions::timestamps`] is set. The order is
    /// deterministic (flags before the container id) so the argv is
    /// assertable.
    #[must_use]
    pub fn logs_argv(id: &str, opts: &LogOptions) -> Vec<String> {
        let mut argv = vec!["logs".to_string()];
        if let Some(n) = opts.tail {
            argv.push("--tail".to_string());
            argv.push(n.to_string());
        }
        if opts.timestamps {
            argv.push("--timestamps".to_string());
        }
        argv.push(id.to_string());
        argv
    }

    /// Parse the pipe-delimited `podman inspect --format` output into a
    /// typed [`ContainerStatus`]. Pure (no podman) so the parse is
    /// unit-testable. Expects `Running|ExitCode|IPAddress`.
    ///
    /// # Errors
    ///
    /// [`KubeletError::Backend`] when the output has fewer than 3
    /// pipe-separated fields (an unexpected podman shape — surfaced, never
    /// silently mis-parsed).
    pub fn parse_inspect(
        container_id: &str,
        text: &str,
    ) -> Result<ContainerStatus, KubeletError> {
        let text = text.trim();
        let parts: Vec<&str> = text.split('|').collect();
        if parts.len() < 3 {
            return Err(KubeletError::Backend(format!(
                "podman inspect output unexpected shape: {text}"
            )));
        }
        let running = parts[0] == "true";
        let exit_code = parts[1].parse::<i32>().ok();
        let pod_ip = if parts[2].is_empty() {
            None
        } else {
            Some(parts[2].to_string())
        };
        Ok(ContainerStatus {
            container_id: container_id.to_string(),
            running,
            pod_ip,
            exit_code,
        })
    }

    /// Build a `podman` command from a pre-built argv, forwarding the
    /// `REGISTRY_AUTH_FILE` override when one is configured. When
    /// `registry_auth_file` is `None` the command inherits the parent
    /// process env (tokio `Command` inherits by default — we never call
    /// `env_clear`), so an ambient `REGISTRY_AUTH_FILE` flows through
    /// unchanged. podman reads the variable natively.
    fn command(&self, argv: &[String]) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new(&self.binary);
        cmd.args(argv);
        if let Some(path) = &self.registry_auth_file {
            cmd.env("REGISTRY_AUTH_FILE", path);
        }
        cmd
    }

    /// Idempotently ensure the shared engenho podman network exists.
    ///
    /// Runs `podman network exists <name>` first; if that exits 0 the
    /// network is already present and we're done. Otherwise we run
    /// `podman network create <name>` and treat a name-conflict
    /// ("already exists" / "already used" — a race or a leftover) as
    /// success, mirroring the run-path name-conflict handling. A
    /// [`PodmanNetwork::Default`] backend has no named network to ensure,
    /// so this is a no-op.
    ///
    /// The kubelet/Runtime calls this once before the first `start`
    /// (or eagerly at construction) so every subsequent `podman run
    /// --network <name>` finds the network present.
    ///
    /// # Errors
    ///
    /// [`KubeletError::Backend`] if `podman network create` fails for a
    /// reason OTHER than the network already existing (spawn failure,
    /// permission denied, etc.) — surfaced, never silently swallowed.
    pub async fn ensure_network(&self) -> Result<(), KubeletError> {
        let Some(name) = self.network.name() else {
            // PodmanNetwork::Default — nothing to ensure.
            return Ok(());
        };

        // (1) exists? — `podman network exists <name>` exits 0 iff present.
        let exists = self
            .command(&Self::network_exists_argv(name))
            .output()
            .await
            .map_err(|e| KubeletError::Backend(format!("podman network exists spawn: {e}")))?;
        if exists.status.success() {
            return Ok(());
        }

        // (2) create — idempotent: a concurrent creator / leftover that
        // wins the race surfaces an "already exists" stderr we treat as
        // success (mirrors the run-path "already in use" handling).
        let created = self
            .command(&Self::network_create_argv(name))
            .output()
            .await
            .map_err(|e| KubeletError::Backend(format!("podman network create spawn: {e}")))?;
        if created.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&created.stderr);
        if stderr.contains("already exists") || stderr.contains("already used") {
            return Ok(());
        }
        Err(KubeletError::Backend(format!(
            "podman network create {name:?} failed (status {:?}): {stderr}",
            created.status.code(),
        )))
    }
}

#[async_trait]
impl ContainerRuntime for PodmanBackend {
    fn name(&self) -> &'static str {
        "podman"
    }

    async fn start(&self, spec: &ContainerSpec) -> Result<ContainerStatus, KubeletError> {
        // (M0.3 item a) Ensure the shared engenho network exists before the
        // first run so `--network <name>` resolves. Idempotent + cheap
        // (exists-check short-circuits after the first creation); a
        // PodmanNetwork::Default backend makes this a no-op.
        self.ensure_network().await?;

        let argv = self.run_argv(spec);
        let out = self
            .command(&argv)
            .output()
            .await
            .map_err(|e| KubeletError::Backend(format!("podman run spawn: {e}")))?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            // Surface a name-conflict (a leftover container from a crashed
            // prior run) as a recognizable typed error so the kubelet/test
            // sees the specific class rather than an opaque failure. Full
            // adopt-existing-by-name is out of M0.2 scope; the
            // deterministic name <namespace>_<pod> makes by-name
            // reconciliation possible in a future item.
            if stderr.contains("already in use") {
                return Err(KubeletError::Backend(format!(
                    "podman run name conflict for {:?}: a container with this name already exists \
                     (leftover from a prior run); stderr: {stderr}",
                    spec.name
                )));
            }
            return Err(KubeletError::Backend(format!(
                "podman run failed (status {:?}): {stderr}",
                out.status.code(),
            )));
        }
        let container_id = String::from_utf8_lossy(&out.stdout).trim().to_string();

        // (M0.3 item c) Re-inspect immediately so `start` can return a real
        // per-network `pod_ip` instead of always `None`, closing the
        // one-tick empty-Endpoints window. The IP is usually assigned by
        // the time `run` returns the container id; if it isn't yet (rare
        // race) we fall back to `running: true, pod_ip: None` — the
        // kubelet's reconcile_running poll converges it on the next tick,
        // so this is a latency optimization, not a correctness dependency.
        // An inspect error here is non-fatal for the same reason: the
        // container DID start (run succeeded); we just couldn't read its IP
        // yet, so we return the started status and let the poll path retry.
        match self.status(&container_id).await {
            Ok(Some(observed)) if observed.running => Ok(observed),
            _ => Ok(ContainerStatus {
                container_id,
                running: true,
                pod_ip: None, // not yet readable; status() poll converges it
                exit_code: None,
            }),
        }
    }

    async fn status(&self, container_id: &str) -> Result<Option<ContainerStatus>, KubeletError> {
        let argv = Self::inspect_argv(container_id);
        let out = self
            .command(&argv)
            .output()
            .await
            .map_err(|e| KubeletError::Backend(format!("podman inspect spawn: {e}")))?;
        if !out.status.success() {
            // Not found is a normal case; return None.
            let stderr = String::from_utf8_lossy(&out.stderr);
            if stderr.contains("no such container") || stderr.contains("not found") {
                return Ok(None);
            }
            return Err(KubeletError::Backend(format!(
                "podman inspect failed: {stderr}"
            )));
        }
        let text = String::from_utf8_lossy(&out.stdout);
        Ok(Some(Self::parse_inspect(container_id, &text)?))
    }

    async fn stop(&self, container_id: &str) -> Result<(), KubeletError> {
        let argv = Self::stop_argv(container_id);
        let out = self
            .command(&argv)
            .output()
            .await
            .map_err(|e| KubeletError::Backend(format!("podman stop spawn: {e}")))?;
        if !out.status.success() {
            return Err(KubeletError::Backend(format!(
                "podman stop failed: {}",
                String::from_utf8_lossy(&out.stderr)
            )));
        }
        Ok(())
    }

    async fn remove(&self, container_id: &str) -> Result<(), KubeletError> {
        let argv = Self::rm_argv(container_id);
        let out = self
            .command(&argv)
            .output()
            .await
            .map_err(|e| KubeletError::Backend(format!("podman rm spawn: {e}")))?;
        if !out.status.success() {
            return Err(KubeletError::Backend(format!(
                "podman rm failed: {}",
                String::from_utf8_lossy(&out.stderr)
            )));
        }
        Ok(())
    }

    async fn logs(
        &self,
        container_id: &str,
        opts: &LogOptions,
    ) -> Result<String, KubeletError> {
        let argv = Self::logs_argv(container_id, opts);
        let out = self
            .command(&argv)
            .output()
            .await
            .map_err(|e| KubeletError::Backend(format!("podman logs spawn: {e}")))?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            // A not-found container is a typed error (the kubelet maps it to a
            // 404 / empty response upstream), NOT a silently-empty Ok.
            return Err(KubeletError::Backend(format!(
                "podman logs failed for {container_id}: {stderr}"
            )));
        }
        // podman writes container stdout to its own stdout; stderr carries
        // podman diagnostics (empty on success). The container log is stdout.
        Ok(String::from_utf8_lossy(&out.stdout).to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fake_backend_starts_assigns_id_and_ip() {
        let backend = FakeBackend::new();
        let spec = ContainerSpec {
            name: "podinfo".into(),
            image: "stefanprodan/podinfo:6".into(),
            env: BTreeMap::new(),
            command: vec![],
            ..Default::default()
        };
        let status = backend.start(&spec).await.unwrap();
        assert!(status.running);
        assert!(status.pod_ip.is_some());
        assert!(status.container_id.starts_with("fake-"));
        assert_eq!(backend.running_count().await, 1);
    }

    #[tokio::test]
    async fn fake_backend_stop_marks_not_running() {
        let backend = FakeBackend::new();
        let spec = ContainerSpec {
            name: "p".into(),
            image: "i".into(),
            env: BTreeMap::new(),
            command: vec![],
            ..Default::default()
        };
        let s = backend.start(&spec).await.unwrap();
        backend.stop(&s.container_id).await.unwrap();
        let after = backend.status(&s.container_id).await.unwrap().unwrap();
        assert!(!after.running);
        assert_eq!(after.exit_code, Some(0));
    }

    #[tokio::test]
    async fn fake_backend_remove_clears_state() {
        let backend = FakeBackend::new();
        let spec = ContainerSpec {
            name: "p".into(),
            image: "i".into(),
            env: BTreeMap::new(),
            command: vec![],
            ..Default::default()
        };
        let s = backend.start(&spec).await.unwrap();
        backend.remove(&s.container_id).await.unwrap();
        assert!(backend.status(&s.container_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn fake_backend_events_record_in_order() {
        let backend = FakeBackend::new();
        let spec = ContainerSpec {
            name: "a".into(),
            image: "i".into(),
            env: BTreeMap::new(),
            command: vec![],
            ..Default::default()
        };
        let s = backend.start(&spec).await.unwrap();
        backend.stop(&s.container_id).await.unwrap();
        backend.remove(&s.container_id).await.unwrap();
        let events = backend.events().await;
        assert_eq!(events.len(), 3);
        assert_eq!(events[0], FakeEvent::Start("a".into()));
        assert_eq!(events[1], FakeEvent::Stop(s.container_id.clone()));
        assert_eq!(events[2], FakeEvent::Remove(s.container_id));
    }

    #[tokio::test]
    async fn fake_backend_set_exit_marks_terminated_without_stop_event() {
        let backend = FakeBackend::new();
        let spec = ContainerSpec {
            name: "p".into(),
            image: "i".into(),
            env: BTreeMap::new(),
            command: vec![],
            ..Default::default()
        };
        let s = backend.start(&spec).await.unwrap();
        backend.set_exit(&s.container_id, 137).await;
        let after = backend.status(&s.container_id).await.unwrap().unwrap();
        assert!(!after.running);
        assert_eq!(after.exit_code, Some(137));
        // set_exit models a self-exit, NOT an operator stop — only the
        // Start event was recorded.
        let events = backend.events().await;
        assert_eq!(events, vec![FakeEvent::Start("p".into())]);
    }

    #[tokio::test]
    async fn fake_backend_set_exit_zero_is_distinct_from_running() {
        let backend = FakeBackend::new();
        let spec = ContainerSpec {
            name: "q".into(),
            image: "i".into(),
            env: BTreeMap::new(),
            command: vec![],
            ..Default::default()
        };
        let s = backend.start(&spec).await.unwrap();
        assert_eq!(backend.running_count().await, 1);
        backend.set_exit(&s.container_id, 0).await;
        assert_eq!(backend.running_count().await, 0);
        let after = backend.status(&s.container_id).await.unwrap().unwrap();
        assert_eq!(after.exit_code, Some(0));
    }

    #[test]
    fn fake_backend_name_is_stable() {
        assert_eq!(FakeBackend::new().name(), "fake");
    }

    #[test]
    fn podman_backend_name_is_stable() {
        assert_eq!(PodmanBackend::new().name(), "podman");
    }

    #[test]
    fn podman_backend_accepts_custom_binary_path() {
        let backend = PodmanBackend::with_binary("/usr/local/bin/podman");
        assert_eq!(backend.binary, "/usr/local/bin/podman");
        // with_binary keeps the safe M0.2 defaults.
        assert_eq!(backend.pull_policy, PullPolicy::Never);
        assert!(backend.registry_auth_file.is_none());
    }

    // ── PullPolicy mapping ──────────────────────────────────────────────

    #[test]
    fn pull_policy_default_is_never() {
        assert_eq!(PullPolicy::default(), PullPolicy::Never);
        assert_eq!(PodmanBackend::new().pull_policy, PullPolicy::Never);
    }

    #[test]
    fn pull_policy_flag_values() {
        assert_eq!(PullPolicy::Never.flag_value(), "never");
        assert_eq!(PullPolicy::Missing.flag_value(), "missing");
        assert_eq!(PullPolicy::IfNotPresent.flag_value(), "newer");
    }

    #[test]
    fn pull_policy_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&PullPolicy::Never).unwrap(),
            "\"never\""
        );
        assert_eq!(
            serde_json::to_string(&PullPolicy::IfNotPresent).unwrap(),
            "\"if_not_present\""
        );
    }

    // ── run_argv mapping (the load-bearing ContainerSpec → podman map) ──

    fn spec(name: &str, image: &str, env: &[(&str, &str)], command: &[&str]) -> ContainerSpec {
        ContainerSpec {
            name: name.into(),
            image: image.into(),
            env: env
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
            command: command.iter().map(|c| (*c).to_string()).collect(),
            ..Default::default()
        }
    }

    /// Like [`spec`] but with explicit `network_aliases` — used by the
    /// M0.3 `--network-alias` emission tests.
    fn spec_with_aliases(
        name: &str,
        image: &str,
        command: &[&str],
        aliases: &[&str],
    ) -> ContainerSpec {
        ContainerSpec {
            name: name.into(),
            image: image.into(),
            env: BTreeMap::new(),
            command: command.iter().map(|c| (*c).to_string()).collect(),
            network_aliases: aliases.iter().map(|a| (*a).to_string()).collect(),
        }
    }

    #[test]
    fn run_argv_full_mapping_in_order() {
        let backend = PodmanBackend::new();
        let s = spec(
            "default_p1",
            "docker.io/library/busybox",
            &[("FOO", "bar")],
            &["sleep", "300"],
        );
        assert_eq!(
            backend.run_argv(&s),
            vec![
                "run",
                "-d",
                // M0.3: shared-network membership, right after -d.
                "--network",
                "engenho-net",
                "--pull",
                "never",
                "--name",
                "default_p1",
                "-e",
                "FOO=bar",
                "docker.io/library/busybox",
                "sleep",
                "300",
            ]
        );
    }

    #[test]
    fn run_argv_empty_env_emits_no_dash_e() {
        let backend = PodmanBackend::new();
        let s = spec("c", "img", &[], &["echo", "hi"]);
        let argv = backend.run_argv(&s);
        assert!(!argv.iter().any(|a| a == "-e"), "no -e for empty env: {argv:?}");
        assert_eq!(
            argv,
            vec![
                "run", "-d", "--network", "engenho-net", "--pull", "never", "--name", "c", "img",
                "echo", "hi"
            ]
        );
    }

    #[test]
    fn run_argv_empty_command_emits_no_trailing_args() {
        let backend = PodmanBackend::new();
        let s = spec("c", "img", &[], &[]);
        assert_eq!(
            backend.run_argv(&s),
            vec!["run", "-d", "--network", "engenho-net", "--pull", "never", "--name", "c", "img"]
        );
    }

    #[test]
    fn run_argv_multi_env_is_sorted_deterministically() {
        // ContainerSpec.env is a BTreeMap → iteration is sorted, so the
        // -e pairs are emitted in deterministic key order regardless of
        // insertion order.
        let backend = PodmanBackend::new();
        let s = spec("c", "img", &[("ZED", "9"), ("ALPHA", "1"), ("MID", "5")], &[]);
        assert_eq!(
            backend.run_argv(&s),
            vec![
                "run", "-d", "--network", "engenho-net", "--pull", "never", "--name", "c", //
                "-e", "ALPHA=1", "-e", "MID=5", "-e", "ZED=9", //
                "img",
            ]
        );
    }

    #[test]
    fn run_argv_honors_custom_pull_policy() {
        let backend = PodmanBackend::new().with_pull_policy(PullPolicy::IfNotPresent);
        let s = spec("c", "img", &[], &[]);
        let argv = backend.run_argv(&s);
        // The --pull flag value reflects the configured policy.
        let pull_idx = argv.iter().position(|a| a == "--pull").unwrap();
        assert_eq!(argv[pull_idx + 1], "newer");
    }

    // ── M0.3 shared-network membership ──────────────────────────────────

    #[test]
    fn run_argv_includes_shared_network_right_after_d() {
        let backend = PodmanBackend::new();
        let s = spec("c", "img", &[], &[]);
        let argv = backend.run_argv(&s);
        // --network engenho-net is emitted immediately after -d.
        let d_idx = argv.iter().position(|a| a == "-d").unwrap();
        assert_eq!(argv[d_idx + 1], "--network");
        assert_eq!(argv[d_idx + 2], "engenho-net");
    }

    #[test]
    fn run_argv_with_custom_named_network_overrides() {
        let backend = PodmanBackend::new().with_network(PodmanNetwork::Named("custom-net".into()));
        let s = spec("c", "img", &[], &[]);
        let argv = backend.run_argv(&s);
        let net_idx = argv.iter().position(|a| a == "--network").unwrap();
        assert_eq!(argv[net_idx + 1], "custom-net");
    }

    #[test]
    fn run_argv_default_network_emits_no_network_flag() {
        // PodmanNetwork::Default → ambient network → NO --network flag.
        let backend = PodmanBackend::new().with_network(PodmanNetwork::Default);
        let s = spec("c", "img", &[], &[]);
        let argv = backend.run_argv(&s);
        assert!(
            !argv.iter().any(|a| a == "--network"),
            "Default network must not emit --network: {argv:?}"
        );
        assert_eq!(
            argv,
            vec!["run", "-d", "--pull", "never", "--name", "c", "img"]
        );
    }

    #[test]
    fn podman_network_default_is_engenho_net() {
        assert_eq!(
            PodmanNetwork::default(),
            PodmanNetwork::Named("engenho-net".into())
        );
        assert_eq!(PodmanBackend::new().network.name(), Some("engenho-net"));
    }

    #[test]
    fn podman_network_name_accessor() {
        assert_eq!(PodmanNetwork::Named("x".into()).name(), Some("x"));
        assert_eq!(PodmanNetwork::Default.name(), None);
    }

    #[test]
    fn network_exists_argv_shape() {
        assert_eq!(
            PodmanBackend::network_exists_argv("engenho-net"),
            vec!["network", "exists", "engenho-net"]
        );
    }

    #[test]
    fn network_create_argv_shape() {
        assert_eq!(
            PodmanBackend::network_create_argv("engenho-net"),
            vec!["network", "create", "engenho-net"]
        );
    }

    #[test]
    fn with_network_keeps_other_defaults() {
        let backend = PodmanBackend::new().with_network(PodmanNetwork::Named("n".into()));
        assert_eq!(backend.network, PodmanNetwork::Named("n".into()));
        assert_eq!(backend.pull_policy, PullPolicy::Never);
        assert!(backend.registry_auth_file.is_none());
    }

    // ── M0.3 cluster-DNS: --network-alias emission ──────────────────────
    //
    // The kubelet computes Service-name aliases (in kubelet.rs) + stores
    // them on ContainerSpec.network_aliases; run_argv emits one
    // `--network-alias <alias>` pair per entry, AFTER `--network <net>`
    // and BEFORE `--pull`, ONLY on a named user network (aardvark-dns
    // resolves --network-alias names only there).

    #[test]
    fn run_argv_emits_network_alias_after_network_before_pull() {
        let backend = PodmanBackend::new();
        let s = spec_with_aliases(
            "default_web-abc",
            "img",
            &[],
            &["web", "web.default", "web.default.svc.cluster.local"],
        );
        let argv = backend.run_argv(&s);
        assert_eq!(
            argv,
            vec![
                "run",
                "-d",
                "--network",
                "engenho-net",
                "--network-alias",
                "web",
                "--network-alias",
                "web.default",
                "--network-alias",
                "web.default.svc.cluster.local",
                "--pull",
                "never",
                "--name",
                "default_web-abc",
                "img",
            ]
        );
        // Position guard (mirrors run_argv_includes_shared_network_right_after_d):
        // every alias flag sits after the --network pair and before --pull.
        let net_idx = argv.iter().position(|a| a == "--network").unwrap();
        let pull_idx = argv.iter().position(|a| a == "--pull").unwrap();
        let first_alias = argv.iter().position(|a| a == "--network-alias").unwrap();
        let last_alias = argv.iter().rposition(|a| a == "--network-alias").unwrap();
        assert!(net_idx < first_alias, "alias flags must follow --network");
        assert!(last_alias < pull_idx, "alias flags must precede --pull");
    }

    #[test]
    fn run_argv_empty_aliases_emit_no_network_alias() {
        // No aliases → NO --network-alias token (mirrors
        // run_argv_empty_env_emits_no_dash_e).
        let backend = PodmanBackend::new();
        let s = spec_with_aliases("c", "img", &[], &[]);
        let argv = backend.run_argv(&s);
        assert!(
            !argv.iter().any(|a| a == "--network-alias"),
            "no --network-alias for empty aliases: {argv:?}"
        );
        // The base shape is unchanged from the no-alias case.
        assert_eq!(
            argv,
            vec!["run", "-d", "--network", "engenho-net", "--pull", "never", "--name", "c", "img"]
        );
    }

    #[test]
    fn run_argv_default_network_omits_aliases_even_when_present() {
        // PodmanNetwork::Default → ambient network → aardvark-dns does NOT
        // resolve --network-alias names → omit them entirely. Regression
        // guards the "ambient network can't resolve aliases" rule: aliases
        // are present on the spec but MUST NOT appear in argv.
        let backend = PodmanBackend::new().with_network(PodmanNetwork::Default);
        let s = spec_with_aliases("c", "img", &[], &["web", "web.default"]);
        let argv = backend.run_argv(&s);
        assert!(
            !argv.iter().any(|a| a == "--network-alias"),
            "Default network must not emit --network-alias even with aliases set: {argv:?}"
        );
        assert!(
            !argv.iter().any(|a| a == "--network"),
            "Default network must not emit --network: {argv:?}"
        );
        assert_eq!(
            argv,
            vec!["run", "-d", "--pull", "never", "--name", "c", "img"]
        );
    }

    #[test]
    fn run_argv_full_mapping_with_aliases_in_order() {
        // Golden: env + aliases + command together pin the complete argv
        // shape (extends run_argv_full_mapping_in_order with the alias
        // block). Ordering: run -d, --network, --network-alias*, --pull,
        // --name, -e*, image, cmd*.
        let backend = PodmanBackend::new();
        let mut s = spec(
            "default_p1",
            "docker.io/library/busybox",
            &[("FOO", "bar")],
            &["sleep", "300"],
        );
        s.network_aliases = vec!["web".into(), "web.default".into()];
        assert_eq!(
            backend.run_argv(&s),
            vec![
                "run",
                "-d",
                "--network",
                "engenho-net",
                "--network-alias",
                "web",
                "--network-alias",
                "web.default",
                "--pull",
                "never",
                "--name",
                "default_p1",
                "-e",
                "FOO=bar",
                "docker.io/library/busybox",
                "sleep",
                "300",
            ]
        );
    }

    // ── inspect / stop / rm argv ────────────────────────────────────────

    #[test]
    fn inspect_argv_reads_per_network_ip() {
        let argv = PodmanBackend::inspect_argv("abc123");
        assert_eq!(
            argv,
            vec![
                "inspect",
                "--format",
                "{{.State.Running}}|{{.State.ExitCode}}|{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                "abc123",
            ]
        );
        // Regression guard: the template MUST read the per-network IP,
        // NOT the empty legacy top-level NetworkSettings.IPAddress field.
        let template = &argv[2];
        assert!(
            template.contains(".NetworkSettings.Networks"),
            "inspect template must range over .NetworkSettings.Networks: {template}"
        );
        assert!(
            !template.contains("{{.NetworkSettings.IPAddress}}"),
            "inspect template must NOT read the empty legacy top-level field: {template}"
        );
    }

    #[test]
    fn stop_argv_maps_id() {
        assert_eq!(PodmanBackend::stop_argv("abc123"), vec!["stop", "abc123"]);
    }

    #[test]
    fn rm_argv_force_removes_id() {
        assert_eq!(PodmanBackend::rm_argv("abc123"), vec!["rm", "-f", "abc123"]);
    }

    // ── logs_argv (kubectl logs builder) ────────────────────────────────

    #[test]
    fn logs_argv_no_options_is_bare() {
        assert_eq!(
            PodmanBackend::logs_argv("abc123", &LogOptions::all()),
            vec!["logs", "abc123"]
        );
    }

    #[test]
    fn logs_argv_tail_emits_flag() {
        assert_eq!(
            PodmanBackend::logs_argv("abc123", &LogOptions::tail(50)),
            vec!["logs", "--tail", "50", "abc123"]
        );
    }

    #[test]
    fn logs_argv_timestamps_emits_flag() {
        let opts = LogOptions {
            tail: Some(10),
            timestamps: true,
        };
        assert_eq!(
            PodmanBackend::logs_argv("cid", &opts),
            vec!["logs", "--tail", "10", "--timestamps", "cid"]
        );
    }

    #[test]
    fn log_options_default_is_all_lines() {
        assert_eq!(LogOptions::default(), LogOptions::all());
        assert!(LogOptions::all().tail.is_none());
        assert_eq!(LogOptions::tail(5).tail, Some(5));
    }

    // ── FakeBackend logs buffer ─────────────────────────────────────────

    #[tokio::test]
    async fn fake_backend_logs_returns_seeded_content() {
        let backend = FakeBackend::new();
        backend.seed_log("default_p", "hello-engenho\n").await;
        let spec = ContainerSpec {
            name: "default_p".into(),
            image: "i".into(),
            ..Default::default()
        };
        let s = backend.start(&spec).await.unwrap();
        let logs = backend.logs(&s.container_id, &LogOptions::all()).await.unwrap();
        assert_eq!(logs, "hello-engenho\n");
    }

    #[tokio::test]
    async fn fake_backend_logs_default_buffer_is_name_derived() {
        let backend = FakeBackend::new();
        let spec = ContainerSpec {
            name: "web".into(),
            image: "i".into(),
            ..Default::default()
        };
        let s = backend.start(&spec).await.unwrap();
        let logs = backend.logs(&s.container_id, &LogOptions::all()).await.unwrap();
        assert!(logs.contains("web"), "default log derives from name: {logs:?}");
    }

    #[tokio::test]
    async fn fake_backend_logs_tail_trims_lines() {
        let backend = FakeBackend::new();
        backend.seed_log("c", "a\nb\nc\n").await;
        let spec = ContainerSpec {
            name: "c".into(),
            image: "i".into(),
            ..Default::default()
        };
        let s = backend.start(&spec).await.unwrap();
        let last = backend.logs(&s.container_id, &LogOptions::tail(1)).await.unwrap();
        assert_eq!(last, "c\n");
    }

    #[tokio::test]
    async fn fake_backend_logs_missing_container_is_typed_error() {
        let backend = FakeBackend::new();
        let err = backend
            .logs("nonexistent", &LogOptions::all())
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "backend");
    }

    #[tokio::test]
    async fn fake_backend_logs_cleared_on_remove() {
        let backend = FakeBackend::new();
        let spec = ContainerSpec {
            name: "c".into(),
            image: "i".into(),
            ..Default::default()
        };
        let s = backend.start(&spec).await.unwrap();
        backend.remove(&s.container_id).await.unwrap();
        assert!(backend.logs(&s.container_id, &LogOptions::all()).await.is_err());
    }

    // ── parse_inspect (pure status parser) ──────────────────────────────

    #[test]
    fn parse_inspect_running_with_ip() {
        let s = PodmanBackend::parse_inspect("cid", "true|0|10.88.0.4").unwrap();
        assert!(s.running);
        assert_eq!(s.exit_code, Some(0));
        assert_eq!(s.pod_ip.as_deref(), Some("10.88.0.4"));
        assert_eq!(s.container_id, "cid");
    }

    #[test]
    fn parse_inspect_terminated_no_ip() {
        let s = PodmanBackend::parse_inspect("cid", "false|137|").unwrap();
        assert!(!s.running);
        assert_eq!(s.exit_code, Some(137));
        assert!(s.pod_ip.is_none());
    }

    #[test]
    fn parse_inspect_trims_trailing_newline() {
        let s = PodmanBackend::parse_inspect("cid", "true|0|10.88.0.4\n").unwrap();
        assert!(s.running);
        assert_eq!(s.pod_ip.as_deref(), Some("10.88.0.4"));
    }

    #[test]
    fn parse_inspect_rejects_short_shape() {
        let err = PodmanBackend::parse_inspect("cid", "true|0").unwrap_err();
        assert_eq!(err.kind(), "backend");
    }

    // ── M0.3 per-network IP regression guards ──────────────────────────
    //
    // The new inspect template ranges over .NetworkSettings.Networks and
    // emits each network's IPAddress (no separator) as the third pipe
    // field. With exactly one engenho-net membership, that's a single
    // dotted-quad. parse_inspect's split('|') + parts[2] logic is
    // unchanged; these tests pin the two ends of the fix.

    #[test]
    fn parse_inspect_per_network_ip_is_surfaced() {
        // The fixed behavior: a non-empty per-network IP → pod_ip Some.
        let s = PodmanBackend::parse_inspect("cid", "true|0|10.89.0.4").unwrap();
        assert!(s.running);
        assert_eq!(s.exit_code, Some(0));
        assert_eq!(s.pod_ip.as_deref(), Some("10.89.0.4"));
    }

    #[test]
    fn parse_inspect_empty_per_network_ip_is_none() {
        // Documents the OLD broken behavior that GAP A produced: the
        // legacy top-level field was empty → third field empty → None →
        // status.podIP never written → Endpoints stayed empty. With the
        // fixed template a netavark container yields a real IP (test
        // above); this proves an empty third field still maps to None so
        // an unbound / not-yet-networked container is honestly reported.
        let s = PodmanBackend::parse_inspect("cid", "true|0|").unwrap();
        assert!(s.running);
        assert!(
            s.pod_ip.is_none(),
            "empty per-network IP must yield pod_ip None"
        );
    }
}
