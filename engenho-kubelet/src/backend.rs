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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerSpec {
    /// Logical container name (namespace/podname/container).
    pub name: String,
    /// Container image (e.g. "stefanprodan/podinfo:6.5.4").
    pub image: String,
    /// Environment variables.
    pub env: BTreeMap<String, String>,
    /// Command to run inside the container; empty = image default.
    pub command: Vec<String>,
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
        state
            .events
            .push(FakeEvent::Remove(container_id.to_string()));
        Ok(())
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
/// [`registry_auth_file`]: PodmanBackend::registry_auth_file
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
}

impl Default for PodmanBackend {
    fn default() -> Self {
        Self {
            binary: "podman".to_string(),
            pull_policy: PullPolicy::Never,
            registry_auth_file: None,
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

    // ── Pure typed argv builders (unit-testable without podman) ─────────
    //
    // Each builder emits the argv AFTER the binary, so a test can assert
    // the exact ContainerSpec → podman mapping without spawning a process.
    // The async methods below construct their `Command` from these vecs.

    /// `podman run` argv for `spec`, honoring `pull_policy`:
    /// `["run","-d","--pull",<policy>,"--name",<name>, (-e k=v)*, <image>, (cmd)*]`.
    #[must_use]
    pub fn run_argv(&self, spec: &ContainerSpec) -> Vec<String> {
        let mut argv = vec![
            "run".to_string(),
            "-d".to_string(),
            "--pull".to_string(),
            self.pull_policy.flag_value().to_string(),
            "--name".to_string(),
            spec.name.clone(),
        ];
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
    /// [`parse_inspect`]: PodmanBackend::parse_inspect
    #[must_use]
    pub fn inspect_argv(id: &str) -> Vec<String> {
        vec![
            "inspect".to_string(),
            "--format".to_string(),
            "{{.State.Running}}|{{.State.ExitCode}}|{{.NetworkSettings.IPAddress}}".to_string(),
            id.to_string(),
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
}

#[async_trait]
impl ContainerRuntime for PodmanBackend {
    fn name(&self) -> &'static str {
        "podman"
    }

    async fn start(&self, spec: &ContainerSpec) -> Result<ContainerStatus, KubeletError> {
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
        Ok(ContainerStatus {
            container_id,
            running: true,
            pod_ip: None, // podman inspect needed; deferred to status()
            exit_code: None,
        })
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
        // ["run","-d","--pull","never","--name","c","img","echo","hi"]
        assert_eq!(
            argv,
            vec!["run", "-d", "--pull", "never", "--name", "c", "img", "echo", "hi"]
        );
    }

    #[test]
    fn run_argv_empty_command_emits_no_trailing_args() {
        let backend = PodmanBackend::new();
        let s = spec("c", "img", &[], &[]);
        assert_eq!(
            backend.run_argv(&s),
            vec!["run", "-d", "--pull", "never", "--name", "c", "img"]
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
                "run", "-d", "--pull", "never", "--name", "c", //
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

    // ── inspect / stop / rm argv ────────────────────────────────────────

    #[test]
    fn inspect_argv_uses_exact_pipe_template() {
        assert_eq!(
            PodmanBackend::inspect_argv("abc123"),
            vec![
                "inspect",
                "--format",
                "{{.State.Running}}|{{.State.ExitCode}}|{{.NetworkSettings.IPAddress}}",
                "abc123",
            ]
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
}
