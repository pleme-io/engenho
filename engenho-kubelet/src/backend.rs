//! Pluggable container runtime backends.
//!
//! The substrate's `ContainerRuntime` trait is THE seam between
//! kubelet (substrate-side) + the host's actual container runner
//! (podman, containerd via CRI, runwasi for wasm). Backend impls
//! receive a typed `ContainerSpec` + return a typed
//! `ContainerStatus`.

use std::collections::BTreeMap;
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

/// Real podman backend. Shells out to the `podman` CLI on the host.
/// Designed for local engenho-managed clusters running on Darwin
/// (podman machine) + Linux nodes. Per the org's NO SHELL rule,
/// this is the ONE acceptable shell-out site — invoking `podman`
/// is the *integration boundary*, not orchestration glue.
pub struct PodmanBackend {
    /// Binary name or absolute path of the podman CLI (default `podman`).
    pub binary: String,
}

impl Default for PodmanBackend {
    fn default() -> Self {
        Self {
            binary: "podman".to_string(),
        }
    }
}

impl PodmanBackend {
    /// New backend using the host's `podman` from `$PATH`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// New backend with an explicit podman binary path.
    #[must_use]
    pub fn with_binary(binary: impl Into<String>) -> Self {
        Self {
            binary: binary.into(),
        }
    }
}

#[async_trait]
impl ContainerRuntime for PodmanBackend {
    fn name(&self) -> &'static str {
        "podman"
    }

    async fn start(&self, spec: &ContainerSpec) -> Result<ContainerStatus, KubeletError> {
        let mut cmd = tokio::process::Command::new(&self.binary);
        cmd.arg("run").arg("-d").arg("--name").arg(&spec.name);
        for (k, v) in &spec.env {
            cmd.arg("-e").arg(format!("{k}={v}"));
        }
        cmd.arg(&spec.image);
        for arg in &spec.command {
            cmd.arg(arg);
        }
        let out = cmd
            .output()
            .await
            .map_err(|e| KubeletError::Backend(format!("podman run spawn: {e}")))?;
        if !out.status.success() {
            return Err(KubeletError::Backend(format!(
                "podman run failed (status {:?}): {}",
                out.status.code(),
                String::from_utf8_lossy(&out.stderr)
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
        let out = tokio::process::Command::new(&self.binary)
            .arg("inspect")
            .arg("--format")
            .arg("{{.State.Running}}|{{.State.ExitCode}}|{{.NetworkSettings.IPAddress}}")
            .arg(container_id)
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
        let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
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
        Ok(Some(ContainerStatus {
            container_id: container_id.to_string(),
            running,
            pod_ip,
            exit_code,
        }))
    }

    async fn stop(&self, container_id: &str) -> Result<(), KubeletError> {
        let out = tokio::process::Command::new(&self.binary)
            .arg("stop")
            .arg(container_id)
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
        let out = tokio::process::Command::new(&self.binary)
            .arg("rm")
            .arg("-f")
            .arg(container_id)
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
    }
}
