//! CommandRunner — typed shell-out boundary.
//!
//! Per the org's NO SHELL rule, every "shell out to a CLI"
//! integration must go through ONE typed boundary. Renderers that
//! need to invoke skopeo / qemu-img / helm / wasm-tools / `nix
//! build` etc. take an `Arc<dyn CommandRunner>` and dispatch
//! through it.
//!
//! Backends:
//!   * `FakeCommandRunner` — records every invocation; returns
//!     pinned outputs per program + args. Tests + bootstrap.
//!   * `RealCommandRunner` — production: spawns the process via
//!     `tokio::process::Command`. Only acceptable shell-out site
//!     in the substrate.
//!
//! The trait is one method; that's the entire shell boundary.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::Mutex;

/// One invocation request — typed program + argv + optional stdin.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandRequest {
    /// Executable name or absolute path (e.g. "skopeo", "/usr/bin/qemu-img").
    pub program: String,
    /// Arguments (each in their own slot — no shell parsing).
    pub args: Vec<String>,
    /// Optional stdin bytes.
    pub stdin: Option<Vec<u8>>,
    /// Optional working directory.
    pub working_dir: Option<std::path::PathBuf>,
    /// Optional environment additions (merged onto the inherited env).
    pub env: BTreeMap<String, String>,
}

impl CommandRequest {
    /// Minimal constructor: program + args.
    #[must_use]
    pub fn new(program: impl Into<String>, args: Vec<String>) -> Self {
        Self {
            program: program.into(),
            args,
            stdin: None,
            working_dir: None,
            env: BTreeMap::new(),
        }
    }

    /// Add stdin bytes.
    #[must_use]
    pub fn with_stdin(mut self, stdin: Vec<u8>) -> Self {
        self.stdin = Some(stdin);
        self
    }

    /// Set working directory.
    #[must_use]
    pub fn with_working_dir(mut self, dir: std::path::PathBuf) -> Self {
        self.working_dir = Some(dir);
        self
    }

    /// Add an env variable.
    #[must_use]
    pub fn with_env(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.env.insert(key.into(), val.into());
        self
    }
}

/// One invocation's typed result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandResponse {
    /// Process exit code; None = killed by signal.
    pub exit_code: Option<i32>,
    /// stdout bytes.
    pub stdout: Vec<u8>,
    /// stderr bytes.
    pub stderr: Vec<u8>,
}

impl CommandResponse {
    /// True if the process exited with code 0.
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.exit_code == Some(0)
    }
}

/// CommandRunner errors.
#[derive(Debug, Clone, Error)]
pub enum CommandError {
    /// Process couldn't be spawned (binary not found / permission denied).
    #[error("spawn: {0}")]
    Spawn(String),
    /// I/O failure during the run (stdin write / stdout read).
    #[error("io: {0}")]
    Io(String),
}

crate::impl_error_kind! {
    CommandError {
        (Spawn(_)) => "spawn",
        (Io(_)) => "io",
    }
}

/// Pluggable command runner.
#[async_trait]
pub trait CommandRunner: Send + Sync {
    /// Backend identifier for telemetry.
    fn name(&self) -> &'static str;

    /// Run the command + return the typed response. Implementations
    /// MUST NOT interpret the program name through a shell — pass
    /// it as the literal argv[0].
    ///
    /// # Errors
    /// [`CommandError::Spawn`] / [`CommandError::Io`] on failure.
    async fn run(&self, req: &CommandRequest) -> Result<CommandResponse, CommandError>;
}

// =================================================================
// FakeCommandRunner — deterministic in-memory runner for tests
// =================================================================

/// Test runner. Operator pins per-(program, args) responses; any
/// non-pinned invocation returns a typed default response or fails
/// loudly per the configured mode.
#[derive(Default, Clone)]
pub struct FakeCommandRunner {
    inner: Arc<Mutex<FakeRunnerState>>,
}

#[derive(Default)]
struct FakeRunnerState {
    pinned: BTreeMap<(String, Vec<String>), CommandResponse>,
    default_response: Option<CommandResponse>,
    fail_unknown: bool,
    invocations: Vec<CommandRequest>,
}

impl FakeCommandRunner {
    /// Fresh runner — returns empty success for any non-pinned
    /// invocation by default.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Pin a typed response for a `(program, args)` match.
    pub async fn pin(
        &self,
        program: impl Into<String>,
        args: Vec<String>,
        response: CommandResponse,
    ) {
        self.inner
            .lock()
            .await
            .pinned
            .insert((program.into(), args), response);
    }

    /// Set a fallback response for non-pinned invocations.
    pub async fn set_default(&self, response: CommandResponse) {
        self.inner.lock().await.default_response = Some(response);
    }

    /// If true, non-pinned invocations return Err(Spawn) — used
    /// when a test asserts NO unexpected commands ran.
    pub async fn fail_on_unknown(&self) {
        self.inner.lock().await.fail_unknown = true;
    }

    /// Snapshot of recorded invocations.
    pub async fn invocations(&self) -> Vec<CommandRequest> {
        self.inner.lock().await.invocations.clone()
    }

    /// Count of invocations.
    pub async fn call_count(&self) -> usize {
        self.inner.lock().await.invocations.len()
    }
}

#[async_trait]
impl CommandRunner for FakeCommandRunner {
    fn name(&self) -> &'static str {
        "fake"
    }

    async fn run(&self, req: &CommandRequest) -> Result<CommandResponse, CommandError> {
        let mut state = self.inner.lock().await;
        state.invocations.push(req.clone());
        let key = (req.program.clone(), req.args.clone());
        if let Some(resp) = state.pinned.get(&key) {
            return Ok(resp.clone());
        }
        if state.fail_unknown {
            return Err(CommandError::Spawn(format!(
                "unexpected invocation: {} {:?}",
                req.program, req.args
            )));
        }
        if let Some(default) = &state.default_response {
            return Ok(default.clone());
        }
        // Default: empty success.
        Ok(CommandResponse {
            exit_code: Some(0),
            stdout: Vec::new(),
            stderr: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(program: &str, args: &[&str]) -> CommandRequest {
        CommandRequest::new(program, args.iter().map(|s| s.to_string()).collect())
    }

    #[tokio::test]
    async fn fake_runner_records_invocations() {
        let r = FakeCommandRunner::new();
        r.run(&req("skopeo", &["copy", "src", "dst"]))
            .await
            .unwrap();
        let calls = r.invocations().await;
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].program, "skopeo");
        assert_eq!(calls[0].args, vec!["copy", "src", "dst"]);
    }

    #[tokio::test]
    async fn fake_runner_returns_pinned_response() {
        let r = FakeCommandRunner::new();
        r.pin(
            "ls",
            vec!["-l".into()],
            CommandResponse {
                exit_code: Some(0),
                stdout: b"file1\nfile2\n".to_vec(),
                stderr: Vec::new(),
            },
        )
        .await;
        let resp = r.run(&req("ls", &["-l"])).await.unwrap();
        assert_eq!(resp.stdout, b"file1\nfile2\n");
        assert!(resp.is_success());
    }

    #[tokio::test]
    async fn fake_runner_default_response_used_when_no_pin() {
        let r = FakeCommandRunner::new();
        r.set_default(CommandResponse {
            exit_code: Some(1),
            stdout: Vec::new(),
            stderr: b"fallback".to_vec(),
        })
        .await;
        let resp = r.run(&req("unknown", &[])).await.unwrap();
        assert_eq!(resp.exit_code, Some(1));
        assert_eq!(resp.stderr, b"fallback");
    }

    #[tokio::test]
    async fn fake_runner_fail_on_unknown() {
        let r = FakeCommandRunner::new();
        r.fail_on_unknown().await;
        let err = r.run(&req("strict", &[])).await.unwrap_err();
        assert_eq!(err.kind(), "spawn");
    }

    #[tokio::test]
    async fn fake_runner_pinned_overrides_fail_unknown() {
        let r = FakeCommandRunner::new();
        r.fail_on_unknown().await;
        r.pin(
            "allowed",
            vec![],
            CommandResponse {
                exit_code: Some(0),
                stdout: Vec::new(),
                stderr: Vec::new(),
            },
        )
        .await;
        let resp = r.run(&req("allowed", &[])).await.unwrap();
        assert!(resp.is_success());
    }

    #[tokio::test]
    async fn fake_runner_default_empty_success() {
        let r = FakeCommandRunner::new();
        let resp = r.run(&req("anything", &[])).await.unwrap();
        assert!(resp.is_success());
        assert!(resp.stdout.is_empty());
    }

    #[tokio::test]
    async fn fake_runner_call_count_helper() {
        let r = FakeCommandRunner::new();
        for _ in 0..3 {
            r.run(&req("x", &[])).await.unwrap();
        }
        assert_eq!(r.call_count().await, 3);
    }

    #[test]
    fn request_builders_chain() {
        let r = CommandRequest::new("p", vec!["a".into()])
            .with_stdin(b"input".to_vec())
            .with_working_dir(std::path::PathBuf::from("/tmp"))
            .with_env("KEY", "value");
        assert_eq!(r.stdin.as_deref(), Some(b"input".as_ref()));
        assert_eq!(r.working_dir, Some(std::path::PathBuf::from("/tmp")));
        assert_eq!(r.env.get("KEY"), Some(&"value".to_string()));
    }

    #[test]
    fn response_is_success_only_for_exit_zero() {
        let s = CommandResponse {
            exit_code: Some(0),
            stdout: vec![],
            stderr: vec![],
        };
        let f = CommandResponse {
            exit_code: Some(1),
            stdout: vec![],
            stderr: vec![],
        };
        let killed = CommandResponse {
            exit_code: None,
            stdout: vec![],
            stderr: vec![],
        };
        assert!(s.is_success());
        assert!(!f.is_success());
        assert!(!killed.is_success());
    }

    #[test]
    fn command_error_kinds_are_stable() {
        assert_eq!(CommandError::Spawn("x".into()).kind(), "spawn");
        assert_eq!(CommandError::Io("x".into()).kind(), "io");
    }

    #[tokio::test]
    async fn fake_runner_name_is_stable() {
        let r = FakeCommandRunner::new();
        assert_eq!(r.name(), "fake");
    }
}
