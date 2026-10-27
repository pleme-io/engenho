//! The wasm backend: a pod whose `runtimeClassName` is [`WASM_RUNTIME_CLASS`]
//! runs each container as a WASI preview 2 component on `tatara-wasm`'s
//! wasmtime host, inside the kubelet's process.
//!
//! Rust is the seam only. It reads a container's facts, hands them to the
//! embedded `kidou` blue program (`blue/kidou/kidou.b`, loaded under an empty
//! posture so it reaches nothing), and applies the verdict: which component
//! runs, what it is granted (derived from the service's access declaration),
//! how a crash is supervised, how an exec probe is answered. The engine
//! enforces the grant, the memory limit and the deadline; this module streams
//! the component's output into a CRI log and reports its state.

use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use blue_lang_host::{Embedded, Entry, Program};
use serde::{Deserialize, Serialize};
use tatara_wasm::wasmtime_impl::WasmtimeEngine;
use tatara_wasm::{
    GuestStream, NetworkGrant, OutputSink, Preopen, WasiGrant, WasiPreview, WasmBoot,
    WasmCapabilities, WasmEngine, WasmExit, WasmFeatures, WasmLimits, WasmModuleSource,
    WasmProcess, WasmRuntime, WasmSpawn,
};
use tokio::sync::oneshot;

use crate::backend::{
    ContainerRuntime, ContainerSpec, ContainerStatus, ExecOutcome, LogOptions, LogRead, Readoption,
    RuntimeClassRequest,
};
use crate::cgroup::{NotEnforced, ResourceEnforcement};
use crate::cri::{ExitDisposition, RunState};
use crate::cri_log::{LogStream, RecordEncoder};
use crate::error::KubeletError;
use crate::pod_volume::BindSource;

/// The `runtimeClassName` this backend serves.
pub const WASM_RUNTIME_CLASS: &str = "wasm";

/// The `kidou` bidama, compiled in.
pub const PROGRAM: &str = include_str!("../blue/kidou/kidou.b");

const ENTRY: &str = "use(\"kidou\")\n";

/// A wasm container shares the host's network: a component that listens
/// binds a host port, and a probe reaches it on loopback, as for a native
/// container.
pub const WASM_POD_IP: &str = "127.0.0.1";

/// The facts `kidou::admit` decides on.
#[derive(Debug, Clone, Serialize)]
pub struct PodFacts {
    /// The container's image reference.
    pub image: String,
    /// The pod's annotations.
    pub annotations: BTreeMap<String, String>,
    /// The container's declared ports.
    pub ports: Vec<u16>,
    /// The container's resolved mounts.
    pub mounts: Vec<MountFact>,
    /// `resources.limits.memory`, in bytes.
    pub memory_limit_bytes: Option<i64>,
    /// `spec.activeDeadlineSeconds`.
    pub active_deadline_seconds: Option<i64>,
    /// `spec.restartPolicy`.
    pub restart_policy: String,
    /// Whether this node runs components from `file:` paths.
    pub local_components: bool,
}

/// One mount, as `kidou` sees it.
#[derive(Debug, Clone, Serialize)]
pub struct MountFact {
    /// The path inside the container.
    pub path: String,
    /// The host directory behind it, when it has one.
    pub host_path: Option<String>,
    /// Whether the pod mounts it read-only.
    pub read_only: bool,
}

impl PodFacts {
    /// The facts of one container of a wasm pod.
    #[must_use]
    pub fn of(spec: &ContainerSpec, request: &RuntimeClassRequest, local_components: bool) -> Self {
        Self {
            image: spec.image.clone(),
            annotations: request.annotations.clone(),
            ports: request.ports.clone(),
            mounts: spec
                .mounts
                .iter()
                .map(|m| MountFact {
                    path: m.mount_path.clone(),
                    host_path: match m.source.bind_source() {
                        BindSource::Path(p) => Some(p.display().to_string()),
                        BindSource::Volume(_) => None,
                    },
                    read_only: m.read_only,
                })
                .collect(),
            memory_limit_bytes: spec.resources.memory_limit_bytes.value(),
            active_deadline_seconds: request.active_deadline_seconds,
            restart_policy: request.restart_policy.clone(),
            local_components,
        }
    }
}

/// `kidou::admit`'s verdict.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Admission {
    /// Why the container is refused, when it is.
    #[serde(default)]
    pub refused: Option<String>,
    /// The component file to load.
    #[serde(default)]
    pub component: Option<String>,
    /// What the component is granted.
    #[serde(default)]
    pub grant: Option<Grant>,
    /// Its bounds.
    #[serde(default)]
    pub limits: Option<Limits>,
    /// The port a proxy component is served on.
    #[serde(default)]
    pub serve_http: Option<u16>,
    /// What the pod declared and the grant does not carry, with the reason.
    #[serde(default)]
    pub withheld: Vec<String>,
}

/// The grant part of an [`Admission`].
#[derive(Debug, Clone, Deserialize)]
pub struct Grant {
    /// Stream stdout to the pod log.
    pub stdout: bool,
    /// Stream stderr to the pod log.
    pub stderr: bool,
    /// Pass the container's environment.
    pub env: bool,
    /// The mounts the component sees.
    pub preopens: Vec<Preopen>,
    /// The sockets it may use.
    pub network: NetworkGrant,
}

/// The limits part of an [`Admission`].
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Limits {
    /// Linear memory ceiling.
    #[serde(default)]
    pub memory_bytes: Option<u64>,
    /// Run deadline.
    #[serde(default)]
    pub deadline_ms: Option<u64>,
}

/// The facts `kidou::supervise` decides on.
#[derive(Debug, Clone, Serialize)]
pub struct ExitFacts<'a> {
    /// How the component ended.
    pub exit: &'a WasmExit,
    /// In-place restarts so far.
    pub restarts: u32,
    /// `spec.restartPolicy`.
    pub restart_policy: &'a str,
}

/// `kidou::supervise`'s verdict.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Supervision {
    /// What to do.
    pub action: Action,
    /// The wait before an in-place restart.
    #[serde(default)]
    pub delay_ms: Option<u64>,
    /// How the exit is reported to the kubelet.
    pub disposition: Disposition,
    /// One line for the pod log.
    pub note: String,
}

/// What the backend does when a component ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    /// Start it again in place.
    Restart,
    /// Report the exit to the kubelet.
    Surface,
}

/// An exit as the kubelet reads it.
#[derive(Debug, Clone, Copy, Deserialize)]
pub struct Disposition {
    /// An exit code.
    #[serde(default)]
    pub code: Option<i32>,
    /// A signal number.
    #[serde(default)]
    pub signal: Option<i32>,
}

impl Disposition {
    /// The kubelet's form. A verdict naming neither reads as a failure.
    #[must_use]
    pub fn exit(self) -> ExitDisposition {
        match (self.signal, self.code) {
            (Some(signal), _) => ExitDisposition::Signal(signal),
            (None, Some(code)) => ExitDisposition::Code(code),
            (None, None) => ExitDisposition::Code(1),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct ProbeFacts<'a> {
    argv: &'a [String],
    running: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProbeAnswer {
    exit_code: i32,
    #[serde(default)]
    stdout: String,
    #[serde(default)]
    stderr: String,
}

enum Call {
    Admit(String),
    Supervise(String),
    Probe(String),
}

struct Job {
    call: Call,
    reply: oneshot::Sender<Result<String, String>>,
}

/// The `kidou` program, running on its own thread: a loaded blue program is
/// not `Send`, so calls reach it over a channel.
#[derive(Clone)]
pub struct Kidou {
    jobs: std::sync::mpsc::Sender<Job>,
}

impl Kidou {
    /// Load the program under an empty posture.
    ///
    /// # Errors
    /// The load error, as text, when the program does not load.
    pub fn start() -> Result<Self, String> {
        let (jobs, inbox) = std::sync::mpsc::channel::<Job>();
        let (ready, loaded) = std::sync::mpsc::channel::<Result<(), String>>();
        std::thread::Builder::new()
            .name("kidou".to_string())
            .spawn(move || {
                let loader = Embedded {
                    name: "kidou",
                    files: &[("kidou/kidou.b", PROGRAM)],
                };
                let mut host = ();
                let loaded = Program::builder(Entry::anonymous(ENTRY))
                    .loader(&loader)
                    .posture(&[])
                    .load(&mut host);
                let mut program = match loaded {
                    Ok(p) => p,
                    Err(e) => {
                        let _ = ready.send(Err(e.to_string()));
                        return;
                    }
                };
                let _ = ready.send(Ok(()));
                for job in inbox {
                    let (word, facts) = match job.call {
                        Call::Admit(f) => ("kidou::admit", f),
                        Call::Supervise(f) => ("kidou::supervise", f),
                        Call::Probe(f) => ("kidou::probe", f),
                    };
                    let answer = program
                        .call::<String>(word, (facts,), &mut host)
                        .map_err(|e| e.to_string());
                    let _ = job.reply.send(answer);
                }
            })
            .map_err(|e| e.to_string())?;
        loaded
            .recv()
            .map_err(|_| "the kidou thread ended before it loaded".to_string())??;
        Ok(Self { jobs })
    }

    async fn ask<T: for<'de> Deserialize<'de>>(&self, call: Call) -> Result<T, String> {
        let (reply, answer) = oneshot::channel();
        self.jobs
            .send(Job { call, reply })
            .map_err(|_| "the kidou thread stopped".to_string())?;
        let text = answer
            .await
            .map_err(|_| "the kidou thread stopped".to_string())??;
        serde_json::from_str(&text).map_err(|e| format!("kidou answered {text}: {e}"))
    }

    /// `kidou::admit`.
    ///
    /// # Errors
    /// The program's error, or an answer that does not read.
    pub async fn admit(&self, facts: &PodFacts) -> Result<Admission, String> {
        let facts = serde_json::to_string(facts).map_err(|e| e.to_string())?;
        self.ask(Call::Admit(facts)).await
    }

    /// `kidou::supervise`.
    ///
    /// # Errors
    /// The program's error, or an answer that does not read.
    pub async fn supervise(&self, facts: &ExitFacts<'_>) -> Result<Supervision, String> {
        let facts = serde_json::to_string(facts).map_err(|e| e.to_string())?;
        self.ask(Call::Supervise(facts)).await
    }

    async fn probe(&self, argv: &[String], running: bool) -> Result<ExecOutcome, String> {
        let facts =
            serde_json::to_string(&ProbeFacts { argv, running }).map_err(|e| e.to_string())?;
        let answer: ProbeAnswer = self.ask(Call::Probe(facts)).await?;
        Ok(ExecOutcome {
            exit_code: answer.exit_code,
            stdout: answer.stdout,
            stderr: answer.stderr,
        })
    }
}

/// A component's CRI log: both output streams framed into records, plus the
/// backend's own notes on stderr.
struct PodLog {
    path: PathBuf,
    open: Mutex<(std::fs::File, RecordEncoder, RecordEncoder)>,
}

impl PodLog {
    fn create(path: PathBuf) -> std::io::Result<Self> {
        let file = std::fs::File::create(&path)?;
        Ok(Self {
            path,
            open: Mutex::new((
                file,
                RecordEncoder::new(LogStream::Stdout),
                RecordEncoder::new(LogStream::Stderr),
            )),
        })
    }

    fn note(&self, text: &str) {
        self.write(
            GuestStream::Stderr,
            ["wasm: ", text, "\n"].concat().as_bytes(),
        );
    }
}

impl OutputSink for PodLog {
    fn write(&self, stream: GuestStream, bytes: &[u8]) {
        let Ok(mut open) = self.open.lock() else {
            return;
        };
        let (file, stdout, stderr) = &mut *open;
        let encoder = match stream {
            GuestStream::Stdout => stdout,
            GuestStream::Stderr => stderr,
        };
        let mut out = Vec::new();
        encoder.push(bytes, SystemTime::now(), &mut out);
        if let Err(e) = file.write_all(&out) {
            tracing::warn!(error = %e, path = %self.path.display(), "wasm container log write failed");
        }
    }

    fn finish(&self) {
        let Ok(mut open) = self.open.lock() else {
            return;
        };
        let (file, stdout, stderr) = &mut *open;
        let mut out = Vec::new();
        stdout.finish(SystemTime::now(), &mut out);
        stderr.finish(SystemTime::now(), &mut out);
        let _ = file.write_all(&out);
    }
}

/// One container's run, shared with its supervisor.
struct Run {
    process: WasmProcess,
    restarts: u32,
    stopping: bool,
    surfaced: Option<ExitDisposition>,
}

struct WasmContainer {
    log: Arc<PodLog>,
    run: Arc<Mutex<Run>>,
    supervisor: tokio::task::JoinHandle<()>,
}

/// Runs wasm pods' containers as components. See the module docs.
pub struct WasmBackend {
    log_dir: PathBuf,
    local_components: bool,
    engine: Arc<WasmtimeEngine>,
    kidou: OnceLock<Result<Kidou, String>>,
    state: Mutex<HashMap<String, WasmContainer>>,
}

impl WasmBackend {
    /// A backend writing container logs under `log_dir`. Components load
    /// only from nix closures unless `local_components` is set.
    #[must_use]
    pub fn new(log_dir: impl Into<PathBuf>, local_components: bool) -> Self {
        Self {
            log_dir: log_dir.into(),
            local_components,
            engine: Arc::new(WasmtimeEngine::default()),
            kidou: OnceLock::new(),
            state: Mutex::new(HashMap::new()),
        }
    }

    fn kidou(&self) -> Result<Kidou, KubeletError> {
        self.kidou.get_or_init(Kidou::start).clone().map_err(|e| {
            KubeletError::Backend(format!("wasm: the kidou program did not load: {e}"))
        })
    }

    fn poisoned() -> KubeletError {
        KubeletError::Backend("wasm: container table lock poisoned".to_string())
    }

    fn spawn_of(
        spec: &ContainerSpec,
        id: &str,
        admission: &Admission,
    ) -> Result<WasmSpawn, String> {
        let component = admission
            .component
            .clone()
            .ok_or("kidou admitted the container and named no component")?;
        let grant = admission
            .grant
            .clone()
            .ok_or("kidou admitted the container and stated no grant")?;
        let limits = admission.limits.unwrap_or(Limits {
            memory_bytes: None,
            deadline_ms: None,
        });
        let mut argv = vec![spec.pod.container_name.clone()];
        argv.extend(spec.argv());
        let env = if grant.env {
            spec.env
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        } else {
            Vec::new()
        };
        Ok(WasmSpawn {
            boot: WasmBoot {
                module: WasmModuleSource::Path(PathBuf::from(component)),
                runtime: WasmRuntime::Wasmtime,
                preview: WasiPreview::P2,
                features: WasmFeatures::default(),
                capabilities: WasmCapabilities {
                    wasi: Some(WasiGrant {
                        stdout: grant.stdout,
                        stderr: grant.stderr,
                        env,
                        argv,
                        preopens: grant.preopens,
                        network: grant.network,
                    }),
                    ..WasmCapabilities::default()
                },
                name: id.to_string(),
            },
            limits: WasmLimits {
                memory_bytes: limits.memory_bytes,
                deadline: limits.deadline_ms.map(Duration::from_millis),
            },
            serve_http: admission.serve_http,
        })
    }

    fn read_from(path: &Path, offset: u64) -> std::io::Result<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = std::fs::File::open(path)?;
        let len = file.metadata()?.len();
        file.seek(SeekFrom::Start(offset.min(len)))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(bytes)
    }
}

async fn supervise(
    engine: Arc<WasmtimeEngine>,
    kidou: Kidou,
    spawn: WasmSpawn,
    log: Arc<PodLog>,
    run: Arc<Mutex<Run>>,
    policy: String,
) {
    loop {
        let Some(mut process) = run.lock().ok().map(|r| r.process.clone()) else {
            return;
        };
        let ended = process.wait().await;
        let Some((stopping, restarts)) = run.lock().ok().map(|r| (r.stopping, r.restarts)) else {
            return;
        };
        let exit = if stopping { WasmExit::Stopped } else { ended };
        let facts = ExitFacts {
            exit: &exit,
            restarts,
            restart_policy: &policy,
        };
        let verdict = match kidou.supervise(&facts).await {
            Ok(v) => v,
            Err(e) => {
                log.note(&["no supervision verdict, surfacing the exit: ", &e].concat());
                Supervision {
                    action: Action::Surface,
                    delay_ms: None,
                    disposition: Disposition {
                        code: Some(1),
                        signal: None,
                    },
                    note: String::new(),
                }
            }
        };
        if !verdict.note.is_empty() {
            log.note(&verdict.note);
        }
        let surfaced = verdict.disposition.exit();
        if verdict.action == Action::Restart && !stopping {
            tokio::time::sleep(Duration::from_millis(verdict.delay_ms.unwrap_or(0))).await;
            let Ok(mut r) = run.lock() else {
                return;
            };
            if r.stopping {
                r.surfaced = Some(ExitDisposition::Signal(15));
                return;
            }
            match engine.spawn(&spawn, Arc::clone(&log) as Arc<dyn OutputSink>) {
                Ok(next) => {
                    r.process = next;
                    r.restarts += 1;
                    continue;
                }
                Err(e) => {
                    drop(r);
                    log.note(&["the restart did not start: ", &e.to_string()].concat());
                }
            }
        }
        if let Ok(mut r) = run.lock() {
            r.surfaced = Some(surfaced);
        }
        return;
    }
}

#[async_trait]
impl ContainerRuntime for WasmBackend {
    fn name(&self) -> &'static str {
        "wasm"
    }

    fn version(&self) -> Option<String> {
        Some(env!("CARGO_PKG_VERSION").to_owned())
    }

    /// A component runs inside the kubelet's process; a new kubelet process
    /// starts with an empty table.
    fn readoption(&self) -> Readoption {
        Readoption::Cannot
    }

    /// Memory is bounded by the engine; CPU is not bounded at all.
    fn resource_enforcement(&self) -> ResourceEnforcement {
        ResourceEnforcement::NotEnforced(NotEnforced::PlatformUnsupported)
    }

    async fn exec(&self, container_id: &str, argv: &[String]) -> Result<ExecOutcome, KubeletError> {
        let running = {
            let state = self.state.lock().map_err(|_| Self::poisoned())?;
            let c = state.get(container_id).ok_or_else(|| {
                KubeletError::Backend(format!("wasm: no container {container_id}"))
            })?;
            let run = c.run.lock().map_err(|_| Self::poisoned())?;
            run.surfaced.is_none()
        };
        self.kidou()?
            .probe(argv, running)
            .await
            .map_err(|e| KubeletError::Backend(format!("wasm: probe verdict: {e}")))
    }

    #[allow(clippy::too_many_lines)]
    async fn start(&self, spec: &ContainerSpec) -> Result<ContainerStatus, KubeletError> {
        let pod = format!("{}/{}", spec.pod.namespace, spec.pod.name);
        let request = spec
            .runtime_class
            .clone()
            .unwrap_or_else(|| RuntimeClassRequest {
                name: WASM_RUNTIME_CLASS.to_string(),
                restart_policy: "Always".to_string(),
                ..RuntimeClassRequest::default()
            });
        let unparseable = spec.resources.unparseable();
        if !unparseable.is_empty() {
            return Err(KubeletError::InvalidPod {
                pod,
                reason: format!(
                    "container {}: resources {unparseable:?} do not parse; refusing rather than running unbounded",
                    spec.pod.container_name
                ),
            });
        }
        let kidou = self.kidou()?;
        let admission = kidou
            .admit(&PodFacts::of(spec, &request, self.local_components))
            .await
            .map_err(|e| KubeletError::Backend(format!("wasm: admission verdict: {e}")))?;
        if let Some(reason) = admission.refused.clone() {
            return Err(KubeletError::InvalidPod {
                pod,
                reason: format!("container {}: {reason}", spec.pod.container_name),
            });
        }
        let id = spec.name.clone();
        let spawn = Self::spawn_of(spec, &id, &admission)
            .map_err(|e| KubeletError::Backend(format!("wasm: {e}")))?;

        let mut state = self.state.lock().map_err(|_| Self::poisoned())?;
        if let Some(previous) = state.get(&id) {
            if !previous.supervisor.is_finished() {
                return Err(KubeletError::NotReaped { container_id: id });
            }
        }
        std::fs::create_dir_all(&self.log_dir).map_err(|e| {
            KubeletError::Backend(format!("wasm: log dir {}: {e}", self.log_dir.display()))
        })?;
        let log = Arc::new(
            PodLog::create(self.log_dir.join(&id))
                .map_err(|e| KubeletError::Backend(format!("wasm: log for {id}: {e}")))?,
        );
        for withheld in &admission.withheld {
            log.note(&["withheld: ", withheld].concat());
        }
        let process = self
            .engine
            .spawn(&spawn, Arc::clone(&log) as Arc<dyn OutputSink>)
            .map_err(|e| KubeletError::Backend(format!("wasm: {e}")))?;
        let run = Arc::new(Mutex::new(Run {
            process,
            restarts: 0,
            stopping: false,
            surfaced: None,
        }));
        let supervisor = tokio::spawn(supervise(
            Arc::clone(&self.engine),
            kidou,
            spawn,
            Arc::clone(&log),
            Arc::clone(&run),
            request.restart_policy,
        ));
        state.insert(
            id.clone(),
            WasmContainer {
                log,
                run,
                supervisor,
            },
        );
        Ok(ContainerStatus {
            container_id: id,
            state: RunState::Running,
            pod_ip: Some(WASM_POD_IP.to_string()),
        })
    }

    async fn status(&self, container_id: &str) -> Result<Option<ContainerStatus>, KubeletError> {
        let state = self.state.lock().map_err(|_| Self::poisoned())?;
        let Some(c) = state.get(container_id) else {
            return Ok(None);
        };
        let run = c.run.lock().map_err(|_| Self::poisoned())?;
        Ok(Some(ContainerStatus {
            container_id: container_id.to_string(),
            state: run.surfaced.map_or(RunState::Running, RunState::Exited),
            pod_ip: Some(WASM_POD_IP.to_string()),
        }))
    }

    /// The component is interrupted at its next epoch tick; a component has
    /// no SIGTERM to handle, so the grace period does not apply.
    async fn stop(&self, container_id: &str) -> Result<(), KubeletError> {
        let state = self.state.lock().map_err(|_| Self::poisoned())?;
        if let Some(c) = state.get(container_id) {
            let mut run = c.run.lock().map_err(|_| Self::poisoned())?;
            run.stopping = true;
            run.process.stop();
        }
        Ok(())
    }

    async fn remove(&self, container_id: &str) -> Result<(), KubeletError> {
        let mut state = self.state.lock().map_err(|_| Self::poisoned())?;
        if let Some(c) = state.get(container_id) {
            if !c.supervisor.is_finished() {
                return Err(KubeletError::NotReaped {
                    container_id: container_id.to_string(),
                });
            }
        }
        state.remove(container_id);
        Ok(())
    }

    async fn logs(&self, container_id: &str, opts: &LogOptions) -> Result<String, KubeletError> {
        Ok(self.read_log(container_id, opts).await?.text)
    }

    async fn read_log(
        &self,
        container_id: &str,
        opts: &LogOptions,
    ) -> Result<LogRead, KubeletError> {
        let (path, ended) = {
            let state = self.state.lock().map_err(|_| Self::poisoned())?;
            let c = state.get(container_id).ok_or_else(|| {
                KubeletError::Backend(format!("wasm: no container {container_id}"))
            })?;
            (c.log.path.clone(), c.supervisor.is_finished())
        };
        let from = opts.from_byte.unwrap_or(0);
        let bytes = Self::read_from(&path, from)
            .map_err(|e| KubeletError::Backend(format!("wasm: log {}: {e}", path.display())))?;
        let rendered = crate::cri_log::render(&bytes, opts.render(), ended);
        Ok(LogRead {
            text: rendered.text,
            cursor: from + u64::try_from(rendered.consumed).unwrap_or(u64::MAX),
        })
    }
}
