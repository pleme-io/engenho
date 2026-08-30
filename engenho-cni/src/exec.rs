//! Plugin invocation — the exec half of the CNI contract.
//!
//! ★ THE PLATFORM SPLIT IS TYPED, NOT FAKED. On darwin there is no network
//! namespace to hand a plugin: `CNI_NETNS` cannot be satisfied, so no
//! conformant plugin can run. That is a fact about the world, not about our
//! abstractions, so it gets a type — [`CniInstall`] — exactly as
//! `DatapathInstall` does for kube-proxy. `Planned` means the chain was
//! read, the invocation computed and the pod IP came from the container
//! runtime instead; `Invoked` means a plugin actually ran and the IP is its
//! result. No `kubectl` command can otherwise tell those apart.
//!
//! ★ THE PLANNING IS PURE AND THE EXEC IS BEHIND A SEAM. [`plan`] turns a
//! config plus a container into an ordered list of [`CniInvocation`]s with
//! no side effects at all, which is what makes the whole contract testable
//! on a machine where it cannot run. [`CniEnv`] is the `Environment` trait
//! the actual fork/exec hides behind — the same discipline `ProvisionerEnv`
//! uses in the PV binder.
//!
//! ★ `DEL` REVERSES THE CHAIN. Not a detail: tearing down in ADD order
//! removes the interface `portmap` maps before `portmap` has removed its
//! rules, leaving stale NAT entries pointing at an address that is about to
//! be reassigned to a different pod.
//!
//! ★ NO SHELL. The plugin is invoked as a typed argv + environment, never
//! through `sh -c`. Exec'ing the plugin binary IS the contract — the spec
//! defines the interface as an executable invocation — but a shell between
//! us and it would add word-splitting to a path we do not control.

use std::collections::BTreeMap;
use std::path::PathBuf;

use async_trait::async_trait;
use serde_json::Value;

use crate::config::{NetworkConfigList, PluginConfig};
use crate::result::{CniError, CniResult};

/// Whether a plugin chain was actually executed.
///
/// The direct analogue of `DatapathInstall` for kube-proxy, and it exists
/// for the same reason: a computed-but-not-executed result is
/// indistinguishable from a real one at every observation point except this
/// field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CniInstall {
    /// Configuration parsed, chain resolved, invocation computed — and NO
    /// plugin was executed. The pod IP came from the container runtime
    /// instead. The honest state on a host with no network namespace to
    /// hand a plugin.
    Planned,
    /// The plugin chain ran; the pod IP is the chain's result.
    Invoked,
}

impl CniInstall {
    /// The string an operator sees on the Node object.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "Planned",
            Self::Invoked => "Invoked",
        }
    }
}

/// The CNI operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CniCommand {
    /// Attach the sandbox to the network.
    Add,
    /// Detach it.
    Del,
    /// Verify the attachment still holds.
    Check,
}

impl CniCommand {
    /// The `CNI_COMMAND` value.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Add => "ADD",
            Self::Del => "DEL",
            Self::Check => "CHECK",
        }
    }
}

/// One fully-computed plugin invocation: what to exec, with what
/// environment, and what to write to its stdin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CniInvocation {
    /// The plugin binary's name, to be found in one of [`Self::search_path`].
    pub plugin_type: String,
    /// Directories to search for the binary (`CNI_PATH`).
    pub search_path: Vec<PathBuf>,
    /// The `CNI_*` environment, complete.
    pub env: BTreeMap<String, String>,
    /// The JSON written to the plugin's stdin.
    pub stdin: Value,
}

impl CniInvocation {
    /// The `CNI_COMMAND` this invocation carries.
    #[must_use]
    pub fn command(&self) -> Option<&str> {
        self.env.get("CNI_COMMAND").map(String::as_str)
    }
}

/// A planned chain: every invocation, in execution order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedCni {
    /// The network's name.
    pub network: String,
    /// The invocations, already ordered for the command (reversed for
    /// `DEL`).
    pub invocations: Vec<CniInvocation>,
}

/// What the runtime knows about the sandbox being attached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sandbox {
    /// `CNI_CONTAINERID`.
    pub container_id: String,
    /// `CNI_NETNS` — the network namespace path.
    ///
    /// ★ EMPTY IS MEANINGFUL AND IS NOT AN ERROR HERE. On `DEL` the spec
    /// explicitly permits an empty netns (the sandbox may already be gone),
    /// and on darwin there is no netns at all. Planning still succeeds; it
    /// is [`CniEnv`] that refuses to execute, which is what keeps the pure
    /// half testable everywhere.
    pub netns: String,
    /// `CNI_IFNAME`, conventionally `eth0`.
    pub ifname: String,
    /// Extra `CNI_ARGS` key=value pairs. The kubelet passes
    /// `K8S_POD_NAMESPACE`, `K8S_POD_NAME` and `K8S_POD_INFRA_CONTAINER_ID`
    /// here, and plugins like Calico key their per-pod policy on them —
    /// omitting them yields a pod with no policy and no error.
    pub args: BTreeMap<String, String>,
}

/// Compute the plugin chain for one command. Pure.
///
/// # Panics
/// Never.
#[must_use]
pub fn plan(
    config: &NetworkConfigList,
    sandbox: &Sandbox,
    command: CniCommand,
    search_path: &[PathBuf],
) -> PlannedCni {
    let mut plugins: Vec<&PluginConfig> = config.plugins.iter().collect();
    if command == CniCommand::Del {
        // Reversed, or portmap's NAT rules outlive the interface they map
        // and point at an address about to be reassigned to another pod.
        plugins.reverse();
    }

    let cni_args = sandbox
        .args
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(";");
    let path = search_path
        .iter()
        .map(|p| p.display().to_string())
        .collect::<Vec<_>>()
        .join(":");

    let invocations = plugins
        .into_iter()
        .map(|p| {
            let mut env = BTreeMap::new();
            env.insert("CNI_COMMAND".into(), command.as_str().to_string());
            env.insert("CNI_CONTAINERID".into(), sandbox.container_id.clone());
            env.insert("CNI_NETNS".into(), sandbox.netns.clone());
            env.insert("CNI_IFNAME".into(), sandbox.ifname.clone());
            env.insert("CNI_PATH".into(), path.clone());
            if !cni_args.is_empty() {
                env.insert("CNI_ARGS".into(), cni_args.clone());
            }

            // The stdin document is the plugin's own body PLUS the
            // envelope keys every plugin reads. A plugin that cannot see
            // `name` cannot key its state, and one that cannot see
            // `cniVersion` does not know which result shape to write.
            let mut stdin = serde_json::Map::new();
            stdin.insert(
                "cniVersion".into(),
                Value::String(config.cni_version.clone()),
            );
            stdin.insert("name".into(), Value::String(config.name.clone()));
            stdin.insert("type".into(), Value::String(p.plugin_type.clone()));
            for (k, v) in &p.body {
                stdin.insert(k.clone(), v.clone());
            }

            CniInvocation {
                plugin_type: p.plugin_type.clone(),
                search_path: search_path.to_vec(),
                env,
                stdin: Value::Object(stdin),
            }
        })
        .collect();

    PlannedCni {
        network: config.name.clone(),
        invocations,
    }
}

/// Inject the previous plugin's result as `prevResult`.
///
/// ★ THIS IS WHAT MAKES A CHAIN A CHAIN. `portmap` needs the interface
/// `bridge` created; without `prevResult` it has no idea what to map and
/// either fails or silently maps nothing.
pub fn with_prev_result(invocation: &mut CniInvocation, prev: &CniResult) {
    if let (Some(obj), Ok(v)) = (invocation.stdin.as_object_mut(), serde_json::to_value(prev)) {
        obj.insert("prevResult".into(), v);
    }
}

/// Errors executing a plugin.
#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    /// This host cannot execute CNI plugins at all.
    #[error(
        "this host cannot invoke CNI plugins: {reason}. The chain was PLANNED, not \
         invoked — the pod address came from the container runtime instead"
    )]
    NotInvocable {
        /// Why, in terms an operator can act on.
        reason: String,
    },
    /// The plugin binary was not found in `CNI_PATH`.
    #[error("CNI plugin {plugin:?} not found in {searched:?}")]
    PluginNotFound {
        /// The plugin type.
        plugin: String,
        /// Where we looked.
        searched: Vec<String>,
    },
    /// The plugin could not be started.
    #[error("spawning CNI plugin {plugin}: {source}")]
    Spawn {
        /// The plugin type.
        plugin: String,
        /// The io error.
        #[source]
        source: std::io::Error,
    },
    /// The plugin ran and reported a CNI error.
    #[error("CNI plugin {plugin}: {error}")]
    Plugin {
        /// The plugin type.
        plugin: String,
        /// The plugin's own error document — `msg` and `details` included,
        /// which is the difference between "ADD failed" and "no addresses
        /// available in 10.244.1.0/24".
        error: Box<CniError>,
    },
    /// The plugin exited non-zero without a parseable error document.
    #[error("CNI plugin {plugin} exited {code}: {stderr}")]
    Unparseable {
        /// The plugin type.
        plugin: String,
        /// Exit code.
        code: i32,
        /// Whatever it wrote to stderr, which is all the diagnostic there is.
        stderr: String,
    },
}

engenho_substrate::impl_error_kind! {
    ExecError {
        { NotInvocable { .. } } => "not_invocable",
        { PluginNotFound { .. } } => "plugin_not_found",
        { Spawn { .. } } => "spawn",
        { Plugin { .. } } => "plugin",
        { Unparseable { .. } } => "unparseable",
    }
}

/// The exec seam.
#[async_trait]
pub trait CniEnv: Send + Sync {
    /// Whether this host can execute plugins at all.
    fn install(&self) -> CniInstall;

    /// Run one invocation, returning its stdout.
    ///
    /// # Errors
    /// Any [`ExecError`].
    async fn invoke(&self, invocation: &CniInvocation) -> Result<Vec<u8>, ExecError>;
}

/// The whole plugin chain, run in order, threading `prevResult`.
///
/// # Errors
/// The first plugin error, naming the plugin. A `DEL` that fails part-way
/// is still an error: silently continuing would report a clean teardown
/// over a leaked interface.
pub async fn run_chain(
    env: &dyn CniEnv,
    planned: &PlannedCni,
) -> Result<Option<CniResult>, ExecError> {
    let mut prev: Option<CniResult> = None;
    for invocation in &planned.invocations {
        let mut invocation = invocation.clone();
        if let Some(p) = &prev {
            with_prev_result(&mut invocation, p);
        }
        let stdout = env.invoke(&invocation).await?;
        // A plugin that writes nothing (legal for DEL) leaves the previous
        // result standing rather than clearing it, which is what upstream
        // does and what keeps a chain's final ADD result intact.
        if stdout.iter().any(|b| !b.is_ascii_whitespace())
            && let Ok(r) = serde_json::from_slice::<CniResult>(&stdout)
        {
            prev = Some(r);
        }
    }
    Ok(prev)
}

/// The honest darwin backend: plans everything, executes nothing.
///
/// ★ THIS IS NOT A STUB. It is the correct implementation for a host with
/// no network namespace, and it says so by name in every error. The
/// alternative — pretending to invoke and returning a synthetic result —
/// would put an address on a pod that nothing routes to.
#[derive(Debug, Clone, Copy)]
pub struct PlanningOnlyCniEnv;

#[async_trait]
impl CniEnv for PlanningOnlyCniEnv {
    fn install(&self) -> CniInstall {
        CniInstall::Planned
    }

    async fn invoke(&self, invocation: &CniInvocation) -> Result<Vec<u8>, ExecError> {
        Err(ExecError::NotInvocable {
            reason: format!(
                "no network namespace exists on this host, so CNI_NETNS cannot be \
                 satisfied for plugin {:?}",
                invocation.plugin_type
            ),
        })
    }
}

/// The real fork/exec backend.
///
/// Lives behind the same trait so the darwin arm and the Linux arm are
/// selected by construction rather than by a runtime `if cfg!(unix)`
/// scattered through the call path.
#[derive(Debug, Clone)]
pub struct ExecCniEnv {
    search_path: Vec<PathBuf>,
}

impl ExecCniEnv {
    /// New backend searching `search_path` for plugin binaries.
    #[must_use]
    pub fn new(search_path: Vec<PathBuf>) -> Self {
        Self { search_path }
    }

    /// Resolve a plugin binary.
    ///
    /// # Errors
    /// [`ExecError::PluginNotFound`] naming every directory searched — a
    /// bare "not found" for a binary the operator installed somewhere is
    /// the least actionable error there is.
    pub fn resolve(&self, plugin_type: &str) -> Result<PathBuf, ExecError> {
        for dir in &self.search_path {
            let candidate = dir.join(plugin_type);
            if candidate.is_file() {
                return Ok(candidate);
            }
        }
        Err(ExecError::PluginNotFound {
            plugin: plugin_type.to_string(),
            searched: self
                .search_path
                .iter()
                .map(|p| p.display().to_string())
                .collect(),
        })
    }
}

#[async_trait]
impl CniEnv for ExecCniEnv {
    fn install(&self) -> CniInstall {
        CniInstall::Invoked
    }

    async fn invoke(&self, invocation: &CniInvocation) -> Result<Vec<u8>, ExecError> {
        use tokio::io::AsyncWriteExt;

        let binary = self.resolve(&invocation.plugin_type)?;
        let stdin_bytes = serde_json::to_vec(&invocation.stdin).unwrap_or_default();

        // A typed Command with an explicit argv and environment. No shell:
        // the spec's interface is an executable invocation, and a shell
        // between us and it would add word-splitting to a path we do not
        // control.
        let mut cmd = tokio::process::Command::new(&binary);
        cmd.env_clear();
        for (k, v) in &invocation.env {
            cmd.env(k, v);
        }
        // A plugin may need PATH for its own helpers; passing the host's is
        // what upstream does.
        if let Ok(path) = std::env::var("PATH") {
            cmd.env("PATH", path);
        }
        cmd.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        let mut child = cmd.spawn().map_err(|source| ExecError::Spawn {
            plugin: invocation.plugin_type.clone(),
            source,
        })?;
        if let Some(mut sin) = child.stdin.take() {
            let _ = sin.write_all(&stdin_bytes).await;
            let _ = sin.shutdown().await;
        }
        let output = child
            .wait_with_output()
            .await
            .map_err(|source| ExecError::Spawn {
                plugin: invocation.plugin_type.clone(),
                source,
            })?;

        if output.status.success() {
            return Ok(output.stdout);
        }

        // A plugin signals failure by writing an error DOCUMENT to stdout
        // and exiting non-zero. Reading only the exit code loses `msg` and
        // `details`, which is the entire diagnostic.
        if let Ok(error) = serde_json::from_slice::<CniError>(&output.stdout)
            && !error.msg.is_empty()
        {
            return Err(ExecError::Plugin {
                plugin: invocation.plugin_type.clone(),
                error: Box::new(error),
            });
        }
        Err(ExecError::Unparseable {
            plugin: invocation.plugin_type.clone(),
            code: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        })
    }
}

/// The trait a caller uses to attach and detach a sandbox.
#[async_trait]
pub trait CniPlugin: Send + Sync {
    /// Attach `sandbox` to the network, returning the chain's result.
    ///
    /// # Errors
    /// Any [`ExecError`].
    async fn add(&self, sandbox: &Sandbox) -> Result<Option<CniResult>, ExecError>;

    /// Detach it.
    ///
    /// # Errors
    /// Any [`ExecError`].
    async fn del(&self, sandbox: &Sandbox) -> Result<(), ExecError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::parse_config;
    use std::path::Path;

    fn conf() -> NetworkConfigList {
        parse_config(
            Path::new("/x/10-cbr0.conflist"),
            br#"{"cniVersion":"1.0.0","name":"cbr0","plugins":[
                 {"type":"bridge","bridge":"cni0"},
                 {"type":"portmap"}]}"#,
        )
        .unwrap()
    }

    fn sandbox() -> Sandbox {
        Sandbox {
            container_id: "abc123".into(),
            netns: "/var/run/netns/cni-1".into(),
            ifname: "eth0".into(),
            args: BTreeMap::from([
                ("K8S_POD_NAMESPACE".into(), "ns".into()),
                ("K8S_POD_NAME".into(), "pod1".into()),
            ]),
        }
    }

    fn path() -> Vec<PathBuf> {
        vec![PathBuf::from("/opt/cni/bin")]
    }

    #[test]
    fn add_runs_the_chain_forward_and_del_runs_it_backward() {
        // Tearing down in ADD order removes the interface portmap maps
        // before portmap removes its rules, leaving stale NAT pointing at
        // an address about to be reassigned to a different pod.
        let add = plan(&conf(), &sandbox(), CniCommand::Add, &path());
        assert_eq!(
            add.invocations
                .iter()
                .map(|i| i.plugin_type.as_str())
                .collect::<Vec<_>>(),
            ["bridge", "portmap"]
        );
        let del = plan(&conf(), &sandbox(), CniCommand::Del, &path());
        assert_eq!(
            del.invocations
                .iter()
                .map(|i| i.plugin_type.as_str())
                .collect::<Vec<_>>(),
            ["portmap", "bridge"]
        );
    }

    #[test]
    fn every_mandatory_cni_variable_is_set() {
        let p = plan(&conf(), &sandbox(), CniCommand::Add, &path());
        let env = &p.invocations[0].env;
        assert_eq!(env["CNI_COMMAND"], "ADD");
        assert_eq!(env["CNI_CONTAINERID"], "abc123");
        assert_eq!(env["CNI_NETNS"], "/var/run/netns/cni-1");
        assert_eq!(env["CNI_IFNAME"], "eth0");
        assert_eq!(env["CNI_PATH"], "/opt/cni/bin");
    }

    #[test]
    fn the_kubernetes_args_reach_the_plugin() {
        // Calico keys its per-pod policy on these. Omitting them yields a
        // pod with no policy and no error anywhere.
        let p = plan(&conf(), &sandbox(), CniCommand::Add, &path());
        let args = &p.invocations[0].env["CNI_ARGS"];
        assert!(args.contains("K8S_POD_NAMESPACE=ns"), "{args}");
        assert!(args.contains("K8S_POD_NAME=pod1"), "{args}");
        assert!(args.contains(';'), "semicolon-separated: {args}");
    }

    #[test]
    fn the_stdin_document_carries_the_envelope_and_the_plugin_body() {
        // A plugin that cannot see `name` cannot key its state; one that
        // cannot see `cniVersion` does not know which result shape to emit.
        let p = plan(&conf(), &sandbox(), CniCommand::Add, &path());
        let stdin = &p.invocations[0].stdin;
        assert_eq!(stdin["cniVersion"], "1.0.0");
        assert_eq!(stdin["name"], "cbr0");
        assert_eq!(stdin["type"], "bridge");
        assert_eq!(stdin["bridge"], "cni0");
    }

    #[test]
    fn prev_result_is_what_makes_a_chain_a_chain() {
        // portmap needs the interface bridge created; without prevResult it
        // either fails or silently maps nothing.
        let mut p = plan(&conf(), &sandbox(), CniCommand::Add, &path());
        let prev = CniResult {
            cni_version: "1.0.0".into(),
            ips: vec![crate::result::IpConfig {
                address: "10.244.1.7/24".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        with_prev_result(&mut p.invocations[1], &prev);
        assert_eq!(
            p.invocations[1].stdin["prevResult"]["ips"][0]["address"],
            "10.244.1.7/24"
        );
        // And the first link must NOT have one.
        assert!(p.invocations[0].stdin.get("prevResult").is_none());
    }

    #[tokio::test]
    async fn the_planning_only_backend_refuses_by_name_and_says_planned() {
        // Not a stub: the correct answer for a host with no netns. The
        // alternative — a synthetic result — puts an address on a pod that
        // nothing routes to.
        let env = PlanningOnlyCniEnv;
        assert_eq!(env.install(), CniInstall::Planned);
        let p = plan(&conf(), &sandbox(), CniCommand::Add, &path());
        let e = env.invoke(&p.invocations[0]).await.unwrap_err();
        assert!(matches!(e, ExecError::NotInvocable { .. }), "{e:?}");
        let msg = e.to_string();
        assert!(msg.contains("PLANNED, not invoked"), "{msg}");
        assert!(msg.contains("bridge"), "names the plugin: {msg}");
    }

    #[test]
    fn a_missing_plugin_names_every_directory_searched() {
        // A bare "not found" for a binary the operator installed somewhere
        // is the least actionable error there is.
        let env = ExecCniEnv::new(vec![
            PathBuf::from("/opt/cni/bin"),
            PathBuf::from("/usr/libexec/cni"),
        ]);
        let e = env.resolve("bridge").unwrap_err();
        let msg = e.to_string();
        assert!(msg.contains("/opt/cni/bin"), "{msg}");
        assert!(msg.contains("/usr/libexec/cni"), "{msg}");
    }

    #[test]
    fn planning_succeeds_even_with_no_netns() {
        // Planning must work everywhere, or the contract is untestable on
        // the machine this is written on. Refusal belongs to the ENV.
        let mut s = sandbox();
        s.netns = String::new();
        let p = plan(&conf(), &s, CniCommand::Del, &path());
        assert_eq!(p.invocations.len(), 2);
        assert_eq!(p.invocations[0].env["CNI_NETNS"], "");
    }
}
