//! `CriBackend` — the kubelet driving a container runtime over CRI.
//!
//! ★ WHY THIS EXISTS AND `podman_api` DOES NOT SUFFICE. Upstream's kubelet
//! speaks CRI and nothing else; containerd, CRI-O and youki all implement it
//! and none of them speaks podman's libpod API. A Kubernetes distribution
//! whose kubelet can only drive one runtime is not a distribution, and the
//! seam being a typed protobuf rather than an HTTP shape nobody versions is
//! what converts a class of silent breakage into a compile error.
//!
//! ── ★ THE SANDBOX IS THE WHOLE DIFFICULTY ─────────────────────────────────
//! CRI is POD-oriented: `RunPodSandbox` → `CreateContainer(sandbox_id)` →
//! `StartContainer`. [`ContainerRuntime`] is CONTAINER-oriented — seven
//! methods, every one keyed by an opaque container id, with no per-pod hook
//! anywhere. The gap is bridged HERE rather than by reshaping the trait,
//! because the trait's other two implementors have no sandbox concept and
//! giving them one would be a large change to serve a single caller.
//!
//! The bridge is honest about its cost: this backend keeps its own
//! `(pod → sandbox)` map and creates a sandbox on the first container of a
//! pod. That is correct *because* [`PodIdentity`] now travels on the spec —
//! before it did, the only pod key available was `format!("{ns}_{pod}_{c}")`,
//! a join the kubelet explicitly forbids reversing, and any grouping built on
//! parsing it would have been wrong for a name containing `_`.
//!
//! ★ WHAT IS DEFERRED, NAMED RATHER THAN HIDDEN — see `sandbox_for`:
//! sandbox teardown is driven by container removal, so a pod whose containers
//! are all removed leaves its sandbox until [`CriBackend::reap_sandboxes`] is
//! called. There is no per-pod delete hook on the trait to do better.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{Mutex, OnceCell};
use tonic::transport::Channel;

use crate::backend::{
    ContainerRuntime, ContainerSpec, ContainerStatus, ExecOutcome, LogOptions, PodIdentity,
};
use crate::cri::{Endpoint, RunState, v1};
use crate::error::KubeletError;

/// Where CRI runtimes are told to write container logs, and where this
/// backend reads them back from.
///
/// ★ CRI HAS NO READ-LOG RPC. The complete `RuntimeService` contains
/// `ReopenContainerLog` and nothing else log-shaped: the kubelet CHOOSES the
/// path, hands it down at create time, and parses the file itself. So
/// `log_directory` is load-bearing — a backend that leaves it empty produces a
/// runtime writing logs wherever it likes, and `kubectl logs` then returns 200
/// with an empty body rather than an error.
pub const POD_LOG_ROOT: &str = "/var/log/pods";

/// The key a sandbox is tracked under. `uid` is included because it is what
/// distinguishes a Pod from its own replacement after a delete/recreate — two
/// Pods with the same namespace/name are a normal occurrence, and keying
/// without the uid would hand the new Pod the dead one's sandbox.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct PodKey {
    namespace: String,
    name: String,
    uid: String,
}

impl PodKey {
    fn of(id: &PodIdentity) -> Self {
        Self {
            namespace: id.namespace.clone(),
            name: id.name.clone(),
            uid: id.uid.clone(),
        }
    }

    /// `/var/log/pods/<ns>_<name>_<uid>/` — upstream's own layout, reproduced
    /// so anything else reading this node's logs (a log shipper, a human) finds
    /// them where every other Kubernetes node puts them.
    fn log_directory(&self) -> String {
        let mut p = String::from(POD_LOG_ROOT);
        p.push('/');
        p.push_str(&self.namespace);
        p.push('_');
        p.push_str(&self.name);
        p.push('_');
        p.push_str(&self.uid);
        p
    }
}

/// The kubelet's CRI client.
pub struct CriBackend {
    endpoint: Endpoint,
    /// Dialed on first use, not in the constructor.
    ///
    /// ★ [`crate::config_bridge::make_container_runtime`] is SYNCHRONOUS and
    /// returns `Arc<dyn ContainerRuntime>`; a gRPC dial is async. Connecting
    /// lazily keeps that signature (so every existing caller is untouched) and
    /// mirrors `PodmanApiBackend::discover`, which likewise decides the
    /// endpoint synchronously by stat-ing it and does its I/O later.
    channel: OnceCell<Channel>,
    sandboxes: Mutex<BTreeMap<PodKey, String>>,
    /// Which sandbox each container id belongs to, so `remove` can drop a
    /// sandbox whose last container is gone.
    container_pod: Mutex<BTreeMap<String, PodKey>>,
    /// `(host, port)` injected into every container as the
    /// `KUBERNETES_SERVICE_*` block.
    ///
    /// Carried here for the same reason both podman backends carry it: an
    /// in-cluster client with no service env reports "no kubeconfig", which is
    /// correct-but-useless and is exactly what pangea-operator hit.
    kubernetes_service: Option<(String, u16)>,
}

impl CriBackend {
    /// Select an endpoint without connecting to it.
    ///
    /// Probes `endpoint` if given, else the upstream default order, and takes
    /// the first socket that EXISTS. Existence is not reachability — the dial
    /// happens on first use and its failure is a typed error there, because a
    /// runtime that is installed but not yet up at kubelet start is a normal
    /// boot ordering, not a reason to refuse to run.
    #[must_use]
    pub fn discover(endpoint: Option<&str>) -> Option<Self> {
        let candidates: Vec<String> = match endpoint {
            Some(e) => vec![e.to_string()],
            None => crate::cri::DEFAULT_ENDPOINTS
                .iter()
                .map(|s| (*s).to_string())
                .collect(),
        };
        for raw in candidates {
            let Some(ep) = Endpoint::parse(&raw) else {
                continue;
            };
            if std::path::Path::new(ep.path()).exists() {
                return Some(Self {
                    endpoint: ep,
                    channel: OnceCell::new(),
                    sandboxes: Mutex::new(BTreeMap::new()),
                    container_pod: Mutex::new(BTreeMap::new()),
                    kubernetes_service: None,
                });
            }
        }
        None
    }

    /// The endpoint this backend will dial.
    #[must_use]
    pub fn endpoint_path(&self) -> &str {
        self.endpoint.path()
    }

    /// Builder: the API-server coordinates injected into every container.
    #[must_use]
    pub fn with_kubernetes_service(mut self, host: impl Into<String>, port: u16) -> Self {
        self.kubernetes_service = Some((host.into(), port));
        self
    }

    async fn conn(&self) -> Result<Channel, KubeletError> {
        self.channel
            .get_or_try_init(|| async {
                let path = PathBuf::from(self.endpoint.path());
                // The URI is ignored by the connector but tonic requires a
                // syntactically valid one. Same shape engenho-csi already uses
                // to dial a CSI driver's unix socket.
                tonic::transport::Endpoint::try_from("http://cri.local")
                    .map_err(|e| KubeletError::Backend(e.to_string()))?
                    .connect_timeout(std::time::Duration::from_secs(10))
                    .connect_with_connector(tower::service_fn(move |_| {
                        let p = path.clone();
                        async move {
                            let s = tokio::net::UnixStream::connect(p).await?;
                            Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(s))
                        }
                    }))
                    .await
                    .map_err(|e| {
                        KubeletError::Backend(format!(
                            "CRI dial failed at {}: {e}",
                            self.endpoint.path()
                        ))
                    })
            })
            .await
            .cloned()
    }

    async fn runtime(
        &self,
    ) -> Result<v1::runtime_service_client::RuntimeServiceClient<Channel>, KubeletError> {
        Ok(v1::runtime_service_client::RuntimeServiceClient::new(
            self.conn().await?,
        ))
    }

    async fn images(
        &self,
    ) -> Result<v1::image_service_client::ImageServiceClient<Channel>, KubeletError> {
        Ok(v1::image_service_client::ImageServiceClient::new(
            self.conn().await?,
        ))
    }

    /// The sandbox for this container's pod, creating it if absent.
    async fn sandbox_for(&self, spec: &ContainerSpec) -> Result<(PodKey, String), KubeletError> {
        if !spec.pod.is_present() {
            // Refused rather than guessed. Inventing an identity would key a
            // sandbox on "" and silently put unrelated containers in one pod.
            return Err(KubeletError::Backend(format!(
                "CRI needs a pod identity and this spec carries none (container {:?}); \
                 a spec built outside the Pod path cannot be run on CRI",
                spec.name
            )));
        }
        let key = PodKey::of(&spec.pod);
        if let Some(id) = self.sandboxes.lock().await.get(&key) {
            return Ok((key, id.clone()));
        }
        let cfg = v1::PodSandboxConfig {
            metadata: Some(v1::PodSandboxMetadata {
                name: key.name.clone(),
                uid: key.uid.clone(),
                namespace: key.namespace.clone(),
                attempt: 0,
            }),
            hostname: key.name.clone(),
            log_directory: key.log_directory(),
            ..Default::default()
        };
        let resp = self
            .runtime()
            .await?
            .run_pod_sandbox(v1::RunPodSandboxRequest {
                config: Some(cfg),
                runtime_handler: String::new(),
            })
            .await
            .map_err(|e| KubeletError::Backend(format!("RunPodSandbox: {e}")))?
            .into_inner();
        self.sandboxes
            .lock()
            .await
            .insert(key.clone(), resp.pod_sandbox_id.clone());
        Ok((key, resp.pod_sandbox_id))
    }

    /// Stop and remove every sandbox that no longer has a tracked container.
    ///
    /// ★ NOT AUTOMATIC, and that is the deferral this module's header names.
    /// [`ContainerRuntime`] has no per-pod delete hook, so the last container's
    /// removal is the only signal available and it does not distinguish "pod
    /// deleted" from "container being recreated". Reaping on container removal
    /// would tear the sandbox down on a restart — and a restart that changes
    /// the pod IP is precisely the defect a sandbox exists to prevent.
    ///
    /// # Errors
    /// Propagates the first teardown failure.
    pub async fn reap_sandboxes(&self) -> Result<usize, KubeletError> {
        let live: std::collections::BTreeSet<PodKey> =
            self.container_pod.lock().await.values().cloned().collect();
        let doomed: Vec<(PodKey, String)> = self
            .sandboxes
            .lock()
            .await
            .iter()
            .filter(|(k, _)| !live.contains(*k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let mut rt = self.runtime().await?;
        let mut n = 0;
        for (key, id) in doomed {
            rt.stop_pod_sandbox(v1::StopPodSandboxRequest {
                pod_sandbox_id: id.clone(),
            })
            .await
            .map_err(|e| KubeletError::Backend(format!("StopPodSandbox: {e}")))?;
            rt.remove_pod_sandbox(v1::RemovePodSandboxRequest { pod_sandbox_id: id })
                .await
                .map_err(|e| KubeletError::Backend(format!("RemovePodSandbox: {e}")))?;
            self.sandboxes.lock().await.remove(&key);
            n += 1;
        }
        Ok(n)
    }

    /// Ensure the image is present per policy, returning the ref to create with.
    ///
    /// ★ `IfNotPresent` IS OURS TO DECIDE, NOT THE RUNTIME'S. Unlike podman's
    /// create endpoint, CRI's `CreateContainer` takes no pull policy —
    /// `PullImage` is an unconditional registry contact on a separate service.
    /// So "just call PullImage and let it decide" silently turns every
    /// `IfNotPresent` container into `Always`: no error, no manifest diff.
    async fn ensure_image(&self, spec: &ContainerSpec) -> Result<String, KubeletError> {
        use crate::backend::PullPolicy;
        let policy = spec.pull_policy.unwrap_or(PullPolicy::IfNotPresent);
        let image_spec = v1::ImageSpec {
            image: spec.image.clone(),
            user_specified_image: spec.image.clone(),
            ..Default::default()
        };
        let mut ic = self.images().await?;
        // ★ ABSENCE IS A SUCCESSFUL RPC WITH A NIL IMAGE, not an error.
        // Mapping it onto Err would report a policy decision as a failure.
        let present = ic
            .image_status(v1::ImageStatusRequest {
                image: Some(image_spec.clone()),
                verbose: false,
            })
            .await
            .map_err(|e| KubeletError::Backend(format!("ImageStatus: {e}")))?
            .into_inner()
            .image;

        match (policy, present) {
            (PullPolicy::Never, None) => Err(KubeletError::Backend(format!(
                "ErrImageNeverPull: {} is absent and imagePullPolicy is Never",
                spec.image
            ))),
            (PullPolicy::Never, Some(img))
            | (PullPolicy::IfNotPresent, Some(img))
            | (PullPolicy::Missing, Some(img)) => Ok(img.id),
            (PullPolicy::Always, _)
            | (PullPolicy::IfNotPresent, None)
            | (PullPolicy::Missing, None) => {
                let resp = ic
                    .pull_image(v1::PullImageRequest {
                        image: Some(image_spec),
                        auth: None,
                        sandbox_config: None,
                    })
                    .await
                    .map_err(|e| KubeletError::Backend(format!("PullImage {}: {e}", spec.image)))?
                    .into_inner();
                // ★ Create with the RETURNED ref, never the user's tag. That is
                // what closes the mutable-tag race where `:latest` resolves to
                // one digest at pull and another at create.
                Ok(resp.image_ref)
            }
        }
    }
}

/// Lower typed resources onto CRI's `LinuxContainerResources`.
///
/// `cpu_period` is fixed at the same 100ms every kubelet uses, so a quota
/// computed here means the same fraction of a core it does on any other node.
#[must_use]
pub fn linux_resources(r: &crate::backend::Resources) -> Option<v1::LinuxContainerResources> {
    if r.is_unset() {
        return None;
    }
    let period = i64::try_from(crate::podman_api::CFS_PERIOD_US).unwrap_or(100_000);
    let quota = r
        .cpu_limit_milli
        .value()
        .map(|milli| (i128::from(milli) * i128::from(period) / 1000) as i64);
    Some(v1::LinuxContainerResources {
        cpu_period: if quota.is_some() { period } else { 0 },
        cpu_quota: quota.unwrap_or(0),
        cpu_shares: r
            .cpu_weight()
            .map(|w| (2 + ((i128::from(w) - 1) * 262_142) / 9999) as i64)
            .unwrap_or(0),
        memory_limit_in_bytes: r.memory_limit_bytes.value().unwrap_or(0),
        ..Default::default()
    })
}

#[async_trait]
impl ContainerRuntime for CriBackend {
    fn name(&self) -> &'static str {
        "cri"
    }

    async fn exec(&self, container_id: &str, argv: &[String]) -> Result<ExecOutcome, KubeletError> {
        // `ExecSync`, not the streaming `Exec`: the trait returns a completed
        // outcome, and streaming `Exec` returns only a URL that needs a
        // streaming server engenho deliberately has not built.
        let resp = self
            .runtime()
            .await?
            .exec_sync(v1::ExecSyncRequest {
                container_id: container_id.to_string(),
                cmd: argv.to_vec(),
                timeout: 0,
            })
            .await
            .map_err(|e| KubeletError::Backend(format!("ExecSync: {e}")))?
            .into_inner();
        Ok(crate::cri::exec_outcome(
            resp.exit_code,
            &resp.stdout,
            &resp.stderr,
        ))
    }

    async fn start(&self, spec: &ContainerSpec) -> Result<ContainerStatus, KubeletError> {
        let (key, sandbox_id) = self.sandbox_for(spec).await?;
        let image_ref = self.ensure_image(spec).await?;

        let mut envs: Vec<v1::KeyValue> = spec
            .env
            .iter()
            .map(|(k, v)| v1::KeyValue {
                key: k.clone(),
                // CRI declares `bytes value = 2` — an env value is not required
                // to be UTF-8 on the wire even though ours always is.
                value: v.clone().into_bytes(),
            })
            .collect();
        if let Some((host, port)) = &self.kubernetes_service {
            let mut merged = spec.env.clone();
            crate::backend::inject_kubernetes_service_env(&mut merged, host, *port);
            envs = merged
                .into_iter()
                .map(|(key, value)| v1::KeyValue {
                    key,
                    value: value.into_bytes(),
                })
                .collect();
        }

        let cname = spec.pod.container_name.clone();
        let mut log_path = cname.clone();
        log_path.push_str("/0.log");

        let cfg = v1::ContainerConfig {
            metadata: Some(v1::ContainerMetadata {
                name: cname,
                attempt: 0,
            }),
            image: Some(v1::ImageSpec {
                image: image_ref,
                user_specified_image: spec.image.clone(),
                ..Default::default()
            }),
            command: spec.command.clone(),
            envs,
            // Relative to the sandbox's log_directory — the contract that makes
            // `logs()` able to find anything at all.
            log_path,
            linux: Some(v1::LinuxContainerConfig {
                resources: linux_resources(&spec.resources),
                security_context: None,
            }),
            ..Default::default()
        };

        let mut rt = self.runtime().await?;
        let created = rt
            .create_container(v1::CreateContainerRequest {
                pod_sandbox_id: sandbox_id,
                config: Some(cfg),
                sandbox_config: None,
            })
            .await
            .map_err(|e| KubeletError::Backend(format!("CreateContainer: {e}")))?
            .into_inner();
        rt.start_container(v1::StartContainerRequest {
            container_id: created.container_id.clone(),
        })
        .await
        .map_err(|e| KubeletError::Backend(format!("StartContainer: {e}")))?;

        self.container_pod
            .lock()
            .await
            .insert(created.container_id.clone(), key);

        // Read back rather than assume — the same rule podman_api follows.
        self.status(&created.container_id).await?.ok_or_else(|| {
            KubeletError::Backend("container vanished between start and status".into())
        })
    }

    async fn status(&self, container_id: &str) -> Result<Option<ContainerStatus>, KubeletError> {
        let resp = match self
            .runtime()
            .await?
            .container_status(v1::ContainerStatusRequest {
                container_id: container_id.to_string(),
                verbose: false,
            })
            .await
        {
            Ok(r) => r.into_inner(),
            Err(s) if s.code() == tonic::Code::NotFound => return Ok(None),
            Err(e) => return Err(KubeletError::Backend(format!("ContainerStatus: {e}"))),
        };
        let Some(st) = resp.status else {
            return Ok(None);
        };
        let run = RunState::from_cri(st.state);
        Ok(Some(ContainerStatus {
            container_id: st.id,
            running: run.is_running(),
            exit_code: run.is_terminal().then_some(st.exit_code),
            pod_ip: None,
        }))
    }

    async fn stop(&self, container_id: &str) -> Result<(), KubeletError> {
        self.runtime()
            .await?
            .stop_container(v1::StopContainerRequest {
                container_id: container_id.to_string(),
                timeout: 30,
            })
            .await
            .map_err(|e| KubeletError::Backend(format!("StopContainer: {e}")))?;
        Ok(())
    }

    async fn remove(&self, container_id: &str) -> Result<(), KubeletError> {
        self.runtime()
            .await?
            .remove_container(v1::RemoveContainerRequest {
                container_id: container_id.to_string(),
            })
            .await
            .map_err(|e| KubeletError::Backend(format!("RemoveContainer: {e}")))?;
        self.container_pod.lock().await.remove(container_id);
        Ok(())
    }

    async fn logs(&self, container_id: &str, _opts: &LogOptions) -> Result<String, KubeletError> {
        // ★ TYPED REFUSAL, NOT AN EMPTY STRING. CRI has no read-log RPC: the
        // logs are a FILE at `<log_directory>/<log_path>` in a line format the
        // kubelet must parse (`<RFC3339Nano> <stream> <F|P> <msg>`), and the
        // parser is not written. Returning `Ok(String::new())` here would
        // violate this trait's own stated rule — "a not-found container
        // surfaces a typed error, never a silently-empty success" — and would
        // present to a user as a container that produced no output.
        Err(KubeletError::Backend(format!(
            "CRI log reading is not implemented: logs for {container_id} live at \
             {POD_LOG_ROOT}/<ns>_<pod>_<uid>/<container>/0.log and need the CRI \
             line-format parser (pending-cri: log-parser)"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ident() -> PodIdentity {
        PodIdentity {
            namespace: "ns".into(),
            name: "p".into(),
            uid: "u-1".into(),
            container_name: "c".into(),
            init: false,
        }
    }

    #[test]
    fn the_log_directory_is_upstreams_layout() {
        // Reproduced, not invented: anything else reading this node's logs
        // expects `/var/log/pods/<ns>_<name>_<uid>/`.
        assert_eq!(
            PodKey::of(&ident()).log_directory(),
            "/var/log/pods/ns_p_u-1"
        );
    }

    #[test]
    fn the_uid_is_part_of_the_sandbox_key() {
        // Two Pods with one name across a delete/recreate are normal. Keying
        // without the uid would hand the new Pod the dead one's sandbox.
        let a = PodKey::of(&ident());
        let mut second = ident();
        second.uid = "u-2".into();
        assert_ne!(a, PodKey::of(&second));
    }

    #[test]
    fn resources_lower_to_the_values_the_kernel_read_back() {
        use crate::backend::Resources;
        let c: serde_json::Value = serde_json::from_str(
            r#"{"name":"c","image":"i","resources":{"limits":{"cpu":"500m","memory":"256Mi"}}}"#,
        )
        .unwrap();
        let r = linux_resources(&Resources::from_container_json(&c)).expect("declared");
        // Same numbers the H1 spike read off a real kernel on rio.
        assert_eq!(r.cpu_quota, 50_000);
        assert_eq!(r.cpu_period, 100_000);
        assert_eq!(r.memory_limit_in_bytes, 268_435_456);
    }

    #[test]
    fn an_undeclared_pod_gets_no_resource_block() {
        assert!(linux_resources(&crate::backend::Resources::default()).is_none());
    }

    #[tokio::test]
    async fn a_spec_without_pod_identity_is_refused_not_guessed() {
        // The alternative — inventing an identity — keys a sandbox on "" and
        // silently puts unrelated containers into one pod.
        let b = CriBackend {
            endpoint: Endpoint("/nonexistent.sock".into()),
            channel: OnceCell::new(),
            sandboxes: Mutex::new(BTreeMap::new()),
            container_pod: Mutex::new(BTreeMap::new()),
            kubernetes_service: None,
        };
        let spec = ContainerSpec {
            name: "ns_p_c".into(),
            image: "i".into(),
            ..Default::default()
        };
        let err = b.sandbox_for(&spec).await.unwrap_err();
        assert!(
            format!("{err}").contains("pod identity"),
            "must name the cause: {err}"
        );
    }

    #[test]
    fn discover_returns_none_when_no_socket_exists() {
        assert!(CriBackend::discover(Some("/definitely/not/here.sock")).is_none());
        // And a tcp endpoint is refused by the parser before any probe.
        assert!(CriBackend::discover(Some("tcp://10.0.0.1:1234")).is_none());
    }
}
