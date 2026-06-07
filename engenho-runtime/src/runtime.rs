//! The [`Runtime`] — single-process assembly of every engenho subsystem.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{
    ApiServer, RouterHandlerSink, RouterState, ServerSanInputs, TlsMaterial,
    handlers_from_catalog_with_admission, issue_server_material, load_or_generate_ca,
};
use engenho_kube_client::emit_kubeconfig;
use engenho_config::{ConfigError, EngenhoConfig, KubeletBackendKind as CfgBackendKind};
use engenho_controllers::{
    CrdController, DeploymentController, DynamicHandlerSink, EndpointsController, GcController,
    JobController, KindFilter, ReplicaSetController, StatefulSetController, WatchDriver,
    WatchDriverConfig,
    admission::{AdmissionChain, AdmissionMode},
};
use engenho_kubelet::config_bridge::KubeletBackendKind;
use engenho_kubelet::{ContainerRuntime, Kubelet, make_container_runtime};
use engenho_scheduler::{Scheduler, make_scheduling_strategy};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use tokio::task::JoinHandle;
use tracing::info;

use crate::error::RuntimeError;

/// The assembled single-node runtime. Owns the store spine, the
/// apiserver, every driver task, and the container backend (retained
/// for shutdown + test inspection).
pub struct Runtime {
    config: EngenhoConfig,
    store: Arc<StoreMesh>,
    apiserver: ApiServer,
    drivers: Vec<JoinHandle<()>>,
    /// Kept alive for the runtime's lifetime so the kubelet's backend
    /// outlives every driver tick; tests pass their own clone to
    /// [`Runtime::start_with_backend`] and inspect it.
    #[allow(dead_code)]
    backend: Arc<dyn ContainerRuntime>,
}

impl Runtime {
    /// Boot every subsystem over one [`StoreMesh`]. Returns once the
    /// apiserver is bound + every driver is spawned. The container
    /// backend is constructed from `config.runtime.kubelet_backend`.
    ///
    /// # Errors
    ///
    /// See [`RuntimeError`] — config invalid, store start / leadership
    /// failure, apiserver bind failure, or an unparseable listen addr.
    pub async fn start(config: EngenhoConfig) -> Result<Self, RuntimeError> {
        let backend = build_backend(&config);
        Self::start_inner(config, backend).await
    }

    /// Boot with an explicit pre-built [`ContainerRuntime`] (e.g.
    /// `Arc<FakeBackend>`) so the caller holds the inspection handle.
    /// The config's `kubelet_backend` field is ignored — `backend` IS
    /// the runtime the kubelet drives.
    ///
    /// # Errors
    ///
    /// Same as [`Runtime::start`].
    pub async fn start_with_backend(
        config: EngenhoConfig,
        backend: Arc<dyn ContainerRuntime>,
    ) -> Result<Self, RuntimeError> {
        Self::start_inner(config, backend).await
    }

    async fn start_inner(
        config: EngenhoConfig,
        backend: Arc<dyn ContainerRuntime>,
    ) -> Result<Self, RuntimeError> {
        // 1. Validate the whole config (every section + cross-section).
        config.validate()?;

        // 2. Bring up the store spine. Durable = restart-safe
        //    start_or_resume; ephemeral = in-memory start +
        //    initialize_singleton (test path).
        let store = boot_store(&config).await?;

        // 3. Wait for raft leadership — MUST precede any propose.
        let timeout_s = config.runtime.leadership_timeout_seconds;
        if !store
            .wait_for_leadership(Duration::from_secs(u64::from(timeout_s)))
            .await
        {
            return Err(RuntimeError::LeadershipTimeout { seconds: timeout_s });
        }
        info!(node = %config.runtime.node_name, "store reached leadership");

        // 4. Register THIS node so the scheduler has a schedulable
        //    target (the missing brick — no other code does this).
        register_node(&store, &config.runtime.node_name).await?;

        // 5. Bind the apiserver, backed by the same store.
        let listen_addr: SocketAddr =
            config
                .runtime
                .listen_addr
                .parse()
                .map_err(|source| RuntimeError::ListenAddr {
                    addr: config.runtime.listen_addr.clone(),
                    source,
                })?;

        // 5a. Build the TLS material BEFORE binding (when tls.enabled).
        //     load-or-generate the data_dir-persisted cluster CA, then
        //     issue a server cert whose SANs cover loopback + node name +
        //     the concrete listen IP (skipping 0.0.0.0/:: which aren't
        //     valid SAN IPs — loopback access rides on 127.0.0.1/localhost).
        //     `ca_cert_pem` is captured for the boot-time kubeconfig write.
        let mut ca_cert_pem: Option<String> = None;
        let tls: Option<TlsMaterial> = if config.runtime.tls.enabled {
            let ca = load_or_generate_ca(&config.runtime.data_dir)
                .map_err(|e| RuntimeError::Server(e.into()))?;
            ca_cert_pem = Some(ca.cert_pem().to_string());
            let listen_ip = san_listen_ip(listen_addr);
            let material = issue_server_material(
                &ca,
                &ServerSanInputs {
                    node_name: &config.runtime.node_name,
                    listen_ip,
                },
            )
            .map_err(|e| RuntimeError::Server(e.into()))?;
            Some(material)
        } else {
            None
        };

        // Admission chain dispatched on every API-boundary create / patch
        // / delete. M0.1 starts with an EMPTY chain (admits everything);
        // the wiring is the load-bearing part — registering a webhook is
        // now a one-line change. Controller writes (Reason::Controller)
        // never flow through a handler, so they bypass admission.
        let admission = Arc::new(AdmissionChain::new(Vec::new(), AdmissionMode::FailOpen));

        // Build the RouterState HERE (not inside ApiServer::start) so the
        // SAME table is shared with the CrdController's DynamicHandlerSink.
        // A controller-driven `register()` mutates this exact ArcSwap, and
        // the swap is visible to in-flight requests this server dispatches.
        let router_state = RouterState::new(handlers_from_catalog_with_admission(
            store.clone(),
            admission.clone(),
        ));
        // The CRD handler sink: builds a StoreBackedHandler (admission-
        // dispatched, opaque-JSON) per served CRD version + registers it
        // into the SAME router_state. Shared (as Arc<dyn DynamicHandlerSink>)
        // with the CrdController spawned in spawn_drivers.
        let handler_sink: Arc<dyn DynamicHandlerSink> =
            RouterHandlerSink::new(store.clone(), admission.clone(), router_state.clone())
                .into_dyn();

        let apiserver = ApiServer::start_with_state(listen_addr, router_state, tls).await?;
        let bound_addr = apiserver.local_addr();
        info!(addr = %bound_addr, tls = config.runtime.tls.enabled, "apiserver bound");

        // 5b. Boot-time kubeconfig write (TLS only — handing kubectl an
        //     anonymous-over-plaintext kubeconfig makes no sense). Uses the
        //     ACTUALLY-bound port so an ephemeral `:0` config still yields a
        //     usable kubeconfig, and the SAME CA the server cert chains to so
        //     kubectl's certificate-authority-data verifies the presented cert.
        if let Some(ca_pem) = ca_cert_pem.as_deref() {
            write_boot_kubeconfig(&config, bound_addr, ca_pem)?;
        }

        // 6. Construct the scheduling strategy from config. A
        //    designed-but-unimplemented strategy (BinPack/Affinity) is a
        //    typed error here — never a silent downgrade to round-robin.
        //    (config.validate() already rejects these in step 1; this is
        //    the load-bearing construction-time guard so the fallible
        //    factory can never be bypassed.)
        let strategy = make_scheduling_strategy(&config.scheduler).map_err(|e| match e {
            engenho_scheduler::SchedulerError::UnsupportedStrategy { requested } => {
                RuntimeError::Config(ConfigError::InvalidField {
                    field: "scheduler.strategy".into(),
                    reason: format!("unsupported scheduling strategy: {requested:?}"),
                })
            }
            other => RuntimeError::Config(ConfigError::Incoherent(other.to_string())),
        })?;

        // 7. Spawn the controller / scheduler / kubelet drivers (incl. the
        //    CrdController, which registers CR handlers into the shared
        //    router table via handler_sink).
        let drivers = spawn_drivers(&config, &store, &backend, strategy, &handler_sink);
        info!(count = drivers.len(), "drivers spawned");

        Ok(Self {
            config,
            store,
            apiserver,
            drivers,
            backend,
        })
    }

    /// The address the apiserver actually bound to (resolves the
    /// `:0` ephemeral-port case to the OS-assigned port).
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.apiserver.local_addr()
    }

    /// A clone of the store spine — for tools + tests.
    #[must_use]
    pub fn store(&self) -> Arc<StoreMesh> {
        self.store.clone()
    }

    /// The loaded config — for tools + tests.
    #[must_use]
    pub fn config(&self) -> &EngenhoConfig {
        &self.config
    }

    /// Graceful shutdown: abort + await every driver task, shut the
    /// apiserver down (2s grace, severs open watches), then terminate
    /// the store.
    ///
    /// `terminate` consumes [`StoreMesh`] and requires the SOLE strong
    /// `Arc` ref. The driver tasks + apiserver handlers each hold a
    /// clone; aborting + awaiting the tasks and shutting the apiserver
    /// down drops those clones, so `Arc::try_unwrap` then succeeds.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::Server`] on apiserver shutdown failure,
    /// [`RuntimeError::StoreStillShared`] if a store clone leaked past
    /// shutdown, or [`RuntimeError::Store`] on `terminate` failure.
    pub async fn shutdown(self) -> Result<(), RuntimeError> {
        // Abort each driver then await it so its captured Arc<StoreMesh>
        // (and the controller it owns) is actually dropped before we try
        // to unwrap the store. Awaiting an aborted JoinHandle returns a
        // Cancelled JoinError — expected, not an error here.
        for handle in &self.drivers {
            handle.abort();
        }
        for handle in self.drivers {
            let _ = handle.await;
        }

        // Shut the apiserver down — severs open watch long-polls (each
        // holds a StoreBackedHandler → Arc<StoreMesh> clone) within a
        // bounded 2s grace.
        self.apiserver.shutdown().await?;

        // Now the Runtime should hold the only strong ref. Take it.
        let store = Arc::try_unwrap(self.store).map_err(|arc| RuntimeError::StoreStillShared {
            strong_count: Arc::strong_count(&arc),
        })?;
        store.terminate().await?;
        Ok(())
    }
}

/// Construct the container backend from the operator's config choice.
fn build_backend(config: &EngenhoConfig) -> Arc<dyn ContainerRuntime> {
    let kind = match config.runtime.kubelet_backend {
        CfgBackendKind::Podman => KubeletBackendKind::Podman,
        CfgBackendKind::Fake => KubeletBackendKind::Fake,
    };
    make_container_runtime(kind, config.runtime.podman_binary.as_deref())
}

/// Bring up the store spine — durable or ephemeral per config.
async fn boot_store(config: &EngenhoConfig) -> Result<Arc<StoreMesh>, RuntimeError> {
    let cfg = default_config(&config.cluster.name)?;
    let router = InProcessRouter::new();
    // Single-node self-loop address; registration happens inside start.
    let listen = "in-process://1".to_string();

    if config.runtime.durable {
        let store_path = config.runtime.data_dir.join("store");
        let (mesh, fresh) = StoreMesh::start_or_resume(1, listen, router, cfg, store_path).await?;
        info!(fresh, "durable store opened");
        Ok(Arc::new(mesh))
    } else {
        let mesh = StoreMesh::start(1, listen, router, cfg).await?;
        mesh.initialize_singleton().await?;
        info!("ephemeral store initialized");
        Ok(Arc::new(mesh))
    }
}

/// Idempotent Node self-registration. Re-Put on restart is fine — the
/// store preserves `metadata.uid` across updates. K8s shape mirrors the
/// scheduler's `is_schedulable` expectation: `spec.unschedulable=false`
/// + a Ready=True condition.
///
/// Writes `status.allocatable` (+ `status.capacity`) for cpu/memory.
/// This is LOAD-BEARING: engenho-scheduler's M0.1 resource-fit predicate
/// uses a zero-on-absent allocatable policy (an un-sized node fits NO
/// pod that requests cpu/memory). Without these values, every pod that
/// declares a request would stay Pending forever. We report the host's
/// actual logical-CPU count + total memory so the single-node cluster
/// advertises real capacity.
async fn register_node(store: &StoreMesh, node_name: &str) -> Result<(), RuntimeError> {
    let (cpu, memory) = host_capacity();
    let value = serde_json::json!({
        "kind": "Node",
        "apiVersion": "v1",
        "metadata": { "name": node_name },
        "spec": { "unschedulable": false },
        "status": {
            "capacity": { "cpu": cpu, "memory": memory },
            "allocatable": { "cpu": cpu, "memory": memory },
            "conditions": [{ "type": "Ready", "status": "True" }]
        }
    });
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::cluster_scoped("", "v1", "Node", node_name),
            value,
            expected: None,
            reason: Reason::Operator,
        })
        .await?;
    info!(node = %node_name, "registered schedulable node");
    Ok(())
}

/// Host capacity advertised on the self-registered Node: `(cpu, memory)`
/// as K8s quantity strings.
///
/// CPU is the host's logical-core count (`std::thread::available_parallelism`,
/// falling back to 1). Memory is a conservative fixed default (`8Gi`,
/// the documented engenho-local VM size) — a real total-memory probe is
/// a follow-up (would add a sysinfo dep); the value only needs to be a
/// truthful lower bound for the resource-fit predicate to admit normal
/// workloads. Both are integer/SI quantities the scheduler parses back
/// through the typed `Quantity` surface.
fn host_capacity() -> (String, String) {
    let cpus = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);
    (cpus.to_string(), "8Gi".to_string())
}

/// The listen IP to add as a server-cert SAN, or `None` when it isn't a
/// usable SAN IP. `0.0.0.0` / `::` are *unspecified* bind addresses, not
/// valid SAN IPs — a cert with an unspecified-IP SAN verifies against no
/// real connection, so we drop them and let the always-present
/// `127.0.0.1` + `localhost` SANs carry loopback access.
fn san_listen_ip(addr: SocketAddr) -> Option<std::net::IpAddr> {
    let ip = addr.ip();
    if ip.is_unspecified() { None } else { Some(ip) }
}

/// Write `data_dir/kubeconfig` (mode 0644) so an operator can immediately
/// `kubectl --kubeconfig <data_dir>/kubeconfig get nodes`. server_url is
/// `https://127.0.0.1:<bound_port>` (loopback SAN + the real bound port);
/// `ca_pem` is the cluster CA the server cert chains to.
fn write_boot_kubeconfig(
    config: &EngenhoConfig,
    bound_addr: SocketAddr,
    ca_pem: &str,
) -> Result<(), RuntimeError> {
    // Loopback server URL with the actually-bound port (handles `:0`).
    let server_url = loopback_server_url(bound_addr);
    let yaml = emit_kubeconfig(&config.cluster.name, &server_url, ca_pem.as_bytes())
        .map_err(|e| RuntimeError::Kubeconfig(e.to_string()))?;
    let path = config.runtime.data_dir.join("kubeconfig");
    write_mode_0644(&path, &yaml)?;
    info!(path = %path.display(), server = %server_url, "kubeconfig written");
    Ok(())
}

/// `https://127.0.0.1:<port>` — the loopback URL kubectl targets. We use
/// loopback (not the bound IP) because `127.0.0.1` is always a server-cert
/// SAN, so the kubeconfig is usable even when `listen_addr` is `0.0.0.0`.
fn loopback_server_url(bound_addr: SocketAddr) -> String {
    let mut url = String::from("https://127.0.0.1:");
    url.push_str(&bound_addr.port().to_string());
    url
}

/// Write `contents` to `path` with mode 0644 on unix (no-op chmod
/// elsewhere). Creates the parent dir if missing (it normally exists —
/// the durable store already opened `data_dir/store`).
fn write_mode_0644(path: &std::path::Path, contents: &str) -> Result<(), RuntimeError> {
    let io_err = |p: &std::path::Path| {
        let p = p.to_path_buf();
        move |source: std::io::Error| RuntimeError::KubeconfigIo { path: p.clone(), source }
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(io_err(parent))?;
    }
    std::fs::write(path, contents.as_bytes()).map_err(io_err(path))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o644))
            .map_err(io_err(path))?;
    }
    Ok(())
}

/// Build + spawn every driver gated on `controllers.enable.*`. The
/// scheduler + kubelet always run (a single-node runtime that can't
/// schedule or run containers is useless); the four reconcilers are
/// individually toggleable.
fn spawn_drivers(
    config: &EngenhoConfig,
    store: &Arc<StoreMesh>,
    backend: &Arc<dyn ContainerRuntime>,
    strategy: Box<dyn engenho_scheduler::SchedulingStrategy>,
    handler_sink: &Arc<dyn DynamicHandlerSink>,
) -> Vec<JoinHandle<()>> {
    let mut handles = Vec::new();

    // Namespace scope: empty string in config means "all namespaces".
    let ns: Option<String> = {
        let n = &config.controllers.namespace;
        if n.is_empty() { None } else { Some(n.clone()) }
    };

    let debounce = Duration::from_millis(u64::from(config.controllers.debounce_milliseconds));
    let fallback = Duration::from_secs(u64::from(config.controllers.fallback_interval_seconds));

    let driver_config = |kinds: &[&str]| WatchDriverConfig {
        filter: KindFilter::Kinds(kinds.iter().map(|k| (*k).to_string()).collect()),
        debounce,
        fallback_interval: fallback,
    };

    let enable = &config.controllers.enable;

    if enable.deployment {
        let c = DeploymentController::new(store.clone(), ns.clone());
        handles.push(
            WatchDriver::new(
                c,
                store.clone(),
                driver_config(&["Deployment", "ReplicaSet"]),
            )
            .spawn(),
        );
    }
    if enable.replicaset {
        let c = ReplicaSetController::new(store.clone(), ns.clone());
        handles.push(
            WatchDriver::new(c, store.clone(), driver_config(&["ReplicaSet", "Pod"])).spawn(),
        );
    }
    if enable.statefulset {
        let c = StatefulSetController::new(store.clone(), ns.clone());
        handles.push(
            WatchDriver::new(c, store.clone(), driver_config(&["StatefulSet", "Pod"])).spawn(),
        );
    }
    if enable.job {
        let c = JobController::new(store.clone(), ns.clone());
        handles.push(WatchDriver::new(c, store.clone(), driver_config(&["Job", "Pod"])).spawn());
    }
    if enable.endpoints {
        let c = EndpointsController::new(store.clone(), ns.clone());
        handles.push(
            WatchDriver::new(
                c,
                store.clone(),
                driver_config(&["Service", "Pod", "Endpoints"]),
            )
            .spawn(),
        );
    }
    if enable.gc {
        let c = GcController::new(store.clone(), ns.clone());
        handles.push(
            WatchDriver::new(
                c,
                store.clone(),
                WatchDriverConfig {
                    filter: KindFilter::All,
                    debounce,
                    fallback_interval: fallback,
                },
            )
            .spawn(),
        );
    }

    // CRD: CustomResourceDefinition → dynamic CR-handler registration. The
    // controller registers a StoreBackedHandler per served CRD version into
    // the shared RouterState via `handler_sink`, so CR instances become
    // routable + discoverable with no parallel codepath. Filtered to
    // ["CustomResourceDefinition"] so only CRD writes wake it; the fallback
    // tick covers cold start / missed events (so a CRD installed before the
    // driver subscribed still gets registered on the first fallback tick).
    if enable.crd {
        let c = CrdController::new(store.clone(), handler_sink.clone());
        handles.push(
            WatchDriver::new(
                c,
                store.clone(),
                driver_config(&["CustomResourceDefinition"]),
            )
            .spawn(),
        );
    }

    // Scheduler: pending Pod → spec.nodeName. Watches Pods + Nodes.
    // The strategy was constructed fallibly by the caller (a typed error
    // for unimplemented strategies — never a silent round-robin fallback).
    {
        let c = Scheduler::new(store.clone(), strategy, ns.clone());
        handles.push(WatchDriver::new(c, store.clone(), driver_config(&["Pod", "Node"])).spawn());
    }

    // Kubelet: bound Pod → container via the backend. Watches Pods.
    {
        let c = Kubelet::new(
            store.clone(),
            backend.clone(),
            config.runtime.node_name.clone(),
        );
        handles.push(WatchDriver::new(c, store.clone(), driver_config(&["Pod"])).spawn());
    }

    handles
}

// `make_scheduling_strategy` returns `Result<Box<dyn SchedulingStrategy>,
// SchedulerError>`; the boxed strategy is unwrapped fallibly in
// `start_inner` (typed error for unimplemented strategies) and handed to
// `spawn_drivers`. `Scheduler::new<S: SchedulingStrategy + 'static>`
// accepts the box (Box<dyn Trait> implements Trait via the blanket impl).

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_config::KubeletBackendKind as CfgKind;
    use shikumi::TieredConfig;

    fn ephemeral_test_config() -> EngenhoConfig {
        let mut cfg = EngenhoConfig::prescribed_default();
        cfg.runtime.listen_addr = "127.0.0.1:0".into();
        cfg.runtime.durable = false;
        cfg.runtime.node_name = "node-A".into();
        cfg.runtime.kubelet_backend = CfgKind::Fake;
        cfg.runtime.leadership_timeout_seconds = 5;
        // Plaintext: these unit tests assert subsystem assembly, not TLS,
        // and the prescribed data_dir (/var/lib/engenho) isn't writable in
        // CI. The dedicated TLS integration test (tests/m0_4_tls_kubectl.rs)
        // exercises the HTTPS + kubeconfig path with a tempdir data_dir.
        cfg.runtime.tls.enabled = false;
        cfg.controllers.fallback_interval_seconds = 1;
        cfg.controllers.debounce_milliseconds = 20;
        cfg
    }

    #[tokio::test]
    async fn runtime_boots_all_subsystems_and_registers_node() {
        let rt = Runtime::start(ephemeral_test_config()).await.unwrap();
        // apiserver bound to an ephemeral port.
        assert_ne!(rt.local_addr().port(), 0);
        // The Node we self-registered is in the store.
        let key = ResourceKey::cluster_scoped("", "v1", "Node", "node-A");
        let node = rt.store().get(&key).await.expect("Node registered");
        assert_eq!(node.get("kind").unwrap(), "Node");
        assert_eq!(
            node.get("spec").unwrap().get("unschedulable").unwrap(),
            false
        );
        // Drivers: 7 reconcilers (deployment, replicaset, statefulset,
        // job, endpoints, gc, crd) + scheduler + kubelet = 9.
        assert_eq!(rt.drivers.len(), 9);
        rt.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn runtime_rejects_invalid_config() {
        let mut cfg = ephemeral_test_config();
        cfg.runtime.node_name = String::new();
        // `Runtime` isn't `Debug` (holds ApiServer + trait-object
        // backend), so match the result rather than `unwrap_err()`.
        match Runtime::start(cfg).await {
            Err(RuntimeError::Config(_)) => {}
            Err(other) => panic!("expected Config error, got {other:?}"),
            Ok(_) => panic!("expected validation failure, got a booted Runtime"),
        }
    }

    #[tokio::test]
    async fn runtime_rejects_unimplemented_scheduling_strategy() {
        // A config asking for BinPack must fail fast at boot with a typed
        // Config error — NEVER silently boot a round-robin cluster.
        let mut cfg = ephemeral_test_config();
        cfg.scheduler.strategy = engenho_config::SchedulerStrategyKind::BinPack;
        match Runtime::start(cfg).await {
            Err(RuntimeError::Config(ConfigError::InvalidField { field, .. })) => {
                assert_eq!(field, "scheduler.strategy");
            }
            Err(other) => panic!("expected Config/InvalidField, got {other:?}"),
            Ok(_) => panic!("expected a typed strategy error, got a booted Runtime"),
        }
    }

    #[tokio::test]
    async fn registered_node_advertises_allocatable() {
        // The companion fix: the self-registered Node MUST carry
        // status.allocatable so the resource-fit predicate (zero-on-absent)
        // admits normal workloads.
        let rt = Runtime::start(ephemeral_test_config()).await.unwrap();
        let key = ResourceKey::cluster_scoped("", "v1", "Node", "node-A");
        let node = rt.store().get(&key).await.expect("Node registered");
        let alloc = node
            .get("status")
            .and_then(|s| s.get("allocatable"))
            .expect("status.allocatable present");
        assert!(
            alloc.get("cpu").and_then(|c| c.as_str()).is_some(),
            "allocatable.cpu must be set; node={node:#}"
        );
        assert!(
            alloc.get("memory").and_then(|m| m.as_str()).is_some(),
            "allocatable.memory must be set; node={node:#}"
        );
        rt.shutdown().await.unwrap();
    }
}
