//! The [`Runtime`] — single-process assembly of every engenho subsystem.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{
    ApiServer, ChainAuthenticator, ClientMaterial, RbacAuthorizer, RouterHandlerSink, RouterState,
    SanEntry, ServerSanInputs, StoreRbacEnv, TlsMaterial, client_verifier,
    handlers_from_catalog_with_admission, issue_admin_client_material, issue_server_material,
    load_or_generate_ca,
};
use engenho_config::{
    ConfigError, EngenhoConfig, KubeletBackendKind as CfgBackendKind, ResolvedDatapath,
};
use engenho_controllers::{
    CrdController, CronJobController, DaemonSetController, DeploymentController,
    DynamicHandlerSink, EndpointsController, FakeRouter, GcController, IptablesRouter, IpvsRouter,
    JobController, KindFilter, NamespaceController, PodDisruptionBudgetController,
    PvBinderController, ReplicaSetController, ServiceRouter, ServiceRoutingController,
    StatefulSetController, WallClock, WatchDriver, WatchDriverConfig,
    admission::{AdmissionChain, AdmissionMode, AdmissionWebhook},
    cluster_ip::{ClusterIpDefaultingWebhook, StoreServiceIpSource},
};
use engenho_kube_client::{emit_kubeconfig, emit_kubeconfig_with_admin};
use engenho_kubelet::config_bridge::KubeletBackendKind;
use engenho_kubelet::{
    ContainerRuntime, Kubelet, LogOptions, make_container_runtime_with_apiserver,
};
use engenho_scheduler::{Scheduler, make_scheduling_strategy};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use engenho_types::generated_v1_34::core_v1::Namespace;
use engenho_types::generated_v1_34::rbac_v1::{
    ClusterRole, ClusterRoleBinding, PolicyRule, RoleRef, Subject,
};
use engenho_types::generated_v1_34::types::{NamespaceSpec, NamespaceStatus};
use tokio::task::JoinHandle;
use tracing::{info, warn};

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
        // Fail LOUDLY here if the configured runtime cannot be reached, rather
        // than discovering it one warn-per-tick at a time forever.
        preflight_backend(&config)?;
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

        // 4.5. Seed the bootstrap RBAC policy (Brick B) — cluster-admin +
        //    system:discovery + system:basic-user + system:public-info-viewer
        //    ClusterRoles + their ClusterRoleBindings — BEFORE the apiserver
        //    binds, so the very first request authorizes against a seeded store.
        //    Idempotent (Put preserves uid across restarts, same as
        //    register_node). MUST precede step 5 so anonymous discovery + bound
        //    roles resolve through real bindings from the first request.
        //    Seed the four system namespaces FIRST: a namespace must exist
        //    before anything namespaced can live in it, and `default` is what
        //    every client opens to.
        seed_system_namespaces(&store).await?;
        //    Then the `kubernetes` Service in `default`. It must follow the
        //    namespaces (it lives in one) and precede the apiserver bind, so
        //    the ClusterIP allocator sees .1 as held before any user Service
        //    can be created.
        seed_kubernetes_service(&store, &config).await?;
        seed_bootstrap_rbac(&store).await?;

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
        //
        //     For authn: also issue + persist the admin CLIENT cert, build the
        //     OPTIONAL client-cert verifier (rooted at the SAME CA), attach the
        //     verifier to the server material, and capture the admin material
        //     for the admin-cert kubeconfig.
        let mut ca_cert_pem: Option<String> = None;
        let mut admin_material: Option<ClientMaterial> = None;
        let tls: Option<TlsMaterial> = if config.runtime.tls.enabled {
            let ca = load_or_generate_ca(&config.runtime.data_dir)
                .map_err(|e| RuntimeError::Server(e.into()))?;
            ca_cert_pem = Some(ca.cert_pem().to_string());
            let listen_ip = san_listen_ip(listen_addr);
            // Classify the operator's declared SANs before anything binds, so a
            // typo is a failed unit with the offending value in the message
            // rather than a certificate that serves happily and verifies for
            // nobody. See `RuntimeError::ExtraSan` for why this is checked here
            // and not lazily at handshake time.
            let extra_sans = server_sans(&config)?;
            let material = issue_server_material(
                &ca,
                &ServerSanInputs {
                    node_name: &config.runtime.node_name,
                    listen_ip,
                    extra_sans: &extra_sans,
                },
            )
            .map_err(|e| RuntimeError::Server(e.into()))?;
            // OPTIONAL client-cert verifier (allow_unauthenticated): existing
            // token/anonymous kubectl keeps connecting; a presented cert is
            // verified against the CA before the handshake completes.
            let verifier = client_verifier(&ca).map_err(|e| RuntimeError::Server(e.into()))?;
            let material = material.with_client_verifier(verifier);
            // Issue + persist the admin client cert (for the operator's
            // kubeconfig + `kubectl auth whoami → engenho-admin`).
            let admin =
                issue_admin_client_material(&ca).map_err(|e| RuntimeError::Server(e.into()))?;
            persist_admin_material(&config.runtime.data_dir, &admin)?;
            admin_material = Some(admin);
            Some(material)
        } else {
            None
        };

        // Load-or-generate the bootstrap admin BEARER token (a second admin
        // credential alongside the client cert). Persisted under
        // data_dir/pki/admin.token (0600) + logged so the operator can
        // `curl -H "Authorization: Bearer <token>"`. Minted REGARDLESS of TLS:
        // the token is a bearer SECRET (an `Authorization:` header value), not
        // TLS material — and with Brick B's default-deny it is the ONLY way a
        // plaintext-mode operator/test gets an admin (system:masters) identity
        // to write through the authorizer. (Pre-Brick-B the plaintext floor had
        // no admin token because authorize-ALL made one unnecessary.)
        let admin_token: Option<String> =
            Some(load_or_generate_admin_token(&config.runtime.data_dir)?);
        if admin_token.is_some() {
            info!("bootstrap admin bearer token available at data_dir/pki/admin.token");
        }

        // Admission chain dispatched on every API-boundary create / patch
        // / delete. Controller writes (Reason::Controller) never flow through
        // a handler, so they bypass admission.
        //
        // The ClusterIP defaulting webhook is the FIRST registered hook: on
        // Service create with no explicit `clusterIP` it allocates a free VIP
        // from `networking.service_cidr` and stamps `spec.clusterIP` +
        // `spec.clusterIPs`. It reads the live Service set off the SAME store
        // (restart-persistent + collision-free — the Services are the ledger).
        // FailClosed so a misconfigured CIDR / exhausted pool denies the
        // create rather than admitting a half-built Service.
        let cluster_ip_hook: Arc<dyn AdmissionWebhook> = Arc::new(ClusterIpDefaultingWebhook::new(
            config.networking.service_cidr.clone(),
            Arc::new(StoreServiceIpSource::new(store.clone())),
        ));
        let admission = Arc::new(AdmissionChain::new(
            vec![cluster_ip_hook],
            AdmissionMode::FailClosed,
        ));

        // The typed authenticator chain (X509 → SA → admin-token → anonymous),
        // carrying the configured bootstrap admin bearer token. Installed into
        // the RouterState so the authn middleware resolves the admin bearer +
        // admin client cert to the admin identity; everything else is unchanged.
        //
        // The SA stage's key is loaded here — see `build_authenticator`, which
        // holds the reasoning and is unit-tested, because THE BUG THIS FIXES WAS
        // A WIRING BUG: every piece of ServiceAccount authentication already
        // existed and worked, and the runtime called the keyless constructor.
        // A capability that is only reachable through the call site nobody
        // audits is indistinguishable from an absent one.
        let authenticator: Arc<ChainAuthenticator> =
            Arc::new(build_authenticator(&config.runtime.data_dir, admin_token));

        // Build the RouterState HERE (not inside ApiServer::start) so the
        // SAME table is shared with the CrdController's DynamicHandlerSink.
        // A controller-driven `register()` mutates this exact ArcSwap, and
        // the swap is visible to in-flight requests this server dispatches.
        // The typed RBAC authorizer (Brick B), over the SAME store. Default-deny
        // for non-admin identities; the system:masters short-circuit keeps the
        // admin kubeconfig allow-all so every existing live proof passes; the
        // seeded bootstrap policy (step 4.5) grants anonymous discovery + the
        // basic-user self-review surface through real bindings.
        let authorizer: Arc<dyn engenho_apiserver::Authorizer> =
            Arc::new(RbacAuthorizer::new(StoreRbacEnv::new(store.clone())));

        let router_state = RouterState::new(handlers_from_catalog_with_admission(
            store.clone(),
            admission.clone(),
        ))
        .with_authenticator(authenticator)
        .with_authorizer(authorizer);
        // The CRD handler sink: builds a StoreBackedHandler (admission-
        // dispatched, opaque-JSON) per served CRD version + registers it
        // into the SAME router_state. Shared (as Arc<dyn DynamicHandlerSink>)
        // with the CrdController spawned in spawn_drivers.
        let handler_sink: Arc<dyn DynamicHandlerSink> =
            RouterHandlerSink::new(store.clone(), admission.clone(), router_state.clone())
                .into_dyn();
        // Keep a router_state clone so the Pod `/log` handler (which needs the
        // kubelet, built later in spawn_drivers) can be registered after the
        // kubelet exists. RouterState is Arc-backed: this clone shares the SAME
        // handler ArcSwap the apiserver dispatches on, so a `register()` here is
        // visible to in-flight requests (identical mechanism to the CRD sink).
        let router_state_for_logs = router_state.clone();

        let apiserver = ApiServer::start_with_state(listen_addr, router_state, tls).await?;
        let bound_addr = apiserver.local_addr();
        info!(addr = %bound_addr, tls = config.runtime.tls.enabled, "apiserver bound");

        // 5b. Boot-time kubeconfig write (TLS only — handing kubectl an
        //     anonymous-over-plaintext kubeconfig makes no sense). Uses the
        //     ACTUALLY-bound port so an ephemeral `:0` config still yields a
        //     usable kubeconfig, and the SAME CA the server cert chains to so
        //     kubectl's certificate-authority-data verifies the presented cert.
        //
        //     With the admin client cert issued, the kubeconfig embeds it as a
        //     CLIENT-CERT user (→ `kubectl auth whoami` = engenho-admin /
        //     system:masters). Without it (shouldn't happen when TLS is on) it
        //     falls back to the anonymous-token kubeconfig.
        if let Some(ca_pem) = ca_cert_pem.as_deref() {
            write_boot_kubeconfig(&config, bound_addr, ca_pem, admin_material.as_ref())?;
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
        //    router table via handler_sink). Returns the Arc<Kubelet> so the
        //    Pod `/log` reader can be wired in.
        let (drivers, kubelet) = spawn_drivers(&config, &store, &backend, strategy, &handler_sink);
        info!(count = drivers.len(), "drivers spawned");

        // 7b. Register the Pod `/log` handler — a StoreBackedHandler for the
        //     Pod kind whose `logs` delegates to the in-process kubelet (the
        //     KubeletLogReader adapter). This REPLACES the catalog-built Pod
        //     handler (which had no log reader → /log returned NotFound) with
        //     one that serves real container stdout. Single-node: the kubelet
        //     IS this process's kubelet, so the read is in-process. `register`
        //     keys on (group, version, plural) so it overwrites the Pod entry
        //     atomically (same swap mechanism the CRD sink uses).
        // 7a. Bind the KUBELET's own HTTP surface (:10250).
        //
        //     ★ THIS IS WHAT MAKES THE SURFACE EXIST. `KubeletApi` and its
        //     router shipped with a trait, a route table and a test double,
        //     and nothing ever bound them — so the port was a type, not a
        //     port. Logs worked only because the kubelet happens to share a
        //     process with the apiserver; the moment there is a second node,
        //     `kubectl logs` against a pod on it has no path at all.
        //
        //     A bind failure is logged and NOT fatal: the apiserver is
        //     already serving, and killing a working control plane because
        //     one auxiliary port is taken trades a partial outage for a
        //     total one. The log line names the address so the cause is not
        //     a mystery.
        let kubelet_api: Arc<dyn engenho_kubelet::server::KubeletApi> = Arc::new(WeakKubeletApi {
            kubelet: Arc::downgrade(&kubelet),
        });
        let kubelet_addr = config.runtime.kubelet_listen_addr.clone();
        tokio::spawn(async move {
            match tokio::net::TcpListener::bind(&kubelet_addr).await {
                Ok(listener) => {
                    let bound = listener
                        .local_addr()
                        .map_or_else(|_| kubelet_addr.clone(), |a| a.to_string());
                    info!(addr = %bound, "kubelet HTTP surface bound");
                    let app = engenho_kubelet::server::KubeletServer::new(kubelet_api).routes();
                    if let Err(e) = axum::serve(listener, app).await {
                        warn!(error = %e, "kubelet HTTP surface stopped");
                    }
                }
                Err(e) => warn!(
                    addr = %kubelet_addr,
                    error = %e,
                    "kubelet HTTP surface could not bind; container logs and exec \
                     are unreachable from off-process (the apiserver is unaffected)"
                ),
            }
        });

        // The etcd v3 façade on :2379. Same failure posture as :10250: a
        // bind failure is a WARNING, not a boot failure — refusing to start
        // the cluster because one auxiliary port is taken trades a partial
        // outage for a total one.
        //
        // ★ THIS IS WHAT MAKES engenho DRIVABLE BY SOFTWARE THAT HAS NEVER
        // HEARD OF IT. `etcdctl get /registry/ --prefix --keys-only`,
        // `snapshot save`, every backup tool and every runbook that was
        // written against etcd. engenho runs no etcd and its apiserver
        // never speaks it — the INTERFACE is the obligation, not the
        // technology.
        //
        // READ-ONLY: Kv serves Range; Put/DeleteRange/Txn are absent rather
        // than silently dropping writes. See `etcd_facade`'s header.
        if !config.runtime.etcd_listen_addr.is_empty() {
            // `MeshEtcdStore` is `Clone` and holds an `Arc<StoreMesh>`
            // inside, so every clone is the SAME store — the three services
            // must not end up with different views of one cluster.
            let etcd_store = crate::etcd_facade::MeshEtcdStore::new(&store);
            let etcd_addr = config.runtime.etcd_listen_addr.clone();
            let identity = engenho_etcd::server::ServerIdentity::default();
            tokio::spawn(async move {
                match tokio::net::TcpListener::bind(&etcd_addr).await {
                    Ok(listener) => {
                        let bound = listener
                            .local_addr()
                            .map_or_else(|_| etcd_addr.clone(), |a| a.to_string());
                        info!(addr = %bound, "etcd v3 facade bound (read-only)");
                        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
                        let kv = engenho_etcd::server::ReadOnlyKv {
                            store: etcd_store.clone(),
                            identity,
                        };
                        let maintenance = engenho_etcd::server::MaintenanceSvc {
                            store: etcd_store.clone(),
                            identity,
                        };
                        let watch =
                            engenho_etcd::server::WatchSvc::new(Arc::new(etcd_store), identity);
                        if let Err(e) = tonic::transport::Server::builder()
                            .add_service(
                                engenho_etcd::pb::etcdserverpb::kv_server::KvServer::new(kv),
                            )
                            .add_service(
                                engenho_etcd::pb::etcdserverpb::watch_server::WatchServer::new(
                                    watch,
                                ),
                            )
                            .add_service(
                                engenho_etcd::pb::etcdserverpb::maintenance_server::MaintenanceServer::new(
                                    maintenance,
                                ),
                            )
                            .serve_with_incoming(incoming)
                            .await
                        {
                            warn!(error = %e, "etcd v3 facade stopped");
                        }
                    }
                    Err(e) => warn!(
                        addr = %etcd_addr,
                        error = %e,
                        "etcd v3 facade could not bind; etcdctl, snapshot tooling and \
                         any --etcd-servers consumer are unreachable (the apiserver is \
                         unaffected)"
                    ),
                }
            });
        }

        let log_reader: Arc<dyn engenho_apiserver::PodLogReader> =
            Arc::new(KubeletLogReader { kubelet });
        if let Some(pod_handler) = build_pod_log_handler(&store, &admission, log_reader) {
            router_state_for_logs.register(pod_handler);
            info!("registered Pod /log handler (in-process kubelet log reader)");
        }

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

/// Adapter making the in-process [`Kubelet`] satisfy the apiserver's
/// [`engenho_apiserver::PodLogReader`] seam (single-node: the apiserver +
/// kubelet share one process, so the Pod `/log` subresource queries the
/// kubelet's local bookkeeping directly). Translates the apiserver's typed
/// [`engenho_apiserver::LogQuery`] → the kubelet's [`LogOptions`] and maps
/// `KubeletError` → `ApiError`.
///
/// This adapter is the layering bridge: the apiserver (below the kubelet) only
/// knows the `PodLogReader` trait; the runtime (above both) supplies the
/// concrete kubelet behind it. A multi-node future swaps this for a node-proxy
/// reader with no apiserver change.
/// The store, seen through the one capability an event sink needs.
struct MeshEventStore {
    store: Arc<engenho_store::StoreMesh>,
}

#[async_trait::async_trait]
impl engenho_controllers::event_recorder::EventStore for MeshEventStore {
    async fn put_event(
        &self,
        key: engenho_store::ResourceKey,
        value: serde_json::Value,
    ) -> Result<(), String> {
        self.store
            .propose(engenho_store::command::ResourceCommand::Put {
                key,
                value,
                // No precondition: an event is a fresh object with a
                // timestamped name, and a CAS here would turn two events in
                // the same second into a conflict the sink must swallow —
                // losing the SECOND one, which is usually the interesting one.
                expected: None,
                reason: engenho_store::command::Reason::Controller,
            })
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

struct KubeletLogReader {
    kubelet: Arc<Kubelet>,
}

/// The kubelet HTTP surface's view of the kubelet, held WEAKLY.
///
/// ★ WHY WEAK AND NOT `Arc`. The :10250 listener outlives a `Runtime` that
/// is being torn down — `axum::serve` owns its router, the router owns this
/// state, and a strong `Arc<Kubelet>` there keeps the `StoreMesh` alive
/// forever. Measured: it turned four graceful-shutdown tests into
/// `StoreStillShared { strong_count: 2 }`, which is not a test artifact —
/// it is a real leak of the whole store behind a port nobody is using.
///
/// A dropped kubelet then answers with a REASON rather than a hang or a
/// panic: the surface is gone because the node is shutting down, and that
/// is exactly what a client should be told.
struct WeakKubeletApi {
    kubelet: std::sync::Weak<Kubelet>,
}

impl WeakKubeletApi {
    fn get(&self) -> Result<Arc<Kubelet>, String> {
        self.kubelet
            .upgrade()
            .ok_or_else(|| "kubelet is shutting down on this node".to_string())
    }
}

#[async_trait::async_trait]
impl engenho_kubelet::server::KubeletApi for WeakKubeletApi {
    async fn container_logs(
        &self,
        namespace: &str,
        pod: &str,
        container: &str,
        opts: &LogOptions,
    ) -> Result<String, String> {
        engenho_kubelet::server::KubeletApi::container_logs(
            self.get()?.as_ref(),
            namespace,
            pod,
            container,
            opts,
        )
        .await
    }

    async fn pods(&self) -> serde_json::Value {
        match self.get() {
            Ok(k) => engenho_kubelet::server::KubeletApi::pods(k.as_ref()).await,
            // An empty list, not an error: `/pods` has no error shape, and a
            // shutting-down kubelet genuinely manages nothing.
            Err(_) => serde_json::json!({ "kind": "PodList", "apiVersion": "v1", "items": [] }),
        }
    }

    async fn running_pods(&self) -> serde_json::Value {
        match self.get() {
            Ok(k) => engenho_kubelet::server::KubeletApi::running_pods(k.as_ref()).await,
            Err(_) => serde_json::json!({ "kind": "PodList", "apiVersion": "v1", "items": [] }),
        }
    }

    async fn exec(
        &self,
        namespace: &str,
        pod: &str,
        container: &str,
        argv: &[String],
    ) -> Result<engenho_kubelet::backend::ExecOutcome, String> {
        engenho_kubelet::server::KubeletApi::exec(
            self.get()?.as_ref(),
            namespace,
            pod,
            container,
            argv,
        )
        .await
    }
}

#[async_trait::async_trait]
impl engenho_apiserver::PodLogReader for KubeletLogReader {
    async fn read_pod_logs(
        &self,
        namespace: &str,
        name: &str,
        query: &engenho_apiserver::LogQuery,
    ) -> Result<String, engenho_apiserver::ApiError> {
        let opts = LogOptions {
            tail: query.tail_lines,
            timestamps: query.timestamps,
        };
        self.kubelet
            .container_logs(namespace, name, query.container.as_deref(), &opts)
            .await
            .map_err(|e| match e.kind() {
                // A pod not on this node / a missing container → a typed 404
                // (the K8s "could not find the requested resource" shape).
                "invalid_pod" => engenho_apiserver::ApiError::NotFound(format!(
                    "could not get logs for pod {namespace}/{name}: {e}"
                )),
                // A backend read failure → 500 (never a fake-empty log).
                _ => engenho_apiserver::ApiError::Internal(format!(
                    "log read failed for pod {namespace}/{name}: {e}"
                )),
            })
    }
}

/// Build the Pod `/log`-capable handler: a `StoreBackedHandler` for the Pod
/// kind (from the generated catalog descriptor) carrying the SAME admission
/// chain as the catalog-built handlers PLUS the in-process kubelet log reader.
/// Registered into the router (overwriting the no-log-reader Pod handler) so
/// `kubectl logs` resolves the `/log` subresource to real container stdout.
///
/// Returns `None` only if the Pod descriptor is somehow absent from the
/// catalog (impossible — Pod is always cataloged); the caller logs + continues
/// (the existing no-log Pod handler stays, and `/log` returns NotFound — never
/// a panic).
fn build_pod_log_handler(
    store: &Arc<StoreMesh>,
    admission: &Arc<AdmissionChain>,
    log_reader: Arc<dyn engenho_apiserver::PodLogReader>,
) -> Option<Arc<dyn engenho_apiserver::ResourceHandler>> {
    let pod_descriptor = engenho_types::generated_v1_34::RESOURCE_CATALOG
        .iter()
        .find(|d| d.kind == "Pod" && d.group.is_empty())?;
    let handler =
        engenho_apiserver::StoreBackedHandler::from_descriptor(store.clone(), pod_descriptor)
            .with_admission(admission.clone())
            .with_log_reader(log_reader);
    Some(Arc::new(handler))
}

/// Render a small signed int without `format!()` (★★ TYPED EMISSION).
fn itoa_i32(n: i32) -> String {
    let mut out = String::new();
    let mut v = i64::from(n);
    if v < 0 {
        out.push('-');
        v = -v;
    }
    let mut digits = Vec::new();
    if v == 0 {
        digits.push(b'0');
    }
    while v > 0 {
        digits.push(b'0' + u8::try_from(v % 10).unwrap_or(0));
        v /= 10;
    }
    digits.reverse();
    out.push_str(&String::from_utf8(digits).unwrap_or_default());
    out
}

/// Verify at BOOT that a configured container runtime is actually usable.
///
/// The `Fake` backend needs nothing. For `Podman` this runs `podman info`,
/// which requires a working CONNECTION to the runtime — not merely a binary on
/// disk.
///
/// `--version` was the first cut and it was too weak, proven by running it:
/// with a redirected `XDG_CONFIG_HOME` the binary answered `--version` happily
/// while every container start failed with `unable to connect to Podman
/// socket`. A preflight that green-lights an unusable runtime is worse than
/// none, because it moves the failure back to where it was — one warn per
/// reconcile tick, forever. `info` is the cheapest call that actually proves
/// the socket answers, and it is exactly the case a launchd daemon hits: it
/// has neither the operator's PATH nor their podman machine connection.
///
/// Deliberately at boot, once, fatal. The failure this replaces emitted one
/// WARN per reconcile tick forever while the API showed pods with no status at
/// all: a permanently-broken node was indistinguishable from a slow one.
fn preflight_backend(config: &EngenhoConfig) -> Result<(), RuntimeError> {
    if matches!(config.runtime.kubelet_backend, CfgBackendKind::Fake) {
        return Ok(());
    }
    let binary = config
        .runtime
        .podman_binary
        .clone()
        .unwrap_or_else(|| "podman".to_string());
    match std::process::Command::new(&binary)
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
    {
        // `status()` is Ok whenever the process RAN — including when it ran
        // and reported a dead connection. The exit code is the part that
        // carries the verdict, and ignoring it is how the weak `--version`
        // check passed on a runtime that could not start a single container.
        Ok(st) if st.success() => {
            info!(backend = "podman", %binary, "container runtime resolved");
            Ok(())
        }
        Ok(st) => Err(RuntimeError::ContainerRuntimeUnavailable {
            backend: "podman".to_string(),
            binary,
            source: std::io::Error::other(match st.code() {
                Some(c) => ["`podman info` exited ", itoa_i32(c).as_str()].concat(),
                None => "`podman info` was terminated by a signal".to_string(),
            }),
        }),
        Err(source) => Err(RuntimeError::ContainerRuntimeUnavailable {
            backend: "podman".to_string(),
            binary,
            source,
        }),
    }
}

/// Construct the container backend from the operator's config choice.
/// The `kubernetes` Service's address, as `seed_kubernetes_service` creates it.
///
/// The IP is the ClusterIP allocator's FIRST assignment (see that function's
/// doc) and the port is upstream's conventional 443, which fronts the
/// apiserver's real listen port as the target. Stated here as a constant
/// because `build_backend` runs before the store exists, so the seeded object
/// cannot be read at that point.
///
/// Verified live 2026-08-30: `kubectl get svc kubernetes -n default` returns
/// `10.96.0.1:443` on a freshly booted engenho. If the allocator's base ever
/// changes, this must change with it — the two are a pair, and the failure
/// mode is silent (a container gets coordinates that route nowhere).
const DEFAULT_KUBERNETES_SERVICE_IP: &str = "10.96.0.1";

/// The `iss` and `aud` engenho stamps on ServiceAccount tokens.
///
/// Upstream's in-cluster default. Kept as one constant because the issuer a
/// token CLAIMS and the audience the apiserver ACCEPTS must agree — split
/// into two literals they drift, and the failure is a 401 that looks like a
/// key problem.
const SA_ISSUER: &str = "https://kubernetes.default.svc";

/// Mints a pod's ServiceAccount credentials from the cluster's signing key.
///
/// Lives here because it is the only layer holding BOTH the apiserver's
/// signing key and the kubelet — `engenho-kubelet` deliberately does not
/// depend on `engenho-apiserver`, so the kubelet takes this as a trait.
///
/// Upstream mints tokens through the TokenRequest API, so a remote kubelet
/// asks the apiserver rather than holding the key. In a single-binary
/// runtime the two are the same process, which makes issuing directly the
/// honest shape — and the thing that must change first when engenho grows a
/// second node.
struct RuntimeSaProjector {
    signing: ed25519_dalek::SigningKey,
    issuer: String,
    audience: String,
    ca_cert_pem: String,
    lifetime_secs: i64,
}

#[async_trait::async_trait]
impl engenho_kubelet::ServiceAccountProjector for RuntimeSaProjector {
    async fn project(
        &self,
        namespace: &str,
        service_account: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<std::collections::BTreeMap<String, Vec<u8>>>, String> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_secs();
        let now = i64::try_from(now).map_err(|e| e.to_string())?;

        let token = engenho_apiserver::sa_token::issue(
            &self.signing,
            &self.issuer,
            namespace,
            service_account,
            // The SA object's uid is not resolved here; the pod's identity is
            // what a reader needs to trace a request back to a workload.
            pod_uid,
            &[self.audience.clone()],
            Some(engenho_apiserver::sa_token::NamedUid {
                name: pod_name.to_string(),
                uid: pod_uid.to_string(),
            }),
            now,
            self.lifetime_secs,
        )
        .map_err(|e| format!("mint ServiceAccount token: {e}"))?;

        let mut files = std::collections::BTreeMap::new();
        files.insert("token".to_string(), token.into_bytes());
        files.insert("ca.crt".to_string(), self.ca_cert_pem.clone().into_bytes());
        // `Config::incluster()` reads THIS first. Its absence is what made a
        // pod with correct service env still report
        // `ReadDefaultNamespace(NotFound)`.
        files.insert("namespace".to_string(), namespace.as_bytes().to_vec());
        Ok(Some(files))
    }
}

/// Where a POD can actually reach this engenho's apiserver.
///
/// ── ★ AN UNREACHABLE ADDRESS IS WORSE THAN NO ADDRESS ─────────────────────
/// `KUBERNETES_SERVICE_HOST` is not advice; it is the coordinate every
/// in-cluster client library commits to. kube-rs' `Config::infer()` tries
/// in-cluster FIRST and only falls back to a kubeconfig when in-cluster
/// CONSTRUCTION fails. Construction needs the env vars and the projected
/// token — both of which engenho supplies — so injecting an address that
/// routes nowhere makes construction SUCCEED and removes the fallback. The
/// client then fails at every request instead of quietly using a kubeconfig.
///
/// That is the same reasoning `ServiceAccountProjector` already records for
/// tokens: a zero-byte token is worse than an absent one, because the client
/// stops looking for a kubeconfig. The address half had not learned it.
///
/// Measured 2026-09-01 on a darwin workstation: engenho injected the
/// `kubernetes` Service ClusterIP (10.96.0.1:443), pods authenticated with a
/// valid token, and every call failed to connect — surfacing as
/// `Leader election acquire attempt failed … client error (Connect)` in an
/// operator that had already started, reached its database and reported
/// healthy. The cause was three layers away from the symptom.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ApiserverReachability {
    /// A service datapath (kube-proxy rules, or an equivalent) actually
    /// serves the cluster Service VIP, so the conventional coordinates work.
    ServiceVip {
        /// The `kubernetes` Service ClusterIP.
        ip: String,
        /// Its port (upstream's conventional 443).
        port: u16,
    },
    /// No service datapath, but the container runtime gives pods a route to
    /// the host the apiserver is bound on. Inject THAT instead — different
    /// coordinates, same apiserver, and it works.
    HostGateway {
        /// A name resolvable from inside a container.
        host: String,
        /// engenho's REAL listen port, not 443 — nothing rewrites it here.
        port: u16,
    },
    /// Neither is known to work. Inject nothing, so `Config::infer()` fails
    /// construction and falls back to a kubeconfig. Absent beats wrong.
    Unknown,
}

impl ApiserverReachability {
    /// The `(host, port)` to inject, or `None` to inject nothing at all.
    fn injectable(&self) -> Option<(String, u16)> {
        match self {
            Self::ServiceVip { ip, port } => Some((ip.clone(), *port)),
            Self::HostGateway { host, port } => Some((host.clone(), *port)),
            Self::Unknown => None,
        }
    }
}

/// The hostname podman resolves, inside every container, to the host running
/// the machine. Documented podman behaviour, and the only route from a pod to
/// a loopback-bound apiserver when the containers live in a VM.
const PODMAN_HOST_GATEWAY: &str = "host.containers.internal";

/// Decide what pods should be told, from what is actually true.
///
/// The service VIP is claimed ONLY when a datapath installs it. engenho
/// computes kube-proxy rules on darwin without installing them (see
/// `DatapathInstall::{Computed,Installed}`), so on that platform the VIP is a
/// bookkeeping entry and the host gateway is the truth.
fn apiserver_reachability(config: &EngenhoConfig) -> ApiserverReachability {
    // The port engenho really listens on. `listen_addr` is `host:port`; a
    // shape we cannot parse is a reason to inject nothing, never to guess.
    let Some(port) = config
        .runtime
        .listen_addr
        .rsplit(':')
        .next()
        .and_then(|p| p.parse::<u16>().ok())
    else {
        return ApiserverReachability::Unknown;
    };

    match config.runtime.kubelet_backend {
        // Pods run in podman. On darwin they are inside a VM and cannot reach
        // a host-loopback apiserver by any cluster address, and even on Linux
        // engenho installs no kube-proxy datapath for the VIP — so the
        // gateway is correct on both, and the VIP is correct on neither.
        CfgBackendKind::Podman => ApiserverReachability::HostGateway {
            host: PODMAN_HOST_GATEWAY.to_string(),
            port,
        },
        // Nothing is dialled under the fake backend, so there is nothing to
        // be reachable. Injecting the conventional VIP keeps the env shape
        // that tests assert on.
        CfgBackendKind::Fake => ApiserverReachability::ServiceVip {
            ip: DEFAULT_KUBERNETES_SERVICE_IP.to_string(),
            port: 443,
        },
    }
}

fn build_backend(config: &EngenhoConfig) -> Arc<dyn ContainerRuntime> {
    let kind = match config.runtime.kubelet_backend {
        CfgBackendKind::Podman => KubeletBackendKind::Podman,
        CfgBackendKind::Fake => KubeletBackendKind::Fake,
    };
    // A node-level fact, set once here rather than resolved per pod — and it
    // must reach the backend, because the kubelet has THREE `backend.start`
    // call sites and stamping any one of them misses the restart path.
    //
    // What is injected is a REACHABILITY CLAIM, not a constant: see
    // `ApiserverReachability`. `None` means "tell pods nothing", which is a
    // working outcome (kubeconfig fallback), not a degraded one.
    let reachability = apiserver_reachability(config);
    match &reachability {
        ApiserverReachability::ServiceVip { ip, port } => {
            info!(%ip, %port, "apiserver advertised to pods via the service VIP");
        }
        ApiserverReachability::HostGateway { host, port } => {
            info!(
                %host, %port,
                "apiserver advertised to pods via the container host gateway \
                 (no service datapath installs the cluster VIP)"
            );
        }
        ApiserverReachability::Unknown => {
            warn!(
                listen_addr = %config.runtime.listen_addr,
                "cannot determine an apiserver address pods can reach; \
                 injecting no KUBERNETES_SERVICE_* env so in-cluster config \
                 fails construction and clients fall back to a kubeconfig"
            );
        }
    }
    make_container_runtime_with_apiserver(
        kind,
        config.runtime.podman_binary.as_deref(),
        reachability.injectable(),
    )
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
/// Kubernetes' `kubernetes.io/arch` label speaks Go's `GOARCH`, not Rust's
/// `std::env::consts::ARCH`.
///
/// The two disagree on exactly the values that matter here: Rust says
/// `aarch64` and `x86_64` where Kubernetes says `arm64` and `amd64`. This
/// is the difference between a label that works and a label that looks
/// right and matches nothing — the reference pangea Postgres carries
/// `nodeSelector: {kubernetes.io/arch: arm64}`, and against an `aarch64`
/// label the scheduler's exact-match predicate leaves it
/// `NodeSelectorMismatch` **forever**, with a correct-looking label
/// visible in `kubectl get node -o yaml`.
///
/// Unknown architectures pass through verbatim rather than guessing: a
/// wrong-but-plausible label is worse than an unfamiliar one, because it
/// matches a selector that meant something else.
#[must_use]
fn kube_arch() -> &'static str {
    match std::env::consts::ARCH {
        "aarch64" => "arm64",
        "x86_64" => "amd64",
        "arm" => "arm",
        "powerpc64" => "ppc64le",
        "s390x" => "s390x",
        other => other,
    }
}

/// The `kubernetes.io/os` label, in Go's `GOOS` vocabulary.
///
/// Rust says `macos`; Kubernetes says `darwin`. Same failure mode as
/// [`kube_arch`].
#[must_use]
fn kube_os() -> &'static str {
    match std::env::consts::OS {
        "macos" => "darwin",
        other => other,
    }
}

/// The well-known labels upstream's kubelet self-applies at registration.
///
/// Without these, `metadata.labels` is ABSENT — not sparse — and
/// `matches_node_selector` is an exact-match AND over that map, so EVERY
/// `nodeSelector` key fails and every pod carrying one stays Pending
/// permanently. Measured on the live node 2026-08-30: `labels: None`.
///
/// The `beta.kubernetes.io/*` pair is deprecated upstream and still
/// emitted, because charts in the wild continue to select on it and a
/// missing label is a silent non-match rather than an error.
#[must_use]
fn well_known_node_labels(node_name: &str) -> serde_json::Value {
    serde_json::json!({
        "kubernetes.io/hostname": node_name,
        "kubernetes.io/os": kube_os(),
        "kubernetes.io/arch": kube_arch(),
        "beta.kubernetes.io/os": kube_os(),
        "beta.kubernetes.io/arch": kube_arch(),
    })
}

async fn register_node(store: &StoreMesh, node_name: &str) -> Result<(), RuntimeError> {
    let (cpu, memory) = host_capacity();
    let mut value = serde_json::json!({
        "kind": "Node",
        "apiVersion": "v1",
        "metadata": {
            "name": node_name,
            "labels": well_known_node_labels(node_name),
        },
        "spec": { "unschedulable": false },
        "status": {
            "capacity": { "cpu": cpu, "memory": memory },
            "allocatable": { "cpu": cpu, "memory": memory },
            "conditions": [{ "type": "Ready", "status": "True" }]
        }
    });
    // Route the self-registered Node through the SAME boundary stamp the
    // apiserver create path uses, so AGE works on the Node too. Frozen once
    // into the replicated Put.
    stamp_creation_timestamp_value(&mut value);
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

/// Inject `metadata.creationTimestamp` (if absent) into an opaque JSON
/// body from the typed RFC3339 boundary render — the non-handler `Put`
/// seeders (register_node) route through this so every born object,
/// including the self-registered Node, carries a real creationTimestamp.
/// Mirrors the apiserver handler's `stamp_creation_timestamp`.
fn stamp_creation_timestamp_value(body: &mut serde_json::Value) {
    if let Some(obj) = body.as_object_mut() {
        let metadata = obj
            .entry("metadata".to_string())
            .or_insert_with(|| serde_json::json!({}));
        if let Some(meta_obj) = metadata.as_object_mut() {
            let absent = !meta_obj.contains_key("creationTimestamp")
                || meta_obj.get("creationTimestamp") == Some(&serde_json::Value::Null);
            if absent {
                meta_obj.insert(
                    "creationTimestamp".to_string(),
                    serde_json::Value::String(engenho_types::time::now_rfc3339_utc()),
                );
            }
        }
    }
}

/// The RBAC group + version every seed key carries.
const RBAC_GROUP: &str = "rbac.authorization.k8s.io";
const RBAC_VERSION: &str = "v1";

/// Seed the bootstrap RBAC policy (Brick B). Idempotently `Put`s the canonical
/// bootstrap ClusterRoles + ClusterRoleBindings so:
///
///   * `system:masters` resolves `*.*` through a REAL binding (cluster-admin),
///     belt-and-suspenders behind the authorizer's short-circuit — so
///     `kubectl auth can-i --list` shows `*.*` via a binding too.
///   * anonymous + authenticated DISCOVERY (`/api`, `/apis`, `/openapi/v3`, …)
///     resolves through the `system:discovery` (authenticated) +
///     `system:public-info-viewer` (anonymous) bindings — TIER-2 reachability,
///     so kubectl's pre-auth discovery works without 403'ing.
///   * every authenticated user gets the minimal `system:basic-user` self-review
///     surface (selfsubject* creates).
///
/// Each seed is a TYPED Rust value (`ClusterRole`/`ClusterRoleBinding`) →
/// `serde_json::to_value` → `ResourceCommand::Put` (TYPED EMISSION — no `json!()`
/// of the policy bodies; only the Put envelope helper). Idempotent because Put
/// preserves `metadata.uid` across restarts, exactly like `register_node`.
/// The four namespaces every conformant control plane has at first boot.
///
/// Upstream's kube-apiserver creates these during bootstrap, and their absence
/// is not subtle: with zero namespaces `kubectl get ns` prints nothing, every
/// namespaced list is empty, and there is nowhere to schedule a workload. On
/// 2026-08-28 a live engenho served `{"items":[]}` from `/api/v1/namespaces`,
/// so k9s showed an empty screen — correctly, because the cluster genuinely
/// contained nothing.
///
/// * `default` — where an unqualified client request lands.
/// * `kube-system` — control-plane workloads.
/// * `kube-public` — world-readable cluster info.
/// * `kube-node-lease` — Node heartbeat Leases (`coordination.k8s.io`).
///
/// Idempotent across restarts for the same reason [`register_node`] is: the
/// apply path preserves `metadata.uid` and `creationTimestamp` on a Put over
/// an existing key, so re-seeding an unchanged namespace is a no-op rather
/// than a new object identity.
///
/// Each is built as a TYPED [`Namespace`] rather than a `json!()` literal, so
/// a field that does not exist is a compile error — the shape
/// `seed_bootstrap_rbac` established and the one `register_node` still
/// predates.
async fn seed_system_namespaces(store: &StoreMesh) -> Result<(), RuntimeError> {
    for name in SYSTEM_NAMESPACES {
        let ns = system_namespace(name);
        put_namespace(store, &ns).await?;
    }
    info!(
        count = SYSTEM_NAMESPACES.len(),
        "seeded system namespaces (default, kube-system, kube-public, kube-node-lease)"
    );
    Ok(())
}

/// The `kubernetes` Service in `default` — the in-cluster address of the
/// apiserver itself, and the object that RESERVES the first address of the
/// service CIDR.
///
/// Two defects in one, both measured 2026-08-28:
///
/// 1. The Service did not exist. `kubernetes.default.svc` is how an in-cluster
///    client reaches the apiserver; every client-go `InClusterConfig()` and
///    every ServiceAccount-mounted kubeconfig resolves it.
/// 2. Because it did not exist, the ClusterIP allocator handed **10.96.0.1**
///    — the address upstream reserves for exactly this Service — to the first
///    user Service that asked. A workload could take the apiserver's address.
///
/// The second is fixed *by* the first, with no allocator change, because the
/// allocator reseeds its in-use set from the live Service set on every
/// allocation ("the Services ARE the ledger", `cluster_ip.rs`). Seeding this
/// Service with the first host address makes every later allocation skip it by
/// construction rather than by a hardcoded exception — which is the difference
/// between a rule and a special case.
async fn seed_kubernetes_service(
    store: &StoreMesh,
    config: &EngenhoConfig,
) -> Result<(), RuntimeError> {
    // The CIDR may legitimately be empty (a control-plane-only node that
    // allocates no VIPs). Nothing to reserve, nothing to seed.
    if config.networking.service_cidr.is_empty() {
        return Ok(());
    }
    let mut allocator = match engenho_controllers::cluster_ip::ClusterIpAllocator::new(
        &config.networking.service_cidr,
    ) {
        Ok(a) => a,
        Err(e) => {
            // A malformed CIDR is the allocator's problem to report at its
            // own boundary, not a reason to refuse to boot the whole node.
            warn!(
                cidr = %config.networking.service_cidr,
                error = %e,
                "service_cidr unparseable; skipping the kubernetes Service seed"
            );
            return Ok(());
        }
    };
    let Ok(vip) = allocator.allocate() else {
        warn!("service CIDR has no assignable address; skipping the kubernetes Service seed");
        return Ok(());
    };

    let port = config
        .runtime
        .listen_addr
        .rsplit(':')
        .next()
        .and_then(|p| p.parse::<i64>().ok())
        .unwrap_or(6443);

    let value = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "kubernetes",
            "namespace": "default",
            "labels": { "component": "apiserver", "provider": "kubernetes" },
        },
        "spec": {
            "type": "ClusterIP",
            "clusterIP": vip,
            "clusterIPs": [vip],
            "ports": [{ "name": "https", "port": 443, "protocol": "TCP", "targetPort": port }],
            "sessionAffinity": "None",
        },
        "status": { "loadBalancer": {} }
    });
    let mut value = value;
    stamp_creation_timestamp_value(&mut value);
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::namespaced("", "v1", "Service", "default", "kubernetes"),
            value,
            expected: None,
            reason: Reason::Operator,
        })
        .await?;
    info!(%vip, port, "seeded the kubernetes Service (reserves the first service VIP)");
    Ok(())
}

/// The bootstrap namespace set, in creation order. A closed list: adding one
/// is a deliberate edit here, never a call site somewhere else.
const SYSTEM_NAMESPACES: &[&str] = &["default", "kube-system", "kube-public", "kube-node-lease"];

/// One system [`Namespace`], shaped exactly as the apiserver's own create path
/// shapes a namespace — because a direct store `Put` BYPASSES that path, and a
/// seeded namespace that differs from a client-created one is precisely the
/// kind of divergence a differential is built to catch.
///
/// Carries all three things upstream guarantees:
/// * the `kubernetes.io/metadata.name` label (upstream's NamespaceDefaultLabelName
///   admission plugin adds it; selectors in the wild rely on it),
/// * `spec.finalizers = ["kubernetes"]`, the namespace-controller's hook,
/// * `status.phase = "Active"`, which clients read to tell Active from Terminating.
fn system_namespace(name: &str) -> Namespace {
    let mut metadata = engenho_types::meta::ObjectMeta {
        name: name.to_string(),
        ..Default::default()
    };
    metadata
        .labels
        .insert("kubernetes.io/metadata.name".to_string(), name.to_string());
    Namespace {
        metadata,
        spec: Some(NamespaceSpec {
            finalizers: vec!["kubernetes".to_string()],
        }),
        status: Some(NamespaceStatus {
            phase: Some("Active".to_string()),
            ..Default::default()
        }),
    }
}

/// `Put` a typed [`Namespace`] (cluster-scoped) with `Reason::Operator`,
/// routed through the same boundary stamp every other seeder uses so the
/// object carries a real `creationTimestamp`.
async fn put_namespace(store: &StoreMesh, ns: &Namespace) -> Result<(), RuntimeError> {
    let mut value = serde_json::to_value(ns)
        .map_err(|e| RuntimeError::Server(seed_serialize_err("Namespace", &e)))?;
    stamp_creation_timestamp_value(&mut value);
    let name = ns.metadata.name.clone();
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::cluster_scoped("", "v1", "Namespace", name),
            value,
            expected: None,
            reason: Reason::Operator,
        })
        .await?;
    Ok(())
}

async fn seed_bootstrap_rbac(store: &StoreMesh) -> Result<(), RuntimeError> {
    // ── cluster-admin: full access to everything (resources + non-resource). ──
    let cluster_admin = ClusterRole {
        metadata: rbac_meta("cluster-admin"),
        rules: vec![
            PolicyRule {
                verbs: vec!["*".into()],
                api_groups: vec!["*".into()],
                resources: vec!["*".into()],
                ..Default::default()
            },
            PolicyRule {
                verbs: vec!["*".into()],
                non_resource_urls: vec!["*".into()],
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let cluster_admin_binding = ClusterRoleBinding {
        metadata: rbac_meta("cluster-admin"),
        role_ref: cluster_role_ref("cluster-admin"),
        subjects: vec![group_subject("system:masters")],
    };

    // ── system:discovery: GET the discovery + openapi non-resource URLs. ──
    let discovery = ClusterRole {
        metadata: rbac_meta("system:discovery"),
        rules: vec![PolicyRule {
            verbs: vec!["get".into()],
            non_resource_urls: vec![
                "/api".into(),
                "/api/*".into(),
                "/apis".into(),
                "/apis/*".into(),
                "/openapi".into(),
                "/openapi/*".into(),
                "/version".into(),
                "/version/*".into(),
                "/healthz".into(),
                "/livez".into(),
                "/readyz".into(),
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    let discovery_binding = ClusterRoleBinding {
        metadata: rbac_meta("system:discovery"),
        role_ref: cluster_role_ref("system:discovery"),
        subjects: vec![group_subject("system:authenticated")],
    };

    // ── system:basic-user: the minimal selfsubject* create surface. ──
    let basic_user = ClusterRole {
        metadata: rbac_meta("system:basic-user"),
        rules: vec![
            PolicyRule {
                verbs: vec!["create".into()],
                api_groups: vec!["authorization.k8s.io".into()],
                resources: vec![
                    "selfsubjectaccessreviews".into(),
                    "selfsubjectrulesreviews".into(),
                ],
                ..Default::default()
            },
            PolicyRule {
                verbs: vec!["create".into()],
                api_groups: vec!["authentication.k8s.io".into()],
                resources: vec!["selfsubjectreviews".into()],
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let basic_user_binding = ClusterRoleBinding {
        metadata: rbac_meta("system:basic-user"),
        role_ref: cluster_role_ref("system:basic-user"),
        subjects: vec![group_subject("system:authenticated")],
    };

    // ── system:public-info-viewer: GET health/version for ALL (incl. anon). ──
    let public_info = ClusterRole {
        metadata: rbac_meta("system:public-info-viewer"),
        rules: vec![PolicyRule {
            verbs: vec!["get".into()],
            non_resource_urls: vec![
                "/healthz".into(),
                "/livez".into(),
                "/readyz".into(),
                "/version".into(),
                "/version/*".into(),
                // Anonymous discovery: kubectl hits these before any
                // authenticated call; granting them to BOTH authenticated +
                // unauthenticated keeps the existing anonymous-kubeconfig path.
                "/api".into(),
                "/api/*".into(),
                "/apis".into(),
                "/apis/*".into(),
                "/openapi".into(),
                "/openapi/*".into(),
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    let public_info_binding = ClusterRoleBinding {
        metadata: rbac_meta("system:public-info-viewer"),
        role_ref: cluster_role_ref("system:public-info-viewer"),
        subjects: vec![
            group_subject("system:authenticated"),
            group_subject("system:unauthenticated"),
        ],
    };

    // Put each typed value. ClusterRole + ClusterRoleBinding are cluster-scoped.
    put_cluster_role(store, &cluster_admin).await?;
    put_cluster_role_binding(store, &cluster_admin_binding).await?;
    put_cluster_role(store, &discovery).await?;
    put_cluster_role_binding(store, &discovery_binding).await?;
    put_cluster_role(store, &basic_user).await?;
    put_cluster_role_binding(store, &basic_user_binding).await?;
    put_cluster_role(store, &public_info).await?;
    put_cluster_role_binding(store, &public_info_binding).await?;

    info!(
        "seeded bootstrap RBAC policy (cluster-admin + system:discovery + system:basic-user + system:public-info-viewer)"
    );
    Ok(())
}

/// Typed [`ObjectMeta`] carrying just a name — the shape every bootstrap RBAC
/// object needs. The `creationTimestamp` is stamped at the Put boundary
/// (`put_cluster_role` / `put_cluster_role_binding` route the serialized
/// value through `stamp_creation_timestamp_value`), so every seeded RBAC
/// object carries a real timestamp exactly like the apiserver create path.
fn rbac_meta(name: &str) -> engenho_types::meta::ObjectMeta {
    engenho_types::meta::ObjectMeta {
        name: name.to_string(),
        ..Default::default()
    }
}

/// A `RoleRef` pointing at a cluster-scoped `ClusterRole` by name.
fn cluster_role_ref(name: &str) -> RoleRef {
    RoleRef {
        api_group: RBAC_GROUP.to_string(),
        kind: "ClusterRole".to_string(),
        name: name.to_string(),
    }
}

/// A `Group` subject — the bootstrap bindings bind to groups
/// (`system:masters`, `system:authenticated`, `system:unauthenticated`).
fn group_subject(name: &str) -> Subject {
    Subject {
        kind: "Group".to_string(),
        api_group: Some(RBAC_GROUP.to_string()),
        name: name.to_string(),
        namespace: None,
    }
}

/// `Put` a typed `ClusterRole` (cluster-scoped) with `Reason::Operator`. The
/// body is serialized from the typed value (TYPED EMISSION); only the Put
/// envelope is hand-built.
async fn put_cluster_role(store: &StoreMesh, cr: &ClusterRole) -> Result<(), RuntimeError> {
    let mut value = serde_json::to_value(cr)
        .map_err(|e| RuntimeError::Server(seed_serialize_err("ClusterRole", &e)))?;
    stamp_creation_timestamp_value(&mut value);
    let name = cr.metadata.name.clone();
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::cluster_scoped(RBAC_GROUP, RBAC_VERSION, "ClusterRole", name),
            value,
            expected: None,
            reason: Reason::Operator,
        })
        .await?;
    Ok(())
}

/// `Put` a typed `ClusterRoleBinding` (cluster-scoped) with `Reason::Operator`.
async fn put_cluster_role_binding(
    store: &StoreMesh,
    crb: &ClusterRoleBinding,
) -> Result<(), RuntimeError> {
    let mut value = serde_json::to_value(crb)
        .map_err(|e| RuntimeError::Server(seed_serialize_err("ClusterRoleBinding", &e)))?;
    stamp_creation_timestamp_value(&mut value);
    let name = crb.metadata.name.clone();
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::cluster_scoped(RBAC_GROUP, RBAC_VERSION, "ClusterRoleBinding", name),
            value,
            expected: None,
            reason: Reason::Operator,
        })
        .await?;
    Ok(())
}

/// A serialize-failure during seeding (effectively impossible for the concrete
/// typed structs) becomes a typed apiserver ServerError so boot fails loudly —
/// never a silent skip.
fn seed_serialize_err(kind: &str, e: &serde_json::Error) -> engenho_apiserver::ServerError {
    engenho_apiserver::ServerError::Serve(std::io::Error::other(format!(
        "failed to serialize bootstrap {kind}: {e}"
    )))
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

/// Build the apiserver's authenticator chain, WITH `ServiceAccount` verification
/// whenever the cluster's SA signing key can be read.
///
/// ── ★ WHY THIS IS A NAMED FUNCTION AND NOT FOUR INLINE LINES ────────────────
/// The defect it closes was not a missing feature. `sa_token::verify`, the
/// `SaVerifier` and `ChainAuthenticator::bootstrap_with_sa` were all written,
/// tested and correct; the runtime called `bootstrap()` instead, which installs
/// the KEYLESS `ServiceAccountTokenAuthenticator::default()`. So a server fully
/// able to validate a `ServiceAccount` token answered every in-cluster client with
/// `401 service account token authentication is not yet supported`.
///
/// The real cost was downstream. In-cluster config could never work, so every
/// workload needing the API had to mount a kubeconfig carrying ADMIN client-key
/// material — cluster-admin credentials distributed to ordinary pods because the
/// pod's own identity was refused. Measured 2026-09-01 while bringing up the
/// pangea-operator stack, where it presented as a crashloop rather than as an
/// authentication gap.
///
/// Pulling it out of the boot sequence makes the wiring itself assertable, which
/// is the property that was missing: a capability reachable only through a call
/// site nobody audits is indistinguishable from one that was never built.
///
/// The key is read with the same idempotent `load_or_generate_sa_key` the pod
/// projector uses, so the token minted for a pod and the key checking it are the
/// same keypair by construction rather than by coordination. Issuer and audience
/// both come from `SA_ISSUER` for the same reason: split into two literals they
/// drift, and the failure is a 401 that looks like a key problem.
///
/// A key that cannot be read falls back to the KEYLESS chain, never a permissive
/// one — refusing to validate must not become refusing to reject.
fn build_authenticator(
    data_dir: &std::path::Path,
    admin_token: Option<String>,
) -> ChainAuthenticator {
    match engenho_apiserver::sa_token::load_or_generate_sa_key(data_dir) {
        Ok(kp) => {
            info!("ServiceAccount token authentication enabled");
            ChainAuthenticator::bootstrap_with_sa(
                admin_token,
                kp.verifying,
                SA_ISSUER.to_string(),
                SA_ISSUER.to_string(),
            )
        }
        Err(e) => {
            warn!(
                error = %e,
                "no ServiceAccount signing key; in-cluster clients will take a typed 401 and \
                 must fall back to a kubeconfig"
            );
            ChainAuthenticator::bootstrap(admin_token)
        }
    }
}

/// Every SAN the serving certificate must carry beyond the derived set: the
/// operator's declared list, plus the advertised address.
///
/// ── ★ THE ADVERTISED ADDRESS IS A SAN BY CONSTRUCTION ──────────────────────
/// `advertise_address` is the `server:` of the remote kubeconfig. A kubeconfig
/// naming an address the certificate does not is a file that cannot work, and
/// the failure surfaces only at whoever tries to use it, as a verification error
/// that reads like THEIR misconfiguration.
///
/// engenho has already paid for this once, in the other direction. After the
/// runtime began advertising `host.containers.internal` to pods, in-cluster
/// clients still failed with a connect error: the name now routed, and TLS
/// verification rejected it because the serving cert did not name it. Two
/// layers, one symptom, the second invisible from the first (see
/// `build_sans`'s note). That was fixed by adding the name to the derived list
/// and pinning the pair with a test.
///
/// Here the pair is not pinned after the fact — it cannot come apart, because
/// there is one field and both consumers read it. Advertising an address IS
/// naming it in the certificate.
///
/// Only the HOST is taken: a certificate names hosts, not ports, and the
/// advertised port is legitimately different from the bound one (a reverse
/// proxy, a tailnet forward, a NAT).
fn server_sans(config: &EngenhoConfig) -> Result<Vec<SanEntry>, RuntimeError> {
    let mut sans = parse_extra_sans(&config.runtime.tls.extra_sans)?;
    if let Some(host) = advertised_host(&config.runtime.advertise_address) {
        let entry = host
            .parse::<SanEntry>()
            .map_err(|source| RuntimeError::ExtraSan { source })?;
        if !sans.contains(&entry) {
            sans.push(entry);
        }
    }
    Ok(sans)
}

/// The host half of `advertise_address`, or `None` when nothing is advertised.
///
/// Accepts `host`, `host:port`, `[v6]:port` and a bare IPv6 literal. The port is
/// dropped: it is meaningful for the kubeconfig's URL and meaningless in a
/// certificate.
fn advertised_host(advertise: &str) -> Option<&str> {
    let value = advertise.trim();
    if value.is_empty() {
        return None;
    }
    // A bracketed IPv6 literal, with or without a port.
    if let Some(rest) = value.strip_prefix('[') {
        return rest.split(']').next().filter(|h| !h.is_empty());
    }
    // A bare IPv6 literal has several colons and no port; anything with exactly
    // one colon is host:port.
    if value.matches(':').count() == 1 {
        return value.split(':').next().filter(|h| !h.is_empty());
    }
    Some(value)
}

/// The `server:` URL remote clients are handed, or `None` when this apiserver is
/// node-local.
///
/// The port defaults to the bound one, so an operator who only needs to name a
/// host does not have to restate a port that is already declared — and cannot
/// restate it wrongly.
fn advertised_server_url(config: &EngenhoConfig, bound: SocketAddr) -> Option<String> {
    let value = config.runtime.advertise_address.trim();
    if value.is_empty() {
        return None;
    }
    let has_port = if value.starts_with('[') {
        value.rsplit(']').next().is_some_and(|t| t.starts_with(':'))
    } else {
        value.matches(':').count() == 1
    };
    if has_port {
        Some(format!("https://{value}"))
    } else {
        // Bracket a bare IPv6 literal so the URL is well-formed.
        let host = if value.parse::<std::net::Ipv6Addr>().is_ok() {
            format!("[{value}]")
        } else {
            value.to_string()
        };
        Some(format!("https://{host}:{}", bound.port()))
    }
}

/// Classify every operator-declared SAN string, failing on the first bad one.
///
/// Fails rather than skipping. A skipped SAN produces exactly the certificate
/// the operator was trying to avoid, and produces it silently — the whole point
/// of the field is that the cert names something the runtime cannot derive, so
/// dropping the entry defeats it while looking like success.
fn parse_extra_sans(raw: &[String]) -> Result<Vec<SanEntry>, RuntimeError> {
    raw.iter()
        .map(|s| s.parse::<SanEntry>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| RuntimeError::ExtraSan { source })
}

/// Write `data_dir/kubeconfig` (mode 0600) so an operator can immediately
/// `kubectl --kubeconfig <data_dir>/kubeconfig get nodes`. server_url is
/// `https://127.0.0.1:<bound_port>` (loopback SAN + the real bound port);
/// `ca_pem` is the cluster CA the server cert chains to.
///
/// When `admin` is supplied, the kubeconfig embeds the admin CLIENT CERT (→
/// `kubectl auth whoami` = engenho-admin / system:masters); otherwise it falls
/// back to the anonymous-token kubeconfig (the plaintext / no-admin path).
///
/// The admin form embeds `client-key-data` — the admin private key, verbatim —
/// so the file IS a credential and is owner-only. The operator reaching for
/// `kubectl --kubeconfig` is the owner, so 0600 costs that path nothing. The
/// anonymous form carries only a public placeholder token, but it is written
/// through the same path at the same mode rather than branching: one mode for
/// one filename means the admin case cannot inherit the laxer one.
fn write_boot_kubeconfig(
    config: &EngenhoConfig,
    bound_addr: SocketAddr,
    ca_pem: &str,
    admin: Option<&ClientMaterial>,
) -> Result<(), RuntimeError> {
    // Loopback server URL with the actually-bound port (handles `:0`).
    let server_url = loopback_server_url(bound_addr);
    let yaml = match admin {
        Some(admin) => emit_kubeconfig_with_admin(
            &config.cluster.name,
            &server_url,
            ca_pem.as_bytes(),
            admin.cert_pem.as_bytes(),
            admin.key_pem.as_bytes(),
        ),
        None => emit_kubeconfig(&config.cluster.name, &server_url, ca_pem.as_bytes()),
    }
    .map_err(|e| RuntimeError::Kubeconfig(e.to_string()))?;
    let path = config.runtime.data_dir.join("kubeconfig");
    write_kubeconfig_file(&path, &yaml)?;
    info!(path = %path.display(), server = %server_url, admin = admin.is_some(), "kubeconfig written");

    // ── ★ ALSO PUBLISH WHERE ORDINARY TOOLING ACTUALLY LOOKS ──────────
    // The `data_dir` copy above is self-contained and nothing reads it:
    // kubectl, k9s and flux resolve through `$KUBECONFIG`, which the fleet
    // composes from `~/.kube/configs/*` via the typed `pleme.kubeconfigs`
    // list (nix: `modules/shared/kubeconfig-paths.nix`). Publishing here is
    // what makes "every node has its own engenho and the tools just work"
    // true without an operator copying a file.
    //
    // A publish FAILURE is deliberately not fatal. The daemon is already
    // serving; refusing to boot because `$HOME` is read-only (or absent, as
    // under launchd) would trade a working cluster for a missing
    // convenience. It is logged at WARN so the reason is visible.
    // ── ★ AND A POD-FACING ONE, IF ASKED FOR ──────────────────────────
    // Same credentials, DIFFERENT server address: the one pods can reach.
    // Derived from `apiserver_reachability` rather than written again, so the
    // address a workload dials cannot disagree with the address engenho tells
    // it to dial.
    //
    // This exists because in-cluster config does not work here: engenho
    // projects a valid ServiceAccount token and its own authenticator returns
    // `service account token authentication is not yet supported` (401). Until
    // that lands, a workload needing the API needs a kubeconfig, and engenho
    // is the only thing that holds both the admin cert and the right address.
    if let Some(pod_publish) = resolve_publish_path(&config.runtime.pod_kubeconfig_publish_path) {
        match apiserver_reachability(config).injectable() {
            Some((host, port)) => {
                let pod_server = format!("https://{host}:{port}");
                let pod_yaml = match admin {
                    Some(admin) => emit_kubeconfig_with_admin(
                        &config.cluster.name,
                        &pod_server,
                        ca_pem.as_bytes(),
                        admin.cert_pem.as_bytes(),
                        admin.key_pem.as_bytes(),
                    ),
                    None => emit_kubeconfig(&config.cluster.name, &pod_server, ca_pem.as_bytes()),
                }
                .map_err(|e| RuntimeError::Kubeconfig(e.to_string()))?;
                match write_kubeconfig_file(&pod_publish, &pod_yaml) {
                    Ok(()) => info!(
                        path = %pod_publish.display(), server = %pod_server,
                        "pod-facing kubeconfig published"
                    ),
                    Err(e) => tracing::warn!(
                        path = %pod_publish.display(), error = %e,
                        "pod-facing kubeconfig publish failed — the daemon is serving"
                    ),
                }
            }
            // No reachable address is known, so there is no honest server URL
            // to write. Writing one anyway would hand a workload a kubeconfig
            // that cannot connect, which is the failure this whole path exists
            // to remove.
            None => tracing::warn!(
                "pod-facing kubeconfig requested but no pod-reachable apiserver \
                 address is known; writing nothing rather than an unusable file"
            ),
        }
    }

    // ── ★ AND A REMOTE ONE, FOR OPERATORS ON ANOTHER MACHINE ──────────
    // The third audience. The `data_dir` copy and `kubeconfig_publish_path`
    // both carry a LOOPBACK server url — correct on this node, useless from any
    // other — and the pod-facing copy carries the address CONTAINERS reach.
    // Nobody served the operator sitting at a different workstation, so
    // distributing access meant copying a kubeconfig and hand-editing its
    // `server:` line, which is exactly the edit that silently disagrees with
    // the serving certificate.
    //
    // The address is not re-derived here: it is `advertise_address`, the same
    // field `server_sans` turns into a certificate SAN. One field, both
    // consumers — so a kubeconfig naming an address the cert does not is
    // unconstructible rather than merely tested for.
    if let Some(remote_publish) =
        resolve_publish_path(&config.runtime.remote_kubeconfig_publish_path)
    {
        if let Some(remote_server) = advertised_server_url(config, bound_addr) {
            let remote_yaml = match admin {
                Some(admin) => emit_kubeconfig_with_admin(
                    &config.cluster.name,
                    &remote_server,
                    ca_pem.as_bytes(),
                    admin.cert_pem.as_bytes(),
                    admin.key_pem.as_bytes(),
                ),
                None => emit_kubeconfig(&config.cluster.name, &remote_server, ca_pem.as_bytes()),
            }
            .map_err(|e| RuntimeError::Kubeconfig(e.to_string()))?;
            match write_kubeconfig_file(&remote_publish, &remote_yaml) {
                Ok(()) => info!(
                    path = %remote_publish.display(), server = %remote_server,
                    "remote kubeconfig published"
                ),
                Err(e) => tracing::warn!(
                    path = %remote_publish.display(), error = %e,
                    "remote kubeconfig publish failed — the daemon is serving"
                ),
            }
        } else {
            // Asked for a remote kubeconfig without saying what address is
            // remote. Writing a loopback url under that name would be worse
            // than writing nothing: it produces a file that looks like remote
            // access and silently is not.
            tracing::warn!(
                "remote_kubeconfig_publish_path is set but advertise_address is empty; \
                 writing nothing rather than a file with a loopback server url"
            );
        }
    }

    if let Some(publish) = resolve_publish_path(&config.runtime.kubeconfig_publish_path) {
        match write_kubeconfig_file(&publish, &yaml) {
            Ok(()) => {
                info!(path = %publish.display(), "kubeconfig published for kubectl/k9s/flux");
            }
            Err(e) => {
                tracing::warn!(
                    path = %publish.display(),
                    error = %e,
                    "kubeconfig publish failed — the daemon is serving; \
                     `$KUBECONFIG` will not see this cluster until the path is writable"
                );
            }
        }
    }
    Ok(())
}

/// Expand the configured publish path, or `None` when publishing is off.
///
/// Handles a leading `~/` because the default is written as a portable
/// string in config (`~/.kube/configs/engenho`) rather than a resolved
/// path — the config layer must stay a pure value with no `$HOME` baked
/// into it, or a rendered config would only be valid for the user who
/// generated it.
fn resolve_publish_path(raw: &str) -> Option<std::path::PathBuf> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    if let Some(rest) = raw.strip_prefix("~/") {
        // `HOME` unset (some launchd contexts) means there is no home to
        // publish into — skip rather than write to a relative path that
        // would land wherever the daemon happens to be running.
        let home = std::env::var("HOME").ok().filter(|h| !h.is_empty())?;
        return Some(std::path::PathBuf::from(home).join(rest));
    }
    Some(std::path::PathBuf::from(raw))
}

/// Persist the admin client cert + key under `data_dir/pki/` (cert 0644, key
/// 0600) so the operator's kubeconfig has a STABLE admin credential across
/// boots (matches the server-cert / CA persistence shape).
fn persist_admin_material(
    data_dir: &std::path::Path,
    admin: &ClientMaterial,
) -> Result<(), RuntimeError> {
    let pki = data_dir.join("pki");
    create_pki_dir(&pki)?;
    write_at_mode(&pki.join("admin.crt"), &admin.cert_pem, 0o644)?;
    write_at_mode(&pki.join("admin.key"), &admin.key_pem, 0o600)?;
    Ok(())
}

/// Load-or-generate the bootstrap admin BEARER token, persisted at
/// `data_dir/pki/admin.token` (0600). Restart-stable: an already-distributed
/// `Authorization: Bearer <token>` keeps working across reboots. The token is
/// 32 random bytes hex-encoded (no external crate — uses `getrandom` via
/// `rand`-free `std`-adjacent entropy from the OS).
fn load_or_generate_admin_token(data_dir: &std::path::Path) -> Result<String, RuntimeError> {
    let pki = data_dir.join("pki");
    let token_path = pki.join("admin.token");
    if let Ok(existing) = std::fs::read_to_string(&token_path) {
        let trimmed = existing.trim().to_string();
        if !trimmed.is_empty() {
            return Ok(trimmed);
        }
    }
    create_pki_dir(&pki)?;
    let token = random_admin_token();
    write_at_mode(&token_path, &token, 0o600)?;
    Ok(token)
}

/// Generate a 32-byte random admin token as a 64-char lowercase hex string,
/// seeded from OS entropy (`getrandom`). On the (effectively impossible) OS
/// entropy failure, fall back to a process-+time-derived value so boot never
/// hard-fails on the secret-mint path (the token is still 32 bytes; it just
/// isn't CSPRNG-grade in that degenerate case — logged is acceptable for a
/// single-node bootstrap admin token).
fn random_admin_token() -> String {
    let mut bytes = [0u8; 32];
    if getrandom::fill(&mut bytes).is_err() {
        // Degenerate fallback: mix process id + nanos. Never expected.
        let pid = std::process::id().to_le_bytes();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
            .to_le_bytes();
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = pid[i % pid.len()] ^ nanos[i % nanos.len()] ^ (i as u8);
        }
    }
    let mut out = String::with_capacity(64);
    for b in bytes {
        out.push(char::from_digit(u32::from(b >> 4), 16).unwrap());
        out.push(char::from_digit(u32::from(b & 0xf), 16).unwrap());
    }
    out
}

/// Create `data_dir/pki` at 0700 — it holds the admin private key and the
/// admin bearer token, and a 0600 file inside a 0755 directory is still
/// listable. Matches the mode `engenho-apiserver`'s PKI loader already gives
/// the same directory, whichever of the two reaches it first.
#[cfg(all(unix, feature = "with-cofre"))]
fn create_pki_dir(pki: &std::path::Path) -> Result<(), RuntimeError> {
    cofre_fs::create_secret_dir(pki, 0o700).map_err(|source| RuntimeError::KubeconfigIo {
        path: pki.to_path_buf(),
        source,
    })
}

/// Unix without cofre: create dir then set mode via chmod.
#[cfg(all(unix, not(feature = "with-cofre")))]
fn create_pki_dir(pki: &std::path::Path) -> Result<(), RuntimeError> {
    std::fs::create_dir_all(pki).map_err(|source| RuntimeError::KubeconfigIo {
        path: pki.to_path_buf(),
        source,
    })?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(pki, std::fs::Permissions::from_mode(0o700)).map_err(|source| {
        RuntimeError::KubeconfigIo {
            path: pki.to_path_buf(),
            source,
        }
    })
}

/// Non-unix: no modes to set.
#[cfg(not(unix))]
fn create_pki_dir(pki: &std::path::Path) -> Result<(), RuntimeError> {
    std::fs::create_dir_all(pki).map_err(|source| RuntimeError::KubeconfigIo {
        path: pki.to_path_buf(),
        source,
    })
}

/// Create `path` holding `contents` with exactly `mode`, set by `open(2)`
/// itself rather than by a follow-up `chmod`.
///
/// `cofre_fs::write_secret` owns that property: the bits land in the syscall
/// that creates the inode, so there is no interval during which the admin key
/// or the bearer token is 0644-and-world-readable, and `create_new` after an
/// unlink means a pre-placed symlink is not written through. The mode is a
/// required argument there, which is why it stays one here.
#[cfg(all(unix, feature = "with-cofre"))]
fn write_at_mode(path: &std::path::Path, contents: &str, mode: u32) -> Result<(), RuntimeError> {
    cofre_fs::write_secret(path, contents.as_bytes(), mode).map_err(|source| {
        RuntimeError::KubeconfigIo {
            path: path.to_path_buf(),
            source,
        }
    })
}

/// Unix without cofre: write then chmod. Has a brief window at default perms.
#[cfg(all(unix, not(feature = "with-cofre")))]
fn write_at_mode(path: &std::path::Path, contents: &str, mode: u32) -> Result<(), RuntimeError> {
    std::fs::write(path, contents.as_bytes()).map_err(|source| RuntimeError::KubeconfigIo {
        path: path.to_path_buf(),
        source,
    })?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).map_err(|source| {
        RuntimeError::KubeconfigIo {
            path: path.to_path_buf(),
            source,
        }
    })
}

/// Non-unix: file modes don't apply, so this is a plain write. cofre-fs is
/// `#![cfg(unix)]` for the same reason.
#[cfg(not(unix))]
fn write_at_mode(path: &std::path::Path, contents: &str, _mode: u32) -> Result<(), RuntimeError> {
    std::fs::write(path, contents.as_bytes()).map_err(|source| RuntimeError::KubeconfigIo {
        path: path.to_path_buf(),
        source,
    })
}

/// `https://127.0.0.1:<port>` — the loopback URL kubectl targets. We use
/// loopback (not the bound IP) because `127.0.0.1` is always a server-cert
/// SAN, so the kubeconfig is usable even when `listen_addr` is `0.0.0.0`.
fn loopback_server_url(bound_addr: SocketAddr) -> String {
    let mut url = String::from("https://127.0.0.1:");
    url.push_str(&bound_addr.port().to_string());
    url
}

/// Write the kubeconfig at mode 0600 (see [`write_boot_kubeconfig`] for why
/// owner-only). Creates the parent dir if missing (it normally exists — the
/// durable store already opened `data_dir/store`); the parent is `data_dir`
/// itself, which holds non-secret state too, so its mode is left alone.
fn write_kubeconfig_file(path: &std::path::Path, contents: &str) -> Result<(), RuntimeError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| RuntimeError::KubeconfigIo {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    write_at_mode(path, contents, 0o600)
}

/// Build + spawn every driver gated on `controllers.enable.*`. The
/// scheduler + kubelet always run (a single-node runtime that can't
/// schedule or run containers is useless); the four reconcilers are
/// individually toggleable.
///
/// Returns `(handles, kubelet)` — the spawned driver tasks PLUS an
/// `Arc<Kubelet>` clone. The kubelet is built once, shared (via the
/// `Controller for Arc<C>` blanket impl) between its WatchDriver AND the
/// apiserver's Pod `/log` reader, so the driver's ticks + the log queries see
/// the SAME local bookkeeping.
/// Where CNI network configuration lives. Upstream's path, and not
/// configurable today on purpose: every CNI installer writes here, and a
/// configurable directory that nobody sets is a knob whose only effect is
/// to let one operator point engenho at an empty dir by accident.
const CNI_CONFIG_DIR: &str = "/etc/cni/net.d";

/// Whether this build can execute CNI plugins.
///
/// ★ A COMPILE-TIME CONSTANT, NOT A RUNTIME PROBE. There is no network
/// namespace on darwin, so `CNI_NETNS` cannot be satisfied and no
/// conformant plugin can run — that is a property of the target, and
/// deciding it at runtime would mean a darwin build carries a code path
/// that can never be correct on it.
#[cfg(target_os = "linux")]
const CNI_INSTALL: engenho_cni::exec::CniInstall = engenho_cni::exec::CniInstall::Invoked;
#[cfg(not(target_os = "linux"))]
const CNI_INSTALL: engenho_cni::exec::CniInstall = engenho_cni::exec::CniInstall::Planned;

fn spawn_drivers(
    config: &EngenhoConfig,
    store: &Arc<StoreMesh>,
    backend: &Arc<dyn ContainerRuntime>,
    strategy: Box<dyn engenho_scheduler::SchedulingStrategy>,
    handler_sink: &Arc<dyn DynamicHandlerSink>,
) -> (Vec<JoinHandle<()>>, Arc<Kubelet>) {
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

    // ★ ONE CSI driver table, shared by three consumers: the registrar
    // fills it, the PV binder provisions through it, and the kubelet's
    // materializer publishes through it. Two tables would let a driver be
    // provisionable but not mountable — a PVC that binds and then never
    // mounts, with nothing anywhere explaining the difference.
    let csi_drivers = engenho_kubelet::DriverTable::new();

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
    if enable.daemonset {
        let c = DaemonSetController::new(store.clone(), ns.clone());
        handles.push(
            WatchDriver::new(
                c,
                store.clone(),
                driver_config(&["DaemonSet", "Pod", "Node"]),
            )
            .spawn(),
        );
    }
    if enable.job {
        let c = JobController::new(store.clone(), ns.clone());
        handles.push(WatchDriver::new(c, store.clone(), driver_config(&["Job", "Pod"])).spawn());
    }
    // CronJob: parses spec.schedule (5-field cron) against the WallClock and
    // creates a batch/v1 Job from the jobTemplate on schedule; the
    // JobController above then runs that Job's Pods. Watches CronJob (its own
    // kind) + Job (to observe owned-Job activity for the concurrency policy);
    // the fallback tick is what actually drives the time-based firing (a
    // CronJob has no spec edit each minute to wake a pure event watch).
    if enable.cronjob {
        let c = CronJobController::new(store.clone(), Arc::new(WallClock), ns.clone());
        handles
            .push(WatchDriver::new(c, store.clone(), driver_config(&["CronJob", "Job"])).spawn());
    }
    if enable.pdb {
        let c = PodDisruptionBudgetController::new(store.clone(), ns.clone());
        handles.push(
            WatchDriver::new(
                c,
                store.clone(),
                driver_config(&["PodDisruptionBudget", "Pod"]),
            )
            .spawn(),
        );
    }
    if enable.endpoints {
        let c = EndpointsController::new(store.clone(), ns.clone());
        handles.push(
            WatchDriver::new(
                c,
                store.clone(),
                driver_config(&["Service", "Pod", "Endpoints", "EndpointSlice"]),
            )
            .spawn(),
        );
    }
    // Service routing: resolves Service + Endpoints → typed ServiceRoutes
    // and drives the platform-selected datapath backend. The backend is
    // chosen by `networking.datapath_mode` resolved against the host
    // platform (Auto → iptables on Linux, compute-only off-Linux) so on a
    // Darwin dev host the controller still runs + computes + observes the
    // desired rules without ever shelling to a non-existent
    // `iptables-restore`. Watches the same kinds the EndpointsController
    // does (Service + Endpoints/EndpointSlice) plus the fallback tick.
    if enable.service_routing {
        let resolved = config
            .networking
            .datapath_mode
            .resolve(cfg!(target_os = "linux"));
        let backend = make_service_router(resolved);
        info!(
            datapath = backend.name(),
            mode = ?config.networking.datapath_mode,
            "service routing backend selected"
        );
        let c = ServiceRoutingController::new(store.clone(), backend, ns.clone());
        handles.push(
            WatchDriver::new(
                c,
                store.clone(),
                driver_config(&["Service", "Endpoints", "EndpointSlice"]),
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

    // Namespace: cascade-deletion of a Terminating namespace's contents +
    // finalizer clear. Watches the cluster-scoped Namespace kind (KindFilter
    // matches on ev.key.kind, so cluster-scoped works) PLUS the fallback tick
    // so a Terminating namespace drains over a few reconcile cycles. Mirrors
    // the gc block; gated on controllers.enable.namespace.
    if enable.namespace {
        let c = NamespaceController::new(store.clone(), ns.clone());
        handles.push(WatchDriver::new(c, store.clone(), driver_config(&["Namespace"])).spawn());
    }

    // PV/PVC binder: binds Pending PersistentVolumeClaims to matching
    // Available PersistentVolumes (capacity ≥ request, accessModes ⊇ requested,
    // storageClassName equal, volumeName pre-bind) and dynamically provisions a
    // node-local hostPath PV (under data_dir/local-path) via the local-path
    // provisioner / default StorageClass when no static PV matches. Watches the
    // three storage kinds; the fallback tick covers a PV/SC that appears after
    // the PVC (and vice versa). The host effect (mkdir of the local-path
    // backing dir) is the HostProvisionerEnv default; gated on
    // controllers.enable.pv_binder.
    if enable.pv_binder {
        let local_path_root = config
            .runtime
            .data_dir
            .join("local-path")
            .to_string_lossy()
            .into_owned();
        // Declared here so both the binder below and the kubelet further
        // down share it. `DriverTable` is an Arc inside, so a clone is the
        // same table.
        // The CSI plane: ONE driver table shared by the registrar (which
        // fills it), the provisioner (CreateVolume) and the materializer
        // (NodePublishVolume). Two tables would let a driver be
        // provisionable but not mountable, or the reverse.
        let c =
            PvBinderController::new(store.clone(), ns.clone(), local_path_root).with_csi(Arc::new(
                engenho_kubelet::DriverCsiProvisioner::new(csi_drivers.clone()),
            ));
        handles.push(
            WatchDriver::new(
                c,
                store.clone(),
                driver_config(&["PersistentVolumeClaim", "PersistentVolume", "StorageClass"]),
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

    // Served-capability honesty: stamps truthful status conditions on the
    // kinds engenho advertises in discovery but does not implement
    // (APIService, FlowSchema, PriorityLevelConfiguration). Without this
    // the module is a well-tested vocabulary nobody emits, and an
    // aggregated APIService registers successfully while every request to
    // its group silently goes nowhere.
    {
        let c =
            engenho_controllers::served_capability::ServedCapabilityController::new(store.clone());
        handles.push(
            WatchDriver::new(
                c,
                store.clone(),
                driver_config(&["APIService", "FlowSchema", "PriorityLevelConfiguration"]),
            )
            .spawn(),
        );
    }

    // The event sink, built once and shared by every producer below. It
    // lives here rather than inside the kubelet block because the
    // NetworkPolicy controller needs it too, and two sinks over one store
    // would be two independent lossy buffers for one cluster's events.
    let events: Arc<dyn engenho_controllers::event_recorder::EventSink> = Arc::new(
        engenho_controllers::event_recorder::StoreEventSink::new(Arc::new(MeshEventStore {
            store: store.clone(),
        })),
    );

    // NetworkPolicy: translate every policy into enforcer rules AND record
    // whether they are actually enforced. Wired HERE, at assembly, for the
    // same reason the event sink is: this is the only layer holding both
    // the store and the enforcer. Without it a default-deny policy applies
    // cleanly and restricts nothing, with no object anywhere saying so —
    // the one gap in this codebase where silence is a SECURITY claim.
    //
    // The backend is `ComputedNetworkPolicyEnforcer` because engenho runs
    // on darwin with pods in a podman VM: there is no kernel here to
    // install a filter into. It is a deliberate, named, operator-visible
    // state, not a stub — see `PolicyDatapath`.
    {
        let enforcer =
            Arc::new(engenho_controllers::network_policy::ComputedNetworkPolicyEnforcer::new());
        let c = engenho_controllers::network_policy_controller::NetworkPolicyController::new(
            store.clone(),
            enforcer,
        )
        .with_event_sink(events.clone());
        handles.push(WatchDriver::new(c, store.clone(), driver_config(&["NetworkPolicy"])).spawn());
    }

    // CSI registration: scan `<kubelet-root>/plugins_registry` and keep the
    // driver table in sync. Without this the whole CSI plane is inert — a
    // driver deploys, creates its sockets, and nothing ever dials them.
    {
        let c = engenho_kubelet::CsiRegistrarController::new(
            &config.runtime.data_dir,
            csi_drivers.clone(),
        );
        handles.push(
            WatchDriver::new(
                c,
                store.clone(),
                WatchDriverConfig {
                    // Registration is a filesystem event, not a store one,
                    // so this rides the FALLBACK tick alone rather than
                    // waking on writes it can never be caused by.
                    filter: KindFilter::Kinds(vec!["CSINode".to_string()]),
                    debounce,
                    fallback_interval: fallback,
                },
            )
            .spawn(),
        );
    }

    // CNI status: publish which network config this node actually resolved
    // and whether its plugin chain is executed or merely planned. On darwin
    // the answer is `Planned` — the pod address comes from podman, not from
    // IPAM — and nothing else in the cluster distinguishes the two.
    {
        let c = engenho_controllers::cni_status::CniStatusController::new(
            store.clone(),
            config.runtime.node_name.clone(),
            std::path::PathBuf::from(CNI_CONFIG_DIR),
            CNI_INSTALL,
        );
        handles.push(WatchDriver::new(c, store.clone(), driver_config(&["Node"])).spawn());
    }

    // Kubelet: bound Pod → container via the backend. Watches Pods. Built
    // ONCE as an Arc<Kubelet> so the SAME instance is shared between its
    // WatchDriver (via the `Controller for Arc<C>` blanket impl) and the
    // apiserver's Pod `/log` reader — both see one local bookkeeping map.
    // The event sink is wired HERE, at assembly, because this is the only
    // layer that has both the kubelet and the store. Without it the kubelet
    // keeps its NullEventSink and the cluster cannot explain itself: that is
    // precisely the state in which a pod reached 149 restarts and `kubectl
    // describe` had nothing to say about it.
    // The CSI node path is layered ON the podman materializer rather than
    // replacing it: configMap / secret / emptyDir keep the behaviour that
    // took several passes to get right, and CSI is two additional methods.
    let csi_materializer: Arc<dyn engenho_kubelet::VolumeMaterializer> =
        Arc::new(engenho_kubelet::CsiVolumeMaterializer::new(
            Arc::new(engenho_kubelet::PodmanVolumeMaterializer::new()),
            csi_drivers.clone(),
            config.runtime.data_dir.clone(),
        ));
    // The pod ServiceAccount projection. Built here because this is the only
    // layer holding both the signing key and the kubelet.
    //
    // A missing key or CA yields the NO-PROJECTION default rather than an
    // empty token: a zero-byte token file is worse than an absent one,
    // because the client stops looking for a kubeconfig and then fails
    // authentication instead of falling back.
    // Both loaders are idempotent (`load_or_generate_*`), so reading them
    // here rather than threading them through costs one file read and keeps
    // the identity plumbing in the layer that uses it.
    let sa_key = engenho_apiserver::sa_token::load_or_generate_sa_key(&config.runtime.data_dir)
        .map_err(|e| warn!(error = %e, "no SA signing key; pods get no ServiceAccount projection"))
        .ok();
    let ca_pem_for_sa = engenho_apiserver::load_or_generate_ca(&config.runtime.data_dir)
        .map(|ca| ca.cert_pem().to_string())
        .map_err(|e| warn!(error = %e, "no cluster CA; pods get no ServiceAccount projection"))
        .ok();
    let sa_projector: Arc<dyn engenho_kubelet::ServiceAccountProjector> =
        match (sa_key.as_ref(), ca_pem_for_sa.as_ref()) {
            (Some(kp), Some(ca)) => Arc::new(RuntimeSaProjector {
                signing: kp.signing.clone(),
                issuer: SA_ISSUER.to_string(),
                audience: SA_ISSUER.to_string(),
                ca_cert_pem: ca.clone(),
                // One hour, matching upstream's default bound-token lifetime.
                // A pod does not re-read its token file, so this is currently
                // a ceiling on pod lifetime for API-calling workloads — the
                // refresh loop is the follow-up this does not ship.
                lifetime_secs: 3600,
            }),
            _ => Arc::new(engenho_kubelet::NoServiceAccountProjection),
        };

    let kubelet = Arc::new(
        Kubelet::new(
            store.clone(),
            backend.clone(),
            config.runtime.node_name.clone(),
        )
        .with_event_sink(events)
        .with_sa_projector(sa_projector)
        .with_volume_materializer(csi_materializer),
    );
    handles.push(WatchDriver::new(kubelet.clone(), store.clone(), driver_config(&["Pod"])).spawn());

    (handles, kubelet)
}

/// Construct the `ServiceRouter` backend for a resolved datapath choice.
///
/// Typed dispatch — `ResolvedDatapath` (the pure output of
/// `DatapathMode::resolve`) maps 1:1 to a backend: `Iptables`/`Ipvs` are
/// the kernel backends (a Linux node installs the VIP datapath), and
/// `ComputeOnly` is the `FakeRouter` (routes computed + observable, nothing
/// installed in the kernel — the fail-safe off-Linux dev path, surfacing
/// `DatapathInstall::Computed`). No `cfg!` lives here: the platform was
/// already folded into `resolved` by the caller, so this stays directly
/// testable.
fn make_service_router(resolved: ResolvedDatapath) -> Arc<dyn ServiceRouter> {
    match resolved {
        ResolvedDatapath::Iptables => Arc::new(IptablesRouter::new()),
        ResolvedDatapath::Ipvs => Arc::new(IpvsRouter::new()),
        ResolvedDatapath::ComputeOnly => Arc::new(FakeRouter::new()),
    }
}

// `make_scheduling_strategy` returns `Result<Box<dyn SchedulingStrategy>,
// SchedulerError>`; the boxed strategy is unwrapped fallibly in
// `start_inner` (typed error for unimplemented strategies) and handed to
// `spawn_drivers`. `Scheduler::new<S: SchedulingStrategy + 'static>`
// accepts the box (Box<dyn Trait> implements Trait via the blanket impl).

#[cfg(test)]
mod node_label_tests {
    use super::{kube_arch, kube_os, well_known_node_labels};

    /// The labels speak Go's vocabulary, not Rust's.
    ///
    /// This is the whole point of the mapping: the reference pangea
    /// Postgres selects `kubernetes.io/arch: arm64`, and Rust's
    /// `std::env::consts::ARCH` is `aarch64` on the same machine. Emitting
    /// the Rust spelling yields a label that reads correctly in
    /// `kubectl get node -o yaml` and matches no selector anyone writes.
    #[test]
    fn labels_use_go_vocabulary_not_rust() {
        assert_ne!(
            kube_arch(),
            "aarch64",
            "kubernetes.io/arch must never be the Rust spelling"
        );
        assert_ne!(
            kube_os(),
            "macos",
            "kubernetes.io/os must never be the Rust spelling"
        );
        assert!(
            matches!(kube_arch(), "arm64" | "amd64" | "arm" | "ppc64le" | "s390x"),
            "unexpected GOARCH rendering: {}",
            kube_arch()
        );
        assert!(
            matches!(kube_os(), "linux" | "darwin" | "windows"),
            "unexpected GOOS rendering: {}",
            kube_os()
        );
    }

    /// Every well-known key upstream's kubelet self-applies is present and
    /// non-empty. An ABSENT labels map is what made every nodeSelector fail
    /// permanently; a present-but-partial one fails the same way, quietly.
    #[test]
    fn every_well_known_label_is_present_and_non_empty() {
        let labels = well_known_node_labels("cid");
        let obj = labels.as_object().expect("labels must be an object");
        for key in [
            "kubernetes.io/hostname",
            "kubernetes.io/os",
            "kubernetes.io/arch",
            "beta.kubernetes.io/os",
            "beta.kubernetes.io/arch",
        ] {
            let v = obj
                .get(key)
                .unwrap_or_else(|| panic!("missing well-known label {key}"))
                .as_str()
                .unwrap_or_else(|| panic!("{key} must be a string"));
            assert!(!v.is_empty(), "{key} is empty, which matches nothing");
        }
        assert_eq!(obj["kubernetes.io/hostname"], "cid");
    }

    /// The deprecated beta aliases must agree with their replacements —
    /// charts in the wild still select on them, and a disagreement would
    /// make the same node match one selector and not its equivalent.
    #[test]
    fn beta_aliases_agree_with_their_replacements() {
        let labels = well_known_node_labels("cid");
        assert_eq!(labels["beta.kubernetes.io/os"], labels["kubernetes.io/os"]);
        assert_eq!(
            labels["beta.kubernetes.io/arch"],
            labels["kubernetes.io/arch"]
        );
    }
}

#[cfg(test)]
mod tests {

    /// ★ The invariant this exists for: pods are never told an address that
    /// routes nowhere. Under podman they get the host gateway, because no
    /// service datapath installs the cluster VIP.
    #[test]
    fn podman_pods_are_told_the_host_gateway_not_the_unrouted_service_vip() {
        let mut cfg = EngenhoConfig::default();
        cfg.runtime.kubelet_backend = CfgBackendKind::Podman;
        cfg.runtime.listen_addr = "127.0.0.1:6443".to_string();

        let r = super::apiserver_reachability(&cfg);
        assert_eq!(
            r,
            super::ApiserverReachability::HostGateway {
                host: super::PODMAN_HOST_GATEWAY.to_string(),
                port: 6443,
            },
            "the ClusterIP has no datapath under podman; advertising it makes \
             in-cluster construction SUCCEED and removes the kubeconfig \
             fallback, so every request fails instead"
        );

        let (host, port) = r.injectable().expect("podman must advertise something");
        assert_ne!(
            host,
            super::DEFAULT_KUBERNETES_SERVICE_IP,
            "injecting the unrouted VIP is the defect"
        );
        assert_eq!(
            port, 6443,
            "the REAL listen port, not 443 — nothing rewrites it"
        );
    }

    /// An unparseable listen address means we do not know a reachable
    /// coordinate. Inject NOTHING: absent env fails in-cluster CONSTRUCTION,
    /// which is what makes a client fall back to a kubeconfig.
    #[test]
    fn an_unknown_address_injects_nothing_so_the_kubeconfig_fallback_survives() {
        let mut cfg = EngenhoConfig::default();
        cfg.runtime.kubelet_backend = CfgBackendKind::Podman;
        cfg.runtime.listen_addr = "not-a-socket-address".to_string();

        let r = super::apiserver_reachability(&cfg);
        assert_eq!(r, super::ApiserverReachability::Unknown);
        assert!(
            r.injectable().is_none(),
            "a guessed address is worse than none: it removes the fallback"
        );
    }

    /// The port is read from config, never assumed — a non-default listen
    /// port must reach the pod, or in-cluster clients dial the wrong one.
    #[test]
    fn the_advertised_port_follows_the_configured_listen_port() {
        let mut cfg = EngenhoConfig::default();
        cfg.runtime.kubelet_backend = CfgBackendKind::Podman;
        cfg.runtime.listen_addr = "127.0.0.1:16443".to_string();
        let (_, port) = super::apiserver_reachability(&cfg).injectable().unwrap();
        assert_eq!(port, 16443);
    }

    /// Empty disables publishing — the escape hatch tests and headless
    /// contexts use to stay out of `$HOME`.
    #[test]
    fn empty_publish_path_disables_publishing() {
        assert!(super::resolve_publish_path("").is_none());
        assert!(super::resolve_publish_path("   ").is_none());
    }

    /// An absolute path is taken verbatim — nix renders one when it wants
    /// the file somewhere other than the convention.
    #[test]
    fn absolute_publish_path_is_verbatim() {
        assert_eq!(
            super::resolve_publish_path("/etc/engenho/kubeconfig"),
            Some(std::path::PathBuf::from("/etc/engenho/kubeconfig"))
        );
    }

    /// ★ `~/` IS EXPANDED HERE, NOT BAKED INTO CONFIG.
    ///
    /// The default is the portable string `~/.kube/configs/engenho`. If
    /// the config layer resolved `$HOME` instead, a rendered config would
    /// be valid only for the user who generated it — which breaks the
    /// nix path, where the config is built once and used by whoever runs
    /// the daemon.
    #[test]
    fn tilde_expands_against_home_at_write_time() {
        let Ok(home) = std::env::var("HOME") else {
            return; // no HOME in this environment; the None case is covered below
        };
        assert_eq!(
            super::resolve_publish_path("~/.kube/configs/engenho"),
            Some(std::path::PathBuf::from(home).join(".kube/configs/engenho"))
        );
    }

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
        // Plaintext: these unit tests assert subsystem assembly, not TLS.
        cfg.runtime.tls.enabled = false;
        // A WRITABLE data_dir under the system temp root — the prescribed
        // `/var/lib/engenho` isn't writable in CI, and since Brick B the
        // bootstrap admin BEARER token is minted (persisted under data_dir/pki)
        // on EVERY boot (plaintext incl.), so a writable data_dir is required
        // even for the ephemeral path. A unique per-test subdir avoids
        // cross-test collisions; the small PKI dir is left for the OS temp
        // sweeper (no cleanup handle needed for a unit test).
        //
        // pid + a process-local COUNTER, not a timestamp. `SystemTime` here
        // resolves to microseconds on macOS (the nanos always end in `000`),
        // so two `#[tokio::test]`s entering this function in the same
        // microsecond got the SAME data_dir and raced over
        // `pki/admin.token`. That was invisible while the token was written
        // with `fs::write` (last writer wins); `cofre_fs::write_secret` uses
        // `create_new`, which reports the collision as `AlreadyExists`
        // instead of absorbing it. The counter makes the name unique by
        // construction rather than by clock luck.
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let unique = format!(
            "engenho-test-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        cfg.runtime.data_dir = std::env::temp_dir().join(unique);
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
        // Drivers: 12 reconcilers (deployment, replicaset, statefulset,
        // daemonset, job, cronjob, endpoints, pdb, service_routing, gc,
        // namespace, pv_binder, crd) + served_capability + scheduler +
        // kubelet = 16.
        //
        // This count is deliberately pinned: a driver that stops being
        // spawned is invisible at runtime (the cluster simply stops
        // converging that kind), so the arithmetic here is the tripwire.
        // Moving it is correct ONLY alongside an intentional change to the
        // driver set — which is what added served_capability.
        assert_eq!(rt.drivers.len(), 19);
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

    #[test]
    fn make_service_router_dispatches_by_resolved_datapath() {
        // The typed backend dispatch: each ResolvedDatapath arm maps to the
        // matching ServiceRouter implementation (by stable backend name).
        assert_eq!(
            make_service_router(ResolvedDatapath::Iptables).name(),
            "iptables"
        );
        assert_eq!(make_service_router(ResolvedDatapath::Ipvs).name(), "ipvs");
        assert_eq!(
            make_service_router(ResolvedDatapath::ComputeOnly).name(),
            "fake"
        );
    }

    #[test]
    fn datapath_auto_selects_compute_only_off_linux() {
        // The platform-selection contract, exercised through the config
        // resolve + backend construction the runtime uses — tested with an
        // explicit platform arg (not cfg!): Auto off-Linux is compute-only
        // (so a Darwin dev host never shells to iptables), Auto on Linux
        // installs the iptables kernel datapath.
        let mode = engenho_config::DatapathMode::Auto;
        assert_eq!(
            make_service_router(mode.resolve(false)).name(),
            "fake",
            "Auto off-Linux must be the compute-only FakeRouter"
        );
        assert_eq!(
            make_service_router(mode.resolve(true)).name(),
            "iptables",
            "Auto on Linux installs the iptables kernel datapath"
        );
    }

    #[tokio::test]
    async fn disabling_service_routing_drops_one_driver() {
        // Gating works: turning off enable.service_routing removes exactly
        // one spawned driver (16 → 15).
        let mut cfg = ephemeral_test_config();
        cfg.controllers.enable.service_routing = false;
        let rt = Runtime::start(cfg).await.unwrap();
        assert_eq!(rt.drivers.len(), 18);
        rt.shutdown().await.unwrap();
    }
}

/// The wiring the runtime actually performs, asserted directly.
///
/// ★ These exist because the defect was invisible to every other test. The
/// authenticator, the verifier, the token format and the chain constructor all
/// had passing tests; what was untested was WHICH constructor the boot sequence
/// reached for. Unit tests of a capability say nothing about whether the
/// capability is connected, and the disconnected version failed in a way that
/// read as a deliberate limitation ("not yet supported") rather than as a bug.
#[cfg(test)]
mod authenticator_wiring {
    use super::{SA_ISSUER, build_authenticator};
    use engenho_apiserver::RequestCreds;

    fn now_secs() -> i64 {
        i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock is after the epoch")
                .as_secs(),
        )
        .expect("seconds fit in i64")
    }

    /// Mint a token exactly as `RuntimeSaProjector` does, then present it to
    /// the chain `build_authenticator` produces. Both halves read the same
    /// idempotent key file, which is the property under test: the pod's token
    /// and the server's verifier are one keypair by construction.
    #[test]
    fn a_pod_token_authenticates_as_its_service_account() {
        let dir = tempfile::tempdir().expect("tempdir");
        let kp = engenho_apiserver::sa_token::load_or_generate_sa_key(dir.path())
            .expect("key generates on first read");

        // Validation reads the real clock, so the token must be minted against
        // it. Pinning a fixed epoch here mints a token that is already years
        // expired — which is how this test first failed, reporting
        // `ServiceAccountUnsupported` for what was actually an expiry. That
        // confusion is exactly what the split AuthnError variants now prevent.
        let now = now_secs();
        let token = engenho_apiserver::sa_token::issue(
            &kp.signing,
            SA_ISSUER,
            "pangea-system",
            "pangea-operator",
            "sa-uid-1",
            &[SA_ISSUER.to_string()],
            None,
            now,
            3600,
        )
        .expect("mint");

        let chain = build_authenticator(dir.path(), None);
        let user = chain
            .authenticate(&RequestCreds {
                client_cert: None,
                bearer: Some(token),
            })
            .expect("a validly-minted token must not be a typed error");

        assert_eq!(
            user.username, "system:serviceaccount:pangea-system:pangea-operator",
            "the workload must authenticate AS ITSELF — this being wrong is what \
             forced every pod to mount an admin kubeconfig instead"
        );
        assert!(
            user.groups.iter().any(|g| g == "system:serviceaccounts"),
            "SA group membership carries the RBAC bindings; got {:?}",
            user.groups
        );
    }

    /// The regression guard proper. `bootstrap()` and `bootstrap_with_sa()`
    /// differ ONLY in whether the SA stage holds a key, and the difference is
    /// invisible until a token is presented — which is precisely why the wrong
    /// one shipped. Assert the observable difference, not the constructor name.
    #[test]
    fn the_runtime_chain_differs_observably_from_the_keyless_one() {
        let dir = tempfile::tempdir().expect("tempdir");
        let kp = engenho_apiserver::sa_token::load_or_generate_sa_key(dir.path()).expect("key");
        let token = engenho_apiserver::sa_token::issue(
            &kp.signing,
            SA_ISSUER,
            "default",
            "probe",
            "uid",
            &[SA_ISSUER.to_string()],
            None,
            now_secs(),
            3600,
        )
        .expect("mint");
        let creds = RequestCreds {
            client_cert: None,
            bearer: Some(token),
        };

        let keyless = engenho_apiserver::ChainAuthenticator::bootstrap(None);
        assert!(
            keyless.authenticate(&creds).is_err(),
            "the keyless chain must still REJECT — the fallback path has to stay \
             a refusal, never a silent anonymous"
        );

        assert!(
            build_authenticator(dir.path(), None)
                .authenticate(&creds)
                .is_ok(),
            "the chain the runtime builds must ACCEPT the same token the keyless \
             one refuses; if these ever agree, the key stopped being wired"
        );
    }

    /// An unreadable key must degrade to refusal, not to permission.
    #[test]
    fn an_unreadable_key_falls_back_to_rejecting_not_to_admitting() {
        // A path that cannot hold a key file: a regular file where the loader
        // wants a directory.
        let dir = tempfile::tempdir().expect("tempdir");
        let not_a_dir = dir.path().join("occupied");
        std::fs::write(&not_a_dir, b"not a directory").expect("write");

        let real = tempfile::tempdir().expect("tempdir");
        let kp = engenho_apiserver::sa_token::load_or_generate_sa_key(real.path()).expect("key");
        let token = engenho_apiserver::sa_token::issue(
            &kp.signing,
            SA_ISSUER,
            "default",
            "probe",
            "uid",
            &[SA_ISSUER.to_string()],
            None,
            now_secs(),
            3600,
        )
        .expect("mint");

        let chain = build_authenticator(&not_a_dir, None);
        assert!(
            chain
                .authenticate(&RequestCreds {
                    client_cert: None,
                    bearer: Some(token)
                })
                .is_err(),
            "with no usable key an SA bearer must be a typed 401 — losing the key \
             must never widen access"
        );
    }
}

/// The advertised address, the remote kubeconfig, and the serving certificate.
///
/// ★ The property under test is that these cannot disagree. engenho has already
/// paid once for the version of this bug where a name ROUTED but was not NAMED
/// by the cert (the container host gateway, 2026-09-01) — two layers, one
/// symptom, the second invisible from the first. That was fixed by adding the
/// name and pinning the pair. Here there is one field and two readers, so the
/// pair is structural; these tests exist to keep it that way.
#[cfg(test)]
mod advertised_address {
    use super::{SanEntry, advertised_host, advertised_server_url, server_sans};
    use engenho_config::EngenhoConfig;
    use shikumi::TieredConfig;

    fn config_with(advertise: &str, extra: &[&str]) -> EngenhoConfig {
        let mut c = EngenhoConfig::prescribed_default();
        c.runtime.advertise_address = advertise.to_string();
        c.runtime.tls.extra_sans = extra.iter().map(|s| (*s).to_string()).collect();
        c
    }

    fn bound() -> std::net::SocketAddr {
        "0.0.0.0:6443".parse().expect("addr")
    }

    #[test]
    fn advertising_a_name_puts_it_in_the_certificate() {
        let sans = server_sans(&config_with("plo.natal.quero.cloud", &[])).expect("sans");
        assert!(
            sans.contains(&SanEntry::Dns("plo.natal.quero.cloud".to_string())),
            "the advertised host MUST be a SAN — a kubeconfig naming an address \
             the cert does not is a file that cannot work, and it fails at the \
             client, not here; got {sans:?}"
        );
    }

    #[test]
    fn advertising_an_address_puts_it_in_the_certificate_as_an_ip() {
        let sans = server_sans(&config_with("100.64.0.7:6443", &[])).expect("sans");
        assert!(
            sans.contains(&SanEntry::Ip("100.64.0.7".parse().unwrap())),
            "got {sans:?}"
        );
    }

    #[test]
    fn the_port_reaches_the_url_and_never_the_certificate() {
        // A certificate names hosts; a URL needs the port. Conflating them
        // yields either a cert with a port in a DNS SAN (matches nothing) or a
        // URL missing its port (dials 443).
        let cfg = config_with("plo.quero.cloud:16443", &[]);
        let sans = server_sans(&cfg).expect("sans");
        assert!(
            sans.contains(&SanEntry::Dns("plo.quero.cloud".to_string())),
            "the SAN must be the bare host; got {sans:?}"
        );
        assert!(
            !sans.iter().any(|s| s.to_string().contains(":16443")),
            "no SAN may carry a port; got {sans:?}"
        );
        assert_eq!(
            advertised_server_url(&cfg, bound()).as_deref(),
            Some("https://plo.quero.cloud:16443"),
            "the URL must keep the advertised port — a proxy or forward makes it \
             legitimately different from the bound one"
        );
    }

    #[test]
    fn an_advertised_host_without_a_port_inherits_the_bound_one() {
        // So an operator naming only a host cannot restate the port wrongly.
        assert_eq!(
            advertised_server_url(&config_with("plo", &[]), bound()).as_deref(),
            Some("https://plo:6443")
        );
    }

    #[test]
    fn advertising_nothing_yields_no_url_and_no_extra_san() {
        let cfg = config_with("", &[]);
        assert_eq!(advertised_server_url(&cfg, bound()), None);
        assert!(
            server_sans(&cfg).expect("sans").is_empty(),
            "a node-local apiserver must gain no SANs from this path"
        );
    }

    #[test]
    fn declaring_the_advertised_name_explicitly_does_not_duplicate_it() {
        // Listing it in extra_sans as well is the natural thing to do before
        // learning it is automatic, and must not produce a doubled SAN.
        let sans =
            server_sans(&config_with("plo.quero.cloud", &["plo.quero.cloud"])).expect("sans");
        assert_eq!(
            sans.iter()
                .filter(|s| **s == SanEntry::Dns("plo.quero.cloud".to_string()))
                .count(),
            1,
            "got {sans:?}"
        );
    }

    #[test]
    fn a_malformed_advertised_address_fails_at_start_rather_than_at_a_client() {
        // Same reasoning as extra_sans: this must not become a cert that serves
        // and verifies for nobody.
        assert!(
            server_sans(&config_with("https://plo:6443", &[])).is_err(),
            "a URL is not an address and must be refused, not encoded"
        );
    }

    #[test]
    fn ipv6_is_bracketed_in_the_url_and_bare_in_the_certificate() {
        let cfg = config_with("fd00::1", &[]);
        assert_eq!(advertised_host("fd00::1"), Some("fd00::1"));
        assert_eq!(
            advertised_server_url(&cfg, bound()).as_deref(),
            Some("https://[fd00::1]:6443"),
            "an unbracketed IPv6 host makes a URL that will not parse"
        );
        assert!(
            server_sans(&cfg)
                .expect("sans")
                .contains(&SanEntry::Ip("fd00::1".parse().unwrap())),
            "the SAN is the bare address, unbracketed"
        );
    }

    #[test]
    fn a_bracketed_ipv6_with_a_port_splits_correctly() {
        assert_eq!(advertised_host("[fd00::1]:6443"), Some("fd00::1"));
        assert_eq!(
            advertised_server_url(&config_with("[fd00::1]:6443", &[]), bound()).as_deref(),
            Some("https://[fd00::1]:6443")
        );
    }
}
