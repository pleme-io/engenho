//! The [`Runtime`] — single-process assembly of every engenho subsystem.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{
    ApiServer, ChainAuthenticator, ClientMaterial, RbacAuthorizer, RouterHandlerSink, RouterState,
    ServerSanInputs, StoreRbacEnv, TlsMaterial, client_verifier,
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
use engenho_kubelet::{ContainerRuntime, Kubelet, LogOptions, make_container_runtime};
use engenho_scheduler::{Scheduler, make_scheduling_strategy};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use engenho_types::generated_v1_34::rbac_v1::{
    ClusterRole, ClusterRoleBinding, PolicyRule, RoleRef, Subject,
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

        // 4.5. Seed the bootstrap RBAC policy (Brick B) — cluster-admin +
        //    system:discovery + system:basic-user + system:public-info-viewer
        //    ClusterRoles + their ClusterRoleBindings — BEFORE the apiserver
        //    binds, so the very first request authorizes against a seeded store.
        //    Idempotent (Put preserves uid across restarts, same as
        //    register_node). MUST precede step 5 so anonymous discovery + bound
        //    roles resolve through real bindings from the first request.
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
            let material = issue_server_material(
                &ca,
                &ServerSanInputs {
                    node_name: &config.runtime.node_name,
                    listen_ip,
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
        let authenticator = Arc::new(ChainAuthenticator::bootstrap(admin_token));

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
struct KubeletLogReader {
    kubelet: Arc<Kubelet>,
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
    let mut value = serde_json::json!({
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
        let c = PvBinderController::new(store.clone(), ns.clone(), local_path_root);
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

    // Kubelet: bound Pod → container via the backend. Watches Pods. Built
    // ONCE as an Arc<Kubelet> so the SAME instance is shared between its
    // WatchDriver (via the `Controller for Arc<C>` blanket impl) and the
    // apiserver's Pod `/log` reader — both see one local bookkeeping map.
    let kubelet = Arc::new(Kubelet::new(
        store.clone(),
        backend.clone(),
        config.runtime.node_name.clone(),
    ));
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
mod tests {

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
        // namespace, pv_binder, crd) + scheduler + kubelet = 15.
        assert_eq!(rt.drivers.len(), 15);
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
        // one spawned driver (15 → 14).
        let mut cfg = ephemeral_test_config();
        cfg.controllers.enable.service_routing = false;
        let rt = Runtime::start(cfg).await.unwrap();
        assert_eq!(rt.drivers.len(), 14);
        rt.shutdown().await.unwrap();
    }
}
