//! The [`Runtime`] — single-process assembly of every engenho subsystem.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{
    ApiServer, ChainAuthenticator, ClientMaterial, RbacAuthorizer, RouterHandlerSink, RouterState,
    SanEntry, ServerSanInputs, StoreRbacEnv, TlsMaterial, client_verifier,
    handlers_from_catalog_with_admission, issue_admin_client_material, issue_server_material,
    load_or_generate_ca, metrics::log_would_reject,
};
use engenho_config::leaf::{flatten, nest};
use engenho_config::mutability::{self, Mutability};
use engenho_config::{
    EngenhoConfig, KubeconfigVisibility, KubeletBackendKind as CfgBackendKind, ResolvedDatapath,
};
use engenho_controllers::{
    Controller, ControllerError, ControllerType, CrdController, CronJobController,
    DaemonSetController, DeclaresReads, DeploymentController, DynamicHandlerSink,
    EndpointsController, FakeRouter, GcController, Heartbeat, IptablesRouter, IpvsRouter,
    JobController, NamespaceController, PodDisruptionBudgetController, PvBinderController,
    PvcProtectionController, Reads, ReconcileOutcome, ReplicaSetController, ServiceRouter,
    ServiceRoutingController, StatefulSetController, WallClock, WatchDriver, WatchDriverConfig,
    admission::{AdmissionChain, AdmissionMode, AdmissionWebhook},
    cluster_ip::{ClusterIpDefaultingWebhook, StoreServiceIpSource},
    event_recorder::EventSink,
    gvk,
};
use engenho_kube_client::{emit_kubeconfig, emit_kubeconfig_with_admin};
use engenho_kubelet::config_bridge::KubeletBackendKind;
use engenho_kubelet::{
    ContainerRuntime, Kubelet, LogOptions, make_container_runtime_with_apiserver,
};
use engenho_scheduler::{ConfiguredScheduler, Scheduler};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use engenho_substrate::WouldRejectLedger;
use engenho_types::generated_v1_34::core_v1::Namespace;
use engenho_types::generated_v1_34::rbac_v1::{
    ClusterRole, ClusterRoleBinding, PolicyRule, RoleRef, Subject,
};
use engenho_types::generated_v1_34::types::{NamespaceSpec, NamespaceStatus};
use engenho_types::kind::GroupVersionKind;
use tracing::{error, info, warn};

use crate::boot::{BootKind, BootPhase, BootRecorder};
use crate::boot_config::{ApiserverTls, BootConfig};
use crate::child::{Child, ChildTask, Children, DeadChild, Driver, Listener, TickLoop, Wiring};
use crate::error::{RuntimeError, ShutdownStage, StrongCounts};
use crate::health::{Health, Row, Tallied, Tally, Windows};
use crate::node_lease::NodeLease;
use crate::node_registration::{HostOwned, register_node};
use crate::panics::PanicCounter;
use crate::publish::{KubeconfigTarget, PublishRecord, SkipReason};
use crate::rebind::serve_rebinding;
use crate::release::{BootFailed, BootUnwind, StoreReleased};
use crate::runtime_health::RuntimeHealthSource;

/// The assembled single-node runtime. Owns the store spine, the
/// apiserver, every child task (drivers + listeners, one owned set), and the
/// container backend (retained for shutdown + test inspection).
pub struct Runtime {
    config: EngenhoConfig,
    store: Arc<StoreMesh>,
    apiserver: ApiServer,
    children: Children,
    /// Every panic in the process, counted by the hook `start` installs.
    panics: PanicCounter,
    /// What `/livez`, `/healthz`, `/readyz` and the runtime's `/metrics`
    /// families are derived from (T2.8): the children's heartbeats and task
    /// handles, the drain state, the store. See [`Runtime::health`].
    health: Arc<Health>,
    /// The daemon's one rollout-gate ledger (T0.11): what every gate in
    /// `Rollout::Shadow` allowed that `Enforce` would have refused. The
    /// apiserver's `/metrics` renders THIS ledger as
    /// `engenho_would_reject_total{gate,reason}`, so a gate that counts
    /// anywhere else is a gate no scrape shows. See [`Runtime::would_reject`].
    would_reject: Arc<WouldRejectLedger>,
    /// Whether this boot created the store, resumed it, or runs in memory.
    boot_kind: BootKind,
    /// Where this boot's kubeconfigs went (or the latest republish's).
    publish: Vec<PublishRecord>,
    /// What publishing them again takes.
    publisher: Publisher,
    /// The SANs the apiserver's certificate was issued with; `None` with
    /// TLS off. The certificate itself lives only in memory.
    server_sans: Option<Vec<String>>,
    /// The backend the runtime was started with. Nothing reads it: the
    /// kubelet holds its own clone, and that clone is what keeps the backend
    /// alive for its ticks. A test passes its own clone to
    /// [`Runtime::start_with_backend`] and inspects that.
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
    /// See [`RuntimeError`] — a config field this runtime does not run
    /// ([`RuntimeError::Unhonoured`]), config invalid, store start /
    /// leadership failure, apiserver bind failure, or an unparseable listen
    /// addr.
    pub async fn start(config: EngenhoConfig) -> Result<Self, RuntimeError> {
        Self::boot(config)
            .await
            .map_err(BootFailed::into_logged_error)
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
        Self::boot_with_backend(config, backend)
            .await
            .map_err(BootFailed::into_logged_error)
    }

    /// [`Runtime::start`], saying what a failed boot did about the store.
    ///
    /// A boot that fails after opening the store does not leave it behind: it
    /// stops what it had started (the apiserver, if it was bound) and
    /// terminates the store, so another boot in the same process can open it.
    /// [`BootFailed::unwind`] says whether that worked
    /// ([`BootUnwind::Released`]) — the fact a supervisor that retries needs.
    ///
    /// # Errors
    ///
    /// [`BootFailed`], carrying the [`RuntimeError`] [`Runtime::start`]
    /// would have returned.
    pub async fn boot(config: EngenhoConfig) -> Result<Self, BootFailed> {
        Self::boot_recorded(config, &mut BootRecorder::silent()).await
    }

    /// [`Runtime::boot`], entering each [`BootPhase`] through `rec`: the
    /// phases are reported to whoever supervises the boot, and a stop
    /// requested through `rec` ends the boot at the next phase boundary
    /// before the apiserver binds (see [`BootRecorder`]).
    ///
    /// # Errors
    ///
    /// Same as [`Runtime::boot`]; [`BootFailed::phase`] names the phase the
    /// boot failed in.
    pub async fn boot_recorded(
        config: EngenhoConfig,
        rec: &mut BootRecorder,
    ) -> Result<Self, BootFailed> {
        // The caller resolved the config; entering the phase records it.
        rec.enter(BootPhase::ResolveConfig)
            .map_err(|e| rec.failed(e))?;
        Self::boot_resolved(config, None, rec).await
    }

    /// [`Runtime::start_with_backend`], saying what a failed boot did about
    /// the store (see [`Runtime::boot`]).
    ///
    /// # Errors
    ///
    /// Same as [`Runtime::boot`].
    pub async fn boot_with_backend(
        config: EngenhoConfig,
        backend: Arc<dyn ContainerRuntime>,
    ) -> Result<Self, BootFailed> {
        let mut rec = BootRecorder::silent();
        rec.enter(BootPhase::ResolveConfig)
            .map_err(|e| rec.failed(e))?;
        Self::boot_resolved(config, Some(backend), &mut rec).await
    }

    /// Boot over a resolved config, with `rec` already in
    /// [`BootPhase::ResolveConfig`]. `backend` is built from the config when
    /// absent.
    pub(crate) async fn boot_resolved(
        config: EngenhoConfig,
        backend: Option<Arc<dyn ContainerRuntime>>,
        rec: &mut BootRecorder,
    ) -> Result<Self, BootFailed> {
        // Every field read once, before anything is probed or written: a
        // field this runtime cannot honour is refused here (I21).
        rec.enter(BootPhase::ReadBootConfig)
            .map_err(|e| rec.failed(e))?;
        let boot = BootConfig::read(&config).map_err(|e| rec.failed(e.into()))?;
        rec.enter(BootPhase::PreflightBackend)
            .map_err(|e| rec.failed(e))?;
        let backend = if let Some(backend) = backend {
            backend
        } else {
            // Fail LOUDLY here if the configured runtime cannot be reached,
            // rather than discovering it one warn-per-tick at a time forever.
            preflight_backend(&boot).map_err(|e| rec.failed(e))?;
            build_backend(&boot).map_err(|e| rec.failed(e))?
        };
        Self::start_inner(config, boot, backend, rec).await
    }

    /// Boot over a read config: open the store, then [`Self::assemble`]
    /// everything over it. When the assembly fails, it has already dropped
    /// (and, for the apiserver, stopped) everything it built, so this holds
    /// the store alone and can take it back.
    async fn start_inner(
        config: EngenhoConfig,
        boot: BootConfig,
        backend: Arc<dyn ContainerRuntime>,
        rec: &mut BootRecorder,
    ) -> Result<Self, BootFailed> {
        // 0. Count every panic in the process from here on (T2.7): the hook
        //    chains to whatever was installed before it, and installs once
        //    however many runtimes start.
        let panics = PanicCounter::install();

        // 0b. The daemon's ONE would-reject ledger (T0.11). Built before
        //     anything that can own a gate — the store's image tripwire
        //     (T3.4) runs inside step 2 — so every gate-owning component is
        //     handed this Arc and none builds its own: a second ledger is a
        //     set of gates `/metrics` never shows. Today only the apiserver
        //     takes it (step 5); the store's tripwire and the scheduler's
        //     filters do not judge through a `Gate` yet.
        let would_reject = Arc::new(WouldRejectLedger::new(log_would_reject));

        // 1. Validate the whole config (every section + cross-section).
        //    Every field was already read into `boot`; what it read and does
        //    not run is said once, here.
        rec.enter(BootPhase::ValidateConfig)
            .map_err(|e| rec.failed(e))?;
        config.validate().map_err(|e| rec.failed(e.into()))?;
        for not_run in &boot.not_run {
            info!(field = not_run.field(), why = %not_run, "config read, not run");
        }

        // 2. Bring up the store spine. Durable = restart-safe
        //    start_or_resume; ephemeral = in-memory start +
        //    initialize_singleton (test path).
        rec.enter(BootPhase::OpenStore).map_err(|e| rec.failed(e))?;
        let (store, boot_kind) = boot_store(&boot).await.map_err(|e| rec.failed(e))?;
        rec.observe(boot_kind);

        // 3–6. Everything else is built over the store. On failure the
        //    assembly has dropped what it built and stopped the apiserver if
        //    it had bound, so `store` is this frame's alone to take back.
        match Self::assemble(&boot, &backend, &store, panics, &would_reject, rec).await {
            Ok(Assembled {
                apiserver,
                children,
                health,
                publish,
                publisher,
                server_sans,
            }) => Ok(Self {
                config,
                store,
                apiserver,
                children,
                panics,
                health,
                would_reject,
                boot_kind,
                publish,
                publisher,
                server_sans,
                backend,
            }),
            Err(error) => {
                let phase = rec.current();
                let unwind = unwind_failed_boot(store).await;
                Err(BootFailed {
                    error,
                    phase,
                    unwind,
                })
            }
        }
    }

    /// Steps 3–6 of the boot: leadership, scheduler, health, node
    /// registration, seeds, PKI, the apiserver and the children, over an
    /// opened store.
    ///
    /// Everything it builds is a local of this function, so an early return
    /// drops it all. The one thing that outlives a drop is the apiserver's
    /// serve task, which holds the router — and through it the store — until
    /// it has severed its connections, so a failure after the bind stops the
    /// apiserver explicitly before returning.
    ///
    /// A stop requested through `rec` is honoured up to the bind. From there
    /// on the boot finishes: the children it spawns are not aborted by a
    /// drop, and unwinding a bound apiserver with its children IS a
    /// shutdown, so whoever cancelled shuts the runtime down instead.
    async fn assemble(
        boot: &BootConfig,
        backend: &Arc<dyn ContainerRuntime>,
        store: &Arc<StoreMesh>,
        panics: PanicCounter,
        would_reject: &Arc<WouldRejectLedger>,
        rec: &mut BootRecorder,
    ) -> Result<Assembled, RuntimeError> {
        // 3. Wait for raft leadership — MUST precede any propose.
        rec.enter(BootPhase::AwaitLeadership)?;
        await_leadership(store, boot, rec).await?;
        rec.enter(BootPhase::BuildScheduler)?;

        // 3a. The scheduler, from every `scheduler.*` field (T5.8):
        //     `Scheduler::from_config` is that section's one reader. It scopes
        //     placement by `scheduler.namespace` and names the fallback its
        //     loop runs on, `scheduler.tick_interval_seconds`, which the
        //     windows below carry. An unimplemented strategy or a zero tick is
        //     a typed error, never a round-robin or controllers-tick fallback
        //     (validate() refuses both in step 1; this guard holds for any
        //     caller that skips it).
        let scheduler = Scheduler::from_config(store.clone(), &boot.scheduler)?;

        // 3b. What health and the runtime's metrics are read from (T2.8).
        //     Built before the apiserver binds, so the router holds it from
        //     its first request; it reports no children (every health check
        //     fails) until step 6 hands it the spawned set. The windows are
        //     built ONCE and given to the drivers as well, so a tick the
        //     driver logs BLOCKED is the tick liveness reports stalled.
        let windows = boot.windows(scheduler.fallback_interval());
        let health = Arc::new(Health::new(store, windows, panics));

        // 4. Register THIS node so the scheduler has a target: create its
        //    Node if absent, else merge only the host-owned fields — a
        //    restart never undoes a cordon, a taint or an operator's label.
        rec.enter(BootPhase::RegisterNode)?;
        let node_name = &boot.node_name;
        register_node(store, node_name, &HostOwned::measured(node_name)).await?;
        rec.enter(BootPhase::SeedCluster)?;

        // 4.5. Seed the bootstrap RBAC policy (Brick B) — cluster-admin +
        //    system:discovery + system:basic-user + system:public-info-viewer
        //    ClusterRoles + their ClusterRoleBindings — BEFORE the apiserver
        //    binds, so the very first request authorizes against a seeded store.
        //    Idempotent (Put preserves uid across restarts). MUST precede
        //    step 5 so anonymous discovery + bound roles resolve through real
        //    bindings from the first request.
        //    Seed the four system namespaces FIRST: a namespace must exist
        //    before anything namespaced can live in it, and `default` is what
        //    every client opens to.
        seed_system_namespaces(store).await?;
        //    Then the `kubernetes` Service in `default`. It must follow the
        //    namespaces (it lives in one) and precede the apiserver bind, so
        //    the ClusterIP allocator sees .1 as held before any user Service
        //    can be created.
        seed_kubernetes_service(store, boot).await?;
        seed_bootstrap_rbac(store).await?;
        //    Then the default StorageClass. It must precede the apiserver bind
        //    for the same reason the others do: a PVC created in the first
        //    moments of a cluster's life should provision like any other, not
        //    hang until something happens to seed a class later.
        seed_default_storage_class(store).await?;
        //    Then the snapshot CRDs, so the VolumeSnapshot controller spawned
        //    below is watching kinds this cluster actually serves.
        seed_snapshot_crds(store).await?;

        // 5. Bind the apiserver, backed by the same store. The listen address
        //    is read with the PKI: the server certificate is issued for it.
        rec.enter(BootPhase::IssuePki)?;
        let listen_addr: SocketAddr =
            boot.listen_addr
                .parse()
                .map_err(|source| RuntimeError::ListenAddr {
                    addr: boot.listen_addr.clone(),
                    source,
                })?;

        // 5a. Build the TLS material BEFORE binding (when tls.enabled).
        let IssuedPki {
            tls,
            ca_cert_pem,
            admin: admin_material,
        } = issue_pki(boot, listen_addr)?;
        let server_sans = tls.as_ref().map(|t| t.sans.clone());

        // Load-or-generate the bootstrap admin BEARER token (a second admin
        // credential alongside the client cert). Persisted under
        // data_dir/pki/admin.token (0600) + logged so the operator can
        // `curl -H "Authorization: Bearer <token>"`. Minted REGARDLESS of TLS:
        // the token is a bearer SECRET (an `Authorization:` header value), not
        // TLS material — and with Brick B's default-deny it is the ONLY way a
        // plaintext-mode operator/test gets an admin (system:masters) identity
        // to write through the authorizer. (Pre-Brick-B the plaintext floor had
        // no admin token because authorize-ALL made one unnecessary.)
        rec.enter(BootPhase::LoadAdminToken)?;
        let admin_token: Option<String> = Some(load_or_generate_admin_token(&boot.data_dir)?);
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
        rec.enter(BootPhase::BuildAuth)?;
        let cluster_ip_hook: Arc<dyn AdmissionWebhook> = Arc::new(ClusterIpDefaultingWebhook::new(
            boot.service_cidr.clone(),
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
            Arc::new(build_authenticator(&boot.data_dir, admin_token));

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

        // The daemon's would-reject ledger goes in with the rest, before the
        // clones below (the CRD sink's, the log handler's, the server's): a
        // RouterState clone copies the ledger `Arc` it holds at that moment,
        // so a clone taken before this would keep the router's private one.
        let router_state = RouterState::new(handlers_from_catalog_with_admission(
            store.clone(),
            admission.clone(),
        ))
        .with_authenticator(authenticator)
        .with_authorizer(authorizer)
        .with_would_reject_ledger(Arc::clone(would_reject))
        // Health and the runtime's metric families, derived from observation
        // (T2.8). Installed here with the ledger, before any clone, for the
        // same reason.
        .with_liveness_source(health.clone())
        .with_metrics_source(health.clone());
        // The minting half, from the SAME key the authenticator verifies with.
        // Without it RBAC is decorative: the authorizer, the Roles and the
        // bindings all work, but nothing can present a non-admin identity to
        // be judged, so every workload needing the API has to mount a
        // kubeconfig carrying ADMIN client-key material.
        let router_state = match build_token_issuer(&boot.data_dir) {
            Some(issuer) => router_state.with_token_issuer(issuer),
            None => router_state,
        };
        // The CRD handler sink: builds a StoreBackedHandler (admission-
        // dispatched, opaque-JSON) per served CRD version + registers it
        // into the SAME router_state. Shared (as Arc<dyn DynamicHandlerSink>)
        // with the CrdController spawned in spawn_children.
        let handler_sink: Arc<dyn DynamicHandlerSink> =
            RouterHandlerSink::new(store.clone(), admission.clone(), router_state.clone())
                .into_dyn();
        // Keep a router_state clone so the Pod `/log` handler (which needs the
        // kubelet, built later in spawn_children) can be registered after the
        // kubelet exists. RouterState is Arc-backed: this clone shares the SAME
        // handler ArcSwap the apiserver dispatches on, so a `register()` here is
        // visible to in-flight requests (identical mechanism to the CRD sink).
        let router_state_for_logs = router_state.clone();

        rec.enter(BootPhase::BindApiserver)?;
        let apiserver = ApiServer::start_with_state(listen_addr, router_state, tls).await?;
        let bound_addr = apiserver.local_addr();
        info!(addr = %bound_addr, tls = boot.tls.is_enabled(), "apiserver bound");
        // Committed: from here on the boot finishes (see above).
        rec.enter_committed(BootPhase::PublishKubeconfigs);

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
        let publisher = Publisher::new(bound_addr, ca_cert_pem, admin_material);
        let publish = match publisher.publish(boot) {
            Ok(records) => records,
            Err(err) => {
                // The apiserver is serving; stop it and wait for its
                // connections, or they keep the store the unwind needs back.
                let _ = apiserver.shutdown().await;
                return Err(err);
            }
        };

        // 6. Spawn every child in the catalog (T2.6): the controller /
        //    scheduler / kubelet drivers (incl. the CrdController, which
        //    registers CR handlers into the shared router table via
        //    handler_sink) and the :10250 kubelet + :2379 etcd-façade
        //    listeners, into ONE owned set that `main` watches. Returns the
        //    Arc<Kubelet> so the Pod `/log` reader can be wired in.
        rec.enter_committed(BootPhase::SpawnChildren);
        let (children, kubelet) =
            spawn_children(boot, store, backend, scheduler, &handler_sink, windows);
        info!(count = children.len(), "children spawned");
        // From here the health endpoints report every spawned child, each
        // Unknown until its first beat.
        rec.enter_committed(BootPhase::AdoptHealth);
        health.adopt(children.rows());

        // 6b. Register the Pod `/log` handler — a StoreBackedHandler for the
        //     Pod kind whose `logs` delegates to the in-process kubelet (the
        //     KubeletLogReader adapter). This REPLACES the catalog-built Pod
        //     handler (which had no log reader → /log returned NotFound) with
        //     one that serves real container stdout. Single-node: the kubelet
        //     IS this process's kubelet, so the read is in-process. `register`
        //     keys on (group, version, plural) so it overwrites the Pod entry
        //     atomically (same swap mechanism the CRD sink uses).
        let log_reader = Arc::new(KubeletLogReader { kubelet });
        if let Some(pod_handler) = build_pod_log_handler(store, &admission, log_reader) {
            router_state_for_logs.register(pod_handler);
            info!("registered Pod /log handler (in-process kubelet log reader)");
        }

        Ok(Assembled {
            apiserver,
            children,
            health,
            publish,
            publisher,
            server_sans,
        })
    }

    /// What the health endpoints and the runtime's metric families are
    /// derived from: every spawned child judged from its heartbeat and its
    /// task, and every driver's reconcile tally and propose rate (T2.8).
    #[must_use]
    pub fn health(&self) -> &Arc<Health> {
        &self.health
    }

    /// The process's panic count: every panic since the runtime started,
    /// caught or not — a contained tick's, a dead child's, a request
    /// handler's.
    #[must_use]
    pub fn panics(&self) -> PanicCounter {
        self.panics
    }

    /// The daemon's rollout-gate ledger (T0.11) — the one `/metrics`
    /// renders as `engenho_would_reject_total{gate,reason}`, built once per
    /// runtime. A component that judges a gate is handed this `Arc`; a
    /// refusal a Shadow gate records here is on the next scrape.
    #[must_use]
    pub fn would_reject(&self) -> &Arc<WouldRejectLedger> {
        &self.would_reject
    }

    /// Every child the runtime spawned, with its state and heartbeat.
    #[must_use]
    pub fn children(&self) -> &Children {
        &self.children
    }

    /// Wait for the next child whose task ends. It is marked Dead and logged
    /// at ERROR before this returns; it is NOT respawned.
    ///
    /// Pends forever while every child runs, and is cancel-safe, so `main`
    /// selects on it beside its stop signal.
    pub async fn next_dead_child(&mut self) -> DeadChild {
        self.children.next_dead().await
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

    /// Whether this boot created the store, resumed it, or runs in memory.
    #[must_use]
    pub const fn boot_kind(&self) -> BootKind {
        self.boot_kind
    }

    /// Where this boot's kubeconfigs went.
    #[must_use]
    pub fn publish_records(&self) -> &[PublishRecord] {
        &self.publish
    }

    /// The SANs the apiserver's certificate was issued with; `None` with
    /// TLS off.
    #[must_use]
    pub fn server_sans(&self) -> Option<&[String]> {
        self.server_sans.as_deref()
    }

    /// Take `effective`'s in-place leaves — the live and inert ones of the
    /// sealed mutability table — into the running configuration, and publish
    /// the kubeconfigs again when a live one moved. Every other leaf stays as
    /// booted: what takes a restart is the caller's to report as pending.
    ///
    /// Returns the new publish records when it published.
    ///
    /// # Errors
    ///
    /// The adopted configuration does not deserialize or a boot would refuse
    /// it (the caller gated it, so neither is expected), or writing the data
    /// directory's own kubeconfig failed. The runtime is unchanged then.
    pub(crate) fn adopt(
        &mut self,
        effective: &EngenhoConfig,
    ) -> Result<Option<Vec<PublishRecord>>, RuntimeError> {
        let json = |c: &EngenhoConfig| serde_json::to_value(c).unwrap_or_default();
        let mut current = flatten(&json(&self.config));
        let target = flatten(&json(effective));
        let (mut moved, mut republish) = (false, false);
        for spec in mutability::leaves()
            .iter()
            .filter(|spec| spec.mutability.applies_in_place())
        {
            let want = target.get(&spec.path);
            if current.get(&spec.path) == want {
                continue;
            }
            match want {
                Some(value) => current.insert(spec.path.clone(), value.clone()),
                None => current.remove(&spec.path),
            };
            moved = true;
            republish |= matches!(spec.mutability, Mutability::Live { .. });
        }
        if !moved {
            return Ok(None);
        }
        let adopted: EngenhoConfig = serde_json::from_value(nest(&current)).map_err(|e| {
            engenho_config::ConfigError::Parse(format!("adopting in-place leaves: {e}"))
        })?;
        let boot = BootConfig::read(&adopted)?;
        let records = if republish {
            Some(self.publisher.publish(&boot)?)
        } else {
            None
        };
        self.config = adopted;
        if let Some(records) = &records {
            self.publish.clone_from(records);
        }
        Ok(records)
    }

    /// Publish the kubeconfigs again, as configured.
    ///
    /// # Errors
    ///
    /// Writing the data directory's own kubeconfig failed.
    pub(crate) fn republish(&mut self) -> Result<Vec<PublishRecord>, RuntimeError> {
        let boot = BootConfig::read(&self.config)?;
        self.publish = self.publisher.publish(&boot)?;
        Ok(self.publish.clone())
    }

    /// The store's current revision and whether this node leads — read
    /// through the runtime's own reference, so asking takes no new hold on
    /// the store.
    pub async fn store_position(&self) -> (u64, bool) {
        let revision = self.store.current_revision().await;
        (revision.get(), self.store.is_leader().await)
    }

    /// Graceful shutdown, one [`ShutdownStage`] at a time: abort + await
    /// every child task, stop the apiserver (2s grace), quiesce the store's
    /// own background tasks, flush its durable image, then take sole
    /// ownership and terminate it.
    ///
    /// The flush runs while the store is still behind the `Arc`, before the
    /// unwrap: once it returns, the next boot replays nothing applied before
    /// it, even if a leaked clone then makes the unwrap fail and `terminate`
    /// never runs. `terminate` flushes again after `Raft::shutdown`, which
    /// catches an entry a driver had in flight when it was aborted and that
    /// raft applied after the first flush. Not covered: an entry openraft's
    /// state-machine worker applies after that second flush (the worker is
    /// not joined); it is durable in the log and replayed on the next boot.
    ///
    /// `terminate` consumes [`StoreMesh`] and requires the SOLE strong
    /// `Arc` ref. The child tasks + apiserver handlers each hold a
    /// clone; aborting + awaiting the tasks and stopping the apiserver
    /// drops those clones, so `Arc::try_unwrap` then succeeds. The store's
    /// strong count is read after each stage, and a failed unwrap is
    /// charged to the first stage that owed sole ownership and did not have
    /// it ([`ShutdownStage::owes_sole_ownership`]).
    ///
    /// A client holding a WATCH open across the stop does not hold the store
    /// up: the apiserver severs it at the grace and awaits the severed
    /// connection (engenho-serve), pinned by `tests/shutdown_stages.rs`.
    ///
    /// On success it returns [`StoreReleased`]: the store is terminated and
    /// its lock is free, so another runtime can boot over it in this process.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::Server`] on apiserver shutdown failure,
    /// [`RuntimeError::Store`] if the flush cannot write the durable image
    /// (the stop ends there: the store is not terminated, and the log still
    /// holds every applied entry for the next boot to replay),
    /// [`RuntimeError::StoreStillShared`] if a store clone leaked past
    /// shutdown, or [`RuntimeError::Store`] on `terminate` failure.
    pub async fn shutdown(self) -> Result<StoreReleased, RuntimeError> {
        let Self {
            mut children,
            apiserver,
            store,
            health,
            ..
        } = self;
        // Readiness fails first, so a load balancer stops routing here
        // before anything below stops serving.
        health.begin_drain();
        // Abort every child then await it, so its captured Arc<StoreMesh>
        // (and the controller it owns) is actually dropped before we try
        // to unwrap the store.
        children.stop().await;
        let drivers_awaited = Arc::strong_count(&store);

        // Stop the apiserver: every StoreBackedHandler holds an
        // Arc<StoreMesh> clone, released with the router.
        apiserver.shutdown().await?;
        let apiserver_stopped = Arc::strong_count(&store);

        // Stop the store's own tasks (raft RPC pump, bookmark ticker) by
        // abort-then-await while it is still behind the Arc, so neither is
        // mid-tick on the store's inner state when `terminate` runs. They
        // hold no Arc<StoreMesh>; this stage owes sole ownership only in
        // the sense that nothing may take a new one meanwhile.
        let quiesced = store.quiesce().await;
        if quiesced.any_panicked() {
            error!(
                rpc_pump = ?quiesced.rpc_pump,
                bookmark_ticker = ?quiesced.bookmark_ticker,
                "a store background task had panicked before shutdown stopped it"
            );
        }
        let store_quiesced = Arc::strong_count(&store);

        // Bring the durable image up to the applied state while the store is
        // still behind the Arc, so the durability of this stop does not
        // depend on the unwrap below succeeding. Nothing new is proposed any
        // more (the drivers and the apiserver are gone, the pump is stopped);
        // an entry already in flight may still land, and terminate's own
        // flush catches it.
        let flushed = store.flush().await?;
        info!(?flushed, "store flushed at shutdown");
        let counts = StrongCounts {
            drivers_awaited,
            apiserver_stopped,
            store_quiesced,
            store_flushed: Arc::strong_count(&store),
        };

        // Now the Runtime should hold the only strong ref. Take it.
        let store = Arc::try_unwrap(store).map_err(|shared| {
            let strong_count = Arc::strong_count(&shared);
            let after = counts.blame();
            error!(
                %after,
                strong_count,
                after_drivers_awaited = counts.after(ShutdownStage::DriversAwaited),
                after_apiserver_stopped = counts.after(ShutdownStage::ApiserverStopped),
                after_store_quiesced = counts.after(ShutdownStage::StoreQuiesced),
                after_store_flushed = counts.after(ShutdownStage::StoreFlushed),
                "store still shared at shutdown (already flushed); strong count after each stage"
            );
            RuntimeError::StoreStillShared {
                strong_count,
                after,
            }
        })?;
        store.terminate().await?;
        Ok(StoreReleased::minted())
    }
}

/// What [`Runtime::assemble`] built over the store.
struct Assembled {
    apiserver: ApiServer,
    children: Children,
    health: Arc<Health>,
    publish: Vec<PublishRecord>,
    publisher: Publisher,
    server_sans: Option<Vec<String>>,
}

/// What publishing the kubeconfigs takes that the configuration does not
/// say, kept from the boot so a changed publish leaf can be applied to the
/// running runtime: the address the apiserver bound, the CA its certificate
/// chains to, and the admin credential (already on disk under `pki/`).
struct Publisher {
    bound_addr: SocketAddr,
    ca_pem: Option<String>,
    admin: Option<ClientMaterial>,
}

impl Publisher {
    const fn new(
        bound_addr: SocketAddr,
        ca_pem: Option<String>,
        admin: Option<ClientMaterial>,
    ) -> Self {
        Self {
            bound_addr,
            ca_pem,
            admin,
        }
    }

    /// Publish every kubeconfig `boot` asks for. With TLS off there is
    /// nothing to hand kubectl, and every target says so.
    fn publish(&self, boot: &BootConfig) -> Result<Vec<PublishRecord>, RuntimeError> {
        self.ca_pem.as_deref().map_or_else(
            || Ok(PublishRecord::all_skipped(SkipReason::TlsDisabled)),
            |ca_pem| write_boot_kubeconfig(boot, self.bound_addr, ca_pem, self.admin.as_ref()),
        )
    }
}

/// Wait for raft leadership. The one wait the boot does not bound by its own
/// work, so a stop requested through `rec` ends it.
async fn await_leadership(
    store: &StoreMesh,
    boot: &BootConfig,
    rec: &BootRecorder,
) -> Result<(), RuntimeError> {
    let timeout_s = boot.leadership_timeout_seconds;
    let led = tokio::select! {
        led = store.wait_for_leadership(Duration::from_secs(u64::from(timeout_s))) => led,
        () = rec.cancelled() => {
            return Err(RuntimeError::BootCancelled {
                phase: BootPhase::AwaitLeadership,
            });
        }
    };
    if !led {
        return Err(RuntimeError::LeadershipTimeout { seconds: timeout_s });
    }
    info!(node = %boot.node_name, "store reached leadership");
    Ok(())
}

/// What [`BootPhase::IssuePki`] produced: the server's TLS material and what
/// the boot kubeconfig is written from. All absent with TLS off.
#[derive(Default)]
struct IssuedPki {
    tls: Option<TlsMaterial>,
    /// The cluster CA, for the kubeconfig's `certificate-authority-data`.
    ca_cert_pem: Option<String>,
    /// The admin client cert, for the admin-cert kubeconfig.
    admin: Option<ClientMaterial>,
}

/// Step 5a of the boot: load-or-generate the cluster CA persisted in
/// `data_dir`, then issue a server cert whose SANs cover loopback + node
/// name + the concrete listen IP (skipping `0.0.0.0` and `::`, which aren't
/// valid SAN IPs — loopback access rides on `127.0.0.1` and `localhost`).
///
/// For authn: also issue + persist the admin CLIENT cert and build the
/// OPTIONAL client-cert verifier (rooted at the SAME CA) the server material
/// carries.
fn issue_pki(boot: &BootConfig, listen_addr: SocketAddr) -> Result<IssuedPki, RuntimeError> {
    let ApiserverTls::SelfIssued { extra_sans } = &boot.tls else {
        return Ok(IssuedPki::default());
    };
    let ca = load_or_generate_ca(&boot.data_dir).map_err(|e| RuntimeError::Server(e.into()))?;
    // ── ★ A PUBLIC CA MAY NOT SERVE A REACHABLE ADDRESS ──────────
    // See `RuntimeError::PublicCaOnReachableAddress`. The danger is not
    // the CA by itself and not the address by itself; it is the pair,
    // so the pair is what is refused. A pre-seed cluster stays usable on
    // loopback and cannot be exposed by accident.
    if ca.is_publicly_derivable() && !is_loopback_only(listen_addr) {
        return Err(RuntimeError::PublicCaOnReachableAddress {
            listen_addr: boot.listen_addr.clone(),
            pki_dir: boot.data_dir.join("pki").display().to_string(),
        });
    }
    let listen_ip = san_listen_ip(listen_addr);
    // Classify the operator's declared SANs before anything binds, so a
    // typo is a failed unit with the offending value in the message
    // rather than a certificate that serves happily and verifies for
    // nobody. See `RuntimeError::ExtraSan` for why this is checked here
    // and not lazily at handshake time.
    let extra_sans = server_sans(extra_sans, &boot.advertise_address)?;
    let material = issue_server_material(
        &ca,
        &ServerSanInputs {
            node_name: &boot.node_name,
            listen_ip,
            extra_sans: &extra_sans,
        },
    )
    .map_err(|e| RuntimeError::Server(e.into()))?;
    // OPTIONAL client-cert verifier (allow_unauthenticated): existing
    // token/anonymous kubectl keeps connecting; a presented cert is
    // verified against the CA before the handshake completes.
    let verifier = client_verifier(&ca).map_err(|e| RuntimeError::Server(e.into()))?;
    // Issue + persist the admin client cert (for the operator's
    // kubeconfig + `kubectl auth whoami → engenho-admin`).
    let admin = issue_admin_client_material(&ca).map_err(|e| RuntimeError::Server(e.into()))?;
    persist_admin_material(&boot.data_dir, &admin)?;
    Ok(IssuedPki {
        tls: Some(material.with_client_verifier(verifier)),
        ca_cert_pem: Some(ca.cert_pem().to_string()),
        admin: Some(admin),
    })
}

/// Take back the store of a boot that failed after opening it.
///
/// By the time this runs the assembly has dropped everything it built and
/// stopped the apiserver, so the caller's `Arc` should be the only one. The
/// store's own tasks are stopped first, as in [`Runtime::shutdown`], so a
/// failed unwrap still leaves nothing running behind the leaked clone.
async fn unwind_failed_boot(store: Arc<StoreMesh>) -> BootUnwind {
    let quiesced = store.quiesce().await;
    if quiesced.any_panicked() {
        error!(
            rpc_pump = ?quiesced.rpc_pump,
            bookmark_ticker = ?quiesced.bookmark_ticker,
            "a store background task had panicked before the failed boot unwound"
        );
    }
    match Arc::try_unwrap(store) {
        Ok(mesh) => match mesh.terminate().await {
            Ok(()) => {
                info!("failed boot unwound: store terminated and released");
                BootUnwind::Released(StoreReleased::minted())
            }
            Err(err) => {
                error!(error = %err, "failed boot unwound, but terminating the store failed");
                BootUnwind::TerminateFailed(Box::new(err.into()))
            }
        },
        Err(shared) => {
            let strong_count = Arc::strong_count(&shared);
            error!(
                strong_count,
                "failed boot could not release its store: something still holds it"
            );
            BootUnwind::StillShared { strong_count }
        }
    }
}

impl BootFailed {
    /// The boot's error, for the callers of [`Runtime::start`] that only
    /// need that. An unwind that did not release the store is logged here,
    /// since those callers cannot see it.
    fn into_logged_error(self) -> RuntimeError {
        match &self.unwind {
            BootUnwind::NeverOpened | BootUnwind::Released(_) => {}
            BootUnwind::StillShared { strong_count } => error!(
                strong_count,
                error = %self.error,
                "boot failed and its store is still held"
            ),
            BootUnwind::TerminateFailed(err) => error!(
                terminate_error = %err,
                error = %self.error,
                "boot failed and its store could not be terminated"
            ),
        }
        self.error
    }
}

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
///
/// Holds the kubelet STRONGLY, unlike the :10250 listener
/// ([`WeakKubeletApi`]): it lives in the apiserver's router, which already
/// holds the store through every handler, and is released with that router
/// when the apiserver stops ([`ShutdownStage::ApiserverStopped`]).
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

/// Longest stderr excerpt carried into a preflight error, in bytes.
const STDERR_TAIL_MAX: usize = 400;

/// The last meaningful line of a failed subprocess's stderr, ready to append.
///
/// Returns `": <line>"`, or empty when there is nothing to say, so a caller
/// never branches on it.
///
/// The LAST non-empty line is the right one for the tools this fronts: podman
/// prints a human preamble first (`OS: …`, `provider: …`) and the actual
/// `Error:` last, so taking the head would reliably select the noise.
///
/// Bounded on purpose. An error message is read by a human in a log, and
/// pasting a runtime's unbounded chatter is how the one useful line gets
/// scrolled away — the same failure-to-communicate this whole preflight exists
/// to fix, arrived at from the opposite direction.
fn stderr_tail(stderr: &[u8]) -> String {
    let text = String::from_utf8_lossy(stderr);
    let Some(line) = text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .next_back()
    else {
        return String::new();
    };
    let mut out = String::from(": ");
    if line.len() > STDERR_TAIL_MAX {
        // Walk back to a char boundary — slicing UTF-8 at an arbitrary byte
        // index panics, and a runtime's error text is not guaranteed ASCII.
        let mut end = STDERR_TAIL_MAX;
        while end > 0 && !line.is_char_boundary(end) {
            end -= 1;
        }
        out.push_str(&line[..end]);
        out.push('…');
    } else {
        out.push_str(line);
    }
    out
}

/// Verify at BOOT that a configured container runtime is actually usable.
///
/// A backend the kubelet refuses to construct
/// ([`KubeletBackendKind::refusal`], T5.9) is refused here FIRST, with that
/// refusal, before anything is probed. The `Fake` and `Native` backends need
/// nothing. For `Podman` this runs `podman info`,
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
fn preflight_backend(boot: &BootConfig) -> Result<(), RuntimeError> {
    // ★ THE REFUSAL COMES BEFORE ANY PROBE, for every backend. It is decided
    // from the kind alone and consults nothing on the host, so a refused
    // backend gets the same verdict on every node. Probing first let the HOST
    // pick the error: a `cri` node without podman failed on `podman info`,
    // naming a runtime it was configured not to use, and only a node that
    // happened to have a working podman got as far as the refusal that says
    // why (I38, node T5.9).
    if let Some(refused) = engenho_kubelet::config_bridge::construction_refusal(
        kubelet_backend_kind(boot.kubelet_backend),
    ) {
        return Err(RuntimeError::BackendRefused(refused));
    }
    // Exhaustive on purpose. This used to be `if matches!(.., Fake)`, which
    // meant every NEW backend silently inherited a podman probe — and a
    // backend with no podman under it then failed to start with an error
    // naming podman, on a node deliberately configured not to use it.
    // Measured on ryn 2026-09-17: `kubelet_backend: native` in the config,
    // `backend="podman"` in the log, and a daemon that refused to come up.
    match boot.kubelet_backend {
        // Runs no containers at all.
        CfgBackendKind::Fake => return Ok(()),
        // No container runtime underneath: a host process out of a Nix
        // closure. There is no podman to probe, and probing one would make a
        // node that cannot reach podman unable to run the backend that does
        // not need it.
        CfgBackendKind::Native => return Ok(()),
        // Reached only once the kubelet ADMITS CRI, which today is never: the
        // refusal above returns while `cri_backend::UNSUPPORTED` is
        // non-empty. CRI dials its own endpoint (containerd / CRI-O), so there
        // is no podman under it to probe.
        //
        // pending-cri: an admitted CRI still falls back to podman when no CRI
        // socket answers (`make_container_runtime_with_apiserver`), so the
        // probe that matches what gets built is "a CRI socket, else podman".
        // The `a_cri_node_*` tests fail the day CRI is admitted, and say so.
        CfgBackendKind::Cri => return Ok(()),
        CfgBackendKind::PodmanApi | CfgBackendKind::Podman => {}
    }
    let binary = boot
        .podman_binary
        .clone()
        .unwrap_or_else(|| "podman".to_string());
    // `output()` rather than `status()` — it captures stderr, which is the
    // only part of a failed run that says WHY. See the error arm below.
    match std::process::Command::new(&binary).arg("info").output() {
        // Ok whenever the process RAN — including when it ran and reported a
        // dead connection. The exit code is the part that carries the verdict,
        // and ignoring it is how the weak `--version` check passed on a
        // runtime that could not start a single container.
        Ok(out) if out.status.success() => {
            info!(backend = "podman", %binary, "container runtime resolved");
            Ok(())
        }
        // ★ The backend's stderr is CARRIED, not dropped. An exit code names
        // THAT it failed; only stderr names WHY, and the gap between those two
        // is paid by whoever is holding the broken node.
        //
        // Measured 2026-09-13 on ryn: two malformed lines in the operator's
        // `~/.ssh/known_hosts` (a key with no host pattern, from an append
        // whose host variable was empty) made podman's Go `knownhosts` parser
        // refuse the machine connection — so every pod on the node was
        // unrunnable. OpenSSH's own client skips such lines with a warning, so
        // `ssh` kept working and nothing else pointed at the file.
        //
        // `podman info` exited 125 and said exactly which file and line:
        //   `knownhosts: /Users/…/.ssh/known_hosts:9: missing key type pattern`
        // This preflight discarded that sentence and reported the number
        // alone, turning a one-line read into a hunt through the ssh stack.
        Ok(out) => Err(RuntimeError::ContainerRuntimeUnavailable {
            backend: "podman".to_string(),
            binary,
            source: std::io::Error::other(
                match out.status.code() {
                    Some(c) => ["`podman info` exited ", itoa_i32(c).as_str()].concat(),
                    None => "`podman info` was terminated by a signal".to_string(),
                } + stderr_tail(&out.stderr).as_str(),
            ),
        }),
        Err(source) => Err(RuntimeError::ContainerRuntimeUnavailable {
            backend: "podman".to_string(),
            binary,
            source,
        }),
    }
}

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

    /// Derived from the SAME `lifetime_secs` the token is minted with, so the
    /// kubelet's refresh cadence cannot drift from the expiry it is racing.
    fn token_lifetime(&self) -> Option<std::time::Duration> {
        u64::try_from(self.lifetime_secs)
            .ok()
            .map(std::time::Duration::from_secs)
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
fn apiserver_reachability(boot: &BootConfig) -> ApiserverReachability {
    // The port engenho really listens on. `listen_addr` is `host:port`; a
    // shape we cannot parse is a reason to inject nothing, never to guess.
    let Some(port) = boot
        .listen_addr
        .rsplit(':')
        .next()
        .and_then(|p| p.parse::<u16>().ok())
    else {
        return ApiserverReachability::Unknown;
    };

    match boot.kubelet_backend {
        // Pods run in podman. On darwin they are inside a VM and cannot reach
        // a host-loopback apiserver by any cluster address, and even on Linux
        // engenho installs no kube-proxy datapath for the VIP — so the
        // gateway is correct on both, and the VIP is correct on neither.
        // Both podman backends drive the same container store, so pods reach
        // the apiserver by the same route regardless of how the kubelet talks
        // to podman. The seam that changed is client-side only.
        CfgBackendKind::PodmanApi | CfgBackendKind::Podman => ApiserverReachability::HostGateway {
            host: PODMAN_HOST_GATEWAY.to_string(),
            port,
        },
        // ★ NATIVE: a pod IS a host process, so it shares the host's network
        // and reaches the apiserver on LOOPBACK directly. This is the one
        // backend where the address the kubelet listens on is literally the
        // address a pod can use — there is no VM boundary to cross and no
        // gateway name to translate. Stated as its own arm rather than folded
        // in with podman's: the two are the same shape and a different fact.
        CfgBackendKind::Native => ApiserverReachability::HostGateway {
            host: "127.0.0.1".to_string(),
            port,
        },
        // ★ CRI: HONESTLY UNKNOWN, NOT GUESSED. A pod under containerd/CRI-O
        // is on a CNI-managed network, not podman's bridge, so
        // `PODMAN_HOST_GATEWAY` is simply the wrong host — and engenho does
        // not yet attach pods to any network under CRI (`run_chain` has no
        // production caller), so there is no address to name. `Unknown`
        // injects nothing and warns, which leaves in-cluster config failing to
        // CONSTRUCT and the client falling back to a kubeconfig. That is the
        // rule this function already states for an unparseable port: a guessed
        // address is worse than none, because it removes the fallback and turns
        // a clear failure into a connection timeout.
        CfgBackendKind::Cri => ApiserverReachability::Unknown,
        // Nothing is dialled under the fake backend, so there is nothing to
        // be reachable. Injecting the conventional VIP keeps the env shape
        // that tests assert on.
        CfgBackendKind::Fake => ApiserverReachability::ServiceVip {
            ip: DEFAULT_KUBERNETES_SERVICE_IP.to_string(),
            port: 443,
        },
    }
}

/// The kubelet's name for the configured backend.
///
/// One mapping, read by [`preflight_backend`] for the refusal and by
/// [`build_backend`] for construction, so the kind that is checked is the kind
/// that is built. pending-I39: the two enums have identical arms and are to
/// collapse into one.
const fn kubelet_backend_kind(kind: CfgBackendKind) -> KubeletBackendKind {
    match kind {
        CfgBackendKind::Cri => KubeletBackendKind::Cri,
        CfgBackendKind::PodmanApi => KubeletBackendKind::PodmanApi,
        CfgBackendKind::Podman => KubeletBackendKind::Podman,
        CfgBackendKind::Fake => KubeletBackendKind::Fake,
        CfgBackendKind::Native => KubeletBackendKind::Native,
    }
}

/// Construct the container backend from the operator's config choice.
fn build_backend(boot: &BootConfig) -> Result<Arc<dyn ContainerRuntime>, RuntimeError> {
    let kind = kubelet_backend_kind(boot.kubelet_backend);
    // A node-level fact, set once here rather than resolved per pod — and it
    // must reach the backend, because the kubelet has THREE `backend.start`
    // call sites and stamping any one of them misses the restart path.
    //
    // What is injected is a REACHABILITY CLAIM, not a constant: see
    // `ApiserverReachability`. `None` means "tell pods nothing", which is a
    // working outcome (kubeconfig fallback), not a degraded one.
    let reachability = apiserver_reachability(boot);
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
                listen_addr = %boot.listen_addr,
                "cannot determine an apiserver address pods can reach; \
                 injecting no KUBERNETES_SERVICE_* env so in-cluster config \
                 fails construction and clients fall back to a kubeconfig"
            );
        }
    }
    Ok(make_container_runtime_with_apiserver(
        kind,
        boot.podman_binary.as_deref(),
        reachability.injectable(),
    )?)
}

/// The directory under `data_dir` a durable node keeps its store in. The
/// census ([`crate::census::DataDirSource`]) reads a node's store from the
/// same place.
pub(crate) const STORE_DIR: &str = "store";

/// Bring up the store spine — durable or ephemeral per config.
async fn boot_store(boot: &BootConfig) -> Result<(Arc<StoreMesh>, BootKind), RuntimeError> {
    let cfg = default_config(&boot.cluster_name)?;
    let router = InProcessRouter::new();
    // Single-node self-loop address; registration happens inside start.
    let listen = "in-process://1".to_string();

    if boot.durable {
        let store_path = boot.data_dir.join(STORE_DIR);
        let (mesh, fresh) = StoreMesh::start_or_resume(1, listen, router, cfg, store_path).await?;
        let kind = if fresh {
            BootKind::FirstBoot
        } else {
            BootKind::Resume
        };
        info!(?kind, "durable store opened");
        Ok((Arc::new(mesh), kind))
    } else {
        let mesh = StoreMesh::start(1, listen, router, cfg).await?;
        mesh.initialize_singleton().await?;
        info!("ephemeral store initialized");
        Ok((Arc::new(mesh), BootKind::Ephemeral))
    }
}

/// Inject `metadata.creationTimestamp` (if absent) into an opaque JSON
/// body from the typed RFC3339 boundary render — the non-handler seeders
/// (and node registration's create path) route through this so every born
/// object, including the self-registered Node, carries a real
/// creationTimestamp. Mirrors the apiserver handler's
/// `stamp_creation_timestamp`.
pub(crate) fn stamp_creation_timestamp_value(body: &mut serde_json::Value) {
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
/// preserves `metadata.uid` across restarts.
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
/// Idempotent across restarts: the apply path preserves `metadata.uid` and `creationTimestamp` on a Put over
/// an existing key, so re-seeding an unchanged namespace is a no-op rather
/// than a new object identity.
///
/// Each is built as a TYPED [`Namespace`] rather than a `json!()` literal, so
/// a field that does not exist is a compile error — the shape
/// `seed_bootstrap_rbac` established.
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

/// The name of the seeded default StorageClass.
const DEFAULT_STORAGE_CLASS: &str = "engenho-local-path";

/// Seed the cluster's default `StorageClass`, so a PVC that names no class is
/// actually provisionable.
///
/// ── ★ WHY A CAPABILITY THAT EXISTS STILL DID NOTHING ───────────────────────
/// `PvBinderController` has shipped a local-path dynamic provisioner for some
/// time, and it was never reachable: it provisions only for a PVC whose
/// effective StorageClass names a local-path provisioner OR carries the
/// default-class annotation, and NO StorageClass was ever seeded. A cluster
/// therefore had a working provisioner, an empty class list, and every PVC
/// sitting `Pending` forever.
///
/// Measured 2026-09-13 on ryn: a 1Gi PVC with no class stayed unbound with
/// `pv-binder examined=1 changed=0 skipped=1` — the controller looking at the
/// claim each tick and correctly declining, because nothing told it which
/// provisioner to use. That is indistinguishable, from the outside, from a
/// runtime with no storage support at all.
///
/// Seeded like the bootstrap RBAC and the `kubernetes` Service: idempotent,
/// at boot, before the apiserver binds. An operator who wants different
/// storage edits or replaces the class; an operator who wants none removes the
/// default annotation. Seeding a WORKING default is the difference between a
/// runtime that stores things and one that merely serves the storage API.
async fn seed_default_storage_class(store: &StoreMesh) -> Result<(), RuntimeError> {
    use engenho_types::generated_v1_34::storage_v1::StorageClass;

    let mut sc = StorageClass {
        provisioner: engenho_controllers::pv_binder::ENGENHO_LOCAL_PATH_PROVISIONER.to_string(),
        ..Default::default()
    };
    sc.metadata.name = DEFAULT_STORAGE_CLASS.to_string();
    sc.metadata.annotations.insert(
        "storageclass.kubernetes.io/is-default-class".to_string(),
        "true".to_string(),
    );
    // `Delete` matches the provisioner's own lifecycle: the backing directory
    // lives under the data dir, so a retained PV would leak a directory nobody
    // is tracking. `Immediate` because it is the only mode the binder
    // provisions in — advertising `WaitForFirstConsumer` here would promise
    // behaviour it typed-defers.
    sc.reclaim_policy = Some("Delete".to_string());
    sc.volume_binding_mode = Some("Immediate".to_string());

    let mut value = serde_json::to_value(&sc)
        .map_err(|e| RuntimeError::Server(seed_serialize_err("StorageClass", &e)))?;
    stamp_creation_timestamp_value(&mut value);
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::cluster_scoped(
                "storage.k8s.io",
                "v1",
                "StorageClass",
                DEFAULT_STORAGE_CLASS.to_string(),
            ),
            value,
            expected: None,
            reason: Reason::Operator,
        })
        .await?;
    info!(
        class = DEFAULT_STORAGE_CLASS,
        provisioner = engenho_controllers::pv_binder::ENGENHO_LOCAL_PATH_PROVISIONER,
        "seeded the default StorageClass (PVCs with no class are now provisionable)"
    );
    Ok(())
}

/// Seed the `snapshot.storage.k8s.io/v1` CRDs that
/// [`engenho_controllers::volume_snapshot`] serves.
///
/// ── ★ WHY THESE ARE CRDs AND NOT BUILT-IN KINDS ────────────────────────────
/// They are CRDs upstream too. `VolumeSnapshot` is not part of Kubernetes
/// proper: the external-snapshotter project ships these three definitions and
/// a controller, and each CSI driver implements the actual copy. Declaring
/// them here as CRDs is therefore the FAITHFUL shape, not a shortcut — a
/// client that installs the upstream definitions sees the same group, version
/// and names.
///
/// Seeded rather than left to the operator because the pairing is what makes
/// the feature real: engenho ships the controller, so shipping the API it
/// reconciles is part of the same promise. A controller watching a kind the
/// cluster does not serve is the "declared but unreachable" failure this whole
/// change set exists to remove.
///
/// The schema is deliberately permissive (`x-kubernetes-preserve-unknown-fields`)
/// — validation is the CRD layer's DEFERRED concern here, and a partial schema
/// that silently PRUNES an unknown field is worse than none: a pruned
/// `spec.source` would make a snapshot request vanish with no error.
async fn seed_snapshot_crds(store: &StoreMesh) -> Result<(), RuntimeError> {
    // (kind, plural, singular, namespaced)
    let kinds = [
        ("VolumeSnapshot", "volumesnapshots", "volumesnapshot", true),
        (
            "VolumeSnapshotContent",
            "volumesnapshotcontents",
            "volumesnapshotcontent",
            false,
        ),
        (
            "VolumeSnapshotClass",
            "volumesnapshotclasses",
            "volumesnapshotclass",
            false,
        ),
    ];
    for (kind, plural, singular, namespaced) in kinds {
        let name = [
            plural,
            ".",
            engenho_controllers::volume_snapshot::SNAPSHOT_GROUP,
        ]
        .concat();
        let list_kind = [kind, "List"].concat();
        let crd = serde_json::json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": { "name": name },
            "spec": {
                "group": engenho_controllers::volume_snapshot::SNAPSHOT_GROUP,
                "scope": if namespaced { "Namespaced" } else { "Cluster" },
                "names": {
                    "kind": kind,
                    "listKind": list_kind,
                    "plural": plural,
                    "singular": singular,
                },
                "versions": [{
                    "name": engenho_controllers::volume_snapshot::SNAPSHOT_VERSION,
                    "served": true,
                    "storage": true,
                    "subresources": { "status": {} },
                    "schema": {
                        "openAPIV3Schema": {
                            "type": "object",
                            "x-kubernetes-preserve-unknown-fields": true
                        }
                    }
                }]
            }
        });
        let mut value = crd;
        stamp_creation_timestamp_value(&mut value);
        store
            .propose(ResourceCommand::Put {
                key: ResourceKey::cluster_scoped(
                    "apiextensions.k8s.io",
                    "v1",
                    "CustomResourceDefinition",
                    name,
                ),
                value,
                expected: None,
                reason: Reason::Operator,
            })
            .await?;
    }
    info!(
        group = engenho_controllers::volume_snapshot::SNAPSHOT_GROUP,
        count = kinds.len(),
        "seeded the VolumeSnapshot CRDs"
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
async fn seed_kubernetes_service(store: &StoreMesh, boot: &BootConfig) -> Result<(), RuntimeError> {
    // The CIDR may legitimately be empty (a control-plane-only node that
    // allocates no VIPs). Nothing to reserve, nothing to seed.
    if boot.service_cidr.is_empty() {
        return Ok(());
    }
    let mut allocator =
        match engenho_controllers::cluster_ip::ClusterIpAllocator::new(&boot.service_cidr) {
            Ok(a) => a,
            Err(e) => {
                // A malformed CIDR is the allocator's problem to report at its
                // own boundary, not a reason to refuse to boot the whole node.
                warn!(
                    cidr = %boot.service_cidr,
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

    let port = boot
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

/// Parse `MemTotal` out of `/proc/meminfo`, in bytes.
///
/// Pure so it is testable without a `/proc`. The line is
/// `MemTotal:       32793532 kB` — the unit is ALWAYS kB on Linux (the
/// kernel hardcodes it in `fs/proc/meminfo.c`), but it is parsed rather
/// than assumed, because silently reading kB as bytes understates the
/// node by 1024× and the resulting number still looks plausible.
#[must_use]
fn parse_mem_total_bytes(meminfo: &str) -> Option<u64> {
    let line = meminfo.lines().find(|l| l.starts_with("MemTotal:"))?;
    let mut it = line.split_whitespace().skip(1);
    let value: u64 = it.next()?.parse().ok()?;
    match it.next() {
        Some("kB" | "KB") => Some(value * 1024),
        Some("mB" | "MB") => Some(value * 1024 * 1024),
        // No unit at all means bytes, per proc(5). An unrecognised unit is
        // refused rather than guessed — see the doc above.
        None => Some(value),
        Some(_) => None,
    }
}

/// Total host memory in bytes, or `None` when this target cannot say.
fn host_memory_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/meminfo")
            .ok()
            .as_deref()
            .and_then(parse_mem_total_bytes)
    }
    #[cfg(not(target_os = "linux"))]
    {
        // darwin would need `sysctlbyname`, i.e. libc — and engenho's
        // production target is `x86_64-unknown-linux-musl`, which links no
        // libc at all. Adding a C dependency to serve a dev-only host is
        // exactly the trade ★★ CONTAIN THE C says not to make. The caller
        // falls back and SAYS SO.
        None
    }
}

/// Host capacity advertised on the self-registered Node: `(cpu, memory)`
/// as K8s quantity strings.
///
/// ── ★ THE MEMORY VALUE WAS A HARDCODED `"8Gi"` STRING UNTIL 2026-09-14 ─────
/// Not a probe that fell back — a literal, on every node, forever. The old
/// comment defended it as "a truthful lower bound", and that defence fails in
/// both directions: on a node with less than 8Gi it is an OVER-claim that lets
/// the scheduler pack a node into swap, and on rio it was a 3.6× UNDER-claim
/// (advertised `8Gi`, host has 29Gi) that silently capped what the node would
/// accept. Measured on the live node: `status.allocatable = {"cpu":"32",
/// "memory":"8Gi"}`.
///
/// It compounds: `engenho-scheduler`'s `fit.rs` packs by
/// `allocatable − Σ requests`, so the scheduler was doing exact arithmetic
/// against a fabricated denominator — and, until the same day, against limits
/// the node then did not enforce either.
///
/// The probe needs no new dependency. `/proc/meminfo` is a FILE; the old note
/// that this "would add a sysinfo dep" was reaching for a crate to read text.
///
/// `capacity` and `allocatable` are still reported EQUAL by the caller, which
/// is not upstream's model — upstream subtracts a system reservation. That is
/// named as `pending-node-allocatable-reservation` rather than approximated,
/// because a made-up reservation is the same class of defect as the made-up
/// total this replaces.
pub(crate) fn host_capacity() -> (String, String) {
    let cpus = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);
    let memory = match host_memory_bytes() {
        Some(bytes) => {
            // Plain bytes, not a rounded `Gi`: rounding down discards real
            // capacity and rounding up over-claims, and the scheduler parses
            // this back through the typed `Quantity` surface either way.
            bytes.to_string()
        }
        None => {
            warn!(
                "cannot read total host memory on this target; advertising the \
                 8Gi fallback — the scheduler will pack this node against a \
                 value that is not measured"
            );
            "8Gi".to_string()
        }
    };
    (cpus.to_string(), memory)
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

/// Build the ServiceAccount token MINTER for `POST serviceaccounts/<n>/token`.
///
/// The exact mirror of [`build_authenticator`], and a named function for the
/// same stated reason: the defect this whole area keeps producing is a WIRING
/// bug, where every piece works and the boot sequence calls the inert
/// constructor. Pulling it out makes the wiring assertable.
///
/// ── ★ WHY ISSUER AND AUDIENCE COME FROM THE SAME CONSTANT AS THE VERIFIER ──
/// Both read `SA_ISSUER`, exactly as `build_authenticator` does. Split into
/// two literals they are free to drift, and the failure mode is silent in the
/// worst way: the server mints a token its OWN authenticator then rejects,
/// which reads as a key problem and sends the reader to the PKI.
///
/// A key that cannot be read yields `None` — `/token` then answers a typed
/// error rather than minting something unverifiable. Refusing to issue must
/// never degrade into issuing.
fn build_token_issuer(
    data_dir: &std::path::Path,
) -> Option<Arc<engenho_apiserver::sa_token::SaIssuer>> {
    match engenho_apiserver::sa_token::load_or_generate_sa_key(data_dir) {
        Ok(kp) => {
            info!("ServiceAccount token issuance enabled (POST serviceaccounts/<name>/token)");
            Some(Arc::new(engenho_apiserver::sa_token::SaIssuer {
                signing: kp.signing,
                issuer: SA_ISSUER.to_string(),
                default_audience: SA_ISSUER.to_string(),
            }))
        }
        Err(e) => {
            warn!(
                error = %e,
                "no ServiceAccount signing key; `kubectl create token` and every in-cluster \
                 identity will take a typed error"
            );
            None
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
fn server_sans(
    extra_sans: &[String],
    advertise_address: &str,
) -> Result<Vec<SanEntry>, RuntimeError> {
    let mut sans = parse_extra_sans(extra_sans)?;
    if let Some(host) = advertised_host(advertise_address) {
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
fn advertised_server_url(advertise_address: &str, bound: SocketAddr) -> Option<String> {
    let value = advertise_address.trim();
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

/// Whether this bind address can only be reached from the host itself.
///
/// `0.0.0.0` and `::` are UNSPECIFIED, not loopback: they bind every interface,
/// which is the most reachable address there is. Treating them as "no specific
/// address, therefore safe" is the inversion this function exists to prevent —
/// and it is an easy one to write, because `is_loopback()` returns false for
/// them and a careless guard reads that as "not remote".
fn is_loopback_only(addr: SocketAddr) -> bool {
    addr.ip().is_loopback()
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
    boot: &BootConfig,
    bound_addr: SocketAddr,
    ca_pem: &str,
    admin: Option<&ClientMaterial>,
) -> Result<Vec<PublishRecord>, RuntimeError> {
    // One kubeconfig for `server`: the same CA and credentials, whatever the
    // address.
    let emit = |server: &str| {
        match admin {
            Some(admin) => emit_kubeconfig_with_admin(
                &boot.cluster_name,
                server,
                ca_pem.as_bytes(),
                admin.cert_pem.as_bytes(),
                admin.key_pem.as_bytes(),
            ),
            None => emit_kubeconfig(&boot.cluster_name, server, ca_pem.as_bytes()),
        }
        .map_err(|e| RuntimeError::Kubeconfig(e.to_string()))
    };
    // A publish that fails is recorded and logged, never fatal (see below).
    let publish = |target: KubeconfigTarget, path: &std::path::Path, yaml: &str, what: &str| {
        let visibility = boot.kubeconfig_publish_visibility;
        match write_kubeconfig_file(path, yaml, visibility) {
            Ok(()) => {
                info!(path = %path.display(), "{what} published");
                PublishRecord::written(target, path, visibility.mode())
            }
            Err(e) => {
                tracing::warn!(
                    path = %path.display(), error = %e,
                    "{what} publish failed — the daemon is serving"
                );
                PublishRecord::failed(target, path, &e)
            }
        }
    };
    // Loopback server URL with the actually-bound port (handles `:0`).
    let server_url = loopback_server_url(bound_addr);
    let yaml = emit(&server_url)?;
    let path = boot.data_dir.join("kubeconfig");
    // The `data_dir` copy is engenho's own bookkeeping and nothing else reads
    // it, so it stays owner-only regardless of the publish intent — widening
    // it would grant access nobody asked for.
    write_kubeconfig_file(&path, &yaml, KubeconfigVisibility::Private)?;
    info!(path = %path.display(), server = %server_url, admin = admin.is_some(), "kubeconfig written");
    let mut records = vec![PublishRecord::written(
        KubeconfigTarget::DataDir,
        &path,
        KubeconfigVisibility::Private.mode(),
    )];

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
    // ★ CORRECTED 2026-09-13. This used to say in-cluster config "does not
    // work here" because the authenticator returned `service account token
    // authentication is not yet supported` (401). That is no longer true —
    // the authenticator is wired to `pki/sa.key` and `/token` mints tokens it
    // accepts (a minted token measures 403 unbound, 200 once bound).
    //
    // The file still exists, for the ADDRESS rather than the identity:
    // the operator-facing kubeconfig names LOOPBACK, and on darwin the
    // apiserver binds the host while containers live in a VM. A pod needs a
    // reachable address; it no longer needs borrowed admin credentials to be
    // ALLOWED, so prefer a ServiceAccount for new workloads.
    records.push(
        match resolve_publish_path(&boot.pod_kubeconfig_publish_path) {
            Err(reason) => PublishRecord::skipped(KubeconfigTarget::Pod, reason),
            Ok(pod_publish) => {
                if let Some((host, port)) = apiserver_reachability(boot).injectable() {
                    let pod_yaml = emit(&format!("https://{host}:{port}"))?;
                    publish(
                        KubeconfigTarget::Pod,
                        &pod_publish,
                        &pod_yaml,
                        "pod-facing kubeconfig",
                    )
                } else {
                    // No reachable address is known, so there is no honest
                    // server URL to write. Writing one anyway would hand a
                    // workload a kubeconfig that cannot connect, which is the
                    // failure this whole path exists to remove.
                    tracing::warn!(
                        "pod-facing kubeconfig requested but no pod-reachable apiserver \
                         address is known; writing nothing rather than an unusable file"
                    );
                    PublishRecord::skipped(KubeconfigTarget::Pod, SkipReason::NoPodAddress)
                }
            }
        },
    );

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
    records.push(
        match resolve_publish_path(&boot.remote_kubeconfig_publish_path) {
            Err(reason) => PublishRecord::skipped(KubeconfigTarget::Remote, reason),
            Ok(remote_publish) => {
                if let Some(remote_server) =
                    advertised_server_url(&boot.advertise_address, bound_addr)
                {
                    let remote_yaml = emit(&remote_server)?;
                    publish(
                        KubeconfigTarget::Remote,
                        &remote_publish,
                        &remote_yaml,
                        "remote kubeconfig",
                    )
                } else {
                    // Asked for a remote kubeconfig without saying what address is
                    // remote. Writing a loopback url under that name would be worse
                    // than writing nothing: it produces a file that looks like remote
                    // access and silently is not.
                    tracing::warn!(
                        "remote_kubeconfig_publish_path is set but advertise_address is empty; \
                     writing nothing rather than a file with a loopback server url"
                    );
                    PublishRecord::skipped(KubeconfigTarget::Remote, SkipReason::NoAdvertiseAddress)
                }
            }
        },
    );

    // `$KUBECONFIG` will not see this cluster until a failed path is writable.
    records.push(match resolve_publish_path(&boot.kubeconfig_publish_path) {
        Err(reason) => PublishRecord::skipped(KubeconfigTarget::Operator, reason),
        Ok(path) => publish(
            KubeconfigTarget::Operator,
            &path,
            &yaml,
            "kubeconfig for kubectl/k9s/flux",
        ),
    });
    Ok(records)
}

/// Expand the configured publish path, or `None` when publishing is off.
///
/// Handles a leading `~/` because the default is written as a portable
/// string in config (`~/.kube/configs/engenho`) rather than a resolved
/// path — the config layer must stay a pure value with no `$HOME` baked
/// into it, or a rendered config would only be valid for the user who
/// generated it.
fn resolve_publish_path(raw: &str) -> Result<std::path::PathBuf, SkipReason> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(SkipReason::NotConfigured);
    }
    if let Some(rest) = raw.strip_prefix("~/") {
        // `HOME` unset (some launchd contexts) means there is no home to
        // publish into — skip rather than write to a relative path that
        // would land wherever the daemon happens to be running.
        let home = std::env::var("HOME")
            .ok()
            .filter(|h| !h.is_empty())
            .ok_or(SkipReason::NoHome)?;
        return Ok(std::path::PathBuf::from(home).join(rest));
    }
    Ok(std::path::PathBuf::from(raw))
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
fn write_kubeconfig_file(
    path: &std::path::Path,
    contents: &str,
    visibility: KubeconfigVisibility,
) -> Result<(), RuntimeError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| RuntimeError::KubeconfigIo {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    // ── ★ ENGENHO IS THE LAST WRITER, SO THE MODE MUST BE DECIDED HERE ──
    // Measured on plo 2026-09-07: a systemd oneshot existed to widen the
    // published kubeconfig to 0640, reported success, the operator was in the
    // owning group — and the file was still 0600 with ctime/mtime identical to
    // this write. Nothing outside can win against a writer that runs on every
    // boot; see `KubeconfigVisibility` for why no ordering fixes it.
    //
    // The write is in-place (`std::fs::write` truncates rather than unlinking),
    // so the file's GROUP survives and the declarative layer owns that once.
    write_at_mode(path, contents, visibility.mode())
}

/// Where CNI network configuration lives. Upstream's path, and not
/// configurable today on purpose: every CNI installer writes here, and a
/// configurable directory that nobody sets is a knob whose only effect is
/// to let one operator point engenho at an empty dir by accident.
const CNI_CONFIG_DIR: &str = "/etc/cni/net.d";

/// Whether this node's pods are attached by a CNI plugin chain.
///
/// ── ★ CORRECTED 2026-09-14: THIS SAID `Invoked` ON LINUX AND IT WAS FALSE ──
/// `CniInstall::Invoked` is defined as *"the plugin chain ran; the pod IP is
/// the chain's result"*, and `Planned` as *"NO plugin was executed — the pod IP
/// came from the container runtime instead"*. The second is what actually
/// happens on every engenho node: `engenho_cni::exec::run_chain` has **zero
/// non-test callers** (measured across the whole tree), so no chain has ever
/// run and every pod IP comes from podman.
///
/// Measured on rio the same day, which is what makes this a correction rather
/// than a tidy-up. Its Node object carried:
///
/// ```text
/// engenho.io/cni-install  Invoked
/// engenho.io/cni-network  cbr0
/// engenho.io/cni-config   /etc/cni/net.d/10-flannel.conflist
/// ```
///
/// All three are wrong together, and the third explains the second: that
/// flannel conflist belongs to **k3s**, which shares `/etc/cni/net.d` with us —
/// exactly the first-lexical-wins hazard `cni_status.rs`'s own header warns
/// about. engenho attaches pods to podman's `engenho-net`, never to `cbr0`.
///
/// The annotation exists so an operator debugging an unreachable pod knows
/// *before* they "start reading plugin logs that do not exist" (that file's
/// words). Publishing `Invoked` sent them to flannel's logs for a pod that
/// never touched flannel — the annotation defeating its own stated purpose.
///
/// The original reasoning was sound about the wrong question: darwin genuinely
/// *cannot* run a plugin, so a target-conditional constant is right for
/// CAPABILITY. But this constant is read as a claim about what HAPPENED, and
/// "this build could execute plugins" is not "this build did". It stays
/// `Planned` on every target until `run_chain` has a production caller, and the
/// commit that gives it one flips this line — with the Linux arm restored,
/// since the darwin arm was never in question.
///
/// ★ AND THAT CALLER IS `butai`, NOT THE CRI BACKEND. Worth writing down
/// because it is the obvious wrong guess: under CRI the kubelet does NOT
/// invoke CNI. The vendored `runtime/v1/api.proto` mentions netns and CNI
/// exactly zero times, there is no field for a kubelet to hand a network
/// namespace down, and `StopPodSandbox`'s own contract is that it "reclaims
/// network resources (e.g., IP addresses) allocated to the sandbox" — i.e.
/// containerd/CRI-O allocated them. The same is true of podman, which owns its
/// own bridge. So `run_chain` has no caller precisely because engenho has never
/// been the runtime; it acquires one when engenho IS the runtime, in `butai`'s
/// sandbox creation, and not before.
const CNI_INSTALL: engenho_cni::exec::CniInstall = engenho_cni::exec::CniInstall::Planned;

/// Build + spawn every child the catalog enables (T2.6) into ONE owned
/// [`Children`] set: the drivers gated on `controllers.enable.*`, the
/// always-on scheduler + kubelet (a single-node runtime that can't schedule
/// or run containers is useless), the :10250 / :2379 listeners, and the
/// node lease, built from the kubelet's row (T1.3c).
///
/// Returns the set PLUS an `Arc<Kubelet>` clone. The kubelet is built once
/// and shared (via the `Controller for Arc<C>` blanket impl) between its
/// driver and the apiserver's Pod `/log` reader, so both see the SAME local
/// bookkeeping; the :10250 listener reaches it through a `Weak`
/// ([`WeakKubeletApi`]).
fn spawn_children(
    boot: &BootConfig,
    store: &Arc<StoreMesh>,
    backend: &Arc<dyn ContainerRuntime>,
    scheduler: ConfiguredScheduler,
    handler_sink: &Arc<dyn DynamicHandlerSink>,
    windows: Windows,
) -> (Children, Arc<Kubelet>) {
    let parts = Parts::assemble(boot, store, backend, scheduler, handler_sink, windows);
    let children = Children::spawn_catalog(boot, |child, before| parts.task(child, before));
    (children, parts.kubelet)
}

/// The scheduler as `scheduler.*` configured it (T5.8), driven as a
/// controller: its namespace scope and strategy are inside it, and its tick
/// is the fallback [`Windows::of_child`] gives the scheduler's loop.
///
/// A forwarding adapter, like [`DeclaredHere`]: its `controller_type` is the
/// scheduler's, so the dormant-controller census sees the type that runs.
struct ConfiguredSchedulerLoop(ConfiguredScheduler);

#[async_trait::async_trait]
impl Controller for ConfiguredSchedulerLoop {
    fn name(&self) -> &'static str {
        Controller::name(self.0.scheduler())
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        Controller::tick(self.0.scheduler()).await
    }

    fn controller_type(&self) -> ControllerType {
        Controller::controller_type(self.0.scheduler())
    }
}

/// A controller whose own crate does not declare what it reads yet, with
/// the declaration made here, beside its spawn site.
///
/// `pending-declared-reads: kubelet, csi-registrar, scheduler`. They live
/// in engenho-kubelet and engenho-scheduler, outside the change that
/// introduced [`DeclaresReads`]; each `impl DeclaresReads` belongs beside
/// its type, and this wrapper is deleted when the three move there. Until
/// then their kinds were read off their ticks, and the read census in this
/// module's tests scans those crates' sources to hold them to it.
struct DeclaredHere<C> {
    controller: C,
    reads: Reads,
}

impl<C> DeclaredHere<C> {
    fn new(controller: C, reads: Reads) -> Self {
        Self { controller, reads }
    }
}

#[async_trait::async_trait]
impl<C: Controller> Controller for DeclaredHere<C> {
    fn name(&self) -> &'static str {
        self.controller.name()
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        self.controller.tick().await
    }

    fn controller_type(&self) -> ControllerType {
        self.controller.controller_type()
    }
}

impl<C> DeclaresReads for DeclaredHere<C> {
    fn reads(&self) -> Reads {
        self.reads.clone()
    }
}

/// What the kubelet's tick reads: the Pods (it keeps those bound to its
/// node), its own Node and node Lease (the Ready condition is judged from
/// the Lease read back out of the store), the Services it resolves into a
/// pod's environment and host aliases, and the `ConfigMaps`, Secrets,
/// `ServiceAccounts`, claims and volumes a pod's volumes and env name.
///
/// The Lease and Node are also what it writes, once per renewal and on a
/// readiness change: its own write wakes it once more, bounded, never in a
/// loop. The filter is by kind, so a Lease renewed by any other holder
/// wakes it too; a filter narrowed to its own objects is not built yet.
const KUBELET_READS: &[GroupVersionKind] = &[
    gvk("", "v1", "Pod"),
    gvk("", "v1", "Node"),
    gvk("coordination.k8s.io", "v1", "Lease"),
    gvk("", "v1", "Service"),
    gvk("", "v1", "ConfigMap"),
    gvk("", "v1", "Secret"),
    gvk("", "v1", "ServiceAccount"),
    gvk("", "v1", "PersistentVolumeClaim"),
    gvk("", "v1", "PersistentVolume"),
];

/// What the scheduler's tick reads: the Pods it binds and the Nodes it
/// binds them to.
const SCHEDULER_READS: &[GroupVersionKind] = &[gvk("", "v1", "Pod"), gvk("", "v1", "Node")];

/// What every catalog child is built from, assembled ONCE before the walk.
///
/// The shared pieces live here rather than inside any one child's arm
/// because more than one child reads each: the event sink (two sinks over
/// one store would be two independent lossy buffers for one cluster's
/// events), the CSI driver table, the scheduler (built fallibly by the
/// caller from `scheduler.*`) and the kubelet.
struct Parts<'a> {
    boot: &'a BootConfig,
    store: &'a Arc<StoreMesh>,
    handler_sink: &'a Arc<dyn DynamicHandlerSink>,
    /// The controllers' namespace scope (`controllers.namespace`): `None`
    /// means all namespaces. The scheduler is scoped by its own,
    /// `scheduler.namespace`.
    ns: Option<String>,
    /// The liveness windows; each driver's fallback, debounce and
    /// stuck-tick threshold are read from here, so they are the ones
    /// liveness judges against.
    windows: Windows,
    /// ★ ONE CSI driver table, shared by three consumers: the registrar
    /// fills it, the PV binder provisions through it, and the kubelet's
    /// materializer publishes through it. Two tables would let a driver be
    /// provisionable but not mountable — a PVC that binds and then never
    /// mounts, with nothing anywhere explaining the difference.
    csi_drivers: engenho_kubelet::DriverTable,
    /// The event sink, shared by every producer. The workload controllers
    /// announce a parent they cannot reconcile (a template of the wrong
    /// shape) on that parent, and carry on with the rest.
    events: Arc<dyn EventSink>,
    scheduler: Arc<ConfiguredSchedulerLoop>,
    kubelet: Arc<Kubelet>,
}

impl<'a> Parts<'a> {
    fn assemble(
        boot: &'a BootConfig,
        store: &'a Arc<StoreMesh>,
        backend: &Arc<dyn ContainerRuntime>,
        scheduler: ConfiguredScheduler,
        handler_sink: &'a Arc<dyn DynamicHandlerSink>,
        windows: Windows,
    ) -> Self {
        let ns = boot.controllers_namespace.clone();
        let csi_drivers = engenho_kubelet::DriverTable::new();
        let events: Arc<dyn EventSink> = Arc::new(
            engenho_controllers::event_recorder::StoreEventSink::new(Arc::new(MeshEventStore {
                store: store.clone(),
            })),
        );
        // Built fallibly by the caller from `scheduler.*` (a typed error for
        // an unimplemented strategy or a zero tick — never a silent
        // fallback). Held in an Arc so the scheduler child wraps a clone of
        // it rather than consuming it.
        let scheduler = Arc::new(ConfiguredSchedulerLoop(scheduler));
        let kubelet = build_kubelet(boot, store, backend, events.clone(), &csi_drivers);
        Self {
            boot,
            store,
            handler_sink,
            ns,
            windows,
            csi_drivers,
            events,
            scheduler,
            kubelet,
        }
    }

    /// The body of one catalog child. `before` is every child spawned
    /// ahead of it in the walk.
    fn task(&self, child: Child, before: &Children) -> Option<ChildTask> {
        match child {
            Child::Driver(driver) => Some(self.driver(driver)),
            Child::Listener(listener) => Some(listener_task(
                listener,
                self.boot,
                Arc::downgrade(&self.kubelet),
                self.store,
            )),
            // Renewed by the kubelet's row, so built from it. The kubelet is
            // walked first and always enabled; were it absent, the lease
            // would have nothing to renew by, and is not spawned.
            Child::NodeLease => {
                let Some(kubelet) = before.row(Child::Driver(Driver::Kubelet)) else {
                    error!(
                        "the node lease has no kubelet to renew by and is not spawned; this \
                         node will read NotReady"
                    );
                    return None;
                };
                // pending-runtime-relist: nothing relists the container
                // runtime yet (`ContainerRuntime` has no relist method), so
                // the lease renews by the kubelet alone and says so once.
                // Waking it: a `Child::RuntimeRelist` driving a `Relister`
                // over the backend, and its ledger here.
                Some(drive_node_lease(
                    self.store,
                    &self.boot.node_name,
                    kubelet,
                    self.windows,
                    RuntimeHealthSource::Unobserved,
                ))
            }
        }
    }

    /// `driver`'s body: `controller` behind a `WatchDriver`, on the windows
    /// [`Windows::of_child`] gives it (the ones liveness judges it by). See
    /// [`drive`].
    fn watch<C: Controller + DeclaresReads + 'static>(
        &self,
        driver: Driver,
        controller: C,
    ) -> ChildTask {
        drive(
            TickLoop::Driver(driver),
            controller,
            self.store,
            self.windows.of_child(Child::Driver(driver)),
        )
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one arm per catalog driver; splitting the match would split the catalog"
    )]
    fn driver(&self, driver: Driver) -> ChildTask {
        let store = self.store;
        let ns = || self.ns.clone();
        let events = || self.events.clone();
        match driver {
            Driver::Deployment => self.watch(
                driver,
                DeploymentController::new(store.clone(), ns()).with_event_sink(events()),
            ),
            Driver::ReplicaSet => self.watch(
                driver,
                ReplicaSetController::new(store.clone(), ns()).with_event_sink(events()),
            ),
            Driver::StatefulSet => self.watch(
                driver,
                StatefulSetController::new(store.clone(), ns()).with_event_sink(events()),
            ),
            Driver::DaemonSet => self.watch(
                driver,
                DaemonSetController::new(store.clone(), ns()).with_event_sink(events()),
            ),
            Driver::Job => self.watch(
                driver,
                JobController::new(store.clone(), ns()).with_event_sink(events()),
            ),
            // CronJob: parses spec.schedule (5-field cron) against the
            // WallClock and creates a batch/v1 Job from the jobTemplate on
            // schedule; the JobController then runs that Job's Pods.
            Driver::CronJob => self.watch(
                driver,
                CronJobController::new(store.clone(), Arc::new(WallClock), ns())
                    .with_event_sink(events()),
            ),
            Driver::PodDisruptionBudget => self.watch(
                driver,
                PodDisruptionBudgetController::new(store.clone(), ns()),
            ),
            Driver::Endpoints => self.watch(
                driver,
                EndpointsController::new(store.clone(), ns()).with_event_sink(events()),
            ),
            // Service routing: resolves Service + Endpoints → typed
            // ServiceRoutes and drives the platform-selected datapath
            // backend. The backend is chosen by `networking.datapath_mode`
            // resolved against the host platform (Auto → iptables on Linux,
            // compute-only off-Linux), so on a Darwin dev host the controller
            // still runs + computes + observes the desired rules without ever
            // shelling to a non-existent `iptables-restore`.
            Driver::ServiceRouting => {
                let resolved = self.boot.datapath_mode.resolve(cfg!(target_os = "linux"));
                let backend = make_service_router(resolved);
                info!(
                    datapath = backend.name(),
                    mode = ?self.boot.datapath_mode,
                    "service routing backend selected"
                );
                self.watch(
                    driver,
                    ServiceRoutingController::new(store.clone(), backend, ns()),
                )
            }
            Driver::Gc => self.watch(driver, GcController::new(store.clone(), ns())),
            // Namespace: cascade-deletion of a Terminating namespace's
            // contents + finalizer clear. It reads every namespaced kind, so
            // each child's deletion wakes it; the fallback tick covers the
            // rest of the drain.
            Driver::Namespace => self.watch(driver, NamespaceController::new(store.clone(), ns())),
            // A claim in use by a pod carries kubernetes.io/pvc-protection and is
            // not removed while in use; a Bound PV whose claim is gone goes
            // Released and is reclaimed per its policy (W9).
            Driver::PvcProtection => {
                self.watch(driver, PvcProtectionController::new(store.clone(), ns()))
            }
            // PV/PVC binder: binds Pending claims to matching Available PVs
            // and dynamically provisions a node-local hostPath PV (under
            // data_dir/local-path) when no static PV matches. A claim that
            // cannot be provisioned says why on the claim
            // (`ProvisioningFailed`), where `kubectl describe pvc` shows it.
            Driver::PvBinder => {
                let local_path_root = self
                    .boot
                    .data_dir
                    .join("local-path")
                    .to_string_lossy()
                    .into_owned();
                self.watch(
                    driver,
                    PvBinderController::new(store.clone(), ns(), local_path_root)
                        .with_csi(Arc::new(engenho_kubelet::DriverCsiProvisioner::new(
                            self.csi_drivers.clone(),
                        )))
                        .with_event_sink(events()),
                )
            }
            // VolumeSnapshot: the snapshot half of the same local-path
            // provisioner — gated with the binder (see `Driver::enabled`).
            Driver::VolumeSnapshot => {
                let snapshot_root = self
                    .boot
                    .data_dir
                    .join("snapshots")
                    .to_string_lossy()
                    .into_owned();
                self.watch(
                    driver,
                    engenho_controllers::volume_snapshot::VolumeSnapshotController::new(
                        store.clone(),
                        snapshot_root,
                        Arc::new(engenho_controllers::volume_snapshot::HostSnapshotEnv),
                    ),
                )
            }
            // CRD: registers a StoreBackedHandler per served CRD version into
            // the shared RouterState via `handler_sink`, so CR instances
            // become routable + discoverable with no parallel codepath. The
            // fallback tick covers a CRD installed before the driver
            // subscribed.
            Driver::Crd => self.watch(
                driver,
                CrdController::new(store.clone(), self.handler_sink.clone()),
            ),
            // Scheduler: pending Pod → spec.nodeName, in `scheduler.namespace`,
            // falling back every `scheduler.tick_interval_seconds` (T5.8).
            Driver::Scheduler => self.watch(
                driver,
                DeclaredHere::new(self.scheduler.clone(), Reads::of(SCHEDULER_READS)),
            ),
            // Served-capability honesty: truthful status conditions on the
            // kinds engenho advertises in discovery but does not implement
            // (APIService, FlowSchema, PriorityLevelConfiguration). Without
            // it an aggregated APIService registers successfully while every
            // request to its group silently goes nowhere.
            Driver::ServedCapability => self.watch(
                driver,
                engenho_controllers::served_capability::ServedCapabilityController::new(
                    store.clone(),
                ),
            ),
            // NetworkPolicy: translate every policy into enforcer rules AND
            // record whether they are actually enforced. Without it a
            // default-deny policy applies cleanly and restricts nothing, with
            // no object anywhere saying so. The backend is
            // `ComputedNetworkPolicyEnforcer` because engenho runs on darwin
            // with pods in a podman VM: there is no kernel here to install a
            // filter into — a named, operator-visible state, not a stub.
            Driver::NetworkPolicy => {
                let enforcer = Arc::new(
                    engenho_controllers::network_policy::ComputedNetworkPolicyEnforcer::new(),
                );
                self.watch(
                    driver,
                    engenho_controllers::network_policy_controller::NetworkPolicyController::new(
                        store.clone(),
                        enforcer,
                    )
                    .with_event_sink(events()),
                )
            }
            // CSI registration: scan `<kubelet-root>/plugins_registry` and
            // keep the driver table in sync. Without it the whole CSI plane
            // is inert — a driver deploys, creates its sockets, and nothing
            // ever dials them.
            //
            // It reads nothing from the store: registration is a filesystem
            // event, so no store event wakes it and the fallback tick drives
            // the scan.
            Driver::CsiRegistrar => self.watch(
                driver,
                DeclaredHere::new(
                    engenho_kubelet::CsiRegistrarController::new(
                        &self.boot.data_dir,
                        self.csi_drivers.clone(),
                    ),
                    Reads::nothing(),
                ),
            ),
            // CNI status: publish which network config this node resolved
            // and whether its plugin chain is executed or merely planned.
            Driver::CniStatus => self.watch(
                driver,
                engenho_controllers::cni_status::CniStatusController::new(
                    store.clone(),
                    self.boot.node_name.clone(),
                    std::path::PathBuf::from(CNI_CONFIG_DIR),
                    CNI_INSTALL,
                ),
            ),
            // Kubelet: bound Pod → container via the backend.
            Driver::Kubelet => self.watch(
                driver,
                DeclaredHere::new(self.kubelet.clone(), Reads::of(KUBELET_READS)),
            ),
        }
    }
}

/// A listener's body: bind and serve at the address `config` gives it, and
/// bind again whenever that ends or panics ([`serve_rebinding`], T2.7).
///
/// The one place a listener's body is built: the runtime's walk and the
/// fault-injection matrix (W6) both call it. `kubelet` is held weakly — a
/// strong `Arc<Kubelet>` behind the :10250 router keeps the store alive past
/// shutdown ([`WeakKubeletApi`]).
pub(crate) fn listener_task(
    listener: Listener,
    boot: &BootConfig,
    kubelet: std::sync::Weak<Kubelet>,
    store: &Arc<StoreMesh>,
) -> ChildTask {
    let beat = Arc::new(Heartbeat::new());
    match listener {
        Listener::KubeletHttp => {
            let api: Arc<dyn engenho_kubelet::server::KubeletApi> =
                Arc::new(WeakKubeletApi { kubelet });
            let addr = boot.kubelet_listen_addr.clone();
            ChildTask::new(
                beat.clone(),
                serve_rebinding(listener, beat, move || {
                    serve_kubelet_http(addr.clone(), api.clone())
                }),
            )
        }
        Listener::EtcdFacade => {
            let addr = boot.etcd_listen_addr.clone();
            let etcd_store = crate::etcd_facade::MeshEtcdStore::new(store);
            ChildTask::new(
                beat.clone(),
                serve_rebinding(listener, beat, move || {
                    serve_etcd_facade(addr.clone(), etcd_store.clone())
                }),
            )
        }
    }
}

/// Wrap `controller` in a `WatchDriver` as the catalog says `tick_loop` (a
/// driver, or the node lease) runs.
///
/// * Woken by exactly the kinds the controller declares it reads (T1.7).
///   This is the only place a driver's filter is built, and it takes nothing
///   but the declaration: a controller that declares no reads cannot be
///   driven (E0277), and no list of what wakes a driver is written anywhere.
///   The three controllers whose crates do not declare yet are declared
///   beside their spawn ([`DeclaredHere`]), as reads, and the read census
///   holds those to their sources too.
/// * A panic in its tick is handled by the loop's catalog
///   [`TickState`](crate::TickState) (T2.7): contained and re-ticked for a
///   Stateless driver, fatal to the child for a Stateful one. It is read
///   here off the tick loop's own row ([`TickLoop::tick_state`]), so no
///   call to this function can drive a loop by another row.
/// * Its ticks are counted ([`Tallied`], T2.8): by how each ended, for
///   `controller_runtime_reconcile_total`, and by whether it landed a write,
///   for the propose-rate detector. A tick it has run longer than the
///   windows' stuck threshold is logged BLOCKED by the driver and reported
///   stalled by liveness: one threshold, from `windows`. Its fallback and
///   debounce are read from the same value, so liveness's idle window is
///   derived from the fallback the loop runs on.
pub(crate) fn drive<C: Controller + DeclaresReads + 'static>(
    tick_loop: TickLoop,
    controller: C,
    store: &Arc<StoreMesh>,
    windows: Windows,
) -> ChildTask {
    let reads = controller.reads();
    let config = WatchDriverConfig {
        filter: reads.filter(),
        debounce: windows.debounce(),
        fallback_interval: windows.fallback(),
        stuck_tick_after: windows.stuck_tick_after(),
        tick_state: tick_loop.tick_state(),
    };
    let controller_type = controller.controller_type();
    // Named by the catalog, like its liveness row and its last-tick gauge,
    // so every family says `controller="<child>"` in one vocabulary.
    let tally = Arc::new(Tally::new(tick_loop.child().name()));
    let watch = WatchDriver::new(
        Tallied::new(controller, tally.clone()),
        store.clone(),
        config,
    );
    let wiring = Wiring::new(controller_type, reads, watch.wakes().clone());
    ChildTask::driver(watch.heartbeat(), wiring, tally, watch.run())
}

/// The node lease's body (T1.3c): [`NodeLease`] behind a `WatchDriver` on
/// the lease's own windows ([`Windows::node_lease`], the ones liveness
/// judges it by), renewing `node`'s Lease while `kubelet`'s row is alive as
/// `windows` (the runtime's, the ones `/livez` judges the kubelet by) say,
/// and `runtime` does not hold it back (W8).
pub(crate) fn drive_node_lease(
    store: &Arc<StoreMesh>,
    node: &str,
    kubelet: Row,
    windows: Windows,
    runtime: RuntimeHealthSource,
) -> ChildTask {
    drive(
        TickLoop::NodeLease,
        NodeLease::new(store.clone(), node, kubelet, windows, runtime),
        store,
        windows.of_child(Child::NodeLease),
    )
}

/// Build the ONE kubelet, with its event sink, `ServiceAccount` projection
/// and CSI-layered volume materializer.
fn build_kubelet(
    boot: &BootConfig,
    store: &Arc<StoreMesh>,
    backend: &Arc<dyn ContainerRuntime>,
    events: Arc<dyn EventSink>,
    csi_drivers: &engenho_kubelet::DriverTable,
) -> Arc<Kubelet> {
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
            // ── Root the podman materializer at the DECLARED data dir ────────
            // `PodmanVolumeMaterializer::new()` defaults its data root to
            // `$HOME/.local/share/engenho/volumes`, and falls back to a
            // RELATIVE path when `$HOME` is unset. A systemd service has no
            // `$HOME`, so this daemon hit the fallback and every secret /
            // configMap mount resolved against the process cwd:
            //
            //   materialize: write ./.local/share/engenho/volumes/<ns>_<pod>/…
            //   rename …tmp → …: No such file or directory (os error 2)
            //
            // Measured on a live single-node engenho 2026-09-06: a pod mounting
            // a Secret stayed Pending with VolumeMaterializeError, while the
            // same code passes its tests — a test process inherits a `$HOME`.
            //
            // The `$HOME` default itself is not wrong where it came from: the
            // macOS applehv podman machine shares the user's home and NOT
            // `/tmp`, so bind sources must live under it (see the type's own
            // header). That is a darwin constraint, and this is the Linux
            // daemon, which already has a declared data dir one line below —
            // the same one the CSI layer is being handed.
            Arc::new(
                engenho_kubelet::PodmanVolumeMaterializer::new()
                    .with_data_root(boot.data_dir.join("volumes")),
            ),
            csi_drivers.clone(),
            boot.data_dir.clone(),
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
    let sa_key = engenho_apiserver::sa_token::load_or_generate_sa_key(&boot.data_dir)
        .map_err(|e| warn!(error = %e, "no SA signing key; pods get no ServiceAccount projection"))
        .ok();
    let ca_pem_for_sa = engenho_apiserver::load_or_generate_ca(&boot.data_dir)
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
                //
                // This was a CEILING ON POD LIFETIME for every API-calling
                // workload until the kubelet learned to rewrite the projected
                // token (`Kubelet::refresh_service_account_projections`). It no
                // longer is: the kubelet re-mints on a cadence DERIVED from
                // this number via `ServiceAccountProjector::token_lifetime`,
                // so changing it here moves the refresh with it and the two
                // cannot drift apart.
                lifetime_secs: 3600,
            }),
            _ => Arc::new(engenho_kubelet::NoServiceAccountProjection),
        };

    Arc::new(
        Kubelet::new(store.clone(), backend.clone(), boot.node_name.clone())
            .with_event_sink(events)
            .with_sa_projector(sa_projector)
            .with_volume_materializer(csi_materializer)
            // Deny-all unless this node named prefixes. Load-bearing for the
            // native backend, whose only honourable volume shape is a hostPath
            // mounted at its own path.
            .with_host_path_policy(engenho_kubelet::pod_volume::HostPathPolicy::allowing(
                boot.host_path_allowlist.clone(),
            )),
    )
}

/// The kubelet's own HTTP surface (:10250), as a catalog child.
///
/// ★ THIS IS WHAT MAKES THE SURFACE EXIST. `KubeletApi` and its router
/// shipped with a trait, a route table and a test double, and nothing ever
/// bound them — so the port was a type, not a port. Logs worked only because
/// the kubelet happens to share a process with the apiserver; the moment
/// there is a second node, `kubectl logs` against a pod on it has no path at
/// all.
///
/// A bind failure is logged and NOT fatal: the apiserver is already serving,
/// and killing a working control plane because one auxiliary port is taken
/// trades a partial outage for a total one. This is ONE attempt; when it
/// ends, [`serve_rebinding`] records it and binds again after a backoff.
async fn serve_kubelet_http(addr: String, api: Arc<dyn engenho_kubelet::server::KubeletApi>) {
    match tokio::net::TcpListener::bind(&addr).await {
        Ok(listener) => {
            let bound = listener
                .local_addr()
                .map_or_else(|_| addr.clone(), |a| a.to_string());
            info!(addr = %bound, "kubelet HTTP surface bound");
            let app = engenho_kubelet::server::KubeletServer::new(api).routes();
            match axum::serve(listener, app).await {
                Ok(()) => warn!(addr = %bound, "kubelet HTTP surface stopped"),
                Err(e) => warn!(addr = %bound, error = %e, "kubelet HTTP surface stopped"),
            }
        }
        Err(e) => warn!(
            addr = %addr,
            error = %e,
            "kubelet HTTP surface could not bind; container logs and exec \
             are unreachable from off-process (the apiserver is unaffected)"
        ),
    }
}

/// The etcd v3 façade on :2379, as a catalog child. Same failure posture as
/// :10250: a bind failure is a WARNING, and [`serve_rebinding`] binds again
/// after a backoff.
///
/// ★ THIS IS WHAT MAKES engenho DRIVABLE BY SOFTWARE THAT HAS NEVER HEARD OF
/// IT. `etcdctl get /registry/ --prefix --keys-only`, `snapshot save`, every
/// backup tool and every runbook that was written against etcd. engenho runs
/// no etcd and its apiserver never speaks it — the INTERFACE is the
/// obligation, not the technology.
///
/// READ-ONLY: Kv serves Range; Put/DeleteRange/Txn are absent rather than
/// silently dropping writes. See `etcd_facade`'s header.
///
/// Ownership (I13, T5.4). `MeshEtcdStore` holds a `Weak<StoreMesh>`, not an
/// `Arc`, and the three services (Kv, Watch, Maintenance) share clones of that
/// one `Weak`, so they read one store. A call upgrades it for as long as the
/// call runs and answers `StoreGone` once the store is dropped; an open watch
/// holds the store's watch stream, not the store. So an idle connection or an
/// open watch keeps nothing alive across a stop, and a Range or Status still
/// in flight holds the store until it returns. Pinned by
/// `etcd_facade::tests::the_facade_and_its_clones_hold_no_strong_reference_and_see_one_store`.
async fn serve_etcd_facade(addr: String, etcd_store: crate::etcd_facade::MeshEtcdStore) {
    match tokio::net::TcpListener::bind(&addr).await {
        Ok(listener) => {
            let bound = listener
                .local_addr()
                .map_or_else(|_| addr.clone(), |a| a.to_string());
            info!(addr = %bound, "etcd v3 facade bound (read-only)");
            let identity = engenho_etcd::server::ServerIdentity::default();
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            let kv = engenho_etcd::server::ReadOnlyKv {
                store: etcd_store.clone(),
                identity,
            };
            let maintenance = engenho_etcd::server::MaintenanceSvc {
                store: etcd_store.clone(),
                identity,
            };
            let watch = engenho_etcd::server::WatchSvc::new(Arc::new(etcd_store), identity);
            let served = tonic::transport::Server::builder()
                .add_service(engenho_etcd::pb::etcdserverpb::kv_server::KvServer::new(kv))
                .add_service(engenho_etcd::pb::etcdserverpb::watch_server::WatchServer::new(watch))
                .add_service(
                    engenho_etcd::pb::etcdserverpb::maintenance_server::MaintenanceServer::new(
                        maintenance,
                    ),
                )
                .serve_with_incoming(incoming)
                .await;
            match served {
                Ok(()) => warn!(addr = %bound, "etcd v3 facade stopped"),
                Err(e) => warn!(addr = %bound, error = %e, "etcd v3 facade stopped"),
            }
        }
        Err(e) => warn!(
            addr = %addr,
            error = %e,
            "etcd v3 facade could not bind; etcdctl, snapshot tooling and \
             any --etcd-servers consumer are unreachable (the apiserver is \
             unaffected)"
        ),
    }
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

#[cfg(test)]
mod tests {
    /// `config` as the runtime reads it.
    fn read(config: &super::EngenhoConfig) -> super::BootConfig {
        super::BootConfig::read(config).expect("the runtime runs this config")
    }

    /// ★ A backend with no podman under it must NOT be podman-probed.
    ///
    /// Regression for a measured failure: ryn was configured
    /// `kubelet_backend: native`, and the daemon refused to start with
    /// `kubelet backend "podman" is configured but its binary ... could not
    /// be resolved`. The preflight early-returned only for `Fake`, so every
    /// new backend inherited a podman probe by default.
    ///
    /// Asserted against a config whose podman binary CANNOT work, so a
    /// regression fails here rather than only on a machine without podman.
    #[test]
    fn the_native_backend_is_not_podman_probed() {
        let mut config = super::EngenhoConfig::prescribed_default();
        config.runtime.kubelet_backend = super::CfgBackendKind::Native;
        config.runtime.podman_binary = Some("/nonexistent/definitely-not-podman".to_string());
        super::preflight_backend(&read(&config))
            .expect("native must skip the podman probe entirely");
    }

    /// The positive control: a podman backend with an unusable binary MUST
    /// still fail, or the test above passes because the probe never runs.
    #[test]
    fn a_podman_backend_with_an_unusable_binary_still_fails_preflight() {
        let mut config = super::EngenhoConfig::prescribed_default();
        config.runtime.kubelet_backend = super::CfgBackendKind::Podman;
        config.runtime.podman_binary = Some("/nonexistent/definitely-not-podman".to_string());
        let verdict = super::preflight_backend(&read(&config));
        assert!(
            matches!(
                verdict,
                Err(super::RuntimeError::ContainerRuntimeUnavailable { .. })
            ),
            "the probe must still catch a broken podman, or skipping it for \
             native proves nothing; and the refusal check before it must not \
             swallow an admitted backend. Got {verdict:?}"
        );
    }

    /// ★ I38 / node T5.9: a `cri` node is refused with the kubelet's own
    /// typed refusal, BEFORE any podman probe.
    ///
    /// Regression: preflight probed podman for `Cri`, so a CRI node with no
    /// podman failed with `ContainerRuntimeUnavailable { backend: "podman" }`,
    /// an error naming a runtime it was configured not to use, and only a
    /// node with a working podman reached the refusal. Asserted against a
    /// podman binary that CANNOT work, so the old order fails here on every
    /// machine, not only on one without podman.
    #[test]
    fn a_cri_node_is_refused_before_any_podman_probe() {
        let mut config = super::EngenhoConfig::prescribed_default();
        config.runtime.kubelet_backend = super::CfgBackendKind::Cri;
        config.runtime.podman_binary = Some("/nonexistent/definitely-not-podman".to_string());
        let expected =
            engenho_kubelet::config_bridge::construction_refusal(super::KubeletBackendKind::Cri)
                .expect(
                    "CRI is refused while cri_backend::UNSUPPORTED is non-empty. Once it is \
             admitted this premise is gone: give preflight_backend's Cri arm the \
             probe its pending-cri note names, then rewrite this test",
                );
        match super::preflight_backend(&read(&config)) {
            Err(super::RuntimeError::BackendRefused(refused)) => assert_eq!(
                refused, expected,
                "the node must carry the kubelet's refusal verbatim"
            ),
            other => panic!("a cri node must fail with the typed refusal, got {other:?}"),
        }
    }

    /// ★ The invariant this exists for: pods are never told an address that
    /// routes nowhere. Under podman they get the host gateway, because no
    /// service datapath installs the cluster VIP.
    #[test]
    fn podman_pods_are_told_the_host_gateway_not_the_unrouted_service_vip() {
        let mut cfg = EngenhoConfig::default();
        cfg.runtime.kubelet_backend = CfgBackendKind::Podman;
        cfg.runtime.listen_addr = "127.0.0.1:6443".to_string();

        let r = super::apiserver_reachability(&read(&cfg));
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

        let r = super::apiserver_reachability(&read(&cfg));
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
        let (_, port) = super::apiserver_reachability(&read(&cfg))
            .injectable()
            .unwrap();
        assert_eq!(port, 16443);
    }

    /// Empty disables publishing — the escape hatch tests and headless
    /// contexts use to stay out of `$HOME`.
    #[test]
    fn empty_publish_path_disables_publishing() {
        assert_eq!(
            super::resolve_publish_path(""),
            Err(crate::publish::SkipReason::NotConfigured)
        );
        assert_eq!(
            super::resolve_publish_path("   "),
            Err(crate::publish::SkipReason::NotConfigured)
        );
    }

    /// An absolute path is taken verbatim — nix renders one when it wants
    /// the file somewhere other than the convention.
    #[test]
    fn absolute_publish_path_is_verbatim() {
        assert_eq!(
            super::resolve_publish_path("/etc/engenho/kubeconfig"),
            Ok(std::path::PathBuf::from("/etc/engenho/kubeconfig"))
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
            Ok(std::path::PathBuf::from(home).join(".kube/configs/engenho"))
        );
    }

    use super::*;
    use crate::Dormant;
    use crate::TickState;
    use crate::child::DeathCause;
    use crate::child::{ChildHandle, ChildState};
    use crate::impl_census::{Implementors, workspace_sources};
    use crate::read_census::{Census, Section};
    use engenho_config::KubeletBackendKind as CfgKind;
    use engenho_controllers::{Beat, KindFilter, TickClass};
    use std::collections::{BTreeMap, BTreeSet};

    // ── host capacity ────────────────────────────────────────────────────

    #[test]
    fn mem_total_is_parsed_in_kb_not_bytes() {
        // rio's real /proc/meminfo shape. The unit is the whole trap: reading
        // 32793532 as BYTES gives 31MiB, which is a plausible-looking number
        // that would make the node refuse nearly every pod.
        let meminfo = "MemTotal:       32793532 kB\nMemFree:         1234 kB\n";
        assert_eq!(
            super::parse_mem_total_bytes(meminfo),
            Some(32_793_532 * 1024)
        );
        // ~31.3 GiB, i.e. the 29G `free -g` reports once the kernel's own
        // reservations are taken out.
        let gib = super::parse_mem_total_bytes(meminfo).unwrap() / (1024 * 1024 * 1024);
        assert_eq!(gib, 31);
    }

    #[test]
    fn an_unrecognised_unit_is_refused_not_guessed() {
        // Guessing here understates or overstates the node by 1024x and the
        // result still looks like a real number. `None` makes the caller fall
        // back loudly instead.
        assert_eq!(
            super::parse_mem_total_bytes("MemTotal: 100 furlongs\n"),
            None
        );
        // No unit means bytes, per proc(5).
        assert_eq!(super::parse_mem_total_bytes("MemTotal: 4096\n"), Some(4096));
    }

    #[test]
    fn a_meminfo_without_memtotal_yields_none() {
        assert_eq!(super::parse_mem_total_bytes("MemFree: 1 kB\n"), None);
        assert_eq!(super::parse_mem_total_bytes(""), None);
        // Not a prefix match on some other key that happens to contain it.
        assert_eq!(super::parse_mem_total_bytes("SwapTotal: 8 kB\n"), None);
    }

    #[test]
    fn host_capacity_no_longer_reports_the_8gi_literal_on_linux() {
        let (cpu, mem) = super::host_capacity();
        assert!(cpu.parse::<u64>().is_ok(), "cpu must be an integer: {cpu}");
        if cfg!(target_os = "linux") {
            // The defect this replaces: the string "8Gi", on every node,
            // regardless of the host. On Linux the probe must have answered.
            assert_ne!(
                mem, "8Gi",
                "memory must be measured on linux, not defaulted"
            );
            assert!(
                mem.parse::<u64>().is_ok(),
                "measured memory is plain bytes: {mem}"
            );
        } else {
            // Honest fallback on a target that cannot say — and it is the
            // documented literal, not a silent zero.
            assert_eq!(mem, "8Gi");
        }
    }

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
        // Ephemeral ports for both listeners: parallel tests (or a real
        // engenho on this host) holding :10250 / :2379 would otherwise halt
        // them, and the child tests below assert that they serve.
        cfg.runtime.kubelet_listen_addr = "127.0.0.1:0".into();
        cfg.runtime.etcd_listen_addr = "127.0.0.1:0".into();
        cfg
    }

    /// The drivers the runtime is running.
    fn running_drivers(rt: &Runtime) -> usize {
        rt.children()
            .running()
            .filter(|c| matches!(c, Child::Driver(_)))
            .count()
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
        // Drivers: the reconciler set (deployment, replicaset, statefulset,
        // daemonset, job, cronjob, endpoints, pdb, service_routing, gc,
        // namespace, pv_binder, volume_snapshot, pvc_protection, crd) + served_capability +
        // scheduler + kubelet.
        //
        // This count is deliberately pinned: a driver that stops being
        // spawned is invisible at runtime (the cluster simply stops
        // converging that kind), so the arithmetic here is the tripwire.
        // Moving it is correct ONLY alongside an intentional change to the
        // driver set — which is what added served_capability, then
        // volume_snapshot (19 → 20), and now pvc_protection (20 → 21, W9).
        // Every driver in the catalog is on in this config, so it is also the
        // catalog's size.
        assert_eq!(running_drivers(&rt), 21);
        assert_eq!(Driver::ALL.len(), 21);
        rt.shutdown().await.unwrap();
    }

    // ── T2.6: owned children ──────────────────────────────────────────
    //
    // Every task the runtime starts is a catalog `Child` in one owned set.
    // These pin the behaviour that set exists for: each child really runs,
    // a listener that cannot serve says so, and a stop ends every one.

    /// How many ticks each driver must finish, and the deadline for all of
    /// them together (the test config's fallback is 1 s).
    const TICKS: u64 = 3;
    const TICK_DEADLINE: Duration = Duration::from_secs(30);

    /// Wait until `ready` holds for every child in `of`, or fail naming the
    /// ones it never held for.
    async fn every_child(rt: &Runtime, of: &[Child], ready: impl Fn(&Beat) -> bool) {
        let deadline = std::time::Instant::now() + TICK_DEADLINE;
        loop {
            let lagging: Vec<(Child, Option<Beat>)> = of
                .iter()
                .map(|c| (*c, rt.children().get(*c).map(|h| h.beat().snapshot())))
                .filter(|(_, beat)| !beat.as_ref().is_some_and(&ready))
                .collect();
            if lagging.is_empty() {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "within {TICK_DEADLINE:?} these children never got there: {lagging:#?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_enabled_child_spawns_and_ticks_under_a_deadline() {
        let rt = Runtime::start(ephemeral_test_config()).await.unwrap();

        let spawned: BTreeSet<Child> = rt.children().iter().map(|(c, _)| c).collect();
        let catalog: BTreeSet<Child> = Child::all().collect();
        assert_eq!(
            catalog.difference(&spawned).copied().collect::<Vec<_>>(),
            [],
            "with every switch on, every child in the catalog is spawned"
        );

        let drivers: Vec<Child> = Driver::ALL.iter().map(|d| Child::Driver(*d)).collect();
        every_child(&rt, &drivers, |b| b.ticks_finished >= TICKS).await;

        // The lease falls back once per renew interval: one tick is its
        // proof of life here.
        every_child(&rt, &[Child::NodeLease], |b| b.ticks_finished >= 1).await;

        let listeners: Vec<Child> = Listener::ALL.iter().map(|l| Child::Listener(*l)).collect();
        every_child(&rt, &listeners, |b| {
            b.in_flight() && b.last_class != Some(TickClass::Halted)
        })
        .await;

        assert_eq!(
            rt.children().running().count(),
            spawned.len(),
            "a child died during boot"
        );
        rt.shutdown().await.unwrap();
    }

    // ── T2.7: a panic in a tick, and a listener that stops serving ────
    //
    // A tick that panicked ended its driver for the life of the process, and
    // a listener whose bind failed parked forever. What a panic does now
    // follows the catalog's `TickState` for the driver, and a listener binds
    // again on a growing backoff.

    /// The fallback the panic tests tick on: short, so several ticks fit in
    /// a test, and no store event wakes a controller that reads nothing.
    const PANIC_FALLBACK: Duration = Duration::from_millis(200);

    /// A controller whose every tick panics. What its driver does about that
    /// is the catalog's call, not this controller's.
    struct PanicsEveryTick;

    #[async_trait::async_trait]
    impl Controller for PanicsEveryTick {
        fn name(&self) -> &'static str {
            "panics-every-tick"
        }

        async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
            panic!("tripped over a bad object");
        }
    }

    /// `PanicsEveryTick` run as the catalog's `driver`, built by the same
    /// [`drive`] every catalog driver is built by, as the only child of a
    /// set. The store is returned so it outlives the set.
    async fn a_panicking(driver: Driver) -> (Children, Arc<StoreMesh>) {
        let boot = read(&ephemeral_test_config());
        let (store, kind) = boot_store(&boot).await.unwrap();
        assert_eq!(kind, BootKind::Ephemeral);
        assert!(store.wait_for_leadership(Duration::from_secs(5)).await);
        let children = Children::spawn_catalog(&boot, |child, _| {
            (child == Child::Driver(driver)).then(|| {
                drive(
                    TickLoop::Driver(driver),
                    DeclaredHere::new(PanicsEveryTick, Reads::nothing()),
                    &store,
                    boot.windows(PANIC_FALLBACK).with_fallback(PANIC_FALLBACK),
                )
            })
        });
        assert_eq!(children.len(), 1, "precondition: {driver:?} is spawned");
        (children, store)
    }

    /// A Stateless driver (the catalog says so) contains its tick's panic:
    /// it keeps ticking, each panic is counted and classed as one, and its
    /// task does not end.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stateless_driver_whose_tick_panics_keeps_ticking() {
        let driver = Driver::CniStatus;
        assert_eq!(driver.tick_state(), TickState::Stateless, "precondition");
        let (mut children, _store) = a_panicking(driver).await;
        let child = Child::Driver(driver);

        let deadline = std::time::Instant::now() + TICK_DEADLINE;
        let beat = loop {
            let beat = children.get(child).map(|h| h.beat().snapshot());
            if let Some(beat) = beat.filter(|b| b.panics >= 3) {
                break beat;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "three panicking ticks never happened: {beat:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        let died = tokio::time::timeout(PANIC_FALLBACK * 3, children.next_dead()).await;
        children.stop().await;

        assert!(died.is_err(), "a contained panic ended the child: {died:?}");
        assert_eq!(beat.last_class, Some(TickClass::Panicked), "{beat:?}");
    }

    /// A Stateful driver (the kubelet) does not contain its tick's panic: the
    /// child is Dead after its first tick, and nothing re-ticks it over the
    /// state the panic tore.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stateful_driver_whose_tick_panics_is_dead() {
        let driver = Driver::Kubelet;
        assert_eq!(driver.tick_state(), TickState::Stateful, "precondition");
        let (mut children, _store) = a_panicking(driver).await;
        let child = Child::Driver(driver);

        let dead = tokio::time::timeout(TICK_DEADLINE, children.next_dead())
            .await
            .expect("a stateful driver's panic never ended its child");
        // Longer than the fallback, so a driver that survived would re-tick.
        tokio::time::sleep(PANIC_FALLBACK * 3).await;
        let beat = children.get(child).map(|h| h.beat().snapshot());

        assert_eq!(
            dead,
            DeadChild {
                child,
                cause: DeathCause::Panicked
            }
        );
        assert_eq!(
            children.get(child).map(ChildHandle::state),
            Some(ChildState::Dead(DeathCause::Panicked))
        );
        assert_eq!(beat.map(|b| b.ticks_started), Some(1), "{beat:?}");
        assert_eq!(beat.map(|b| b.panics), Some(1), "{beat:?}");
    }

    /// A listener whose port is taken at boot records `Halted`, stays a
    /// Running child, and binds the port once it is free — no restart.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_listener_whose_port_is_taken_binds_it_once_it_is_free() {
        let blocker = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = blocker.local_addr().unwrap();
        let mut cfg = ephemeral_test_config();
        cfg.runtime.kubelet_listen_addr = addr.to_string();
        let rt = Runtime::start(cfg).await.unwrap();
        let kubelet_http = Child::Listener(Listener::KubeletHttp);

        every_child(&rt, &[kubelet_http], |b| {
            b.last_class == Some(TickClass::Halted)
        })
        .await;
        drop(blocker);

        let deadline = std::time::Instant::now() + TICK_DEADLINE;
        while tokio::net::TcpStream::connect(addr).await.is_err() {
            assert!(
                std::time::Instant::now() < deadline,
                "within {TICK_DEADLINE:?} the listener never bound {addr} after it was freed"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(
            rt.children().get(kubelet_http).map(ChildHandle::state),
            Some(ChildState::Running),
            "a listener that could not bind is not dead"
        );
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
            Err(RuntimeError::Config(engenho_config::ConfigError::InvalidField {
                field, ..
            })) => {
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
        // one spawned driver (21 → 20).
        let mut cfg = ephemeral_test_config();
        cfg.controllers.enable.service_routing = false;
        let rt = Runtime::start(cfg).await.unwrap();
        assert_eq!(running_drivers(&rt), 20);
        assert!(
            rt.children()
                .get(Child::Driver(Driver::ServiceRouting))
                .is_none()
        );
        rt.shutdown().await.unwrap();
    }

    // ── T1.7: a driver wakes on every kind its controller reads ──────────
    //
    // A driver's filter is derived from its controller's declared reads.
    // These hold the SPAWNED drivers to that — what the runtime actually
    // wired, not the function that is supposed to wire it — and hold each
    // declaration to the reads its controller's source makes.

    /// Where each driver's controller reads from. An exhaustive match: a
    /// driver added without its sources is E0004, so no spawned controller
    /// escapes the census.
    fn controller_sources(driver: Driver) -> Vec<Section> {
        macro_rules! file {
            ($path:literal) => {
                Section::file($path, include_str!(concat!("../../", $path)))
            };
        }
        macro_rules! section {
            ($path:literal, $from:literal, $until:expr) => {
                Section::between($path, include_str!(concat!("../../", $path)), $from, $until)
            };
        }
        match driver {
            Driver::Deployment => vec![file!("engenho-controllers/src/deployment.rs")],
            Driver::ReplicaSet => vec![file!("engenho-controllers/src/replicaset.rs")],
            Driver::StatefulSet => vec![file!("engenho-controllers/src/statefulset.rs")],
            Driver::DaemonSet => vec![file!("engenho-controllers/src/daemonset.rs")],
            Driver::Job => vec![section!(
                "engenho-controllers/src/job.rs",
                "pub struct JobController",
                Some("pub struct CronJobController")
            )],
            Driver::CronJob => vec![section!(
                "engenho-controllers/src/job.rs",
                "pub struct CronJobController",
                None
            )],
            Driver::PodDisruptionBudget => vec![file!("engenho-controllers/src/pdb.rs")],
            Driver::Endpoints => vec![file!("engenho-controllers/src/endpoints.rs")],
            Driver::ServiceRouting => vec![file!("engenho-controllers/src/service_router.rs")],
            Driver::Gc => vec![file!("engenho-controllers/src/gc.rs")],
            Driver::Namespace => vec![file!("engenho-controllers/src/namespace.rs")],
            Driver::PvcProtection => vec![file!("engenho-controllers/src/pvc_protection.rs")],
            Driver::PvBinder => vec![
                file!("engenho-controllers/src/pv_binder.rs"),
                file!("engenho-controllers/src/pv_binder/identity.rs"),
            ],
            Driver::VolumeSnapshot => vec![file!("engenho-controllers/src/volume_snapshot.rs")],
            Driver::Crd => vec![file!("engenho-controllers/src/crd.rs")],
            Driver::Scheduler => vec![file!("engenho-scheduler/src/scheduler.rs")],
            Driver::ServedCapability => {
                vec![file!("engenho-controllers/src/served_capability.rs")]
            }
            Driver::NetworkPolicy => {
                vec![file!(
                    "engenho-controllers/src/network_policy_controller.rs"
                )]
            }
            Driver::CsiRegistrar => vec![section!(
                "engenho-kubelet/src/csi_materializer.rs",
                "pub struct CsiRegistrarController",
                None
            )],
            Driver::CniStatus => vec![file!("engenho-controllers/src/cni_status.rs")],
            Driver::Kubelet => vec![file!("engenho-kubelet/src/kubelet.rs")],
        }
    }

    /// For every spawned driver, every kind its controller reads wakes it:
    /// each kind it declares, and each kind its source reads by literal.
    /// A controller reading a kind missing from its filter fails here —
    /// whether the kind was never declared or the filter was not built
    /// from the declaration.
    #[tokio::test]
    async fn every_driver_wakes_on_every_kind_its_controller_reads() {
        let rt = Runtime::start(ephemeral_test_config()).await.unwrap();
        let mut undeclared: BTreeSet<(Driver, String)> = BTreeSet::new();
        let mut sleeps_through: BTreeSet<(Driver, String)> = BTreeSet::new();
        for &driver in Driver::ALL {
            let wiring = rt
                .children()
                .get(Child::Driver(driver))
                .and_then(ChildHandle::wiring)
                .unwrap_or_else(|| panic!("{driver:?} was not spawned with its wiring"));
            let (reads, wakes) = (wiring.reads(), wiring.wakes());
            let census = Census::of(&controller_sources(driver));

            if census.every && reads.kinds().is_some() {
                undeclared.insert((driver, "every kind (the whole catalog)".to_owned()));
            }
            for kind in &census.kinds {
                if !reads.includes(kind) {
                    undeclared.insert((driver, kind.clone()));
                }
                if !wakes.wakes_on(kind) {
                    sleeps_through.insert((driver, kind.clone()));
                }
            }
            match reads.kinds() {
                Some(declared) => {
                    for kind in declared {
                        if !wakes.wakes_on(kind.kind) {
                            sleeps_through.insert((driver, kind.kind.to_owned()));
                        }
                    }
                }
                None => {
                    if !matches!(wakes, KindFilter::All) {
                        sleeps_through.insert((driver, "every kind".to_owned()));
                    }
                }
            }
        }
        rt.shutdown().await.unwrap();

        assert!(
            undeclared.is_empty(),
            "a controller reads a kind it does not declare: {undeclared:?}"
        );
        assert!(
            sleeps_through.is_empty(),
            "a driver does not wake on a kind its controller reads: {sleeps_through:?}"
        );
    }

    /// The positive control for the test above: the census, run on the
    /// real sources, sees the reads the old hand-written filters missed.
    /// A census gone blind would pass every controller vacuously.
    #[test]
    fn the_census_sees_the_reads_the_hand_lists_missed() {
        let seen = |driver| Census::of(&controller_sources(driver)).kinds;
        assert!(seen(Driver::StatefulSet).contains("PersistentVolumeClaim"));
        assert!(seen(Driver::DaemonSet).contains("Node"));
        let binder = seen(Driver::PvBinder);
        assert!(binder.contains("VolumeSnapshot") && binder.contains("VolumeSnapshotContent"));
        let kubelet = Census::of(&controller_sources(Driver::Kubelet));
        // Node is read, but since T1.3b through node_readiness::publish_ready,
        // which takes the key as an argument: the census counts that read as
        // computed and cannot name it. The declaration is what the wake filter
        // is built from, so it is checked directly instead.
        assert!(
            kubelet.computed >= 1,
            "the helper's Node read is still seen as a read"
        );
        assert!(
            KUBELET_READS.iter().any(|g| g.kind == "Node"),
            "the kubelet declares the Node it reads through node_readiness"
        );
        let kubelet = kubelet.kinds;
        for kind in [
            "Pod",
            "Service",
            "ConfigMap",
            "Secret",
            "ServiceAccount",
            "PersistentVolumeClaim",
            "PersistentVolume",
        ] {
            assert!(kubelet.contains(kind), "the kubelet census missed {kind}");
        }
        assert_eq!(
            seen(Driver::Scheduler).into_iter().collect::<Vec<_>>(),
            ["Node", "Pod"]
        );
        let registrar = Census::of(&controller_sources(Driver::CsiRegistrar));
        assert!(registrar.kinds.is_empty() && registrar.computed == 0);
    }

    /// The CSI registrar reads nothing from the store, so no store event
    /// wakes it: registration is a filesystem event the fallback tick scans
    /// for. The hand list this replaced woke it on `CSINode`, a kind it
    /// never reads.
    #[tokio::test]
    async fn a_driver_whose_controller_reads_nothing_wakes_on_nothing() {
        let rt = Runtime::start(ephemeral_test_config()).await.unwrap();
        let wiring = rt
            .children()
            .get(Child::Driver(Driver::CsiRegistrar))
            .and_then(ChildHandle::wiring)
            .cloned()
            .expect("the CSI registrar is spawned with its wiring");
        rt.shutdown().await.unwrap();
        assert_eq!(wiring.reads().kinds(), Some(&[][..]));
        assert!(!wiring.wakes().wakes_on("CSINode"));
        assert!(!wiring.wakes().wakes_on("Pod"));
    }

    // ── T5.11: every controller type is spawned or declared dormant ──────
    //
    // Rust cannot list a trait's implementors, so the list comes from the
    // impl census over the workspace's shipped source — a CI gate, not a
    // type. It is held against the types the SPAWNED drivers record, not
    // against a second list of what the runtime is supposed to spawn.

    /// A controller type as both the census and the catalog can name it.
    fn crate_and_name(t: ControllerType) -> (String, String) {
        (t.krate().to_owned(), t.ident().to_owned())
    }

    /// The adapters that hand `Controller` to another controller instead of
    /// being one. Each forwards `controller_type`, so no driver ever records
    /// one. A new adapter fails the gate until it is named here: that a type
    /// only forwards is a claim someone has to make.
    const FORWARDING: &[(&str, &str)] = &[
        ("engenho_controllers", "Arc"),
        ("engenho_runtime", "DeclaredHere"),
        ("engenho_runtime", "Tallied"),
    ];

    /// Concrete newtypes that only forward `Controller` to the one they hold.
    /// The census reads an adapter off its generic bound
    /// (`impl<C: Controller>`), which a newtype does not have, so it counts
    /// each as a controller type of its own; each is named here instead, the
    /// same claim `FORWARDING` makes. A name the census no longer sees fails
    /// the gate, so the list cannot go stale.
    ///
    /// `pending-scheduler-into-parts`: `ConfiguredSchedulerLoop` wraps the
    /// scheduler because `ConfiguredScheduler` does not hand its `Scheduler`
    /// out; it goes when engenho-scheduler adds `into_parts`.
    const FORWARDING_NEWTYPES: &[(&str, &str)] = &[("engenho_runtime", "ConfiguredSchedulerLoop")];

    #[tokio::test]
    async fn every_controller_type_is_spawned_or_dormant() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("the runtime crate sits inside the workspace");
        let census = Implementors::of(&workspace_sources(root), "Controller");
        let mut implemented: BTreeMap<(String, String), String> = census
            .concrete
            .iter()
            .map(|i| ((i.krate.clone(), i.ident.clone()), i.path.clone()))
            .collect();
        for (krate, ident) in FORWARDING_NEWTYPES {
            assert!(
                implemented
                    .remove(&((*krate).to_owned(), (*ident).to_owned()))
                    .is_some(),
                "FORWARDING_NEWTYPES names {krate}::{ident}, which the census does not see"
            );
        }

        let rt = Runtime::start(ephemeral_test_config()).await.unwrap();
        let wirings: Vec<Wiring> = rt
            .children()
            .iter()
            .filter_map(|(_, handle)| handle.wiring().cloned())
            .collect();
        rt.shutdown().await.unwrap();
        let spawned: BTreeSet<(String, String)> = wirings
            .iter()
            .map(|w| crate_and_name(w.controller()))
            .collect();
        let dormant: BTreeSet<(String, String)> = Dormant::ALL
            .iter()
            .map(|d| crate_and_name(d.controller_type()))
            .collect();

        // Positive controls: a census gone blind, or drivers that record no
        // type, would pass the gate below vacuously.
        assert_eq!(
            wirings.len(),
            Child::all().filter(|c| c.tick_state().is_some()).count(),
            "every tick loop (each driver, the node lease) records the controller it runs"
        );
        let unseen: Vec<&(String, String)> = spawned
            .iter()
            .chain(&dormant)
            .filter(|t| !implemented.contains_key(*t))
            .collect();
        assert!(
            unseen.is_empty(),
            "the census does not see these controller types: {unseen:?}"
        );
        let forwarding: BTreeSet<(String, String)> = census
            .forwarding
            .iter()
            .map(|i| (i.krate.clone(), i.ident.clone()))
            .collect();
        let named: BTreeSet<(String, String)> = FORWARDING
            .iter()
            .map(|(k, i)| ((*k).to_owned(), (*i).to_owned()))
            .collect();
        assert_eq!(
            forwarding, named,
            "the adapters forwarding Controller are not the ones FORWARDING names"
        );

        // The gate.
        let neither: Vec<(&(String, String), &String)> = implemented
            .iter()
            .filter(|(t, _)| !spawned.contains(*t) && !dormant.contains(*t))
            .collect();
        assert!(
            neither.is_empty(),
            "a Controller type is neither run by a Driver nor declared Dormant: {neither:?}"
        );
        let both: Vec<&(String, String)> = spawned.intersection(&dormant).collect();
        assert!(
            both.is_empty(),
            "a Dormant controller is run by a Driver; delete its row: {both:?}"
        );
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
    use super::{
        ApiserverTls, BootConfig, RuntimeError, SanEntry, advertised_host, advertised_server_url,
        server_sans,
    };
    use engenho_config::EngenhoConfig;
    use shikumi::TieredConfig;

    fn config_with(advertise: &str, extra: &[&str]) -> BootConfig {
        let mut c = EngenhoConfig::prescribed_default();
        c.runtime.advertise_address = advertise.to_string();
        c.runtime.tls.extra_sans = extra.iter().map(|s| (*s).to_string()).collect();
        BootConfig::read(&c).expect("the runtime runs this config")
    }

    /// The SANs the serving certificate is issued with.
    fn sans(boot: &BootConfig) -> Result<Vec<SanEntry>, RuntimeError> {
        let ApiserverTls::SelfIssued { extra_sans } = &boot.tls else {
            panic!("the prescribed default serves TLS");
        };
        server_sans(extra_sans, &boot.advertise_address)
    }

    fn bound() -> std::net::SocketAddr {
        "0.0.0.0:6443".parse().expect("addr")
    }

    #[test]
    fn advertising_a_name_puts_it_in_the_certificate() {
        let sans = sans(&config_with("plo.natal.quero.cloud", &[])).expect("sans");
        assert!(
            sans.contains(&SanEntry::Dns("plo.natal.quero.cloud".to_string())),
            "the advertised host MUST be a SAN — a kubeconfig naming an address \
             the cert does not is a file that cannot work, and it fails at the \
             client, not here; got {sans:?}"
        );
    }

    #[test]
    fn advertising_an_address_puts_it_in_the_certificate_as_an_ip() {
        let sans = sans(&config_with("100.64.0.7:6443", &[])).expect("sans");
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
        let sans = sans(&cfg).expect("sans");
        assert!(
            sans.contains(&SanEntry::Dns("plo.quero.cloud".to_string())),
            "the SAN must be the bare host; got {sans:?}"
        );
        assert!(
            !sans.iter().any(|s| s.to_string().contains(":16443")),
            "no SAN may carry a port; got {sans:?}"
        );
        assert_eq!(
            advertised_server_url(&cfg.advertise_address, bound()).as_deref(),
            Some("https://plo.quero.cloud:16443"),
            "the URL must keep the advertised port — a proxy or forward makes it \
             legitimately different from the bound one"
        );
    }

    #[test]
    fn an_advertised_host_without_a_port_inherits_the_bound_one() {
        // So an operator naming only a host cannot restate the port wrongly.
        assert_eq!(
            advertised_server_url(&config_with("plo", &[]).advertise_address, bound()).as_deref(),
            Some("https://plo:6443")
        );
    }

    #[test]
    fn advertising_nothing_yields_no_url_and_no_extra_san() {
        let cfg = config_with("", &[]);
        assert_eq!(advertised_server_url(&cfg.advertise_address, bound()), None);
        assert!(
            sans(&cfg).expect("sans").is_empty(),
            "a node-local apiserver must gain no SANs from this path"
        );
    }

    #[test]
    fn declaring_the_advertised_name_explicitly_does_not_duplicate_it() {
        // Listing it in extra_sans as well is the natural thing to do before
        // learning it is automatic, and must not produce a doubled SAN.
        let sans = sans(&config_with("plo.quero.cloud", &["plo.quero.cloud"])).expect("sans");
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
            sans(&config_with("https://plo:6443", &[])).is_err(),
            "a URL is not an address and must be refused, not encoded"
        );
    }

    #[test]
    fn ipv6_is_bracketed_in_the_url_and_bare_in_the_certificate() {
        let cfg = config_with("fd00::1", &[]);
        assert_eq!(advertised_host("fd00::1"), Some("fd00::1"));
        assert_eq!(
            advertised_server_url(&cfg.advertise_address, bound()).as_deref(),
            Some("https://[fd00::1]:6443"),
            "an unbracketed IPv6 host makes a URL that will not parse"
        );
        assert!(
            sans(&cfg)
                .expect("sans")
                .contains(&SanEntry::Ip("fd00::1".parse().unwrap())),
            "the SAN is the bare address, unbracketed"
        );
    }

    #[test]
    fn a_bracketed_ipv6_with_a_port_splits_correctly() {
        assert_eq!(advertised_host("[fd00::1]:6443"), Some("fd00::1"));
        assert_eq!(
            advertised_server_url(
                &config_with("[fd00::1]:6443", &[]).advertise_address,
                bound()
            )
            .as_deref(),
            Some("https://[fd00::1]:6443")
        );
    }
}

/// The pairing that decides whether a public CA is harmless or fatal.
#[cfg(test)]
mod public_ca_guard {
    use super::is_loopback_only;

    #[test]
    fn loopback_is_the_only_safe_address() {
        for addr in ["127.0.0.1:6443", "127.0.0.53:6443", "[::1]:6443"] {
            assert!(
                is_loopback_only(addr.parse().unwrap()),
                "{addr} is loopback and a pre-seed cluster stays usable on it"
            );
        }
    }

    #[test]
    fn unspecified_addresses_are_the_most_reachable_not_the_least() {
        // ★ The inversion this guard exists to prevent. `0.0.0.0` binds EVERY
        // interface, so it is maximally reachable — but `is_loopback()` returns
        // false for it and a careless guard reads that as "no specific address,
        // therefore nothing can reach it". Getting this backwards would let the
        // one case that most needs refusing sail through.
        for addr in ["0.0.0.0:6443", "[::]:6443"] {
            assert!(
                !is_loopback_only(addr.parse().unwrap()),
                "{addr} binds every interface and must NOT count as loopback"
            );
        }
    }

    #[test]
    fn routable_addresses_are_not_loopback() {
        for addr in ["100.91.10.110:6443", "192.168.50.3:6443", "[fd00::1]:6443"] {
            assert!(!is_loopback_only(addr.parse().unwrap()), "{addr}");
        }
    }
}

#[cfg(test)]
mod preflight_stderr_tail {
    use super::{STDERR_TAIL_MAX, stderr_tail};

    /// The shape that cost real time on ryn, 2026-09-13: podman prints a human
    /// preamble and puts the actual cause LAST. Taking the head would select
    /// the noise and report an unusable node as merely "exited 125".
    #[test]
    fn selects_the_cause_not_the_preamble() {
        let stderr = b"OS: darwin/arm64\nprovider: applehv\nversion: 5.7.0\n\n\
Cannot connect to Podman. Please verify your connection\n\
Error: unable to connect to Podman socket: knownhosts: /Users/x/.ssh/known_hosts:9: knownhosts: missing key type pattern\n";
        let tail = stderr_tail(stderr);
        assert!(
            tail.contains("known_hosts:9"),
            "the file AND line are the whole diagnosis; got {tail:?}"
        );
        assert!(
            !tail.contains("provider: applehv"),
            "preamble must not be selected; got {tail:?}"
        );
        assert!(tail.starts_with(": "), "must append cleanly; got {tail:?}");
    }

    /// ★ NEGATIVE CONTROL. Without this the function could return a constant
    /// non-empty string and every other assertion here would still pass.
    #[test]
    fn silent_stderr_yields_nothing_to_append() {
        assert_eq!(stderr_tail(b""), "");
        assert_eq!(stderr_tail(b"\n   \n\t\n"), "", "whitespace is not a cause");
    }

    /// Bounded, because an unbounded paste scrolls the useful line away — the
    /// same failure this preflight exists to fix, from the other direction.
    #[test]
    fn a_chatty_runtime_cannot_flood_the_error() {
        let noisy = "x".repeat(5_000);
        let tail = stderr_tail(noisy.as_bytes());
        assert!(
            tail.len() <= STDERR_TAIL_MAX + 8,
            "unbounded tail: {} bytes",
            tail.len()
        );
        assert!(
            tail.ends_with('…'),
            "truncation must be visible to the reader"
        );
    }

    /// Slicing UTF-8 at an arbitrary byte index panics. A runtime's error text
    /// is not guaranteed ASCII, so the truncation walks to a char boundary.
    #[test]
    fn truncating_multibyte_text_does_not_panic() {
        let wide = "é".repeat(STDERR_TAIL_MAX);
        let tail = stderr_tail(wide.as_bytes());
        assert!(tail.ends_with('…'));
    }

    /// Invalid UTF-8 from a subprocess is data, not a reason to report nothing.
    #[test]
    fn invalid_utf8_still_reports_something() {
        assert!(!stderr_tail(&[b'E', b'r', b'r', 0xFF, 0xFE]).is_empty());
    }
}
