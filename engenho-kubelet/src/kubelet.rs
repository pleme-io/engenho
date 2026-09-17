//! `Kubelet` — the per-node reconcile loop.
//!
//! Watches Pods bound to this node (`spec.nodeName == self.node_name`),
//! materializes containers via the [`ContainerRuntime`] backend, and
//! reconciles `status` (phase / conditions / podIP / containerStatuses)
//! through the shared item-5 CAS write primitive.
//!
//! ## Reconcile-diff (M0.1 item 9)
//!
//! The tick is a full diff over `(live bound set)` vs `(local
//! bookkeeping)`, plus a running-status poll:
//!
//!   * **Delete-cleanup** — a Pod we started locally that is no longer in
//!     the freshly-listed bound set (hard-deleted, or its `spec.nodeName`
//!     moved away) is orphaned on this node: `stop` THEN `remove` on the
//!     backend, then drop the local entry. (The store key is already
//!     gone for a delete; there is nothing to patch.)
//!   * **Start** — a bound Pod not yet in `local` (and not terminal) gets
//!     a container started + an initial `Running` status written via CAS.
//!   * **Running-status reconciliation** — a bound Pod already in `local`
//!     is polled via `backend.status`; still-running stays `Running`,
//!     terminated maps `exit_code → Succeeded | Failed`. A vanished
//!     container (no backend record) clears the local entry so the next
//!     tick re-creates it (a managed bound Pod converges back to running).
//!
//! Every status write goes through [`write_status_cas`] (item-5
//! optimistic concurrency); the kubelet issues NO unconditional
//! (`expected: None`) status writes. A still-running Pod produces a
//! `NoChange` (idempotent-skip) → zero store writes → no watch storm.
//! A Pod's container is started exactly ONCE across its lifetime —
//! membership in `local` is the guard, never `phase == Running`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use engenho_controllers::{
    Controller, ControllerError, ReconcileOutcome, ReconcileReport, ReconcileResult,
    dns::DEFAULT_CLUSTER_DOMAIN,
    selector::{matches_labels, service_selector},
    status::{resource_version_of, write_status_cas},
};
use engenho_store::{StoreMesh, resource::ResourceKey};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::backend::{ContainerRuntime, ContainerSpec, LogOptions, NetProber, TokioNetProber};
use crate::error::KubeletError;
use crate::lifecycle::{
    ContainerObservation, ContainerState, ContainerStatusOut, RestartPolicy, reconcile_pod_phase,
};
use crate::pod_volume::{
    MountSource, PodVolumeSource, PodmanVolumeMaterializer, VolumeMaterializer, VolumeResolveError,
    container_mounts, pod_volumes,
};
use crate::probe::{
    ProbeKind, ProbeRuntime, ProbeSpec, aggregate_container_readiness, fold_probe_observation,
    run_handler,
};

/// The lowest period the kubelet will requeue at — a 1s floor so a
/// `periodSeconds: 1` probe does not spin the loop faster than the runtime can
/// service it. Mirrors the K8s min period.
const MIN_PROBE_REQUEUE: Duration = Duration::from_secs(1);

/// The three probes (liveness/readiness/startup) a container may carry, each
/// paired with its persistent [`ProbeRuntime`] counters. Lives on the
/// per-container [`ContainerRecord`] so the counters persist across ticks (like
/// `restart_count`) + reset on a container restart (fresh startup window).
///
/// `None` for a probe the container doesn't declare. An all-`None`
/// `ContainerProbeState` is the behavior-preserving common case: no probe ⇒
/// `aggregate_container_readiness` returns `ready = is_running`, byte-identical
/// to the pre-probe kubelet.
#[derive(Clone, Debug, Default)]
struct ContainerProbeState {
    liveness: Option<(ProbeSpec, ProbeRuntime)>,
    readiness: Option<(ProbeSpec, ProbeRuntime)>,
    startup: Option<(ProbeSpec, ProbeRuntime)>,
}

impl ContainerProbeState {
    /// `true` iff the container declares NO probes (the behavior-preserving
    /// common case — no requeue armed, ready mirrors is_running).
    fn is_empty(&self) -> bool {
        self.liveness.is_none() && self.readiness.is_none() && self.startup.is_none()
    }

    /// Reset all probe runtimes to a fresh window (called on a container
    /// restart so the startup gate re-arms + counters zero). Preserves the
    /// parsed specs.
    fn reset(&mut self, now: Instant) {
        for (_, rt) in [&mut self.liveness, &mut self.readiness, &mut self.startup]
            .into_iter()
            .flatten()
        {
            *rt = ProbeRuntime::new(now);
        }
    }
}

/// The aggregated probe decision for one running container this tick: its
/// effective readiness + whether a liveness/startup verdict requests a restart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ProbeOutcome {
    /// The container's effective `ready` (→ `containerStatuses[].ready`).
    ready: bool,
    /// `true` iff a liveness/startup verdict (post-gating) requests a restart.
    needs_restart: bool,
}

/// What the kubelet remembers about ONE container of a Pod it started.
#[derive(Clone, Debug, Default)]
struct ContainerRecord {
    /// Opaque backend handle returned by `backend.start` for this container.
    /// Re-assigned on a restart (the backend mints a new id).
    container_id: String,
    /// How many times this container has been (re)started. `0` on the first
    /// start; bumped on each restart-policy-driven re-`start`.
    restart_count: u32,
    /// Per-container probe specs + runtime counters (liveness/readiness/
    /// startup). Default = all-None = no probes = behavior-preserving.
    probes: ContainerProbeState,
    /// When this container was (re)started. Feeds `uptime_before_exit`, which
    /// is what lets a container that stayed up long enough earn a clean slate
    /// instead of inheriting an old crash's penalty.
    started_at: Option<Instant>,
    /// The resolved volume mounts this container was STARTED with.
    ///
    /// ── ★ WHY THE RECORD REMEMBERS THEM ──────────────────────────────
    /// Mounts are resolved on the create path only: it holds the
    /// `volName → MountSource` map and the projected ServiceAccount, and
    /// stamps them onto the spec. The restart path rebuilds its specs from
    /// `pod_to_container_specs`, a pure function of the Pod value, which
    /// emits `mounts: vec![]` because resolution needs `&self` and an
    /// await. So a restart re-ran `podman run` with NO `-v` flags at all
    /// and the replacement container came up with every volume missing.
    ///
    /// This is the same defect the `kubernetes_service` field records one
    /// field over — an input written at ONE of the three `backend.start`
    /// call sites, missing from the restart path. Measured 2026-08-31: a
    /// controller lost its projected ServiceAccount token on its first
    /// restart and then CrashLooped on `Config::incluster()`, which reads
    /// as an auth bug and is really a lost mount.
    ///
    /// Remembering is correct rather than re-resolving: mounts are
    /// materialized once at pod-start (see `ContainerSpec::mounts`), the
    /// backing directories outlive the container, and re-projecting here
    /// would change that documented semantic.
    mounts: Vec<crate::pod_volume::ResolvedMount>,
    /// When the kubelet FIRST observed this container terminated. `None`
    /// while it is running.
    ///
    /// ★ FIRST observation, not most recent: the backoff clock must run from
    /// the exit, and re-stamping it every tick would reset the wait on every
    /// poll — a hold that never elapses, which is a hang wearing a
    /// CrashLoopBackOff label.
    terminated_at: Option<Instant>,
}

/// What the kubelet remembers about a Pod it started on this node.
/// Keyed in [`Kubelet::local`] by the Pod's typed [`ResourceKey`] so
/// delete-cleanup can reconstruct the Pod identity unambiguously (the
/// `format!("{ns}_{name}")` container name is a lossy join — a `_` in
/// either part can't be split back, so it MUST NOT be used as a key).
///
/// MULTI-CONTAINER: a Pod runs every `spec.containers[i]`, so the
/// bookkeeping is a map keyed by CONTAINER NAME (`spec.containers[i].name`)
/// → its [`ContainerRecord`]. The deterministic backend name per container is
/// `<ns>_<pod>_<containerName>`.
/// What one projected-token refresh pass looked at and rewrote.
///
/// Carries its own denominator (`examined`) on purpose: a pass that refreshed
/// 0 of 0 pods and a pass that refreshed 0 of 12 are completely different
/// events, and a bare "refreshed" count renders them identically. A discovery
/// regression shows up here as `examined: 0`, not as a quiet success.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SaRefreshReport {
    /// Started pods carrying a namespace that this pass considered.
    pub examined: usize,
    /// Tokens successfully re-minted AND rewritten.
    pub refreshed: usize,
    /// Pods whose token could not be re-minted or could not be written.
    pub failed: usize,
    /// The cadence gate declined to run this tick. Distinct from
    /// `examined: 0`, which means the pass RAN and found nothing.
    pub skipped_not_due: bool,
}

#[derive(Clone, Debug, Default)]
struct LocalPod {
    /// Per-container records keyed by the container's logical name. These are
    /// the APP containers (`spec.containers[i]`); they are started only AFTER
    /// every init container has Succeeded (`init_complete == true`).
    containers: BTreeMap<String, ContainerRecord>,
    /// Per-INIT-container records keyed by the init container's logical name
    /// (`spec.initContainers[i].name`). Init containers run ONE AT A TIME, in
    /// order; this map holds only those the kubelet has started so far (one,
    /// then the next once the prior Succeeds). Empty for a pod with no init
    /// containers (the common case) — that pod's `init_complete` is true from
    /// the first reconcile and the app-start path runs identically to before
    /// the init-container brick. Init containers do NOT carry probes (K8s does
    /// not run liveness/readiness/startup on init containers), so each
    /// [`ContainerRecord::probes`] stays default (all-None).
    init_containers: BTreeMap<String, ContainerRecord>,
    /// `true` once every init container has Succeeded (or the pod declares no
    /// init containers). Latched: once the init sequence is `Complete`, app
    /// containers may start + the pod proceeds to the app reconcile. A pod with
    /// no init containers reaches `init_complete = true` on its first start.
    init_complete: bool,
    /// emptyDir volume NAMES (`spec.volumes[i].name`, NOT the backing podman
    /// volume name) this pod created. Recorded at start so delete-cleanup can
    /// reap each one via `volume_materializer.remove_empty_dir(ns, pod, name)`
    /// — emptyDir is pod-lifetime scratch, so its named volume dies with the
    /// pod (alongside the container stop+remove). configMap/secret HostDir
    /// sources are NOT recorded here — they're plain files under the data root,
    /// reaped by the pod-dir GC, not a podman named volume.
    empty_dir_volumes: Vec<String>,
}

/// The kubelet's clock — a `now()` source. Defaults to [`Instant::now`];
/// tests inject a controllable clock so probe `period` / `initialDelay`
/// cadence is exercised deterministically without sleeping (the Environment
/// trait discipline applied to time — the prober's period logic is testable
/// WITHOUT wall-clock waits).
type Clock = Arc<dyn Fn() -> Instant + Send + Sync>;

/// A controllable test clock: a shared [`Instant`] the test advances. Wrap in a
/// [`Clock`] via [`TestClock::as_clock`].
#[derive(Clone)]
pub struct TestClock {
    inner: Arc<std::sync::Mutex<Instant>>,
}

impl TestClock {
    /// New test clock anchored at the current instant.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(Instant::now())),
        }
    }

    /// Advance the clock by `delta`.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while held) —
    /// test-only, so a poisoned clock is a test bug to surface, not absorb.
    pub fn advance(&self, delta: Duration) {
        let mut g = self.inner.lock().unwrap();
        *g += delta;
    }

    /// A [`Clock`] reading this test clock. The returned closure panics if the
    /// internal mutex is poisoned (test-only).
    #[must_use]
    pub fn as_clock(&self) -> Clock {
        let inner = self.inner.clone();
        Arc::new(move || *inner.lock().unwrap())
    }
}

impl Default for TestClock {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-node kubelet. Implements [`Controller`] so it slots into the
/// standard [`engenho_controllers::ControllerRuntime`] + benefits from
/// `WatchDriver` event-driven wakeup.
pub struct Kubelet {
    store: Arc<StoreMesh>,
    backend: Arc<dyn ContainerRuntime>,
    /// The network-probe seam (httpGet + tcpSocket against the pod IP).
    /// Defaults to [`TokioNetProber`]; tests inject a `FakeNetProber` via
    /// [`Kubelet::with_net_prober`]. Held separately from `backend` because
    /// http/tcp target a routable IP (no container-namespace dependency) — see
    /// the [`NetProber`] doc.
    net_prober: Arc<dyn NetProber>,
    /// The volume-materializer seam (configMap/secret → host files;
    /// emptyDir → podman named volume). Defaults to
    /// [`PodmanVolumeMaterializer`]; tests inject a `FakeVolumeMaterializer`
    /// via [`Kubelet::with_volume_materializer`]. Held as a THIRD trait
    /// object (alongside `backend` + `net_prober`) per the M0.7 kubelet-
    /// volumes brick — the trait IS the testability contract, so volume
    /// resolution is unit-testable WITHOUT real podman.
    volume_materializer: Arc<dyn VolumeMaterializer>,
    /// Supplies a pod's ServiceAccount credentials. Defaults to
    /// [`NoServiceAccountProjection`] so a kubelet with no signing key
    /// projects nothing rather than an empty token.
    sa_projector: Arc<dyn crate::pod_volume::ServiceAccountProjector>,
    /// The `now()` source (defaults to [`Instant::now`]; overridable for
    /// deterministic probe-cadence tests via [`Kubelet::with_clock`]).
    clock: Clock,
    /// Where lifecycle events go. Defaults to the null sink so emission is
    /// safe to add to a code path before the plumbing exists — the
    /// alternative being an `Option` check at every call site.
    events: Arc<dyn engenho_controllers::event_recorder::EventSink>,
    /// When this kubelet last wrote its node lease.
    ///
    /// ★ THE CADENCE IS LOAD-BEARING, not a nicety. A heartbeat written on
    /// every tick makes every idle reconcile a STORE WRITE, which advances
    /// the revision forever and defeats the idempotent-skip defense the
    /// rest of this controller is built around — caught by
    /// `deployment_status_converges_then_reconcile_is_bounded`, which is
    /// exactly the hot-loop tripwire it exists to be. Upstream renews on
    /// `RENEW_INTERVAL`, not per sync loop, for the same reason.
    last_lease_renewal: Mutex<Option<Instant>>,
    /// When this kubelet last rewrote its pods' projected ServiceAccount
    /// tokens. Same cadence discipline as `last_lease_renewal` and for the
    /// same reason — re-minting on every tick would make every idle reconcile
    /// a filesystem write.
    last_sa_refresh: Mutex<Option<Instant>>,
    node_name: String,
    /// Bookkeeping for every Pod we started, keyed by its typed
    /// [`ResourceKey`]. Persists for the kubelet's process lifetime; on
    /// restart we re-derive by re-creating (a managed bound Pod with no
    /// live container converges back to running).
    local: Mutex<BTreeMap<ResourceKey, LocalPod>>,
}

impl Kubelet {
    /// Construct a kubelet for `node_name` with the real [`TokioNetProber`].
    #[must_use]
    pub fn new(
        store: Arc<StoreMesh>,
        backend: Arc<dyn ContainerRuntime>,
        node_name: impl Into<String>,
    ) -> Self {
        Self {
            store,
            backend,
            net_prober: Arc::new(TokioNetProber::new()),
            volume_materializer: Arc::new(PodmanVolumeMaterializer::new()),
            clock: Arc::new(Instant::now),
            events: Arc::new(engenho_controllers::event_recorder::NullEventSink),
            sa_projector: Arc::new(crate::pod_volume::NoServiceAccountProjection),
            last_lease_renewal: Mutex::new(None),
            last_sa_refresh: Mutex::new(None),
            node_name: node_name.into(),
            local: Mutex::new(BTreeMap::new()),
        }
    }

    /// Builder: override the [`VolumeMaterializer`] (configMap/secret/emptyDir
    /// host-effecting seam). Tests pass a `FakeVolumeMaterializer` so volume
    /// resolution is exercised without real podman / a real filesystem;
    /// production keeps the default [`PodmanVolumeMaterializer`].
    #[must_use]
    pub fn with_volume_materializer(mut self, m: Arc<dyn VolumeMaterializer>) -> Self {
        self.volume_materializer = m;
        self
    }

    /// Builder: override the [`NetProber`] (httpGet/tcpSocket seam). Tests pass
    /// a `FakeNetProber` so http/tcp probe logic is exercised without a real
    /// socket; production keeps the default [`TokioNetProber`].
    #[must_use]
    pub fn with_net_prober(mut self, net_prober: Arc<dyn NetProber>) -> Self {
        self.net_prober = net_prober;
        self
    }

    /// Builder: override the clock (defaults to [`Instant::now`]). Tests inject
    /// a [`TestClock`] so probe-cadence (`period` / `initialDelay`) is exercised
    /// deterministically without sleeping.
    #[must_use]
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// Builder: override the event sink (defaults to the null sink).
    #[must_use]
    pub fn with_event_sink(
        mut self,
        events: Arc<dyn engenho_controllers::event_recorder::EventSink>,
    ) -> Self {
        self.events = events;
        self
    }

    /// Builder: supply the pod ServiceAccount projector.
    #[must_use]
    pub fn with_sa_projector(
        mut self,
        projector: Arc<dyn crate::pod_volume::ServiceAccountProjector>,
    ) -> Self {
        self.sa_projector = projector;
        self
    }

    /// Emit one Pod lifecycle event.
    ///
    /// The timestamp is wall-clock rather than this kubelet's injected
    /// `Instant` clock: an `Instant` has no calendar meaning, and an event a
    /// human reads needs a time they can compare against their own logs.
    /// That is why the test clock does not reach here — deliberately.
    async fn emit(
        &self,
        key: &ResourceKey,
        reason: engenho_controllers::event_recorder::Reason,
        message: impl Into<String>,
    ) {
        engenho_controllers::event_recorder::record_pod_event(
            self.events.as_ref(),
            key.namespace.as_deref().unwrap_or("default"),
            &key.name,
            None,
            reason,
            message,
            "kubelet",
            &engenho_types::time::now_rfc3339_utc(),
        )
        .await;
    }

    /// How often to rewrite a projected token, DERIVED from its lifetime.
    ///
    /// A third of the lifetime, so a pod gets three chances to be rewritten
    /// before its credential expires and two consecutive failures are still
    /// survivable. A fraction rather than a constant is the whole point: the
    /// lifetime is owned by whoever mints (the runtime), and any absolute
    /// number here would be a second declaration of the same fact, free to
    /// drift the moment the lifetime changes.
    ///
    /// Floored, because a pathologically short lifetime must degrade into a
    /// busy kubelet rather than a hot loop that rewrites files continuously.
    /// A floor above the lifetime means the token expires — that is a
    /// misconfiguration and the floor makes it survivable, not correct.
    fn sa_refresh_interval(lifetime: std::time::Duration) -> std::time::Duration {
        (lifetime / 3).max(std::time::Duration::from_secs(10))
    }

    /// Rewrite every started pod's projected ServiceAccount token before it
    /// expires.
    ///
    /// ── ★ WHAT ITS ABSENCE BROKE, AND WHY IT LOOKED LIKE SOMETHING ELSE ───
    /// The projection ran exactly ONCE, on the path that starts a pod's
    /// containers. Tokens are bound and therefore carry an `exp`, so every
    /// API-calling workload worked perfectly and then began failing at a
    /// fixed age — one hour by default. Nothing in the kubelet's own state
    /// looked wrong at that moment: the pod was Running, its containers were
    /// healthy, the signing key was fine, and the apiserver was correctly
    /// rejecting a genuinely expired credential. The symptom (an operator
    /// that works after a restart and dies an hour later) points at the
    /// workload, which is the most expensive place for it to point.
    ///
    /// ── ★ WHY REWRITING IN PLACE IS SOUND, AND NOT A RESTART ─────────────
    /// `materialize_files` derives a STABLE directory from
    /// `(namespace, pod, volume)` and that directory is bind-mounted into the
    /// container, so a rewrite on the host is visible inside the container
    /// immediately — no restart, no remount, no container churn. And it
    /// writes through `write_atomic`, so a client reading the token
    /// concurrently gets either the old token or the new one and never a
    /// truncated one. A torn token would surface as a signature failure,
    /// i.e. as a key-rotation incident that never happened.
    ///
    /// ── ★ SCOPED TO PODS WE STARTED, DELIBERATELY ────────────────────────
    /// A bound pod with no local entry has not reached the start path, and
    /// that path projects afresh every time it runs — so a pod still waiting
    /// on an image gets a fresh token when it finally starts, and refreshing
    /// it here would write files nothing has mounted.
    ///
    /// Failure is logged, never fatal, and never stops pod work: a pod whose
    /// refresh failed keeps the token it has until that token expires, which
    /// is strictly better than a kubelet that stopped reconciling.
    async fn refresh_service_account_projections(
        &self,
        bound: &BTreeMap<ResourceKey, Value>,
    ) -> SaRefreshReport {
        let mut report = SaRefreshReport::default();

        // No lifetime ⇒ this projector mints nothing ⇒ there is nothing to
        // keep alive. The honest no-op, not a silent skip.
        let Some(lifetime) = self.sa_projector.token_lifetime() else {
            return report;
        };
        let interval = Self::sa_refresh_interval(lifetime);

        // Due yet? Read against the injected clock so the cadence is testable
        // without sleeping — the same discipline `renew_node_lease` uses.
        {
            let now = self.now();
            let mut last = self.last_sa_refresh.lock().await;
            if let Some(prev) = *last {
                if now.saturating_duration_since(prev) < interval {
                    report.skipped_not_due = true;
                    return report;
                }
            }
            *last = Some(now);
        }

        let started: BTreeSet<ResourceKey> = self.local.lock().await.keys().cloned().collect();

        for (key, pod) in bound {
            if !started.contains(key) {
                continue;
            }
            let Some(namespace) = key.namespace.as_deref() else {
                continue;
            };
            report.examined += 1;

            let sa_name = pod
                .pointer("/spec/serviceAccountName")
                .and_then(Value::as_str)
                .unwrap_or("default");
            let pod_uid = pod
                .pointer("/metadata/uid")
                .and_then(Value::as_str)
                .unwrap_or_default();

            match self
                .sa_projector
                .project(namespace, sa_name, &key.name, pod_uid)
                .await
            {
                Ok(Some(files)) => {
                    match self
                        .volume_materializer
                        .materialize_files(namespace, &key.name, "kube-api-access", &files)
                        .await
                    {
                        Ok(_) => report.refreshed += 1,
                        Err(e) => {
                            report.failed += 1;
                            warn!(
                                pod = %key.label(),
                                error = %e,
                                "ServiceAccount token refresh could not be written; the pod \
                                 keeps its current token until that token expires"
                            );
                        }
                    }
                }
                // Nothing to project for this pod — not a failure.
                Ok(None) => {}
                Err(e) => {
                    report.failed += 1;
                    warn!(
                        pod = %key.label(),
                        error = %e,
                        "ServiceAccount token refresh could not be minted; the pod keeps its \
                         current token until that token expires"
                    );
                }
            }
        }

        report
    }

    /// Write this node's heartbeat into `kube-node-lease`.
    ///
    /// ★ THIS IS THE PRODUCER `node_lease` WAS MISSING. The module defined
    /// the key, the object shape, the renew interval, the grace period and
    /// the readiness derivation — and nothing ever wrote a lease, so the
    /// derivation had no input and every node's readiness stayed whatever
    /// it was first set to. A `Ready` condition that cannot become
    /// `Unknown` is not a health signal, it is a constant.
    ///
    /// `transitions` is left at 0: it counts LEADER transitions, which is
    /// meaningful for a lock-style Lease and not for a heartbeat one. A
    /// number incremented for its own sake is worse than a stable zero.
    async fn renew_node_lease(&self) {
        // Due yet? First call always is; after that, only every
        // RENEW_INTERVAL. Read against this kubelet's injected clock so the
        // cadence is testable without sleeping.
        {
            let now = self.now();
            let mut last = self.last_lease_renewal.lock().await;
            if let Some(prev) = *last {
                if now.saturating_duration_since(prev) < crate::node_lease::RENEW_INTERVAL {
                    return;
                }
            }
            *last = Some(now);
        }

        let key = crate::node_lease::lease_key(&self.node_name);
        let value = crate::node_lease::lease_value(
            &self.node_name,
            &engenho_types::time::now_rfc3339_utc(),
            0,
        );
        if let Err(e) = self
            .store
            .propose(engenho_store::command::ResourceCommand::Put {
                key,
                value,
                // No precondition: a heartbeat is last-writer-wins by
                // definition. A CAS would make a lost race look like a dead
                // node, which is the exact misreading this lease exists to
                // prevent.
                expected: None,
                reason: engenho_store::command::Reason::Controller,
            })
            .await
        {
            warn!(
                node = %self.node_name,
                error = %e,
                "node lease renewal failed; the node may be reported NotReady"
            );
        }
    }

    /// Derive this node's `Ready` condition from its Lease and publish it.
    ///
    /// ── ★ THE CONSUMER `node_lease` WAS MISSING ───────────────────────────
    /// `readiness()` and `ready_condition()` were written, documented and
    /// tested, and had **zero non-test callers**. The producer above writes a
    /// heartbeat every 10s and nothing ever read it, so the Node's condition
    /// stayed the literal `{"type":"Ready","status":"True"}` that
    /// `register_node` stamps once at boot — for the life of the process, on
    /// every node. The scheduler CONSUMES that value
    /// (`engenho-scheduler/src/strategy.rs`'s `is_schedulable`), so it was a
    /// constant standing in for a health signal, not an unused field.
    ///
    /// ── ★ WHY IT DERIVES FROM THE LEASE AS READ BACK, NOT FROM `now()` ────
    /// Judging our own liveness from our own clock is circular: this code only
    /// runs when the kubelet is alive, so it could only ever conclude "alive".
    /// Reading the Lease back out of the store makes one real failure
    /// observable — **the store being unreachable**. If `propose` above failed,
    /// or the mesh is partitioned, the lease we read is stale (or absent) and
    /// the condition honestly degrades, even though this process is running.
    ///
    /// ── ★ WHAT IT STILL CANNOT DO, NAMED RATHER THAN IMPLIED ──────────────
    /// A node whose engenho process is DEAD writes neither the lease nor the
    /// condition, so its last-published value stands. Detecting that requires a
    /// different actor reading the lease — upstream's node-lifecycle-controller,
    /// which is a separate process precisely because a dead kubelet cannot
    /// report its own death. `pending-node-lifecycle-controller`: on a
    /// multi-node engenho (`engenho-revoada` carries the membership layer)
    /// peers can judge each other's leases; single-node cannot, and no amount
    /// of code here changes that.
    async fn publish_node_readiness(&self) {
        let lease_key = crate::node_lease::lease_key(&self.node_name);
        // Age of the heartbeat AS THE STORE HAS IT.
        let since_renew = match self.store.get(&lease_key).await {
            Some(lease) => lease
                .get("spec")
                .and_then(|s| s.get("renewTime"))
                .and_then(|t| t.as_str())
                .and_then(engenho_types::time::age_since_rfc3339),
            // No lease in the store at all — never written, or lost.
            None => None,
        };
        let state = crate::node_lease::readiness(since_renew);

        let node_key = ResourceKey::cluster_scoped("", "v1", "Node", &self.node_name);
        let Some(node) = self.store.get(&node_key).await else {
            // No Node object yet: registration has not landed. Not an error —
            // the next tick will find it.
            return;
        };
        let previous = crate::node_lease::find_ready_condition(&node);
        let condition = crate::node_lease::ready_condition(
            state,
            &engenho_types::time::now_rfc3339_utc(),
            previous,
        );

        // Skip the write when nothing an operator would act on has changed.
        // `lastHeartbeatTime` moves every tick by design, so comparing whole
        // conditions would write on every single tick forever — which is how
        // the store journal grows without bound while the cluster is idle.
        let unchanged = previous.is_some_and(|p| {
            p.get("status") == condition.get("status") && p.get("reason") == condition.get("reason")
        });
        if unchanged {
            return;
        }

        // Merge BY TYPE. Replacing `status.conditions` wholesale would drop
        // every condition this kubelet does not own — the same array-replacement
        // defect the Pod status path has.
        let mut conditions: Vec<serde_json::Value> = node
            .get("status")
            .and_then(|s| s.get("conditions"))
            .and_then(|c| c.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|c| c.get("type").and_then(|t| t.as_str()) != Some("Ready"))
            .collect();
        conditions.push(condition);

        let mut desired = node.clone();
        if let Some(obj) = desired.as_object_mut() {
            let status = obj.entry("status").or_insert_with(|| serde_json::json!({}));
            if let Some(status_obj) = status.as_object_mut() {
                status_obj.insert("conditions".to_string(), serde_json::json!(conditions));
            }
        }

        if let Err(e) = self
            .store
            .propose(engenho_store::command::ResourceCommand::Put {
                key: node_key,
                value: desired,
                expected: None,
                reason: engenho_store::command::Reason::Controller,
            })
            .await
        {
            warn!(
                node = %self.node_name,
                error = %e,
                "could not publish the node Ready condition"
            );
        } else {
            info!(
                node = %self.node_name,
                status = state.condition_status(),
                reason = state.reason(),
                "node Ready condition published"
            );
        }
    }

    /// The current instant per this kubelet's clock.
    fn now(&self) -> Instant {
        (self.clock)()
    }

    /// This kubelet's node name (telemetry helper).
    #[must_use]
    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    /// Backend name (telemetry helper).
    #[must_use]
    pub fn backend_name(&self) -> &'static str {
        self.backend.name()
    }

    /// Extract a [`ContainerSpec`] for EVERY `spec.containers[i]` of a Pod
    /// manifest, paired with that container's logical name (the local
    /// bookkeeping key). The returned `(container_name, spec)` pairs preserve
    /// `spec.containers` order.
    ///
    /// MULTI-CONTAINER: the kubelet runs one [`ContainerSpec`] per
    /// `spec.containers[i]`. INIT CONTAINERS are extracted separately by
    /// [`Self::pod_to_init_container_specs`] (which consumes the pure
    /// sequencing interpreter [`crate::lifecycle::next_init_action`]).
    /// Ephemeral containers remain a documented no-op. The backend container
    /// name is the deterministic `<ns>_<pod>_<containerName>` join so
    /// status/stop/remove by-name is possible per container.
    ///
    /// Env + command + args are read per container: `command` →
    /// `ContainerSpec.command` (the entrypoint override) followed by `args`
    /// (appended, mirroring K8s where `args` are the entrypoint's arguments).
    ///
    /// `spec.containers` is REQUIRED (a pod with no app containers is invalid):
    /// a missing / empty array is a typed [`KubeletError::InvalidPod`].
    fn pod_to_container_specs(
        namespace: &str,
        name: &str,
        pod: &Value,
        sources: &BTreeMap<(String, String), Value>,
    ) -> Result<Vec<(String, ContainerSpec)>, KubeletError> {
        Self::extract_container_specs(namespace, name, pod, "containers", false, sources)
    }

    /// Extract a [`ContainerSpec`] for EVERY `spec.initContainers[i]`, in
    /// `spec.initContainers` order — the I/O-side companion to the pure
    /// [`crate::lifecycle::next_init_action`] sequencer.
    ///
    /// INIT-CONTAINER NAME DISAMBIGUATION: the backend `--name` for an init
    /// container is `<ns>_<pod>_init-<cname>` (the `init-` prefix on the
    /// container segment), NOT the app-container `<ns>_<pod>_<cname>`. K8s
    /// permits an init container and an app container of a pod to share a
    /// logical name; without the prefix their deterministic podman names would
    /// collide ("name already in use"). The logical name returned in the
    /// `(cname, spec)` pair is the RAW `spec.initContainers[i].name` (no
    /// prefix) — it is the bookkeeping key under `LocalPod::init_containers`
    /// and the `status.initContainerStatuses[].name`, matching the manifest.
    ///
    /// EMPTY / ABSENT `spec.initContainers` ⇒ `Ok(vec![])` WITHOUT error — the
    /// common case (every pre-init-brick pod has zero init containers). This is
    /// the behavior-preserving guarantee: no init containers → empty Vec → the
    /// init sequencer returns `Complete` immediately → the app-start path runs
    /// byte-identically to before this brick.
    fn pod_to_init_container_specs(
        namespace: &str,
        name: &str,
        pod: &Value,
        sources: &BTreeMap<(String, String), Value>,
    ) -> Result<Vec<(String, ContainerSpec)>, KubeletError> {
        Self::extract_container_specs(namespace, name, pod, "initContainers", true, sources)
    }

    /// Shared container-extraction core, parameterized by the `spec` key
    /// (`"containers"` | `"initContainers"`) and whether an absent / empty
    /// array is allowed (`true` for init containers — the common no-init case;
    /// `false` for app containers — a pod with no app containers is invalid).
    ///
    /// `optional` ALSO selects the backend `--name` shape: init containers
    /// (`optional == true`) get the `<ns>_<pod>_init-<cname>` disambiguating
    /// prefix so they never collide with a same-named app container's
    /// `<ns>_<pod>_<cname>`. The RETURNED logical name is always the raw
    /// `spec.<key>[i].name` (the bookkeeping key + status name).
    /// Resolve one `spec.containers[].env[]` entry to a `(name, value)` pair.
    ///
    /// ## The defect this replaces
    ///
    /// The previous extractor was a `filter_map` requiring a literal
    /// `value` key:
    ///
    /// ```ignore
    /// let v = e.get("value")?.as_str()?.to_string();
    /// ```
    ///
    /// Any entry carrying `valueFrom` has no `value` key, so `?` yielded
    /// `None` and the variable **vanished** — no error, no Pending reason,
    /// no log line. The container started, looked healthy, and was simply
    /// missing the variable. Measured casualties in the reference pangea
    /// render were the downward-API `POD_NAME` / `POD_NAMESPACE` /
    /// `NODE_NAME`, and `leader.rs` resolves pod identity from `POD_NAME`,
    /// so leader election degraded **silently**.
    ///
    /// ## What this does instead
    ///
    /// * a literal `value` is used as-is;
    /// * a bare `{name}` with neither `value` nor `valueFrom` is the empty
    ///   string, which is upstream's semantics, not a guess;
    /// * `valueFrom.fieldRef` is RESOLVED — every supported path is
    ///   answerable from the pod object already in hand, needing no store
    ///   access;
    /// * every other `valueFrom` source is a typed `InvalidPod` naming the
    ///   variable AND the source kind.
    ///
    /// That last arm is the point. `secretKeyRef` and `configMapKeyRef`
    /// need store access the kubelet does not have here, so they are not
    /// supported yet — but an unsupported source now **fails loudly at
    /// admission** instead of producing a container that runs without its
    /// credentials. A pod that cannot get its environment must not reach
    /// Running, because "started successfully, silently misconfigured" is
    /// the single hardest state to debug from outside.
    /// Expand `$(VAR)` references in an env value or an argv element, using
    /// the variables already defined for this container.
    ///
    /// **Kubernetes does this and every workload assumes it.** A manifest
    /// writes `value: "source-controller.$(RUNTIME_NAMESPACE).svc.cluster.local."`
    /// and expects a hostname; without expansion the container receives the
    /// literal text and fails at whatever it hands the string to — far from
    /// the kubelet, with nothing naming it. Measured on rio 2026-09-15: Flux's
    /// kustomize-controller logged
    /// `Get "http://source-controller.$(RUNTIME_NAMESPACE).svc.cluster.local./…"`
    /// and could not fetch a single artifact, so nothing was ever applied.
    ///
    /// Upstream's three rules, all load-bearing:
    ///   * only variables defined EARLIER in the list are visible — the caller
    ///     passes the map accumulated so far, so order is the semantics;
    ///   * `$$` escapes to a single literal `$`;
    ///   * an UNRESOLVABLE `$(VAR)` is left exactly as written, never blanked.
    ///     Blanking would turn a typo into a silently-empty hostname, which is
    ///     strictly harder to debug than the unexpanded text.
    #[must_use]
    fn expand_env_refs(raw: &str, defined: &BTreeMap<String, String>) -> String {
        let bytes = raw.as_bytes();
        let mut out = String::with_capacity(raw.len());
        let mut i = 0usize;
        while i < bytes.len() {
            if bytes[i] != b'$' {
                // Push the whole UTF-8 character, not the byte: indexing by
                // byte and emitting bytes would split a multi-byte codepoint.
                let ch = raw[i..].chars().next().unwrap_or('$');
                out.push(ch);
                i += ch.len_utf8();
                continue;
            }
            match bytes.get(i + 1) {
                Some(b'$') => {
                    out.push('$');
                    i += 2;
                }
                Some(b'(') => {
                    if let Some(close) = raw[i + 2..].find(')') {
                        let name = &raw[i + 2..i + 2 + close];
                        if let Some(v) = defined.get(name) {
                            out.push_str(v);
                        } else {
                            // Unresolvable: emit verbatim, including the
                            // delimiters, so the operator sees what was asked
                            // for rather than an empty string.
                            out.push_str(&raw[i..i + 3 + close]);
                        }
                        i += 3 + close;
                    } else {
                        // No closing paren — not a reference at all.
                        out.push('$');
                        i += 1;
                    }
                }
                _ => {
                    out.push('$');
                    i += 1;
                }
            }
        }
        out
    }

    fn resolve_env_entry(
        namespace: &str,
        pod_name: &str,
        pod: &Value,
        // The container this env entry belongs to. Needed because
        // `resourceFieldRef.containerName` is OPTIONAL and defaults to the
        // enclosing container — without it, an omitted name has no referent.
        container_name: &str,
        entry: &Value,
        sources: &BTreeMap<(String, String), Value>,
    ) -> Result<(String, String), KubeletError> {
        let invalid = |reason: String| KubeletError::InvalidPod {
            pod: format!("{namespace}/{pod_name}"),
            reason,
        };

        let key = entry
            .get("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| invalid("env entry has no name".to_string()))?
            .to_string();

        // A literal value wins, exactly as upstream.
        if let Some(v) = entry.get("value") {
            let v = v
                .as_str()
                .ok_or_else(|| invalid(format!("env {key}: value is not a string")))?;
            return Ok((key, v.to_string()));
        }

        let Some(from) = entry.get("valueFrom") else {
            // `{name: FOO}` with neither value nor valueFrom is the empty
            // string upstream, not an error and not an omission.
            return Ok((key, String::new()));
        };

        if let Some(field_ref) = from.get("fieldRef") {
            let path = field_ref
                .get("fieldPath")
                .and_then(|p| p.as_str())
                .ok_or_else(|| invalid(format!("env {key}: fieldRef has no fieldPath")))?;

            let resolved = match path {
                "metadata.name" => Some(pod_name.to_string()),
                "metadata.namespace" => Some(namespace.to_string()),
                "metadata.uid" => pod
                    .pointer("/metadata/uid")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                "spec.nodeName" => pod
                    .pointer("/spec/nodeName")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                "spec.serviceAccountName" => pod
                    .pointer("/spec/serviceAccountName")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                "status.podIP" => pod
                    .pointer("/status/podIP")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                "status.hostIP" => pod
                    .pointer("/status/hostIP")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                other => {
                    return Err(invalid(format!(
                        "env {key}: unsupported fieldRef path {other:?} \
                         (supported: metadata.name, metadata.namespace, metadata.uid, \
                         spec.nodeName, spec.serviceAccountName, status.podIP, status.hostIP)"
                    )));
                }
            };

            // A KNOWN path whose value is not yet populated (status.podIP
            // before the sandbox exists) resolves to empty rather than
            // failing — upstream does the same, and refusing here would
            // make a legal pod permanently unadmittable on a timing detail.
            return Ok((key, resolved.unwrap_or_default()));
        }

        // Secret / ConfigMap lookups — the source object was pre-fetched by
        // the caller into `sources`; here it's purely a keyed read + decoding.
        for (kind, kubekind) in [("secretKeyRef", "Secret"), ("configMapKeyRef", "ConfigMap")] {
            let Some(krf) = from.get(kind) else {
                continue;
            };
            let ref_name = krf
                .get("name")
                .and_then(|n| n.as_str())
                .ok_or_else(|| invalid(format!("env {key}: valueFrom.{kind}.name missing")))?;
            let ref_key = krf
                .get("key")
                .and_then(|n| n.as_str())
                .ok_or_else(|| invalid(format!("env {key}: valueFrom.{kind}.key missing")))?;
            let optional = krf
                .get("optional")
                .and_then(|o| o.as_bool())
                .unwrap_or(false);

            let Some(obj) = sources.get(&(kubekind.to_string(), ref_name.to_string())) else {
                if optional {
                    return Ok((key, String::new()));
                }
                return Err(invalid(format!(
                    "env {key}: {kubekind} {namespace}/{ref_name} not found"
                )));
            };

            let val = if kubekind == "Secret" {
                let enc = obj
                    .pointer(&format!("/data/{ref_key}"))
                    .and_then(|v| v.as_str());
                let Some(enc) = enc else {
                    if optional {
                        return Ok((key, String::new()));
                    }
                    return Err(invalid(format!(
                        "env {key}: Secret {namespace}/{ref_name} has no key {ref_key}"
                    )));
                };
                use base64::Engine as _;
                match base64::engine::general_purpose::STANDARD.decode(enc) {
                    Ok(bytes) => String::from_utf8(bytes).map_err(|e| {
                        invalid(format!(
                            "env {key}: Secret {namespace}/{ref_name}/{ref_key} not utf-8: {e}"
                        ))
                    })?,
                    Err(e) => {
                        return Err(invalid(format!(
                            "env {key}: Secret {namespace}/{ref_name}/{ref_key} not base64: {e}"
                        )));
                    }
                }
            } else {
                let Some(s) = obj
                    .pointer(&format!("/data/{ref_key}"))
                    .and_then(|v| v.as_str())
                else {
                    if optional {
                        return Ok((key, String::new()));
                    }
                    return Err(invalid(format!(
                        "env {key}: ConfigMap {namespace}/{ref_name} has no key {ref_key}"
                    )));
                };
                s.to_string()
            };
            return Ok((key, val));
        }

        // ── valueFrom.resourceFieldRef — the DOWNWARD API for resources ────
        //
        // ★ WHY THIS HAD TO EXIST BEFORE FLUX COULD RUN AT ALL. Flux's three
        // controllers each set `GOMEMLIMIT` from `resourceFieldRef`
        // (limits.memory). Refusing it is the right call — a Go runtime told
        // the wrong memory limit misbehaves silently — but it meant every Flux
        // pod was rejected at admission, and the kubelet retried each one
        // EVERY TICK. Measured on rio 2026-09-15: 273 `invalid manifest` warns
        // in 3 minutes, engenho pinned at ~126% CPU, node readiness flapping to
        // Unknown, and a single Secret write taking 70s — all downstream of
        // three pods that could never be admitted.
        //
        // Semantics follow upstream exactly:
        //   * `containerName` omitted ⇒ THIS container (hence `container`).
        //   * `divisor` defaults to 1; the value is the resource quantity
        //     divided by it, rounded UP (upstream uses ceiling), rendered as a
        //     bare integer.
        //   * A `limits.*` reference with no limit set falls back to the
        //     NODE's allocatable in upstream. We do not have that here, so we
        //     refuse by name rather than substituting 0 — a Go runtime handed
        //     `GOMEMLIMIT=0` would thrash immediately, which is precisely the
        //     silent-misconfiguration this function exists to prevent.
        if let Some(rref) = from.get("resourceFieldRef") {
            let resource = rref
                .get("resource")
                .and_then(|r| r.as_str())
                .ok_or_else(|| invalid(format!("env {key}: resourceFieldRef has no resource")))?;

            // Which container's resources? Default is the enclosing one.
            let target = rref
                .get("containerName")
                .and_then(|c| c.as_str())
                .unwrap_or(container_name);
            let cspec = Self::find_container_spec(pod, target).ok_or_else(|| {
                invalid(format!(
                    "env {key}: resourceFieldRef names container {target}, which this pod \
                     does not declare"
                ))
            })?;

            // "limits.memory" → ("limits", "memory")
            let (bucket, field) = resource.split_once('.').ok_or_else(|| {
                invalid(format!(
                    "env {key}: resourceFieldRef resource {resource:?} is not \
                     <limits|requests>.<resource>"
                ))
            })?;
            if bucket != "limits" && bucket != "requests" {
                return Err(invalid(format!(
                    "env {key}: resourceFieldRef resource {resource:?} must start with \
                     limits. or requests."
                )));
            }

            // ★ THE UNIT IS WHAT MAKES THE DIVISOR DEFAULT CORRECT. Upstream
            // reports cpu in CORES and memory in BYTES, with `divisor`
            // defaulting to the quantity "1". Reading BOTH the resource and
            // the divisor in the same canonical unit makes that fall out:
            // "1" as MilliCores is 1000 (one core), "1" as Bytes is 1. So
            // 500m cpu / default → ceil(500/1000) = 1 core, and 1Gi memory /
            // default → 1073741824 bytes, both matching upstream without a
            // special case per resource.
            let unit = if field == "cpu" {
                crate::backend::QuantityUnit::MilliCores
            } else {
                crate::backend::QuantityUnit::Bytes
            };

            let bucket_map = cspec.pointer(&format!("/resources/{bucket}"));
            let amount = match crate::backend::ResourceBound::read(bucket_map, field, unit) {
                crate::backend::ResourceBound::Set(n) => n,
                crate::backend::ResourceBound::Unset => {
                    return Err(invalid(format!(
                        "env {key}: resourceFieldRef wants {resource} of container {target}, \
                         but it declares none — refusing rather than substituting a value the \
                         workload would act on"
                    )));
                }
                crate::backend::ResourceBound::Unparseable(s) => {
                    return Err(invalid(format!(
                        "env {key}: {resource} of container {target} is {s:?}, which is not a \
                         quantity this kubelet can parse"
                    )));
                }
            };

            let divisor_raw = rref
                .get("divisor")
                .and_then(|d| match d {
                    Value::String(s) if !s.is_empty() => Some(s.clone()),
                    Value::Number(n) => Some(n.to_string()),
                    _ => None,
                })
                .unwrap_or_else(|| "1".to_string());
            let divisor = match crate::backend::ResourceBound::read(
                Some(&serde_json::json!({ "d": divisor_raw })),
                "d",
                unit,
            ) {
                crate::backend::ResourceBound::Set(n) if n > 0 => n,
                _ => {
                    return Err(invalid(format!(
                        "env {key}: resourceFieldRef divisor {divisor_raw:?} is not a positive \
                         quantity"
                    )));
                }
            };

            // Upstream rounds UP, so a quantity smaller than the divisor
            // reports 1 rather than 0 — a workload dividing by this must never
            // see a zero it did not ask for.
            let scaled = amount.div_euclid(divisor) + i64::from(amount.rem_euclid(divisor) != 0);
            return Ok((key, scaled.to_string()));
        }

        // Any remaining unknown source class stays unsupported, loudly.
        Err(invalid(format!(
            "env {key}: valueFrom.unknown source is not supported yet — refusing rather than \
             starting the container without it"
        )))
    }

    /// The container object named `want` within `pod`, searched across
    /// `containers` and `initContainers` — `resourceFieldRef.containerName`
    /// may legally name either.
    fn find_container_spec<'p>(pod: &'p Value, want: &str) -> Option<&'p Value> {
        for key in ["containers", "initContainers"] {
            if let Some(arr) = pod
                .pointer(&format!("/spec/{key}"))
                .and_then(|c| c.as_array())
            {
                for c in arr {
                    if c.get("name").and_then(|n| n.as_str()) == Some(want) {
                        return Some(c);
                    }
                }
            }
        }
        None
    }

    fn extract_container_specs(
        namespace: &str,
        name: &str,
        pod: &Value,
        spec_key: &str,
        optional: bool,
        sources: &BTreeMap<(String, String), Value>,
    ) -> Result<Vec<(String, ContainerSpec)>, KubeletError> {
        let containers = match pod
            .get("spec")
            .and_then(|s| s.get(spec_key))
            .and_then(|c| c.as_array())
        {
            Some(c) => c,
            None if optional => return Ok(Vec::new()),
            None => {
                return Err(KubeletError::InvalidPod {
                    pod: format!("{namespace}/{name}"),
                    reason: format!("spec.{spec_key} missing"),
                });
            }
        };
        if containers.is_empty() {
            if optional {
                return Ok(Vec::new());
            }
            return Err(KubeletError::InvalidPod {
                pod: format!("{namespace}/{name}"),
                reason: format!("spec.{spec_key} is empty"),
            });
        }
        let mut out = Vec::with_capacity(containers.len());
        for (i, c) in containers.iter().enumerate() {
            // Container logical name: spec.<key>[i].name, else a positional
            // fallback (matches the "main"/index shape so status names
            // round-trip).
            let cname = c
                .get("name")
                .and_then(|n| n.as_str())
                .map(String::from)
                .unwrap_or_else(|| {
                    if i == 0 {
                        "main".to_string()
                    } else {
                        format!("container-{i}")
                    }
                });
            let image = c
                .get("image")
                .and_then(|im| im.as_str())
                .ok_or_else(|| KubeletError::InvalidPod {
                    pod: format!("{namespace}/{name}"),
                    reason: format!("spec.{spec_key}[{i}].image missing"),
                })?
                .to_string();
            // Read the container's OWN imagePullPolicy. Nothing in the tree
            // read this field before, so every container inherited the
            // backend-wide `Never` and any uncached image failed as
            // "image not known".
            let pull_policy = crate::backend::PullPolicy::resolve(
                c.get("imagePullPolicy").and_then(|p| p.as_str()),
                &image,
            );
            let env = match c.get("env").and_then(|e| e.as_array()) {
                Some(arr) => {
                    let mut map = BTreeMap::new();
                    for entry in arr {
                        let (k, v) =
                            Self::resolve_env_entry(namespace, name, pod, &cname, entry, sources)?;
                        // `$(VAR)` expansion applies to a literal `value` only.
                        // A `valueFrom` result is the value of another object
                        // and upstream does NOT re-scan it — expanding there
                        // would let a ConfigMap's contents reach into the
                        // container's environment, which is a different and
                        // much larger promise than the one Kubernetes makes.
                        let v = if entry.get("value").is_some() {
                            Self::expand_env_refs(&v, &map)
                        } else {
                            v
                        };
                        map.insert(k, v);
                    }
                    map
                }
                None => BTreeMap::new(),
            };
            // command = entrypoint override; args = appended arguments
            // (K8s semantics). The container's run argv is command ++ args.
            //
            // `$(VAR)` is expanded here too, against the container's FULLY
            // resolved environment — upstream applies the same substitution to
            // argv as to env values, and a manifest that writes
            // `--webhook-addr=$(POD_IP):9443` is relying on it. Unlike env,
            // every variable is visible: argv is processed after the whole
            // environment exists, so there is no ordering rule to honour.
            let str_array = |key: &str| -> Vec<String> {
                c.get(key)
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|x| x.as_str())
                            .map(|x| Self::expand_env_refs(x, &env))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            };
            let mut command = str_array("command");
            command.extend(str_array("args"));
            // Backend (podman --name) handle. App containers: <ns>_<pod>_<cname>.
            // Init containers: <ns>_<pod>_init-<cname> (the disambiguating
            // prefix — see pod_to_init_container_specs).
            let backend_name = if optional {
                format!("{namespace}_{name}_init-{cname}")
            } else {
                format!("{namespace}_{name}_{cname}")
            };
            out.push((
                cname.clone(),
                ContainerSpec {
                    name: backend_name,
                    image,
                    env,
                    command,
                    pull_policy: Some(pull_policy),
                    // Service-name DNS aliases are computed once per pod in
                    // `start_bound_pod` and assigned onto each spec there.
                    network_aliases: Vec::new(),
                    // Volume mounts are resolved once per pod in
                    // `start_bound_pod` (after alias compute, before the start
                    // loop) and stamped onto each spec there. Empty here →
                    // a no-volume pod produces `mounts: vec![]` → identical
                    // argv to before the kubelet-volumes brick.
                    mounts: Vec::new(),
                    // ★ READ, not defaulted. Until 2026-09-06 this field did
                    // not exist and `securityContext` was never consulted, so a
                    // Pod asking for runAsNonRoot / cap-drop-ALL /
                    // readOnlyRootFilesystem got none of it and nothing said so.
                    confinement: crate::backend::Confinement::from_pod_json(
                        c.get("securityContext"),
                        &format!("{namespace}/{name}"),
                        &cname,
                    )?,
                    // Extra /etc/hosts entries; the backend fills in
                    // host.containers.internal on Linux native when needed.
                    host_add: Vec::new(),
                    // ★ READ, not defaulted — the same class of miss as
                    // `confinement` above, and measured on 2026-09-14: nothing
                    // in the tree consulted `resources` at all, so every
                    // container engenho has ever run was unlimited, while
                    // engenho-scheduler packed nodes by `allocatable − Σ
                    // requests`. The scheduler did arithmetic about a bound the
                    // node declined to enforce.
                    resources: crate::backend::Resources::from_container_json(c),
                    // ★ The identity, carried rather than fused. The four
                    // components are all in scope right here and were being
                    // thrown away into `backend_name`'s lossy join — which the
                    // note at the top of this file forbids reversing, and which
                    // is exactly what a CRI `PodSandboxMetadata` needs.
                    // ★ Read for INIT containers only. A present
                    // `restartPolicy` on an APP container is rejected — upstream
                    // forbids it, and accepting it would silently imply a
                    // semantic engenho does not implement.
                    init_kind: {
                        let raw = c.get("restartPolicy").and_then(Value::as_str);
                        if optional {
                            crate::lifecycle::InitKind::from_spec_str(raw).map_err(|e| {
                                KubeletError::InvalidPod {
                                    pod: format!("{namespace}/{name}"),
                                    reason: format!("initContainers[{cname}]: {e}"),
                                }
                            })?
                        } else {
                            if raw.is_some() {
                                return Err(KubeletError::InvalidPod {
                                    pod: format!("{namespace}/{name}"),
                                    reason: format!(
                                        "containers[{cname}]: restartPolicy is not \
                                         permitted on an app container"
                                    ),
                                });
                            }
                            crate::lifecycle::InitKind::Regular
                        }
                    },
                    pod: crate::backend::PodIdentity {
                        namespace: namespace.to_string(),
                        name: name.to_string(),
                        uid: pod
                            .pointer("/metadata/uid")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        container_name: cname.clone(),
                        init: optional,
                    },
                },
            ));
        }
        Ok(out)
    }

    /// Parse the three probes (`livenessProbe`/`readinessProbe`/
    /// `startupProbe`) of a single `spec.containers[i]` JSON object into a
    /// [`ContainerProbeState`], resolving named ports against the container's
    /// own `ports[]`. The probe specs are parsed; their [`ProbeRuntime`]
    /// counters are stamped from `now` (the container's start instant).
    ///
    /// # Errors
    ///
    /// Propagates a [`ProbeParseError`](crate::probe::ProbeParseError) (mapped
    /// to a typed [`KubeletError::InvalidPod`]) for a no-handler / grpc /
    /// unresolved-port probe — the pod is skipped, NEVER a fake pass.
    fn parse_container_probes(
        container: &Value,
        pod_label: &str,
        now: Instant,
    ) -> Result<ContainerProbeState, KubeletError> {
        // Resolve the container's named ports once for port resolution.
        let ports: Vec<(String, u16)> = container
            .get("ports")
            .and_then(|p| p.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|p| {
                        let name = p.get("name").and_then(|n| n.as_str())?.to_string();
                        let number = p.get("containerPort").and_then(serde_json::Value::as_i64)?;
                        u16::try_from(number).ok().map(|n| (name, n))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let parse_one = |field: &str,
                         kind: ProbeKind|
         -> Result<Option<(ProbeSpec, ProbeRuntime)>, KubeletError> {
            match container.get(field) {
                None => Ok(None),
                Some(probe) => {
                    let spec = ProbeSpec::from_k8s(kind, probe, &ports).map_err(|e| {
                        KubeletError::InvalidPod {
                            pod: pod_label.to_string(),
                            reason: format!("{field}: {e}"),
                        }
                    })?;
                    Ok(Some((spec, ProbeRuntime::new(now))))
                }
            }
        };

        Ok(ContainerProbeState {
            liveness: parse_one("livenessProbe", ProbeKind::Liveness)?,
            readiness: parse_one("readinessProbe", ProbeKind::Readiness)?,
            startup: parse_one("startupProbe", ProbeKind::Startup)?,
        })
    }

    /// Look up a single `spec.containers[i]` JSON object by its logical name
    /// (`spec.containers[i].name`, with the same positional fallback
    /// `pod_to_container_specs` uses). Returns the raw `Value` so probe parsing
    /// reads from the same JSON-driven source as the rest of the kubelet.
    fn container_json<'a>(pod: &'a Value, cname: &str) -> Option<&'a Value> {
        let containers = pod.get("spec")?.get("containers")?.as_array()?;
        containers
            .iter()
            .enumerate()
            .find(|(i, c)| {
                let name = c
                    .get("name")
                    .and_then(|n| n.as_str())
                    .map(String::from)
                    .unwrap_or_else(|| {
                        if *i == 0 {
                            "main".to_string()
                        } else {
                            format!("container-{i}")
                        }
                    });
                name == cname
            })
            .map(|(_, c)| c)
    }

    /// Read the Pod's `spec.restartPolicy` into the typed [`RestartPolicy`].
    /// Absent → the K8s default [`RestartPolicy::Always`].
    fn pod_restart_policy(pod: &Value) -> RestartPolicy {
        RestartPolicy::from_spec_str(
            pod.get("spec")
                .and_then(|s| s.get("restartPolicy"))
                .and_then(|p| p.as_str()),
        )
    }

    /// Compute the Service-name DNS aliases a Pod earns by matching
    /// Services' selectors (M0.3 cluster-DNS brick).
    ///
    /// For each `(svc_key, svc_value)` in `services`, if the Service has a
    /// non-empty `spec.selector` AND the Pod's `metadata.labels` satisfy it
    /// (using the EXACT same predicate the [`EndpointsController`] uses —
    /// [`service_selector`] + [`matches_labels`], reused not reimplemented),
    /// the Pod earns three aliases for that Service's name:
    ///
    ///   * `<service>`
    ///   * `<service>.<namespace>`
    ///   * `<service>.<namespace>.svc.<cluster_domain>`
    ///
    /// These are the three Service-name forms aardvark-dns resolves on a
    /// user-defined network, mirroring K8s headless-Service DNS. The
    /// `namespace` is the POD's namespace (NOT hard-coded `default`); the
    /// `cluster_domain` is the cluster's DNS suffix (default
    /// [`DEFAULT_CLUSTER_DOMAIN`] = `cluster.local`).
    ///
    /// The result is sorted + deduped for determinism (multiple matching
    /// Services contribute their own three aliases; aardvark-dns accepts an
    /// alias appearing once per container). A Pod matching zero Services
    /// (no labels, or no selector matched) earns zero aliases — it still
    /// runs, just unaddressable by Service name (preserving today's
    /// behavior). A Service with an empty/absent selector contributes
    /// nothing ([`matches_labels`] returns false on an empty selector, per
    /// K8s convention).
    ///
    /// Pure: takes the already-listed Services (no store, no podman) so it
    /// is unit-testable. The store-using LIST lives in `start_bound_pod`.
    ///
    /// [`EndpointsController`]: engenho_controllers::EndpointsController
    #[must_use]
    /// `/etc/hosts` entries mapping every Service's DNS names to its
    /// **ClusterIP**, in podman's `"name:ip"` `--add-host` shape.
    ///
    /// ## Why this exists alongside `service_aliases_for_pod`
    ///
    /// The alias path registers a pod's OWN Service names with aardvark-dns,
    /// so a name resolves to the backing **pod IP**. That is correct only
    /// while `port == targetPort`. Measured on rio 2026-09-15: Flux's
    /// source-controller Service is `port: 80` → `targetPort: http` (9090),
    /// so a client resolving the name got `10.89.0.33` and connected to
    /// **:80**, where nothing listens — the pod answered 200 on 9090 the
    /// whole time. kustomize-controller could not fetch a single artifact.
    ///
    /// `/etc/hosts` is consulted BEFORE DNS, so mapping the same names to the
    /// ClusterIP puts the request back on the Service VIP, where the iptables
    /// datapath performs the port translation the Service declares. That is
    /// what makes a `port != targetPort` Service work at all.
    ///
    /// Headless Services (no ClusterIP, or the literal `"None"`) are
    /// deliberately SKIPPED — their contract is "resolve to the pod IPs",
    /// which is exactly what the alias path already provides. Overriding them
    /// here would break the one case the aliases get right.
    ///
    /// Resolution follows Kubernetes' search shape: a Service in the pod's own
    /// namespace is reachable by all three forms, one in another namespace
    /// only by its qualified forms.
    ///
    /// ## Start-time only — the same accepted limitation as the aliases
    ///
    /// `--add-host` is a `podman run` flag, so a Service created after a pod
    /// starts is not visible to it until the pod is recreated. The named
    /// destination is unchanged and is the real fix: the `engenho-dns`
    /// authority (`engenho-controllers/src/dns.rs` already computes exactly
    /// these `fqdn → clusterIP` records and is not yet served to pods).
    /// Everything a container needs to resolve Service names, computed in ONE
    /// place: the aardvark aliases for the Services that select this pod, and
    /// the ClusterIP `/etc/hosts` map for the Services it consumes.
    ///
    /// ★ Why this is a single function. The two halves used to be computed
    /// separately at each start path, and there are THREE start paths — first
    /// start, init containers, and restart-after-exit. The ClusterIP map was
    /// wired into two of them. Measured on rio 2026-09-15: kustomize-controller
    /// exited once while waiting on its leader lease, came back through the
    /// RESTART path with an empty `host_add`, resolved source-controller to
    /// its pod IP on :80 and got `connection refused` — while
    /// notification-controller, started through the normal path, had the map
    /// and worked. A partial guard that read as a complete one. Returning both
    /// halves together means a path that sets one cannot forget the other.
    async fn service_name_resolution(
        &self,
        value: &Value,
        namespace: &str,
    ) -> (Vec<String>, Vec<String>) {
        let own_ns = self.store.list("", "v1", "Service", Some(namespace)).await;
        let aliases =
            Self::service_aliases_for_pod(value, namespace, &own_ns, DEFAULT_CLUSTER_DOMAIN);
        // Listed across ALL namespaces: a pod resolves Services it CONSUMES,
        // which are frequently not its own, whereas the aliases are about
        // Services that SELECT this pod. Two questions, two lists.
        let all = self.store.list("", "v1", "Service", None).await;
        let hosts = Self::service_cluster_ip_hosts(namespace, &all, DEFAULT_CLUSTER_DOMAIN);
        (aliases, hosts)
    }

    fn service_cluster_ip_hosts(
        namespace: &str,
        services: &[(ResourceKey, Value)],
        cluster_domain: &str,
    ) -> Vec<String> {
        let mut hosts: Vec<String> = Vec::new();
        for (svc_key, svc_value) in services {
            let Some(cluster_ip) = svc_value
                .get("spec")
                .and_then(|s| s.get("clusterIP"))
                .and_then(Value::as_str)
            else {
                continue;
            };
            // "None" is how a headless Service spells "I have no VIP".
            if cluster_ip.is_empty() || cluster_ip == "None" {
                continue;
            }
            let Some(svc_name) = svc_value
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(Value::as_str)
            else {
                continue;
            };
            let svc_ns = svc_key.namespace.as_deref().unwrap_or("default");

            // Qualified forms are reachable from any namespace.
            hosts.push([svc_name, ".", svc_ns, ":", cluster_ip].concat());
            hosts.push(
                [
                    svc_name,
                    ".",
                    svc_ns,
                    ".svc.",
                    cluster_domain,
                    ":",
                    cluster_ip,
                ]
                .concat(),
            );
            // The bare name resolves only within the Service's own namespace.
            if svc_ns == namespace {
                hosts.push([svc_name, ":", cluster_ip].concat());
            }
        }
        hosts.sort();
        hosts.dedup();
        hosts
    }

    fn service_aliases_for_pod(
        pod: &Value,
        namespace: &str,
        services: &[(ResourceKey, Value)],
        cluster_domain: &str,
    ) -> Vec<String> {
        let mut aliases: Vec<String> = Vec::new();
        for (_svc_key, svc_value) in services {
            // Same selector semantics as EndpointsController::tick: a
            // present selector that the pod's labels satisfy. An empty /
            // absent selector → matches_labels false → no alias (correct
            // K8s behavior — empty selector matches nothing here).
            let Some(selector) = service_selector(svc_value) else {
                continue;
            };
            if !matches_labels(pod, selector) {
                continue;
            }
            let Some(svc_name) = svc_value
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
            else {
                continue;
            };
            aliases.push(svc_name.to_string());
            aliases.push(format!("{svc_name}.{namespace}"));
            aliases.push(format!("{svc_name}.{namespace}.svc.{cluster_domain}"));
        }
        aliases.sort();
        aliases.dedup();
        aliases
    }

    /// `true` iff the Pod has already reached a terminal phase
    /// (`Succeeded` or `Failed`). A terminal Pod is never (re)started in
    /// the start phase, and a terminal Pod we've forgotten locally (after
    /// a process restart) is not blindly restarted. Per item-9 scope
    /// (restartPolicy:Never), terminal Pods stay terminal.
    fn pod_already_terminal(pod: &Value) -> bool {
        matches!(
            pod.get("status")
                .and_then(|s| s.get("phase"))
                .and_then(|p| p.as_str()),
            Some("Succeeded" | "Failed")
        )
    }

    fn pod_is_bound_to(pod_value: &Value, node_name: &str) -> bool {
        pod_value
            .get("spec")
            .and_then(|s| s.get("nodeName"))
            .and_then(|n| n.as_str())
            .map(|n| n == node_name)
            .unwrap_or(false)
    }

    /// Render a single [`ContainerStatusOut`] into its
    /// `status.containerStatuses[]` JSON entry. The typed
    /// [`ContainerState`] enum is the render surface; `json!` inside this impl
    /// is the allowed TYPED EMISSION site (per ★★ TYPED EMISSION rule #1).
    fn render_container_status(cs: &ContainerStatusOut) -> Value {
        let state = match &cs.state {
            ContainerState::Waiting { reason } => json!({ "waiting": { "reason": reason } }),
            ContainerState::Running => json!({ "running": {} }),
            ContainerState::Terminated { exit_code, reason } => json!({
                "terminated": { "exitCode": exit_code, "reason": reason }
            }),
        };
        let mut entry = json!({
            "name": cs.name,
            "ready": cs.ready,
            "state": state,
            "restartCount": cs.restart_count,
        });
        if let Some(id) = &cs.container_id {
            entry["containerID"] = Value::String(id.clone());
        }
        entry
    }

    /// Build the desired Pod `status` from the typed
    /// `(PodPhase, Vec<ContainerStatusOut>)` fold output + the pod's IP.
    ///
    /// The pod-level conditions are the standard K8s pair:
    ///   * `ContainersReady = phase==Running AND all containerStatuses[].ready`
    ///     — the readiness AND across containers. With the probe brick,
    ///     `containerStatuses[].ready` sources from the readiness-probe verdict
    ///     (or `is_running` when there is no readiness probe — behavior-
    ///     preserving).
    ///   * `Ready = ContainersReady AND all readinessGates`. No readinessGates
    ///     today ⇒ `Ready == ContainersReady` whenever Running. Both are
    ///     `False` while the phase is not `Running`.
    ///
    /// Emitting BOTH conditions (deterministically ordered: `ContainersReady`
    /// then `Ready`) on EVERY write keeps the Running↔Running steady state
    /// byte-identical → `write_status_cas` yields `NoChange` → no watch storm.
    /// A probe that legitimately flips ready True→False→True produces a changed
    /// status (the signal kubectl shows); steady-passing produces `NoChange`.
    ///
    /// `pod_ip` is retained for a terminated Pod too (K8s keeps the last IP),
    /// which ALSO keeps the field set stable across Running→terminal so the
    /// idempotent-skip in [`write_status_cas`] yields `NoChange` at steady
    /// state (no hot loop, no watch storm).
    ///
    /// NO-INIT path: this 3-arg form is the behavior-preserving entry for a pod
    /// with zero init containers — it delegates to
    /// [`Self::build_pod_status_with_init`] with `has_init = false`, which emits
    /// NO `Initialized` condition + NO `initContainerStatuses`, so the rendered
    /// status is byte-identical to before the init-container brick. The init
    /// path uses the with-init form directly.
    fn build_pod_status(
        live: &Value,
        phase: engenho_types::curated_enums::PodPhase,
        statuses: &[ContainerStatusOut],
        pod_ip: Option<&str>,
    ) -> Value {
        Self::build_pod_status_with_init(live, phase, &[], statuses, pod_ip, true, false)
    }

    /// Build the desired Pod `status`, optionally carrying init-container state.
    ///
    /// When `has_init == false` the output is byte-identical to the pre-init
    /// render (no `Initialized` condition, no `initContainerStatuses`) — the
    /// no-init behavior-preserving guarantee. When `has_init == true` the
    /// `Initialized` condition (`True`/`False` from `initialized`) is APPENDED
    /// as the THIRD condition (after `ContainersReady`, `Ready`) and an
    /// `initContainerStatuses` array (rendered like `containerStatuses`) is
    /// added. Appending (not prepending) `Initialized` keeps the existing
    /// `conditions[0] = ContainersReady` / `conditions[1] = Ready` indices
    /// stable.
    ///
    /// `ContainersReady`/`Ready` fold over the APP `statuses` exactly as before
    /// — while init runs (phase Pending) both are `False`; once init completes
    /// and the app containers are up they become `True`. Init containers do NOT
    /// contribute to `ContainersReady` (K8s excludes them).
    fn build_pod_status_with_init(
        live: &Value,
        phase: engenho_types::curated_enums::PodPhase,
        init_statuses: &[ContainerStatusOut],
        statuses: &[ContainerStatusOut],
        pod_ip: Option<&str>,
        initialized: bool,
        has_init: bool,
    ) -> Value {
        use engenho_types::curated_enums::PodPhase;
        let phase_str = match phase {
            PodPhase::Pending => "Pending",
            PodPhase::Running => "Running",
            PodPhase::Succeeded => "Succeeded",
            PodPhase::Failed => "Failed",
            PodPhase::Unknown => "Unknown",
        };
        // ContainersReady = phase Running AND all (app) containers ready. A
        // non-running phase (Pending / terminal) is never ready. Init
        // containers are excluded (K8s does not count them toward readiness).
        let containers_ready =
            matches!(phase, PodPhase::Running) && statuses.iter().all(|c| c.ready);
        // Ready = ContainersReady AND all readinessGates (none today ⇒ mirrors
        // ContainersReady).
        let ready = containers_ready;
        let status_str = |b: bool| if b { "True" } else { "False" };
        let container_statuses: Vec<Value> =
            statuses.iter().map(Self::render_container_status).collect();
        // ── ★ CONDITIONS ARE MERGED BY TYPE, NOT REPLACED ────────────────
        // This array is shipped through an RFC 7396 JSON Merge Patch, whose
        // defined semantics are "arrays replace whole". So rendering a fresh
        // 2-3 element array DELETED every condition the kubelet does not
        // author — and engenho's scheduler writes exactly one:
        // `PodScheduled=False/Unschedulable`. The first kubelet status write
        // silently removed it, turning "this pod could not be placed, here is
        // why" into no condition at all.
        //
        // The fix is read-modify-write HERE rather than strategic-merge in the
        // store, for a reason that is not stylistic: `write_status_cas`
        // compares the rendered `desired` against the stored `live` for exact
        // whole-JSON equality to decide `NoChange`. Under a store-side merge
        // the STORE would decide the merged array, `desired` would never equal
        // `live`, `NoChange` would never fire, and the kubelet would write on
        // every tick — waking every Pod-subscribed controller forever. Computed
        // FROM `live`, equality holds by construction.
        //
        // Upstream's own list, `kubetypes.PodConditionsByKubelet`.
        const KUBELET_OWNED: [&str; 4] =
            ["PodScheduled", "Initialized", "Ready", "ContainersReady"];
        // (a) Everything we do NOT own, in its existing order — readiness
        // gates, DisruptionTarget, anything a future controller adds.
        let mut conditions: Vec<Value> = live
            .pointer("/status/conditions")
            .and_then(Value::as_array)
            .map(|cs| {
                cs.iter()
                    .filter(|c| {
                        !c.get("type")
                            .and_then(Value::as_str)
                            .is_some_and(|t| KUBELET_OWNED.contains(&t))
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        // (b) Ours, in a fixed order so the render is stable across writes
        // (stable render ⇒ NoChange at steady state ⇒ no watch storm).
        conditions.push(json!({
            "type": "ContainersReady",
            "status": status_str(containers_ready),
        }));
        conditions.push(json!({
            "type": "Ready",
            "status": status_str(ready),
        }));
        // Initialized ONLY for a pod with init containers. A no-init pod omits
        // it entirely (byte-identical pre-init render).
        if has_init {
            conditions.push(json!({
                "type": "Initialized",
                "status": status_str(initialized),
            }));
        }
        // ★ PodScheduled=True, ALWAYS. The kubelet only reconciles pods whose
        // `spec.nodeName` is this node, so by the time this renders, being
        // scheduled is a tautology — which is exactly upstream's reasoning for
        // the kubelet owning this condition. Before this, `PodScheduled=True`
        // appeared NOWHERE in engenho: the scheduler writes the condition only
        // on the failure path, so a successfully-placed pod simply never had
        // one. Asserting it here is also what TRANSITIONS a stale
        // `False/Unschedulable` rather than deleting it.
        conditions.push(json!({
            "type": "PodScheduled",
            "status": "True",
        }));
        let mut status = json!({
            "phase": phase_str,
            "conditions": conditions,
            "containerStatuses": container_statuses,
        });
        // initContainerStatuses ONLY for an init-bearing pod.
        if has_init {
            let init_container_statuses: Vec<Value> = init_statuses
                .iter()
                .map(Self::render_container_status)
                .collect();
            status["initContainerStatuses"] = Value::Array(init_container_statuses);
        }
        if let Some(ip) = pod_ip {
            status["podIP"] = Value::String(ip.to_string());
            // `podIPs` is the plural upstream added for dual-stack and it is
            // what modern clients read; `podIP` is retained as the first entry.
            // Derived from the same value so `podIP == podIPs[0]` holds by
            // construction rather than by two writers agreeing.
            status["podIPs"] = json!([{ "ip": ip }]);
        }
        // ★ `startTime` is LATCHED, never minted per render. It records when
        // the kubelet first accepted the pod; re-stamping it every tick would
        // make "how long has this been running" always read zero — the same
        // defect as the Node condition's lastTransitionTime. It also breaks
        // `NoChange`: a value that differs on every render writes on every
        // tick.
        let start_time = live
            .pointer("/status/startTime")
            .and_then(Value::as_str)
            .map_or_else(engenho_types::time::now_rfc3339_utc, str::to_string);
        status["startTime"] = Value::String(start_time);
        status
    }
}

#[async_trait]
impl Controller for Kubelet {
    fn name(&self) -> &'static str {
        "kubelet"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        // ── NODE LEASE. Renewed FIRST, before any pod work, and this
        // ordering is the point: the lease says "this kubelet is alive",
        // and a kubelet that renews only after a slow reconcile reports
        // itself unhealthy precisely when it is busiest. Upstream renews on
        // its own cadence for the same reason.
        //
        // Renewal failure is logged, never fatal — a kubelet that stops
        // managing containers because it could not write a heartbeat has
        // turned an observability problem into an outage.
        self.renew_node_lease().await;
        // Read that heartbeat back and turn it into the Node's Ready
        // condition. Deliberately AFTER the renew and on EVERY tick, not on
        // the renew's 10s cadence: the interesting case is the one where the
        // renew above FAILED, and a consumer that only runs when the producer
        // succeeded can never observe that.
        self.publish_node_readiness().await;

        let pods = self.store.list("", "v1", "Pod", None).await;
        let mut report = ReconcileReport::default();

        // Bound set = Pods whose spec.nodeName == this node, keyed by the
        // typed ResourceKey (the unambiguous identity).
        let bound: BTreeMap<ResourceKey, Value> = pods
            .into_iter()
            .filter(|(_, p)| Self::pod_is_bound_to(p, &self.node_name))
            .collect();
        report.objects_examined = bound.len();

        // ── (A) Delete-cleanup: local entries no longer in the bound set ──
        // A Pod we started that's absent from the freshly-listed bound set
        // was hard-deleted or its spec.nodeName moved away → orphaned on
        // this node. stop THEN remove, then drop the local entry. The
        // store key is already gone for a delete; nothing to patch.
        let orphaned: Vec<(ResourceKey, LocalPod)> = {
            let local = self.local.lock().await;
            let live: BTreeSet<&ResourceKey> = bound.keys().collect();
            local
                .iter()
                .filter(|(key, _)| !live.contains(key))
                .map(|(key, lp)| (key.clone(), lp.clone()))
                .collect()
        };
        for (key, lp) in orphaned {
            // MULTI-CONTAINER: stop THEN remove EVERY container of the pod.
            // All-or-nothing: only drop the local entry if every container
            // cleaned up; otherwise retain it so the next tick retries the
            // stragglers (no silent leak).
            match self.cleanup_pod_containers(&key, &lp).await {
                Ok(()) => {
                    self.local.lock().await.remove(&key);
                    report.objects_changed += 1;
                    debug!(
                        pod = %key.label(),
                        containers = lp.containers.len(),
                        "kubelet cleaned up orphaned pod containers"
                    );
                }
                Err(e) => {
                    // Leave the local entry so the next tick retries — no
                    // silent leak.
                    warn!(
                        pod = %key.label(),
                        error = %e,
                        "kubelet cleanup failed; will retry next tick"
                    );
                    report.objects_skipped += 1;
                }
            }
        }

        // ── PROJECTED-TOKEN REFRESH. After cleanup so a pod on its way out
        // is not re-minted, and before the start/status work so a long-lived
        // pod's credential is renewed on the same tick that keeps it running.
        // Its own cadence gate, like the lease above; failures are logged
        // there and never reach this outcome.
        let sa = self.refresh_service_account_projections(&bound).await;
        if sa.refreshed > 0 || sa.failed > 0 {
            debug!(
                examined = sa.examined,
                refreshed = sa.refreshed,
                failed = sa.failed,
                "kubelet refreshed projected ServiceAccount tokens"
            );
        }

        // ── (B)+(C) Start + running-status reconciliation over bound set ──
        // `soonest_requeue` accumulates the smallest next-probe-due across all
        // bound containers, so the kubelet wakes on its OWN probe clock (a
        // one-shot Requeue) rather than only on Pod-watch events / the coarse
        // fallback. None = no probes anywhere = no Requeue = today's wake
        // behavior (the behavior-preserving guarantee for no-probe pods).
        let mut soonest_requeue: Option<Duration> = None;
        for (key, value) in &bound {
            // Membership decides start (B) vs poll (C); compute it under a
            // short lock to avoid holding it across the backend await.
            let local_entry = self.local.lock().await.get(key).cloned();

            match local_entry {
                None => {
                    // (B) Not started locally. Skip if already terminal
                    // (don't restart a Succeeded/Failed pod we've
                    // forgotten — restartPolicy:Never, item-9 scope).
                    if Self::pod_already_terminal(value) {
                        continue;
                    }
                    self.start_bound_pod(key, value, &mut report, &mut soonest_requeue)
                        .await?;
                }
                Some(lp) => {
                    // (C) Already started → poll + reconcile running status.
                    self.reconcile_running(key, value, &lp, &mut report, &mut soonest_requeue)
                        .await?;
                }
            }
        }

        if report.objects_changed > 0 {
            info!(
                node = %self.node_name,
                changed = report.objects_changed,
                examined = report.objects_examined,
                "kubelet tick"
            );
        }
        // Arm a one-shot re-tick at the soonest next-probe-due (clamped to the
        // 1s floor) so probes run on `periodSeconds`. A pod with NO probes
        // contributes nothing → soonest_requeue stays None → ReconcileResult
        // Done → same wake behavior as the pre-probe kubelet.
        let result = match soonest_requeue {
            Some(after) => ReconcileResult::Requeue(after.max(MIN_PROBE_REQUEUE)),
            None => ReconcileResult::Done,
        };
        Ok(ReconcileOutcome::new(report, result))
    }
}

impl Kubelet {
    /// stop THEN remove a container; idempotent on the backend (already
    /// stopped / not found are success). Stop-before-remove ordering is
    /// the invariant.
    async fn cleanup_container(&self, container_id: &str) -> Result<(), KubeletError> {
        self.backend.stop(container_id).await?;
        self.backend.remove(container_id).await?;
        Ok(())
    }

    /// MULTI-CONTAINER cleanup: stop THEN remove EVERY container of the pod,
    /// THEN reap each emptyDir named volume the pod created. Returns `Ok(())`
    /// only if all containers AND all emptyDir volumes cleaned up; the FIRST
    /// failure is surfaced (so the caller retains the local entry + retries).
    /// Each container's stop-before-remove ordering is preserved; volume
    /// removal happens AFTER all containers are gone (a volume still in use by
    /// a live container can't be removed). emptyDir-volume removal is
    /// idempotent (already-absent is success), so a retry after a partial
    /// failure converges.
    async fn cleanup_pod_containers(
        &self,
        key: &ResourceKey,
        lp: &LocalPod,
    ) -> Result<(), KubeletError> {
        // ── ★ APP CONTAINERS FIRST, INIT/SIDECARS SECOND ─────────────────
        // This order was REVERSED until 2026-09-14, and with native sidecars
        // that is a live defect rather than a cosmetic one: a sidecar is a
        // proxy or an agent the app container is actively talking to, so
        // stopping it first tears the proxy out from under in-flight traffic.
        // It surfaces as connection-refused noise in the app's final log lines
        // and reads as an application bug.
        //
        // Upstream's rule (KEP-753 R8): stop the regular app containers, and
        // only once they are gone stop the sidecars.
        for record in lp.containers.values() {
            self.cleanup_container(&record.container_id).await?;
        }
        // Then the init containers. A pod deleted mid-init, or after a
        // completed init sequence, still has them recorded — an exited init
        // container retains its podman name until removed. stop is a no-op on
        // an already-exited container; remove frees the
        // `<ns>_<pod>_init-<cname>` name. Idempotent (already-gone is success).
        //
        // ★ `pending-sidecar-reverse-order`: upstream stops sidecars in REVERSE
        // `spec.initContainers` order, and that is not expressible here —
        // `LocalPod::init_containers` is a `BTreeMap<String, _>`, i.e.
        // ALPHABETICAL, so "reverse the iteration" would yield
        // reverse-alphabetical, which is a different sequence that happens to
        // look right whenever containers are declared alphabetically. Fixing it
        // means making the map ordered, which is a wider change than this one;
        // recorded rather than approximated, because an ordering that is wrong
        // in a way tests can pass is worse than one that is openly absent.
        for record in lp.init_containers.values() {
            self.cleanup_container(&record.container_id).await?;
        }
        // emptyDir is pod-lifetime scratch → reap its backing podman named
        // volume now that every container is stopped+removed. The volume name
        // recorded on the LocalPod is the logical `spec.volumes[i].name`; the
        // materializer maps it to the deterministic backing volume.
        let namespace = key.namespace.as_deref().unwrap_or("default");
        for vol in &lp.empty_dir_volumes {
            self.volume_materializer
                .remove_empty_dir(namespace, &key.name, vol)
                .await
                .map_err(|e| KubeletError::Backend(format!("remove emptyDir {vol}: {e}")))?;
        }
        Ok(())
    }

    /// Pre-fetch every `Secret` / `ConfigMap` referenced by `env[].valueFrom`
    /// (across both `spec.containers` and `spec.initContainers`) into a
    /// `(kind, name) → Value` lookup, so [`Self::resolve_env_entry`] can stay
    /// pure. Same pattern as [`Self::resolve_pod_volume_mounts`].
    ///
    /// A missing source object stays out of the map: the pure resolver treats
    /// `optional: true` refs as empty and non-optional refs as a typed
    /// [`KubeletError::InvalidPod`], so the pod stays Pending until the
    /// object appears (and the store watch will requeue on that create).
    async fn resolve_pod_env_sources(
        &self,
        namespace: &str,
        pod: &Value,
    ) -> BTreeMap<(String, String), Value> {
        let mut refs: BTreeMap<(String, String), ()> = BTreeMap::new();
        for spec_key in ["containers", "initContainers"] {
            let Some(cs) = pod
                .get("spec")
                .and_then(|s| s.get(spec_key))
                .and_then(|c| c.as_array())
            else {
                continue;
            };
            for c in cs {
                let Some(env) = c.get("env").and_then(|e| e.as_array()) else {
                    continue;
                };
                for e in env {
                    let Some(vf) = e.get("valueFrom") else {
                        continue;
                    };
                    for (field, kind) in
                        [("secretKeyRef", "Secret"), ("configMapKeyRef", "ConfigMap")]
                    {
                        if let Some(name) = vf
                            .get(field)
                            .and_then(|r| r.get("name"))
                            .and_then(|n| n.as_str())
                        {
                            refs.insert((kind.to_string(), name.to_string()), ());
                        }
                    }
                }
            }
        }
        let mut out: BTreeMap<(String, String), Value> = BTreeMap::new();
        for (kind, name) in refs.into_keys() {
            let key = ResourceKey::namespaced("", "v1", &kind, namespace, &name);
            if let Some(val) = self.store.get(&key).await {
                out.insert((kind, name), val);
            }
        }
        out
    }

    /// Resolve the Pod's `spec.volumes[]` into a `volName → MountSource` map
    /// (the M0.7 kubelet-volumes brick), reusing the SAME in-process store
    /// read the kubelet already does for Services.
    ///
    /// Pre-fetches every referenced ConfigMap/Secret ASYNC from the store
    /// (`self.store.get(ResourceKey::namespaced("","v1","ConfigMap"|"Secret",ns,name))`)
    /// into a lookup map, then drives the PURE [`crate::pod_volume::resolve_pod_volumes`]
    /// interpreter with a sync closure over that map + this kubelet's
    /// [`VolumeMaterializer`]. The split keeps the resolver pure (mockable
    /// without the store) while the store reads stay where the async lives.
    ///
    /// Empty / absent `spec.volumes` ⇒ `Ok(empty map)` (no store reads, no
    /// materialization) — the no-volume fast path.
    ///
    /// # Errors
    ///
    /// Any [`VolumeResolveError`] (missing non-optional source, missing key,
    /// multi/no/unsupported source, materializer failure). The caller maps
    /// [`VolumeResolveError::pending_reason`] onto every container's
    /// `waiting.reason` + keeps the pod Pending.
    async fn resolve_pod_volume_mounts(
        &self,
        namespace: &str,
        pod_name: &str,
        pod: &Value,
    ) -> Result<BTreeMap<String, MountSource>, VolumeResolveError> {
        // No-volume fast path: skip ALL store reads + materialization.
        let volumes = pod_volumes(pod)?;
        if volumes.is_empty() {
            return Ok(BTreeMap::new());
        }

        // Pre-fetch every referenced source object ASYNC, keyed by (kind,
        // name). ConfigMap/Secret are namespaced; a persistentVolumeClaim
        // fetches the namespaced PVC AND — when the PVC is Bound — its
        // cluster-scoped bound PV, so the pure resolver can map PVC → PV →
        // node-local hostPath without a store read. Deferred arms
        // (hostPath/projected/downwardAPI) surface their typed error in the
        // pure resolver without a fetch.
        let mut fetched: BTreeMap<(String, String), Value> = BTreeMap::new();
        for vol in &volumes {
            // from_volume errors (multi/no source) are re-detected by the pure
            // resolver below; here we only need the (kind, name) to pre-fetch.
            let source = match PodVolumeSource::from_volume(vol) {
                Ok(s) => s,
                Err(_) => continue,
            };
            match source {
                PodVolumeSource::ConfigMap { name, .. } | PodVolumeSource::Secret { name, .. }
                    if name.is_empty() =>
                {
                    let _ = name;
                }
                PodVolumeSource::ConfigMap { name, .. } => {
                    let key = ResourceKey::namespaced("", "v1", "ConfigMap", namespace, &name);
                    if let Some(val) = self.store.get(&key).await {
                        fetched.insert(("ConfigMap".to_string(), name), val);
                    }
                }
                PodVolumeSource::Secret { name, .. } => {
                    let key = ResourceKey::namespaced("", "v1", "Secret", namespace, &name);
                    if let Some(val) = self.store.get(&key).await {
                        fetched.insert(("Secret".to_string(), name), val);
                    }
                }
                PodVolumeSource::Pvc { claim_name, .. } => {
                    if claim_name.is_empty() {
                        continue;
                    }
                    // The PVC lives in the pod's namespace.
                    let pvc_key = ResourceKey::namespaced(
                        "",
                        "v1",
                        "PersistentVolumeClaim",
                        namespace,
                        &claim_name,
                    );
                    let Some(pvc_val) = self.store.get(&pvc_key).await else {
                        continue; // resolver emits PvcNotBound
                    };
                    // If Bound, pre-fetch the cluster-scoped bound PV too.
                    if let Some(pv_name) = pvc_val
                        .get("spec")
                        .and_then(|s| s.get("volumeName"))
                        .and_then(Value::as_str)
                        .filter(|n| !n.is_empty())
                    {
                        let pv_key =
                            ResourceKey::cluster_scoped("", "v1", "PersistentVolume", pv_name);
                        if let Some(pv_val) = self.store.get(&pv_key).await {
                            fetched.insert(
                                ("PersistentVolume".to_string(), pv_name.to_string()),
                                pv_val,
                            );
                        }
                    }
                    fetched.insert(("PersistentVolumeClaim".to_string(), claim_name), pvc_val);
                }
                _ => {}
            }
        }

        // Drive the PURE interpreter with a sync closure over the pre-fetched
        // map + this kubelet's materializer.
        let fetch = |kind: &str, name: &str| -> Option<Value> {
            fetched.get(&(kind.to_string(), name.to_string())).cloned()
        };
        crate::pod_volume::resolve_pod_volumes(
            pod,
            namespace,
            pod_name,
            fetch,
            self.volume_materializer.as_ref(),
        )
        .await
    }

    /// Build + write a Pod `status` with EVERY container `Waiting{reason}` —
    /// the no-silent-wrong-answer path for a volume-resolution failure. The
    /// pod stays `Pending` with `containerStatuses[].state.waiting.reason`
    /// set to the typed [`VolumeResolveError::pending_reason`] (e.g.
    /// `ConfigMapNotFound`); NO container is started. The caller arms a
    /// requeue so a later-created source converges the pod to Running.
    async fn write_pod_volume_pending(
        &self,
        key: &ResourceKey,
        value: &Value,
        container_names: &[String],
        reason: &str,
        report: &mut ReconcileReport,
    ) -> Result<(), ControllerError> {
        let statuses: Vec<ContainerStatusOut> = container_names
            .iter()
            .map(|name| ContainerStatusOut {
                name: name.clone(),
                ready: false,
                state: ContainerState::Waiting {
                    reason: reason.to_string(),
                },
                container_id: None,
                restart_count: 0,
            })
            .collect();
        let desired = Self::build_pod_status(
            value,
            engenho_types::curated_enums::PodPhase::Pending,
            &statuses,
            None,
        );
        self.write_pod_status(key, value, &desired, report).await
    }

    /// Resolve `spec.volumes[]`, OR write the pod Pending with a typed
    /// `waiting.reason` on failure. Returns `Ok(Some(map))` on success (the
    /// `volName → MountSource` map to stamp onto specs); `Ok(None)` when a
    /// resolution error already wrote the pod Pending + armed a requeue (the
    /// caller returns without starting any container — the no-silent-wrong-
    /// answer path).
    async fn resolve_or_pending(
        &self,
        key: &ResourceKey,
        value: &Value,
        namespace: &str,
        container_names: &[String],
        report: &mut ReconcileReport,
        soonest_requeue: &mut Option<Duration>,
    ) -> Result<Option<BTreeMap<String, MountSource>>, ControllerError> {
        match self
            .resolve_pod_volume_mounts(namespace, &key.name, value)
            .await
        {
            Ok(map) => Ok(Some(map)),
            Err(e) => {
                let reason = e.pending_reason();
                warn!(
                    pod = %key.label(),
                    error = %e,
                    reason = reason,
                    "volume resolution failed; pod stays Pending (no container started)"
                );
                self.write_pod_volume_pending(key, value, container_names, reason, report)
                    .await?;
                // Arm a requeue so the next tick re-resolves once the source
                // appears (mirrors the probe-cadence requeue).
                let next = soonest_requeue.map_or(MIN_PROBE_REQUEUE, |d| d.min(MIN_PROBE_REQUEUE));
                *soonest_requeue = Some(next);
                report.objects_skipped += 1;
                Ok(None)
            }
        }
    }

    /// (B) Start a bound Pod's containers (one per `spec.containers[i]`) +
    /// write its initial status via CAS, inserting the local bookkeeping on
    /// success.
    ///
    /// MULTI-CONTAINER: computes the Service-name aliases ONCE for the pod,
    /// applies them to every container's spec, then starts each container
    /// with the deterministic backend name `<ns>_<pod>_<cname>`. A container
    /// that fails to start leaves the pod Pending (the started siblings stay
    /// in the record + the next tick re-attempts the missing one — the pod
    /// converges).
    async fn start_bound_pod(
        &self,
        key: &ResourceKey,
        value: &Value,
        report: &mut ReconcileReport,
        soonest_requeue: &mut Option<Duration>,
    ) -> Result<(), ControllerError> {
        let namespace = key.namespace.as_deref().unwrap_or("default");

        // Pre-fetch env valueFrom sources (Secret/ConfigMap) once for the
        // pod, keeping the spec extraction pure.
        let env_sources = self.resolve_pod_env_sources(namespace, value).await;

        // INIT CONTAINERS: if the pod declares any init containers and they
        // haven't all Succeeded yet, the kubelet runs the init sequence FIRST —
        // one init container at a time, in order — and does NOT start any app
        // container this pass. `reconcile_init` drives the sequence + renders
        // status (Pending + initContainerStatuses + Initialized=False). Only
        // once init is Complete does the app-start path below run. A pod with
        // NO init containers returns an empty Vec here, so this whole block is
        // skipped and the app-start path runs BYTE-IDENTICALLY to before the
        // init-container brick (the behavior-preserving guarantee).
        let init_specs =
            match Self::pod_to_init_container_specs(namespace, &key.name, value, &env_sources) {
                Ok(s) => s,
                Err(e) => {
                    warn!(
                        pod = %key.label(),
                        error = %e,
                        "skipping pod with invalid init-container manifest"
                    );
                    report.objects_skipped += 1;
                    return Ok(());
                }
            };
        if !init_specs.is_empty() {
            let init_complete = self
                .local
                .lock()
                .await
                .get(key)
                .map(|lp| lp.init_complete)
                .unwrap_or(false);
            if !init_complete {
                // Drive the init sequence (starts init[0] on the first pass).
                return self
                    .reconcile_init(key, value, &init_specs, report, soonest_requeue)
                    .await;
            }
        }

        let specs = match Self::pod_to_container_specs(namespace, &key.name, value, &env_sources) {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    pod = %key.label(),
                    error = %e,
                    "skipping pod with invalid manifest"
                );
                report.objects_skipped += 1;
                return Ok(());
            }
        };

        // Parse the per-container probes BEFORE starting anything: a parse
        // error (no-handler / grpc / unresolved port) skips the whole pod
        // (NEVER a fake pass). Parsing here (not after start) means a bad probe
        // never even starts a container. The ProbeRuntimes are stamped at the
        // container's start instant below, but the spec parse is what can fail.
        let now = self.now();
        let pod_label = key.label().to_string();
        let mut probe_state_by_cname: BTreeMap<String, ContainerProbeState> = BTreeMap::new();
        for (cname, _spec) in &specs {
            let Some(cjson) = Self::container_json(value, cname) else {
                continue;
            };
            match Self::parse_container_probes(cjson, &pod_label, now) {
                Ok(ps) => {
                    probe_state_by_cname.insert(cname.clone(), ps);
                }
                Err(e) => {
                    warn!(
                        pod = %key.label(),
                        container = %cname,
                        error = %e,
                        "skipping pod with invalid probe (never a fake pass)"
                    );
                    report.objects_skipped += 1;
                    return Ok(());
                }
            }
        }

        // (M0.3 cluster-DNS) Compute the Service-name aliases this Pod earns
        // ONCE (they're pod-level, not per-container) BEFORE building any run
        // argv — `--network-alias` is a `podman run` flag and cannot be added
        // to a running container. LIST Services in the pod's namespace + reuse
        // the EndpointsController selector predicate. aardvark-dns resolves
        // these names to the pod's IP.
        let (aliases, cluster_ip_hosts) = self.service_name_resolution(value, namespace).await;

        // (M0.7 kubelet-volumes) Resolve + materialize `spec.volumes[]` ONCE
        // for the pod (configMap/secret → host files; emptyDir → a shared
        // podman named volume) BEFORE building any run argv — `-v` is a
        // `podman run` flag. The resolved `volName → MountSource` map is then
        // mapped per-container via `volumeMounts[]` onto each spec's `mounts`.
        //
        // NO SILENT WRONG ANSWER: a missing non-optional source (or an
        // unsupported source class) does NOT start any container + does NOT
        // skip silently — it writes the pod Pending with EVERY container
        // `Waiting{ reason: <typed> }` (e.g. ConfigMapNotFound) + arms a
        // requeue so a later-created source converges the pod to Running on a
        // future tick. A no-volume pod returns an empty map (no store reads,
        // no materialization) → every spec keeps `mounts: vec![]` → identical
        // behavior to before this brick.
        // Project the pod's ServiceAccount credentials ONCE for the pod, the
        // way upstream's admission injects a `kube-api-access-*` volume into
        // every pod that has a service account. Without these three files an
        // in-cluster client finds the service env, tries
        // `Config::incluster()`, and dies on a missing `namespace` file.
        //
        // A projection FAILURE is not swallowed: a pod that cannot get its
        // identity must not reach Running and then fail every API call it
        // makes. It stays Pending with the reason visible.
        // ── ★ `automountServiceAccountToken: false` MEANS IT ─────────────
        // Upstream defaults this to true and injects the projection; a pod
        // that sets it false is saying it does not use the API and does not
        // want a credential. engenho projected unconditionally, so the field
        // was accepted by the apiserver and then ignored by the kubelet —
        // exactly the "inheriting a promise nobody writes down" failure this
        // repo's CLAUDE.md names.
        //
        // It is load-bearing beyond tidiness: the projection mounts at a FIXED
        // path from a source directory under engenho's state dir, and a native
        // (no-mount-namespace) backend cannot reconcile the two. A workload
        // that correctly declines the token was still refused for it.
        let automount = value
            .pointer("/spec/automountServiceAccountToken")
            .and_then(Value::as_bool)
            .unwrap_or(true);

        let sa_name = value
            .pointer("/spec/serviceAccountName")
            .and_then(Value::as_str)
            .unwrap_or("default");
        let pod_uid = value
            .pointer("/metadata/uid")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let sa_mount = if !automount {
            // No projection, no materialization, no mount. The pod asked for
            // no credential and gets none.
            None
        } else {
            match self
                .sa_projector
                .project(namespace, sa_name, &key.name, pod_uid)
                .await
            {
                Ok(Some(files)) => {
                    let src = self
                        .volume_materializer
                        .materialize_files(namespace, &key.name, "kube-api-access", &files)
                        .await
                        .map_err(|e| {
                            ControllerError::Internal(format!(
                                "materialize ServiceAccount projection: {e}"
                            ))
                        })?;
                    Some(crate::pod_volume::ResolvedMount {
                        source: src,
                        mount_path: crate::pod_volume::SA_MOUNT_PATH.to_string(),
                        // Read-only, as upstream projects it. A writable
                        // credential directory lets a compromised container
                        // rewrite its own identity.
                        read_only: true,
                        sub_path: None,
                    })
                }
                Ok(None) => None,
                Err(e) => {
                    warn!(
                        pod = %key.label(),
                        error = %e,
                        "ServiceAccount projection failed; pod stays Pending"
                    );
                    report.objects_skipped += 1;
                    return Ok(());
                }
            }
        };

        let cnames: Vec<String> = specs.iter().map(|(c, _)| c.clone()).collect();
        let Some(resolved) = self
            .resolve_or_pending(key, value, namespace, &cnames, report, soonest_requeue)
            .await?
        else {
            // A resolution error already wrote the pod Pending + armed a
            // requeue; nothing else to do this tick.
            return Ok(());
        };

        // Ensure a fresh local record exists, then start each container not
        // yet recorded. (On a partial prior start the record already holds
        // some containers; this is start-only-the-missing.)
        let mut started_any = false;
        for (cname, mut spec) in specs {
            // Skip containers already started (membership guard — never a
            // spurious restart).
            if self
                .local
                .lock()
                .await
                .get(key)
                .map(|lp| lp.containers.contains_key(&cname))
                .unwrap_or(false)
            {
                continue;
            }
            spec.network_aliases = aliases.clone();
            spec.host_add = cluster_ip_hosts.clone();
            // (M0.7 kubelet-volumes) Map this container's `volumeMounts[]`
            // against the resolved `volName → MountSource` map onto its
            // `spec.mounts`. A volumeMount naming an unknown volume is an
            // invalid pod (NoSource) — skip the WHOLE pod (never a fake
            // start). A container with no volumeMounts gets `mounts: vec![]`
            // → identical argv to before this brick.
            if let Some(cjson) = Self::container_json(value, &cname) {
                match container_mounts(cjson, &resolved) {
                    Ok(mut mounts) => {
                        // Append the projected ServiceAccount credentials.
                        // Upstream's admission injects a `kube-api-access-*`
                        // projected volume into every pod with a service
                        // account; engenho materializes the same three files
                        // directly, reaching the same place by the route a
                        // single-binary runtime has available.
                        if let Some(m) = &sa_mount {
                            mounts.push(m.clone());
                        }
                        spec.mounts = mounts;
                    }
                    Err(e) => {
                        warn!(
                            pod = %key.label(),
                            container = %cname,
                            error = %e,
                            "skipping pod: container references an undeclared volume"
                        );
                        report.objects_skipped += 1;
                        return Ok(());
                    }
                }
            }
            debug!(
                pod = %key.label(),
                container = %cname,
                image = %spec.image,
                backend = self.backend.name(),
                aliases = spec.network_aliases.len(),
                mounts = spec.mounts.len(),
                "kubelet starting container"
            );
            match self.backend.start(&spec).await {
                Ok(status) => {
                    let mut local = self.local.lock().await;
                    let entry = local.entry(key.clone()).or_default();
                    // Record this pod's emptyDir volume names ONCE so
                    // delete-cleanup can reap the backing podman named volumes.
                    // emptyDir sources resolve to MountSource::NamedVolume
                    // (legacy) OR MountSource::EmptyDirHostDir (current, since
                    // a563f42 resolved the named volume to its host mountpoint
                    // to sidestep a libpod-API/crun mismatch). configMap/secret
                    // (HostDir files) are NOT recorded — they aren't podman
                    // named volumes. Idempotent: only set on the first
                    // container start (when the list is still empty).
                    if entry.empty_dir_volumes.is_empty() {
                        entry.empty_dir_volumes = resolved
                            .iter()
                            .filter(|(_, src)| {
                                matches!(
                                    src,
                                    crate::pod_volume::MountSource::NamedVolume(_)
                                        | crate::pod_volume::MountSource::EmptyDirHostDir(_)
                                )
                            })
                            .map(|(name, _)| name.clone())
                            .collect();
                    }
                    entry.containers.insert(
                        cname.clone(),
                        ContainerRecord {
                            container_id: status.container_id.clone(),
                            restart_count: 0,
                            // Attach the parsed probe state (its runtimes are
                            // stamped at `now`, the start instant). Default
                            // (all-None) for a container with no probes.
                            probes: probe_state_by_cname
                                .get(&cname)
                                .cloned()
                                .unwrap_or_default(),
                            started_at: Some(now),
                            terminated_at: None,
                            // Remember what this container was started with,
                            // so a restart can be given the same mounts.
                            mounts: spec.mounts.clone(),
                        },
                    );
                    started_any = true;
                    self.emit(
                        key,
                        engenho_controllers::event_recorder::Reason::Started,
                        format!("Started container {cname}"),
                    )
                    .await;
                }
                Err(e) => {
                    warn!(
                        pod = %key.label(),
                        container = %cname,
                        error = %e,
                        "container start failed; pod remains pending"
                    );
                    // Tell the CLUSTER, not just the log.
                    //
                    // `Reason::Failed` sat in the closed vocabulary with no
                    // emitting site, under a doc comment promising that
                    // "only reasons engenho can actually emit today are
                    // listed" — so a container that could not start was
                    // visible ONLY to whoever could read the daemon's
                    // stdout. Measured 2026-08-30: a pod wedged at
                    // ContainerCreating for two minutes showed `kubectl get
                    // events` empty, `status.containerStatuses[].state.
                    // waiting` with no message, and the real cause
                    // ("image not known") in the log alone. An operator has
                    // no way in to that.
                    //
                    // The error text goes in the message verbatim. A
                    // generic "failed to start" would preserve the silence
                    // that made this worth fixing: the reason a container
                    // did not start IS the diagnostic.
                    self.emit(
                        key,
                        engenho_controllers::event_recorder::Reason::Failed,
                        format!("Failed to start container {cname}: {e}"),
                    )
                    .await;
                    report.objects_skipped += 1;
                }
            }
        }

        // Reconcile the status UNCONDITIONALLY — including when NOTHING
        // started.
        //
        // This used to be guarded on `started_any`, which meant a pod whose
        // containers ALL failed to start had no status written at all: no
        // `phase`, no conditions, no containerStatuses. Measured 2026-08-28 on
        // the live daemon, where every container start was failing with
        // `podman … spawn: No such file or directory` (the launchd agent has
        // no podman on PATH) — the API showed pods with a `nodeName` and
        // literally nothing else, so a total, permanent failure was
        // indistinguishable from "not yet processed" to every client. The
        // operator saw an empty k9s screen and no error anywhere.
        //
        // Upstream ALWAYS reports such a pod as `Pending` with each container
        // `Waiting{reason}`, plus an Event. `reconcile_running` already
        // renders exactly that for a container with no local record (see its
        // `ContainerObservation::waiting` arm — "partial start. Waiting → pod
        // Pending"), so the zero-started case needs no special handling; the
        // guard was pure loss of signal.
        //
        // `started_any` is retained for the log line below: "nothing started"
        // is worth saying once per tick at debug, and it keeps the variable
        // meaningful rather than deleting information.
        if !started_any {
            debug!(
                pod = %key.label(),
                "no container started this tick; rendering Pending/Waiting status"
            );
        }
        let lp = self
            .local
            .lock()
            .await
            .get(key)
            .cloned()
            .unwrap_or_default();
        self.reconcile_running(key, value, &lp, report, soonest_requeue)
            .await?;
        Ok(())
    }

    /// Run the DUE probes of one running container, fold their verdicts into
    /// the per-container [`ProbeRuntime`]s (persisted back into `self.local`),
    /// and return the aggregated `(ready, needs_restart)` decision via
    /// [`ProbeOutcome`]. Also folds the container's soonest next-probe-due into
    /// `soonest_requeue`.
    ///
    /// A container with NO probes short-circuits: `ready = is_running` (true,
    /// since this is only called on a running container), `needs_restart =
    /// false`, no requeue contributed — the behavior-preserving common case.
    async fn run_container_probes(
        &self,
        record: &ContainerRecord,
        _spec: &ContainerSpec,
        container_id: &str,
        pod_ip: Option<&str>,
        now: Instant,
        soonest_requeue: &mut Option<Duration>,
    ) -> ProbeOutcome {
        // Fast path: no probes → behavior-preserving (ready = is_running).
        if record.probes.is_empty() {
            return ProbeOutcome {
                ready: true,
                needs_restart: false,
            };
        }

        // Work on a clone of the probe state so we drive the I/O without
        // holding the lock, then persist the advanced runtimes back.
        let mut probes = record.probes.clone();

        // Helper: for one probe slot, if due, run + fold; always fold the
        // probe's next-due into soonest_requeue.
        // We run them sequentially: startup first (it gates the others), then
        // readiness + liveness. The verdicts are aggregated below.
        let mut startup_done = true;
        let mut has_startup = false;
        let mut startup_needs_restart = false;

        if let Some((spec, rt)) = probes.startup.as_mut() {
            has_startup = true;
            if rt.is_due(spec, now) {
                let obs = run_handler(
                    spec,
                    &*self.backend,
                    &*self.net_prober,
                    container_id,
                    pod_ip,
                )
                .await;
                let verdict = fold_probe_observation(spec, rt, obs, now);
                startup_needs_restart = verdict.needs_restart;
            }
            startup_done = rt.gate_satisfied;
            Self::accumulate_requeue(soonest_requeue, rt.next_due_in(spec, now));
        }

        let mut readiness_ready = false;
        let mut has_readiness = false;
        if let Some((spec, rt)) = probes.readiness.as_mut() {
            has_readiness = true;
            if rt.is_due(spec, now) {
                let obs = run_handler(
                    spec,
                    &*self.backend,
                    &*self.net_prober,
                    container_id,
                    pod_ip,
                )
                .await;
                let _ = fold_probe_observation(spec, rt, obs, now);
            }
            readiness_ready = rt.gate_satisfied;
            Self::accumulate_requeue(soonest_requeue, rt.next_due_in(spec, now));
        }

        let mut liveness_needs_restart = false;
        if let Some((spec, rt)) = probes.liveness.as_mut() {
            if rt.is_due(spec, now) {
                let obs = run_handler(
                    spec,
                    &*self.backend,
                    &*self.net_prober,
                    container_id,
                    pod_ip,
                )
                .await;
                let verdict = fold_probe_observation(spec, rt, obs, now);
                liveness_needs_restart = verdict.needs_restart;
            }
            Self::accumulate_requeue(soonest_requeue, rt.next_due_in(spec, now));
        }

        // Aggregate the per-kind gates into effective readiness + whether
        // liveness restart may fire (startup window suppresses it).
        let (effective_ready, may_run_liveness) = aggregate_container_readiness(
            startup_done,
            readiness_ready,
            has_startup,
            has_readiness,
            /* is_running */ true,
        );

        // A startup probe that itself failed past threshold ALWAYS restarts (a
        // container that never boots IS restarted), regardless of the gate.
        // Liveness restart only fires once the startup window has passed.
        let needs_restart = startup_needs_restart || (may_run_liveness && liveness_needs_restart);

        // Persist the advanced probe runtimes back into the local record.
        {
            let key_probes = &mut probes;
            let mut local = self.local.lock().await;
            // Find the record by container_id (the cname isn't threaded here,
            // but container_id is stable for this tick). Iterate the pod's
            // containers to locate it.
            for pod in local.values_mut() {
                if let Some(rec) = pod
                    .containers
                    .values_mut()
                    .find(|r| r.container_id == container_id)
                {
                    rec.probes = key_probes.clone();
                    break;
                }
            }
        }

        ProbeOutcome {
            ready: effective_ready,
            needs_restart,
        }
    }

    /// Fold a candidate next-due `delay` into the running soonest minimum.
    fn accumulate_requeue(soonest: &mut Option<Duration>, delay: Duration) {
        *soonest = Some(match *soonest {
            Some(cur) => cur.min(delay),
            None => delay,
        });
    }

    /// Restart ONE container (the existing stop→remove→start→record-update
    /// sequence used by BOTH the exit-code restart path and the liveness/
    /// startup probe restart path). Re-applies the pod-level Service aliases,
    /// starts a fresh container, removes the old one (best-effort), bumps
    /// `restart_count`, RESETS the container's probe runtimes (fresh startup
    /// window), and returns the new container's status. The caller owns the
    /// `ContainerObservation` it builds from the result.
    ///
    /// Errors from `start` are returned so the caller reports the failure +
    /// retries next tick (never silent).
    ///
    /// ORDERING: stop THEN remove the OLD container BEFORE starting the
    /// replacement. The new container reuses the deterministic `--name`
    /// `<ns>_<pod>_<cname>`, so the old one (running for a liveness restart,
    /// or exited-but-still-named for an exit-code restart) MUST be removed
    /// first or `podman run --name` fails with "name already in use". (The
    /// M0.2 exit-code path's start-then-remove only worked under FakeBackend,
    /// which doesn't enforce name uniqueness — surfaced live by the liveness
    /// restart bar.)
    // The args are the precise restart inputs (key/value/namespace/cname/spec +
    // old id + old restart count); threading them as one struct would add a
    // single-use type for no clarity gain.
    #[allow(clippy::too_many_arguments)]
    async fn restart_container(
        &self,
        key: &ResourceKey,
        value: &Value,
        namespace: &str,
        cname: &str,
        spec: &ContainerSpec,
        old_container_id: &str,
        old_restart_count: u32,
    ) -> Result<crate::backend::ContainerStatus, KubeletError> {
        let mut restart_spec = spec.clone();
        // BOTH halves — this path used to set only the aliases, which is the
        // defect `service_name_resolution` documents.
        let (aliases, cluster_ip_hosts) = self.service_name_resolution(value, namespace).await;
        restart_spec.network_aliases = aliases;
        restart_spec.host_add = cluster_ip_hosts;
        // ── ★ RESTORE THE MOUNTS THE CONTAINER STARTED WITH ──────────────
        // `spec` here came from `pod_to_container_specs`, a pure function of
        // the Pod value, so its `mounts` is ALWAYS empty — resolution lives on
        // the create path, which alone holds the volume map and the projected
        // ServiceAccount. Without this the replacement container is started
        // with no `-v` flags and comes up missing every volume, including
        // `/var/run/secrets/kubernetes.io/serviceaccount`. Measured
        // 2026-08-31: a controller restarted once and then CrashLooped on
        // `Config::incluster()` for the rest of the pod's life.
        //
        // Same shape as the `kubernetes_service` note on `PodmanBackend`: an
        // input stamped at one of three `backend.start` call sites, missed by
        // the restart path.
        restart_spec.mounts = {
            let local = self.local.lock().await;
            local
                .get(key)
                .and_then(|p| p.containers.get(cname))
                .map(|rec| rec.mounts.clone())
                .unwrap_or_default()
        };
        // Free the deterministic name first: stop THEN remove the old container
        // (best-effort — an exited container is already stopped). Only then can
        // the replacement reuse `--name`.
        let _ = self.backend.stop(old_container_id).await;
        let _ = self.backend.remove(old_container_id).await;
        let new_status = self.backend.start(&restart_spec).await?;
        let new_count = old_restart_count + 1;
        let now = self.now();
        {
            let mut local = self.local.lock().await;
            if let Some(rec) = local.get_mut(key).and_then(|p| p.containers.get_mut(cname)) {
                // The replacement was started with these; keep them so the
                // NEXT restart is given them too.
                rec.mounts.clone_from(&restart_spec.mounts);
                rec.container_id.clone_from(&new_status.container_id);
                rec.restart_count = new_count;
                rec.started_at = Some(now);
                rec.terminated_at = None;
                // Fresh startup window + zeroed probe counters on restart.
                rec.probes.reset(now);
            }
        }
        Ok(new_status)
    }

    /// (C) Poll the backend for EVERY container of a started Pod, fold the
    /// observed states into the pod phase via [`reconcile_pod_phase`], apply
    /// restartPolicy + probe verdicts (restart an exited / liveness-failing /
    /// startup-failing container under the policy; source each container's
    /// readiness from the readiness-probe verdict), and write the
    /// multi-container status. The `soonest_requeue` accumulator collects the
    /// smallest next-probe-due so `tick` can arm a one-shot re-tick.
    async fn reconcile_running(
        &self,
        key: &ResourceKey,
        value: &Value,
        lp: &LocalPod,
        report: &mut ReconcileReport,
        soonest_requeue: &mut Option<Duration>,
    ) -> Result<(), ControllerError> {
        let restart_policy = Self::pod_restart_policy(value);
        let namespace = key.namespace.as_deref().unwrap_or("default");
        let env_sources = self.resolve_pod_env_sources(namespace, value).await;

        // INIT CONTAINERS: if the pod has init containers and they haven't all
        // Succeeded yet (`!init_complete`), route to the init reconcile —
        // poll/advance the init sequence + render Pending + initContainerStatuses
        // + Initialized=False, NEVER touching the app containers. A pod with no
        // init containers (or one already init_complete) falls through to the
        // app reconcile below, which itself renders initContainerStatuses +
        // Initialized=True alongside the app status once init_complete.
        let init_specs =
            match Self::pod_to_init_container_specs(namespace, &key.name, value, &env_sources) {
                Ok(s) => s,
                Err(e) => {
                    warn!(pod = %key.label(), error = %e, "invalid init manifest during reconcile");
                    report.objects_skipped += 1;
                    return Ok(());
                }
            };
        let has_init = !init_specs.is_empty();
        if has_init && !lp.init_complete {
            return self
                .reconcile_init(key, value, &init_specs, report, soonest_requeue)
                .await;
        }

        // Re-derive the expected container set from the manifest so a
        // not-yet-started container shows up as Waiting (the pod is Pending
        // until every container has started at least once).
        let specs = match Self::pod_to_container_specs(namespace, &key.name, value, &env_sources) {
            Ok(s) => s,
            Err(e) => {
                warn!(pod = %key.label(), error = %e, "invalid manifest during reconcile");
                report.objects_skipped += 1;
                return Ok(());
            }
        };

        let now = self.now();
        let mut observations: Vec<ContainerObservation> = Vec::with_capacity(specs.len());
        let mut pod_ip: Option<String> = None;
        let mut vanished = false;
        // Per-container poll. Collect the typed observations + handle restart.
        for (cname, spec) in &specs {
            let record = lp.containers.get(cname);
            let Some(record) = record else {
                // Recorded by neither start nor record → the container hasn't
                // been started yet (partial start). Waiting → pod Pending.
                observations.push(ContainerObservation::waiting(cname));
                continue;
            };
            match self.backend.status(&record.container_id).await {
                Ok(Some(s)) if s.running => {
                    if let Some(ip) = &s.pod_ip {
                        pod_ip.get_or_insert_with(|| ip.clone());
                    }
                    // ── PROBE TICK: run due probes, fold verdicts, decide the
                    // container's effective readiness + whether liveness/startup
                    // requests a restart. A container with NO probes short-
                    // circuits to ready = is_running (behavior-preserving) +
                    // contributes no requeue.
                    let outcome = self
                        .run_container_probes(
                            record,
                            spec,
                            &record.container_id,
                            s.pod_ip.as_deref().or(pod_ip.as_deref()),
                            now,
                            soonest_requeue,
                        )
                        .await;

                    if outcome.needs_restart && restart_policy != RestartPolicy::Never {
                        // Liveness/startup failed past threshold → restart THIS
                        // container via the existing restart machinery
                        // (restartPolicy:Never suppresses it — K8s semantics).
                        match self
                            .restart_container(
                                key,
                                value,
                                namespace,
                                cname,
                                spec,
                                &record.container_id,
                                record.restart_count,
                            )
                            .await
                        {
                            Ok(new_status) => {
                                if let Some(ip) = &new_status.pod_ip {
                                    pod_ip.get_or_insert_with(|| ip.clone());
                                }
                                // A freshly-restarted container is not-ready
                                // until its probes re-pass (startup gate / first
                                // readiness success).
                                observations.push(ContainerObservation {
                                    name: cname.clone(),
                                    state: ContainerState::Running,
                                    container_id: Some(new_status.container_id.clone()),
                                    restart_count: record.restart_count + 1,
                                    ready: false,
                                    // App container: the init-kind field is
                                    // meaningless here and stays Regular.
                                    kind: crate::lifecycle::InitKind::Regular,
                                    ever_started: true,
                                });
                                report.objects_changed += 1;
                                self.emit(
                                    key,
                                    engenho_controllers::event_recorder::Reason::Unhealthy,
                                    format!(
                                        "Container {cname} failed its liveness/startup probe and was restarted (restart #{})",
                                        record.restart_count + 1
                                    ),
                                )
                                .await;
                                debug!(
                                    pod = %key.label(),
                                    container = %cname,
                                    restart_count = record.restart_count + 1,
                                    "kubelet restarted container (probe verdict)"
                                );
                            }
                            Err(e) => {
                                warn!(
                                    pod = %key.label(),
                                    container = %cname,
                                    error = %e,
                                    "probe-driven restart failed; retrying next tick"
                                );
                                observations.push(ContainerObservation {
                                    name: cname.clone(),
                                    state: ContainerState::Running,
                                    container_id: Some(record.container_id.clone()),
                                    restart_count: record.restart_count,
                                    ready: outcome.ready,
                                    kind: crate::lifecycle::InitKind::Regular,
                                    ever_started: true,
                                });
                                report.objects_skipped += 1;
                            }
                        }
                    } else {
                        // No restart this tick → readiness sources from the
                        // probe verdict (REPLACING the hard-`true`). A no-probe
                        // container's outcome.ready == is_running (true here).
                        observations.push(ContainerObservation {
                            name: cname.clone(),
                            state: ContainerState::Running,
                            container_id: Some(record.container_id.clone()),
                            restart_count: record.restart_count,
                            ready: outcome.ready,
                            kind: crate::lifecycle::InitKind::Regular,
                            ever_started: true,
                        });
                    }
                }
                Ok(Some(s)) => {
                    // Terminated. Retain the last pod_ip (K8s keeps it).
                    if let Some(ip) = &s.pod_ip {
                        pod_ip.get_or_insert_with(|| ip.clone());
                    }
                    let exit = s.exit_code.unwrap_or(0);
                    // restartPolicy: restart THIS one container if the policy
                    // says so (Always, or OnFailure+nonzero). The pod stays
                    // Running across the restart (reconcile_pod_phase folds a
                    // restartable-terminated container to Running). Uses the
                    // shared restart_container helper (same stop→remove→start→
                    // record-update + probe-reset as the liveness path).
                    // ── CRASHLOOP BACKOFF. Without this the kubelet
                    // restarts a failing container on EVERY tick, which is
                    // the hot loop that produced a pod at 149 restarts with
                    // nothing in the cluster able to explain it.
                    //
                    // The stamp is taken here, on the FIRST tick that sees
                    // the exit, so the delay is measured from the exit and
                    // not from whenever the operator happened to look.
                    let backoff = if restart_policy.should_restart(s.exit_code) {
                        let (since_exit, uptime) = {
                            let mut local = self.local.lock().await;
                            let rec = local.get_mut(key).and_then(|p| p.containers.get_mut(cname));
                            match rec {
                                Some(r) => {
                                    let exited = *r.terminated_at.get_or_insert(now);
                                    let uptime = r.started_at.map_or(Duration::ZERO, |st| {
                                        exited.saturating_duration_since(st)
                                    });
                                    (now.saturating_duration_since(exited), uptime)
                                }
                                None => (Duration::ZERO, Duration::ZERO),
                            }
                        };
                        crate::backoff::decide(record.restart_count, since_exit, uptime)
                    } else {
                        // Not restartable at all — the terminal-latch branch
                        // below owns it. `Restart` here is never acted on.
                        crate::backoff::BackoffDecision::Restart
                    };

                    if let crate::backoff::BackoffDecision::Wait { remaining } = backoff {
                        // Ask to be re-ticked when the hold expires rather
                        // than relying on the next periodic sweep: a 5-minute
                        // cap with a 30-second sweep would restart up to
                        // 4m30s late, and the lateness grows with the delay.
                        let soon = soonest_requeue.get_or_insert(remaining);
                        *soon = (*soon).min(remaining);
                        debug!(
                            pod = %key.label(),
                            container = %cname,
                            restart_count = record.restart_count,
                            remaining_secs = remaining.as_secs(),
                            "container held in CrashLoopBackOff"
                        );
                        // ★ THE EVENT THAT WAS MISSING. A pod at 149
                        // restarts said nothing; this is the line that
                        // would have explained it without reading podman.
                        self.emit(
                            key,
                            engenho_controllers::event_recorder::Reason::BackOff,
                            format!(
                                "Back-off restarting failed container {cname} ({}s remaining, {} prior restarts)",
                                remaining.as_secs(),
                                record.restart_count
                            ),
                        )
                        .await;
                        observations.push(ContainerObservation::backing_off(
                            cname,
                            &record.container_id,
                            backoff.waiting_reason().unwrap_or("CrashLoopBackOff"),
                            record.restart_count,
                        ));
                        continue;
                    }

                    if restart_policy.should_restart(s.exit_code) {
                        match self
                            .restart_container(
                                key,
                                value,
                                namespace,
                                cname,
                                spec,
                                &record.container_id,
                                record.restart_count,
                            )
                            .await
                        {
                            Ok(new_status) => {
                                if let Some(ip) = &new_status.pod_ip {
                                    pod_ip.get_or_insert_with(|| ip.clone());
                                }
                                observations.push(ContainerObservation::running(
                                    cname,
                                    &new_status.container_id,
                                    record.restart_count + 1,
                                ));
                                report.objects_changed += 1;
                                self.emit(
                                    key,
                                    engenho_controllers::event_recorder::Reason::Started,
                                    format!(
                                        "Restarted container {cname} (exit {exit}, restart #{})",
                                        record.restart_count + 1
                                    ),
                                )
                                .await;
                                debug!(
                                    pod = %key.label(),
                                    container = %cname,
                                    restart_count = record.restart_count + 1,
                                    "kubelet restarted exited container (restartPolicy)"
                                );
                            }
                            Err(e) => {
                                // Restart failed — report the terminated state
                                // this tick; next tick retries. Never silent.
                                warn!(
                                    pod = %key.label(),
                                    container = %cname,
                                    error = %e,
                                    "container restart failed; retrying next tick"
                                );
                                observations.push(ContainerObservation::terminated(
                                    cname,
                                    &record.container_id,
                                    exit,
                                    record.restart_count,
                                ));
                                report.objects_skipped += 1;
                            }
                        }
                    } else {
                        // restartPolicy:Never (or OnFailure+zero) → terminal
                        // latch. Leave the local entry; later ticks keep
                        // reporting the terminal phase (idempotent-skip).
                        observations.push(ContainerObservation::terminated(
                            cname,
                            &record.container_id,
                            exit,
                            record.restart_count,
                        ));
                    }
                }
                Ok(None) => {
                    // The backend lost THIS container out-of-band. Clear the
                    // whole pod's local entry so the next tick re-creates it
                    // (a managed bound pod converges back to running). One
                    // vanished container forces a full re-create — simplest
                    // safe behavior at this brick.
                    vanished = true;
                }
                Err(e) => {
                    warn!(
                        pod = %key.label(),
                        container = %cname,
                        error = %e,
                        "container status poll failed; retrying next tick"
                    );
                    report.objects_skipped += 1;
                    // Treat as Waiting so the pod doesn't flip terminal on a
                    // transient inspect error.
                    observations.push(ContainerObservation::waiting(cname));
                }
            }
        }

        if vanished {
            self.local.lock().await.remove(key);
            debug!(
                pod = %key.label(),
                "backend lost a container; clearing local entry to re-create next tick"
            );
            report.objects_changed += 1;
            return Ok(());
        }

        // Fold the observations → pod phase + per-container statuses, render,
        // and CAS-write. The pure reconcile_pod_phase is the interpreter; this
        // is the I/O shell.
        let (phase, statuses) = reconcile_pod_phase(restart_policy, &observations);
        let desired = if has_init {
            // init_complete pod (we only reach here past the init route once
            // init_complete): render initContainerStatuses (every init
            // container Terminated exit 0) + Initialized=True alongside the app
            // status. The init records hold the succeeded init containers; we
            // build their typed statuses from the manifest order so a kubectl
            // describe shows the completed init sequence.
            let init_statuses = self.init_statuses_terminated(key, &init_specs).await;
            Self::build_pod_status_with_init(
                value,
                phase,
                &init_statuses,
                &statuses,
                pod_ip.as_deref(),
                /* initialized */ true,
                /* has_init */ true,
            )
        } else {
            Self::build_pod_status(value, phase, &statuses, pod_ip.as_deref())
        };
        self.write_pod_status(key, value, &desired, report).await
    }

    /// Build the `initContainerStatuses` array for an init-complete pod — every
    /// init container reported `Terminated{ exit 0 }` (Succeeded), in
    /// `spec.initContainers` order. Reads the recorded init [`ContainerRecord`]
    /// for each container's id + restart count; an init container missing from
    /// the local record (shouldn't happen once init_complete, but defensively
    /// handled) is rendered Terminated exit 0 with no id rather than dropped.
    async fn init_statuses_terminated(
        &self,
        key: &ResourceKey,
        init_specs: &[(String, ContainerSpec)],
    ) -> Vec<ContainerStatusOut> {
        let local = self.local.lock().await;
        let init_recs = local.get(key).map(|lp| &lp.init_containers);
        init_specs
            .iter()
            .map(|(cname, _)| {
                let rec = init_recs.and_then(|m| m.get(cname));
                ContainerStatusOut {
                    name: cname.clone(),
                    ready: false,
                    state: ContainerState::terminated(0),
                    container_id: rec.map(|r| r.container_id.clone()),
                    restart_count: rec.map(|r| r.restart_count).unwrap_or(0),
                }
            })
            .collect()
    }

    /// Build a single init container's [`ContainerSpec`] ready to start:
    /// stamps the pod-level Service aliases + the resolved volume mounts (init
    /// containers may mount volumes exactly like app containers). The base spec
    /// already carries the disambiguating `<ns>_<pod>_init-<cname>` backend
    /// name from [`Self::pod_to_init_container_specs`].
    ///
    /// Returns the populated spec, or a typed error if the container's
    /// `volumeMounts[]` reference an undeclared volume (NoSource) — the caller
    /// surfaces it (never a fake start).
    fn build_init_spec(
        value: &Value,
        cname: &str,
        base: &ContainerSpec,
        aliases: &[String],
        cluster_ip_hosts: &[String],
        resolved: &BTreeMap<String, MountSource>,
    ) -> Result<ContainerSpec, KubeletError> {
        let mut spec = base.clone();
        spec.network_aliases = aliases.to_vec();
        // An init container resolves Service names too — a migration job that
        // waits on its database is the common shape — so it gets the same
        // ClusterIP map as the app containers.
        spec.host_add = cluster_ip_hosts.to_vec();
        if let Some(cjson) = Self::container_json_in(value, "initContainers", cname) {
            spec.mounts =
                container_mounts(cjson, resolved).map_err(|e| KubeletError::InvalidPod {
                    pod: cname.to_string(),
                    reason: format!("init container volume: {e}"),
                })?;
        }
        Ok(spec)
    }

    /// Start ONE init container (the spec already carries its name/aliases/
    /// mounts) and record it under [`LocalPod::init_containers`] with a fresh
    /// (zero) restart count + default (empty) probe state — init containers do
    /// NOT carry probes. Returns the new container's status.
    ///
    /// # Errors
    ///
    /// Propagates a [`KubeletError::Backend`] from the runtime `start`.
    async fn start_init_container(
        &self,
        key: &ResourceKey,
        cname: &str,
        spec: &ContainerSpec,
        restart_count: u32,
    ) -> Result<crate::backend::ContainerStatus, KubeletError> {
        let status = self.backend.start(spec).await?;
        let mut local = self.local.lock().await;
        let entry = local.entry(key.clone()).or_default();
        entry.init_containers.insert(
            cname.to_string(),
            ContainerRecord {
                container_id: status.container_id.clone(),
                restart_count,
                // Init containers never carry probes (K8s does not run
                // liveness/readiness/startup on init containers).
                probes: ContainerProbeState::default(),
                // Init containers are not subject to CrashLoopBackOff here:
                // the init sequence has its own ordering, and stamping a
                // start it does not read would be a field nobody consults.
                started_at: None,
                terminated_at: None,
                mounts: spec.mounts.clone(),
            },
        );
        Ok(status)
    }

    /// Look up a single `spec.<key>[i]` JSON object (generalized
    /// [`Self::container_json`] over the spec key so init containers resolve
    /// their `volumeMounts[]` from `spec.initContainers[i]`).
    fn container_json_in<'a>(pod: &'a Value, spec_key: &str, cname: &str) -> Option<&'a Value> {
        let containers = pod.get("spec")?.get(spec_key)?.as_array()?;
        containers
            .iter()
            .enumerate()
            .find(|(i, c)| {
                let name = c
                    .get("name")
                    .and_then(|n| n.as_str())
                    .map(String::from)
                    .unwrap_or_else(|| {
                        if *i == 0 {
                            "main".to_string()
                        } else {
                            format!("container-{i}")
                        }
                    });
                name == cname
            })
            .map(|(_, c)| c)
    }

    /// INIT reconcile — the I/O driver over the pure
    /// [`crate::lifecycle::next_init_action`] sequencer. NO probes (K8s does
    /// not run them on init containers). Polls each init container's recorded
    /// state, builds the ORDERED `Vec<ContainerObservation>` (Waiting if not
    /// yet recorded, Running if up, Terminated{exit} if exited), asks the pure
    /// sequencer for the next [`InitAction`], and acts:
    ///
    ///   * `AwaitInit{index}` — ensure init[index] is started (start it if not
    ///     in the record) or restarted (if it Terminated with a restartable
    ///     exit under the policy: stop+remove the old, start fresh, bump the
    ///     restart count). Status = Pending + initContainerStatuses +
    ///     Initialized=False; arm a near requeue so the next tick advances.
    ///   * `InitFailed` — phase Failed + initContainerStatuses +
    ///     Initialized=False; latch (do NOT start app containers).
    ///   * `Complete` — set `init_complete = true`, then start the app
    ///     containers via the normal start path (which now routes past init)
    ///     and render the full status.
    async fn reconcile_init(
        &self,
        key: &ResourceKey,
        value: &Value,
        init_specs: &[(String, ContainerSpec)],
        report: &mut ReconcileReport,
        soonest_requeue: &mut Option<Duration>,
    ) -> Result<(), ControllerError> {
        let restart_policy = Self::pod_restart_policy(value);
        let namespace = key.namespace.as_deref().unwrap_or("default");

        // Compute pod-level Service aliases + resolve volume mounts ONCE (init
        // containers earn the same aliases + may mount the pod's volumes). A
        // volume-resolution error writes the pod Pending with the typed reason
        // + arms a requeue (the no-silent-wrong-answer path), exactly like the
        // app-start path.
        let (aliases, cluster_ip_hosts) = self.service_name_resolution(value, namespace).await;
        let cnames: Vec<String> = init_specs.iter().map(|(c, _)| c.clone()).collect();
        let Some(resolved) = self
            .resolve_or_pending(key, value, namespace, &cnames, report, soonest_requeue)
            .await?
        else {
            return Ok(());
        };

        // Build the ORDERED observations from the recorded init state + a poll.
        let lp = self
            .local
            .lock()
            .await
            .get(key)
            .cloned()
            .unwrap_or_default();
        let mut observations: Vec<ContainerObservation> = Vec::with_capacity(init_specs.len());
        for (cname, spec) in init_specs {
            // Every observation carries the kind so the fold can tell a
            // sidecar from a regular init container — without it the fold is
            // correct code reading uniformly-Regular input, i.e. the old
            // behaviour with extra steps.
            let mark = |o: ContainerObservation| {
                let o = if spec.init_kind.is_sidecar() {
                    o.as_sidecar()
                } else {
                    o
                };
                // A RECORD existing means this container has been started
                // before, which `state` alone cannot say.
                o.with_ever_started(lp.init_containers.contains_key(cname))
            };
            match lp.init_containers.get(cname) {
                None => observations.push(mark(ContainerObservation::waiting(cname))),
                Some(record) => match self.backend.status(&record.container_id).await {
                    Ok(Some(s)) if s.running => {
                        observations.push(mark(ContainerObservation::running(
                            cname,
                            &record.container_id,
                            record.restart_count,
                        )))
                    }
                    Ok(Some(s)) => observations.push(mark(ContainerObservation::terminated(
                        cname,
                        &record.container_id,
                        s.exit_code.unwrap_or(0),
                        record.restart_count,
                    ))),
                    Ok(None) => {
                        // Backend lost this init container out-of-band → treat
                        // as Waiting so it re-starts on the AwaitInit path.
                        observations.push(mark(ContainerObservation::waiting(cname)));
                    }
                    Err(e) => {
                        warn!(
                            pod = %key.label(),
                            container = %cname,
                            error = %e,
                            "init container status poll failed; treating as Waiting"
                        );
                        report.objects_skipped += 1;
                        observations.push(mark(ContainerObservation::waiting(cname)));
                    }
                },
            }
        }

        match crate::lifecycle::next_init_action(restart_policy, &observations) {
            crate::lifecycle::InitAction::Complete => {
                // Every init container Succeeded → latch init_complete, then
                // run the app-start path (which now routes PAST init since
                // init_complete is set) to start the app containers + render
                // the full status (initContainerStatuses + Initialized=True).
                {
                    let mut local = self.local.lock().await;
                    local.entry(key.clone()).or_default().init_complete = true;
                }
                report.objects_changed += 1;
                debug!(
                    pod = %key.label(),
                    init_containers = init_specs.len(),
                    "kubelet init sequence complete; starting app containers"
                );
                // Box the recursive call: reconcile_init → start_bound_pod →
                // (init_complete now true) → app path. Boxing breaks the
                // infinitely-sized async future (E0733).
                Box::pin(self.start_bound_pod(key, value, report, soonest_requeue)).await
            }
            crate::lifecycle::InitAction::InitFailed { index, exit_code } => {
                // Terminal init failure (non-zero exit under restartPolicy:Never)
                // → pod Failed; app containers never start. Render the init
                // statuses (the failed one Terminated non-zero) + Initialized
                // False. Latch (no app start, init_complete stays false).
                warn!(
                    pod = %key.label(),
                    index,
                    exit_code,
                    "init container failed terminally; pod Failed (app never starts)"
                );
                let init_statuses = self.init_statuses_observed(&observations);
                let desired = Self::build_pod_status_with_init(
                    value,
                    engenho_types::curated_enums::PodPhase::Failed,
                    &init_statuses,
                    &[],
                    None,
                    /* initialized */ false,
                    /* has_init */ true,
                );
                self.write_pod_status(key, value, &desired, report).await
            }
            crate::lifecycle::InitAction::AwaitInit { start, blocked_on } => {
                // ── ★ `start` IS A SET NOW, NOT ONE INDEX ────────────────────
                // Before native sidecars, `AwaitInit { index }` meant both
                // "ensure index is started" and "start nothing after it" —
                // with sidecars those separate. `start` carries every index
                // that must be running this tick (the sidecars cleared so far,
                // plus whichever container the sequence is gated on), and
                // `blocked_on` says whether anything still blocks at all.
                let mut pod_ip = None;
                for index in start {
                    let (cname, base_spec) = &init_specs[index];
                    let ip = self
                        .advance_active_init(
                            key,
                            value,
                            index,
                            cname,
                            base_spec,
                            &aliases,
                            &cluster_ip_hosts,
                            &resolved,
                            &lp,
                            report,
                        )
                        .await?;
                    pod_ip = pod_ip.or(ip);
                }
                // `blocked_on: None` means every REGULAR init container has
                // succeeded and only sidecars were (re)started — the pod is
                // initialized and app containers may run. Re-enter the
                // reconcile rather than writing Pending over a pod that is
                // ready to proceed, which would be the forever-Pending hang
                // wearing a different hat.
                if blocked_on.is_none() {
                    self.local
                        .lock()
                        .await
                        .entry(key.clone())
                        .or_default()
                        .init_complete = true;
                    return Ok(());
                }

                // Re-read the (possibly just-updated) init records so the
                // rendered initContainerStatuses reflect the freshly-started /
                // restarted container.
                let init_statuses = self.init_statuses_current(key, init_specs).await;
                let desired = Self::build_pod_status_with_init(
                    value,
                    engenho_types::curated_enums::PodPhase::Pending,
                    &init_statuses,
                    &[],
                    pod_ip.as_deref(),
                    /* initialized */ false,
                    /* has_init */ true,
                );
                self.write_pod_status(key, value, &desired, report).await?;

                // Arm a near requeue so the next tick advances the sequence
                // (mirrors the probe-cadence / volume-pending requeue floor).
                let next = soonest_requeue.map_or(MIN_PROBE_REQUEUE, |d| d.min(MIN_PROBE_REQUEUE));
                *soonest_requeue = Some(next);
                Ok(())
            }
        }
    }

    /// Ensure the active init container (`index`) is started or restarted.
    ///
    ///   * Not yet recorded → start it fresh (restart_count 0).
    ///   * Recorded + Terminated with a restartable exit (the sequencer only
    ///     returns `AwaitInit` for a Terminated init container when the policy
    ///     restarts it) → stop+remove the old, start fresh, bump restart_count.
    ///   * Recorded + Running → in flight: nothing to do (await its exit).
    ///
    /// Returns the active init container's pod IP when freshly started/restarted
    /// (so the Pending status carries it), else `None`.
    #[allow(clippy::too_many_arguments)]
    async fn advance_active_init(
        &self,
        key: &ResourceKey,
        value: &Value,
        index: usize,
        cname: &str,
        base_spec: &ContainerSpec,
        aliases: &[String],
        cluster_ip_hosts: &[String],
        resolved: &BTreeMap<String, MountSource>,
        lp: &LocalPod,
        report: &mut ReconcileReport,
    ) -> Result<Option<String>, ControllerError> {
        let spec = match Self::build_init_spec(
            value,
            cname,
            base_spec,
            aliases,
            cluster_ip_hosts,
            resolved,
        ) {
            Ok(s) => s,
            Err(e) => {
                warn!(pod = %key.label(), container = %cname, error = %e,
                    "skipping pod: init container references an undeclared volume");
                report.objects_skipped += 1;
                return Ok(None);
            }
        };

        match lp.init_containers.get(cname) {
            None => {
                // Not yet started → start init[index] fresh.
                debug!(
                    pod = %key.label(),
                    container = %cname,
                    index,
                    image = %spec.image,
                    "kubelet starting init container"
                );
                match self.start_init_container(key, cname, &spec, 0).await {
                    Ok(status) => {
                        report.objects_changed += 1;
                        Ok(status.pod_ip)
                    }
                    Err(e) => {
                        warn!(pod = %key.label(), container = %cname, error = %e,
                            "init container start failed; pod remains Pending");
                        report.objects_skipped += 1;
                        Ok(None)
                    }
                }
            }
            Some(record) => {
                // Recorded. Poll once: a Terminated-but-restartable init
                // container is restarted (stop+remove old, start fresh, bump
                // count); a Running one is awaited (no-op).
                match self.backend.status(&record.container_id).await {
                    Ok(Some(s)) if s.running => Ok(s.pod_ip),
                    Ok(Some(_)) | Ok(None) => {
                        // Terminated (restartable — the sequencer said
                        // AwaitInit for it) OR vanished → (re)start fresh.
                        let new_count = record.restart_count + 1;
                        let _ = self.backend.stop(&record.container_id).await;
                        let _ = self.backend.remove(&record.container_id).await;
                        match self
                            .start_init_container(key, cname, &spec, new_count)
                            .await
                        {
                            Ok(status) => {
                                report.objects_changed += 1;
                                debug!(
                                    pod = %key.label(),
                                    container = %cname,
                                    restart_count = new_count,
                                    "kubelet restarted failed init container (restartPolicy)"
                                );
                                Ok(status.pod_ip)
                            }
                            Err(e) => {
                                warn!(pod = %key.label(), container = %cname, error = %e,
                                    "init container restart failed; retrying next tick");
                                report.objects_skipped += 1;
                                Ok(None)
                            }
                        }
                    }
                    Err(e) => {
                        warn!(pod = %key.label(), container = %cname, error = %e,
                            "init container status poll failed; retrying next tick");
                        report.objects_skipped += 1;
                        Ok(None)
                    }
                }
            }
        }
    }

    /// Render `initContainerStatuses` from the freshly-built observations (used
    /// on the InitFailed path so the failed container's exact non-zero exit is
    /// reported).
    fn init_statuses_observed(
        &self,
        observations: &[ContainerObservation],
    ) -> Vec<ContainerStatusOut> {
        observations
            .iter()
            .map(|o| ContainerStatusOut {
                name: o.name.clone(),
                ready: o.ready,
                state: o.state.clone(),
                container_id: o.container_id.clone(),
                restart_count: o.restart_count,
            })
            .collect()
    }

    /// Render `initContainerStatuses` by re-reading the CURRENT init records +
    /// polling each (used on the AwaitInit path so the just-started/restarted
    /// active container shows Running, prior ones Terminated exit 0, later ones
    /// Waiting). Order follows `init_specs`.
    async fn init_statuses_current(
        &self,
        key: &ResourceKey,
        init_specs: &[(String, ContainerSpec)],
    ) -> Vec<ContainerStatusOut> {
        let lp = self
            .local
            .lock()
            .await
            .get(key)
            .cloned()
            .unwrap_or_default();
        let mut out = Vec::with_capacity(init_specs.len());
        for (cname, _spec) in init_specs {
            let status_out = match lp.init_containers.get(cname) {
                None => ContainerStatusOut {
                    name: cname.clone(),
                    ready: false,
                    state: ContainerState::creating(),
                    container_id: None,
                    restart_count: 0,
                },
                Some(record) => {
                    let state = match self.backend.status(&record.container_id).await {
                        Ok(Some(s)) if s.running => ContainerState::Running,
                        Ok(Some(s)) => ContainerState::terminated(s.exit_code.unwrap_or(0)),
                        // Vanished / poll error → Waiting (will re-start).
                        _ => ContainerState::creating(),
                    };
                    ContainerStatusOut {
                        name: cname.clone(),
                        ready: false,
                        state,
                        container_id: Some(record.container_id.clone()),
                        restart_count: record.restart_count,
                    }
                }
            };
            out.push(status_out);
        }
        out
    }

    /// Stream a container's logs. The apiserver's Pod `/log` subresource calls
    /// this in-process (single-node) with the pod's namespace/name + optional
    /// `-c <container>` selector.
    ///
    /// Resolves the container's backend id from the local bookkeeping, then
    /// asks the backend. `container` selects which container; `None` defaults
    /// to the FIRST container in the pod (deterministic — sorted by name in
    /// the BTreeMap; kubectl defaults to the first container in spec order, and
    /// for a single-container pod they coincide).
    ///
    /// # Errors
    ///
    /// [`KubeletError::InvalidPod`] when the pod isn't tracked locally (not
    /// running on this node) or the named container doesn't exist;
    /// [`KubeletError::Backend`] on a backend log-read failure. NEVER an
    /// empty-Ok for a missing container.
    pub async fn container_logs(
        &self,
        namespace: &str,
        name: &str,
        container: Option<&str>,
        opts: &LogOptions,
    ) -> Result<String, KubeletError> {
        let key = ResourceKey::namespaced("", "v1", "Pod", namespace, name);
        let local = self.local.lock().await;
        let lp = local.get(&key).ok_or_else(|| KubeletError::InvalidPod {
            pod: format!("{namespace}/{name}"),
            reason: "pod is not running on this node (no local container record)".into(),
        })?;
        let record = match container {
            Some(c) => lp
                .containers
                .get(c)
                .ok_or_else(|| KubeletError::InvalidPod {
                    pod: format!("{namespace}/{name}"),
                    reason: format!("container {c:?} not found in pod"),
                })?,
            // Default: the first container by name (BTreeMap iteration order).
            None => lp
                .containers
                .values()
                .next()
                .ok_or_else(|| KubeletError::InvalidPod {
                    pod: format!("{namespace}/{name}"),
                    reason: "pod has no started containers".into(),
                })?,
        };
        let container_id = record.container_id.clone();
        // Drop the lock before the backend await (no lock held across I/O).
        drop(local);
        self.backend.logs(&container_id, opts).await
    }

    /// Write a computed Pod `status` via the shared item-5 CAS primitive.
    /// A `Conflict` is benign (the operator raced a spec change) — dropped;
    /// the next wake recomputes. Only a committed write bumps
    /// `objects_changed`.
    ///
    /// When the parent has no parseable `resourceVersion` yet (freshly
    /// minted, not re-listed), `write_status_cas` skips the CAS write
    /// rather than issuing an unconditional one — that NoChange is a
    /// genuine no-op here too (the next watch-wake re-reads fresh state).
    async fn write_pod_status(
        &self,
        key: &ResourceKey,
        parent: &Value,
        desired: &Value,
        report: &mut ReconcileReport,
    ) -> Result<(), ControllerError> {
        // Defensive symmetry with the rest of the controller suite: if the
        // parent carries no resourceVersion this tick, write_status_cas
        // already skips — but logging here keeps the gap observable.
        if resource_version_of(parent).is_none() {
            debug!(
                pod = %key.label(),
                "pod has no resourceVersion this tick; status write deferred"
            );
        }
        if write_status_cas(&self.store, key, parent, desired)
            .await?
            .changed()
        {
            report.objects_changed += 1;
        }
        Ok(())
    }
}

#[cfg(test)]
mod env_resolution_tests {
    use super::Kubelet;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn pod() -> serde_json::Value {
        json!({
            "metadata": { "name": "pangea-operator-abc", "namespace": "pangea-system",
                          "uid": "11111111-2222-3333-4444-555555555555" },
            "spec": { "nodeName": "cid", "serviceAccountName": "pangea-operator" },
            "status": { "podIP": "10.42.0.7", "hostIP": "192.168.1.10" }
        })
    }

    /// The exact value that stopped Flux: an unexpanded `$(RUNTIME_NAMESPACE)`
    /// reached the container as literal text, so kustomize-controller asked
    /// for a hostname containing `$(…)` and fetched nothing.
    #[test]
    fn a_dollar_paren_reference_expands_from_an_earlier_variable() {
        let mut defined = BTreeMap::new();
        defined.insert("RUNTIME_NAMESPACE".to_string(), "flux-system".to_string());
        assert_eq!(
            Kubelet::expand_env_refs(
                "http://source-controller.$(RUNTIME_NAMESPACE).svc.cluster.local.",
                &defined
            ),
            "http://source-controller.flux-system.svc.cluster.local."
        );
    }

    /// `$$` is the escape for a literal `$`. Without it, a password or a shell
    /// snippet carrying `$$` would be silently rewritten.
    #[test]
    fn a_doubled_dollar_is_an_escape_not_a_reference() {
        let defined = BTreeMap::new();
        assert_eq!(
            Kubelet::expand_env_refs("cost is $$5", &defined),
            "cost is $5"
        );
        assert_eq!(
            Kubelet::expand_env_refs("$$(NOT_A_REF)", &defined),
            "$(NOT_A_REF)"
        );
    }

    /// An unresolvable reference is left EXACTLY as written. Blanking it would
    /// turn a typo into a silently-empty hostname, which is strictly harder to
    /// debug than seeing the text that was asked for.
    #[test]
    fn an_unresolvable_reference_is_left_verbatim_never_blanked() {
        let defined = BTreeMap::new();
        assert_eq!(
            Kubelet::expand_env_refs("host.$(MISSING).local", &defined),
            "host.$(MISSING).local"
        );
    }

    /// A bare `$` and an unclosed `$(` are not references and must survive.
    #[test]
    fn a_bare_dollar_is_not_a_reference() {
        let defined = BTreeMap::new();
        assert_eq!(Kubelet::expand_env_refs("100$", &defined), "100$");
        assert_eq!(
            Kubelet::expand_env_refs("$(unclosed", &defined),
            "$(unclosed"
        );
        assert_eq!(Kubelet::expand_env_refs("a $ b", &defined), "a $ b");
    }

    /// Multi-byte text must survive byte-wise scanning — emitting bytes rather
    /// than characters would split a codepoint and produce invalid UTF-8.
    #[test]
    fn multibyte_text_is_not_corrupted() {
        let mut defined = BTreeMap::new();
        defined.insert("NS".to_string(), "café".to_string());
        assert_eq!(
            Kubelet::expand_env_refs("日本語-$(NS)-日本語", &defined),
            "日本語-café-日本語"
        );
    }

    /// Several references in one value, and a reference whose value itself
    /// contains `$(` — which must NOT be re-expanded (upstream substitutes
    /// once, so a value carrying a reference is data, not a further lookup).
    #[test]
    fn expansion_is_single_pass() {
        let mut defined = BTreeMap::new();
        defined.insert("A".to_string(), "$(B)".to_string());
        defined.insert("B".to_string(), "final".to_string());
        assert_eq!(Kubelet::expand_env_refs("$(A)", &defined), "$(B)");
        assert_eq!(
            Kubelet::expand_env_refs("$(A)/$(B)", &defined),
            "$(B)/final"
        );
    }

    fn resolve(entry: serde_json::Value) -> Result<(String, String), super::KubeletError> {
        let sources: BTreeMap<(String, String), serde_json::Value> = BTreeMap::new();
        Kubelet::resolve_env_entry(
            "pangea-system",
            "pangea-operator-abc",
            &pod(),
            "main",
            &entry,
            &sources,
        )
    }

    /// A pod shaped like Flux's controllers: one container declaring both
    /// limits, which is what `resourceFieldRef` reads.
    fn pod_with_resources() -> serde_json::Value {
        json!({
            "metadata": { "name": "helm-controller-1", "namespace": "flux-system" },
            "spec": { "containers": [ {
                "name": "manager",
                "image": "ghcr.io/fluxcd/helm-controller:v1.4.5",
                "resources": {
                    "limits": { "cpu": "1", "memory": "1Gi" },
                    "requests": { "cpu": "500m", "memory": "64Mi" }
                }
            } ] }
        })
    }

    fn resolve_in(
        pod: &serde_json::Value,
        container: &str,
        entry: serde_json::Value,
    ) -> Result<(String, String), super::KubeletError> {
        let sources: BTreeMap<(String, String), serde_json::Value> = BTreeMap::new();
        Kubelet::resolve_env_entry(
            "flux-system",
            "helm-controller-1",
            pod,
            container,
            &entry,
            &sources,
        )
    }

    /// THE ONE FLUX NEEDS. Every Flux controller sets GOMEMLIMIT from
    /// `limits.memory` via the downward API. Until this resolved, all three
    /// were refused at admission and the kubelet retried them EVERY TICK —
    /// measured on rio 2026-09-15: 273 `invalid manifest` warnings in three
    /// minutes, engenho pinned near 126% CPU, node readiness flapping to
    /// Unknown, and a single Secret write taking 70s. Three unadmittable pods
    /// degraded the whole apiserver.
    #[test]
    fn gomemlimit_from_limits_memory_resolves_in_bytes() {
        assert_eq!(
            resolve_in(
                &pod_with_resources(),
                "manager",
                json!({
                    "name": "GOMEMLIMIT",
                    "valueFrom": { "resourceFieldRef": {
                        "containerName": "manager", "resource": "limits.memory"
                    } }
                })
            )
            .unwrap(),
            ("GOMEMLIMIT".to_string(), "1073741824".to_string()),
            "1Gi must render as BYTES — a Go runtime acts on this number"
        );
    }

    /// `containerName` is OPTIONAL and defaults to the enclosing container.
    /// Flux omits it, so an implementation that required it would still
    /// refuse every Flux pod while looking correct against an explicit test.
    #[test]
    fn an_omitted_container_name_means_the_enclosing_container() {
        assert_eq!(
            resolve_in(
                &pod_with_resources(),
                "manager",
                json!({
                    "name": "GOMEMLIMIT",
                    "valueFrom": { "resourceFieldRef": { "resource": "limits.memory" } }
                })
            )
            .unwrap()
            .1,
            "1073741824"
        );
    }

    /// Upstream reports cpu in CORES and rounds UP, so `500m` with the
    /// default divisor is 1, not 0. A zero here would be acted on.
    #[test]
    fn cpu_is_reported_in_whole_cores_rounded_up() {
        assert_eq!(
            resolve_in(
                &pod_with_resources(),
                "manager",
                json!({
                    "name": "GOMAXPROCS",
                    "valueFrom": { "resourceFieldRef": { "resource": "requests.cpu" } }
                })
            )
            .unwrap()
            .1,
            "1",
            "500m must round UP to 1 core, never down to 0"
        );
    }

    /// An explicit divisor scales in the resource's own unit.
    #[test]
    fn a_divisor_scales_in_the_resources_unit() {
        assert_eq!(
            resolve_in(
                &pod_with_resources(),
                "manager",
                json!({
                    "name": "MEM_MI",
                    "valueFrom": { "resourceFieldRef": {
                        "resource": "limits.memory", "divisor": "1Mi"
                    } }
                })
            )
            .unwrap()
            .1,
            "1024"
        );
    }

    /// A reference to a resource the container does not declare REFUSES.
    /// Upstream falls back to the node's allocatable; we do not have that
    /// here, and substituting 0 would hand a Go runtime a GOMEMLIMIT it would
    /// thrash on. Refusing by name is the honest answer.
    #[test]
    fn an_undeclared_resource_refuses_rather_than_substituting() {
        let err = resolve_in(
            &pod_with_resources(),
            "manager",
            json!({
                "name": "EPH",
                "valueFrom": { "resourceFieldRef": {
                    "resource": "limits.ephemeral-storage"
                } }
            }),
        )
        .expect_err("must not invent a value");
        assert!(err.to_string().contains("declares none"), "{err}");
    }

    /// THE REGRESSION. Every one of these used to VANISH — the extractor
    /// required a literal `value` key, so `valueFrom` entries were dropped
    /// with no error and no Pending reason. `leader.rs` reads POD_NAME, so
    /// leader election degraded silently on a healthy-looking pod.
    #[test]
    fn downward_api_entries_no_longer_vanish() {
        for (path, expected) in [
            ("metadata.name", "pangea-operator-abc"),
            ("metadata.namespace", "pangea-system"),
            ("metadata.uid", "11111111-2222-3333-4444-555555555555"),
            ("spec.nodeName", "cid"),
            ("spec.serviceAccountName", "pangea-operator"),
            ("status.podIP", "10.42.0.7"),
            ("status.hostIP", "192.168.1.10"),
        ] {
            let (k, v) = resolve(json!({
                "name": "VAR", "valueFrom": { "fieldRef": { "fieldPath": path } }
            }))
            .unwrap_or_else(|e| panic!("{path} must resolve, got {e}"));
            assert_eq!(k, "VAR");
            assert_eq!(v, expected, "wrong value for {path}");
        }
    }

    /// A literal value still wins and is unchanged.
    #[test]
    fn a_literal_value_is_passed_through() {
        assert_eq!(
            resolve(json!({ "name": "LOG_LEVEL", "value": "debug" })).unwrap(),
            ("LOG_LEVEL".to_string(), "debug".to_string())
        );
    }

    /// Upstream treats `{name: FOO}` with no value as the empty string.
    /// This is semantics, not a fallback — do not "fix" it into an error.
    #[test]
    fn a_bare_name_is_the_empty_string_not_an_omission() {
        assert_eq!(
            resolve(json!({ "name": "EMPTY" })).unwrap(),
            ("EMPTY".to_string(), String::new())
        );
    }

    /// An unsupported SOURCE must fail loudly and name itself. Starting a
    /// container without its credentials is the failure mode this whole
    /// function exists to prevent.
    ///
    /// ★ CHANGED 2026-09-15: this used to assert that `resourceFieldRef` was
    /// unsupported. It is now IMPLEMENTED (Flux needs it for GOMEMLIMIT), so
    /// asserting its refusal would pin the very gap that broke the cluster.
    /// The invariant under test is unchanged — an unknown source still
    /// refuses and still names the variable — only the example moved to a
    /// source that genuinely remains unsupported.
    #[test]
    fn an_unsupported_source_refuses_and_names_itself() {
        let err = resolve(json!({
            "name": "CPU_LIMIT",
            "valueFrom": { "someFutureRef": { "resource": "limits.cpu" } }
        }))
        .expect_err("an unresolvable source must not be silently dropped");
        let msg = err.to_string();
        assert!(
            msg.contains("not supported yet"),
            "error must say the source is unsupported: {msg}"
        );
        assert!(
            msg.contains("CPU_LIMIT"),
            "error must name the variable: {msg}"
        );
    }

    /// A `secretKeyRef` resolves when the caller pre-fetched the Secret.
    /// The Secret's `data` values are base64; the kubelet decodes into utf-8.
    #[test]
    fn a_secret_key_ref_resolves_when_prefetched() {
        use base64::Engine as _;
        let enc = base64::engine::general_purpose::STANDARD.encode("hunter2");
        let mut sources: BTreeMap<(String, String), serde_json::Value> = BTreeMap::new();
        sources.insert(
            ("Secret".to_string(), "pg-app".to_string()),
            json!({ "data": { "password": enc } }),
        );
        let (k, v) = Kubelet::resolve_env_entry(
            "pangea-system",
            "pangea-operator-abc",
            &pod(),
            "main",
            &json!({
                "name": "PGPASSWORD",
                "valueFrom": { "secretKeyRef": { "name": "pg-app", "key": "password" } }
            }),
            &sources,
        )
        .unwrap();
        assert_eq!(k, "PGPASSWORD");
        assert_eq!(v, "hunter2");
    }

    /// A `configMapKeyRef` resolves plaintext from `data`.
    #[test]
    fn a_config_map_key_ref_resolves_when_prefetched() {
        let mut sources: BTreeMap<(String, String), serde_json::Value> = BTreeMap::new();
        sources.insert(
            ("ConfigMap".to_string(), "app-cfg".to_string()),
            json!({ "data": { "level": "debug" } }),
        );
        let (k, v) = Kubelet::resolve_env_entry(
            "pangea-system",
            "pangea-operator-abc",
            &pod(),
            "main",
            &json!({
                "name": "LOG_LEVEL",
                "valueFrom": { "configMapKeyRef": { "name": "app-cfg", "key": "level" } }
            }),
            &sources,
        )
        .unwrap();
        assert_eq!(k, "LOG_LEVEL");
        assert_eq!(v, "debug");
    }

    /// A missing non-optional Secret is a typed InvalidPod naming the object.
    #[test]
    fn a_missing_secret_is_a_typed_invalid_pod() {
        let err = resolve(json!({
            "name": "PGPASSWORD",
            "valueFrom": { "secretKeyRef": { "name": "pg-app", "key": "password" } }
        }))
        .expect_err("a missing non-optional Secret must fail loudly");
        let msg = err.to_string();
        assert!(msg.contains("Secret"), "error must name the kind: {msg}");
        assert!(msg.contains("pg-app"), "error must name the object: {msg}");
    }

    /// An optional missing Secret resolves to empty — upstream semantics.
    #[test]
    fn an_optional_missing_secret_resolves_empty() {
        let (k, v) = resolve(json!({
            "name": "PGPASSWORD",
            "valueFrom": { "secretKeyRef": {
                "name": "pg-app",
                "key": "password",
                "optional": true
            } }
        }))
        .expect("an optional missing Secret is not an error");
        assert_eq!(k, "PGPASSWORD");
        assert_eq!(v, "");
    }

    /// An unknown fieldRef path fails and lists what IS supported, rather
    /// than resolving to empty — which would be indistinguishable from a
    /// legitimately-empty value.
    #[test]
    fn an_unknown_fieldref_path_refuses_and_lists_the_supported_set() {
        let err = resolve(json!({
            "name": "WAT", "valueFrom": { "fieldRef": { "fieldPath": "spec.hostNetwork" } }
        }))
        .expect_err("an unknown fieldPath must not resolve to empty");
        let msg = err.to_string();
        assert!(
            msg.contains("spec.hostNetwork"),
            "must name the bad path: {msg}"
        );
        assert!(
            msg.contains("metadata.name"),
            "must list the supported set: {msg}"
        );
    }

    /// A KNOWN path that is not yet populated resolves to empty rather
    /// than failing. status.podIP is absent before the sandbox exists, and
    /// refusing there would make a legal pod permanently unadmittable on a
    /// timing detail.
    #[test]
    fn a_known_but_unpopulated_path_resolves_empty() {
        let bare = json!({ "metadata": { "name": "p", "namespace": "n" } });
        let sources: BTreeMap<(String, String), serde_json::Value> = BTreeMap::new();
        let (_, v) = Kubelet::resolve_env_entry(
            "n",
            "p",
            &bare,
            "main",
            &json!({ "name": "POD_IP", "valueFrom": { "fieldRef": { "fieldPath": "status.podIP" } } }),
            &sources,
        )
        .expect("a known-but-unset path is not an error");
        assert_eq!(v, "");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pod_identity_survives_the_pod_path_unfused() {
        // A present-but-empty identity would be WORSE than none: a consumer
        // would read `is_present()` as a contract and get a sandbox keyed on
        // "". So assert the values, not the field.
        //
        // The names are chosen to contain the separator on purpose. Joined,
        // "my_ns_my_pod_my_container" cannot be split back — which pod is it,
        // `my/ns_my/pod_my/container` or `my/ns_my_pod/my/container`? The point
        // of carrying identity is that nobody has to answer that.
        let pod = json!({
            "metadata": {"uid": "0e1f-CAFE"},
            "spec": {"containers": [{"name": "my_container", "image": "i"}]}
        });
        let specs =
            Kubelet::pod_to_container_specs("my_ns", "my_pod", &pod, &BTreeMap::new()).unwrap();
        let (_, spec) = &specs[0];
        assert_eq!(spec.name, "my_ns_my_pod_my_container", "the lossy join");
        assert_eq!(spec.name.split('_').count(), 6, "6 pieces, 3 fields");
        assert!(spec.pod.is_present());
        assert_eq!(spec.pod.namespace, "my_ns");
        assert_eq!(spec.pod.name, "my_pod");
        assert_eq!(spec.pod.uid, "0e1f-CAFE");
        assert_eq!(spec.pod.container_name, "my_container");
        assert!(!spec.pod.init);
    }

    #[test]
    fn an_init_container_is_marked_as_one() {
        let pod = json!({
            "metadata": {"uid": "u1"},
            "spec": {
                "initContainers": [{"name": "setup", "image": "i"}],
                "containers": [{"name": "app", "image": "i"}]
            }
        });
        let init = Kubelet::pod_to_init_container_specs("ns", "p", &pod, &BTreeMap::new()).unwrap();
        assert!(
            init[0].1.pod.init,
            "init containers must be distinguishable"
        );
        assert_eq!(init[0].1.pod.container_name, "setup");
        let app = Kubelet::pod_to_container_specs("ns", "p", &pod, &BTreeMap::new()).unwrap();
        assert!(!app[0].1.pod.init);
    }

    #[test]
    fn pod_to_container_specs_extracts_image_and_env() {
        let pod = json!({
            "spec": {
                "containers": [{
                    "name": "main",
                    "image": "nginx:1.27",
                    "env": [
                        {"name": "FOO", "value": "bar"}
                    ]
                }]
            }
        });
        let specs =
            Kubelet::pod_to_container_specs("default", "p1", &pod, &BTreeMap::new()).unwrap();
        assert_eq!(specs.len(), 1);
        let (cname, spec) = &specs[0];
        assert_eq!(cname, "main");
        // Backend name is <ns>_<pod>_<cname>.
        assert_eq!(spec.name, "default_p1_main");
        assert_eq!(spec.image, "nginx:1.27");
        assert_eq!(spec.env.get("FOO").map(String::as_str), Some("bar"));
        assert!(spec.command.is_empty());
    }

    #[test]
    fn pod_to_container_specs_extracts_command_and_args() {
        let pod = json!({
            "spec": {
                "containers": [{
                    "name": "c",
                    "image": "alpine",
                    "command": ["sh", "-c"],
                    "args": ["echo hi; sleep 3600"]
                }]
            }
        });
        let specs = Kubelet::pod_to_container_specs("ns", "x", &pod, &BTreeMap::new()).unwrap();
        // command ++ args.
        assert_eq!(specs[0].1.command, vec!["sh", "-c", "echo hi; sleep 3600"]);
    }

    #[test]
    fn pod_to_container_specs_multi_container() {
        let pod = json!({
            "spec": {
                "containers": [
                    {"name": "web", "image": "nginx"},
                    {"name": "sidecar", "image": "busybox", "command": ["sleep", "300"]}
                ]
            }
        });
        let specs =
            Kubelet::pod_to_container_specs("default", "p", &pod, &BTreeMap::new()).unwrap();
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].0, "web");
        assert_eq!(specs[0].1.name, "default_p_web");
        assert_eq!(specs[1].0, "sidecar");
        assert_eq!(specs[1].1.name, "default_p_sidecar");
        assert_eq!(specs[1].1.command, vec!["sleep", "300"]);
    }

    #[test]
    fn pod_to_container_specs_rejects_missing_image() {
        let pod = json!({"spec": {"containers": [{"name": "c"}]}});
        let err = Kubelet::pod_to_container_specs("ns", "p", &pod, &BTreeMap::new()).unwrap_err();
        assert_eq!(err.kind(), "invalid_pod");
    }

    #[test]
    fn pod_to_container_specs_rejects_empty_containers() {
        let pod = json!({"spec": {"containers": []}});
        assert!(Kubelet::pod_to_container_specs("n", "p", &pod, &BTreeMap::new()).is_err());
    }

    #[test]
    fn pod_to_container_specs_rejects_no_spec() {
        let pod = json!({"metadata": {"name": "p"}});
        assert!(Kubelet::pod_to_container_specs("n", "p", &pod, &BTreeMap::new()).is_err());
    }

    #[test]
    fn pod_restart_policy_reads_spec_with_always_default() {
        assert_eq!(
            Kubelet::pod_restart_policy(&json!({"spec": {"restartPolicy": "Never"}})),
            RestartPolicy::Never
        );
        assert_eq!(
            Kubelet::pod_restart_policy(&json!({"spec": {"restartPolicy": "OnFailure"}})),
            RestartPolicy::OnFailure
        );
        // Absent → Always (K8s default).
        assert_eq!(
            Kubelet::pod_restart_policy(&json!({"spec": {}})),
            RestartPolicy::Always
        );
    }

    #[test]
    fn pod_is_bound_to_matches_node_name() {
        let pod = json!({"spec": {"nodeName": "node-1"}});
        assert!(Kubelet::pod_is_bound_to(&pod, "node-1"));
        assert!(!Kubelet::pod_is_bound_to(&pod, "other-node"));
    }

    #[test]
    fn pod_is_bound_to_false_when_unbound() {
        let pod = json!({"spec": {}});
        assert!(!Kubelet::pod_is_bound_to(&pod, "node-1"));
    }

    #[test]
    fn pod_to_container_specs_names_default_main_then_index() {
        // Unnamed first container → "main"; unnamed second → "container-1".
        let pod = json!({"spec": {"containers": [{"image": "a"}, {"image": "b"}]}});
        let specs = Kubelet::pod_to_container_specs("ns", "p", &pod, &BTreeMap::new()).unwrap();
        assert_eq!(specs[0].0, "main");
        assert_eq!(specs[1].0, "container-1");
    }

    #[test]
    fn pod_already_terminal_detects_terminal_phases() {
        assert!(Kubelet::pod_already_terminal(
            &json!({"status": {"phase": "Succeeded"}})
        ));
        assert!(Kubelet::pod_already_terminal(
            &json!({"status": {"phase": "Failed"}})
        ));
        assert!(!Kubelet::pod_already_terminal(
            &json!({"status": {"phase": "Running"}})
        ));
        assert!(!Kubelet::pod_already_terminal(
            &json!({"status": {"phase": "Pending"}})
        ));
        assert!(!Kubelet::pod_already_terminal(&json!({"spec": {}})));
    }

    fn one_running_status() -> Vec<ContainerStatusOut> {
        vec![ContainerStatusOut {
            name: "c".into(),
            ready: true,
            state: ContainerState::Running,
            container_id: Some("fake-1".into()),
            restart_count: 0,
        }]
    }

    fn type_of<'a>(status: &'a Value, ty: &str) -> Option<&'a Value> {
        status["conditions"]
            .as_array()?
            .iter()
            .find(|c| c["type"] == ty)
    }

    #[test]
    fn a_condition_the_kubelet_does_not_own_survives_the_render() {
        use engenho_types::curated_enums::PodPhase;
        // THE BUG. The rendered array ships through an RFC 7396 merge patch,
        // where arrays REPLACE. So a fresh 2-3 element render deleted every
        // condition the kubelet does not author. engenho's scheduler writes
        // exactly one — PodScheduled=False/Unschedulable — and the first
        // kubelet status write removed it, turning "could not be placed, here
        // is why" into no condition at all.
        let live = json!({"status": {"conditions": [
            {"type": "PodScheduled", "status": "False", "reason": "Unschedulable"},
            {"type": "DisruptionTarget", "status": "True", "reason": "EvictionByEvictionAPI"}
        ]}});
        let status =
            Kubelet::build_pod_status(&live, PodPhase::Running, &one_running_status(), None);

        // The foreign condition is preserved verbatim, reason included.
        let foreign = type_of(&status, "DisruptionTarget").expect("preserved");
        assert_eq!(foreign["reason"], "EvictionByEvictionAPI");

        // NEGATIVE CONTROL: rendering against an EMPTY live pod must NOT
        // produce it. Without this, the assertion above would also pass if the
        // renderer simply invented a DisruptionTarget, and would pass on a
        // renderer that ignored `live` entirely and got lucky.
        let fresh =
            Kubelet::build_pod_status(&json!({}), PodPhase::Running, &one_running_status(), None);
        assert!(
            type_of(&fresh, "DisruptionTarget").is_none(),
            "the renderer must PRESERVE, never invent: {fresh}"
        );
    }

    #[test]
    fn a_stale_unschedulable_is_transitioned_not_deleted() {
        use engenho_types::curated_enums::PodPhase;
        // The kubelet only reconciles pods already bound to this node, so
        // PodScheduled is a tautology by the time this renders — which is why
        // the kubelet owns it upstream. Before this it appeared NOWHERE in
        // engenho: the scheduler writes it only on the failure path, so a
        // successfully-placed pod simply never had one.
        let live = json!({"status": {"conditions": [
            {"type": "PodScheduled", "status": "False", "reason": "Unschedulable"}
        ]}});
        let status =
            Kubelet::build_pod_status(&live, PodPhase::Running, &one_running_status(), None);
        let sched =
            type_of(&status, "PodScheduled").expect("PodScheduled is owned and always emitted");
        assert_eq!(
            sched["status"], "True",
            "transitioned, not deleted: {status}"
        );
        // Exactly one — the stale False was replaced, not appended beside.
        let n = status["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|c| c["type"] == "PodScheduled")
            .count();
        assert_eq!(n, 1, "no duplicate PodScheduled: {status}");
    }

    #[test]
    fn start_time_is_latched_not_reminted_on_every_render() {
        use engenho_types::curated_enums::PodPhase;
        // Re-stamping per render makes "how long has this been running" always
        // read zero, and — worse — makes `desired` differ on every tick, so
        // write_status_cas never yields NoChange and the kubelet writes
        // forever, waking every Pod-subscribed controller.
        let live = json!({"status": {"startTime": "2026-01-01T00:00:00Z"}});
        let a = Kubelet::build_pod_status(&live, PodPhase::Running, &one_running_status(), None);
        assert_eq!(a["startTime"], "2026-01-01T00:00:00Z");
        // Two renders of the same live pod are byte-identical — the property
        // NoChange depends on.
        let b = Kubelet::build_pod_status(&live, PodPhase::Running, &one_running_status(), None);
        assert_eq!(a, b, "the render must be stable at steady state");
        // NEGATIVE CONTROL: with no previous startTime one is minted, so the
        // field is genuinely produced rather than merely echoed.
        let fresh =
            Kubelet::build_pod_status(&json!({}), PodPhase::Running, &one_running_status(), None);
        assert!(fresh["startTime"].as_str().is_some_and(|t| !t.is_empty()));
    }

    #[test]
    fn pod_ips_is_derived_from_pod_ip_not_written_twice() {
        use engenho_types::curated_enums::PodPhase;
        let status = Kubelet::build_pod_status(
            &json!({}),
            PodPhase::Running,
            &one_running_status(),
            Some("10.42.0.7"),
        );
        assert_eq!(status["podIP"], "10.42.0.7");
        // Derived, so `podIP == podIPs[0]` holds by construction rather than
        // by two writers agreeing.
        assert_eq!(status["podIPs"][0]["ip"], "10.42.0.7");
        // No IP ⇒ neither field, rather than an empty array a client must
        // distinguish from "no address yet".
        let none =
            Kubelet::build_pod_status(&json!({}), PodPhase::Pending, &one_running_status(), None);
        assert!(none.get("podIPs").is_none(), "{none}");
    }

    #[test]
    fn build_pod_status_running_carries_ready_and_pod_ip() {
        use engenho_types::curated_enums::PodPhase;
        let statuses = vec![ContainerStatusOut {
            name: "web".into(),
            ready: true,
            state: ContainerState::Running,
            container_id: Some("fake-1".into()),
            restart_count: 0,
        }];
        let status =
            Kubelet::build_pod_status(&json!({}), PodPhase::Running, &statuses, Some("10.42.0.5"));
        assert_eq!(status["phase"], "Running");
        assert_eq!(status["podIP"], "10.42.0.5");
        // Deterministic pair: ContainersReady then Ready, both True when Running
        // + all containers ready.
        assert_eq!(status["conditions"][0]["type"], "ContainersReady");
        assert_eq!(status["conditions"][0]["status"], "True");
        assert_eq!(status["conditions"][1]["type"], "Ready");
        assert_eq!(status["conditions"][1]["status"], "True");
        assert_eq!(status["containerStatuses"][0]["name"], "web");
        assert_eq!(status["containerStatuses"][0]["ready"], true);
        assert!(status["containerStatuses"][0]["state"]["running"].is_object());
        assert_eq!(status["containerStatuses"][0]["containerID"], "fake-1");
        assert_eq!(status["containerStatuses"][0]["restartCount"], 0);
    }

    #[test]
    fn build_pod_status_succeeded_retains_pod_ip() {
        use engenho_types::curated_enums::PodPhase;
        let statuses = vec![ContainerStatusOut {
            name: "web".into(),
            ready: false,
            state: ContainerState::terminated(0),
            container_id: Some("fake-2".into()),
            restart_count: 0,
        }];
        let status = Kubelet::build_pod_status(
            &json!({}),
            PodPhase::Succeeded,
            &statuses,
            Some("10.42.0.9"),
        );
        assert_eq!(status["phase"], "Succeeded");
        // Both conditions False for a terminal pod.
        assert_eq!(status["conditions"][0]["type"], "ContainersReady");
        assert_eq!(status["conditions"][0]["status"], "False");
        assert_eq!(status["conditions"][1]["type"], "Ready");
        assert_eq!(status["conditions"][1]["status"], "False");
        let term = &status["containerStatuses"][0]["state"]["terminated"];
        assert_eq!(term["exitCode"], 0);
        assert_eq!(term["reason"], "Completed");
        // Terminated pods retain their last IP (keeps the field set stable
        // across Running→terminal → no hot loop).
        assert_eq!(status["podIP"], "10.42.0.9");
    }

    #[test]
    fn build_pod_status_failed_nonzero() {
        use engenho_types::curated_enums::PodPhase;
        let statuses = vec![ContainerStatusOut {
            name: "web".into(),
            ready: false,
            state: ContainerState::terminated(137),
            container_id: Some("fake-3".into()),
            restart_count: 0,
        }];
        let status = Kubelet::build_pod_status(&json!({}), PodPhase::Failed, &statuses, None);
        assert_eq!(status["phase"], "Failed");
        let term = &status["containerStatuses"][0]["state"]["terminated"];
        assert_eq!(term["exitCode"], 137);
        assert_eq!(term["reason"], "Error");
        assert!(status.get("podIP").is_none());
    }

    #[test]
    fn build_pod_status_multi_container_array() {
        use engenho_types::curated_enums::PodPhase;
        let statuses = vec![
            ContainerStatusOut {
                name: "web".into(),
                ready: true,
                state: ContainerState::Running,
                container_id: Some("id-web".into()),
                restart_count: 0,
            },
            ContainerStatusOut {
                name: "sidecar".into(),
                ready: true,
                state: ContainerState::Running,
                container_id: Some("id-sc".into()),
                restart_count: 2,
            },
        ];
        let status =
            Kubelet::build_pod_status(&json!({}), PodPhase::Running, &statuses, Some("10.0.0.1"));
        let arr = status["containerStatuses"].as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["name"], "web");
        assert_eq!(arr[1]["name"], "sidecar");
        assert_eq!(arr[1]["restartCount"], 2);
        assert!(arr.iter().all(|c| c["state"]["running"].is_object()));
    }

    #[test]
    fn build_pod_status_pending_with_waiting_container() {
        use engenho_types::curated_enums::PodPhase;
        let statuses = vec![
            ContainerStatusOut {
                name: "web".into(),
                ready: true,
                state: ContainerState::Running,
                container_id: Some("id-web".into()),
                restart_count: 0,
            },
            ContainerStatusOut {
                name: "sidecar".into(),
                ready: false,
                state: ContainerState::creating(),
                container_id: None,
                restart_count: 0,
            },
        ];
        let status = Kubelet::build_pod_status(&json!({}), PodPhase::Pending, &statuses, None);
        assert_eq!(status["phase"], "Pending");
        // Pod not Ready / ContainersReady while a container is Waiting.
        assert_eq!(status["conditions"][0]["type"], "ContainersReady");
        assert_eq!(status["conditions"][0]["status"], "False");
        assert_eq!(status["conditions"][1]["type"], "Ready");
        assert_eq!(status["conditions"][1]["status"], "False");
        let arr = status["containerStatuses"].as_array().unwrap();
        assert_eq!(arr[1]["state"]["waiting"]["reason"], "ContainerCreating");
        // Waiting container has no containerID.
        assert!(arr[1].get("containerID").is_none());
    }

    // ── M0.3 cluster-DNS: service_aliases_for_pod (pure, no podman/store) ─

    /// Build a `(ResourceKey, Value)` Service entry for the alias tests.
    fn svc(name: &str, namespace: &str, selector: Value) -> (ResourceKey, Value) {
        let key = ResourceKey::namespaced("", "v1", "Service", namespace, name);
        let value = json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": { "name": name },
            "spec": { "selector": selector },
        });
        (key, value)
    }

    /// The exact Service that stopped Flux: `port: 80` -> `targetPort: http`
    /// (9090). The alias path resolves the name to the POD IP, so a client
    /// using the service port connects to a port nothing listens on. The
    /// ClusterIP entry puts it back on the VIP, where the datapath translates.
    #[test]
    fn a_service_maps_to_its_cluster_ip_not_its_pod() {
        let services = vec![(
            ResourceKey::namespaced("", "v1", "Service", "flux-system", "source-controller"),
            serde_json::json!({
                "metadata": {"name": "source-controller", "namespace": "flux-system"},
                "spec": {"clusterIP": "10.97.0.5",
                         "ports": [{"name":"http","port":80,"targetPort":"http"}]}
            }),
        )];
        let hosts = Kubelet::service_cluster_ip_hosts("flux-system", &services, "cluster.local");
        assert!(
            hosts
                .contains(&"source-controller.flux-system.svc.cluster.local:10.97.0.5".to_string()),
            "got: {hosts:?}"
        );
        // Same-namespace pods reach it by the bare name too.
        assert!(
            hosts.contains(&"source-controller:10.97.0.5".to_string()),
            "got: {hosts:?}"
        );
    }

    /// A headless Service must NOT be overridden — its contract is "resolve to
    /// the pod IPs", which is exactly what the alias path already provides.
    /// Writing a hosts entry here would break the one case aliases get right.
    #[test]
    fn a_headless_service_is_left_to_the_alias_path() {
        let services = vec![(
            ResourceKey::namespaced("", "v1", "Service", "default", "headless"),
            serde_json::json!({
                "metadata": {"name": "headless", "namespace": "default"},
                "spec": {"clusterIP": "None"}
            }),
        )];
        assert!(
            Kubelet::service_cluster_ip_hosts("default", &services, "cluster.local").is_empty()
        );
    }

    /// A Service in ANOTHER namespace is reachable by its qualified forms and
    /// NOT by the bare name — the bare form belongs to the pod's own namespace,
    /// and claiming it cluster-wide would shadow a local Service of the same
    /// name with a foreign address.
    #[test]
    fn a_foreign_namespace_service_does_not_claim_the_bare_name() {
        let services = vec![(
            ResourceKey::namespaced("", "v1", "Service", "other", "api"),
            serde_json::json!({
                "metadata": {"name": "api", "namespace": "other"},
                "spec": {"clusterIP": "10.97.0.9"}
            }),
        )];
        let hosts = Kubelet::service_cluster_ip_hosts("default", &services, "cluster.local");
        assert!(
            hosts.contains(&"api.other:10.97.0.9".to_string()),
            "got: {hosts:?}"
        );
        assert!(hosts.contains(&"api.other.svc.cluster.local:10.97.0.9".to_string()));
        assert!(
            !hosts.contains(&"api:10.97.0.9".to_string()),
            "a foreign Service must not claim the bare name: {hosts:?}"
        );
    }

    #[test]
    fn service_aliases_single_match_emits_three_forms_in_order() {
        let pod = json!({"metadata": {"labels": {"app": "web"}}});
        let services = vec![svc("web", "default", json!({"app": "web"}))];
        let aliases = Kubelet::service_aliases_for_pod(&pod, "default", &services, "cluster.local");
        // Exactly the three forms, sorted+deduped.
        assert_eq!(
            aliases,
            vec![
                "web".to_string(),
                "web.default".to_string(),
                "web.default.svc.cluster.local".to_string(),
            ]
        );
    }

    #[test]
    fn service_aliases_excludes_non_matching_selector() {
        // Core "excluding non-matching selectors" assertion: a pod labeled
        // app=web with two Services (web→app=web, db→app=db) earns ONLY
        // web's three aliases, none from db.
        let pod = json!({"metadata": {"labels": {"app": "web"}}});
        let services = vec![
            svc("web", "default", json!({"app": "web"})),
            svc("db", "default", json!({"app": "db"})),
        ];
        let aliases = Kubelet::service_aliases_for_pod(&pod, "default", &services, "cluster.local");
        assert_eq!(
            aliases,
            vec![
                "web".to_string(),
                "web.default".to_string(),
                "web.default.svc.cluster.local".to_string(),
            ]
        );
        assert!(
            !aliases.iter().any(|a| a.starts_with("db")),
            "db's aliases must be excluded: {aliases:?}"
        );
    }

    #[test]
    fn service_aliases_multiple_matches_union_sorted_deduped() {
        // A pod matching BOTH `web` (app=web) and `frontend` (tier=web)
        // earns the union of each Service's three aliases, sorted+deduped.
        let pod = json!({"metadata": {"labels": {"app": "web", "tier": "web"}}});
        let services = vec![
            svc("web", "default", json!({"app": "web"})),
            svc("frontend", "default", json!({"tier": "web"})),
        ];
        let aliases = Kubelet::service_aliases_for_pod(&pod, "default", &services, "cluster.local");
        assert_eq!(
            aliases,
            vec![
                "frontend".to_string(),
                "frontend.default".to_string(),
                "frontend.default.svc.cluster.local".to_string(),
                "web".to_string(),
                "web.default".to_string(),
                "web.default.svc.cluster.local".to_string(),
            ]
        );
    }

    #[test]
    fn service_aliases_empty_selector_contributes_nothing() {
        // A Service with an empty selector → matches_labels false → zero
        // aliases (K8s empty-selector-matches-nothing convention).
        let pod = json!({"metadata": {"labels": {"app": "web"}}});
        let services = vec![svc("web", "default", json!({}))];
        let aliases = Kubelet::service_aliases_for_pod(&pod, "default", &services, "cluster.local");
        assert!(
            aliases.is_empty(),
            "empty selector → no aliases: {aliases:?}"
        );
    }

    #[test]
    fn service_aliases_absent_selector_contributes_nothing() {
        // A Service with no spec.selector at all → service_selector None →
        // skipped → zero aliases.
        let pod = json!({"metadata": {"labels": {"app": "web"}}});
        let key = ResourceKey::namespaced("", "v1", "Service", "default", "web");
        let value = json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": { "name": "web" }, "spec": { "ports": [{ "port": 80 }] }
        });
        let services = vec![(key, value)];
        let aliases = Kubelet::service_aliases_for_pod(&pod, "default", &services, "cluster.local");
        assert!(
            aliases.is_empty(),
            "absent selector → no aliases: {aliases:?}"
        );
    }

    #[test]
    fn service_aliases_pod_without_labels_gets_none() {
        let pod = json!({"metadata": {"name": "p"}});
        let services = vec![svc("web", "default", json!({"app": "web"}))];
        let aliases = Kubelet::service_aliases_for_pod(&pod, "default", &services, "cluster.local");
        assert!(
            aliases.is_empty(),
            "no labels → no Service matches: {aliases:?}"
        );
    }

    #[test]
    fn service_aliases_threads_pod_namespace() {
        // The <ns> segment uses the POD's namespace, not a hard-coded
        // default. A pod in `prod` earns web.prod + web.prod.svc.*.
        let pod = json!({"metadata": {"labels": {"app": "web"}}});
        let services = vec![svc("web", "prod", json!({"app": "web"}))];
        let aliases = Kubelet::service_aliases_for_pod(&pod, "prod", &services, "cluster.local");
        assert_eq!(
            aliases,
            vec![
                "web".to_string(),
                "web.prod".to_string(),
                "web.prod.svc.cluster.local".to_string(),
            ]
        );
    }

    #[test]
    fn service_aliases_threads_custom_cluster_domain() {
        let pod = json!({"metadata": {"labels": {"app": "web"}}});
        let services = vec![svc("web", "default", json!({"app": "web"}))];
        let aliases =
            Kubelet::service_aliases_for_pod(&pod, "default", &services, "engenho.internal");
        assert_eq!(
            aliases,
            vec![
                "web".to_string(),
                "web.default".to_string(),
                "web.default.svc.engenho.internal".to_string(),
            ]
        );
    }

    #[test]
    fn service_aliases_default_cluster_domain_is_cluster_local() {
        // Wiring sanity: the production call uses DEFAULT_CLUSTER_DOMAIN.
        let pod = json!({"metadata": {"labels": {"app": "web"}}});
        let services = vec![svc("web", "default", json!({"app": "web"}))];
        let aliases =
            Kubelet::service_aliases_for_pod(&pod, "default", &services, DEFAULT_CLUSTER_DOMAIN);
        assert!(aliases.contains(&"web.default.svc.cluster.local".to_string()));
    }
}

// =====================================================================
// THE HTTP SURFACE'S PRODUCER
// =====================================================================

/// The real kubelet behind [`crate::server::KubeletServer`].
///
/// ★ THIS IMPL IS THE POINT. `KubeletApi` shipped with a trait, a router
/// and a FakeApi in its own test module, and NO production implementor —
/// so :10250 existed as a type and not as a port. That shape (a type, a
/// backend, and no producer) has now been the root of four separate gaps
/// in this codebase; it defeats grep, because every symbol it names is
/// present and every test is green.
#[async_trait::async_trait]
impl crate::server::KubeletApi for Kubelet {
    async fn container_logs(
        &self,
        namespace: &str,
        pod: &str,
        container: &str,
        opts: &crate::backend::LogOptions,
    ) -> Result<String, String> {
        let id = self
            .container_id_of(namespace, pod, container)
            .await
            .ok_or_else(|| Self::no_such_container(namespace, pod, container))?;
        self.backend
            .logs(&id, opts)
            .await
            .map_err(|e| e.to_string())
    }

    async fn pods(&self) -> Value {
        self.pod_list(false).await
    }

    async fn running_pods(&self) -> Value {
        self.pod_list(true).await
    }

    async fn exec(
        &self,
        namespace: &str,
        pod: &str,
        container: &str,
        argv: &[String],
    ) -> Result<crate::backend::ExecOutcome, String> {
        let id = self
            .container_id_of(namespace, pod, container)
            .await
            .ok_or_else(|| Self::no_such_container(namespace, pod, container))?;
        self.backend
            .exec(&id, argv)
            .await
            .map_err(|e| e.to_string())
    }
}

impl Kubelet {
    /// The message for a container this kubelet is not running.
    ///
    /// Names all three parts: on a multi-node cluster the overwhelmingly
    /// common cause is asking the WRONG kubelet, and a bare "not found"
    /// sends the operator to look for a deleted pod instead.
    fn no_such_container(namespace: &str, pod: &str, container: &str) -> String {
        format!("no container {container:?} of pod {namespace}/{pod} is running on this node")
    }

    /// Resolve (namespace, pod, container) → the backend container id.
    ///
    /// Looks in app containers first, then init containers: an init
    /// container's logs are exactly what an operator wants while a pod is
    /// stuck Pending, and that is the moment the app container has no id.
    async fn container_id_of(&self, namespace: &str, pod: &str, container: &str) -> Option<String> {
        let key = ResourceKey::namespaced("", "v1", "Pod", namespace, pod);
        let local = self.local.lock().await;
        let entry = local.get(&key)?;
        entry
            .containers
            .get(container)
            .or_else(|| entry.init_containers.get(container))
            .map(|r| r.container_id.clone())
    }

    /// The `v1.PodList` this kubelet is managing.
    ///
    /// `running_only` filters to pods with at least one running container —
    /// upstream's `/runningpods/` — which is a DIFFERENT question from
    /// `/pods` and is why both endpoints exist.
    ///
    /// The pod bodies come from the store rather than being reconstructed
    /// here: a second renderer of a Pod is a second thing to drift.
    async fn pod_list(&self, running_only: bool) -> Value {
        let keys: Vec<ResourceKey> = {
            let local = self.local.lock().await;
            local
                .iter()
                .filter(|(_, p)| {
                    !running_only || p.containers.values().any(|c| !c.container_id.is_empty())
                })
                .map(|(k, _)| k.clone())
                .collect()
        };

        let mut items = Vec::new();
        for key in keys {
            if let Some(v) = self.store.get(&key).await {
                if running_only {
                    let running = v
                        .get("status")
                        .and_then(|s| s.get("containerStatuses"))
                        .and_then(Value::as_array)
                        .is_some_and(|cs| {
                            cs.iter()
                                .any(|c| c.get("state").and_then(|st| st.get("running")).is_some())
                        });
                    if !running {
                        continue;
                    }
                }
                items.push(v);
            }
        }
        json!({ "kind": "PodList", "apiVersion": "v1", "items": items })
    }
}
