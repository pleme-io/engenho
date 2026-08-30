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
            last_lease_renewal: Mutex::new(None),
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
    ) -> Result<Vec<(String, ContainerSpec)>, KubeletError> {
        Self::extract_container_specs(namespace, name, pod, "containers", false)
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
    ) -> Result<Vec<(String, ContainerSpec)>, KubeletError> {
        Self::extract_container_specs(namespace, name, pod, "initContainers", true)
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
    fn resolve_env_entry(
        namespace: &str,
        pod_name: &str,
        pod: &Value,
        entry: &Value,
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

        // Everything else is a source we cannot serve yet. Name the source
        // rather than emitting a generic message: the operator's next
        // question is always "which one".
        let source = ["secretKeyRef", "configMapKeyRef", "resourceFieldRef"]
            .into_iter()
            .find(|k| from.get(*k).is_some())
            .unwrap_or("unknown source");
        Err(invalid(format!(
            "env {key}: valueFrom.{source} is not supported yet — refusing rather than \
             starting the container without it"
        )))
    }

    fn extract_container_specs(
        namespace: &str,
        name: &str,
        pod: &Value,
        spec_key: &str,
        optional: bool,
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
            let env = match c.get("env").and_then(|e| e.as_array()) {
                Some(arr) => {
                    let mut map = BTreeMap::new();
                    for entry in arr {
                        let (k, v) = Self::resolve_env_entry(namespace, name, pod, entry)?;
                        map.insert(k, v);
                    }
                    map
                }
                None => BTreeMap::new(),
            };
            // command = entrypoint override; args = appended arguments
            // (K8s semantics). The container's run argv is command ++ args.
            let str_array = |key: &str| -> Vec<String> {
                c.get(key)
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|x| x.as_str().map(String::from))
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
                    // Service-name DNS aliases are computed once per pod in
                    // `start_bound_pod` and assigned onto each spec there.
                    network_aliases: Vec::new(),
                    // Volume mounts are resolved once per pod in
                    // `start_bound_pod` (after alias compute, before the start
                    // loop) and stamped onto each spec there. Empty here →
                    // a no-volume pod produces `mounts: vec![]` → identical
                    // argv to before the kubelet-volumes brick.
                    mounts: Vec::new(),
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
        phase: engenho_types::curated_enums::PodPhase,
        statuses: &[ContainerStatusOut],
        pod_ip: Option<&str>,
    ) -> Value {
        Self::build_pod_status_with_init(phase, &[], statuses, pod_ip, true, false)
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
        // Deterministic order: ContainersReady then Ready (stable across writes
        // → NoChange at steady state).
        let mut conditions = vec![
            json!({
                "type": "ContainersReady",
                "status": status_str(containers_ready),
            }),
            json!({
                "type": "Ready",
                "status": status_str(ready),
            }),
        ];
        // Append the Initialized condition ONLY for a pod with init containers.
        // A no-init pod omits it entirely (byte-identical pre-init render).
        if has_init {
            conditions.push(json!({
                "type": "Initialized",
                "status": status_str(initialized),
            }));
        }
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
        }
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
        // Reap the INIT containers too (a pod deleted mid-init, or after a
        // completed init sequence, still has its init containers recorded — an
        // exited init container retains its podman name until removed). stop is
        // a no-op on an already-exited container; remove frees the
        // `<ns>_<pod>_init-<cname>` name. Idempotent (already-gone is success).
        for record in lp.init_containers.values() {
            self.cleanup_container(&record.container_id).await?;
        }
        for record in lp.containers.values() {
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

        // INIT CONTAINERS: if the pod declares any init containers and they
        // haven't all Succeeded yet, the kubelet runs the init sequence FIRST —
        // one init container at a time, in order — and does NOT start any app
        // container this pass. `reconcile_init` drives the sequence + renders
        // status (Pending + initContainerStatuses + Initialized=False). Only
        // once init is Complete does the app-start path below run. A pod with
        // NO init containers returns an empty Vec here, so this whole block is
        // skipped and the app-start path runs BYTE-IDENTICALLY to before the
        // init-container brick (the behavior-preserving guarantee).
        let init_specs = match Self::pod_to_init_container_specs(namespace, &key.name, value) {
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

        let specs = match Self::pod_to_container_specs(namespace, &key.name, value) {
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
        let services = self.store.list("", "v1", "Service", Some(namespace)).await;
        let aliases =
            Self::service_aliases_for_pod(value, namespace, &services, DEFAULT_CLUSTER_DOMAIN);

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
            // (M0.7 kubelet-volumes) Map this container's `volumeMounts[]`
            // against the resolved `volName → MountSource` map onto its
            // `spec.mounts`. A volumeMount naming an unknown volume is an
            // invalid pod (NoSource) — skip the WHOLE pod (never a fake
            // start). A container with no volumeMounts gets `mounts: vec![]`
            // → identical argv to before this brick.
            if let Some(cjson) = Self::container_json(value, &cname) {
                match container_mounts(cjson, &resolved) {
                    Ok(mounts) => spec.mounts = mounts,
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
                    // emptyDir sources resolve to MountSource::NamedVolume;
                    // configMap/secret (HostDir files) are NOT recorded — they
                    // aren't podman named volumes. Idempotent: only set on the
                    // first container start (when the list is still empty).
                    if entry.empty_dir_volumes.is_empty() {
                        entry.empty_dir_volumes = resolved
                            .iter()
                            .filter(|(_, src)| {
                                matches!(src, crate::pod_volume::MountSource::NamedVolume(_))
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
        let services = self.store.list("", "v1", "Service", Some(namespace)).await;
        restart_spec.network_aliases =
            Self::service_aliases_for_pod(value, namespace, &services, DEFAULT_CLUSTER_DOMAIN);
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

        // INIT CONTAINERS: if the pod has init containers and they haven't all
        // Succeeded yet (`!init_complete`), route to the init reconcile —
        // poll/advance the init sequence + render Pending + initContainerStatuses
        // + Initialized=False, NEVER touching the app containers. A pod with no
        // init containers (or one already init_complete) falls through to the
        // app reconcile below, which itself renders initContainerStatuses +
        // Initialized=True alongside the app status once init_complete.
        let init_specs = match Self::pod_to_init_container_specs(namespace, &key.name, value) {
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
        let specs = match Self::pod_to_container_specs(namespace, &key.name, value) {
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
                phase,
                &init_statuses,
                &statuses,
                pod_ip.as_deref(),
                /* initialized */ true,
                /* has_init */ true,
            )
        } else {
            Self::build_pod_status(phase, &statuses, pod_ip.as_deref())
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
        resolved: &BTreeMap<String, MountSource>,
    ) -> Result<ContainerSpec, KubeletError> {
        let mut spec = base.clone();
        spec.network_aliases = aliases.to_vec();
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
        let services = self.store.list("", "v1", "Service", Some(namespace)).await;
        let aliases =
            Self::service_aliases_for_pod(value, namespace, &services, DEFAULT_CLUSTER_DOMAIN);
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
        for (cname, _spec) in init_specs {
            match lp.init_containers.get(cname) {
                None => observations.push(ContainerObservation::waiting(cname)),
                Some(record) => match self.backend.status(&record.container_id).await {
                    Ok(Some(s)) if s.running => observations.push(ContainerObservation::running(
                        cname,
                        &record.container_id,
                        record.restart_count,
                    )),
                    Ok(Some(s)) => observations.push(ContainerObservation::terminated(
                        cname,
                        &record.container_id,
                        s.exit_code.unwrap_or(0),
                        record.restart_count,
                    )),
                    Ok(None) => {
                        // Backend lost this init container out-of-band → treat
                        // as Waiting so it re-starts on the AwaitInit path.
                        observations.push(ContainerObservation::waiting(cname));
                    }
                    Err(e) => {
                        warn!(
                            pod = %key.label(),
                            container = %cname,
                            error = %e,
                            "init container status poll failed; treating as Waiting"
                        );
                        report.objects_skipped += 1;
                        observations.push(ContainerObservation::waiting(cname));
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
                    engenho_types::curated_enums::PodPhase::Failed,
                    &init_statuses,
                    &[],
                    None,
                    /* initialized */ false,
                    /* has_init */ true,
                );
                self.write_pod_status(key, value, &desired, report).await
            }
            crate::lifecycle::InitAction::AwaitInit { index } => {
                // init[index] is the active one. Ensure it's started (start it
                // if not recorded) or restarted (if it Terminated with a
                // restartable exit under the policy).
                let (cname, base_spec) = &init_specs[index];
                let pod_ip = self
                    .advance_active_init(
                        key, value, index, cname, base_spec, &aliases, &resolved, &lp, report,
                    )
                    .await?;

                // Re-read the (possibly just-updated) init records so the
                // rendered initContainerStatuses reflect the freshly-started /
                // restarted container.
                let init_statuses = self.init_statuses_current(key, init_specs).await;
                let desired = Self::build_pod_status_with_init(
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
        resolved: &BTreeMap<String, MountSource>,
        lp: &LocalPod,
        report: &mut ReconcileReport,
    ) -> Result<Option<String>, ControllerError> {
        let spec = match Self::build_init_spec(value, cname, base_spec, aliases, resolved) {
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

    fn pod() -> serde_json::Value {
        json!({
            "metadata": { "name": "pangea-operator-abc", "namespace": "pangea-system",
                          "uid": "11111111-2222-3333-4444-555555555555" },
            "spec": { "nodeName": "cid", "serviceAccountName": "pangea-operator" },
            "status": { "podIP": "10.42.0.7", "hostIP": "192.168.1.10" }
        })
    }

    fn resolve(entry: serde_json::Value) -> Result<(String, String), super::KubeletError> {
        Kubelet::resolve_env_entry("pangea-system", "pangea-operator-abc", &pod(), &entry)
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
    #[test]
    fn an_unsupported_source_refuses_and_names_itself() {
        for source in ["secretKeyRef", "configMapKeyRef", "resourceFieldRef"] {
            let err = resolve(json!({
                "name": "PGPASSWORD",
                "valueFrom": { source: { "name": "pangea-database-app", "key": "password" } }
            }))
            .expect_err("an unresolvable source must not be silently dropped");
            let msg = err.to_string();
            assert!(msg.contains(source), "error must name the source: {msg}");
            assert!(
                msg.contains("PGPASSWORD"),
                "error must name the variable: {msg}"
            );
        }
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
        let (_, v) = Kubelet::resolve_env_entry(
            "n",
            "p",
            &bare,
            &json!({ "name": "POD_IP", "valueFrom": { "fieldRef": { "fieldPath": "status.podIP" } } }),
        )
        .expect("a known-but-unset path is not an error");
        assert_eq!(v, "");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let specs = Kubelet::pod_to_container_specs("default", "p1", &pod).unwrap();
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
        let specs = Kubelet::pod_to_container_specs("ns", "x", &pod).unwrap();
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
        let specs = Kubelet::pod_to_container_specs("default", "p", &pod).unwrap();
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
        let err = Kubelet::pod_to_container_specs("ns", "p", &pod).unwrap_err();
        assert_eq!(err.kind(), "invalid_pod");
    }

    #[test]
    fn pod_to_container_specs_rejects_empty_containers() {
        let pod = json!({"spec": {"containers": []}});
        assert!(Kubelet::pod_to_container_specs("n", "p", &pod).is_err());
    }

    #[test]
    fn pod_to_container_specs_rejects_no_spec() {
        let pod = json!({"metadata": {"name": "p"}});
        assert!(Kubelet::pod_to_container_specs("n", "p", &pod).is_err());
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
        let specs = Kubelet::pod_to_container_specs("ns", "p", &pod).unwrap();
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
        let status = Kubelet::build_pod_status(PodPhase::Running, &statuses, Some("10.42.0.5"));
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
        let status = Kubelet::build_pod_status(PodPhase::Succeeded, &statuses, Some("10.42.0.9"));
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
        let status = Kubelet::build_pod_status(PodPhase::Failed, &statuses, None);
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
        let status = Kubelet::build_pod_status(PodPhase::Running, &statuses, Some("10.0.0.1"));
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
        let status = Kubelet::build_pod_status(PodPhase::Pending, &statuses, None);
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
