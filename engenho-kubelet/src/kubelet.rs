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
    /// Per-container records keyed by the container's logical name.
    containers: BTreeMap<String, ContainerRecord>,
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
    /// MULTI-CONTAINER: today the kubelet runs one [`ContainerSpec`] per
    /// `spec.containers[i]`. INIT CONTAINERS: the pure sequencing interpreter
    /// ([`crate::lifecycle::next_init_action`] +
    /// [`crate::lifecycle::reconcile_pod_phase_with_init`]) is implemented +
    /// unit-tested; the kubelet I/O driver that consumes it (sequential
    /// init-then-app start, `initContainerStatuses`, the `Initialized`
    /// condition) is the next brick. Ephemeral containers remain a documented
    /// no-op. The backend container name is the
    /// deterministic `<ns>_<pod>_<containerName>` join so status/stop/remove
    /// by-name is possible per container.
    ///
    /// Env + command + args are read per container: `command` →
    /// `ContainerSpec.command` (the entrypoint override) followed by `args`
    /// (appended, mirroring K8s where `args` are the entrypoint's arguments).
    fn pod_to_container_specs(
        namespace: &str,
        name: &str,
        pod: &Value,
    ) -> Result<Vec<(String, ContainerSpec)>, KubeletError> {
        let containers = pod
            .get("spec")
            .and_then(|s| s.get("containers"))
            .and_then(|c| c.as_array())
            .ok_or_else(|| KubeletError::InvalidPod {
                pod: format!("{namespace}/{name}"),
                reason: "spec.containers missing".into(),
            })?;
        if containers.is_empty() {
            return Err(KubeletError::InvalidPod {
                pod: format!("{namespace}/{name}"),
                reason: "spec.containers is empty".into(),
            });
        }
        let mut out = Vec::with_capacity(containers.len());
        for (i, c) in containers.iter().enumerate() {
            // Container logical name: spec.containers[i].name, else a
            // positional fallback (matches container_name()'s "main"/index
            // shape so status names round-trip).
            let cname = c
                .get("name")
                .and_then(|n| n.as_str())
                .map(String::from)
                .unwrap_or_else(|| if i == 0 { "main".to_string() } else { format!("container-{i}") });
            let image = c
                .get("image")
                .and_then(|im| im.as_str())
                .ok_or_else(|| KubeletError::InvalidPod {
                    pod: format!("{namespace}/{name}"),
                    reason: format!("spec.containers[{i}].image missing"),
                })?
                .to_string();
            let env = c
                .get("env")
                .and_then(|e| e.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|e| {
                            let n = e.get("name")?.as_str()?.to_string();
                            let v = e.get("value")?.as_str()?.to_string();
                            Some((n, v))
                        })
                        .collect::<BTreeMap<_, _>>()
                })
                .unwrap_or_default();
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
            out.push((
                cname.clone(),
                ContainerSpec {
                    // Backend (podman --name) handle: <ns>_<pod>_<cname>.
                    name: format!("{namespace}_{name}_{cname}"),
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

        let parse_one = |field: &str, kind: ProbeKind| -> Result<Option<(ProbeSpec, ProbeRuntime)>, KubeletError> {
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
        containers.iter().enumerate().find(|(i, c)| {
            let name = c
                .get("name")
                .and_then(|n| n.as_str())
                .map(String::from)
                .unwrap_or_else(|| if *i == 0 { "main".to_string() } else { format!("container-{i}") });
            name == cname
        }).map(|(_, c)| c)
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
    fn build_pod_status(
        phase: engenho_types::curated_enums::PodPhase,
        statuses: &[ContainerStatusOut],
        pod_ip: Option<&str>,
    ) -> Value {
        use engenho_types::curated_enums::PodPhase;
        let phase_str = match phase {
            PodPhase::Pending => "Pending",
            PodPhase::Running => "Running",
            PodPhase::Succeeded => "Succeeded",
            PodPhase::Failed => "Failed",
            PodPhase::Unknown => "Unknown",
        };
        // ContainersReady = phase Running AND all containers ready. A non-running
        // phase (Pending / terminal) is never ready.
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
        let mut status = json!({
            "phase": phase_str,
            "conditions": [
                {
                    "type": "ContainersReady",
                    "status": status_str(containers_ready),
                },
                {
                    "type": "Ready",
                    "status": status_str(ready),
                },
            ],
            "containerStatuses": container_statuses,
        });
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

        // Pre-fetch every referenced ConfigMap/Secret ASYNC, keyed by
        // (kind, name). Only the in-scope source arms need a fetch; deferred
        // arms (hostPath/PVC/projected/downwardAPI) surface their typed error
        // in the pure resolver without a store read.
        let mut fetched: BTreeMap<(String, String), Value> = BTreeMap::new();
        for vol in &volumes {
            // from_volume errors (multi/no source) are re-detected by the pure
            // resolver below; here we only need the (kind, name) to pre-fetch.
            let source = match PodVolumeSource::from_volume(vol) {
                Ok(s) => s,
                Err(_) => continue,
            };
            let (kind, name) = match source {
                PodVolumeSource::ConfigMap { name, .. } => ("ConfigMap", name),
                PodVolumeSource::Secret { name, .. } => ("Secret", name),
                _ => continue,
            };
            if name.is_empty() {
                continue;
            }
            let key = ResourceKey::namespaced("", "v1", kind, namespace, &name);
            if let Some(val) = self.store.get(&key).await {
                fetched.insert((kind.to_string(), name), val);
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
                        },
                    );
                    started_any = true;
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

        // Reconcile the status from the freshly-started set (a partial start
        // shows the started containers Running + the rest Waiting → Pending).
        if started_any {
            let lp = self.local.lock().await.get(key).cloned().unwrap_or_default();
            self.reconcile_running(key, value, &lp, report, soonest_requeue)
                .await?;
        }
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
                let obs = run_handler(spec, &*self.backend, &*self.net_prober, container_id, pod_ip)
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
                let obs = run_handler(spec, &*self.backend, &*self.net_prober, container_id, pod_ip)
                    .await;
                let _ = fold_probe_observation(spec, rt, obs, now);
            }
            readiness_ready = rt.gate_satisfied;
            Self::accumulate_requeue(soonest_requeue, rt.next_due_in(spec, now));
        }

        let mut liveness_needs_restart = false;
        if let Some((spec, rt)) = probes.liveness.as_mut() {
            if rt.is_due(spec, now) {
                let obs = run_handler(spec, &*self.backend, &*self.net_prober, container_id, pod_ip)
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
        let needs_restart =
            startup_needs_restart || (may_run_liveness && liveness_needs_restart);

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
        let desired = Self::build_pod_status(phase, &statuses, pod_ip.as_deref());
        self.write_pod_status(key, value, &desired, report).await
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
            Some(c) => lp.containers.get(c).ok_or_else(|| KubeletError::InvalidPod {
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
        assert!(aliases.is_empty(), "empty selector → no aliases: {aliases:?}");
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
        assert!(aliases.is_empty(), "absent selector → no aliases: {aliases:?}");
    }

    #[test]
    fn service_aliases_pod_without_labels_gets_none() {
        let pod = json!({"metadata": {"name": "p"}});
        let services = vec![svc("web", "default", json!({"app": "web"}))];
        let aliases = Kubelet::service_aliases_for_pod(&pod, "default", &services, "cluster.local");
        assert!(aliases.is_empty(), "no labels → no Service matches: {aliases:?}");
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
