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

use async_trait::async_trait;
use engenho_controllers::{
    Controller, ControllerError, ReconcileOutcome, ReconcileReport,
    dns::DEFAULT_CLUSTER_DOMAIN,
    selector::{matches_labels, service_selector},
    status::{resource_version_of, write_status_cas},
};
use engenho_store::{StoreMesh, resource::ResourceKey};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::backend::{ContainerRuntime, ContainerSpec, ContainerStatus};
use crate::error::KubeletError;

/// What the kubelet remembers about a Pod it started on this node.
/// Keyed in [`Kubelet::local`] by the Pod's typed [`ResourceKey`] so
/// delete-cleanup can reconstruct the Pod identity unambiguously (the
/// `format!("{ns}_{name}")` container name is a lossy join — a `_` in
/// either part can't be split back, so it MUST NOT be used as a key).
#[derive(Clone, Debug)]
struct LocalPod {
    /// Opaque backend handle returned by `backend.start`.
    container_id: String,
}

/// Per-node kubelet. Implements [`Controller`] so it slots into the
/// standard [`engenho_controllers::ControllerRuntime`] + benefits from
/// `WatchDriver` event-driven wakeup.
pub struct Kubelet {
    store: Arc<StoreMesh>,
    backend: Arc<dyn ContainerRuntime>,
    node_name: String,
    /// Bookkeeping for every Pod we started, keyed by its typed
    /// [`ResourceKey`]. Persists for the kubelet's process lifetime; on
    /// restart we re-derive by re-creating (a managed bound Pod with no
    /// live container converges back to running).
    local: Mutex<BTreeMap<ResourceKey, LocalPod>>,
}

impl Kubelet {
    /// Construct a kubelet for `node_name`.
    #[must_use]
    pub fn new(
        store: Arc<StoreMesh>,
        backend: Arc<dyn ContainerRuntime>,
        node_name: impl Into<String>,
    ) -> Self {
        Self {
            store,
            backend,
            node_name: node_name.into(),
            local: Mutex::new(BTreeMap::new()),
        }
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

    /// Extract the first container's spec from a Pod manifest.
    fn pod_to_container_spec(
        namespace: &str,
        name: &str,
        pod: &Value,
    ) -> Result<ContainerSpec, KubeletError> {
        let containers = pod
            .get("spec")
            .and_then(|s| s.get("containers"))
            .and_then(|c| c.as_array())
            .ok_or_else(|| KubeletError::InvalidPod {
                pod: format!("{namespace}/{name}"),
                reason: "spec.containers missing".into(),
            })?;
        let first = containers.first().ok_or_else(|| KubeletError::InvalidPod {
            pod: format!("{namespace}/{name}"),
            reason: "spec.containers is empty".into(),
        })?;
        let image = first
            .get("image")
            .and_then(|i| i.as_str())
            .ok_or_else(|| KubeletError::InvalidPod {
                pod: format!("{namespace}/{name}"),
                reason: "spec.containers[0].image missing".into(),
            })?
            .to_string();
        let env = first
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
        let command = first
            .get("command")
            .and_then(|c| c.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|c| c.as_str().map(String::from))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Ok(ContainerSpec {
            // Container name = namespace_name to avoid collisions. This is
            // the backend-facing (podman --name) handle, NOT a local key.
            name: format!("{namespace}_{name}"),
            image,
            env,
            command,
            // Service-name DNS aliases are computed separately in
            // `start_bound_pod` (it needs the live Service list from the
            // store, which this pure manifest→spec map deliberately does
            // not touch) and assigned onto the returned spec there.
            network_aliases: Vec::new(),
        })
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

    /// First container's logical name, as it appears in
    /// `status.containerStatuses[0].name` — `spec.containers[0].name`
    /// when present, else falls back to `"main"`.
    fn container_name(pod: &Value) -> String {
        pod.get("spec")
            .and_then(|s| s.get("containers"))
            .and_then(|c| c.as_array())
            .and_then(|arr| arr.first())
            .and_then(|c| c.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or("main")
            .to_string()
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

    /// Map a terminated container's exit code → terminal Pod phase.
    /// `Some(0)` ⇒ `Succeeded`; any other code (or `None`, a terminated
    /// container with no recorded code — defensively unhealthy) ⇒
    /// `Failed`.
    fn terminal_phase_for(exit_code: Option<i32>) -> &'static str {
        match exit_code {
            Some(0) => "Succeeded",
            _ => "Failed",
        }
    }

    /// Desired `status` for a Pod whose container the backend reports as
    /// running. Carries `phase:Running`, `Ready=True`, the per-container
    /// running state, and (when known) `podIP`.
    ///
    /// The field set is stable tick-to-tick so the idempotent-skip in
    /// [`write_status_cas`] (full equality of live-vs-desired) yields
    /// `NoChange` at steady state — no store write, no watch storm.
    fn running_status(container_name: &str, s: &ContainerStatus) -> Value {
        let mut status = json!({
            "phase": "Running",
            "conditions": [{ "type": "Ready", "status": "True" }],
            "containerStatuses": [{
                "name": container_name,
                "ready": true,
                "state": { "running": {} },
                "containerID": s.container_id,
            }],
        });
        if let Some(ip) = &s.pod_ip {
            status["podIP"] = Value::String(ip.clone());
        }
        status
    }

    /// Desired `status` for a Pod whose container the backend reports as
    /// terminated. Maps `exit_code → phase`, sets `Ready=False`, and
    /// records the per-container terminated state. `podIP` is retained
    /// when known (K8s keeps a terminated Pod's last IP) — and retaining
    /// it ALSO keeps the field set stable across the Running→terminal
    /// transition, so the post-merge live status equals this desired one
    /// and the next tick is a `NoChange` (no hot loop).
    fn terminated_status(container_name: &str, s: &ContainerStatus) -> Value {
        let phase = Self::terminal_phase_for(s.exit_code);
        let reason = if s.exit_code == Some(0) {
            "Completed"
        } else {
            "Error"
        };
        let mut status = json!({
            "phase": phase,
            "conditions": [{ "type": "Ready", "status": "False" }],
            "containerStatuses": [{
                "name": container_name,
                "ready": false,
                "state": { "terminated": {
                    "exitCode": s.exit_code.unwrap_or(0),
                    "reason": reason,
                }},
                "containerID": s.container_id,
            }],
        });
        if let Some(ip) = &s.pod_ip {
            status["podIP"] = Value::String(ip.clone());
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
            match self.cleanup_container(&lp.container_id).await {
                Ok(()) => {
                    self.local.lock().await.remove(&key);
                    report.objects_changed += 1;
                    debug!(
                        pod = %key.label(),
                        container = %lp.container_id,
                        "kubelet cleaned up orphaned container"
                    );
                }
                Err(e) => {
                    // Leave the local entry so the next tick retries — no
                    // silent leak.
                    warn!(
                        pod = %key.label(),
                        container = %lp.container_id,
                        error = %e,
                        "kubelet cleanup failed; will retry next tick"
                    );
                    report.objects_skipped += 1;
                }
            }
        }

        // ── (B)+(C) Start + running-status reconciliation over bound set ──
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
                    self.start_bound_pod(key, value, &mut report).await?;
                }
                Some(lp) => {
                    // (C) Already started → poll + reconcile running status.
                    self.reconcile_running(key, value, &lp, &mut report).await?;
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
        Ok(report.into())
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

    /// (B) Start a bound Pod's container + write its initial Running
    /// status via CAS, inserting the local bookkeeping entry on success.
    async fn start_bound_pod(
        &self,
        key: &ResourceKey,
        value: &Value,
        report: &mut ReconcileReport,
    ) -> Result<(), ControllerError> {
        let namespace = key.namespace.as_deref().unwrap_or("default");
        let mut spec = match Self::pod_to_container_spec(namespace, &key.name, value) {
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

        // (M0.3 cluster-DNS) Compute the Service-name aliases this Pod
        // earns BEFORE building the run argv — `--network-alias` is a
        // `podman run` flag and cannot be added to a running container. LIST
        // Services in the pod's namespace from the store (the store-using
        // step — mirrors EndpointsController::tick's Service LIST) and reuse
        // the EndpointsController selector predicate to compute matches. A
        // Service created AFTER this pod starts won't alias it until the pod
        // is recreated (accepted M0.3 limitation; M0.4 engenho-dns removes
        // it). aardvark-dns then resolves these names to the pod's IP.
        let services = self.store.list("", "v1", "Service", Some(namespace)).await;
        spec.network_aliases =
            Self::service_aliases_for_pod(value, namespace, &services, DEFAULT_CLUSTER_DOMAIN);

        debug!(
            pod = %key.label(),
            image = %spec.image,
            backend = self.backend.name(),
            aliases = spec.network_aliases.len(),
            "kubelet starting container"
        );

        match self.backend.start(&spec).await {
            Ok(status) => {
                self.local.lock().await.insert(
                    key.clone(),
                    LocalPod {
                        container_id: status.container_id.clone(),
                    },
                );
                let desired = Self::running_status(&Self::container_name(value), &status);
                self.write_pod_status(key, value, &desired, report).await?;
                Ok(())
            }
            Err(e) => {
                warn!(
                    pod = %key.label(),
                    error = %e,
                    "container start failed; pod remains pending"
                );
                report.objects_skipped += 1;
                Ok(())
            }
        }
    }

    /// (C) Poll the backend for a started Pod's container + reconcile the
    /// observed running state into `status`.
    async fn reconcile_running(
        &self,
        key: &ResourceKey,
        value: &Value,
        lp: &LocalPod,
        report: &mut ReconcileReport,
    ) -> Result<(), ControllerError> {
        let container_name = Self::container_name(value);
        match self.backend.status(&lp.container_id).await {
            Ok(Some(s)) if s.running => {
                // Still running — do NOT restart (membership guard). The
                // local-map membership is the "never spuriously restart a
                // still-running pod" invariant; we never call start here.
                let desired = Self::running_status(&container_name, &s);
                self.write_pod_status(key, value, &desired, report).await
            }
            Ok(Some(s)) => {
                // Terminated. Map exit_code → phase. Leave the local entry
                // so later ticks keep reporting the terminal phase (via
                // idempotent-skip) and never restart.
                let desired = Self::terminated_status(&container_name, &s);
                self.write_pod_status(key, value, &desired, report).await
            }
            Ok(None) => {
                // The backend has no record though we hold a container_id:
                // the container vanished out-of-band (host reboot, manual
                // podman rm). Clear the local entry so the next tick
                // re-creates it — a managed bound Pod with no live
                // container converges back to running.
                self.local.lock().await.remove(key);
                debug!(
                    pod = %key.label(),
                    container = %lp.container_id,
                    "backend lost the container; clearing local entry to re-create next tick"
                );
                report.objects_changed += 1;
                Ok(())
            }
            Err(e) => {
                // Backend inspection failed — retain the local entry,
                // count as skipped, retry next tick. Never silent.
                warn!(
                    pod = %key.label(),
                    container = %lp.container_id,
                    error = %e,
                    "container status poll failed; retrying next tick"
                );
                report.objects_skipped += 1;
                Ok(())
            }
        }
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
    fn pod_to_container_spec_extracts_image_and_env() {
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
        let spec = Kubelet::pod_to_container_spec("default", "p1", &pod).unwrap();
        assert_eq!(spec.name, "default_p1");
        assert_eq!(spec.image, "nginx:1.27");
        assert_eq!(spec.env.get("FOO").map(String::as_str), Some("bar"));
        assert!(spec.command.is_empty());
    }

    #[test]
    fn pod_to_container_spec_extracts_command_when_present() {
        let pod = json!({
            "spec": {
                "containers": [{
                    "image": "alpine",
                    "command": ["sleep", "3600"]
                }]
            }
        });
        let spec = Kubelet::pod_to_container_spec("ns", "x", &pod).unwrap();
        assert_eq!(spec.command, vec!["sleep", "3600"]);
    }

    #[test]
    fn pod_to_container_spec_rejects_missing_image() {
        let pod = json!({"spec": {"containers": [{}]}});
        let err = Kubelet::pod_to_container_spec("ns", "p", &pod).unwrap_err();
        assert_eq!(err.kind(), "invalid_pod");
    }

    #[test]
    fn pod_to_container_spec_rejects_empty_containers() {
        let pod = json!({"spec": {"containers": []}});
        assert!(Kubelet::pod_to_container_spec("n", "p", &pod).is_err());
    }

    #[test]
    fn pod_to_container_spec_rejects_no_spec() {
        let pod = json!({"metadata": {"name": "p"}});
        assert!(Kubelet::pod_to_container_spec("n", "p", &pod).is_err());
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
    fn container_name_reads_first_container_or_defaults() {
        let named = json!({"spec": {"containers": [{"name": "web", "image": "i"}]}});
        assert_eq!(Kubelet::container_name(&named), "web");
        let unnamed = json!({"spec": {"containers": [{"image": "i"}]}});
        assert_eq!(Kubelet::container_name(&unnamed), "main");
        let empty = json!({"spec": {}});
        assert_eq!(Kubelet::container_name(&empty), "main");
    }

    #[test]
    fn terminal_phase_for_maps_exit_code() {
        assert_eq!(Kubelet::terminal_phase_for(Some(0)), "Succeeded");
        assert_eq!(Kubelet::terminal_phase_for(Some(1)), "Failed");
        assert_eq!(Kubelet::terminal_phase_for(Some(137)), "Failed");
        assert_eq!(Kubelet::terminal_phase_for(None), "Failed");
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
    fn running_status_carries_ready_and_pod_ip() {
        let s = ContainerStatus::running("fake-1", "10.42.0.5");
        let status = Kubelet::running_status("web", &s);
        assert_eq!(status["phase"], "Running");
        assert_eq!(status["podIP"], "10.42.0.5");
        assert_eq!(status["conditions"][0]["type"], "Ready");
        assert_eq!(status["conditions"][0]["status"], "True");
        assert_eq!(status["containerStatuses"][0]["name"], "web");
        assert_eq!(status["containerStatuses"][0]["ready"], true);
        assert!(status["containerStatuses"][0]["state"]["running"].is_object());
        assert_eq!(status["containerStatuses"][0]["containerID"], "fake-1");
    }

    #[test]
    fn terminated_status_maps_exit_zero_to_succeeded() {
        let s = ContainerStatus {
            container_id: "fake-2".into(),
            running: false,
            pod_ip: Some("10.42.0.9".into()),
            exit_code: Some(0),
        };
        let status = Kubelet::terminated_status("web", &s);
        assert_eq!(status["phase"], "Succeeded");
        assert_eq!(status["conditions"][0]["status"], "False");
        let term = &status["containerStatuses"][0]["state"]["terminated"];
        assert_eq!(term["exitCode"], 0);
        assert_eq!(term["reason"], "Completed");
        // Terminated pods retain their last IP (also keeps the field set
        // stable across the Running→terminal transition → no hot loop).
        assert_eq!(status["podIP"], "10.42.0.9");
    }

    #[test]
    fn terminated_status_maps_nonzero_to_failed() {
        let s = ContainerStatus {
            container_id: "fake-3".into(),
            running: false,
            pod_ip: None,
            exit_code: Some(137),
        };
        let status = Kubelet::terminated_status("web", &s);
        assert_eq!(status["phase"], "Failed");
        let term = &status["containerStatuses"][0]["state"]["terminated"];
        assert_eq!(term["exitCode"], 137);
        assert_eq!(term["reason"], "Error");
        assert!(status.get("podIP").is_none());
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
