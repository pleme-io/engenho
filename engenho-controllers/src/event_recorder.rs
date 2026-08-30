//! EVENT RECORDING — how the cluster explains itself.
//!
//! ★ WHY THIS EXISTS. `Event` has always been in the catalog: storable,
//! listable, watchable like any other kind. Nothing ever WROTE one. The
//! consequence is not cosmetic — `kubectl describe pod` shows an empty
//! Events section, `kubectl get events` is empty, and every "why did this
//! fail" question has no in-cluster answer at all.
//!
//! Measured on cid 2026-08-29: diagnosing a pod that had restarted 160
//! times required reading podman's container list and the daemon's own log
//! file directly, because the cluster could say *that* it had restarted and
//! never *why*. A distribution whose failures are only legible by sshing to
//! the node is not substitutable for one whose are not.
//!
//! ★ THE REASON VOCABULARY IS UPSTREAM'S, NOT OURS. Every string in
//! [`reason`] is one kubectl, operators, alerting rules and a decade of
//! runbooks already recognise. Inventing `PodDidNotStart` where upstream
//! says `Failed` would produce events that are technically present and
//! practically useless — worse than none, because they look right.
//!
//! ★ ONE TYPE, NOT FREE STRINGS. An event is emitted from the kubelet, the
//! scheduler and several controllers; a `&str` reason at each site drifts
//! within a release. The closed [`Reason`] enum makes a typo a compile
//! error and lets the vocabulary be diffed against upstream in one place.

use engenho_store::{ResourceKey, ResourceValue};
use serde_json::json;

/// Event severity — upstream's `type` field, which has exactly two values.
///
/// Not a bool: `Normal`/`Warning` are what appears on the wire and in
/// `kubectl get events --field-selector type=Warning`, and a bool would
/// have to be translated at every boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Normal,
    Warning,
}

impl Severity {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "Normal",
            Self::Warning => "Warning",
        }
    }
}

/// The upstream reason vocabulary, closed.
///
/// Only reasons engenho can actually emit today are listed. A reason with
/// no emitting site would be a promise the cluster does not keep — the same
/// "advertised with zero behaviour" pattern this codebase otherwise avoids.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    // ── kubelet, pod lifecycle ──
    /// The image was pulled successfully.
    Pulled,
    /// A container was created from the image.
    Created,
    /// A container was started.
    Started,
    /// A container terminated unexpectedly, or could not be started.
    Failed,
    /// A container was killed (delete, probe failure, restart).
    Killing,
    /// A liveness/readiness/startup probe failed.
    Unhealthy,
    /// The container is in a crash-restart backoff.
    BackOff,
    // ── scheduler ──
    /// The pod was assigned to a node.
    Scheduled,
    /// No node satisfied the pod's requirements.
    FailedScheduling,
    // ── workload controllers ──
    /// A ReplicaSet was scaled up or down.
    ScalingReplicaSet,
    /// A ReplicaSet created a pod.
    SuccessfulCreate,
    /// A ReplicaSet deleted a pod.
    SuccessfulDelete,
    /// A PersistentVolumeClaim was bound.
    ProvisioningSucceeded,
}

impl Reason {
    /// The exact upstream string. Diffed in one place, not per call site.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pulled => "Pulled",
            Self::Created => "Created",
            Self::Started => "Started",
            Self::Failed => "Failed",
            Self::Killing => "Killing",
            Self::Unhealthy => "Unhealthy",
            Self::BackOff => "BackOff",
            Self::Scheduled => "Scheduled",
            Self::FailedScheduling => "FailedScheduling",
            Self::ScalingReplicaSet => "ScalingReplicaSet",
            Self::SuccessfulCreate => "SuccessfulCreate",
            Self::SuccessfulDelete => "SuccessfulDelete",
            Self::ProvisioningSucceeded => "ProvisioningSucceeded",
        }
    }

    /// The severity upstream pairs with this reason.
    ///
    /// Derived rather than passed in: a `Warning`/`Normal` argument at each
    /// call site is exactly where drift appears, and upstream's pairing is
    /// fixed per reason.
    #[must_use]
    pub fn severity(self) -> Severity {
        match self {
            Self::Failed | Self::Unhealthy | Self::BackOff | Self::FailedScheduling => {
                Severity::Warning
            }
            _ => Severity::Normal,
        }
    }
}

/// What the event is about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvolvedObject {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub uid: Option<String>,
}

/// A single recorded event, ready to be written.
#[derive(Debug, Clone)]
pub struct EventRecord {
    pub involved: InvolvedObject,
    pub reason: Reason,
    pub message: String,
    /// The component that observed it (`kubelet`, `default-scheduler`,
    /// `replicaset-controller`) — upstream's `source.component`.
    pub component: String,
    /// Frozen at the boundary by the caller. The clock is not a replicated
    /// input, the same law `deletion_timestamp` obeys.
    pub timestamp: String,
}

impl EventRecord {
    /// The object's name in the events namespace.
    ///
    /// Upstream appends a nanosecond-ish suffix so repeated events about
    /// one object do not collide. engenho derives the suffix from the
    /// frozen timestamp instead of reading a clock here, keeping the name
    /// deterministic for a given (object, reason, timestamp) — which is
    /// what makes an event write replay-safe on a Raft follower.
    #[must_use]
    pub fn event_name(&self) -> String {
        let stamp: String = self
            .timestamp
            .chars()
            .filter(char::is_ascii_alphanumeric)
            .collect();
        format!("{}.{}.{}", self.involved.name, self.reason.as_str(), stamp)
    }

    /// The store key this event is written at.
    #[must_use]
    pub fn key(&self) -> ResourceKey {
        let ns = self.involved.namespace.as_deref().unwrap_or("default");
        ResourceKey::namespaced("", "v1", "Event", ns, &self.event_name())
    }

    /// The `v1.Event` object.
    #[must_use]
    pub fn to_value(&self) -> ResourceValue {
        let ns = self.involved.namespace.as_deref().unwrap_or("default");
        json!({
            "apiVersion": "v1",
            "kind": "Event",
            "metadata": { "name": self.event_name(), "namespace": ns },
            "involvedObject": {
                "apiVersion": self.involved.api_version,
                "kind": self.involved.kind,
                "namespace": self.involved.namespace,
                "name": self.involved.name,
                "uid": self.involved.uid,
            },
            "reason": self.reason.as_str(),
            "message": self.message,
            "type": self.reason.severity().as_str(),
            "source": { "component": self.component },
            "firstTimestamp": self.timestamp,
            "lastTimestamp": self.timestamp,
            "eventTime": self.timestamp,
            "count": 1,
            "reportingComponent": self.component,
            "reportingInstance": "",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(reason: Reason) -> EventRecord {
        EventRecord {
            involved: InvolvedObject {
                api_version: "v1".into(),
                kind: "Pod".into(),
                namespace: Some("default".into()),
                name: "nginx".into(),
                uid: Some("abc".into()),
            },
            reason,
            message: "because".into(),
            component: "kubelet".into(),
            timestamp: "2026-08-29T21:00:00Z".into(),
        }
    }

    #[test]
    fn the_reason_vocabulary_is_upstreams_exact_strings() {
        // kubectl, alerting rules and a decade of runbooks match on these.
        // A near-miss is worse than nothing: it looks right and matches
        // nothing.
        assert_eq!(Reason::Pulled.as_str(), "Pulled");
        assert_eq!(Reason::BackOff.as_str(), "BackOff");
        assert_eq!(Reason::FailedScheduling.as_str(), "FailedScheduling");
        assert_eq!(Reason::ScalingReplicaSet.as_str(), "ScalingReplicaSet");
        assert_eq!(Reason::SuccessfulCreate.as_str(), "SuccessfulCreate");
    }

    #[test]
    fn severity_is_derived_from_the_reason_never_passed_in() {
        // A per-call-site Normal/Warning is exactly where drift appears.
        for r in [
            Reason::Failed,
            Reason::Unhealthy,
            Reason::BackOff,
            Reason::FailedScheduling,
        ] {
            assert_eq!(r.severity(), Severity::Warning, "{} must warn", r.as_str());
        }
        for r in [Reason::Pulled, Reason::Started, Reason::Scheduled] {
            assert_eq!(r.severity(), Severity::Normal, "{}", r.as_str());
        }
    }

    #[test]
    fn the_event_object_carries_every_field_kubectl_describe_reads() {
        let v = rec(Reason::Started).to_value();
        assert_eq!(v["kind"], "Event");
        assert_eq!(v["reason"], "Started");
        assert_eq!(v["type"], "Normal");
        assert_eq!(v["message"], "because");
        assert_eq!(v["source"]["component"], "kubelet");
        assert_eq!(v["involvedObject"]["kind"], "Pod");
        assert_eq!(v["involvedObject"]["name"], "nginx");
        assert_eq!(v["involvedObject"]["uid"], "abc");
        // kubectl sorts by these; an absent timestamp renders as <unknown>.
        assert_eq!(v["firstTimestamp"], "2026-08-29T21:00:00Z");
        assert_eq!(v["lastTimestamp"], "2026-08-29T21:00:00Z");
    }

    #[test]
    fn the_event_name_is_deterministic_for_a_frozen_timestamp() {
        // Replay-safety: a Raft follower re-applying the write must derive
        // the identical name, so the name cannot read a clock.
        let a = rec(Reason::Started).event_name();
        let b = rec(Reason::Started).event_name();
        assert_eq!(a, b);
        // And distinct events about one object do not collide.
        assert_ne!(a, rec(Reason::Killing).event_name());
    }

    #[test]
    fn events_are_written_into_the_involved_objects_namespace() {
        // kubectl -n <ns> get events must find it; a cluster-scoped
        // fallback would hide every event for a namespaced object.
        let k = rec(Reason::Started).key();
        assert_eq!(k.namespace.as_deref(), Some("default"));
        assert_eq!(k.kind, "Event");
    }
}

// ─────────────────────────────────────────────────────────────────────
// THE SINK — the seam between "an event happened" and "it is in the store".
//
// ★ WHY A TRAIT AND NOT A DIRECT STORE WRITE. Events are emitted from the
// kubelet, the scheduler and a dozen controllers, none of which should
// take a store dependency just to say what they observed. More
// importantly, event recording MUST NOT be able to fail a reconcile: if
// writing an event errored upward, a full event store would stop the
// cluster from working. The sink's contract is therefore infallible from
// the caller's view — it records or it drops, and it never propagates.
//
// ★ DROPPING IS A REAL POLICY, NOT AN OVERSIGHT. Upstream's recorder is
// lossy under pressure for exactly this reason. A dropped event costs
// visibility; a failed reconcile costs the workload.
// ─────────────────────────────────────────────────────────────────────

/// Where recorded events go.
#[async_trait::async_trait]
pub trait EventSink: Send + Sync + 'static {
    /// Record one event. Never returns an error: see the block above.
    async fn record(&self, event: EventRecord);
}

/// A sink that discards everything.
///
/// The honest default for a component with no store wired, and what makes
/// event emission safe to add to a code path before the plumbing exists —
/// the alternative being an `Option<Sink>` check at every call site.
pub struct NullEventSink;

#[async_trait::async_trait]
impl EventSink for NullEventSink {
    async fn record(&self, _event: EventRecord) {}
}

/// A sink that collects into memory, for tests.
#[derive(Default)]
pub struct CollectingEventSink {
    events: std::sync::Mutex<Vec<EventRecord>>,
}

impl CollectingEventSink {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every event recorded so far, in order.
    #[must_use]
    pub fn drain(&self) -> Vec<EventRecord> {
        std::mem::take(&mut *self.events.lock().expect("event sink poisoned"))
    }

    /// The reasons recorded so far — the assertion most tests actually want.
    #[must_use]
    pub fn reasons(&self) -> Vec<Reason> {
        self.events
            .lock()
            .expect("event sink poisoned")
            .iter()
            .map(|e| e.reason)
            .collect()
    }
}

#[async_trait::async_trait]
impl EventSink for CollectingEventSink {
    async fn record(&self, event: EventRecord) {
        self.events.lock().expect("event sink poisoned").push(event);
    }
}

/// Helper: record an event about a pod.
///
/// Exists so a call site names only what it observed — the object, the
/// reason, the message — and cannot get the involvedObject shape wrong.
pub async fn record_pod_event(
    sink: &dyn EventSink,
    namespace: &str,
    pod: &str,
    uid: Option<&str>,
    reason: Reason,
    message: impl Into<String>,
    component: &str,
    timestamp: &str,
) {
    sink.record(EventRecord {
        involved: InvolvedObject {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some(namespace.to_string()),
            name: pod.to_string(),
            uid: uid.map(ToString::to_string),
        },
        reason,
        message: message.into(),
        component: component.to_string(),
        timestamp: timestamp.to_string(),
    })
    .await;
}

#[cfg(test)]
mod sink_tests {
    use super::*;

    #[tokio::test]
    async fn the_collecting_sink_preserves_order() {
        let sink = CollectingEventSink::new();
        for r in [Reason::Pulled, Reason::Created, Reason::Started] {
            record_pod_event(
                &sink,
                "default",
                "nginx",
                Some("u"),
                r,
                "m",
                "kubelet",
                "2026-08-29T21:00:00Z",
            )
            .await;
        }
        // Order matters: kubectl describe renders events chronologically,
        // and a reordered lifecycle reads as a different failure.
        assert_eq!(
            sink.reasons(),
            vec![Reason::Pulled, Reason::Created, Reason::Started]
        );
    }

    #[tokio::test]
    async fn the_null_sink_makes_emission_safe_before_the_plumbing_exists() {
        // The alternative is an Option<Sink> check at every call site,
        // which is where a forgotten branch silently stops recording.
        let sink = NullEventSink;
        record_pod_event(
            &sink,
            "default",
            "nginx",
            None,
            Reason::Failed,
            "m",
            "kubelet",
            "2026-08-29T21:00:00Z",
        )
        .await;
        // Nothing to assert but the absence of a panic — which IS the
        // contract: recording must never be able to fail a reconcile.
    }

    #[tokio::test]
    async fn the_helper_cannot_get_the_involved_object_shape_wrong() {
        let sink = CollectingEventSink::new();
        record_pod_event(
            &sink,
            "kube-system",
            "coredns",
            Some("abc"),
            Reason::Unhealthy,
            "probe failed",
            "kubelet",
            "2026-08-29T21:00:00Z",
        )
        .await;
        let e = &sink.drain()[0];
        assert_eq!(e.involved.kind, "Pod");
        assert_eq!(e.involved.api_version, "v1");
        assert_eq!(e.involved.namespace.as_deref(), Some("kube-system"));
        assert_eq!(e.involved.name, "coredns");
        assert_eq!(e.involved.uid.as_deref(), Some("abc"));
        // And the severity still derives from the reason, not the caller.
        assert_eq!(e.reason.severity(), Severity::Warning);
    }
}
