//! Typed `PodSpec` + `PodStatus` — the M0.0.2 hand-authored bullseye.
//!
//! Owns the minimum-viable typed expansion of the Pod resource's
//! spec + status. The full upstream Pod schema has 40+ nested
//! types; this module ships the 8 that podinfo (the canonical
//! pleme-io reconciliation demo) actually populates — proven by
//! end-to-end round-trip against the live engenho-local cluster's
//! podinfo replicas.
//!
//! ## Why hand-author
//!
//! `engenho-kube-codegen` will emit byte-for-byte equivalent
//! shapes at M0.0.3. Until then, every consumer of `engenho-types`
//! that wants typed access to PodSpec/PodStatus reads an opaque
//! `serde_json::Value`. The cost of that opacity compounds: typed
//! kubelet diffs, typed admission gates, typed scheduler policies,
//! typed MCP responses — all blocked until the bullseye lands.
//!
//! The pleme-io theory frame (ENGENHO.md §X) explicitly calls out
//! this pattern: hand-author one kind as the byte-for-byte target,
//! then the generator reproduces it. Same reason: zero rewrites
//! when codegen catches up.
//!
//! ## Scope discipline
//!
//! Every field below has a counterpart in the vendored OpenAPI v3
//! schema at `vendor/openapi/v1.34.0/api__v1_openapi.json`. Fields
//! that podinfo + the engenho-local FluxCD bootstrap don't touch
//! are deliberately omitted (will land in M0.0.3's full codegen).
//! Adding a field here without an OpenAPI counterpart is a bug.

#![allow(clippy::module_name_repetitions)]

use serde::{Deserialize, Serialize};

// ── PodSpec ───────────────────────────────────────────────────────

/// `PodSpec` is the specification of a Pod's desired behavior.
///
/// Reference: `io.k8s.api.core.v1.PodSpec` in `vendor/openapi/v1.34.0/api__v1_openapi.json`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodSpec {
    /// List of containers belonging to the pod.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub containers: Vec<Container>,

    /// `RestartPolicy` for all containers within the pod. One of
    /// `Always`, `OnFailure`, `Never`. Default is `Always`.
    #[serde(
        default,
        rename = "restartPolicy",
        skip_serializing_if = "Option::is_none"
    )]
    pub restart_policy: Option<String>,

    /// `NodeName` is a request to schedule this pod onto a specific
    /// node. Empty for unscheduled pods.
    #[serde(default, rename = "nodeName", skip_serializing_if = "Option::is_none")]
    pub node_name: Option<String>,

    /// `ServiceAccountName` is the name of the ServiceAccount to
    /// use to run this pod. Defaults to `default`.
    #[serde(
        default,
        rename = "serviceAccountName",
        skip_serializing_if = "Option::is_none"
    )]
    pub service_account_name: Option<String>,

    /// `TerminationGracePeriodSeconds` — duration in seconds the
    /// pod needs to terminate gracefully. Defaults to 30s.
    #[serde(
        default,
        rename = "terminationGracePeriodSeconds",
        skip_serializing_if = "Option::is_none"
    )]
    pub termination_grace_period_seconds: Option<i64>,

    /// `NodeSelector` is a selector which must be true for the pod
    /// to fit on a node.
    #[serde(
        default,
        rename = "nodeSelector",
        skip_serializing_if = "std::collections::BTreeMap::is_empty"
    )]
    pub node_selector: std::collections::BTreeMap<String, String>,
}

/// `Container` is a single application container that runs in a pod.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Container {
    /// Name of the container. Required.
    pub name: String,

    /// Container image name. Required (in practice).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub image: String,

    /// Entrypoint array. Not executed within a shell.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,

    /// Arguments to the entrypoint.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,

    /// List of ports to expose from the container.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<ContainerPort>,

    /// List of environment variables to set in the container.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<EnvVar>,

    /// Image pull policy. One of `Always`, `Never`, `IfNotPresent`.
    #[serde(
        default,
        rename = "imagePullPolicy",
        skip_serializing_if = "Option::is_none"
    )]
    pub image_pull_policy: Option<String>,
}

/// `ContainerPort` represents a network port in a single container.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ContainerPort {
    /// Number of port to expose on the pod's IP. Required.
    #[serde(rename = "containerPort")]
    pub container_port: i32,

    /// `IP` to bind the external port to. Default 0.0.0.0.
    #[serde(default, rename = "hostIP", skip_serializing_if = "Option::is_none")]
    pub host_ip: Option<String>,

    /// Number of port to expose on the host. If specified, this
    /// must equal `container_port` unless using `hostNetwork`.
    #[serde(default, rename = "hostPort", skip_serializing_if = "Option::is_none")]
    pub host_port: Option<i32>,

    /// Each named port in a pod must have a unique name. Name for
    /// the port that can be referred to by services.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Protocol for port. Must be `UDP`, `TCP`, or `SCTP`. Default
    /// `TCP`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
}

/// `EnvVar` represents an environment variable present in a Container.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EnvVar {
    /// Name of the environment variable. Required.
    pub name: String,

    /// Value of the environment variable. Defaults to empty string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    // NOTE: valueFrom (ConfigMapKeyRef / SecretKeyRef / FieldRef
    // / ResourceFieldRef) is deferred to M0.0.3 codegen. Operators
    // setting env from a ConfigMap/Secret today land it as a JSON
    // round-trip via the Value blob until then.
}

// ── PodStatus ─────────────────────────────────────────────────────

/// `PodStatus` represents information about the status of a pod.
/// Status may trail the actual state of a system, especially if
/// the node that hosts the pod cannot contact the control plane.
///
/// Reference: `io.k8s.api.core.v1.PodStatus`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodStatus {
    /// The phase of a Pod is a simple, high-level summary of where
    /// the Pod is in its lifecycle. One of: `Pending`, `Running`,
    /// `Succeeded`, `Failed`, `Unknown`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<PodPhase>,

    /// Current service state of pod.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<PodCondition>,

    /// A human readable message indicating details about why the
    /// pod is in this condition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// A brief CamelCase message indicating details about why the
    /// pod is in this state. e.g. `Evicted`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// `IP` address allocated to the pod. Routable at least within
    /// the cluster. Empty if not yet allocated.
    #[serde(default, rename = "podIP", skip_serializing_if = "Option::is_none")]
    pub pod_ip: Option<String>,

    /// `IP` address of the host to which the pod is assigned. Empty
    /// if not yet scheduled.
    #[serde(default, rename = "hostIP", skip_serializing_if = "Option::is_none")]
    pub host_ip: Option<String>,

    /// The list of container statuses.
    #[serde(
        default,
        rename = "containerStatuses",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub container_statuses: Vec<ContainerStatus>,
}

/// `PodPhase` — the simple high-level summary of a Pod's lifecycle.
///
/// Encoded as a string in JSON. Engenho's typed surface uses a
/// closed enum because the upstream contract is closed: a Pod's
/// phase is always one of the five named below or absent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PodPhase {
    Pending,
    Running,
    Succeeded,
    Failed,
    Unknown,
}

/// `PodCondition` contains details for the current condition of a pod.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodCondition {
    /// Type of pod condition. e.g. `Ready`, `PodScheduled`,
    /// `ContainersReady`, `Initialized`.
    #[serde(rename = "type")]
    pub r#type: String,

    /// Status of the condition. One of `True`, `False`, `Unknown`.
    pub status: String,

    /// Last time the condition transitioned from one status to another.
    #[serde(
        default,
        rename = "lastTransitionTime",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_transition_time: Option<String>,

    /// Unique, one-word, CamelCase reason for the condition's last
    /// transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// Human-readable message indicating details about last transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// `ContainerStatus` contains details for the current status of
/// this container.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ContainerStatus {
    /// Name of the container.
    pub name: String,

    /// The image the container is running. The container image may
    /// not match the image used in the PodSpec, as it may have been
    /// resolved by the runtime.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub image: String,

    /// `Ready` specifies whether the container has passed its
    /// readiness probe.
    #[serde(default)]
    pub ready: bool,

    /// The number of times the container has been restarted.
    #[serde(default, rename = "restartCount")]
    pub restart_count: i32,

    /// `ContainerID` reported by the runtime.
    #[serde(
        default,
        rename = "containerID",
        skip_serializing_if = "Option::is_none"
    )]
    pub container_id: Option<String>,

    /// Whether the container is currently started (passed startup probe).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started: Option<bool>,
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pod_phase_serializes_as_pascal_string() {
        // Apple-style enum: PodPhase::Running → "Running".
        assert_eq!(
            serde_json::to_string(&PodPhase::Running).unwrap(),
            "\"Running\""
        );
        assert_eq!(
            serde_json::to_string(&PodPhase::Succeeded).unwrap(),
            "\"Succeeded\""
        );
    }

    #[test]
    fn pod_phase_round_trips() {
        for phase in [
            PodPhase::Pending,
            PodPhase::Running,
            PodPhase::Succeeded,
            PodPhase::Failed,
            PodPhase::Unknown,
        ] {
            let s = serde_json::to_string(&phase).unwrap();
            let back: PodPhase = serde_json::from_str(&s).unwrap();
            assert_eq!(back, phase);
        }
    }

    #[test]
    fn container_minimal_round_trip() {
        let c = Container {
            name: "main".into(),
            image: "ghcr.io/stefanprodan/podinfo:6.12.0".into(),
            ports: vec![ContainerPort {
                container_port: 9898,
                name: Some("http".into()),
                protocol: Some("TCP".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let s = serde_json::to_string(&c).unwrap();
        // Spot-check camelCase field names that the K8s wire expects.
        assert!(s.contains("\"containerPort\":9898"), "got: {s}");
        let back: Container = serde_json::from_str(&s).unwrap();
        assert_eq!(back, c);
    }

    #[test]
    fn pod_spec_with_node_selector_round_trips() {
        let mut spec = PodSpec {
            containers: vec![Container {
                name: "a".into(),
                image: "x".into(),
                ..Default::default()
            }],
            restart_policy: Some("Always".into()),
            ..Default::default()
        };
        spec.node_selector
            .insert("zone".into(), "us-east-1a".into());
        let s = serde_json::to_string(&spec).unwrap();
        assert!(s.contains("\"restartPolicy\":\"Always\""), "got: {s}");
        assert!(
            s.contains("\"nodeSelector\":{\"zone\":\"us-east-1a\"}"),
            "got: {s}"
        );
        let back: PodSpec = serde_json::from_str(&s).unwrap();
        assert_eq!(back, spec);
    }

    #[test]
    fn pod_status_running_with_two_containers_round_trips() {
        let status = PodStatus {
            phase: Some(PodPhase::Running),
            conditions: vec![PodCondition {
                r#type: "Ready".into(),
                status: "True".into(),
                ..Default::default()
            }],
            pod_ip: Some("10.42.0.5".into()),
            host_ip: Some("192.168.64.10".into()),
            container_statuses: vec![ContainerStatus {
                name: "main".into(),
                image: "ghcr.io/stefanprodan/podinfo:6.12.0".into(),
                ready: true,
                restart_count: 1,
                ..Default::default()
            }],
            ..Default::default()
        };
        let s = serde_json::to_string(&status).unwrap();
        // Wire-shape assertions — K8s consumers expect exactly these field names.
        assert!(s.contains("\"podIP\":\"10.42.0.5\""), "got: {s}");
        assert!(s.contains("\"hostIP\":\"192.168.64.10\""), "got: {s}");
        assert!(s.contains("\"containerStatuses\""), "got: {s}");
        assert!(s.contains("\"restartCount\":1"), "got: {s}");
        let back: PodStatus = serde_json::from_str(&s).unwrap();
        assert_eq!(back, status);
    }

    /// The empty `PodSpec` serializes to `{}` — no spurious empty
    /// arrays / nulls. Critical for ApplyPatch wire economy.
    #[test]
    fn empty_pod_spec_serializes_to_empty_object() {
        let s = serde_json::to_string(&PodSpec::default()).unwrap();
        assert_eq!(s, "{}");
    }

    use proptest::prelude::*;

    /// Property-based round-trip: any randomly generated Container
    /// must survive JSON serialize → deserialize identity. Catches
    /// regressions like a field rename or a `skip_serializing_if`
    /// that drops a populated field. 256 random cases per test run.
    proptest! {
        #[test]
        fn arb_container_round_trips(
            name in "[a-z][a-z0-9-]{0,30}",
            image in "[a-z][a-z0-9./-]{0,50}",
            port in 1i32..65535,
            port_name in "[a-z][a-z0-9-]{0,14}",
            protocol in prop_oneof!["TCP", "UDP", "SCTP"],
            arg_count in 0usize..5,
        ) {
            let mut c = Container {
                name: name.clone(),
                image: image.clone(),
                ports: vec![ContainerPort {
                    container_port: port,
                    name: Some(port_name.clone()),
                    protocol: Some(protocol.to_string()),
                    ..Default::default()
                }],
                args: (0..arg_count).map(|i| format!("--arg-{i}")).collect(),
                ..Default::default()
            };
            // env vars with no value (None) must also round-trip.
            c.env.push(EnvVar { name: "PATH".into(), value: None });
            c.env.push(EnvVar { name: "HOME".into(), value: Some("/root".into()) });
            let json = serde_json::to_string(&c).unwrap();
            let back: Container = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(back, c);
        }

        #[test]
        fn arb_pod_phase_round_trips(idx in 0usize..5) {
            let phases = [
                PodPhase::Pending,
                PodPhase::Running,
                PodPhase::Succeeded,
                PodPhase::Failed,
                PodPhase::Unknown,
            ];
            let phase = phases[idx];
            let s = serde_json::to_string(&phase).unwrap();
            let back: PodPhase = serde_json::from_str(&s).unwrap();
            prop_assert_eq!(back, phase);
        }
    }

    /// Parsing a live podinfo Pod's status from the cluster's
    /// `kubectl get -o json` output (captured 2026-05-21):
    /// `podinfo-8df8b84cd-6lb4k` after a stable Running state.
    #[test]
    fn parses_real_podinfo_status() {
        let real = r#"{
            "phase": "Running",
            "conditions": [
                {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-05-21T17:21:42Z"},
                {"type": "Ready", "status": "True", "lastTransitionTime": "2026-05-21T20:20:01Z"},
                {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-05-21T20:20:01Z"},
                {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-05-21T17:21:42Z"}
            ],
            "hostIP": "192.168.64.10",
            "podIP": "10.42.0.20",
            "containerStatuses": [
                {
                    "name": "podinfod",
                    "image": "ghcr.io/stefanprodan/podinfo:6.12.0",
                    "ready": true,
                    "restartCount": 1,
                    "containerID": "containerd://abcd1234",
                    "started": true
                }
            ]
        }"#;
        let status: PodStatus = serde_json::from_str(real).unwrap();
        assert_eq!(status.phase, Some(PodPhase::Running));
        assert_eq!(status.conditions.len(), 4);
        assert_eq!(status.pod_ip.as_deref(), Some("10.42.0.20"));
        assert_eq!(status.container_statuses[0].restart_count, 1);
        assert!(status.container_statuses[0].ready);
        assert_eq!(status.container_statuses[0].started, Some(true));
    }
}
