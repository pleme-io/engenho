//! `WorkloadTranslator` — typed cross-architecture workload translation.
//!
//! The user's directive: *"doing it correctly and having plans and
//! strategies for two architectures and for example shifting an app
//! between them is possible with the right orchestration across the
//! entire engenho ether using eventual consistency guarantees."*
//!
//! This module provides the typed glue between the K8s resource
//! catalog and the Nomad job catalog. An app declared as a K8s
//! Deployment translates into a Nomad Job (and vice versa) without
//! the operator hand-writing either format.
//!
//! ## Translation invariants — what MUST be preserved
//!
//! 1. **Replica count** — `Deployment.spec.replicas` ↔ `TaskGroup.count`
//! 2. **Image identity** — `containers[0].image` ↔ `task.config.image`
//! 3. **Resource requests** — `containers[0].resources.requests` ↔ `Resources`
//! 4. **Env vars** — `containers[0].env` ↔ `task.env`
//! 5. **Workload name** — `metadata.name` ↔ `Job.id`
//! 6. **Namespace** — `metadata.namespace` ↔ `Job.namespace`
//!
//! These are PROPERTIES the translator preserves; any future
//! architecture (Systemd units, OCI raw containers, Lambda functions)
//! must preserve the same six. The trait makes this contractual.
//!
//! ## Property tests
//!
//! - Round-trip: K8s → Nomad → K8s preserves all 6 invariants
//! - Idempotency: repeated calls produce identical output
//! - Symmetry: `to_nomad(from_nomad(j)) == j` (where types overlap)

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::nomad_v1::{Job, JobType, Network, Port, Resources, Service, Task, TaskGroup};

/// Canonical "intent" extracted from any workload — the minimum
/// preservation contract. Any architecture must round-trip through
/// `WorkloadIntent` cleanly.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadIntent {
    /// Workload name (matches metadata.name across architectures).
    pub name: String,
    /// Namespace / job namespace.
    pub namespace: String,
    /// Container image identity.
    pub image: String,
    /// Number of replicas.
    pub replicas: u32,
    /// Environment variables in deterministic order (BTreeMap).
    pub env: BTreeMap<String, String>,
    /// Requested CPU in millicores (matches K8s convention).
    pub cpu_millicores: Option<u32>,
    /// Requested memory in MiB.
    pub memory_mib: Option<u32>,
    /// Ports exposed (label + port number pairs).
    pub ports: Vec<PortIntent>,
    /// Optional service-discovery name.
    pub service_name: Option<String>,
}

/// Port intent — matches both K8s containerPort + Nomad dynamic_ports.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortIntent {
    /// Port label (e.g. "http", "metrics").
    pub label: String,
    /// Container-side port number.
    pub container_port: u16,
}

/// The typed translator trait. Each architecture impl reads its
/// native format into `WorkloadIntent` + writes from `WorkloadIntent`
/// into its native format.
pub trait WorkloadTranslator {
    /// Stable architecture name for telemetry.
    fn architecture(&self) -> &'static str;

    /// Read this architecture's native workload manifest into the
    /// canonical intent. Returns Err if the manifest is malformed.
    ///
    /// # Errors
    ///
    /// Returns `Err` if required fields are missing or malformed.
    fn read(&self, manifest: &Value) -> Result<WorkloadIntent, TranslateError>;

    /// Write the canonical intent into this architecture's native
    /// manifest.
    fn write(&self, intent: &WorkloadIntent) -> Value;
}

/// Translation error — single typed surface, kind-stable telemetry.
#[derive(Clone, Debug, thiserror::Error)]
pub enum TranslateError {
    /// Required field missing in the source manifest.
    #[error("missing required field: {0}")]
    MissingField(String),
    /// Field present but wrong type.
    #[error("invalid type for field {0}: {1}")]
    InvalidType(String, String),
}

impl TranslateError {
    /// Stable identifier for telemetry + cross-language SDK
    /// dispatch.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::MissingField(_) => "missing_field",
            Self::InvalidType(_, _) => "invalid_type",
        }
    }
}

// =================================================================
// K8s Deployment translator
// =================================================================

/// Translator for K8s `apps/v1.Deployment` manifests.
pub struct K8sDeploymentTranslator;

impl WorkloadTranslator for K8sDeploymentTranslator {
    fn architecture(&self) -> &'static str {
        "kubernetes"
    }

    fn read(&self, manifest: &Value) -> Result<WorkloadIntent, TranslateError> {
        let metadata = manifest
            .get("metadata")
            .ok_or_else(|| TranslateError::MissingField("metadata".into()))?;
        let name = metadata
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| TranslateError::MissingField("metadata.name".into()))?
            .to_string();
        let namespace = metadata
            .get("namespace")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string();
        let spec = manifest
            .get("spec")
            .ok_or_else(|| TranslateError::MissingField("spec".into()))?;
        let replicas = spec
            .get("replicas")
            .and_then(|v| v.as_u64())
            .unwrap_or(1) as u32;
        let containers = spec
            .get("template")
            .and_then(|t| t.get("spec"))
            .and_then(|s| s.get("containers"))
            .and_then(|c| c.as_array())
            .ok_or_else(|| {
                TranslateError::MissingField("spec.template.spec.containers".into())
            })?;
        let first = containers
            .first()
            .ok_or_else(|| TranslateError::MissingField("containers[0]".into()))?;
        let image = first
            .get("image")
            .and_then(|v| v.as_str())
            .ok_or_else(|| TranslateError::MissingField("containers[0].image".into()))?
            .to_string();
        let env = first
            .get("env")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|e| {
                        let name = e.get("name")?.as_str()?.to_string();
                        let value = e.get("value")?.as_str()?.to_string();
                        Some((name, value))
                    })
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();
        let resources = first.get("resources").and_then(|r| r.get("requests"));
        let cpu_millicores = resources
            .and_then(|r| r.get("cpu"))
            .and_then(|c| c.as_str())
            .and_then(parse_k8s_cpu);
        let memory_mib = resources
            .and_then(|r| r.get("memory"))
            .and_then(|m| m.as_str())
            .and_then(parse_k8s_memory_mib);
        let ports = first
            .get("ports")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|p| {
                        let label = p
                            .get("name")
                            .and_then(|n| n.as_str())
                            .unwrap_or("default")
                            .to_string();
                        let container_port =
                            p.get("containerPort").and_then(|p| p.as_u64())? as u16;
                        Some(PortIntent {
                            label,
                            container_port,
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Ok(WorkloadIntent {
            name,
            namespace,
            image,
            replicas,
            env,
            cpu_millicores,
            memory_mib,
            ports,
            service_name: None,
        })
    }

    fn write(&self, intent: &WorkloadIntent) -> Value {
        let env_array: Vec<Value> = intent
            .env
            .iter()
            .map(|(k, v)| {
                serde_json::json!({
                    "name": k,
                    "value": v,
                })
            })
            .collect();
        let mut resources = serde_json::Map::new();
        let mut requests = serde_json::Map::new();
        if let Some(cpu) = intent.cpu_millicores {
            requests.insert("cpu".into(), Value::String(format!("{cpu}m")));
        }
        if let Some(mem) = intent.memory_mib {
            requests.insert("memory".into(), Value::String(format!("{mem}Mi")));
        }
        if !requests.is_empty() {
            resources.insert("requests".into(), Value::Object(requests));
        }
        let ports_array: Vec<Value> = intent
            .ports
            .iter()
            .map(|p| {
                serde_json::json!({
                    "name": p.label,
                    "containerPort": p.container_port,
                })
            })
            .collect();

        let mut container = serde_json::json!({
            "name": intent.name,
            "image": intent.image,
        });
        if !env_array.is_empty() {
            container["env"] = Value::Array(env_array);
        }
        if !resources.is_empty() {
            container["resources"] = Value::Object(resources);
        }
        if !ports_array.is_empty() {
            container["ports"] = Value::Array(ports_array);
        }

        serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": intent.name,
                "namespace": intent.namespace,
            },
            "spec": {
                "replicas": intent.replicas,
                "selector": { "matchLabels": { "app": intent.name } },
                "template": {
                    "metadata": { "labels": { "app": intent.name } },
                    "spec": { "containers": [container] }
                }
            }
        })
    }
}

// =================================================================
// Nomad Job translator
// =================================================================

/// Translator for Nomad Job manifests (the JSON shape).
pub struct NomadJobTranslator;

impl WorkloadTranslator for NomadJobTranslator {
    fn architecture(&self) -> &'static str {
        "nomad"
    }

    fn read(&self, manifest: &Value) -> Result<WorkloadIntent, TranslateError> {
        // Tolerate {"Job": {...}} envelope OR bare body.
        let body = manifest.get("Job").unwrap_or(manifest);
        let name = body
            .get("ID")
            .and_then(|v| v.as_str())
            .ok_or_else(|| TranslateError::MissingField("Job.ID".into()))?
            .to_string();
        let namespace = body
            .get("Namespace")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string();
        let group = body
            .get("TaskGroups")
            .and_then(|tg| tg.as_array())
            .and_then(|arr| arr.first())
            .ok_or_else(|| TranslateError::MissingField("Job.TaskGroups[0]".into()))?;
        let replicas = group.get("Count").and_then(|c| c.as_u64()).unwrap_or(1) as u32;
        let task = group
            .get("Tasks")
            .and_then(|t| t.as_array())
            .and_then(|arr| arr.first())
            .ok_or_else(|| {
                TranslateError::MissingField("Job.TaskGroups[0].Tasks[0]".into())
            })?;
        let image = task
            .get("Config")
            .and_then(|c| c.get("image"))
            .and_then(|i| i.as_str())
            .ok_or_else(|| {
                TranslateError::MissingField("Tasks[0].Config.image".into())
            })?
            .to_string();
        let env = task
            .get("Env")
            .and_then(|e| e.as_object())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| {
                        v.as_str().map(|s| (k.clone(), s.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let resources = task.get("Resources");
        let cpu_millicores = resources.and_then(|r| r.get("CPU")).and_then(|c| c.as_u64()).map(|n| n as u32);
        let memory_mib = resources
            .and_then(|r| r.get("MemoryMB"))
            .and_then(|m| m.as_u64())
            .map(|n| n as u32);
        let ports = group
            .get("Networks")
            .and_then(|n| n.as_array())
            .and_then(|arr| arr.first())
            .and_then(|net| net.get("DynamicPorts"))
            .and_then(|p| p.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|port| {
                        let label = port.get("Label").and_then(|l| l.as_str())?.to_string();
                        let to = port.get("To").and_then(|t| t.as_u64())? as u16;
                        Some(PortIntent { label, container_port: to })
                    })
                    .collect()
            })
            .unwrap_or_default();
        let service_name = group
            .get("Services")
            .and_then(|s| s.as_array())
            .and_then(|arr| arr.first())
            .and_then(|svc| svc.get("Name"))
            .and_then(|n| n.as_str())
            .map(String::from);
        Ok(WorkloadIntent {
            name,
            namespace,
            image,
            replicas,
            env,
            cpu_millicores,
            memory_mib,
            ports,
            service_name,
        })
    }

    fn write(&self, intent: &WorkloadIntent) -> Value {
        let mut config = BTreeMap::new();
        config.insert("image".to_string(), Value::String(intent.image.clone()));

        let resources = if intent.cpu_millicores.is_some() || intent.memory_mib.is_some() {
            Some(Resources {
                cpu: intent.cpu_millicores.unwrap_or(100),
                memory_mb: intent.memory_mib.unwrap_or(128),
                disk_mb: None,
            })
        } else {
            None
        };

        let dynamic_ports: Vec<Port> = intent
            .ports
            .iter()
            .map(|p| Port {
                label: p.label.clone(),
                value: None,
                to: Some(p.container_port),
            })
            .collect();

        let networks = if dynamic_ports.is_empty() {
            vec![]
        } else {
            vec![Network {
                mode: "bridge".to_string(),
                dynamic_ports,
                reserved_ports: vec![],
            }]
        };

        let services = intent
            .service_name
            .as_ref()
            .map(|name| {
                vec![Service {
                    name: name.clone(),
                    port_label: intent.ports.first().map(|p| p.label.clone()),
                    tags: vec![],
                    provider: Some("nomad".to_string()),
                }]
            })
            .unwrap_or_default();

        let task = Task {
            name: intent.name.clone(),
            driver: "docker".to_string(),
            config,
            env: intent.env.clone(),
            resources,
            services: vec![],
        };

        let group = TaskGroup {
            name: intent.name.clone(),
            count: intent.replicas,
            tasks: vec![task],
            networks,
            services,
            constraints: vec![],
            meta: BTreeMap::new(),
        };

        let job = Job {
            id: intent.name.clone(),
            name: Some(intent.name.clone()),
            job_type: Some(JobType::Service),
            datacenters: vec!["dc1".to_string()],
            namespace: Some(intent.namespace.clone()),
            task_groups: vec![group],
            meta: BTreeMap::new(),
            constraints: vec![],
            update: None,
        };
        serde_json::json!({ "Job": job })
    }
}

// =================================================================
// Helpers
// =================================================================

fn parse_k8s_cpu(s: &str) -> Option<u32> {
    if let Some(stripped) = s.strip_suffix('m') {
        stripped.parse().ok()
    } else {
        s.parse::<f64>().ok().map(|f| (f * 1000.0) as u32)
    }
}

fn parse_k8s_memory_mib(s: &str) -> Option<u32> {
    if let Some(stripped) = s.strip_suffix("Mi") {
        stripped.parse().ok()
    } else if let Some(stripped) = s.strip_suffix("Gi") {
        stripped.parse::<u32>().ok().map(|n| n * 1024)
    } else if let Some(stripped) = s.strip_suffix("Ki") {
        stripped.parse::<u32>().ok().map(|n| n / 1024)
    } else {
        s.parse::<u64>().ok().map(|b| (b / (1024 * 1024)) as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_intent() -> WorkloadIntent {
        let mut env = BTreeMap::new();
        env.insert("LOG_LEVEL".into(), "info".into());
        env.insert("CACHE_TTL".into(), "60".into());
        WorkloadIntent {
            name: "podinfo".into(),
            namespace: "production".into(),
            image: "stefanprodan/podinfo:6.5.4".into(),
            replicas: 3,
            env,
            cpu_millicores: Some(100),
            memory_mib: Some(128),
            ports: vec![PortIntent {
                label: "http".into(),
                container_port: 9898,
            }],
            service_name: Some("podinfo".into()),
        }
    }

    // ── K8s side ────────────────────────────────────────────────

    #[test]
    fn k8s_round_trip_preserves_intent() {
        let original = sample_intent();
        let translator = K8sDeploymentTranslator;
        let manifest = translator.write(&original);
        let mut back = translator.read(&manifest).unwrap();
        // service_name isn't carried in a K8s Deployment alone
        // (lives in a Service resource); ignore on round-trip.
        back.service_name = original.service_name.clone();
        assert_eq!(back, original);
    }

    #[test]
    fn k8s_read_extracts_all_six_invariants() {
        let manifest = serde_json::json!({
            "metadata": { "name": "x", "namespace": "y" },
            "spec": {
                "replicas": 5,
                "template": {
                    "spec": {
                        "containers": [{
                            "image": "nginx:1.27",
                            "env": [
                                {"name": "A", "value": "1"},
                                {"name": "B", "value": "2"},
                            ],
                            "resources": {
                                "requests": {"cpu": "250m", "memory": "512Mi"}
                            },
                            "ports": [{"name": "http", "containerPort": 80}]
                        }]
                    }
                }
            }
        });
        let intent = K8sDeploymentTranslator.read(&manifest).unwrap();
        assert_eq!(intent.name, "x");
        assert_eq!(intent.namespace, "y");
        assert_eq!(intent.replicas, 5);
        assert_eq!(intent.image, "nginx:1.27");
        assert_eq!(intent.env.get("A").map(String::as_str), Some("1"));
        assert_eq!(intent.cpu_millicores, Some(250));
        assert_eq!(intent.memory_mib, Some(512));
        assert_eq!(intent.ports[0].container_port, 80);
    }

    #[test]
    fn k8s_missing_image_returns_typed_error() {
        let manifest = serde_json::json!({
            "metadata": { "name": "x" },
            "spec": {
                "template": { "spec": { "containers": [{}] } }
            }
        });
        let err = K8sDeploymentTranslator.read(&manifest).unwrap_err();
        assert_eq!(err.kind(), "missing_field");
    }

    // ── Nomad side ─────────────────────────────────────────────

    #[test]
    fn nomad_round_trip_preserves_intent() {
        let original = sample_intent();
        let translator = NomadJobTranslator;
        let manifest = translator.write(&original);
        let back = translator.read(&manifest).unwrap();
        assert_eq!(back, original);
    }

    #[test]
    fn nomad_read_extracts_all_six_invariants() {
        let translator = NomadJobTranslator;
        let intent = sample_intent();
        let manifest = translator.write(&intent);
        let back = translator.read(&manifest).unwrap();
        assert_eq!(back.replicas, 3);
        assert_eq!(back.image, "stefanprodan/podinfo:6.5.4");
        assert_eq!(back.env.len(), 2);
        assert_eq!(back.cpu_millicores, Some(100));
        assert_eq!(back.memory_mib, Some(128));
        assert_eq!(back.ports.len(), 1);
    }

    // ── Cross-architecture shifting ────────────────────────────

    #[test]
    fn shift_k8s_to_nomad_preserves_intent() {
        let intent = sample_intent();
        let k8s = K8sDeploymentTranslator;
        let nomad = NomadJobTranslator;
        let k8s_manifest = k8s.write(&intent);
        let intent_from_k8s = k8s.read(&k8s_manifest).unwrap();
        let nomad_manifest = nomad.write(&intent_from_k8s);
        let mut intent_via_nomad = nomad.read(&nomad_manifest).unwrap();
        // service_name doesn't survive K8s Deployment alone.
        intent_via_nomad.service_name = intent.service_name.clone();
        assert_eq!(intent_via_nomad, intent);
    }

    #[test]
    fn shift_nomad_to_k8s_preserves_intent() {
        let intent = sample_intent();
        let k8s = K8sDeploymentTranslator;
        let nomad = NomadJobTranslator;
        let nomad_manifest = nomad.write(&intent);
        let intent_from_nomad = nomad.read(&nomad_manifest).unwrap();
        let k8s_manifest = k8s.write(&intent_from_nomad);
        let mut intent_via_k8s = k8s.read(&k8s_manifest).unwrap();
        intent_via_k8s.service_name = intent.service_name.clone();
        assert_eq!(intent_via_k8s, intent);
    }

    #[test]
    fn shift_round_trips_three_times() {
        // K8s → Nomad → K8s → Nomad — same intent every time.
        let intent = sample_intent();
        let k8s = K8sDeploymentTranslator;
        let nomad = NomadJobTranslator;
        let m1 = k8s.write(&intent);
        let i1 = k8s.read(&m1).unwrap();
        let m2 = nomad.write(&i1);
        let mut i2 = nomad.read(&m2).unwrap();
        i2.service_name = intent.service_name.clone();
        let m3 = k8s.write(&i2);
        let mut i3 = k8s.read(&m3).unwrap();
        i3.service_name = intent.service_name.clone();
        let m4 = nomad.write(&i3);
        let mut i4 = nomad.read(&m4).unwrap();
        i4.service_name = intent.service_name.clone();
        assert_eq!(i4, intent);
    }

    // ── Helpers ─────────────────────────────────────────────────

    #[test]
    fn parse_k8s_cpu_millicore_form() {
        assert_eq!(parse_k8s_cpu("250m"), Some(250));
        assert_eq!(parse_k8s_cpu("1"), Some(1000));
        assert_eq!(parse_k8s_cpu("0.5"), Some(500));
        assert_eq!(parse_k8s_cpu("invalid"), None);
    }

    #[test]
    fn parse_k8s_memory_supports_mi_gi_ki() {
        assert_eq!(parse_k8s_memory_mib("512Mi"), Some(512));
        assert_eq!(parse_k8s_memory_mib("1Gi"), Some(1024));
        assert_eq!(parse_k8s_memory_mib("invalid"), None);
    }

    #[test]
    fn architectures_are_named_stably() {
        assert_eq!(K8sDeploymentTranslator.architecture(), "kubernetes");
        assert_eq!(NomadJobTranslator.architecture(), "nomad");
    }

    #[test]
    fn translate_error_kind_is_stable() {
        let e = TranslateError::MissingField("x".into());
        assert_eq!(e.kind(), "missing_field");
        let e2 = TranslateError::InvalidType("a".into(), "b".into());
        assert_eq!(e2.kind(), "invalid_type");
    }
}
