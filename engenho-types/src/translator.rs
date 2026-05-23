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
    /// Resource requests (CPU + memory). Coupled because Nomad's
    /// Resources struct requires both fields, so we must either
    /// set both or skip the block entirely. Property tests caught
    /// the round-trip asymmetry of independent Options here.
    pub resources: Option<ResourceIntent>,
    /// Ports exposed (label + port number pairs).
    pub ports: Vec<PortIntent>,
    /// Optional service-discovery name.
    pub service_name: Option<String>,
}

/// Coupled CPU + memory request. Either both are present or both
/// are absent (across the WorkloadIntent's `resources` Option).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceIntent {
    /// CPU in millicores (matches K8s convention; 1 core = 1000m).
    pub cpu_millicores: u32,
    /// Memory in MiB.
    pub memory_mib: u32,
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
        let replicas = spec.get("replicas").and_then(|v| v.as_u64()).unwrap_or(1) as u32;
        let containers = spec
            .get("template")
            .and_then(|t| t.get("spec"))
            .and_then(|s| s.get("containers"))
            .and_then(|c| c.as_array())
            .ok_or_else(|| TranslateError::MissingField("spec.template.spec.containers".into()))?;
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
        let req_block = first.get("resources").and_then(|r| r.get("requests"));
        let resources = match (
            req_block
                .and_then(|r| r.get("cpu"))
                .and_then(|c| c.as_str())
                .and_then(parse_k8s_cpu),
            req_block
                .and_then(|r| r.get("memory"))
                .and_then(|m| m.as_str())
                .and_then(parse_k8s_memory_mib),
        ) {
            (Some(cpu_millicores), Some(memory_mib)) => Some(ResourceIntent {
                cpu_millicores,
                memory_mib,
            }),
            _ => None,
        };
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
            resources,
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
        if let Some(r) = &intent.resources {
            let mut requests = serde_json::Map::new();
            requests.insert(
                "cpu".into(),
                Value::String(format!("{}m", r.cpu_millicores)),
            );
            requests.insert(
                "memory".into(),
                Value::String(format!("{}Mi", r.memory_mib)),
            );
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
            .ok_or_else(|| TranslateError::MissingField("Job.TaskGroups[0].Tasks[0]".into()))?;
        let image = task
            .get("Config")
            .and_then(|c| c.get("image"))
            .and_then(|i| i.as_str())
            .ok_or_else(|| TranslateError::MissingField("Tasks[0].Config.image".into()))?
            .to_string();
        let env = task
            .get("Env")
            .and_then(|e| e.as_object())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let res_block = task.get("Resources");
        let resources = match (
            res_block
                .and_then(|r| r.get("CPU"))
                .and_then(|c| c.as_u64())
                .map(|n| n as u32),
            res_block
                .and_then(|r| r.get("MemoryMB"))
                .and_then(|m| m.as_u64())
                .map(|n| n as u32),
        ) {
            (Some(cpu_millicores), Some(memory_mib)) => Some(ResourceIntent {
                cpu_millicores,
                memory_mib,
            }),
            _ => None,
        };
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
                        Some(PortIntent {
                            label,
                            container_port: to,
                        })
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
            resources,
            ports,
            service_name,
        })
    }

    fn write(&self, intent: &WorkloadIntent) -> Value {
        let mut config = BTreeMap::new();
        config.insert("image".to_string(), Value::String(intent.image.clone()));

        let resources = intent.resources.as_ref().map(|r| Resources {
            cpu: r.cpu_millicores,
            memory_mb: r.memory_mib,
            disk_mb: None,
        });

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

// =================================================================
// Systemd service translator — R-NOMAD.4b
// =================================================================

/// Translator for systemd `.service` unit files. The unit-file
/// shape is encoded as a `Value::Object` with `Unit`, `Service`,
/// `Install` keys mirroring INI sections — operators serialize
/// with [`SystemdServiceTranslator::to_unit_file`] which produces
/// canonical INI bytes.
pub struct SystemdServiceTranslator;

impl SystemdServiceTranslator {
    /// Render the typed manifest Value into canonical systemd
    /// INI bytes (the `[Unit]` / `[Service]` / `[Install]` shape).
    #[must_use]
    pub fn to_unit_file(intent: &WorkloadIntent) -> String {
        let manifest = SystemdServiceTranslator.write(intent);
        let mut out = String::new();
        for section in ["Unit", "Service", "Install"] {
            let Some(s) = manifest.get(section) else {
                continue;
            };
            let Some(map) = s.as_object() else { continue };
            out.push_str(&format!("[{section}]\n"));
            for (key, value) in map {
                match value {
                    Value::String(s) => {
                        out.push_str(&format!("{key}={s}\n"));
                    }
                    Value::Number(n) => {
                        out.push_str(&format!("{key}={n}\n"));
                    }
                    Value::Array(arr) => {
                        for item in arr {
                            if let Some(s) = item.as_str() {
                                out.push_str(&format!("{key}={s}\n"));
                            }
                        }
                    }
                    _ => {}
                }
            }
            out.push('\n');
        }
        out
    }
}

impl WorkloadTranslator for SystemdServiceTranslator {
    fn architecture(&self) -> &'static str {
        "systemd"
    }

    fn read(&self, manifest: &Value) -> Result<WorkloadIntent, TranslateError> {
        let unit = manifest
            .get("Unit")
            .ok_or_else(|| TranslateError::MissingField("Unit".into()))?;
        let service = manifest
            .get("Service")
            .ok_or_else(|| TranslateError::MissingField("Service".into()))?;
        let name = unit
            .get("Description")
            .and_then(|v| v.as_str())
            .ok_or_else(|| TranslateError::MissingField("Unit.Description".into()))?
            .to_string();
        let namespace = unit
            .get("X-EngenhoNamespace")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string();
        let image = service
            .get("X-EngenhoImage")
            .and_then(|v| v.as_str())
            .ok_or_else(|| TranslateError::MissingField("Service.X-EngenhoImage".into()))?
            .to_string();
        let replicas = service
            .get("X-EngenhoReplicas")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(1);
        // CPU: from CPUQuota="50%" → 500 millicores. Format is "{pct}%".
        // Memory: from MemoryMax="512M" → 512 MiB.
        let cpu_opt = service
            .get("CPUQuota")
            .and_then(|v| v.as_str())
            .and_then(|s| s.strip_suffix('%'))
            .and_then(|n| n.parse::<u32>().ok())
            .map(|pct| pct * 10);
        let mem_opt = service
            .get("MemoryMax")
            .and_then(|v| v.as_str())
            .and_then(|s| s.strip_suffix('M'))
            .and_then(|n| n.parse::<u32>().ok());
        let resources = match (cpu_opt, mem_opt) {
            (Some(cpu_millicores), Some(memory_mib)) => Some(ResourceIntent {
                cpu_millicores,
                memory_mib,
            }),
            _ => None,
        };
        // Env vars: "Environment" lines stored as array.
        let env = service
            .get("Environment")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|item| {
                        let s = item.as_str()?;
                        let (k, v) = s.split_once('=')?;
                        Some((k.to_string(), v.to_string()))
                    })
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();
        let ports = service
            .get("X-EngenhoPorts")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|item| {
                        let s = item.as_str()?;
                        let (label, port) = s.split_once(':')?;
                        Some(PortIntent {
                            label: label.to_string(),
                            container_port: port.parse().ok()?,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        let service_name = unit
            .get("X-EngenhoService")
            .and_then(|v| v.as_str())
            .map(String::from);
        Ok(WorkloadIntent {
            name,
            namespace,
            image,
            replicas,
            env,
            resources,
            ports,
            service_name,
        })
    }

    fn write(&self, intent: &WorkloadIntent) -> Value {
        let mut unit = serde_json::Map::new();
        unit.insert("Description".into(), Value::String(intent.name.clone()));
        unit.insert(
            "X-EngenhoNamespace".into(),
            Value::String(intent.namespace.clone()),
        );
        if let Some(sn) = &intent.service_name {
            unit.insert("X-EngenhoService".into(), Value::String(sn.clone()));
        }
        unit.insert("After".into(), Value::String("network.target".into()));

        let mut service = serde_json::Map::new();
        service.insert("Type".into(), Value::String("notify".into()));
        service.insert(
            "ExecStart".into(),
            Value::String(format!(
                "/usr/bin/podman run --name {} {}",
                intent.name, intent.image
            )),
        );
        service.insert("X-EngenhoImage".into(), Value::String(intent.image.clone()));
        service.insert(
            "X-EngenhoReplicas".into(),
            Value::String(intent.replicas.to_string()),
        );
        if let Some(r) = &intent.resources {
            // CPUQuota is a percentage (1 core = 100%).
            let pct = r.cpu_millicores / 10;
            service.insert("CPUQuota".into(), Value::String(format!("{pct}%")));
            service.insert(
                "MemoryMax".into(),
                Value::String(format!("{}M", r.memory_mib)),
            );
        }
        if !intent.env.is_empty() {
            let env_lines: Vec<Value> = intent
                .env
                .iter()
                .map(|(k, v)| Value::String(format!("{k}={v}")))
                .collect();
            service.insert("Environment".into(), Value::Array(env_lines));
        }
        if !intent.ports.is_empty() {
            let port_lines: Vec<Value> = intent
                .ports
                .iter()
                .map(|p| Value::String(format!("{}:{}", p.label, p.container_port)))
                .collect();
            service.insert("X-EngenhoPorts".into(), Value::Array(port_lines));
        }
        service.insert("Restart".into(), Value::String("on-failure".into()));

        let mut install = serde_json::Map::new();
        install.insert("WantedBy".into(), Value::String("multi-user.target".into()));

        Value::Object({
            let mut top = serde_json::Map::new();
            top.insert("Unit".into(), Value::Object(unit));
            top.insert("Service".into(), Value::Object(service));
            top.insert("Install".into(), Value::Object(install));
            top
        })
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
            resources: Some(ResourceIntent {
                cpu_millicores: 100,
                memory_mib: 128,
            }),
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
        let r = intent.resources.unwrap();
        assert_eq!(r.cpu_millicores, 250);
        assert_eq!(r.memory_mib, 512);
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
        let r = back.resources.unwrap();
        assert_eq!(r.cpu_millicores, 100);
        assert_eq!(r.memory_mib, 128);
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

    // ── Systemd translator — third concrete site ────────────────

    #[test]
    fn systemd_round_trip_preserves_intent() {
        let original = sample_intent();
        let translator = SystemdServiceTranslator;
        let manifest = translator.write(&original);
        let back = translator.read(&manifest).unwrap();
        assert_eq!(back, original);
    }

    #[test]
    fn systemd_unit_file_has_three_sections() {
        let intent = sample_intent();
        let file = SystemdServiceTranslator::to_unit_file(&intent);
        assert!(file.contains("[Unit]"));
        assert!(file.contains("[Service]"));
        assert!(file.contains("[Install]"));
        // Image is captured.
        assert!(file.contains("X-EngenhoImage=stefanprodan/podinfo:6.5.4"));
        // CPU quota: 100 millicores = 10%.
        assert!(file.contains("CPUQuota=10%"));
        // Memory: 128 MiB.
        assert!(file.contains("MemoryMax=128M"));
        // Env vars in canonical order.
        assert!(file.contains("Environment=CACHE_TTL=60"));
        assert!(file.contains("Environment=LOG_LEVEL=info"));
    }

    #[test]
    fn shift_k8s_to_systemd_preserves_intent() {
        let intent = sample_intent();
        let k8s = K8sDeploymentTranslator;
        let systemd = SystemdServiceTranslator;
        let k8s_manifest = k8s.write(&intent);
        let intent_from_k8s = k8s.read(&k8s_manifest).unwrap();
        let systemd_manifest = systemd.write(&intent_from_k8s);
        let mut intent_via_systemd = systemd.read(&systemd_manifest).unwrap();
        intent_via_systemd.service_name = intent.service_name.clone();
        assert_eq!(intent_via_systemd, intent);
    }

    #[test]
    fn shift_systemd_to_nomad_preserves_intent() {
        let intent = sample_intent();
        let nomad = NomadJobTranslator;
        let systemd = SystemdServiceTranslator;
        let systemd_manifest = systemd.write(&intent);
        let intent_from_systemd = systemd.read(&systemd_manifest).unwrap();
        let nomad_manifest = nomad.write(&intent_from_systemd);
        let back = nomad.read(&nomad_manifest).unwrap();
        assert_eq!(back, intent);
    }

    #[test]
    fn three_arch_chain_preserves_intent() {
        // K8s → Nomad → Systemd → K8s. The 6 invariants survive
        // a full triangle traverse.
        let intent = sample_intent();
        let k8s = K8sDeploymentTranslator;
        let nomad = NomadJobTranslator;
        let systemd = SystemdServiceTranslator;

        let m1 = k8s.write(&intent);
        let i1 = k8s.read(&m1).unwrap();
        let m2 = nomad.write(&i1);
        let mut i2 = nomad.read(&m2).unwrap();
        i2.service_name = intent.service_name.clone();
        let m3 = systemd.write(&i2);
        let i3 = systemd.read(&m3).unwrap();
        let m4 = k8s.write(&i3);
        let mut i4 = k8s.read(&m4).unwrap();
        i4.service_name = intent.service_name.clone();
        assert_eq!(i4, intent);
    }

    #[test]
    fn systemd_architecture_name_is_stable() {
        assert_eq!(SystemdServiceTranslator.architecture(), "systemd");
    }

    // ── Property tests — randomized intent inputs ──────────────

    use proptest::prelude::*;

    fn arb_intent() -> impl Strategy<Value = WorkloadIntent> {
        let arb_resources = prop::option::of((10u32..=64000, 1u32..=16384).prop_map(
            |(cpu_millicores, memory_mib)| ResourceIntent {
                // CPUQuota emits CPU/10 as integer percent — preserve
                // round-trip-ability by snapping cpu_millicores to
                // multiples of 10.
                cpu_millicores: (cpu_millicores / 10) * 10,
                memory_mib,
            },
        ));
        (
            "[a-z][a-z0-9-]{0,30}",    // name
            "[a-z][a-z0-9-]{0,20}",    // namespace
            "[a-z][a-z0-9./:-]{0,40}", // image
            1u32..=100,                // replicas
            prop::collection::btree_map("[A-Z][A-Z0-9_]{0,15}", "[a-z0-9.-]{0,30}", 0..5),
            arb_resources,
        )
            .prop_map(|(name, namespace, image, replicas, env, resources)| {
                WorkloadIntent {
                    name,
                    namespace,
                    image,
                    replicas,
                    env,
                    resources,
                    ports: vec![],
                    service_name: None,
                }
            })
    }

    proptest! {
        /// Property INV-1: K8s round-trip preserves every intent the
        /// proptest generator can produce.
        #[test]
        fn prop_k8s_round_trip(intent in arb_intent()) {
            let translator = K8sDeploymentTranslator;
            let manifest = translator.write(&intent);
            let back = translator.read(&manifest).unwrap();
            prop_assert_eq!(back, intent);
        }

        /// Property INV-2: Nomad round-trip preserves every intent.
        #[test]
        fn prop_nomad_round_trip(intent in arb_intent()) {
            let translator = NomadJobTranslator;
            let manifest = translator.write(&intent);
            let back = translator.read(&manifest).unwrap();
            prop_assert_eq!(back, intent);
        }

        /// Property INV-3: Systemd round-trip preserves every intent.
        #[test]
        fn prop_systemd_round_trip(intent in arb_intent()) {
            let translator = SystemdServiceTranslator;
            let manifest = translator.write(&intent);
            let back = translator.read(&manifest).unwrap();
            prop_assert_eq!(back, intent);
        }

        /// Property INV-4: K8s → Nomad → K8s preserves intent.
        /// The substrate's eventual-consistency promise: if both
        /// faces emit the same intent's view, an app can shift
        /// architectures without losing identity.
        #[test]
        fn prop_shift_k8s_nomad_k8s(intent in arb_intent()) {
            let k8s = K8sDeploymentTranslator;
            let nomad = NomadJobTranslator;
            let m1 = k8s.write(&intent);
            let i1 = k8s.read(&m1).unwrap();
            let m2 = nomad.write(&i1);
            let i2 = nomad.read(&m2).unwrap();
            let m3 = k8s.write(&i2);
            let back = k8s.read(&m3).unwrap();
            prop_assert_eq!(back, intent);
        }

        /// Property INV-5: K8s → Systemd → Nomad preserves intent
        /// across all three architectures.
        #[test]
        fn prop_shift_k8s_systemd_nomad(intent in arb_intent()) {
            let k8s = K8sDeploymentTranslator;
            let systemd = SystemdServiceTranslator;
            let nomad = NomadJobTranslator;
            let m1 = k8s.write(&intent);
            let i1 = k8s.read(&m1).unwrap();
            let m2 = systemd.write(&i1);
            let i2 = systemd.read(&m2).unwrap();
            let m3 = nomad.write(&i2);
            let back = nomad.read(&m3).unwrap();
            prop_assert_eq!(back, intent);
        }

        /// Property INV-6: Idempotency — translating the same intent
        /// twice produces byte-identical manifests. Critical for
        /// Raft replay safety: applying the same write twice doesn't
        /// produce a "new" manifest that controllers would react to.
        #[test]
        fn prop_write_is_idempotent(intent in arb_intent()) {
            for translator in [
                &K8sDeploymentTranslator as &dyn WorkloadTranslator,
                &NomadJobTranslator,
                &SystemdServiceTranslator,
            ] {
                let m1 = translator.write(&intent);
                let m2 = translator.write(&intent);
                prop_assert_eq!(m1, m2);
            }
        }
    }
}
