//! Typed Nomad job + task + group + driver-config shapes.
//!
//! The Nomad universe in engenho-types — peer to the K8s catalog.
//! Round-trips through both Nomad JSON (free, serde) and Nomad HCL
//! (R-NOMAD.1 — adds `hcl-rs` parser/emitter in a follow-up).
//!
//! Scope today (R-NOMAD.0):
//!   * Job + TaskGroup + Task + Resources + DriverConfig (docker,
//!     exec, raw_exec).
//!   * Service registration block.
//!   * Network + port definitions.
//!   * Update strategy (rolling deploys).
//!   * Constraints.
//!
//! This is the minimum surface to translate a K8s Deployment + Pod
//! into a Nomad job at R-NOMAD.4. JSON in+out works now; HCL ships
//! at R-NOMAD.1/.2.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Top-level Nomad job.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Job {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "Type", skip_serializing_if = "Option::is_none")]
    pub job_type: Option<JobType>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub datacenters: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub task_groups: Vec<TaskGroup>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
    pub meta: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub constraints: Vec<Constraint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update: Option<UpdateStrategy>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobType {
    Service,
    Batch,
    System,
    Sysbatch,
}

/// A group of tasks scheduled together on the same client node.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct TaskGroup {
    pub name: String,
    #[serde(default = "default_count")]
    pub count: u32,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub tasks: Vec<Task>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub networks: Vec<Network>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub services: Vec<Service>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub constraints: Vec<Constraint>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
    pub meta: BTreeMap<String, String>,
}

fn default_count() -> u32 {
    1
}

/// A single task — the unit Nomad schedules.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Task {
    pub name: String,
    pub driver: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
    pub config: BTreeMap<String, serde_json::Value>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
    pub env: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<Resources>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub services: Vec<Service>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Resources {
    #[serde(rename = "CPU")]
    pub cpu: u32,
    #[serde(rename = "MemoryMB")]
    pub memory_mb: u32,
    #[serde(rename = "DiskMB", skip_serializing_if = "Option::is_none")]
    pub disk_mb: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Network {
    pub mode: String,
    #[serde(
        rename = "DynamicPorts",
        skip_serializing_if = "Vec::is_empty",
        default
    )]
    pub dynamic_ports: Vec<Port>,
    #[serde(
        rename = "ReservedPorts",
        skip_serializing_if = "Vec::is_empty",
        default
    )]
    pub reserved_ports: Vec<Port>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Port {
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<u16>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Service {
    pub name: String,
    #[serde(rename = "PortLabel", skip_serializing_if = "Option::is_none")]
    pub port_label: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>, // "consul" / "nomad"
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Constraint {
    #[serde(rename = "LTarget")]
    pub l_target: String,
    pub operand: String,
    #[serde(rename = "RTarget")]
    pub r_target: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct UpdateStrategy {
    pub max_parallel: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canary: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_healthy_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_revert: Option<bool>,
}

impl Job {
    /// Serialize to Nomad JSON (the format `nomad job run` accepts).
    /// First-class for the Nomad face today (R-NOMAD.3).
    ///
    /// # Errors
    /// Returns `serde_json::Error` if serialization fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(&serde_json::json!({ "Job": self }))
    }

    /// Parse a Nomad JSON job spec (the `nomad job inspect -json` shape).
    ///
    /// # Errors
    /// Returns `serde_json::Error` if parsing fails.
    pub fn from_json(s: &str) -> Result<Self, serde_json::Error> {
        let envelope: serde_json::Value = serde_json::from_str(s)?;
        // Accept either { "Job": {...} } envelope or raw job body.
        let body = envelope.get("Job").cloned().unwrap_or(envelope);
        serde_json::from_value(body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_job() -> Job {
        Job {
            id: "podinfo".into(),
            name: Some("podinfo".into()),
            job_type: Some(JobType::Service),
            datacenters: vec!["dc1".into()],
            namespace: Some("default".into()),
            task_groups: vec![TaskGroup {
                name: "web".into(),
                count: 3,
                tasks: vec![Task {
                    name: "main".into(),
                    driver: "docker".into(),
                    config: {
                        let mut m = BTreeMap::new();
                        m.insert("image".into(), serde_json::json!("stefanprodan/podinfo:6"));
                        m
                    },
                    env: BTreeMap::new(),
                    resources: Some(Resources {
                        cpu: 100,
                        memory_mb: 128,
                        disk_mb: None,
                    }),
                    services: vec![],
                }],
                networks: vec![Network {
                    mode: "bridge".into(),
                    dynamic_ports: vec![Port {
                        label: "http".into(),
                        value: None,
                        to: Some(9898),
                    }],
                    reserved_ports: vec![],
                }],
                services: vec![Service {
                    name: "podinfo".into(),
                    port_label: Some("http".into()),
                    tags: vec!["v1".into()],
                    provider: Some("nomad".into()),
                }],
                constraints: vec![],
                meta: BTreeMap::new(),
            }],
            meta: BTreeMap::new(),
            constraints: vec![],
            update: Some(UpdateStrategy {
                max_parallel: 1,
                canary: Some(1),
                min_healthy_time: Some("10s".into()),
                auto_revert: Some(true),
            }),
        }
    }

    #[test]
    fn job_serializes_with_pascal_case_keys() {
        let j = sample_job();
        let json = j.to_json().unwrap();
        // Nomad uses PascalCase keys.
        assert!(json.contains("\"ID\": \"podinfo\""));
        assert!(json.contains("\"TaskGroups\""));
        assert!(json.contains("\"Driver\": \"docker\""));
        // Envelope present.
        assert!(json.starts_with("{\n  \"Job\""));
    }

    #[test]
    fn job_round_trips_through_json() {
        let original = sample_job();
        let json = original.to_json().unwrap();
        let parsed = Job::from_json(&json).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn job_parses_envelope_or_bare_body() {
        let j = sample_job();
        let envelope = j.to_json().unwrap();
        // Strip the envelope.
        let v: serde_json::Value = serde_json::from_str(&envelope).unwrap();
        let bare = serde_json::to_string(&v["Job"]).unwrap();
        let parsed = Job::from_json(&bare).unwrap();
        assert_eq!(parsed, j);
    }

    #[test]
    fn job_type_serializes_lowercase() {
        let json = serde_json::to_string(&JobType::Service).unwrap();
        assert_eq!(json, "\"service\"");
        let parsed: JobType = serde_json::from_str("\"batch\"").unwrap();
        assert_eq!(parsed, JobType::Batch);
    }

    #[test]
    fn resources_emits_cpu_memorymb_uppercase() {
        let r = Resources {
            cpu: 500,
            memory_mb: 256,
            disk_mb: Some(1024),
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"CPU\":500"));
        assert!(json.contains("\"MemoryMB\":256"));
        assert!(json.contains("\"DiskMB\":1024"));
    }

    #[test]
    fn task_default_drops_empty_collections() {
        let t = Task {
            name: "x".into(),
            driver: "docker".into(),
            config: BTreeMap::new(),
            env: BTreeMap::new(),
            resources: None,
            services: vec![],
        };
        let json = serde_json::to_string(&t).unwrap();
        // Only Name + Driver should appear.
        assert!(json.contains("\"Name\":\"x\""));
        assert!(json.contains("\"Driver\":\"docker\""));
        assert!(!json.contains("\"Config\""));
        assert!(!json.contains("\"Env\""));
        assert!(!json.contains("\"Resources\""));
    }

    #[test]
    fn minimal_job_default_count_is_one() {
        let json = r#"{"ID":"j","TaskGroups":[{"Name":"g"}]}"#;
        let parsed = Job::from_json(json).unwrap();
        assert_eq!(parsed.task_groups[0].count, 1);
    }
}
