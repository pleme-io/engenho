//! Typed views — the wire shapes the MCP server emits.
//!
//! Two cardinal rules govern every type in this module:
//!
//!   1. **No secret material.** Tokens, age keys, admin passwords,
//!      cofre payloads — none of them ever sit in a serialized
//!      response. `KubeAuthDescriptor` names the auth method
//!      (Bearer / ClientCert / Exec / Anonymous) WITHOUT carrying
//!      the credential itself. Operators can resolve the secret
//!      separately via cofre; the MCP wire is non-confidential.
//!
//!   2. **No PathBuf in serialized form.** Every path is a `String`
//!      so the JSON Schema is well-formed and operators on
//!      non-darwin clients can read responses without OS quirks.
//!
//! All types derive `(Serialize, Deserialize, JsonSchema)` so they
//! plug straight into rmcp's `#[tool]` machinery — schemars walks
//! the derives to emit the input/output schemas the MCP client
//! sees on initialize. Type-driven; no hand-rolled JSON Schemas.

use rmcp::schemars;
use serde::{Deserialize, Serialize};

// ── Cluster status ────────────────────────────────────────────────

/// Aggregate health snapshot — one row per subsystem.
/// Mirrors `kikai status`'s shape but in machine-readable form.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ClusterStatus {
    pub cluster: String,
    pub rows: Vec<StatusRow>,
}

/// Six rows in canonical order: Agent, VM, API, Node, Flux, Pods.
/// Each row carries its name, an outcome enum, and a free-form
/// detail string for diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StatusRow {
    pub name: String,
    pub outcome: StatusOutcome,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StatusOutcome {
    Ok,
    Warn,
    Fail,
    NotApplicable,
}

// ── Cluster config view ───────────────────────────────────────────

/// Non-secret slice of the deployed cluster config. The serialized
/// shape intentionally drops everything secret-adjacent so the
/// response is safe to pass over any transport.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ClusterConfigView {
    pub cluster: String,
    pub vm_mode: String,
    pub cpus: u32,
    pub memory_mib: u32,
    pub disk_size: String,
    pub api_port: u16,
    pub ssh_port: u16,
    pub mac_address: String,
    pub vm_ip: String,
    pub boot_timeout_secs: u64,
    pub gitops: GitopsBackendView,
}

/// Typed projection of kikai's `GitopsBackend` — same tagged-union
/// shape but without any auth material.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "backend", rename_all = "snake_case")]
pub enum GitopsBackendView {
    None,
    Fluxcd(FluxcdView),
    Argocd(ArgocdView),
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct FluxcdView {
    pub namespace: String,
    pub source_url: String,
    pub source_ref: String,
    pub source_path: String,
    pub target_namespace: String,
    pub interval_secs: u64,
    pub has_secret: bool,
    pub target_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ArgocdView {
    pub namespace: String,
    pub application: String,
    pub source_url: String,
    pub target_revision: String,
    pub source_path: String,
    pub destination_namespace: String,
    pub sync_mode: String,
    pub has_secret: bool,
    pub target_count: usize,
}

// ── Kubeconfig descriptor ─────────────────────────────────────────

/// Description of the kubeconfig + auth method, without the secret
/// material itself. Operators that need the actual token go through
/// cofre — kikai never exposes plaintext credentials.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct KubeAuthDescriptor {
    pub cluster: String,
    pub kubeconfig_path: String,
    pub kubeconfig_exists: bool,
    pub auth_method: AuthMethod,
}

/// Tagged enum naming the auth method. Variants are deliberately
/// data-free; the actual secret lookup is the operator's
/// responsibility (typically via cofre).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuthMethod {
    /// No auth (rare; usually a misconfiguration).
    Anonymous,
    /// Bearer token (resolved out-of-band).
    BearerToken,
    /// mTLS client cert + key (resolved out-of-band).
    ClientCert,
    /// Exec plugin (kubeconfig instructs kubectl to call an
    /// external binary to mint a token).
    Exec,
    /// The kubeconfig file is missing — can't determine method.
    Unknown,
}

// ── Snapshot meta view ────────────────────────────────────────────

/// Operator's view of the cluster's auto-snapshot state. `None`
/// means no snapshot present; `Some` means meta.json parsed
/// cleanly + the referenced store paths still exist.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SnapshotMetaView {
    pub cluster: String,
    pub state_path: String,
    pub state_size_bytes: u64,
    pub meta_path: String,
    pub schema_version: u32,
    pub kernel: String,
    pub initrd: String,
    pub init: String,
    pub root_disk: String,
    pub all_paths_exist: bool,
}

// ── Pod listing ───────────────────────────────────────────────────

/// One pod's typed-but-flattened view for the MCP wire. Mirrors
/// `engenho-types::Pod` but pre-flattened so MCP clients don't
/// have to nest-traverse spec.containers etc. Secret-free by type
/// (no Secret-volumeMount or token-projected-volume payloads).
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PodSummary {
    pub name: String,
    pub namespace: String,
    /// Phase as a string (Pending / Running / Succeeded / Failed
    /// / Unknown). Sourced from typed `PodPhase` enum.
    pub phase: String,
    /// PodIP if scheduled; `None` if pending.
    pub pod_ip: Option<String>,
    /// HostIP — the node IP.
    pub host_ip: Option<String>,
    /// Container summaries (name + image + ready + restart count).
    pub containers: Vec<ContainerSummary>,
    /// Pod-level conditions (Ready / PodScheduled / ContainersReady
    /// / Initialized) flattened to a string→string map for the
    /// MCP wire.
    pub conditions: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ContainerSummary {
    pub name: String,
    pub image: String,
    pub ready: bool,
    pub restart_count: i32,
    pub started: Option<bool>,
}

/// Aggregate response for the `cluster_pods` MCP tool.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PodListView {
    pub cluster: String,
    pub namespace: String,
    pub pods: Vec<PodSummary>,
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_outcome_serializes_snake_case() {
        let s = serde_json::to_string(&StatusOutcome::Ok).unwrap();
        assert_eq!(s, "\"ok\"");
        let s = serde_json::to_string(&StatusOutcome::NotApplicable).unwrap();
        assert_eq!(s, "\"not_applicable\"");
    }

    #[test]
    fn gitops_backend_view_serializes_tagged() {
        let v = GitopsBackendView::None;
        let s = serde_json::to_string(&v).unwrap();
        assert_eq!(s, r#"{"backend":"none"}"#);

        let v = GitopsBackendView::Fluxcd(FluxcdView {
            namespace: "flux-system".into(),
            source_url: "https://github.com/x/y".into(),
            source_ref: "main".into(),
            source_path: "./".into(),
            target_namespace: "default".into(),
            interval_secs: 60,
            has_secret: false,
            target_count: 0,
        });
        let s = serde_json::to_string(&v).unwrap();
        assert!(s.contains(r#""backend":"fluxcd""#), "got: {s}");
        // Critical: no secret material in the serialized shape.
        assert!(!s.contains("token"), "secret leaked into view: {s}");
        assert!(!s.contains("password"), "secret leaked into view: {s}");
    }

    #[test]
    fn auth_method_never_carries_token_in_serialized_form() {
        // The enum is data-free by design; round-trip proves it.
        for variant in [
            AuthMethod::Anonymous,
            AuthMethod::BearerToken,
            AuthMethod::ClientCert,
            AuthMethod::Exec,
            AuthMethod::Unknown,
        ] {
            let s = serde_json::to_string(&variant).unwrap();
            // A bare string variant — no inline secret possible.
            assert!(s.starts_with('"'), "got: {s}");
        }
    }

    #[test]
    fn cluster_status_round_trips() {
        let status = ClusterStatus {
            cluster: "engenho-local".into(),
            rows: vec![
                StatusRow {
                    name: "Agent".into(),
                    outcome: StatusOutcome::Ok,
                    detail: "loaded".into(),
                },
                StatusRow {
                    name: "Flux".into(),
                    outcome: StatusOutcome::Warn,
                    detail: "kustomization pending".into(),
                },
            ],
        };
        let json = serde_json::to_string(&status).unwrap();
        let back: ClusterStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back.cluster, "engenho-local");
        assert_eq!(back.rows.len(), 2);
    }

    /// schemars must emit a valid schema for every view type — if
    /// any type forgets to derive JsonSchema, this fails to compile.
    #[test]
    fn every_view_type_has_a_jsonschema() {
        let _ = schemars::schema_for!(ClusterStatus);
        let _ = schemars::schema_for!(ClusterConfigView);
        let _ = schemars::schema_for!(KubeAuthDescriptor);
        let _ = schemars::schema_for!(SnapshotMetaView);
        let _ = schemars::schema_for!(GitopsBackendView);
    }
}
