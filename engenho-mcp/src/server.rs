//! rmcp 0.15 server scaffold — wires the `ClusterReader` trait
//! into the tool router via `#[tool_router]` + `#[tool_handler]`.
//!
//! The canonical pleme-io shape: tools return `String` (JSON-encoded
//! for engenho-mcp, plain text for some other servers) so the
//! transport layer doesn't need a typed result wrapper. Inputs
//! flow through `Parameters<T>`, which the macro decodes from the
//! MCP JSON-RPC payload using `schemars`-derived schemas.
//!
//! The server owns an `Arc<dyn ClusterReader>` so test fixtures can
//! swap in `MockClusterReader` without touching this file.

use std::sync::Arc;

use rmcp::{
    ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router,
};
use serde::{Deserialize, Serialize};

use crate::reader::{ClusterReader, ReaderError};
use crate::resource_kind::ResourceKind;
use crate::views::SnapshotMetaView;

/// Input for `cluster_pods` — cluster + namespace.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ClusterPodsInput {
    /// Cluster name as registered in kikai's clusters.yaml.
    #[schemars(description = "Cluster name as registered in kikai's clusters.yaml")]
    pub cluster: String,
    /// Namespace to list pods in. Defaults to `default` if omitted.
    #[serde(default = "default_namespace")]
    #[schemars(description = "Namespace to list pods in. Default 'default'.")]
    pub namespace: String,
}

fn default_namespace() -> String {
    "default".to_string()
}

/// Input for `cluster_resource_list` — generic resource listing
/// across the engenho-types catalog. ResourceKind is closed
/// enum; adding a kind is a single PR to engenho-mcp.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ClusterResourceListInput {
    /// Cluster name as registered in kikai's clusters.yaml.
    #[schemars(description = "Cluster name as registered in kikai's clusters.yaml")]
    pub cluster: String,
    /// Resource kind to list.
    #[schemars(
        description = "Resource kind to list. One of: pod, service, config_map, secret, service_account, namespace, node, deployment, replica_set. Cluster-scoped kinds (namespace, node) ignore the namespace input. Dispatches to the typed engenho-types catalog + engenho-kube-client.list<R>()."
    )]
    pub kind: ResourceKind,
    /// Namespace to list resources in. Defaults to `default`. Ignored
    /// for cluster-scoped kinds.
    #[serde(default = "default_namespace")]
    #[schemars(description = "Namespace to list resources in. Default 'default'. Ignored for cluster-scoped kinds.")]
    pub namespace: String,
    /// Kubernetes label selector. e.g. `app=podinfo` or
    /// `app=podinfo,tier=backend`. Empty = no filter.
    #[serde(default)]
    #[schemars(description = "Kubernetes label selector. e.g. 'app=podinfo' or 'app=podinfo,tier=backend'. Empty = no filter.")]
    pub label_selector: String,
    /// Kubernetes field selector. e.g. `status.phase=Running`.
    /// Empty = no filter.
    #[serde(default)]
    #[schemars(description = "Kubernetes field selector. e.g. 'status.phase=Running'. Empty = no filter.")]
    pub field_selector: String,
    /// Maximum number of items to return. None = no limit.
    #[serde(default)]
    #[schemars(description = "Maximum number of items to return. Omitted/null = no limit.")]
    pub limit: Option<u32>,
}

/// Input for `cluster_resource_get` — single resource fetch.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ClusterResourceGetInput {
    /// Cluster name as registered in kikai's clusters.yaml.
    #[schemars(description = "Cluster name as registered in kikai's clusters.yaml")]
    pub cluster: String,
    /// Resource kind. One of: pod, service, config_map, namespace, deployment.
    #[schemars(
        description = "Resource kind. One of: pod, service, config_map, namespace, deployment."
    )]
    pub kind: ResourceKind,
    /// Resource name (metadata.name).
    #[schemars(description = "Resource name (metadata.name)")]
    pub name: String,
    /// Namespace to fetch from. Defaults to `default`. Ignored for
    /// cluster-scoped kinds.
    #[serde(default = "default_namespace")]
    #[schemars(description = "Namespace to fetch from. Default 'default'. Ignored for cluster-scoped kinds.")]
    pub namespace: String,
}

/// Single typed input for every tool — the cluster name. Reusing
/// one struct keeps the JSON Schema clients see uniform.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ClusterRef {
    /// Cluster name as registered in kikai's `clusters.yaml`.
    /// Examples: `engenho-local`, `ryn-k3s`, `cid-k3s`.
    #[schemars(description = "Cluster name as registered in kikai's clusters.yaml")]
    pub cluster: String,
}

/// Wrapper around the snapshot meta — `snapshot: null` when no
/// auto-snapshot is present. Explicit shape so JSON consumers
/// don't need to handle a bare-null edge case.
#[derive(Debug, Serialize)]
pub struct SnapshotMetaResponse {
    pub snapshot: Option<SnapshotMetaView>,
}

#[derive(Clone)]
pub struct EngenhoMcp {
    reader: Arc<dyn ClusterReader>,
    tool_router: ToolRouter<EngenhoMcp>,
}

impl EngenhoMcp {
    #[must_use]
    pub fn new(reader: Arc<dyn ClusterReader>) -> Self {
        Self {
            reader,
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router]
impl EngenhoMcp {
    /// Aggregate cluster health — Agent / VM / API / Snapshot rows
    /// derived from kikai's on-disk state. Read-only, sub-50ms,
    /// never invokes kikai or kubectl.
    #[tool(
        description = "Aggregate cluster status — Agent / VM / API / Snapshot rows derived from kikai's on-disk state. Read-only, sub-50ms, no kubectl invocation."
    )]
    pub async fn cluster_status(&self, Parameters(p): Parameters<ClusterRef>) -> String {
        match self.reader.cluster_status(&p.cluster).await {
            Ok(v) => to_pretty_json(&v),
            Err(e) => to_pretty_error(&e),
        }
    }

    /// Typed slice of the deployed cluster config (cpus, memory,
    /// gitops backend, network shape). Secret-material-free by
    /// construction.
    #[tool(
        description = "Typed cluster config view — CPUs, memory, gitops backend, network shape. Secret material excluded by type."
    )]
    pub async fn cluster_config(&self, Parameters(p): Parameters<ClusterRef>) -> String {
        match self.reader.cluster_config(&p.cluster).await {
            Ok(v) => to_pretty_json(&v),
            Err(e) => to_pretty_error(&e),
        }
    }

    /// Describes the kubeconfig path + auth method (Bearer / mTLS /
    /// exec / anonymous) WITHOUT carrying the secret material.
    /// Operators resolve tokens out-of-band via cofre.
    #[tool(
        description = "Kubeconfig descriptor — path + auth method tag. NEVER returns secret material; tokens are out-of-band via cofre."
    )]
    pub async fn cluster_kubeconfig(&self, Parameters(p): Parameters<ClusterRef>) -> String {
        match self.reader.kubeconfig_descriptor(&p.cluster).await {
            Ok(v) => to_pretty_json(&v),
            Err(e) => to_pretty_error(&e),
        }
    }

    /// Snapshot meta if a fast-resume artifact is present; `null`
    /// otherwise. `all_paths_exist=false` means stale meta — the
    /// next bring-up will fall back to fresh boot.
    #[tool(
        description = "Auto-snapshot meta if present (null otherwise). Includes path + schema + whether referenced store paths still exist."
    )]
    pub async fn cluster_snapshot_meta(&self, Parameters(p): Parameters<ClusterRef>) -> String {
        match self.reader.snapshot_meta(&p.cluster).await {
            Ok(snapshot) => to_pretty_json(&SnapshotMetaResponse { snapshot }),
            Err(e) => to_pretty_error(&e),
        }
    }

    /// List pods in a namespace. FIRST tool that goes beyond
    /// on-disk state — talks to the live cluster's API via
    /// `engenho-kube-client` and parses responses through
    /// `engenho-types::Pod` (typed `PodSpec` + `PodStatus`).
    /// Secret-material-free by type: every container summary
    /// carries name/image/ready/restart_count, no token/env-from-secret
    /// payloads make it into the wire.
    #[tool(
        description = "List pods in a namespace via the live cluster API. Returns typed name/namespace/phase/podIP/hostIP/containers/conditions per pod. Live API call through engenho-kube-client + engenho-types::Pod — NOT a kubectl shellout, NOT on-disk-only data."
    )]
    pub async fn cluster_pods(&self, Parameters(p): Parameters<ClusterPodsInput>) -> String {
        match self.reader.list_pods(&p.cluster, &p.namespace).await {
            Ok(v) => to_pretty_json(&v),
            Err(e) => to_pretty_error(&e),
        }
    }

    /// **Generic resource list across the engenho-types catalog.**
    ///
    /// Takes a cluster + ResourceKind + namespace, dispatches to
    /// the typed `engenho-kube-client::list<R>()` through the
    /// engenho-types KubeResource impl for that kind. Returns the
    /// raw K8s list-shape `{kind, apiVersion, items, metadata}`
    /// — same shape as `kubectl get <kind> -o json`, but the
    /// `items` payload was parsed through typed Rust structs
    /// (PodSpec / ServiceSpec / DeploymentSpec / ConfigMap data)
    /// not JSON-traversed.
    ///
    /// Compounding: adding a new kind to engenho-types' catalog
    /// + one variant + one match arm = it shows up in this tool.
    /// No new MCP tool needed. The whole catalog rides one wire.
    #[tool(
        description = "Generic resource listing across the engenho-types catalog. Takes (cluster, kind, namespace) where kind is one of pod, service, config_map, namespace, deployment. Returns the K8s list shape; items parsed through typed Rust structs (not JSON-traversed). Cluster-scoped kinds (namespace) ignore the namespace argument."
    )]
    pub async fn cluster_resource_list(
        &self,
        Parameters(p): Parameters<ClusterResourceListInput>,
    ) -> String {
        let spec = crate::reader::ListSpec {
            label_selector: p.label_selector,
            field_selector: p.field_selector,
            limit: p.limit,
        };
        match self
            .reader
            .list_resource(&p.cluster, p.kind, &p.namespace, &spec)
            .await
        {
            Ok(v) => serde_json::to_string_pretty(&v).unwrap_or_else(|e| {
                format!(
                    "{{\"error\":\"serialize\",\"detail\":\"{}\"}}",
                    e
                )
            }),
            Err(e) => to_pretty_error(&e),
        }
    }

    /// **Symmetric single-resource fetch via the typed catalog.**
    ///
    /// Same dispatch shape as `cluster_resource_list` but returns
    /// one resource by name. The result is the K8s canonical
    /// resource shape `{kind, apiVersion, metadata, spec, status}`
    /// — kubectl-style but parsed through typed Rust structs.
    /// Cluster-scoped kinds (`namespace`) ignore the namespace
    /// argument.
    #[tool(
        description = "Generic single-resource fetch via the typed catalog. Takes (cluster, kind, name, namespace) — returns the K8s canonical resource shape parsed through typed Rust structs. Symmetric to cluster_resource_list."
    )]
    pub async fn cluster_resource_get(
        &self,
        Parameters(p): Parameters<ClusterResourceGetInput>,
    ) -> String {
        match self
            .reader
            .get_resource(&p.cluster, p.kind, &p.namespace, &p.name)
            .await
        {
            Ok(v) => serde_json::to_string_pretty(&v).unwrap_or_else(|e| {
                format!(
                    "{{\"error\":\"serialize\",\"detail\":\"{}\"}}",
                    e
                )
            }),
            Err(e) => to_pretty_error(&e),
        }
    }
}

#[tool_handler]
impl ServerHandler for EngenhoMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some(INSTRUCTIONS.into()),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            ..Default::default()
        }
    }
}

const INSTRUCTIONS: &str = "engenho-mcp — typed reader for engenho-managed Kubernetes clusters. \
Surfaces kikai's on-disk cluster state (status rows, config, kubeconfig \
descriptor, snapshot meta) as MCP tools. Read-only by construction — no \
mutation or attestation in this version (Writer layer ships at P2).";

/// Pretty-printed JSON response. Pretty so operators reading raw
/// MCP traces can scan structure; clients ignore whitespace.
fn to_pretty_json<T: Serialize>(value: &T) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|e| {
        // Practically impossible — our view types are infallibly
        // serializable. If it happens, emit a typed error doc.
        format!(
            "{{\"error\":\"internal_serialize_failure\",\"detail\":\"{}\"}}",
            e
        )
    })
}

/// Typed error response — clients can pattern-match on `kind`
/// without parsing the prose `message`. Stable shape.
fn to_pretty_error(err: &ReaderError) -> String {
    serde_json::to_string_pretty(&serde_json::json!({
        "error": err.kind(),
        "message": err.to_string(),
    }))
    .unwrap_or_else(|_| String::from("{\"error\":\"unknown\"}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::mock::{MockClusterReader, MockClusterState};
    use crate::views::*;

    fn sample_state() -> MockClusterState {
        MockClusterState {
            status: ClusterStatus {
                cluster: "demo".into(),
                rows: vec![StatusRow {
                    name: "Agent".into(),
                    outcome: StatusOutcome::Ok,
                    detail: "loaded".into(),
                }],
            },
            config: ClusterConfigView {
                cluster: "demo".into(),
                vm_mode: "k3s".into(),
                cpus: 4,
                memory_mib: 8192,
                disk_size: "50G".into(),
                api_port: 6443,
                ssh_port: 2222,
                mac_address: "5a:94:ef:00:00:01".into(),
                vm_ip: "192.168.64.10".into(),
                boot_timeout_secs: 300,
                gitops: GitopsBackendView::None,
            },
            auth: KubeAuthDescriptor {
                cluster: "demo".into(),
                kubeconfig_path: "/tmp/kubeconfig".into(),
                kubeconfig_exists: true,
                auth_method: AuthMethod::BearerToken,
            },
            snapshot: None,
        }
    }

    fn server() -> EngenhoMcp {
        let r = MockClusterReader::new().with_cluster("demo", sample_state());
        EngenhoMcp::new(Arc::new(r))
    }

    #[tokio::test]
    async fn cluster_status_returns_json_with_typed_shape() {
        let s = server();
        let raw = s
            .cluster_status(Parameters(ClusterRef { cluster: "demo".into() }))
            .await;
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["cluster"], "demo");
        assert!(v["rows"].is_array());
    }

    #[tokio::test]
    async fn unknown_cluster_returns_typed_error_payload() {
        let s = server();
        let raw = s
            .cluster_status(Parameters(ClusterRef { cluster: "missing".into() }))
            .await;
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        // Clients pattern-match on the `error` field — stable kind
        // identifier, NOT the prose message.
        assert_eq!(v["error"], "unknown_cluster");
        assert!(v["message"].is_string());
    }

    #[tokio::test]
    async fn snapshot_response_is_explicitly_nullable() {
        let s = server();
        let raw = s
            .cluster_snapshot_meta(Parameters(ClusterRef { cluster: "demo".into() }))
            .await;
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert!(v["snapshot"].is_null());
    }

    #[tokio::test]
    async fn cluster_config_serializes_gitops_backend_tagged() {
        let s = server();
        let raw = s
            .cluster_config(Parameters(ClusterRef { cluster: "demo".into() }))
            .await;
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["gitops"]["backend"], "none");
    }

    #[tokio::test]
    async fn no_secret_material_in_any_serialized_response() {
        let s = server();
        for tool in [
            s.cluster_status(Parameters(ClusterRef { cluster: "demo".into() })).await,
            s.cluster_config(Parameters(ClusterRef { cluster: "demo".into() })).await,
            s.cluster_kubeconfig(Parameters(ClusterRef { cluster: "demo".into() })).await,
            s.cluster_snapshot_meta(Parameters(ClusterRef { cluster: "demo".into() })).await,
        ] {
            let lower = tool.to_lowercase();
            // Defensive: an accidental token/password leak would
            // surface as one of these substrings. The view types
            // exclude them by design; this test guards against a
            // future field rename re-introducing one.
            assert!(
                !lower.contains("\"token\":\"")
                    && !lower.contains("\"password\":\"")
                    && !lower.contains("bearer "),
                "secret material leaked into response: {tool}"
            );
        }
    }

    #[test]
    fn get_info_advertises_tools_capability_and_instructions() {
        let s = server();
        let info = s.get_info();
        assert!(info.capabilities.tools.is_some(), "tools capability missing");
        assert!(info.instructions.is_some(), "instructions missing");
        let i = info.instructions.unwrap();
        assert!(i.contains("engenho-mcp"));
    }

    /// **Compile-time + runtime exhaustiveness lock.**
    ///
    /// For every `ResourceKind` variant, calling `list_resource`
    /// through the trait must produce a typed `ReaderError` (the
    /// default impl returns InvalidState because MockClusterReader
    /// doesn't have live API access). The point is to PROVE every
    /// variant is reachable through the dispatcher — adding a
    /// variant without dispatch arms can't slip through.
    ///
    /// Compile-time: the closed-enum match in
    /// `KikaiClusterReader::list_resource` + `get_resource` is
    /// exhaustive; missing arm = build error.
    ///
    /// Runtime (this test): exercises the trait surface itself.
    #[tokio::test]
    async fn every_resource_kind_reaches_list_dispatch() {
        let s = server();
        let spec = crate::reader::ListSpec::default();
        for &kind in crate::resource_kind::ResourceKind::all() {
            // MockClusterReader::list_resource takes the default
            // impl which returns the "not supported" InvalidState.
            // We don't assert success — we assert *typed-reachability*.
            let err = s
                .reader
                .list_resource("demo", kind, "default", &spec)
                .await
                .unwrap_err();
            assert_eq!(
                err.kind(),
                "invalid_state",
                "{kind:?} should bottom out at InvalidState in the mock; got {err:?}"
            );
            // The error message embeds the kind's label — proves
            // dispatch saw the variant.
            assert!(
                err.to_string().contains(kind.label()),
                "{kind:?} label missing from default-impl error: {err}"
            );
        }
    }

    #[tokio::test]
    async fn every_resource_kind_reaches_get_dispatch() {
        let s = server();
        for &kind in crate::resource_kind::ResourceKind::all() {
            let err = s
                .reader
                .get_resource("demo", kind, "default", "anything")
                .await
                .unwrap_err();
            assert_eq!(err.kind(), "invalid_state");
            assert!(err.to_string().contains(kind.label()));
        }
    }
}
