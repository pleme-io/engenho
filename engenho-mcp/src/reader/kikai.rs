//! Production `ClusterReader` reading from kikai's canonical paths.
//!
//! The kikai CLI is the substrate's cluster-lifecycle daemon; it
//! writes typed state to a small set of well-known on-disk
//! locations the operator already trusts. This module reads
//! those files + computes the view types, NEVER mutating anything.
//!
//! Canonical paths (mirror kikai's `traits::DiskLayout`):
//!   ~/.config/kikai/clusters.yaml
//!   ~/.local/share/kikai/<cluster>/snapshots/auto.meta.json
//!   ~/.local/share/kikai/<cluster>/snapshots/auto.vmstate
//!   ~/.local/share/kikai/<cluster>/vm.pid
//!   ~/.kube/configs/<cluster>
//!
//! The reader never invokes kikai or kubectl; it derives state
//! from files + the current process's environment so the MCP
//! response is fast (sub-50ms) and side-effect-free.

use async_trait::async_trait;
use std::collections::BTreeMap;
use std::path::PathBuf;

use engenho_kube_client::{client::ReqwestKubeClient, config::Kubeconfig};
use engenho_types::client::{KubeClient, ListOptions};
use engenho_types::generated_v1_34::core_v1::{Pod, PodPhase};

use crate::reader::{ClusterReader, ReaderError};
use crate::views::{
    ArgocdView, AuthMethod, ClusterConfigView, ClusterStatus, ContainerSummary, FluxcdView,
    GitopsBackendView, KubeAuthDescriptor, PodListView, PodSummary, SnapshotMetaView,
    StatusOutcome, StatusRow,
};

pub struct KikaiClusterReader {
    home: PathBuf,
}

impl KikaiClusterReader {
    /// Construct from the operator's `$HOME`. Production callers
    /// use `from_env()`; tests can pass a tmpdir to exercise the
    /// path layout in isolation.
    #[must_use]
    pub fn new(home: PathBuf) -> Self {
        Self { home }
    }

    /// Resolve `$HOME` from the environment; errors if unset.
    pub fn from_env() -> Result<Self, ReaderError> {
        let home = std::env::var_os("HOME").ok_or_else(|| {
            ReaderError::InvalidState("HOME env var not set — cannot locate kikai state".into())
        })?;
        Ok(Self::new(PathBuf::from(home)))
    }

    fn clusters_yaml_path(&self) -> PathBuf {
        self.home.join(".config/kikai/clusters.yaml")
    }

    fn data_dir(&self, cluster: &str) -> PathBuf {
        self.home.join(".local/share/kikai").join(cluster)
    }

    fn kubeconfig_path(&self, cluster: &str) -> PathBuf {
        self.home.join(".kube/configs").join(cluster)
    }

    /// Read the typed clusters.yaml, return only the requested
    /// cluster's entry. Errors fall through as typed `ReaderError`.
    fn load_cluster_entry(&self, cluster: &str) -> Result<ClusterYamlEntry, ReaderError> {
        let path = self.clusters_yaml_path();
        let contents = std::fs::read_to_string(&path).map_err(|source| ReaderError::Io {
            what: path.display().to_string(),
            source,
        })?;
        let all: BTreeMap<String, ClusterYamlEntry> = serde_yaml::from_str(&contents)
            .map_err(|source| ReaderError::Parse {
                what: path.display().to_string(),
                source,
            })?;
        all.into_iter()
            .find(|(k, _)| k == cluster)
            .map(|(_, v)| v)
            .ok_or_else(|| ReaderError::UnknownCluster(cluster.to_string()))
    }
}

#[async_trait]
impl ClusterReader for KikaiClusterReader {
    async fn cluster_status(&self, cluster: &str) -> Result<ClusterStatus, ReaderError> {
        // Verify the cluster exists in the registry first.
        let _ = self.load_cluster_entry(cluster)?;

        let agent_row = status_row_agent(cluster);
        let vm_row = status_row_vm(&self.data_dir(cluster));
        let kubeconfig_exists = self.kubeconfig_path(cluster).exists();
        let api_row = status_row_api(kubeconfig_exists);
        let snapshot = read_snapshot_meta(cluster, &self.data_dir(cluster))?;
        let snap_row = status_row_snapshot(&snapshot);

        Ok(ClusterStatus {
            cluster: cluster.to_string(),
            rows: vec![agent_row, vm_row, api_row, snap_row],
        })
    }

    async fn cluster_config(&self, cluster: &str) -> Result<ClusterConfigView, ReaderError> {
        let entry = self.load_cluster_entry(cluster)?;
        Ok(entry.into_view(cluster))
    }

    async fn kubeconfig_descriptor(
        &self,
        cluster: &str,
    ) -> Result<KubeAuthDescriptor, ReaderError> {
        let _ = self.load_cluster_entry(cluster)?;
        let path = self.kubeconfig_path(cluster);
        let exists = path.exists();
        let auth_method = if !exists {
            AuthMethod::Unknown
        } else {
            detect_auth_method(&path)
        };
        Ok(KubeAuthDescriptor {
            cluster: cluster.to_string(),
            kubeconfig_path: path.display().to_string(),
            kubeconfig_exists: exists,
            auth_method,
        })
    }

    async fn snapshot_meta(
        &self,
        cluster: &str,
    ) -> Result<Option<SnapshotMetaView>, ReaderError> {
        let _ = self.load_cluster_entry(cluster)?;
        read_snapshot_meta(cluster, &self.data_dir(cluster))
    }

    async fn list_pods(
        &self,
        cluster: &str,
        namespace: &str,
    ) -> Result<PodListView, ReaderError> {
        // Verify cluster registered + kubeconfig exists.
        let _ = self.load_cluster_entry(cluster)?;
        let kc_path = self.kubeconfig_path(cluster);
        if !kc_path.exists() {
            return Err(ReaderError::InvalidState(format!(
                "kubeconfig missing at {} — cluster API not yet bootstrapped",
                kc_path.display()
            )));
        }

        // engenho-kube-client load → resolve → REST.
        let kc = Kubeconfig::load(&kc_path).map_err(|e| {
            ReaderError::InvalidState(format!("kubeconfig parse for '{cluster}': {e}"))
        })?;
        let conn = kc.resolve_connection().map_err(|e| {
            ReaderError::InvalidState(format!("kubeconfig resolve for '{cluster}': {e}"))
        })?;
        let client = ReqwestKubeClient::new(conn);

        let list = client
            .list::<Pod>(Some(namespace), &ListOptions::default())
            .await
            .map_err(|e| {
                ReaderError::InvalidState(format!(
                    "LIST pods in {cluster}/{namespace} failed: {e}"
                ))
            })?;

        Ok(PodListView {
            cluster: cluster.to_string(),
            namespace: namespace.to_string(),
            pods: list.items.iter().map(pod_summary_from_typed).collect(),
        })
    }
}

/// Flatten an engenho-types Pod into the wire view. Pure function;
/// no allocation beyond cloning the strings the MCP client gets.
fn pod_summary_from_typed(p: &Pod) -> PodSummary {
    let phase_str = p
        .status
        .as_ref()
        .and_then(|s| s.phase)
        .map(phase_as_str)
        .unwrap_or("Unknown")
        .to_string();
    let (pod_ip, host_ip) = p
        .status
        .as_ref()
        .map(|s| (s.pod_ip.clone(), s.host_ip.clone()))
        .unwrap_or((None, None));
    let containers = p
        .status
        .as_ref()
        .map(|s| {
            s.container_statuses
                .iter()
                .map(|cs| ContainerSummary {
                    name: cs.name.clone(),
                    image: cs.image.clone(),
                    ready: cs.ready,
                    restart_count: cs.restart_count,
                    started: cs.started,
                })
                .collect()
        })
        .unwrap_or_default();
    let mut conditions = BTreeMap::new();
    if let Some(s) = &p.status {
        for cond in &s.conditions {
            conditions.insert(cond.r#type.clone(), cond.status.clone());
        }
    }
    PodSummary {
        name: p.metadata.name.clone(),
        namespace: p.metadata.namespace.clone().unwrap_or_default(),
        phase: phase_str,
        pod_ip,
        host_ip,
        containers,
        conditions,
    }
}

fn phase_as_str(p: PodPhase) -> &'static str {
    match p {
        PodPhase::Pending => "Pending",
        PodPhase::Running => "Running",
        PodPhase::Succeeded => "Succeeded",
        PodPhase::Failed => "Failed",
        PodPhase::Unknown => "Unknown",
    }
}

// ── On-disk YAML shape ────────────────────────────────────────────

#[derive(Debug, serde::Deserialize)]
struct ClusterYamlEntry {
    vm_mode: String,
    cpus: u32,
    memory: u32,
    disk_size: String,
    api_port: u16,
    ssh_port: u16,
    mac_address: String,
    vm_ip: String,
    boot_timeout_secs: u64,
    gitops: serde_json::Value,
}

impl ClusterYamlEntry {
    fn into_view(self, cluster: &str) -> ClusterConfigView {
        ClusterConfigView {
            cluster: cluster.to_string(),
            vm_mode: self.vm_mode,
            cpus: self.cpus,
            memory_mib: self.memory,
            disk_size: self.disk_size,
            api_port: self.api_port,
            ssh_port: self.ssh_port,
            mac_address: self.mac_address,
            vm_ip: self.vm_ip,
            boot_timeout_secs: self.boot_timeout_secs,
            gitops: gitops_view_from_json(&self.gitops),
        }
    }
}

fn gitops_view_from_json(v: &serde_json::Value) -> GitopsBackendView {
    let Some(backend) = v.get("backend").and_then(|x| x.as_str()) else {
        return GitopsBackendView::None;
    };
    match backend {
        "fluxcd" => GitopsBackendView::Fluxcd(FluxcdView {
            namespace: v.get("namespace").and_then(|x| x.as_str()).unwrap_or("flux-system").into(),
            source_url: v.get("source_url").and_then(|x| x.as_str()).unwrap_or_default().into(),
            source_ref: v.get("source_ref").and_then(|x| x.as_str()).unwrap_or("main").into(),
            source_path: v.get("source_path").and_then(|x| x.as_str()).unwrap_or("./").into(),
            target_namespace: v
                .get("target_namespace")
                .and_then(|x| x.as_str())
                .unwrap_or("default")
                .into(),
            interval_secs: v.get("interval_secs").and_then(|x| x.as_u64()).unwrap_or(60),
            has_secret: v.get("secret").is_some_and(|s| !s.is_null()),
            target_count: v.get("targets").and_then(|x| x.as_array()).map_or(0, Vec::len),
        }),
        "argocd" => GitopsBackendView::Argocd(ArgocdView {
            namespace: v.get("namespace").and_then(|x| x.as_str()).unwrap_or("argocd").into(),
            application: v.get("application").and_then(|x| x.as_str()).unwrap_or_default().into(),
            source_url: v.get("source_url").and_then(|x| x.as_str()).unwrap_or_default().into(),
            target_revision: v
                .get("target_revision")
                .and_then(|x| x.as_str())
                .unwrap_or("main")
                .into(),
            source_path: v.get("source_path").and_then(|x| x.as_str()).unwrap_or("./").into(),
            destination_namespace: v
                .get("destination_namespace")
                .and_then(|x| x.as_str())
                .unwrap_or("default")
                .into(),
            sync_mode: v
                .get("sync_policy")
                .and_then(|p| p.get("mode"))
                .and_then(|x| x.as_str())
                .unwrap_or("automatic")
                .into(),
            has_secret: v.get("secret").is_some_and(|s| !s.is_null()),
            target_count: v.get("targets").and_then(|x| x.as_array()).map_or(0, Vec::len),
        }),
        _ => GitopsBackendView::None,
    }
}

// ── Status row computation ────────────────────────────────────────

fn status_row_agent(cluster: &str) -> StatusRow {
    // Best-effort: check whether the plist file exists. Active
    // load state requires `launchctl print`; we avoid spawning
    // subprocesses in the read path (operators run `kikai status`
    // for the canonical answer).
    let label = format!("io.pleme.kikai.{cluster}");
    let home = std::env::var("HOME").unwrap_or_default();
    let plist = PathBuf::from(home)
        .join("Library/LaunchAgents")
        .join(format!("{label}.plist"));
    if plist.exists() {
        StatusRow {
            name: "Agent".into(),
            outcome: StatusOutcome::Ok,
            detail: format!("plist present at {}", plist.display()),
        }
    } else {
        StatusRow {
            name: "Agent".into(),
            outcome: StatusOutcome::NotApplicable,
            detail: "no launchd plist installed (non-darwin or not deployed)".into(),
        }
    }
}

fn status_row_vm(data_dir: &std::path::Path) -> StatusRow {
    let pid_file = data_dir.join("vm.pid");
    if !pid_file.exists() {
        return StatusRow {
            name: "VM".into(),
            outcome: StatusOutcome::Fail,
            detail: "no vm.pid file — daemon not bootstrapped or VM not launched".into(),
        };
    }
    let Ok(contents) = std::fs::read_to_string(&pid_file) else {
        return StatusRow {
            name: "VM".into(),
            outcome: StatusOutcome::Warn,
            detail: format!("vm.pid present but unreadable at {}", pid_file.display()),
        };
    };
    let Ok(pid) = contents.trim().parse::<i32>() else {
        return StatusRow {
            name: "VM".into(),
            outcome: StatusOutcome::Warn,
            detail: format!("vm.pid has non-integer content: {contents:?}"),
        };
    };
    // Cheap liveness check — SIGCONT(18) → 0 if reachable, ESRCH if dead.
    // Using kill(pid, 0) which sends no signal but checks existence.
    let alive = unsafe { libc_kill_check(pid) };
    if alive {
        StatusRow {
            name: "VM".into(),
            outcome: StatusOutcome::Ok,
            detail: format!("vm pid {pid} alive"),
        }
    } else {
        StatusRow {
            name: "VM".into(),
            outcome: StatusOutcome::Fail,
            detail: format!("vm.pid={pid} but process is dead"),
        }
    }
}

/// libc kill(pid, 0) — true if the pid is reachable. Linux + darwin
/// both have this; we wrap it with no_mangle FFI to avoid pulling
/// the whole `libc` crate just for this one call.
unsafe fn libc_kill_check(pid: i32) -> bool {
    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    unsafe { kill(pid, 0) == 0 }
}

fn status_row_api(kubeconfig_exists: bool) -> StatusRow {
    if kubeconfig_exists {
        StatusRow {
            name: "API".into(),
            outcome: StatusOutcome::Ok,
            detail: "kubeconfig present (probe via `kikai status` for live reachability)".into(),
        }
    } else {
        StatusRow {
            name: "API".into(),
            outcome: StatusOutcome::Fail,
            detail: "kubeconfig absent — cluster API not yet bootstrapped".into(),
        }
    }
}

fn status_row_snapshot(snap: &Option<SnapshotMetaView>) -> StatusRow {
    match snap {
        None => StatusRow {
            name: "Snapshot".into(),
            outcome: StatusOutcome::NotApplicable,
            detail: "no auto-snapshot present (next bring-up will be a fresh boot)".into(),
        },
        Some(s) if s.all_paths_exist => StatusRow {
            name: "Snapshot".into(),
            outcome: StatusOutcome::Ok,
            detail: format!("schema v{} — fast-path resume available", s.schema_version),
        },
        Some(s) => StatusRow {
            name: "Snapshot".into(),
            outcome: StatusOutcome::Warn,
            detail: format!(
                "schema v{} but referenced store paths are missing — will fall back to fresh boot",
                s.schema_version
            ),
        },
    }
}

// ── Snapshot meta parsing ─────────────────────────────────────────

#[derive(serde::Deserialize)]
struct AutoSnapshotMetaOnDisk {
    schema: u32,
    kernel: String,
    initrd: String,
    init: String,
    root_disk: String,
}

fn read_snapshot_meta(
    cluster: &str,
    data_dir: &std::path::Path,
) -> Result<Option<SnapshotMetaView>, ReaderError> {
    let state_path = data_dir.join("snapshots/auto.vmstate");
    let meta_path = data_dir.join("snapshots/auto.meta.json");
    if !state_path.exists() || !meta_path.exists() {
        return Ok(None);
    }
    let meta_str = std::fs::read_to_string(&meta_path).map_err(|source| ReaderError::Io {
        what: meta_path.display().to_string(),
        source,
    })?;
    let meta: AutoSnapshotMetaOnDisk = serde_json::from_str(&meta_str).map_err(|e| {
        ReaderError::InvalidState(format!("snapshot meta parse failed: {e}"))
    })?;
    let state_size = std::fs::metadata(&state_path).map(|m| m.len()).unwrap_or(0);
    let all_paths_exist = [&meta.kernel, &meta.initrd, &meta.init, &meta.root_disk]
        .iter()
        .all(|p| std::path::Path::new(p).exists());
    Ok(Some(SnapshotMetaView {
        cluster: cluster.to_string(),
        state_path: state_path.display().to_string(),
        state_size_bytes: state_size,
        meta_path: meta_path.display().to_string(),
        schema_version: meta.schema,
        kernel: meta.kernel,
        initrd: meta.initrd,
        init: meta.init,
        root_disk: meta.root_disk,
        all_paths_exist,
    }))
}

// ── Auth method detection ─────────────────────────────────────────

/// Open the kubeconfig and look for typed shape markers. We don't
/// parse YAML strictly — we look for the presence of `token:`,
/// `client-certificate-data:`, or `exec:` under `users[].user`,
/// which is enough to classify. No secret material is captured
/// in the result — only the auth-method tag.
fn detect_auth_method(path: &std::path::Path) -> AuthMethod {
    let Ok(contents) = std::fs::read_to_string(path) else {
        return AuthMethod::Unknown;
    };
    // Order matters: exec > clientCert > token > anonymous.
    if contents.contains("exec:") {
        AuthMethod::Exec
    } else if contents.contains("client-certificate-data") || contents.contains("client-certificate:") {
        AuthMethod::ClientCert
    } else if contents.contains("token:") {
        AuthMethod::BearerToken
    } else if contents.contains("users:") {
        // Has a users section but no recognized credential shape.
        AuthMethod::Anonymous
    } else {
        AuthMethod::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup_home_with_cluster(yaml: &str) -> (TempDir, KikaiClusterReader) {
        let tmp = TempDir::new().unwrap();
        let cfg = tmp.path().join(".config/kikai");
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::write(cfg.join("clusters.yaml"), yaml).unwrap();
        let reader = KikaiClusterReader::new(tmp.path().to_path_buf());
        (tmp, reader)
    }

    fn sample_yaml() -> String {
        r#"
demo:
  vm_mode: k3s
  cpus: 4
  memory: 8192
  disk_size: 50G
  api_port: 6443
  ssh_port: 2222
  mac_address: "5a:94:ef:00:00:01"
  vm_ip: "192.168.64.10"
  boot_timeout_secs: 300
  gitops:
    backend: fluxcd
    namespace: flux-system
    source_url: https://github.com/stefanprodan/podinfo
    source_ref: master
    source_path: ./kustomize
    target_namespace: default
    interval_secs: 60
    kustomizations: []
    targets: []
    secret: null
    wait_timeout_secs: null
"#
        .to_string()
    }

    #[tokio::test]
    async fn unknown_cluster_yields_typed_error() {
        let (_t, reader) = setup_home_with_cluster(&sample_yaml());
        let err = reader.cluster_config("missing").await.unwrap_err();
        assert_eq!(err.kind(), "unknown_cluster");
    }

    #[tokio::test]
    async fn known_cluster_parses_typed_view() {
        let (_t, reader) = setup_home_with_cluster(&sample_yaml());
        let view = reader.cluster_config("demo").await.unwrap();
        assert_eq!(view.cluster, "demo");
        assert_eq!(view.cpus, 4);
        assert_eq!(view.memory_mib, 8192);
        match view.gitops {
            GitopsBackendView::Fluxcd(f) => {
                assert_eq!(f.source_url, "https://github.com/stefanprodan/podinfo");
                assert_eq!(f.target_namespace, "default");
                assert!(!f.has_secret);
            }
            other => panic!("expected Fluxcd, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn missing_clusters_yaml_yields_io_error() {
        let tmp = TempDir::new().unwrap();
        let reader = KikaiClusterReader::new(tmp.path().to_path_buf());
        let err = reader.cluster_config("demo").await.unwrap_err();
        assert_eq!(err.kind(), "io");
    }

    #[tokio::test]
    async fn snapshot_meta_returns_none_when_absent() {
        let (_t, reader) = setup_home_with_cluster(&sample_yaml());
        let snap = reader.snapshot_meta("demo").await.unwrap();
        assert!(snap.is_none());
    }

    #[tokio::test]
    async fn snapshot_meta_parses_when_present() {
        let (_t, reader) = setup_home_with_cluster(&sample_yaml());
        let data = _t.path().join(".local/share/kikai/demo/snapshots");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join("auto.vmstate"), [0u8; 16]).unwrap();
        let meta = serde_json::json!({
            "schema": 2,
            "kernel": "/nix/store/x/Image",
            "initrd": "/nix/store/x/initrd",
            "init": "/nix/store/x/init",
            "root_disk": "/nix/store/x/disk"
        });
        std::fs::write(
            data.join("auto.meta.json"),
            serde_json::to_string(&meta).unwrap(),
        )
        .unwrap();
        let snap = reader.snapshot_meta("demo").await.unwrap().unwrap();
        assert_eq!(snap.schema_version, 2);
        // Store paths in the meta point at nowhere real — should
        // surface that explicitly so operators don't trust stale meta.
        assert!(!snap.all_paths_exist);
    }

    #[test]
    fn auth_method_detection_recognises_each_shape() {
        let tmp = TempDir::new().unwrap();
        for (body, expected) in [
            ("users:\n- user:\n    token: redacted\n", AuthMethod::BearerToken),
            (
                "users:\n- user:\n    client-certificate-data: redacted\n",
                AuthMethod::ClientCert,
            ),
            (
                "users:\n- user:\n    exec:\n      command: aws\n",
                AuthMethod::Exec,
            ),
            ("users:\n- user: {}\n", AuthMethod::Anonymous),
            ("not a kubeconfig\n", AuthMethod::Unknown),
        ] {
            let path = tmp.path().join("kc");
            std::fs::write(&path, body).unwrap();
            assert_eq!(detect_auth_method(&path), expected, "body={body:?}");
        }
    }
}
