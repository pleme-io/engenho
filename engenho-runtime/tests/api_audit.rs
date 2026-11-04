//! `runtime.api_audit` — a daemon that names nothing records who did what, a
//! denied request included; a daemon that says `off` writes no log.

mod support;

use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use engenho_config::{ApiAudit, EngenhoConfig, KubeletBackendKind};
use engenho_runtime::Runtime;
use reqwest::StatusCode;
use serde_json::{Value, json};
use shikumi::TieredConfig;

const RECORDED_WITHIN: Duration = Duration::from_secs(10);

fn config(data_dir: &Path) -> EngenhoConfig {
    let mut cfg = EngenhoConfig::prescribed_default();
    cfg.runtime.listen_addr = "127.0.0.1:0".into();
    cfg.runtime.kubelet_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.etcd_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.durable = false;
    cfg.runtime.node_name = "node-A".into();
    cfg.runtime.kubelet_backend = KubeletBackendKind::Fake;
    cfg.runtime.leadership_timeout_seconds = 5;
    cfg.runtime.tls.enabled = false;
    support::place(&mut cfg, data_dir);
    cfg
}

fn log_path(data_dir: &Path) -> PathBuf {
    data_dir.join("audit").join("audit.log")
}

fn events(data_dir: &Path) -> Vec<Value> {
    std::fs::read_to_string(log_path(data_dir))
        .unwrap_or_default()
        .lines()
        .map(|l| serde_json::from_str(l).expect("one JSON object per line"))
        .collect()
}

async fn requests_as_admin_and_as_nobody(rt: &Runtime, data_dir: &Path) -> String {
    let base = format!("http://{}", rt.local_addr());
    let token = std::fs::read_to_string(data_dir.join("pki/admin.token"))
        .expect("the runtime minted the admin token")
        .trim()
        .to_string();
    let client = reqwest::Client::new();
    let denied = client
        .delete(format!("{base}/api/v1/namespaces/default/secrets/db"))
        .send()
        .await
        .expect("the apiserver answers");
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    let created = client
        .post(format!("{base}/api/v1/namespaces/default/configmaps"))
        .bearer_auth(&token)
        .json(&json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": { "name": "audited", "namespace": "default" },
        }))
        .send()
        .await
        .expect("the apiserver answers");
    assert!(created.status().is_success(), "{}", created.status());
    token
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_daemon_that_names_nothing_records_who_did_what() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = config(dir.path());
    assert_eq!(cfg.runtime.api_audit, ApiAudit::Record);
    let rt = Runtime::start(cfg).await.expect("the runtime boots");
    let token = requests_as_admin_and_as_nobody(&rt, dir.path()).await;

    let deadline = Instant::now() + RECORDED_WITHIN;
    while events(dir.path()).len() < 2 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let events = events(dir.path());
    let denied = events
        .iter()
        .find(|e| e["verb"] == "delete")
        .unwrap_or_else(|| panic!("the denied delete is recorded: {events:?}"));
    assert_eq!(denied["objectRef"]["resource"], "secrets");
    assert_eq!(denied["objectRef"]["name"], "db");
    assert_eq!(denied["responseStatus"]["code"], 403);
    assert_eq!(
        denied["annotations"]["authorization.k8s.io/decision"],
        "forbid"
    );
    assert_eq!(denied["user"]["username"], "system:anonymous");
    let created = events
        .iter()
        .find(|e| e["verb"] == "create" && e["objectRef"]["resource"] == "configmaps")
        .unwrap_or_else(|| panic!("the create is recorded: {events:?}"));
    assert_eq!(
        created["annotations"]["authorization.k8s.io/decision"],
        "allow"
    );
    assert_ne!(created["user"]["username"], "system:anonymous");

    let text = std::fs::read_to_string(log_path(dir.path())).expect("the log exists");
    assert!(!text.contains(&token), "the admin token reached the log");
    let mode = std::fs::metadata(log_path(dir.path()))
        .expect("the log exists")
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600);
    rt.shutdown().await.expect("a clean stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_daemon_configured_off_writes_no_log() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut cfg = config(dir.path());
    cfg.runtime.api_audit = ApiAudit::Off;
    let rt = Runtime::start(cfg).await.expect("the runtime boots");
    requests_as_admin_and_as_nobody(&rt, dir.path()).await;
    rt.shutdown().await.expect("a clean stop");
    assert!(
        !dir.path().join("audit").exists(),
        "`off` created the audit directory"
    );
}
