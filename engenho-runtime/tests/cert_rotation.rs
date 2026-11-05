#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

mod support;

use std::path::Path;

use engenho_apiserver::pki_inventory::{CertFact, inventory};
use engenho_apiserver::{PkiFile, issue_admin_client_material, load_or_generate_ca};
use engenho_config::{CertRotation, EngenhoConfig, KubeletBackendKind};
use engenho_runtime::Runtime;
use shikumi::TieredConfig;

const STANDING_NOT_AFTER: i64 = 4_102_444_800;
const YEAR: i64 = 365 * 86_400;

fn config(data_dir: &Path, rotation: CertRotation) -> EngenhoConfig {
    let mut cfg = EngenhoConfig::prescribed_default();
    cfg.runtime.listen_addr = "127.0.0.1:0".into();
    cfg.runtime.kubelet_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.etcd_listen_addr = "127.0.0.1:0".into();
    support::place(&mut cfg, data_dir);
    cfg.runtime.durable = true;
    cfg.runtime.node_name = "node-A".into();
    cfg.runtime.kubelet_backend = KubeletBackendKind::Fake;
    cfg.runtime.leadership_timeout_seconds = 10;
    cfg.runtime.tls.enabled = true;
    cfg.runtime.kubeconfig_publish_path = String::new();
    cfg.runtime.cert_rotation = rotation;
    cfg
}

fn admin_not_after(data_dir: &Path) -> i64 {
    match inventory(data_dir).admin_cert {
        CertFact::Present { not_after, .. } => not_after,
        other => panic!("{other:?}"),
    }
}

async fn status_with(rt: &Runtime, ca: &str, cert: &str, key: &str) -> u16 {
    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .add_root_certificate(reqwest::Certificate::from_pem(ca.as_bytes()).unwrap())
        .identity(reqwest::Identity::from_pem(format!("{cert}\n{key}").as_bytes()).unwrap())
        .build()
        .unwrap();
    client
        .get(format!(
            "https://127.0.0.1:{}/api/v1/namespaces",
            rt.local_addr().port()
        ))
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_with_long_lived_certs_boots_bounded_and_a_copied_kubeconfig_keeps_working() {
    let tmp = tempfile::tempdir().unwrap();
    let ca = load_or_generate_ca(tmp.path()).unwrap();
    let copied = issue_admin_client_material(&ca).unwrap();
    std::fs::write(PkiFile::AdminCert.path(tmp.path()), &copied.cert_pem).unwrap();
    std::fs::write(PkiFile::AdminKey.path(tmp.path()), &copied.key_pem).unwrap();
    assert_eq!(admin_not_after(tmp.path()), STANDING_NOT_AFTER);
    let ca_before = std::fs::read_to_string(PkiFile::CaCert.path(tmp.path())).unwrap();

    let rt = Runtime::boot(config(tmp.path(), CertRotation::Renew))
        .await
        .expect("an existing data dir boots");

    assert_eq!(
        std::fs::read_to_string(PkiFile::CaCert.path(tmp.path())).unwrap(),
        ca_before,
        "the CA is kept"
    );
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    )
    .unwrap();
    let end = admin_not_after(tmp.path());
    assert!(end > now && end <= now + YEAR, "{end}");
    assert_eq!(
        status_with(&rt, &ca_before, &copied.cert_pem, &copied.key_pem).await,
        200,
        "a kubeconfig copied before rotation keeps authenticating"
    );
    let published = std::fs::read_to_string(PkiFile::AdminCert.path(tmp.path())).unwrap();
    let key = std::fs::read_to_string(PkiFile::AdminKey.path(tmp.path())).unwrap();
    assert_eq!(status_with(&rt, &ca_before, &published, &key).await, 200);
    rt.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rotation_configured_off_keeps_the_standing_span() {
    let tmp = tempfile::tempdir().unwrap();
    let rt = Runtime::boot(config(tmp.path(), CertRotation::Off))
        .await
        .unwrap();
    assert_eq!(admin_not_after(tmp.path()), STANDING_NOT_AFTER);
    rt.shutdown().await.unwrap();
}
