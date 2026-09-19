//! I38 / node T5.9: a node configured `kubelet_backend: cri` fails
//! `Runtime::start` with the kubelet's typed refusal, before anything boots.
//!
//! | configured | podman on the host | what `Runtime::start` returns |
//! |---|---|---|
//! | `cri` | unusable | `RuntimeError::BackendRefused`, the kubelet's own refusal |
//! | `cri` | usable | the same: the refusal consults nothing on the host |
//!
//! Before I38 the preflight probed podman for `cri`, so the first row
//! returned `ContainerRuntimeUnavailable { backend: "podman" }`: an error
//! naming a runtime the node was configured not to use. Only the second row
//! reached the refusal. The test pins the first row, with a podman binary
//! that cannot work, so it fails on every machine the old order runs on.
//!
//! The data directory is a path that does not exist yet. It still does not
//! exist afterwards: the refusal lands before the store, the PKI or any
//! listener is created.

use engenho_config::{EngenhoConfig, KubeletBackendKind};
use engenho_runtime::{Runtime, RuntimeError};
use shikumi::TieredConfig;

#[tokio::test]
async fn a_cri_node_fails_start_with_the_typed_refusal_not_a_podman_error() {
    let scratch = tempfile::tempdir().expect("a scratch directory");
    let data_dir = scratch.path().join("never-created");

    let mut cfg = EngenhoConfig::prescribed_default();
    cfg.runtime.listen_addr = "127.0.0.1:0".into();
    cfg.runtime.kubelet_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.etcd_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.data_dir = data_dir.clone();
    cfg.runtime.tls.enabled = false;
    cfg.runtime.kubelet_backend = KubeletBackendKind::Cri;
    cfg.runtime.podman_binary = Some("/nonexistent/definitely-not-podman".into());

    let expected = engenho_kubelet::config_bridge::construction_refusal(
        engenho_kubelet::config_bridge::KubeletBackendKind::Cri,
    )
    .expect(
        "CRI is refused while cri_backend::UNSUPPORTED is non-empty. Once it is \
         admitted this premise is gone: give preflight_backend's Cri arm the probe \
         its pending-cri note names, then rewrite this test",
    );

    match Runtime::start(cfg).await {
        Err(RuntimeError::BackendRefused(refused)) => assert_eq!(
            refused, expected,
            "the node must carry the kubelet's refusal verbatim"
        ),
        Err(other) => panic!("a cri node must fail with the typed refusal, got: {other}"),
        Ok(_) => panic!("a cri node booted, but the kubelet refuses to construct CRI"),
    }
    assert!(
        !data_dir.exists(),
        "the refusal must land before anything is written under data_dir"
    );
}
