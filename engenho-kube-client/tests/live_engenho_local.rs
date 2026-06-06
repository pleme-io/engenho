//! Live integration test: exercise the engenho-kube-client end-to-end
//! against the operator's `engenho-local` cluster.
//!
//! Skipped when `~/.kube/configs/engenho-local` is absent — so this
//! file is safe to ship in CI without the cluster on the runner.
//! Locally (with the cluster up), exercises the FULL engenho stack:
//!
//!   Kubeconfig::load → resolve_connection → ReqwestKubeClient
//!     → KubeClient::list<Pod> via reqwest/rustls
//!     → engenho-types::Pod (typed PodSpec / PodStatus)
//!     → assertion that podinfo replicas show up + parse correctly
//!
//! This is the first test that exercises engenho-types' typed
//! resource catalog against a real k3s API. M0.0.2 milestone
//! validation — if it goes green we know the stack composes.

use std::path::PathBuf;

use engenho_kube_client::{client::ReqwestKubeClient, config::Kubeconfig};
use engenho_types::client::{KubeClient, ListOptions};
use engenho_types::generated_v1_34::core_v1::Pod;

fn engenho_local_kubeconfig_path() -> Option<PathBuf> {
    let home = std::env::var_os("HOME")?;
    let path = PathBuf::from(home).join(".kube/configs/engenho-local");
    if path.exists() { Some(path) } else { None }
}

#[tokio::test]
#[ignore = "live: requires a reachable engenho-local cluster; run with --ignored"]
async fn list_pods_from_engenho_local() {
    let Some(kc_path) = engenho_local_kubeconfig_path() else {
        eprintln!("skipping: ~/.kube/configs/engenho-local not present");
        return;
    };

    let kc = Kubeconfig::load(&kc_path).expect("parse engenho-local kubeconfig");
    let conn = kc
        .resolve_connection()
        .expect("resolve engenho-local connection");
    let client = ReqwestKubeClient::new(conn);

    let pods = client
        .list::<Pod>(Some("default"), &ListOptions::default())
        .await
        .expect("LIST pods in default namespace");

    eprintln!(
        "list_pods: {} pods in default ns; resource_version={}",
        pods.items.len(),
        pods.resource_version
    );

    // The default namespace should contain the reconciled podinfo
    // workload (engenho-local's fluxcd backend points at
    // stefanprodan/podinfo). Two replicas per the kustomize/
    // deployment.
    let podinfo: Vec<&Pod> = pods
        .items
        .iter()
        .filter(|p| p.metadata.name.starts_with("podinfo-"))
        .collect();
    assert!(
        !podinfo.is_empty(),
        "no podinfo pods found in default namespace — is the cluster reconciled?"
    );

    // Validate typed PodSpec parsed: container name + image.
    let first = podinfo[0];
    let pod_spec = first
        .spec
        .as_ref()
        .expect("podinfo pod has no typed spec — check engenho-types PodSpec expansion");
    assert!(
        !pod_spec.containers.is_empty(),
        "podinfo pod has no containers in typed spec — check engenho-types PodSpec expansion"
    );
    let c = &pod_spec.containers[0];
    assert_eq!(c.name, "podinfod", "container name mismatch: {}", c.name);
    let img = c.image.as_deref().unwrap_or_default();
    assert!(
        img.contains("podinfo"),
        "container image doesn't reference podinfo: {img}"
    );

    // Validate typed PodStatus parsed for running pod: phase +
    // container status.
    let status = first
        .status
        .as_ref()
        .expect("typed PodStatus must be present on Running pod");
    use engenho_types::curated_enums::PodPhase;
    assert_eq!(
        status.phase,
        Some(PodPhase::Running),
        "podinfo should be Running; got phase={:?}",
        status.phase
    );
    assert!(
        !status.container_statuses.is_empty(),
        "no typed containerStatuses on Running pod"
    );
    let cs = &status.container_statuses[0];
    assert_eq!(cs.name, "podinfod");
    assert!(cs.ready, "podinfod container should be ready");
}

#[tokio::test]
#[ignore = "live: requires a reachable engenho-local cluster; run with --ignored"]
async fn get_specific_pod_from_engenho_local() {
    let Some(kc_path) = engenho_local_kubeconfig_path() else {
        eprintln!("skipping: ~/.kube/configs/engenho-local not present");
        return;
    };

    let kc = Kubeconfig::load(&kc_path).expect("parse kubeconfig");
    let conn = kc.resolve_connection().expect("resolve");
    let client = ReqwestKubeClient::new(conn);

    // First LIST to discover an existing pod name (cluster has
    // dynamic suffixes).
    let pods = client
        .list::<Pod>(Some("default"), &ListOptions::default())
        .await
        .expect("LIST default ns");
    let Some(target) = pods
        .items
        .iter()
        .find(|p| p.metadata.name.starts_with("podinfo-"))
    else {
        eprintln!("skipping: no podinfo pod to GET — is cluster reconciled?");
        return;
    };

    let by_name: Pod = client
        .get::<Pod>(Some("default"), &target.metadata.name)
        .await
        .expect("GET specific podinfo pod");

    assert_eq!(by_name.metadata.name, target.metadata.name);
    assert_eq!(by_name.metadata.namespace.as_deref(), Some("default"));
}
