//! M0.6 — namespaced reconcile-chain regression suite.
//!
//! The end-to-end regression for the namespace-propagation bug (commit
//! 7e09b84): the DeploymentController created child ReplicaSets, and the
//! ReplicaSetController created Pods, in the controller's SCOPE namespace
//! ("default") instead of the PARENT object's namespace. A Deployment in
//! ns `team-x` produced RS+Pods in `default` (wrong ns + a
//! recreate-every-tick hot loop because the owned-children query, scoped
//! to `team-x` by owner-ref, never saw the children it created).
//!
//! This suite boots the SAME in-process apiserver + StoreMesh + the
//! deployment/replicaset/scheduler/endpoints controllers `m0_1` wires
//! (FakeBackend — no real container runtime, runs in CI), then proves the
//! chain converges in the PARENT namespace and NOTHING lands in `default`:
//!
//!   1. `namespaced_chain_lands_in_parent_namespace` — POST a Namespace
//!      `team-x` + a 2-replica Deployment in `team-x`; assert the RS + 2
//!      Pods all appear in `team-x` (each Pod's `metadata.namespace ==
//!      "team-x"`), the Deployment status reaches `replicas:2`, AND
//!      NEGATIVELY that `default` holds zero replicasets/pods.
//!   2. `namespaced_chain_does_not_thrash` — after convergence, run more
//!      reconcile ticks and assert the RS is NOT recreated (same uid/name
//!      persists; no duplicate RS) — pins the hot-loop fix.
//!   3. `same_name_deployments_in_two_namespaces_do_not_collide` — two
//!      Deployments with the SAME name in namespaces `a` and `b` each get
//!      their own RS+Pods in their own namespace; they don't collide.

use std::sync::Arc;
use std::time::{Duration, Instant};

use engenho_config::{EngenhoConfig, KubeletBackendKind};
use engenho_kubelet::{ContainerRuntime, FakeBackend};
use engenho_runtime::Runtime;
use shikumi::TieredConfig;

// =====================================================================
// Config + admin client (identical to m0_1's rig)
// =====================================================================

fn durable_config(data_dir: &std::path::Path) -> EngenhoConfig {
    let mut cfg = EngenhoConfig::prescribed_default();
    cfg.runtime.listen_addr = "127.0.0.1:0".into();
    cfg.runtime.data_dir = data_dir.to_path_buf();
    cfg.runtime.durable = true;
    cfg.runtime.node_name = "node-A".into();
    cfg.runtime.kubelet_backend = KubeletBackendKind::Fake;
    cfg.runtime.leadership_timeout_seconds = 5;
    cfg.runtime.tls.enabled = false;
    cfg.controllers.enable.deployment = true;
    cfg.controllers.enable.replicaset = true;
    cfg.controllers.enable.endpoints = true;
    cfg.controllers.enable.gc = true;
    cfg.controllers.fallback_interval_seconds = 1;
    cfg.controllers.debounce_milliseconds = 20;
    cfg
}

fn admin_client(data_dir: &std::path::Path) -> reqwest::Client {
    let token = std::fs::read_to_string(data_dir.join("pki/admin.token"))
        .expect("runtime minted the admin bearer token")
        .trim()
        .to_string();
    let mut headers = reqwest::header::HeaderMap::new();
    let mut auth = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
        .expect("valid bearer header");
    auth.set_sensitive(true);
    headers.insert(reqwest::header::AUTHORIZATION, auth);
    reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .expect("admin client builds")
}

// =====================================================================
// HTTP + poll helpers (mirror m0_1)
// =====================================================================

async fn poll_until<F, Fut, T>(timeout: Duration, interval: Duration, mut predicate: F) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(v) = predicate().await {
            return Some(v);
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(interval).await;
    }
}

async fn http_list(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    path: &str,
) -> Vec<serde_json::Value> {
    let resp = client
        .get(format!("http://{addr}{path}"))
        .send()
        .await
        .expect("LIST request");
    if resp.status() != reqwest::StatusCode::OK {
        return Vec::new();
    }
    let list: serde_json::Value = resp.json().await.expect("LIST json");
    list.get("items")
        .and_then(|i| i.as_array())
        .cloned()
        .unwrap_or_default()
}

async fn http_get(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    path: &str,
) -> Option<serde_json::Value> {
    let resp = client
        .get(format!("http://{addr}{path}"))
        .send()
        .await
        .ok()?;
    if resp.status() != reqwest::StatusCode::OK {
        return None;
    }
    resp.json().await.ok()
}

fn is_owned_by(child: &serde_json::Value, owner_uid: &str) -> bool {
    child
        .get("metadata")
        .and_then(|m| m.get("ownerReferences"))
        .and_then(|o| o.as_array())
        .is_some_and(|refs| {
            refs.iter().any(|r| {
                r.get("uid").and_then(|u| u.as_str()) == Some(owner_uid)
                    && r.get("controller").and_then(serde_json::Value::as_bool) == Some(true)
            })
        })
}

fn obj_namespace(o: &serde_json::Value) -> Option<&str> {
    o.get("metadata")
        .and_then(|m| m.get("namespace"))
        .and_then(|n| n.as_str())
}

fn obj_name(o: &serde_json::Value) -> Option<&str> {
    o.get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|n| n.as_str())
}

fn obj_uid(o: &serde_json::Value) -> Option<&str> {
    o.get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|n| n.as_str())
}

// =====================================================================
// manifests
// =====================================================================

fn namespace_body(name: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": name }
    })
}

/// A 2-replica `podinfo` Deployment. `name` so two namespaces can carry
/// the SAME-named Deployment (the collision test).
fn deployment_body(name: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": name },
        "spec": {
            "replicas": 2,
            "selector": { "matchLabels": { "app": name } },
            "template": {
                "metadata": { "labels": { "app": name } },
                "spec": { "containers": [{ "name": "main", "image": "podinfo:6" }] }
            }
        }
    })
}

/// POST a Namespace then a Deployment named `dep` into namespace `ns`;
/// return the Deployment uid.
async fn post_namespace_and_deployment(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    ns: &str,
    dep: &str,
) -> String {
    let ns_resp = client
        .post(format!("http://{addr}/api/v1/namespaces"))
        .json(&namespace_body(ns))
        .send()
        .await
        .expect("POST namespace");
    assert_eq!(
        ns_resp.status(),
        reqwest::StatusCode::CREATED,
        "namespace {ns} POST should 201"
    );

    let resp = client
        .post(format!(
            "http://{addr}/apis/apps/v1/namespaces/{ns}/deployments"
        ))
        .json(&deployment_body(dep))
        .send()
        .await
        .expect("POST deployment");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::CREATED,
        "deployment {dep} in {ns} POST should 201"
    );
    let created: serde_json::Value = resp.json().await.unwrap();
    obj_uid(&created)
        .expect("deployment got a uid")
        .to_string()
}

const TIMEOUT: Duration = Duration::from_secs(15);
const INTERVAL: Duration = Duration::from_millis(50);

/// Poll until exactly one RS in `ns` is owned by `dep_uid`; return it.
async fn await_owned_rs(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    ns: &str,
    dep_uid: &str,
) -> serde_json::Value {
    poll_until(TIMEOUT, INTERVAL, || async {
        let items = http_list(
            client,
            addr,
            &format!("/apis/apps/v1/namespaces/{ns}/replicasets"),
        )
        .await;
        let owned: Vec<_> = items.into_iter().filter(|r| is_owned_by(r, dep_uid)).collect();
        if owned.len() == 1 { Some(owned[0].clone()) } else { None }
    })
    .await
    .unwrap_or_else(|| panic!("exactly 1 RS owned by {dep_uid} never appeared in ns {ns}"))
}

/// Poll until exactly two Pods in `ns` are owned by `rs_uid`; return them.
async fn await_owned_pods(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    ns: &str,
    rs_uid: &str,
) -> Vec<serde_json::Value> {
    poll_until(TIMEOUT, INTERVAL, || async {
        let items = http_list(client, addr, &format!("/api/v1/namespaces/{ns}/pods")).await;
        let owned: Vec<_> = items.into_iter().filter(|p| is_owned_by(p, rs_uid)).collect();
        if owned.len() == 2 { Some(owned) } else { None }
    })
    .await
    .unwrap_or_else(|| panic!("expected 2 Pods owned by RS {rs_uid} in ns {ns}; never reached 2"))
}

// =====================================================================
// 1. The end-to-end regression: chain lands in the PARENT namespace
// =====================================================================

#[tokio::test]
async fn namespaced_chain_lands_in_parent_namespace() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(FakeBackend::new());
    let backend_dyn: Arc<dyn ContainerRuntime> = backend.clone();
    let rt = Runtime::start_with_backend(durable_config(tmp.path()), backend_dyn)
        .await
        .expect("Runtime boots");
    let addr = rt.local_addr();
    let client = admin_client(tmp.path());

    let dep_uid = post_namespace_and_deployment(&client, addr, "team-x", "podinfo").await;

    // RS appears in team-x (NOT default), spec.replicas == 2.
    let rs = await_owned_rs(&client, addr, "team-x", &dep_uid).await;
    assert_eq!(
        obj_namespace(&rs),
        Some("team-x"),
        "the RS must carry metadata.namespace=team-x"
    );
    assert_eq!(
        rs.get("spec").and_then(|s| s.get("replicas")).unwrap(),
        2,
        "RS carries the desired replica count"
    );
    let rs_uid = obj_uid(&rs).expect("RS uid").to_string();

    // The controller-created RS carries a non-empty creationTimestamp (the
    // Part-B compat fix: kubectl AGE renders a real duration, not <unknown>).
    let rs_ts = rs
        .get("metadata")
        .and_then(|m| m.get("creationTimestamp"))
        .and_then(|t| t.as_str())
        .unwrap_or("");
    assert!(
        !rs_ts.is_empty() && rs_ts.ends_with('Z'),
        "controller-created RS must carry a non-empty RFC3339 creationTimestamp; got {rs_ts:?}"
    );

    // 2 Pods appear in team-x, each with metadata.namespace == team-x.
    let pods = await_owned_pods(&client, addr, "team-x", &rs_uid).await;
    for pod in &pods {
        assert_eq!(
            obj_namespace(pod),
            Some("team-x"),
            "every Pod must carry metadata.namespace=team-x; pod={}",
            obj_name(pod).unwrap_or("?")
        );
        // Each controller-created Pod carries a non-empty creationTimestamp.
        let pod_ts = pod
            .get("metadata")
            .and_then(|m| m.get("creationTimestamp"))
            .and_then(|t| t.as_str())
            .unwrap_or("");
        assert!(
            !pod_ts.is_empty() && pod_ts.ends_with('Z'),
            "controller-created Pod {} must carry a non-empty creationTimestamp; got {pod_ts:?}",
            obj_name(pod).unwrap_or("?")
        );
    }

    // creationTimestamp is STABLE across reconciles (never bumped): re-read
    // the RS after several more ticks and assert the same timestamp.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let rs_again = await_owned_rs(&client, addr, "team-x", &dep_uid).await;
    let rs_ts_again = rs_again
        .get("metadata")
        .and_then(|m| m.get("creationTimestamp"))
        .and_then(|t| t.as_str())
        .unwrap_or("");
    assert_eq!(
        rs_ts, rs_ts_again,
        "RS creationTimestamp must be stable across reconciles (never bumped)"
    );

    // The Deployment status converges to replicas:2 (proves the controller
    // saw its OWN children — the hot-loop bug would have left status at 0).
    let converged = poll_until(TIMEOUT, INTERVAL, || async {
        let dep = http_get(
            &client,
            addr,
            "/apis/apps/v1/namespaces/team-x/deployments/podinfo",
        )
        .await?;
        let replicas = dep.get("status")?.get("replicas")?.as_i64()?;
        if replicas == 2 { Some(()) } else { None }
    })
    .await;
    assert!(
        converged.is_some(),
        "Deployment .status.replicas should converge to 2 in team-x"
    );

    // ── NEGATIVE: NOTHING was created in `default` ──────────────────────
    // The whole point of the bug fix. Give the chain time to (incorrectly)
    // leak into default before asserting it didn't.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let default_rs = http_list(&client, addr, "/apis/apps/v1/namespaces/default/replicasets").await;
    assert!(
        default_rs.is_empty(),
        "NO ReplicaSet may land in default; found {}: {:?}",
        default_rs.len(),
        default_rs.iter().filter_map(obj_name).collect::<Vec<_>>()
    );
    let default_pods = http_list(&client, addr, "/api/v1/namespaces/default/pods").await;
    assert!(
        default_pods.is_empty(),
        "NO Pod may land in default; found {}: {:?}",
        default_pods.len(),
        default_pods.iter().filter_map(obj_name).collect::<Vec<_>>()
    );

    rt.shutdown().await.expect("graceful shutdown");
}

// =====================================================================
// 2. No-thrash: the RS is not recreated after convergence
// =====================================================================

#[tokio::test]
async fn namespaced_chain_does_not_thrash() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(FakeBackend::new());
    let backend_dyn: Arc<dyn ContainerRuntime> = backend.clone();
    let rt = Runtime::start_with_backend(durable_config(tmp.path()), backend_dyn)
        .await
        .expect("Runtime boots");
    let addr = rt.local_addr();
    let client = admin_client(tmp.path());

    let dep_uid = post_namespace_and_deployment(&client, addr, "team-x", "podinfo").await;
    let rs = await_owned_rs(&client, addr, "team-x", &dep_uid).await;
    let rs_uid_before = obj_uid(&rs).expect("RS uid").to_string();
    let rs_name_before = obj_name(&rs).expect("RS name").to_string();
    // Drive the chain to its fixpoint (2 pods) before measuring stability.
    let _pods = await_owned_pods(&client, addr, "team-x", &rs_uid_before).await;

    // Let several fallback intervals (fallback = 1s) elapse with NO external
    // mutation — the window the recreate-every-tick hot loop would manifest in.
    tokio::time::sleep(Duration::from_secs(4)).await;

    // EXACTLY ONE RS still owned by the Deployment, SAME uid + name (never
    // recreated; no duplicate).
    let rses: Vec<_> = http_list(&client, addr, "/apis/apps/v1/namespaces/team-x/replicasets")
        .await
        .into_iter()
        .filter(|r| is_owned_by(r, &dep_uid))
        .collect();
    assert_eq!(
        rses.len(),
        1,
        "exactly one owned RS must persist (no recreate-every-tick duplicates); found {}: {:?}",
        rses.len(),
        rses.iter().filter_map(obj_name).collect::<Vec<_>>()
    );
    assert_eq!(
        obj_uid(&rses[0]),
        Some(rs_uid_before.as_str()),
        "the RS uid must be STABLE — a recreated RS would carry a fresh uid"
    );
    assert_eq!(
        obj_name(&rses[0]),
        Some(rs_name_before.as_str()),
        "the RS name must be stable"
    );

    // Pods also stable at 2 (no thrash-recreate beneath the RS).
    let pods: Vec<_> = http_list(&client, addr, "/api/v1/namespaces/team-x/pods")
        .await
        .into_iter()
        .filter(|p| is_owned_by(p, &rs_uid_before))
        .collect();
    assert_eq!(pods.len(), 2, "pod count must be stable at 2 (no thrash)");

    rt.shutdown().await.expect("graceful shutdown");
}

// =====================================================================
// 3. Multi-namespace isolation: same-name Deployments don't collide
// =====================================================================

#[tokio::test]
async fn same_name_deployments_in_two_namespaces_do_not_collide() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(FakeBackend::new());
    let backend_dyn: Arc<dyn ContainerRuntime> = backend.clone();
    let rt = Runtime::start_with_backend(durable_config(tmp.path()), backend_dyn)
        .await
        .expect("Runtime boots");
    let addr = rt.local_addr();
    let client = admin_client(tmp.path());

    // SAME deployment name "web" in two DIFFERENT namespaces.
    let uid_a = post_namespace_and_deployment(&client, addr, "a", "web").await;
    let uid_b = post_namespace_and_deployment(&client, addr, "b", "web").await;
    assert_ne!(uid_a, uid_b, "the two Deployments are distinct objects");

    // Each gets its OWN RS + 2 Pods in its OWN namespace.
    let rs_a = await_owned_rs(&client, addr, "a", &uid_a).await;
    let rs_b = await_owned_rs(&client, addr, "b", &uid_b).await;
    assert_eq!(obj_namespace(&rs_a), Some("a"));
    assert_eq!(obj_namespace(&rs_b), Some("b"));
    let rs_a_uid = obj_uid(&rs_a).unwrap().to_string();
    let rs_b_uid = obj_uid(&rs_b).unwrap().to_string();
    assert_ne!(rs_a_uid, rs_b_uid, "the two RSes are distinct");

    let pods_a = await_owned_pods(&client, addr, "a", &rs_a_uid).await;
    let pods_b = await_owned_pods(&client, addr, "b", &rs_b_uid).await;
    for p in &pods_a {
        assert_eq!(obj_namespace(p), Some("a"), "ns-a pod stays in a");
    }
    for p in &pods_b {
        assert_eq!(obj_namespace(p), Some("b"), "ns-b pod stays in b");
    }

    // Cross-check: ns `a` holds exactly its OWN RS (not b's), and vice versa.
    let a_rses: Vec<_> = http_list(&client, addr, "/apis/apps/v1/namespaces/a/replicasets")
        .await
        .into_iter()
        .filter(|r| obj_name(r).is_some())
        .collect();
    assert!(
        a_rses.iter().all(|r| is_owned_by(r, &uid_a)),
        "ns a must hold ONLY a's RS — b's RS must not leak in"
    );
    let b_rses: Vec<_> = http_list(&client, addr, "/apis/apps/v1/namespaces/b/replicasets")
        .await
        .into_iter()
        .filter(|r| obj_name(r).is_some())
        .collect();
    assert!(
        b_rses.iter().all(|r| is_owned_by(r, &uid_b)),
        "ns b must hold ONLY b's RS"
    );

    // And `default` is empty (neither chain leaked into the scope ns).
    tokio::time::sleep(Duration::from_secs(1)).await;
    let default_pods = http_list(&client, addr, "/api/v1/namespaces/default/pods").await;
    assert!(
        default_pods.is_empty(),
        "neither chain may leak into default; found {:?}",
        default_pods.iter().filter_map(obj_name).collect::<Vec<_>>()
    );

    rt.shutdown().await.expect("graceful shutdown");
}
