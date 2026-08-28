//! M0.1 item 7 — single-node Runtime convergence proof.
//!
//! The FIRST test in the repo exercising ALL subsystems on ONE store
//! in ONE process. Existing tests prove each link in isolation (r7
//! apiserver↔store, r8 scheduler bind, r9.5 deployment→RS→pod
//! tick-by-tick, r10 kubelet start). This test proves they COMPOSE
//! and that the WatchDriver wake-chain converges AUTONOMOUSLY — no
//! manual `.tick()` calls; the spawned drivers run the loops.
//!
//! Constraints honored: no real container runtime (FakeBackend), no
//! network beyond loopback (127.0.0.1:0), durable store in a tempdir
//! (proves the durable path + the restart-resume seam, not just
//! ephemeral).
//!
//! The REAL-container peer is `m0_2_real_container.rs` (M0.2): same
//! autonomous chain but driven by the live `PodmanBackend` instead of the
//! FakeBackend, ignore-gated because it shells to `podman`. This test
//! stays FakeBackend-only so it runs in CI with no host runtime.

use std::sync::Arc;
use std::time::{Duration, Instant};

use engenho_config::{EngenhoConfig, KubeletBackendKind};
use engenho_kubelet::{ContainerRuntime, FakeBackend};
use engenho_runtime::Runtime;
use shikumi::TieredConfig;

/// Durable config in `data_dir`, apiserver on an ephemeral loopback
/// port, kubelet driven by the FakeBackend, fast fallback so a missed
/// watch-wake never strands the test for 30s.
fn durable_config(data_dir: &std::path::Path) -> EngenhoConfig {
    let mut cfg = EngenhoConfig::prescribed_default();
    cfg.runtime.listen_addr = "127.0.0.1:0".into();
    cfg.runtime.data_dir = data_dir.to_path_buf();
    cfg.runtime.durable = true;
    cfg.runtime.node_name = "node-A".into();
    cfg.runtime.kubelet_backend = KubeletBackendKind::Fake;
    cfg.runtime.leadership_timeout_seconds = 5;
    // Plaintext: this suite asserts convergence over http://; the TLS +
    // kubeconfig path has its own integration test (m0_4_tls_kubectl.rs).
    cfg.runtime.tls.enabled = false;
    // Every reconciler on.
    cfg.controllers.enable.deployment = true;
    cfg.controllers.enable.replicaset = true;
    cfg.controllers.enable.endpoints = true;
    cfg.controllers.enable.gc = true;
    // Short fallback + small debounce so the chain converges quickly
    // even if a single watch-wake is missed.
    cfg.controllers.fallback_interval_seconds = 1;
    cfg.controllers.debounce_milliseconds = 20;
    cfg
}

/// Build a reqwest client that authenticates as the bootstrap ADMIN
/// (`system:masters`) on EVERY request — Brick B (RBAC) default-denies a
/// non-admin write, so the convergence suite (which POSTs Deployments + reads
/// objects) authenticates as admin via the bearer token the runtime mints under
/// `data_dir/pki/admin.token`. The admin short-circuit keeps these exactly as
/// permissive as the pre-RBAC authorize-ALL path.
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

/// Bounded poll: call `predicate` every `interval` until it returns
/// `Some(T)` or `timeout` elapses. Returns the value, or `None` on
/// timeout (the caller dumps last-seen state + fails).
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

fn deployment_body() -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": "podinfo" },
        "spec": {
            "replicas": 2,
            "selector": { "matchLabels": { "app": "podinfo" } },
            "template": {
                "metadata": { "labels": { "app": "podinfo" } },
                "spec": { "containers": [{ "name": "main", "image": "podinfo:6" }] }
            }
        }
    })
}

/// HTTP LIST helper — returns the items array from a `<Kind>List`.
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

/// HTTP GET helper — returns the object, or `None` on non-200.
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

/// True if `child`'s controlling owner-ref UID equals `owner_uid`.
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

#[tokio::test]
async fn deployment_posted_over_http_converges_to_running_container() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(FakeBackend::new());
    let backend_dyn: Arc<dyn ContainerRuntime> = backend.clone();

    let rt = Runtime::start_with_backend(durable_config(tmp.path()), backend_dyn)
        .await
        .expect("Runtime boots");
    let addr = rt.local_addr();
    let client = admin_client(tmp.path());

    // ── ACT: POST the Deployment over real HTTP ─────────────────────
    let resp = client
        .post(format!(
            "http://{addr}/apis/apps/v1/namespaces/default/deployments"
        ))
        .json(&deployment_body())
        .send()
        .await
        .expect("POST deployment");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::CREATED,
        "deployment POST should 201"
    );
    let created: serde_json::Value = resp.json().await.unwrap();
    let dep_uid = created
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|u| u.as_str())
        .expect("deployment got a uid")
        .to_string();
    let rv = created
        .get("metadata")
        .and_then(|m| m.get("resourceVersion"))
        .and_then(|r| r.as_str())
        .expect("deployment got a resourceVersion");
    assert!(!rv.is_empty(), "resourceVersion non-empty");

    // ── ASSERT: convergence, bounded poll, all via real HTTP ────────
    const TIMEOUT: Duration = Duration::from_secs(15);
    const INTERVAL: Duration = Duration::from_millis(50);

    // 1. ReplicaSet created by the deployment controller — owned by the
    //    Deployment uid, spec.replicas == 2.
    let rs = poll_until(TIMEOUT, INTERVAL, || async {
        let items = http_list(
            &client,
            addr,
            "/apis/apps/v1/namespaces/default/replicasets",
        )
        .await;
        let owned: Vec<_> = items
            .iter()
            .filter(|r| is_owned_by(r, &dep_uid))
            .cloned()
            .collect();
        if owned.len() == 1 {
            Some(owned[0].clone())
        } else {
            None
        }
    })
    .await;
    let rs = rs.unwrap_or_else(|| {
        panic!(
            "ReplicaSet never created for Deployment {dep_uid}; \
             last-seen replicasets did not yield exactly 1 owned RS"
        )
    });
    assert_eq!(
        rs.get("spec").and_then(|s| s.get("replicas")).unwrap(),
        2,
        "RS should carry the desired replica count"
    );
    let rs_uid = rs
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|u| u.as_str())
        .expect("RS uid")
        .to_string();

    // 2. Pod(s) created by the RS controller — 2 Pods owned by the RS.
    let pods = poll_until(TIMEOUT, INTERVAL, || async {
        let items = http_list(&client, addr, "/api/v1/namespaces/default/pods").await;
        let owned: Vec<_> = items
            .iter()
            .filter(|p| is_owned_by(p, &rs_uid))
            .cloned()
            .collect();
        if owned.len() == 2 { Some(owned) } else { None }
    })
    .await;
    let pods =
        pods.unwrap_or_else(|| panic!("expected 2 Pods owned by RS {rs_uid}; never reached 2"));
    let pod_names: Vec<String> = pods
        .iter()
        .filter_map(|p| {
            p.get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .map(String::from)
        })
        .collect();
    assert_eq!(pod_names.len(), 2, "two named pods");

    // 3. Scheduler binds each Pod to node-A (poll the GET).
    for name in &pod_names {
        let bound = poll_until(TIMEOUT, INTERVAL, || async {
            let pod = http_get(
                &client,
                addr,
                &format!("/api/v1/namespaces/default/pods/{name}"),
            )
            .await?;
            let node = pod
                .get("spec")
                .and_then(|s| s.get("nodeName"))
                .and_then(|n| n.as_str())
                .map(String::from);
            node.filter(|n| !n.is_empty())
        })
        .await;
        assert_eq!(
            bound.as_deref(),
            Some("node-A"),
            "pod {name} should be scheduled onto node-A"
        );
    }

    // 4. Kubelet started containers (read the FakeBackend directly) AND
    //    each Pod's status reports Running + Ready=True (via HTTP GET).
    let started = poll_until(TIMEOUT, INTERVAL, || {
        let backend = backend.clone();
        async move {
            if backend.running_count().await == 2 {
                Some(())
            } else {
                None
            }
        }
    })
    .await;
    assert!(
        started.is_some(),
        "kubelet FakeBackend should report 2 running containers; \
         got {}",
        backend.running_count().await
    );

    let events = backend.events().await;
    let start_count = events
        .iter()
        .filter(|e| matches!(e, engenho_kubelet::backend::FakeEvent::Start(_)))
        .count();
    assert_eq!(start_count, 2, "exactly 2 Start events");
    // Container backend names are `<ns>_<pod>_<containerName>` (multi-container
    // brick); the podinfo template has a single container named `main`.
    for name in &pod_names {
        let expected = format!("default_{name}_main");
        assert!(
            events.iter().any(|e| matches!(
                e,
                engenho_kubelet::backend::FakeEvent::Start(n) if n == &expected
            )),
            "missing Start event for container {expected}; events={events:?}"
        );
    }

    // Each Pod's status phase=Running + a Ready=True condition (HTTP).
    for name in &pod_names {
        let ok = poll_until(TIMEOUT, INTERVAL, || async {
            let pod = http_get(
                &client,
                addr,
                &format!("/api/v1/namespaces/default/pods/{name}"),
            )
            .await?;
            let status = pod.get("status")?;
            let phase = status.get("phase").and_then(|p| p.as_str());
            let ready = status
                .get("conditions")
                .and_then(|c| c.as_array())
                .is_some_and(|conds| {
                    conds.iter().any(|c| {
                        c.get("type").and_then(|t| t.as_str()) == Some("Ready")
                            && c.get("status").and_then(|s| s.as_str()) == Some("True")
                    })
                });
            if phase == Some("Running") && ready {
                Some(())
            } else {
                None
            }
        })
        .await;
        assert!(
            ok.is_some(),
            "pod {name} should report status.phase=Running + Ready=True"
        );
    }

    // ── CLEANUP: graceful shutdown must not hang ────────────────────
    rt.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn deployment_status_converges_then_reconcile_is_bounded() {
    // M0.1 item 8 (Part E) — status converges through the autonomous
    // WatchDriver chain reading REAL children, AND the post-convergence
    // reconcile is BOUNDED (no unbounded hot-loop): once at the fixpoint,
    // an idle window with no external mutation advances the store
    // revision by at most a small bounded constant (0 in steady state).
    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(FakeBackend::new());
    let backend_dyn: Arc<dyn ContainerRuntime> = backend.clone();

    let rt = Runtime::start_with_backend(durable_config(tmp.path()), backend_dyn)
        .await
        .expect("Runtime boots");
    let addr = rt.local_addr();
    let client = admin_client(tmp.path());

    // POST the Deployment.
    let resp = client
        .post(format!(
            "http://{addr}/apis/apps/v1/namespaces/default/deployments"
        ))
        .json(&deployment_body())
        .send()
        .await
        .expect("POST deployment");
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    const TIMEOUT: Duration = Duration::from_secs(20);
    const INTERVAL: Duration = Duration::from_millis(50);

    // The Deployment status converges: observedGeneration matches the
    // spec-intent generation AND readyReplicas == 2 (proves status was
    // computed from REAL ready children flowing RS→Pod→kubelet, not a
    // counter).
    let converged = poll_until(TIMEOUT, INTERVAL, || async {
        let dep = http_get(
            &client,
            addr,
            "/apis/apps/v1/namespaces/default/deployments/podinfo",
        )
        .await?;
        let status = dep.get("status")?;
        let generation = dep.get("metadata")?.get("generation")?.as_i64()?;
        let observed = status.get("observedGeneration")?.as_i64()?;
        let ready = status
            .get("readyReplicas")
            .and_then(|r| r.as_i64())
            .unwrap_or(0);
        if observed == generation && ready == 2 {
            Some(())
        } else {
            None
        }
    })
    .await;
    assert!(
        converged.is_some(),
        "Deployment .status should converge to observedGeneration==generation + readyReplicas==2"
    );

    // The RS status also converged (the source of truth the Deployment
    // aggregates).
    let rs_ready = poll_until(TIMEOUT, INTERVAL, || async {
        let items = http_list(
            &client,
            addr,
            "/apis/apps/v1/namespaces/default/replicasets",
        )
        .await;
        let rs = items.into_iter().next()?;
        let ready = rs.get("status")?.get("readyReplicas")?.as_i64()?;
        if ready == 2 { Some(()) } else { None }
    })
    .await;
    assert!(
        rs_ready.is_some(),
        "RS status.readyReplicas should converge to 2"
    );

    // ── BOUNDED RECONCILE: capture the revision at the fixpoint, idle
    // past several fallback intervals with NO external mutation, re-read.
    // The revision must advance by at most a small bounded constant
    // (steady-state idempotent skip proposes nothing → 0), NEVER climb
    // monotonically.
    //
    // We sample twice across an idle window: rev grows by a bounded
    // amount (the last in-flight status converging) then STOPS.
    let store = rt.store();
    // Let any final in-flight status writes settle.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let rev_a = store.current_catalog().await.revision();
    // Idle window > 2 fallback intervals (fallback = 1s) with no mutation.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let rev_b = store.current_catalog().await.revision();
    assert_eq!(
        rev_a, rev_b,
        "post-convergence idle reconcile must NOT advance the revision \
         (idempotent-skip hot-loop defense); rev_a={rev_a}, rev_b={rev_b}"
    );

    // Drop our store clone BEFORE shutdown — `terminate` needs the sole
    // strong ref (every driver/handler clone is dropped by shutdown).
    drop(store);
    rt.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn durable_store_resumes_deployment_across_runtime_restart() {
    // Both starts share ONE data_dir in ONE sequential test (not
    // parallel fns) — proves start_or_resume resumption at the Runtime
    // seam (the durable StoreMesh restart is unit-tested in
    // engenho-store r6.5, but never at the Runtime layer).
    let tmp = tempfile::tempdir().unwrap();

    // ── First boot: POST + converge to a ReplicaSet, then shutdown ──
    {
        let backend = Arc::new(FakeBackend::new());
        let backend_dyn: Arc<dyn ContainerRuntime> = backend.clone();
        let rt = Runtime::start_with_backend(durable_config(tmp.path()), backend_dyn)
            .await
            .expect("first boot");
        let addr = rt.local_addr();
        // The admin bearer token is minted on this first boot under the shared
        // tempdir; build the admin client now (Brick B default-denies anonymous
        // writes).
        let client = admin_client(tmp.path());

        let resp = client
            .post(format!(
                "http://{addr}/apis/apps/v1/namespaces/default/deployments"
            ))
            .json(&deployment_body())
            .send()
            .await
            .expect("POST deployment");
        assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
        let dep_uid = resp
            .json::<serde_json::Value>()
            .await
            .unwrap()
            .get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(|u| u.as_str())
            .unwrap()
            .to_string();

        // Wait until at least the RS is created (proves the chain ran
        // before we tear down, so the store holds real converged state).
        let converged = poll_until(
            Duration::from_secs(15),
            Duration::from_millis(50),
            || async {
                let items = http_list(
                    &client,
                    addr,
                    "/apis/apps/v1/namespaces/default/replicasets",
                )
                .await;
                if items.iter().any(|r| is_owned_by(r, &dep_uid)) {
                    Some(())
                } else {
                    None
                }
            },
        )
        .await;
        assert!(converged.is_some(), "first boot should converge to an RS");

        rt.shutdown().await.expect("first shutdown");
    }

    // ── Second boot on the SAME data_dir: Deployment still present ───
    {
        let backend = Arc::new(FakeBackend::new());
        let backend_dyn: Arc<dyn ContainerRuntime> = backend.clone();
        let rt = Runtime::start_with_backend(durable_config(tmp.path()), backend_dyn)
            .await
            .expect("second boot (resume)");
        let addr = rt.local_addr();
        // The admin token is restart-stable (load-or-generate reads the
        // persisted one), so the resumed boot's admin client uses the same
        // bearer.
        let client = admin_client(tmp.path());

        // The Deployment persisted across the restart — readable over
        // HTTP from the resumed durable store.
        let dep = poll_until(
            Duration::from_secs(10),
            Duration::from_millis(50),
            || async {
                http_get(
                    &client,
                    addr,
                    "/apis/apps/v1/namespaces/default/deployments/podinfo",
                )
                .await
            },
        )
        .await;
        let dep = dep.expect("Deployment should survive Runtime restart");
        assert_eq!(dep.get("kind").unwrap(), "Deployment");
        assert_eq!(dep.get("spec").and_then(|s| s.get("replicas")).unwrap(), 2);

        rt.shutdown().await.expect("second shutdown");
    }
}

/// **A freshly booted engenho has the four system namespaces.**
///
/// Regression test for the defect that opened this whole investigation.
/// Measured 2026-08-28 on the live daemon: `GET /api/v1/namespaces` returned
/// `{"items":[]}`. Not one namespace — not even `default`. So k9s showed an
/// empty screen, correctly, because the cluster genuinely contained nothing,
/// and there was nowhere to schedule a workload.
///
/// Upstream's kube-apiserver seeds these during bootstrap. Asserted over real
/// HTTP against a booted `Runtime`, because the thing that was broken is what
/// a CLIENT sees — an in-process store assertion would not have caught the
/// class (the store was fine; nothing ever wrote to it).
#[tokio::test]
async fn boot_seeds_the_four_system_namespaces() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(FakeBackend::new());
    let backend_dyn: Arc<dyn ContainerRuntime> = backend.clone();

    let rt = Runtime::start_with_backend(durable_config(tmp.path()), backend_dyn)
        .await
        .expect("Runtime boots");
    let addr = rt.local_addr();
    let client = admin_client(tmp.path());

    let resp = client
        .get(format!("http://{addr}/api/v1/namespaces"))
        .send()
        .await
        .expect("GET namespaces");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let list: serde_json::Value = resp.json().await.unwrap();
    let items = list
        .get("items")
        .and_then(|i| i.as_array())
        .expect("NamespaceList carries items");

    let names: std::collections::BTreeSet<&str> = items
        .iter()
        .filter_map(|n| n.get("metadata")?.get("name")?.as_str())
        .collect();

    for want in ["default", "kube-system", "kube-public", "kube-node-lease"] {
        assert!(
            names.contains(want),
            "a booted cluster MUST have the `{want}` namespace; got {names:?}"
        );
    }

    // Shape, not just presence: a seeded namespace must look exactly like a
    // client-created one, because a direct store Put bypasses the apiserver's
    // create path and a divergence there is invisible until something reads it.
    let default_ns = items
        .iter()
        .find(|n| {
            n.get("metadata").and_then(|m| m.get("name")).and_then(|v| v.as_str()) == Some("default")
        })
        .expect("default namespace present");

    assert_eq!(
        default_ns
            .get("status")
            .and_then(|s| s.get("phase"))
            .and_then(|p| p.as_str()),
        Some("Active"),
        "a seeded namespace is Active — clients read phase to tell Active from Terminating: {default_ns:#}"
    );
    assert_eq!(
        default_ns
            .get("metadata")
            .and_then(|m| m.get("labels"))
            .and_then(|l| l.get("kubernetes.io/metadata.name"))
            .and_then(|v| v.as_str()),
        Some("default"),
        "upstream guarantees the kubernetes.io/metadata.name label; selectors rely on it"
    );
    assert!(
        default_ns
            .get("spec")
            .and_then(|s| s.get("finalizers"))
            .and_then(|f| f.as_array())
            .is_some_and(|f| f.iter().any(|v| v.as_str() == Some("kubernetes"))),
        "spec.finalizers carries the kubernetes finalizer: {default_ns:#}"
    );
    assert!(
        default_ns
            .get("metadata")
            .and_then(|m| m.get("creationTimestamp"))
            .and_then(|t| t.as_str())
            .is_some_and(|t| !t.is_empty()),
        "seeded through the boundary stamp, so AGE works in kubectl/k9s"
    );
}

/// **An invalid object name is rejected with 422 Invalid, over real HTTP.**
///
/// Measured 2026-08-28 on the live daemon: `UPPERCASE`, `has spaces`,
/// `-leading-dash`, `has_underscore` and a 64-character name were **all
/// accepted with 201**. There was no name validation anywhere in the
/// workspace, so `kubectl get ns` listed a namespace literally called
/// `has spaces`.
///
/// Asserted over HTTP rather than against the validator, because a validator
/// nobody calls is not a fix — the class here is "the boundary does not run
/// the check", and only a request can prove the boundary runs it.
#[tokio::test]
async fn invalid_object_names_are_rejected_with_422() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(FakeBackend::new());
    let backend_dyn: Arc<dyn ContainerRuntime> = backend.clone();

    let rt = Runtime::start_with_backend(durable_config(tmp.path()), backend_dyn)
        .await
        .expect("Runtime boots");
    let addr = rt.local_addr();
    let client = admin_client(tmp.path());

    // Every name the live daemon wrongly accepted, plus the 64-char one.
    let sixty_four = "b".repeat(64);
    for bad in [
        "UPPERCASE",
        "has spaces",
        "-leading-dash",
        "trailing-dash-",
        "has_underscore",
        sixty_four.as_str(),
    ] {
        let resp = client
            .post(format!("http://{addr}/api/v1/namespaces"))
            .json(&serde_json::json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": { "name": bad },
            }))
            .send()
            .await
            .expect("POST namespace");

        assert_eq!(
            resp.status(),
            reqwest::StatusCode::UNPROCESSABLE_ENTITY,
            "name {bad:?} must be rejected 422; the live daemon returned 201 for it"
        );

        let body: serde_json::Value = resp.json().await.unwrap();
        // The envelope matters as much as the code: client-go's
        // `apierrors::IsInvalid` keys on reason "Invalid", and a controller
        // that cannot tell "permanently invalid" from "transient" retries a
        // doomed object forever.
        assert_eq!(
            body.get("kind").and_then(|k| k.as_str()),
            Some("Status"),
            "a rejection is a metav1.Status: {body:#}"
        );
        assert_eq!(
            body.get("reason").and_then(|r| r.as_str()),
            Some("Invalid"),
            "reason must be Invalid (not BadRequest) for {bad:?}: {body:#}"
        );
        assert_eq!(body.get("code").and_then(serde_json::Value::as_u64), Some(422));
    }

    // A VALID name still works — the guard must not have become a wall.
    let ok = client
        .post(format!("http://{addr}/api/v1/namespaces"))
        .json(&serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": { "name": "a-legal-name" },
        }))
        .send()
        .await
        .expect("POST valid namespace");
    assert_eq!(
        ok.status(),
        reqwest::StatusCode::CREATED,
        "a legal DNS-1123 label must still be accepted"
    );

    // A Pod MAY carry dots (subdomain rule) where a Namespace may not —
    // proving the per-kind rule is live, not a single global regex.
    let dotted = client
        .post(format!("http://{addr}/api/v1/namespaces/default/pods"))
        .json(&serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "a.dotted.pod" },
            "spec": { "containers": [{ "name": "c", "image": "img" }] },
        }))
        .send()
        .await
        .expect("POST dotted pod");
    assert_eq!(
        dotted.status(),
        reqwest::StatusCode::CREATED,
        "a Pod takes the DNS-1123 SUBDOMAIN rule, so dots are legal for it"
    );
}

/// **An unresolvable container runtime fails at BOOT, loudly.**
///
/// The failure this replaces was silent for hours. Measured 2026-08-28: the
/// launchd agent had no `podman` on its PATH, so every container start
/// returned `spawn: No such file or directory` — one WARN per reconcile tick,
/// forever — while every pod sat with no status at all. A permanently-broken
/// node was indistinguishable from a slow one, and the operator's only symptom
/// was an empty k9s screen.
///
/// A control plane that cannot run a container must say so ONCE, at boot,
/// fatally. Uses `Runtime::start` (not `start_with_backend`) because the
/// preflight is exactly the path a real daemon takes.
#[tokio::test]
async fn unresolvable_container_runtime_fails_at_boot() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = durable_config(tmp.path());
    // The real shape of the defect: the Podman backend selected, and a binary
    // that does not resolve.
    cfg.runtime.kubelet_backend = KubeletBackendKind::Podman;
    cfg.runtime.podman_binary = Some("/nonexistent/definitely-not-podman".to_string());

    let err = engenho_runtime::Runtime::start(cfg)
        .await
        .err()
        .expect("boot MUST fail when the configured runtime cannot be resolved");

    let rendered = err.to_string();
    assert!(
        rendered.contains("could not be \nresolved") || rendered.contains("could not be"),
        "the error must name the failure: {rendered}"
    );
    assert!(
        rendered.contains("/nonexistent/definitely-not-podman"),
        "the error must name the BINARY it could not resolve, or the operator \
         cannot act on it: {rendered}"
    );
    assert!(
        rendered.contains("kubelet_backend") || rendered.contains("fake"),
        "the error must name the way OUT (set a path, or use the fake backend): {rendered}"
    );
}

/// The `fake` backend needs no container runtime, so preflight must not block
/// a control-plane-only node. The guard must not have become a wall.
#[tokio::test]
async fn fake_backend_boots_without_any_container_runtime() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = durable_config(tmp.path());
    cfg.runtime.kubelet_backend = KubeletBackendKind::Fake;
    cfg.runtime.podman_binary = Some("/nonexistent/definitely-not-podman".to_string());

    let rt = engenho_runtime::Runtime::start(cfg)
        .await
        .expect("a fake-backend node boots with no container runtime present");
    assert!(rt.local_addr().port() > 0, "apiserver bound");
}
