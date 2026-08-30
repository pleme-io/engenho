//! M0.5 — CRD serving end-to-end through the live Runtime.
//!
//! Proves the full CRD lifecycle over ONE store in ONE process, driven by
//! the SPAWNED `CrdController` (no manual `.tick()` — the WatchDriver loop +
//! fallback tick run it). The chain under test:
//!
//!   1. The `apiextensions.k8s.io/v1.CustomResourceDefinition` kind is
//!      SERVED out of the box (cataloged opaque-JSON `StoreBackedHandler`):
//!      `GET /apis/apiextensions.k8s.io/v1` advertises
//!      `customresourcedefinitions`, and POSTing a CRD object 201s.
//!   2. The CrdController observes the CRD write + registers a
//!      `StoreBackedHandler` for the served `example.com/v1` Widget version
//!      into the live `RouterState` (via the DynamicHandlerSink).
//!   3. The CR group + resource AUTO-APPEAR in discovery
//!      (`/apis/example.com/v1` lists `widgets` with shortNames `[wd]`).
//!   4. Generic CR-instance CRUD flows through the SAME catch-all + do_*
//!      bodies (POST a Widget → 201 with resourceVersion + uid; GET reads
//!      it back; DELETE removes it).
//!   5. Deleting the CRD unregisters the handler — subsequent CR access is a
//!      typed 404 NotFound ("the server doesn't have a resource type
//!      widgets").
//!
//! Constraints: no real container runtime (FakeBackend), no network beyond
//! loopback (127.0.0.1:0), ephemeral store, fast fallback so the controller
//! registers within ~1s.

use std::time::{Duration, Instant};

use engenho_config::{EngenhoConfig, KubeletBackendKind};
use engenho_runtime::Runtime;
use shikumi::TieredConfig;

/// Ephemeral, plaintext, fast-fallback config so the CrdController converges
/// quickly even if a single watch-wake is missed.
fn crd_test_config(data_dir: &std::path::Path) -> EngenhoConfig {
    let mut cfg = EngenhoConfig::prescribed_default();
    cfg.runtime.listen_addr = "127.0.0.1:0".into();
    cfg.runtime.durable = false;
    cfg.runtime.node_name = "node-A".into();
    cfg.runtime.kubelet_backend = KubeletBackendKind::Fake;
    cfg.runtime.leadership_timeout_seconds = 5;
    cfg.runtime.tls.enabled = false;
    // A WRITABLE data_dir — the bootstrap admin BEARER token is minted under
    // data_dir/pki on every boot (Brick B), so the prescribed /var/lib/engenho
    // (not writable in CI) won't do.
    cfg.runtime.data_dir = data_dir.to_path_buf();
    // CRD controller on (default), plus a short fallback so registration
    // happens within ~1s of the CRD write even if the watch-wake is missed.
    cfg.controllers.enable.crd = true;
    cfg.controllers.fallback_interval_seconds = 1;
    cfg.controllers.debounce_milliseconds = 20;
    cfg
}

/// Build a reqwest client that authenticates as the bootstrap ADMIN
/// (`system:masters`) — Brick B default-denies a non-admin write, so this CRD
/// CRUD suite authenticates as admin via the minted bearer token under
/// `data_dir/pki/admin.token`.
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

/// Bounded poll: call `predicate` until it returns `Some(T)` or `timeout`.
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

fn crd_body() -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "widgets.example.com" },
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {
                "plural": "widgets",
                "singular": "widget",
                "kind": "Widget",
                "listKind": "WidgetList",
                "shortNames": ["wd"]
            },
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "spec": { "type": "object", "x-kubernetes-preserve-unknown-fields": true }
                        }
                    }
                }
            }]
        }
    })
}

fn widget_body() -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "example.com/v1",
        "kind": "Widget",
        "metadata": { "name": "w1" },
        "spec": { "color": "blue", "size": 42 }
    })
}

#[tokio::test]
async fn crd_lifecycle_register_crud_discovery_unregister() {
    let tmp = tempfile::tempdir().unwrap();
    let rt = Runtime::start(crd_test_config(tmp.path()))
        .await
        .expect("runtime boots");
    let addr = rt.local_addr();
    let client = admin_client(tmp.path());
    let base = format!("http://{addr}");

    // ── (1) the CRD kind itself is served out of the box ──────────────
    //
    // Discovery advertises apiextensions.k8s.io/v1 customresourcedefinitions
    // (the cataloged opaque-JSON handler). This is `kubectl get crd`'s plural
    // resolution path.
    let disc: serde_json::Value = client
        .get(format!("{base}/apis/apiextensions.k8s.io/v1"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let crd_res = disc
        .get("resources")
        .and_then(|r| r.as_array())
        .expect("apiextensions group resource list");
    assert!(
        crd_res
            .iter()
            .any(|r| r.get("name").and_then(|n| n.as_str()) == Some("customresourcedefinitions")),
        "apiextensions.k8s.io/v1 advertises customresourcedefinitions: {disc}"
    );

    // ── (2) POST the CRD object → 201 (opaque-JSON store) ─────────────
    let resp = client
        .post(format!(
            "{base}/apis/apiextensions.k8s.io/v1/customresourcedefinitions"
        ))
        .json(&crd_body())
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::CREATED,
        "CRD POST must 201"
    );

    // GET the CRD back (kubectl get crd widgets.example.com).
    let resp = client
        .get(format!(
            "{base}/apis/apiextensions.k8s.io/v1/customresourcedefinitions/widgets.example.com"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "GET crd by name");

    // ── (3) the CrdController registers the Widget handler; the CR group
    //         + resource auto-appear in discovery ─────────────────────
    let widget_row = poll_until(Duration::from_secs(10), Duration::from_millis(100), || {
        let client = client.clone();
        let base = base.clone();
        async move {
            let resp = client
                .get(format!("{base}/apis/example.com/v1"))
                .send()
                .await
                .ok()?;
            if resp.status() != reqwest::StatusCode::OK {
                return None; // group not registered yet
            }
            let list: serde_json::Value = resp.json().await.ok()?;
            let rows = list.get("resources")?.as_array()?;
            rows.iter()
                .find(|r| r.get("name").and_then(|n| n.as_str()) == Some("widgets"))
                .cloned()
        }
    })
    .await
    .expect("example.com/v1 widgets advertised after CrdController registers");

    // shortNames=[wd], singular=widget, namespaced.
    assert_eq!(
        widget_row.get("singularName").and_then(|s| s.as_str()),
        Some("widget")
    );
    assert_eq!(
        widget_row.get("namespaced").and_then(|n| n.as_bool()),
        Some(true)
    );
    let short = widget_row
        .get("shortNames")
        .and_then(|s| s.as_array())
        .expect("widgets row has shortNames");
    assert!(
        short.iter().any(|s| s.as_str() == Some("wd")),
        "widgets advertises shortName wd: {widget_row}"
    );

    // The group also shows in /apis.
    let apis: serde_json::Value = client
        .get(format!("{base}/apis"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        apis.get("groups")
            .and_then(|g| g.as_array())
            .map(|gs| gs
                .iter()
                .any(|g| g.get("name").and_then(|n| n.as_str()) == Some("example.com")))
            .unwrap_or(false),
        "/apis advertises example.com after registration: {apis}"
    );

    // ── (4) generic CR-instance CRUD via the catch-all ────────────────
    //
    // POST a Widget → 201 with resourceVersion + uid (TypeMeta inherited).
    let resp = client
        .post(format!(
            "{base}/apis/example.com/v1/namespaces/default/widgets"
        ))
        .json(&widget_body())
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::CREATED,
        "Widget CR POST must 201 (routed through the registered handler)"
    );
    let created: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(created.get("kind").and_then(|k| k.as_str()), Some("Widget"));
    assert_eq!(
        created.get("apiVersion").and_then(|a| a.as_str()),
        Some("example.com/v1")
    );
    let rv = created
        .get("metadata")
        .and_then(|m| m.get("resourceVersion"))
        .and_then(|r| r.as_str())
        .expect("created Widget carries resourceVersion");
    assert!(!rv.is_empty());

    // GET it back.
    let got: serde_json::Value = client
        .get(format!(
            "{base}/apis/example.com/v1/namespaces/default/widgets/w1"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        got.get("spec")
            .and_then(|s| s.get("color"))
            .and_then(|c| c.as_str()),
        Some("blue")
    );

    // LIST widgets in the namespace.
    let list: serde_json::Value = client
        .get(format!(
            "{base}/apis/example.com/v1/namespaces/default/widgets"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        list.get("kind").and_then(|k| k.as_str()),
        Some("WidgetList")
    );
    assert_eq!(
        list.get("items")
            .and_then(|i| i.as_array())
            .map(|a| a.len()),
        Some(1)
    );

    // DELETE the Widget instance.
    let resp = client
        .delete(format!(
            "{base}/apis/example.com/v1/namespaces/default/widgets/w1"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "Widget DELETE 200");

    // ── (5) delete the CRD → handler unregistered → CR access NotFound ─
    let resp = client
        .delete(format!(
            "{base}/apis/apiextensions.k8s.io/v1/customresourcedefinitions/widgets.example.com"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "CRD DELETE 200");

    // After the controller's GC tick, the widgets plural no longer resolves.
    let gone = poll_until(Duration::from_secs(10), Duration::from_millis(100), || {
        let client = client.clone();
        let base = base.clone();
        async move {
            let resp = client
                .get(format!(
                    "{base}/apis/example.com/v1/namespaces/default/widgets"
                ))
                .send()
                .await
                .ok()?;
            // Once unregistered, the catch-all lookup misses → 404 NotFound.
            if resp.status() == reqwest::StatusCode::NOT_FOUND {
                Some(())
            } else {
                None
            }
        }
    })
    .await;
    assert!(
        gone.is_some(),
        "after CRD delete + GC tick, widgets access is a typed NotFound"
    );

    rt.shutdown().await.unwrap();
}

/// **A CR that violates its CRD's declared schema is rejected 422.**
///
/// Measured 2026-08-28: a CRD declaring `spec.size: {type: integer}` accepted a
/// CR carrying `spec.size: "NOT-AN-INT"` and returned **201**. `crd.rs` was
/// explicit that this was pending — `CrdEntry::schema` is documented "the
/// opaque openAPIV3Schema for this version (validation DEFERRED)" — so the
/// schema was captured all along and simply never reached the handler.
///
/// A CRD's whole promise is that its schema is enforced. An unvalidated CR is
/// worse than an unschema'd one: every controller downstream was written
/// against the declared types and will panic, mis-branch, or silently coerce on
/// data the apiserver swore could not exist.
///
/// NOTE the scope, so a green run is not over-read: `schema_validation` checks
/// TYPES (plus `required`), not full JSON Schema — no `enum`, `pattern`,
/// bounds, or CEL. See that module's header.
#[tokio::test]
async fn a_cr_violating_its_declared_schema_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let rt = Runtime::start(crd_test_config(tmp.path()))
        .await
        .expect("runtime boots");
    let addr = rt.local_addr();
    let client = admin_client(tmp.path());
    let base = format!("http://{addr}");

    // A CRD with a TYPED spec — unlike `crd_body()`, which marks spec
    // preserve-unknown-fields and so declares nothing to enforce.
    let typed_crd = serde_json::json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "gadgets.example.com" },
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {
                "plural": "gadgets", "singular": "gadget",
                "kind": "Gadget", "listKind": "GadgetList"
            },
            "versions": [{
                "name": "v1", "served": true, "storage": true,
                "schema": { "openAPIV3Schema": {
                    "type": "object",
                    "properties": {
                        "spec": {
                            "type": "object",
                            "properties": { "size": { "type": "integer" } }
                        }
                    }
                }}
            }]
        }
    });

    let resp = client
        .post(format!(
            "{base}/apis/apiextensions.k8s.io/v1/customresourcedefinitions"
        ))
        .json(&typed_crd)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED, "CRD POST 201");

    // Wait for the CrdController to register the dynamic handler.
    let ready = poll_until(
        Duration::from_secs(10),
        Duration::from_millis(50),
        || async {
            let r = client
                .get(format!(
                    "{base}/apis/example.com/v1/namespaces/default/gadgets"
                ))
                .send()
                .await
                .ok()?;
            (r.status() == reqwest::StatusCode::OK).then_some(())
        },
    )
    .await;
    assert!(ready.is_some(), "the Gadget handler must register");

    // ── the defect: a string where the schema declares an integer ──────
    let bad = client
        .post(format!(
            "{base}/apis/example.com/v1/namespaces/default/gadgets"
        ))
        .json(&serde_json::json!({
            "apiVersion": "example.com/v1",
            "kind": "Gadget",
            "metadata": { "name": "bad" },
            "spec": { "size": "NOT-AN-INT" }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        bad.status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY,
        "a CR violating its own declared schema must be rejected; the live \
         daemon returned 201 and stored it"
    );
    let body: serde_json::Value = bad.json().await.unwrap();
    assert_eq!(
        body.get("reason").and_then(|r| r.as_str()),
        Some("Invalid"),
        "client-go's IsInvalid keys on this: {body:#}"
    );
    let msg = body.get("message").and_then(|m| m.as_str()).unwrap_or("");
    assert!(
        msg.contains("spec.size"),
        "the error must name the offending FIELD or the author cannot act: {msg}"
    );

    // ── and a well-typed CR still works: the guard is not a wall ───────
    let good = client
        .post(format!(
            "{base}/apis/example.com/v1/namespaces/default/gadgets"
        ))
        .json(&serde_json::json!({
            "apiVersion": "example.com/v1",
            "kind": "Gadget",
            "metadata": { "name": "good" },
            "spec": { "size": 42 }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        good.status(),
        reqwest::StatusCode::CREATED,
        "a schema-conforming CR is still accepted"
    );
}
