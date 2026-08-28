//! Brick A — server-side authentication integration tests.
//!
//! Boots the apiserver over OPTIONAL-mTLS on `127.0.0.1:0` with the typed
//! authenticator chain (admin token configured) + the admin client cert issued
//! from the same cluster CA, then drives it with `reqwest` clients that present
//! (a) the admin client cert, (b) no credentials, (c) the admin bearer token.
//!
//! Proves:
//!   * `POST /apis/authentication.k8s.io/v1/selfsubjectreviews` with the admin
//!     client cert → `status.userInfo.username == "engenho-admin"`, groups
//!     contains `system:masters`.
//!   * same with NO credentials → `system:anonymous` (still allowed — authn is
//!     identity, not enforcement).
//!   * same with the admin BEARER token → `engenho-admin`.
//!   * a no-credential request to `/api/v1/namespaces` returns 200 (existing
//!     flows STILL WORK through the optional-mTLS path).
//!   * an admission webhook OBSERVES the threaded `user_info` on a create
//!     (NOT the empty default) — userInfo reaches AdmissionRequest.

use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{
    ApiServer, ChainAuthenticator, ResourceHandler, RouterState, ServerSanInputs, TlsMaterial,
    client_verifier, handlers_from_catalog, handlers_from_catalog_with_admission,
    issue_admin_client_material, issue_server_material, load_or_generate_ca,
};
use engenho_controllers::{AdmissionChain, AdmissionMode, FakeAdmissionWebhook};
use engenho_store::{InProcessRouter, ResourceKey, StoreMesh, default_config};

const ADMIN_TOKEN: &str = "test-admin-bootstrap-token";

/// Boot a store + an OPTIONAL-mTLS apiserver carrying the configured admin
/// token. Returns the base URL, the CA PEM, the admin client cert + key PEM,
/// the server handle, and the tempdir (kept alive).
async fn boot_authn_server(
    handlers: Vec<Arc<dyn ResourceHandler>>,
) -> (String, String, String, String, ApiServer, tempfile::TempDir) {
    let router = InProcessRouter::new();
    let cfg = default_config("apiserver-authn").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);

    let data_dir = tempfile::tempdir().unwrap();
    let ca = load_or_generate_ca(data_dir.path()).unwrap();
    let ca_pem = ca.cert_pem().to_string();
    let admin = issue_admin_client_material(&ca).unwrap();
    let verifier = client_verifier(&ca).unwrap();
    let material: TlsMaterial = issue_server_material(
        &ca,
        &ServerSanInputs {
            node_name: "engenho-node",
            listen_ip: None,
        },
    )
    .unwrap()
    .with_client_verifier(verifier);

    let state = RouterState::new(handlers).with_authenticator(Arc::new(
        ChainAuthenticator::bootstrap(Some(ADMIN_TOKEN.to_string())),
    ));
    let server = ApiServer::start_with_state("127.0.0.1:0".parse().unwrap(), state, Some(material))
        .await
        .unwrap();
    let addr = server.local_addr();
    let base = format!("https://127.0.0.1:{}", addr.port());

    drop(store);
    (
        base,
        ca_pem,
        admin.cert_pem,
        admin.key_pem,
        server,
        data_dir,
    )
}

/// A reqwest client trusting ONLY the cluster CA, presenting the admin client
/// identity (cert + key) for mTLS.
fn client_with_admin_cert(ca_pem: &str, cert_pem: &str, key_pem: &str) -> reqwest::Client {
    let ca = reqwest::Certificate::from_pem(ca_pem.as_bytes()).unwrap();
    // reqwest's rustls `Identity::from_pem` takes a combined key+cert PEM
    // bundle (it scans for both blocks). rcgen emits a PKCS#8 key
    // (`BEGIN PRIVATE KEY`); concatenate key then cert.
    let mut bundle = String::new();
    bundle.push_str(key_pem);
    if !key_pem.ends_with('\n') {
        bundle.push('\n');
    }
    bundle.push_str(cert_pem);
    let identity = reqwest::Identity::from_pem(bundle.as_bytes()).expect("admin identity parses");
    reqwest::Client::builder()
        .use_rustls_tls()
        .tls_built_in_root_certs(false)
        .add_root_certificate(ca)
        .identity(identity)
        .build()
        .unwrap()
}

/// A reqwest client trusting ONLY the cluster CA, presenting NO client cert.
fn client_ca_only(ca_pem: &str) -> reqwest::Client {
    let ca = reqwest::Certificate::from_pem(ca_pem.as_bytes()).unwrap();
    reqwest::Client::builder()
        .use_rustls_tls()
        .tls_built_in_root_certs(false)
        .add_root_certificate(ca)
        .build()
        .unwrap()
}

fn ssr_url(base: &str) -> String {
    format!("{base}/apis/authentication.k8s.io/v1/selfsubjectreviews")
}

#[tokio::test]
async fn self_subject_review_with_admin_cert_is_engenho_admin() {
    let store = boot_store_handlers().await;
    let (base, ca_pem, cert_pem, key_pem, server, _dir) = boot_authn_server(store).await;
    let client = client_with_admin_cert(&ca_pem, &cert_pem, &key_pem);

    let resp = client
        .post(ssr_url(&base))
        .json(&serde_json::json!({
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "SelfSubjectReview"
        }))
        .send()
        .await
        .expect("SelfSubjectReview over mTLS");
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let v: serde_json::Value = resp.json().await.unwrap();
    let user = &v["status"]["userInfo"];
    assert_eq!(
        user["username"], "engenho-admin",
        "admin cert → engenho-admin: {v}"
    );
    let groups: Vec<String> = serde_json::from_value(user["groups"].clone()).unwrap();
    assert!(
        groups.iter().any(|g| g == "system:masters"),
        "admin cert groups contain system:masters; got {groups:?}"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn self_subject_review_with_no_creds_is_anonymous() {
    let store = boot_store_handlers().await;
    let (base, ca_pem, _cert, _key, server, _dir) = boot_authn_server(store).await;
    let client = client_ca_only(&ca_pem);

    let resp = client
        .post(ssr_url(&base))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("SelfSubjectReview with no creds still connects (allow_unauthenticated)");
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        v["status"]["userInfo"]["username"], "system:anonymous",
        "no creds → system:anonymous: {v}"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn self_subject_review_with_admin_bearer_is_engenho_admin() {
    let store = boot_store_handlers().await;
    let (base, ca_pem, _cert, _key, server, _dir) = boot_authn_server(store).await;
    let client = client_ca_only(&ca_pem);

    let resp = client
        .post(ssr_url(&base))
        .bearer_auth(ADMIN_TOKEN)
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("SelfSubjectReview with admin bearer");
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        v["status"]["userInfo"]["username"], "engenho-admin",
        "admin bearer → engenho-admin: {v}"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn no_cert_request_is_still_allowed() {
    // Existing flow STILL WORKS: a CA-only client (no cert, no token) hitting a
    // real resource path returns 200 — authorize-ALL retained, anonymous is
    // allowed.
    let store = boot_store_handlers().await;
    let (base, ca_pem, _cert, _key, server, _dir) = boot_authn_server(store).await;
    let client = client_ca_only(&ca_pem);

    let resp = client
        .get(format!("{base}/api/v1/namespaces"))
        .send()
        .await
        .expect("anonymous LIST namespaces still connects + is allowed");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "anonymous request is STILL allowed (authorize-ALL retained)"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn admission_observes_authenticated_user_info() {
    // The userInfo reaches AdmissionRequest: an admin-bearer create makes the
    // admission webhook observe engenho-admin / system:masters (NOT the empty
    // default).
    let store = boot_store().await;
    let hook = Arc::new(FakeAdmissionWebhook::new("observer"));
    let chain = Arc::new(AdmissionChain::new(
        vec![hook.clone()],
        AdmissionMode::FailOpen,
    ));
    let handlers = handlers_from_catalog_with_admission(store.clone(), chain);

    let (base, ca_pem, _cert, _key, server, _dir) = boot_authn_server(handlers).await;
    let client = client_ca_only(&ca_pem);

    let resp = client
        .post(format!("{base}/api/v1/namespaces/default/pods"))
        .bearer_auth(ADMIN_TOKEN)
        .json(&serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "authn-pod" },
            "spec": { "containers": [{"name": "app", "image": "podinfo:6"}] }
        }))
        .send()
        .await
        .expect("admin-bearer create");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::CREATED,
        "create succeeds"
    );

    let observed = hook
        .last_user_info()
        .await
        .expect("the admission webhook reviewed the create");
    assert_eq!(
        observed.username, "engenho-admin",
        "admission saw the authenticated identity, not empty: {observed:?}"
    );
    assert!(
        observed.groups.iter().any(|g| g == "system:masters"),
        "admission user_info groups carry system:masters"
    );
    // The committed object exists (sanity).
    assert!(
        store
            .get(&ResourceKey::namespaced(
                "",
                "v1",
                "Pod",
                "default",
                "authn-pod"
            ))
            .await
            .is_some()
    );

    server.shutdown().await.unwrap();
    drop(store);
}

/// Boot a bare store (caller wires handlers separately).
async fn boot_store() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("apiserver-authn-store").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    // Seed `default`, because the assembled (with_admission) handler set now
    // enforces NamespaceLifecycle: a create into a namespace that does not
    // exist is rejected 404, exactly as upstream does. The real Runtime seeds
    // the four system namespaces at boot; a hand-built store must do the same
    // or it is testing a cluster that could not exist.
    store
        .propose(engenho_store::command::ResourceCommand::Put {
            key: engenho_store::ResourceKey::cluster_scoped(
                "",
                "v1",
                "Namespace",
                "default".to_string(),
            ),
            value: serde_json::json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": { "name": "default" },
                "status": { "phase": "Active" }
            }),
            expected: None,
            reason: engenho_store::command::Reason::Operator,
        })
        .await
        .unwrap();
    store
}

/// A small handler set (Namespace cluster-scoped + Pod namespaced) so the
/// SelfSubjectReview + namespaces + pods routes have content. The store is
/// dropped — the SSR/anonymous tests don't read it back.
async fn boot_store_handlers() -> Vec<Arc<dyn ResourceHandler>> {
    let store = boot_store().await;
    let handlers = handlers_from_catalog(store);
    handlers
}
