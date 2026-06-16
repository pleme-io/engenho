//! M0.4 brick 1 — in-process HTTPS integration test.
//!
//! Boots the apiserver over TLS on `127.0.0.1:0` with a freshly-generated
//! cluster CA, then drives it with a `reqwest::Client` that TRUSTS that CA
//! (NOT `danger_accept_invalid_certs` — the point is to PROVE the SANs +
//! chain are correct, so a real verifying client must succeed). Asserts:
//!
//!   * `GET /version`  → 200, `gitVersion=v1.34.0`.
//!   * `GET /readyz` / `/livez` / `/healthz` → 200 `"ok"`.
//!   * `GET /api`      → 200 `APIVersions` (the existing discovery surface
//!     is reachable end-to-end through the TLS handshake).
//!   * a client trusting a DIFFERENT CA fails the handshake (proves we're
//!     not silently insecure).
//!
//! The external-kubectl proof (`kubectl get nodes` / `kubectl apply`
//! against the daemon's emitted kubeconfig) is run by the operator, not
//! the test suite.

use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{
    ApiServer, ResourceHandler, ServerSanInputs, StoreBackedHandler, issue_server_material,
    load_or_generate_ca,
};
use engenho_store::{InProcessRouter, StoreMesh, default_config};

/// Boot a store + a TLS apiserver; return the bound URL base, the CA PEM
/// (for a trusting client), and the server handle + tempdir (kept alive).
async fn boot_tls_server() -> (String, String, ApiServer, tempfile::TempDir) {
    let router = InProcessRouter::new();
    let cfg = default_config("apiserver-m0-4").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);

    // Exercise the durable-CA-persist path with a tempdir data_dir.
    let data_dir = tempfile::tempdir().unwrap();
    let ca = load_or_generate_ca(data_dir.path()).unwrap();
    let ca_pem = ca.cert_pem().to_string();
    let material = issue_server_material(
        &ca,
        &ServerSanInputs {
            node_name: "engenho-node",
            listen_ip: None,
        },
    )
    .unwrap();

    // A couple of handlers so /api discovery has content.
    let pod: Arc<dyn ResourceHandler> =
        Arc::new(StoreBackedHandler::for_core_kind(store.clone(), "Pod", true));
    let ns: Arc<dyn ResourceHandler> = Arc::new(StoreBackedHandler::for_core_kind(
        store.clone(),
        "Namespace",
        false,
    ));

    let server = ApiServer::start(
        "127.0.0.1:0".parse().unwrap(),
        vec![pod, ns],
        Some(material),
    )
    .await
    .unwrap();
    let addr = server.local_addr();
    // Loopback URL with the bound port (127.0.0.1 is always a SAN).
    let base = format!("https://127.0.0.1:{}", addr.port());

    // Leak the store clone we still hold so shutdown can unwrap if needed;
    // returning the tempdir keeps the persisted CA on disk for the test.
    drop(store);
    (base, ca_pem, server, data_dir)
}

/// A reqwest client that trusts ONLY the supplied CA (no system roots).
fn client_trusting(ca_pem: &str) -> reqwest::Client {
    let cert = reqwest::Certificate::from_pem(ca_pem.as_bytes()).expect("CA parses");
    reqwest::Client::builder()
        .use_rustls_tls()
        .tls_built_in_root_certs(false)
        .add_root_certificate(cert)
        .build()
        .expect("client builds")
}

#[tokio::test]
async fn version_endpoint_over_https() {
    let (base, ca_pem, server, _dir) = boot_tls_server().await;
    let client = client_trusting(&ca_pem);

    let resp = client
        .get(format!("{base}/version"))
        .send()
        .await
        .expect("HTTPS GET /version succeeds against trusted CA");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v.get("major").unwrap(), "1");
    assert_eq!(v.get("minor").unwrap(), "34");
    assert_eq!(v.get("gitVersion").unwrap(), "v1.34.0");

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn health_endpoints_over_https() {
    let (base, ca_pem, server, _dir) = boot_tls_server().await;
    let client = client_trusting(&ca_pem);

    for path in ["/readyz", "/livez", "/healthz"] {
        let resp = client
            .get(format!("{base}{path}"))
            .send()
            .await
            .unwrap_or_else(|e| panic!("GET {path} over HTTPS failed: {e}"));
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "{path} must be 200");
        let body = resp.text().await.unwrap();
        assert_eq!(body, "ok", "{path} body must be \"ok\"");
    }

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn discovery_reachable_over_https() {
    // Proves the EXISTING discovery surface is reachable end-to-end
    // through the TLS handshake (not just the new health endpoints).
    let (base, ca_pem, server, _dir) = boot_tls_server().await;
    let client = client_trusting(&ca_pem);

    let resp = client.get(format!("{base}/api")).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v.get("kind").unwrap(), "APIVersions");
    let versions = v.get("versions").unwrap().as_array().unwrap();
    assert!(versions.iter().any(|x| x == "v1"));

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn untrusted_ca_fails_the_handshake() {
    // The negative: a client trusting a DIFFERENT CA must FAIL the TLS
    // handshake. Proves we present a real cert that verifies — not a
    // silently-insecure server.
    let (base, _ca_pem, server, _dir) = boot_tls_server().await;

    // A genuinely DIFFERENT CA. The cluster CA is now DETERMINISTIC, so a
    // second `load_or_generate_ca` would mint the SAME CA — the untrusted-CA
    // negative needs a real foreign root, so build a one-off random one.
    let other_ca_pem = {
        let mut params = rcgen::CertificateParams::default();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let key = rcgen::KeyPair::generate().unwrap();
        params.self_signed(&key).unwrap().pem()
    };
    let bad_client = client_trusting(&other_ca_pem);

    let result = bad_client.get(format!("{base}/version")).send().await;
    assert!(
        result.is_err(),
        "a client trusting the WRONG CA must fail the TLS handshake"
    );

    server.shutdown().await.unwrap();
}
