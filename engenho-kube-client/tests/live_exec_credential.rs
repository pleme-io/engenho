//! Live integration test: prove an **exec-credential** kubeconfig
//! actually authenticates.
//!
//! This is the regression test for a real defect. `KubeAuth::Exec` was
//! parsed out of the kubeconfig and then never run: `bearer_token()`
//! matched only the `BearerToken` variants and fell through to
//! `_ => Ok(None)`, so every request went out with **no Authorization
//! header at all** and the apiserver answered 401. It presents as an
//! RBAC problem, which is why it survived — the fix is in
//! `connection.rs::exec_credential`.
//!
//! Unit tests cover the plumbing with `echo`/`false` stand-ins. Only a
//! live cluster proves the real thing: that an EKS kubeconfig whose
//! user section is an `aws eks get-token` plugin gets a token that the
//! apiserver accepts.
//!
//! Skipped when the kubeconfig is absent, so CI without the cluster is
//! unaffected.

use std::path::PathBuf;

use engenho_kube_client::config::Kubeconfig;

/// An exec-credential kubeconfig, if the operator has one. camelot-eks
/// is the reference: its user section is an `aws eks get-token` plugin.
fn exec_kubeconfig_path() -> Option<PathBuf> {
    let home = std::env::var_os("HOME")?;
    let path = PathBuf::from(home).join(".kube/camelot/config");
    if path.exists() { Some(path) } else { None }
}

#[tokio::test]
#[ignore = "live: requires an exec-credential kubeconfig + reachable cluster; run with --ignored"]
async fn exec_credential_kubeconfig_authenticates() {
    let Some(path) = exec_kubeconfig_path() else {
        eprintln!("skip: no exec-credential kubeconfig");
        return;
    };

    let kc = Kubeconfig::load(&path).expect("kubeconfig parses");
    let conn = kc
        .resolve_connection()
        .expect("connection resolves from the current context");

    // 1. The plugin must actually run and yield a token. Before the fix
    //    this returned Ok(None) and the assertion below is what fails.
    let token = conn
        .bearer_token()
        .expect("exec plugin runs")
        .expect("exec plugin must produce a bearer token, not None");
    assert!(
        !engenho_substrate::Risca::expose_secret(&token).is_empty(),
        "token must be non-empty"
    );

    // 2. The apiserver must ACCEPT it. A token that parses but is
    //    rejected is the same outage with a different error, so the
    //    proof is a real authenticated round trip — /version is the
    //    cheapest endpoint that still requires auth on EKS.
    let url = format!("{}/version", conn.server());
    let rb = conn
        .auth_header(conn.http().get(&url))
        .expect("auth header applies");
    let resp = rb.send().await.expect("apiserver reachable");
    assert!(
        resp.status().is_success(),
        "apiserver rejected the exec credential: {} — this is the 401 the fix exists to remove",
        resp.status()
    );
}
