//! M3.0 — the CNI contract against a REAL plugin binary, exec'd for real.
//!
//! CNI's interface IS an executable invocation: argv, environment, stdin,
//! stdout, exit code. A test that calls a Rust function pretending to be a
//! plugin exercises none of that. Every test here forks a real process.
//!
//! * **E1** `ADD` execs the plugin and the pod IP comes back
//! * **E2** the environment and stdin arrive intact
//! * **E3** a chain threads `prevResult`
//! * **E4** `DEL` reverses the chain and tolerates empty output
//! * **E5** a plugin's error DOCUMENT survives, not just its exit code
//! * **E6** a missing plugin names every directory searched
//! * **E7** the planning-only backend refuses and says `Planned`

use std::collections::BTreeMap;
use std::path::PathBuf;

use engenho_cni::config::parse_config;
use engenho_cni::exec::{
    CniCommand, CniEnv, CniInstall, ExecCniEnv, ExecError, PlanningOnlyCniEnv, Sandbox, plan,
    run_chain,
};

/// The reference plugin's directory, which is also the `CNI_PATH` the
/// planner is given — so resolution goes through the real search path.
fn plugin_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_cni-reference-plugin"))
        .parent()
        .expect("the bin lives in a directory")
        .to_path_buf()
}

/// A conflist naming the reference plugin by its real binary name.
fn conf(chain: &[&str]) -> engenho_cni::config::NetworkConfigList {
    let plugins: Vec<_> = chain
        .iter()
        .map(|t| serde_json::json!({ "type": t, "bridge": "cni0" }))
        .collect();
    let body = serde_json::json!({
        "cniVersion": "1.0.0",
        "name": "cbr0",
        "plugins": plugins,
    });
    parse_config(
        std::path::Path::new("/x/10-cbr0.conflist"),
        body.to_string().as_bytes(),
    )
    .unwrap()
}

fn sandbox(fail: bool) -> Sandbox {
    let mut args = BTreeMap::from([
        ("K8S_POD_NAMESPACE".to_string(), "ns".to_string()),
        ("K8S_POD_NAME".to_string(), "pod1".to_string()),
    ]);
    if fail {
        args.insert("FAIL".into(), "1".into());
    }
    Sandbox {
        container_id: "abc123".into(),
        netns: "/var/run/netns/cni-1".into(),
        ifname: "eth0".into(),
        args,
    }
}

/// E1 — a real exec, and the address that comes back.
#[tokio::test]
async fn add_execs_the_plugin_and_returns_a_pod_ip() {
    let env = ExecCniEnv::new(vec![plugin_dir()]);
    assert_eq!(env.install(), CniInstall::Invoked);

    let planned = plan(
        &conf(&["cni-reference-plugin"]),
        &sandbox(false),
        CniCommand::Add,
        &[plugin_dir()],
    );
    let result = run_chain(&env, &planned)
        .await
        .expect("the plugin ran")
        .expect("and returned a result");

    // E1 — and the HOST end of the veth pair was not taken. The plugin
    // deliberately reports it first, which is what makes a naive `ips[0]`
    // assign an address that routes nowhere while still landing in
    // Endpoints.
    assert_eq!(result.pod_ip(), Some("10.244.1.7"));

    // And the routes came back too, so the result parse is not partial.
    assert_eq!(result.routes.len(), 1);
    assert_eq!(result.routes[0].gw, "10.244.1.1");
}

/// E2, read off the wire.
#[tokio::test]
async fn the_environment_and_stdin_reach_the_plugin_intact() {
    let env = ExecCniEnv::new(vec![plugin_dir()]);
    let planned = plan(
        &conf(&["cni-reference-plugin"]),
        &sandbox(false),
        CniCommand::Add,
        &[plugin_dir()],
    );
    let stdout = env.invoke(&planned.invocations[0]).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
    let got = &v["received"];

    assert_eq!(got["containerId"], "abc123");
    assert_eq!(got["name"], "cbr0", "the plugin can key its own state");
    assert_eq!(got["type"], "cni-reference-plugin");
    assert_eq!(got["path"], plugin_dir().display().to_string());
    // Calico keys per-pod policy on these; omitting them yields a pod with
    // no policy and no error anywhere.
    let args = got["args"].as_str().unwrap();
    assert!(args.contains("K8S_POD_NAMESPACE=ns"), "{args}");
    assert!(args.contains("K8S_POD_NAME=pod1"), "{args}");
    // The first link must not receive a prevResult.
    assert_eq!(got["hadPrevResult"], false);
}

/// E3 — a chain threads the previous result. This is what makes a chain a
/// chain: `portmap` needs the interface `bridge` created.
#[tokio::test]
async fn a_chain_threads_prev_result_to_the_second_plugin() {
    let env = ExecCniEnv::new(vec![plugin_dir()]);
    let planned = plan(
        &conf(&["cni-reference-plugin", "cni-reference-plugin"]),
        &sandbox(false),
        CniCommand::Add,
        &[plugin_dir()],
    );
    assert_eq!(planned.invocations.len(), 2);

    // First link: no prevResult. Second: the first's result.
    let first = env.invoke(&planned.invocations[0]).await.unwrap();
    let first: engenho_cni::result::CniResult = serde_json::from_slice(&first).unwrap();

    let mut second = planned.invocations[1].clone();
    engenho_cni::exec::with_prev_result(&mut second, &first);
    let out = env.invoke(&second).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(
        v["received"]["hadPrevResult"], true,
        "the second plugin saw the first's result"
    );
}

/// E4 — `DEL` reverses, and empty output is not an error.
#[tokio::test]
async fn del_reverses_the_chain_and_tolerates_empty_output() {
    let env = ExecCniEnv::new(vec![plugin_dir()]);
    let planned = plan(
        &conf(&["a-plugin", "cni-reference-plugin"]),
        &sandbox(false),
        CniCommand::Del,
        &[plugin_dir()],
    );
    // Reversed: the second-declared plugin runs first on DEL. Tearing down
    // in ADD order leaves portmap's NAT rules pointing at an address about
    // to be reassigned to a different pod.
    assert_eq!(planned.invocations[0].plugin_type, "cni-reference-plugin");
    assert_eq!(planned.invocations[1].plugin_type, "a-plugin");

    // The one that exists returns nothing on DEL, and that is success.
    let out = env.invoke(&planned.invocations[0]).await.unwrap();
    assert!(out.iter().all(u8::is_ascii_whitespace), "empty: {out:?}");
}

/// E5 — the plugin's error document survives.
///
/// "CNI ADD failed" versus "no IP addresses available in range
/// 10.244.1.0/24" is the entire diagnostic value, and it is only in the
/// document — reading the exit code alone throws it away.
#[tokio::test]
async fn a_plugin_failure_keeps_the_reason_the_plugin_gave() {
    let env = ExecCniEnv::new(vec![plugin_dir()]);
    let planned = plan(
        &conf(&["cni-reference-plugin"]),
        &sandbox(true),
        CniCommand::Add,
        &[plugin_dir()],
    );
    let e = run_chain(&env, &planned).await.unwrap_err();
    let ExecError::Plugin { plugin, error } = &e else {
        panic!("expected a parsed plugin error, got {e:?}");
    };
    assert_eq!(plugin, "cni-reference-plugin");
    assert_eq!(error.code, 11);
    assert!(error.msg.contains("no IP addresses available"), "{error}");
    assert!(error.details.contains("10.244.1.0/24"), "{error}");
}

/// E6 — a missing plugin names where we looked.
#[tokio::test]
async fn a_missing_plugin_names_every_directory_searched() {
    let env = ExecCniEnv::new(vec![
        PathBuf::from("/opt/cni/bin"),
        PathBuf::from("/usr/libexec/cni"),
    ]);
    let planned = plan(
        &conf(&["definitely-not-installed"]),
        &sandbox(false),
        CniCommand::Add,
        &[PathBuf::from("/opt/cni/bin")],
    );
    let e = env.invoke(&planned.invocations[0]).await.unwrap_err();
    let msg = e.to_string();
    assert!(msg.contains("definitely-not-installed"), "{msg}");
    assert!(msg.contains("/opt/cni/bin"), "{msg}");
    assert!(msg.contains("/usr/libexec/cni"), "{msg}");
}

/// E7 — the darwin arm plans everything and executes nothing, and says so.
#[tokio::test]
async fn the_planning_only_backend_plans_and_refuses() {
    let env = PlanningOnlyCniEnv;
    assert_eq!(
        env.install(),
        CniInstall::Planned,
        "an operator must be able to tell a planned chain from an invoked one"
    );

    // Planning still fully succeeds — the whole contract is exercised on a
    // machine where it cannot run.
    let planned = plan(
        &conf(&["bridge", "portmap"]),
        &sandbox(false),
        CniCommand::Add,
        &[PathBuf::from("/opt/cni/bin")],
    );
    assert_eq!(planned.invocations.len(), 2);
    assert_eq!(planned.invocations[0].env["CNI_COMMAND"], "ADD");

    let e = run_chain(&env, &planned).await.unwrap_err();
    let msg = e.to_string();
    assert!(msg.contains("PLANNED, not invoked"), "{msg}");
    assert!(
        msg.contains("came from the container runtime"),
        "it must say where the address DID come from: {msg}"
    );
}
