//! M3.1 — the DIFFERENTIAL: engenho's CNI runtime against plugins we did
//! not write.
//!
//! ★ WHY, WHEN THE CRATE ALREADY HAS A REFERENCE PLUGIN. The in-crate
//! `cni-reference-plugin` is a real binary, exec'd for real, and it still
//! cannot falsify us: it was written from the same reading of the same spec
//! by the same author as the runtime. It proves our encoder agrees with our
//! decoder — the one thing that cannot fail.
//!
//! The CSI differential found nothing wrong with engenho and the etcd one
//! found a divide-by-zero within the hour. That asymmetry is the argument:
//! you cannot know which it will be until you run it.
//!
//! ★ THESE ARE NOT DEPENDENCIES. `containernetworking/plugins` is the CNI
//! project's own reference implementation. Never built by our build, never
//! linked, never shipped — a measuring instrument whose language is
//! incidental.
//!
//! ★ TWO CLASSES OF PLUGIN, AND ONLY ONE RUNS OFF-LINUX. `host-local` and
//! `static` are IPAM: pure bookkeeping, no netns, and they run anywhere.
//! `ptp`, `bridge` and `portmap` manipulate network namespaces and are
//! Linux-only — `cni-plugins` is not even buildable on darwin, which is a
//! fact about the world rather than about our abstractions.
//!
//! Run:
//!
//! ```text
//! ENGENHO_CNI_PLUGIN_DIR=/nix/store/…-cni-plugins-1.8.0/bin \
//!   cargo test -p engenho-cni --test m3_1_foreign_plugin_differential -- --ignored
//! ```

use std::collections::BTreeMap;
use std::path::PathBuf;

use engenho_cni::config::parse_config;
use engenho_cni::exec::{CniCommand, ExecCniEnv, Sandbox, plan, run_chain};

fn plugin_dir() -> PathBuf {
    PathBuf::from(
        std::env::var("ENGENHO_CNI_PLUGIN_DIR")
            .expect("set ENGENHO_CNI_PLUGIN_DIR to a containernetworking/plugins bin dir"),
    )
}

fn sandbox(id: &str) -> Sandbox {
    Sandbox {
        container_id: id.into(),
        // IPAM plugins never touch it, so a placeholder is honest here and
        // the netns-using plugins are a separate, Linux-only test.
        netns: "/var/run/netns/engenho-differential".into(),
        ifname: "eth0".into(),
        args: BTreeMap::from([
            ("K8S_POD_NAMESPACE".to_string(), "ns".to_string()),
            ("K8S_POD_NAME".to_string(), "pod1".to_string()),
        ]),
    }
}

/// P1 — engenho's runtime drives upstream's `host-local` IPAM.
///
/// This is the whole exec contract against foreign software: the `CNI_*`
/// environment, the config on stdin, the versioned result off stdout.
#[tokio::test]
#[ignore = "needs containernetworking/plugins; see the module header"]
async fn engenho_drives_upstreams_host_local_ipam() {
    let dir = plugin_dir();
    let state = tempfile::tempdir().unwrap();
    let body = serde_json::json!({
        "cniVersion": "1.0.0",
        "name": "engenho-differential",
        "plugins": [{
            "type": "host-local",
            "ipam": {
                "type": "host-local",
                "subnet": "10.244.9.0/24",
                "dataDir": state.path().to_string_lossy(),
            },
            // host-local reads its range from the TOP level when invoked
            // directly rather than as a chained ipam block.
            "subnet": "10.244.9.0/24",
            "dataDir": state.path().to_string_lossy(),
        }],
    });
    let config = parse_config(
        std::path::Path::new("/x/differential.conflist"),
        body.to_string().as_bytes(),
    )
    .unwrap();

    let env = ExecCniEnv::new(vec![dir.clone()]);
    let planned = plan(&config, &sandbox("engenho-diff-1"), CniCommand::Add, &[dir]);
    let result = run_chain(&env, &planned)
        .await
        .expect("upstream host-local ran")
        .expect("and returned a result");

    // The address came from software we did not write, and engenho parsed
    // it: CIDR stripped, family derived, no interface index invented.
    let ip = result.pod_ip().expect("an address");
    assert!(ip.starts_with("10.244.9."), "{ip}");
    assert!(!ip.contains('/'), "the prefix is stripped: {ip}");

    // Teardown through the same path.
    let del = plan(
        &config,
        &sandbox("engenho-diff-1"),
        CniCommand::Del,
        &[plugin_dir()],
    );
    run_chain(&env, &del).await.expect("DEL against upstream");
}

/// P2 — a real plugin's ERROR document survives engenho's error path.
///
/// `static` refuses an ADD with no addresses. The value of asserting it
/// against a foreign plugin is that the error shape is upstream's, not
/// ours: reading only the exit code would lose `msg` and `details`, which
/// is the difference between "ADD failed" and a diagnosis.
#[tokio::test]
#[ignore = "needs containernetworking/plugins; see the module header"]
async fn a_foreign_plugins_error_document_reaches_the_operator() {
    let dir = plugin_dir();
    let body = serde_json::json!({
        "cniVersion": "1.0.0",
        "name": "engenho-differential-err",
        // `static` with no addresses is a configuration it must reject.
        "plugins": [{ "type": "static" }],
    });
    let config = parse_config(
        std::path::Path::new("/x/err.conflist"),
        body.to_string().as_bytes(),
    )
    .unwrap();

    let env = ExecCniEnv::new(vec![dir.clone()]);
    let planned = plan(
        &config,
        &sandbox("engenho-diff-err"),
        CniCommand::Add,
        &[dir],
    );
    let err = run_chain(&env, &planned)
        .await
        .expect_err("static with no addresses must fail");

    let msg = err.to_string();
    assert!(msg.contains("static"), "names the plugin: {msg}");
    // The plugin's own words, not a generic wrapper. Either the parsed
    // error document or its stderr — both are the plugin's, and losing
    // both is the failure this asserts against.
    assert!(
        msg.len() > "CNI plugin static: ".len(),
        "no reason survived: {msg}"
    );
}

/// P3 — engenho resolves a real plugin by name out of a real `CNI_PATH`.
///
/// The lookup is the first thing that breaks in a misconfigured
/// deployment, and a bare "not found" for a binary the operator installed
/// somewhere is the least actionable error there is.
#[tokio::test]
#[ignore = "needs containernetworking/plugins; see the module header"]
async fn plugin_resolution_finds_real_binaries_and_names_the_search_path() {
    let dir = plugin_dir();
    let env = ExecCniEnv::new(vec![dir.clone()]);

    for known in ["host-local", "static", "portmap", "loopback"] {
        assert!(
            env.resolve(known).is_ok(),
            "{known} is in {}",
            dir.display()
        );
    }

    let e = env
        .resolve("definitely-not-a-cni-plugin")
        .expect_err("an absent plugin is an error");
    assert!(e.to_string().contains(&dir.display().to_string()), "{e}");
}
