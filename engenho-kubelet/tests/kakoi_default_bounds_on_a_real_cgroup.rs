//! An enclosed pod's default bounds, read back from a real cgroup v2 leaf.
//!
//! Adopting moves the test process, so this holds one test in its own binary. A
//! delegated cgroup is the precondition, which a plain `cargo test` does not have:
//! `systemd-run --scope -p Delegate=yes <this binary>` is the run that proves it.

#![cfg(target_os = "linux")]
#![allow(
    clippy::disallowed_methods,
    reason = "drives the native runtime directly; no kubelet in the loop"
)]

use engenho_kubelet::backend::{ContainerRuntime, ContainerSpec, PodIdentity};
use engenho_kubelet::cgroup::{Cgroups, HostFs};
use engenho_kubelet::native_backend::{Isolation, NativeBackend};

#[tokio::test]
async fn a_pod_that_declares_nothing_is_bounded_by_the_kernel() {
    if !rustix::process::geteuid().is_root() {
        return;
    }
    let cgroups = Cgroups::adopt(Box::new(HostFs::system()));
    let Cgroups::Delegated(tree) = &cgroups else {
        eprintln!("not delegated, so this run proves nothing: {cgroups:?}");
        return;
    };
    let tree = std::sync::Arc::clone(tree);
    let out = std::process::Command::new("nix")
        .args(["build", "--no-link", "--print-out-paths", "nixpkgs#busybox"])
        .output()
        .expect("nix");
    assert!(out.status.success());
    let tools = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        std::env::temp_dir().join("engenho-native-e2e-real-cgroup"),
        cgroups,
    );
    let script = format!(
        "{tools}/bin/cat /proc/self/cgroup; {tools}/bin/sleep 30 & \
         {tools}/bin/sleep 30"
    );
    let spec = ContainerSpec {
        name: "probe".to_string(),
        image: format!("nix:{tools}"),
        command: ["sh", "-c", &script].map(str::to_string).to_vec(),
        pod: PodIdentity {
            namespace: "kakoi-real-cgroup".to_string(),
            name: "bounded".to_string(),
            container_name: "probe".to_string(),
            ..PodIdentity::default()
        },
        ..ContainerSpec::default()
    };
    let started = backend.start(&spec).await.expect("start");
    let leaf = tree.leaf(&started.container_id);
    let at = |knob: &str| std::fs::read_to_string(leaf.join(knob)).unwrap();
    assert_eq!(at("pids.max").trim(), "4096");
    let memory: u64 = at("memory.max").trim().parse().unwrap();
    let node = engenho_kubelet::kakoi::node_memory_bytes();
    assert!(
        memory >= (1 << 30).min(node / 4) && memory <= node,
        "{memory}"
    );
    assert_eq!(at("memory.swap.max").trim(), "0");
    assert_eq!(at("memory.oom.group").trim(), "1");
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let tasks: u64 = at("pids.current").trim().parse().unwrap();
    assert!(tasks >= 2, "the leaf holds the pod's tasks: {tasks}");
    backend.stop(&started.container_id).await.expect("stop");
}
