//! I21 / T5.8 — every config field the runtime is given is run, refused or
//! declared not run; and I13 — the node-selector claim registration makes is
//! the scheduler's behaviour.
//!
//! | config | before | now |
//! |---|---|---|
//! | `scheduler.namespace` | ignored: the scheduler was scoped by `controllers.namespace` | scopes the scheduler |
//! | `scheduler.tick_interval_seconds` | ignored: the scheduler fell back on the controllers' tick | the scheduler's fallback |
//! | `runtime.tls` naming an operator PKI | ignored: the runtime minted its own CA | refused at start |
//! | `revoada.topology.strategy` naming several masters | ignored: one voter ran | refused at start |
//! | `consistency.default_tier` other than strong | ignored | refused at start |
//!
//! Tier: the refusals are a `Result::Err` at start (parse-boundary, not a
//! type). What makes a NEW field impossible to ignore is the destructure in
//! `boot_config.rs` (E0027); these tests pin what the existing fields do.

use std::time::{Duration, Instant};

use engenho_config::{
    ConsistencyTierKind, EngenhoConfig, KubeletBackendKind, TopologyStrategyKind,
};
use engenho_runtime::{Child, Driver, PkiField, Runtime, RuntimeError, Unhonoured};
use engenho_store::command::{Reason, ResourceCommand};
use engenho_store::{ResourceKey, StoreMesh};
use serde_json::{Value, json};
use shikumi::TieredConfig;

const NODE: &str = "node-A";

/// How long a booted daemon may take to bind a pending pod: the node lease's
/// first renewal makes the node Ready, then one scheduler tick. The rest is
/// CI slack.
const BOUND_WITHIN: Duration = Duration::from_secs(30);

/// Ephemeral store, fake kubelet backend, plaintext, every listener on an
/// ephemeral loopback port, a 1 s controller fallback.
fn config(data_dir: &std::path::Path) -> EngenhoConfig {
    let mut cfg = EngenhoConfig::prescribed_default();
    cfg.runtime.listen_addr = "127.0.0.1:0".into();
    cfg.runtime.kubelet_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.etcd_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.durable = false;
    cfg.runtime.node_name = NODE.into();
    cfg.runtime.kubelet_backend = KubeletBackendKind::Fake;
    cfg.runtime.leadership_timeout_seconds = 5;
    cfg.runtime.tls.enabled = false;
    cfg.runtime.data_dir = data_dir.to_path_buf();
    cfg.controllers.fallback_interval_seconds = 1;
    cfg.controllers.debounce_milliseconds = 20;
    cfg
}

fn pod_key(namespace: &str, name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", namespace, name)
}

async fn put_pod(store: &StoreMesh, namespace: &str, name: &str, spec_extra: Value) {
    let mut spec = json!({ "containers": [{ "name": "main", "image": "podinfo:6" }] });
    if let (Some(spec), Some(extra)) = (spec.as_object_mut(), spec_extra.as_object()) {
        spec.extend(extra.clone());
    }
    store
        .propose(ResourceCommand::Put {
            key: pod_key(namespace, name),
            value: json!({
                "kind": "Pod",
                "apiVersion": "v1",
                "metadata": { "name": name, "namespace": namespace },
                "spec": spec,
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .expect("the pod is written");
}

async fn pod(store: &StoreMesh, namespace: &str, name: &str) -> Value {
    store
        .get(&pod_key(namespace, name))
        .await
        .expect("the pod exists")
}

fn node_name(pod: &Value) -> Option<&str> {
    pod.pointer("/spec/nodeName")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

/// Poll until `done` holds for the pod, or fail with the pod as last read.
async fn until(
    store: &StoreMesh,
    namespace: &str,
    name: &str,
    what: &str,
    done: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = Instant::now() + BOUND_WITHIN;
    loop {
        let current = pod(store, namespace, name).await;
        if done(&current) {
            return current;
        }
        assert!(
            Instant::now() < deadline,
            "{namespace}/{name} never {what} within {BOUND_WITHIN:?}: {current}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// `scheduler.namespace` scopes the scheduler the runtime runs. It used to be
/// read by nothing: the runtime scoped the scheduler by
/// `controllers.namespace`, empty here, so team-b's pod was bound too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_scheduler_places_only_in_the_scheduler_namespace() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut cfg = config(dir.path());
    cfg.scheduler.namespace = "team-a".into();
    cfg.scheduler.tick_interval_seconds = 1;
    assert!(cfg.controllers.namespace.is_empty(), "precondition");
    let rt = Runtime::start(cfg).await.expect("the runtime boots");
    let store = rt.store();

    put_pod(&store, "team-a", "pa", json!({})).await;
    put_pod(&store, "team-b", "pb", json!({})).await;

    let pa = until(&store, "team-a", "pa", "was bound", |p| {
        node_name(p).is_some()
    })
    .await;
    assert_eq!(node_name(&pa), Some(NODE));

    // Three more scheduler fallbacks: time enough to have bound team-b's pod
    // were it in scope.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let pb = pod(&store, "team-b", "pb").await;
    assert_eq!(node_name(&pb), None, "team-b is out of scope: {pb}");
    assert!(
        pb.pointer("/status/conditions").is_none(),
        "nothing is written to an out-of-scope pod: {pb}"
    );
}

/// `scheduler.tick_interval_seconds` is the fallback the scheduler's loop
/// runs on. It used to be read by nothing: the scheduler fell back on the
/// controllers' interval, an hour here, so an idle scheduler did not tick.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_scheduler_falls_back_on_its_own_tick_not_the_controllers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut cfg = config(dir.path());
    cfg.controllers.fallback_interval_seconds = 3600;
    cfg.scheduler.tick_interval_seconds = 1;
    let rt = Runtime::start(cfg).await.expect("the runtime boots");
    let ticks = || {
        rt.children()
            .get(Child::Driver(Driver::Scheduler))
            .expect("the scheduler is spawned")
            .beat()
            .snapshot()
            .ticks_started
    };

    // Let boot's own writes (the Node, the first lease) wake it and settle.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let before = ticks();
    tokio::time::sleep(Duration::from_millis(4500)).await;
    let after = ticks();
    assert!(
        after.saturating_sub(before) >= 3,
        "an idle scheduler on a 1s fallback ticks about four times in 4.5s; it ticked \
         {} times (from {before} to {after})",
        after.saturating_sub(before)
    );
}

/// A field the runtime would not honour fails `Runtime::start` with the
/// field named, before the data directory is created. Each used to boot a
/// runtime that did something else under the operator's name.
#[tokio::test]
async fn a_field_the_runtime_does_not_run_is_refused_before_anything_is_written() {
    type Edit = fn(&mut EngenhoConfig);
    let cases: [(Edit, Unhonoured); 4] = [
        (
            |c| {
                c.runtime.tls.enabled = true;
                c.runtime.tls.ca_cert_path = Some("/etc/pki/ca.crt".into());
            },
            Unhonoured::OperatorPki {
                field: PkiField::CaCertPath,
            },
        ),
        (
            |c| {
                c.runtime.tls.enabled = true;
                c.runtime.tls.auto_generate = false;
                c.runtime.tls.ca_cert_path = Some("/etc/pki/ca.crt".into());
                c.runtime.tls.ca_key_path = Some("/etc/pki/ca.key".into());
                c.runtime.tls.cert_path = Some("/etc/pki/srv.crt".into());
                c.runtime.tls.key_path = Some("/etc/pki/srv.key".into());
            },
            Unhonoured::OperatorPki {
                field: PkiField::AutoGenerateOff,
            },
        ),
        (
            |c| {
                c.revoada.topology.strategy = TopologyStrategyKind::Quorum3M;
                c.revoada.topology.min_nodes = 3;
            },
            Unhonoured::MultiNodeFormation {
                strategy: TopologyStrategyKind::Quorum3M,
                masters: 3,
            },
        ),
        (
            |c| c.consistency.default_tier = ConsistencyTierKind::EventualGossip,
            Unhonoured::ConsistencyTier {
                requested: ConsistencyTierKind::EventualGossip,
            },
        ),
    ];
    for (edit, expected) in cases {
        let scratch = tempfile::tempdir().expect("tempdir");
        let data_dir = scratch.path().join("never-created");
        let mut cfg = config(&data_dir);
        edit(&mut cfg);
        cfg.validate()
            .expect("precondition: the config is valid, and only unhonoured");
        match Runtime::start(cfg).await {
            Err(RuntimeError::Unhonoured(refused)) => assert_eq!(refused, expected),
            Err(other) => panic!("expected {expected:?}, got: {other}"),
            Ok(_) => panic!("booted a runtime that does not do what {expected:?} asks"),
        }
        assert!(
            !data_dir.exists(),
            "{expected:?}: the refusal must land before anything is written"
        );
    }
}

/// I13: what registration's comments claim. The scheduler's `NodeSelector`
/// filter (T5.7) evaluates a pod's `spec.nodeSelector` against the labels
/// registration writes, in Go's vocabulary: a pod selecting the node's own
/// `kubernetes.io/arch` and `kubernetes.io/os` is bound, and one selecting
/// Rust's spelling of the architecture stays Pending with the selector named.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_selector_in_go_vocabulary_matches_the_registered_node() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut cfg = config(dir.path());
    cfg.scheduler.tick_interval_seconds = 1;
    let rt = Runtime::start(cfg).await.expect("the runtime boots");
    let store = rt.store();

    let node = store
        .get(&ResourceKey::cluster_scoped("", "v1", "Node", NODE))
        .await
        .expect("the node registered itself");
    let label = |key: &str| {
        node.pointer("/metadata/labels")
            .and_then(|labels| labels.get(key))
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("the registered node carries {key}: {node}"))
            .to_owned()
    };
    let (arch, os) = (label("kubernetes.io/arch"), label("kubernetes.io/os"));
    assert_ne!(
        arch,
        std::env::consts::ARCH,
        "precondition: the label is Go's GOARCH, not Rust's spelling"
    );

    put_pod(
        &store,
        "default",
        "go",
        json!({ "nodeSelector": { "kubernetes.io/arch": arch, "kubernetes.io/os": os } }),
    )
    .await;
    put_pod(
        &store,
        "default",
        "rust",
        json!({ "nodeSelector": { "kubernetes.io/arch": std::env::consts::ARCH } }),
    )
    .await;

    let go = until(&store, "default", "go", "was bound", |p| {
        node_name(p).is_some()
    })
    .await;
    assert_eq!(node_name(&go), Some(NODE));

    let rust = until(&store, "default", "rust", "was marked unschedulable", |p| {
        p.pointer("/status/conditions")
            .and_then(Value::as_array)
            .is_some_and(|conditions| {
                conditions.iter().any(|c| {
                    c["type"] == "PodScheduled"
                        && c["status"] == "False"
                        && c["reason"] == "Unschedulable"
                })
            })
    })
    .await;
    assert_eq!(node_name(&rust), None, "{rust}");
    let message = rust
        .pointer("/status/conditions")
        .and_then(Value::as_array)
        .and_then(|conditions| {
            conditions
                .iter()
                .find(|c| c["type"] == "PodScheduled")
                .and_then(|c| c["message"].as_str())
        })
        .unwrap_or_default();
    assert!(
        message.contains("didn't match Pod's node selector"),
        "the rejection names the selector: {message:?}"
    );
}
