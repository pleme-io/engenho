mod common;

use common::{boot_store, bound_node, get_pod, put_node, teardown, tick, unschedulable_message};
use engenho_scheduler::RoundRobinStrategy;
use engenho_store::{
    ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
};
use serde_json::{Value, json};

async fn put_pod(store: &StoreMesh, name: &str, image: &str, annotations: Value) {
    let spec = json!({ "containers": [{ "name": "main", "image": image }] });
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::namespaced("", "v1", "Pod", "default", name),
            value: json!({
                "kind": "Pod",
                "apiVersion": "v1",
                "metadata": { "name": name, "annotations": annotations },
                "spec": spec,
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

const OCI: &str = "podinfo:6";
const NIX: &str = "nix:/nix/store/00000000000000000000000000000000-app";

fn native_only() -> Value {
    json!({
        "capability.engenho.pleme.io/runtime.native": "true",
        "capability.engenho.pleme.io/runtime.oci": "false",
    })
}

fn oci_only() -> Value {
    json!({
        "capability.engenho.pleme.io/runtime.native": "false",
        "capability.engenho.pleme.io/runtime.oci": "true",
    })
}

#[tokio::test]
async fn an_oci_workload_never_lands_on_a_native_only_node() {
    let store = boot_store().await;
    put_node(&store, "ryn", native_only(), json!([])).await;
    put_pod(&store, "web", OCI, json!({})).await;

    let report = tick(&store, RoundRobinStrategy::new()).await;
    let pod = get_pod(&store, "web").await;

    assert_eq!(
        bound_node(&pod),
        None,
        "OCI pod bound to a native-only node"
    );
    assert_eq!(report.unschedulable, 1, "{report:?}");
    assert_eq!(
        unschedulable_message(&pod),
        Some("0/1 nodes are available: 1 node(s) lacked capability runtime.oci."),
        "{pod:#}"
    );
    teardown(store).await;
}

#[tokio::test]
async fn each_workload_lands_on_the_node_whose_runtime_it_needs_whichever_is_listed_first() {
    for (first, second) in [("a-native", "b-oci"), ("a-oci", "b-native")] {
        let store = boot_store().await;
        for name in [first, second] {
            let labels = if name.ends_with("native") {
                native_only()
            } else {
                oci_only()
            };
            put_node(&store, name, labels, json!([])).await;
        }
        put_pod(&store, "web", OCI, json!({})).await;
        put_pod(&store, "db", NIX, json!({})).await;

        let report = tick(&store, RoundRobinStrategy::new()).await;
        assert_eq!(report.bound.len(), 2, "{report:?}");
        let oci = if first.ends_with("oci") {
            first
        } else {
            second
        };
        let native = if first.ends_with("native") {
            first
        } else {
            second
        };
        assert_eq!(bound_node(&get_pod(&store, "web").await), Some(oci));
        assert_eq!(bound_node(&get_pod(&store, "db").await), Some(native));
        teardown(store).await;
    }
}

#[tokio::test]
async fn a_requirement_met_nowhere_is_pending_with_a_typed_reason_not_dropped() {
    let store = boot_store().await;
    put_node(&store, "ryn", native_only(), json!([])).await;
    put_node(&store, "rio", oci_only(), json!([])).await;
    put_pod(
        &store,
        "trainer",
        OCI,
        json!({ "requirements.engenho.pleme.io/gpu": "1" }),
    )
    .await;

    let report = tick(&store, RoundRobinStrategy::new()).await;
    let pod = get_pod(&store, "trainer").await;

    assert_eq!(bound_node(&pod), None);
    assert_eq!(report.unschedulable, 1, "{report:?}");
    assert_eq!(
        unschedulable_message(&pod),
        Some(
            "0/2 nodes are available: 1 node(s) lacked capability runtime.oci, \
             1 node(s) lacked capability gpu."
        ),
        "{pod:#}"
    );
    teardown(store).await;
}

#[tokio::test]
async fn an_unreadable_requirement_fits_no_node_and_says_so() {
    let store = boot_store().await;
    put_node(&store, "rio", oci_only(), json!([])).await;
    put_pod(
        &store,
        "trainer",
        OCI,
        json!({ "requirements.engenho.pleme.io/gpu": "lots" }),
    )
    .await;

    tick(&store, RoundRobinStrategy::new()).await;
    let pod = get_pod(&store, "trainer").await;

    assert_eq!(bound_node(&pod), None);
    assert_eq!(
        unschedulable_message(&pod),
        Some(
            "0/1 nodes are available: 1 node(s) could not satisfy the unreadable \
             requirement requirements.engenho.pleme.io/gpu."
        ),
        "{pod:#}"
    );
    teardown(store).await;
}

#[tokio::test]
async fn a_node_that_advertises_no_runtime_is_not_judged_on_runtime() {
    let store = boot_store().await;
    put_node(&store, "legacy", json!({}), json!([])).await;
    put_pod(&store, "web", OCI, json!({})).await;

    let report = tick(&store, RoundRobinStrategy::new()).await;

    assert_eq!(report.bound.len(), 1, "{report:?}");
    teardown(store).await;
}
