//! M1.2 — the `NetworkPolicy` producer reaches the enforcer AND the cluster.
//!
//! Instance #8 of "type + backend + no producer" closed. Every invariant is
//! stated as an ID so a future reader can tell which one a red run broke.
//!
//! * **N1** a policy in the store reaches the enforcer as rules
//! * **N2** the enforcement verdict is annotated on the object
//! * **N3** a computed-only enforcer emits the warning event
//! * **N4** a second tick changes nothing (no revision hot-loop, no event spam)
//! * **N5** deleting a policy reaps its rules from the enforcer
//! * **N6** an enforcer that FAILS does not stop the annotation or the tick

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};

use engenho_controllers::controller::Controller;
use engenho_controllers::event_recorder::{CollectingEventSink, Reason};
use engenho_controllers::network_policy::{
    FakeNetworkPolicyEnforcer, NetworkPolicyEnforcer, NetworkPolicyError, NetworkPolicyRule,
    PolicyDatapath,
};
use engenho_controllers::network_policy_controller::{
    ENFORCEMENT_ANNOTATION, NetworkPolicyController,
};
use engenho_store::command::{Reason as CommandReason, ResourceCommand};
use engenho_store::{InProcessRouter, ResourceKey, StoreMesh, default_config};

async fn boot(name: &str) -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config(name).unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

fn key(ns: &str, name: &str) -> ResourceKey {
    ResourceKey::namespaced("networking.k8s.io", "v1", "NetworkPolicy", ns, name)
}

async fn put(store: &StoreMesh, k: ResourceKey, v: Value) {
    store
        .propose(ResourceCommand::Put {
            key: k,
            value: v,
            expected: None,
            reason: CommandReason::Operator,
        })
        .await
        .unwrap();
}

fn deny_all(ns: &str, name: &str) -> Value {
    json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": { "name": name, "namespace": ns },
        "spec": { "podSelector": {}, "policyTypes": ["Ingress"] },
    })
}

/// N1 + N2 + N3 — the whole honest path in one pass.
#[tokio::test]
async fn a_policy_reaches_the_enforcer_and_says_it_is_not_enforced() {
    let store = boot("np-producer").await;
    put(&store, key("ns", "deny"), deny_all("ns", "deny")).await;

    let enforcer = Arc::new(FakeNetworkPolicyEnforcer::new());
    let events = Arc::new(CollectingEventSink::default());
    let c = NetworkPolicyController::new(store.clone(), enforcer.clone())
        .with_event_sink(events.clone());

    c.tick().await.unwrap();

    // N1 — the rule actually reached the backend. Before this controller
    // existed, this count was zero for every policy ever written.
    assert_eq!(
        enforcer.rule_count().await,
        1,
        "the rule reached the enforcer"
    );

    // N2 — and the object says which of the two things happened, because
    // no kubectl command can otherwise tell a computed policy from an
    // enforced one.
    let got = store.get(&key("ns", "deny")).await.expect("policy");
    assert_eq!(
        got["metadata"]["annotations"][ENFORCEMENT_ANNOTATION], "Computed",
        "a fake enforcer installs no filter and must not read as enforced"
    );

    // N3 — and an operator running `kubectl describe` sees it.
    let recorded = events.drain();
    let ev = recorded
        .iter()
        .find(|e| e.reason == Reason::NetworkPolicyNotEnforced)
        .expect("the not-enforced warning");
    assert_eq!(ev.involved.kind, "NetworkPolicy");
    assert_eq!(ev.involved.name, "deny");
    assert!(ev.message.contains("NOT restricted"), "{}", ev.message);

    // N4 — a second tick must not rewrite the object or re-emit the event.
    let before = store.current_catalog().await.revision();
    c.tick().await.unwrap();
    assert_eq!(
        store.current_catalog().await.revision(),
        before,
        "an unchanged rewrite every tick is the revision hot-loop class"
    );
    // `drain` emptied the sink above, so anything here is a re-emission.
    assert!(
        events.drain().is_empty(),
        "one event per transition, not one per tick"
    );

    drop(c);
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}

/// N5 — a deleted policy stops being enforced.
///
/// The failure this guards is the nastier direction of the same bug class:
/// a rule left installed after `kubectl delete netpol` keeps blocking
/// traffic with no object anywhere explaining why.
#[tokio::test]
async fn deleting_a_policy_reaps_its_rules() {
    let store = boot("np-reap").await;
    put(&store, key("ns", "a"), deny_all("ns", "a")).await;
    put(&store, key("ns", "b"), deny_all("ns", "b")).await;

    let enforcer = Arc::new(FakeNetworkPolicyEnforcer::new());
    let c = NetworkPolicyController::new(store.clone(), enforcer.clone());
    c.tick().await.unwrap();
    assert_eq!(enforcer.rule_count().await, 2);

    store
        .propose(ResourceCommand::Delete {
            key: key("ns", "b"),
            expected: None,
            reason: CommandReason::Operator,
            deletion_timestamp: None,
        })
        .await
        .unwrap();
    c.tick().await.unwrap();

    let left = enforcer.list().await.unwrap();
    assert_eq!(left.len(), 1, "the deleted policy's rule is gone: {left:?}");
    assert!(left[0].policy_id.starts_with("ns/a#"), "{:?}", left[0]);

    drop(c);
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}

/// N6 — a failing enforcer degrades, it does not wedge.
///
/// The annotation must still land: an operator whose enforcer is broken
/// needs the verdict MORE than one whose enforcer works.
#[tokio::test]
async fn a_failing_enforcer_still_leaves_the_verdict_on_the_object() {
    struct Broken;
    #[async_trait]
    impl NetworkPolicyEnforcer for Broken {
        fn name(&self) -> &'static str {
            "broken"
        }
        fn datapath(&self) -> PolicyDatapath {
            PolicyDatapath::Computed
        }
        async fn upsert(&self, _r: &NetworkPolicyRule) -> Result<(), NetworkPolicyError> {
            Err(NetworkPolicyError::Backend("down".into()))
        }
        async fn remove(&self, _id: &str) -> Result<(), NetworkPolicyError> {
            Err(NetworkPolicyError::Backend("down".into()))
        }
        async fn list(&self) -> Result<Vec<NetworkPolicyRule>, NetworkPolicyError> {
            Err(NetworkPolicyError::Backend("down".into()))
        }
    }

    let store = boot("np-broken").await;
    put(&store, key("ns", "deny"), deny_all("ns", "deny")).await;

    let c = NetworkPolicyController::new(store.clone(), Arc::new(Broken));
    let out = c.tick().await;
    assert!(out.is_ok(), "a broken enforcer must not fail the tick");

    let got = store.get(&key("ns", "deny")).await.expect("policy");
    assert_eq!(
        got["metadata"]["annotations"][ENFORCEMENT_ANNOTATION], "Computed",
        "the verdict lands even when the backend is down"
    );

    drop(c);
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}
