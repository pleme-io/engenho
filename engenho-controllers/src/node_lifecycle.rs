use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::StoreMesh;
use engenho_store::resource::ResourceKey;

use crate::controller::{Controller, ReconcileOutcome, ReconcileReport};
use crate::effect::{Effect, Refusal};
use crate::error::ControllerError;
use crate::node_lease::lease_key;
use crate::node_readiness::{ReadinessPublish, publish_ready};
use crate::reads::{DeclaresReads, Reads, gvk};

pub struct NodeLifecycleController {
    store: Arc<StoreMesh>,
}

impl NodeLifecycleController {
    #[must_use]
    pub fn new(store: Arc<StoreMesh>) -> Self {
        Self { store }
    }
}

impl DeclaresReads for NodeLifecycleController {
    fn reads(&self) -> Reads {
        Reads::of(&[
            gvk("", "v1", "Node"),
            gvk("coordination.k8s.io", "v1", "Lease"),
        ])
    }
}

#[async_trait]
impl Controller for NodeLifecycleController {
    fn name(&self) -> &'static str {
        "node-lifecycle"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let mut report = ReconcileReport::default();
        let nodes = self.store.list("", "v1", "Node", None).await;
        for (key, node) in nodes {
            report.objects_examined += 1;
            let Some(name) = node
                .pointer("/metadata/name")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
            else {
                report.objects_skipped += 1;
                continue;
            };
            let lease = self.store.get(&lease_key(&name)).await;
            let now = engenho_types::time::now_rfc3339_utc();
            record(
                &mut report,
                &key,
                publish_ready(&self.store, &key, lease.as_ref(), &now, None).await,
            );
        }
        Ok(ReconcileOutcome::from(report))
    }
}

fn record(report: &mut ReconcileReport, key: &ResourceKey, outcome: ReadinessPublish) {
    match outcome {
        ReadinessPublish::Published {
            condition, effect, ..
        } => {
            tracing::info!(
                node = %key.name,
                status = condition.get("status").and_then(serde_json::Value::as_str),
                reason = condition.get("reason").and_then(serde_json::Value::as_str),
                "node Ready condition published from its lease"
            );
            report.record(effect);
        }
        ReadinessPublish::Unchanged | ReadinessPublish::NoNode => {}
        ReadinessPublish::Contended => report.record(Effect::Rejected(Refusal::Conflict)),
        ReadinessPublish::Unversioned | ReadinessPublish::Malformed => {
            report.objects_skipped += 1;
        }
        ReadinessPublish::Store(e) => {
            tracing::warn!(node = %key.name, error = %e, "could not publish the node Ready condition");
            report.objects_skipped += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use engenho_store::command::{Reason, ResourceCommand};
    use engenho_store::{InProcessRouter, WatchEventKind, WatchOpts, WatchSignal, default_config};
    use serde_json::json;

    use super::*;
    use crate::node_lease::{find_ready_condition, lease_value};
    use crate::status::resource_version_of;

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

    fn node_key() -> ResourceKey {
        ResourceKey::cluster_scoped("", "v1", "Node", "node-A")
    }

    async fn put(store: &StoreMesh, key: ResourceKey, value: serde_json::Value) {
        store
            .propose(ResourceCommand::Put {
                key,
                value,
                expected: None,
                reason: Reason::Operator,
            })
            .await
            .unwrap();
    }

    async fn seed_ready_node(store: &StoreMesh) {
        put(
            store,
            node_key(),
            json!({
                "kind": "Node",
                "apiVersion": "v1",
                "metadata": { "name": "node-A" },
                "status": { "conditions": [{
                    "type": "Ready",
                    "status": "True",
                    "reason": "KubeletReady",
                    "lastHeartbeatTime": "2026-10-15T12:00:00Z",
                    "lastTransitionTime": "2026-10-15T12:00:00Z"
                }]}
            }),
        )
        .await;
    }

    #[tokio::test]
    async fn a_stale_lease_moves_the_stored_node_to_unknown_with_a_new_revision_and_an_event() {
        let store = boot("node-lifecycle-stale").await;
        seed_ready_node(&store).await;
        put(
            &store,
            lease_key("node-A"),
            lease_value("node-A", "2020-01-01T00:00:00Z", 0),
        )
        .await;
        let before = store.get(&node_key()).await.unwrap();
        let from = store.current_revision().await;
        let mut watch = store
            .watch_from(WatchOpts::from_revision(from))
            .await
            .unwrap();

        let controller = NodeLifecycleController::new(store.clone());
        controller.tick().await.unwrap();

        let after = store.get(&node_key()).await.unwrap();
        let ready = find_ready_condition(&after).unwrap();
        assert_eq!(ready["status"], "Unknown", "{after}");
        assert_eq!(ready["reason"], "NodeStatusUnknown", "{after}");
        assert!(
            resource_version_of(&after).unwrap() > resource_version_of(&before).unwrap(),
            "the transition is a new revision: {after}"
        );
        let event = tokio::time::timeout(Duration::from_secs(2), watch.next())
            .await
            .expect("an event within 2s")
            .expect("the stream is open")
            .expect("not gone");
        let WatchSignal::Event(event) = event else {
            panic!("expected a MODIFIED event, got {event:?}");
        };
        assert_eq!(event.kind, WatchEventKind::Modified);
        assert_eq!(event.key, node_key());
        assert_eq!(
            find_ready_condition(&event.object).unwrap()["status"],
            "Unknown"
        );

        let held = store.current_revision().await;
        controller.tick().await.unwrap();
        assert_eq!(
            store.current_revision().await,
            held,
            "an unchanged judgement writes nothing"
        );
    }

    #[tokio::test]
    async fn a_fresh_lease_moves_an_unknown_node_back_to_ready() {
        let store = boot("node-lifecycle-fresh").await;
        put(
            &store,
            node_key(),
            json!({
                "kind": "Node",
                "apiVersion": "v1",
                "metadata": { "name": "node-A" },
                "status": { "conditions": [{ "type": "Ready", "status": "Unknown",
                    "reason": "NodeStatusNeverUpdated" }]}
            }),
        )
        .await;
        put(
            &store,
            lease_key("node-A"),
            lease_value("node-A", &engenho_types::time::now_rfc3339_utc(), 0),
        )
        .await;
        NodeLifecycleController::new(store.clone())
            .tick()
            .await
            .unwrap();
        let after = store.get(&node_key()).await.unwrap();
        assert_eq!(find_ready_condition(&after).unwrap()["status"], "True");
    }
}
