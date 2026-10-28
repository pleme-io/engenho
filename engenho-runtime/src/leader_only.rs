use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::{
    Controller, ControllerError, ControllerType, DeclaresReads, Reads, ReconcileOutcome,
    ReconcileReport, ReconcileResult,
};
use engenho_store::StoreMesh;

pub(crate) const STANDBY_POLL: Duration = Duration::from_secs(1);

pub(crate) struct OnLeader<C> {
    controller: C,
    store: Arc<StoreMesh>,
}

impl<C> OnLeader<C> {
    pub(crate) fn new(controller: C, store: Arc<StoreMesh>) -> Self {
        Self { controller, store }
    }
}

#[async_trait::async_trait]
impl<C: Controller> Controller for OnLeader<C> {
    fn name(&self) -> &'static str {
        self.controller.name()
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        if self.store.is_leader().await {
            return self.controller.tick().await;
        }
        Ok(ReconcileOutcome::new(
            ReconcileReport {
                note: Some(String::from(
                    "standing by: this node is not the raft leader",
                )),
                ..ReconcileReport::default()
            },
            ReconcileResult::Requeue(STANDBY_POLL),
        ))
    }

    fn controller_type(&self) -> ControllerType {
        self.controller.controller_type()
    }
}

impl<C: DeclaresReads> DeclaresReads for OnLeader<C> {
    fn reads(&self) -> Reads {
        self.controller.reads()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use engenho_store::testing::fast_config;
    use engenho_store::{InProcessRouter, RaftNodeId};

    use super::*;
    use crate::child::{Driver, Placement};

    const BOUND: Duration = Duration::from_secs(10);

    #[derive(Clone, Default)]
    struct Counting(Arc<AtomicUsize>);

    #[async_trait::async_trait]
    impl Controller for Counting {
        fn name(&self) -> &'static str {
            "counting"
        }

        async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(ReconcileOutcome::new(
                ReconcileReport::default(),
                ReconcileResult::Done,
            ))
        }
    }

    async fn three() -> Vec<Arc<StoreMesh>> {
        let router = InProcessRouter::new();
        let mut nodes = Vec::new();
        for id in 1..=3u64 {
            nodes.push(Arc::new(
                StoreMesh::start(
                    id,
                    format!("in-process://{id}"),
                    router.clone(),
                    fast_config("leader-only").unwrap(),
                )
                .await
                .unwrap(),
            ));
        }
        nodes[0]
            .initialize_with_voters(
                (1..=3u64)
                    .map(|id| (id, format!("in-process://{id}")))
                    .collect(),
            )
            .await
            .unwrap();
        nodes
    }

    async fn leader(nodes: &[Arc<StoreMesh>]) -> Arc<StoreMesh> {
        let deadline = tokio::time::Instant::now() + BOUND;
        loop {
            for node in nodes {
                if node.is_leader().await {
                    return node.clone();
                }
            }
            assert!(tokio::time::Instant::now() < deadline, "no leader");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn handed_over(
        nodes: &[Arc<StoreMesh>],
        heir: &Arc<StoreMesh>,
        from: RaftNodeId,
    ) -> Arc<StoreMesh> {
        let deadline = tokio::time::Instant::now() + BOUND;
        loop {
            heir.campaign().await.unwrap();
            let attempt = tokio::time::Instant::now() + Duration::from_secs(1);
            while tokio::time::Instant::now() < attempt {
                for node in nodes {
                    if node.node_id() != from && node.is_leader().await {
                        return node.clone();
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "leadership never left node {from}"
            );
        }
    }

    async fn ticks(on: &Arc<StoreMesh>) -> (usize, ReconcileResult) {
        let counting = Counting::default();
        let gated = OnLeader::new(counting.clone(), on.clone());
        let outcome = gated.tick().await.unwrap();
        (counting.0.load(Ordering::SeqCst), outcome.result)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_leader_only_loop_runs_on_the_leader_stands_by_elsewhere_and_follows_leadership() {
        let nodes = three().await;
        let first = leader(&nodes).await;
        assert_eq!(ticks(&first).await.0, 1, "the leader's loop did not run");
        for node in nodes.iter().filter(|n| n.node_id() != first.node_id()) {
            assert_eq!(
                ticks(node).await,
                (0, ReconcileResult::Requeue(STANDBY_POLL)),
                "node {} is not the leader, yet its loop ran",
                node.node_id()
            );
        }

        let heir = nodes
            .iter()
            .find(|n| n.node_id() != first.node_id())
            .unwrap();
        let second = handed_over(&nodes, heir, first.node_id()).await;
        let deadline = tokio::time::Instant::now() + BOUND;
        while first.is_leader().await {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the old leader never saw the new term"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            ticks(&second).await.0,
            1,
            "the new leader's loop did not start"
        );
        assert_eq!(
            ticks(&first).await.0,
            0,
            "the deposed leader's loop did not stop"
        );

        drop((first, second));
        for node in nodes {
            Arc::try_unwrap(node)
                .ok()
                .unwrap()
                .terminate()
                .await
                .unwrap();
        }
    }

    #[test]
    fn the_scheduler_and_the_cluster_controllers_are_the_leaders_and_node_loops_run_everywhere() {
        for driver in [
            Driver::Scheduler,
            Driver::Deployment,
            Driver::ReplicaSet,
            Driver::Gc,
            Driver::Namespace,
            Driver::NodeLifecycle,
            Driver::Endpoints,
        ] {
            assert_eq!(driver.placement(), Placement::Leader, "{driver:?}");
        }
        for driver in [
            Driver::Kubelet,
            Driver::Crd,
            Driver::ServiceRouting,
            Driver::CniStatus,
            Driver::CsiRegistrar,
            Driver::NodeManifests,
            Driver::NetworkPolicy,
        ] {
            assert_eq!(driver.placement(), Placement::EveryNode, "{driver:?}");
        }
    }
}
