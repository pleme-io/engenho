//! `Reconciler<R>` — the typed contract every engenho controller satisfies.
//!
//! A controller is "a thing that watches a resource type + makes the
//! world match its spec." Engenho's controller-manager (M0.2) hosts N
//! reconcilers, one per controlled kind, each implemented as a
//! `Reconciler` impl.
//!
//! The trait is intentionally minimal — `reconcile()` is the only
//! callback. Implementations control idempotency, error handling, and
//! requeue policy via the typed [`ReconcileResult`].

use crate::error::KubeError;
use crate::kind::KubeResource;
use std::time::Duration;

/// One reconciliation attempt's outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileResult {
    /// Reconciliation succeeded; no follow-up scheduled. The next
    /// invocation will come from the next watch event or the resync
    /// timer.
    Done,

    /// Schedule another reconcile of the same object after `delay`.
    /// Useful for polling external state (image pulls, external load
    /// balancer provisioning) that the apiserver can't surface via a
    /// watch.
    Requeue(Duration),

    /// Like `Requeue` but signalling "I made progress but more work
    /// is left." Allows the requeue scheduler to prioritize this
    /// resource over idle/quiet ones.
    RequeueWithProgress(Duration),
}

/// Identifier for one reconciliation request. Mirrors upstream
/// `controller-runtime.Request`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ReconcileRequest {
    /// `metadata.namespace`, or `None` for cluster-scoped resources.
    pub namespace: Option<String>,
    /// `metadata.name`.
    pub name:      String,
}

/// A reconciler: idempotent, side-effecting, returns a typed outcome.
///
/// Implementations should be:
///   * **Idempotent** — calling twice with the same input must produce
///     the same observable state.
///   * **Read-driven** — fetch current state from the informer / client
///     each call; never rely on in-memory state from the previous call.
///   * **Total** — every code path returns `Result` (no panics).
#[async_trait::async_trait]
pub trait Reconciler<R: KubeResource>: Send + Sync {
    /// Reconcile one resource. Called by the controller framework
    /// per watch event or per requeue timer fire.
    ///
    /// # Errors
    ///
    /// Return [`KubeError`] to surface a transient failure (the
    /// framework will requeue with exponential backoff) or a
    /// declarative one (the framework deadletter + emits an event).
    async fn reconcile(
        &self,
        req: &ReconcileRequest,
    ) -> Result<ReconcileResult, KubeError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconcile_request_round_trips_via_json() {
        let r = ReconcileRequest {
            namespace: Some("default".into()),
            name:      "my-pod".into(),
        };
        let s = serde_json::to_string(&r).unwrap();
        let back: ReconcileRequest = serde_json::from_str(&s).unwrap();
        assert_eq!(r, back);
    }

    #[test]
    fn cluster_scoped_request_has_no_namespace() {
        let r = ReconcileRequest { namespace: None, name: "my-crd".into() };
        let s = serde_json::to_string(&r).unwrap();
        assert!(s.contains("\"namespace\":null"));
    }

    #[test]
    fn reconcile_result_variants() {
        let _ = ReconcileResult::Done;
        let _ = ReconcileResult::Requeue(Duration::from_secs(30));
        let _ = ReconcileResult::RequeueWithProgress(Duration::from_secs(5));
    }
}
