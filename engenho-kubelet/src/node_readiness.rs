//! Publishing this node's `Ready` condition WITHOUT undoing anyone else's
//! write to the Node (T1.3b).
//!
//! ── ★ THE DEFECT THIS CLOSES ──────────────────────────────────────────────
//! The kubelet read the whole Node, merged its `Ready` condition in, and wrote
//! the whole Node back with no precondition. Anything that committed between
//! that read and that write — `kubectl cordon` setting `spec.unschedulable`,
//! a taint, a label — was replaced by the kubelet's older copy. Nothing
//! errored: the write succeeded, the condition it carried was correct, and the
//! cordon was simply gone. The scheduler then placed onto a node an operator
//! had just drained.
//!
//! ── ★ THE SHAPE ───────────────────────────────────────────────────────────
//! The write is a `Put` whose precondition is the per-key revision the Node
//! carried when it was read. [`ObservedNode`] is the only thing the write
//! accepts, and it cannot be built without that revision, so this module has
//! no path to a Node write without a precondition. A lost race is refused by
//! the store (replicated CAS: no mutation, no revision, no watch event); the
//! Node is re-read, the condition is re-derived against the fresh copy, and
//! the write is tried ONCE more. A second loss is dropped: the next tick
//! recomputes from whatever is current then, so a busy concurrent writer can
//! delay a readiness publish by a tick but can never be overwritten by one.
//!
//! The condition itself comes from
//! [`engenho_controllers::node_lease::project_ready_condition`] — the same
//! derivation the apiserver applies at read time — so the value the kubelet
//! writes and the value a reader is served cannot be computed two ways.
//!
//! ── ★ TIER, STATED ────────────────────────────────────────────────────────
//! A stale write being refused is enforced by the store's CAS, identically on
//! every replica. That this module always states a precondition is a
//! construction discipline local to this file (a value that carries its
//! revision), not a crate-wide type: another module holding the store can
//! still issue an unconditional Node `Put`.

use engenho_controllers::node_lease::{find_ready_condition, project_ready_condition};
use engenho_controllers::status::resource_version_of;
use engenho_store::{
    ResourceKey, ResourceValue, StoreError, StoreMesh,
    command::{Reason, ResourceCommand, ResourceOp},
    revision::Revision,
};
use serde_json::Value;

/// A Node as read from the store, paired with the revision it was read at.
///
/// Every Node write in this module takes one of these, and the write's
/// precondition is `revision` — so what was written is always what was
/// derived from, or nothing.
#[derive(Debug, Clone)]
pub(crate) struct ObservedNode {
    value: ResourceValue,
    revision: Revision,
}

/// Why a Node could not be observed at a revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unobservable {
    /// No object at the key: registration has not landed, or the Node was
    /// deleted.
    Absent,
    /// The stored object carries no parseable `metadata.resourceVersion`.
    /// The store stamps one on every write, so this is a store defect; it is
    /// still a state rather than a guess, because the alternative — writing
    /// with no precondition — is the defect this module exists to remove.
    Unversioned,
}

impl ObservedNode {
    /// Pair a stored Node with the revision it carries.
    ///
    /// # Errors
    ///
    /// [`Unobservable::Unversioned`] when `metadata.resourceVersion` is
    /// absent or not a revision.
    pub(crate) fn from_stored(value: ResourceValue) -> Result<Self, Unobservable> {
        let revision = resource_version_of(&value).ok_or(Unobservable::Unversioned)?;
        Ok(Self { value, revision })
    }

    /// Read the Node at `key` together with its revision.
    ///
    /// # Errors
    ///
    /// [`Unobservable::Absent`] when there is no object at `key`;
    /// [`Unobservable::Unversioned`] as for [`Self::from_stored`].
    pub(crate) async fn read(store: &StoreMesh, key: &ResourceKey) -> Result<Self, Unobservable> {
        let value = store.get(key).await.ok_or(Unobservable::Absent)?;
        Self::from_stored(value)
    }
}

/// What one readiness publish did. Each arm is a fact the publish observed;
/// none is inferred from another.
#[derive(Debug)]
pub(crate) enum ReadinessPublish {
    /// The derived condition committed. `retried` is true when the first write
    /// lost to a concurrent one and the re-derived write committed instead.
    /// `condition` is the `Ready` condition as written.
    Published {
        /// Whether the committed write was the second attempt.
        retried: bool,
        /// The `Ready` condition that was written.
        condition: Value,
    },
    /// The stored `Ready` condition already carries the derived status and
    /// reason. Nothing was proposed: `lastHeartbeatTime` moves every tick, so
    /// writing on it would grow the journal without bound on an idle cluster.
    Unchanged,
    /// There is no Node at the key. It is never created here: a Node deleted
    /// between the read and the write stays deleted.
    NoNode,
    /// The stored Node carries no revision, so no precondition can be stated
    /// and nothing was proposed.
    Unversioned,
    /// The stored Node is not a JSON object, so it cannot carry a condition.
    /// Nothing was proposed: rewriting it is not the kubelet's call.
    Malformed,
    /// Both attempts lost to concurrent writes. Dropped; the next tick
    /// re-derives from the Node as it is then.
    Contended,
    /// The store could not take the proposal at all.
    Store(StoreError),
}

impl From<Unobservable> for ReadinessPublish {
    fn from(why: Unobservable) -> Self {
        match why {
            Unobservable::Absent => Self::NoNode,
            Unobservable::Unversioned => Self::Unversioned,
        }
    }
}

/// One CAS attempt, before the retry decision.
enum Attempt {
    Written(Value),
    Unchanged,
    Malformed,
    Lost,
    Failed(StoreError),
}

impl Attempt {
    /// The publish outcome for an attempt that did not lose the race.
    /// `Lost` is resolved by the caller, which alone knows whether a retry
    /// remains.
    fn settled(self, retried: bool) -> Option<ReadinessPublish> {
        match self {
            Self::Written(condition) => Some(ReadinessPublish::Published { retried, condition }),
            Self::Unchanged => Some(ReadinessPublish::Unchanged),
            Self::Malformed => Some(ReadinessPublish::Malformed),
            Self::Failed(e) => Some(ReadinessPublish::Store(e)),
            Self::Lost => None,
        }
    }
}

/// Read the Node at `key`, derive its `Ready` condition from `lease`, and
/// write it with a precondition — retrying once after a re-read if a
/// concurrent write got there first.
///
/// `lease` is this node's Lease as read from the store (`None` when it has
/// none); `now` stamps `lastHeartbeatTime`.
pub(crate) async fn publish_ready(
    store: &StoreMesh,
    key: &ResourceKey,
    lease: Option<&ResourceValue>,
    now: &str,
) -> ReadinessPublish {
    match ObservedNode::read(store, key).await {
        Ok(observed) => publish_ready_onto(store, key, observed, lease, now).await,
        Err(why) => why.into(),
    }
}

/// [`publish_ready`] from a Node already read.
///
/// The seam exists so the race is testable: a caller may hand in an
/// observation that a concurrent write has since superseded, which is exactly
/// what happens when a cordon lands between the read and the write.
pub(crate) async fn publish_ready_onto(
    store: &StoreMesh,
    key: &ResourceKey,
    observed: ObservedNode,
    lease: Option<&ResourceValue>,
    now: &str,
) -> ReadinessPublish {
    if let Some(done) = attempt(store, key, &observed, lease, now)
        .await
        .settled(false)
    {
        return done;
    }
    // Lost the race once: whatever committed since our read is now the base.
    let fresh = match ObservedNode::read(store, key).await {
        Ok(fresh) => fresh,
        Err(why) => return why.into(),
    };
    attempt(store, key, &fresh, lease, now)
        .await
        .settled(true)
        .unwrap_or(ReadinessPublish::Contended)
}

/// Derive the condition onto `observed` and write it at `observed.revision`.
async fn attempt(
    store: &StoreMesh,
    key: &ResourceKey,
    observed: &ObservedNode,
    lease: Option<&ResourceValue>,
    now: &str,
) -> Attempt {
    let mut desired = observed.value.clone();
    project_ready_condition(&mut desired, lease, now);
    // `project_ready_condition` leaves a Ready condition on every JSON
    // object, so its absence here means the stored Node is not one.
    let Some(condition) = find_ready_condition(&desired).cloned() else {
        return Attempt::Malformed;
    };

    // Only what an operator acts on counts as a change; see
    // [`ReadinessPublish::Unchanged`].
    let unchanged = find_ready_condition(&observed.value).is_some_and(|previous| {
        previous.get("status") == condition.get("status")
            && previous.get("reason") == condition.get("reason")
    });
    if unchanged {
        return Attempt::Unchanged;
    }

    match store
        .propose(ResourceCommand::Put {
            key: key.clone(),
            value: desired,
            expected: Some(observed.revision),
            reason: Reason::Controller,
        })
        .await
    {
        Ok(applied) if applied.op == ResourceOp::Conflict => Attempt::Lost,
        Ok(_) => Attempt::Written(condition),
        Err(e) => Attempt::Failed(e),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use engenho_controllers::node_lease::lease_value;
    use engenho_store::{InProcessRouter, default_config};
    use serde_json::json;

    use super::*;

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

    async fn shutdown(store: Arc<StoreMesh>) {
        Arc::try_unwrap(store)
            .ok()
            .unwrap()
            .terminate()
            .await
            .unwrap();
    }

    fn node_key() -> ResourceKey {
        ResourceKey::cluster_scoped("", "v1", "Node", "node-A")
    }

    /// A lease renewed just now — derives `Ready`.
    fn fresh_lease() -> ResourceValue {
        lease_value("node-A", &engenho_types::time::now_rfc3339_utc(), 0)
    }

    /// Seed the Node the way `register_node` stamps it at boot, plus a
    /// condition this kubelet does not own.
    async fn seed_node(store: &StoreMesh) {
        store
            .propose(ResourceCommand::Put {
                key: node_key(),
                value: json!({
                    "kind": "Node",
                    "apiVersion": "v1",
                    "metadata": { "name": "node-A" },
                    "status": { "conditions": [
                        { "type": "Ready", "status": "True" },
                        { "type": "MemoryPressure", "status": "False" }
                    ]}
                }),
                expected: None,
                reason: Reason::Operator,
            })
            .await
            .unwrap();
    }

    /// `kubectl cordon` plus a taint and a label, committed as an operator
    /// write on top of whatever the Node currently is.
    async fn cordon(store: &StoreMesh) {
        let mut node = store.get(&node_key()).await.unwrap();
        node["spec"] = json!({
            "unschedulable": true,
            "taints": [{
                "key": "node.kubernetes.io/unschedulable",
                "effect": "NoSchedule"
            }]
        });
        node["metadata"]["labels"] = json!({ "drain": "in-progress" });
        store
            .propose(ResourceCommand::Put {
                key: node_key(),
                value: node,
                expected: None,
                reason: Reason::Operator,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_cordon_committed_between_read_and_write_survives() {
        let store = boot("readiness-cordon-race").await;
        seed_node(&store).await;

        // The kubelet reads the Node...
        let stale = ObservedNode::read(&store, &node_key()).await.unwrap();
        // ...an operator cordons it...
        cordon(&store).await;
        // ...and the kubelet writes what it derived from the older read.
        let lease = fresh_lease();
        let outcome = publish_ready_onto(
            &store,
            &node_key(),
            stale,
            Some(&lease),
            &engenho_types::time::now_rfc3339_utc(),
        )
        .await;

        let node = store.get(&node_key()).await.unwrap();
        assert_eq!(
            node["spec"]["unschedulable"], true,
            "a readiness publish must never undo a cordon that committed after its read: {node}"
        );
        assert_eq!(
            node["spec"]["taints"][0]["key"], "node.kubernetes.io/unschedulable",
            "nor the taint that came with it: {node}"
        );
        assert_eq!(
            node["metadata"]["labels"]["drain"], "in-progress",
            "nor a label: {node}"
        );

        // Losing the race is not a reason to publish nothing: the retry
        // re-derives onto the cordoned Node and commits.
        assert!(
            matches!(outcome, ReadinessPublish::Published { retried: true, .. }),
            "the first write loses, the re-read write commits: {outcome:?}"
        );
        let ready = find_ready_condition(&node).expect("Ready was published");
        assert_eq!(ready["status"], "True", "{node}");
        assert_eq!(ready["reason"], "KubeletReady", "{node}");
        assert!(
            node["status"]["conditions"]
                .as_array()
                .is_some_and(|cs| cs.iter().any(|c| c["type"] == "MemoryPressure")),
            "a condition this kubelet does not own survives the merge: {node}"
        );

        shutdown(store).await;
    }

    #[tokio::test]
    async fn a_node_deleted_between_read_and_write_is_not_recreated() {
        let store = boot("readiness-delete-race").await;
        seed_node(&store).await;

        let stale = ObservedNode::read(&store, &node_key()).await.unwrap();
        store
            .propose(ResourceCommand::delete(node_key(), Reason::Operator))
            .await
            .unwrap();

        let lease = fresh_lease();
        let outcome = publish_ready_onto(
            &store,
            &node_key(),
            stale,
            Some(&lease),
            &engenho_types::time::now_rfc3339_utc(),
        )
        .await;

        assert!(
            store.get(&node_key()).await.is_none(),
            "a readiness publish must not resurrect a deleted Node"
        );
        assert!(
            matches!(outcome, ReadinessPublish::NoNode),
            "the re-read finds nothing and says so: {outcome:?}"
        );

        shutdown(store).await;
    }

    #[tokio::test]
    async fn an_uncontended_publish_commits_first_time_then_holds() {
        let store = boot("readiness-uncontended").await;
        seed_node(&store).await;
        let lease = fresh_lease();

        let first = publish_ready(
            &store,
            &node_key(),
            Some(&lease),
            &engenho_types::time::now_rfc3339_utc(),
        )
        .await;
        assert!(
            matches!(first, ReadinessPublish::Published { retried: false, .. }),
            "nothing raced it, so the first write commits: {first:?}"
        );

        // Same derivation again: nothing an operator acts on changed, so
        // nothing is proposed and the revision does not move.
        let before = store.current_revision().await;
        let second = publish_ready(
            &store,
            &node_key(),
            Some(&lease),
            &engenho_types::time::now_rfc3339_utc(),
        )
        .await;
        assert!(matches!(second, ReadinessPublish::Unchanged), "{second:?}");
        assert_eq!(store.current_revision().await, before);

        shutdown(store).await;
    }

    #[tokio::test]
    async fn no_node_means_nothing_is_written() {
        let store = boot("readiness-no-node").await;
        let lease = fresh_lease();
        let outcome = publish_ready(
            &store,
            &node_key(),
            Some(&lease),
            &engenho_types::time::now_rfc3339_utc(),
        )
        .await;
        assert!(matches!(outcome, ReadinessPublish::NoNode), "{outcome:?}");
        assert!(
            store.get(&node_key()).await.is_none(),
            "registration creates Nodes; a readiness publish never does"
        );
        shutdown(store).await;
    }

    #[test]
    fn a_node_without_a_revision_cannot_be_observed() {
        // No revision, no precondition — and so no write. An observation that
        // silently defaulted the revision would reopen the unconditional put.
        let unversioned = json!({ "kind": "Node", "metadata": { "name": "node-A" } });
        assert_eq!(
            ObservedNode::from_stored(unversioned).err(),
            Some(Unobservable::Unversioned)
        );
        let garbage = json!({ "metadata": { "resourceVersion": "not-a-number" } });
        assert_eq!(
            ObservedNode::from_stored(garbage).err(),
            Some(Unobservable::Unversioned)
        );
    }
}
