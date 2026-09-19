//! Fixtures shared by the scheduler's store-backed integration tests.
//!
//! The scheduler derives a Node's `Ready` from its Lease, so a node a test
//! means to be schedulable needs a heartbeat as well as a Node object.

use engenho_controllers::node_lease::{lease_key, lease_value};
use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand},
};

/// A `renewTime` far past the grace period: the node stopped heartbeating.
#[allow(dead_code)] // Each test binary compiles this module; not all use every item.
pub const STALE_RENEW_TIME: &str = "2020-01-01T00:00:00Z";

/// Write `node`'s Lease with the given `renewTime`.
pub async fn put_lease(store: &StoreMesh, node: &str, renew_time: &str) {
    store
        .propose(ResourceCommand::Put {
            key: lease_key(node),
            value: lease_value(node, renew_time, 0),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

/// Write `node`'s Lease renewed just now.
pub async fn put_fresh_lease(store: &StoreMesh, node: &str) {
    put_lease(store, node, &engenho_types::time::now_rfc3339_utc()).await;
}
