//! Tests for the GossipBroker trait + the FederationBroker adapter.

use engenho_fonte::{FederationBroker, GossipBroker};
use std::sync::Arc;

#[tokio::test]
async fn federation_broker_implements_gossip_broker() {
    let b = FederationBroker::new(32);
    let bx: Arc<dyn GossipBroker> = Arc::new(b);
    let r1 = bx
        .announce("rio".into(), r#"{"name":"rio"}"#.into())
        .await
        .unwrap();
    let r2 = bx
        .announce("rio".into(), r#"{"name":"rio2"}"#.into())
        .await
        .unwrap();
    assert!(r1 < r2);
}

#[tokio::test]
async fn gossip_broker_dyn_dispatch_works() {
    // The trait is dyn-compatible — operators can hold N transports
    // behind one shared Arc<dyn GossipBroker>.
    fn assert_dyn_compat(_: Arc<dyn GossipBroker>) {}
    let b: Arc<dyn GossipBroker> = Arc::new(FederationBroker::new(8));
    assert_dyn_compat(b);
}
