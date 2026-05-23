//! Tests for the FaceGossipBroker — proves announce() lands as a
//! face resource (the face's resource_count reflects every peer's
//! announce).

#![cfg(feature = "with-revoada")]

use engenho_fonte::{FaceGossipBroker, GossipBroker};
use engenho_revoada::face::Face;
use engenho_revoada::{FabricFace, FaceKind, PureRaftFace};
use std::sync::Arc;

#[tokio::test]
async fn announce_lands_as_face_resource() {
    let face = PureRaftFace::from_declaration(&FabricFace {
        name: "fonte-gossip".into(),
        kind: FaceKind::PureRaft,
    })
    .unwrap();
    face.start().unwrap();
    let face_arc: Arc<dyn Face> = Arc::new(face);
    let broker: Arc<dyn GossipBroker> = Arc::new(FaceGossipBroker::new(face_arc.clone()));

    let r0 = broker
        .announce("rio".into(), r#"{"name":"rio"}"#.into())
        .await
        .unwrap();
    let r1 = broker
        .announce("sao".into(), r#"{"name":"sao"}"#.into())
        .await
        .unwrap();
    assert_eq!(r0, 0);
    assert_eq!(r1, 1);
    assert_eq!(face_arc.resource_count(), 2);

    face_arc.shutdown().unwrap();
}

#[tokio::test]
async fn dyn_gossip_broker_dispatch_works() {
    let face = PureRaftFace::from_declaration(&FabricFace {
        name: "fonte-dyn".into(),
        kind: FaceKind::PureRaft,
    })
    .unwrap();
    face.start().unwrap();
    let face_arc: Arc<dyn Face> = Arc::new(face);
    fn assert_dyn(_: Arc<dyn GossipBroker>) {}
    let b: Arc<dyn GossipBroker> = Arc::new(FaceGossipBroker::new(face_arc.clone()));
    assert_dyn(b);
    face_arc.shutdown().unwrap();
}
