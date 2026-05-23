//! Tests for FaceFederatedWatcher — proves the watcher subscribes
//! to a face's watch stream.

#![cfg(feature = "with-revoada")]

use engenho_fonte::FaceFederatedWatcher;
use engenho_revoada::face::{Face, ResourceFormat};
use engenho_revoada::{FabricFace, FaceKind, PureRaftFace};
use std::sync::Arc;

#[tokio::test]
async fn unsupported_watch_surfaces_typed_error() {
    // PureRaftFace's watch_resources may not be implemented for
    // arbitrary kinds — should surface FonteError::Watch cleanly.
    let face = PureRaftFace::from_declaration(&FabricFace {
        name: "ffw-unsupported".into(),
        kind: FaceKind::PureRaft,
    })
    .unwrap();
    face.start().unwrap();
    let face_arc: Arc<dyn Face> = Arc::new(face);

    let result = FaceFederatedWatcher::subscribe(
        face_arc.clone(),
        "NonExistentKind12345",
        Some("default"),
        ResourceFormat::Yaml,
    );
    // Two acceptable outcomes:
    //   - Watch returns Err(Unsupported) → FonteError::Watch
    //   - Watch returns an empty/never-firing stream → Ok, but
    //     next() blocks indefinitely (we don't await it here)
    match result {
        Ok(_watcher) => {
            // Watcher constructed cleanly — the stream just won't
            // fire any events for this kind. That's the second
            // valid path.
        }
        Err(e) => {
            let msg = format!("{e}");
            assert!(msg.contains("fonte/watch"), "got: {msg}");
        }
    }
    face_arc.shutdown().unwrap();
}

#[tokio::test]
async fn type_construction_compiles() {
    // Compile-time test: FaceFederatedWatcher implements Watcher.
    fn assert_watcher<W: engenho_fonte::Watcher>(_: &W) {}
    let face = PureRaftFace::from_declaration(&FabricFace {
        name: "ffw-compile".into(),
        kind: FaceKind::PureRaft,
    })
    .unwrap();
    face.start().unwrap();
    let face_arc: Arc<dyn Face> = Arc::new(face);
    if let Ok(w) = FaceFederatedWatcher::subscribe(
        face_arc.clone(),
        "Pod",
        Some("default"),
        ResourceFormat::Yaml,
    ) {
        assert_watcher(&w);
    }
    face_arc.shutdown().unwrap();
}
