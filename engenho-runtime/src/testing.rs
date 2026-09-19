//! Test-only helpers shared by this crate's unit tests.

use std::sync::Arc;
use std::time::Duration;

use engenho_store::StoreMesh;

/// An in-process, single-voter store that has reached leadership: the
/// runtime's store without the runtime.
pub(crate) async fn single_voter_store(name: &str) -> Arc<StoreMesh> {
    let router = engenho_store::InProcessRouter::new();
    let config = engenho_store::default_config(name).expect("the default store config builds");
    let mesh = StoreMesh::start(1, "in-process://1".into(), router, config)
        .await
        .expect("the store starts");
    mesh.initialize_singleton()
        .await
        .expect("the single voter initializes");
    assert!(
        mesh.wait_for_leadership(Duration::from_secs(5)).await,
        "the single voter leads"
    );
    Arc::new(mesh)
}
