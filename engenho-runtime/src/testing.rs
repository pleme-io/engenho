//! Test-only helpers shared by this crate's unit tests.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use engenho_store::StoreMesh;

use crate::runtime_health::{Relist, RelistFault, Relisted};

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

/// What a [`ScriptedRuntime`] answers a relist with.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) enum RelistAnswer {
    /// A list of [`ScriptedRuntime::CONTAINERS`] containers.
    #[default]
    Lists,
    /// A refused connection.
    Fails,
    /// A list, after the duration.
    Slowly(Duration),
    /// Nothing, ever.
    Hangs,
}

/// A container runtime a test scripts: each relist answers what the runtime
/// is set to when the relist begins.
#[derive(Debug, Default)]
pub(crate) struct ScriptedRuntime {
    answer: Mutex<RelistAnswer>,
}

impl ScriptedRuntime {
    /// How many containers a relist that answers lists.
    pub(crate) const CONTAINERS: usize = 3;

    /// Answer every relist from now on with `answer`.
    pub(crate) fn set(&self, answer: RelistAnswer) {
        *self.answer.lock().unwrap_or_else(PoisonError::into_inner) = answer;
    }
}

#[async_trait::async_trait]
impl Relist for ScriptedRuntime {
    async fn relist(&self) -> Result<Relisted, RelistFault> {
        let answer = *self.answer.lock().unwrap_or_else(PoisonError::into_inner);
        match answer {
            RelistAnswer::Lists => Ok(Relisted::new(Self::CONTAINERS)),
            RelistAnswer::Fails => Err(RelistFault::new(std::io::Error::other(
                "podman.sock: connection refused",
            ))),
            RelistAnswer::Slowly(after) => {
                tokio::time::sleep(after).await;
                Ok(Relisted::new(Self::CONTAINERS))
            }
            RelistAnswer::Hangs => std::future::pending().await,
        }
    }
}
