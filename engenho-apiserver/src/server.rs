//! `ApiServer` — the public lifecycle wrapper. Boots an Axum
//! server on a TCP listener, serves the K8s API surface, terminates
//! gracefully.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use tokio::task::JoinHandle;

use crate::handler::ResourceHandler;
use crate::router::{RouterState, build};

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("bind failed: {0}")]
    Bind(#[source] std::io::Error),
    #[error("serve failed: {0}")]
    Serve(#[source] std::io::Error),
}

pub struct ApiServer {
    addr: SocketAddr,
    task: JoinHandle<Result<(), std::io::Error>>,
    shutdown: tokio::sync::oneshot::Sender<()>,
}

impl ApiServer {
    /// Spawn the server on `addr` with the given handlers. Returns
    /// after the listener is bound; the server runs in a background
    /// tokio task.
    pub async fn start(
        addr: SocketAddr,
        handlers: Vec<Arc<dyn ResourceHandler>>,
    ) -> Result<Self, ServerError> {
        let state = RouterState::new(handlers);
        let router: Router = build(state);

        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(ServerError::Bind)?;
        let bound_addr = listener.local_addr().map_err(ServerError::Bind)?;

        let (tx, rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = rx.await;
                })
                .await
        });

        Ok(Self {
            addr: bound_addr,
            task,
            shutdown: tx,
        })
    }

    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Initiate graceful shutdown + await the server task.
    pub async fn shutdown(self) -> Result<(), ServerError> {
        let _ = self.shutdown.send(());
        self.task.await.ok();
        Ok(())
    }
}
