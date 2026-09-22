//! `ApiServer` — the public lifecycle wrapper. Boots an Axum router on a
//! TCP listener, serves the K8s API surface, terminates gracefully.
//!
//! Two serve paths, selected by whether [`TlsMaterial`] is supplied:
//!
//!   * **TLS (the production default).** A rustls `ServerConfig` built from
//!     the issued server-cert chain + key. Real kubectl negotiates HTTPS
//!     against a `server: https://…` cluster and validates the presented
//!     cert against its SANs, so this is the load-bearing half of
//!     local-kubectl compatibility.
//!   * **Plaintext (tests + explicit dev opt-out).** For `tls.enabled =
//!     false`.
//!
//! Both run on [`engenho_serve::serve`], which OWNS every connection task.
//! A shutdown signals every connection, gives in-flight requests a bounded
//! 2s grace, then severs any still-open long-poll WATCH streams (a K8s
//! apiserver severs watches on shutdown; clients reconnect) — and awaits
//! the severed tasks, so when [`ApiServer::shutdown`] returns no connection
//! holds the router, and through it the store.
//!
//! That last clause is the reason for the owned loop. axum 0.7's `serve` and
//! axum-server 0.7 run connections in detached tasks: aborting the serve
//! task left a watching client's connection alive, holding every
//! `StoreBackedHandler`'s `Arc<StoreMesh>` for the life of the process
//! (measured 2026-09-19, `StoreStillShared { strong_count: 57 }`).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use engenho_serve::{Plain, ServeReport, StopHandle, Tls, stop_channel};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::handler::ResourceHandler;
use crate::pki::{PkiError, TlsMaterial};
use crate::router::{RouterState, build};

mod tls_acceptor;

/// How long graceful shutdown waits for in-flight requests to drain
/// before severing open long-poll watches.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

/// How long a TLS handshake may take before the connection is dropped.
/// The same bound axum-server applied by default.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("bind failed: {0}")]
    Bind(#[source] std::io::Error),
    #[error("serve failed: {0}")]
    Serve(#[source] std::io::Error),
    #[error("tls setup failed: {0}")]
    Tls(#[source] std::io::Error),
    #[error("pki error: {0}")]
    Pki(#[from] PkiError),
}

engenho_substrate::impl_error_kind! {
    ServerError {
        (Bind(_)) => "bind",
        (Serve(_)) => "serve",
        (Tls(_)) => "tls",
        (Pki(_)) => "pki",
    }
}

/// A bound, serving apiserver.
///
/// Dropping it without [`Self::shutdown`] also stops the server (the stop
/// handle goes with it), so a boot that fails after binding does not leave
/// an apiserver serving behind it — but only `shutdown` waits for the
/// connections to be gone.
pub struct ApiServer {
    addr: SocketAddr,
    task: JoinHandle<ServeReport>,
    stop: StopHandle,
}

impl ApiServer {
    /// Spawn the server on `addr` with the given handlers. When `tls` is
    /// `Some`, serve over HTTPS using the supplied cert chain + key;
    /// otherwise serve plaintext. Returns after the listener is bound;
    /// the server runs in a background tokio task.
    ///
    /// # Errors
    ///
    /// [`ServerError::Bind`] if the listener can't bind; [`ServerError::Tls`]
    /// if the rustls config can't be built from the supplied PEM.
    pub async fn start(
        addr: SocketAddr,
        handlers: Vec<Arc<dyn ResourceHandler>>,
        tls: Option<TlsMaterial>,
    ) -> Result<Self, ServerError> {
        let state = RouterState::new(handlers);
        Self::start_with_state(addr, state, tls).await
    }

    /// Spawn the server over a pre-built [`RouterState`]. Identical to
    /// [`Self::start`] except the caller owns the `RouterState` + can RETAIN
    /// a clone of it — the load-bearing seam for CRD serving: the runtime
    /// builds ONE `RouterState`, hands a clone here AND a clone to the
    /// `CrdController`'s [`crate::handler::RouterHandlerSink`], so a
    /// controller-driven `register()` mutates the SAME live `ArcSwap` table
    /// this server dispatches on (the swap is visible to in-flight requests).
    ///
    /// # Errors
    ///
    /// [`ServerError::Bind`] if the listener can't bind; [`ServerError::Tls`]
    /// if the rustls config can't be built from the supplied PEM.
    pub async fn start_with_state(
        addr: SocketAddr,
        state: RouterState,
        tls: Option<TlsMaterial>,
    ) -> Result<Self, ServerError> {
        let router: Router = build(state);

        match tls {
            Some(material) => Self::start_tls(addr, router, material).await,
            None => Self::start_plain(addr, router).await,
        }
    }

    /// Plaintext serve path (tls.enabled = false).
    async fn start_plain(addr: SocketAddr, router: Router) -> Result<Self, ServerError> {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(ServerError::Bind)?;
        let bound_addr = listener.local_addr().map_err(ServerError::Bind)?;

        let (stop_handle, stop) = stop_channel();
        let task = tokio::spawn(engenho_serve::serve(
            listener,
            Plain,
            move |_: &TcpStream, _: &SocketAddr| router.clone(),
            stop,
            SHUTDOWN_GRACE,
        ));

        Ok(Self {
            addr: bound_addr,
            task,
            stop: stop_handle,
        })
    }

    /// TLS serve path (the production default). Builds the rustls
    /// `ServerConfig` OURSELVES so the OPTIONAL client-cert verifier can be
    /// injected, then threads each connection's verified peer cert into its
    /// requests' extensions.
    ///
    /// When `material.client_verifier` is `Some`, the server REQUESTS a client
    /// cert (verified against the cluster CA) but `allow_unauthenticated` keeps
    /// the handshake completing for a no-cert client — so existing
    /// token/anonymous kubectl keeps connecting. After the handshake the
    /// leaf is read off the session ([`tls_acceptor::verified_client_cert`])
    /// and every request of the connection carries it as a
    /// [`crate::pki::VerifiedClientCert`] extension; the authn middleware's
    /// X509 stage reads it. When `None`, the server runs with no client auth
    /// (the pre-authn behavior).
    async fn start_tls(
        addr: SocketAddr,
        router: Router,
        material: TlsMaterial,
    ) -> Result<Self, ServerError> {
        // A single rustls crypto provider must be installed before any
        // `ServerConfig::builder()` runs. Both `ring` (via reqwest's
        // rustls-tls) and `aws-lc-rs` can be linked in the closure, so
        // pick `ring` explicitly + install it idempotently — without
        // this, `ServerConfig::builder()` panics with "no process-level
        // CryptoProvider" / "multiple providers". Ignoring the Err means
        // a provider is already installed (another component won the
        // race), which is fine.
        let _ = rustls::crypto::ring::default_provider().install_default();

        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(ServerError::Bind)?;
        let bound_addr = listener.local_addr().map_err(ServerError::Bind)?;

        let server_config = build_server_config(&material)?;
        let handshake = Tls::new(Arc::new(server_config), TLS_HANDSHAKE_TIMEOUT);

        let (stop_handle, stop) = stop_channel();
        let task = tokio::spawn(engenho_serve::serve(
            listener,
            handshake,
            move |stream: &tokio_rustls::server::TlsStream<TcpStream>, _: &SocketAddr| {
                tls_acceptor::CertInjectingService::new(
                    router.clone(),
                    tls_acceptor::verified_client_cert(stream),
                )
            },
            stop,
            SHUTDOWN_GRACE,
        ));

        Ok(Self {
            addr: bound_addr,
            task,
            stop: stop_handle,
        })
    }

    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Stop serving and wait until no connection is left.
    ///
    /// Signals every connection to shut down gracefully, waits a bounded
    /// grace for in-flight requests to drain, then severs what is still open.
    /// Long-poll WATCH streams never drain on their own (they stay open until
    /// the client disconnects), so a K8s apiserver SEVERS them on shutdown;
    /// clients reconnect. The severed connection tasks are awaited, not just
    /// aborted: when this returns, nothing a connection captured — the
    /// router, its handlers, their `Arc<StoreMesh>` — is still alive.
    ///
    /// # Errors
    ///
    /// Never returns `Err` today — kept fallible so a future serve path can
    /// surface a real shutdown failure without a signature change. A serve
    /// task that panicked is logged, not returned: the stop it was asked for
    /// has happened either way.
    pub async fn shutdown(self) -> Result<(), ServerError> {
        self.stop.stop();
        match self.task.await {
            Ok(report) => info!(
                open = report.open_at_stop,
                drained = report.drained,
                severed = report.severed,
                "apiserver stopped"
            ),
            Err(err) => error!(error = %err, "apiserver serve task ended abnormally"),
        }
        Ok(())
    }
}

/// Build the rustls [`rustls::ServerConfig`] from the issued cert chain + key,
/// installing the OPTIONAL client-cert verifier when present.
///
/// This is the minimal hand-built equivalent of `RustlsConfig::from_pem` — it
/// parses the SAME leaf chain + key DER, calls the SAME `with_single_cert`, and
/// sets the SAME ALPN protocols — diverging only in the client-auth posture:
///   * `Some(verifier)` → `with_client_cert_verifier` (OPTIONAL mTLS: requests
///     a cert, verifies it against the cluster CA, but `allow_unauthenticated`
///     keeps a no-cert client connecting).
///   * `None`           → `with_no_client_auth` (the pre-authn behavior).
fn build_server_config(material: &TlsMaterial) -> Result<rustls::ServerConfig, ServerError> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};

    let cert_chain: Vec<CertificateDer<'static>> = {
        let mut pem = material.cert_chain_pem.as_bytes();
        rustls_pemfile::certs(&mut pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                ServerError::Tls(std::io::Error::other(format!("parse cert chain PEM: {e}")))
            })?
    };
    if cert_chain.is_empty() {
        return Err(ServerError::Tls(std::io::Error::other(
            "no certificate found in cert_chain_pem",
        )));
    }

    let key: PrivateKeyDer<'static> = {
        let mut pem = material.key_pem.as_bytes();
        rustls_pemfile::private_key(&mut pem)
            .map_err(|e| {
                ServerError::Tls(std::io::Error::other(format!("parse server key PEM: {e}")))
            })?
            .ok_or_else(|| {
                ServerError::Tls(std::io::Error::other("no private key found in key_pem"))
            })?
    };

    let builder = rustls::ServerConfig::builder();
    let mut config = match &material.client_verifier {
        // OPTIONAL mTLS — requests a client cert, verifies against the CA,
        // allow_unauthenticated keeps no-cert clients connecting.
        Some(verifier) => builder.with_client_cert_verifier(verifier.clone()),
        // No client auth (the pre-authn floor).
        None => builder.with_no_client_auth(),
    }
    .with_single_cert(cert_chain, key)
    .map_err(|e| ServerError::Tls(std::io::Error::other(format!("with_single_cert: {e}"))))?;

    // Same ALPN as `RustlsConfig::from_pem` so HTTP/2 + HTTP/1.1 both work.
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(config)
}
