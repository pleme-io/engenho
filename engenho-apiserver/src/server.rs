//! `ApiServer` — the public lifecycle wrapper. Boots an Axum server on a
//! TCP listener, serves the K8s API surface, terminates gracefully.
//!
//! Two serve paths, selected by whether [`TlsMaterial`] is supplied:
//!
//!   * **TLS (the production default).** `axum_server::bind_rustls` over
//!     a rustls `ServerConfig` built from the issued server-cert chain +
//!     key. Real kubectl negotiates HTTPS against a `server: https://…`
//!     cluster and validates the presented cert against its SANs, so this
//!     is the load-bearing half of local-kubectl compatibility.
//!   * **Plaintext (tests + explicit dev opt-out).** The original
//!     `axum::serve` path, kept for `tls.enabled = false`.
//!
//! Both paths preserve the same graceful-shutdown semantics: signal a
//! shutdown, give in-flight requests a bounded 2s grace, then sever any
//! still-open long-poll WATCH streams (a K8s apiserver severs watches on
//! shutdown; clients reconnect).

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum_server::Handle;
use tokio::task::JoinHandle;

use crate::handler::ResourceHandler;
use crate::pki::{PkiError, TlsMaterial};
use crate::router::{RouterState, build};

mod tls_acceptor;

/// How long graceful shutdown waits for in-flight requests to drain
/// before severing open long-poll watches.
const SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

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

/// How the server severs its background task on shutdown — one variant
/// per serve path so the two stay symmetric (signal-then-grace) without
/// duplicating the lifecycle logic.
enum ShutdownGate {
    /// Plaintext path: the `axum::serve` graceful-shutdown oneshot.
    Plain(tokio::sync::oneshot::Sender<()>),
    /// TLS path: `axum_server::Handle::graceful_shutdown`.
    Tls(Handle),
}

pub struct ApiServer {
    addr: SocketAddr,
    task: JoinHandle<Result<(), std::io::Error>>,
    gate: ShutdownGate,
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
    /// builds ONE RouterState, hands a clone here AND a clone to the
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

    /// Plaintext serve path (tls.enabled = false). The original
    /// `axum::serve` lifecycle.
    async fn start_plain(addr: SocketAddr, router: Router) -> Result<Self, ServerError> {
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
            gate: ShutdownGate::Plain(tx),
        })
    }

    /// TLS serve path (the production default). Builds the rustls
    /// `ServerConfig` OURSELVES (so the OPTIONAL client-cert verifier can be
    /// injected — `RustlsConfig::from_pem` builds a no-client-auth config and
    /// can't), then serves through a custom acceptor that threads the verified
    /// peer cert into request extensions.
    ///
    /// When `material.client_verifier` is `Some`, the server REQUESTS a client
    /// cert (verified against the cluster CA) but `allow_unauthenticated` keeps
    /// the handshake completing for a no-cert client — so existing
    /// token/anonymous kubectl keeps connecting. The custom acceptor reads
    /// `peer_certificates()` post-handshake, parses the leaf into a
    /// [`crate::pki::VerifiedClientCert`], and injects it as a request
    /// extension; the authn middleware's X509 stage reads it. When `None`, the
    /// server runs with no client auth (the pre-authn behavior).
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

        // Bind a std listener first so we can resolve the `:0` ephemeral
        // port BEFORE handing the socket to axum-server (which otherwise
        // hides the bound port behind its own accept loop).
        let std_listener = std::net::TcpListener::bind(addr).map_err(ServerError::Bind)?;
        std_listener
            .set_nonblocking(true)
            .map_err(ServerError::Bind)?;
        let bound_addr = std_listener.local_addr().map_err(ServerError::Bind)?;

        // Build the ServerConfig ourselves so we can inject the OPTIONAL client
        // verifier. The leaf chain + key are identical to the `from_pem` path
        // (same DER, same single-cert call) — the only change is the client
        // auth posture.
        let server_config = build_server_config(&material)?;
        let rustls_config =
            axum_server::tls_rustls::RustlsConfig::from_config(Arc::new(server_config));

        // Wrap the standard RustlsAcceptor so the verified peer cert is read
        // post-handshake + injected into each request's extensions.
        let acceptor = tls_acceptor::PeerCertAcceptor::new(rustls_config.clone());

        let handle = Handle::new();
        let serve_handle = handle.clone();
        let task = tokio::spawn(async move {
            axum_server::from_tcp(std_listener)
                .acceptor(acceptor)
                .handle(serve_handle)
                .serve(router.into_make_service())
                .await
        });

        Ok(Self {
            addr: bound_addr,
            task,
            gate: ShutdownGate::Tls(handle),
        })
    }

    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Initiate graceful shutdown + await the server task.
    ///
    /// Signals graceful shutdown, then waits a bounded grace for in-flight
    /// requests to drain. Long-poll WATCH streams never drain on their own
    /// (they stay open until the client disconnects), so a K8s apiserver
    /// SEVERS them on shutdown — after the grace elapses we abort the serve
    /// task, dropping any open watch connections. This is the correct
    /// production behavior (clients reconnect) AND keeps shutdown bounded
    /// instead of hanging forever on an open watch.
    ///
    /// # Errors
    ///
    /// Never returns `Err` today — kept fallible so a future serve path can
    /// surface a real shutdown failure without a signature change.
    pub async fn shutdown(self) -> Result<(), ServerError> {
        match self.gate {
            ShutdownGate::Plain(tx) => {
                let _ = tx.send(());
            }
            ShutdownGate::Tls(handle) => {
                // axum-server drains for the grace, then drops the rest.
                handle.graceful_shutdown(Some(SHUTDOWN_GRACE));
            }
        }
        let abort = self.task.abort_handle();
        if tokio::time::timeout(SHUTDOWN_GRACE, self.task).await.is_err() {
            // Open long-poll watches didn't drain within the grace —
            // sever them by aborting the serve task (no leaked task).
            abort.abort();
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
