//! Threads the verified peer client cert into each request's extensions.
//!
//! After [`engenho_serve::Tls`] completes a handshake (and, for a presented
//! cert, rustls's `ClientCertVerifier` has ALREADY VERIFIED it against the
//! cluster CA), [`verified_client_cert`] reads the leaf off the session and
//! parses it into a typed [`VerifiedClientCert`]. The connection's service is
//! wrapped in [`CertInjectingService`], which inserts that cert into every
//! request's extensions; the authn middleware's X509 stage reads it.
//!
//! This is the rustls-blessed pattern: the cert is read ONLY after rustls has
//! verified it, so a `VerifiedClientCert` in the extensions is, by
//! construction, an authenticated identity (the verifier rejected any cert that
//! doesn't chain to the CA before the handshake completed). A no-cert client
//! (the `allow_unauthenticated` path) yields no extension — the middleware
//! falls through to the token/anonymous stages.

use std::task::{Context, Poll};

use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;
use tower::Service;

use crate::pki::{VerifiedClientCert, parse_client_cert};

/// The verified client cert of a finished TLS session, if the peer
/// presented one. `get_ref().1` is the rustls `ServerConnection`.
#[must_use]
pub fn verified_client_cert(stream: &TlsStream<TcpStream>) -> Option<VerifiedClientCert> {
    stream
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|chain| chain.first())
        .and_then(|leaf| parse_client_cert(leaf.as_ref()).ok().flatten())
}

/// A per-connection service wrapper that inserts the connection's verified
/// peer cert (if any) into every request's extensions. The authn middleware's
/// X509 stage reads it as a [`VerifiedClientCert`].
#[derive(Clone)]
pub struct CertInjectingService<S> {
    inner: S,
    /// The verified peer cert for THIS connection (cloned into every request).
    /// `None` when the client presented no cert (the `allow_unauthenticated`
    /// path) — the middleware then falls through to token/anonymous.
    client_cert: Option<VerifiedClientCert>,
}

impl<S> CertInjectingService<S> {
    /// Wrap `inner` so every request carries `client_cert`.
    pub fn new(inner: S, client_cert: Option<VerifiedClientCert>) -> Self {
        Self { inner, client_cert }
    }
}

impl<S, B> Service<axum::http::Request<B>> for CertInjectingService<S>
where
    S: Service<axum::http::Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: axum::http::Request<B>) -> Self::Future {
        if let Some(cert) = &self.client_cert {
            req.extensions_mut().insert(cert.clone());
        }
        self.inner.call(req)
    }
}
