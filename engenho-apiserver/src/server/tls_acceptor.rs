//! Custom TLS acceptor that threads the verified peer client cert into each
//! request's extensions.
//!
//! axum-server's [`RustlsAcceptor`] produces a `tokio_rustls::server::TlsStream`
//! but does NOT expose `peer_certificates()` to the request pipeline (no
//! built-in `TlsConnectInfo`, no `with_connect_info` wiring). So the peer cert
//! is unreachable from a tower middleware on `into_make_service()` as-is.
//!
//! [`PeerCertAcceptor`] wraps `RustlsAcceptor`: after the inner accept yields
//! `(TlsStream, Service)` (handshake complete — and, for a presented cert,
//! ALREADY VERIFIED against the cluster CA by the rustls `ClientCertVerifier`),
//! it reads `tls_stream.get_ref().1.peer_certificates()`, parses the leaf into
//! a typed [`VerifiedClientCert`], and wraps the per-connection service in
//! [`CertInjectingService`] — which inserts the cert into every request's
//! extensions. The authn middleware's X509 stage then reads that extension.
//!
//! This is the rustls-blessed pattern: the cert is read ONLY after rustls has
//! verified it, so a `VerifiedClientCert` in the extensions is, by
//! construction, an authenticated identity (the verifier rejected any cert that
//! doesn't chain to the CA before the handshake completed). A no-cert client
//! (the `allow_unauthenticated` path) yields no extension — the middleware
//! falls through to the token/anonymous stages.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use axum_server::accept::Accept;
use axum_server::tls_rustls::{RustlsAcceptor, RustlsConfig};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;
use tower::Service;

use crate::pki::{VerifiedClientCert, parse_client_cert};

/// A TLS acceptor wrapping [`RustlsAcceptor`] that injects the verified peer
/// client cert into request extensions.
#[derive(Clone)]
pub struct PeerCertAcceptor {
    inner: RustlsAcceptor,
}

impl PeerCertAcceptor {
    /// New acceptor over the given rustls config.
    #[must_use]
    pub fn new(config: RustlsConfig) -> Self {
        Self {
            inner: RustlsAcceptor::new(config),
        }
    }
}

impl<S> Accept<TcpStream, S> for PeerCertAcceptor
where
    S: Send + 'static,
{
    type Stream = TlsStream<TcpStream>;
    type Service = CertInjectingService<S>;
    type Future = Pin<
        Box<
            dyn Future<Output = std::io::Result<(Self::Stream, Self::Service)>> + Send,
        >,
    >;

    fn accept(&self, stream: TcpStream, service: S) -> Self::Future {
        let inner = self.inner.clone();
        Box::pin(async move {
            // Run the standard rustls handshake. For a presented cert this has
            // ALREADY verified it against the cluster CA (the ClientCertVerifier
            // runs inside the handshake); for a no-cert client it completes via
            // allow_unauthenticated.
            let (tls_stream, service) = inner.accept(stream, service).await?;

            // Read the verified peer cert chain (None for a no-cert client).
            // `get_ref().1` is the rustls `ServerConnection`.
            let client_cert = tls_stream
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|chain| chain.first())
                .and_then(|leaf| parse_client_cert(leaf.as_ref()).ok().flatten());

            Ok((
                tls_stream,
                CertInjectingService {
                    inner: service,
                    client_cert,
                },
            ))
        })
    }
}

/// A per-connection service wrapper that inserts the connection's verified
/// peer cert (if any) into every request's extensions. The authn middleware's
/// X509 stage reads it as a [`VerifiedClientCert`].
#[derive(Clone)]
pub struct CertInjectingService<S> {
    inner: S,
    /// The verified peer cert for THIS connection (cloned into every request).
    /// `None` when the client presented no cert (the allow_unauthenticated
    /// path) — the middleware then falls through to token/anonymous.
    client_cert: Option<VerifiedClientCert>,
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
