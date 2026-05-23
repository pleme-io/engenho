//! `Connection` — the shared HTTP/TLS state every request and watch
//! reuses.
//!
//! A [`Connection`] is constructed once per cluster (from a
//! [`Kubeconfig`](crate::config::Kubeconfig) or directly) and then
//! shared across [`ReqwestKubeClient`](crate::ReqwestKubeClient) +
//! [`ReqwestWatcher`](crate::ReqwestWatcher) instances. It carries:
//!
//!   * the API server URL,
//!   * the (cached) reqwest::Client with TLS + client-cert preloaded,
//!   * the resolved [`KubeAuth`] (re-read every request when it's a
//!     file-backed bearer token so SA-token rotation just works).

use std::sync::Arc;

use engenho_substrate::Risca;
use engenho_types::auth::{BytesOrPath, KubeAuth, TokenSource};
use engenho_types::error::KubeError;
use reqwest::Client;

/// Shared connection state for one cluster.
#[derive(Clone)]
pub struct Connection {
    /// Base URL of the apiserver, e.g. `https://192.168.64.10:6443`.
    /// Trailing slash stripped.
    server: Arc<str>,
    /// reqwest client with rustls + (optional) client-cert preloaded.
    http: Client,
    /// Auth method. Resolved per-request when it's a file-backed source.
    auth: Arc<KubeAuth>,
}

impl Connection {
    /// Construct a connection.
    ///
    /// # Errors
    ///
    /// Returns [`KubeError::Auth`] if client-cert decoding fails;
    /// [`KubeError::Network`] if the reqwest builder rejects the
    /// TLS config (e.g. invalid CA chain).
    pub fn new(server: &str, auth: KubeAuth, server_ca: Option<&[u8]>) -> Result<Self, KubeError> {
        let mut b = Client::builder().use_rustls_tls();
        if let Some(ca) = server_ca {
            let cert = reqwest::Certificate::from_pem(ca)
                .map_err(|e| KubeError::Auth(format!("server CA parse: {e}")))?;
            b = b.add_root_certificate(cert);
        }
        if let KubeAuth::ClientCert { cert, key } = &auth {
            let cert_bytes = resolve(cert)?;
            let key_bytes = resolve(key)?;
            let mut bundle = cert_bytes.clone();
            bundle.extend_from_slice(&key_bytes);
            let identity = reqwest::Identity::from_pem(&bundle)
                .map_err(|e| KubeError::Auth(format!("client identity: {e}")))?;
            b = b.identity(identity);
        }
        let http = b
            .build()
            .map_err(|e| KubeError::Network(format!("reqwest builder: {e}")))?;
        let server = server.trim_end_matches('/').into();
        Ok(Self {
            server,
            http,
            auth: Arc::new(auth),
        })
    }

    /// Base URL (without trailing slash).
    #[must_use]
    pub fn server(&self) -> &str {
        &self.server
    }

    /// reqwest client clone (cheap; reqwest::Client is internally Arc).
    #[must_use]
    pub fn http(&self) -> Client {
        self.http.clone()
    }

    /// Resolve the bearer token if applicable. Re-reads files each
    /// call so rotated SA tokens are picked up.
    ///
    /// The returned token is wrapped in [`Risca`] — accidental
    /// leakage via `Debug` / `Display` / `Serialize` is impossible
    /// at the type level. The single legitimate exposure path is
    /// `.expose_secret()` (used internally by [`Self::auth_header`]
    /// when handing the value to reqwest).
    ///
    /// # Errors
    ///
    /// Returns [`KubeError::Auth`] when a file-backed token can't be read.
    pub fn bearer_token(&self) -> Result<Option<Risca<String>>, KubeError> {
        match &*self.auth {
            KubeAuth::BearerToken(TokenSource::Inline { token }) => {
                Ok(Some(Risca::new(token.clone())))
            }
            KubeAuth::BearerToken(TokenSource::File { path }) => {
                let raw = std::fs::read_to_string(path).map_err(|e| {
                    KubeError::Auth(format!("read token file {}: {e}", path.display()))
                })?;
                Ok(Some(Risca::new(raw.trim().to_string())))
            }
            _ => Ok(None),
        }
    }

    /// Apply `Authorization: Bearer …` if applicable. The single
    /// substrate-blessed exposure site for the bearer token —
    /// `.expose_secret()` is called inline to hand the value to
    /// reqwest's `bearer_auth`.
    ///
    /// # Errors
    ///
    /// Same as [`Self::bearer_token`].
    pub fn auth_header(
        &self,
        mut rb: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, KubeError> {
        if let Some(t) = self.bearer_token()? {
            rb = rb.bearer_auth(t.expose_secret());
        }
        Ok(rb)
    }
}

fn resolve(b: &BytesOrPath) -> Result<Vec<u8>, KubeError> {
    match b {
        BytesOrPath::Inline { data } => Ok(data.as_bytes().to_vec()),
        BytesOrPath::Path { path } => std::fs::read(path)
            .map_err(|e| KubeError::Auth(format!("read {}: {e}", path.display()))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_strip_trailing_slash() {
        let c = Connection::new("https://api.example.com/", KubeAuth::Anonymous, None).unwrap();
        assert_eq!(c.server(), "https://api.example.com");
    }

    #[test]
    fn anonymous_has_no_bearer_token() {
        let c = Connection::new("https://api.example.com", KubeAuth::Anonymous, None).unwrap();
        assert_eq!(c.bearer_token().unwrap(), None);
    }

    #[test]
    fn inline_bearer_token_returns_value() {
        let c = Connection::new(
            "https://api.example.com",
            KubeAuth::BearerToken(TokenSource::Inline {
                token: "abc123".into(),
            }),
            None,
        )
        .unwrap();
        assert_eq!(
            c.bearer_token().unwrap().map(|r| r.expose_secret().clone()),
            Some("abc123".to_string())
        );
    }

    #[test]
    fn file_bearer_token_strips_trailing_newline() {
        let tmp = tempfile_path("token-test");
        std::fs::write(&tmp, "secret-token\n").unwrap();
        let c = Connection::new(
            "https://api.example.com",
            KubeAuth::BearerToken(TokenSource::File { path: tmp.clone() }),
            None,
        )
        .unwrap();
        assert_eq!(
            c.bearer_token().unwrap().map(|r| r.expose_secret().clone()),
            Some("secret-token".to_string())
        );
        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn bearer_token_debug_output_does_not_leak_inner() {
        // The Risca<String> wrapper makes leakage via Debug impossible
        // at the type level. Routed through assert_risca_no_leak! from
        // engenho-substrate — the canonical fleet-wide leak-proof check.
        let secret = "super-secret-bearer-9f3a";
        let c = Connection::new(
            "https://api.example.com",
            KubeAuth::BearerToken(TokenSource::Inline {
                token: secret.into(),
            }),
            None,
        )
        .unwrap();
        let t = c.bearer_token().unwrap();
        engenho_substrate::assert_risca_no_leak!(t, secret);
    }

    #[test]
    fn file_bearer_token_missing_path_errors() {
        let c = Connection::new(
            "https://api.example.com",
            KubeAuth::BearerToken(TokenSource::File {
                path: "/nonexistent".into(),
            }),
            None,
        )
        .unwrap();
        let r = c.bearer_token();
        assert!(matches!(r, Err(KubeError::Auth(_))));
    }

    fn tempfile_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        p.push(format!("engenho-{name}-{nanos}"));
        p
    }
}
