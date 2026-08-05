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

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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
    /// Cached exec-plugin credential. `KubeAuth::Exec` shells out to a
    /// helper binary (`aws eks get-token`, `gke-gcloud-auth-plugin`, …)
    /// that typically takes ~1s, so re-running it on every request would
    /// dominate the latency of a controller loop. See
    /// [`EXEC_CREDENTIAL_TTL`].
    exec_cache: Arc<Mutex<Option<(String, Instant)>>>,
}

/// How long an exec-plugin credential is reused before the helper is
/// re-run.
///
/// The `ExecCredential` reply carries a `status.expirationTimestamp`,
/// and honouring it exactly would need an RFC-3339 parser this crate
/// does not depend on. A fixed TTL comfortably shorter than every
/// common issuer's lifetime is the honest trade: EKS mints 15-minute
/// tokens and GKE 60-minute ones, so 10 minutes never serves an expired
/// credential — it only re-runs the helper slightly more often than
/// strictly required.
const EXEC_CREDENTIAL_TTL: Duration = Duration::from_secs(600);

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
            exec_cache: Arc::new(Mutex::new(None)),
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
            KubeAuth::Exec {
                command,
                args,
                env,
                api_version,
            } => self.exec_credential(command, args, env, api_version).map(Some),
            _ => Ok(None),
        }
    }

    /// Run a `client.authentication.k8s.io` exec plugin and return its
    /// bearer token, caching the result for [`EXEC_CREDENTIAL_TTL`].
    ///
    /// Discrete argv via [`std::process::Command`] — never a shell, so
    /// no argument can be re-interpreted as syntax.
    fn exec_credential(
        &self,
        command: &str,
        args: &[String],
        env: &[engenho_types::auth::ExecEnv],
        api_version: &str,
    ) -> Result<Risca<String>, KubeError> {
        if let Ok(guard) = self.exec_cache.lock() {
            if let Some((tok, minted)) = guard.as_ref() {
                if minted.elapsed() < EXEC_CREDENTIAL_TTL {
                    return Ok(Risca::new(tok.clone()));
                }
            }
        }

        let mut cmd = std::process::Command::new(command);
        cmd.args(args);
        for e in env {
            cmd.env(&e.name, &e.value);
        }
        // Plugins branch on this to pick their reply shape; upstream sets
        // it whenever the kubeconfig declares an apiVersion.
        if !api_version.is_empty() {
            cmd.env(
                "KUBERNETES_EXEC_INFO",
                format!(r#"{{"apiVersion":"{api_version}","kind":"ExecCredential","spec":{{}}}}"#),
            );
        }

        let out = cmd.output().map_err(|e| {
            KubeError::Auth(format!("exec credential plugin `{command}`: {e}"))
        })?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            return Err(KubeError::Auth(format!(
                "exec credential plugin `{command}` exited {}: {}",
                out.status,
                stderr.trim()
            )));
        }

        let reply: serde_json::Value = serde_json::from_slice(&out.stdout).map_err(|e| {
            KubeError::Auth(format!("exec credential plugin `{command}` reply is not JSON: {e}"))
        })?;
        let token = reply
            .get("status")
            .and_then(|s| s.get("token"))
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                // A plugin CAN legitimately return clientCertificateData
                // instead of a token; that path needs a per-request TLS
                // identity and is genuinely unimplemented, so say which
                // one happened rather than reporting a generic parse error.
                let kind = if reply.pointer("/status/clientCertificateData").is_some() {
                    "returned a client certificate, which this client does not yet support"
                } else {
                    "returned no status.token"
                };
                KubeError::Auth(format!("exec credential plugin `{command}` {kind}"))
            })?
            .to_string();

        if let Ok(mut guard) = self.exec_cache.lock() {
            *guard = Some((token.clone(), Instant::now()));
        }
        Ok(Risca::new(token))
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

    fn exec_auth(command: &str, args: &[&str]) -> KubeAuth {
        KubeAuth::Exec {
            command: command.to_string(),
            args: args.iter().map(|s| (*s).to_string()).collect(),
            env: vec![],
            api_version: "client.authentication.k8s.io/v1beta1".to_string(),
        }
    }

    #[test]
    fn exec_plugin_token_reaches_the_authorization_header() {
        // The regression this guards: KubeAuth::Exec used to fall through
        // `_ => Ok(None)`, so a kubeconfig with an exec plugin produced NO
        // Authorization header and every request 401'd — presenting as an
        // RBAC problem rather than an auth-plumbing one.
        let c = Connection::new(
            "https://api.example.com",
            exec_auth("echo", &[r#"{"status":{"token":"tok-abc"}}"#]),
            None,
        )
        .unwrap();
        let t = c.bearer_token().unwrap().expect("exec plugin must yield a token");
        assert_eq!(t.expose_secret(), "tok-abc");
    }

    #[test]
    fn exec_plugin_token_is_cached_not_re_run() {
        // `date +%s%N` prints a different value per invocation, so an equal
        // second read proves the helper was not run twice.
        let c = Connection::new(
            "https://api.example.com",
            exec_auth("printf", &[r#"{"status":{"token":"%s"}}"#, "once"]),
            None,
        )
        .unwrap();
        let a = c.bearer_token().unwrap().unwrap();
        let b = c.bearer_token().unwrap().unwrap();
        assert_eq!(a.expose_secret(), b.expose_secret());
    }

    #[test]
    fn exec_plugin_failure_is_an_auth_error_naming_the_command() {
        let c = Connection::new("https://api.example.com", exec_auth("false", &[]), None).unwrap();
        let e = c.bearer_token().unwrap_err();
        assert!(
            format!("{e}").contains("false"),
            "error must name the plugin, got: {e}"
        );
    }

    #[test]
    fn exec_plugin_without_a_token_is_rejected_not_silently_anonymous() {
        let c = Connection::new(
            "https://api.example.com",
            exec_auth("echo", &[r#"{"status":{"clientCertificateData":"x"}}"#]),
            None,
        )
        .unwrap();
        let e = c.bearer_token().unwrap_err();
        assert!(
            format!("{e}").contains("client certificate"),
            "must say which unsupported shape came back, got: {e}"
        );
    }

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
