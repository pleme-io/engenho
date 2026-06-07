//! Kubeconfig parsing — read a `~/.kube/config` YAML, resolve the
//! current context's auth + TLS, produce a [`Connection`].
//!
//! The shape mirrors upstream's `clientcmdapi.Config` exactly so any
//! kubeconfig written by `kubectl`, k3s, or our HM-sops template
//! parses without translation. Only the fields engenho needs at M0
//! are populated — extension points (preferences, deprecated fields)
//! are accepted-and-ignored.

use std::path::PathBuf;

use engenho_types::auth::{BytesOrPath, ExecEnv, KubeAuth, TokenSource};
use engenho_types::error::KubeError;
use serde::{Deserialize, Serialize};

use crate::connection::Connection;

/// Upstream `apiVersion: v1` kubeconfig document.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct Kubeconfig {
    /// Required: `v1`.
    #[serde(default, rename = "apiVersion")]
    pub api_version: String,
    /// Required: `Config`.
    #[serde(default)]
    pub kind: String,
    /// Active context name. Required for [`Self::resolve_connection`].
    #[serde(default, rename = "current-context")]
    pub current_context: String,
    /// Cluster entries (server URL + CA cert, per cluster).
    #[serde(default)]
    pub clusters: Vec<NamedCluster>,
    /// Auth-info entries (auth method, per user).
    #[serde(default)]
    pub users: Vec<NamedAuthInfo>,
    /// Contexts — bind a cluster + user + namespace.
    #[serde(default)]
    pub contexts: Vec<NamedContext>,
}

/// `clusters[].cluster` from upstream.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Cluster {
    /// API server URL, e.g. `https://192.168.64.10:6443`.
    pub server: String,
    /// Inline CA bundle (base64 PEM) — preferred over path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub certificate_authority_data: Option<String>,
    /// File path to a CA bundle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub certificate_authority: Option<PathBuf>,
    /// If true, skip TLS verification entirely (development clusters only).
    #[serde(default)]
    pub insecure_skip_tls_verify: bool,
}

/// `users[].user` from upstream.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct AuthInfo {
    /// Inline bearer token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// Path to a token file (SA token rotation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_file: Option<PathBuf>,
    /// Inline client cert (base64 PEM).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_certificate_data: Option<String>,
    /// Path to client cert PEM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_certificate: Option<PathBuf>,
    /// Inline client key (base64 PEM).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_key_data: Option<String>,
    /// Path to client key PEM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_key: Option<PathBuf>,
    /// Exec credential plugin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exec: Option<ExecConfig>,
}

/// Exec plugin section, mirrors `client.authentication.k8s.io/v1`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ExecConfig {
    /// Plugin binary path (or PATH-resolvable name).
    pub command: String,
    /// Args passed to the plugin.
    #[serde(default)]
    pub args: Vec<String>,
    /// Env vars set in the plugin's process.
    #[serde(default)]
    pub env: Vec<ExecEnv>,
    /// API version the plugin returns.
    #[serde(default, rename = "apiVersion")]
    pub api_version: String,
}

/// `contexts[].context` from upstream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Context {
    /// Name of the cluster entry this context refers to.
    pub cluster: String,
    /// Name of the user entry this context refers to.
    pub user: String,
    /// Default namespace for this context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Named wrapper for a cluster entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedCluster {
    /// Cluster entry name (looked up by `Context::cluster`).
    pub name: String,
    /// Cluster body.
    pub cluster: Cluster,
}

/// Named wrapper for an auth-info entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedAuthInfo {
    /// User entry name (looked up by `Context::user`).
    pub name: String,
    /// User body.
    pub user: AuthInfo,
}

/// Named wrapper for a context entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedContext {
    /// Context entry name (used by `current-context`).
    pub name: String,
    /// Context body.
    pub context: Context,
}

/// Emit a kubeconfig YAML for an engenho-served cluster.
///
/// Builds the typed [`Kubeconfig`] value and serializes it with
/// `serde_yaml` — a typed AST render, never a hand-rolled YAML string
/// (per the ★★ TYPED EMISSION rule). Because the emitter and the parser
/// ([`Kubeconfig::from_yaml`]) share ONE type, emit∘parse round-trips for
/// free.
///
/// **Anonymous over TLS — the M0.4 posture.** The emitted `user` is
/// `anonymous` with an all-`None` [`AuthInfo`], which
/// [`Kubeconfig::resolve_connection`] resolves to
/// [`KubeAuth::Anonymous`](engenho_types::auth::KubeAuth::Anonymous) — no
/// client credential is emitted. Authn/RBAC is explicitly out of scope at
/// M0.4 (the apiserver runs a FailOpen admission chain that admits
/// everything); a later brick adds bearer-token / client-cert auth and
/// this emitter grows the matching `user` block. TLS is still load-bearing:
/// kubectl verifies the server's presented cert against `ca_pem`.
///
/// * `cluster_name` — the `clusters[].name` + `current-context` value.
/// * `server_url`   — `https://<host>:<port>`; for a loopback engenho use
///   `https://127.0.0.1:<bound_port>` so the loopback SAN matches.
/// * `ca_pem`       — the cluster CA cert PEM (the SAME CA the server cert
///   chains to); embedded base64-encoded as `certificate-authority-data`.
///
/// # Errors
///
/// [`KubeError::Encode`] if serde_yaml can't serialize the value (does not
/// happen for the fixed shape built here, but the typed surface keeps the
/// fallible contract).
pub fn emit_kubeconfig(
    cluster_name: &str,
    server_url: &str,
    ca_pem: &[u8],
) -> Result<String, KubeError> {
    use base64::Engine as _;
    let ca_b64 = base64::engine::general_purpose::STANDARD.encode(ca_pem);

    let config = Kubeconfig {
        api_version: "v1".to_string(),
        kind: "Config".to_string(),
        current_context: cluster_name.to_string(),
        clusters: vec![NamedCluster {
            name: cluster_name.to_string(),
            cluster: Cluster {
                server: server_url.to_string(),
                certificate_authority_data: Some(ca_b64),
                certificate_authority: None,
                insecure_skip_tls_verify: false,
            },
        }],
        users: vec![NamedAuthInfo {
            name: "anonymous".to_string(),
            // All-None AuthInfo → resolve_connection yields KubeAuth::Anonymous.
            user: AuthInfo::default(),
        }],
        contexts: vec![NamedContext {
            name: cluster_name.to_string(),
            context: Context {
                cluster: cluster_name.to_string(),
                user: "anonymous".to_string(),
                namespace: Some("default".to_string()),
            },
        }],
    };

    serde_yaml::to_string(&config)
        .map_err(|e| KubeError::Encode(format!("kubeconfig emit: {e}")))
}

impl Kubeconfig {
    /// Parse YAML.
    ///
    /// # Errors
    ///
    /// Returns [`KubeError::Decode`] if the YAML is malformed.
    pub fn from_yaml(s: &str) -> Result<Self, KubeError> {
        serde_yaml::from_str(s).map_err(|e| KubeError::Decode(format!("kubeconfig parse: {e}")))
    }

    /// Read + parse from disk.
    ///
    /// # Errors
    ///
    /// [`KubeError::Auth`] when the file can't be read,
    /// [`KubeError::Decode`] when YAML parse fails.
    pub fn load(path: &std::path::Path) -> Result<Self, KubeError> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| KubeError::Auth(format!("read {}: {e}", path.display())))?;
        Self::from_yaml(&raw)
    }

    /// Build a [`Connection`] from the current context.
    ///
    /// # Errors
    ///
    /// [`KubeError::Auth`] if the current context names a cluster /
    /// user that isn't in the document, or the auth method can't be
    /// resolved.
    pub fn resolve_connection(&self) -> Result<Connection, KubeError> {
        let ctx = self
            .contexts
            .iter()
            .find(|c| c.name == self.current_context)
            .ok_or_else(|| {
                KubeError::Auth(format!(
                    "current-context {:?} not in `contexts:`",
                    self.current_context
                ))
            })?;
        let cluster = self
            .clusters
            .iter()
            .find(|c| c.name == ctx.context.cluster)
            .ok_or_else(|| {
                KubeError::Auth(format!(
                    "cluster {:?} not in `clusters:`",
                    ctx.context.cluster
                ))
            })?;
        let user = self
            .users
            .iter()
            .find(|u| u.name == ctx.context.user)
            .ok_or_else(|| {
                KubeError::Auth(format!("user {:?} not in `users:`", ctx.context.user))
            })?;

        let auth = auth_from_user(&user.user)?;
        let ca = ca_from_cluster(&cluster.cluster)?;
        Connection::new(&cluster.cluster.server, auth, ca.as_deref())
    }
}

fn auth_from_user(u: &AuthInfo) -> Result<KubeAuth, KubeError> {
    if let Some(t) = &u.token {
        return Ok(KubeAuth::BearerToken(TokenSource::Inline {
            token: t.clone(),
        }));
    }
    if let Some(p) = &u.token_file {
        return Ok(KubeAuth::BearerToken(TokenSource::File { path: p.clone() }));
    }
    let cert_src = bytes_or_path(
        "client-certificate",
        &u.client_certificate_data,
        &u.client_certificate,
    )?;
    let key_src = bytes_or_path("client-key", &u.client_key_data, &u.client_key)?;
    if let (Some(cert), Some(key)) = (cert_src, key_src) {
        return Ok(KubeAuth::ClientCert { cert, key });
    }
    if let Some(e) = &u.exec {
        return Ok(KubeAuth::Exec {
            command: e.command.clone(),
            args: e.args.clone(),
            env: e.env.clone(),
            api_version: e.api_version.clone(),
        });
    }
    Ok(KubeAuth::Anonymous)
}

fn bytes_or_path(
    field: &str,
    inline_b64: &Option<String>,
    path: &Option<PathBuf>,
) -> Result<Option<BytesOrPath>, KubeError> {
    if let Some(d) = inline_b64 {
        use base64::Engine as _;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(d)
            .map_err(|e| KubeError::Auth(format!("base64-decode {field}: {e}")))?;
        // BytesOrPath::Inline holds a String (the PEM text), so we
        // expect the inline content to be valid UTF-8 PEM bytes.
        let pem = String::from_utf8(decoded)
            .map_err(|e| KubeError::Auth(format!("{field} not UTF-8 PEM: {e}")))?;
        return Ok(Some(BytesOrPath::Inline { data: pem }));
    }
    if let Some(p) = path {
        return Ok(Some(BytesOrPath::Path { path: p.clone() }));
    }
    Ok(None)
}

fn ca_from_cluster(c: &Cluster) -> Result<Option<Vec<u8>>, KubeError> {
    if let Some(d) = &c.certificate_authority_data {
        use base64::Engine as _;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(d)
            .map_err(|e| {
                KubeError::Auth(format!("base64-decode certificate-authority-data: {e}"))
            })?;
        return Ok(Some(decoded));
    }
    if let Some(p) = &c.certificate_authority {
        let bytes = std::fs::read(p)
            .map_err(|e| KubeError::Auth(format!("read CA {}: {e}", p.display())))?;
        return Ok(Some(bytes));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_kubeconfig() {
        let yaml = r#"
apiVersion: v1
kind: Config
current-context: foo
clusters:
  - name: c1
    cluster:
      server: https://api.example.com
      insecure-skip-tls-verify: true
users:
  - name: u1
    user:
      token: secret-token-value
contexts:
  - name: foo
    context:
      cluster: c1
      user: u1
      namespace: default
"#;
        let kc = Kubeconfig::from_yaml(yaml).unwrap();
        assert_eq!(kc.current_context, "foo");
        assert_eq!(kc.clusters.len(), 1);
        assert_eq!(kc.users.len(), 1);
    }

    #[test]
    fn resolves_bearer_token() {
        let yaml = r#"
apiVersion: v1
kind: Config
current-context: foo
clusters:
  - name: c1
    cluster:
      server: https://api.example.com
users:
  - name: u1
    user:
      token: secret-token-value
contexts:
  - name: foo
    context:
      cluster: c1
      user: u1
"#;
        let kc = Kubeconfig::from_yaml(yaml).unwrap();
        let conn = kc.resolve_connection().unwrap();
        assert_eq!(conn.server(), "https://api.example.com");
        assert_eq!(
            conn.bearer_token()
                .unwrap()
                .map(|r| r.expose_secret().clone()),
            Some("secret-token-value".to_string())
        );
    }

    #[test]
    fn missing_current_context_errors() {
        let yaml = r#"
apiVersion: v1
kind: Config
current-context: nonexistent
clusters: []
users: []
contexts: []
"#;
        let kc = Kubeconfig::from_yaml(yaml).unwrap();
        let r = kc.resolve_connection();
        assert!(matches!(r, Err(KubeError::Auth(_))));
    }

    // ── emit_kubeconfig round-trip + anonymous-posture tests ───────────

    const SAMPLE_CA_PEM: &[u8] =
        b"-----BEGIN CERTIFICATE-----\nMIIBdummyCAcontent==\n-----END CERTIFICATE-----\n";

    #[test]
    fn emit_kubeconfig_round_trips_through_parser() {
        use base64::Engine as _;
        let yaml = emit_kubeconfig("engenho-local", "https://127.0.0.1:6443", SAMPLE_CA_PEM)
            .expect("emit");
        // Parse it BACK with the shared type — emit∘parse identity on the
        // fields we set.
        let kc = Kubeconfig::from_yaml(&yaml).expect("parse emitted kubeconfig");

        assert_eq!(kc.api_version, "v1");
        assert_eq!(kc.kind, "Config");
        assert_eq!(kc.current_context, "engenho-local");
        assert_eq!(kc.clusters.len(), 1);
        let cluster = &kc.clusters[0].cluster;
        assert!(
            cluster.server.starts_with("https://"),
            "server must be https; got {}",
            cluster.server
        );
        assert_eq!(cluster.server, "https://127.0.0.1:6443");
        // certificate-authority-data decodes back to the input PEM.
        let cad = cluster
            .certificate_authority_data
            .as_ref()
            .expect("certificate-authority-data present");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(cad)
            .expect("CA data is valid base64");
        assert_eq!(decoded, SAMPLE_CA_PEM);
        // not insecure.
        assert!(!cluster.insecure_skip_tls_verify);
    }

    #[test]
    fn emitted_kubeconfig_resolves_to_anonymous() {
        // The anonymous `user` block resolves to KubeAuth::Anonymous —
        // proven directly via `auth_from_user` (the auth resolver the
        // apiserver's eventual authn will key on). The full
        // `resolve_connection` (which builds a verifying reqwest Client and
        // needs a REAL CA cert) is exercised end-to-end by the apiserver's
        // m0_4 TLS integration test, not here.
        let yaml =
            emit_kubeconfig("c", "https://127.0.0.1:6443", SAMPLE_CA_PEM).expect("emit");
        let kc = Kubeconfig::from_yaml(&yaml).expect("parse");
        let resolved = auth_from_user(&kc.users[0].user).expect("resolve auth");
        assert!(
            matches!(resolved, KubeAuth::Anonymous),
            "M0.4 posture is anonymous-over-TLS; got {resolved:?}"
        );
    }

    #[test]
    fn emitted_kubeconfig_has_no_client_credential() {
        // The anonymous posture: NO token, NO client cert/key anywhere.
        let yaml =
            emit_kubeconfig("c", "https://127.0.0.1:6443", SAMPLE_CA_PEM).expect("emit");
        let kc = Kubeconfig::from_yaml(&yaml).expect("parse");
        assert_eq!(kc.users.len(), 1);
        let u = &kc.users[0].user;
        assert!(u.token.is_none(), "no bearer token in anonymous posture");
        assert!(u.token_file.is_none());
        assert!(
            u.client_certificate_data.is_none() && u.client_certificate.is_none(),
            "no client cert in anonymous posture"
        );
        assert!(
            u.client_key_data.is_none() && u.client_key.is_none(),
            "no client key in anonymous posture"
        );
        assert!(u.exec.is_none());
    }

    #[test]
    fn emitted_kubeconfig_is_valid_yaml() {
        // serde_yaml round-trips the emitted document (no structural junk).
        let yaml =
            emit_kubeconfig("c", "https://127.0.0.1:6443", SAMPLE_CA_PEM).expect("emit");
        let _v: serde_yaml::Value =
            serde_yaml::from_str(&yaml).expect("emitted kubeconfig is valid YAML");
    }
}
