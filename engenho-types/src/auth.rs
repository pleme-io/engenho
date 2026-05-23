//! `KubeAuth` — the typed surface every connection negotiates against.
//!
//! Models the four auth methods upstream `client-go` supports:
//! anonymous, bearer token, mTLS (client cert + key), and an
//! exec-plugin (`kubectl --user` style — runs a credential helper).
//!
//! Each variant carries only the *configuration*, not the resolved
//! material. Resolution (e.g. running an exec plugin, reading a
//! certificate file) is the implementation's responsibility — the
//! typed surface stays pure.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Auth configuration for a `KubeClient`. Construct via
/// [`Self::from_kubeconfig`] (parsing) or one of the explicit
/// constructors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum KubeAuth {
    /// No credentials. Most clusters will reject this; used by some
    /// readiness probes + by the apiserver's own discovery endpoints.
    Anonymous,

    /// Bearer-token in the `Authorization: Bearer …` header. The
    /// token may be inline (`Inline`) or loaded from a file at every
    /// request (`File` — for ServiceAccount tokens that rotate).
    BearerToken(TokenSource),

    /// Mutual TLS — client cert + private key. The CA the apiserver
    /// presents is verified against [`KubeAuth::server_ca`] of the
    /// containing connection (separate field, not in this enum).
    ClientCert {
        /// PEM-encoded x509 certificate (inline) or path.
        cert: BytesOrPath,
        /// PEM-encoded private key (inline) or path.
        key: BytesOrPath,
    },

    /// Run an external binary that produces a credential (token /
    /// cert chain) per `ExecCredential` protocol. Mirrors
    /// `client.authentication.k8s.io/v1.ExecConfig`.
    Exec {
        /// Path or PATH-resolvable name of the helper binary.
        command: String,
        /// Args passed to the helper.
        args: Vec<String>,
        /// Env vars to set in the helper's process.
        env: Vec<ExecEnv>,
        /// API version the helper returns (`client.authentication.k8s.io/v1`).
        api_version: String,
    },
}

/// Inline vs file source for a bearer token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "from", rename_all = "kebab-case")]
pub enum TokenSource {
    /// The token bytes inline.
    Inline {
        /// Bearer token (sent as `Authorization: Bearer <token>`).
        token: String,
    },
    /// Path to a file containing the token. Re-read per request so
    /// rotated ServiceAccount tokens pick up automatically (matches
    /// upstream client-go behavior).
    File {
        /// Filesystem path holding the token (one line, no trailing newline).
        path: PathBuf,
    },
}

/// Either inline bytes or a path on disk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BytesOrPath {
    /// Path to a file. Resolved at connection time.
    Path { path: PathBuf },
    /// Inline content (typically base64-encoded PEM, the kubeconfig
    /// convention).
    Inline { data: String },
}

/// Env var passed to the exec credential helper.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecEnv {
    /// Variable name.
    pub name: String,
    /// Variable value.
    pub value: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anonymous_is_yaml_safe() {
        let v = KubeAuth::Anonymous;
        let s = serde_yaml::to_string(&v).unwrap();
        assert!(s.contains("kind: anonymous"));
        let back: KubeAuth = serde_yaml::from_str(&s).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn bearer_inline_round_trips() {
        let v = KubeAuth::BearerToken(TokenSource::Inline {
            token: "abc".into(),
        });
        let s = serde_yaml::to_string(&v).unwrap();
        let back: KubeAuth = serde_yaml::from_str(&s).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn bearer_file_round_trips() {
        let v = KubeAuth::BearerToken(TokenSource::File {
            path: "/var/run/sa-token".into(),
        });
        let s = serde_yaml::to_string(&v).unwrap();
        let back: KubeAuth = serde_yaml::from_str(&s).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn client_cert_inline_round_trips() {
        let v = KubeAuth::ClientCert {
            cert: BytesOrPath::Inline {
                data: "PEM-BYTES".into(),
            },
            key: BytesOrPath::Inline {
                data: "PEM-KEY".into(),
            },
        };
        let s = serde_yaml::to_string(&v).unwrap();
        let back: KubeAuth = serde_yaml::from_str(&s).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn exec_round_trips() {
        let v = KubeAuth::Exec {
            command: "aws-iam-authenticator".into(),
            args: vec!["token".into(), "-i".into(), "my-cluster".into()],
            env: vec![ExecEnv {
                name: "AWS_REGION".into(),
                value: "us-east-1".into(),
            }],
            api_version: "client.authentication.k8s.io/v1".into(),
        };
        let s = serde_yaml::to_string(&v).unwrap();
        let back: KubeAuth = serde_yaml::from_str(&s).unwrap();
        assert_eq!(v, back);
    }
}
