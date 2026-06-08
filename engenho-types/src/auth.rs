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
use std::collections::BTreeMap;
use std::path::PathBuf;

/// A SERVER-SIDE resolved identity — the authenticated principal a request
/// belongs to AFTER the apiserver's authenticator chain runs.
///
/// DISTINCT from [`KubeAuth`] (which is the CLIENT-side *configuration* of
/// HOW to authenticate). `UserInfo` is the RESULT: who the request turned
/// out to be. Its shape mirrors upstream `authentication/v1.UserInfo`
/// field-for-field so it serializes byte-compatibly into an
/// `AdmissionReview.request.userInfo` and a `SelfSubjectReview.status.userInfo`
/// — the two wire surfaces that carry it.
///
/// `Default` is the empty identity (all fields empty). It exists so an
/// `AdmissionRequest.user_info` can default to empty in the test
/// constructors that predate authn, and so an unauthenticated path never
/// hand-builds a partial identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UserInfo {
    /// The principal's name (`system:anonymous`, `engenho-admin`, a cert CN,
    /// …). Omitted from the wire when empty.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub username: String,
    /// A stable unique identifier for the principal. Omitted when empty.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub uid: String,
    /// The groups the principal belongs to. Omitted when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<String>,
    /// Free-form `map[string][]string` extra attributes. `BTreeMap` for
    /// deterministic serialization. Omitted when empty.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, Vec<String>>,
}

/// The group every authenticated principal carries (upstream convention).
const GROUP_AUTHENTICATED: &str = "system:authenticated";
/// The group an unauthenticated (anonymous) request carries.
const GROUP_UNAUTHENTICATED: &str = "system:unauthenticated";
/// The super-user group the bootstrap admin identity carries.
const GROUP_MASTERS: &str = "system:masters";

impl UserInfo {
    /// The anonymous identity — a no-credential request resolves here. Still
    /// ALLOWED while authorize-ALL is retained; the identity merely becomes
    /// *known* (`system:anonymous` / `system:unauthenticated`).
    #[must_use]
    pub fn anonymous() -> Self {
        Self {
            username: "system:anonymous".to_string(),
            uid: String::new(),
            groups: vec![GROUP_UNAUTHENTICATED.to_string()],
            extra: BTreeMap::new(),
        }
    }

    /// The bootstrap admin identity — the holder of the admin client cert
    /// (`O=system:masters`) or the configured admin bearer token. Groups
    /// `system:masters` + `system:authenticated`.
    #[must_use]
    pub fn admin() -> Self {
        Self {
            username: "engenho-admin".to_string(),
            uid: String::new(),
            groups: vec![
                GROUP_MASTERS.to_string(),
                GROUP_AUTHENTICATED.to_string(),
            ],
            extra: BTreeMap::new(),
        }
    }

    /// The identity of an X509-authenticated client: username = the cert's
    /// Subject CN, groups = the cert's Subject Organizations PLUS
    /// `system:authenticated` (upstream's cert-authn group mapping). The
    /// `system:authenticated` group is appended (not prepended) so an
    /// `O=system:masters` cert lands `[system:masters, system:authenticated]`,
    /// identical to [`Self::admin`].
    #[must_use]
    pub fn from_client_cert(common_name: &str, organizations: &[String]) -> Self {
        let mut groups: Vec<String> = organizations.to_vec();
        groups.push(GROUP_AUTHENTICATED.to_string());
        Self {
            username: common_name.to_string(),
            uid: String::new(),
            groups,
            extra: BTreeMap::new(),
        }
    }
}

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
    fn user_info_round_trips_json() {
        let mut extra = BTreeMap::new();
        extra.insert("scopes".to_string(), vec!["openid".to_string()]);
        let v = UserInfo {
            username: "alice".to_string(),
            uid: "uid-1".to_string(),
            groups: vec!["g1".to_string(), GROUP_AUTHENTICATED.to_string()],
            extra,
        };
        let s = serde_json::to_string(&v).unwrap();
        let back: UserInfo = serde_json::from_str(&s).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn user_info_serializes_camel_case_authentication_v1_fields() {
        // The wire field names MUST match upstream authentication/v1.UserInfo
        // so an AdmissionReview / SelfSubjectReview body is byte-compatible.
        let v = UserInfo {
            username: "u".to_string(),
            uid: "x".to_string(),
            groups: vec!["g".to_string()],
            extra: BTreeMap::new(),
        };
        let val: serde_json::Value = serde_json::to_value(&v).unwrap();
        let obj = val.as_object().unwrap();
        assert!(obj.contains_key("username"), "field `username`");
        assert!(obj.contains_key("uid"), "field `uid`");
        assert!(obj.contains_key("groups"), "field `groups`");
        // extra omitted (empty) — skip_serializing_if.
        assert!(!obj.contains_key("extra"), "empty extra omitted");
    }

    #[test]
    fn default_user_info_serializes_to_empty_object() {
        // Every field is skip-if-empty, so the default identity is `{}` —
        // the shape an AdmissionRequest carries before authn threads one in.
        let v = UserInfo::default();
        let s = serde_json::to_string(&v).unwrap();
        assert_eq!(s, "{}");
    }

    #[test]
    fn anonymous_constructor_is_system_anonymous() {
        let a = UserInfo::anonymous();
        assert_eq!(a.username, "system:anonymous");
        assert!(a.groups.iter().any(|g| g == GROUP_UNAUTHENTICATED));
        assert!(
            !a.groups.iter().any(|g| g == GROUP_AUTHENTICATED),
            "anonymous is NOT system:authenticated"
        );
    }

    #[test]
    fn admin_constructor_carries_masters() {
        let a = UserInfo::admin();
        assert_eq!(a.username, "engenho-admin");
        assert!(a.groups.iter().any(|g| g == GROUP_MASTERS));
        assert!(a.groups.iter().any(|g| g == GROUP_AUTHENTICATED));
    }

    #[test]
    fn from_client_cert_maps_orgs_to_groups_plus_authenticated() {
        let u = UserInfo::from_client_cert(
            "engenho-admin",
            &["system:masters".to_string()],
        );
        assert_eq!(u.username, "engenho-admin");
        assert_eq!(
            u.groups,
            vec![GROUP_MASTERS.to_string(), GROUP_AUTHENTICATED.to_string()],
            "orgs first, then system:authenticated appended"
        );
        // An O=system:masters client cert is identity-equivalent to admin().
        assert_eq!(u.groups, UserInfo::admin().groups);
    }

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
