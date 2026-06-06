//! Curated enums for fields the upstream OpenAPI types as a plain `string`
//! but whose value set is closed in prose (no `enum` array in the schema).
//!
//! The generator (`engenho-kube-codegen`) cannot mechanically derive these —
//! the OpenAPI gives only a description listing the values. They live here,
//! hand-authored, and the generator REFERENCES them via its field-type
//! override table (`emit_typed::FIELD_OVERRIDES`) so a regenerate keeps the
//! typed surface. Everything else in `generated_v1_34/` is machine-emitted;
//! this module is the small, declared exception.
//!
//! Add an entry here + a matching row in `emit_typed::FIELD_OVERRIDES` to
//! promote any other prose-only-enum field to a typed enum.

use serde::{Deserialize, Serialize};

/// `PodPhase` — the simple high-level summary of a Pod's lifecycle.
///
/// Encoded as a string in JSON. Engenho's typed surface uses a closed enum
/// because the upstream contract is closed: a Pod's phase is always one of the
/// five named below or absent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PodPhase {
    /// Accepted by the cluster but not yet running.
    Pending,
    /// Bound to a node, all containers created.
    Running,
    /// All containers terminated successfully.
    Succeeded,
    /// At least one container terminated in failure.
    Failed,
    /// State could not be obtained.
    Unknown,
}

/// Typed Secret type — upstream's canonical types as a closed enum. Custom
/// string types deserialize as `Other(s)` to keep the wire faithful.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SecretType {
    /// Known upstream-canonical type identifiers.
    Known(KnownSecretType),
    /// Any other string passed through verbatim.
    Other(String),
}

/// The closed set of `kubernetes.io/*` Secret types upstream documents.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KnownSecretType {
    /// `Opaque` — default catch-all.
    Opaque,
    /// `kubernetes.io/service-account-token`
    #[serde(rename = "kubernetes.io/service-account-token")]
    ServiceAccountToken,
    /// `kubernetes.io/dockercfg`
    #[serde(rename = "kubernetes.io/dockercfg")]
    Dockercfg,
    /// `kubernetes.io/dockerconfigjson`
    #[serde(rename = "kubernetes.io/dockerconfigjson")]
    DockerConfigJson,
    /// `kubernetes.io/basic-auth`
    #[serde(rename = "kubernetes.io/basic-auth")]
    BasicAuth,
    /// `kubernetes.io/ssh-auth`
    #[serde(rename = "kubernetes.io/ssh-auth")]
    SshAuth,
    /// `kubernetes.io/tls`
    #[serde(rename = "kubernetes.io/tls")]
    Tls,
    /// `bootstrap.kubernetes.io/token`
    #[serde(rename = "bootstrap.kubernetes.io/token")]
    BootstrapToken,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pod_phase_round_trips_canonical_strings() {
        assert_eq!(serde_json::to_string(&PodPhase::Running).unwrap(), "\"Running\"");
        assert_eq!(
            serde_json::from_str::<PodPhase>("\"Succeeded\"").unwrap(),
            PodPhase::Succeeded
        );
    }

    #[test]
    fn secret_type_serializes_canonical_and_custom() {
        assert_eq!(
            serde_json::to_string(&SecretType::Known(KnownSecretType::ServiceAccountToken)).unwrap(),
            "\"kubernetes.io/service-account-token\""
        );
        assert_eq!(
            serde_json::to_string(&SecretType::Other("custom".into())).unwrap(),
            "\"custom\""
        );
        assert_eq!(
            serde_json::from_str::<SecretType>("\"Opaque\"").unwrap(),
            SecretType::Known(KnownSecretType::Opaque)
        );
    }
}
