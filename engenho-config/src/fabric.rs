//! The fabric: how engenho carries bytes between its parts.
//!
//! Decision (docs/IMPROVEMENT-PLAN.md §5.1): NATS is not engenho's fabric. A
//! NATS server is a second process every node would need, which is a sidecar
//! under another name. The fabric lives inside the engenho binary, and the
//! config says so with a closed type rather than with a NATS server list that
//! nothing reads.

use std::fmt;

use serde::{Deserialize, Serialize};

/// How engenho's components reach one another.
///
/// One arm. There is no arm for an external broker, so "this node's fabric is
/// a NATS server" has no representation: `fabric: nats` is refused at parse.
/// When the multi-node transport is built (§5.1 (d), gated by edge 18) it also
/// lives inside the binary and arrives as a new arm here.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Fabric {
    /// Every byte between engenho's components stays inside the one process:
    /// the store's Raft traffic rides the in-process router. No sidecar, no
    /// second daemon to start, supervise or upgrade.
    #[default]
    InBinary,
}

/// The retired `teia:` section, exactly as operators and the Nix module wrote
/// it.
///
/// It is accepted, for one release, so that a node whose config still carries
/// the key boots; it is read by nothing. The fields are private and there is
/// no accessor, so no code can consult a NATS setting through
/// [`crate::EngenhoConfig`]; its only observable effect is
/// [`ConfigDeprecation::TeiaSection`].
///
/// Every field is optional because the section used to be merged onto a
/// default and may be partial. Unknown sub-keys are still refused, as they
/// were before, so a config that failed to parse still fails. What is no
/// longer done is validation: an unread section cannot fail a boot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyTeiaSection {
    // `pub(crate)` so the mutability table can name every field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) servers: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) cluster: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) credentials_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) connect_timeout_seconds: Option<u32>,
}

/// A key engenho still accepts from operator config but no longer reads.
///
/// Returned by [`crate::EngenhoConfig::deprecations`] so the boot path can log
/// each one; typed so a caller or a test matches the variant, not the text.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConfigDeprecation {
    /// The `teia:` section (NATS servers for the teia mesh). Superseded by
    /// `fabric: in_binary`; see [`Fabric`].
    TeiaSection,
}

impl ConfigDeprecation {
    /// The operator-facing config key that is deprecated.
    #[must_use]
    pub fn key(self) -> &'static str {
        match self {
            Self::TeiaSection => "teia",
        }
    }

    /// What the operator should write instead, if anything.
    #[must_use]
    pub fn replacement(self) -> &'static str {
        match self {
            Self::TeiaSection => "fabric: in_binary",
        }
    }

    /// Stable identifier for telemetry (`config.deprecation.kind`).
    #[must_use]
    pub fn kind(self) -> &'static str {
        match self {
            Self::TeiaSection => "teia_section",
        }
    }

    fn reason(self) -> &'static str {
        match self {
            Self::TeiaSection => "NATS is not engenho's fabric",
        }
    }
}

impl fmt::Display for ConfigDeprecation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "config key `{}` is deprecated and ignored ({}); the replacement is `{}`. \
             It is accepted for one release only: remove it.",
            self.key(),
            self.reason(),
            self.replacement()
        )
    }
}
