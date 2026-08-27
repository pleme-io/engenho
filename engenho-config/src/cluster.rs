//! Cluster identity config.

use serde::{Deserialize, Serialize};
use shikumi::TieredConfig;

use crate::error::ConfigError;

/// The prefix every per-node cluster name carries.
const CLUSTER_PREFIX: &str = "engenho";

/// How many hex chars of the BLAKE3 digest land in the name.
///
/// 8 hex chars = 32 bits. Collision risk across a fleet of even a few
/// thousand nodes is negligible, and it keeps the name short enough to
/// read in a `kubectl config get-contexts` listing.
const HASH_LEN: usize = 8;

/// The node's hostname, reduced to a token the substrate accepts.
///
/// `validate()` below rejects dots and spaces because the cluster name
/// becomes a NATS subject token, a Helm release name and a label value —
/// so a raw hostname like `cid.local` cannot be used directly.
fn node_token(raw: &str) -> String {
    let t: String = raw
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    // Collapse runs and trim separators so `cid..local` is `cid-local`,
    // not `cid--local`.
    let mut out = String::with_capacity(t.len());
    let mut prev_dash = true; // leading dashes are dropped
    for c in t.chars() {
        if c == '-' {
            if !prev_dash {
                out.push('-');
            }
            prev_dash = true;
        } else {
            out.push(c);
            prev_dash = false;
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    if out.is_empty() { "node".into() } else { out }
}

/// The deterministic per-node cluster name: `engenho-<node>-<hash8>`.
///
/// ── ★ WHY A HASH WHEN THE HOSTNAME IS ALREADY IN THE NAME ────────────
/// Because `node_token` is LOSSY and therefore not injective. It maps
/// every non-alphanumeric byte to `-` and collapses runs, so
/// `cid.example.com` and `cid-example-com` — two genuinely different
/// hosts — both reduce to `cid-example-com`. The digest is taken over
/// the RAW hostname, so it restores the distinction the sanitizer
/// destroys. Without it, "named after the node" would be a name that
/// two nodes can share.
///
/// Deterministic by construction: same host ⇒ same name, across
/// reboots, reinstalls and rebuilds, with no state to persist. That is
/// what lets kubectl / k9s / flux contexts stay valid without anything
/// tracking them.
#[must_use]
pub fn default_cluster_name() -> String {
    let raw = gethostname::gethostname().to_string_lossy().into_owned();
    cluster_name_for_host(&raw)
}

/// [`default_cluster_name`] against an explicit hostname — the testable
/// half, kept separate so the naming rule is provable without depending
/// on whatever machine the suite runs on.
#[must_use]
pub fn cluster_name_for_host(raw: &str) -> String {
    let token = node_token(raw);
    let digest = blake3::hash(raw.trim().to_ascii_lowercase().as_bytes());
    let hash = &digest.to_hex()[..HASH_LEN];
    format!("{CLUSTER_PREFIX}-{token}-{hash}")
}

/// Cluster identity — name + region.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    /// Cluster name (e.g. "rio", "engenho-local"). Used as the
    /// teia subject namespace + Helm release name + per-resource
    /// labels.
    pub name: String,
    /// Cloud region or homelab location identifier.
    pub region: String,
}

impl TieredConfig for ClusterConfig {
    fn bare() -> Self {
        Self {
            name: String::new(),
            region: String::new(),
        }
    }

    fn prescribed_default() -> Self {
        Self {
            // Per-node by DEFAULT — see `default_cluster_name`. Every node
            // gets its own engenho without configuring anything, and two
            // nodes never collide. Override with `ENGENHO__CLUSTER__NAME`
            // or the file tier when a shared name is genuinely wanted.
            name: default_cluster_name(),
            region: "homelab".into(),
        }
    }

    fn extend(self, base: &Self) -> Self {
        Self {
            name: if self.name.is_empty() {
                base.name.clone()
            } else {
                self.name
            },
            region: if self.region.is_empty() {
                base.region.clone()
            } else {
                self.region
            },
        }
    }
}

impl ClusterConfig {
    /// Validate the cluster config.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidField`] if name is empty or
    /// contains dots/spaces (the substrate uses cluster name in
    /// NATS subjects which require alphanumeric tokens).
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.name.is_empty() {
            return Err(ConfigError::InvalidField {
                field: "cluster.name".into(),
                reason: "cluster name cannot be empty".into(),
            });
        }
        if self.name.contains('.') || self.name.contains(' ') {
            return Err(ConfigError::InvalidField {
                field: "cluster.name".into(),
                reason: format!(
                    "cluster name '{}' contains invalid chars (.,space)",
                    self.name
                ),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ★ THE GENERATED NAME MUST SURVIVE THE VALIDATOR.
    ///
    /// `validate()` rejects dots and spaces because the name becomes a
    /// NATS subject token, a Helm release name and a label value. A
    /// hostname like `cid.local` or `My Mac` would fail all three, so the
    /// sanitizer is not cosmetic — it is what makes a default derived
    /// from an arbitrary hostname legal at all.
    #[test]
    fn generated_names_validate_for_hostile_hostnames() {
        for raw in [
            "cid",
            "cid.local",
            "CID.Local",
            "my mac.lan",
            "..--..",
            "",
            "host_with_underscores.example.com",
        ] {
            let cfg = ClusterConfig {
                name: cluster_name_for_host(raw),
                region: "homelab".into(),
            };
            cfg.validate()
                .unwrap_or_else(|e| panic!("{raw:?} produced an invalid name {:?}: {e}", cfg.name));
        }
    }

    /// Same host, same name — across reboots, reinstalls, rebuilds. This
    /// is what lets a kubectl/k9s/flux context stay valid with nothing
    /// persisting it.
    #[test]
    fn naming_is_deterministic() {
        assert_eq!(cluster_name_for_host("cid"), cluster_name_for_host("cid"));
        assert_eq!(
            cluster_name_for_host("cid.local"),
            cluster_name_for_host("CID.Local"),
            "case and host are normalized together, so the name is stable"
        );
    }

    /// ★ THE REASON THE HASH EXISTS, as an executable fact.
    ///
    /// `node_token` is lossy: both of these sanitize to `cid-example-com`.
    /// Without the digest over the RAW hostname, two different machines
    /// would claim the same cluster name — and "named after the node"
    /// would be false precisely when it matters.
    #[test]
    fn hosts_that_sanitize_alike_still_get_distinct_names() {
        let a = cluster_name_for_host("cid.example.com");
        let b = cluster_name_for_host("cid-example-com");
        assert!(a.starts_with("engenho-cid-example-com-"), "{a}");
        assert!(b.starts_with("engenho-cid-example-com-"), "{b}");
        assert_ne!(a, b, "the digest must restore what the sanitizer lost");
    }

    #[test]
    fn node_token_collapses_and_trims_separators() {
        assert_eq!(node_token("cid..local"), "cid-local");
        assert_eq!(node_token("--cid--"), "cid");
        assert_eq!(node_token(""), "node");
    }

    #[test]
    fn prescribed_default_validates() {
        ClusterConfig::prescribed_default().validate().unwrap();
    }

    #[test]
    fn bare_fails_validation() {
        assert!(ClusterConfig::bare().validate().is_err());
    }

    #[test]
    fn name_with_dot_rejected() {
        let cfg = ClusterConfig {
            name: "rio.example".into(),
            region: "x".into(),
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn extend_fills_empty_fields_from_base() {
        let overlay = ClusterConfig {
            name: "myname".into(),
            region: String::new(),
        };
        let base = ClusterConfig::prescribed_default();
        let merged = overlay.extend(&base);
        assert_eq!(merged.name, "myname");
        assert_eq!(merged.region, "homelab");
    }
}
