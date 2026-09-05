//! TLS config — how the apiserver presents itself over HTTPS.
//!
//! At M0.4 the apiserver is HTTPS-by-default with a self-generated,
//! `data_dir`-persisted cluster CA. Real kubectl will not talk to a
//! `server: https://…` cluster over plaintext, so TLS is the
//! load-bearing half of the local-kubectl-compat brick.
//!
//! ```text
//! enabled        = true   → serve over TLS (the default).
//! auto_generate  = true   → mint + persist a cluster CA under
//!                           data_dir/pki/ when absent (the default).
//! *_path fields  = None   → derive from data_dir/pki/{ca.crt,ca.key,
//!                           apiserver.crt,apiserver.key}.
//! ```
//!
//! `bare()` disables TLS (the test / explicit-escape-hatch floor —
//! plaintext `axum::serve`). When `enabled && !auto_generate`, an
//! operator is supplying their OWN PKI, so all four explicit paths MUST
//! be set; `validate()` enforces that.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use shikumi::TieredConfig;

use crate::error::ConfigError;

/// Apiserver TLS configuration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Serve the K8s API over TLS. `true` in production (kubectl requires
    /// HTTPS); `false` is the plaintext escape hatch for tests + the
    /// explicit dev opt-out.
    pub enabled: bool,
    /// When `enabled`, mint + persist a self-signed cluster CA under
    /// `data_dir/pki/` if one isn't already there. `false` means the
    /// operator supplies their own PKI via the explicit `*_path` fields.
    pub auto_generate: bool,
    /// Explicit CA cert path. `None` → `data_dir/pki/ca.crt`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_cert_path: Option<PathBuf>,
    /// Explicit CA key path. `None` → `data_dir/pki/ca.key`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_key_path: Option<PathBuf>,
    /// Explicit server cert path. `None` → `data_dir/pki/apiserver.crt`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cert_path: Option<PathBuf>,
    /// Explicit server key path. `None` → `data_dir/pki/apiserver.key`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_path: Option<PathBuf>,
    /// Additional Subject Alternative Names for the serving certificate.
    ///
    /// ── ★ WHY THIS IS CONFIGURATION AND NOT DERIVED ──────────────────────
    /// The runtime derives the SANs it can KNOW: loopback, `kubernetes`, the
    /// container host gateway, this node's name, and the concrete listen IP.
    /// That set answers for the node. It cannot answer for the names the
    /// cluster is REACHED BY, because those are facts about the deployment
    /// rather than about the process — a tailnet name, a LAN alias, a
    /// `*.quero.cloud` record, a VIP in front of several apiservers. Deriving
    /// them would mean guessing, and a guessed SAN is worse than an absent
    /// one: the cert still builds and still serves.
    ///
    /// The failure this closes is quiet. A serving cert missing the name a
    /// client dials does not fail at boot, does not log, and does not degrade
    /// anything on the node; it fails at the CLIENT, once, as a certificate
    /// verification error that reads like a misconfigured kubeconfig. Adding
    /// the name later requires deleting the persisted PKI, because the CA and
    /// leaf are generated once and reloaded thereafter.
    ///
    /// Entries are plain host strings — `100.64.0.1`, `plo.tail1234.ts.net`,
    /// `engenho.plo.natal.quero.cloud`. No scheme, no port: anything that
    /// parses as an IP address becomes an IP SAN and everything else becomes a
    /// DNS SAN, the same rule `kubeadm`'s `certSANs` uses. Entries that
    /// duplicate a derived SAN collapse rather than erroring, so an operator
    /// may safely list a name without first checking whether it is automatic.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_sans: Vec<String>,
}

impl TieredConfig for TlsConfig {
    fn bare() -> Self {
        // The zero-opinion floor: plaintext, no PKI. Tests + the explicit
        // dev opt-out land here.
        Self {
            enabled: false,
            auto_generate: false,
            ca_cert_path: None,
            ca_key_path: None,
            cert_path: None,
            key_path: None,
            // No opinion about how this cluster is reached — the bare tier
            // has no deployment to have an opinion about.
            extra_sans: Vec::new(),
        }
    }

    fn prescribed_default() -> Self {
        // What 90% of operators want on first launch: HTTPS with a
        // self-generated, persisted cluster CA.
        Self {
            enabled: true,
            auto_generate: true,
            ca_cert_path: None,
            ca_key_path: None,
            cert_path: None,
            key_path: None,
            // Still none: the derived SANs cover a single-node local cluster
            // completely, which is what the prescribed tier is for. A name
            // beyond that is a deployment fact and arrives from the file tier.
            extra_sans: Vec::new(),
        }
    }

    fn extend(self, base: &Self) -> Self {
        Self {
            // `enabled` / `auto_generate` are plain bools with no "unset"
            // sentinel — the overlay's value wins (an operator disabling
            // TLS or auto-gen is explicit).
            enabled: self.enabled,
            auto_generate: self.auto_generate,
            ca_cert_path: self.ca_cert_path.or_else(|| base.ca_cert_path.clone()),
            ca_key_path: self.ca_key_path.or_else(|| base.ca_key_path.clone()),
            cert_path: self.cert_path.or_else(|| base.cert_path.clone()),
            key_path: self.key_path.or_else(|| base.key_path.clone()),
            // REPLACE, not append. An overlay that names SANs is stating the
            // full set it wants, exactly as a `values.yaml` list overrides
            // rather than extends. Appending would make a lower tier's entry
            // impossible to remove — you could add a SAN but never retract
            // one, which is the wrong shape for a security-relevant list.
            extra_sans: if self.extra_sans.is_empty() {
                base.extra_sans.clone()
            } else {
                self.extra_sans
            },
        }
    }
}

impl TlsConfig {
    /// Validate the TLS config.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidField`] when `enabled && !auto_generate` but
    /// one of the four explicit PKI paths is missing — an operator opting
    /// out of auto-generation MUST supply their own CA cert + key + server
    /// cert + key (there's nothing to derive and nothing to generate).
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.enabled && !self.auto_generate {
            let all_set = self.ca_cert_path.is_some()
                && self.ca_key_path.is_some()
                && self.cert_path.is_some()
                && self.key_path.is_some();
            if !all_set {
                return Err(ConfigError::InvalidField {
                    field: "runtime.tls".into(),
                    reason: "tls.enabled with auto_generate=false requires all four explicit \
                             paths (ca_cert_path, ca_key_path, cert_path, key_path)"
                        .into(),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_is_plaintext() {
        let t = TlsConfig::bare();
        assert!(!t.enabled);
        t.validate().unwrap();
    }

    #[test]
    fn prescribed_default_is_https_auto_generate() {
        let t = TlsConfig::prescribed_default();
        assert!(t.enabled);
        assert!(t.auto_generate);
        t.validate().unwrap();
    }

    #[test]
    fn explicit_pki_without_all_paths_is_rejected() {
        let mut t = TlsConfig::prescribed_default();
        t.auto_generate = false;
        // Missing all four explicit paths → invalid.
        assert!(t.validate().is_err());
    }

    #[test]
    fn explicit_pki_with_all_paths_validates() {
        let t = TlsConfig {
            enabled: true,
            auto_generate: false,
            ca_cert_path: Some("/pki/ca.crt".into()),
            ca_key_path: Some("/pki/ca.key".into()),
            cert_path: Some("/pki/srv.crt".into()),
            key_path: Some("/pki/srv.key".into()),
            extra_sans: Vec::new(),
        };
        t.validate().unwrap();
    }

    #[test]
    fn disabled_tls_never_requires_paths() {
        let mut t = TlsConfig::bare();
        t.auto_generate = false;
        // Disabled → no path requirement regardless of auto_generate.
        t.validate().unwrap();
    }

    #[test]
    fn extend_fills_paths_from_base() {
        let overlay = TlsConfig {
            enabled: true,
            auto_generate: false,
            ca_cert_path: Some("/o/ca.crt".into()),
            ca_key_path: None,
            cert_path: None,
            key_path: None,
            extra_sans: Vec::new(),
        };
        let base = TlsConfig {
            enabled: true,
            auto_generate: true,
            ca_cert_path: Some("/b/ca.crt".into()),
            ca_key_path: Some("/b/ca.key".into()),
            cert_path: Some("/b/srv.crt".into()),
            key_path: Some("/b/srv.key".into()),
            extra_sans: vec!["base.example".to_string()],
        };
        let merged = overlay.extend(&base);
        // overlay's ca_cert_path wins; the rest fall back to base.
        assert_eq!(merged.ca_cert_path, Some("/o/ca.crt".into()));
        assert_eq!(merged.ca_key_path, Some("/b/ca.key".into()));
        assert!(!merged.auto_generate, "overlay's bool wins");
        assert_eq!(
            merged.extra_sans,
            vec!["base.example".to_string()],
            "an overlay silent about SANs inherits the base's — otherwise \
             every tier above the one declaring them would erase them"
        );
    }

    #[test]
    fn an_overlay_that_names_sans_replaces_rather_than_appends() {
        // The decision worth pinning. Appending would mean a SAN, once
        // declared at any tier, could never be RETRACTED — you could widen
        // the certificate but never narrow it, which is the wrong direction
        // for a list that decides who a server will answer as.
        let base = TlsConfig {
            extra_sans: vec!["old.example".to_string(), "stale.example".to_string()],
            ..TlsConfig::prescribed_default()
        };
        let overlay = TlsConfig {
            extra_sans: vec!["new.example".to_string()],
            ..TlsConfig::prescribed_default()
        };
        let merged = overlay.extend(&base);
        assert_eq!(
            merged.extra_sans,
            vec!["new.example".to_string()],
            "the overlay states the full set it wants; a retracted name must \
             actually disappear"
        );
    }

    #[test]
    fn a_config_written_before_this_field_existed_still_parses() {
        // `deny_unknown_fields` makes the reverse direction strict, so the
        // compatibility that matters is this one: every engenho.yaml already
        // deployed omits `extra_sans` entirely.
        let yaml = "enabled: true\nauto_generate: true\n";
        let parsed: TlsConfig = serde_yaml::from_str(yaml).expect("legacy config must parse");
        assert!(parsed.extra_sans.is_empty());
    }
}
