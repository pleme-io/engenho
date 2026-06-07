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
        };
        let base = TlsConfig {
            enabled: true,
            auto_generate: true,
            ca_cert_path: Some("/b/ca.crt".into()),
            ca_key_path: Some("/b/ca.key".into()),
            cert_path: Some("/b/srv.crt".into()),
            key_path: Some("/b/srv.key".into()),
        };
        let merged = overlay.extend(&base);
        // overlay's ca_cert_path wins; the rest fall back to base.
        assert_eq!(merged.ca_cert_path, Some("/o/ca.crt".into()));
        assert_eq!(merged.ca_key_path, Some("/b/ca.key".into()));
        assert!(!merged.auto_generate, "overlay's bool wins");
    }
}
