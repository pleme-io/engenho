//! engenho-controllers tunables + per-controller toggles.

use serde::{Deserialize, Serialize};
use shikumi::TieredConfig;

use crate::error::ConfigError;

/// Controllers config — per-controller enable toggles + runtime
/// tunables.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllersConfig {
    /// Per-controller enable toggles.
    pub enable: ControllerEnable,
    /// Namespace scope (empty = all).
    pub namespace: String,
    /// WatchDriver fallback tick interval (seconds).
    pub fallback_interval_seconds: u32,
    /// WatchDriver event-coalescing window (milliseconds).
    pub debounce_milliseconds: u32,
}

/// Per-controller enable flags.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerEnable {
    /// ReplicaSet → Pod controller.
    pub replicaset: bool,
    /// Deployment → ReplicaSet controller.
    pub deployment: bool,
    /// StatefulSet → ordered Pod controller.
    #[serde(default = "default_true")]
    pub statefulset: bool,
    /// Job → Pod-to-completion controller.
    #[serde(default = "default_true")]
    pub job: bool,
    /// Service → Endpoints controller.
    pub endpoints: bool,
    /// Owner-reference garbage collector.
    pub gc: bool,
    /// CRD → dynamic CR-handler registration controller. Watches
    /// CustomResourceDefinition objects + registers a `StoreBackedHandler`
    /// per served version into the live apiserver router table.
    #[serde(default = "default_true")]
    pub crd: bool,
}

/// serde default for the `statefulset` / `job` toggles — `true` so an
/// operator YAML written before these flags existed still enables them
/// (mirrors `prescribed_default`).
fn default_true() -> bool {
    true
}

impl TieredConfig for ControllersConfig {
    fn bare() -> Self {
        Self {
            enable: ControllerEnable {
                replicaset: false,
                deployment: false,
                statefulset: false,
                job: false,
                endpoints: false,
                gc: false,
                crd: false,
            },
            namespace: String::new(),
            fallback_interval_seconds: 0,
            debounce_milliseconds: 0,
        }
    }

    fn prescribed_default() -> Self {
        Self {
            enable: ControllerEnable {
                replicaset: true,
                deployment: true,
                statefulset: true,
                job: true,
                endpoints: true,
                gc: true,
                crd: true,
            },
            namespace: String::new(),
            fallback_interval_seconds: 30,
            debounce_milliseconds: 50,
        }
    }

    fn extend(self, base: &Self) -> Self {
        Self {
            enable: self.enable,
            namespace: if self.namespace.is_empty() {
                base.namespace.clone()
            } else {
                self.namespace
            },
            fallback_interval_seconds: if self.fallback_interval_seconds == 0 {
                base.fallback_interval_seconds
            } else {
                self.fallback_interval_seconds
            },
            debounce_milliseconds: if self.debounce_milliseconds == 0 {
                base.debounce_milliseconds
            } else {
                self.debounce_milliseconds
            },
        }
    }
}

impl ControllersConfig {
    /// Validate the controllers config.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidField`] when fallback interval is zero.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.fallback_interval_seconds == 0 {
            return Err(ConfigError::InvalidField {
                field: "controllers.fallback_interval_seconds".into(),
                reason: "fallback interval must be > 0".into(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prescribed_default_enables_all_controllers_and_validates() {
        let cfg = ControllersConfig::prescribed_default();
        assert!(cfg.enable.replicaset);
        assert!(cfg.enable.deployment);
        assert!(cfg.enable.statefulset);
        assert!(cfg.enable.job);
        assert!(cfg.enable.endpoints);
        assert!(cfg.enable.gc);
        assert!(cfg.enable.crd);
        cfg.validate().unwrap();
    }

    #[test]
    fn bare_disables_everything_and_fails_validation() {
        let cfg = ControllersConfig::bare();
        assert!(!cfg.enable.replicaset);
        assert!(cfg.validate().is_err());
    }
}
