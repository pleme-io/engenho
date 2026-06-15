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
    /// DaemonSet → one node-pinned Pod per schedulable node.
    /// `#[serde(default)]` is REQUIRED (the struct is
    /// `deny_unknown_fields`) so pre-existing operator YAML written
    /// before this flag still deserializes (defaults on).
    #[serde(default = "default_true")]
    pub daemonset: bool,
    /// Job → Pod-to-completion controller.
    #[serde(default = "default_true")]
    pub job: bool,
    /// CronJob → Job factory controller. Parses `spec.schedule` (5-field
    /// cron) and creates a `batch/v1` Job from the jobTemplate on schedule;
    /// the JobController then runs that Job's Pods. `#[serde(default)]` is
    /// REQUIRED (the struct is `deny_unknown_fields`) so pre-existing
    /// operator YAML written before this flag still deserializes (on).
    #[serde(default = "default_true")]
    pub cronjob: bool,
    /// Service → Endpoints controller.
    pub endpoints: bool,
    /// Service VIP routing controller (`ServiceRoutingController`):
    /// resolves Service + Endpoints → typed `ServiceRoute`s and drives the
    /// platform-selected datapath backend (iptables/ipvs on Linux,
    /// compute-only off-Linux per `networking.datapath_mode`).
    /// `#[serde(default)]` is REQUIRED (the struct is
    /// `deny_unknown_fields`) so pre-existing operator YAML written before
    /// this flag still deserializes (defaults on).
    #[serde(default = "default_true")]
    pub service_routing: bool,
    /// Owner-reference garbage collector.
    pub gc: bool,
    /// CRD → dynamic CR-handler registration controller. Watches
    /// CustomResourceDefinition objects + registers a `StoreBackedHandler`
    /// per served version into the live apiserver router table.
    #[serde(default = "default_true")]
    pub crd: bool,
    /// Namespace → cascade-deletion controller. Watches Namespaces; for
    /// each Terminating namespace holding the `kubernetes` finalizer it
    /// lists+deletes every namespaced object in that namespace then clears
    /// the finalizer (Background propagation). `#[serde(default)]` is
    /// REQUIRED (the struct is `deny_unknown_fields`) so pre-existing
    /// operator YAML written before this flag still deserializes.
    #[serde(default = "default_true")]
    pub namespace: bool,
    /// PV/PVC binder + local-path dynamic provisioner
    /// (`PvBinderController`). Binds Pending PersistentVolumeClaims to
    /// matching Available PersistentVolumes (capacity/accessModes/SC/
    /// volumeName) and dynamically provisions a node-local hostPath PV via
    /// the local-path provisioner / default StorageClass when no PV matches.
    /// `#[serde(default)]` is REQUIRED (the struct is `deny_unknown_fields`)
    /// so pre-existing operator YAML written before this flag still
    /// deserializes (defaults on).
    #[serde(default = "default_true")]
    pub pv_binder: bool,
    /// PodDisruptionBudget controller: computes `status.{currentHealthy,
    /// desiredHealthy,expectedPods,disruptionsAllowed,observedGeneration}`
    /// from the pods its selector matches vs minAvailable/maxUnavailable.
    /// `#[serde(default)]` REQUIRED (deny_unknown_fields) so pre-existing
    /// operator YAML still deserializes (defaults on).
    #[serde(default = "default_true")]
    pub pdb: bool,
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
                daemonset: false,
                job: false,
                cronjob: false,
                endpoints: false,
                service_routing: false,
                gc: false,
                crd: false,
                namespace: false,
                pv_binder: false,
                pdb: false,
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
                daemonset: true,
                job: true,
                cronjob: true,
                endpoints: true,
                service_routing: true,
                gc: true,
                crd: true,
                namespace: true,
                pv_binder: true,
                pdb: true,
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
        assert!(cfg.enable.cronjob);
        assert!(cfg.enable.endpoints);
        assert!(cfg.enable.service_routing);
        assert!(cfg.enable.gc);
        assert!(cfg.enable.crd);
        assert!(cfg.enable.namespace);
        assert!(cfg.enable.pv_binder);
        cfg.validate().unwrap();
    }

    #[test]
    fn pre_existing_yaml_without_namespace_flag_still_deserializes() {
        // The serde default-true contract: an operator YAML written before
        // `enable.namespace` existed (deny_unknown_fields struct) still
        // deserializes, defaulting the flag on.
        let yaml = r#"
enable:
  replicaset: true
  deployment: true
  statefulset: true
  job: true
  endpoints: true
  gc: true
  crd: true
namespace: ""
fallback_interval_seconds: 30
debounce_milliseconds: 50
"#;
        let cfg: ControllersConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.enable.namespace, "missing flag defaults to true");
        // The same default-true contract covers the later service_routing
        // flag — pre-existing YAML lacking it still enables it.
        assert!(
            cfg.enable.service_routing,
            "missing service_routing flag defaults to true"
        );
        // Same default-true contract covers the later cronjob flag — the
        // YAML above omits it, so it must default on.
        assert!(cfg.enable.cronjob, "missing cronjob flag defaults to true");
        // And the later pv_binder flag — omitted above, must default on.
        assert!(
            cfg.enable.pv_binder,
            "missing pv_binder flag defaults to true"
        );
    }

    #[test]
    fn bare_disables_everything_and_fails_validation() {
        let cfg = ControllersConfig::bare();
        assert!(!cfg.enable.replicaset);
        assert!(cfg.validate().is_err());
    }
}
