//! What changing each leaf does to a running engenho — sealed.
//!
//! Every leaf of [`EngenhoConfig`] has exactly one [`Mutability`], and the
//! table is written so it cannot fall behind the configuration: each section
//! is classified by a [`section!`] that names every field of its struct, and
//! the macro also destructures the struct with those names and no `..`. A
//! field added to a section is therefore a compile error (E0027) until it is
//! classified here, and `tests::every_leaf_is_classified_and_nothing_else`
//! pins the table against what the configuration actually serializes to.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

use crate::leaf::LeafPath;
use crate::{
    ClusterConfig, ConsistencyConfig, ControlConfig, ControlSocketConfig, ControllerEnable,
    ControllersConfig, EngenhoConfig, LegacyTeiaSection, NetworkingConfig, RemoteControlConfig,
    RevoadaConfig, RuntimeConfig, SchedulerConfig, TlsConfig, TopologyConfig,
};

/// What applying a change to one leaf does. The spellings are the control
/// API's `Mutability`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "class", rename_all = "snake_case")]
pub enum Mutability {
    /// Nothing reads it at run time: the effective configuration changes and
    /// nothing else does.
    Inert {
        /// Why nothing reads it.
        why: InertWhy,
    },
    /// Applied to the running runtime at once.
    Live {
        /// How.
        effect: LiveEffect,
    },
    /// Takes a set of children respawning (a runtime restart until they can
    /// be respawned one by one).
    Respawn {
        /// Which set.
        set: RespawnSet,
    },
    /// Read only while booting; the next boot picks it up, no restart needed.
    NextBoot,
    /// Takes a runtime restart.
    RestartRuntime,
    /// Cannot be overridden through the control plane at all.
    NotOverridable {
        /// What it anchors.
        anchor: Anchor,
    },
}

/// Why an [`Mutability::Inert`] leaf is not read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InertWhy {
    /// The code that would read it does not run in this process.
    NotRun,
    /// It has one accepted value, so it cannot change.
    SingleAcceptedValue,
}

/// How a [`Mutability::Live`] leaf is applied.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveEffect {
    /// The kubeconfigs are published again.
    Republish,
}

/// The children a [`Mutability::Respawn`] leaf concerns.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RespawnSet {
    /// The controller drivers that are enabled.
    EnabledDrivers,
    /// The kubelet and etcd-façade listeners.
    NodeLocalListeners,
}

/// Why a leaf is [`Mutability::NotOverridable`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Anchor {
    /// It is read before the override tier is — the override tier itself
    /// lives under the data directory, and the control socket is bound
    /// before any configuration resolves.
    OverlayAnchor,
    /// It decides whether the store is durable at all.
    StorageMode,
    /// It is the cluster's or the node's identity, which certificates and
    /// stored objects already name.
    Identity,
    /// Values already allocated from it live in the store.
    Allocated,
    /// A retired key, accepted from a file and read by nothing.
    Retired,
}

impl Mutability {
    /// Whether the control plane may override the leaf.
    #[must_use]
    pub const fn overridable(self) -> bool {
        !matches!(self, Self::NotOverridable { .. })
    }

    /// Whether the running runtime can take the change without a restart:
    /// it is applied (live) or has nothing to apply (inert).
    #[must_use]
    pub const fn applies_in_place(self) -> bool {
        matches!(self, Self::Inert { .. } | Self::Live { .. })
    }

    /// Whether the running runtime reflects the change only after a restart.
    #[must_use]
    pub const fn needs_restart(self) -> bool {
        matches!(self, Self::Respawn { .. } | Self::RestartRuntime)
    }
}

/// One classified leaf.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeafSpec {
    /// Its path.
    pub path: LeafPath,
    /// What changing it does.
    pub mutability: Mutability,
}

/// Classify every field of one section. A field with `=> class` is a leaf;
/// one without is a sub-section, classified by its own `section!`. `as
/// "wire"` names a field whose serialized key differs from its name.
macro_rules! section {
    ($rows:ident, $prefix:literal, $ty:ident {
        $($field:ident $(as $wire:literal)? $(=> $class:expr)?),* $(,)?
    }) => {{
        // Never called: it only has to compile, and it names every field
        // with no `..`, so a new field fails to compile until it is here.
        let _every_field_named = |v: &$ty| {
            let $ty { $($field: _),* } = v;
        };
        $( section!(@row $rows, $prefix, $field [$($wire)?] [$($class)?]); )*
    }};
    // A sub-section: its own `section!` classifies it.
    (@row $rows:ident, $prefix:literal, $field:ident [$($wire:literal)?] []) => {};
    (@row $rows:ident, $prefix:literal, $field:ident [] [$class:expr]) => {
        section!(@push $rows, $prefix, stringify!($field), $class)
    };
    (@row $rows:ident, $prefix:literal, $field:ident [$wire:literal] [$class:expr]) => {
        section!(@push $rows, $prefix, $wire, $class)
    };
    (@push $rows:ident, $prefix:literal, $key:expr, $class:expr) => {
        $rows.push(LeafSpec {
            path: LeafPath::join($prefix, $key)
                .expect("a configuration field name is a leaf segment"),
            mutability: $class,
        })
    };
}

#[allow(
    clippy::too_many_lines,
    reason = "one row per leaf: the table is as long as the configuration is wide"
)]
fn build() -> Vec<LeafSpec> {
    use Mutability as M;
    let inert = M::Inert {
        why: InertWhy::NotRun,
    };
    let republish = M::Live {
        effect: LiveEffect::Republish,
    };
    let drivers = M::Respawn {
        set: RespawnSet::EnabledDrivers,
    };
    let listeners = M::Respawn {
        set: RespawnSet::NodeLocalListeners,
    };
    let restart = M::RestartRuntime;
    let anchored = |anchor| M::NotOverridable { anchor };

    let mut rows = Vec::new();
    section!(rows, "", EngenhoConfig {
        cluster,
        revoada,
        fabric => M::Inert { why: InertWhy::SingleAcceptedValue },
        legacy_teia as "teia",
        scheduler,
        controllers,
        consistency,
        networking,
        runtime,
        control,
    });
    section!(rows, "cluster", ClusterConfig {
        name => anchored(Anchor::Identity),
        region => inert,
    });
    section!(rows, "revoada", RevoadaConfig { topology });
    section!(rows, "revoada.topology", TopologyConfig {
        strategy => inert,
        min_nodes => inert,
        grace_period_seconds => inert,
    });
    section!(rows, "teia", LegacyTeiaSection {
        servers => anchored(Anchor::Retired),
        cluster => anchored(Anchor::Retired),
        credentials_path => anchored(Anchor::Retired),
        connect_timeout_seconds => anchored(Anchor::Retired),
    });
    section!(rows, "scheduler", SchedulerConfig {
        strategy => restart,
        namespace => restart,
        tick_interval_seconds => restart,
    });
    section!(rows, "controllers", ControllersConfig {
        enable,
        namespace => restart,
        fallback_interval_seconds => restart,
        debounce_milliseconds => restart,
    });
    section!(rows, "controllers.enable", ControllerEnable {
        replicaset => drivers,
        deployment => drivers,
        statefulset => drivers,
        daemonset => drivers,
        job => drivers,
        cronjob => drivers,
        endpoints => drivers,
        service_routing => drivers,
        gc => drivers,
        crd => drivers,
        namespace => drivers,
        pv_binder => drivers,
        pdb => drivers,
    });
    section!(rows, "consistency", ConsistencyConfig {
        default_tier => inert,
    });
    section!(rows, "networking", NetworkingConfig {
        service_cidr => anchored(Anchor::Allocated),
        datapath_mode => restart,
    });
    section!(rows, "runtime", RuntimeConfig {
        listen_addr => restart,
        data_dir => anchored(Anchor::OverlayAnchor),
        durable => anchored(Anchor::StorageMode),
        node_name => anchored(Anchor::Identity),
        kubeconfig_publish_path => republish,
        kubeconfig_publish_visibility => republish,
        pod_kubeconfig_publish_path => republish,
        // The server certificate's SANs and the remote kubeconfig's address
        // are both derived from it, and the certificate is issued at boot.
        advertise_address => restart,
        remote_kubeconfig_publish_path => republish,
        kubelet_listen_addr => listeners,
        etcd_listen_addr => listeners,
        kubelet_backend => restart,
        podman_binary => restart,
        host_path_allowlist => restart,
        leadership_timeout_seconds => M::NextBoot,
        tls,
    });
    // The server certificate is issued in memory at every boot, so a TLS
    // change — SANs included — is a restart, never a re-initialization.
    section!(rows, "runtime.tls", TlsConfig {
        enabled => restart,
        auto_generate => restart,
        ca_cert_path => restart,
        ca_key_path => restart,
        cert_path => restart,
        key_path => restart,
        extra_sans => restart,
    });
    section!(rows, "control", ControlConfig { socket, remote });
    section!(rows, "control.socket", ControlSocketConfig {
        path => anchored(Anchor::OverlayAnchor),
        access => anchored(Anchor::OverlayAnchor),
        group_tier => anchored(Anchor::OverlayAnchor),
    });
    // Who may control this daemon remotely is never settable through the
    // control plane itself: a mutate-tier caller could otherwise pin a key
    // of its own at destructive.
    section!(rows, "control.remote", RemoteControlConfig {
        enable => anchored(Anchor::OverlayAnchor),
        listen_addr => anchored(Anchor::OverlayAnchor),
        authorized_clients => anchored(Anchor::OverlayAnchor),
    });
    rows
}

static TABLE: LazyLock<Vec<LeafSpec>> = LazyLock::new(build);
static BY_PATH: LazyLock<BTreeMap<LeafPath, Mutability>> = LazyLock::new(|| {
    TABLE
        .iter()
        .map(|spec| (spec.path.clone(), spec.mutability))
        .collect()
});

/// Every leaf of the configuration, classified, in declaration order.
#[must_use]
pub fn leaves() -> &'static [LeafSpec] {
    &TABLE
}

/// What changing `path` does; `None` when it is not a leaf.
#[must_use]
pub fn classify(path: &LeafPath) -> Option<Mutability> {
    BY_PATH.get(path).copied()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::PathBuf;

    use super::*;
    use crate::TieredConfig;
    use crate::leaf::flatten;

    /// A configuration with every optional leaf present, so that what it
    /// serializes to names every leaf there is.
    fn maximal() -> EngenhoConfig {
        let mut config = EngenhoConfig::prescribed_default();
        config.legacy_teia = Some(LegacyTeiaSection {
            servers: Some(vec!["nats://a".into()]),
            cluster: Some("c".into()),
            credentials_path: Some("/c".into()),
            connect_timeout_seconds: Some(1),
        });
        config.runtime.podman_binary = Some("/bin/podman".into());
        config.runtime.host_path_allowlist = vec!["/srv".into()];
        let tls = &mut config.runtime.tls;
        tls.ca_cert_path = Some(PathBuf::from("/ca.crt"));
        tls.ca_key_path = Some(PathBuf::from("/ca.key"));
        tls.cert_path = Some(PathBuf::from("/tls.crt"));
        tls.key_path = Some(PathBuf::from("/tls.key"));
        tls.extra_sans = vec!["node.example".into()];
        config.control.socket.path = Some(PathBuf::from("/run/engenho/control.sock"));
        config
    }

    #[test]
    fn every_leaf_is_classified_and_nothing_else() {
        let serialized: BTreeSet<LeafPath> = flatten(&serde_json::to_value(maximal()).unwrap())
            .into_keys()
            .collect();
        let classified: BTreeSet<LeafPath> = leaves().iter().map(|s| s.path.clone()).collect();
        assert_eq!(
            classified.difference(&serialized).collect::<Vec<_>>(),
            Vec::<&LeafPath>::new(),
            "classified, but the configuration has no such leaf"
        );
        assert_eq!(
            serialized.difference(&classified).collect::<Vec<_>>(),
            Vec::<&LeafPath>::new(),
            "a leaf of the configuration nobody classified"
        );
        assert_eq!(
            classified.len(),
            leaves().len(),
            "no leaf is classified twice"
        );
    }

    #[test]
    fn identity_storage_and_the_overlay_anchor_are_not_overridable() {
        for path in [
            "runtime.data_dir",
            "runtime.durable",
            "runtime.node_name",
            "cluster.name",
            "networking.service_cidr",
            "control.socket.path",
            "teia.servers",
        ] {
            let class = classify(&LeafPath::parse(path).unwrap()).unwrap();
            assert!(!class.overridable(), "{path} is {class:?}");
        }
    }

    #[test]
    fn a_section_is_not_a_leaf() {
        for path in ["runtime", "runtime.tls", "controllers.enable", "nonsense"] {
            assert_eq!(classify(&LeafPath::parse(path).unwrap()), None, "{path}");
        }
    }

    #[test]
    fn the_spelling_is_the_control_apis() {
        let json = |m: Mutability| serde_json::to_value(m).unwrap();
        assert_eq!(
            json(Mutability::Inert {
                why: InertWhy::NotRun
            }),
            serde_json::json!({"class": "inert", "why": "not_run"})
        );
        assert_eq!(
            json(Mutability::NotOverridable {
                anchor: Anchor::OverlayAnchor
            }),
            serde_json::json!({"class": "not_overridable", "anchor": "overlay_anchor"})
        );
        assert_eq!(
            json(Mutability::NextBoot),
            serde_json::json!({"class": "next_boot"})
        );
    }
}
