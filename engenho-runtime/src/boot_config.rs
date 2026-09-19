//! The runtime's one reading of [`EngenhoConfig`] (I21, T5.8).
//!
//! ── ★ AN UNREAD FIELD IS A COMPILE ERROR ─────────────────────────────────
//! [`BootConfig::read`] destructures `EngenhoConfig` and every section in it
//! with no `..`, and nothing else in this crate reads an `EngenhoConfig`
//! field: every runtime path takes a [`BootConfig`]. A field added to any
//! section therefore does not compile (E0027) until `read` gives it one of
//! three meanings:
//!
//!   * **run** — moved into [`BootConfig`], where the runtime reads it;
//!   * **refused** — the runtime cannot do what the field asks, so
//!     [`Runtime::start`](crate::Runtime::start) fails with [`Unhonoured`]
//!     rather than booting something else under the operator's name;
//!   * **read, not run** — the field asks for nothing this binary could do
//!     differently, so it is recorded as a [`NotRun`] and logged at boot.
//!
//! `scheduler` is handed on whole: [`engenho_scheduler::Scheduler::from_config`]
//! is its one reader and destructures it the same way.
//!
//! ── ★ WHAT THIS DOES NOT MAKE IMPOSSIBLE ─────────────────────────────────
//! A field can still be bound as `field: _` beside a false comment; only
//! review catches that. The tier is "forced decision", not "proved use".
//! The runtime also keeps the whole `EngenhoConfig` for
//! [`Runtime::config`](crate::Runtime::config); nothing reads a field
//! through it.

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use engenho_config::{
    ClusterConfig, ConsistencyConfig, ConsistencyTierKind, ControllerEnable, ControllersConfig,
    DatapathMode, EngenhoConfig, KubeconfigVisibility, KubeletBackendKind, NetworkingConfig,
    RevoadaConfig, RuntimeConfig, SchedulerConfig, TeiaConfig, TlsConfig, TopologyConfig,
    TopologyStrategyKind,
};

use crate::health::Windows;

/// Everything the runtime runs, read from [`EngenhoConfig`] in one place.
///
/// Built only by [`BootConfig::read`].
#[derive(Clone, Debug)]
pub(crate) struct BootConfig {
    /// `cluster.name`: the store's raft cluster name, and the cluster every
    /// kubeconfig this node writes names.
    pub(crate) cluster_name: String,
    /// `runtime.listen_addr`, as written. Parsed where it is bound, so the
    /// parse error can name the string.
    pub(crate) listen_addr: String,
    /// `runtime.data_dir`.
    pub(crate) data_dir: PathBuf,
    /// `runtime.durable`.
    pub(crate) durable: bool,
    /// `runtime.node_name`.
    pub(crate) node_name: String,
    /// `runtime.kubeconfig_publish_path`.
    pub(crate) kubeconfig_publish_path: String,
    /// `runtime.kubeconfig_publish_visibility`.
    pub(crate) kubeconfig_publish_visibility: KubeconfigVisibility,
    /// `runtime.pod_kubeconfig_publish_path`.
    pub(crate) pod_kubeconfig_publish_path: String,
    /// `runtime.advertise_address`.
    pub(crate) advertise_address: String,
    /// `runtime.remote_kubeconfig_publish_path`.
    pub(crate) remote_kubeconfig_publish_path: String,
    /// `runtime.kubelet_listen_addr`.
    pub(crate) kubelet_listen_addr: String,
    /// `runtime.etcd_listen_addr`. Empty disables the façade.
    pub(crate) etcd_listen_addr: String,
    /// `runtime.kubelet_backend`. Read by [`Runtime::start`](crate::Runtime::start)
    /// only: [`Runtime::start_with_backend`](crate::Runtime::start_with_backend)
    /// is handed the backend it drives.
    pub(crate) kubelet_backend: KubeletBackendKind,
    /// `runtime.podman_binary`, read with `kubelet_backend`.
    pub(crate) podman_binary: Option<String>,
    /// `runtime.host_path_allowlist`.
    pub(crate) host_path_allowlist: Vec<String>,
    /// `runtime.leadership_timeout_seconds`.
    pub(crate) leadership_timeout_seconds: u32,
    /// `runtime.tls`, as the apiserver serves it.
    pub(crate) tls: ApiserverTls,
    /// `scheduler`, whole. Its one reader is `Scheduler::from_config`.
    pub(crate) scheduler: SchedulerConfig,
    /// `controllers.enable`.
    pub(crate) enable: ControllerEnable,
    /// `controllers.namespace`: `None` is every namespace (empty in config).
    pub(crate) controllers_namespace: Option<String>,
    /// `controllers.fallback_interval_seconds`.
    fallback: Duration,
    /// `controllers.debounce_milliseconds`.
    debounce: Duration,
    /// `networking.service_cidr`.
    pub(crate) service_cidr: String,
    /// `networking.datapath_mode`.
    pub(crate) datapath_mode: DatapathMode,
    /// What was read and is not run, in section order.
    pub(crate) not_run: Vec<NotRun>,
}

impl BootConfig {
    /// Read `config`: every field of every section, each run, refused or
    /// recorded as not run.
    ///
    /// # Errors
    ///
    /// [`Unhonoured`] for a field that asks for something this runtime
    /// does not do.
    pub(crate) fn read(config: &EngenhoConfig) -> Result<Self, Unhonoured> {
        // ★ No `..` here or in the section readers it calls. A new field is
        // E0027 until it is run, refused or recorded as not run.
        let EngenhoConfig {
            cluster,
            revoada,
            teia,
            scheduler,
            controllers,
            consistency,
            networking,
            runtime,
        } = config;
        let ClusterConfig { name, region } = cluster;
        // Section order: the not-run records read in the order the config does.
        let not_run = [
            (!region.is_empty()).then_some(NotRun::Region),
            Some(read_topology(revoada)?),
            Some(read_teia(teia)),
        ]
        .into_iter()
        .flatten()
        .collect();
        read_consistency(consistency)?;

        let ControllersConfig {
            enable,
            namespace,
            fallback_interval_seconds,
            debounce_milliseconds,
        } = controllers;

        let NetworkingConfig {
            service_cidr,
            datapath_mode,
        } = networking;

        let RuntimeConfig {
            listen_addr,
            data_dir,
            durable,
            node_name,
            kubeconfig_publish_path,
            kubeconfig_publish_visibility,
            pod_kubeconfig_publish_path,
            advertise_address,
            remote_kubeconfig_publish_path,
            kubelet_listen_addr,
            etcd_listen_addr,
            kubelet_backend,
            podman_binary,
            host_path_allowlist,
            leadership_timeout_seconds,
            tls,
        } = runtime;

        Ok(Self {
            cluster_name: name.clone(),
            listen_addr: listen_addr.clone(),
            data_dir: data_dir.clone(),
            durable: *durable,
            node_name: node_name.clone(),
            kubeconfig_publish_path: kubeconfig_publish_path.clone(),
            kubeconfig_publish_visibility: *kubeconfig_publish_visibility,
            pod_kubeconfig_publish_path: pod_kubeconfig_publish_path.clone(),
            advertise_address: advertise_address.clone(),
            remote_kubeconfig_publish_path: remote_kubeconfig_publish_path.clone(),
            kubelet_listen_addr: kubelet_listen_addr.clone(),
            etcd_listen_addr: etcd_listen_addr.clone(),
            kubelet_backend: *kubelet_backend,
            podman_binary: podman_binary.clone(),
            host_path_allowlist: host_path_allowlist.clone(),
            leadership_timeout_seconds: *leadership_timeout_seconds,
            tls: ApiserverTls::read(tls)?,
            scheduler: scheduler.clone(),
            enable: enable.clone(),
            controllers_namespace: Some(namespace).filter(|n| !n.is_empty()).cloned(),
            fallback: Duration::from_secs(u64::from(*fallback_interval_seconds)),
            debounce: Duration::from_millis(u64::from(*debounce_milliseconds)),
            service_cidr: service_cidr.clone(),
            datapath_mode: *datapath_mode,
            not_run,
        })
    }

    /// The windows every tick loop is driven on and judged by, with the
    /// scheduler's own fallback: the one `Scheduler::from_config` resolved
    /// from `scheduler.tick_interval_seconds`.
    pub(crate) fn windows(&self, scheduler_fallback: Duration) -> Windows {
        Windows::new(self.fallback, self.debounce, scheduler_fallback)
    }
}

/// How the apiserver serves, from `runtime.tls`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ApiserverTls {
    /// `enabled: false`: plaintext. Nothing is issued, so the PKI fields do
    /// not apply.
    Plaintext,
    /// `enabled` with `auto_generate`: the cluster CA under
    /// `data_dir/pki`, loaded or minted, and a serving certificate issued
    /// from it at every boot.
    SelfIssued {
        /// `extra_sans`, as written; classified where the certificate is
        /// issued.
        extra_sans: Vec<String>,
    },
}

impl ApiserverTls {
    /// Read `tls`.
    ///
    /// # Errors
    ///
    /// [`Unhonoured::OperatorPki`] when TLS is on and the config names a
    /// PKI the runtime would not use: `auto_generate: false`, or an explicit
    /// path. The runtime only loads or mints the CA under `data_dir/pki`,
    /// so either would be answered with the generated CA, silently.
    fn read(tls: &TlsConfig) -> Result<Self, Unhonoured> {
        let TlsConfig {
            enabled,
            auto_generate,
            ca_cert_path,
            ca_key_path,
            cert_path,
            key_path,
            extra_sans,
        } = tls;
        if !*enabled {
            return Ok(Self::Plaintext);
        }
        if !*auto_generate {
            return Err(Unhonoured::OperatorPki {
                field: PkiField::AutoGenerateOff,
            });
        }
        for (field, path) in [
            (PkiField::CaCertPath, ca_cert_path),
            (PkiField::CaKeyPath, ca_key_path),
            (PkiField::CertPath, cert_path),
            (PkiField::KeyPath, key_path),
        ] {
            if path.is_some() {
                return Err(Unhonoured::OperatorPki { field });
            }
        }
        Ok(Self::SelfIssued {
            extra_sans: extra_sans.clone(),
        })
    }

    /// Whether the apiserver serves TLS.
    pub(crate) const fn is_enabled(&self) -> bool {
        matches!(self, Self::SelfIssued { .. })
    }
}

/// `revoada`: a formation one node already is is read and not run; one that
/// names several masters is refused.
fn read_topology(revoada: &RevoadaConfig) -> Result<NotRun, Unhonoured> {
    let RevoadaConfig { topology } = revoada;
    let TopologyConfig {
        strategy,
        // Both are reactions to membership changes. One in-process voter has
        // no membership to change, so neither can fire.
        min_nodes: _,
        grace_period_seconds: _,
    } = topology;
    match masters_named(*strategy) {
        Some(masters) => Err(Unhonoured::MultiNodeFormation {
            strategy: *strategy,
            masters,
        }),
        None => Ok(NotRun::Topology {
            strategy: *strategy,
        }),
    }
}

/// `teia`: read and not run.
fn read_teia(teia: &TeiaConfig) -> NotRun {
    let TeiaConfig {
        // The four describe a NATS connection nothing opens: the store's raft
        // transport is in-process (`boot_store`). I11 (T5.2) moves this
        // section behind a feature and replaces it with `fabric`.
        servers: _,
        cluster: _,
        credentials_path: _,
        connect_timeout_seconds: _,
    } = teia;
    NotRun::Teia
}

/// `consistency`: the store's own tier is run; any other is refused.
fn read_consistency(consistency: &ConsistencyConfig) -> Result<(), Unhonoured> {
    let ConsistencyConfig { default_tier } = consistency;
    match default_tier {
        // What the store is: every write a linearizable raft commit.
        ConsistencyTierKind::Strong => Ok(()),
        requested @ (ConsistencyTierKind::EventualGossip
        | ConsistencyTierKind::DurableStream
        | ConsistencyTierKind::Content) => Err(Unhonoured::ConsistencyTier {
            requested: *requested,
        }),
    }
}

/// How many masters `strategy` names by definition, when that is more than
/// the one voter this runtime runs; `None` for a formation one node already
/// is.
const fn masters_named(strategy: TopologyStrategyKind) -> Option<u32> {
    match strategy {
        // One node is a solo cluster, a mesh of one peer, and a phalanx of
        // ceil(2 * 1 / 5) = 1 master.
        TopologyStrategyKind::Solo
        | TopologyStrategyKind::MeshAllPeers
        | TopologyStrategyKind::Phalanx => None,
        TopologyStrategyKind::Pair => Some(2),
        TopologyStrategyKind::Quorum3M | TopologyStrategyKind::Cluster3MNW => Some(3),
    }
}

/// A PKI field the runtime does not serve from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PkiField {
    /// `runtime.tls.auto_generate: false`: an operator-supplied PKI.
    AutoGenerateOff,
    /// `runtime.tls.ca_cert_path`.
    CaCertPath,
    /// `runtime.tls.ca_key_path`.
    CaKeyPath,
    /// `runtime.tls.cert_path`.
    CertPath,
    /// `runtime.tls.key_path`.
    KeyPath,
}

impl fmt::Display for PkiField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::AutoGenerateOff => "runtime.tls.auto_generate: false",
            Self::CaCertPath => "runtime.tls.ca_cert_path",
            Self::CaKeyPath => "runtime.tls.ca_key_path",
            Self::CertPath => "runtime.tls.cert_path",
            Self::KeyPath => "runtime.tls.key_path",
        })
    }
}

/// A config field that asks for something this runtime does not do.
///
/// Refused at start, before anything is written: booting would run
/// something else under the operator's name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Unhonoured {
    /// TLS names a PKI other than the one the runtime mints under
    /// `data_dir/pki`. Serving the minted CA instead would hand clients a
    /// cluster identity the operator did not choose.
    #[error(
        "{field} is set, and this runtime serves only the CA it loads or mints under \
         data_dir/pki; unset it (with runtime.tls.auto_generate: true) or disable TLS"
    )]
    OperatorPki {
        /// The field that names the other PKI.
        field: PkiField,
    },
    /// `revoada.topology.strategy` names a formation of more than one
    /// master. This runtime is one in-process raft voter; the formation
    /// would not form, and nothing else would say so.
    #[error(
        "revoada.topology.strategy {strategy:?} names {masters} masters, and this runtime is \
         one in-process raft voter; use solo, phalanx or mesh_all_peers"
    )]
    MultiNodeFormation {
        /// The formation asked for.
        strategy: TopologyStrategyKind,
        /// How many masters it names.
        masters: u32,
    },
    /// `consistency.default_tier` names a tier the store does not serve.
    /// Every write is a linearizable raft commit; no gossip, stream or
    /// content-addressed tier exists behind the name.
    #[error(
        "consistency.default_tier {requested:?} is not served: the store runs strong \
         (linearizable raft) only"
    )]
    ConsistencyTier {
        /// The tier asked for.
        requested: ConsistencyTierKind,
    },
}

/// A config field the runtime reads and does not run, and why.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NotRun {
    /// `cluster.region`: nothing in a single-node runtime places or labels
    /// by region. Destination: the Node's `topology.kubernetes.io/region`
    /// label, once a label value is validated at the border.
    Region,
    /// `revoada.topology`: a formation one node already is. No topology
    /// strategy runs, and `min_nodes` and `grace_period_seconds` react to
    /// membership changes one voter never has.
    Topology {
        /// The formation named.
        strategy: TopologyStrategyKind,
    },
    /// `teia`: no NATS fabric runs. The store's raft transport is
    /// in-process.
    Teia,
}

impl NotRun {
    /// The config section the field lives in, for the boot log.
    pub(crate) const fn field(self) -> &'static str {
        match self {
            Self::Region => "cluster.region",
            Self::Topology { .. } => "revoada.topology",
            Self::Teia => "teia",
        }
    }
}

impl fmt::Display for NotRun {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Region => f.write_str("nothing in a single-node runtime places by region"),
            Self::Topology { strategy } => write!(
                f,
                "{strategy:?} is a formation one node already is; no topology strategy runs"
            ),
            Self::Teia => f.write_str("no NATS fabric runs; the store's raft is in-process"),
        }
    }
}

#[cfg(test)]
impl BootConfig {
    /// The prescribed default, read. Every test that needs a `BootConfig`
    /// and has no opinion starts here.
    pub(crate) fn prescribed() -> Self {
        use shikumi::TieredConfig as _;
        Self::read(&EngenhoConfig::prescribed_default()).expect("the prescribed default is run")
    }

    /// The windows the prescribed default runs on, the scheduler's fallback
    /// read as `Scheduler::from_config` reads it.
    pub(crate) fn prescribed_windows() -> Windows {
        let boot = Self::prescribed();
        let scheduler_fallback =
            Duration::from_secs(u64::from(boot.scheduler.tick_interval_seconds));
        boot.windows(scheduler_fallback)
    }
}

#[cfg(test)]
mod tests {
    use engenho_config::{ConsistencyTierKind, EngenhoConfig, TopologyStrategyKind};
    use shikumi::TieredConfig as _;

    use super::{ApiserverTls, BootConfig, NotRun, PkiField, Unhonoured};

    fn read(edit: impl FnOnce(&mut EngenhoConfig)) -> Result<BootConfig, Unhonoured> {
        let mut config = EngenhoConfig::prescribed_default();
        edit(&mut config);
        BootConfig::read(&config)
    }

    /// The prescribed default boots, and says what it reads and does not
    /// run: the region, the formation and the NATS section.
    #[test]
    fn the_prescribed_default_is_run_and_names_what_it_does_not_run() {
        let boot = BootConfig::prescribed();
        assert_eq!(
            boot.not_run,
            vec![
                NotRun::Region,
                NotRun::Topology {
                    strategy: TopologyStrategyKind::Phalanx
                },
                NotRun::Teia,
            ]
        );
        assert!(matches!(boot.tls, ApiserverTls::SelfIssued { .. }));
    }

    #[test]
    fn an_empty_region_is_not_recorded() {
        let boot = read(|c| c.cluster.region.clear()).expect("run");
        assert!(
            !boot.not_run.contains(&NotRun::Region),
            "{:?}",
            boot.not_run
        );
    }

    /// Before I21 the runtime minted its own CA whatever `runtime.tls`
    /// named, so an operator's PKI was answered with a different cluster
    /// identity, silently.
    #[test]
    fn an_operator_pki_is_refused_not_replaced_by_the_minted_ca() {
        let refused = |edit: fn(&mut EngenhoConfig)| read(edit).map(|_| ()).unwrap_err();
        assert_eq!(
            refused(|c| {
                c.runtime.tls.auto_generate = false;
                c.runtime.tls.ca_cert_path = Some("/pki/ca.crt".into());
                c.runtime.tls.ca_key_path = Some("/pki/ca.key".into());
                c.runtime.tls.cert_path = Some("/pki/srv.crt".into());
                c.runtime.tls.key_path = Some("/pki/srv.key".into());
            }),
            Unhonoured::OperatorPki {
                field: PkiField::AutoGenerateOff
            }
        );
        assert_eq!(
            refused(|c| c.runtime.tls.ca_cert_path = Some("/pki/ca.crt".into())),
            Unhonoured::OperatorPki {
                field: PkiField::CaCertPath
            }
        );
        assert_eq!(
            refused(|c| c.runtime.tls.key_path = Some("/pki/srv.key".into())),
            Unhonoured::OperatorPki {
                field: PkiField::KeyPath
            }
        );
    }

    /// With TLS off nothing is issued, so the PKI fields ask for nothing.
    #[test]
    fn plaintext_reads_no_pki_field() {
        let boot = read(|c| {
            c.runtime.tls.enabled = false;
            c.runtime.tls.auto_generate = false;
            c.runtime.tls.ca_cert_path = Some("/pki/ca.crt".into());
        })
        .expect("run");
        assert_eq!(boot.tls, ApiserverTls::Plaintext);
    }

    #[test]
    fn a_multi_master_formation_is_refused() {
        for (strategy, masters) in [
            (TopologyStrategyKind::Pair, 2),
            (TopologyStrategyKind::Quorum3M, 3),
            (TopologyStrategyKind::Cluster3MNW, 3),
        ] {
            assert_eq!(
                read(|c| {
                    c.revoada.topology.strategy = strategy;
                    c.revoada.topology.min_nodes = 3;
                })
                .map(|_| ())
                .unwrap_err(),
                Unhonoured::MultiNodeFormation { strategy, masters },
            );
        }
    }

    #[test]
    fn a_formation_one_node_already_is_is_run() {
        for strategy in [
            TopologyStrategyKind::Solo,
            TopologyStrategyKind::MeshAllPeers,
            TopologyStrategyKind::Phalanx,
        ] {
            let boot = read(|c| c.revoada.topology.strategy = strategy).expect("run");
            assert!(
                boot.not_run.contains(&NotRun::Topology { strategy }),
                "{:?}",
                boot.not_run
            );
        }
    }

    #[test]
    fn only_the_strong_tier_is_served() {
        for requested in [
            ConsistencyTierKind::EventualGossip,
            ConsistencyTierKind::DurableStream,
            ConsistencyTierKind::Content,
        ] {
            assert_eq!(
                read(|c| c.consistency.default_tier = requested)
                    .map(|_| ())
                    .unwrap_err(),
                Unhonoured::ConsistencyTier { requested },
            );
        }
    }

    /// An empty `controllers.namespace` is every namespace, never the
    /// namespace named "".
    #[test]
    fn an_empty_controllers_namespace_is_every_namespace() {
        assert_eq!(BootConfig::prescribed().controllers_namespace, None);
        let boot = read(|c| c.controllers.namespace = "team-a".into()).expect("run");
        assert_eq!(boot.controllers_namespace.as_deref(), Some("team-a"));
    }
}
