//! The closed catalog of every long-lived task the runtime spawns (T2.6).
//!
//! ## What this replaces
//!
//! `spawn_drivers` used to push twenty `WatchDriver::spawn()` handles into a
//! `Vec<JoinHandle<()>>` that nothing ever polled, and `start_inner` spawned
//! the :10250 and :2379 listeners with bare `tokio::spawn` and kept no handle
//! at all. A driver loop could RETURN — that is how the kubelet's controller
//! was retired for 12 hours on ryn (2026-09-18) — and a task could panic,
//! and in both cases nothing in the process noticed. The only evidence was a
//! log line that stopped appearing.
//!
//! ## What is now impossible, and what is only caught
//!
//! * A driver loop that returns: `WatchDriver::run` is `-> Infallible`, so it
//!   is a type error (E0308). A child's task ends only by panic or abort.
//! * A child with no name or no [`TickState`]: every fact about a child is
//!   an exhaustive `match` with no wildcard arm, so a new variant without
//!   its row is E0004. The no-wildcard rule is held by
//!   `clippy::wildcard_enum_match_arm` (denied in this module) and by a test
//!   that scans this file — a gate, not a type.
//! * A driver whose controller declares no reads (T1.7): a driver's wake
//!   filter is not a row here. The runtime derives it from the controller's
//!   declared reads ([`engenho_controllers::DeclaresReads`]) and drives no
//!   controller without them (E0277). That the filter really is built from
//!   the declaration, and that the declaration names what the controller's
//!   source reads, is a test, not a type: each spawned driver's [`Wiring`]
//!   records both halves so the test can check the drivers actually
//!   spawned.
//! * A driver or listener that is declared but never walked: [`Driver::ALL`]
//!   and [`Listener::ALL`] are generated from the enums' own variant lists by
//!   `closed_enum!`. The three top-level shapes are walked by
//!   [`Child::all`]; a new top-level shape is caught by a test, not a type.
//! * A child that dies unseen: every child's task is owned by one
//!   [`Children`] set. [`Children::next_dead`] is how `main` learns that one
//!   ended; it is marked [`ChildState::Dead`] and logged at ERROR. Nothing
//!   respawns it on its own: an operator does ([`crate::Runtime::respawn`],
//!   `engenho ctl children restart`), by [`Child::respawn`]'s row.
//! * A child with two tasks: [`Children::spawn_one`] refuses a child that
//!   runs, so a respawn stops it first ([`Children::stop_one`]) — a type
//!   (`Result`) the caller cannot ignore, not a convention.
//! * A child built before the sibling it watches: the walk hands each
//!   child's builder the children spawned so far, and the one child that
//!   watches another (the node lease, which watches the kubelet) is walked
//!   after every driver. That the kubelet comes first is a test over
//!   [`Child::all`], not a type.
//!
//! ## What a panic does (T2.7)
//!
//! Decided per child by its [`TickState`], which the runtime hands its
//! driver: a Stateless driver contains a panicking tick, counts it and is
//! re-ticked by the next event or the fallback, so its task never ends; a
//! Stateful driver's panic ends its task, and the set above marks it Dead —
//! for the kubelet, Dead is the park. A listener whose serve ends, or
//! panics, rebinds on a growing backoff: an attempt holds nothing across
//! attempts. See [`engenho_controllers::contain`].
//!
//! ## What every fault does (W6)
//!
//! [`Child::supervision`] declares, for every child and every [`Fault`] — a
//! panic, a hang past the stuck window, an error return, a bind failure —
//! what the supervisor does about it: a [`Supervision`]. It is derived from
//! the rows above (a tick loop's [`TickState`], a listener's row), never
//! written a second time. The fault-injection matrix (`fault_matrix`, a
//! test) strikes every child in [`Child::all`] with every fault and holds
//! the runtime to that declaration. A new child shape or fault with no row
//! is E0004 in the declaration and in the matrix's injector; a new driver
//! or listener joins the matrix through [`Child::all`] with no new line.
//!
//! A tick loop's [`TickState`] reaches its driver through
//! [`TickLoop::tick_state`]: the runtime's one driving function takes the
//! tick loop, not a tick state, so no call to it can drive a loop by a row
//! other than its own. (A `WatchDriver` built by hand, outside that
//! function, bypasses the catalog altogether — the same gap as a task
//! spawned outside it, below.)
//!
//! Still only caught: a task spawned OUTSIDE this catalog (T0.4's
//! `disallowed-methods` on every spawn path, once clippy blocks).

#![deny(clippy::wildcard_enum_match_arm)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use engenho_config::ControllerEnable;
pub use engenho_controllers::TickState;
use engenho_controllers::{ControllerType, Heartbeat, KindFilter, PanicMessage, Reads};
use tokio::task::{AbortHandle, Id, JoinSet};
use tracing::{error, info};

use crate::boot::Timestamp;
use crate::boot_config::BootConfig;
use crate::health::{Row, Tally};

engenho_controllers::closed_enum! {
    /// Every controller loop the runtime runs, each behind a
    /// `WatchDriver`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub enum Driver {
        Deployment,
        ReplicaSet,
        StatefulSet,
        DaemonSet,
        Job,
        CronJob,
        PodDisruptionBudget,
        Endpoints,
        ServiceRouting,
        Gc,
        Namespace,
        PvBinder,
        VolumeSnapshot,
        PvcProtection,
        Crd,
        Scheduler,
        ServedCapability,
        NetworkPolicy,
        CsiRegistrar,
        CniStatus,
        Kubelet,
    }
}

engenho_controllers::closed_enum! {
    /// Every network listener the runtime binds besides the apiserver
    /// (which `ApiServer` owns and stops itself).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub enum Listener {
        /// The kubelet's own HTTP surface, upstream's :10250.
        KubeletHttp,
        /// The read-only etcd v3 façade, upstream's :2379.
        EtcdFacade,
    }
}

/// One long-lived task the runtime owns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Child {
    /// A controller loop behind a `WatchDriver`.
    Driver(Driver),
    /// A network listener.
    Listener(Listener),
    /// The node-lease renewal task (T1.3c): renews this node's Lease every
    /// renew interval while, and only while, the kubelet's liveness row is
    /// alive, so a kubelet that wedges or dies stops heartbeating and the
    /// node reads `NotReady`. Its body is the runtime's `node_lease` module.
    NodeLease,
}

impl Driver {
    /// Stable name, for logs and liveness rows.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Deployment => "deployment",
            Self::ReplicaSet => "replicaset",
            Self::StatefulSet => "statefulset",
            Self::DaemonSet => "daemonset",
            Self::Job => "job",
            Self::CronJob => "cronjob",
            Self::PodDisruptionBudget => "pdb",
            Self::Endpoints => "endpoints",
            Self::ServiceRouting => "service-routing",
            Self::Gc => "gc",
            Self::Namespace => "namespace",
            Self::PvBinder => "pv-binder",
            Self::VolumeSnapshot => "volume-snapshot",
            Self::PvcProtection => "pvc-protection",
            Self::Crd => "crd",
            Self::Scheduler => "scheduler",
            Self::ServedCapability => "served-capability",
            Self::NetworkPolicy => "network-policy",
            Self::CsiRegistrar => "csi-registrar",
            Self::CniStatus => "cni-status",
            Self::Kubelet => "kubelet",
        }
    }

    /// Whether a tick leaves state the next tick relies on. See [`TickState`].
    #[must_use]
    pub const fn tick_state(self) -> TickState {
        match self {
            Self::Deployment
            | Self::ReplicaSet
            | Self::StatefulSet
            | Self::DaemonSet
            | Self::Job
            | Self::CronJob
            | Self::PodDisruptionBudget
            | Self::Endpoints
            | Self::Gc
            | Self::Namespace
            | Self::PvBinder
            | Self::VolumeSnapshot
            | Self::PvcProtection
            | Self::ServedCapability
            | Self::CniStatus => TickState::Stateless,
            // The `local` pod map lives across ticks.
            Self::Kubelet
            // The registered-handler map (a std Mutex) drives
            // unregister-on-delete.
            | Self::Crd
            // The router backend holds the routes it installed.
            | Self::ServiceRouting
            // The enforcer holds the rules it computed.
            | Self::NetworkPolicy
            // The driver table the binder and the kubelet also read.
            | Self::CsiRegistrar
            // The round-robin cursor sits behind a std Mutex; a panic while
            // holding it poisons every later tick.
            | Self::Scheduler => TickState::Stateful,
        }
    }

    /// The `controllers.enable` switch that turns this driver on; `None` for
    /// a driver with no switch, which always runs.
    #[must_use]
    pub const fn switch(self) -> Option<EnableSwitch> {
        use EnableSwitch as S;
        match self {
            Self::Deployment => Some(S::Deployment),
            Self::ReplicaSet => Some(S::Replicaset),
            Self::StatefulSet => Some(S::Statefulset),
            Self::DaemonSet => Some(S::Daemonset),
            Self::Job => Some(S::Job),
            Self::CronJob => Some(S::Cronjob),
            Self::PodDisruptionBudget => Some(S::Pdb),
            Self::Endpoints => Some(S::Endpoints),
            Self::ServiceRouting => Some(S::ServiceRouting),
            Self::Gc => Some(S::Gc),
            Self::Namespace => Some(S::Namespace),
            // The snapshot controller snapshots the directories the binder
            // provisions; enabling one without the other yields a controller
            // that can only ever decline. pvc-protection guards the claims the
            // binder binds, so it runs whenever the binder does.
            Self::PvBinder | Self::VolumeSnapshot | Self::PvcProtection => Some(S::PvBinder),
            Self::Crd => Some(S::Crd),
            Self::Scheduler
            | Self::ServedCapability
            | Self::NetworkPolicy
            | Self::CsiRegistrar
            | Self::CniStatus
            | Self::Kubelet => None,
        }
    }

    /// Whether `controllers.enable` turns this driver on. The drivers with
    /// no switch always run.
    #[must_use]
    pub const fn enabled(self, enable: &ControllerEnable) -> bool {
        match self.switch() {
            Some(switch) => switch.read(enable),
            None => true,
        }
    }
}

engenho_controllers::closed_enum! {
    /// One `controllers.enable` switch: the leaf the control plane toggles a
    /// driver by ([`Driver::switch`]).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub enum EnableSwitch {
        /// `replicaset`.
        Replicaset,
        /// `deployment`.
        Deployment,
        /// `statefulset`.
        Statefulset,
        /// `daemonset`.
        Daemonset,
        /// `job`.
        Job,
        /// `cronjob`.
        Cronjob,
        /// `endpoints`.
        Endpoints,
        /// `service_routing`.
        ServiceRouting,
        /// `gc`.
        Gc,
        /// `crd`.
        Crd,
        /// `namespace`.
        Namespace,
        /// `pv_binder`: the binder, the snapshot controller and pvc-protection.
        PvBinder,
        /// `pdb`.
        Pdb,
    }
}

impl EnableSwitch {
    /// The switch whose leaf is `path`.
    #[must_use]
    pub fn of_leaf(path: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|switch| switch.leaf() == path)
    }

    /// The drivers it turns on and off.
    pub fn drivers(self) -> impl Iterator<Item = Driver> {
        Driver::ALL
            .iter()
            .copied()
            .filter(move |driver| driver.switch() == Some(self))
    }

    /// Whether flipping it can be applied to a running runtime: every driver
    /// it gates can be spawned and stopped alone ([`Child::respawn`]). A
    /// switch that gates one that cannot waits for a runtime restart.
    #[must_use]
    pub fn toggles_live(self) -> bool {
        self.drivers()
            .all(|driver| Child::Driver(driver).respawn() != Respawn::RuntimeRestartOnly)
    }

    /// The switch's value in `enable`.
    ///
    /// The one reader of `controllers.enable`, destructured with no `..`
    /// (I21): a new switch is E0027 here until it is named — and, being a
    /// new variant, E0004 in [`Self::leaf`] until it has a leaf.
    #[must_use]
    pub const fn read(self, enable: &ControllerEnable) -> bool {
        let ControllerEnable {
            replicaset,
            deployment,
            statefulset,
            daemonset,
            job,
            cronjob,
            endpoints,
            service_routing,
            gc,
            crd,
            namespace,
            pv_binder,
            pdb,
        } = enable;
        *match self {
            Self::Replicaset => replicaset,
            Self::Deployment => deployment,
            Self::Statefulset => statefulset,
            Self::Daemonset => daemonset,
            Self::Job => job,
            Self::Cronjob => cronjob,
            Self::Endpoints => endpoints,
            Self::ServiceRouting => service_routing,
            Self::Gc => gc,
            Self::Crd => crd,
            Self::Namespace => namespace,
            Self::PvBinder => pv_binder,
            Self::Pdb => pdb,
        }
    }

    /// Its configuration leaf, dotted.
    #[must_use]
    pub const fn leaf(self) -> &'static str {
        match self {
            Self::Replicaset => "controllers.enable.replicaset",
            Self::Deployment => "controllers.enable.deployment",
            Self::Statefulset => "controllers.enable.statefulset",
            Self::Daemonset => "controllers.enable.daemonset",
            Self::Job => "controllers.enable.job",
            Self::Cronjob => "controllers.enable.cronjob",
            Self::Endpoints => "controllers.enable.endpoints",
            Self::ServiceRouting => "controllers.enable.service_routing",
            Self::Gc => "controllers.enable.gc",
            Self::Crd => "controllers.enable.crd",
            Self::Namespace => "controllers.enable.namespace",
            Self::PvBinder => "controllers.enable.pv_binder",
            Self::Pdb => "controllers.enable.pdb",
        }
    }
}

impl Listener {
    /// Stable name, for logs and liveness rows.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::KubeletHttp => "kubelet-http",
            Self::EtcdFacade => "etcd-facade",
        }
    }

    /// What the supervisor does when `fault` strikes this listener.
    ///
    /// Every listener runs one bind-and-serve attempt at a time and builds
    /// each attempt afresh from what it was spawned with; what it serves
    /// (the kubelet, the store) it reaches only from request tasks, which
    /// its server spawns. So an attempt that fails, returns or panics
    /// leaves nothing torn behind, and the listener binds again.
    #[must_use]
    pub const fn supervision(self, fault: Fault) -> Supervision {
        match self {
            Self::KubeletHttp | Self::EtcdFacade => match fault {
                Fault::Panic | Fault::Error | Fault::BindFailure => Supervision::Rebinds,
                Fault::Hang => Supervision::Unobserved,
            },
        }
    }

    /// Whether the config binds this listener. An empty `etcd_listen_addr`
    /// disables the façade.
    #[must_use]
    pub(crate) fn enabled(self, boot: &BootConfig) -> bool {
        match self {
            Self::KubeletHttp => true,
            Self::EtcdFacade => !boot.etcd_listen_addr.is_empty(),
        }
    }

    /// The address the config binds it at.
    #[must_use]
    pub(crate) fn listen_addr(self, boot: &BootConfig) -> &str {
        match self {
            Self::KubeletHttp => &boot.kubelet_listen_addr,
            Self::EtcdFacade => &boot.etcd_listen_addr,
        }
    }
}

impl Child {
    /// The node lease's [`TickState`], its row in the catalog: it renews from
    /// the kubelet's heartbeat and holds nothing across ticks, so a check
    /// that panics is contained and re-ticked. The runtime drives the lease
    /// with this value; [`Child::tick_state`] reports it.
    pub const NODE_LEASE_TICK_STATE: TickState = TickState::Stateless;

    /// Every child, each exactly once: the drivers, then the listeners, then
    /// the node-lease task. The lease comes last because it watches the
    /// kubelet's row, so the kubelet is spawned before it is built.
    pub fn all() -> impl Iterator<Item = Self> {
        // The walk below names each top-level shape once. This match is the
        // reminder beside it: a new shape is E0004 here, one screen from the
        // list it must join. The test `all_walks_every_child_once` catches
        // one that is added here and not below.
        const fn _every_shape_is_walked(child: Child) {
            match child {
                Child::Driver(_) | Child::Listener(_) | Child::NodeLease => {}
            }
        }
        Driver::ALL
            .iter()
            .copied()
            .map(Self::Driver)
            .chain(Listener::ALL.iter().copied().map(Self::Listener))
            .chain(std::iter::once(Self::NodeLease))
    }

    /// Stable name, for logs and liveness rows.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Driver(d) => d.name(),
            Self::Listener(l) => l.name(),
            Self::NodeLease => "node-lease",
        }
    }

    /// The child's [`TickState`]; `None` for a listener, which serves rather
    /// than ticks.
    #[must_use]
    pub const fn tick_state(self) -> Option<TickState> {
        match self.tick_loop() {
            Some(tick_loop) => Some(tick_loop.tick_state()),
            None => None,
        }
    }

    /// The child as a tick loop; `None` for a listener.
    #[must_use]
    pub(crate) const fn tick_loop(self) -> Option<TickLoop> {
        match self {
            Self::Driver(d) => Some(TickLoop::Driver(d)),
            Self::NodeLease => Some(TickLoop::NodeLease),
            Self::Listener(_) => None,
        }
    }

    /// What the supervisor does when `fault` strikes this child (W6).
    ///
    /// Derived from the child's own rows — a tick loop's [`TickState`], a
    /// listener's [`Listener::supervision`] — so it cannot disagree with
    /// what the runtime drives the child by. The fault-injection matrix
    /// holds the runtime to it.
    #[must_use]
    pub const fn supervision(self, fault: Fault) -> Supervision {
        match self {
            Self::Driver(d) => TickLoop::Driver(d).supervision(fault),
            Self::NodeLease => TickLoop::NodeLease.supervision(fault),
            Self::Listener(l) => l.supervision(fault),
        }
    }

    /// Whether this config spawns the child.
    #[must_use]
    pub(crate) fn enabled(self, boot: &BootConfig) -> bool {
        match self {
            Self::Driver(d) => d.enabled(&boot.enable),
            Self::Listener(l) => l.enabled(boot),
            // The lease proves the kubelet alive: with no kubelet there is
            // nothing for it to renew by.
            Self::NodeLease => Driver::Kubelet.enabled(&boot.enable),
        }
    }

    /// Whether a child both `before` and `after` spawn is built differently
    /// by them, and so is rebuilt to follow a change applied in place: a
    /// listener moved to a new address. No driver's body reads a leaf the
    /// control plane applies in place (those are the kubeconfig publish
    /// leaves, the switches, and the listen addresses).
    #[must_use]
    pub(crate) fn rebuilt_between(self, before: &BootConfig, after: &BootConfig) -> bool {
        match self {
            Self::Listener(l) => l.listen_addr(before) != l.listen_addr(after),
            Self::Driver(_) | Self::NodeLease => false,
        }
    }

    /// How this child is brought back on its own, without restarting the
    /// runtime.
    ///
    /// A stateless loop, a listener and the lease rebuild from what they
    /// were spawned with. The kubelet's pod map is its own, but the lease
    /// and its HTTP listener read the kubelet they were built against, so
    /// they are rebuilt with it. A child whose state other parts of the
    /// runtime also hold (the CRD handler table in the router, the routes
    /// the service router installed, the CSI driver table) is not rebuilt
    /// alone until it is shown to resync from scratch: a stated limit.
    #[must_use]
    pub const fn respawn(self) -> Respawn {
        match self {
            Self::Driver(d) => match d {
                Driver::Deployment
                | Driver::ReplicaSet
                | Driver::StatefulSet
                | Driver::DaemonSet
                | Driver::Job
                | Driver::CronJob
                | Driver::PodDisruptionBudget
                | Driver::Endpoints
                | Driver::Gc
                | Driver::Namespace
                | Driver::PvBinder
                | Driver::VolumeSnapshot
                | Driver::PvcProtection
                | Driver::ServedCapability
                | Driver::CniStatus
                | Driver::Scheduler
                | Driver::NetworkPolicy => Respawn::Rebuild,
                Driver::Kubelet => {
                    Respawn::RebuildWith(&[Self::NodeLease, Self::Listener(Listener::KubeletHttp)])
                }
                Driver::Crd | Driver::ServiceRouting | Driver::CsiRegistrar => {
                    Respawn::RuntimeRestartOnly
                }
            },
            Self::Listener(Listener::KubeletHttp | Listener::EtcdFacade) | Self::NodeLease => {
                Respawn::Rebuild
            }
        }
    }
}

/// How a child is brought back on its own ([`Child::respawn`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Respawn {
    /// Built again from what it was spawned with.
    Rebuild,
    /// Built again, and these dependents with it, after it.
    RebuildWith(&'static [Child]),
    /// Only a runtime restart brings it back.
    RuntimeRestartOnly,
}

impl fmt::Display for Child {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A child whose body is a tick loop behind a `WatchDriver`: a driver, or
/// the node lease. Every child but a listener.
///
/// The runtime's one driving function takes this, not a [`TickState`]: the
/// tick state a loop is driven by is read off its catalog row here, so it
/// cannot be handed a different one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TickLoop {
    /// A controller loop.
    Driver(Driver),
    /// The node-lease renewal loop.
    NodeLease,
}

impl TickLoop {
    /// The catalog child this loop is.
    #[must_use]
    pub(crate) const fn child(self) -> Child {
        match self {
            Self::Driver(d) => Child::Driver(d),
            Self::NodeLease => Child::NodeLease,
        }
    }

    /// The loop's catalog row: what a panic in its tick does.
    #[must_use]
    pub(crate) const fn tick_state(self) -> TickState {
        match self {
            Self::Driver(d) => d.tick_state(),
            Self::NodeLease => Child::NODE_LEASE_TICK_STATE,
        }
    }

    /// What the supervisor does when `fault` strikes this loop.
    const fn supervision(self, fault: Fault) -> Supervision {
        match fault {
            Fault::Panic => match self.tick_state() {
                TickState::Stateless => Supervision::Contained,
                TickState::Stateful => Supervision::Dead,
            },
            // The tick is never cancelled (`WatchDriverConfig::stuck_tick_after`):
            // cancelling mid-tick strands whatever the tick already did.
            Fault::Hang => Supervision::Stalled,
            Fault::Error => Supervision::Retried,
            Fault::BindFailure => Supervision::Inapplicable,
        }
    }
}

engenho_controllers::closed_enum! {
    /// A fault the supervisor must answer for, in any child (W6).
    ///
    /// The fault-injection matrix strikes every child in [`Child::all`] with
    /// every one of these; [`Child::supervision`] says what must happen.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub enum Fault {
        /// The child's unit of work panics: a tick loop's tick, a
        /// listener's bind-and-serve attempt.
        Panic,
        /// The unit of work never ends: a tick in flight past the stuck-tick
        /// window, a serve attempt that never returns.
        Hang,
        /// The unit of work fails and returns: a tick's Transient `Err`, a
        /// listener's server returning.
        Error,
        /// The child cannot bind its port.
        BindFailure,
    }
}

engenho_controllers::closed_enum! {
    /// What the supervisor does when a [`Fault`] strikes a child, as the
    /// catalog declares it ([`Child::supervision`]). Each variant says what is
    /// observable afterwards, because that is what the matrix checks.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub enum Supervision {
        /// Contained: each panic is counted in the child's heartbeat, the child
        /// keeps running, and the next event or fallback ticks it again.
        /// Liveness reads it alive. A Stateless tick loop's panic.
        Contained,
        /// The child's task ends: [`Children::next_dead`] reports it panicked,
        /// it is marked Dead after one tick, and liveness reads it Dead. It is
        /// never re-ticked or respawned. A Stateful tick loop's panic: a tick
        /// over the state the panic tore would act on something no longer true.
        Dead,
        /// The failure is classified in the child's heartbeat, the child keeps
        /// running, and it ticks again (a Transient failure on the retry curve,
        /// or sooner on the fallback). Liveness reads it alive: a failing
        /// controller is still ticking, and its failures are counted in its
        /// reconcile metrics, not in its liveness.
        Retried,
        /// The tick is never cancelled and no second tick starts beside it: the
        /// child keeps running with one tick in flight, and liveness reads it
        /// stalled since the tick began once the stuck window has passed.
        Stalled,
        /// The listener stops serving and says so — its heartbeat is not in
        /// flight and liveness reads it stalled — and it binds again on the
        /// rebind curve. Its task keeps running. A panic is counted too.
        Rebinds,
        /// The supervisor cannot see this fault. A listener's serve in flight is
        /// read as serving, so a serve that never returns reads alive whether or
        /// not it accepts anything. A named blind spot, not a verdict of health:
        /// seeing it needs a probe of the port itself.
        Unobserved,
        /// The fault cannot strike this child: a tick loop binds no port.
        Inapplicable,
    }
}

/// Which controller a tick loop (a driver, or the node lease) runs, what it
/// reads, and which events wake it, as the runtime wired it (T1.7, T5.11).
///
/// The runtime builds `wakes` from `reads` and from nothing else; both are
/// recorded so the claim "a driver wakes on every kind its controller reads"
/// can be checked against the drivers actually spawned, not against the
/// function that is supposed to build them. `controller` is recorded for
/// the same reason: "every controller type is spawned or dormant"
/// ([`crate::Dormant`]) is checked against the types the spawned drivers
/// really run.
#[derive(Debug, Clone)]
pub struct Wiring {
    controller: ControllerType,
    reads: Reads,
    wakes: KindFilter,
}

impl Wiring {
    /// Record the controller a driver runs, its declared reads, and the
    /// filter its driver got.
    pub(crate) fn new(controller: ControllerType, reads: Reads, wakes: KindFilter) -> Self {
        Self {
            controller,
            reads,
            wakes,
        }
    }

    /// The type that does the driver's reconciling (never an adapter
    /// around it).
    #[must_use]
    pub fn controller(&self) -> ControllerType {
        self.controller
    }

    /// Every kind the controller declares it reads.
    #[must_use]
    pub fn reads(&self) -> &Reads {
        &self.reads
    }

    /// Which events wake the driver.
    #[must_use]
    pub fn wakes(&self) -> &KindFilter {
        &self.wakes
    }
}

/// A child's body, ready to spawn: the future, the heartbeat it writes and,
/// for a driver, its [`Wiring`] and the [`Tally`] its ticks are counted in.
pub(crate) struct ChildTask {
    beat: Arc<Heartbeat>,
    wiring: Option<Wiring>,
    tally: Option<Arc<Tally>>,
    run: Pin<Box<dyn Future<Output = Infallible> + Send + 'static>>,
}

impl ChildTask {
    /// A body that never returns, recording into `beat`. For a child that
    /// does not run a controller (a listener).
    pub(crate) fn new(
        beat: Arc<Heartbeat>,
        run: impl Future<Output = Infallible> + Send + 'static,
    ) -> Self {
        Self {
            beat,
            wiring: None,
            tally: None,
            run: Box::pin(run),
        }
    }

    /// A tick loop's body (a driver, or the node lease): as [`Self::new`],
    /// plus what wakes it and why, and the tally its controller's ticks are
    /// counted in.
    pub(crate) fn driver(
        beat: Arc<Heartbeat>,
        wiring: Wiring,
        tally: Arc<Tally>,
        run: impl Future<Output = Infallible> + Send + 'static,
    ) -> Self {
        Self {
            beat,
            wiring: Some(wiring),
            tally: Some(tally),
            run: Box::pin(run),
        }
    }
}

/// How a child's task ended. There is no third way: its output is
/// `Infallible`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeathCause {
    /// The task panicked.
    Panicked,
    /// The task was aborted.
    Cancelled,
}

impl fmt::Display for DeathCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Panicked => "panicked",
            Self::Cancelled => "cancelled",
        })
    }
}

/// What the supervisor knows about a spawned child's task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChildState {
    /// Its task has not ended. Whether it is making progress is the
    /// heartbeat's to say, not this.
    Running,
    /// Its task ended. Nothing respawns it but an operator
    /// (`engenho ctl children restart`).
    Dead(DeathCause),
}

/// How and when a child's task last ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Death {
    /// When the supervisor saw it end.
    pub at: Timestamp,
    /// How.
    pub cause: DeathCause,
}

/// Whether a task that ended by `cause` ended because it was being stopped
/// (`stopping`): only an abort asked for is a stop. A child being stopped
/// that panicked first still died, and is logged as a death.
const fn stopped_on_purpose(stopping: bool, cause: DeathCause) -> bool {
    stopping && matches!(cause, DeathCause::Cancelled)
}

/// A child is already running: stop it before spawning it again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{0} is running")]
pub struct StillRunning(pub Child);

/// A child built again ([`crate::Runtime::respawn`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Respawned {
    /// The child asked for.
    pub child: Child,
    /// Its generation now.
    pub generation: u64,
    /// Every child rebuilt: it first, then its dependents.
    pub rebuilt: Vec<Child>,
    /// Children that died on their own while these were stopped: the
    /// caller's to report, as [`Children::next_dead`]'s are.
    pub died: Vec<DeadChild>,
}

/// Why a child was not built again.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RespawnError {
    /// Only a runtime restart rebuilds it ([`Respawn::RuntimeRestartOnly`]).
    #[error("{0} is rebuilt only by restarting the runtime")]
    RuntimeRestartOnly(Child),
    /// The configuration does not spawn it.
    #[error("{0} is not spawned: the configuration disables it")]
    NotSpawned(Child),
    /// What it shares with the runtime could not be built again; it was
    /// left as it was.
    #[error("{child} could not be built again: {reason}")]
    Build {
        /// Which.
        child: Child,
        /// Why.
        reason: String,
    },
}

/// What bringing the children to a configuration applied in place did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChildrenFollowed {
    /// Every child stopped, spawned or rebuilt.
    pub changed: Vec<Child>,
    /// Children that died on their own meanwhile.
    pub died: Vec<DeadChild>,
}

/// A child whose task ended, as [`Children::next_dead`] reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "a dead child is already marked Dead and logged; this is the notice"]
pub struct DeadChild {
    pub child: Child,
    pub cause: DeathCause,
}

/// A spawned child's entry: the heartbeat it writes and what the supervisor
/// has seen of its task.
///
/// This is the plan's `DriverHandle { join, beat }`, with the join half held
/// by the [`Children`] set rather than by the entry: one set is what lets
/// `main` wait on every child at once.
#[doc(alias = "DriverHandle")]
#[derive(Debug, Clone)]
pub struct ChildHandle {
    beat: Arc<Heartbeat>,
    wiring: Option<Wiring>,
    /// The task's handle: whether it has ended is read off it directly, so
    /// liveness sees a dead task before [`Children::next_dead`] is polled.
    task: AbortHandle,
    tally: Option<Arc<Tally>>,
    state: ChildState,
    /// When the task was spawned.
    spawned_at: Timestamp,
    /// When the supervisor saw it end.
    ended_at: Option<Timestamp>,
    /// How many times it has been spawned again since the first (0: never).
    generation: u64,
    /// How the latest of its tasks that ended ended — kept across respawns.
    last_death: Option<Death>,
}

impl ChildHandle {
    /// How many times the child has been spawned again since the first.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// How its latest task that ended ended; `None` if none ever has.
    #[must_use]
    pub const fn last_death(&self) -> Option<Death> {
        self.last_death
    }

    /// When the task was spawned.
    #[must_use]
    pub const fn spawned_at(&self) -> Timestamp {
        self.spawned_at
    }

    /// When the supervisor saw the task end; `None` while it runs.
    #[must_use]
    pub const fn ended_at(&self) -> Option<Timestamp> {
        self.ended_at
    }

    /// The heartbeat the child writes.
    #[must_use]
    pub fn beat(&self) -> &Arc<Heartbeat> {
        &self.beat
    }

    /// What the child's controller reads and what wakes it; `None` for a
    /// child that runs no controller (a listener).
    #[must_use]
    pub fn wiring(&self) -> Option<&Wiring> {
        self.wiring.as_ref()
    }

    /// What the supervisor has seen of the child's task.
    #[must_use]
    pub const fn state(&self) -> ChildState {
        self.state
    }

    /// This entry as health reads it: clones of its handles.
    fn row(&self, child: Child) -> Row {
        Row::new(
            child,
            self.beat.clone(),
            self.task.clone(),
            self.tally.clone(),
        )
    }
}

/// The runtime's owned children: one task set, and which child each task is.
///
/// Built by [`Children::spawn_catalog`], which walks [`Child::all`] once.
/// After that a child is spawned again only by [`Children::spawn_one`], which
/// refuses one still running — so no child ever has two tasks — and none is
/// spawned from outside the catalog. Dropping the set aborts every task in it.
#[derive(Debug, Default)]
pub struct Children {
    set: JoinSet<Infallible>,
    by_task: HashMap<Id, Child>,
    entries: BTreeMap<Child, ChildHandle>,
    /// Children being stopped on purpose: their end is logged as a stop, not
    /// as a death.
    stopping: BTreeSet<Child>,
}

impl Children {
    /// Walk the catalog once, spawning every child `boot` enables with the
    /// body `build` returns for it.
    ///
    /// `build` is handed the children spawned so far, so a child can watch
    /// a sibling walked before it: the node lease reads the kubelet's
    /// [`Row`]. `build` returning `None` leaves a child unspawned.
    pub(crate) fn spawn_catalog(
        boot: &BootConfig,
        mut build: impl FnMut(Child, &Self) -> Option<ChildTask>,
    ) -> Self {
        let mut children = Self::default();
        for child in Child::all().filter(|c| c.enabled(boot)) {
            if let Some(task) = build(child, &children) {
                children.spawn(child, task);
            }
        }
        children
    }

    /// Spawn one child for the first time. Private: only
    /// [`Self::spawn_catalog`] (which visits each child once) and this
    /// module's tests reach it.
    fn spawn(&mut self, child: Child, task: ChildTask) {
        self.install(child, task, 0, None);
    }

    /// Spawn `child` — for the first time (a driver enabled after boot), or
    /// again after its task ended — keeping its last death, one generation
    /// on. Returns its generation.
    ///
    /// # Errors
    ///
    /// [`StillRunning`]: a child has one task at a time.
    pub(crate) fn spawn_one(&mut self, child: Child, task: ChildTask) -> Result<u64, StillRunning> {
        let (generation, last_death) = match self.entries.get(&child) {
            Some(entry) if entry.state == ChildState::Running => return Err(StillRunning(child)),
            Some(entry) => (entry.generation.saturating_add(1), entry.last_death),
            None => (0, None),
        };
        self.install(child, task, generation, last_death);
        Ok(generation)
    }

    fn install(
        &mut self,
        child: Child,
        task: ChildTask,
        generation: u64,
        last_death: Option<Death>,
    ) {
        let abort = self.set.spawn(task.run);
        self.by_task.insert(abort.id(), child);
        self.entries.insert(
            child,
            ChildHandle {
                beat: task.beat,
                wiring: task.wiring,
                task: abort,
                tally: task.tally,
                state: ChildState::Running,
                spawned_at: Timestamp::now(),
                ended_at: None,
                generation,
                last_death,
            },
        );
    }

    /// Stop `child`: abort its task and wait for it to end; it is then Dead
    /// (cancelled). Returns every child whose task ended meanwhile — `child`
    /// itself, and any other that happened to die, which is marked and
    /// logged as [`Self::next_dead`] would. Empty when `child` was not
    /// running.
    pub(crate) async fn stop_one(&mut self, child: Child) -> Vec<DeadChild> {
        let Some(entry) = self.entries.get(&child) else {
            return Vec::new();
        };
        if entry.state != ChildState::Running {
            return Vec::new();
        }
        self.stopping.insert(child);
        entry.task.abort();
        let mut ended = Vec::new();
        loop {
            let dead = self.next_dead().await;
            ended.push(dead);
            if dead.child == child {
                return ended;
            }
        }
    }

    /// Forget a child that is not running, as if the config had never
    /// enabled it (a driver disabled after boot). Its health row goes with
    /// it.
    ///
    /// # Errors
    ///
    /// [`StillRunning`]: stop it first.
    pub(crate) fn forget(&mut self, child: Child) -> Result<(), StillRunning> {
        match self.entries.get(&child) {
            Some(entry) if entry.state == ChildState::Running => Err(StillRunning(child)),
            Some(_) | None => {
                self.entries.remove(&child);
                Ok(())
            }
        }
    }

    /// Every spawned child as health reads it, in catalog order: what it
    /// beats into, its task's handle and, for a driver, its tally. Clones
    /// of handles, so health reads them without borrowing the set.
    pub(crate) fn rows(&self) -> Vec<Row> {
        self.iter().map(|(child, entry)| entry.row(child)).collect()
    }

    /// `child`'s row, as [`Self::rows`] builds it; `None` if it was never
    /// spawned.
    pub(crate) fn row(&self, child: Child) -> Option<Row> {
        self.get(child).map(|entry| entry.row(child))
    }

    /// Wait for the next child whose task ends, mark it
    /// [`ChildState::Dead`], count a panic in its heartbeat, and log it — at
    /// ERROR, unless it was being stopped on purpose ([`Self::stop_one`]).
    /// Nothing respawns it here.
    ///
    /// Pends forever when no task is left, so a caller selecting on this
    /// beside its stop signal never spins. Cancel-safe: it holds no state
    /// across its one await.
    pub async fn next_dead(&mut self) -> DeadChild {
        loop {
            let Some(joined) = self.set.join_next_with_id().await else {
                return std::future::pending().await;
            };
            let ended = match joined {
                // A child's output is uninhabited: completing normally is
                // not a thing its task can do.
                Ok((_, never)) => match never {},
                Err(ended) => ended,
            };
            let task = ended.id();
            let cause = if ended.is_panic() {
                DeathCause::Panicked
            } else {
                DeathCause::Cancelled
            };
            let panic = ended
                .try_into_panic()
                .ok()
                .map(|p| PanicMessage::of(p.as_ref()));
            let Some(child) = self.by_task.remove(&task) else {
                // Every task in the set was spawned by `spawn`, which records
                // its id first. Say so rather than guess which child it was.
                error!(%task, %cause, "a runtime task with no catalog entry ended");
                continue;
            };
            let at = Timestamp::now();
            if let Some(entry) = self.entries.get_mut(&child) {
                entry.state = ChildState::Dead(cause);
                entry.ended_at = Some(at);
                entry.last_death = Some(Death { at, cause });
                if cause == DeathCause::Panicked {
                    entry.beat.record_panic();
                }
            }
            if stopped_on_purpose(self.stopping.remove(&child), cause) {
                info!(%child, "runtime child stopped");
            } else {
                error!(
                    %child,
                    %cause,
                    panic = panic.as_ref().map(tracing::field::display),
                    "runtime child ended; it is Dead until an operator restarts it"
                );
            }
            return DeadChild { child, cause };
        }
    }

    /// Abort every child and wait for each to end; each is then Dead
    /// (cancelled).
    pub(crate) async fn stop(&mut self) {
        self.set.shutdown().await;
        self.by_task.clear();
        self.stopping.clear();
        let at = Timestamp::now();
        for entry in self.entries.values_mut() {
            if entry.state == ChildState::Running {
                entry.state = ChildState::Dead(DeathCause::Cancelled);
                entry.ended_at = Some(at);
                entry.last_death = Some(Death {
                    at,
                    cause: DeathCause::Cancelled,
                });
            }
        }
    }

    /// The entry for `child`; `None` if it was never spawned.
    #[must_use]
    pub fn get(&self, child: Child) -> Option<&ChildHandle> {
        self.entries.get(&child)
    }

    /// Every spawned child with its entry, in catalog order.
    pub fn iter(&self) -> impl Iterator<Item = (Child, &ChildHandle)> {
        self.entries.iter().map(|(child, entry)| (*child, entry))
    }

    /// The spawned children whose tasks have not ended.
    pub fn running(&self) -> impl Iterator<Item = Child> + '_ {
        self.iter()
            .filter(|(_, entry)| entry.state == ChildState::Running)
            .map(|(child, _)| child)
    }

    /// How many children were spawned.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no child was spawned.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use super::*;

    const DEADLINE: Duration = Duration::from_secs(5);

    fn body(run: impl Future<Output = Infallible> + Send + 'static) -> (Arc<Heartbeat>, ChildTask) {
        let beat = Arc::new(Heartbeat::new());
        (beat.clone(), ChildTask::new(beat, run))
    }

    #[test]
    fn all_walks_every_child_once() {
        let walked: Vec<Child> = Child::all().collect();
        let distinct: BTreeSet<Child> = walked.iter().copied().collect();
        assert_eq!(
            walked.len(),
            distinct.len(),
            "a child is walked twice: {walked:?}"
        );
        assert_eq!(
            walked.len(),
            Driver::ALL.len() + Listener::ALL.len() + 1,
            "a top-level shape is declared and not walked: {walked:?}"
        );
        for d in Driver::ALL {
            assert!(distinct.contains(&Child::Driver(*d)), "{d:?} is not walked");
        }
        for l in Listener::ALL {
            assert!(
                distinct.contains(&Child::Listener(*l)),
                "{l:?} is not walked"
            );
        }
        assert!(distinct.contains(&Child::NodeLease));
    }

    /// The node lease is built from the kubelet's row, which exists only
    /// once the kubelet is spawned: the walk must reach the kubelet first.
    #[test]
    fn the_node_lease_is_walked_after_the_kubelet_it_watches() {
        let walked: Vec<Child> = Child::all().collect();
        let at = |c: Child| walked.iter().position(|w| *w == c);
        let (kubelet, lease) = (at(Child::Driver(Driver::Kubelet)), at(Child::NodeLease));
        assert!(
            matches!((kubelet, lease), (Some(k), Some(l)) if k < l),
            "kubelet at {kubelet:?}, node lease at {lease:?}: {walked:?}"
        );
    }

    /// A child is built with the children spawned before it, so it can read
    /// a sibling's row.
    #[tokio::test]
    async fn a_child_is_built_seeing_the_children_spawned_before_it() {
        let mut seen = None;
        let children =
            Children::spawn_catalog(&BootConfig::prescribed(), |child, before| match child {
                Child::Driver(Driver::Kubelet) => Some(body(std::future::pending()).1),
                Child::NodeLease => {
                    seen = Some(before.row(Child::Driver(Driver::Kubelet)).is_some());
                    Some(body(std::future::pending()).1)
                }
                Child::Driver(_) | Child::Listener(_) => None,
            });
        assert_eq!(
            seen,
            Some(true),
            "the lease was built without the kubelet's row"
        );
        assert_eq!(
            children.iter().map(|(c, _)| c).collect::<Vec<_>>(),
            [Child::Driver(Driver::Kubelet), Child::NodeLease]
        );
    }

    #[test]
    fn the_node_lease_is_enabled_wherever_the_kubelet_runs() {
        let boot = BootConfig::prescribed();
        assert!(Child::Driver(Driver::Kubelet).enabled(&boot));
        assert!(
            Child::NodeLease.enabled(&boot),
            "a node whose kubelet runs renews its lease"
        );
    }

    #[test]
    fn every_child_has_a_distinct_name() {
        let names: BTreeSet<&str> = Child::all().map(Child::name).collect();
        assert_eq!(
            names.len(),
            Child::all().count(),
            "two children share a name"
        );
    }

    /// The plan's one explicit classification: the kubelet's `local` map
    /// lives across ticks, so it must never be re-ticked over a torn one.
    #[test]
    fn the_kubelet_is_stateful_and_listeners_do_not_tick() {
        assert_eq!(
            Child::Driver(Driver::Kubelet).tick_state(),
            Some(TickState::Stateful)
        );
        for l in Listener::ALL {
            assert_eq!(Child::Listener(*l).tick_state(), None);
        }
    }

    /// Every per-child fact is an exhaustive match, so a new variant without
    /// its row is E0004 — but only while no arm is a wildcard. This keeps it
    /// that way for this module's non-test code, and for the fault-injection
    /// matrix, whose injector is the same kind of row (W6).
    #[test]
    fn the_child_catalog_has_no_wildcard_arm() {
        let files = [
            ("child.rs", include_str!("child.rs")),
            ("fault_matrix.rs", include_str!("fault_matrix.rs")),
        ];
        let offenders: Vec<(&str, usize, &str)> = files
            .iter()
            .flat_map(|(file, src)| {
                let code = src.split("#[cfg(test)]").next().unwrap_or(src);
                code.lines()
                    .enumerate()
                    .filter(|(_, line)| !line.trim_start().starts_with("//"))
                    .filter(|(_, line)| line.contains("_ =>") || line.contains("| _"))
                    .map(|(i, line)| (*file, i + 1, line))
            })
            .collect();
        assert!(
            offenders.is_empty(),
            "a wildcard arm hides a missing catalog row: {offenders:?}"
        );
    }

    #[tokio::test]
    async fn a_child_that_panics_is_reported_dead_and_its_panic_counted() {
        let mut children = Children::default();
        let child = Child::Driver(Driver::Gc);
        let (beat, task) = body(async { panic!("tick blew up") });
        children.spawn(child, task);

        let dead = tokio::time::timeout(DEADLINE, children.next_dead())
            .await
            .expect("a panicked child was never reported");

        assert_eq!(
            dead,
            DeadChild {
                child,
                cause: DeathCause::Panicked
            }
        );
        assert_eq!(
            children.get(child).map(ChildHandle::state),
            Some(ChildState::Dead(DeathCause::Panicked))
        );
        assert_eq!(beat.snapshot().panics, 1);
        assert_eq!(children.running().count(), 0);
    }

    #[tokio::test]
    async fn an_aborted_child_is_reported_dead_as_cancelled() {
        let mut children = Children::default();
        let (_, task) = body(std::future::pending());
        let child = Child::Listener(Listener::KubeletHttp);
        children.spawn(child, task);
        children.set.abort_all();

        let dead = tokio::time::timeout(DEADLINE, children.next_dead())
            .await
            .expect("an aborted child was never reported");

        assert_eq!(
            dead,
            DeadChild {
                child,
                cause: DeathCause::Cancelled
            }
        );
        assert_eq!(
            children.get(child).map(ChildHandle::state),
            Some(ChildState::Dead(DeathCause::Cancelled))
        );
    }

    /// Only the child that died is marked: its siblings keep running.
    #[tokio::test]
    async fn one_death_marks_one_child() {
        let mut children = Children::default();
        let (_, alive) = body(std::future::pending());
        let (_, doomed) = body(async { panic!("only me") });
        children.spawn(Child::Driver(Driver::Kubelet), alive);
        children.spawn(Child::Driver(Driver::Job), doomed);

        let dead = tokio::time::timeout(DEADLINE, children.next_dead())
            .await
            .expect("reported");

        assert_eq!(dead.child, Child::Driver(Driver::Job));
        assert_eq!(
            children.running().collect::<Vec<_>>(),
            [Child::Driver(Driver::Kubelet)]
        );
    }

    /// With nothing left to die, waiting pends instead of returning at once
    /// — a caller looping on it beside a stop signal would otherwise spin.
    #[tokio::test]
    async fn waiting_on_no_children_pends() {
        let mut children = Children::default();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), children.next_dead())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn stop_ends_every_child() {
        let mut children = Children::default();
        let (_, a) = body(std::future::pending());
        let (_, b) = body(std::future::pending());
        children.spawn(Child::Driver(Driver::Kubelet), a);
        children.spawn(Child::Listener(Listener::EtcdFacade), b);

        children.stop().await;

        assert_eq!(children.running().count(), 0);
        assert!(
            children
                .iter()
                .all(|(_, e)| e.state() == ChildState::Dead(DeathCause::Cancelled))
        );
    }

    /// Only an abort that was asked for is a stop; anything else is a death,
    /// logged as one.
    #[test]
    fn only_an_asked_for_abort_is_a_stop() {
        assert!(stopped_on_purpose(true, DeathCause::Cancelled));
        assert!(!stopped_on_purpose(true, DeathCause::Panicked));
        assert!(!stopped_on_purpose(false, DeathCause::Cancelled));
        assert!(!stopped_on_purpose(false, DeathCause::Panicked));
    }

    /// A child has one task at a time: spawning it again is refused while it
    /// runs, and after it dies it comes back one generation on, still
    /// carrying how it last died.
    #[tokio::test]
    async fn a_dead_child_respawns_one_generation_on_with_its_death_kept() {
        let mut children = Children::default();
        let child = Child::Driver(Driver::Gc);
        children.spawn(child, body(async { panic!("first life") }).1);
        let first = children.get(child).expect("spawned");
        assert_eq!(first.generation(), 0);
        assert_eq!(first.last_death(), None);
        assert_eq!(
            children.spawn_one(child, body(std::future::pending()).1),
            Err(StillRunning(child)),
            "a running child was given a second task"
        );

        let dead = tokio::time::timeout(DEADLINE, children.next_dead())
            .await
            .expect("reported");
        assert_eq!(dead.cause, DeathCause::Panicked);
        assert_eq!(
            children.spawn_one(child, body(std::future::pending()).1),
            Ok(1)
        );

        let handle = children.get(child).expect("respawned");
        assert_eq!(handle.state(), ChildState::Running);
        assert_eq!(handle.generation(), 1);
        assert_eq!(
            handle.last_death().map(|d| d.cause),
            Some(DeathCause::Panicked),
            "the respawn forgot how its previous task ended"
        );
        assert_eq!(handle.ended_at(), None);
    }

    /// Stopping one child ends it alone, and says so; its siblings keep
    /// running, and a child not running is left as it is.
    #[tokio::test]
    async fn stopping_one_child_leaves_its_siblings_running() {
        let mut children = Children::default();
        let (target, sibling) = (
            Child::Listener(Listener::KubeletHttp),
            Child::Driver(Driver::Kubelet),
        );
        children.spawn(target, body(std::future::pending()).1);
        children.spawn(sibling, body(std::future::pending()).1);

        let ended = tokio::time::timeout(DEADLINE, children.stop_one(target))
            .await
            .expect("the stop finished");

        assert_eq!(
            ended,
            [DeadChild {
                child: target,
                cause: DeathCause::Cancelled
            }]
        );
        assert_eq!(children.running().collect::<Vec<_>>(), [sibling]);
        assert!(
            children.stopping.is_empty(),
            "a finished stop is still pending"
        );
        assert!(children.stop_one(target).await.is_empty());
        assert!(
            children
                .stop_one(Child::Listener(Listener::EtcdFacade))
                .await
                .is_empty()
        );
    }

    /// A disabled child is forgotten as if never spawned — but only once it
    /// has stopped.
    #[tokio::test]
    async fn only_a_stopped_child_is_forgotten() {
        let mut children = Children::default();
        let child = Child::Driver(Driver::Job);
        children.spawn(child, body(std::future::pending()).1);
        assert_eq!(children.forget(child), Err(StillRunning(child)));

        let _ = children.stop_one(child).await;
        assert_eq!(children.forget(child), Ok(()));
        assert!(children.get(child).is_none());
        assert!(children.rows().is_empty(), "its health row outlived it");
        assert_eq!(
            children.spawn_one(child, body(std::future::pending()).1),
            Ok(0),
            "a forgotten child comes back as new"
        );
    }

    /// Every switch is one leaf, gates at least one driver, and is toggled
    /// in place unless a driver it gates is rebuilt only by a restart.
    #[test]
    fn every_switch_is_a_leaf_that_gates_a_driver() {
        for switch in EnableSwitch::ALL.iter().copied() {
            assert_eq!(EnableSwitch::of_leaf(switch.leaf()), Some(switch));
            assert!(
                switch.drivers().next().is_some(),
                "{switch:?} gates nothing"
            );
        }
        let restart_only: Vec<EnableSwitch> = EnableSwitch::ALL
            .iter()
            .copied()
            .filter(|s| !s.toggles_live())
            .collect();
        assert_eq!(
            restart_only,
            [EnableSwitch::ServiceRouting, EnableSwitch::Crd]
        );
        assert_eq!(
            EnableSwitch::PvBinder.drivers().collect::<Vec<_>>(),
            [
                Driver::PvBinder,
                Driver::VolumeSnapshot,
                Driver::PvcProtection
            ]
        );
        assert_eq!(EnableSwitch::of_leaf("controllers.enable"), None);
    }

    /// A listener moved to a new address is rebuilt to follow it; nothing
    /// else a change applies in place rebuilds a child.
    #[test]
    fn only_a_moved_listener_is_rebuilt_by_a_change_in_place() {
        let before = BootConfig::prescribed();
        let mut after = before.clone();
        after.kubelet_listen_addr = "127.0.0.1:20250".into();
        for child in Child::all() {
            assert_eq!(
                child.rebuilt_between(&before, &after),
                child == Child::Listener(Listener::KubeletHttp),
                "{child}"
            );
        }
        assert!(Child::all().all(|c| !c.rebuilt_between(&before, &before)));
    }
}
