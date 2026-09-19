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
//!   ended; it is marked [`ChildState::Dead`] and logged at ERROR. There is
//!   no respawn.
//!
//! ## What a panic does (T2.7)
//!
//! Decided per child by its [`TickState`], which the runtime hands its
//! driver: a Stateless driver contains a panicking tick, counts it and is
//! re-ticked by the next event or the fallback, so its task never ends; a
//! Stateful driver's panic ends its task, and the set above marks it Dead —
//! for the kubelet, Dead is the park. A listener whose serve ends rebinds on
//! a growing backoff. See [`engenho_controllers::contain`].
//!
//! Still only caught: a task spawned OUTSIDE this catalog (T0.4's
//! `disallowed-methods` on every spawn path, once clippy blocks).

#![deny(clippy::wildcard_enum_match_arm)]

use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use engenho_config::{ControllerEnable, EngenhoConfig};
pub use engenho_controllers::TickState;
use engenho_controllers::{ControllerType, Heartbeat, KindFilter, PanicMessage, Reads};
use tokio::task::{Id, JoinSet};
use tracing::error;

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
    /// The node-lease renewal task.
    ///
    /// ★ A PLACEHOLDER. It is declared so every exhaustive match over the
    /// catalog already carries its row; T1.3c gives it a body (renew the
    /// Lease only while the kubelet's heartbeat is young). Until then
    /// [`Child::enabled`] is `false` for it and nothing spawns it.
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

    /// Whether `controllers.enable` turns this driver on. The drivers with
    /// no switch always run.
    #[must_use]
    pub const fn enabled(self, enable: &ControllerEnable) -> bool {
        match self {
            Self::Deployment => enable.deployment,
            Self::ReplicaSet => enable.replicaset,
            Self::StatefulSet => enable.statefulset,
            Self::DaemonSet => enable.daemonset,
            Self::Job => enable.job,
            Self::CronJob => enable.cronjob,
            Self::PodDisruptionBudget => enable.pdb,
            Self::Endpoints => enable.endpoints,
            Self::ServiceRouting => enable.service_routing,
            Self::Gc => enable.gc,
            Self::Namespace => enable.namespace,
            // The snapshot controller snapshots the directories the binder
            // provisions; enabling one without the other yields a controller
            // that can only ever decline.
            Self::PvBinder | Self::VolumeSnapshot => enable.pv_binder,
            Self::Crd => enable.crd,
            Self::Scheduler
            | Self::ServedCapability
            | Self::NetworkPolicy
            | Self::CsiRegistrar
            | Self::CniStatus
            | Self::Kubelet => true,
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

    /// Whether the config binds this listener. An empty `etcd_listen_addr`
    /// disables the façade.
    #[must_use]
    pub fn enabled(self, config: &EngenhoConfig) -> bool {
        match self {
            Self::KubeletHttp => true,
            Self::EtcdFacade => !config.runtime.etcd_listen_addr.is_empty(),
        }
    }
}

impl Child {
    /// Every child, each exactly once: the drivers, then the listeners, then
    /// the node-lease task.
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
        match self {
            Self::Driver(d) => Some(d.tick_state()),
            Self::Listener(_) => None,
            // It renews from the store and the kubelet's heartbeat; it holds
            // nothing across ticks.
            Self::NodeLease => Some(TickState::Stateless),
        }
    }

    /// Whether this config spawns the child.
    #[must_use]
    pub fn enabled(self, config: &EngenhoConfig) -> bool {
        match self {
            Self::Driver(d) => d.enabled(&config.controllers.enable),
            Self::Listener(l) => l.enabled(config),
            // A placeholder until T1.3c: see the variant.
            Self::NodeLease => false,
        }
    }
}

impl fmt::Display for Child {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Which controller a driver runs, what it reads, and which events wake the
/// driver, as the runtime wired it (T1.7, T5.11).
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
/// for a driver, its [`Wiring`].
pub(crate) struct ChildTask {
    beat: Arc<Heartbeat>,
    wiring: Option<Wiring>,
    run: Pin<Box<dyn Future<Output = Infallible> + Send + 'static>>,
}

impl ChildTask {
    /// A body that never returns, recording into `beat`. For a child that
    /// is not woken by store events (a listener).
    pub(crate) fn new(
        beat: Arc<Heartbeat>,
        run: impl Future<Output = Infallible> + Send + 'static,
    ) -> Self {
        Self {
            beat,
            wiring: None,
            run: Box::pin(run),
        }
    }

    /// A driver's body: as [`Self::new`], plus what wakes it and why.
    pub(crate) fn driver(
        beat: Arc<Heartbeat>,
        wiring: Wiring,
        run: impl Future<Output = Infallible> + Send + 'static,
    ) -> Self {
        Self {
            beat,
            wiring: Some(wiring),
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
    /// Its task ended. It is not respawned.
    Dead(DeathCause),
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
    state: ChildState,
}

impl ChildHandle {
    /// The heartbeat the child writes.
    #[must_use]
    pub fn beat(&self) -> &Arc<Heartbeat> {
        &self.beat
    }

    /// What the child's controller reads and what wakes it; `None` for a
    /// child no store event wakes (a listener).
    #[must_use]
    pub fn wiring(&self) -> Option<&Wiring> {
        self.wiring.as_ref()
    }

    /// What the supervisor has seen of the child's task.
    #[must_use]
    pub const fn state(&self) -> ChildState {
        self.state
    }
}

/// The runtime's owned children: one task set, and which child each task is.
///
/// Built only by [`Children::spawn_catalog`], which walks [`Child::all`] once,
/// so no child is spawned twice and none is spawned from outside the catalog.
/// Dropping the set aborts every task in it.
#[derive(Debug, Default)]
pub struct Children {
    set: JoinSet<Infallible>,
    by_task: HashMap<Id, Child>,
    entries: BTreeMap<Child, ChildHandle>,
}

impl Children {
    /// Walk the catalog once, spawning every child `config` enables with the
    /// body `build` returns for it. `build` returning `None` leaves a child
    /// unspawned (the node-lease placeholder).
    pub(crate) fn spawn_catalog(
        config: &EngenhoConfig,
        mut build: impl FnMut(Child) -> Option<ChildTask>,
    ) -> Self {
        let mut children = Self::default();
        for child in Child::all().filter(|c| c.enabled(config)) {
            if let Some(task) = build(child) {
                children.spawn(child, task);
            }
        }
        children
    }

    /// Spawn one child. Private: only [`Self::spawn_catalog`] (which visits
    /// each child once) and this module's tests reach it.
    fn spawn(&mut self, child: Child, task: ChildTask) {
        let abort = self.set.spawn(task.run);
        self.by_task.insert(abort.id(), child);
        self.entries.insert(
            child,
            ChildHandle {
                beat: task.beat,
                wiring: task.wiring,
                state: ChildState::Running,
            },
        );
    }

    /// Wait for the next child whose task ends, mark it
    /// [`ChildState::Dead`], count a panic in its heartbeat, and log it at
    /// ERROR. It is not respawned.
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
            if let Some(entry) = self.entries.get_mut(&child) {
                entry.state = ChildState::Dead(cause);
                if cause == DeathCause::Panicked {
                    entry.beat.record_panic();
                }
            }
            error!(
                %child,
                %cause,
                panic = panic.as_ref().map(tracing::field::display),
                "runtime child ended; it is Dead and will not be respawned"
            );
            return DeadChild { child, cause };
        }
    }

    /// Abort every child and wait for each to end; each is then Dead
    /// (cancelled).
    pub(crate) async fn stop(&mut self) {
        self.set.shutdown().await;
        self.by_task.clear();
        for entry in self.entries.values_mut() {
            if entry.state == ChildState::Running {
                entry.state = ChildState::Dead(DeathCause::Cancelled);
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
    /// that way for this module's non-test code.
    #[test]
    fn the_child_catalog_has_no_wildcard_arm() {
        let src = include_str!("child.rs");
        let code = src.split("#[cfg(test)]").next().unwrap_or(src);
        let offenders: Vec<(usize, &str)> = code
            .lines()
            .enumerate()
            .filter(|(_, line)| !line.trim_start().starts_with("//"))
            .filter(|(_, line)| line.contains("_ =>") || line.contains("| _"))
            .map(|(i, line)| (i + 1, line))
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
}
