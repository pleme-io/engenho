//! The runtime's children, by the control API's names.
//!
//! The runtime names its drivers for logs (`replicaset`, `pdb`); the spec
//! names them as `DriverName` / `ChildName` (`replica_set`,
//! `pod_disruption_budget`). One list below generates both directions, and
//! each is an exhaustive `match`: a driver added to the runtime and not the
//! spec — or the spec and not the runtime — does not compile. That is the
//! parity the spec's `DriverName` description promises.

use engenho_control_types::types;

use crate::child::{Child, DeathCause, Driver, Listener, Respawn};
use crate::control::reinit::ReinitOp;
use crate::layout::Area;

macro_rules! driver_names {
    ($($variant:ident),* $(,)?) => {
        /// A driver by its API name.
        #[must_use]
        pub const fn driver_to_wire(driver: Driver) -> types::DriverName {
            match driver { $(Driver::$variant => types::DriverName::$variant),* }
        }

        /// The driver an API name names.
        #[must_use]
        pub const fn driver_from_wire(name: types::DriverName) -> Driver {
            match name { $(types::DriverName::$variant => Driver::$variant),* }
        }

        /// A child by its API name.
        #[must_use]
        pub const fn child_to_wire(child: Child) -> types::ChildName {
            match child {
                $(Child::Driver(Driver::$variant) => types::ChildName::$variant,)*
                Child::Listener(Listener::KubeletHttp) => types::ChildName::KubeletHttp,
                Child::Listener(Listener::EtcdFacade) => types::ChildName::EtcdFacade,
                Child::NodeLease => types::ChildName::NodeLease,
            }
        }

        /// The child an API name names.
        #[must_use]
        pub const fn child_from_wire(name: types::ChildName) -> Child {
            match name {
                $(types::ChildName::$variant => Child::Driver(Driver::$variant),)*
                types::ChildName::KubeletHttp => Child::Listener(Listener::KubeletHttp),
                types::ChildName::EtcdFacade => Child::Listener(Listener::EtcdFacade),
                types::ChildName::NodeLease => Child::NodeLease,
            }
        }
    };
}

driver_names!(
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
);

/// A child's kind.
#[must_use]
pub const fn child_kind(child: Child) -> types::ChildKind {
    match child {
        Child::Driver(_) => types::ChildKind::Driver,
        Child::Listener(_) => types::ChildKind::Listener,
        Child::NodeLease => types::ChildKind::NodeLease,
    }
}

/// A death's cause.
#[must_use]
pub const fn death_to_wire(cause: DeathCause) -> types::DeathCause {
    match cause {
        DeathCause::Panicked => types::DeathCause::Panicked,
        DeathCause::Cancelled => types::DeathCause::Cancelled,
    }
}

/// How a child is brought back on its own.
#[must_use]
pub const fn respawn_to_wire(respawn: Respawn) -> types::RespawnClass {
    match respawn {
        Respawn::Rebuild => types::RespawnClass::Rebuild,
        Respawn::RebuildWith(_) => types::RespawnClass::RebuildWith,
        Respawn::RuntimeRestartOnly => types::RespawnClass::RuntimeRestartOnly,
    }
}

/// A confirm-gated operation, by its API name.
#[must_use]
pub const fn reinit_op_to_wire(op: ReinitOp) -> types::ReinitOp {
    match op {
        ReinitOp::RotateAdminToken => types::ReinitOp::RotateAdminToken,
        ReinitOp::ReseedPki => types::ReinitOp::ReseedPki,
        ReinitOp::WipeStore => types::ReinitOp::WipeStore,
        ReinitOp::RotateControlIdentity => types::ReinitOp::RotateControlIdentity,
    }
}

/// An area of the data directory, by its API name.
#[must_use]
pub const fn area_to_wire(area: Area) -> types::LayoutName {
    match area {
        Area::Store => types::LayoutName::Store,
        Area::Pki => types::LayoutName::Pki,
        Area::LocalPath => types::LayoutName::LocalPath,
        Area::Volumes => types::LayoutName::Volumes,
        Area::Plugins => types::LayoutName::Plugins,
        Area::Pods => types::LayoutName::Pods,
        Area::Snapshots => types::LayoutName::Snapshots,
        Area::Control => types::LayoutName::Control,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every child round-trips through its API name, and the names are the
    /// spec's spellings.
    #[test]
    fn every_child_round_trips_through_its_api_name() {
        for child in Child::all() {
            assert_eq!(child_from_wire(child_to_wire(child)), child);
        }
        for driver in Driver::ALL {
            assert_eq!(driver_from_wire(driver_to_wire(*driver)), *driver);
        }
        assert_eq!(
            child_to_wire(Child::Driver(Driver::PodDisruptionBudget)).to_string(),
            "pod_disruption_budget"
        );
        assert_eq!(child_to_wire(Child::NodeLease).to_string(), "node_lease");
    }
}
