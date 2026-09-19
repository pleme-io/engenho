//! The controllers the runtime compiles in and does not run (T5.11).
//!
//! Every type that implements [`Controller`] is exactly one of two things:
//! the controller behind a catalog [`Driver`](crate::Driver), which the
//! runtime spawns, or a row here, which says why it does not. A controller
//! type that is neither is code that reads as a working feature — an HPA,
//! an Ingress, cluster DNS — while nothing ever ticks it.
//!
//! ## What is a type, and what is only a gate
//!
//! * A row names its controller through [`ControllerType::of`], so a row
//!   for a type that was renamed or deleted (E0412/E0433), or that no
//!   longer implements `Controller` (E0277), does not compile. Every
//!   per-row fact is an exhaustive `match` (E0004 for a row without one),
//!   and a row's reason is a closed [`DormantReason`].
//! * That every `Controller` type is spawned or a row here, and that no
//!   row is also spawned, is a TEST — a CI gate, not a type. Rust cannot
//!   enumerate a trait's implementors, so the runtime's test
//!   `every_controller_type_is_spawned_or_dormant` lexes the workspace's
//!   shipped sources for them (`impl_census`) and compares that census with
//!   the types the spawned drivers record in their [`Wiring`](crate::Wiring).
//!
//! ## Waking one
//!
//! Wiring a dormant controller in is a `Driver` variant with its arm in
//! the runtime, and deleting its row here in the same commit: the gate
//! fails while a type is both. Some rows name what must be fixed first.

#![deny(clippy::wildcard_enum_match_arm)]

use std::fmt;

use engenho_controllers::{
    Controller, ControllerType, DnsController, DrvBuildController, DrvController,
    EventDrivenController, HorizontalPodAutoscalerController, IngressController, PlantioController,
    TieredCacheReconciler,
};

engenho_controllers::closed_enum! {
    /// Every controller type compiled into engenho that no driver runs.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub enum Dormant {
        /// The HorizontalPodAutoscaler controller. Before it is wired in,
        /// its scale target's group and version must come from the
        /// target's own `apiVersion`, not the hand list in hpa.rs
        /// `target_gv`, which sends every kind but three to the core group.
        Hpa,
        /// The Ingress controller.
        Ingress,
        /// The cluster DNS controller.
        Dns,
        /// The Plantio (roça) controller. Before it is wired in, the path
        /// that issues a PASS receipt for a check that never ran goes
        /// (T5.6): `bootstrap_pipeline`, `RoceiroChoice`'s default, and
        /// `FakeVerifier`'s allow-when-no-policy.
        Plantio,
        /// The Derivation controller.
        Drv,
        /// The Derivation build controller.
        DrvBuild,
        /// The derivation cache's tier-promotion reconciler.
        TieredCache,
        /// The event-channel adapter that ticks a wrapped controller.
        EventDriven,
    }
}

engenho_controllers::closed_enum! {
    /// Why a controller type is compiled in and not run.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub enum DormantReason {
        /// It scales from a metrics source, and the only `MetricsProvider`
        /// is the test fake. engenho does not serve `metrics.k8s.io`
        /// either: served-capability reports that APIService unavailable.
        NoMetricsSource,
        /// Its backends write config for a proxy the binary does not run
        /// (Traefik routes, nginx server blocks). Running one beside engenho
        /// is a sidecar, which the one-binary rule refuses.
        NoIngressDatapath,
        /// Its only backend is an in-memory zone that no resolver serves.
        /// The zone-file backend its docs name was never written, and the
        /// CoreDNS path they describe is a sidecar.
        NoDnsResolver,
        /// It belongs to the derivation plane — substrate's cache tiers and
        /// a build backend — which no binary assembles. The only build
        /// backend is the test fake, and nothing serves the `Derivation`
        /// kind it reconciles.
        NoDerivationPlane,
        /// It belongs to the roça materialization plane, which only
        /// `PlantioPipeline` assembles, and no binary calls it.
        NoRocaPlane,
        /// The runtime's `WatchDriver` already drives every controller from
        /// store events, which is the job this adapter was written for.
        SupersededByWatchDriver,
    }
}

impl Dormant {
    /// Stable name, for logs and failure messages.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Hpa => "hpa",
            Self::Ingress => "ingress",
            Self::Dns => "dns",
            Self::Plantio => "plantio",
            Self::Drv => "drv",
            Self::DrvBuild => "drv-build",
            Self::TieredCache => "tiered-cache",
            Self::EventDriven => "event-driven",
        }
    }

    /// The controller type this row declares dormant. Each arm names the
    /// type itself, so it exists and implements `Controller` or this does
    /// not compile.
    #[must_use]
    pub fn controller_type(self) -> ControllerType {
        match self {
            Self::Hpa => of::<HorizontalPodAutoscalerController>(),
            Self::Ingress => of::<IngressController>(),
            Self::Dns => of::<DnsController>(),
            Self::Plantio => of::<PlantioController>(),
            Self::Drv => of::<DrvController>(),
            Self::DrvBuild => of::<DrvBuildController>(),
            Self::TieredCache => of::<TieredCacheReconciler>(),
            // Generic over its event type; the unit event stands for them
            // all (the catalog compares a type by crate and name).
            Self::EventDriven => of::<EventDrivenController<()>>(),
        }
    }

    /// Why nothing runs it.
    #[must_use]
    pub const fn reason(self) -> DormantReason {
        match self {
            Self::Hpa => DormantReason::NoMetricsSource,
            Self::Ingress => DormantReason::NoIngressDatapath,
            Self::Dns => DormantReason::NoDnsResolver,
            Self::Plantio => DormantReason::NoRocaPlane,
            Self::Drv | Self::DrvBuild | Self::TieredCache => DormantReason::NoDerivationPlane,
            Self::EventDriven => DormantReason::SupersededByWatchDriver,
        }
    }
}

impl fmt::Display for Dormant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl fmt::Display for DormantReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NoMetricsSource => "no metrics source: the only MetricsProvider is the test fake",
            Self::NoIngressDatapath => {
                "no ingress datapath: its backends configure a proxy the binary does not run"
            }
            Self::NoDnsResolver => "no DNS resolver: its only backend is a zone nothing serves",
            Self::NoDerivationPlane => "the derivation plane is assembled by no binary",
            Self::NoRocaPlane => {
                "the roça plane is assembled only by PlantioPipeline, which no binary calls"
            }
            Self::SupersededByWatchDriver => "superseded by the runtime's WatchDriver",
        })
    }
}

/// [`ControllerType::of`], shortened for the table above.
fn of<C: Controller>() -> ControllerType {
    ControllerType::of::<C>()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    /// Each row declares a different type: two rows for one type would let
    /// deleting one (to wire the controller in) leave it still declared.
    #[test]
    fn every_row_names_a_different_controller_type() {
        let types: BTreeSet<(&str, &str)> = Dormant::ALL
            .iter()
            .map(|d| (d.controller_type().krate(), d.controller_type().ident()))
            .collect();
        assert_eq!(types.len(), Dormant::ALL.len(), "two rows name one type");
        let names: BTreeSet<&str> = Dormant::ALL.iter().map(|d| d.name()).collect();
        assert_eq!(names.len(), Dormant::ALL.len(), "two rows share a name");
    }

    /// A reason no row gives is a reason nothing is dormant for: when the
    /// last controller it held is wired in, the reason goes with it.
    #[test]
    fn every_reason_is_some_rows_reason() {
        let given: BTreeSet<DormantReason> = Dormant::ALL.iter().map(|d| d.reason()).collect();
        let unused: Vec<DormantReason> = DormantReason::ALL
            .iter()
            .copied()
            .filter(|r| !given.contains(r))
            .collect();
        assert!(unused.is_empty(), "reasons no row gives: {unused:?}");
    }

    /// The generic adapter is declared by its own name, not by the event
    /// type standing in for its parameter.
    #[test]
    fn the_generic_adapter_is_declared_by_its_own_name() {
        let t = Dormant::EventDriven.controller_type();
        assert_eq!(
            (t.krate(), t.ident()),
            ("engenho_controllers", "EventDrivenController")
        );
    }
}
