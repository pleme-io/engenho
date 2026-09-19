//! What a controller reads: the one declaration its wake filter is derived
//! from (T1.7).
//!
//! ## The defect this closes
//!
//! A [`crate::WatchDriver`] wakes its controller only for events whose kind
//! its [`KindFilter`] names; anything else waits for the fallback tick. Those
//! filters used to be hand lists written at the spawn site, away from the
//! code that reads, and they drifted from what the controllers actually
//! read:
//!
//! * the `StatefulSet` controller lists the claims it creates from
//!   `volumeClaimTemplates`, and woke only on `StatefulSet` and `Pod`;
//! * the PV binder lists `VolumeSnapshot` and `VolumeSnapshotContent` to
//!   restore a claim from a snapshot, and never woke on either;
//! * the kubelet reads the `Secret`, `ConfigMap`, `ServiceAccount`, claim and
//!   volume a pod names to start it, and woke only on `Pod`.
//!
//! In each case a change the controller was waiting for sat unread until the
//! next fallback tick (30 s by default) or an unrelated event. A pod whose
//! Secret had just been created stayed where it was for that long.
//!
//! ## The shape
//!
//! Each controller declares, beside its own code, every kind its tick reads
//! ([`DeclaresReads`]). The runtime builds a driver's filter from that
//! declaration and from nothing else ([`Reads::filter`]), and it only drives
//! a controller that has one, so spawning an undeclared controller is E0277.
//! The owned-children family derives its declaration from the same parent
//! and child kinds its blanket tick lists, plus the kinds it reads beyond
//! them ([`crate::OwnedChildrenReconciler::also_reads`]).
//!
//! ## What is only caught
//!
//! That a declaration names every kind its controller really reads is a
//! test, not a type. The runtime's read census scans each spawned
//! controller's source for store reads by literal kind and fails when one is
//! not declared. A read whose kind is computed at run time is invisible to
//! it: a controller that computes its kinds from a table declares them from
//! the same table, and one that cannot name them ahead of time (the garbage
//! collector, which follows owner references of any kind) declares
//! [`Reads::every`].
//!
//! Reads made by an event sink to deduplicate the Events a controller emits
//! are the sink's, through its own store handle, and are not declared here:
//! an Event is the controller's output, never an input to its decision.

use std::sync::Arc;

use engenho_types::kind::GroupVersionKind;

use crate::watch_driver::KindFilter;

/// A `(group, version, kind)` triple, usable in a `const`.
#[must_use]
pub const fn gvk(
    group: &'static str,
    version: &'static str,
    kind: &'static str,
) -> GroupVersionKind {
    GroupVersionKind {
        group,
        version,
        kind,
    }
}

/// Every kind a controller's tick reads from the store.
///
/// Built only through its constructors, so a declared kind is held once, in
/// the order first declared.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reads(Scope);

#[derive(Clone, Debug, PartialEq, Eq)]
enum Scope {
    /// Exactly these kinds. Empty means the controller reads nothing from
    /// the store (its inputs live elsewhere), so no event wakes it.
    Kinds(Vec<GroupVersionKind>),
    /// Kinds the controller cannot name ahead of time. Every event wakes it.
    Every,
}

impl Reads {
    /// Reads exactly `kinds`.
    #[must_use]
    pub fn of(kinds: &[GroupVersionKind]) -> Self {
        Self(Scope::Kinds(Vec::new())).and(kinds.iter().copied())
    }

    /// Reads nothing from the store. Only the fallback tick drives it.
    #[must_use]
    pub const fn nothing() -> Self {
        Self(Scope::Kinds(Vec::new()))
    }

    /// Reads kinds it cannot name ahead of time: every event wakes it.
    #[must_use]
    pub const fn every() -> Self {
        Self(Scope::Every)
    }

    /// These reads plus `more`. A kind already declared is not repeated;
    /// [`Reads::every`] already covers anything added to it.
    #[must_use]
    pub fn and(self, more: impl IntoIterator<Item = GroupVersionKind>) -> Self {
        match self.0 {
            Scope::Every => Self(Scope::Every),
            Scope::Kinds(mut kinds) => {
                for kind in more {
                    if !kinds.contains(&kind) {
                        kinds.push(kind);
                    }
                }
                Self(Scope::Kinds(kinds))
            }
        }
    }

    /// The declared kinds; `None` for [`Reads::every`].
    #[must_use]
    pub fn kinds(&self) -> Option<&[GroupVersionKind]> {
        match &self.0 {
            Scope::Kinds(kinds) => Some(kinds),
            Scope::Every => None,
        }
    }

    /// Whether an object of `kind` is among what the controller reads. Kinds
    /// are compared by name, as the store's watch events carry them.
    #[must_use]
    pub fn includes(&self, kind: &str) -> bool {
        match &self.0 {
            Scope::Kinds(kinds) => kinds.iter().any(|k| k.kind == kind),
            Scope::Every => true,
        }
    }

    /// The wake filter these reads call for: every declared kind, and no
    /// other. The runtime builds a driver's filter here and nowhere else.
    #[must_use]
    pub fn filter(&self) -> KindFilter {
        match &self.0 {
            Scope::Every => KindFilter::All,
            Scope::Kinds(kinds) => {
                let mut names: Vec<String> = Vec::with_capacity(kinds.len());
                for kind in kinds {
                    if !names.iter().any(|n| n == kind.kind) {
                        names.push(kind.kind.to_owned());
                    }
                }
                KindFilter::Kinds(names)
            }
        }
    }
}

impl FromIterator<GroupVersionKind> for Reads {
    fn from_iter<I: IntoIterator<Item = GroupVersionKind>>(kinds: I) -> Self {
        Self::nothing().and(kinds)
    }
}

/// A controller that says which kinds its tick reads.
///
/// The runtime derives the controller's wake filter from [`Self::reads`] and
/// will not drive a controller without it. Declare every kind the tick
/// reads through its store handle, whether to decide or to check what is
/// already there; see the module docs for what is not a read.
pub trait DeclaresReads {
    /// Every kind this controller's tick reads from the store.
    fn reads(&self) -> Reads;
}

/// An `Arc<C>` reads what `C` reads, so a controller shared behind an `Arc`
/// (the kubelet, the scheduler) declares once.
impl<C: DeclaresReads + ?Sized> DeclaresReads for Arc<C> {
    fn reads(&self) -> Reads {
        (**self).reads()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const POD: GroupVersionKind = gvk("", "v1", "Pod");
    const NODE: GroupVersionKind = gvk("", "v1", "Node");
    const CONFIG_MAP: GroupVersionKind = gvk("", "v1", "ConfigMap");

    fn wakes(filter: &KindFilter) -> Vec<&str> {
        match filter {
            KindFilter::All => vec!["*"],
            KindFilter::Kinds(kinds) => kinds.iter().map(String::as_str).collect(),
        }
    }

    #[test]
    fn the_filter_wakes_on_every_declared_kind_and_no_other() {
        let filter = Reads::of(&[POD, NODE]).filter();
        assert!(filter.wakes_on("Pod"));
        assert!(filter.wakes_on("Node"));
        assert!(!filter.wakes_on("ConfigMap"));
    }

    #[test]
    fn reading_every_kind_wakes_on_every_event() {
        let filter = Reads::every().filter();
        assert!(matches!(filter, KindFilter::All));
        assert!(Reads::every().includes("AnythingAtAll"));
    }

    /// A controller whose inputs are not in the store (the CSI registrar
    /// scans a directory) is driven by the fallback tick alone.
    #[test]
    fn reading_nothing_wakes_on_nothing() {
        let filter = Reads::nothing().filter();
        assert!(wakes(&filter).is_empty());
        assert!(!filter.wakes_on("Pod"));
        assert_eq!(Reads::nothing().kinds(), Some(&[][..]));
    }

    #[test]
    fn a_kind_declared_twice_is_held_once() {
        let reads = Reads::of(&[POD, NODE]).and([POD, CONFIG_MAP]);
        assert_eq!(reads.kinds(), Some(&[POD, NODE, CONFIG_MAP][..]));
        assert_eq!(wakes(&reads.filter()), ["Pod", "Node", "ConfigMap"]);
    }

    /// Two groups may name the same kind; the filter matches by name, as
    /// watch events carry it, so the name appears once.
    #[test]
    fn one_name_in_two_groups_is_one_filter_entry() {
        let reads = Reads::of(&[gvk("a.io", "v1", "Widget"), gvk("b.io", "v1", "Widget")]);
        assert_eq!(reads.kinds().map(<[_]>::len), Some(2));
        assert_eq!(wakes(&reads.filter()), ["Widget"]);
    }

    #[test]
    fn every_absorbs_what_is_added_to_it() {
        assert_eq!(Reads::every().and([POD]), Reads::every());
        assert_eq!(Reads::every().kinds(), None);
    }

    #[test]
    fn an_arc_reads_what_its_controller_reads() {
        struct Reader;
        impl DeclaresReads for Reader {
            fn reads(&self) -> Reads {
                Reads::of(&[POD])
            }
        }
        assert_eq!(Arc::new(Reader).reads(), Reads::of(&[POD]));
    }
}
