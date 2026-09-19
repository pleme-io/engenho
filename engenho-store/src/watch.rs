//! Watch events — K8s-style change notifications.
//!
//! Every successful apply on the state machine emits a
//! [`WatchEvent`] over a tokio broadcast channel. The apiserver's
//! /watch endpoint forwards these as SSE / chunked-JSON to clients
//! (controllers, kubectl --watch, etc.).
//!
//! At R7.5 we emit FULL resource objects per event (matches the
//! default K8s watch behavior). R7.6 may add bookmark events +
//! resourceVersion-based resume.
//!
//! ## A watch with a selector sees an object LEAVE its view (T3.7)
//!
//! A watch filtered by a label or field selector is a view of the objects
//! that match it. An update that takes an object out of that view has to
//! reach the client as a DELETED, or the client's cache keeps the object
//! forever. Filtering each change on its post-image alone drops exactly
//! that update, because the post-image is the one that no longer matches.
//!
//! [`project`] is upstream's `cacheWatcher.convertToWatchEvent`
//! (`staging/src/k8s.io/apiserver/pkg/storage/cacher/cache_watcher.go`,
//! v1.34): the selector is asked about BOTH images of a change, and the
//! pair of verdicts decides the event, not the change's own kind. The
//! pure table is [`Projection::of`]. Every [`crate::revision::Change`]
//! already carries its prior image, and the history ring and the live
//! fan-out share one `Arc` of it, so asking about the prior costs no copy.

use serde::{Deserialize, Serialize};

use crate::resource::{ResourceKey, ResourceValue};
use crate::revision::{Change, ChangeKind};
use crate::state::stamp_resource_version;

/// One change notification.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WatchEvent {
    #[serde(rename = "type")]
    pub kind: WatchEventKind,
    /// The full resource object at the moment of the change. For
    /// `Deleted`, the object is the LAST KNOWN state (the value
    /// that was just removed).
    pub object: ResourceValue,
    /// The resource key (group/version/kind/ns/name) — convenience
    /// for clients filtering by kind without parsing `object.kind`.
    pub key: ResourceKey,
    /// The Raft log index at which this change was committed.
    /// Clients can use it to seek/resume.
    pub resource_version: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum WatchEventKind {
    Added,
    Modified,
    Deleted,
    /// Periodic marker (R7.6) — included for forward compat with
    /// resumable watches.
    Bookmark,
}

/// Which images of its object one committed [`Change`] has. Both the event
/// an unfiltered watch sends and which images a filtered watch's selector
/// can be asked about are read from this one fact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeShape {
    /// A first create: a post-image and no prior.
    Created,
    /// A replace, a patch or a Terminating stamp: a prior and a post-image.
    Updated,
    /// A removal: a prior and no post-image. The tombstone in
    /// [`Change::value`] is the prior again, not a new state of the object.
    Deleted,
}

impl ChangeShape {
    /// The shape of `change`: a removal is [`Self::Deleted`], and a write is
    /// [`Self::Created`] exactly when it has no prior, the same distinction
    /// the catalog draws between `ResourceOp::Created` and `Replaced`.
    #[must_use]
    pub fn of(change: &Change) -> Self {
        match (change.kind, &change.prior) {
            (ChangeKind::Delete, _) => Self::Deleted,
            (ChangeKind::Put, None) => Self::Created,
            (ChangeKind::Put, Some(_)) => Self::Updated,
        }
    }

    /// The event a watch with no selector sends. Every object is in its view
    /// before and after, so a create is ADDED, an update MODIFIED and a
    /// removal DELETED: the all-pass row of [`Projection::of`].
    #[must_use]
    pub const fn unfiltered(self) -> WatchEventKind {
        match self {
            Self::Created => WatchEventKind::Added,
            Self::Updated => WatchEventKind::Modified,
            Self::Deleted => WatchEventKind::Deleted,
        }
    }
}

/// The event a watch filtered by a selector sends for one change, when it
/// sends one ([`Projection::of`] answers `None` when it does not).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Projection {
    /// The object entered the view: ADDED, carrying the post-image.
    Added,
    /// The object was in the view and still is: MODIFIED, carrying the
    /// post-image.
    Modified,
    /// The object left the view, because it was removed or because it no
    /// longer matches: DELETED, carrying the PRIOR image stamped with the
    /// change's revision.
    Deleted,
}

impl Projection {
    /// The twelve-cell table: a change's shape, and the selector's verdict on
    /// each of its two images. `None` is the fourth outcome, skip: the object
    /// was not in the view before and is not after.
    ///
    /// `passes_before` is the verdict on the prior image and `passes_after`
    /// the verdict on the post-image. An image the shape does not have is not
    /// in the view, so a create's `passes_before` and a removal's
    /// `passes_after` never reach the answer, as upstream's `oldObjPasses` is
    /// false without a `PrevObject` and `curObjPasses` is false for a
    /// `Deleted` event. Those are the cells that matter: a removal whose
    /// tombstone still matches the selector is DELETED, not MODIFIED.
    #[must_use]
    pub const fn of(shape: ChangeShape, passes_before: bool, passes_after: bool) -> Option<Self> {
        let before = passes_before && !matches!(shape, ChangeShape::Created);
        let after = passes_after && !matches!(shape, ChangeShape::Deleted);
        match (before, after) {
            (false, true) => Some(Self::Added),
            (true, true) => Some(Self::Modified),
            (true, false) => Some(Self::Deleted),
            (false, false) => None,
        }
    }

    /// The wire kind of the event this projection sends.
    #[must_use]
    pub const fn event_kind(self) -> WatchEventKind {
        match self {
            Self::Added => WatchEventKind::Added,
            Self::Modified => WatchEventKind::Modified,
            Self::Deleted => WatchEventKind::Deleted,
        }
    }
}

/// The event a watch whose selector is `passes` receives for `change`, or
/// `None` when the object is outside its view both before and after.
///
/// `passes` is asked only about the images the change has: the prior when
/// there is one, and the post-image unless the change is a removal. The
/// verdicts go through [`Projection::of`]. ADDED and MODIFIED carry the
/// post-image. DELETED carries the prior, the object as the watch last saw
/// it, with `metadata.resourceVersion` set to the change's revision, as
/// upstream's `updateResourceVersion(oldObj, ..., event.ResourceVersion)`
/// does, so a client that tracks its resume point from the objects it
/// receives never moves backwards.
///
/// A watch with no selector passes everything and gets
/// [`ChangeShape::unfiltered`].
#[must_use]
pub fn project(
    change: &Change,
    mut passes: impl FnMut(&ResourceValue) -> bool,
) -> Option<WatchEvent> {
    let shape = ChangeShape::of(change);
    let passes_before = change.prior.as_ref().is_some_and(&mut passes);
    let passes_after = shape != ChangeShape::Deleted && passes(&change.value);
    let projection = Projection::of(shape, passes_before, passes_after)?;
    let object = match projection {
        Projection::Added | Projection::Modified => change.value.clone(),
        Projection::Deleted => {
            // Present: only a prior that passed can make a DELETED.
            let mut prior = change.prior.clone()?;
            stamp_resource_version(&mut prior, change.revision);
            prior
        }
    };
    Some(WatchEvent {
        kind: projection.event_kind(),
        object,
        key: change.key.clone(),
        resource_version: change.revision.get(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::revision::{Revision, VersionMeta};
    use crate::watch_backend::watch_event_from_change;
    use serde_json::json;

    // ── T3.7: the projection a watch with a selector reads ─────────────

    /// Every shape. The match is exhaustive, so a new shape does not compile
    /// here until it is listed, and then [`TABLE`] has no row for it and
    /// `the_twelve_cell_table` fails.
    fn every_shape() -> [ChangeShape; 3] {
        let all = [
            ChangeShape::Created,
            ChangeShape::Updated,
            ChangeShape::Deleted,
        ];
        for shape in all {
            match shape {
                ChangeShape::Created | ChangeShape::Updated | ChangeShape::Deleted => {}
            }
        }
        all
    }

    /// `(shape, passes_before, passes_after) → what the watch sends`, from
    /// upstream's `cacheWatcher.convertToWatchEvent` (cache_watcher.go,
    /// v1.34): `curObjPasses := event.Type != watch.Deleted && filter(obj)`,
    /// `oldObjPasses := PrevObject != nil && filter(prevObj)`, then
    /// cur&&!old → Added, cur&&old → Modified, !cur&&old → Deleted, else nil.
    const TABLE: [(ChangeShape, bool, bool, Option<Projection>); 12] = [
        // A create has no prior: `passes_before` never reaches the answer.
        (ChangeShape::Created, false, false, None),
        (ChangeShape::Created, false, true, Some(Projection::Added)),
        (ChangeShape::Created, true, false, None),
        (ChangeShape::Created, true, true, Some(Projection::Added)),
        // An update: the pair of verdicts decides, not the change's kind.
        (ChangeShape::Updated, false, false, None),
        // Relabelled INTO the view: the watch has never seen the object.
        (ChangeShape::Updated, false, true, Some(Projection::Added)),
        // Relabelled OUT of the view: the item T3.7 names.
        (ChangeShape::Updated, true, false, Some(Projection::Deleted)),
        (ChangeShape::Updated, true, true, Some(Projection::Modified)),
        // A removal has no post-image: `passes_after` never reaches the
        // answer, even though the tombstone still matches.
        (ChangeShape::Deleted, false, false, None),
        (ChangeShape::Deleted, false, true, None),
        (ChangeShape::Deleted, true, false, Some(Projection::Deleted)),
        (ChangeShape::Deleted, true, true, Some(Projection::Deleted)),
    ];

    #[test]
    fn the_twelve_cell_table() {
        for shape in every_shape() {
            for passes_before in [false, true] {
                for passes_after in [false, true] {
                    let rows: Vec<_> = TABLE
                        .iter()
                        .filter(|(s, b, a, _)| (*s, *b, *a) == (shape, passes_before, passes_after))
                        .collect();
                    assert_eq!(
                        rows.len(),
                        1,
                        "cell ({shape:?}, {passes_before}, {passes_after}) needs exactly one row"
                    );
                    assert_eq!(
                        Projection::of(shape, passes_before, passes_after),
                        rows[0].3,
                        "cell ({shape:?}, before passes: {passes_before}, after passes: {passes_after})"
                    );
                }
            }
        }
    }

    /// A ConfigMap image, told apart by `data.image`.
    fn image(tag: &str) -> ResourceValue {
        json!({
            "metadata": { "name": "cm", "resourceVersion": "4" },
            "data": { "image": tag },
        })
    }

    /// A committed change of `shape` at revision 7, built the way the
    /// catalog builds one: a removal's tombstone is its prior again.
    fn change_of(shape: ChangeShape) -> Change {
        let (kind, prior, value) = match shape {
            ChangeShape::Created => (ChangeKind::Put, None, image("post")),
            ChangeShape::Updated => (ChangeKind::Put, Some(image("prior")), image("post")),
            ChangeShape::Deleted => (ChangeKind::Delete, Some(image("prior")), image("prior")),
        };
        Change {
            revision: Revision(7),
            key: ResourceKey::namespaced("", "v1", "ConfigMap", "default", "cm"),
            kind,
            value,
            prior,
            version_meta: VersionMeta::created_at(Revision(7)),
        }
    }

    /// Every cell again, through [`project`] on a real change: the selector
    /// answers by WHICH image it is handed (by address, since a removal's
    /// tombstone equals its prior), so a projection that asks about an
    /// image the change does not have lands in the wrong cell.
    #[test]
    fn every_cell_holds_when_projecting_a_change() {
        for shape in every_shape() {
            let change = change_of(shape);
            assert_eq!(ChangeShape::of(&change), shape);
            for passes_before in [false, true] {
                for passes_after in [false, true] {
                    let selector = |obj: &ResourceValue| {
                        if change.prior.as_ref().is_some_and(|p| std::ptr::eq(obj, p)) {
                            passes_before
                        } else {
                            assert!(
                                std::ptr::eq(obj, &raw const change.value),
                                "asked about a stranger"
                            );
                            passes_after
                        }
                    };
                    let cell = (shape, passes_before, passes_after);
                    let expected = Projection::of(shape, passes_before, passes_after);
                    let got = project(&change, selector);
                    assert_eq!(
                        got.as_ref().map(|ev| ev.kind),
                        expected.map(Projection::event_kind),
                        "cell {cell:?}"
                    );
                    let Some(ev) = got else { continue };
                    assert_eq!(ev.key, change.key, "cell {cell:?}");
                    assert_eq!(ev.resource_version, 7, "cell {cell:?}");
                    let want = if ev.kind == WatchEventKind::Deleted {
                        // The prior, at the change's revision.
                        let mut prior = image("prior");
                        prior["metadata"]["resourceVersion"] = json!("7");
                        prior
                    } else {
                        change.value.clone()
                    };
                    assert_eq!(ev.object, want, "cell {cell:?}");
                }
            }
        }
    }

    /// A watch with no selector passes everything, and gets exactly what
    /// the unfiltered stream ([`watch_event_from_change`]) has always sent:
    /// the same kind, and for a create or an update the same object.
    #[test]
    fn a_watch_with_no_selector_is_the_all_pass_row() {
        for shape in every_shape() {
            assert_eq!(
                Projection::of(shape, true, true).map(Projection::event_kind),
                Some(shape.unfiltered()),
                "{shape:?}"
            );
            let change = change_of(shape);
            let unfiltered = watch_event_from_change(&change);
            let projected = project(&change, |_| true).expect("everything passes");
            assert_eq!(projected.kind, unfiltered.kind, "{shape:?}");
            if shape != ChangeShape::Deleted {
                assert_eq!(projected, unfiltered, "{shape:?}");
            }
        }
    }

    /// A removal DELETEs at its own revision. The catalog's tombstone keeps
    /// the object's last `resourceVersion`; upstream stamps the event's
    /// revision on the DELETED object (`updateResourceVersion`), so a
    /// client tracking its resume point from objects never moves back.
    #[test]
    fn a_deleted_event_carries_the_prior_at_the_change_revision() {
        let change = change_of(ChangeShape::Deleted);
        assert_eq!(change.value["metadata"]["resourceVersion"], "4");
        let ev = project(&change, |_| true).expect("the prior passes");
        assert_eq!(ev.kind, WatchEventKind::Deleted);
        assert_eq!(ev.object["metadata"]["resourceVersion"], "7");
        assert_eq!(ev.object["data"]["image"], "prior");
        assert_eq!(
            change
                .prior
                .as_ref()
                .map(|p| &p["metadata"]["resourceVersion"]),
            Some(&json!("4")),
            "projecting never touches the shared change"
        );
    }

    /// Upstream's `TestCacheWatcherHandlesFiltering`
    /// (staging/src/k8s.io/apiserver/pkg/storage/cacher/cache_watcher_test.go,
    /// v1.34), both cases, ported row for row. The filter is
    /// `spec.nodeName == "host"`; each event is `(its revision, the
    /// PrevObject as (rv, nodeName) if any, the Object as (rv, nodeName))`,
    /// and each expected event is `(type, the object's resourceVersion)`.
    #[test]
    fn upstream_cache_watcher_handles_filtering() {
        /// An object as `(its resourceVersion, its spec.nodeName)`.
        type Pod = (u64, &'static str);
        type Event = (u64, Option<Pod>, Pod);
        type Case = (Vec<Event>, Vec<(WatchEventKind, &'static str)>);
        fn pod((rv, node): Pod) -> ResourceValue {
            json!({ "metadata": { "resourceVersion": rv.to_string() }, "spec": { "nodeName": node } })
        }
        fn change((revision, prev, obj): Event) -> Change {
            Change {
                revision: Revision(revision),
                key: ResourceKey::namespaced("", "v1", "Pod", "default", "p"),
                kind: ChangeKind::Put,
                value: pod(obj),
                prior: prev.map(pod),
                version_meta: VersionMeta::created_at(Revision(revision)),
            }
        }
        let on_host = |obj: &ResourceValue| obj["spec"]["nodeName"] == "host";
        let cases: [Case; 2] = [
            // properly handle starting with the filter, then being deleted,
            // then re-added
            (
                vec![
                    (1, None, (1, "host")),
                    (2, Some((1, "host")), (2, "")),
                    (3, Some((2, "")), (3, "host")),
                ],
                vec![
                    (WatchEventKind::Added, "1"),
                    (WatchEventKind::Deleted, "2"),
                    (WatchEventKind::Added, "3"),
                ],
            ),
            // properly handle ignoring changes prior to the filter, then
            // getting added, then deleted
            (
                vec![
                    (1, None, (1, "")),
                    (2, Some((1, "")), (2, "")),
                    (3, Some((2, "")), (3, "host")),
                    (4, Some((3, "host")), (4, "host")),
                    (5, Some((4, "host")), (5, "")),
                    (6, Some((5, "")), (6, "")),
                ],
                vec![
                    (WatchEventKind::Added, "3"),
                    (WatchEventKind::Modified, "4"),
                    (WatchEventKind::Deleted, "5"),
                ],
            ),
        ];
        for (i, (events, expected)) in cases.into_iter().enumerate() {
            let got: Vec<WatchEvent> = events
                .into_iter()
                .filter_map(|e| project(&change(e), on_host))
                .collect();
            let kinds: Vec<(WatchEventKind, &str)> = got
                .iter()
                .map(|ev| {
                    (
                        ev.kind,
                        ev.object["metadata"]["resourceVersion"]
                            .as_str()
                            .unwrap_or_default(),
                    )
                })
                .collect();
            assert_eq!(kinds, expected, "case {i}");
            for ev in got.iter().filter(|ev| ev.kind == WatchEventKind::Deleted) {
                assert_eq!(
                    ev.object["spec"]["nodeName"], "host",
                    "case {i}: DELETED carries the PREVIOUS object, the one the watch saw"
                );
            }
        }
    }

    #[test]
    fn watch_event_serializes_as_k8s_format() {
        let ev = WatchEvent {
            kind: WatchEventKind::Added,
            object: serde_json::json!({"kind": "Pod"}),
            key: ResourceKey::namespaced("", "v1", "Pod", "default", "x"),
            resource_version: 42,
        };
        let s = serde_json::to_string(&ev).unwrap();
        // K8s wire format uses uppercase ADDED/MODIFIED/DELETED.
        assert!(s.contains("\"type\":\"ADDED\""), "got: {s}");
        let back: WatchEvent = serde_json::from_str(&s).unwrap();
        assert_eq!(back.kind, WatchEventKind::Added);
        assert_eq!(back.resource_version, 42);
    }

    #[test]
    fn watch_event_kinds_round_trip() {
        for k in [
            WatchEventKind::Added,
            WatchEventKind::Modified,
            WatchEventKind::Deleted,
            WatchEventKind::Bookmark,
        ] {
            let s = serde_json::to_string(&k).unwrap();
            let back: WatchEventKind = serde_json::from_str(&s).unwrap();
            assert_eq!(back, k);
        }
    }
}
