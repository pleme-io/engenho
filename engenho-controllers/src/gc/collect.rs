//! Collecting one dependent: classify EVERY owner reference, then act.
//!
//! Upstream's `attemptToDeleteItem` (`garbagecollector.go`, v1.34.0), over a
//! mockable [`GcEnv`]:
//!
//! 1. An item already being deleted is left alone, with no read.
//! 2. The item is read live. Gone, or its name now holds another uid: that
//!    incarnation needs nothing.
//! 3. Each ownerReference, in order, is resolved through the served catalog
//!    and looked up live at the dependent's namespace (cluster-wide for a
//!    cluster-scoped kind). It is **solid**, **dangling** (absent, or the
//!    name holds another uid), or **waiting** (the owner is being deleted in
//!    the foreground: `deletionTimestamp` plus the `foregroundDeletion`
//!    finalizer). The first reference that cannot be resolved stops
//!    classification, and the item is left untouched.
//! 4. Any solid owner keeps the item; the dangling and waiting references
//!    are removed from it. No solid owner: the item is deleted.
//!
//! Every write is pinned to the resourceVersion read in step 2, so a
//! dependent adopted, recreated or edited after it was classified is not
//! touched on a stale verdict: the store answers `Conflict` and the next
//! tick re-reads it.

use async_trait::async_trait;
use engenho_store::{ResourceKey, revision::Revision};
use engenho_types::kind::Scope;
use serde_json::Value;

use super::served::{ServedKind, ServedKinds, Unresolvable};
use crate::effect::Effect;
use crate::error::ControllerError;
use crate::meta::ObjectMeta;
use crate::owner::OwnerReference;
use crate::status::resource_version_of;

/// The finalizer an owner carries while its dependents are deleted first.
pub const FOREGROUND_DELETION: &str = "foregroundDeletion";

/// Everything collection reads and writes. The store in production; a
/// recording fake in tests.
#[async_trait]
pub trait GcEnv: Send + Sync {
    /// The live object at `key`, if any.
    async fn get(&self, key: &ResourceKey) -> Option<Value>;

    /// Delete the object at `key` if it is still at revision `pinned`.
    ///
    /// # Errors
    ///
    /// [`ControllerError`] on a transport failure. A refused write is an
    /// [`Effect::Rejected`], not an error.
    async fn delete(&self, key: &ResourceKey, pinned: Revision) -> Result<Effect, ControllerError>;

    /// Replace `metadata.ownerReferences` of the object at `key` with
    /// `references`, if it is still at revision `pinned`.
    ///
    /// # Errors
    ///
    /// As [`GcEnv::delete`].
    async fn replace_owner_references(
        &self,
        key: &ResourceKey,
        references: Vec<Value>,
        pinned: Revision,
    ) -> Result<Effect, ControllerError>;
}

/// How one owner reference stands, read live (upstream `classifyReferences`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerState {
    /// The owner exists with the recorded uid and is not waiting on its
    /// dependents. A Terminating owner with no `foregroundDeletion`
    /// finalizer is solid: it may be orphaning them.
    Solid,
    /// No object at the owner's coordinates holds the recorded uid: it is
    /// absent, or its name belongs to another incarnation.
    Dangling,
    /// The owner is being deleted in the foreground and waits for its
    /// dependents to go first.
    WaitingForDependents,
}

/// The state of a live owner object whose uid matched.
fn state_of_live_owner(owner: &Value) -> OwnerState {
    if owner.deletion_timestamp().is_some() && owner.has_finalizer(FOREGROUND_DELETION) {
        OwnerState::WaitingForDependents
    } else {
        OwnerState::Solid
    }
}

fn key_at(kind: &ServedKind, namespace: Option<&str>, name: &str) -> ResourceKey {
    match namespace {
        Some(ns) => ResourceKey::namespaced(&kind.group, &kind.version, &kind.kind, ns, name),
        None => ResourceKey::cluster_scoped(&kind.group, &kind.version, &kind.kind, name),
    }
}

/// Classify one owner reference of a dependent in `dependent_namespace`
/// (`None` for a cluster-scoped dependent).
///
/// The owner is looked up at its own coordinates only: the dependent's
/// namespace for a namespaced kind, cluster-wide for a cluster-scoped one.
/// Never by uid alone, never in another namespace.
///
/// # Errors
///
/// [`Unresolvable`] when the reference's kind is not served at its exact
/// apiVersion, or a cluster-scoped dependent names a namespaced owner kind.
/// No read is made in either case.
pub async fn owner_state(
    env: &dyn GcEnv,
    served: &ServedKinds,
    dependent_namespace: Option<&str>,
    owner: &OwnerReference,
) -> Result<OwnerState, Unresolvable> {
    let kind = served.resolve(&owner.api_version, &owner.kind)?;
    let namespace = match (kind.scope, dependent_namespace) {
        (Scope::Namespaced, Some(ns)) => Some(ns),
        (Scope::Namespaced, None) => return Err(Unresolvable::NamespacedOwnerOfClusterScoped),
        (Scope::Cluster, _) => None,
    };
    // The reference's own version first: for a kind served at one version,
    // that is the only read. A kind served at several may hold the owner
    // under any of them (see `ServedKinds::other_versions`).
    for at in std::iter::once(kind).chain(served.other_versions(kind)) {
        let Some(found) = env.get(&key_at(at, namespace, &owner.name)).await else {
            continue;
        };
        if found.uid() == Some(owner.uid.as_str()) {
            return Ok(state_of_live_owner(&found));
        }
    }
    Ok(OwnerState::Dangling)
}

/// One owner reference as the item holds it, and how it stands.
#[derive(Clone, Debug, PartialEq)]
pub struct Classified {
    /// The entry exactly as it sits in `metadata.ownerReferences`.
    pub raw: Value,
    /// The entry, parsed.
    pub reference: OwnerReference,
    /// Its state.
    pub state: OwnerState,
}

/// Every owner reference of one dependent, classified in list order.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Classification {
    /// One per entry, in order.
    pub references: Vec<Classified>,
}

/// What a classification calls for (upstream `attemptToDeleteItem`'s switch).
#[derive(Clone, Debug, PartialEq)]
pub enum Decision {
    /// Every owner is solid: nothing to do.
    Keep,
    /// Some owner is solid and some are not: keep the item, and drop the
    /// references that are not solid so a foreground owner can finish and a
    /// dead one stops being named.
    RemoveReferences {
        /// The entries that stay, verbatim and in order.
        keep: Vec<Value>,
        /// The uids of the entries that go.
        remove: Vec<String>,
    },
    /// No owner is solid: delete the item.
    Delete,
}

impl Classification {
    /// The uids whose state is `state`, in order.
    #[must_use]
    pub fn uids(&self, state: OwnerState) -> Vec<&str> {
        self.references
            .iter()
            .filter(|c| c.state == state)
            .map(|c| c.reference.uid.as_str())
            .collect()
    }

    /// What to do with the item. A classification of no references is
    /// [`Decision::Delete`]; [`collect`] never builds one.
    #[must_use]
    pub fn decide(&self) -> Decision {
        let solid = |c: &&Classified| c.state == OwnerState::Solid;
        if !self.references.iter().any(|c| solid(&c)) {
            return Decision::Delete;
        }
        let (keep, remove): (Vec<&Classified>, Vec<&Classified>) =
            self.references.iter().partition(solid);
        if remove.is_empty() {
            return Decision::Keep;
        }
        Decision::RemoveReferences {
            keep: keep.into_iter().map(|c| c.raw.clone()).collect(),
            remove: remove
                .into_iter()
                .map(|c| c.reference.uid.clone())
                .collect(),
        }
    }
}

/// `metadata.ownerReferences` of `item`: absent or `null` is none.
///
/// # Errors
///
/// [`Unresolvable::NotAList`] when the field holds anything else.
pub fn owner_references(item: &Value) -> Result<&[Value], Unresolvable> {
    match item.get("metadata").and_then(|m| m.get("ownerReferences")) {
        None | Some(Value::Null) => Ok(&[]),
        Some(Value::Array(refs)) => Ok(refs),
        Some(_) => Err(Unresolvable::NotAList),
    }
}

/// Classify every reference in `references`, in order, stopping at the
/// first one that cannot be classified (upstream does the same: one
/// unresolvable reference pins the dependent even when every other one is
/// verifiably dangling).
///
/// # Errors
///
/// The first [`Unresolvable`], including [`Unresolvable::Malformed`] for an
/// entry that does not parse.
pub async fn classify(
    env: &dyn GcEnv,
    served: &ServedKinds,
    dependent_namespace: Option<&str>,
    references: &[Value],
) -> Result<Classification, Unresolvable> {
    let mut out = Classification::default();
    for (index, raw) in references.iter().enumerate() {
        let reference = serde_json::from_value::<OwnerReference>(raw.clone())
            .map_err(|_| Unresolvable::Malformed { index })?;
        let state = owner_state(env, served, dependent_namespace, &reference).await?;
        out.references.push(Classified {
            raw: raw.clone(),
            reference,
            state,
        });
    }
    Ok(out)
}

/// One dependent to collect, as the scan saw it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidate {
    /// Where it lives.
    pub key: ResourceKey,
    /// The incarnation the scan saw.
    pub uid: String,
    /// The scan saw a `deletionTimestamp` on it.
    pub being_deleted: bool,
}

impl Candidate {
    /// A listed object worth collecting: one that names at least one owner.
    /// `None` without owner references, or without a uid (the store stamps
    /// one on every write, so a uid-less object is not one it holds).
    #[must_use]
    pub fn listed(key: ResourceKey, value: &Value) -> Option<Self> {
        if owner_references(value).is_ok_and(<[Value]>::is_empty) {
            return None;
        }
        Some(Self {
            uid: value.uid()?.to_owned(),
            being_deleted: value.deletion_timestamp().is_some(),
            key,
        })
    }
}

/// What [`collect`] did with one dependent. Each outcome is named; none of
/// them is a silent default.
#[derive(Clone, Debug, PartialEq)]
pub enum Collected {
    /// The item is already being deleted. Nothing proposed.
    BeingDeleted,
    /// The item is gone, or its name now holds another uid. Nothing proposed.
    ItemGone,
    /// The item names no owner. Nothing proposed.
    NoOwners,
    /// The item carries no resourceVersion to pin a write to. Nothing
    /// proposed.
    Unpinned,
    /// An owner reference could not be classified. The item is left exactly
    /// as it is and read again next tick.
    Unresolvable(Unresolvable),
    /// Every owner is solid. Nothing proposed.
    Kept,
    /// A solid owner remains; the other references were removed.
    ReferencesRemoved {
        /// The uids removed.
        uids: Vec<String>,
        /// What the store did with the patch.
        effect: Effect,
    },
    /// No owner is solid; the item was deleted.
    Deleted(Effect),
}

/// Collect one dependent: classify every owner reference it holds and act.
///
/// # Errors
///
/// [`ControllerError`] only on a transport failure of a write.
pub async fn collect(
    env: &dyn GcEnv,
    served: &ServedKinds,
    item: &Candidate,
) -> Result<Collected, ControllerError> {
    if item.being_deleted {
        return Ok(Collected::BeingDeleted);
    }
    let Some(live) = env.get(&item.key).await else {
        return Ok(Collected::ItemGone);
    };
    if live.uid() != Some(item.uid.as_str()) {
        return Ok(Collected::ItemGone);
    }
    if live.deletion_timestamp().is_some() {
        return Ok(Collected::BeingDeleted);
    }
    let references = match owner_references(&live) {
        Ok([]) => return Ok(Collected::NoOwners),
        Ok(references) => references,
        Err(why) => return Ok(Collected::Unresolvable(why)),
    };
    let Some(pinned) = resource_version_of(&live) else {
        return Ok(Collected::Unpinned);
    };
    let classification =
        match classify(env, served, item.key.namespace.as_deref(), references).await {
            Ok(c) => c,
            Err(why) => return Ok(Collected::Unresolvable(why)),
        };
    Ok(match classification.decide() {
        Decision::Keep => Collected::Kept,
        Decision::RemoveReferences { keep, remove } => Collected::ReferencesRemoved {
            effect: env
                .replace_owner_references(&item.key, keep, pinned)
                .await?,
            uids: remove,
        },
        Decision::Delete => Collected::Deleted(env.delete(&item.key, pinned).await?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn classified(uid: &str, state: OwnerState) -> Classified {
        Classified {
            raw: json!({"apiVersion": "v1", "kind": "ReplicationController", "name": uid, "uid": uid}),
            reference: OwnerReference {
                api_version: "v1".into(),
                kind: "ReplicationController".into(),
                name: uid.into(),
                uid: uid.into(),
                controller: false,
                block_owner_deletion: false,
            },
            state,
        }
    }

    fn decide(states: &[(&str, OwnerState)]) -> Decision {
        Classification {
            references: states.iter().map(|(u, s)| classified(u, *s)).collect(),
        }
        .decide()
    }

    #[test]
    fn a_solid_owner_keeps_the_item_whatever_the_others_are() {
        use OwnerState::*;
        assert_eq!(decide(&[("a", Solid), ("b", Solid)]), Decision::Keep);
        let Decision::RemoveReferences { keep, remove } = decide(&[
            ("gone", Dangling),
            ("live", Solid),
            ("fg", WaitingForDependents),
        ]) else {
            panic!("a solid owner must keep the item");
        };
        assert_eq!(remove, ["gone", "fg"]);
        assert_eq!(keep, [classified("live", Solid).raw]);
    }

    #[test]
    fn no_solid_owner_deletes_the_item() {
        use OwnerState::*;
        assert_eq!(decide(&[("gone", Dangling)]), Decision::Delete);
        assert_eq!(decide(&[("fg", WaitingForDependents)]), Decision::Delete);
        assert_eq!(
            decide(&[("gone", Dangling), ("fg", WaitingForDependents)]),
            Decision::Delete
        );
    }

    #[test]
    fn only_a_foreground_deleting_owner_is_waiting() {
        let ts = "2026-01-01T00:00:00Z";
        let owner = |meta: Value| json!({"metadata": meta});
        assert_eq!(
            state_of_live_owner(&owner(
                json!({"deletionTimestamp": ts, "finalizers": [FOREGROUND_DELETION]})
            )),
            OwnerState::WaitingForDependents
        );
        for solid in [
            json!({"deletionTimestamp": ts, "finalizers": ["orphan"]}),
            json!({"deletionTimestamp": ts}),
            json!({"finalizers": [FOREGROUND_DELETION]}),
            json!({}),
        ] {
            assert_eq!(
                state_of_live_owner(&owner(solid.clone())),
                OwnerState::Solid,
                "{solid}"
            );
        }
    }

    #[test]
    fn owner_references_reads_a_list_or_nothing() {
        assert_eq!(owner_references(&json!({})), Ok(&[][..]));
        assert_eq!(
            owner_references(&json!({"metadata": {"ownerReferences": null}})),
            Ok(&[][..])
        );
        assert_eq!(
            owner_references(&json!({"metadata": {"ownerReferences": {}}})),
            Err(Unresolvable::NotAList)
        );
    }

    #[test]
    fn a_listed_object_is_a_candidate_only_with_owners_and_a_uid() {
        let key = ResourceKey::namespaced("", "v1", "Pod", "ns", "p");
        let owned = json!({"metadata": {"uid": "u", "ownerReferences": [{"uid": "o"}]}});
        assert_eq!(
            Candidate::listed(key.clone(), &owned),
            Some(Candidate {
                key: key.clone(),
                uid: "u".into(),
                being_deleted: false
            })
        );
        assert_eq!(
            Candidate::listed(key.clone(), &json!({"metadata": {"uid": "u"}})),
            None
        );
        assert_eq!(
            Candidate::listed(key, &json!({"metadata": {"ownerReferences": [{}]}})),
            None
        );
    }
}
