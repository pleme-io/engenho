//! `OwnedChildrenReconciler` — the blanket-impl collapse of the
//! owned-children controller family.
//!
//! ## The duplication this primitive removes
//!
//! Four status-writing workload controllers (ReplicaSet / Deployment /
//! StatefulSet / Job) carried a byte-identical `tick` skeleton: list
//! parents → skip parents with no `metadata.uid` → build the parent's
//! owner-ref → gather owned children → reconcile the child delta →
//! re-list the children → write `.status` via CAS. The ONLY thing that
//! differed between them was (a) which parent kind they sweep and (b)
//! the per-parent reconcile body (`reconcile_one`) + status math
//! (`compute_status`).
//!
//! Per the Prime Directive (extend a primitive; collapse duplication to
//! a trait + blanket impl, the way [`crate::status::write_status_cas`]
//! collapsed the status half) this module hoists the shared S1–S6
//! skeleton into ONE place. Each concrete controller now reduces to:
//!
//!   * [`OwnedChildrenReconciler::parent_gvk`] — the parent it sweeps,
//!   * [`OwnedChildrenReconciler::child_kinds`] — the child kind(s) it
//!     gathers as owned children,
//!   * [`OwnedChildrenReconciler::reconcile_one`] — the per-parent child
//!     delta (typed [`ResourceCommand`]s, never `format!()` of JSON),
//!   * [`OwnedChildrenReconciler::compute_status`] — the `.status` JSON
//!     computed from the LIVE children AFTER the delta committed.
//!
//! The blanket `impl<T: OwnedChildrenReconciler> Controller for T` runs
//! the shared skeleton exactly once, in the same order, with the same
//! [`ReconcileReport`] bookkeeping the hand-written ticks performed —
//! so the migration is behavior-preserving by construction.
//!
//! ## Out of family (left as raw `impl Controller`)
//!
//! `endpoints` (selector-based, not owner-based; idempotency via a
//! custom subset diff, NOT `write_status_cas`) and `gc` (an orphan
//! sweep with NO single parent kind) are SIBLING shapes — they stay raw.
//! Per the Prime Directive's "stop at diminishing returns," the blanket
//! is NOT over-fit to those two outliers; if a third selector-shaped
//! controller ripens, a sibling `SelectorReconciler` trait is the move.

use async_trait::async_trait;
use engenho_store::{StoreMesh, command::ResourceCommand, resource::ResourceKey};
use serde_json::Value;

use crate::controller::{Controller, ReconcileOutcome, ReconcileReport, ReconcileResult};
use crate::create_stamp::{CreateClock, stamp_create_timestamp, wall_clock};
use crate::error::ControllerError;
use crate::meta::ObjectMeta;
use crate::owner::is_owned_by;
use crate::status::write_status_cas;

/// `(group, version, kind)` of a child kind the parent owns. Borrowed
/// `'static` literals — every controller passes its own canonical
/// strings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChildKind {
    pub group: &'static str,
    pub version: &'static str,
    pub kind: &'static str,
}

impl ChildKind {
    #[must_use]
    pub const fn new(group: &'static str, version: &'static str, kind: &'static str) -> Self {
        Self {
            group,
            version,
            kind,
        }
    }
}

/// The `(group, version, kind)` of the parent an owned-children
/// controller sweeps, plus the `apiVersion` literal its owner-refs
/// stamp. A typed struct (not a bare tuple) so the four fields can't be
/// transposed at a call site.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParentGvk {
    pub group: &'static str,
    pub version: &'static str,
    pub kind: &'static str,
    /// The `apiVersion` literal owner-refs use (e.g. `"apps/v1"`,
    /// `"batch/v1"`). Distinct from `group`/`version` because a child's
    /// owner-ref carries the combined `apiVersion` string.
    pub api_version: &'static str,
}

impl ParentGvk {
    #[must_use]
    pub const fn new(
        group: &'static str,
        version: &'static str,
        kind: &'static str,
        api_version: &'static str,
    ) -> Self {
        Self {
            group,
            version,
            kind,
            api_version,
        }
    }
}

/// The typed delta one owned-children parent produces in a tick: the
/// child commands to propose (creates / evictions / scales) authored as
/// typed [`ResourceCommand`]s — never `format!()` of JSON.
#[derive(Debug, Default)]
pub struct ReconcileDelta {
    /// Commands to propose, in order. Each committed command bumps
    /// `ReconcileReport.objects_changed`.
    pub commands: Vec<ResourceCommand>,
}

impl ReconcileDelta {
    /// Empty delta — the parent is already at its fixpoint.
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    /// Build a delta from a command vec.
    #[must_use]
    pub fn from_commands(commands: Vec<ResourceCommand>) -> Self {
        Self { commands }
    }
}

/// The shared owned-children reconcile shape. Implement this for a
/// controller and it gets `Controller` for free via the blanket impl
/// below — `parent_gvk` + `child_kinds` + `reconcile_one` +
/// `compute_status` is the WHOLE per-controller surface.
#[async_trait]
pub trait OwnedChildrenReconciler: Send + Sync {
    /// Stable controller name (telemetry + registration).
    fn name(&self) -> &'static str;

    /// The parent kind this controller sweeps + the `apiVersion` its
    /// owner-refs stamp.
    fn parent_gvk(&self) -> ParentGvk;

    /// The child kind(s) the blanket gathers as owned children (filtered
    /// by `is_owned_by(child, parent_uid)`). Usually a single kind
    /// (ReplicaSet→Pod, Deployment→ReplicaSet); the slice supports
    /// controllers that own more than one child kind.
    fn child_kinds(&self) -> &'static [ChildKind];

    /// The store handle the blanket lists + proposes through.
    fn store(&self) -> &StoreMesh;

    /// Optional namespace scope (`None` = all namespaces).
    fn namespace(&self) -> Option<&str>;

    /// The boundary clock the blanket reads ONCE per created child to
    /// freeze `metadata.creationTimestamp` into the replicated `Put`
    /// (see [`crate::create_stamp`]). Defaults to the production
    /// wall-clock; unit tests override it to pin a fixed instant so the
    /// reconcile stays deterministic + the stamped bytes are assertable.
    fn create_clock(&self) -> CreateClock {
        wall_clock
    }

    /// The ONLY per-controller child-reconcile logic: given one live
    /// parent + the owned children gathered BEFORE this delta, return
    /// the typed child delta (creates / evictions / scales). The blanket
    /// proposes each command in order, bumping `objects_changed`.
    ///
    /// # Errors
    ///
    /// Returns [`ControllerError`] only on a genuine store/serialize
    /// failure encountered while computing the delta.
    async fn reconcile_one(
        &self,
        parent: &Value,
        owned: &[(ResourceKey, Value)],
    ) -> Result<ReconcileDelta, ControllerError>;

    /// Compute the parent's desired `.status` from the LIVE owned
    /// children re-listed AFTER the delta committed. `None` means this
    /// controller writes no status (none of the four owned-children
    /// status-writers return `None`, but the surface is open for an
    /// owned-children controller that legitimately has no status).
    ///
    /// `owned_after` is the post-delta owned-children set; the status
    /// math is the per-controller logic (replica counting, phase
    /// aggregation, …).
    fn compute_status(&self, parent: &Value, owned_after: &[(ResourceKey, Value)])
    -> Option<Value>;
}

/// Gather the owned children of `parent_uid` across every kind in
/// `child_kinds`, namespaced to `ns`. The single place
/// `is_owned_by(child, uid)` is applied for the family.
async fn gather_owned(
    store: &StoreMesh,
    child_kinds: &'static [ChildKind],
    parent_uid: &str,
    ns: Option<&str>,
) -> Vec<(ResourceKey, Value)> {
    let mut owned = Vec::new();
    for ck in child_kinds {
        let all = store.list(ck.group, ck.version, ck.kind, ns).await;
        for (k, v) in all {
            if is_owned_by(&v, parent_uid) {
                owned.push((k, v));
            }
        }
    }
    owned
}

#[async_trait]
impl<T: OwnedChildrenReconciler> Controller for T {
    fn name(&self) -> &'static str {
        OwnedChildrenReconciler::name(self)
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let gvk = self.parent_gvk();
        let store = self.store();
        let child_kinds = self.child_kinds();

        // S1 — list parents + record objects_examined.
        let parents = store
            .list(gvk.group, gvk.version, gvk.kind, self.namespace())
            .await;
        let mut report = ReconcileReport::default();
        report.objects_examined = parents.len();

        for (parent_key, parent_value) in &parents {
            // S2 — skip parents the apiserver hasn't stamped a uid on yet.
            let Some(uid) = parent_value.uid() else {
                tracing::debug!(
                    parent = %parent_key.label(),
                    "skipping parent with no metadata.uid"
                );
                report.objects_skipped += 1;
                continue;
            };
            let uid = uid.to_string();
            let ns = parent_key.namespace.as_deref();

            // S3/S4 — gather the owned children (pre-delta) the per-parent
            // logic reconciles against. Owner-ref construction itself is
            // the per-controller logic's concern (it calls
            // owner::owner_ref_for with its own apiVersion literal); the
            // blanket only orchestrates the gather/propose/status order.
            let owned = gather_owned(store, child_kinds, &uid, ns).await;

            // S5 — the ONLY per-controller child reconcile. Propose each
            // typed command in order, bumping objects_changed per commit.
            //
            // creationTimestamp boundary stamp (★★ determinism): a `Put`
            // of a key NOT yet in the store is a CREATE — freeze
            // `metadata.creationTimestamp` from ONE clock read HERE (the
            // leader boundary, before propose) so every Raft replica
            // replays identical bytes. An update-shaped `Put` (key exists)
            // is left untouched: the field is create-time, never bumped.
            let delta = self.reconcile_one(parent_value, &owned).await?;
            for mut command in delta.commands {
                if let ResourceCommand::Put { key, value, .. } = &mut command {
                    if store.get(key).await.is_none() {
                        stamp_create_timestamp(value, self.create_clock());
                    }
                }
                store.propose(command).await?;
                report.objects_changed += 1;
            }

            // S6 — re-list owned children AFTER the delta committed (the
            // store applies proposals synchronously) so the status math
            // reflects this tick's own creates/deletes. Then write the
            // CAS status (the same `write_status_cas` primitive the
            // hand-written ticks used).
            let owned_after = gather_owned(store, child_kinds, &uid, ns).await;
            if let Some(desired_status) = self.compute_status(parent_value, &owned_after) {
                if write_status_cas(store, parent_key, parent_value, &desired_status)
                    .await?
                    .changed()
                {
                    report.objects_changed += 1;
                }
            }
        }

        // Owned-children controllers don't opt into requeue at this brick;
        // result defaults to Done → drivers behave exactly as before.
        Ok(ReconcileOutcome::new(report, ReconcileResult::Done))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_store::command::{Reason, ResourceCommand};
    use engenho_store::{InProcessRouter, default_config};
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;

    const FIXED_TS: &str = "2026-06-14T12:00:00Z";
    fn fixed_clock() -> String {
        FIXED_TS.to_string()
    }

    async fn test_store() -> Arc<StoreMesh> {
        let router = InProcessRouter::new();
        let cfg = default_config("controllers-owned-children").unwrap();
        let store = Arc::new(
            StoreMesh::start(1, "in-process://1".into(), router, cfg)
                .await
                .unwrap(),
        );
        store.initialize_singleton().await.unwrap();
        assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
        store
    }

    /// A minimal owned-children controller: a "Widget" parent owns one
    /// "Gadget" child it creates (a single child per parent, never
    /// updated). The fixed clock proves the blanket stamps
    /// creationTimestamp on the CREATE Put, frozen at the boundary.
    struct WidgetReconciler {
        store: Arc<StoreMesh>,
    }

    #[async_trait]
    impl OwnedChildrenReconciler for WidgetReconciler {
        fn name(&self) -> &'static str {
            "widget"
        }
        fn parent_gvk(&self) -> ParentGvk {
            ParentGvk::new("demo", "v1", "Widget", "demo/v1")
        }
        fn child_kinds(&self) -> &'static [ChildKind] {
            const CK: &[ChildKind] = &[ChildKind::new("demo", "v1", "Gadget")];
            CK
        }
        fn store(&self) -> &StoreMesh {
            &self.store
        }
        fn namespace(&self) -> Option<&str> {
            None
        }
        fn create_clock(&self) -> CreateClock {
            fixed_clock
        }
        async fn reconcile_one(
            &self,
            parent: &Value,
            owned: &[(ResourceKey, Value)],
        ) -> Result<ReconcileDelta, ControllerError> {
            if !owned.is_empty() {
                return Ok(ReconcileDelta::none());
            }
            let ns = parent.namespace().unwrap_or("default").to_string();
            let owner = crate::owner::owner_ref_for(parent, "demo/v1", "Widget").unwrap();
            let mut child = json!({
                "kind": "Gadget", "apiVersion": "demo/v1",
                "metadata": {"name": "g", "namespace": ns}
            });
            crate::owner::set_owner_reference(&mut child, owner);
            let key = ResourceKey::namespaced("demo", "v1", "Gadget", &ns, "g");
            Ok(ReconcileDelta::from_commands(vec![ResourceCommand::Put {
                key,
                value: child,
                expected: None,
                reason: Reason::Controller,
            }]))
        }
        fn compute_status(
            &self,
            _parent: &Value,
            _owned_after: &[(ResourceKey, Value)],
        ) -> Option<Value> {
            None
        }
    }

    #[tokio::test]
    async fn blanket_stamps_creation_timestamp_on_created_child() {
        let store = test_store().await;
        // Seed a parent Widget (the apiserver path would stamp its uid; here
        // we propose it as Operator so the store mints uid deterministically).
        let parent_key = ResourceKey::namespaced("demo", "v1", "Widget", "team-x", "w");
        store
            .propose(ResourceCommand::put(
                parent_key,
                json!({"kind": "Widget", "apiVersion": "demo/v1",
                       "metadata": {"name": "w", "namespace": "team-x"}}),
                Reason::Operator,
            ))
            .await
            .unwrap();

        let c = WidgetReconciler {
            store: store.clone(),
        };
        let out = c.tick().await.unwrap();
        assert!(out.objects_changed >= 1, "the child was created");

        // The created child carries the FROZEN creationTimestamp.
        let child = store
            .get(&ResourceKey::namespaced(
                "demo", "v1", "Gadget", "team-x", "g",
            ))
            .await
            .expect("child Gadget exists");
        assert_eq!(
            child
                .get("metadata")
                .unwrap()
                .get("creationTimestamp")
                .unwrap(),
            FIXED_TS,
            "controller-created child must carry the boundary-frozen creationTimestamp"
        );
    }

    #[tokio::test]
    async fn blanket_does_not_bump_creation_timestamp_on_subsequent_ticks() {
        let store = test_store().await;
        let parent_key = ResourceKey::namespaced("demo", "v1", "Widget", "team-y", "w");
        store
            .propose(ResourceCommand::put(
                parent_key,
                json!({"kind": "Widget", "apiVersion": "demo/v1",
                       "metadata": {"name": "w", "namespace": "team-y"}}),
                Reason::Operator,
            ))
            .await
            .unwrap();

        let c = WidgetReconciler {
            store: store.clone(),
        };
        // First tick creates the child + stamps the timestamp.
        c.tick().await.unwrap();
        let key = ResourceKey::namespaced("demo", "v1", "Gadget", "team-y", "g");
        let ts1 = store
            .get(&key)
            .await
            .unwrap()
            .get("metadata")
            .unwrap()
            .get("creationTimestamp")
            .unwrap()
            .clone();
        assert_eq!(ts1, FIXED_TS);

        // Several more ticks: the child already exists (reconcile_one returns
        // none), and even if a Put were re-issued the store-get-is-none guard
        // prevents a re-stamp. The timestamp is STABLE — never bumped.
        for _ in 0..3 {
            c.tick().await.unwrap();
        }
        let ts2 = store
            .get(&key)
            .await
            .unwrap()
            .get("metadata")
            .unwrap()
            .get("creationTimestamp")
            .unwrap()
            .clone();
        assert_eq!(
            ts1, ts2,
            "creationTimestamp must be stable across reconciles"
        );
    }

    #[test]
    fn parent_gvk_carries_four_fields() {
        let g = ParentGvk::new("apps", "v1", "ReplicaSet", "apps/v1");
        assert_eq!(g.group, "apps");
        assert_eq!(g.version, "v1");
        assert_eq!(g.kind, "ReplicaSet");
        assert_eq!(g.api_version, "apps/v1");
    }

    #[test]
    fn child_kind_construction() {
        let c = ChildKind::new("", "v1", "Pod");
        assert_eq!(c.group, "");
        assert_eq!(c.version, "v1");
        assert_eq!(c.kind, "Pod");
    }

    #[test]
    fn reconcile_delta_none_is_empty() {
        let d = ReconcileDelta::none();
        assert!(d.commands.is_empty());
    }
}
