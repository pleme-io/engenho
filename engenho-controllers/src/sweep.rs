//! PER-OBJECT ISOLATION — one object's failure costs only that object.
//!
//! ★ THE DEFECT. A controller's tick is a sweep: list every object of a
//! kind, reconcile each. Before this module the only way out of an
//! object's work was `?`, and `?` left the whole tick. The pv-binder is
//! the worked case: a PVC whose local-path directory could not be created
//! returned `Err` from `tick`, so every PVC after it in the list stayed
//! Pending — on that tick, the retry, and every tick after, for as long as
//! the one directory failed. Nothing about the other claims was wrong.
//!
//! ★ THE SHAPE. [`Sweep::run`] walks the objects and calls a per-object
//! closure whose future resolves to `Result<ObjectOutcome, SweepAbort>`:
//!
//!   * A [`ControllerError`] cannot be `?`-ed out of that closure: there is
//!     no `From<ControllerError> for SweepAbort`, so it does not compile
//!     (the `compile_fail` example on [`Sweep`] pins it). The error
//!     has to pass through [`ControllerError::triage`] (or
//!     [`ObjectOutcome::settle`], which calls it), and triage reads
//!     [`ControllerError::scope`]: an `Item` error becomes
//!     [`ObjectOutcome::Failed`] and the sweep moves to the next object; only
//!     a `Sweep` error (the store) becomes the [`SweepAbort`] that ends it.
//!   * [`ItemFailure`] and [`SweepAbort`] have private fields and are built
//!     only by triage, so a `Failed` always holds an Item-scoped error and
//!     an abort always holds a Sweep-scoped one.
//!   * The [`SweepReport`] is folded from the outcomes, one per object, and
//!     stores no `examined` count of its own: `examined` is the sum of
//!     changed + unchanged + skipped + failed, so the identity holds by
//!     construction rather than by every call site counting right.
//!
//! ★ A FAILURE IS STILL ANSWERED. Isolating a failure must not make it
//! quieter than aborting did.
//!
//!   * Every failed object is logged with its key and error.
//!   * A Transient failure still earns a targeted retry: each failing
//!     object keeps its own [`Streak`] on [`TRANSIENT_RETRY`], and the
//!     sweep's outcome asks for a requeue at the earliest delay any of
//!     them is owed. Without this a tick that isolated a failure would
//!     return `Ok`, and the driver would wait for the fallback timer.
//!   * A Declarative failure emits an Event on the object itself, so
//!     `kubectl describe` says why it is stuck. It is emitted once per
//!     (object, resourceVersion): a declaration that is still broken on
//!     the next sweep is the same failure, and an Event per sweep would be
//!     a Raft write per fallback tick, forever.
//!
//! ★ WHY `FnMut -> Future` AND NOT AN ASYNC CLOSURE. An `AsyncFnMut` bound
//! reads better and lets the closure mutate captured state directly, but a
//! tick is an `#[async_trait]` future that must be `Send`, and rustc cannot
//! prove that for a generic `AsyncFnMut` today ("implementation of `Send`
//! is not general enough"). So the closure returns a future that owns its
//! borrows (`async move` over copied references), and state shared across
//! objects goes through a lock held for one object's reconcile — the
//! pv-binder's list of PVs claimed this pass is the worked example.
//!
//! Tier-honest: the scope decision is a type (a closure cannot `?` a
//! [`ControllerError`] out; it must be triaged). Whether a controller USES
//! the helper is not — a controller that still loops by hand can still `?`
//! out of its sweep, and only review catches that.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use engenho_store::{ResourceKey, ResourceValue};
use serde_json::Value;
use shigoto_types::failure::FailureKind;

use crate::controller::{ReconcileOutcome, ReconcileReport, ReconcileResult};
use crate::curve::Streak;
use crate::effect::{Effect, Landed};
use crate::error::{ControllerError, ErrorScope};
use crate::event_recorder::{EventRecord, EventSink, InvolvedObject, NullEventSink, Reason};
use crate::watch_driver::TRANSIENT_RETRY;

/// What reconciling one object did. A sweep records exactly one per object.
#[derive(Debug)]
pub enum ObjectOutcome {
    /// A write made for this object landed. The [`Landed`] comes only from
    /// an [`Effect`] that observed it, so an object cannot be reported
    /// changed on the strength of a write having been attempted.
    Changed(Landed),
    /// Examined and already converged: nothing to write.
    Unchanged,
    /// Deliberately not acted on this pass — waiting on something outside
    /// the object, or outside this controller's reach. `note`, when given,
    /// becomes the report's note.
    Skipped { note: Option<&'static str> },
    /// This object's reconcile failed with an Item-scoped error. Built only
    /// by [`ControllerError::triage`].
    Failed(ItemFailure),
}

/// A per-object outcome from what its write did: landed is Changed, a
/// write that changed nothing is Unchanged, and a write the store refused
/// is a Skip carrying the refusal as its note. The object is left as it
/// was, and the next tick re-reads it.
impl From<Effect> for ObjectOutcome {
    fn from(effect: Effect) -> Self {
        match effect {
            Effect::Written(landed) => Self::Changed(landed),
            Effect::Unchanged => Self::Unchanged,
            Effect::Rejected(refusal) => Self::skipped_because(refusal.note()),
        }
    }
}

impl ObjectOutcome {
    /// Skipped, with nothing further to say.
    pub const SKIPPED: Self = Self::Skipped { note: None };

    /// Skipped, with a note for the report.
    #[must_use]
    pub const fn skipped_because(note: &'static str) -> Self {
        Self::Skipped { note: Some(note) }
    }

    /// Settle a per-object result written with ordinary `?`: an `Item`
    /// error becomes [`ObjectOutcome::Failed`], a `Sweep` error the
    /// [`SweepAbort`] that ends the sweep.
    ///
    /// This is how a per-object method returning `ControllerError` joins a
    /// sweep: `?` inside the method leaves only the method, and this call
    /// decides — by scope, never by the call site — whether that is the
    /// end of the object or of the sweep.
    ///
    /// # Errors
    ///
    /// [`SweepAbort`] when the error's scope is [`ErrorScope::Sweep`].
    pub fn settle(result: Result<Self, ControllerError>) -> Result<Self, SweepAbort> {
        match result {
            Ok(outcome) => Ok(outcome),
            Err(e) => e.triage().map(Self::Failed),
        }
    }
}

/// An Item-scoped error, recorded against the one object it belongs to.
///
/// The field is private and the only constructor is
/// [`ControllerError::triage`], so an `ItemFailure` never holds a
/// Sweep-scoped error.
#[derive(Debug)]
pub struct ItemFailure(ControllerError);

impl ItemFailure {
    /// The underlying error.
    #[must_use]
    pub const fn error(&self) -> &ControllerError {
        &self.0
    }

    /// Its retry class.
    #[must_use]
    pub const fn class(&self) -> FailureKind {
        self.0.classify()
    }
}

/// A Sweep-scoped error: the sweep stops here and the tick fails with it.
///
/// Built only by [`ControllerError::triage`]; turns back into the
/// [`ControllerError`] it holds, so a tick can `?` it. Displays as that
/// error, unchanged.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct SweepAbort(ControllerError);

impl From<SweepAbort> for ControllerError {
    fn from(abort: SweepAbort) -> Self {
        abort.0
    }
}

impl ControllerError {
    /// Split this error by [`scope`](Self::scope): an Item error is kept
    /// for its object, a Sweep error ends the sweep.
    ///
    /// # Errors
    ///
    /// [`SweepAbort`] when the scope is [`ErrorScope::Sweep`].
    pub fn triage(self) -> Result<ItemFailure, SweepAbort> {
        match self.scope() {
            ErrorScope::Item => Ok(ItemFailure(self)),
            ErrorScope::Sweep => Err(SweepAbort(self)),
        }
    }
}

/// The tally of one sweep, folded from its per-object outcomes.
///
/// There is no stored `examined`: [`examined`](Self::examined) is the sum of
/// the four outcome counts, so `examined = changed + unchanged + skipped +
/// failed` cannot be broken. The counts are private and only
/// [`Sweep::run`] adds to them.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SweepReport {
    changed: usize,
    unchanged: usize,
    skipped: usize,
    failed: usize,
    note: Option<&'static str>,
    /// The earliest targeted retry any Transient failure in this sweep is
    /// owed, read off that object's own streak.
    retry_after: Option<Duration>,
}

impl SweepReport {
    /// Objects the sweep reached: the sum of the four outcomes.
    #[must_use]
    pub const fn examined(&self) -> usize {
        self.changed + self.unchanged + self.skipped + self.failed
    }

    #[must_use]
    pub const fn changed(&self) -> usize {
        self.changed
    }

    #[must_use]
    pub const fn unchanged(&self) -> usize {
        self.unchanged
    }

    #[must_use]
    pub const fn skipped(&self) -> usize {
        self.skipped
    }

    #[must_use]
    pub const fn failed(&self) -> usize {
        self.failed
    }

    /// The last note a skipped object gave, if any.
    #[must_use]
    pub const fn note(&self) -> Option<&'static str> {
        self.note
    }

    /// When the sweep wants to run again because of a Transient failure.
    #[must_use]
    pub const fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    fn record(&mut self, outcome: &ObjectOutcome) {
        match outcome {
            ObjectOutcome::Changed(_) => self.changed += 1,
            ObjectOutcome::Unchanged => self.unchanged += 1,
            ObjectOutcome::Skipped { note } => {
                self.skipped += 1;
                if note.is_some() {
                    self.note = *note;
                }
            }
            ObjectOutcome::Failed(_) => self.failed += 1,
        }
    }

    fn owe(&mut self, after: Duration) {
        self.retry_after = Some(self.retry_after.map_or(after, |d| d.min(after)));
    }

    /// Log this tally for `controller`, at WARN when anything failed.
    pub fn log(&self, controller: &str) {
        let note = self.note.unwrap_or("");
        if self.failed > 0 {
            tracing::warn!(
                controller,
                examined = self.examined(),
                changed = self.changed,
                unchanged = self.unchanged,
                skipped = self.skipped,
                failed = self.failed,
                note,
                "reconcile tick (objects failed; the rest were reconciled)"
            );
        } else if self.changed > 0 || self.skipped > 0 {
            tracing::info!(
                controller,
                examined = self.examined(),
                changed = self.changed,
                unchanged = self.unchanged,
                skipped = self.skipped,
                note,
                "reconcile tick"
            );
        } else {
            tracing::debug!(
                controller,
                examined = self.examined(),
                "reconcile tick (no-op)"
            );
        }
    }
}

/// The legacy counters, derived. `objects_skipped` is skips only; the
/// failed and unchanged counts live on the [`SweepReport`] the outcome
/// carries alongside.
impl From<SweepReport> for ReconcileReport {
    fn from(s: SweepReport) -> Self {
        Self {
            objects_examined: s.examined(),
            objects_changed: s.changed,
            objects_skipped: s.skipped,
            note: s.note.map(str::to_string),
        }
    }
}

/// A tick's outcome from its sweep: the derived report, the tally itself,
/// and a requeue at the earliest owed retry when a Transient failure was
/// isolated (otherwise `Done`).
impl From<SweepReport> for ReconcileOutcome {
    fn from(s: SweepReport) -> Self {
        let result = s
            .retry_after
            .map_or(ReconcileResult::Done, ReconcileResult::Requeue);
        let mut outcome = Self::new(ReconcileReport::from(s), result);
        outcome.sweep = Some(s);
        outcome
    }
}

/// What a sweep remembers about an object that failed, until it stops
/// failing or disappears.
#[derive(Debug)]
struct Remembered {
    /// Consecutive failed sweeps, read against [`TRANSIENT_RETRY`].
    streak: Streak,
    /// The resourceVersion a Declarative failure was last announced at.
    announced: Announced,
}

#[derive(Debug, PartialEq, Eq)]
enum Announced {
    Never,
    /// An Event was emitted while the object stood at this version
    /// (`None` when the object carried none).
    AtVersion(Option<String>),
}

/// The per-object isolation runner a controller owns, one per controller.
///
/// It holds the controller's failure vocabulary (the component name that
/// signs its Events and the upstream reason a Declarative failure is
/// reported under) and a small memory of objects that are currently
/// failing, bounded by that set: an object leaves it on its first
/// non-failed outcome, or when it is no longer listed.
///
/// The per-object work is written with ordinary `?` against
/// [`ControllerError`], and joins the sweep through
/// [`ObjectOutcome::settle`]:
///
/// ```
/// use engenho_controllers::event_recorder::Reason;
/// use engenho_controllers::{ControllerError, ObjectOutcome, Sweep};
/// use engenho_store::{ResourceKey, ResourceValue};
///
/// fn reconcile(_key: &ResourceKey) -> Result<ObjectOutcome, ControllerError> {
///     Err(ControllerError::Internal("this object's directory".into()))
/// }
///
/// async fn tick(
///     sweep: &Sweep,
///     objects: &[(ResourceKey, ResourceValue)],
/// ) -> Result<usize, ControllerError> {
///     let report = sweep
///         .run(objects, |key, _value| async move {
///             ObjectOutcome::settle(reconcile(key))
///         })
///         .await?;
///     Ok(report.failed())
/// }
/// # let _ = Sweep::new("example", Reason::ProvisioningFailed);
/// ```
///
/// `?`-ing the error straight out of the closure does not compile — there
/// is no way to turn a [`ControllerError`] into a [`SweepAbort`] except by
/// triaging it. This differs from the example above only in that line:
///
/// ```compile_fail
/// use engenho_controllers::event_recorder::Reason;
/// use engenho_controllers::{ControllerError, ObjectOutcome, Sweep};
/// use engenho_store::{ResourceKey, ResourceValue};
///
/// fn reconcile(_key: &ResourceKey) -> Result<ObjectOutcome, ControllerError> {
///     Err(ControllerError::Internal("this object's directory".into()))
/// }
///
/// async fn tick(
///     sweep: &Sweep,
///     objects: &[(ResourceKey, ResourceValue)],
/// ) -> Result<usize, ControllerError> {
///     let report = sweep
///         .run(objects, |key, _value| async move {
///             let outcome = reconcile(key)?;
///             Ok(outcome)
///         })
///         .await?;
///     Ok(report.failed())
/// }
/// # let _ = Sweep::new("example", Reason::ProvisioningFailed);
/// ```
pub struct Sweep {
    component: &'static str,
    failure_reason: Reason,
    events: Arc<dyn EventSink>,
    memory: tokio::sync::Mutex<BTreeMap<ResourceKey, Remembered>>,
}

impl Sweep {
    /// A runner for `component` (upstream's `source.component`, e.g.
    /// `persistentvolume-controller`), announcing Declarative failures
    /// under `failure_reason`. Events go nowhere until
    /// [`with_event_sink`](Self::with_event_sink) wires a sink.
    #[must_use]
    pub fn new(component: &'static str, failure_reason: Reason) -> Self {
        Self {
            component,
            failure_reason,
            events: Arc::new(NullEventSink),
            memory: tokio::sync::Mutex::new(BTreeMap::new()),
        }
    }

    /// Builder: the sink Declarative failures are announced through.
    #[must_use]
    pub fn with_event_sink(mut self, events: Arc<dyn EventSink>) -> Self {
        self.events = events;
        self
    }

    /// Reconcile each of `objects` with `per_object`, isolating failures.
    ///
    /// `per_object` is called once per object, in order, and its future is
    /// awaited before the next call — objects are never reconciled
    /// concurrently, so state shared through a lock is never contended.
    /// Every object is visited in order. An `Ok` outcome is tallied; a
    /// [`ObjectOutcome::Failed`] is tallied, logged, and — by its class —
    /// owed a retry or announced as an Event on the object; the sweep then
    /// continues. Only an `Err(SweepAbort)` stops it, and that error is
    /// returned as-is.
    ///
    /// # Errors
    ///
    /// The first [`SweepAbort`] a per-object call returns.
    pub async fn run<'a, F, Fut>(
        &self,
        objects: &'a [(ResourceKey, ResourceValue)],
        mut per_object: F,
    ) -> Result<SweepReport, SweepAbort>
    where
        F: FnMut(&'a ResourceKey, &'a ResourceValue) -> Fut,
        Fut: Future<Output = Result<ObjectOutcome, SweepAbort>>,
    {
        let mut memory = self.memory.lock().await;
        let mut report = SweepReport::default();
        let mut failing: BTreeSet<&ResourceKey> = BTreeSet::new();
        let mut now: Option<String> = None;

        for (key, value) in objects {
            let outcome = per_object(key, value).await?;
            if let ObjectOutcome::Failed(failure) = &outcome {
                failing.insert(key);
                let seen = memory.entry(key.clone()).or_insert_with(|| Remembered {
                    streak: Streak::new(TRANSIENT_RETRY),
                    announced: Announced::Never,
                });
                let owed = seen.streak.miss();
                match failure.class() {
                    // No targeted retry, the same rule the driver applies
                    // to a Declarative tick: re-trying an unusable
                    // declaration does not fix it. The Event is the answer,
                    // and it and the WARN are given once per version; a
                    // later sweep meeting the same declaration says so at
                    // DEBUG only.
                    FailureKind::Declarative => {
                        let version = Announced::AtVersion(resource_version(value));
                        if seen.announced == version {
                            tracing::debug!(
                                controller = self.component,
                                object = %key.label(),
                                error = %failure.error(),
                                "object still fails (declarative, already announced)"
                            );
                        } else {
                            tracing::warn!(
                                controller = self.component,
                                object = %key.label(),
                                error = %failure.error(),
                                "object reconcile failed (declarative: not retried until it \
                                 changes); the sweep continues with the next object"
                            );
                            let stamp = now
                                .get_or_insert_with(engenho_types::time::now_rfc3339_utc)
                                .clone();
                            self.events
                                .record(EventRecord {
                                    involved: involved_object(key, value),
                                    reason: self.failure_reason,
                                    message: failure.error().to_string(),
                                    component: self.component.to_string(),
                                    timestamp: stamp,
                                })
                                .await;
                            seen.announced = version;
                        }
                    }
                    // Transient, and any class shigoto adds later (the enum
                    // is non_exhaustive): retried on this object's curve,
                    // never faster than a Transient.
                    class => {
                        tracing::warn!(
                            controller = self.component,
                            object = %key.label(),
                            class = ?class,
                            error = %failure.error(),
                            retry_after_ms = u64::try_from(owed.as_millis()).unwrap_or(u64::MAX),
                            "object reconcile failed; retrying it on its own curve, and the \
                             sweep continues with the next object"
                        );
                        report.owe(owed);
                    }
                }
            } else {
                memory.remove(key);
            }
            report.record(&outcome);
        }

        // Every object was reached: forget any that are not failing now —
        // recovered ones were removed above, and this drops the ones that
        // were deleted. The memory is bounded by the failing set.
        memory.retain(|k, _| failing.contains(k));
        Ok(report)
    }
}

/// Give a controller that owns a `sweep: Sweep` field its event-sink
/// builder. Invoked in the controller's own module (the field is private
/// there), once per controller, so the builder is written once:
///
/// ```ignore
/// crate::sweep::impl_sweep_event_sink!(ReplicaSetController);
/// ```
macro_rules! impl_sweep_event_sink {
    ($controller:ty) => {
        impl $controller {
            /// Builder: wire the sink this controller announces a failed
            /// object through — a Warning Event on that object, once per
            /// resourceVersion (see [`crate::sweep::Sweep`]).
            #[must_use]
            pub fn with_event_sink(
                mut self,
                events: ::std::sync::Arc<dyn $crate::event_recorder::EventSink>,
            ) -> Self {
                self.sweep = self.sweep.with_event_sink(events);
                self
            }
        }
    };
}
pub(crate) use impl_sweep_event_sink;

/// `metadata.resourceVersion`, if the object carries one.
fn resource_version(value: &ResourceValue) -> Option<String> {
    value
        .get("metadata")
        .and_then(|m| m.get("resourceVersion"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// The Event subject for a listed object, from its key and its own uid.
fn involved_object(key: &ResourceKey, value: &ResourceValue) -> InvolvedObject {
    let api_version = if key.group.is_empty() {
        key.version.clone()
    } else {
        [key.group.as_str(), "/", key.version.as_str()].concat()
    };
    InvolvedObject {
        api_version,
        kind: key.kind.clone(),
        namespace: key.namespace.clone(),
        name: key.name.clone(),
        uid: value
            .get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(Value::as_str)
            .map(str::to_string),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_recorder::CollectingEventSink;
    use engenho_store::StoreError;
    use engenho_store::command::ResourceOp;
    use serde_json::json;

    fn obj(name: &str, rv: &str) -> (ResourceKey, ResourceValue) {
        (
            ResourceKey::namespaced("", "v1", "PersistentVolumeClaim", "ns", name),
            json!({ "metadata": { "name": name, "namespace": "ns",
                                  "uid": (["uid-", name].concat()), "resourceVersion": rv } }),
        )
    }

    fn objs(names: &[&str]) -> Vec<(ResourceKey, ResourceValue)> {
        names.iter().map(|n| obj(n, "1")).collect()
    }

    /// Per-object answers keyed by name; anything unnamed is Changed.
    fn answer(name: &str) -> Result<ObjectOutcome, ControllerError> {
        match name {
            "broken" => Err(ControllerError::InvalidResource(
                "no storage request".into(),
            )),
            "flaky" => Err(ControllerError::Internal("mkdir: permission denied".into())),
            "store-down" => Err(ControllerError::Store(StoreError::ClientWriteFailed(
                "no leader".into(),
            ))),
            "settled" => Ok(ObjectOutcome::Unchanged),
            "waiting" => Ok(ObjectOutcome::skipped_because("waiting on a driver")),
            _ => Ok(ObjectOutcome::from(Effect::of(ResourceOp::Created))),
        }
    }

    fn sweep_with(events: Arc<CollectingEventSink>) -> Sweep {
        Sweep::new("test-controller", Reason::ProvisioningFailed).with_event_sink(events)
    }

    async fn run(
        sweep: &Sweep,
        objects: &[(ResourceKey, ResourceValue)],
    ) -> Result<SweepReport, SweepAbort> {
        sweep
            .run(objects, |key, _value| {
                std::future::ready(ObjectOutcome::settle(answer(&key.name)))
            })
            .await
    }

    #[tokio::test]
    async fn one_poisoned_object_does_not_stop_the_others() {
        let events = Arc::new(CollectingEventSink::new());
        let sweep = sweep_with(events.clone());
        let objects = objs(&["a", "broken", "b", "flaky", "c"]);
        let mut visited = Vec::new();
        let report = sweep
            .run(&objects, |key, _value| {
                visited.push(key.name.clone());
                std::future::ready(ObjectOutcome::settle(answer(&key.name)))
            })
            .await
            .expect("item failures never abort the sweep");
        assert_eq!(
            visited,
            ["a", "broken", "b", "flaky", "c"],
            "every object was reached"
        );
        assert_eq!(
            report.changed(),
            3,
            "the three healthy objects were reconciled"
        );
        assert_eq!(report.failed(), 2);
    }

    #[tokio::test]
    async fn the_report_identity_holds_for_every_mix_of_outcomes() {
        let names = [
            "a", "broken", "settled", "waiting", "flaky", "b", "settled2",
        ];
        // Every prefix of the list, so each outcome kind is tallied alone
        // and in combination.
        for n in 0..=names.len() {
            let sweep = sweep_with(Arc::new(CollectingEventSink::new()));
            let objects = objs(&names[..n]);
            let report = run(&sweep, &objects).await.unwrap();
            assert_eq!(report.examined(), n, "examined is every object reached");
            assert_eq!(
                report.examined(),
                report.changed() + report.unchanged() + report.skipped() + report.failed(),
            );
            let legacy = ReconcileReport::from(report);
            assert_eq!(legacy.objects_examined, n);
            assert_eq!(legacy.objects_changed, report.changed());
            assert_eq!(legacy.objects_skipped, report.skipped());
        }
    }

    /// An object is changed only if its write landed: a `NoOp` is unchanged
    /// and a refused write is a skip naming the refusal, and neither is
    /// tallied as a change.
    #[tokio::test]
    async fn an_object_is_changed_only_when_its_write_landed() {
        let sweep = sweep_with(Arc::new(CollectingEventSink::new()));
        let objects = objs(&["created", "noop", "conflict"]);
        let report = sweep
            .run(&objects, |key, _value| {
                let op = match key.name.as_str() {
                    "created" => ResourceOp::Created,
                    "noop" => ResourceOp::NoOp,
                    _ => ResourceOp::Conflict,
                };
                std::future::ready(Ok(ObjectOutcome::from(Effect::of(op))))
            })
            .await
            .unwrap();
        assert_eq!(
            (report.changed(), report.unchanged(), report.skipped()),
            (1, 1, 1)
        );
        assert_eq!(
            report.note(),
            Some(crate::effect::Refusal::Conflict.note()),
            "the skip says the store refused the write"
        );
    }

    #[tokio::test]
    async fn a_store_error_stops_the_sweep_at_that_object() {
        let sweep = sweep_with(Arc::new(CollectingEventSink::new()));
        let objects = objs(&["a", "store-down", "b"]);
        let mut visited = Vec::new();
        let err = sweep
            .run(&objects, |key, _value| {
                visited.push(key.name.clone());
                std::future::ready(ObjectOutcome::settle(answer(&key.name)))
            })
            .await
            .expect_err("a store error is sweep-scoped");
        assert_eq!(
            visited,
            ["a", "store-down"],
            "nothing after the store error ran"
        );
        assert!(matches!(
            ControllerError::from(err),
            ControllerError::Store(StoreError::ClientWriteFailed(_))
        ));
    }

    #[test]
    fn triage_splits_by_scope_and_keeps_the_error() {
        let item = ControllerError::Internal("x".into()).triage().unwrap();
        assert!(matches!(item.error(), ControllerError::Internal(_)));
        let abort = ControllerError::Store(StoreError::Fatal("y".into()))
            .triage()
            .unwrap_err();
        // One Display: the abort renders exactly as the error it holds.
        assert_eq!(
            abort.to_string(),
            ControllerError::Store(StoreError::Fatal("y".into())).to_string()
        );
    }

    #[tokio::test]
    async fn a_declarative_failure_is_announced_on_its_object_once_per_version() {
        let events = Arc::new(CollectingEventSink::new());
        let sweep = sweep_with(events.clone());
        let objects = objs(&["a", "broken", "flaky"]);
        run(&sweep, &objects).await.unwrap();
        let first = events.drain();
        assert_eq!(first.len(), 1, "only the Declarative failure is announced");
        let e = &first[0];
        assert_eq!(e.reason, Reason::ProvisioningFailed);
        assert_eq!(e.involved.kind, "PersistentVolumeClaim");
        assert_eq!(e.involved.api_version, "v1");
        assert_eq!(e.involved.namespace.as_deref(), Some("ns"));
        assert_eq!(e.involved.name, "broken");
        assert_eq!(e.involved.uid.as_deref(), Some("uid-broken"));
        assert_eq!(e.component, "test-controller");
        assert!(e.message.contains("no storage request"), "{}", e.message);

        // The same declaration on the next sweep is the same failure.
        run(&sweep, &objects).await.unwrap();
        assert!(
            events.drain().is_empty(),
            "no Event per sweep for an unchanged object"
        );

        // An edited object that still fails is a new failure.
        let edited = vec![obj("broken", "2")];
        run(&sweep, &edited).await.unwrap();
        assert_eq!(events.drain().len(), 1, "a new version is announced again");
    }

    #[tokio::test]
    async fn a_recovered_object_is_announced_again_if_it_breaks_again() {
        let events = Arc::new(CollectingEventSink::new());
        let sweep = sweep_with(events.clone());
        let broken = objs(&["broken"]);
        run(&sweep, &broken).await.unwrap();
        assert_eq!(events.drain().len(), 1);
        // Gone from the list: forgotten.
        run(&sweep, &[]).await.unwrap();
        run(&sweep, &broken).await.unwrap();
        assert_eq!(
            events.drain().len(),
            1,
            "a failure after the object left is new"
        );
    }

    #[tokio::test]
    async fn a_transient_failure_owes_a_retry_that_grows_per_object() {
        let sweep = sweep_with(Arc::new(CollectingEventSink::new()));
        let objects = objs(&["a", "flaky"]);
        let mut owed = Vec::new();
        for _ in 0..4 {
            let report = run(&sweep, &objects).await.unwrap();
            owed.push(
                report
                    .retry_after()
                    .expect("a transient failure asks for a retry"),
            );
        }
        assert_eq!(
            owed,
            (0..4).map(|n| TRANSIENT_RETRY.delay(n)).collect::<Vec<_>>(),
            "the object's own streak, on the shared curve"
        );
        let outcome = ReconcileOutcome::from(run(&sweep, &objects).await.unwrap());
        assert_eq!(
            outcome.result,
            ReconcileResult::Requeue(TRANSIENT_RETRY.delay(4))
        );

        // The same object succeeding resets its streak: it is forgotten,
        // and the sweep owes nothing.
        let report = sweep
            .run(&objects, |_key, _value| {
                std::future::ready(Ok(ObjectOutcome::from(Effect::of(ResourceOp::Replaced))))
            })
            .await
            .unwrap();
        assert_eq!(report.retry_after(), None);
        assert_eq!(ReconcileOutcome::from(report).result, ReconcileResult::Done);
        // ...so its next failure starts from the curve's base again.
        let report = run(&sweep, &objects).await.unwrap();
        assert_eq!(report.retry_after(), Some(TRANSIENT_RETRY.base()));
    }

    #[tokio::test]
    async fn a_declarative_failure_owes_no_targeted_retry() {
        let sweep = sweep_with(Arc::new(CollectingEventSink::new()));
        let report = run(&sweep, &objs(&["broken"])).await.unwrap();
        assert_eq!(report.failed(), 1);
        assert_eq!(report.retry_after(), None);
    }
}
