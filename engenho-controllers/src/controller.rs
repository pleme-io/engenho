//! The `Controller` trait — second-site extraction of the
//! reconcile-loop pattern. First site: `engenho-scheduler::Scheduler`.
//!
//! A controller observes resources of a particular kind + (re)
//! drives the world toward the declared spec. Pure I/O at the
//! boundary; pure decision logic inside.
//!
//! ## The unified loop outcome
//!
//! Pre-unification engenho carried TWO competing loop traits: the live
//! `Controller` (sweep-based, counter-only) and a dead generic
//! `Reconciler<R>` in `engenho-types` that owned a typed
//! requeue-bearing [`ReconcileResult`] but was never driven by anything.
//! Per the Prime Directive (two traits modelling one concept is the
//! duplication-bug), the dead trait is gone and its requeue vocabulary
//! lives HERE, folded into the live loop: `tick` now returns a
//! [`ReconcileOutcome`] carrying BOTH the [`ReconcileReport`] AND a
//! [`ReconcileResult`]. The default `ReconcileResult::Done` preserves
//! today's behavior for every controller that doesn't opt into a
//! requeue — the drivers act on the result only when it's non-`Done`.

use std::fmt;
use std::ops::Deref;
use std::time::Duration;

use async_trait::async_trait;

use crate::effect::Effect;
use crate::error::ControllerError;
use crate::sweep::SweepReport;

/// One controller — implements the standard reconcile-loop shape.
#[async_trait]
pub trait Controller: Send + Sync {
    /// Stable name for telemetry + runtime registration.
    fn name(&self) -> &'static str;

    /// One reconcile tick. Returns a typed [`ReconcileOutcome`] so the
    /// runtime can log the [`ReconcileReport`] AND act on the
    /// [`ReconcileResult`] (requeue decision). A controller that does
    /// not opt into requeue returns `ReconcileResult::Done` (the
    /// default), which the drivers treat exactly as the pre-unification
    /// counter-only behavior.
    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError>;

    /// The type that does this controller's reconciling (T5.11).
    ///
    /// The default names `Self`. An adapter that only forwards to another
    /// controller — `Arc<C>` here, the runtime's `DeclaredHere` — forwards
    /// this too, so what the runtime records for a driver is the type it
    /// really runs, not the wrapper around it. Through a `dyn Controller`
    /// it is the concrete type behind the object.
    fn controller_type(&self) -> ControllerType {
        ControllerType::of::<Self>()
    }
}

/// A type that implements [`Controller`], named by the compiler (T5.11).
///
/// The only constructor is [`ControllerType::of`], which takes the type
/// itself: a catalog row built from it names a type that exists and
/// implements `Controller`, or the row does not compile (E0412/E0433 for a
/// renamed or deleted type, E0277 for one that stopped being a
/// controller).
///
/// It holds `std::any::type_name`, whose exact text Rust does not promise
/// to keep across compiler versions. So it is an identity for catalogs and
/// the gates over them within one build — never something to persist, or
/// to compare with a name written down by hand.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ControllerType {
    path: &'static str,
}

impl ControllerType {
    /// The controller type `C`.
    #[must_use]
    pub fn of<C: Controller + ?Sized>() -> Self {
        Self {
            path: std::any::type_name::<C>(),
        }
    }

    /// The type's full path, generic arguments included.
    #[must_use]
    pub const fn path(self) -> &'static str {
        self.path
    }

    /// The crate that defines the type: the path's first segment.
    #[must_use]
    pub fn krate(self) -> &'static str {
        self.bare().split("::").next().unwrap_or(self.path)
    }

    /// The type's own name: the path's last segment, without generic
    /// arguments.
    #[must_use]
    pub fn ident(self) -> &'static str {
        self.bare().rsplit("::").next().unwrap_or(self.path)
    }

    /// The path without its generic arguments, which may themselves hold
    /// `::`.
    fn bare(self) -> &'static str {
        self.path.split('<').next().unwrap_or(self.path)
    }
}

impl fmt::Display for ControllerType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.path)
    }
}

/// Blanket impl so an `Arc<C>` is itself a [`Controller`]. This lets a caller
/// keep an `Arc<C>` handle (e.g. the kubelet, queried in-process by the
/// apiserver's `/log` subresource) AND hand the SAME controller to a
/// [`crate::WatchDriver`] (which takes its controller by value + re-wraps it in
/// an Arc). Both share one instance — the driver's ticks and the out-of-band
/// queries see the same internal state.
#[async_trait]
impl<C: Controller + ?Sized> Controller for std::sync::Arc<C> {
    fn name(&self) -> &'static str {
        (**self).name()
    }
    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        (**self).tick().await
    }
    fn controller_type(&self) -> ControllerType {
        (**self).controller_type()
    }
}

/// One reconciliation attempt's requeue decision.
///
/// Moved here (was `engenho_types::reconciler::ReconcileResult`, a dead
/// duplicate) so it becomes the LIVE requeue vocabulary the loop
/// propagates. `Done` is the default — every controller that doesn't
/// schedule a follow-up keeps the pre-unification behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileResult {
    /// Reconciliation succeeded; no follow-up scheduled. The next
    /// invocation comes from the next watch event or the fallback timer.
    Done,

    /// Schedule another reconcile after `delay`. Useful for polling
    /// external state (image pulls, external load-balancer provisioning)
    /// the apiserver can't surface via a watch.
    Requeue(Duration),

    /// Like `Requeue` but signalling "I made progress, more work is
    /// left." Lets a future requeue scheduler prioritize this resource
    /// over idle ones. The drivers treat it the same as `Requeue` for
    /// now (the driver's one requeue slot, armed for `delay`).
    RequeueWithProgress(Duration),
}

impl Default for ReconcileResult {
    fn default() -> Self {
        Self::Done
    }
}

impl ReconcileResult {
    /// The follow-up delay this result asks for, if any. `Done` →
    /// `None`; both requeue variants → `Some(delay)`. Read through
    /// [`crate::next_wake`], which arms the driver's one requeue slot
    /// when this is `Some` — a slot, never a spawned task, so repeated
    /// requeues replace each other instead of accumulating.
    #[must_use]
    pub fn requeue_after(&self) -> Option<Duration> {
        match self {
            Self::Done => None,
            Self::Requeue(d) | Self::RequeueWithProgress(d) => Some(*d),
        }
    }
}

/// Outcome of one reconcile tick — the report PLUS the requeue decision.
///
/// `Deref`s to [`ReconcileReport`] so existing call sites that read
/// `outcome.objects_changed` / `outcome.objects_examined` keep
/// compiling unchanged. `From<ReconcileReport>` (result = `Done`) lets
/// every legacy `Ok(report)` site become `Ok(report.into())`.
#[derive(Debug, Default, Clone)]
pub struct ReconcileOutcome {
    pub report: ReconcileReport,
    pub result: ReconcileResult,
    /// The per-object tally, when the tick ran through a
    /// [`crate::sweep::Sweep`]. `report` is derived from it; this keeps the
    /// counts `ReconcileReport` has no field for (unchanged, failed).
    pub sweep: Option<SweepReport>,
}

impl ReconcileOutcome {
    /// Build an outcome from a report + an explicit requeue decision.
    #[must_use]
    pub fn new(report: ReconcileReport, result: ReconcileResult) -> Self {
        Self {
            report,
            result,
            sweep: None,
        }
    }

    /// Log the tick: the sweep's tally when there is one (it can say that
    /// objects failed), else the embedded report.
    pub fn log(&self, controller_name: &str) {
        match &self.sweep {
            Some(sweep) => sweep.log(controller_name),
            None => self.report.log(controller_name),
        }
    }
}

impl From<ReconcileReport> for ReconcileOutcome {
    fn from(report: ReconcileReport) -> Self {
        Self::new(report, ReconcileResult::Done)
    }
}

impl Deref for ReconcileOutcome {
    type Target = ReconcileReport;
    fn deref(&self) -> &Self::Target {
        &self.report
    }
}

/// Outcome of one reconcile tick.
#[derive(Debug, Default, Clone)]
pub struct ReconcileReport {
    pub objects_examined: usize,
    pub objects_changed: usize,
    pub objects_skipped: usize,
    /// Human-readable note for logs.
    pub note: Option<String>,
}

impl ReconcileReport {
    /// Count one write by what it did, never by the fact that it was made.
    ///
    /// A write that landed is a change. One the store accepted and did
    /// nothing with is not counted. One the store refused is a skip: the
    /// object was reached and left as it was.
    pub fn record(&mut self, effect: Effect) {
        match effect {
            Effect::Written(_) => self.objects_changed += 1,
            Effect::Unchanged => {}
            Effect::Rejected(_) => self.objects_skipped += 1,
        }
    }

    /// Convenience: log this report at info level via the
    /// `tracing` crate. The runtime calls this after each tick.
    pub fn log(&self, controller_name: &str) {
        if self.objects_changed > 0 || self.objects_skipped > 0 {
            tracing::info!(
                controller = controller_name,
                examined = self.objects_examined,
                changed = self.objects_changed,
                skipped = self.objects_skipped,
                note = self.note.as_deref().unwrap_or(""),
                "reconcile tick"
            );
        } else {
            tracing::debug!(
                controller = controller_name,
                examined = self.objects_examined,
                "reconcile tick (no-op)"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that the Controller trait is object-safe — meaning
    /// the runtime can hold `Box<dyn Controller>` for dynamic
    /// dispatch over heterogeneous controller types.
    #[test]
    fn controller_trait_is_object_safe() {
        struct Dummy;
        #[async_trait]
        impl Controller for Dummy {
            fn name(&self) -> &'static str {
                "dummy"
            }
            async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
                Ok(ReconcileReport::default().into())
            }
        }
        let _boxed: Box<dyn Controller> = Box::new(Dummy);
    }

    struct Named;
    #[async_trait]
    impl Controller for Named {
        fn name(&self) -> &'static str {
            "named"
        }
        async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
            Ok(ReconcileReport::default().into())
        }
    }

    struct Generic<T>(std::marker::PhantomData<T>);
    #[async_trait]
    impl<T: Send + Sync> Controller for Generic<T> {
        fn name(&self) -> &'static str {
            "generic"
        }
        async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
            Ok(ReconcileReport::default().into())
        }
    }

    /// A controller type is named by its crate and its own name, whatever
    /// generic arguments it carries (they may hold `::` themselves).
    #[test]
    fn a_controller_type_is_its_crate_and_its_name() {
        let named = ControllerType::of::<Named>();
        assert_eq!(named.krate(), "engenho_controllers");
        assert_eq!(named.ident(), "Named");
        let generic = ControllerType::of::<Generic<std::collections::BTreeMap<String, u8>>>();
        assert_eq!(generic.krate(), "engenho_controllers");
        assert_eq!(generic.ident(), "Generic");
        assert_ne!(named, generic);
    }

    /// What a controller reports is the type that reconciles: through an
    /// `Arc` (which the runtime's drivers hold), through a `dyn`, and
    /// through both — never the wrapper.
    #[test]
    fn a_controller_reports_the_type_that_reconciles_not_its_wrapper() {
        let want = ControllerType::of::<Named>();
        assert_eq!(Named.controller_type(), want);
        assert_eq!(std::sync::Arc::new(Named).controller_type(), want);
        let object: std::sync::Arc<dyn Controller> = std::sync::Arc::new(Named);
        assert_eq!(object.controller_type(), want);
        assert_eq!(std::sync::Arc::new(object).controller_type(), want);
    }

    #[test]
    fn report_default_is_empty() {
        let r = ReconcileReport::default();
        assert_eq!(r.objects_examined, 0);
        assert_eq!(r.objects_changed, 0);
        assert_eq!(r.objects_skipped, 0);
        assert!(r.note.is_none());
    }

    /// A write is counted by what it did: landed is a change, a `NoOp` is
    /// nothing, a refusal is a skip. Never "a write was made, add one".
    #[test]
    fn a_report_counts_writes_by_their_effect() {
        use engenho_store::command::ResourceOp;
        let mut r = ReconcileReport::default();
        for op in [
            ResourceOp::Created,
            ResourceOp::Patched,
            ResourceOp::NoOp,
            ResourceOp::NoOp,
            ResourceOp::Conflict,
            ResourceOp::PatchRejected,
        ] {
            r.record(Effect::of(op));
        }
        assert_eq!(r.objects_changed, 2, "only the two writes that landed");
        assert_eq!(r.objects_skipped, 2, "the two refusals");
    }

    #[test]
    fn reconcile_result_default_is_done() {
        // A controller that doesn't opt into requeue returns the default
        // — the drivers then behave exactly as the pre-unification loop.
        assert_eq!(ReconcileResult::default(), ReconcileResult::Done);
        assert_eq!(ReconcileResult::Done.requeue_after(), None);
    }

    #[test]
    fn reconcile_result_requeue_carries_delay() {
        let d = Duration::from_secs(5);
        assert_eq!(ReconcileResult::Requeue(d).requeue_after(), Some(d));
        assert_eq!(
            ReconcileResult::RequeueWithProgress(d).requeue_after(),
            Some(d)
        );
    }

    #[test]
    fn outcome_from_report_defaults_to_done() {
        // Every legacy `Ok(report)` site becomes `Ok(report.into())`
        // with result = Done (identical behavior).
        let mut report = ReconcileReport {
            objects_examined: 3,
            ..Default::default()
        };
        report.record(Effect::of(engenho_store::command::ResourceOp::Created));
        report.record(Effect::of(engenho_store::command::ResourceOp::Patched));
        let outcome: ReconcileOutcome = report.into();
        assert_eq!(outcome.result, ReconcileResult::Done);
        // Deref reaches the report fields directly.
        assert_eq!(outcome.objects_examined, 3);
        assert_eq!(outcome.objects_changed, 2);
    }
}
