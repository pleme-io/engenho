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

use std::ops::Deref;
use std::time::Duration;

use async_trait::async_trait;

use crate::error::ControllerError;

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
    /// now (a one-shot re-tick at `delay`).
    RequeueWithProgress(Duration),
}

impl Default for ReconcileResult {
    fn default() -> Self {
        Self::Done
    }
}

impl ReconcileResult {
    /// The follow-up delay this result asks for, if any. `Done` →
    /// `None`; both requeue variants → `Some(delay)`. Drivers arm a
    /// one-shot re-tick when this is `Some`.
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
}

impl ReconcileOutcome {
    /// Build an outcome from a report + an explicit requeue decision.
    #[must_use]
    pub fn new(report: ReconcileReport, result: ReconcileResult) -> Self {
        Self { report, result }
    }

    /// Log the embedded report (delegates to [`ReconcileReport::log`]).
    pub fn log(&self, controller_name: &str) {
        self.report.log(controller_name);
    }
}

impl From<ReconcileReport> for ReconcileOutcome {
    fn from(report: ReconcileReport) -> Self {
        Self {
            report,
            result: ReconcileResult::Done,
        }
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

    #[test]
    fn report_default_is_empty() {
        let r = ReconcileReport::default();
        assert_eq!(r.objects_examined, 0);
        assert_eq!(r.objects_changed, 0);
        assert_eq!(r.objects_skipped, 0);
        assert!(r.note.is_none());
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
        let outcome: ReconcileOutcome = ReconcileReport {
            objects_examined: 3,
            objects_changed: 2,
            ..Default::default()
        }
        .into();
        assert_eq!(outcome.result, ReconcileResult::Done);
        // Deref reaches the report fields directly.
        assert_eq!(outcome.objects_examined, 3);
        assert_eq!(outcome.objects_changed, 2);
    }
}
