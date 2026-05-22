//! The `Controller` trait — second-site extraction of the
//! reconcile-loop pattern. First site: `engenho-scheduler::Scheduler`.
//!
//! A controller observes resources of a particular kind + (re)
//! drives the world toward the declared spec. Pure I/O at the
//! boundary; pure decision logic inside.

use async_trait::async_trait;

use crate::error::ControllerError;

/// One controller — implements the standard reconcile-loop shape.
#[async_trait]
pub trait Controller: Send + Sync {
    /// Stable name for telemetry + runtime registration.
    fn name(&self) -> &'static str;

    /// One reconcile tick. Returns a typed [`ReconcileReport`] so
    /// the runtime can log + collect metrics.
    async fn tick(&self) -> Result<ReconcileReport, ControllerError>;
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
            async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
                Ok(ReconcileReport::default())
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
}
