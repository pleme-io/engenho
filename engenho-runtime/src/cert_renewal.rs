//! The loop that renews a node's leaf certificates
//! ([`engenho_apiserver::pki_rotation`]). No store event wakes it: the clock
//! is its input.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use engenho_apiserver::ClientMaterial;
use engenho_apiserver::pki_rotation::CertRenewer;
use engenho_controllers::{
    Controller, ControllerError, DeclaresReads, Reads, ReconcileOutcome, ReconcileReport,
};

/// Persists and publishes a renewed admin leaf.
pub(crate) type PublishClient = Arc<dyn Fn(&ClientMaterial) -> Result<(), String> + Send + Sync>;

/// Seconds since the epoch, by this node's clock.
pub(crate) fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// One tick asks the renewer; with TLS or rotation off there is none.
#[derive(Clone)]
pub(crate) struct CertRenewalController {
    renewer: Option<Arc<CertRenewer>>,
    publish: PublishClient,
}

impl CertRenewalController {
    pub(crate) fn new(renewer: Option<Arc<CertRenewer>>, publish: PublishClient) -> Self {
        Self { renewer, publish }
    }
}

impl DeclaresReads for CertRenewalController {
    fn reads(&self) -> Reads {
        Reads::nothing()
    }
}

#[async_trait::async_trait]
impl Controller for CertRenewalController {
    fn name(&self) -> &'static str {
        "cert-renewal"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        if let Some(renewer) = &self.renewer {
            renewer
                .tick(unix_now(), |admin| (self.publish)(admin))
                .await;
        }
        Ok(ReconcileOutcome::from(ReconcileReport::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn with_no_renewer_a_tick_does_nothing_and_succeeds() {
        let c = CertRenewalController::new(None, Arc::new(|_| panic!("nothing to publish")));
        assert_eq!(c.name(), "cert-renewal");
        assert!(c.tick().await.is_ok());
        assert_eq!(c.reads().kinds(), Some(&[][..]));
    }

    #[test]
    fn the_clock_reads_after_this_code_was_written() {
        assert!(unix_now() > 1_790_000_000);
    }

    #[tokio::test]
    async fn a_tick_asks_the_renewer_at_the_clock_and_finds_nothing_due_after_first_issue() {
        let dir = tempfile::tempdir().unwrap();
        let ca = Arc::new(engenho_apiserver::load_or_generate_ca(dir.path()).unwrap());
        let first = CertRenewer::issue_first(
            ca,
            engenho_apiserver::pki_rotation::OwnedSans {
                node_name: "n".to_string(),
                listen_ip: None,
                extra_sans: Vec::new(),
            },
            engenho_apiserver::pki_rotation::Leases::start(),
            unix_now(),
        )
        .await
        .unwrap();
        let renewer = Arc::new(first.renewer);
        let counts = renewer.counts();
        let c = CertRenewalController::new(
            Some(renewer),
            Arc::new(|_| panic!("nothing is due inside a period")),
        );
        c.tick().await.unwrap();
        assert_eq!((counts.renewed(), counts.failed()), (0, 0));
    }
}
