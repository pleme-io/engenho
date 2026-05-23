//! ProvacaoConduit — Conduit wrapper with typed chaos injection.
//!
//! Wraps the standard [`Conduit`] in a typed Provacao<FonteFault>
//! injection point. Each tick rolls the Provacao policies first; if
//! a typed fault fires, it short-circuits the tick as a typed
//! `FonteError`. Otherwise the underlying Conduit runs normally.
//!
//! Used to test the convergence loop's resilience: configure
//! EveryNth or Probability policies + verify the loop's downstream
//! handlers (retry budgets, anomaly chain, mirante alerts) react
//! correctly to typed faults.

use crate::{FonteError, FonteResult, Outcome};
use engenho_substrate::provacao::Provacao;
use engenho_substrate::relogio::Clock;
use std::sync::Arc;
use thiserror::Error;

/// Typed fault kinds that the chaos layer can inject between
/// Conduit ticks. Each maps to a typed `FonteError` so consumer
/// retry/alert logic uses the same code paths as real failures.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FonteFault {
    /// Simulate a Watcher failure — emerges as `FonteError::Watch`.
    #[error("provacao: simulated watcher fault")]
    WatchFault,
    /// Simulate an Evaluator failure — emerges as `FonteError::Eval`
    /// (via a typed Invariant inside).
    #[error("provacao: simulated evaluator fault")]
    EvalFault,
    /// Simulate a Proposer failure — emerges as `FonteError::Propose`.
    #[error("provacao: simulated proposer fault")]
    ProposeFault,
    /// Simulate an Attester failure — emerges as `FonteError::Attest`.
    #[error("provacao: simulated attester fault")]
    AttestFault,
    /// Simulate a Publisher failure — emerges as `FonteError::Publish`.
    #[error("provacao: simulated publisher fault")]
    PublishFault,
}

engenho_substrate::impl_error_kind! {
    FonteFault {
        WatchFault   => "watch_fault",
        EvalFault    => "eval_fault",
        ProposeFault => "propose_fault",
        AttestFault  => "attest_fault",
        PublishFault => "publish_fault",
    }
}

impl FonteFault {
    /// Translate to the typed FonteError flavor the substrate
    /// downstream expects.
    #[must_use]
    pub fn into_fonte_error(self) -> FonteError {
        match self {
            Self::WatchFault => FonteError::Watch("provacao: watcher fault".into()),
            Self::EvalFault => FonteError::Eval(engenho_sui_typescape::TypescapeError::Invariant {
                location: "provacao".into(),
                reason: "simulated evaluator fault".into(),
            }),
            Self::ProposeFault => FonteError::Propose("provacao: proposer fault".into()),
            Self::AttestFault => FonteError::Attest("provacao: attester fault".into()),
            Self::PublishFault => FonteError::Publish("provacao: publisher fault".into()),
        }
    }
}

/// Conduit wrapper that injects typed chaos via Provacao before each
/// tick. Real Conduit runs unchanged when no fault fires.
pub struct ProvacaoConduit {
    inner: crate::Conduit,
    provacao: Arc<Provacao<FonteFault>>,
    clock: Arc<dyn Clock>,
}

impl ProvacaoConduit {
    /// Wrap a Conduit with a typed chaos layer + clock.
    #[must_use]
    pub fn new(
        inner: crate::Conduit,
        provacao: Arc<Provacao<FonteFault>>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            inner,
            provacao,
            clock,
        }
    }

    /// Roll Provacao first; if it returns a typed fault, error the
    /// tick with the matching FonteError. Otherwise delegate to
    /// the underlying Conduit.
    pub async fn tick(&self) -> FonteResult<Option<Outcome>> {
        if let Some(fault) = self.provacao.maybe_fault(self.clock.as_ref()) {
            return Err(fault.into_fonte_error());
        }
        self.inner.tick().await
    }

    /// Drain — like Conduit::drain but each tick is chaos-gated.
    pub async fn drain(&self) -> FonteResult<Vec<Outcome>> {
        let mut out = Vec::new();
        loop {
            match self.tick().await {
                Ok(Some(o)) => out.push(o),
                Ok(None) => return Ok(out),
                Err(e) => return Err(e),
            }
        }
    }

    /// Borrow the inner Conduit for assertions.
    #[must_use]
    pub fn inner(&self) -> &crate::Conduit {
        &self.inner
    }

    /// Borrow the Provacao injector for policy queries.
    #[must_use]
    pub fn provacao(&self) -> &Arc<Provacao<FonteFault>> {
        &self.provacao
    }
}
