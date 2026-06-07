//! Typed errors at every stage of the convergence pipeline.

use engenho_sui_typescape::TypescapeError;
use thiserror::Error;

/// Errors raised by any of the five typed roles (Watcher, Evaluator,
/// Proposer, Attester, Publisher) or the supervising [`Conduit`]
/// itself.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FonteError {
    /// The Watcher failed to surface a change (e.g. file vanished,
    /// notify watcher channel closed).
    #[error("fonte/watch: {0}")]
    Watch(String),

    /// The Evaluator rejected the source. Wraps the typed
    /// [`TypescapeError`] from the bridge layer.
    #[error("fonte/eval: {0}")]
    Eval(#[from] TypescapeError),

    /// The Proposer failed to commit the new desired state (Raft
    /// timeout, quorum lost, malformed proposal).
    #[error("fonte/propose: {0}")]
    Propose(String),

    /// The Attester failed to record the transition (BLAKE3 chain
    /// broken, signing key missing, sekiban admission rejected).
    #[error("fonte/attest: {0}")]
    Attest(String),

    /// The Publisher failed to broadcast the outcome (channel closed,
    /// mirante registry full).
    #[error("fonte/publish: {0}")]
    Publish(String),

    /// The Conduit's per-stage budget was exceeded. The convergence
    /// tick decided to back off rather than amplify a runaway loop.
    #[error("fonte/budget: stage `{stage}` exceeded budget after {tries} tries")]
    Budget {
        /// Pipeline stage name (`watch` | `eval` | … | `publish`).
        stage: &'static str,
        /// Number of retries already attempted.
        tries: u32,
    },

    /// The Conduit was asked to shut down mid-tick. The current
    /// change was abandoned cleanly; the next tick will pick up the
    /// next change from the Watcher.
    #[error("fonte/abort: conduit shut down mid-tick")]
    Abort,
}

engenho_substrate::impl_error_kind! {
    FonteError {
        (Watch(_)) => "watch",
        (Eval(_)) => "eval",
        (Propose(_)) => "propose",
        (Attest(_)) => "attest",
        (Publish(_)) => "publish",
        { Budget { .. } } => "budget",
        Abort => "abort",
    }
}

/// Convenience alias for the result type used throughout the crate.
pub type FonteResult<T> = Result<T, FonteError>;
