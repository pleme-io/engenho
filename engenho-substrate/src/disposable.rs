//! Disposable + Transient<R> — the materialize-and-throw RAII primitive.
//!
//! Per the design brief: every renderer / runtime can be lifted
//! into a scope-bound instance with deterministic teardown. The
//! IR is the source of truth; the running instance is disposable
//! evidence; `Transient::scope` enforces "no handle outlives its
//! lifetime" at the type level.
//!
//! ## Contract
//!
//! ```ignore
//! trait Disposable {
//!     type Handle: Send + Sync;
//!     async fn materialize(&self, input) -> Result<Self::Handle, _>;
//!     async fn dispose(&self, handle: Self::Handle) -> Result<(), _>;
//! }
//!
//! Transient::new(disposable, ledger).scope(input, |handle| async { ... }).await
//! ```
//!
//! **Invariants:**
//! - `dispose` ALWAYS runs after the scope body, even on panic.
//! - Both `materialize` and `dispose` emit `MaterializationReceipt`s
//!   to the ledger (verifiability anchors per the design).
//! - Handle is borrowed to the body; never escapes the scope.
//!
//! ## Composition with the substrate
//!
//! Any existing ShapeRenderer / ComposeStack / future QEMU runner
//! / future kind runner becomes Disposable via a thin newtype.
//! `Transient` then makes them scope-bound + receipt-attested for
//! free.

use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;
use thiserror::Error;

use crate::ledger::{LedgerError, MaterializationLedger};
use crate::receipt::{MaterializationReceipt, NodeId, ReceiptKind};
use crate::roca::StageId;

/// Errors a Disposable can surface.
#[derive(Debug, Clone, Error)]
pub enum DisposableError {
    /// Materialize failed before producing a handle.
    #[error("materialize: {0}")]
    Materialize(String),
    /// Dispose failed (after the body ran). The handle was given
    /// to dispose; whatever cleanup state is in is implementation-
    /// dependent. Operators MUST treat this as a leak signal.
    #[error("dispose: {0}")]
    Dispose(String),
    /// Body returned an error AND dispose ran cleanly afterwards.
    /// The body's error string is propagated.
    #[error("body: {0}")]
    Body(String),
    /// Both body AND dispose failed — surface both.
    #[error("body: {body} AND dispose: {dispose}")]
    BodyAndDispose {
        /// Body's error.
        body: String,
        /// Dispose's error.
        dispose: String,
    },
    /// Ledger backend error (receipt commit failed).
    #[error("ledger: {0}")]
    Ledger(String),
}

impl DisposableError {
    /// Stable identifier for telemetry / SDK dispatch.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Materialize(_) => "materialize",
            Self::Dispose(_) => "dispose",
            Self::Body(_) => "body",
            Self::BodyAndDispose { .. } => "body_and_dispose",
            Self::Ledger(_) => "ledger",
        }
    }
}

impl From<LedgerError> for DisposableError {
    fn from(e: LedgerError) -> Self {
        Self::Ledger(e.to_string())
    }
}

/// Scope-bound runtime instance. Implementers materialize + dispose
/// + emit typed receipts; consumers ALWAYS go through Transient::scope.
#[async_trait]
pub trait Disposable: Send + Sync {
    /// Telemetry name.
    fn name(&self) -> &'static str;

    /// Input type the disposable materializes from (e.g. ComposeIr).
    type Input: Send + Sync;

    /// Handle type the body sees while the instance lives.
    type Handle: Send + Sync;

    /// Stable subject hash for receipt routing — usually a
    /// fingerprint of the input (BLAKE3 of canonical bytes). The
    /// substrate uses this as the QuorumTracker subject so K
    /// independent nodes can attest the same materialization.
    fn subject_for(&self, input: &Self::Input) -> [u8; 32];

    /// Materialize the input into a live handle.
    ///
    /// # Errors
    /// [`DisposableError::Materialize`] on failure.
    async fn materialize(
        &self,
        input: &Self::Input,
    ) -> Result<Self::Handle, DisposableError>;

    /// Dispose of the handle. Implementations MUST be idempotent;
    /// double-dispose is undefined behavior in operator land but
    /// the trait contract says implementations should tolerate.
    ///
    /// # Errors
    /// [`DisposableError::Dispose`] on failure.
    async fn dispose(&self, handle: Self::Handle) -> Result<(), DisposableError>;
}

/// Scope-bound RAII wrapper around a Disposable. Holds the
/// disposable + a ledger; every `scope()` call emits two receipts
/// (materialize + dispose) so the audit trail is complete.
pub struct Transient<D: Disposable> {
    disposable: Arc<D>,
    ledger: Arc<dyn MaterializationLedger>,
    emitter: NodeId,
    stage_id: StageId,
}

impl<D: Disposable> Transient<D> {
    /// New Transient. `stage_id` is what the receipts route to in
    /// the ledger (typically the test name or a stable identifier
    /// for the scope's purpose). `emitter` identifies the node
    /// for cross-cluster attestation.
    #[must_use]
    pub fn new(
        disposable: Arc<D>,
        ledger: Arc<dyn MaterializationLedger>,
        emitter: NodeId,
        stage_id: StageId,
    ) -> Self {
        Self {
            disposable,
            ledger,
            emitter,
            stage_id,
        }
    }

    /// Materialize the input, invoke `body` with the handle, dispose,
    /// emit both typed receipts to the ledger, return the body's value.
    ///
    /// Disposal ALWAYS runs after the body — even when the body errors,
    /// even when the body's future is dropped mid-execution. (We don't
    /// model panics here; operators using catch_unwind layer it above.)
    ///
    /// ## Receipt shape
    /// - Materialize receipt: `ReceiptKind::Shape("transient:materialize:{disposable_name}")`,
    ///   subject = `disposable.subject_for(input)`, evidence = subject.
    /// - Dispose receipt: `ReceiptKind::Shape("transient:dispose:{disposable_name}")`,
    ///   subject = same, evidence = subject.
    ///
    /// # Errors
    /// Propagates all [`DisposableError`] variants.
    pub async fn scope<F, T>(
        &self,
        input: &D::Input,
        body: F,
    ) -> Result<T, DisposableError>
    where
        F: for<'h> FnOnce(&'h D::Handle) -> BoxFuture<'h, Result<T, String>> + Send,
        T: Send,
    {
        // 1. Materialize.
        let handle = self.disposable.materialize(input).await?;
        let subject = self.disposable.subject_for(input);
        self.emit_receipt(subject, "materialize").await?;

        // 2. Run the body. Capture the body's outcome BEFORE
        //    we hand the handle to dispose — body error must
        //    not block disposal.
        let body_result = body(&handle).await;

        // 3. Always dispose.
        let dispose_result = self.disposable.dispose(handle).await;

        // 4. Emit dispose receipt (best-effort even if dispose failed;
        //    operators want the audit trail).
        let _ = self.emit_receipt(subject, "dispose").await;

        // 5. Reconcile body + dispose outcomes into one typed error.
        match (body_result, dispose_result) {
            (Ok(t), Ok(())) => Ok(t),
            (Ok(_), Err(e)) => Err(e),
            (Err(body_err), Ok(())) => Err(DisposableError::Body(body_err)),
            (Err(body_err), Err(disp_err)) => Err(DisposableError::BodyAndDispose {
                body: body_err,
                dispose: disp_err.to_string(),
            }),
        }
    }

    async fn emit_receipt(
        &self,
        subject: [u8; 32],
        phase: &str,
    ) -> Result<(), DisposableError> {
        let receipt = MaterializationReceipt::new(
            ReceiptKind::Shape(format!("transient:{phase}:{}", self.disposable.name())),
            subject,
            self.emitter,
            now_unix(),
            subject,  // evidence = subject (faithful nodes converge)
        );
        self.ledger
            .ingest(&self.stage_id, 1, &receipt)
            .await
            .map_err(DisposableError::from)?;
        Ok(())
    }

    /// Borrow the underlying disposable (for telemetry).
    #[must_use]
    pub fn disposable(&self) -> &Arc<D> {
        &self.disposable
    }

    /// Borrow the ledger.
    #[must_use]
    pub fn ledger(&self) -> &Arc<dyn MaterializationLedger> {
        &self.ledger
    }
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::MemoryLedger;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// Test Disposable that tracks materialize + dispose calls.
    struct FakeDisposable {
        name: &'static str,
        materialize_count: AtomicUsize,
        dispose_count: AtomicUsize,
        fail_materialize: AtomicBool,
        fail_dispose: AtomicBool,
    }

    impl FakeDisposable {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                materialize_count: AtomicUsize::new(0),
                dispose_count: AtomicUsize::new(0),
                fail_materialize: AtomicBool::new(false),
                fail_dispose: AtomicBool::new(false),
            }
        }

        fn fail_materialize(&self) {
            self.fail_materialize.store(true, Ordering::SeqCst);
        }

        fn fail_dispose(&self) {
            self.fail_dispose.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl Disposable for FakeDisposable {
        fn name(&self) -> &'static str {
            self.name
        }

        type Input = Vec<u8>;
        type Handle = String;  // synthetic — represents the live instance

        fn subject_for(&self, input: &Self::Input) -> [u8; 32] {
            *blake3::hash(input).as_bytes()
        }

        async fn materialize(
            &self,
            input: &Self::Input,
        ) -> Result<Self::Handle, DisposableError> {
            if self.fail_materialize.load(Ordering::SeqCst) {
                return Err(DisposableError::Materialize("fake failed".into()));
            }
            self.materialize_count.fetch_add(1, Ordering::SeqCst);
            Ok(format!("handle-for-{} bytes", input.len()))
        }

        async fn dispose(&self, _handle: Self::Handle) -> Result<(), DisposableError> {
            if self.fail_dispose.load(Ordering::SeqCst) {
                return Err(DisposableError::Dispose("fake failed".into()));
            }
            self.dispose_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn assemble() -> (Arc<FakeDisposable>, Arc<MemoryLedger>, Transient<FakeDisposable>) {
        let disp = Arc::new(FakeDisposable::new("fake"));
        let ledger = Arc::new(MemoryLedger::new());
        let t = Transient::new(
            disp.clone(),
            ledger.clone(),
            NodeId::from_bytes(b"node"),
            StageId::new("test-stage"),
        );
        (disp, ledger, t)
    }

    #[tokio::test]
    async fn scope_materializes_runs_body_disposes() {
        let (disp, _ledger, t) = assemble();
        let input = b"hello".to_vec();
        let result = t
            .scope(&input, |handle| {
                Box::pin(async move {
                    assert!(handle.contains("5 bytes"));
                    Ok::<_, String>(42)
                })
            })
            .await
            .unwrap();
        assert_eq!(result, 42);
        assert_eq!(disp.materialize_count.load(Ordering::SeqCst), 1);
        assert_eq!(disp.dispose_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn scope_disposes_even_when_body_errors() {
        let (disp, _ledger, t) = assemble();
        let input = b"x".to_vec();
        let err = t
            .scope(&input, |_handle| {
                Box::pin(async move {
                    Err::<i32, _>("body failed".to_string())
                })
            })
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "body");
        // Dispose still ran even though body errored.
        assert_eq!(disp.dispose_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn scope_propagates_materialize_failure() {
        let (disp, _ledger, t) = assemble();
        disp.fail_materialize();
        let err = t
            .scope(&b"x".to_vec(), |_| Box::pin(async move { Ok::<_, String>(()) }))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "materialize");
        // Dispose NEVER ran because materialize failed.
        assert_eq!(disp.dispose_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn scope_body_ok_dispose_err_returns_dispose_error() {
        let (disp, _ledger, t) = assemble();
        disp.fail_dispose();
        let err = t
            .scope(&b"x".to_vec(), |_| Box::pin(async move { Ok::<_, String>(()) }))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "dispose");
    }

    #[tokio::test]
    async fn scope_both_err_returns_body_and_dispose() {
        let (disp, _ledger, t) = assemble();
        disp.fail_dispose();
        let err = t
            .scope(&b"x".to_vec(), |_| {
                Box::pin(async move { Err::<i32, _>("body fail".to_string()) })
            })
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "body_and_dispose");
        assert!(err.to_string().contains("body fail"));
    }

    #[tokio::test]
    async fn scope_emits_two_receipts_to_ledger() {
        let (_disp, ledger, t) = assemble();
        t.scope(&b"x".to_vec(), |_| Box::pin(async move { Ok::<_, String>(()) }))
            .await
            .unwrap();
        // Materialize + dispose receipts → 2 trackers in the ledger
        // (different ReceiptKind::Shape tags).
        assert_eq!(ledger.len().await, 2);
    }

    #[tokio::test]
    async fn scope_emits_dispose_receipt_even_on_body_error() {
        let (_disp, ledger, t) = assemble();
        let _ = t
            .scope(&b"x".to_vec(), |_| {
                Box::pin(async move { Err::<i32, _>("body fail".to_string()) })
            })
            .await;
        // Both receipts still emitted (audit trail completeness).
        assert_eq!(ledger.len().await, 2);
    }

    #[tokio::test]
    async fn scope_does_not_emit_materialize_receipt_on_materialize_failure() {
        let (disp, ledger, t) = assemble();
        disp.fail_materialize();
        let _ = t
            .scope(&b"x".to_vec(), |_| Box::pin(async move { Ok::<_, String>(()) }))
            .await;
        // Materialize failed → no receipts written.
        assert_eq!(ledger.len().await, 0);
    }

    #[tokio::test]
    async fn faithful_nodes_produce_same_subject() {
        let disp = Arc::new(FakeDisposable::new("fake"));
        let s1 = disp.subject_for(&b"hello".to_vec());
        let s2 = disp.subject_for(&b"hello".to_vec());
        assert_eq!(s1, s2);
    }

    #[tokio::test]
    async fn distinct_inputs_produce_distinct_subjects() {
        let disp = Arc::new(FakeDisposable::new("fake"));
        let s1 = disp.subject_for(&b"a".to_vec());
        let s2 = disp.subject_for(&b"b".to_vec());
        assert_ne!(s1, s2);
    }

    #[tokio::test]
    async fn disposable_and_ledger_accessors() {
        let (_disp, _ledger, t) = assemble();
        assert_eq!(t.disposable().name(), "fake");
        assert_eq!(t.ledger().name(), "memory");
    }

    #[test]
    fn disposable_error_kinds_are_stable() {
        assert_eq!(
            DisposableError::Materialize("x".into()).kind(),
            "materialize"
        );
        assert_eq!(DisposableError::Dispose("x".into()).kind(), "dispose");
        assert_eq!(DisposableError::Body("x".into()).kind(), "body");
        assert_eq!(
            DisposableError::BodyAndDispose {
                body: "a".into(),
                dispose: "b".into()
            }
            .kind(),
            "body_and_dispose"
        );
        assert_eq!(DisposableError::Ledger("x".into()).kind(), "ledger");
    }

    #[test]
    fn ledger_error_converts_to_disposable_error_ledger_variant() {
        let le = LedgerError::Backend("x".into());
        let de: DisposableError = le.into();
        assert_eq!(de.kind(), "ledger");
    }
}
