//! StoreBackedLedger — `MaterializationLedger` impl backed by
//! `engenho-store`. Receipts commit through the store's existing
//! Raft path; reads aggregate the committed receipts back into a
//! `QuorumTracker`.
//!
//! This is the load-bearing impl that makes
//! `ConfirmacaoPolicy::RaftCommitted` meaningful: receipts are
//! durable, replicated, byzantine-fault-tolerant within the
//! cluster's existing consensus group.
//!
//! ## Wire shape
//!
//! Each receipt commits as a typed resource:
//!
//!   group: engenho.io
//!   version: v1
//!   kind: MaterializationReceipt
//!   namespace: configurable (default: engenho-system)
//!   name: {stage_id}-{kind}-{subject_hex_short}-{emitter_hex_short}
//!   spec.receipt: <serialized MaterializationReceipt>
//!   spec.stage_id: {stage_id}
//!   spec.threshold: {usize}
//!
//! ## Aggregation
//!
//! `outcome()` walks all receipts matching a `LedgerKey`, folds them
//! into a `QuorumTracker` built with the stored threshold, and returns
//! the tracker's own verdict. It never computes a verdict itself: the
//! verdict is sealed, so there is no second copy of the fold here.
//! A receipt whose stored threshold is missing or zero is skipped like
//! an undecodable one.
//! `forget_stage` deletes every receipt resource for the stage.

use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand},
    resource::ResourceKey,
};
use engenho_substrate::{
    LedgerError, LedgerKey, MaterializationLedger, MaterializationReceipt, QuorumTracker,
    QuorumVerdict, ReceiptKind, StageId,
};
use serde_json::json;

/// Default namespace for receipt resources.
pub const DEFAULT_RECEIPT_NAMESPACE: &str = "engenho-system";

/// MaterializationLedger backed by engenho-store.
pub struct StoreBackedLedger {
    store: Arc<StoreMesh>,
    namespace: String,
}

impl StoreBackedLedger {
    /// New ledger writing receipts to the default namespace
    /// (`engenho-system`).
    #[must_use]
    pub fn new(store: Arc<StoreMesh>) -> Self {
        Self::with_namespace(store, DEFAULT_RECEIPT_NAMESPACE.into())
    }

    /// New ledger with explicit namespace.
    #[must_use]
    pub fn with_namespace(store: Arc<StoreMesh>, namespace: String) -> Self {
        Self { store, namespace }
    }

    /// Compose the resource name for a receipt. Uses short hex
    /// prefixes to keep names readable while ensuring uniqueness
    /// per (stage, kind, subject, emitter) tuple.
    #[must_use]
    pub fn receipt_name(stage_id: &StageId, receipt: &MaterializationReceipt) -> String {
        let kind_tag = match &receipt.kind {
            ReceiptKind::Drv => "drv".to_string(),
            ReceiptKind::Nar => "nar".to_string(),
            ReceiptKind::Realisation => "realisation".to_string(),
            ReceiptKind::BuildResult => "build-result".to_string(),
            ReceiptKind::Shape(t) => format!("shape-{}", sanitize(t)),
        };
        let subj_short = hex_short(&receipt.subject);
        let emit_short = hex_short(&receipt.emitter.0);
        format!(
            "{}-{}-{}-{}",
            sanitize(stage_id.as_str()),
            kind_tag,
            subj_short,
            emit_short,
        )
    }

    fn resource_key_for(
        &self,
        stage_id: &StageId,
        receipt: &MaterializationReceipt,
    ) -> ResourceKey {
        ResourceKey::namespaced(
            "engenho.io",
            "v1",
            "MaterializationReceipt",
            &self.namespace,
            &Self::receipt_name(stage_id, receipt),
        )
    }
}

#[async_trait]
impl MaterializationLedger for StoreBackedLedger {
    fn name(&self) -> &'static str {
        "store-backed"
    }

    async fn ingest(
        &self,
        stage_id: &StageId,
        threshold: NonZeroUsize,
        receipt: &MaterializationReceipt,
    ) -> Result<QuorumVerdict, LedgerError> {
        // Commit the receipt as a typed resource.
        let receipt_json = serde_json::to_value(receipt)
            .map_err(|e| LedgerError::Backend(format!("encode receipt: {e}")))?;
        let key = self.resource_key_for(stage_id, receipt);
        let value = json!({
            "apiVersion": "engenho.io/v1",
            "kind": "MaterializationReceipt",
            "metadata": {
                "name": Self::receipt_name(stage_id, receipt),
                "namespace": self.namespace,
            },
            "spec": {
                "stage_id": stage_id.as_str(),
                "threshold": threshold.get(),
                "receipt": receipt_json,
            },
        });
        self.store
            .propose(ResourceCommand::Put {
                key,
                value,
                expected: None,
                reason: Reason::Controller,
            })
            .await
            .map_err(|e| LedgerError::Backend(format!("store propose: {e}")))?;

        // Re-aggregate to produce the outcome.
        let ledger_key = LedgerKey {
            stage_id: stage_id.clone(),
            kind: receipt.kind.clone(),
            subject: receipt.subject,
        };
        match self.outcome(&ledger_key).await? {
            Some(verdict) => Ok(verdict),
            // The list did not show the receipt just committed: report
            // the empty fold (pending, nothing counted) rather than guess.
            None => {
                Ok(QuorumTracker::new(receipt.kind.clone(), receipt.subject, threshold).verdict())
            }
        }
    }

    async fn outcome(&self, key: &LedgerKey) -> Result<Option<QuorumVerdict>, LedgerError> {
        let all = self
            .store
            .list(
                "engenho.io",
                "v1",
                "MaterializationReceipt",
                Some(&self.namespace),
            )
            .await;
        let mut tracker: Option<QuorumTracker> = None;
        for (_, v) in all {
            let spec = match v.get("spec") {
                Some(s) => s,
                None => continue,
            };
            let stage_id = spec
                .get("stage_id")
                .and_then(|s| s.as_str())
                .map(StageId::new);
            if stage_id.as_ref() != Some(&key.stage_id) {
                continue;
            }
            let receipt: MaterializationReceipt = match spec
                .get("receipt")
                .cloned()
                .and_then(|r| serde_json::from_value(r).ok())
            {
                Some(r) => r,
                None => continue,
            };
            if receipt.kind != key.kind || receipt.subject != key.subject {
                continue;
            }
            // The first committed receipt fixes the threshold (operators
            // shouldn't disagree on threshold for the same key).
            let Some(threshold) = spec
                .get("threshold")
                .and_then(serde_json::Value::as_u64)
                .and_then(|n| usize::try_from(n).ok())
                .and_then(NonZeroUsize::new)
            else {
                continue;
            };
            tracker
                .get_or_insert_with(|| {
                    QuorumTracker::new(receipt.kind.clone(), receipt.subject, threshold)
                })
                .ingest(&receipt);
        }
        Ok(tracker.map(|t| t.verdict()))
    }

    async fn forget_stage(&self, stage_id: &StageId) -> Result<(), LedgerError> {
        let all = self
            .store
            .list(
                "engenho.io",
                "v1",
                "MaterializationReceipt",
                Some(&self.namespace),
            )
            .await;
        for (key, v) in all {
            let stage = v
                .get("spec")
                .and_then(|s| s.get("stage_id"))
                .and_then(|s| s.as_str())
                .map(StageId::new);
            if stage.as_ref() == Some(stage_id) {
                self.store
                    .propose(ResourceCommand::delete(key, Reason::Controller))
                    .await
                    .map_err(|e| LedgerError::Backend(format!("store delete: {e}")))?;
            }
        }
        Ok(())
    }
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' => c,
            _ => '-',
        })
        .collect::<String>()
        .trim_matches('-')
        .to_lowercase()
}

fn hex_short(bytes: &[u8]) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut s = String::with_capacity(8);
    for b in bytes.iter().take(4) {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_substrate::{NodeId, ReceiptKind};

    fn rcpt_drv(emitter: u8, evidence: u8) -> MaterializationReceipt {
        MaterializationReceipt::for_drv([7u8; 32], NodeId::new([emitter; 32]), 100, [evidence; 32])
    }

    fn rcpt_shape(emitter: u8, evidence: u8, tag: &str) -> MaterializationReceipt {
        MaterializationReceipt::new(
            ReceiptKind::Shape(tag.to_string()),
            [7u8; 32],
            NodeId::new([emitter; 32]),
            100,
            [evidence; 32],
        )
    }

    fn stage() -> StageId {
        StageId::new("build-image")
    }

    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).expect("test thresholds are non-zero")
    }

    async fn test_store(name: &str) -> Arc<StoreMesh> {
        use engenho_store::{InProcessRouter, default_config};
        let router = InProcessRouter::new();
        let cfg = default_config(name).unwrap();
        let store = Arc::new(
            StoreMesh::start(1, "in-process://1".into(), router, cfg)
                .await
                .unwrap(),
        );
        store.initialize_singleton().await.unwrap();
        assert!(
            store
                .wait_for_leadership(std::time::Duration::from_secs(3))
                .await
        );
        store
    }

    fn drv_key() -> LedgerKey {
        LedgerKey {
            stage_id: stage(),
            kind: ReceiptKind::Drv,
            subject: [7u8; 32],
        }
    }

    // ── outcome: the tracker's verdict, not a second fold ──────

    #[tokio::test]
    async fn outcome_is_the_trackers_verdict_against_the_stored_threshold() {
        let ledger = StoreBackedLedger::new(test_store("store-ledger-threshold").await);
        let first = ledger
            .ingest(&stage(), nz(3), &rcpt_drv(1, 5))
            .await
            .unwrap();
        let read = ledger
            .outcome(&drv_key())
            .await
            .unwrap()
            .expect("slot exists");
        assert_eq!(read.state(), engenho_substrate::QuorumState::Pending);
        assert_eq!((read.confirmed(), read.threshold()), (1, nz(3)));
        assert_eq!(read, first);
        ledger
            .ingest(&stage(), nz(3), &rcpt_drv(2, 5))
            .await
            .unwrap();
        let last = ledger
            .ingest(&stage(), nz(3), &rcpt_drv(3, 5))
            .await
            .unwrap();
        assert!(last.is_reached());
        assert_eq!(ledger.outcome(&drv_key()).await.unwrap(), Some(last));
    }

    #[tokio::test]
    async fn a_receipt_with_a_zero_threshold_is_not_counted() {
        let store = test_store("store-ledger-zero").await;
        let ledger = StoreBackedLedger::new(store.clone());
        let receipt = rcpt_drv(1, 5);
        let name = StoreBackedLedger::receipt_name(&stage(), &receipt);
        store
            .propose(ResourceCommand::Put {
                key: ResourceKey::namespaced(
                    "engenho.io",
                    "v1",
                    "MaterializationReceipt",
                    DEFAULT_RECEIPT_NAMESPACE,
                    &name,
                ),
                value: json!({
                    "apiVersion": "engenho.io/v1",
                    "kind": "MaterializationReceipt",
                    "metadata": {"name": name, "namespace": DEFAULT_RECEIPT_NAMESPACE},
                    "spec": {
                        "stage_id": stage().as_str(),
                        "threshold": 0,
                        "receipt": serde_json::to_value(&receipt).unwrap(),
                    },
                }),
                expected: None,
                reason: Reason::Controller,
            })
            .await
            .unwrap();
        assert_eq!(ledger.outcome(&drv_key()).await.unwrap(), None);
    }

    // ── receipt_name ──────────────────────────────────────────

    #[test]
    fn receipt_name_includes_stage_kind_subject_emitter() {
        let r = rcpt_drv(1, 5);
        let name = StoreBackedLedger::receipt_name(&stage(), &r);
        assert!(name.starts_with("build-image-drv-"));
        assert!(name.contains("0707")); // subject prefix (4 bytes hex)
        assert!(name.contains("0101")); // emitter prefix (4 bytes hex)
    }

    #[test]
    fn receipt_name_sanitizes_stage_id() {
        let r = rcpt_drv(1, 5);
        let name = StoreBackedLedger::receipt_name(&StageId::new("build/with.slash"), &r);
        assert!(!name.contains('/'));
        assert!(!name.contains('.'));
    }

    #[test]
    fn receipt_name_uses_kind_tag() {
        let r_drv = rcpt_drv(1, 5);
        let r_nar =
            MaterializationReceipt::for_nar([7u8; 32], NodeId::new([1u8; 32]), 100, [5u8; 32]);
        let r_shape = rcpt_shape(1, 5, "oci_image");
        assert!(StoreBackedLedger::receipt_name(&stage(), &r_drv).contains("-drv-"));
        assert!(StoreBackedLedger::receipt_name(&stage(), &r_nar).contains("-nar-"));
        let shape_name = StoreBackedLedger::receipt_name(&stage(), &r_shape);
        assert!(shape_name.contains("shape-oci-image"));
    }

    #[test]
    fn receipt_name_per_emitter_is_unique() {
        let r1 = rcpt_drv(1, 5);
        let r2 = rcpt_drv(2, 5);
        let n1 = StoreBackedLedger::receipt_name(&stage(), &r1);
        let n2 = StoreBackedLedger::receipt_name(&stage(), &r2);
        assert_ne!(n1, n2);
    }

    #[test]
    fn sanitize_drops_special_chars() {
        assert_eq!(sanitize("hello/world"), "hello-world");
        assert_eq!(sanitize("X.Y.Z"), "x-y-z");
        assert_eq!(sanitize("--leading-trailing--"), "leading-trailing");
    }

    #[test]
    fn hex_short_is_8_chars_lowercase_hex() {
        let h = hex_short(&[0xab, 0xcd, 0xef, 0x12, 0x34]);
        assert_eq!(h, "abcdef12");
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn default_namespace_constant() {
        assert_eq!(DEFAULT_RECEIPT_NAMESPACE, "engenho-system");
    }
}
