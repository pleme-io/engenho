//! Tests for TameshiOutcomeChain — outcome-level cryptographic
//! attestation chain.

#![cfg(feature = "with-tameshi")]

use engenho_fonte::{Outcome, OutcomeChainRecorder, TameshiOutcomeChain};
use std::sync::Arc;

fn sample_outcome(rev: u64, proposal: u64) -> Outcome {
    Outcome {
        revision: rev,
        proposal_id: proposal,
        receipt_id: Arc::from("test"),
        finalized_at_ms: rev * 1000,
    }
}

#[tokio::test]
async fn record_appends_one_chain_entry() {
    let chain = TameshiOutcomeChain::new("inst", "0.1.0");
    let id = chain.record(&sample_outcome(1, 0)).await.unwrap();
    assert!(!id.is_empty());
    let entries = chain.chain().entries();
    assert_eq!(entries.len(), 1);
}

#[tokio::test]
async fn n_outcomes_chain_with_blake3_linkage() {
    let chain = TameshiOutcomeChain::new("inst", "0.1.0");
    for i in 1..=5u64 {
        chain.record(&sample_outcome(i, i - 1)).await.unwrap();
    }
    let entries = chain.chain().entries();
    assert_eq!(entries.len(), 5);
    for w in entries.windows(2) {
        assert_eq!(w[1].previous_hash, w[0].entry_hash);
    }
}

#[tokio::test]
async fn outcome_chain_separate_from_attest_chain() {
    use engenho_fonte::{Attester, Change, ChangeKind, Decision, TameshiAttester};
    use engenho_sui_typescape::TypescapeValue;

    let attest_chain = TameshiAttester::new("inst", "0.1.0");
    let outcome_chain = TameshiOutcomeChain::new("inst", "0.1.0");

    let decision = Decision {
        change: Change {
            source: "rio".into(),
            kind: ChangeKind::Initial,
            source_text: "{}".into(),
            revision: 1,
        },
        typed: TypescapeValue::null(),
    };
    let _receipt = attest_chain.attest(&decision, 0).await.unwrap();
    let _outcome_id = outcome_chain.record(&sample_outcome(1, 0)).await.unwrap();

    assert_eq!(attest_chain.chain().entries().len(), 1);
    assert_eq!(outcome_chain.chain().entries().len(), 1);
    // The two chains are independent — proving the SAME (rev,
    // proposal) tuple has two distinct cryptographic receipts
    // (attestation + outcome).
}
