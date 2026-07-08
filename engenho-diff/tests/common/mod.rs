//! Shared differential-run scaffold for the per-operation-class parity tests
//! (`patch_parity`, `status_subresource_parity`, `selector_parity`, …).
//!
//! The m0 test predates this module and inlines the same shape; the new
//! per-class files route through here so the boot → preflight → run → report →
//! RATCHET pipeline lives in ONE place (★ ruthless standardization). The
//! ratchet has TWO guards:
//!
//!   * **no-new** — an observed hard divergence NOT in the file's baseline
//!     fails (a regression, or a real gap to record).
//!   * **no-stale** — a baselined signature that NO LONGER appears fails,
//!     forcing the ledger to SHRINK as fixes land (the ratchet only tightens).

#![allow(dead_code)] // each test file uses a subset of these helpers.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use engenho_diff::{
    DiffTarget, Divergence, EngenhoTarget, HttpMethod, K3sTarget, Normalizer, Operation, Severity,
    Verdict, cotejo,
};

/// The live k3s oracle kubeconfig (`~/.kube/engenho-local-tunnel.yaml`).
#[must_use]
pub fn oracle_kubeconfig() -> PathBuf {
    let home = std::env::var("HOME").expect("HOME set");
    PathBuf::from(home).join(".kube/engenho-local-tunnel.yaml")
}

/// A per-run unique suffix (namespace / object names never collide across runs
/// against the shared oracle).
#[must_use]
pub fn unique_suffix() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .to_string()
}

/// The paired findings for one operation.
pub struct OpFindings {
    pub op_id: String,
    pub hard: Vec<Divergence>,
    pub cosmetic: Vec<Divergence>,
}

/// Boot engenho in-process with store-backed handlers for `kinds`.
pub async fn boot_engenho(kinds: &[&str]) -> EngenhoTarget {
    EngenhoTarget::boot_in_process(kinds)
        .await
        .expect("engenho boots in-process")
}

/// Load the oracle + FAIL LOUD if it is unreachable (never a silent skip).
pub async fn load_oracle() -> K3sTarget {
    let kubeconfig = oracle_kubeconfig();
    let k3s = K3sTarget::from_kubeconfig(&kubeconfig)
        .unwrap_or_else(|e| panic!("cannot load oracle kubeconfig {kubeconfig:?}: {e}"));
    if let Some(Verdict::ReferenceUnreachable) = k3s.preflight().await {
        panic!(
            "Verdict::ReferenceUnreachable — the k3s oracle at {kubeconfig:?} did not answer \
             GET /api/v1. A live differential run cannot proceed. (Is the tunnel up? \
             ssh -f -N -L 16443:127.0.0.1:6443 root@192.168.64.10)"
        );
    }
    k3s
}

/// Execute `op` on both targets + fold into paired findings.
pub async fn run_one(
    op: &Operation,
    engenho: &EngenhoTarget,
    k3s: &K3sTarget,
    norm: &Normalizer,
) -> OpFindings {
    let (hard, cosmetic) = cotejo::run_strict(op, engenho, k3s, norm)
        .await
        .unwrap_or_else(|e| panic!("op {} failed: {e}", op.id));
    OpFindings {
        op_id: op.id.clone(),
        hard,
        cosmetic,
    }
}

/// Fire `op` at BOTH targets WITHOUT diffing — for setup/mutation steps whose
/// full-object response would otherwise flood the ratchet with unrelated
/// defaulting divergence (e.g. a Pod create's PodSpec defaulting). The object
/// must exist on both sides for the subsequent focused probes; its shape is
/// diffed elsewhere (m0 covers create parity for the parity kinds).
pub async fn exec_only(op: &Operation, engenho: &EngenhoTarget, k3s: &K3sTarget) {
    let body = op.body_bytes();
    for t in [engenho as &dyn DiffTarget, k3s as &dyn DiffTarget] {
        let _ = t
            .raw(
                op.method,
                &op.path,
                body.as_deref(),
                op.content_type.as_deref(),
            )
            .await;
    }
}

/// Best-effort DELETE of a path on the oracle (shared-cluster cleanup; not
/// diffed).
pub async fn cleanup(k3s: &K3sTarget, path: &str) {
    let _ = k3s.raw(HttpMethod::Delete, path, None, None).await;
}

/// Print the per-op report (PARITY / DIVERGENT + owner attribution).
pub fn print_report(title: &str, findings: &[OpFindings]) {
    eprintln!("\n════════════ engenho-diff {title}: engenho vs k3s v1.34 ════════════");
    let mut total_hard = 0usize;
    let mut total_cosmetic = 0usize;
    for f in findings {
        let verdict = if f.hard.is_empty() {
            "PARITY"
        } else {
            "DIVERGENT"
        };
        eprintln!("\n▶ {} — {verdict}", f.op_id);
        for d in &f.hard {
            eprintln!("    {d}   [owner: {}]", d.owning_crate());
            total_hard += 1;
        }
        for d in &f.cosmetic {
            debug_assert_eq!(d.severity(), Severity::Cosmetic);
            eprintln!("    (masked) {d}");
            total_cosmetic += 1;
        }
    }
    eprintln!(
        "\n──────────── {total_hard} hard divergence(s), {total_cosmetic} cosmetic (masked) ────────────\n"
    );
}

/// The RATCHET: observed hard divergences must EQUAL the baseline (no new, no
/// stale). Returns the observed hard signature set for callers that also want
/// to assert count.
pub fn assert_ratchet(findings: &[OpFindings], known: &[&str]) -> BTreeSet<String> {
    let observed: BTreeSet<String> = findings
        .iter()
        .flat_map(|f| f.hard.iter())
        .map(Divergence::signature)
        .collect();
    let known_set: BTreeSet<String> = known.iter().map(|s| (*s).to_string()).collect();

    let new: Vec<&String> = observed.difference(&known_set).collect();
    let stale: Vec<&String> = known_set.difference(&observed).collect();

    assert!(
        new.is_empty(),
        "NEW hard divergences not in the baseline (a regression, or a gap to \
         record):\n  {}\n\nFull observed hard baseline (copy in if intended):\n{}",
        join_lines(&new, "\n  "),
        observed
            .iter()
            .map(|s| {
                let mut line = String::from("    \"");
                line.push_str(s);
                line.push_str("\",");
                line
            })
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert!(
        stale.is_empty(),
        "STALE baseline entries that NO LONGER diverge (the ratchet must SHRINK \
         — remove these from the baseline):\n  {}",
        join_lines(&stale, "\n  ")
    );
    observed
}

fn join_lines(items: &[&String], sep: &str) -> String {
    items
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<_>>()
        .join(sep)
}
