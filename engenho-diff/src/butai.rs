//! The OCI-runtime differential — `butai` (or any runtime) against a
//! reference binary, over the same bundle.
//!
//! ── ★ WHY THIS LIVES IN `engenho-diff` AND IS NOT ITS OWN HARNESS ─────────
//! The butai plan called for a fresh `butai/tests/differential.rs`. It should
//! not be fresh: this crate already IS the differential harness, generalized —
//! typed operations fired at two targets, each observation folded through a
//! [`Normalizer`] into a recursive JSON differ that yields a typed
//! [`Verdict`]. Writing a second one would give the fleet two differs free to
//! disagree about what counts as a difference, which is the failure this crate
//! exists to prevent.
//!
//! What genuinely does NOT fit is [`crate::target::DiffTarget`]: it is
//! HTTP-shaped (`method` + `path` → `{status, content_type, json}`) and an OCI
//! runtime is a CLI (`create` / `start` / `state` / `kill` / `delete` →
//! exit code + stdout + stderr). Same GOAL, different SHAPE — so this module
//! adds its own observation and reuses the differ, the normalizer, the
//! [`Divergence`] vocabulary and the [`Verdict`], rather than forcing an exit
//! code into a `u16` HTTP status where it would read as one.
//!
//! ── ★ THE NEGATIVE CONTROL IS THE GATE ────────────────────────────────────
//! A differential that has never failed proves nothing. The M0 gate is
//! deliberately two runs: the same binary against itself must be
//! [`Verdict::Parity`], and a binary against `/bin/false` must be
//! [`Verdict::Divergent`] NAMING THE FIRST DIFFERING FIELD. Only the second
//! shows the harness can see anything at all.

use std::collections::BTreeSet;

use serde_json::Value;

use crate::cotejo::diff_object;
use crate::normalize::Normalizer;
use crate::verdict::{Divergence, Severity, Side};

/// One subcommand of the OCI runtime lifecycle.
///
/// The set the runtime spec defines and containerd's shim actually drives.
/// `state` is the one that carries a comparable document; the others are
/// compared on their exit code and output.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RuntimeOp {
    /// Create the container from the bundle; blocks until `start`.
    Create,
    /// Release the create/start rendezvous.
    Start,
    /// Report the container's OCI state document.
    State,
    /// Signal it.
    Kill,
    /// Remove it.
    Delete,
}

impl RuntimeOp {
    /// The subcommand as the binary spells it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Start => "start",
            Self::State => "state",
            Self::Kill => "kill",
            Self::Delete => "delete",
        }
    }
}

/// What one runtime did for one subcommand.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeObservation {
    /// Which subcommand.
    pub op: RuntimeOp,
    /// Process exit code. `None` when the process was signalled (no code).
    pub exit_code: Option<i32>,
    /// Raw stdout bytes.
    pub stdout: Vec<u8>,
    /// Raw stderr bytes.
    ///
    /// ★ COMPARED ONLY FOR EMPTINESS, never byte-for-byte. Two conformant
    /// runtimes legitimately word their diagnostics differently, and a
    /// differential that fails on prose is a differential nobody runs.
    pub stderr: Vec<u8>,
}

impl RuntimeObservation {
    /// The `state` document, when this observation carries one.
    ///
    /// A non-JSON stdout is `None` rather than an error: only `state` is
    /// expected to emit a document, and the differ compares presence too.
    #[must_use]
    pub fn state_json(&self) -> Option<Value> {
        if self.op != RuntimeOp::State {
            return None;
        }
        serde_json::from_slice(&self.stdout).ok()
    }

    /// Whether the subcommand succeeded.
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.exit_code == Some(0)
    }
}

/// Keys of the OCI `state` document that are legitimately per-run or
/// per-runtime and must be masked before comparison.
///
/// ── ★ AN EXPLICIT KEY LIST, NEVER A REGEX ────────────────────────────────
/// `pid` differs every run; `bundle` is an absolute path the harness chose.
/// The runtime-private keys are the interesting ones: crun emits several
/// non-spec keys that runc does not, and runc emits `annotations` that crun
/// omits. Masking those by PATTERN would quietly swallow a real divergence
/// that happened to share a prefix; naming them means adding a runtime costs
/// an explicit line, which is the point.
#[must_use]
pub fn state_normalizer() -> Normalizer {
    use crate::normalize::Mask;
    use crate::path::JsonPath;
    let mut n = Normalizer::default();
    for key in [
        // Per-run, on every runtime.
        "pid",
        "bundle",
        // crun-private, absent from runc.
        "systemd-scope",
        "owner",
        "created",
        // runc-private, absent from crun.
        "annotations",
    ] {
        n = n.with(Mask::Drop(JsonPath::parse(key)));
    }
    n
}

/// Compare one subcommand across two runtimes.
///
/// `reference` is the known-good binary (crun today), `under_test` the one
/// being judged. Returns every hard divergence, first-listed first.
#[must_use]
pub fn diff_runtime_op(
    reference: &RuntimeObservation,
    under_test: &RuntimeObservation,
    norm: &Normalizer,
) -> Vec<Divergence> {
    use crate::path::JsonPath;

    assert_eq!(reference.op, under_test.op, "compare like with like");
    let op = reference.op;

    // 1. Exit code. A mismatch short-circuits — comparing the output of a
    //    subcommand that failed on one side is comparing an error message to
    //    a result.
    if reference.exit_code != under_test.exit_code {
        return vec![Divergence::FieldDiff {
            path: JsonPath::parse(&format!("{}.exitCode", op.as_str())),
            engenho: serde_json::json!(under_test.exit_code),
            k3s: serde_json::json!(reference.exit_code),
            severity: Severity::Hard,
        }];
    }
    // Both failed identically ⇒ they agree.
    if !reference.is_success() {
        return Vec::new();
    }

    // 2. `state` carries a document; compare it under the mask.
    if op == RuntimeOp::State {
        return match (reference.state_json(), under_test.state_json()) {
            (Some(r), Some(u)) => diff_object(&norm.apply(&u), &norm.apply(&r), Severity::Hard),
            (r, u) => vec![Divergence::FieldDiff {
                path: JsonPath::parse("state.document"),
                engenho: serde_json::json!(if u.is_some() { "json" } else { "not json" }),
                k3s: serde_json::json!(if r.is_some() { "json" } else { "not json" }),
                severity: Severity::Hard,
            }],
        };
    }

    // 3. Every other subcommand: stdout must match byte-for-byte (these
    //    produce no document, so any output at all is a difference), and
    //    stderr is compared only for emptiness.
    let mut out = Vec::new();
    if reference.stdout != under_test.stdout {
        out.push(Divergence::FieldDiff {
            path: JsonPath::parse(&format!("{}.stdout", op.as_str())),
            engenho: serde_json::json!(String::from_utf8_lossy(&under_test.stdout)),
            k3s: serde_json::json!(String::from_utf8_lossy(&reference.stdout)),
            severity: Severity::Hard,
        });
    }
    if reference.stderr.is_empty() != under_test.stderr.is_empty() {
        out.push(Divergence::FieldDiff {
            path: JsonPath::parse(&format!("{}.stderr", op.as_str())),
            engenho: serde_json::json!(if under_test.stderr.is_empty() { "empty" } else { "non-empty" }),
            k3s: serde_json::json!(if reference.stderr.is_empty() { "empty" } else { "non-empty" }),
            severity: Severity::Hard,
        });
    }
    out
}

/// Compare a whole lifecycle run.
///
/// Both sides must have observed the SAME set of subcommands; a set mismatch
/// is itself a divergence rather than a silent short run.
#[must_use]
pub fn diff_runtime_run(
    reference: &[RuntimeObservation],
    under_test: &[RuntimeObservation],
    norm: &Normalizer,
) -> Vec<Divergence> {
    use crate::path::JsonPath;

    let r_ops: BTreeSet<RuntimeOp> = reference.iter().map(|o| o.op).collect();
    let u_ops: BTreeSet<RuntimeOp> = under_test.iter().map(|o| o.op).collect();
    if r_ops != u_ops {
        return vec![Divergence::FieldDiff {
            path: JsonPath::parse("lifecycle.subcommands"),
            engenho: serde_json::json!(format!("{u_ops:?}")),
            k3s: serde_json::json!(format!("{r_ops:?}")),
            severity: Severity::Hard,
        }];
    }
    // In lifecycle order, so the FIRST divergence reported is the earliest
    // point the two runtimes parted company — which is the one worth reading.
    let mut out = Vec::new();
    for op in r_ops {
        let r = reference.iter().find(|o| o.op == op).expect("present");
        let u = under_test.iter().find(|o| o.op == op).expect("present");
        out.extend(diff_runtime_op(r, u, norm));
    }
    out
}

/// Which side a runtime differential calls which.
///
/// Re-exported for callers that report a divergence; the reference binary is
/// [`Side::K3s`] because that is the vocabulary [`Divergence`] already speaks.
#[must_use]
pub fn reference_side() -> Side {
    Side::K3s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(op: RuntimeOp, code: i32, stdout: &str) -> RuntimeObservation {
        RuntimeObservation {
            op,
            exit_code: Some(code),
            stdout: stdout.as_bytes().to_vec(),
            stderr: Vec::new(),
        }
    }

    fn state(json: &str) -> RuntimeObservation {
        obs(RuntimeOp::State, 0, json)
    }

    #[test]
    fn the_same_run_against_itself_is_parity() {
        // The green half of the gate.
        let run = vec![
            obs(RuntimeOp::Create, 0, ""),
            obs(RuntimeOp::Start, 0, ""),
            state(r#"{"ociVersion":"1.0.2","id":"c","status":"running","pid":1,"bundle":"/a"}"#),
            obs(RuntimeOp::Delete, 0, ""),
        ];
        assert!(diff_runtime_run(&run, &run, &state_normalizer()).is_empty());
    }

    #[test]
    fn a_runtime_that_does_nothing_diverges_naming_the_field() {
        // The RED half, and it is the gate — a differential that has never
        // failed proves nothing. `/bin/false` exits 1 and prints nothing.
        let good = vec![obs(RuntimeOp::Create, 0, "")];
        let dead = vec![obs(RuntimeOp::Create, 1, "")];
        let d = diff_runtime_run(&good, &dead, &state_normalizer());
        assert_eq!(d.len(), 1, "{d:?}");
        assert!(
            format!("{:?}", d[0]).contains("exitCode"),
            "the divergence must NAME the field: {:?}",
            d[0]
        );
    }

    #[test]
    fn per_run_and_runtime_private_state_keys_are_masked() {
        // pid differs every run; crun emits systemd-scope/owner/created that
        // runc does not; runc emits annotations that crun does not. None of
        // those is a divergence.
        let crun = state(
            r#"{"ociVersion":"1.0.2","id":"c","status":"running","pid":11,
                "bundle":"/x","systemd-scope":"s","owner":"root","created":"t1"}"#,
        );
        let runc = state(
            r#"{"ociVersion":"1.0.2","id":"c","status":"running","pid":22,
                "bundle":"/y","annotations":{"a":"b"}}"#,
        );
        assert!(
            diff_runtime_run(&[crun], &[runc], &state_normalizer()).is_empty(),
            "only the spec-defined fields may be compared"
        );
    }

    #[test]
    fn a_real_state_difference_is_still_caught_through_the_mask() {
        // NEGATIVE CONTROL for the mask: masking must not be so broad that it
        // swallows the fields the differential exists to compare.
        let a = state(r#"{"ociVersion":"1.0.2","id":"c","status":"running","pid":1}"#);
        let b = state(r#"{"ociVersion":"1.0.2","id":"c","status":"stopped","pid":2}"#);
        let d = diff_runtime_run(&[a], &[b], &state_normalizer());
        assert!(!d.is_empty(), "a status difference must survive the mask");
        assert!(format!("{d:?}").contains("status"), "{d:?}");
    }

    #[test]
    fn a_short_run_is_a_divergence_not_a_silent_pass() {
        // A runtime that never got to `state` must not compare equal to one
        // that did just because the ops they share happen to agree.
        let full = vec![obs(RuntimeOp::Create, 0, ""), state(r#"{"id":"c"}"#)];
        let short = vec![obs(RuntimeOp::Create, 0, "")];
        let d = diff_runtime_run(&full, &short, &state_normalizer());
        assert_eq!(d.len(), 1);
        assert!(format!("{d:?}").contains("subcommands"), "{d:?}");
    }

    #[test]
    fn stderr_is_compared_for_emptiness_not_for_prose() {
        // Two conformant runtimes word diagnostics differently; a
        // differential that fails on prose is one nobody runs.
        let mut a = obs(RuntimeOp::Kill, 0, "");
        let mut b = obs(RuntimeOp::Kill, 0, "");
        a.stderr = b"container not running\n".to_vec();
        b.stderr = b"the container is not running\n".to_vec();
        assert!(diff_runtime_run(&[a.clone()], &[b], &state_normalizer()).is_empty());
        // But present-vs-absent IS a difference.
        let quiet = obs(RuntimeOp::Kill, 0, "");
        assert!(!diff_runtime_run(&[a], &[quiet], &state_normalizer()).is_empty());
    }
}
