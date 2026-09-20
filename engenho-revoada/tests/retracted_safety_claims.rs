//! Claims the docs made about revoada, the store and the fabric, which
//! the code does not deliver (IMPROVEMENT-PLAN T5.4). Most are safety
//! claims; the FABRIC.md row is an architecture one — a document
//! describing a NATS fabric engenho decided not to have (§5.1).
//!
//! Three rows name a file outside this crate. That is the point: the
//! sentence was written where the code is, and a copy of it here would
//! pin nothing. The coupling is the tripwire — an edit to
//! `engenho-apiserver`, `engenho-etcd` or `engenho-runtime` that puts the
//! sentence back fails this test, in this crate.
//!
//! Each row names a file, the sentence that was retracted, a phrase from
//! the corrected text, and what would have to exist in code before the
//! retracted claim could return. The corrected phrase is the positive
//! control: it proves the file read is the one that was corrected, so a
//! moved, renamed or emptied file fails here instead of passing because
//! the retracted sentence is "absent".
//!
//! Both sides are matched against the file FLATTENED (see [`flatten`]),
//! because a doc comment's line wrapping is not a difference in the
//! claim. A sentence that fits on one line today wraps tomorrow, and a
//! raw `contains` would then read the claim as absent and go green —
//! measured, not feared: the `:2379` row below was written as a raw
//! match first and stayed green against the very sentence it retracts,
//! re-inserted wrapped after "there is no".
//!
//! Tier: a phrase tripwire. It stops the claim coming back in any
//! wrapping; a REWORDED claim still passes. Binding a doc claim to the
//! code it describes is T5.10's work.

use std::path::PathBuf;

/// The file as one line: every newline, the `//!`, `///` or `//` that
/// continues a comment on the next line, and every run of whitespace
/// collapse to a single space.
///
/// A claim is a sentence, not a sequence of source lines, so this is what a
/// row is matched against. Without it a row silently stops matching the
/// moment the sentence it names is rewrapped — which is a green test
/// reporting the claim gone when it is merely wrapped across two lines.
fn flatten(text: &str) -> String {
    let mut out = String::new();
    for line in text.lines() {
        let line = line.trim_start();
        // `//!` and `///` first: stripping `//` from them would leave a
        // stray `!` or `/` as a word and break the match.
        let line = line
            .strip_prefix("//!")
            .or_else(|| line.strip_prefix("///"))
            .or_else(|| line.strip_prefix("//"))
            .unwrap_or(line);
        for word in line.split_whitespace() {
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str(word);
        }
    }
    out
}

struct Retracted {
    /// Path relative to this crate's manifest directory.
    file: &'static str,
    /// The sentence that must not come back.
    retracted: &'static str,
    /// A phrase from the corrected text (the positive control).
    corrected: &'static str,
    /// What the code needs before the retracted claim is true.
    until: &'static str,
}

const RETRACTED: &[Retracted] = &[
    Retracted {
        file: "../docs/DISTRIBUTED.md",
        retracted: "split-brain is impossible",
        corrected: "split-brain freedom cannot be claimed until three missing pieces exist",
        until: "a durable Raft vote and log, a real has_majority, and a quorum check on promotion",
    },
    Retracted {
        file: "../docs/CONSISTENCY-FABRIC.md",
        retracted: "**Semantics:** linearizable.",
        corrected: "Reads are **not** linearizable.",
        until: "StoreMesh reads that go through a read-index round or a leader lease",
    },
    Retracted {
        file: "../docs/RESILIENCE.md",
        retracted: "reads are linearizable;",
        corrected: "so they are not linearizable.",
        until: "revoada reads that go through a read-index round or a leader lease",
    },
    Retracted {
        file: "../docs/FABRIC.md",
        // The whole document is a draft for a fabric engenho does not have.
        // The claim is written on one line at the top and wrapped after
        // "messaging" in "What this is NOT"; flattening makes both the
        // same string, so the whole phrase can be named here.
        retracted: "NATS is the messaging spine",
        corrected: "This document is a fenced-off draft. NATS is not engenho's fabric.",
        until: "`Fabric` has an arm other than `InBinary` — a broker engenho \
                actually runs, which edge 18 gates",
    },
    Retracted {
        file: "src/fabric.rs",
        retracted: "theorems the type system",
        corrected: "Neither is a property of a running fabric.",
        until: "a split-brain-free implementation behind ConsensusKind, not only a quorum-only enum",
    },
    Retracted {
        file: "src/topology.rs",
        retracted: "True if the assignment satisfies Raft majority",
        corrected: "A majority check is not a safety property on its own.",
        until: "a promotion path that calls has_majority and a durable vote for it to count",
    },
    // The three rows below leave this crate. Each sentence was written in
    // the source file it describes, so the tripwire has to read THAT file.
    Retracted {
        file: "../engenho-apiserver/src/params.rs",
        retracted: "which satisfies `NotOlderThan` by construction",
        corrected: "one ahead of the snapshot is refused in-band",
        until: "a read contract under which the store's revision is not older than \
                every client `resourceVersion`, instead of the ahead case being \
                refused in-band with a 410 (T3.9b)",
    },
    Retracted {
        file: "../engenho-etcd/src/lib.rs",
        retracted: "there is no listener on :2379",
        corrected: "Writes (`Put`, `DeleteRange`, `Txn`, `Compact`) and `Snapshot` are",
        until: "the runtime stops mounting the facade; `serve_etcd_facade` binds \
                `runtime.etcd_listen_addr` today, so a listener exists and is \
                merely read-only",
    },
    Retracted {
        file: "../engenho-runtime/src/runtime.rs",
        retracted: "holds an `Arc<StoreMesh>`",
        corrected: "`MeshEtcdStore` holds a `Weak<StoreMesh>`, not an",
        until: "never by intent: a strong handle would keep the store alive past \
                a stop, which `etcd_facade`'s no-strong-reference test pins",
    },
];

#[test]
fn retracted_safety_claims_stay_retracted() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for row in RETRACTED {
        let path = root.join(row.file);
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{}: cannot read ({e})", path.display()));
        let text = flatten(&raw);
        assert!(
            text.contains(row.corrected),
            "{}: the corrected text is gone (expected {:?}); this row no longer \
             checks the file it names",
            row.file,
            row.corrected,
        );
        assert!(
            !text.contains(row.retracted),
            "{}: retracted safety claim {:?} is back; it is false until {}",
            row.file,
            row.retracted,
            row.until,
        );
    }
}
