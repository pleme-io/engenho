//! Safety claims the docs made about revoada and the store, which the
//! code does not deliver (IMPROVEMENT-PLAN T5.4).
//!
//! Each row names a file, the sentence that was retracted, a phrase from
//! the corrected text, and what would have to exist in code before the
//! retracted claim could return. The corrected phrase is the positive
//! control: it proves the file read is the one that was corrected, so a
//! moved, renamed or emptied file fails here instead of passing because
//! the retracted sentence is "absent".
//!
//! Tier: a phrase tripwire. It stops the exact sentence coming back; a
//! reworded claim passes. Binding a doc claim to the code it describes
//! is T5.10's work.

use std::path::PathBuf;

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
        corrected: "so they are not\n   linearizable.",
        until: "revoada reads that go through a read-index round or a leader lease",
    },
    Retracted {
        file: "src/fabric.rs",
        retracted: "theorems the type system",
        corrected: "Neither\n//! is a property of a running fabric.",
        until: "a split-brain-free implementation behind ConsensusKind, not only a quorum-only enum",
    },
    Retracted {
        file: "src/topology.rs",
        retracted: "True if the assignment satisfies Raft majority",
        corrected: "**Not a majority check yet.**",
        until: "has_majority counting against the configured voter set",
    },
];

#[test]
fn retracted_safety_claims_stay_retracted() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for row in RETRACTED {
        let path = root.join(row.file);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{}: cannot read ({e})", path.display()));
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
