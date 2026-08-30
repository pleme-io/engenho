//! # junkyo (準拠) — adherence to a standard
//!
//! The **xfail axis** the differential harness was missing. [`cotejo`] answers
//! *"do the two sides agree?"*; junkyo answers the question that actually gates
//! a release: **"is this row's agreement-or-disagreement the one we declared?"**
//!
//! [`cotejo`]: crate::cotejo
//!
//! ## Why a bare [`Verdict`] cannot gate
//!
//! Before this module, a known divergence had exactly one representation:
//! **an absent test**. And an absent test is green. So engenho's eight measured
//! v1.34 divergences (2026-08-28) were invisible to CI not because anyone
//! decided to waive them, but because there was nowhere to write them down.
//! A waiver you cannot express becomes a waiver you cannot revoke.
//!
//! junkyo gives a divergence a **name, a reason, and a tier** — and then makes
//! the matrix fail in *both* directions:
//!
//! | declared | observed | outcome | gate |
//! |---|---|---|---|
//! | [`Expect::Match`] | `Parity` | [`Outcome::Held`] | pass |
//! | [`Expect::Match`] | `Divergent` | [`Outcome::Regressed`] | **FAIL** — a conformance regression |
//! | [`Expect::KnownDiverge`] | `Divergent` | [`Outcome::Tracked`] | pass — the debt is known |
//! | [`Expect::KnownDiverge`] | `Parity` | [`Outcome::Graduated`] | **FAIL** — fixed but undeclared |
//! | *any* | `ReferenceUnreachable` | [`Outcome::OracleLost`] | **FAIL** — never a silent skip |
//!
//! ## The load-bearing rule: graduation is a failure
//!
//! [`Outcome::Graduated`] failing is the half that is easy to get wrong and is
//! the whole point. If a `KnownDiverge` row silently passed once it started
//! matching, the matrix would rot downward exactly the way a dated coverage
//! claim does — it would keep reporting "8 known gaps" long after some were
//! fixed, and nobody would ever be told the debt shrank. Forcing the fixer to
//! *delete the waiver in the same commit* is what keeps the ledger honest, and
//! it is why the count of standing waivers is a number you can trust.
//!
//! This mirrors sui's `parity_corpus` (`sui/src/parity_corpus.rs`), the fleet's
//! proven Parity Method: `engenho:kube-apiserver :: sui:CppNix`. The shape is
//! deliberately the same so a reader who knows one knows the other.
//!
//! ## Tier honesty
//!
//! This is **only-mitigated (C2 — external-world observation)**, not
//! unrepresentability, and the ceiling is named: whether engenho matches
//! upstream is a fact about a *foreign process*, which no Rust type can decide.
//! The strongest available rung is a differential that cannot silently pass —
//! which is what this is. Do not describe junkyo as making divergence
//! unrepresentable; it makes an *undeclared* divergence unrepresentable **as a
//! green run**, which is a strictly weaker and honest claim.

use core::fmt;

use crate::verdict::Verdict;

/// Why a divergence is being tolerated. A closed vocabulary, so a waiver is
/// **classified rather than excused** — free-text reasons are how a debt
/// ledger becomes unreadable.
///
/// Adding a variant is deliberately a typed decision: if a new divergence does
/// not fit one of these, that is a signal worth stopping on, not a reason to
/// widen the enum reflexively.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DivergenceClass {
    /// engenho has not implemented the surface yet. The honest default for a
    /// milestone gap (e.g. Table conversion, absent workspace-wide).
    NotYetImplemented,
    /// engenho deliberately differs and intends to keep differing. Requires a
    /// stated rationale at the call site — this is the only variant that is
    /// not automatically a debt to burn down.
    IntentionalDeviation,
    /// The oracle and the pinned contract are at different patch levels, so
    /// the difference is upstream's, not engenho's. Keeps patch skew from
    /// being laundered through the normalizer.
    UpstreamPatchSkew,
    /// The divergence is real but its conformance relevance is not yet
    /// established. Recorded rather than guessed — promotion should be driven
    /// off upstream's `[Conformance]` tag list, never from memory.
    ConformanceRelevanceUnclassified,
}

impl DivergenceClass {
    /// Whether this class represents debt that should eventually reach zero.
    /// [`Self::IntentionalDeviation`] does not; everything else does.
    #[must_use]
    pub fn is_burn_down_debt(self) -> bool {
        !matches!(self, Self::IntentionalDeviation)
    }
}

/// What the matrix declares this row *should* do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expect {
    /// engenho must agree with the oracle. Any divergence fails the gate.
    Match,
    /// engenho is known to diverge here. The divergence is tolerated — but
    /// only while it persists: a row that starts matching fails as
    /// [`Outcome::Graduated`] until the waiver is removed.
    KnownDiverge {
        /// The classified reason. See [`DivergenceClass`].
        class: DivergenceClass,
        /// Short human note naming the specific defect, for the failure
        /// message a future reader will actually see.
        note: &'static str,
    },
}

/// The adjudicated result of one corpus row: `(Expect, Verdict) -> Outcome`.
///
/// Closed by construction — every pairing above maps into exactly one variant,
/// so a future `Verdict` variant is a non-exhaustive-match compile error here
/// rather than a silently-unhandled case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Declared `Match`, observed parity. The good case.
    Held,
    /// Declared `KnownDiverge`, observed divergence. Debt, but *known* debt.
    Tracked {
        /// The class carried through from the waiver, so a report can group
        /// the ledger without re-deriving it.
        class: DivergenceClass,
    },
    /// Declared `Match`, observed divergence. **A conformance regression.**
    Regressed {
        /// How many divergences the differ found, for the failure message.
        divergences: usize,
    },
    /// Declared `KnownDiverge`, observed parity. **Fixed but still waived** —
    /// remove the waiver in the same commit as the fix.
    Graduated {
        /// The note from the stale waiver, so the message can name what to delete.
        stale_note: &'static str,
    },
    /// The oracle could not be reached. Never a silent skip.
    OracleLost,
}

impl Outcome {
    /// Whether this outcome lets the gate stay green.
    ///
    /// Only [`Self::Held`] and [`Self::Tracked`] pass. In particular
    /// [`Self::Graduated`] does **not** — see the module docs for why that is
    /// the load-bearing half.
    #[must_use]
    pub fn passes(&self) -> bool {
        matches!(self, Self::Held | Self::Tracked { .. })
    }

    /// Whether this outcome fails the gate — the negation of [`Self::passes`],
    /// named positively so a call site reads as intent.
    #[must_use]
    pub fn is_failure(&self) -> bool {
        !self.passes()
    }
}

/// Renders the adjudication verdict. `write!()` inside a `Display` block is
/// this crate's sanctioned typed emission (★★ TYPED EMISSION) — the reason
/// text is NEVER `format!()`-composed free text.
impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Held => write!(f, "HELD"),
            Self::Tracked { class } => write!(f, "TRACKED ({class})"),
            Self::Regressed { divergences } => write!(
                f,
                "REGRESSED: declared Match but observed {divergences} divergence(s)"
            ),
            Self::Graduated { stale_note } => write!(
                f,
                "GRADUATED: declared KnownDiverge ({stale_note}) but observed parity \
                 — delete the waiver in the same commit as the fix"
            ),
            Self::OracleLost => write!(f, "ORACLE LOST: the reference could not be reached"),
        }
    }
}

/// A stable label per class, so a report groups the ledger without re-deriving.
impl fmt::Display for DivergenceClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::NotYetImplemented => "not-yet-implemented",
                Self::IntentionalDeviation => "intentional-deviation",
                Self::UpstreamPatchSkew => "upstream-patch-skew",
                Self::ConformanceRelevanceUnclassified => "conformance-relevance-unclassified",
            }
        )
    }
}

/// Adjudicate one row: what we declared against what we observed.
///
/// This is the whole invariant, and it is deliberately total over both enums.
#[must_use]
pub fn adjudicate(expect: &Expect, verdict: &Verdict) -> Outcome {
    match (expect, verdict) {
        // The oracle arm is checked first: an unreachable oracle tells us
        // nothing about either side, so it can never be read as agreement.
        (_, Verdict::ReferenceUnreachable) => Outcome::OracleLost,

        (Expect::Match, Verdict::Parity(_)) => Outcome::Held,
        (Expect::Match, Verdict::Divergent(d)) => Outcome::Regressed {
            divergences: d.len(),
        },

        (Expect::KnownDiverge { class, .. }, Verdict::Divergent(_)) => {
            Outcome::Tracked { class: *class }
        }
        (Expect::KnownDiverge { note, .. }, Verdict::Parity(_)) => {
            Outcome::Graduated { stale_note: note }
        }
    }
}

/// The verdict over a whole corpus run.
///
/// Carries its own **denominator** (`rows`) inside the compared value — the
/// fleet's anti-vacuity rule. A matrix that stopped discovering rows must fail
/// loudly rather than report an empty green, which is exactly how a coverage
/// gate rots without anyone noticing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatrixReport {
    /// Total rows adjudicated. The denominator.
    pub rows: usize,
    /// Rows that held (declared Match, observed parity).
    pub held: usize,
    /// Rows tracked as known divergences.
    pub tracked: usize,
    /// Failures, each `(row name, the adjudicated outcome)`. The outcome is
    /// kept typed rather than pre-rendered — it renders through [`Display`]
    /// at report time, never as a stored string.
    pub failures: Vec<(String, Outcome)>,
}

impl MatrixReport {
    /// Fold adjudicated rows into a report.
    ///
    /// `named` is `(row name, outcome)`.
    #[must_use]
    pub fn from_rows(named: impl IntoIterator<Item = (String, Outcome)>) -> Self {
        let mut report = Self {
            rows: 0,
            held: 0,
            tracked: 0,
            failures: Vec::new(),
        };
        for (name, outcome) in named {
            report.rows += 1;
            match &outcome {
                Outcome::Held => report.held += 1,
                Outcome::Tracked { .. } => report.tracked += 1,
                _ => {}
            }
            if outcome.is_failure() {
                report.failures.push((name, outcome));
            }
        }
        report
    }

    /// Whether the gate is green.
    ///
    /// **An empty corpus is NOT green.** `rows == 0` means the matrix
    /// discovered nothing, which is indistinguishable from every row having
    /// been deleted — the vacuity failure this field exists to catch.
    #[must_use]
    pub fn is_green(&self) -> bool {
        self.rows > 0 && self.failures.is_empty()
    }
}

/// One line carrying the denominator, suitable for a gate's output. Always
/// states `rows`, so a SHRINKING corpus is visible in the log even on a green
/// run — the anti-vacuity rule made readable.
impl fmt::Display for MatrixReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "junkyo: {} rows — {} held, {} tracked, {} failed",
            self.rows,
            self.held,
            self.tracked,
            self.failures.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::path::JsonPath;
    use crate::verdict::{Divergence, ParityWitness, Side, Verdict};

    /// A `Parity` verdict. `ParityWitness::new()` is `pub(crate)` — proof-by-
    /// absence — so only an in-crate test can build one, which is the point.
    fn parity() -> Verdict {
        Verdict::Parity(ParityWitness::new())
    }

    /// A `Divergent` verdict carrying one arbitrary divergence.
    fn divergent() -> Verdict {
        Verdict::Divergent(vec![Divergence::MissingResource {
            path: JsonPath::root(),
            present_on: Side::K3s,
        }])
    }

    fn known() -> Expect {
        Expect::KnownDiverge {
            class: DivergenceClass::NotYetImplemented,
            note: "Table conversion absent",
        }
    }

    #[test]
    fn match_plus_parity_holds() {
        let v = parity();
        assert_eq!(adjudicate(&Expect::Match, &v), Outcome::Held);
        assert!(adjudicate(&Expect::Match, &v).passes());
    }

    #[test]
    fn match_plus_divergence_is_a_regression() {
        let out = adjudicate(&Expect::Match, &divergent());
        assert!(matches!(out, Outcome::Regressed { divergences: 1 }));
        assert!(!out.passes(), "a regression MUST fail the gate");
    }

    #[test]
    fn known_diverge_plus_divergence_is_tracked() {
        let out = adjudicate(&known(), &divergent());
        assert!(out.passes(), "known debt does not fail the gate");
        assert!(matches!(
            out,
            Outcome::Tracked {
                class: DivergenceClass::NotYetImplemented
            }
        ));
    }

    /// The load-bearing rule: a fixed-but-still-waived row FAILS.
    #[test]
    fn known_diverge_plus_parity_is_a_failure() {
        let out = adjudicate(&known(), &parity());
        assert!(
            !out.passes(),
            "a KnownDiverge that starts matching MUST fail — otherwise the \
             ledger silently over-reports debt forever"
        );
        assert!(out.to_string().contains("GRADUATED"));
    }

    #[test]
    fn unreachable_oracle_never_reads_as_agreement() {
        for e in [Expect::Match, known()] {
            let out = adjudicate(&e, &Verdict::ReferenceUnreachable);
            assert_eq!(out, Outcome::OracleLost);
            assert!(
                !out.passes(),
                "an unreachable oracle is never a silent skip"
            );
        }
    }

    /// Anti-vacuity: an empty corpus is not green.
    #[test]
    fn empty_corpus_is_not_green() {
        let r = MatrixReport::from_rows(std::iter::empty());
        assert_eq!(r.rows, 0);
        assert!(
            !r.is_green(),
            "an empty matrix must FAIL — a corpus that discovered nothing is \
             indistinguishable from one whose rows were all deleted"
        );
    }

    #[test]
    fn report_carries_its_denominator() {
        let r = MatrixReport::from_rows([
            ("a".to_string(), Outcome::Held),
            (
                "b".to_string(),
                Outcome::Tracked {
                    class: DivergenceClass::NotYetImplemented,
                },
            ),
        ]);
        assert!(r.is_green());
        assert_eq!((r.rows, r.held, r.tracked), (2, 1, 1));
        assert!(
            r.to_string().contains("2 rows"),
            "denominator must be visible"
        );
    }

    #[test]
    fn a_single_failure_reddens_the_whole_matrix() {
        let r = MatrixReport::from_rows([
            ("ok".to_string(), Outcome::Held),
            ("bad".to_string(), Outcome::Regressed { divergences: 3 }),
        ]);
        assert!(!r.is_green());
        assert_eq!(r.failures.len(), 1);
        assert!(r.failures[0].1.to_string().contains("REGRESSED"));
    }

    #[test]
    fn intentional_deviation_is_the_only_non_debt_class() {
        assert!(!DivergenceClass::IntentionalDeviation.is_burn_down_debt());
        for c in [
            DivergenceClass::NotYetImplemented,
            DivergenceClass::UpstreamPatchSkew,
            DivergenceClass::ConformanceRelevanceUnclassified,
        ] {
            assert!(c.is_burn_down_debt());
        }
    }
}
