//! Upstream oracle tables.
//!
//! Each file in `vectors/` is a list of behaviour rows taken from upstream
//! source (Kubernetes v1.34.0, kube-rs 4.2.0). Every row names the upstream
//! line it came from, and the `.md` beside each file says how it was built.
//! The tables are data. This crate is the one harness that runs a table
//! against engenho.
//!
//! A run is closed in both directions, so a table cannot pass by default:
//!
//! - Every row is **claimed**. Either the adapter checks it, or its kind is
//!   declared out of scope with a reason. A row nobody claimed fails the run,
//!   so a new upstream row forces a decision.
//! - Every exemption is **live**. A declared deviation must name a checked row
//!   that still disagrees; an out-of-scope kind must still occur in the table
//!   and must not also be checked. A stale exemption fails the run.
//!
//! What a check compares: when a row's `expected` is an object, the adapter
//! returns an object holding a non-empty subset of its keys, and each returned
//! key must equal upstream's value. Keys the adapter does not return are listed
//! per kind in the [`Report`], so a narrowed check stays visible. When
//! `expected` is not an object, the adapter's value must equal it whole.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::Deserialize;
use serde_json::Value;

/// One upstream table, compiled into the crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Vector {
    /// `ResourceMatches` in RBAC evaluation (T4.2).
    RbacResourceMatches,
    /// Probe worker and prober results (T1.1).
    ProberResults,
    /// Container exit, restart and CrashLoopBackOff (T1.2, T2.4).
    ContainerRestart,
    /// Watch-cache 410, bookmarks and watch end forms (T3.9a, T3.7).
    Watch410Bookmark,
    /// What client-go and kube-rs do with 410 and 429 (T3.7).
    Watch429Clients,
    /// Garbage-collector owner resolution (gc owner presence, T3.6).
    GcOwnerResolution,
}

impl Vector {
    /// Every table. A new file in `vectors/` without a variant here fails
    /// `tests/tables.rs::every_vector_file_has_a_variant`.
    pub const ALL: [Self; 6] = [
        Self::RbacResourceMatches,
        Self::ProberResults,
        Self::ContainerRestart,
        Self::Watch410Bookmark,
        Self::Watch429Clients,
        Self::GcOwnerResolution,
    ];

    /// The file stem under `vectors/`.
    #[must_use]
    pub const fn stem(self) -> &'static str {
        match self {
            Self::RbacResourceMatches => "rbac-resource-matches",
            Self::ProberResults => "prober-results",
            Self::ContainerRestart => "container-restart",
            Self::Watch410Bookmark => "watch-410-bookmark",
            Self::Watch429Clients => "watch-429-clients",
            Self::GcOwnerResolution => "gc-owner-resolution",
        }
    }

    const fn json(self) -> &'static str {
        match self {
            Self::RbacResourceMatches => include_str!("../vectors/rbac-resource-matches.json"),
            Self::ProberResults => include_str!("../vectors/prober-results.json"),
            Self::ContainerRestart => include_str!("../vectors/container-restart.json"),
            Self::Watch410Bookmark => include_str!("../vectors/watch-410-bookmark.json"),
            Self::Watch429Clients => include_str!("../vectors/watch-429-clients.json"),
            Self::GcOwnerResolution => include_str!("../vectors/gc-owner-resolution.json"),
        }
    }

    /// The row's kind: the value its out-of-scope declarations are keyed by.
    /// Each table names it differently; this is the one place that knows how.
    fn kind_of(self, case: &Case) -> String {
        let field = |v: &Value, key: &str| v.get(key).and_then(Value::as_str).map(str::to_owned);
        let kind = match self {
            Self::RbacResourceMatches => Some("resource_matches".to_owned()),
            Self::ContainerRestart => field(&case.extra_value(), "kind"),
            Self::ProberResults => field(&case.input, "kind"),
            Self::GcOwnerResolution => field(&case.input, "fn"),
            Self::Watch410Bookmark => field(&case.extra_value(), "layer"),
            Self::Watch429Clients => field(&case.extra_value(), "client"),
        };
        kind.unwrap_or_else(|| "<no kind>".to_owned())
    }

    /// Load and parse this table.
    ///
    /// # Panics
    ///
    /// When the compiled-in fixture does not parse, or two rows share a name.
    /// Both are defects in the fixture, caught by `tests/tables.rs`.
    #[must_use]
    pub fn load(self) -> Table {
        Table::parse(self, self.json()).unwrap_or_else(|e| panic!("{e}"))
    }
}

impl fmt::Display for Vector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.stem())
    }
}

/// One upstream row.
#[derive(Debug, Clone, Deserialize)]
pub struct Case {
    /// Unique within its table. Deviations are keyed by it.
    pub name: String,
    /// The situation upstream was given.
    pub input: Value,
    /// What upstream did.
    pub expected: Value,
    /// The upstream line the row came from.
    #[serde(default)]
    pub upstream_ref: Option<String>,
    /// Table-specific fields (`kind`, `layer`, `client`, `note`, `naive_trap`, …).
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

impl Case {
    fn extra_value(&self) -> Value {
        Value::Object(self.extra.clone())
    }
}

#[derive(Deserialize)]
struct RawTable {
    cases: Vec<Case>,
}

/// A parsed table.
#[derive(Debug, Clone)]
pub struct Table {
    /// Which table.
    pub vector: Vector,
    /// Its rows, in file order.
    pub cases: Vec<Case>,
}

/// Why a fixture could not be loaded.
#[derive(Debug)]
pub enum LoadError {
    /// The JSON did not parse into rows.
    Parse(Vector, serde_json::Error),
    /// Two rows share a name, so a deviation could not say which it means.
    DuplicateName(Vector, String),
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(v, e) => write!(f, "vectors/{v}.json does not parse: {e}"),
            Self::DuplicateName(v, n) => write!(f, "vectors/{v}.json has two rows named {n:?}"),
        }
    }
}

impl std::error::Error for LoadError {}

impl Table {
    /// Parse a table from JSON. Public so the harness's own tests can build
    /// synthetic tables.
    ///
    /// # Errors
    ///
    /// [`LoadError`] when the JSON is malformed or row names repeat.
    pub fn parse(vector: Vector, json: &str) -> Result<Self, LoadError> {
        let raw: RawTable = serde_json::from_str(json).map_err(|e| LoadError::Parse(vector, e))?;
        let mut seen = BTreeSet::new();
        for case in &raw.cases {
            if !seen.insert(case.name.as_str()) {
                return Err(LoadError::DuplicateName(vector, case.name.clone()));
            }
        }
        Ok(Self {
            vector,
            cases: raw.cases,
        })
    }

    /// Every distinct kind in the table.
    #[must_use]
    pub fn kinds(&self) -> BTreeSet<String> {
        self.cases.iter().map(|c| self.vector.kind_of(c)).collect()
    }

    /// The kind of one row.
    #[must_use]
    pub fn kind_of(&self, case: &Case) -> String {
        self.vector.kind_of(case)
    }
}

/// What an out-of-scope declaration covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// Every row of this kind, exactly as [`Table::kinds`] reports it.
    Kind(&'static str),
    /// One row, by name. For a kind engenho answers only in part: a row whose
    /// situation is decided outside the code the adapter drives.
    Case(&'static str),
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Kind(k) => write!(f, "kind {k:?}"),
            Self::Case(c) => write!(f, "row {c:?}"),
        }
    }
}

/// Rows engenho has no counterpart for, and why.
#[derive(Debug, Clone, Copy)]
pub struct OutOfScope {
    /// Which rows.
    pub scope: Scope,
    /// Why no engenho code answers them.
    pub why: &'static str,
}

impl OutOfScope {
    /// Every row of `kind`.
    #[must_use]
    pub const fn kind(kind: &'static str, why: &'static str) -> Self {
        Self {
            scope: Scope::Kind(kind),
            why,
        }
    }

    /// The one row named `case`.
    #[must_use]
    pub const fn case(case: &'static str, why: &'static str) -> Self {
        Self {
            scope: Scope::Case(case),
            why,
        }
    }
}

/// A checked row where engenho deliberately differs from upstream.
#[derive(Debug, Clone, Copy)]
pub struct Deviation {
    /// The row's name.
    pub case: &'static str,
    /// Why engenho differs, and where that decision is recorded.
    pub why: &'static str,
}

/// What the adapter said about one row.
#[derive(Debug, Clone)]
pub enum Answer {
    /// engenho's behaviour, in upstream's `expected` shape.
    Checked(Value),
    /// The adapter does not check this row. Its kind must be declared
    /// [`OutOfScope`].
    NotChecked,
}

/// One way a run failed.
#[derive(Debug, Clone)]
pub enum Failure {
    /// A checked row disagrees with upstream and is not a declared deviation.
    Disagrees {
        case: String,
        upstream_ref: Option<String>,
        expected: Value,
        got: Value,
    },
    /// The adapter's value is not comparable with `expected`: not an object
    /// where upstream has one, empty, or holding a key upstream lacks.
    Unshaped {
        case: String,
        why: &'static str,
        got: Value,
    },
    /// A row nobody checked that no out-of-scope declaration covers.
    Unclaimed { case: String, kind: String },
    /// An out-of-scope declaration naming a kind or row the table does not have.
    StaleOutOfScope { scope: Scope },
    /// A row declared out of scope that the adapter checked after all.
    CheckedOutOfScope { case: String, scope: Scope },
    /// A deviation naming a row the table does not have.
    DeviationNoSuchRow { case: &'static str },
    /// A deviation on a row the adapter did not check.
    DeviationNotChecked { case: &'static str },
    /// A deviation on a row that now agrees with upstream.
    DeviationNowAgrees { case: &'static str },
    /// The adapter checked nothing.
    Vacuous,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disagrees {
                case,
                upstream_ref,
                expected,
                got,
            } => {
                write!(f, "{case}: upstream {expected}, engenho {got}")?;
                if let Some(r) = upstream_ref {
                    write!(f, " [{r}]")?;
                }
                Ok(())
            }
            Self::Unshaped { case, why, got } => write!(f, "{case}: adapter answer {got} {why}"),
            Self::Unclaimed { case, kind } => {
                write!(
                    f,
                    "{case}: kind {kind:?} is neither checked nor declared out of scope"
                )
            }
            Self::StaleOutOfScope { scope } => {
                write!(f, "out-of-scope {scope} does not occur in the table")
            }
            Self::CheckedOutOfScope { case, scope } => {
                write!(
                    f,
                    "{case}: {scope} is declared out of scope but was checked"
                )
            }
            Self::DeviationNoSuchRow { case } => write!(f, "deviation names no row: {case:?}"),
            Self::DeviationNotChecked { case } => {
                write!(f, "deviation on a row the adapter does not check: {case:?}")
            }
            Self::DeviationNowAgrees { case } => {
                write!(
                    f,
                    "deviation on a row that now agrees with upstream: {case:?}"
                )
            }
            Self::Vacuous => f.write_str("the adapter checked no row"),
        }
    }
}

/// A run that passed.
#[derive(Debug, Clone, Default)]
pub struct Report {
    /// Rows the adapter checked.
    pub checked: usize,
    /// Checked rows that agree with upstream.
    pub agree: usize,
    /// Checked rows that disagree, each a declared deviation.
    pub deviations: usize,
    /// Rows skipped because their kind is out of scope.
    pub out_of_scope: usize,
    /// Per kind, the `expected` keys the adapter never compared.
    pub unchecked_keys: BTreeMap<String, BTreeSet<String>>,
}

/// A run that failed, with every failure found (not just the first).
#[derive(Debug, Clone)]
pub struct Failures {
    /// Which table.
    pub vector: Vector,
    /// Each failure.
    pub failures: Vec<Failure>,
}

impl fmt::Display for Failures {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "oracle table {} failed ({} findings):",
            self.vector,
            self.failures.len()
        )?;
        for failure in &self.failures {
            writeln!(f, "  - {failure}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Failures {}

enum Compared {
    Agrees,
    Disagrees,
    Unshaped(&'static str),
}

/// Compare an adapter answer with upstream, recording keys left unchecked.
fn compare(expected: &Value, got: &Value, unchecked: &mut BTreeSet<String>) -> Compared {
    let Value::Object(exp) = expected else {
        return if got == expected {
            Compared::Agrees
        } else {
            Compared::Disagrees
        };
    };
    let Value::Object(got) = got else {
        return Compared::Unshaped("is not an object; upstream's expected is");
    };
    if got.is_empty() {
        return Compared::Unshaped("is empty");
    }
    if got.keys().any(|k| !exp.contains_key(k)) {
        return Compared::Unshaped("holds a key upstream's expected lacks");
    }
    unchecked.extend(exp.keys().filter(|k| !got.contains_key(*k)).cloned());
    if got.iter().all(|(k, v)| exp.get(k) == Some(v)) {
        Compared::Agrees
    } else {
        Compared::Disagrees
    }
}

/// Run a table against an adapter.
///
/// # Errors
///
/// [`Failures`] listing every disagreement, unclaimed row and stale exemption.
pub fn run(
    table: &Table,
    out_of_scope: &[OutOfScope],
    deviations: &[Deviation],
    mut adapter: impl FnMut(&Case) -> Answer,
) -> Result<Report, Failures> {
    let mut report = Report::default();
    let mut failures = Vec::new();
    let kinds = table.kinds();
    let names: BTreeSet<&str> = table.cases.iter().map(|c| c.name.as_str()).collect();
    // A row's declaration: its own name first, then its kind.
    let covering = |case: &Case, kind: &str| -> Option<Scope> {
        out_of_scope.iter().map(|o| o.scope).find(|s| match s {
            Scope::Case(c) => *c == case.name,
            Scope::Kind(k) => *k == kind,
        })
    };
    let deviating: BTreeSet<&str> = deviations.iter().map(|d| d.case).collect();
    let mut checked_rows = BTreeSet::new();
    let mut disagreeing_rows = BTreeSet::new();

    for o in out_of_scope {
        let present = match o.scope {
            Scope::Kind(k) => kinds.contains(k),
            Scope::Case(c) => names.contains(c),
        };
        if !present {
            failures.push(Failure::StaleOutOfScope { scope: o.scope });
        }
    }

    for case in &table.cases {
        let kind = table.kind_of(case);
        match adapter(case) {
            Answer::NotChecked => match covering(case, &kind) {
                Some(_) => report.out_of_scope += 1,
                None => failures.push(Failure::Unclaimed {
                    case: case.name.clone(),
                    kind,
                }),
            },
            Answer::Checked(got) => {
                if let Some(scope) = covering(case, &kind) {
                    failures.push(Failure::CheckedOutOfScope {
                        case: case.name.clone(),
                        scope,
                    });
                }
                report.checked += 1;
                checked_rows.insert(case.name.as_str());
                let unchecked = report.unchecked_keys.entry(kind).or_default();
                match compare(&case.expected, &got, unchecked) {
                    Compared::Agrees => report.agree += 1,
                    Compared::Unshaped(why) => {
                        failures.push(Failure::Unshaped {
                            case: case.name.clone(),
                            why,
                            got,
                        });
                    }
                    Compared::Disagrees if deviating.contains(case.name.as_str()) => {
                        report.deviations += 1;
                        disagreeing_rows.insert(case.name.as_str());
                    }
                    Compared::Disagrees => failures.push(Failure::Disagrees {
                        case: case.name.clone(),
                        upstream_ref: case.upstream_ref.clone(),
                        expected: case.expected.clone(),
                        got,
                    }),
                }
            }
        }
    }
    report.unchecked_keys.retain(|_, keys| !keys.is_empty());

    for d in deviations {
        if !table.cases.iter().any(|c| c.name == d.case) {
            failures.push(Failure::DeviationNoSuchRow { case: d.case });
        } else if !checked_rows.contains(d.case) {
            failures.push(Failure::DeviationNotChecked { case: d.case });
        } else if !disagreeing_rows.contains(d.case) {
            failures.push(Failure::DeviationNowAgrees { case: d.case });
        }
    }
    if report.checked == 0 {
        failures.push(Failure::Vacuous);
    }

    if failures.is_empty() {
        Ok(report)
    } else {
        Err(Failures {
            vector: table.vector,
            failures,
        })
    }
}

/// [`run`] for a test: panics with every failure listed.
///
/// # Panics
///
/// When [`run`] fails.
pub fn assert_table(
    table: &Table,
    out_of_scope: &[OutOfScope],
    deviations: &[Deviation],
    adapter: impl FnMut(&Case) -> Answer,
) -> Report {
    run(table, out_of_scope, deviations, adapter).unwrap_or_else(|f| panic!("{f}"))
}
