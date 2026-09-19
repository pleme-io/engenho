//! rollout — one gate type with a shadow mode and an enforce mode.
//!
//! ★ WHY ONE TYPE. Several changes tighten a check that objects already in
//! the cluster may violate: the store's boot tripwire, RBAC's would-deny,
//! defaulting and validation per GVK, conformance rows, scheduler filters.
//! Each one needs the same two steps: first see what the tighter check
//! WOULD refuse, then refuse. Written once per check, that is one log line,
//! one counter and one ad-hoc boolean per site, each drifting on its own.
//! Here it is one [`Rollout`] value per named [`Gate`], one ledger, one
//! metric family (`engenho_would_reject_total{gate,reason}`, rendered by the
//! apiserver) and one Event reason ([`WouldReject::EVENT_REASON`]).
//!
//! ★ THE MODE IS CODE, NOT CONFIGURATION. A gate's [`Rollout`] is fixed in
//! the source for a release and changes in a reviewed diff. [`Rollout`]
//! therefore implements neither `Deserialize` nor `FromStr` nor `Default`:
//! no config file, flag or environment variable can carry one, because none
//! of them can be parsed into one.
//!
//! ```
//! fn requires_deserialize<T: serde::de::DeserializeOwned>() {}
//! requires_deserialize::<u32>(); // the harness itself compiles
//! ```
//!
//! ```compile_fail,E0277
//! fn requires_deserialize<T: serde::de::DeserializeOwned>() {}
//! requires_deserialize::<engenho_substrate::rollout::Rollout>();
//! ```
//!
//! ```compile_fail,E0277
//! let _ = "shadow".parse::<engenho_substrate::rollout::Rollout>();
//! ```
//!
//! ★ SHADOW CANNOT ALLOW SILENTLY. [`Gate::judge`] is the only path from a
//! failed check to "proceed", and it records into the [`WouldRejectLedger`]
//! before it returns. The ledger's record method is private to this module,
//! so a count cannot come from anywhere else, and the ledger's constructor
//! requires the log hook, so a ledger that counts without logging has to be
//! asked for explicitly.
//!
//! ★ AN ENFORCED REFUSAL IS NOT A WOULD-REJECT. In Enforce the gate refuses
//! through the caller's own typed error, which already has its own surface
//! (an HTTP status, a condition, a failed boot). The would-reject series
//! counts only what was allowed despite failing, which is exactly what an
//! operator reads before flipping a gate to Enforce.
//!
//! ★ LABELS ARE BOUNDED BY TYPE. The gate name and the reason label are
//! `&'static str`, so a metric label can never be built from a request, an
//! object name or an error message. The unbounded part, what was judged,
//! travels only to the log hook as [`WouldReject::subject`].
//!
//! Tier: the mode's parse-absence is a compile error (the doctests above);
//! "shadow allows only after counting" is structural inside this module.
//! Whether a check is routed through a gate at all is the caller's choice,
//! so that part is only-mitigated.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Mutex, PoisonError};

/// How a named gate acts on a failed check.
///
/// Set in code per release. See the module docs for why this type cannot be
/// parsed from configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Rollout {
    /// Allow, and record the refusal Enforce would have made.
    Shadow,
    /// Refuse.
    Enforce,
}

impl Rollout {
    /// The stable lowercase name, for logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Shadow => "shadow",
            Self::Enforce => "enforce",
        }
    }
}

impl fmt::Display for Rollout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A typed reason a gate's check can fail with.
///
/// Implement it on the check's own error enum. The label becomes the
/// `reason` metric label, so it must be a small, stable, `snake_case` set.
pub trait RejectReason {
    /// The bounded metric label for this reason.
    fn label(&self) -> &'static str;
}

/// A named check with a fixed [`Rollout`].
///
/// Declare each gate once, as a `const`, next to the check it guards:
///
/// ```
/// use engenho_substrate::rollout::{Gate, Rollout};
/// pub const BOOT_TRIPWIRE: Gate = Gate::new("boot_tripwire", Rollout::Shadow);
/// assert_eq!(BOOT_TRIPWIRE.name(), "boot_tripwire");
/// ```
///
/// The name is the `gate` metric label. Two gates that share a name share a
/// series, so names are `snake_case` and unique per process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gate {
    name: &'static str,
    rollout: Rollout,
}

impl Gate {
    /// A gate with a fixed name and mode.
    #[must_use]
    pub const fn new(name: &'static str, rollout: Rollout) -> Self {
        Self { name, rollout }
    }

    /// The gate's name, which is its `gate` metric label.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// The gate's mode for this release.
    #[must_use]
    pub const fn rollout(&self) -> Rollout {
        self.rollout
    }

    /// Apply this gate to a check's result.
    ///
    /// - A passing check returns [`Proceed::Clean`] and records nothing.
    /// - A failing check in [`Rollout::Shadow`] is recorded into `ledger`
    ///   (counted, and handed to the ledger's log hook with `subject`) and
    ///   returns [`Proceed::Shadowed`]: the caller proceeds.
    /// - A failing check in [`Rollout::Enforce`] returns [`Refused`]: the
    ///   caller refuses through its own error.
    ///
    /// # Errors
    ///
    /// [`Refused`] when the check failed and this gate enforces.
    pub fn judge<R: RejectReason>(
        &self,
        check: Result<(), R>,
        ledger: &WouldRejectLedger,
        subject: &dyn fmt::Display,
    ) -> Result<Proceed<R>, Refused<R>> {
        let Err(reason) = check else {
            return Ok(Proceed::Clean);
        };
        match self.rollout {
            Rollout::Shadow => {
                ledger.record(&WouldReject {
                    gate: self.name,
                    reason: reason.label(),
                    subject,
                });
                Ok(Proceed::Shadowed(reason))
            }
            Rollout::Enforce => Err(Refused {
                gate: self.name,
                reason,
            }),
        }
    }
}

/// What a gate lets the caller do.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Proceed<R> {
    /// The check passed.
    Clean,
    /// The check failed, the gate is in Shadow, and the refusal was recorded.
    /// The reason is returned so the caller can surface it further, for
    /// example as a warning or an Event with [`WouldReject::EVENT_REASON`].
    Shadowed(R),
}

/// An enforcing gate refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refused<R> {
    gate: &'static str,
    reason: R,
}

impl<R> Refused<R> {
    /// The gate that refused.
    #[must_use]
    pub fn gate(&self) -> &'static str {
        self.gate
    }

    /// Why it refused.
    #[must_use]
    pub fn reason(&self) -> &R {
        &self.reason
    }

    /// Why it refused, by value, for mapping into the caller's own error.
    #[must_use]
    pub fn into_reason(self) -> R {
        self.reason
    }
}

impl<R: RejectReason> fmt::Display for Refused<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "gate {} refused: {}", self.gate, self.reason.label())
    }
}

impl<R: RejectReason + fmt::Debug> std::error::Error for Refused<R> {}

/// One refusal a Shadow gate allowed, as handed to the ledger's log hook.
#[derive(Clone, Copy)]
pub struct WouldReject<'a> {
    /// The gate's name.
    pub gate: &'static str,
    /// The reason's bounded label.
    pub reason: &'static str,
    /// What was judged. Log-only: it never becomes a metric label.
    pub subject: &'a dyn fmt::Display,
}

impl WouldReject<'_> {
    /// The one Event reason every gate uses for a would-reject.
    ///
    /// Not an upstream reason: upstream has no shadow gates. It shares its
    /// vocabulary with the `engenho_would_reject_total` metric so the two
    /// are found together. Severity is `Warning`.
    pub const EVENT_REASON: &'static str = "WouldReject";
}

impl fmt::Debug for WouldReject<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WouldReject")
            .field("gate", &self.gate)
            .field("reason", &self.reason)
            .field("subject", &format_args!("{}", self.subject))
            .finish()
    }
}

impl fmt::Display for WouldReject<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "gate {} would refuse {}: {}",
            self.gate, self.subject, self.reason
        )
    }
}

/// The log hook a [`WouldRejectLedger`] calls once per recorded refusal.
///
/// A plain function, because logging is global: the production hook is a
/// structured `tracing` event supplied by the process that owns logging.
pub type WouldRejectHook = fn(&WouldReject<'_>);

/// One `(gate, reason)` series and its count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WouldRejectCount {
    /// The gate's name.
    pub gate: &'static str,
    /// The reason's label.
    pub reason: &'static str,
    /// How many refusals the gate allowed in Shadow.
    pub count: u64,
}

/// The process's would-reject counts, keyed by gate and reason.
///
/// Share one `Arc<WouldRejectLedger>` across every gate in a process so that
/// one `/metrics` scrape shows them all.
pub struct WouldRejectLedger {
    counts: Mutex<BTreeMap<(&'static str, &'static str), u64>>,
    hook: WouldRejectHook,
}

impl WouldRejectLedger {
    /// An empty ledger that calls `hook` for every refusal it records.
    #[must_use]
    pub fn new(hook: WouldRejectHook) -> Self {
        Self {
            counts: Mutex::new(BTreeMap::new()),
            hook,
        }
    }

    /// Count and log one refusal. Private: only [`Gate::judge`] records.
    fn record(&self, event: &WouldReject<'_>) {
        {
            // A poisoned lock still guards a consistent map: every write is
            // a single entry update, so there is no half-applied state.
            let mut counts = self.counts.lock().unwrap_or_else(PoisonError::into_inner);
            let n = counts.entry((event.gate, event.reason)).or_insert(0);
            *n = n.saturating_add(1);
        }
        (self.hook)(event);
    }

    /// Every series recorded so far, ordered by gate then reason.
    #[must_use]
    pub fn snapshot(&self) -> Vec<WouldRejectCount> {
        let counts = self.counts.lock().unwrap_or_else(PoisonError::into_inner);
        counts
            .iter()
            .map(|(&(gate, reason), &count)| WouldRejectCount {
                gate,
                reason,
                count,
            })
            .collect()
    }

    /// The count for one series; zero when it was never recorded.
    #[must_use]
    pub fn count(&self, gate: &str, reason: &str) -> u64 {
        let counts = self.counts.lock().unwrap_or_else(PoisonError::into_inner);
        counts.get(&(gate, reason)).copied().unwrap_or(0)
    }
}

impl fmt::Debug for WouldRejectLedger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WouldRejectLedger")
            .field("counts", &self.snapshot())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Why {
        Stale,
        Missing,
    }

    impl RejectReason for Why {
        fn label(&self) -> &'static str {
            match self {
                Self::Stale => "stale",
                Self::Missing => "missing",
            }
        }
    }

    const SHADOW: Gate = Gate::new("test_gate", Rollout::Shadow);
    const ENFORCE: Gate = Gate::new("test_gate", Rollout::Enforce);

    thread_local! {
        static LOGGED: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    }

    fn remember(event: &WouldReject<'_>) {
        LOGGED.with(|l| l.borrow_mut().push(event.to_string()));
    }

    /// A ledger whose hook appends to this thread's log, starting empty.
    fn remembering_ledger() -> WouldRejectLedger {
        LOGGED.with(|l| l.borrow_mut().clear());
        WouldRejectLedger::new(remember)
    }

    fn logged() -> Vec<String> {
        LOGGED.with(|l| l.borrow().clone())
    }

    fn quiet(_: &WouldReject<'_>) {}

    #[test]
    fn shadow_allows_a_failed_check_and_counts_it() {
        let ledger = WouldRejectLedger::new(quiet);
        let out = SHADOW.judge(Err(Why::Stale), &ledger, &"ns/a");
        assert_eq!(out, Ok(Proceed::Shadowed(Why::Stale)));
        assert_eq!(ledger.count("test_gate", "stale"), 1);
        let _ = SHADOW.judge(Err(Why::Stale), &ledger, &"ns/b");
        assert_eq!(ledger.count("test_gate", "stale"), 2);
    }

    #[test]
    fn shadow_logs_what_enforce_would_refuse() {
        let ledger = remembering_ledger();
        let _ = SHADOW.judge(Err(Why::Missing), &ledger, &"ns/pod-1");
        assert_eq!(
            logged(),
            vec!["gate test_gate would refuse ns/pod-1: missing".to_string()]
        );
    }

    #[test]
    fn enforce_refuses_a_failed_check() {
        let ledger = WouldRejectLedger::new(quiet);
        let refused = ENFORCE
            .judge(Err(Why::Missing), &ledger, &"ns/a")
            .expect_err("an enforcing gate must refuse a failed check");
        assert_eq!(refused.gate(), "test_gate");
        assert_eq!(refused.reason(), &Why::Missing);
        assert_eq!(refused.to_string(), "gate test_gate refused: missing");
    }

    #[test]
    fn an_enforced_refusal_is_not_counted_as_a_would_reject() {
        let ledger = remembering_ledger();
        let _ = ENFORCE.judge(Err(Why::Missing), &ledger, &"ns/a");
        assert!(ledger.snapshot().is_empty());
        assert!(logged().is_empty());
    }

    #[test]
    fn a_passing_check_proceeds_and_records_nothing_in_either_mode() {
        let ledger = remembering_ledger();
        assert_eq!(
            SHADOW.judge(Ok::<(), Why>(()), &ledger, &"x"),
            Ok(Proceed::Clean)
        );
        assert_eq!(
            ENFORCE.judge(Ok::<(), Why>(()), &ledger, &"x"),
            Ok(Proceed::Clean)
        );
        assert!(ledger.snapshot().is_empty());
        assert!(logged().is_empty());
    }

    #[test]
    fn series_are_keyed_by_gate_and_reason_and_ordered() {
        let ledger = WouldRejectLedger::new(quiet);
        let other = Gate::new("another_gate", Rollout::Shadow);
        let _ = SHADOW.judge(Err(Why::Stale), &ledger, &"a");
        let _ = SHADOW.judge(Err(Why::Missing), &ledger, &"b");
        let _ = SHADOW.judge(Err(Why::Missing), &ledger, &"c");
        let _ = other.judge(Err(Why::Stale), &ledger, &"d");
        assert_eq!(
            ledger.snapshot(),
            vec![
                WouldRejectCount {
                    gate: "another_gate",
                    reason: "stale",
                    count: 1
                },
                WouldRejectCount {
                    gate: "test_gate",
                    reason: "missing",
                    count: 2
                },
                WouldRejectCount {
                    gate: "test_gate",
                    reason: "stale",
                    count: 1
                },
            ]
        );
    }
}
