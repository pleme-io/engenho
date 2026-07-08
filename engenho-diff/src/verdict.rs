//! The typed verdict + divergence taxonomy.
//!
//! [`Verdict::Parity`] is **proof-by-absence**: its payload is a
//! [`ParityWitness`] whose only field is private, so NO code outside this
//! crate can construct a `Parity`. The single way to obtain one is the
//! differ ([`crate::cotejo`]) returning it on an EMPTY divergence set —
//! a fabricated "they agree" has no code path (★★ UNREPRESENTABILITY,
//! the truly-unrepresentable tier for external callers).

use std::fmt::{self, Write as _};

use serde_json::Value;

use crate::observe::HttpMethod;
use crate::path::JsonPath;

/// Which cluster a fact belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Engenho,
    K3s,
}

impl Side {
    /// A stable label.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Engenho => "engenho",
            Self::K3s => "k3s",
        }
    }
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.label())
    }
}

/// An owned Group/Version/Resource triple (the discovery/verb surface
/// works over dynamic strings, unlike the `&'static str`
/// `engenho_types::kind::GroupVersionResource`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Gvr {
    pub group: String,
    pub version: String,
    pub resource: String,
}

impl fmt::Display for Gvr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.group.is_empty() {
            write!(f, "{}/{}", self.version, self.resource)
        } else {
            write!(f, "{}/{}/{}", self.group, self.version, self.resource)
        }
    }
}

/// A Kubernetes API verb.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verb {
    Get,
    List,
    Create,
    Update,
    Patch,
    Delete,
    Watch,
    DeleteCollection,
}

impl Verb {
    /// The upstream verb label as it appears in `APIResource.verbs`.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::List => "list",
            Self::Create => "create",
            Self::Update => "update",
            Self::Patch => "patch",
            Self::Delete => "delete",
            Self::Watch => "watch",
            Self::DeleteCollection => "deletecollection",
        }
    }
}

impl fmt::Display for Verb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.label())
    }
}

/// A validation `Status.details.causes[]` entry — for shape-comparing how
/// each side rejects an invalid object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatusCause {
    pub field: String,
    pub reason: String,
    pub message: String,
}

/// Whether a divergence is a hard (unmasked) mismatch or a masked one
/// re-surfaced in strict mode so the mask stays auditable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Severity {
    /// Survives normalization — a real conformance gap.
    Hard,
    /// Suppressed by a mask (legitimately-varying field). Surfaced only
    /// in strict mode.
    Cosmetic,
}

impl Severity {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Hard => "HARD",
            Self::Cosmetic => "cosmetic",
        }
    }
}

/// One typed way engenho's surface diverges from k3s. Each arm is
/// attributed to an owning engenho crate via [`Divergence::owning_crate`].
#[derive(Clone, Debug, PartialEq)]
pub enum Divergence {
    /// Same operation, different HTTP status code.
    StatusCodeDiff {
        op: String,
        method: HttpMethod,
        engenho: u16,
        k3s: u16,
    },
    /// A resource present on one side and absent (404) on the other.
    MissingResource { path: JsonPath, present_on: Side },
    /// A field present on both but with different values.
    FieldDiff {
        path: JsonPath,
        engenho: Value,
        k3s: Value,
        severity: Severity,
    },
    /// A field k3s populates by default that engenho leaves unset.
    MissingDefault {
        path: JsonPath,
        k3s_value: Value,
        severity: Severity,
    },
    /// A field engenho adds that k3s does not carry.
    ExtraField {
        path: JsonPath,
        side: Side,
        value: Value,
        severity: Severity,
    },
    /// A verb one side supports on a resource and the other refuses.
    MissingVerb {
        gvr: Gvr,
        verb: Verb,
        present_on: Side,
    },
    /// A subresource (`/status`, `/scale`, `/log`) one side exposes.
    MissingSubresource {
        gvr: Gvr,
        sub: String,
        present_on: Side,
    },
    /// The two sides reject an invalid object with different cause shapes.
    ValidationShapeDiff {
        engenho: StatusCause,
        k3s: StatusCause,
    },
    /// The discovery surface (`/api/v1`, `/apis/...`) differs.
    DiscoveryDiff {
        group: String,
        version: String,
        detail: String,
    },
    /// A watch stream emitted a differing event at `index`.
    WatchEventDiff { index: usize, detail: String },
}

impl Divergence {
    /// The severity of this divergence (arms without an explicit severity
    /// slot are always hard).
    #[must_use]
    pub fn severity(&self) -> Severity {
        match self {
            Self::FieldDiff { severity, .. }
            | Self::MissingDefault { severity, .. }
            | Self::ExtraField { severity, .. } => *severity,
            _ => Severity::Hard,
        }
    }

    /// The engenho crate that owns the fix for this divergence — the
    /// D-matrix attribution. Lets the report route each gap to its owner
    /// mechanically instead of by prose.
    #[must_use]
    pub fn owning_crate(&self) -> &'static str {
        match self {
            // A refused verb / status mismatch is the router + handler
            // wiring (and the store command it dispatches).
            Self::StatusCodeDiff { .. } | Self::MissingVerb { .. } => "engenho-apiserver",
            // A resource missing on engenho's side is either an
            // uncataloged kind (apiserver) or an un-run controller that
            // would have auto-created it.
            Self::MissingResource { .. } => "engenho-apiserver / engenho-controllers",
            // Server-defaulted fields (namespace finalizers, auto-labels,
            // status seeding) are stamped by the create handler / a
            // controller.
            Self::MissingDefault { .. } => {
                "engenho-apiserver (handler defaulting) / engenho-controllers"
            }
            // An extra field engenho adds is the handler's stamping path.
            Self::ExtraField { .. } => "engenho-apiserver (handler stamping)",
            // A value mismatch on a shared field is the store round-trip /
            // handler.
            Self::FieldDiff { .. } => "engenho-apiserver / engenho-store",
            // Subresource wiring is the router.
            Self::MissingSubresource { .. } => "engenho-apiserver (router subresources)",
            // Validation shape is admission.
            Self::ValidationShapeDiff { .. } => "engenho-apiserver (admission/validation)",
            // Discovery is the discovery module.
            Self::DiscoveryDiff { .. } => "engenho-apiserver (discovery.rs)",
            // Watch framing is the store watch + router.
            Self::WatchEventDiff { .. } => "engenho-store (watch) / engenho-apiserver",
        }
    }

    /// A stable signature — the divergence's *location* + *kind*,
    /// independent of the concrete values or severity. This is the ratchet
    /// key: a baseline records signatures, and the test fails only when a
    /// signature NOT in the baseline appears. Built via `write!` (the
    /// sanctioned emission macro), never `format!()` of free text.
    #[must_use]
    pub fn signature(&self) -> String {
        let mut s = String::new();
        // `write!` into a String is infallible for these arms.
        let _ = match self {
            Self::StatusCodeDiff { op, method, .. } => {
                write!(s, "StatusCodeDiff:{op}:{}", method.label())
            }
            Self::MissingResource { path, present_on } => {
                write!(s, "MissingResource:{path}:{present_on}")
            }
            Self::FieldDiff { path, .. } => write!(s, "FieldDiff:{path}"),
            Self::MissingDefault { path, .. } => write!(s, "MissingDefault:{path}"),
            Self::ExtraField { path, side, .. } => write!(s, "ExtraField:{path}:{side}"),
            Self::MissingVerb {
                gvr,
                verb,
                present_on,
            } => write!(s, "MissingVerb:{gvr}:{verb}:{present_on}"),
            Self::MissingSubresource {
                gvr,
                sub,
                present_on,
            } => write!(s, "MissingSubresource:{gvr}:{sub}:{present_on}"),
            Self::ValidationShapeDiff { .. } => write!(s, "ValidationShapeDiff"),
            Self::DiscoveryDiff {
                group,
                version,
                detail,
            } => write!(s, "DiscoveryDiff:{group}/{version}:{detail}"),
            Self::WatchEventDiff { index, .. } => write!(s, "WatchEventDiff:{index}"),
        };
        s
    }
}

impl fmt::Display for Divergence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sev = self.severity().label();
        match self {
            Self::StatusCodeDiff {
                op,
                method,
                engenho,
                k3s,
            } => write!(
                f,
                "[{sev}] StatusCodeDiff {op} ({}): engenho={engenho} k3s={k3s}",
                method.label()
            ),
            Self::MissingResource { path, present_on } => {
                write!(
                    f,
                    "[{sev}] MissingResource {path}: present only on {present_on}"
                )
            }
            Self::FieldDiff {
                path, engenho, k3s, ..
            } => write!(f, "[{sev}] FieldDiff {path}: engenho={engenho} k3s={k3s}"),
            Self::MissingDefault {
                path, k3s_value, ..
            } => write!(
                f,
                "[{sev}] MissingDefault {path}: k3s sets {k3s_value}, engenho unset"
            ),
            Self::ExtraField {
                path, side, value, ..
            } => write!(f, "[{sev}] ExtraField {path}: {side} adds {value}"),
            Self::MissingVerb {
                gvr,
                verb,
                present_on,
            } => write!(
                f,
                "[{sev}] MissingVerb {gvr} verb={verb}: present only on {present_on}"
            ),
            Self::MissingSubresource {
                gvr,
                sub,
                present_on,
            } => write!(
                f,
                "[{sev}] MissingSubresource {gvr}/{sub}: present only on {present_on}"
            ),
            Self::ValidationShapeDiff { engenho, k3s } => write!(
                f,
                "[{sev}] ValidationShapeDiff: engenho={engenho:?} k3s={k3s:?}"
            ),
            Self::DiscoveryDiff {
                group,
                version,
                detail,
            } => write!(f, "[{sev}] DiscoveryDiff {group}/{version}: {detail}"),
            Self::WatchEventDiff { index, detail } => {
                write!(f, "[{sev}] WatchEventDiff #{index}: {detail}")
            }
        }
    }
}

/// Proof-by-absence witness for [`Verdict::Parity`]. The `()` field is
/// private, so a `Parity` cannot be constructed outside this crate — the
/// only producer is [`crate::cotejo`] on an empty divergence set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParityWitness(());

impl ParityWitness {
    /// Crate-internal constructor — the differ's sole path to Parity.
    pub(crate) fn new() -> Self {
        Self(())
    }
}

/// The result of diffing one operation across both targets.
#[derive(Clone, Debug, PartialEq)]
pub enum Verdict {
    /// The two sides agreed (no divergences). Proof-by-absence.
    Parity(ParityWitness),
    /// The two sides diverged in the listed ways.
    Divergent(Vec<Divergence>),
    /// The k3s reference oracle could not be reached — fail loud, never
    /// a silent skip.
    ReferenceUnreachable,
}

impl Verdict {
    /// `true` iff this is a `Parity`.
    #[must_use]
    pub fn is_parity(&self) -> bool {
        matches!(self, Self::Parity(_))
    }

    /// The divergences, or an empty slice for `Parity` / `ReferenceUnreachable`.
    #[must_use]
    pub fn divergences(&self) -> &[Divergence] {
        match self {
            Self::Divergent(d) => d,
            _ => &[],
        }
    }
}
