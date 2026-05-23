//! The `Sistema` typed value — the top-level `(defsistema …)` form.
//!
//! A `Sistema` declares **the complete system** the cluster runs:
//! every workload (caixa ref), every infra unit feeding it (magma /
//! pangea ref), every promise it carries (viggy promessa ref), the
//! distribution-layer topology shape (revoada fabric face ref).
//!
//! It is the *root* of the typescape — every other typed primitive in
//! the cluster is reachable from a Sistema by name. When the operator
//! edits a `(defsistema …)` form, the change flows through
//! [`crate::Conduit`] → the typed Sistema lands in consensus → controllers
//! reconcile every named sub-primitive in turn.
//!
//! ## Authoring shape (tlisp, future M1.2 wiring)
//!
//! ```text
//! (defsistema "rio-cluster"
//!   :apps     ["podinfo" "lilitu" "tend"]
//!   :infra    [(infraref "rio-network" :backend :magma)
//!              (infraref "rio-dns"     :backend :pangea)]
//!   :promises [(promessaref "sla"      :kind :availability :target 99.99)
//!              (promessaref "cost"     :kind :budget       :target 5000)]
//!   :topology (topologyref "quorum-3m" :nodes 3))
//! ```
//!
//! ## Round-trip discipline
//!
//! `Sistema` implements [`Typescape`] (via the bridge crate). The
//! round-trip property is enforced in `tests/sistema_typescape.rs`:
//! every well-formed `Sistema` can project to `TypescapeValue`, be
//! sui-evaluated (M1.2+), and re-typed back without loss.

use engenho_sui_typescape::{Typescape, TypescapeError, TypescapeValue};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// The top-level system declaration.
///
/// Each field is a *reference* (typed name) to a sub-primitive that
/// a controller reconciles independently. The Sistema itself never
/// stores the sub-primitive's body — it stores the name + enough
/// typing to route the reconcile request. This is the *one-shape*
/// rule: a Sistema is a directory, not an inventory.
// Note: no `Eq` — `PromessaRef::target` is f64, which is not Eq.
// `PartialEq` is the strongest derive available; the rest of the
// type hierarchy mirrors this constraint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sistema {
    /// Cluster name. Stable identity across re-declarations.
    pub name: Arc<str>,
    /// Workload refs — caixa names the cluster runs.
    pub apps: Vec<AppRef>,
    /// Infra refs — magma / pangea declarations the cluster sits on.
    pub infra: Vec<InfraRef>,
    /// Promise refs — viggy promessas the cluster carries.
    pub promises: Vec<PromessaRef>,
    /// Topology ref — revoada fabric face the cluster exposes.
    pub topology: TopologyRef,
}

/// Reference to a caixa workload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppRef {
    /// Caixa name.
    pub name: Arc<str>,
    /// Optional pinned version (Cargo-style semver). When `None`,
    /// the reconciler resolves the latest released version.
    pub version: Option<Arc<str>>,
}

/// Backend an [`InfraRef`] declares against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InfraBackend {
    /// magma — the Rust-native OpenTofu-compatible IaC executor.
    Magma,
    /// pangea — the Ruby DSL producing Terraform JSON.
    Pangea,
    /// crossplane — the K8s-native composer.
    Crossplane,
}

/// Reference to an infra primitive (network, dns, storage, ...).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InfraRef {
    /// Logical name.
    pub name: Arc<str>,
    /// Which substrate renders the actual resources.
    pub backend: InfraBackend,
}

/// Kind of promise — matches the discriminant in viggy's promessa
/// CRD.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromessaKind {
    /// SLA availability promise.
    Availability,
    /// Cost budget promise.
    Budget,
    /// Compliance posture promise.
    Compliance,
    /// Security posture promise.
    Security,
    /// Customer-facing KPI promise.
    CustomerKpi,
}

/// Reference to a viggy promessa.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromessaRef {
    /// Promessa name.
    pub name: Arc<str>,
    /// Promessa kind.
    pub kind: PromessaKind,
    /// Target value (f64 — the substrate's universal numeric promise
    /// target).
    pub target: f64,
}

/// Distribution-layer topology shape. Maps 1:1 to revoada's typed
/// `TopologyStrategy` enum (Solo / Pair / Quorum3M / Cluster3MNW /
/// MeshAllPeers / Phalanx).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologyRef {
    /// Revoada strategy name (`"solo"` | `"pair"` | …).
    pub strategy: Arc<str>,
    /// Node count.
    pub nodes: u32,
}

// ── Typescape impls — round-trippable to/from sui ────────────────

impl Typescape for InfraBackend {
    fn to_typescape_value(&self) -> TypescapeValue {
        TypescapeValue::string(match self {
            Self::Magma => "magma",
            Self::Pangea => "pangea",
            Self::Crossplane => "crossplane",
        })
    }
    fn from_typescape_value(value: &TypescapeValue) -> Result<Self, TypescapeError> {
        match value.as_str()? {
            "magma" => Ok(Self::Magma),
            "pangea" => Ok(Self::Pangea),
            "crossplane" => Ok(Self::Crossplane),
            other => Err(TypescapeError::Invariant {
                location: "InfraBackend".into(),
                reason: format!("unknown backend {other}"),
            }),
        }
    }
}

impl Typescape for PromessaKind {
    fn to_typescape_value(&self) -> TypescapeValue {
        TypescapeValue::string(match self {
            Self::Availability => "availability",
            Self::Budget => "budget",
            Self::Compliance => "compliance",
            Self::Security => "security",
            Self::CustomerKpi => "customer_kpi",
        })
    }
    fn from_typescape_value(value: &TypescapeValue) -> Result<Self, TypescapeError> {
        match value.as_str()? {
            "availability" => Ok(Self::Availability),
            "budget" => Ok(Self::Budget),
            "compliance" => Ok(Self::Compliance),
            "security" => Ok(Self::Security),
            "customer_kpi" => Ok(Self::CustomerKpi),
            other => Err(TypescapeError::Invariant {
                location: "PromessaKind".into(),
                reason: format!("unknown kind {other}"),
            }),
        }
    }
}

impl Typescape for AppRef {
    fn to_typescape_value(&self) -> TypescapeValue {
        TypescapeValue::attrs(vec![
            ("name", TypescapeValue::string(self.name.as_ref())),
            (
                "version",
                self.version.as_ref().map_or(TypescapeValue::null(), |v| {
                    TypescapeValue::string(v.as_ref())
                }),
            ),
        ])
    }
    fn from_typescape_value(value: &TypescapeValue) -> Result<Self, TypescapeError> {
        let attrs = value.as_attrs()?;
        let name: Arc<str> = attrs
            .get("name")
            .ok_or_else(|| TypescapeError::MissingAttr("name".into()))?
            .as_str()?
            .into();
        let version = match attrs.get("version") {
            Some(TypescapeValue::Null) | None => None,
            Some(v) => Some(v.as_str()?.into()),
        };
        Ok(Self { name, version })
    }
}

impl Typescape for InfraRef {
    fn to_typescape_value(&self) -> TypescapeValue {
        TypescapeValue::attrs(vec![
            ("name", TypescapeValue::string(self.name.as_ref())),
            ("backend", self.backend.to_typescape_value()),
        ])
    }
    fn from_typescape_value(value: &TypescapeValue) -> Result<Self, TypescapeError> {
        Ok(Self {
            name: value.attr("name")?.as_str()?.into(),
            backend: InfraBackend::from_typescape_value(value.attr("backend")?)?,
        })
    }
}

impl Typescape for PromessaRef {
    fn to_typescape_value(&self) -> TypescapeValue {
        TypescapeValue::attrs(vec![
            ("name", TypescapeValue::string(self.name.as_ref())),
            ("kind", self.kind.to_typescape_value()),
            ("target", TypescapeValue::float(self.target)),
        ])
    }
    fn from_typescape_value(value: &TypescapeValue) -> Result<Self, TypescapeError> {
        Ok(Self {
            name: value.attr("name")?.as_str()?.into(),
            kind: PromessaKind::from_typescape_value(value.attr("kind")?)?,
            target: value.attr("target")?.as_float()?,
        })
    }
}

impl Typescape for TopologyRef {
    fn to_typescape_value(&self) -> TypescapeValue {
        TypescapeValue::attrs(vec![
            ("strategy", TypescapeValue::string(self.strategy.as_ref())),
            ("nodes", TypescapeValue::int(i64::from(self.nodes))),
        ])
    }
    fn from_typescape_value(value: &TypescapeValue) -> Result<Self, TypescapeError> {
        let nodes_i = value.attr("nodes")?.as_int()?;
        let nodes = u32::try_from(nodes_i).map_err(|_| TypescapeError::Invariant {
            location: "TopologyRef.nodes".into(),
            reason: format!("node count {nodes_i} out of u32 range"),
        })?;
        Ok(Self {
            strategy: value.attr("strategy")?.as_str()?.into(),
            nodes,
        })
    }
}

impl Typescape for Sistema {
    fn to_typescape_value(&self) -> TypescapeValue {
        TypescapeValue::attrs(vec![
            ("name", TypescapeValue::string(self.name.as_ref())),
            ("apps", self.apps.to_typescape_value()),
            ("infra", self.infra.to_typescape_value()),
            ("promises", self.promises.to_typescape_value()),
            ("topology", self.topology.to_typescape_value()),
        ])
    }
    fn from_typescape_value(value: &TypescapeValue) -> Result<Self, TypescapeError> {
        Ok(Self {
            name: value.attr("name")?.as_str()?.into(),
            apps: <Vec<AppRef>>::from_typescape_value(value.attr("apps")?)?,
            infra: <Vec<InfraRef>>::from_typescape_value(value.attr("infra")?)?,
            promises: <Vec<PromessaRef>>::from_typescape_value(value.attr("promises")?)?,
            topology: TopologyRef::from_typescape_value(value.attr("topology")?)?,
        })
    }
}

// `Vec<T>` for our refs needs `T: Typescape` — already covered by
// the foundational impl in engenho-sui-typescape's ext module.
