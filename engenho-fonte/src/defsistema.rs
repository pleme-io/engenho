//! `defsistema` authoring surface — operator-facing constructors
//! + JSON serde helpers for the typed `Sistema` value.
//!
//! Full tatara-lisp `(defsistema …)` keyword registration via
//! `#[derive(TataraDomain)]` is the eventual destination — but
//! requires a cross-workspace tatara-lisp dep + macro crate
//! (substantial closure pull-in). This module ships the
//! always-on operator-facing API that exercises the same shape:
//!
//!   * [`parse_json`] — typed Sistema from a JSON declaration
//!   * [`parse_nix`] — typed Sistema from a Nix declaration
//!     (gated `with-sui-eval`, currently blocked on sui-bytecode
//!     upstream; ready when that lands)
//!   * [`SistemaBuilder`] — typed fluent builder for in-Rust
//!     construction; the same shape a `#[derive(TataraDomain)]`
//!     macro will emit once wired
//!   * [`to_authoring_form`] — emit the canonical
//!     `(defsistema "name" :apps [...] :infra [...] …)` lisp text
//!     for round-trip diagnostics + dry-run rendering
//!
//! ## Shape parity with the future `(defsistema)` keyword
//!
//! Once tatara-lisp is wired, the form is:
//!
//! ```text
//! (defsistema "rio-cluster"
//!   :apps     [(appref "podinfo" :version "6.4.1")]
//!   :infra    [(inframagma "rio-net")]
//!   :promises [(promessaref "sla" :kind :availability :target 99.99)]
//!   :topology (topology "quorum-3m" :nodes 3))
//! ```
//!
//! [`to_authoring_form`] emits exactly this text; the parsers
//! consume the same shape via JSON/Nix bridges. When the macro
//! lands, all three converge.

use crate::{
    AppRef, FonteError, FonteResult, InfraBackend, InfraRef, PromessaKind, PromessaRef, Sistema,
    TopologyRef,
};
use engenho_sui_typescape::{Typescape, TypescapeValue};
use std::fmt::Write;
use std::sync::Arc;

fn json_to_typescape(j: serde_json::Value) -> TypescapeValue {
    use serde_json::Value as J;
    match j {
        J::Null => TypescapeValue::null(),
        J::Bool(b) => TypescapeValue::bool(b),
        J::Number(n) => {
            if let Some(i) = n.as_i64() {
                TypescapeValue::int(i)
            } else if let Some(f) = n.as_f64() {
                TypescapeValue::float(f)
            } else {
                TypescapeValue::null()
            }
        }
        J::String(s) => TypescapeValue::string(s.as_str()),
        J::Array(a) => TypescapeValue::list(a.into_iter().map(json_to_typescape)),
        J::Object(m) => {
            TypescapeValue::attrs(m.into_iter().map(|(k, v)| (k, json_to_typescape(v))))
        }
    }
}

/// Parse a Sistema from a JSON source.
///
/// # Errors
///
/// - [`FonteError::Eval`] when the JSON is malformed or shape-mismatched.
pub fn parse_json(json: &str) -> FonteResult<Sistema> {
    let raw: serde_json::Value = serde_json::from_str(json).map_err(|e| {
        FonteError::Eval(engenho_sui_typescape::TypescapeError::Invariant {
            location: "defsistema::parse_json".into(),
            reason: format!("invalid JSON: {e}"),
        })
    })?;
    let typed = json_to_typescape(raw);
    Sistema::from_typescape_value(&typed).map_err(FonteError::Eval)
}

/// Parse a Sistema from a Nix source via sui-typescape. Gated
/// `with-sui-eval`.
///
/// # Errors
///
/// - [`FonteError::Eval`] when the Nix is malformed or shape-mismatched.
#[cfg(feature = "with-sui-eval")]
pub fn parse_nix(nix: &str) -> FonteResult<Sistema> {
    let typed = engenho_sui_typescape::eval_nix_str(nix).map_err(FonteError::Eval)?;
    Sistema::from_typescape_value(&typed).map_err(FonteError::Eval)
}

/// Fluent typed builder for in-Rust Sistema construction. Mirrors
/// the shape a `#[derive(TataraDomain)]` macro will emit once
/// tatara-lisp is wired into this crate's deps.
#[derive(Debug, Clone)]
pub struct SistemaBuilder {
    sistema: Sistema,
}

impl SistemaBuilder {
    /// Start a new Sistema with the given name + default empty
    /// apps/infra/promises + Solo(1) topology.
    #[must_use]
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        Self {
            sistema: Sistema {
                name: name.into(),
                apps: Vec::new(),
                infra: Vec::new(),
                promises: Vec::new(),
                topology: TopologyRef {
                    strategy: "solo".into(),
                    nodes: 1,
                },
            },
        }
    }

    /// Append an app reference.
    #[must_use]
    pub fn app(mut self, name: impl Into<Arc<str>>, version: Option<impl Into<Arc<str>>>) -> Self {
        self.sistema.apps.push(AppRef {
            name: name.into(),
            version: version.map(Into::into),
        });
        self
    }

    /// Append an infra reference.
    #[must_use]
    pub fn infra(mut self, name: impl Into<Arc<str>>, backend: InfraBackend) -> Self {
        self.sistema.infra.push(InfraRef {
            name: name.into(),
            backend,
        });
        self
    }

    /// Append a promessa reference.
    #[must_use]
    pub fn promessa(mut self, name: impl Into<Arc<str>>, kind: PromessaKind, target: f64) -> Self {
        self.sistema.promises.push(PromessaRef {
            name: name.into(),
            kind,
            target,
        });
        self
    }

    /// Set the topology shape (strategy name + node count).
    #[must_use]
    pub fn topology(mut self, strategy: impl Into<Arc<str>>, nodes: u32) -> Self {
        self.sistema.topology = TopologyRef {
            strategy: strategy.into(),
            nodes,
        };
        self
    }

    /// Finalize into the typed Sistema value.
    #[must_use]
    pub fn build(self) -> Sistema {
        self.sistema
    }
}

/// Render a typed Sistema into the canonical `(defsistema …)`
/// lisp authoring form. Operators read this for diagnostics + dry-
/// run rendering; round-trips through `parse_*` when the tlisp
/// reader lands.
#[must_use]
pub fn to_authoring_form(sistema: &Sistema) -> String {
    let mut out = String::new();
    let _ = write!(&mut out, "(defsistema {:?}", sistema.name.as_ref());

    if !sistema.apps.is_empty() {
        out.push_str("\n  :apps     (");
        for (i, app) in sistema.apps.iter().enumerate() {
            if i > 0 {
                out.push(' ');
            }
            match &app.version {
                Some(v) => {
                    let _ = write!(
                        &mut out,
                        "(appref {:?} :version {:?})",
                        app.name.as_ref(),
                        v.as_ref()
                    );
                }
                None => {
                    let _ = write!(&mut out, "(appref {:?})", app.name.as_ref());
                }
            }
        }
        out.push(')');
    }

    if !sistema.infra.is_empty() {
        out.push_str("\n  :infra    (");
        for (i, inf) in sistema.infra.iter().enumerate() {
            if i > 0 {
                out.push(' ');
            }
            let backend_slug = match inf.backend {
                InfraBackend::Magma => "inframagma",
                InfraBackend::Pangea => "infrapangea",
                InfraBackend::Crossplane => "infracrossplane",
            };
            let _ = write!(&mut out, "({} {:?})", backend_slug, inf.name.as_ref());
        }
        out.push(')');
    }

    if !sistema.promises.is_empty() {
        out.push_str("\n  :promises (");
        for (i, p) in sistema.promises.iter().enumerate() {
            if i > 0 {
                out.push(' ');
            }
            let kind_slug = match p.kind {
                PromessaKind::Availability => "availability",
                PromessaKind::Budget => "budget",
                PromessaKind::Compliance => "compliance",
                PromessaKind::Security => "security",
                PromessaKind::CustomerKpi => "customer-kpi",
            };
            let _ = write!(
                &mut out,
                "(promessaref {:?} :kind :{} :target {})",
                p.name.as_ref(),
                kind_slug,
                p.target
            );
        }
        out.push(')');
    }

    let _ = write!(
        &mut out,
        "\n  :topology (topology {:?} :nodes {}))\n",
        sistema.topology.strategy.as_ref(),
        sistema.topology.nodes
    );

    out
}
