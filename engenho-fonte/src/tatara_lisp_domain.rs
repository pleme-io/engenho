//! `with-tatara-lisp` — Sistema as a TataraDomain keyword.
//!
//! Registers Sistema with tatara-lisp's typed authoring surface:
//!
//! ```text
//! (defsistema "rio-cluster"
//!   :apps     [(appref "podinfo" :version "6.4.1")]
//!   :infra    [(inframagma "rio-net")]
//!   :promises [(promessaref "sla" :kind :availability :target 99.99)]
//!   :topology (topology "quorum-3m" :nodes 3))
//! ```
//!
//! Hand-rolled `impl TataraDomain for Sistema` rather than the
//! `#[derive(TataraDomain)]` macro — the schema has nested forms
//! (list-of-records for apps/infra/promises) that the derive macro
//! handles for flat structs but not for typed enum slugs +
//! per-record kwarg shapes. The hand-roll is ~80 LoC; the macro
//! shape is the future path once tatara-lisp-derive supports
//! nested keyword-forms.
//!
//! Gated `with-tatara-lisp`.

use crate::{AppRef, InfraBackend, InfraRef, PromessaKind, PromessaRef, Sistema, TopologyRef};
use std::sync::Arc;
use tatara_lisp::domain::{kwarg_form, parse_kwargs, reject_unknown_kwargs, required};
use tatara_lisp::{LispError, Sexp, TataraDomain, read};
type LispResult<T> = std::result::Result<T, LispError>;

/// `LispError::Compile { form, message }` expects `form: String` but
/// `kwarg_form()` returns a typed `KwargPath`. This thin shim renders
/// the typed path to its display form before handing it to Compile.
fn kwarg_form_string(key: &str) -> String {
    kwarg_form(key).to_string()
}

impl TataraDomain for Sistema {
    const KEYWORD: &'static str = "defsistema";

    fn compile_from_args(args: &[Sexp]) -> LispResult<Self> {
        // First positional arg: the cluster name (string).
        let name_sexp = args.first().ok_or_else(|| LispError::Compile {
            form: kwarg_form_string("name"),
            message: "(defsistema) requires a name string as the first argument".into(),
        })?;
        let name = name_sexp.as_string().ok_or_else(|| LispError::Compile {
            form: kwarg_form_string("name"),
            message: format!("(defsistema) name must be a string, got: {name_sexp}"),
        })?;
        let name: Arc<str> = Arc::from(name);

        // Remaining args: keyword-value pairs.
        let kw = parse_kwargs(&args[1..])?;
        reject_unknown_kwargs(&kw, &["apps", "infra", "promises", "topology"])?;

        let apps = match kw.get("apps") {
            Some(sexp) => parse_app_list(sexp)?,
            None => Vec::new(),
        };
        let infra = match kw.get("infra") {
            Some(sexp) => parse_infra_list(sexp)?,
            None => Vec::new(),
        };
        let promises = match kw.get("promises") {
            Some(sexp) => parse_promessa_list(sexp)?,
            None => Vec::new(),
        };
        let topology_sexp = required(&kw, "topology")?;
        let topology = parse_topology(topology_sexp)?;

        Ok(Sistema {
            name,
            apps,
            infra,
            promises,
            topology,
        })
    }
}

fn parse_app_list(sexp: &Sexp) -> LispResult<Vec<AppRef>> {
    let list = sexp.as_list().ok_or_else(|| LispError::Compile {
        form: kwarg_form_string("apps"),
        message: format!(":apps must be a list, got: {sexp}"),
    })?;
    list.iter().map(parse_app_ref).collect()
}

fn parse_app_ref(sexp: &Sexp) -> LispResult<AppRef> {
    let list = sexp.as_list().ok_or_else(|| LispError::Compile {
        form: kwarg_form_string("apps"),
        message: format!("appref must be a list, got: {sexp}"),
    })?;
    if list.first().and_then(Sexp::as_symbol) != Some("appref") {
        return Err(LispError::Compile {
            form: kwarg_form_string("apps"),
            message: format!("expected (appref ...), got: {sexp}"),
        });
    }
    let name_sexp = list.get(1).ok_or_else(|| LispError::Compile {
        form: kwarg_form_string("apps"),
        message: "(appref ...) requires a name string".into(),
    })?;
    let name: Arc<str> = Arc::from(name_sexp.as_string().ok_or_else(|| LispError::Compile {
        form: kwarg_form_string("apps"),
        message: format!("appref name must be a string, got: {name_sexp}"),
    })?);
    let kw = parse_kwargs(&list[2..])?;
    reject_unknown_kwargs(&kw, &["version"])?;
    let version = kw
        .get("version")
        .and_then(|s| s.as_string().map(|s| Arc::from(s)));
    Ok(AppRef { name, version })
}

fn parse_infra_list(sexp: &Sexp) -> LispResult<Vec<InfraRef>> {
    let list = sexp.as_list().ok_or_else(|| LispError::Compile {
        form: kwarg_form_string("infra"),
        message: format!(":infra must be a list, got: {sexp}"),
    })?;
    list.iter().map(parse_infra_ref).collect()
}

fn parse_infra_ref(sexp: &Sexp) -> LispResult<InfraRef> {
    let list = sexp.as_list().ok_or_else(|| LispError::Compile {
        form: kwarg_form_string("infra"),
        message: format!("inframagma/infrapangea/infracrossplane must be a list, got: {sexp}"),
    })?;
    let head = list
        .first()
        .and_then(Sexp::as_symbol)
        .ok_or_else(|| LispError::Compile {
            form: kwarg_form_string("infra"),
            message: format!("infra head must be a symbol, got: {sexp}"),
        })?;
    let backend = match head {
        "inframagma" => InfraBackend::Magma,
        "infrapangea" => InfraBackend::Pangea,
        "infracrossplane" => InfraBackend::Crossplane,
        other => {
            return Err(LispError::Compile {
                form: kwarg_form_string("infra"),
                message: format!(
                    "infra head must be inframagma/infrapangea/infracrossplane, got: {other}"
                ),
            });
        }
    };
    let name_sexp = list.get(1).ok_or_else(|| LispError::Compile {
        form: kwarg_form_string("infra"),
        message: format!("({head} ...) requires a name string").into(),
    })?;
    let name: Arc<str> = Arc::from(name_sexp.as_string().ok_or_else(|| LispError::Compile {
        form: kwarg_form_string("infra"),
        message: format!("infra name must be a string, got: {name_sexp}"),
    })?);
    Ok(InfraRef { name, backend })
}

fn parse_promessa_list(sexp: &Sexp) -> LispResult<Vec<PromessaRef>> {
    let list = sexp.as_list().ok_or_else(|| LispError::Compile {
        form: kwarg_form_string("promises"),
        message: format!(":promises must be a list, got: {sexp}"),
    })?;
    list.iter().map(parse_promessa_ref).collect()
}

fn parse_promessa_ref(sexp: &Sexp) -> LispResult<PromessaRef> {
    let list = sexp.as_list().ok_or_else(|| LispError::Compile {
        form: kwarg_form_string("promises"),
        message: format!("promessaref must be a list, got: {sexp}"),
    })?;
    if list.first().and_then(Sexp::as_symbol) != Some("promessaref") {
        return Err(LispError::Compile {
            form: kwarg_form_string("promises"),
            message: format!("expected (promessaref ...), got: {sexp}"),
        });
    }
    let name_sexp = list.get(1).ok_or_else(|| LispError::Compile {
        form: kwarg_form_string("promises"),
        message: "(promessaref ...) requires a name string".into(),
    })?;
    let name: Arc<str> = Arc::from(name_sexp.as_string().ok_or_else(|| LispError::Compile {
        form: kwarg_form_string("promises"),
        message: format!("promessaref name must be a string, got: {name_sexp}"),
    })?);
    let kw = parse_kwargs(&list[2..])?;
    reject_unknown_kwargs(&kw, &["kind", "target"])?;
    let kind_sexp = required(&kw, "kind")?;
    let kind_slug = kind_sexp.as_keyword().ok_or_else(|| LispError::Compile {
        form: kwarg_form_string("promises"),
        message: format!(":kind must be a keyword, got: {kind_sexp}"),
    })?;
    let kind = match kind_slug {
        "availability" => PromessaKind::Availability,
        "budget" => PromessaKind::Budget,
        "compliance" => PromessaKind::Compliance,
        "security" => PromessaKind::Security,
        "customer-kpi" => PromessaKind::CustomerKpi,
        other => {
            return Err(LispError::Compile {
                form: kwarg_form_string("promises"),
                message: format!(
                    "unknown promessa kind :{other} (allowed: availability, budget, compliance, security, customer-kpi)"
                ),
            });
        }
    };
    let target_sexp = required(&kw, "target")?;
    let target = target_sexp
        .as_float()
        .or_else(|| target_sexp.as_int().map(|i| i as f64))
        .ok_or_else(|| LispError::Compile {
            form: kwarg_form_string("promises"),
            message: format!(":target must be a number, got: {target_sexp}"),
        })?;
    Ok(PromessaRef { name, kind, target })
}

fn parse_topology(sexp: &Sexp) -> LispResult<TopologyRef> {
    let list = sexp.as_list().ok_or_else(|| LispError::Compile {
        form: kwarg_form_string("topology"),
        message: format!("topology must be a list, got: {sexp}"),
    })?;
    if list.first().and_then(Sexp::as_symbol) != Some("topology") {
        return Err(LispError::Compile {
            form: kwarg_form_string("topology"),
            message: format!("expected (topology ...), got: {sexp}"),
        });
    }
    let strategy_sexp = list.get(1).ok_or_else(|| LispError::Compile {
        form: kwarg_form_string("topology"),
        message: "(topology ...) requires a strategy string".into(),
    })?;
    let strategy: Arc<str> =
        Arc::from(
            strategy_sexp
                .as_string()
                .ok_or_else(|| LispError::Compile {
                    form: kwarg_form_string("topology"),
                    message: format!("topology strategy must be a string, got: {strategy_sexp}"),
                })?,
        );
    let kw = parse_kwargs(&list[2..])?;
    reject_unknown_kwargs(&kw, &["nodes"])?;
    let nodes_sexp = required(&kw, "nodes")?;
    let nodes_i = nodes_sexp.as_int().ok_or_else(|| LispError::Compile {
        form: kwarg_form_string("topology"),
        message: format!(":nodes must be an int, got: {nodes_sexp}"),
    })?;
    let nodes = u32::try_from(nodes_i).map_err(|_| LispError::Compile {
        form: kwarg_form_string("topology"),
        message: format!(":nodes must fit in u32, got: {nodes_i}"),
    })?;
    Ok(TopologyRef { strategy, nodes })
}

/// Read + parse a tlisp source string into a Sistema.
///
/// # Errors
///
/// - `tatara_lisp::LispError::Reader` if the source isn't well-formed
///   Lisp.
/// - `LispError::Compile` if the (defsistema …) shape
///   doesn't match Sistema's expected schema.
pub fn parse_tlisp(src: &str) -> LispResult<Sistema> {
    let forms = tatara_lisp::read(src)?;
    let form = forms.first().ok_or_else(|| LispError::Compile {
        form: kwarg_form_string("source"),
        message: "no forms found in source".into(),
    })?;
    Sistema::compile_from_sexp(form)
}
