use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, anyhow, bail, ensure};
use blue_lang_syntax::doc::{DOC_HEAD, MODULEDOC_HEAD};
use blue_lang_syntax::record::{LIST_TYPE, LOWERED_RECORD};
use blue_lang_syntax::{Atom, LOWERED_MAP, Sexp};

use crate::catalog::{KIND_CATALOG, KindEntry};
use crate::emit_typed::{SchemaView, snake_case};
use crate::types::{RustType, map_schema, type_name_from_ref};

pub const BLUE_KINDS: &[(&str, &str)] = &[
    ("", "ConfigMap"),
    ("", "Secret"),
    ("", "Service"),
    ("", "ServiceAccount"),
    ("apps", "Deployment"),
    ("networking.k8s.io", "NetworkPolicy"),
    ("rbac.authorization.k8s.io", "Role"),
    ("rbac.authorization.k8s.io", "RoleBinding"),
];

pub const BLUE_SHAPES: &[&str] = &[
    "io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta",
    "io.k8s.apimachinery.pkg.apis.meta.v1.OwnerReference",
    "io.k8s.apimachinery.pkg.apis.meta.v1.LabelSelector",
    "io.k8s.apimachinery.pkg.apis.meta.v1.LabelSelectorRequirement",
    "io.k8s.api.core.v1.LocalObjectReference",
    "io.k8s.api.core.v1.ObjectReference",
    "io.k8s.api.core.v1.ServiceSpec",
    "io.k8s.api.core.v1.ServicePort",
    "io.k8s.api.core.v1.PodTemplateSpec",
    "io.k8s.api.core.v1.PodSpec",
    "io.k8s.api.core.v1.Container",
    "io.k8s.api.core.v1.ContainerPort",
    "io.k8s.api.core.v1.EnvVar",
    "io.k8s.api.core.v1.VolumeMount",
    "io.k8s.api.core.v1.ResourceRequirements",
    "io.k8s.api.apps.v1.DeploymentSpec",
    "io.k8s.api.apps.v1.DeploymentStrategy",
    "io.k8s.api.apps.v1.RollingUpdateDeployment",
    "io.k8s.api.networking.v1.NetworkPolicySpec",
    "io.k8s.api.networking.v1.NetworkPolicyIngressRule",
    "io.k8s.api.networking.v1.NetworkPolicyEgressRule",
    "io.k8s.api.networking.v1.NetworkPolicyPeer",
    "io.k8s.api.networking.v1.NetworkPolicyPort",
    "io.k8s.api.networking.v1.IPBlock",
    "io.k8s.api.rbac.v1.PolicyRule",
    "io.k8s.api.rbac.v1.RoleRef",
    "io.k8s.api.rbac.v1.Subject",
];

const TEXT_SCALARS: &[&str] = &[
    "io.k8s.apimachinery.pkg.api.resource.Quantity",
    "io.k8s.apimachinery.pkg.apis.meta.v1.Time",
    "io.k8s.apimachinery.pkg.apis.meta.v1.MicroTime",
];

const UNTYPED_SCALARS: &[&str] = &[
    "io.k8s.apimachinery.pkg.util.intstr.IntOrString",
    "io.k8s.apimachinery.pkg.runtime.RawExtension",
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlueType {
    Value(&'static str),
    Record(String),
    Object,
    List(Box<BlueType>),
    Any,
}

impl BlueType {
    fn form(&self) -> Option<Sexp> {
        match self {
            BlueType::Value(v) => Some(sym(v)),
            BlueType::Record(r) => Some(sym(r)),
            BlueType::Object => Some(sym("Map")),
            BlueType::List(inner) => Some(Sexp::List(vec![sym(LIST_TYPE), inner.form()?])),
            BlueType::Any => None,
        }
    }

    fn unset(&self) -> Sexp {
        match self {
            BlueType::List(_) => Sexp::List(vec![sym("list")]),
            BlueType::Value("Map") => Sexp::List(vec![sym(LOWERED_MAP)]),
            _ => Sexp::Nil,
        }
    }

    fn sample(&self) -> Option<Sexp> {
        match self {
            BlueType::Value("Str") => Some(text("x")),
            BlueType::Value("Int") => Some(Sexp::Atom(Atom::Int(1))),
            BlueType::Value("Bool") => Some(Sexp::Atom(Atom::Bool(true))),
            BlueType::Value("Float") => Some(Sexp::Atom(Atom::Float(1.0))),
            BlueType::Value(_) | BlueType::Object => Some(map([])),
            BlueType::List(_) => Some(Sexp::List(vec![sym("list")])),
            BlueType::Record(_) | BlueType::Any => None,
        }
    }

    fn holds_empty(&self) -> bool {
        matches!(self, BlueType::List(_) | BlueType::Value("Map"))
    }

    fn records(&self, out: &mut BTreeSet<String>) {
        match self {
            BlueType::Record(r) => {
                out.insert(r.clone());
            }
            BlueType::List(inner) => inner.records(out),
            BlueType::Value(_) | BlueType::Object | BlueType::Any => {}
        }
    }
}

#[derive(Clone, Debug)]
pub struct BlueField {
    pub name: String,
    pub wire: String,
    pub ty: BlueType,
    pub required: bool,
}

impl BlueField {
    fn declared_type(&self) -> Option<Sexp> {
        if self.required || self.ty.holds_empty() {
            self.ty.form()
        } else {
            None
        }
    }
}

#[derive(Clone)]
pub struct BlueRecord {
    pub name: String,
    pub key: String,
    pub description: String,
    pub fields: Vec<BlueField>,
    pub kind: Option<&'static KindEntry>,
}

#[must_use]
pub fn record_name(type_name: &str) -> String {
    snake_case(type_name)
}

fn blue_type(
    rust: &RustType,
    refs: &BTreeSet<String>,
    selected: &BTreeMap<String, String>,
) -> BlueType {
    match rust {
        RustType::Scalar("String") => BlueType::Value("Str"),
        RustType::Scalar("bool") => BlueType::Value("Bool"),
        RustType::Scalar("f64") => BlueType::Value("Float"),
        RustType::Scalar(_) => BlueType::Value("Int"),
        RustType::Ref(name) => {
            let key = refs
                .iter()
                .find(|k| type_name_from_ref(k) == *name)
                .cloned()
                .unwrap_or_default();
            if let Some(record) = selected.get(&key) {
                BlueType::Record(record.clone())
            } else if TEXT_SCALARS.contains(&key.as_str()) {
                BlueType::Value("Str")
            } else if UNTYPED_SCALARS.contains(&key.as_str()) {
                BlueType::Any
            } else {
                BlueType::Object
            }
        }
        RustType::Vec(inner) => match blue_type(inner, refs, selected) {
            BlueType::Any => BlueType::Any,
            t => BlueType::List(Box::new(t)),
        },
        RustType::Map(_) => BlueType::Value("Map"),
        RustType::Json => BlueType::Any,
    }
}

fn fields_of(
    shape: &SchemaView,
    envelope: bool,
    selected: &BTreeMap<String, String>,
) -> Vec<BlueField> {
    shape
        .properties
        .iter()
        .filter(|(wire, _)| !(envelope && matches!(wire.as_str(), "apiVersion" | "kind")))
        .map(|(wire, schema)| {
            let mut refs = BTreeSet::new();
            let rust = map_schema(schema, &mut refs);
            BlueField {
                name: snake_case(wire),
                wire: wire.clone(),
                ty: blue_type(&rust, &refs, selected),
                required: shape.required.iter().any(|r| r == wire),
            }
        })
        .collect()
}

fn kind_entry(group: &str, kind: &str) -> Result<&'static KindEntry> {
    KIND_CATALOG
        .iter()
        .find(|e| e.group == group && e.kind == kind && !e.opaque)
        .ok_or_else(|| anyhow!("{group}/{kind} is not a schema-backed KIND_CATALOG row"))
}

pub fn blue_records(schemas: &BTreeMap<String, SchemaView>) -> Result<Vec<BlueRecord>> {
    let mut wanted: Vec<(String, Option<&'static KindEntry>)> = Vec::new();
    for (group, kind) in BLUE_KINDS {
        let entry = kind_entry(group, kind)?;
        wanted.push((entry.openapi_key.to_string(), Some(entry)));
    }
    wanted.extend(BLUE_SHAPES.iter().map(|k| ((*k).to_string(), None)));

    let mut selected: BTreeMap<String, String> = BTreeMap::new();
    for (key, _) in &wanted {
        let name = record_name(&type_name_from_ref(key));
        ensure!(
            !selected.values().any(|n| *n == name),
            "two selected schemas render as the record {name}"
        );
        selected.insert(key.clone(), name);
    }

    let mut records = Vec::new();
    for (key, kind) in wanted {
        let shape = schemas
            .get(&key)
            .ok_or_else(|| anyhow!("{key} is not in the vendored OpenAPI schemas"))?;
        records.push(BlueRecord {
            name: selected[&key].clone(),
            key: key.clone(),
            description: shape.description.clone(),
            fields: fields_of(shape, kind.is_some(), &selected),
            kind,
        });
    }
    order(records)
}

fn order(records: Vec<BlueRecord>) -> Result<Vec<BlueRecord>> {
    let by_name: BTreeMap<String, BlueRecord> =
        records.into_iter().map(|r| (r.name.clone(), r)).collect();
    let mut placed: BTreeSet<String> = BTreeSet::new();
    let mut open: BTreeSet<String> = BTreeSet::new();
    let mut out = Vec::new();
    for name in by_name.keys() {
        visit(name, &by_name, &mut placed, &mut open, &mut out)?;
    }
    Ok(out)
}

fn visit(
    name: &str,
    by_name: &BTreeMap<String, BlueRecord>,
    placed: &mut BTreeSet<String>,
    open: &mut BTreeSet<String>,
    out: &mut Vec<BlueRecord>,
) -> Result<()> {
    if placed.contains(name) {
        return Ok(());
    }
    if !open.insert(name.to_string()) {
        bail!("the record {name} reaches itself; blue records cannot be recursive");
    }
    let record = &by_name[name];
    let mut needs = BTreeSet::new();
    for f in &record.fields {
        if f.declared_type().is_some() {
            f.ty.records(&mut needs);
        }
    }
    for n in &needs {
        visit(n, by_name, placed, open, out)?;
    }
    open.remove(name);
    placed.insert(name.to_string());
    out.push(record.clone());
    Ok(())
}

fn sym(s: &str) -> Sexp {
    Sexp::Atom(Atom::Symbol(s.to_string()))
}

fn kw(s: &str) -> Sexp {
    Sexp::Atom(Atom::Keyword(s.to_string()))
}

fn text(s: &str) -> Sexp {
    Sexp::Atom(Atom::Str(s.to_string()))
}

fn map(pairs: impl IntoIterator<Item = (Sexp, Sexp)>) -> Sexp {
    let mut items = vec![sym(LOWERED_MAP)];
    for (k, v) in pairs {
        items.push(k);
        items.push(v);
    }
    Sexp::List(items)
}

fn call(head: &str, args: impl IntoIterator<Item = Sexp>) -> Sexp {
    let mut items = vec![sym(head)];
    items.extend(args);
    Sexp::List(items)
}

fn documented(doc: &str, form: Sexp) -> Sexp {
    Sexp::List(vec![sym(DOC_HEAD), text(doc), form])
}

fn define(name: &str, body: Sexp) -> Sexp {
    Sexp::List(vec![sym("define"), Sexp::List(vec![sym(name)]), body])
}

fn sentence(description: &str) -> Result<String> {
    let first = description.split("\n\n").next().unwrap_or(description);
    let joined = first
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    ensure!(
        !joined.contains("#{") && !joined.contains("\"\"\""),
        "an OpenAPI description holds blue doc syntax: {joined}"
    );
    Ok(joined.replace("...", "…"))
}

fn record_doc(r: &BlueRecord) -> Result<String> {
    let what = match r.kind {
        Some(k) => {
            let gv = if k.group.is_empty() {
                k.version.to_string()
            } else {
                format!("{}/{}", k.group, k.version)
            };
            format!("the {gv} {} kind.", k.kind)
        }
        None => format!("the `{}` shape.", r.key),
    };
    let example = if r.fields.iter().any(|f| f.required) {
        format!("    {name}?({{}}) #=> false", name = r.name)
    } else {
        format!(
            "    {name}?({name}(**unset.{name})) #=> true",
            name = r.name
        )
    };
    Ok(format!(
        "{name}: {what} {desc}\n\n{example}",
        name = r.name,
        desc = sentence(&r.description)?
    ))
}

fn record_form(r: &BlueRecord) -> Sexp {
    let mut items = vec![
        sym(LOWERED_RECORD),
        sym(&r.name),
        sym(&format!("{}?", r.name)),
    ];
    for f in &r.fields {
        items.push(match f.declared_type() {
            Some(ty) => Sexp::List(vec![kw(&f.name), ty]),
            None => kw(&f.name),
        });
    }
    Sexp::List(items)
}

fn catalog_form(records: &[BlueRecord]) -> Sexp {
    map(records.iter().filter_map(|r| {
        let k = r.kind?;
        let api_version = if k.group.is_empty() {
            k.version.to_string()
        } else {
            format!("{}/{}", k.group, k.version)
        };
        let scope = if k.cluster_scoped {
            "cluster"
        } else {
            "namespaced"
        };
        Some((
            kw(&r.name),
            map([
                (kw("api_version"), text(&api_version)),
                (kw("kind"), text(k.kind)),
                (kw("plural"), text(k.resource)),
                (kw("scope"), kw(scope)),
            ]),
        ))
    }))
}

fn unset_form(records: &[BlueRecord]) -> Sexp {
    map(records.iter().map(|r| {
        (
            kw(&r.name),
            map(r
                .fields
                .iter()
                .filter(|f| !f.required)
                .map(|f| (kw(&f.name), f.ty.unset()))),
        )
    }))
}

fn wire_form(records: &[BlueRecord]) -> Result<Sexp> {
    let mut wire: BTreeMap<String, String> = BTreeMap::new();
    for f in records.iter().flat_map(|r| &r.fields) {
        if f.name == f.wire {
            continue;
        }
        if let Some(seen) = wire.insert(f.name.clone(), f.wire.clone()) {
            ensure!(
                seen == f.wire,
                "the field {} is spelled {seen} and {} on the wire",
                f.name,
                f.wire
            );
        }
    }
    Ok(map(wire.iter().map(|(n, w)| (kw(n), text(w)))))
}

const MODULEDOC: &str = "kinds: the Kubernetes kinds blue speaks, as records. engenho-kube-codegen writes this file from the OpenAPI v1.34.0 schemas vendored in engenho-types, with the plural and scope from its KIND_CATALOG; edit the generator, never this file.

A record refuses an unknown field and a missing one. A Kubernetes field the schema marks required is a typed field the call must give; every other field is in `unset`, so a call spreads `**unset.<record>` and names only what it sets. A list or map field keeps its type and starts empty; any other optional field starts nil. Field names are snake_case; `wire_names` gives each one's spelling on the wire, and `catalog` the apiVersion, kind, plural and scope of each kind.

    config_map(**unset.config_map, data: {a: \"1\"}).data #=> {a: \"1\"}
    catalog.deployment.plural #=> \"deployments\"
    wire_names.api_group #=> \"apiGroup\"";

const CATALOG_DOC: &str =
    "catalog: each kind's apiVersion, kind, plural and scope, keyed by its record.

    catalog.role_binding.api_version #=> \"rbac.authorization.k8s.io/v1\"
    catalog.config_map.scope #=> :namespaced";

const UNSET_DOC: &str = "unset: each record's optional fields at their empty value, keyed by the record: an empty list, an empty map, or nil.

    unset.container.ports #=> []
    unset.object_meta.name #=> nil";

const WIRE_DOC: &str =
    "wire_names: the Kubernetes spelling of every field whose blue name differs from it.

    wire_names.container_port #=> \"containerPort\"
    wire_names.match_labels #=> \"matchLabels\"";

fn assertion(expr: Sexp) -> Sexp {
    Sexp::List(vec![
        sym("blue-assert"),
        Sexp::Quote(Box::new(expr.clone())),
        expr,
    ])
}

fn record_test(r: &BlueRecord) -> Sexp {
    let predicate = format!("{}?", r.name);
    let required: Option<Vec<(Sexp, Sexp)>> = r
        .fields
        .iter()
        .filter(|f| f.required)
        .map(|f| Some((kw(&f.name), f.ty.sample()?)))
        .collect();
    let (title, expr) = match required {
        Some(given) => {
            let mut args = vec![
                sym("blue-kwargs"),
                Sexp::List(vec![
                    sym("blue-splice"),
                    Sexp::List(vec![sym("blue-field"), sym("unset"), kw(&r.name)]),
                ]),
            ];
            let title = if given.is_empty() {
                format!("{} builds from unset", r.name)
            } else {
                args.push(map(given));
                format!("{} builds from unset and its required fields", r.name)
            };
            (title, call(&predicate, [call(&r.name, [Sexp::List(args)])]))
        }
        None => (
            format!("{} is not an empty map", r.name),
            call("not", [call(&predicate, [map([])])]),
        ),
    };
    Sexp::List(vec![sym("deftest"), text(&title), assertion(expr)])
}

pub fn blue_forms(records: &[BlueRecord]) -> Result<Vec<Sexp>> {
    let mut forms = vec![call(MODULEDOC_HEAD, [text(MODULEDOC)])];
    for r in records {
        forms.push(documented(&record_doc(r)?, record_form(r)));
    }
    forms.push(documented(
        CATALOG_DOC,
        define("catalog", catalog_form(records)),
    ));
    forms.push(documented(UNSET_DOC, define("unset", unset_form(records))));
    forms.push(documented(
        WIRE_DOC,
        define("wire_names", wire_form(records)?),
    ));
    forms.extend(records.iter().map(record_test));
    Ok(forms)
}

pub fn emit_blue_kinds(schemas: &BTreeMap<String, SchemaView>) -> Result<String> {
    let records = blue_records(schemas)?;
    let source = blue_lang_fmt::format_forms(&blue_forms(&records)?);
    let canonical = blue_lang_fmt::format_source_lossless(&source)
        .map_err(|e| anyhow!("generated blue does not parse: {e}"))?;
    ensure!(
        canonical == source,
        "generated blue is not in blue's one formatting"
    );
    Ok(source)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn view(props: &serde_json::Value, required: &[&str]) -> SchemaView {
        SchemaView {
            description: "A shape.".into(),
            properties: props
                .as_object()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            required: required.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    #[test]
    fn types_follow_the_schema_and_the_selection() {
        let selected: BTreeMap<String, String> = [(
            "io.k8s.api.core.v1.ContainerPort".to_string(),
            "container_port".to_string(),
        )]
        .into();
        let fields = fields_of(
            &view(
                &json!({
                    "apiVersion": {"type": "string"},
                    "name": {"type": "string"},
                    "ports": {"type": "array", "items": {"$ref": "#/components/schemas/io.k8s.api.core.v1.ContainerPort"}},
                    "env": {"type": "array", "items": {"$ref": "#/components/schemas/io.k8s.api.core.v1.EnvFromSource"}},
                    "limits": {"type": "object", "additionalProperties": {"$ref": "#/components/schemas/io.k8s.apimachinery.pkg.api.resource.Quantity"}},
                    "port": {"$ref": "#/components/schemas/io.k8s.apimachinery.pkg.util.intstr.IntOrString"},
                    "size": {"$ref": "#/components/schemas/io.k8s.apimachinery.pkg.api.resource.Quantity"},
                    "valueFrom": {"$ref": "#/components/schemas/io.k8s.api.core.v1.EnvVarSource"}
                }),
                &["name"],
            ),
            true,
            &selected,
        );
        let by: BTreeMap<&str, &BlueField> = fields.iter().map(|f| (f.name.as_str(), f)).collect();
        assert!(!by.contains_key("api_version"));
        assert_eq!(by["name"].ty, BlueType::Value("Str"));
        assert!(by["name"].required);
        assert_eq!(
            by["ports"].ty,
            BlueType::List(Box::new(BlueType::Record("container_port".into())))
        );
        assert_eq!(by["env"].ty, BlueType::List(Box::new(BlueType::Object)));
        assert_eq!(by["limits"].ty, BlueType::Value("Map"));
        assert_eq!(by["port"].ty, BlueType::Any);
        assert_eq!(by["size"].ty, BlueType::Value("Str"));
        assert!(by["size"].declared_type().is_none());
        assert!(by["ports"].declared_type().is_some());
        assert_eq!(by["value_from"].ty, BlueType::Object);
        assert!(by["value_from"].declared_type().is_none());
        assert_eq!(by["value_from"].ty.unset(), Sexp::Nil);
        assert_eq!(by["limits"].ty.unset(), Sexp::List(vec![sym(LOWERED_MAP)]));
    }

    #[test]
    fn a_record_follows_the_records_its_types_name() {
        let records = vec![
            BlueRecord {
                name: "a".into(),
                key: "a".into(),
                description: String::new(),
                fields: vec![BlueField {
                    name: "b".into(),
                    wire: "b".into(),
                    ty: BlueType::Record("b".into()),
                    required: true,
                }],
                kind: None,
            },
            BlueRecord {
                name: "b".into(),
                key: "b".into(),
                description: String::new(),
                fields: Vec::new(),
                kind: None,
            },
        ];
        let names: Vec<String> = order(records)
            .unwrap()
            .into_iter()
            .map(|r| r.name)
            .collect();
        assert_eq!(names, ["b", "a"]);
    }

    #[test]
    fn a_wire_spelling_is_one_per_name() {
        let r = |wire: &str| BlueRecord {
            name: wire.into(),
            key: wire.into(),
            description: String::new(),
            fields: vec![BlueField {
                name: "api_group".into(),
                wire: wire.into(),
                ty: BlueType::Any,
                required: false,
            }],
            kind: None,
        };
        assert!(wire_form(&[r("apiGroup"), r("apiGroup")]).is_ok());
        assert!(wire_form(&[r("apiGroup"), r("api_Group")]).is_err());
    }
}
