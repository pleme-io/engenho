//! Derive engenho-control-types from `spec/engenho-control.openapi.yaml`.
//!
//! Three outputs, all into `OUT_DIR` (nothing generated is committed, and no
//! JVM is involved — see docs/CONTROL-PLANE.md § spike):
//!
//! * `types.rs`   — typify over `components.schemas`.
//! * `catalog.rs` — the closed `OperationId` and `CATALOG`, read from the
//!   operations and their `x-engenho-*` extensions.
//! * `ops.rs`     — one marker type + one typed `…Request` per operation, the
//!   `Operation` trait binding them to their response type, and the
//!   `EngenhoControl` trait with one method per operation.
//!
//! The build FAILS (panics with the offending operation named) on a spec that
//! is missing an operationId, a tag, an authority or a CLI spelling, or whose
//! parameter/response shapes this generator cannot type. The softer,
//! cross-operation invariants live in `tests/spec_invariants.rs`.

use std::collections::BTreeMap;
use std::path::PathBuf;

use heck::{ToPascalCase, ToSnakeCase};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use serde_json::Value;

const SPEC: &str = "../spec/engenho-control.openapi.yaml";
const METHODS: [&str; 5] = ["get", "put", "post", "delete", "patch"];

fn main() {
    println!("cargo:rerun-if-changed={SPEC}");
    println!("cargo:rerun-if-changed=build.rs");

    let text = std::fs::read_to_string(SPEC).unwrap_or_else(|e| panic!("read {SPEC}: {e}"));
    let spec: Value = serde_yaml::from_str(&text).unwrap_or_else(|e| panic!("parse {SPEC}: {e}"));
    let out = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));

    write(&out.join("types.rs"), &types(&spec));
    let ops = operations(&spec);
    write(&out.join("catalog.rs"), &catalog(&ops));
    write(&out.join("ops.rs"), &ops_module(&ops));
}

fn write(path: &std::path::Path, tokens: &TokenStream) {
    let file: syn::File = syn::parse2(tokens.clone())
        .unwrap_or_else(|e| panic!("generated {} is not valid Rust: {e}", path.display()));
    std::fs::write(path, prettyplease::unparse(&file))
        .unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
}

// ── types.rs ────────────────────────────────────────────────────────────────

fn types(spec: &Value) -> TokenStream {
    let schemas = spec
        .pointer("/components/schemas")
        .and_then(Value::as_object)
        .expect("spec has components.schemas");
    let map: BTreeMap<String, schemars::schema::Schema> = schemas
        .iter()
        .map(|(name, schema)| {
            let schema: schemars::schema::Schema = serde_json::from_value(schema.clone())
                .unwrap_or_else(|e| panic!("components.schemas.{name} is not a JSON schema: {e}"));
            (name.clone(), schema)
        })
        .collect();
    let mut settings = typify::TypeSpaceSettings::default();
    settings
        .with_struct_builder(false)
        .with_derive("PartialEq".to_string());
    let mut space = typify::TypeSpace::new(&settings);
    space
        .add_ref_types(map)
        .unwrap_or_else(|e| panic!("typify rejected components.schemas: {e}"));
    space.to_stream()
}

// ── operations ──────────────────────────────────────────────────────────────

struct Param {
    name: String,
    field: Ident,
    location: Location,
    required: bool,
    ty: TokenStream,
    kind: Kind,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Location {
    Path,
    Query,
    Header,
}

/// How a parameter's raw text becomes a JSON value before serde deserializes
/// it into its typed field.
#[derive(Clone, Copy)]
enum Kind {
    Text,
    Integer,
    Boolean,
}

struct Op {
    id: String,
    variant: Ident,
    snake: Ident,
    method: String,
    path: String,
    tag: String,
    tier: String,
    sensitive: bool,
    confirm: Option<String>,
    confirmation: Option<String>,
    resource: String,
    verb: String,
    params: Vec<Param>,
    body: Option<TokenStream>,
    response: TokenStream,
    /// The success body is `application/yaml` text rather than JSON.
    yaml_response: bool,
    success_status: u16,
}

fn operations(spec: &Value) -> Vec<Op> {
    let paths = spec
        .get("paths")
        .and_then(Value::as_object)
        .expect("spec has paths");
    let mut ops = Vec::new();
    for (path, item) in paths {
        let shared = item
            .get("parameters")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for method in METHODS {
            let Some(op) = item.get(method) else { continue };
            ops.push(operation(spec, path, method, op, &shared));
        }
    }
    ops
}

fn operation(spec: &Value, path: &str, method: &str, op: &Value, shared: &[Value]) -> Op {
    let where_ = format!("{} {path}", method.to_uppercase());
    let id = str_at(op, "operationId").unwrap_or_else(|| panic!("{where_}: no operationId"));
    let tags = op
        .get("tags")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("{id}: no tags"));
    assert!(
        tags.len() == 1,
        "{id}: exactly one tag required, found {}",
        tags.len()
    );
    let tag = tags[0]
        .as_str()
        .unwrap_or_else(|| panic!("{id}: tag is not a string"))
        .to_string();
    let tier =
        str_at(op, "x-engenho-authority").unwrap_or_else(|| panic!("{id}: no x-engenho-authority"));
    assert!(
        matches!(tier.as_str(), "observe" | "mutate" | "destructive"),
        "{id}: x-engenho-authority {tier:?} is not observe|mutate|destructive"
    );
    let sensitive = op.get("x-engenho-sensitive").is_some_and(|v| {
        v.as_bool()
            .unwrap_or_else(|| panic!("{id}: x-engenho-sensitive must be a bool"))
    });
    let cli = op
        .get("x-engenho-cli")
        .unwrap_or_else(|| panic!("{id}: no x-engenho-cli"));
    let resource =
        str_at(cli, "resource").unwrap_or_else(|| panic!("{id}: x-engenho-cli.resource"));
    let verb = str_at(cli, "verb").unwrap_or_else(|| panic!("{id}: x-engenho-cli.verb"));

    let params = operation_params(spec, &id, op, shared);
    let body = op.get("requestBody").map(|rb| {
        let schema = rb
            .pointer("/content/application~1json/schema")
            .unwrap_or_else(|| panic!("{id}: requestBody must be application/json"));
        schema_type(&id, schema)
    });
    let (success_status, response, yaml_response) = operation_response(&id, op);

    Op {
        variant: ident(&id.to_pascal_case()),
        snake: ident(&id.to_snake_case()),
        id,
        method: method.to_string(),
        path: path.to_string(),
        tag,
        tier,
        sensitive,
        confirm: str_at(op, "x-engenho-confirm"),
        confirmation: str_at(op, "x-engenho-confirmation"),
        resource,
        verb,
        params,
        body,
        response,
        yaml_response,
        success_status,
    }
}

/// Every parameter of an operation: the path item's shared ones, then its
/// own, each resolved and typed.
fn operation_params(spec: &Value, id: &str, op: &Value, shared: &[Value]) -> Vec<Param> {
    let mut params: Vec<Param> = Vec::new();
    for raw in shared.iter().chain(
        op.get("parameters")
            .and_then(Value::as_array)
            .into_iter()
            .flatten(),
    ) {
        let p = resolve(spec, raw);
        let name = str_at(p, "name").unwrap_or_else(|| panic!("{id}: parameter without a name"));
        let location = match str_at(p, "in").as_deref() {
            Some("path") => Location::Path,
            Some("query") => Location::Query,
            Some("header") => Location::Header,
            other => panic!("{id}: parameter {name} in {other:?} is not supported"),
        };
        let required = p.get("required").and_then(Value::as_bool).unwrap_or(false);
        let schema = p
            .get("schema")
            .unwrap_or_else(|| panic!("{id}: parameter {name} has no schema"));
        let (ty, kind) = param_type(id, &name, schema);
        params.push(Param {
            field: ident(&name.to_snake_case()),
            name,
            location,
            required: required || location == Location::Path,
            ty,
            kind,
        });
    }
    params
}

/// An operation's success status, the Rust type of its success body, and
/// whether that body is YAML text rather than JSON.
fn operation_response(id: &str, op: &Value) -> (u16, TokenStream, bool) {
    let responses = op
        .get("responses")
        .and_then(Value::as_object)
        .unwrap_or_else(|| panic!("{id}: no responses"));
    let (status, success) = responses
        .iter()
        .find(|(code, _)| code.starts_with('2'))
        .unwrap_or_else(|| panic!("{id}: no 2xx response"));
    let success_status: u16 = status
        .parse()
        .unwrap_or_else(|_| panic!("{id}: bad status {status}"));
    if let Some(schema) = success.pointer("/content/application~1json/schema") {
        (success_status, schema_type(id, schema), false)
    } else if success
        .pointer("/content/application~1yaml/schema/type")
        .and_then(Value::as_str)
        == Some("string")
    {
        (success_status, quote!(::std::string::String), true)
    } else {
        panic!("{id}: 2xx response must be application/json or an application/yaml string")
    }
}

fn resolve<'a>(spec: &'a Value, v: &'a Value) -> &'a Value {
    match v.get("$ref").and_then(Value::as_str) {
        Some(r) => {
            let pointer = r
                .strip_prefix('#')
                .unwrap_or_else(|| panic!("non-local $ref {r}"));
            spec.pointer(pointer)
                .unwrap_or_else(|| panic!("dangling $ref {r}"))
        }
        None => v,
    }
}

fn str_at(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(Value::as_str).map(str::to_string)
}

fn ident(s: &str) -> Ident {
    Ident::new(s, Span::call_site())
}

/// The Rust type of a `$ref` to a component schema.
fn schema_type(id: &str, schema: &Value) -> TokenStream {
    let r = schema
        .get("$ref")
        .and_then(Value::as_str)
        .unwrap_or_else(|| {
            panic!("{id}: body/response schemas must be a $ref to components.schemas")
        });
    let name = r
        .strip_prefix("#/components/schemas/")
        .unwrap_or_else(|| panic!("{id}: $ref {r} is not a component schema"));
    let name = ident(name);
    quote!(crate::types::#name)
}

fn param_type(id: &str, name: &str, schema: &Value) -> (TokenStream, Kind) {
    if schema.get("$ref").is_some() {
        return (schema_type(id, schema), Kind::Text);
    }
    match (
        schema.get("type").and_then(Value::as_str),
        schema.get("format").and_then(Value::as_str),
    ) {
        (Some("string"), _) => (quote!(::std::string::String), Kind::Text),
        (Some("boolean"), _) => (quote!(bool), Kind::Boolean),
        (Some("integer"), Some("uint64")) => (quote!(u64), Kind::Integer),
        (Some("integer"), Some("uint32")) => (quote!(u32), Kind::Integer),
        other => panic!("{id}: parameter {name} has unsupported inline schema {other:?}"),
    }
}

// ── catalog.rs ──────────────────────────────────────────────────────────────

fn catalog(ops: &[Op]) -> TokenStream {
    let variants: Vec<&Ident> = ops.iter().map(|o| &o.variant).collect();
    let ids: Vec<&str> = ops.iter().map(|o| o.id.as_str()).collect();
    let n = ops.len();
    let entries = ops.iter().map(|o| {
        let variant = &o.variant;
        let method = ident(&o.method.to_pascal_case());
        let path = &o.path;
        let tag = &o.tag;
        let tier = ident(&o.tier.to_pascal_case());
        let sensitive = o.sensitive;
        let gate = match (&o.confirm, &o.confirmation) {
            (Some(op), None) => {
                let op = ident(&op.to_pascal_case());
                quote!(ConfirmGate::Executes(crate::types::ReinitOp::#op))
            }
            (None, Some(c)) if c == "issue" => quote!(ConfirmGate::Issue),
            (None, Some(c)) if c == "cancel" => quote!(ConfirmGate::Cancel),
            (None, None) => quote!(ConfirmGate::None),
            other => panic!("{}: bad confirm extensions {other:?}", o.id),
        };
        let resource = &o.resource;
        let verb = &o.verb;
        let status = o.success_status;
        let params = o.params.iter().map(|p| {
            let name = &p.name;
            let location = match p.location {
                Location::Path => quote!(ParamLocation::Path),
                Location::Query => quote!(ParamLocation::Query),
                Location::Header => quote!(ParamLocation::Header),
            };
            let required = p.required;
            let kind = kind_tokens(p.kind);
            quote!(ParamSpec { name: #name, location: #location, required: #required, kind: #kind })
        });
        let body = o.body.is_some();
        let media = if o.yaml_response {
            quote!(MediaType::Yaml)
        } else {
            quote!(MediaType::Json)
        };
        quote! {
            OperationSpec {
                id: OperationId::#variant,
                method: HttpMethod::#method,
                path: #path,
                tag: #tag,
                tier: crate::types::AuthorityTier::#tier,
                sensitive: #sensitive,
                gate: #gate,
                cli: CliSpelling { resource: #resource, verb: #verb },
                success_status: #status,
                params: &[#(#params),*],
                body: #body,
                response_media: #media,
            }
        }
    });
    let variants2 = variants.clone();
    let variants3 = variants.clone();
    let ids2 = ids.clone();
    quote! {
        /// Every operation of the control API, in spec order. Closed: adding
        /// an operation to the spec adds a variant here, and every exhaustive
        /// `match` over it (the router, the CLI, the MCP catalog) stops
        /// compiling until the new operation is handled.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum OperationId { #(#variants),* }

        impl OperationId {
            /// Every operation, in spec (and `CATALOG`) order.
            pub const ALL: &'static [OperationId] = &[#(OperationId::#variants2),*];

            /// The spec's `operationId`.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self { #(OperationId::#variants3 => #ids),* }
            }

            /// This operation's row in [`CATALOG`].
            #[must_use]
            pub const fn spec(self) -> &'static OperationSpec {
                &CATALOG[self as usize]
            }

            /// Look an operation up by its spec `operationId`.
            #[must_use]
            pub fn from_operation_id(s: &str) -> Option<Self> {
                match s { #(#ids2 => Some(OperationId::#variants),)* _ => None }
            }
        }

        /// One row per operation, read from the spec's `x-engenho-*` extensions.
        pub static CATALOG: [OperationSpec; #n] = [#(#entries),*];
    }
}

// ── ops.rs ──────────────────────────────────────────────────────────────────

fn ops_module(ops: &[Op]) -> TokenStream {
    let items = ops.iter().map(op_items);
    let visits = ops.iter().map(|o| {
        let marker = &o.variant;
        quote!(crate::OperationId::#marker => visitor.visit::<#marker>())
    });
    let unserved = ops.iter().map(|o| {
        let snake = &o.snake;
        let req = request_ident(o);
        let resp = &o.response;
        let id = &o.id;
        quote! {
            async fn #snake(&self, _: &crate::Principal, _: #req) -> ::std::result::Result<#resp, crate::ControlError> {
                Err(crate::ControlError::refused(
                    crate::types::RefusalReason::Unsupported,
                    concat!(#id, " is not served here"),
                ))
            }
        }
    });
    let trait_methods = ops.iter().map(|o| {
        let snake = &o.snake;
        let req = request_ident(o);
        let resp = &o.response;
        let doc = format!("`{} {}` — `{}` ({} tier).", o.method.to_uppercase(), o.path, o.id, o.tier);
        quote! {
            #[doc = #doc]
            async fn #snake(&self, by: &crate::Principal, req: #req) -> ::std::result::Result<#resp, crate::ControlError>;
        }
    });
    quote! {
        /// The transport-agnostic control surface: one method per spec
        /// operation, over the spec's own types. The daemon implements it once;
        /// the Unix-socket and mTLS routers, and any in-process caller, reach
        /// it through [`Operation::invoke`].
        #[::async_trait::async_trait]
        pub trait EngenhoControl: Send + Sync + 'static {
            #(#trait_methods)*
        }

        /// Something done once per operation type: the one way to go from a
        /// runtime [`crate::OperationId`] to its marker type. Implement it
        /// once (route, parse, render) and [`visit`] dispatches every
        /// operation through it.
        pub trait OperationVisitor {
            /// What a visit produces.
            type Output;
            /// Visit operation `O`.
            fn visit<O: crate::Operation>(self) -> Self::Output;
        }

        /// Dispatch `visitor` on the marker type of `id`. Exhaustive: a new
        /// operation in the spec is a new arm here, generated with it.
        pub fn visit<V: OperationVisitor>(id: crate::OperationId, visitor: V) -> V::Output {
            match id { #(#visits),* }
        }

        /// A control that serves nothing: every operation is refused as
        /// unsupported. For exercising a transport across the whole
        /// catalog, and as the base of a partial implementation's tests.
        #[derive(Debug, Clone, Copy, Default)]
        pub struct Unserved;

        #[::async_trait::async_trait]
        impl EngenhoControl for Unserved {
            #(#unserved)*
        }

        #(#items)*
    }
}

fn kind_tokens(kind: Kind) -> TokenStream {
    match kind {
        Kind::Text => quote!(crate::wire::Kind::Text),
        Kind::Integer => quote!(crate::wire::Kind::Integer),
        Kind::Boolean => quote!(crate::wire::Kind::Boolean),
    }
}

fn request_ident(o: &Op) -> Ident {
    ident(&format!("{}Request", o.variant))
}

/// The marker, the typed request, its HTTP conversions, and the `Operation`
/// impl binding them — everything one spec operation contributes to `ops.rs`.
fn op_items(o: &Op) -> TokenStream {
    let marker = &o.variant;
    let request_ty = request_ident(o);
    let response_ty = &o.response;
    let snake = &o.snake;
    let marker_doc = format!(
        "Marker for `{}` (`{} {}`).",
        o.id,
        o.method.to_uppercase(),
        o.path
    );
    let request_doc = format!(
        "Every input of `{}`: path, query and header parameters, and the body.",
        o.id
    );
    let fields = request_fields(o);
    let to_http = to_http_body(o);
    let from_http = from_http_body(o);
    quote! {
        #[doc = #marker_doc]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub struct #marker;

        #[doc = #request_doc]
        #[derive(Debug, Clone, PartialEq)]
        pub struct #request_ty { #fields }

        impl crate::wire::OperationRequest for #request_ty {
            #[allow(unused_mut)]
            fn to_http(&self) -> crate::wire::HttpRequest { #to_http }

            fn from_http(parts: &crate::wire::HttpParts) -> ::std::result::Result<Self, crate::wire::BadRequest> {
                #from_http
            }
        }

        impl crate::Operation for #marker {
            const ID: crate::OperationId = crate::OperationId::#marker;
            type Request = #request_ty;
            type Response = #response_ty;
            fn invoke<'a>(
                ctl: &'a dyn EngenhoControl,
                by: &'a crate::Principal,
                req: #request_ty,
            ) -> crate::BoxFuture<'a, ::std::result::Result<#response_ty, crate::ControlError>> {
                ctl.#snake(by, req)
            }
        }
    }
}

/// The request struct's fields: one per parameter (optional ones as
/// `Option`), then the body.
fn request_fields(o: &Op) -> TokenStream {
    let params = o.params.iter().map(|p| {
        let field = &p.field;
        let ty = &p.ty;
        let doc = format!("`{}` ({} parameter).", p.name, loc_str(p.location));
        if p.required {
            quote!(#[doc = #doc] pub #field: #ty,)
        } else {
            quote!(#[doc = #doc] pub #field: ::std::option::Option<#ty>,)
        }
    });
    let body = o
        .body
        .as_ref()
        .map(|b| quote!(#[doc = "The JSON request body."] pub body: #b,));
    quote!(#(#params)* #body)
}

/// `to_http`: substitute and encode the path params, collect query and
/// header pairs, serialize the body.
fn to_http_body(o: &Op) -> TokenStream {
    let marker = &o.variant;
    let unused_self = if o.params.is_empty() && o.body.is_none() {
        quote!(let _ = self;)
    } else {
        TokenStream::new()
    };
    let path = path_render(o);
    let query = push_params(o, Location::Query, &ident("query"));
    let headers = push_params(o, Location::Header, &ident("headers"));
    let body = if o.body.is_some() {
        quote!(Some(
            ::serde_json::to_value(&self.body).expect("spec types serialize")
        ))
    } else {
        quote!(None)
    };
    quote! {
        #unused_self
        let mut p = ::std::string::String::new();
        #path
        let mut query: ::std::vec::Vec<(&'static str, ::std::string::String)> = ::std::vec::Vec::new();
        #(#query)*
        let mut headers: ::std::vec::Vec<(&'static str, ::std::string::String)> = ::std::vec::Vec::new();
        #(#headers)*
        crate::wire::HttpRequest {
            method: <#marker as crate::Operation>::ID.spec().method,
            path: p,
            query,
            headers,
            body: #body,
        }
    }
}

/// Statements that build the concrete path into `p`.
fn path_render(o: &Op) -> TokenStream {
    let mut out = TokenStream::new();
    let mut rest = o.path.as_str();
    while let Some(start) = rest.find('{') {
        let literal = &rest[..start];
        let end = rest[start..].find('}').expect("unclosed path param") + start;
        let name = &rest[start + 1..end];
        let param = o
            .params
            .iter()
            .find(|p| p.name == name && p.location == Location::Path)
            .unwrap_or_else(|| panic!("{}: path param {name} is not declared", o.id));
        let field = &param.field;
        out.extend(quote! {
            p.push_str(#literal);
            p.push_str(&crate::wire::encode_segment(&crate::wire::render(&self.#field)));
        });
        rest = &rest[end + 1..];
    }
    out.extend(quote!(p.push_str(#rest);));
    out
}

/// One push per parameter at `location` into the vector named `into`.
fn push_params(o: &Op, location: Location, into: &Ident) -> Vec<TokenStream> {
    o.params
        .iter()
        .filter(|p| p.location == location)
        .map(|p| {
            let field = &p.field;
            let name = &p.name;
            if p.required {
                quote!(#into.push((#name, crate::wire::render(&self.#field)));)
            } else {
                quote!(if let Some(v) = &self.#field { #into.push((#name, crate::wire::render(v))); })
            }
        })
        .collect()
}

/// `from_http`: parse each parameter from its raw text, then the body.
fn from_http_body(o: &Op) -> TokenStream {
    let unused_parts = if o.params.is_empty() && o.body.is_none() {
        quote!(let _ = parts;)
    } else {
        TokenStream::new()
    };
    let fields = o.params.iter().map(|p| {
        let field = &p.field;
        let name = &p.name;
        let kind = kind_tokens(p.kind);
        let source = match p.location {
            Location::Path => quote!(parts.path_param(#name)),
            Location::Query => quote!(parts.query_param(#name)),
            Location::Header => quote!(parts.header(#name)),
        };
        if p.required {
            quote!(#field: crate::wire::parse(#name, #kind, #source.ok_or(crate::wire::BadRequest::Missing(#name))?)?,)
        } else {
            quote!(#field: match #source { Some(raw) => Some(crate::wire::parse(#name, #kind, raw)?), None => None },)
        }
    });
    let body = o.body.as_ref().map(|_| {
        quote!(body: ::serde_json::from_value(parts.body.clone().ok_or(crate::wire::BadRequest::Missing("body"))?)
            .map_err(|e| crate::wire::BadRequest::Body(e.to_string()))?,)
    });
    quote! {
        #unused_parts
        Ok(Self { #(#fields)* #body })
    }
}

fn loc_str(l: Location) -> &'static str {
    match l {
        Location::Path => "path",
        Location::Query => "query",
        Location::Header => "header",
    }
}
