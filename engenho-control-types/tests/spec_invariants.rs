//! The control spec's cross-operation invariants, checked against the
//! authored YAML independently of `build.rs` (which only refuses to generate
//! from a spec it cannot type). Precedent: pangea-api's
//! `tests/spec_invariants.rs`.
//!
//! The engenho-runtime side of the contract — `ChildName`/`DriverName` equal
//! `Driver::ALL ∪ Listener::ALL ∪ {NodeLease}` — lives in engenho-runtime,
//! because this crate must not depend on the runtime it describes.

use std::collections::{BTreeMap, BTreeSet};

use engenho_control_types::wire::{HttpParts, HttpRequest, OperationRequest};
use engenho_control_types::{
    AuthorityTier, CATALOG, ConfirmGate, HttpMethod, OperationId, SPEC_YAML, ops, types,
};
use serde_yaml::Value;

struct RawOp {
    method: String,
    path: String,
    op: Value,
    shared_params: Vec<Value>,
}

fn spec() -> Value {
    serde_yaml::from_str(SPEC_YAML).expect("the spec parses")
}

fn raw_ops(spec: &Value) -> Vec<RawOp> {
    let mut out = Vec::new();
    for (path, item) in spec["paths"].as_mapping().expect("paths") {
        let path = path.as_str().expect("path key").to_string();
        let shared = item
            .get("parameters")
            .and_then(Value::as_sequence)
            .cloned()
            .unwrap_or_default();
        for method in ["get", "put", "post", "delete", "patch"] {
            if let Some(op) = item.get(method) {
                out.push(RawOp {
                    method: method.to_string(),
                    path: path.clone(),
                    op: op.clone(),
                    shared_params: shared.clone(),
                });
            }
        }
    }
    out
}

fn s<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(Value::as_str)
}

fn resolve<'a>(spec: &'a Value, v: &'a Value) -> &'a Value {
    match s(v, "$ref") {
        Some(r) => {
            let mut cur = spec;
            for seg in r.trim_start_matches("#/").split('/') {
                cur = &cur[seg];
            }
            cur
        }
        None => v,
    }
}

fn enum_values(spec: &Value, schema: &str) -> BTreeSet<String> {
    spec["components"]["schemas"][schema]["enum"]
        .as_sequence()
        .unwrap_or_else(|| panic!("{schema} is a string enum"))
        .iter()
        .map(|v| v.as_str().expect("string enum value").to_string())
        .collect()
}

#[test]
fn every_operation_has_an_id_one_declared_tag_an_authority_and_a_cli_spelling() {
    let spec = spec();
    let declared: BTreeSet<&str> = spec["tags"]
        .as_sequence()
        .expect("top-level tags")
        .iter()
        .map(|t| s(t, "name").expect("tag name"))
        .collect();
    for o in raw_ops(&spec) {
        let id = s(&o.op, "operationId")
            .unwrap_or_else(|| panic!("{} {} has no operationId", o.method, o.path));
        let tags = o.op["tags"]
            .as_sequence()
            .unwrap_or_else(|| panic!("{id}: no tags"));
        assert_eq!(tags.len(), 1, "{id}: exactly one tag");
        let tag = tags[0].as_str().expect("tag string");
        assert!(
            declared.contains(tag),
            "{id}: tag {tag} is not declared at the top level"
        );
        let tier = s(&o.op, "x-engenho-authority")
            .unwrap_or_else(|| panic!("{id}: no x-engenho-authority"));
        assert!(
            matches!(tier, "observe" | "mutate" | "destructive"),
            "{id}: tier {tier}"
        );
        let cli = &o.op["x-engenho-cli"];
        assert!(
            s(cli, "resource").is_some() && s(cli, "verb").is_some(),
            "{id}: x-engenho-cli"
        );
        if let Some(sens) = o.op.get("x-engenho-sensitive") {
            assert!(
                sens.as_bool().is_some(),
                "{id}: x-engenho-sensitive must be a bool"
            );
        }
    }
}

#[test]
fn operation_ids_are_unique() {
    let spec = spec();
    let mut seen = BTreeSet::new();
    for o in raw_ops(&spec) {
        let id = s(&o.op, "operationId").expect("id").to_string();
        assert!(seen.insert(id.clone()), "duplicate operationId {id}");
    }
}

#[test]
fn get_is_exactly_the_observe_tier() {
    let spec = spec();
    for o in raw_ops(&spec) {
        let id = s(&o.op, "operationId").expect("id");
        let observe = s(&o.op, "x-engenho-authority") == Some("observe");
        assert_eq!(o.method == "get", observe, "{id}: GET ⇔ observe");
    }
}

#[test]
fn destructive_is_exactly_the_confirm_gated_operations() {
    let spec = spec();
    for o in raw_ops(&spec) {
        let id = s(&o.op, "operationId").expect("id");
        let destructive = s(&o.op, "x-engenho-authority") == Some("destructive");
        let confirm = s(&o.op, "x-engenho-confirm");
        let confirmation = s(&o.op, "x-engenho-confirmation");
        assert_eq!(
            destructive,
            confirm.is_some() || confirmation.is_some(),
            "{id}: destructive ⇔ confirm-gated"
        );
        assert!(
            !(confirm.is_some() && confirmation.is_some()),
            "{id}: both confirm extensions"
        );
        if let Some(c) = confirmation {
            assert!(
                matches!(c, "issue" | "cancel"),
                "{id}: x-engenho-confirmation {c}"
            );
        }
    }
}

#[test]
fn every_reinit_op_is_executed_by_exactly_one_operation() {
    let spec = spec();
    let declared = enum_values(&spec, "ReinitOp");
    let mut executed: BTreeMap<String, String> = BTreeMap::new();
    for o in raw_ops(&spec) {
        if let Some(c) = s(&o.op, "x-engenho-confirm") {
            let id = s(&o.op, "operationId").expect("id").to_string();
            assert!(
                declared.contains(c),
                "{id}: x-engenho-confirm {c} is not a ReinitOp"
            );
            if let Some(prev) = executed.insert(c.to_string(), id.clone()) {
                panic!("ReinitOp {c} executed by both {prev} and {id}");
            }
        }
    }
    let executed: BTreeSet<String> = executed.into_keys().collect();
    assert_eq!(
        executed, declared,
        "every ReinitOp has exactly one executing operation"
    );

    // The confirmation request names the same closed set of operations.
    let variants: BTreeSet<String> = spec["components"]["schemas"]["ReinitRequest"]["oneOf"]
        .as_sequence()
        .expect("ReinitRequest oneOf")
        .iter()
        .map(|v| {
            v["properties"]["operation"]["enum"][0]
                .as_str()
                .expect("tag")
                .to_string()
        })
        .collect();
    assert_eq!(variants, declared, "ReinitRequest covers every ReinitOp");
}

#[test]
fn confirm_gated_operations_require_the_header_and_the_phrase() {
    let spec = spec();
    for o in raw_ops(&spec) {
        if s(&o.op, "x-engenho-confirm").is_none() {
            continue;
        }
        let id = s(&o.op, "operationId").expect("id");
        let params: Vec<&Value> = o
            .shared_params
            .iter()
            .chain(
                o.op.get("parameters")
                    .and_then(Value::as_sequence)
                    .into_iter()
                    .flatten(),
            )
            .map(|p| resolve(&spec, p))
            .collect();
        let header = params
            .iter()
            .find(|p| s(p, "in") == Some("header") && s(p, "name") == Some("Engenho-Confirmation"))
            .unwrap_or_else(|| panic!("{id}: no Engenho-Confirmation header"));
        assert_eq!(
            header["required"].as_bool(),
            Some(true),
            "{id}: the header is required"
        );
        let body = resolve(
            &spec,
            &o.op["requestBody"]["content"]["application/json"]["schema"],
        );
        let required: BTreeSet<&str> = body["required"]
            .as_sequence()
            .unwrap_or_else(|| panic!("{id}: body has required fields"))
            .iter()
            .filter_map(Value::as_str)
            .collect();
        assert!(
            required.contains("confirm_phrase"),
            "{id}: body requires confirm_phrase"
        );
    }
}

#[test]
fn cli_spellings_are_injective() {
    let spec = spec();
    let mut seen: BTreeMap<(String, String), String> = BTreeMap::new();
    for o in raw_ops(&spec) {
        let id = s(&o.op, "operationId").expect("id").to_string();
        let cli = &o.op["x-engenho-cli"];
        let key = (
            s(cli, "resource").expect("r").to_string(),
            s(cli, "verb").expect("v").to_string(),
        );
        if let Some(prev) = seen.insert(key.clone(), id.clone()) {
            panic!(
                "`engenho ctl {} {}` names both {prev} and {id}",
                key.0, key.1
            );
        }
    }
}

#[test]
fn every_path_is_versioned() {
    let spec = spec();
    for o in raw_ops(&spec) {
        assert!(o.path.starts_with("/v1/"), "{} is not under /v1", o.path);
    }
}

#[test]
fn every_union_is_a_oneof_of_inline_variants_with_one_single_valued_tag() {
    let spec = spec();
    for (name, schema) in spec["components"]["schemas"].as_mapping().expect("schemas") {
        let Some(variants) = schema.get("oneOf").and_then(Value::as_sequence) else {
            continue;
        };
        let name = name.as_str().expect("schema name");
        assert!(
            schema.get("discriminator").is_none(),
            "{name}: inline unions carry no discriminator"
        );
        // The tag is the property that is required and single-valued in
        // EVERY variant.
        let tags_of = |v: &Value| -> BTreeSet<String> {
            assert!(v.get("$ref").is_none(), "{name}: union variants are inline");
            let required: BTreeSet<&str> = v["required"]
                .as_sequence()
                .expect("variant.required")
                .iter()
                .filter_map(Value::as_str)
                .collect();
            v["properties"]
                .as_mapping()
                .expect("variant.properties")
                .iter()
                .filter(|(k, p)| {
                    required.contains(k.as_str().unwrap_or_default())
                        && p["enum"].as_sequence().is_some_and(|e| e.len() == 1)
                })
                .map(|(k, _)| k.as_str().expect("key").to_string())
                .collect()
        };
        let common = variants
            .iter()
            .map(tags_of)
            .reduce(|a, b| &a & &b)
            .unwrap_or_default();
        assert_eq!(
            common.len(),
            1,
            "{name}: exactly one common single-valued tag, found {common:?}"
        );
        let tag = common.into_iter().next().expect("tag");
        let values: Vec<&str> = variants
            .iter()
            .map(|v| {
                v["properties"][tag.as_str()]["enum"][0]
                    .as_str()
                    .expect("tag value")
            })
            .collect();
        let distinct: BTreeSet<&&str> = values.iter().collect();
        assert_eq!(
            distinct.len(),
            values.len(),
            "{name}: tag values are distinct"
        );
    }
}

#[test]
fn every_enum_value_is_snake_case() {
    fn walk(v: &Value, at: &str, bad: &mut Vec<String>) {
        match v {
            Value::Mapping(m) => {
                for (k, child) in m {
                    let key = k.as_str().unwrap_or_default();
                    if key == "enum" {
                        for e in child.as_sequence().into_iter().flatten() {
                            let e = e.as_str().unwrap_or_default();
                            let ok = e == engenho_control_types::API_VERSION
                                || (e.chars().next().is_some_and(|c| c.is_ascii_lowercase())
                                    && e.chars().all(|c| {
                                        c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'
                                    }));
                            if !ok {
                                bad.push(format!("{at}: {e}"));
                            }
                        }
                    } else {
                        walk(child, &format!("{at}.{key}"), bad);
                    }
                }
            }
            Value::Sequence(s) => {
                for (i, child) in s.iter().enumerate() {
                    walk(child, &format!("{at}[{i}]"), bad);
                }
            }
            _ => {}
        }
    }
    let mut bad = Vec::new();
    walk(&spec()["components"], "components", &mut bad);
    assert!(bad.is_empty(), "non-snake_case enum values: {bad:?}");
}

#[test]
fn the_catalog_mirrors_the_spec() {
    let spec = spec();
    let raw = raw_ops(&spec);
    assert_eq!(CATALOG.len(), raw.len(), "one CATALOG row per operation");
    assert_eq!(OperationId::ALL.len(), raw.len());
    for (i, id) in OperationId::ALL.iter().enumerate() {
        assert_eq!(CATALOG[i].id, *id, "ALL and CATALOG agree on order");
        assert_eq!(id.spec().id, *id, "spec() returns the operation's own row");
        assert_eq!(OperationId::from_operation_id(id.as_str()), Some(*id));
    }
    for o in &raw {
        let id = s(&o.op, "operationId").expect("id");
        let row = OperationId::from_operation_id(id)
            .unwrap_or_else(|| panic!("{id} missing"))
            .spec();
        assert_eq!(row.path, o.path, "{id}: path");
        assert_eq!(row.method.as_str(), o.method.to_uppercase(), "{id}: method");
        assert_eq!(row.tag, o.op["tags"][0].as_str().expect("tag"), "{id}: tag");
        let tier = match s(&o.op, "x-engenho-authority").expect("tier") {
            "observe" => AuthorityTier::Observe,
            "mutate" => AuthorityTier::Mutate,
            _ => AuthorityTier::Destructive,
        };
        assert_eq!(row.tier, tier, "{id}: tier");
        assert_eq!(
            row.sensitive,
            o.op.get("x-engenho-sensitive")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            "{id}: sensitive"
        );
        assert_eq!(
            row.cli.resource,
            s(&o.op["x-engenho-cli"], "resource").expect("r"),
            "{id}: cli"
        );
        assert_eq!(
            row.cli.verb,
            s(&o.op["x-engenho-cli"], "verb").expect("v"),
            "{id}: cli"
        );
        match (
            row.gate,
            s(&o.op, "x-engenho-confirm"),
            s(&o.op, "x-engenho-confirmation"),
        ) {
            (ConfirmGate::None, None, None)
            | (ConfirmGate::Issue, None, Some("issue"))
            | (ConfirmGate::Cancel, None, Some("cancel")) => {}
            (ConfirmGate::Executes(op), Some(c), None) => {
                assert_eq!(op.to_string(), c, "{id}: confirm");
            }
            other => panic!("{id}: gate {other:?}"),
        }
        let status = o.op["responses"]
            .as_mapping()
            .expect("responses")
            .keys()
            .filter_map(Value::as_str)
            .find(|c| c.starts_with('2'))
            .expect("2xx");
        assert_eq!(
            row.success_status.to_string(),
            status,
            "{id}: success status"
        );
    }
}

#[test]
fn the_tier_order_is_observe_mutate_destructive() {
    assert!(AuthorityTier::Observe < AuthorityTier::Mutate);
    assert!(AuthorityTier::Mutate < AuthorityTier::Destructive);
}

// ── generated request rendering round-trips ─────────────────────────────────

/// What a server would hand `from_http`: path params matched against the
/// spec's template and percent-decoded, query and headers as sent.
fn received(template: &str, sent: &HttpRequest) -> HttpParts {
    let decode = |seg: &str| -> String {
        let bytes = seg.as_bytes();
        let mut out = Vec::new();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' {
                out.push(u8::from_str_radix(&seg[i + 1..i + 3], 16).expect("hex"));
                i += 3;
            } else {
                out.push(bytes[i]);
                i += 1;
            }
        }
        String::from_utf8(out).expect("utf8")
    };
    let path_params = template
        .split('/')
        .zip(sent.path.split('/'))
        .filter_map(|(t, v)| {
            t.strip_prefix('{')
                .and_then(|t| t.strip_suffix('}'))
                .map(|n| (n.to_string(), decode(v)))
        })
        .collect();
    HttpParts {
        path_params,
        query: sent
            .query
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect(),
        headers: sent
            .headers
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect(),
        body: sent.body.clone(),
    }
}

fn round_trip<R: OperationRequest + PartialEq + std::fmt::Debug>(id: OperationId, req: &R) {
    let sent = req.to_http();
    assert_eq!(sent.method, id.spec().method, "{}: method", id.as_str());
    let back = R::from_http(&received(id.spec().path, &sent)).expect("parses back");
    assert_eq!(&back, req, "{}: round trip", id.as_str());
}

#[test]
fn set_config_leaf_round_trips_with_a_dotted_path_param() {
    let req = ops::SetConfigLeafRequest {
        leaf: "runtime.kubeconfig_publish_visibility"
            .parse()
            .expect("valid leaf"),
        body: types::SetLeafRequest {
            value: serde_json::json!("group"),
            persist: true,
            dry_run: false,
            restart_policy: Some(types::RestartPolicy::Defer),
            precondition_generation: Some(7),
        },
    };
    assert_eq!(
        req.to_http().path,
        "/v1/config/leaves/runtime.kubeconfig_publish_visibility"
    );
    round_trip(OperationId::SetConfigLeaf, &req);
}

#[test]
fn unset_config_leaf_round_trips_its_query_parameters() {
    let req = ops::UnsetConfigLeafRequest {
        leaf: "scheduler.tick_interval_ms".parse().expect("valid leaf"),
        dry_run: Some(true),
        restart_policy: Some(types::RestartPolicy::Now),
        precondition_generation: None,
    };
    round_trip(OperationId::UnsetConfigLeaf, &req);
}

#[test]
fn list_logs_round_trips_integers_and_an_enum() {
    let req = ops::ListLogsRequest {
        after: Some(42),
        wait_ms: Some(5_000),
        limit: None,
        level: Some(types::LogLevel::Warn),
    };
    round_trip(OperationId::ListLogs, &req);
}

#[test]
fn a_confirm_gated_operation_round_trips_its_header() {
    let req = ops::WipeStoreRequest {
        engenho_confirmation: "0123456789abcdef0123456789abcdef"
            .parse()
            .expect("valid id"),
        body: types::WipeStoreExecute {
            confirm_phrase: "engenho-ryn-1a2b3c4d".to_string(),
            scope: types::WipeScope::StoreOnly,
        },
    };
    let sent = req.to_http();
    assert_eq!(sent.headers[0].0, "Engenho-Confirmation");
    round_trip(OperationId::WipeStore, &req);
}

#[test]
fn a_child_path_param_round_trips() {
    for child in [
        types::ChildName::Kubelet,
        types::ChildName::EtcdFacade,
        types::ChildName::NodeLease,
    ] {
        round_trip(
            OperationId::RestartChild,
            &ops::RestartChildRequest { child },
        );
    }
}

#[test]
fn a_malformed_parameter_is_a_typed_bad_request() {
    let mut parts = HttpParts::default();
    parts
        .path_params
        .push(("leaf".to_string(), "Not A Leaf".to_string()));
    let err = ops::GetConfigLeafRequest::from_http(&parts).expect_err("pattern enforced");
    assert!(
        matches!(
            err,
            engenho_control_types::wire::BadRequest::Parameter { name: "leaf", .. }
        ),
        "{err}"
    );

    let missing = ops::GetChildRequest::from_http(&HttpParts::default()).expect_err("required");
    assert_eq!(
        missing,
        engenho_control_types::wire::BadRequest::Missing("child")
    );
}

#[test]
fn a_lifecycle_state_serializes_internally_tagged() {
    let s = types::LifecycleState::Exiting {
        intent: types::ExitIntent::Relaunch,
    };
    assert_eq!(
        serde_json::to_value(&s).expect("json"),
        serde_json::json!({"state": "exiting", "intent": "relaunch"})
    );
}

#[test]
fn the_error_statuses_match_the_refusal_classes() {
    use engenho_control_types::ControlError;
    use types::RefusalReason as R;
    assert_eq!(
        ControlError::refused(R::InsufficientAuthority, "x").status(),
        403
    );
    assert_eq!(ControlError::refused(R::RuntimeRunning, "x").status(), 409);
    assert_eq!(
        ControlError::refused(R::NotOverridableLeaf, "x").status(),
        422
    );
    assert_eq!(
        ControlError::blind(types::BlindReason::Internal, "x").status(),
        503
    );
    // GET methods only for the observe tier, seen from the generated side.
    for row in &CATALOG {
        assert_eq!(
            row.method == HttpMethod::Get,
            row.tier == AuthorityTier::Observe,
            "{}",
            row.id.as_str()
        );
    }
}
