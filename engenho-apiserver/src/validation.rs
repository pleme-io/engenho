//! API VALIDATION — the stage upstream calls `validation.ValidateX`.
//!
//! ★ WHY THIS MODULE EXISTS. Upstream's pipeline is
//! `decode → default → VALIDATE → admit → persist`. engenho gained
//! defaulting; validation was still absent for every core kind. The only
//! validator in the workspace ran CRD structural schemas, so a **built-in**
//! object could be stored with a `restartPolicy` of `"Sometimes"`, a
//! `protocol` of `"SCTPP"`, a containerPort of `99999`, or no containers at
//! all — accepted with 201 and rejected by nothing.
//!
//! ★ THE HARM IS DOWNSTREAM AND SILENT. A validator is not politeness: it
//! is the boundary that lets every consumer BELOW it assume its input is
//! well-formed. Without one, an invalid object is stored, replicated, and
//! then handed to a controller and a kubelet that were written against the
//! declared types — so the failure surfaces far from the request that
//! caused it, as a confusing runtime error on a node rather than a 422 to
//! the client who typed it.
//!
//! ★ VALIDATION RUNS AFTER DEFAULTING, and the order is what makes the
//! enum checks below legal at all. `restartPolicy` is optional on the wire;
//! if validation ran first it would have to accept absent-or-valid, and
//! then every rule would carry an "or missing" arm that hides real typos.
//! Because defaulting has already filled it, validation can demand a legal
//! value outright.
//!
//! ★ MESSAGES NAME THE FIELD PATH, because that is what a client renders.
//! `spec.containers[0].ports[0].protocol` tells an operator where to look;
//! "invalid pod" does not, and an error nobody can act on is close to no
//! error at all.

use serde_json::Value;

/// One validation failure, in upstream's `Status.details.causes` shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// The dotted field path, e.g. `spec.containers[0].name`.
    pub field: String,
    /// What is wrong, phrased so a client can render it verbatim.
    pub message: String,
}

impl Violation {
    fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
        }
    }
}

/// Validate `body` for `kind`, returning every violation found.
///
/// ★ EVERY violation, not the first. A client that fixes one error and
/// resubmits only to hit the next spends a round trip per mistake; upstream
/// returns them together and so does this.
#[must_use]
pub fn validate(group: &str, version: &str, kind: &str, body: &Value) -> Vec<Violation> {
    // Core `v1` only, matching the defaulting stage's scope. A CRD is
    // validated by its structural schema, which is a different mechanism
    // with a different source of truth.
    if !(group.is_empty() && version == "v1") {
        return Vec::new();
    }
    match kind {
        "Pod" => validate_pod(body),
        "Service" => validate_service(body),
        _ => Vec::new(),
    }
}

/// Values upstream accepts for `spec.restartPolicy`.
const RESTART_POLICIES: &[&str] = &["Always", "OnFailure", "Never"];
/// Values upstream accepts for `spec.dnsPolicy`.
const DNS_POLICIES: &[&str] = &["ClusterFirst", "ClusterFirstWithHostNet", "Default", "None"];
/// Values upstream accepts for a container's `imagePullPolicy`.
const PULL_POLICIES: &[&str] = &["Always", "IfNotPresent", "Never"];
/// Values upstream accepts for a port's `protocol`.
const PROTOCOLS: &[&str] = &["TCP", "UDP", "SCTP"];
/// Values upstream accepts for `Service.spec.type`.
const SERVICE_TYPES: &[&str] = &["ClusterIP", "NodePort", "LoadBalancer", "ExternalName"];

fn enum_field(out: &mut Vec<Violation>, obj: &Value, path: &str, key: &str, allowed: &[&str]) {
    let Some(v) = obj.get(key) else { return };
    let Some(s) = v.as_str() else {
        out.push(Violation::new(
            format!("{path}.{key}"),
            "must be a string".to_string(),
        ));
        return;
    };
    if !allowed.contains(&s) {
        out.push(Violation::new(
            format!("{path}.{key}"),
            format!(
                "unsupported value \"{s}\": supported values: {}",
                allowed.join(", ")
            ),
        ));
    }
}

fn validate_pod(body: &Value) -> Vec<Violation> {
    let mut out = Vec::new();
    let Some(spec) = body.get("spec") else {
        out.push(Violation::new("spec", "is required"));
        return out;
    };

    enum_field(&mut out, spec, "spec", "restartPolicy", RESTART_POLICIES);
    enum_field(&mut out, spec, "spec", "dnsPolicy", DNS_POLICIES);

    let containers = spec.get("containers").and_then(Value::as_array);
    match containers {
        // Upstream requires at least one. A pod with none is not a
        // degenerate pod — it can never become Running, so accepting it
        // creates an object whose only possible future is confusion.
        None => {
            out.push(Violation::new(
                "spec.containers",
                "must have at least one container",
            ));
        }
        Some(list) if list.is_empty() => {
            out.push(Violation::new(
                "spec.containers",
                "must have at least one container",
            ));
        }
        Some(list) => {
            let mut seen: Vec<&str> = Vec::new();
            for (i, c) in list.iter().enumerate() {
                let path = format!("spec.containers[{i}]");
                validate_container(&mut out, c, &path, &mut seen);
            }
        }
    }

    // Init containers are optional, but each is validated identically —
    // upstream shares the same routine, and a rule that applied to one and
    // not the other would be a gap nobody could predict from the docs.
    if let Some(list) = spec.get("initContainers").and_then(Value::as_array) {
        let mut seen: Vec<&str> = Vec::new();
        for (i, c) in list.iter().enumerate() {
            let path = format!("spec.initContainers[{i}]");
            validate_container(&mut out, c, &path, &mut seen);
        }
    }
    out
}

fn validate_container<'a>(
    out: &mut Vec<Violation>,
    c: &'a Value,
    path: &str,
    seen: &mut Vec<&'a str>,
) {
    match c.get("name").and_then(Value::as_str) {
        None | Some("") => out.push(Violation::new(format!("{path}.name"), "is required")),
        Some(name) => {
            if seen.contains(&name) {
                // Duplicate names make container status unaddressable:
                // `kubectl logs -c <name>` cannot say which one is meant.
                out.push(Violation::new(
                    format!("{path}.name"),
                    format!("duplicate container name \"{name}\""),
                ));
            } else {
                seen.push(name);
            }
        }
    }
    if c.get("image")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .is_empty()
    {
        out.push(Violation::new(format!("{path}.image"), "is required"));
    }
    enum_field(out, c, path, "imagePullPolicy", PULL_POLICIES);

    if let Some(ports) = c.get("ports").and_then(Value::as_array) {
        for (i, p) in ports.iter().enumerate() {
            let ppath = format!("{path}.ports[{i}]");
            enum_field(out, p, &ppath, "protocol", PROTOCOLS);
            if let Some(n) = p.get("containerPort").and_then(Value::as_i64) {
                if !(1..=65535).contains(&n) {
                    out.push(Violation::new(
                        format!("{ppath}.containerPort"),
                        format!("must be between 1 and 65535, inclusive (got {n})"),
                    ));
                }
            } else {
                out.push(Violation::new(
                    format!("{ppath}.containerPort"),
                    "is required",
                ));
            }
        }
    }
}

fn validate_service(body: &Value) -> Vec<Violation> {
    let mut out = Vec::new();
    let Some(spec) = body.get("spec") else {
        return out;
    };
    enum_field(&mut out, spec, "spec", "type", SERVICE_TYPES);

    // An ExternalName service is the one shape whose required field differs,
    // and omitting the check would let a Service exist that resolves to
    // nothing while reporting healthy.
    if spec.get("type").and_then(Value::as_str) == Some("ExternalName")
        && spec
            .get("externalName")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .is_empty()
    {
        out.push(Violation::new(
            "spec.externalName",
            "is required for type ExternalName",
        ));
    }

    if let Some(ports) = spec.get("ports").and_then(Value::as_array) {
        for (i, p) in ports.iter().enumerate() {
            let ppath = format!("spec.ports[{i}]");
            enum_field(&mut out, p, &ppath, "protocol", PROTOCOLS);
            if let Some(n) = p.get("port").and_then(Value::as_i64) {
                if !(1..=65535).contains(&n) {
                    out.push(Violation::new(
                        format!("{ppath}.port"),
                        format!("must be between 1 and 65535, inclusive (got {n})"),
                    ));
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pod(spec: Value) -> Value {
        json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"p"},"spec":spec})
    }

    fn v(body: &Value) -> Vec<Violation> {
        validate("", "v1", body["kind"].as_str().unwrap_or("Pod"), body)
    }

    fn fields(vs: &[Violation]) -> Vec<&str> {
        vs.iter().map(|x| x.field.as_str()).collect()
    }

    fn ok_pod() -> Value {
        pod(json!({
            "restartPolicy": "Always",
            "dnsPolicy": "ClusterFirst",
            "containers": [{
                "name": "c", "image": "busybox:1.36", "imagePullPolicy": "IfNotPresent",
                "ports": [{ "containerPort": 80, "protocol": "TCP" }]
            }]
        }))
    }

    #[test]
    fn a_well_formed_pod_has_no_violations() {
        // The anti-vacuity half: a validator that rejected everything would
        // pass every "invalid input is caught" test below.
        assert!(v(&ok_pod()).is_empty(), "{:?}", v(&ok_pod()));
    }

    #[test]
    fn an_illegal_enum_value_is_caught_with_the_supported_set() {
        // These were all accepted with 201 before this stage existed.
        let vs = v(&pod(json!({
            "restartPolicy": "Sometimes",
            "dnsPolicy": "Whatever",
            "containers": [{ "name": "c", "image": "i", "imagePullPolicy": "Maybe" }]
        })));
        assert_eq!(
            fields(&vs),
            vec![
                "spec.restartPolicy",
                "spec.dnsPolicy",
                "spec.containers[0].imagePullPolicy"
            ]
        );
        // The message must name the legal values, or the client cannot fix
        // it without reading source.
        assert!(vs[0].message.contains("Always"), "{}", vs[0].message);
    }

    #[test]
    fn every_violation_is_returned_not_just_the_first() {
        // One round trip per mistake is what returning early costs.
        let vs = v(&pod(json!({
            "restartPolicy": "Nope",
            "containers": [{ "name": "", "image": "" }]
        })));
        assert!(vs.len() >= 3, "expected several, got {:?}", fields(&vs));
    }

    #[test]
    fn a_pod_with_no_containers_is_rejected() {
        // It can never become Running, so accepting it creates an object
        // whose only possible future is confusion.
        assert_eq!(fields(&v(&pod(json!({})))), vec!["spec.containers"]);
        assert_eq!(
            fields(&v(&pod(json!({ "containers": [] })))),
            vec!["spec.containers"]
        );
    }

    #[test]
    fn duplicate_container_names_are_rejected() {
        // They make container status unaddressable: `kubectl logs -c name`
        // cannot say which one is meant.
        let vs = v(&pod(json!({
            "containers": [
                { "name": "dup", "image": "i" },
                { "name": "dup", "image": "i" }
            ]
        })));
        assert_eq!(fields(&vs), vec!["spec.containers[1].name"]);
        assert!(vs[0].message.contains("duplicate"));
    }

    #[test]
    fn port_numbers_are_bounded_and_the_path_names_the_exact_element() {
        // `spec.containers[0].ports[1].containerPort` is what a client
        // renders; "invalid pod" is an error nobody can act on.
        let vs = v(&pod(json!({
            "containers": [{
                "name": "c", "image": "i",
                "ports": [
                    { "containerPort": 80 },
                    { "containerPort": 99999 },
                    { "containerPort": 0 },
                    { "protocol": "TCP" }
                ]
            }]
        })));
        assert_eq!(
            fields(&vs),
            vec![
                "spec.containers[0].ports[1].containerPort",
                "spec.containers[0].ports[2].containerPort",
                "spec.containers[0].ports[3].containerPort",
            ]
        );
    }

    #[test]
    fn init_containers_get_the_same_rules() {
        // A rule applying to one list and not the other is a gap nobody
        // could predict from the docs.
        let vs = v(&pod(json!({
            "containers": [{ "name": "c", "image": "i" }],
            "initContainers": [{ "name": "", "image": "" }]
        })));
        assert_eq!(
            fields(&vs),
            vec![
                "spec.initContainers[0].name",
                "spec.initContainers[0].image"
            ]
        );
    }

    #[test]
    fn service_type_and_external_name_are_validated_together() {
        let bad = json!({"apiVersion":"v1","kind":"Service","metadata":{"name":"s"},
            "spec":{"type":"Nonsense"}});
        assert_eq!(fields(&v(&bad)), vec!["spec.type"]);

        // An ExternalName service with no externalName resolves to nothing
        // while reporting healthy.
        let missing = json!({"apiVersion":"v1","kind":"Service","metadata":{"name":"s"},
            "spec":{"type":"ExternalName"}});
        assert_eq!(fields(&v(&missing)), vec!["spec.externalName"]);

        let good = json!({"apiVersion":"v1","kind":"Service","metadata":{"name":"s"},
            "spec":{"type":"ExternalName","externalName":"example.com"}});
        assert!(v(&good).is_empty());
    }

    #[test]
    fn a_crd_is_left_to_its_structural_schema() {
        // A different mechanism with a different source of truth; running
        // Pod rules against a CR would reject valid objects.
        let cr = json!({"apiVersion":"example.com/v1","kind":"Pod","spec":{}});
        assert!(validate("example.com", "v1", "Pod", &cr).is_empty());
    }

    #[test]
    fn an_unvalidated_kind_passes_rather_than_being_rejected() {
        // Absence of rules must not become denial — that would break every
        // kind this stage has not learned yet.
        let cm = json!({"apiVersion":"v1","kind":"ConfigMap","data":{"k":"v"}});
        assert!(validate("", "v1", "ConfigMap", &cm).is_empty());
    }
}
