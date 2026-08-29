//! API DEFAULTING — the stage upstream calls `scheme.Default(obj)`.
//!
//! ★ WHY THIS MODULE EXISTS. Kubernetes' request pipeline is
//! `decode → DEFAULT → validate → admit → persist`, and until this module
//! landed engenho had every stage but the second. The consequence was not
//! cosmetic: a Pod POSTed without `spec.restartPolicy` was STORED without
//! one and READ BACK without one, so `kubectl get pod -o json` returned an
//! object whose `restartPolicy` was absent. Upstream guarantees that field
//! is always populated after a successful create — every client, controller
//! and conformance test is written against that guarantee.
//!
//! Measured on the live cid cluster 2026-08-29: pod `default/final-check`
//! read back `restartPolicy: null` while the kubelet restarted it 160 times,
//! i.e. the runtime was ALREADY behaving as `Always` while the stored object
//! declined to say so. The behaviour was right and the API contract was
//! broken — which is the failure mode defaulting exists to prevent, because
//! nothing crashes and nothing logs.
//!
//! ★ WHY A STAGE AND NOT A FIELD FIX. Defaulting is not "restartPolicy is
//! missing"; it is a per-kind, per-version transformation applied to EVERY
//! create. Adding one `if restart_policy.is_none()` at the Pod write path
//! would leave `dnsPolicy`, `schedulerName`, `imagePullPolicy` and the rest
//! equally absent, and would put the rule somewhere no future kind can
//! reach. The registry below is the shape upstream has, so a new kind is an
//! arm here rather than a new branch in the handler.
//!
//! ★ ORDER IS LOAD-BEARING: defaulting runs BEFORE validation and BEFORE
//! admission. Before validation, because a validator must judge the object
//! the cluster will actually store — a rule like "restartPolicy must be one
//! of Always/OnFailure/Never" would otherwise reject every object that
//! simply omitted it. Before admission, because a mutating webhook is
//! entitled to see, and to override, the defaults — a webhook that patches
//! `restartPolicy` must win, and it only can if the default is already
//! there when the webhook is called.
//!
//! ★ DEFAULTING NEVER OVERWRITES. Every rule here fills an ABSENT field.
//! A field the client set — including one set to a value engenho considers
//! wrong — travels through untouched, and it is validation's job, not this
//! module's, to reject it. A defaulter that overwrites is indistinguishable
//! from a mutating admission controller the client never asked for.

use serde_json::{Map, Value};

/// Apply the defaults for `kind` to `body`, in place.
///
/// A kind with no registered defaults is left byte-identical — this is the
/// common case and must stay free, since it runs on every create of every
/// resource including CRs.
pub fn apply(group: &str, version: &str, kind: &str, body: &mut Value) {
    // Core-group `v1` only, for now. A group-qualified kind (`apps/v1`
    // Deployment) defaults its embedded PodTemplateSpec upstream; that is a
    // separate arm and is NOT silently approximated here — see the module
    // ledger in the tests below for what is and is not covered.
    if !(group.is_empty() && version == "v1") {
        return;
    }
    if kind == "Pod" {
        if let Some(spec) = body.get_mut("spec").and_then(Value::as_object_mut) {
            default_pod_spec(spec);
        }
    }
}

/// The PodSpec defaults upstream applies in `SetDefaults_PodSpec` plus the
/// per-container ones from `SetDefaults_Container`. Values are upstream's,
/// not ours — a "sensible" value that disagrees with upstream is worse than
/// no default, because it silently diverges only for clients that omit the
/// field.
fn default_pod_spec(spec: &mut Map<String, Value>) {
    fill_str(spec, "restartPolicy", "Always");
    fill_str(spec, "dnsPolicy", "ClusterFirst");
    fill_str(spec, "schedulerName", "default-scheduler");
    spec.entry("terminationGracePeriodSeconds")
        .or_insert_with(|| Value::from(30));
    // An absent securityContext is an EMPTY OBJECT upstream, not null — a
    // client doing `spec.securityContext.runAsUser` gets undefined rather
    // than a type error.
    spec.entry("securityContext")
        .or_insert_with(|| Value::Object(Map::new()));

    for field in ["containers", "initContainers"] {
        if let Some(list) = spec.get_mut(field).and_then(Value::as_array_mut) {
            for c in list.iter_mut().filter_map(Value::as_object_mut) {
                default_container(c);
            }
        }
    }
}

fn default_container(c: &mut Map<String, Value>) {
    fill_str(c, "terminationMessagePath", "/dev/termination-log");
    fill_str(c, "terminationMessagePolicy", "File");

    // imagePullPolicy is the one default that READS another field: upstream
    // is `Always` for a `:latest` (or untagged, which resolves to latest)
    // image and `IfNotPresent` otherwise. Getting this wrong is invisible
    // until a pod silently runs a stale image.
    if !c.contains_key("imagePullPolicy") {
        let image = c.get("image").and_then(Value::as_str).unwrap_or_default();
        let policy = if pulls_always(image) {
            "Always"
        } else {
            "IfNotPresent"
        };
        c.insert("imagePullPolicy".into(), Value::from(policy));
    }

    if let Some(ports) = c.get_mut("ports").and_then(Value::as_array_mut) {
        for p in ports.iter_mut().filter_map(Value::as_object_mut) {
            fill_str(p, "protocol", "TCP");
        }
    }
}

/// `:latest` or no tag at all ⇒ `Always`.
///
/// The tag is the segment after the LAST `:` that contains no `/` — a
/// registry port (`localhost:5000/img`) is not a tag, and reading it as one
/// would flip the policy for every private-registry image.
fn pulls_always(image: &str) -> bool {
    match image.rsplit_once(':') {
        Some((_, tag)) if !tag.contains('/') => tag == "latest",
        // No colon at all, or the colon belonged to a registry port and the
        // remainder has no tag: untagged ⇒ latest ⇒ Always.
        _ => true,
    }
}

fn fill_str(m: &mut Map<String, Value>, key: &str, value: &str) {
    m.entry(key.to_string())
        .or_insert_with(|| Value::from(value));
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pod(spec: Value) -> Value {
        json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"p"},"spec":spec})
    }

    fn defaulted(spec: Value) -> Value {
        let mut b = pod(spec);
        apply("", "v1", "Pod", &mut b);
        b
    }

    #[test]
    fn restart_policy_defaults_to_always() {
        // The live defect, pinned: this exact spec read back with no
        // restartPolicy while the kubelet restarted it 160 times.
        let b = defaulted(json!({"containers":[{"name":"c","image":"busybox:1.36"}]}));
        assert_eq!(b["spec"]["restartPolicy"], "Always");
    }

    #[test]
    fn defaulting_never_overwrites_what_the_client_set() {
        let b = defaulted(json!({
            "restartPolicy": "Never",
            "dnsPolicy": "None",
            "schedulerName": "custom",
            "terminationGracePeriodSeconds": 5,
            "containers":[{"name":"c","image":"busybox:latest","imagePullPolicy":"Never"}]
        }));
        assert_eq!(b["spec"]["restartPolicy"], "Never");
        assert_eq!(b["spec"]["dnsPolicy"], "None");
        assert_eq!(b["spec"]["schedulerName"], "custom");
        assert_eq!(b["spec"]["terminationGracePeriodSeconds"], 5);
        // Even though `:latest` would default to Always, an explicit Never wins.
        assert_eq!(b["spec"]["containers"][0]["imagePullPolicy"], "Never");
    }

    #[test]
    fn pod_spec_defaults_match_upstream() {
        let b = defaulted(json!({"containers":[{"name":"c","image":"nginx:1.27"}]}));
        assert_eq!(b["spec"]["dnsPolicy"], "ClusterFirst");
        assert_eq!(b["spec"]["schedulerName"], "default-scheduler");
        assert_eq!(b["spec"]["terminationGracePeriodSeconds"], 30);
        assert!(
            b["spec"]["securityContext"].is_object(),
            "absent securityContext is an empty OBJECT upstream, not null"
        );
    }

    #[test]
    fn container_defaults_match_upstream() {
        let b = defaulted(json!({"containers":[{"name":"c","image":"nginx:1.27",
            "ports":[{"containerPort":80}]}]}));
        let c = &b["spec"]["containers"][0];
        assert_eq!(c["terminationMessagePath"], "/dev/termination-log");
        assert_eq!(c["terminationMessagePolicy"], "File");
        assert_eq!(c["ports"][0]["protocol"], "TCP");
    }

    #[test]
    fn image_pull_policy_follows_the_tag_not_the_colon() {
        let cases = [
            ("busybox:latest", "Always"),
            ("busybox", "Always"), // untagged ⇒ latest
            ("busybox:1.36", "IfNotPresent"),
            ("docker.io/library/busybox:1.36", "IfNotPresent"),
            // The trap: a registry PORT is not a tag. Read as one, every
            // private-registry image would flip to IfNotPresent-vs-Always
            // on the wrong signal.
            ("localhost:5000/img", "Always"),
            ("localhost:5000/img:2.1", "IfNotPresent"),
            ("localhost:5000/img:latest", "Always"),
        ];
        for (image, want) in cases {
            let b = defaulted(json!({"containers":[{"name":"c","image":image}]}));
            assert_eq!(
                b["spec"]["containers"][0]["imagePullPolicy"], want,
                "image {image}"
            );
        }
    }

    #[test]
    fn init_containers_are_defaulted_too() {
        let b = defaulted(json!({
            "initContainers":[{"name":"i","image":"busybox:1.36"}],
            "containers":[{"name":"c","image":"busybox:1.36"}]
        }));
        assert_eq!(
            b["spec"]["initContainers"][0]["imagePullPolicy"],
            "IfNotPresent"
        );
        assert_eq!(
            b["spec"]["initContainers"][0]["terminationMessagePolicy"],
            "File"
        );
    }

    #[test]
    fn a_kind_with_no_rules_is_untouched() {
        // Runs on EVERY create including CRs, so the no-op path must be
        // byte-exact, not merely harmless.
        let before = json!({"apiVersion":"v1","kind":"ConfigMap",
            "metadata":{"name":"c"},"data":{"k":"v"}});
        let mut after = before.clone();
        apply("", "v1", "ConfigMap", &mut after);
        assert_eq!(before, after);

        let before_cr = json!({"apiVersion":"example.com/v1","kind":"Pod",
            "metadata":{"name":"c"},"spec":{"containers":[]}});
        let mut after_cr = before_cr.clone();
        // Same KIND name, different GROUP — a CRD may legally call itself
        // Pod, and defaulting it as a core Pod would corrupt it.
        apply("example.com", "v1", "Pod", &mut after_cr);
        assert_eq!(before_cr, after_cr);
    }

    #[test]
    fn a_pod_with_no_spec_does_not_panic() {
        let mut b = json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"p"}});
        apply("", "v1", "Pod", &mut b);
        assert!(
            b.get("spec").is_none(),
            "no spec is left absent, not invented"
        );
    }
}
