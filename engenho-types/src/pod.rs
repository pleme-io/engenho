use serde_json::{Map, Value};

pub fn stamp_birth_status(body: &mut Value) {
    let qos = qos_class(body.get("spec"));
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    let status = obj
        .entry("status".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    if !status.is_object() {
        *status = Value::Object(Map::new());
    }
    if let Some(status) = status.as_object_mut() {
        status
            .entry("phase".to_string())
            .or_insert_with(|| Value::String("Pending".to_string()));
        status
            .entry("qosClass".to_string())
            .or_insert_with(|| Value::String(qos.to_string()));
    }
}

#[must_use]
pub fn qos_class(spec: Option<&Value>) -> &'static str {
    let containers: Vec<&Value> = ["containers", "initContainers"]
        .iter()
        .filter_map(|f| spec.and_then(|s| s.get(*f)).and_then(Value::as_array))
        .flatten()
        .collect();
    let quantity = |c: &Value, kind: &str, res: &str| {
        c.get("resources")
            .and_then(|r| r.get(kind))
            .and_then(|m| m.get(res))
            .cloned()
    };
    let mut any = false;
    let mut guaranteed = !containers.is_empty();
    for c in &containers {
        for res in ["cpu", "memory"] {
            let request = quantity(c, "requests", res);
            let limit = quantity(c, "limits", res);
            any |= request.is_some() || limit.is_some();
            let effective_request = request.or_else(|| limit.clone());
            if limit.is_none() || effective_request != limit {
                guaranteed = false;
            }
        }
    }
    match (any, guaranteed) {
        (false, _) => "BestEffort",
        (true, true) => "Guaranteed",
        (true, false) => "Burstable",
    }
}

/// The annotation naming the container `kubectl` and k9s address when a
/// request names none.
pub const DEFAULT_CONTAINER_ANNOTATION: &str = "kubectl.kubernetes.io/default-container";

/// The container a request that names none is about: the
/// [`DEFAULT_CONTAINER_ANNOTATION`] when it names an app container, else the
/// first app container in spec order (never an alphabetical first).
/// `None` for a pod with no app containers.
#[must_use]
pub fn default_container(pod: &Value) -> Option<&str> {
    let names = || {
        pod.pointer("/spec/containers")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|c| c.get("name").and_then(Value::as_str))
    };
    let annotated = pod
        .pointer("/metadata/annotations")
        .and_then(|a| a.get(DEFAULT_CONTAINER_ANNOTATION))
        .and_then(Value::as_str)
        .filter(|d| names().any(|n| n == *d));
    annotated.or_else(|| names().next())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_pod_is_born_pending_with_its_qos_class() {
        let mut body = json!({"metadata": {"name": "p"}, "spec": {"containers": [{"name": "c", "image": "x"}]}});
        stamp_birth_status(&mut body);
        assert_eq!(body["status"]["phase"], "Pending");
        assert_eq!(body["status"]["qosClass"], "BestEffort");
    }

    #[test]
    fn qos_class_follows_requests_and_limits() {
        let pod = |resources: Value| json!({"containers": [{"name": "c", "resources": resources}]});
        let both = json!({"cpu": "1", "memory": "1Gi"});
        assert_eq!(qos_class(Some(&pod(json!({})))), "BestEffort");
        assert_eq!(
            qos_class(Some(&pod(json!({"limits": both.clone()})))),
            "Guaranteed"
        );
        assert_eq!(
            qos_class(Some(&pod(
                json!({"requests": both.clone(), "limits": both.clone()})
            ))),
            "Guaranteed"
        );
        assert_eq!(
            qos_class(Some(&pod(json!({"requests": {"cpu": "1"}})))),
            "Burstable"
        );
        assert_eq!(
            qos_class(Some(&pod(
                json!({"requests": {"cpu": "1", "memory": "1Gi"}, "limits": {"cpu": "2", "memory": "1Gi"}})
            ))),
            "Burstable"
        );
        assert_eq!(qos_class(None), "BestEffort");
    }

    #[test]
    fn a_pod_status_the_client_set_is_kept() {
        let mut body = json!({"metadata": {"name": "p"}, "status": {"phase": "Running", "qosClass": "Guaranteed"}});
        stamp_birth_status(&mut body);
        assert_eq!(body["status"]["phase"], "Running");
        assert_eq!(body["status"]["qosClass"], "Guaranteed");
    }

    #[test]
    fn the_default_container_is_the_annotation_else_the_first_in_spec_order() {
        let spec = json!({"containers": [{"name": "zeta"}, {"name": "alpha"}]});
        let plain = json!({"metadata": {"name": "p"}, "spec": spec});
        assert_eq!(default_container(&plain), Some("zeta"));
        let annotated = json!({"metadata": {"name": "p", "annotations": {
            DEFAULT_CONTAINER_ANNOTATION: "alpha"}}, "spec": spec});
        assert_eq!(default_container(&annotated), Some("alpha"));
        let stale = json!({"metadata": {"name": "p", "annotations": {
            DEFAULT_CONTAINER_ANNOTATION: "gone"}}, "spec": spec});
        assert_eq!(default_container(&stale), Some("zeta"));
        assert_eq!(default_container(&json!({"spec": {}})), None);
    }
}
