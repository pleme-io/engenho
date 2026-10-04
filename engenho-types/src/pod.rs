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
}
