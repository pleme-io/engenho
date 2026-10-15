use serde_json::{Map, Value};

#[must_use]
pub fn creation_timestamp_is_unset(slot: Option<&Value>) -> bool {
    match slot {
        None | Some(Value::Null) => true,
        Some(Value::String(s)) => s.is_empty(),
        Some(Value::Object(m)) => m.is_empty(),
        _ => false,
    }
}

fn metadata_mut(body: &mut Value) -> Option<&mut Map<String, Value>> {
    let obj = body.as_object_mut()?;
    let metadata = obj
        .entry("metadata".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    if !metadata.is_object() {
        *metadata = Value::Object(Map::new());
    }
    metadata.as_object_mut()
}

fn fill_if_unset(meta: &mut Map<String, Value>, field: &str, value: &str) {
    let unset = match meta.get(field) {
        None | Some(Value::Null) => true,
        Some(Value::String(s)) => s.is_empty(),
        Some(_) => false,
    };
    if unset && !value.is_empty() {
        meta.insert(field.to_string(), Value::String(value.to_string()));
    }
}

pub fn stamp_creation_timestamp(body: &mut Value, now: &str) -> bool {
    let Some(meta) = metadata_mut(body) else {
        return false;
    };
    if creation_timestamp_is_unset(meta.get("creationTimestamp")) {
        meta.insert(
            "creationTimestamp".to_string(),
            Value::String(now.to_string()),
        );
        true
    } else {
        false
    }
}

pub fn stamp_claim_phase(body: &mut Value) {
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    let status = obj
        .entry("status".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    if !status.is_object() {
        *status = Value::Object(Map::new());
    }
    if let Some(status) = status.as_object_mut()
        && status
            .get("phase")
            .is_none_or(|p| p.is_null() || p.as_str() == Some(""))
    {
        status.insert("phase".to_string(), Value::String("Pending".to_string()));
    }
}

pub fn stamp_namespace_defaults(body: &mut Value) {
    const KUBERNETES_FINALIZER: &str = "kubernetes";
    const NAME_LABEL: &str = "kubernetes.io/metadata.name";
    let name = body
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .map(str::to_string);
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    if let Some(status) = obj
        .entry("status".to_string())
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
    {
        status
            .entry("phase".to_string())
            .or_insert_with(|| Value::String("Active".to_string()));
    }
    if let Some(finalizers) = obj
        .entry("spec".to_string())
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .and_then(|spec| {
            spec.entry("finalizers".to_string())
                .or_insert_with(|| Value::Array(Vec::new()))
                .as_array_mut()
        })
        && !finalizers
            .iter()
            .any(|v| v.as_str() == Some(KUBERNETES_FINALIZER))
    {
        finalizers.push(Value::String(KUBERNETES_FINALIZER.to_string()));
    }
    if let Some(name) = name
        && let Some(labels) = obj
            .entry("metadata".to_string())
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .and_then(|meta| {
                meta.entry("labels".to_string())
                    .or_insert_with(|| Value::Object(Map::new()))
                    .as_object_mut()
            })
    {
        labels
            .entry(NAME_LABEL.to_string())
            .or_insert_with(|| Value::String(name));
    }
}

pub fn stamp_born(
    group: &str,
    kind: &str,
    name: &str,
    namespace: Option<&str>,
    body: &mut Value,
    now: &str,
) {
    let Some(meta) = metadata_mut(body) else {
        return;
    };
    fill_if_unset(meta, "name", name);
    if let Some(namespace) = namespace {
        fill_if_unset(meta, "namespace", namespace);
    }
    stamp_creation_timestamp(body, now);
    if group.is_empty() {
        match kind {
            "Pod" => crate::pod::stamp_birth_status(body),
            "Namespace" => stamp_namespace_defaults(body),
            "PersistentVolumeClaim" => stamp_claim_phase(body),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const NOW: &str = "2026-10-15T12:00:00Z";

    #[test]
    fn an_unset_timestamp_is_absent_null_empty_or_an_empty_object() {
        assert!(creation_timestamp_is_unset(None));
        assert!(creation_timestamp_is_unset(Some(&Value::Null)));
        assert!(creation_timestamp_is_unset(Some(&json!(""))));
        assert!(creation_timestamp_is_unset(Some(&json!({}))));
        assert!(!creation_timestamp_is_unset(Some(&json!(NOW))));
    }

    #[test]
    fn the_creation_timestamp_is_stamped_once_and_never_bumped() {
        let mut body = json!({"metadata": {"name": "x"}});
        assert!(stamp_creation_timestamp(&mut body, NOW));
        assert!(!stamp_creation_timestamp(&mut body, "2030-01-01T00:00:00Z"));
        assert_eq!(body["metadata"]["creationTimestamp"], NOW);
    }

    #[test]
    fn a_born_object_carries_its_name_namespace_and_creation_timestamp() {
        let mut job = json!({"kind": "Job", "metadata": {"name": "j"}, "spec": {}});
        stamp_born("batch", "Job", "j", Some("default"), &mut job, NOW);
        assert_eq!(job["metadata"]["namespace"], "default");
        assert_eq!(job["metadata"]["creationTimestamp"], NOW);
        assert!(job.get("status").is_none(), "{job}");

        let mut bare = json!({"spec": {}});
        stamp_born("", "ConfigMap", "cm", None, &mut bare, NOW);
        assert_eq!(bare["metadata"]["name"], "cm");
        assert!(bare["metadata"].get("namespace").is_none());
    }

    #[test]
    fn a_born_object_keeps_what_it_already_carries() {
        let mut body = json!({"metadata": {"name": "keep", "namespace": "here",
            "creationTimestamp": "2020-01-01T00:00:00Z"}});
        let before = body.clone();
        stamp_born("apps", "Deployment", "other", Some("there"), &mut body, NOW);
        assert_eq!(body, before);
    }

    #[test]
    fn a_born_pod_is_pending_with_a_qos_class() {
        let mut pod = json!({"metadata": {"name": "p"},
            "spec": {"containers": [{"name": "c", "image": "i"}]}});
        stamp_born("", "Pod", "p", Some("default"), &mut pod, NOW);
        assert_eq!(pod["status"]["phase"], "Pending");
        assert_eq!(pod["status"]["qosClass"], "BestEffort");
    }

    #[test]
    fn a_born_claim_is_pending() {
        for status in [None, Some(json!({})), Some(json!({"phase": ""}))] {
            let mut body = json!({"metadata": {"name": "data"}, "spec": {}});
            if let Some(status) = &status {
                body["status"] = status.clone();
            }
            stamp_born("", "PersistentVolumeClaim", "data", None, &mut body, NOW);
            assert_eq!(body["status"]["phase"], "Pending", "{status:?}");
        }
        let mut bound = json!({"metadata": {"name": "data"}, "status": {"phase": "Bound"}});
        stamp_born("", "PersistentVolumeClaim", "data", None, &mut bound, NOW);
        assert_eq!(bound["status"]["phase"], "Bound");
    }

    #[test]
    fn a_born_namespace_is_active_with_its_finalizer_and_name_label() {
        let mut ns = json!({"metadata": {"name": "team-a"}});
        stamp_born("", "Namespace", "team-a", None, &mut ns, NOW);
        assert_eq!(ns.pointer("/spec/finalizers"), Some(&json!(["kubernetes"])));
        assert!(ns.pointer("/metadata/finalizers").is_none());
        assert_eq!(
            ns.pointer("/metadata/labels/kubernetes.io~1metadata.name"),
            Some(&json!("team-a"))
        );
        assert_eq!(ns.pointer("/status/phase"), Some(&json!("Active")));
        let again = ns.clone();
        stamp_namespace_defaults(&mut ns);
        assert_eq!(ns, again);
    }

    #[test]
    fn a_body_that_is_not_an_object_is_left_alone() {
        let mut body = json!("scalar");
        stamp_born("", "Pod", "p", Some("default"), &mut body, NOW);
        assert_eq!(body, json!("scalar"));
    }
}
