//! Label-selector typed helpers.
//!
//! K8s controllers match parents to children via label selectors:
//!   * EndpointsController: Service.spec.selector → matching Pods
//!   * ReplicaSetController: rs.spec.selector → matching Pods
//!   * Deployment: d.spec.selector → matching ReplicaSets
//!   * Job: job.spec.selector → matching Pods
//!   * NetworkPolicy: np.spec.podSelector → matching Pods
//!
//! Six sites + counting — extract once, reuse forever.

use serde_json::Value;

/// True iff every key/value in `match_labels` is present on
/// `resource.metadata.labels`. Missing labels on the resource =
/// no match. Empty selector = match nothing (K8s convention,
/// not "match everything"; an empty selector at the API level is
/// rejected before reaching controllers).
#[must_use]
pub fn matches_labels(resource: &Value, match_labels: &Value) -> bool {
    let want = match match_labels.as_object() {
        Some(o) => o,
        None => return false,
    };
    if want.is_empty() {
        return false;
    }
    let have = resource
        .get("metadata")
        .and_then(|m| m.get("labels"))
        .and_then(|l| l.as_object());
    let Some(have) = have else {
        return false;
    };
    want.iter()
        .all(|(k, v)| have.get(k).map(|h| h == v).unwrap_or(false))
}

/// Extract `spec.selector.matchLabels` from a parent (Service /
/// ReplicaSet / Deployment / etc.). Returns `None` if missing.
#[must_use]
pub fn selector_match_labels(parent: &Value) -> Option<&Value> {
    parent
        .get("spec")
        .and_then(|s| s.get("selector"))
        .and_then(|sel| sel.get("matchLabels").or(Some(sel)))
}

/// Specialized helper for Service.spec.selector — at the K8s API
/// level Service uses a flat map (no matchLabels wrapper). Falls
/// back to the structured form for forward-compat with other
/// kinds that may pass through here.
#[must_use]
pub fn service_selector(service: &Value) -> Option<&Value> {
    service.get("spec").and_then(|s| s.get("selector"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn matches_labels_full_match() {
        let pod = json!({"metadata": {"labels": {"app": "podinfo", "tier": "backend"}}});
        let sel = json!({"app": "podinfo", "tier": "backend"});
        assert!(matches_labels(&pod, &sel));
    }

    #[test]
    fn matches_labels_partial_match_is_match() {
        // K8s rule: selector is a subset condition.
        let pod = json!({"metadata": {"labels": {"app": "podinfo", "tier": "backend", "extra": "x"}}});
        let sel = json!({"app": "podinfo"});
        assert!(matches_labels(&pod, &sel));
    }

    #[test]
    fn matches_labels_mismatch_value() {
        let pod = json!({"metadata": {"labels": {"app": "podinfo"}}});
        let sel = json!({"app": "other"});
        assert!(!matches_labels(&pod, &sel));
    }

    #[test]
    fn matches_labels_missing_label_no_match() {
        let pod = json!({"metadata": {"labels": {"app": "podinfo"}}});
        let sel = json!({"tier": "backend"});
        assert!(!matches_labels(&pod, &sel));
    }

    #[test]
    fn matches_labels_no_labels_on_resource() {
        let pod = json!({"metadata": {"name": "p"}});
        let sel = json!({"app": "podinfo"});
        assert!(!matches_labels(&pod, &sel));
    }

    #[test]
    fn matches_labels_empty_selector_no_match() {
        let pod = json!({"metadata": {"labels": {"app": "x"}}});
        let sel = json!({});
        assert!(!matches_labels(&pod, &sel));
    }

    #[test]
    fn selector_match_labels_reads_structured_form() {
        let rs = json!({"spec": {"selector": {"matchLabels": {"app": "x"}}}});
        let s = selector_match_labels(&rs).unwrap();
        assert_eq!(s.get("app").unwrap(), "x");
    }

    #[test]
    fn service_selector_reads_flat_form() {
        let svc = json!({"spec": {"selector": {"app": "x"}}});
        let s = service_selector(&svc).unwrap();
        assert_eq!(s.get("app").unwrap(), "x");
    }
}
