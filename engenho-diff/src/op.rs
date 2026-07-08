//! The stimulus half of the harness — a typed [`Operation`] fired at both
//! targets. Constructors build the K8s REST path from typed segments (a
//! [`RestPath`], rendered via `Display`), never a `format!()`-composed URL.

use serde_json::{Value, json};

use crate::observe::HttpMethod;
use crate::verdict::{Gvr, Verb};

/// What KIND of surface an operation exercises — object CRUD, a list
/// collection, or the discovery index. The differ dispatches on this.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpKind {
    /// A single object round-trip (create/get/patch/replace/delete).
    Object,
    /// A collection LIST.
    List,
    /// A discovery index (`/api/v1`, `/apis/<g>/<v>`).
    Discovery,
}

/// A typed REST path builder — segments in, a `/`-joined path out. Keeps
/// path construction typed (no `format!()` of URL syntax, ★★ TYPED EMISSION).
#[derive(Clone, Debug, Default)]
pub struct RestPath(Vec<String>);

impl RestPath {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn seg(mut self, s: impl Into<String>) -> Self {
        self.0.push(s.into());
        self
    }

    /// Render to a leading-slash path string.
    #[must_use]
    pub fn render(&self) -> String {
        let mut out = String::new();
        for s in &self.0 {
            out.push('/');
            out.push_str(s);
        }
        out
    }
}

/// One authored operation.
#[derive(Clone, Debug)]
pub struct Operation {
    /// Stable identifier (report + status-diff signature).
    pub id: String,
    /// The HTTP verb.
    pub method: HttpMethod,
    /// The REST path (already rendered, incl. any query string).
    pub path: String,
    /// Request body, if any.
    pub body: Option<Value>,
    /// `Content-Type` header, if any.
    pub content_type: Option<String>,
    /// When set, a status mismatch on this op is reported as a
    /// [`Divergence::MissingVerb`](crate::verdict::Divergence::MissingVerb)
    /// (one side supports the verb, the other refuses) rather than a bare
    /// status-code diff.
    pub verb_probe: Option<(Gvr, Verb)>,
    /// The surface class.
    pub kind: OpKind,
}

fn configmaps_gvr() -> Gvr {
    Gvr {
        group: String::new(),
        version: "v1".into(),
        resource: "configmaps".into(),
    }
}

impl Operation {
    fn ns_configmaps(ns: &str) -> RestPath {
        RestPath::new()
            .seg("api")
            .seg("v1")
            .seg("namespaces")
            .seg(ns)
            .seg("configmaps")
    }

    /// POST a new Namespace.
    #[must_use]
    pub fn create_namespace(name: &str) -> Self {
        let mut id = String::from("create_namespace/");
        id.push_str(name);
        Self {
            id,
            method: HttpMethod::Post,
            path: RestPath::new()
                .seg("api")
                .seg("v1")
                .seg("namespaces")
                .render(),
            body: Some(json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": name},
            })),
            content_type: Some("application/json".into()),
            verb_probe: None,
            kind: OpKind::Object,
        }
    }

    /// GET a Namespace (where server-side defaulting divergences surface).
    #[must_use]
    pub fn get_namespace(name: &str) -> Self {
        let mut id = String::from("get_namespace/");
        id.push_str(name);
        Self {
            id,
            method: HttpMethod::Get,
            path: RestPath::new()
                .seg("api")
                .seg("v1")
                .seg("namespaces")
                .seg(name)
                .render(),
            body: None,
            content_type: None,
            verb_probe: None,
            kind: OpKind::Object,
        }
    }

    /// POST a new ConfigMap with the given `data`.
    #[must_use]
    pub fn create_configmap(ns: &str, name: &str, data: Value) -> Self {
        let mut id = String::from("create_configmap/");
        id.push_str(name);
        Self {
            id,
            method: HttpMethod::Post,
            path: Self::ns_configmaps(ns).render(),
            body: Some(json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": name},
                "data": data,
            })),
            content_type: Some("application/json".into()),
            verb_probe: None,
            kind: OpKind::Object,
        }
    }

    /// GET a ConfigMap.
    #[must_use]
    pub fn get_configmap(ns: &str, name: &str) -> Self {
        let mut id = String::from("get_configmap/");
        id.push_str(name);
        Self {
            id,
            method: HttpMethod::Get,
            path: Self::ns_configmaps(ns).seg(name).render(),
            body: None,
            content_type: None,
            verb_probe: None,
            kind: OpKind::Object,
        }
    }

    /// LIST ConfigMaps filtered to a single name via `fieldSelector`
    /// (isolates the diff from cluster-auto-created objects like
    /// `kube-root-ca.crt`). The `=` in the selector is percent-encoded.
    #[must_use]
    pub fn list_configmaps(ns: &str, name_filter: &str) -> Self {
        let mut path = Self::ns_configmaps(ns).render();
        path.push_str("?fieldSelector=metadata.name%3D");
        path.push_str(name_filter);
        let mut id = String::from("list_configmaps/");
        id.push_str(name_filter);
        Self {
            id,
            method: HttpMethod::Get,
            path,
            body: None,
            content_type: None,
            verb_probe: None,
            kind: OpKind::List,
        }
    }

    /// PATCH a ConfigMap (JSON merge patch).
    #[must_use]
    pub fn merge_patch_configmap(ns: &str, name: &str, patch: Value) -> Self {
        let mut id = String::from("merge_patch_configmap/");
        id.push_str(name);
        Self {
            id,
            method: HttpMethod::Patch,
            path: Self::ns_configmaps(ns).seg(name).render(),
            body: Some(patch),
            content_type: Some("application/merge-patch+json".into()),
            verb_probe: None,
            kind: OpKind::Object,
        }
    }

    /// PUT (replace) a ConfigMap — the sharp verb probe. engenho's router
    /// refuses PUT on the main object (400); k3s returns 200. A status
    /// mismatch here becomes `MissingVerb{Update, present_on: K3s}`.
    #[must_use]
    pub fn replace_configmap(ns: &str, name: &str, body: Value) -> Self {
        let mut id = String::from("replace_configmap/");
        id.push_str(name);
        Self {
            id,
            method: HttpMethod::Put,
            path: Self::ns_configmaps(ns).seg(name).render(),
            body: Some(body),
            content_type: Some("application/json".into()),
            verb_probe: Some((configmaps_gvr(), Verb::Update)),
            kind: OpKind::Object,
        }
    }

    /// DELETE a ConfigMap.
    #[must_use]
    pub fn delete_configmap(ns: &str, name: &str) -> Self {
        let mut id = String::from("delete_configmap/");
        id.push_str(name);
        Self {
            id,
            method: HttpMethod::Delete,
            path: Self::ns_configmaps(ns).seg(name).render(),
            body: None,
            content_type: None,
            verb_probe: None,
            kind: OpKind::Object,
        }
    }

    /// DELETE a Namespace (cleanup on the shared k3s cluster).
    #[must_use]
    pub fn delete_namespace(name: &str) -> Self {
        let mut id = String::from("delete_namespace/");
        id.push_str(name);
        Self {
            id,
            method: HttpMethod::Delete,
            path: RestPath::new()
                .seg("api")
                .seg("v1")
                .seg("namespaces")
                .seg(name)
                .render(),
            body: None,
            content_type: None,
            verb_probe: None,
            kind: OpKind::Object,
        }
    }

    /// GET the core/v1 discovery index.
    #[must_use]
    pub fn discovery_core_v1() -> Self {
        Self {
            id: "discovery/api/v1".into(),
            method: HttpMethod::Get,
            path: RestPath::new().seg("api").seg("v1").render(),
            body: None,
            content_type: None,
            verb_probe: None,
            kind: OpKind::Discovery,
        }
    }

    /// The body serialized to bytes, if present.
    #[must_use]
    pub fn body_bytes(&self) -> Option<Vec<u8>> {
        self.body
            .as_ref()
            .map(|b| serde_json::to_vec(b).unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_render_typed() {
        assert_eq!(Operation::create_namespace("x").path, "/api/v1/namespaces");
        assert_eq!(
            Operation::get_configmap("ns", "cm").path,
            "/api/v1/namespaces/ns/configmaps/cm"
        );
        assert_eq!(
            Operation::list_configmaps("ns", "cm").path,
            "/api/v1/namespaces/ns/configmaps?fieldSelector=metadata.name%3Dcm"
        );
    }

    #[test]
    fn replace_carries_verb_probe() {
        let op = Operation::replace_configmap("ns", "cm", json!({}));
        let (gvr, verb) = op.verb_probe.expect("replace is a verb probe");
        assert_eq!(gvr.resource, "configmaps");
        assert_eq!(verb, Verb::Update);
    }
}
