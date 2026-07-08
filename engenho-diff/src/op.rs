//! The stimulus half of the harness — a typed [`Operation`] fired at both
//! targets. Constructors build the K8s REST path from typed segments (a
//! [`RestPath`], rendered via `Display`), never a `format!()`-composed URL.

use serde_json::{Value, json};

use crate::observe::HttpMethod;
use crate::path::JsonPath;
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
    /// When set (for [`OpKind::Object`]), the differ restricts the object
    /// comparison to the subtree at this path — a FIELD-LEVEL probe. It
    /// isolates a targeted invariant (e.g. "a `/status` PUT left
    /// `spec.containers[0].image` unchanged") from a kind's unrelated
    /// server-side-defaulting divergence, so a Pod field-diff need not carry
    /// the whole (heavily-defaulted) PodSpec into the ratchet.
    pub focus: Option<JsonPath>,
}

fn configmaps_gvr() -> Gvr {
    Gvr {
        group: String::new(),
        version: "v1".into(),
        resource: "configmaps".into(),
    }
}

impl Operation {
    // ── internal builders (ONE construction shape; ★ standardize) ────────
    //
    // Every public constructor routes through `object`/`list`/`discovery` +
    // the `with_*` builders, so a new field (like `focus`) is added in ONE
    // place, not fanned across a dozen struct literals.

    fn object(id: String, method: HttpMethod, path: String) -> Self {
        Self {
            id,
            method,
            path,
            body: None,
            content_type: None,
            verb_probe: None,
            kind: OpKind::Object,
            focus: None,
        }
    }

    #[must_use]
    fn with_body(mut self, body: Value, content_type: &str) -> Self {
        self.body = Some(body);
        self.content_type = Some(content_type.into());
        self
    }

    #[must_use]
    fn with_kind(mut self, kind: OpKind) -> Self {
        self.kind = kind;
        self
    }

    #[must_use]
    fn with_verb_probe(mut self, gvr: Gvr, verb: Verb) -> Self {
        self.verb_probe = Some((gvr, verb));
        self
    }

    /// Restrict the object diff to the subtree at `focus` (a field-level
    /// probe). Builder style; see [`Operation::focus`].
    #[must_use]
    pub fn with_focus(mut self, focus: JsonPath) -> Self {
        self.focus = Some(focus);
        self
    }

    /// `/api/v1/namespaces/<ns>/<plural>` — the namespaced collection path.
    fn ns_collection(ns: &str, plural: &str) -> RestPath {
        RestPath::new()
            .seg("api")
            .seg("v1")
            .seg("namespaces")
            .seg(ns)
            .seg(plural)
    }

    fn ns_configmaps(ns: &str) -> RestPath {
        Self::ns_collection(ns, "configmaps")
    }

    fn ident(prefix: &str, name: &str) -> String {
        let mut id = String::from(prefix);
        id.push_str(name);
        id
    }

    /// POST a new Namespace.
    #[must_use]
    pub fn create_namespace(name: &str) -> Self {
        Self::object(
            Self::ident("create_namespace/", name),
            HttpMethod::Post,
            RestPath::new()
                .seg("api")
                .seg("v1")
                .seg("namespaces")
                .render(),
        )
        .with_body(
            json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": name},
            }),
            "application/json",
        )
    }

    /// GET a Namespace (where server-side defaulting divergences surface).
    #[must_use]
    pub fn get_namespace(name: &str) -> Self {
        Self::object(
            Self::ident("get_namespace/", name),
            HttpMethod::Get,
            RestPath::new()
                .seg("api")
                .seg("v1")
                .seg("namespaces")
                .seg(name)
                .render(),
        )
    }

    /// POST a new ConfigMap with the given `data`.
    #[must_use]
    pub fn create_configmap(ns: &str, name: &str, data: Value) -> Self {
        Self::object(
            Self::ident("create_configmap/", name),
            HttpMethod::Post,
            Self::ns_configmaps(ns).render(),
        )
        .with_body(
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": name},
                "data": data,
            }),
            "application/json",
        )
    }

    /// GET a ConfigMap.
    #[must_use]
    pub fn get_configmap(ns: &str, name: &str) -> Self {
        Self::object(
            Self::ident("get_configmap/", name),
            HttpMethod::Get,
            Self::ns_configmaps(ns).seg(name).render(),
        )
    }

    /// LIST ConfigMaps filtered to a single name via `fieldSelector`
    /// (isolates the diff from cluster-auto-created objects like
    /// `kube-root-ca.crt`). The `=` in the selector is percent-encoded.
    #[must_use]
    pub fn list_configmaps(ns: &str, name_filter: &str) -> Self {
        let mut path = Self::ns_configmaps(ns).render();
        path.push_str("?fieldSelector=metadata.name%3D");
        path.push_str(name_filter);
        Self::object(
            Self::ident("list_configmaps/", name_filter),
            HttpMethod::Get,
            path,
        )
        .with_kind(OpKind::List)
    }

    /// PATCH a ConfigMap (JSON merge patch).
    #[must_use]
    pub fn merge_patch_configmap(ns: &str, name: &str, patch: Value) -> Self {
        Self::object(
            Self::ident("merge_patch_configmap/", name),
            HttpMethod::Patch,
            Self::ns_configmaps(ns).seg(name).render(),
        )
        .with_body(patch, "application/merge-patch+json")
    }

    /// PUT (replace) a ConfigMap — the sharp verb probe. engenho's router
    /// refuses PUT on the main object (400); k3s returns 200. A status
    /// mismatch here becomes `MissingVerb{Update, present_on: K3s}`.
    #[must_use]
    pub fn replace_configmap(ns: &str, name: &str, body: Value) -> Self {
        Self::object(
            Self::ident("replace_configmap/", name),
            HttpMethod::Put,
            Self::ns_configmaps(ns).seg(name).render(),
        )
        .with_body(body, "application/json")
        .with_verb_probe(configmaps_gvr(), Verb::Update)
    }

    /// DELETE a ConfigMap.
    #[must_use]
    pub fn delete_configmap(ns: &str, name: &str) -> Self {
        Self::object(
            Self::ident("delete_configmap/", name),
            HttpMethod::Delete,
            Self::ns_configmaps(ns).seg(name).render(),
        )
    }

    /// DELETE a Namespace (cleanup on the shared k3s cluster).
    #[must_use]
    pub fn delete_namespace(name: &str) -> Self {
        Self::object(
            Self::ident("delete_namespace/", name),
            HttpMethod::Delete,
            RestPath::new()
                .seg("api")
                .seg("v1")
                .seg("namespaces")
                .seg(name)
                .render(),
        )
    }

    /// GET the core/v1 discovery index.
    #[must_use]
    pub fn discovery_core_v1() -> Self {
        Self::object(
            "discovery/api/v1".into(),
            HttpMethod::Get,
            RestPath::new().seg("api").seg("v1").render(),
        )
        .with_kind(OpKind::Discovery)
    }

    // ── PATCH matrix (merge / strategic / json) over any kind ────────────

    /// PATCH an object with an `application/merge-patch+json` body (RFC 7386).
    #[must_use]
    pub fn merge_patch(ns: &str, plural: &str, name: &str, patch: Value) -> Self {
        Self::object(
            Self::ident("merge_patch/", name),
            HttpMethod::Patch,
            Self::ns_collection(ns, plural).seg(name).render(),
        )
        .with_body(patch, "application/merge-patch+json")
    }

    /// PATCH an object with an `application/strategic-merge-patch+json` body
    /// (K8s strategic merge — list-merge-by-key, `$patch: delete`,
    /// null-removes-key).
    #[must_use]
    pub fn strategic_patch(ns: &str, plural: &str, name: &str, patch: Value) -> Self {
        Self::object(
            Self::ident("strategic_patch/", name),
            HttpMethod::Patch,
            Self::ns_collection(ns, plural).seg(name).render(),
        )
        .with_body(patch, "application/strategic-merge-patch+json")
    }

    /// PATCH an object with an `application/json-patch+json` body (RFC 6902 —
    /// an op array).
    #[must_use]
    pub fn json_patch(ns: &str, plural: &str, name: &str, ops: Value) -> Self {
        Self::object(
            Self::ident("json_patch/", name),
            HttpMethod::Patch,
            Self::ns_collection(ns, plural).seg(name).render(),
        )
        .with_body(ops, "application/json-patch+json")
    }

    /// GET an object of any kind.
    #[must_use]
    pub fn get(ns: &str, plural: &str, name: &str) -> Self {
        Self::object(
            Self::ident("get/", name),
            HttpMethod::Get,
            Self::ns_collection(ns, plural).seg(name).render(),
        )
    }

    // ── Pod (the strategic-list-merge + status-subresource vehicle) ──────

    /// POST a minimal single-container Pod (container named `c`).
    #[must_use]
    pub fn create_pod(ns: &str, name: &str, image: &str) -> Self {
        Self::create_pod_containers(ns, name, &[("c", image)])
    }

    /// POST a Pod with the given `(container-name, image)` list — the
    /// vehicle for the strategic list-merge-by-key probe (patch ONE container
    /// by `name`, prove the others survive).
    #[must_use]
    pub fn create_pod_containers(ns: &str, name: &str, containers: &[(&str, &str)]) -> Self {
        let containers: Vec<Value> = containers
            .iter()
            .map(|(n, img)| json!({"name": n, "image": img}))
            .collect();
        Self::object(
            Self::ident("create_pod/", name),
            HttpMethod::Post,
            Self::ns_collection(ns, "pods").render(),
        )
        .with_body(
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": name},
                "spec": {"containers": containers},
            }),
            "application/json",
        )
    }

    /// DELETE a Pod (force, no grace, best-effort cleanup on the oracle).
    #[must_use]
    pub fn delete_pod(ns: &str, name: &str) -> Self {
        let mut path = Self::ns_collection(ns, "pods").seg(name).render();
        path.push_str("?gracePeriodSeconds=0");
        Self::object(Self::ident("delete_pod/", name), HttpMethod::Delete, path)
    }

    // ── status subresource ───────────────────────────────────────────────

    /// GET the `/status` subresource of an object.
    #[must_use]
    pub fn get_status(ns: &str, plural: &str, name: &str) -> Self {
        Self::object(
            Self::ident("get_status/", name),
            HttpMethod::Get,
            Self::ns_collection(ns, plural)
                .seg(name)
                .seg("status")
                .render(),
        )
    }

    /// PUT the `/status` subresource of an object.
    #[must_use]
    pub fn put_status(ns: &str, plural: &str, name: &str, body: Value) -> Self {
        Self::object(
            Self::ident("put_status/", name),
            HttpMethod::Put,
            Self::ns_collection(ns, plural)
                .seg(name)
                .seg("status")
                .render(),
        )
        .with_body(body, "application/json")
    }

    /// PUT (replace) the MAIN object of any kind.
    #[must_use]
    pub fn replace(ns: &str, plural: &str, name: &str, body: Value) -> Self {
        Self::object(
            Self::ident("replace/", name),
            HttpMethod::Put,
            Self::ns_collection(ns, plural).seg(name).render(),
        )
        .with_body(body, "application/json")
    }

    // ── LIST with a selector (label + field) ─────────────────────────────

    /// LIST a namespaced collection with an optional `labelSelector` and/or
    /// `fieldSelector`. Both selector strings are percent-encoded through the
    /// typed [`pct_encode`] (never a raw `format!()` of the query), so the
    /// full k8s selector grammar (`in (a,b)`, `!k`, `k!=v`) round-trips on the
    /// wire.
    #[must_use]
    pub fn list_selector(
        ns: &str,
        plural: &str,
        id_tag: &str,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
    ) -> Self {
        let mut path = Self::ns_collection(ns, plural).render();
        let mut sep = '?';
        if let Some(ls) = label_selector {
            path.push(sep);
            path.push_str("labelSelector=");
            path.push_str(&pct_encode(ls));
            sep = '&';
        }
        if let Some(fs) = field_selector {
            path.push(sep);
            path.push_str("fieldSelector=");
            path.push_str(&pct_encode(fs));
        }
        Self::object(Self::ident("list_selector/", id_tag), HttpMethod::Get, path)
            .with_kind(OpKind::List)
    }

    /// The body serialized to bytes, if present.
    #[must_use]
    pub fn body_bytes(&self) -> Option<Vec<u8>> {
        self.body
            .as_ref()
            .map(|b| serde_json::to_vec(b).unwrap_or_default())
    }
}

/// Percent-encode a query-parameter VALUE per RFC 3986 (encode everything
/// outside the unreserved set). Typed byte-wise encoder — NOT a `format!()`
/// of the query string; the hex digits come from a fixed table.
#[must_use]
pub fn pct_encode(s: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                out.push('%');
                out.push(HEX[(b >> 4) as usize] as char);
                out.push(HEX[(b & 0x0f) as usize] as char);
            }
        }
    }
    out
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

    #[test]
    fn patch_variants_carry_the_content_type() {
        assert_eq!(
            Operation::merge_patch("ns", "configmaps", "cm", json!({})).content_type,
            Some("application/merge-patch+json".into())
        );
        assert_eq!(
            Operation::strategic_patch("ns", "pods", "p", json!({})).content_type,
            Some("application/strategic-merge-patch+json".into())
        );
        assert_eq!(
            Operation::json_patch("ns", "pods", "p", json!([])).content_type,
            Some("application/json-patch+json".into())
        );
    }

    #[test]
    fn status_paths_render() {
        assert_eq!(
            Operation::get_status("ns", "pods", "p").path,
            "/api/v1/namespaces/ns/pods/p/status"
        );
        assert_eq!(
            Operation::put_status("ns", "pods", "p", json!({})).path,
            "/api/v1/namespaces/ns/pods/p/status"
        );
    }

    #[test]
    fn selector_query_is_percent_encoded() {
        let op =
            Operation::list_selector("ns", "configmaps", "in", Some("tier in (web,api)"), None);
        assert_eq!(
            op.path,
            "/api/v1/namespaces/ns/configmaps?labelSelector=tier%20in%20%28web%2Capi%29"
        );
        assert_eq!(op.kind, OpKind::List);
        let both = Operation::list_selector(
            "ns",
            "configmaps",
            "ne",
            Some("a!=b"),
            Some("metadata.name=x"),
        );
        assert_eq!(
            both.path,
            "/api/v1/namespaces/ns/configmaps?labelSelector=a%21%3Db&fieldSelector=metadata.name%3Dx"
        );
    }

    #[test]
    fn focus_is_carried() {
        let op = Operation::get("ns", "pods", "p")
            .with_focus(JsonPath::parse("spec.containers[0].image"));
        assert_eq!(
            op.focus.map(|p| p.to_string()),
            Some(".spec.containers[0].image".to_string())
        );
    }
}
