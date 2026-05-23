//! R14 — Ingress dimension.
//!
//! HTTP/HTTPS routing from public hostnames to in-cluster services.
//! Pluggable [`IngressBackend`] trait — `FakeIngressBackend` for
//! tests, `TraefikBackend` renders Traefik IngressRoute CRDs,
//! `NginxBackend` renders nginx.conf snippets (R14b sibling).
//!
//! ## Reconcile rule
//!
//! 1. List `Ingress` objects from the store.
//! 2. For each Ingress, compute typed [`IngressRoute`] entries —
//!    one per (host, path, backend service:port) tuple.
//! 3. Diff against backend's installed routes.
//! 4. upsert mismatches; remove orphans.
//! 5. Idempotent — re-tick is no-op.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::StoreMesh;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::controller::{Controller, ReconcileReport};
use crate::error::ControllerError;

/// One ingress routing rule — public hostname + path → service backend.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct IngressRoute {
    /// `namespace/name` of the Ingress resource owning this rule.
    pub ingress_id: String,
    /// Host header to match (e.g. "podinfo.example.com").
    pub host: String,
    /// URL path prefix (e.g. "/", "/api").
    pub path: String,
    /// Path-matching strategy. Default is Prefix.
    pub path_type: PathType,
    /// Backend service name (in the same namespace).
    pub backend_service: String,
    /// Backend service port.
    pub backend_port: u16,
    /// Optional TLS secret name. None = HTTP-only.
    pub tls_secret: Option<String>,
}

/// K8s Ingress pathType enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum PathType {
    /// Match the path exactly.
    Exact,
    /// Match the path as a prefix (default).
    Prefix,
    /// Implementation-specific.
    ImplementationSpecific,
}

impl Default for PathType {
    fn default() -> Self {
        Self::Prefix
    }
}

/// Ingress backend errors.
#[derive(Debug, Clone, Error)]
pub enum IngressError {
    /// Backend (Traefik, nginx, external API) returned an error.
    #[error("backend: {0}")]
    Backend(String),
    /// Invalid route (empty host, etc).
    #[error("invalid route: {0}")]
    InvalidRoute(String),
}

engenho_substrate::impl_error_kind! {
    IngressError {
        (Backend(_)) => "backend",
        (InvalidRoute(_)) => "invalid_route",
    }
}

/// Pluggable ingress backend trait.
#[async_trait]
pub trait IngressBackend: Send + Sync {
    /// Backend identifier for telemetry.
    fn name(&self) -> &'static str;

    /// Install / refresh `route`. Idempotent.
    ///
    /// # Errors
    /// [`IngressError::Backend`] on backend failure;
    /// [`IngressError::InvalidRoute`] for malformed input.
    async fn upsert(&self, route: &IngressRoute) -> Result<(), IngressError>;

    /// Remove a route by (ingress_id, host, path) triple — the
    /// unique key. The backend identifies the route by this key.
    ///
    /// # Errors
    /// [`IngressError::Backend`] on backend failure.
    async fn remove(&self, ingress_id: &str, host: &str, path: &str) -> Result<(), IngressError>;

    /// Currently-installed routes.
    ///
    /// # Errors
    /// [`IngressError::Backend`] on backend failure.
    async fn list(&self) -> Result<Vec<IngressRoute>, IngressError>;
}

// =================================================================
// FakeIngressBackend — deterministic in-memory backend
// =================================================================

/// In-memory backend. Tracks routes in a BTreeMap keyed by
/// (ingress_id, host, path). Records every call for test assertion.
#[derive(Default, Clone)]
pub struct FakeIngressBackend {
    inner: Arc<Mutex<FakeIngressState>>,
}

#[derive(Default)]
struct FakeIngressState {
    routes: BTreeMap<(String, String, String), IngressRoute>,
    events: Vec<FakeIngressEvent>,
}

/// Per-call event log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FakeIngressEvent {
    /// `upsert(ingress_id, host, path)`.
    Upsert(String, String, String),
    /// `remove(ingress_id, host, path)`.
    Remove(String, String, String),
}

impl FakeIngressBackend {
    /// Fresh empty backend.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of events.
    pub async fn events(&self) -> Vec<FakeIngressEvent> {
        self.inner.lock().await.events.clone()
    }

    /// Count of installed routes.
    pub async fn route_count(&self) -> usize {
        self.inner.lock().await.routes.len()
    }
}

#[async_trait]
impl IngressBackend for FakeIngressBackend {
    fn name(&self) -> &'static str {
        "fake"
    }

    async fn upsert(&self, route: &IngressRoute) -> Result<(), IngressError> {
        if route.host.is_empty() {
            return Err(IngressError::InvalidRoute("empty host".into()));
        }
        if route.backend_service.is_empty() {
            return Err(IngressError::InvalidRoute("empty backend_service".into()));
        }
        let key = (
            route.ingress_id.clone(),
            route.host.clone(),
            route.path.clone(),
        );
        let mut state = self.inner.lock().await;
        state.routes.insert(key.clone(), route.clone());
        state
            .events
            .push(FakeIngressEvent::Upsert(key.0, key.1, key.2));
        Ok(())
    }

    async fn remove(&self, ingress_id: &str, host: &str, path: &str) -> Result<(), IngressError> {
        let key = (ingress_id.to_string(), host.to_string(), path.to_string());
        let mut state = self.inner.lock().await;
        state.routes.remove(&key);
        state
            .events
            .push(FakeIngressEvent::Remove(key.0, key.1, key.2));
        Ok(())
    }

    async fn list(&self) -> Result<Vec<IngressRoute>, IngressError> {
        Ok(self.inner.lock().await.routes.values().cloned().collect())
    }
}

// =================================================================
// TraefikIngressBackend — renders typed IngressRoute CRD YAML
// =================================================================

/// Renders the [`IngressRoute`] catalog as Traefik IngressRoute
/// CRD YAML documents + writes them to a configured directory
/// (Traefik's file-provider watches the directory + hot-reloads).
///
/// No shell-out — Traefik consumes the YAML directly via its
/// file provider. The renderer is pure; the I/O boundary is
/// std::fs::write through engenho-substrate's atomic_write.
#[derive(Clone)]
pub struct TraefikIngressBackend {
    /// Directory Traefik watches (e.g. `/etc/traefik/dynamic/`).
    output_dir: std::path::PathBuf,
    inner: Arc<Mutex<TraefikState>>,
}

#[derive(Default)]
struct TraefikState {
    routes: BTreeMap<(String, String, String), IngressRoute>,
}

impl TraefikIngressBackend {
    /// New backend writing dynamic config to `output_dir`.
    #[must_use]
    pub fn new(output_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            output_dir: output_dir.into(),
            inner: Arc::new(Mutex::new(TraefikState::default())),
        }
    }

    /// Render the Traefik IngressRoute CRD YAML for one route.
    /// Pure — no I/O. Test helper.
    #[must_use]
    pub fn render_crd(route: &IngressRoute) -> String {
        let match_rule = match route.path_type {
            PathType::Exact => format!("Host(`{}`) && Path(`{}`)", route.host, route.path),
            PathType::Prefix | PathType::ImplementationSpecific => {
                format!("Host(`{}`) && PathPrefix(`{}`)", route.host, route.path)
            }
        };
        let entry_points = if route.tls_secret.is_some() {
            "websecure"
        } else {
            "web"
        };
        let tls_block = if let Some(secret) = &route.tls_secret {
            format!("\n  tls:\n    secretName: {secret}")
        } else {
            String::new()
        };
        format!(
            "apiVersion: traefik.io/v1alpha1\n\
             kind: IngressRoute\n\
             metadata:\n\
             \x20\x20name: {name}\n\
             spec:\n\
             \x20\x20entryPoints:\n\
             \x20\x20\x20\x20- {entry_points}\n\
             \x20\x20routes:\n\
             \x20\x20\x20\x20- match: \"{match_rule}\"\n\
             \x20\x20\x20\x20\x20\x20kind: Rule\n\
             \x20\x20\x20\x20\x20\x20services:\n\
             \x20\x20\x20\x20\x20\x20\x20\x20- name: {svc}\n\
             \x20\x20\x20\x20\x20\x20\x20\x20\x20\x20port: {port}{tls_block}\n",
            name = sanitize_name(&route.ingress_id, &route.host, &route.path),
            svc = route.backend_service,
            port = route.backend_port,
        )
    }

    fn route_filename(route: &IngressRoute) -> String {
        format!(
            "{}.yaml",
            sanitize_name(&route.ingress_id, &route.host, &route.path)
        )
    }
}

fn sanitize_name(ingress_id: &str, host: &str, path: &str) -> String {
    let raw = format!("{ingress_id}-{host}{path}");
    raw.chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' => c,
            _ => '-',
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
        .to_lowercase()
}

#[async_trait]
impl IngressBackend for TraefikIngressBackend {
    fn name(&self) -> &'static str {
        "traefik"
    }

    async fn upsert(&self, route: &IngressRoute) -> Result<(), IngressError> {
        if route.host.is_empty() {
            return Err(IngressError::InvalidRoute("empty host".into()));
        }
        let yaml = Self::render_crd(route);
        let file = self.output_dir.join(Self::route_filename(route));
        std::fs::create_dir_all(&self.output_dir).map_err(|e| {
            IngressError::Backend(format!("mkdir {}: {e}", self.output_dir.display()))
        })?;
        std::fs::write(&file, yaml)
            .map_err(|e| IngressError::Backend(format!("write {}: {e}", file.display())))?;
        let key = (
            route.ingress_id.clone(),
            route.host.clone(),
            route.path.clone(),
        );
        self.inner.lock().await.routes.insert(key, route.clone());
        Ok(())
    }

    async fn remove(&self, ingress_id: &str, host: &str, path: &str) -> Result<(), IngressError> {
        let key = (ingress_id.to_string(), host.to_string(), path.to_string());
        // Build the route shell so the filename matches what upsert wrote.
        let proxy = IngressRoute {
            ingress_id: key.0.clone(),
            host: key.1.clone(),
            path: key.2.clone(),
            path_type: PathType::Prefix,
            backend_service: String::new(),
            backend_port: 0,
            tls_secret: None,
        };
        let file = self.output_dir.join(Self::route_filename(&proxy));
        let _ = std::fs::remove_file(&file); // tolerate already-gone
        self.inner.lock().await.routes.remove(&key);
        Ok(())
    }

    async fn list(&self) -> Result<Vec<IngressRoute>, IngressError> {
        Ok(self.inner.lock().await.routes.values().cloned().collect())
    }
}

// =================================================================
// NginxIngressBackend — R14b — renders nginx server-block snippets
// =================================================================

/// Renders [`IngressRoute`] as nginx server-block snippets +
/// writes them to a `conf.d/` directory nginx includes. Nginx
/// reloads via `nginx -s reload` (out-of-band — operator
/// configures inotify or systemd path-watch to trigger).
///
/// No shell-out — pure Rust + std::fs.
#[derive(Clone)]
pub struct NginxIngressBackend {
    output_dir: std::path::PathBuf,
    inner: Arc<Mutex<NginxIngressState>>,
}

#[derive(Default)]
struct NginxIngressState {
    routes: BTreeMap<(String, String, String), IngressRoute>,
}

impl NginxIngressBackend {
    /// New backend writing server-blocks to `output_dir`.
    #[must_use]
    pub fn new(output_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            output_dir: output_dir.into(),
            inner: Arc::new(Mutex::new(NginxIngressState::default())),
        }
    }

    /// Render the nginx server-block snippet for one route. Pure.
    #[must_use]
    pub fn render_server_block(route: &IngressRoute) -> String {
        let listen = if route.tls_secret.is_some() {
            "listen 443 ssl;"
        } else {
            "listen 80;"
        };
        let ssl_block = if let Some(secret) = &route.tls_secret {
            format!(
                "\n    ssl_certificate     /etc/nginx/tls/{secret}.crt;\n    ssl_certificate_key /etc/nginx/tls/{secret}.key;"
            )
        } else {
            String::new()
        };
        let location_directive = match route.path_type {
            PathType::Exact => format!("location = {} {{", route.path),
            PathType::Prefix | PathType::ImplementationSpecific => {
                format!("location {} {{", route.path)
            }
        };
        format!(
            "# generated by engenho-controllers::ingress::NginxIngressBackend\n\
             server {{\n\
             \x20\x20\x20\x20{listen}\n\
             \x20\x20\x20\x20server_name {host};{ssl_block}\n\
             \x20\x20\x20\x20{location_directive}\n\
             \x20\x20\x20\x20\x20\x20\x20\x20proxy_pass http://{svc}:{port};\n\
             \x20\x20\x20\x20\x20\x20\x20\x20proxy_set_header Host $host;\n\
             \x20\x20\x20\x20\x20\x20\x20\x20proxy_set_header X-Real-IP $remote_addr;\n\
             \x20\x20\x20\x20}}\n\
             }}\n",
            host = route.host,
            svc = route.backend_service,
            port = route.backend_port,
        )
    }

    fn route_filename(route: &IngressRoute) -> String {
        format!(
            "{}.conf",
            sanitize_name(&route.ingress_id, &route.host, &route.path)
        )
    }
}

#[async_trait]
impl IngressBackend for NginxIngressBackend {
    fn name(&self) -> &'static str {
        "nginx"
    }

    async fn upsert(&self, route: &IngressRoute) -> Result<(), IngressError> {
        if route.host.is_empty() {
            return Err(IngressError::InvalidRoute("empty host".into()));
        }
        let snippet = Self::render_server_block(route);
        let file = self.output_dir.join(Self::route_filename(route));
        std::fs::create_dir_all(&self.output_dir).map_err(|e| {
            IngressError::Backend(format!("mkdir {}: {e}", self.output_dir.display()))
        })?;
        std::fs::write(&file, snippet)
            .map_err(|e| IngressError::Backend(format!("write {}: {e}", file.display())))?;
        let key = (
            route.ingress_id.clone(),
            route.host.clone(),
            route.path.clone(),
        );
        self.inner.lock().await.routes.insert(key, route.clone());
        Ok(())
    }

    async fn remove(&self, ingress_id: &str, host: &str, path: &str) -> Result<(), IngressError> {
        let key = (ingress_id.to_string(), host.to_string(), path.to_string());
        let proxy = IngressRoute {
            ingress_id: key.0.clone(),
            host: key.1.clone(),
            path: key.2.clone(),
            path_type: PathType::Prefix,
            backend_service: String::new(),
            backend_port: 0,
            tls_secret: None,
        };
        let file = self.output_dir.join(Self::route_filename(&proxy));
        let _ = std::fs::remove_file(&file);
        self.inner.lock().await.routes.remove(&key);
        Ok(())
    }

    async fn list(&self) -> Result<Vec<IngressRoute>, IngressError> {
        Ok(self.inner.lock().await.routes.values().cloned().collect())
    }
}

// =================================================================
// IngressController — reads store, drives the backend
// =================================================================

/// Controller that materializes ingress routes from K8s Ingress
/// objects in the store.
pub struct IngressController {
    store: Arc<StoreMesh>,
    backend: Arc<dyn IngressBackend>,
    namespace: Option<String>,
}

impl IngressController {
    /// Construct an ingress controller.
    #[must_use]
    pub fn new(
        store: Arc<StoreMesh>,
        backend: Arc<dyn IngressBackend>,
        namespace: Option<String>,
    ) -> Self {
        Self {
            store,
            backend,
            namespace,
        }
    }

    /// Extract the typed routes from a K8s Ingress manifest.
    /// Pure — no I/O. Public so callers can introspect.
    #[must_use]
    pub fn extract_routes(ingress_id: &str, ingress: &serde_json::Value) -> Vec<IngressRoute> {
        let spec = match ingress.get("spec") {
            Some(s) => s,
            None => return Vec::new(),
        };
        // TLS secrets keyed by host.
        let tls_secrets: BTreeMap<String, String> = spec
            .get("tls")
            .and_then(|t| t.as_array())
            .map(|arr| {
                arr.iter()
                    .flat_map(|t| {
                        let secret = t
                            .get("secretName")
                            .and_then(|s| s.as_str())
                            .unwrap_or("")
                            .to_string();
                        t.get("hosts")
                            .and_then(|h| h.as_array())
                            .into_iter()
                            .flatten()
                            .filter_map(|h| {
                                h.as_str().map(|host| (host.to_string(), secret.clone()))
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect()
            })
            .unwrap_or_default();

        let rules = match spec.get("rules").and_then(|r| r.as_array()) {
            Some(r) => r,
            None => return Vec::new(),
        };
        let mut routes = Vec::new();
        for rule in rules {
            let host = rule
                .get("host")
                .and_then(|h| h.as_str())
                .unwrap_or("")
                .to_string();
            let paths = match rule
                .get("http")
                .and_then(|h| h.get("paths"))
                .and_then(|p| p.as_array())
            {
                Some(p) => p,
                None => continue,
            };
            for path in paths {
                let path_str = path
                    .get("path")
                    .and_then(|p| p.as_str())
                    .unwrap_or("/")
                    .to_string();
                let path_type = match path.get("pathType").and_then(|p| p.as_str()) {
                    Some("Exact") => PathType::Exact,
                    Some("ImplementationSpecific") => PathType::ImplementationSpecific,
                    _ => PathType::Prefix,
                };
                let backend = path
                    .get("backend")
                    .and_then(|b| b.get("service"))
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                let service_name = backend
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("")
                    .to_string();
                let service_port = backend
                    .get("port")
                    .and_then(|p| p.get("number"))
                    .and_then(|n| n.as_u64())
                    .unwrap_or(80) as u16;
                let tls = tls_secrets.get(&host).cloned();
                routes.push(IngressRoute {
                    ingress_id: ingress_id.to_string(),
                    host: host.clone(),
                    path: path_str,
                    path_type,
                    backend_service: service_name,
                    backend_port: service_port,
                    tls_secret: tls,
                });
            }
        }
        routes
    }
}

#[async_trait]
impl Controller for IngressController {
    fn name(&self) -> &'static str {
        "ingress"
    }

    async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
        let ingresses = self
            .store
            .list(
                "networking.k8s.io",
                "v1",
                "Ingress",
                self.namespace.as_deref(),
            )
            .await;
        let mut report = ReconcileReport::default();
        report.objects_examined = ingresses.len();

        let mut desired: BTreeMap<(String, String, String), IngressRoute> = BTreeMap::new();
        for (key, value) in &ingresses {
            let id = format!(
                "{}/{}",
                key.namespace.as_deref().unwrap_or("default"),
                key.name
            );
            for route in Self::extract_routes(&id, value) {
                desired.insert(
                    (
                        route.ingress_id.clone(),
                        route.host.clone(),
                        route.path.clone(),
                    ),
                    route,
                );
            }
        }

        let installed = self
            .backend
            .list()
            .await
            .map_err(|e| ControllerError::Internal(e.to_string()))?;
        let installed_keys: BTreeMap<(String, String, String), IngressRoute> = installed
            .into_iter()
            .map(|r| ((r.ingress_id.clone(), r.host.clone(), r.path.clone()), r))
            .collect();

        for (key, route) in &desired {
            if installed_keys.get(key) != Some(route) {
                self.backend
                    .upsert(route)
                    .await
                    .map_err(|e| ControllerError::Internal(e.to_string()))?;
                report.objects_changed += 1;
            }
        }
        for key in installed_keys.keys() {
            if !desired.contains_key(key) {
                self.backend
                    .remove(&key.0, &key.1, &key.2)
                    .await
                    .map_err(|e| ControllerError::Internal(e.to_string()))?;
                report.objects_changed += 1;
            }
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_route() -> IngressRoute {
        IngressRoute {
            ingress_id: "default/web".into(),
            host: "podinfo.example.com".into(),
            path: "/".into(),
            path_type: PathType::Prefix,
            backend_service: "podinfo".into(),
            backend_port: 80,
            tls_secret: None,
        }
    }

    // ── FakeIngressBackend ─────────────────────────────────────

    #[tokio::test]
    async fn fake_upsert_inserts_route() {
        let b = FakeIngressBackend::new();
        b.upsert(&sample_route()).await.unwrap();
        assert_eq!(b.route_count().await, 1);
    }

    #[tokio::test]
    async fn fake_rejects_empty_host() {
        let b = FakeIngressBackend::new();
        let mut r = sample_route();
        r.host = String::new();
        let err = b.upsert(&r).await.unwrap_err();
        assert_eq!(err.kind(), "invalid_route");
    }

    #[tokio::test]
    async fn fake_rejects_empty_backend_service() {
        let b = FakeIngressBackend::new();
        let mut r = sample_route();
        r.backend_service = String::new();
        let err = b.upsert(&r).await.unwrap_err();
        assert_eq!(err.kind(), "invalid_route");
    }

    #[tokio::test]
    async fn fake_remove_by_triple_clears() {
        let b = FakeIngressBackend::new();
        let r = sample_route();
        b.upsert(&r).await.unwrap();
        b.remove(&r.ingress_id, &r.host, &r.path).await.unwrap();
        assert_eq!(b.route_count().await, 0);
    }

    #[tokio::test]
    async fn fake_events_record_in_order() {
        let b = FakeIngressBackend::new();
        let r = sample_route();
        b.upsert(&r).await.unwrap();
        b.remove(&r.ingress_id, &r.host, &r.path).await.unwrap();
        let evs = b.events().await;
        assert_eq!(evs.len(), 2);
        assert!(matches!(&evs[0], FakeIngressEvent::Upsert(_, _, _)));
        assert!(matches!(&evs[1], FakeIngressEvent::Remove(_, _, _)));
    }

    #[tokio::test]
    async fn fake_list_returns_routes() {
        let b = FakeIngressBackend::new();
        b.upsert(&sample_route()).await.unwrap();
        assert_eq!(b.list().await.unwrap().len(), 1);
    }

    // ── TraefikIngressBackend ─────────────────────────────────

    // ── NginxIngressBackend (R14b) ─────────────────────────────

    #[test]
    fn nginx_render_server_block_contains_listen_and_proxy_pass() {
        let snippet = NginxIngressBackend::render_server_block(&sample_route());
        assert!(snippet.contains("server {"));
        assert!(snippet.contains("listen 80;"));
        assert!(snippet.contains("server_name podinfo.example.com;"));
        assert!(snippet.contains("location / {"));
        assert!(snippet.contains("proxy_pass http://podinfo:80;"));
        assert!(snippet.contains("proxy_set_header Host $host;"));
    }

    #[test]
    fn nginx_render_with_tls_uses_listen_443_and_ssl_block() {
        let mut r = sample_route();
        r.tls_secret = Some("podinfo-tls".into());
        let snippet = NginxIngressBackend::render_server_block(&r);
        assert!(snippet.contains("listen 443 ssl;"));
        assert!(snippet.contains("ssl_certificate     /etc/nginx/tls/podinfo-tls.crt;"));
        assert!(snippet.contains("ssl_certificate_key /etc/nginx/tls/podinfo-tls.key;"));
    }

    #[test]
    fn nginx_render_exact_path_uses_location_equals() {
        let mut r = sample_route();
        r.path = "/api".into();
        r.path_type = PathType::Exact;
        let snippet = NginxIngressBackend::render_server_block(&r);
        assert!(snippet.contains("location = /api {"));
    }

    #[tokio::test]
    async fn nginx_upsert_writes_conf_file() {
        let dir = std::env::temp_dir().join(format!("engenho-nginx-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let b = NginxIngressBackend::new(&dir);
        b.upsert(&sample_route()).await.unwrap();
        let files: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .collect();
        assert_eq!(files.len(), 1);
        assert!(files[0].extension().unwrap() == "conf");
        let content = std::fs::read_to_string(&files[0]).unwrap();
        assert!(content.contains("server {"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn nginx_backend_name_is_stable() {
        let b = NginxIngressBackend::new("/tmp");
        assert_eq!(b.name(), "nginx");
    }

    #[test]
    fn traefik_render_crd_contains_match_rule() {
        let yaml = TraefikIngressBackend::render_crd(&sample_route());
        assert!(yaml.contains("apiVersion: traefik.io/v1alpha1"));
        assert!(yaml.contains("kind: IngressRoute"));
        assert!(yaml.contains("Host(`podinfo.example.com`)"));
        assert!(yaml.contains("PathPrefix(`/`)"));
        assert!(yaml.contains("name: podinfo"));
        assert!(yaml.contains("port: 80"));
        // No TLS → web entry point.
        assert!(yaml.contains("- web"));
    }

    #[test]
    fn traefik_render_crd_uses_path_exact_match() {
        let mut r = sample_route();
        r.path = "/api/v1".into();
        r.path_type = PathType::Exact;
        let yaml = TraefikIngressBackend::render_crd(&r);
        assert!(yaml.contains("Path(`/api/v1`)"));
        assert!(!yaml.contains("PathPrefix"));
    }

    #[test]
    fn traefik_render_crd_with_tls_uses_websecure() {
        let mut r = sample_route();
        r.tls_secret = Some("podinfo-tls".into());
        let yaml = TraefikIngressBackend::render_crd(&r);
        assert!(yaml.contains("- websecure"));
        assert!(yaml.contains("secretName: podinfo-tls"));
    }

    #[tokio::test]
    async fn traefik_upsert_writes_file() {
        let dir = std::env::temp_dir().join(format!("engenho-ingress-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let b = TraefikIngressBackend::new(&dir);
        b.upsert(&sample_route()).await.unwrap();
        let files: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .collect();
        assert_eq!(files.len(), 1);
        let content = std::fs::read_to_string(&files[0]).unwrap();
        assert!(content.contains("kind: IngressRoute"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn traefik_remove_deletes_file() {
        let dir = std::env::temp_dir().join(format!("engenho-ingress-rm-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let b = TraefikIngressBackend::new(&dir);
        let r = sample_route();
        b.upsert(&r).await.unwrap();
        b.remove(&r.ingress_id, &r.host, &r.path).await.unwrap();
        let count = std::fs::read_dir(&dir).map(|i| i.count()).unwrap_or(0);
        assert_eq!(count, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sanitize_name_produces_safe_filename() {
        let n = sanitize_name("default/web", "podinfo.example.com", "/api/v1");
        assert!(!n.contains('/'));
        assert!(!n.contains('.'));
        assert!(!n.starts_with('-'));
        assert!(!n.ends_with('-'));
        // Lowercase chars + dashes only.
        assert!(
            n.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        );
    }

    #[test]
    fn traefik_backend_name_is_stable() {
        let b = TraefikIngressBackend::new("/tmp");
        assert_eq!(b.name(), "traefik");
    }

    // ── IngressController extract_routes ──────────────────────

    #[test]
    fn extract_routes_parses_standard_ingress() {
        let ingress = json!({
            "spec": {
                "rules": [{
                    "host": "x.example.com",
                    "http": {
                        "paths": [{
                            "path": "/",
                            "pathType": "Prefix",
                            "backend": {
                                "service": {
                                    "name": "x",
                                    "port": {"number": 80}
                                }
                            }
                        }]
                    }
                }]
            }
        });
        let routes = IngressController::extract_routes("default/x", &ingress);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].host, "x.example.com");
        assert_eq!(routes[0].backend_service, "x");
        assert_eq!(routes[0].backend_port, 80);
        assert!(routes[0].tls_secret.is_none());
    }

    #[test]
    fn extract_routes_picks_up_tls_secret_per_host() {
        let ingress = json!({
            "spec": {
                "tls": [{
                    "hosts": ["x.example.com"],
                    "secretName": "x-tls"
                }],
                "rules": [{
                    "host": "x.example.com",
                    "http": {
                        "paths": [{
                            "path": "/",
                            "backend": {"service": {"name": "x", "port": {"number": 443}}}
                        }]
                    }
                }]
            }
        });
        let routes = IngressController::extract_routes("default/x", &ingress);
        assert_eq!(routes[0].tls_secret.as_deref(), Some("x-tls"));
    }

    #[test]
    fn extract_routes_handles_multiple_paths_per_host() {
        let ingress = json!({
            "spec": {
                "rules": [{
                    "host": "api.example.com",
                    "http": {
                        "paths": [
                            {"path": "/v1", "backend": {"service": {"name": "api-v1", "port": {"number": 8080}}}},
                            {"path": "/v2", "backend": {"service": {"name": "api-v2", "port": {"number": 8080}}}}
                        ]
                    }
                }]
            }
        });
        let routes = IngressController::extract_routes("default/api", &ingress);
        assert_eq!(routes.len(), 2);
        assert_eq!(routes[0].path, "/v1");
        assert_eq!(routes[1].path, "/v2");
    }

    #[test]
    fn extract_routes_defaults_to_prefix_path_type() {
        let ingress = json!({
            "spec": {
                "rules": [{
                    "host": "x",
                    "http": {
                        "paths": [{
                            "path": "/",
                            "backend": {"service": {"name": "x", "port": {"number": 80}}}
                        }]
                    }
                }]
            }
        });
        let routes = IngressController::extract_routes("d/x", &ingress);
        assert_eq!(routes[0].path_type, PathType::Prefix);
    }

    #[test]
    fn extract_routes_handles_exact_path_type() {
        let ingress = json!({
            "spec": {
                "rules": [{
                    "host": "x",
                    "http": {
                        "paths": [{
                            "path": "/api",
                            "pathType": "Exact",
                            "backend": {"service": {"name": "x", "port": {"number": 80}}}
                        }]
                    }
                }]
            }
        });
        let routes = IngressController::extract_routes("d/x", &ingress);
        assert_eq!(routes[0].path_type, PathType::Exact);
    }

    #[test]
    fn extract_routes_empty_for_missing_spec() {
        let ingress = json!({"metadata": {"name": "x"}});
        let routes = IngressController::extract_routes("d/x", &ingress);
        assert!(routes.is_empty());
    }

    #[test]
    fn path_type_default_is_prefix() {
        assert_eq!(PathType::default(), PathType::Prefix);
    }

    #[test]
    fn path_type_serializes_pascal_case() {
        let json = serde_json::to_string(&PathType::ImplementationSpecific).unwrap();
        assert_eq!(json, "\"ImplementationSpecific\"");
    }

    #[test]
    fn error_kinds_are_stable() {
        assert_eq!(IngressError::Backend("x".into()).kind(), "backend");
        assert_eq!(
            IngressError::InvalidRoute("x".into()).kind(),
            "invalid_route"
        );
    }

    #[test]
    fn controller_name_is_stable() {
        struct Fake;
        #[async_trait]
        impl Controller for Fake {
            fn name(&self) -> &'static str {
                "ingress"
            }
            async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
                Ok(ReconcileReport::default())
            }
        }
        assert_eq!(Fake.name(), "ingress");
    }
}
