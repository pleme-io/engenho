//! Health + version endpoints — the probes kubectl / client-go / kubeadm
//! / controllers run BEFORE they will trust a server.
//!
//!   * `GET /version` → the `version.Info` JSON kubectl negotiates
//!     against. Absent → kubectl prints "couldn't get current server API
//!     group list" and refuses to talk to the server. This is the
//!     load-bearing reason a real kubectl rejected engenho before M0.4.
//!   * `GET /readyz`  → readiness gate (`kubectl wait`, kubeadm,
//!     controllers poll it).
//!   * `GET /livez`   → liveness gate.
//!   * `GET /healthz` → legacy health (older clients + many probes).
//!
//! Every response is a typed serde struct (`VersionInfo`) or a fixed
//! `"ok"` string — NEVER `serde_json::json!()` of an ad-hoc map (per the
//! ★★ TYPED EMISSION rule). The version is sourced from the SINGLE
//! [`engenho_types::KUBE_VERSION`] anchor so `/version`, discovery, and
//! the vendored OpenAPI surface can never drift.

use axum::Json;
use axum::response::IntoResponse;
use serde::Serialize;

use engenho_types::{KUBE_VERSION, KUBE_VERSION_MAJOR, KUBE_VERSION_MINOR};

/// Mirror of upstream `k8s.io/apimachinery/pkg/version.Info` — the JSON
/// body served at `GET /version`. Field names are camelCase per the K8s
/// wire contract (serde `rename` where Rust idiom diverges).
///
/// We are not a Go server, so `goVersion` is empty and `compiler` is
/// `rustc`; everything kubectl actually negotiates on (`major`, `minor`,
/// `gitVersion`) is sourced from [`engenho_types::KUBE_VERSION`].
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct VersionInfo {
    /// Major version (`"1"`).
    pub major: &'static str,
    /// Minor version (`"34"`).
    pub minor: &'static str,
    /// `vMAJOR.MINOR.PATCH` — what kubectl version-skew-checks against.
    #[serde(rename = "gitVersion")]
    pub git_version: &'static str,
    /// Commit the build was cut from. Empty until we wire a build-time
    /// stamp (no `unimplemented!()` — an empty string is a truthful
    /// "unknown commit", which kubectl tolerates).
    #[serde(rename = "gitCommit")]
    pub git_commit: &'static str,
    /// Working-tree state at build time. `clean` for released builds.
    #[serde(rename = "gitTreeState")]
    pub git_tree_state: &'static str,
    /// Build timestamp. Empty until a build-time stamp is wired.
    #[serde(rename = "buildDate")]
    pub build_date: &'static str,
    /// Go runtime version — empty: engenho is Rust, not Go.
    #[serde(rename = "goVersion")]
    pub go_version: &'static str,
    /// Compiler that produced the binary.
    pub compiler: &'static str,
    /// `<os>/<arch>` of the running binary.
    pub platform: &'static str,
}

impl VersionInfo {
    /// The version this build reports. Sources `major` / `minor` /
    /// `gitVersion` from the single [`engenho_types::KUBE_VERSION`]
    /// anchor; `platform` from the compile-time target triple.
    #[must_use]
    pub fn current() -> Self {
        Self {
            major: KUBE_VERSION_MAJOR,
            minor: KUBE_VERSION_MINOR,
            git_version: KUBE_VERSION,
            git_commit: "",
            git_tree_state: "clean",
            build_date: "",
            go_version: "",
            compiler: "rustc",
            // `<os>/<arch>` — the K8s `version.Info.platform` shape. The
            // pairs are enumerated as static literals (no `format!`, per
            // the ★★ TYPED EMISSION rule) keyed on the compile-time
            // target the binary was built for. Note rust's arch spelling
            // (`x86_64`/`aarch64`) differs from Go's (`amd64`/`arm64`);
            // kubectl only DISPLAYS platform (never parses it), so the
            // rust spelling is truthful + harmless.
            platform: match (std::env::consts::OS, std::env::consts::ARCH) {
                ("linux", "x86_64") => "linux/x86_64",
                ("linux", "aarch64") => "linux/aarch64",
                ("macos", "x86_64") => "darwin/x86_64",
                ("macos", "aarch64") => "darwin/aarch64",
                // Any target we haven't enumerated: a truthful fallback
                // (kubectl only displays it). Engenho's ship targets are
                // linux + darwin on x86_64 + aarch64.
                _ => "unknown/unknown",
            },
        }
    }
}

// ── axum route handlers ────────────────────────────────────────────────

/// `GET /version` → the typed [`VersionInfo`]. 200.
pub async fn version() -> impl IntoResponse {
    Json(VersionInfo::current())
}

/// `GET /readyz` → 200 `"ok"`. At M0.4 this is a simple readiness gate;
/// a later milestone can gate it on store leadership / controller sync.
pub async fn readyz() -> impl IntoResponse {
    "ok"
}

/// `GET /livez` → 200 `"ok"`.
pub async fn livez() -> impl IntoResponse {
    "ok"
}

/// `GET /healthz` → 200 `"ok"` (legacy health endpoint).
pub async fn healthz() -> impl IntoResponse {
    "ok"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_info_reports_vendored_surface() {
        let v = VersionInfo::current();
        assert_eq!(v.major, "1");
        assert_eq!(v.minor, "34");
        assert_eq!(v.git_version, "v1.34.0");
        // The drift guard: /version MUST equal the single KUBE_VERSION
        // anchor so it can never diverge from the generated_v1_34 surface.
        assert_eq!(v.git_version, engenho_types::KUBE_VERSION);
        assert_eq!(v.compiler, "rustc");
    }

    #[test]
    fn version_info_serializes_camel_case() {
        let v = VersionInfo::current();
        let json = serde_json::to_value(&v).unwrap();
        // camelCase field names per the K8s version.Info wire contract.
        assert_eq!(json.get("major").unwrap(), "1");
        assert_eq!(json.get("minor").unwrap(), "34");
        assert_eq!(json.get("gitVersion").unwrap(), "v1.34.0");
        assert_eq!(json.get("gitTreeState").unwrap(), "clean");
        assert_eq!(json.get("compiler").unwrap(), "rustc");
        // These keys (not snake_case) MUST be present for client-go.
        assert!(json.get("gitCommit").is_some());
        assert!(json.get("buildDate").is_some());
        assert!(json.get("goVersion").is_some());
        assert!(json.get("platform").is_some());
        // No snake_case leakage.
        assert!(json.get("git_version").is_none());
    }

    #[test]
    fn platform_is_os_slash_arch() {
        let p = VersionInfo::current().platform;
        assert!(p.contains('/'), "platform {p:?} must be <os>/<arch>");
    }
}
