//! GitOps bootstrap — FluxCD + ArgoCD, both opt-in, both pointing at
//! typed sources. The bootstrap path emits manifests dropped into
//! `/var/lib/rancher/k3s/server/manifests/` which k3s auto-applies at
//! startup. Per Operating Principle #1, both controllers share the same
//! [`GitopsSource`] shape — same fields, different reconciler.

use serde::{Deserialize, Serialize};

/// Top-level GitOps bootstrap config.
///
/// Both [`Self::fluxcd`] and [`Self::argocd`] default to disabled.
/// Enabling either causes the renderer to emit:
///
/// 1. The controller install manifest (FluxCD or ArgoCD CRDs +
///    controllers + RBAC) into `/var/lib/rancher/k3s/server/manifests/`.
/// 2. A source-of-truth CR (GitRepository for Flux; Application for
///    Argo) pointing at the typed [`GitopsSource`].
/// 3. Optional secret-population manifests if [`GitopsSource::auth`]
///    is set — the secret content itself comes from sops-nix, but the
///    `kubernetes.io/secret` shape is rendered here.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "kebab-case", default)]
pub struct BootstrapConfig {
    /// FluxCD bootstrap. Disabled by default. Enabling installs
    /// flux-system controllers + a GitRepository pointing at the
    /// configured source.
    pub fluxcd: FluxcdBootstrap,

    /// ArgoCD bootstrap. Disabled by default. Enabling installs
    /// argocd controllers + an Application pointing at the source.
    pub argocd: ArgocdBootstrap,
}

/// FluxCD bootstrap config.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "kebab-case", default)]
pub struct FluxcdBootstrap {
    /// `true` ⇒ render the flux-system install manifest + the
    /// GitRepository + initial Kustomization.
    pub enable: bool,

    /// The Git source this cluster reconciles from.
    pub source: Option<GitopsSource>,

    /// Reconciliation interval for the GitSource (e.g. `1m`, `10m`).
    /// Default `1m` — k3s clusters typically iterate fast in dev.
    #[serde(default = "default_interval")]
    pub interval: String,

    /// Path within the repo containing the initial Kustomization.
    /// k8s-fleet convention: `./clusters/<name>`.
    #[serde(default = "default_flux_path")]
    pub path: String,

    /// FluxCD version to install. `latest` pulls upstream's most recent
    /// stable; a pinned version like `v2.3.0` is recommended for prod.
    #[serde(default = "default_flux_version")]
    pub version: String,
}

/// ArgoCD bootstrap config.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "kebab-case", default)]
pub struct ArgocdBootstrap {
    /// `true` ⇒ render the argocd install manifest + initial
    /// Application.
    pub enable: bool,

    /// The Git source this cluster reconciles from.
    pub source: Option<GitopsSource>,

    /// Initial Application's `targetRevision` (branch / tag / SHA).
    /// Defaults to the source's branch.
    pub target_revision: Option<String>,

    /// Path within the repo containing the initial Application's
    /// manifests. Default `./argocd`.
    #[serde(default = "default_argo_path")]
    pub path: String,

    /// Argo CD version. `latest` ⇒ upstream stable; pinned otherwise.
    #[serde(default = "default_argo_version")]
    pub version: String,
}

/// Git source for either GitOps controller.
///
/// Shared by FluxCD's `GitRepository` and ArgoCD's `Application.spec.source`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct GitopsSource {
    /// Repository URL — https or ssh.
    pub url: String,

    /// Branch / tag / ref. Default `main`.
    #[serde(default = "default_branch")]
    pub branch: String,

    /// Auth config. `None` ⇒ public repo, no auth header. `Some` ⇒
    /// renderer emits a `kubernetes.io/basic-auth` (https) or
    /// `kubernetes.io/ssh-auth` (ssh) Secret backed by sops-nix.
    pub auth: Option<SecretRef>,
}

/// Reference to a secret materialized by sops-nix on the VM.
///
/// The renderer emits the Kubernetes `Secret` manifest with a
/// placeholder; sops-nix's activation script substitutes the actual
/// value at `/var/lib/rancher/k3s/server/manifests/<secret>.yaml`
/// before k3s auto-applies. See `theory/SAGUAO.md` §III for the
/// secret-materialization shape.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct SecretRef {
    /// Auth type. `https-token` (the most common; uses
    /// `kubernetes.io/basic-auth` with `username: oauth2` +
    /// `password: <token>`); `ssh-key` (uses `kubernetes.io/ssh-auth`).
    pub kind: SecretKind,

    /// Path within sops where the credential value lives, e.g.
    /// `clusters/engenho-local/flux-github-token`. The VM-side
    /// sops-nix module declares the secret + nixos-k3s-vm wires it
    /// into the Secret manifest at render time.
    pub sops_key: String,
}

/// Kind of auth secret.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum SecretKind {
    /// HTTPS Personal Access Token. Renderer emits
    /// `kubernetes.io/basic-auth` Secret.
    HttpsToken,
    /// SSH deploy key. Renderer emits `kubernetes.io/ssh-auth` Secret.
    SshKey,
}

fn default_interval() -> String {
    "1m".to_string()
}
fn default_flux_path() -> String {
    "./clusters".to_string()
}
fn default_flux_version() -> String {
    "v2.3.0".to_string()
}
fn default_argo_path() -> String {
    "./argocd".to_string()
}
fn default_argo_version() -> String {
    "v2.13.0".to_string()
}
fn default_branch() -> String {
    "main".to_string()
}
