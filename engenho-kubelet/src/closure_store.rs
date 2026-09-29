//! The Nix store a native pod's image lives in: fetch before spawn, and root
//! what an accepted pod references so garbage collection cannot take it.
//!
//! ## The incident this exists for
//!
//! Measured on a native node when the daemon restarted from 0.53.118 onto
//! 0.53.119: the node's `DaemonSet`s were re-rendered with the new closure,
//! the eight existing pods still named `nix:/nix/store/p27v2…-0.53.118`, and a
//! routine `nix-collect-garbage` removed that closure. Nothing had rooted it —
//! it had survived only because the previous system generation referenced
//! it. The pods then sat `ContainerCreating` for five and a half hours: a
//! spawn of a program that no longer existed, retried on the start curve
//! forever.
//!
//! Three things close that class, and this module is the seam for all three:
//!
//! * **Roots.** Every closure a pod bound to this node names gets an INDIRECT
//!   GC root: a symlink under the kubelet's own state directory, registered
//!   with the store (`nix-store --realise <path> --add-root <link>` puts the
//!   registration under `/nix/var/nix/gcroots/auto`). The kubelet reconciles
//!   the set every tick, so the root is released once no pod bound here
//!   names the closure — deleted pod, rolled pod, or a pod that left the
//!   node while the daemon was down.
//! * **Fetch before spawn.** A closure missing from the store is realised
//!   from the store's configured substituters first — the same call as the
//!   root.
//! * **A typed verdict.** A closure that is missing and cannot be realised is
//!   [`ClosureState::Unavailable`], naming the path, and the kubelet publishes
//!   the pod `Failed` with reason `ImageUnavailable` instead of retrying a
//!   spawn that cannot succeed. A controller that keeps one pod per slot then
//!   replaces it from its current template.
//!
//! ## Which layer owns it, and why
//!
//! The kubelet. A Nix store is per node, so the question "which closures does
//! this node need" has exactly one answer-holder: the component that lists
//! the pods bound to the node. A controller sees templates cluster-wide and no
//! store; the backend sees one container at a time and forgets everything on
//! a restart, while roots must outlive the daemon (they are files). The roots
//! are level-triggered from the pod list, so a restart of the daemon neither
//! loses nor leaks one.
//!
//! Tier, stated plainly: a pod's closure is rooted from the first tick that
//! sees it bound here. A closure referenced only by a TEMPLATE that has no pod
//! on this node yet is not rooted — the node does not need it until a pod is
//! created, and the fetch-before-spawn above realises it then.
//!
//! The store is spoken to through `nix-store` rather than the daemon protocol:
//! the Nix store is a fact about the world with its own daemon and its own
//! locking, and `--add-root` is its supported interface for an indirect root.
//! [`UnmanagedClosures`] is the default, so a kubelet with no store (every
//! non-native backend, and tests) behaves exactly as before.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// The name of one GC root: `<pod-uid>.<container>`, filesystem-safe.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RootName(String);

impl RootName {
    /// The root for `container` of the pod `pod_identity` (its uid, or
    /// `namespace_name` when it has none).
    #[must_use]
    pub fn new(pod_identity: &str, container: &str) -> Self {
        let clean = |s: &str| {
            s.chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                        c
                    } else {
                        '_'
                    }
                })
                .collect::<String>()
        };
        Self(format!("{}.{}", clean(pod_identity), clean(container)))
    }

    /// Wrap a name read back from the root directory.
    #[must_use]
    pub fn from_file_name(name: &str) -> Self {
        Self(name.to_string())
    }

    /// The file name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// What asking for a closure came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClosureState {
    /// In the store and rooted.
    Rooted,
    /// In the store, but the root could not be added. The pod may run; the
    /// closure is exposed to garbage collection until a later tick roots it.
    PresentUnrooted {
        /// What the store said.
        detail: String,
    },
    /// Not in the store, and realising it from the substituters failed. The
    /// pod cannot run on this node.
    Unavailable {
        /// The closure.
        path: PathBuf,
        /// What the store said.
        detail: String,
    },
    /// This kubelet manages no store: nothing was checked, fetched or rooted.
    Unmanaged,
}

/// The node's closure store.
#[async_trait::async_trait]
pub trait ClosureStore: Send + Sync {
    /// Stable identifier for telemetry.
    fn name(&self) -> &'static str;

    /// Make `closure` present (fetching it when it is missing) and rooted
    /// under `root`.
    async fn ensure(&self, root: &RootName, closure: &Path) -> ClosureState;

    /// Whether `closure` is in the store now. Cheap: no fetch.
    fn is_present(&self, closure: &Path) -> bool;

    /// Every root this store holds, and the closure each points at.
    ///
    /// # Errors
    /// The root directory could not be read.
    fn roots(&self) -> std::io::Result<BTreeMap<RootName, PathBuf>>;

    /// Drop `root`. The closure becomes collectable once nothing else roots
    /// it.
    ///
    /// # Errors
    /// The root could not be removed.
    fn release(&self, root: &RootName) -> std::io::Result<()>;
}

/// No store: the behaviour before closures were managed. Every pod is started
/// as it always was, and nothing is rooted.
#[derive(Debug, Default, Clone, Copy)]
pub struct UnmanagedClosures;

#[async_trait::async_trait]
impl ClosureStore for UnmanagedClosures {
    fn name(&self) -> &'static str {
        "unmanaged"
    }

    async fn ensure(&self, _root: &RootName, _closure: &Path) -> ClosureState {
        ClosureState::Unmanaged
    }

    fn is_present(&self, _closure: &Path) -> bool {
        true
    }

    fn roots(&self) -> std::io::Result<BTreeMap<RootName, PathBuf>> {
        Ok(BTreeMap::new())
    }

    fn release(&self, _root: &RootName) -> std::io::Result<()> {
        Ok(())
    }
}

/// The host's Nix store, driven through `nix-store`.
#[derive(Debug, Clone)]
pub struct NixClosureStore {
    root_dir: PathBuf,
    nix_store: PathBuf,
    realise_timeout: Duration,
}

impl NixClosureStore {
    /// How long one realise may take — a download of a whole closure. Bounded
    /// so a stalled substituter holds one pod's start, not the node forever.
    pub const DEFAULT_REALISE_TIMEOUT: Duration = Duration::from_secs(300);

    /// A store whose roots live in `root_dir`, driven by the `nix-store`
    /// binary at `nix_store`.
    #[must_use]
    pub fn new(root_dir: impl Into<PathBuf>, nix_store: impl Into<PathBuf>) -> Self {
        Self {
            root_dir: root_dir.into(),
            nix_store: nix_store.into(),
            realise_timeout: Self::DEFAULT_REALISE_TIMEOUT,
        }
    }

    /// Find `nix-store` on this host: `ENGENHO_NIX_STORE_BIN`, then the
    /// system and default profiles (a daemon's `PATH` often has neither),
    /// then `PATH`. `None` when there is none.
    #[must_use]
    pub fn discover(root_dir: impl Into<PathBuf>) -> Option<Self> {
        let mut candidates: Vec<PathBuf> = Vec::new();
        if let Some(p) = std::env::var_os("ENGENHO_NIX_STORE_BIN") {
            candidates.push(PathBuf::from(p));
        }
        candidates.push(PathBuf::from("/run/current-system/sw/bin/nix-store"));
        candidates.push(PathBuf::from("/nix/var/nix/profiles/default/bin/nix-store"));
        if let Some(user) = std::env::var_os("USER") {
            let mut p = PathBuf::from("/etc/profiles/per-user");
            p.push(user);
            p.push("bin/nix-store");
            candidates.push(p);
        }
        if let Some(path) = std::env::var_os("PATH") {
            candidates.extend(std::env::split_paths(&path).map(|d| d.join("nix-store")));
        }
        candidates
            .into_iter()
            .find(|p| p.is_file())
            .map(|bin| Self::new(root_dir, bin))
    }

    /// Where the roots live.
    #[must_use]
    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    fn link(&self, root: &RootName) -> PathBuf {
        self.root_dir.join(root.as_str())
    }

    /// `nix-store --realise <closure> --add-root <link>`: substitutes the
    /// closure when it is missing, and registers `link` as an indirect root.
    async fn realise(&self, closure: &Path, link: &Path) -> Result<(), String> {
        let mut cmd = tokio::process::Command::new(&self.nix_store);
        cmd.arg("--realise")
            .arg(closure)
            .arg("--add-root")
            .arg(link)
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true);
        match tokio::time::timeout(self.realise_timeout, cmd.output()).await {
            Err(_) => Err(format!(
                "nix-store --realise did not finish within {}s",
                self.realise_timeout.as_secs()
            )),
            Ok(Err(e)) => Err(format!("cannot run {}: {e}", self.nix_store.display())),
            Ok(Ok(out)) if out.status.success() => Ok(()),
            Ok(Ok(out)) => Err(String::from_utf8_lossy(&out.stderr).trim().to_string()),
        }
    }
}

#[async_trait::async_trait]
impl ClosureStore for NixClosureStore {
    fn name(&self) -> &'static str {
        "nix"
    }

    async fn ensure(&self, root: &RootName, closure: &Path) -> ClosureState {
        let link = self.link(root);
        // Already rooted at this closure and present: nothing to ask the
        // store. This is every tick after the first, so it must be free.
        if closure.exists() && std::fs::read_link(&link).is_ok_and(|t| t == closure) {
            return ClosureState::Rooted;
        }
        if let Err(e) = std::fs::create_dir_all(&self.root_dir) {
            return self.verdict(
                closure,
                format!("cannot create {}: {e}", self.root_dir.display()),
            );
        }
        // A root left pointing elsewhere (the pod's image changed) is
        // replaced; `--add-root` refuses to overwrite a foreign symlink.
        if std::fs::symlink_metadata(&link).is_ok() {
            let _ = std::fs::remove_file(&link);
        }
        match self.realise(closure, &link).await {
            Ok(()) if closure.exists() => ClosureState::Rooted,
            Ok(()) => self.verdict(closure, "realised, yet the path is absent".to_string()),
            Err(detail) => self.verdict(closure, detail),
        }
    }

    fn is_present(&self, closure: &Path) -> bool {
        closure.exists()
    }

    fn roots(&self) -> std::io::Result<BTreeMap<RootName, PathBuf>> {
        let entries = match std::fs::read_dir(&self.root_dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
            Err(e) => return Err(e),
        };
        let mut out = BTreeMap::new();
        for entry in entries {
            let entry = entry?;
            if let Ok(target) = std::fs::read_link(entry.path()) {
                out.insert(
                    RootName::from_file_name(&entry.file_name().to_string_lossy()),
                    target,
                );
            }
        }
        Ok(out)
    }

    fn release(&self, root: &RootName) -> std::io::Result<()> {
        match std::fs::remove_file(self.link(root)) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            other => other,
        }
    }
}

impl NixClosureStore {
    /// A failed ensure: whether the pod can still run decides the arm.
    fn verdict(&self, closure: &Path, detail: String) -> ClosureState {
        if closure.exists() {
            ClosureState::PresentUnrooted { detail }
        } else {
            ClosureState::Unavailable {
                path: closure.to_path_buf(),
                detail,
            }
        }
    }
}

/// The `nix:` closures a pod's containers (init and app) name, by container.
#[must_use]
pub fn pod_closures(pod: &serde_json::Value) -> Vec<(String, PathBuf)> {
    let mut out = Vec::new();
    for field in ["initContainers", "containers"] {
        let Some(list) = pod
            .pointer("/spec")
            .and_then(|s| s.get(field))
            .and_then(serde_json::Value::as_array)
        else {
            continue;
        };
        for c in list {
            let (Some(name), Some(image)) = (
                c.get("name").and_then(serde_json::Value::as_str),
                c.get("image").and_then(serde_json::Value::as_str),
            ) else {
                continue;
            };
            if let Ok(crate::image_source::ImageSource::NixClosure(path)) =
                crate::image_source::ImageSource::parse(image)
            {
                out.push((name.to_string(), path));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_root_name_is_one_safe_file_name() {
        assert_eq!(RootName::new("4aed-37", "app").as_str(), "4aed-37.app");
        assert_eq!(
            RootName::new("default/home", "a/../b").as_str(),
            "default_home.a____b"
        );
    }

    #[test]
    fn only_nix_closures_are_collected_from_both_container_lists() {
        let pod = json!({"spec": {
            "initContainers": [{"name": "init", "image": "nix:/nix/store/aaa-init:latest"}],
            "containers": [
                {"name": "app", "image": "nix:/nix/store/bbb-app"},
                {"name": "oci", "image": "docker.io/library/postgres:16"},
            ],
        }});
        assert_eq!(
            pod_closures(&pod),
            vec![
                ("init".to_string(), PathBuf::from("/nix/store/aaa-init")),
                ("app".to_string(), PathBuf::from("/nix/store/bbb-app")),
            ]
        );
    }

    #[tokio::test]
    async fn a_missing_closure_with_a_broken_store_is_unavailable_naming_the_path() {
        let dir = std::env::temp_dir().join(format!("engenho-roots-{}", std::process::id()));
        let store = NixClosureStore::new(&dir, "/nonexistent/nix-store");
        let path = Path::new("/nix/store/00000000000000000000000000000000-gone");
        match store.ensure(&RootName::new("uid", "c"), path).await {
            ClosureState::Unavailable { path: p, detail } => {
                assert_eq!(p, path);
                assert!(detail.contains("/nonexistent/nix-store"), "{detail}");
            }
            other => panic!("{other:?}"),
        }
        let _ = std::fs::remove_dir_all(dir);
    }
}
