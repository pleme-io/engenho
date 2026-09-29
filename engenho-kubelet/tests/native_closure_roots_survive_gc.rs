//! The closure roots against a REAL Nix store and a REAL garbage collection.
//!
//! A path is added to the host's store, rooted through [`NixClosureStore`]
//! exactly as the kubelet roots a pod's closure, and then deleted with a
//! targeted GC (`nix-store --delete`, which runs the collector's own root
//! scan and refuses a live path). While rooted the store must refuse; once
//! released it must collect. Then the path is gone and not substitutable, so
//! asking for it again must come back [`ClosureState::Unavailable`] naming
//! it — the verdict the kubelet turns into `ImageUnavailable`.
//!
//! Needs `nix-store`; like the other native tests it fails rather than skips
//! without one.

use std::path::{Path, PathBuf};
use std::process::Command;

use engenho_kubelet::{ClosureState, ClosureStore, NixClosureStore, RootName};

fn scratch(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("engenho-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// `nix-store --delete <path>`: true when the store collected it.
fn gc(nix_store: &Path, path: &Path) -> bool {
    Command::new(nix_store)
        .arg("--delete")
        .arg(path)
        .output()
        .expect("run nix-store --delete")
        .status
        .success()
}

#[tokio::test]
async fn a_rooted_closure_survives_gc_and_a_released_one_does_not() {
    let dir = scratch("gcroots");
    let store = NixClosureStore::discover(dir.join("roots"))
        .expect("nix-store is required for this test (it fails rather than skips)");
    let nix_store = {
        let found = std::env::var_os("ENGENHO_NIX_STORE_BIN").map(PathBuf::from);
        found.unwrap_or_else(|| which_nix_store().expect("nix-store on PATH or a known profile"))
    };

    // A fresh store path nothing else references.
    let file = dir.join("payload");
    std::fs::write(
        &file,
        format!("engenho closure root test {}", std::process::id()),
    )
    .unwrap();
    let out = Command::new(&nix_store)
        .arg("--add")
        .arg(&file)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let path = PathBuf::from(String::from_utf8(out.stdout).unwrap().trim());
    assert!(path.exists(), "premise: {} is in the store", path.display());

    let root = RootName::new("test-uid", "app");
    assert_eq!(store.ensure(&root, &path).await, ClosureState::Rooted);
    assert_eq!(store.roots().unwrap().get(&root), Some(&path));
    assert!(!gc(&nix_store, &path), "a rooted closure must survive GC");
    assert!(path.exists());
    // A second ensure is free and stays rooted.
    assert_eq!(store.ensure(&root, &path).await, ClosureState::Rooted);

    store.release(&root).unwrap();
    assert!(gc(&nix_store, &path), "a released closure is collectable");
    assert!(!path.exists());

    match store.ensure(&root, &path).await {
        ClosureState::Unavailable { path: p, .. } => assert_eq!(p, path),
        other => panic!("a collected, unsubstitutable closure is Unavailable: {other:?}"),
    }
    let _ = std::fs::remove_dir_all(dir);
}

fn which_nix_store() -> Option<PathBuf> {
    let mut candidates = vec![
        PathBuf::from("/run/current-system/sw/bin/nix-store"),
        PathBuf::from("/nix/var/nix/profiles/default/bin/nix-store"),
    ];
    if let Some(path) = std::env::var_os("PATH") {
        candidates.extend(std::env::split_paths(&path).map(|d| d.join("nix-store")));
    }
    candidates.into_iter().find(|p| p.is_file())
}
