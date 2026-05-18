//! engenho — typed, attested, Rust-native Kubernetes runtime.
//!
//! M0.0 placeholder. The full binary composes apiserver + datastore +
//! controllers + scheduler + kubelet + kube-proxy + DNS + local-path
//! under tatara daemon supervision per theory/ENGENHO.md §IX. Until
//! M0.1 lands the apiserver, this binary just prints the destination
//! pointer and exits 0 — enough to validate the build chain (cargo +
//! crate2nix + substrate's rust-workspace-release-flake.nix) end-to-end.

fn main() {
    println!(
        "engenho v{} — typed, attested, Rust-native Kubernetes runtime",
        env!("CARGO_PKG_VERSION")
    );
    println!();
    println!("M0.0 — engenho-types crate seed.");
    println!("See theory/ENGENHO.md for destination, M0-ROADMAP.md for path.");
    println!();
    println!("KubeResource trait: {} kinds (M0.0.4 target: ~150)",
             count_registered_kinds());
}

/// M0.0 placeholder. Replaced at M0.0.4 with a real registry inspection.
fn count_registered_kinds() -> usize {
    // Zero generated kinds at M0.0 — engenho-types ships KubeResource trait
    // + meta/v1 types + GVK helpers only. The first generated kind lands at
    // M0.0.1 (Pod as the kube-forge bullseye); ~150 at M0.0.4.
    0
}
