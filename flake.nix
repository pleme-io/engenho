{
  description = "engenho — typed, attested, Rust-native Kubernetes runtime. One single-binary distribution wire-compatible with kubectl/CRI/CNI/etcd-v3; generation-driven typed resource registry from upstream OpenAPI v3; Pillar 7 runtime half; sibling to magma. Spec: theory/ENGENHO.md.";

  # substrate.rust.workspace dispatches over Cargo.gen.lock (the slim gen delta,
  # reconstructed to the full BuildSpec in pure Nix) — no crate2nix, no Cargo.nix.
  inputs.substrate.url = "github:pleme-io/substrate";

  outputs = { substrate, ... }: substrate.rust.workspace {
    src = ./.;
    member = "engenho";
  };
}
