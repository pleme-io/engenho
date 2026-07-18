{
  description = "engenho — typed, attested, Rust-native Kubernetes runtime. One single-binary distribution wire-compatible with kubectl/CRI/CNI/etcd-v3; generation-driven typed resource registry from upstream OpenAPI v3; Pillar 7 runtime half; sibling to magma. Spec: theory/ENGENHO.md.";

  # substrate.rust.workspace dispatches over Cargo.gen.lock (the slim gen delta,
  # reconstructed to the full BuildSpec in pure Nix) — no crate2nix, no Cargo.nix.
  inputs = {
    substrate.url = "github:pleme-io/substrate";
    # For lib.genAttrs in the engenho-mcp secondary-package graft below.
    nixpkgs.follows = "substrate/nixpkgs";
  };

  outputs = { substrate, nixpkgs, ... }:
    let
      base = substrate.rust.workspace {
        src = ./.;
        member = "engenho";
      };

      # `engenho-mcp` — the MCP surface for engenho-managed clusters (crate
      # engenho-mcp). The fleet's claude MCP overlay consumes
      # `engenho.packages.<system>.engenho-mcp`, but the bare `member = "engenho"`
      # build dropped it. Restore as a second member build grafted per-system.
      mcpBase = substrate.rust.workspace {
        src = ./.;
        member = "engenho-mcp";
      };
      mcpSystems = [ "aarch64-darwin" "x86_64-darwin" "x86_64-linux" "aarch64-linux" ];
      withMcp = nixpkgs.lib.genAttrs mcpSystems (system:
        (base.packages.${system} or { }) // {
          engenho-mcp = mcpBase.packages.${system}.default;
        });
    in
    base // {
      packages = withMcp;
    };
}
