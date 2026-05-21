{
  description = "engenho — typed, attested, Rust-native Kubernetes runtime. One single-binary distribution wire-compatible with kubectl/CRI/CNI/etcd-v3; generation-driven typed resource registry from upstream OpenAPI v3; Pillar 7 runtime half; sibling to magma. Spec: theory/ENGENHO.md.";

  inputs = {
    nixpkgs.url     = "github:nixos/nixpkgs?ref=nixos-25.11";
    crate2nix.url   = "github:nix-community/crate2nix";
    flake-utils.url = "github:numtide/flake-utils";
    substrate = {
      url = "github:pleme-io/substrate";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    nixpkgs,
    crate2nix,
    flake-utils,
    substrate,
  }: let
    standardFlake = (import "${substrate}/lib/rust-workspace-release-flake.nix" {
      inherit nixpkgs crate2nix flake-utils;
    }) {
      toolName    = "engenho";
      packageName = "engenho";
      src         = self;
      repo        = "pleme-io/engenho";
    };

    # Per-system slim package for the cluster-config renderer. Built
    # via the COMMITTED Cargo.nix (no crate2nix IFD) so consumers
    # (nixos-k3s-vm in pleme-io/nix) can evaluate the renderer
    # derivation without first triggering an IFD chain on the
    # linux-builder. Crate2nix-of-the-tree is regenerated explicitly
    # via `nix run .#regenerate-cargo-nix` whenever Cargo.lock changes.
    renderBinFor = system: let
      pkgs = import nixpkgs { inherit system; };
      generated = import ./Cargo.nix {
        inherit pkgs;
        defaultCrateOverrides = pkgs.defaultCrateOverrides // {};
      };
    in generated.workspaceMembers.engenho-cluster-config-render.build;

    # engenho-mcp is a sibling MCP server binary built from the same
    # workspace. Same Cargo.nix-driven path as the renderer above so
    # IFD doesn't fire on consumers. Operators get the binary at
    # `engenho.packages.<system>.engenho-mcp` and wire it into Claude
    # Code / Cursor / OpenCode via blackmatter-kubernetes' HM module.
    #
    # rmcp's `model.rs:860` uses `env!("CARGO_CRATE_NAME")`, which
    # crate2nix doesn't set by default. The override matches the
    # zoekt-mcp precedent so the build resolves the crate name from
    # the env var at compile time.
    mcpBinFor = system: let
      pkgs = import nixpkgs { inherit system; };
      generated = import ./Cargo.nix {
        inherit pkgs;
        defaultCrateOverrides = pkgs.defaultCrateOverrides // {
          rmcp = attrs: { CARGO_CRATE_NAME = "rmcp"; };
        };
      };
    in generated.workspaceMembers.engenho-mcp.build;
  in
    standardFlake
    // {
      packages = nixpkgs.lib.recursiveUpdate (standardFlake.packages or {}) (
        nixpkgs.lib.genAttrs [ "aarch64-linux" "aarch64-darwin" "x86_64-linux" "x86_64-darwin" ]
          (system: {
            engenho-cluster-config-render = renderBinFor system;
            engenho-mcp = mcpBinFor system;
          })
      );

      # Overlay so downstream consumers (pleme-io/nix/parts/overlays.nix)
      # can drop in `inputs.engenho.overlays.default` and get
      # `pkgs.engenho-mcp` (+ the cluster-config renderer) on every
      # system. Mirrors the zoekt-mcp / amimori / kurage pattern.
      overlays.default = final: _prev: {
        engenho-mcp = mcpBinFor final.stdenv.hostPlatform.system;
        engenho-cluster-config-render =
          renderBinFor final.stdenv.hostPlatform.system;
      };
    };
}
