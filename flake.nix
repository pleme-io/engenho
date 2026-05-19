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
    # via the same Cargo.nix the workspace uses; consumers (nixos-k3s-vm
    # in pleme-io/nix) invoke this at NixOS build time inside a
    # `runCommand` to turn typed nix module options into k3s
    # config.yaml + extra cmdline args + auto-apply manifests.
    renderBinFor = system: let
      pkgs = import nixpkgs { inherit system; };
      generated = import (crate2nix.tools.${system}.generatedCargoNix {
        name = "engenho-cluster-config-render";
        src  = self;
      }) {
        inherit pkgs;
        defaultCrateOverrides = pkgs.defaultCrateOverrides // {};
      };
    in generated.workspaceMembers.engenho-cluster-config-render.build;
  in
    standardFlake
    // {
      packages = nixpkgs.lib.recursiveUpdate (standardFlake.packages or {}) (
        nixpkgs.lib.genAttrs [ "aarch64-linux" "aarch64-darwin" "x86_64-linux" "x86_64-darwin" ]
          (system: { engenho-cluster-config-render = renderBinFor system; })
      );
    };
}
