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
    # Image-publishing pipeline — substrate's
    # rust-tool-image-flake.nix calls into forge to push images to
    # ghcr.io/pleme-io/* via the release-${toolName} apps.
    forge = {
      url = "github:pleme-io/forge";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    devenv = {
      url = "github:cachix/devenv";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    nixpkgs,
    crate2nix,
    flake-utils,
    substrate,
    forge,
    devenv,
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
    # IFD doesn't fire on consumers. rmcp's `model.rs:860` uses
    # `env!("CARGO_CRATE_NAME")`, which crate2nix doesn't set by
    # default. The override matches the zoekt-mcp precedent.
    mcpBinFor = system: let
      pkgs = import nixpkgs { inherit system; };
      generated = import ./Cargo.nix {
        inherit pkgs;
        defaultCrateOverrides = pkgs.defaultCrateOverrides // {
          rmcp = attrs: { CARGO_CRATE_NAME = "rmcp"; };
        };
      };
    in generated.workspaceMembers.engenho-mcp.build;

    # ============================================================
    # Docker images via substrate's rust-tool-image-flake.nix.
    # Per pleme-io standard (sui, tatara, ...): one substrate call
    # per binary; merge resulting packages + apps into the final
    # flake outputs. Pushed to ghcr.io/pleme-io/{name} via
    # `nix run .#release-{name}` (forge handles auth + push).
    # ============================================================
    mcpImageOutputs = (import "${substrate}/lib/rust-tool-image-flake.nix" {
      inherit nixpkgs crate2nix flake-utils forge devenv;
    }) {
      toolName    = "engenho-mcp";
      packageName = "engenho-mcp";
      src         = self;
      repo        = "pleme-io/engenho-mcp";
      architectures = [ "amd64" "arm64" ];
      env = [
        "RUST_LOG=info"
      ];
    };

    renderImageOutputs = (import "${substrate}/lib/rust-tool-image-flake.nix" {
      inherit nixpkgs crate2nix flake-utils forge devenv;
    }) {
      toolName    = "engenho-cluster-config-render";
      packageName = "engenho-cluster-config-render";
      src         = self;
      repo        = "pleme-io/engenho-cluster-config-render";
      architectures = [ "amd64" "arm64" ];
      env = [];
    };
  in
    standardFlake
    // {
      # Merge: workspace binaries + substrate-generated Docker image
      # packages (per-system, per-arch) under stable namespaced keys.
      packages = nixpkgs.lib.recursiveUpdate (standardFlake.packages or {}) (
        nixpkgs.lib.genAttrs [ "aarch64-linux" "aarch64-darwin" "x86_64-linux" "x86_64-darwin" ]
          (system:
            {
              engenho-cluster-config-render = renderBinFor system;
              engenho-mcp = mcpBinFor system;
            }
            // (let m = mcpImageOutputs.packages.${system} or {}; in {
              engenho-mcp-image-amd64 = m.dockerImage-amd64 or null;
              engenho-mcp-image-arm64 = m.dockerImage-arm64 or null;
            })
            // (let r = renderImageOutputs.packages.${system} or {}; in {
              engenho-cluster-config-render-image-amd64 = r.dockerImage-amd64 or null;
              engenho-cluster-config-render-image-arm64 = r.dockerImage-arm64 or null;
            })
          )
      );

      # `nix run .#release-engenho-mcp` pushes the multi-arch image
      # to ghcr.io/pleme-io/engenho-mcp via forge. Same for the
      # renderer. forge consumes GITHUB_TOKEN automatically.
      apps = nixpkgs.lib.recursiveUpdate (standardFlake.apps or {}) (
        nixpkgs.lib.genAttrs
          [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" "x86_64-darwin" ]
          (system: {
            release-engenho-mcp =
              (mcpImageOutputs.apps.${system} or {}).release or {
                type = "app";
                program = "${nixpkgs.legacyPackages.${system}.coreutils}/bin/echo 'release-engenho-mcp not available on ${system}'";
              };
            release-engenho-cluster-config-render =
              (renderImageOutputs.apps.${system} or {}).release or {
                type = "app";
                program = "${nixpkgs.legacyPackages.${system}.coreutils}/bin/echo 'release-engenho-cluster-config-render not available on ${system}'";
              };
          })
      );

      # Overlay so downstream consumers (pleme-io/nix/parts/overlays.nix)
      # drop in `inputs.engenho.overlays.default` and get engenho-mcp +
      # the cluster-config renderer on every system. Mirrors zoekt-mcp /
      # amimori / kurage pattern.
      overlays.default = final: _prev: {
        engenho-mcp = mcpBinFor final.stdenv.hostPlatform.system;
        engenho-cluster-config-render =
          renderBinFor final.stdenv.hostPlatform.system;
      };
    };
}
