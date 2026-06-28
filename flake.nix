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

      # Self-emit the NixOS / nix-darwin / home-manager module trio so an
      # isolated engenho instance can be declared from a node config (the
      # `nix` private repo). Substrate's `mkModuleTrio` factory generates:
      #
      #   * a system-level systemd service (NixOS) / launchd daemon (Darwin)
      #     running `engenho daemon` — Type=simple, Restart=always, long-lived
      #     (see `daemonSubcommand` + `withSystemDaemon`), and
      #   * a typed shikumi config surface (`services.engenho.<group>.<field>`,
      #     HM) that renders to YAML pointed at by `ENGENHO_CONFIG`.
      #
      # The node consumer wires the load-bearing isolation knobs
      # (dedicated `engenho` system user, StateDirectory, CPUQuota /
      # MemoryMax caps, `Environment=ENGENHO_CONFIG=<rendered yaml>`) via
      # `systemd.services.engenho-daemon` overrides + the daemon
      # `environment` option — the factory already exposes
      # `services.engenho.daemon.{enable,extraArgs,environment}`.
      module = {
        description = "engenho — typed, attested, Rust-native Kubernetes runtime (Pillar 7)";

        # System-level daemon: `engenho daemon` is an explicit alias for the
        # bare no-arg daemon path (engenho/src/main.rs Command::parse). The
        # factory's `mkNixOSService` emits Type=simple + Restart=always +
        # Environment from `services.engenho.daemon.environment` — exactly a
        # long-lived daemon with an env-pointed config.
        withSystemDaemon = true;
        daemonSubcommand = "daemon";

        # Typed config surface — renders to a partial YAML that
        # `EngenhoConfig::from_yaml_with_defaults` deep-merges onto the
        # prescribed defaults (operators specify only overrides). The unit
        # points `ENGENHO_CONFIG` at this file. Groups mirror the
        # `cluster` + `runtime` sections of EngenhoConfig 1:1 so the
        # rendered keys round-trip through serde (`deny_unknown_fields`).
        withShikumiConfig = true;
        shikumiConfigPath = ".config/engenho/engenho.yaml";

        shikumiTypedGroups = {
          # cluster.name — the cluster identity (rejects dots/spaces).
          cluster = {
            name = {
              type = "str";
              default = "engenho-local";
              description = "Cluster identity name (no dots or spaces).";
            };
          };

          # runtime.* — the process-level assembly knobs. These are the
          # load-bearing isolation surface: a co-resident instance pins
          # listen_addr to loopback, a dedicated data_dir, and the fake
          # kubelet backend (zero container/network activity).
          runtime = {
            listen_addr = {
              type = "str";
              default = "0.0.0.0:6443";
              description = "Apiserver bind address. A co-resident instance MUST use a non-default loopback port (e.g. 127.0.0.1:16443) so it never collides with another apiserver.";
            };
            data_dir = {
              type = "str";
              default = "/var/lib/engenho";
              description = "Durable-store + PKI keyspace root. Co-resident instances each need their own (e.g. /var/lib/engenho-rio).";
            };
            node_name = {
              type = "str";
              default = "engenho-node";
              description = "This node's name. The kubelet binds Pods whose spec.nodeName matches; the Runtime self-registers Node/<node_name> at boot.";
            };
            kubelet_backend = {
              type = "enum";
              values = [ "podman" "fake" ];
              default = "fake";
              description = "Container backend the kubelet drives. `fake` = in-memory deterministic (zero containers / networking — cannot touch cni0/flannel/iptables). `podman` = real shell-out.";
            };
            durable = {
              type = "bool";
              default = true;
              description = "true = durable restart-safe fjall store; false = ephemeral in-memory (tests/dev).";
            };
          };
        };
      };
    };

    # Per-system slim package for the cluster-config renderer. Built
    # via substrate's lockfile-builder (gen-driven; Cargo.build-spec.json
    # is auto-derived from Cargo.lock + cargo metadata, no Cargo.nix
    # regenerate dance). Regenerate the spec via `gen build .` whenever
    # Cargo.lock changes.
    plemeCrateOverrides = import "${substrate}/lib/build/rust/pleme-crate-overrides.nix";
    renderBinFor = system: let
      pkgs = import nixpkgs { inherit system; };
      lockfileBuilder = import "${substrate}/lib/build/rust/lockfile-builder.nix" { inherit pkgs; };
      project = lockfileBuilder.mkProject {
        src = self;
        defaultCrateOverrides = pkgs.defaultCrateOverrides // plemeCrateOverrides;
      };
    in project.workspaceMembers.engenho-cluster-config-render.build;

    # engenho-mcp is a sibling MCP server binary built from the same
    # workspace. rmcp override now lives in pleme-crate-overrides.nix —
    # composed via the shared plemeCrateOverrides attrset.
    mcpBinFor = system: let
      pkgs = import nixpkgs { inherit system; };
      lockfileBuilder = import "${substrate}/lib/build/rust/lockfile-builder.nix" { inherit pkgs; };
      project = lockfileBuilder.mkProject {
        src = self;
        defaultCrateOverrides = pkgs.defaultCrateOverrides // plemeCrateOverrides;
      };
    in project.workspaceMembers.engenho-mcp.build;

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
      #
      # Plus operator-side equivalents of the GH-Actions auto-bump-
      # and-publish flow (.github/workflows/{auto-bump,crates-publish}.yml):
      #
      #   nix run .#bump-workspace -- patch   ← cargo set-version --workspace --bump
      #                                         + regenerate Cargo.nix + git
      #                                         commit + tag (does NOT push)
      #   nix run .#publish-crates            ← cargo publish --workspace
      #                                         (requires CARGO_REGISTRY_TOKEN)
      #   nix run .#publish-crates -- --dry-run
      apps = nixpkgs.lib.recursiveUpdate (standardFlake.apps or {}) (
        nixpkgs.lib.genAttrs
          [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" "x86_64-darwin" ]
          (system: let
            pkgs = import nixpkgs { inherit system; };
          in {
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

            # Workspace patch bump — mirrors auto-bump.yml's behavior
            # so operators can preview a release locally before pushing.
            bump-workspace = {
              type = "app";
              program = toString (pkgs.writeShellScript "engenho-bump-workspace" ''
                set -euo pipefail
                export PATH="${pkgs.cargo}/bin:${pkgs.cargo-edit}/bin:${pkgs.jq}/bin:${pkgs.git}/bin:$PATH"
                BUMP_TYPE="''${1:-patch}"
                case "$BUMP_TYPE" in major|minor|patch) ;;
                  *) echo "Usage: nix run .#bump-workspace -- {patch|minor|major}"; exit 1 ;;
                esac
                OLD=$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[0].version')
                echo "Bumping $BUMP_TYPE from $OLD..."
                cargo set-version --workspace --bump "$BUMP_TYPE"
                NEW=$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[0].version')
                echo "==> regenerating Cargo.nix..."
                ${crate2nix.packages.${system}.default}/bin/crate2nix generate
                cargo update --workspace --offline 2>/dev/null || cargo update --workspace 2>/dev/null || true
                git add Cargo.toml Cargo.lock Cargo.nix engenho-*/Cargo.toml engenho/Cargo.toml
                git commit -m "release: workspace v$NEW"
                git tag "v$NEW"
                echo ""
                echo "Bumped $OLD -> $NEW + tagged v$NEW."
                echo "Push with:  git push origin main && git push origin v$NEW"
              '');
            };

            # Publish every workspace member to crates.io. Cargo 1.84+
            # `--workspace` flag handles topological order + waits
            # for each crate to land before publishing dependents.
            publish-crates = {
              type = "app";
              program = toString (pkgs.writeShellScript "engenho-publish-crates" ''
                set -euo pipefail
                export PATH="${pkgs.cargo}/bin:$PATH"
                if [ -z "''${CARGO_REGISTRY_TOKEN:-}" ] \
                   && [ "''${1:-}" != "--dry-run" ]; then
                  echo "Error: CARGO_REGISTRY_TOKEN is not set."
                  echo "  export CARGO_REGISTRY_TOKEN=<token>   (or pass --dry-run)"
                  exit 1
                fi
                echo "==> cargo publish --workspace $@"
                cargo publish --workspace --no-verify "$@"
              '');
            };
          })
      );

      # Overlay so downstream consumers (pleme-io/nix/parts/overlays.nix)
      # drop in `inputs.engenho.overlays.default` and get the main engenho
      # binary + engenho-mcp + the cluster-config renderer on every system.
      # Mirrors zoekt-mcp / amimori / kurage pattern.
      #
      # NOTE: this `overlays.default` REPLACES the one
      # rust-workspace-release-flake.nix auto-emits (which only carried
      # `engenho`). We re-export `engenho` here so the emitted module trio's
      # default package option (`pkgs.engenho`) resolves downstream AND the
      # MCP / render siblings stay available — without it `pkgs.engenho`
      # would be undefined wherever the module's `services.engenho.package`
      # default (`pkgs.engenho`) is forced.
      overlays.default = final: _prev: {
        engenho = (standardFlake.packages.${final.stdenv.hostPlatform.system}.default);
        engenho-mcp = mcpBinFor final.stdenv.hostPlatform.system;
        engenho-cluster-config-render =
          renderBinFor final.stdenv.hostPlatform.system;
      };
    };
}
