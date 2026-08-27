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
      # `engenho-cluster-config-render` — the SAME graft, for the same
      # reason, and it was missing.
      #
      # ── ★ WHY THIS IS A BUG FIX, NOT A NEW FEATURE ──────────────────
      # `kindling-profiles`' `profiles/nixos-k3s-vm/default.nix:37` reads
      # `inputs.engenho.packages.${system}.engenho-cluster-config-render`
      # to render a cluster's config into the VM image. That attribute did
      # not exist: `member = "engenho"` builds one member, and the only
      # `engenho-cluster-config-render` outputs here were the two OCI
      # IMAGES (`-image-amd64`/`-image-arm64`) — an image cannot be a
      # NixOS `environment.etc` input.
      #
      # Measured 2026-08-27: every `kikai up --cluster engenho-local`
      # failed in PREFLIGHT with `attribute
      # 'engenho-cluster-config-render' missing`, so the local k3s VM
      # could not be built at all. The images built fine, which is why
      # nothing else flagged it.
      renderBase = substrate.rust.workspace {
        src = ./.;
        member = "engenho-cluster-config-render";
      };
      mcpSystems = [ "aarch64-darwin" "x86_64-darwin" "x86_64-linux" "aarch64-linux" ];
      withMcp = nixpkgs.lib.genAttrs mcpSystems (system:
        (base.packages.${system} or { }) // {
          engenho-mcp = mcpBase.packages.${system}.default;
          engenho-cluster-config-render = renderBase.packages.${system}.default;
        });

      # ============================================================
      # OCI images — Nix-native, Pillar 8 (no Dockerfiles). Restores
      # `packages.<system>.<toolName>-image-<arch>`, the attrs
      # `.github/workflows/release.yml`'s `image-push.yml` calls
      # expect (`nix build .#engenho-mcp-image-amd64` etc.) and which
      # every release since v0.7.1 has failed to find (the
      # crate2nix → gen-pattern flake migrations dropped them).
      #
      # Root cause of the CVEs a trivy scan found in the last
      # successfully-published tag (ghcr.io/pleme-io/engenho-mcp:0.7.0,
      # commit 3c1432e): that image was built from a Dockerfile
      # (`FROM gcr.io/distroless/cc-debian12`, deleted in 78be81e) —
      # the libssl3 CVEs were the Debian base image's package, not
      # anything engenho pins (`cargo tree -p engenho-mcp` shows zero
      # openssl-sys/native-tls; engenho-kube-client's reqwest is
      # `rustls-tls` only). This dockerTools image carries no distro
      # base and no dpkg package database at all — the whole libssl3
      # CVE class is structurally absent, not merely patched.
      #
      # `genBuild = true` drives substrate's lockfile-builder (the
      # same gen-based engine `base`/`mcpBase` already use above) —
      # no crate2nix, no Cargo.nix, consistent with the workspace's
      # 2026-07-17 migration off crate2nix (Cargo.nix regen was
      # failing in the auto-release bump job).
      mkToolImage = import "${substrate}/lib/build/rust/tool-image.nix" {
        inherit nixpkgs;
        # Only gates the native-binary/devShell side of tool-image.nix
        # (unused here — we only read `.packages.dockerImage-{amd64,arm64}`
        # below); `mkImage` always targets x86_64-linux/aarch64-linux
        # for the actual container regardless of this value.
        system = "x86_64-linux";
      };

      mkToolImages = toolName: (mkToolImage {
        inherit toolName;
        packageName = toolName;
        src = ./.;
        repo = "pleme-io/engenho";
        genBuild = true;
        architectures = [ "amd64" "arm64" ];
      }).packages;

      mcpImages = mkToolImages "engenho-mcp";
      renderImages = mkToolImages "engenho-cluster-config-render";

      imageAttrs = {
        engenho-mcp-image-amd64 = mcpImages.dockerImage-amd64;
        engenho-mcp-image-arm64 = mcpImages.dockerImage-arm64;
        engenho-cluster-config-render-image-amd64 = renderImages.dockerImage-amd64;
        engenho-cluster-config-render-image-arm64 = renderImages.dockerImage-arm64;
      };
      # Linux-only — dockerTools images have no meaning on darwin systems.
      imageSystems = [ "x86_64-linux" "aarch64-linux" ];
    in
    base // {
      packages = nixpkgs.lib.genAttrs mcpSystems (system:
        withMcp.${system} // (
          if builtins.elem system imageSystems then imageAttrs else { }
        ));
    } // (
      # ── The module trio ────────────────────────────────────────────────
      # engenho's `main.rs` has documented this integration since it was
      # written ("the verb the substrate `mkModuleTrio` factory invokes"),
      # but no module existed: the whole repo contained ONE .nix file, and
      # `nixosModules` / `darwinModules` / `homeManagerModules` were all
      # absent. So "every node runs its own engenho by default" had no way
      # to actually start one.
      #
      # `hmNamespace = "services"` (not the "programs" default) is
      # deliberate: it puts the option path at `services.engenho.*` on ALL
      # THREE arms, which is what lets one typed-config module serve them
      # all instead of three copies with three chances to drift.
      let
        trio = (import "${substrate}/lib/module-trio.nix" {
          inherit (nixpkgs) lib;
        }).mkModuleTrio {
          name = "engenho";
          description = "engenho — typed, attested, Rust-native Kubernetes runtime";
          binaryName = "engenho";
          packageAttr = "engenho";
          hmNamespace = "services";

          # Both arms, because engenho is legitimately either: a per-user
          # local cluster on a workstation (HM user agent), or the node
          # runtime on a server (system daemon). `daemonSubcommand` matches
          # engenho's actual CLI verb — the bare form boots the daemon too,
          # but naming it keeps the generated unit self-describing.
          withSystemDaemon = true;
          withUserDaemon = true;
          daemonSubcommand = "daemon";

          # engenho reads shikumi TieredConfig, so the YAML the trio deploys
          # IS its file tier. Defaults stay EMPTY on purpose: engenho's own
          # progressive fold already supplies prescribed defaults, and a key
          # written here would be read as an explicit operator opinion that
          # SUPPRESSES that fold — including the derived per-node cluster
          # name. See nix/typed-config.nix's `prune`.
          withShikumiConfig = true;
          shikumiDefaults = { };
        };
        # The typed surface rides with every arm, so a consumer gets one
        # import and gets eval-time type checking with it.
        withTyped = m: { imports = [ m ./nix/typed-config.nix ]; };
      in
      {
        # Eval-time proof for the typed surface (IFD-free — it stubs the
        # trio's `settings` option rather than building engenho, so it runs
        # anywhere `nix flake check` does). Red-run verified: weakening the
        # kubeletBackend enum, and disabling the null-prune, each turn it red.
        checks = nixpkgs.lib.genAttrs mcpSystems (system: {
          typed-config = import ./nix/tests/typed-config-test.nix {
            pkgs = import nixpkgs { inherit system; };
          };
        });

        nixosModules.default = withTyped trio.nixosModule;
        nixosModules.engenho = withTyped trio.nixosModule;
        darwinModules.default = withTyped trio.darwinModule;
        darwinModules.engenho = withTyped trio.darwinModule;
        homeManagerModules.default = withTyped trio.homeManagerModule;
        homeManagerModules.engenho = withTyped trio.homeManagerModule;
      }
    );
}
