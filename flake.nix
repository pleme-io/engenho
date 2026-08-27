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
    };
}
