{
  description = "engenho — typed, attested, Rust-native Kubernetes runtime. One single-binary distribution wire-compatible with kubectl/CRI/CNI/etcd-v3; generation-driven typed resource registry from upstream OpenAPI v3; Pillar 7 runtime half; sibling to magma. Spec: theory/ENGENHO.md.";

  # substrate.rust.workspace dispatches over Cargo.gen.lock (the slim gen delta,
  # reconstructed to the full BuildSpec in pure Nix) — no crate2nix, no Cargo.nix.
  inputs = {
    substrate.url = "github:pleme-io/substrate";
    # For lib.genAttrs in the engenho-mcp secondary-package graft below.
    nixpkgs.follows = "substrate/nixpkgs";
  };

  outputs = { self, substrate, nixpkgs, ... }:
    let
      inherit (nixpkgs) lib;

      # ── checks.<system>.tests: `cargo test` over the workspace ─────────
      # Substrate 83bab67's opt-in runner (lib/build/rust/workspace-tests.nix):
      # `cargo test --frozen` over a vendor dir built from Cargo.lock, reading
      # this repo's [profile.*] itself. The two runs are test.yml's gate: every
      # target with every feature (engenho gates real code behind with-tameshi,
      # with-sui-eval, openapi-roundtrip, mock, …), then the doctests, which
      # `--all-targets` leaves out. Both runs share one build tree. Both pass
      # `--no-fail-fast`, as test.yml does: a red build then reports every
      # failing test binary, not the first, so one ~17 GB build is enough to
      # count what is red.
      #
      # System libraries. Of the -sys crates in Cargo.lock only openssl-sys
      # needs one from nixpkgs (pkg-config + openssl; sui-store turns reqwest's
      # `default-tls` on, see the OCI comment below). aws-lc-sys 0.41 (cc
      # builder, no cmake unless asked), libsqlite3-sys (sqlx's `bundled`),
      # lzma-sys and zstd-sys compile their vendored C with stdenv's cc, and
      # the darwin ones (core-foundation, security-framework,
      # system-configuration, fsevent, kqueue) link frameworks from stdenv's
      # SDK, so they need nothing here.
      #
      # Tests the build sandbox cannot run, skipped by exact name (libtest
      # `--exact --skip`), each for a fact about the sandbox:
      #   * the live-oracle binaries. They diff engenho against a live
      #     Kubernetes cluster, and the sandbox has none. The names are read
      #     from .config/nextest.toml's default filter, the one list every
      #     nextest gate uses, so they are not restated here. Each binary holds
      #     one test named after it, which is what lets a binary name serve as
      #     a test name.
      #   * the engenho-kubelet tests that realise a closure with a real
      #     `nix build`. The sandbox has no Nix daemon. test.yml still runs them.
      # A skip that stops matching (a renamed test, a second test in an oracle
      # binary) lets the test run and fail on the missing oracle or daemon, so
      # drift turns this check red rather than quietly skipping more.
      nextestDefaultFilter =
        (builtins.fromTOML (builtins.readFile ./.config/nextest.toml)).profile.default.default-filter;
      oracleBinaries = lib.concatLists (builtins.filter builtins.isList
        (builtins.split "binary\\(=([A-Za-z0-9_]+)\\)" nextestDefaultFilter));
      needsNixDaemon = [
        # engenho-kubelet/tests/native_runs_a_real_closure.rs
        "a_nix_closure_runs_as_a_native_process_and_its_output_is_readable"
        "a_signalled_container_reports_its_signal_not_a_clean_exit"
        "a_container_sees_only_its_declared_environment"
        "a_workload_ignoring_sigterm_is_sigkilled_after_the_pods_grace_and_reaped"
        # engenho-kubelet/tests/native_runs_postgres.rs
        "postgres_runs_natively_under_the_kubelet_from_a_nix_closure"
      ];
      sandboxSkips =
        if oracleBinaries == [ ]
        then throw ''
          engenho flake: .config/nextest.toml's profile.default.default-filter
          names no `binary(=…)`, so checks.tests would run the live-oracle
          binaries in a sandbox with no oracle. Name them there, one binary each.
        ''
        else oracleBinaries ++ needsNixDaemon;
      testsCargo = {
        runs = [
          {
            args = [ "--workspace" "--all-features" "--all-targets" "--no-fail-fast" ];
            harnessArgs = [ "--exact" ] ++ lib.concatMap (name: [ "--skip" name ]) sandboxSkips;
          }
          { args = [ "--workspace" "--all-features" "--doc" "--no-fail-fast" ]; }
        ];
        nativeBuildInputs = [ "pkg-config" ];
        buildInputs = [ "openssl" ];
      };

      base = substrate.rust.workspace {
        src = ./.;
        member = "engenho";
        tests.cargo = testsCargo;
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
      # The secondary members only; `mergeOutputs` below lays them over
      # base's packages per system.
      memberPackages = lib.genAttrs mcpSystems (system: {
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
      # the libssl3 CVEs were the Debian base image's package. This
      # dockerTools image carries no distro base and no dpkg package
      # database, so that package is gone.
      #
      # OpenSSL itself is NOT gone (measured 2026-09-19). `cargo tree -p
      # engenho-mcp` shows no openssl-sys because cargo resolves features
      # per package, but this image is built by lockfile-builder from
      # Cargo.gen.lock's ONE workspace-wide resolve. There, sui-store
      # 0.1.153 (engenho-fonte-cli -> with-sui-eval -> sui-eval) asks
      # reqwest for its default features, so reqwest carries
      # `default-tls`. The x86_64-linux engenho-mcp derivation depends
      # on rust_openssl-sys 0.9.116, built against
      # openssl-static-x86_64-unknown-linux-musl-3.6.2, so the binary
      # links OpenSSL statically, where no package database lists it.
      # The fix is sui 66a289f, which is in no sui release yet.
      # ci/no-c-tls.tlisp records sui-store as the one known source and
      # fails on any other.
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
      trio = (import "${substrate}/lib/module-trio.nix" {
        inherit lib;
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

        # The daemon ends on purpose: `engenho ctl runtime exit` with a halt
        # intent exits 0 and means "stay down", a relaunch intent exits 75
        # and means "bring me back" (docs/CONTROL-PLANE.md). Under the
        # service managers' `always` a halt is relaunched like a crash, so
        # both arms restart on failure only.
        daemonRestartPolicy = "on-failure";

        # engenho reads shikumi TieredConfig, so the YAML the trio deploys
        # IS its file tier. Defaults stay EMPTY on purpose: engenho's own
        # progressive fold already supplies prescribed defaults, and a key
        # written here would be read as an explicit operator opinion that
        # SUPPRESSES that fold — including the derived per-node cluster
        # name. See nix/typed-config.nix's `prune`.
        withShikumiConfig = true;
        shikumiDefaults = { };

        # ── ★ KillMode=control-group is LOAD-BEARING on Linux ─────────────
        # The native backend (engenho-kubelet/src/native_backend.rs) runs a
        # pod as a child process of the daemon and cannot re-adopt one after
        # a restart (`Readoption::Cannot`). What keeps a daemon restart from
        # leaving a second copy of every native workload running is systemd
        # killing the WHOLE cgroup when the daemon's unit stops — which is
        # KillMode=control-group, substrate's mkNixOSService default. Under
        # `process` or `none`, a restarted engenho would start every pod a
        # second time beside the orphaned first copy (two Postgres on one
        # data directory). The default is not enough: a later
        # `mkForce "process"` anywhere in the fleet would change it silently,
        # so evaluation refuses it.
        extraNixosConfigFn = { cfg, lib, config, ... }:
          lib.mkIf (cfg.daemon.enable or false) {
            assertions = [{
              assertion =
                (config.systemd.services.engenho-daemon.serviceConfig.KillMode or null)
                == "control-group";
              message = ''
                services.engenho: systemd.services.engenho-daemon must keep
                KillMode=control-group. engenho's native backend cannot re-adopt
                the workloads a previous daemon spawned, so only the cgroup kill
                on stop keeps a restart from running every native pod twice.
              '';
            }];
          };
      };
      # The typed surface rides with every arm, so a consumer gets one
      # import and gets eval-time type checking with it.
      withTyped = m: { imports = [ m ./nix/typed-config.nix ]; };

      pkgsFor = system: nixpkgs.legacyPackages.${system};

      # ── One merge for every output layer ───────────────────────────────
      # The layers used to be joined with a plain `//`, which replaces an
      # output that both sides declare. The module-trio layer declared
      # `checks` for typed-config, and so `checks.<system>` lost base's
      # `build` and `gen-confirm`: `nix flake check` built typed-config
      # alone (integration item I36).
      #
      # Per-system outputs now merge per system, and a name that two layers
      # both declare is an evaluation error: an output such as
      # `nixosModules`, or an attribute inside a per-system output such as
      # `checks.x86_64-linux.build`. No layer can replace another's output
      # without the flake failing to evaluate.
      perSystemOutputs = [ "packages" "checks" "apps" "devShells" ];
      disjointUnion = where: lhs: rhs:
        let clash = builtins.attrNames (builtins.intersectAttrs lhs rhs);
        in
          if clash == [ ] then lhs // rhs
          else throw ''
            engenho flake: ${where} declares ${lib.concatStringsSep ", " clash} in two
            layers. mergeOutputs never lets one layer replace another's output;
            declare it in one layer.
          '';
      mergeOutputs = lhs: rhs:
        let
          mergeShared = name:
            if builtins.elem name perSystemOutputs
            then lib.zipAttrsWith
              (system: sets: lib.foldl' (disjointUnion "${name}.${system}") { } sets)
              [ lhs.${name} rhs.${name} ]
            else disjointUnion "the flake" { ${name} = lhs.${name}; } { ${name} = rhs.${name}; };
        in
          lhs // rhs // lib.genAttrs (builtins.attrNames (builtins.intersectAttrs lhs rhs)) mergeShared;

      # ── checks.<system>.flake-surface ──────────────────────────────────
      # An evaluation-time check (it builds nothing) of what `nix flake check`
      # is given:
      #   1. mergeOutputs keeps both layers' checks for a system, and throws
      #      when two layers declare the same check.
      #   2. The flake's OWN output (`self`, so the real merge, not a copy of
      #      it) carries every check below on this system.
      # Put a plain `//` back between the layers and (2) fails evaluation,
      # naming the checks that went missing.
      requiredChecks = [ "build" "gen-confirm" "tests" "typed-config" ];
      mergeKeepsBoth =
        (mergeOutputs { checks.s = { a = 1; }; } { checks.s = { b = 2; }; }).checks.s == { a = 1; b = 2; };
      mergeRefusesClash =
        !(builtins.tryEval (builtins.deepSeq
          (mergeOutputs { checks.s = { a = 1; }; } { checks.s = { a = 2; }; }) true)).success;
      flakeSurface = system:
        let missing = lib.subtractLists (builtins.attrNames self.checks.${system}) requiredChecks;
        in
          if !mergeKeepsBoth
          then throw "engenho flake: mergeOutputs dropped a layer's checks for a system."
          else if !mergeRefusesClash
          then throw "engenho flake: mergeOutputs let one layer replace another's check."
          else if missing != [ ]
          then throw ''
            engenho flake: checks.${system} is missing ${lib.concatStringsSep ", " missing}.
            `nix flake check` would not build them. A layer merged with a plain
            `//` replaces every check the layers before it declared.
          ''
          else (pkgsFor system).runCommand "engenho-flake-surface" { } "touch $out";
    in
    lib.foldl' mergeOutputs { } [
      base

      {
        packages = lib.genAttrs mcpSystems (system:
          memberPackages.${system}
          // lib.optionalAttrs (builtins.elem system imageSystems) imageAttrs);
      }

      {
        checks = lib.genAttrs mcpSystems (system: {
          # Eval-time proof for the typed surface (IFD-free — it stubs the
          # trio's `settings` option rather than building engenho, so it runs
          # anywhere `nix flake check` does). Red-run verified: weakening the
          # kubeletBackend enum, and disabling the null-prune, each turn it red.
          typed-config = import ./nix/tests/typed-config-test.nix {
            pkgs = pkgsFor system;
          };
          flake-surface = flakeSurface system;
        });
      }

      {
        nixosModules.default = withTyped trio.nixosModule;
        nixosModules.engenho = withTyped trio.nixosModule;
        darwinModules.default = withTyped trio.darwinModule;
        darwinModules.engenho = withTyped trio.darwinModule;
        homeManagerModules.default = withTyped trio.homeManagerModule;
        homeManagerModules.engenho = withTyped trio.homeManagerModule;
      }
    ];
}
