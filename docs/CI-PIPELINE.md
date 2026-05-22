# engenho — CI/release pipeline architecture

> **Prime directive applied:** every workflow file in `.github/workflows/`
> is a **thin shim** delegating to a substrate reusable workflow at
> `pleme-io/substrate/.github/workflows/`. Hand-authored composite
> steps inside `engenho/.github/workflows/` are drift and get
> refactored into substrate primitives on sight.

## Why

Per the org's CLAUDE.md (Pillar 9 + ★ pleme-actions):

> *Any Nix expression repeating across repos → a helper function in
> substrate/lib/. Any GitHub Actions pattern → a reusable workflow
> in substrate/.github/workflows/. Hand-authored composite actions
> in workflows are drift.*

Every other pleme-io repo follows the same pattern. Pangea-operator
ships 3 jobs (binary / image / chart) that are all 5-line shims;
tatara similarly. Engenho follows the same shape.

## Engenho's two workflows

### `.github/workflows/ci.yml` — every commit

  Delegates to: `pleme-io/substrate/.github/workflows/cargo-ci.yml@main`

  Which runs `nix flake check` — that evaluates substrate's
  `rust-workspace-release-flake` helper, builds via crate2nix,
  and runs whatever `checks.<system>.*` the flake exposes.

  Total engenho file size: 21 lines (95% header + the `uses:` clause).

### `.github/workflows/release.yml` — on `v*` tag

  Eight jobs, **seven of which are substrate-reusable-workflow shims**:

  | Job | Substrate workflow | What |
  |---|---|---|
  | `binary-engenho-mcp` | `rust-binary-release.yml` | Linux/macOS × x86_64/aarch64 binaries → GH Release |
  | `binary-engenho-cluster-config-render` | `rust-binary-release.yml` | same |
  | `image-engenho-mcp-amd64` | `image-push.yml` | nix-built image → ghcr.io |
  | `image-engenho-mcp-arm64` | `image-push.yml` | same |
  | `image-engenho-cluster-config-render-amd64` | `image-push.yml` | same |
  | `image-engenho-cluster-config-render-arm64` | `image-push.yml` | same |
  | `chart` | `helm-chart-release.yml` | chart → ghcr.io OCI |
  | `image-manifest` | (inline — see below) | combines per-arch tags into multi-arch manifest |

  The one inline job (`image-manifest`) uses `docker buildx
  imagetools create` to assemble per-arch tags into a single
  `:${version}` + `:latest` multi-arch manifest. This is a clear
  candidate for extraction to a future substrate
  `image-manifest.yml` reusable workflow (see "Gaps" below).

## Substrate primitives in use

| Workflow | Source |
|---|---|
| `cargo-ci.yml` | pleme-io/substrate/.github/workflows/cargo-ci.yml |
| `rust-binary-release.yml` | pleme-io/substrate/.github/workflows/rust-binary-release.yml |
| `image-push.yml` | pleme-io/substrate/.github/workflows/image-push.yml |
| `helm-chart-release.yml` | pleme-io/substrate/.github/workflows/helm-chart-release.yml |

These workflows are themselves built on substrate's `rust-tool-image-flake.nix`
+ `forge` for image builds, and `pleme-io/actions/nix-flake-check@v1` for CI.

## Gaps that should be filled in substrate

If the same pattern shows up in engenho + 1 other repo, it becomes a
substrate candidate per the third-site rule.

| Gap | Currently inline in engenho | Substrate proposal |
|---|---|---|
| Multi-arch manifest creation from per-arch tags | `image-manifest` job | `pleme-io/substrate/.github/workflows/image-manifest.yml@main` taking `imageName` + `tag` + `archs` list |
| Multi-binary workspace release | 2× `rust-binary-release.yml` calls | `pleme-io/substrate/.github/workflows/rust-workspace-binary-release.yml@main` taking `binaries: [name1, name2, ...]` list (eliminates per-binary job duplication in workspaces like engenho + nexus + tatara) |

These gaps are tracked here for future substrate PRs. They aren't
blockers — the current 8-job shim works end to end. The proposals
above just reduce engenho's release.yml from 146 lines to ~40.

## Reusable actions vs reusable workflows

Per the org rule, **engenho creates ZERO custom GitHub Actions**.
Any new action it'd need belongs in `pleme-io/actions/` or
`pleme-io/pleme-actions/` (the canonical homes). The substrate
workflows here already compose `pleme-io/actions/*@v1` actions
under the hood; engenho doesn't need to.

## Secrets propagation

All substrate workflow calls use `secrets: inherit`. This is a
GitHub Actions quirk — reusable workflows don't auto-inherit
the caller's `GITHUB_TOKEN`. Without `secrets: inherit`, image
push + helm chart push would fail with `unauthorized`.

## What CI exercises

  * `cargo test --workspace` runs all 274 unit + integration tests
    across the 7 workspace crates (types, mcp, revoada, store,
    apiserver, teia, scheduler).
  * `nix flake check` validates the flake outputs (apps, packages,
    overlays).
  * `nix build .#default` validates the workspace builds.

## What release produces

On every `v*` tag:

  * GitHub Release with 6 binary artefacts:
      engenho-mcp-{darwin-arm64, linux-x86_64, linux-arm64}
      engenho-cluster-config-render-{...}
      (plus .sha256 sidecars)
  * 6 OCI images on ghcr.io:
      ghcr.io/pleme-io/engenho-mcp:{${tag}-amd64, ${tag}-arm64,
                                    ${tag} (multi-arch), latest}
      ghcr.io/pleme-io/engenho-cluster-config-render:{... same set}
  * 1 OCI Helm chart:
      ghcr.io/pleme-io/engenho/charts/engenho:${tag-without-v}

All on the free-tier `ghcr.io` (public packages).
