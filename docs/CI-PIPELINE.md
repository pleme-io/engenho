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

  No longer a substrate shim (4757f50). After `pleme-io/actions/nix-setup`
  it runs `nix run github:pleme-io/gen -- confirm` (fatal: the
  `Cargo.lock` ↔ `Cargo.gen.lock` tie) and a non-fatal `nix flake
  check`, whose only check is the eval-time `checks.typed-config`. It
  compiles no Rust; `test.yml` is the gate that does.

### `.github/workflows/release.yml` — on `v*` tag

  Eleven jobs. Eight build and push exact tags only; three publish,
  in order, after the gate:

  | Job | Uses | What |
  |---|---|---|
  | `binary-engenho-mcp` | `rust-binary-release.yml`, `artifact-only`, `package: engenho-mcp` | Linux/macOS × x86_64/aarch64 binaries + `.sha256` → one workflow artifact |
  | `binary-engenho-cluster-config-render` | same, `package: engenho-cluster-config-render` | same |
  | `image-engenho-mcp-amd64` | `image-push.yml` | nix-built image → ghcr.io `:${tag}-amd64` |
  | `image-engenho-mcp-arm64` | `image-push.yml` | same, `-arm64` |
  | `image-engenho-cluster-config-render-amd64` | `image-push.yml` | same |
  | `image-engenho-cluster-config-render-arm64` | `image-push.yml` | same |
  | `chart` | `helm-chart-release.yml` | chart → ghcr.io OCI |
  | `image-manifest` | (inline — see below) | joins the per-arch tags into the `:${tag}` index |
  | `release-assets` | `ci/release-contract.tlisp` (verify) | needs all eight; fails unless every asset exists and every binary matches its `.sha256` |
  | `publish-release` | `pleme-io/actions/gh-release-create` | creates the GitHub Release in one call, from exactly the files `release-assets` checked |
  | `promote-latest` | `pleme-io/actions/release-promote` | after `publish-release`: moves `:latest` to the checked `:${tag}`, per image |

  **`:latest` moves only after the gate** (improvement plan T0.3a).
  Every image-push call sets `additionalTags: ''`: that reusable
  defaults it to `latest`, so before this each arch leg pushed
  `:latest` (the two legs raced for it) and `image-manifest` tagged the
  index `:latest` as well, with nothing checked first. Now
  `release-assets` waits on every publishing job, runs only for a `v*`
  tag, and derives every asset the release promises from release.yml
  (16 GitHub Release files, 4 arch images, 2 multi-arch indexes,
  1 chart), looking each up: the files in `release-files/`, the OCI refs
  with `docker buildx imagetools inspect --raw`. One missing asset fails
  it, and `promote-latest`, which takes its image list from
  `release-assets`' output, does not run.

  **The GitHub Release is created only after the gate** (improvement
  plan T0.3c). In rust-binary-release's default mode each build leg
  attaches its files to the tag's release as soon as that leg finishes;
  the first leg creates the release and it becomes Latest. Run
  35396851404 (v0.53.117) published it one second after the first upload
  and ended with 12 of 16 binaries, with both linux-aarch64 legs, all
  four image pushes and the chart had failed. Both binary jobs now run with
  `artifact-only: true`: no leg touches the release, and each job leaves
  one workflow artifact named by its `artifact-name` output. (The default
  mode also uploaded a `linux-x86_64-binary` artifact from both jobs of
  one run, a name conflict.) `release-assets` downloads both artifacts
  into `release-files/`, checks the 16 files there (each binary's sha256
  against its `.sha256`) along with the OCI refs, and outputs `files`,
  the staged paths it checked. `publish-release` downloads the same
  artifacts and hands exactly that list to `gh-release-create` with
  `if-exists: fail` (its default, `skip`, reports success on an existing
  release without uploading a file). `gh release create` with assets
  uploads to a draft and publishes after the last upload, deleting the
  draft if one fails; that is gh's behaviour as read from its source
  (cli/cli `pkg/cmd/release/create/create.go`), not something this repo
  checks. `promote-latest` needs `publish-release`, so no image becomes
  `:latest` for a release that has no GitHub Release.

  **`package:` on the binary jobs** (improvement plan T0.8). `--bin`
  alone builds one binary, but cargo unifies features across every
  workspace member; sui-store asks reqwest for its default (native-tls)
  features, so openssl-sys reached engenho-mcp's aarch64 legs, which the
  improvement plan (T0.8) names as why both failed. `-p` resolves
  features for the one package: `cargo tree
  -p engenho-mcp -e normal --target aarch64-unknown-linux-gnu -i
  openssl-sys` matches no package, and the same query with `--workspace`
  finds openssl-sys under engenho-mcp through reqwest (measured
  2026-09-19; same for engenho-cluster-config-render).

  `test.yml`'s `release-contract` job runs the same script in check
  mode on every push, so a binary job that leaves `artifact-only` or
  drops its `package`, a second writer of the GitHub Release, a new
  `latest`, a dropped `needs`, or a new publishing job with no row in
  its catalog fails before merge.

  The inline `image-manifest` job uses `docker buildx imagetools
  create` to assemble per-arch tags into the `:${tag}` index. It is a
  clear candidate for extraction to a future substrate
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

  * `test.yml` runs `cargo nextest run --workspace --all-targets
    --all-features` with substrate's pinned nextest. Which tests run
    is set by `.config/nextest.toml`, the same file substrate's release
    gate reads; the measured count is in CLAUDE.md § Test count.
  * `nix flake check` (non-fatal, in ci.yml) evaluates the flake and
    runs `checks.typed-config`; it compiles no Rust.
  * `ci/cargo-profiles.test.tlisp` (test.yml, job `ci-contract-tests`)
    runs `ci/cargo-profiles.tlisp` against the real `Cargo.toml` and
    workflows: `[profile.release]` stays at opt-level 3, the level the
    Nix-built daemon is compiled at; `[profile.stress]` declares
    opt-level 3 with debug assertions and overflow checks on; every
    step that raises `PROPTEST_CASES` selects it and never `--release`.
    deep-test.yml's `property-stress` job is that step, over the whole
    workspace. Values are compared by TOML type, as cargo reads them:
    `opt-level = "3"`, `opt-level = 3.0` and `debug-assertions = "true"`
    fail the check, as cargo refuses each of them.
  * `ci/doc-sources.test.tlisp` (test.yml, job `ci-contract-tests`)
    runs `ci/doc-sources.tlisp` against `docs/STATE-MACHINES.md` and
    `docs/TYPESCAPE.md`: every repository path they name exists, none
    starts at a crate directory outside the workspace, every
    `path.rs::Item` names an item that file declares, and every row of a
    table with a `Source` column names a path. Declarations are matched
    by text, so an item declared through a macro is not seen. The suite
    also fails if `engenho-machines` comes back (improvement plan T5.3).
  * `ci/no-c-tls.test.tlisp` (test.yml, job `ci-contract-tests`) runs
    `ci/no-c-tls.tlisp` against the real `Cargo.toml`, every member
    manifest and `Cargo.lock` (improvement plan T0.8). The workspace
    `reqwest` line turns reqwest's default features off and names
    `rustls-tls`, `rustls-tls-native-roots`, `charset`, `http2` and
    `system-proxy`; no member asks reqwest for its default or native-tls
    features; `Cargo.lock` holds reqwest, rustls and hyper-rustls; and
    `openssl-sys`, `openssl`, `native-tls`, `hyper-tls` and
    `tokio-native-tls` are locked only through a source the check's
    attribution table names. It reads `Cargo.lock` because that is
    resolved for every target at once: a host-only
    `cargo tree -i openssl-sys` prints nothing on darwin, where
    native-tls uses Security.framework. **Not closed yet:** sui-store
    0.1.153 (through `engenho-fonte-cli`'s `with-sui-eval`) still asks
    reqwest for its defaults, so feature unification turns `default-tls`
    on in every workspace build, and `Cargo.gen.lock`'s resolve, which
    Nix builds the daemon from, carries it. The fix is sui 66a289f; no
    published sui release has it as of 0.1.219. Once one does, move the
    lock to it and delete the attribution rows; the check then fails
    until every row is gone, and from then on it is a plain ban.
  * `ci/release-contract.tlisp` (test.yml, job `release-contract`)
    checks that release.yml creates the GitHub Release only in
    `publish-release` and moves `:latest` only in `promote-latest`,
    both after `release-assets`, and that every binary job is
    artifact-only and names the package that declares its binary;
    `ci/release-contract.test.tlisp` (job
    `ci-contract-tests`) shows each of its rules firing on a fixture
    with that defect.
  * `ci/replay-backward.tlisp` (test.yml, job `replay-backward`) is
    the backward half of the restart oracle's replay case (improvement
    plan T3.1 case 5). It records a raft log with this tree's
    `record_replay_fixture` into `ENGENHO_REPLAY_FIXTURE_DIR`, checks
    the previous release (the newest `v*` tag reachable from `HEAD^`)
    out into a git worktree, and runs that release's own case 5 on the
    log: a red means the previous release cannot replay what this tree
    writes, so a rollback after a crash would not boot the same store.
    Each tree builds into its own target directory. Until a release
    contains the harness (v0.53.118 and older do not), the job reports
    `predates-harness` with a warning and replays nothing; ancestry,
    not a version number, decides that. `ci/replay-backward.test.tlisp`
    (job `ci-contract-tests`) checks every verdict and that the job
    exists with full history and credentials.
  * `.github/workflows/mutation.yml` runs `cargo mutants` over the
    files in `ci/seam-files.txt`: every mutant nightly, the changed
    lines on a push or PR that touches a seam. A surviving mutant fails
    the leg unless `ci/mutants-allowlist.txt` has a row saying why; the
    judge is `ci/mutation-gate.tlisp`. It is hand-authored, like
    test.yml: substrate has no reusable for it, and
    `pleme-io/actions/mutation-test` cannot gate (it ignores
    cargo-mutants' exit status and reads survivors from the summary
    text). test.yml's `ci-contract-tests` job lints the two lists on
    every push.
  * `nix build .#default` validates the workspace builds.

## What release produces

On every `v*` tag:

  * GitHub Release with 16 files, created by `publish-release` in one
    call once every asset is checked: 8 binaries plus a .sha256 each,
      engenho-mcp-{linux-x86_64, linux-aarch64, macos-x86_64, macos-aarch64}
      engenho-cluster-config-render-{... same legs}
  * 6 OCI refs on ghcr.io:
      ghcr.io/pleme-io/engenho-mcp:{${tag}-amd64, ${tag}-arm64,
                                    ${tag} (multi-arch)}
      ghcr.io/pleme-io/engenho-cluster-config-render:{... same set}
    and `:latest` on each image once promote-latest has run.
  * 1 OCI Helm chart:
      ghcr.io/pleme-io/engenho/charts/engenho:${tag-without-v}

All on the free-tier `ghcr.io` (public packages).
