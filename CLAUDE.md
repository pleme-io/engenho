# engenho — typed, attested, Rust-native Kubernetes runtime

> **★★★ CSE / Knowable Construction.** This repo operates under
> **Constructive Substrate Engineering** — canonical specification at
> [`pleme-io/theory/CONSTRUCTIVE-SUBSTRATE-ENGINEERING.md`](https://github.com/pleme-io/theory/blob/main/CONSTRUCTIVE-SUBSTRATE-ENGINEERING.md).
> The Compounding Directive (operational rules: solve once,
> load-bearing fixes only, idiom-first, models stay current, direction
> beats velocity) is in the org-level pleme-io/CLAUDE.md ★★★ section.
>
> **Destination doc:** [`pleme-io/theory/ENGENHO.md`](https://github.com/pleme-io/theory/blob/main/ENGENHO.md).
> Read it before touching anything load-bearing — typed surface in §II,
> wire-compat in §III, compounding hierarchy in §IV, testing pyramid in
> §V, determinism contract in §VI, verifiability surface in §VII, caixa
> integration in §VIII, tatara integration in §IX, phases in §X,
> open questions in §XII, anti-patterns + risks in Appendix B.
>
> **Lean engineering rationale:** [`docs/LEAN.md`](./docs/LEAN.md) —
> the decision matrix for every K8s subsystem (what engenho owns, what
> it composes from Rust OSS, what it consumes from pleme-io shared
> libs). Read after ENGENHO.md.
>
> **Local roadmap:** [`docs/M0-ROADMAP.md`](./docs/M0-ROADMAP.md) — the
> M0.0.1 → M0.0.4 step-by-step inside this repo.

The runtime layer of pleme-io's Pillar 7 (Kubernetes control). Pangea
declares; magma realizes (cloud); **engenho runs the land** (containers
on real hardware). Sibling primitive to magma; never duplicates scope —
see theory/ENGENHO.md §XI.1.a for the explicit boundary table.

## Architecture

See [`README.md`](./README.md) for the target workspace shape. Today
(M0.0 + M0.0.2):

  - `engenho-types` — typed K8s resource catalog. 18 kinds scaffolded;
    Pod has the **M0.0.2 typed bullseye** (PodSpec + PodStatus +
    Container + ContainerPort + EnvVar + PodCondition + ContainerStatus
    + PodPhase). Other kinds carry opaque spec/status pending M0.0.3
    codegen catching up.
  - `engenho-cluster-config` + `engenho-cluster-config-render` —
    typed k3s/engenho cluster bootstrap config. Renders config.yaml +
    server-args.txt + manifests.
  - `engenho-kube-client` — reqwest+rustls impl of the KubeClient
    trait. **Live-validated against the engenho-local cluster's
    podinfo replicas via `tests/live_engenho_local.rs`.**
  - `engenho-kube-codegen` — codegen scaffold for M0.0.3 typed
    spec/status expansion across the catalog.
  - `engenho-mcp` — MCP server (5 tools: cluster_status, cluster_config,
    cluster_kubeconfig, cluster_snapshot_meta, **cluster_pods** — the
    last goes through the typed Pod catalog + engenho-kube-client to
    the live cluster). Operator surface exposed to Claude Code /
    Cursor / OpenCode / Gemini via anvil.
  - `engenho` — placeholder binary; composes apiserver + datastore at
    M0.1.

## The non-negotiable rule

**No hand-authored Kubernetes resource types.** Per theory/ENGENHO.md
§IV, every K8s kind is mechanically emitted by `kube-forge` from
upstream OpenAPI v3. Hand-authoring a `Pod` / `Deployment` / `Service`
struct is a CI-rejected anti-pattern — extend the generator instead.

The single transient exception is M0.0.1 (the Pod bullseye). M0.0.1
hand-authors the full Pod shape as the byte-for-byte target that
M0.0.3's generator must reproduce. Once M0.0.3 lands, the hand-author
is deleted and replaced by generator output. After M0.0.3, the rule
holds forever.

Same shape as Crossplane's ban on `format!()` of Go syntax
([`pleme-io/theory/CONSTRUCTIVE-CROSSPLANE-PROVIDERS.md`](https://github.com/pleme-io/theory/blob/main/CONSTRUCTIVE-CROSSPLANE-PROVIDERS.md))
and NixAST's ban on string-concat of Nix
([`pleme-io/theory/NIX-AST.md`](https://github.com/pleme-io/theory/blob/main/NIX-AST.md)).

## One binary — the shape every change is measured against

**engenho IS the Kubernetes system, it does not supervise one.** One
statically-linked binary, no second daemon, no sidecar, no shell. When you
add a capability the default is that it lives in this workspace, in Rust,
in-process — reached by a trait call, not by a socket or a subprocess.

Destination: [`pleme-io/theory/BUTAI.md`](https://github.com/pleme-io/theory/blob/main/BUTAI.md)
§1. What is still outside the binary today:
[`docs/PITR-READINESS.md`](./docs/PITR-READINESS.md) §4.

**For a new capability, in order:**

1. **A Rust module in this workspace** — the route the store, scheduler,
   controllers, kubelet, CSI provisioner and snapshot controller all took.
2. **A plugin CONTRACT the ecosystem ships binaries for** (CNI, CSI) —
   implement the *seam* in-process and let someone else's binary plug into
   it. The interface outlives the technology, so satisfying the seam buys
   ~150 drivers without engenho implementing any of them.
3. **A fact about the WORLD** — type it as a permanent front door, and say
   in the code that it is one.

**Embedding a CAPABILITY is not embedding someone else's IMPLEMENTATION.**
The anti-patterns below forbid vendoring upstream Go; this section requires
owning the capability in Rust. They are one rule seen twice — engenho
implements what it runs. Re-deriving is embedding; copying is not.

### ★★ Replacing a component means inheriting its PROMISES, not just its API

The API surface is the easy half and the half that gets tested. The hard
half is every **default and side effect** a controller was written against
without ever naming it — and those fail *inside somebody else's binary*,
with engenho named nowhere.

Four measured on rio 2026-09-15, the first day engenho ran a real
controller fleet alone:

| what engenho did | what the replaced component promised | how it surfaced |
|---|---|---|
| left CRD `default:` unapplied | the apiserver defaults on decode | source-controller nil-deref'd `spec.timeout` and panicked every reconcile, reporting "building artifact" forever |
| created `emptyDir` `0755 root:root` (podman's default) | kubelet creates it `0777` so any `runAsUser` can write | the pod stayed **Running**, the kubelet reported **success**, and the container got EACCES |
| appended to `KUBE-SERVICES` without creating it | kube-proxy owns that chain and its hooks | Service routing worked off the *dead* k3s rules still in the kernel, and would have vanished at the next reboot |
| left `Secret.type` unset | the apiserver defaults it to `Opaque` | a consumer that branches on type takes its not-mine path and does nothing, silently |

**The test to run before claiming a capability is embedded:** name what the
thing you replaced did that nobody writes down — its defaults, the modes it
sets, the state it installs in the kernel, the fields it fills on decode.
Then ask whether engenho does it, on a node where the replaced component
has **never run**. rio could not answer that question honestly until k3s
was stood down, because its corpse was still providing two of the four.

★ And the diagnostic tell: when a Kubernetes-ecosystem binary panics,
hangs, or no-ops against engenho and its own logs blame nothing, suspect an
unkept promise before suspecting the binary. Three of the four above were
first read as "Flux is broken".

**The exception is a world-fact, never a convenience.** `engenho → CRI →
containerd` stays reachable **by design, permanently** — not because butai
cannot replace it, but because a cluster we do not own may already run it and
because upstream publishes OCI images rather than Nix derivations. Per
[`MIRAGEM`](https://github.com/pleme-io/theory/blob/main/MIRAGEM.md), a limit
phrased in terms of *our own* abstractions is ours to dissolve; one phrased as
a fact about *the world* gets typed. "We shell out to podman" is ours. "Docker
Hub serves images" is not. Deleting the CRI backend to chase a purity number
is the misreading of this section.

A build-time tool is not a violation: `engenho-kube-codegen` and
`engenho-cluster-config-render` never ship in the runtime path.

## Build

```bash
cargo build --workspace                    # debug build
# THE gate command — mirrors .github/workflows/test.yml exactly. The tool is
# substrate's pinned nextest (`nix run github:pleme-io/substrate#cargo-nextest
# -- nextest run …` if you have none; .config/nextest.toml requires >= 0.9.114):
cargo nextest run --workspace --all-targets --all-features \
  --locked --no-fail-fast --no-tests=fail
cargo test --workspace --all-features --locked --doc   # doctests (see below)
cargo fmt --all -- --check                 # formatting gate
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings

nix build                          # release build via substrate.rust.workspace
nix flake check                    # ⚠ compiles no Rust — see § CI + gating

# property tests at 4,096 cases, as deep-test.yml runs them:
PROPTEST_CASES=4096 cargo nextest run --workspace --all-targets \
  --all-features --locked --cargo-profile stress --no-fail-fast
```

Every flag above is load-bearing:

- **`--all-features`** — engenho gates real code behind `with-tameshi`,
  `with-sui-eval`, `with-shikumi`, `with-revoada`,
  `with-engenho-kube-client`, `openapi-roundtrip`, `bit-repro`, `mock`.
  Default features leave all of it uncompiled and unrun.
- **`--all-targets`** — includes `tests/`, `examples/`, `benches/`.
  **It EXCLUDES doctests**, and nextest never runs them, which is why
  `--doc` is a separate `cargo test` line.
- **`--no-fail-fast`** — without it cargo stops at the first failing
  *binary*. Measured: a plain run aborted at `engenho-diff`
  (alphabetically first to fail) and reported **881** tests; the same
  run with `--no-fail-fast` reports **3,169**. A count taken without
  this flag is not a count of the suite.
- **`--locked`** — a drifted `Cargo.lock` fails loudly instead of being
  silently re-resolved.
- **`--no-tests=fail`** — the release gate's anti-vacuity assertion. A
  run that selects zero tests is red; `cargo test` exits 0 on it.
- **No exclusion flag, on purpose.** Which tests run is decided by
  [`.config/nextest.toml`](./.config/nextest.toml), which nextest reads
  on its own. That is what makes test.yml, `deep-test.yml`, substrate's
  release gate and a local run select the same tests. See § Live-oracle
  tests.

### Cargo profiles: the daemon never reads them

`nix build` compiles every crate with nixpkgs' `buildRustCrate`
(`release = true`), which calls rustc directly with `-C opt-level=3` and
no LTO. No `[profile.*]` in `Cargo.toml` reaches the daemon. Cargo reads
them for the two binaries in each GitHub Release (`cargo build
--release`), the property-stress lane and every test run:

| Profile | Used by | Settings that matter |
|---|---|---|
| `release` | release.yml's engenho-mcp and engenho-cluster-config-render | `opt-level = 3`, the daemon's level. It was `"z"`, 1.7-1.9x slower on serde_json round trips |
| `stress` | deep-test.yml `property-stress`, `--cargo-profile stress` | `opt-level = 3` with `debug-assertions` and `overflow-checks` on. `--release` turned both off |

`ci/cargo-profiles.tlisp` checks both on every push (its suite runs in
test.yml's `ci-contract-tests`). It also fails on a step that raises
`PROPTEST_CASES` without the stress profile, and on a
`CARGO_PROFILE_RELEASE_*` / `CARGO_PROFILE_STRESS_*` env or a
`.cargo/config` `[profile]` that would override either from outside
`Cargo.toml`. Under nextest, `--profile` names a nextest profile; the
cargo profile is `--cargo-profile`.

### Test count (measured 2026-07-27, not estimated)

An earlier version of this file claimed *"16 unit + 5 manifest + 3
proptests"* — **24**. The real figure is **3,211**, off by ~134x. A
claimed count nobody can reproduce is the reliable tell that a gate is
not being run; this one had not been reproducible for months.

| Leg | Binaries | Tests | Pass | Fail | Ignored |
|---|---|---|---|---|---|
| Gate scope (`--workspace --exclude engenho-diff --all-targets --all-features`) | 162 | 3,147 | 3,140 | 0 | 7 |
| Doctests (`--doc`) | — | 42 | 1 | 0 | **41** |
| `engenho-diff` (live-oracle, excluded from CI) | 4 | 22 | — | 4 | — |
| **Total** | | **3,211** | | | |

Source attributes in-tree: 2,306 `#[test]` + 824 `#[tokio::test]` =
3,130 (exclude `target/` when counting, or the number inflates).

The table was taken under the old `cargo test --exclude engenho-diff`
gate. The gate now runs nextest over `.config/nextest.toml`, which skips
only the four oracle binaries, so engenho-diff's **34** mocked library
unit tests now run in the gate (measured 2026-09-19 with nextest 0.9.136,
`-p engenho-diff --all-features`: 34 run, 34 pass, 4 binaries skipped).
The whole workspace has not been re-counted under nextest yet.

The doctest leg is **41 of 42 `ignore`d** — close to a vacuous guard
today. It is wired anyway so the next real doctest lands guarded, and
so the 41 are visible as debt rather than counted as coverage.

### Live-oracle tests

`engenho-diff`'s four integration binaries are a *differential* suite:
each drives the same operation against engenho-in-process **and a live
Kubernetes oracle cluster**, then diffs the responses. They resolve
`ENGENHO_ORACLE_KUBECONFIG`, falling back to
`$HOME/.kube/engenho-local-tunnel.yaml`, and are written to **fail
loud, never silently skip**. No oracle exists on a CI runner, so they
are kept out **of execution only**, by name, in
[`.config/nextest.toml`](./.config/nextest.toml) — the one
test-selection contract, with the full rationale as its header. The
crate still compiles on every PR (`test.yml` runs
`cargo test -p engenho-diff --no-run` first), and its mocked library
unit tests run in the gate like any other.

Where an oracle exists:
`ENGENHO_ORACLE_KUBECONFIG=<kubeconfig> cargo nextest run --profile oracle`.
The `live-oracle` test group runs them one at a time, because they share
one cluster. Not `#[ignore]`d (that would hide them from the operator,
where the oracle *does* exist and they are the entire point).

## CI + gating

**`nix flake check` compiles no Rust, and neither does `ci.yml`.**
`ci.yml` runs `gen confirm` (fatal: the `Cargo.lock` ↔ `Cargo.gen.lock`
tie) and a non-fatal `nix flake check`. That builds only
`checks.<system>.*`, and the flake declares one:
`checks.<system>.typed-config` (since 51e702a), an eval-time test of the
module trio's typed options that stubs `settings` instead of building
engenho. On sibling repo `forge`, a flake with no Rust checks was probed:
clean tree → exit 0; `compile_error!` in a test module → exit 0; literal
non-Rust garbage in a function body → **exit 0**.

The load-bearing fix — the flake exposing Rust `checks` — now exists
upstream and engenho has **not adopted it yet**. Substrate 83bab67
(2026-09-19) added an opt-in runner: `substrate.rust.workspace {
tests.cargo.runs = [ … ]; }` yields `checks.<system>.tests`, which runs
`cargo test --frozen` over a vendor directory built from this repo's
`Cargo.lock`, and reads this repo's `[profile.*]` itself (so
`--profile stress` means `[profile.stress]`). engenho's `flake.lock`
pins substrate fcd3514, which predates it. Adoption is pending: bump
the substrate input, declare `tests.cargo` in `flake.nix`, and check
that the five private git dependencies vendor inside Nix. Only then can
test.yml's cargo legs fold into `nix flake check`. Building an in-repo
`checks` by hand would be a second, divergent Rust build path, which
Operating Principle #1 forbids.

| Workflow | Trigger | Scope | Blocking |
|---|---|---|---|
| `test.yml` | push + PR | **the real gate** — whole workspace under substrate's nextest (selection from `.config/nextest.toml`), all-features, all-targets, + doctests on cargo, + `engenho-diff` compile-only, + fmt + clippy, + `ci/nix-on-runner.tlisp` (Nix installed only via `pleme-io/actions/nix-setup`, before any step needing it), + `ci/release-contract.tlisp` (release.yml moves `:latest` only after the gate), + `ci-contract-tests`: every `ci/*.test.tlisp` (release-contract, mutation-gate, cargo-profiles, whose suite checks `Cargo.toml`'s release and stress profiles and every stress lane) and a lint of the mutation gate's two lists | yes |
| `release.yml` | `v*` tag | 2 binaries, 4 arch images, 2 multi-arch indexes, 1 chart, exact tags only; then `release-assets` (needs every publishing job, finds all 23 assets) and `promote-latest` (moves `:latest` per image). A red leg or a missing asset leaves `:latest` where it was | — |
| `deep-test.yml` | schedule + dispatch | breadth — macOS leg, the whole workspace under `[profile.stress]` with `PROPTEST_CASES=4096`, coverage artifact, `cargo audit` | no |
| `mutation.yml` | schedule + dispatch; push + PR touching a seam | `cargo mutants` over `ci/seam-files.txt`: every mutant nightly, the changed lines on a push. A surviving mutant fails unless `ci/mutants-allowlist.txt` says why (`ci/mutation-gate.tlisp`) | yes, on a push that touches a seam |
| `ci.yml` | push + PR | `gen confirm` (fatal lock tie) + `nix flake check` (non-fatal; runs `checks.typed-config`, compiles no Rust) | fails only on `gen confirm` |

`deep-test.yml` deliberately has **no** `push`/`pull_request` trigger:
it ran on every PR while permanently red, which is how a never-green
gate came to sit beside a shim that compiled nothing without anyone
noticing either. It had **21 runs and 21 failures — never once green**
since 2026-05-23. `cargo audit` lives there rather than on the PR path
because it fails on advisories published against the dependency tree
(i.e. on the calendar, with no change to this repo); making it blocking
manufactures exactly the permanently-red gate this split removes.

**Private deps resolve in CI; what is still red** (runs 35418242848
and 35421804575, 2026-09-19). `Cargo.lock` has git deps on five
pleme-io repos (`tameshi`, `cofre`, `promessa`, `sui`, `tatara`), and
cargo resolves the whole lock graph whatever the features. BOT_PAT
arrives set (`bot-pat: ***` in the step log) and they resolve: engenho
is **public**, and on the GitHub Free plan an org secret reaches public
repos. The org posture catalog (`pangea-architectures`
`workspaces/pleme-io-opensource/org.yaml`) declares it `visibility:
public` with `actions_secrets: []`. If engenho goes private again, the
fix is an `actions_secrets` row there, never `gh secret set`.

The test leg's reds in both runs, 7 tests in 4 binaries:

| Binary | Tests | Cause |
|---|---|---|
| `engenho-kubelet --test native_runs_a_real_closure` | 2 | no `nix` on the runner |
| `engenho-kubelet --test native_runs_postgres` | 1 | no `nix` on the runner |
| `engenho-runtime --test m0_1_single_node_convergence` | 1 | `StoreStillShared { strong_count: 2 }` at shutdown |
| `engenho-runtime --test m0_6_namespaced_reconcile` | 3 | `StoreStillShared { strong_count: 2 }` at shutdown |

The nix tests fail rather than skip on purpose; `test.yml` now installs
Nix before the test leg through `pleme-io/actions/nix-setup`. The
StoreStillShared failures are a code defect (improvement plan T2.1).

Local builds do **not** reproduce credential problems: `~/.cargo/git`
holds credentialed checkouts, so a workstation can be green while CI is
red.

### Mutation gate — tests that pin behaviour (plan T0.6)

`ci/seam-files.txt` lists the files where engenho decides what an
observation means (gc, watch_driver, probe, backoff, native_backend).
On 2026-09-19, 25 of 99 viable mutants in the first four survived: the
tests executed that code and would not have noticed it change.
`mutation.yml` runs `cargo mutants` over each seam nightly, and over the
changed lines of any push or PR that touches one. A surviving mutant
fails the leg unless `ci/mutants-allowlist.txt` has a row
(`<path>: <mutant description> # why: <reason>`). A full run also fails
on an allowlist row that no longer matches a survivor. The judge reads
every outcome in `outcomes.json`: a mutant whose test run ended on a
signal (`Failure`) appears in no `.txt` file and in none of the totals,
so a gate built on either would miss it. A failed baseline, missing
tools, an unfinished run or files that disagree make the leg **blind**
(exit 3), never green. Listing a new seam, or removing a mutant's row
once a test kills it, goes in the same commit as the code change.

**Standing rule:** a commit that touches a seam ends with a mutation
pass over what it touched, from the repository root:

```bash
MUTATION_GATE_MODE=run MUTATION_GATE_FILE=engenho-kubelet/src/probe.rs \
  MUTATION_GATE_BASE=HEAD~1 tatara-script ci/mutation-gate.tlisp
```

Leave out `MUTATION_GATE_BASE` for every mutant in the file. Outside
GitHub Actions cargo-mutants builds in a copy of the tree. The gate is
only as green as `test.yml`: a package whose own tests are red makes
every leg over it blind.

## Substrate integration (no escape hatches)

| Primitive | How engenho uses it |
|---|---|
| `substrate.rust.workspace` | `flake.nix` — the gen/`Cargo.gen.lock` pattern (routes `mk-rust-workspace.nix` → `lockfile-builder.nix`; no crate2nix, no committed `Cargo.nix`). Note it exposes **no Rust `checks`** — see § CI + gating |
| `tatara` | engenho is a tatara binary; every subsystem under `defguest` daemon mode (small surgery in `pleme-io/tatara/docs/daemon-supervision.md`) |
| `shigoto` | Every controller's reconcile loop is a `shigoto::Dag`; watch dispatch is fan-out wave execution |
| `shikumi` | All operator config (engenho.lisp, per-component YAML overrides) typed |
| `cofre` | kubelet has cofre client; k8s Secret objects carry references, not plaintext |
| `tameshi` / `sekiban` / `kensa` | In-process admission chain + BLAKE3 receipts on every artifact |
| `arch-synthesizer` | engenho-types re-exports for typescape participation |
| `forge-gen` + `kube-forge` (M0.0.3) | OpenAPI v3 → Rust source; the load-bearing generator |
| `nix-ast` | All emitted Nix (in-process flake fragments) typed |
| `pleme-actions` | CI workflows migrate to `pleme-io/rust-ci-action@v1` once published |
| `repo-forge` archetype | `rust-substrate-workspace-tool` (back-pointer: `repo-forge.lisp`) |

## Per-crate notes (M0.0)

- **engenho-types** owns the typed K8s resource catalog. Today: just
  the `KubeResource` trait + `meta/v1` types + GVK helpers + vendored
  OpenAPI v3 schemas. M0.0.4 expansion: ~150 kinds across ~16 groups
  (`core_v1`, `apps_v1`, `rbac_v1`, `networking_v1`, `storage_v1`,
  `coordination_v1`, `apiextensions_v1`, …). Every kind generated, none
  hand-authored.
- **engenho** is the placeholder binary that today prints the destination
  pointer. At M0.1 it composes apiserver + datastore; later milestones
  add controllers / scheduler / kubelet / kube-proxy / DNS / local-path
  / CA. Per theory/ENGENHO.md §IX, the binary's main loop is
  `tatara_reconciler::run(processes)` — engenho describes the cluster
  as typed processes, tatara runs them.

## Determinism contract (§VI)

| Artifact | What's reproducible | How verified |
|---|---|---|
| `engenho-types` generated source | bit-identical from same OpenAPI input | `kube-forge --check` (M0.0.3+) |
| Vendored OpenAPI schemas | bit-identical from `MANIFEST.yaml` BLAKE3 | `tests/vendored_openapi_blake3.rs` (live now) |
| `ObjectMeta` serialization | byte-deterministic across runs | proptest in `tests/determinism_proptest.rs` (256 cases × 3 properties = 768 random cases per run) |
| `nix build .#default` | byte-identical from same git rev | substrate flake's check |

### ★ Reading the codegen gate — two traps that cost a day

**`cargo ... 2>&1 | grep -E "^error"` matches NOTHING, even on a failed
build.** Cargo emits ANSI colour, so the line is
`\x1b[1m\x1b[38;5;9merror[E0432]...` and does not *start* with `error`.
The empty output reads as success while a stale binary sits in `target/`.
Measured 2026-08-30: three "no errors" reports from builds that had failed,
the worst of which sent a live test against a binary 6 minutes older than
the source and produced a diagnosis of a bug that did not exist.

```
cargo build 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | grep -E "^error" -A5
```

**A fast build is the tell** — `Finished in 0.34s` after editing a large file
means nothing recompiled. Check `ls -lT target/debug/engenho` against the
source mtime before trusting any live test.

**`--check` prints DRIFT to stderr, and `$?` after a pipe is the pipe's
status, not the program's.** `... --check 2>/dev/null | head` reported clean
while 46 files were drifting. Redirect stderr to a file and read the exit
code directly.

### ★ A permanently-red gate is an incident, not a quirk

`--check` was unpassable from `16d0fd5` (a workspace `cargo fmt` that swept
this generated directory) until 2026-08-30. It reported 46 drifting files of
which **40 were whitespace**. Behind them hid three hand-authored files
inside `generated_v1_34/` — `rbac_v1/policy.rs`,
`apps_v1/{deployment,replicaset}_spec.rs` — which a regen DELETES, and which
is how a routine regeneration broke `engenho-apiserver`.

They existed to work around two generator bugs that were never fixed:
`snake_case` mangling `nonResourceURLs` → `non_resource_ur_ls` (the WIRE name
was right, so it was invisible in JSON and fatal in Rust), and TypeMeta being
stripped from nested structs where `kind` is real data (`RoleRef`, `Subject`).

Emission now runs through `prettyplease` — a Cargo-pinned library, **never a
`rustfmt` subprocess**, since `--check` compares bytes and must not depend on
whatever is on PATH. If you find `--check` red, fix the noise before reading
the signal.

## Anti-patterns

- **Hand-authoring K8s resource types.** The non-negotiable rule above.
- **`format!()` of K8s YAML.** Use `engenho-types` typed structs and
  serde, never sprintf strings.
- **Bypassing cofre for secrets.** Kubelet must materialize secrets
  through cofre; no plaintext k8s `Secret` object in flight.
- **Embedding upstream Go.** Engenho ships zero vendored Go (theory/ENGENHO.md §I).
  This bans copying an implementation, not owning a capability — see
  "One binary" above for the distinction.
- **A second daemon, sidecar or `Command::new` for a capability** a Rust
  trait impl could serve. The two shell-outs that remain are tracked, not
  precedent: podman (theory/BUTAI.md M0–M8) and helm
  (`engenho-fonte/src/caixa_helm_installer.rs`).
- **Hand-rolled work-graph orchestration.** Use `shigoto::Dag` per
  pleme-io/theory/SHIGOTO.md.
- **Shell scripts beyond 3-line glue.** Tatara-lisp via `tatara-script`
  for anything more complex.

## Phases

See [`docs/M0-ROADMAP.md`](./docs/M0-ROADMAP.md) for the M0.0.1 → M0.0.4
local step-by-step; theory/ENGENHO.md §X for the M0–M5 fleet-wide arc.

## License

MIT.
