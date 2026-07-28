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

## Build

```bash
cargo build --workspace                    # debug build
# THE gate command — mirrors .github/workflows/test.yml exactly:
cargo test --workspace --exclude engenho-diff \
  --all-targets --all-features --locked --no-fail-fast
cargo test --workspace --all-features --locked --doc   # doctests (see below)
cargo fmt --all -- --check                 # formatting gate
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings

nix build                          # release build via substrate.rust.workspace
nix flake check                    # ⚠ compiles NOTHING — see § CI + gating
```

Every flag above is load-bearing:

- **`--all-features`** — engenho gates real code behind `with-tameshi`,
  `with-sui-eval`, `with-shikumi`, `with-revoada`,
  `with-engenho-kube-client`, `openapi-roundtrip`, `bit-repro`, `mock`.
  Default features leave all of it uncompiled and unrun.
- **`--all-targets`** — includes `tests/`, `examples/`, `benches/`.
  **It EXCLUDES doctests**, which is why `--doc` is a separate line.
- **`--no-fail-fast`** — without it cargo stops at the first failing
  *binary*. Measured: a plain run aborted at `engenho-diff`
  (alphabetically first to fail) and reported **881** tests; the same
  run with `--no-fail-fast` reports **3,169**. A count taken without
  this flag is not a count of the suite.
- **`--locked`** — a drifted `Cargo.lock` fails loudly instead of being
  silently re-resolved.
- **`--exclude engenho-diff`** — see § Live-oracle tests.

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

The doctest leg is **41 of 42 `ignore`d** — close to a vacuous guard
today. It is wired anyway so the next real doctest lands guarded, and
so the 41 are visible as debt rather than counted as coverage.

### Live-oracle tests

`engenho-diff`'s four tests are a *differential* suite: each drives the
same operation against engenho-in-process **and a real k3s cluster**,
then diffs the responses. They resolve
`$HOME/.kube/engenho-local-tunnel.yaml` and are written to **fail loud,
never silently skip**. No such cluster exists on a CI runner, so CI
excludes them **from execution only** — `test.yml` still runs
`cargo test -p engenho-diff --no-run`, so the crate must compile on
every PR. Enumerated with rationale in
[`ci/live-oracle-tests.txt`](./ci/live-oracle-tests.txt). Not
`#[ignore]`d (that would hide them from the operator, where the oracle
*does* exist and they are the entire point).

## CI + gating

**`nix flake check` compiles nothing, and neither does `ci.yml`.**
`ci.yml` is a shim onto substrate's `cargo-ci.yml`, whose whole body is
`nix flake check`. That builds only `checks.<system>.*`, and engenho's
flake declares **none**. On sibling repo `forge` this was proven by
probe: clean tree → exit 0; `compile_error!` in a test module → exit 0;
literal non-Rust garbage in a function body → **exit 0**.

The load-bearing fix — the flake exposing real `checks` — is **blocked
outside this repo**, verified rather than assumed: `runTests` is a
crate2nix-generated-`Cargo.nix` concept, nixpkgs' `buildRustCrate` has
no such argument, and `substrate/lib/build/rust/lockfile-builder.nix`
(the engine `substrate.rust.workspace` actually routes to, via
`mk-rust-workspace.nix`) has **zero** occurrences of it. Building an
in-repo `checks` would mean a second, divergent Rust build path —
Operating Principle #1 forbids it. **Substrate follow-up:** give
`lockfile-builder.nix` a `runTests`/`mkWorkspaceChecks` so
`substrate.rust.workspace` can expose `checks.<system>.test`; until
then `nix flake check` cannot compile any gen-pattern consumer.

| Workflow | Trigger | Scope | Blocking |
|---|---|---|---|
| `test.yml` | push + PR | **the real gate** — whole workspace, all-features, all-targets, + doctests, + `engenho-diff` compile-only, + fmt + clippy | yes |
| `deep-test.yml` | schedule + dispatch | breadth — macOS leg, 4k-case proptest stress, coverage artifact, `cargo audit` | no |
| `ci.yml` | push + PR | `nix flake check` (compiles nothing today — kept so the flake still evaluates) | — |

`deep-test.yml` deliberately has **no** `push`/`pull_request` trigger:
it ran on every PR while permanently red, which is how a never-green
gate came to sit beside a shim that compiled nothing without anyone
noticing either. It had **21 runs and 21 failures — never once green**
since 2026-05-23. `cargo audit` lives there rather than on the PR path
because it fails on advisories published against the dependency tree
(i.e. on the calendar, with no change to this repo); making it blocking
manufactures exactly the permanently-red gate this split removes.

> **⚠ Both workflows are currently blocked on one operator action.**
> `Cargo.lock` has git deps on five **private** pleme-io repos —
> `tameshi`, `cofre`, `promessa`, `sui`, `tatara` — and cargo resolves
> the whole lock graph regardless of features, so *every* cargo command
> needs them. `pleme-io` is on the GitHub **Free** plan, where org
> secrets reach **public** repos only; engenho is **private** with no
> repo-level secrets, so `secrets.BOT_PAT` arrives empty and the
> repo-scoped `GITHUB_TOKEN` cannot read a sibling private repo. Fix:
> `gh secret set BOT_PAT --repo pleme-io/engenho` — the same
> repo-level-`BOT_PAT` workaround pangea-operator already carries. The
> workflows are wired correctly and need no edit once it exists.
>
> Local builds do **not** reproduce this: `~/.cargo/git` holds
> credentialed checkouts, so a workstation is green while CI is red.
> That divergence is why the failure survived unnoticed for months.

## Substrate integration (no escape hatches)

| Primitive | How engenho uses it |
|---|---|
| `substrate.rust.workspace` | `flake.nix` — the gen/`Cargo.gen.lock` pattern (routes `mk-rust-workspace.nix` → `lockfile-builder.nix`; no crate2nix, no committed `Cargo.nix`). Note it exposes **no `checks`** — see § CI + gating |
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

## Anti-patterns

- **Hand-authoring K8s resource types.** The non-negotiable rule above.
- **`format!()` of K8s YAML.** Use `engenho-types` typed structs and
  serde, never sprintf strings.
- **Bypassing cofre for secrets.** Kubelet must materialize secrets
  through cofre; no plaintext k8s `Secret` object in flight.
- **Embedding upstream Go.** Engenho ships zero vendored Go (theory/ENGENHO.md §I).
- **Hand-rolled work-graph orchestration.** Use `shigoto::Dag` per
  pleme-io/theory/SHIGOTO.md.
- **Shell scripts beyond 3-line glue.** Tatara-lisp via `tatara-script`
  for anything more complex.

## Phases

See [`docs/M0-ROADMAP.md`](./docs/M0-ROADMAP.md) for the M0.0.1 → M0.0.4
local step-by-step; theory/ENGENHO.md §X for the M0–M5 fleet-wide arc.

## License

MIT.
