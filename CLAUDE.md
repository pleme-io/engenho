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
cargo build --workspace            # debug build, ~2s incremental
cargo test  --workspace            # 16 unit + 5 manifest + 3 proptests (768 cases)
cargo fmt   --all -- --check       # formatting gate
cargo clippy --workspace -- -D warnings  # lint gate (workspace.lints clippy::pedantic)

nix build                          # release build via substrate's crate2nix
nix flake check                    # hermetic build + tests
```

`engenho/.github/workflows/ci.yml` runs `cargo fmt --check` + `cargo
test --workspace` + `nix flake check` + `nix build .#default` on every
PR. CI on `macos-15` runner.

## Substrate integration (no escape hatches)

| Primitive | How engenho uses it |
|---|---|
| `substrate/lib/rust-workspace-release-flake.nix` | `flake.nix` — one import, no hand-rolled build glue |
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
