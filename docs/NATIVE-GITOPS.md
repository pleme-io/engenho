# Native GitOps — engenho does what Flux does, in the binary

> **Status: design, 2026-09-22.** Nothing below is shipped unless §7 says so.
> Operator decision: engenho reconciles git natively, "not actual fluxcd but the
> fact that it can do what fluxcd does". First consumer: `plo`, a managed engenho
> node whose base state is **no applications**.

## 0. The destination

A node runs engenho and nothing else. One declaration — a git URL, a ref, a
path — and engenho keeps the cluster equal to that path forever: fetch, build,
apply, prune what left the repo, correct drift, report typed status. Plain
manifests, kustomize overlays and Helm charts all arrive that way. No Flux
controllers, no `helm` binary, no `kustomize` binary, no second process: per
CLAUDE.md's **one binary** rule this is Rust, in-process, reached by a trait call.

It speaks the **Flux API** (same groups, kinds and fields for the subset it
implements) because that is the lingua franca every chart repo, every
`clusters/<name>` tree and every operator's muscle memory already uses — and
because `pleme-io/k8s` is already written against it (rio runs upstream Flux on
engenho today). Compatibility is the migration path: rio can drop upstream Flux
the day the native controllers cover its tree, with no repo change.

## 1. What already exists (do not rebuild)

| Need | Already in the fleet | Where |
|---|---|---|
| CRD serving (schema, defaults, `/status`, printer columns) | shipped | `engenho-controllers/src/crd.rs`, `engenho-runtime/tests/m0_5_crd_serving.rs` |
| Server-side apply, admission, defaulting, schema validation | shipped, HTTP | `engenho-apiserver/src/router.rs:1330` |
| The reconcile loop shape | `Controller::tick → ReconcileOutcome` | `engenho-controllers/src/controller.rs:34` |
| `RefSpec` (branch/tag/semver/commit/name/digest) | shipped, unused | `magma-converge/src/refspec.rs` |
| `Inventory` + GVKNN diff | shipped, unused | `magma-converge/src/inventory.rs` |
| `DriftPolicy` + ignore paths | shipped, unused | `magma-converge/src/drift.rs` |
| `Artifact` (BLAKE3 content address + provenance) | shipped | `magma-converge/src/artifact.rs` |
| The CRD→primitive mapping | written | `theory/FLUXCD-CONVERGENCE.md` §II–III |

## 2. The controllers

Three controllers in `engenho-controllers`, each a `Controller`:

| Controller | Kinds (Flux-compatible) | Does |
|---|---|---|
| `source` | `source.toolkit.fluxcd.io/v1` **GitRepository**, **HelmRepository**; `v1beta2` **OCIRepository** | fetch at a `RefSpec`, pack the tree as a BLAKE3-addressed `Artifact` in `<data_dir>/artifacts`, publish `status.artifact` |
| `kustomize` | `kustomize.toolkit.fluxcd.io/v1` **Kustomization** | build the artifact's `path` (plain manifests or a kustomization), apply, record the `Inventory`, prune the set difference, re-apply on `interval` to correct drift, gate on `dependsOn`, report conditions |
| `helm` | `helm.toolkit.fluxcd.io/v2` **HelmRelease** | resolve the chart from its source, render it IN-PROCESS, apply through the same applier, keep release history, upgrade/rollback on failure |

The kinds are **built-in** — registered at startup, not applied as CRDs — so a
fresh cluster can reconcile before anything has been installed into it.

### 2.1 The applier: the apiserver's own pipeline, in-process

Manifests are NOT written to the store directly. A controller that writes
`StoreMesh` bypasses admission, defaulting and CRD schema validation — the
measured cause of engenho once storing spec-violating objects. The applier
drives the apiserver's axum `Router` as a tower `Service` (`oneshot`) with an
`application/apply-patch+yaml` request: the full pipeline, server-side apply
field ownership (`fieldManager: engenho-gitops`), no socket, no subprocess. The
caller is a typed in-process principal, not a borrowed admin credential.

### 2.2 Git, in Rust

`gix` (gitoxide, pure Rust) — never `git2`/libgit2 (CONTAIN THE C: no `-sys`
crate) and never a `git` subprocess. Shallow fetch at the resolved ref; the
resolved commit is the artifact's revision.

### 2.3 Kustomize, in Rust

The subset `pleme-io/k8s` uses, implemented natively: `resources` (files, dirs,
nested kustomizations), `namespace`, `namePrefix`/`nameSuffix`,
`labels`/`commonLabels`, `commonAnnotations`, `patches` (strategic-merge and
JSON 6902), `images`, `configMapGenerator`/`secretGenerator`. A directory with
no `kustomization.yaml` is every manifest in it, recursively — Flux's own rule.
Anything outside the subset is a typed refusal on the Kustomization's status,
never a partial build that applies silently.

### 2.4 Helm, in Rust — the hard part

Rendering a chart means Go `text/template` + sprig + Helm's own functions
(`include`, `tpl`, `required`, `lookup`, `toYaml`…). That engine is the single
largest piece of this programme and gets its own milestone (N2) and its own
conformance corpus: render every chart `pleme-io/helmworks` ships and compare
byte-for-byte against `helm template`, the reference oracle, in CI.

### 2.5 Bootstrap: one declaration

engenho config grows `gitops.bootstrap = { url, ref, path, interval }`. On
start the daemon upserts `flux-system/GitRepository/root` and
`flux-system/Kustomization/root` from it — idempotent, so the declaration and
the cluster cannot disagree. The nix surface is
`pleme.nixos.engenhoNode.gitops.*`; plo points it at `pleme-io/k8s`
`clusters/plo`, which starts empty — so the reconciled state is exactly the
"no applications" base state.

## 3. Runtime interaction

plo runs `kubeletBackend = native`: workloads are `nix:` store closures and OCI
references are refused. A HelmRelease whose images are OCI therefore reconciles
to a typed `ImagesNotRunnable` condition on that node rather than a pod stuck
in `ErrImagePull` — the refusal is legible at the GitOps layer.

## 4. Invariants and their tiers

| Invariant | Tier |
|---|---|
| a manifest reaches the store without admission/defaulting/validation | truly-unrep: the applier has no store handle, only the router |
| prune removes something the Kustomization never applied | truly-unrep at the diff: only `Inventory` entries are prune candidates |
| a kustomize feature outside the subset applies partially | parse-time-rejected: the builder returns a typed `Unsupported`, the apply does not start |
| two controllers fight over a field | only-mitigated: SSA field ownership + conflict condition |
| Helm output diverges from `helm template` | CI-caught: the conformance corpus |

## 5. Milestones

| | Delivers | Receipt |
|---|---|---|
| **N0** | built-in kinds registered; bootstrap config; applier-through-router | an empty `clusters/plo` reconciles to `Ready=True` with an empty inventory |
| **N1** | GitRepository (gix) + Kustomization (plain + kustomize subset) + prune + drift | add a ConfigMap to the repo → it appears; remove it → it is pruned; `kubectl edit` it → reverted on the next interval |
| **N2** | HelmRepository/OCIRepository + HelmRelease + the in-process template engine | conformance corpus green against `helm template` for every helmworks chart |
| **N3** | rio drops upstream Flux | rio's `clusters/rio` reconciles natively; the flux-system deployments are deleted |

## 6. Where it plugs into the fleet

- `nix`: `pleme.nixos.engenhoNode.gitops.{url,ref,path,interval}` → engenho
  typed config → `gitops.bootstrap`.
- `pleme-io/k8s`: `clusters/plo/` (empty, with a README naming the base state).

## 7. Ledger

| Item | State |
|---|---|
| this design | written 2026-09-22 |
| N0–N3 | not started |
