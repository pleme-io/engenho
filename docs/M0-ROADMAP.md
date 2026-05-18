# M0 — generation pipeline + kubectl-conformant single-node

> Companion to [`pleme-io/theory/ENGENHO.md`](https://github.com/pleme-io/theory/blob/main/ENGENHO.md)
> §X. The theory doc owns the *destination*; this doc owns the
> *step-by-step path-down* for milestone M0 inside this repo.
>
> **Phase semantics.** Each substep is a discrete PR with a passing
> CI gate (L0-L4 of theory/ENGENHO.md §V). Skipping a substep is a
> drift signal — the build either passes its tests or it doesn't ship.

---

## M0.0 — `engenho-types` seed (DONE — commit `d5c1547`)

- `KubeResource` typed contract (`kind.rs`).
- `meta/v1` types — ObjectMeta, TypeMeta, ListMeta. BTreeMap-everywhere
  for byte-deterministic serialization (theory/ENGENHO.md §VI.4).
- API path helpers (`api.rs`).
- Vendored Kubernetes v1.32 OpenAPI v3 schemas (core/v1, apps/v1, rbac/v1).
- BLAKE3-attested `MANIFEST.yaml` + verification test.
- 16/16 tests green.

---

## M0.0.1 — Pod as the forge-gen bullseye

**Scope.** Hand-author `core_v1::Pod` (and its transitive types — PodSpec,
Container, ContainerPort, Volume, etc.) as the smallest end-to-end
target shape. This hand-author is **the generator's bullseye**, the
byte-for-byte target that `kube-forge` (M0.0.3) must emit. The kind
itself is then deleted from the tree and replaced by generator output
at M0.0.3.

**Test gates.**

- L0: `cargo check -p engenho-types` — clean compile.
- L1: `cargo test -p engenho-types` — unit tests cover `Pod::name()`,
  `Pod::namespace()`, `Pod::GVK == { group: "", version: "v1", kind: "Pod" }`.
- L3 (wire-axis): round-trip a Pod manifest against the vendored
  `api__v1_openapi.json` schema definition. Field names match, required
  fields enforce, default values land correctly.

**Anti-pattern flag.** Authoring a partial Pod (e.g., just `name + image`)
is debt — we throw it away at M0.0.3 either way. Author the full shape
including PodSpec's ~80 fields. Container's ~30 fields. The 17 Volume
variants. Probes. Affinity. Resource requirements. This is the
"absolute-best long-term answer" per CSE Operating Principle #0; the
generator must reproduce ALL of it bit-for-bit.

**Estimated size.** ~1500-2500 LoC including transitive types.

---

## M0.0.2 — `kube-forge` crate sibling (new repo)

**Scope.** Create `pleme-io/kube-forge` — sibling to `mcp-forge`,
`pangea-forge`, `completion-forge`. NOT an iac-forge backend
(iac-forge's Backend trait is provider-emission shape; K8s resource
emission is a different semantic domain — typed Rust structs from
OpenAPI v3 schemas).

**Crate layout.**

```
pleme-io/kube-forge
├── kube-forge-types         — OpenApiInput, KubeKindIr, GeneratedModule
├── kube-forge-openapi       — OpenAPI v3 schema parser (depends on openapi3)
├── kube-forge-rust-emit     — Rust source rendering (uses NixAST-style typed
│                             AST for Rust syntax — `format!()` of Rust
│                             source forbidden, peer to crossplane-forge's
│                             ban on `format!()` of Go syntax)
├── kube-forge-cli           — `kube-forge generate --input … --output …`
└── tests                    — integration: vendored OpenAPI → expected
                                generated Rust modules (insta snapshots)
```

**Why a separate crate, not an iac-forge backend.**

| Axis | iac-forge `Backend` | engenho-types generation |
|---|---|---|
| Input shape | `IacResource` (CRUD endpoints, attribute list) | OpenAPI v3 `$ref`-resolved schemas |
| Output shape | Per-resource Terraform/Pulumi/Crossplane code | Rust structs with `#[derive(KubeResource, TataraDomain)]` |
| Per-resource semantics | Provider authenticate + CRUD against vendor API | Pure type → pure type, no auth, no I/O |
| Scope axis | "Generate a provider" (1 provider → N resources) | "Generate a type catalog" (1 schema → N kinds) |

Wedging this into iac-forge would force-fit an authentication / CRUD
shape onto pure type emission. Sibling crate is cleaner.

**Test gates.**

- L0-L1: standard cargo test.
- L3 (wire-axis): `cargo test --features openapi-roundtrip` — emit
  source from the vendored Pod schema; the emitted source compiles
  AND its serialize-deserialize matches the upstream OpenAPI schema's
  shape.

**Estimated size.** ~3000-5000 LoC.

---

## M0.0.3 — Pod regenerated bit-reproducibly

**Scope.** Run `kube-forge generate` on the vendored
`api__v1_openapi.json`. The output for Pod is byte-identical to the
M0.0.1 hand-authored shape. Delete the hand-author; replace with
generator output. CI gate ensures regeneration produces no diff.

**Test gates.**

- L0-L4 retained.
- **Determinism gate** (theory/ENGENHO.md §VI.1) — `kube-forge generate
  --check` returns 0 iff regenerated source == tree source byte-for-byte.

**Compounding unlock.** Adding any other kind to engenho-types is now
a `kube-forge generate` invocation. The marginal cost of a new kind is
zero generator work.

---

## M0.0.4 — All ~150 kinds

**Scope.** Generate engenho-types/src/{core_v1,apps_v1,rbac_v1,…}
modules for every kind in every vendored OpenAPI schema (~16 groups,
~150 kinds total). All compile. All round-trip against upstream OpenAPI.

**Test gates.**

- L0-L4 retained at full coverage.
- L3 round-trip: every kind passes openapi-roundtrip.
- L4: integration tests can apply a manifest of every kind into an
  in-process apiserver and read it back.

---

## M0.1 — `engenho-datastore` + `engenho-apiserver` (kubectl handshake)

Scope per theory/ENGENHO.md §X, M0.1 row. ~10-14 weeks of focused work.

The load-bearing single milestone of the entire engenho program. Every
downstream component (controllers, scheduler, kubelet, kube-proxy)
depends on the apiserver working. Build it first; build it well.

---

## M0.2 — controller-manager + scheduler

Per theory/ENGENHO.md §X. ~8-10 weeks.

---

## M0.3 — kubelet (CRI client + pod lifecycle)

Per theory/ENGENHO.md §X. ~10-14 weeks.

---

## M0.4 — networking + DNS + local-path (single-node complete)

Per theory/ENGENHO.md §X. ~6-8 weeks. **End of M0 — engenho is a
complete single-node Kubernetes distribution ready for `rio`.**

---

## Cross-cutting invariants (every milestone)

1. **Generation over hand-authoring.** Any K8s resource type touched by
   hand is a CI-rejected anti-pattern per theory/ENGENHO.md §IV.
   Generator-extension is the only valid fix path.
2. **Tameshi attestation chain.** Every release artifact ships with a
   BLAKE3 receipt covering inputs → outputs.
3. **shigoto for all work graphs.** Every reconciliation loop is a
   `shigoto::Dag` per theory/SHIGOTO.md.
4. **NixAST for emitted Nix.** No `format!()`-of-Nix in any emitter.
5. **Determinism.** Every output is bit-reproducible from the same
   inputs (theory/ENGENHO.md §VI).
6. **Tatara daemon supervision.** Every long-running engenho subsystem
   runs under tatara's `defguest` daemon mode per
   [tatara/docs/daemon-supervision.md](https://github.com/pleme-io/tatara/blob/main/docs/daemon-supervision.md).
