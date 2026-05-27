# engenho — the strategy (one truth, many faces, attested bits)

> The decision frame that every engenho action obeys. Where
> [`LEAN.md`](LEAN.md) is the *what-we-own-vs-compose* matrix and
> [`M0-ROADMAP.md`](M0-ROADMAP.md) is the *path-down*, this doc is the
> **invariant set + action taxonomy + phase spine** that ties the
> three load-bearing axes into one design.
>
> Companions: [`STATE-MACHINES.md`](STATE-MACHINES.md) (every FSM),
> [`TYPESCAPE.md`](TYPESCAPE.md) (the typed universe),
> [`DISTRIBUTED.md`](DISTRIBUTED.md) · [`FABRIC.md`](FABRIC.md) ·
> [`CONSISTENCY-FABRIC.md`](CONSISTENCY-FABRIC.md) ·
> [`MANY-FACES.md`](MANY-FACES.md) · [`RESILIENCE.md`](RESILIENCE.md).
> Canonical destination: [`../../theory/ENGENHO.md`](../../theory/ENGENHO.md).

## The thesis

engenho is pleme-io's Pillar 7 **runtime** — *Pangea declares the
supercontinent; magma realizes it on cloud; engenho runs the land
(terreno)*. It is a typed, attested, Rust-native Kubernetes (and
Nomad, and PureRaft) distribution.

The three axes the design is built around are **one** claim:

> **One committed truth, many faces onto it, a content-addressed
> substrate that materializes and moves the actual bits, and an
> attestation chain over every transition.**

| Axis | Layer | One-line |
|---|---|---|
| **Fully distributed** | `engenho-revoada` + `engenho-teia` + `engenho-store` | Every node is role-equivalent at boot; roles emerge from consensus. `kubectl` never knows the nodes shuffled. |
| **API-compatible (many faces)** | `Face` trait + `engenho-apiserver` + `engenho-types` | One `StoreMesh` truth rendered into K8s / Nomad / PureRaft / REST / gRPC / GraphQL / MCP. Adding a face adds a view, never forks the substrate. |
| **Shift bits to forms (nix/derivation)** | `engenho-substrate` | A `Drv` is a location-independent typed value; `WorkloadShape` renderers turn it into an OCI image / Nix closure / qcow2 / wasm / static binary; the gossip+broadcast ledger distributes it; K-of-N independent-rebuild quorum makes it trustworthy. |

## Governing invariants (the strategy, as rules)

1. **One truth, many faces.** No face owns state. Each translates its
   protocol to `ResourceCommand` / `StoreMesh::get/list`
   ([`MANY-FACES.md`](MANY-FACES.md), [`API-SURFACE.md`](API-SURFACE.md)).
2. **Generation over hand-authoring.** Every K8s kind is mechanically
   emitted from upstream OpenAPI v3 by `kube-forge`. Hand-authoring a
   `Pod` struct is CI-rejected — extend the generator instead (peer to
   Crossplane's ban on `format!()`-of-Go and NixAST's ban on
   string-concat-of-Nix). Single transient exception: the M0.0.1 Pod
   bullseye.
3. **Content-address everything; attest every transition.** Bits are
   BLAKE3-addressed `Drv` / `WorkloadShape` outputs. Role shifts and
   materializations write signed, hash-linked chain blocks (tameshi).
   **Trust = K-of-N independent rebuild agreement** (`QuorumOutcome`),
   not authority.
4. **Formation by configuration, not procedure.** Operators pick a
   `TopologyStrategy`; the substrate handles every loss case the
   strategy declared, atomically via Raft joint consensus
   ([`RESILIENCE.md`](RESILIENCE.md)).
5. **Typed config cascade.** Everything flows through
   `shikumi::TieredConfig` (`bare` / `prescribed_default` / `extend`),
   5-tier discovery, cross-section coherence `validate`
   (`engenho-config`).
6. **Mock-universe default.** Real integrations are feature-gated
   (`with-revoada`, `with-tameshi`, `with-sui-eval`, …); the always-on
   mock universe means cross-substrate tests never need a live cluster
   (`engenho-fonte`).
7. **Never get stuck.** Every wait has a timeout; quorum loss →
   read-only (503), not freeze; idempotent applies; phi-accrual +
   grace period before eviction. Enforced by construction
   ([`RESILIENCE.md`](RESILIENCE.md) "never-stuck invariants").
8. **Own / compose / consume.** engenho *owns* the typed substrate;
   *composes* the dataplane from proven Rust OSS; *consumes* the
   pleme-io shared libs. See the matrix below.

## Own / compose / consume (from LEAN.md)

| engenho **owns** | **composes** (Rust OSS) | **consumes** (pleme-io) |
|---|---|---|
| resource catalog (`engenho-types`), apiserver wire, controller dispatch, scheduler policy, the substrate derivation engine | hyper/axum/rustls, openraft, chitchat, iroh, youki, cloud-hypervisor, runwasi, cilium, skopeo | tatara (engenho *is* a tatara binary), shigoto (DAG), shikumi (config), cofre (secrets), tameshi/sekiban/kensa (attest/admit/comply), nix-ast, forge-gen/kube-forge |

engenho deliberately does **not**: reimplement runc, reimplement CNI,
use `k8s-openapi` (macro-driven; incompatible with the typescape's
`#[derive(TataraDomain)]`), build its own etcd, vendor Go, or use
OpenSSL.

## Action taxonomy (all the verbs, by layer)

- **Cluster lifecycle** (`kikai`): `init · up · down · status · destroy ·
  daemon · pause · resume · snapshot · park · dump-config`. → drives the
  14-state cluster FSM (SM ①).
- **Resource** (faces → store): `get · list · watch · create · update ·
  patch · delete` (+ cluster-scoped variants) → `ResourceCommand`.
- **Consensus** (`engenho-revoada` / `engenho-store`): `propose ·
  initialize_singleton / initialize_with_voters · is_leader ·
  wait_for_applied` + `RoleAssignment::{Promote,Demote,Quarantine,Restore}`.
- **Membership** (gossip): `start · subscribe · update_local_state ·
  wait_for_members · peers · shutdown`.
- **Substrate** (`engenho-substrate`): `put_drv/get_drv · build ·
  render(shape) · ingest(receipt)→QuorumOutcome · verify ·
  ledger.broadcast/gossip · cache.{get,promote}`.
- **Reconcile** (`controllers` / `scheduler` / `kubelet` / `fonte`):
  `tick → {ReconcileReport | Binding | ContainerStatus | Outcome}`.
- **Operator** (`engenho-mcp`): reader tools (live) → writer tools (P2,
  gated on saguão passport authority).

## How the three axes compose (the data path)

```
 kubectl / nomad / grpc / mcp                       ← clients see protocol-native faces
        │  (FACE: translate to ResourceCommand)
        ▼
   StoreMesh  ── the ONE committed truth ──         ← openraft (Strong tier)
        │  per-resource consistency tier:
        │  Strong(raft) · EventualGossip(chitchat) · DurableStream(jetstream) · Content(iroh)
        ▼
   revoada (gossip + raft + content + attest) over teia (NATS, 5 channels, multi-region leaf nodes)
        ▼
   SUBSTRATE:  Drv ─hash─▶ build ─▶ Realisation ─quorum(K independent rebuilds)─▶
               ─shape-render─▶ {OciImage | NixClosure | Qcow2 | Wasm | StaticBinary | HelmChart}
               ─ledger/gossip─▶ every node that needs it
        ▼
   kubelet runs it · scheduler binds it · controllers reconcile it
        ▼
   fonte:  (defsistema …) ─▶ Watcher→Evaluator→Proposer→Attester→Publisher  (7-beat Viggy tick)
```

## Phase spine

The fleet arc and the per-subsystem series run in parallel. The
binary itself is an M0.0 placeholder today; **k3s-via-kikai is the
real cluster** until engenho-native lands (M0.4).

| Fleet (theory §X) | Scope |
|---|---|
| M0.0 → M0.0.4 | typed catalog (Pod bullseye → kube-forge → all ~150 kinds) |
| M0.1 | datastore + apiserver (kubectl handshake) |
| M0.2 / M0.3 / M0.4 | controllers+scheduler / kubelet / networking+DNS+local-path (single-node complete) |
| M0.5 / M1 / M2 / M3 / M4 / M5 | multi-node / HA+caixa / typescape-native / mesh+compliance / **CNCF Certified** / programs-as-RuntimeClass |

| Per-subsystem series | Lives in |
|---|---|
| `R0–R7` revoada (membership → raft → policy → attest → content → consume) | [`DISTRIBUTED.md`](DISTRIBUTED.md) |
| `F0–F9` teia/fabric (NATS transport, watch, content, attest, observ, topology, helm, federation) | [`FABRIC.md`](FABRIC.md) |
| `C0–C7` consistency (in-mem → attest → watch → NATS raft → disk → tier-hints → content → observ) | [`CONSISTENCY-FABRIC.md`](CONSISTENCY-FABRIC.md) |
| `R-TOPO.0–7` resilience (typed strategies → policy wiring → madsim → maelstrom → chaos-mesh → openraft-maelstrom → jepsen) | [`RESILIENCE.md`](RESILIENCE.md) |
| `R-K8S` / `R-NOMAD` faces (typed admission, `-o yaml`, HCL parse/emit, translator) | [`MANY-FACES.md`](MANY-FACES.md) |
| `M1.1–M1.5` fonte real integrations (shikumi/sui/revoada/tameshi/mirante) | `engenho-fonte` |

## Why this compounds

Each typed primitive unlocks the next layer for free: a `Drv` that
content-addresses unlocks the ledger that distributes it; a
`QuorumOutcome` over independent rebuilds unlocks trust without
authority; a `Face` that translates unlocks every CLI without forking
the store; a `TopologyStrategy` that declares a shape unlocks
self-healing without procedural failure code; a `Typescape` impl
unlocks `(defsistema)` authoring + MCP exposure + attestation for any
new primitive with zero per-consumer code. The strategy is: **add typed
primitives at the substrate; let every layer above compound on them.**
