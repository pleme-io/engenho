# engenho — distributed substrate design

> **Codename: `revoada`** (Portuguese: *the murmuration of starlings* —
> a flock whose global pattern emerges from each bird's local sensing,
> no central conductor required).
>
> `engenho-revoada` is the **distribution layer** that sits ABOVE
> `engenho` (the K8s runtime), letting nodes dynamically elect
> control-plane / worker roles, replicate cluster state via P2P, and
> survive partitions through eventual consistency. The K8s API surface
> stays kubectl-compatible end-to-end; what changes is HOW the
> apiserver / scheduler / controllers / kubelet roles are distributed
> across the mesh.

## Thesis

K8s today is a **static topology**: you declare `3 masters + 7 workers`
in your cluster manifest, that's what gets provisioned, and a node
failure means manual replacement until the cluster autoscaler swaps
in a fresh VM. The control plane and worker roles are fixed at
bootstrap time.

`revoada` reframes this. **Every engenho node is role-equivalent at
boot.** The roles a node plays — control-plane participation, workload
hosting, dedicated etcd-shim duty, dedicated scheduler duty — emerge
from a **distributed consensus continuously voting** on the mesh's
current shape. If 3 control-plane nodes is the target and one fails,
the mesh elects a worker to promote, materializes the missing
components on that node, and the cluster reaches steady state without
operator intervention.

The K8s API never knows or cares. From `kubectl`'s perspective the
cluster always has its target shape; the underlying nodes just
shuffle.

## The four layers

Engenho-revoada is **a stack of four typed primitives**, each backed
by a production-grade Rust library + a pleme-io consumer:

```
┌──────────────────────────────────────────────────────────────────┐
│ Layer D: Attested transitions  (tameshi BLAKE3 receipt chain)    │
│           Every role shift writes an immutable audit entry.      │
├──────────────────────────────────────────────────────────────────┤
│ Layer C: Content-addressed workload sync  (iroh / mainline DHT)  │
│           Pod manifests, images, configs distributed P2P.         │
│           BitTorrent-style; any node serves what it has.          │
├──────────────────────────────────────────────────────────────────┤
│ Layer B: Role-assignment consensus  (openraft, via tatara)        │
│           Raft log of typed RoleAssignment commands.              │
│           Joint consensus = atomic dynamic membership.            │
├──────────────────────────────────────────────────────────────────┤
│ Layer A: Membership + failure detection  (chitchat, via tatara)   │
│           Gossip mesh; phi-accrual failure detector.              │
│           Eventually consistent node state across partitions.     │
└──────────────────────────────────────────────────────────────────┘
                            ↑
        Every node runs all four layers from boot
```

### Layer A — Membership + failure detection (chitchat / SWIM)

Source library: [`quickwit-oss/chitchat`](https://github.com/quickwit-oss/chitchat).
Already in pleme-io's `tatara-engine` (`src/cluster/gossip.rs`).

Each engenho node runs a chitchat gossip endpoint. Gossiped per-node
state:

```rust
pub struct NodeState {
    pub node_id: NodeId,                     // ed25519 pubkey
    pub gossip_addr: SocketAddr,
    pub roles: BTreeSet<NodeRole>,           // ControlPlane | Worker | Etcd | Scheduler
    pub capacity: NodeCapacity,              // cpu + memory + storage Quantity
    pub k8s_version: String,
    pub uptime_sec: u64,
    pub membership_generation: u64,
}
```

**Phi-accrual failure detector** (Cassandra-style) decides when a node
is suspected vs. confirmed-dead. Defaults: phi-threshold 8.0, dead
grace period 30s — operator-tunable per cluster.

Gossip CANNOT be the source of truth for role assignments (eventually
consistent, no quorum guarantee). It feeds Layer B's view of "who's
alive."

### Layer B — Role-assignment consensus (openraft via tatara)

Source library: [`databendlabs/openraft`](https://github.com/databendlabs/openraft).
Already in pleme-io's `tatara-engine` (`src/cluster/raft_*.rs`).

The engenho-mesh elects a **Raft leader** among current control-plane
nodes. The leader serializes typed `RoleAssignment` commands:

```rust
pub enum RoleAssignment {
    /// Promote a worker to control-plane (with which K8s components).
    Promote {
        node_id: NodeId,
        roles: BTreeSet<ControlPlaneRole>,    // ApiServer | Scheduler | ControllerManager | Etcd
        reason: Reason,                       // ReplacingFailed | ScalingUp | Operator
    },
    /// Demote a control-plane node to worker.
    Demote {
        node_id: NodeId,
        roles_relinquished: BTreeSet<ControlPlaneRole>,
        reason: Reason,
    },
    /// Mark a node as quarantined — receives no new pods.
    Quarantine { node_id: NodeId, reason: Reason },
    /// Restore a previously quarantined node.
    Restore { node_id: NodeId },
}
```

**Joint consensus** (openraft's native dynamic-membership protocol)
makes shifts atomic — a single Raft commit transitions the mesh from
"old shape" to "new shape" without an intermediate state operators
have to reason about.

**Election triggers** (the policy layer that proposes commands):
- Periodic mesh-shape audit (every 30s): does the current set of
  control-plane nodes match the target topology?
- Failure detector signal (Layer A): a control-plane node is dead;
  promote a replacement.
- Capacity-driven scaling: too few workers for pending pods; promote
  more (typically not done by revoada itself — defer to cluster-
  autoscaler if the underlying infra supports it).
- Operator override via MCP tool: explicit human-driven role change
  (gated by saguão passport per LEAN.md).

### Layer C — Content-addressed workload sync (iroh / BitTorrent DHT)

Source library: [`n0-computer/iroh`](https://github.com/n0-computer/iroh)
+ `iroh-mainline-content-discovery` (BitTorrent mainline DHT,
BLAKE3-hashed addresses).

K8s today distributes Pod manifests via the kube-apiserver — every
worker pulls from the central control plane. At scale this bottlenecks
on apiserver bandwidth. `revoada` lets workers **pull from peers**:

1. Control-plane node admits a Pod spec → computes BLAKE3 hash →
   announces `(hash, ipport)` to the DHT.
2. Worker scheduling the Pod queries DHT for the hash → gets a list
   of peers that have it → fetches from the closest one.
3. Image layers + ConfigMap data + Secret references travel the same
   substrate.

Iroh's design already aligns: **BLAKE3 hashes** for content identity
(same algorithm tameshi uses), QUIC transport, mainline DHT for
peer discovery. Pkarr (public-key addressable resource records) for
node discovery.

**Open question**: Secrets. The redaction-by-view rule (LEAN.md) means
engenho-mcp never sees plaintext Secret values; the runtime materializer
(engenho-kubelet) needs them. P2P sync of Secret material would need
encrypted-at-rest + decrypt-only-by-target-pod cryptography. Likely
out of scope for v0 — Secrets stay apiserver-mediated; only ConfigMaps
+ images flow P2P.

### Layer D — Attested role transitions (tameshi)

Source library: pleme-io's `tameshi`.

Every Raft-committed `RoleAssignment` from Layer B is wrapped in a
`tameshi::Block` and appended to the mesh's `RoleAttestationChain`:

```rust
pub struct RoleAttestationBlock {
    pub prev_hash: blake3::Hash,
    pub assignment: RoleAssignment,
    pub committed_at: SystemTime,
    pub raft_term: u64,
    pub raft_log_index: u64,
    pub leader_signature: ed25519::Signature,
    pub witness_signatures: Vec<ed25519::Signature>,   // co-signed by N other nodes
}
```

Auditor (operator running `kensa verify --chain role-attestation`)
walks the chain end-to-end + verifies signatures. Immutable history
of "which node held which role at what time" — the substrate for
post-incident analysis, compliance attestation, debugging.

## Why this composition wins

| Concern | Why this stack handles it |
|---|---|
| **Dynamic role shifts** | Layer B's joint consensus = atomic membership changes |
| **Survives N node failures** | Raft tolerates ⌊(N-1)/2⌋ failures in the control-plane set; the mesh elects replacements via Layer A+B |
| **Eventual consistency in partitions** | Layer A keeps every partition's node state observable locally; Layer B blocks role changes during partition (Raft requires quorum) so split-brain is impossible |
| **kubectl wire compatibility** | The K8s apiserver (engenho-apiserver at M0.1) is unchanged; revoada just decides WHICH NODES run it |
| **No single point of failure** | Layer C distributes workload data P2P; if all control planes fail simultaneously, workers can continue serving cached pod state until the consensus reforms |
| **Attestable** | Layer D writes every role transition to a BLAKE3 chain (same shape as tameshi's other chains — compounding the attestation primitive) |
| **Pleme-io-native** | 3 of 4 layers reuse existing pleme-io infrastructure (tatara, chitchat-in-tatara, tameshi). Only Layer C is new code. |

## Naming — why `revoada`

Per pleme-io's convention (Brazilian-Portuguese for Tier-2+ primitives
evoking enclosed spaces, flows, growth), `revoada` captures the
distinctive behavior of distributed engenho:

- **Murmuration** — starlings shift formation continuously, each
  bird responding to its 6-7 nearest neighbors; no leader. The
  global pattern emerges. Maps directly onto engenho-mesh's
  gossip + Raft + role-rotation dance.
- **Already evocative in the org's typed primitives** — saguão
  (vestibule), caixa (box), terreiro (compound), cordel
  (cord/chord). Revoada (flock-flight) sits in the same lexical
  field.
- **Non-clashing** — "Swarm" is Docker; "Federation" is K8s v1
  legacy. "Revoada" is undefended in the K8s lexicon.

Canonical spec: this doc. Long-form theory once `revoada` proves out:
`pleme-io/theory/REVOADA.md` (companion to ENGENHO.md, MAGMA.md,
TERRENO.md).

## Layer mapping to crates

```
engenho-revoada/                          ← new workspace crate
├── src/
│   ├── lib.rs                            ← public surface
│   ├── membership/                       ← Layer A wrapping chitchat
│   │   └── mod.rs
│   ├── consensus/                        ← Layer B wrapping openraft
│   │   ├── mod.rs
│   │   ├── role_assignment.rs            ← typed RoleAssignment enum + state machine
│   │   └── policy.rs                     ← when-to-shift-roles policies
│   ├── content/                          ← Layer C wrapping iroh
│   │   └── mod.rs
│   └── attestation/                      ← Layer D wrapping tameshi
│       └── mod.rs

engenho/                                  ← consumes revoada at M0.5
└── (apiserver / scheduler / kubelet wired through revoada's
   typed views of "which nodes do I run on now?")
```

## Phased rollout

| Phase | Deliverable | Gate |
|---|---|---|
| **R0** | Crate scaffold + design freeze + typed RoleAssignment enum | This doc landed; types compile |
| **R1** | Layer A — membership via chitchat (wrapping tatara-engine's existing gossip impl) | Two engenho nodes discover each other |
| **R2** | Layer B — Raft via openraft for `RoleAssignment` commands | 3-node mesh elects leader; commits a typed command |
| **R3** | Policy engine — periodic mesh-shape audit + auto-promote on phi-failure | Killing the leader auto-promotes within 15s (the GKE target) |
| **R4** | Layer D — tameshi `RoleAttestationChain` | `kensa verify` walks the chain end-to-end |
| **R5** | Layer C — iroh content sync for ConfigMaps + Pod specs | Worker fetches a Pod manifest from a peer instead of apiserver |
| **R6** | Engenho-apiserver consumes revoada's role view; engenho-kubelet ditto | Cluster shifts shape under load |
| **R7** | Operator MCP tool: `mesh_status` + `mesh_propose_shift` + `mesh_witness_chain` (Reader-only at first; Writer gated on saguão) | MCP catalog gains 3 mesh tools |

## OSS library survey

| Concern | Chosen library | Why | Maturity 2026 |
|---|---|---|---|
| Consensus | **openraft** (`databendlabs/openraft`) | Joint consensus = atomic dynamic membership; Rust-native; powers Databend's meta-service in prod | Stable; pre-1.0 API drift but production-used |
| Gossip + failure detection | **chitchat** (`quickwit-oss/chitchat`) | Scuttlebutt reconciliation + phi-accrual detector; Cassandra-style; designed for K8s | Production at Quickwit |
| CRDT (for node metadata that must survive partitions) | **automerge** (`automerge/automerge`) | Rust + WASM; nested map/list CRDTs; battle-tested | Mature; wide collaborative-editing adoption |
| P2P content sync | **iroh** (`n0-computer/iroh`) | BLAKE3-keyed content addresses (matches tameshi!); QUIC transport; mainline DHT for discovery | Active development; production at n0 |
| BitTorrent DHT specifically | **iroh-mainline-content-discovery** (sub-crate of iroh) | Direct mainline DHT client | Active |
| TLS | **rustls** | Already engenho-canonical | Mature |

## Academic + industry priors

- **Raft** ([Diego Ongaro, 2014](https://raft.github.io/)) — the consensus algorithm everyone uses today (etcd, CockroachDB, Consul, openraft).
- **SWIM / Scuttlebutt** ([Das et al., 2002](https://www.cs.cornell.edu/info/projects/spinglass/public_pdfs/swim.pdf) / [van Renesse et al., 2008](https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf)) — the gossip protocols chitchat implements.
- **Coordinated Leader Election** ([k8s.io 1.36 beta](https://kubernetes.io/docs/concepts/cluster-administration/coordinated-leader-election/)) — upstream K8s now ships typed leader election among control-plane components. Revoada extends this to the *role* itself.
- **Talos Linux** ([siderolabs](https://github.com/siderolabs/talos)) — the immutable-OS approach; revoada complements it (Talos hardens individual nodes; revoada distributes the cluster across them).
- **Phi-accrual failure detector** ([Hayashibara, 2004](https://web.archive.org/web/20131017053620/http://ddg.jaist.ac.jp/pub/HDY+04.pdf)) — used by chitchat + Cassandra + Akka.

## What revoada is NOT

- **Not a replacement for etcd.** The Raft of Layer B serializes
  *role assignments*; the K8s store (engenho-store at M0.1) handles
  *resource CRUD*. Two distinct Raft groups in the same mesh.
- **Not a "self-healing magic"** that solves all failure modes. Network
  partitions still hurt; quorum still requires majority; failed VMs
  still need replacement infrastructure.
- **Not for static homelab clusters of 1-3 nodes.** The single-node
  engenho-local pattern uses k3s today and engenho-the-runtime tomorrow;
  revoada becomes load-bearing at 5+ nodes where dynamic shifting is
  worth the consensus overhead.

## Open design questions

1. **Secret material in P2P sync.** Per LEAN.md, engenho-mcp never sees
   plaintext Secret data; engenho-kubelet does. P2P sync would expose
   bytes to intermediate peers. Defer Secret sync to apiserver path
   (v0); design encrypted-at-rest layer separately later.

2. **Multi-region / WAN clusters.** Raft latency-sensitive; gossip
   handles WAN better. May need a layered architecture: per-region
   Raft + global gossip — possibly federation via tatara's existing
   patterns.

3. **Witness signatures in Layer D.** How many co-signers required
   per role transition? Defaults: 2 of N control-plane nodes (per
   the Raft quorum) — operator-tunable.

4. **Election storms.** Periodic mesh-shape audits could oscillate
   if policy disagrees with itself. Need a damper (no more than 1
   role shift per node per minute) and a typed "audit decision"
   shape so operators can reason about the policy.

5. **Bootstrap from zero.** First node knows of no peers. Options:
   (a) static seed list (current tatara approach), (b) mDNS discovery
   on LAN, (c) pkarr+mainline DHT for global. Probably (a)+(b)+(c)
   with priority cascade.

## What unlocks for the operator

```
                              BEFORE revoada
                              ─────────────
Operator: "I declared 3 masters + 4 workers."
K8s:      "Yes, that's what's running. Master-2 just died."
Operator: "Replace it manually or wait for cluster-autoscaler."

                              AFTER revoada
                              ─────────────
Operator: "I want 3 masters + 4 workers."
revoada:  "Got it. (gossip: detected master-2 dead).
           (Raft commit: promoting worker-3 to master).
           (tameshi: receipt 0xabcd…).
           (~12s elapsed)
           Current shape: 3 masters + 3 workers + 1 worker promoting.
           Reaching target in ~20s."
Operator: (verifies via kensa later) "All transitions signed; chain valid."
```

The K8s API surface kubectl operators use is unchanged. The substrate
manages itself.

---

**Status**: design freeze; R0 scaffold next session.
**Companion docs**: [LEAN.md](LEAN.md) (lean library strategy),
[../theory/ENGENHO.md](https://github.com/pleme-io/theory/blob/main/ENGENHO.md) (engenho destination).
