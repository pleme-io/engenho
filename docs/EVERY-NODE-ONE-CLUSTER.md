# Every node, one cluster: eventually consistent state behind the Kubernetes face

Status: **plan** (2026-10-08). Nothing here is built. Companion docs:
[FLEET-DESIGN.md](FLEET-DESIGN.md) (the whole-fleet design this adjusts),
[RECOVERABLE-STATE.md](RECOVERABLE-STATE.md) (the guarantees and their seals),
[CONSISTENCY-FABRIC.md](CONSISTENCY-FABRIC.md), [DISTRIBUTED.md](DISTRIBUTED.md),
[IMPROVEMENT-PLAN.md](IMPROVEMENT-PLAN.md) (edge 18 gates every step below).

## 0. Destination

engenho runs on every machine in the fleet as one Kubernetes cluster. Every node
holds a replica of the cluster's state and serves the Kubernetes API for the
whole cluster from it. Replication between nodes is **eventually consistent**.
Serialization exists only where Kubernetes itself is stateful: a write to one
object, and the handful of coordination facts (membership, leases, ownership)
that need a quorum. A node that sleeps or roams keeps running its own pods and
writing its own state, and catches up when it is back.

RECOVERABLE-STATE.md already states the licence: engenho only has to honour the
Kubernetes API facade; underneath it may use any mechanism. This plan picks the
mechanism.

## 1. Where it stands (measured 2026-10-08, HEAD 4e0c1aa)

| Area | As built | Evidence |
|---|---|---|
| Writes | every mutation is a `ResourceCommand` through one openraft log; controllers, scheduler and kubelet call `StoreMesh::propose` directly | `engenho-store/src/command.rs:64-185`, `mesh.rs:420-427` |
| Voters | always one: `boot_store` builds `InProcessRouter` and node 1 at `in-process://1`, `initialize_singleton` | `engenho-runtime/src/runtime.rs:1825-1846` |
| Ordering | one global revision per apply; per-key CAS decided inside that log; one catalog `BTreeMap` | `engenho-store/src/state.rs:369-382,573-623,71-76` |
| Reads | local catalog, no read index | `mesh.rs:429-434` |
| Watch | history ring of 8192 changes; 410 after restart; bookmarks every 5 s | `watch_history.rs:71`, `state.rs:484-503`, `watch_backend.rs:76` |
| Eventual tier | none in the binary; `ConsistencyTier` exists and nothing calls it; `consistency.default_tier` accepts only `Strong` | `engenho-types/src/consistency_tier.rs:38-63`, `boot_config.rs:309-321` |
| Transport | in-process routers only; NATS behind `teia-nats` (off) and unwired; `Fabric` has one arm, `InBinary` | `engenho-store/src/network.rs:37-161`, `nats_network.rs`, `engenho-config/src/fabric.rs:15-27` |
| Membership | no join, no peer keys; multi-master formations refused; a node that is not leader fails boot | `mesh.rs:385-413`, `boot_config.rs:276-293`, `runtime.rs:1038-1057` |
| revoada | chitchat gossip + a role-assignment Raft, exercised only in its own tests; in no binary | `engenho-revoada/src/membership/mod.rs:42`, `tests/r1_*`, `r2_consensus.rs` |
| Kubelet | needs a local `StoreMesh`; runs only pods whose `spec.nodeName` is this host | `engenho-kubelet/src/kubelet.rs:840-862,2328-2333` |
| Scheduler | binds with an unconditional merge Patch of `spec.nodeName`, no CAS; `pods/binding` not served | `engenho-scheduler/src/scheduler.rs:159-166`, `router.rs:1876-1879` |
| Fleet | five standalone one-node clusters: plo, rio, zek (NixOS), ryn, cid (Macs); `cluster.name` per host | nix `modules/pleme/nixos/engenho-node.nix:675-681`, `engenho-config/src/cluster.rs:51-84` |
| banken | one context per screen, read through the kube API; absorbers per kind (`Absorbing` / `Synced` / `Degraded`) | banken `src/absorb.rs:61-79`, `src/mcp.rs:321,359` |

## 2. The doctrine change

FLEET-DESIGN.md §1 makes **Declared** objects strong through one replicated
resource log. That puts every write in the fleet behind one quorum, so a laptop
on a train cannot update its own pod status. This plan moves the unit of
serialization from the cluster to the **object**:

- **Every object has one owner**, a node. Only the owner orders writes to that
  object, and it checks `resourceVersion` there (compare-and-swap). One object's
  history is therefore linearizable, as etcd promises.
- **Committed history replicates to every node eventually.** Each owner appends
  to its own log; peers pull what they lack (anti-entropy). Reads are served
  from the local replica.
- **A quorum is needed only for consensus metadata**: membership, the ownership
  map, leases and role leases. That stays a small openraft group on the
  always-on nodes.

Why this keeps the Kubernetes promises: clients already read through informer
caches, which are eventually consistent by design. What they rely on is that a
write with a stale `resourceVersion` fails, that one object's watch events
arrive in order, and that a resume point either works or returns 410. Each of
those is kept below.

## 3. The promise table

| Kubernetes promise (what etcd + kube-apiserver give) | How it is kept |
|---|---|
| A write with a stale `resourceVersion` fails with 409 | the write is forwarded to the object's owner, which checks the version (the existing `check_precondition`, run at the owner) |
| One object's versions are monotonic | the owner's log sequence; `resourceVersion` encodes owner, owner epoch and sequence |
| List and watch from a `resourceVersion` | the version a node hands out for lists is its own replica's apply position. A resume on the same node works; a resume on a different node returns 410, which every client already handles by relisting |
| Read your own write | the node that forwarded a write applies the owner's result to its replica before it replies |
| Create is unique by name | the owner of `namespace/name` decides `NotExists` (the existing `Txn{NotExists}` at the owner) |
| `generateName` | the owner picks the suffix (not implemented today; `handler.rs:1243-1248` returns 400) |
| Leases and leader election (`coordination.k8s.io`) | served from the consensus group, linearizable |
| Exactly one node gets a pod | the scheduler binds with CAS through `pods/binding`, at the pod's owner |
| Finalizers and graceful delete | ordered at the owner (existing `state.rs:1026-1170` logic, run there) |
| A node's own state is always writable | a node owns its `Node`, its pods' status, its node `Lease` and its Events, so those writes never need a peer |

Ownership: node-scoped objects belong to their node. Everything else
(Deployments, ConfigMaps, CRDs, Secrets) belongs to a voter assigned through the
ownership map, with an epoch that fences a stale owner.

## 4. Partitions and sleeping laptops

| Situation | Reads | Writes to its own objects | Writes to objects owned elsewhere | Leases, ownership, membership |
|---|---|---|---|---|
| A laptop is asleep or offline | its own replica, marked stale with its lag | continue; replicate on return | refused with a typed 503 naming the unreachable owner | unchanged; it is not a voter |
| One voter is down (3 voters) | every node | continue | objects that voter owned are re-owned after the lease ends (new epoch) | continue (2 of 3) |
| Voters lose their majority | every node | continue | continue where the owner is reachable | blocked, and reported as blocked, never guessed |

## 5. Pieces, and where each comes from

| Piece | Build or reuse |
|---|---|
| Consensus group (membership, ownership map, leases, role leases) | revoada's role-assignment Raft (`engenho-revoada/src/consensus/mesh.rs`), promoted into the binary with fencing epochs and a quorum-seeded bootstrap (RECOVERABLE-STATE.md §6) |
| Per-owner logs | split the single resource log: the existing fjall log and apply path, one log per owner instead of one per cluster |
| Ordering stamps | the HLC in `engenho-substrate-core/src/relogio.rs` |
| Anti-entropy | each node gossips per-owner high-water marks (revoada's chitchat layer); peers pull the missing ranges; a new node takes a catalog snapshot from any peer (`install_snapshot`) |
| Transport, inside the binary (doctrine 5.1) | QUIC with rustls; ed25519 node identities and SPKI pins (FLEET-DESIGN.md §11); bound to the tailnet address; one transport for Raft RPC, log pulls, forwarded writes and kubelet proxying |
| Kubelet on every node | unchanged in shape: it reads its local replica and runs its own pods |
| Controllers and scheduler | one active instance per role cluster-wide, under a role lease with a fencing epoch (FLEET-DESIGN.md §4) |
| Faces | the apiserver on every node serves the whole cluster; one kubeconfig names every node |

## 6. Build order

Each step lands with the test that proves it, and edge 18 gates every step that
adds a second node.

| Step | What | Done when |
|---|---|---|
| P0 | Doctrine: FLEET-DESIGN.md §1's Declared row, CONSISTENCY-FABRIC.md's tier table, the promise table above as a test plan; correct the stale lines the recon found (CONSISTENCY-FABRIC.md:86-88, 241-245, 300-304; IMPROVEMENT-PLAN.md:47, 54, 56, 126, 320) | docs agree with each other and with the code |
| P1 | One node, new model, no behaviour change: split the store into a consensus group (one voter) and a per-owner log (owner = self); `resourceVersion` encodes owner and sequence; `StoreMesh` takes any `RaftNetwork` instead of the concrete `InProcessRouter`; RPC replies become `Result` | every existing test passes, and `engenho-diff` against upstream shows no new divergence |
| P2 | Cluster identity and transport: a shared `cluster.name`, a stable node id, seed peers in config; the QUIC transport; a FaultRouter that drops, delays and partitions in tests; read fences | the FaultRouter partition test is recorded red, then green (edge 18) |
| P3 | Two nodes and a witness (FLEET-DESIGN.md R3): anti-entropy, forwarded writes, owner fencing, join as a learner, promotion to voter | a property test: any delivery order of the same writes converges to the same replica; per-object linearizability checked over recorded histories |
| P4 | Workloads across nodes: `pods/binding` with CAS; role leases for scheduler and controllers; `generateName`; authenticated kubelet and etcd listeners (edge 26) so logs and exec reach any node | a pod scheduled from plo runs on rio, and its logs read from ryn |
| P5 | The fleet: plo, rio and cid as voters, ryn and zek as owners of their own node objects; the nix modules render one cluster (`pleme.nixos.engenhoNode` and the darwin profile) | the five nodes show as one cluster; closing ryn's lid blocks nothing the table in §4 says continues |
| P6 | Faces: banken shows the fleet as one context, with each node's replica lag beside its rows (an engenho source next to the kube API one); revoada's R7 MCP tools | banken never blocks on a slow or sleeping node; a lagging node reads as lagging, not as empty |

## 7. Decisions for the operator

1. **Voters.** plo, rio and cid are the always-on machines (cid is a server Mac
   that must not sleep, `modules/pleme/darwin/server.nix:141`). Three voters
   tolerate one down.
2. **Where Secrets replicate.** Full replication puts every Secret on every
   laptop. FLEET-DESIGN.md §11 says Secret values are never gossiped. Options:
   voters plus the nodes that run a consuming pod, or voters only with reads
   proxied.
3. **Migrating today's five clusters.** Each node's store holds real state
   (ryn runs pangea-operator and Postgres). Either each store becomes that
   node's owned shard when it joins, or the fleet cluster starts empty and
   GitOps re-declares everything.
4. **The fleet cluster's name**, which becomes every kubeconfig context.

## 8. What stays only mitigated

- Objects owned by different nodes are not ordered against each other. A
  client can see a later change to one before an earlier change to another;
  informer-based controllers tolerate this, but a client that compares two
  objects' versions does not get a global order.
- A list is a snapshot of one node's replica, not of the cluster.
- Staleness on a sleeping node is unbounded while it sleeps; it is reported,
  never hidden.
