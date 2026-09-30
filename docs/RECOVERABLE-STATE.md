# RECOVERABLE-STATE — no state engenho cannot get back

engenho only has to honour the Kubernetes API facade. Underneath it may use
any mechanism (Raft, gossip, CRDTs, a message log, a custom store) to keep a
decentralised mesh in sync. This document fixes what that mechanism must
guarantee, picks the algorithm for each guarantee, and names the type that
makes the violation impossible to write, graded honestly.

Companion docs: [DISTRIBUTED.md](DISTRIBUTED.md) (revoada, the mesh layer),
[CONSISTENCY-FABRIC.md](CONSISTENCY-FABRIC.md) (correnteza, the read/write
contract), [FABRIC.md](FABRIC.md) (the fenced NATS draft),
[NATIVE-GITOPS.md](NATIVE-GITOPS.md), and
[IMPROVEMENT-PLAN.md §5.3](IMPROVEMENT-PLAN.md) (revoada stays fenced).

## 0. The law and how it is graded

**Every piece of state has a recovery path.** A node that crashes, restarts,
is partitioned or is replaced must be able to get back every fact it held,
from its own disk or from a quorum of peers, and must never act on a fact it
cannot prove it still holds. Leaders are elected, the control plane moves
between nodes, and what a node hosts is inferred from what it advertises at
the moment, never from a hand-assigned name.

Tiers, from [theory/UNREPRESENTABILITY.md](https://github.com/pleme-io/theory/blob/main/UNREPRESENTABILITY.md):

| Tier | Meaning | The test |
|---|---|---|
| **truly-unrep** | No expression outside the owning module constructs the bad state. | Name the expression that would build it. There is none. |
| **parse-time** | The bad state is refused at a boundary (deserialise, config validate, API admission). | An in-process caller could still build it; nothing from outside can. |
| **only-mitigated** | A test, a gate or a runtime check catches it. | The constructing expression is named, with the gate that catches it. |

The **ceiling** is the reason a row cannot go higher. A ceiling phrased in
terms of our own code is ours to lift. A ceiling that is a fact about the
world (a disk that lies about fsync, a clock that jumps) is typed and stays.

## 1. The invariants

| # | Invariant | Best-fit canon | Seal (the type) | Tier today | Ceiling / constructing expression |
|---|---|---|---|---|---|
| I1 | **No unrecoverable state.** Every fact is on local disk or re-derivable from a quorum. | Raft hard state (term, vote, log) persisted before any RPC reply; snapshots + log compaction; state machine re-derived by replay. | The durable store: `RaftStore::durable(dir)` writes the hard-state file with tmp + fsync + rename before `save_vote` / `append` / `save_committed` return, and `open` restores it. Snapshots are persisted with the log, so a purged prefix is still recoverable. | **truly-unrep inside `RaftStore::durable`**; **only-mitigated at the crate** | `RaftStore::volatile(identity)` still exists (kept for tests and the in-process simulator, configured off, not deleted). Any caller can pick it. World ceiling: a disk that acknowledges fsync and loses the write. |
| I2 | **At most one leader per term.** | Raft election safety: one durable vote per term; PreVote so a rejoining node cannot bump the term and depose a healthy leader. | `save_vote` persists before openraft replies to `RequestVote`, so a restarted node reads back the vote it cast and refuses a second one in the same term. openraft 0.9's candidate path is already PreVote-shaped (`vote` with last-log comparison). | **only-mitigated** | The guarantee holds inside openraft's state machine; our code proves it with `restarted_node_refuses_a_second_vote_in_the_same_term`. A `Durable<Vote>` witness type at our border would lift this to truly-unrep for our code paths, but openraft owns the vote path, so the witness would be ceremony around a call we do not make. |
| I3 | **No split-brain promotion.** A minority partition never changes roles. | Majority quorum over the configured voter set (Raft commit rule); CheckQuorum on the leader; a local majority check before a policy even proposes. | `QuorumWitness`: private field, built only by `RoleAssignment::quorum_witness(reachable)` when `reachable` holds a strict majority of configured voters, through the one quorum fold (`Tally`). `TopologyReactor::observe_membership` reacts to loss only with a witness in hand. `AutoReplacementPolicy` promotes only when gossip sees a majority of the Etcd holders. | **truly-unrep for the reactor's loss path**; **only-mitigated for `AutoReplacementPolicy`** | The reactor's bootstrap path (no configured voters yet) needs no witness, so two cold partitions can each form. That is bounded by the seed list, not by a type. `AutoReplacementPolicy` builds `RoleAssignment::Promote` directly; the gate is a function it calls. Raft's commit rule is the backstop for both: a minority leader cannot commit. |
| I4 | **Committed = durable on a quorum.** | Raft commit index advances only on a majority of persisted appends. | openraft advances `committed` from follower acks; our `append` calls `log_io_completed` only after the fsync, so an ack means on disk. | **only-mitigated** | Our side is truly-unrep (no ack before fsync); the quorum count is openraft's. A `Committed<T>` newtype built only from a `ClientWriteResponse` is the next step (§6). |
| I5 | **A stale leader cannot write (fencing).** | Fencing tokens (Kleppmann) = the Raft term; leader leases only with bounded clock drift, fencing tokens otherwise. | Designed: `FencingToken { term, index }`, built only from a committed log id, required by every external side effect (bind, allocate, hand off). A sink rejects a token lower than the highest it has seen. | **only-mitigated** (design) | Today nothing outside Raft takes a token; a side effect performed by a deposed leader after its last commit is not refused. |
| I6 | **Membership change safety.** | Joint consensus, or single-server change one at a time (Ongaro §4). | `RaftMesh::add_voter` goes learner, then `change_membership`, which openraft runs as joint consensus. | **only-mitigated** | Nothing stops a caller issuing two changes concurrently; openraft serialises them. A typed `MembershipChange` queue with one in flight is the seal. |
| I7 | **The capability view converges.** | SWIM + Lifeguard for liveness; the advertised record as an LWW register per node keyed by (node, generation) with a hybrid logical clock; Merkle anti-entropy (chitchat's scuttlebutt digest) to repair. | Single-node today: the capability record is a pure function of the node's own config (runtime backend, arch) rendered into Node labels at registration, so it cannot disagree with the node. Mesh: the same record gossiped, last-writer-wins on the node's own generation. | **truly-unrep single-node**; **only-mitigated in the mesh** | Gossip convergence is probabilistic in bounded time; a reader acting on a stale view is only corrected by the placement being committed (I8). |
| I8 | **Placement decided exactly once and survives leader change.** | Decision committed through the log (the bind is a write to the store, which is the log); the decision is idempotent by pod UID; a new leader re-derives pending work from committed state, never from memory. | `Feasible` is built only by `filter`, which now runs the `Capability` plugin; the bind is a store write guarded by `resourceVersion`, so two schedulers racing produce one bind and one conflict. An unmet requirement is a typed `Rejection::MissingCapability`, surfaced as `PodScheduled=False / Unschedulable`, never a silent drop. | **truly-unrep for "placed on a node lacking a required capability"**; **only-mitigated for exactly-once** | Exactly-once rests on the store's compare-and-set; a second scheduler instance on a stale store view is refused by the conflict, not by a type. |

## 2. The steps, and their status

| Step | Status | What | Test (red before the fix) | Tier after |
|---|---|---|---|---|
| S1 | planned | Durable Raft hard state in revoada (`RaftStore::durable`) | `tests/durable_vote.rs::restarted_node_refuses_a_second_vote_in_the_same_term` granted the second vote on the volatile store | I1, I2 as in §1 |
| S2 | planned | Quorum-gated loss reaction in `TopologyReactor` | `tests/topology_reactor.rs::a_minority_partition_cannot_promote` promoted 2 standbys with 2 of 5 voters visible | I3 reactor: truly-unrep |
| S3 | planned | Majority-gated `AutoReplacementPolicy` | `policy::tests::a_minority_partition_cannot_promote_a_replacement` promoted with 2 of 5 Etcd holders visible | I3 policy: only-mitigated |
| S4 | planned | Typed capability record + `Capability` filter plugin in engenho-scheduler, advertised at node registration | `capability` tests: an OCI pod never lands on a native-only node; a requirement met nowhere yields `Unschedulable` with the missing capability named | I7 single-node, I8 capability |

revoada stays behind the fence of IMPROVEMENT-PLAN §5.3: S1 to S3 change
its safety story, not its shipping status. Shipping multi-node is still
gated on edge 18, and it now needs I5 and I6 sealed too, not only the three
gaps DISTRIBUTED.md named. The capability record (S4) ships, because it runs
single-node today and is mesh-ready by construction: the matcher reads Node
objects, which are the same whether one node or fifty wrote them.

## 3. Capability-driven placement

What a node hosts is inferred at the moment from what it advertises.

**The record** (`engenho_scheduler::capability::NodeCapabilities`) is read
from Node labels under `capability.engenho.pleme.io/`:

| Label | Meaning | Written by |
|---|---|---|
| `runtime.native` = `true` | Runs `nix:` closures as host processes | registration, from `kubelet_backend = native` |
| `runtime.oci` = `true` | Runs OCI images | registration, from `podman_api` / `podman` |
| `gpu` = `<count>` | GPU devices | operator or a device plugin (not auto-detected yet) |
| `house` = `true` | The house node (radios, resolver) | operator |
| `kubernetes.io/arch` | Go `GOARCH` spelling | registration (already) |

**The requirement** (`WorkloadRequirements`) is inferred from the pod, never
from a node name: a container image `nix:/nix/store/...` requires
`runtime.native`; any other image requires `runtime.oci`; annotations
`requirements.engenho.pleme.io/gpu` and `.../house` add the rest.

**The matcher** is a trait, `CapabilityMatcher`, with one shipped
implementation (`LabelCapabilityMatcher`). It runs as the `Capability`
filter plugin, after `NodeSelector` and before `TaintToleration`, so an
unmet requirement is reported in upstream's `FailedScheduling` shape:
`0/2 nodes are available: 2 node(s) lacked capability runtime.oci.` That is
written as `PodScheduled=False / reason=Unschedulable` on the pod. The facade
shows `spec.nodeName`, the condition and the message; nothing underneath is
visible to kubectl.

Mesh-ready: the matcher consumes Node objects, so it does not change when
Nodes come from gossip instead of one registration. What changes is I7's
tier (the view may lag) and I8's reliance on the commit.

## 4. Transparent workload movement

The facade shows the workload continuously; underneath it moves between
nodes when capability or health changes. Design only; implementation follows
S1 to S4.

**The move state machine**, as a typestate:

```
Planned ──drain──▶ Draining ──revoke──▶ HandedOff ──start──▶ Running
   │                  │
   └──abort──▶ Aborted ◀──abort──┘
```

`Move<Planned>`, `Move<Draining>`, `Move<HandedOff>`, `Move<Running>` are
distinct types; each transition consumes `self` and returns the next state,
so `Planned → Running` or `Running → Draining` has no method and does not
compile. `HandedOff` is built only from a `Revoked` witness (below).

| Invariant | Canon | Seal | Tier (designed) | Ceiling |
|---|---|---|---|---|
| M1 **No double-running a singleton.** | Fencing tokens (Kleppmann); lease-based handoff; blue/green with a drain barrier. | The new instance starts only in `Move<HandedOff>`, whose sole constructor takes `Revoked { token }`: proof that the old holder's `FencingToken` was superseded by a committed higher token, or its lease expired by the leader's clock plus the maximum drift bound. Every side effect of the workload carries its token and the sink refuses a lower one. | truly-unrep for our start path; only-mitigated end to end | A partitioned old instance that keeps running and writes to a sink that does not check tokens. That sink is outside engenho. |
| M2 **State carried or re-derivable.** | Snapshot + log (Raft's own recovery shape); CRIU-style checkpoint/restore where the runtime supports it; stateless-by-construction otherwise. | A `MovePlan` names its `StateCarriage`: `Stateless` (proved by the pod having no writable volume), `Volume { snapshot }`, or `Checkpoint { image }`. There is no arm for "state left behind", so a plan cannot be built without saying how state travels. | parse-time (the plan is refused at construction) | Whether a `Stateless` workload really keeps no state outside its volumes (a process writing to `/tmp` on a native host) is not observable. |
| M3 **Identity and endpoint stable.** | Atomic cut-over: the Endpoints update is committed in the same log entry as the handoff. | `HandedOff` carries the EndpointSlice change; `start` commits both, so there is no moment when the Service names neither or both instances. | only-mitigated (design) | The facade's Service is updated by the endpoint controller today, not by the move; binding them is the implementation work. |

## 5. Dynamic allocation of shared resources

IPs and ports, volumes, leader slots, GPU and device shares, name and ID
ranges, CPU and memory reservations. Design only; implementation follows
S1 to S4.

| Invariant | Canon | Seal | Tier (designed) | Ceiling |
|---|---|---|---|---|
| A1 **No double allocation across the mesh.** | Consensus-committed allocation table; range or bitmap allocators replicated through the log (Kubernetes' own ClusterIP allocator is a bitmap in etcd). CRDTs only where a double grant is harmless (an idempotent label, not an IP). | `Allocation<T>` has private fields and one constructor, the state machine's apply of a committed `Grant { resource, holder, lease, token }`; a caller holding an `Allocation<Ip>` holds proof it was committed. The allocator's apply is deterministic, so every replica agrees who holds what. | truly-unrep for the committed table | Something allocating outside engenho (a DHCP server, the host picking a port) is not in the table. |
| A2 **No leak.** | Lease-owned allocations with expiry, renewed by the holder, reclaimed by the leader when the lease lapses (Chubby, etcd leases). | Every `Grant` carries a `Lease`; there is no grant without one. Reclaim is a committed `Expire` entry, so a reclaimed resource is re-grantable only after the commit. | only-mitigated | Reclaim needs a live leader; a mesh with no quorum leaks until quorum returns, by design (reclaiming without quorum would risk A1). |
| A3 **Allocation survives leader change.** | The table is Raft state, so it is in the snapshot and the log (I1). | Same as I1: no in-memory-only table exists; the allocator is a `MeshShape`-like state machine. | inherits I1 | Inherits I1's ceiling. |
| A4 **A stale holder cannot use a reclaimed grant.** | Fencing tokens again. | `Allocation<T>` carries the grant's `FencingToken`; a device or port sink checks it. | only-mitigated | The sink must check; the kernel does not check tokens for a port bind. |

## 6. What remains only-mitigated, and why

1. **The volatile store is still constructible** (I1, I2). Kept for the
   in-process simulator and tests; making `RaftMesh` take a durability
   parameter with no volatile arm outside `cfg(test)` would seal it, and it
   touches every existing revoada test. Owed.
2. **Bootstrap forms without a quorum** (I3). A cold mesh has no voters to
   count. The seal is a typed seed set agreed before first election
   (`initialize_with_voters`), which is where openraft already requires it;
   the reactor's bootstrap path does not yet consume it.
3. **`AutoReplacementPolicy` gate is a function call** (I3). The policy
   returns `RoleAssignment` values it builds itself. Sealing it means
   `Promote` constructible only with a `QuorumWitness`, which changes the
   command type every face serialises. Owed with I5.
4. **Fencing** (I5, M1, A4). No sink outside Raft checks a token yet.
5. **Committed<T>** (I4). The newtype does not exist; the property is
   openraft's.
6. **Capability view in the mesh** (I7). Gossip is eventually consistent;
   only the commit (I8) makes a placement exact.
7. **Exactly-once placement** (I8) rests on the store's compare-and-set.

## 7. Later

The **tatara-lisp / blue workload face**, compiled to WASM components with
WASI capabilities, is the future workload face that makes movement (§4) and
allocation (§5) language-native: an instance's memory and WASI handles are a
snapshot by construction, and the form that declares the workload types its
own placement requirements. It is deliberately not designed here.
