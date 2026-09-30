# engenho everywhere — the whole design

> **Status: design, 2026-09-30.** Nothing here is shipped unless §12 marks the
> rung done. This document ties together designs that already exist in this
> directory and adds what they leave open. It does not restate them.
>
> Chapters it builds on: [DISTRIBUTED.md](DISTRIBUTED.md) (revoada: membership,
> role consensus, content sync, attestation) · [CONSISTENCY-FABRIC.md](CONSISTENCY-FABRIC.md)
> (per-resource consistency tiers) · [FABRIC.md](FABRIC.md) (NATS carrier) ·
> [RESILIENCE.md](RESILIENCE.md) (formations, testing pyramid) ·
> [NATIVE-GITOPS.md](NATIVE-GITOPS.md) · [STATE-MACHINES.md](STATE-MACHINES.md) ·
> [QUALIFICATION.md](QUALIFICATION.md) · [IMPROVEMENT-PLAN.md](IMPROVEMENT-PLAN.md).
> The consensus seals (durable votes, quorum-gated promotion, fencing, and the
> tier of each) live in [RECOVERABLE-STATE.md](RECOVERABLE-STATE.md).

## 0. The destination

1. **engenho runs on every node, and every node is a full peer.** No node is
   special at install time. What a node does is decided while it runs.
2. **The Kubernetes API is the contract; everything under it is ours.** kubectl,
   Helm and Flux-shaped objects see an ordinary cluster. Under the facade engenho
   uses whatever the problem calls for: Raft, gossip, content addressing, NATS
   where the world already speaks it. A second face (tatara-lisp authoring over
   WASM/WASI workloads) comes later and sits beside the first.
3. **No state that cannot be recovered.** Every byte engenho holds has a named
   way back after the node that held it is gone.
4. **Leaders are elected and the control plane moves.** Any node can hold any
   control role, and roles move when nodes come, go or change.
5. **What a node hosts is inferred at the moment.** Nodes publish what they can
   do; workloads say (or reveal) what they need; placement matches the two. No
   workload names a node.
6. **Workloads move and resources are allocated dynamically, without the facade
   noticing.**
7. **Every mechanism is a best-in-class algorithm sealed into a type**, and each
   seal carries an honest tier: truly unrepresentable, rejected at a boundary, or
   only mitigated with its ceiling named.
8. **A node is a Helm release.** Below a thin boot floor, a machine is NixOS
   that starts engenho. Above it, everything the node is (its system generation,
   its packages, its services) is Kubernetes objects in one release, reconciled
   by engenho through the Flux API it already speaks, with Nix as the build
   layer those objects reference (§10).

The single rule under all seven: **engenho is one binary** (CLAUDE.md). Every
mechanism below is a Rust module reached by a trait call. An outside daemon
enters only as a fact about the world (an existing NATS, a registry, a
substituter), never as a requirement.

## 1. The one invariant: every byte has a class, and every class has a way back

| Class | What lives here | Source of truth | Way back after loss | Consistency |
|---|---|---|---|---|
| **Declared** | Kubernetes objects written through the API or reconciled from git | the replicated resource log | log replay or a snapshot from a peer; git-owned objects can also be re-derived from git | strong (Raft) |
| **Consensus metadata** | membership, role leases, placements, allocations, move records | the meta log | log replay or a snapshot from a peer | strong (Raft) |
| **Observed** | pod status, node conditions, capability facts, metrics | the world | observe again | eventual (gossip) |
| **Derived** | Endpoints, controller revisions, indices, the catalog blob | computed from declared + observed | recompute; the computation must be deterministic | follows its inputs |
| **Content** | Nix closures, OCI layers, large ConfigMaps, artifacts | a content address (BLAKE3, OCI digest, store path) | fetch the address from any holder, substituter or registry | immutable |
| **Workload data** | volumes, databases | the workload's replication or backup | restore from a replica, snapshot or log to a point in time | per volume |
| **Identity and secrets** | node keys, the cluster CA, ServiceAccount tokens, Secret values | keys: the node; CA: sealed on the voters; Secret values: the external secret source | keys regenerate and re-enrol; the CA recovers from a quorum; Secrets re-sync from their source | strong, never gossiped |
| **Ephemeral** | caches, backoff timers, in-flight RPCs | none | nothing to recover; losing it is harmless by construction | local |

**The seal.** A `StateClass` enum, and a registry where every stored kind and
every long-lived in-memory map declares its class. A parity gate fails the build
when a stored kind, or a map that survives a request, has no class. Tier: **CI
caught (C2)** until the store's write API requires a class in its signature, at
which point a new stored kind without a class does not compile (**truly
unrepresentable** for new kinds).

**Known violations, from IMPROVEMENT-PLAN §2, each a rung-0 fix (§12):**

- The catalog blob can fall more than the purge window behind the log, and
  `terminate()` persists nothing, so a stop during a write burst leaves a node
  that cannot boot. *Declared* state with no way back.
- `build_snapshot` writes neither the catalog nor `last_applied`. A snapshot is
  not a complete way back.
- Replaying the log after a change to what counts as a mutation renumbers
  history. *Derived* state that is not deterministic.
- Every boot rewrites the whole Node, dropping the operator's labels and taints.
  *Declared* state overwritten by *observed* state.
- The native backend cannot re-adopt running processes, so a restart re-runs a
  `restartPolicy: Never` pod. *Observed* state lost with no way to observe it
  again.

## 2. Layers

```
┌───────────────────────────────────────────────────────────────────────┐
│ Faces        Kubernetes API (the contract) · later: tatara-lisp/WASM   │
├───────────────────────────────────────────────────────────────────────┤
│ Roles        apiserver face · placement · controllers · gitops ·       │
│              allocators · movers. Each is a LEASED role (§4).          │
├───────────────────────────────────────────────────────────────────────┤
│ Consensus    one durable Raft engine, many groups (§3):                │
│              meta (membership, leases, placements, allocations, moves) │
│              resources (the Kubernetes store; sharded by key later)    │
├───────────────────────────────────────────────────────────────────────┤
│ Fabric       gossip: membership + capability facts (§5, §8)            │
│              anti-entropy: Merkle-summarised eventual sets (§8)        │
│              content: BLAKE3 / OCI / Nix addresses, fetched P2P (§8)   │
│              NATS: spoken where the world already runs it (§8)         │
├───────────────────────────────────────────────────────────────────────┤
│ Runtimes     native (Nix closures) · OCI (podman API, CRI) · later WASM│
└───────────────────────────────────────────────────────────────────────┘
```

## 3. Consensus: one engine, many groups

**Today:** the resource store runs openraft on a durable fjall log with a single
voter; revoada's role consensus runs openraft on `InMemoryStore`. Two Raft users,
two durability stories.

**Design:** one engine, **Multi-Raft** in the TiKV / CockroachDB sense. Many Raft
groups share one durable write-ahead log and one fsync pipeline; each group has
its own term, vote, log and state machine. The fjall-backed store the resource
log already uses becomes that engine; revoada's `InMemoryStore` goes away except
in tests.

| Concern | Choice | Why |
|---|---|---|
| Term, vote and log durability | written and fsynced **before** the reply that depends on them | Raft's safety proof assumes it; a node that forgets its vote can vote twice in a term |
| Disruptive elections | **PreVote** + **CheckQuorum** | a partitioned node rejoining must not depose a healthy leader; a leader that lost its quorum steps down |
| Membership change | openraft **joint consensus**; new members join as **learners** and are promoted only once caught up | a slow new voter never counts toward quorum before it can serve it |
| Small clusters | **witness** members (vote, keep the log tail, hold no state machine) | two data nodes plus a witness tolerate one failure without a third full replica |
| Snapshots | complete (state + `last_applied` + membership), written atomically, installable from any peer | a snapshot is a way back only if it is whole |
| Reads | ReadIndex by default; leader-lease reads only where bounded clock drift is an accepted assumption, stated per call site | lease reads trade safety for latency and must say so |
| Log format | a versioned, deterministic encoding; a change to apply semantics is a new version, never a silent reinterpretation | replay must reproduce history, not renumber it |

The seals for each row, and their tiers, are in RECOVERABLE-STATE.md.

**Groups:**

- **meta**: membership, role leases, placements, allocation tables, move records.
  Small, hot, always on the voter set.
- **resources**: the Kubernetes objects. One group today; split into key-range
  groups (by namespace or kind) only when one group's throughput is measured to
  be the limit.
- **workload groups** (later): a workload that wants replicated state gets its
  own group, placed like any workload.

**Who votes.** The voter set is itself placed (§5): voters need low churn, a
durable disk and no protected-node flag. Formations from RESILIENCE.md (Solo,
Pair, Quorum3M, Phalanx) pick the target count; the placement engine picks the
nodes.

## 4. Leased roles and the shifting control plane

Two kinds of leadership, kept separate:

- **Raft leadership** of a group: who orders that group's log. openraft decides.
- **Role leases**: who runs a control role (placement, a controller, the gitops
  reconciler, an allocator, the apiserver face on a given node). Granted by
  committing a lease entry to the meta group.

**A lease is a fencing token.** Its epoch is the meta-log index of the grant, so
epochs only grow. Every side effect a role performs carries its epoch, and every
receiver checks it: the store's write border refuses a write whose epoch is older
than the role's current one, and a node's runtime refuses a start or stop from a
stale epoch. This is Kleppmann's fencing-token construction. It makes safety
independent of clocks: a paused or partitioned old holder can still try, and the
border refuses it.

**Seal:** role write paths take a `LeaseEpoch`, so "a role writes without an
epoch" does not compile (**truly unrepresentable**). "A stale epoch is refused"
is enforced at the single write border (**rejected at a boundary**). What remains
**only mitigated** is lease *expiry* timing, which depends on bounded clock drift
for liveness (never for safety).

**The control plane moves** because control roles are workloads with
requirements (§5). When a holder dies, gossip suspects it, its lease expires, the
lease is re-granted to an eligible node at a higher epoch, and the old holder's
late writes are refused.

**Damping.** A typed `ShiftDecision` records why each move happened, and a
hysteresis window (at most one role shift per node per interval, configurable)
stops oscillation (DISTRIBUTED.md open question 4).

**Bootstrap from zero.** The first node forms `Solo` and grants itself every
role. Later nodes find peers through a seed list, then local discovery, then
key-addressed discovery, in that order, and join as learners.

## 5. Capabilities and inferred placement

**Capability record.** Each node measures itself and publishes typed facts by
gossip. They are *observed*, never configured, so they are true now:

| Fact | Examples |
|---|---|
| runtimes | `native` (Nix system, store reachable), `oci` (platforms, registries reachable), `wasm` (later: WASI version) |
| platform | arch, OS |
| capacity and allocatable | CPU, memory, disk, per-device counts |
| devices | GPUs, radios, serial devices |
| reachability | which networks the node sits on (a home LAN, a VPN) |
| stability | uptime, battery or mains, churn history |
| protection | a node that must not host infrastructure that could take its main job down |

Each fact carries a version (an incarnation number plus a hybrid logical clock),
so a newer observation always wins and a restarted node cannot resurrect an old
fact.

**Requirements are mostly inferred from the workload itself:**

- An image reference that is a Nix store path needs `native` and that path's
  system.
- An OCI image needs `oci` and one of the platforms in the image's own index
  manifest. An image built only for amd64 therefore lands only where amd64 runs,
  with nobody writing that down.
- Resource requests, node selectors, affinities and tolerations are read from the
  pod spec as usual.
- A control role needs what §3 and §4 say (durable disk, low churn, not
  protected).

**Matching.** Filter by hard requirements, then score by preferences
(spreading, packing, locality). For single pods, that is enough. For batches
(a formation change, a node draining), assignment is solved as a min-cost flow
(the Quincy / Firmament construction), with multi-resource packing scored by the
dot product of demand and free capacity (the Tetris heuristic) and fair sharing
by dominant resource fairness where tenants compete. Start with filter-and-score;
add the flow solver when a measured batch placement is poor.

**A placement is decided once.** It is committed to the meta group as
`Placement { pod, node, epoch }`. The facade shows it as `spec.nodeName`. A
node's kubelet starts only pods with a committed placement naming it. If no node
qualifies, the pod stays `Pending` with a typed reason naming the unmet
requirement; nothing is dropped silently.

**Placements follow the facts.** When a node's capabilities change (it leaves,
it gains a runtime, it becomes protected), the pods whose requirements it no
longer meets are re-placed through §6.

## 6. Transparent workload movement

**A move is a typed state machine:**

```
Planned → Preparing → Draining → HandedOff → Running → Cleaned
```

- *Preparing*: the target fetches the content (realises the closure, pulls the
  image) and pre-warms. Nothing is stopped yet.
- *Draining*: the source stops taking new work. For a singleton, its lease is
  revoked.
- *HandedOff*: the target is granted the lease at the next epoch. Service
  endpoints switch in the **same** meta-log entry, so the facade never shows two
  owners or none.
- Transitions are methods on typestate types, so skipping a state (starting on
  the target before the source's lease is revoked) does not compile.

**By kind of workload:**

| Kind | How it moves | Invariant |
|---|---|---|
| Stateless, replicated | surge first on the target, then drain the source behind a readiness barrier | capacity never dips below the declared minimum |
| Singleton | fenced handoff: the new instance starts only after the old lease is revoked or expired | at most one instance acts at a time |
| Stateful with a volume | replicate or re-attach the volume, then hand off behind a sync barrier | no write is lost across the move |
| Native closure | the target realises the closure before *Draining* | the target never waits on content after the source stops |
| OCI | the target pulls before *Draining* | same |
| WASM (later) | snapshot the instance's memory and WASI handles, resume on the target | the workload observes no restart |

**Triggers:** a capability change (§5), a node drain, a rebalancing decision
(damped like §4), or an operator request.

## 7. Dynamic allocation

Everything scarce is allocated the same way: pod and service IPs, ports, volumes,
device shares, ID ranges, CPU and memory reservations.

- **Allocation tables live in the meta group.** Range and bitmap allocators are
  replicated state machines; `Allocate` and `Release` are log commands. One
  serialised state machine means nothing is granted twice.
- **Every allocation is owned by a lease.** The holder is a pod or a node. When
  the holder's lease expires, the allocation is reclaimed, as with etcd's
  lease-attached keys. Nothing leaks.
- **Blocks keep the hot path local.** A node takes a block (for example a range of
  pod IPs) through consensus and hands out addresses inside it locally, the way
  Calico's IPAM blocks work. Consensus sees one entry per block, not one per pod.
- **Seal:** `Allocation<R>` can only be built from a committed grant and carries
  its lease. Double allocation has no code path (**truly unrepresentable**
  inside the state machine); a leak is bounded by lease expiry (**only
  mitigated**, ceiling: the expiry interval).

## 8. The data fabric

engenho already chose a tier per resource (CONSISTENCY-FABRIC.md). This design
keeps that table and adds what the fleet needs:

- **Gossip** (SWIM with the Lifeguard refinements, via chitchat) carries
  membership and capability facts. Suspicion is local and cheap; a fact's
  version (§5) makes merges deterministic.
- **Anti-entropy** for larger eventual sets (audit trails, metric rollups):
  nodes compare Merkle summaries and exchange only the differing ranges, as
  Dynamo and Cassandra do.
- **CRDTs** only where concurrent writers are legitimate and merging is the
  right answer (counters, add-wins sets of observations). Never for anything a
  single decision must own; those go through consensus.
- **Time:** hybrid logical clocks order events across nodes without trusting wall
  clocks.
- **Content:** BLAKE3-addressed transfer between peers (iroh), Nix substituters,
  OCI registries. Anything addressed by content can be fetched from whoever has
  it, which is what makes the *Content* class recoverable.
- **NATS** is a protocol engenho speaks, not a server it needs: at a fleet edge
  where NATS already runs (a leaf connection between sites), engenho uses it as a
  carrier. Inside a site the carriers are in-process.

## 9. GitOps and configuration

engenho reconciles git itself (NATIVE-GITOPS.md), speaking the Flux API for the
subset it implements. For the fleet this means:

- A site's desired state is a git path. Each component is a `HelmRelease` with
  its own values, and a component is switched on or off by its values, not by
  editing manifests.
- Git-owned objects are *Declared* state with a second way back: if the resource
  log were lost, reconciling the path rebuilds them.
- The same path shape serves testing: a test scenario is a git path (or an
  overlay) that turns components on or off.

### 9.1 A release chooses the Kubernetes it runs against

Charts already say which Kubernetes they expect: `kubeVersion` in `Chart.yaml`,
`.Capabilities.KubeVersion` and `.Capabilities.APIVersions.Has` in templates, and
the `apiVersion` of every object they render. Today engenho presents one face, a
compile-time constant (`engenho_types::KUBE_VERSION`) behind `/version` and
discovery. Here the face becomes a value, chosen per release while running.

- **The type.** `ApiFace { version, served, flavour }`: a Kubernetes version, the
  set of group-versions served, and a flavour (upstream, or a named provider
  surface). A face is built only from a registry of versions engenho can
  convert: every kind has one storage version, and each served version converts
  to and from it, as the upstream apiserver does. A face that serves a version
  with no conversion path cannot be constructed (**truly unrepresentable**).
- **Resolution, in tiers.** The same layered, typed, hot-reloaded configuration
  every engenho setting uses (MANY-FACES.md), highest tier winning: the compiled
  default → the node's config file → the cluster's live config object → a
  namespace's face object → the release, derived from its chart. The chart tier
  picks the newest face that satisfies the chart's `kubeVersion` constraint and
  serves every `apiVersion` it renders. `engenho ctl config` already reports which
  tier set a value; the face is one more value it reports on.
- **Rendering against the real target.** Native GitOps renders Helm in-process,
  so `.Capabilities` is an input engenho supplies from the resolved face. A chart
  is rendered against exactly the API it will be applied to.
- **Several faces at once.** Each face is a view with its own endpoint,
  `/version`, discovery document and kubeconfig, over the shared store. Objects
  are stored at their storage version and converted per view on read and watch.
  A release lands in the view of its face.
- **It changes while running.** A chart bump that raises `kubeVersion` moves the
  release to a newer view without a restart. Nothing is migrated, because storage
  is shared.
- **Refusal is scoped to the release.** A chart whose constraint no face
  satisfies, or which renders an `apiVersion` its face does not serve, fails that
  release with a typed reason naming the constraint and the faces available.
  Other releases proceed.
- **Testing across targets.** The face is an axis of a test matrix: one chart,
  rendered and applied against several faces in one run, answers "does this chart
  work on the versions and providers it claims" before any real cluster is
  involved.
- **Tier.** Schema and discovery per face are exact. Behaviour that differs
  between Kubernetes versions beyond schema (defaulting changes, feature gates) is
  **only mitigated**; its ceiling is the versions the `engenho-diff` oracle has
  been run against.

## 10. The node is a release: Nix through the same facade

§9 reconciles workloads. This section extends the same reconciler down to the
machine, so a Helm release describes a whole node and Linux is reached through
the Kubernetes API.

**The floor.** A node's control plane may own anything it can rebuild without
itself. What engenho cannot rebuild without itself is what starts and repairs
engenho, so that stays one shared NixOS base, identical on every node:

| Below the floor (NixOS, boots the machine) | Why |
|---|---|
| kernel, boot loader, disks, mounts | needed before any process runs; changes land at reboot |
| network, and any name resolution the node serves to others | engenho and its peers are unreachable without it |
| remote access (ssh, the VPN) | how an operator recovers a node whose engenho is broken |
| the Nix daemon | builds and realises everything above the floor |
| an out-of-band reconciler that can switch the boot generation | it repairs engenho, so it cannot run on engenho |
| boot-generation rollback on failed health | acts before engenho exists |
| the engenho daemon and its keys | the platform itself |

Nothing below the floor may depend on anything above it. That is an evaluation
assertion on the base (no base unit ordered after engenho), so a chart
dependency cannot creep into it (**rejected at a boundary**, at evaluation).

**Above the floor, everything is an object.** engenho serves these kinds and
reconciles them in-process, beside the Flux kinds of §9:

| Kind | Declares | Reconciler | Class (§1) |
|---|---|---|---|
| `NixClosure` | a store path or flake output that must be present on a node | realise from substituters or build (§ builds, below), hold a GC root while any object refers to it, report `ClosureUnavailable` with the reason | Content |
| `NixProfile` | packages installed for a user or the whole node | build a profile generation from closures, switch it atomically, keep the previous one | Declared + Content |
| `NodeGeneration` | the node's system closure above the floor | realise, activate, gate on health, keep the previous generation, roll back on a failed gate | Declared |
| workloads with `image: nix:<path>` | services (already served today) | start only once the referenced closure is realised | Declared + Observed |

A flake is a source like a git repository: a `GitRepository` (or an OCI
artifact) pins it, and the lock inside it pins everything it builds.

**Together or apart.** One release may carry a `NodeGeneration`, its
`NixProfile`s and its workloads, and then they roll forward and back as one
Helm revision: the node changes as a unit, and a rollback restores the system,
the packages and the services that were tested together. Or they are separate
releases on separate cadences (a package set that changes daily, a system
generation that changes weekly), ordered with `dependsOn`. The choice is the
chart author's; the reconciler handles both.

**The switch is a typed move.** Activation reuses the movement machinery of §6:

```
Realised → Activating → Healthy
                      ↘ RolledBack
```

- Activating a closure that is not realised has no code path: `activate` takes a
  `Realised` value (**truly unrepresentable**).
- A switch that leaves no previous generation to return to cannot be built: the
  move holds both (**truly unrepresentable**).
- The health gate observes the world: probes, reachability, the node's own
  control API. It is **only mitigated**; its ceiling is what the probes cover.
- A generation that needs a reboot (kernel, boot loader) reports
  `PendingReboot`. The reboot is a node drain (§6) followed by the out-of-band
  reconciler's boot switch, so workloads move before the node goes down.

**Builds are placed.** Realising a closure is substitution when a cache has it
and a build when none does. A build needs the `native` runtime and the target's
system, so it is placed like any workload (§5): on the node itself, on a builder
with spare capacity, or on a node of the right architecture. The result moves to
the target by content address (§8). Remote builders stop being configuration and
become placement.

**What this abstracts.** A chart sees Linux only through the API: users and
sysctls as fields of a `NodeGeneration`, ports as allocations (§7) from which
the firewall is derived, devices as capability facts (§5) that pods claim. The
node's identity and the floor are the only per-machine facts left outside the
release.

**Authoring.** The kinds are generated from the same Rust types that serve them,
and their values schemas from the Nix option types that produce the closures, so
the chart surface and the NixOS surface cannot drift. A new kind is a Rust type
with a derive, never a hand-written schema.

## 11. Identity and security

- Each node has an ed25519 identity; peers authenticate with mutual TLS (rustls)
  and pin keys (CONTROL-PLANE.md's SPKI pins).
- The cluster CA key is sealed on the voters and recoverable from a quorum; if it
  is lost, the fleet re-roots with a rotation, not a rebuild.
- Secret values are never gossiped or content-synced (DISTRIBUTED.md open
  question 1). They travel through the resource log and are re-synced from their
  external source on recovery.

## 12. Testing and proof

| Layer | Tool | What it proves |
|---|---|---|
| Deterministic simulation | madsim or turmoil in `cargo test` | consensus, leases, placement, moves and allocation under seeded partitions, delays and crashes; every failure replays from its seed |
| Model checking | Stateright (Rust) | every interleaving of the lease handoff, the move state machine and the allocator, for small node counts |
| Linearizability | a Maelstrom adapter for openraft, checked by Knossos | the store and the meta group never return a non-linearizable history |
| Chaos | RESILIENCE.md's eight scenarios, plus: capability flap, move during a partition, allocation across a leader change, restart during a write burst | the system recovers every class in §1 |
| Facade fidelity | `engenho-diff` and `engenho-oracle` against upstream Kubernetes | the contract in §0.2 holds (QUALIFICATION.md) |

Every gate is recorded red once before it lands.

## 13. The build order

| Rung | Delivers | Proof it is done |
|---|---|---|
| **R0 Recoverable single node** | the §1 known violations fixed; `StateClass` registry and its gate | a restart during a write burst boots; a snapshot restores a node from empty; labels and taints survive a reboot; a native pod is re-adopted, not re-run |
| **R1 One consensus engine** | revoada on the durable log; PreVote, CheckQuorum; complete snapshots; versioned log format | the restart-and-vote test and the minority-cannot-promote test (RECOVERABLE-STATE.md) |
| **R2 Capabilities and placement, one node** | capability record; requirements inferred from specs; filter-and-score; committed placements; typed `Pending` reasons | an OCI pod never lands on a native-only node; an impossible pod reports why |
| **R3 Two nodes and a witness** | learner join; gossiped capabilities; cross-node placement | deterministic simulation of join, crash and rejoin |
| **R4 Leased roles and a moving control plane** | lease grants with fencing epochs; role placement; damping | killing a role holder moves the role and the old holder's writes are refused; chaos scenarios 1–8 pass in simulation |
| **R5 Allocation** | meta-group allocators; IP blocks; ports; volumes | no double grant and no leak under simulated leader changes |
| **R6 Movement** | the move state machine for stateless, then singleton, then stateful | Stateright over the handoff; a singleton never runs twice |
| **R7 Native GitOps** | HelmRelease / GitRepository in-process; values-driven switches | a component turns on and off by changing only its values |
| **R7a Faces per release** | `ApiFace` from the conversion registry; tiered resolution; per-face views; `.Capabilities` from the face | one chart pinned to an older `kubeVersion` and one to the current face run side by side on one store, each seeing its own `/version` and discovery |
| **R7b The node is a release** | `NixClosure` realisation with GC roots; `NixProfile`; `NodeGeneration` with a health-gated switch and rollback; the floor assertion; placed builds | a node's packages, services and system generation change by editing only release values, and a generation that fails its gate rolls back with no operator |
| **R8 OCI-capable nodes** | nodes with an OCI runtime join the fleet and third-party charts land on them by inference | a chart whose images are amd64-only lands on an amd64 OCI node without any node named |
| **Later** | the tatara-lisp / WASM-WASI face | a WASM workload moves with no observed restart |

## 14. What stays only mitigated, and why

- **Lease expiry** depends on bounded clock drift. Fencing keeps it a liveness
  concern, never a safety one.
- **Reads served under a leader lease** trade safety for latency; each such read
  site is named.
- **The outside world**: a registry that deletes an image, a disk that lies about
  fsync, a volume configured without replication. engenho reports these as typed
  failures; it cannot recover what the world threw away.
- **Leaks** are bounded by lease expiry, not prevented outright.

## 15. Open questions

1. Split rule for the resource group: by namespace, by kind, or by key hash.
2. Witness placement: which nodes may be witnesses, and whether a witness may
   also be a protected node (it holds no workloads).
3. Whether rebalancing moves (as opposed to capability-driven ones) are on by
   default.
4. How the WASM face declares requirements: as typed forms in the authoring
   language, so placement needs are checked when the workload is written.
