# aldeia: every node, one cluster, eventually consistent behind the Kubernetes face

Status: **plan** (2026-10-08, revision 3). Nothing here is built. Companion docs:
[FLEET-DESIGN.md](FLEET-DESIGN.md) (the whole-fleet design this adjusts),
[RECOVERABLE-STATE.md](RECOVERABLE-STATE.md) (the guarantees and their seals),
[CONSISTENCY-FABRIC.md](CONSISTENCY-FABRIC.md), [DISTRIBUTED.md](DISTRIBUTED.md),
[IMPROVEMENT-PLAN.md](IMPROVEMENT-PLAN.md) (edge 18 gates every step that adds a
second node).

**aldeia** (Brazilian-Portuguese *village*: many houses, one settlement) is the
name of the one cluster: its `cluster.name` and its kube context
(`theory/NAMING.md`, The Swarm 群).

## 0. Destination

engenho runs on every machine in the fleet as one Kubernetes cluster, aldeia.
Every node holds a replica of the cluster's state and serves the Kubernetes API
for the whole cluster from it. Replication between nodes is **eventually
consistent**. Serialization exists only where Kubernetes itself is stateful: a
write to one object, and the coordination facts (membership, leases, ownership)
that need a quorum. **No node is assigned a role by name**: who votes, who
leads, who owns an object and who runs a control role are elected from what
each node observably is, and re-elected as that changes. A node that sleeps or
roams keeps running its own pods and writing its own state, and catches up when
it is back.

RECOVERABLE-STATE.md §0 already states both licences: engenho only has to
honour the Kubernetes API facade, and "what a node hosts is inferred from what
it advertises at the moment, never from a hand-assigned name". This plan picks
the mechanisms.

## 1. Where it stands (measured 2026-10-08, HEAD 4e0c1aa)

| Area | As built | Evidence |
|---|---|---|
| Writes | every mutation is a `ResourceCommand` through one openraft log; controllers, scheduler and kubelet call `StoreMesh::propose` directly | `engenho-store/src/command.rs:64-185`, `mesh.rs:420-427` |
| Voters | always one: `boot_store` builds `InProcessRouter` and node 1 at `in-process://1`, `initialize_singleton` | `engenho-runtime/src/runtime.rs:1825-1846` |
| Ordering | one global revision per apply; per-key CAS decided inside that log; one catalog `BTreeMap` | `engenho-store/src/state.rs:369-382,573-623,71-76` |
| Reads | local catalog, no read index | `mesh.rs:429-434` |
| Watch | history ring of 8192 changes; 410 after restart; bookmarks every 5 s | `watch_history.rs:71`, `state.rs:484-503`, `watch_backend.rs:76` |
| Eventual tier | none in the binary; `ConsistencyTier` exists and nothing calls it; `consistency.default_tier` accepts only `Strong` | `engenho-types/src/consistency_tier.rs:38-63`, `boot_config.rs:309-321` |
| Transport | in-process routers only; NATS behind `teia-nats` (off) and unwired; `Fabric` has one arm, `InBinary` | `engenho-store/src/network.rs:37-161`, `engenho-config/src/fabric.rs:15-27` |
| Membership | no join, no peer keys; multi-master formations refused; a node that is not leader fails boot | `mesh.rs:385-413`, `boot_config.rs:276-293`, `runtime.rs:1038-1057` |
| revoada | chitchat gossip with a phi-accrual detector, a role-assignment Raft with a durable vote store, and a pure `PolicyEngine` (`AutoReplacementPolicy`, `FormationPolicy`), exercised only in its own in-process tests and linked by no shipped binary | `engenho-revoada/src/membership/mod.rs:42,79`, `src/policy/mod.rs:135,256`, `tests/r1_*`..`r4_*` |
| Kubelet | needs a local `StoreMesh`; runs only pods whose `spec.nodeName` is this host | `engenho-kubelet/src/kubelet.rs:840-862,2328-2333` |
| Scheduler | binds with an unconditional merge Patch of `spec.nodeName`, no CAS; `pods/binding` not served | `engenho-scheduler/src/scheduler.rs:159-166`, `router.rs:1876-1879` |
| Fleet | five standalone one-node clusters (plo, rio, zek on NixOS; ryn, cid on macOS); `cluster.name` per host | `engenho-config/src/cluster.rs:51-84` |
| banken | one context per screen, read through the kube API; per-kind absorbers (`Absorbing` / `Synced` / `Degraded`) | banken `src/absorb.rs:61-79`, `src/mcp.rs:321,359` |

## 2. The consistency change

FLEET-DESIGN.md §1 makes **Declared** objects strong through one replicated
resource log, which puts every write in the fleet behind one quorum: a laptop on
a train could not update its own pod status. This plan moves the unit of
serialization from the cluster to the **object**:

- **Every object has one owner**, a node. Only the owner orders writes to that
  object, and it checks `resourceVersion` there (compare-and-swap). One object's
  history is linearizable, as etcd promises.
- **Committed history replicates to every node eventually.** Each owner appends
  to its own log; peers pull what they lack (anti-entropy). Reads are served
  from the local replica.
- **A quorum is needed only for consensus metadata**: membership, the ownership
  map, leases and role leases, in one small openraft group whose voters are
  elected (§4).

Clients already read through informer caches, which are eventually consistent
by design. What they rely on is that a stale write fails, that one object's
events arrive in order, and that a resume point works or returns 410.

## 3. The promise table

| Kubernetes promise | How it is kept |
|---|---|
| A write with a stale `resourceVersion` fails with 409 | the write is forwarded to the object's owner, which runs the existing `check_precondition` |
| One object's versions are monotonic | the owner's log sequence; `resourceVersion` encodes owner, owner epoch and sequence |
| List and watch from a `resourceVersion` | a list's version is the serving node's replica position; resuming on the same node works, on another node returns 410, which clients handle by relisting |
| Read your own write | the node that forwarded a write applies the owner's result to its replica before replying |
| Create is unique by name | the owner of `namespace/name` decides `NotExists` (the existing `Txn{NotExists}`, run there) |
| `generateName` | the owner picks the suffix (not implemented today; `handler.rs:1243-1248` returns 400) |
| Leases and leader election (`coordination.k8s.io`) | served from the consensus group, linearizable |
| Exactly one node gets a pod | the scheduler binds with CAS through `pods/binding`, at the pod's owner |
| Finalizers and graceful delete | ordered at the owner (`state.rs:1026-1170`, run there) |
| A node's own state is always writable | a node owns its `Node`, its pods' status, its node `Lease` and its Events |

## 4. Autopilot: nothing is assigned by name

Taken from three systems that already run this in production:

| Source | What aldeia takes |
|---|---|
| **Raft** | leader election with PreVote and CheckQuorum; learners that replicate without voting; joint consensus for every membership change; leadership transfer (`TimeoutNow`) to a chosen voter |
| **Consul / Nomad Autopilot** | dead-server cleanup; promotion only after a stabilization time and within a trailing-log bound; non-voting standbys; redundancy zones (one voter per zone, a standby promoted when the zone's voter fails); upgrade migration (new-version servers promoted only when enough of them exist) |
| **Serf** (under Consul and Nomad) | SWIM gossip membership, which revoada's chitchat layer already is; graceful *leave* as an announced intent, distinct from failure |
| **Nomad** | control work (scheduling, evaluation) runs where a lease says, and placements are serialized through the leader's log so two schedulers cannot both win |

It lands as **`AutopilotPolicy`**, a new policy in revoada's existing
`PolicyEngine` (`engenho-revoada/src/policy/`), keeping that engine's
discipline: pure, idempotent, bounded per tick, every decision committed
through the consensus log with a typed reason.

**Inputs, all observed** (FLEET-DESIGN.md §5 capability records, gossiped with
an incarnation number and an HLC): liveness and phi from gossip; last contact
and trailing-log distance from Raft; uptime, power source (battery or mains) and
churn history (how often the node left in the last week); durable disk; the
networks it is reachable on, from which its **zone** derives (a home LAN, a
datacentre, a tailnet-only roamer); its engenho version.

**Every node joins as a learner.** It replicates and serves, and votes only
when promoted.

**Voter count is derived, not configured per node:** the largest odd number
not above `autopilot.max_voters` (default 5) that the eligible nodes allow.

**Eligibility to vote:** healthy in gossip and Raft for `stabilization`
(default 10 minutes), within `max_trailing_logs` (default 250), durable disk,
not protected.

**Choice among eligible nodes**, scored: spread across zones first (at most one
voter per zone while zones ≥ voters), then stability (mains power, long uptime,
low churn), then lowest last contact. A laptop is never excluded by name; it
loses on stability, and wins when it is the best node left.

**Changes are add-then-remove**, one at a time, by joint consensus:

- **Promote** the best-scored learner, then **demote** the voter it replaces.
- **Dead voter**: demoted after `dead_threshold` of failed gossip and Raft
  contact, never below the configuration's quorum.
- **Graceful leave**: a node about to sleep or shut down announces a leave
  (macOS: `IORegisterForSystemPower`'s `kIOMessageSystemWillSleep`, with an
  `IOPMAssertion` holding sleep off for the hand-off; Linux: a systemd-logind
  `PrepareForSleep` delay lock). If it leads, leadership transfers first; if it
  votes, it is demoted before it goes. Closing ryn's lid never costs quorum.

**Leadership preference:** when the leader's score drops below another
voter's by a margin (it went on battery, its churn rose), leadership transfers.

**Owners and role holders are elected the same way:**

- Shards of non-node objects are leased to nodes by score, with a fencing epoch.
- Control roles (scheduler, controllers, the gitops reconciler) are leased the
  same way, with FLEET-DESIGN.md §4's fencing epochs.
- Node-scoped objects always belong to their node.

**Damping:** at most one shift per node per `hysteresis` interval. Every move is
recorded as a typed `ShiftDecision` with its reason.

**Upgrade migration:** nodes on a newer engenho version are preferred as voters
only once at least as many of them are eligible as there are voters; old-version
voters are then demoted one at a time.

**Surfaces:** `engenho ctl autopilot state` (Consul's `operator autopilot state`)
and an MCP tool list every node's score, role, zone, version and the last
`ShiftDecision`; banken shows the same beside each node.

## 5. revoada changes, each locked by tests

revoada becomes the consensus group's home. What it needs, from the recon and
from its own open items (DISTRIBUTED.md:194, RECOVERABLE-STATE.md §6,
IMPROVEMENT-PLAN.md §5.3):

| Change | Why |
|---|---|
| `AutopilotPolicy` (§4), replacing `AutoReplacementPolicy`'s "any healthy member" choice with eligibility and scoring | today a replacement is the first healthy node without the role |
| Fencing epochs on every role and ownership grant | a paused old holder must be refused, not trusted |
| Quorum-seeded bootstrap | the first node forms `Solo`; no later node becomes a voter except through a committed decision |
| A network `RaftNetwork` on the shared in-binary transport (§8) | revoada's Raft is in-process only |
| RPC replies as `Result`; errors surfaced, never swallowed | edge 18; `engenho-store/src/mesh.rs:334-346` swallows them today |
| Graceful leave as a gossip state distinct from failure | Serf's leave intent |
| Zone, power source and churn history in the capability record | the autopilot's scoring inputs |
| Delete `RoundRobin` (plan T5.4); fix the chart's revoada DaemonSet, which runs a `revoadactl` no crate builds (`charts/engenho/templates/revoada-daemonset.yaml:49`) | dead and broken surfaces |
| Into the shipped binary, behind its own feature until P5 | today no shipped crate links it |

**Every essential quality gets a regression test that is shown red first** (the
house rule: a test that cannot fail locks nothing). New seams go on
`ci/seam-files.txt` so the mutation gate covers them.

| Quality | Unit (pure policy over built inputs) | Property | Integration (FaultRouter, then real transport) |
|---|---|---|---|
| Never demote below quorum | ✓ | random memberships: no proposal set leaves < quorum | kill nodes one by one: the cluster keeps committing until a majority is gone |
| Promote only after stabilization and within the trailing-log bound | ✓ | | a new node joins, is not promoted early, is promoted on time |
| A dead voter is replaced add-then-remove | ✓ | | kill a voter: replaced within the bound, quorum held throughout |
| Graceful leave costs no quorum | ✓ | | announce leave on the leader: leadership moves, then it is demoted |
| One voter per zone while zones allow | ✓ | random zone layouts | partition one zone: the rest keep quorum |
| Prefer stable nodes; a laptop is chosen only when best | ✓ | scoring is monotone in stability | |
| No oscillation | | flapping membership: shifts per node per interval ≤ 1 | |
| Voter count odd and ≤ `max_voters` | ✓ | every eligible count | |
| A stale-epoch holder is refused | ✓ | | pause a role holder past its lease, resume it: its writes are refused |
| No split brain | | | partition 2/3: the minority neither elects nor commits (edge 18's recorded red) |
| Bootstrap from zero | ✓ | | start five nodes in any order: one Solo, the rest learners, then a promoted voter set |
| Upgrade migration | ✓ | | mixed versions: new voters only once enough exist |
| Policy evaluation is pure and idempotent | | same inputs, same proposals, any number of runs | |
| Per-object linearizability | | | recorded histories under partitions checked per object |
| Replicas converge | | any delivery order of the same writes → the same replica | |
| No plaintext Secret on disk or on the wire | ✓ | random values: no stored or transported byte equals a plaintext | a scan of every node's fjall files and a capture of the transport find no plaintext |
| A node decrypts offline | ✓ | | partition a node, restart it: its pods still get their Secrets |
| Key rotation re-encrypts, and a removed node's key opens nothing new | ✓ | | remove a node, rotate: values written afterwards fail with its sealed key |
| KV check-and-set and blocking queries behave like Consul's | ✓ | | differential against a Consul binary as the oracle, as `engenho-diff` does against Kubernetes |

## 6. Partitions and sleeping laptops

| Situation | Reads | Writes to its own objects | Writes to objects owned elsewhere | Leases, ownership, membership |
|---|---|---|---|---|
| A laptop sleeps (announced) | its own replica, marked stale with its lag | continue; replicate on return | refused with a typed 503 naming the unreachable owner | unchanged: it left the voter set before sleeping, if it was in it |
| A node vanishes (unannounced) | every node | continue | objects it owned are re-leased after the lease ends, new epoch | it is demoted after `dead_threshold` if it voted; quorum kept |
| Voters lose their majority | every node | continue | continue where the owner is reachable | blocked, and reported as blocked, never guessed |

## 7. The built-in KV: configuration and Secrets, Consul-style

aldeia carries a key-value store the way Consul does, inside engenho: every
node holds all of it, reads are local, and it is redistributed to every node
by the same replication as everything else (§2). It is the global
configuration cache, and **Secrets live in it**.

| Consul KV behaviour | aldeia KV |
|---|---|
| hierarchical keys, prefix list and delete | the same, over the replicated store |
| check-and-set on `ModifyIndex` | a write carries the version it read; the key's owner checks it (§3) |
| blocking queries (`?index=&wait=`) | a watch on a key or prefix, served from the local replica |
| `?stale` reads (any server) and `?consistent` reads (leader) | local reads by default; `consistent` forwards to the key's owner |
| sessions and locks | leases from the consensus group (§4), fenced by epoch |
| ACL tokens per path | one policy model: Kubernetes RBAC on the Kubernetes face, the same roles mapped onto key prefixes on the KV face |

**One store, two faces.** ConfigMaps and Secrets are Kubernetes objects and
KV entries at once (`config/<namespace>/<name>`, `secret/<namespace>/<name>`);
the KV face adds keys that are not Kubernetes objects. Faces: the Kubernetes
API, a Consul-shaped HTTP API, `engenho ctl kv`, and MCP tools.

**Secrets, replicated everywhere and readable only where consumed:**

- Values are envelope-encrypted with the cluster data key before they are
  stored or sent; gossip never carries them (it carries replication
  high-water marks only), so FLEET-DESIGN.md §11's rule holds in spirit:
  ciphertext replicates, plaintext never travels.
- The data-key ring is committed through the consensus group and rotated like
  `consul keyring`; each node holds it sealed under its own identity in the OS
  keystore (the macOS Keychain, backed by the Secure Enclave where present; a
  TPM through systemd-creds on Linux), so a node decrypts offline.
- Plaintext exists only in the consuming process: the kubelet decrypts into a
  tmpfs volume for the pod, as kubelet secret volumes do.
- A node removed for good triggers a key rotation and a re-encryption pass, so
  its sealed copy of the old key opens nothing written after it left.
- Where a Secret's source of truth is external (Akeyless, SOPS), cofre writes
  it into the KV and refreshes it on rotation; the KV is the distribution
  cache. A Secret authored in-cluster has the KV as its source and follows
  RECOVERABLE-STATE.md's identity-and-secrets class for its way back. P5
  updates engenho's CLAUDE.md cofre rows to this shape when it lands.

## 8. Pieces, and where each comes from

| Piece | Build or reuse |
|---|---|
| Consensus group and autopilot | revoada's role-assignment Raft and `PolicyEngine`, with §5's changes |
| Per-owner logs | the existing fjall log and apply path, one log per owner instead of one per cluster |
| Ordering stamps | the HLC in `engenho-substrate-core/src/relogio.rs` |
| Membership and anti-entropy | revoada's chitchat gossip; per-owner high-water marks gossiped, missing ranges pulled; a new node takes a catalog snapshot from any peer |
| Transport, inside the binary (doctrine 5.1) | QUIC with rustls; ed25519 node identities and SPKI pins (FLEET-DESIGN.md §11); bound to the tailnet address; one transport for Raft RPC, log pulls, forwarded writes and kubelet proxying |
| Discovery | seeds are a starting point, never a role: tailnet peers first, then local discovery, then the last-known peers on disk (FLEET-DESIGN.md §4 bootstrap) |
| Kubelet on every node | unchanged in shape: it reads its local replica and runs its own pods |
| Faces | the apiserver on every node serves the whole cluster; one kubeconfig context, `aldeia`, names every node |

## 9. Build order

Edge 18 gates every step that adds a second node.

| Step | What | Done when |
|---|---|---|
| P0 | Doctrine: FLEET-DESIGN.md §1's Declared row and §4's bootstrap, CONSISTENCY-FABRIC.md's tier table; correct the stale lines the recon found (CONSISTENCY-FABRIC.md:86-88, 241-245, 300-304; IMPROVEMENT-PLAN.md:47, 54, 56, 126, 320) | docs agree with each other and with the code |
| P1 | One node, new model, no behaviour change: the store splits into a consensus group (one voter) and a per-owner log (owner = self); `resourceVersion` encodes owner and sequence; `StoreMesh` takes any `RaftNetwork`; RPC replies become `Result` | every existing test passes; `engenho-diff` shows no new divergence |
| P2 | revoada §5 in pure form: `AutopilotPolicy`, fencing epochs, leave intent, scoring inputs, with every unit and property test of §5 red first | the §5 unit and property rows green; mutation gate clean on the new seams |
| P3 | Transport and identity: QUIC, the FaultRouter, the shared `aldeia` cluster name, stable node ids, discovery, read fences | the FaultRouter partition test recorded red, then green (edge 18) |
| P4 | Many nodes: anti-entropy, forwarded writes, owner leases, join as a learner, the autopilot live | §5's integration rows green over the FaultRouter, then over real sockets |
| P5 | The KV (§7): the Consul-shaped face, envelope encryption for Secrets, the keyring through the consensus group, sealing in the OS keystore, cofre writing external Secrets in | the §7 rows of the test matrix green |
| P6 | Workloads across nodes: `pods/binding` with CAS, role leases for scheduler and controllers, `generateName`, authenticated kubelet and etcd listeners (edge 26) | a pod scheduled anywhere runs on its node, and its logs read from any other |
| P7 | The fleet: today's five clusters join aldeia one at a time (§10.2), rendered by the nix modules with no voter list anywhere | closing a laptop's lid costs nothing §6 says continues; the autopilot state shows who votes and why |
| P8 | Faces: banken shows aldeia as one context with each node's lag and autopilot role; revoada's MCP tools | banken never blocks on a slow or sleeping node |

## 10. Decisions (2026-10-08)

1. **Secrets live in the built-in KV**, replicated to every node (operator
   decision). §7 is the design: envelope-encrypted everywhere, decrypted only in
   the consuming process, the keyring sealed per node, cofre bridging external
   sources.

2. **Migration: a rolling join, one node at a time, with an explicit import of
   what git does not hold.** Merging the five stores wholesale is unsafe: each
   carries its own `default/kubernetes` Service, namespaces and system objects
   under identical names. So for each node:
   1. export what no GitOps source declares (in-cluster Secrets, objects with no
      git owner, CR status that cannot be re-observed) as a typed snapshot;
   2. join aldeia as a learner and let GitOps re-declare everything git owns;
   3. import the snapshot; a name that already exists is refused and listed,
      never overwritten;
   4. workload volumes stay on the node's disk as local PersistentVolumes bound
      to that node, so a stateful pod (ryn's Postgres) comes back on the same
      disk;
   5. switch the node's kubeconfig to the `aldeia` context; keep the old store
      read-only until the node has run a day in aldeia.

   Order, least state first and the control plane last: rio, cid, zek, plo,
   ryn. The first node forms aldeia `Solo`; the autopilot moves the voters as
   better nodes arrive. Every node in the fleet ends in aldeia, and so does
   every new machine.

3. **Autopilot and Raft defaults**, as typed config, re-measured in P4 over the
   FaultRouter and in P7 on the fleet. The fleet spans intercontinental round
   trips (rio in Bristol, the rest in Brazil), so Consul's datacentre defaults
   would mark rio unhealthy.

   | Setting | Default | Why |
   |---|---|---|
   | `max_voters` | 5 | Consul's ceiling; derived down to 3 when fewer nodes are eligible |
   | `stabilization` | 10 min | a laptop that just woke is not promoted on its first minutes |
   | `max_trailing_logs` | 250 | Consul's default |
   | `last_contact_threshold` | 1 s | above the Bristol–Brazil round trip with margin |
   | Raft heartbeat / election timeout | 500 ms / 3–6 s | the same round trip; openraft's LAN defaults would elect on every jitter |
   | `dead_threshold` | 3 min | an unannounced loss outlives a tailnet blip before a voter is replaced; an announced leave is demoted at once |
   | `hysteresis` | one shift per node per 10 min | no flapping |
   | leadership-transfer margin | the leader's score 20% below the best voter's | moves leadership off a node that went on battery, not on noise |

## 11. What stays only mitigated

- Objects owned by different nodes are not ordered against each other. A client
  can see a later change to one before an earlier change to another; informer
  controllers tolerate this, a client comparing two objects' versions does not
  get a global order.
- A list is a snapshot of one node's replica, not of the cluster.
- Staleness on a sleeping node is unbounded while it sleeps; it is reported,
  never hidden.
- Lease expiry depends on bounded clock drift for liveness, never for safety
  (FLEET-DESIGN.md §4).
