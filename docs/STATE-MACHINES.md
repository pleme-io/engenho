# engenho — the state-machine catalog

> Every finite-state machine in the engenho runtime, in one place:
> **states · events · transitions · never-stuck invariants · source ·
> maturity**. "Leave no territory unknown."
>
> The canonical FSM substrate is
> [`engenho-substrate::maquina::StateMachine`](../engenho-substrate/src/maquina.rs)
> — a pure `step(&state, &event) -> Result<(state, effect), err>` with a
> `MachineRunner` that records a typed `TransitionRecord` history and is
> itself `mirante::Observable`. The **formalization target**: every SM
> below lifts into a `StateMachine` impl (see
> [`engenho-machines`](../engenho-machines/) — substrate-first). The one
> already-perfect exemplar (exhaustive proptest: deterministic,
> all-states-reachable, no spurious self-loops) is **SM ①, kikai**.
>
> Companions: [`STRATEGY.md`](STRATEGY.md) · [`TYPESCAPE.md`](TYPESCAPE.md)
> · [`RESILIENCE.md`](RESILIENCE.md) · [`DISTRIBUTED.md`](DISTRIBUTED.md).

## Index

| # | Machine | Layer | Source | Maturity |
|---|---|---|---|---|
| ① | Cluster lifecycle (kikai) | lifecycle backend | `kikai/src/state.rs` | ✅ exhaustively tested |
| ② | Node membership (phi-accrual) | revoada A | `engenho-revoada/src/membership/` | ✅ shipped |
| ③ | Topology / formation node-state | revoada policy | `engenho-revoada/src/topology.rs` | ✅ typed (reactor; not yet a `maquina` FSM) |
| ④ | Raft consensus (per group) | revoada B / store | `engenho-revoada/src/consensus/`, `engenho-store/` | ✅ openraft (in-process transport) |
| ⑤ | Store write path | store | `engenho-store/src/{mesh,state,watch}.rs` | ✅ in-memory; disk pending C4 |
| ⑥ | Derivation / materialization | substrate | `engenho-substrate/src/{derivation,shape,quorum,roca}.rs` | ✅ primitives; lifecycle FSM in `engenho-machines` |
| ⑦ | Pod lifecycle (kubelet) | control | `engenho-kubelet/src/kubelet.rs` | 🟡 start path; stop/probes pending |
| ⑧ | Scheduler placement | control | `engenho-scheduler/src/scheduler.rs` | 🟡 RoundRobin |
| ⑨ | Controller reconcile | control | `engenho-controllers/src/controller.rs` | ✅ generic trait + core set |
| ⑩ | Fonte convergence (7-beat) | source-of-truth | `engenho-fonte/src/lib.rs` | 🔴 mock-universe; real M1.x |
| ⑪ | Face lifecycle | revoada | `engenho-revoada/src/face.rs` | ✅ start/stop; verbs per impl |
| ⑫ | MCP reader → writer | operator | `engenho-mcp/src/{reader,writer}/` | reader ✅ · writer P2 (saguão) |

---

## ① Cluster lifecycle — kikai (`kikai/src/state.rs`)

The exemplar. 14 states, 17 events, **29 valid transitions**, every
`(state,event)` pair deterministic, every state reachable from
`Uninitialized`, no self-loops except the two monitoring ones — all
proven by proptest.

**States:** `Uninitialized · Initialized · DisksReady · WaitingForApi ·
WaitingForNode · WaitingForFlux · Healthy · Degraded · BlockedDeclarative
· Paused · ShuttingDown · Stopped · SavingSnapshot · RestoringSnapshot ·
Destroyed`.

**Events:** `SecretsGenerated · DisksCreated · VmLaunched · ApiHealthy ·
NodeReady · FluxReady · HealthCheckPassed · HealthCheckFailed ·
ShutdownRequested · VmExited · PauseRequested · ResumeRequested ·
SnapshotRequested · SnapshotComplete · RestoreRequested · RestoreComplete
· DataRemoved`.

```
Uninitialized ─SecretsGenerated▶ Initialized ─DisksCreated▶ DisksReady ─VmLaunched▶
WaitingForApi ─ApiHealthy▶ WaitingForNode ─NodeReady▶ WaitingForFlux ─FluxReady▶ Healthy
   Healthy  ⇄ (HealthCheckPassed/Failed) ⇄  Degraded
   (Healthy|Degraded) ─PauseRequested▶ Paused ─ResumeRequested▶ WaitingForApi
   (running|Paused) ─ShutdownRequested▶ ShuttingDown ─VmExited▶ Stopped ─DataRemoved▶ Destroyed
   Paused ─SnapshotRequested▶ SavingSnapshot ─SnapshotComplete▶ Paused
   Stopped ─RestoreRequested▶ RestoringSnapshot ─RestoreComplete▶ Paused
   Stopped ─{VmLaunched|SecretsGenerated|DisksCreated}▶ (restart paths)   Destroyed ─SecretsGenerated▶ Initialized
```

**Predicates:** `is_operational` ⇔ {Healthy, Degraded}; `vm_expected_running`;
`is_initialized`; `is_terminal` ⇔ {Stopped, Destroyed, Uninitialized}.
**Distinct terminal:** `BlockedDeclarative` = bring-up blocked by a broken
declaration (no retry fixes it; needs operator action) — separate from
`Degraded` ("running but health failing — keep watching").

---

## ② Node membership — phi-accrual (`engenho-revoada/src/membership/`)

Eventually-consistent (chitchat/scuttlebutt). Feeds ④'s "who's alive";
never the source of truth for roles.

**States:** `Missing → Healthy → Suspect → Dead → Removed` (recover:
`Suspect → Healthy`). **Triggers:** gossip discovery; φ > `phi_threshold`
(default 8.0) ⇒ Suspect; responses resume ⇒ Healthy; grace period
(`marked_for_deletion_grace_period`, default 30s) ⇒ Dead → Removed.
Per-node gossiped state: `NodeState{node_id, gossip_addr, roles,
capacity, k8s_version, uptime_sec, membership_generation}` (generation
counter rejects stale gossip).

---

## ③ Topology / formation node-state (`engenho-revoada/src/topology.rs`)

The "lose-planes-shift-formation" machine. Typed; driven by a
`TopologyReactor` (pure, Raft-decoupled) rather than a `maquina` FSM
today — **the prime candidate to formalize** in `engenho-machines`.

**States (`NodeState`):** `Joining · Standby · Active(Role) · Demoting ·
Departing · Failed`, where `Role = Master | Worker | Bootstrap | Observer`.
**Transitions (`Transition`):** `Admit(id) · Promote(id,role) · Demote(id)
· Reassign(id,role) · Evict(id)`.

```
            phi-accrual flags ↘
 Joining ─admit▶ Standby ─promote▶ Active(Role) ─demote/reassign▶ Demoting ─▶ Standby
                                          │                          │
                                          └──── depart ───▶ Departing ─▶ evicted
                                                  Failed ◀─ phi-accrual ─┘
```

Every edge is a Raft `Transition` log entry — **no out-of-band
promotion**, even self-promotion at bootstrap goes through a
quorum-of-1 commit. Six pre-packed strategies (`Solo · Pair · Quorum3M ·
Cluster3MNW · MeshAllPeers · Phalanx`) each define `assign` (ideal shape),
`react_to_loss` (the shift), and `validate` (the invariant). Never-stuck:
below `min_nodes` the cluster goes read-only, never freezes; every
strategy provably yields ≥1 voter.

---

## ④ Raft consensus — per group (`engenho-revoada/src/consensus/`, `engenho-store/`)

Two independent openraft groups in one mesh: **Revoada** (commands =
`RoleAssignment` → `MeshShape` state machine) and **Store** (commands =
`ResourceCommand` → `ResourceCatalog`). Standard Raft:

**States:** `Follower → Candidate → Leader` (+ step-down on higher term).
**Triggers:** election timeout (F→C), majority votes (C→L), AppendEntries/
heartbeat, InstallSnapshot, higher-term observation (→F). `DefaultConfig`:
heartbeat 250 ms, election timeout 500–1000 ms. Transport is in-process
today; `engenho-teia::RaftTransport` over NATS subjects swaps in at C3/F2.

---

## ⑤ Store write path (`engenho-store/src/{mesh,state,watch}.rs`)

Not a node FSM but the canonical **write pipeline**:

```
client propose(cmd) ─▶ leader append to log ─▶ replicate (AppendEntries) ─▶
commit (quorum ack) ─▶ state-machine apply ─▶ resourceVersion = log index ─▶
WatchEvent{Added|Modified|Deleted} fan-out (broadcast; JetStream at F3)
```

Reads are synchronous local lookups (eventual on followers, bounded by
heartbeat). Idempotent deletes; patch-on-missing errors. Durability:
in-memory state machine now; `DurableInMemoryStore` + `PersistentLog`
exist behind the API, wired at C4 (snapshot every 10k entries).

---

## ⑥ Derivation / materialization — the substrate→ether path (`engenho-substrate/`)

The "shift the right bits to the right form across the ether" machine.
Composed from real types: `Drv`/`DrvHash`/`Realisation`
([`derivation.rs`](../engenho-substrate/src/derivation.rs)),
`WorkloadShape` ([`shape.rs`](../engenho-substrate/src/shape.rs):
`OciImage · NixClosure · Qcow2 · Wasm · StaticBinary{triple} · HelmChart ·
Custom{name}`), `MaterializationReceipt` + `QuorumOutcome`
([`quorum.rs`](../engenho-substrate/src/quorum.rs): `Pending · Reached ·
Dissent`), and the `roca` materialization-job staging. Formalized as a
`maquina::StateMachine` in [`engenho-machines`](../engenho-machines/).

```
Defined ─hash▶ Hashed ─build▶ Built ─nar▶ Realised ─put_realisation▶ StoreCommitted
  ─emit▶ ReceiptEmitted ─ingest▶ QuorumPending
       ├─(K confirmed, 1 evidence variant)▶ QuorumReached ─render(shape)▶ Rendered
       │        ─ledger.broadcast/gossip▶ Distributed ─▶ Terminal(available)
       └─(K confirmed, >1 evidence variant)▶ QuorumDissent   ← hard fault (re-derive / evict)
```

**Content-addressing:** BLAKE3 over canonical drv ATerm (`DrvHash`),
over NAR bytes (`NarHash`), over rendered artifact bytes (`evidence_hash`).
**Closures:** `input_drvs: BTreeMap<DrvHash, Vec<String>>`. **Trust:**
two nodes rendering the same `Drv` must produce the same `evidence_hash`
(verified in `oci_renderer` tests); disagreement ⇒ `Dissent`. The
`OciImageRenderer` is the concrete bridge (Drv → `docker-archive:` →
skopeo copy → `oci-archive:` → registry-servable bytes).

---

## ⑦ Pod lifecycle — kubelet (`engenho-kubelet/src/kubelet.rs`)

**States:** `Unscheduled → Scheduled → PendingMaterialization →
Materializing → Running → Terminating → Terminated`, with
`MaterializationFailed → (retry) PendingMaterialization`. Driven by the
`ContainerRuntime` trait (`start/status/stop/remove`); `FakeBackend`
today, youki/containerd/runwasi/cloud-hypervisor planned. On `Running`,
kubelet patches `status.phase=Running` + `Ready=True` + `podIP`, which
④/⑨ observe (EndpointsController). **Maturity:** start path implemented;
`stop`/probes/init-containers/volumes/graceful-termination pending.

---

## ⑧ Scheduler placement (`engenho-scheduler/src/scheduler.rs`)

**States:** `Unscheduled → PlacementDecision → {Placeable → Scheduled |
Unplaceable → (retry)}`. `SchedulingStrategy::pick(pod, candidates) →
Option<node>`; schedulable ⇔ not-cordoned ∧ Ready (optimistic for nodes
with no status). On Placeable: patch `spec.nodeName` (`Reason::Scheduler`);
kubelet ⑦ on that node takes over. Today: `RoundRobinStrategy` (rotating
cursor). Planned: BestFit/Priority/affinity/taint filters.

---

## ⑨ Controller reconcile (`engenho-controllers/src/controller.rs`)

Generic `Controller::tick() -> ReconcileReport{examined, changed,
skipped, note}`. **Runner states:** `Idle → Ticking → {Succeeded |
TransientErr → Backoff → Idle | Failed → (operator)}`. **Per-resource
within a tick:** `Discovered → Filter → {Skip | Decide → Act(propose) →
Changed | AlreadyDone}`. Per-controller intervals so a slow controller
never blocks a fast one. The 18 controllers (deployment, replicaset,
endpoints, hpa, ingress, job, dns, network_policy, gc, admission,
attestation, crd, owner, event_driven, + drv-build) each instantiate
this loop.

---

## ⑩ Fonte convergence — the 7-beat Viggy tick (`engenho-fonte/src/lib.rs`)

The live source-of-truth reconciler: a `(defsistema …)` declaration
drives the cluster to *be* what it says, via five typed roles wired
under a `Conduit` supervisor.

```
Observe ─▶ Diff ─▶ Classify ─▶ Decide ─▶ Act ─▶ Attest ─▶ Tick(Publish)
Watcher          (internal)            Evaluator Proposer Attester Publisher
shikumi                                sui       revoada  tameshi  mirante
ChangeEvent                            Decision  ProposalId Receipt Snapshot
```

`Change → Decision → Outcome` are the typed events. `Sistema{apps, infra,
topology, promessas}`; `PromessaKind = Compliance | CostBudget |
CustomerKpi | Sla | Security`. **Maturity:** mock-universe is the
always-on default; real role impls are feature-gated M1.1–M1.5
(`with-shikumi/-sui-eval/-revoada/-tameshi/-mirante`). `ProvacaoConduit`
wraps the conduit for deterministic fault injection.

---

## ⑪ Face lifecycle (`engenho-revoada/src/face.rs`)

**States:** `Stopped → Running → Stopped` (`start` / `shutdown` /
`is_running`). Resource verbs (`apply/get/list/delete_resource`) default
to `Unsupported` until an impl provides them. In-tree: `PureRaftFace`,
`KubernetesFace`; planned: `NomadFace`, systemd, bare-metal supervisor.
Snapshot/restore for hot-swap (`FaceSnapshot`).

---

## ⑫ MCP operator surface (`engenho-mcp/`)

**Phase gate, not a runtime FSM:** `Reader (live)` → `Writer (P2)`. Reader
tools — `cluster_status · cluster_config · cluster_kubeconfig ·
cluster_snapshot_meta · cluster_pods · cluster_resource_list ·
cluster_resource_get` (13-kind catalog, Secrets redacted at the
boundary). Writer trait is scaffolded but MCP-exposure is **gated on
saguão passport authority** at P2 — no mutation/attestation in the
current version.

---

## Never-stuck invariants (cross-cutting, from RESILIENCE.md)

1. **Election-timeout fallback** — no leader for `min(election_timeout×3,
   30s)` ⇒ surviving majority goes single-master read-only.
2. **Reassignment timeout** — a `Demoting` node forcibly → `Standby`
   after 30 s (no infinite wait).
3. **Quorum-aware writes** — apiserver returns 503 when no quorum;
   clients retry. Better than blocking forever.
4. **Phi + grace** — `Failed` only after φ>8.0 for ≥3 heartbeats; grace
   period lets transient blips heal.
5. **Witnessed transitions** — every Promote/Demote/Reassign emits a
   BLAKE3+ed25519 attestation block; replay proves no node was ever in
   two states at once.

**Testing pyramid** (SM correctness): madsim/turmoil (DST, in cargo
test) → Maelstrom+Knossos (linearizability) → chaos-mesh (production
shape) → Jepsen (audit). Eight chaos scenarios enumerated in
[`RESILIENCE.md`](RESILIENCE.md).
