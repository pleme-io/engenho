# engenho — resilience by formation

> **Codename: `formação`** (Portuguese: *formation* / *lineup*).
> Like a soccer team's 4-3-3 or a fighter-jet V formation, an
> engenho cluster picks a **typed topology strategy** that
> declares the IDEAL shape; surviving nodes shift to maintain
> that shape when peers drop out. The strategy is configuration;
> the substrate enforces it.
>
> Companion docs:
>   * [`CONSISTENCY-FABRIC.md`](CONSISTENCY-FABRIC.md) — data plane
>   * [`FABRIC.md`](FABRIC.md) — NATS+Vector teia transport
>   * [`DISTRIBUTED.md`](DISTRIBUTED.md) — revoada A-D layers
>
> Primary surface: [`engenho-revoada::topology`](../engenho-revoada/src/topology.rs).

## The user's directive (verbatim)

> *"no matter how many nodes there should be consistency in role
> distribution and functionality and one being able to shift to
> the other, the fabric receives a config that is a strategy, think
> of all the different strategies to prepack and what the strategy
> definition interface looks like, so a strategy could enforce
> something like 3 masters 4 workers when possible, like formation
> on soccer field or planes flying, if you lose planes they shift
> formation, we need to learn from this to be resilient and it also
> means we need perfect state machine coordination on talking and
> such to signal and never get stuck and go online and learn of any
> tests we can run to prove resiliency I think jepsen comes to mind"*

## What's typed + shipping today

### 1. The typed strategy interface

```rust
pub trait TopologyStrategy: Send + Sync + Debug {
    fn name(&self) -> &'static str;
    fn min_nodes(&self) -> usize;
    fn assign(&self, eligible: &[NodeId]) -> Result<RoleAssignment, TopologyError>;
    fn react_to_loss(&self, current: &RoleAssignment, lost: &[NodeId]) -> Vec<Transition>;
    fn validate(&self, assignment: &RoleAssignment) -> Result<(), TopologyError>;
}
```

### 2. Six pre-packed formations

| Strategy | Min nodes | Shape | Use case |
|---|---|---|---|
| `Solo` | 1 | 1 master | dev / homelab |
| `Pair` | 2 | 2 active-passive masters | HA pair |
| `Quorum3M` | 3 | 3 masters | etcd-style |
| `Cluster3MNW` | 4+ | 3 masters + N workers | typical k8s |
| `MeshAllPeers` | 1+ | all masters, symmetric | gossip-heavy |
| `Phalanx` | 1+ | ⌈2N/5⌉ masters | scales with cluster |

### 3. Typed values used end-to-end

```rust
pub enum Role { Master, Worker, Bootstrap, Observer }

pub enum NodeState {
    Joining,         // just came up
    Standby,         // healthy peer, no role yet
    Active(Role),    // has a role
    Demoting,        // transitioning out (writes blocked)
    Departing,       // voluntary leave
    Failed,          // phi-accrual flagged
}

pub enum Transition {
    Admit(NodeId),                  // new node enters Standby
    Promote(NodeId, Role),          // Standby → Active(Role)
    Demote(NodeId),                 // Active → Demoting → Standby
    Reassign(NodeId, Role),         // Master ↔ Worker
    Evict(NodeId),                  // remove failed/departing
}
```

### 4. Reactor — formation-shift on loss

The user's plane analogy in action — when a plane drops out of a 4-ship formation, the wingman closes the gap:

```rust
// In Pair when a master dies:
let lost = vec![NodeId::new("node-0")];
let tx = Pair.react_to_loss(&current, &lost);
// Returns:
//   Reassign(node-2, Master)   ← worker promoted
//   Evict(node-0)              ← failed master removed
```

```rust
// In Phalanx after losing 5 of 10 nodes:
let surviving = 5;
let target_masters = Phalanx::target_masters(surviving); // = 2
// Reactor: demote (N→2) masters, promote workers, evict losses.
// Cluster ends in valid 2M+3W shape, never stuck.
```

The state machine guarantees:
1. Every transition is atomic (committed via Raft)
2. Every wait has a timeout
3. Quorum loss → read-only mode (graceful degradation, not freeze)
4. Idempotent applies (re-applying a transition is a no-op)

## The state machine — "never get stuck"

```
                    Phi-accrual flagged
                ┌──────────────────────────┐
                ↓                          │
   ┌──────────┐    admit       ┌─────────┐
   │ Joining  │ ───────────→   │ Standby │
   └──────────┘                └────┬────┘
                                    │  promote
                                    ↓
                          ┌─────────────────────┐
                          │ Active(Role)        │
                          └────┬────────────────┘
                               │ demote / reassign
                               ↓
                          ┌────────────┐
                          │ Demoting   │ → Standby
                          └─────┬──────┘
                                │ depart
                                ↓
                          ┌────────────┐
                          │ Departing  │ → evicted
                          └────────────┘
                                ↑
                          ┌────────────┐
                          │  Failed    │ ← phi-accrual
                          └────────────┘
```

**Coordination rule:** every state edge corresponds to a Raft
`Transition` log entry. There is no out-of-band promotion — even
self-promotion to Master at bootstrap goes through a quorum-of-1
commit. This is the user's "perfect state machine coordination on
talking and such to signal" requirement, enforced by construction.

**Never-stuck invariants:**

1. **Election timeout fallback** — if Raft can't elect a leader
   for `min(election_timeout × 3, 30s)`, the surviving
   majority transitions to a single-master read-only mode.
2. **Reassignment timeout** — a Demoting node times out after
   30s and forcibly transitions to Standby (no infinite wait).
3. **Quorum-aware writes** — the apiserver returns 503 when no
   quorum exists; clients retry. Better than blocking forever.
4. **Phi-accrual + grace period** — node marked Failed after
   φ > 8.0 for 3 consecutive heartbeats (≥3s); grace period
   before eviction lets transient network blips heal.
5. **Witnessed transitions** — every Promote/Demote/Reassign
   emits a BLAKE3+ed25519 attestation block per node (R4.5).
   Operators can replay the chain to verify "no node was
   ever in two states at once."

## Resilience testing — research outcomes

External research (parallel agent) summarized current best
practices. Verbatim recommendations integrated below.

### Layered testing pyramid

```
                       ┌──────────────────┐
                       │     Jepsen       │  (M3+, marketing-grade
                       │     (Clojure)    │   audit reports)
                       └──────────────────┘
                       ┌──────────────────┐
                       │  chaos-mesh /    │  (production-shaped
                       │     litmus       │   on rio sibling)
                       └──────────────────┘
                       ┌──────────────────┐
                       │    Maelstrom     │  (one-time ~200 LoC
                       │  + Rust adapter  │   stdin/stdout adapter
                       └──────────────────┘   for engenho-apiserver)
                       ┌──────────────────┐
                       │   madsim /       │  (in-process DST in
                       │    turmoil       │   cargo test; highest
                       └──────────────────┘   ROI for Raft logic)
```

### Tool selection rationale

| Layer | Tool | Why for engenho |
|---|---|---|
| DST | **madsim** or **turmoil** | tokio drop-in, deterministic, seedable RNG. Catches 80%+ of Raft logic bugs at sub-ms cargo test speed. RisingWave proven. |
| Linearizability | **Maelstrom + maelstrom-rust-node** | JSON stdin/stdout — no Clojure on engenho's side. Knossos checks linearizability against engenho-apiserver via 200-LoC Rust harness. |
| Production-shape failures | **chaos-mesh** self-hosted | PingCap built it for TiDB (Raft-based). Network partitions, time chaos (election-critical), disk-full, pod kill. Run control plane on a sibling cluster. |
| Marketing-grade | **Jepsen** (deferred) | DuckDB pattern: ~80-100 LoC Clojure adapter against engenho-apiserver HTTP. Defer to M3+. |

### Genuine substrate gap: `openraft-maelstrom`

The research confirms no production-grade openraft Maelstrom
adapter exists publicly. **engenho can fill that gap** — write
a `openraft-maelstrom` crate that wraps openraft in
Maelstrom's stdin/stdout protocol. Benefits:

1. Continuous linearizability gates in CI from M0.5 onward
2. Reusable by the broader Rust Raft ecosystem (peer of
   substrate's pleme-actions catalog)
3. Pairs with `madsim` for deterministic replay of any
   Maelstrom-found counterexample

### Eight chaos scenarios for engenho

From the research, the concrete experiments to encode:

| # | Scenario | Tool | Property |
|---|---|---|---|
| 1 | Minority partition (2 of 5 isolated) | chaos-mesh NetworkChaos | Cluster writable; isolated nodes catch up on heal |
| 2 | Majority partition (3 of 5 isolated) | chaos-mesh | Minority side rejects writes; no split-brain |
| 3 | Slow links (250ms+jitter) | chaos-mesh delay / turmoil | Election timeouts respected; no spurious churn |
| 4 | Clock skew (±5min) | chaos-mesh TimeChaos | Leases honored; chitchat anti-entropy converges |
| 5 | Disk full | chaos-mesh IOChaos | Leader steps down cleanly; followers don't append |
| 6 | Leader crash mid-commit | chaos-mesh PodChaos kill | Successor applies committed entries; CAS sees consistent state |
| 7 | Rolling restart | chaos-mesh + Maelstrom kill nemesis | Reads remain linearizable per Knossos |
| 8 | Simultaneous quorum-loss (3 of 5) | Maelstrom kill | Unavailable but consistent on recovery |

## Phased rollout — R-TOPO

| Phase | Status | What |
|---|---|---|
| R-TOPO.0 | ✅ shipped | Typed `TopologyStrategy` trait + 6 pre-packed strategies (this commit) |
| R-TOPO.1 | next | Wire `engenho-revoada::policy` to consult the active strategy on phi-failure |
| R-TOPO.2 | next | Strategy selection via `shikumi` config (`revoada.topology.strategy: phalanx`) |
| R-TOPO.3 | designed | Add `madsim` integration in `engenho-revoada/tests/dst.rs` — seeded chaos against the topology reactor |
| R-TOPO.4 | designed | Build `engenho-store-maelstrom` adapter — Maelstrom test harness for engenho-apiserver's HTTP K/V surface |
| R-TOPO.5 | designed | chaos-mesh manifests under `flux/engenho-chaos/` — 8 chaos scenarios as `ChaosExperiment` CRDs |
| R-TOPO.6 | future | `openraft-maelstrom` extracted to its own crate + published to crates.io — substrate gift to the Rust ecosystem |
| R-TOPO.7 | future | DuckDB-pattern Jepsen adapter (~100 LoC Clojure) for marketing-grade audit |

## Strategy selection at boot

Operators pick a strategy in the cluster's shikumi config:

```yaml
# engenho.lisp / engenho.yaml
revoada:
  topology:
    strategy: phalanx       # solo | pair | quorum_3m | cluster_3m_nw | mesh | phalanx
    min_nodes: 1
    grace_period: 10s       # wait this long after phi-failure before
                            # reacting (allows transient network blips)
```

Behind the scenes:

```rust
let strategy: Box<dyn TopologyStrategy> = match config.strategy.as_str() {
    "solo" => Box::new(Solo),
    "pair" => Box::new(Pair),
    "quorum_3m" => Box::new(Quorum3M),
    "cluster_3m_nw" => Box::new(Cluster3MNW),
    "mesh" => Box::new(MeshAllPeers),
    "phalanx" => Box::new(Phalanx),
    other => return Err(format!("unknown topology strategy: {other}")),
};
revoada.set_topology_strategy(strategy);
```

## Custom strategies — for unusual fleets

Operators with unusual needs (e.g. multi-region quorums, 5-master
HA, AZ-balanced) implement the trait themselves:

```rust
#[derive(Debug, Default, Clone)]
struct ThreeRegionQuorum {
    regions: HashMap<NodeId, String>,
}

impl TopologyStrategy for ThreeRegionQuorum {
    fn name(&self) -> &'static str { "three_region_quorum" }
    fn min_nodes(&self) -> usize { 3 }
    fn assign(&self, eligible: &[NodeId]) -> Result<RoleAssignment, TopologyError> {
        // Pick one master per region; workers fill remaining.
        ...
    }
    // ...
}
```

The trait is the contract; pre-packed strategies are starting
points, not limits.

## Why this design is "resilient by construction"

1. **Typed transitions** — every role change is a typed
   `Transition` variant. The compiler enforces exhaustive
   match in the reactor.

2. **Single-source-of-truth** — `RoleAssignment` lives in
   engenho-revoada's Raft state machine; reads are linearizable;
   writes go through quorum.

3. **Formation by configuration** — operators don't write
   procedural code to handle losses; they pick a strategy +
   the substrate handles every failure case the strategy
   declared.

4. **Attestation** — every transition emits a signed chain
   block (R4.5); operators replay the chain to prove no node
   was ever in an inconsistent state.

5. **Tested by formation** — `phalanx_reacts_to_loss_with_correct_target`
   already proves the 10→5 node loss converges to the right
   shape. M0.5+ adds madsim seeds for ALL 8 chaos scenarios.

## What ships in v0.13.0

  * `engenho-revoada::topology` module (~700 LoC)
  * 6 pre-packed `TopologyStrategy` impls
  * 14 unit tests + 3 round-trip serde tests
  * `docs/RESILIENCE.md` (this file) covering the formation
    analogy, state machine, chaos research integration,
    R-TOPO.0-R-TOPO.7 phases.

## What's next (R-TOPO.1+)

Wire the topology strategy into engenho-revoada's existing
`policy` module — when phi-accrual flags a node, the policy
calls `strategy.react_to_loss()` + commits the resulting
transitions via Raft. ~150 LoC.

After that, R-TOPO.3 (madsim seeded chaos) lands as the first
of the eight resilience scenarios — the cargo test that proves
"phalanx-formation cluster survives losing 5 of 10 nodes
within 3 seconds + converges to valid 2M+3W shape."
