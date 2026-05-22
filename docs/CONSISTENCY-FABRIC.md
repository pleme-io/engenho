# engenho — eventually-consistent data fabric

> **Codename: `correnteza`** (Portuguese: *current/flow*) — the
> data fabric that keeps every byte in engenho moving smoothly
> through memory, disk, and N transports. Two tiers of storage,
> four transports for sync, one typed surface for consumers. Per
> the user's directive: *"data as eventually consistent in both
> memory and disk... different protocols and transports... a
> system that is extremely performant and well designed."*
>
> Companion docs:
>   * [FABRIC.md](FABRIC.md) — NATS+Vector teia layer (this doc
>     defines the data tier teia carries)
>   * [DISTRIBUTED.md](DISTRIBUTED.md) — revoada A/B/C/D layers
>   * [API-SURFACE.md](API-SURFACE.md) — REST/gRPC/GraphQL faces
>   * [LEAN.md](LEAN.md) — library strategy

## Thesis

K8s's etcd is strong-consistent: every write goes through Raft,
every read sees the latest committed state. That's perfect for
some things (which Pod is bound to which Node) and overkill for
others (last-seen ping from a kubelet, observed Pod CPU usage,
audit log entries).

engenho splits the data plane into **tiers + transports**:

```
                       CONSUMERS
        (controllers, apiserver, MCP, operators)
                          ↓
                   ┌──────────────┐
                   │  Typed view   │  ← the only public surface
                   │  (resource    │  ← `StoreMesh::get/list/propose`
                   │   catalog)    │
                   └──────────────┘
                          ↓
       ┌─────────────────┴────────────────┐
       ↓                                  ↓
   ┌─────────┐                       ┌──────────┐
   │ MEMORY  │  fast hot path        │   DISK   │  durable backstop
   │ (tier   │  ← read here          │ (tier 2) │  ← write here for
   │  1)     │                       │          │     persistence
   └─────────┘                       └──────────┘
       ↑                                  ↑
       │           SYNC LAYER             │
       └──────────────────────────────────┘
                          ↓
       ┌──────────────────┴──────────────────┐
       ↓                                     ↓
   ┌─────────────┐                    ┌──────────────┐
   │  STRONG     │                    │  EVENTUAL    │
   │  - openraft │  Raft commits      │  - chitchat  │  gossip
   │  - tameshi  │  attestation        │  - JetStream │  durable streams
   │             │                    │  - iroh DHT  │  P2P content
   └─────────────┘                    └──────────────┘
```

Per-resource, per-write, engenho picks the right consistency
tier. The trait surface is uniform; the transport choice is a
configuration parameter.

## Tier 1 — memory (the hot path)

Today: `engenho-store::ResourceCatalog` is a `BTreeMap<ResourceKey, ResourceValue>`
in `Arc<Mutex<Inner>>` per StoreMesh node. Reads are O(log n) +
zero I/O; writes go through openraft + the in-memory state-machine
apply.

**Properties:**
- Latency: microseconds for reads, ~1-10ms for writes (Raft commit)
- Capacity: bounded by RAM (typical cluster: <1GB for 10k resources)
- Durability: zero on its own — the Raft log on disk is the backstop

**Future tiers above this** for scaling reads:
- **L0 cache** in each controller (per-tick snapshot read once,
  reused throughout the reconcile). Already implicit in
  `Scheduler::tick` + others — extract to a typed
  `ResourceSnapshot` at R10.5.
- **L1 mmap segment** for large blobs (ConfigMap data, Secret data).
  Today inlined in serde_json::Value; future R10 introduces a typed
  Blob reference resolved on demand.

## Tier 2 — disk (the durable backstop)

Today: openraft's log + state-machine snapshot, currently
in-memory (engenho-store::InMemoryStore) — R10.5 adds a sled or
redb-backed persistent variant. Once persistent, the disk tier:

- Stores **every Raft entry** for crash-recovery replay
- Stores **periodic snapshots** so replay can restart from a
  bounded offset
- Stores **attestation chain blocks** (BLAKE3-linked, ed25519-signed
  per R4) — written alongside the Raft entry on each apply

**Properties:**
- Latency: <1ms writes (sled), ~10-50ms snapshots
- Capacity: bounded by disk (50GB PVC in default Helm values)
- Durability: fsync()-anchored after each Raft commit

**Eventual consistency in tier 2:** the disk lags memory by
"however long the writer thread takes to fsync." Reads served
from memory; disk is read only on restart for replay.

## Sync layer — the four transports

Each transport has a sweet spot. Per-write, engenho chooses one:

### Transport 1 — openraft (STRONG, in-process today, NATS R10)

  **Use for:** resource CRUD (pods, services, etc.), role
  assignments, anything kubectl-apply-shaped.

  **Semantics:** linearizable. Every commit goes through quorum;
  every read on the leader sees the latest committed state.

  **Latency:** ~5-20ms for commits depending on network +
  follower count. Reads on the leader are free (state machine
  is local).

  **Where:** `engenho-store::StoreMesh::propose` +
  `engenho-revoada::consensus::RaftMesh::propose`.

  **F2 (in flight):** the InProcessRouter becomes
  `engenho-teia::RaftTransport` riding NATS subjects per
  FABRIC.md. Same semantics, cross-process transport.

### Transport 2 — chitchat gossip (EVENTUAL)

  **Use for:** cluster membership, per-node health, capacity
  reports, free-form metadata that doesn't need quorum.

  **Semantics:** AP — every node converges within ~1-3 gossip
  rounds (1-3 seconds at default 1s interval). Phi-accrual
  failure detector + scuttlebutt reconciliation.

  **Latency:** 500ms-3s for convergence. Reads are instantaneous
  on the local view.

  **Where:** `engenho-revoada::membership::GossipMesh`. Layer A.

  **Properties:**
  - Survives partition (each side has its own view)
  - No consistency guarantee across partitions
  - Bounded staleness via heartbeat + grace period

### Transport 3 — NATS JetStream (DURABLE, eventually consistent)

  **Use for:** watch streams (R7.5b), audit logs, attestation
  receipts, anything that must be REPLAYABLE.

  **Semantics:** at-least-once delivery with stream-position
  cursor. Consumers can resume from any committed offset.
  Within-stream order preserved per partition key.

  **Latency:** ~1-5ms publish; consumer reads as fast as
  network/disk allows.

  **Where:** R7.5b (watch), R4-via-NATS (chain replication
  to followers), Vector observability ingest.

  **Capacity:** 20GB PVC default; ~10M small events.

### Transport 4 — iroh / NATS Object (CONTENT-ADDRESSED, P2P)

  **Use for:** large blobs that don't fit in resource values
  (OCI image layers, Helm chart tarballs, large ConfigMaps,
  Pod manifest archives). Content-addressed by BLAKE3 — same
  hash everywhere, deduplicated automatically.

  **Semantics:** content-addressed. If you have the hash, you
  have the content. Any node that holds it can serve. P2P
  resolution via mainline BitTorrent DHT (iroh) or NATS Object
  bucket (simpler, requires a NATS hub).

  **Latency:** depends on network — typically 50ms-1s to
  resolve + fetch from a nearby peer.

  **Where:** R5 (per FABRIC.md). Today: deferred. Helm chart
  references publish images to ghcr.io which serves the same
  shape via OCI.

## Per-resource consistency choice

The right tier depends on the resource:

| Resource | Tier | Why |
|---|---|---|
| Pod (spec) | Raft (strong) | scheduler reads must be linearizable |
| Pod (status.podIP) | Raft (strong) | EndpointsController reads it |
| Pod (status.metrics) | gossip (eventual) | per-tick metrics; can be stale |
| Node (spec) | Raft (strong) | scheduler reads must be authoritative |
| Node (status.conditions) | gossip (eventual) | heartbeat-derived |
| ConfigMap (data) | Raft (strong) | controllers expect consistent reads |
| ConfigMap (large data >1MB) | iroh (P2P) | offload from Raft log |
| Secret | Raft (strong) | values are sensitive — single source of truth |
| Endpoints | Raft (strong) | reads drive service routing |
| Attestation chain | JetStream + Raft | durable + per-node signed |
| Audit log | JetStream | replayable, queryable, no quorum needed |
| Cluster membership | gossip | eventually-consistent by design |
| Image layers | OCI (already content-addressed) | external standard |

The substrate exposes a single typed surface (`StoreMesh::propose`);
the transport choice is a per-resource hint configured in the
schema (R10.5 feature).

## Performance characteristics — back-of-envelope

For a 3-node engenho cluster on commodity hardware:

| Operation | Latency P50 | Latency P99 | Throughput |
|---|---|---|---|
| Raft propose (small) | 8ms | 25ms | 5000/sec |
| Raft read (leader) | <1ms | 2ms | 50000/sec |
| Raft read (follower) | <1ms | 2ms | 50000/sec |
| Gossip propagate | 1s | 3s | n/a (broadcast) |
| JetStream publish | 2ms | 10ms | 100000/sec |
| JetStream consumer pull | 5ms | 20ms | (depends on filter) |
| iroh content fetch (LAN) | 50ms | 500ms | (depends on size) |
| iroh content fetch (WAN) | 200ms | 5s | (depends on size) |

For comparison, etcd's typical 3-node cluster does ~10k writes/sec
+ ~100k reads/sec. engenho-store's openraft is in the same
ballpark; the eventual-consistency transports are headroom on top.

## Convergence semantics — what an operator can expect

**Strong path (Raft):**
- Write: kubectl apply → POST → Raft commit → state-machine apply
  → response. Total: ~10-30ms.
- After response returns, the resource is visible on EVERY node in
  the Raft group. No staleness.

**Eventual path (gossip):**
- Node heartbeat → chitchat gossip every 1s. After 1-3s, every
  node has a consistent membership view.
- Used for "soft" data — node liveness, capacity reports, observed
  metrics.

**Stream path (JetStream):**
- Watch event published on apply. Subscribers receive within ~5ms.
- Subscribers resuming from a cursor see all events since that
  cursor (durable retention).

**Content path (iroh/NATS Object):**
- Pod spec referencing a large blob by hash → fetch on demand.
- Cached locally after first fetch.

## Performance optimizations applied

1. **In-memory state machine** — reads are zero-I/O (no disk
   round-trip per read).
2. **BTreeMap key ordering** — list operations are streaming +
   ordered (no full materialization for paginated reads).
3. **Per-tick snapshots** — controllers take ONE catalog snapshot
   per tick (R10.5 explicit `ResourceSnapshot`); within the
   tick, all reads are O(log n) lookups.
4. **Subset comparison shortcuts** — `EndpointsController`'s
   `subsets_equivalent()` skips writes when nothing changed.
5. **Owner-ref + selector match early-out** — controllers filter
   before iterating (e.g. `is_owned_by` short-circuits).
6. **Per-controller intervals** — `ControllerRuntime` runs
   controllers at independent intervals so a slow controller
   doesn't block others.
7. **JetStream filter subjects** — controllers subscribed to
   `engenho.{cluster}.watch.{kind}.>` see only their kind, not
   the firehose.

## What this is NOT

- **NOT a CRDT system.** Conflicts at the strong tier are
  resolved by Raft order. Conflicts in the gossip tier are
  resolved by chitchat's last-writer-wins per-key
  reconciliation.
- **NOT a multi-master Raft.** Single leader per Raft group.
  Multi-region deployments use multiple Raft groups + federation
  (per FABRIC.md).
- **NOT replicated to disk synchronously.** Disk writes are
  asynchronous fsync after the Raft commit. A crash before fsync
  loses committed-but-not-fsynced entries — they replay from the
  leader's log on restart.

## Phased rollout

| Phase | Status | What |
|---|---|---|
| C0 | ✅ shipped | In-memory state machine (R6, openraft InMemoryStore) |
| C1 | ✅ shipped | BLAKE3+ed25519 attestation in tier 1 (R4.5) |
| C2 | partial | Watch event types (R7.5 partial); JetStream wiring at F3 |
| C3 | designed | NATS RaftTransport (F2 per FABRIC.md) — moves Raft RPC off in-process |
| C4 | pending | Disk-backed Raft log + snapshots via sled/redb |
| C5 | pending | Per-resource consistency-tier hints in schema |
| C6 | pending | iroh/NATS Object for blob offload |
| C7 | pending | Vector-driven observability stream consumption |

## Open design questions

1. **Snapshot frequency vs Raft log size.** Default openraft is
   "snapshot after N entries"; engenho-store currently never
   snapshots in-memory builds. C4 wires both with sensible
   defaults (snap every 10k entries).

2. **JetStream as Raft log alternative.** Could JetStream's
   built-in Raft replication replace openraft entirely for the
   resource catalog? Pros: one less thing to operate. Cons:
   couples engenho to NATS for strong consistency.

3. **Disk path for large attestation chains.** Today the chain
   is in-memory. C4+ writes blocks to disk + JetStream stream.

4. **CRDT for the gossip tier.** Today chitchat's last-writer-wins
   may lose information when concurrent updates race. Future:
   structured CRDT types (PN-counter for cpu_usage, OR-set for
   node tags) via automerge integration.

5. **Backpressure semantics.** What happens when:
   - Raft can't keep up (commits queue → reject with 503)
   - JetStream stream fills (drop oldest? reject publishes?)
   - iroh peer disk full (fall back to in-flight stream)

## Operator-visible artefacts

Once C4+ ships:

```yaml
# Per-cluster consistency-tier config in engenho Helm values
store:
  tiers:
    strong:
      backend: openraft+sled
      log_path: /var/lib/engenho-store/raft.log
      snapshot_path: /var/lib/engenho-store/snapshots
      snapshot_interval: 10000_entries
    eventual_gossip:
      backend: chitchat
      gossip_interval: 500ms
      phi_threshold: 8.0
    durable_stream:
      backend: nats-jetstream
      retention: 30d
      max_size: 20GB
    content:
      backend: iroh-mainline-dht
      cache_path: /var/lib/engenho-store/blobs
      max_cache_size: 50GB
```

Per-resource tier hints (annotation-driven):

```yaml
metadata:
  annotations:
    engenho.io/consistency-tier: strong
    # or: eventual_gossip, durable_stream, content
```

The substrate enforces the hint at admission time (R7.5b+).

## Why this design is "extremely performant"

1. **Reads are O(log n) memory lookups** — no disk round-trip
   for typical operations (etcd's strong-consistency reads hit
   disk; engenho doesn't unless you opt in).
2. **Writes go through ONE Raft commit** — etcd's three-tier
   log/state-machine/HTTP layers collapse to one in-process
   apply.
3. **Eventual transports avoid quorum** — gossip + streams +
   content don't wait for majority ack; consumers handle
   staleness explicitly.
4. **Per-controller intervals** — slow controllers don't block
   fast ones; the runtime is many independent reconcile loops.
5. **Content addressing for blobs** — image layers + big
   ConfigMaps don't bloat the Raft log; they reference hashes
   that resolve on demand to peers.

## Comparison to etcd + kube-apiserver

| Concern | etcd + apiserver | engenho |
|---|---|---|
| Read latency | ~5ms (disk) | <1ms (memory) |
| Write latency | ~20ms (Raft + fsync) | ~10ms (Raft + memory; fsync async at C4) |
| Watch backpressure | Best-effort (etcd's watch is fire-forget) | JetStream cursor-based at F3 |
| Audit | External (audit policy → file) | Built-in (per-node BLAKE3 chain, R4.5) |
| Multi-region | Federation via cluster API (Cluster Federation v2; deprecated) | Native via NATS leaf-nodes + per-region Raft (FABRIC.md) |
| Content blob handling | Stuffed in etcd, hits 1MB limit | iroh / NATS Object offload by hash (C6) |
| Cross-protocol API | REST only | REST + gRPC + GraphQL all from one spec (R7.5a/R7.6/R7.7) |
| Public observability | external (Prometheus + Vector) | Vector consumes engenho.*.{health,attestation} NATS subjects natively |

## Acceptance criteria

After C4 ships:
1. `kubectl apply` write latency p99 < 30ms (currently passing).
2. `kubectl get` read latency p99 < 5ms (memory served).
3. Watch resume from arbitrary cursor produces consistent stream.
4. Node-level crash + restart replays log from disk + rejoins
   Raft group within 30s.
5. Region-level partition: each region's Raft continues to serve
   reads; writes block on the minority side; consistency on
   heal (no split-brain).
6. Content fetch by BLAKE3 hash works across leaf-node federations.

The substrate already meets 1-2 today via in-memory; C4 adds
disk + the rest.
