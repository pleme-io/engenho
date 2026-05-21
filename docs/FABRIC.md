# engenho — the fabric layer (NATS + Vector global web)

> **Codename: `teia`** (Portuguese: *web/weave*) — the unifying
> transport substrate ALL engenho layers ride on. Where revoada
> handles the cluster shape and store handles the K8s data,
> `teia` carries every byte between processes, between clusters,
> between regions. NATS is the messaging spine; Vector is the
> observability spine; together they make a globally-meshed
> engenho web that survives partitions, region failures, and
> intermittent links.
>
> Companion docs:
>   * [DISTRIBUTED.md](DISTRIBUTED.md) — revoada layers A–D'
>   * [API-SURFACE.md](API-SURFACE.md) — REST/gRPC/GraphQL faces
>   * [LEAN.md](LEAN.md) — library strategy

## Thesis

We've built four cluster-local layers (gossip, raft, policy,
attestation) + a typed K8s store + an HTTP apiserver. They work
**inside one cluster**. The fabric layer says:

> Every cross-process communication in engenho — Raft RPC, watch
> streams, content sync, attestation receipt fan-out, telemetry
> shipping, multi-cluster federation — rides one typed substrate.
> That substrate is NATS for messaging + Vector for observability.
> One transport. One topology. One trust model.

The payoff: an engenho instance in São Paulo, a second in
Tokyo, a third on an Apple silicon homelab in Brooklyn — all
participate in the **same logical engenho web**. The leaf-node
NATS topology means none of them needs to be reachable from any
of the others; they all dial out to regional hubs.

## What we already have (the research)

Surveyed across `~/code/github/pleme-io/`:

| Repo / module | NATS usage | Maturity |
|---|---|---|
| **tatara-engine** (`tatara/tatara-engine/src/nats/mod.rs`) | `NatsEventBus`: subject hierarchy `.events.{kind}`, `.logs.{alloc_id}.{task}`, `.health.{service}`, `.catalog.changes`. JetStream RESERVED (`_jetstream_reserved: false`). Uses `async-nats` 0.38. | Production |
| **tatara-operator** | JetStream stream `BUILD.request` → `BUILD.complete` (WorkQueue retention). Pull consumers driving Nix builds. | Production |
| **denshin** | WebSocket gateway with room-based `tokio::broadcast`. Pure state — no transport I/O. | Production (different shape) |
| **engenho-revoada** (current) | In-process `InProcessRouter` (mpsc channels). No external transport. | Local-only |
| **engenho-store** (current) | Same in-process pattern. No external transport. | Local-only |

| Repo / config | Vector usage | Maturity |
|---|---|---|
| `pangea-vector` flake module | DaemonSet sources (kubernetes_logs, host_metrics) → sinks (Datadog SaaS or VictoriaMetrics homelab) | ★★ Production (rio + nexus) |
| `pleme-io/k8s/clusters/*/observability/` | Vector Helm deployments | Production |

**Gap analysis:**
- No unified "fabric" abstraction. NATS use is single-purpose per crate.
- No JetStream KV store deployed (the natural distributed-state primitive sits unused).
- No leaf-node topology for multi-cluster.
- No NATS-backed transport for Raft RPC — it's all in-process today.

## The fabric — five logical channels over NATS

`teia` defines five typed channel families. All ride the same
NATS cluster (or supercluster, for multi-region):

```text
                          ┌──────────────────────────────────────┐
                          │              NATS supercluster        │
                          │   (one hub per region, leaf-node      │
                          │    overlay for edge clusters)         │
                          └──────────────────────────────────────┘
                                          │
   ┌─────────────────┬───────────────────┼───────────────────┬─────────────────┐
   ↓                 ↓                   ↓                   ↓                 ↓
┌────────┐    ┌─────────────┐   ┌───────────────┐   ┌───────────────┐   ┌──────────────┐
│ raft   │    │  watch      │   │  content      │   │  attestation  │   │  observ.     │
│ subj.  │    │  subj.      │   │  obj. store   │   │  jetstream    │   │  (Vector)    │
└────────┘    └─────────────┘   └───────────────┘   └───────────────┘   └──────────────┘
   ↑                 ↑                   ↑                   ↑                 ↑
   │                 │                   │                   │                 │
 Raft RPC       K8s watch API       Pod manifests +     BLAKE3 chain      Logs +
 (append,        + WatchEvent       images +           per-node          metrics +
  vote,          fan-out            ConfigMaps         blocks            traces
  snapshot)
```

### Channel 1 — Raft RPC over NATS subjects

Replaces engenho-revoada + engenho-store's `InProcessRouter`.

**Subjects:**

```
engenho.{cluster}.raft.{group}.append.{target_node}
engenho.{cluster}.raft.{group}.vote.{target_node}
engenho.{cluster}.raft.{group}.snapshot.{target_node}
```

Where `{group}` is either `revoada` (role assignments) or `store`
(K8s resources). Each Raft node subscribes to its own
`.{node_id}` subjects + publishes to peers'. Request-reply pattern
(`async-nats::request`) handles the AppendEntries response.

**Why NATS, not tonic gRPC:** subject hierarchy gives us free
fan-out (broadcast to all peers via wildcard) + transparent leaf-
node routing across regions. gRPC requires point-to-point
mesh wiring.

### Channel 2 — Watch streams via JetStream

Replaces the half-done R7.5 watch event broadcast.

**Streams:**

```
ENGENHO_{cluster}_WATCH_{group_version_kind}
  subjects: engenho.{cluster}.watch.{gvk}.>
  retention: limits + max_age = 30 days
  storage: file
```

Every commit on engenho-store's state machine publishes a
`WatchEvent` (`type: ADDED|MODIFIED|DELETED`, full resource) to
`engenho.{cluster}.watch.{gvk}.{namespace}.{name}`. The K8s
WATCH API + every controller subscribes via a JetStream
**pull consumer** with start-from-resource-version cursor.

**Why JetStream not core pub-sub:** at-least-once delivery +
replay from cursor (essential for controllers restarting),
plus per-subject durable consumers.

### Channel 3 — Content-addressed object store via NATS Object

Replaces the iroh-based Layer E in revoada's original design.

**Buckets:**

```
engenho-{cluster}-images       — OCI image layers, BLAKE3-keyed
engenho-{cluster}-charts       — Helm chart tarballs
engenho-{cluster}-configmaps   — large ConfigMap data blobs
engenho-{cluster}-manifests    — Pod / Deployment manifests
```

Workers fetch content by hash from the nearest NATS server
(leaf-node routing handles regional locality automatically).
Sets up the substrate for: nodes pull content from the closest
NATS server, not always from a central artifact registry.

**Why NATS Object not iroh:** already deployed (we're already
running NATS for raft + watch), no new transport, leverages
the leaf-node hub topology, content-addressed keys match
BLAKE3 hashes from our attestation chain.

### Channel 4 — Attestation chain via JetStream KV

Multi-witness chain receipts (R4.5's per-node chains).

**Stream:**

```
ENGENHO_{cluster}_ATTESTATION
  subjects: engenho.{cluster}.attestation.{node_id}.>
  retention: limits (forever)
  storage: file + R3 replication
```

Every node publishes its locally-signed `RoleAttestationBlock`
to `engenho.{cluster}.attestation.{node_id}.{index}` on apply.
The stream provides durable history; an auditor subscribes to
all `engenho.{cluster}.attestation.>` for the federated multi-
witness view.

**Future R8+:** A JetStream KV bucket `engenho-{cluster}-chain-heads`
tracks the latest committed head per node — enables fast catch-
up after node restart.

### Channel 5 — Observability via Vector

Vector DaemonSet on every node + aggregator per region.

**Sources:**

```toml
[sources.kubernetes_logs]
type = "kubernetes_logs"

[sources.engenho_nats_health]
type = "nats"
url = "nats://engenho-nats:4222"
subject = "engenho.*.health.>"

[sources.engenho_attestation]
type = "nats"
url = "nats://engenho-nats:4222"
subject = "engenho.*.attestation.>"
```

**Sinks:**

```toml
[sinks.datadog]                          # SaaS clusters
type = "datadog_logs"

[sinks.victorialogs]                     # homelab clusters
type = "loki"
endpoint = "http://victorialogs:9428"

[sinks.attestation_archive]              # compliance trail
type = "aws_s3"
bucket = "engenho-attestation-archive"
```

Vector reads engenho's NATS subjects + ships to the right
sink per cluster's compliance tier. **No engenho code touches
Vector** — it's purely consuming NATS subjects engenho already
publishes.

## Multi-region topology — the global engenho web

```text
        Tokyo region                   São Paulo region                  Brooklyn (edge)
   ┌─────────────────────┐         ┌─────────────────────┐         ┌─────────────────────┐
   │  NATS hub cluster    │ ◄────► │  NATS hub cluster    │ ◄────► │  NATS leaf node      │
   │  (3 servers, R3)     │ GW     │  (3 servers, R3)     │ GW     │  (1 server,          │
   │  JetStream Meta R3   │        │  JetStream Meta R3   │        │   dials hub)         │
   └─────────────────────┘         └─────────────────────┘         └─────────────────────┘
            │                                │                                │
   ┌─────────────────────┐         ┌─────────────────────┐         ┌─────────────────────┐
   │  engenho cluster A   │         │  engenho cluster B   │         │  engenho cluster C   │
   │  - apiserver         │         │  - apiserver         │         │  - apiserver         │
   │  - store (3-node     │         │  - store (3-node     │         │  - store (3-node     │
   │     raft, leader     │         │     raft, leader     │         │     raft, leader     │
   │     in this region)  │         │     in this region)  │         │     in this region)  │
   │  - revoada           │         │  - revoada           │         │  - revoada           │
   └─────────────────────┘         └─────────────────────┘         └─────────────────────┘
```

### Three federation modes

**(1) Independent clusters with cross-region observability.**

Each region's engenho-store Raft group is local. Vector ships
logs/metrics/traces across NATS gateways for centralized
observability. Default for most workloads.

**(2) Shared resource catalog across regions (federation).**

A single engenho-store Raft group spans regions via NATS
gateway connections. Higher latency (cross-region Raft
heartbeats) but global resource visibility. Used for
ClusterScope objects (Namespaces, Nodes, ClusterRoles).

**(3) Sharded — each region owns a subset of namespaces.**

Resources sharded by namespace label. Cross-region reads
follow leaf-node routing to the home region. Compromise
between (1) and (2). Default for production multi-tenant.

### Leaf-node bridge for unreachable edges

The Brooklyn homelab can't be reached from the internet but
can DIAL OUT to a hub. NATS leaf-node:

```
# Brooklyn edge
[leafnodes]
  remotes = [
    { url: "nats://hub.engenho.global:7422", credentials: "/etc/nats/edge.creds" }
  ]
```

Now the homelab participates in the engenho web. The hub
clusters publish events to subjects the leaf subscribes to;
the leaf publishes its local engenho state up to the hub.
Symmetric. No port-forwarding. No VPN. Just a TCP connection
dialed from the edge.

## The new crate — `engenho-teia`

```text
engenho-teia/
├── Cargo.toml
├── src/
│   ├── lib.rs                       Public surface
│   ├── client.rs                    TeiaClient wrapping async-nats::Client
│   ├── config.rs                    TeiaConfig (servers, jwt, leaf)
│   ├── subjects.rs                  Typed subject builder
│   │                                  Subject::raft_append("revoada", 7)
│   │                                  Subject::watch("default", &gvk, "podinfo")
│   │                                  Subject::attestation(node_id, index)
│   ├── raft_transport.rs            RaftTransport over NATS subjects
│   │                                  swaps in for engenho-revoada's
│   │                                  InProcessRouter + engenho-store's
│   ├── watch_pub.rs                 Watch event publisher → JetStream
│   ├── watch_sub.rs                 Watch consumer with resource-version cursor
│   ├── content_store.rs             NATS Object store wrapper, BLAKE3 keys
│   ├── attestation_pub.rs           Per-node chain block publisher
│   └── observability.rs             Vector source configuration emitters
└── tests/
    ├── teia_raft_two_node.rs        Two RaftMesh instances over NATS
    ├── teia_watch_round_trip.rs     publish → consume cursor → resume
    ├── teia_object_store.rs         put → get by BLAKE3 hash
    └── teia_attestation_archive.rs  multi-node fan-out
```

### Backwards compatibility

`engenho-revoada` and `engenho-store` keep their `InProcessRouter`
as the test/local default. `engenho-teia::RaftTransport`
implements the same trait shape; production deployments swap
the router via dependency injection at `RaftMesh::start`.

## Phased rollout

| Phase | Deliverable | Layer |
|---|---|---|
| **F0** (this doc) | Design freeze | — |
| F1 | `engenho-teia` crate scaffold + TeiaConfig + Subject builder | fabric/connect |
| F2 | RaftTransport NATS impl + integration test with 2 RaftMesh over NATS | fabric/raft |
| F3 | JetStream Watch publisher + cursor-based consumer | fabric/watch |
| F4 | NATS Object store wrapper for content sync (BLAKE3 keys) | fabric/content |
| F5 | Attestation chain publisher + auditor's multi-witness aggregator | fabric/attest |
| F6 | Vector source configs + Helm chart sink wiring | fabric/observ |
| F7 | NATS supercluster Helm chart + leaf-node templates | fabric/topology |
| F8 | engenho-helm: full chart with HelmRelease orchestration via FluxCD | deploy |
| F9 | Global mesh test — 3-cluster federation via leaf nodes | proof |

## Why this is the right shape

1. **One transport.** Today engenho has four cross-process
   transports (axum HTTP, tonic gRPC at R7.6, in-process channels,
   WebSocket via denshin). After teia: one. NATS subjects carry
   everything.

2. **Globally meshed by default.** NATS leaf-node + gateway
   topology is mature production tech (Synadia runs it for
   payment-grade enterprise customers). No invention required.

3. **Persistence is durable + replayable.** JetStream gives
   us at-least-once delivery + cursor replay. Controllers
   restart with zero lost events. Auditors verify chains
   without coordinating with cluster owners.

4. **Compounds with existing pleme-io patterns.** Tatara-engine
   already publishes events to NATS. Tatara-operator already
   uses JetStream WorkQueues. Vector is already deployed.
   `engenho-teia` standardizes the shape across all of them.

5. **The "never fails" promise becomes operational.** A region
   loses connectivity → leaf nodes reconnect when it returns →
   missed events replay from JetStream cursor → state catches
   up. Multi-region quorum survives single-region outage. The
   web heals itself.

6. **MCP, REST, gRPC, GraphQL all become NATS subjects.** Future
   R7.6+ can route protocol-specific RPC over NATS subjects via
   `async-nats::service` (request-reply). Same dispatch shape;
   external clients see protocol-native APIs; internal callers
   see typed subjects.

## What this is NOT

- **Not** a replacement for openraft. The Raft algorithm stays;
  only the RPC transport changes.
- **Not** a replacement for Vector. We standardize Vector
  source configs; we don't reimplement.
- **Not** a centralized state store. NATS is the messaging
  spine; engenho-store remains the typed source of truth.

## Open design questions

1. **Per-cluster Raft groups or one global group?** Defaults
   to per-cluster (lower latency). Federation modes (1)/(2)/(3)
   above let operators choose.

2. **Subject naming for multi-tenant.** Should subjects include
   tenant prefix? Currently `engenho.{cluster}.*`; multi-tenant
   would be `engenho.{tenant}.{cluster}.*`.

3. **Backpressure semantics.** JetStream can rate-limit; how
   should engenho-store handle backpressure on the watch stream
   when downstream consumers fall behind?

4. **Trust model.** NATS supports JWT-based decentralized auth
   via NSC (NATS Security Configuration). engenho-teia issues
   per-node JWTs signed by the cluster's saguão-equivalent
   passport authority.

5. **NATS server itself becomes a dep.** We're standardizing on
   running NATS in every engenho cluster. Helm chart bundles
   the official NATS chart as a subchart.

## Operator-visible artefacts

After F8 ships:

```sh
# Deploy the full engenho stack to a cluster via FluxCD
$ kubectl apply -f flux/engenho-stack/kustomization.yaml

# FluxCD reconciles the HelmReleases in dependency order:
#   1. nats           (the fabric carrier)
#   2. vector         (observability) — depends on nats
#   3. engenho-store  (3-replica StatefulSet) — depends on nats
#   4. engenho-revoada (DaemonSet, per-node gossip) — depends on store
#   5. engenho-apiserver (Deployment) — depends on revoada + store

# All cross-component traffic rides NATS subjects.
# All observability rides Vector → regional sink.
```

A second cluster joining the global mesh adds one leaf-node
section to its NATS subchart config. Everything else is automatic.
