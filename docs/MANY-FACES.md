# engenho — many faces, one substrate

> **The user's directive (verbatim):**
> *"review everything we have created for full shikumi style
> configuration breadth making the most flexible and dynamic
> software possible and of course kubernetes yaml must be a first
> class citizen but apparently nomad hcl and json for both as well
> for our many faces"*

## Thesis

engenho already has a `Face` trait (engenho-revoada/src/face.rs)
with three impls (PureRaft, Kubernetes, Nomad). It's a typed
boundary between "what the operator sees" and "what the substrate
runs." The user wants this boundary widened so every face is
**first-class** — meaning:

  * **Input parity**: every face accepts its native config format
    (Kubernetes YAML, Nomad HCL+JSON) without translation gymnastics.
  * **Output parity**: every face emits the same — operators can
    `kubectl get -o yaml` against engenho-apiserver, or
    `nomad job inspect -t '{{.}}'` against engenho-nomad-face,
    and see byte-for-byte familiar output.
  * **Shikumi at the top**: cluster-wide engenho configuration
    flows through `shikumi::TieredConfig` (typed, layered,
    hot-reloadable) like every other pleme-io tool.

## Face × format matrix — target state

| Face | Input | Output | Status |
|---|---|---|---|
| `PureRaftFace` | engenho.yaml + node bootstrap | engenho-store API | shipping (R6/R7) |
| `KubernetesFace` | Kubernetes YAML manifests | etcd-shaped responses | apiserver HTTP API shipping; YAML round-trip parity ongoing |
| `NomadFace` | Nomad HCL + Nomad JSON | Nomad job API shape | typed stub (R-FACE.0); parser pending |

## Configuration tiers — shikumi style

Every engenho process loads its config from a **tiered cascade**
identical to other pleme-io tools (mado / tend / frost / kurage):

```
Tier 1: $ENGENHO_CONFIG  (single TOML/YAML/JSON file)
Tier 2: $XDG_CONFIG_HOME/engenho/{engenho,scheduler,controllers}.{toml,yaml}
Tier 3: /etc/engenho/{engenho,scheduler,controllers}.{toml,yaml}
Tier 4: ConfigMap inside the engenho cluster (live, hot-reloadable)
Tier 5: Compiled-in `prescribed_default()` — last-resort safe values
```

`shikumi::TieredConfig` provides:

  * Typed config struct (one per tool — scheduler.toml, controllers.toml, etc.)
  * Deep-merge across tiers — operator overrides cluster defaults
    overrides prescribed defaults
  * `notify`-based hot-reload — controllers re-read on file mtime
  * Schema-aware diff: which tier last touched which field?

### Config breakdown (target)

```yaml
# /etc/engenho/engenho.yaml — top-level config
cluster:
  name: rio
  region: us-east-2

revoada:
  topology:
    strategy: phalanx
    min_nodes: 1
    grace_period: 10s
  membership:
    gossip_addr: 0.0.0.0:7800
    phi_threshold: 8.0

face:
  active: kubernetes      # or "pure_raft" / "nomad"
  kubernetes:
    apiserver_bind: 0.0.0.0:6443
    tls_cert: /etc/engenho/tls/server.crt
    tls_key: /etc/engenho/tls/server.key
  nomad:
    api_bind: 0.0.0.0:4646
    region: global

consistency:
  default_tier: strong
  per_kind_overrides:
    Pod.status.metrics: eventual_gossip
    AuditEvent: durable_stream

scheduler:
  strategy: round_robin
  tick_interval: 5s
  fallback_interval: 30s

controllers:
  enable: { replicaset: true, deployment: true, endpoints: true, gc: true }
  fallback_interval: 30s
  debounce: 50ms
```

## First-class Kubernetes YAML

The apiserver (R7) already accepts Kubernetes YAML via HTTP POST.
The gap is **structural**:

  * Apiserver currently stores resources as `serde_json::Value`
    + the catalog routes by `(group, version, kind)`. YAML
    round-trip is implicit via serde.
  * **Gap**: no schema validation at admission — invalid YAML
    that parses to a `Value` is accepted. R-K8S.1 adds typed
    admission via `engenho-types` Pod/Service/etc. structs.
  * **Gap**: no `-o yaml` output — apiserver only returns JSON.
    R-K8S.2 adds a `serde_yaml` writer + the `Accept: application/yaml`
    content negotiation kubectl uses.

## First-class Nomad HCL + JSON

The NomadFace trait impl is a stub. To make it real:

  * **R-NOMAD.0**: typed Nomad Job + TaskGroup + Task structs
    (engenho-types/src/nomad/v1.rs). Same shape as Kubernetes —
    a typed catalog for the Nomad universe.
  * **R-NOMAD.1**: HCL parser via the `hcl-rs` crate. Reads
    Nomad job HCL → typed `nomad::v1::Job`. Tolerant of unknown
    blocks (forward-compat).
  * **R-NOMAD.2**: HCL emitter — typed Job → HCL via
    `hcl-rs`'s serializer. Round-trip tests confirm idempotency.
  * **R-NOMAD.3**: JSON parser + emitter via serde_json (free —
    Nomad accepts JSON natively).
  * **R-NOMAD.4**: Translator — `kubernetes::v1::Pod` →
    `nomad::v1::Job::TaskGroup::Task`. Lets operators run the
    same workload through either face.

## Phased rollout

| Phase | Status | What |
|---|---|---|
| C-CONF.0 | designed | This doc + audit |
| C-CONF.1 | next | Top-level `engenho.yaml` shikumi schema + loader |
| R-K8S.1 | next | Apiserver admission validates via typed K8s catalog |
| R-K8S.2 | next | Apiserver `-o yaml` content negotiation |
| R-NOMAD.0 | next | `engenho-types/src/nomad/v1.rs` — Job/TaskGroup/Task structs |
| R-NOMAD.1 | next | HCL parser via hcl-rs |
| R-NOMAD.2 | next | HCL emitter |
| R-NOMAD.3 | next | JSON in/out for Nomad face (free via serde) |
| R-NOMAD.4 | future | Pod ↔ Task translator |

Each phase is bounded by:
  * Typed surface in `engenho-types` (no hand-authored types — generator extension when possible)
  * Round-trip property tests (read → write → re-read → identical bytes)
  * Integration test that the face accepts + emits the format
  * Documentation in this file

## Why "many faces" matters

The substrate has one truth — the engenho-store openraft state
machine. Every face is a **view** onto that truth:

  * Kubernetes face: presents the store AS IF it were etcd + an
    apiserver. Operators use kubectl unmodified.
  * Nomad face: presents the store AS IF it were Nomad. Operators
    use nomad CLI unmodified.
  * PureRaft face: the underlying truth, no translation.

Adding a face doesn't fork the substrate — it adds a new
view. The store underneath remains canonical; every face
serializes the same committed bytes. R-TOPO.0 + C5 + R-K8S +
R-NOMAD all compose: a Pod committed via kubectl applies as a
Job through the Nomad face automatically.

This is the user's vision operationalized — **maximum format
breadth with zero substrate fork**.
