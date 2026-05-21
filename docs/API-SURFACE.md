# engenho — API Surface Review + Multi-Face Generation Plan

> The pleme-io rule (org CLAUDE.md, Pillar 3): **API generation —
> forge-gen (OpenAPI → SDKs + REST + gRPC + GraphQL + MCP + IaC +
> completions + docs)**. Engenho commits to that pipeline. This
> document is the inventory + the path.

## Current API surfaces (v0.5.0)

### 1. HTTP REST — engenho-apiserver (R7)

The K8s API surface. Operators point kubectl at it; controllers
do reconcile loops via it.

  GET    /api/v1/namespaces/{ns}/{plural}/{name}
  GET    /api/v1/namespaces/{ns}/{plural}
  POST   /api/v1/namespaces/{ns}/{plural}
  PATCH  /api/v1/namespaces/{ns}/{plural}/{name}
  DELETE /api/v1/namespaces/{ns}/{plural}/{name}

  Plus cluster-scoped variants (no /namespaces segment) for
  Namespace, Node, ClusterRole, ClusterRoleBinding, etc.

  Status: ✅ shipped. 8 integration tests proving end-to-end
  REST → openraft → state machine apply.

### 2. MCP — engenho-mcp (Layer M0.0.2)

Operator/LLM-facing read surface for the kikai-managed live
cluster. Stable JSON tool catalog:

  cluster_status                 — kikai cluster lifecycle status
  cluster_config                 — typed config view
  cluster_kubeconfig             — kubeconfig export
  cluster_snapshot_meta          — VM snapshot metadata
  cluster_pods                   — typed Pod list (M0.0.1)
  cluster_resource_list          — generic typed list (kind + selectors)
  cluster_resource_get           — generic typed get

  ResourceKind catalog (13 kinds):
    Pod, Service, ConfigMap, Secret (redacted view),
    ServiceAccount, Endpoints, PersistentVolumeClaim,
    Namespace, Node, Deployment, ReplicaSet, Role, RoleBinding

  Status: ✅ shipped. Writer trait scaffolded but MCP-exposure
  gated on saguão authority at P2.

### 3. Internal Rust APIs (engenho-revoada + engenho-store libraries)

Programmatic surface for in-process callers (controllers,
scheduler, kubelet):

  engenho_revoada::{
    membership::GossipMesh,
    consensus::RaftMesh,
    policy::PolicyEngine,
    attestation::AttestationChain,
  }

  engenho_store::{
    StoreMesh,
    ResourceCommand,
    ResourceKey, ResourceValue,
    WatchEvent (R7.5 partial),
  }

  Status: ✅ shipped + tested (210 + tests across crates).

## What's missing per the directive

The user's directive: *"all gRPC, GraphQL, REST faces together
from central structures"* + *"full openapi document spec full
generation from there of sdk"*.

| Face | Status | Plan |
|---|---|---|
| REST | ✅ shipped | Wire utoipa to emit OpenAPI v3 spec |
| gRPC | pending R7.6 | tonic-based server alongside REST |
| GraphQL | pending R7.7 | async-graphql alongside REST + gRPC |
| **OpenAPI spec** | **next** | **utoipa derive(ToSchema) on types + /openapi.json route** |
| Rust SDK | downstream | openapi-generator-cli OR substrate's openapi-forge |
| Go SDK | downstream | openapi-generator-cli; bundled in releases |
| Python SDK | downstream | openapi-generator-cli; bundled in releases |
| JS/TS SDK | downstream | openapi-generator-cli; bundled in releases |
| MCP | ✅ shipped (engenho-mcp) | Catalog-driven; could derive from spec |

## Central structures — the source of truth

Per pleme-io's prime directive (single source of truth), every
face derives from the same typed primitives:

```text
                  ┌──────────────────────────────┐
                  │   engenho-types               │
                  │   typed K8s resource catalog  │  ← central typed source
                  │   (Pod, Service, …)            │
                  └──────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────────┐
        ↓                     ↓                         ↓
  ┌──────────────┐    ┌──────────────┐         ┌──────────────────┐
  │  REST        │    │  gRPC        │         │  GraphQL         │
  │  Axum router │    │  tonic       │         │  async-graphql   │
  │  + utoipa    │    │              │         │                  │
  └──────────────┘    └──────────────┘         └──────────────────┘
        │                     │                         │
        └─────────────────────┴─────────────────────────┘
                              ↓
                  ┌──────────────────────────────┐
                  │   StoreMesh (engenho-store)   │
                  │   single source of truth      │
                  │   for K8s resource catalog    │
                  └──────────────────────────────┘
                              │
                              ↓
                  ┌──────────────────────────────┐
                  │   openraft Raft group         │
                  │   3-node replication          │
                  └──────────────────────────────┘
```

The dispatch shape: each face is a translation layer from its
own protocol to `ResourceCommand`/`StoreMesh::get/list`. Same
typed commands; same Raft commits; same attestation chains.

## Configurability story

Per pleme-io's ★★ Configuration Management directive, every
operator-facing tool exposes config via:

1. typed schema (shikumi-style)
2. `shikumi::ConfigStore` discovery + load
3. hot-reload via notify
4. broadcast where needed

For engenho-apiserver, the config surface is:

```toml
# engenho-apiserver.toml
[server]
listen_addr = "0.0.0.0:6443"
tls_cert_path = "/etc/engenho/tls.crt"
tls_key_path = "/etc/engenho/tls.key"

[grpc]
listen_addr = "0.0.0.0:6444"

[graphql]
listen_addr = "0.0.0.0:6445"
introspection = true

[openapi]
serve_at = "/openapi.json"
spec_version = "3.0.3"

[store]
raft_addr = "0.0.0.0:7777"
peers = ["node-2:7777", "node-3:7777"]

[attestation]
identity_path = "/etc/engenho/identity.bin"
```

Status: not yet wired (engenho-apiserver currently takes
arguments via `ApiServer::start(addr, handlers)`). R7.6 wires
shikumi-style config.

## Phased rollout

| Phase | Deliverable |
|---|---|
| **R7.5a** (this commit) | utoipa-derive on types; /openapi.json endpoint serving v3 spec; integration test asserts spec validity + key paths |
| R7.5b | Wire watch event emission (the half-done R7.5 from prior round) into ResourceCatalog::apply + StoreMesh::watch + /watch SSE endpoint |
| R7.6 | gRPC face — tonic server with a small ResourceService proto; shares StoreMesh |
| R7.7 | GraphQL face — async-graphql with a Schema derived from engenho-types catalog; shares StoreMesh |
| R7.8 | SDK build via openapi-generator-cli wired into the release.yml matrix; per-language SDKs published as release artefacts |
| R7.9 | shikumi-style config for engenho-apiserver — TLS, listen addrs, peer list, identity path |
| R8 | engenho-scheduler — first real controller consuming StoreMesh::watch |
| R9 | engenho-controllers — Deployment/ReplicaSet/Service controllers |
| R10 | engenho-kubelet — per-node container lifecycle |
| R11 | engenho-proxy + CNI integration |

## Why utoipa first (not forge-gen)

The pleme-io substrate's forge-gen pipeline is the canonical
path: typed Rust → TOML spec → OpenAPI → SDKs. For engenho's
existing handlers, a faster path is utoipa derives — gets the
OpenAPI spec landed today + serves as the source for SDK gen
+ enables tooling like swagger-ui / openapi-generator-cli to
work immediately.

The forge-gen path can be added later as a separate codegen
step that feeds into the same OpenAPI spec.

## Acceptance test

Each face must satisfy:

1. **REST** — `kubectl --server=http://engenho-apiserver:6443 get pods`
2. **gRPC** — `grpcurl engenho-apiserver:6444 engenho.v1.ResourceService/ListResources`
3. **GraphQL** — `query { pods(namespace: "default") { name } }`
4. **OpenAPI** — `curl http://engenho-apiserver:6443/openapi.json | jq .openapi` returns `"3.0.x"`
5. **MCP** — already working via `engenho-mcp` (kikai-managed cluster) and future `engenho-mcp` (consuming engenho-apiserver directly).

All return the same data because all dispatch to the same
underlying `StoreMesh::propose` / `StoreMesh::get/list`.
