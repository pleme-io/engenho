# engenho

> Pangea declares the supercontinent's shape; magma realizes it on cloud
> substrate; engenho is the engine that runs the land (terreno) — the
> typed, attested, Rust-native Kubernetes runtime.

One single-binary Kubernetes distribution, wire-compatible with kubectl /
CRI / CNI / etcd-v3. Resource catalog mechanically generated from
upstream OpenAPI v3 via `kube-forge` (Pillar 12 — generation over
composition). Caixa M2/M3 slots reconciled natively without Helm
intermediation. Secrets flow through cofre (no plaintext k8s Secrets).
Compliance via tameshi + sekiban + kensa at admission. Pods can be OCI
containers OR tatara tlisp programs via `runwasi`.

**Status:** Draft v1, pre-implementation (M0.0 in flight). The
destination doc —
[`pleme-io/theory/ENGENHO.md`](https://github.com/pleme-io/theory/blob/main/ENGENHO.md)
— is canonical; read it before touching anything load-bearing.

**Repo docs:**
- [`CLAUDE.md`](./CLAUDE.md) — agent-facing repo guide (architecture, build, anti-patterns)
- [`docs/M0-ROADMAP.md`](./docs/M0-ROADMAP.md) — local M0.0.1 → M0.0.4 path-down
- [`theory/ENGENHO.md`](https://github.com/pleme-io/theory/blob/main/ENGENHO.md) — destination, typed surface, compatibility contract, phases
- [`theory/ENGENHO-LOCAL.md`](https://github.com/pleme-io/theory/blob/main/ENGENHO-LOCAL.md) — the canonical operator local-dev path: kasou + kikai → working k8s cluster on every `nix run .#rebuild`

## Quick start — the canonical path

The default first way to run engenho is via the kasou + kikai
substrate: a typed kasou-managed Linux VM on macOS, running k3s
today as a 1:1 wire-compat bridge → engenho once M0.4 ships. Full
spec at [`theory/ENGENHO-LOCAL.md`](https://github.com/pleme-io/theory/blob/main/ENGENHO-LOCAL.md).

One Nix declaration on the operator's `nodes/<host>/engenho-local.nix`:

```nix
blackmatter.components.kubernetes.clusters.engenho-local = {
  enable    = true;
  vmMode    = "engenho";      # → routes to k3s today; native engenho M0.4+
  autoStart = true;
  cpus      = 4;
  memory    = 8192;
  diskSize  = "50G";
  apiPort   = 6443;
};
```

After `nix run .#rebuild`:

```bash
$ kubectl --context engenho-local get nodes
NAME            STATUS   ROLES                  AGE   VERSION
engenho-local   Ready    control-plane,master   2m    v1.32.0+k3s1
```

## Build the binary itself

```bash
cargo build --workspace
cargo test  --workspace      # 12 unit + 5 manifest + 3 proptests (768 cases)

nix build                    # hermetic release build via substrate's
                             # rust-workspace-release-flake.nix
./result/bin/engenho         # M0.0 placeholder — prints destination pointer
```

## Architecture (target — theory/ENGENHO.md §II.1)

```
engenho (workspace root)
├── engenho-types         — generated from upstream OpenAPI v3 via kube-forge.
│                           ONE #[derive(KubeResource, TataraDomain)] per kind.
├── engenho-datastore     — fjall-backed local KV + tonic-served etcd-v3 gRPC
│                           shim (Range / Txn / Watch / Lease) speaking
│                           revision-MVCC. openraft + fjall in HA tier (M1).
├── engenho-apiserver     — Axum + tonic + tokio + rustls. Authn (JWT-SA),
│                           RBAC, admission chain, OpenAPIv3 emitter,
│                           watch broadcast, SSA + JSONPatch + StrategicMergePatch.
├── engenho-controllers   — 18 controllers, ONE #[derive(TataraController)] per
│                           kind. Workqueue / leader election via shigoto + Lease.
├── engenho-scheduler     — 11-plugin default profile. Filter+Score+Reserve+Bind.
├── engenho-kubelet       — CRI gRPC client (tonic against containerd today,
│                           youki+oci-distribution direct M3+). Evented PLEG.
├── engenho-cni           — bridge + IPAM + portmap + loopback (M0). VXLAN (M0.5).
├── engenho-kubeproxy     — rustables/nftnl-rs netlink. Pure-Rust nftables only.
├── engenho-dns           — hickory-dns + KubernetesAuthority. Drop-in CoreDNS.
├── engenho-localpath     — local-path-provisioner-equivalent (M0).
├── engenho-ca            — rcgen built-in CA + CSR signer.
├── engenho-caixa         — native Caixa M2/M3 reconcilers (M1+).
├── engenho-mesh          — native Aplicacao mesh (M3+).
├── engenho-gateway       — pingora-based Gateway API (M3+).
├── engenho-attest        — tameshi integration; sekiban admission webhook.
├── engenho-cli           — kubectl-shaped operator CLI.
└── engenho               — Umbrella binary; one ~30-50 MB Rust binary.
```

**Today (M0.0)**: `engenho-types` + `engenho` binary only.

## Wire compatibility (theory/ENGENHO.md §III)

| Surface | Format | Source of truth |
|---|---|---|
| Kubernetes REST API | HTTPS :6443; JSON / protobuf; v1.32 schema | upstream `kubernetes/api/openapi-spec/v3/` |
| etcd v3 gRPC | Range/Put/Txn/Watch/Lease over UDS | upstream `etcd-io/etcd@v3.5/api/etcdserverpb/` |
| CRI v1 | gRPC client to containerd / youki | upstream `kubernetes/cri-api/v1` |
| CNI v1 | stdin JSON + ADD/DEL/CHECK/VERSION | `containernetworking/cni` spec |
| Gateway API | v1.1 GA — GatewayClass/Gateway/HTTPRoute (M3+) | sigs.k8s.io/gateway-api |

## CNCF Certified Kubernetes target — M4

The certified-conformance e2e suite (~300 `[Conformance]` tests, zero
skips) is the M4 release gate. Engenho aims to be the first non-Go
distribution to pass.

## Substrate integration (no escape hatches)

| Primitive | How engenho uses it |
|---|---|
| `substrate/lib/rust-workspace-release-flake.nix` | `flake.nix` — one import, no hand-rolled build glue |
| `forge-gen` + `kube-forge` (sibling, M0.0.3) | OpenAPI v3 → typed Rust catalog. No hand-authored kinds. |
| `tatara` | engenho is a tatara binary; every subsystem under `defguest` daemon mode (small tatara surgery — see `pleme-io/tatara/docs/daemon-supervision.md`) |
| `shigoto` | Every controller's reconcile loop is a `shigoto::Dag` |
| `shikumi` | All operator config typed |
| `cofre` | kubelet has cofre client; k8s Secrets carry references, not plaintext |
| `tameshi` / `sekiban` / `kensa` | Admission chain + BLAKE3 attestation on every artifact |
| `nix-ast` | All emitted Nix (deploy.yaml fragments etc.) typed |
| `pleme-actions` | CI workflows migrate to `pleme-io/rust-ci-action@v1` |

## Phases (theory/ENGENHO.md §X)

| Phase | Scope | Target |
|---|---|---|
| M0.0 | `engenho-types` seed (DONE) | typed contract trait + meta/v1 types + vendored OpenAPI |
| M0.0.1 | Pod as kube-forge bullseye | smallest end-to-end target shape |
| M0.0.2 | `pleme-io/kube-forge` sibling crate | OpenAPI v3 → Rust source emitter |
| M0.0.3 | Pod regenerated bit-reproducibly | deterministic generator gate |
| M0.0.4 | All ~150 kinds | full catalog generated |
| M0.1 | Datastore + apiserver | kubectl handshake works |
| M0.2 | Controller-manager + scheduler | workloads bind to nodes |
| M0.3 | Kubelet (CRI client) | workloads actually RUN |
| M0.4 | Networking + DNS + local-path | single-node complete |
| M0.5 | Multi-node | VXLAN + ServiceLB + CSR bootstrap |
| M1 | HA + native Caixa | openraft + caixa CRD reconciler |
| M2 | Typescape-native | controllers as tatara programs |
| M3 | Mesh + compliance + Gateway | full Aplicacao + admission attestation |
| M4 | CNCF Certified | conformance + submission |
| M5 | Programs as RuntimeClass | tlisp WASI pods |

## License

MIT.
