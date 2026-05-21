# engenho — the lean engineering rationale

> **What engenho ships itself, what it consumes, and why the
> composition compounds to the leanest Rust-native Kubernetes
> runtime that can pass v1.34 conformance.**
>
> Read [`pleme-io/theory/ENGENHO.md`](../../theory/ENGENHO.md) first
> for the long-form theory. This doc is the operating decision
> matrix — every Kubernetes subsystem, what we own vs what we
> compose, and the source of the choice.

## Thesis

A Kubernetes runtime is ~30 distinct subsystems (apiserver,
controller-manager, scheduler, kubelet, kube-proxy, DNS, network
plugin, container runtime, storage drivers, admission, RBAC,
secrets, observability, …). The lean question isn't "can we
rewrite all of them in Rust" — it's *"which subset must engenho
own to deliver the proof we promise, and which can it adopt from
the OSS Rust ecosystem?"*.

Engenho ships the **typed substrate** — the resource catalog
(generated from OpenAPI), the apiserver wire surface, the
controller dispatch fabric, the scheduler policy. Everything
below the wire (container exec, networking, VMM) is **adopted**
from purpose-built Rust primitives the Rust community already
proved at scale.

## The decision matrix

| Subsystem | What engenho owns | What engenho composes | Why |
|---|---|---|---|
| **Resource catalog** | `engenho-types` (typed-first, OpenAPI-driven via `engenho-kube-codegen`) | — | We deliberately do NOT use `k8s-openapi` because it's macro-driven; engenho-types is `#[derive(KubeResource)]`-first so the typescape (arch-synthesizer) can participate. Generation IS composition (Pillar 12). |
| **API server** | `engenho-apiserver` over `hyper` + `tokio` | hyper, rustls, h2 (TLS + HTTP/2 + protobuf), `tower` middleware | hyper is the canonical Rust HTTP server. We ship the K8s wire shape on top. |
| **etcd store** | `engenho-store` over SeaORM | sqlx (sqlite for single-node; postgres for HA) | SeaORM is pleme-io's Pillar 4 default. Mirrors kine's approach (sqlite-backed etcd shim) but consumed directly via SeaORM rather than as a separate process. |
| **Watch streams** | `engenho-watch` | tokio broadcast channels + hyper SSE | Pure-Rust async; no etcd raft dependency. |
| **Controllers** | `engenho-controllers` (Pod, Deployment, ReplicaSet, …) | `shigoto::Dag` for typed work scheduling | shigoto's typed Job + retry + budget + gates is exactly the controller shape; no hand-rolled work queues. |
| **Scheduler** | `engenho-scheduler` (policy) | `shigoto` queue | Policy is engenho's (typed predicate/priority); transport is shigoto's. |
| **Admission** | (consume) | `sekiban` webhook + signature gates | sekiban already implements admission with tameshi-backed attestation. Engenho registers it as the default webhook. |
| **Compliance** | (consume) | `kensa` (continuous attestation + OSCAL/NIST mappings) | Kensa is the canonical pleme-io compliance engine. Engenho's PromessaController surfaces compliance posture as a typed CR. |
| **Secrets** | (consume) | `cofre` (zero-plaintext, typed backend refs) | k8s Secret objects carry references, not plaintext. Cofre materializes at admission time. |
| **Config** | (consume) | `shikumi::TieredConfig` | Engenho daemon + every controller pulls config through shikumi. |
| **Container exec** | `engenho-kubelet` (CRI server) | **youki** (production-ready Rust OCI runtime, passes OCI conformance, drop-in for runc) | Don't reimplement runc. youki is the path. |
| **Networking (CNI)** | `engenho-cni-config` (declarative CNI manifest renderer) | **cilium** (eBPF-based; dominant CNI in production 2026, surpassed Flannel + Calico) | eBPF is the right substrate; cilium owns it. We provide the typed cluster-config bindings. |
| **kube-proxy** | (skip in cilium mode) | cilium's eBPF kube-proxy replacement | When CNI is cilium, kube-proxy becomes redundant. Optional `engenho-kube-proxy` (Rust iptables/ipvs) for cilium-less clusters. |
| **DNS** | (consume) | CoreDNS as a Deployment in engenho clusters | DNS is not the runtime; it's a workload. |
| **Pod sandbox (microVM mode)** | `engenho-vm-runtime` | **cloud-hypervisor** (Rust VMM, ~50K LoC vs QEMU's 2M LoC) | Kata-style microVM pods become a RuntimeClass. cloud-hypervisor is the Rust VMM. firecracker for ephemeral workloads. |
| **kasou-VM (macOS dev)** | (consume) | `kasou` (Apple Virtualization.framework wrapper) | macOS operator dev path; production goes through cloud-hypervisor. |
| **WASI workloads** | `engenho-runtime-class-wasi` | `runwasi` containerd shim | runwasi runs WASI modules as pods under containerd. Engenho registers it as a RuntimeClass. |
| **Cluster lifecycle** | `engenho-bootstrap` (reference impl) | `kikai` as the **operator-side cluster lifecycle daemon** | kikai owns the on-disk state + bringup/teardown ergonomics; engenho-bootstrap is the in-cluster lifecycle hooks. |
| **TLS** | (consume) | `rustls` everywhere | No OpenSSL. |
| **Attestation chain** | `engenho-tameshi-bridge` (per-pod receipt emission) | `tameshi` + `sekiban` + `kensa` | Every pod is a continuously-attested theorem (Pillar 11 + Viggy Method). |

## What engenho deliberately DOES NOT do

- **Reimplement runc.** youki already exists, passes OCI conformance, and is adopted by containerd.
- **Reimplement CNI from scratch.** cilium owns eBPF; engenho's value is in the typed CNI manifest shape, not the dataplane.
- **Use k8s-openapi.** Their macro-driven approach is incompatible with the typescape's `#[derive(TataraDomain)]` pattern. engenho-types is typed-first.
- **Build its own etcd.** SeaORM-backed datastore over sqlite/postgres is the kine pattern; we adopt it.
- **Vendor Go.** Per ENGENHO.md §I, zero Go in the binary.
- **Use OpenSSL.** rustls everywhere; FedRAMP-friendly by construction.

## What engenho's typed surface compounds toward

Each typed primitive in `engenho-types` unlocks:

- **Compile-time admission gates.** A `Pod` with an invalid `image:` field fails to deserialize — operators can't ship it.
- **Typed controller diffs.** Reconciler sees `PodSpec.containers[0].resources.limits.memory` as a typed `Quantity`, not a string.
- **Typed kubelet exec contracts.** kubelet → youki handoff is `OciSpec::from(&pod)`, not string templating.
- **Typed attestation manifests.** `tameshi::Block::from(&pod_apply)` is a Serialize derive, not a JSON template.
- **MCP exposure for free.** Any `KubeResource` becomes mcp__engenho__kube_list-able with zero additional code (P1 of engenho-mcp).

## Conformance plan

CNCF Certified Kubernetes Software Conformance v1.34 — zero
skips — is the M0.4 ship gate per ENGENHO.md §XIII.

| Conformance category | Engenho's path |
|---|---|
| API discovery / OpenAPI | engenho-apiserver serves `/openapi/v3` from the same vendored schemas that generated `engenho-types`. Same source of truth on both sides of the wire. |
| Core resource CRUD (Pod / Service / ConfigMap / Secret / Namespace) | engenho-apiserver + engenho-store. Conformance tests are JSON wire compatibility — engenho-types' `serde::Serialize` impls are the canonical encoder. |
| Watch streams | engenho-watch over SSE; behavior matches etcd's resource-version semantics (kine pattern). |
| Controllers (Deployment / ReplicaSet / StatefulSet / DaemonSet) | engenho-controllers via shigoto. |
| Scheduler (PreFilter / Filter / Score) | engenho-scheduler with typed predicate/priority plugins. |
| kubelet (Pod lifecycle / CRI / exec / port-forward) | engenho-kubelet over youki for OCI, cloud-hypervisor for microVM RuntimeClass, runwasi for WASI RuntimeClass. |
| Auth (RBAC / TokenReview / SubjectAccessReview) | engenho-authn integrates with cofre + saguão for fleet identity; standard RBAC types in engenho-types. |
| Volumes (HostPath / EmptyDir / PVC / CSI) | engenho-csi-bridge; in-tree volume plugins explicitly NOT shipped (k3s lean precedent). |
| Storage (PV / PVC / StorageClass) | engenho-store handles CR side; CSI drivers (longhorn, openebs) handle dataplane. |
| Compliance hardening | sekiban admission + kensa continuous attestation; FedRAMP-High eligible by construction. |

The N-skip = 0 promise lives or dies on whether engenho-apiserver
+ engenho-store produce the same JSON wire bytes as kube-apiserver
+ etcd for every conformance request. Type-driven from a single
vendored OpenAPI source on both client (engenho-types) and server
(engenho-apiserver schema export) makes that promise tractable.

## Phased rollout — what M0.x actually delivers

| Phase | Deliverable | Conformance % |
|---|---|---|
| M0.0 | scaffold + engenho-mcp (shipped) + engenho-cluster-config (shipped) | 0% (no apiserver yet) |
| M0.0.1–M0.0.4 | typed resource catalog (Pod expanded, all 18 kinds typed spec/status) | 0% (still no apiserver; types validated end-to-end) |
| M0.1 | engenho-apiserver + engenho-store; serves `/api/v1/pods` LIST/GET against engenho-store SQLite | ~10% (core LIST/GET) |
| M0.2 | engenho-controllers (Deployment + ReplicaSet via shigoto); engenho-scheduler (default policy); kubelet over youki | ~40% (controllers + scheduler + CRI) |
| M0.3 | sekiban admission integration; cofre secret materialization; kensa attestation gates; CSI bridge | ~60% (compliance + storage) |
| M0.4 | CNCF Certified Kubernetes Software Conformance v1.34, zero skips | 100% |
| M0.5 | engenho-vm-runtime (cloud-hypervisor RuntimeClass); runwasi RuntimeClass for tatara workloads; cilium CNI integration | beyond conformance |

## References — read these next

- **External**:
  - [k3s — Lightweight Kubernetes](https://github.com/k3s-io/k3s) — the precedent we measure against
  - [kine](https://github.com/k3s-io/kine) — etcd shim over SQL; engenho-store mirrors the design
  - [youki](https://github.com/youki-dev/youki) — Rust OCI runtime, OCI-conformance-passing
  - [cloud-hypervisor](https://github.com/cloud-hypervisor/cloud-hypervisor) — Rust VMM, ~50K LoC
  - [cilium](https://github.com/cilium/cilium) — eBPF CNI, dominant in production 2026
  - [kube-rs](https://github.com/kube-rs/kube) — the Rust k8s client we deliberately don't use (macro-driven; engenho-types is typed-first)
  - [Apple VZ](https://developer.apple.com/documentation/virtualization) — engenho's macOS dev path via kasou

- **Pleme-io**:
  - [`pleme-io/shigoto`](https://github.com/pleme-io/shigoto) — typed Dag for controllers + scheduler
  - [`pleme-io/tatara`](https://github.com/pleme-io/tatara) — convergence engine (7 execution drivers); engenho IS a tatara binary per ENGENHO.md §IX
  - [`pleme-io/sekiban`](https://github.com/pleme-io/sekiban) — admission webhook + integrity gates
  - [`pleme-io/kensa`](https://github.com/pleme-io/kensa) — compliance + continuous attestation
  - [`pleme-io/cofre`](https://github.com/pleme-io/cofre) — zero-plaintext secrets
  - [`pleme-io/shikumi`](https://github.com/pleme-io/shikumi) — TieredConfig + hot-reload
  - [`pleme-io/kikai`](https://github.com/pleme-io/kikai) — cluster lifecycle reference (already used)
  - [`pleme-io/kasou`](https://github.com/pleme-io/kasou) — Apple VZ wrapper (already used)

- **Engenho own**:
  - [`ENGENHO.md`](../../theory/ENGENHO.md) — destination doc
  - [`engenho-mcp/`](../engenho-mcp/) — MCP operator surface (shipped 2026-05-21)
  - [`engenho-cluster-config/`](../engenho-cluster-config/) — typed bootstrap config (shipped)
  - [`engenho-types/`](../engenho-types/) — typed resource catalog (M0.0 scaffold; M0.0.2+ in flight)
