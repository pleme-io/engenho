# engenho M0→M5 ROADMAP — the complete, distributed k3s replacement

> **Status:** canonical execution spec (2026-06-06). The destination + theory
> live in [`theory/ENGENHO.md`](../theory/ENGENHO.md); this file is the
> **path** — the dependency-ordered, gate-checked build order an operator +
> agents execute against. `src/main.rs` points here. Resume by finding the
> first phase whose exit-gate is not green and continuing from its first
> un-shipped deliverable.

## Thesis — invert k3s, don't port it

engenho replaces k3s not by porting Go but by inverting it:
- **GENERATION-DRIVEN** — every K8s kind is `#[derive(KubeResource, TataraDomain)]`
  mechanically emitted from upstream OpenAPI v3 by `kube-forge`. Hand-authoring a
  kind is a CI-rejected anti-pattern. ~15× LoC advantage over vendored Go.
- **TYPED + ATTESTED** — bad states unrepresentable; tameshi/sekiban/kensa at
  admission; cofre for secrets (zero plaintext k8s Secrets).
- **RUST-NATIVE, ONE BINARY** — ~30–50 MB, no vendored Go, no kine, no external
  etcd; kubectl/etcd-v3/CRI/CNI/Gateway **wire-compatible**; every subsystem a
  daemon supervised under tatara `defguest`. Pods are OCI containers OR
  tlisp/wasm programs (runwasi RuntimeClass).
- **DISTRIBUTED-BY-DESIGN** — engenho-revoada is a four-layer attested fabric;
  Kubernetes is ONE face of it. This is the axis where engenho **surpasses** k3s.

**Ship gate:** CNCF Certified Kubernetes Software Conformance on v1.34, **zero
skips** (theory §XIII). No engenho artifact promotes past staging without it.

## The surpass-k3s axis — revoada (already the most mature subsystem)

k3s HA = one embedded/external etcd + bolted-on leader election; static topology,
manual role changes, no attestation, partition recovery = log archaeology.
revoada is a typed, attested, policy-driven fabric — **and Layers A/B/D ship today
with real libraries, composed (not stubbed):**
- **Layer A — Membership** ✅ (chitchat gossip + phi-accrual): join/leave without
  touching consensus; `MembershipView` is a `watch::Receiver` stream.
- **Layer B — Consensus** ✅ (openraft, a *separate* Raft group from the K8s data
  store) applying a CLOSED typed `RoleAssignment` enum (Promote/Demote/Quarantine/
  Restore) via joint consensus — not free-form blobs.
- **Layer C — Content** (typed surface shipped; **iroh P2P wiring = the R5 gap**):
  content-addressed Pod/ConfigMap/image distribution → apiserver stops being the
  read bottleneck; degraded-read during partition.
- **Layer D — Attestation** ✅ (real BLAKE3 + ed25519): every role transition is a
  hash-linked signed block — "who held control-plane" is provable.

## Keystone de-risked (2026-06-06)

`kube-forge` is **a forge-gen sibling backend, not a new framework** — verified:
forge-gen exposes a `Backend` trait with ~7 sibling backends already
(terraform/crossplane/pulumi/helm/ansible/compliance/iac). `kube-forge` is the
8th, semantic domain = K8s OpenAPI → typed Rust (pure type→type, no auth/CRUD).
This collapses M0.0 from "build a codegen framework" to "add a backend."

## Honest current state (M0.0 seed)

| Subsystem | Maturity | Gap |
|---|---|---|
| **revoada** (distribution) | **most mature** — R0–R4.5 composed, 13k LoC, real libs, proptested | iroh content layer (R5); bridge to apiserver |
| apiserver | M0.1 seed — full CRUD + OpenAPI v3, 8 r7 tests | watch streaming, authn, RBAC enforce, admission dispatch, CRD, SSA/SMP, API-group routing |
| datastore | M0.1 seed — openraft + 3-node replication proven (r6) | **in-memory only** (no fjall persistence, no etcd-v3 gRPC, no revision-MVCC) |
| kubelet + CRI | **real kubelet — pods actually RUN (2026-06-08)** — `ContainerRuntime` trait (Podman/Fake) drives `podman run -d` per `spec.containers[i]` with the deterministic name `<ns>_<pod>_<cname>`; **MULTI-CONTAINER** (one `start` per container, N stops + N removes on delete); **real status** via a typed-spec triplet — closed `PodPhase`/`ContainerState`/`RestartPolicy` enums (the typed border) + a PURE `reconcile_pod_phase(restart_policy, &[ContainerObservation]) -> (PodPhase, Vec<ContainerStatusOut>)` interpreter (proptested: Always anti-latch, Never terminal-latch, any-Waiting→Pending) + `FakeBackend` as the mock environment — Pending→Running→Succeeded/Failed fold, per-container `containerStatuses[]` (running/waiting/terminated + restartCount), `podIP`, Ready condition; **`kubectl logs`** (real container stdout, `-c <container>` selector, `--tail`) via a new `ContainerRuntime::logs` + the `/log` subresource (catalog-declared on Pod, router-dispatched to the in-process kubelet through the apiserver `PodLogReader` seam); **restartPolicy Always + Never + OnFailure** (a terminated container is re-`start`ed under the policy, pod stays Running, restartCount bumps); delete = stop+remove ALL containers + RS self-heal (RS index-collision fix: recreate the FREED index, never clobber a surviving pod); fake backend stays config-selectable (`kubelet_backend: fake`); **probes — liveness/readiness/startup (2026-06-08)** — a typed-spec triplet in `probe.rs` (typed border `ProbeKind`/`ProbeHandler`/`ProbeTiming`/`ProbeSpec` + `ProbeRuntime` counters; PURE `fold_probe_observation` + `aggregate_container_readiness` interpreters; `ProbeSpec::from_k8s` parser — no-handler/grpc/unresolved-port are typed errors, NEVER a fake pass) driven by TWO mockable seams (the `ContainerRuntime::exec` extension for exec-probes + a new `NetProber` trait for httpGet/tcpSocket against the pod IP, each with a Fake); readiness re-sources `containerStatuses[].ready` (replacing the hard-`true`); `build_pod_status` emits the standard `ContainersReady`+`Ready` condition pair; liveness/startup drive restart via the shared `restart_container` (stop→remove→start, restartPolicy:Never suppresses liveness restart); startup gates liveness + forces readiness false during the boot window; `tick()` returns `Requeue{next-probe-due}` so probes run on `periodSeconds` (a no-probe pod arms NO requeue + is Ready 1/1 immediately — behavior-preserving); live real-podman PROBE_BAR green (readiness exec flips Ready, liveness exec restarts, startup gates liveness, no-probe unchanged; tcp/http proven via FakeNetProber since the macOS podman-VM pod IP is not host-routable) — all apiserver/daemon flows + the live real-podman POD_BAR green | real containerd CRI client; volumes (hostPath/Secret/ConfigMap/PVC); `/exec` `/portforward` (WebSocket-v5); cgroup-v2 limit enforcement; init/ephemeral containers; lifecycle hooks; grpc-probe handler (typed-deferred); httpGet/tcpSocket LIVE on a host-routable cluster |
| types catalog | **typed emission DONE (2026-06-06)** — 18 cataloged kinds GENERATED typed (typed spec/status + globally-deduped shared sub-struct module + curated-enum overrides) by in-tree `engenho-kube-codegen`; `--check` deterministic; zero hand-authored kinds | expand 18 → ~150 kinds across ~16 API groups (mechanical: add `KIND_CATALOG` rows + vendor the group's OpenAPI) |
| controllers + scheduler | isolated library seeds (admission trait, crd scaffold) | 18-controller set, scheduler profile, apiserver-driven reconcile |
| **networking** (CNI/DNS/kube-proxy/local-path/CA) | **M0.3 foundation + cluster-DNS bricks landed** — pods get real IPs on a shared `engenho-net` podman network, kubelet records `status.podIP`, EndpointsController populates Service Endpoints; **cluster-DNS now works headless-style** — at pod-start the kubelet computes the Services whose selector matches the pod (reusing the EndpointsController selector predicate) + feeds podman `--network-alias <svc>/<svc>.<ns>/<svc>.<ns>.svc.cluster.local`, so aardvark-dns (already running for the user network) resolves Service names to backend pod IPs (multi-A, no ClusterIP/kube-proxy) — ignore-gated real-podman e2e green: 2-replica Deployment → real IPs → Endpoints carry them → pod-to-pod TCP reachability → a separate client container resolves the Service name + connects to a backend BY NAME; the `dns.rs` zone-file/clusterIP controller stays a typed-surface seed for M0.4 | ClusterIP allocation + real kube-proxy apply, CNI bridge/IPAM, the M0.4 `engenho-dns` authority (hickory + KubernetesAuthority owning ClusterIP A-records + SRV + start-time-alias limitation removal), local-path, cluster CA (the M0.4 greenfield) |
| binary + supervision | M0.0 — `main.rs` prints + exits | tatara `defguest` surgery; subsystem assembly |

## Critical path (what unblocks what)

```
engenho-kube-codegen (18 kinds typed ✅) ──► ~150 typed kinds ──► everything downstream
   └─ datastore (fjall+etcd-v3+MVCC) ──► apiserver (WATCH + RBAC + admission + SSA)
        └─ controllers + scheduler ──► kubelet + CRI (pods RUN)
             └─ networking (CNI/DNS/kube-proxy/local-path/CA) ──► single-node complete
                  └─ revoada multi-node (iroh R5 + apiserver bridge) ──► HA, surpass-k3s
                       └─ caixa-native + mesh + compliance ──► CNCF conformance (M4 gate)
```
**Watch dispatch is where every k8s clone dies** (revision-coherent listwatch,
bookmarks, compact semantics). Treat it as the program's highest-risk feature.

## Phased roadmap (each phase has a HARD exit gate)

### M0.0 — Generation pipeline *(the force-multiplier)* — **typed emission DONE (2026-06-06)**
The generator is the in-tree `engenho-kube-codegen` (a separate forge-gen
`Backend`-trait sibling `kube-forge` can still be extracted later if cross-repo
reuse is wanted — not needed to ship M0.0). What actually landed:
- ✅ **Typed-emission engine** — OpenAPI property→`RustType` mapper (`types.rs`),
  struct emitter + transitive `$ref` closure (`emit_typed.rs`), globally-deduped
  **shared sub-struct module** (`generated_v1_34/types.rs`) re-exported per group
  so one canonical type is referenced everywhere, acronym + Rust-keyword field
  handling (`podIP`→`pod_ip`, `type`→`r#type`), `Json` fallback for exotic shapes
  (`x-kubernetes-int-or-string`/`anyOf`) → output always compiles.
- ✅ **Curated-enum overrides** — prose-only enums (no upstream `enum` array):
  `PodPhase`, `SecretType`/`KnownSecretType` live hand-authored in
  `engenho_types::curated_enums`, referenced via `emit_typed::FIELD_OVERRIDES`.
  Add a row + an enum to promote any other prose-only-enum field.
- ✅ All 18 cataloged kinds GENERATED typed; hand-authored `_spec` modules
  deleted; 147 workspace test groups green; `--check` determinism exit-0;
  consumers (engenho-mcp / -fonte / -kube-client) adapted to the typed shapes.
- ⬜ **Remaining (mechanical):** expand 18 → ~150 kinds across ~16 API groups —
  add `KIND_CATALOG` rows + vendor each group's OpenAPI; `arch-synthesizer::k8s`
  re-export.
- **Superseded framing:** regeneration REPLACES the hand-authoring (the generated
  tree IS canonical) rather than byte-reproducing a hand-authored bullseye — the
  earlier "regenerate Pod byte-identical" plan is moot now that generation owns
  the tree.
- **GATE:** L0–L3 green; `--check` deterministic; zero hand-authored kinds. ✅ met
  for the 18 cataloged kinds; re-asserted automatically as the catalog grows.

### M0.1 — Datastore + apiserver *(the load-bearing milestone; ~10–14 wk)*
- datastore: fjall persistence + revision-MVCC + crash recovery + snapshot; etcd-v3
  gRPC (Range/Put/DeleteRange/Txn/Compact/Watch/Lease/Maintenance) over tonic.
- apiserver: **HTTP watch streaming** (WebSocket-v5 + chunked, resourceVersion
  resume, bookmarks) — the single highest-value feature.
- authn middleware (JWT-SA/bearer/client-cert/bootstrap-token); RBAC evaluator on
  typed `(Verb,Group,Resource,Name)` + enforcement + SubjectAccessReview.
- SSA + JSONPatch + StrategicMergePatch (BTreeMap-everywhere → byte-deterministic,
  proptested associativity); CRD machinery + dynamic dispatch; admission dispatch.
- `engenho-test` in-process cluster spawner (replaces kind/k3d for fleet tests).
- **GATE:** L0–L4 green; `kubectl version/get/apply/auth-can-i` + watch/SSA proptests;
  Sonobuoy pass-rate climbing monotonically per PR.

### M0.2 — Controllers + scheduler *(~8–10 wk)*
- 18 built-in controllers, each `#[derive(TataraController)]` over a shared
  `shigoto::Dag` (workqueue + retry + backoff); leader election via Lease.
- 11-plugin scheduler profile (Filter/Score/Reserve/Permit/Bind), pure function of
  `(snapshot, candidate-pod)`, proptested deterministic.
- **GATE:** Deployment→ReplicaSet→Pods→nodeName; Job→pending Pod; RBAC binding e2e.

### M0.3 — Kubelet + CRI *(pods RUN; ~10–14 wk)*
- **DONE (2026-06-08):** pods actually RUN via `PodmanBackend` (single + MULTI
  container, one `start` per `spec.containers[i]`); real status (Pending→Running→
  Succeeded/Failed via the typed `PodPhase` fold + per-container
  `containerStatuses`, `podIP`, Ready); `kubectl logs` (real stdout, `-c`
  selector, `--tail`); `kubectl delete` (stop+remove all containers + RS
  self-heal); `RestartPolicy` Always + Never + OnFailure; container
  command/args/env. **GATE met:** real-podman POD_BAR (2-replica busybox
  Deployment → 2 Running pods with real podIPs → `kubectl logs` → delete-pod
  self-heal → delete-deployment clean → 2-container Pod) + fake-backend
  regression.
- **DEFERRED (typed-error / documented-no-op, NEVER a fake Running):** CRI v1
  gRPC client to containerd (tonic); evented PLEG; volumes (hostPath +
  Secret-via-cofre + ConfigMap + projected SA-token + PVC→PV); probes
  (http/tcp/exec — Ready reflects container-running only); initContainers +
  ephemeral containers; lifecycle hooks; `/exec` `/portforward` (WebSocket-v5,
  typed `NotFound`); cgroup-v2 limit enforcement via rustix; eviction; image
  pull + imagePullSecrets.
- **GATE:** real-pod e2e; `kubectl logs` **(MET)**; `kubectl exec/port-forward`
  (deferred); Pending→Running→Ready→Terminated **(MET for the run/restart
  lifecycle; probes-driven Ready deferred)**.

### M0.4 — Data-plane networking *(single-node complete; rio-ready; ~6–8 wk)*
- `engenho-cni` (bridge + host-local IPAM + portmap); `engenho-kubeproxy`
  (pure-Rust nftables; ClusterIP + NodePort); `engenho-dns` (hickory-dns +
  KubernetesAuthority, drop-in CoreDNS); `engenho-localpath`; `engenho-ca` (rcgen
  CA + CSR API/signer/approver).
- tatara `defguest` surgery landed; binary assembled (every subsystem a supervised
  daemon); `engenho doctor` pre-boot L4 self-check.
- **GATE:** L0–L5 green; two pods talk via ClusterIP; NodePort from host; PVC binds;
  in-cluster DNS resolves; **engenho runs on rio bare-metal NixOS under tatara**.

### M0.5 / M1 — Multi-node HA via revoada *(the surpass-k3s milestone)*
- revoada R5: wire iroh P2P content sync (Pod/ConfigMap/image); apiserver stops
  being read bottleneck; degraded-read during partition.
- bridge revoada↔apiserver: `KubernetesFace` linking the RoleAssignment Raft
  (control-plane membership) to the engenho-store data Raft (K8s CRUD).
- CSR bootstrap join (`engenho join`); VXLAN overlay + ServiceLB (klipper-equiv);
  HA datastore (openraft+fjall opt-in); revoada R4.5 witness co-signatures (saguão
  passaporte identity); **Jepsen partition-resilience proof (no split-brain).**
- **GATE:** L5 multi-node + HA; 3-node bootstrap; cross-node pod-to-pod via VXLAN;
  kill leader → follower takes over <30s; Jepsen green.

### M2–M3 — Caixa-native + mesh + compliance + Gateway
- `engenho-caixa`: native CaixaSpec CRD reconcilers (Biblioteca/Binário/Serviço/
  Supervisor/Aplicação) emitting typed Deployment/Service/NetworkPolicy DIRECTLY
  (collapse caixa→pod from 5–6 steps to 1).
- `engenho-mesh`: Aplicação `:contratos` WIT-contract enforcement at admission;
  `engenho-gateway` (pingora, Gateway API v1.1).
- `engenho-attest`: in-process sekiban+kensa admission webhook; tameshi receipt per
  Pod create; image-pull seal validation; FedRAMP-Moderate/SOC-2/CIS-K8s profiles.
- controllers-as-tatara-programs (`defguest` daemon mode); CRDs via
  `#[derive(TataraDomain, KubeCustomResource)]`.
- **GATE:** apply CaixaSpec→typed Deployment; conflicting `:contratos`→admission
  denial; non-compliant Pod denied with typed evidence.

### M4 — CNCF Certified *(THE ship gate)* + M5 — programs-as-RuntimeClass
- Sonobuoy `--mode=certified-conformance` **zero skips** on a real 3-node cluster;
  fix watch/RBAC/discovery edge cases; PR to `cncf/k8s-conformance`.
- ship-gate workflow (generated from pleme-actions) spins a 3-node kasou+kikai
  cluster, asserts `skips==0 && failures==0`, tameshi-receipts + lacre-signs.
- M5: runwasi RuntimeClass — `runtimeClassName=tlisp` runs as WASI in wasmtime,
  seeing the cluster's typed env.
- **GATE:** zero skips; CNCF submission accepted — **first non-Go certified
  distribution**; engenho promoted to fleet default; rio cutover complete.

## Generation strategy (the highest-leverage investment)
Extend forge-gen — do NOT build a new generator. Package `kube-forge` as a sibling
backend so the K8s-OpenAPI domain (pure type→type) doesn't force-fit iac-forge's
provider shape. Once `kube-forge --check` is byte-identical on Pod, the remaining
~149 kinds are mechanical (marginal cost → near-zero). This single investment is
what makes the ~15× LoC advantage real.

## Conformance strategy (the test pyramid → the M4 gate)
Build bottom-up as a hard gate ladder: **L0** type proofs (compiler is the
verifier) · **L1** per-crate unit + insta snapshots · **L2** proptest on every
load-bearing pure fn (SSA associativity, watch revision-order, RBAC monotonicity,
scheduler determinism, pod-worker termination) · **L3** wire-contract replay of
vendored attested upstream fixtures (etcd-v3, CRI v1, kubectl) · **L4** in-process
cluster e2e · **L5** multi-node + Jepsen · **L6** Sonobuoy certified, zero skips.

## Honest timeline + top risks
**~12–15 calendar months / 18–24 person-months at full focus** (one operator +
agents). Per-phase: M0.0 3–4wk · M0.1 10–14wk (watch/SSA each ~3wk, hardest) ·
M0.2 8–10wk · M0.3 10–14wk · M0.4 6–8wk · M0.5/M1 12–16wk · M2–M3 ~24wk · M4 4–8wk.
**Risks:** (1) watch dispatch correctness — where every clone dies; (2) serial
bottleneck through apiserver-watch→kubelet-CRI (~24wk, hard to parallelize); (3)
CRI-via-containerd reintroduces Go until the M3+ youki Rust shim; (4) conformance
is a moving target (~4-mo cadence) — the bump is mechanical but real; (5) iroh
content layer is typed-surface-only today; (6) SSA strategic-merge KEY semantics;
(7) no proven prior art for a production pure-Rust distribution.

## How to resume this build
1. Find the first phase above whose **exit gate is not green**.
2. Within it, take the first un-shipped deliverable (dependency-ordered).
3. Build it as ONE typed crate / PR; gate it on its L0–L6 ladder rung; commit only
   when its tests are green ("core and test, stack what works" — no bolt-ons).
4. Update the current-state matrix + the gate status here in the same commit
   (models stay current).
