# engenho

**A Kubernetes control plane written from scratch in Rust, in one binary.**

API server, scheduler, controllers and kubelet run in a single process.
Real `kubectl` drives it — not a mock, not a shim. It serves 18 API
groups with server-side apply, watch, RBAC and admission, and real
`etcdctl` reads its store over etcd's own gRPC wire protocol.

It is **not** a Kubernetes distribution and it is not certified. It runs
a lot and refuses a lot, and the difference is written down rather than
discovered: the [contract ledger](#contract-ledger--measured-2026-08-30)
below states, per interface, whether it is **SHIPPED**, **PARTIAL**,
**REFUSED** (deliberately, with the reason) or **ABSENT**. Read that
before the prose.

**Why it exists.** Nothing in the ecosystem asks *"do you have etcd?"*.
It asks to `Range` a keyspace, to `exec` on :10250, to scrape `/metrics`.
Those verbs are the contract; what answers them is an implementation
detail. engenho satisfies the interfaces and then does what it likes
underneath — which is the point, and is explained in full under
[the load-bearing insight](#the-load-bearing-insight-interfaces-are-the-contract-technology-is-not).

```
git clone https://github.com/pleme-io/engenho && cd engenho
cargo run --bin engenho -- --help
```

**Status: working, incomplete, and honest about which is which.** CRI,
`/stats/summary` and etcd `Lease` are ABSENT. `kubectl exec` returns a
reasoned 501. ServiceAccount-token authentication has an implementation
with no caller. The ledger names all of it.

> Internal framing, for pleme-io readers: Pangea declares the
> supercontinent's shape; magma realizes it on cloud substrate; engenho
> is the engine that runs the land (terreno). Resource catalog generated
> from upstream OpenAPI v3 via `kube-forge` (Pillar 12). Caixa M2/M3
> slots reconciled natively without Helm. Secrets flow through cofre.
> Pods can be OCI containers OR tatara tlisp programs via `runwasi`. The
> destination doc —
> [`pleme-io/theory/ENGENHO.md`](https://github.com/pleme-io/theory/blob/main/ENGENHO.md)
> — is canonical; read it before touching anything load-bearing.

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
engenho-local   Ready    control-plane,master   2m    v1.34.5+k3s1
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

**Today — MEASURED 2026-08-29 on the live cid cluster, not inferred from this
document.** Re-measure before citing; a status line rots DOWNWARD (it reads as
modest and so never gets flagged as wrong), which is exactly what happened to
the `M0.0` line this replaces.

| Surface | Measured |
|---|---|
| built-in API resources | **52 of 57** upstream built-ins (91%) across 19 groups |
| absent built-ins | `bindings`, `componentstatuses`, `selfsubjectreviews`, `validatingadmissionpolicies`, `validatingadmissionpolicybindings`, and — found 2026-08-29 by parsing a real apiserver's whole keyspace — `networking.k8s.io` **IPAddress** and **ServiceCIDR**. The "52 of 57" figure above counts against a hand-written upstream list that omitted those two, so the catalog gap is WIDER than 5 |
| API machinery | server-side apply (+`managedFields`), WATCH, label selectors, resourceVersion, API **defaulting** and **validation** (core kinds), field selectors incl. `spec.nodeName`/`status.phase` — all working |
| controller chain | Deployment → ReplicaSet → Pod reconciles end to end |
| endpoints serving | `/healthz` `/readyz` `/livez` `/version` (`v1.34.0`) `/api` `/apis` `/openapi/v3` |
| crates | 24 workspace members; `engenho-datastore`, `-cni`, `-kubeproxy`, `-dns`, `-localpath`, `-ca`, `-caixa`, `-mesh`, `-gateway`, `-attest`, `-cli` are TARGET names above and do **not** exist yet |

## The load-bearing insight: interfaces are the contract, technology is not

engenho does not run etcd, and does not intend to. It runs a journalled,
partitioned segment store (`~/.local/share/engenho/store`). That is a
legitimate choice about TECHNOLOGY — and it is not the whole obligation.

**Nothing in the ecosystem asks "do you have etcd?".** It asks to `Range` a
keyspace, to take a snapshot, to be pointed at `--etcd-servers`, to scrape
`/metrics`, to `exec` into a container on :10250. Those verbs are the contract,
and they are load-bearing even when the technology behind them is not. A system
that satisfies them is transparently substitutable for k3s; a system that does
not is a Kubernetes-shaped thing that no existing runbook, backup tool or
dashboard can drive.

So the order is deliberate: **establish the interfaces first, then do what we
like with the technology underneath.** Every façade below is owed to the world;
what answers it is ours.

### Contract ledger — MEASURED 2026-08-30

| Contract | Consumed by | Status |
|---|---|---|
| Kubernetes REST API | kubectl, every controller | **SHIPPED** — 52/57 kinds, SSA + WATCH. ★ one verified WATCH gap (`timeoutSeconds`, now fixed) + an open long-uptime degradation — see *Watch* below |
| `/healthz` `/readyz` `/livez` `/version` | probes, HA | **SHIPPED** |


### ★ Watch: one verified gap, and a mis-attribution worth reading (2026-09-06)

**The gap, verified from source and still true:** `?timeoutSeconds=N` was parsed
into `ListWatchParams` and read nowhere — unlike `limit` and `continue`, it had
no typed accessor. A client asking the server to close the watch after N seconds
was silently ignored. Now honoured (`ListWatchParams::timeout()` plus a
per-poll deadline in the watch `unfold` that ENDS the stream cleanly), covered by
`watch_timeout_seconds_ends_the_stream_cleanly` and a negative control, with a
red-run receipt: forcing `deadline: None` fails the positive test and leaves the
control green.

**★ The mis-attribution, recorded because the reasoning error is more useful than
the fix.** That gap was found while diagnosing a real symptom: `pangea-operator`
logging ~28 `WatchFailed` per hour across all 12 of its controllers. The two were
connected by a story that sounded airtight — the server never closes the watch,
so the client's own read timeout fires — and the story was WRONG.

What the per-minute timeline actually showed, once plotted instead of summed:

    07:23   4      steady churn: exactly 4 every 5 minutes
    07:28   4
    07:33   4
    07:35  20      <- engenho restarting (the fix deploying)
    07:36..07:52   ZERO, for 17 minutes
    07:53  16      <- engenho restarting (the revert deploying)
    07:54   4
    07:55..08:06   ZERO, for 16 minutes — on PRE-FIX engenho

The decisive control: after reverting to pre-fix engenho — binary identity
confirmed by the absence of the fix's marker string, not by the flake pin — the
churn did **not** return in 16 minutes, where the pre-fix rate predicts ~13.

**So the cure was the RESTART, not the fix.** The churn is a degraded state that
a long-running engenho accumulates, and its cause is UNKNOWN. It is not
`timeoutSeconds`.

Three errors compounded, each worth naming:

1. **A restart artifact read as steady state.** Both measurement windows
   contained a deploy. The tell was in the number — 20 and 16 are both about the
   controller count, i.e. one failed watch per controller at reconnect — and
   summing hid it. Plotting per-minute made it obvious instantly.
2. **A verified fact carrying an unverified claim.** "`timeoutSeconds` is parsed
   and never read" is true and checkable in the source. "…and that is why the
   watches churn" is a different claim needing different evidence, and it
   inherited the first one's confidence.
3. **A confident retraction that was also wrong.** On the contaminated numbers
   this file previously said the fix made things 4x worse and must not be
   deployed. That was as unfounded as the original claim, in the opposite
   direction — a correction is not automatically more reliable than what it
   corrects.

**Consequently unresolved, stated so nobody inherits a false answer:** whether
`close_notify` is genuinely missing. `UnexpectedEof` — *"peer closed connection
without sending TLS close_notify"* — was observed only inside restart spikes, and
a restarting server closes connections uncleanly whether or not the watch path
does. `rg close_notify` returns nothing across the tree, so the gap is plausible;
it is not demonstrated.

**The open question is the useful one:** what does a long-running engenho
accumulate that makes ~12 watches fail every 5 minutes, and that a restart
clears? Reproduce by leaving a controller attached for hours, then plot
`WatchFailed` per minute — do not sum across a restart.

| OpenAPI v3 | generators, modern clients | **SHIPPED** |
| OpenAPI v2 | older clients, codegen | **ABSENT** |
| etcd v3 gRPC — reads | `etcdctl get`, backup/DR inspection | **SHIPPED (read path)** — `KV.Range` served over tonic: point reads, prefix scans, intervals, `limit`/`more`/`count`, `keys_only`. Wire types generated from upstream protos; `/registry` bijection verified against a real apiserver's 699-key keyspace |
| etcd v3 gRPC — writes | `--etcd-servers` (a real kube-apiserver) | **REFUSED BY DESIGN** — `Put`/`Txn`/`DeleteRange`/`Compact` return a typed `Unimplemented` naming the reason: writes go through engenho's apiserver, which owns admission, defaulting and validation. Serving them raw would let a client store an object no apiserver would have admitted. The store prerequisites (multi-key `Txn`, point-in-time reads, explicit `Compact`) are landed and tested, so opening the write path is a policy decision, not missing machinery |
| etcd v3 gRPC — `Maintenance` | `etcdctl endpoint status/health` | **SHIPPED** — `Status`/`Alarm`/`Defragment` served. `Snapshot` REFUSED: `etcdctl snapshot save` writes whatever it receives and reports success, so streaming non-restorable bytes hands an operator a backup that fails only during a disaster |
| etcd v3 gRPC — `Watch` | `etcdctl watch`, cache-building clients | **SHIPPED** — multiplexed bidi streaming: many watches per gRPC stream, server-assigned ids, subscribe-before-replay, live event fan-out, cancel ack, progress requests, and a compaction cancel carrying `compact_revision`. A store that cannot subscribe gets the watch REFUSED, never history-then-silence |
| etcd v3 gRPC — `Lease` | lease-holding clients | **ABSENT** — traits generated, not implemented |
| kubelet API :10250 — logs/pods | `kubectl logs`, node inspection | **SHIPPED (routes)** — `/containerLogs/{ns}/{pod}/{container}`, `/pods`, `/runningpods/`, `/healthz` on upstream's exact paths and camelCase query spelling. Not yet bound to a listening socket by the runtime |
| kubelet API :10250 — exec/attach/portForward | `kubectl exec/cp/port-forward` | **REFUSED (501, reasoned)** — needs SPDY or WebSocket stream multiplexing; a half-implemented stream protocol hangs a client rather than failing it |
| kubelet `/stats/summary`, `/metrics/*` | metrics-server, HPA | **ABSENT** |
| Event recording | every "why did this fail" question | **PARTIAL** — typed recorder + upstream reason vocabulary landed; emission sites (kubelet/scheduler/controllers) pending |
| `/metrics` (Prometheus) | monitoring, HPA | **SHIPPED** — Prometheus text exposition, served at `/metrics` |
| CRI (`runtime.v1`) | runtime substitutability | **ABSENT** — drives podman directly |
| CNI / CSI | network + storage plugins | **PARTIAL** — `engenho-cni` and `engenho-csi` implement both plugin contracts (CNI file+exec+JSON with chain ordering and `prevResult` threading; CSI Identity/Controller/Node over gRPC with the two-socket registration handshake), each differentially tested against a foreign reference plugin. Both differentials found real defects. The kubelet reaches CSI through a materializer with refusing defaults; the CNI side computes but does not install on darwin. Corrected 2026-08-30 — this row read ABSENT |
| Validating admission webhooks | policy engines (Kyverno, Gatekeeper) | **SHIPPED** — run after mutation, may not mutate |
| ServiceAccount tokens | every in-cluster client | **ABSENT (implementation exists, nothing calls it)** — `sa_token.rs` is 397 lines of ed25519 issue/verify with 11 passing tests and **zero non-test callers**; `authn.rs` still answers any SA-shaped bearer with `ServiceAccountUnsupported` → 401, there is no `serviceaccounts/token` subresource, and nothing projects a token into a pod. Corrected 2026-08-30 — this row read SHIPPED, which counted the code existing as the contract being met |
| CRD conversion | multi-version CRDs | **SHIPPED** — `None` strategy relabels; a webhook strategy fails the read rather than falling back |
| Audit logging | compliance, incident response | **SHIPPED** — upstream's four levels; secrets capped at `Metadata` by a first-match rule |
| API validation (core kinds) | every client, every controller below it | **SHIPPED** — Pod + Service |
| Scheduler placement rules | HA guarantees an operator writes | **SHIPPED** — cordon, `nodeSelector`, `nodeName`, taints/tolerations, pod affinity/anti-affinity, topology spread. **Preemption absent** |
| DNS SRV | port-discovering clients (StatefulSet peers, etcd, Kafka) | **SHIPPED** — derived from a Service's named ports |
| `CrashLoopBackOff` | `kubectl get pods`, every alerting rule | **SHIPPED** (decision function; not yet called from the sync loop) |
| Node lease heartbeats | NotReady detection, autoscalers | **SHIPPED** (readiness derived; renew loop not yet wired) |

Ports measured on the live node: only `6443` listens. `10250` (kubelet),
`10248`, `10256`, `10257`, `10259`, `2379`/`2381` (etcd) are all closed.

## Wire compatibility (theory/ENGENHO.md §III)

**Status column added 2026-08-29 — the rows below are the TARGET wire
contracts; only the first is realised.**

| Surface | Format | Source of truth | Status |
|---|---|---|---|
| Kubernetes REST API | HTTPS :6443; JSON / protobuf; v1.34 schema | upstream `kubernetes/api/openapi-spec/v3/` | **SHIPPED** |
| etcd v3 gRPC | Range/Put/Txn/Watch/Lease over UDS | upstream `etcd-io/etcd@v3.5/api/etcdserverpb/` | **PARTIAL** — `KV.Range` served; writes refused by design; Watch/Lease/Maintenance absent |
| CRI v1 | gRPC client to containerd / youki | upstream `kubernetes/cri-api/v1` | **ABSENT** (podman driven directly) |
| CNI v1 | stdin JSON + ADD/DEL/CHECK/VERSION | `containernetworking/cni` spec | **ABSENT** |
| Gateway API | v1.1 GA — GatewayClass/Gateway/HTTPRoute (M3+) | sigs.k8s.io/gateway-api | **ABSENT** |

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
