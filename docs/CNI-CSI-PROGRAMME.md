# CNI + CSI — the plugin-contract programme

**Status: IMPLEMENTED, 2026-08-30 — with one honest limit, stated in
"What is NOT done" at the end.** The measurements below were taken before
the work; they are kept as the starting position because they explain why
each piece is shaped the way it is.

## Why these two, and why now

The contract programme (`radiant-kindling-stonebraker`) closed the ring of
*peripheral* contracts: etcd v3 on :2379, the kubelet API on :10250, Events,
`/metrics`. Those make engenho drivable by existing tooling — etcdctl, kubectl,
a dashboard, a backup job.

CNI and CSI are a different class. They are the two **plugin contracts** — the
seams where the wider ecosystem ships *its own binaries and daemons* and expects
the kubelet to call them. Every storage vendor ships a CSI driver; every network
vendor ships a CNI plugin. Satisfy the seam and Cilium, Calico, Longhorn,
local-path-provisioner, the AWS EBS driver and ~150 other drivers work against
engenho unmodified. Skip it and engenho can only ever run what engenho itself
implements — which is the opposite of the operating thesis.

The same load-bearing property as etcd: **the interface outlives the
technology.** A CSI `NodePublishVolume` call is a stable contract regardless of
what engenho does with the bytes underneath.

---

## Measured starting position

### Storage — a runtime with no producer

| piece | state |
|---|---|
| `PvBinderController` | **live** — static bind + local-path dynamic provisioning, 986 lines, driven from `engenho-runtime` |
| `PodVolumeSource` | **live** — `configMap`, `secret`, `emptyDir`, `hostPath`, PVC→node-local hostPath |
| `downwardAPI`, `projected` | typed-deferred, named refusals |
| `VolumeRuntime` trait + `FakeVolumeBackend` + `HostPathVolumeBackend` | **declared, zero consumers** |
| `CSIDriver`, `CSINode`, `VolumeAttachment`, `CSIStorageCapacity`, `StorageClass` | stored + watchable; **no controller reads any of them** |
| CSI gRPC | absent — no protos, no client, no registration socket |

`engenho-kubelet/src/volume.rs` names `CsiVolumeBackend (R13b — gRPC to CSI
plugins)` in its header as future work. Verified today:

```
$ grep -rn 'VolumeRuntime' --include=*.rs . | grep -v volume.rs
engenho-substrate/src/derivation.rs:199:  (a doc comment)
engenho-kubelet/src/lib.rs:77:            (the re-export)
```

Zero non-test consumers. **This is instance #7 of the "type + backend + no
producer" pattern** — the trait exists, two backends exist, every test passes,
and nothing calls it. The kubelet's real volume path goes through
`pod_volume::resolve_pod_volumes` and never touches `VolumeRuntime`.

### Networking — an enforcer with no producer, and no CNI at all

| piece | state |
|---|---|
| pod IP | comes from **podman's own shared named network**, parsed out of `podman inspect` (`backend.rs:978`). Not allocated by engenho, not a CNI result. |
| `ServiceRouter` + `DatapathInstall::{Computed,Installed}` | **live and honest** — kube-proxy rules are computed and observable; `Computed` on darwin means no kernel rule is installed |
| `NetworkPolicyEnforcer` + `FakeNetworkPolicyEnforcer` + `CiliumNetworkPolicyAdapter` | **declared, zero consumers** — instance #8 |
| CNI spec | **absent entirely** — no `/etc/cni/net.d` reader, no plugin exec, no `ADD`/`DEL`/`CHECK`, no `CNI_*` env contract |
| `CniChoice::{Flannel,Calico,Cilium}` in `engenho-cluster-config` | **not engenho's CNI** — it renders *k3s* flags and install manifests. Easy to misread; it is config for someone else's cluster. |

The consequence, stated plainly: a `NetworkPolicy` object applies successfully
today and enforces nothing, with no condition saying so. That is the exact
pattern Phase 4 of the previous programme was written to eliminate.

---

## The honest constraint that shapes everything below

**engenho runs on macOS baremetal, with containers in podman.** CNI is a Linux
contract — it manipulates netns, veth pairs, iptables. There is no netns to hand
a plugin on darwin. CSI has no such barrier: it is gRPC over a unix socket plus
a mount, and a CSI *driver* is free to be a darwin binary.

So the two are not symmetric, and pretending otherwise would produce a plan that
cannot land:

- **CSI is buildable end-to-end today, on this machine.** Phase A.
- **CNI's plugin-invocation half is Linux-only.** The darwin arm gets the same
  treatment `ServiceRouter` already has — compute the result, install nothing,
  say which one happened in a typed field. Phase B builds the whole contract and
  a `Computed` darwin backend; the `Installed` arm proves out on rio.

`DatapathInstall` is the precedent to copy, not to reinvent. It is the one place
this codebase already got "we can't do the kernel half here" right.

---

## Phase A — CSI (buildable now, ~2–3 weeks)

### A.1 — the protos, vendored

`container-storage-interface/spec` `csi.proto` at v1.9.0, vendored under
`engenho-csi/vendor/proto/` with a header naming source + rev + date, built with
protox (no protoc), mirroring `engenho-kube-proto/build.rs`. Same discipline as
the etcd protos: **fetched, never reconstructed.**

Three services: `Identity`, `Controller`, `Node`.

### A.2 — plugin registration (the kubelet seam)

The contract a driver actually expects on startup:

1. Driver creates its socket at `<kubelet-root>/plugins/<driver>/csi.sock`.
2. Driver creates a registration socket at
   `<kubelet-root>/plugins_registry/<driver>-reg.sock` serving
   `pluginregistration.v1.Registration`.
3. Kubelet watches `plugins_registry/`, calls `GetInfo`, dials the driver,
   calls `Identity.GetPluginInfo` + `GetPluginCapabilities` +
   `Node.NodeGetInfo`, then **creates/updates the `CSINode` object**.

That last step is the observable one — `kubectl get csinode` is how an operator
checks a driver registered. It is also a *store write from the kubelet*, which
the kubelet already does for node status and leases, so the path exists.

`pluginregistration.proto` vendors alongside csi.proto.

### A.3 — the node path: `VolumeRuntime`'s missing producer

Wire `PodVolumeSource::PersistentVolumeClaim` to route on the bound PV's source:

- PV has `hostPath`/`local` → today's behavior, unchanged (byte-identical argv).
- PV has `csi` → `NodeStageVolume` (if the driver declares
  `STAGE_UNSTAGE_VOLUME`) then `NodePublishVolume` at the pod's mount dir, then
  a `MountSource::HostDir` pointing at it.
- Teardown: `NodeUnpublishVolume` / `NodeUnstageVolume` on pod delete, in the
  same place `remove_empty_dir` already runs.

This is where `VolumeRuntime` finally gets its producer — and the `CsiVolumeBackend`
its header has named since R13 gets written. **Do not add a second trait.**

### A.4 — the controller path

`CsiAttachController` (`VolumeAttachment` → `ControllerPublishVolume`) and
extending `PvBinderController` to call `CreateVolume`/`DeleteVolume` for
StorageClasses whose `provisioner` names a registered CSI driver. Static bind and
local-path provisioning stay exactly as they are — CSI is a third branch, not a
replacement.

`WaitForFirstConsumer` becomes implementable here and should be closed in the
same phase; it is currently typed-deferred and CSI drivers depend on it heavily
(topology-aware provisioning is the normal case for EBS/GCE).

### A.5 — the payoff test

Run a **real, unmodified CSI driver** against engenho. The right first target is
`csi-driver-host-path` (the CSI project's own reference driver): no cloud
account, no kernel module, and it is what the CSI ecosystem itself tests with.

Success = `kubectl get csinode` shows it, a PVC with its StorageClass provisions,
a pod mounts it, data survives pod restart, delete reclaims it.

That is an oracle in the Tier-B sense: upstream's own reference implementation
judging engenho's contract.

---

## Phase B — CNI (~3–4 weeks, split by platform)

### B.1 — the spec, typed

CNI 1.0.0 is a *file + exec + JSON* contract, not gRPC:

- Read `/etc/cni/net.d/*.conflist` in lexical order; first wins.
- For each plugin in the chain, exec `<cni-bin-dir>/<type>` with `CNI_COMMAND`,
  `CNI_CONTAINERID`, `CNI_NETNS`, `CNI_IFNAME`, `CNI_PATH`, `CNI_ARGS` in env,
  the network config JSON on stdin, and a `Result` JSON on stdout.
- Chain semantics: each plugin's result becomes `prevResult` in the next's input.

This is genuinely small — the whole contract is a few hundred lines of typed
serde plus a process seam. **The config-parsing and result-typing half is
platform-independent and should be built and tested first, on darwin, against
the spec's own conformance fixtures.**

Note for the NO SHELL rule: exec'ing a CNI plugin binary is not shell — it is
the contract. A typed `Command` construction, no `sh -c`.

### B.2 — the invocation seam

`CniPlugin` trait behind an `Environment`-style seam, exactly like
`ProvisionerEnv` in `pv_binder`:

- `FakeCniEnv` — records invocations, returns scripted results. Tests run here.
- `ExecCniEnv` — the real fork/exec. Linux only.

The kubelet calls `ADD` after the pod sandbox exists and before app containers
start, takes `result.ips[0]` as `status.podIP`, and calls `DEL` on teardown.

### B.3 — the darwin honesty arm

On darwin there is no netns to pass, so `CNI_NETNS` cannot be satisfied and no
plugin can run. Copy `DatapathInstall` exactly:

```rust
pub enum CniInstall {
    /// Config parsed, chain resolved, invocation PLANNED — but no plugin
    /// was executed and the podIP came from the container runtime instead.
    Planned,
    /// The plugin chain ran; the podIP is the plugin's result.
    Invoked,
}
```

Surfaced on the Node object, so `kubectl` can tell which one is true. The podman
network path stays as the darwin producer of `status.podIP` — it works, it is
honest, and it should not be ripped out for a plan that only lands on Linux.

### B.4 — NetworkPolicy gets its producer

`NetworkPolicyEnforcer` (instance #8) gets wired: a controller watching
`NetworkPolicy` objects, computing `NetworkPolicyRule`s through the existing
`selector.rs`, and handing them to the enforcer. On darwin the enforcer is
`Computed`; the `IptablesNetworkPolicyEnforcer` its header names as "R17b" is
the Linux arm.

**Until it is wired, stamp the honesty condition** — the same
`engenho.io/Served=False` treatment `FlowSchema` got. That is a one-day change
and it should land *first*, ahead of the rest of Phase B, because a NetworkPolicy
that silently allows everything is a security-shaped lie and the cheapest fix is
to say so.

### B.5 — the payoff test

A real CNI plugin chain — `ptp` + `host-local` + `portmap` from
`containernetworking/plugins` — on rio. Pod gets an IP from the plugin's IPAM,
not from podman. Then Cilium as the stretch target, since `CiliumNetworkPolicyAdapter`
already exists and would finally have something to adapt to.

---

## Sequencing, and what to do first

**B.4's honesty stamp, then Phase A, then Phase B.**

Reasons, in order:

1. The NetworkPolicy no-op is the only item here with a *security* shape, and
   declaring it costs a day.
2. CSI is fully buildable on this machine and has a real external oracle. CNI's
   payoff test needs rio.
3. CSI has a live controller to extend (`PvBinderController`); CNI has to build
   its producer from nothing.
4. Both phases close a "type + backend + no producer" instance, which is now the
   most frequently recurring defect class in this codebase — eight instances, and
   grep cannot find any of them because every symbol exists and every test passes.

## What this plan deliberately does not do

- **No second volume trait, no second network trait.** `VolumeRuntime` and
  `NetworkPolicyEnforcer` already have the right shape; they need callers, not
  successors. Writing a parallel abstraction would make instance #9.
- **No in-tree CNI or CSI plugins.** engenho implements the *runtime side* of
  both contracts. Shipping our own plugins is a separate decision and would
  compete with the ecosystem we are trying to become compatible with.
- **No darwin netns emulation.** The `Planned`/`Invoked` split is the honest
  answer; faking a netns would produce a green test suite and a wrong cluster.

## Verification

Unchanged from the contract programme: local gate
(`cargo test --workspace --exclude engenho-diff --all-targets --all-features
--locked --no-fail-fast`, `fmt --check`, `clippy -D warnings`), `gen build` after
any workspace-member change, and every new gate red-run once against a
deliberately-broken input before it is trusted.

Two additions specific to this programme:

- **Every phase ends with a real external plugin, not a fake.** A CSI/CNI
  implementation that only ever talks to its own test double has not tested the
  contract — it has tested itself.
- **`engenho-diff` `Expect::Divergent` entries** for the darwin `Planned` and
  `Computed` states, so the day they start passing on a Linux node is itself a
  signal rather than a silent change.


---

# What landed

| piece | where | state |
|---|---|---|
| NetworkPolicy producer + `PolicyDatapath` | `engenho-controllers/src/network_policy_controller.rs` | wired, 18 tests |
| CSI protos (v1.9.0 + k8s v1.34.0 registration) | `engenho-csi/vendor/proto/` | vendored, provenance recorded |
| CSI client + registration handshake | `engenho-csi/src/{client,registry}.rs` | 17 tests against a real gRPC driver |
| CSI node path (stage/publish/unpublish) | `engenho-kubelet/src/csi_materializer.rs` | wired into the kubelet, 9 tests |
| CSI dynamic provisioning | `engenho-controllers/src/csi_provisioner.rs` + the PV binder | wired, 13 tests |
| CSI registrar | `CsiRegistrarController` | wired as a runtime driver |
| CNI config + result + exec | `engenho-cni/` | 30 tests, 7 forking a real plugin binary |
| CNI status publication | `engenho-controllers/src/cni_status.rs` | wired as a runtime driver |

Every one of these has a producer wired in the same commit that introduced
it. That was the explicit goal: the programme opened by naming two
instances of "type + backend + no producer" and it would have been absurd
to close them by adding two more.

## Three decisions worth re-reading before changing any of it

**`VolumeRuntime` was NOT the seam, and the reason is a rule not a
preference.** It looked like the obvious home — its own header names
`CsiVolumeBackend (R13b — gRPC to CSI plugins)` as future work. Measured:
its inputs are provisioning-shaped and its output is mounting-shaped, while
CSI splits those across two services on two machines. That is the org
rule's *same goal, different shapes* case, where one type fits neither side
and looks well-motivated anyway. Two seams landed instead, and
`volume.rs`'s header now records the decision. The declaration stays (★★
MODULARIZE, DON'T DELETE).

**The darwin arm is typed, not faked** — `CniInstall::{Planned, Invoked}`
and `PolicyDatapath::{Computed, Installed}`, both copying
`DatapathInstall`'s precedent. A pod gets an address either way and
`kubectl get pod -o wide` shows it either way; the annotation is the only
thing that distinguishes them.

**Tests go through the real thing.** The CSI tests talk to a generated
`tonic` server on a real `UnixStream`; the CNI tests fork a real plugin
binary. A CSI client tested against a hand-written double proves only that
our encoder agrees with our decoder, and a CNI test that calls a Rust
function exercises none of the env marshalling, stdin write, exit code or
error-document convention that ARE the contract.

# What is NOT done

**The CNI pod-attach path.** `engenho-cni` can read `net.d`, plan a chain,
exec a plugin, thread `prevResult` and parse the result — and the kubelet
does not yet call `ADD` at pod start, because on darwin there is no netns
to pass and the call would refuse every time. What ships is the contract
plus an honest published verdict; wiring the attach is a Linux task whose
payoff test is on rio. `pending-cni: pod-attach (needs a Linux node)`

**Neither payoff test has run.** The plan named `csi-driver-host-path` and
`containernetworking/plugins` as the external oracles. Everything here is
tested against engenho's own reference driver and reference plugin, which
are real processes speaking the real wire — but they are still ours. Until
a third-party driver registers and a third-party plugin assigns an address,
the claim is "the contract is implemented", not "the contract is proven".

**`ControllerPublishVolume` (attach/detach) is reachable but unwired.** The
client method exists and is tested; no controller calls it, because
attaching is only meaningful for a driver whose volumes move between nodes
and engenho is single-node today. Named rather than silently absent.
