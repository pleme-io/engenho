# Why engenho — orchestrators are not special, and three things follow

**Written 2026-08-30, after a day of building the plugin contracts and
running three foreign oracles against them.** Every claim about engenho's
own code is measured; every claim about the outside world carries a source.

---

## I. The observation: orchestrators are not special

The operator's framing: *Kubernetes and Nomad "do a lot of the same things
the same way", most of them "will be much better in Rust", and the
interesting opportunities are testing, simulation and embedding.* This
document is the research behind that, and the conclusion is that the first
claim is stronger than it sounds, the second needs one correction, and the
third is where the real value is.

### The evidence from building the contracts

A day spent implementing the ring of contracts around engenho's API turned
up how small each one actually is:

| contract | what it is, in full |
|---|---|
| **etcd v3** | a keyspace convention (`/registry/<segment>/<ns>/<name>`) plus `Range`, `Watch`, `Txn`, `Compact` |
| **CSI** | three gRPC services and a two-socket registration handshake |
| **CNI** | exec a binary, JSON on stdin, JSON on stdout, `CNI_*` in the environment |
| **kubelet API** | an HTTP surface with `/containerLogs`, `/pods`, `/exec` |

None is architecturally deep. The etcd façade that makes real `etcdctl`
work against engenho's store is ~1,200 lines and a keyspace table. The CNI
contract — the thing the entire container-networking ecosystem is built on
— is a process invocation with a JSON envelope.

### And Nomad confirms it from the other side

Nomad and Kubernetes are the same shape wearing different packaging. Both
are server/client with Raft consensus and a reconciliation loop; Nomad
collapses the components into one binary with an embedded BoltDB/SQLite
store, while Kubernetes splits them across microservices over etcd
([HashiCorp](https://developer.hashicorp.com/nomad/docs/k8s-nomad),
[NetApp](https://www.netapp.com/learn/cvo-blg-kubernetes-vs-nomad-understanding-the-tradeoffs/)).
The difference is deployment topology, not algorithm.

**engenho already sits at the interesting point of that space: Nomad's
packaging (one binary, embedded Raft store, no external etcd) speaking
Kubernetes' contract.** That was not a stated goal; it fell out of building
the store first and the faces second.

### The correction: not-special is not the same as not-hard

Everything hard about today was **accumulated convention nobody wrote
down**, not architecture. Three foreign oracles, run for the first time:

| oracle | what it found |
|---|---|
| real `etcdctl` | `db_size: 0` → `integer divide by zero` in `endpoint status`. The trait's own doc asserted clients read 0 as "unknown". They do not; they divide by it. |
| `csi-driver-host-path` v1.15.0 | nothing. 3/3 clean. |
| `containernetworking/plugins` 1.8.0 | `host-local: ARGS: unknown args ["K8S_POD_NAME=pod1"]` — upstream's `types.LoadArgs` rejects any undeclared `CNI_ARGS` key unless `IgnoreUnknown` is set. Without it engenho could drive **no upstream plugin at all** while passing the args Calico and Cilium need. |

Two of three found real defects. Neither was findable in-house: we would
never write a client that divides by `db_size`, and our own reference plugin
parses args leniently.

**So the moat around an orchestrator is not its design. It is the thousand
undocumented agreements its ecosystem has accumulated.** That is a moat you
cross by measurement, not by cleverness — which is why every contract
engenho implements now ships with a differential against foreign software,
and why the honest claim is "the contract is implemented", never "proven",
until one has run.

---

## II. Rust: what it actually bought, and what it cost

The claim "most of them will be better in Rust" is true here, but the
reason is narrower and more interesting than "Rust is fast".

### What it bought, measured

**The store's types made the etcd façade a projection rather than a
translation.** `VersionMeta { create_revision, mod_revision, version }` is
an exact mirror of etcd's triple, so a client doing a compare-and-swap on
`mod_revision` compares against the same counter engenho's own preconditions
use. Nothing is converted; the façade renders.

**Closed enums caught real mistakes, twice today.** Adding
`VolumeResolveError::CsiUnavailable` failed compilation at two match sites
that had to decide what a new failure means. Adding a driver to the runtime
failed a count assertion. Both are cases where a Go implementation would
have compiled and been wrong in one branch.

**Typed refusals carry information a string cannot.** `CsiUnavailable`
("the class is supported, this node cannot serve it") and
`PvcSourceUnsupported` ("engenho serves this class through no plane at
all") are completely different things for an operator to act on. In an
error-string world they are one grep away from being the same.

### What it cost, also measured

**The same store-leak footgun twice.** A detached listener task holding
`Arc<StoreMesh>` keeps the Raft log and fjall handles alive forever. It hit
the :10250 kubelet listener (fixed with `WeakKubeletApi`) and then hit the
:2379 etcd listener the same way, turning eight tests red. Two independent
arrivals at one defect — the convergence signal — so it now carries a
regression test rather than a comment.

**A sync trait over an async store blocked a whole subsystem, probably for
months.** `engenho-etcd` shipped complete with 48 passing tests and zero
consumers. The reason nothing wired it: its store traits were synchronous
while `StoreMesh` is async, which forces either blocking inside the runtime
or serving a stale snapshot. The fix was three signatures. Rust made the
incompatibility *visible* — but it also made it a wall rather than a
`time.Sleep`-shaped shrug.

**Honest summary: Rust moved the failure class rather than removing
failure.** It eliminated the "compiled and wrong in one branch" family and
introduced a "correct but unwireable until the types agree" family. The
first is worse, so the trade is good — but it is a trade.

---

## III. Where this actually goes: testing, simulation, embedding

This is the part worth building toward, and engenho is closer than it
looks — for a reason that is worth stating precisely.

### The prerequisite nobody usually has

Deterministic simulation testing — the FoundationDB / TigerBeetle /
Antithesis lineage — requires four things under control
([S2](https://s2.dev/blog/dst),
[Pierre Zemb](https://pierrezemb.fr/posts/learn-about-dst/)):

1. **execution** — single-threaded, no scheduler noise
2. **entropy** — every RNG seeded
3. **time** — no physical clocks
4. **I/O** — nothing outside the simulation

Retrofitting these is the expensive part. Every third-party dependency is a
non-determinism vector, and even Rust's randomized `HashMap` seeding counts.

### engenho already satisfies most of them, by accident of a different goal

**Entropy is already derived, not random.** `mint_uid`
(`engenho-store/src/state.rs`) produces an RFC 9562 version-8 UUID from
`BLAKE3(namespace ‖ key-label ‖ create-revision)`. Its doc comment states
why: *"replaying the same command sequence must reproduce the same bytes. A
`uuid::new_v4()` would break that."*

**The clock is already a typed seam.** `engenho-substrate/src/relogio.rs`
ships a `Clock` trait with `WallClock`, `FrozenClock`, `LogicalClock` and an
HLC. And the codebase already treats wall-time as a non-replicated input
that must be frozen at boundaries — `EventRecord.timestamp` carries the
comment *"Frozen at the boundary by the caller. The clock is not a
replicated input, the same law `deletion_timestamp` obeys."*

**I/O is already behind Environment traits, everywhere.** `InMemoryStore`,
`InProcessRouter`, `FakeBackend`, `ProvisionerEnv`, `VolumeMaterializer`,
`CniEnv`, `EtcdReadStore`, `CsiProvisioner`. This was house discipline for
testability; it is also exactly the DST requirement.

**★ THE POINT: Raft determinism and simulation determinism are the same
requirement, and engenho paid for it already.** A replicated state machine
must produce identical bytes from identical command sequences. That is the
DST contract, arrived at for attestation reasons. The remaining gap is
execution scheduling — a harness choice — plus an audit for stray
`SystemTime::now()` calls.

Nothing else in this space is positioned that way. Which leads to:

### What that unlocks that the ecosystem cannot do today

**Testing.** The state of the art for testing Kubernetes controllers is
KWOK — fake nodes and pods against a *real* control plane, so thousands of
nodes fit on a laptop
([KWOK](https://kwok.sigs.k8s.io/docs/technical-outcomes/performance/control-plane/single-cluster/)),
or SimKube, which records and replays traces against KWOK in a real cluster
([SimKube](https://github.com/acrlabs/simkube)). Both still boot a genuine
apiserver and etcd. Neither can inject a fault *at the store layer* or
replay a run bit-for-bit.

engenho's tests today boot a full store, run controllers, and assert on
revisions — in-process, in milliseconds, with no etcd and no tmpdir. The
gap from there to "10,000 pods, virtual clock, injected partitions,
reproducible from a seed" is a harness, not an architecture.

**Simulation.** A deterministic state machine over a replayable command log
is, definitionally, a simulator. The interesting version is not "test
Kubernetes" but *answer questions about a cluster that does not exist yet*:
would this scheduler change repack the fleet, would this rollout policy
survive that failure sequence, what does a 5,000-node convergence cost. That
requires exactly the four properties above and nothing else.

**Embedding.** This is the largest and least explored. `engenho-runtime` is
a **library** — `Runtime::new()`, not a distribution. Today's work showed
the contracts are independently composable: a deployment can serve
`:2379` and not `:10250`, or the apiserver and no plugins at all. That
means a Kubernetes-shaped control plane can live *inside* another product.

And the deepest version: **the control plane is domain-agnostic.** A typed
store with revisions and watch, a resource catalog, RBAC, and a controller
framework describe *any* declarative system. Kubernetes happens to have
chosen containers. `engenho-kubelet` is one renderer of a `Pod`; nothing in
the store, the apiserver or the controller runtime knows what a container
is. The "many faces" design already anticipates this — a `Face` renders one
`StoreMesh` truth as K8s, Nomad, or REST — but the sharper framing is that
the *workload domain* is as replaceable as the *API face*.

---

## IV. What to do about it

In dependency order, and none of it is speculative:

1. **Finish the differentials.** Two of three oracles found real defects on
   first contact. `NodePublishVolume` against `csi-driver-host-path` and
   the netns-using CNI plugins both need a Linux host. Until they run, the
   compatibility claim is unmeasured.
2. **Audit for wall-clock reads and wire `relogio` through.** The seam
   exists and the discipline is written down; what is missing is a gate
   proving no live path calls `SystemTime::now()` directly. That single
   check converts "close to deterministic" into "deterministic", and it is
   the cheapest high-value item here.
3. **Then the simulation harness.** Seeded, single-threaded, virtual-clock,
   fault-injecting, replayable from a seed. `madsim`
   ([madsim-rs](https://github.com/madsim-rs/madsim)) is the Rust framework
   for it and is worth evaluating before writing one.
4. **Then embedding, as a stated product surface.** It is already true that
   `engenho-runtime` is a library; what is missing is the documented,
   tested, minimal embedding — which contracts are optional, what a
   consumer must supply, what it gets.

## What this document deliberately does not claim

engenho is **not** a Kubernetes replacement today, and nothing here says it
is. It serves 18 API groups on one node, has never faced a conformance
suite, and its CNI pod-attach path is unwired. The argument is about
*trajectory and structural position*, not parity — and the trajectory only
means anything if step 1 keeps happening, because the whole thesis rests on
measurement against software we did not write.

---

**Sources**

- [Nomad for Kubernetes practitioners — HashiCorp](https://developer.hashicorp.com/nomad/docs/k8s-nomad)
- [Kubernetes vs. Nomad: Understanding the Tradeoffs — NetApp](https://www.netapp.com/learn/cvo-blg-kubernetes-vs-nomad-understanding-the-tradeoffs/)
- [Deterministic simulation testing for async Rust — S2](https://s2.dev/blog/dst)
- [So, You Want to Learn More About Deterministic Simulation Testing? — Pierre Zemb](https://pierrezemb.fr/posts/learn-about-dst/)
- [madsim — Magical Deterministic Simulator for distributed systems in Rust](https://github.com/madsim-rs/madsim)
- [Protocol-Aware Deterministic Simulation Testing — TigerBeetle](https://tigerbeetle.com/blog/2026-08-20-protocol-aware-dst/)
- [KWOK — Single Cluster Control Plane Performance Evaluation](https://kwok.sigs.k8s.io/docs/technical-outcomes/performance/control-plane/single-cluster/)
- [SimKube — record-and-replay Kubernetes simulator](https://github.com/acrlabs/simkube)
- [awesome-deterministic-simulation-testing](https://github.com/ivanyu/awesome-deterministic-simulation-testing)
