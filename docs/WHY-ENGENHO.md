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

## III.5 The sharper frame: convergence is FORCED, and that is the opening

The operator's framing, which is stronger than §I's and supersedes it:

> *Progressive standardization across time will force integration markets,
> and integration markets will force normalization — so it is the DESTINY of
> orchestrators to reach a point of similarity diminishing returns. That
> presents the opportunity: hook in, augment, control.*

§I observed convergence. This explains it, and the difference matters
because an accident might reverse while a forcing function will not.

### The mechanism, and its name

The loop: a standard interface appears → vendors build to it → an
integration market forms → the market exerts pressure BACK on every
implementation, because an orchestrator that does not speak CSI has no
storage vendors → speaking CSI constrains your object model → the
implementations normalize toward each other.

This is the **narrow waist**, the shape that made IP, POSIX, SQL and LLVM
IR what they are. A modular interface commoditizes the layer beneath it and
pushes value elsewhere. Once ~150 CSI drivers exist, no orchestrator can
afford not to speak CSI — and having spoken it, it has agreed to a
significant part of its own design.

**The consequence for strategy: building "a better orchestrator" is a
low-value move, because the forcing function drags every entrant to the
same place.** The high-value move is to stand BELOW the waist, where the
market has guaranteed your integration surface stays valid.

Today measured how thin the waist actually is: four contracts, each small,
implemented once on the runtime side, working against the whole vendor
ecosystem. That thinness is not a coincidence — it is what a waist is.

### engenho as a hook point, not a clone

The operator's completion of the thought:

> *engenho is a hook point across the development matrix where we can
> onboard but at the same time control and evolve safely — for others and
> for ourselves — for the common workloads which will be Kubernetes soon
> across the world, to being ours.*

This reframes what compatibility is FOR. Compatibility is normally
defensive: *we can run your things*. Here it is a **migration surface**:
you enter through the standard interface, and the implementation underneath
is ours and free to evolve.

**★ That is `mata-pau`, applied to the orchestrator layer** — the fleet
doctrine for replacing a system you cannot stop. The incumbent keeps
running; units migrate one at a time; each migration is gated on an
**oracle**, one output both systems produce, compared directly. *No oracle
⇒ no plan.* The differentials built today ARE those oracles, which is why
they are not optional decoration: they are the mechanism that makes the
substitution safe rather than merely attempted.

The strangler fig grows in the host's shape until the host is gone and the
fig stands where it stood. engenho grows in Kubernetes' shape — its API,
its CSI, its CNI, its etcd — while the substance underneath becomes ours.

### Three things a hook point must be, and the honest scoreboard

| requirement | engenho, measured 2026-08-30 |
|---|---|
| **where the workload already is** | 18 API groups + core v1, real `kubectl`, real `etcdctl` |
| **thin enough to reimplement** | 4 contracts; the etcd façade is ~1,200 lines |
| **provable at the seam** | 3 differentials; 2 found real defects on first contact |
| **able to EVOLVE past the standard** | the §III case: determinism, simulation, embedding |

The fourth is the one that separates a hook point from a clone. Matching
only makes you a compatible implementation with worse support. The value
is in what the substrate permits that the incumbent architecturally cannot
— and §III argues that is deterministic simulation, because Kubernetes'
process topology (etcd + apiserver + N controllers) forecloses it and
engenho's does not.

### The risk to name: waists ossify, and you inherit their model

Kubernetes' API is not a good API. It is an accident with enormous
momentum — namespaces, labels, `resourceVersion` semantics, the
`metadata`/`spec`/`status` shape. Standing below the waist means inheriting
all of it, forever, if the waist is your MODEL.

The escape is that the waist must be a **rendering target**, not the model.
The "many faces" design asserts this; today made it measurable. Grepping
the production paths of `engenho-store` and `engenho-apiserver` for
container concepts finds **only test fixtures** — `state.rs`,
`command.rs` and `handler.rs` mention `Pod` exclusively inside `#[cfg(test)]`,
and `engenho-controllers/src/controller.rs` (the framework itself) has zero
mentions. Nothing in the store, the apiserver core or the controller
framework knows what a container is. `engenho-kubelet` is one renderer of
one kind.

### The falsifiable test of the whole thesis

The claim "engenho is a hook point onto all of computing, expressible in
tatara-lisp" reduces to one measurable question:

> **Can the store + apiserver + controller framework reconcile a domain
> that has nothing to do with containers, with no changes to those crates?**

If yes, Kubernetes is genuinely one face of a general declarative substrate
and `(defentidade …)`-style authoring can drive any of them. If it requires
changes, the container model has leaked into the core and the "many faces"
claim is aspiration rather than architecture.

The grep above says it should be yes. **A grep is not a proof.** The proof
is a working non-container domain — reconciled, watched, RBAC'd, served
through the same apiserver — and it is cheap enough to be the next thing
built after the determinism audit.

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
4. **Prove the substrate is domain-agnostic** with one non-container domain
   through the unchanged store + apiserver + controller framework. This is
   the falsifiable test of §III.5 and it gates the whole tatara-lisp thesis:
   until it passes, "many faces" is a design intention rather than a
   measured property.
5. **Then embedding, as a stated product surface.** It is already true that
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
