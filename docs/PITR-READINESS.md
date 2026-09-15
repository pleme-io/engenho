# PITR on engenho — the readiness ledger

**Measured 2026-09-14 at `b08927a`.** This answers one question: can a
point-in-time-recovery drill — *snapshot → restore → verify* — run on engenho,
and what is still missing if not.

It does **not** restate the single-binary roadmap; that is
[`theory/BUTAI.md`](https://github.com/pleme-io/theory/blob/main/BUTAI.md)
(M0–M8). §4 here records only the parts of it that bear on a drill.

---

## 1. The verdict

**The platform is ready. The drill is not written.**

Every Kubernetes primitive a drill needs is implemented and driven from the
runtime. What does not exist is a PITR *workload* that targets engenho — the
existing one (`lareira-camelot-pitr`) is a Crossplane composition aimed at a
cluster that no longer exists.

That is a much better position than it looks, because the two things that
blocked it for months were both identity bugs, and both are now closed.

### ★ Addendum, measured live on rio 2026-09-15 — the platform's readiness was
### a CODE claim, and running it found nine more blockers

§1 above was measured by reading code on a workstation. rio then became the
first node to run the whole stack for real — engenho alone, k3s stood down —
and the difference between "every primitive is implemented" and "a controller
can complete one reconcile" turned out to be nine defects, each hidden behind
the one before it. None was visible from the code alone; each needed the
system running under a real controller.

| # | defect | how it presented |
|---|---|---|
| 1 | LIST cloned the whole history ring | writes slowed as the ring filled, plateauing at 8192 |
| 2 | the watch path cloned the catalog to read one `u64` | writes 60s → 6s once removed |
| 3 | `valueFrom.resourceFieldRef` unsupported | 273 invalid-manifest warnings in 3 minutes |
| 4 | the libpod backend never pulled an image | pods sat `Pending` with no error |
| 5 | ServiceAccounts were not in `system:authenticated` | discovery 403 |
| 6 | `coordination/v1 Lease` uncataloged for protobuf | no controller could ever become leader |
| 7 | `emptyDir` created `0755 root:root` | a `runAsUser` pod could not write `/tmp`; the pod stayed *Running* and the kubelet reported success |
| 8 | **CRD schema defaults were never applied** | source-controller dereferenced a nil `*metav1.Duration` and panicked every reconcile, reporting only "building artifact" |
| 9 | the iptables router appended into `KUBE-SERVICES` without creating it | Service routing worked only on a node where kube-proxy had run — i.e. exactly not engenho's |

**The generalizable lesson is #7 and #8 together.** Both are cases where
engenho did something *reasonable in isolation* and wrong with respect to a
promise the ecosystem depends on: podman's default directory mode, and a
schema keyword treated as advisory. In both, the failure surfaced inside
somebody else's binary, with the apiserver named nowhere. A Kubernetes
runtime's compatibility surface is not its API shapes — it is every default
and every side effect a controller was written against.

**So amend §1's verdict:** the platform is ready *as code*, and each claim
below is worth exactly its last LIVE measurement. Before citing a row as
live, check whether a controller has actually completed the operation on a
node, not whether the type exists.

---

## 2. What a drill needs, and where it stands

| # | capability | state | where |
|---|---|---|---|
| 1 | schedule the drill | **live** | `engenho-controllers/src/job.rs` — Job + CronJob, spawned from `runtime.rs` |
| 2 | quiesce the source (scale to 0) | **live** | `/scale` subresource for Deployment + StatefulSet (`engenho-apiserver/src/scale.rs`) |
| 3 | snapshot the volume | **live** | `VolumeSnapshotController`, spawned `runtime.rs:2775`; the three snapshot CRDs seeded at `runtime.rs:1386` |
| 4 | restore it to a new PVC | **live** | `pv_binder.rs` reads `spec.dataSource` **and** `spec.dataSourceRef` |
| 5 | run a verify Job against the restore | **live** | (1) + the PVC→pod volume path |
| 6 | the drill pod calls the API | **live, fixed 2026-09-14** | §3 |
| 7 | the drill pod is still authenticated an hour later | **live, fixed 2026-09-14** | §3 |
| 8 | the drill pod is authorized | **live** | `engenho-apiserver/src/authz/` — typed RBAC triplet |
| 9 | a controller-shaped engine (CRDs + webhooks) | **live** | `crd.rs`, `crd_validator.rs`, `webhook_admission.rs` |
| 10 | external CSI drivers | **live** | `engenho-csi`, `csi_provisioner.rs` |
| 11 | **the drill itself** | **ABSENT** | §5 |

### The two properties that are deliberately NOT provided

Both are stated in the code rather than hidden, and both are correct choices:

- **The snapshot does not quiesce.** It copies a live directory, so a writer
  mid-write yields a torn file exactly as an un-quiesced disk snapshot does. A
  caller who needs consistency stops the workload first — which is why row 2
  above is part of the drill and not an afterthought. Claiming
  crash-consistency we do not implement would be the worse failure.
- **It snapshots local-path volumes only.** A PV with no `hostPath` is skipped
  with a typed note and **never reported ready**. A snapshot that claims
  success while copying nothing is precisely the *"ten clean receipts, real
  residue underneath"* failure the PITR programme already recorded once.

---

## 3. The identity chain — the part that was broken twice

Rows 6 and 7 are one story and it is worth keeping, because both failures
presented as something other than what they were.

**First failure — a wiring bug that read as a missing feature.**
`sa_token.rs` (mint + verify, bound ed25519 tokens, `exp` always present,
`aud` checked, four distinct rejection reasons) was written, tested and
correct. The runtime called `bootstrap()`, which installs the **keyless**
authenticator, instead of `bootstrap_with_sa()`. So a server fully able to
validate a ServiceAccount token answered every in-cluster client with
`401 service account token authentication is not yet supported`.

The downstream cost was the expensive part: in-cluster config could never
work, so every workload needing the API mounted a kubeconfig carrying **admin
client-key material** — cluster-admin credentials handed to ordinary pods
because the pod's own identity was refused.

Closed by `build_authenticator`, which is a named function precisely so the
wiring is assertable: *a capability reachable only through a call site nobody
audits is indistinguishable from one that was never built.*

**Second failure — a correct credential with an expiry nobody renewed.**
Projection ran exactly **once**, on the pod-start path. Bound tokens carry an
`exp`, so every API-calling workload worked perfectly and began failing at a
fixed age — one hour. At that moment nothing looked wrong: the pod Running,
containers healthy, key fine, and the apiserver correctly rejecting a
genuinely expired credential. *An operator that works after a restart and dies
an hour later reads as a bug in the operator*, which is the most expensive
place for the symptom to point.

Closed by `Kubelet::refresh_service_account_projections` (`b08927a`), with two
properties worth naming:

- **The cadence is DERIVED, not declared twice.** `ServiceAccountProjector::token_lifetime`
  is the single typed source; the kubelet refreshes at a third of it. Written
  as a constant in the kubelet the two would be free to drift, and the drift is
  silent in the worst direction — shorten the lifetime and pods start taking
  401s with nothing in the kubelet's state looking wrong.
- **Rewriting in place is sound and needs no restart.** The materialized
  directory is stable per `(namespace, pod, volume)` and bind-mounted, so a
  host-side rewrite is visible in the container immediately; `write_atomic`
  means a concurrent reader gets the old token or the new one and never a
  truncated one. A torn token would surface as a signature failure — a
  key-rotation incident that never happened.

Red-run verified: with the refresh disabled, the two tests asserting it happens
go red and the four asserting restraint stay green.

---

## 4. What is still outside the binary (only what touches a drill)

| gap | bearing on PITR | owner |
|---|---|---|
| **container runtime** — podman (remote CLI) or an external CRI | a drill's containers are started by something engenho does not contain. On darwin this is also the `podman rm` reclaim trap | `theory/BUTAI.md` M0–M8 |
| **CNI** — absent entirely; pod IP is parsed out of `podman inspect` | a NetworkPolicy applies and enforces nothing. Irrelevant to a single-node drill, disqualifying for a multi-tenant one | `docs/CNI-CSI-PROGRAMME.md` |
| **helm** — one shell-out, `engenho-fonte/src/caixa_helm_installer.rs:138` | only if the drill is delivered as a chart | unowned |

Everything else in the workspace is either the daemon itself or a build-time
tool (`engenho-kube-codegen`, `engenho-cluster-config-render`) that never ships
in the runtime path.

---

## 5. The remaining work — writing the drill

Row 11 is the only gap, and the honest framing is that it is a **naturalize**,
not a port. The existing engine is a Crossplane composition function pinned by
digest, aimed at `camelot-eks` — a cluster deleted 2026-07-20. Re-hosting it
would mean standing Crossplane up on engenho to run a function whose only job
is to emit the five steps engenho now serves natively.

The five steps, as engenho objects:

1. `PATCH /scale` the source workload to 0 (cold, consistent, no quiescing
   claim needed)
2. create a `VolumeSnapshot` of its PVC; wait for `status.readyToUse`
3. `PATCH /scale` the source back up
4. create a PVC with `spec.dataSource` naming that snapshot — the binder leaves
   it **Pending rather than provisioned empty** if the snapshot is not ready,
   which is the refusal that makes step 5 meaningful
5. run a verify Job mounting the restored PVC

**Do not reuse `verify --mode=presence`.** It never reads the secret back and
exits 0 even on failure; switching to it makes every drill pass. That is
recorded from the camelot chart's own release notes and is the single most
dangerous line in the PITR family.

**And promotion is gated on receipt coverage, not on a green run.** Ten
historical staging drills returned "3/3 absent" every time while six orphaned
KMS grants sat untouched the whole time — the grant class was simply not in the
declared set. Ten clean receipts, real residue underneath.

---

## 6. Honest tier

| claim | tier |
|---|---|
| the five drill steps are servable by engenho today | **measured** — every row in §2 cites a live, runtime-spawned controller |
| a drill has ever RUN on engenho | **no** — none is written |
| SA identity works end to end | **code-measured, not live-measured** — the wiring, the tests and the red-run are green on this workstation; no drill pod has exercised it on a real node since `b08927a` |
| local-path snapshot is crash-consistent | **no, and never claimed** — quiesce first |
| engenho is one binary | **no** — the container runtime is still external; see §4 |
