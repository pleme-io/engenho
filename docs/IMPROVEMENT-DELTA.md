# engenho improvement plan — the delta

`docs/IMPROVEMENT-PLAN.md` was worked end to end. This file is the other half
of that answer: what the plan asks for that is **not** in the tree, and why.
Everything not listed here is implemented, carries a regression test, and had
its red run recorded in the commit that closed it.

Three kinds of gap, kept apart because a reader does different things with each:

1. **Not possible here** — the precondition is a machine, a published release
   or a decision this session does not have. Each row names what unblocks it.
2. **Deliberately not done** — the plan itself defers or refuses it. Doing it
   would contradict the plan, so the row records the reason rather than a date.
3. **Remaining** — possible, intended, not done. The honest backlog.

A note on how to read the tiers. Where a row says a class is **impossible**, an
illegal state has no code path — a compile error, an absent method, a rejection
at the parse boundary. Where it says **caught**, a test or a lint finds it after
the fact. The two are not rounded together anywhere in this document.

> **Status, 2026-09-20.** Sections 1 and 2 are complete and measured. Section 3
> carries what the wave-3 pass has established so far; that pass is still
> running, and its remaining per-item outcomes land here when it finishes.
> Nothing in sections 1 and 2 is waiting on it.

## 1. Not possible here — the precondition is a machine, a release or a decision this session does not have

Fourteen items. Each names what would unblock it, so none of them is a mystery;
none can be closed from this checkout.

### Needs a live node (rio / ryn / plo)

| Item | What is waiting | What unblocks it |
|---|---|---|
| store-19, rt-27 | The durable-image tripwire (T3.4) has never been run over copies of the rio, ryn and plo data directories. The plan requires that census before the Shadow release. | Read access to the three data dirs; the predicate already exists (`image_tripwire()`), so this is a run, not a build. |
| store-20 | Promote `ImageInconsistency::ReplayGap` from Shadow to a boot refusal. It is the one finding neither the snapshot nor the log can rebuild. | One release with zero tripwire hits on all three nodes (plan edge 24). |
| api-41 | Flip `typed_decode::TYPED_DECODE` from `Rollout::Shadow` to `Enforce`. | `engenho_would_reject_total{gate="typed_decode",reason="decode_error"}` reading zero on rio, ryn and plo. |
| api-42 | Objects written by the old protobuf codec carry proto3 shapes (Lease `renewTime` and Event times as `{seconds,nanos}`, `generation` as a string, `creationTimestamp` as an object). JSON clients see the wrong shape until those objects are rewritten. | A store census, then a rewrite pass over the affected keys on each node. |
| node-39 | The live SIGKILL and restart checks for T1.2, plus the operator notice that Job pods on ryn now fail visibly across an engenho restart and are replaced within `backoffLimit`. | A run on ryn. |
| node-40 | The `podman_api` and `podman` rows of the backend conformance matrix have never executed anywhere — the lane host has no podman machine and no libpod socket. | A host with podman (rio or plo) and `ENGENHO_C...` set for those rows. |
| node-41 | Count unbound pods on ryn and rio carrying a non-default `spec.schedulerName` before the scheduler adopts them — after adoption engenho no longer places them. | Read access to the live clusters. |
| rt-28 | The T2.9 stop path's gate (four `StoreStillShared` tests) and its live check were measured on macOS only. | A Linux CI run, plus `systemctl stop` / `launchctl kickstart -k` both logging "stopped cleanly", exit 0, and a next boot replaying 0 entries. |
| meta-09 | Build `checks.x86_64-linux.tests` (cargo test through Nix), fix what the sandbox exposes, then fold `test.yml`'s cargo legs into `nix flake check`. | `ssh rio` (a darwin host cannot build a linux derivation natively). |
| ops-01 | Bump engenho in `pleme-io/nix` and rebuild ryn so the running node carries the waves. | The bump itself is done and pushed (`pleme-io/nix 93da8bf1`); activation needs sudo, and `luis.d` is not currently in sudoers on this machine. |

### Needs a publish or a tag (irreversible, so left to the operator)

| Item | What is waiting | What unblocks it |
|---|---|---|
| sui-01 | engenho still links C TLS. Its own reqwest is rustls-only and `ci/no-c-tls.tlisp` bans new C-TLS sources, but `sui-store` asks reqwest for **default** features, and Cargo unifies features across one workspace resolve — so `default-tls` pulls `native-tls` -> `openssl-sys` into the Nix-built binaries. | A sui release published **after** `66a289f`. Re-measured 2026-09-19 against crates.io, and a plain version bump does NOT fix it: engenho pins `sui-store 0.1.153`, the latest published is **0.1.219** (2026-08-19), and 0.1.219's manifest still declares `reqwest ^0.12` with `default_features = true` (it adds `rustls-tls` to the feature list, which does not turn the default off). So the row is not "engenho is behind" — every published version has the defect. Once a fixed version exists: `cargo update -p sui-eval -p sui-store`, `gen build`, then delete the three `tls:ATTRIBUTION` rows in `ci/no-c-tls.tlisp` so the check becomes a plain ban. **This is not only hardening debt — it is why the release path is red.** Measured 2026-09-20 on `f743984`: `auto-release`'s Test gate fails building `openssl-sys v0.9.116` (`fatal error: openssl/opensslconf.h: No such file or directory`), and the dependency edge is `reqwest v0.12.28 -> hyper-tls -> native-tls -> openssl-sys`, confirmed with `cargo tree -i native-tls`. The `test` workflow passes the same tree (5057/5057) because its environment differs; the release job's resolved devshell provides no OpenSSL headers. **Do not fix it by adding OpenSSL to the devshell** — that would make the gate green by supplying the very C TLS the gate exists to remove. Note the repo already recorded the version fact itself: `ci/no-c-tls.tlisp`'s attribution row for `sui-store 0.1.153` reads "fixed in pleme-io/sui 66a289f, not in any release as of 0.1.219". |
| rel-03 | No `v*` tag has yet run the new `release.yml`. | The first tag — it should show `publish-release` creating the release with every checked file and `promote-latest` running after it. |

### Needs an operator decision, not work

| Item | The choice |
|---|---|
| sub-core-03 | The plan says the tokio-free core keeps the name `engenho-substrate`. The carve kept the leaf as `engenho-substrate` and named the core `engenho-substrate-core`. Renaming is mechanical; which way round is the operator's call. |

### Needs a credential this session does not hold

| Item | What is waiting | What unblocks it |
|---|---|---|
| dependabot-01 | The repository's open Dependabot alerts (10 at the last count taken through the web UI) were never triaged against the plan. | A token with `security_events`. Measured 2026-09-19: `gh api repos/pleme-io/engenho/dependabot/alerts` returns `403 Resource not accessible by personal access token`, so the list cannot be read here at all — this is a *blind* answer, not an empty one. |

**Reachability, measured 2026-09-19 (this is a *blind* result, not a verdict on
the nodes).** Tailscale is up on this host and `tailscale status` shows `rio`
**active** with traffic flowing (relay "iad", tx 579696). Despite that, `ssh
rio` and `ssh 100.96.225.66` both time out on port 22, and so does `plo`. So the
rows above are blocked on *this host's* path to the nodes, and nothing here says
whether the nodes themselves are healthy — do not read these rows as "rio is
down". The likely cause is the corporate full-tunnel VPN that already blocked
the tailnet earlier in the session; the check to re-run after disconnecting it
is `ssh rio 'uname -sm'`.
## 2. Deliberately not done — the plan defers or refuses these

Doing any of these would contradict `docs/IMPROVEMENT-PLAN.md`, so each row
records the plan's reason rather than a date.

### Gated on an edge that has not been crossed (§6)

| Item | Why it is not done | Gate |
|---|---|---|
| Multi-node / a second Raft voter | Needs a FaultRouter partition test with a recorded red, every Raft RPC reply a `Result`, read fences, `/readyz` typed per role, durable votes, and an in-binary transport. | edge 18 |
| revoada going live (**13,740** lines as of 2026-09-19, in no binary) | Its safety rests on a durable vote store, a real `has_majority`, and a quorum check on promotion — none of which exist. Kept as a fenced typed draft. **"In no binary" is not prose here: `engenho/tests/shipped_closure.rs` asserts `!linked.contains("engenho-revoada")`, so the day something links it, that test goes red.** | §5.3, edge 18 |
| fonte fixes beyond fencing | fonte is not shipped and runs only mocks; hardening code no binary runs is refused. | §9 |
| CSI protocol work | Zero CSI drivers are registered; hardening an unexercised surface is refused. | §9 |
| A keyed WorkQueue | The single requeue slot covers every observed failure mode. | §9 |
| Respawn / escalate after a child dies | Needs liveness to be visible first; a rebuilt kubelet loses `local` and would duplicate native workloads. | edges 11, 12 |
| sd_notify watchdog | Must not arm before ryn's native workloads are isolated. | §9, edge 11 |
| Deployment RollingUpdate | Waits on T2.1, T2.3, T3.5 and T4.10. | §9 |
| Native `process_group(0)` | launchd killing the job's process group is today's only protection against a restart leaving a second copy of every native workload. | edge 12 |
| Off-loopback listeners and `hostNetwork` | :10250 needs authentication and :2379 mutual TLS first. | edge 26, T4.9 |
| Wiring in any dormant controller (HPA, Ingress, DNS, Plantio, Drv, DrvBuild, TieredCacheReconciler, EventDrivenController) | Declared in the Dormant catalog instead (T5.11). HPA's hand-listed group/version is fixed when it is wired in. | §5.4 |

### Refused outright (§9, "What I would not do")

- Make pedantic lints a gate, or keep `-D warnings` over ~3,800 style warnings.
- Build the full supervisor now.
- Split `kubelet.rs` or `runtime.rs` with move-only PRs.
- Re-model the kubelet lifecycle as a maquina machine.
- Adopt NATS as the fabric.
- Normalize nulls in request bodies (it breaks merge-patch deletion; edge 9).
- Widen gc to every served kind.
- Build a self-differential trajectory oracle, or an effect ledger across all 28 controller types.
- Persist a `writer` field on `ResourceCommand`, or add a `ResourceCommand` variant in a single release.
- Change `[profile.release]` expecting a faster daemon.
- Expand `/metrics` without a scraper; add a `not-ready:NoSchedule` taint before T5.7.
- Re-tick a stateful child after a panic.
- Treat a 0600 socket or an on-disk mTLS key as isolation on ryn.
- Rate-limit watch clients per client (API Priority and Fairness).

### Rollout pacing, dropped on operator instruction

The plan's per-release discipline — one hot-path change per release, a week of
data before `/livez`, Shadow before Enforce, a census before every
workload-affecting change — was dropped on 2026-09-19: *"no need to be careful,
this is a dev cluster essentially."* Gates were implemented enforcing directly
unless noted. One exception was kept, because its failure is unrecoverable
rather than merely disruptive:

- **T3.4's snapshot-inconsistency tripwire stays Shadow** (log + count). Making
  `open()` fatal the first time it meets production data can leave a node that
  cannot boot.
### Raised by a lane, then triaged out — 28 notes that are not work

Every lane recorded what it chose not to do. Triage read each one against the
code and found 28 that no one should pick up later. They are listed by *why*,
because the reason is the useful part: a reader who disagrees with the reason
has found a real item, and one who agrees can stop.

**A note, not work (9).** The lane was telling its neighbours something, not
leaving a to-do: every `ContainerRuntime` implementor now implements
`readoption` and `FakeEvent` gained an `Adopt` variant (node-42); the panic hook
installs in `Runtime::start`, not `main`, because the count is only observable
through `/metrics` and that does not exist earlier (rt-47); `RaftMesh::terminate`
now awaits its RPC pump and callers need no change (rt-50); renames must update
`docs/STATE-MACHINES.md` and `docs/TYPESCAPE.md` in the same change or
`ci/doc-sources` fails — a standing rule, already enforced (ctrl-13, docs-09);
`rust-auto-release` runs default features where `test.yml` runs `--all-features`,
which the lane itself says needs no change (meta-16); `M0.1-PLAN.md` names the
pre-rename `TickReport.unschedulable_no_fit` in a dated history row (node-44,
meta-17); nightly mutation runs are *intended* red on the 25 measured survivors
until T1/T2 tests kill them (meta-07).

**Conditional on a premise that does not hold (6).** The store could re-export
`ReadConsistency`/`ReadRefused` through a facade crate *if* the router did not
depend on `engenho-store` directly — it does, at `engenho-apiserver/Cargo.toml:56`
(api-43, store-os-01). The five private git dependencies were to be checked for
vendoring without `BOT_PAT`; `Cargo.lock` now has zero `source = "git+"` entries
(meta-10). T0.2 was to land before T0.3b — both have landed (meta-15).
Regenerating substrate's `patterns-full.nix` would change nothing: it lists only
`uses`/backend rows, not the new `package` input (rel-02). pangea-operator would
need `jobs/status` in its ClusterRole only if it were found writing batch Job
status; nothing shows it does (pangea-01).

**Contingent on a trigger that has not fired (5).** Preflight should probe a CRI
socket once `cri_backend::UNSUPPORTED` empties — CRI is refused at config today
(node-43, rt-48). A read-index linearizable read for `/readyz` needs more than one
voter; the store is single-voter (rt-49). The NATS listener becomes a catalog
Child when a daemon carries `teia-nats`; none does (rt-46). HPA's scale target
should resolve its group/version from the target's own `apiVersion` instead of
the hand list in `hpa.rs` — the plan defers this until HPA is wired, and it is
recorded on the `Dormant::Hpa` row (ctrl-19).

**Optional by its own terms (4).** `objects_unchanged`/`objects_failed` on
`ReconcileReport` (ctrl-09); `tracing` in `engenho-substrate` so the ledger logs
itself rather than taking a hook, where the required hook argument already means
a ledger cannot exist without one (meta-12); per-object wake filters so the
kubelet wakes only on its own Node and Lease, which the note calls design-only
(ctrl-18); running `crd_validator`'s required-fields check on Patch as well as
Put, where `CrdValidationWebhook` is wired nowhere (ctrl-23).

**Outside this plan (4).** The fleet default of release `opt-level = "z"` is a
substrate/repo-forge policy question, not engenho's, even though serde-heavy
binaries measured 1.7–1.9× (meta-11). Registering the `k8s-api-v0.34.1` JSON
fixtures as an `engenho-oracle` Vector does not fit: a Vector is a *table* of
cases, not a fixture tree (oracle-01). revoada's `federation.rs` leaks one
detached thread per member and fonte's pump discards transport errors — both
crates are unshipped and mock-only, which §9 refuses to harden (hard-01).
## 3. Remaining — possible, intended, not done

### 3.0 The shape of what is left, measured rather than estimated

A wave-3 pass is working 162 triaged deferred items across seven crate-disjoint
lanes. As of 2026-09-20 it has resolved 69, and the distribution is the useful
part:

| outcome | count | what it means |
|---|---|---|
| `already_done` | 52 | the work was already in the tree |
| `done` | 10 | implemented here, with a test and a red run |
| `partial` | 5 | this lane's half done, the rest named for wave 4 |
| `deferred` / `not_possible` | 2 | belongs to another repo or another lane |

**`already_done` dominating is a finding, not an anomaly.** Spot-checked
against the code rather than the commit messages: `MAX_PAGE_PREALLOC` bounds
the LIST reservation at handler.rs:55/:1249; `engenho-store/src/owned_task.rs`
is gone with lib.rs:139 re-exporting the substrate type; the `ProbeBlind`
condition exists with exactly one `Reason::Unhealthy` left on the
observed-failure path. The triaged list was built from deferred NOTES, and
waves 1 and 2 had already closed much of what those notes described. So the
162 substantially overstates the work that remained.

### 3.1 Wave 4 — the cross-boundary halves

**71 of the 162 items name crates in more than one lane**, so a lane can only
ever do its own half; the rest is recorded in its `deferred` field. 57 items
carry such deferrals today. Counted by the crate each names, the work
concentrates sharply:

    engenho-apiserver 15 · engenho-controllers 13 · engenho-scheduler 6
    engenho-runtime 6 · engenho-kubelet 6 · engenho-substrate-core 4 · …

Build that list from the deferred fields, deduped — not from the
cross-boundary census, which over-counts items one lane owned both sides of.
Run it SERIALLY on merged main: every item in it edits across a crate boundary
by definition, which is exactly what the lane discipline forbids.

### 3.2 The two serial gates

- **T0.5 / I17 — the panic-site ratchet. These are the SAME item**; doing both
  would do it twice. The plan's T0.5 asks for
  `#![cfg_attr(not(test), deny(clippy::unwrap_used, expect_used, panic))]` in
  the six daemon-core crates. Measured on non-test code only (inline
  `#[cfg(test)]` modules stripped by brace matching): controllers 22, runtime
  20, store 13, kubelet 10, apiserver 3, types 2 — and four candidate crates
  are already at zero, so the deny can land there with no code change at all.
  The raw count overstates: several hits are doc comments naming the macros,
  and the dominant real shape is `.lock().expect("… poisoned")`, which wants
  one decision applied uniformly rather than 30 judgements.
- **I16 — `[lints] workspace = true` on every member.** 13 of 29 members opt
  in. **The literal change breaks CI in the way §9 refuses**: the workspace
  lint table sets `pedantic = warn` and CI runs `clippy --workspace … -D
  warnings`, so opting in the other 16 enacts the very "pedantic lints as a
  gate over ~3,800 warnings" that §9 rules out. So I16 is reshaped: membership
  becomes DECLARED and TESTED — every member either opts in or appears in an
  `EXEMPT` table with a measured warning count. What becomes impossible is a
  member skipping the lint set *silently*, which is today's state.



These are ordinary backlog: no missing machine, no plan refusal. Each row says
what it is and what it would take.

### The gen-lock hook watches the lock, not the manifests

Found 2026-09-20 by the rest-a merge going red on `ci`. A commit changed
`Cargo.toml` only — the rustls line naming back what `default-features = false`
turns off — and `ci`'s delta-freshness job failed with

    { "status": "manifest-drift",
      "changed": [ { "path": "Cargo.toml",
                     "expected": "e04778f4…", "actual": "d15a4e27…" } ] }

**Why the local hook did not catch it.** The pre-commit hook refuses a
`Cargo.lock` that moves without its `Cargo.gen.lock`. A feature-flag edit
resolves to the same dependency versions, so `Cargo.lock` never moved and the
hook had nothing to fire on. The CI gate hashes the MANIFESTS as well as the
lock, so it sees what the hook does not watch. Any manifest-only edit —
features, lints, profile, workspace members — can pass locally and fail there.

**It self-healed.** `reusable-autoheal` regenerated the lock and pushed
`f451098` before this could be fixed by hand, which is the intended behaviour
and worth stating: the gap is real but it is *repaired automatically after the
fact*, not left open. The residue is a wasted CI cycle and a red main for a few
minutes, not a broken tree.

The durable fix is to widen the hook to the manifest set the gen lock records.
It is not done here because that hook lives in the repo-forge archetype and
changing it reaches every consumer — a fleet change that wants its own census.

### Found outside engenho — what was fixed, and what was not

The triage pulled in items living in `pleme-io/substrate` and
`pleme-io/actions`. A lane may not push to another repository, so each was
correctly returned `blocked`/`deferred` and done here instead. All of these are
pushed to `main` in their own repo.

| Item | What it was | Commit |
|---|---|---|
| sub-01 | The mutation gate took its runner from substrate's pin and its mutator from the consumer's nixpkgs — two toolchains measuring one thing. substrate now exposes `cargo-mutants` beside `cargo-nextest`. | substrate `7e1e04b` |
| sub-06 | `catalog/check.tlisp` was **red on main**: four reusables had no `defworkflow` row. Also `nix-image-auto-release-slim.yml` was shipping under its sibling's `name:`, so two reusables were indistinguishable in the Actions UI. | substrate `db8412f` |
| sub-02 | Dropped a `always()` kept only to work around a linter gap that `actions e223e9c` had already closed. | substrate `e988e6b` |
| sub-04 | **A real gate hole.** `waiver` errors when `require-tests: false` carries no typed reason, but `bump` declared `needs: test` alone — so the error was a red job BESIDE a tag that had already been pushed. Red run: `'bump' was expected to skip and ran`. | substrate `331879a` |
| sub-03 | An installer step plus a non-GC-rooted `nix build` for nextest collapse into one `nix-setup`. `pending-expired-token-fallback` recorded rather than claimed fixed. | substrate `8726a21` |
| sub-07 | substrate had `.test.tlisp` files and **no job that ran them** — one file, 11 assertions, green only when run by hand. Now gated, with a `min-files` floor against the zero-denominator green. | substrate `2a5b70c` |
| act-03 | **`job-selection-lint` guessed.** `re-find-all` returns only what matches and the token regex had no catch-all, so `a > 3` tokenized as `a`,`3` and evaluated as `a` — a confident verdict for an expression never read, from the tool whose header says a wrong verdict "is worse than having no oracle at all". | actions `2e18aa1` |
| act-02 | `rust-cross-build`'s sha256 sidecar was `sh -c` with a pasted filename; the `>` redirect also meant the captured status was the redirect's, so a shasum failing after truncating the file did not report failure. Now in-process, format pinned byte-for-byte. | actions `dc70ad7` |

#### Not taken, and why

- **sub-05 — the 419-caller release gate.** `cargo-auto-release.yml` has no
  test job at all, so its tag is ungated; `rust-binary-auto-release.yml` has
  only a bump job. The plan's own precondition was a caller census, and **the
  census is done**: of **415** caller repos (locally cloned — a lower bound),
  **375** have at least one test attribute, **37** have none, 3 have no root
  `Cargo.toml`. `cargo nextest run --no-tests=fail` exits 4 on a crate with no
  tests, so those 37 stop releasing the day the default flips — and a test
  attribute is not a passing test, so that is a floor. For scale,
  rust-auto-release's census found 14 of 53 (26%); this is 9%.

  The flip needs a design change first, not just a default change: a **boolean**
  `require-tests` cannot tell "undeclared" from "deliberately off", so applying
  sub-04's waiver rule to it demands a typed reason from all 419 callers at
  once. The shape that fixes it is a tri-state — `""` undeclared (today's
  behaviour), `"true"` enforced, `"false"` off-with-waiver-required — which
  makes the silent bypass unrepresentable and turns the rollout into a
  per-caller opt-in rather than a flag day. Changing the release path of 419
  repositories is outward-facing and hard to reverse, so it wants an explicit
  yes. Order: land the tri-state inert at `""`, commit a waiver row to each of
  the 37, flip the default, then converge rust-auto-release's boolean onto the
  same tri-state.

- **act-01 — lift engenho's mutation gate into `actions/mutation-test`.** The
  action as it stands cannot gate: it installs cargo-mutants with
  `nix profile install … || true` (a failed install is ignored), pins
  nixos-24.05, and reads survivors by grepping summary text with `|| echo 0`,
  so "could not count" and "zero survived" produce the same number. engenho
  carries its own 466-line `ci/mutation-gate.tlisp` for exactly that reason,
  and says so in a `skip-workflow-shim:` row.

  **Measured, and it changes the priority: the action has ZERO callers.**
  `uses: pleme-io/actions/mutation-test` appears in no workflow in the
  locally-cloned fleet; the single grep hit is engenho's comment explaining why
  it is not used. So nothing is harmed by the action being broken today — this
  is a promotion (engenho's proven 466 lines become fleet surface), not a
  repair, and its value only lands when a consumer switches. Not started here
  because the consumer half is `engenho/ci/` + `mutation.yml`, which a wave-3
  lane owns while it is in flight.

- **sub-09 — a substrate `image-manifest.yml` reusable.** engenho's multi-arch
  index is an inline `docker buildx imagetools create` job that should be a
  substrate reusable, with release-contract matching it by `uses:`. Same
  reason for not starting it: the consumer half is engenho's `.github/`, owned
  by a lane right now. Prefer `doca` (the fleet container tool) over buildx if
  it can create an index.

- **sub-08's second half — `additionalTags`.** Recorded in the workflow header
  rather than taken: 12 of 41 call sites omit it and would stop publishing a
  `:latest` pointer. Removing a tag other repositories may pull is
  caller-visible.
