# engenho: improvement plan (revision 2)

*HEAD `5d7b38e` (v0.53.118), 2026-09-19. This revision keeps the plan's structure and adds the results of two independent critiques:*
- *a code-truth pass: 33 claims checked, 28 verified, 2 false, 3 mis-cited;*
- *a cluster-safety and ordering pass.*

*I re-read at HEAD every critic claim the plan now depends on, plus several nearby ones. §12 lists each change and each critic point I did not adopt. **read** means I verified it in the source. **run** means I measured it by running something.*

## 1. Destination

engenho should be one binary whose crate graph is exactly what runs. Every compiled module and every compiled controller must be in one of four places:
- on a path `main()` constructs;
- behind a feature that is off by default;
- in an incubator crate outside the shipped closure;
- in a typed catalog of dormant controllers.

A CI check fails the day that stops being true.

- **The store.** There is one store, and its history is honest:
  - a revision advances only when content changes;
  - a compaction floor never claims history the ring cannot replay;
  - the durable image is never older than the snapshot;
  - replaying the log twice changes nothing;
  - a log entry means the same thing to every binary that replays it, because any change to apply semantics carries a marker on the entry itself;
  - a delete always carries its clock.

  A clean stop leaves nothing to replay. A crash, tested as a real SIGKILL, loses nothing that was acknowledged.
- **User writes.** Every user write passes through one border. Authorization judges the same parsed request the dispatcher executes. Create, replace, patch and apply all compute the object that will be stored, then default it, validate it and write it with compare-and-swap (CAS), the same way for all four. Node-local surfaces (the kubelet API and the etcd façade) either authenticate their callers or bind only to loopback.
- **Workload identity.** No workload runs as the operator. Until BUTAI's supervisor gives native workloads their own identity, ryn is one trust domain, and the plan says so instead of hardening listeners inside it.
- **Reconciliation.** Reconciliation is a closed catalog of owned children, and the spawner iterates an enum. Each driver loop:
  - returns `Infallible`;
  - holds one requeue slot, with a retry delay that grows;
  - isolates per-object failures;
  - records a heartbeat.

  `/livez`, `/readyz`, the kubelet's `/healthz`, and a Node's Ready condition (as both kubectl and the scheduler see it) are derived from heartbeats and leases. None of them is a constant.
- **Observation.** Every place that observes the world returns *seen*, *absent* and *not observed* as separate constructors. That covers probes, container exits, watch ends, owner lookups, integer fields, node readiness, runtime relists and re-adoption after a restart. "I did not look" then cannot be written as healthy, gone, failed, zero, succeeded or never-started.
- **Rollout.** A change that could strand a workload or reject a stored object ships only after a census of live state shows it strands nothing. While the census count is above zero, the change runs in shadow mode.

Each of these properties is held by a check that CI has seen fail. A second node joins only through the same checks, over a transport inside the binary.

## 2. Where it stands

| | Fact | Evidence |
|---|---|---|
| Reach | 165k lines across 27 crates. **83%** (137k lines, 19 crates) is in the dependency closure of the three shipped packages. **17%** (27.6k lines, 8 crates) is in none of them: revoada 13,358 · fonte 5,570 · diff 3,380 · kube-codegen 3,291 · machines 905 · sui-typescape 598 · substrate-props 279 · fonte-cli 209. | `cargo tree` (run) |
| Compiled but never executed | **Code:**<br>- All of engenho-teia: `boot_store` always builds `InProcessRouter` with node 1 at `in-process://1`.<br>- 36 of substrate's 40 modules.<br>- The scheduler's `predicates`, `affinity` and `preemption` modules (1,257 of 2,369 lines). Nothing else evaluates nodeSelector or tolerations.<br>- **8 of the 28 `Controller` types, which no shipped crate constructs:** HPA, Ingress, DNS, Plantio, Drv, DrvBuild, TieredCacheReconciler and EventDrivenController. `spawn_drivers` builds the other 20.<br>- `ControllerRuntime` and the apiserver's `audit.rs`.<br>- `install_snapshot` and `Txn`: there is a single voter, and the etcd `txn` handler ignores its request.<br>- CNI exec (0 callers), CRI (selected on 0 of 3 nodes), CSI (0 drivers registered).<br>**Config:** the `teia` and `revoada.topology` sections are rendered for every node and validated at every boot, but nothing reads them.<br>**Live, though easy to file with the above:** snapshot build and log purge. openraft 0.9.22 by default snapshots every 5,000 log entries and keeps 1,000, and `default_config` overrides neither. | runtime.rs:1129-1146, :2607-3056; scheduler lib.rs:40-47; etcd server.rs:165; typed-config.nix:474-486; engenho-config lib.rs:227-243; mesh.rs:566-575; openraft config.rs:165-181 (read) |
| Tests | 3,926 tests; 992 of them (25%) are on unshipped crates. The four 09-18 defects lived in controllers (615 tests) and the kubelet (438 tests), so coverage was not the gap. The tests check a single tick; none checks liveness or how an unknown state is resolved. Mutation testing (cargo-mutants) over the four fixed files: 25 of 99 viable mutants survive. | mutants-full.log (run) |
| CI after the compile fix | Run 35418242848 on 1de473e with `--no-fail-fast`: every target compiled and ran. **4 targets (7 tests) failed:**<br>- 3 because the runner has no `nix` (native_runs_a_real_closure ×2, native_runs_postgres ×1);<br>- 4 at shutdown with `StoreStillShared { strong_count: 2 }` (m0_1 ×1, m0_6 ×3).<br>Private dependencies resolved, and `bot-pat` appears masked in the log, meaning it is set. | job 105830927952 (run) |
| Gates that cannot fail | - The clippy step reports success because of `continue-on-error` (test.yml:183).<br>- `[workspace.lints]` reaches 13 of 27 crates, and none of them is apiserver, controllers, kubelet, scheduler, store, substrate or config.<br>- `nix flake check` runs one check at evaluation time (`checks.typed-config`, flake.nix:164-168, since 51e702a) and compiles no Rust. CLAUDE.md § CI + gating still says the flake declares no checks.<br>- deep-test has passed 0 of 21 runs. | manifests (read) |
| Release path | release.yml has never succeeded (ghcr 403; the aarch64 openssl-sys build fails). Substrate's rust-auto-release runs `bump`, which tags and pushes, in parallel with `test`. `ship` needs both jobs, but it is skipped for private repos. So engenho's tag, and the release.yml run it triggers, never wait for the test gate. v0.53.117 was tagged at 21:27:21, its gate went red at 21:33:33, and it was published as Latest 16 s after the tag. | rust-auto-release.yml:161-218, :258-420 (read); release runs (run) |
| Panic surface | **48** unwrap/expect/panic calls in the lib and bin targets of the six daemon-core crates: controllers 22, store 9, apiserver 7, kubelet 7, runtime 2, scheduler 1. | clippy with all features (run) |
| Health | - `/healthz`, `/livez` and `/readyz` return the constant `"ok"` (health.rs:106-118). So does the kubelet's `/healthz` (server.rs:264), and a test at :420 pins that.<br>- Since f866f6e, a Node's Ready condition is derived from its Lease at read time (handler.rs:512), but only for readers that go through the apiserver.<br>- The scheduler reads the stored Node (scheduler.rs:70). It treats a Node with no conditions, or with no Ready condition, as schedulable (strategy.rs:114-124).<br>- `/metrics` renders `engenho_store_revision 0` and empty object counts from literals (metrics.rs:~265). Nothing scrapes it. | read |
| Durability | - The durable store is the default.<br>- The catalog blob is persisted every 64 **apply calls** or every 5 s, and that check runs only inside `apply` (fjall_store.rs:829-834). One call carries a batch of entries, so under a burst of writes the blob can fall more than 1,000 entries behind, which is more than purge keeps.<br>- `terminate()` persists nothing (mesh.rs:558-563). Every stop is therefore a kill -9 for the catalog, and a stop during such a burst leaves a node that cannot boot (T3.1 case 2).<br>- `install_snapshot` already writes the catalog, `last_applied`, membership and snapshot metadata, as separate inserts followed by one persist (:893-935). `build_snapshot` writes neither the catalog nor `last_applied` (:715-750).<br>- The blob's old compaction floor survives a restart while the history ring comes back empty (state.rs:315). | read |
| Log format | - Raft log entries are serde_json (fjall_store.rs:475).<br>- `ResourceCommand` is internally tagged, ignores unknown fields, and already has fields marked `#[serde(default)]` so newer fields are optional when reading older entries (command.rs:96-111).<br>- The state machine reserves `current_revision.next()` for every apply and commits it only when the apply really mutates something (state.rs:347-350).<br>Consequence: a change to what counts as a real mutation renumbers history the next time the log is replayed. | read |
| Write border and authz | - RBAC is live and mints ServiceAccount tokens (runtime.rs:274-276).<br>- A rule on a bare resource grants every subresource of it (authz/mod.rs:462-465). So `create serviceaccounts` grants `serviceaccounts/token`, which means minting a token for any ServiceAccount the rule covers.<br>- `pods/exec` is authorized the same way, but the apiserver does not serve it. It dispatches only `status`, `scale`, `log` and `token` (router.rs:1444-1454).<br>- Authz parses the raw path (router.rs:533) while dispatch parses the decoded path (coords.rs:335). Verified by reading, not by running.<br>- Defaulting, validation, and CRD-schema defaulting and validation run on POST only (handler.rs:1233-1316). PUT, PATCH and server-side apply (SSA) skip all four. Flux creates and updates objects with SSA, so no object Flux wrote has ever been validated. | read |
| Node-local surfaces | - The kubelet API (upstream's :10250; rio uses 127.0.0.1:10251) serves `/exec` and `/containerLogs` with no authentication.<br>- The etcd façade (:2379) is read-only and unauthenticated, and it serves every Secret.<br>- Both bind to loopback by default, as a deliberate choice.<br>- On rio and plo, podman pods have their own loopback, and podman_api does not implement `hostNetwork`, so only host processes can reach these listeners.<br>- On ryn, engenho is a launchd agent of the operator's account. The native backend neither changes UID nor uses a mount namespace. So every native workload runs as the operator, on the host's loopback and filesystem. | engenho-config runtime.rs:239-272; nix nodes/ryn/default.nix:489-490 (read) |
| Node registration | On every boot, `register_node` writes a whole Node with `expected: None`: `spec: {unschedulable: false}`, only the well-known labels, and Ready=True (runtime.rs:1219-1247). Every restart therefore uncordons the node and wipes the operator's labels and taints. ryn labels itself `kubernetes.io/os: darwin` (:1192-1196). | read |
| Restart on ryn | - The native backend cannot re-adopt running containers (`ContainerRuntime` has no `list()`; podman_api re-adopts by name, podman_api.rs:1299-1330).<br>- The kubelet starts any non-terminal pod it has no local record for (kubelet.rs:2046-2056). So after a restart, a running `restartPolicy: Never` pod is silently run again in place.<br>- The native `stop` sends SIGTERM only, with no escalation and no reaping, and `remove` drops the record (native_backend.rs:482-501). A process that ignores SIGTERM keeps running untracked until launchd kills the job's process group. | read |
| Unbounded repetition, observed in production | - On plo, 2026-09-06, a controller failing with a Transient error retried about 3.5 times per second against an advertised 1 s backoff. It pegged a core, and every API read hung while `/healthz` said ok (nix nodes/plo/engenho.nix:216-229). Concurrent `arm_requeue` chains produce that rate; the evidence is consistent with that cause but does not prove it.<br>- The scheduler rewrites an unschedulable pod's condition about 19 times per second (38 ticks in 2 s, measured 09-18). | read / run |
| Release profile | `opt-level="z"` costs 1.7-1.9× on serde_json round trips. The Nix-built daemon never reads `[profile.release]`: buildRustCrate passes `-C opt-level=3` and no LTO. Only the cargo-built engenho-mcp and render tarballs pay that cost. | build-crate.nix:31 (read); bench (run) |

**Corrections to figures in circulation:**
- The "2,275 unwraps" figure came from a grep that counted `#[cfg(test)]` modules. The real count is 48.
- BOT_PAT is not a blocker. The explanations at test.yml:42-66 and CLAUDE.md:250-256 are stale and should be replaced by what was observed.
- The compile break is fixed. 1de473e deleted `tmp_path_appends_tmp_suffix`, which asserted the race's precondition, and added `concurrent_writers_never_tear_or_leak`. That test's red run reproduces the production rename error.
- The earlier "exactly four reds" was a guess; it is now measured: 4 StoreStillShared failures plus 3 nix failures.
- The Node-Ready observer already shipped in f866f6e. The remaining gap is that the scheduler reads the stored Node.
- The release profile's `opt-level` does not affect the daemon.
- **"`nix flake check` has no checks"** (in rev 1 and the 09-18 fact sheet) is false. It runs one evaluation-time check and compiles no Rust.
- **"27 sites discard the `ResourceOp`"** is inaccurate. There are 37 `objects_changed += 1` sites, 27 of them in spawned code. The ones after a store write count without reading the returned op, and network_policy_controller.rs:470-471 counts a removal whose error was discarded.
- **"`create pods` grants `pods/exec`"** is true of the matcher but moot at the apiserver, which does not serve exec. The live escalation is `serviceaccounts/token`.
- **"38 controllers"** is wrong. There are 28 controller types, and 20 of them are spawned.

## 3. Defect classes

**A. A verdict that nothing observed.** It fails in both directions:
- *"Definitely negative":*
  - gc's list of owner kinds read as "orphan";
  - a watch stream ending read as "shutdown";
  - no pod IP read as "unhealthy";
  - an unobserved container exit read as exit 0 / Succeeded (kubelet.rs:3281, :3685, :3975);
  - **no local record read as "never started"** on a backend that cannot re-adopt (kubelet.rs:2046-2056);
  - an empty replay ring read as "no changes";
  - a relabel with no event read as "still matches";
  - a truncated page read as "last page";
  - `replicas: "3"` read as the default of 1;
  - the sibling operator's "imported 90, failed 0".
- *"Definitely fine":*
  - the constant `ok`;
  - "no conditions" read as schedulable;
  - Ready=True written at every registration.

It has two sub-shapes:
- *A hand-written closed list standing in for the full set of kinds:*
  - gc's kind list;
  - the hand-listed KindFilters (runtime.rs:2633-3056);
  - HPA's `_ => ("", "v1")` (hpa.rs:196-200). This is the same shape, but in a controller nothing spawns, so it counts as class D until the controller is actually spawned.
- *Reported counts that do not match the effects:*
  - 37 `objects_changed += 1` sites (27 in spawned code) count writes without reading the returned `ResourceOp`;
  - `write_status_cas` counts `PatchRejected`/`ApplyConflict` as Written.

**B. Repetition with no bound.**
- `arm_requeue` chains that re-arm themselves.
- A flat 1 s Transient retry, forever.
- The scheduler waking itself.
- GC proposing the same delete again on every tick, with no clock, as a Raft entry that is fsynced to disk.
- The kubelet retrying volume-pending pods every 1 s, forever.
- One pod's error aborting the kubelet's sweep, which then retries and fails at the same pod.
- A failed start retried with no backoff (ground-truth instance 4). T2.4 c2 must not bring this back.

**C. Work nobody owns.**
- Re-ticks spawned as detached tasks.
- Driver `JoinHandle`s that are only ever aborted at shutdown.
- Listeners that log one warning and end.
- SIGTERM left unhandled.
- Tasks that hold a strong reference to the store across an await (BookmarkTicker, watch_backend.rs:784-795).
- Cleanup obligations nobody records:
  - materialized Secret directories are never removed;
  - the results of stop and remove are thrown away with `let _ =` (kubelet.rs:3081-3082, :3892-3893);
  - the native `remove` drops the record of a process it only sent SIGTERM.

**D. Declared-but-unbound artifacts.** These are artifacts that claim a relationship to running behaviour that nothing enforces.
- *Models that contradict what they model:* engenho-machines.
- *Code compiled in and unreachable, yet validated at every boot and deployed by the chart:* teia.
- *Controllers compiled into the shipped binary that nothing constructs:* the 8 dormant ones.
- *Predicates nothing references,* while the scheduler writes "insufficient cpu/memory" as if it had checked them.
- *Config fields that are validated and never read:* `scheduler.namespace`, `tick_interval_seconds`, and the `teia` and `revoada.topology` sections.
- *A tested renderer fed a literal:* metrics.rs:~265.
- *Comments that state what the code does not do:*
  - authz/mod.rs:445-449, which claims upstream matches a subresource against its parent;
  - state.rs:276-280, which says history is rebuilt by re-applying the log;
  - kubelet.rs:3778-3782, which says "re-enter the reconcile" in an arm that returns;
  - pv_binder.rs:378, which says the name uses the PVC's uid when the code uses its namespace and name;
  - runtime.rs:410-412, which says `Arc` where the code holds a `Weak` (etcd_facade.rs:115);
  - **runtime.rs:1199-1206**, which says a nodeSelector keeps pods "Pending permanently", although no shipped code evaluates nodeSelector;
  - **fjall_store.rs:114-119**, "together or not at all", which is true for `apply` and false for snapshots.
- *Tests that pin a wrong belief.*
- *Docs that claim safety:* DISTRIBUTED.md:194 ("split-brain is impossible"), and CLAUDE.md's statement that the flake declares no checks.
- *A CI header that explains the red by a cause that no longer applies.*

**Decision: D is its own class, not an instance of A.** Logically, D has A's shape one level up: a claim about the code that nothing checked, stated as definite. It still needs its own class, for three reasons.
1. **It is repaired in a different place.** A is fixed in the running system, with a type at the point of observation. D is fixed in the repository: a check ties the artifact to the code, or the artifact is fenced off or deleted.
2. **It is detected differently.** A is caught by tests of how unknown states resolve and by runtime detectors. D is caught by build-time checks that compare two artifacts.
3. **D is what let A survive.** Every live A defect this month was vouched for by a D artifact:
   - the probe defect by a test asserting that no address means Failure (probe.rs:1279-1295);
   - the stale floor by two tests asserting it survives serde (state.rs:1891-1895, fjall_store.rs:1212);
   - the RBAC widening by a comment claiming upstream parity;
   - the atomic-write race by a test pinning `<path>.tmp`;
   - 249 red CI runs by the BOT_PAT header.

A and D share a goal but not a shape, so the right move is to write a rule, not to force them into one type.

The rule for D is **bind, fence or delete**:
- *bind* an artifact that is correct and needed (the scheduler predicates: wire them in);
- *fence* one that is plausible but not needed yet (teia behind a feature, unused substrate modules in an incubator crate, dormant controllers in a catalog);
- *delete* one that contradicts what it names (engenho-machines).

| Class | Structural fix | Strongest guarantee reachable |
|---|---|---|
| A | Three-armed values at each boundary, using the fleet's kotae vocabulary (found · empty · blind). Identity and kind are resolved from the object's own declaration, never from a list. Reports are derived from per-object outcomes. | A type or private constructor at each boundary. Only mitigated where a raw `Value` read still compiles. |
| B | One requeue slot per driver. Constant retry curves indexed by consecutive failures. The store refuses to bump a revision for unchanged content. Writers skip work already in progress. Condition writes skip when nothing changed. | Type (slot, gate) plus a test. |
| C | A closed catalog of owned children. `run() -> Infallible`. A JoinSet that `main` watches. Every spawn path disallowed outside named seams. Cleanup obligations recorded as values that teardown must consume. | Compile error (E0308/E0004) for catalog members; a CI gate for everything else. |
| D | Bind, fence or delete. A crate-closure gate. A dormant-controller catalog. Exhaustive destructuring of config structs. Render-from-source and doc-truth tests. Tests that take expected values from upstream. A model may exist only with a differential test, or as the implementation itself. | CI gate; compile error for config. |

**A risk that applies to changes, not a defect at HEAD: reinterpreting persisted history.** The Raft log is replayed by whichever binary boots next. A change to apply semantics that is not marked on each log entry silently rewrites history at the next restart or rollback. §10 now has a column saying whether each change is safe to replay, and edge 20 governs.

## 4. Themes

Item IDs are T\<theme\>.\<n\>. The IDs in parentheses are the digest items each row merges.

**Two shared tools serve many rows, so each is built once:**
- **The census harness (T0.10)** runs one named check, with no side effects, over live state and reports counts. Seven items use it.
- **The rollout gate (T0.11)** is one type with a shadow mode and an enforce mode, plus one metric. Six items use it.

### T0: Gates that can fail
*A check is only a claim until CI has shown it failing for the right reason.*

| ID | Change | Tier | Size |
|---|---|---|---|
| T0.1 | **Let the runner run the suite.** Install Nix before `cargo test`, the way pleme-io/actions/nix-build/action.yml:95 does. That would be the third inline copy of this step in the fleet, so extract `pleme-io/actions/nix-setup` instead of pasting it again. Replace test.yml:42-66 and CLAUDE.md:250-256 with the measured facts. Correct CLAUDE.md § CI + gating: the flake has declared `checks.typed-config` since 51e702a; what it lacks is Rust. BOT_PAT belongs in the org posture catalog, not in a `gh secret set` instruction, which the platform-mediated rule forbids. | ci-gate | S |
| T0.2 | **One test-selection contract.** Today there are two, and the release gate follows neither:<br>- test.yml excludes engenho-diff with a flag, documented in `ci/live-oracle-tests.txt`;<br>- substrate's release gate runs nextest over `--workspace --no-fail-fast` (rust-auto-release.yml:382) and panics at m0_core_crud_parity.rs:154.<br>**Change:**<br>- Move the contract into `.config/nextest.toml`, which nextest discovers on its own: `default-filter = 'not (package(engenho-diff) & kind(test))'`, an `oracle` profile naming the four live-oracle tests, and the txt file's rationale as its header.<br>- Delete the txt in the same commit; its content has moved.<br>- test.yml's workspace leg runs nextest from substrate's pinned version. Doctests stay on `cargo test --doc`, and the `-p engenho-diff --no-run` compile leg stays.<br>- The four tests are not marked `#[ignore]`, as the txt file requires. `--profile oracle` or `cargo test -p engenho-diff` runs them wherever a live oracle cluster exists.<br>This needs a nextest version with `default-filter`; check substrate's pin. (buildci-2) | ci-gate | S |
| T0.3 | **Publish only after the gate.**<br>(a) release.yml pushes only `:<tag>`, and a `promote-latest` job depends on every leg and checks that all 12 assets exist.<br>(b) In substrate's rust-auto-release, `bump` (which creates the tag) waits for `test`. The job condition is written out as `!cancelled()` and (test succeeded, or tests are waived and test was skipped); a plain `needs:` would skip `bump` for every repo that waives tests. Prove that condition in a throwaway repo first (the file's own `pending-release-path` row), after a census of the ~52 consumers' test results.<br>(c) An opt-in artifact-only mode for rust-binary-release. (buildci-1) | ci-gate | M |
| T0.4 | **A lint gate that can fail.**<br>- Add `[lints] workspace = true` to the 14 members that lack it, plus a test that names any member without it.<br>- Before changing the gate, run `cargo clippy --workspace --all-targets --all-features --locked --keep-going` and save the output to a file.<br>- Gating set:<br>&nbsp;&nbsp;- `let_underscore_must_use`;<br>&nbsp;&nbsp;- `allow_attributes_without_reason`;<br>&nbsp;&nbsp;- `disallowed_methods` on `Path::exists`;<br>&nbsp;&nbsp;- once T2.6 lands, `disallowed_methods` on every way to spawn a task or thread: `tokio::spawn`, `tokio::task::spawn_blocking`, `JoinSet::spawn`, `Handle::spawn`, `std::thread::spawn`.<br>- Declare the gating set in **both** clippy.toml files, because engenho/clippy.toml replaces the root file rather than extending it. Pedantic lints stay at warn.<br>- A `gate-check` cargo alias runs first in CI and in a pre-push hook. Set `cache-on-failure`.<br>- Delete `continue-on-error` once the gating set is clean. Add `pending-clippy-debt: <n>` at the top of CLAUDE.md. (buildci-3/-4, substrate-3) | ci-gate | S, then L |
| T0.5 | **Ratchet the panic sites down.** Add `#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used, clippy::panic))]` to the six daemon-core crates and remove the 48 sites. Allow `#[allow(…, reason)]` only on invariants that are proven. (reliability-3 c4) | ci-gate | M |
| T0.6 | **Tests that pin behaviour.**<br>- Add `ci/seam-files.txt` listing gc.rs, watch_driver.rs, probe.rs, backoff.rs, native_backend.rs and every new seam.<br>- Run cargo-mutants nightly, and with `--in-diff` on pushes that touch those files. A surviving mutant fails the run unless an allowlist row says why.<br>- Standing rule: every commit below ends with a mutation pass over the functions it touched. | ci-gate | S |
| T0.7 | **Tests that pin the *right* behaviour.** Any test asserting Kubernetes semantics takes its expected value from an upstream artifact vendored into the repo: the k8s.io/api testdata corpus (T4.6), or an upstream test table ported with a `path@v1.34` header.<br>- **Tables to port:** RBAC `ResourceMatches`; prober result handling; container exit and restart mapping; watch 410 and bookmark semantics; GC owner resolution; how client-go's reflector and kube-rs's watcher handle a watch 429 (T3.7).<br>- **Why:** mutation testing shows that a test pins something; only an external oracle shows it pins the right thing (`pod_ip.is_none()` killed every mutant).<br>- **Wrong pins to invert as each fix lands:** probe.rs:1279-1295, state.rs:1891-1895, fjall_store.rs:1212, r7_8_listwatch.rs:402, kubelet server.rs:420, controllers meta.rs:135, uc_unified_loop.rs:212, pv_binder.rs:1152, the one-directional `subresource_match` (authz/mod.rs:811-850), fonte's kube_app_reconciler test at :17. | ci-gate | M |
| T0.8 | **Link less C.**<br>- rust-cross-build always appends `--bin <bin>`, and adds `-p` only when it is passed; defaulting `-p` would change ~17 callers. This removes libssl/libcrypto from the released engenho-mcp and makes both aarch64 legs pass.<br>- Then switch sui's workspace reqwest to rustls (sui/Cargo.toml:291).<br>- Then ban openssl-sys and native-tls in deny.toml. (buildci-5) | ci-gate | M |
| T0.9 | **Be truthful about the build path.** Set `opt-level = 3`, with a comment that the Nix-built daemon never reads this profile, and add `[profile.stress]` for property tests. Destination, owned by substrate: lockfile-builder gains `runTests` and honours the Cargo profile, so `nix flake check` compiles and tests engenho and test.yml can fold into it. (buildci-6) | hygiene | S / L |
| T0.10 | **Census harness.** `engenho census --predicate <name>` evaluates one named check, with no side effects, over live state and prints counts by GVK and reason.<br>**Input**, one of:<br>- a LIST through the apiserver, for checks over objects;<br>- a copy of a node's data directory, for store checks. FjallStore replays its log on open, so a copy taken after the process has stopped is consistent.<br>**Checks.** The census calls, unchanged, the functions the change will ship:<br>- RBAC matching over live roles (T4.2);<br>- `NodeLedger` headroom (T1.5);<br>- each `FilterPlugin` against every bound pod on that pod's own node (T5.7);<br>- defaulting, validation and schema checks per GVK (T4.4, T4.5);<br>- ReplicaSet matching and template normalization (T4.10);<br>- `open()` on a copied data directory (T3.4). | tool | S-M |
| T0.11 | **One rollout gate.** Each named gate has a `Rollout{Shadow, Enforce}` value, set in code per release, with no operator knob. There is one metric family, `engenho_would_reject_total{gate,reason}`, and one Event reason. In Shadow the gate logs and counts what Enforce would refuse. Used by T3.4's boot tripwire, T4.2's would-deny log, T4.4, T4.5 per GVK, T4.6 per row, and T5.7 per filter. | type | S |

**First commit (T0.1).**
- In `.github/workflows/test.yml`, add a Nix install step before `cargo test --workspace`, extracted into `pleme-io/actions/nix-setup`.
- Replace the KNOWN BLOCKER block (test.yml:42-66) with the facts measured in run 35418242848, and make the same correction in CLAUDE.md:250-256 and CLAUDE.md § CI + gating.
- Expected next run: red only on `m0_1_single_node_convergence` and `m0_6_namespaced_reconcile`, with `StoreStillShared { strong_count: 2 }`. Anything else that goes red is a new finding and gets recorded, not skipped.
- No runtime risk.
- If the installer is refused, skipping the three nix tests by name is the worse trade. They are the only tests that run a real native process, and the native backend is where the probe defect lived.

### T1: Report only what was seen (class A)
*At every place that observes the world, "seen", "absent" and "not observed" are different constructors.*

| ID | Change | Tier | Size |
|---|---|---|---|
| T1.1 | **Probes get a third arm.** Add `ProbeObservation::Blind(BlindCause{NoTargetAddress, RuntimeUnavailable, ProberSetup})` (probe.rs:491).<br>- The two `let Some(ip) = pod_ip else { return Failure }` sites (:663, :682) become `Blind(NoTargetAddress)`.<br>- An exec that fails in the runtime or the transport becomes Blind. A non-zero exit, exit 127 and a timeout stay Failure.<br>- On Blind, the fold records `last_run` and leaves both counters alone, as upstream's prober does.<br>- `needs_restart: bool` becomes `Option<ProbeTrip>`. `ProbeTrip`'s constructor is private to its own submodule, because Rust privacy is per module, and it is called only in the Failure arm once the threshold is crossed.<br>- A probe entering Blind emits a Warning event. After N consecutive Blind results, set a pod condition. Never restart on Blind.<br>(kubelet-2 = testing-6A = obs-3) | type/constructor | S |
| T1.2 | **Container exits and re-adoption.**<br>**Commit 1:**<br>- Put `ExitDisposition{Code(i32), Signal(i32)}` inside the existing `cri::RunState` (cri.rs:102). It replaces `running: bool, exit_code: Option<i32>` (backend.rs:493-502). Do not add a second RunState.<br>- `is_success()` is true only for `Code(0)`. The native backend maps `ExitStatusExt::signal()` (native_backend.rs:473). Delete the three `unwrap_or(0)`.<br>- `should_restart` takes the disposition. For Unknown it follows upstream: restart under Always and OnFailure, and mark the pod Failed under Never.<br>- An unobserved exit stays Unknown internally. On the wire it is rendered as upstream does: terminated, exit code 137, reason `ContainerStatusUnknown`.<br>**Commit 2:**<br>- When polling a container's status fails, withhold the write instead of fabricating a container that never started (kubelet.rs:3430-3441, :3977).<br>- `ever_started` is latched from the local record on the app-container path, as the init path already does (:3661-3670).<br>- **A pod the kubelet has no local record for, on a backend that cannot re-adopt, whose stored status shows a started container, is Unknown, not "never started."** Under Never it then fails visibly; otherwise the restart is counted. This replaces today's silent re-run in place on every ryn restart.<br>- Before commit 2 ships, tell the operator: Job pods on ryn have been re-run in place across restarts. Afterwards they fail visibly, and JobController replaces them within `backoffLimit`.<br>- podman_api decodes `InspectState.status`.<br>This is live on ryn today: a SIGKILLed `Never` pod is published as Succeeded, and JobController counts it. (kubelet-1 = machines-2) | type/constructor | S + M |
| T1.3 | **One readiness derivation.**<br>(a) `register_node` only creates the Node if it is absent.<br>&nbsp;&nbsp;- On create, it writes Ready=Unknown with reason `NodeStatusNeverUpdated`.<br>&nbsp;&nbsp;- On an existing Node, it merges only capacity, allocatable and the well-known labels, with CAS. It never touches `spec`, taints or other labels (runtime.rs:1219-1247).<br>(b) `publish_node_readiness` writes with CAS at the revision it read and retries once after re-reading. Today it writes the whole Node without a precondition, which can undo a concurrent cordon (kubelet.rs:727-733).<br>(c) Lease renewal moves to its own owned interval task. It renews only while the kubelet's pod-sync heartbeat (T2.6) is younger than a threshold modelled on upstream's PLEG health check, and later only while RuntimeHealth is healthy. A long image pull then no longer flips Ready, while a hung tick still does. This deviates from upstream on purpose: engenho's Lease is the only input to its readiness projection, so it must carry progress.<br>(d) `is_schedulable` calls `node_lease::project_ready_condition` (node_lease.rs:456), the same function handler.rs:512 uses, and the two "assume schedulable" branches are deleted.<br>Until (c) lands, config parsing rejects `controllers.fallback_interval_seconds ≥ 40` (controllers.rs:182). | mitigated (one call site) + parse-boundary | S (a) + M |
| T1.4 | **Identity is the UID.**<br>- Dynamic PV names and hostPath directories come from the PVC's `metadata.uid` through `PvName::for_claim(&ClaimUid)`. The field is private and the type lives in its own submodule.<br>- A missing uid leaves the claim Pending with a typed reason.<br>- The hostPath becomes `<root>/pvc-<uid>_<ns>_<name>`.<br>- `pv_claimref_compatible` compares `claimRef.uid` when both sides carry one.<br>- Create-if-absent replaces the unconditional Put.<br>- Existing PVs keep their names.<br>Sites: pv_binder.rs:275-284, :389, :518, :630-638. This is live on every cluster: today a PVC recreated under the same name silently mounts the deleted claim's directory. (plugins-5a) | type/constructor | S |
| T1.5 | **Scheduler capacity has one ledger.**<br>- `NodeLedger::seed(nodes, pods)` plus `debit`, over a private signed map that is clamped only when read. It replaces the two clamp loops (scheduler.rs:91-95, :155-158).<br>- A three-armed `holds_capacity`: only an observed Succeeded or Failed phase releases capacity; an unknown phase holds it.<br>- Requests follow upstream's formula: sidecar init containers, the maximum over plain init containers, and pod overhead.<br>- Every one of these changes shrinks free capacity. First run the ledger through T0.10 against the live pods on each node and publish per-node headroom. Do not ship if any node would go negative.<br>(scheduler-5) | type/constructor | S |
| T1.6 | **Integer fields have three states.** Add `SpecInt<'a>{Absent, Int(i64), Malformed(&Value)}` in engenho-types, with no helper that collapses them. In the same commit, delete `spec_i64` and `read_i64` (scale.rs:96), and convert the 7 `spec_i64` sites and about 8 inline `.as_i64().unwrap_or` sites. Malformed means do nothing and emit a structured warning. (types-6) | parse-boundary | S |
| T1.7 | **Filters from declared reads, not a list.** Each driver's `KindFilter` is derived from the controller's declared reads instead of the hand lists at runtime.rs:2633-3056. It lands with T2.6's catalog. HPA's hand-listed target group and version moves to T5.11, because HPA is not spawned. | type/constructor | S |
| T1.8 | **Counts come from effects.**<br>- An exhaustive `Effect::of(ResourceOp)` goes at every increment that follows a store write: 27 in spawned code, 37 in total.<br>- Backend effects (service_router.rs:975 and :985, network_policy_controller.rs:470-471) count only on `Ok`.<br>- `write_status_cas` reports `PatchRejected` and `ApplyConflict` as rejected.<br>- T2.3's sweep builds `ReconcileReport` from per-object outcomes, so examined = changed + unchanged + skipped + failed holds by construction. | type (for migrated sweeps) | M |

**First commit (T1.1), `engenho-kubelet/src/probe.rs`:**
- Add the Blind arm, and remap :663, :682 and exec runtime errors to it.
- `fold_probe_observation` (:532): on Blind, record `last_run`, increment `consecutive_blind`, and return the latched verdict.
- Invert `the_same_healthy_workload_fails_when_it_reports_no_address` (:1279-1295) so it asserts `Blind(NoTargetAddress)`.
- Add tests:
  - 1,000 Blind folds against `failure_threshold: 3` trip nothing, while 3 Failures do trip.
  - A seeded HTTP 500 is Failure, and 399/400 is the boundary. This kills the surviving mutant that replaces `(200..400).contains` with `true`.
- On HEAD, the fold test trips `needs_restart`.

### T2: Bounded, owned loops (classes B and C)
*At most one pending re-tick per driver. Retries that slow down. One object's failure costs only that object. Nothing wakes itself. Every long-lived task is a named child that something outside it watches.*

| ID | Change | Tier | Size |
|---|---|---|---|
| T2.1 | **One requeue slot per driver.**<br>- Delete `arm_requeue` (watch_driver.rs:298-308).<br>- Add a pure `next_wake(&Result<ReconcileOutcome, ControllerError>) -> Option<Duration>`.<br>- `run` (:197) keeps `requeue_at: Option<Instant>`. It is cleared when a tick starts, because a tick is a full sweep, and set from the tick's outcome, with the earliest deadline winning.<br>- `wait_for_relevant_event` sleeps until `min(fallback, requeue_at)` through a new `EventOrTimer::Requeue`, both in the live-stream `select!` and in the branch with no stream (:334-336).<br>- **Second holder of the store, fixed in the same commit:** BookmarkTicker keeps an upgraded `Arc` across `tick_once().await` (watch_backend.rs:784-795). Shutdown calls a new `StoreMesh::quiesce()` that aborts and awaits the ticker before `try_unwrap`, and `StoreStillShared` gains `after: ShutdownStage`, naming the stage at which the count failed to reach 1.<br>(reliability-2 = controlloop-1) | type/constructor | S |
| T2.2 | **Classify retries by type, and make them grow.**<br>- `classify` becomes an exhaustive match over `ControllerError`/`StoreError`, with no `to_string` and no wildcard (error.rs:44-51). `Store(ClientWriteFailed)` is Transient. ConfigInvalid, InitializeFailed and Fatal are Declarative.<br>- Delete `retry_after` (:58-66).<br>- One `Curve{base, cap}` with a `const fn delay(n)` that panics at compile time when cap ≤ base. It is shared by:<br>&nbsp;&nbsp;- the kubelet's backoff.rs (10 s→300 s);<br>&nbsp;&nbsp;- the re-subscribe `grow` (100 ms→30 s);<br>&nbsp;&nbsp;- a new per-driver `consecutive_failures` curve (1 s→60 s, reset on Ok).<br>- The kubelet's flat 1 s volume-pending retry gets a per-pod count.<br>- Replace the test at uc_unified_loop.rs:212, which asserts that the message text decides the class.<br>(controlloop-4 + -3) | type/constructor | S |
| T2.3 | **Isolate each object, with a typed scope.**<br>- `ControllerError::scope() -> {Item, Sweep}`. Store errors are Sweep.<br>- A `sweep` helper whose per-object closure returns `Result<ObjectOutcome, SweepAbort>`, so an Item error cannot `?` out of its object.<br>- The kubelet goes first: match per pod in its loop (kubelet.rs:2041-2061). The ServiceAccount-projection `?` (:2600-2608) becomes Pending plus a Warning plus a skip.<br>- A Declarative item failure emits an Event on that object.<br>(controlloop-2 + kubelet-4) | mitigated at the helper | M |
| T2.4 | **Start or observe per container, not per pod.**<br>**Commit 1:** the sidecar arm calls the start path, as the `Complete` arm already does (:3727). Today a pod with a native sidecar latches `init_complete` and returns without starting its app containers (:3784-3791).<br>**Commit 2:** for a pod that already has a record, start any missing containers before observing, so a container whose first start failed is no longer abandoned once a sibling has started. **Every start attempt, first or retry, goes through that container's backoff `Curve`** (backoff.rs, 10 s→300 s, reset after 10 minutes running, as upstream does). Ground-truth instance 4 was a failed start with no backoff, and this commit must not bring it back. Test: a container whose start always fails is attempted at most 16 times in one virtual hour. | test-gated | S + M |
| T2.5 | **The scheduler stops waking itself.**<br>- Add `upsert_condition_cas` next to `write_status_cas`. It replaces the condition by type, keeps `lastTransitionTime` when the status is unchanged, and proposes nothing when nothing changed. `mark_unschedulable` uses it, and the FailedScheduling Event is emitted only when a write happened.<br>- Then bind with `patch_cas` at the observed revision, with an exhaustive `ResourceOp → BindOutcome` mapping.<br>- A closed `PodSchedulingState{Bound, Terminating, OtherScheduler, NoRevision, Schedulable}`.<br>- `Binding` can be constructed only from `Bound`.<br>(scheduler-2A, -4) | type/constructor | S + S |
| T2.6 | **Owned children.**<br>- A closed `enum Child` covers the 20 drivers, both listeners (:374, :416), the node-lease task (T1.3c) and, under `teia-nats`, the NATS listener. `spawn_drivers` iterates it, and each driver's `KindFilter` is derived from it.<br>- `Child` declares `TickState{Stateless, Stateful}`. The kubelet is Stateful, because its `local` map lives across ticks.<br>- `WatchDriver::run -> Infallible`, and `spawn(child) -> DriverHandle{join: JoinHandle<Infallible>, beat: Arc<Heartbeat>}`.<br>- `Runtime.drivers` (:54) becomes a `JoinSet` plus an id→`Child` map, which `main` watches together with the stop signals. A child that completes has, by construction, panicked or been aborted. Log it at ERROR and mark it Dead; there is no respawn.<br>- **Every spawn site gets a disposition in the same commit:**<br>&nbsp;&nbsp;- driver loops, listeners and the lease task become `Child`;<br>&nbsp;&nbsp;- the Raft RPC task (mesh.rs:246) and BookmarkTicker (watch_backend.rs:784) are owned by `StoreMesh` and awaited in `quiesce`;<br>&nbsp;&nbsp;- request-scoped pumps are exceptions with a stated reason: podman_api.rs:723, etcd server.rs:870/976/1017/1054, etcd_facade.rs:280, apiserver server.rs:126/193;<br>&nbsp;&nbsp;- `ControllerRuntime`'s spawn (controllers runtime.rs:91) is fenced with it;<br>&nbsp;&nbsp;- nats_listener.rs:96 becomes a `Child` under `teia-nats`.<br>(obs-1, testing-1A, controlloop-6 part 1) | compile error inside the catalog; CI-caught outside it | M |
| T2.7 | **Contain panics and exits. Ships after T2.8.**<br>- `tick_observed` wraps `tick()` in `catch_unwind` for **Stateless** children only. They re-read everything from the store each tick, which is what controller-runtime's `RecoverPanic` relies on. A panic maps to `ControllerError::Panicked` with no targeted retry. The fallback timer re-ticks, so a panic caused by bad data costs one counted tick per fallback interval.<br>- **Stateful** children are not wrapped. A panic ends the child, T2.6 marks it Dead, and T2.8 makes that visible. For the kubelet, Dead is the park: nothing re-ticks over a torn `local` map, a poisoned std `Mutex` or a half-updated tokio lock.<br>- A panic hook that chains to the previous hook and counts panics.<br>- Listeners rebind using the existing capped backoff.<br>- `disallowed-methods` covers every spawn path (T0.4), with a stated reason at each request-scoped pump.<br>(reliability-1b, -4) | mitigated + ci-gate | S |
| T2.8 | **Derive health from observation.**<br>- Liveness per child: `Liveness{Unknown, Alive, Stalled{since}, Dead}`. Unknown is never rendered as ok.<br>- `/livez` and `/healthz` aggregate through a `LivenessSource` trait defined in the apiserver, and return upstream's `[+]/[-]` verbose body.<br>- `/readyz` passes only when all three hold: a linearizable store read succeeds under a timeout, every child is past Unknown, and the node is not draining. It does not mean "I am the leader".<br>- The kubelet's `/healthz` comes from its own liveness row and its pod-sync age.<br>- `STUCK_TICK_AFTER` (runtime.rs:2629) and the pod-sync threshold move into config, with their bounds cross-checked.<br>- A pure `Freshness{NeverObserved, Fresh, Stale}` judge in substrate core.<br>- `/metrics` gains:<br>&nbsp;&nbsp;- the real `engenho_store_revision`, through a `MetricsSource` seam;<br>&nbsp;&nbsp;- `engenho_panics_total`;<br>&nbsp;&nbsp;- `controller_runtime_reconcile_total{controller,result}`;<br>&nbsp;&nbsp;- last-tick timestamps;<br>&nbsp;&nbsp;- watch overflow ends per client (T3.7);<br>&nbsp;&nbsp;- `engenho_would_reject_total` (T0.11).<br>- A propose-rate detector at `StoreMesh::propose`, keyed by (key, controller) through a task-local set in `tick_observed`.<br>(reliability-4, obs-2, obs-5) | detector | M |
| T2.9 | **The stop path.**<br>- `main.rs:181` selects over ctrl_c and `SignalKind::terminate()` into `StopCause{Interrupt, Terminate}`, then runs, in order: `Runtime::shutdown`, `StoreMesh::quiesce`, `FjallStore::flush` (catalog and `last_applied` in one batch, the same pair `apply` persists), and `terminate`.<br>- After a clean stop there is nothing left to replay. That is the durability rev 1 wrongly assumed SIGTERM handling provided, and it is what makes rollback across a semantics change safe (edge 20).<br>- Ships only after CI shows the four StoreStillShared tests green.<br>(reliability-1a) | exhaustive match (SIGTERM, SIGINT) | S |
| T2.10 | **Cleanup obligations are values.**<br>- Split `MountSource::HostDir` into `UserHostPath` (never removed) and `Materialized` (only the materializer can build it, and it is removed on teardown). `LocalPod.volume_teardowns` is filled by an exhaustive `teardown_obligation` and consumed by `cleanup_pod_containers`.<br>- Stop and remove results are no longer thrown away (kubelet.rs:3081-3082, :3892-3893).<br>- The native stop sends SIGTERM to the child's pid, escalates to SIGKILL after `terminationGracePeriodSeconds`, and reaps the process. It does this in an owned termination task (a `Child` or `JoinSet` member), never inline in the tick. `remove` refuses to drop a record whose process has not been reaped.<br>- **No `process_group(0)`** until native re-adoption or BUTAI's supervisor exists (edge 12). When launchd stops the job, it kills the job's process group, and that is the only thing that keeps a restart from leaving a second copy of every native workload running. A native workload's grandchildren are therefore reaped only by launchd, and the code says so.<br>(plugins-2a, kubelet-3) | type/constructor | M + M |

**First commit (T2.1), `engenho-controllers/src/watch_driver.rs` plus `StoreMesh::quiesce`:**
- Make the changes above. `next_wake(Err(Declarative))` returns `None`. A Transient error returns 1 s until T2.2 replaces it with the curve.
- Tests under `tokio::time::pause`:
  - A controller that always returns `Requeue(1 s)`, an event every 5 s, a 30 s fallback, one virtual hour. Assert at most one tick in flight and at most 4,440 ticks.
  - The same with a controller that always returns a Transient error.
  - `next_wake` on a Declarative error returns None, which kills the surviving `==`→`!=` mutant.
- Record the red run on HEAD: more than one tick in flight, and a tick count that grows faster than linearly.
- **Predicted side effect:** the four StoreStillShared tests pass, because both remaining holders of the store are removed or awaited. If they stay red, `after: ShutdownStage` names the stage, and the kubelet `Arc` reachable through the PodLogReader adapter is the next candidate.

### T3: The store tells the truth across restarts
*A revision means a change. A floor means history that can be replayed. A delete carries a clock. A log entry means the same thing to every binary that reads it.*

| ID | Change | Tier | Size |
|---|---|---|---|
| T3.1 | **Restart oracle** (test only, lands first). Five cases:<br>(1) FjallStore: 1 persisted apply, then 3 batched applies within 5 s; drop without `terminate`; reopen via `StoreMesh::start_durable`. Every acknowledged write must be present, `current_revision` unchanged, and `changes_since` below the blob's revision must return `Err(CompactedTooOld)`.<br>(2) 40 applies of 50 entries within one persist window, then `build_snapshot`, commit, `purge(1500)`, drop, reopen.<br>(3) A child process proposing in a loop is SIGKILLed mid-propose. Every write it acknowledged must be present after reopening.<br>(4) A clean `Runtime::shutdown`, then reopen. Nothing is left to replay and the revision is continuous.<br>**Red runs, recorded when the test landed (7ea9ff2, 2026-09-19): cases 1, 2 and 4 were red, and case 3 was green.** Case 1 returned `Ok` with a strict subset; case 2 failed in `Raft::new` with "expected index"; case 4 was red until T2.9. Each fixing item deleted only its own `#[ignore]`: T3.3 case 1 (382e8e6), T3.4 case 2 (e261535), T2.9-store case 4 (3d45b6a).<br>(5) **Replay compatibility.**<br>&nbsp;&nbsp;- Forward: a log fixture recorded by the released binary replays through the working tree to identical revisions and catalog bytes.<br>&nbsp;&nbsp;- Backward: a log written by the working tree replays through the previous release, built from its tag in CI. test.yml's `replay-backward` job runs `ci/replay-backward.tlisp`: it records with the working tree into `ENGENHO_REPLAY_FIXTURE_DIR`, then runs the previous release's own case 5 on that directory. v0.53.118 and every older release predate the harness, so until a release contains it the job reports `predates-harness` and replays nothing. Its limit, measured through 7ea9ff2 (before T3.5 added `ResourceOp::Unchanged`): the previous release decodes the results this tree recorded, so a new `ResourceOp` variant fails that decode before any entry replays, although `ResourceOp` is never logged.<br>&nbsp;&nbsp;- Green on HEAD. It is the gate for T3.5 and T4.5. | ci-gate | S + S |
| T3.2 | **No full-catalog clone on a read path.**<br>(a) `list_page_at_revision` runs under one guard in both backends, with the range bounded to the GVK. state.rs:1263-1278 uses `Unbounded` plus a post-filter, and mesh.rs:444 clones the whole catalog on every page of every informer relist.<br>(b) `ResourceCatalog` becomes `pub(crate)` under `#![deny(private_interfaces)]`, so a `pub fn` returning it is a compile error. `current_catalog()` goes with it, and the production clones (etcd_facade.rs:155, :203, :230, :253; mesh.rs:543) get scalar or single-guard accessors. Owned collections derived from the catalog are caught by a test over the store's public signatures.<br>(apistore-3A + distributed's unowned finding) | type + ci-gate | S + M |
| T3.3 | **The floor on load equals the current revision.** In `ResourceCatalog`'s Deserialize, set `compacted_revision = current_revision` (state.rs:315); keep reading the old field but ignore it. Flip state.rs:1891-1895 and fjall_store.rs:1212 so they assert the right semantics. Later, seal it: `WatchHistory{ring, floor, capacity}` with private fields and one constructor, used by `changes_since`, `state_at` and the etcd facade. (apistore-1 = distributed-1) | type/constructor | S + M |
| T3.4 | **The durable image is never older than a snapshot, and replay is idempotent.** This path is live: snapshot build and purge run on every long-lived node.<br>(a) `build_snapshot` writes the catalog blob and `last_applied` together with the snapshot data and metadata. Today it writes neither (:715-750).<br>(b) Both snapshot paths write through one `keyspace.batch()`. `install_snapshot` already writes the right keys, but as separate inserts (:893-935).<br>(c) `apply` skips entries at or below `last_applied`.<br>(d) `open()` loads state from the snapshot data when it is newer than the blob.<br>(e) The inconsistency check ships as `Rollout::Shadow` (log and count only) for one release. Before that release, T0.10 runs the new `open()` against copies of rio's, ryn's and plo's data directories. It becomes Fatal only in a later release, and only when neither loading the snapshot nor replaying the log can produce a state.<br>(f) Correct fjall_store.rs:114-119.<br>No new keys, so the previous binary reads the same image.<br>(distributed-2) | detector + a single structural site | M |
| T3.5 | **A revision means a change.**<br>- One pure `unchanged(prior, candidate, manager)` check in the state machine. It ignores resourceVersion, the computed generation and the caller's managedFields `time`.<br>- It runs before any stamping, in `apply_put` (:500), `apply_patch` (with `bumped_at` at :643 moved to after the merge) and `apply_ssa_command` (:780).<br>- Equal content returns a **new** `ResourceOp::Unchanged`, not `NoOp`. `ResourceOp` is a response and is never written to the log, so the new variant is safe to replay.<br>- Add identical-write cases to the r9_mvcc_revision generator.<br>- Before landing, list every writer that rewrites identical content to nudge a watcher.<br>- The four hand-written guards (cni_status, served_capability, network_policy_controller, pdb) stay, because they save a Raft round trip.<br>**Replay safety:**<br>- `Put` and `Patch` gain `#[serde(default)] semantics: ApplySemantics{V0, V1}`, the same optional-field pattern `patch_type` and `apply` already use (command.rs:96-111).<br>- A missing field means V0, which is today's rules. New constructors write V1, and `apply` matches the field exhaustively. Old entries therefore replay identically under the new binary.<br>- An old binary ignores the field and would apply V0 rules to V1 entries. So rollback across T3.5 starts only from a clean stop, which T2.9 leaves with nothing to replay. After a crash: restart the new binary once, stop it cleanly, then roll back.<br>- Paths covered: put and patch/SSA. That is every content write, status included, since `write_status_cas` proposes `patch_cas` (status.rs:150-156). `apply_txn` has no caller that issues it.<br>- Gated by T3.1 case 5. Ships after T2.9 and T3.9a.<br>(fonte-4a + scheduler-2B) | type (V1 entries, single apply path) | M |
| T3.6 | **A delete carries its clock.**<br>- `ResourceCommand::delete()` always stamps `now_rfc3339_utc()`, and `delete_at` takes a required `String`. The wire field stays `Option`.<br>- Remove the apiserver's `prior.filter(object_has_finalizers)` gate (handler.rs:1679-1682).<br>- Fix all ten callers that delete without a clock: gc.rs:179, replicaset.rs:199, statefulset.rs:367, daemonset.rs:217/432/471, job.rs:498, store_ledger.rs:251, crd.rs:827, drv_committal.rs:97.<br>- Writers skip children that already carry `deletionTimestamp`, and gc counts only real changes. Today these deletes return NoOp (state.rs:955-957) and are proposed again on every tick.<br>- Replay-compatible in both directions: the field already exists, and old `None` entries replay as they do today.<br>(distributed-5) | mitigated (constructor) | S |
| T3.7 | **Watches say how they ended.**<br>- An overflow after which the client has made progress ends with a BOOKMARK at `last_seen` (when bookmarks were requested) and a clean close, as upstream's cacher does with unresponsive watchers.<br>- **An overflow with no progress,** where nothing was delivered past the watch's start revision, would send the client straight back into the same overflow. It ends instead with an in-band `Status{429, TooManyRequests, retryAfterSeconds}` that only `WatchEnd::NoProgress` can build. client-go's reflector backs off on a watch 429 and resumes from its resourceVersion, and kube-rs's watcher applies its backoff; both are rows in T0.7's ported tables. If either client fails its row, the fallback is an HTTP 429 on that client's next watch.<br>- `status_410_line` takes a `Compacted` newtype that can be built only from `WatchGone::CompactedTooOld` (router.rs:1372-1377).<br>- A relabel that takes an object out of a selector emits DELETED from the object's previous state: an `Arc<Change>` ring built once at apply time, plus `project(&Change, passes)` over a 12-cell table. Port `watch-configmaps-label-changed`.<br>(apistore-6 L1, apistore-4) | type/constructor | S + M |
| T3.8 | **The etcd façade.** One atomic `watch_from(prefix, start)` replaces subscribe plus `changes_since`. Every end, including a bare channel close, sends a canceled response with a reason. A dropped store returns `Unavailable`, never an empty Ok. (distributed-3, reliability-5) | mitigated | M |
| T3.9 | **resourceVersion as a typed read contract.**<br>**T3.9a** (wave 1, before T3.5): a WATCH whose resourceVersion is ahead of the store gets an in-band 410, not upstream's 504. Flip r7_8_listwatch.rs:402, which pins today's behaviour: a watch ahead of the store is quietly served from the current revision, so after a restore or a replay that renumbers history, clients go stale silently. client-go relists on a 504, but kube-rs retries at the same resourceVersion, which would wedge pangea-operator.<br>**T3.9b** (wave 3): `ReadConsistency{MostRecent, Any, NotOlderThan, Exact}`, constructed only by `ListWatchParams`, and LIST honours NotOlderThan and Exact.<br>(apistore-5) | parse-boundary | S + S-M |

**First commit (T3.1):** `engenho-store/tests/restart_is_a_kill.rs`, test only.
- Cases 1-3 are red today; record the red runs in the commit message.
- Case 4 is red until T2.9.
- Case 5 is the replay harness.

Then T3.2a, then the one-line T3.3, in that order. The honest 410 after a restart makes clients relist, and those relists must not clone the whole catalog on every page. That is the shape of the 5-day Flux wedge on rio.

### T4: One border for every write
*Authorization judges the request the dispatcher executes. Every write verb computes the object that will be stored, then defaults, validates and CAS-writes it the same way. Surfaces on the node itself stay reachable only from the host until they can authenticate.*

| ID | Change | Tier | Size |
|---|---|---|---|
| T4.1 | **Parse once.** A layer before authz computes `RequestInfo` once from the percent-decoded path and inserts it into the request extensions. `authz_middleware` (router.rs:529-560) and `ResourceCoords` (coords.rs:322-350) both read that one value. A missing value is a typed 500; the path is never parsed a second time. A router test asserts that every route layer carries this layer, including WebSocket upgrades and watch routes. The kubelet API and the etcd façade never pass through this router (T4.9). | type/constructor | S |
| T4.2 | **RBAC matches the way upstream's `ResourceMatches` does:** `*`, the exact `resource/subresource`, and `*/subresource`.<br>- Delete the branch that lets a bare parent match (authz/mod.rs:462-465), and its comment.<br>- Check the `resource/*` branch against the vendored v1.34 source before keeping it.<br>- Port upstream's test table, and add the missing direction to `subresource_match`.<br>- The live escalation it closes: `create serviceaccounts` currently also grants `serviceaccounts/token`. `pods/exec` is also authorized today, but the apiserver does not serve it.<br>- Pre-flight: run T0.10 over every live Role and ClusterRole on rio, ryn and plo. List every subject other than `system:masters` that reaches a subresource only through a bare parent. If that list is not empty, ship one release in `Rollout::Shadow`, logging would-deny decisions, before enforcing. | ci-gate (ported table) | S |
| T4.3 | **Total metadata access.**<br>- `object_mut` and `array_mut` treat absent or null as empty, and anything else as `ShapeError::Wrong{path, found}`.<br>- `set_owner_reference -> Result<bool, ShapeError>` (owner.rs:44-66). Its nine production callers: daemonset.rs:198, deployment.rs:208, endpoints.rs:261/352, job.rs:210/510, owned_children.rs:363, replicaset.rs:183, statefulset.rs:351.<br>- `network_policy_controller::annotated` returns `Result`.<br>- Every caller skips that object with an Event and continues.<br>- Regression test: a NetworkPolicy with `annotations: null`, followed by a clean one that must still get annotated.<br>(reliability-3 c1) | type/constructor | S |
| T4.4 | **Normalize at the border, and only where the request body *is* the stored object (POST, PUT).** Drop null labels, annotations, ownerReferences and finalizers under `metadata` and `spec.template.metadata`. Return 422 for metadata that is not an object, map values that are not strings, and ownerReferences that is not an array. Before it ships, T0.10 counts the stored objects that would get a 422 and the Deployment templates it would change. T4.10 lands first. (reliability-3 c2) | parse-boundary | S |
| T4.5 | **One write pipeline** (destination).<br>**Pipeline:** read the prior object at its revision; compute the candidate, using the store's own patch/SSA merge extracted into pure functions both sides call; normalize the candidate; default it; validate it; apply CRD-schema defaults and validation; run admission; propose a CAS `Put` at that revision, with a bounded retry on Conflict. If nothing changed, return `Unchanged` early.<br>**Sealing:** a private `WritePlan` is the only way a user write reaches `propose`. `dryRun` becomes computable for PATCH and SSA (today it is refused, handler.rs:1517-1524, :1580-1587).<br>**Three preconditions per verb stage:**<br>(i) **Stored objects were never validated,** because SSA skipped all four checks and Flux creates objects with SSA. So T0.10 runs defaulting, validation and schema checks over every stored object. Each GVK is enforced only once its would-reject count is zero, via T0.11. Otherwise the first reconcile after enforcement returns 422, and Flux fails the whole Kustomization.<br>(ii) **Replay:** the store's merge is moved into the shared functions, not rewritten. T3.1 case 5 replays a recorded log through old and new code, byte for byte. Any intended change to the merge gets a new `ApplySemantics` version.<br>(iii) T4.10.<br>**Tier, stated plainly:** `WritePlan` seals only the apiserver's path. `propose` stays callable by anything holding `Arc<StoreMesh>`, including `register_node`. A sealed `Proposal` argument type is the destination.<br>Staged PUT → PATCH → SSA, one release each. | type/constructor (apiserver path) | L |
| T4.6 | **A typed border that runs the generated structs.**<br>- ObjectMeta gains `ownerReferences`, `generateName` and `selfLink`, plus a single generated OwnerReference with a three-armed `ControllerRef`.<br>- `Quantity` accepts a number or a string.<br>- `fieldValidation` is parsed into `{Ignore, Warn, Strict}`. Every row not yet enforced returns `Warning: 299`.<br>- A typed decode runs per catalog row in `Rollout::Shadow`. It persists the original `Value` and only counts mismatches.<br>- A row is enforced only once both its upstream-fixture entry and its accept-set entry are clean. kubectl 1.27 and later sends Strict.<br>(types-2/-5/-1) | parse-boundary | L |
| T4.7 | **Protobuf codec.** A transcoder driven by the protobuf descriptors, covering Time, MicroTime, Quantity, IntOrString, RawExtension and FieldsV1, exhaustive with no `_` arm. Fall back to JSON when the client's Accept header allows it. A `KnownLossy` ratchet starting at 36. **Measure first:** count requests per content type at router.rs:861/908/990 on rio for a day. If any client negotiates protobuf, this moves up to wave 1. (types-4) | parse-boundary | M |
| T4.8 | **Apiserver invariants as types.** `admit_object` and `admit_delete` replace the three `expect("…preserves Some…")` calls (handler.rs:1323, :1423, :1492). `resolve_subresource` returns the name together with the variant. `for_core_kind` returns `Option`. (reliability-3 c3) | type/constructor | M |
| T4.9 | **Node-local surfaces stay host-only until they can authenticate.** Config parsing rejects a non-loopback `kubelet_listen_addr` or `etcd_listen_addr` until :10250 has authentication and :2379 has mutual TLS. `hostNetwork` stays unimplemented under the same condition, because on rio and plo it would be the only way for a pod to reach the host's loopback. On ryn this closes nothing, because native workloads run as the operator (§5.4), and the plan does not claim otherwise. | parse-boundary | XS |
| T4.10 | **A Deployment finds its ReplicaSet by comparing normalized templates** (upstream's `EqualIgnoreHash`), not by the FNV hash of the raw template bytes (deployment.rs:53-64). serde_json maps are sorted here (`preserve_order` is not enabled), so key order is not the risk; content changes are. After this change, none of these can roll every Deployment at once: T4.4/T4.5 normalization, the `apps/v1` template-defaulting arm (deliberately absent today, defaulting.rs:51), or a change to the hash. Tests over T0.10:<br>- switching matchers creates zero ReplicaSets;<br>- normalizing every stored template creates zero;<br>- a real template edit still rolls out. | type/constructor | S |

**First commit (T4.1):**
- router.rs: add the `request_info` layer ahead of authz. `authz_middleware` reads the stored value instead of calling `from_method_path(&method, req.uri().path(), …)` (:533, :550).
- coords.rs: read the same value.
- Add the route-coverage test.
- Test: a Role that grants only `create` on `serviceaccounts`. `POST /api/v1/namespaces/default/serviceaccounts/foo%2Ftoken` must be judged and dispatched as the same (serviceaccounts, token, foo).
- Today authz sees a ServiceAccount named `foo%2Ftoken` with no subresource, while dispatch mints a token for `foo`. T4.2's matcher fix alone would not change that. I verified this by reading; the test is the proof.
- The matcher fix (T4.2) goes in the next release, after the census.

### T5: What ships is what runs (class D)
*Every artifact that describes running behaviour is either tied to it by a check, fenced out of the shipped closure, declared dormant, or deleted.*

| ID | Change | Tier | Size |
|---|---|---|---|
| T5.1 | **Closure gate.** A test parses the workspace manifests and computes the normal-dependency closure, with default features, of engenho, engenho-mcp and engenho-cluster-config-render. It asserts that the eight unshipped crates are absent and that substrate core pulls in no tokio or serde_yaml. Positive control: the closure contains engenho-controllers. Any per-module table is advisory and labelled as compile-reach. (substrate-2, reduced) | ci-gate | S |
| T5.2 | Put the NATS fabric behind a feature that is off by default (decision in §5.1). | parse-boundary | M |
| T5.3 | Delete engenho-machines (decision in §5.2). STATE-MACHINES.md's SM3 and SM6 rows name the code that actually implements them. In wave 3, `QuorumTracker` becomes the only quorum fold: a sealed `QuorumVerdict`, a `NonZeroUsize` threshold, and the MemoryLedger and StoreLedger mirrors deleted. | delete | S |
| T5.4 | revoada is a typed draft (§5.3). Correct these claims now: DISTRIBUTED.md:194; params.rs:64-66 and CONSISTENCY-FABRIC.md:114-118; engenho-etcd lib.rs:37; runtime.rs:410-412. Delete `RoutingPolicy::RoundRobin`, which always returns member 0. | — | S |
| T5.5 | fonte stops presenting mocks as a daemon: add `required-features = ["mock-universe"]` to its `[[bin]]`, and resolve and log a typed `Universe{Mock, Real}` at startup. | parse-boundary | S |
| T5.6 | **Carve substrate by what is reachable.**<br>- A tokio-free core keeps the name: error_kind, named, hex, hash_newtype, fingerprint, atomic_write, magic_blob, risca, relogio.<br>- One leaf crate carries every module a shipped crate *references*. That includes the Drv, receipt and verifier types the dormant controllers use (tiered_reconciler, drv, drv_build, roceiro, store_ledger, build_backend_roceiro).<br>- `engenho-substrate-incubator` gets only the seven modules nothing references: pesquisa, orcamento, compose_ir, oci_renderer, command_runner, fake_shell, disposable.<br>- Before plantio is ever wired, delete the one path that issues a PASS receipt for a check that never ran: `bootstrap_pipeline`, `impl Default for RoceiroChoice`, and FakeVerifier's allow-when-no-policy behaviour.<br>- Seal `NarBlob` and delete `CacheError::NotFound`.<br>(substrate-1/-4/-5) | type + hygiene | M |
| T5.7 | **Wire the scheduler's predicates in rather than deleting them.** A pod with `gpu=true` was bound to a `gpu=false` node carrying NoSchedule (measured 09-18).<br>- A closed `FilterPlugin{NodeReady, Cordon, NodeName, NodeSelector, TaintToleration, Resources}` over the existing functions.<br>- `Feasible`, a private non-empty set, can be built only by `filter()`.<br>- A `NoNodesObserved` result writes nothing.<br>- `pick(&Feasible) -> String`.<br>- Delete `skipped_no_node`.<br>**Preconditions:**<br>- T1.3(a) must land first, so a boot no longer wipes the taints and labels these filters read.<br>- A T0.10 census of every bound pod against each filter, on the pod's own node. Any filter with a non-zero count stays in `Rollout::Shadow`. ryn labels itself `kubernetes.io/os: darwin`, so any pod carrying upstream's `nodeSelector: kubernetes.io/os: linux` would be stranded there. Taints have never been enforced anywhere.<br>Affinity and spread wait; preemption stays unwired. (scheduler-1 c1) | type/constructor | M |
| T5.8 | **An unread config field is a compile error.**<br>- `Scheduler::from_config(&SchedulerConfig)` destructures the struct with no `..` (E0027).<br>- Route `scheduler.namespace` through.<br>- Either wire `tick_interval_seconds` or retire it as a typed flag.<br>- `revoada.topology` and `teia` get the same treatment: they are validated at boot (engenho-config lib.rs:227-243) and read by nothing.<br>- Apply the same shape to each `*Config` whenever its consumer is touched.<br>E0027 forces a decision for every field. Discarding one as `field: _` is still possible and only review catches it. | compile error | S |
| T5.9 | **CRI is refused at construction** (kubelet config_bridge.rs:127), with a typed reason, until it sets mounts and pod IPs. No node selects it. | parse-boundary | XS |
| T5.10 | **Claims are bound or deleted.**<br>- metrics.rs renders from a source, and a doc-truth test fails when the header names a metric family the output lacks.<br>- Each comment listed under class D becomes a named test, or is deleted when its code is next touched. That list now includes runtime.rs:1199-1206 and fjall_store.rs:114-119.<br>- The links to the non-existent `tatara/docs/daemon-supervision.md` point at T2.6 instead. | ci-gate / convention | S |
| T5.11 | **Dormant controllers are declared.** The 8 unspawned `Controller` types go into a typed `Dormant` catalog, each with a reason. A test fails when a type implementing `Controller` is in neither `Child` nor `Dormant`. HPA's hand-listed target group and version (hpa.rs:196-200) gets fixed when HPA is wired in, not before. | ci-gate | S |

**First commit (T5.1):** `engenho/tests/shipped_closure.rs`, with `toml` as a dev-dependency. Record the red run by adding `engenho-machines` to engenho-runtime's dependencies in a scratch branch.

## 5. Doctrine decisions

**5.1 teia/NATS vs the one-binary rule: NATS is not engenho's fabric. teia stays, fenced off.**
A NATS server is a second process every node would need, which is a sidecar under another name. Today teia is compiled into every binary and executed by none, yet it still costs something live:
- validation at every boot (engenho-config lib.rs:228);
- a `teia` section rendered into every node's config (typed-config.nix:481-486);
- a NATS StatefulSet in the chart.

One commit per step:
- **(a)** Make `engenho-teia` optional behind a `teia-nats` feature that is off by default. CI builds with the feature on. Check: `cargo tree -p engenho -i async-nats` comes back empty.
- **(b)** Replace `teia: TeiaConfig` with `fabric: Fabric{InBinary}`. `TeiaConfig` stays declared, with a `pending-fabric:` marker. A legacy `teia:` key is accepted for one release with a typed warning. The Nix option `teia` (typed-config.nix:378) stays declared and is marked deprecated with a warning; it is never removed, because deleting it would break evaluation for any consumer that sets it. The Rust key, the Nix render and the option's deprecation land in one commit, because `EngenhoConfig` is `deny_unknown_fields`.
- **(c)** The chart stops deploying NATS.
- **(d)** The multi-node transport will live inside the binary. It is named here, not built, and gated by edge 18.

teia is not deleted: MODULARIZE-DON'T-DELETE keeps code that was retired because "we stopped using it".

**5.2 engenho-machines: delete it.** It is a genuine orphan, and its content is wrong:
- it has had zero reverse dependencies since it was created;
- it is in no binary;
- its 14 tests never ran in CI;
- it contradicts the code it claims to model.

Keep `substrate::maquina`. From now on, a state machine exists only as the implementation itself, or with a differential test over every (state, event) pair.

**5.3 revoada (13,358 lines, in no binary): keep it as a typed draft, fenced off, not hardened.**
- Its safety depends on things that do not exist yet: a durable vote store, a real `has_majority`, and a quorum check on promotion.
- Now: T5.4's corrections, delete RoundRobin, and T5.1 asserts revoada stays outside the shipped closure.
- CI reports test counts for shipped and unshipped crates separately.
- No reliability work until a decision to ship multi-node, which edge 18 gates.

**5.4 The same rule applied elsewhere:**
- Scheduler predicates: bind (T5.7).
- Substrate's unreferenced modules: fence (T5.6).
- fonte: fence (T5.5).
- CRI: refuse (T5.9).
- The 8 dormant controllers: declare them (T5.11). HPA gets fixed when it is wired in.
- `ControllerRuntime`: leave it, with `pending-requeue-unify`.
- `audit.rs`: bind it as the sink for T4.2's would-deny log if its event shape fits.
- The etcd façade stays on, read-only and loopback-only until it has mutual TLS (T4.9). T3.2b and T3.8 fix it.
- **ryn is one trust domain.** Native workloads run as the operator's UID and share the host's filesystem and loopback. They can already read the store's files and anything else the operator can.
  - A 0600 socket or a mutual-TLS key on disk is readable by that same UID, so neither isolates anything.
  - The real boundary is a separate identity per workload, from BUTAI's supervisor (theory/BUTAI.md §1, gated by edge 12).
  - Until then, ryn hosts only workloads the operator would run by hand.
  - T4.2 is the highest-severity *remote* escalation path, not the only escalation path.

## 6. Sequencing

**Rules:**
- One hot-path change per release.
- A hot-path change never ships in the same release as a lint or doc change.
- Verify each change on the node whose backend exercises it (native → ryn; podman_api → rio and plo) before shipping the next.
- A change that could strand a workload or reject a stored object ships only after its census (T0.10). While the census count is above zero, it ships in Shadow (T0.11).
- A change to apply semantics carries a marker on each log entry. Rollback across it starts from a clean stop.

| Wave | Order | Exit condition |
|---|---|---|
| 0: truth, no runtime change | T0.1 · T0.2 · T0.3(a) · T0.4's measurement and `[lints]` opt-in · T0.10 · T0.11 (type and metric only) · T5.1 · T3.1 (red runs recorded) · T4.7's measurement · censuses: live roles (T4.2), ledger headroom (T1.5), bound pods vs filters (T5.7), stored-object would-rejects (T4.4, T4.5), template matching (T4.10) · T5.4's doc corrections | CI red on exactly the 4 StoreStillShared tests; every census result recorded |
| 1: live defects, size S each | T2.1 (+ `quiesce`) → (CI fully green) → T2.9 · T4.1 → T4.2 · T1.3(a) · T2.2 · T1.1 · T1.2 c1 · T2.5 (upsert) · T3.6 · T3.2a → T3.3 · T3.9a · T1.4 · T1.5 · T2.3 (kubelet) · T2.4 c1 · T4.9 | a week with no new red and no drift in restart counts on ryn or rio |
| 2: structural seals | T2.6 → T2.8 → T2.7 · T1.3 (b) → (c) → (d) · T3.4 (Shadow, then Fatal one release later) · T3.5 · T3.7 · T3.2b → T3.8 · T1.2 c2 · T2.4 c2 · T1.6 · T1.7 · T1.8 · T4.3 · T4.10 → T4.4 · T2.10 · T2.5 (bind) · T5.7 (Shadow, then Enforce per filter) · T5.8 · T5.2 · T5.3 · T5.5 · T5.6 · T5.9 · T5.11 · T0.4 flip to blocking · T0.5 · T0.6 · T0.8 | clippy blocking; mutation gate live; a week of liveness data; every Shadow gate at zero would-rejects before its Enforce release |
| 3: destination | T4.5 (per verb, PUT → PATCH → SSA; per GVK, census → Shadow → Enforce) · T4.6 · T4.7 (unless promoted) · T3.9b · T3.3 seal · a fault-injection matrix over `Child` · a backend conformance matrix · RuntimeHealth relist · pvc-protection, then Released/reclaim · T0.3(b, c) · T0.9 · QuorumTracker as the only fold | — |
| Gated, not scheduled | multi-node · revoada · fonte fixes · CSI protocol work · a keyed WorkQueue · respawn/escalate · sd_notify watchdog · Deployment rolling update · native `process_group(0)` · off-loopback listeners and `hostNetwork` · wiring in any dormant controller | edges 11, 12, 18, 26 |

**Dependency edges:**
1. **T0.1 before any "caught in CI" claim.**
2. **T2.1 → CI green → T2.9.** This is a measured gate, not wave order.
3. **T2.1 before T2.2, before T2.3's retries, before T2.7, and before T1.3c.** Every re-tick goes through the one slot.
4. **T2.2's exhaustive classify before any shigoto upgrade that adds a `FailureKind`.**
5. **T3.1 before T3.3, T3.4 and T2.9's flush.** It is their proof.
6. **T3.2a with or before T3.3.** The honest 410 turns every restart into a relist.
7. **T2.5's upsert before T5.7; T3.5 before T5.7's affinity follow-up.**
8. **T4.1 and T4.2 in adjacent releases, T4.1 first. The live-role census comes before T4.2 enforces.**
9. **Normalization only ever touches the object that will be stored.** Never a patch body, where `labels: {x: null}` means "delete x".
10. **T4.3 before any controller re-tick or rebuild (T2.7).** A re-ticked controller re-reads the object that poisoned it.
11. **T2.8 (liveness is visible) before any actuator.** This covers T2.7's re-tick and listener rebind, parking, respawn, and the watchdog.
12. **A process-level restart and native `process_group(0)` wait for native re-adoption or BUTAI's detached supervisor.** When launchd stops the job on ryn, it kills the job's process group; that is today's only protection against a restart leaving a duplicate. Nodes under systemd run podman, where re-adoption by name already exists.
13. **T2.6's heartbeat before T1.3c, and T1.3c before T1.3d.**
14. **T1.2 c1 before T2.10's native stop, and before the conformance row for signal exits.**
15. **For each types row: the ObjectMeta fields, number-accepting Quantity, and that row's fixture and accept-set entries come before enforcement (T4.6).**
16. **T0.2 before T0.3(b).**
17. **T5.2's Rust key, the typed-config.nix render and the deprecated Nix option change in one commit.**
18. **No second voter until all of these hold:** the FaultRouter partition test has a recorded red; every Raft RPC reply is a `Result`; read fences exist; `/readyz` is typed per role; votes are durable; and the in-binary transport exists.
19. **T0.6 only once test.yml is green.**
20. **Replay.** Any change to apply semantics ships after T2.9, carries a marker on each log entry, and is gated by T3.1 case 5. This covers T3.5, and T4.5 if its merge changes any bytes. Rollback across it starts only from a clean stop. A new `ResourceCommand` variant takes two releases: decoding first, emitting second.
21. **T3.9a before T3.5.**
22. **T1.3(a) before T5.7.**
23. **Each census (T0.10) comes before its change:** T1.5, T4.2, T4.4, each GVK of T4.5, T4.10, T5.7. A non-zero count means the change stays in Shadow until the count reaches zero.
24. **T3.4 becomes Fatal only after one release in Shadow with zero hits on rio, ryn and plo.**
25. **T4.10 before T4.4, and before any `apps/v1` template-defaulting arm.**
26. **Authentication on :10250 and mutual TLS on :2379 before either binds off loopback, or before `hostNetwork` is implemented (T4.9).**

## 7. What becomes impossible, and what is only caught

| Change | Becomes impossible | Only caught or mitigated |
|---|---|---|
| T2.1 slot | A second pending re-tick per driver. A detached re-tick (the spawn is deleted). | The store outliving shutdown through some other task. BookmarkTicker is awaited in `quiesce`, but a new task that upgrades a `Weak` is not prevented. |
| T2.6 `Infallible` + `Child` | A driver loop that returns (E0308). A `Child` variant missing its liveness row, filter or `TickState` (E0004). | A task spawned outside the catalog (every spawn path disallowed; CI catches it once clippy blocks). A panic outside the tick (the JoinSet monitor reports it). |
| T2.7 `catch_unwind` | — | A tick panic retiring a stateless controller (caught, counted, re-ticked by the fallback). A stateful child re-ticked over torn state (it is not wrapped, so it goes Dead, visibly). |
| T2.2 classify + Curve | A new error variant with no retry class (E0004). A curve with cap ≤ base (compile-time panic). | Classifying by message text (removed; only review stops it coming back). |
| T2.4 c2 start curve | — | A start retried faster than its container's curve (test). |
| T2.9 stop path | An unhandled SIGTERM or SIGINT (exhaustive over the two signals it subscribes to). A clean stop that leaves log entries to replay. | SIGHUP and SIGQUIT, which are not subscribed. |
| T1.1 Blind + `ProbeTrip` | A probe-driven restart without an observed Failure past the threshold (the constructor is private to its submodule). | An exec error mapped to the wrong arm (conformance test + mutation). |
| T1.2 `ExitDisposition` | An unobserved exit counted as success. A started container with no local record read as never started. | The wire rendering of 137 / `ContainerStatusUnknown` (upstream convention). |
| T1.3 one derivation | A boot that uncordons a node or drops its taints (`register_node` never overwrites). | The scheduler placing onto an unobserved node (one call site plus a test). |
| T1.4 `PvName::for_claim` | Two new claims sharing a PV name or directory. | PVs that already exist. |
| T1.5 `NodeLedger` | Capacity arithmetic outside the ledger. | Handling of unknown phases (a three-arm test). |
| T1.6 `SpecInt` | The collapsing helpers (deleted). | A new inline `.as_i64().unwrap_or` (review). |
| T1.8 / T2.3 sweep | Counts that disagree with outcomes, in migrated controllers. | One object's `?` aborting the sweep in controllers not yet migrated. |
| T3.2 sealed catalog | A `pub fn` returning `ResourceCatalog` (`private_interfaces`, denied). | An owned collection derived from it (a test over public signatures). |
| T3.3 `WatchHistory` | A floor the ring does not back. | — |
| T3.4 snapshot batch | — | A durable image older than the snapshot (one structural site; a boot tripwire in Shadow, then Fatal). A double apply (runtime guard). |
| T3.5 no-op gate | A revision bump for identical content in a V1 entry, through put or patch/SSA. "Unchanged" being read as "missing". | V0 entries keep V0 rules, by design. Rollback with log entries still to replay (a procedure, edge 20). managedFields time normalization (test). |
| T3.6 delete clock | — | A delete with no clock (the constructor always stamps one; a struct literal with `None` still compiles). |
| T3.7 `Compacted`, `NoProgress` | A 410 built from `last_seen` (E0308). An overflow with no progress ending without a backoff signal. | How each client recovers (client-go and kube-rs rows). |
| T4.1 one parse | Authz and dispatch disagreeing, on every route that carries `RequestInfo` (router test). | The kubelet API and the etcd façade, which bypass the router (T4.9). |
| T4.2 matcher | — | Parity with upstream (the ported table). |
| T4.3 total accessors | The panic path inside those helpers. | Skipping the malformed object (mitigation). |
| T4.5 `WritePlan` | A user write through the apiserver reaching `propose` without defaulting and validation. | Every other holder of `Arc<StoreMesh>`, such as controllers and `register_node`: `propose` itself is not sealed. |
| T4.9 loopback rule | Binding either listener off loopback without authentication (rejected when config is parsed). | Native workloads on ryn that share the operator's UID (no mechanism until BUTAI). |
| T4.10 template matching | A normalization, defaulting or hash change rolling out every Deployment. | — |
| T5.2 feature gate | NATS code in the default binary. | The conditions for enabling it (a pending row). |
| T5.7 `Feasible` | A bind that skipped a filter (only `filter()` builds `Feasible`). | Filters still in Shadow because their census count is not zero. |
| T5.8 destructure | An unread config field with no written decision (E0027). | A field discarded as `field: _` (review). |
| T5.9 CRI refusal | Booting with a backend that drops mounts and IPs (rejected when config is parsed). | — |
| T5.11 dormant catalog | — | A `Controller` type in neither `Child` nor `Dormant` (test). |
| T2.8 derived health | A constant health answer (the handlers are deleted). | A stalled or dead child (detector). |
| T0.3 tag after gate | — | A tag on a red tree (the workflow's `needs:`; a manual push bypasses it). |
| T0.4 / T0.5 / T5.1 | — | New discarded must-use results, `Path::exists`, `format!`, unwrap in core crates, crates leaking into the closure, spawns outside the catalog (CI, once blocking). |
| T0.6 / T0.7 | — | Tests that pin nothing, or pin a wrong belief (CI). |
| T0.10 / T0.11 | — | A rollout that strands workloads or rejects stored objects (census first, Shadow while the count is non-zero). |

## 8. The single highest-leverage change

**T2.1: delete `arm_requeue` and give each WatchDriver one requeue slot.**

1. **It is the only defect that grows with uptime on every node in steady state.**
   - Any pod with a probe makes the kubelet return `Requeue` (kubelet.rs:2078).
   - Every such tick leaves behind a chain of re-ticks that re-arms itself forever: about 120 new chains an hour from the 30 s fallback alone, plus one per Pod event.
   - These chains run `Kubelet::tick` concurrently. `local` is unlocked before `start_bound_pod` (kubelet.rs:2044), so two chains can start the same pod twice. Every chain also does a full `store.list(Pod)`.
2. **Production shows the signature.** On plo on 09-06, a Transient error retried about 3.5 times per second against an advertised 1 s backoff. That is consistent with this cause, but not proven.
3. **It is predicted to be the last code defect keeping CI red.**
   - The only failures other than missing `nix` are the four StoreStillShared tests.
   - Shutdown already aborts and awaits every driver handle (runtime.rs:518-527), and both listeners hold `Weak` references. Two holders of the store remain:
     - a sleeping detached re-tick, which this commit deletes;
     - BookmarkTicker's upgraded `Arc` during a tick, which `quiesce` awaits in the same commit.
   - With T0.1 in place, this is predicted to give the first green test.yml since 4f32e6f (2026-08-28, 795 commits before HEAD). Edge 2 gates SIGTERM handling on that measured result, not on this prediction.
4. **Four other items depend on it:** T2.9, T2.2, T2.7 and T1.3c.
5. **It is cheap and falsifiable:** size S, a paused-time test with a recorded red run, low risk.

It is not the highest-*severity* item. That is T4.2 together with T4.1:
- `create serviceaccounts` mints a token for any ServiceAccount the rule covers;
- because authz parses the raw path and dispatch the decoded one, a `create serviceaccounts` grant can still reach `token` after the matcher is fixed.

Both ship in wave 1, in adjacent releases. On ryn, native workloads already hold the operator's authority (§5.4), and no listener change alters that.

## 9. What I would not do

- **Make pedantic lints a gate, or keep `-D warnings` over roughly 3,800 style warnings.** That is exactly what produced `continue-on-error`.
- **Build the full supervisor now.** `Infallible`, plus `catch_unwind` for stateless children, plus a visible Dead state, covers every failure mode observed so far.
- **Arm a watchdog before ryn's native workloads are isolated.**
- **Give native workloads their own process group before they can be re-adopted.** launchd killing the job's process group is what stops a restart from leaving two copies of each workload.
- **Split kubelet.rs or runtime.rs with move-only PRs.**
- **Re-model the kubelet lifecycle as a maquina machine.**
- **Build a keyed WorkQueue now.**
- **Take revoada or any second voter live, or adopt NATS as the fabric.**
- **Harden code that no binary runs:** revoada, fonte, the CSI protocol, CRI, and the 8 dormant controllers.
- **Normalize nulls in request bodies.** It breaks merge-patch deletion (edge 9).
- **Widen gc to every served kind.** A GET at the storage version would read Absent and delete live dependents.
- **Build a self-differential trajectory oracle, or an effect ledger across all 28 controller types.**
- **Persist a `writer` field on `ResourceCommand` now, or add a `ResourceCommand` variant in a single release.** Decoding must ship a release before emitting, or a rollback cannot read the log.
- **Change `[profile.release]` expecting a faster daemon.**
- **Expand `/metrics` without a scraper, or add a `not-ready:NoSchedule` taint before T5.7.**
- **Model states that no producer can emit.**
- **Let a crate-wide lint flip ride along with a local fix, or use `disallowed-methods` as enforcement while clippy cannot fail.**
- **Edit the Helm chart beyond removing NATS.**
- **Build RollingUpdate for Deployments before T2.1, T2.3, T3.5 and T4.10.**
- **Rank work by severity in principle rather than by reachability.**
- **Enforce a filter, validation or ledger change without its census, or while the census count is non-zero.**
- **Make `open()` Fatal the first time it meets production data.** A node that booted yesterday must still boot.
- **Re-tick a stateful child after a panic.**
- **Change apply semantics without a marker on each log entry, or roll back across such a change without a clean stop.**
- **Treat a 0600 socket or an on-disk mutual-TLS key as isolation on ryn.** The workloads run as the same UID.
- **Rate-limit watch clients per client, the way upstream's API Priority and Fairness does, for now.** Only an overflow with no progress loops, and T3.7 ends it with a typed 429.

## 10. Risk on a running cluster

The rules from §6 apply, and each live check must pass before the next hot-path change ships.

Rollback means booting the previous NixOS/darwin generation. Every byte persisted to disk stays readable by the previous binary. What a replayed log entry *means* changes only for T3.5, and for T4.5 if its merge changes any bytes. Both carry a marker on each log entry, and rollback across either starts from a clean stop only (edge 20).

| Change | Runs on | If it is wrong | Proof before merge | Live check before the next change | Safe to replay |
|---|---|---|---|---|---|
| T2.1 slot | every controller loop, every node | probes run late; the kubelet idles | paused-time red→green; mutation pass on `next_wake` | probes per container per minute = 60/periodSeconds, flat over 1 h; tokio task count flat against uptime; the 4 CI tests green | yes: no store change |
| T2.9 stop path | every stop | the stop exceeds launchd's ExitTimeOut, or exits 1; the flush fails | gated on CI green; SIGTERM test; T3.1 case 4 | `systemctl stop` and `launchctl kickstart -k` both log "stopped cleanly" and exit 0; the next boot replays 0 entries | yes: writes the same keys `apply` writes |
| T1.3(a) register_node | every boot | a new node never reaches Ready; stale labels survive | create and merge tests; a cordon and a taint survive a restart (test) | cordon a node, add a taint, restart: both survive on rio, ryn and plo; pods still place | yes: different command, same apply rules |
| T4.1 one parse | every request | 403 or 404 on paths with encoded characters | both parsers run over every path in the r*/m* test corpus; route-coverage test | apiserver error rate unchanged for 24 h | yes |
| T4.2 RBAC | every non-admin request | a workload loses a subresource it relied on | the live-role census plus the ported table | no new 403s for 24 h; Flux and pangea-operator reconcile | yes |
| T2.2 curve | failing controllers | a transient failure recovers more slowly (cap 60 s) | the gap between retries grows ≥1.8× until the cap | recovery after `systemctl restart podman` stays within the cap | yes |
| T1.1 Blind | every probe | a workload whose exec endpoint is permanently broken is never liveness-restarted (upstream has the same hole) | fold tests; 500 → Failure | pangea-operator restartCount flat; a Blind probe produces an Event and a condition, never a restart | yes |
| T1.2 exit (c1) and re-adoption (c2) | container status on ryn; every ryn restart | a successful `Never` pod reported Failed; a Job pod re-run as a new pod rather than in place | SIGKILL → Failed/137; exit 0 → Succeeded; no record + started → Unknown | restart engenho on ryn with a Job pod running, and record the Job's pods before and after: the Job completes within backoffLimit; Jobs that exit 0 complete as before | yes |
| T2.5 upsert | pods that cannot be scheduled | a stale condition | driver-level tick-count test | with one unschedulable pod, ≤3 scheduler ticks per 2 s (was 38) | yes |
| T3.6 delete clock | every GC or controller delete | objects stuck Terminating (upstream semantics) | state and gc red tests | deletes of objects without finalizers unchanged; Raft entries per minute drop | yes: the field exists, and `None` replays as today |
| T3.2a paged LIST | every relist | a page drops or duplicates items | proptest: pages concatenated = the unpaged list | Flux relists finish; p99 write latency flat during a relist | yes |
| T3.3 floor | every restart | every informer relists after each restart | T3.1 | after a restart: 410s, then convergence within one relist | yes: the old field is kept and ignored on load |
| T3.9a watch-ahead 410 | watches ahead of the store | a client relists where it previously received silently stale data | client-go and kube-rs rows | pangea-operator recovers after a restart with no manual action | yes |
| T1.4 PV UID | every dynamic PVC | new directories get new names | recreate red test | a new PVC binds; existing PVs untouched | yes |
| T1.5 ledger | every placement | a rollout or restart goes Pending | three-arm tests; per-node headroom census ≥ 0 | no overcommit; Succeeded pods free their capacity; pangea-operator restarts place | yes |
| T2.3 / T2.4 kubelet | every pod on every node | pod work reordered; start storms | FakeBackend fail-one-pod; sidecar, partial-start and start-backoff (≤16 per hour) tests | restart engenho mid-run: exactly one process per container, and no `rm -f` of a running container | yes |
| T2.6 / T2.8 children and health | every spawn; `/healthz` (rio's bootstrap reads it) | a child is not spawned; bootstrap waits forever | every `Child` ticks K=3 times under a deadline; Unknown→Alive boot test | rio bootstrap completes; a killed child turns `/livez` red within the window | yes |
| T2.7 panic containment | ticks of stateless children | a poisoned Mutex re-panics once per fallback interval | a stateless panic is counted and re-ticked; a stateful panic goes Dead | `engenho_panics_total` flat; a Dead child shows red on `/livez` | yes |
| T1.3 (b-d) readiness | every placement | pods stay Pending if the projection is wrong | fresh lease → schedulable; absent lease → not; long-pull test; schedulable within one tick of boot | after boot, pods place within one renew interval; no NotReady flaps during pulls | yes |
| T3.4 snapshot | durable apply, snapshot, purge, boot | a false tripwire (in Shadow, that is only a log line) | T3.1 case 2; double-apply test; `open()` on copies of all three data directories | release N: zero tripwire hits on rio, ryn and plo; restart ryn twice and the revision is continuous | yes: no new keys |
| T3.5 no-op gate | every write | a nudge-by-rewrite is lost; revisions renumbered on replay | generator cases; census of nudging writers; T3.1 case 5 | lease renewals still bump; an identical `kubectl apply` keeps its resourceVersion | **only through the marker:** V0 entries replay unchanged; rollback from a clean stop only |
| T4.10 template matching | every Deployment reconcile | an intended rollout is missed | matcher tests over the census | switching matchers creates zero ReplicaSets; a real template edit still rolls out | yes |
| T4.4 normalization | every POST and PUT | 422 on writes that clients rely on | census: zero would-422s, zero template changes | a Flux re-apply is clean; label removal still works | yes: the border only |
| T5.7 filters | every placement | workloads stranded (ryn's `darwin` label vs `os: linux` selectors) | per-filter census: zero would-rejects per node | one Shadow release with zero would-rejects, then Enforce per filter | yes |
| T2.10 native stop | every native stop on ryn | a workload SIGKILLed before it finishes shutting down | escalation and reap tests; no `process_group` | a pod that ignores SIGTERM is reaped within its grace period + 1 s; after a restart, one process per workload | yes |
| T4.5 pipeline | every PUT, PATCH and SSA | valid writes rejected; stored objects fail on their next reconcile | per-GVK census; a recorded-request corpus including `kubectl label … x-` and an SSA field removal; replay differential | per GVK: a Shadow week at zero would-rejects, then Enforce; a full Flux re-apply is clean | only if the merge moves byte for byte (T3.1 case 5); a changed merge needs a new marker version |
| T5.2 config migration | boot; Nix evaluation | nodes fail to parse config; Nix evaluation fails | the existing typed-config flake check; the legacy key and the deprecated option are both accepted | all three nodes rebuild and boot | yes |

## 11. Rev-1 critique ledger (condensed)

| Finding | Resolution |
|---|---|
| T1.3 re-proposed the projection that already shipped in f866f6e | One derivation, which the scheduler consumes (T1.3). |
| T4.4 normalization would break deletion by merge-patch | Only the object that will be stored is normalized (edge 9). |
| SIGTERM handling depended on T2.1 | Gated on CI showing the four tests green (edge 2). |
| A retry landed in a wildcard arm | Classify is exhaustive; `retry_after` is deleted (T2.2, edge 4). |
| `set_owner_reference` has 9 callers in 7 files | All nine are listed (T4.3). |
| No kill -9 durability test | T3.1, now with clean-stop and replay cases. |
| Four defect classes had no structural fix | T1.7 + T2.6, T5.10, T0.7, T1.8. |
| "Exactly four reds" was a guess | Measured in run 35418242848. |
| A `writer` field on a tagged enum needs backward replay | Deferred; a task-local controller name at `propose` instead (T2.8, §9). |
| `/readyz` = leader would make every follower unready | A linearizable read instead (T2.8, edge 18). |
| The NATS listener was missing from the task roster | Feature-gated (T5.2); a `Child` under the feature (T2.6). |
| Plugin contracts had no items | T1.4, T2.10, T5.9. |
| revoada had no reliability item | §5.3 and edge 18. |

## 12. Corrections applied in revision 2

**From the code-truth critique:**
1. "`nix flake check` has no checks" was false. It runs `checks.typed-config` (flake.nix:164-168) and compiles no Rust. Corrected in §2, T0.1 (with CLAUDE.md) and §10.
2. HPA is not spawned. It moved from T1.7 to T5.11 (class D). Checking this turned up 8 dormant controller types, now in §2 and class D.
3. T3.4: `install_snapshot` already writes the catalog, `last_applied` and membership. The remaining work is making both snapshot paths atomic, and making `build_snapshot` write the blob and `last_applied`.
4. The persist check is at fjall_store.rs:829-834; :120-125 are only the constants.
5. T0.2: one contract. `.config/nextest.toml`, which the release gate's nextest discovers on its own, replaces `--exclude` plus the txt file. The reason for not using `#[ignore]` is kept.
6. There are 37 increment sites, 27 of them in spawned code. This replaces "27 sites discard the op" (T1.8, §2).
7. T5.6: substrate modules referenced by the dormant controllers stay in the leaf crate.

**From the cluster-safety critique:**

8. T3.5: a marker on each log entry, replay case 5 in T3.1, rollback from a clean stop, and T3.9a moved ahead of it (edges 20-21).
9. T2.9 now flushes the catalog on stop.
10. T4.5: a per-GVK census of stored objects, Shadow then Enforce, and byte-identical merge replay. T4.10 was added to prevent a rollout of every Deployment when the template hash changes.
11. T3.4: snapshot build and purge are live. The tripwire runs in Shadow for one release, after read-only opens against copies of each node's data (edge 24).
12. `register_node` wipes cordons, labels and taints on every boot. Fixed by T1.3(a) in wave 1, a precondition of T5.7 (edge 22).
13. T5.7: a census and Shadow per filter; ryn's `darwin` label is named as a risk.
14. T1.5: a headroom census before shipping.
15. T2.10: no `process_group(0)`. The native stop becomes SIGTERM → SIGKILL → reap on the pid. Edge 12 now names launchd's process group, not a cgroup.
16. T1.2: `should_restart(Unknown)` is specified; a Job-restart check on ryn and an operator notice were added.
17. The Nix `teia` option is kept and marked deprecated (§5.1).
18. T2.7 now ships after T2.8. Only stateless children re-tick after a panic.
19. T2.1: BookmarkTicker named as a second holder of the store; `quiesce` awaits it.
20. T1.3(b): the CAS publish retries once after a re-read.
21. T2.4 c2: start attempts go through the per-container curve.
22. T2.6: every spawn site has a disposition.
23. §7's tier claims corrected for T2.1, T2.6, T2.9, T3.2, T3.5, T4.1, T4.5, T5.8, T1.1 and T1.4. §10 gained a replay column.
24. Two shared tools (T0.10, T0.11) were extracted, because seven and six rows respectively needed them.

**Found while re-reading:**

25. The apiserver does not serve `pods/exec`. The live escalation is `serviceaccounts/token`, and the T4.1 test now uses it.
26. The comment at runtime.rs:1199-1206 claims nodeSelector keeps pods Pending, but nothing shipped evaluates nodeSelector.
27. The `teia` and `revoada.topology` config sections are validated and never read.
28. The native stop never escalates or reaps, and `remove` drops the record of processes that have not been reaped.
29. A watch ahead of the store is served from the current revision, which r7_8_listwatch.rs:402 pins. Fixed by T3.9a.
30. T0.3(b)'s parenthetical was backwards: a plain `needs:` skips `bump` for repos that waive tests.
31. "38 controllers" was wrong: 28 controller types, 20 of them spawned.

**Critic points not adopted as written:**
- *A 0600 socket or mutual TLS before T4.2 counts as done:* on ryn, the workloads run as the operator's UID, so neither isolates anything. Adopted instead as T4.9 plus the trust-domain statement in §5.4.
- *Count re-watches and return 429 past a limit:* an overflow after which the client has made progress moves it forward, as upstream does. Only the no-progress case loops, and it now ends with a typed 429 (T3.7).
- *Park stateless controllers as Dead too:* adopted until T2.8. After T2.8, stateless children re-tick, because they re-read the store every tick.
- *Status writes bypass T3.5's paths:* they don't. Status writes go through `patch_cas` into `apply_patch`. Only `apply_txn` is uncovered, and nothing issues it.
- *Exits are fabricated as 0 across a restart:* not the actual mechanism. A restart re-runs the pod in place (kubelet.rs:2046-2056). The critic's conclusions were adopted anyway.