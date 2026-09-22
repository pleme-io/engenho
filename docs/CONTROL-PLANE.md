# engenho control plane — managing a running engenho without the Kubernetes API

The `engenho` binary talks to a running engenho, locally and from another
machine, and manages its **running state**, **configuration** and
**initialization state** through a dedicated control plane. It never goes
through the Kubernetes API: there is no ConfigMap tier, no CRD, and the
control plane stays reachable when the Kubernetes apiserver is down, has not
booted yet, or failed to boot.

## Operator decisions (2026-09-22)

| Question | Decision |
|---|---|
| Reach | Local Unix socket **and** a remote mTLS listener on the tailnet |
| A config change made through the control plane | **Persisted by default** (a data-dir overlay tier), reported as drift from the Nix-declared file |
| A boot that fails | The process **stays up** and serves the control plane in a typed `Failed` state, instead of exiting into a service-manager crash loop |
| Destructive re-initialization | **Included, gated**: a single-use confirmation bound to the cluster identity, on a stopped runtime |

## Shape

```
engenho daemon ─┬─ Supervisor (typed lifecycle; outlives every boot)
                │    └─ Runtime (restartable child; store handed back as StoreReleased)
                ├─ control listener: UDS    (kernel peer credentials)
                └─ control listener: TLS    (SPKI-pinned clients, both ways)
engenho ctl <resource> <verb>   — the client, same binary
engenho-mcp                     — control tools (observe; mutate behind --allow-mutate)
```

* **One spec is the source of truth.** `spec/engenho-control.openapi.yaml`
  (OpenAPI 3.0.3, every path under `/v1`) defines every operation, and each
  operation carries `x-engenho-authority` (`observe | mutate | destructive`),
  `x-engenho-cli` (`{resource, verb}`), and `x-engenho-confirm` on the
  destructive operations the confirmation handshake gates.
* **`engenho-control-types`** derives everything from it at build time: the
  schema types, the `OperationId` closed enum and `CATALOG`, a typed
  `…Request` per operation with `to_http`/`from_http`, the `Operation` trait
  that binds marker → request → response, and the transport-agnostic
  `EngenhoControl` trait (one method per operation). The router, the CLI and
  the MCP tools are all driven from `CATALOG`, so they cannot disagree with
  the spec about what exists or what authority it needs.
* **`engenho-serve`** is the one HTTP serve loop (Kubernetes apiserver and
  both control listeners). It owns every connection task, so a stop drains
  in-flight requests for a grace, then severs and awaits what is left —
  which is what made a clean in-process restart possible (P0a).

## The lifecycle (P1)

`engenho daemon` is a supervisor (`engenho-runtime/src/lifecycle/`) above a
restartable runtime. Its state is one pure machine, `DaemonLifecycle`, on
`maquina::StateMachine`:

| State | The store | Leaves on |
|---|---|---|
| `resolving` | not opened | start → `booting`; hold marker → `stopped` |
| `booting{attempt, phase}` | may be open | booted → `running`; failed → `failed` (or `wedged`); stop/restart/exit → `draining` |
| `running{attempt, pending}` | open | stop/restart/exit → `draining` |
| `draining{then}` | open | released → `stopped` / next boot / `exiting`; still held → `wedged` |
| `stopped{reason, epoch}` | released | start → `booting` |
| `failed{report, retry}` | released | retry, a declared-file change, or a due backoff → `booting` |
| `wedged{cause}` | held, unreleasable | exit only |
| `exiting{intent}` | — | terminal: `halt` exits 0, `relaunch` exits 75 |

* **A boot starts only over a released store.** The supervisor's runtime slot
  is typed: a boot starts from `Idle`, entered only when this process holds no
  store. The machine's side is checked over arbitrary event sequences
  (`lifecycle::machine::tests::the_store_is_never_held_at_rest`).
* **A failed boot is classified, not retried blindly.** `FailureClass::of` is
  one exhaustive match over `RuntimeError` (and the store's and apiserver's
  errors): a held port, a busy store or an absent container runtime back off
  (1 s doubling to 60 s); a config the runtime refuses is held until the
  declared file changes or an operator retries.
* **A boot is 16 named phases** (`BootPhase`, `resolve_config` →
  `adopt_health`), each entered through a `BootRecorder` that reports it to the
  supervisor. A stop is honoured at phase boundaries and inside the leadership
  wait, up to the apiserver bind; after the bind the boot finishes and the stop
  is a shutdown (the children it spawned are not aborted by a drop).
* **What survives the process**, in `data_dir/control/` (0700): the boot
  journal (last 16 attempts, per-phase timing), `run.json` (whether dying now
  would leave the store released — read back as `previous_run`), the
  first-boot identity, the `hold` marker, and `daemon.lock` (one daemon per
  data directory). The data directory itself is placed before any boot, even
  when the config does not resolve (`ControlBootstrap`), and is fixed for the
  life of the process.

## The local socket (P2, P3)

`engenho daemon` binds its control socket before the first boot and keeps it
across every boot, restart and failure — it is the recovery path, so there is
no switch to turn it off.

* **Where.** `control.socket.path`, else the daemon user's default: root →
  `/run/engenho/control.sock` (Linux) or `/var/run/engenho/control.sock`
  (macOS); anyone else → `$XDG_STATE_HOME/engenho/control.sock`, else
  `~/.local/state/engenho/control.sock`. `engenho ctl` resolves it the same
  way (`--socket` or `$ENGENHO_CONTROL_SOCKET` name it outright), reading the
  `control` section leniently, so a broken config still names its socket.
* **Binding it safely** (`engenho-control-server::socket`). One daemon per
  socket: a `flock` on `<socket>.lock` (the store's `DataDirLock`, reused); a
  socket whose lock is free was a dead daemon's and is replaced; a symlink, a
  file or a directory at the path is refused, never removed; the directory
  must be the daemon's and nobody else's to write (`0700` for owner access);
  a path longer than `sun_path` is refused by name.
* **Who may do what** (`grant`). The kernel's peer credentials, never the
  request: the daemon's uid or root → destructive; a member of the socket
  directory's group on a `group` socket → `control.socket.group_tier`
  (observe or mutate, never destructive); anyone else → nothing. A caller
  may lower its own tier (`Engenho-Ceiling`); `Engenho-Actor` is recorded,
  never authority.
* **Every mutation is audited** (`audit`): intent before it runs, result
  after, and every refusal — the spec's `AuditRecord`, one JSON line each in
  `data_dir/control/audit/audit.jsonl`, fsync'd, each carrying the BLAKE3 of
  the line before it. Parameters are hashed, never written.
* **Routing is the catalog's** (`router`): `OperationId::route` names the
  operation from method and path, its catalog row says what tier it needs,
  and the generated `visit` dispatches it — one exhaustive match, generated
  from the spec.
* **The daemon's answers** (`engenho-runtime::control`): lifecycle and journal
  from the supervisor's snapshot (never waiting on its loop); children, store
  and kubeconfigs from `SupervisorHandle::inspect`; the PKI from disk,
  read-only (`engenho_apiserver::pki_inventory` never mints a CA to describe
  one); events and logs from sequenced rings the caller long-polls.
* **`engenho ctl <resource> <verb>`** is table-driven over the catalog:
  positional arguments fill path parameters, `--param value` fills query and
  header parameters, other `--field value` pairs build the body, and the
  daemon's own generated parser checks the request before it is sent. Exit
  codes: 0 answered, 2 usage, 3 refused, 4 blind or unreachable.
* **A halt stays halted.** Every service unit the module trio renders for
  engenho restarts it on failure only (`daemonRestartPolicy = "on-failure"`
  in `flake.nix`, projected by substrate's `lib/hm/restart-policy.nix`):
  launchd `KeepAlive = { SuccessfulExit = false; Crashed = true; }`, systemd
  `Restart=on-failure`. So `runtime exit` with a halt intent (exit 0) stays
  down, and a relaunch intent (exit 75) comes back.

## Remote trust: SPKI pins, not the cluster CA

The control listener does **not** trust engenho's cluster CA. It has its own
random ed25519 identity under `data_dir/control/identity/`, created with no
dependency on anything boot creates, and it authorizes clients by
**SPKI-SHA256 pin** (the `authorized_keys` model), each pin carrying a tier
declared on the server. Clients pin the server the same way.

Why not the cluster CA, measured in this repository:

* Cluster-CA leaves are pinned to a fixed 2020–2100 window —
  `engenho-apiserver/src/pki.rs` `set_validity` ignores `_days` on purpose,
  for byte-deterministic certs — with seed-derived keys and fixed serials. A
  control credential issued from it could never expire or be revoked short
  of re-seeding the whole cluster.
* The apiserver accepts **any** leaf that chains to that CA as a Kubernetes
  identity (`client_verifier` with `allow_unauthenticated`,
  `server/tls_acceptor.rs`). A control cert from the same CA would double as
  a Kubernetes credential.
* Boot refuses a legacy public CA on a reachable address
  (`RuntimeError::PublicCaOnReachableAddress`, `engenho-runtime/src/runtime.rs`)
  — exactly plo/rio's posture. A control plane rooted in that CA would be down
  in the one case it is needed to repair.

With pins, the two planes structurally reject each other's certificates,
revocation is removing a pin, and remote control survives a broken cluster
PKI.

## P0b spike: forge-gen vs. the fallback

The org standard is spec-first: one OpenAPI spec projected by `forge-gen`.
The spike ran forge-gen's `rust-axum` server and `rust` SDK targets over
this spec (openapi-generator-cli from nixpkgs) against the plan's checklist:

| # | Check | Result |
|---|---|---|
| 1 | Output builds in the workspace | ✗ — the server's `Cargo.toml` gets `version = "v1.0.0"` (invalid semver); the SDK fails with 12 errors (a duplicate `Replay`: an inline tag property collides with the schema name) |
| 2 | Closed enums and tagged `oneOf`s | ✗ — every union comes out as an untagged `…OneOf..OneOf7` with an arbitrary `Default` |
| 3 | No native-tls (`ci/no-c-tls.tlisp`) | ✗ — the SDK defaults reqwest to `native-tls` |
| 4 | `MatchedPath` visible to a `route_layer` | ✓ |
| 5 | SDK accepts an injected `reqwest::Client` | ✓ |
| 6 | Byte-identical regeneration | ✗ — a build timestamp in the generated README |
| 7 | `Cargo.gen.lock` + Nix build | unmeasured (the build host ran out of disk) |

**Verdict: FALLBACK**, still spec-first. `engenho-control-types/build.rs`
runs `typify` over the spec's `components.schemas` into `OUT_DIR` and emits
the catalog, the typed requests and the traits beside them. Nothing
generated is committed, and no JVM is needed at build time. `forge-gen.toml`
stays (completions and docs), and records
`pending-forge-gen: rust-typed target` — the load-bearing fix is a forge-gen
target that emits tagged enums and a rustls-only client, after which this
build script folds back into forge-gen.

Deviations the spec settled while being authored:

* `ReinitOp` has a fourth variant, `rotate_control_identity`, because
  rotating the control identity is destructive and so confirm-gated.
* `createConfirmation` / `cancelConfirmation` carry their own
  `x-engenho-confirmation: issue | cancel` marker, so "destructive ⇔
  confirm-gated" holds for every other destructive operation.
* `exitProcess` is **mutate**, not destructive: it ends the process, which a
  service manager restarts; nothing on disk changes.

## Crates

| Crate | Holds |
|---|---|
| `engenho-serve` | the owned-connection serve loop (`Listener`, `Handshake`, `serve`, `StopHandle`) |
| `engenho-control-types` | everything derived from the spec, plus `Principal` (Serialize-only), `ControlError`, the HTTP rendering helpers |
| `engenho-runtime` | the supervisor, the boot journal, the publish records, and `control::DaemonControl` — the one `EngenhoControl` |
| `engenho-control-server` | the socket, the grant, the router, the audit chain |
| `engenho-control-client` | typed and generic calls, and the socket resolution `engenho ctl` shares with the daemon |

## Phases

| # | What | Status |
|---|---|---|
| P0a | Clean store handoff: owned connection tasks (`engenho-serve`), `shutdown → StoreReleased`, boot unwind | done |
| P0b | Spec, `engenho-control-types`, forge-gen spike | done |
| P1 | Supervisor, boot-phase journal, stay-up on failure, retry classes | done |
| P2 | Local UDS, observe tier, `engenho ctl` | done |
| P3 | Operate verbs, authorization tiers, audit chain, restart on failure only | done |
| P4 | Persisted config overrides, sealed mutability, drift | — |
| P5 | Remote mTLS with SPKI pins | — |
| P6 | Child control | — |
| P7 | Gated destructive re-init | — |
| P8 | MCP tools, completions, docs | — |
