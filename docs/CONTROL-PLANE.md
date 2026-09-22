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
* **A held boot settles against the file, not against the events.** Each
  attempt records the BLAKE3 of the declared file it read; when the attempt
  lands failed, the file is re-read and a difference is a retry the supervisor
  asks for itself. `ConfigChanged` is refused inside `booting` — a change
  during a boot is not a second trigger — so without this, a change whose
  event arrived in that window was lost and the daemon held on a configuration
  that was already correct on disk. It covers an event that never arrives at
  all, too: the watcher is an optimisation, and the file is the truth.
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

## Configuration after it is running (P4)

The declared file is the one Nix writes. Over it sits the **override tier**:
leaves set through the control plane, kept in
`data_dir/control/overrides.yaml` (mode `0600`, written atomically) and folded
after the declared file, so an override wins leaf by leaf and every view
credits it — `engenho ctl config leaves` as `override` (with who set it, as
the kernel attested, and when), `engenho config-show` as the overrides file.
There is no ConfigMap tier.

* **Every leaf is classified, and the table cannot fall behind.**
  `engenho_config::mutability` gives each leaf one class — `inert`, `live`,
  `respawn`, `next_boot`, `restart_runtime` or `not_overridable` — through a
  `section!` per config struct that names every field and destructures the
  struct with no `..`: a new field is a compile error until it is classified,
  and a test pins the table to what the configuration serializes to.
  `data_dir` (the tier lives under it), `durable`, the cluster and node names,
  the service CIDR, the control socket and the retired `teia` keys are
  `not_overridable`.
* **One pipeline** (`engenho-runtime::control::apply`) serves `config set`,
  `unset`, `clear` and `reload`: build the candidate override set, resolve it
  with the fold a boot uses, gate it with a boot's own validators
  (`validate()`, `deny_unknown_fields`, `BootConfig::read` — there is no
  second validator to disagree with boot), diff it leaf by leaf, refuse the
  whole change if a `not_overridable` leaf moves, stop there on `--dry-run
  true`, then commit and apply. `--precondition-generation` makes it
  optimistic-concurrency safe; the generation is persisted, so it keeps
  rising across restarts.
* **Applying** is the supervisor's: a running runtime adopts the in-place
  leaves (`live` ones republish the kubeconfigs at once), and what takes a
  restart is recorded as `pending: restart_needed` on the lifecycle — or
  restarts it with `--restart-policy now`. A boot held on its configuration
  is retried by an override change, exactly as by a change to the declared
  file, so a broken declared file is repaired without touching it:
  `engenho ctl config set scheduler.tick_interval_seconds --value 5`.
  `respawn` leaves apply in place by moving children (see Children below):
  the effect is `respawned` with the children it moved. A driver switch
  whose drivers cannot be spawned alone (`crd`, `service_routing`) is
  `restart_deferred` instead.
* **Drift** (`engenho ctl config drift`): a unified diff against the declared
  file alone, and per override whether removing it would change its leaf
  (`shadowing`) or not (`redundant`).
* **Persisted by default.** `--persist false` keeps an override in memory
  only; it is gone when the process ends.

## Children

Every long-lived task the runtime runs — each controller driver, the
:10250 and :2379 listeners, the node lease — is one row of the closed
catalog in `engenho-runtime/src/child.rs`, and a dead one is never
respawned on its own. The control plane respawns it on request:

* **`engenho ctl children restart <child>`** builds a child again, stopping
  it first if it runs, from the parts it was first built from (kept on the
  runtime for that). Its `generation` moves on and its `last_death` is kept.
  How is the child's `respawn` row, one exhaustive match:
  * `rebuild` — alone: every stateless driver, the scheduler (a fresh one
    from `scheduler.*`), a listener, the node lease;
  * `rebuild_with` — the kubelet, fresh, with the node lease and its HTTP
    listener after it, since both were built against the old one. The Pod
    `/log` reader follows it through the runtime's kubelet slot;
  * `runtime_restart_only` — `crd`, `service_routing`, `csi_registrar`,
    refused (`respawn_refused`, pointing at `runtime restart`). Each shares
    state with the rest of the runtime (the router's CRD handlers, the
    installed service routes, the CSI driver table) and is not rebuilt alone
    until it is shown to resync from scratch — a stated limit.
* **`engenho ctl children enable|disable <driver>`** sets the driver's
  `controllers.enable` switch as a persisted override through the one apply
  pipeline, so it is gated, audited and reported as `config set` of that
  leaf is. The runtime follows at once: a disabled driver is stopped and
  forgotten, exactly as if the boot had never enabled it; an enabled one is
  spawned. One switch can gate several drivers (`pv_binder` gates the
  binder, the snapshot controller and pvc-protection). A driver with no
  switch always runs, and toggling it is refused.
* **A listener's address** (`runtime.kubelet_listen_addr`,
  `runtime.etcd_listen_addr`) set while running rebuilds that listener at the
  new address; an empty etcd address stops the façade.

Every respawn is on the event stream as `child_respawned`; a child that
happens to die while others are being stopped is still reported as
`child_died`.

## Destructive re-initialization (P7)

Four operations replace state a running cluster depends on:

| Operation | Replaces | Needs the runtime stopped |
|---|---|---|
| `reinit rotate-admin-token` | `pki/admin.token` (a new one is written at once; the running apiserver keeps the old until it boots again) | no |
| `reinit reseed-pki --sa-key keep\|rotate` | the cluster seed, and the CA and admin credential derived from it (`PkiFile::seed_derived`); `sa.key` with `rotate` | yes |
| `reinit wipe-store --scope store_only\|store_and_node_local` | the store; with `store_and_node_local`, every node-local area (`Area::node_local`) | yes |
| `control rotate-identity` | the control identity's key; the remote listener presents the new one from the next handshake, with no restart | no |

**The handshake** (`engenho-runtime/src/control/confirm.rs`). `engenho ctl`
runs it for any operation the catalog marks `ConfirmGate::Executes`, so
nothing about any one operation is written in the client:

1. `reinit prepare` binds a challenge to the cluster and node names, the CA's
   fingerprint, the stop epoch (for the store operations), the operation and
   a BLAKE3 digest of its parameters, and the caller — the uid on the local
   socket, the pinned key remotely, never the process. It returns a
   single-use 128-bit id, the phrase (the cluster's name), and the blast
   radius, one consequence a line. It stands for 120 seconds.
2. The operation carries the id (`Engenho-Confirmation`) and the phrase
   (`--confirm-phrase`, or typed at a terminal). The daemon takes the
   challenge out of its book first — it is used up whatever happens next —
   then checks each bound fact again, afresh. A stale epoch (the runtime ran
   since), another operation, other parameters, another caller or a wrong
   phrase is `confirmation_mismatch`; an unknown or used id is
   `confirmation_required`; an expired one `confirmation_expired`. Without a
   phrase and without a terminal, or with a wrong one, `engenho ctl`
   withdraws the challenge and exits 5, having done nothing.
3. The data-directory operations run in the supervisor's loop, which checks
   again that the runtime rests (stopped, or its boot failed, nothing booting
   or draining) in the bound epoch and holds the store's lock throughout, so
   nothing boots between the checks and the move.

Tier-honest: this proves the operation is aimed at the cluster the operator
named, as it was when they looked, by whoever looked, once. It does not prove
a human typed it, which is why no destructive operation will be an MCP tool.
The plan named `Selo` for the challenge; the client never carries the binding,
only an id the daemon keeps beside it, so there is nothing for a MAC to
protect, and single use needs the daemon's state regardless.

**Nothing is deleted.** What an operation replaces is renamed into
`data_dir/control/attic/<operation>-<time>/` at the same relative path — a
rename, so it costs nothing however large the store — and a move that fails
part-way puts back what it had moved. `data_dir/control/` itself is never
replaced by a data-directory operation (a test holds every row out of it).
The report names the attic, every published kubeconfig a re-seed made stale,
and what to do next. After a wipe the next boot is a first boot; after a
re-seed the next boot mints a new CA.

The file names these operations move are named once: `PkiFile` (in
`engenho-apiserver`, which owns the PKI) and `Area` (the data directory's
layout, in `engenho-runtime`) — the same enums every reader and writer of
those files and directories now goes through.

## Agents: the control plane as MCP tools (P8)

`engenho-mcp` offers one tool per operation of the catalog,
`control_<resource>_<verb>` (the `engenho ctl` spelling), generated at start
from `CATALOG` (`engenho-mcp/src/control.rs`): parameters by name, the body as
`body`, the call over this machine's socket through the same resolution and
request rendering (`engenho_control_client::render`, the daemon's own parser)
`engenho ctl` uses. An answer is the daemon's own: found, refused (reason and
what would be accepted) or blind — a daemon that cannot be reached is blind,
never an empty success.

The launch decides the set: without flags the server offers the observe
operations; `engenho-mcp --allow-mutate` adds the mutate ones; no flag offers
a destructive or sensitive one (a test holds both). Every call carries
`Engenho-Actor: agent` and `Engenho-Ceiling: <tier>`, so the daemon caps it at
the launch's tier on its own and the audit chain names the agent. The grant
is control-plane mutation only: Kubernetes writes through the writer trait
stay closed until the saguão passport.

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

### How it works (P5)

* **The daemon's side.** `control.remote.{enable, listen_addr,
  authorized_clients}` in the declared file (Nix:
  `services.engenho.config.control.remote`). Each client is
  `{name, spki_sha256, tier}`: the tier is the daemon's word for that pin,
  never the certificate's. The listener's key is created in
  `data_dir/control/identity/key.pem` (0600) on first start whether or not
  remote control is on, so `engenho ctl control show` prints its pin
  (`identity.spki`) before it is. A key others can read is refused.
* **The client's side.** `engenho remote keygen <name>` makes this
  machine's key for a daemon (`~/.config/engenho/remotes/<name>.key`, 0600,
  never replaced) and prints its pin; `~/.config/engenho/remotes.yaml` lists
  each daemon's `address` and `server_spki` (a list, so a rotation can
  overlap); `engenho ctl --remote <name> …` then speaks the same API as over
  the socket, and `hello` answers `transport: mtls` with the pinned client as
  the principal.
* **The handshake.** TLS 1.3 only, ring, both sides self-signed ed25519. The
  server admits a client certificate iff its key's pin is in the set; the
  client trusts the server iff its key's pin is listed. Both still verify the
  handshake signature, so a pinned certificate presented without its key
  fails — a test proves it, and a negative control (the verification stubbed
  out) proves the test would notice. `engenho-control-types`' `pin` module is
  the one place both verifiers live.
* **Revocation.** The daemon follows its declared file (one watcher, shared
  with the supervisor): a pin removed there is refused on the client's next
  request, over an open connection too, because the pin is looked up per
  request rather than per connection. Turning the listener on or moving it
  takes a restart.
* **Never fatal.** What keeps the listener from serving is its state,
  reported by `engenho ctl control show`: `serving{addr}`, or
  `absent{disabled | no_authorized_clients | control_config_invalid |
  identity_unavailable | bind_failed{retry_in}}`; a failed bind (the tailnet
  address not up yet) is retried on a doubling backoff to 60 s.
* **Not overridable.** `control.remote.*` is `not_overridable`: a mutate-tier
  caller could otherwise pin a key of its own at destructive.

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
| `engenho-control-types` | everything derived from the spec, plus `Principal` (Serialize-only), `ControlError`, the HTTP rendering helpers; with feature `tls`, SPKI pins and both rustls pin verifiers |
| `engenho-config` | the configuration, its leaves (`leaf`), their sealed classes (`mutability`), and the fold with the override tier |
| `engenho-runtime` | the supervisor, the boot journal, the publish records, the override store and apply pipeline, and `control::DaemonControl` — the one `EngenhoControl` |
| `engenho-control-server` | the socket, the remote listener, its identity and pins, the grant, the router, the audit chain |
| `engenho-control-client` | typed and generic calls, the socket resolution `engenho ctl` shares with the daemon, and remote endpoints (`remotes.yaml`, client keys) |

## Phases

| # | What | Status |
|---|---|---|
| P0a | Clean store handoff: owned connection tasks (`engenho-serve`), `shutdown → StoreReleased`, boot unwind | done |
| P0b | Spec, `engenho-control-types`, forge-gen spike | done |
| P1 | Supervisor, boot-phase journal, stay-up on failure, retry classes | done |
| P2 | Local UDS, observe tier, `engenho ctl` | done |
| P3 | Operate verbs, authorization tiers, audit chain, restart on failure only | done |
| P4 | Persisted config overrides, sealed mutability, drift | done |
| P5 | Remote mTLS with SPKI pins | done (the fleet's Nix wiring: the `pleme-io/nix` repo) |
| P6 | Child control: respawn, driver switches and listener moves in place | done |
| P7 | Gated destructive re-init: confirmation handshake, attic, control identity rotation | done |
| P8 | MCP tools, docs | done — completions pending (below) |

`pending-completions: no consumer for nested verbs`. Shell completion of
`engenho ctl <resource> <verb>` has nothing in the fleet's shell stack to
read it yet, measured 2026-09-22: skim-tab (`skim-tab/src/specs.rs`,
`SpecRegistry::lookup`) consults only a spec's first level, and only to
decorate candidates with a glyph and a description; frostmourne's
`defcompletion` forms are flat and wait on a `frost-complete` dispatcher that
has not landed (`frostmourne/lisp/40-completions.lisp`). Emitting a nested spec
now would be an artifact nobody reads. What is done: sekkei 0.2 keeps an
operation's vendor extensions (`Operation::extensions`), so a generator can
group this API by its `x-engenho-cli` spelling instead of guessing from tags
and operation ids. When the dispatcher lands, the completions are generated
from the same `CATALOG` the client and the MCP tools are.
