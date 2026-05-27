# engenho — the typescape (the typed universe)

> The complete set of typed primitives that make up engenho, and the
> **bridge** that lets them cross from sui's Nix value tree into the
> live runtime. "Typescape this" — the universe of types, organized by
> domain, with the canonical type per node and the round-trip law that
> binds them.
>
> Companions: [`STATE-MACHINES.md`](STATE-MACHINES.md) (the dynamics) ·
> [`STRATEGY.md`](STRATEGY.md) (the invariants) ·
> [`MANY-FACES.md`](MANY-FACES.md). Platform-wide typescape: the
> `typescape` skill (8 typed dimensions, arch-synthesizer).

## The bridge — `engenho-sui-typescape`

engenho's types are `Send + Sync` (they cross Raft logs, attestation
workers, mirante channels, the tokio reconcile runtime). sui's
`sui_eval::Value` tree is single-threaded (`Rc` everywhere) and carries
eval-time machinery (Lambda/Builtin/Thunk). The two cannot meet
directly. [`TypescapeValue`](../engenho-sui-typescape/src/value.rs) is
the Send+Sync mirror; [`Typescape`](../engenho-sui-typescape/src/ext.rs)
is the conversion trait.

```rust
pub enum TypescapeValue { Null, Bool(bool), Int(i64), Float(f64),
    String(Arc<str>), Path(Arc<str>), List(Arc<[TypescapeValue]>),
    Attrs(Arc<BTreeMap<Arc<str>, TypescapeValue>>) }   // sorted ⇒ deterministic BLAKE3

pub trait Typescape: Sized {
    fn to_typescape_value(&self) -> TypescapeValue;
    fn from_typescape_value(v: &TypescapeValue) -> Result<Self, TypescapeError>;
}
```

**Round-trip law (proptest-enforced for every implementer):**

```text
for every well-formed t: T ⇒ T::from_typescape_value(&t.to_typescape_value())? == t
```

Variants Lambda/Builtin/Thunk are *intentionally absent* — thunks force
before crossing; the bridge is lossless because none ever appears.
Foundational impls ship for `bool · i64 · u64 · f64 · String · Vec<T> ·
Option<T>`; every domain type below adds its own (newtype + `Attrs`
shape). The payoff: a primitive that impls `Typescape` can be authored
in a `(defsistema …)` / `(deftypescape …)` form, reconciled by
`engenho-fonte`, attested by tameshi, and observed via mirante — **with
zero per-consumer code**.

## The universe, by domain

Eight domains. Each row is a *node* in the typescape; the canonical type
is the one everything else in that domain hangs off.

### 1. Identity
| Type | Where | Shape |
|---|---|---|
| `NodeId` | revoada/substrate | `[u8;32]` = ed25519 pubkey (revoada); `String` alias in topology; `to_hex`/`from_hex` |
| `RaftNodeId` | store | `u64` (compact, serde-friendly) |
| `Selo` | substrate `selo.rs` | capability token `(subject, capability, expires_at)` BLAKE3-MAC'd |

### 2. Cluster / topology
| Type | Where | Shape |
|---|---|---|
| `ClusterState` / `ClusterEvent` | `kikai/src/state.rs` | 14-state lifecycle FSM (SM ①) |
| `Role` | `topology.rs` | `Master · Worker · Bootstrap · Observer`; `is_voting()` |
| `NodeState` | `topology.rs` | `Joining · Standby · Active(Role) · Demoting · Departing · Failed` |
| `RoleAssignment` | `topology.rs` | committed `Vec<(NodeId, NodeState)>`; `has_majority()` |
| `Transition` | `topology.rs` | `Admit · Promote · Demote · Reassign · Evict` |
| `RoleAssignment` (cmd) | `consensus/role_assignment.rs` | `Promote · Demote · Quarantine · Restore` + `Reason` |
| `RoleAttestationBlock` | revoada/attestation | `{prev_hash, assignment, raft_term, raft_log_index, leader_sig, witness_sigs}` |

### 3. Resource / kind (the faces)
| Type | Where | Shape |
|---|---|---|
| `KubeResource` (trait) | `engenho-types/src/kind.rs` | `const GVK/GVR/SCOPE`; `name/namespace/resource_version` |
| `GroupVersionKind`, `Scope` | kind.rs | `{group,version,kind}`; `Namespaced \| Cluster` |
| `ObjectMeta` | meta.rs | BTreeMap-everywhere (byte-deterministic) |
| `WatchEvent<R>` | watch.rs | `Added · Modified · Deleted · Bookmark · Error` |
| `WorkloadIntent` + `WorkloadTranslator` | translator.rs | canonical IR; K8s `Deployment` ↔ Nomad `Job` (6 invariants) |
| `nomad_v1::{Job,TaskGroup,Task}` | nomad_v1.rs | the Nomad face's typed catalog |
| generated catalog | `generated_v1_34/{core,apps,rbac}` | OpenAPI-emitted; **no hand-authored kinds** |

### 4. Store / command
| Type | Where | Shape |
|---|---|---|
| `ResourceKey` / `ResourceValue` | `engenho-store/src/resource.rs` | catalog key/value |
| `ResourceCommand` | command.rs | `Put · Patch · Delete` + `Reason` |
| `ResourceOp` | command.rs | `Created · Replaced · Patched · Deleted · NoOp` |
| `WatchEvent` / `WatchEventKind` | watch.rs | mutation broadcast |

### 5. Consistency
| Type | Where | Shape |
|---|---|---|
| `ConsistencyTier` / `ConsistencyTierKind` | `engenho-types`, `engenho-config/src/consistency.rs` | `Strong · EventualGossip · DurableStream · Content` (per-resource hint) |

### 6. Substrate / shape (the derivation universe)
| Type | Where | Shape |
|---|---|---|
| `Drv` / `DrvHash` / `NarHash` | `derivation.rs` | content-addressed derivation + closure `input_drvs` |
| `Realisation` | derivation.rs | `{drv_hash, output_name, output_path, nar_hash}` |
| `WorkloadShape` | `shape.rs` | `OciImage · NixClosure · Qcow2 · Wasm · StaticBinary{triple} · HelmChart · Custom{name}` |
| `MaterializationReceipt` / `ReceiptKind` | `receipt.rs` | signed evidence of materialization |
| `QuorumOutcome` | `quorum.rs` | `Pending · Reached · Dissent` (K-of-N independent rebuilds) |
| `LineageGraph` / `LineageProof` | `linhagem_aberta.rs` | causality DAG, BLAKE3 fingerprints |
| `Budget` (orçamento) · `Provacao` · `Clock`/`HlcClock` (relógio) | resp. files | token-bucket · deterministic fault inject · logical clock |
| `StateMachine` / `MachineRunner` / `TransitionRecord` | `maquina.rs` | the FSM substrate every SM lifts into |

### 7. Sistema / promessa (the declaration)
| Type | Where | Shape |
|---|---|---|
| `Sistema` | `engenho-fonte/src/sistema.rs` | `{apps, infra, topology, promessas}` |
| `AppRef · InfraRef · PromessaRef · TopologyRef` | sistema.rs | typed references |
| `PromessaKind` | sistema.rs | `Compliance · CostBudget · CustomerKpi · Sla · Security` |
| `Change · Decision · Outcome` | change.rs | the convergence event pipeline (SM ⑩) |

### 8. Config
| Type | Where | Shape |
|---|---|---|
| `EngenhoConfig` | `engenho-config/src/lib.rs` | `{cluster, revoada, teia, scheduler, controllers, consistency}` (shikumi `TieredConfig`) |
| `TopologyStrategyKind` | revoada.rs | mirrors the 6 strategies |
| `SchedulerStrategyKind` · `ControllerEnable` | scheduler.rs, controllers.rs | tunables + per-controller toggles |

## Authoring surface — `(defsistema)` / `(deftypescape)`

The eventual front door (tatara-lisp keyword `defsistema`, registered
via `#[derive(TataraDomain)]`):

```text
(defsistema "rio-cluster"
  :apps     [(appref "podinfo" :version "6.4.1")]
  :infra    [(inframagma "rio-net")]
  :promises [(promessaref "sla" :kind :availability :target 99.99)]
  :topology (topology "quorum-3m" :nodes 3))
```

Today the same `Sistema` shape is reachable three ways that all
converge: `parse_json` (always-on), `SistemaBuilder` (in-Rust),
`parse_nix` (gated `with-sui-eval`), and `to_authoring_form` emits the
canonical lisp for round-trip diagnostics. When the macro lands, all
paths collapse into the keyword.

## Registration status

| Tier | Typescape coverage |
|---|---|
| Foundational scalars/collections | ✅ in `engenho-sui-typescape::ext` |
| Substrate state enums + shapes | scaffolded in [`engenho-machines`](../engenho-machines/) (substrate-first) |
| Resource catalog (`KubeResource` kinds) | via serde today; `Typescape` derive arrives with kube-forge (M0.0.3+) |
| caixa / pangea / magma / viggy-promessa | gain `Typescape` so a `(defsistema)` references them by name without losing type-safety |

The rule (from `engenho-sui-typescape` lib docs): **arch-synthesizer's
typescape registers a `Typescape` impl for every TataraDomain alongside
its Serialize/Deserialize.** engenho participates as one region of that
platform-wide universe; this doc is engenho's slice of it.
