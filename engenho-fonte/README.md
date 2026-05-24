# engenho-fonte

**The live source-of-truth reconciler.** An operator edits a typed
declaration; the cluster converges to be what the declaration says.
Every transition is typed, attested, observable.

```
ShikumiWatcher → SuiEvaluator → SystemController → MockAttester → MirantePublisher
                                ├ KubeAppReconciler (typed K8s Deployment)
                                ├ CaixaAppReconciler → CaixaHelmInstaller
                                ├ LinhagemAnomalyChain (typed DAG-backed drift log)
                                └ AnomalyRouter (typed RemediationPolicy fan-out)
+ RevoadaProposer → PureRaftFace (cluster-visible typed resources)
+ FederationBroker / FaceGossipBroker (cross-cluster sync)
+ ProvisioningController / ShigotoProvisioningController (typed M5 bootstrap)
+ TameshiAttester + TameshiOutcomeChain (cryptographic chains)
+ ProvacaoConduit (typed chaos injection)
+ ShigotoRetryConduit (typed retry policy)
```

## Quickstart

```rust
use engenho_fonte::*;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    // 5 typed roles wired to mock impls by default. Real
    // backends slot in behind feature flags (see matrix below).
    let watcher = Arc::new(MockWatcher::new());
    let evaluator = Arc::new(MockEvaluator::new());
    let (_a, _i, _p, _t, ctrl) = mock_system_controller();
    let conduit = Conduit::new(
        watcher.clone(),
        evaluator,
        Arc::new(ctrl),
        Arc::new(MockAttester::new()),
        Arc::new(MockPublisher::new()),
    );

    // Push a typed Sistema declaration.
    watcher.push(Change {
        source: "rio".into(),
        kind: ChangeKind::Initial,
        source_text: r#"{"name":"rio","apps":[],"infra":[],"promises":[],"topology":{"strategy":"solo","nodes":1}}"#.into(),
        revision: 1,
    }).await;

    // One tick converges the system.
    let outcome = conduit.tick().await.unwrap().unwrap();
    println!("converged: rev={} proposal={}", outcome.revision, outcome.proposal_id);
}
```

## Cargo feature matrix

| Feature | What | Cross-workspace dep |
|---|---|---|
| (default) | Five typed roles + mock impls everywhere | none |
| `with-shikumi` | Real file-watch `Watcher` via notify | shikumi |
| `with-sui-eval` | Real `Evaluator` (sui Nix bytecode VM) | sui-eval |
| `with-revoada` | Real `Proposer` + `FaceGossipBroker` (revoada `Face`) | engenho-revoada |
| `with-engenho-kube-client` | Live K8s apply for `KubeAppReconciler` | engenho-kube-client |
| `with-tameshi` | Real cryptographic chain (TameshiAttester + TameshiOutcomeChain) | tameshi |
| `with-promessa` | 5 typed TargetControllers (Sla / CostBudget / Compliance / Security / CustomerKpi) | promessa-types |
| `with-tatara-lisp` | `(defsistema)` keyword authoring + parse_tlisp | tatara-lisp |
| `with-caixa` | Real chart renderer (CaixaAppReconciler + CaixaHelmInstaller) | caixa-core, caixa-helm |
| `with-shigoto` | Typed DAG provisioning + retry policy | shigoto-dag, shigoto-retry, shigoto-budget, shigoto-types |

Operators stack features per cluster shape. The 5 mock impls stay always-on
so tests + CI never depend on a real cluster.

## The 5 typed roles

1. **`Watcher`** — emits typed `Change` events. Mocks: `MockWatcher`. Real: `ShikumiWatcher` (notify), `FederatedWatcher` (in-process broadcast), `FaceFederatedWatcher` (revoada Face).
2. **`Evaluator`** — parses + types the source into a `Decision`. Mocks: `MockEvaluator` (JSON). Real: `SuiEvaluator` (Nix).
3. **`Proposer`** — commits the typed decision to consensus. Mocks: `MockProposer`. Real: `SystemController` (fans out to sub-reconcilers), `RevoadaProposer` (face.apply_resource).
4. **`Attester`** — chains a typed receipt. Mocks: `MockAttester` (BLAKE3 Vec). Real: `TameshiAttester` (HeartbeatChain).
5. **`Publisher`** — broadcasts the terminal `Outcome`. Mocks: `MockPublisher` (Vec). Real: `MirantePublisher` (typed ObservationChannel).

Plus optional add-ons that wrap the conduit:

- `OutcomeChainRecorder` — second cryptographic chain for outcomes (separate from attester).
- `AnomalyRouter` + `RemediationPolicy` — typed routing per drift event.
- `ProvacaoConduit` — typed chaos injection.
- `ShigotoRetryConduit` — typed retry policy with FailureKind classification.

## Sistema authoring

Three equivalent ways to declare a Sistema:

**JSON** (always-on):
```rust
let s = parse_json(r#"{ "name": "rio", "apps": [], ... }"#)?;
```

**Builder** (always-on):
```rust
let s = SistemaBuilder::new("rio")
    .app("podinfo", Some("6.4.1"))
    .topology("quorum_3m", 3)
    .build();
```

**Nix** (`with-sui-eval`):
```rust
let s = parse_nix(r#"{ name = "rio"; apps = [...]; ... }"#)?;
```

**Lisp** (`with-tatara-lisp`):
```rust
let s = parse_tlisp(r#"(defsistema "rio"
  :apps     ((appref "podinfo" :version "6.4.1"))
  :infra    ((inframagma "rio-net"))
  :promises ((promessaref "sla" :kind :availability :target 99.99))
  :topology (topology "quorum_3m" :nodes 3))"#)?;
```

Round-trip: `to_authoring_form(&s)` renders any Sistema back to the canonical
`(defsistema …)` lisp form; parse_tlisp consumes it.

## Operator-facing daemon

`engenho-fonte-cli` (sibling crate) ships a binary that wires the full loop:

```bash
engenho-fonte --file ./sistemas/rio.nix
engenho-fonte --file ./sistemas/rio.nix --with-revoada
engenho-fonte --file ./sistemas/rio.nix --log-level debug
```

Internally: shikumi notify → sui eval → SystemController + KubeAppReconciler
+ LinhagemAnomalyChain → MockAttester → MockPublisher (or real backends
behind feature flags).

## Pillar alignment

Per `pleme-io/CLAUDE.md` Pillar 7 (Kubernetes control rendered from typescape)
+ the Viggy Method (CONTINUOUS-SOLUTION-MACHINE.md):

- **CONTINUOUS CONVERGENCE**: every Conduit tick is one beat of the seven-beat
  Viggy convergence loop (Observe → Diff → Classify → Decide → Act → Attest → Tick).
- **PROVABLE OUTCOMES**: TameshiAttester + TameshiOutcomeChain together prove
  every transition cryptographically — auditors verify with public keys.
- **Typescape**: every typed value passes through `engenho-sui-typescape`'s
  `TypescapeValue` bridge so sui's lazy evaluator + Rust's typed substrate
  share one shape.

## Testing

```bash
cargo test --workspace          # mock impls only
cargo test --workspace --all-features  # all real impls
```

Per-feature tests live in `tests/<feature>_*.rs` files. The full-stack
acceptance test (`tests/full_stack.rs`) drives a real Nix file → real notify
watch → real sui eval → typed K8s manifest + DAG-backed drift chain +
cryptographic chain end-to-end.

## Status

Substrate-complete. Every gap from the destination roadmap shipped across
v1.16–v1.52. Real cluster integration via `cargo test --features with-engenho-kube-client`
gated by `ENGENHO_LOCAL_LIVE_TEST=1` + engenho-local kubeconfig.

License: MIT.
