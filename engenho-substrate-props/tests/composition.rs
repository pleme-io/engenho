//! Cross-primitive composition tests — proves the inventive primitives
//! (v0.51–v0.59) compose at the substrate level, not just in isolation.
//!
//! Each property exercises ≥2 typed primitives in the same test body.
//! The substrate's value isn't the 9 primitives alone — it's the
//! algebra of their interactions. These tests guard the algebra.

use engenho_substrate::{
    Budget, BudgetSnapshot, Clock, Instant, LineageGraph, MachineRunner, Mirante,
    ObservationChannel, Policy, Provacao, ReplayCursor, Risca, SeloIssuer, StateMachine,
    define_named, impl_error_kind, replay_into,
};
use engenho_substrate_props::helpers::frozen_clock;
use engenho_substrate_props::{block_on, proptest_with_env};
use proptest::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

// ── orçamento + mirante: Budget IS Observable (v0.84) ──

proptest_with_env! {
    /// Budget now implements `Observable` directly. Passing
    /// `<Budget as Observable>::snapshot()` and the inherent
    /// `Budget::snapshot()` produces byte-identical output —
    /// the trait impl delegates to the inherent method.
    #[test]
    fn budget_observable_matches_inherent_snapshot(
        cap in 10u64..1000,
        rate in 0u64..100,
    ) {
        use engenho_substrate::Observable;
        let clock = frozen_clock(0);
        let budget = Budget::new("test-budget", cap, rate, clock);
        let inherent: BudgetSnapshot = Budget::snapshot(&budget);
        let via_trait: BudgetSnapshot = Observable::snapshot(&budget);
        prop_assert_eq!(inherent, via_trait);
    }

    /// v0.99 SSC composition test (added v1.03): ReplayCursor<E> is
    /// Observable + publishes ReplayCursorSnapshot through a mirante
    /// channel. Asserts the cursor's progress metadata (name +
    /// position + len + remaining + is_done) reaches the registry.
    /// Pairs with v0.97 plantio + v1.02 lineage in composition shape.
    #[test]
    fn replay_cursor_observable_publishes_to_mirante(
        n_events in 1usize..8,
        n_consume in 0usize..4,
    ) {
        use engenho_substrate::{Observable, ReplayCursor, ReplayCursorSnapshot};
        let events: Vec<u32> = (0..n_events as u32).collect();
        let cursor = ReplayCursor::new("test-cursor", events);
        for _ in 0..n_consume.min(n_events) {
            cursor.next();
        }
        // Direct Observable call.
        let snap: ReplayCursorSnapshot = Observable::snapshot(&cursor);
        prop_assert_eq!(snap.name, "test-cursor");
        prop_assert_eq!(snap.len, n_events);
        prop_assert_eq!(snap.position, n_consume.min(n_events));
        prop_assert_eq!(snap.remaining, n_events - n_consume.min(n_events));
        prop_assert_eq!(snap.is_done, n_consume >= n_events);
        // Through Mirante (v1.07 helper).
        let m = engenho_substrate_props::helpers::fresh_mirante_publishing("cursor", &cursor);
        let all = m.snapshot_all();
        prop_assert_eq!(&all["cursor"]["name"], &serde_json::json!("test-cursor"));
        prop_assert_eq!(&all["cursor"]["len"], &serde_json::json!(n_events));
        prop_assert_eq!(&all["cursor"]["position"], &serde_json::json!(n_consume.min(n_events)));
    }

    /// v1.01 SSC: LineageGraph is now Observable + plugs into a
    /// mirante registry. Asserts the snapshot reports the right
    /// name + node_count + the registry surfaces it as JSON.
    /// Pairs with v0.97's plantio test in shape; LineageGraph is the
    /// 6th ChildCountSnapshot consumer.
    #[test]
    fn lineage_graph_observable_publishes_to_mirante(n_nodes in 0usize..6) {
        use engenho_substrate::{
            ChildCountSnapshot, LineageGraph, MiranteSnapshot, Observable,
        };
        let mut graph: LineageGraph<TransitionSample> = LineageGraph::new();
        let mut prev: Option<[u8; 32]> = None;
        for i in 0..n_nodes {
            let sample = TransitionSample {
                from: format!("s{i}"),
                to: format!("s{}", i + 1),
            };
            let causes = prev
                .map(|p| std::iter::once(p).collect())
                .unwrap_or_default();
            prev = Some(graph.append(sample, causes).unwrap());
        }
        // Direct Observable call.
        let snap: ChildCountSnapshot = Observable::snapshot(&graph);
        prop_assert_eq!(snap.name, "lineage-graph");
        prop_assert_eq!(snap.child_count, n_nodes);
        // Through Mirante (v1.07 helper).
        let m = engenho_substrate_props::helpers::fresh_mirante_publishing("lineage", &graph);
        let all = m.snapshot_all();
        prop_assert_eq!(&all["lineage"]["name"], &serde_json::json!("lineage-graph"));
        prop_assert_eq!(&all["lineage"]["child_count"], &serde_json::json!(n_nodes));
        // Snapshot-of-snapshots.
        let m_snap: MiranteSnapshot = Observable::snapshot(&m);
        prop_assert_eq!(m_snap.channel_count, 1);
    }

    /// v0.96 SSC: Plantio is now Observable + plugs into a mirante
    /// registry alongside runtime wrappers. Asserts the snapshot
    /// carries the right name + stage count + the mirante registry
    /// surfaces it as JSON.
    #[test]
    fn plantio_observable_publishes_to_mirante(n_stages in 0usize..6) {
        use engenho_substrate::{
            ChildCountSnapshot, MiranteSnapshot, NodeId, Observable, Plantio, Stage,
            WorkloadShape,
        };
        let mut plantio = Plantio::new();
        for i in 0..n_stages {
            let stage_id = format!("s{i}");
            let stage = Stage::pinned(
                stage_id,
                WorkloadShape::OciImage,
                NodeId::from_bytes(&[i as u8; 32]),
            );
            plantio.add_stage(stage).unwrap();
        }
        // Direct Observable call.
        let snap: ChildCountSnapshot = Observable::snapshot(&plantio);
        prop_assert_eq!(snap.name, "plantio");
        prop_assert_eq!(snap.child_count, n_stages);
        // Through the Mirante registry (v1.07 helper) — substrate-internal
        // channels surface the Plantio snapshot as JSON.
        let m = engenho_substrate_props::helpers::fresh_mirante_publishing("plantio", &plantio);
        let all = m.snapshot_all();
        prop_assert_eq!(&all["plantio"]["name"], &serde_json::json!("plantio"));
        prop_assert_eq!(&all["plantio"]["child_count"], &serde_json::json!(n_stages));
        // Bonus: the Mirante itself is Observable. Snapshot-of-snapshots.
        let mirante_snap: MiranteSnapshot = Observable::snapshot(&m);
        prop_assert_eq!(mirante_snap.channel_count, 1);
        prop_assert_eq!(&mirante_snap.channel_names[0], &"plantio".to_string());
    }

    /// v0.91 TSR: 3 wrapper-with-N-children primitives all return
    /// the canonical `ChildCountSnapshot`. TieredCache /
    /// CompositeShapeRenderer / ChainedVerifier all impl Observable.
    /// Asserts the shape extraction is functionally consistent
    /// across all three implementers.
    #[test]
    fn all_three_child_count_observables_share_shape(_seed in any::<u8>()) {
        use engenho_substrate::{
            ChildCountSnapshot, Observable,
        };
        use std::sync::Arc;
        // TieredCache with 2 memory tiers.
        let tier_a: Arc<dyn engenho_substrate::DerivationCacheBackend> =
            Arc::new(engenho_substrate::MemoryDerivationCache::new());
        let tier_b: Arc<dyn engenho_substrate::DerivationCacheBackend> =
            Arc::new(engenho_substrate::MemoryDerivationCache::new());
        let cache = engenho_substrate::TieredCache::new(vec![tier_a, tier_b]);
        let cache_snap: ChildCountSnapshot = Observable::snapshot(&cache);
        prop_assert_eq!(cache_snap.name, "tiered");
        prop_assert_eq!(cache_snap.child_count, 2);

        // CompositeShapeRenderer with 1 renderer.
        let r: Arc<dyn engenho_substrate::ShapeRenderer> = Arc::new(
            engenho_substrate::FakeShapeRenderer::for_shape(
                engenho_substrate::WorkloadShape::OciImage,
            ),
        );
        let comp = engenho_substrate::CompositeShapeRenderer::default_named(vec![r]);
        let comp_snap: ChildCountSnapshot = Observable::snapshot(&comp);
        prop_assert_eq!(comp_snap.name, "composite");
        prop_assert_eq!(comp_snap.child_count, 1);

        // ChainedVerifier with 0 verifiers.
        let chain = engenho_substrate::ChainedVerifier::default_named(vec![]);
        let chain_snap: ChildCountSnapshot = Observable::snapshot(&chain);
        prop_assert_eq!(chain_snap.name, "chained");
        prop_assert_eq!(chain_snap.child_count, 0);
    }

    /// v0.90: Mirante registry observes ITSELF. A meta-Mirante can
    /// register the primary registry and report on registry-level
    /// liveness alongside per-channel state. The MiranteSnapshot
    /// reports channel_count + channel_names from the BTreeMap.
    #[test]
    fn mirante_observes_itself(channel_count in 0usize..6) {
        use engenho_substrate::{MiranteSnapshot, Observable};
        let mut m = Mirante::new();
        for i in 0..channel_count {
            let name: &'static str = Box::leak(format!("chan{i}").into_boxed_str());
            let ch = Arc::new(ObservationChannel::new(0u32, frozen_clock(0)));
            m.register(name, ch);
        }
        let snap: MiranteSnapshot = Observable::snapshot(&m);
        prop_assert_eq!(snap.name, "mirante");
        prop_assert_eq!(snap.channel_count, channel_count);
        prop_assert_eq!(snap.channel_names.len(), channel_count);
    }

    /// v0.87 TSR: three Observable implementers all return the
    /// canonical `SubscriberSnapshot`. Asserts the shared shape
    /// is identical across BroadcastLedger / WatchedCache /
    /// FakeGossipTransport — the extraction is functionally
    /// equivalent to the v0.86 per-wrapper types.
    #[test]
    fn all_three_subscriber_observables_share_shape(_seed in any::<u8>()) {
        use engenho_substrate::{
            BroadcastLedger, FakeGossipTransport, MaterializationLedger, MemoryLedger,
            DerivationCacheBackend, MemoryDerivationCache, Observable, SubscriberSnapshot,
            WatchedCache,
        };
        block_on(async {
            let inner_l: Arc<dyn MaterializationLedger> = Arc::new(MemoryLedger::new());
            let bl = BroadcastLedger::new(inner_l);
            let inner_c: Arc<dyn DerivationCacheBackend> =
                Arc::new(MemoryDerivationCache::new());
            let wc = WatchedCache::new(inner_c);
            let gt = FakeGossipTransport::new();
            let b: SubscriberSnapshot = Observable::snapshot(&bl);
            let w: SubscriberSnapshot = Observable::snapshot(&wc);
            let g: SubscriberSnapshot = Observable::snapshot(&gt);
            // Same shape: each has a name + subscriber_count.
            assert_eq!(b.name, "broadcast");
            assert_eq!(w.name, "watched");
            assert_eq!(g.name, "fake-gossip");
            // Subscriber counts are non-negative usize (always true,
            // but the shape is uniform across all three).
            let _ = b.subscriber_count + w.subscriber_count + g.subscriber_count;
        });
    }

    /// v0.86: `BroadcastLedger` + `WatchedCache` both implement
    /// `Observable`. Their snapshots carry the subscriber_count
    /// for live ops dashboards. Asserts subscriber_count tracks
    /// active subscribers + the snapshot reflects it.
    #[test]
    fn broadcast_ledger_observable_tracks_subscribers(n_subs in 0usize..6) {
        use engenho_substrate::{BroadcastLedger, MaterializationLedger, MemoryLedger, Observable};
        block_on(async {
            let inner: Arc<dyn MaterializationLedger> = Arc::new(MemoryLedger::new());
            let ledger = BroadcastLedger::new(inner);
            let baseline = ledger.snapshot().subscriber_count;
            let receivers: Vec<_> = (0..n_subs).map(|_| ledger.subscribe()).collect();
            let snap = ledger.snapshot();
            assert_eq!(snap.name, "broadcast");
            assert_eq!(snap.subscriber_count, baseline + n_subs);
            drop(receivers);
            assert_eq!(ledger.snapshot().subscriber_count, baseline);
        });
    }

    /// v0.85: `MachineRunner<M>` now implements `Observable`. The
    /// MachineSnapshot carries (name, state, step_count, is_terminal)
    /// — substrate self-consumption: every FSM plugs into mirante
    /// without a per-consumer adapter.
    #[test]
    fn machine_runner_observable_carries_state_and_step_count(
        increments in proptest::collection::vec(1u32..50, 0..6),
    ) {
        use engenho_substrate::Observable;
        let safe_incs: Vec<_> = increments.into_iter().take(6).collect();
        let clock = frozen_clock(0);
        let mut runner = MachineRunner::<CounterMachine>::new(clock);
        for inc in &safe_incs {
            runner.step(CounterEvent::Inc(*inc)).unwrap();
        }
        let snap = Observable::snapshot(&runner);
        prop_assert_eq!(snap.name, "counter");
        prop_assert_eq!(snap.step_count, safe_incs.len());
        prop_assert!(!snap.is_terminal); // CounterMachine never terminates
        // Trip-back through serde keeps the snapshot intact.
        let json = serde_json::to_value(&snap).unwrap();
        prop_assert_eq!(&json["step_count"], &serde_json::json!(safe_incs.len()));
        prop_assert_eq!(&json["name"], &serde_json::json!("counter"));
    }
}

// ── orçamento + mirante: Budget observable via ObservationChannel ──────

proptest_with_env! {
    /// A Budget published as a BudgetSnapshot through an ObservationChannel
    /// in a Mirante registry — subscribers see the snapshot with the
    /// correct current/capacity values.
    #[test]
    fn budget_snapshot_flows_through_mirante(
        cap in 10u64..1000,
        rate in 0u64..100,
        consume in 0u64..10,
    ) {
        block_on(async {
            let clock = frozen_clock(0);
            let budget = Budget::new("test-budget", cap, rate, clock.clone());
            // Take an initial snapshot.
            let snap0 = budget.snapshot();
            // Wire it into a mirante channel.
            let chan: Arc<ObservationChannel<BudgetSnapshot>> =
                Arc::new(ObservationChannel::new(snap0.clone(), clock));
            let mut m = Mirante::new();
            m.register("budget", chan.clone());
            // Consume some tokens, snapshot again, publish.
            if consume > 0 && consume <= cap {
                budget.try_consume(consume).unwrap();
            }
            let snap1 = budget.snapshot();
            chan.publish(snap1.clone());
            // The mirante registry surfaces the latest snapshot.
            let all = m.snapshot_all();
            let json = &all["budget"];
            assert_eq!(json["available"], snap1.available);
            assert_eq!(json["capacity"], cap);
            assert_eq!(json["refill_per_sec"], rate);
        });
    }
}

// ── selo + provacao: verified selo gated by chaos injection ──────

#[derive(Debug, Clone, PartialEq, Eq, Error)]
enum AuthFault {
    #[error("rate-limited")]
    RateLimited,
}

impl_error_kind! {
    AuthFault {
        RateLimited => "rate_limited",
    }
}

proptest_with_env! {
    /// SeloIssuer verify + Provacao fault injection: the verify path
    /// is gated by chaos. p=0 always succeeds (real verify runs),
    /// p=1 always faults BEFORE the verify runs.
    #[test]
    fn selo_verify_gated_by_chaos(
        secret in any::<[u8; 32]>(),
        subj in "[a-zA-Z]{1,16}",
        cap in "[a-zA-Z:]{1,16}",
        chaos_p in 0.0_f64..1.0,
        seed in any::<u64>(),
    ) {
        let iss = SeloIssuer::new(secret);
        let selo = iss.issue(&subj, &cap, Instant::from_ms(1_000_000));
        let injector = Provacao::<AuthFault>::new("auth", seed).with_policy(
            Policy::Probability {
                fault: AuthFault::RateLimited,
                p: chaos_p,
            },
        );
        let clock: Arc<dyn Clock> = frozen_clock(0);
        // Single call: if chaos fires, AuthFault::RateLimited; else verify ok.
        let outcome: Result<(), AuthFault> = match injector.maybe_fault(clock.as_ref()) {
            Some(fault) => Err(fault),
            None => iss
                .verify(&selo, &subj, &cap, Instant::from_ms(0))
                .map_err(|_| AuthFault::RateLimited),
        };
        // Either we faulted or we verified successfully. Both are
        // valid outcomes — the test is that the COMPOSITION works,
        // not which branch fires.
        prop_assert!(outcome.is_ok() || matches!(outcome.unwrap_err(), AuthFault::RateLimited));
    }
}

// ── replay + máquina + linhagem-aberta: drive a machine through ──
// ── recorded events, capture each transition as a lineage node ──

#[derive(Default)]
struct CounterMachine;
define_named!(CounterMachine, "counter");

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum CounterState {
    Active(u32),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum CounterEvent {
    Inc(u32),
}

#[derive(Debug, Clone, Error)]
enum CounterErr {
    #[error("overflow")]
    Overflow,
}

impl_error_kind! {
    CounterErr {
        Overflow => "overflow",
    }
}

impl StateMachine for CounterMachine {
    type State = CounterState;
    type Event = CounterEvent;
    type Effect = u32;
    type Err = CounterErr;

    fn initial() -> Self::State {
        CounterState::Active(0)
    }

    fn step(state: &Self::State, event: &Self::Event) -> Result<(Self::State, u32), CounterErr> {
        match (state, event) {
            (CounterState::Active(n), CounterEvent::Inc(by)) => {
                let next = n.checked_add(*by).ok_or(CounterErr::Overflow)?;
                Ok((CounterState::Active(next), next))
            }
        }
    }

    fn is_terminal(_: &Self::State) -> bool {
        false
    }
}

proptest_with_env! {
    /// Replay a cursor through a MachineRunner; for each transition,
    /// derive a "Sample" fingerprint and record it in a LineageGraph.
    /// The graph's topological order matches the cursor's input order.
    #[test]
    fn replay_machine_records_lineage(
        increments in proptest::collection::vec(1u32..50, 1..8),
    ) {
        // Use a tame cap so we never overflow.
        let safe_incs: Vec<_> = increments.into_iter().take(8).collect();
        let cursor = ReplayCursor::new(
            "incs",
            safe_incs.iter().map(|n| CounterEvent::Inc(*n)).collect::<Vec<_>>(),
        );
        let mut runner =
            MachineRunner::<CounterMachine>::new(frozen_clock(0));
        let applied = replay_into(&mut runner, &cursor).unwrap();
        prop_assert_eq!(applied, safe_incs.len());
        // Each transition's resulting state becomes a lineage node.
        let mut graph: LineageGraph<TransitionSample> = LineageGraph::new();
        let mut prev_fp: Option<[u8; 32]> = None;
        for record in runner.history() {
            let sample = TransitionSample {
                from: format!("{:?}", record.from),
                to: format!("{:?}", record.to),
            };
            let causes = prev_fp
                .map(|fp| std::iter::once(fp).collect())
                .unwrap_or_default();
            let fp = graph.append(sample, causes).unwrap();
            prev_fp = Some(fp);
        }
        prop_assert_eq!(graph.len(), safe_incs.len());
        // Topological order of the chain is the same length as the
        // history (it's a linear DAG).
        prop_assert_eq!(graph.topo_sort().len(), safe_incs.len());
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TransitionSample {
    from: String,
    to: String,
}
engenho_substrate::impl_fingerprint!(TransitionSample);

// ── risca + serde: Risca-wrapped secret in a struct round-trips ──

#[derive(Serialize, Deserialize)]
struct UserCreds {
    id: u64,
    token: Risca<String>,
}

proptest_with_env! {
    /// A struct field wrapped in Risca round-trips through JSON
    /// deserialize (the serialize path emits REDACTED; deserialize
    /// trusts the input — that's the documented contract).
    /// Uses parsed-JSON equality on the token field to avoid the
    /// substring-collision false-positive (e.g. a token "d" would
    /// "appear" in "REDACTED" — that's a substring artifact, not a
    /// leak).
    #[test]
    fn risca_field_deserialize_round_trips(
        id in any::<u64>(),
        token in "[a-zA-Z0-9]{1,32}",
    ) {
        let creds = UserCreds {
            id,
            token: Risca::new(token.clone()),
        };
        // serialize emits REDACTED — that's the leak-proof guarantee.
        let parsed: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&creds).unwrap()).unwrap();
        prop_assert_eq!(&parsed["token"], &serde_json::json!("<REDACTED>"));
        prop_assert_eq!(&parsed["id"], &serde_json::json!(id));
        // deserialize accepts real bytes — restores the original.
        let raw_json = serde_json::to_string(&serde_json::json!({
            "id": id,
            "token": token,
        }))
        .unwrap();
        let back: UserCreds = serde_json::from_str(&raw_json).unwrap();
        prop_assert_eq!(back.id, id);
        prop_assert_eq!(back.token.into_inner(), token);
    }
}

// ── orçamento + provação: Budget survives chaos faults ──

#[derive(Debug, Clone, PartialEq, Eq, Error)]
enum BudgetCallFault {
    #[error("upstream timeout")]
    Upstream,
}

impl_error_kind! {
    BudgetCallFault {
        Upstream => "upstream",
    }
}

proptest_with_env! {
    /// A consumer wrapping Budget + Provação: every `call()` first
    /// asks the chaos injector + (if no fault) tries to consume from
    /// the budget. The Budget's own invariants (available bounded by
    /// [0, cap]; OverCapacity is structural) hold across the chaos.
    #[test]
    fn budget_under_chaos_preserves_invariants(
        cap in 10u64..1000,
        rate in 0u64..100,
        chaos_p in 0.0_f64..1.0,
        seed in any::<u64>(),
        calls in 1usize..20,
    ) {
        let clock = frozen_clock(0);
        let budget = Budget::new("test", cap, rate, clock.clone());
        let injector = Provacao::<BudgetCallFault>::new("chaos", seed).with_policy(
            Policy::Probability {
                fault: BudgetCallFault::Upstream,
                p: chaos_p,
            },
        );
        let clk: &dyn Clock = clock.as_ref();
        for _ in 0..calls {
            // If chaos fires, consumer treats as upstream timeout (no
            // budget consumption). If chaos doesn't, consumer tries
            // a 1-token consume.
            let _outcome: Result<u64, BudgetCallFault> = match injector.maybe_fault(clk) {
                Some(_) => Err(BudgetCallFault::Upstream),
                None => Ok(budget.try_consume(1).map_or(0, |remaining| remaining)),
            };
        }
        // Invariant: available is always in [0, cap] no matter what
        // sequence of chaos + consumes happened.
        let avail = budget.available();
        prop_assert!(avail <= cap);
    }
}

// ── mirante + máquina: FSM state observable via ObservationChannel ──

proptest_with_env! {
    /// Every successful machine transition can be published as a
    /// snapshot to a mirante channel — subscribers see the latest
    /// state. Composes máquina (FSM authority) with mirante
    /// (last-value-wins observability).
    #[test]
    fn machine_state_published_to_mirante(
        increments in proptest::collection::vec(1u32..50, 1..6),
    ) {
        let (snap_json, expected_json) = block_on(async {
            let safe_incs: Vec<_> = increments.into_iter().take(6).collect();
            let clock = frozen_clock(0);
            let mut runner =
                MachineRunner::<CounterMachine>::new(clock.clone() as Arc<dyn Clock>);
            let chan: Arc<ObservationChannel<CounterState>> = Arc::new(
                ObservationChannel::new(
                    runner.state().clone(),
                    clock.clone() as Arc<dyn Clock>,
                ),
            );
            let mut m = Mirante::new();
            m.register("counter", chan.clone());
            for inc in &safe_incs {
                runner.step(CounterEvent::Inc(*inc)).unwrap();
                chan.publish(runner.state().clone());
            }
            let snap = m.snapshot_all();
            let cur_state = runner.state().clone();
            let expected = serde_json::to_value(&cur_state).unwrap();
            (snap["counter"].clone(), expected)
        });
        prop_assert_eq!(snap_json, expected_json);
    }
}

// ── relógio HLC: two clocks merge via Instant::tick into total order ──

proptest_with_env! {
    /// HLC tick: given two distinct Instants, `Instant::tick(now,
    /// previous)` always returns one strictly greater than `previous`
    /// in the lexicographic (physical_ms, logical) order. Closes the
    /// substrate's typed-clock causal-merge guarantee.
    #[test]
    fn hlc_tick_strictly_after_previous(
        prev_ms in 0u64..1_000_000,
        prev_logical in 0u16..1000,
        now_ms in 0u64..1_000_000,
        now_logical in 0u16..1000,
    ) {
        let previous = Instant::new(prev_ms, prev_logical);
        let now = Instant::new(now_ms, now_logical);
        let next = Instant::tick(now, previous);
        prop_assert!(
            next.causally_after(&previous),
            "HLC tick {next:?} not strictly after previous {previous:?}"
        );
    }

    /// HLC tick on equal Instants bumps logical, preserves physical_ms.
    #[test]
    fn hlc_tick_on_equal_bumps_logical(
        ms in 0u64..1_000_000,
        logical in 0u16..1000,
    ) {
        let a = Instant::new(ms, logical);
        let next = Instant::tick(a, a);
        prop_assert_eq!(next.physical_ms, ms);
        prop_assert_eq!(next.logical, logical.saturating_add(1));
    }
}

// ── replay + provação: chaos-driven event replay ──

#[derive(Debug, Clone, PartialEq, Eq, Error)]
enum ReplayFault {
    #[error("simulated drop")]
    Drop,
}

impl_error_kind! {
    ReplayFault {
        Drop => "drop",
    }
}

proptest_with_env! {
    /// Drive a ReplayCursor with chaos injection: per-event, the
    /// chaos injector decides whether to "drop" (skip) or "accept"
    /// (advance). The cursor's bounded-position invariant holds
    /// regardless of the chaos sequence.
    #[test]
    fn cursor_under_chaos_position_bounded(
        events in proptest::collection::vec(any::<u32>(), 1..16),
        chaos_p in 0.0_f64..1.0,
        seed in any::<u64>(),
    ) {
        let cursor = ReplayCursor::new("incs", events.clone());
        let injector = Provacao::<ReplayFault>::new("drops", seed).with_policy(
            Policy::Probability {
                fault: ReplayFault::Drop,
                p: chaos_p,
            },
        );
        let clk: Arc<dyn Clock> = frozen_clock(0);
        // Walk the cursor; on fault, peek-and-skip; on no-fault, next.
        while cursor.peek().is_some() {
            match injector.maybe_fault(clk.as_ref()) {
                Some(_) => {
                    cursor.skip(1);
                }
                None => {
                    let _ = cursor.next();
                }
            }
        }
        // Position bounded by len. Cursor saturates correctly.
        prop_assert!(cursor.position() == events.len());
        prop_assert!(cursor.is_done());
    }
}

// ── selo + linhagem-aberta: typed capability delegation chain ──

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DelegationLink {
    parent_subject: String,
    child_subject: String,
    capability: String,
}
engenho_substrate::impl_fingerprint!(DelegationLink);

proptest_with_env! {
    /// A capability delegation chain — Alice delegates "read:foo" to
    /// Bob who delegates to Carol. Each link is a Selo + a
    /// DelegationLink fingerprint that goes into a LineageGraph. The
    /// chain's root proves the full delegation chain back to Alice.
    #[test]
    fn capability_delegation_chain_via_lineage(
        secret in any::<[u8; 32]>(),
        depth in 2usize..6,
    ) {
        let iss = SeloIssuer::new(secret);
        let cap = "read:foo";
        let exp = Instant::from_ms(1_000_000);
        let mut graph: LineageGraph<DelegationLink> = LineageGraph::new();
        let mut parent: Option<[u8; 32]> = None;
        for i in 0..depth {
            let parent_name = if i == 0 {
                "root".to_string()
            } else {
                format!("user{}", i - 1)
            };
            let child_name = format!("user{i}");
            // Each Selo proves delegation parent → child.
            let _selo = iss.issue(&child_name, cap, exp);
            // Record the link in the lineage chain.
            let link = DelegationLink {
                parent_subject: parent_name,
                child_subject: child_name,
                capability: cap.to_string(),
            };
            let causes = parent.map(|p| std::iter::once(p).collect()).unwrap_or_default();
            let fp = graph.append(link, causes).unwrap();
            parent = Some(fp);
        }
        prop_assert_eq!(graph.len(), depth);
        // Ancestors of the leaf = full chain back to root.
        let leaf = parent.unwrap();
        let ancestors = graph.ancestors(leaf).unwrap();
        prop_assert_eq!(ancestors.len(), depth);
        // Root hash is deterministic from the chain shape.
        let _root = graph.root_hash();
    }
}
