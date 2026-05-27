//! # engenho-machines
//!
//! The documented engenho state machines ([`docs/STATE-MACHINES.md`])
//! lifted into typed [`engenho_substrate::StateMachine`] impls, plus
//! [`engenho_sui_typescape::Typescape`] registrations so each FSM
//! state can cross the sui bridge (be authored in a `(defsistema …)`
//! form, reconciled, attested, observed).
//!
//! Substrate-first (the deepest follow-on axis): the flagship machine
//! is [`materialization::MaterializationMachine`] — the
//! derivation→ether lifecycle (SM ⑥) — which renders a `Drv` into a
//! [`engenho_substrate::WorkloadShape`] and reaches a quorum of
//! independent rebuilds before distribution. [`topology_node`]
//! formalizes the per-node formation lifecycle (SM ③) that
//! `engenho-revoada::topology` drives via its reactor.
//!
//! Every machine here is `pure`: `step(&state, &event) -> (state,
//! effect)` with no I/O. Drive one with
//! [`engenho_substrate::MachineRunner`] to get free transition
//! history + replay + `mirante::Observable` snapshots.
//!
//! [`docs/STATE-MACHINES.md`]: https://github.com/pleme-io/engenho/blob/main/docs/STATE-MACHINES.md

#![warn(missing_docs)]

pub mod materialization;
pub mod shape_ts;
pub mod topology_node;

pub use materialization::{
    MaterializationEffect, MaterializationError, MaterializationEvent, MaterializationMachine,
    MaterializationState,
};
pub use shape_ts::{ShapeTs, workload_shape_from_typescape, workload_shape_to_typescape};
pub use topology_node::{
    NodeRole, TopologyNodeEffect, TopologyNodeError, TopologyNodeEvent, TopologyNodeMachine,
    TopologyNodeState,
};
