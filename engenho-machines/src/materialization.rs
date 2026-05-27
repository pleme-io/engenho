//! `MaterializationMachine` — SM ⑥, the substrate→ether derivation
//! lifecycle.
//!
//! A `Drv` becomes a content-addressed [`WorkloadShape`] that the
//! fabric distributes, but only after a K-of-N quorum of *independent
//! rebuilds* agrees on the evidence hash. This machine encodes that
//! lifecycle as a pure [`engenho_substrate::StateMachine`]:
//!
//! ```text
//! Defined ─Hash▶ Hashed ─Build▶ Built ─Realise▶ Realised ─Commit▶ StoreCommitted
//!   ─EmitReceipt{threshold}▶ ReceiptEmitted{threshold}
//!   ─Confirm{agrees}▶ { QuorumPending{confirmed,threshold} | QuorumReached | Dissent }
//!   QuorumReached ─Render{shape}▶ Rendered{shape} ─Distribute▶ Distributed ─Consume▶ Terminal
//! ```
//!
//! `Dissent` and `Terminal` are terminal. See `docs/STATE-MACHINES.md`
//! §⑥ for the prose.

use engenho_substrate::WorkloadShape;
use engenho_sui_typescape::{Typescape, TypescapeError, TypescapeValue};
use serde::{Deserialize, Serialize};

use crate::shape_ts::{workload_shape_from_typescape, workload_shape_to_typescape};

/// The lifecycle states of a derivation moving from definition to a
/// distributed, consumable artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MaterializationState {
    /// `Drv` constructed; not yet hashed.
    Defined,
    /// `DrvHash` computed (BLAKE3 over canonical ATerm).
    Hashed,
    /// Builder ran to completion; outputs staged.
    Built,
    /// `Realisation` recorded (outputs → store paths + NAR hash).
    Realised,
    /// Realisation persisted to the derivation cache backend.
    StoreCommitted,
    /// A `MaterializationReceipt` was emitted; awaiting confirmations.
    /// Carries the quorum `threshold` (K of N).
    ReceiptEmitted {
        /// Distinct-emitter confirmations required for quorum.
        threshold: u32,
    },
    /// Confirmations accumulating; not yet at threshold.
    QuorumPending {
        /// Distinct emitters that have confirmed agreeing evidence.
        confirmed: u32,
        /// Confirmations required to reach quorum.
        threshold: u32,
    },
    /// Threshold reached with all evidence agreeing.
    QuorumReached,
    /// Rendered into a concrete shape, ready to ship.
    Rendered {
        /// The shape the `Drv` was rendered into.
        shape: WorkloadShape,
    },
    /// Distributed to the nodes that need it (ledger / gossip).
    Distributed,
    /// Available for consumption — terminal success.
    Terminal,
    /// Evidence disagreement at quorum — terminal hard fault.
    Dissent,
}

/// Events driving the materialization lifecycle.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MaterializationEvent {
    /// Compute the `DrvHash`.
    Hash,
    /// Run the builder.
    Build,
    /// Record the `Realisation`.
    Realise,
    /// Persist to the cache backend.
    Commit,
    /// Emit the receipt and open the quorum window.
    EmitReceipt {
        /// Confirmations required for quorum.
        threshold: u32,
    },
    /// A node confirms it independently rebuilt the artifact.
    Confirm {
        /// Whether this node's evidence hash agrees with the rest.
        evidence_agrees: bool,
    },
    /// Render the realised output into a concrete shape.
    Render {
        /// Target shape.
        shape: WorkloadShape,
    },
    /// Distribute the rendered artifact across the fabric.
    Distribute,
    /// A downstream consumer takes the artifact.
    Consume,
}

/// Side-effects a step requests the runner's consumer to perform.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MaterializationEffect {
    /// Advanced to a named milestone (telemetry tag).
    Advanced(&'static str),
    /// Quorum progressed; surface for dashboards.
    QuorumProgress {
        /// Confirmations so far.
        confirmed: u32,
        /// Threshold required.
        threshold: u32,
    },
    /// Rendered into the given shape.
    Rendered(WorkloadShape),
    /// A hard fault (evidence dissent) was raised.
    FaultRaised,
}

/// Step failures for the materialization machine.
#[derive(Clone, Debug, thiserror::Error)]
pub enum MaterializationError {
    /// The event is not valid for the current state.
    #[error("invalid materialization transition from {state} on {event}")]
    InvalidTransition {
        /// Debug-rendered source state.
        state: String,
        /// Debug-rendered event.
        event: String,
    },
}

engenho_substrate::impl_error_kind! {
    MaterializationError {
        { InvalidTransition { .. } } => "invalid_transition",
    }
}

/// The materialization state machine (SM ⑥).
#[derive(Debug, Default, Clone, Copy)]
pub struct MaterializationMachine;

engenho_substrate::define_named!(MaterializationMachine, "materialization");

/// Apply one confirmation, returning the resulting state + effect.
fn confirm(
    confirmed: u32,
    threshold: u32,
    agrees: bool,
) -> (MaterializationState, MaterializationEffect) {
    if !agrees {
        return (
            MaterializationState::Dissent,
            MaterializationEffect::FaultRaised,
        );
    }
    let c = confirmed.saturating_add(1);
    if c >= threshold {
        (
            MaterializationState::QuorumReached,
            MaterializationEffect::Advanced("quorum_reached"),
        )
    } else {
        (
            MaterializationState::QuorumPending {
                confirmed: c,
                threshold,
            },
            MaterializationEffect::QuorumProgress {
                confirmed: c,
                threshold,
            },
        )
    }
}

impl engenho_substrate::StateMachine for MaterializationMachine {
    type State = MaterializationState;
    type Event = MaterializationEvent;
    type Effect = MaterializationEffect;
    type Err = MaterializationError;

    fn initial() -> Self::State {
        MaterializationState::Defined
    }

    fn step(
        state: &Self::State,
        event: &Self::Event,
    ) -> Result<(Self::State, Self::Effect), Self::Err> {
        use MaterializationEffect as Fx;
        use MaterializationEvent as E;
        use MaterializationState as S;

        let next = match (state, event) {
            (S::Defined, E::Hash) => (S::Hashed, Fx::Advanced("hashed")),
            (S::Hashed, E::Build) => (S::Built, Fx::Advanced("built")),
            (S::Built, E::Realise) => (S::Realised, Fx::Advanced("realised")),
            (S::Realised, E::Commit) => (S::StoreCommitted, Fx::Advanced("store_committed")),
            (S::StoreCommitted, E::EmitReceipt { threshold }) => (
                S::ReceiptEmitted {
                    threshold: *threshold,
                },
                Fx::QuorumProgress {
                    confirmed: 0,
                    threshold: *threshold,
                },
            ),
            (S::ReceiptEmitted { threshold }, E::Confirm { evidence_agrees }) => {
                confirm(0, *threshold, *evidence_agrees)
            }
            (
                S::QuorumPending {
                    confirmed,
                    threshold,
                },
                E::Confirm { evidence_agrees },
            ) => confirm(*confirmed, *threshold, *evidence_agrees),
            (S::QuorumReached, E::Render { shape }) => (
                S::Rendered {
                    shape: shape.clone(),
                },
                Fx::Rendered(shape.clone()),
            ),
            (S::Rendered { .. }, E::Distribute) => (S::Distributed, Fx::Advanced("distributed")),
            (S::Distributed, E::Consume) => (S::Terminal, Fx::Advanced("terminal")),
            (s, e) => {
                return Err(MaterializationError::InvalidTransition {
                    state: format!("{s:?}"),
                    event: format!("{e:?}"),
                });
            }
        };
        Ok(next)
    }

    fn is_terminal(state: &Self::State) -> bool {
        matches!(
            state,
            MaterializationState::Terminal | MaterializationState::Dissent
        )
    }
}

// ── Typescape registration for the FSM state ─────────────────────

fn unit(tag: &str) -> TypescapeValue {
    TypescapeValue::attrs([("state", TypescapeValue::string(tag))])
}

fn read_u32(v: &TypescapeValue, key: &str) -> Result<u32, TypescapeError> {
    let i = v.attr(key)?.as_int()?;
    u32::try_from(i).map_err(|_| TypescapeError::Invariant {
        location: format!("MaterializationState.{key}"),
        reason: format!("expected non-negative u32, got {i}"),
    })
}

impl Typescape for MaterializationState {
    fn to_typescape_value(&self) -> TypescapeValue {
        match self {
            Self::Defined => unit("defined"),
            Self::Hashed => unit("hashed"),
            Self::Built => unit("built"),
            Self::Realised => unit("realised"),
            Self::StoreCommitted => unit("store_committed"),
            Self::ReceiptEmitted { threshold } => TypescapeValue::attrs([
                ("state", TypescapeValue::string("receipt_emitted")),
                ("threshold", TypescapeValue::int(i64::from(*threshold))),
            ]),
            Self::QuorumPending {
                confirmed,
                threshold,
            } => TypescapeValue::attrs([
                ("state", TypescapeValue::string("quorum_pending")),
                ("confirmed", TypescapeValue::int(i64::from(*confirmed))),
                ("threshold", TypescapeValue::int(i64::from(*threshold))),
            ]),
            Self::QuorumReached => unit("quorum_reached"),
            Self::Rendered { shape } => TypescapeValue::attrs([
                ("state", TypescapeValue::string("rendered")),
                ("shape", workload_shape_to_typescape(shape)),
            ]),
            Self::Distributed => unit("distributed"),
            Self::Terminal => unit("terminal"),
            Self::Dissent => unit("dissent"),
        }
    }

    fn from_typescape_value(value: &TypescapeValue) -> Result<Self, TypescapeError> {
        let tag = value.attr("state")?.as_str()?;
        Ok(match tag {
            "defined" => Self::Defined,
            "hashed" => Self::Hashed,
            "built" => Self::Built,
            "realised" => Self::Realised,
            "store_committed" => Self::StoreCommitted,
            "receipt_emitted" => Self::ReceiptEmitted {
                threshold: read_u32(value, "threshold")?,
            },
            "quorum_pending" => Self::QuorumPending {
                confirmed: read_u32(value, "confirmed")?,
                threshold: read_u32(value, "threshold")?,
            },
            "quorum_reached" => Self::QuorumReached,
            "rendered" => Self::Rendered {
                shape: workload_shape_from_typescape(value.attr("shape")?)?,
            },
            "distributed" => Self::Distributed,
            "terminal" => Self::Terminal,
            "dissent" => Self::Dissent,
            other => {
                return Err(TypescapeError::Invariant {
                    location: "MaterializationState".into(),
                    reason: format!("unknown state tag: {other}"),
                });
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_substrate::{FrozenClock, MachineRunner, StateMachine};
    use std::sync::Arc;

    fn runner() -> MachineRunner<MaterializationMachine> {
        MachineRunner::new(Arc::new(FrozenClock::at(0)))
    }

    #[test]
    fn happy_path_reaches_terminal() {
        let mut r = runner();
        r.step(MaterializationEvent::Hash).unwrap();
        r.step(MaterializationEvent::Build).unwrap();
        r.step(MaterializationEvent::Realise).unwrap();
        r.step(MaterializationEvent::Commit).unwrap();
        r.step(MaterializationEvent::EmitReceipt { threshold: 2 })
            .unwrap();
        // First confirm → pending(1/2).
        r.step(MaterializationEvent::Confirm {
            evidence_agrees: true,
        })
        .unwrap();
        assert_eq!(
            r.state(),
            &MaterializationState::QuorumPending {
                confirmed: 1,
                threshold: 2
            }
        );
        // Second confirm → reached.
        r.step(MaterializationEvent::Confirm {
            evidence_agrees: true,
        })
        .unwrap();
        assert_eq!(r.state(), &MaterializationState::QuorumReached);
        r.step(MaterializationEvent::Render {
            shape: WorkloadShape::OciImage,
        })
        .unwrap();
        r.step(MaterializationEvent::Distribute).unwrap();
        let fx = r.step(MaterializationEvent::Consume).unwrap();
        assert_eq!(fx, MaterializationEffect::Advanced("terminal"));
        assert!(r.is_terminal());
    }

    #[test]
    fn dissent_is_terminal_hard_fault() {
        let mut r = runner();
        for e in [
            MaterializationEvent::Hash,
            MaterializationEvent::Build,
            MaterializationEvent::Realise,
            MaterializationEvent::Commit,
            MaterializationEvent::EmitReceipt { threshold: 3 },
        ] {
            r.step(e).unwrap();
        }
        let fx = r
            .step(MaterializationEvent::Confirm {
                evidence_agrees: false,
            })
            .unwrap();
        assert_eq!(fx, MaterializationEffect::FaultRaised);
        assert_eq!(r.state(), &MaterializationState::Dissent);
        assert!(r.is_terminal());
        // Cannot step from terminal.
        assert!(r.step(MaterializationEvent::Distribute).is_err());
    }

    #[test]
    fn invalid_transition_errors_and_preserves_state() {
        // The machine's own typed error carries the kind (the runner
        // wraps it as MachineError::Step, whose kind is "step").
        let inner = <MaterializationMachine as StateMachine>::step(
            &MaterializationState::Defined,
            &MaterializationEvent::Build,
        )
        .unwrap_err();
        assert_eq!(inner.kind(), "invalid_transition");
        // Driven through the runner, an invalid step is rejected and
        // leaves state + history untouched.
        let mut r = runner();
        assert!(r.step(MaterializationEvent::Build).is_err());
        assert_eq!(r.state(), &MaterializationState::Defined);
        assert_eq!(r.step_count(), 0);
    }

    #[test]
    fn state_typescape_round_trips() {
        let cases = [
            MaterializationState::Defined,
            MaterializationState::ReceiptEmitted { threshold: 5 },
            MaterializationState::QuorumPending {
                confirmed: 2,
                threshold: 5,
            },
            MaterializationState::Rendered {
                shape: WorkloadShape::OciImage,
            },
            MaterializationState::Rendered {
                shape: WorkloadShape::StaticBinary {
                    triple: "aarch64-unknown-linux-musl".into(),
                },
            },
            MaterializationState::Dissent,
        ];
        for s in cases {
            let tv = s.to_typescape_value();
            let back = MaterializationState::from_typescape_value(&tv).unwrap();
            assert_eq!(back, s);
        }
    }
}
