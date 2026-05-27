//! `TopologyNodeMachine` — SM ③, the per-node formation lifecycle.
//!
//! Mirrors `engenho-revoada::topology::NodeState` (which is driven by
//! a `TopologyReactor`, not yet a `maquina` FSM). Formalizing it here
//! gives the per-node lifecycle free transition history + replay +
//! observability, and a typescape registration so a node's state can
//! be authored / attested / observed uniformly.
//!
//! ```text
//! Joining ─Admit▶ Standby ─Promote(role)▶ Active(role) ─Demote▶ Demoting ─SettleDemotion▶ Standby
//!   Active(_) ─Reassign(role)▶ Active(role)
//!   {Joining|Standby|Active|Demoting} ─Depart▶ Departing
//!   {…|Departing} ─Fail▶ Failed
//!   {Departing|Failed} ─Evict▶ Evicted        (terminal)
//! ```

use engenho_sui_typescape::{Typescape, TypescapeError, TypescapeValue};
use serde::{Deserialize, Serialize};

/// Role a node may hold (mirror of `engenho_revoada::topology::Role`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    /// Voting Raft member; reads + writes.
    Master,
    /// Workload-capable; not in the Raft quorum.
    Worker,
    /// Initial seed before quorum forms.
    Bootstrap,
    /// Non-voting peer (Raft learner).
    Observer,
}

/// Per-node lifecycle state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopologyNodeState {
    /// Just came up; gossiping presence, no role yet.
    Joining,
    /// Healthy peer; no role assigned yet.
    Standby,
    /// Holds an active role.
    Active(NodeRole),
    /// Transitioning out of an active role (writes blocked).
    Demoting,
    /// Voluntarily leaving the cluster.
    Departing,
    /// Phi-accrual flagged; eligible for replacement.
    Failed,
    /// Removed from the assignment — terminal.
    Evicted,
}

/// Events driving the per-node lifecycle.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TopologyNodeEvent {
    /// New node enters `Standby`.
    Admit,
    /// `Standby` → `Active(role)`.
    Promote(NodeRole),
    /// `Active(_)` → `Active(role)` (rebalance).
    Reassign(NodeRole),
    /// `Active(_)` → `Demoting`.
    Demote,
    /// `Demoting` → `Standby` (the timed settle).
    SettleDemotion,
    /// Begin a voluntary leave.
    Depart,
    /// Phi-accrual flags the node failed.
    Fail,
    /// Remove a `Departing`/`Failed` node entirely.
    Evict,
}

/// Side-effects of a per-node transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TopologyNodeEffect {
    /// Transitioned to a named milestone (telemetry tag).
    Transitioned(&'static str),
    /// A failure was observed.
    Fault,
}

/// Step failures for the topology-node machine.
#[derive(Clone, Debug, thiserror::Error)]
pub enum TopologyNodeError {
    /// The event is not valid for the current state.
    #[error("invalid topology-node transition from {state} on {event}")]
    InvalidTransition {
        /// Debug-rendered source state.
        state: String,
        /// Debug-rendered event.
        event: String,
    },
}

engenho_substrate::impl_error_kind! {
    TopologyNodeError {
        { InvalidTransition { .. } } => "invalid_transition",
    }
}

/// The per-node formation state machine (SM ③).
#[derive(Debug, Default, Clone, Copy)]
pub struct TopologyNodeMachine;

engenho_substrate::define_named!(TopologyNodeMachine, "topology-node");

impl engenho_substrate::StateMachine for TopologyNodeMachine {
    type State = TopologyNodeState;
    type Event = TopologyNodeEvent;
    type Effect = TopologyNodeEffect;
    type Err = TopologyNodeError;

    fn initial() -> Self::State {
        TopologyNodeState::Joining
    }

    fn step(
        state: &Self::State,
        event: &Self::Event,
    ) -> Result<(Self::State, Self::Effect), Self::Err> {
        use TopologyNodeEffect as Fx;
        use TopologyNodeEvent as E;
        use TopologyNodeState as S;

        let next = match (state, event) {
            (S::Joining, E::Admit) => (S::Standby, Fx::Transitioned("admitted")),
            (S::Standby, E::Promote(r)) => (S::Active(*r), Fx::Transitioned("promoted")),
            (S::Active(_), E::Reassign(r)) => (S::Active(*r), Fx::Transitioned("reassigned")),
            (S::Active(_), E::Demote) => (S::Demoting, Fx::Transitioned("demoting")),
            (S::Demoting, E::SettleDemotion) => (S::Standby, Fx::Transitioned("demoted")),
            (S::Joining | S::Standby | S::Active(_) | S::Demoting, E::Depart) => {
                (S::Departing, Fx::Transitioned("departing"))
            }
            (S::Joining | S::Standby | S::Active(_) | S::Demoting | S::Departing, E::Fail) => {
                (S::Failed, Fx::Fault)
            }
            (S::Departing | S::Failed, E::Evict) => (S::Evicted, Fx::Transitioned("evicted")),
            (s, e) => {
                return Err(TopologyNodeError::InvalidTransition {
                    state: format!("{s:?}"),
                    event: format!("{e:?}"),
                });
            }
        };
        Ok(next)
    }

    fn is_terminal(state: &Self::State) -> bool {
        matches!(state, TopologyNodeState::Evicted)
    }
}

// ── Typescape registration ───────────────────────────────────────

fn role_to_typescape(role: &NodeRole) -> TypescapeValue {
    TypescapeValue::string(match role {
        NodeRole::Master => "master",
        NodeRole::Worker => "worker",
        NodeRole::Bootstrap => "bootstrap",
        NodeRole::Observer => "observer",
    })
}

fn role_from_typescape(v: &TypescapeValue) -> Result<NodeRole, TypescapeError> {
    Ok(match v.as_str()? {
        "master" => NodeRole::Master,
        "worker" => NodeRole::Worker,
        "bootstrap" => NodeRole::Bootstrap,
        "observer" => NodeRole::Observer,
        other => {
            return Err(TypescapeError::Invariant {
                location: "NodeRole".into(),
                reason: format!("unknown role: {other}"),
            });
        }
    })
}

fn unit(tag: &str) -> TypescapeValue {
    TypescapeValue::attrs([("state", TypescapeValue::string(tag))])
}

impl Typescape for TopologyNodeState {
    fn to_typescape_value(&self) -> TypescapeValue {
        match self {
            Self::Joining => unit("joining"),
            Self::Standby => unit("standby"),
            Self::Active(role) => TypescapeValue::attrs([
                ("state", TypescapeValue::string("active")),
                ("role", role_to_typescape(role)),
            ]),
            Self::Demoting => unit("demoting"),
            Self::Departing => unit("departing"),
            Self::Failed => unit("failed"),
            Self::Evicted => unit("evicted"),
        }
    }

    fn from_typescape_value(value: &TypescapeValue) -> Result<Self, TypescapeError> {
        let tag = value.attr("state")?.as_str()?;
        Ok(match tag {
            "joining" => Self::Joining,
            "standby" => Self::Standby,
            "active" => Self::Active(role_from_typescape(value.attr("role")?)?),
            "demoting" => Self::Demoting,
            "departing" => Self::Departing,
            "failed" => Self::Failed,
            "evicted" => Self::Evicted,
            other => {
                return Err(TypescapeError::Invariant {
                    location: "TopologyNodeState".into(),
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

    fn runner() -> MachineRunner<TopologyNodeMachine> {
        MachineRunner::new(Arc::new(FrozenClock::at(0)))
    }

    #[test]
    fn promote_demote_settle_cycle() {
        let mut r = runner();
        r.step(TopologyNodeEvent::Admit).unwrap();
        assert_eq!(r.state(), &TopologyNodeState::Standby);
        r.step(TopologyNodeEvent::Promote(NodeRole::Master))
            .unwrap();
        assert_eq!(r.state(), &TopologyNodeState::Active(NodeRole::Master));
        r.step(TopologyNodeEvent::Demote).unwrap();
        assert_eq!(r.state(), &TopologyNodeState::Demoting);
        r.step(TopologyNodeEvent::SettleDemotion).unwrap();
        assert_eq!(r.state(), &TopologyNodeState::Standby);
    }

    #[test]
    fn reassign_changes_role_in_place() {
        let mut r = runner();
        r.step(TopologyNodeEvent::Admit).unwrap();
        r.step(TopologyNodeEvent::Promote(NodeRole::Worker))
            .unwrap();
        r.step(TopologyNodeEvent::Reassign(NodeRole::Master))
            .unwrap();
        assert_eq!(r.state(), &TopologyNodeState::Active(NodeRole::Master));
    }

    #[test]
    fn fail_then_evict_is_terminal() {
        let mut r = runner();
        r.step(TopologyNodeEvent::Admit).unwrap();
        r.step(TopologyNodeEvent::Promote(NodeRole::Worker))
            .unwrap();
        let fx = r.step(TopologyNodeEvent::Fail).unwrap();
        assert_eq!(fx, TopologyNodeEffect::Fault);
        assert_eq!(r.state(), &TopologyNodeState::Failed);
        r.step(TopologyNodeEvent::Evict).unwrap();
        assert!(r.is_terminal());
        assert!(r.step(TopologyNodeEvent::Admit).is_err());
    }

    #[test]
    fn invalid_transition_preserves_state() {
        // Promote before Admit is invalid (still Joining).
        let inner = <TopologyNodeMachine as StateMachine>::step(
            &TopologyNodeState::Joining,
            &TopologyNodeEvent::Promote(NodeRole::Master),
        )
        .unwrap_err();
        assert_eq!(inner.kind(), "invalid_transition");
        let mut r = runner();
        assert!(
            r.step(TopologyNodeEvent::Promote(NodeRole::Master))
                .is_err()
        );
        assert_eq!(r.state(), &TopologyNodeState::Joining);
    }

    #[test]
    fn state_typescape_round_trips() {
        for s in [
            TopologyNodeState::Joining,
            TopologyNodeState::Standby,
            TopologyNodeState::Active(NodeRole::Master),
            TopologyNodeState::Active(NodeRole::Observer),
            TopologyNodeState::Demoting,
            TopologyNodeState::Departing,
            TopologyNodeState::Failed,
            TopologyNodeState::Evicted,
        ] {
            let tv = s.to_typescape_value();
            let back = TopologyNodeState::from_typescape_value(&tv).unwrap();
            assert_eq!(back, s);
        }
    }
}
