//! Which world the daemon acts on.
//!
//! Every role the daemon wires is either a stand-in that lives in this
//! process's memory or the real subsystem it names. The daemon as a
//! whole is only [`Universe::Real`] when every slot is; one stand-in
//! makes the whole run [`Universe::Mock`]. The universe is read off the
//! concrete types `wire` constructs (through [`Grounded`]), so swapping
//! a mock for a real impl changes the resolved universe without anyone
//! editing a second list, and wiring a type with no declared universe
//! does not compile (E0277).

use engenho_fonte::{
    MockAnomalyChain, MockAnomalyHandler, MockAppReconciler, MockAttester, MockInfraReconciler,
    MockPromessaReconciler, MockPublisher, MockTopologyReconciler, ShikumiWatcher, SuiEvaluator,
};
use engenho_revoada::PureRaftFace;
use std::fmt;

/// Which world one role, or the whole daemon, acts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Universe {
    /// An in-process stand-in. What it records lives in this process's
    /// memory, is gone when the process exits, and nothing outside the
    /// process observes it.
    Mock,
    /// Backed by the subsystem it names: a file on disk, the sui
    /// evaluator, a cluster, a durable chain.
    Real,
}

impl Universe {
    /// Resolve a composite. It is `Real` only when it has at least one
    /// part and every part is `Real`; a single `Mock` part, or no part
    /// at all, resolves to `Mock`. Nothing is claimed real without a
    /// part that is.
    pub fn resolve(parts: impl IntoIterator<Item = Universe>) -> Universe {
        let mut witnessed = false;
        for part in parts {
            match part {
                Universe::Mock => return Universe::Mock,
                Universe::Real => witnessed = true,
            }
        }
        if witnessed {
            Universe::Real
        } else {
            Universe::Mock
        }
    }
}

impl fmt::Display for Universe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Universe::Mock => "mock",
            Universe::Real => "real",
        })
    }
}

/// A role type whose universe is a property of the type itself.
pub trait Grounded {
    /// The universe every value of this type acts on.
    const UNIVERSE: Universe;
}

/// The universe of `value`, read off its type.
pub fn of<T: Grounded + ?Sized>(_value: &T) -> Universe {
    T::UNIVERSE
}

/// Declare the universe of each role type the daemon can wire.
macro_rules! grounded {
    ($universe:ident: $($ty:ty),+ $(,)?) => {
        $(impl Grounded for $ty {
            const UNIVERSE: Universe = Universe::$universe;
        })+
    };
}

// Reads the declaration file from disk, and evaluates it with sui.
grounded!(Real: ShikumiWatcher, SuiEvaluator);

// Each records into a Vec, or (PureRaftFace) an in-memory map that a
// restart discards.
grounded!(
    Mock: MockAppReconciler,
    MockInfraReconciler,
    MockPromessaReconciler,
    MockTopologyReconciler,
    MockAttester,
    MockPublisher,
    MockAnomalyChain,
    MockAnomalyHandler,
    PureRaftFace,
);

/// The universe resolved for each slot the daemon wires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Wiring {
    /// Surfaces changes to the declaration.
    pub watcher: Universe,
    /// Types the declaration.
    pub evaluator: Universe,
    /// Takes the typed declaration and acts on it.
    pub proposer: Universe,
    /// Receipts each transition.
    pub attester: Universe,
    /// Broadcasts each outcome.
    pub publisher: Universe,
    /// Records drift between successive declarations.
    pub anomaly_chain: Universe,
    /// Receives each routed anomaly.
    pub remediation: Universe,
}

impl Wiring {
    /// The daemon's universe: `Real` only when every slot is.
    pub fn universe(&self) -> Universe {
        // No `..`: a new slot is E0027 here until it is folded in.
        let Self {
            watcher,
            evaluator,
            proposer,
            attester,
            publisher,
            anomaly_chain,
            remediation,
        } = *self;
        Universe::resolve([
            watcher,
            evaluator,
            proposer,
            attester,
            publisher,
            anomaly_chain,
            remediation,
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_REAL: Wiring = Wiring {
        watcher: Universe::Real,
        evaluator: Universe::Real,
        proposer: Universe::Real,
        attester: Universe::Real,
        publisher: Universe::Real,
        anomaly_chain: Universe::Real,
        remediation: Universe::Real,
    };

    #[test]
    fn one_mock_part_makes_the_whole_mock() {
        assert_eq!(
            Universe::resolve([Universe::Real, Universe::Mock, Universe::Real]),
            Universe::Mock
        );
        assert_eq!(Universe::resolve([Universe::Mock]), Universe::Mock);
    }

    #[test]
    fn every_part_real_resolves_real() {
        assert_eq!(
            Universe::resolve([Universe::Real, Universe::Real]),
            Universe::Real
        );
    }

    #[test]
    fn no_parts_is_not_claimed_real() {
        assert_eq!(Universe::resolve([]), Universe::Mock);
    }

    #[test]
    fn a_wiring_is_real_only_when_every_slot_is() {
        assert_eq!(ALL_REAL.universe(), Universe::Real);
        // Flip each slot to Mock in turn: each one alone makes it Mock.
        let flips: [fn(&mut Wiring); 7] = [
            |w| w.watcher = Universe::Mock,
            |w| w.evaluator = Universe::Mock,
            |w| w.proposer = Universe::Mock,
            |w| w.attester = Universe::Mock,
            |w| w.publisher = Universe::Mock,
            |w| w.anomaly_chain = Universe::Mock,
            |w| w.remediation = Universe::Mock,
        ];
        for (slot, flip) in flips.iter().enumerate() {
            let mut w = ALL_REAL;
            flip(&mut w);
            assert_eq!(w.universe(), Universe::Mock, "slot {slot} flipped to mock");
        }
    }

    #[test]
    fn mock_role_types_are_grounded_mock() {
        assert_eq!(of(&MockAttester::new()), Universe::Mock);
        assert_eq!(of(&MockPublisher::new()), Universe::Mock);
        assert_eq!(of(&MockAppReconciler::new()), Universe::Mock);
        assert_eq!(of(&MockAnomalyHandler::new()), Universe::Mock);
    }

    #[test]
    fn display_is_the_lowercase_name() {
        assert_eq!(Universe::Mock.to_string(), "mock");
        assert_eq!(Universe::Real.to_string(), "real");
    }
}
