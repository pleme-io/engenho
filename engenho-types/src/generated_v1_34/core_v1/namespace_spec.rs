//! Typed `NamespaceSpec` + `NamespaceStatus` — M0.0.2 #5.
//!
//! Namespace is the first **cluster-scoped** kind to land typed
//! expansion. Tests that engenho-kube-client correctly routes
//! `/api/v1/namespaces` (no namespace path component) vs the
//! Pod/Service/ConfigMap/Deployment path which all carry a
//! namespace segment.

#![allow(clippy::module_name_repetitions)]

use serde::{Deserialize, Serialize};

/// `NamespaceSpec` describes the attributes on a Namespace.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NamespaceSpec {
    /// `Finalizers` is an opaque list of values that must be empty
    /// to permanently remove an object from storage. More:
    /// https://kubernetes.io/docs/tasks/administer-cluster/namespaces/
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub finalizers: Vec<String>,
}

/// `NamespaceStatus` is information about the current status of a Namespace.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NamespaceStatus {
    /// `Phase` is the current lifecycle phase. One of: `Active`,
    /// `Terminating`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<NamespacePhase>,

    /// `Conditions` is a list of typed conditions that describe
    /// the namespace's current state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<NamespaceCondition>,
}

/// `NamespacePhase` — closed enum of valid namespace phases.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NamespacePhase {
    Active,
    Terminating,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NamespaceCondition {
    #[serde(rename = "type")]
    pub r#type: String,
    pub status: String,
    #[serde(default, rename = "lastTransitionTime", skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_phase_round_trips() {
        assert_eq!(serde_json::to_string(&NamespacePhase::Active).unwrap(), "\"Active\"");
        assert_eq!(
            serde_json::to_string(&NamespacePhase::Terminating).unwrap(),
            "\"Terminating\""
        );
        for p in [NamespacePhase::Active, NamespacePhase::Terminating] {
            let s = serde_json::to_string(&p).unwrap();
            let back: NamespacePhase = serde_json::from_str(&s).unwrap();
            assert_eq!(back, p);
        }
    }

    #[test]
    fn namespace_status_active_round_trips() {
        let status = NamespaceStatus {
            phase: Some(NamespacePhase::Active),
            conditions: vec![NamespaceCondition {
                r#type: "NamespaceDeletionDiscoveryFailure".into(),
                status: "False".into(),
                ..Default::default()
            }],
        };
        let s = serde_json::to_string(&status).unwrap();
        assert!(s.contains("\"Active\""));
        let back: NamespaceStatus = serde_json::from_str(&s).unwrap();
        assert_eq!(back, status);
    }

    use proptest::prelude::*;

    proptest! {
        /// Any NamespacePhase round-trips identity.
        #[test]
        fn arb_namespace_phase_round_trips(idx in 0usize..2) {
            let phases = [NamespacePhase::Active, NamespacePhase::Terminating];
            let phase = phases[idx];
            let s = serde_json::to_string(&phase).unwrap();
            let back: NamespacePhase = serde_json::from_str(&s).unwrap();
            prop_assert_eq!(back, phase);
        }
    }

    #[test]
    fn empty_namespace_spec_serializes_to_empty_object() {
        let s = serde_json::to_string(&NamespaceSpec::default()).unwrap();
        assert_eq!(s, "{}");
    }
}
