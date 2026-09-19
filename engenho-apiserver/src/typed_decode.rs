//! A typed decode per catalog row, in shadow (plan T4.6).
//!
//! engenho stores the `serde_json::Value` a client sent; upstream stores the
//! Go struct it decoded that body into. The generated structs in
//! `engenho_types::generated_v1_34` are meant to be that struct. Before any
//! code may decode a stored object into one and trust the result, every
//! body upstream accepts must decode — and today not every one does (a
//! `Quantity` written as a JSON number is refused by `Quantity(String)`;
//! an `ObjectMeta` string given as `null` is refused where Go leaves it
//! empty).
//!
//! So each POST, PUT and apply body is decoded into its row's struct here,
//! and the result only COUNTS: [`TYPED_DECODE`] is a [`Rollout::Shadow`]
//! gate, the body that is stored is the original `Value`, and a body the
//! struct refuses is one `engenho_would_reject_total{gate="typed_decode",
//! reason="decode_error"}` plus a log line naming the kind, the object and
//! serde's error. That series is the census that decides when a row may be
//! enforced; `tests/w2_typed_border.rs` holds the other half, every row's
//! upstream roundtrip fixture and an accept-set of bodies upstream takes.
//!
//! [`TYPED_ROWS`] is built from the generated types themselves (each
//! carries its own `KubeResource::GVK`), and a test fails when a
//! schema-backed catalog row has no entry.
//!
//! Tier: counting is structural (the gate cannot allow without recording).
//! Whether a verb calls [`judge`] at all is the router's choice — PATCH
//! bodies are partial documents and are not decoded.

use std::fmt;

use engenho_substrate::WouldRejectLedger;
use engenho_substrate::rollout::{Gate, Proceed, RejectReason, Rollout};
use engenho_types::generated_v1_34::{
    apps_v1, autoscaling_v2, batch_v1, coordination_v1, core_v1, networking_v1, node_v1, policy_v1,
    rbac_v1, scheduling_v1, storage_v1,
};
use engenho_types::kind::{GroupVersionKind, KubeResource};
use serde_json::Value;

use crate::error::ApiError;
use crate::object_body::Undecodable;

/// The typed-decode gate. Shadow until the census for a row reads zero.
pub const TYPED_DECODE: Gate = Gate::new("typed_decode", Rollout::Shadow);

/// One catalog row's generated struct, as a decode function.
#[derive(Clone, Copy)]
pub struct TypedRow {
    gvk: GroupVersionKind,
    decode: fn(&Value) -> Result<(), serde_json::Error>,
}

impl fmt::Debug for TypedRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TypedRow")
            .field("gvk", &self.gvk)
            .finish_non_exhaustive()
    }
}

impl TypedRow {
    /// The row's group/version/kind, read off the generated type.
    #[must_use]
    pub fn gvk(&self) -> GroupVersionKind {
        self.gvk
    }

    /// Decode `value` into the row's struct and discard the result.
    ///
    /// # Errors
    ///
    /// serde's error when the struct refuses the body.
    pub fn decode(&self, value: &Value) -> Result<(), serde_json::Error> {
        (self.decode)(value)
    }
}

fn decode_as<T: KubeResource>(value: &Value) -> Result<(), serde_json::Error> {
    T::deserialize(value).map(drop)
}

macro_rules! typed_rows {
    ($($t:ty),* $(,)?) => {
        &[$(TypedRow {
            gvk: <$t as KubeResource>::GVK,
            decode: decode_as::<$t>,
        }),*]
    };
}

/// Every generated kind, one row each.
pub static TYPED_ROWS: &[TypedRow] = typed_rows![
    core_v1::Pod,
    core_v1::Service,
    core_v1::ConfigMap,
    core_v1::Secret,
    core_v1::Namespace,
    core_v1::ServiceAccount,
    core_v1::Node,
    core_v1::PersistentVolume,
    core_v1::PersistentVolumeClaim,
    core_v1::Endpoints,
    core_v1::ReplicationController,
    core_v1::PodTemplate,
    core_v1::LimitRange,
    core_v1::ResourceQuota,
    core_v1::Event,
    apps_v1::Deployment,
    apps_v1::ReplicaSet,
    apps_v1::StatefulSet,
    apps_v1::DaemonSet,
    apps_v1::ControllerRevision,
    rbac_v1::Role,
    rbac_v1::ClusterRole,
    rbac_v1::RoleBinding,
    rbac_v1::ClusterRoleBinding,
    batch_v1::Job,
    batch_v1::CronJob,
    autoscaling_v2::HorizontalPodAutoscaler,
    policy_v1::PodDisruptionBudget,
    networking_v1::Ingress,
    networking_v1::IngressClass,
    networking_v1::NetworkPolicy,
    storage_v1::StorageClass,
    storage_v1::CSINode,
    storage_v1::CSIDriver,
    storage_v1::VolumeAttachment,
    storage_v1::CSIStorageCapacity,
    node_v1::RuntimeClass,
    scheduling_v1::PriorityClass,
    coordination_v1::Lease,
];

/// The row for `group/version, kind`, when one is generated.
#[must_use]
pub fn typed_row(group: &str, version: &str, kind: &str) -> Option<&'static TypedRow> {
    TYPED_ROWS
        .iter()
        .find(|r| r.gvk.group == group && r.gvk.version == version && r.gvk.kind == kind)
}

/// Why the gate would refuse: the generated struct refused the body. The
/// serde error itself travels in the log line's subject.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Undecoded;

impl RejectReason for Undecoded {
    fn label(&self) -> &'static str {
        "decode_error"
    }
}

/// What the log line names: `<Kind> <namespace>/<name>: <serde error>`.
struct Subject<'a> {
    kind: &'a str,
    value: &'a Value,
    error: Option<&'a serde_json::Error>,
}

impl fmt::Display for Subject<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let meta = |field: &str| {
            self.value
                .pointer("/metadata")
                .and_then(|m| m.get(field))
                .and_then(Value::as_str)
                .unwrap_or_default()
        };
        write!(f, "{} {}/{}", self.kind, meta("namespace"), meta("name"))?;
        if let Some(error) = self.error {
            write!(f, ": {error}")?;
        }
        Ok(())
    }
}

/// Decode `value` as `kind` of `group/version` through [`TYPED_DECODE`].
/// A kind with no generated struct is not judged.
///
/// # Errors
///
/// Only if the gate enforces: upstream's 400 for a body it cannot decode.
pub fn judge(
    group: &str,
    version: &str,
    kind: &str,
    value: &Value,
    ledger: &WouldRejectLedger,
) -> Result<(), ApiError> {
    let Some(row) = typed_row(group, version, kind) else {
        return Ok(());
    };
    let decoded = row.decode(value);
    let subject = Subject {
        kind,
        value,
        error: decoded.as_ref().err(),
    };
    let check = decoded.as_ref().copied().map_err(|_| Undecoded);
    match (TYPED_DECODE.judge(check, ledger, &subject), &decoded) {
        (Err(_), Err(error)) => Err(ApiError::BadRequest(
            Undecodable {
                version,
                kind,
                error,
            }
            .to_string(),
        )),
        (Ok(Proceed::Clean | Proceed::Shadowed(Undecoded)), _) | (Err(_), Ok(())) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_schema_backed_catalog_row_has_exactly_one_typed_row() {
        for d in engenho_types::generated_v1_34::RESOURCE_CATALOG {
            let rows = TYPED_ROWS
                .iter()
                .filter(|r| {
                    r.gvk.group == d.group && r.gvk.version == d.version && r.gvk.kind == d.kind
                })
                .count();
            assert_eq!(
                rows,
                usize::from(!d.opaque),
                "{}/{} {}: one typed row per schema-backed catalog row, none for an opaque one",
                d.group,
                d.version,
                d.kind
            );
        }
        assert_eq!(
            TYPED_ROWS.len(),
            engenho_types::generated_v1_34::RESOURCE_CATALOG
                .iter()
                .filter(|d| !d.opaque)
                .count(),
            "no typed row outside the catalog"
        );
    }

    #[test]
    fn a_refused_body_is_counted_and_still_allowed() {
        let ledger = WouldRejectLedger::new(|_| {});
        let body = serde_json::json!({
            "metadata": {"name": "p"},
            "spec": {"containers": [{"name": "c", "resources": {"limits": {"cpu": 1}}}]}
        });
        judge("", "v1", "Pod", &body, &ledger).expect("shadow allows");
        let counts = ledger.snapshot();
        assert_eq!(counts.len(), 1, "{counts:?}");
        assert_eq!(counts[0].gate, "typed_decode");
        assert_eq!(counts[0].reason, "decode_error");
        assert_eq!(counts[0].count, 1);

        let clean = serde_json::json!({"metadata": {"name": "p"}, "data": {"k": "v"}});
        judge("", "v1", "ConfigMap", &clean, &ledger).expect("clean");
        assert_eq!(
            ledger.snapshot()[0].count,
            1,
            "a clean decode counts nothing"
        );
    }
}
