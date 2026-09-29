//! Template revisions and update strategies — how a controller that keeps
//! pods under STABLE names (`DaemonSet`, `StatefulSet`) knows a pod is out of
//! date, and how many it may replace at once.
//!
//! ## The defect this closes
//!
//! Both controllers used to decide a pod's fate by its NAME alone: a node (or
//! an ordinal) with a pod was covered, whatever template the pod was made
//! from. A template change therefore reached no running pod, ever. Measured on
//! a native node when the daemon restarted onto a new release: every
//! `DaemonSet` was re-rendered with the new closure as its image, the eight
//! existing pods kept the old one, the old closure was garbage-collected, and
//! the pods sat `ContainerCreating` for five and a half hours with nothing
//! replacing them. Deleting them by hand was the only way out.
//!
//! Upstream stamps every pod with the revision of the template it was built
//! from — the `controller-revision-hash` label, and for a `DaemonSet` also
//! `pod-template-generation` — and replaces the pods whose revision differs,
//! as the parent's `updateStrategy` allows. This module is that vocabulary,
//! once, for both controllers.
//!
//! ## What the revision is
//!
//! [`TemplateRevision`] is the naming hash of the parent's normalized
//! template ([`crate::pod_template::NormalizedTemplate`]), the hash a
//! Deployment already names its `ReplicaSet`s by. Normalized, so a template
//! whose bytes change without its meaning changing (`labels: null` dropped,
//! `env: []` omitted) does not roll every pod in the cluster. Upstream hashes
//! a `ControllerRevision`; engenho does not keep `ControllerRevision` objects
//! (see `docs/QUALIFICATION.md`), so the hash value differs from upstream's
//! while the label, and the rule it drives, are the same.
//!
//! A pod with NO revision label is out of date, as upstream reads it. Pods
//! created before this module existed carry none, so the first reconcile
//! after an upgrade rolls every such pod once — within the strategy's budget,
//! which is the behaviour an upgrade should have.

use serde_json::Value;

use crate::error::ControllerError;
use crate::meta::{ShapeError, object_mut};
use crate::pod_template::NormalizedTemplate;

/// The label naming the template revision a pod was built from. Upstream's
/// `apps.ControllerRevisionHashLabelKey`.
pub const CONTROLLER_REVISION_HASH_LABEL: &str = "controller-revision-hash";

/// The label naming the `DaemonSet` generation a daemon pod was built at.
/// Upstream's `extensions.DaemonSetTemplateGenerationKey`.
pub const POD_TEMPLATE_GENERATION_LABEL: &str = "pod-template-generation";

/// The revision of a parent's pod template.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateRevision(String);

impl TemplateRevision {
    /// The revision of `parent`'s `spec.template`. `None` when it declares no
    /// template.
    #[must_use]
    pub fn of(parent: &Value) -> Option<Self> {
        NormalizedTemplate::of_spec_template(parent)
            .and_then(|t| t.naming_hash())
            .map(|h| Self(h.to_string()))
    }

    /// The revision label a pod carries, if any.
    #[must_use]
    pub fn of_pod(pod: &Value) -> Option<&str> {
        pod.pointer("/metadata/labels")
            .and_then(|l| l.get(CONTROLLER_REVISION_HASH_LABEL))
            .and_then(Value::as_str)
    }

    /// Whether `pod` was built from this revision. A pod with no revision
    /// label was not.
    #[must_use]
    pub fn is_current(&self, pod: &Value) -> bool {
        Self::of_pod(pod) == Some(self.0.as_str())
    }

    /// The label value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Stamp this revision (and, for a `DaemonSet`, the parent's
    /// `generation`) onto a pod built from the template.
    ///
    /// # Errors
    ///
    /// [`ShapeError`] when the pod's `metadata.labels` is not an object.
    pub fn stamp(&self, pod: &mut Value, generation: Option<i64>) -> Result<(), ShapeError> {
        let labels = object_mut(pod, &["metadata", "labels"])?;
        labels.insert(
            CONTROLLER_REVISION_HASH_LABEL.into(),
            Value::from(self.0.as_str()),
        );
        if let Some(g) = generation {
            labels.insert(
                POD_TEMPLATE_GENERATION_LABEL.into(),
                Value::from(g.to_string()),
            );
        }
        Ok(())
    }
}

/// How many pods an update may take down at once: `maxUnavailable`, an
/// integer or a percentage (`IntOrString`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaxUnavailable {
    /// An absolute count.
    Count(u32),
    /// A percentage of the desired pod count.
    Percent(u32),
}

impl MaxUnavailable {
    /// Upstream's default for both `DaemonSet` and `StatefulSet`.
    pub const DEFAULT: Self = Self::Count(1);

    /// Resolve against `desired` pods, rounding a percentage UP as the
    /// `DaemonSet` controller does, and never below one: a budget of zero
    /// would stall every update forever (upstream turns `0` into `1` when
    /// `maxSurge` is also `0`, and engenho has no surge).
    #[must_use]
    pub fn resolve(self, desired: usize) -> usize {
        let n = match self {
            Self::Count(c) => c as usize,
            Self::Percent(p) => (desired * p as usize).div_ceil(100),
        };
        n.max(1)
    }

    fn parse(v: &Value) -> Result<Self, String> {
        match v {
            Value::Number(n) => n
                .as_u64()
                .and_then(|n| u32::try_from(n).ok())
                .map(Self::Count)
                .ok_or_else(|| format!("maxUnavailable {n} is not a non-negative integer")),
            Value::String(s) => s
                .strip_suffix('%')
                .and_then(|p| p.parse::<u32>().ok())
                .filter(|p| *p <= 100)
                .map(Self::Percent)
                .ok_or_else(|| format!("maxUnavailable {s:?} is not an integer or a percentage")),
            other => Err(format!(
                "maxUnavailable is {other}, not an integer or a percentage"
            )),
        }
    }
}

/// A parent's `spec.updateStrategy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateStrategy {
    /// Replace out-of-date pods, at most `max_unavailable` down at once.
    /// Upstream's default for both kinds.
    RollingUpdate {
        /// The budget.
        max_unavailable: MaxUnavailable,
        /// `StatefulSet` only: ordinals below this are never updated.
        partition: u32,
    },
    /// Replace a pod only once something else deletes it.
    OnDelete,
}

impl UpdateStrategy {
    /// Read `spec.updateStrategy` from `parent`. Absent is
    /// `RollingUpdate` with `maxUnavailable: 1` — upstream's default.
    ///
    /// # Errors
    ///
    /// [`ControllerError::InvalidResource`] for an unknown `type` or a
    /// malformed `rollingUpdate`. Item-scoped: that one parent is refused
    /// (an Event on it) and every other parent reconciles.
    pub fn of(parent: &Value) -> Result<Self, ControllerError> {
        let Some(s) = parent
            .pointer("/spec/updateStrategy")
            .filter(|v| !v.is_null())
        else {
            return Ok(Self::default());
        };
        let invalid = |why: String| ControllerError::InvalidResource(why);
        match s
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("RollingUpdate")
        {
            "OnDelete" => Ok(Self::OnDelete),
            "RollingUpdate" => {
                let ru = s.get("rollingUpdate").filter(|v| !v.is_null());
                let max_unavailable = match ru.and_then(|r| r.get("maxUnavailable")) {
                    None | Some(Value::Null) => MaxUnavailable::DEFAULT,
                    Some(v) => MaxUnavailable::parse(v).map_err(invalid)?,
                };
                let partition = match ru.and_then(|r| r.get("partition")) {
                    None | Some(Value::Null) => 0,
                    Some(v) => v
                        .as_u64()
                        .and_then(|p| u32::try_from(p).ok())
                        .ok_or_else(|| {
                            invalid(format!("partition {v} is not a non-negative integer"))
                        })?,
                };
                Ok(Self::RollingUpdate {
                    max_unavailable,
                    partition,
                })
            }
            other => Err(invalid(format!(
                "updateStrategy.type {other:?} is neither RollingUpdate nor OnDelete"
            ))),
        }
    }
}

impl Default for UpdateStrategy {
    fn default() -> Self {
        Self::RollingUpdate {
            max_unavailable: MaxUnavailable::DEFAULT,
            partition: 0,
        }
    }
}

/// Whether a pod has reached phase `Failed`: terminal, and replaced by a
/// controller that keeps one pod per slot.
#[must_use]
pub fn pod_is_failed(pod: &Value) -> bool {
    pod.pointer("/status/phase").and_then(Value::as_str) == Some("Failed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parent(template: &Value) -> Value {
        json!({"spec": {"template": template}})
    }

    #[test]
    fn the_revision_follows_the_template_and_ignores_meaningless_bytes() {
        let a = TemplateRevision::of(&parent(
            &json!({"spec": {"containers": [{"name": "c", "image": "nix:/nix/store/p27v2-engenho"}]}}),
        ))
        .unwrap();
        let b = TemplateRevision::of(&parent(
            &json!({"spec": {"containers": [{"name": "c", "image": "nix:/nix/store/q91n8-engenho"}]}}),
        ))
        .unwrap();
        assert_ne!(a, b, "a new image is a new revision");
        let a_again = TemplateRevision::of(&parent(&json!({
            "metadata": {"labels": null},
            "spec": {"containers": [{"name": "c", "image": "nix:/nix/store/p27v2-engenho", "env": []}]}
        })))
        .unwrap();
        assert_eq!(
            a, a_again,
            "bytes that mean the same template are the same revision"
        );
        assert!(TemplateRevision::of(&json!({"spec": {}})).is_none());
    }

    #[test]
    fn a_pod_without_a_revision_label_is_out_of_date() {
        let rev = TemplateRevision::of(&parent(&json!({"spec": {}}))).unwrap();
        assert!(!rev.is_current(&json!({"metadata": {"name": "p"}})));
        let mut pod = json!({"metadata": {"name": "p"}});
        rev.stamp(&mut pod, Some(3)).unwrap();
        assert!(rev.is_current(&pod));
        assert_eq!(
            pod["metadata"]["labels"][POD_TEMPLATE_GENERATION_LABEL],
            "3"
        );
    }

    #[test]
    fn the_default_strategy_is_upstreams_rolling_update_of_one() {
        assert_eq!(
            UpdateStrategy::of(&json!({"spec": {}})).unwrap(),
            UpdateStrategy::RollingUpdate {
                max_unavailable: MaxUnavailable::Count(1),
                partition: 0
            }
        );
        assert_eq!(
            UpdateStrategy::of(&json!({"spec": {"updateStrategy": {"type": "RollingUpdate"}}}))
                .unwrap(),
            UpdateStrategy::default()
        );
    }

    #[test]
    fn on_delete_and_explicit_budgets_are_read() {
        assert_eq!(
            UpdateStrategy::of(&json!({"spec": {"updateStrategy": {"type": "OnDelete"}}})).unwrap(),
            UpdateStrategy::OnDelete
        );
        let s = UpdateStrategy::of(&json!({"spec": {"updateStrategy": {
            "type": "RollingUpdate", "rollingUpdate": {"maxUnavailable": "25%", "partition": 2}
        }}}))
        .unwrap();
        assert_eq!(
            s,
            UpdateStrategy::RollingUpdate {
                max_unavailable: MaxUnavailable::Percent(25),
                partition: 2
            }
        );
    }

    #[test]
    fn an_unknown_strategy_refuses_that_parent() {
        for bad in [
            json!({"type": "Recreate"}),
            json!({"type": "RollingUpdate", "rollingUpdate": {"maxUnavailable": "x%"}}),
            json!({"type": "RollingUpdate", "rollingUpdate": {"maxUnavailable": -1}}),
            json!({"type": "RollingUpdate", "rollingUpdate": {"partition": "1"}}),
        ] {
            let err = UpdateStrategy::of(&json!({"spec": {"updateStrategy": bad}})).unwrap_err();
            assert!(matches!(err, ControllerError::InvalidResource(_)), "{err}");
        }
    }

    #[test]
    fn a_budget_resolves_rounding_up_and_never_to_zero() {
        assert_eq!(MaxUnavailable::Count(1).resolve(8), 1);
        assert_eq!(MaxUnavailable::Count(0).resolve(8), 1);
        assert_eq!(MaxUnavailable::Percent(25).resolve(8), 2);
        assert_eq!(MaxUnavailable::Percent(10).resolve(8), 1);
        assert_eq!(MaxUnavailable::Percent(100).resolve(8), 8);
        assert_eq!(MaxUnavailable::Percent(0).resolve(8), 1);
    }
}
