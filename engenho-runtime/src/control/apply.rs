//! Applying a configuration change — the one pipeline `config set`, `unset`,
//! `clear` and `reload` share.
//!
//! 1. Build the candidate override set (or, for a reload, keep the set and
//!    re-read the declared file).
//! 2. Resolve it with the same fold a boot uses.
//! 3. Gate it with the same validators a boot uses: the fold's own
//!    (`validate()`, `deny_unknown_fields`) and [`BootConfig::read`]. There is
//!    no second validator to disagree with boot: a change is accepted iff a
//!    boot on the result would get past its configuration.
//! 4. Diff it leaf by leaf, classified by [`engenho_config::mutability`]; a
//!    change to a `not_overridable` leaf refuses the whole change.
//! 5. A dry run stops here.
//! 6. Commit the override set, then hand the running runtime what it can take
//!    in place; what takes a restart is recorded as pending (or restarts it,
//!    when asked to).
//!
//! This module is the pure half: [`diff`], [`gate`], [`restart_pending`] and
//! [`effect`]. [`super::service`] runs the pipeline; the supervisor applies.

use engenho_config::leaf::flatten;
use engenho_config::mutability::{classify, leaves};
use engenho_config::{EngenhoConfig, LeafPath, Mutability};
use serde::{Deserialize, Serialize};

use crate::boot_config::BootConfig;
use crate::child::Child;
use crate::lifecycle::supervisor::Reconfigured;

/// One leaf a change moves.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LeafChange {
    /// Which.
    pub path: LeafPath,
    /// Its effective value before (`null` when it had none).
    pub previous: serde_json::Value,
    /// Its effective value after.
    pub next: serde_json::Value,
    /// What changing it does.
    pub mutability: Mutability,
}

/// What an applied change did: the control API's `ApplyEffect`, crossed to
/// it by one exhaustive conversion in [`super::service`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplyEffect {
    /// Nothing effective changed.
    NoChange,
    /// A dry run: nothing was written or applied.
    PlannedOnly,
    /// The running runtime took it in place.
    AppliedLive,
    /// The running runtime took it in place by spawning, stopping or
    /// rebuilding these children.
    Respawned {
        /// Which.
        children: Vec<Child>,
    },
    /// A boot is coming that reads it: a restart, or a held failure's retry.
    RestartScheduled,
    /// The running runtime reflects it only after a restart, which was not
    /// asked for; these leaves are pending.
    RestartDeferred {
        /// Every leaf the running runtime does not reflect yet.
        leaves: Vec<LeafPath>,
    },
    /// Nothing runs (or it is read only while booting): the next boot reads
    /// it.
    NextBoot,
}

/// Every leaf whose effective value differs between `before` and `after`,
/// classified, in table order.
#[must_use]
pub fn diff(before: &EngenhoConfig, after: &EngenhoConfig) -> Vec<LeafChange> {
    let before = flat(before);
    let after = flat(after);
    leaves()
        .iter()
        .filter_map(|spec| {
            let previous = before.get(&spec.path).cloned().unwrap_or_default();
            let next = after.get(&spec.path).cloned().unwrap_or_default();
            (previous != next).then(|| LeafChange {
                path: spec.path.clone(),
                previous,
                next,
                mutability: spec.mutability,
            })
        })
        .collect()
}

/// The leaves a runtime running `running` would reflect only after a restart
/// to be running `effective`.
#[must_use]
pub fn restart_pending(running: &EngenhoConfig, effective: &EngenhoConfig) -> Vec<LeafPath> {
    diff(running, effective)
        .into_iter()
        .filter(|change| change.mutability.needs_restart())
        .map(|change| change.path)
        .collect()
}

/// Why a candidate configuration is refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    /// A boot would refuse it.
    #[error("a boot would refuse this configuration: {0}")]
    BootRefuses(String),
    /// It changes a leaf the control plane may not change.
    #[error(
        "{path} cannot be overridden ({anchor}); change it in the declared file and restart the daemon"
    )]
    NotOverridable {
        /// Which leaf.
        path: LeafPath,
        /// What it anchors, spelled as the control API does.
        anchor: &'static str,
    },
}

/// Step 3 and the refusal half of step 4: the candidate must get past a
/// boot's configuration phase, and may not move a leaf that is not
/// overridable.
///
/// # Errors
///
/// [`Refusal`].
pub fn gate(after: &EngenhoConfig, changed: &[LeafChange]) -> Result<(), Refusal> {
    let anchored = changed.iter().find_map(|c| match c.mutability {
        Mutability::NotOverridable { anchor } => Some((c, anchor)),
        _ => None,
    });
    if let Some((change, anchor)) = anchored {
        return Err(Refusal::NotOverridable {
            path: change.path.clone(),
            anchor: anchor_name(anchor),
        });
    }
    BootConfig::read(after).map_err(|e| Refusal::BootRefuses(e.to_string()))?;
    Ok(())
}

/// What a target leaf of a `set` is, or why it cannot be set.
///
/// # Errors
///
/// `Err(None)` when it is not a leaf; `Err(Some(anchor))` when it is not
/// overridable.
pub fn settable(path: &LeafPath) -> Result<Mutability, Option<&'static str>> {
    match classify(path) {
        None => Err(None),
        Some(Mutability::NotOverridable { anchor }) => Err(Some(anchor_name(anchor))),
        Some(class) => Ok(class),
    }
}

/// Leaves under `prefix` (every leaf when it names none), to say what a
/// caller could have meant.
#[must_use]
pub fn leaves_under(prefix: &str) -> Vec<LeafPath> {
    let under: Vec<LeafPath> = leaves()
        .iter()
        .filter(|spec| spec.path.is_under(prefix))
        .map(|spec| spec.path.clone())
        .collect();
    if under.is_empty() {
        leaves().iter().map(|spec| spec.path.clone()).collect()
    } else {
        under
    }
}

fn anchor_name(anchor: engenho_config::Anchor) -> &'static str {
    use engenho_config::Anchor as A;
    match anchor {
        A::OverlayAnchor => "overlay_anchor",
        A::StorageMode => "storage_mode",
        A::Identity => "identity",
        A::Allocated => "allocated",
        A::Retired => "retired",
    }
}

/// Step 6's summary: what applying `changed` did, given what the supervisor
/// reported.
#[must_use]
pub fn effect(changed: &[LeafChange], outcome: &Reconfigured) -> ApplyEffect {
    match outcome {
        Reconfigured::Retrying => ApplyEffect::RestartScheduled,
        Reconfigured::NotRunning => {
            if changed.is_empty() {
                ApplyEffect::NoChange
            } else {
                ApplyEffect::NextBoot
            }
        }
        Reconfigured::Running {
            republished,
            respawned,
            pending,
            restarted,
        } => {
            let any = |test: fn(Mutability) -> bool| changed.iter().any(|c| test(c.mutability));
            if *restarted {
                ApplyEffect::RestartScheduled
            } else if changed.iter().any(|c| pending.contains(&c.path)) {
                // Every pending leaf, not only this change's: the restart
                // that brings this one in brings them all.
                ApplyEffect::RestartDeferred {
                    leaves: pending.clone(),
                }
            } else if changed.is_empty() {
                ApplyEffect::NoChange
            } else if !respawned.is_empty() {
                ApplyEffect::Respawned {
                    children: respawned.clone(),
                }
            } else if *republished || !any(|m| matches!(m, Mutability::NextBoot)) {
                ApplyEffect::AppliedLive
            } else {
                ApplyEffect::NextBoot
            }
        }
    }
}

fn flat(config: &EngenhoConfig) -> std::collections::BTreeMap<LeafPath, serde_json::Value> {
    // A configuration always serializes: every field is plain data.
    serde_json::to_value(config)
        .map(|v| flatten(&v))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use engenho_config::TieredConfig;

    use super::*;

    fn base() -> EngenhoConfig {
        EngenhoConfig::prescribed_default()
    }

    fn leaf(path: &str) -> LeafPath {
        LeafPath::parse(path).unwrap()
    }

    #[test]
    fn the_diff_is_classified_leaf_by_leaf() {
        let mut after = base();
        after.runtime.kubeconfig_publish_path = "/tmp/kc".into();
        after.scheduler.tick_interval_seconds = 9;
        let changed = diff(&base(), &after);
        let paths: Vec<_> = changed.iter().map(|c| c.path.as_str()).collect();
        assert_eq!(
            paths,
            [
                "scheduler.tick_interval_seconds",
                "runtime.kubeconfig_publish_path"
            ]
        );
        assert_eq!(changed[0].mutability, Mutability::RestartRuntime);
        assert_eq!(changed[1].next, "/tmp/kc");
        assert!(diff(&base(), &base()).is_empty());
    }

    #[test]
    fn only_restart_class_leaves_are_pending() {
        let mut after = base();
        after.runtime.kubeconfig_publish_path = "/tmp/kc".into();
        after.runtime.leadership_timeout_seconds += 1;
        after.controllers.enable.gc = !after.controllers.enable.gc;
        after.runtime.listen_addr = "0.0.0.0:7443".into();
        assert_eq!(
            restart_pending(&base(), &after),
            [leaf("controllers.enable.gc"), leaf("runtime.listen_addr")]
        );
    }

    #[test]
    fn a_not_overridable_change_refuses_the_whole_change() {
        let mut after = base();
        after.runtime.data_dir = "/elsewhere".into();
        after.scheduler.tick_interval_seconds = 9;
        let changed = diff(&base(), &after);
        assert_eq!(
            gate(&after, &changed),
            Err(Refusal::NotOverridable {
                path: leaf("runtime.data_dir"),
                anchor: "overlay_anchor",
            })
        );
    }

    #[test]
    fn a_target_must_be_an_overridable_leaf() {
        assert_eq!(settable(&leaf("runtime.tls")), Err(None));
        assert_eq!(settable(&leaf("cluster.name")), Err(Some("identity")));
        assert_eq!(
            settable(&leaf("runtime.leadership_timeout_seconds")),
            Ok(Mutability::NextBoot)
        );
        assert!(
            leaves_under("runtime.tls")
                .iter()
                .all(|p| p.is_under("runtime.tls"))
        );
        assert_eq!(leaves_under("nonsense").len(), leaves().len());
    }

    #[test]
    fn the_effect_follows_the_strongest_change() {
        let mut after = base();
        after.runtime.kubeconfig_publish_path = "/tmp/kc".into();
        let live = diff(&base(), &after);
        after.scheduler.tick_interval_seconds = 9;
        let both = diff(&base(), &after);
        let pending = vec![leaf("scheduler.tick_interval_seconds")];
        let running = |restarted| Reconfigured::Running {
            republished: true,
            respawned: Vec::new(),
            pending: pending.clone(),
            restarted,
        };

        assert_eq!(effect(&live, &running(false)), ApplyEffect::AppliedLive);
        assert_eq!(
            effect(&both, &running(false)),
            ApplyEffect::RestartDeferred {
                leaves: pending.clone()
            }
        );
        assert_eq!(effect(&both, &running(true)), ApplyEffect::RestartScheduled);
        assert_eq!(effect(&[], &running(false)), ApplyEffect::NoChange);
        assert_eq!(
            effect(&both, &Reconfigured::Retrying),
            ApplyEffect::RestartScheduled
        );
        assert_eq!(
            effect(&both, &Reconfigured::NotRunning),
            ApplyEffect::NextBoot
        );

        let mut boot_only = base();
        boot_only.runtime.leadership_timeout_seconds += 1;
        let changed = diff(&base(), &boot_only);
        let quiet = Reconfigured::Running {
            republished: false,
            respawned: Vec::new(),
            pending: Vec::new(),
            restarted: false,
        };
        assert_eq!(effect(&changed, &quiet), ApplyEffect::NextBoot);
    }

    #[test]
    fn a_switch_taken_in_place_reports_the_children_it_moved() {
        let mut after = base();
        after.controllers.enable.gc = !after.controllers.enable.gc;
        let changed = diff(&base(), &after);
        let gc = vec![crate::child::Child::Driver(crate::child::Driver::Gc)];
        let taken = Reconfigured::Running {
            republished: false,
            respawned: gc.clone(),
            pending: Vec::new(),
            restarted: false,
        };
        assert_eq!(
            effect(&changed, &taken),
            ApplyEffect::Respawned { children: gc }
        );
    }

    #[test]
    fn an_earlier_pending_leaf_does_not_defer_a_live_change() {
        let mut after = base();
        after.runtime.kubeconfig_publish_path = "/tmp/kc".into();
        let live = diff(&base(), &after);
        let earlier = Reconfigured::Running {
            republished: true,
            respawned: Vec::new(),
            pending: vec![leaf("runtime.listen_addr")],
            restarted: false,
        };
        assert_eq!(effect(&live, &earlier), ApplyEffect::AppliedLive);
    }
}
