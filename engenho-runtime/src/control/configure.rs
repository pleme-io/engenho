//! The configuration half of [`DaemonControl`]: leaves, overrides, drift,
//! and the [`super::apply`] pipeline run end to end.

use std::path::Path;
use std::sync::Arc;

use engenho_config::leaf::flatten;
use engenho_config::mutability::{classify, leaves};
use engenho_config::{ConfigTierKind, EngenhoConfig, LeafPath, ProvenanceMap, TieredConfig};
use engenho_control_types::types::{self, BlindReason, RefusalReason};
use engenho_control_types::{ControlError, Principal};

use super::apply::{self, LeafChange, Refusal};
use super::overrides::{
    Change, Durability, OverrideSet, OverrideStore, OverrideWriteError, Stored,
};
use super::service::{DaemonControl, effect_to_wire, wire};
use crate::lifecycle::{DaemonEvent, ReconfigureError, ResolvedConfig};

/// How a change is to be applied, whichever operation carried it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ApplyOptions {
    /// Plan it, change nothing.
    pub dry_run: bool,
    /// Restart the runtime now for what it cannot take in place.
    pub restart_now: bool,
    /// Refuse unless the override set is at this generation.
    pub precondition: Option<u64>,
}

/// Whether a restart policy asks for the restart now.
pub(super) const fn restart_now(policy: Option<types::RestartPolicy>) -> bool {
    matches!(policy, Some(types::RestartPolicy::Now))
}

/// A leaf path from the API.
pub(super) fn leaf_path(path: &types::LeafPath) -> Result<LeafPath, ControlError> {
    wire(path)
}

/// The override file cannot be read.
pub(super) fn unreadable_overrides(why: &str) -> ControlError {
    ControlError::blind(
        BlindReason::IoError,
        format!("the override tier cannot be read ({why}); `engenho ctl config clear` replaces it"),
    )
}

/// The supervisor could not apply or publish.
pub(super) fn reconfigure_refusal(err: ReconfigureError) -> ControlError {
    match err {
        ReconfigureError::NotRunning => ControlError::refused_with(
            RefusalReason::RuntimeNotRunning,
            "no runtime is running",
            vec!["engenho ctl runtime start".into()],
        ),
        ReconfigureError::Config(why) => ControlError::refused(
            RefusalReason::ConfigRejected,
            format!("the configuration does not resolve: {why}"),
        ),
        ReconfigureError::Runtime(why) => ControlError::blind(BlindReason::IoError, why),
        ReconfigureError::Gone => {
            ControlError::blind(BlindReason::Internal, "the daemon's supervisor has ended")
        }
    }
}

fn unknown_leaf(path: &str) -> ControlError {
    // What the caller could have meant: the leaves under what they typed, or
    // the sections there are.
    let under = apply::leaves_under(path);
    let legal: Vec<String> = if under.len() == leaves().len() {
        let mut sections: Vec<String> = leaves()
            .iter()
            .filter_map(|spec| spec.path.segments().next().map(str::to_owned))
            .collect();
        sections.dedup();
        sections
    } else {
        under.iter().map(ToString::to_string).collect()
    };
    ControlError::refused_with(
        RefusalReason::UnknownLeaf,
        format!("{path} is not a leaf of the configuration"),
        legal,
    )
}

fn plan_refusal(refusal: &Refusal) -> ControlError {
    match refusal {
        Refusal::BootRefuses(_) => {
            ControlError::refused(RefusalReason::ConfigRejected, refusal.to_string())
        }
        Refusal::NotOverridable { .. } => {
            ControlError::refused(RefusalReason::NotOverridableLeaf, refusal.to_string())
        }
    }
}

fn change_to_wire(change: &LeafChange) -> Result<types::LeafChange, ControlError> {
    wire(change)
}

impl DaemonControl {
    fn store(&self) -> Result<&Arc<OverrideStore>, ControlError> {
        self.p.source.overrides().ok_or_else(|| {
            ControlError::refused(
                RefusalReason::Unsupported,
                "this daemon was started without an override tier",
            )
        })
    }

    /// The override set now (empty for a daemon without an override tier).
    pub(super) fn override_set(&self) -> Result<OverrideSet, ControlError> {
        Ok(self.store()?.current())
    }

    /// Where a leaf's effective value came from.
    fn provenance(
        provenance: Option<&ProvenanceMap>,
        path: &LeafPath,
        overrides: &OverrideSet,
        store: &Path,
    ) -> types::LeafProvenance {
        let segments: Vec<&str> = path.segments().collect();
        let Some(p) = provenance.and_then(|m| m.provenance_of(&segments)) else {
            return types::LeafProvenance::Default;
        };
        match (p.tier(), p.source().as_path()) {
            (ConfigTierKind::Bare, _) => types::LeafProvenance::Bare,
            (ConfigTierKind::Discovered, _) => types::LeafProvenance::Discovered,
            (ConfigTierKind::Custom, Some(file)) if file == store => match overrides.get(path) {
                Some((stored, _)) => types::LeafProvenance::Override {
                    path: file.display().to_string(),
                    set_by: stored.set_by.clone(),
                    set_at: stored.set_at,
                },
                None => types::LeafProvenance::Declared {
                    path: file.display().to_string(),
                },
            },
            (ConfigTierKind::Custom, Some(file)) => types::LeafProvenance::Declared {
                path: file.display().to_string(),
            },
            _ => types::LeafProvenance::Default,
        }
    }

    fn leaf_views(
        &self,
        effective: &ResolvedConfig,
        paths: impl IntoIterator<Item = LeafPath>,
    ) -> Result<Vec<types::ConfigLeaf>, ControlError> {
        let store = self.store()?;
        let overrides = store.current();
        let values =
            flatten(&serde_json::to_value(&effective.config).map_err(super::service::internal)?);
        paths
            .into_iter()
            .map(|path| {
                let mutability = classify(&path).ok_or_else(|| unknown_leaf(path.as_str()))?;
                Ok(types::ConfigLeaf {
                    value: values.get(&path).cloned().unwrap_or_default(),
                    provenance: Self::provenance(
                        effective.provenance.as_ref(),
                        &path,
                        &overrides,
                        store.path(),
                    ),
                    mutability: wire(&mutability)?,
                    path: wire(&path)?,
                })
            })
            .collect()
    }

    /// Every leaf (under `prefix`), with its effective value, where it came
    /// from and what changing it does.
    pub(super) fn leaves(
        &self,
        prefix: Option<&str>,
    ) -> Result<Vec<types::ConfigLeaf>, ControlError> {
        let effective = self.effective()?;
        let paths: Vec<LeafPath> = leaves()
            .iter()
            .map(|spec| spec.path.clone())
            .filter(|path| prefix.is_none_or(|p| path.is_under(p)))
            .collect();
        if paths.is_empty() {
            return Err(unknown_leaf(prefix.unwrap_or_default()));
        }
        self.leaf_views(&effective, paths)
    }

    /// One leaf.
    pub(super) fn leaf(&self, path: &LeafPath) -> Result<types::ConfigLeaf, ControlError> {
        if classify(path).is_none() {
            return Err(unknown_leaf(path.as_str()));
        }
        let effective = self.effective()?;
        self.leaf_views(&effective, [path.clone()])?
            .pop()
            .ok_or_else(|| super::service::internal("one leaf asked, one answered"))
    }

    /// How the effective configuration differs from the declared one: a
    /// unified diff against the declared file alone, and, per override, the
    /// value the leaf would have without it — so an override is `shadowing`
    /// when removing it would change its leaf and `redundant` when it would
    /// not. (`null` when the configuration would not resolve without it: an
    /// override repairing the declared file shadows by definition.)
    pub(super) fn drift(&self) -> Result<(String, Vec<types::LeafDrift>), ControlError> {
        let effective = self.effective()?;
        let store = self.store()?;
        let overrides = store.current();
        let flat = |config: &EngenhoConfig| {
            serde_json::to_value(config)
                .map(|v| flatten(&v))
                .map_err(super::service::internal)
        };
        let effective_values = flat(&effective.config)?;
        let unified = self.p.source.resolve_with(None).map_or_else(
            |_| String::new(),
            |declared| {
                effective
                    .config
                    .diff_against(&declared.config)
                    .render_unified()
            },
        );
        let without = |path: &LeafPath| -> Result<serde_json::Value, ControlError> {
            let rest = overrides.with(&Change::Unset { path: path.clone() });
            let Ok(layer) = rest.layer(store.path()) else {
                return Ok(serde_json::Value::Null);
            };
            match self.p.source.resolve_with(Some(&layer)) {
                Ok(resolved) => Ok(flat(&resolved.config)?.remove(path).unwrap_or_default()),
                Err(_) => Ok(serde_json::Value::Null),
            }
        };
        let leaves = overrides
            .entries()
            .into_iter()
            .map(|entry| {
                let declared = without(&entry.path)?;
                let effective = effective_values
                    .get(&entry.path)
                    .cloned()
                    .unwrap_or_default();
                Ok(types::LeafDrift {
                    kind: if declared == effective {
                        types::LeafDriftKind::Redundant
                    } else {
                        types::LeafDriftKind::Shadowing
                    },
                    declared,
                    effective,
                    path: wire(&entry.path)?,
                })
            })
            .collect::<Result<_, ControlError>>()?;
        Ok((unified, leaves))
    }

    /// `config set`: the target must be an overridable leaf, and the value
    /// a value.
    pub(super) async fn set(
        &self,
        by: &Principal,
        path: LeafPath,
        body: types::SetLeafRequest,
    ) -> Result<types::ApplyReport, ControlError> {
        match apply::settable(&path) {
            Err(None) => return Err(unknown_leaf(path.as_str())),
            Err(Some(anchor)) => {
                return Err(ControlError::refused(
                    RefusalReason::NotOverridableLeaf,
                    Refusal::NotOverridable { path, anchor }.to_string(),
                ));
            }
            Ok(_) => {}
        }
        if body.value.is_null() {
            return Err(ControlError::refused_with(
                RefusalReason::InvalidValue,
                format!("null is not a value for {path}"),
                vec![["engenho ctl config unset ", path.as_str()].concat()],
            ));
        }
        let change = Change::Set {
            path,
            stored: Stored {
                value: body.value,
                set_by: by.view(),
                set_at: chrono::Utc::now(),
            },
            durability: if body.persist {
                Durability::Persisted
            } else {
                Durability::Ephemeral
            },
        };
        self.configure(
            Some(change),
            ApplyOptions {
                dry_run: body.dry_run,
                restart_now: restart_now(body.restart_policy),
                precondition: body.precondition_generation,
            },
        )
        .await
    }

    /// The pipeline ([`super::apply`]): plan, gate, commit, apply, report.
    /// `None` is a reload: the override set stays, the declared file is read
    /// again, and the running runtime is brought to the result.
    pub(super) async fn configure(
        &self,
        change: Option<Change>,
        options: ApplyOptions,
    ) -> Result<types::ApplyReport, ControlError> {
        let _one_at_a_time = self.applying.lock().await;
        let store = Arc::clone(self.store()?);
        let current = store.current();
        if let Some(expected) = options.precondition
            && expected != current.generation()
        {
            return Err(ControlError::refused_with(
                RefusalReason::PreconditionFailed,
                format!(
                    "the override set is at generation {}, not {expected}",
                    current.generation()
                ),
                vec!["engenho ctl config overrides".into()],
            ));
        }

        // 1–2. The candidate, resolved with the fold a boot uses.
        let next = change
            .as_ref()
            .map_or_else(|| current.clone(), |c| current.with(c));
        let next_layer = next
            .layer(store.path())
            .map_err(|e| ControlError::refused(RefusalReason::ConfigRejected, e.to_string()))?;
        let after = self.p.source.resolve_with(Some(&next_layer)).map_err(|e| {
            let reason = if matches!(change, Some(Change::Set { .. })) {
                RefusalReason::InvalidValue
            } else {
                RefusalReason::ConfigRejected
            };
            ControlError::refused(reason, e.to_string())
        })?;

        // What it changes: against the effective configuration before the
        // change — or, for a reload, against what the runtime runs.
        let running = self.inspect_config().await?;
        let before = match (&change, running) {
            (None, Some(running)) => Some(running),
            _ => current
                .layer(store.path())
                .ok()
                .and_then(|layer| self.p.source.resolve_with(Some(&layer)).ok())
                .map(|r| r.config),
        };
        let changed = before
            .as_ref()
            .map_or_else(Vec::new, |b| apply::diff(b, &after.config));

        // 3–4. The gate.
        apply::gate(&after.config, &changed).map_err(|r| plan_refusal(&r))?;

        let changed_wire = changed
            .iter()
            .map(change_to_wire)
            .collect::<Result<Vec<_>, _>>()?;
        if options.dry_run {
            return Ok(types::ApplyReport {
                dry_run: true,
                generation: current.generation(),
                changed: changed_wire,
                effect: types::ApplyEffect::PlannedOnly,
            });
        }

        // 6. Commit, then apply.
        let generation = if change.is_some() {
            store.commit(next).map_err(|e| match e {
                OverrideWriteError::Moved { .. } => {
                    ControlError::refused(RefusalReason::PreconditionFailed, e.to_string())
                }
                other => ControlError::blind(BlindReason::IoError, other.to_string()),
            })?
        } else {
            current.generation()
        };
        let outcome = self
            .p
            .supervisor
            .reconfigure(options.restart_now)
            .await
            .map_err(|e| {
                let saved = generation.to_string();
                let mut err = reconfigure_refusal(e);
                if change.is_some() {
                    err = ControlError::blind(
                        BlindReason::Internal,
                        [
                            "the override set was saved at generation ",
                            &saved,
                            ", but: ",
                            &err.to_string(),
                        ]
                        .concat(),
                    );
                }
                err
            })?;
        let effect = apply::effect(&changed, &outcome);
        self.p.supervisor.events().push(DaemonEvent::ConfigApplied {
            generation,
            leaves: changed.iter().map(|c| c.path.clone()).collect(),
            effect: effect.clone(),
        });
        Ok(types::ApplyReport {
            dry_run: false,
            generation,
            changed: changed_wire,
            effect: effect_to_wire(&effect)?,
        })
    }

    /// The running runtime's configuration, when one runs.
    async fn inspect_config(&self) -> Result<Option<EngenhoConfig>, ControlError> {
        Ok(self
            .p
            .supervisor
            .inspect()
            .await
            .map_err(|_| {
                ControlError::blind(BlindReason::Internal, "the daemon's supervisor has ended")
            })?
            .runtime
            .map(|facts| facts.config))
    }
}

/// Parity: the domain's change and mutability spellings are the API's.
#[cfg(test)]
mod tests {
    use engenho_config::Mutability;

    use super::*;
    use crate::control::apply::ApplyEffect;

    #[test]
    fn every_classified_leaf_crosses_to_the_api() {
        for spec in leaves() {
            let change = LeafChange {
                path: spec.path.clone(),
                previous: serde_json::Value::Null,
                next: serde_json::json!(1),
                mutability: spec.mutability,
            };
            change_to_wire(&change).unwrap_or_else(|e| panic!("{}: {e}", spec.path));
        }
        let every_class = [
            Mutability::NextBoot,
            Mutability::RestartRuntime,
            Mutability::Inert {
                why: engenho_config::InertWhy::SingleAcceptedValue,
            },
            Mutability::Respawn {
                set: engenho_config::RespawnSet::NodeLocalListeners,
            },
        ];
        for m in every_class {
            wire::<_, types::Mutability>(&m).unwrap();
        }
    }

    #[test]
    fn every_effect_crosses_to_the_api() {
        for effect in [
            ApplyEffect::NoChange,
            ApplyEffect::PlannedOnly,
            ApplyEffect::AppliedLive,
            ApplyEffect::Respawned {
                children: crate::child::Child::all().collect(),
            },
            ApplyEffect::RestartScheduled,
            ApplyEffect::RestartDeferred {
                leaves: vec![LeafPath::parse("runtime.listen_addr").unwrap()],
            },
            ApplyEffect::NextBoot,
        ] {
            effect_to_wire(&effect).unwrap();
        }
    }

    #[test]
    fn durability_crosses_to_the_api() {
        for d in [Durability::Persisted, Durability::Ephemeral] {
            wire::<_, types::Durability>(&d).unwrap();
        }
    }
}
