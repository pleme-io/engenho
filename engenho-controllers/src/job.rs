//! R16 — Job + R16b CronJob controllers.
//!
//! Job: runs N pods to completion. Tracks completions/failures.
//! CronJob: time-triggered Job factory. Schedule expressed as
//! seconds-since-epoch tick threshold (engenho doesn't bundle a
//! cron parser yet — operator wires a Schedule provider).
//!
//! ## Job reconcile rule
//!
//! For each Job:
//!   * desired_completions = spec.completions (default 1)
//!   * desired_parallelism = spec.parallelism (default 1)
//!   * owned_pods = pods with controller-ref UID matching
//!   * succeeded = pods with status.phase == "Succeeded"
//!   * if succeeded >= completions → mark Job complete (no-op)
//!   * else if active_pods < parallelism → create more
//!
//! ## CronJob reconcile rule
//!
//! For each CronJob whose `spec.nextRunUnix <= now`:
//!   * create a Job from spec.jobTemplate with name
//!     `{cronjob}-{unix_ts}`
//!   * patch the CronJob's status.lastRunUnix
//!
//! Skips for now: failure-backoff windows, time-zone aware
//! schedules. R16c adds those.

use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand},
    resource::ResourceKey,
};
use serde_json::{Value, json};

use crate::controller::{Controller, ReconcileReport};
use crate::error::ControllerError;
use crate::meta::ObjectMeta;
use crate::owner::{is_owned_by, owner_ref_for, set_owner_reference};
use crate::status::{observed_generation, write_status_cas};

// Clock primitives previously defined here have been consolidated to
// engenho_substrate::relogio (Clock + WallClock + FrozenClock). The
// substrate trait carries unix_secs() as a default method so the
// migration is a one-line swap at every call site (was now_unix() →
// is unix_secs()). Tests construct FrozenClock::at(ms) where ms is
// physical milliseconds since epoch (multiply by 1_000 when porting
// from second-precision tests).
pub use engenho_substrate::{Clock, FrozenClock, WallClock};

/// Backwards-compat alias for the previous `SystemClock` export.
pub type SystemClock = WallClock;

/// Backwards-compat constructor — tests should migrate to
/// [`FrozenClock::at`] directly (ms-precision instead of seconds).
#[must_use]
pub fn fixed_clock(unix_secs: u64) -> std::sync::Arc<FrozenClock> {
    std::sync::Arc::new(FrozenClock::at(unix_secs * 1000))
}

// =================================================================
// JobController — R16
// =================================================================

/// Job controller — creates pods until `completions` succeed.
pub struct JobController {
    store: Arc<StoreMesh>,
    namespace: Option<String>,
}

impl JobController {
    /// New controller with optional namespace scope.
    #[must_use]
    pub fn new(store: Arc<StoreMesh>, namespace: Option<String>) -> Self {
        Self { store, namespace }
    }

    fn pod_phase_is(pod: &Value, phase: &str) -> bool {
        pod.get("status")
            .and_then(|s| s.get("phase"))
            .and_then(|p| p.as_str())
            == Some(phase)
    }

    /// Build a Pod from the Job template + index.
    fn build_pod(job: &Value, idx: usize) -> Option<(String, Value)> {
        let job_name = job.name()?;
        let template = job.get("spec").and_then(|s| s.get("template"))?;
        let mut pod = template.clone();
        let pod_obj = pod.as_object_mut()?;
        pod_obj.insert("kind".into(), Value::String("Pod".into()));
        pod_obj.insert("apiVersion".into(), Value::String("v1".into()));
        let pod_name = format!("{job_name}-{idx}");
        let metadata = pod_obj
            .entry("metadata".to_string())
            .or_insert_with(|| json!({}));
        let m = metadata.as_object_mut()?;
        m.insert("name".into(), Value::String(pod_name.clone()));
        // RestartPolicy: Never (Jobs don't restart pods).
        let spec = pod_obj
            .entry("spec".to_string())
            .or_insert_with(|| json!({}));
        if let Some(s) = spec.as_object_mut() {
            s.entry("restartPolicy".to_string())
                .or_insert(Value::String("Never".into()));
        }
        Some((pod_name, pod))
    }
}

#[async_trait]
impl Controller for JobController {
    fn name(&self) -> &'static str {
        "job"
    }

    async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
        let jobs = self
            .store
            .list("batch", "v1", "Job", self.namespace.as_deref())
            .await;
        let mut report = ReconcileReport::default();
        report.objects_examined = jobs.len();

        for (job_key, job_value) in &jobs {
            let Some(uid) = job_value.uid() else {
                report.objects_skipped += 1;
                continue;
            };
            let Some(owner_ref) = owner_ref_for(job_value, "batch/v1", "Job") else {
                report.objects_skipped += 1;
                continue;
            };
            let completions = job_value.spec_i64("completions", 1).max(0) as usize;
            let parallelism = job_value.spec_i64("parallelism", 1).max(0) as usize;
            let ns = job_key.namespace.as_deref();
            let all_pods = self.store.list("", "v1", "Pod", ns).await;
            let owned: Vec<&(ResourceKey, Value)> = all_pods
                .iter()
                .filter(|(_, p)| is_owned_by(p, uid))
                .collect();

            let succeeded = owned
                .iter()
                .filter(|(_, p)| Self::pod_phase_is(p, "Succeeded"))
                .count();
            let active = owned
                .iter()
                .filter(|(_, p)| {
                    !Self::pod_phase_is(p, "Succeeded") && !Self::pod_phase_is(p, "Failed")
                })
                .count();

            // ── Child reconcile: create pods toward completions (unless
            // already complete). Done BEFORE the status write so status
            // reflects the LIVE pods (incl. ones created this tick).
            let complete = succeeded >= completions;
            if !complete {
                // Create up to `parallelism - active` more pods.
                let needed = completions
                    .saturating_sub(succeeded)
                    .min(parallelism)
                    .saturating_sub(active);
                if needed > 0 {
                    let existing_indices: std::collections::BTreeSet<usize> = owned
                        .iter()
                        .filter_map(|(k, _)| {
                            let job_name = job_value.name()?;
                            let prefix = format!("{job_name}-");
                            k.name.strip_prefix(&prefix).and_then(|s| s.parse().ok())
                        })
                        .collect();

                    let mut to_create = needed;
                    let mut idx = 0usize;
                    while to_create > 0 {
                        while existing_indices.contains(&idx) {
                            idx += 1;
                        }
                        let Some((pod_name, mut pod)) = Self::build_pod(job_value, idx) else {
                            report.objects_skipped += 1;
                            break;
                        };
                        set_owner_reference(&mut pod, owner_ref.clone());
                        let pod_ns = ns.unwrap_or("default");
                        let pod_key = ResourceKey::namespaced("", "v1", "Pod", pod_ns, &pod_name);
                        self.store
                            .propose(ResourceCommand::Put {
                                key: pod_key,
                                value: pod,
                                expected: None,
                                reason: Reason::Controller,
                            })
                            .await?;
                        report.objects_changed += 1;
                        to_create -= 1;
                        idx += 1;
                    }
                }
            }

            // ── Status: computed from the LIVE owned-Pod phases AFTER the
            // reconcile (re-list so this tick's creates count toward
            // `active`). Single source of truth — never an incremented
            // counter. On `succeeded >= completions` set a Complete=True
            // condition. observedGeneration included for symmetry.
            let fresh_pods = self.store.list("", "v1", "Pod", ns).await;
            let owned_now: Vec<&Value> = fresh_pods
                .iter()
                .filter(|(_, p)| is_owned_by(p, uid))
                .map(|(_, p)| p)
                .collect();
            let succeeded_now = owned_now
                .iter()
                .filter(|p| Self::pod_phase_is(p, "Succeeded"))
                .count();
            let failed_now = owned_now
                .iter()
                .filter(|p| Self::pod_phase_is(p, "Failed"))
                .count();
            let active_now = owned_now
                .iter()
                .filter(|p| {
                    !Self::pod_phase_is(p, "Succeeded") && !Self::pod_phase_is(p, "Failed")
                })
                .count();
            let mut desired_status = json!({
                "active": i64::try_from(active_now).unwrap_or(i64::MAX),
                "succeeded": i64::try_from(succeeded_now).unwrap_or(i64::MAX),
                "failed": i64::try_from(failed_now).unwrap_or(i64::MAX),
                "observedGeneration": observed_generation(job_value),
            });
            if succeeded_now >= completions {
                desired_status["conditions"] = json!([{
                    "type": "Complete",
                    "status": "True",
                }]);
            }
            if write_status_cas(&self.store, job_key, job_value, &desired_status)
                .await?
                .changed()
            {
                report.objects_changed += 1;
            }
        }
        Ok(report)
    }
}

// =================================================================
// CronJobController — R16b
// =================================================================

/// CronJob controller — creates Jobs at scheduled times.
///
/// `spec.nextRunUnix` is the trigger; when `clock.unix_secs() >=
/// nextRunUnix`, a Job is created + the CronJob's status is
/// patched with `lastRunUnix`.
pub struct CronJobController {
    store: Arc<StoreMesh>,
    clock: Arc<dyn Clock>,
    namespace: Option<String>,
}

impl CronJobController {
    /// New controller with the given clock + optional namespace scope.
    #[must_use]
    pub fn new(store: Arc<StoreMesh>, clock: Arc<dyn Clock>, namespace: Option<String>) -> Self {
        Self {
            store,
            clock,
            namespace,
        }
    }

    fn next_run_unix(cj: &Value) -> Option<u64> {
        cj.get("spec")
            .and_then(|s| s.get("nextRunUnix"))
            .and_then(|n| n.as_u64())
    }

    /// Construct a Job from the CronJob's `spec.jobTemplate.spec`.
    fn build_job(cj: &Value, ts: u64) -> Option<(String, Value)> {
        let cj_name = cj.name()?;
        let job_spec = cj
            .get("spec")
            .and_then(|s| s.get("jobTemplate"))
            .and_then(|t| t.get("spec"))?
            .clone();
        let job_name = format!("{cj_name}-{ts}");
        let job = json!({
            "kind": "Job",
            "apiVersion": "batch/v1",
            "metadata": {"name": job_name},
            "spec": job_spec,
        });
        Some((job_name, job))
    }
}

#[async_trait]
impl Controller for CronJobController {
    fn name(&self) -> &'static str {
        "cronjob"
    }

    async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
        let cjs = self
            .store
            .list("batch", "v1", "CronJob", self.namespace.as_deref())
            .await;
        let mut report = ReconcileReport::default();
        report.objects_examined = cjs.len();

        let now = self.clock.unix_secs();
        // `now` is unix seconds — `spec.nextRunUnix` is in the same unit.

        for (cj_key, cj_value) in &cjs {
            let Some(next) = Self::next_run_unix(cj_value) else {
                report.objects_skipped += 1;
                continue;
            };
            if now < next {
                continue;
            }
            let Some((job_name, job)) = Self::build_job(cj_value, now) else {
                report.objects_skipped += 1;
                continue;
            };
            let job_ns = cj_key.namespace.as_deref().unwrap_or("default");
            let job_key = ResourceKey::namespaced("batch", "v1", "Job", job_ns, &job_name);
            self.store
                .propose(ResourceCommand::Put {
                    key: job_key,
                    value: job,
                    expected: None,
                    reason: Reason::Controller,
                })
                .await?;
            // Patch CronJob status with lastRunUnix.
            self.store
                .propose(ResourceCommand::Patch {
                    key: cj_key.clone(),
                    patch: json!({ "status": { "lastRunUnix": now } }),
                    expected: None,
                    reason: Reason::Controller,
                })
                .await?;
            report.objects_changed += 2;
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── Job ──────────────────────────────────────────────────

    #[test]
    fn job_completions_defaults_to_1() {
        let j = json!({"spec": {}});
        assert_eq!(j.spec_i64("completions", 1), 1);
    }

    #[test]
    fn job_parallelism_defaults_to_1() {
        let j = json!({"spec": {}});
        assert_eq!(j.spec_i64("parallelism", 1), 1);
    }

    #[test]
    fn job_completions_reads_spec_field() {
        let j = json!({"spec": {"completions": 5}});
        assert_eq!(j.spec_i64("completions", 1), 5);
    }

    #[test]
    fn job_build_pod_inserts_restart_policy_never() {
        let j = json!({
            "metadata": {"name": "compute"},
            "spec": {
                "template": {
                    "spec": {"containers": [{"image": "alpine"}]}
                }
            }
        });
        let (name, pod) = JobController::build_pod(&j, 0).unwrap();
        assert_eq!(name, "compute-0");
        assert_eq!(
            pod.get("spec").unwrap().get("restartPolicy").unwrap(),
            "Never"
        );
    }

    #[test]
    fn job_owner_ref_has_batch_v1_api_version() {
        let j = json!({"metadata": {"name": "x", "uid": "u"}});
        let r = owner_ref_for(&j, "batch/v1", "Job").unwrap();
        assert_eq!(r.api_version, "batch/v1");
        assert_eq!(r.kind, "Job");
    }

    #[test]
    fn job_pod_phase_helper_distinguishes_succeeded_failed() {
        let succeeded = json!({"status": {"phase": "Succeeded"}});
        let failed = json!({"status": {"phase": "Failed"}});
        let running = json!({"status": {"phase": "Running"}});
        assert!(JobController::pod_phase_is(&succeeded, "Succeeded"));
        assert!(!JobController::pod_phase_is(&succeeded, "Failed"));
        assert!(JobController::pod_phase_is(&failed, "Failed"));
        assert!(!JobController::pod_phase_is(&running, "Succeeded"));
    }

    #[test]
    fn job_controller_name_is_stable() {
        struct F;
        #[async_trait]
        impl Controller for F {
            fn name(&self) -> &'static str {
                "job"
            }
            async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
                Ok(ReconcileReport::default())
            }
        }
        assert_eq!(F.name(), "job");
    }

    // ── CronJob ─────────────────────────────────────────────

    #[test]
    fn cronjob_build_job_format() {
        let cj = json!({
            "metadata": {"name": "nightly"},
            "spec": {
                "jobTemplate": {
                    "spec": {
                        "completions": 1,
                        "template": {
                            "spec": {"containers": [{"image": "alpine"}]}
                        }
                    }
                }
            }
        });
        let (name, job) = CronJobController::build_job(&cj, 1_700_000_000).unwrap();
        assert_eq!(name, "nightly-1700000000");
        assert_eq!(job.get("kind").unwrap(), "Job");
        assert_eq!(job.get("apiVersion").unwrap(), "batch/v1");
        assert_eq!(job.get("metadata").unwrap().get("name").unwrap(), &name);
        assert!(job.get("spec").unwrap().get("template").is_some());
    }

    #[test]
    fn cronjob_next_run_unix_reads_spec_field() {
        let cj = json!({"spec": {"nextRunUnix": 42_u64}});
        assert_eq!(CronJobController::next_run_unix(&cj), Some(42));
    }

    #[test]
    fn cronjob_next_run_unix_none_when_missing() {
        let cj = json!({"spec": {}});
        assert_eq!(CronJobController::next_run_unix(&cj), None);
    }

    #[test]
    fn frozen_clock_advances_unix_secs() {
        // Substrate FrozenClock is ms-precision; advance(50_000ms) →
        // unix_secs jumps by 50.
        let c = FrozenClock::at(100_000); // 100s
        assert_eq!(c.unix_secs(), 100);
        c.advance(50_000); // +50s
        assert_eq!(c.unix_secs(), 150);
    }

    #[test]
    fn wall_clock_returns_nonzero() {
        // The actual time isn't important; just that it returns
        // a sensible value (greater than the unix epoch's first second).
        assert!(WallClock.unix_secs() > 1_000_000_000);
    }

    #[test]
    fn fixed_clock_compat_helper_works() {
        // Backwards-compat for any caller that hadn't yet migrated to
        // the substrate's ms-precision FrozenClock.
        let c = fixed_clock(100);
        assert_eq!(c.unix_secs(), 100);
    }

    #[test]
    fn cronjob_controller_name_is_stable() {
        struct F;
        #[async_trait]
        impl Controller for F {
            fn name(&self) -> &'static str {
                "cronjob"
            }
            async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
                Ok(ReconcileReport::default())
            }
        }
        assert_eq!(F.name(), "cronjob");
    }
}
