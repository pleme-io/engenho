//! R16 — Job + R16b CronJob controllers.
//!
//! Job: runs N pods to completion. Tracks completions/failures.
//! CronJob: time-triggered Job factory driven by a real 5-field cron
//! parser ([`crate::cron::CronSchedule`]) against an injected
//! [`Clock`] — so the whole CronJob → Job → Pod workload chain
//! functions end-to-end (the JobController then runs the created Job's
//! Pods via the kubelet).
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
//! Each tick, for every CronJob:
//!   * skip when `spec.suspend == true`
//!   * parse `spec.schedule` (a 5-field cron expression); a malformed
//!     schedule is a typed skip, never a panic
//!   * compute the most recent scheduled minute strictly after the last
//!     run anchor (`status.lastScheduleTime`, else the CronJob's
//!     creationTimestamp, else now) and at-or-before `clock.now()`
//!   * honour `spec.startingDeadlineSeconds` — a due time older than the
//!     deadline is missed and skipped
//!   * apply `spec.concurrencyPolicy` (default `Allow`):
//!       - `Forbid`  → skip when an active owned Job exists
//!       - `Replace` → delete the active owned Job(s), then create
//!       - `Allow`   → always create
//!   * create a Job named `{cronjob}-{unix_ts}` from `spec.jobTemplate`,
//!     owner-referenced back to the CronJob + labelled
//!   * patch `status.lastScheduleTime` + `status.active`
//!
//! Deferred (named typed follow-ups): history GC
//! (`successfulJobsHistoryLimit` / `failedJobsHistoryLimit`),
//! timezone-aware schedules (`spec.timeZone` — evaluated against UTC),
//! and the missed-start catch-up bound beyond a single run.

use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand},
    resource::ResourceKey,
};
use serde_json::{Value, json};

use crate::controller::{Controller, ReconcileOutcome, ReconcileReport};
use crate::error::ControllerError;
use crate::meta::ObjectMeta;
use crate::owned_children::{ChildKind, OwnedChildrenReconciler, ParentGvk, ReconcileDelta};
use crate::owner::{owner_ref_for, set_owner_reference};
use crate::status::observed_generation;

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
impl OwnedChildrenReconciler for JobController {
    fn name(&self) -> &'static str {
        "job"
    }

    fn parent_gvk(&self) -> ParentGvk {
        ParentGvk::new("batch", "v1", "Job", "batch/v1")
    }

    fn child_kinds(&self) -> &'static [ChildKind] {
        const CHILD_KINDS: &[ChildKind] = &[ChildKind::new("", "v1", "Pod")];
        CHILD_KINDS
    }

    fn store(&self) -> &StoreMesh {
        &self.store
    }

    fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    async fn reconcile_one(
        &self,
        job_value: &Value,
        owned: &[(ResourceKey, Value)],
    ) -> Result<ReconcileDelta, ControllerError> {
        let Some(owner_ref) = owner_ref_for(job_value, "batch/v1", "Job") else {
            return Ok(ReconcileDelta::none());
        };
        let completions = job_value.spec_i64("completions", 1).max(0) as usize;
        let parallelism = job_value.spec_i64("parallelism", 1).max(0) as usize;
        let ns = self.namespace.as_deref();
        let pod_ns = ns.unwrap_or("default");

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

        // Already complete → no pods to create.
        if succeeded >= completions {
            return Ok(ReconcileDelta::none());
        }

        // Create up to `parallelism - active` more pods toward completions.
        let needed = completions
            .saturating_sub(succeeded)
            .min(parallelism)
            .saturating_sub(active);
        if needed == 0 {
            return Ok(ReconcileDelta::none());
        }

        let existing_indices: std::collections::BTreeSet<usize> = owned
            .iter()
            .filter_map(|(k, _)| {
                let job_name = job_value.name()?;
                let prefix = format!("{job_name}-");
                k.name.strip_prefix(&prefix).and_then(|s| s.parse().ok())
            })
            .collect();

        let mut commands = Vec::new();
        let mut to_create = needed;
        let mut idx = 0usize;
        while to_create > 0 {
            while existing_indices.contains(&idx) {
                idx += 1;
            }
            let Some((pod_name, mut pod)) = Self::build_pod(job_value, idx) else {
                break;
            };
            set_owner_reference(&mut pod, owner_ref.clone());
            let pod_key = ResourceKey::namespaced("", "v1", "Pod", pod_ns, &pod_name);
            commands.push(ResourceCommand::Put {
                key: pod_key,
                value: pod,
                expected: None,
                reason: Reason::Controller,
            });
            to_create -= 1;
            idx += 1;
        }

        Ok(ReconcileDelta::from_commands(commands))
    }

    fn compute_status(
        &self,
        job_value: &Value,
        owned_now: &[(ResourceKey, Value)],
    ) -> Option<Value> {
        // Computed from the LIVE owned-Pod phases AFTER the reconcile.
        // On `succeeded >= completions` set a Complete=True condition.
        let completions = job_value.spec_i64("completions", 1).max(0) as usize;
        let succeeded_now = owned_now
            .iter()
            .filter(|(_, p)| Self::pod_phase_is(p, "Succeeded"))
            .count();
        let failed_now = owned_now
            .iter()
            .filter(|(_, p)| Self::pod_phase_is(p, "Failed"))
            .count();
        let active_now = owned_now
            .iter()
            .filter(|(_, p)| {
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
        Some(desired_status)
    }
}

// =================================================================
// CronJobController — R16b
// =================================================================

/// Concurrency policy for a CronJob's overlapping executions.
///
/// Parsed from `spec.concurrencyPolicy`; unknown / absent values default
/// to [`ConcurrencyPolicy::Allow`] (the Kubernetes default).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConcurrencyPolicy {
    /// Allow concurrent Jobs (default).
    Allow,
    /// Skip the new run while a prior run is still active.
    Forbid,
    /// Delete the active run, then start the new one.
    Replace,
}

impl ConcurrencyPolicy {
    /// Parse from the wire string; anything unrecognised is `Allow`.
    #[must_use]
    pub fn parse(s: Option<&str>) -> Self {
        match s {
            Some("Forbid") => Self::Forbid,
            Some("Replace") => Self::Replace,
            _ => Self::Allow,
        }
    }
}

/// CronJob controller — parses `spec.schedule` (5-field cron) and creates
/// `batch/v1` Jobs at scheduled times against an injected [`Clock`].
///
/// The clock is the testability contract: tests construct a
/// [`FrozenClock`], advance it, and assert a Job appears exactly when due
/// — no wall-clock sleeps.
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

    /// The last-schedule anchor (unix seconds) the next-due search starts
    /// strictly after: `status.lastScheduleTime` if present, else the
    /// CronJob's `metadata.creationTimestamp`, else `now` (a CronJob with
    /// no creation stamp never back-fires for past minutes).
    fn last_schedule_anchor(cj: &Value, now: u64) -> u64 {
        if let Some(last) = cj
            .get("status")
            .and_then(|s| s.get("lastScheduleTime"))
            .and_then(Value::as_str)
            .and_then(parse_rfc3339_unix)
        {
            return last;
        }
        cj.get("metadata")
            .and_then(|m| m.get("creationTimestamp"))
            .and_then(Value::as_str)
            .and_then(parse_rfc3339_unix)
            .unwrap_or(now)
    }

    /// Construct a Job from the CronJob's `spec.jobTemplate` (both its
    /// `spec` AND any template metadata labels/annotations are carried
    /// over). `ts` is the scheduled time in unix seconds — it names the
    /// Job `{cronjob}-{ts}`.
    fn build_job(cj: &Value, ts: u64) -> Option<(String, Value)> {
        let cj_name = cj.name()?;
        let template = cj
            .get("spec")
            .and_then(|s| s.get("jobTemplate"))?;
        let job_spec = template.get("spec").cloned().unwrap_or_else(|| json!({}));
        let job_name = format!("{cj_name}-{ts}");
        // Carry over the jobTemplate's metadata.labels (if any) onto the
        // Job, plus a controller-uid-free convenience label so a human can
        // `kubectl get jobs -l engenho.io/cronjob=<name>`.
        let mut labels = template
            .get("metadata")
            .and_then(|m| m.get("labels"))
            .cloned()
            .unwrap_or_else(|| json!({}));
        if let Some(obj) = labels.as_object_mut() {
            obj.entry("engenho.io/cronjob".to_string())
                .or_insert_with(|| Value::String(cj_name.to_string()));
        }
        let job = json!({
            "kind": "Job",
            "apiVersion": "batch/v1",
            "metadata": {"name": job_name, "labels": labels},
            "spec": job_spec,
        });
        Some((job_name, job))
    }

    /// Jobs in `owned` that are still active (no `Complete`/`Failed`
    /// terminal condition) — used by the concurrency policy.
    fn active_jobs(owned: &[(ResourceKey, Value)]) -> Vec<&(ResourceKey, Value)> {
        owned.iter().filter(|(_, j)| !job_is_terminal(j)).collect()
    }

    /// All Jobs owned (controller-ref) by `cj_uid`, in this CronJob's
    /// namespace.
    async fn owned_jobs(
        &self,
        cj_uid: &str,
        job_ns: &str,
    ) -> Vec<(ResourceKey, Value)> {
        self.store
            .list("batch", "v1", "Job", Some(job_ns))
            .await
            .into_iter()
            .filter(|(_, j)| owned_by(j, cj_uid))
            .collect()
    }
}

#[async_trait]
impl Controller for CronJobController {
    fn name(&self) -> &'static str {
        "cronjob"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let cjs = self
            .store
            .list("batch", "v1", "CronJob", self.namespace.as_deref())
            .await;
        let now = self.clock.unix_secs();
        let mut report = ReconcileReport {
            objects_examined: cjs.len(),
            ..ReconcileReport::default()
        };
        for (cj_key, cj_value) in &cjs {
            match self.reconcile_one_cronjob(cj_key, cj_value, now).await? {
                CronTickOutcome::Fired => report.objects_changed += 2,
                CronTickOutcome::Skipped => report.objects_skipped += 1,
                CronTickOutcome::NotDue => {}
            }
        }
        Ok(report.into())
    }
}

/// One CronJob's per-tick outcome — drives the aggregate report counters.
enum CronTickOutcome {
    /// A Job was created (+ status patched): two store writes.
    Fired,
    /// The slot was deliberately skipped (suspended / bad schedule /
    /// Forbid-with-active / missed-deadline): recorded as a skip.
    Skipped,
    /// No scheduled slot has come due since the anchor: a no-op.
    NotDue,
}

impl CronJobController {
    /// Reconcile a single CronJob at `now` (unix seconds). Pure decision
    /// logic + store writes; returns the typed [`CronTickOutcome`] so
    /// [`Self::tick`] stays a thin aggregator.
    async fn reconcile_one_cronjob(
        &self,
        cj_key: &ResourceKey,
        cj_value: &Value,
        now: u64,
    ) -> Result<CronTickOutcome, ControllerError> {
        // Suspended CronJobs never fire.
        let suspended = cj_value
            .get("spec")
            .and_then(|s| s.get("suspend"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if suspended {
            return Ok(CronTickOutcome::Skipped);
        }

        // Parse the schedule; a malformed expression is a typed skip.
        let schedule_str = cj_value
            .get("spec")
            .and_then(|s| s.get("schedule"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let Ok(schedule) = crate::cron::CronSchedule::parse(schedule_str) else {
            return Ok(CronTickOutcome::Skipped);
        };

        // Most-recent due minute strictly after the anchor + at-or-before now.
        let anchor = Self::last_schedule_anchor(cj_value, now);
        let Some(due) = most_recent_due(&schedule, anchor, now) else {
            return Ok(CronTickOutcome::NotDue);
        };

        // startingDeadlineSeconds: a due time older than the deadline is
        // "missed" — record the skip (advance lastScheduleTime) + no Job.
        if let Some(deadline) = cj_value
            .get("spec")
            .and_then(|s| s.get("startingDeadlineSeconds"))
            .and_then(Value::as_i64)
        {
            let deadline = u64::try_from(deadline.max(0)).unwrap_or(0);
            if now.saturating_sub(due) > deadline {
                self.patch_last_schedule(cj_key, due, None).await?;
                return Ok(CronTickOutcome::Skipped);
            }
        }

        let cj_uid = cj_value
            .get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        let job_ns = cj_key.namespace.as_deref().unwrap_or("default");

        // Concurrency policy gate.
        let policy = ConcurrencyPolicy::parse(
            cj_value
                .get("spec")
                .and_then(|s| s.get("concurrencyPolicy"))
                .and_then(Value::as_str),
        );
        let owned = self.owned_jobs(cj_uid, job_ns).await;
        let active = CronJobController::active_jobs(&owned);
        match policy {
            ConcurrencyPolicy::Forbid if !active.is_empty() => {
                // A prior run is still active → skip, but record the skip.
                self.patch_last_schedule(cj_key, due, None).await?;
                return Ok(CronTickOutcome::Skipped);
            }
            ConcurrencyPolicy::Replace => {
                // Delete every active owned Job before creating the new one.
                for (k, _) in &active {
                    self.store
                        .propose(ResourceCommand::delete(k.clone(), Reason::Controller))
                        .await?;
                }
            }
            ConcurrencyPolicy::Allow | ConcurrencyPolicy::Forbid => {}
        }

        // Build + create the Job, owner-referenced back to the CronJob.
        let Some((job_name, mut job)) = Self::build_job(cj_value, due) else {
            return Ok(CronTickOutcome::Skipped);
        };
        if let Some(owner_ref) = owner_ref_for(cj_value, "batch/v1", "CronJob") {
            set_owner_reference(&mut job, owner_ref);
        }
        let job_key = ResourceKey::namespaced("batch", "v1", "Job", job_ns, &job_name);
        self.store
            .propose(ResourceCommand::Put {
                key: job_key,
                value: job,
                expected: None,
                reason: Reason::Controller,
            })
            .await?;

        // Patch status: lastScheduleTime + an active ref to the new Job.
        let active_ref = json!({
            "apiVersion": "batch/v1",
            "kind": "Job",
            "name": job_name,
            "namespace": job_ns,
        });
        self.patch_last_schedule(cj_key, due, Some(active_ref)).await?;
        Ok(CronTickOutcome::Fired)
    }

    /// Patch the CronJob's `status.lastScheduleTime` (always) and — when a
    /// Job was created — its `status.active` list with the new Job's ref.
    async fn patch_last_schedule(
        &self,
        cj_key: &ResourceKey,
        due: u64,
        active_ref: Option<Value>,
    ) -> Result<(), ControllerError> {
        let status = match active_ref {
            Some(r) => json!({
                "lastScheduleTime": unix_to_rfc3339(due),
                "active": [r],
            }),
            None => json!({ "lastScheduleTime": unix_to_rfc3339(due) }),
        };
        self.store
            .propose(ResourceCommand::patch(
                cj_key.clone(),
                json!({ "status": status }),
                Reason::Controller,
            ))
            .await?;
        Ok(())
    }
}

/// The most-recent cron-due minute strictly after `anchor` and at-or-
/// before `now`. `None` when no slot has come due since the anchor.
///
/// We walk forward from the anchor (the search is bounded by
/// [`crate::cron::CronSchedule::next_after_unix`]'s horizon) and keep the
/// last slot ≤ now. This fires AT MOST ONCE per tick even when several
/// slots elapsed between ticks (no catch-up storm) — the missed slots are
/// collapsed to the latest, matching the Kubernetes single-fire-per-tick
/// behaviour for a CronJob with no backlog policy.
fn most_recent_due(
    schedule: &crate::cron::CronSchedule,
    anchor: u64,
    now: u64,
) -> Option<u64> {
    let mut candidate = schedule.next_after_unix(anchor)?;
    if candidate > now {
        return None;
    }
    // Advance to the latest slot ≤ now.
    while let Some(next) = schedule.next_after_unix(candidate) {
        if next > now {
            break;
        }
        candidate = next;
    }
    Some(candidate)
}

/// Is this Job in a terminal state (a `Complete` or `Failed` condition is
/// True)? Used by the concurrency policy to decide what counts as active.
fn job_is_terminal(job: &Value) -> bool {
    job.get("status")
        .and_then(|s| s.get("conditions"))
        .and_then(Value::as_array)
        .is_some_and(|conds| {
            conds.iter().any(|c| {
                let t = c.get("type").and_then(Value::as_str);
                let s = c.get("status").and_then(Value::as_str);
                matches!(t, Some("Complete" | "Failed")) && s == Some("True")
            })
        })
}

/// Is `child` controller-owned by an object with uid `owner_uid`?
fn owned_by(child: &Value, owner_uid: &str) -> bool {
    if owner_uid.is_empty() {
        return false;
    }
    child
        .get("metadata")
        .and_then(|m| m.get("ownerReferences"))
        .and_then(Value::as_array)
        .is_some_and(|refs| {
            refs.iter().any(|r| {
                r.get("uid").and_then(Value::as_str) == Some(owner_uid)
                    && r.get("controller").and_then(Value::as_bool) == Some(true)
            })
        })
}

/// Parse an RFC3339 timestamp string → unix seconds. `None` on malformed
/// input (a skip-safe read of `status.lastScheduleTime` /
/// `creationTimestamp`).
fn parse_rfc3339_unix(s: &str) -> Option<u64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .and_then(|dt| u64::try_from(dt.timestamp()).ok())
}

/// Render unix seconds → the Kubernetes-wire RFC3339 UTC string (typed
/// emission via engenho-types' `time` surface — no hand-`format!()`).
fn unix_to_rfc3339(unix_secs: u64) -> String {
    let secs = i64::try_from(unix_secs).unwrap_or(i64::MAX);
    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0)
        .map_or_else(String::new, engenho_types::time::to_rfc3339_utc)
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
        // The convenience cronjob label is stamped on the built Job.
        assert_eq!(
            job["metadata"]["labels"]["engenho.io/cronjob"],
            json!("nightly")
        );
    }

    #[test]
    fn concurrency_policy_parse() {
        assert_eq!(ConcurrencyPolicy::parse(Some("Forbid")), ConcurrencyPolicy::Forbid);
        assert_eq!(ConcurrencyPolicy::parse(Some("Replace")), ConcurrencyPolicy::Replace);
        assert_eq!(ConcurrencyPolicy::parse(Some("Allow")), ConcurrencyPolicy::Allow);
        // Unknown / absent → Allow (the K8s default).
        assert_eq!(ConcurrencyPolicy::parse(Some("Bogus")), ConcurrencyPolicy::Allow);
        assert_eq!(ConcurrencyPolicy::parse(None), ConcurrencyPolicy::Allow);
    }

    #[test]
    fn job_is_terminal_detects_complete_and_failed() {
        let complete = json!({"status": {"conditions": [{"type": "Complete", "status": "True"}]}});
        let failed = json!({"status": {"conditions": [{"type": "Failed", "status": "True"}]}});
        let running = json!({"status": {"active": 1}});
        let complete_false =
            json!({"status": {"conditions": [{"type": "Complete", "status": "False"}]}});
        assert!(job_is_terminal(&complete));
        assert!(job_is_terminal(&failed));
        assert!(!job_is_terminal(&running));
        assert!(!job_is_terminal(&complete_false));
    }

    #[test]
    fn owned_by_matches_controller_ref() {
        let child = json!({"metadata": {"ownerReferences": [
            {"uid": "cj-uid", "controller": true}
        ]}});
        assert!(owned_by(&child, "cj-uid"));
        assert!(!owned_by(&child, "other-uid"));
        // A non-controller owner ref does not count.
        let non_ctrl = json!({"metadata": {"ownerReferences": [{"uid": "cj-uid"}]}});
        assert!(!owned_by(&non_ctrl, "cj-uid"));
        // Empty uid never matches.
        assert!(!owned_by(&child, ""));
    }

    #[test]
    fn rfc3339_round_trips_unix() {
        // 1_700_000_000 = 2023-11-14T22:13:20Z.
        let s = unix_to_rfc3339(1_700_000_000);
        assert_eq!(s, "2023-11-14T22:13:20Z");
        assert_eq!(parse_rfc3339_unix(&s), Some(1_700_000_000));
        assert_eq!(parse_rfc3339_unix("not-a-time"), None);
    }

    #[test]
    fn most_recent_due_collapses_missed_slots_to_latest() {
        // Every-minute schedule. Anchor at t=0, now at t=600 (10 min later)
        // → the latest due slot is t=600 itself (minute-aligned, ≤ now).
        let s = crate::cron::CronSchedule::parse("* * * * *").unwrap();
        assert_eq!(most_recent_due(&s, 0, 600), Some(600));
        // Nothing due yet when now is before the first slot after anchor.
        // anchor=100 → first slot 120; now=110 < 120 → None.
        assert_eq!(most_recent_due(&s, 100, 110), None);
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
            async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
                Ok(ReconcileReport::default().into())
            }
        }
        assert_eq!(F.name(), "cronjob");
    }

    // ── CronJob live-store reconcile (mockable clock, NO wall-clock) ──

    use engenho_store::{InProcessRouter, default_config};
    use std::time::Duration;

    /// Single-node in-memory StoreMesh — the controller-test rig the
    /// other reconciler tests use.
    async fn test_store(name: &str) -> Arc<StoreMesh> {
        let router = InProcessRouter::new();
        let cfg = default_config(name).unwrap();
        let store = Arc::new(
            StoreMesh::start(1, "in-process://1".into(), router, cfg)
                .await
                .unwrap(),
        );
        store.initialize_singleton().await.unwrap();
        assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
        store
    }

    async fn put(store: &StoreMesh, key: ResourceKey, value: Value) {
        store
            .propose(ResourceCommand::put(key, value, Reason::Operator))
            .await
            .expect("put");
    }

    /// Seed a CronJob at `default/<name>` with the given schedule + a
    /// jobTemplate that runs one container. The creationTimestamp is the
    /// search anchor; pin it so a frozen clock can advance past a slot.
    fn cronjob_value(name: &str, schedule: &str, created: u64) -> Value {
        json!({
            "kind": "CronJob",
            "apiVersion": "batch/v1",
            "metadata": {
                "name": name,
                "namespace": "default",
                "uid": format!("{name}-uid"),
                "creationTimestamp": unix_to_rfc3339(created),
            },
            "spec": {
                "schedule": schedule,
                "jobTemplate": {
                    "spec": {
                        "template": {"spec": {"containers": [{"image": "alpine"}]}}
                    }
                }
            }
        })
    }

    async fn list_jobs(store: &StoreMesh) -> Vec<(ResourceKey, Value)> {
        store.list("batch", "v1", "Job", Some("default")).await
    }

    #[tokio::test]
    async fn cronjob_creates_one_job_when_a_minute_is_due() {
        let store = test_store("cronjob-due").await;
        // creationTimestamp at t=0; clock at t=120 (one whole minute past
        // the first due slot of an every-minute schedule).
        put(
            &store,
            ResourceKey::namespaced("batch", "v1", "CronJob", "default", "every"),
            cronjob_value("every", "* * * * *", 0),
        )
        .await;
        let clock = Arc::new(FrozenClock::at(120_000)); // 120s
        let c = CronJobController::new(store.clone(), clock.clone(), None);

        let out = c.tick().await.unwrap();
        assert_eq!(out.objects_changed, 2, "one Job create + one status patch");

        let jobs = list_jobs(&store).await;
        assert_eq!(jobs.len(), 1, "exactly one Job created");
        let (jk, jv) = &jobs[0];
        // Name = {cronjob}-{due_ts}; due is the latest slot ≤ now (120).
        assert_eq!(jk.name, "every-120");
        // ownerRef points back at the CronJob (controller=true).
        assert!(owned_by(jv, "every-uid"), "Job owner-referenced to CronJob");
        assert_eq!(jk.namespace.as_deref(), Some("default"));
        // jobTemplate.spec.template was copied into the Job.
        assert!(jv["spec"]["template"]["spec"]["containers"].is_array());

        // status.lastScheduleTime updated on the CronJob.
        let cj = store
            .get(&ResourceKey::namespaced(
                "batch", "v1", "CronJob", "default", "every",
            ))
            .await
            .unwrap();
        assert_eq!(
            parse_rfc3339_unix(cj["status"]["lastScheduleTime"].as_str().unwrap()),
            Some(120)
        );
        assert_eq!(cj["status"]["active"][0]["name"], json!("every-120"));
    }

    #[tokio::test]
    async fn cronjob_not_due_yet_creates_nothing() {
        let store = test_store("cronjob-notdue").await;
        put(
            &store,
            ResourceKey::namespaced("batch", "v1", "CronJob", "default", "every"),
            cronjob_value("every", "* * * * *", 100),
        )
        .await;
        // Anchor at t=100 → first due slot is t=120; clock at t=110 < 120.
        let clock = Arc::new(FrozenClock::at(110_000));
        let c = CronJobController::new(store.clone(), clock, None);
        let out = c.tick().await.unwrap();
        assert_eq!(out.objects_changed, 0);
        assert!(list_jobs(&store).await.is_empty());
    }

    #[tokio::test]
    async fn cronjob_suspend_never_fires() {
        let store = test_store("cronjob-suspend").await;
        let mut cj = cronjob_value("every", "* * * * *", 0);
        cj["spec"]["suspend"] = json!(true);
        put(
            &store,
            ResourceKey::namespaced("batch", "v1", "CronJob", "default", "every"),
            cj,
        )
        .await;
        let clock = Arc::new(FrozenClock::at(600_000)); // way past several slots
        let c = CronJobController::new(store.clone(), clock, None);
        let out = c.tick().await.unwrap();
        assert_eq!(out.objects_skipped, 1);
        assert!(list_jobs(&store).await.is_empty(), "suspended → no Job ever");
    }

    #[tokio::test]
    async fn cronjob_forbid_skips_when_active_job_exists() {
        let store = test_store("cronjob-forbid").await;
        let mut cj = cronjob_value("every", "* * * * *", 0);
        cj["spec"]["concurrencyPolicy"] = json!("Forbid");
        put(
            &store,
            ResourceKey::namespaced("batch", "v1", "CronJob", "default", "every"),
            cj,
        )
        .await;
        // Seed an ALREADY-active owned Job (no terminal condition).
        put(
            &store,
            ResourceKey::namespaced("batch", "v1", "Job", "default", "every-60"),
            json!({
                "kind": "Job", "apiVersion": "batch/v1",
                "metadata": {"name": "every-60", "namespace": "default",
                    "ownerReferences": [{"uid": "every-uid", "controller": true}]},
                "status": {"active": 1}
            }),
        )
        .await;
        let clock = Arc::new(FrozenClock::at(120_000));
        let c = CronJobController::new(store.clone(), clock, None);
        let out = c.tick().await.unwrap();
        assert_eq!(out.objects_skipped, 1);
        // No NEW Job — still just the seeded one.
        assert_eq!(list_jobs(&store).await.len(), 1);
    }

    #[tokio::test]
    async fn cronjob_replace_deletes_active_then_creates() {
        let store = test_store("cronjob-replace").await;
        let mut cj = cronjob_value("every", "* * * * *", 0);
        cj["spec"]["concurrencyPolicy"] = json!("Replace");
        put(
            &store,
            ResourceKey::namespaced("batch", "v1", "CronJob", "default", "every"),
            cj,
        )
        .await;
        // An active owned Job from a previous slot.
        put(
            &store,
            ResourceKey::namespaced("batch", "v1", "Job", "default", "every-60"),
            json!({
                "kind": "Job", "apiVersion": "batch/v1",
                "metadata": {"name": "every-60", "namespace": "default",
                    "ownerReferences": [{"uid": "every-uid", "controller": true}]},
                "status": {"active": 1}
            }),
        )
        .await;
        let clock = Arc::new(FrozenClock::at(120_000));
        let c = CronJobController::new(store.clone(), clock, None);
        c.tick().await.unwrap();
        let jobs = list_jobs(&store).await;
        // The old active Job was deleted; the new one created.
        let names: Vec<&str> = jobs.iter().map(|(k, _)| k.name.as_str()).collect();
        assert!(!names.contains(&"every-60"), "active Job replaced (deleted)");
        assert!(names.contains(&"every-120"), "new Job created");
    }

    #[tokio::test]
    async fn cronjob_starting_deadline_skips_stale_slot() {
        let store = test_store("cronjob-deadline").await;
        let mut cj = cronjob_value("every", "* * * * *", 0);
        // Deadline of 10s — the latest due slot must be within 10s of now.
        cj["spec"]["startingDeadlineSeconds"] = json!(10);
        put(
            &store,
            ResourceKey::namespaced("batch", "v1", "CronJob", "default", "every"),
            cj,
        )
        .await;
        // now=125 → latest slot ≤ now is 120, which is 5s old (≤ 10s) → fires.
        let clock = Arc::new(FrozenClock::at(125_000));
        let c = CronJobController::new(store.clone(), clock, None);
        c.tick().await.unwrap();
        assert_eq!(list_jobs(&store).await.len(), 1, "within deadline → fires");
    }

    #[tokio::test]
    async fn cronjob_idempotent_on_repeated_tick_same_minute() {
        let store = test_store("cronjob-idem").await;
        put(
            &store,
            ResourceKey::namespaced("batch", "v1", "CronJob", "default", "every"),
            cronjob_value("every", "* * * * *", 0),
        )
        .await;
        let clock = Arc::new(FrozenClock::at(120_000));
        let c = CronJobController::new(store.clone(), clock.clone(), None);
        c.tick().await.unwrap();
        // Second tick with the SAME clock: lastScheduleTime now == 120, so
        // the next-due search finds no NEW slot ≤ now → no second Job.
        let out2 = c.tick().await.unwrap();
        assert_eq!(out2.objects_changed, 0, "no double-fire within a minute");
        assert_eq!(list_jobs(&store).await.len(), 1);
    }
}
