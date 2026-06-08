//! M0.1 item 8 — status-subresource writes + CAS, in isolation.
//!
//! Each link of item-8 proven against a real in-memory `StoreMesh`:
//!
//!   * status reflects the LIVE converged children (RS / Deployment /
//!     StatefulSet / Job) — counted from the store, never an internal
//!     counter;
//!   * `status.observedGeneration == metadata.generation` after a tick;
//!   * the status write is idempotent (a no-op tick proposes nothing);
//!   * the status write is optimistic-concurrency safe — a concurrent
//!     spec change committed between the controller's read and its status
//!     write is NEVER clobbered (the stale-rv patch conflicts + drops);
//!   * the shared [`write_status_cas`] returns the typed
//!     [`StatusWriteOutcome`] (Written / NoChange / Conflict).

use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::{
    Controller, DeploymentController, JobController, ReplicaSetController, StatefulSetController,
    is_owned_by, status::{StatusWriteOutcome, write_status_cas},
};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand, ResourceOp},
    default_config,
};
use serde_json::{Value, json};

async fn boot_store() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("status-m0-1").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

async fn teardown(store: Arc<StoreMesh>) {
    let mesh = Arc::try_unwrap(store).ok().expect("only owner");
    mesh.terminate().await.unwrap();
}

/// Put a value (operator, unconditional) + read back the assigned uid.
async fn put_and_uid(store: &StoreMesh, key: &ResourceKey, value: Value) -> String {
    store
        .propose(ResourceCommand::Put {
            key: key.clone(),
            value,
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    store
        .get(key)
        .await
        .unwrap()
        .get("metadata")
        .unwrap()
        .get("uid")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string()
}

/// Mark every Pod owned by `owner_uid` Ready (Put a Ready=True condition
/// + Running phase). Returns the count marked.
async fn mark_owned_pods_ready(store: &StoreMesh, owner_uid: &str) -> usize {
    let pods = store.list("", "v1", "Pod", Some("default")).await;
    let mut n = 0;
    for (key, _) in pods.iter().filter(|(_, p)| is_owned_by(p, owner_uid)) {
        store
            .propose(ResourceCommand::Patch {
                key: key.clone(),
                patch: json!({
                    "status": {
                        "phase": "Running",
                        "conditions": [{"type": "Ready", "status": "True"}]
                    }
                }),
                patch_type: engenho_store::command::PatchType::Merge,
                apply: None,
                expected: None,
                reason: Reason::Operator,
            })
            .await
            .unwrap();
        n += 1;
    }
    n
}

fn status_i64(v: &Value, field: &str) -> i64 {
    v.get("status")
        .and_then(|s| s.get(field))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_else(|| panic!("status.{field} missing in {v}"))
}

fn metadata_generation(v: &Value) -> i64 {
    v.get("metadata")
        .and_then(|m| m.get("generation"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_else(|| panic!("metadata.generation missing"))
}

// ──────────────────────────────────────────────────────────────────────
// ReplicaSet: status reflects live owned children + observedGeneration
// ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn replicaset_status_reflects_live_ready_pods() {
    let store = boot_store().await;
    let rs_key = ResourceKey::namespaced("apps", "v1", "ReplicaSet", "default", "web");
    let rs_uid = put_and_uid(
        &store,
        &rs_key,
        json!({
            "kind": "ReplicaSet",
            "apiVersion": "apps/v1",
            "metadata": {"name": "web"},
            "spec": {
                "replicas": 3,
                "selector": {"matchLabels": {"app": "web"}},
                "template": {"metadata": {"labels": {"app": "web"}}, "spec": {}}
            }
        }),
    )
    .await;

    let rc = ReplicaSetController::new(store.clone(), Some("default".into()));
    // Tick 1 creates 3 pods + writes status (replicas:3, readyReplicas:0).
    rc.tick().await.unwrap();
    let rs = store.get(&rs_key).await.unwrap();
    assert_eq!(status_i64(&rs, "replicas"), 3);
    assert_eq!(status_i64(&rs, "readyReplicas"), 0);
    assert_eq!(status_i64(&rs, "availableReplicas"), 0);
    assert_eq!(status_i64(&rs, "fullyLabeledReplicas"), 3);
    // observedGeneration tracks metadata.generation (status writes don't
    // bump generation, so they stay aligned).
    assert_eq!(status_i64(&rs, "observedGeneration"), metadata_generation(&rs));

    // Mark pods Ready → readyReplicas/availableReplicas converge to 3.
    assert_eq!(mark_owned_pods_ready(&store, &rs_uid).await, 3);
    rc.tick().await.unwrap();
    let rs = store.get(&rs_key).await.unwrap();
    assert_eq!(status_i64(&rs, "replicas"), 3);
    assert_eq!(status_i64(&rs, "readyReplicas"), 3);
    assert_eq!(status_i64(&rs, "availableReplicas"), 3);
    assert_eq!(status_i64(&rs, "observedGeneration"), metadata_generation(&rs));

    drop(rc);
    teardown(store).await;
}

#[tokio::test]
async fn replicaset_status_write_is_idempotent_at_fixpoint() {
    let store = boot_store().await;
    let rs_key = ResourceKey::namespaced("apps", "v1", "ReplicaSet", "default", "stable");
    let _uid = put_and_uid(
        &store,
        &rs_key,
        json!({
            "kind": "ReplicaSet", "apiVersion": "apps/v1",
            "metadata": {"name": "stable"},
            "spec": {"replicas": 2, "selector": {"matchLabels": {"app": "s"}},
                     "template": {"metadata": {"labels": {"app": "s"}}, "spec": {}}}
        }),
    )
    .await;
    let rc = ReplicaSetController::new(store.clone(), Some("default".into()));
    rc.tick().await.unwrap(); // create + first status
    // Reach the fixpoint: a tick that changes nothing.
    let _ = rc.tick().await.unwrap();
    let rev_before = store.current_catalog().await.revision();
    let report = rc.tick().await.unwrap();
    let rev_after = store.current_catalog().await.revision();
    assert_eq!(
        report.objects_changed, 0,
        "fixpoint tick proposes nothing"
    );
    assert_eq!(
        rev_before, rev_after,
        "no revision advance on a no-op status tick (the hot-loop defense)"
    );
    drop(rc);
    teardown(store).await;
}

// ──────────────────────────────────────────────────────────────────────
// Deployment: status aggregates over current-template owned RS(es)
// ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn deployment_status_aggregates_replicaset_status() {
    let store = boot_store().await;
    let dep_key = ResourceKey::namespaced("apps", "v1", "Deployment", "default", "podinfo");
    let dep_uid = put_and_uid(
        &store,
        &dep_key,
        json!({
            "kind": "Deployment", "apiVersion": "apps/v1",
            "metadata": {"name": "podinfo"},
            "spec": {
                "replicas": 2,
                "selector": {"matchLabels": {"app": "podinfo"}},
                "template": {"metadata": {"labels": {"app": "podinfo"}}, "spec": {}}
            }
        }),
    )
    .await;

    let dc = DeploymentController::new(store.clone(), Some("default".into()));
    let rc = ReplicaSetController::new(store.clone(), Some("default".into()));

    // D tick → RS created; RS tick → 2 pods created + RS status written.
    dc.tick().await.unwrap();
    rc.tick().await.unwrap();

    // Find the RS uid + mark its pods Ready, RS tick again → RS status
    // readyReplicas:2.
    let rs_list = store.list("apps", "v1", "ReplicaSet", Some("default")).await;
    let (_, rs_value) = rs_list
        .iter()
        .find(|(_, r)| is_owned_by(r, &dep_uid))
        .expect("RS owned by deployment");
    let rs_uid = rs_value
        .get("metadata")
        .unwrap()
        .get("uid")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(mark_owned_pods_ready(&store, &rs_uid).await, 2);
    rc.tick().await.unwrap();

    // D tick aggregates the RS status into the Deployment status.
    dc.tick().await.unwrap();
    let dep = store.get(&dep_key).await.unwrap();
    assert_eq!(status_i64(&dep, "replicas"), 2);
    assert_eq!(status_i64(&dep, "readyReplicas"), 2);
    assert_eq!(status_i64(&dep, "availableReplicas"), 2);
    assert_eq!(status_i64(&dep, "updatedReplicas"), 2);
    assert_eq!(
        status_i64(&dep, "observedGeneration"),
        metadata_generation(&dep),
        "Deployment observedGeneration tracks its own generation"
    );

    drop(dc);
    drop(rc);
    teardown(store).await;
}

// ──────────────────────────────────────────────────────────────────────
// StatefulSet: ordinal pods → replicas/ready status
// ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn statefulset_status_reflects_ordinal_pods() {
    let store = boot_store().await;
    let sts_key = ResourceKey::namespaced("apps", "v1", "StatefulSet", "default", "db");
    let sts_uid = put_and_uid(
        &store,
        &sts_key,
        json!({
            "kind": "StatefulSet", "apiVersion": "apps/v1",
            "metadata": {"name": "db"},
            "spec": {"replicas": 3,
                     "template": {"metadata": {"labels": {"app": "db"}}, "spec": {}}}
        }),
    )
    .await;

    let sc = StatefulSetController::new(store.clone(), Some("default".into()));
    sc.tick().await.unwrap(); // create db-0, db-1, db-2 + status
    let sts = store.get(&sts_key).await.unwrap();
    assert_eq!(status_i64(&sts, "replicas"), 3);
    assert_eq!(status_i64(&sts, "readyReplicas"), 0);
    assert_eq!(status_i64(&sts, "currentReplicas"), 3);
    assert_eq!(status_i64(&sts, "updatedReplicas"), 3);

    assert_eq!(mark_owned_pods_ready(&store, &sts_uid).await, 3);
    sc.tick().await.unwrap();
    let sts = store.get(&sts_key).await.unwrap();
    assert_eq!(status_i64(&sts, "readyReplicas"), 3);
    assert_eq!(status_i64(&sts, "availableReplicas"), 3);
    assert_eq!(status_i64(&sts, "observedGeneration"), metadata_generation(&sts));

    drop(sc);
    teardown(store).await;
}

// ──────────────────────────────────────────────────────────────────────
// Job: active/succeeded/failed from pod phases + Complete condition
// ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn job_status_tracks_phases_and_completes() {
    let store = boot_store().await;
    let job_key = ResourceKey::namespaced("batch", "v1", "Job", "default", "batch");
    let job_uid = put_and_uid(
        &store,
        &job_key,
        json!({
            "kind": "Job", "apiVersion": "batch/v1",
            "metadata": {"name": "batch"},
            "spec": {"completions": 1, "parallelism": 1,
                     "template": {"spec": {"containers": [{"image": "alpine"}]}}}
        }),
    )
    .await;

    let jc = JobController::new(store.clone(), Some("default".into()));
    // Tick 1 creates 1 pod (active) + writes status active:1.
    jc.tick().await.unwrap();
    let job = store.get(&job_key).await.unwrap();
    assert_eq!(status_i64(&job, "active"), 1);
    assert_eq!(status_i64(&job, "succeeded"), 0);
    assert_eq!(status_i64(&job, "failed"), 0);

    // Mark the owned pod Succeeded.
    let pods = store.list("", "v1", "Pod", Some("default")).await;
    let (pod_key, _) = pods
        .iter()
        .find(|(_, p)| is_owned_by(p, &job_uid))
        .expect("owned job pod");
    store
        .propose(ResourceCommand::patch(
            pod_key.clone(),
            json!({"status": {"phase": "Succeeded"}}),
            Reason::Operator,
        ))
        .await
        .unwrap();

    // Tick 2: succeeded >= completions → Complete=True condition.
    jc.tick().await.unwrap();
    let job = store.get(&job_key).await.unwrap();
    assert_eq!(status_i64(&job, "succeeded"), 1);
    assert_eq!(status_i64(&job, "active"), 0);
    let complete = job
        .get("status")
        .and_then(|s| s.get("conditions"))
        .and_then(|c| c.as_array())
        .is_some_and(|conds| {
            conds.iter().any(|c| {
                c.get("type").and_then(|t| t.as_str()) == Some("Complete")
                    && c.get("status").and_then(|s| s.as_str()) == Some("True")
            })
        });
    assert!(complete, "Job should carry Complete=True once succeeded >= completions");

    drop(jc);
    teardown(store).await;
}

// ──────────────────────────────────────────────────────────────────────
// CAS / no-clobber: a status write must NEVER clobber a concurrent spec
// change, and the shared helper returns the typed outcome.
// ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn status_write_conflicts_with_concurrent_spec_change_and_does_not_clobber() {
    let store = boot_store().await;
    let rs_key = ResourceKey::namespaced("apps", "v1", "ReplicaSet", "default", "race");
    let _uid = put_and_uid(
        &store,
        &rs_key,
        json!({
            "kind": "ReplicaSet", "apiVersion": "apps/v1",
            "metadata": {"name": "race"},
            "spec": {"replicas": 2, "selector": {"matchLabels": {"app": "r"}},
                     "template": {"metadata": {"labels": {"app": "r"}}, "spec": {}}}
        }),
    )
    .await;

    // The controller reads the live RS this tick (carrying its current rv).
    let parent = store.get(&rs_key).await.unwrap();

    // A concurrent operator scale commits BEFORE the controller's status
    // write — advancing mod_revision so the stale rv no longer matches.
    store
        .propose(ResourceCommand::patch(
            rs_key.clone(),
            json!({"spec": {"replicas": 5}}),
            Reason::Operator,
        ))
        .await
        .unwrap();

    // Now the controller issues its status write against the STALE parent
    // (rv captured before the scale). It must conflict — never clobber.
    let desired = json!({"replicas": 2, "readyReplicas": 0, "observedGeneration": 1});
    let outcome = write_status_cas(&store, &rs_key, &parent, &desired)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        StatusWriteOutcome::Conflict,
        "stale-rv status write must conflict (not clobber)"
    );

    // The concurrent spec change survives intact + no stale status landed.
    let live = store.get(&rs_key).await.unwrap();
    assert_eq!(
        live.get("spec").unwrap().get("replicas").unwrap(),
        5,
        "the concurrent spec change was NOT clobbered"
    );
    assert!(
        live.get("status").is_none(),
        "the stale status write did not land"
    );

    drop(store);
}

#[tokio::test]
async fn status_write_succeeds_with_current_rv_then_noop_on_reissue() {
    let store = boot_store().await;
    let rs_key = ResourceKey::namespaced("apps", "v1", "ReplicaSet", "default", "ok");
    let _uid = put_and_uid(
        &store,
        &rs_key,
        json!({
            "kind": "ReplicaSet", "apiVersion": "apps/v1",
            "metadata": {"name": "ok"},
            "spec": {"replicas": 1, "selector": {"matchLabels": {"app": "o"}},
                     "template": {"metadata": {"labels": {"app": "o"}}, "spec": {}}}
        }),
    )
    .await;

    // Positive: current rv → Written.
    let parent = store.get(&rs_key).await.unwrap();
    let generation = metadata_generation(&parent);
    let desired = json!({
        "replicas": 0, "readyReplicas": 0, "availableReplicas": 0,
        "fullyLabeledReplicas": 0, "observedGeneration": generation
    });
    let outcome = write_status_cas(&store, &rs_key, &parent, &desired)
        .await
        .unwrap();
    assert_eq!(outcome, StatusWriteOutcome::Written);

    // Re-read the (now status-bearing) object + re-issue the SAME status →
    // NoChange, and the catalog revision must not advance.
    let parent2 = store.get(&rs_key).await.unwrap();
    let rev_before = store.current_catalog().await.revision();
    let outcome2 = write_status_cas(&store, &rs_key, &parent2, &desired)
        .await
        .unwrap();
    let rev_after = store.current_catalog().await.revision();
    assert_eq!(outcome2, StatusWriteOutcome::NoChange);
    assert_eq!(
        rev_before, rev_after,
        "a NoChange status write proposes nothing → revision unchanged"
    );

    // Sanity: a genuine status write returns ResourceOp::Patched (item-5
    // CAS path), proving the write went through the deterministic apply.
    let parent3 = store.get(&rs_key).await.unwrap();
    let rv = parent3
        .get("metadata")
        .and_then(|m| m.get("resourceVersion"))
        .and_then(|r| r.as_str())
        .and_then(|s| s.parse::<u64>().ok())
        .map(engenho_store::Revision)
        .unwrap();
    let result = store
        .propose(ResourceCommand::Patch {
            key: rs_key.clone(),
            patch: json!({"status": {"readyReplicas": 1}}),
            patch_type: engenho_store::command::PatchType::Merge,
            apply: None,
            expected: Some(rv),
            reason: Reason::Controller,
        })
        .await
        .unwrap();
    assert_eq!(result.op, ResourceOp::Patched);

    drop(store);
}
