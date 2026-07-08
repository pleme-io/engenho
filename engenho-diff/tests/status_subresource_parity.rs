//! `/status` subresource parity.
//!
//! Two axes, against a Pod (declares a `/status` subresource) and a ConfigMap
//! (declares NONE — the negative case that matters):
//!
//!   1. **Routing.** `/status` GET+PUT on a ConfigMap MUST 404 on both sides
//!      (no status subresource); `/status` GET on a Pod MUST 200 on both.
//!   2. **The spec/status write-split.** A `/status` PUT MUST NOT change
//!      `spec` (spec edits on the status endpoint are dropped) — proven
//!      FIELD-FOCUSED on `spec.containers[0].image`, immune to PodSpec
//!      defaulting.
//!
//! ── The other half of the write-split (a MAIN-object PUT drops status) ──
//! is enforced in engenho's `replace()` (preserve the live `.status` for any
//! kind declaring a Status subresource) and proven by the engenho-apiserver
//! unit test `m0_1_subresources::main_put_preserves_status` (on a Deployment,
//! whose spec is mutable). It is NOT differentially observable on a Pod: k3s
//! rejects a minimal-body Pod main-PUT with a spec-immutability 422 (a
//! separate gap engenho lacks — recorded below), and a Pod's status is
//! kubelet-churned while engenho populates none. The main-PUT op here THEREFORE
//! surfaces that immutability divergence, honestly baselined.
//!
//! Fail-loud on an unreachable oracle; RATCHET against [`KNOWN_DIVERGENCES`].

mod common;

use engenho_diff::{JsonPath, Operation, volatile_meta};
use serde_json::json;

/// status-subresource baseline.
///
/// The routing negatives + the spec-write-split run to PARITY. The ONE
/// recorded divergence is a MAIN-object PUT on a Pod:
///
///   * `StatusCodeDiff:replace/status-pod:PUT` — engenho returns 200; k3s
///     returns 422 `Forbidden: pod updates may not change fields other than
///     spec.containers[*].image,…`. This is engenho's MISSING Pod-spec
///     IMMUTABILITY admission validation (a client can mutate an immutable
///     PodSpec field), NOT a status-subresource bug — the status-drop half of
///     the write-split is separately enforced + unit-tested (see the module
///     doc). Gated on the engenho-apiserver admission/validation milestone
///     (per-kind immutable-field enforcement).
const KNOWN_DIVERGENCES: &[&str] = &["StatusCodeDiff:replace/status-pod:PUT"];

#[tokio::test(flavor = "multi_thread")]
async fn status_subresource_parity() {
    let engenho = common::boot_engenho(&["ConfigMap", "Namespace", "Pod"]).await;
    let k3s = common::load_oracle().await;

    let norm = volatile_meta();
    let suffix = common::unique_suffix();
    let ns = {
        let mut s = String::from("engenho-diff-status-");
        s.push_str(&suffix);
        s
    };
    let cm = "status-cm";
    let pod = "status-pod";
    let orig_image = "registry.k8s.io/pause:3.9";
    let c0 = JsonPath::parse("spec.containers[0].image");

    // Setup (not diffed): namespace + a ConfigMap + a Pod.
    let setup = [
        Operation::create_namespace(&ns),
        Operation::create_configmap(&ns, cm, json!({"a": "b"})),
        Operation::create_pod(&ns, pod, orig_image),
    ];
    for op in &setup {
        common::exec_only(op, &engenho, &k3s).await;
    }

    // A /status body carrying a MUTATED spec.image — the spec edit MUST be
    // dropped by the /status endpoint (both sides).
    let status_body_mut_spec = json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": pod, "namespace": ns},
        "spec": {"containers": [{"name": "c", "image": "mutated:9.9"}]},
        "status": {"message": "engenho-diff-status-probe"},
    });
    // A MAIN-object body carrying a bogus status — surfaces the immutability
    // divergence (engenho 200, k3s 422) recorded in the baseline.
    let main_body_mut_status = json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": pod, "namespace": ns},
        "spec": {"containers": [{"name": "c", "image": orig_image}]},
        "status": {"phase": "Bogus"},
    });

    let ops = [
        // ── routing negatives (ConfigMap has NO /status) ─────────────────
        Operation::get_status(&ns, "configmaps", cm),
        Operation::put_status(&ns, "configmaps", cm, json!({"status": {"x": "y"}})),
        // ── spec/status write-split: a /status PUT drops spec ────────────
        Operation::put_status(&ns, "pods", pod, status_body_mut_spec).with_focus(c0.clone()),
        // ── routing positive: /status GET on a Pod routes + returns object ─
        Operation::get_status(&ns, "pods", pod).with_focus(c0.clone()),
        // ── main-PUT (surfaces the immutability divergence; baselined) ────
        Operation::replace(&ns, "pods", pod, main_body_mut_status),
    ];

    let mut findings = Vec::new();
    for op in &ops {
        findings.push(common::run_one(op, &engenho, &k3s, &norm).await);
    }

    common::cleanup(&k3s, &Operation::delete_namespace(&ns).path).await;

    common::print_report("STATUS SUBRESOURCE", &findings);
    engenho.shutdown().await.expect("engenho shuts down");
    common::assert_ratchet(&findings, KNOWN_DIVERGENCES);
}
