//! PATCH parity — the three K8s patch content-types against a ConfigMap
//! (full-object parity kind) AND a Pod (strategic list-merge vehicle,
//! FIELD-FOCUSED to isolate the merge signal from PodSpec defaulting).
//!
//! Content-types exercised:
//!   * `application/merge-patch+json`            (RFC 7386 — deep map merge,
//!                                                 `null` deletes a key)
//!   * `application/strategic-merge-patch+json`  (K8s strategic merge —
//!                                                 list-merge-by-key)
//!   * `application/json-patch+json`             (RFC 6902 — op array)
//!
//! Each PATCH's RESPONSE (the mutated object) is diffed against the oracle.
//! ConfigMap diffs whole-object (proven parity in m0); Pod diffs are FOCUSED
//! on the single field the patch touches, so a Pod's unrelated server-side
//! defaulting (imagePullPolicy, terminationMessagePath, …) never pollutes the
//! ratchet — the highest-value probe (strategic `kubectl set image` preserving
//! a sibling container) is proven at `spec.containers[1].image`.
//!
//! Fail-loud on an unreachable oracle; RATCHET against [`KNOWN_DIVERGENCES`].

mod common;

use engenho_diff::{JsonPath, Operation, volatile_meta};
use serde_json::json;

/// PATCH baseline. engenho's store ships all three algorithms
/// (`engenho_store::patch_apply`: RFC7386 merge, RFC6902 json-patch,
/// OpenAPI-backed strategic list-merge), so the matrix runs to PARITY —
/// this baseline is EMPTY. A new content-type divergence appears here.
const KNOWN_DIVERGENCES: &[&str] = &[];

#[tokio::test(flavor = "multi_thread")]
async fn patch_parity() {
    let engenho = common::boot_engenho(&["ConfigMap", "Namespace", "Pod"]).await;
    let k3s = common::load_oracle().await;

    let norm = volatile_meta();
    let suffix = common::unique_suffix();
    let ns = {
        let mut s = String::from("engenho-diff-patch-");
        s.push_str(&suffix);
        s
    };

    let mut findings = Vec::new();

    // Setup: namespace + a ConfigMap with two keys.
    let cm = "patch-cm";
    let setup = [
        Operation::create_namespace(&ns),
        Operation::create_configmap(&ns, cm, json!({"greeting": "ola", "keep": "yes"})),
    ];

    // ── ConfigMap: whole-object patch matrix ─────────────────────────────
    let cm_ops = [
        // merge-patch: null deletes `greeting`, adds `mp`.
        Operation::merge_patch(
            &ns,
            "configmaps",
            cm,
            json!({"data": {"greeting": null, "mp": "1"}}),
        ),
        // strategic-patch on a ConfigMap = map merge (no list metadata): adds `sp`.
        Operation::strategic_patch(&ns, "configmaps", cm, json!({"data": {"sp": "1"}})),
        // json-patch: remove `keep`, add `jp`.
        Operation::json_patch(
            &ns,
            "configmaps",
            cm,
            json!([
                {"op": "remove", "path": "/data/keep"},
                {"op": "add", "path": "/data/jp", "value": "1"},
            ]),
        ),
        // Final GET — the converged object.
        Operation::get_configmap(&ns, cm),
    ];

    // ── Pod: strategic list-merge-by-key, FIELD-FOCUSED ──────────────────
    let pod = "patch-pod";
    let c0 = JsonPath::parse("spec.containers[0].image");
    let c1 = JsonPath::parse("spec.containers[1].image");
    let lbl_mp = JsonPath::parse("metadata.labels.mp");
    let lbl_jp = JsonPath::parse("metadata.labels.jp");
    let pod_setup = [Operation::create_pod_containers(
        &ns,
        pod,
        &[("main", "nginx:1.27"), ("sidecar", "envoy:1.30")],
    )];
    let pod_ops = [
        // strategic set-image on `main` ONLY: main updates, sidecar MUST survive
        // (merge-by-key `name`, not whole-list replace). Two focused probes.
        Operation::strategic_patch(
            &ns,
            "pods",
            pod,
            json!({"spec": {"containers": [{"name": "main", "image": "nginx:1.28"}]}}),
        )
        .with_focus(c0.clone()),
        Operation::get(&ns, "pods", pod).with_focus(c1.clone()),
        // merge-patch adds a label; json-patch adds another. Focused on the key.
        Operation::merge_patch(
            &ns,
            "pods",
            pod,
            json!({"metadata": {"labels": {"mp": "1"}}}),
        )
        .with_focus(lbl_mp.clone()),
        Operation::json_patch(
            &ns,
            "pods",
            pod,
            json!([{"op": "add", "path": "/metadata/labels/jp", "value": "1"}]),
        )
        .with_focus(lbl_jp.clone()),
    ];

    // Setup mutates state on both sides but is NOT diffed (a Pod create's
    // whole-object response would flood the ratchet with PodSpec defaulting).
    for op in setup.iter().chain(pod_setup.iter()) {
        common::exec_only(op, &engenho, &k3s).await;
    }
    // The PATCH responses + final GET ARE diffed (ConfigMap whole-object;
    // Pod field-focused).
    for op in cm_ops.iter().chain(pod_ops.iter()) {
        findings.push(common::run_one(op, &engenho, &k3s, &norm).await);
    }

    // Cleanup the shared cluster (best-effort).
    common::cleanup(&k3s, &Operation::delete_namespace(&ns).path).await;

    common::print_report("PATCH", &findings);
    engenho.shutdown().await.expect("engenho shuts down");
    common::assert_ratchet(&findings, KNOWN_DIVERGENCES);
}
