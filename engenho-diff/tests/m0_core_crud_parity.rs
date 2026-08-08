//! M0 — the core CRUD differential run.
//!
//! Boots engenho IN-PROCESS and diffs it against the LIVE k3s reference
//! oracle (`~/.kube/engenho-local-tunnel.yaml`, k3s v1.34.x) across a full
//! object lifecycle (create Namespace → create/get/list/patch ConfigMap →
//! the sharp PUT/replace verb probe → delete) plus a core/v1 discovery diff.
//!
//! **Fail loud, never silent-skip.** If the oracle is unreachable the test
//! PANICS with `Verdict::ReferenceUnreachable` — a live differential harness
//! is meaningless without its oracle. (Mark `#[ignore]` to gate it out of a
//! cluster-less CI; it is intentionally NOT ignored so the operator's
//! `cargo test -p engenho-diff --test m0_core_crud_parity -- --nocapture`
//! exercises it directly.)
//!
//! **The ratchet.** The observed HARD divergences must be a SUBSET of
//! [`KNOWN_DIVERGENCES`]; the test is green on the known pre-conformance gaps
//! and goes red only when a NEW (unlisted) hard divergence appears. Cosmetic
//! (masked) diffs are reported for auditability but never ratchet.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use engenho_diff::{
    DiffTarget, Divergence, EngenhoTarget, HttpMethod, K3sTarget, Operation, Severity, Verdict,
    cotejo, volatile_meta,
};

/// Baseline of engenho-vs-k3s divergence *signatures* known to exist before
/// conformance. Recorded from a live run (see the report the test prints).
/// The test fails only on a signature NOT in this set.
const KNOWN_DIVERGENCES: &[&str] = &[
    // ── D1. PUT/replace — IRONED OUT. engenho's router used to refuse PUT on
    //    the main object (router.rs resource_put → BadRequest 400). It now
    //    does a real optimistic-concurrency replace via the store's
    //    ResourceCommand::Put (handler.rs `replace` + `do_replace`), so this
    //    op is now PARITY. The old
    //    `MissingVerb:v1/configmaps:update:k3s` signature is intentionally
    //    absent from this baseline — the ratchet would go RED if it regressed.
    // ── D2. Namespace server-side defaulting — IRONED OUT.
    //    `stamp_namespace_create_defaults` now seeds the `kubernetes`
    //    finalizer on `spec.finalizers` (not metadata.finalizers) and adds
    //    the `kubernetes.io/metadata.name` auto-label, matching
    //    kube-apiserver. The old `MissingDefault:.spec` /
    //    `ExtraField:.metadata.finalizers:engenho` /
    //    `MissingDefault:.metadata.labels` signatures are intentionally
    //    absent — the ratchet goes RED if they regress. ──
    // ── D3. LIST items carry TypeMeta — IRONED OUT. The handler's list
    //    emission now STRIPS item-level apiVersion+kind (handler.rs
    //    `strip_type_meta`); the <Kind>List envelope carries the GVK, each
    //    item is TypeMeta-less on the wire (matching kube-apiserver). The
    //    old `ExtraField:.apiVersion:engenho` / `ExtraField:.kind:engenho`
    //    signatures are intentionally absent — the ratchet goes RED if they
    //    regress. ──
    // ── D4. DELETE response shape — BASELINED (gated on the deletion
    //    lifecycle; needs a design decision, NOT a bounded parity tweak).
    //    The k8s DELETE return shape is PER-RESOURCE, not uniform (verified
    //    against the oracle 2026-07-08):
    //      * an IMMEDIATELY-removed object (ConfigMap / Secret — no grace
    //        period, no finalizer) ⇒ metav1.Status{status:Success,
    //        details:{name,kind,uid}};
    //      * an object that survives the call as Terminating (a Pod's default
    //        30s grace period; anything with a finalizer / deletionTimestamp)
    //        ⇒ the OBJECT itself (kind:Pod, status.phase advanced).
    //    engenho has no grace-period / graceful-deletion lifecycle, so it
    //    removes every object immediately and returns the pre-image
    //    uniformly. That happens to be CONFORMANT for the graceful kinds
    //    (Pod delete returns a Pod — engenho's r7 delete test asserts exactly
    //    this, and k8s agrees) but WRONG for the immediate kinds (ConfigMap
    //    should be Status). A uniform switch to Status-return would both
    //    break the r7 Pod-object contract AND be non-conformant for Pods.
    //    The load-bearing fix is the pod deletion lifecycle (grace period →
    //    Terminating → kubelet-confirmed removal + immediate-vs-graceful
    //    dispatch), a real milestone — so D4 stays baselined until then. ──
    "FieldDiff:.kind",
    "MissingDefault:.details",
    "ExtraField:.metadata.name:engenho",
    "ExtraField:.metadata.namespace:engenho",
    "ExtraField:.data:engenho",
    // ── D5a. configmaps deletecollection verb — IRONED OUT. engenho now
    //    serves deletecollection (router collection-DELETE →
    //    ResourceHandler::delete_collection, list-then-delete-each returning
    //    the <Kind>List of pre-images) and advertises it in discovery for
    //    every kind that has a CollectionDeleter (all but namespaces /
    //    bindings / componentstatuses). The old
    //    `DiscoveryDiff:/v1:configmaps: engenho missing verbs
    //    [deletecollection]` signature is intentionally absent. ──
    // ── D5b. namespaces/finalize subresource — still absent. k3s exposes
    //    `namespaces/finalize` (verbs [update]), used by the
    //    NamespaceController to clear spec.finalizers during Terminating.
    //    engenho has no namespace-GC controller wired, so the subresource
    //    route + handler are gated on that controller (adding the route
    //    without the controller would advertise a no-op). Baselined. ──
    "MissingSubresource:v1/namespaces:finalize:k3s",
];

fn oracle_kubeconfig() -> PathBuf {
    // ENGENHO_ORACLE_KUBECONFIG first. The path was hardcoded to a hand-built
    // k3s VM's tunnel, and that VM is GONE (192.168.64.10 unreachable
    // 2026-08-08) — which is why all four engenho-diff binaries fail with
    // `Verdict::ReferenceUnreachable` rather than for any engenho defect. A
    // harness whose oracle cannot be repointed dies with the machine that
    // built it; `k3d cluster create` is reproducible where that VM was not.
    if let Ok(p) = std::env::var("ENGENHO_ORACLE_KUBECONFIG") {
        if !p.is_empty() {
            return PathBuf::from(p);
        }
    }
    let home = std::env::var("HOME").expect("HOME set");
    PathBuf::from(home).join(".kube/engenho-local-tunnel.yaml")
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    nanos.to_string()
}

struct OpFindings {
    op_id: String,
    hard: Vec<Divergence>,
    cosmetic: Vec<Divergence>,
}

#[tokio::test(flavor = "multi_thread")]
async fn m0_core_crud_parity() {
    // ── Boot the two targets ─────────────────────────────────────────────
    let engenho = EngenhoTarget::boot_in_process(&["ConfigMap", "Namespace", "Pod"])
        .await
        .expect("engenho boots in-process");

    let kubeconfig = oracle_kubeconfig();
    let k3s = K3sTarget::from_kubeconfig(&kubeconfig)
        .unwrap_or_else(|e| panic!("cannot load oracle kubeconfig {kubeconfig:?}: {e}"));

    // ── Fail loud if the oracle is down ──────────────────────────────────
    if let Some(Verdict::ReferenceUnreachable) = k3s.preflight().await {
        panic!(
            "Verdict::ReferenceUnreachable — the k3s oracle at {kubeconfig:?} did not answer \
             GET /api/v1. A live differential run cannot proceed. (Is the tunnel up? \
             `kubectl --kubeconfig {} --tls-server-name 127.0.0.1 get ns`)",
            kubeconfig.display()
        );
    }

    let norm = volatile_meta();
    let suffix = unique_suffix();
    let ns = {
        let mut s = String::from("engenho-diff-");
        s.push_str(&suffix);
        s
    };
    let cm = "probe-cm";

    // ── The authored operation set ───────────────────────────────────────
    // The PUT/replace body is fetched from the LIVE k3s object so k3s
    // returns 200 (a valid replace) — engenho's router refuses PUT
    // regardless, so the two sides disagree on the verb.
    let mut ops: Vec<Operation> = vec![
        Operation::create_namespace(&ns),
        Operation::get_namespace(&ns),
        Operation::create_configmap(&ns, cm, serde_json::json!({"greeting": "ola"})),
        Operation::get_configmap(&ns, cm),
        Operation::list_configmaps(&ns, cm),
        Operation::merge_patch_configmap(&ns, cm, serde_json::json!({"data": {"tchau": "bye"}})),
    ];

    // Run the pre-PUT ops so the object exists on both sides, capturing
    // findings. Then fetch k3s's live object to build the replace body.
    let mut findings: Vec<OpFindings> = Vec::new();
    for op in &ops {
        findings.push(run_one(op, &engenho, &k3s, &norm).await);
    }

    // Build + run the PUT/replace verb probe. A MINIMAL body WITHOUT a
    // resourceVersion ⇒ an unconditional replace that BOTH sides accept (the
    // one-op/two-target model can't carry each side's own rv for a CAS). This
    // was the sharp D1 divergence (engenho's router refused PUT → 400); after
    // the replace-path fix in engenho-apiserver it flips to Parity.
    let put_body = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": cm, "namespace": ns},
        "data": {"greeting": "ola", "tchau": "bye"},
    });
    let put_op = Operation::replace_configmap(&ns, cm, put_body);
    findings.push(run_one(&put_op, &engenho, &k3s, &norm).await);
    ops.push(put_op);

    // Delete + discovery.
    let delete_op = Operation::delete_configmap(&ns, cm);
    findings.push(run_one(&delete_op, &engenho, &k3s, &norm).await);
    ops.push(delete_op);

    let disc_op = Operation::discovery_core_v1();
    findings.push(run_one(&disc_op, &engenho, &k3s, &norm).await);
    ops.push(disc_op);

    // ── Cleanup the shared cluster (best-effort, not diffed) ──────────────
    let _ = k3s
        .raw(
            HttpMethod::Delete,
            &Operation::delete_namespace(&ns).path,
            None,
            None,
        )
        .await;

    // ── Report ───────────────────────────────────────────────────────────
    print_report(&findings);

    // ── Ratchet ──────────────────────────────────────────────────────────
    let observed_hard: BTreeSet<String> = findings
        .iter()
        .flat_map(|f| f.hard.iter())
        .map(Divergence::signature)
        .collect();
    let known: BTreeSet<String> = KNOWN_DIVERGENCES.iter().map(|s| (*s).to_string()).collect();
    let new: Vec<&String> = observed_hard.difference(&known).collect();

    // Shut engenho down before the assert (so a failing assert still cleans).
    engenho.shutdown().await.expect("engenho shuts down");

    assert!(
        new.is_empty(),
        "NEW hard divergences not in KNOWN_DIVERGENCES (the ratchet caught a regression \
         OR the baseline needs updating):\n  {}\n\nFull observed hard baseline (copy into \
         KNOWN_DIVERGENCES if intended):\n{}",
        new.iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\n  "),
        observed_hard
            .iter()
            .map(|s| {
                let mut line = String::from("    \"");
                line.push_str(s);
                line.push_str("\",");
                line
            })
            .collect::<Vec<_>>()
            .join("\n")
    );
}

async fn run_one(
    op: &Operation,
    engenho: &EngenhoTarget,
    k3s: &K3sTarget,
    norm: &engenho_diff::Normalizer,
) -> OpFindings {
    let (hard, cosmetic) = cotejo::run_strict(op, engenho, k3s, norm)
        .await
        .unwrap_or_else(|e| panic!("op {} failed: {e}", op.id));
    OpFindings {
        op_id: op.id.clone(),
        hard,
        cosmetic,
    }
}

fn print_report(findings: &[OpFindings]) {
    eprintln!("\n════════════ engenho-diff M0: engenho vs k3s v1.34 ════════════");
    let mut total_hard = 0usize;
    let mut total_cosmetic = 0usize;
    for f in findings {
        let verdict = if f.hard.is_empty() {
            "PARITY"
        } else {
            "DIVERGENT"
        };
        eprintln!("\n▶ {} — {verdict}", f.op_id);
        for d in &f.hard {
            eprintln!("    {d}   [owner: {}]", d.owning_crate());
            total_hard += 1;
        }
        for d in &f.cosmetic {
            debug_assert_eq!(d.severity(), Severity::Cosmetic);
            eprintln!("    (masked) {d}");
            total_cosmetic += 1;
        }
    }
    eprintln!(
        "\n──────────── {total_hard} hard divergence(s), {total_cosmetic} cosmetic (masked) ────────────\n"
    );
}
